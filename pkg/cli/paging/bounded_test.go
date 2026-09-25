package paging

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestListBoundedPreservesSelectorsAndStopsAtItemLimit(t *testing.T) {
	t.Parallel()

	base := metav1.ListOptions{
		LabelSelector: "ome.io/inferenceservice=chat",
		FieldSelector: "status.phase=Running",
	}
	var requests []metav1.ListOptions
	pages := []Page[string]{
		{Items: []string{"a", "b"}, Continue: "next"},
		{Items: []string{"c"}, Continue: "more"},
	}

	got, err := ListBounded(context.Background(), base, Limits{
		PageSize:       2,
		MaxItems:       3,
		MaxPages:       5,
		RequestTimeout: time.Second,
	}, func(_ context.Context, opts metav1.ListOptions) (Page[string], error) {
		requests = append(requests, opts)
		return pages[len(requests)-1], nil
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, got.Items)
	assert.True(t, got.Truncated)
	assert.Equal(t, 2, got.Pages)
	assert.Equal(t, 2, got.ObservedPages)
	assert.Equal(t, 3, got.ReturnedItems)
	assert.Equal(t, 3, got.ConsumedItems)
	require.Len(t, requests, 2)
	assert.Equal(t, int64(2), requests[0].Limit)
	assert.Empty(t, requests[0].Continue)
	assert.Equal(t, int64(1), requests[1].Limit)
	assert.Equal(t, "next", requests[1].Continue)
	for _, request := range requests {
		assert.Equal(t, base.LabelSelector, request.LabelSelector)
		assert.Equal(t, base.FieldSelector, request.FieldSelector)
	}
}

func TestListBoundedRejectsInvalidLimitsWithoutFetching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		limits Limits
	}{
		{name: "page size", limits: Limits{MaxItems: 1, MaxPages: 1, RequestTimeout: time.Second}},
		{name: "item limit", limits: Limits{PageSize: 1, MaxPages: 1, RequestTimeout: time.Second}},
		{name: "page limit", limits: Limits{PageSize: 1, MaxItems: 1, RequestTimeout: time.Second}},
		{name: "request timeout", limits: Limits{PageSize: 1, MaxItems: 1, MaxPages: 1}},
		{name: "requests below pages", limits: Limits{PageSize: 1, MaxItems: 1, MaxPages: 2, MaxRequests: 1, RequestTimeout: time.Second}},
		{name: "requests above recovery cap", limits: Limits{PageSize: 1, MaxItems: 1, MaxPages: 2, MaxRequests: 5, RequestTimeout: time.Second}},
		{name: "work items below items", limits: Limits{PageSize: 1, MaxItems: 2, MaxPages: 1, MaxConsumedItems: 1, RequestTimeout: time.Second}},
		{name: "work items above recovery cap", limits: Limits{PageSize: 1, MaxItems: 2, MaxPages: 1, MaxConsumedItems: 5, RequestTimeout: time.Second}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			called := false
			_, err := ListBounded(context.Background(), metav1.ListOptions{}, test.limits,
				func(context.Context, metav1.ListOptions) (Page[string], error) {
					called = true
					return Page[string]{}, nil
				})

			require.Error(t, err)
			assert.False(t, called)
		})
	}
}

func TestListBoundedClipsNonconformingOverfilledPage(t *testing.T) {
	t.Parallel()

	calls := 0
	got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize:         2,
		MaxItems:         2,
		MaxPages:         1,
		MaxConsumedItems: 2,
		RequestTimeout:   time.Second,
	}, func(context.Context, metav1.ListOptions) (Page[string], error) {
		calls++
		return Page[string]{Items: []string{"a", "b", "c"}, Continue: "more"}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, got.Items)
	assert.True(t, got.Truncated)
	assert.Equal(t, 1, got.Pages)
	assert.Equal(t, 1, got.ObservedPages)
	assert.Equal(t, 3, got.ReturnedItems)
	assert.Equal(t, 3, got.ConsumedItems)
	assert.Equal(t, 1, calls)
}

func TestListBoundedExpiredRestartRespectsTightenedRequestBudget(t *testing.T) {
	t.Parallel()

	expired := apierrors.NewResourceExpired("private detail")
	calls := 0
	got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize: 1, MaxItems: 2, MaxPages: 2, MaxRequests: 2,
		RequestTimeout: time.Second,
	}, func(context.Context, metav1.ListOptions) (Page[string], error) {
		calls++
		if calls == 1 {
			return Page[string]{Items: []string{"stale"}, Continue: "old-token"}, nil
		}
		return Page[string]{}, expired
	})

	require.ErrorIs(t, err, expired)
	assert.Empty(t, got.Items)
	assert.Equal(t, 2, got.Pages)
	assert.Zero(t, got.ObservedPages)
	assert.Zero(t, got.ReturnedItems)
	assert.Equal(t, 1, got.ConsumedItems)
	assert.Equal(t, 2, calls)
}

func TestListBoundedRejectsNonAdvancingContinueToken(t *testing.T) {
	t.Parallel()

	calls := 0
	got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize:       1,
		MaxItems:       5,
		MaxPages:       5,
		RequestTimeout: time.Second,
	}, func(context.Context, metav1.ListOptions) (Page[string], error) {
		calls++
		return Page[string]{Items: []string{"item"}, Continue: "stuck"}, nil
	})

	require.EqualError(t, err, "continue token did not advance after page 2")
	assert.Equal(t, []string{"item"}, got.Items)
	assert.Equal(t, 2, got.Pages)
	assert.Equal(t, 2, got.ObservedPages)
	assert.Equal(t, 2, got.ReturnedItems)
	assert.Equal(t, 2, got.ConsumedItems)
	assert.Equal(t, 2, calls)
}

func TestListBoundedRejectsContinueTokenCycle(t *testing.T) {
	t.Parallel()

	responses := []Page[string]{
		{Items: []string{"first"}, Continue: "secret-token-a"},
		{Items: []string{"second"}, Continue: "secret-token-b"},
		{Items: []string{"cycle-closing"}, Continue: "secret-token-a"},
	}
	calls := 0
	got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize:       1,
		MaxItems:       5,
		MaxPages:       5,
		RequestTimeout: time.Second,
	}, func(context.Context, metav1.ListOptions) (Page[string], error) {
		if calls >= len(responses) {
			return Page[string]{}, assert.AnError
		}
		response := responses[calls]
		calls++
		return response, nil
	})

	require.EqualError(t, err, "continue token cycle detected after page 3")
	assert.Equal(t, []string{"first", "second"}, got.Items)
	assert.Equal(t, 3, got.Pages)
	assert.Equal(t, 3, calls)
	assert.NotContains(t, err.Error(), "secret-token-a")
	assert.NotContains(t, err.Error(), "secret-token-b")
}

func TestListBoundedRejectsCycleToInitialContinueToken(t *testing.T) {
	t.Parallel()

	base := metav1.ListOptions{Continue: "secret-start-token"}
	responses := []Page[string]{
		{Items: []string{"first"}, Continue: "secret-token-b"},
		{Items: []string{"cycle-closing"}, Continue: "secret-start-token"},
	}
	var requests []metav1.ListOptions
	got, err := ListBounded(context.Background(), base, Limits{
		PageSize:       1,
		MaxItems:       5,
		MaxPages:       5,
		RequestTimeout: time.Second,
	}, func(_ context.Context, opts metav1.ListOptions) (Page[string], error) {
		requests = append(requests, opts)
		if len(requests) > len(responses) {
			return Page[string]{}, assert.AnError
		}
		return responses[len(requests)-1], nil
	})

	require.EqualError(t, err, "continue token cycle detected after page 2")
	assert.Equal(t, []string{"first"}, got.Items)
	assert.Equal(t, 2, got.Pages)
	require.Len(t, requests, 2)
	assert.Equal(t, "secret-start-token", requests[0].Continue)
	assert.Equal(t, "secret-token-b", requests[1].Continue)
	assert.NotContains(t, err.Error(), "secret-start-token")
	assert.NotContains(t, err.Error(), "secret-token-b")
}

func TestListBoundedGivesEveryRequestItsOwnTimeout(t *testing.T) {
	t.Parallel()

	const timeout = 5 * time.Second
	_, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize:       1,
		MaxItems:       1,
		MaxPages:       1,
		RequestTimeout: timeout,
	}, func(ctx context.Context, _ metav1.ListOptions) (Page[string], error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok, "fetch context must have a deadline")
		remaining := time.Until(deadline)
		assert.Positive(t, remaining)
		assert.LessOrEqual(t, remaining, timeout)
		return Page[string]{Items: []string{"item"}}, nil
	})

	require.NoError(t, err)
}

func TestListBoundedRejectsSuccessAfterRequestTimeout(t *testing.T) {
	t.Parallel()

	got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize:       1,
		MaxItems:       1,
		MaxPages:       1,
		RequestTimeout: time.Millisecond,
	}, func(ctx context.Context, _ metav1.ListOptions) (Page[string], error) {
		<-ctx.Done()
		return Page[string]{Items: []string{"too-late"}}, nil
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, got.Items)
	assert.Equal(t, 1, got.Pages)
	assert.Zero(t, got.ObservedPages)
	assert.Zero(t, got.ReturnedItems)
	assert.Zero(t, got.ConsumedItems)
}

func TestListBoundedRestartsExpiredContinuationWithinTotalBudgets(t *testing.T) {
	t.Parallel()

	expired := apierrors.NewResourceExpired("private expiration detail")
	base := metav1.ListOptions{
		LabelSelector:        "ome.io/inferenceservice=chat",
		ResourceVersion:      "17",
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
	}
	var requests []metav1.ListOptions
	got, err := ListBounded(context.Background(), base, Limits{
		PageSize:       2,
		MaxItems:       3,
		MaxPages:       2,
		RequestTimeout: time.Second,
	}, func(_ context.Context, opts metav1.ListOptions) (Page[string], error) {
		requests = append(requests, opts)
		switch len(requests) {
		case 1:
			return Page[string]{
				Items: []string{"stale-a", "stale-b"}, Continue: "shared-token",
			}, nil
		case 2:
			return Page[string]{}, expired
		case 3:
			return Page[string]{
				Items: []string{"fresh-a", "fresh-b"}, Continue: "shared-token",
			}, nil
		case 4:
			return Page[string]{Items: []string{"fresh-c"}}, nil
		default:
			return Page[string]{}, assert.AnError
		}
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"fresh-a", "fresh-b", "fresh-c"}, got.Items)
	assert.Equal(t, 4, got.Pages, "the discarded attempt must consume page budget")
	assert.Equal(t, 2, got.ObservedPages)
	assert.Equal(t, 3, got.ReturnedItems)
	assert.Equal(t, 5, got.ConsumedItems, "discarded items must consume item budget")
	assert.False(t, got.Truncated)
	require.Len(t, requests, 4)
	assert.Equal(t, []string{"", "shared-token", "", "shared-token"}, []string{
		requests[0].Continue,
		requests[1].Continue,
		requests[2].Continue,
		requests[3].Continue,
	})
	assert.Equal(t, []int64{2, 1, 2, 1}, []int64{
		requests[0].Limit,
		requests[1].Limit,
		requests[2].Limit,
		requests[3].Limit,
	})
	for _, request := range requests {
		assert.Equal(t, base.LabelSelector, request.LabelSelector)
	}
	assert.Equal(t, []string{"17", "", "17", ""}, []string{
		requests[0].ResourceVersion,
		requests[1].ResourceVersion,
		requests[2].ResourceVersion,
		requests[3].ResourceVersion,
	})
	assert.Equal(t, []metav1.ResourceVersionMatch{
		metav1.ResourceVersionMatchExact, "", metav1.ResourceVersionMatchExact, "",
	}, []metav1.ResourceVersionMatch{
		requests[0].ResourceVersionMatch,
		requests[1].ResourceVersionMatch,
		requests[2].ResourceVersionMatch,
		requests[3].ResourceVersionMatch,
	})
}

func TestListBoundedRestartsWrappedKubernetes410Reasons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "expired",
			err:  fmt.Errorf("safe request wrapper: %w", apierrors.NewResourceExpired("private")),
		},
		{
			name: "gone",
			err: fmt.Errorf("safe request wrapper: %w", &apierrors.StatusError{ErrStatus: metav1.Status{
				Status:  metav1.StatusFailure,
				Code:    http.StatusGone,
				Reason:  metav1.StatusReasonGone,
				Message: "private",
			}}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			calls := 0
			got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
				PageSize: 1, MaxItems: 2, MaxPages: 2, RequestTimeout: time.Second,
			}, func(context.Context, metav1.ListOptions) (Page[string], error) {
				calls++
				switch calls {
				case 1:
					return Page[string]{Items: []string{"stale"}, Continue: "old-token"}, nil
				case 2:
					return Page[string]{}, test.err
				case 3:
					return Page[string]{Items: []string{"fresh"}}, nil
				default:
					return Page[string]{}, assert.AnError
				}
			})

			require.NoError(t, err)
			assert.Equal(t, []string{"fresh"}, got.Items)
			assert.Equal(t, 3, got.Pages)
			assert.Equal(t, 1, got.ObservedPages)
			assert.Equal(t, 1, got.ReturnedItems)
			assert.Equal(t, 2, got.ConsumedItems)
			assert.Equal(t, 3, calls)
		})
	}
}

func TestListBoundedReturnsSecondExpiredContinuationWithoutStaleItems(t *testing.T) {
	t.Parallel()

	firstExpired := apierrors.NewResourceExpired("first private detail")
	secondExpired := apierrors.NewResourceExpired("second private detail")
	calls := 0
	got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize:       1,
		MaxItems:       5,
		MaxPages:       5,
		RequestTimeout: time.Second,
	}, func(context.Context, metav1.ListOptions) (Page[string], error) {
		calls++
		switch calls {
		case 1:
			return Page[string]{Items: []string{"stale"}, Continue: "old-token"}, nil
		case 2:
			return Page[string]{}, firstExpired
		case 3:
			return Page[string]{Items: []string{"fresh"}, Continue: "new-token"}, nil
		case 4:
			return Page[string]{}, secondExpired
		default:
			return Page[string]{}, assert.AnError
		}
	})

	require.ErrorIs(t, err, secondExpired)
	assert.Equal(t, []string{"fresh"}, got.Items)
	assert.Equal(t, 4, got.Pages)
	assert.Equal(t, 1, got.ObservedPages)
	assert.Equal(t, 1, got.ReturnedItems)
	assert.Equal(t, 2, got.ConsumedItems)
	assert.Equal(t, 4, calls, "an expired continuation must restart at most once")
}

func TestListBoundedDoesNotRestartExpiredFirstPage(t *testing.T) {
	t.Parallel()

	expired := apierrors.NewResourceExpired("private detail")
	calls := 0
	got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize:       1,
		MaxItems:       5,
		MaxPages:       5,
		RequestTimeout: time.Second,
	}, func(context.Context, metav1.ListOptions) (Page[string], error) {
		calls++
		return Page[string]{}, expired
	})

	require.ErrorIs(t, err, expired)
	assert.Empty(t, got.Items)
	assert.Equal(t, 1, got.Pages)
	assert.Equal(t, 1, calls)
}

func TestListBoundedDoesNotRestartCallerProvidedContinuation(t *testing.T) {
	t.Parallel()

	expired := apierrors.NewResourceExpired("private detail")
	base := metav1.ListOptions{Continue: "private-start-token"}
	var requests []metav1.ListOptions
	got, err := ListBounded(context.Background(), base, Limits{
		PageSize:       1,
		MaxItems:       5,
		MaxPages:       5,
		RequestTimeout: time.Second,
	}, func(_ context.Context, opts metav1.ListOptions) (Page[string], error) {
		requests = append(requests, opts)
		if len(requests) == 1 {
			return Page[string]{Items: []string{"item"}, Continue: "next-token"}, nil
		}
		return Page[string]{}, expired
	})

	require.ErrorIs(t, err, expired)
	assert.Equal(t, []string{"item"}, got.Items)
	assert.Equal(t, 2, got.Pages)
	require.Len(t, requests, 2)
	assert.Equal(t, "private-start-token", requests[0].Continue)
	assert.Equal(t, "next-token", requests[1].Continue)
}

func TestListBoundedCancellationStopsExpiredRestart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	expired := apierrors.NewResourceExpired("private detail")
	calls := 0
	got, err := ListBounded(ctx, metav1.ListOptions{}, Limits{
		PageSize:       1,
		MaxItems:       5,
		MaxPages:       5,
		RequestTimeout: time.Second,
	}, func(context.Context, metav1.ListOptions) (Page[string], error) {
		calls++
		if calls == 1 {
			return Page[string]{Items: []string{"stale"}, Continue: "old-token"}, nil
		}
		cancel()
		return Page[string]{}, expired
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, got.Items, "an expired snapshot must never escape as partial evidence")
	assert.Equal(t, 2, got.Pages)
	assert.Zero(t, got.ObservedPages)
	assert.Zero(t, got.ReturnedItems)
	assert.Equal(t, 1, got.ConsumedItems)
	assert.Equal(t, 2, calls, "cancellation must prevent the restart request")
}

func TestListBoundedExpiredRestartHasBoundedProtocolAllowance(t *testing.T) {
	t.Parallel()

	expired := apierrors.NewResourceExpired("private detail")
	calls := 0
	got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize:       1,
		MaxItems:       2,
		MaxPages:       2,
		RequestTimeout: time.Second,
	}, func(context.Context, metav1.ListOptions) (Page[string], error) {
		calls++
		switch calls {
		case 1:
			return Page[string]{Items: []string{"stale"}, Continue: "old-token"}, nil
		case 2:
			return Page[string]{}, expired
		case 3:
			return Page[string]{Items: []string{"fresh-a"}, Continue: "new-token"}, nil
		case 4:
			return Page[string]{Items: []string{"fresh-b"}}, nil
		default:
			return Page[string]{}, assert.AnError
		}
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"fresh-a", "fresh-b"}, got.Items)
	assert.Equal(t, 4, got.Pages)
	assert.Equal(t, 2, got.ObservedPages)
	assert.Equal(t, 2, got.ReturnedItems)
	assert.Equal(t, 3, got.ConsumedItems)
	assert.False(t, got.Truncated)
	assert.Equal(t, 4, calls, "one restart may at most double the request budget")
}

func TestListBoundedDoesNotRetryOrdinaryContinuationError(t *testing.T) {
	t.Parallel()

	calls := 0
	got, err := ListBounded(context.Background(), metav1.ListOptions{}, Limits{
		PageSize:       1,
		MaxItems:       5,
		MaxPages:       5,
		RequestTimeout: time.Second,
	}, func(context.Context, metav1.ListOptions) (Page[string], error) {
		calls++
		if calls == 1 {
			return Page[string]{Items: []string{"partial"}, Continue: "next-token"}, nil
		}
		return Page[string]{}, assert.AnError
	})

	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, []string{"partial"}, got.Items)
	assert.Equal(t, 2, got.Pages)
	assert.Equal(t, 2, calls)
}
