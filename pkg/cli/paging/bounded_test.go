package paging

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
