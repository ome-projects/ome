package paging

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// fakeObj is a minimal runtime.Object double so these tests can exercise
// ListAllPaged's control flow without depending on any concrete API type.
type fakeObj struct {
	metav1.TypeMeta
	N string
}

func (f *fakeObj) DeepCopyObject() runtime.Object {
	c := *f
	return &c
}

func obj(n string) runtime.Object { return &fakeObj{N: n} }

func TestListAllPagedDrainsThreePagesFollowingContinueTokens(t *testing.T) {
	pages := [][]runtime.Object{
		{obj("a"), obj("b")},
		{obj("c")},
		{obj("d"), obj("e")},
	}
	tokens := []string{"tok-1", "tok-2", ""} // empty token on the 3rd page ends the list
	var gotOpts []metav1.ListOptions
	call := 0
	page := func(opts metav1.ListOptions) ([]runtime.Object, string, error) {
		gotOpts = append(gotOpts, opts)
		items := pages[call]
		tok := tokens[call]
		call++
		return items, tok, nil
	}

	got, err := ListAllPaged(context.Background(), page)

	require.NoError(t, err)
	require.Len(t, got, 5)
	assert.Equal(t, 3, call, "page should be called exactly once per page")

	// Every request asks for a full chunk...
	for _, o := range gotOpts {
		assert.Equal(t, int64(ChunkSize), o.Limit)
	}
	// ...and each page's continue token becomes the *next* request's
	// Continue -- the first request has none yet.
	assert.Empty(t, gotOpts[0].Continue, "first page has no continue token yet")
	assert.Equal(t, "tok-1", gotOpts[1].Continue)
	assert.Equal(t, "tok-2", gotOpts[2].Continue)
}

func TestListAllPagedPropagatesErrorMidStreamAndStopsPaging(t *testing.T) {
	boom := errors.New("etcd is on fire")
	call := 0
	page := func(opts metav1.ListOptions) ([]runtime.Object, string, error) {
		call++
		if call == 1 {
			return []runtime.Object{obj("a")}, "tok-1", nil
		}
		return nil, "", boom
	}

	got, err := ListAllPaged(context.Background(), page)

	require.ErrorIs(t, err, boom)
	assert.Nil(t, got, "a mid-stream error should not return a partial page")
	assert.Equal(t, 2, call, "paging must stop at the failing page, not retry or continue")
}

func TestListAllPagedRestartsOnceAfterExpiredContinuation(t *testing.T) {
	expired := apierrors.NewResourceExpired("private continuation detail")
	var gotOpts []metav1.ListOptions
	call := 0
	page := func(opts metav1.ListOptions) ([]runtime.Object, string, error) {
		gotOpts = append(gotOpts, opts)
		call++
		switch call {
		case 1:
			return []runtime.Object{obj("stale-prefix")}, "old-token", nil
		case 2:
			return nil, "", expired
		case 3:
			return []runtime.Object{obj("fresh-a")}, "new-token", nil
		case 4:
			return []runtime.Object{obj("fresh-b")}, "", nil
		default:
			return nil, "", errors.New("unexpected extra request")
		}
	}

	got, err := ListAllPaged(context.Background(), page)

	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "fresh-a", got[0].(*fakeObj).N)
	assert.Equal(t, "fresh-b", got[1].(*fakeObj).N)
	assert.Equal(t, []string{"", "old-token", "", "new-token"}, []string{
		gotOpts[0].Continue,
		gotOpts[1].Continue,
		gotOpts[2].Continue,
		gotOpts[3].Continue,
	})
	for _, opts := range gotOpts {
		assert.Equal(t, int64(ChunkSize), opts.Limit)
	}
}

func TestListAllPagedExpiredRestartResetsTokenCycleState(t *testing.T) {
	expired := apierrors.NewResourceExpired("expired")
	call := 0
	page := func(opts metav1.ListOptions) ([]runtime.Object, string, error) {
		call++
		switch call {
		case 1:
			return []runtime.Object{obj("discarded")}, "same-token", nil
		case 2:
			return nil, "", expired
		case 3:
			return []runtime.Object{obj("kept-a")}, "same-token", nil
		case 4:
			return []runtime.Object{obj("kept-b")}, "", nil
		default:
			return nil, "", errors.New("unexpected extra request")
		}
	}

	got, err := ListAllPaged(context.Background(), page)

	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, 4, call)
}

func TestListAllPagedReturnsSecondExpiredContinuationWithoutPartialData(t *testing.T) {
	firstExpired := apierrors.NewResourceExpired("first private detail")
	secondExpired := apierrors.NewResourceExpired("second private detail")
	call := 0
	page := func(opts metav1.ListOptions) ([]runtime.Object, string, error) {
		call++
		switch call {
		case 1:
			return []runtime.Object{obj("discarded")}, "old-token", nil
		case 2:
			return nil, "", firstExpired
		case 3:
			return []runtime.Object{obj("also-discarded")}, "new-token", nil
		case 4:
			return nil, "", secondExpired
		default:
			return nil, "", errors.New("unexpected extra request")
		}
	}

	got, err := ListAllPaged(context.Background(), page)

	require.ErrorIs(t, err, secondExpired)
	assert.Nil(t, got)
	assert.Equal(t, 4, call, "an expired continuation must restart at most once")
}

func TestListAllPagedDoesNotRestartExpiredFirstPage(t *testing.T) {
	expired := apierrors.NewResourceExpired("private detail")
	call := 0
	page := func(opts metav1.ListOptions) ([]runtime.Object, string, error) {
		call++
		assert.Empty(t, opts.Continue)
		return nil, "", expired
	}

	got, err := ListAllPaged(context.Background(), page)

	require.ErrorIs(t, err, expired)
	assert.Nil(t, got)
	assert.Equal(t, 1, call)
}

func TestListAllPagedCancellationStopsExpiredRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	expired := apierrors.NewResourceExpired("private detail")
	call := 0
	page := func(metav1.ListOptions) ([]runtime.Object, string, error) {
		call++
		if call == 1 {
			return []runtime.Object{obj("discarded")}, "old-token", nil
		}
		cancel()
		return nil, "", expired
	}

	got, err := ListAllPaged(ctx, page)

	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, got)
	assert.Equal(t, 2, call, "cancellation must prevent the restart request")
}

func TestListAllPagedEmptyResultStopsAfterOneCall(t *testing.T) {
	call := 0
	page := func(opts metav1.ListOptions) ([]runtime.Object, string, error) {
		call++
		return nil, "", nil
	}

	got, err := ListAllPaged(context.Background(), page)

	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Equal(t, 1, call, "an empty first page with no continue token must not be re-fetched")
}

func TestListAllPagedRejectsContinueTokenCycle(t *testing.T) {
	responses := []struct {
		items         []runtime.Object
		continueToken string
	}{
		{items: []runtime.Object{obj("first")}, continueToken: "secret-token-a"},
		{items: []runtime.Object{obj("second")}, continueToken: "secret-token-b"},
		{items: []runtime.Object{obj("cycle-closing")}, continueToken: "secret-token-a"},
	}
	calls := 0
	page := func(metav1.ListOptions) ([]runtime.Object, string, error) {
		if calls >= len(responses) {
			return nil, "", errors.New("fixture exhausted without detecting token cycle")
		}
		response := responses[calls]
		calls++
		return response.items, response.continueToken, nil
	}

	got, err := ListAllPaged(context.Background(), page)

	require.EqualError(t, err, "continue token cycle detected after page 3")
	assert.Nil(t, got, "a cycle must preserve ListAllPaged's all-or-nothing contract")
	assert.Equal(t, 3, calls)
	assert.NotContains(t, err.Error(), "secret-token-a")
	assert.NotContains(t, err.Error(), "secret-token-b")
}

func TestListAllPagedRejectsImmediateContinueTokenCycle(t *testing.T) {
	calls := 0
	page := func(metav1.ListOptions) ([]runtime.Object, string, error) {
		calls++
		if calls > 2 {
			return nil, "", errors.New("fixture exhausted without detecting token cycle")
		}
		return []runtime.Object{obj("item")}, "secret-token-a", nil
	}

	got, err := ListAllPaged(context.Background(), page)

	require.EqualError(t, err, "continue token cycle detected after page 2")
	assert.Nil(t, got, "a cycle must preserve ListAllPaged's all-or-nothing contract")
	assert.Equal(t, 2, calls)
	assert.NotContains(t, err.Error(), "secret-token-a")
}

func TestChunkSizeMatchesKubectlDefault(t *testing.T) {
	// Pin the documented contract (500-item chunks, kubectl parity) so a
	// change here is a deliberate, reviewed decision rather than an
	// accidental drift.
	assert.EqualValues(t, 500, ChunkSize)
}
