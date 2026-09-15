package quotacollection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

var collectionTestLimits = paging.Limits{
	PageSize:       2,
	MaxItems:       10,
	MaxPages:       5,
	RequestTimeout: time.Second,
}

// Removing Kubernetes continuation-token handling makes this fail by losing
// the second quota and by changing the exact bounded request sequence.
func TestCollectDrainsBoundedPagesAndReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	requests := []metav1.ListOptions{}
	first := collectionQuota("root")
	second := collectionQuota("team-a")
	client.PrependReactor("list", "acceleratorquotas", func(action ktesting.Action) (bool, runtime.Object, error) {
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		requests = append(requests, options)
		if options.Continue == "" {
			return true, &omev1beta1.AcceleratorQuotaList{
				Items:    []omev1beta1.AcceleratorQuota{first},
				ListMeta: metav1.ListMeta{Continue: "page-2"},
			}, nil
		}
		require.Equal(t, "page-2", options.Continue)
		return true, &omev1beta1.AcceleratorQuotaList{
			Items: []omev1beta1.AcceleratorQuota{second},
		}, nil
	})

	got, err := Collect(context.Background(), client.OmeV1beta1().AcceleratorQuotas(), collectionTestLimits)

	require.NoError(t, err)
	assert.Equal(t, Completeness{ObservedPages: 2, ObservedItems: 2}, got.Completeness)
	require.Len(t, got.Quotas, 2)
	assert.Equal(t, []string{"root", "team-a"}, []string{got.Quotas[0].Name, got.Quotas[1].Name})
	assert.Equal(t, []metav1.ListOptions{{Limit: 2}, {Limit: 2, Continue: "page-2"}}, requests)

	got.Quotas[0].Annotations["canary"] = "output-mutated"
	first.Annotations["canary"] = "source-mutated"
	assert.Equal(t, "output-mutated", got.Quotas[0].Annotations["canary"])
	assert.Equal(t, "original", second.Annotations["canary"])
}

// Removing the total-page guard makes this fail because an incomplete tree
// would be presented as complete structural evidence.
func TestCollectMarksPageLimitedSnapshotsTruncated(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &omev1beta1.AcceleratorQuotaList{
			Items:    []omev1beta1.AcceleratorQuota{collectionQuota("root")},
			ListMeta: metav1.ListMeta{Continue: "more"},
		}, nil
	})
	limits := collectionTestLimits
	limits.MaxPages = 1

	got, err := Collect(context.Background(), client.OmeV1beta1().AcceleratorQuotas(), limits)

	require.NoError(t, err)
	assert.Equal(t, Completeness{ObservedPages: 1, ObservedItems: 1, Truncated: true}, got.Completeness)
}

// Accepting an API result after its request context expired makes this fail.
func TestCollectRejectsLateSuccessAfterRequestTimeout(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) {
		time.Sleep(30 * time.Millisecond)
		return true, &omev1beta1.AcceleratorQuotaList{
			Items: []omev1beta1.AcceleratorQuota{collectionQuota("too-late")},
		}, nil
	})
	limits := collectionTestLimits
	limits.RequestTimeout = 5 * time.Millisecond

	got, err := Collect(context.Background(), client.OmeV1beta1().AcceleratorQuotas(), limits)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, got.Quotas)
	assert.Equal(t, Completeness{}, got.Completeness)
}

// Dropping upstream cancellation checks makes this fail by returning a stale
// successful snapshot after the caller has stopped waiting.
func TestCollectRejectsSuccessAfterCallerCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) {
		cancel()
		return true, &omev1beta1.AcceleratorQuotaList{
			Items: []omev1beta1.AcceleratorQuota{collectionQuota("too-late")},
		}, nil
	})

	got, err := Collect(ctx, client.OmeV1beta1().AcceleratorQuotas(), collectionTestLimits)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, got.Quotas)
	assert.Equal(t, Completeness{}, got.Completeness)
}

// Treating nil responses or client/configuration failures as empty trees makes
// these cases fail loudly instead of manufacturing RootMissing evidence.
func TestCollectRejectsInvalidDependenciesAndResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		limits paging.Limits
		setup  func(*omefake.Clientset)
		nilAPI bool
		want   error
	}{
		{name: "nil client", limits: collectionTestLimits, nilAPI: true, want: ErrClientRequired},
		{name: "bad limits", limits: paging.Limits{}, want: ErrInvalidLimits},
		{name: "nil response", limits: collectionTestLimits, setup: func(client *omefake.Clientset) {
			client.PrependReactor("list", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, nil
			})
		}, want: ErrEmptyResponse},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset()
			if test.setup != nil {
				test.setup(client)
			}
			api := client.OmeV1beta1().AcceleratorQuotas()
			if test.name == "nil response" {
				api = nilListClient{}
			}
			if test.nilAPI {
				api = nil
			}

			got, err := Collect(context.Background(), api, test.limits)

			assert.ErrorIs(t, err, test.want)
			assert.Empty(t, got.Quotas)
		})
	}
}

type nilListClient struct {
	omeclient.AcceleratorQuotaInterface
}

func (nilListClient) List(context.Context, metav1.ListOptions) (*omev1beta1.AcceleratorQuotaList, error) {
	return nil, nil
}

// Swallowing the server error makes this fail and would mislabel unavailable
// required evidence as a structurally empty tree.
func TestCollectPreservesListFailure(t *testing.T) {
	t.Parallel()

	boom := errors.New("list failed")
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, boom
	})

	got, err := Collect(context.Background(), client.OmeV1beta1().AcceleratorQuotas(), collectionTestLimits)

	assert.ErrorIs(t, err, boom)
	assert.Empty(t, got.Quotas)
}

func collectionQuota(name string) omev1beta1.AcceleratorQuota {
	return omev1beta1.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{
		Name: name, Generation: 1, Annotations: map[string]string{"canary": "original"},
	}}
}

func TestItemCutoffAndCanceledCaller(t *testing.T) {
	for _, tt := range []struct {
		name      string
		count     int
		more      string
		truncated bool
	}{
		{"exact end", 2, "", false}, {"exact with more", 2, "next", true}, {"overfilled", 3, "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := omefake.NewSimpleClientset()
			client.PrependReactor("list", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, &omev1beta1.AcceleratorQuotaList{Items: make([]omev1beta1.AcceleratorQuota, tt.count), ListMeta: metav1.ListMeta{Continue: tt.more}}, nil
			})
			limits := collectionTestLimits
			limits.MaxItems = 2
			got, err := Collect(context.Background(), client.OmeV1beta1().AcceleratorQuotas(), limits)
			require.NoError(t, err)
			assert.Equal(t, Completeness{ObservedPages: 1, ObservedItems: 2, Truncated: tt.truncated}, got.Completeness)
			assert.Len(t, got.Quotas, 2)
			assert.Len(t, client.Actions(), 1)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := omefake.NewSimpleClientset()
	_, err := Collect(ctx, client.OmeV1beta1().AcceleratorQuotas(), collectionTestLimits)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, client.Actions())
}
