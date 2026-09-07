package inferenceservicecollection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	ometyped "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

var testLimits = paging.Limits{
	PageSize:       2,
	MaxItems:       10,
	MaxPages:       5,
	RequestTimeout: time.Second,
}

func TestCollectDrainsInferenceServicesAcrossAllNamespaces(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	var requests []metav1.ListOptions
	var namespaces []string
	client.PrependReactor("list", "inferenceservices", func(action ktesting.Action) (bool, runtime.Object, error) {
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		requests = append(requests, options)
		namespaces = append(namespaces, action.GetNamespace())
		if options.Continue == "" {
			return true, &omev1beta1.InferenceServiceList{
				Items:    []omev1beta1.InferenceService{inferenceService("team-b", "service-b")},
				ListMeta: metav1.ListMeta{Continue: "next"},
			}, nil
		}
		require.Equal(t, "next", options.Continue)
		return true, &omev1beta1.InferenceServiceList{
			Items: []omev1beta1.InferenceService{inferenceService("team-a", "service-a")},
		}, nil
	})

	got, err := Collect(context.Background(), client.OmeV1beta1(), testLimits)

	require.NoError(t, err)
	assert.Equal(t, []string{"team-b/service-b", "team-a/service-a"}, []string{
		got.InferenceServices[0].Namespace + "/" + got.InferenceServices[0].Name,
		got.InferenceServices[1].Namespace + "/" + got.InferenceServices[1].Name,
	})
	assert.Equal(t, Completeness{ObservedPages: 2, ObservedItems: 2}, got.Completeness)
	assert.Equal(t, []metav1.ListOptions{{Limit: 2}, {Limit: 2, Continue: "next"}}, requests)
	assert.Equal(t, []string{metav1.NamespaceAll, metav1.NamespaceAll}, namespaces)
}

func TestCollectInNamespaceNeverUsesAClusterWideList(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	var namespaces []string
	client.PrependReactor("list", "inferenceservices", func(action ktesting.Action) (bool, runtime.Object, error) {
		namespaces = append(namespaces, action.GetNamespace())
		if action.GetNamespace() != "team-a" {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"},
				"", errors.New("cluster-wide list denied"),
			)
		}
		return true, &omev1beta1.InferenceServiceList{
			Items: []omev1beta1.InferenceService{inferenceService("team-a", "service-a")},
		}, nil
	})

	got, err := CollectInNamespace(
		context.Background(), client.OmeV1beta1(), "team-a", testLimits,
	)

	require.NoError(t, err)
	require.Len(t, got.InferenceServices, 1)
	assert.Equal(t, "team-a/service-a",
		got.InferenceServices[0].Namespace+"/"+got.InferenceServices[0].Name)
	assert.Equal(t, []string{"team-a"}, namespaces)
}

func TestCollectReturnsDefensiveCopies(t *testing.T) {
	t.Parallel()

	source := inferenceService("team-a", "service")
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &omev1beta1.InferenceServiceList{
			Items: []omev1beta1.InferenceService{source},
		}, nil
	})

	got, err := Collect(context.Background(), client.OmeV1beta1(), testLimits)
	require.NoError(t, err)

	got.InferenceServices[0].Annotations["source"] = "output-mutated"
	got.InferenceServices[0].Spec.Runtime.Name = "output-mutated"
	assert.Equal(t, "fixture", source.Annotations["source"])
	assert.Equal(t, "runtime", source.Spec.Runtime.Name)

	source.Annotations["source"] = "source-mutated"
	source.Spec.Runtime.Name = "source-mutated"
	assert.Equal(t, "output-mutated", got.InferenceServices[0].Annotations["source"])
	assert.Equal(t, "output-mutated", got.InferenceServices[0].Spec.Runtime.Name)
}

func TestCollectReportsItemAndPageTruncation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limits     paging.Limits
		response   *omev1beta1.InferenceServiceList
		wantItems  int
		wantPages  int
		wantLimits []int64
	}{
		{
			name: "item limit",
			limits: paging.Limits{
				PageSize: 2, MaxItems: 3, MaxPages: 5, RequestTimeout: time.Second,
			},
			response: &omev1beta1.InferenceServiceList{
				Items: []omev1beta1.InferenceService{
					inferenceService("team-a", "one"),
					inferenceService("team-a", "two"),
				},
				ListMeta: metav1.ListMeta{Continue: "more"},
			},
			wantItems: 3, wantPages: 2, wantLimits: []int64{2, 1},
		},
		{
			name: "page limit",
			limits: paging.Limits{
				PageSize: 1, MaxItems: 5, MaxPages: 2, RequestTimeout: time.Second,
			},
			response: &omev1beta1.InferenceServiceList{
				Items:    []omev1beta1.InferenceService{inferenceService("team-a", "one")},
				ListMeta: metav1.ListMeta{Continue: "more"},
			},
			wantItems: 2, wantPages: 2, wantLimits: []int64{1, 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := omefake.NewSimpleClientset()
			var limits []int64
			client.PrependReactor("list", "inferenceservices", func(action ktesting.Action) (bool, runtime.Object, error) {
				options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
				limits = append(limits, options.Limit)
				response := test.response.DeepCopy()
				if int(options.Limit) < len(response.Items) {
					response.Items = response.Items[:options.Limit]
				}
				response.Continue += options.Continue + "n"
				return true, response, nil
			})

			got, err := Collect(context.Background(), client.OmeV1beta1(), test.limits)

			require.NoError(t, err)
			assert.Len(t, got.InferenceServices, test.wantItems)
			assert.Equal(t, Completeness{
				ObservedPages: test.wantPages, ObservedItems: test.wantItems, Truncated: true,
			}, got.Completeness)
			assert.Equal(t, test.wantLimits, limits)
		})
	}
}

func TestCollectRejectsSuccessfulResponseAfterCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		cancel()
		return true, &omev1beta1.InferenceServiceList{
			Items: []omev1beta1.InferenceService{inferenceService("team-a", "too-late")},
		}, nil
	})

	got, err := Collect(ctx, client.OmeV1beta1(), testLimits)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, got.InferenceServices)
	assert.Equal(t, Completeness{}, got.Completeness)
}

func TestCollectRejectsSuccessfulResponseAfterRequestTimeout(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		time.Sleep(30 * time.Millisecond)
		return true, &omev1beta1.InferenceServiceList{
			Items: []omev1beta1.InferenceService{inferenceService("team-a", "too-late")},
		}, nil
	})
	limits := testLimits
	limits.RequestTimeout = time.Millisecond

	got, err := Collect(context.Background(), client.OmeV1beta1(), limits)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, got.InferenceServices)
	assert.Equal(t, Completeness{}, got.Completeness)
}

func TestCollectPreservesBoundedProgressAndTypedErrors(t *testing.T) {
	t.Parallel()

	t.Run("non-advancing continue token", func(t *testing.T) {
		t.Parallel()
		client := omefake.NewSimpleClientset()
		client.PrependReactor("list", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, &omev1beta1.InferenceServiceList{
				Items:    []omev1beta1.InferenceService{inferenceService("team-a", "service")},
				ListMeta: metav1.ListMeta{Continue: "stuck"},
			}, nil
		})

		got, err := Collect(context.Background(), client.OmeV1beta1(), testLimits)

		require.ErrorContains(t, err, "continue token did not advance")
		assert.Len(t, got.InferenceServices, 1)
		assert.Equal(t, Completeness{ObservedPages: 2, ObservedItems: 1}, got.Completeness)
	})

	t.Run("forbidden", func(t *testing.T) {
		t.Parallel()
		client := omefake.NewSimpleClientset()
		forbidden := apierrors.NewForbidden(schema.GroupResource{
			Group: "ome.io", Resource: "inferenceservices",
		}, "", errors.New("denied"))
		client.PrependReactor("list", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, forbidden
		})

		got, err := Collect(context.Background(), client.OmeV1beta1(), testLimits)

		require.Error(t, err)
		assert.True(t, apierrors.IsForbidden(err))
		assert.ErrorIs(t, err, forbidden)
		assert.Empty(t, got.InferenceServices)
		assert.Equal(t, Completeness{}, got.Completeness)
	})

	t.Run("nil response", func(t *testing.T) {
		t.Parallel()

		got, err := Collect(context.Background(), nilListClient{}, testLimits)

		require.ErrorContains(t, err, "empty response")
		assert.Empty(t, got.InferenceServices)
		assert.Equal(t, Completeness{}, got.Completeness)
	})
}

type nilListClient struct {
	ometyped.OmeV1beta1Interface
}

func (nilListClient) InferenceServices(string) ometyped.InferenceServiceInterface {
	return nilListInferenceServices{}
}

type nilListInferenceServices struct {
	ometyped.InferenceServiceInterface
}

func (nilListInferenceServices) List(
	context.Context,
	metav1.ListOptions,
) (*omev1beta1.InferenceServiceList, error) {
	return nil, nil
}

func TestCollectRejectsInvalidLimitsWithoutListing(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	called := false
	client.PrependReactor("list", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		called = true
		return true, &omev1beta1.InferenceServiceList{}, nil
	})

	got, err := Collect(context.Background(), client.OmeV1beta1(), paging.Limits{})

	require.Error(t, err)
	assert.False(t, called)
	assert.Empty(t, got.InferenceServices)
	assert.Equal(t, Completeness{}, got.Completeness)
}

func inferenceService(namespace, name string) omev1beta1.InferenceService {
	return omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Annotations: map[string]string{
				"source": "fixture",
			},
		},
		Spec: omev1beta1.InferenceServiceSpec{
			Runtime: &omev1beta1.ServingRuntimeRef{Name: "runtime"},
		},
	}
}
