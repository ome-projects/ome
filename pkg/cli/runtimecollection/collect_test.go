package runtimecollection

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

var testLimits = paging.Limits{
	PageSize:       2,
	MaxItems:       10,
	MaxPages:       5,
	RequestTimeout: time.Second,
}

func TestCollectDrainsBothRuntimeKindsAcrossAllNamespaces(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	requests := map[string][]metav1.ListOptions{}
	namespaces := map[string][]string{}
	client.PrependReactor("list", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
		resource := action.GetResource().Resource
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		requests[resource] = append(requests[resource], options)
		namespaces[resource] = append(namespaces[resource], action.GetNamespace())
		switch resource {
		case "clusterservingruntimes":
			if options.Continue == "" {
				return true, &omev1beta1.ClusterServingRuntimeList{
					Items:    []omev1beta1.ClusterServingRuntime{clusterRuntime("cluster-a")},
					ListMeta: metav1.ListMeta{Continue: "cluster-next"},
				}, nil
			}
			require.Equal(t, "cluster-next", options.Continue)
			return true, &omev1beta1.ClusterServingRuntimeList{
				Items: []omev1beta1.ClusterServingRuntime{clusterRuntime("cluster-b")},
			}, nil
		case "servingruntimes":
			if options.Continue == "" {
				return true, &omev1beta1.ServingRuntimeList{
					Items:    []omev1beta1.ServingRuntime{servingRuntime("team-b", "runtime-b")},
					ListMeta: metav1.ListMeta{Continue: "runtime-next"},
				}, nil
			}
			require.Equal(t, "runtime-next", options.Continue)
			return true, &omev1beta1.ServingRuntimeList{
				Items: []omev1beta1.ServingRuntime{servingRuntime("team-a", "runtime-a")},
			}, nil
		default:
			t.Fatalf("unexpected list resource %q", resource)
			return true, nil, nil
		}
	})

	got, err := Collect(context.Background(), client.OmeV1beta1(), testLimits)

	require.NoError(t, err)
	assert.Equal(t, []string{"cluster-a", "cluster-b"}, []string{
		got.Snapshot.ClusterServingRuntimes[0].Name,
		got.Snapshot.ClusterServingRuntimes[1].Name,
	})
	assert.Equal(t, []string{"team-b/runtime-b", "team-a/runtime-a"}, []string{
		got.Snapshot.ServingRuntimes[0].Namespace + "/" + got.Snapshot.ServingRuntimes[0].Name,
		got.Snapshot.ServingRuntimes[1].Namespace + "/" + got.Snapshot.ServingRuntimes[1].Name,
	})
	assert.Equal(t, KindCompleteness{ObservedPages: 2, ObservedItems: 2}, got.Completeness.ClusterServingRuntimes)
	assert.Equal(t, KindCompleteness{ObservedPages: 2, ObservedItems: 2}, got.Completeness.ServingRuntimes)
	for _, resource := range []string{"clusterservingruntimes", "servingruntimes"} {
		require.Equal(t, []metav1.ListOptions{
			{Limit: 2},
			{Limit: 2, Continue: map[string]string{
				"clusterservingruntimes": "cluster-next",
				"servingruntimes":        "runtime-next",
			}[resource]},
		}, requests[resource])
		assert.Equal(t, []string{metav1.NamespaceAll, metav1.NamespaceAll}, namespaces[resource])
	}
}

func TestCollectInNamespaceScopesOnlyServingRuntimes(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	seen := map[string][]string{}
	client.PrependReactor("list", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
		resource := action.GetResource().Resource
		seen[resource] = append(seen[resource], action.GetNamespace())
		switch resource {
		case "clusterservingruntimes":
			return true, &omev1beta1.ClusterServingRuntimeList{
				Items: []omev1beta1.ClusterServingRuntime{clusterRuntime("cluster")},
			}, nil
		case "servingruntimes":
			if action.GetNamespace() != "team-a" {
				return true, nil, apierrors.NewForbidden(
					schema.GroupResource{Group: "ome.io", Resource: resource},
					"", errors.New("cluster-wide list denied"),
				)
			}
			return true, &omev1beta1.ServingRuntimeList{
				Items: []omev1beta1.ServingRuntime{servingRuntime("team-a", "runtime")},
			}, nil
		default:
			t.Fatalf("unexpected list resource %q", resource)
			return true, nil, nil
		}
	})

	got, err := CollectInNamespace(
		context.Background(), client.OmeV1beta1(), "team-a", testLimits,
	)

	require.NoError(t, err)
	assert.Equal(t, []string{""}, seen["clusterservingruntimes"])
	assert.Equal(t, []string{"team-a"}, seen["servingruntimes"])
	require.Len(t, got.Snapshot.ServingRuntimes, 1)
	assert.Equal(t, "team-a", got.Snapshot.ServingRuntimes[0].Namespace)
}

func TestCollectServingRuntimesCanExpandAcrossNamespaces(t *testing.T) {
	t.Parallel()

	runtimeA := servingRuntime("team-a", "runtime-a")
	runtimeB := servingRuntime("team-b", "runtime-b")
	client := omefake.NewSimpleClientset(&runtimeA, &runtimeB)

	got, err := CollectServingRuntimes(
		context.Background(), client.OmeV1beta1(), metav1.NamespaceAll, testLimits,
	)

	require.NoError(t, err)
	assert.Len(t, got.ServingRuntimes, 2)
	assert.Equal(t, KindCompleteness{ObservedPages: 1, ObservedItems: 2}, got.Completeness)
	require.Len(t, client.Actions(), 1)
	assert.Equal(t, "servingruntimes", client.Actions()[0].GetResource().Resource)
	assert.Empty(t, client.Actions()[0].GetNamespace())
}

func TestCollectReturnsDefensiveRuntimeCopies(t *testing.T) {
	t.Parallel()

	clusterSource := clusterRuntime("cluster")
	runtimeSource := servingRuntime("team-a", "runtime")
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &omev1beta1.ClusterServingRuntimeList{Items: []omev1beta1.ClusterServingRuntime{clusterSource}}, nil
	})
	client.PrependReactor("list", "servingruntimes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &omev1beta1.ServingRuntimeList{Items: []omev1beta1.ServingRuntime{runtimeSource}}, nil
	})

	got, err := Collect(context.Background(), client.OmeV1beta1(), testLimits)
	require.NoError(t, err)

	got.Snapshot.ClusterServingRuntimes[0].Annotations["source"] = "output-mutated"
	got.Snapshot.ServingRuntimes[0].Annotations["source"] = "output-mutated"
	assert.Equal(t, "cluster", clusterSource.Annotations["source"])
	assert.Equal(t, "runtime", runtimeSource.Annotations["source"])

	clusterSource.Annotations["source"] = "source-mutated"
	runtimeSource.Annotations["source"] = "source-mutated"
	assert.Equal(t, "output-mutated", got.Snapshot.ClusterServingRuntimes[0].Annotations["source"])
	assert.Equal(t, "output-mutated", got.Snapshot.ServingRuntimes[0].Annotations["source"])
}

func TestCollectDoesNotAcceptSuccessfulResponseAfterCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(ktesting.Action) (bool, runtime.Object, error) {
		cancel()
		return true, &omev1beta1.ClusterServingRuntimeList{
			Items: []omev1beta1.ClusterServingRuntime{clusterRuntime("too-late")},
		}, nil
	})

	got, err := Collect(ctx, client.OmeV1beta1(), testLimits)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, got.Snapshot.ClusterServingRuntimes)
	assert.Zero(t, got.Completeness.ClusterServingRuntimes.ObservedPages)
	assert.Zero(t, got.Completeness.ClusterServingRuntimes.ObservedItems)
	assert.Empty(t, got.Snapshot.ServingRuntimes)
}

func TestCollectRejectsSuccessfulRuntimeListAfterRequestTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
	}{
		{name: "cluster runtime", resource: "clusterservingruntimes"},
		{name: "namespaced runtime", resource: "servingruntimes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := omefake.NewSimpleClientset()
			client.PrependReactor("list", test.resource, func(ktesting.Action) (bool, runtime.Object, error) {
				time.Sleep(50 * time.Millisecond)
				if test.resource == "clusterservingruntimes" {
					return true, &omev1beta1.ClusterServingRuntimeList{
						Items: []omev1beta1.ClusterServingRuntime{clusterRuntime("too-late")},
					}, nil
				}
				return true, &omev1beta1.ServingRuntimeList{
					Items: []omev1beta1.ServingRuntime{servingRuntime("team-a", "too-late")},
				}, nil
			})
			limits := testLimits
			limits.RequestTimeout = 10 * time.Millisecond

			got, err := Collect(context.Background(), client.OmeV1beta1(), limits)

			require.Error(t, err)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			if test.resource == "clusterservingruntimes" {
				assert.Empty(t, got.Snapshot.ClusterServingRuntimes)
				assert.Zero(t, got.Completeness.ClusterServingRuntimes.ObservedPages)
				assert.Zero(t, got.Completeness.ClusterServingRuntimes.ObservedItems)
				assert.Empty(t, got.Snapshot.ServingRuntimes)
				return
			}
			assert.Empty(t, got.Snapshot.ServingRuntimes)
			assert.Zero(t, got.Completeness.ServingRuntimes.ObservedPages)
			assert.Zero(t, got.Completeness.ServingRuntimes.ObservedItems)
		})
	}
}

func TestCollectReportsIndependentKindTruncation(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &omev1beta1.ClusterServingRuntimeList{
			Items:    []omev1beta1.ClusterServingRuntime{clusterRuntime("cluster")},
			ListMeta: metav1.ListMeta{Continue: "more-clusters"},
		}, nil
	})
	client.PrependReactor("list", "servingruntimes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &omev1beta1.ServingRuntimeList{
			Items: []omev1beta1.ServingRuntime{servingRuntime("team-a", "runtime")},
		}, nil
	})
	limits := testLimits
	limits.MaxPages = 1

	got, err := Collect(context.Background(), client.OmeV1beta1(), limits)

	require.NoError(t, err)
	assert.Equal(t, KindCompleteness{ObservedPages: 1, ObservedItems: 1, Truncated: true}, got.Completeness.ClusterServingRuntimes)
	assert.Equal(t, KindCompleteness{ObservedPages: 1, ObservedItems: 1}, got.Completeness.ServingRuntimes)
}

func TestCollectEnforcesItemAndPageLimitsForEachKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limits     paging.Limits
		items      int
		pages      int
		wantLimits []int64
	}{
		{
			name: "items",
			limits: paging.Limits{
				PageSize: 2, MaxItems: 3, MaxPages: 5, RequestTimeout: time.Second,
			},
			items:      3,
			pages:      2,
			wantLimits: []int64{2, 1},
		},
		{
			name: "pages",
			limits: paging.Limits{
				PageSize: 1, MaxItems: 5, MaxPages: 2, RequestTimeout: time.Second,
			},
			items:      2,
			pages:      2,
			wantLimits: []int64{1, 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := omefake.NewSimpleClientset()
			limitsByResource := map[string][]int64{}
			client.PrependReactor("list", "*", func(action ktesting.Action) (bool, runtime.Object, error) {
				resource := action.GetResource().Resource
				options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
				limitsByResource[resource] = append(limitsByResource[resource], options.Limit)
				continueToken := options.Continue + "n"
				if resource == "clusterservingruntimes" {
					items := []omev1beta1.ClusterServingRuntime{clusterRuntime("cluster-a"), clusterRuntime("cluster-b")}
					return true, &omev1beta1.ClusterServingRuntimeList{
						Items: items[:int(options.Limit)], ListMeta: metav1.ListMeta{Continue: continueToken},
					}, nil
				}
				items := []omev1beta1.ServingRuntime{
					servingRuntime("team-a", "runtime-a"), servingRuntime("team-b", "runtime-b"),
				}
				return true, &omev1beta1.ServingRuntimeList{
					Items: items[:int(options.Limit)], ListMeta: metav1.ListMeta{Continue: continueToken},
				}, nil
			})

			got, err := Collect(context.Background(), client.OmeV1beta1(), test.limits)

			require.NoError(t, err)
			for _, completeness := range []KindCompleteness{
				got.Completeness.ClusterServingRuntimes, got.Completeness.ServingRuntimes,
			} {
				assert.Equal(t, test.items, completeness.ObservedItems)
				assert.Equal(t, test.pages, completeness.ObservedPages)
				assert.True(t, completeness.Truncated)
			}
			assert.Len(t, got.Snapshot.ClusterServingRuntimes, test.items)
			assert.Len(t, got.Snapshot.ServingRuntimes, test.items)
			assert.Equal(t, test.wantLimits, limitsByResource["clusterservingruntimes"])
			assert.Equal(t, test.wantLimits, limitsByResource["servingruntimes"])
		})
	}
}

func TestCollectRejectsNonAdvancingContinueToken(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	client.PrependReactor("list", "clusterservingruntimes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &omev1beta1.ClusterServingRuntimeList{
			Items:    []omev1beta1.ClusterServingRuntime{clusterRuntime("cluster")},
			ListMeta: metav1.ListMeta{Continue: "stuck"},
		}, nil
	})

	got, err := Collect(context.Background(), client.OmeV1beta1(), testLimits)

	require.ErrorContains(t, err, "continue token did not advance")
	assert.Equal(t, KindCompleteness{ObservedPages: 2, ObservedItems: 1}, got.Completeness.ClusterServingRuntimes)
	assert.Len(t, got.Snapshot.ClusterServingRuntimes, 1)
	assert.Empty(t, got.Snapshot.ServingRuntimes)
}

func TestCollectPreservesTypedRequiredListErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
	}{
		{name: "cluster runtime", resource: "clusterservingruntimes"},
		{name: "namespaced runtime", resource: "servingruntimes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := omefake.NewSimpleClientset()
			forbidden := apierrors.NewForbidden(schema.GroupResource{
				Group: "ome.io", Resource: test.resource,
			}, "", errors.New("denied"))
			client.PrependReactor("list", test.resource, func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, forbidden
			})

			got, err := Collect(context.Background(), client.OmeV1beta1(), testLimits)

			require.Error(t, err)
			var collectionErr *CollectionError
			require.ErrorAs(t, err, &collectionErr)
			wantKind := CollectionClusterServingRuntime
			if test.resource == "servingruntimes" {
				wantKind = CollectionServingRuntime
			}
			assert.Equal(t, wantKind, collectionErr.Kind)
			assert.True(t, apierrors.IsForbidden(err))
			assert.ErrorIs(t, err, forbidden)
			if test.resource == "servingruntimes" {
				assert.Equal(t, KindCompleteness{ObservedPages: 1}, got.Completeness.ClusterServingRuntimes)
			}
			assert.Zero(t, got.Completeness.ServingRuntimes.ObservedPages)
		})
	}
}

func TestCollectUsesFiniteRequestsWithPerRequestTimeouts(t *testing.T) {
	t.Parallel()

	const requestTimeout = 3 * time.Second
	seen := 0
	config := &rest.Config{Host: "https://ome.test"}
	client, err := omeclient.NewForConfigAndClient(
		config,
		&http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			seen++
			deadline, ok := request.Context().Deadline()
			require.True(t, ok)
			remaining := time.Until(deadline)
			assert.Positive(t, remaining)
			assert.LessOrEqual(t, remaining, requestTimeout)
			assert.Equal(t, "4", request.URL.Query().Get("limit"))
			resource := strings.TrimPrefix(request.URL.Path, "/apis/ome.io/v1beta1/")
			body := `{"apiVersion":"ome.io/v1beta1","kind":"` + map[string]string{
				"clusterservingruntimes": "ClusterServingRuntimeList",
				"servingruntimes":        "ServingRuntimeList",
			}[resource] + `","items":[]}`
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    request,
			}, nil
		})},
	)
	require.NoError(t, err)
	limits := paging.Limits{PageSize: 4, MaxItems: 8, MaxPages: 2, RequestTimeout: requestTimeout}

	_, err = Collect(context.Background(), client.OmeV1beta1(), limits)

	require.NoError(t, err)
	assert.Equal(t, 2, seen)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func clusterRuntime(name string) omev1beta1.ClusterServingRuntime {
	return omev1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Name: name, Annotations: map[string]string{"source": "cluster"},
	}}
}

func servingRuntime(namespace, name string) omev1beta1.ServingRuntime {
	return omev1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace, Name: name, Annotations: map[string]string{"source": "runtime"},
	}}
}
