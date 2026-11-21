package workloadcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The API serves LIST/WATCH but rejects individual GETs. Informer reads must
// still find shared runtimes and services without placement ownership labels.
func TestMemberInformersReadAndObserveChanges(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("runtimeDenied=%t", blocked), func(t *testing.T) {

			resources := []metav1.APIResource{
				{Name: "inferenceservices", Kind: "InferenceService", Namespaced: true, Verbs: []string{"get", "list", "watch"}},
				{Name: "servingruntimes", Kind: "ServingRuntime", Namespaced: true, Verbs: []string{"get", "list", "watch"}},
				{Name: "clusterservingruntimes", Kind: "ClusterServingRuntime", Verbs: []string{"get", "list", "watch"}},
			}
			s := fakeclient.Scheme(t)
			kinds := sets.New[schema.GroupKind]()
			objects := make(map[string]client.Object)
			events := make(map[string]chan watch.Event)
			for _, resource := range resources {
				gvk := v1beta1.SchemeGroupVersion.WithKind(resource.Kind)
				kinds.Insert(gvk.GroupKind())
				obj, err := s.New(gvk)
				require.NoError(t, err)
				member := obj.(client.Object)
				member.GetObjectKind().SetGroupVersionKind(gvk)
				member.SetName("shared")
				member.SetResourceVersion("1")
				if resource.Namespaced {
					member.SetNamespace("team-a")
				}
				objects[resource.Name] = member
				events[resource.Name] = make(chan watch.Event, 1)
			}
			var individualGets, watches atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				encoder := json.NewEncoder(w)
				switch r.URL.Path {
				case "/api":
					_ = encoder.Encode(&metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions", APIVersion: "v1"}})
					return
				case "/apis":
					version := metav1.GroupVersionForDiscovery{GroupVersion: "ome.io/v1beta1", Version: "v1beta1"}
					_ = encoder.Encode(&metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"}, Groups: []metav1.APIGroup{{Name: "ome.io", Versions: []metav1.GroupVersionForDiscovery{version}, PreferredVersion: version}}})
					return
				case "/apis/ome.io/v1beta1":
					_ = encoder.Encode(&metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "ome.io/v1beta1", APIResources: resources})
					return
				}
				// Only ome.io API paths can read ome.io objects. Any local process can
				// dial this ephemeral port, so requests to other paths are not counted.
				resource, omeAPI := strings.CutPrefix(r.URL.Path, "/apis/ome.io/v1beta1/")
				if !omeAPI {
					http.NotFound(w, r)
					return
				}
				obj, ok := objects[resource]
				if !ok {
					individualGets.Add(1)
					http.Error(w, "individual GET is unavailable", http.StatusServiceUnavailable)
					return
				}
				if blocked && resource == "servingruntimes" {
					http.Error(w, "runtime watch denied", http.StatusForbidden)
					return
				}
				if r.URL.Query().Get("labelSelector") != "" {
					t.Error("shared objects must not be excluded by an ownership selector")
				}
				if r.URL.Query().Get("watch") != "true" {
					_ = encoder.Encode(map[string]any{"apiVersion": "ome.io/v1beta1", "kind": obj.GetObjectKind().GroupVersionKind().Kind + "List", "metadata": map[string]string{"resourceVersion": "1"}, "items": []client.Object{obj}})
					return
				}
				watches.Add(1)
				if r.URL.Query().Get("sendInitialEvents") == "true" {
					_ = encoder.Encode(watchEvent(watch.Added, obj))
					bookmark := obj.DeepCopyObject().(client.Object)
					bookmark.SetAnnotations(map[string]string{"k8s.io/initial-events-end": "true"})
					_ = encoder.Encode(watchEvent(watch.Bookmark, bookmark))
				}
				w.(http.Flusher).Flush()
				for {
					select {
					case <-r.Context().Done():
						return
					case event := <-events[resource]:
						if err := encoder.Encode(watchEvent(event.Type, event.Object)); err != nil {
							return
						}
						w.(http.Flusher).Flush()
					}
				}
			}))
			t.Cleanup(server.Close)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			config := &rest.Config{Host: server.URL, Timeout: 250 * time.Millisecond}
			apiClient, err := client.NewWithWatch(config, client.Options{Scheme: s})
			require.NoError(t, err)
			direct, directReads := recordingClient(t, apiClient)
			syncTimeout := 10 * time.Second
			if blocked {
				syncTimeout = time.Second
			}
			c, err := NewSelectivelyCachingClient(ctx, config, direct, s, kinds, nil, nil, syncTimeout)
			if blocked {
				require.ErrorContains(t, err, "synchronizing remote cache")
				require.Nil(t, c, "one unavailable kind must prevent connection publication")
				return
			}
			require.NoError(t, err)

			// A watch must survive the configured deadline for ordinary requests.
			time.Sleep(3 * config.Timeout)
			require.Equal(t, int32(len(resources)), watches.Load())
			for resource, obj := range objects {
				got := obj.DeepCopyObject().(client.Object)
				require.Eventually(t, func() bool { return c.Get(t.Context(), client.ObjectKeyFromObject(obj), got) == nil }, 5*time.Second, time.Millisecond)
				require.Equal(t, "1", got.GetResourceVersion())
				list, err := s.New(obj.GetObjectKind().GroupVersionKind().GroupVersion().WithKind(obj.GetObjectKind().GroupVersionKind().Kind + "List"))
				require.NoError(t, err)
				require.NoError(t, c.List(t.Context(), list.(client.ObjectList)))

				updated := obj.DeepCopyObject().(client.Object)
				updated.SetResourceVersion("2")
				updated.SetLabels(map[string]string{"revision": "new"})
				events[resource] <- watch.Event{Type: watch.Modified, Object: updated}
				require.Eventually(t, func() bool {
					return c.Get(t.Context(), client.ObjectKeyFromObject(obj), got) == nil && got.GetLabels()["revision"] == "new"
				}, 5*time.Second, time.Millisecond)
				events[resource] <- watch.Event{Type: watch.Deleted, Object: updated}
				require.Eventually(t, func() bool {
					return apierrors.IsNotFound(c.Get(t.Context(), client.ObjectKeyFromObject(obj), got))
				}, 5*time.Second, time.Millisecond)
			}
			cancel()
			require.ErrorIs(t, c.Get(t.Context(), client.ObjectKey{}, &v1beta1.InferenceService{}), ErrMemberCacheUnavailable)
			require.Zero(t, individualGets.Load())
			require.Zero(t, directReads.gets.Count()+directReads.lists.Count())

		})
	}
}

func watchEvent(eventType watch.EventType, object runtime.Object) map[string]any {
	return map[string]any{"type": eventType, "object": object}
}
