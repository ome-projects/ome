package query

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// TestListCachedOMENativePodsByName_AsksTheCacheForItsOwnPods holds the
// cached list to the request the manager's cache answers without a copy:
// the namespace, the Component's index value, and the deep copy switched
// off. The plain list over the same reader leaves the switch unset.
func TestListCachedOMENativePodsByName_AsksTheCacheForItsOwnPods(t *testing.T) {
	lists := fakeclient.CountingVerb(fakeclient.List).OnKind(&corev1.PodList{})
	reader := fakeclient.New(t, lists,
		fakeclient.WithIndex(&corev1.Pod{}, OMENativePodIndexField, OMENativePodIndexExtractor),
		fakeclient.WithObjects(
			newDiscoverPodFor("eng-0", "isvc", workload.ComponentEngine),
			newDiscoverPodFor("dec-0", "isvc", workload.ComponentDecoder)))
	got, err := ListCachedOMENativePodsByName(context.Background(), reader, "ns", "isvc", workload.ComponentEngine)
	if err != nil {
		t.Fatalf("ListCachedOMENativePodsByName: %v", err)
	}
	assertPodSet(t, "cached", got, map[string]bool{"eng-0": true})
	if _, err := ListOMENativePodsByName(context.Background(), reader, "ns", "isvc", workload.ComponentEngine, true); err != nil {
		t.Fatalf("ListOMENativePodsByName: %v", err)
	}
	options := fakeclient.OptionsOf[client.ListOptions](lists)
	if len(options) != 2 {
		t.Fatalf("recorded %d pod lists, want 2", len(options))
	}
	cached, plain := options[0], options[1]
	if cached.Namespace != "ns" {
		t.Errorf("the cached list asked namespace %q, want ns", cached.Namespace)
	}
	want := OMENativePodIndexValue("isvc", workload.ComponentEngine)
	if value, ok := cached.FieldSelector.RequiresExactMatch(OMENativePodIndexField); !ok || value != want {
		t.Errorf("the cached list asked the field selector %v, want %s=%s", cached.FieldSelector, OMENativePodIndexField, want)
	}
	if cached.UnsafeDisableDeepCopy == nil || !*cached.UnsafeDisableDeepCopy {
		t.Errorf("the cached list asked for copies; want the cache's own pods (UnsafeDisableDeepCopy)")
	}
	if plain.UnsafeDisableDeepCopy != nil {
		t.Errorf("the plain list set the deep-copy switch to %v; want it unset", *plain.UnsafeDisableDeepCopy)
	}
}

// TestListCachedOMENativePodsByName_HandsOutTheHeldPods holds the list over
// a cache stand-in to no copy at all: the pods handed back carry the held
// pods' own labels. The stand-in has no index, so the list takes the label
// path, which asks for the cache's own pods as the index path does.
func TestListCachedOMENativePodsByName_HandsOutTheHeldPods(t *testing.T) {
	held := newDiscoverPodFor("eng-0", "isvc", workload.ComponentEngine)
	reader := fakeclient.ReadFromMemory(held)
	got, err := ListCachedOMENativePodsByName(context.Background(), reader, "ns", "isvc", workload.ComponentEngine)
	if err != nil {
		t.Fatalf("ListCachedOMENativePodsByName: %v", err)
	}
	if reader.Copies != 0 {
		t.Errorf("the cache copied %d pods for the list, want none", reader.Copies)
	}
	if len(got) != 1 || reflect.ValueOf(got[0].Labels).Pointer() != reflect.ValueOf(held.Labels).Pointer() {
		t.Errorf("the list handed back a copy of %s; want the held pod's own labels", held.Name)
	}
}
