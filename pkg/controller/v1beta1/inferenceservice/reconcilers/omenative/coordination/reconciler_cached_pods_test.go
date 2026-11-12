package coordination

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// TestObservePerRevisionPods_AsksTheCacheForItsOwnPods holds the cached
// observation to the request the manager's cache answers without a copy,
// and the live observation, the canary's, to a plain list.
func TestObservePerRevisionPods_AsksTheCacheForItsOwnPods(t *testing.T) {
	isvc := testOMENativeISVC()
	engine := v1beta1.EngineComponent
	lists := fakeclient.CountingVerb(fakeclient.List).OnKind(&corev1.PodList{})
	c := fakeclient.New(t, lists,
		fakeclient.WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor),
		fakeclient.WithObjects(instancePod(isvc, engine, "v1", 0, 0)))
	ctx := context.Background()
	total, _, _, observed, err := observePerRevisionPods(ctx, c, isvc, []v1beta1.ComponentType{engine}, true)
	if err != nil {
		t.Fatalf("observe through the cache: %v", err)
	}
	if total[engine]["v1"] != 1 || len(observed[engine]) != 1 {
		t.Fatalf("the cached observation saw %v and %d pods, want one pod on v1", total, len(observed[engine]))
	}
	if _, _, _, _, err := observePerRevisionPods(ctx, c, isvc, []v1beta1.ComponentType{engine}, false); err != nil {
		t.Fatalf("observe through the live reader: %v", err)
	}
	options := fakeclient.OptionsOf[client.ListOptions](lists)
	if len(options) != 2 {
		t.Fatalf("recorded %d pod lists, want 2", len(options))
	}
	if cached := options[0]; cached.UnsafeDisableDeepCopy == nil || !*cached.UnsafeDisableDeepCopy {
		t.Errorf("the cached observation asked for copies; want the cache's own pods (UnsafeDisableDeepCopy)")
	}
	if live := options[1]; live.UnsafeDisableDeepCopy != nil {
		t.Errorf("the live observation set the deep-copy switch to %v; want a plain list", *live.UnsafeDisableDeepCopy)
	}
}

// TestCoordinationPassCopiesNoPod runs the pass over the prefill/decode
// service at rest: the cache hands out every pod of its three Components
// and copies none of them, and the pass leaves the service and the pods as
// they were.
func TestCoordinationPassCopiesNoPod(t *testing.T) {
	shapes := passFleetShapes()
	f := buildPassFleet(t, shapes[len(shapes)-1])
	f.pass(t)
	if f.cache.podsListed != 44 || f.cache.podsCopied != 0 {
		t.Fatalf("the pass listed %d pods and the cache copied %d; want 44 listed and none copied", f.cache.podsListed, f.cache.podsCopied)
	}
	f.checkAtRest(t)
}
