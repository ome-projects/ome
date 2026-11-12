package coordination

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ObservePerRevisionPods is the exported entry point to the per-revision pod-count
// observation the coordination layer uses, so sibling rollout executors (e.g.
// canary) can reuse the exact same accounting instead of re-implementing it.
// Returns maps keyed by component → revision hash: total pods, the READY serving
// subset, observed routing selector capabilities, and the Pods behind those
// aggregates. Capacity gates use the ready map; Service producers use routing.
//
// useIndex MUST match the reader (see observePerRevisionPods): the canary capacity
// gate passes the LIVE API reader, which has no Pod field index, so it MUST pass
// useIndex=false.
//
// With the cached client and useIndex=true the pods handed back are the
// informer cache's own objects: read them during the pass, write nothing
// through them, keep none past the pass, and copy one before changing it.
// A live reader hands back a plain list.
func ObservePerRevisionPods(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, components []v1beta1.ComponentType, useIndex bool) (total, ready map[v1beta1.ComponentType]map[string]int32, routing map[v1beta1.ComponentType]map[string]RevisionRoutingSelector, pods map[v1beta1.ComponentType][]*corev1.Pod, err error) {
	return observePerRevisionPods(ctx, reads, isvc, components, useIndex)
}

// PodReadyAndServing reports whether a Pod is eligible for the ready subset
// returned by ObservePerRevisionPods.
func PodReadyAndServing(pod *corev1.Pod) bool {
	return podReadyAndServing(pod)
}
