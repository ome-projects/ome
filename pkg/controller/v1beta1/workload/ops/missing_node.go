package ops

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// FreePodsOnMissingNodes removes the pods the pass read as bound to a Node
// the cluster no longer holds (query.SplitPodsOnMissingNodes), in the
// order given. No kubelet is left to run such a pod or to finish a
// graceful delete of it, so each goes the way the force-delete sweep
// removes a pod (forceDeletePod): grace zero under its UID, so a same-name
// successor can never be hit, a Warning naming the pod and its node, and
// the audit row. The pass already counts without these pods; this step
// keeps the objects from blocking the names the rebuild needs.
//
// A pod already gone, or replaced under its name, is done. A pod another
// controller's finalizer keeps in the API is asked again on every pass
// until that controller lets go; the apiserver answers each delete after
// the first with the object as it is. Every pod is visited whatever
// happened to the ones before it, and the errors are joined: the caller
// ends the pass on them, since its plan assumed the removals.
func FreePodsOnMissingNodes(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, gone []*corev1.Pod) error {
	var errs []error
	for _, pod := range gone {
		if pod == nil || input.Swept.Swept(pod.UID) {
			continue
		}
		idx, _ := query.InstanceIdxFromLabels(pod)
		text := fmt.Sprintf("force-deleted pod %s bound to node %s, which no longer exists", pod.Name, pod.Spec.NodeName)
		if _, err := forceDeletePod(ctx, deps, input, pod, idx, text); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
