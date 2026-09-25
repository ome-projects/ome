package evidence

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// CrashLoopWedge reports whether a Ready, operation-free row holds a pod
// wedged in a terminal kubelet waiting reason on the Component's current
// revision for longer than the configured stuck-pod grace, and names the
// wedge for the repair's Reason.
//
// Three things keep it narrow:
//
//   - The grace comes from lifecycle.stuckPodGracePeriod and is measured
//     the way every other reader of this evidence measures it. Unset
//     disables the shape, exactly as it disables the fast escalation.
//   - An off-revision wedge is a leftover, not this row's workload:
//     there is no revision for a rebuild to land on, and the escalation
//     pass owns that pod through its wedged-pod edge to Failed.
//   - A fully serving pod set is never a wedge on the strength of a
//     container waiting reason: the Instance is answering traffic.
//
// Evidence only. Whether the repair may actually open — the RetryBlock
// held against the revision, the unavailability budget, the coordination
// gate — belongs to the restart pass that reads this.
func CrashLoopWedge(input types.ReconcileInput, s *types.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	if s == nil || s.Phase != types.InstancePhaseReady || s.Operation != nil {
		return "", false
	}
	if input.StuckPodGrace <= 0 {
		return "", false
	}
	if query.PodSetFullyServing(pods, expected) {
		return "", false
	}
	currentHash := query.RevisionFromName(input.ObservedState.CurrentRevision).Hash()
	now := input.Now()
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if !podOnCurrentRevision(pod, currentHash) {
			continue
		}
		reason, stuck := PodStuckInTerminalWaiting(pod, now, input.StuckPodGrace)
		if !stuck {
			continue
		}
		return fmt.Sprintf("pod %s wedged in %s past the stuck-pod grace", pod.Name, reason), true
	}
	return "", false
}

// podOnCurrentRevision reports whether a pod belongs to the Component's
// current revision. An unknown hash on either side leaves the pod in
// scope: the label is the only evidence of ownership, and without it
// every pod of the Instance stays a candidate.
func podOnCurrentRevision(pod *corev1.Pod, currentHash string) bool {
	podRev := query.RevisionFromPod(pod)
	if currentHash == "" || podRev.IsZero() {
		return true
	}
	return podRev.Same(query.RevisionFromHash(currentHash))
}
