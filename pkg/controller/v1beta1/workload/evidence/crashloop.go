package evidence

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// CrashLoopWedge reports whether a Ready, operation-free row holds a pod
// wedged in a terminal kubelet waiting reason on the Component's current
// revision for longer than the configured stuck-pod grace, and names the
// wedge for the repair's Reason. When no pod is past the grace yet, the
// third result is how long the earliest such pod has left in it, zero
// when none is parked: a gang is a wedge as soon as any member is, so
// the earliest grace end is when this reading can change.
//
// Three things keep it narrow:
//
//   - The grace comes from lifecycle.stuckPodGracePeriod and is measured
//     from the pod's creation (PodWedgedPastGrace): the time a pod gets to
//     come up, after which a parked pod is a wedge at once. Unset
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
func CrashLoopWedge(input types.ReconcileInput, s *types.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool, time.Duration) {
	pod, reason, graceLeft := CrashLoopWedgedPod(input, s, expected, pods)
	if pod == nil {
		return "", false, graceLeft
	}
	return fmt.Sprintf("pod %s wedged in %s past the stuck-pod grace", pod.Name, reason), true, 0
}

// CrashLoopWedgedPod is CrashLoopWedge's reading with the wedged pod and
// its kubelet waiting reason instead of the repair's Reason: nil when the
// row holds no wedge, with the grace left as CrashLoopWedge reports it.
func CrashLoopWedgedPod(input types.ReconcileInput, s *types.InstanceStatus, expected int32, pods []*corev1.Pod) (*corev1.Pod, string, time.Duration) {
	if s == nil || s.Phase != types.InstancePhaseReady || s.Operation != nil {
		return nil, "", 0
	}
	if input.StuckPodGrace <= 0 {
		return nil, "", 0
	}
	if query.PodSetFullyServing(pods, expected) {
		return nil, "", 0
	}
	currentHash := query.RevisionFromName(input.ObservedState.CurrentRevision).Hash()
	now := input.Now()
	var graceLeft time.Duration
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if !podOnCurrentRevision(pod, currentHash) {
			continue
		}
		if reason, wedged := PodWedgedPastGrace(pod, now, input.StuckPodGrace); wedged {
			return pod, reason, 0
		}
		if left := TerminalWaitingGraceLeft(pod, now, input.StuckPodGrace); left > 0 && (graceLeft == 0 || left < graceLeft) {
			graceLeft = left
		}
	}
	return nil, "", graceLeft
}

// PodSetServesNothing reports whether a non-empty pod set is provably
// out of service: no routed pod is in rotation, and every live routed pod
// is either in a terminal phase or parked in a terminal kubelet waiting
// reason. A start that replaces such a set opens no new unavailability,
// and the coordination gate already counts the outage in its serving-based
// unavailability. A pod that is merely not yet Ready is not counted:
// its wait may still resolve, so replacing it is charged like any start.
//
// A gang serves through its leader. Its workers are never members of the
// routing Service (query.RoutedRunner), so a worker that is Ready proves
// no service and a worker that is not has no wedge to show: the verdict
// rests on the routed members alone.
func PodSetServesNothing(pods []*corev1.Pod) bool {
	live := 0
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		live++
		if query.IsTerminalPod(pod) || !query.RoutedRunner(pod) {
			continue
		}
		if podreadiness.ReadyAndServing(pod) {
			return false
		}
		if _, parked := terminalWaitingReason(pod); !parked {
			return false
		}
	}
	return live > 0
}

// PodSetUnavailableOnParkedMember reports whether a pod set is out of
// the coordination gate's serving count because a member is parked: fewer
// than desired live pods are in rotation, and a live pod is parked in a
// terminal kubelet waiting reason. The gate counts an Instance as serving
// only when every desired pod is, so such an Instance is already inside
// the gate's unavailability whichever member is parked; a start that
// replaces the set takes nothing further out of that count, while the
// per-Component budget still charges it when a routed member serves. A
// member that is merely not yet Ready parks nothing: its wait may still
// resolve, so the set keeps the consult.
func PodSetUnavailableOnParkedMember(pods []*corev1.Pod, desired int32) bool {
	var serving int32
	parked := false
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil || query.IsTerminalPod(pod) {
			continue
		}
		if podreadiness.ReadyAndServing(pod) {
			serving++
			continue
		}
		if _, waiting := terminalWaitingReason(pod); waiting {
			parked = true
		}
	}
	return parked && serving < desired
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
