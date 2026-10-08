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
// out of service on the Component's current revision for longer than the
// configured stuck-pod grace — parked in a terminal kubelet waiting
// reason, or promoted and then unready (PodUnreadyPastGrace) — and names
// the wedge for the repair's Reason. When no pod is past the grace yet,
// the third result is how long the earliest such pod has left in it,
// zero when none is parked or unready: a gang is a wedge as soon as any
// member is, so the earliest grace end is when this reading can change.
//
// Three things keep it narrow:
//
//   - The grace comes from lifecycle.stuckPodGracePeriod. A parked pod
//     measures it from its creation (PodWedgedPastGrace): the time a pod
//     gets to come up, after which a parked pod is a wedge at once. An
//     unready pod measures it from the moment its containers last
//     stopped being ready. Unset disables the shape, exactly as it
//     disables the fast escalation.
//   - An off-revision wedge is a leftover, not this row's workload:
//     there is no revision for a rebuild to land on, and the escalation
//     pass owns that pod through its wedged-pod edge to Failed.
//   - A fully serving pod set is never a wedge on the strength of a
//     container waiting reason: the Instance is answering traffic.
//
// Evidence only. Whether the repair may actually open — the RetryBlock
// held against the revision, the unavailability budget, the coordination
// gate, whether the row's set is one the unready shape is read for —
// belongs to the restart pass that reads this.
func CrashLoopWedge(input types.ReconcileInput, s *types.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool, time.Duration) {
	pod, reason, graceLeft := CrashLoopWedgedPod(input, s, expected, pods, true)
	if pod == nil {
		return "", false, graceLeft
	}
	return WedgeReason(pod, reason), true, 0
}

// WedgeReason names a wedge for a repair's Reason: the terminal waiting reason
// the pod is parked in, or the kubelet's reason for a pod unready after serving.
func WedgeReason(pod *corev1.Pod, reason string) string {
	if types.IsTerminalWaitingReason(reason) {
		return fmt.Sprintf("pod %s wedged in %s past the stuck-pod grace", pod.Name, reason)
	}
	return fmt.Sprintf("pod %s unready (%s) past the stuck-pod grace after serving", pod.Name, reason)
}

// CrashLoopWedgedPod is CrashLoopWedge's reading with the wedged pod and
// its kubelet reason instead of the repair's Reason: nil when the row
// holds no wedge, with the grace left as CrashLoopWedge reports it.
// unreadyIsWedge admits the promoted-then-unready shape; without it only
// a parked pod is a wedge, and only parked pods report a grace left.
func CrashLoopWedgedPod(input types.ReconcileInput, s *types.InstanceStatus, expected int32, pods []*corev1.Pod, unreadyIsWedge bool) (*corev1.Pod, string, time.Duration) {
	return CrashLoopWedgedPodOn(input, s, expected, pods, input.ObservedState.CurrentRevision, unreadyIsWedge)
}

// CrashLoopWedgedPodOn is CrashLoopWedgedPod read against rev instead of
// the Component's current revision. A roll promotes a row onto the
// revision it runs ahead of the current one, so a reader that already
// knows the wedged set is that row's own — the retry ladder rebuilding a
// crash it counted on the running revision — reads the wedge there; the
// repair of a wedge no counted crash is behind stays on the current
// revision, and an off-current wedge is the escalation's. A parked pod
// is named ahead of an unready one.
func CrashLoopWedgedPodOn(input types.ReconcileInput, s *types.InstanceStatus, expected int32, pods []*corev1.Pod, rev string, unreadyIsWedge bool) (*corev1.Pod, string, time.Duration) {
	if s == nil || s.Phase != types.InstancePhaseReady || s.Operation != nil {
		return nil, "", 0
	}
	if input.StuckPodGrace <= 0 {
		return nil, "", 0
	}
	if query.PodSetFullyServing(pods, expected) {
		return nil, "", 0
	}
	own := query.RevisionFromName(rev)
	now := input.Now()
	var members []*corev1.Pod
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil || !podOnRevision(pod, own) {
			continue
		}
		members = append(members, pod)
	}
	var graceLeft time.Duration
	earliest := func(left time.Duration) {
		if left > 0 && (graceLeft == 0 || left < graceLeft) {
			graceLeft = left
		}
	}
	for _, pod := range members {
		if reason, wedged := PodWedgedPastGrace(pod, now, input.StuckPodGrace); wedged {
			return pod, reason, 0
		}
		earliest(TerminalWaitingGraceLeft(pod, now, input.StuckPodGrace))
	}
	if !unreadyIsWedge {
		return nil, "", graceLeft
	}
	for _, pod := range members {
		if PodUnreadyPastGrace(pod, now, input.StuckPodGrace) {
			return pod, ReasonContainersNotReady, 0
		}
		earliest(UnreadyGraceLeft(pod, now, input.StuckPodGrace))
	}
	return nil, "", graceLeft
}

// podOutOfService reports whether a live pod is provably out of service: parked
// in a terminal kubelet waiting reason, or promoted and unready past grace. The
// one reading the budgets, the gate exemption and the repair share.
func podOutOfService(pod *corev1.Pod, now time.Time, grace time.Duration) bool {
	if _, parked := terminalWaitingReason(pod); parked {
		return true
	}
	return PodUnreadyPastGrace(pod, now, grace)
}

// PodSetServesNothing reports whether a non-empty pod set is provably
// out of service: no routed pod is in rotation, and every live routed pod
// is either in a terminal phase or out of service (podOutOfService). A
// start that replaces such a set opens no new unavailability, and the
// coordination gate already counts that unavailability in its serving-based
// unavailability. A pod that is merely not yet Ready, or unready for
// less than grace, is not counted: its wait may still resolve, so
// replacing it is charged like any start.
//
// A gang serves through its leader. Its workers are never members of the
// routing Service (query.RoutedRunner), so a worker that is Ready proves
// no service and a worker that is not has no wedge to show: the verdict
// rests on the routed members alone.
func PodSetServesNothing(pods []*corev1.Pod, now time.Time, grace time.Duration) bool {
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
		if !podOutOfService(pod, now, grace) {
			return false
		}
	}
	return live > 0
}

// PodSetOutOfRotation reports whether a non-empty pod set has no routed pod
// in rotation at this moment, whatever the reason and for however long: the
// Instance serves nothing now. It is the roll's ordering reading, wider than
// PodSetServesNothing, which waits out the stuck-pod grace before it reads a
// set as provably out of service: replacing an Instance that serves nothing
// first costs no capacity whether or not its wait resolves, while charging
// its start as a free one would, so the budgets and the gate keep the graced
// reading. A gang reads its routed members alone, as PodSetServesNothing does.
func PodSetOutOfRotation(pods []*corev1.Pod) bool {
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
	}
	return live > 0
}

// PodSetDarkOnReadiness reports whether a non-empty pod set is dark on its
// kubelet readiness alone: every live routed pod runs with ContainersReady
// False for the stuck-pod grace (ContainersUnreadyPastGrace), whatever its
// serving gate reads. It is the reading for a pod set a pass has already
// drained, which PodSetServesNothing cannot tell from the drain. A gang
// reads its routed members alone, as PodSetServesNothing does.
func PodSetDarkOnReadiness(pods []*corev1.Pod, now time.Time, grace time.Duration) bool {
	live, routed := 0, 0
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		live++
		if query.IsTerminalPod(pod) || !query.RoutedRunner(pod) {
			continue
		}
		routed++
		if !ContainersUnreadyPastGrace(pod, now, grace) {
			return false
		}
	}
	return live > 0 && routed > 0
}

// PodSetUnavailableOnParkedMember reports whether a pod set is out of
// the coordination gate's serving count because a member is out of
// service: fewer than desired live pods are in rotation, and a live pod
// is parked in a terminal kubelet waiting reason or unready past grace.
// The gate counts an Instance as serving only when every desired pod is,
// so such an Instance is already inside the gate's unavailability
// whichever member is out; a start that replaces the set takes nothing
// further out of that count, while the per-Component budget still
// charges it when a routed member serves. A member that is merely not
// yet Ready, or unready inside grace, parks nothing: its wait may still
// resolve, so the set keeps the consult.
func PodSetUnavailableOnParkedMember(pods []*corev1.Pod, desired int32, now time.Time, grace time.Duration) bool {
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
		if podOutOfService(pod, now, grace) {
			parked = true
		}
	}
	return parked && serving < desired
}

// podOnRevision reports whether a pod belongs to rev. An unknown hash on
// either side leaves the pod in scope: the label is the only evidence of
// ownership, and without it every pod of the Instance stays a candidate.
func podOnRevision(pod *corev1.Pod, rev query.RevisionID) bool {
	podRev := query.RevisionFromPod(pod)
	if rev.IsZero() || podRev.IsZero() {
		return true
	}
	return podRev.Same(rev)
}
