package evidence

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ReasonContainersNotReady is the failure reason for a pod stuck in
// readiness limbo: phase Running, every container started, and
// ContainersReady never True. It is the kubelet's own reason string on
// that condition, so the record an operator reads matches what
// `kubectl describe pod` shows.
//
// AMBIGUOUS: a probe that never passes can be the image or its
// configuration, but it can equally be the node the pod landed on, so the
// reason is evidence only. It never selects the disposition branch and
// never charges the revision's retry ladder; relocation keeps first try.
const ReasonContainersNotReady = "ContainersNotReady"

// podRunningNotReady reports whether a live pod is in readiness limbo.
//
// The shape is deliberately narrow: phase Running, at least one
// container status, EVERY regular container actually running, and
// ContainersReady not True. A container parked in a waiting state
// (CrashLoopBackOff, ImagePullBackOff, ...) is excluded because the
// kubelet has already named that failure and the engine classifies it
// on its own terms.
func podRunningNotReady(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Running == nil {
			return false
		}
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.ContainersReady {
			return cond.Status != corev1.ConditionTrue
		}
	}
	// No ContainersReady condition yet: the kubelet has not vouched for
	// the pod, so it is not ready.
	return true
}

// ReasonReadinessGateNotSatisfied is the failure reason for the other
// readiness limbo: every container is running and ContainersReady is True,
// but the pod's Ready condition never follows because a readiness gate the
// pod declares stays unsatisfied. The pod is not eligible for its Service,
// so no promote bar it stands behind can clear.
//
// AMBIGUOUS, exactly like ReasonContainersNotReady: the unsatisfied gate can
// be one no writer owns, one a second writer is deliberately holding, or
// the serving gate this controller has not written yet, so the reason is
// evidence only. It never selects the disposition branch and
// never charges the revision's retry ladder; relocation keeps first try.
const ReasonReadinessGateNotSatisfied = "ReadinessGateNotSatisfied"

// podGateNotFolded reports whether a live pod is in gate limbo: phase
// Running, at least one container status, EVERY regular container actually
// running, ContainersReady True — and PodReady still not True, because
// kubelet ANDs the declared readiness gates into it.
//
// Disjoint from PodRunningNotReady by construction (that shape requires
// ContainersReady not True), so a pod is evidence for at most one of them.
func podGateNotFolded(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Running == nil {
			return false
		}
	}
	containersReady, podReady := false, false
	for _, cond := range pod.Status.Conditions {
		switch cond.Type {
		case corev1.ContainersReady:
			containersReady = cond.Status == corev1.ConditionTrue
		case corev1.PodReady:
			podReady = cond.Status == corev1.ConditionTrue
		}
	}
	return containersReady && !podReady
}

// GateNotFoldedTermination is the LastFailure record for a pod in gate
// limbo: which of the readiness gates it declares are unsatisfied, and what
// the Ready condition says about them. Without it the only trace a deadline
// expiry leaves is the elapsed timeout, which points at nothing an operator
// can act on — every container passed its probes.
//
// Time is the Ready condition's last transition when the kubelet stamped
// one, so re-observing an unchanged pod yields an identical record and the
// status no-op write guard holds.
func GateNotFoldedTermination(pod *corev1.Pod, now metav1.Time) *types.InstanceTermination {
	if pod == nil {
		return nil
	}
	t := &types.InstanceTermination{
		PodName: pod.Name,
		Reason:  ReasonReadinessGateNotSatisfied,
		Message: "pod containers are ready but a readiness gate keeps it out of its Service",
		Time:    now,
	}
	if gates := unsatisfiedReadinessGates(pod); len(gates) > 0 {
		t.Message += "; unsatisfied gates: " + strings.Join(gates, ", ")
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type != corev1.PodReady {
			continue
		}
		if cond.Reason != "" {
			t.Message += "; Ready=False (" + cond.Reason + ")"
		}
		if cond.Message != "" {
			t.Message += ": " + cond.Message
		}
		if !cond.LastTransitionTime.IsZero() {
			t.Time = cond.LastTransitionTime
		}
		break
	}
	return t
}

// unsatisfiedReadinessGates lists the condition types the pod declares as
// readiness gates whose condition is missing or not True — the exact set
// kubelet is still waiting on before it flips Ready.
func unsatisfiedReadinessGates(pod *corev1.Pod) []string {
	var gates []string
	for _, gate := range pod.Spec.ReadinessGates {
		satisfied := false
		for _, cond := range pod.Status.Conditions {
			if cond.Type == gate.ConditionType {
				satisfied = cond.Status == corev1.ConditionTrue
				break
			}
		}
		if !satisfied {
			gates = append(gates, string(gate.ConditionType))
		}
	}
	return gates
}

// podHasWaitingContainer reports whether any container of a pod —
// regular or init — is parked in a waiting state. The readiness-limbo
// classification is suppressed for a pod set containing one: the kubelet
// has named a failure there, and that naming decides the blame.
func podHasWaitingContainer(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting != nil {
				return true
			}
		}
	}
	return false
}

// ContainersNotReadyTermination is the LastFailure record for a pod in
// readiness limbo: which containers are not ready, and what the
// ContainersReady condition says about them. Without it the only trace a
// deadline expiry leaves is the elapsed timeout, which tells an operator
// nothing about the probe that never passed.
//
// Time is the condition's last transition when the kubelet stamped one,
// so re-observing an unchanged pod yields an identical record and the
// status no-op write guard holds.
func ContainersNotReadyTermination(pod *corev1.Pod, now metav1.Time) *types.InstanceTermination {
	if pod == nil {
		return nil
	}
	t := &types.InstanceTermination{
		PodName: pod.Name,
		Reason:  ReasonContainersNotReady,
		Message: "pod is Running but its containers never became ready",
		Time:    now,
	}
	if names := notReadyContainerNames(pod); len(names) > 0 {
		t.ContainerName = names[0]
		t.Message += "; not ready: " + strings.Join(names, ", ")
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type != corev1.ContainersReady {
			continue
		}
		if cond.Reason != "" {
			t.Message += "; ContainersReady=False (" + cond.Reason + ")"
		}
		if cond.Message != "" {
			t.Message += ": " + cond.Message
		}
		if !cond.LastTransitionTime.IsZero() {
			t.Time = cond.LastTransitionTime
		}
		break
	}
	return t
}

// notReadyContainerNames lists the pod's containers the kubelet has not
// marked Ready, regular containers first so the primary runner leads.
func notReadyContainerNames(pod *corev1.Pod) []string {
	var names []string
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if !cs.Ready {
				names = append(names, cs.Name)
			}
		}
	}
	return names
}

// FirstRunningNotReadyPod returns the first live pod in readiness limbo
// — running every container yet never reporting ContainersReady. Pure
// evidence: it names what to record, never which branch to take.
//
// Scoped to attemptRev when the pods say which revision they belong to.
// A single-pod surge shares its Instance index with the source it is
// replacing, so the set holds both; blaming a source whose own probes
// happen to be flapping charges nothing (its revision is superseded) and
// leaves the attempt to expire again on every pass.
//
// Suppressed whenever ANY live pod in the scoped set has a container
// parked in a waiting state: the kubelet has named a failure there, and
// that name owns the blame. Several of those names (CrashLoopBackOff,
// RunContainerError) are deliberately ambiguous between the revision and
// the node, and a not-ready sibling must not be what decides to hold the
// revision instead of relocating.
func FirstRunningNotReadyPod(pods []*corev1.Pod, attemptRev string) (*corev1.Pod, string) {
	var limbo *corev1.Pod
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if !podOnAttemptRevision(pod, attemptRev) {
			continue
		}
		if podHasWaitingContainer(pod) {
			return nil, ""
		}
		if limbo == nil && podRunningNotReady(pod) {
			limbo = pod
		}
	}
	if limbo == nil {
		return nil, ""
	}
	return limbo, ReasonContainersNotReady
}

// FirstGateNotFoldedPod returns the first live pod whose containers are
// ready but whose Ready condition never followed, under the same scoping and
// suppression rules as firstRunningNotReadyPod: revision-scoped when the pods
// say which revision they belong to, and suppressed outright when the kubelet
// has named a waiting reason on any pod of the scoped set.
func FirstGateNotFoldedPod(pods []*corev1.Pod, attemptRev string) *corev1.Pod {
	var limbo *corev1.Pod
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if !podOnAttemptRevision(pod, attemptRev) {
			continue
		}
		if podHasWaitingContainer(pod) {
			return nil
		}
		if limbo == nil && podGateNotFolded(pod) {
			limbo = pod
		}
	}
	return limbo
}

// podOnAttemptRevision reports whether a pod may be read as the
// attempt's own. A pod carrying no revision-hash label, or an attempt
// with no resolvable target, leaves the scope open — the label is the
// only evidence of ownership, and without it every pod stays a
// candidate.
func podOnAttemptRevision(pod *corev1.Pod, attemptRev string) bool {
	target := query.RevisionFromName(attemptRev)
	podRev := query.RevisionFromPod(pod)
	if target.IsZero() || podRev.IsZero() {
		return true
	}
	return podRev.Same(target)
}
