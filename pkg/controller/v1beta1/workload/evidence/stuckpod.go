package evidence

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Two readings share the terminal waiting set and the operator's grace,
// and differ in what the grace is measured from.
//
// PodStuckInTerminalWaiting is the escalation's reading of an attempt's
// pod: parked for the grace since its waiting episode began
// (WaitingEpisodeStart). An attempt's pod that served and then broke is
// measured from the break, so a pod older than the grace is not stuck on
// the pass that first reads its waiting reason.
//
// PodWedgedPastGrace is the repair's reading of a settled row's pod:
// parked, and older than the grace. The grace is the time a pod gets to
// come up, and a pod that had it and parks is broken at once, whether or
// not it served first.

// PodStuckInTerminalWaiting returns (reason, true) when pod has at
// least one container — regular or init — parked in a terminal waiting
// reason for at least grace since its waiting episode began, otherwise
// ("", false).
//
// The kubelet exposes no per-state transition time on a waiting
// container, so the episode begins at the pod's creation, or at the
// moment its containers last stopped being ready when it served first.
// For ImagePullBackOff the kubelet retries at least once before flipping
// to BackOff (~30s on a 404), so the operator's grace is what separates
// a stuck pull from the brief PodInitializing / ContainerCreating /
// ImagePulling window. A pod with no creation stamp cannot prove any
// duration and is never stuck.
func PodStuckInTerminalWaiting(pod *corev1.Pod, now time.Time, grace time.Duration) (string, bool) {
	since := WaitingEpisodeStart(pod)
	if since.IsZero() {
		return "", false
	}
	if now.Sub(since) < grace {
		return "", false
	}
	return terminalWaitingReason(pod)
}

// PodWedgedPastGrace returns (reason, true) when pod is older than grace
// and has at least one container — regular or init — parked in a
// terminal waiting reason, otherwise ("", false). A pod with no creation
// stamp cannot prove any age and is never wedged.
func PodWedgedPastGrace(pod *corev1.Pod, now time.Time, grace time.Duration) (string, bool) {
	if pod == nil || pod.CreationTimestamp.IsZero() {
		return "", false
	}
	if now.Sub(pod.CreationTimestamp.Time) < grace {
		return "", false
	}
	return terminalWaitingReason(pod)
}

// TerminalWaitingGraceLeft is how much of grace a pod parked in a
// terminal waiting reason has left before PodWedgedPastGrace reads it as
// wedged: zero once the grace has passed, for a pod parked in no such
// reason, and for a pod with no creation stamp. The grace ending raises
// no event, so a pass that read the pod inside it owes itself this wait.
func TerminalWaitingGraceLeft(pod *corev1.Pod, now time.Time, grace time.Duration) time.Duration {
	if pod == nil || pod.CreationTimestamp.IsZero() {
		return 0
	}
	if _, parked := terminalWaitingReason(pod); !parked {
		return 0
	}
	if left := grace - now.Sub(pod.CreationTimestamp.Time); left > 0 {
		return left
	}
	return 0
}

// WaitingEpisodeStart is when the pod's current waiting episode began,
// the instant the escalation's grace is measured from: the later of the
// pod's creation and the last transition of its ContainersReady condition
// away from True. A pod that served and then broke is measured from the
// break, not from its creation; one that never served, or carries no
// condition yet, is measured from its creation. Zero for a nil pod or
// one with no creation stamp, which can prove no duration.
func WaitingEpisodeStart(pod *corev1.Pod) time.Time {
	if pod == nil || pod.CreationTimestamp.IsZero() {
		return time.Time{}
	}
	since := pod.CreationTimestamp.Time
	for _, cond := range pod.Status.Conditions {
		if cond.Type != corev1.ContainersReady || cond.Status == corev1.ConditionTrue {
			continue
		}
		if cond.LastTransitionTime.Time.After(since) {
			since = cond.LastTransitionTime.Time
		}
	}
	return since
}

// terminalWaitingReason names the terminal kubelet waiting reason a
// container of the pod, regular or init, is parked in.
func terminalWaitingReason(pod *corev1.Pod) (string, bool) {
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting != nil && types.IsTerminalWaitingReason(cs.State.Waiting.Reason) {
				return cs.State.Waiting.Reason, true
			}
		}
	}
	return "", false
}

// FirstStuckPodForInstance returns the first pod in pods that is stuck
// in a terminal kubelet waiting state past the grace window, plus the
// kubelet reason string. Returns (nil, "") when no pod is stuck —
// caller skips escalation.
//
// "First" is order-deterministic by the input slice; callers that need
// a stable index should sort pods before calling. For event emission
// the order doesn't matter (the message names the specific pod).
func FirstStuckPodForInstance(pods []*corev1.Pod, now time.Time, grace time.Duration) (*corev1.Pod, string) {
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		// Skip pods marked for deletion — kubelet may be tearing them
		// down, and "ImagePullBackOff on a deleting pod" is not an
		// escalation signal (the pod is about to be gone anyway).
		if pod.DeletionTimestamp != nil {
			continue
		}
		if reason, stuck := PodStuckInTerminalWaiting(pod, now, grace); stuck {
			return pod, reason
		}
	}
	return nil, ""
}

// PodsForStuckCheck returns the pods whose stuck state should escalate s.
// During a gang Update surge, inspect the replacement gang exclusively: the
// source stays on the prior revision by design and may be the broken workload
// this corrective rollout is replacing. Treating that old failure as a surge
// failure makes the recovery path delete each replacement immediately. An
// empty replacement bucket is also intentional while its pods are created.
//
// Migrate also uses SurgeIndex, including as a reverse sibling pointer on its
// target status. It keeps the own-plus-sibling classification; its failure
// semantics are separate from the gang Update state machine.
func PodsForStuckCheck(s types.InstanceStatus, byIdx map[int32][]*corev1.Pod) []*corev1.Pod {
	own := byIdx[s.Index]
	if s.Operation == nil || s.Operation.SurgeIndex == nil {
		return own
	}
	sibling := byIdx[*s.Operation.SurgeIndex]
	if s.Operation.Type == types.InstanceOperationUpdate {
		return sibling
	}
	if len(sibling) == 0 {
		return own
	}
	out := make([]*corev1.Pod, 0, len(own)+len(sibling))
	out = append(out, own...)
	out = append(out, sibling...)
	return out
}

// AttemptStuckPods narrows the pods a stuck check may blame an attempt
// for to the pods that are the attempt's own. The pod an attempt is
// replacing may be the broken workload the attempt corrects, and a wedge
// on it is the attempt's reason, never its failure.
//
// An Update is judged on the pods that carry its revision: a single-pod
// surge shares its index with the source it will drain, a recreate holds
// the old incarnation until its drain deletes it, and an in-place patch
// carries the old revision until the patch restamps it; the gang surge
// reads the same rule through its replacement bucket. A pod with no
// revision label stays in scope. attemptRev names the revision the
// attempt is converging toward; empty leaves the scope open.
//
// A Restart is judged on the pods at the row's incarnation: the set of
// the incarnation before it is what the repair drains and deletes, and
// the repair owns that set until its replacement runs. A pod with no
// incarnation label stays in scope. Every pod of any other attempt is in
// scope.
func AttemptStuckPods(s types.InstanceStatus, pods []*corev1.Pod, attemptRev string) []*corev1.Pod {
	if s.Operation == nil {
		return pods
	}
	var own func(*corev1.Pod) bool
	switch s.Operation.Type {
	case types.InstanceOperationUpdate:
		own = func(pod *corev1.Pod) bool { return podOnAttemptRevision(pod, attemptRev) }
	case types.InstanceOperationRestart:
		own = func(pod *corev1.Pod) bool { return !podBelowIncarnation(pod, s.Incarnation) }
	default:
		return pods
	}
	out := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod != nil && own(pod) {
			out = append(out, pod)
		}
	}
	return out
}

// podBelowIncarnation reports whether the pod carries an incarnation
// label below incarnation: a pod of a set a Restart is replacing. Only a
// Restart bumps a row's incarnation, so no other attempt leaves such a
// pod behind. A pod with no label is not read as below.
func podBelowIncarnation(pod *corev1.Pod, incarnation int64) bool {
	inc, ok := query.InstanceIncarnationFromLabels(pod)
	return ok && inc < incarnation
}

// FirstWorkloadCausedPod returns the first live (non-deleting) pod whose
// failure is deterministically scoped to the revision, plus the matched
// reason: a workload-caused container or init-container waiting reason
// (types.IsWorkloadCausedReason).
//
// Readiness limbo is deliberately NOT in this set. A pod that runs yet
// never reports ready is ambiguous between the revision and the node it
// landed on, so it must reach the relocation branch first, exactly as an
// attempt with no evidence at all does. It contributes EVIDENCE for
// whichever branch runs (FirstRunningNotReadyPod), never the branch
// choice.
func FirstWorkloadCausedPod(pods []*corev1.Pod) (*corev1.Pod, string) {
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Waiting == nil {
				continue
			}
			if types.IsWorkloadCausedReason(cs.State.Waiting.Reason) {
				return pod, cs.State.Waiting.Reason
			}
		}
		for _, cs := range pod.Status.InitContainerStatuses {
			if cs.State.Waiting == nil {
				continue
			}
			if types.IsWorkloadCausedReason(cs.State.Waiting.Reason) {
				return pod, cs.State.Waiting.Reason
			}
		}
	}
	return nil, ""
}

// ReasonCrashLoop is the reason a crash loop is read under whichever face
// the pass sees it in: the kubelet's own CrashLoopBackOff waiting reason,
// so a record that names it matches `kubectl describe pod`.
const ReasonCrashLoop = "CrashLoopBackOff"

// FirstCrashLoopingPod returns the first live pod of the attempt whose
// container keeps exiting, plus ReasonCrashLoop. A crash loop has two
// faces and the pass may catch either: the container parked in
// CrashLoopBackOff between the kubelet's backoff retries, or Running
// again for the moment after a restart, with the restart count and a
// non-zero last termination as the only trace. Both read as the same
// crash — a restarted container that is Ready again has recovered and
// is not one. Scoped to attemptRev like the readiness readers, so a
// surge never reads its source's crash as its own.
func FirstCrashLoopingPod(pods []*corev1.Pod, attemptRev string) (*corev1.Pod, string) {
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil || !podOnAttemptRevision(pod, attemptRev) {
			continue
		}
		for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
			for _, cs := range statuses {
				if containerCrashLooping(cs) {
					return pod, ReasonCrashLoop
				}
			}
		}
	}
	return nil, ""
}

// containerCrashLooping reports whether a container status shows either
// face of a crash loop.
func containerCrashLooping(cs corev1.ContainerStatus) bool {
	if cs.State.Waiting != nil {
		return cs.State.Waiting.Reason == ReasonCrashLoop
	}
	if cs.Ready {
		return false
	}
	if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
		return true
	}
	return cs.RestartCount > 0 && cs.LastTerminationState.Terminated != nil && cs.LastTerminationState.Terminated.ExitCode != 0
}

// FirstPodWaitingForReason returns the first live pod with a container —
// regular or init — parked in the named waiting reason. It answers "which
// pod is the one the caller already classified", so the caller's cleanup
// removes exactly that object rather than the whole set.
func FirstPodWaitingForReason(pods []*corev1.Pod, reason string) *corev1.Pod {
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
			for _, status := range statuses {
				if status.State.Waiting != nil && status.State.Waiting.Reason == reason {
					return pod
				}
			}
		}
	}
	return nil
}
