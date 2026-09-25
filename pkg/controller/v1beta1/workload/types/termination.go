package types

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DrainOverdueReason is the LastFailure.Reason that records an
// announced overdue drain. It is not a failure: the row keeps draining
// and keeps its phase, and the record exists so the announcement is
// made exactly once per episode and so readers of the status can see
// which Instances are past their drain deadline. Time carries the
// elapsed deadline, which both keys the episode and keeps the record
// byte-stable across reconciles.
const DrainOverdueReason = "DrainOverdue"

// PodTermination extracts the most operator-relevant container failure
// diagnostics from pod into an *InstanceTermination. Returns nil only when
// pod is nil.
//
// Time records WHEN THE FAILURE HAPPENED, taken from kubelet's FinishedAt,
// and NOT when this function ran. `now` is only a fallback for the cases
// where no container-level termination timestamp survived (branch 4 below,
// and a zero FinishedAt). This matters beyond cosmetics: InstanceTermination
// is stored on InstanceStatus.LastFailure, which is part of the status the
// no-op write guard DeepEquals before writing (see inferencereplica/
// status.go). Stamping `now` made a permanently-failed Instance produce a
// differing status on EVERY reconcile, defeating that guard and driving a
// self-sustaining write -> watch -> reconcile loop.
//
// Selection precedence (the failure operators most want to see first):
//
//  1. A container terminated with a non-zero exit code — the canonical
//     crash signal (OOMKilled exit 137, generic Error exit 1, ...). Both
//     the live State and the LastTerminationState are consulted, since a
//     CrashLoopBackOff pod shows the crash in LastTerminationState while
//     its live State is Waiting.
//  2. A container stuck in a terminal waiting state (CrashLoopBackOff,
//     ImagePullBackOff, CreateContainerError, ...) — the wedge signal the
//     stuck-pod escalator fires on. ExitCode is left nil (no process ran).
//  3. Any terminated container at all (exit 0 but the pod is Failed — rare,
//     e.g. a restartPolicy=Never sidecar exiting).
//  4. Pod-level fallback: Reason="PodFailed" with the pod's status message,
//     when the pod phase is Failed but no per-container detail survived.
//
// Init containers are consulted with the same precedence after regular
// containers so an init-stage crash is still captured.
func PodTermination(pod *corev1.Pod, now metav1.Time) *InstanceTermination {
	if pod == nil {
		return nil
	}

	allStatuses := func() []corev1.ContainerStatus {
		out := make([]corev1.ContainerStatus, 0, len(pod.Status.ContainerStatuses)+len(pod.Status.InitContainerStatuses))
		out = append(out, pod.Status.ContainerStatuses...)
		out = append(out, pod.Status.InitContainerStatuses...)
		return out
	}()

	// 1. Non-zero terminated exit code (live or last-termination).
	for _, cs := range allStatuses {
		if t := nonZeroTerminated(cs); t != nil {
			return terminationFromTerminated(pod, cs.Name, t, now)
		}
	}
	// 2. Terminal waiting-state wedge. The waiting state itself carries no
	// timestamp, but a backoff-looping container (CrashLoopBackOff) records
	// the prior crash in LastTerminationState — prefer that over `now` so
	// the record is stable across observations.
	for _, cs := range allStatuses {
		if cs.State.Waiting != nil && IsTerminalWaitingReason(cs.State.Waiting.Reason) {
			return &InstanceTermination{
				PodName:       pod.Name,
				ContainerName: cs.Name,
				Reason:        cs.State.Waiting.Reason,
				Message:       cs.State.Waiting.Message,
				Time:          lastTerminationTimeOr(cs, now),
			}
		}
	}
	// 3. Any terminated container (exit 0 but pod Failed).
	for _, cs := range allStatuses {
		if cs.State.Terminated != nil {
			return terminationFromTerminated(pod, cs.Name, cs.State.Terminated, now)
		}
		if cs.LastTerminationState.Terminated != nil {
			return terminationFromTerminated(pod, cs.Name, cs.LastTerminationState.Terminated, now)
		}
	}
	// 4. Pod-level fallback. No container-level FinishedAt survived, so `now`
	// is the only timestamp available here.
	if pod.Status.Phase == corev1.PodFailed {
		return &InstanceTermination{
			PodName: pod.Name,
			Reason:  "PodFailed",
			Message: pod.Status.Message,
			Time:    now,
		}
	}
	return nil
}

// PodTerminationWithReason is PodTermination with an explicit reason
// override used by the stuck-pod escalator, which has already classified
// the wedge reason from the live waiting state. When PodTermination can't
// extract any per-container detail (e.g. the status snapshot raced the
// kubelet write), the override still gives operators the classified reason
// + pod name rather than an empty record. When PodTermination DID extract a
// record but with an empty Reason, the override fills it in.
func PodTerminationWithReason(pod *corev1.Pod, reason string, now metav1.Time) *InstanceTermination {
	t := PodTermination(pod, now)
	if t == nil {
		name := ""
		if pod != nil {
			name = pod.Name
		}
		return &InstanceTermination{PodName: name, Reason: reason, Time: now}
	}
	if t.Reason == "" {
		t.Reason = reason
	}
	return t
}

// nonZeroTerminated returns the terminated state (live or last) carrying a
// non-zero exit code, or nil. Live State wins over LastTerminationState so
// the freshest crash is reported.
func nonZeroTerminated(cs corev1.ContainerStatus) *corev1.ContainerStateTerminated {
	if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
		return cs.State.Terminated
	}
	if cs.LastTerminationState.Terminated != nil && cs.LastTerminationState.Terminated.ExitCode != 0 {
		return cs.LastTerminationState.Terminated
	}
	return nil
}

// terminationFromTerminated builds an InstanceTermination from a terminated
// container state. Reason falls back to "Error" when kubelet left it blank
// (it occasionally does for OOM races) so the record is never reason-less.
// Time is kubelet's FinishedAt; `now` is used only when kubelet left it zero.
func terminationFromTerminated(pod *corev1.Pod, container string, t *corev1.ContainerStateTerminated, now metav1.Time) *InstanceTermination {
	reason := t.Reason
	if reason == "" {
		reason = "Error"
	}
	exit := t.ExitCode
	return &InstanceTermination{
		PodName:       pod.Name,
		ContainerName: container,
		Reason:        reason,
		ExitCode:      &exit,
		Message:       t.Message,
		Time:          finishedAtOr(t, now),
	}
}

// finishedAtOr returns kubelet's recorded termination time, or `fallback`
// when it is absent. A zero FinishedAt means kubelet never stamped one, so
// there is no event time to report and the observation time is the best
// available answer.
func finishedAtOr(t *corev1.ContainerStateTerminated, fallback metav1.Time) metav1.Time {
	if t != nil && !t.FinishedAt.IsZero() {
		return t.FinishedAt
	}
	return fallback
}

// lastTerminationTimeOr returns the FinishedAt of the container's previous
// termination, or `fallback` when there is none. Used by the waiting-state
// branch, where the live state carries no timestamp but a backoff-looping
// container has one from the crash that put it there.
func lastTerminationTimeOr(cs corev1.ContainerStatus, fallback metav1.Time) metav1.Time {
	return finishedAtOr(cs.LastTerminationState.Terminated, fallback)
}

// ShortString renders an InstanceTermination as a compact, grep-friendly
// fragment for K8s event messages, e.g. `pod foo-0 container main failed
// (OOMKilled, exit 137)` or `pod foo-0 container main stuck
// (ImagePullBackOff)`. Returns "" for a nil receiver so callers can embed
// it unconditionally.
func (t *InstanceTermination) ShortString() string {
	if t == nil {
		return ""
	}
	var b string
	if t.PodName != "" {
		b = "pod " + t.PodName
	} else {
		b = "pod <unknown>"
	}
	if t.ContainerName != "" {
		b += " container " + t.ContainerName
	}
	switch {
	case t.ExitCode != nil:
		b += fmt.Sprintf(" failed (%s, exit %d)", reasonOrUnknown(t.Reason), *t.ExitCode)
	case t.Reason == "PodFailed":
		b += " failed (PodFailed)"
	default:
		b += fmt.Sprintf(" stuck (%s)", reasonOrUnknown(t.Reason))
	}
	return b
}

func reasonOrUnknown(reason string) string {
	if reason == "" {
		return "unknown"
	}
	return reason
}
