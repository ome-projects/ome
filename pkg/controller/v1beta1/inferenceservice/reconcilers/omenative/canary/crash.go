package canary

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// CanaryCrash is a canary-revision pod in a crash loop after its Instance
// was serving, the first one a pass observed across the unit's members. It
// is the one capacity change the step machine does not wait out: a pod that
// is gone, or that died once and is coming back, is the workload's repair
// and the step keeps its split; a pod that keeps dying after it served is
// the canary's verdict and the ladder parks.
type CanaryCrash struct {
	Component v1beta1.ComponentType
	PodName   string
	// Detail is what the kubelet reported for the run that died: the
	// termination reason and exit code when it recorded them.
	Detail string
}

// crashAnchor is the open of the pinned run the canary is bound to: a canary
// pod's deaths after serving count across steps while that run is open. A
// resume keeps the run; a rollback or a new push opens a fresh one.
func crashAnchor(isvc *v1beta1.InferenceService, cs *v1beta1.CanaryStatus, now time.Time) time.Time {
	if cs == nil || isvc == nil || isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return now
	}
	return isvc.Status.Rollout.ActiveRun.OpenedAt.Time
}

// crashedCanaryPod finds a pod of the member's target revision whose runner
// is in a crash loop after its Instance was serving: the pod died a second
// time since the canary began, the kubelet is backing off its restarts, or
// the Instance was already rebuilt for a death inside the canary's life and
// the rebuilt pod died again. A single death is a capacity dip the capacity
// gate waits out: the kubelet restarts the runner in place or the restart
// policy rebuilds the Instance, and the step keeps its state meanwhile.
// The Instance row anchors the reading, as the workload's own restart
// trigger does: a Ready row speaks for its pods, a Restarting row only for
// the pod set it is draining, and a row that is forming, rolling or
// migrating has no serving run to lose.
//
// A death is evidenced by the pod itself, a recorded termination or its
// restart count, never by the Instance's readiness: a fresh pod with no
// restart is never a crash, so a pod replaced after a deletion reads as
// the dip it is while the replacement comes up. A pod under deletion is
// stopped by the kubelet on its way out, with the term signal, the kill
// after its grace or a clean exit, and a pod in a terminal phase is one the
// workload reads as absent and recycles; neither pod's statuses are read.
func crashedCanaryPod(component v1beta1.ComponentType, observed observedCanaryRevisions, pods []*corev1.Pod, since time.Time) *CanaryCrash {
	for _, member := range canaryPodRows(observed, pods) {
		anchor, ok := servingAnchor(member.row, member.pod, since)
		if !ok {
			continue
		}
		if detail, looping := runnerCrashLooping(member.pod, member.row, anchor, since); looping {
			return &CanaryCrash{Component: component, PodName: member.pod.Name, Detail: detail}
		}
	}
	return nil
}

// canaryPodRow is a live pod of a member's target revision with the Instance
// row it belongs to; the row is nil when the observation carries none.
type canaryPodRow struct {
	pod *corev1.Pod
	row *v1beta1.OMENativeInstanceStatus
}

// canaryPodRows pairs each live pod of the member's target revision with its
// Instance row. A pod under deletion is stopped by the kubelet on its way
// out, and a pod in a terminal phase is one the workload reads as absent and
// recycles; both are a loss the capacity gate waits out, so neither is read.
func canaryPodRows(observed observedCanaryRevisions, pods []*corev1.Pod) []canaryPodRow {
	if observed.targetHash == "" {
		return nil
	}
	rows := make(map[int32]*v1beta1.OMENativeInstanceStatus, len(observed.rows))
	for i := range observed.rows {
		rows[observed.rows[i].Index] = &observed.rows[i]
	}
	var members []canaryPodRow
	for _, pod := range pods {
		if pod == nil || pod.Labels[query.LabelRevisionHash] != observed.targetHash {
			continue
		}
		if pod.DeletionTimestamp != nil || query.IsTerminalPod(pod) {
			continue
		}
		index, ok := query.InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		members = append(members, canaryPodRow{pod: pod, row: rows[index]})
	}
	return members
}

// latestCanaryRestart is the newest moment a canary pod of the member died
// or came back: a termination its runner recorded, the start of the run the
// kubelet began after one, the pod serving again after it, or the failure
// that opened its Instance's last rebuild, which outlives the pod that
// failed, and the rebuilt pod serving in its place. Zero when no canary pod
// carries one. The timed soak measures from it, so a step moves only once
// its canary pods have served a full soak since the newest of their
// restarts; whether a restart is also a crash loop is the crash reading's
// call, made on the same pods.
func latestCanaryRestart(observed observedCanaryRevisions, pods []*corev1.Pod) time.Time {
	var latest time.Time
	for _, member := range canaryPodRows(observed, pods) {
		latest = laterOf(latest, runnerRestartedAt(member.pod))
		if member.row == nil || member.row.LastFailure == nil {
			continue
		}
		failed := member.row.LastFailure.Time.Time
		latest = laterOf(latest, failed)
		// A pod created after the failure is the rebuild that answers it: its
		// first readiness is the Instance serving again. A pod that predates
		// the failure keeps its first readiness out, as any first run does.
		if member.pod.CreationTimestamp.Time.After(failed) {
			latest = laterOf(latest, readinessMovedAt(member.pod))
		}
	}
	return latest
}

// runnerRestartedAt is the newest moment pod's runner died, was started
// again, or served again: the finish of a recorded termination, the current
// one or the one before the current run, the start of a run that follows a
// restart, and the pod's readiness last moving after that. The kubelet
// starts a restarted runner at once, but the runner serves only once its
// startup is done and its readiness probe passes, so the moment the pod
// turned Ready again is when it is serving again. Zero for a runner on its
// first run with no death recorded, whose readiness is its first serve, not
// a restart. Sidecars and init containers never count; the kubelet restarts
// them in place without touching the serving process.
func runnerRestartedAt(pod *corev1.Pod) time.Time {
	var latest time.Time
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name != constants.MainContainerName {
			continue
		}
		if died := status.State.Terminated; died != nil {
			latest = laterOf(latest, died.FinishedAt.Time)
		}
		if died := status.LastTerminationState.Terminated; died != nil {
			latest = laterOf(latest, died.FinishedAt.Time)
		}
		if running := status.State.Running; running != nil && status.RestartCount > 0 {
			latest = laterOf(latest, running.StartedAt.Time)
		}
	}
	if latest.IsZero() {
		return latest
	}
	return laterOf(latest, readinessMovedAt(pod))
}

// readinessMovedAt is the moment pod's readiness last changed, zero when
// the kubelet has not reported it: for a Ready pod, the moment it began
// serving its current run.
func readinessMovedAt(pod *corev1.Pod) time.Time {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			return pod.Status.Conditions[i].LastTransitionTime.Time
		}
	}
	return time.Time{}
}

// laterOf is the later of two instants; a zero instant is earlier than any.
func laterOf(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// servingAnchor is the moment after which a death of pod's runner is a death
// of a serving Instance: the later of the row's entry into Ready and since.
// It is absent for a row that never served, and for a pod of a repair's
// rebuilt set, whose boot restarts are the ordinary start of a fresh pod. A
// row the workload escalated to Failed for its pod's crash loop still
// speaks for that pod: the escalation is the workload's reading of the same
// deaths, and the pod it left in place carries them.
func servingAnchor(row *v1beta1.OMENativeInstanceStatus, pod *corev1.Pod, since time.Time) (time.Time, bool) {
	if row == nil || row.ReadySince == nil || row.ReadySince.IsZero() {
		return time.Time{}, false
	}
	switch row.Phase {
	case v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceFailed:
	case v1beta1.OMENativeInstanceRestarting:
		if incarnation, ok := query.InstanceIncarnationFromLabels(pod); !ok || incarnation >= row.Incarnation {
			return time.Time{}, false
		}
	default:
		return time.Time{}, false
	}
	anchor := row.ReadySince.Time
	if since.After(anchor) {
		anchor = since
	}
	return anchor, true
}

// runnerCrashLooping reports whether pod's runner container is in a crash
// loop dated after anchor, and names the death. The latest recorded
// termination, the current one or the one before the current run, dates
// the death; a run with no termination recorded is a start, not a death.
// Two faces read as the loop: a death of a run that a restart inside the
// window began, so the pod has died twice since the Instance served,
// whether the kubelet has restarted it again or is backing off; and a
// death of the rebuilt pod of an Instance the workload already rebuilt for
// a death inside the canary's life, the Instance's second death under one
// restart policy's repair. The kubelet's back-off alone names no loop: it
// remembers a crash from before the anchor as well, a boot crash or an
// exit in the second the Instance entered Ready, which neither the
// workload nor this reading dates after the Instance served, so the death
// it backs off from is that run's one death inside the window. Sidecars
// and init containers never count; the kubelet restarts them in place
// without touching the serving process.
func runnerCrashLooping(pod *corev1.Pod, row *v1beta1.OMENativeInstanceStatus, anchor, since time.Time) (string, bool) {
	for i := range pod.Status.ContainerStatuses {
		status := &pod.Status.ContainerStatuses[i]
		if status.Name != constants.MainContainerName {
			continue
		}
		// priorRuns is the index of the run the recorded termination
		// belongs to: the restart count while that run is the current or
		// the awaited one, one less once the kubelet has started the next.
		died := status.State.Terminated
		priorRuns := status.RestartCount
		if died == nil {
			died = status.LastTerminationState.Terminated
			if status.State.Running != nil {
				priorRuns--
			}
		}
		if died == nil || !died.FinishedAt.Time.After(anchor) {
			return "", false
		}
		if priorRuns >= 1 && died.StartedAt.Time.After(anchor) {
			reason := died.Reason
			if waiting := status.State.Waiting; waiting != nil && waiting.Reason == constants.StateReasonCrashLoopBackOff {
				reason = waiting.Reason
			}
			return crashDetail(status.Name, reason, died), true
		}
		if rebuiltForDeathAfter(row, since) && died.FinishedAt.Time.After(row.LastFailure.Time.Time) {
			return crashDetail(status.Name, died.Reason, died), true
		}
		return "", false
	}
	return "", false
}

// rebuiltForDeathAfter reports whether the workload rebuilt row's Instance
// for a pod failure dated after since: the row keeps the termination that
// opened its last failure-driven rebuild, and a rebuild for a pod that
// merely vanished records none. Without an anchor nothing is dated.
func rebuiltForDeathAfter(row *v1beta1.OMENativeInstanceStatus, since time.Time) bool {
	if row == nil || row.LastFailure == nil || since.IsZero() {
		return false
	}
	return row.LastFailure.Time.Time.After(since)
}

// crashDetail names the death for the park's event and record.
func crashDetail(container, reason string, died *corev1.ContainerStateTerminated) string {
	if reason == "" {
		reason = constants.StateReasonError
	}
	if died == nil {
		return fmt.Sprintf("container %s %s", container, reason)
	}
	return fmt.Sprintf("container %s %s (exit %d)", container, reason, died.ExitCode)
}

// memberRetargeted reports whether the pinned run moves comp to a revision
// other than its stable one, so the pods on its target are canary pods. A
// member the run left alone serves its stable revision on its "target", and
// a crash there is the stable side's, not the canary's. A member the run
// recorded no target for is read as retargeted, as the arming guard does.
func memberRetargeted(isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) bool {
	target, ok := activeRunTarget(isvc, comp)
	if !ok {
		return true
	}
	return target.Revision != target.StableRevision
}
