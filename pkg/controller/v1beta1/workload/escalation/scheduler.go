// scheduler.go — the scheduler-hold arm of the escalation
// pass. A pod the scheduler reports it cannot place is waiting on
// cluster capacity, not failing: the hold (holds/scheduler.go) parks the
// InstanceReadyTimeout clock, and only an operator-configured grace
// turns the wait into a terminal failure. This is that grace, and beside
// it the bounded writes that ask the scheduler again for a gang pod left
// Pending with no node (lifecycle.schedulingRetry).
package escalation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// unschedulableGraceElapsed reports whether the hold has outlasted the
// operator's grace. A non-positive grace means the escalation is
// unconfigured — the hold then lasts until an operator resolves it — and
// a zero transition time means the scheduler left no start to measure
// from, which is never treated as "long enough".
func unschedulableGraceElapsed(since metav1.Time, now time.Time, grace time.Duration) bool {
	if grace <= 0 || since.IsZero() {
		return false
	}
	return now.Sub(since.Time) > grace
}

// schedulerHoldGrace is the grace a scheduler hold is measured against.
// When the scheduler keeps a gang in the topology domain where some of its
// members run and that domain cannot take the rest, more capacity
// elsewhere does not help: only removing the placed members frees the
// gang, and ending the attempt does that. Such a hold takes the operator's
// full-domain grace when that one is shorter, unless a pod of the attempt
// is still terminating: a member on its way out may be what fills the
// domain, and its room frees as it goes.
func schedulerHoldGrace(input types.ReconcileInput, message string, pods []*corev1.Pod) time.Duration {
	grace, domainGrace := input.UnschedulableGrace, input.GangDomainFullGrace
	if domainGrace <= 0 || !types.SchedulerReportsGangDomainFull(message) || anyPodTerminating(pods) {
		return grace
	}
	if grace > 0 && grace < domainGrace {
		return grace
	}
	return domainGrace
}

// anyPodTerminating reports whether any pod is being deleted.
func anyPodTerminating(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod != nil && pod.DeletionTimestamp != nil {
			return true
		}
	}
	return false
}

// schedulerHoldSince is the moment a scheduler hold is measured from:
// the attempt's placement wait when the operation records one that began
// earlier, else the unplaceable pod's own PodScheduled transition. The
// attempt's moment spans the members of a gang, so a member placed and
// another then unplaceable is one wait, not two. An operation written
// before the moment existed records none and is measured as before.
//
// So is a gang surge replacement whose source reads Failed at the drain
// step in statuses, the rows the pass read at its start
// (drainSourceEnded). The moment such a row carries belongs to the attempt
// that ended. The update pass may open the next attempt in this same pass,
// before the escalation runs, and that attempt waits on its own clock.
func schedulerHoldSince(statuses []types.InstanceStatus, row types.InstanceStatus, podSince metav1.Time) metav1.Time {
	op := row.Operation
	if op == nil || op.PlacementWaitSince == nil || drainSourceEnded(statuses, row) {
		return podSince
	}
	if podSince.IsZero() || op.PlacementWaitSince.Before(&podSince) {
		return *op.PlacementWaitSince
	}
	return podSince
}

// drainSourceEnded reports whether replacement is the row of a gang surge
// replacement whose source reads Failed at the drain step: the ending was
// written on the source alone, and the replacement's row stays live. The
// hold pass reads the pair the same way: it records no placement wait on
// such a row and retires one the row still carries.
func drainSourceEnded(statuses []types.InstanceStatus, replacement types.InstanceStatus) bool {
	source := gangSurgeSourceOf(statuses, replacement, types.UpdateStepSurgeDrain)
	return source != nil && source.Phase == types.InstancePhaseFailed
}

// unschedulableFailureMessage is the operator-facing summary of a
// scheduler hold: the grep-stable token plus the scheduler's own
// explanation of which constraint had no placement.
func unschedulableFailureMessage(message string) string {
	if message == "" {
		return types.WaitingReasonUnschedulable
	}
	return types.WaitingReasonUnschedulable + ": " + message
}

// escalateSchedulerHold ends an attempt whose pod outlasted the
// operator's placement grace. ENVIRONMENT-CAUSED: the evidence names the
// scheduler's message and the revision's retry ladder is left alone,
// because an unplaceable pod proves nothing about the pod template. The
// route mirrors the deadline backstop: an attempt that takes the deadline
// decision is classified and cleared, a gang keeps its Operation so the
// abandon path can consume the continuation. The hold stands on the
// replacement gang's row, but the attempt it ends is the source's surge,
// which the update pass retires only once the source reads Failed: both
// rows of the pair are ended in the same write, as the deadline ends them.
// Once the replacement has taken over serving, the attempt is the
// source's continuation at the drain step, which retires the refused
// member and rebuilds it as the next attempt: the ending lands on the
// source alone, and the replacement's row stays live so its hold is
// released once the member is gone.
//
// This escalation owns its operator event on BOTH routes. The deadline
// decision suppresses a warning whose reason repeats the one already on
// LastFailure, which is exactly what the hold wrote while it waited, so
// leaving the announcement to it would let the row go Failed with no
// event. Its callback is left out of that call and fired here instead,
// but only when the decision wrote something: it writes nothing for a
// replaced revision's leftover pod or for a row that moved on, and
// announcing a transition that did not happen would re-fire every pass.
func escalateSchedulerHold(ctx context.Context, deps types.Deps, input types.ReconcileInput, deadlineDeps types.DispositionDeps, stamps *failureStampBuffer, row types.InstanceStatus, pods []*corev1.Pod, desired int32, rowEvidence InstanceEvidence, now time.Time) error {
	reason := unschedulableFailureMessage(rowEvidence.UnschedulableMessage)
	idx, podName := row.Index, rowEvidence.Unschedulable.Name
	warn := func() {
		if input.WarnInstanceFailed != nil {
			input.WarnInstanceFailed(idx, podName, reason)
		}
	}
	if takesDeadlineDecision(&row) {
		// The deadline decision's writes are write-ahead-ordered; land the
		// pending plain writes first.
		if err := stamps.flush(ctx); err != nil {
			return err
		}
		silent := input
		silent.WarnInstanceFailed = nil
		decision, err := DecideExpiredAttempt(ctx, deps, silent, deadlineDeps, row, blamedForHold(&row, pods, desired, rowEvidence), reason)
		if err != nil {
			return err
		}
		if decision != DeadlineDecisionSkippedTargetReplaced && decision != DeadlineDecisionHeldBack {
			warn()
		}
		return nil
	}
	termination := types.UnschedulableTermination(rowEvidence.Unschedulable, reason, metav1.NewTime(now))
	ended := status.StampFailedKeepingOperation(termination)
	warnSource := func(sourceIdx int32) func() {
		return func() {
			if input.WarnInstanceFailed != nil {
				input.WarnInstanceFailed(sourceIdx, podName, reason)
			}
		}
	}
	if source := gangSurgeSourceOf(input.ObservedState.InstanceStatuses, row, types.UpdateStepSurgeDrain); source != nil {
		stamps.add(source.Index, ended, warnSource(source.Index))
		return nil
	}
	stamps.add(idx, ended, warn)
	if source := gangSurgeSourceOf(input.ObservedState.InstanceStatuses, row, types.UpdateStepSurge); source != nil {
		stamps.add(source.Index, ended, warnSource(source.Index))
	}
	return nil
}

// gangSurgeSourceOf returns the source row at the named surge step whose
// surge built the replacement gang at replacement's index, or nil when
// replacement is not a live replacement gang's row or no such source
// claims it. At the Surge step the source still serves and the pair is
// ended together; at the drain step the replacement has taken over and
// the source's continuation rebuilds the member that has no placement.
func gangSurgeSourceOf(statuses []types.InstanceStatus, replacement types.InstanceStatus, sourceStep string) *types.InstanceStatus {
	if replacement.Operation == nil || replacement.Operation.Type != types.InstanceOperationUpdate ||
		replacement.Operation.Step != types.UpdateStepGangSurgeTarget {
		return nil
	}
	for i := range statuses {
		op := statuses[i].Operation
		if op != nil && op.Type == types.InstanceOperationUpdate && op.Step == sourceStep &&
			op.SurgeIndex != nil && *op.SurgeIndex == replacement.Index {
			return &statuses[i]
		}
	}
	return nil
}

// blamedForHold narrows the pod set the deadline decision reads. A single-pod
// surge shares its Instance index with the source it is replacing, so
// the blamed set holds both; the hold is about the pod with no placement
// and nothing else. Reading the source's own container failure there
// would route an environment cause into the workload-caused branch and
// charge the target revision a RetryBlock for a shortage of nodes. This
// mirrors how the stuck-pod path narrows to its exact wedged pod.
func blamedForHold(row *types.InstanceStatus, pods []*corev1.Pod, desired int32, rowEvidence InstanceEvidence) []*corev1.Pod {
	if singlePodSurgeAttempt(row, desired) && rowEvidence.Unschedulable != nil {
		return []*corev1.Pod{rowEvidence.Unschedulable}
	}
	return pods
}

// schedulingRetryAnnotation is the pod record of the writes that asked the
// scheduler to look at the pod again.
var schedulingRetryAnnotation = constants.InferenceServicePodSchedulingRetryAnnotationKey

// schedulingRetryRecord is that record: how many times the pod was
// written for the scheduler.
type schedulingRetryRecord struct {
	Count int32 `json:"count"`
}

// schedulingRetryCount reads the record off the pod. A missing or
// unreadable record counts as no write yet.
func schedulingRetryCount(pod *corev1.Pod) int32 {
	raw, ok := pod.Annotations[schedulingRetryAnnotation]
	if !ok {
		return 0
	}
	var rec schedulingRetryRecord
	if err := json.Unmarshal([]byte(raw), &rec); err != nil || rec.Count < 0 {
		return 0
	}
	return rec.Count
}

// waitingForNode reports whether a live pod is still waiting for the
// scheduler: no node, no scheduling gate holding it back, not being
// deleted, and nothing on its status that says it was placed or ran. A
// pod with no phase yet counts as Pending, the phase the apiserver gives
// every new pod.
func waitingForNode(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" || len(pod.Spec.SchedulingGates) > 0 {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed:
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Status != corev1.ConditionTrue {
			continue
		}
		if cond.Type == corev1.PodScheduled || cond.Type == corev1.ContainersReady || cond.Type == corev1.PodReady {
			return false
		}
	}
	return true
}

// nodeWaitStart is when the pod's wait for a node began: its creation,
// or the scheduler's later PodScheduled ruling when there is one. The
// ruling is the later mark, so a pod the scheduler is visibly working on
// is given the whole wait again from that point.
func nodeWaitStart(pod *corev1.Pod) time.Time {
	start := pod.CreationTimestamp.Time
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.LastTransitionTime.After(start) {
			start = cond.LastTransitionTime.Time
		}
	}
	return start
}

// schedulingRetryDue reports whether the next write is due: the k-th
// write waits k times the configured wait from the start of the pod's
// wait, and no write follows the configured limit.
func schedulingRetryDue(policy *types.SchedulingRetryPolicy, count int32, start, now time.Time) bool {
	if policy == nil || policy.After <= 0 || count >= policy.MaxAttempts {
		return false
	}
	return now.Sub(start) > time.Duration(count+1)*policy.After
}

// requestSchedulingAgain writes every pod of the Instance that has waited
// for a node past the configured wait, one record per pod, and tells the
// operator about each write. The write moves the pod's resourceVersion and changes
// nothing the scheduler reads as a placement input: that is what puts a
// pod the scheduler's queue has lost back in its active queue, and what
// leaves a pod the queue still holds exactly where it is.
func requestSchedulingAgain(ctx context.Context, deps types.Deps, input types.ReconcileInput, component types.ComponentType, idx int32, pods []*corev1.Pod, now time.Time) error {
	policy := input.SchedulingRetry
	for _, pod := range pods {
		if !waitingForNode(pod) {
			continue
		}
		count := schedulingRetryCount(pod)
		start := nodeWaitStart(pod)
		if !schedulingRetryDue(policy, count, start, now) {
			continue
		}
		if err := writeSchedulingRetry(ctx, deps.Client, pod, count+1); err != nil {
			return err
		}
		eventSchedulingRetry(deps, input, component, idx, pod, now.Sub(start), count+1, policy.MaxAttempts)
	}
	return nil
}

// writeSchedulingRetry records the write on the pod. The patch names the
// one annotation this pass owns and nothing else, so it never contends
// with the scheduler's own status writes; a pod that is gone needs
// nothing.
func writeSchedulingRetry(ctx context.Context, c client.Client, pod *corev1.Pod, count int32) error {
	if c == nil {
		return fmt.Errorf("ask the scheduler again for pod %s/%s: the pass has no client", pod.Namespace, pod.Name)
	}
	value, err := json.Marshal(schedulingRetryRecord{Count: count})
	if err != nil {
		return fmt.Errorf("marshal scheduling retry record for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	raw, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]string{schedulingRetryAnnotation: string(value)}},
	})
	if err != nil {
		return fmt.Errorf("marshal scheduling retry patch for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Patch(ctx, pod.DeepCopy(), client.RawPatch(apitypes.MergePatchType, raw)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("ask the scheduler again for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// eventSchedulingRetry tells the operator which pod was written, how long
// it had waited, and how many writes remain.
func eventSchedulingRetry(deps types.Deps, input types.ReconcileInput, component types.ComponentType, idx int32, pod *corev1.Pod, waited time.Duration, attempt, limit int32) {
	types.RecordNormal(deps.Recorder, types.EventTarget(input), types.EventReasonSchedulingRequestedAgain,
		"OMENative %s pod %s has waited %s for a node; the pod was written so the scheduler looks at it again (%d of %d)",
		types.InstanceKey(component, idx), pod.Name, waited.Truncate(time.Second), attempt, limit)
}
