package workload

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DeadlineExceededReason is the LastFailure.Reason stamped when an
// Instance is failed because its in-flight Operation.Deadline elapsed
// and no blamed pod shows a workload-caused waiting reason (that reason
// is recorded instead — see escalateFromEvidence). Distinct from the
// kubelet waiting-state reasons the stuck-pod escalator records so
// operators can tell a broad-timeout backstop ("gang never became
// Ready") from a terminal-state escalation ("pod stuck in
// CrashLoopBackOff").
const DeadlineExceededReason = "DeadlineExceeded"

// operationDeadlinePassed reports whether s is in a transient phase with
// an in-flight Operation whose Deadline lies in the past. A zero
// Deadline (the metav1.Time zero value) is treated as "never expires" so
// an unset field can't trip the timeout — see plan.go's
// InstanceReadyTimeoutOrDefault for where the deadline window comes from.
func operationDeadlinePassed(s *InstanceStatus, now time.Time) bool {
	if s == nil || s.Operation == nil {
		return false
	}
	if s.Operation.Deadline.IsZero() {
		return false
	}
	if !isTransientPhase(s.Phase) {
		return false
	}
	return now.After(s.Operation.Deadline.Time)
}

// isTransientPhase reports whether the phase is an in-flight operation
// phase the InstanceReadyTimeout backstop bounds (Creating / Updating /
// Restarting / Migrating). Terminal phases (Ready / Failed / Deleting)
// and the empty zero value are not.
func isTransientPhase(p InstancePhase) bool {
	switch p {
	case InstancePhaseCreating,
		InstancePhaseUpdating,
		InstancePhaseRestarting,
		InstancePhaseMigrating:
		return true
	default:
		return false
	}
}

// PodAdmissionGated reports whether a pod is still held by a scheduling
// gate — queued for admission (e.g. by Kueue) and not yet allowed to run.
// A gated pod is waiting on an external admission authority, not stuck, so
// the InstanceReadyTimeout clock must not run while it is gated.
func PodAdmissionGated(pod *corev1.Pod) bool {
	return pod != nil && len(pod.Spec.SchedulingGates) > 0
}

// ReconcileGatedDeadlines keeps InstanceReadyTimeout measured from
// admission, not from operation start, so a workload an external
// authority legitimately holds queued is not failed by the deadline
// backstop (the escalation pass additionally skips gated Instances
// outright, covering the window before a gate-enter is parked).
//
// Two waits qualify, both external to the workload:
//   - a pod still carrying an admission scheduling gate (e.g. Kueue) —
//     supplied by the caller in `gated`, which maps Instance index ->
//     "any of its pods is gated";
//   - an operation parked on the capacity-blocked waiting token
//     (instanceCapacityBlocked): admission refused the create for lack of
//     quota, so there is no pod to carry a gate and the create path
//     records the wait on the Operation instead — on the surge row for a
//     gang surge, which the source is held by in turn.
//
// For each transient-phase Instance with an in-flight Operation:
//   - held    -> PARK: zero the deadline (the "never expires" sentinel
//     the deadline predicate already honors). The clock pauses.
//   - released with a parked (zero) deadline -> RESTART: now+timeout. The
//     clock starts from admission.
//   - released with a live (non-zero) deadline -> untouched, so the
//     no-hold path behaves exactly as the per-op writer stamped it.
//
// Edge-triggered: it writes only on the hold-enter (park) and hold-exit
// (restart) transitions — a workload queued for hours does not churn its
// status.
func ReconcileGatedDeadlines(ctx context.Context, input ReconcileInput, instances []InstanceStatus, gated map[int32]bool, timeout time.Duration) error {
	now := input.Now()
	for i := range instances {
		s := &instances[i]
		if s.Operation == nil || !isTransientPhase(s.Phase) {
			continue
		}
		held := gated[s.Index] || instanceCapacityBlocked(s, instances)
		switch {
		case held && !s.Operation.Deadline.IsZero():
			if err := setInstanceDeadline(ctx, input, s.Index, metav1.Time{}); err != nil {
				return fmt.Errorf("park deadline for held instance %d: %w", s.Index, err)
			}
		case !held && s.Operation.Deadline.IsZero():
			if err := setInstanceDeadline(ctx, input, s.Index, metav1.NewTime(now.Add(timeout))); err != nil {
				return fmt.Errorf("restart deadline for released instance %d: %w", s.Index, err)
			}
		}
	}
	return nil
}

// instanceCapacityBlocked reports whether s's operation is waiting on
// capacity — its own waiting token, or the one on the surge row its
// Operation.SurgeIndex points at. The surge indirection matters because a
// gang surge creates pods under the SURGE index while the governing
// operation and its deadline live on the SOURCE: without it, the source's
// clock would keep running through a wait its own replacement is stuck
// in. Mirrors how an admission-gated surge bucket parks its source.
func instanceCapacityBlocked(s *InstanceStatus, statuses []InstanceStatus) bool {
	if s == nil || s.Operation == nil {
		return false
	}
	if OperationCapacityBlocked(s.Operation) {
		return true
	}
	if s.Operation.SurgeIndex == nil {
		return false
	}
	for i := range statuses {
		if statuses[i].Index == *s.Operation.SurgeIndex {
			return OperationCapacityBlocked(statuses[i].Operation)
		}
	}
	return false
}

// setInstanceDeadline writes Operation.Deadline via MutateInstance,
// preserving the rest of the Operation. No-op when the slot is gone, has
// no Operation, or already holds the target deadline.
func setInstanceDeadline(ctx context.Context, input ReconcileInput, idx int32, deadline metav1.Time) error {
	return input.MutateInstance(ctx, idx, func(s *InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil {
			return false
		}
		if s.Operation.Deadline.Time.Equal(deadline.Time) {
			return false
		}
		s.Operation.Deadline = deadline
		return true
	})
}

// deadlineFailureMessage formats the operator-facing reason passed to
// WarnInstanceFailed. Names the operation type + step that timed out so
// `kubectl describe` points at what was in flight.
func deadlineFailureMessage(op *InstanceOperation) string {
	if op == nil {
		return DeadlineExceededReason
	}
	return fmt.Sprintf("%s: %s/%s exceeded InstanceReadyTimeout", DeadlineExceededReason, op.Type, op.Step)
}

// deadlineTermination is the LastFailure record for an elapsed Operation
// deadline with no more specific evidence: the DeadlineExceeded reason and
// the operator-facing message naming what was in flight.
func deadlineTermination(now time.Time, op *InstanceOperation) *InstanceTermination {
	return &InstanceTermination{
		Reason:  DeadlineExceededReason,
		Message: deadlineFailureMessage(op),
		Time:    metav1.NewTime(now),
	}
}

// deadlineFailedMutation builds the Phase=Failed stamp for an elapsed
// Operation deadline, recording termination on LastFailure. Mirrors
// stuckPodFailedMutation's guard logic: a fresh-empty slot (Phase=="")
// from the writer's append path is a sentinel for a slot deleted out
// from under us (don't resurrect), an already-Failed slot is a no-op,
// and any other phase flips to Failed. A slot whose Operation is already
// gone is also a no-op: the attempt concluded (or was disposed) since the
// deadline was observed, so there is nothing in flight left to expire.
//
// The Operation is preserved — operators want to see WHAT was in flight
// when the deadline elapsed.
func deadlineFailedMutation(termination *InstanceTermination) func(*InstanceStatus) bool {
	return func(s *InstanceStatus) bool {
		if s.Phase == "" {
			return false
		}
		if s.Phase == InstancePhaseFailed {
			return false
		}
		if s.Operation == nil {
			return false
		}
		s.Phase = InstancePhaseFailed
		captured := *termination
		s.LastFailure = &captured
		return true
	}
}
