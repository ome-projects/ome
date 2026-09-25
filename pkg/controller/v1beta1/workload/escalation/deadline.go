package escalation

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// DeadlineExceededReason is the LastFailure.Reason stamped when an
// Instance is failed because its in-flight Operation.Deadline elapsed
// and no blamed pod shows a workload-caused waiting reason (that reason
// is recorded instead — see Run). Distinct from the
// kubelet waiting-state reasons the stuck-pod escalator records so
// operators can tell a broad-timeout backstop ("gang never became
// Ready") from a terminal-state escalation ("pod stuck in
// CrashLoopBackOff").
const DeadlineExceededReason = "DeadlineExceeded"

// operationDeadlinePassed reports whether s is in a transient phase with
// an in-flight Operation whose Deadline lies in the past. A zero
// Deadline (the metav1.Time zero value) is treated as "never expires" so
// an unset field can't trip the timeout — see plan.go's
// ResolveInstanceReadyTimeout for where the deadline window comes from,
// and types.DeadlineAt for the unconfigured case that stamps the zero.
func operationDeadlinePassed(s *types.InstanceStatus, now time.Time) bool {
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

// isTransientPhase is types.TransientPhase under the name the deadline
// backstop's guards read by.
func isTransientPhase(p types.InstancePhase) bool {
	return types.TransientPhase(p)
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
//   - an operation parked on an external waiting token
//     (instanceHeldExternally): admission refused the create for lack of
//     quota, or the scheduler found no placement for the pods. Neither
//     wait can be read off a scheduling gate — the quota refusal leaves
//     no pod at all — so it is recorded on the Operation instead, on the
//     surge row for a gang surge, which the source is held by in turn.
//
// For each transient-phase Instance with an in-flight Operation:
//   - held    -> PARK: zero the deadline (the "never expires" sentinel
//     the deadline predicate already honors). The clock pauses.
//   - released with a parked (zero) deadline -> RESTART: now+timeout. The
//     clock starts from admission.
//   - released with a live (non-zero) deadline -> untouched, so the
//     no-hold path behaves exactly as the per-op writer stamped it.
//
// A non-positive timeout means no readiness window is configured at either
// level, so there is no clock to restart and the zero deadline stands.
//
// Edge-triggered: it writes only on the hold-enter (park) and hold-exit
// (restart) transitions — a workload queued for hours does not churn its
// status.
func ReconcileGatedDeadlines(ctx context.Context, input types.ReconcileInput, instances []types.InstanceStatus, gated map[int32]bool, timeout time.Duration) error {
	now := input.Now()
	for i := range instances {
		s := &instances[i]
		if s.Operation == nil || !isTransientPhase(s.Phase) {
			continue
		}
		held := gated[s.Index] || instanceHeldExternally(s, instances)
		switch {
		case held && !s.Operation.Deadline.IsZero():
			if err := status.StampDeadline(ctx, input, s.Index, metav1.Time{}); err != nil {
				return fmt.Errorf("park deadline for held instance %d: %w", s.Index, err)
			}
		case !held && s.Operation.Deadline.IsZero() && timeout > 0:
			if err := status.StampDeadline(ctx, input, s.Index, types.DeadlineAt(metav1.NewTime(now), timeout)); err != nil {
				return fmt.Errorf("restart deadline for released instance %d: %w", s.Index, err)
			}
		}
	}
	return nil
}

// instanceHeldExternally reports whether s's operation is waiting on
// something outside the workload's control — quota the apiserver refused
// it, or a placement the scheduler could not find. It reads its own
// waiting token, or the one on the surge row its Operation.SurgeIndex
// points at. The surge indirection matters because a gang surge creates
// pods under the SURGE index while the governing operation and its
// deadline live on the SOURCE: without it, the source's clock would keep
// running through a wait its own replacement is stuck in. Mirrors how an
// admission-gated surge bucket parks its source.
func instanceHeldExternally(s *types.InstanceStatus, statuses []types.InstanceStatus) bool {
	if s == nil || s.Operation == nil {
		return false
	}
	if types.OperationExternallyHeld(s.Operation) {
		return true
	}
	if s.Operation.SurgeIndex == nil {
		return false
	}
	for i := range statuses {
		if statuses[i].Index == *s.Operation.SurgeIndex {
			return types.OperationExternallyHeld(statuses[i].Operation)
		}
	}
	return false
}

// deadlineFailureMessage formats the operator-facing reason passed to
// WarnInstanceFailed. Names the operation type + step that timed out so
// `kubectl describe` points at what was in flight.
func deadlineFailureMessage(op *types.InstanceOperation) string {
	if op == nil {
		return DeadlineExceededReason
	}
	return fmt.Sprintf("%s: %s/%s exceeded InstanceReadyTimeout", DeadlineExceededReason, op.Type, op.Step)
}

// deadlineTermination is the LastFailure record for an elapsed Operation
// deadline with no more specific evidence: the DeadlineExceeded reason and
// the operator-facing message naming what was in flight.
func deadlineTermination(now time.Time, op *types.InstanceOperation) *types.InstanceTermination {
	return &types.InstanceTermination{
		Reason:  DeadlineExceededReason,
		Message: deadlineFailureMessage(op),
		Time:    metav1.NewTime(now),
	}
}
