// scheduler.go — the scheduler-hold arm of the escalation
// pass. A pod the scheduler reports it cannot place is waiting on
// cluster capacity, not failing: the hold (holds/scheduler.go) parks the
// InstanceReadyTimeout clock, and only an operator-configured grace
// turns the wait into a terminal failure. This is that grace.
package escalation

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
// route mirrors the deadline backstop — a disposable attempt is
// classified and cleared, a gang keeps its Operation so the abandon path
// can consume the continuation.
//
// This escalation owns its operator event on BOTH routes. The
// disposition suppresses a warning whose reason repeats the one already
// on LastFailure, which is exactly what the hold wrote while it waited,
// so leaving the announcement to it would let the row go Failed in
// silence. Its callback is withheld for that call and fired here
// instead — but only when the disposition actually wrote something. It
// declines outright for a superseded leftover, and announcing a
// transition that did not happen would re-fire every pass.
func escalateSchedulerHold(ctx context.Context, deps types.Deps, input types.ReconcileInput, disposition types.DispositionDeps, stamps *failureStampBuffer, row types.InstanceStatus, pods []*corev1.Pod, desired int32, rowEvidence InstanceEvidence, now time.Time) error {
	reason := unschedulableFailureMessage(rowEvidence.UnschedulableMessage)
	idx, podName := row.Index, rowEvidence.Unschedulable.Name
	warn := func() {
		if input.WarnInstanceFailed != nil {
			input.WarnInstanceFailed(idx, podName, reason)
		}
	}
	if disposableAttempt(&row, desired) {
		// The disposition's writes are write-ahead-ordered; land the
		// pending plain stamps first.
		if err := stamps.flush(ctx); err != nil {
			return err
		}
		silent := input
		silent.WarnInstanceFailed = nil
		outcome, err := DisposeExpiredAttempt(ctx, deps, silent, disposition, row, blamedForHold(&row, pods, desired, rowEvidence), reason)
		if err != nil {
			return err
		}
		if outcome != DispositionSkippedSuperseded {
			warn()
		}
		return nil
	}
	termination := types.UnschedulableTermination(rowEvidence.Unschedulable, reason, metav1.NewTime(now))
	stamps.add(idx, status.StampFailedKeepingOperation(termination), warn)
	return nil
}

// blamedForHold narrows the pod set the disposition judges. A single-pod
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
