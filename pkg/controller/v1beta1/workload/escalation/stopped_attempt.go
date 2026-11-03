// stopped_attempt.go — the phase of an attempt the escalation stopped. The
// deadline decision ends a recreate or in-place attempt whose pod set still
// runs by stopping it: the operation stays on the row at step Parked with
// the wait the roll's next attempt stands behind, and no pass drives it
// until that wait ends. The set it left stays with the kubelet — a runner
// that died after serving is restarted in place and serves between crashes
// — so the row's phase is read off the set on every pass: Updating once the
// full set serves, Failed once it is gone or serves nothing past the
// stuck-pod grace, and unchanged while a restart in place is inside it.
package escalation

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// FollowStoppedAttempts gives every stopped attempt the phase its pod set
// earns this pass and the wait its next attempt stands behind now. A
// stopped attempt is one the escalation ended while its pods still ran,
// kept on the row at step Parked. Status truth about a set no pass drives,
// so it runs on paused reconciles too. A row the scale-down wave holds back
// is left to the wave. The pod read is made only when a stopped attempt is
// observed, and the row is written only when the observation says the
// phase or the wait moves.
func FollowStoppedAttempts(ctx context.Context, in PassInput) error {
	input, plan := in.Input, in.Plan
	var stopped []types.InstanceStatus
	for _, row := range input.ObservedState.InstanceStatuses {
		if _, skip := in.Excluded[row.Index]; skip {
			continue
		}
		if types.OperationParked(row.Operation) {
			stopped = append(stopped, row)
		}
	}
	if len(stopped) == 0 {
		return nil
	}
	byIdx, err := in.Pods(ctx)
	if err != nil {
		return fmt.Errorf("workload.Reconcile: list pods to follow parked attempts (component=%s): %w", plan.Component, err)
	}
	desiredByIdx := status.DesiredPodCountByInstance(plan)
	wait := stoppedAttemptWait(input, nextAttemptRevision(in))
	now := input.Now()
	for _, row := range stopped {
		own := evidence.AttemptStuckPods(row, byIdx[row.Index], row.Operation.TargetRevision)
		desired := status.DesiredFor(desiredByIdx, row.Index, row.PodCount)
		phase := stoppedAttemptPhase(row.Phase, own, desired, now, input.StuckPodGrace)
		if row.Phase == phase && (wait == "" || row.Operation.Waiting == wait) {
			continue
		}
		if _, err := status.ApplyStamp(ctx, input, row.Index, status.FollowStoppedAttempt(phase == types.InstancePhaseUpdating, wait)); err != nil {
			return fmt.Errorf("follow parked attempt (instance=%d): %w", row.Index, err)
		}
	}
	return nil
}

// stoppedAttemptPhase is the phase a stopped attempt's own pod set earns:
// Updating once the full set serves; Failed once no live pod is left or the
// set serves nothing — no routed member in rotation, and every live one in
// a terminal waiting reason or unready past the stuck-pod grace; and
// otherwise the phase the row read before, so a runner the kubelet restarts
// in place inside the grace moves nothing.
func stoppedAttemptPhase(was types.InstancePhase, own []*corev1.Pod, desired int32, now time.Time, grace time.Duration) types.InstancePhase {
	switch {
	case query.PodSetFullyServing(own, desired):
		return types.InstancePhaseUpdating
	case !anyLivePod(own) || evidence.PodSetServesNothing(own, now, grace):
		return types.InstancePhaseFailed
	}
	return was
}

// anyLivePod reports whether a pod of the set is not on its way out.
func anyLivePod(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if pod != nil && pod.DeletionTimestamp == nil {
			return true
		}
	}
	return false
}

// nextAttemptRevision is the revision a stopped attempt's next attempt is
// at: the reconcile's roll target, else the owner's update revision. A
// pass with neither has no roll to wait on.
func nextAttemptRevision(in PassInput) string {
	if in.Target != nil {
		return in.Target.Name
	}
	return in.Input.ObservedState.UpdateRevision
}

// stoppedAttemptWait is the wait a stopped attempt names this pass: the
// hold when the retry record of the next attempt's revision is held, the
// retry schedule while it denies a start, and otherwise whatever the update
// pass last named — the budget or gate that denied the start the schedule
// admitted — read as "" to leave it. A row whose schedule admits with no
// record left keeps that last token: it records what last denied the
// start, and the update trigger re-opens the row regardless of it.
func stoppedAttemptWait(input types.ReconcileInput, rev string) string {
	if rev == "" {
		return ""
	}
	block := types.FindRetryBlock(input.ObservedState.RetryBlocks, rev)
	switch {
	case block != nil && block.State == types.RetryBlockHeld:
		return string(types.RolloutHoldGateHeld)
	case workloadops.LadderDeniesStart(input, rev):
		return string(types.RolloutHoldGateRetryBlock)
	}
	return ""
}
