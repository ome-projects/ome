// retryblock_prune.go — the supersede-prune GC for persisted
// RetryBlocks. A RetryBlock is revision-scoped, and the gate only ever
// consults the block for a revision some path still resolves as a
// target. A block whose revision is fully superseded is therefore dead
// weight at best — and a live hazard at worst: stale per-Instance
// state (e.g. abandoned-surge wreckage) can resolve the superseded
// revision as an instance's effective target again, and the lingering
// block then denies the very recovery a corrective revision was
// published to unblock. Removing blocks as soon as their revision is
// superseded caps that exposure.
package escalation

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// pruneSupersededRetryBlocks removes every RetryBlock whose
// TargetRevision names none of the revisions still in play:
//
//   - ObservedState.CurrentRevision (the last fully-rolled revision),
//   - ObservedState.UpdateRevision (the spec target),
//   - this reconcile's roll target (diverges from the spec target
//     during a canary rollback),
//   - any Instance's in-flight Operation.TargetRevision (an attempt
//     still resolving toward that revision keeps its block live).
//
// Each removal is a standalone status write nothing later in the pass
// depends on, so the prune runs at the end of the reconcile, after the
// op passes and the escalation flush, with no write-ahead-ordering
// obligations. The caller gates it with the escalation pass: never
// under Teardown or Paused. One V(1) line names the pruned revisions.
func PruneSupersededRetryBlocks(ctx context.Context, input types.ReconcileInput, target *appsv1.ControllerRevision) error {
	if input.MutateRetryBlock == nil || len(input.ObservedState.RetryBlocks) == 0 {
		return nil
	}
	live := make(map[string]struct{}, 4)
	keep := func(rev string) {
		if rev != "" {
			live[rev] = struct{}{}
		}
	}
	keep(input.ObservedState.CurrentRevision)
	keep(input.ObservedState.UpdateRevision)
	if target != nil {
		keep(target.Name)
	}
	// Only an operation that still claims its verb references its
	// revision. A parked attempt's pin is history: its next attempt is the
	// roll's at the target, so the block it stood behind goes with the
	// revision it names once that revision is superseded.
	for i := range input.ObservedState.InstanceStatuses {
		row := &input.ObservedState.InstanceStatuses[i]
		if types.ClaimOf(row) != types.OwnerNone {
			keep(row.Operation.TargetRevision)
		}
	}
	var pruned []string
	for i := range input.ObservedState.RetryBlocks {
		rev := input.ObservedState.RetryBlocks[i].TargetRevision
		if rev == "" {
			continue
		}
		if _, ok := live[rev]; ok {
			continue
		}
		if err := input.MutateRetryBlock(ctx, rev, func(*types.RetryBlock) types.RetryBlockDisposition {
			return types.RetryBlockRemove
		}); err != nil {
			return fmt.Errorf("prune superseded retry block (rev=%s): %w", rev, err)
		}
		pruned = append(pruned, rev)
	}
	if len(pruned) > 0 {
		logf.FromContext(ctx).V(1).Info("Pruned RetryBlocks for superseded revisions",
			"component", input.Key.Component, "revisions", pruned)
	}
	return nil
}

// PruneOutlivedRetryBlocks removes the RetryBlock of a revision whose
// rows have outlived the failure it records: every row that runs the
// revision, or has an attempt pinned to it, is Ready on it with no
// operation open and holds the bar a rolled Instance must hold to count
// as serving again (ops.RolledInstanceNotServing): every pod Ready and
// in rotation, a runner that restarted since the row entered Ready back
// and Available for the window, none restarted twice. The window is the
// Component's minReadySeconds and no shorter than the stuck-pod grace,
// the age below which a crash is not yet read as a wedge: a pod that has
// not outlived the grace has not yet stood where its crash would count.
// A Held block is never pruned here: the ladder holds until a different
// target revision arrives or an operator releases it.
//
// A crash of a promoted pod set counts on the revision's ladder, and its
// rebuild promotes without pruning the block (status.StampReady), so this
// is where a crash that never came back costs its one attempt and clears,
// while a loop, whose rebuilt pods never hold the window, exhausts the
// ladder. Runs with the supersede-prune, at the end of the reconcile.
func PruneOutlivedRetryBlocks(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, pods func(context.Context) (map[int32][]*corev1.Pod, error)) error {
	if input.MutateRetryBlock == nil || len(input.ObservedState.RetryBlocks) == 0 {
		return nil
	}
	window := plan.MinReadySeconds
	if grace := int32(input.StuckPodGrace / time.Second); grace > window {
		window = grace
	}
	now := input.Now()
	rows := input.ObservedState.InstanceStatuses
	desiredByIdx := status.DesiredPodCountByInstance(plan)
	var byIdx map[int32][]*corev1.Pod
	var pruned []string
	for i := range input.ObservedState.RetryBlocks {
		block := &input.ObservedState.RetryBlocks[i]
		rev := block.TargetRevision
		if rev == "" || block.State == types.RetryBlockHeld {
			continue
		}
		if byIdx == nil {
			var err error
			if byIdx, err = pods(ctx); err != nil {
				return fmt.Errorf("list pods to prune outlived retry blocks (component=%s): %w", plan.Component, err)
			}
		}
		// Only the roll target's birth is known here; a block of any other
		// revision is superseded and the supersede-prune removes it.
		born := time.Time{}
		if target != nil && rev == target.Name {
			born = target.CreationTimestamp.Time
		}
		if !rowsOutlived(rows, byIdx, desiredByIdx, rev, born, window, now, plan.RestartPolicy) {
			continue
		}
		if err := input.MutateRetryBlock(ctx, rev, func(*types.RetryBlock) types.RetryBlockDisposition {
			return types.RetryBlockRemove
		}); err != nil {
			return fmt.Errorf("prune outlived retry block (rev=%s): %w", rev, err)
		}
		pruned = append(pruned, rev)
	}
	if len(pruned) > 0 {
		logf.FromContext(ctx).V(1).Info("Pruned RetryBlocks whose rows outlived the failure",
			"component", input.Key.Component, "revisions", pruned)
	}
	return nil
}

// rowsOutlived reports whether at least one row runs rev and every row
// that runs it, or has an attempt pinned to it, is Ready on it with no
// operation open, has been Ready for window holding the serving bar, and
// owes no rebuild: under RecreateInstanceOnPodRestart a runner that
// restarted since the row entered Ready is rebuilt, and the block paces
// that rebuild until it opens.
func rowsOutlived(rows []types.InstanceStatus, byIdx map[int32][]*corev1.Pod, desiredByIdx map[int32]int32, rev string, revisionBorn time.Time, window int32, now time.Time, policy types.RestartPolicy) bool {
	held := 0
	for i := range rows {
		row := &rows[i]
		pinned := row.Operation != nil && row.Operation.TargetRevision == rev
		if row.RunningRevision != rev && row.TargetRevision != rev && !pinned {
			continue
		}
		if row.Phase != types.InstancePhaseReady || row.Operation != nil || row.RunningRevision != rev {
			return false
		}
		if row.ReadySince == nil || now.Sub(row.ReadySince.Time) < time.Duration(window)*time.Second {
			return false
		}
		desired := status.DesiredFor(desiredByIdx, row.Index, row.PodCount)
		if notServing, _, _ := workloadops.RolledInstanceNotServing(row, rev, revisionBorn, byIdx[row.Index], desired, window, now); notServing {
			return false
		}
		if policy == types.RestartPolicyRecreateInstance {
			for _, pod := range byIdx[row.Index] {
				if _, restarted := workloadops.RunnerRestartedSinceReady(pod, row.ReadySince); restarted {
					return false
				}
			}
		}
		held++
	}
	return held > 0
}
