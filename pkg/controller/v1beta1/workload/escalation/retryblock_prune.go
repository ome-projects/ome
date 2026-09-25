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

	appsv1 "k8s.io/api/apps/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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
	for i := range input.ObservedState.InstanceStatuses {
		if op := input.ObservedState.InstanceStatuses[i].Operation; op != nil {
			keep(op.TargetRevision)
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
