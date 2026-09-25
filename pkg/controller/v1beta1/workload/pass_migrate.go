package workload

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// executeMigratePass drives the Decision's one Manual migration record.
// Migrate's third return (accepted) distinguishes two done=false modes:
//
//   - accepted=true: the migration is mid-flight (the record carries the
//     surge index, statuses stamped). The record paces the pair at the
//     Migrate interval and the passes after it have no work on a row the
//     record owns, so the pass ends here rather than letting a later
//     action set the wake-up.
//   - accepted=false: the migration was deferred without taking
//     ownership (fresh record, source not yet steady-Ready because of an
//     in-flight Update/Restart/Create). The pass continues to
//     Update/Create so the in-flight op converges; the next reconcile
//     re-picks the same record against a steady-Ready source. Without
//     the fall-through the dispatcher loops at the Migrate interval and
//     the in-flight op never runs.
//
// stop=true means the pass consumed the reconcile: either the record
// paces it, or Migrate completed and the plan is stale. A completed
// Migrate removed the source InstanceStatus and promoted the surge to
// Ready (or wrote a terminal Failed), and the plan was computed on the
// pre-migration view, so the passes after it would recreate the
// source-side index; the next reconcile rebuilds the plan from the
// post-migration status.
func executeMigratePass(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, record types.MigrationRecord) (res ctrl.Result, stop bool, err error) {
	// Reconstruct the executor's request view from the record — the
	// annotation was consumed at accept time.
	request := &audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       string(plan.Component),
		Instance:        record.SourceInstance,
		FromNode:        record.FromNode,
		HintTargetNodes: append([]string(nil), record.HintTargetNodes...),
		Reason:          record.Reason,
	}
	sourceIdx := record.SourceInstance
	done, accepted, migrateErr := workloadops.Migrate(ctx, deps, input, plan, sourceIdx, record.RequestUUID, request)
	if migrateErr != nil {
		return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: migrate instance %d: %w", sourceIdx, migrateErr)
	}
	if !done && accepted {
		return types.PassRequeue(workloadops.MigrateRequeueInterval(input)), true, nil
	}
	if done {
		return types.RequeueNow(), true, nil
	}
	return ctrl.Result{}, false, nil
}
