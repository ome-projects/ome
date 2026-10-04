package workload

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// executeMigratePass runs the migrate pass over the Decision's
// selection: the records parked on their source teardown are tended
// first, oldest first, then the head record is driven. Each record goes
// through Migrate, whose third return (accepted) distinguishes two
// done=false modes:
//
//   - accepted=true: the migration is mid-flight (the record carries the
//     surge index, statuses stamped). The record paces the pair at the
//     Migrate interval and the passes after it have no work on a row the
//     record owns, so the pass ends here rather than letting a later
//     action set the wake-up. A parked record in flight ends the pass the
//     same way, but only after the head has been driven: its drive only
//     re-checks a source teardown the kubelet owns, so it is never what
//     keeps the head waiting.
//   - accepted=false: the migration was deferred without taking
//     ownership (fresh record, source not yet steady-Ready because of an
//     in-flight Update/Restart/Create). The pass continues to
//     Update/Create so the in-flight op converges; the next reconcile
//     re-picks the same record against a steady-Ready source. Without
//     the fall-through the dispatcher loops at the Migrate interval and
//     the in-flight op never runs.
//
// stop=true means the pass consumed the reconcile: either a record
// paces it, or Migrate completed and the plan is stale. A completed
// Migrate removed the source InstanceStatus and promoted the surge to
// Ready (or wrote a terminal Failed), and the plan was computed on the
// pre-migration view, so the passes after it would recreate the
// source-side index; the next reconcile rebuilds the plan from the
// post-migration status.
func executeMigratePass(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, selection MigrationSelection) (res ctrl.Result, stop bool, err error) {
	pacing := false
	for _, record := range selection.Parked {
		done, accepted, err := driveMigration(ctx, deps, input, plan, target, record)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		if done {
			return types.RequeueNow(), true, nil
		}
		pacing = pacing || accepted
	}
	if selection.Head != nil {
		done, accepted, err := driveMigration(ctx, deps, input, plan, target, *selection.Head)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		if done {
			return types.RequeueNow(), true, nil
		}
		pacing = pacing || accepted
	}
	if pacing {
		return types.PassRequeue(workloadops.MigrateRequeueInterval(input)), true, nil
	}
	return ctrl.Result{}, false, nil
}

// driveMigration runs one Migrate step for record, reconstructing the
// executor's request view from it — the annotation was consumed at
// accept time.
func driveMigration(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, record types.MigrationRecord) (done, accepted bool, err error) {
	request := &audit.MigrationRequest{
		SchemaVersion:   audit.SchemaV1,
		Component:       string(plan.Component),
		Instance:        record.SourceInstance,
		FromNode:        record.FromNode,
		HintTargetNodes: append([]string(nil), record.HintTargetNodes...),
		Reason:          record.Reason,
	}
	sourceIdx := record.SourceInstance
	done, accepted, err = workloadops.Migrate(ctx, deps, input, plan, target, sourceIdx, record.RequestUUID, request)
	if err != nil {
		return false, false, fmt.Errorf("workload.Reconcile: migrate instance %d: %w", sourceIdx, err)
	}
	return done, accepted, nil
}
