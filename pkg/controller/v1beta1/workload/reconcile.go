// Workload-side per-Component dispatcher. Reconcile is the single entry
// point the ISVC OMENative dispatcher and the InferenceReplica
// controller call into; it drives one logical workload (one Component
// of one owner) through the Create / Update / Restart / Migrate /
// scale-down pipelines by way of the workload/ops state machines.
//
// What this dispatcher does NOT own (the caller runs these around
// Reconcile):
//   - PodMonitor — handled by the top-level podmonitor reconciler.
//   - ensureRevisionWithCollisionRetry — ISVC-shape collision-counter
//     bookkeeping; the caller computes the target ControllerRevision
//     and passes it in.
//   - AggregateAndWriteStatus — ISVC-shape counters + top-level
//     EngineReady / DecoderReady / RouterReady condition.
//   - service.ReconcileHeadlessService — invoked by the caller before
//     Reconcile so both the ISVC adapter and the IR adapter can share
//     the Service renderer.
//
// Cross-Component coordination gates reach the dispatcher via the
// UpdateGate callback on ReconcileInput; the workload package itself
// never imports `inferenceservice/reconcilers/omenative/coordination/`.
package workload

import (
	"context"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/holds"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Reconcile drives one workload (one Component of one owner) toward its
// desired state. The caller constructs a fully populated ReconcileInput
// + Deps + plan + target and calls Reconcile once per per-Component
// reconcile pass.
//
// Shape: observe → plan → execute. One ObservedSnapshot is built up
// front; Plan (plan_decision.go) evaluates every pass trigger against
// it and produces the Decision; Execute applies the Decision through
// the workload/ops state machines and runs the escalation pass.
//
// Execute runs the hold pass, then the op actions in the order
// executeActions numbers them (scale-down, demotion, restart, migration
// expiry, migration, update, create), then escalation. Which pass may
// advance a row is the ownership table's answer, never the order's.
//
// target may be nil when DesiredSpec.PodSpec is nil (MinReplicas=0).
// Restart / Update passes short-circuit on nil target; Create returns
// immediately.
func Reconcile(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if deps.Client == nil {
		return ctrl.Result{}, fmt.Errorf("workload.Reconcile: nil client (component=%s)", plan.Component)
	}
	// One pacing sink per pass. The op state machines report progress as
	// (done, error) and a throttled write is neither, so the server's
	// suggested delay is deposited here and floors the wake-up below.
	if input.Pacing == nil {
		input.Pacing = &types.APIPacing{}
	}
	// One promote-window sink per pass, for the same reason: a pod crossing
	// its minReadySeconds window raises no watch event, so a promote waiting
	// on it deposits the remainder here and wakes the pass below.
	if input.PromoteWindow == nil {
		input.PromoteWindow = &types.PromoteWindow{}
	}
	// One held-work sink per pass, for the same reason: an operator
	// supplying a missing configuration key raises no watch event, so a
	// pass that held work on it deposits its wake-up here.
	if input.PassWake == nil {
		input.PassWake = &types.PassWake{}
	}

	// Teardown: the owner is being deleted, so every observed Instance is
	// a scale-down extra and nothing else runs. The caller owns completion
	// detection and finalizer decisions.
	if input.Teardown {
		return reconcileTeardown(ctx, deps, input, plan)
	}

	// Single observation for this reconcile: pod reads are lazy + memoized
	// per source (live for destructive planning, cached otherwise), so each
	// source is Listed at most once and only when a pass needs it.
	snapshot := NewObservedSnapshot(deps, input, plan.Component, input.ObservedState.InstanceStatuses)

	decision, err := Plan(ctx, input, plan, target, snapshot)
	if err != nil {
		return ctrl.Result{}, err
	}
	res, err := Execute(ctx, deps, input, plan, target, snapshot, decision)
	res = types.EarliestWake(res, input.PromoteWindow.Pending(), input.PassWake.Pending())
	if input.PassWake.Bare() && res.RequeueAfter == 0 {
		res = types.RequeueNow()
	}
	return types.NoSoonerThan(res, input.Pacing.Pending()), err
}

// Execute applies the Decision: the hold pass, then the op-pass action
// loop, then the end-of-pass bookkeeping (endOfPassBookkeeping). A
// scale-down status commit
// is a pass boundary because it invalidates the plan. While an admitted wave
// is polling, escalation still runs for retained Instances but excludes every
// scale-down extra; deferred victims must receive no lifecycle mutation before
// admission. Other operation requeues retain the existing escalation behavior
// so a wedged surge or gang can fail while its operation keeps polling.
// Teardown never reaches Execute.
//
// An errored op pass does NOT suspend that bookkeeping. Both end-of-pass
// steps are per-Instance and re-derive their own evidence from the
// snapshot, so one Instance's failed operation — a rejected write, a
// throttled apiserver — would otherwise freeze every other Instance's
// deadline clock and leak superseded RetryBlocks for as long as the
// failure persists. The op error is the pass's primary failure: it is
// reported first and an end-of-pass error is appended to it, never
// substituted for it.
func Execute(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, decision Decision) (ctrl.Result, error) {
	// Holds first, for every row, before any verb pass: a token is
	// written as soon as its condition is observed. What the owner does
	// about it — abandon the step now or finish to its boundary — is the
	// ownership table's answer, not the hold pass's.
	//
	// An errored hold pass does not suspend the verb passes, for the same
	// reason an errored op pass does not suspend the bookkeeping below:
	// every row derives its own holds, so one row's failed read must not
	// freeze another row's operation. The op error stays the pass's
	// primary failure and is reported first.
	// The decision layer grouped the rows by owner once; every pass below
	// reads its own list out of that one answer.
	input.Owned = &decision.Owned
	heldRows, holdErr := runHoldPass(ctx, deps, input, plan, snapshot, scaleDownExtras(decision))
	res, endsPass, err := executeActions(ctx, deps, input, plan, target, snapshot, decision)
	if holdErr != nil {
		err = errors.Join(err, holdErr)
	}
	// A scale-down status commit is a pass boundary: it invalidates the
	// plan, so nothing that reads the plan runs after it.
	if endsPass {
		return res, err
	}
	endOfPass := endOfPassBookkeeping(ctx, deps, input, plan, target, snapshot, decision, heldRows)
	if endOfPass == nil {
		return res, err
	}
	if err == nil {
		return ctrl.Result{}, endOfPass
	}
	return res, errors.Join(err, endOfPass)
}

// endOfPassBookkeeping runs the end-of-pass steps the Decision allows:
// the terminal-failure escalation pass, which repairs from the evidence
// and from the holds this pass already recorded, followed by the
// RetryBlock supersede-prune.
//
// A paused Component runs neither. A pause freezes repair, not
// observation — the hold authorities ran at the top of the pass and
// reported what they found, so the row still names the wait an operator
// needs to see — and the supersede-prune is withheld work in the same
// sense, not an observation a pause hides.
func endOfPassBookkeeping(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, decision Decision, held holds.Result) error {
	if !decision.Escalate {
		return nil
	}
	if err := escalation.Run(ctx, escalation.PassInput{
		Deps:     deps,
		Input:    input,
		Plan:     plan,
		Target:   target,
		Pods:     snapshot.CachedPods,
		Excluded: scaleDownExtras(decision),
		Held:     held,
	}); err != nil {
		return err
	}
	return escalation.PruneSupersededRetryBlocks(ctx, input, target)
}

// runHoldPass evaluates every external hold once, for every observed
// row, before the verb passes run. It supplies the pass observation the
// authorities judge on — the persisted row, the plan's desired shape,
// and the reconcile's one memoized cached pod read — and the holds
// package owns everything decided from it.
func runHoldPass(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, snapshot *ObservedSnapshot, excluded map[int32]struct{}) (holds.Result, error) {
	return holds.Run(ctx, holds.PassInput{
		Deps:  deps,
		Input: input,
		Plan:  plan,
		Rows:  holds.RowsForPlan(plan, input.ObservedState.InstanceStatuses, excluded),
		Pods:  snapshot.CachedPods,
	})
}

// executeActions runs the Decision's selected actions in order; the
// numbered arms below are the pass's one step list. The bool result
// marks a scale-down status-commit boundary — the only return that
// suppresses the caller's end-of-pass escalation and prune.
//
// Every early return below encodes an ordering constraint, and each one
// says which. None of them fences a row off from a later pass: which pass
// may advance a row is the ownership table's answer, asked per row, not
// something the action order has to arrange. Two constraints recur:
//   - the observation a later pass would read is stale, because this
//     action committed a status change the plan was computed without;
//   - this action consumed the reconcile and owns its wake-up, so a later
//     action must not overwrite the interval it asked for.
func executeActions(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, decision Decision) (ctrl.Result, bool, error) {
	for actionIndex, action := range decision.Actions {
		switch action.Kind {
		// 1. Scale-down. Constraint: a status commit ends the pass — the
		// wave's own write is what the plan below was computed without —
		// and an admitted wave polling its victims must see no lifecycle
		// mutation from a later action before it completes.
		case ActionScaleDown:
			outcome, err := deleteExtraInstances(ctx, deps, input, plan, snapshot, action.Extras)
			if err != nil {
				return ctrl.Result{}, false, err
			}
			if res, stop, endsPass := scaleDownResult(input, outcome); stop {
				return res, endsPass, nil
			}

		// 2. Truth pass: status-only, never a scheduling or lifecycle
		// effect — apply and continue the pipeline.
		case ActionDemote:
			if err := demoteUnbackedInstances(ctx, deps, input, plan, action.Demotions); err != nil {
				return ctrl.Result{}, false, err
			}

		// 3. Per-Instance restart pass. Constraint: a repair opened or
		// held owns the wake-up — nothing in the cluster changes while a
		// denial stands, so no watch event is coming — and the pass ends
		// on it after materializing the surge-free indices itself.
		case ActionRestart:
			createFollows := hasActionKind(decision.Actions[actionIndex+1:], ActionCreate)
			res, stop, err := executeRestartPass(ctx, deps, input, plan, target, action.Restarts, createFollows)
			if err != nil {
				return res, false, err
			}
			if stop {
				return res, false, nil
			}

		// 4. Migration expiry pass. Constraint: the plan is stale when
		// anything expired — ObservedState and plan carry the pre-expiry
		// record (record terminal, pair ops cleared) — and the rebuilt plan
		// drops the unpinned surge index so the scale-down arm (1) tears
		// it down.
		case ActionMigrateExpiry:
			if expiredCount, err := workloadops.ExpireMigrations(ctx, deps, input, plan); err != nil {
				return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: expire migrations: %w", err)
			} else if expiredCount > 0 {
				return types.RequeueNow(), false, nil
			}

		// 5. Per-Component migration pass, one record per pass.
		// Constraint: a migration in flight paces the pass and a completed
		// one leaves the plan stale, so both end it; a fresh-record defer
		// falls through so the in-flight op it waits on converges.
		case ActionMigrate:
			res, stop, err := executeMigratePass(ctx, deps, input, plan, action.Migration.Record)
			if err != nil {
				return res, false, err
			}
			if stop {
				return res, false, nil
			}

		// 6. Per-Instance update pass. Constraint: an Update that ran
		// leaves the observation stale for Create — see executeUpdatePass's
		// anyUpdateRan — so stop=true ends the pass with the interval the
		// update asked for.
		case ActionUpdate:
			res, stop, err := executeUpdatePass(ctx, deps, input, plan, target, snapshot, action.Update, decision.RequeueAfter)
			if err != nil {
				return res, false, err
			}
			if stop {
				return res, false, nil
			}

		// 7. Create pass, always last: nothing after it reads the
		// observation it changes. The decision's RetryBlock wake-up merges
		// into its result.
		case ActionCreate:
			res, err := createPass(ctx, deps, input, plan, target, createScopeFull)
			if err != nil {
				return res, false, err
			}
			return types.EarliestWake(res, decision.RequeueAfter), false, nil
		}
	}

	// Only a paused Decision with no committed create ends without a
	// Create action: scale-down (if any) has run, nothing else may.
	return ctrl.Result{}, false, nil
}

// demoteUnbackedInstances applies the truth pass: the status-only
// Ready→Pending transition for the Instances Plan proved unbacked. It
// lives at the dispatcher and not under a verb because no verb owns it —
// it is what the pass says about rows no operation is advancing.
func demoteUnbackedInstances(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, selections []types.DemotionSelection) error {
	for _, selection := range selections {
		demoted, err := status.DemoteUnbacked(ctx, input, selection.Index)
		if err != nil {
			return fmt.Errorf("workload.Reconcile: demote instance %d (component=%s): %w", selection.Index, plan.Component, err)
		}
		if demoted {
			types.RecordWarning(deps.Recorder, types.EventTarget(input), types.EventReasonInstanceDemoted,
				"Instance %d (component=%s) demoted Ready→Pending: %s", selection.Index, plan.Component, selection.Reason)
		}
	}
	return nil
}

// createScope names how much of the Create pass a caller asks for.
type createScope int

const (
	// createScopeFull materializes every planned index — the Create
	// pass at its own position in the pipeline.
	createScopeFull createScope = iota
	// createScopeFresh materializes only surge-free indices, so a
	// scale-up is not starved behind another Instance's in-flight
	// rollout or held repair.
	createScopeFresh
)

// createPass runs the Create pass at the requested scope. A paused
// Component narrows either scope to the indices whose create is already
// committed: the pause finishes a materialization under way and begins
// none, whichever caller reaches the pass.
func createPass(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, scope createScope) (ctrl.Result, error) {
	switch {
	case plan.Paused:
		return workloadops.CreateCommittedIndices(ctx, deps, input, plan, target)
	case scope == createScopeFresh:
		return workloadops.CreateFreshIndices(ctx, deps, input, plan, target)
	default:
		return workloadops.Create(ctx, deps, input, plan, target)
	}
}

func hasActionKind(actions []PlannedAction, kind ActionKind) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}

// scaleDownResult maps one scale-down batch outcome onto the pass. A
// status commit ends the pass at a boundary (the wave's own write is
// what the plan was computed without); a wave still polling its victims
// returns its wake-up and stops the actions without ending the pass; a
// finished wave lets the actions continue.
func scaleDownResult(input types.ReconcileInput, outcome workloadops.DeleteBatchResult) (res ctrl.Result, stop, endsPass bool) {
	if outcome.ImmediateRequeue {
		return types.RequeueNow(), true, true
	}
	if outcome.InProgress {
		return scaleDownPollResult(input, outcome.RequeueAfter, outcome.PolicyDeadlineDue), true, false
	}
	return ctrl.Result{}, false, false
}

// reconcileTeardown runs the scale-down pipeline over every observed
// Instance. Not the Paused gate, not Restart / Migrate / Update / Create,
// not the escalation pass: the scale-down pipeline owns wedge escalation
// through lifecycle.forceDelete.
func reconcileTeardown(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan) (ctrl.Result, error) {
	extras := TeardownExtras(input.ObservedState.InstanceStatuses)
	snapshot := NewObservedSnapshot(deps, input, plan.Component, input.ObservedState.InstanceStatuses)
	outcome, err := deleteExtraInstances(ctx, deps, input, plan, snapshot, extras)
	if err != nil {
		return ctrl.Result{}, err
	}
	res, _, _ := scaleDownResult(input, outcome)
	return res, nil
}

func scaleDownPollResult(input types.ReconcileInput, policyRequeueAfter time.Duration, policyDeadlineDue bool) ctrl.Result {
	if policyDeadlineDue {
		return types.RequeueNow()
	}
	return types.EarliestWake(ctrl.Result{}, input.ScaleDownRequeueInterval, policyRequeueAfter)
}

func scaleDownExtras(decision Decision) map[int32]struct{} {
	for _, action := range decision.Actions {
		if action.Kind != ActionScaleDown || len(action.Extras) == 0 {
			continue
		}
		extras := make(map[int32]struct{}, len(action.Extras))
		for _, index := range action.Extras {
			extras[index] = struct{}{}
		}
		return extras
	}
	return nil
}

// deleteExtraInstances advances the Component's one durable scale-down wave.
// The snapshot supplies the single authoritative Pod observation shared by
// selection, drain, deletion, and completion.
func deleteExtraInstances(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, snapshot *ObservedSnapshot, extras []int32) (workloadops.DeleteBatchResult, error) {
	pods, err := snapshot.LivePods(ctx)
	if err != nil {
		return workloadops.DeleteBatchResult{}, fmt.Errorf("workload.Reconcile: list pods for scale-down (component=%s): %w", plan.Component, err)
	}
	outcome, err := workloadops.DeleteBatch(ctx, deps, input, plan, extras, pods)
	if err != nil {
		return workloadops.DeleteBatchResult{}, fmt.Errorf("workload.Reconcile: delete batch: %w", err)
	}
	return outcome, nil
}

// ScaleDownExtras returns the observed indices the plan no longer
// covers — the scale-down targets. Set-difference framing handles
// sparse indices from surge migration (index 7 is extra only when no
// InstancePlan covers it, regardless of replica count). A mid-migration
// pair — the Phase=Migrating source and its Operation.Type=Migrate
// surge — is not extra, so it is not scale-down-deleted out from under
// Migrate.
func ScaleDownExtras(observed []types.InstanceStatus, plan types.ComponentPlan) []int32 {
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var extras []int32
	for _, row := range observed {
		if _, inPlan := planned[row.Index]; inPlan {
			continue
		}
		if migrationPinned(&row) {
			continue
		}
		extras = append(extras, row.Index)
	}
	return extras
}

// TeardownExtras returns every observed index: under teardown the
// planned set is empty and a mid-migration pair dies like everything
// else. Source and surge each carry their own InstanceStatus, so both
// run the full scale-down pipeline; excluding the pair would leave its
// pods with no scale-down operation and wedge the teardown.
func TeardownExtras(observed []types.InstanceStatus) []int32 {
	var extras []int32
	for _, row := range observed {
		extras = append(extras, row.Index)
	}
	return extras
}
