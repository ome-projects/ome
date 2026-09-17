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
//   - workload.ReconcileHeadlessService — invoked by the caller before
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
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// ratioGateRequeueInterval is short enough that a peer Component
// catching up next reconcile unblocks this one promptly; long
// enough not to spam the apiserver. Unlike the Create, Restart, Migrate, and
// Update intervals or the configured scale-down cadence and deadlines, this
// pacing is dispatcher-owned.
const ratioGateRequeueInterval = 3 * time.Second

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
// The op chain:
//
//  1. Scale-down — DeleteBatch for any InstanceStatus index the plan no
//     longer asks for. Completes before scale-up so excess pods don't
//     run alongside in-flight drains.
//  2. Truth — status-only demotion of Ready Instances with no live pods
//     and no in-flight operation, where no op pass will act. Selected
//     only on paused reconciles (every depth — unpaused, Create both
//     recovers and re-stamps the phase); never touches a pod.
//  3. Restart — per-Instance pod-loss / pod-Failed triggers; dispatched
//     when the Component's restart policy allows it. One pass per
//     reconcile.
//  4. Migration expiry — Manual records past their Deadline are
//     consumed (ops.ExpireMigrations): record closed, pair unpinned,
//     source restored from observation. Runs BEFORE the drive pass so
//     an expired record can never be driven (re-stamped) again, and
//     regardless of MigrationMode so a mode flip to Never cannot
//     strand a non-terminal record.
//  5. Migration — the oldest non-terminal Manual record from
//     ObservedState.Migrations drives Migrate, one per pass. Returns
//     Requeue=true after completion so the next reconcile rebuilds
//     plan from the post-migration status.
//  6. Update — DetectUpdateTrigger then Update. Tracks within-pass
//     in-flight counters and consults input.UpdateGate so
//     cross-Component coordination can throttle the rollout.
//  7. Create — materialize any missing Instances.
//  8. Escalation — the terminal-failure pass (escalation.go): stuck-pod
//     and elapsed-deadline evidence from the snapshot decides the
//     Phase=Failed transition through the disposition classification.
//     Runs after eligible non-error op-pass returns. A scale-down admission
//     or completion commit ends the pass; an active wave excludes every extra
//     index while retained Instances remain eligible for escalation.
//
// target may be nil when DesiredSpec.PodSpec is nil (MinReplicas=0).
// Restart / Update passes short-circuit on nil target; Create returns
// immediately.
func Reconcile(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if deps.Client == nil {
		return ctrl.Result{}, fmt.Errorf("workload.Reconcile: nil client (component=%s)", plan.Component)
	}
	// One pacing sink per pass. The op state machines report progress as
	// (done, error) and a throttled write is neither, so the server's
	// suggested delay is deposited here and floors the wake-up below.
	if input.Pacing == nil {
		input.Pacing = &APIPacing{}
	}

	// Teardown mode: the owner is being deleted. The planned index set is
	// treated as empty so every observed Instance is a scale-down extra
	// and runs the scale-down batch pipeline; nothing else runs — not the
	// Paused gate, not Restart / Migrate / Update / Create, not the
	// escalation pass (the scale-down pipeline owns wedge escalation via
	// lifecycle.forceDelete). The caller owns completion detection and
	// finalizer decisions.
	//
	// includeMigrating=true: a mid-migration source and its surge are
	// deleted like everything else. The normal-path exclusion protects an
	// in-flight Migrate from the scale-down pass, but under teardown there
	// is no Migrate to protect — excluding the pair would leave its pods
	// with no scale-down operation and wedge the teardown forever.
	if input.Teardown {
		emptied := plan
		emptied.Instances = nil
		extras := ExtraInstanceIndices(input.ObservedState.InstanceStatuses, emptied, true)
		snapshot := NewObservedSnapshot(deps, input, plan.Component, input.ObservedState.InstanceStatuses)
		outcome, err := deleteExtraInstances(ctx, deps, input, plan, snapshot, extras)
		if err != nil {
			return ctrl.Result{}, err
		}
		if outcome.ImmediateRequeue {
			return ctrl.Result{Requeue: true}, nil
		}
		if outcome.InProgress {
			return scaleDownPollResult(input, outcome.RequeueAfter, outcome.PolicyDeadlineDue), nil
		}
		return ctrl.Result{}, nil
	}

	// Single observation for this reconcile: pod reads are lazy + memoized
	// per source (live for destructive planning, cached otherwise), so each
	// source is Listed at most once and only when a pass needs it.
	snapshot := NewObservedSnapshot(deps, input, plan.Component, input.ObservedState.InstanceStatuses)

	decision, perr := Plan(ctx, input, plan, target, snapshot)
	if perr != nil {
		return ctrl.Result{}, perr
	}
	res, err := Execute(ctx, deps, input, plan, target, snapshot, decision)
	return floorRetryAfter(res, input.Pacing.Throttle()), err
}

// Execute applies the Decision: the op-pass action loop, then — when
// Decision.Escalate allows it — the escalation pass and the RetryBlock
// supersede-prune. A scale-down status commit is a pass boundary because it
// invalidates the plan. While an admitted wave is polling, escalation still
// runs for retained Instances but excludes every scale-down extra; deferred
// victims must receive no lifecycle mutation before admission. Other operation
// requeues retain the existing escalation behavior so a wedged surge or gang
// can fail while its operation keeps polling. The supersede-prune shares the
// Escalate gate (both are end-of-pass bookkeeping suspended while paused, and
// Teardown never reaches Execute).
func Execute(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, d Decision) (ctrl.Result, error) {
	res, statusCommitBoundary, err := executeActions(ctx, deps, input, plan, target, snapshot, d)
	if err != nil || !d.Escalate || statusCommitBoundary {
		return res, err
	}
	if eerr := escalateFromEvidence(ctx, deps, input, plan, target, snapshot, scaleDownExtras(d)); eerr != nil {
		return ctrl.Result{}, eerr
	}
	if perr := pruneSupersededRetryBlocks(ctx, input, target); perr != nil {
		return ctrl.Result{}, perr
	}
	return res, nil
}

// executeActions runs the op-pass pipeline (steps 1-7 of the chain documented
// on Reconcile) for the Decision's selected actions, in order. The bool result
// marks a scale-down status-commit boundary. Other early returns preserve the
// existing end-of-pass escalation and pruning behavior.
func executeActions(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, d Decision) (ctrl.Result, bool, error) {
	for actionIndex, action := range d.Actions {
		switch action.Kind {
		// 1. Scale-down.
		case ActionScaleDown:
			outcome, err := deleteExtraInstances(ctx, deps, input, plan, snapshot, action.Extras)
			if err != nil {
				return ctrl.Result{}, false, err
			}
			if outcome.ImmediateRequeue {
				return ctrl.Result{Requeue: true}, true, nil
			}
			if outcome.InProgress {
				return scaleDownPollResult(input, outcome.RequeueAfter, outcome.PolicyDeadlineDue), false, nil
			}

		// 2. Truth pass: status-only, never a scheduling or lifecycle
		// effect — apply and continue the pipeline.
		case ActionDemote:
			if derr := workloadops.DemoteUnbackedInstances(ctx, deps, input, plan, action.Demotions); derr != nil {
				return ctrl.Result{}, false, derr
			}

		// 3. Per-Instance restart pass. Instances are isolated from each
		// other: one Instance's failed step never skips the Instances
		// after it, and the pass fails as a whole (joined error) only
		// after every selection has run.
		case ActionRestart:
			anyRestarting := false
			var restartErrs []error
			for _, sel := range action.Restarts {
				done, rerr := workloadops.Restart(ctx, deps, input, plan, sel.Instance, sel.Reason)
				if rerr != nil {
					logf.FromContext(ctx).Error(rerr, "restart pass: instance failed",
						"component", plan.Component, "instance", sel.Instance.Index)
					restartErrs = append(restartErrs, fmt.Errorf("workload.Reconcile: restart instance %d: %w", sel.Instance.Index, rerr))
					continue
				}
				if !done {
					anyRestarting = true
				}
			}
			if anyRestarting {
				result := ctrl.Result{RequeueAfter: workloadops.RestartRequeueInterval}
				if hasActionKind(d.Actions[actionIndex+1:], ActionCreate) {
					// Surge-free indices are materialized even when another
					// Instance's restart failed this pass: a persistently
					// failing restart must not starve an unrelated scale-up.
					freshPlan := planExcludingRestartSelections(plan, action.Restarts)
					createResult, ferr := workloadops.CreateFreshIndices(ctx, deps, input, freshPlan, target)
					if ferr != nil {
						restartErrs = append(restartErrs, fmt.Errorf("workload.Reconcile: create fresh indices during restart: %w", ferr))
						return createResult, false, errors.Join(restartErrs...)
					}
					result = foldRetryAfter(createResult, workloadops.RestartRequeueInterval)
				}
				if len(restartErrs) > 0 {
					return ctrl.Result{}, false, errors.Join(restartErrs...)
				}
				return result, false, nil
			}
			if len(restartErrs) > 0 {
				return ctrl.Result{}, false, errors.Join(restartErrs...)
			}

		// 4. Migration expiry pass. When anything expired, requeue
		// immediately: ObservedState and plan are now stale (record
		// terminal, pair ops cleared), and the next pass's rebuilt plan
		// drops the unpinned surge index so the ordinary step-1 Delete
		// pipeline tears it down.
		case ActionMigrateExpiry:
			if expiredCount, eerr := workloadops.ExpireMigrations(ctx, deps, input, plan); eerr != nil {
				return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: expire migrations: %w", eerr)
			} else if expiredCount > 0 {
				return ctrl.Result{Requeue: true}, false, nil
			}

		// 4. Per-Component migration pass (dispatch pacing unchanged:
		// one record per pass).
		//
		// Migrate's third return (accepted) distinguishes two done=false
		// modes:
		//   - accepted=true: migration is mid-flight (record carries the
		//     surge index, statuses stamped). Requeue at the Migrate
		//     interval; do NOT fall through (DetectUpdateTrigger already
		//     suppresses Migrate-owned status, so falling through would be
		//     a no-op anyway, but the explicit requeue is cleaner).
		//   - accepted=false: migration deferred without taking ownership
		//     (fresh record, source not yet steady-Ready because of an
		//     in-flight Update/Restart/Create). Fall through to Update/Create
		//     so the in-flight op converges. Without fall-through the
		//     dispatcher loops indefinitely at the MigrateRequeueInterval and
		//     the in-flight op never runs — the affinity-trigger-not-detected
		//     deadlock.
		case ActionMigrate:
			rec := action.Migration.Record
			// Reconstruct the executor's request view from the record —
			// the annotation was consumed at accept time.
			req := &audit.MigrationRequest{
				SchemaVersion:   audit.SchemaV1,
				Component:       string(plan.Component),
				Instance:        rec.SourceInstance,
				FromNode:        rec.FromNode,
				HintTargetNodes: append([]string(nil), rec.HintTargetNodes...),
				Reason:          rec.Reason,
			}
			sourceIdx := rec.SourceInstance
			done, accepted, merr := workloadops.Migrate(ctx, deps, input, plan, sourceIdx, rec.RequestUUID, req)
			if merr != nil {
				return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: migrate instance %d: %w", sourceIdx, merr)
			}
			if !done && accepted {
				// Mid-flight: requeue at the per-op interval.
				return ctrl.Result{RequeueAfter: workloadops.MigrateRequeueInterval}, false, nil
			}
			if done {
				// Migrate just removed the source InstanceStatus and
				// promoted the surge to Ready (or wrote a terminal
				// Failed). plan was computed with the stale pre-migration
				// view (both source and surge present, or pre-failure
				// state), so the Update + Create passes below would fall
				// through to recreate the source-side index. Re-queue so
				// the next reconcile rebuilds plan from the post-
				// migration status.
				return ctrl.Result{Requeue: true}, false, nil
			}
			// !done && !accepted: fresh-record defer (e.g., source
			// Phase=Updating from an in-flight spec edit). Fall through
			// to Update/Create so the in-flight op converges; the next
			// reconcile re-picks the same record against a steady-Ready
			// source.

		// 5. Per-Instance update pass.
		case ActionUpdate:
			res, stop, uerr := executeUpdatePass(ctx, deps, input, plan, target, snapshot, action.Update, d.RequeueAfter)
			if uerr != nil {
				return ctrl.Result{}, false, uerr
			}
			if stop {
				return res, false, nil
			}

		// 6. Create pass.
		case ActionCreate:
			res, cerr := workloadops.Create(ctx, deps, input, plan, target)
			if cerr != nil {
				return res, false, cerr
			}
			return foldRetryAfter(res, d.RequeueAfter), false, nil
		}
	}

	// Only a paused Decision ends without a Create action: scale-down
	// (if any) has run, nothing else may.
	return ctrl.Result{}, false, nil
}

func hasActionKind(actions []PlannedAction, kind ActionKind) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}

func planExcludingRestartSelections(plan ComponentPlan, restarts []RestartSelection) ComponentPlan {
	excluded := make(map[int32]struct{}, len(restarts))
	for _, restart := range restarts {
		excluded[restart.Instance.Index] = struct{}{}
	}

	filtered := plan
	filtered.Instances = make([]InstancePlan, 0, len(plan.Instances))
	for _, instance := range plan.Instances {
		if _, restarting := excluded[instance.Index]; restarting {
			continue
		}
		filtered.Instances = append(filtered.Instances, instance)
	}
	return filtered
}

func scaleDownPollResult(input ReconcileInput, policyRequeueAfter time.Duration, policyDeadlineDue bool) ctrl.Result {
	if policyDeadlineDue {
		return ctrl.Result{Requeue: true}
	}
	result := foldRetryAfter(ctrl.Result{}, input.ScaleDownRequeueInterval)
	return foldRetryAfter(result, policyRequeueAfter)
}

func scaleDownExtras(d Decision) map[int32]struct{} {
	for _, action := range d.Actions {
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

// executeUpdatePass runs the Update op for the Decision's selected
// Instances, throttled by the within-pass counters, the per-Component
// budget, and the coordination UpdateGate. The gate consult lives HERE,
// not in Plan: it reads live peer/self state, so it must run at the
// update pass's position — after earlier passes' effects (a restart
// completion, a finished scale-down) have landed — to observe the same
// state the pass pipeline showed it. stop=true means the pass consumed
// the reconcile (the caller returns res without running Create).
func executeUpdatePass(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, sel *UpdateSelection, retryBlockWait time.Duration) (res ctrl.Result, stop bool, err error) {
	anyUpdating := false
	anyGated := false
	// hold records the first StartingFresh denial this pass (Budget or
	// UpdateGate) for RecordRolloutHold — one Component has one hold
	// slot, so the first denial found (plan order) wins, matching the
	// gate stack's own first-denial-wins precedence.
	var hold *RolloutHold
	// anyUpdateRan tracks whether ANY Update call fired this wake-up
	// — including ones that returned done=true (e.g., a surge that
	// just promoted Phase to Ready and stamped RunningRevision). Even
	// a "done" Update mutates InstanceStatus; the subsequent Create
	// pass would observe the pre-Update ObservedState snapshot, which
	// is stale wrt ActiveOrdinal / RunningRevision / Phase. Letting
	// Create run on stale state leads to two bugs:
	//
	//   1. Create's Ready-promote stamps RunningRevision=target.Name
	//      even when the existing pods carry a different revision
	//      (the X-2 bump-during-bump corruption mode where status
	//      says vN but pods are still on vN-1).
	//   2. Create's scale-up reads activeOrdinalForInstance from
	//      stale ObservedState, sees a "missing" pod at the
	//      pre-promote ordinal slot, and creates a duplicate
	//      alongside the post-surge canonical pod.
	//
	// Skipping Create + requeue when Update fired forces the next
	// reconcile to read fresh ObservedState that reflects the just-
	// committed mutations, so Create's per-pod decisions are made
	// against the current cluster state.
	anyUpdateRan := false
	// Count only Instances STARTING a new update this wake-up
	// (item.StartingFresh). Instances already in Phase=Updating from a
	// prior wake-up are anchored in the selection's Prior* counts —
	// counting them again would double-charge the budget and deadlock
	// the in-flight pod. Closes the within-wake-up stale-snapshot hole
	// that otherwise lets the dispatcher fire every instance in one
	// shot (mass outage at scale).
	var inFlightUnavail int32
	var inFlightSurge int32
	// gateUnavail is the coordination gate's within-pass delta: fresh
	// starts that pull a SERVING pod from rotation this wake-up. It
	// diverges from inFlightUnavail (the per-Component, op-based
	// counter) on CoordGateExempt starts — a Failed zero-serving
	// Instance's recreate takes nothing additional offline, and its
	// outage is already inside the gate's serving-based count, so
	// charging it here would over-project every later consult in the
	// same pass.
	var gateUnavail int32
	isSurgeStrategy := sel.Strategy == UpdateStrategySurgeThenDrain
	// Same memoized cached read Plan selected from — the pods handed to
	// the Update op match the selection's evidence.
	updateByInstance, lerr := snapshot.CachedPods(ctx)
	if lerr != nil {
		return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: list pods for update pass (component=%s): %w", plan.Component, lerr)
	}
	for _, item := range sel.Items {
		if item.AdoptRevision {
			if berr := workloadops.BackfillRunningRevision(ctx, input, item.Instance.Index, target.Name); berr != nil {
				return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: detect update trigger (instance=%d): %w", item.Instance.Index, berr)
			}
			continue
		}
		if item.CleanupOnly {
			// Superseded-revision wreckage: abandon toward the current
			// desired state. Never budget-charged and never gated —
			// cleanup only deletes dead pods / resets a stranded
			// continuation, freeing capacity rather than consuming it.
			done, cerr := workloadops.CleanupWreckage(ctx, deps, input, plan, item.Instance, target, updateByInstance[item.Instance.Index])
			if cerr != nil {
				return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: cleanup wreckage (instance=%d): %w", item.Instance.Index, cerr)
			}
			if !done {
				anyUpdating = true
				anyUpdateRan = true
			}
			continue
		}
		if item.StartingFresh {
			// Per-Component within-Component cap. Independent from
			// the coordination-group cap below: each is its own
			// capacity, both must allow; first denial stops the start.
			// We project (prior + this-wake-up + 1) against the layer's
			// budget the same way coordination/ratio.go's CheckSurge /
			// CheckUnavailability project against the group budget.
			if isSurgeStrategy {
				if sel.SurgeBudget != BudgetNoLimit {
					projected := sel.PriorSurgeInFlight + inFlightSurge + 1
					if projected > sel.SurgeBudget {
						anyGated = true
						if hold == nil {
							hold = &RolloutHold{
								Gate:   RolloutHoldGateBudget,
								Reason: fmt.Sprintf("per-Component surge budget %d exhausted (would become %d)", sel.SurgeBudget, projected),
								Target: target.Name,
							}
						}
						continue
					}
				}
			} else {
				if sel.UnavailBudget != BudgetNoLimit {
					projected := sel.PriorUnavailInFlight + inFlightUnavail + 1
					if projected > sel.UnavailBudget {
						anyGated = true
						if hold == nil {
							hold = &RolloutHold{
								Gate:   RolloutHoldGateBudget,
								Reason: fmt.Sprintf("per-Component unavailability budget %d exhausted (would become %d)", sel.UnavailBudget, projected),
								Target: target.Name,
							}
						}
						continue
					}
				}
			}
			if input.UpdateGate != nil && !item.CoordGateExempt {
				// Pass in-flight counters so the gate can project
				// against the post-this-pass shape. Independent
				// layer from the per-Component check above.
				// CoordGateExempt starts skip the consult: the gate
				// already counts their outage in its serving-based
				// unavailability, so gating their own recreate is a
				// double count that starves the recovery (see
				// UpdateItem.CoordGateExempt).
				if allowed, gate, reason := input.UpdateGate(sel.Strategy, inFlightSurge, gateUnavail); !allowed {
					anyGated = true
					logf.FromContext(ctx).V(1).Info("update start denied by coordination gate",
						"component", plan.Component, "instance", item.Instance.Index,
						"target", target.Name, "gate", gate, "reason", reason,
						"inFlightSurge", inFlightSurge, "gateUnavail", gateUnavail)
					if hold == nil {
						hold = &RolloutHold{Gate: gate, Reason: reason, Target: target.Name}
					}
					continue
				}
			}
		}
		done, uerr := workloadops.UpdateWithPods(ctx, deps, input, plan, item.Instance, target, input.DesiredSpec.PodSpec, updateByInstance[item.Instance.Index])
		if uerr != nil {
			return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: update instance %d: %w", item.Instance.Index, uerr)
		}
		anyUpdateRan = true
		if item.StartingFresh {
			// Charge the wake-up budget only for fresh starts —
			// subsequent gate checks must account for this pod.
			if isSurgeStrategy {
				inFlightSurge++
			} else {
				inFlightUnavail++
				if !item.CoordGateExempt {
					gateUnavail++
				}
			}
		}
		if !done {
			anyUpdating = true
		}
	}
	if anyUpdateRan {
		// Forward progress this pass: whatever was gated for a DIFFERENT
		// Instance is superseded — the Component is not stuck, it will
		// re-observe fresh state (including any still-active gate) next
		// pass. Clearing here is also what lets a resolved hold disappear
		// promptly instead of lingering until the next denial-free pass.
		hold = nil
	}
	if input.RecordRolloutHold != nil {
		input.RecordRolloutHold(hold)
	}
	if anyUpdating || anyGated || anyUpdateRan {
		// About to requeue without the full Create pass. Brand-new
		// (surge-free) indices legitimately bypass the skip-Create
		// gate above: they have a genuine ActiveOrdinal=0 and no
		// RunningRevision to mis-stamp, so neither X-2 corruption
		// mode applies — see ops.CreateFreshIndices. Materialize them
		// now so a concurrent scale-up isn't starved behind the
		// in-flight rollout. The full Create pass still owns
		// surge-sensitive (touched) indices once the rollout drains.
		if _, ferr := workloadops.CreateFreshIndices(ctx, deps, input, plan, target); ferr != nil {
			return ctrl.Result{}, false, ferr
		}
	}
	if anyUpdating {
		return foldRetryAfter(ctrl.Result{RequeueAfter: workloadops.UpdateRequeueInterval}, retryBlockWait), true, nil
	}
	if anyGated {
		return foldRetryAfter(ctrl.Result{RequeueAfter: ratioGateRequeueInterval}, retryBlockWait), true, nil
	}
	// All Updates that ran returned done=true (steady-state for those
	// instances). Requeue without running Create so the next pass
	// sees fresh ObservedState — see anyUpdateRan above for the X-2
	// (bump-during-bump) corruption modes this guards against.
	// Immediate requeue is already sooner than any retryBlockWait.
	if anyUpdateRan {
		return ctrl.Result{Requeue: true}, true, nil
	}
	return ctrl.Result{}, false, nil
}

// deleteExtraInstances advances the Component's one durable scale-down wave.
// The snapshot supplies the single authoritative Pod observation shared by
// selection, drain, deletion, and completion.
func deleteExtraInstances(ctx context.Context, deps Deps, input ReconcileInput, plan ComponentPlan, snapshot *ObservedSnapshot, extras []int32) (workloadops.DeleteBatchResult, error) {
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

func hasDeleteOwnedInstance(statuses []InstanceStatus) bool {
	for _, status := range statuses {
		if status.Phase == InstancePhaseDeleting && status.Operation != nil && status.Operation.Type == InstanceOperationDelete {
			return true
		}
	}
	return false
}

// foldRetryAfter folds a RetryBlock re-evaluation wake-up into res.
// Additive only: an immediate requeue or an already-earlier
// RequeueAfter wins; otherwise take the min. Never removes a wake-up.
func foldRetryAfter(res ctrl.Result, retryAfter time.Duration) ctrl.Result {
	if retryAfter <= 0 {
		return res
	}
	if res.Requeue && res.RequeueAfter == 0 {
		// Immediate requeue is sooner than any positive retryAfter.
		return res
	}
	if res.RequeueAfter == 0 || retryAfter < res.RequeueAfter {
		res.RequeueAfter = retryAfter
	}
	return res
}

// floorRetryAfter raises res's wake-up to at least retryAfter. Used for a
// delay the apiserver itself asked for: unlike foldRetryAfter's min, a
// server-suggested Retry-After is a FLOOR — waking sooner just re-earns
// the rejection. An immediate requeue is left alone; the caller's own
// reasons to come straight back outrank a pacing hint.
func floorRetryAfter(res ctrl.Result, retryAfter time.Duration) ctrl.Result {
	if retryAfter <= 0 || res.Requeue {
		return res
	}
	if retryAfter > res.RequeueAfter {
		res.RequeueAfter = retryAfter
	}
	return res
}

// liveObservePods performs the selector-scoped live-role List.
func liveObservePods(ctx context.Context, deps Deps, input ReconcileInput, component ComponentType) (PodObservation, error) {
	// The API reader has no Pod field index. useIndex=false preserves one List
	// for this role, including its cached-client fallback.
	reader := deps.Reader()
	source := PodObservationSourceAPIReader
	if deps.APIReader == nil {
		source = PodObservationSourceCache
	}
	pods, err := query.ListOMENativePodsByName(ctx, reader, input.Key.Namespace, input.Key.OwnerName, component, false)
	if err != nil {
		return PodObservation{}, err
	}
	return newPodObservation(source, PodObservationScopeSelector, pods, nil), nil
}

// cachedObservePods performs the selector-scoped cache List.
func cachedObservePods(ctx context.Context, deps Deps, input ReconcileInput, component ComponentType) (PodObservation, error) {
	// Cached client has the OMENative Pod field index — useIndex=true takes
	// the index fast path instead of scanning every cached pod.
	pods, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, component, true)
	if err != nil {
		return PodObservation{}, err
	}
	return NewCachedSelectorPodObservation(pods, nil), nil
}

// ExtraInstanceIndices returns InstanceStatus indices the plan no
// longer covers — scale-down targets. Set-difference framing handles
// sparse indices from surge migration (index 7 is extra only when no
// InstancePlan covers it, regardless of replica count).
//
// includeMigrating=false (the normal scale-down pass) excludes
// mid-migration Instances — Phase=Migrating sources and
// Operation.Type=Migrate surges — so the pair isn't scale-down-deleted
// out from under Migrate. Teardown passes includeMigrating=true:
// everything must die, and since source and surge each carry their own
// InstanceStatus entry, both run the full scale-down pipeline
// (batch admission stamps Deleting over any phase).
func ExtraInstanceIndices(observed []InstanceStatus, plan ComponentPlan, includeMigrating bool) []int32 {
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var extras []int32
	for _, s := range observed {
		if _, inPlan := planned[s.Index]; inPlan {
			continue
		}
		if !includeMigrating {
			if s.Phase == InstancePhaseMigrating {
				continue
			}
			if s.Operation != nil && s.Operation.Type == InstanceOperationMigrate {
				continue
			}
		}
		extras = append(extras, s.Index)
	}
	return extras
}

// findObservedInstanceStatus returns the matching InstanceStatus
// pointer, or nil if absent.
func findObservedInstanceStatus(observed []InstanceStatus, idx int32) *InstanceStatus {
	for i := range observed {
		if observed[i].Index == idx {
			return &observed[i]
		}
	}
	return nil
}
