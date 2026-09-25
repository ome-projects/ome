package workload

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// executeUpdatePass runs the Update op for the Decision's selected
// Instances, throttled by the within-pass counters, the per-Component
// budget, and the coordination UpdateGate. The gate consult lives HERE,
// not in Plan: it reads live peer/self state, so it must run at the
// update pass's position — after earlier passes' effects (a restart
// completion, a finished scale-down) have landed — to observe the same
// state the pass pipeline showed it. stop=true means the pass consumed
// the reconcile (the caller returns res without running Create).
func executeUpdatePass(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, selection *UpdateSelection, retryBlockWait time.Duration) (res ctrl.Result, stop bool, err error) {
	anyUpdating := false
	anyGated := false
	// First StartingFresh denial this pass (Budget or UpdateGate), for
	// RecordRolloutHold — one Component has one hold slot, so the first
	// denial found (plan order) wins, matching the gate stack's own
	// first-denial-wins precedence.
	var firstDenial *types.RolloutHold
	// anyUpdateRan tracks whether ANY Update call fired this pass,
	// including one that returned done=true: even a done Update mutates
	// the row. Create must not run on an ObservedState an Update just
	// mutated — its Ready-promote and its ordinal read would both act on
	// the pre-Update row — so the pass requeues instead and the next
	// reconcile reads fresh state.
	anyUpdateRan := false
	admission := newUpdateAdmission(input, selection, target)
	// Same memoized cached read Plan selected from — the pods handed to
	// the Update op match the selection's evidence.
	updateByInstance, listErr := snapshot.CachedPods(ctx)
	if listErr != nil {
		return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: list pods for update pass (component=%s): %w", plan.Component, listErr)
	}
	for _, item := range selection.Items {
		if item.AdoptRevision {
			if backfillErr := workloadops.BackfillRunningRevision(ctx, input, item.Instance.Index, target.Name); backfillErr != nil {
				return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: detect update trigger (instance=%d): %w", item.Instance.Index, backfillErr)
			}
			continue
		}
		if item.CleanupOnly {
			// Superseded-revision wreckage: abandon toward the current
			// desired state. Never budget-charged and never gated —
			// cleanup only deletes dead pods / resets a stranded
			// continuation, freeing capacity rather than consuming it.
			done, cleanupErr := workloadops.CleanupWreckage(ctx, deps, input, plan, item.Instance, target, updateByInstance[item.Instance.Index])
			if cleanupErr != nil {
				return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: cleanup wreckage (instance=%d): %w", item.Instance.Index, cleanupErr)
			}
			if !done {
				anyUpdating = true
				anyUpdateRan = true
			}
			continue
		}
		// Only a fresh start is admitted; an Instance already in
		// Phase=Updating from a prior wake-up is anchored in the
		// selection's Prior* counts, and charging it again would deadlock
		// the in-flight pod.
		if item.StartingFresh {
			allowed, denial := admission.admit(ctx, plan, item)
			if !allowed {
				anyGated = true
				if firstDenial == nil {
					firstDenial = denial
				}
				continue
			}
		}
		done, updateErr := workloadops.UpdateWithPods(ctx, deps, input, plan, item.Instance, target, input.DesiredSpec.PodSpec, updateByInstance[item.Instance.Index])
		if updateErr != nil {
			return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: update instance %d: %w", item.Instance.Index, updateErr)
		}
		anyUpdateRan = true
		if item.StartingFresh {
			admission.charge(item)
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
		firstDenial = nil
	}
	if input.RecordRolloutHold != nil {
		input.RecordRolloutHold(firstDenial)
	}
	if anyUpdating || anyGated || anyUpdateRan {
		// About to requeue without the full Create pass. Brand-new
		// (surge-free) indices legitimately bypass the skip-Create
		// gate above: they have a genuine ActiveOrdinal=0 and no
		// RunningRevision to mis-stamp, so the stale-observation hazard
		// above does not apply — see ops.CreateFreshIndices. Materialize them
		// now so a concurrent scale-up isn't starved behind the
		// in-flight rollout. The full Create pass still owns
		// surge-sensitive (touched) indices once the rollout drains.
		if _, createErr := createPass(ctx, deps, input, plan, target, createScopeFresh); createErr != nil {
			return ctrl.Result{}, false, createErr
		}
	}
	if anyUpdating {
		return types.PassRequeue(workloadops.UpdateRequeueInterval(input), retryBlockWait), true, nil
	}
	if anyGated {
		return types.PassRequeue(input.Requeue.Gate, retryBlockWait), true, nil
	}
	// All Updates that ran returned done=true (steady-state for those
	// instances). Requeue without running Create so the next pass
	// sees fresh ObservedState (see anyUpdateRan above). Immediate
	// requeue is already sooner than any retryBlockWait.
	if anyUpdateRan {
		return types.RequeueNow(), true, nil
	}
	return ctrl.Result{}, false, nil
}

// updateAdmission paces the fresh update starts of one pass. The two
// layers a start answers to are consulted in order, against the same
// numbers repairAdmission uses: the per-Component budget on the arm the
// strategy uses (surge or unavailability), then the cross-Component
// coordination gate. Starts already in flight from a prior wake-up are
// anchored in the selection's Prior* counts; the pass's own starts are
// charged here as they open, so every later consult in the pass
// projects against the post-this-pass shape rather than firing every
// Instance in one shot.
type updateAdmission struct {
	selection *UpdateSelection
	target    *appsv1.ControllerRevision
	gate      func(strategy types.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, types.RolloutHoldGate, string)

	// inFlightSurge and inFlightUnavail are the fresh starts this pass
	// charged to the per-Component budget, one per strategy arm.
	inFlightSurge   int32
	inFlightUnavail int32
	// gateUnavail is the coordination gate's within-pass delta: fresh
	// starts that pull a SERVING pod from rotation. It diverges from
	// inFlightUnavail on CoordGateExempt starts — a Failed zero-serving
	// Instance's recreate takes nothing additional offline, and its
	// outage is already inside the gate's serving-based count, so
	// charging it here would over-project every later consult in the
	// same pass.
	gateUnavail int32
}

func newUpdateAdmission(input types.ReconcileInput, selection *UpdateSelection, target *appsv1.ControllerRevision) *updateAdmission {
	return &updateAdmission{selection: selection, target: target, gate: input.UpdateGate}
}

func (a *updateAdmission) surge() bool {
	return a.selection.Strategy == types.UpdateStrategySurgeThenDrain
}

// admit reports whether item may start this pass. The per-Component
// budget and the coordination gate are independent capacities and both
// must allow; the first denial is returned as the hold that produced
// it, so the caller can report which layer is holding the Component.
// Both layers project (prior + this pass + 1) against their budget, the
// way the coordination group budget is projected.
func (a *updateAdmission) admit(ctx context.Context, plan types.ComponentPlan, item UpdateItem) (bool, *types.RolloutHold) {
	if a.surge() {
		if projected, denied := escalation.BudgetDenies(a.selection.SurgeBudget, a.selection.PriorSurgeInFlight, a.inFlightSurge); denied {
			return false, &types.RolloutHold{
				Gate:   types.RolloutHoldGateBudget,
				Reason: fmt.Sprintf("per-Component surge budget %d exhausted (would become %d)", a.selection.SurgeBudget, projected),
				Target: a.target.Name,
			}
		}
	} else if projected, denied := escalation.BudgetDenies(a.selection.UnavailBudget, a.selection.PriorUnavailInFlight, a.inFlightUnavail); denied {
		return false, &types.RolloutHold{
			Gate:   types.RolloutHoldGateBudget,
			Reason: fmt.Sprintf("per-Component unavailability budget %d exhausted (would become %d)", a.selection.UnavailBudget, projected),
			Target: a.target.Name,
		}
	}
	// A CoordGateExempt start skips the consult: the gate already counts
	// its outage in its serving-based unavailability, so gating its own
	// recreate would double count and starve the recovery.
	if a.gate != nil && !item.CoordGateExempt {
		if allowed, gate, reason := a.gate(a.selection.Strategy, a.inFlightSurge, a.gateUnavail); !allowed {
			logf.FromContext(ctx).V(1).Info("update start denied by coordination gate",
				"component", plan.Component, "instance", item.Instance.Index,
				"target", a.target.Name, "gate", gate, "reason", reason,
				"inFlightSurge", a.inFlightSurge, "gateUnavail", a.gateUnavail)
			return false, &types.RolloutHold{Gate: gate, Reason: reason, Target: a.target.Name}
		}
	}
	return true, nil
}

// charge counts a fresh start the pass opened, so every later consult
// projects against it.
func (a *updateAdmission) charge(item UpdateItem) {
	if a.surge() {
		a.inFlightSurge++
		return
	}
	a.inFlightUnavail++
	if !item.CoordGateExempt {
		a.gateUnavail++
	}
}
