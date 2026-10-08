package workload

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// executeUpdatePass runs the Update op for the Decision's selected
// Instances, throttled by the within-pass counters, the per-Component
// budget, and the coordination UpdateGate. The gate consult lives HERE,
// not in Plan: it reads live peer/self state, so it must run at the
// update pass's position — after earlier passes' effects (a restart
// completion, a finished scale-down) have landed — to observe the same
// state the pass pipeline showed it. stop=true means the pass consumed
// the reconcile (the caller returns res without running the full Create
// pass).
func executeUpdatePass(ctx context.Context, deps types.Deps, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot, selection *UpdateSelection, retryBlockWait time.Duration) (res ctrl.Result, stop bool, err error) {
	anyUpdating := false
	anyGated := false
	// First StartingFresh denial this pass (Budget or UpdateGate), for
	// RecordRolloutHold — one Component has one hold slot, so the first
	// denial found (selection order) wins, matching the gate stack's own
	// first-denial-wins precedence.
	var firstDenial *types.RolloutHold
	// anyUpdateRan tracks whether ANY Update call fired this pass,
	// including one that returned done=true: even a done Update mutates
	// the row. Create must not run on an ObservedState an Update just
	// mutated — its Ready-promote and its ordinal read would both act on
	// the pre-Update row — so the pass requeues instead and the next
	// reconcile reads fresh state.
	anyUpdateRan := false
	// admitted marks a fresh start the admission let through this pass.
	admitted := false
	admission := newUpdateAdmission(input, selection, target)
	// A surge source leaves rotation only while the Component is at its
	// floor; below it every drain is withheld and the hold says why.
	if hold := admission.drainWithheld(); hold != nil {
		input.DrainGate = func([]string) (bool, types.RolloutHoldGate, string) {
			return false, hold.Gate, hold.Reason
		}
	}
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
				// A parked attempt names the wait its next attempt stands
				// behind; a denial here is that wait once the ladder admits.
				if row := input.ObservedState.Instance(item.Instance.Index); row != nil && types.OperationParked(row.Operation) && row.Operation.Waiting != string(denial.Gate) {
					if _, err := status.ApplyStamp(ctx, input, item.Instance.Index, status.ParkedAttemptWaits(string(denial.Gate))); err != nil {
						return ctrl.Result{}, false, fmt.Errorf("workload.Reconcile: record the wait on the parked attempt (instance=%d): %w", item.Instance.Index, err)
					}
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
			admitted = true
		}
		if !done {
			anyUpdating = true
		}
	}
	// A start the target's RetryBlock denied at the trigger stage never
	// reached the admission above; the block is the denial it stands on,
	// superseded by progress on the same terms as any other.
	if firstDenial == nil && len(selection.RetryBlockDenied) > 0 {
		firstDenial = selection.LadderHold
	}
	// The Instances already on the target that do not serve it hold the
	// roll whether or not a start reached the admission: with nothing
	// admitted, the hold they stand behind is the pass's verdict.
	if firstDenial == nil && !admitted {
		firstDenial = selection.StandingHold
	}
	if anyUpdateRan && len(selection.RolledNotServing) == 0 {
		// Forward progress this pass: whatever was gated for a DIFFERENT
		// Instance is superseded — the Component is not stuck, it will
		// re-observe fresh state (including any still-active gate) next
		// pass. Clearing here is also what lets a resolved hold disappear
		// promptly instead of lingering until the next denial-free pass.
		// A denial that stands on an Instance already rolled onto the
		// target that does not serve it is not superseded by an attempt
		// elsewhere: that attempt polling, or a rebuild of the set itself,
		// moves nothing the roll waits on, and every pass of the wait
		// reports the hold.
		firstDenial = nil
	}
	// A drain the gate held is progress withheld, not an admission denial:
	// it is reported even when another Instance moved this pass.
	if hold := input.DrainHolds.First(); hold != nil {
		firstDenial = hold
	}
	if input.RecordRolloutHold != nil {
		input.RecordRolloutHold(firstDenial)
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

// updateAdmission paces the fresh update starts of one pass. The pin is
// asked first and for every start: a roll executes a pinned plan, and no
// outage of the Instance being replaced waives that. The two capacity
// layers a start then answers to are consulted in order, against the same
// numbers repairAdmission uses: the per-Component budget on the arm the
// strategy uses (surge or unavailability), then the cross-Component
// coordination gate. Starts already in flight from a prior wake-up are
// anchored in the selection's Prior* counts, and the Instances the roll
// already moved onto the target that do not serve it hold a slot of the
// same budget; the pass's own starts are charged here as they open, so
// every later consult in the pass projects against the post-this-pass
// shape rather than firing every Instance in one shot.
type updateAdmission struct {
	selection *UpdateSelection
	target    *appsv1.ControllerRevision
	gate      func(strategy types.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, types.RolloutHoldGate, string)
	// planGate is the adapter's plan precondition; planAsked and planHold
	// keep its one verdict per pass, since the pin does not move within one.
	planGate  func() (bool, types.RolloutHoldGate, string)
	planAsked bool
	planHold  *types.RolloutHold

	// inFlightSurge and inFlightUnavail are the fresh starts this pass
	// charged to the per-Component budget, one per strategy arm.
	inFlightSurge   int32
	inFlightUnavail int32
	// replacedExtraSlots are the surge slots held by live extra pods that
	// a start admitted this pass replaces: that start is charged for the
	// slot, so the pods stop holding it against the starts after it.
	replacedExtraSlots int32
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
	return &updateAdmission{selection: selection, target: target, gate: input.UpdateGate, planGate: input.PlanGate}
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
	// The pin is asked for every start, the ones that skip the capacity
	// consult included: a start that takes nothing further out of serving
	// still rolls on the plan, and an unpinned plan admits no roll.
	if hold := a.pinHold(); hold != nil {
		return false, hold
	}
	rolled := int32(len(a.selection.RolledNotServing))
	if a.surge() {
		// An extra pod still listed holds a slot: a Terminating one inside
		// its grace, a live one until a start on its Instance evicts it.
		// The hold names them so the wait reads as the pods'.
		prior := a.selection.PriorSurgeInFlight + rolled + a.unreplacedExtraSlots(item)
		if projected, denied := escalation.BudgetDenies(a.selection.SurgeBudget, prior, a.inFlightSurge); denied {
			return false, &types.RolloutHold{
				Gate:   types.RolloutHoldGateBudget,
				Reason: fmt.Sprintf("per-Component surge budget %d exhausted (would become %d)%s%s", a.selection.SurgeBudget, projected, rolledNotServingClause(a.selection.RolledNotServing, a.target.Name), a.extraPodClause()),
				Target: a.target.Name,
			}
		}
	} else if projected, denied := escalation.BudgetDenies(a.selection.UnavailBudget, a.selection.PriorUnavailInFlight+rolled, a.inFlightUnavail); denied {
		return false, &types.RolloutHold{
			Gate:   types.RolloutHoldGateBudget,
			Reason: fmt.Sprintf("per-Component unavailability budget %d exhausted (would become %d)%s", a.selection.UnavailBudget, projected, rolledNotServingClause(a.selection.RolledNotServing, a.target.Name)),
			Target: a.target.Name,
		}
	}
	// A CoordGateExempt start skips the capacity consult: the gate already
	// counts its outage in its serving-based unavailability, so gating its
	// own recreate would double count and starve the recovery.
	if a.gate != nil && !item.CoordGateExempt {
		mechanism := a.mechanism(item)
		if allowed, gate, reason := a.gate(mechanism, a.inFlightSurge, a.gateUnavail); !allowed {
			logf.FromContext(ctx).V(1).Info("update start denied by coordination gate",
				"component", plan.Component, "instance", item.Instance.Index,
				"target", a.target.Name, "gate", gate, "reason", reason, "mechanism", mechanism,
				"inFlightSurge", a.inFlightSurge, "gateUnavail", a.gateUnavail)
			return false, &types.RolloutHold{Gate: gate, Reason: reason, Target: a.target.Name}
		}
	}
	return true, nil
}

// pinHold is the plan seam's verdict for the pass: nil while a run pins the
// plan or no seam is wired, else the hold every fresh start stands behind.
func (a *updateAdmission) pinHold() *types.RolloutHold {
	if a.planGate == nil {
		return nil
	}
	if !a.planAsked {
		a.planAsked = true
		if allowed, gate, reason := a.planGate(); !allowed {
			a.planHold = &types.RolloutHold{Gate: gate, Reason: reason, Target: a.target.Name}
		}
	}
	return a.planHold
}

// mechanism is the strategy arm item's start runs on, which is what the
// coordination gate paces by: the Component's strategy, or RecreatePod
// when an in-place strategy resolves to a rebuild on this Instance. The
// gate waives its capacity checks for an in-place start only because the
// patch returns the same pod; a fallback recreate drains the pods first,
// like any other drain-first start, and is consulted as one.
func (a *updateAdmission) mechanism(item UpdateItem) types.UpdateStrategyType {
	if item.RecreateFallback {
		return types.UpdateStrategyRecreatePod
	}
	return a.selection.Strategy
}

// drainWithheld is the hold that keeps every surge source in rotation
// this pass: the Instances already on the target revision that do not
// serve it exceed the unavailability budget, so the Component is below
// its floor and no further source may leave it. Nil under a non-surge
// strategy, an uncapped budget, or a floor that still holds.
func (a *updateAdmission) drainWithheld() *types.RolloutHold {
	return floorHold(a.selection, a.target)
}

// floorHold is drainWithheld's reading of the selection: the hold under
// which no surge source leaves rotation, nil when the floor holds.
func floorHold(selection *UpdateSelection, target *appsv1.ControllerRevision) *types.RolloutHold {
	rolled := int32(len(selection.RolledNotServing))
	if selection.Strategy != types.UpdateStrategySurgeThenDrain || selection.UnavailBudget == escalation.BudgetNoLimit || rolled <= selection.UnavailBudget {
		return nil
	}
	return &types.RolloutHold{
		Gate:   types.RolloutHoldGateBudget,
		Reason: fmt.Sprintf("per-Component unavailability budget %d exhausted%s; no source leaves rotation", selection.UnavailBudget, rolledNotServingClause(selection.RolledNotServing, target.Name)),
		Target: target.Name,
	}
}

// ladderHold is the hold the target's RetryBlock stands for when it
// denies a fresh start this pass: the Held gate for a held ladder, the
// RetryBlock gate for an attempt already in flight (the ladder admits
// one at a time) or a Backoff not yet due. Nil when the target carries
// no block in a denying state, by the trigger gate's own reading
// (workloadops.RetryBlockDenyingFreshStart), unless Instances already on
// the target crashed after promotion (crashedNotServing): the block
// counts their crash and its attempt is their rebuild, so while that
// attempt is due or under way (workloadops.RetryBlockOfCrashedAttempt)
// the ladder stays the hold the roll stands behind, through the rebuild
// and while the rebuilt set proves itself. Plan computes it once per
// pass: the update pass reports it for a start the trigger denied, and
// the idle verdict yields to it. The wording is the status layer's own
// reading of the block, with the Instances the roll waits on named and
// the Instances whose crashed set the ladder's attempt rebuilds
// (ladderRebuilds), so the operator reads one story whichever layer
// reports it and a reader pacing on the block's nextRetryAt knows every
// rebuild the attempt admits.
func ladderHold(input types.ReconcileInput, target *appsv1.ControllerRevision, rolledNotServing, crashedNotServing, ladderRebuilds []int32) *types.RolloutHold {
	b := workloadops.RetryBlockDenyingFreshStart(input, target)
	attemptOfCrashed := false
	if b == nil && len(crashedNotServing) > 0 {
		b, attemptOfCrashed = workloadops.RetryBlockOfCrashedAttempt(input, target), true
	}
	if b == nil {
		return nil
	}
	gate, state := types.RolloutHoldGateRetryBlock, ""
	switch b.State {
	case types.RetryBlockHeld:
		gate, state = types.RolloutHoldGateHeld, "held"
	case types.RetryBlockRetryInProgress:
		state = "retrying one attempt at a time"
	case types.RetryBlockBackoff:
		state = "in backoff"
		if attemptOfCrashed {
			state = "retrying one attempt at a time"
		}
	default:
		return nil
	}
	return &types.RolloutHold{
		Gate: gate,
		Reason: fmt.Sprintf("update to %s %s after %d failed attempt(s): %s%s%s", b.TargetRevision, state, b.AttemptsStarted, b.Reason,
			rolledNotServingClause(rolledNotServing, target.Name), ladderRebuildsClause(ladderRebuilds)),
		Target: target.Name,
	}
}

// standingHold is the hold the roll stands behind on account of the
// Instances already on the target that do not serve it (UpdateSelection.
// StandingHold), read as the update pass would report it with nothing
// admitted: the ladder's when the target's block denies a fresh start or
// counts their crash, which already names them; under SurgeThenDrain the
// floor's when they exceed the unavailability budget and no source may
// leave rotation; else the budget of the strategy's arm, projected as the
// admission projects a start, when those Instances and the starts in
// flight leave it no room. It stands only while one of them crashed
// after promotion, and the budget's only while one of those is out of
// rotation: a crashed set back in rotation is unproven, not down, and the
// window it has yet to hold Ready for paces a further start at the
// admission, which reports its own denial. Nil otherwise, or when a
// further start would still be admitted: the roll is then moving or
// complete, and holds nothing.
func standingHold(selection *UpdateSelection, target *appsv1.ControllerRevision) *types.RolloutHold {
	if len(selection.CrashedNotServing) == 0 {
		return nil
	}
	if selection.LadderHold != nil {
		return selection.LadderHold
	}
	if len(selection.CrashedOutOfRotation) == 0 {
		return nil
	}
	if hold := floorHold(selection, target); hold != nil {
		return hold
	}
	rolled := int32(len(selection.RolledNotServing))
	arm, budget, prior := "unavailability", selection.UnavailBudget, selection.PriorUnavailInFlight+rolled
	if selection.Strategy == types.UpdateStrategySurgeThenDrain {
		arm, budget, prior = "surge", selection.SurgeBudget, selection.PriorSurgeInFlight+rolled+selection.ExtraPodSurge.Slots
	}
	if _, denied := escalation.BudgetDenies(budget, prior, 0); !denied {
		return nil
	}
	return &types.RolloutHold{
		Gate:   types.RolloutHoldGateBudget,
		Reason: fmt.Sprintf("per-Component %s budget %d exhausted%s", arm, budget, rolledNotServingClause(selection.RolledNotServing, target.Name)),
		Target: target.Name,
	}
}

// rolledNotServingClause names, for a denial, the Instances already on
// the target revision that do not serve it; empty when there are none.
func rolledNotServingClause(rolled []int32, target string) string {
	if len(rolled) == 0 {
		return ""
	}
	noun, indices := instanceNoun(rolled)
	return fmt.Sprintf("; %s %s on revision %s not serving", noun, indices, target)
}

// ladderRebuildsClause names, for a ladder denial, the Instances whose
// crashed set the ladder's attempt rebuilds when it opens, one rebuild
// per Instance named; empty when the attempt rebuilds none.
func ladderRebuildsClause(rebuilds []int32) string {
	if len(rebuilds) == 0 {
		return ""
	}
	noun, indices := instanceNoun(rebuilds)
	return fmt.Sprintf("; the attempt rebuilds %s %s", noun, indices)
}

// instanceNoun is the noun and the comma-joined list a clause names a
// non-empty set of Instance indices with.
func instanceNoun(indices []int32) (string, string) {
	names := make([]string, 0, len(indices))
	for _, idx := range indices {
		names = append(names, strconv.Itoa(int(idx)))
	}
	noun := "Instance"
	if len(indices) > 1 {
		noun = "Instances"
	}
	return noun, strings.Join(names, ", ")
}

// unreplacedExtraSlots is what the Component's extra pods hold against
// the surge budget when item is consulted: every slot counted, less the
// ones starts admitted this pass already replace, less the ones item's
// own Instance holds with live pods — a start there evicts them before it
// creates anything, so the slot it takes is theirs.
func (a *updateAdmission) unreplacedExtraSlots(item UpdateItem) int32 {
	extra := a.selection.ExtraPodSurge
	return extra.Slots - a.replacedExtraSlots - extra.LiveSlotsByInstance[item.Instance.Index]
}

// extraPodClause names, for a surge denial, the extra pods still counted
// against the budget; empty when none are.
func (a *updateAdmission) extraPodClause() string {
	var clauses []string
	switch terminating := a.selection.ExtraPodSurge.Terminating; len(terminating) {
	case 0:
	case 1:
		clauses = append(clauses, fmt.Sprintf("Terminating pod %s still counts against the surge budget until its deletion grace elapses", terminating[0]))
	default:
		clauses = append(clauses, fmt.Sprintf("Terminating pods %s still count against the surge budget until their deletion grace elapses", strings.Join(terminating, ", ")))
	}
	switch live := a.selection.ExtraPodSurge.Live; len(live) {
	case 0:
	case 1:
		clauses = append(clauses, fmt.Sprintf("extra pod %s holds a surge slot until a start on its Instance replaces it", live[0]))
	default:
		clauses = append(clauses, fmt.Sprintf("extra pods %s hold surge slots until a start on their Instance replaces them", strings.Join(live, ", ")))
	}
	if len(clauses) == 0 {
		return ""
	}
	return "; " + strings.Join(clauses, "; ")
}

// charge counts a fresh start the pass opened, so every later consult
// projects against it. A surge start on an Instance whose live extra
// pods hold slots takes those slots over.
func (a *updateAdmission) charge(item UpdateItem) {
	if a.surge() {
		a.inFlightSurge++
		a.replacedExtraSlots += a.selection.ExtraPodSurge.LiveSlotsByInstance[item.Instance.Index]
		return
	}
	a.inFlightUnavail++
	if !item.CoordGateExempt {
		a.gateUnavail++
	}
}
