// plan_decision.go — the decision layer of the workload reconcile.
// Plan evaluates the pass triggers against the reconcile's single
// observation and produces a Decision; Execute (reconcile.go) applies
// it through the workload/ops state machines. Every SELECTION lives
// here, every EFFECT lives in Execute — so a Decision is unit-testable
// with a fake clock and recorder-instrumented callbacks.
package workload

import (
	"context"
	"fmt"
	"sort"
	"time"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
)

// ActionKind names one pass-level action of the reconcile pipeline.
type ActionKind string

const (
	// ActionScaleDown deletes the InstanceStatus indices the plan no
	// longer covers.
	ActionScaleDown ActionKind = "ScaleDown"
	// ActionDemote applies the truth pass: a status-only Ready→Pending
	// transition for Instances with no live pods and no in-flight
	// operation, where no op pass will act.
	ActionDemote ActionKind = "Demote"
	// ActionRestart advances the Restart state machine for the selected
	// Instances.
	ActionRestart ActionKind = "Restart"
	// ActionMigrateExpiry consumes expired Manual migration records.
	ActionMigrateExpiry ActionKind = "MigrateExpiry"
	// ActionMigrate runs the migrate pass over its selection: the
	// records parked on their source teardown, tended in place, then the
	// head record it drives.
	ActionMigrate ActionKind = "Migrate"
	// ActionUpdate runs the update pass over the selected Instances.
	ActionUpdate ActionKind = "Update"
	// ActionCreate materializes missing Instances (the op self-detects
	// missing pods per planned Instance).
	ActionCreate ActionKind = "Create"
)

// RestartSelection is one Instance the restart pass advances, with the
// trigger reason that lands on Operation.Reason on the first pass.
type RestartSelection struct {
	Instance types.InstancePlan
	Reason   string

	// OpensRepair marks a selection that opens a fresh crash-loop repair
	// this pass rather than driving one already in flight or recovering
	// capacity already lost. The executor opens at most
	// input.RepairBatchSize of them per pass.
	OpensRepair bool

	// OpensUnavailability marks the fresh repairs that take serving
	// capacity offline: a crash-loop repair of a pod set that still
	// serves. The executor puts exactly those to the per-Component
	// unavailability budget before they open, so a wedge shared by the
	// whole Component repairs at the operator's pace instead of recycling
	// every serving Instance at once.
	OpensUnavailability bool

	// LadderRevision names the revision whose retry ladder admits the
	// rebuild this selection opens, "" for an open the ladder does not
	// pace. The ladder authorizes one attempt at a revision at a time, so
	// the executor opens one such rebuild per revision per pass.
	LadderRevision string
}

// MigrationSelection is the migrate pass's work for one reconcile
// (copies of the ObservedState records), in the order the pass runs it.
type MigrationSelection struct {
	// Parked are the Draining records whose every live source pod is
	// Terminating past its own deletion deadline, oldest first. The
	// drive has issued every delete it owns for such a record and can
	// only wait for the kubelet, a finalizer owner or the force-delete
	// escalation to remove the pods, so the pass tends it — re-reads the
	// pair, runs the escalation it is configured for, completes it once
	// the pods are gone — ahead of the head instead of letting it be the
	// head.
	Parked []types.MigrationRecord

	// Head is the record the pass drives: the oldest one that is not
	// parked. Nil when every record is parked, or when the oldest one
	// is a fresh record the surge gate holds back.
	Head *types.MigrationRecord
}

// UpdateItem is one Instance the update pass touches, in plan order.
type UpdateItem struct {
	Instance types.InstancePlan
	// AdoptRevision: stamp Ready-on-target (the RunningRevision
	// backfill) instead of running the Update op — the Instance's
	// runtime-ready pods already match the target.
	AdoptRevision bool
	// StartingFresh: the Instance starts a new update rather than
	// continuing one (!types.UpdateContinuation), so it is admitted and charged.
	StartingFresh bool
	// CoordGateExempt: the fresh start skips the coordination gate consult
	// because it takes nothing further out of the gate's serving count
	// (types.RecreateOfDarkRow, a Ready row whose pod set is provably out
	// of service — evidence.PodSetServesNothing — or one the count already
	// excludes on a parked member — evidence.PodSetUnavailableOnParkedMember);
	// the plan gate and the per-Component budget still apply.
	CoordGateExempt bool
	// ReplacesDarkPodSet: the fresh start replaces an Instance that serves
	// nothing by the graced reading the budgets and the gate share
	// (replacesDarkPodSet).
	ReplacesDarkPodSet bool
	// OutOfRotation: the fresh start's Instance serves nothing at this
	// pass — a dark one (ReplacesDarkPodSet) or a pod set with no routed
	// pod in rotation, its grace run or not (evidence.PodSetOutOfRotation).
	// Taking it first costs no capacity, so such starts are listed ahead
	// of the rest and the budget reaches them before any serving Instance
	// is taken offline; the budgets and the gate still read the grace.
	OutOfRotation bool
	// RecreateFallback: the Component's in-place strategy runs as a
	// recreate on this Instance (ops.InPlaceFallsBackToRecreate: a gang, or
	// a single-pod diff beyond container images), so the fresh start is
	// consulted with the coordination gate as a RecreatePod start: it takes
	// the pods out of rotation before anything returns, which is the
	// capacity loss the gate waives only for a same-pod patch. Decided for
	// fresh starts only; a continuation is neither admitted nor consulted.
	RecreateFallback bool
	// CleanupOnly: the update trigger declined (zero revision distance —
	// the corrective roll-back — or a third-party-revision leftover) but
	// the instance carries superseded-revision wreckage
	// (ops.EvaluateWreckage). Execute routes the item into
	// ops.CleanupWreckage — abandon toward the current desired state —
	// instead of the Update op. Never budget-charged and never gated:
	// cleanup only removes dead pods and frees capacity, so gating it
	// would re-wedge the recovery the cleanup exists to unblock.
	CleanupOnly bool
}

// UpdateSelection carries the update pass's per-Instance selections
// plus the pure budget inputs the executor's within-pass counters
// project against.
type UpdateSelection struct {
	// Items in the order the executor admits them: the fresh starts of an
	// Instance out of rotation first (OutOfRotation), then plan order.
	// Instances held by the effective partition (canary) and Instances
	// with no trigger are not listed.
	Items []UpdateItem
	// Strategy is the resolved update strategy (empty Type defaults to
	// SurgeThenDrain — matches workload.BuildPlan's defaulting).
	// SurgeThenDrain doesn't take pods offline, so the executor gates
	// on the budget that actually applies to the strategy.
	Strategy types.UpdateStrategyType
	// SurgeBudget / UnavailBudget are the per-Component caps
	// (BudgetNoLimit when uncapped) — a distinct layer from the
	// cross-Component coordination-group gate (input.UpdateGate). Both
	// layers must allow before an Instance starts a fresh update; see
	// budget.go's package comment for the composition rule:
	//   effective_cap = min(group_cap, per_component_cap)
	SurgeBudget   int32
	UnavailBudget int32
	// PriorSurgeInFlight / PriorUnavailInFlight anchor the executor's
	// within-pass counters: operations in flight from prior wake-ups,
	// counted ONCE. Without this anchor the per-Component check would
	// re-count every iteration and double-charge the budget against
	// the same Instance.
	PriorSurgeInFlight   int32
	PriorUnavailInFlight int32
	// ExtraPodSurge is what the Component's extra pods hold against the
	// surge budget — Terminating inside their grace, or alive beyond an
	// Instance's own count — computed for a capped SurgeThenDrain budget
	// (escalation.ExtraPodSurgeInFlight).
	ExtraPodSurge escalation.ExtraPodSurge
	// RolledNotServing lists, in plan order, the Instances already on the
	// target revision that do not serve it (ops.RolledInstanceNotServing).
	// They are the roll's open work: each holds a slot in the budget of
	// the strategy's arm ahead of any fresh start, and under
	// SurgeThenDrain no source leaves rotation while they exceed the
	// unavailability budget.
	RolledNotServing []int32
	// RetryBlockDenied lists, in plan order, the Instances the target's
	// RetryBlock denied a fresh start at the trigger stage: a Held
	// ladder, an attempt already in flight, or a Backoff not yet due.
	// They never reach the pass's admission, so the block is the denial
	// the pass reports when nothing else progressed.
	RetryBlockDenied []int32
	// LadderHold is the hold the target's RetryBlock stands for when it
	// denies a fresh start this pass, nil when it denies none. One reading
	// serves the update pass, which reports it for the starts in
	// RetryBlockDenied, and the idle verdict, which yields to it: a ladder
	// that gave up on the revision is reported even when no candidate
	// reaches the trigger.
	LadderHold *types.RolloutHold
	// CrashedNotServing lists, in plan order, the Instances of
	// RolledNotServing whose pushed pod crashed after the row entered
	// Ready (ops.RolledInstanceCrashed): the roll's open work that no
	// other pass says anything about while the pod is up between
	// crashes.
	CrashedNotServing []int32
	// CrashedOutOfRotation lists, in plan order, the Instances of
	// CrashedNotServing whose set is short of fully in rotation
	// (ops.RolledSetInRotation): a pod down, restarting or not yet
	// serving. The rest are back in rotation and merely unproven for the
	// window, which paces a further start and nothing else. It is the
	// standing hold's reading alone: an item's OutOfRotation ordering key
	// asks whether the Instance serves nothing now, a different question.
	CrashedOutOfRotation []int32
	// StandingHold is the hold the roll stands behind on account of the
	// Instances already on the target that do not serve it, whether or
	// not a start reaches the admission this pass: the ladder's when the
	// target's block denies a fresh start or counts their crash, else the
	// budget's when those Instances and the starts in flight already
	// exhaust the strategy's arm. The budget's stands only while one of
	// them crashed after promotion and is out of rotation
	// (CrashedOutOfRotation): a rolled Instance short of serving for
	// another reason is repaired or parked by a pass that says so itself,
	// and one back in rotation leaves a roll with nothing to start nothing
	// to stand behind. Nil otherwise, when a further start would still be
	// admitted, or while paused. The update pass reports it when it admits
	// nothing, a roll with nothing selected reports it at the update
	// position (Decision.StandingHold), and a pass the restart pass
	// consumes reports it in the update pass's place.
	StandingHold *types.RolloutHold
}

// PlannedAction is one pass-level action selected for this reconcile.
// Exactly the field matching Kind is populated.
type PlannedAction struct {
	Kind ActionKind

	// Extras are the scale-down target indices (ActionScaleDown).
	Extras []int32

	// Demotions are the truth-pass targets (ActionDemote).
	Demotions []types.DemotionSelection

	// Restarts are the Instances the restart pass advances
	// (ActionRestart).
	Restarts []RestartSelection

	// Migration is the migrate pass's selection (ActionMigrate).
	Migration *MigrationSelection

	// Update is the update pass's selection (ActionUpdate).
	Update *UpdateSelection
}

// Decision is the outcome of Plan for one reconcile: the ordered
// pass-level actions to run. Ordering is the pass precedence
// (scale-down > restart > migration expiry > migration > update >
// create); Execute runs the actions in order and stops at the first
// one whose op outcome requires a requeue, so the ordering IS the
// precedence. The ops are step machines advanced once per reconcile —
// a Decision selects WHICH op advances for WHICH instances, not the
// op's internal effect sequence.
type Decision struct {
	// Actions are the selected pass-level actions,
	// first-precedence-first. A paused plan drops the Migration
	// actions and narrows Update and Create to the attempts already in
	// flight: a deliberate replica reduction still releases capacity,
	// the RestartPolicy keeps repairing existing Instances, and an open
	// attempt runs its current step to the step's boundary, but nothing
	// new starts. A frozen pause (PauseFreeze) narrows the restart pass
	// the same way — no repair starts, one under way finishes its step.
	// The truth pass (ActionDemote) is status-only, selected only while
	// paused (in every depth): pause suspends lifecycle operations,
	// never status truth, while unpaused reconciles leave the correction
	// to the status publication, which demotes the same shape at the end
	// of every pass, and to the pass that rebuilds the row.
	Actions []PlannedAction

	// RequeueAfter is the earliest not-yet-due Backoff RetryBlock
	// wake-up reported by the update-trigger evaluation — the one
	// requeue the decision layer owns without running an op. The
	// executor folds it additively into the pass result
	// (foldRetryAfter) so the gate is re-evaluated on time; it only
	// ever ADDS a wake-up, never delays one an op scheduled.
	RequeueAfter time.Duration

	// UpdateIdle reports that the update selection ran for an unpaused
	// Component and found nothing: no Instance to drive, no same-target
	// RetryBlock that still denies a start, and no Instance on the target
	// that crashed after promotion and is out of rotation or under the
	// ladder's attempt. Execute reports a nil rollout hold at the update
	// pass's position, so a hold a prior pass recorded does not outlive
	// the roll it paced or the loss it named, while a ladder that gave up
	// on the revision stays reported.
	UpdateIdle bool

	// LadderHold is the hold an unpaused roll waits on when the target's
	// RetryBlock denies every start at the trigger stage and nothing else
	// is selected (UpdateSelection.LadderHold with no items). Execute
	// reports it at the update pass's position, so the wait says what
	// the ladder's attempt rebuilds on every pass that reaches it.
	LadderHold *types.RolloutHold

	// StandingHold is the hold an unpaused roll with nothing selected
	// stands behind on account of the Instances already on the target that
	// are out of rotation because a pushed pod crashed after promotion
	// (UpdateSelection.StandingHold with no items). Execute reports it at
	// the update pass's position after the ladder's hold, and a pass the
	// restart pass consumes reports it there in the update pass's place.
	StandingHold *types.RolloutHold

	// Owned is the pass's rows grouped by the owner that may advance
	// them. Built once here, from the ownership table; Execute hands it
	// to every pass so a verb reads the list it owns rather than
	// deciding eligibility row by row for itself.
	Owned types.OwnedRows

	// Escalate gates the terminal-failure escalation pass (the
	// escalation package). False while paused: the repair decisions are
	// suspended with the rest of the lifecycle machinery, and the executor
	// runs the pass's evidence half alone so the hold authorities keep
	// reporting what they see (deadlines are parked by the adapter while
	// paused). The executor separately guards status-commit boundaries and
	// excludes deferred scale-down extras.
	Escalate bool
}

// Plan is the pure decision layer: it evaluates the pass triggers in
// precedence order and returns the Decision Execute applies.
//
// PURITY CONTRACT: Plan performs NO mutations of any kind — no client
// writes, no events, no MutateInstance / ApplyInstanceMutations /
// MutateRetryBlock / MutateMigration / AppendMigration / RemoveInstance
// calls, no expectation records. It reads ONLY the snapshot's pod buckets,
// input.ObservedState + input.DesiredSpec, and the injected clock
// (input.Now). input.UpdateGate is a decision input by contract, but
// Execute consults it at the update pass's position so the gate
// observes live peer/self state after earlier passes' effects have
// landed, exactly as the pass pipeline did.
func Plan(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) (Decision, error) {
	var decision Decision

	// One owner grouping for the whole pass, ahead of every selection:
	// the triggers below and the verb passes Execute dispatches all read
	// their own rows out of it.
	decision.Owned = types.RowsByOwner(input.ObservedState.InstanceStatuses)
	input.Owned = &decision.Owned

	// Scale-down selection: indices observed but no longer planned.
	// Runs before everything else so excess pods don't run alongside
	// in-flight drains.
	if extras := ScaleDownExtras(input.ObservedState.InstanceStatuses, plan); len(extras) > 0 || decision.Owned.Any(types.OwnerDelete) {
		decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionScaleDown, Extras: extras})
	}

	// Restart selection, evaluated ahead of the truth pass because the
	// truth pass leaves the rows the repair claims. A frozen pause
	// suspends the pass, so the selection is only worth computing — it
	// costs a live pod read — when a repair is already under way to
	// finish.
	var restarts []RestartSelection
	if !plan.PauseFreeze || anyRestartContinuation(input) {
		selected, err := planRestartSelections(ctx, input, plan, target, snapshot)
		if err != nil {
			return Decision{}, err
		}
		if plan.PauseFreeze {
			selected = restartContinuations(input, selected)
		}
		restarts = selected
	}

	// Truth pass, paused reconciles only: a Ready Instance whose pods are
	// all gone, with no operation in flight and no op pass that will act
	// (pause parks the Create pass that would otherwise re-materialize
	// it, and the restart pass has not selected the row — a freeze
	// suspends it), must not keep claiming Ready. Status-only; selected
	// here, applied by Execute; runs in every pause depth because pause
	// suspends lifecycle operations, never status truth. Unpaused
	// reconciles skip it: the status publication demotes the same shape
	// (status.DemotableReady) at the end of every pass, and the pass that
	// owns the rebuild re-materializes the row when it runs. The paused
	// pass keeps its own correction because it confirms the loss against
	// a live read and announces it.
	if plan.Paused {
		demotions, err := planUnbackedDemotions(ctx, input, plan, snapshot, restarts)
		if err != nil {
			return Decision{}, err
		}
		if len(demotions) > 0 {
			decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionDemote, Demotions: demotions})
		}
	}

	// Paused is an operator circuit breaker over WHAT MAY BEGIN:
	// scale-down above still releases capacity, the restart pass below
	// keeps repairing existing Instances at their current revision (the
	// pause withholds the roll, so a rebuild under it never follows the
	// target, and an Instance whose revision is not the roll target is the
	// roll's), and an attempt already in flight still runs the step it is
	// on to that
	// step's boundary. What a pause withholds is the next operation and
	// the next step, so a pause never leaves an Instance mid-step — dark
	// between a delete and its recreate, or half-materialized — for the
	// length of the hold. PauseFreeze deepens the hold onto the restart
	// pass: no repair starts, and one already under way only finishes
	// its step. Pod and spec watches enqueue the workload again after
	// unpause; no periodic requeue is needed while the desired state is
	// intentionally held.
	if !plan.Paused {
		decision.Escalate = true
	}

	if len(restarts) > 0 {
		decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionRestart, Restarts: restarts})
	}

	if !plan.Paused {
		// Migration expiry selection — the deadline consumer. Planned
		// BEFORE the drive pass so an expired record is consumed before it
		// can be driven (re-stamped) again, and regardless of MigrationMode
		// so a mode flip to Never can never strand a non-terminal record.
		if workloadops.HasExpiredMigrationCandidate(input.ObservedState.Migrations, input.Now()) {
			decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionMigrateExpiry})
		}

		// Migration drive selection. Work comes from the owner's
		// status.migrations records, oldest first: the records parked on
		// their source teardown are tended, and the oldest record that is
		// not parked is the head the pass drives. Terminal records and
		// Auto records (born terminal) are excluded structurally —
		// records, never work. Skip when mode is Never.
		if plan.MigrationMode != types.MigrationModeNever {
			selection, err := planMigrationDrive(ctx, input, snapshot)
			if err != nil {
				return Decision{}, err
			}
			if selection != nil {
				decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionMigrate, Migration: selection})
			}
		}
	}

	// Update selection. A nil target short-circuits the pass
	// (DesiredSpec.PodSpec nil / MinReplicas=0). While paused only an
	// attempt already in flight is selected, so the executor's budget
	// and gate layers decide nothing: a held Component starts nothing
	// for them to pace.
	if target != nil && (!plan.Paused || anyUpdateContinuation(input, plan)) {
		selection, retryBlockWait, err := planUpdateSelection(ctx, input, plan, target, snapshot)
		if err != nil {
			return Decision{}, err
		}
		if plan.Paused {
			// A RetryBlock wake-up paces a fresh re-trigger, which a pause
			// withholds; the unpause is the wake-up that matters here. The
			// pause is the roll's hold; what its parked Instances stand
			// behind is reported once the roll may move again.
			selection.Items = pausedUpdateContinuations(input, selection.Items)
			selection.StandingHold = nil
		} else {
			decision.RequeueAfter = retryBlockWait
			// A roll with nothing left to do holds nothing: no Instance to
			// drive, no ladder that still holds the target, whether a
			// candidate reached its gate or a partition kept every one back,
			// and no Instance on the target out of rotation after a crash,
			// or proving itself under the ladder's attempt, that holds the
			// roll.
			decision.UpdateIdle = len(selection.Items) == 0 && len(selection.RetryBlockDenied) == 0 && selection.LadderHold == nil && selection.StandingHold == nil
			// A roll with nothing to run but a hold to report waits on it:
			// the ladder's when the target's block denies every start, else
			// the one its parked Instances stand behind. Either is reported
			// at the update position.
			if len(selection.Items) == 0 {
				decision.LadderHold = selection.LadderHold
				decision.StandingHold = selection.StandingHold
			}
		}
		if len(selection.Items) > 0 {
			decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionUpdate, Update: &selection})
		}
	}

	// Create — always planned on a non-paused reconcile: the op is the
	// pass's own detector (it lists and diffs per planned Instance) and
	// a nil target returns immediately. While paused it is planned only
	// for an index whose committed create the pass would finish, and
	// Execute narrows the pass to those: an index with no row stays
	// empty, but a set already being materialized is completed and
	// promoted rather than held incomplete with the Instance short of it.
	if !plan.Paused || workloadops.HasCommittedCreate(input, plan, target) {
		decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionCreate})
	}

	return decision, nil
}

// pausedUpdateContinuations keeps the update items a paused Component may
// still run: an attempt already in flight, resuming on the step it is
// already on. A fresh start, a revision backfill, and a wreckage cleanup
// are new work and wait for the unpause.
//
// A gang SurgeThenDrain cycle is excluded whole. It spans two rows — the
// source and the replacement-gang marker it pins — and keeps the source
// serving from its first pass to its last, so parking it costs the
// Instance no capacity and closes no step half-open.
func pausedUpdateContinuations(input types.ReconcileInput, items []UpdateItem) []UpdateItem {
	kept := items[:0]
	for _, item := range items {
		if item.StartingFresh || item.AdoptRevision || item.CleanupOnly {
			continue
		}
		row := input.ObservedState.Instance(item.Instance.Index)
		if row == nil || gangSurgeRow(row) {
			continue
		}
		kept = append(kept, item)
	}
	return kept
}

// anyUpdateContinuation reports whether any planned index carries an
// Update attempt a paused Component would still advance. The claim, not
// the owner: a Failed row whose preserved operation is an Update is a
// continuation the selection keeps (see startingFresh), so the pause
// re-drives it rather than leaving a half-done roll parked.
func anyUpdateContinuation(input types.ReconcileInput, plan types.ComponentPlan) bool {
	for _, inst := range plan.Instances {
		row := input.ObservedState.Instance(inst.Index)
		if row == nil || gangSurgeRow(row) {
			continue
		}
		if types.ClaimOf(row) == types.OwnerUpdate {
			return true
		}
	}
	return false
}

// gangSurgeRow reports whether the row belongs to a gang SurgeThenDrain
// cycle: the source that pins a replacement index, or the replacement
// marker itself.
func gangSurgeRow(row *types.InstanceStatus) bool {
	op := row.Operation
	if op == nil || op.Type != types.InstanceOperationUpdate {
		return false
	}
	return op.SurgeIndex != nil ||
		op.Step == types.UpdateStepGangSurgeTarget ||
		op.Step == types.UpdateStepGangSurgeTargetCleanup
}

// anyRestartContinuation reports whether any row carries an in-flight
// Restart attempt.
func anyRestartContinuation(input types.ReconcileInput) bool {
	return input.OwnedRows().Any(types.OwnerRestart)
}

// restartContinuations keeps only the selections that advance a repair
// already under way. A frozen pause starts no repair, but the pods an
// open one deleted must still be recreated at the bumped Incarnation
// rather than stay missing for the length of the hold.
func restartContinuations(input types.ReconcileInput, restarts []RestartSelection) []RestartSelection {
	owned := input.OwnedRows()
	kept := restarts[:0]
	for _, selection := range restarts {
		if owned.Owns(types.OwnerRestart, selection.Instance.Index) {
			kept = append(kept, selection)
		}
	}
	return kept
}

// planRestartSelections selects the per-Instance restart triggers:
// pod loss, a Failed pod, a lost gang member, an operation-free crash
// loop, and an open repair that must keep advancing. Selection is
// evaluated against the LIVE pod read — restart is destructive and must
// not select from a stale cache.
//
// A repair is opened only while the revision the Instance runs is still
// the Component's target. An Instance off the target belongs to the pass
// that rebuilds it AT the target (workloadops.RepairYieldsToTarget): its
// outage is one the roll crosses anyway, and a repair would rebuild what
// the roll is about to tear down. An open Restart is driven on regardless
// — a rebuild that has created no pod yet follows the target itself — and
// a paused Component keeps every repair, because the roll it would yield
// to is the very thing the pause withholds.
//
// The live read is unconditional only under the policy whose triggers
// fire on ordinary pod churn. The two policy-independent triggers are
// rare, so for every other policy the cached view screens for a
// candidate first and the live read confirms it; a healthy Component
// therefore costs no extra live List per reconcile.
func planRestartSelections(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) ([]RestartSelection, error) {
	if plan.RestartPolicy != types.RestartPolicyRecreateInstance {
		cachedByInstance, err := snapshot.CachedPods(ctx)
		if err != nil {
			return nil, fmt.Errorf("workload.Reconcile: list pods to screen the restart pass (component=%s): %w", plan.Component, err)
		}
		if !anyRestartTrigger(input, plan, target, cachedByInstance) {
			return nil, nil
		}
	}
	liveByInstance, err := snapshot.LivePods(ctx)
	if err != nil {
		return nil, fmt.Errorf("workload.Reconcile: list pods for restart pass (component=%s): %w", plan.Component, err)
	}
	var restarts []RestartSelection
	for _, inst := range plan.Instances {
		needs, reason := restartTrigger(input, plan, inst, target, liveByInstance[inst.Index])
		if !needs {
			continue
		}
		restarts = append(restarts, RestartSelection{
			Instance:            inst,
			Reason:              reason,
			OpensRepair:         workloadops.RestartOpensRepair(input, plan, inst, liveByInstance[inst.Index]),
			OpensUnavailability: workloadops.RestartOpensUnavailability(input, plan, inst, liveByInstance[inst.Index]),
			LadderRevision:      workloadops.RestartOpensLadderAttempt(input, plan, inst, liveByInstance[inst.Index]),
		})
	}
	return restarts, nil
}

// restartTrigger is the restart pass's selection for one Instance: the
// trigger, bounded by the roll target.
func restartTrigger(input types.ReconcileInput, plan types.ComponentPlan, inst types.InstancePlan, target *appsv1.ControllerRevision, pods []*corev1.Pod) (bool, string) {
	needs, reason := workloadops.DetectRestartTriggerWithPods(input, plan, inst, pods)
	if !needs || workloadops.RepairYieldsToTarget(input, plan, inst, target, pods) {
		return false, ""
	}
	return true, reason
}

// anyRestartTrigger reports whether any planned Instance would be
// selected against the supplied pod buckets.
func anyRestartTrigger(input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, byInstance map[int32][]*corev1.Pod) bool {
	for _, inst := range plan.Instances {
		if needs, _ := restartTrigger(input, plan, inst, target, byInstance[inst.Index]); needs {
			return true
		}
	}
	return false
}

// planUnbackedDemotions selects Ready Instances with no live pods and no
// in-flight Operation for a status-only demotion to Pending. Phase is
// op-owned, so this narrow observation-only correction fires exclusively
// where no op pass will (status.DemotableReady): an Operation in any
// state keeps ownership with its op, and a row the restart pass selected
// this reconcile is left to the repair it opens — under
// RecreateInstanceOnPodRestart and a plain pause that is every such row,
// while a frozen pause suspends the pass and the row is demoted; the
// running revision it keeps is what the repair rebuilds at once the pass
// runs again. Extras belong to scale-down. Candidates are screened on the
// cached read and confirmed against the live read, so the demotion can
// never fire on cache lag; a Terminating pod still counts as live,
// deferring the correction until the loss is total and settled.
func planUnbackedDemotions(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, snapshot *ObservedSnapshot, restarts []RestartSelection) ([]types.DemotionSelection, error) {
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	repairing := make(map[int32]struct{}, len(restarts))
	for _, selection := range restarts {
		repairing[selection.Instance.Index] = struct{}{}
	}
	var candidates []int32
	for i := range input.ObservedState.InstanceStatuses {
		row := &input.ObservedState.InstanceStatuses[i]
		if _, ok := planned[row.Index]; !ok {
			continue
		}
		if _, ok := repairing[row.Index]; ok {
			continue
		}
		if !status.DemotableReady(row) {
			continue
		}
		candidates = append(candidates, row.Index)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	cached, err := snapshot.CachedPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("workload.Reconcile: list pods for truth pass (component=%s): %w", plan.Component, err)
	}
	unbacked := candidates[:0]
	for _, idx := range candidates {
		if len(cached[idx]) == 0 {
			unbacked = append(unbacked, idx)
		}
	}
	if len(unbacked) == 0 {
		return nil, nil
	}
	live, err := snapshot.LivePods(ctx)
	if err != nil {
		return nil, fmt.Errorf("workload.Reconcile: confirm pods for truth pass (component=%s): %w", plan.Component, err)
	}
	var out []types.DemotionSelection
	for _, idx := range unbacked {
		if len(live[idx]) == 0 {
			out = append(out, types.DemotionSelection{Index: idx, Reason: "no live pods back the Ready phase"})
		}
	}
	return out, nil
}

// planUpdateSelection evaluates the per-Instance update triggers
// (pure) over the snapshot's cached pods and returns the selection
// plus the earliest not-yet-due RetryBlock wake-up across denied
// Instances.
func planUpdateSelection(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) (UpdateSelection, time.Duration, error) {
	var rollingUpdate *types.RollingUpdate
	if plan.UpdateStrategy.RollingUpdate != nil {
		rollingUpdate = plan.UpdateStrategy.RollingUpdate
	}
	strategy := plan.UpdateStrategy.Type
	if strategy == "" {
		strategy = types.UpdateStrategySurgeThenDrain
	}
	selection := UpdateSelection{
		Strategy:             strategy,
		SurgeBudget:          escalation.PerComponentMaxSurgeBudget(rollingUpdate, plan.Replicas),
		UnavailBudget:        escalation.PerComponentMaxUnavailableBudget(rollingUpdate, plan.Replicas),
		PriorSurgeInFlight:   escalation.CurrentSurgeInFlight(input.ObservedState.InstanceStatuses),
		PriorUnavailInFlight: escalation.CurrentUnavailableInFlight(input.ObservedState.InstanceStatuses),
	}
	// One cached List + bucket for the whole Component (the snapshot's
	// non-destructive read source) — an absent bucket is an empty pod
	// set.
	updateByInstance, err := snapshot.CachedPods(ctx)
	if err != nil {
		return UpdateSelection{}, 0, fmt.Errorf("workload.Reconcile: list pods for update pass (component=%s): %w", plan.Component, err)
	}
	// The hold reads the projected pacing partition first (a canary step
	// or plan-gate hold), then the user's rollingUpdate partition.
	partition := workloadops.EffectivePartition(input.DesiredSpec.Pacing, rollingUpdate)
	heldIndices := workloadops.PartitionHeldIndices(partition, input, plan.Instances, target.Name)
	// The window a promoted pod that restarted must serve again before
	// the roll reads it as serving: minReadySeconds floored by the
	// stuck-pod grace.
	provenWindow := workloadops.ProvenWindowSeconds(plan.MinReadySeconds, input.StuckPodGrace)
	var retryBlockWait time.Duration
	for _, inst := range plan.Instances {
		// Partition hold (canary): hold the selected Instances on their
		// current revision — skip their update. Membership is keyed to
		// revision, not index position, and Instances already converging
		// to target are never held (they must finish); see
		// PartitionHeldIndices for the full candidacy rule.
		if heldIndices[inst.Index] {
			logf.FromContext(ctx).V(1).Info("update not selected: held by partition",
				"component", plan.Component, "instance", inst.Index, "target", target.Name)
			continue
		}
		if notServing, wait, why := workloadops.RolledInstanceNotServing(input.ObservedState.Instance(inst.Index), target.Name, target.CreationTimestamp.Time, updateByInstance[inst.Index], inst.TotalPods(), provenWindow, input.Now()); notServing {
			// A shortfall of dark pods alone is not the roll's open work:
			// nothing those pods do lifts it, so the Instance holds no slot.
			dark, graceLeft := workloadops.RolledInstanceDark(input.ObservedState.Instance(inst.Index), target.Name, target.CreationTimestamp.Time, updateByInstance[inst.Index], inst.TotalPods(), provenWindow, input.StuckPodGrace, input.Now())
			if dark {
				logf.FromContext(ctx).V(1).Info("rolled Instance dark: not the roll's open work",
					"component", plan.Component, "instance", inst.Index, "target", target.Name, "reason", why)
			} else {
				selection.RolledNotServing = append(selection.RolledNotServing, inst.Index)
				if workloadops.RolledInstanceCrashed(input.ObservedState.Instance(inst.Index), target.Name, target.CreationTimestamp.Time, updateByInstance[inst.Index], provenWindow) {
					selection.CrashedNotServing = append(selection.CrashedNotServing, inst.Index)
					if !workloadops.RolledSetInRotation(target.Name, updateByInstance[inst.Index], inst.TotalPods()) {
						selection.CrashedOutOfRotation = append(selection.CrashedOutOfRotation, inst.Index)
					}
				}
				logf.FromContext(ctx).V(1).Info("rolled Instance not serving",
					"component", plan.Component, "instance", inst.Index, "target", target.Name, "reason", why, "wait", wait, "graceLeft", graceLeft)
				// Neither a pod Ready again inside its window nor one unready
				// inside its grace raises a watch event when the window or
				// the grace elapses; the pass comes back for it.
				if wait > 0 {
					input.PassWake.Observe(wait)
				}
				if graceLeft > 0 {
					input.PassWake.Observe(graceLeft)
				}
			}
		}
		decision := workloadops.EvaluateUpdateTrigger(input, inst, target, updateByInstance[inst.Index])
		if decision.AdoptRevision {
			logf.FromContext(ctx).V(1).Info("update not selected: adopting revision in place",
				"component", plan.Component, "instance", inst.Index, "target", target.Name)
			selection.Items = append(selection.Items, UpdateItem{Instance: inst, AdoptRevision: true})
			continue
		}
		if !decision.Trigger {
			if decision.RetryBlockDenied {
				selection.RetryBlockDenied = append(selection.RetryBlockDenied, inst.Index)
			}
			// Denied by a not-yet-due Backoff RetryBlock — keep the
			// earliest re-evaluation time across Instances.
			if decision.RetryAfter > 0 && (retryBlockWait == 0 || decision.RetryAfter < retryBlockWait) {
				retryBlockWait = decision.RetryAfter
			}
			// Wreckage scan: the trigger is revision-diff-keyed, so an
			// instance at zero revision distance (corrective roll-back)
			// or carrying third-party-revision debris never dispatches —
			// yet its superseded-revision wreckage must still be
			// abandoned toward the current desired state. Pure snapshot
			// read; the effect runs in Execute (ops.CleanupWreckage).
			row := input.ObservedState.Instance(inst.Index)
			cleanup := workloadops.EvaluateWreckage(row, target, updateByInstance[inst.Index])
			if cleanup {
				selection.Items = append(selection.Items, UpdateItem{Instance: inst, CleanupOnly: true})
			}
			// The trigger declining is the single most common reason a
			// rollout makes no progress, and the phase/revision pair it
			// keyed on is not otherwise recoverable after the fact.
			log := logf.FromContext(ctx).V(1)
			if row == nil {
				log.Info("update not triggered: no observed status",
					"component", plan.Component, "instance", inst.Index,
					"target", target.Name, "cleanupOnly", cleanup)
			} else {
				log.Info("update not triggered",
					"component", plan.Component, "instance", inst.Index,
					"phase", row.Phase, "runningRevision", row.RunningRevision,
					"target", target.Name, "hasOperation", row.Operation != nil,
					"retryAfter", decision.RetryAfter, "cleanupOnly", cleanup)
			}
			continue
		}
		row := input.ObservedState.Instance(inst.Index)
		startingFresh := !types.UpdateContinuation(row)
		pods := updateByInstance[inst.Index]
		// The mechanism the start runs on: the shared rules the update op
		// dispatches on, judged against the same target and the same
		// recorded running revision (a snapshot read, memoized per revision).
		recreateFallback := false
		if startingFresh {
			fallback, ferr := workloadops.InPlaceFallsBackToRecreate(strategy, inst.TotalPods() > 1, input.DesiredSpec.PodSpec,
				func() (*corev1.PodSpec, error) { return snapshot.RunningRevisionPodSpec(ctx, inst.Index) })
			if ferr != nil {
				return UpdateSelection{}, 0, fmt.Errorf("workload.Reconcile: resolve update mechanism (instance=%d): %w", inst.Index, ferr)
			}
			recreateFallback = fallback || workloadops.SurgeHasNoSource(row)
			// A step that replaces no container cannot restore a pod set dark
			// on readiness, so such an in-place start runs as a recreate and
			// is consulted as the drain-first start it is.
			if !recreateFallback && strategy == types.UpdateStrategyInPlaceIfPossible {
				running, rerr := snapshot.RunningRevisionPodSpec(ctx, inst.Index)
				if rerr != nil {
					return UpdateSelection{}, 0, fmt.Errorf("workload.Reconcile: resolve update mechanism (instance=%d): %w", inst.Index, rerr)
				}
				recreateFallback = workloadops.InPlaceCannotRestore(running, input.DesiredSpec.PodSpec, pods, input.Now(), input.StuckPodGrace)
			}
		}
		mechanism := strategy
		if recreateFallback {
			mechanism = types.UpdateStrategyRecreatePod
		}
		// The gate's serving count needs every pod of an Instance in
		// rotation, so a Ready Instance with a member parked is already
		// inside the gate's unavailability, and a drain-first start on it
		// skips the consult for its own outage; the per-Component budget
		// still charges the start when a routed member serves.
		gateExempt := types.RecreateOfDarkRow(row, mechanism) ||
			(startingFresh && mechanism != types.UpdateStrategySurgeThenDrain &&
				row != nil && row.Phase == types.InstancePhaseReady &&
				(evidence.PodSetServesNothing(pods, input.Now(), input.StuckPodGrace) || evidence.PodSetUnavailableOnParkedMember(pods, inst.TotalPods(), input.Now(), input.StuckPodGrace)))
		dark := startingFresh && replacesDarkPodSet(input, row, pods)
		outOfRotation := startingFresh && (dark || evidence.PodSetOutOfRotation(pods))
		logf.FromContext(ctx).V(1).Info("update selected",
			"component", plan.Component, "instance", inst.Index, "target", target.Name,
			"startingFresh", startingFresh, "coordGateExempt", gateExempt, "recreateFallback", recreateFallback,
			"replacesDarkPodSet", dark, "outOfRotation", outOfRotation)
		selection.Items = append(selection.Items, UpdateItem{Instance: inst, StartingFresh: startingFresh, CoordGateExempt: gateExempt, ReplacesDarkPodSet: dark, OutOfRotation: outOfRotation, RecreateFallback: recreateFallback})
	}
	// A start on an Instance out of rotation restores capacity while every
	// other start spends it, so the budget is offered to those first, from
	// the moment the Instance left rotation rather than once its grace has
	// run; within each group plan order stands. The order is all this
	// decides: the budgets and the gate admit or deny each start on their
	// own terms, and the partition has already chosen who is listed.
	sort.SliceStable(selection.Items, func(i, j int) bool {
		return selection.Items[i].OutOfRotation && !selection.Items[j].OutOfRotation
	})
	// An extra pod the API still lists holds a surge slot: a Terminating
	// one until it exits or passes its deletion deadline, which raises no
	// watch event, so a pass with a fresh start to admit wakes for it; a
	// live one until a start on its Instance replaces it.
	if selection.Strategy == types.UpdateStrategySurgeThenDrain && selection.SurgeBudget != escalation.BudgetNoLimit {
		now := input.Now()
		selection.ExtraPodSurge = escalation.ExtraPodSurgeInFlight(plan, input.ObservedState.InstanceStatuses, updateByInstance, selection.RolledNotServing, now)
		if !selection.ExtraPodSurge.NextRelease.IsZero() && anyFreshStart(selection.Items) {
			input.PassWake.Observe(selection.ExtraPodSurge.NextRelease.Sub(now))
		}
	}
	// Budgets are decided here but spent in Execute, so record them with the
	// selection they apply to: a selection that is non-empty yet starts
	// nothing is a budget or gate outcome, not a selection one.
	logf.FromContext(ctx).V(1).Info("update selection complete",
		"component", plan.Component, "target", target.Name,
		"considered", len(plan.Instances), "selected", len(selection.Items),
		"strategy", selection.Strategy, "surgeBudget", selection.SurgeBudget,
		"unavailBudget", selection.UnavailBudget,
		"priorSurgeInFlight", selection.PriorSurgeInFlight,
		"priorUnavailInFlight", selection.PriorUnavailInFlight,
		"extraPodSurge", selection.ExtraPodSurge.Slots,
		"terminatingPods", selection.ExtraPodSurge.Terminating,
		"liveExtraPods", selection.ExtraPodSurge.Live,
		"rolledNotServing", selection.RolledNotServing,
		"crashedNotServing", selection.CrashedNotServing,
		"crashedOutOfRotation", selection.CrashedOutOfRotation,
		"retryBlockDenied", selection.RetryBlockDenied)
	selection.LadderHold = ladderHold(input, target, selection.RolledNotServing, selection.CrashedNotServing, ladderRebuilds(input, plan, target, updateByInstance))
	selection.StandingHold = standingHold(&selection, target)
	return selection, retryBlockWait, nil
}

// ladderRebuilds lists, in plan order, the Instances whose crashed set
// the target's retry ladder rebuilds in its attempt
// (workloadops.LadderAttemptRebuilds), for the ladder hold to name.
func ladderRebuilds(input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, byInstance map[int32][]*corev1.Pod) []int32 {
	var rebuilds []int32
	for _, inst := range plan.Instances {
		if workloadops.LadderAttemptRebuilds(input, plan, inst, target.Name, byInstance[inst.Index]) {
			rebuilds = append(rebuilds, inst.Index)
		}
	}
	return rebuilds
}

// updateSelection is the selection the update pass runs this reconcile,
// nil when none is planned.
func (d Decision) updateSelection() *UpdateSelection {
	for _, action := range d.Actions {
		if action.Kind == ActionUpdate {
			return action.Update
		}
	}
	return nil
}

// anyFreshStart reports whether the selection carries a start the pass
// would admit against the budget.
func anyFreshStart(items []UpdateItem) bool {
	for _, item := range items {
		if item.StartingFresh {
			return true
		}
	}
	return false
}

// replacesDarkPodSet reports whether a fresh start on row replaces an
// Instance that serves nothing: a Failed row with no serving pod (the
// counter types.RecreateOfDarkRow reads), or a pod set provably out of
// service (evidence.PodSetServesNothing, the reading a Ready row is dark
// on). Replacing such an Instance takes no capacity out of rotation.
func replacesDarkPodSet(input types.ReconcileInput, row *types.InstanceStatus, pods []*corev1.Pod) bool {
	if row != nil && row.Phase == types.InstancePhaseFailed && row.ServingPodCount == 0 {
		return true
	}
	return evidence.PodSetServesNothing(pods, input.Now(), input.StuckPodGrace)
}

// planMigrationDrive selects the migrate pass's work in dispatch order:
// every record parked on its source teardown (oldest first, each tended
// in place), then the head — the oldest record that is not parked. One
// head per pass keeps migrations serial; a parked record is excluded
// from that seriality because the drive has nothing left to do for it
// but wait, and a wait on a kubelet that missed its own deadline has no
// bound of its own — held at the head it would starve every later
// request for as long as the pod stays. The pods are read from the
// snapshot's cache view: a stale reading costs a pass either way, since
// the parked record's own drive re-lists live pods and the head is
// re-selected on the next reconcile. Nil when there is no work.
func planMigrationDrive(ctx context.Context, input types.ReconcileInput, snapshot *ObservedSnapshot) (*MigrationSelection, error) {
	var selection MigrationSelection
	var pods map[int32][]*corev1.Pod
	for _, record := range types.ManualMigrationsOldestFirst(input.ObservedState.Migrations) {
		if migrationInSourceTeardown(record) {
			if pods == nil {
				var err error
				if pods, err = snapshot.CachedPods(ctx); err != nil {
					return nil, fmt.Errorf("workload.Plan: observe pods for migration dispatch: %w", err)
				}
			}
			if evidence.AllPodsTerminatingOverdue(pods[record.SourceInstance], input.Now()) {
				selection.Parked = append(selection.Parked, record)
				continue
			}
		}
		// A fresh migration adds one surge Instance. Let an existing update
		// surge finish first so the two operations do not stack extra capacity.
		// Allocated migrations always resume from their durable record.
		if record.SurgeAllocated() || escalation.CurrentSurgeInFlight(input.ObservedState.InstanceStatuses) == 0 {
			head := record
			selection.Head = &head
		}
		break
	}
	if len(selection.Parked) == 0 && selection.Head == nil {
		return nil, nil
	}
	return &selection, nil
}

// migrationInSourceTeardown reports whether a record is in its source
// teardown — Draining with its surge allocated — the one phase the
// drive's delete loop runs in, so the only one in which every source
// pod can be a pod the drive already deleted.
func migrationInSourceTeardown(record types.MigrationRecord) bool {
	return record.Phase == types.MigrationPhaseDraining && record.SurgeAllocated()
}
