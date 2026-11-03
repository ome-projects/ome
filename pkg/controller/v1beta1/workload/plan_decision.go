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
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
)

// ActionKind names one pass-level action of the reconcile pipeline.
type ActionKind string

const (
	// ActionScaleDown deletes the InstanceStatus indices the plan does
	// not cover.
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

	// pods is this Instance's bucket from the authoritative, Component-wide
	// decision snapshot. The executor treats the slice and Pods as immutable
	// and reuses them for every restart phase in this pass.
	pods []*corev1.Pod

	// OpensRepair marks a selection that opens a fresh crash-loop repair
	// this pass rather than driving one already in flight or recovering
	// capacity already lost. The executor opens at most
	// input.RepairBatchSize of them per pass.
	OpensRepair bool

	// OpensUnavailability marks the fresh repairs that take serving
	// capacity offline: a crash-loop repair of a pod set that still
	// serves. The executor puts exactly those to the per-Component
	// unavailability budget before they open, so a crash loop shared by
	// the whole Component repairs at the operator's pace instead of
	// rebuilding every serving Instance at once.
	OpensUnavailability bool

	// RetryScheduleRevision names the revision whose retry schedule admits
	// the rebuild this selection opens, "" for an open no schedule paces.
	// A retry schedule authorizes one attempt at a revision at a time, so
	// the executor opens one such rebuild per revision per pass.
	RetryScheduleRevision string
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
	// AdoptRevision: write Ready on the target (the RunningRevision
	// backfill) instead of running the Update op — the Instance's
	// runtime-ready pods already match the target.
	AdoptRevision bool
	// StartingFresh: the Instance starts a new update rather than
	// continuing one (!types.UpdateContinuation, or a Failed surge claim
	// the strategy replaces drain-first), so it is admitted and charged.
	StartingFresh bool
	// CoordGateExempt: the fresh start skips the coordination gate consult
	// because it takes nothing further out of the gate's serving count
	// (types.RecreateOfUnservingRow, a Ready row whose pod set is provably
	// out of service — evidence.PodSetServesNothing — or one the count
	// already excludes on a member held in a waiting state or missing —
	// evidence.PodSetUnavailableOnParkedMember, evidence.PodSetShortOfMember —
	// or with no pod left that could serve — PodSetLost); the plan gate
	// and the per-Component budget still apply.
	CoordGateExempt bool
	// ReplacesUnservingPodSet: the fresh start replaces an Instance that
	// serves nothing by the graced reading the budgets and the gate share
	// (replacesUnservingPodSet).
	ReplacesUnservingPodSet bool
	// OutOfRotation: the fresh start's Instance serves nothing at this
	// pass — an unserving one (ReplacesUnservingPodSet), a pod set with
	// no routed pod in rotation, its grace run or not
	// (evidence.PodSetOutOfRotation), or one with no pod left that could
	// serve (PodSetLost).
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
	// the Instance carries leftover pods of a replaced revision
	// (ops.EvaluateWreckage). Execute routes the item into
	// ops.CleanupWreckage, which moves the row toward the current desired
	// state, instead of the Update op. Never budget-charged and never
	// gated: cleanup only removes exited pods and frees capacity, so
	// gating it would hold back the recovery the cleanup exists to free.
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
	// surge budget — Terminating inside their grace, or running beyond an
	// Instance's own count — computed for a capped SurgeThenDrain budget
	// (escalation.ExtraPodSurgeInFlight).
	ExtraPodSurge escalation.ExtraPodSurge
	// RolledNotServing lists, in plan order, the Instances already on the
	// target revision that do not serve it (ops.RolledInstanceNotServing).
	// They are the roll's open work: each holds a slot in the budget the
	// strategy uses, ahead of any fresh start, and under
	// SurgeThenDrain no source leaves rotation while they exceed the
	// unavailability budget.
	RolledNotServing []int32
	// NotServingOutsideRoll lists, in plan order, the planned Instances
	// that serve nothing and that the roll has neither moved onto the
	// target nor is moving (ops.InstanceNotServingOutsideRoll): an Instance
	// a scale-up is still starting, one under repair, one that broke or
	// crash-loops on its own revision, a canary step's held Instance
	// included. Each is one Instance down already, so under a drain-first
	// strategy the unavailability budget counts them ahead of a fresh start
	// on an Instance that still serves; a start on one of them takes over
	// the slot its own outage holds and is charged as any start.
	NotServingOutsideRoll []int32
	// RetryBlockDenied lists, in plan order, the Instances the target's
	// RetryBlock denied a fresh start at the trigger stage: a block in
	// the Held state, an attempt already in flight, or a Backoff not yet due.
	// They never reach the pass's admission, so the block is the denial
	// the pass reports when nothing else progressed.
	RetryBlockDenied []int32
	// RetryScheduleHold is the hold the target's RetryBlock stands for
	// when it denies a fresh start this pass, nil when it denies none. One
	// reading serves the update pass, which reports it for the starts in
	// RetryBlockDenied, and the idle reading, which yields to it: a retry
	// schedule that gave up on the revision is reported even when no
	// candidate reaches the trigger.
	RetryScheduleHold *types.RolloutHold
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
	// not a start reaches the admission this pass: the retry schedule's
	// when the target's block denies a fresh start or counts their crash,
	// else the budget's when those Instances and the starts in flight
	// already exhaust the budget the strategy uses. The budget's stands
	// only while one of them crashed after promotion and is out of rotation
	// (CrashedOutOfRotation): a rolled Instance short of serving for
	// another reason is repaired or stopped by a pass that says so itself,
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
	// retry schedule's attempt. Execute reports a nil rollout hold at the
	// update pass's position, so a hold a prior pass recorded does not
	// outlive the roll it paced or the loss it named, while a retry
	// schedule that gave up on the revision stays reported.
	UpdateIdle bool

	// RetryScheduleHold is the hold an unpaused roll waits on when the
	// target's RetryBlock denies every start at the trigger stage and
	// nothing else is selected (UpdateSelection.RetryScheduleHold with no
	// items). Execute reports it at the update pass's position, so the
	// wait says what the next attempt rebuilds on every pass that reaches
	// it.
	RetryScheduleHold *types.RolloutHold

	// StandingHold is the hold an unpaused roll with nothing selected
	// stands behind on account of the Instances already on the target that
	// are out of rotation because a pushed pod crashed after promotion
	// (UpdateSelection.StandingHold with no items). Execute reports it at
	// the update pass's position after the retry schedule's hold, and a
	// pass the restart pass consumes reports it there in the update pass's
	// place.
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
	// reporting what they see (the adapter holds the deadlines while
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

	// The rows are indexed once ahead of every selection, so a caller that
	// enters here directly reads them as cheaply as one that came through
	// Reconcile.
	input.ObservedState.IndexRows()
	// One owner grouping for the whole pass, ahead of every selection:
	// the triggers below and the verb passes Execute dispatches all read
	// their own rows out of it.
	decision.Owned = types.RowsByOwner(input.ObservedState.InstanceStatuses)
	input.Owned = &decision.Owned
	// One held set for the selections below, for a caller that enters
	// here directly; the one Reconcile allocates reaches Execute as well.
	if input.Held == nil {
		input.Held = &types.HeldInstances{}
	}

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
	// (a pause withholds the Create pass that would otherwise
	// re-materialize it, and the restart pass has not selected the row —
	// a freeze suspends it), must not keep claiming Ready. Status-only;
	// selected here, applied by Execute; runs in every pause depth because
	// pause suspends lifecycle operations, never status truth. Unpaused
	// reconciles skip it: the status publication demotes the same shape
	// (status.DemotableReady) at the end of every pass, and the pass that
	// owns the rebuild re-materializes the row when it runs. The paused
	// pass keeps its own correction because it confirms the loss against
	// a live read and records a warning event for it.
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
	// the next step, so a pause never leaves an Instance mid-step —
	// unserving between a delete and its recreate, or half-materialized —
	// for the length of the hold. PauseFreeze deepens the hold onto the restart
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
		// BEFORE the drive pass so an expired record is consumed before
		// the drive writes it again, and regardless of MigrationMode so a
		// mode flip to Never can never strand a non-terminal record.
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
			// pause is the roll's hold; the holds its Instances stand behind
			// are reported once the roll may move again.
			selection.Items = pausedUpdateContinuations(input, selection.Items)
			selection.StandingHold = nil
		} else {
			decision.RequeueAfter = retryBlockWait
			// A roll with nothing left to do holds nothing: no Instance to
			// drive, no retry schedule that still holds the target, whether
			// a candidate reached its gate or a partition kept every one
			// back, and no Instance on the target out of rotation after a
			// crash, or proving itself under the retry schedule's attempt,
			// that holds the roll.
			decision.UpdateIdle = len(selection.Items) == 0 && len(selection.RetryBlockDenied) == 0 && selection.RetryScheduleHold == nil && selection.StandingHold == nil
			// A roll with nothing to run but a hold to report waits on it:
			// the retry schedule's when the target's block denies every
			// start, else the one its Instances crashed after promotion
			// stand behind. Either is reported at the update position.
			if len(selection.Items) == 0 {
				decision.RetryScheduleHold = selection.RetryScheduleHold
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
// already on. A fresh start, a revision backfill, and a leftover-pod
// cleanup are new work and wait for the unpause.
//
// A gang SurgeThenDrain cycle is excluded whole. It spans two rows — the
// source and the replacement gang's target row it claims — and keeps the
// source serving from its first pass to its last, so holding it costs the
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
// drives it on rather than leaving a half-done roll standing.
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
// cycle: the source that claims a replacement index, or the replacement's
// target row itself.
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
// open one deleted must still be recreated at the next Incarnation
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
// The cached view only screens for a candidate. One Component-wide live view
// then confirms every selection and travels with the Decision into execution,
// so a restart pass performs at most one authoritative Pod List regardless of
// how many Instances it advances. Mutations after that observation remain
// fenced by Pod UID and identity checks, and a create that races a same-name
// occupant converges through AlreadyExists. A healthy Component costs no live
// List under any policy. Under RecreateInstance, a failed cache read falls back
// to the live view because that policy can select entirely from the
// authoritative observation.
func planRestartSelections(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) ([]RestartSelection, error) {
	cachedByInstance, err := snapshot.CachedPods(ctx)
	if err != nil && plan.RestartPolicy != types.RestartPolicyRecreateInstance {
		return nil, fmt.Errorf("workload.Reconcile: list pods to screen the restart pass (component=%s): %w", plan.Component, err)
	}
	if err == nil && !anyRestartTrigger(input, plan, target, cachedByInstance) {
		return nil, nil
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
			Instance:              inst,
			Reason:                reason,
			pods:                  liveByInstance[inst.Index],
			OpensRepair:           workloadops.RestartOpensRepair(input, plan, inst, liveByInstance[inst.Index]),
			OpensUnavailability:   workloadops.RestartOpensUnavailability(input, plan, inst, liveByInstance[inst.Index]),
			RetryScheduleRevision: workloadops.RestartOpensLadderAttempt(input, plan, inst, liveByInstance[inst.Index]),
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
// deferring the correction until the loss is total and settled, while a
// pod in a terminal phase backs nothing (status.PodSetBacksRow): it never
// leaves on its own, and the reason names it so the pass that rebuilds
// the row is known to have it to delete.
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
		if !status.PodSetBacksRow(cached[idx]) {
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
		if !status.PodSetBacksRow(live[idx]) {
			out = append(out, types.DemotionSelection{Index: idx, Reason: unbackedRowReason(live[idx])})
		}
	}
	return out, nil
}

// unbackedRowReason says why a Ready row is written Pending: no live pod
// backs it, and when pods in a terminal phase still hold its names, which
// ones and with what reason the kubelet left them.
func unbackedRowReason(pods []*corev1.Pod) string {
	reason := "no live pods back the Ready phase"
	var terminal []string
	for _, pod := range pods {
		if pod == nil || !query.IsTerminalPod(pod) {
			continue
		}
		desc := pod.Name + " (" + string(pod.Status.Phase)
		if pod.Status.Reason != "" {
			desc += "/" + pod.Status.Reason
		}
		terminal = append(terminal, desc+")")
	}
	if len(terminal) == 0 {
		return reason
	}
	noun := "terminal pod"
	if len(terminal) > 1 {
		noun = "terminal pods"
	}
	return reason + "; " + noun + " " + strings.Join(terminal, ", ") + " still hold the Instance's pod names"
}

// planUpdateSelection evaluates the per-Instance update triggers
// (pure) over the snapshot's cached pods and returns the selection
// plus the earliest not-yet-due RetryBlock wake-up across denied
// Instances.
func planUpdateSelection(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) (UpdateSelection, time.Duration, error) {
	p, err := readUpdateSelection(ctx, input, plan, target, snapshot)
	if err != nil {
		return UpdateSelection{}, 0, err
	}
	if err := p.run(ctx); err != nil {
		return UpdateSelection{}, 0, err
	}
	return p.selection, p.retryBlockWait, nil
}

// updateSelectionPass is one call of planUpdateSelection as it reads its
// input: the strategy with its budgets and the starts already in flight,
// the cached pods of every Instance, the Instances the partition holds,
// the window a restarted pod must serve again for, and the selection it
// builds.
type updateSelectionPass struct {
	input    types.ReconcileInput
	plan     types.ComponentPlan
	target   *appsv1.ControllerRevision
	snapshot *ObservedSnapshot
	log      logr.Logger
	// strategy is the resolved update strategy; an empty type reads as
	// SurgeThenDrain, as BuildPlan defaults it.
	strategy types.UpdateStrategyType
	// pods is the cached pod set of every Instance by index, the
	// snapshot's non-destructive read source; an absent bucket is an
	// empty pod set.
	pods map[int32][]*corev1.Pod
	// held is the set of Instances the effective partition keeps on their
	// current revision: the projected pacing partition (a canary step or
	// a plan-gate hold) when one is set, else the user's rollingUpdate
	// partition (workloadops.EffectivePartition); the pass's one reading
	// of it (workloadops.RollHeldIndices).
	held map[int32]bool
	// provenWindow is how long, in seconds, a promoted pod that restarted
	// must serve again before the roll reads it as serving:
	// minReadySeconds, or the stuck-pod grace when that is longer.
	provenWindow int32

	selection UpdateSelection
	// retryBlockWait is the earliest not-yet-due Backoff requeue among the
	// Instances the target's RetryBlock denied; zero when none is.
	retryBlockWait time.Duration
}

// readUpdateSelection reads the input once: the strategy, the two
// per-Component budgets with the starts already in flight, the cached pod
// buckets, the Instances the partition holds and the window a restarted
// pod must serve again for.
func readUpdateSelection(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) (*updateSelectionPass, error) {
	rollingUpdate := plan.UpdateStrategy.RollingUpdate
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
	pods, err := snapshot.CachedPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("workload.Reconcile: list pods for update pass (component=%s): %w", plan.Component, err)
	}
	return &updateSelectionPass{
		input:        input,
		plan:         plan,
		target:       target,
		snapshot:     snapshot,
		log:          logf.FromContext(ctx).V(1),
		strategy:     strategy,
		pods:         pods,
		held:         workloadops.RollHeldIndices(input, plan, target),
		provenWindow: workloadops.ProvenWindowSeconds(plan.MinReadySeconds, input.StuckPodGrace),
		selection:    selection,
	}, nil
}

// run builds the selection, in this order:
//
//  1. Every planned Instance, in plan order: the partition, the rolled
//     set, the trigger and the start it selects (selectInstance).
//  2. The order of the starts: Instances out of rotation first, then
//     plan order (orderStarts).
//  3. The surge slots the Component's extra pods hold, under a capped
//     surge budget (countExtraPodSurge).
//  4. The two holds a roll with nothing admitted stands behind: the
//     retry schedule's and the standing hold (readHolds).
func (p *updateSelectionPass) run(ctx context.Context) error {
	for _, inst := range p.plan.Instances {
		if err := p.selectInstance(ctx, p.candidate(inst)); err != nil {
			return err
		}
	}
	p.orderStarts()
	p.countExtraPodSurge()
	// Budgets are decided here but spent in Execute, so they are logged
	// with the selection they apply to: a selection that is non-empty yet
	// starts nothing is a budget or gate outcome, not a selection one.
	p.log.Info("update selection complete",
		"component", p.plan.Component, "target", p.target.Name,
		"considered", len(p.plan.Instances), "selected", len(p.selection.Items),
		"strategy", p.selection.Strategy, "surgeBudget", p.selection.SurgeBudget,
		"unavailBudget", p.selection.UnavailBudget,
		"priorSurgeInFlight", p.selection.PriorSurgeInFlight,
		"priorUnavailInFlight", p.selection.PriorUnavailInFlight,
		"extraPodSurge", p.selection.ExtraPodSurge.Slots,
		"terminatingPods", p.selection.ExtraPodSurge.Terminating,
		"liveExtraPods", p.selection.ExtraPodSurge.Live,
		"rolledNotServing", p.selection.RolledNotServing,
		"notServingOutsideRoll", p.selection.NotServingOutsideRoll,
		"crashedNotServing", p.selection.CrashedNotServing,
		"crashedOutOfRotation", p.selection.CrashedOutOfRotation,
		"retryBlockDenied", p.selection.RetryBlockDenied)
	p.readHolds()
	return nil
}

// updateCandidate is one planned Instance as the selection reads it: its
// row, nil when no row is observed, and its cached pods.
type updateCandidate struct {
	inst types.InstancePlan
	row  *types.InstanceStatus
	pods []*corev1.Pod
}

func (p *updateSelectionPass) candidate(inst types.InstancePlan) updateCandidate {
	return updateCandidate{inst: inst, row: p.input.ObservedState.Instance(inst.Index), pods: p.pods[inst.Index]}
}

// startsFresh reports an Instance whose start opens a new attempt rather
// than continuing one in flight (types.UpdateContinuation). Only a fresh
// start is admitted and charged. A Failed row whose kept surge claim the
// strategy no longer continues as a surge is one: the drain-first start
// that replaces the claim takes a serving set offline
// (workloadops.FailedSurgeStartsDrainFirst).
func (p *updateSelectionPass) startsFresh(c updateCandidate) bool {
	return !types.UpdateContinuation(c.row) || workloadops.FailedSurgeStartsDrainFirst(c.row, p.strategy)
}

// heldByPartition reports an Instance the effective partition keeps on
// its current revision. Membership is keyed to the revision, not the
// index: the lowest-indexed Instances still off the target are held, and
// one already converging to the target never is
// (workloadops.RollHeldIndices).
func (p *updateSelectionPass) heldByPartition(c updateCandidate) bool {
	return p.held[c.inst.Index]
}

// rolledPods is the pod count of the set an Instance runs at the target:
// the target's Runner layout at the width the target records.
func (p *updateSelectionPass) rolledPods(c updateCandidate) int32 {
	return types.InstancePlan{Runners: p.plan.RunnersFor(c.inst, p.target.Name)}.TotalPods()
}

// crashedAfterPromotion reports an Instance on the target whose pushed
// pod crashed after the row entered Ready (workloadops.RolledInstanceCrashed).
func (p *updateSelectionPass) crashedAfterPromotion(c updateCandidate) bool {
	return workloadops.RolledInstanceCrashed(c.row, p.target.Name, p.target.CreationTimestamp.Time, c.pods, p.provenWindow)
}

// rolledSetInRotation reports a pod set on the target whose every pod is
// live, ContainersReady and serving (workloadops.RolledSetInRotation).
func (p *updateSelectionPass) rolledSetInRotation(c updateCandidate, rolledPods int32) bool {
	return workloadops.RolledSetInRotation(p.target.Name, c.pods, rolledPods)
}

// selectInstance decides what the roll does with one planned Instance,
// the first of these that applies:
//
//  1. The partition holds the Instance: nothing is listed.
//  2. The pods already run the target: an AdoptRevision item.
//  3. The trigger declines: the retry wait is kept, and the pods of a
//     replaced revision are listed for cleanup (declineStart).
//  4. The Instance starts: fresh, with its mechanism and the flags the
//     admission reads, or as the continuation of an attempt in flight
//     (listStart).
//
// Before the partition is read, an Instance down outside the roll is
// recorded as holding a slot of the unavailability budget
// (recordDownOutsideRoll), a held one included; before the trigger is
// read, an Instance already on the target that does not serve it is
// recorded as the roll's open work (recordRolledSet), so every Instance
// but a held one is read for it.
func (p *updateSelectionPass) selectInstance(ctx context.Context, c updateCandidate) error {
	p.recordDownOutsideRoll(c)
	if p.heldByPartition(c) {
		p.log.Info("update not selected: held by partition",
			"component", p.plan.Component, "instance", c.inst.Index, "target", p.target.Name)
		return nil
	}
	p.recordRolledSet(c)
	trigger := workloadops.EvaluateUpdateTrigger(p.input, c.inst, p.target, c.pods)
	switch {
	case trigger.AdoptRevision:
		p.adoptRevision(c)
	case !trigger.Trigger:
		p.declineStart(c, trigger)
	default:
		return p.listStart(ctx, c)
	}
	return nil
}

// recordRolledSet records an Instance already on the target revision that
// does not serve it (workloadops.RolledInstanceNotServing). One short of
// serving by unserving pods alone holds no slot: nothing those pods do
// clears the shortfall (workloadops.RolledInstanceDark). Every other one
// is the roll's open work: it holds a budget slot (RolledNotServing); when
// its pushed pod crashed after promotion it is CrashedNotServing, and when
// that set is short of rotation CrashedOutOfRotation. A pod Ready again
// inside its window and a pod unready inside its grace raise no watch
// event when the window or the grace elapses, so the pass requeues for them.
func (p *updateSelectionPass) recordRolledSet(c updateCandidate) {
	rolledPods := p.rolledPods(c)
	notServing, wait, why := workloadops.RolledInstanceNotServing(c.row, p.target.Name, p.target.CreationTimestamp.Time, c.pods, rolledPods, p.provenWindow, p.input.Now())
	if !notServing {
		return
	}
	unserving, graceLeft := workloadops.RolledInstanceDark(c.row, p.target.Name, p.target.CreationTimestamp.Time, c.pods, rolledPods, p.provenWindow, p.input.StuckPodGrace, p.input.Now())
	if unserving {
		p.log.Info("rolled Instance serves nothing: not the roll's open work",
			"component", p.plan.Component, "instance", c.inst.Index, "target", p.target.Name, "reason", why)
		return
	}
	p.selection.RolledNotServing = append(p.selection.RolledNotServing, c.inst.Index)
	if p.crashedAfterPromotion(c) {
		p.selection.CrashedNotServing = append(p.selection.CrashedNotServing, c.inst.Index)
		if !p.rolledSetInRotation(c, rolledPods) {
			p.selection.CrashedOutOfRotation = append(p.selection.CrashedOutOfRotation, c.inst.Index)
		}
	}
	p.log.Info("rolled Instance not serving",
		"component", p.plan.Component, "instance", c.inst.Index, "target", p.target.Name, "reason", why, "wait", wait, "graceLeft", graceLeft)
	if wait > 0 {
		p.input.PassWake.Observe(wait)
	}
	if graceLeft > 0 {
		p.input.PassWake.Observe(graceLeft)
	}
}

// recordDownOutsideRoll records an Instance that serves nothing and that
// the roll has neither moved onto the target nor is moving
// (workloadops.InstanceNotServingOutsideRoll): it holds a slot of the
// unavailability budget whether or not the roll may take it. A pod Ready
// again inside its window raises no watch event when the window elapses,
// so the pass requeues for it.
func (p *updateSelectionPass) recordDownOutsideRoll(c updateCandidate) {
	notServing, wait, why := workloadops.InstanceNotServingOutsideRoll(c.row, p.target.Name, c.pods, p.provenWindow, p.input.Now())
	if !notServing {
		return
	}
	p.selection.NotServingOutsideRoll = append(p.selection.NotServingOutsideRoll, c.inst.Index)
	p.log.Info("Instance not serving outside the roll",
		"component", p.plan.Component, "instance", c.inst.Index, "target", p.target.Name, "reason", why, "wait", wait)
	if wait > 0 {
		p.input.PassWake.Observe(wait)
	}
}

// adoptRevision lists an Instance whose runtime-ready pods already run the
// target: Execute writes Ready on the target instead of running an update.
func (p *updateSelectionPass) adoptRevision(c updateCandidate) {
	p.log.Info("update not selected: adopting revision in place",
		"component", p.plan.Component, "instance", c.inst.Index, "target", p.target.Name)
	p.selection.Items = append(p.selection.Items, UpdateItem{Instance: c.inst, AdoptRevision: true})
}

// declineStart records an Instance the trigger declined. A denial by the
// target's RetryBlock is listed (RetryBlockDenied) and the earliest
// not-yet-due Backoff is kept as the pass's requeue. The trigger reads the
// revision distance, so an Instance at zero distance (a corrective
// roll-back) or on a third-party revision never dispatches; the pods of a
// replaced revision it still carries are listed for cleanup
// (workloadops.EvaluateWreckage), the one effect Execute runs on such an
// Instance (ops.CleanupWreckage).
func (p *updateSelectionPass) declineStart(c updateCandidate, trigger workloadops.UpdateTriggerDecision) {
	if trigger.RetryBlockDenied {
		p.selection.RetryBlockDenied = append(p.selection.RetryBlockDenied, c.inst.Index)
	}
	p.keepEarliestRetryWait(trigger.RetryAfter)
	cleanup := workloadops.EvaluateWreckage(c.row, p.target, c.pods)
	if cleanup {
		p.selection.Items = append(p.selection.Items, UpdateItem{Instance: c.inst, CleanupOnly: true})
	}
	// The trigger declining is the most common reason a roll makes no
	// progress, and the phase and revision it read are not recoverable
	// after the fact.
	if c.row == nil {
		p.log.Info("update not triggered: no observed status",
			"component", p.plan.Component, "instance", c.inst.Index,
			"target", p.target.Name, "cleanupOnly", cleanup)
		return
	}
	p.log.Info("update not triggered",
		"component", p.plan.Component, "instance", c.inst.Index,
		"phase", c.row.Phase, "runningRevision", c.row.RunningRevision,
		"target", p.target.Name, "hasOperation", c.row.Operation != nil,
		"retryAfter", trigger.RetryAfter, "cleanupOnly", cleanup)
}

// keepEarliestRetryWait keeps the earliest not-yet-due Backoff requeue
// across the Instances the target's RetryBlock denied.
func (p *updateSelectionPass) keepEarliestRetryWait(retryAfter time.Duration) {
	if retryAfter > 0 && (p.retryBlockWait == 0 || retryAfter < p.retryBlockWait) {
		p.retryBlockWait = retryAfter
	}
}

// listStart lists an Instance the trigger selected. A fresh start carries
// the mechanism it runs on and the flags the admission reads: whether it
// skips the coordination gate, whether it replaces an unserving pod set,
// and whether its Instance is out of rotation, which orders it first. A
// continuation is listed as it is: it is neither admitted nor charged
// again.
func (p *updateSelectionPass) listStart(ctx context.Context, c updateCandidate) error {
	item := UpdateItem{Instance: c.inst, StartingFresh: p.startsFresh(c)}
	if item.StartingFresh {
		recreate, err := p.recreateFallback(ctx, c)
		if err != nil {
			return err
		}
		item.RecreateFallback = recreate
		item.ReplacesUnservingPodSet = replacesUnservingPodSet(p.input, c.row, c.pods)
		item.OutOfRotation = p.outOfRotation(c, item.ReplacesUnservingPodSet)
	}
	item.CoordGateExempt = p.skipsCoordinationGate(c, item.mechanism(p.strategy), item.StartingFresh)
	p.log.Info("update selected",
		"component", p.plan.Component, "instance", c.inst.Index, "target", p.target.Name,
		"startingFresh", item.StartingFresh, "coordGateExempt", item.CoordGateExempt, "recreateFallback", item.RecreateFallback,
		"replacesDarkPodSet", item.ReplacesUnservingPodSet, "outOfRotation", item.OutOfRotation)
	p.selection.Items = append(p.selection.Items, item)
	return nil
}

// mechanism is the strategy a start is consulted as: RecreatePod when the
// Component's in-place strategy runs as a recreate on this Instance, else
// the Component's own.
func (item UpdateItem) mechanism(strategy types.UpdateStrategyType) types.UpdateStrategyType {
	if item.RecreateFallback {
		return types.UpdateStrategyRecreatePod
	}
	return strategy
}

// recreateFallback reports whether a fresh start runs as a recreate
// although the strategy would patch in place: the Instance is a gang, the
// diff against the running revision goes beyond container images
// (workloadops.InPlaceFallsBackToRecreate), the row has no serving source
// a surge could keep (workloadops.SurgeHasNoSource), or the step would
// replace no container on a pod set that failed its readiness
// (inPlaceCannotRestore). The running revision's PodSpec is a snapshot
// read, memoized per revision.
func (p *updateSelectionPass) recreateFallback(ctx context.Context, c updateCandidate) (bool, error) {
	fallback, err := workloadops.InPlaceFallsBackToRecreate(p.strategy, c.inst.TotalPods() > 1, p.input.DesiredSpec.PodSpec,
		func() (*corev1.PodSpec, error) { return p.snapshot.RunningRevisionPodSpec(ctx, c.inst.Index) })
	if err != nil {
		return false, p.mechanismError(c, err)
	}
	if fallback || workloadops.SurgeHasNoSource(c.row) {
		return true, nil
	}
	if p.strategy != types.UpdateStrategyInPlaceIfPossible {
		return false, nil
	}
	return p.inPlaceCannotRestore(ctx, c)
}

// inPlaceCannotRestore reports whether an in-place step would replace no
// container on a pod set that failed its readiness
// (workloadops.InPlaceCannotRestore). Such a patch could only wait out its
// deadline, so the start runs as a recreate and is consulted as the
// drain-first start it is.
func (p *updateSelectionPass) inPlaceCannotRestore(ctx context.Context, c updateCandidate) (bool, error) {
	running, err := p.snapshot.RunningRevisionPodSpec(ctx, c.inst.Index)
	if err != nil {
		return false, p.mechanismError(c, err)
	}
	return workloadops.InPlaceCannotRestore(running, p.input.DesiredSpec.PodSpec, c.pods, p.input.Now(), p.input.StuckPodGrace), nil
}

func (p *updateSelectionPass) mechanismError(c updateCandidate, err error) error {
	return fmt.Errorf("workload.Reconcile: resolve update mechanism (instance=%d): %w", c.inst.Index, err)
}

// skipsCoordinationGate reports a fresh drain-first start on a row the
// gate's serving count already leaves out: a Failed row with no serving
// pod (types.RecreateOfUnservingRow), or a Ready row outside the count
// (readyRowOutsideServingCount). The consult would count the Instance's
// own outage a second time, so the start skips it; the plan gate and the
// per-Component budget still apply, and a surge start keeps the consult
// because its replacement adds a pod.
func (p *updateSelectionPass) skipsCoordinationGate(c updateCandidate, mechanism types.UpdateStrategyType, fresh bool) bool {
	return types.RecreateOfUnservingRow(c.row, mechanism) ||
		(fresh && mechanism != types.UpdateStrategySurgeThenDrain && p.readyRowOutsideServingCount(c))
}

// readyRowOutsideServingCount reports a Ready row whose pod set the
// coordination gate's serving count already leaves out. The count needs
// every pod of an Instance in rotation, so an Instance whose set provably
// serves nothing, whose member is held in a waiting state or unready past
// the grace, whose member is missing, or with no pod left that could
// serve is already inside the gate's unavailability. A row that lost its
// pods while off an open roll's target is the roll's to rebuild, since
// the repair yields such a row to the roll, so the roll's own recreate of
// it must not pay for the outage the row already is.
func (p *updateSelectionPass) readyRowOutsideServingCount(c updateCandidate) bool {
	if c.row == nil || c.row.Phase != types.InstancePhaseReady {
		return false
	}
	return evidence.PodSetServesNothing(c.pods, p.input.Now(), p.input.StuckPodGrace) ||
		evidence.PodSetLost(c.pods) ||
		evidence.PodSetUnavailableOnParkedMember(c.pods, c.inst.TotalPods(), p.input.Now(), p.input.StuckPodGrace) ||
		evidence.PodSetShortOfMember(c.pods, c.inst.TotalPods())
}

// outOfRotation reports a fresh start whose Instance serves nothing at
// this pass: it replaces an unserving pod set, no routed pod of the set
// is in rotation, whatever the reason and for however long
// (evidence.PodSetOutOfRotation), or no pod is left that could serve
// (PodSetLost). Such a start restores capacity, so it is listed ahead of
// the rest.
func (p *updateSelectionPass) outOfRotation(c updateCandidate, replacesUnserving bool) bool {
	return replacesUnserving ||
		evidence.PodSetLost(c.pods) ||
		evidence.PodSetOutOfRotation(c.pods)
}

// orderStarts puts the starts on Instances out of rotation first and
// keeps plan order inside each group. Such a start restores capacity
// while every other start spends it, so the budget is offered to those
// first, from the moment the Instance left rotation rather than once
// its grace has run. The order is all this decides: the budgets and the
// gate admit or deny each start on their own terms, and the partition
// has already chosen who is listed.
func (p *updateSelectionPass) orderStarts() {
	sort.SliceStable(p.selection.Items, func(i, j int) bool {
		return p.selection.Items[i].OutOfRotation && !p.selection.Items[j].OutOfRotation
	})
}

// countExtraPodSurge counts, under a capped surge budget, the surge
// slots the Component's extra pods hold: a Terminating pod until it exits
// or passes its deletion deadline, a live one until a start on its
// Instance replaces it. The deadline passing raises no watch event, so a
// pass with a fresh start to admit requeues for the next release.
func (p *updateSelectionPass) countExtraPodSurge() {
	if p.selection.Strategy != types.UpdateStrategySurgeThenDrain || p.selection.SurgeBudget == escalation.BudgetNoLimit {
		return
	}
	now := p.input.Now()
	p.selection.ExtraPodSurge = escalation.ExtraPodSurgeInFlight(p.plan, p.input.ObservedState.InstanceStatuses, p.pods, p.selection.RolledNotServing, now)
	if !p.selection.ExtraPodSurge.NextRelease.IsZero() && anyFreshStart(p.selection.Items) {
		p.input.PassWake.Observe(p.selection.ExtraPodSurge.NextRelease.Sub(now))
	}
}

// readHolds reads the two holds a roll with nothing admitted stands
// behind: the retry schedule's, when the target's RetryBlock denies a
// fresh start or counts the crash of an Instance already on the target,
// and the standing hold of the Instances on the target that do not
// serve it.
func (p *updateSelectionPass) readHolds() {
	p.selection.RetryScheduleHold = retryScheduleHold(p.input, p.target, p.selection.RolledNotServing, p.selection.CrashedNotServing, retryRebuilds(p.input, p.plan, p.target, p.pods))
	p.selection.StandingHold = standingHold(&p.selection, p.target)
}

// retryRebuilds lists, in plan order, the Instances whose crashed set the
// target's retry schedule rebuilds in its next attempt
// (workloadops.LadderAttemptRebuilds), for the retry schedule hold to
// name.
func retryRebuilds(input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, byInstance map[int32][]*corev1.Pod) []int32 {
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

// downOutsideRoll reports whether the Instance at idx is one the selection
// lists as not serving outside the roll.
func (s *UpdateSelection) downOutsideRoll(idx int32) bool {
	for _, down := range s.NotServingOutsideRoll {
		if down == idx {
			return true
		}
	}
	return false
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

// replacesUnservingPodSet reports whether a fresh start on row replaces
// an Instance that serves nothing: a Failed row with no serving pod (the
// counter types.RecreateOfUnservingRow reads), or a pod set provably out
// of service (evidence.PodSetServesNothing, the reading that makes a Ready
// row unserving). Replacing such an Instance takes no capacity out of
// rotation.
func replacesUnservingPodSet(input types.ReconcileInput, row *types.InstanceStatus, pods []*corev1.Pod) bool {
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
