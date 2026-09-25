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
	"time"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
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
	// ActionMigrate drives the selected Manual migration record.
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

	// OpensUnavailability marks a selection that takes serving capacity
	// offline rather than recovering capacity already lost: a fresh
	// crash-loop repair. The executor puts exactly those to the
	// unavailability budget and the coordination gate before they open,
	// so a wedge shared by the whole Component repairs at the
	// operator's pace instead of recycling every Instance at once.
	OpensUnavailability bool
}

// MigrationSelection is the Manual migration record the migrate pass
// drives this reconcile (a copy of the ObservedState record).
type MigrationSelection struct {
	Record types.MigrationRecord
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
	// (types.RecreateOfDarkFailedRow); the per-Component budget still applies.
	CoordGateExempt bool
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
	// Items in plan order. Instances held by the effective partition
	// (canary) and Instances with no trigger are not listed.
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

	// Migration is the record the migrate pass drives (ActionMigrate).
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
	// to Create.
	Actions []PlannedAction

	// RequeueAfter is the earliest not-yet-due Backoff RetryBlock
	// wake-up reported by the update-trigger evaluation — the one
	// requeue the decision layer owns without running an op. The
	// executor folds it additively into the pass result
	// (foldRetryAfter) so the gate is re-evaluated on time; it only
	// ever ADDS a wake-up, never delays one an op scheduled.
	RequeueAfter time.Duration

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

	// Truth pass, paused reconciles only: a Ready Instance whose pods are
	// all gone, with no operation in flight and no op pass that will act
	// (the policy is not RecreateInstanceOnPodRestart, and pause parks the
	// Create pass that would otherwise re-materialize it), must not keep
	// claiming Ready. Status-only; selected here, applied by Execute; runs
	// in every pause depth because pause suspends lifecycle operations,
	// never status truth. Unpaused reconciles skip it: Create both
	// recovers the pods and re-stamps the phase in the same pass.
	if plan.Paused {
		demotions, err := planUnbackedDemotions(ctx, input, plan, snapshot)
		if err != nil {
			return Decision{}, err
		}
		if len(demotions) > 0 {
			decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionDemote, Demotions: demotions})
		}
	}

	// Paused is an operator circuit breaker over WHAT MAY BEGIN:
	// scale-down above still releases capacity, the restart pass below
	// keeps repairing existing Instances at their current revision
	// (RunningRevision — repair can never advance a rollout), and an
	// attempt already in flight still runs the step it is on to that
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

	// Restart selection. A frozen pause suspends the pass, so the
	// selection is only worth computing — it costs a live pod read —
	// when a repair is already under way to finish.
	if !plan.PauseFreeze || anyRestartContinuation(input) {
		restarts, err := planRestartSelections(ctx, input, plan, snapshot)
		if err != nil {
			return Decision{}, err
		}
		if plan.PauseFreeze {
			restarts = restartContinuations(input, restarts)
		}
		if len(restarts) > 0 {
			decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionRestart, Restarts: restarts})
		}
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
		// status.migrations records: the oldest non-terminal Manual record
		// is driven, one per pass. Terminal records and Auto records (born
		// terminal) are excluded structurally — records, never work. Skip
		// when mode is Never.
		if plan.MigrationMode != types.MigrationModeNever {
			if record := types.NextManualMigration(input.ObservedState.Migrations); record != nil {
				// A fresh migration adds one surge Instance. Let an existing update
				// surge finish first so the two operations do not stack extra capacity.
				// Allocated migrations always resume from their durable record.
				if record.SurgeAllocated() || escalation.CurrentSurgeInFlight(input.ObservedState.InstanceStatuses) == 0 {
					decision.Actions = append(decision.Actions, PlannedAction{Kind: ActionMigrate, Migration: &MigrationSelection{Record: *record}})
				}
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
			// withholds; the unpause is the wake-up that matters here.
			selection.Items = pausedUpdateContinuations(input, selection.Items)
		} else {
			decision.RequeueAfter = retryBlockWait
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
// The live read is unconditional only under the policy whose triggers
// fire on ordinary pod churn. The two policy-independent triggers are
// rare, so for every other policy the cached view screens for a
// candidate first and the live read confirms it; a healthy Component
// therefore costs no extra live List per reconcile.
func planRestartSelections(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, snapshot *ObservedSnapshot) ([]RestartSelection, error) {
	if plan.RestartPolicy != types.RestartPolicyRecreateInstance {
		cachedByInstance, err := snapshot.CachedPods(ctx)
		if err != nil {
			return nil, fmt.Errorf("workload.Reconcile: list pods to screen the restart pass (component=%s): %w", plan.Component, err)
		}
		if !anyRestartTrigger(input, plan, cachedByInstance) {
			return nil, nil
		}
	}
	liveByInstance, err := snapshot.LivePods(ctx)
	if err != nil {
		return nil, fmt.Errorf("workload.Reconcile: list pods for restart pass (component=%s): %w", plan.Component, err)
	}
	var restarts []RestartSelection
	for _, inst := range plan.Instances {
		needs, reason := workloadops.DetectRestartTriggerWithPods(input, plan, inst, liveByInstance[inst.Index])
		if !needs {
			continue
		}
		restarts = append(restarts, RestartSelection{
			Instance:            inst,
			Reason:              reason,
			OpensUnavailability: workloadops.RestartOpensUnavailability(input, inst, liveByInstance[inst.Index]),
		})
	}
	return restarts, nil
}

// anyRestartTrigger reports whether any planned Instance would be
// selected against the supplied pod buckets.
func anyRestartTrigger(input types.ReconcileInput, plan types.ComponentPlan, byInstance map[int32][]*corev1.Pod) bool {
	for _, inst := range plan.Instances {
		if needs, _ := workloadops.DetectRestartTriggerWithPods(input, plan, inst, byInstance[inst.Index]); needs {
			return true
		}
	}
	return false
}

// planUnbackedDemotions selects Ready Instances with no live pods and no
// in-flight Operation for a status-only demotion to Pending. Phase is
// op-owned, so this narrow observation-only correction fires exclusively
// where no op pass will: components under RecreateInstanceOnPodRestart are
// excluded outright (their restart pass owns Ready-with-pod-loss and
// recreates at the running revision), and an Operation in any state keeps
// ownership with its op. Extras belong to scale-down. Candidates are
// screened on the cached read and confirmed against the live read, so the
// demotion can never fire on cache lag; a Terminating pod still counts as
// live, deferring the correction until the loss is total and settled.
func planUnbackedDemotions(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, snapshot *ObservedSnapshot) ([]types.DemotionSelection, error) {
	if plan.RestartPolicy == types.RestartPolicyRecreateInstance {
		return nil, nil
	}
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var candidates []int32
	for i := range input.ObservedState.InstanceStatuses {
		row := &input.ObservedState.InstanceStatuses[i]
		if _, ok := planned[row.Index]; !ok {
			continue
		}
		if row.Phase != types.InstancePhaseReady || row.Operation != nil {
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
	partition := escalation.EffectivePartition(input.DesiredSpec.Pacing, rollingUpdate)
	heldIndices := escalation.PartitionHeldIndices(partition, input, plan.Instances, target.Name)
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
		decision := workloadops.EvaluateUpdateTrigger(input, inst, target, updateByInstance[inst.Index])
		if decision.AdoptRevision {
			logf.FromContext(ctx).V(1).Info("update not selected: adopting revision in place",
				"component", plan.Component, "instance", inst.Index, "target", target.Name)
			selection.Items = append(selection.Items, UpdateItem{Instance: inst, AdoptRevision: true})
			continue
		}
		if !decision.Trigger {
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
		gateExempt := types.RecreateOfDarkFailedRow(row, strategy)
		logf.FromContext(ctx).V(1).Info("update selected",
			"component", plan.Component, "instance", inst.Index, "target", target.Name,
			"startingFresh", startingFresh, "coordGateExempt", gateExempt)
		selection.Items = append(selection.Items, UpdateItem{Instance: inst, StartingFresh: startingFresh, CoordGateExempt: gateExempt})
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
		"priorUnavailInFlight", selection.PriorUnavailInFlight)
	return selection, retryBlockWait, nil
}
