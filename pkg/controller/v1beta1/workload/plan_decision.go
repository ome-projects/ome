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

	appsv1 "k8s.io/api/apps/v1"
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
	Instance InstancePlan
	Reason   string
}

// MigrationSelection is the Manual migration record the migrate pass
// drives this reconcile (a copy of the ObservedState record).
type MigrationSelection struct {
	Record MigrationRecord
}

// UpdateItem is one Instance the update pass touches, in plan order.
type UpdateItem struct {
	Instance InstancePlan
	// AdoptRevision: stamp Ready-on-target (the RunningRevision
	// backfill) instead of running the Update op — the Instance's
	// runtime-ready pods already match the target.
	AdoptRevision bool
	// StartingFresh: this Instance STARTS a new update rather than
	// continuing an in-flight one, so it is subject to the
	// per-Component budget and the coordination UpdateGate before the
	// op runs, and it charges the within-pass counters once dispatched.
	// An Instance whose in-flight UPDATE escalated to Phase=Failed
	// (Update operation preserved for retry) is a CONTINUATION of that
	// surge/drain: its in-flight pod already counts against the budget
	// (CurrentUnavailableInFlight charges Failed+Update), so charging
	// it again would project over budget and gate the recovery forever.
	// Failed with any OTHER preserved operation is not budget-charged
	// as in-flight and starts fresh — the exemption and the charge must
	// cover the same set, or unbudgeted recreates skew the gate.
	StartingFresh bool
	// CoordGateExempt: this fresh start skips the coordination
	// UpdateGate consult (the per-Component budget still applies). Set
	// for a Failed Instance with zero serving pods on a non-surge
	// strategy: the gate's unavailability accounting is serving-based
	// (desired - ServingReplicas), so the Instance's outage is already
	// inside the gate's current count — charging its own recreate +1 on
	// top projects over budget on every pass and starves the recovery
	// (nothing else can raise ServingReplicas; the denied recreate IS
	// the recovery). Surge strategies keep the consult: their gates
	// count surge pods, not serving loss, and the recreate genuinely
	// adds one.
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
	// Items in plan order. Instances held by RollingUpdate.Partition
	// (canary) and Instances with no trigger are not listed.
	Items []UpdateItem
	// Strategy is the resolved update strategy (empty Type defaults to
	// SurgeThenDrain — matches workload.BuildPlan's defaulting).
	// SurgeThenDrain doesn't take pods offline, so the executor gates
	// on the budget that actually applies to the strategy.
	Strategy UpdateStrategyType
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
	Demotions []DemotionSelection

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
	// first-precedence-first. A paused plan truncates the list after
	// the restart pass: a deliberate replica reduction still releases
	// capacity and the RestartPolicy keeps repairing existing
	// Instances, but no Migration, Update, or Create operation is
	// started or advanced. A frozen pause (PauseFreeze) truncates
	// after the truth pass — repair is suspended too. The truth pass
	// (ActionDemote) is status-only, selected only while paused (in
	// every depth): pause suspends lifecycle operations, never status
	// truth, while unpaused reconciles leave the correction to Create.
	Actions []PlannedAction

	// RequeueAfter is the earliest not-yet-due Backoff RetryBlock
	// wake-up reported by the update-trigger evaluation — the one
	// requeue the decision layer owns without running an op. The
	// executor folds it additively into the pass result
	// (foldRetryAfter) so the gate is re-evaluated on time; it only
	// ever ADDS a wake-up, never delays one an op scheduled.
	RequeueAfter time.Duration

	// Escalate gates the terminal-failure escalation pass
	// (escalation.go). False while paused: escalation is suspended with the
	// rest of the lifecycle machinery (deadlines are parked by the adapter
	// while paused). The executor separately guards status-commit boundaries
	// and excludes deferred scale-down extras.
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
func Plan(ctx context.Context, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) (Decision, error) {
	var d Decision

	// Scale-down selection: indices observed but no longer planned.
	// Runs before everything else so excess pods don't run alongside
	// in-flight drains.
	if extras := ExtraInstanceIndices(input.ObservedState.InstanceStatuses, plan, false); len(extras) > 0 || hasDeleteOwnedInstance(input.ObservedState.InstanceStatuses) {
		d.Actions = append(d.Actions, PlannedAction{Kind: ActionScaleDown, Extras: extras})
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
		demotions, derr := planUnbackedDemotions(ctx, input, plan, snapshot)
		if derr != nil {
			return Decision{}, derr
		}
		if len(demotions) > 0 {
			d.Actions = append(d.Actions, PlannedAction{Kind: ActionDemote, Demotions: demotions})
		}
	}

	// Paused is an operator circuit breaker: scale-down above still
	// releases capacity, and the restart pass below keeps repairing
	// existing Instances at their current revision (RunningRevision —
	// repair can never advance a rollout), but no Migration, Update, or
	// Create work is planned. PauseFreeze suspends the restart pass
	// too. Pod and spec watches enqueue the workload again after
	// unpause; no periodic requeue is needed while the desired state is
	// intentionally held.
	if plan.Paused && plan.PauseFreeze {
		return d, nil
	}
	if !plan.Paused {
		d.Escalate = true
	}

	// Restart selection: per-Instance pod-loss / pod-Failed triggers,
	// evaluated against the LIVE pod read — restart is destructive and
	// must not select from a stale cache. In-flight restarts
	// (Phase=Restarting) re-select so their state machine advances.
	if plan.RestartPolicy == RestartPolicyRecreateInstance {
		liveByInstance, lerr := snapshot.LivePods(ctx)
		if lerr != nil {
			return Decision{}, fmt.Errorf("workload.Reconcile: list pods for restart pass (component=%s): %w", plan.Component, lerr)
		}
		var restarts []RestartSelection
		for _, inst := range plan.Instances {
			if needs, reason := workloadops.DetectRestartTriggerWithPods(input, plan, inst, liveByInstance[inst.Index]); needs {
				restarts = append(restarts, RestartSelection{Instance: inst, Reason: reason})
			}
		}
		if len(restarts) > 0 {
			d.Actions = append(d.Actions, PlannedAction{Kind: ActionRestart, Restarts: restarts})
		}
	}

	// A standard pause plans nothing beyond repair.
	if plan.Paused {
		return d, nil
	}

	// Migration expiry selection — the deadline consumer. Planned
	// BEFORE the drive pass so an expired record is consumed before it
	// can be driven (re-stamped) again, and regardless of MigrationMode
	// so a mode flip to Never can never strand a non-terminal record.
	if workloadops.HasExpiredMigrationCandidate(input.ObservedState.Migrations, input.Now()) {
		d.Actions = append(d.Actions, PlannedAction{Kind: ActionMigrateExpiry})
	}

	// Migration drive selection. Work comes from the owner's
	// status.migrations records: the oldest non-terminal Manual record
	// is driven, one per pass. Terminal records and Auto records (born
	// terminal) are excluded structurally — records, never work. Skip
	// when mode is Never.
	if plan.MigrationMode != MigrationModeNever {
		if rec := NextManualMigration(input.ObservedState.Migrations); rec != nil {
			// A fresh migration adds one surge Instance. Let an existing update
			// surge finish first so the two operations do not stack extra capacity.
			// Allocated migrations always resume from their durable record.
			allocated := rec.SurgeInstance != nil && *rec.SurgeInstance >= 0
			if allocated || CurrentSurgeInFlight(input.ObservedState.InstanceStatuses) == 0 {
				d.Actions = append(d.Actions, PlannedAction{Kind: ActionMigrate, Migration: &MigrationSelection{Record: *rec}})
			}
		}
	}

	// Update selection. A nil target short-circuits the pass
	// (DesiredSpec.PodSpec nil / MinReplicas=0).
	if target != nil {
		sel, retryBlockWait, uerr := planUpdateSelection(ctx, input, plan, target, snapshot)
		if uerr != nil {
			return Decision{}, uerr
		}
		d.RequeueAfter = retryBlockWait
		if len(sel.Items) > 0 {
			d.Actions = append(d.Actions, PlannedAction{Kind: ActionUpdate, Update: &sel})
		}
	}

	// Create — always planned on a non-paused reconcile: the op is the
	// pass's own detector (it lists and diffs per planned Instance) and
	// a nil target returns immediately.
	d.Actions = append(d.Actions, PlannedAction{Kind: ActionCreate})

	return d, nil
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
func planUnbackedDemotions(ctx context.Context, input ReconcileInput, plan ComponentPlan, snapshot *ObservedSnapshot) ([]DemotionSelection, error) {
	if plan.RestartPolicy == RestartPolicyRecreateInstance {
		return nil, nil
	}
	planned := make(map[int32]struct{}, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var candidates []int32
	for i := range input.ObservedState.InstanceStatuses {
		s := &input.ObservedState.InstanceStatuses[i]
		if _, ok := planned[s.Index]; !ok {
			continue
		}
		if s.Phase != InstancePhaseReady || s.Operation != nil {
			continue
		}
		candidates = append(candidates, s.Index)
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
	var out []DemotionSelection
	for _, idx := range unbacked {
		if len(live[idx]) == 0 {
			out = append(out, DemotionSelection{Index: idx, Reason: "no live pods back the Ready phase"})
		}
	}
	return out, nil
}

// planUpdateSelection evaluates the per-Instance update triggers
// (pure) over the snapshot's cached pods and returns the selection
// plus the earliest not-yet-due RetryBlock wake-up across denied
// Instances.
func planUpdateSelection(ctx context.Context, input ReconcileInput, plan ComponentPlan, target *appsv1.ControllerRevision, snapshot *ObservedSnapshot) (UpdateSelection, time.Duration, error) {
	var rollingUpdate *RollingUpdate
	if plan.UpdateStrategy.RollingUpdate != nil {
		rollingUpdate = plan.UpdateStrategy.RollingUpdate
	}
	strategy := plan.UpdateStrategy.Type
	if strategy == "" {
		strategy = UpdateStrategySurgeThenDrain
	}
	sel := UpdateSelection{
		Strategy:             strategy,
		SurgeBudget:          PerComponentMaxSurgeBudget(rollingUpdate, plan.Replicas),
		UnavailBudget:        PerComponentMaxUnavailableBudget(rollingUpdate, plan.Replicas),
		PriorSurgeInFlight:   CurrentSurgeInFlight(input.ObservedState.InstanceStatuses),
		PriorUnavailInFlight: CurrentUnavailableInFlight(input.ObservedState.InstanceStatuses),
	}
	// One cached List + bucket for the whole Component (the snapshot's
	// non-destructive read source) — an absent bucket is an empty pod
	// set.
	updateByInstance, lerr := snapshot.CachedPods(ctx)
	if lerr != nil {
		return UpdateSelection{}, 0, fmt.Errorf("workload.Reconcile: list pods for update pass (component=%s): %w", plan.Component, lerr)
	}
	heldIndices := PartitionHeldIndices(rollingUpdate, input.ObservedState.InstanceStatuses, plan.Instances, target.Name)
	var retryBlockWait time.Duration
	for _, inst := range plan.Instances {
		// RollingUpdate.Partition (canary): hold the selected Instances
		// on their current revision — skip their update. Membership is
		// keyed to revision, not index position, and Instances already
		// converging to target are never held (they must finish); see
		// PartitionHeldIndices for the full candidacy rule.
		if heldIndices[inst.Index] {
			logf.FromContext(ctx).V(1).Info("update not selected: held by partition",
				"component", plan.Component, "instance", inst.Index, "target", target.Name)
			continue
		}
		dec := workloadops.EvaluateUpdateTrigger(input, inst, target, input.DesiredSpec.PodSpec, updateByInstance[inst.Index])
		if dec.AdoptRevision {
			logf.FromContext(ctx).V(1).Info("update not selected: adopting revision in place",
				"component", plan.Component, "instance", inst.Index, "target", target.Name)
			sel.Items = append(sel.Items, UpdateItem{Instance: inst, AdoptRevision: true})
			continue
		}
		if !dec.Trigger {
			// Denied by a not-yet-due Backoff RetryBlock — keep the
			// earliest re-evaluation time across Instances.
			if dec.RetryAfter > 0 && (retryBlockWait == 0 || dec.RetryAfter < retryBlockWait) {
				retryBlockWait = dec.RetryAfter
			}
			// Wreckage scan: the trigger is revision-diff-keyed, so an
			// instance at zero revision distance (corrective roll-back)
			// or carrying third-party-revision debris never dispatches —
			// yet its superseded-revision wreckage must still be
			// abandoned toward the current desired state. Pure snapshot
			// read; the effect runs in Execute (ops.CleanupWreckage).
			s := findObservedInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
			cleanup := workloadops.EvaluateWreckage(s, target, updateByInstance[inst.Index])
			if cleanup {
				sel.Items = append(sel.Items, UpdateItem{Instance: inst, CleanupOnly: true})
			}
			// The trigger declining is the single most common reason a
			// rollout makes no progress, and the phase/revision pair it
			// keyed on is not otherwise recoverable after the fact.
			log := logf.FromContext(ctx).V(1)
			if s == nil {
				log.Info("update not triggered: no observed status",
					"component", plan.Component, "instance", inst.Index,
					"target", target.Name, "cleanupOnly", cleanup)
			} else {
				log.Info("update not triggered",
					"component", plan.Component, "instance", inst.Index,
					"phase", s.Phase, "runningRevision", s.RunningRevision,
					"target", target.Name, "hasOperation", s.Operation != nil,
					"retryAfter", dec.RetryAfter, "cleanupOnly", cleanup)
			}
			continue
		}
		s := findObservedInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
		startingFresh := s == nil ||
			(s.Phase != InstancePhaseUpdating &&
				!(s.Phase == InstancePhaseFailed && s.Operation != nil && s.Operation.Type == InstanceOperationUpdate))
		gateExempt := startingFresh && strategy != UpdateStrategySurgeThenDrain &&
			s != nil && s.Phase == InstancePhaseFailed && s.ServingPodCount == 0
		logf.FromContext(ctx).V(1).Info("update selected",
			"component", plan.Component, "instance", inst.Index, "target", target.Name,
			"startingFresh", startingFresh, "coordGateExempt", gateExempt)
		sel.Items = append(sel.Items, UpdateItem{Instance: inst, StartingFresh: startingFresh, CoordGateExempt: gateExempt})
	}
	// Budgets are decided here but spent in Execute, so record them with the
	// selection they apply to: a selection that is non-empty yet starts
	// nothing is a budget or gate outcome, not a selection one.
	logf.FromContext(ctx).V(1).Info("update selection complete",
		"component", plan.Component, "target", target.Name,
		"considered", len(plan.Instances), "selected", len(sel.Items),
		"strategy", sel.Strategy, "surgeBudget", sel.SurgeBudget,
		"unavailBudget", sel.UnavailBudget,
		"priorSurgeInFlight", sel.PriorSurgeInFlight,
		"priorUnavailInFlight", sel.PriorUnavailInFlight)
	return sel, retryBlockWait, nil
}
