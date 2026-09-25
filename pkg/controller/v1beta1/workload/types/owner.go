// owner.go — who drives one Instance row.
//
// A reconcile runs five verb passes over the same InstanceStatus slice.
// Exactly one of them may advance a given row's phase and operation in a
// pass, and which one is a property of the row alone: its phase, the type
// of the operation it carries, and that operation's step. This file is
// the single answer, so a pass, a hold authority and the escalation arm
// cannot disagree about a row they all meet.
//
// The table below is keyed by the state ids of the Instance state machine
// and by its event ids, so a reader can hold the two side by side and a
// spec-side test can prove they name the same states.
package types

// RowOwner names the one pass that may advance a row's phase and
// operation this reconcile. The values are the verb packages under ops/,
// plus OwnerNone for a row no verb pass drives — a settled row whose next
// move belongs to a trigger, or a Failed row whose next move belongs to
// the escalation arm and the operator.
type RowOwner string

const (
	OwnerNone    RowOwner = "none"
	OwnerCreate  RowOwner = "create"
	OwnerUpdate  RowOwner = "update"
	OwnerRestart RowOwner = "restart"
	OwnerMigrate RowOwner = "migrate"
	OwnerDelete  RowOwner = "delete"
)

// RowState is one persisted (Phase, Operation.Type, Operation.Step) an
// InstanceStatus can carry. The ids mirror the state ids of the Instance
// state machine; a spec-side test holds the two sets together.
type RowState string

const (
	// StateUnknown is a row whose phase and operation name no state of
	// the machine. No pass owns it: an unrecognized combination is not a
	// step anything knows how to advance.
	StateUnknown RowState = ""

	StateEmpty   RowState = "Empty"
	StatePending RowState = "Pending"
	StateReady   RowState = "Ready"
	StateFailed  RowState = "Failed"

	StateCreateCreatePods RowState = "Create/CreatePods"

	StateUpdateSurge                  RowState = "Update/Surge"
	StateUpdateSurgeDrain             RowState = "Update/SurgeDrain"
	StateUpdateGangSurgeTarget        RowState = "Update/GangSurgeTarget"
	StateUpdateGangSurgeTargetCleanup RowState = "Update/GangSurgeTargetCleanup"
	StateUpdateInPlace                RowState = "Update/InPlace"
	StateUpdateDrain                  RowState = "Update/Drain"

	StateRestartDrain RowState = "Restart/Drain"

	StateMigrateCreateSurge RowState = "Migrate/CreateSurge"
	StateMigrateSurgeTarget RowState = "Migrate/SurgeTarget"

	StateDeleteDrain RowState = "Delete/Drain"
)

// Event is something that happens to a row, named in the state machine's
// own vocabulary: an event id such as "spec.pauseTrue", or "event[variant]"
// such as "pod.unschedulable[target]" when only one variant is meant. The
// events named here are the ones that are not a verb and mean the same
// thing in most states: the six external holds and the two clocks the
// escalation arm runs. They are the events whose effect on a row's owner
// every pass would otherwise decide for itself.
type Event string

const (
	// The six external holds.
	EventQuotaDenied         Event = "api.quotaDenied"
	EventUnschedulableSource Event = "pod.unschedulable[source]"
	EventUnschedulableTarget Event = "pod.unschedulable[target]"
	EventPodGroupTerminating Event = "gang.podGroup[Terminating]"
	EventNodeUnknown         Event = "pod.phaseTerminal[Unknown]"
	EventSourceUnrouted      Event = "pod.servingOff[source]"
	EventPauseTrue           Event = "spec.pauseTrue"
	EventPauseFreeze         Event = "spec.pauseFreeze"

	// The two clocks the escalation arm runs against an in-flight step.
	EventOperationDeadline Event = "timer.operationDeadline"
	EventStuckPodGrace     Event = "timer.stuckPodGrace"
)

// ownership is one row of the ownership table: who drives the state, and
// which events act on it mid-step instead of waiting for the step
// to reach a boundary.
type ownership struct {
	owner RowOwner
	// interrupts holds the events whose cell in this state fires a
	// transition or an escalation in the "before" window — the event
	// takes the row out from under its owner without the owner's step
	// having to land first. Every other event defers: the observation is
	// recorded, and the row moves only when its owner's step does.
	interrupts map[Event]struct{}
}

// ownershipTable is the ownership half of the Instance state machine,
// one row per state.
//
// The interrupts sets say the same thing in every in-flight state: the
// two clocks end a step, and none of the six holds does — a hold parks
// the deadline and the owner keeps its row. The exceptions are the states
// whose owner is not the escalation arm's to end: a migration pair is the
// migration record's, which fails fast through itself, and a teardown is
// DeleteBatch's, which paces its own removal past any grace. A settled
// row (Empty, Ready, Pending) has no step to interrupt, and the stuck-pod
// grace reaches it only as the leftover-pod escalation.
var ownershipTable = map[RowState]ownership{
	StateEmpty:   {owner: OwnerCreate},
	StatePending: {owner: OwnerCreate, interrupts: interruptedBy(EventStuckPodGrace)},
	StateReady:   {owner: OwnerNone, interrupts: interruptedBy(EventStuckPodGrace)},
	StateFailed:  {owner: OwnerNone},

	StateCreateCreatePods: {owner: OwnerCreate, interrupts: interruptedBy(EventOperationDeadline, EventStuckPodGrace)},

	StateUpdateSurge:                  {owner: OwnerUpdate, interrupts: interruptedBy(EventOperationDeadline, EventStuckPodGrace)},
	StateUpdateSurgeDrain:             {owner: OwnerUpdate, interrupts: interruptedBy(EventOperationDeadline, EventStuckPodGrace)},
	StateUpdateGangSurgeTarget:        {owner: OwnerUpdate, interrupts: interruptedBy(EventOperationDeadline, EventStuckPodGrace)},
	StateUpdateGangSurgeTargetCleanup: {owner: OwnerUpdate, interrupts: interruptedBy(EventOperationDeadline, EventStuckPodGrace)},
	StateUpdateInPlace:                {owner: OwnerUpdate, interrupts: interruptedBy(EventOperationDeadline, EventStuckPodGrace)},
	StateUpdateDrain:                  {owner: OwnerUpdate, interrupts: interruptedBy(EventOperationDeadline, EventStuckPodGrace)},

	StateRestartDrain: {owner: OwnerRestart, interrupts: interruptedBy(EventOperationDeadline, EventStuckPodGrace)},

	StateMigrateCreateSurge: {owner: OwnerMigrate},
	StateMigrateSurgeTarget: {owner: OwnerMigrate},

	StateDeleteDrain: {owner: OwnerDelete},

	StateUnknown: {owner: OwnerNone},
}

func interruptedBy(list ...Event) map[Event]struct{} {
	out := make(map[Event]struct{}, len(list))
	for _, c := range list {
		out[c] = struct{}{}
	}
	return out
}

// StateOf resolves a persisted row to its state. A nil row is the empty
// slot the status writer's append path hands out, which is the Empty
// state.
//
// The operation decides wherever one is recorded: a row carrying a step
// is that machine's whatever else it reports. Phase decides for the two
// shapes that carry no step — a Failed row, whose preserved operation is
// the record of a spent attempt rather than one in flight, and a settled
// row — and it disambiguates the migration pair, whose two rows carry the
// same step literal and are told apart by phase.
func StateOf(s *InstanceStatus) RowState {
	if s == nil {
		return StateEmpty
	}
	if s.Phase == InstancePhaseFailed {
		return StateFailed
	}
	if s.Operation == nil {
		switch s.Phase {
		case InstancePhaseEmpty:
			return StateEmpty
		case InstancePhasePending:
			return StatePending
		case InstancePhaseReady:
			return StateReady
		case InstancePhaseMigrating:
			// The pin is the migration record's claim on the row; the
			// phase outlives any one write of it.
			return StateMigrateCreateSurge
		}
		return StateUnknown
	}
	switch s.Operation.Type {
	case InstanceOperationCreate:
		return StateCreateCreatePods
	case InstanceOperationUpdate:
		return updateStateOf(s.Operation.Step)
	case InstanceOperationRestart:
		return StateRestartDrain
	case InstanceOperationMigrate:
		if s.Phase == InstancePhaseMigrating {
			return StateMigrateCreateSurge
		}
		return StateMigrateSurgeTarget
	case InstanceOperationDelete:
		return StateDeleteDrain
	}
	return StateUnknown
}

// updateStateOf splits the Update machine into its sub-machines by step;
// SurgeDrainSettle reads as the drain half of the surge cycle.
func updateStateOf(step string) RowState {
	switch step {
	case UpdateStepSurge:
		return StateUpdateSurge
	case UpdateStepSurgeDrain, UpdateStepSurgeDrainSettle:
		return StateUpdateSurgeDrain
	case UpdateStepGangSurgeTarget:
		return StateUpdateGangSurgeTarget
	case UpdateStepGangSurgeTargetCleanup:
		return StateUpdateGangSurgeTargetCleanup
	case UpdateStepInPlace:
		return StateUpdateInPlace
	case UpdateStepDrain:
		return StateUpdateDrain
	}
	return StateUnknown
}

// Owner reports the one pass that may advance this row's phase and
// operation this reconcile. It reads the persisted row and nothing else:
// not the pass's own inputs, not the pause, not the counters the status
// publication writes.
func Owner(s *InstanceStatus) RowOwner {
	return ownershipTable[StateOf(s)].owner
}

// OwnerOfOperation is Owner reduced to the operation, for the one caller
// that cannot see the whole row: the external-hold writer judges
// eligibility against the fresh operation it re-reads inside the
// mutation. A row with no operation is nobody's to hold.
func OwnerOfOperation(op *InstanceOperation) RowOwner {
	if op == nil {
		return OwnerNone
	}
	return Owner(&InstanceStatus{Operation: op})
}

// ClaimOf is the second question a row answers, beside Owner: not who
// may advance it this reconcile, but which verb the operation it carries
// still belongs to, whatever the phase says. The two differ on a Failed
// row. Its preserved operation is a spent attempt no pass drives, so
// Owner is OwnerNone; but the pinned revision, the surge index and the
// migration pair it names are still that verb's, and a trigger deciding
// whether to start something new on the row asks this question. A row
// with no operation claims nothing.
func ClaimOf(s *InstanceStatus) RowOwner {
	if s == nil || s.Operation == nil {
		return OwnerNone
	}
	switch s.Operation.Type {
	case InstanceOperationCreate:
		return OwnerCreate
	case InstanceOperationUpdate:
		return OwnerUpdate
	case InstanceOperationRestart:
		return OwnerRestart
	case InstanceOperationMigrate:
		return OwnerMigrate
	case InstanceOperationDelete:
		return OwnerDelete
	}
	return OwnerNone
}

// UpdateContinuation reports whether the row carries an Update attempt
// the update pass continues rather than starts: a row in Phase=Updating,
// or a Failed row whose preserved operation is an Update. The in-flight
// pod of either already counts against the unavailability budget, so a
// continuation is never admitted or charged again — the exemption and
// the charge cover the same set.
func UpdateContinuation(s *InstanceStatus) bool {
	if s == nil {
		return false
	}
	return s.Phase == InstancePhaseUpdating ||
		(s.Phase == InstancePhaseFailed && ClaimOf(s) == OwnerUpdate)
}

// RecreateOfDarkFailedRow reports a fresh update start that takes
// nothing further offline: a Failed row with no serving pod, recreated
// in place on a non-surge strategy. The coordination gate already counts
// its outage in its serving-based unavailability, so this start skips
// the gate consult; a surge strategy keeps it, because its gate counts
// surge pods and the recreate genuinely adds one.
func RecreateOfDarkFailedRow(s *InstanceStatus, strategy UpdateStrategyType) bool {
	return s != nil && !UpdateContinuation(s) &&
		strategy != UpdateStrategySurgeThenDrain &&
		s.Phase == InstancePhaseFailed && s.ServingPodCount == 0
}

// Interruptible reports whether the named event acts on this row
// while its owner's step is still in flight, rather than waiting for the
// step to reach a boundary.
//
// An event this table does not name is not answered here and reads as
// waiting.
func Interruptible(s *InstanceStatus, event Event) bool {
	_, ok := ownershipTable[StateOf(s)].interrupts[event]
	return ok
}

// OwnedRows is one pass's rows grouped by the owner that may advance
// them. It is built once per observation, from the table, and handed to
// the passes: a pass reads the list it owns instead of walking every row
// and deciding eligibility for itself, so every pass in the reconcile
// gets the same answer.
//
// Rows keep observation order. InFlight is the subset carrying a step —
// a row in one of the machine's operation states rather than a settled
// one — because "may I act on this row" and "is an attempt open on it"
// are different questions and the passes ask both.
type OwnedRows struct {
	rows     map[RowOwner][]int32
	inFlight map[RowOwner][]int32
}

// OwnedRows returns the pass's owner grouping: the one the decision
// layer built when the pass is running under a reconcile, and a freshly
// derived one when a pass is invoked on its own.
func (in ReconcileInput) OwnedRows() OwnedRows {
	if in.Owned != nil {
		return *in.Owned
	}
	return RowsByOwner(in.ObservedState.InstanceStatuses)
}

// RowsByOwner groups the observed rows by their owner.
func RowsByOwner(statuses []InstanceStatus) OwnedRows {
	o := OwnedRows{
		rows:     make(map[RowOwner][]int32, len(ownershipTable)),
		inFlight: make(map[RowOwner][]int32, len(ownershipTable)),
	}
	for i := range statuses {
		state := StateOf(&statuses[i])
		owner := ownershipTable[state].owner
		idx := statuses[i].Index
		o.rows[owner] = append(o.rows[owner], idx)
		if !settledState(state) {
			o.inFlight[owner] = append(o.inFlight[owner], idx)
		}
	}
	return o
}

// settledState reports whether a state carries no step for an owner to
// advance: no row, a row waiting to be materialized, a serving row, or a
// spent attempt.
func settledState(state RowState) bool {
	switch state {
	case StateEmpty, StatePending, StateReady, StateFailed, StateUnknown:
		return true
	}
	return false
}

// Any reports whether the owner drives any row this pass.
func (o OwnedRows) Any(owner RowOwner) bool { return len(o.rows[owner]) > 0 }

// AnyInFlight reports whether the owner has an open step on any row
// other than except. A caller re-arming one row must not count that
// row's own attempt; pass a negative index to ask about every row.
func (o OwnedRows) AnyInFlight(owner RowOwner, except int32) bool {
	for _, idx := range o.inFlight[owner] {
		if idx != except {
			return true
		}
	}
	return false
}

// Owns reports whether this owner drives the row at idx.
func (o OwnedRows) Owns(owner RowOwner, idx int32) bool {
	for _, i := range o.rows[owner] {
		if i == idx {
			return true
		}
	}
	return false
}

// OwnershipStates lists every state the table answers for, in machine
// order, so a test can hold the table and the machine side by side.
func OwnershipStates() []RowState {
	return []RowState{
		StateEmpty, StatePending, StateReady, StateFailed,
		StateCreateCreatePods,
		StateUpdateSurge, StateUpdateSurgeDrain,
		StateUpdateGangSurgeTarget, StateUpdateGangSurgeTargetCleanup,
		StateUpdateInPlace, StateUpdateDrain,
		StateRestartDrain,
		StateMigrateCreateSurge, StateMigrateSurgeTarget,
		StateDeleteDrain,
	}
}

// OwnershipEvents lists every event the table answers for, in
// table order.
func OwnershipEvents() []Event {
	return []Event{
		EventQuotaDenied,
		EventUnschedulableSource, EventUnschedulableTarget,
		EventPodGroupTerminating, EventNodeUnknown, EventSourceUnrouted,
		EventPauseTrue, EventPauseFreeze,
		EventOperationDeadline, EventStuckPodGrace,
	}
}

// StateInterruptible is Interruptible for a state named directly.
func StateInterruptible(state RowState, event Event) bool {
	_, ok := ownershipTable[state].interrupts[event]
	return ok
}
