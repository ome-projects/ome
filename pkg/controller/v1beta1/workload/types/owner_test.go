package types

import "testing"

func rowFor(phase InstancePhase, opType InstanceOperationType, step string) *InstanceStatus {
	s := &InstanceStatus{Phase: phase}
	if opType != "" {
		s.Operation = &InstanceOperation{Type: opType, Step: step}
	}
	return s
}

// TestOwner_EveryMachineStateHasExactlyOneOwner pins the owner event of
// the ownership table for every state of the Instance machine. The rows
// are the states' own (phase, operation type, step); a spec-side test
// proves this list is the machine's list.
func TestOwner_EveryMachineStateHasExactlyOneOwner(t *testing.T) {
	cases := []struct {
		state RowState
		row   *InstanceStatus
		want  RowOwner
	}{
		{StateEmpty, nil, OwnerCreate},
		{StateEmpty, rowFor(InstancePhaseEmpty, "", ""), OwnerCreate},
		{StatePending, rowFor(InstancePhasePending, "", ""), OwnerCreate},
		{StateReady, rowFor(InstancePhaseReady, "", ""), OwnerNone},
		{StateFailed, rowFor(InstancePhaseFailed, "", ""), OwnerNone},
		{StateCreateCreatePods, rowFor(InstancePhaseCreating, InstanceOperationCreate, "CreatePods"), OwnerCreate},
		{StateUpdateSurge, rowFor(InstancePhaseUpdating, InstanceOperationUpdate, UpdateStepSurge), OwnerUpdate},
		{StateUpdateSurgeDrain, rowFor(InstancePhaseUpdating, InstanceOperationUpdate, UpdateStepSurgeDrain), OwnerUpdate},
		{StateUpdateGangSurgeTarget, rowFor(InstancePhaseCreating, InstanceOperationUpdate, UpdateStepGangSurgeTarget), OwnerUpdate},
		{StateUpdateGangSurgeTargetCleanup, rowFor(InstancePhaseCreating, InstanceOperationUpdate, UpdateStepGangSurgeTargetCleanup), OwnerUpdate},
		{StateUpdateInPlace, rowFor(InstancePhaseUpdating, InstanceOperationUpdate, UpdateStepInPlace), OwnerUpdate},
		{StateUpdateDrain, rowFor(InstancePhaseUpdating, InstanceOperationUpdate, UpdateStepDrain), OwnerUpdate},
		{StateRestartDrain, rowFor(InstancePhaseRestarting, InstanceOperationRestart, "Drain"), OwnerRestart},
		{StateMigrateCreateSurge, rowFor(InstancePhaseMigrating, InstanceOperationMigrate, "CreateSurge"), OwnerMigrate},
		{StateMigrateSurgeTarget, rowFor(InstancePhaseCreating, InstanceOperationMigrate, "CreateSurge"), OwnerMigrate},
		{StateDeleteDrain, rowFor(InstancePhaseDeleting, InstanceOperationDelete, "Drain"), OwnerDelete},
	}
	seen := map[RowState]bool{}
	for _, tc := range cases {
		if got := StateOf(tc.row); got != tc.state {
			t.Errorf("StateOf(%s row) = %q, want %q", tc.state, got, tc.state)
		}
		if got := Owner(tc.row); got != tc.want {
			t.Errorf("Owner(%s) = %q, want %q", tc.state, got, tc.want)
		}
		seen[tc.state] = true
	}
	for _, state := range OwnershipStates() {
		if !seen[state] {
			t.Errorf("state %q has no owner case", state)
		}
	}
}

// TestOwnerUnknownCombinations pins the total answer: a row whose phase and
// operation name no state of the machine is nobody's to advance.
func TestOwnerUnknownCombinations(t *testing.T) {
	cases := []struct {
		name string
		row  *InstanceStatus
	}{
		{"transient phase with no operation", rowFor(InstancePhaseUpdating, "", "")},
		{"update step nothing writes", rowFor(InstancePhaseUpdating, InstanceOperationUpdate, "Teleport")},
		{"operation type nothing writes", rowFor(InstancePhaseUpdating, "Reticulate", "Splines")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := StateOf(tc.row); got != StateUnknown {
				t.Errorf("StateOf = %q, want unknown", got)
			}
			if got := Owner(tc.row); got != OwnerNone {
				t.Errorf("Owner = %q, want %q", got, OwnerNone)
			}
		})
	}
}

// TestOwnerOfOperationMatchesOwner holds the two entry points together: the
// external-hold writer sees only the operation, and must reach the same
// owner the whole row would, for every operation type. A nil operation is
// nobody's — there is no attempt to hold.
func TestOwnerOfOperationMatchesOwner(t *testing.T) {
	if got := OwnerOfOperation(nil); got != OwnerNone {
		t.Errorf("OwnerOfOperation(nil) = %q, want %q", got, OwnerNone)
	}
	cases := []struct {
		opType InstanceOperationType
		step   string
		want   RowOwner
	}{
		{InstanceOperationCreate, "CreatePods", OwnerCreate},
		{InstanceOperationUpdate, UpdateStepSurge, OwnerUpdate},
		{InstanceOperationUpdate, UpdateStepInPlace, OwnerUpdate},
		{InstanceOperationRestart, "Drain", OwnerRestart},
		{InstanceOperationMigrate, "CreateSurge", OwnerMigrate},
		{InstanceOperationDelete, "Drain", OwnerDelete},
	}
	for _, tc := range cases {
		op := &InstanceOperation{Type: tc.opType, Step: tc.step}
		if got := OwnerOfOperation(op); got != tc.want {
			t.Errorf("OwnerOfOperation(%s/%s) = %q, want %q", tc.opType, tc.step, got, tc.want)
		}
	}
}

// TestClaimOfReadsTheOperationWhateverThePhase holds the two questions
// apart: Owner is who may advance the row, and is nobody for a Failed
// row; ClaimOf is the verb the preserved operation still belongs to, and
// the phase does not change it. A row with no operation claims nothing.
func TestClaimOfReadsTheOperationWhateverThePhase(t *testing.T) {
	if got := ClaimOf(nil); got != OwnerNone {
		t.Errorf("ClaimOf(nil) = %q, want %q", got, OwnerNone)
	}
	if got := ClaimOf(&InstanceStatus{Phase: InstancePhaseMigrating}); got != OwnerNone {
		t.Errorf("ClaimOf(Migrating, no operation) = %q, want %q: the pin is the phase's, not an operation's", got, OwnerNone)
	}
	for _, tc := range []struct {
		opType InstanceOperationType
		want   RowOwner
	}{
		{InstanceOperationCreate, OwnerCreate},
		{InstanceOperationUpdate, OwnerUpdate},
		{InstanceOperationRestart, OwnerRestart},
		{InstanceOperationMigrate, OwnerMigrate},
		{InstanceOperationDelete, OwnerDelete},
	} {
		for _, phase := range []InstancePhase{InstancePhaseFailed, InstancePhaseReady, InstancePhaseUpdating} {
			row := &InstanceStatus{Phase: phase, Operation: &InstanceOperation{Type: tc.opType, Step: UpdateStepSurge}}
			if got := ClaimOf(row); got != tc.want {
				t.Errorf("ClaimOf(%s + %s) = %q, want %q", phase, tc.opType, got, tc.want)
			}
		}
		failed := &InstanceStatus{Phase: InstancePhaseFailed, Operation: &InstanceOperation{Type: tc.opType, Step: UpdateStepSurge}}
		if Owner(failed) != OwnerNone {
			t.Errorf("Owner(Failed + %s) = %q, want %q: a spent attempt is nobody's to advance", tc.opType, Owner(failed), OwnerNone)
		}
	}
}

// TestFailedRowIsNobodys pins the one place where the phase overrules the
// operation: a Failed row's preserved operation records a spent attempt, so
// no verb pass may resume it.
func TestFailedRowIsNobodys(t *testing.T) {
	for _, opType := range []InstanceOperationType{
		InstanceOperationCreate, InstanceOperationUpdate, InstanceOperationRestart,
		InstanceOperationMigrate, InstanceOperationDelete,
	} {
		row := rowFor(InstancePhaseFailed, opType, "Drain")
		if got := StateOf(row); got != StateFailed {
			t.Errorf("StateOf(Failed + %s) = %q, want %q", opType, got, StateFailed)
		}
		if got := Owner(row); got != OwnerNone {
			t.Errorf("Owner(Failed + %s) = %q, want %q", opType, got, OwnerNone)
		}
	}
}

// TestInterruptible_OnlyTheTwoClocksEndAStep pins the interrupt event: the two clocks end an
// in-flight step, and none of the six external holds does — a hold parks
// the deadline and the owner keeps its row. A migration pair and a teardown
// are their own timeout authority, so neither clock reaches them.
func TestInterruptible_OnlyTheTwoClocksEndAStep(t *testing.T) {
	clockStates := []RowState{
		StateCreateCreatePods, StateUpdateSurge, StateUpdateSurgeDrain,
		StateUpdateGangSurgeTarget, StateUpdateGangSurgeTargetCleanup,
		StateUpdateInPlace, StateUpdateDrain, StateRestartDrain,
	}
	for _, state := range clockStates {
		for _, event := range []Event{EventOperationDeadline, EventStuckPodGrace} {
			if !StateInterruptible(state, event) {
				t.Errorf("%s/%s: want interrupting", state, event)
			}
		}
	}
	selfTimed := []RowState{StateMigrateCreateSurge, StateMigrateSurgeTarget, StateDeleteDrain}
	for _, state := range selfTimed {
		for _, event := range []Event{EventOperationDeadline, EventStuckPodGrace} {
			if StateInterruptible(state, event) {
				t.Errorf("%s/%s: want waiting, the owner is its own timeout authority", state, event)
			}
		}
	}
	holds := []Event{
		EventQuotaDenied, EventUnschedulableSource, EventUnschedulableTarget,
		EventPodGroupTerminating, EventNodeUnknown, EventSourceUnrouted,
		EventPauseTrue, EventPauseFreeze,
	}
	for _, state := range OwnershipStates() {
		for _, event := range holds {
			if StateInterruptible(state, event) {
				t.Errorf("%s/%s: a hold parks the deadline, it does not end the step", state, event)
			}
		}
	}
}

// TestInterruptibleReadsTheRow proves Interruptible resolves the row to its
// state rather than taking one, which is what the passes call it with.
func TestInterruptibleReadsTheRow(t *testing.T) {
	row := rowFor(InstancePhaseUpdating, InstanceOperationUpdate, UpdateStepSurge)
	if !Interruptible(row, EventOperationDeadline) {
		t.Error("a surge in flight is ended by its deadline")
	}
	if Interruptible(row, EventPauseTrue) {
		t.Error("a pause holds the next step, it does not end the one under way")
	}
	if Interruptible(nil, EventOperationDeadline) {
		t.Error("an empty slot has no step to end")
	}
}

// TestRowsByOwner_GroupsEveryRowOnce proves the grouping is the table's
// answer, so a pass reading its own list sees exactly the rows Owner
// names for it, and that InFlight keeps only the rows carrying a step.
func TestRowsByOwner_GroupsEveryRowOnce(t *testing.T) {
	statuses := []InstanceStatus{
		{Index: 0, Phase: InstancePhaseReady},
		{Index: 1, Phase: InstancePhaseCreating, Operation: &InstanceOperation{Type: InstanceOperationCreate}},
		{Index: 2, Phase: InstancePhaseUpdating, Operation: &InstanceOperation{Type: InstanceOperationUpdate, Step: UpdateStepSurge}},
		{Index: 3, Phase: InstancePhaseDeleting, Operation: &InstanceOperation{Type: InstanceOperationDelete}},
		{Index: 4, Phase: InstancePhaseFailed, Operation: &InstanceOperation{Type: InstanceOperationUpdate, Step: UpdateStepSurge}},
	}
	owned := RowsByOwner(statuses)

	for _, tc := range []struct {
		owner RowOwner
		rows  []int32
	}{
		{OwnerNone, []int32{0, 4}},
		{OwnerCreate, []int32{1}},
		{OwnerUpdate, []int32{2}},
		{OwnerDelete, []int32{3}},
		{OwnerRestart, nil},
		{OwnerMigrate, nil},
	} {
		for _, idx := range []int32{0, 1, 2, 3, 4} {
			want := false
			for _, i := range tc.rows {
				want = want || i == idx
			}
			if got := owned.Owns(tc.owner, idx); got != want {
				t.Errorf("%s owns %d = %v, want %v", tc.owner, idx, got, want)
			}
		}
	}
	if !owned.Any(OwnerDelete) || owned.Any(OwnerRestart) {
		t.Error("Any must follow the same grouping")
	}
	if !owned.Owns(OwnerUpdate, 2) || owned.Owns(OwnerUpdate, 4) {
		t.Error("a Failed row is nobody's, whatever operation it preserves")
	}
	// A settled row carries no step: Ready and Failed are grouped but
	// never in flight.
	if owned.AnyInFlight(OwnerNone, allRows) {
		t.Error("settled rows must never be in flight")
	}
	if !owned.AnyInFlight(OwnerCreate, allRows) {
		t.Error("a committed create attempt is in flight")
	}
	if owned.AnyInFlight(OwnerCreate, 1) {
		t.Error("a row re-arming its own attempt must not count itself")
	}
}

// allRows is the "exclude nothing" index the AnyInFlight callers pass.
const allRows int32 = -1

// TestOwnedRows_EmptyInputAnswersForEveryOwner proves the zero grouping
// is usable: a pass invoked outside a reconcile derives its own and no
// lookup panics.
func TestOwnedRows_EmptyInputAnswersForEveryOwner(t *testing.T) {
	owned := ReconcileInput{}.OwnedRows()
	for _, owner := range []RowOwner{OwnerNone, OwnerCreate, OwnerUpdate, OwnerRestart, OwnerMigrate, OwnerDelete} {
		if owned.Any(owner) || owned.AnyInFlight(owner, allRows) || owned.Owns(owner, 0) {
			t.Errorf("%s: an empty observation owns nothing", owner)
		}
	}
	built := RowsByOwner([]InstanceStatus{{Index: 7, Phase: InstancePhaseReady}})
	from := ReconcileInput{Owned: &built}.OwnedRows()
	if !from.Owns(OwnerNone, 7) {
		t.Error("a handed-down grouping must be the one the pass reads")
	}
}

// A continuation is a row in Phase=Updating or a Failed row whose
// preserved operation is an Update; everything else, including a Failed
// row with another operation, starts fresh.
func TestUpdateContinuation_PhaseUpdatingOrFailedWithAnUpdateClaim(t *testing.T) {
	update := &InstanceOperation{Type: InstanceOperationUpdate}
	restart := &InstanceOperation{Type: InstanceOperationRestart}
	cases := []struct {
		name string
		row  *InstanceStatus
		want bool
	}{
		{"nil row", nil, false},
		{"updating", &InstanceStatus{Phase: InstancePhaseUpdating, Operation: update}, true},
		{"failed with update kept", &InstanceStatus{Phase: InstancePhaseFailed, Operation: update}, true},
		{"failed with restart kept", &InstanceStatus{Phase: InstancePhaseFailed, Operation: restart}, false},
		{"failed with nothing", &InstanceStatus{Phase: InstancePhaseFailed}, false},
		{"ready", &InstanceStatus{Phase: InstancePhaseReady}, false},
	}
	for _, tc := range cases {
		if got := UpdateContinuation(tc.row); got != tc.want {
			t.Errorf("%s: UpdateContinuation = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Only a fresh start of a Failed row serving nothing, on a strategy that
// recreates in place, skips the coordination gate.
func TestRecreateOfDarkFailedRow_OnlyANonSurgeFreshStartServingNothing(t *testing.T) {
	dark := &InstanceStatus{Phase: InstancePhaseFailed, ServingPodCount: 0}
	if !RecreateOfDarkFailedRow(dark, UpdateStrategyRecreatePod) {
		t.Error("a dark Failed row recreated in place skips the gate")
	}
	if RecreateOfDarkFailedRow(dark, UpdateStrategySurgeThenDrain) {
		t.Error("a surge strategy keeps the consult: its gate counts surge pods")
	}
	serving := &InstanceStatus{Phase: InstancePhaseFailed, ServingPodCount: 1}
	if RecreateOfDarkFailedRow(serving, UpdateStrategyRecreatePod) {
		t.Error("a row still serving takes capacity offline and is gated")
	}
	continuation := &InstanceStatus{Phase: InstancePhaseFailed, Operation: &InstanceOperation{Type: InstanceOperationUpdate}}
	if RecreateOfDarkFailedRow(continuation, UpdateStrategyRecreatePod) {
		t.Error("a continuation is not a fresh start")
	}
	if RecreateOfDarkFailedRow(nil, UpdateStrategyRecreatePod) {
		t.Error("no row, no start")
	}
}
