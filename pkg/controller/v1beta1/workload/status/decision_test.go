package status

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestStampStep_GuardedAndWireStable(t *testing.T) {
	now := time.Date(2026, time.August, 15, 12, 0, 0, 123456789, time.UTC)
	expected := terminalStatusFixture(3)
	persisted := cloneTerminalStatus(expected)
	persisted.Operation.StartedAt = metav1.NewTime(persisted.Operation.StartedAt.Truncate(time.Second))
	persisted.Operation.LastProgressAt = metav1.NewTime(persisted.Operation.LastProgressAt.Truncate(time.Second))
	store := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{3: persisted}}
	input := types.ReconcileInput{
		OwnerObject:                          &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		Clock:                                clocktesting.NewFakeClock(now),
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}

	claimed, err := StampStep(context.Background(), input, &expected, types.UpdateStepSurgeDrain, true)
	if err != nil || !claimed {
		t.Fatalf("transition: claimed=%v err=%v", claimed, err)
	}
	if store.writes != 1 || store.statuses[3].Operation.Step != types.UpdateStepSurgeDrain {
		t.Fatalf("persisted transition: writes=%d row=%+v", store.writes, store.statuses[3])
	}
	if !store.statuses[3].Operation.LastProgressAt.Time.Equal(now) {
		t.Fatalf("LastProgressAt=%v want %v", store.statuses[3].Operation.LastProgressAt, now)
	}

	stale := terminalStatusFixture(3)
	stale.Operation.ID = "stale-attempt"
	claimed, err = StampStep(context.Background(), input, &stale, types.UpdateStepSurgeDrainSettle, true)
	if err != nil || claimed || store.writes != 1 {
		t.Fatalf("stale transition: claimed=%v err=%v writes=%d", claimed, err, store.writes)
	}
}

// TestDemoteUnbacked_KeepsARowAnOperationClaimed pins the truth-pass write: Ready with no Operation
// demotes to Pending preserving everything else, and a row an operation
// claimed since selection is left untouched by the fresh-row guard.
func TestDemoteUnbacked_KeepsARowAnOperationClaimed(t *testing.T) {
	rows := map[int32]*types.InstanceStatus{
		0: {Index: 0, Incarnation: 3, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"},
		1: {Index: 1, Incarnation: 2, Phase: types.InstancePhaseReady,
			Operation: &types.InstanceOperation{ID: "u-1", Type: types.InstanceOperationUpdate}},
	}
	input := types.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			mutate(rows[idx])
			return nil
		},
	}

	demoted, err := DemoteUnbacked(context.Background(), input, 0)
	if err != nil || !demoted {
		t.Fatalf("unbacked row: demoted=%v err=%v", demoted, err)
	}
	if rows[0].Phase != types.InstancePhasePending {
		t.Errorf("phase = %s, want Pending", rows[0].Phase)
	}
	if rows[0].Incarnation != 3 || rows[0].RunningRevision != "rev-a" {
		t.Errorf("the demotion rewrote more than the phase: %+v", rows[0])
	}

	demoted, err = DemoteUnbacked(context.Background(), input, 1)
	if err != nil || demoted {
		t.Fatalf("claimed row: demoted=%v err=%v", demoted, err)
	}
	if rows[1].Phase != types.InstancePhaseReady {
		t.Errorf("phase = %s, want the claiming operation's row untouched", rows[1].Phase)
	}
}

// The clearing stamp ends the attempt and records its cause; a slot the
// writer had to append is never resurrected, and an already-disposed row
// is left alone.
func TestStampFailed_NeverResurrectsAnEmptySlot(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	termination := &types.InstanceTermination{Reason: "OOMKilled", Time: metav1.NewTime(now)}

	w := newRowWriter(openRow(0, types.InstancePhaseUpdating))
	if err := StampFailed(context.Background(), w.input(now), 0, termination); err != nil {
		t.Fatal(err)
	}
	got := w.rows[0]
	if got.Phase != types.InstancePhaseFailed || got.Operation != nil || got.LastFailure == nil {
		t.Fatalf("row = %+v, want Failed with no operation and a record", got)
	}
	if err := StampFailed(context.Background(), w.input(now), 0, termination); err != nil {
		t.Fatal(err)
	}
	if w.writes != 1 {
		t.Fatalf("writes = %d, want the second call to be a no-op", w.writes)
	}

	empty := newRowWriter()
	if err := StampFailed(context.Background(), empty.input(now), 9, termination); err != nil {
		t.Fatal(err)
	}
	if empty.writes != 0 {
		t.Fatal("a slot with no phase must not be resurrected")
	}
}

// The deadline backstop keeps the Operation, because an operator reading
// the row wants to see what was in flight when the clock ran out.
func TestStampFailedKeepingOperation_KeepsTheOperationOnTheRow(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	termination := &types.InstanceTermination{Reason: "DeadlineExceeded", Time: now}
	mutate := StampFailedKeepingOperation(termination)

	row := openRow(0, types.InstancePhaseUpdating)
	if !mutate(&row) || row.Phase != types.InstancePhaseFailed || row.Operation == nil {
		t.Fatalf("row = %+v, want Failed with the operation preserved", row)
	}
	if mutate(&row) {
		t.Fatal("an already-Failed row is a no-op")
	}
	concluded := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating}
	if mutate(&concluded) {
		t.Fatal("a row whose attempt already ended has nothing left to expire")
	}
	appended := types.InstanceStatus{Index: 0}
	if mutate(&appended) {
		t.Fatal("a slot with no phase must not be resurrected")
	}
}

// The stuck-pod stamp needs no operation: the wedged-pod recovery shape
// under a settled row has none, and the row still fails with the
// diagnostics on it.
func TestStampFailedOnStuckPod_NeedsNoOperation(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	termination := &types.InstanceTermination{PodName: "engine-0-default-0", Reason: "CrashLoopBackOff", Time: now}
	mutate := StampFailedOnStuckPod(termination, 0)

	settled := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	if !mutate(&settled) || settled.Phase != types.InstancePhaseFailed || settled.LastFailure == nil {
		t.Fatalf("row = %+v, want Failed with the record and no operation required", settled)
	}
	inFlight := openRow(0, types.InstancePhaseUpdating)
	if !mutate(&inFlight) || inFlight.Operation == nil {
		t.Fatalf("row = %+v, want the operation in flight preserved", inFlight)
	}
	if mutate(&inFlight) {
		t.Fatal("an already-Failed row is a no-op")
	}
	appended := types.InstanceStatus{Index: 0}
	if mutate(&appended) {
		t.Fatal("a slot with no phase must not be resurrected")
	}
	untouched := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady, LastFailure: settled.LastFailure}
	if !StampFailedOnStuckPod(nil, 0)(&untouched) || untouched.LastFailure != settled.LastFailure {
		t.Fatal("a nil termination leaves LastFailure alone")
	}
}

// A Restart stamped over a Failed row that already carries a Restart is
// that repair's next attempt, so the new operation continues the spent
// one's retry count; a Restart opened on any other row starts at zero.
func TestStampRestarting_ContinuesASpentRepairsRetryCount(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	rows := map[int32]*types.InstanceStatus{
		0: {Index: 0, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a",
			Operation: &types.InstanceOperation{ID: "restart-0-1", Type: types.InstanceOperationRestart, Step: types.RestartStepDrain, RetryCount: 1}},
		1: {Index: 1, Incarnation: 2, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"},
		2: {Index: 2, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a",
			Operation: &types.InstanceOperation{ID: "update-2-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, RetryCount: 1}},
	}
	input := types.ReconcileInput{
		Clock: clocktesting.NewFakeClock(now),
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			mutate(rows[idx])
			return nil
		},
	}
	for idx, want := range map[int32]int32{0: 2, 1: 0, 2: 0} {
		inc, err := StampRestarting(context.Background(), input, idx, "again", time.Hour, nil)
		if err != nil {
			t.Fatalf("row %d: %v", idx, err)
		}
		got := rows[idx]
		if inc != 3 || got.Incarnation != 3 || got.Phase != types.InstancePhaseRestarting || got.Operation == nil {
			t.Fatalf("row %d = %+v, want Restarting at incarnation 3", idx, got)
		}
		if got.Operation.RetryCount != want {
			t.Errorf("row %d: RetryCount = %d, want %d", idx, got.Operation.RetryCount, want)
		}
	}
}

// TestStampRestartRevision_OnlyAnOpenRepairRecordsIt: the stamp moves the
// running revision of a Restarting row and leaves every other row as it
// is, so a repair that ended, or a row another pass owns, never has its
// revision moved under it; recording the revision the row already carries
// writes nothing.
func TestStampRestartRevision_OnlyAnOpenRepairRecordsIt(t *testing.T) {
	openRepair := func() *types.InstanceOperation {
		return &types.InstanceOperation{ID: "restart-0-1", Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}
	}
	rows := map[int32]*types.InstanceStatus{
		0: {Index: 0, Incarnation: 2, Phase: types.InstancePhaseRestarting, RunningRevision: "rev-a", Operation: openRepair()},
		1: {Index: 1, Incarnation: 2, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"},
		2: {Index: 2, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", Operation: openRepair()},
		3: {Index: 3, Incarnation: 2, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-a", TargetRevision: "rev-b",
			Operation: &types.InstanceOperation{ID: "update-3-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-b"}},
	}
	writes := 0
	input := types.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			if mutate(rows[idx]) {
				writes++
			}
			return nil
		},
	}
	for idx, want := range map[int32]string{0: "rev-b", 1: "rev-a", 2: "rev-a", 3: "rev-a"} {
		if err := StampRestartRevision(context.Background(), input, idx, "rev-b"); err != nil {
			t.Fatalf("row %d: %v", idx, err)
		}
		if got := rows[idx].RunningRevision; got != want {
			t.Errorf("row %d: RunningRevision = %q, want %q", idx, got, want)
		}
	}
	if writes != 1 {
		t.Errorf("writes = %d, want 1: only the open repair's row is written", writes)
	}
	if err := StampRestartRevision(context.Background(), input, 0, "rev-b"); err != nil {
		t.Fatalf("second stamp: %v", err)
	}
	if writes != 1 {
		t.Errorf("writes = %d after stamping the revision the row already carries, want 1", writes)
	}
}

// The unpark write: a Failed, operation-free row returns to Ready with
// its anchor and record time kept and the note appended once; other rows stay.
func TestUnparkServing_ReturnsAParkToReadyKeepingItsAnchor(t *testing.T) {
	readySince := metav1.NewTime(time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC))
	crashedAt := metav1.NewTime(readySince.Add(10 * time.Minute))
	record := func() *types.InstanceTermination {
		return &types.InstanceTermination{PodName: "p-0", ContainerName: "runner", Reason: "Error", Message: "back-off", Time: crashedAt}
	}
	rows := map[int32]*types.InstanceStatus{
		0: {Index: 0, Incarnation: 3, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", ReadySince: &readySince, LastFailure: record()},
		1: {Index: 1, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", ReadySince: &readySince, LastFailure: record(),
			Operation: &types.InstanceOperation{ID: "r-1", Type: types.InstanceOperationRestart}},
		2: {Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince, LastFailure: record()},
		3: {Index: 3, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", ReadySince: &readySince},
		4: {Index: 4, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", ReadySince: &readySince, LastFailure: record()},
	}
	writes := 0
	input := types.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			if mutate(rows[idx]) {
				writes++
			}
			return nil
		},
	}
	const note = "crash loop on a held revision"
	for idx := int32(0); idx < 4; idx++ {
		if err := UnparkServing(context.Background(), input, idx, note); err != nil {
			t.Fatalf("UnparkServing(%d): %v", idx, err)
		}
	}
	if writes != 1 {
		t.Fatalf("writes = %d, want the one park", writes)
	}
	got := rows[0]
	if got.Phase != types.InstancePhaseReady || got.Operation != nil || got.Incarnation != 3 || got.RunningRevision != "rev-a" {
		t.Fatalf("park after unpark = %+v, want Ready with everything else kept", got)
	}
	if got.ReadySince == nil || !got.ReadySince.Equal(&readySince) {
		t.Fatalf("ReadySince = %v, want the promotion %v kept", got.ReadySince, readySince)
	}
	if got.LastFailure == nil || !got.LastFailure.Time.Equal(&crashedAt) || got.LastFailure.Message != "back-off; "+note {
		t.Fatalf("record = %+v, want its time kept and the note appended", got.LastFailure)
	}
	if rows[1].Phase != types.InstancePhaseFailed || rows[2].Phase != types.InstancePhaseReady || rows[3].Phase != types.InstancePhaseFailed {
		t.Fatalf("rows an operation claims, already Ready, or without a record must be untouched: %+v %+v %+v", rows[1], rows[2], rows[3])
	}
	if err := UnparkServing(context.Background(), input, 4, ""); err != nil {
		t.Fatalf("UnparkServing(4): %v", err)
	}
	if rows[4].Phase != types.InstancePhaseReady || rows[4].LastFailure.Message != "back-off" {
		t.Fatalf("an unpark with no note = %+v, want Ready with the message as it was", rows[4])
	}
}

// The stuck-pod stamp is decided on the pass's observation and applied
// to the fresh row, and only a Restart bumps a row's incarnation: a
// blamed pod below the fresh row's incarnation is the set a repair
// claimed since the observation, and the stamp is withheld. A pod at the
// row's incarnation, a pod with no label, and a row that records no
// incarnation are stamped.
func TestStampFailedOnStuckPod_WithholdsFromAPodTheRepairReplaced(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	termination := &types.InstanceTermination{PodName: "engine-0-default-0", Reason: "CrashLoopBackOff", Time: now}
	claimed := func() types.InstanceStatus {
		return types.InstanceStatus{Index: 0, Incarnation: 2, Phase: types.InstancePhaseRestarting,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}}
	}
	row := claimed()
	if StampFailedOnStuckPod(termination, 1)(&row) || row.Phase != types.InstancePhaseRestarting || row.LastFailure != nil {
		t.Fatalf("row = %+v, want the repair's claim kept over a pod of the incarnation it replaces", row)
	}
	row = claimed()
	if !StampFailedOnStuckPod(termination, 2)(&row) || row.Phase != types.InstancePhaseFailed || row.Operation == nil {
		t.Fatalf("row = %+v, want Failed with the operation preserved for a pod of the rebuild", row)
	}
	row = claimed()
	if !StampFailedOnStuckPod(termination, 0)(&row) || row.Phase != types.InstancePhaseFailed {
		t.Fatalf("row = %+v, want Failed for a pod that carries no incarnation", row)
	}
	unrecorded := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	if !StampFailedOnStuckPod(termination, 1)(&unrecorded) || unrecorded.Phase != types.InstancePhaseFailed {
		t.Fatalf("row = %+v, want Failed for a row that records no incarnation", unrecorded)
	}
}

// ApplyStamp runs the stamp through the single-row seam and reports
// whether it changed the row; the seam's own answer is not visible to
// the caller otherwise.
func TestApplyStamp_ReportsWhetherTheStampTook(t *testing.T) {
	rows := map[int32]*types.InstanceStatus{0: {Index: 0, Phase: types.InstancePhaseReady}}
	writes := 0
	input := types.ReconcileInput{MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
		if mutate(rows[idx]) {
			writes++
		}
		return nil
	}}
	stamp := StampFailedOnStuckPod(&types.InstanceTermination{Reason: "CrashLoopBackOff"}, 0)
	committed, err := ApplyStamp(context.Background(), input, 0, stamp)
	if err != nil || !committed || writes != 1 {
		t.Fatalf("first apply: committed=%v err=%v writes=%d, want the stamp to take once", committed, err, writes)
	}
	committed, err = ApplyStamp(context.Background(), input, 0, stamp)
	if err != nil || committed || writes != 1 {
		t.Fatalf("second apply: committed=%v err=%v writes=%d, want a no-op on the Failed row", committed, err, writes)
	}
}
