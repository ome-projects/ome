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
	mutate := StampFailedOnStuckPod(termination)

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
	if !StampFailedOnStuckPod(nil)(&untouched) || untouched.LastFailure != settled.LastFailure {
		t.Fatal("a nil termination leaves LastFailure alone")
	}
}
