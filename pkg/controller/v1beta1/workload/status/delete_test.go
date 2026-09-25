package status

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// deleteWave is a two-row table, both rows Ready, with the input a wave
// write needs: the owner it fences on, the table the pass planned from,
// a clock, and the atomic seam over store.
func deleteWave(store *terminalMutationStore) ([]types.InstanceStatus, types.ReconcileInput) {
	rows := []types.InstanceStatus{
		{Index: 0, Incarnation: 2, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"},
		{Index: 1, Incarnation: 3, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"},
	}
	store.statuses = map[int32]types.InstanceStatus{}
	for _, row := range rows {
		store.statuses[row.Index] = cloneTerminalStatus(row)
	}
	input := types.ReconcileInput{
		OwnerObject:                          &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		Key:                                  types.Key{Namespace: "prod", OwnerName: "model", Component: types.ComponentEngine},
		ObservedState:                        types.WorkloadObservedState{InstanceStatuses: rows},
		Clock:                                clocktesting.NewFakeClock(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)),
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}
	return rows, input
}

// The admission commits every selected row together, fenced on the table
// the pass planned from; a table that moved since admits nothing.
func TestStampDeletingBatch_AdmitsTheWaveOnlyAgainstThePlannedTable(t *testing.T) {
	store := &terminalMutationStore{ownerUID: "owner-a"}
	rows, input := deleteWave(store)

	committed, err := StampDeletingBatch(context.Background(), input, time.Hour, rows[1:])
	if err != nil || !committed || store.writes != 1 {
		t.Fatalf("admit = %v, %v (writes=%d), want the wave committed once", committed, err, store.writes)
	}
	if row := store.statuses[1]; row.Phase != types.InstancePhaseDeleting || row.Operation == nil ||
		row.Operation.Type != types.InstanceOperationDelete || row.Operation.Step != "Drain" ||
		!row.Operation.Deadline.Time.Equal(input.Now().Add(time.Hour)) {
		t.Fatalf("row = %+v, want Deleting under a Delete/Drain operation with the deadline", row)
	}
	if row := store.statuses[0]; row.Phase != types.InstancePhaseReady {
		t.Fatalf("row 0 = %+v, want untouched", row)
	}

	moved := &terminalMutationStore{ownerUID: "owner-a"}
	rows, input = deleteWave(moved)
	changed := moved.statuses[0]
	changed.Incarnation++
	moved.statuses[0] = changed
	committed, err = StampDeletingBatch(context.Background(), input, time.Hour, rows[1:])
	if !errors.Is(err, types.ErrStatusMutationPrecondition) || committed || moved.writes != 0 {
		t.Fatalf("admit against a moved table = %v, %v (writes=%d), want the precondition refused", committed, err, moved.writes)
	}

	grown := &terminalMutationStore{ownerUID: "owner-a"}
	rows, input = deleteWave(grown)
	grown.statuses[2] = types.InstanceStatus{Index: 2, Phase: types.InstancePhaseCreating}
	committed, err = StampDeletingBatch(context.Background(), input, time.Hour, rows[1:])
	if !errors.Is(err, types.ErrStatusMutationPrecondition) || committed || grown.writes != 0 {
		t.Fatalf("admit against a grown table = %v, %v (writes=%d), want the precondition refused", committed, err, grown.writes)
	}
}

// The completion removes every row of the wave together, fenced on each
// row still carrying the wave's own operation; a row that left Deleting
// since keeps the whole wave in place.
func TestRemoveDeletedBatch_RemovesTheWaveOnlyWhileItOwnsEveryRow(t *testing.T) {
	store := &terminalMutationStore{ownerUID: "owner-a"}
	rows, input := deleteWave(store)
	if _, err := StampDeletingBatch(context.Background(), input, time.Hour, rows); err != nil {
		t.Fatal(err)
	}
	admitted := []types.InstanceStatus{store.statuses[0], store.statuses[1]}
	expectations := types.NewExpectations()
	expectations.ExpectDeletes("prod", "model", types.ComponentEngine, 0, 1)
	deps := types.Deps{Expectations: expectations}

	drifted := cloneTerminalStatus(admitted[1])
	drifted.Phase = types.InstancePhaseFailed
	store.statuses[1] = drifted
	removed, err := RemoveDeletedBatch(context.Background(), deps, input, admitted)
	if !errors.Is(err, types.ErrStatusMutationPrecondition) || removed || len(store.statuses) != 2 {
		t.Fatalf("remove with drift = %v, %v (rows=%d), want the precondition refused and both rows kept", removed, err, len(store.statuses))
	}

	store.statuses[1] = cloneTerminalStatus(admitted[1])
	removed, err = RemoveDeletedBatch(context.Background(), deps, input, admitted)
	if err != nil || !removed || len(store.statuses) != 0 {
		t.Fatalf("remove = %v, %v (rows=%d), want the wave removed", removed, err, len(store.statuses))
	}
	if !expectations.Satisfied("prod", "model", types.ComponentEngine, 0) {
		t.Fatal("removal must forget the row's delete expectations")
	}
}

// A wave is gang-atomic: an adapter that confirmed only part of it is an
// error, not a partial success.
func TestRemoveDeletedBatch_PartialConfirmationIsAnError(t *testing.T) {
	store := &terminalMutationStore{ownerUID: "owner-a"}
	rows, input := deleteWave(store)
	if _, err := StampDeletingBatch(context.Background(), input, time.Hour, rows); err != nil {
		t.Fatal(err)
	}
	admitted := []types.InstanceStatus{store.statuses[0], store.statuses[1]}
	delete(store.statuses, 1)
	// The guard reads the row as absent, so the batch is refused before any
	// partial commit can happen.
	removed, err := RemoveDeletedBatch(context.Background(), types.Deps{Expectations: types.NewExpectations()}, input, admitted)
	if !errors.Is(err, types.ErrStatusMutationPrecondition) || removed {
		t.Fatalf("remove with a row gone = %v, %v, want refused", removed, err)
	}
	if !SameDeleteOperation(admitted[0].Operation, admitted[0].Operation) || SameDeleteOperation(admitted[0].Operation, admitted[1].Operation) {
		t.Fatal("a wave's identity is its operation id")
	}
}
