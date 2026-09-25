package status

import (
	"context"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The claim opens a fresh attempt once, keeps a committed attempt's
// identity across retries, and pins an unpinned one to the target it
// now knows.
func TestCreatingMutation_ClaimsOnceAndPinsAnUnpinnedAttempt(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	row := types.InstanceStatus{Index: 2}
	if !CreatingMutation(2, 1, time.Hour, "", now).Mutate(&row) {
		t.Fatal("a fresh slot must be claimed")
	}
	if row.Phase != types.InstancePhaseCreating || row.Incarnation != 1 ||
		row.Operation == nil || row.Operation.Step != CreateStepCreatePods || row.Operation.TargetRevision != "" {
		t.Fatalf("row = %+v, want Creating at incarnation 1 with an unpinned Create attempt", row)
	}
	id := row.Operation.ID
	if CreatingMutation(2, 5, time.Hour, "", now).Mutate(&row) || row.Incarnation != 1 {
		t.Fatal("a committed attempt must not be re-claimed")
	}
	if !CreatingMutation(2, 5, time.Hour, "rev-a", now).Mutate(&row) || row.Operation.TargetRevision != "rev-a" || row.Operation.ID != id {
		t.Fatalf("row = %+v, want the same attempt pinned to rev-a", row)
	}
	if CreatingMutation(2, 5, time.Hour, "rev-b", now).Mutate(&row) || row.Operation.TargetRevision != "rev-a" {
		t.Fatal("a pinned attempt keeps its pin")
	}
}

// The plain promote enters Ready once, clears the attempt and leaves the
// running revision alone.
func TestStampReady_ClearsTheAttemptAndKeepsTheRevision(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	w := newRowWriter(types.InstanceStatus{
		Index: 0, Phase: types.InstancePhaseCreating, RunningRevision: "rev-old",
		Operation: &types.InstanceOperation{ID: "create-0", Type: types.InstanceOperationCreate},
	})
	if err := StampReady(context.Background(), w.input(now), 0); err != nil {
		t.Fatal(err)
	}
	row := w.rows[0]
	if row.Phase != types.InstancePhaseReady || row.Operation != nil || row.RunningRevision != "rev-old" ||
		row.ReadySince == nil || !row.ReadySince.Time.Equal(now) {
		t.Fatalf("row = %+v, want Ready since now on rev-old with no attempt", row)
	}
	writes := w.writes
	if err := StampReady(context.Background(), w.input(now.Add(time.Minute)), 0); err != nil {
		t.Fatal(err)
	}
	if w.writes != writes || !w.rows[0].ReadySince.Time.Equal(now) {
		t.Fatal("a Ready row with no attempt must not be re-stamped")
	}
}

// The replacement lands only on the attempt it retires; a row whose
// attempt has moved on since the pass read it writes nothing.
func TestCreateReplacementMutation_RequiresTheRetiredAttemptsIdentity(t *testing.T) {
	expected := types.InstanceStatus{
		Index: 1, Incarnation: 3, Phase: types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{ID: "create-1-old", Type: types.InstanceOperationCreate, TargetRevision: "rev-old"},
	}
	replacement := types.InstanceOperation{ID: "create-1-new", Type: types.InstanceOperationCreate, TargetRevision: "rev-new"}
	mutation := CreateReplacementMutation(&expected, replacement)

	drifted := []struct {
		name   string
		mutate func(*types.InstanceStatus)
	}{
		{name: "incarnation", mutate: func(s *types.InstanceStatus) { s.Incarnation++ }},
		{name: "phase", mutate: func(s *types.InstanceStatus) { s.Phase = types.InstancePhaseFailed }},
		{name: "operation missing", mutate: func(s *types.InstanceStatus) { s.Operation = nil }},
		{name: "operation type", mutate: func(s *types.InstanceStatus) { s.Operation.Type = types.InstanceOperationRestart }},
		{name: "operation ID", mutate: func(s *types.InstanceStatus) { s.Operation.ID = "create-1-other" }},
		{name: "pinned revision", mutate: func(s *types.InstanceStatus) { s.Operation.TargetRevision = "rev-other" }},
	}
	for _, test := range drifted {
		t.Run(test.name, func(t *testing.T) {
			current := Clone(expected)
			test.mutate(&current)
			if mutation.Mutate(&current) {
				t.Fatal("a drifted row must not take the replacement")
			}
		})
	}

	current := Clone(expected)
	if !mutation.Mutate(&current) || current.Operation.ID != "create-1-new" || current.Operation.TargetRevision != "rev-new" {
		t.Fatalf("row = %+v, want the replacement attempt", current)
	}
	if mutation.Mutate(&current) {
		t.Fatal("a landed replacement must not be written twice")
	}
}

// The rollback owns exactly the lifecycle state the claim wrote: a row
// whose lifecycle moved since the commit is left alone, publication-only
// pod observations that advanced are carried into the restored row, and
// a claim that created the row removes it under the same precondition.
func TestCreateRollbackMutation_RestoresOnlyTheCommittedTransition(t *testing.T) {
	previous := types.InstanceStatus{
		Index: 4, Incarnation: 3, Phase: types.InstancePhaseFailed,
		RunningRevision: "revision-a", PodCount: 2, ServingPodCount: 1,
		AvailablePodCount: 1, Admitted: true, ActiveOrdinal: 1,
	}
	committed := previous
	committed.Phase = types.InstancePhaseCreating
	committed.Operation = &types.InstanceOperation{ID: "create-4", Type: types.InstanceOperationCreate}

	if _, ok := CreateRollbackMutation(4, &previous, nil); ok {
		t.Fatal("nothing committed, nothing to roll back")
	}

	mutation, ok := CreateRollbackMutation(4, &previous, &committed)
	if !ok || mutation.Mutate == nil || mutation.Remove {
		t.Fatalf("rollback mutation = %+v, want a restoring mutation", mutation)
	}
	moved := committed
	moved.Operation = &types.InstanceOperation{ID: "create-4-next", Type: types.InstanceOperationCreate}
	if mutation.Mutate(&moved) {
		t.Fatal("a row whose attempt moved on must not be rolled back")
	}
	current := committed
	current.ReadyPodCount = 2
	current.ScheduledPodCount = 2
	current.NodesOccupied = []string{"node-a", "node-b"}
	if !mutation.Mutate(&current) {
		t.Fatal("publication-only changes must not block the rollback")
	}
	want := previous
	want.ReadyPodCount = 2
	want.ScheduledPodCount = 2
	want.NodesOccupied = []string{"node-a", "node-b"}
	if !reflect.DeepEqual(current, want) {
		t.Fatalf("restored status:\n got: %+v\nwant: %+v", current, want)
	}

	removal, ok := CreateRollbackMutation(4, nil, &committed)
	if !ok || !removal.Remove || removal.Precondition == nil {
		t.Fatalf("rollback mutation = %+v, want a conditional removal", removal)
	}
	if !removal.Precondition(&committed) || removal.Precondition(&moved) {
		t.Fatal("the removal must own exactly the committed row")
	}
}
