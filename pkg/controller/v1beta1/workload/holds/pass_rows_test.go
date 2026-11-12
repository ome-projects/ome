package holds

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestRowsForPlan_ReadsEachRowAgainstThePlan: the pass's rows follow the
// observed rows in order. Each reads the observed row in place; the plan's pod
// count for its index, or the observed count when the plan no longer
// covers the index; the plan's entry, or none; and whether the scale-down
// still holds the Instance out.
func TestRowsForPlan_ReadsEachRowAgainstThePlan(t *testing.T) {
	single := []types.RunnerPlan{{Name: types.RunnerDefault, Size: 1}}
	gang := []types.RunnerPlan{{Name: types.RunnerLeader, Size: 1}, {Name: types.RunnerWorker, Size: 3}}
	plan := types.ComponentPlan{Instances: []types.InstancePlan{{Index: 0, Runners: single}, {Index: 2, Runners: gang}}}
	statuses := []types.InstanceStatus{
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 4, ServingPodCount: 4},
		{Index: 5, Incarnation: 2, Phase: types.InstancePhaseDeleting, PodCount: 2, Operation: operation("delete-5", types.InstanceOperationDelete, "Drain", "")},
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseCreating, Operation: operation("create-0", types.InstanceOperationCreate, "CreatePods", "")},
		{Index: 7, Incarnation: 3, Phase: types.InstancePhaseReady, PodCount: 1},
	}
	want := []struct {
		desired  int32
		instance *types.InstancePlan
		excluded bool
	}{
		{desired: 4, instance: &plan.Instances[1]},
		{desired: 2, excluded: true},
		{desired: 1, instance: &plan.Instances[0]},
		{desired: 1},
	}
	rows := RowsForPlan(plan, statuses, map[int32]struct{}{5: {}})
	if len(rows) != len(want) {
		t.Fatalf("%d rows over %d observed rows", len(rows), len(statuses))
	}
	for i := range rows {
		if diff := cmp.Diff(statuses[i], *rows[i].Status); diff != "" {
			t.Errorf("row %d (-observed +row):\n%s", i, diff)
		}
		if rows[i].Status != &statuses[i] {
			t.Errorf("row %d is a copy of the observed row; the pass reads the observed row itself", i)
		}
		if rows[i].Desired != want[i].desired || rows[i].Instance != want[i].instance || rows[i].Excluded != want[i].excluded {
			t.Errorf("row %d reads %d desired pods, entry %p, excluded %t; want %d, %p, %t",
				i, rows[i].Desired, rows[i].Instance, rows[i].Excluded, want[i].desired, want[i].instance, want[i].excluded)
		}
	}

	t.Run("a plan with no entries reads every row at its observed count", func(t *testing.T) {
		for i, row := range RowsForPlan(types.ComponentPlan{}, statuses, nil) {
			if row.Desired != statuses[i].PodCount || row.Instance != nil || row.Excluded {
				t.Errorf("row %d reads %d desired pods, entry %p, excluded %t; want the observed %d, no entry, not excluded", i, row.Desired, row.Instance, row.Excluded, statuses[i].PodCount)
			}
		}
	})
	t.Run("no observed rows read as no rows", func(t *testing.T) {
		if rows := RowsForPlan(plan, nil, nil); len(rows) != 0 {
			t.Errorf("%d rows over no observed rows", len(rows))
		}
	})
}

// observedRows is a copy of the pass's observation, row for row.
func observedRows(in types.ReconcileInput) []types.InstanceStatus {
	out := make([]types.InstanceStatus, len(in.ObservedState.InstanceStatuses))
	for i, row := range in.ObservedState.InstanceStatuses {
		out[i] = copyRow(row)
	}
	return out
}

// TestRun_LeavesTheObservedRowsAsTheyWere: the hold pass writes through the
// row writer alone. The pass that records a pause on an attempt and the
// pass that releases it leave the observation they read as it was, while
// the store holds the writes.
func TestRun_LeavesTheObservedRowsAsTheyWere(t *testing.T) {
	store, in := newStore(
		types.InstanceStatus{Index: 0, Incarnation: 1, Phase: types.InstancePhaseUpdating,
			Operation: operation("update-0", types.InstanceOperationUpdate, "InPlace", "")},
		types.InstanceStatus{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady},
		types.InstanceStatus{Index: 2, Incarnation: 1, Phase: types.InstancePhaseCreating,
			Operation: operation("create-2", types.InstanceOperationCreate, "CreatePods", "")},
	)

	before := observedRows(in)
	apply(t, in, types.ComponentPlan{Paused: true}, nil)
	if got := store.waiting(0); got != types.WaitingReasonPaused {
		t.Fatalf("the paused attempt waits on %q, want %q", got, types.WaitingReasonPaused)
	}
	if diff := cmp.Diff(before, in.ObservedState.InstanceStatuses); diff != "" {
		t.Errorf("recording the pause changed the observation (-before +after):\n%s", diff)
	}

	in = store.input()
	before = observedRows(in)
	apply(t, in, types.ComponentPlan{}, nil)
	if got := store.waiting(0); got != "" {
		t.Fatalf("the released attempt still waits on %q", got)
	}
	if diff := cmp.Diff(before, in.ObservedState.InstanceStatuses); diff != "" {
		t.Errorf("releasing the pause changed the observation (-before +after):\n%s", diff)
	}
	if store.writes != 2 {
		t.Errorf("the store took %d writes, want 2: the record and the release", store.writes)
	}
}
