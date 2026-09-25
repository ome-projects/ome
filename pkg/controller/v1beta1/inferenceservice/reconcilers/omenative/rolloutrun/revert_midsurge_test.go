package rolloutrun

import (
	"context"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// irPublished builds the IR snapshot the run reconciler reads by running
// the same publication the InferenceReplica controller runs, so the
// counters under test are the published ones rather than hand-written
// numbers.
func irPublished(t *testing.T, current, target string, rows []workloadtypes.InstanceStatus) *v1beta1.InferenceReplica {
	t.Helper()
	observation, err := workload.NewOwnedPublicationObservation(
		append([]workloadtypes.InstanceStatus(nil), rows...),
		workload.NewCachedSelectorPodObservation(nil, nil),
		nil,
		workloadstatus.AvailabilityWindow{},
	)
	if err != nil {
		t.Fatalf("NewOwnedPublicationObservation: %v", err)
	}
	desired := map[int32]int32{}
	for _, row := range rows {
		desired[row.Index] = 1
	}
	_, counters, err := observation.TakeInlineV1Publication(desired, target)
	if err != nil {
		t.Fatalf("TakeInlineV1Publication: %v", err)
	}
	ir := irFixture(current, target)
	ir.Status.Replicas = counters.Replicas
	ir.Status.UpdatedReplicas = counters.UpdatedReplicas
	return ir
}

func rowOn(index int32, running string) workloadtypes.InstanceStatus {
	return workloadtypes.InstanceStatus{Index: index, RunningRevision: running}
}

func rowRollingTo(index int32, running, pinned string) workloadtypes.InstanceStatus {
	return workloadtypes.InstanceStatus{
		Index:           index,
		RunningRevision: running,
		Operation:       &workloadtypes.InstanceOperation{Type: workloadtypes.InstanceOperationUpdate, TargetRevision: pinned},
	}
}

// A revert issued while one Instance is mid-surge must not read as
// converged. The surging row still runs the revision the revert restores,
// but it owes a roll: its runtime-ready replacement promotes onto the
// pinned revision first and only then rolls back. Closing the run on that
// false convergence hands the promote-then-roll-back sequence to a second
// run, which is the churn this pins against.
func TestRevertMidSurgeKeepsOneRunOpen(t *testing.T) {
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		BlueGreen:  &v1beta1.GroupBlueGreen{},
	})

	pass := func(t *testing.T, ir *v1beta1.InferenceReplica) {
		t.Helper()
		local := isvc.DeepCopy()
		local.ObjectMeta.ResourceVersion = ""
		if _, err := Reconcile(context.Background(), testInputs(t, local, ir)); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		isvc.Status = local.Status
	}
	active := func() *v1beta1.RolloutRun {
		if isvc.Status.Rollout == nil {
			return nil
		}
		return isvc.Status.Rollout.ActiveRun
	}
	runID := func(t *testing.T, step string) string {
		t.Helper()
		if active() == nil {
			t.Fatalf("%s: no run open", step)
		}
		return active().RunID
	}

	// Converged on the old revision.
	pass(t, irPublished(t, oldRev, oldRev, []workloadtypes.InstanceStatus{
		rowOn(0, oldRev), rowOn(1, oldRev), rowOn(2, oldRev),
	}))
	if active() != nil {
		t.Fatalf("converged workload opened a run: %+v", active())
	}

	// Roll toward the new revision; index 1 surges first.
	pass(t, irPublished(t, oldRev, newRev, []workloadtypes.InstanceStatus{
		rowOn(0, oldRev), rowRollingTo(1, oldRev, newRev), rowOn(2, oldRev),
	}))
	rolling := runID(t, "roll to the new revision")

	// Revert while index 1 is still surging. Every row runs the restored
	// revision, so only the pin distinguishes this from convergence.
	pass(t, irPublished(t, oldRev, oldRev, []workloadtypes.InstanceStatus{
		rowOn(0, oldRev), rowRollingTo(1, oldRev, newRev), rowOn(2, oldRev),
	}))
	reverting := runID(t, "revert mid-surge")

	// The surge promotes its replacement onto the pinned revision, then
	// rolls that replacement back. Both steps belong to the run the
	// revert is already running.
	pass(t, irPublished(t, oldRev, oldRev, []workloadtypes.InstanceStatus{
		rowOn(0, oldRev), rowOn(1, newRev), rowOn(2, oldRev),
	}))
	if got := runID(t, "surge promoted"); got != reverting {
		t.Fatalf("promote opened a second run: got %s want %s (superseded %s)", got, reverting, rolling)
	}
	pass(t, irPublished(t, oldRev, oldRev, []workloadtypes.InstanceStatus{
		rowOn(0, oldRev), rowRollingTo(1, newRev, oldRev), rowOn(2, oldRev),
	}))
	if got := runID(t, "rolling back"); got != reverting {
		t.Fatalf("rollback opened a second run: got %s want %s", got, reverting)
	}

	// Genuine convergence closes the run, and it stays closed.
	for i := 0; i < 5; i++ {
		pass(t, irPublished(t, oldRev, oldRev, []workloadtypes.InstanceStatus{
			rowOn(0, oldRev), rowOn(1, oldRev), rowOn(2, oldRev),
		}))
		if active() != nil {
			t.Fatalf("pass %d reopened a run on a converged workload: %+v", i, active())
		}
	}
}
