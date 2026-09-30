package rolloutrun

import (
	"context"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
)

// rejectedISVC is a canary unit whose ladder rejected the new revision, with
// the primary reporting the given phase and no run pinned: the state a
// status loss leaves behind during or after the revert.
func rejectedISVC(t *testing.T, phase v1beta1.RolloutPhase) *v1beta1.InferenceService {
	t.Helper()
	isvc := isvcFixture(v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 100),
	})
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			RolloutPhase: phase,
			Canary: &v1beta1.CanaryStatus{
				CanaryRevisionHash:     hashOf(t, newRev),
				RolledBackRevisionHash: hashOf(t, newRev),
				StableRevisionHash:     hashOf(t, oldRev),
			},
		},
	}
	return isvc
}

// passOver runs one run-layer pass against the IR snapshot and carries the
// resulting status forward on isvc, the way the controller's flush does.
func passOver(t *testing.T, isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) Outcome {
	t.Helper()
	local := isvc.DeepCopy()
	local.ObjectMeta.ResourceVersion = ""
	out, err := Reconcile(context.Background(), testInputs(t, local, ir))
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	isvc.Status = local.Status
	return out
}

// A run lost while a canary unit is rolling back is adopted in place. The
// revert is an update the plan gate admits only under a pinned run, and the
// rejected target opens no run of its own, so without adoption the rejected
// pods would never drain. The adopted run pins the rejected revision, which
// is what lets it close RolledBack once the revert completes; the settled
// hold then opens nothing.
func TestRunLostWhileRollingBackIsAdopted(t *testing.T) {
	isvc := rejectedISVC(t, v1beta1.RolloutPhaseRollingBack)

	// The loss finds one Instance still on the rejected revision; the IR
	// names that revision as its spec target for the whole revert.
	draining := irPublished(t, oldRev, newRev, []workloadtypes.InstanceStatus{
		rowOn(0, oldRev), rowOn(1, newRev),
	})
	out := passOver(t, isvc, draining)
	if !out.Opened || !out.Adopted {
		t.Fatalf("revert in flight with no run: got %+v, want an adopted open", out)
	}
	active := isvc.Status.Rollout.ActiveRun
	if active == nil {
		t.Fatal("no run pinned after the adopted open")
	}
	if engine := pinnedTargetFor(t, active, v1beta1.EngineComponent); engine.Revision != hashOf(t, newRev) || engine.StableRevision != hashOf(t, oldRev) {
		t.Fatalf("adopted pin = %+v, want the rejected revision %s over stable %s", engine, hashOf(t, newRev), hashOf(t, oldRev))
	}
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs.RolledBackRevisionHash != hashOf(t, newRev) || cs.CurrentStep != 0 {
		t.Fatalf("adoption touched the canary state: %+v", cs)
	}

	// The straggler rolls back under the adopted run, which stays pinned.
	reverting := irPublished(t, oldRev, newRev, []workloadtypes.InstanceStatus{
		rowOn(0, oldRev), rowRollingTo(1, newRev, oldRev),
	})
	if out := passOver(t, isvc, reverting); out.StateChanged || isvc.Status.Rollout.ActiveRun == nil || isvc.Status.Rollout.ActiveRun.RunID != active.RunID {
		t.Fatalf("the adopted run did not stay pinned through the revert: outcome %+v, rollout %+v", out, isvc.Status.Rollout)
	}

	// The rejected pods are gone: the executor reports RolledBack and the
	// run closes on it.
	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseRolledBack
	isvc.Status.Components[v1beta1.EngineComponent] = engine
	settled := irPublished(t, oldRev, newRev, []workloadtypes.InstanceStatus{
		rowOn(0, oldRev), rowOn(1, oldRev),
	})
	passOver(t, isvc, settled)
	if isvc.Status.Rollout.ActiveRun != nil {
		t.Fatalf("run stayed open past the completed revert: %+v", isvc.Status.Rollout.ActiveRun)
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunRolledBack {
		t.Fatalf("lastRun = %+v, want RolledBack", last)
	}

	// The settled hold is terminal: nothing reopens toward the rejected
	// revision the IR still names as its target.
	for i := 0; i < 3; i++ {
		if out := passOver(t, isvc, settled); out.Opened || isvc.Status.Rollout.ActiveRun != nil {
			t.Fatalf("pass %d opened a run for a settled rollback: %+v", i, out)
		}
	}
}

// A settled rollback with no run and no closed-run record on file is the
// terminal hold, not a revert to adopt: every Instance rests on stable, so
// there is no update left for a run to admit.
func TestSettledRollbackWithoutRunOpensNothing(t *testing.T) {
	isvc := rejectedISVC(t, v1beta1.RolloutPhaseRolledBack)
	settled := irPublished(t, oldRev, newRev, []workloadtypes.InstanceStatus{
		rowOn(0, oldRev), rowOn(1, oldRev),
	})
	for i := 0; i < 3; i++ {
		if out := passOver(t, isvc, settled); out.Opened || v1beta1.RolloutRunActive(isvc) {
			t.Fatalf("pass %d opened a run for a settled rollback: %+v", i, out)
		}
	}
	if cond := planReady(isvc); cond == nil || cond.Reason != v1beta1.RolloutPlanReasonNoRun {
		t.Fatalf("plan condition = %+v, want NoRun", cond)
	}
}
