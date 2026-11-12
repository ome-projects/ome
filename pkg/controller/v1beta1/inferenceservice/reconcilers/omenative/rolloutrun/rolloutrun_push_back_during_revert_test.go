package rolloutrun

import (
	"context"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

// pushedBackDuringRevertISVC is eight engines under a four-step manual
// canary, rolled back at the second step with three engines on version 2,
// whose template the operator then set back to version 1 while the three
// still drained: the unit holds the rejection of version 2 and reads
// RollingBack, and the fresh run the push back opened records version 1 as
// the engine's target and as its stable revision.
func pushedBackDuringRevertISVC(t *testing.T) *v1beta1.InferenceService {
	t.Helper()
	group := v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
		Canary:     canaryBody(10, 30, 60, 100),
	}
	isvc := isvcFixture(group)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			RolloutPhase: v1beta1.RolloutPhaseRollingBack,
			Canary: &v1beta1.CanaryStatus{
				CurrentStep:            1,
				CanaryRevisionHash:     hashOf(t, newRev),
				RolledBackRevisionHash: hashOf(t, newRev),
				StableRevisionHash:     hashOf(t, oldRev),
			},
		},
	}
	digest, err := rolloutpolicy.ProgressionDigest(&group)
	if err != nil {
		t.Fatalf("ProgressionDigest: %v", err)
	}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{
		ActiveRun: &v1beta1.RolloutRun{
			RunID: "llm-a-run-back-to-version-1",
			TargetRevisions: []v1beta1.RolloutRunTarget{
				{Component: v1beta1.EngineComponent, Revision: hashOf(t, oldRev), StableRevision: hashOf(t, oldRev)},
			},
			Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{
				Source:         v1beta1.RolloutPlanSourceInline,
				PortableDigest: digest,
				Group:          *group.DeepCopy(),
			}}},
		},
	}
	return isvc
}

// engineRows is eight engine rows, the first onStable of them running
// version 1 and the rest still running version 2.
func engineRows(onStable int) []workloadtypes.InstanceStatus {
	rows := make([]workloadtypes.InstanceStatus, 0, 8)
	for index := int32(0); index < 8; index++ {
		running := newRev
		if int(index) < onStable {
			running = oldRev
		}
		rows = append(rows, rowOn(index, running))
	}
	return rows
}

// Eight engines, three of them still on the rejected version 2 while the
// operator's template is back on version 1: the run that records version 1
// for the engine stays open until all eight run version 1 and the unit
// reads the revert complete, then closes Completed once, and no later pass
// opens or adopts a run.
func TestAPushBackToTheStableDuringARevertKeepsTheRunOpenUntilEveryInstanceLands(t *testing.T) {
	isvc := pushedBackDuringRevertISVC(t)
	run := isvc.Status.Rollout.ActiveRun.RunID

	threeTrailing := irPublished(t, oldRev, oldRev, engineRows(5))
	for pass := 1; pass <= 3; pass++ {
		out := passOver(t, isvc, threeTrailing)
		if out.StateChanged || out.Opened || isvc.Status.Rollout.ActiveRun == nil || isvc.Status.Rollout.ActiveRun.RunID != run {
			t.Fatalf("pass %d with three engines still on version 2: outcome %+v, rollout %+v; want run %s still open", pass, out, isvc.Status.Rollout, run)
		}
	}

	// Every engine runs version 1 again. The unit's phase still reads
	// RollingBack from the pass before, because the dispatch reads the
	// revert complete after the run layer decided, so the run stays open
	// for this pass too.
	landed := irPublished(t, oldRev, oldRev, engineRows(8))
	if out := passOver(t, isvc, landed); out.StateChanged || isvc.Status.Rollout.ActiveRun == nil {
		t.Fatalf("the pass that reads every engine on version 1 while the unit still reads RollingBack: outcome %+v, rollout %+v; want the run still open", out, isvc.Status.Rollout)
	}

	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseRolledBack
	isvc.Status.Components[v1beta1.EngineComponent] = engine
	passOver(t, isvc, landed)
	if isvc.Status.Rollout.ActiveRun != nil {
		t.Fatalf("run stayed open after every engine landed on version 1 and the unit read RolledBack: %+v", isvc.Status.Rollout.ActiveRun)
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunCompleted || last.RunID != run {
		t.Fatalf("lastRun = %+v, want run %s closed Completed", last, run)
	}

	for pass := 1; pass <= 3; pass++ {
		if out := passOver(t, isvc, landed); out.Opened || isvc.Status.Rollout.ActiveRun != nil {
			t.Fatalf("pass %d after the close opened a run again: %+v", pass, out)
		}
	}
}

// A unit whose settled rejection is of a revision the run does not name
// holds the run open for nothing: its replica keeps the rejected revision
// as its target, which says nothing about the revision the run recorded,
// and every other member converged, so the run closes Completed.
func TestASettledRejectionOfAnotherRevisionLetsTheRunClose(t *testing.T) {
	isvc := concurrentCanaryISVC(t)
	in := testInputs(t, isvc,
		namedIR("llm-a-router", routerStableRev, routerRejectedRev),
		namedIR("llm-a-engine", engineStableRev, engineNewRev),
	)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatalf("open: %v", err)
	}
	if isvc.Status.Rollout.ActiveRun == nil {
		t.Fatal("no ActiveRun after open")
	}

	converged := testInputs(t, isvc,
		namedIR("llm-a-router", routerStableRev, routerRejectedRev),
		namedIR("llm-a-engine", engineNewRev, engineNewRev),
	)
	if _, err := Reconcile(context.Background(), converged); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if isvc.Status.Rollout.ActiveRun != nil {
		t.Fatalf("run stayed open with the engine converged and the router's rejection settled: %+v", isvc.Status.Rollout.ActiveRun)
	}
	if last := isvc.Status.Rollout.LastRun; last == nil || last.Outcome != v1beta1.RolloutRunCompleted {
		t.Fatalf("lastRun = %+v, want Completed", last)
	}
}
