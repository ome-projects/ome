package testfixture_test

import (
	"testing"
	"time"

	fixture "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestReconcileInput_ObservesTheStoredRowsAndWritesThemBack(t *testing.T) {
	isvc, ir := fixture.ServiceWithReadyInstance("svc", "ns", 1)
	c := fixture.FakeClient(t, isvc, ir)
	in := fixture.ReconcileInput(isvc, c, types.ComponentEngine)
	if in.Key.Namespace != "ns" || in.Key.OwnerName != "svc" || in.Key.Component != types.ComponentEngine {
		t.Fatalf("key = %+v, want ns/svc engine", in.Key)
	}
	if rows := in.ObservedState.InstanceStatuses; len(rows) != 1 || rows[0].Phase != types.InstancePhaseReady {
		t.Fatalf("observed rows = %+v, want the stored Ready row", rows)
	}
	if in.DesiredSpec.PodSpec == nil || in.DesiredSpec.PodSpec.Containers[0].Name != fixture.ContainerName {
		t.Errorf("desired spec = %+v, want one %s container", in.DesiredSpec.PodSpec, fixture.ContainerName)
	}
	if in.Requeue.Operation != 5*time.Second || in.Requeue.Gate != 3*time.Second {
		t.Errorf("requeue = %+v, want 5s/3s", in.Requeue)
	}
	if err := in.MutateInstance(t.Context(), 0, func(row *types.InstanceStatus) bool { row.Phase = types.InstancePhaseUpdating; return true }); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	if rows := fixture.InstanceStatuses(c, isvc, types.ComponentEngine); rows[0].Phase != types.InstancePhaseUpdating {
		t.Fatalf("stored rows after the write = %+v, want Updating", rows)
	}
	if deps := fixture.Deps(c); deps.Client == nil || deps.Recorder != nil {
		t.Errorf("deps = %+v, want the client and no recorder", deps)
	}
}

func TestPlans_NameTheirRunnersAndStrategy(t *testing.T) {
	inPlace := &types.InPlaceUpdateStrategy{}
	single := fixture.SinglePodPlan(types.UpdateStrategyInPlaceIfPossible, inPlace)
	if single.Component != types.ComponentEngine || single.Replicas != 1 || len(single.Instances) != 1 ||
		len(single.Instances[0].Runners) != 1 || single.Instances[0].Runners[0].Name != types.RunnerDefault ||
		single.UpdateStrategy.Type != types.UpdateStrategyInPlaceIfPossible || single.UpdateStrategy.InPlaceUpdateStrategy != inPlace ||
		single.InstanceReadyTimeout != 30*time.Minute {
		t.Errorf("single-pod plan = %+v, want one default runner under InPlaceIfPossible", single)
	}
	gang := fixture.LeaderWorkerPlan(types.UpdateStrategyRecreatePod)
	if runners := gang.Instances[0].Runners; len(runners) != 2 || runners[0].Name != types.RunnerLeader || runners[1].Name != types.RunnerWorker ||
		gang.UpdateStrategy.Type != types.UpdateStrategyRecreatePod {
		t.Errorf("leader/worker plan = %+v, want a leader and a worker under RecreatePod", gang)
	}
}

func TestResetExpectations_ReplacesTheProcessWideSet(t *testing.T) {
	before := types.DefaultExpectations
	fixture.ResetExpectations(t)
	if types.DefaultExpectations == before || types.DefaultExpectations == nil {
		t.Fatalf("expectations were not replaced")
	}
}
