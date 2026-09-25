package workload

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func intPtr(v int) *int    { return &v }
func boolPtr(v bool) *bool { return &v }
func restartPolicyPtr(v types.RestartPolicy) *types.RestartPolicy {
	return &v
}
func readyPolicyPtr(v types.InstanceReadyPolicy) *types.InstanceReadyPolicy {
	return &v
}

// singlePodDesired builds a single-pod WorkloadDesiredSpec for tests.
// replicas <= 0 mirrors the production path where MinReplicas=nil/0
// defaults to 1.
func singlePodDesired(replicas int32, lifecycle types.Lifecycle) types.WorkloadDesiredSpec {
	return types.WorkloadDesiredSpec{
		Replicas:  replicas,
		Runners:   []types.Runner{{Name: "default", Size: 1}},
		Lifecycle: lifecycle,
	}
}

func multiPodDesired(replicas, workerSize int32, lifecycle types.Lifecycle) types.WorkloadDesiredSpec {
	return types.WorkloadDesiredSpec{
		Replicas: replicas,
		MultiPod: true,
		Runners: []types.Runner{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: workerSize},
		},
		Lifecycle: lifecycle,
	}
}

// TestBuildPlan_MultiPodEmitsLeaderWorkerRunners pins the multi-pod
// layout: one leader runner of size 1 plus one worker runner of size
// WorkerSize.
func TestBuildPlan_MultiPodEmitsLeaderWorkerRunners(t *testing.T) {
	desired := multiPodDesired(2, 3, types.Lifecycle{})
	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := plan.Replicas, int32(2); got != want {
		t.Errorf("Replicas: got %d want %d", got, want)
	}
	if got, want := len(plan.Instances), 2; got != want {
		t.Fatalf("len(Instances): got %d want %d", got, want)
	}
	for i, inst := range plan.Instances {
		if int(inst.Index) != i {
			t.Errorf("Instances[%d].Index: got %d want %d", i, inst.Index, i)
		}
		want := []types.RunnerPlan{
			{Name: "leader", Size: 1},
			{Name: "worker", Size: 3},
		}
		if diff := cmp.Diff(want, inst.Runners); diff != "" {
			t.Errorf("Instances[%d].Runners mismatch (-want +got):\n%s", i, diff)
		}
	}
}

// TestBuildPlan_MultiPodDefaults pins the defaults for multi-pod
// Instances: RestartPolicy=RecreateInstance, ReadyPolicy=AllPodReady.
// Single-pod defaults stay None for both.
func TestBuildPlan_MultiPodDefaults(t *testing.T) {
	desired := multiPodDesired(1, 1, types.Lifecycle{})
	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.RestartPolicy != types.RestartPolicyRecreateInstance {
		t.Errorf("RestartPolicy: got %q want RecreateInstanceOnPodRestart", plan.RestartPolicy)
	}
	if plan.ReadyPolicy != types.InstanceReadyPolicyAllPodReady {
		t.Errorf("ReadyPolicy: got %q want AllPodReady", plan.ReadyPolicy)
	}
}

func TestBuildPlan_ProjectsPausedCircuitBreaker(t *testing.T) {
	desired := multiPodDesired(1, 1, types.Lifecycle{})
	desired.Paused = true

	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !plan.Paused {
		t.Fatal("Paused: got false want true")
	}
}

// TestBuildPlan_MultiPodWithZeroWorkerSize confirms WorkerSize=0 still
// produces leader+worker entries; the webhook validator rejects orphan
// leader without worker.size>0, but BuildPlan stays defensive.
func TestBuildPlan_MultiPodWithZeroWorkerSize(t *testing.T) {
	desired := multiPodDesired(1, 0, types.Lifecycle{})
	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := len(plan.Instances[0].Runners), 2; got != want {
		t.Fatalf("Runners length: got %d want %d (leader + worker even if size=0)", got, want)
	}
	if plan.Instances[0].Runners[0].Size != 1 || plan.Instances[0].Runners[0].Name != "leader" {
		t.Errorf("leader runner shape: got %+v", plan.Instances[0].Runners[0])
	}
	if plan.Instances[0].Runners[1].Size != 0 || plan.Instances[0].Runners[1].Name != "worker" {
		t.Errorf("worker runner shape: got %+v", plan.Instances[0].Runners[1])
	}
}

func TestBuildPlan_SinglePod_DefaultsFromDefaulter(t *testing.T) {
	// Simulates a WorkloadDesiredSpec produced by an adapter whose source
	// went through the mutating webhook defaulter — lifecycle is fully
	// populated.
	desired := singlePodDesired(2, types.Lifecycle{
		RestartPolicy: restartPolicyPtr(types.RestartPolicyNone),
		ReadyPolicy:   readyPolicyPtr(types.InstanceReadyPolicyNone),
		UpdateStrategy: &types.UpdateStrategy{
			Type: types.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &types.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: boolPtr(true),
			},
		},
		InstanceReadyTimeout: &metav1.Duration{Duration: 30 * time.Minute},
		MigrationPolicy:      &types.MigrationPolicy{Mode: types.MigrationModeAuto},
	})
	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		RestartPolicy: types.RestartPolicyNone,
		UpdateStrategy: types.UpdateStrategy{
			Type: types.UpdateStrategyInPlaceIfPossible,
			InPlaceUpdateStrategy: &types.InPlaceUpdateStrategy{
				MarkNotReadyDuringLifecycle: boolPtr(true),
			},
		},
		ReadyPolicy:          types.InstanceReadyPolicyNone,
		InstanceReadyTimeout: 30 * time.Minute,
		MigrationMode:        types.MigrationModeAuto,
	}
	if diff := cmp.Diff(want, plan); diff != "" {
		t.Fatalf("plan mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildPlan_SinglePod_InlineDefaultsWhenWebhookSkipped(t *testing.T) {
	// Simulates a pre-defaulter object — adapter projected an empty
	// lifecycle. BuildPlan applies the same defaults inline, except for the
	// readiness window, which is operator configuration the adapter overlays.
	desired := singlePodDesired(1, types.Lifecycle{})
	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Replicas != 1 {
		t.Errorf("Replicas: got %d want 1", plan.Replicas)
	}
	if plan.RestartPolicy != types.RestartPolicyNone {
		t.Errorf("RestartPolicy: got %q want None", plan.RestartPolicy)
	}
	if plan.ReadyPolicy != types.InstanceReadyPolicyNone {
		t.Errorf("ReadyPolicy: got %q want None", plan.ReadyPolicy)
	}
	if plan.UpdateStrategy.Type != types.UpdateStrategySurgeThenDrain {
		t.Errorf("UpdateStrategy.Type: got %q want SurgeThenDrain (default)", plan.UpdateStrategy.Type)
	}
	if plan.UpdateStrategy.InPlaceUpdateStrategy == nil ||
		plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle == nil ||
		!*plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle {
		t.Errorf("gracePeriodSeconds: got %+v want 30", plan.UpdateStrategy.InPlaceUpdateStrategy)
	}
	if plan.InstanceReadyTimeout != 0 {
		t.Errorf("InstanceReadyTimeout: got %v want 0 (no per-resource window; the adapter overlays the operator's)", plan.InstanceReadyTimeout)
	}
	if plan.MigrationMode != types.MigrationModeAuto {
		t.Errorf("MigrationMode: got %q want auto", plan.MigrationMode)
	}
}

func TestBuildPlan_ReplicaCount(t *testing.T) {
	tests := []struct {
		name   string
		minRep *int
		want   int32
	}{
		{name: "nil MinReplicas defaults to 1", minRep: nil, want: 1},
		{name: "zero MinReplicas defaults to 1", minRep: intPtr(0), want: 1},
		{name: "explicit 1", minRep: intPtr(1), want: 1},
		{name: "explicit 4", minRep: intPtr(4), want: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rep int32
			if tt.minRep != nil {
				rep = int32(*tt.minRep)
			}
			desired := singlePodDesired(rep, types.Lifecycle{})
			plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if plan.Replicas != tt.want {
				t.Errorf("Replicas: got %d want %d", plan.Replicas, tt.want)
			}
			if int32(len(plan.Instances)) != tt.want {
				t.Errorf("len(Instances): got %d want %d", len(plan.Instances), tt.want)
			}
			for i, inst := range plan.Instances {
				if inst.Index != int32(i) {
					t.Errorf("Instances[%d].Index: got %d", i, inst.Index)
				}
				if len(inst.Runners) != 1 ||
					inst.Runners[0].Name != "default" ||
					inst.Runners[0].Size != 1 {
					t.Errorf("Instances[%d].Runners: got %+v want [{default 1}]", i, inst.Runners)
				}
			}
		})
	}
}

func TestBuildPlan_IncarnationDefaultsToOneOnFirstReconcile(t *testing.T) {
	// Observed state has no InstanceStatuses — every Instance gets
	// Incarnation=1.
	desired := singlePodDesired(3, types.Lifecycle{})
	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, inst := range plan.Instances {
		if inst.Incarnation != 1 {
			t.Errorf("Instances[%d].Incarnation: got %d want 1", inst.Index, inst.Incarnation)
		}
	}
}

func TestBuildPlan_IncarnationPreservedFromStatus(t *testing.T) {
	// Observed state carries InstanceStatuses with explicit Incarnations —
	// BuildPlan reads them back instead of resetting to 1.
	desired := singlePodDesired(2, types.Lifecycle{})
	observed := types.WorkloadObservedState{
		InstanceStatuses: []types.InstanceStatus{
			{Index: 0, Incarnation: 4},
			{Index: 1, Incarnation: 2},
		},
	}
	plan, err := BuildPlan(types.ComponentEngine, desired, observed)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[int32]int64{0: 4, 1: 2}
	for _, inst := range plan.Instances {
		if got := inst.Incarnation; got != want[inst.Index] {
			t.Errorf("Instances[%d].Incarnation: got %d want %d", inst.Index, got, want[inst.Index])
		}
	}
}

func TestBuildPlan_IncarnationScopedToWorkload(t *testing.T) {
	// The observed state is per-workload now (Source.ObservedState
	// returns only this Component's statuses), so cross-Component leakage
	// is impossible at the call site. The test pins the BuildPlan contract:
	// no InstanceStatuses → default to 1 for every Instance.
	desired := singlePodDesired(1, types.Lifecycle{})
	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Instances[0].Incarnation != 1 {
		t.Errorf("Engine[0].Incarnation: got %d want 1", plan.Instances[0].Incarnation)
	}
}

func TestBuildPlan_RestartPolicyExplicitWinsOverDefault(t *testing.T) {
	desired := singlePodDesired(0, types.Lifecycle{
		RestartPolicy: restartPolicyPtr(types.RestartPolicyRecreateInstance),
	})
	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Single-pod defaults to None; an explicit RecreateInstance must win.
	if plan.RestartPolicy != types.RestartPolicyRecreateInstance {
		t.Errorf("RestartPolicy: got %q want RecreateInstanceOnPodRestart", plan.RestartPolicy)
	}
}

func TestLowestUnusedIndex(t *testing.T) {
	used := map[int32]struct{}{0: {}, 1: {}, 3: {}}
	if got := lowestUnusedIndex(used); got != 2 {
		t.Errorf("got %d want 2", got)
	}
	if got := lowestUnusedIndex(map[int32]struct{}{}); got != 0 {
		t.Errorf("empty set: got %d want 0", got)
	}
}

func TestInstancePlanIndices_MultiReplicaMigrationPreservesUnrelatedInstance(t *testing.T) {
	// Regression: with replicas=2 and statuses {0 Ready, 1 Ready},
	// migrating instance 0 (surge at 2) must yield plan {0, 1, 2}.
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseMigrating},
		{Index: 1, Phase: types.InstancePhaseReady},
		{Index: 2, Phase: types.InstancePhaseCreating, Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate}},
	}
	got := instancePlanIndices(instances, 2)
	hit := map[int32]bool{}
	for _, idx := range got {
		hit[idx] = true
	}
	if !hit[0] || !hit[1] || !hit[2] {
		t.Errorf("plan must include {0,1,2}; got %v", got)
	}
}

func TestInstancePlanIndices_PreservesSparseMigrationLayout(t *testing.T) {
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseMigrating},
		{Index: 2, Phase: types.InstancePhaseCreating, Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate}},
	}
	got := instancePlanIndices(instances, 1)
	// Both migration-in-flight indices must be in the plan even though
	// replicas=1.
	hit := map[int32]bool{}
	for _, idx := range got {
		hit[idx] = true
	}
	if !hit[0] || !hit[2] {
		t.Errorf("plan must include both migration indices; got %v", got)
	}
}

func TestInstancePlanIndices_MigrationReadyTargetReplacesSource(t *testing.T) {
	const revision = "comp-rev-current"
	targetIndex := int32(1)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseMigrating, RunningRevision: revision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, RequestUUID: "request-a", SurgeIndex: &targetIndex}},
		{Index: targetIndex, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: revision},
	}
	indices := instancePlanIndices(instances, 1)
	if diff := cmp.Diff([]int32{targetIndex}, indices); diff != "" {
		t.Fatalf("promoted migration target must replace its source (-want +got):\n%s", diff)
	}
	plan := types.ComponentPlan{Instances: []types.InstancePlan{{Index: targetIndex}}}
	if extras := ScaleDownExtras(instances, plan); len(extras) != 0 {
		t.Fatalf("normal scale-down selected migration source as extra: %v", extras)
	}
}

func TestInstancePlanIndices_MigrationRetiringSourceDoesNotDisplaceSteadyInstance(t *testing.T) {
	const revision = "comp-rev-current"
	targetIndex := int32(8)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseMigrating, RunningRevision: revision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, RequestUUID: "request-a", SurgeIndex: &targetIndex}},
		{Index: 1, Phase: types.InstancePhaseReady},
		{Index: 2, Phase: types.InstancePhaseReady},
		{Index: 3, Phase: types.InstancePhaseReady},
		{Index: 4, Phase: types.InstancePhaseReady},
		{Index: 5, Phase: types.InstancePhaseReady},
		{Index: 6, Phase: types.InstancePhaseReady},
		{Index: 7, Phase: types.InstancePhaseCreating},
		{Index: targetIndex, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: revision},
	}
	want := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	if diff := cmp.Diff(want, instancePlanIndices(instances, 8)); diff != "" {
		t.Fatalf("retiring migration source displaced a steady instance (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_MigrationSourceRequiresPromotedTargetProof(t *testing.T) {
	const revision = "comp-rev-current"
	targetIndex := int32(2)
	source := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseMigrating, RunningRevision: revision,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, RequestUUID: "request-a", SurgeIndex: &targetIndex}}
	tests := []struct {
		name   string
		target types.InstanceStatus
	}{
		{
			name:   "wrong revision",
			target: types.InstanceStatus{Index: targetIndex, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "comp-rev-other"},
		},
		{
			name: "operation still active",
			target: types.InstanceStatus{Index: targetIndex, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: revision,
				Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate}},
		},
		{
			name: "target revision still set",
			target: types.InstanceStatus{Index: targetIndex, Incarnation: 1, Phase: types.InstancePhaseReady,
				RunningRevision: revision, TargetRevision: revision},
		},
		{
			name:   "incarnation does not match a fresh surge",
			target: types.InstanceStatus{Index: targetIndex, Incarnation: 2, Phase: types.InstancePhaseReady, RunningRevision: revision},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instances := []types.InstanceStatus{
				source,
				{Index: 1, Phase: types.InstancePhaseCreating, RunningRevision: revision},
				test.target,
			}
			want := []int32{0, 1, 2}
			if diff := cmp.Diff(want, instancePlanIndices(instances, 2)); diff != "" {
				t.Fatalf("unproven migration target released its source (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInstancePlanIndices_SharedMigrationTargetKeepsAllParticipants(t *testing.T) {
	const revision = "comp-rev-current"
	sharedTarget := int32(3)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseMigrating, RunningRevision: revision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, RequestUUID: "request-a", SurgeIndex: &sharedTarget}},
		{Index: 1, Phase: types.InstancePhaseMigrating, RunningRevision: revision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, RequestUUID: "request-b", SurgeIndex: &sharedTarget}},
		{Index: 2, Phase: types.InstancePhaseCreating, RunningRevision: revision},
		{Index: sharedTarget, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: revision},
	}
	want := []int32{0, 1, 2, 3}
	if diff := cmp.Diff(want, instancePlanIndices(instances, 3)); diff != "" {
		t.Fatalf("shared migration target released a source or sibling (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_WrongPhaseMigrationReferenceBlocksRetirement(t *testing.T) {
	const revision = "comp-rev-current"
	sharedTarget := int32(3)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseMigrating, RunningRevision: revision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, RequestUUID: "request-a", SurgeIndex: &sharedTarget}},
		{Index: 1, Phase: types.InstancePhaseFailed, RunningRevision: revision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, RequestUUID: "request-stale", SurgeIndex: &sharedTarget}},
		{Index: 2, Phase: types.InstancePhaseCreating, RunningRevision: revision},
		{Index: sharedTarget, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: revision},
	}
	want := []int32{0, 1, 2, 3}
	if diff := cmp.Diff(want, instancePlanIndices(instances, 2)); diff != "" {
		t.Fatalf("wrong-phase migration reference released a valid source (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_MixedHandoffsSharingTargetKeepAllParticipants(t *testing.T) {
	const (
		oldRevision = "comp-rev-oldbbbb"
		newRevision = "comp-rev-newaaaa"
	)
	sharedTarget := int32(3)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseMigrating, RunningRevision: newRevision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, RequestUUID: "request-a", SurgeIndex: &sharedTarget}},
		{Index: 1, Phase: types.InstancePhaseUpdating, RunningRevision: oldRevision, TargetRevision: newRevision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain,
				SurgeIndex: &sharedTarget, TargetRevision: newRevision}},
		{Index: 2, Phase: types.InstancePhaseCreating, RunningRevision: oldRevision},
		{Index: sharedTarget, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: newRevision},
	}
	want := []int32{0, 1, 2, 3}
	if diff := cmp.Diff(want, instancePlanIndices(instances, 3)); diff != "" {
		t.Fatalf("mixed handoffs sharing one target released a participant (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_GangSurgePinsPair(t *testing.T) {
	// replicas=2 with a gang surge in flight: source 0 (Updating, surge→3),
	// unrelated sibling 1 (Ready), replacement 3 (GangSurgeTarget). The
	// plan must include all three — the source counts toward the steady
	// budget (so sibling 1 isn't dropped into scale-down) and the surge
	// target is pinned as the transient +1.
	k := int32(3)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: "Surge", SurgeIndex: &k}},
		{Index: 1, Phase: types.InstancePhaseReady},
		{Index: 3, Phase: types.InstancePhaseCreating, Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget}},
	}
	got := instancePlanIndices(instances, 2)
	hit := map[int32]bool{}
	for _, idx := range got {
		hit[idx] = true
	}
	if !hit[0] || !hit[1] || !hit[3] {
		t.Errorf("plan must include source 0, sibling 1, surge target 3; got %v", got)
	}
}

// A retired replacement gang is pinned by the same rule as a live one:
// the marker stays in the plan for as long as a source references the
// index, so the scale-down wave cannot take the retirement away from it
// mid-teardown. An UNREFERENCED cleanup marker is a different shape and
// is left for the scale-down pipeline to reap.
func TestInstancePlanIndices_GangCleanupMarkerPinnedWhileReferenced(t *testing.T) {
	k := int32(1)
	referenced := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: "comp-rev-v1aaaaaa",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: "Surge", SurgeIndex: &k}},
		{Index: 1, Phase: types.InstancePhaseCreating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTargetCleanup}},
	}
	hit := map[int32]bool{}
	for _, idx := range instancePlanIndices(referenced, 1) {
		hit[idx] = true
	}
	if !hit[0] || !hit[1] {
		t.Errorf("a referenced cleanup marker must stay pinned at replicas=1; got %v", instancePlanIndices(referenced, 1))
	}

	orphan := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: "comp-rev-v1aaaaaa"},
		{Index: 1, Phase: types.InstancePhaseCreating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTargetCleanup}},
	}
	if got := instancePlanIndices(orphan, 1); len(got) != 1 || got[0] != 0 {
		t.Errorf("an unreferenced cleanup marker must fall out for the scale-down pipeline; got %v", got)
	}
}

func TestInstancePlanIndices_OrphanGangSurgeMarkerUnpinned(t *testing.T) {
	// Marker-liveness invariant: a GangSurgeTarget marker whose
	// source no longer carries a SurgeIndex operation referencing it is
	// an orphan — it must FALL OUT of the plan so the scale-down pass
	// reaps the dead replacement gang, instead of leaking it forever.
	// Shape: the corrective roll-back re-adopted the source back to
	// Ready (operation cleared) while the crashed surge gang's marker
	// is still present.
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: "comp-rev-v1aaaaaa"},
		{Index: 1, Phase: types.InstancePhaseFailed,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget}},
	}
	got := instancePlanIndices(instances, 1)
	if len(got) != 1 || got[0] != 0 {
		t.Errorf("orphan marker must be unpinned (plan {0}, marker 1 becomes a scale-down extra); got %v", got)
	}

	// Liveness counter-case: the SAME marker stays pinned while its
	// source op references it — a mid-flight healthy surge is untouched.
	k := int32(1)
	instances[0] = types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: "comp-rev-v1aaaaaa",
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: "Surge", SurgeIndex: &k}}
	got = instancePlanIndices(instances, 1)
	hit := map[int32]bool{}
	for _, idx := range got {
		hit[idx] = true
	}
	if !hit[0] || !hit[1] {
		t.Errorf("live surge pair must stay pinned; got %v", got)
	}
}

func TestInstancePlanIndices_GangSurgeReadyTargetRequiresDrainProof(t *testing.T) {
	newRev, oldRev := "comp-rev-newaaaa", "comp-rev-oldbbbb"
	zero := int32(0)
	statuses := func(step string) []types.InstanceStatus {
		return []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: newRev},
			{Index: 1, Phase: types.InstancePhaseUpdating, RunningRevision: oldRev, TargetRevision: newRev,
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: step, SurgeIndex: &zero, TargetRevision: newRev}},
			{Index: 2, Phase: types.InstancePhaseReady, RunningRevision: newRev},
		}
	}

	// A Ready occupant does not prove that a fresh claim owns the target.
	if diff := cmp.Diff([]int32{0, 1, 2}, instancePlanIndices(statuses("Surge"), 2)); diff != "" {
		t.Fatalf("unconfirmed claim must stay pinned (-want +got):\n%s", diff)
	}

	// SurgeDrain follows target validation and is safe to release after the
	// replacement is promoted.
	if diff := cmp.Diff([]int32{0, 2}, instancePlanIndices(statuses(types.UpdateStepSurgeDrain), 2)); diff != "" {
		t.Fatalf("validated source should leave the steady plan (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_GangSurgeSourceRequiresPromotedTargetProof(t *testing.T) {
	const (
		oldRevision = "comp-rev-oldbbbb"
		newRevision = "comp-rev-newaaaa"
	)
	targetIndex := int32(2)
	validSource := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating,
		RunningRevision: oldRevision, TargetRevision: newRevision,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain,
			SurgeIndex: &targetIndex, TargetRevision: newRevision}}
	validTarget := types.InstanceStatus{Index: targetIndex, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: newRevision}

	wrongIncarnation := validTarget
	wrongIncarnation.Incarnation = 2
	wrongOrdinal := validTarget
	wrongOrdinal.ActiveOrdinal = 1
	wrongTargetRevision := validTarget
	wrongTargetRevision.RunningRevision = oldRevision
	activeTarget := validTarget
	activeTarget.Operation = &types.InstanceOperation{Type: types.InstanceOperationUpdate}
	pinnedTarget := validTarget
	pinnedTarget.TargetRevision = newRevision
	wrongPhase := validSource
	wrongPhase.Phase = types.InstancePhaseFailed
	missingRunningRevision := validSource
	missingRunningRevision.RunningRevision = ""
	missingTargetRevision := validSource
	missingTargetRevision.TargetRevision = ""
	mismatchedTargetRevision := validSource
	mismatchedTargetRevision.TargetRevision = "comp-rev-other"

	tests := []struct {
		name   string
		source types.InstanceStatus
		target types.InstanceStatus
	}{
		{name: "target incarnation", source: validSource, target: wrongIncarnation},
		{name: "target active ordinal", source: validSource, target: wrongOrdinal},
		{name: "target running revision", source: validSource, target: wrongTargetRevision},
		{name: "target operation", source: validSource, target: activeTarget},
		{name: "target revision pin", source: validSource, target: pinnedTarget},
		{name: "source phase", source: wrongPhase, target: validTarget},
		{name: "source running revision", source: missingRunningRevision, target: validTarget},
		{name: "source target revision", source: missingTargetRevision, target: validTarget},
		{name: "source revision pin", source: mismatchedTargetRevision, target: validTarget},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instances := []types.InstanceStatus{
				test.source,
				{Index: 1, Phase: types.InstancePhaseCreating, RunningRevision: oldRevision},
				test.target,
			}
			want := []int32{0, 1, 2}
			if diff := cmp.Diff(want, instancePlanIndices(instances, 2)); diff != "" {
				t.Fatalf("unproven gang-surge target released its source (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInstancePlanIndices_GangSurgeRetiringSourceDoesNotDisplaceSteadyInstance(t *testing.T) {
	const (
		oldRevision = "comp-rev-oldbbbb"
		newRevision = "comp-rev-newaaaa"
	)
	targetIndex := int32(8)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: oldRevision, TargetRevision: newRevision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain,
				SurgeIndex: &targetIndex, TargetRevision: newRevision}},
		{Index: 1, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 2, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 3, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 4, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 5, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 6, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 7, Phase: types.InstancePhaseCreating, TargetRevision: oldRevision},
		{Index: targetIndex, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: newRevision},
	}
	want := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	indices := instancePlanIndices(instances, 8)
	if diff := cmp.Diff(want, indices); diff != "" {
		t.Fatalf("retiring gang-surge source displaced a steady instance (-want +got):\n%s", diff)
	}
	planned := make([]types.InstancePlan, 0, len(indices))
	for _, index := range indices {
		planned = append(planned, types.InstancePlan{Index: index})
	}
	if diff := cmp.Diff([]int32{0}, ScaleDownExtras(instances, types.ComponentPlan{Instances: planned})); diff != "" {
		t.Fatalf("normal scale-down ownership crossed the retiring update source (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_ConcurrentRetiringSourcesDoNotDisplaceSteadyInstances(t *testing.T) {
	const (
		oldRevision = "comp-rev-oldbbbb"
		newRevision = "comp-rev-newaaaa"
	)
	targetEight, targetNine := int32(8), int32(9)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: oldRevision, TargetRevision: newRevision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain,
				SurgeIndex: &targetEight, TargetRevision: newRevision}},
		{Index: 1, Phase: types.InstancePhaseUpdating, RunningRevision: oldRevision, TargetRevision: newRevision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain,
				SurgeIndex: &targetNine, TargetRevision: newRevision}},
		{Index: 2, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 3, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 4, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 5, Phase: types.InstancePhaseReady, RunningRevision: oldRevision},
		{Index: 6, Phase: types.InstancePhaseCreating, TargetRevision: oldRevision},
		{Index: 7, Phase: types.InstancePhaseCreating, TargetRevision: oldRevision},
		{Index: targetEight, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: newRevision},
		{Index: targetNine, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: newRevision},
	}
	want := []int32{2, 3, 4, 5, 6, 7, 8, 9}
	if diff := cmp.Diff(want, instancePlanIndices(instances, 8)); diff != "" {
		t.Fatalf("retiring gang-surge sources displaced steady instances (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_SharedUpdateTargetKeepsAllParticipants(t *testing.T) {
	const (
		oldRevision = "comp-rev-oldbbbb"
		newRevision = "comp-rev-newaaaa"
	)
	sharedTarget := int32(3)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: oldRevision, TargetRevision: newRevision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain,
				SurgeIndex: &sharedTarget, TargetRevision: newRevision}},
		{Index: 1, Phase: types.InstancePhaseUpdating, RunningRevision: oldRevision, TargetRevision: newRevision,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain,
				SurgeIndex: &sharedTarget, TargetRevision: newRevision}},
		{Index: 2, Phase: types.InstancePhaseCreating, TargetRevision: oldRevision},
		{Index: sharedTarget, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: newRevision},
	}
	want := []int32{0, 1, 2, 3}
	if diff := cmp.Diff(want, instancePlanIndices(instances, 3)); diff != "" {
		t.Fatalf("shared update target released a source or sibling (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_NonRetiringSelectionControls(t *testing.T) {
	tests := []struct {
		name      string
		instances []types.InstanceStatus
	}{
		{
			name: "Ready Instance keeps priority",
			instances: []types.InstanceStatus{
				{Index: 0, Phase: types.InstancePhaseReady},
				{Index: 1, Phase: types.InstancePhaseCreating},
				{Index: 2, Phase: types.InstancePhaseReady},
			},
		},
		{
			name: "oldest non-Ready Instance remains the fallback",
			instances: []types.InstanceStatus{
				{Index: 0, Phase: types.InstancePhaseCreating},
				{Index: 1, Phase: types.InstancePhaseCreating},
				{Index: 2, Phase: types.InstancePhaseReady},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := []int32{0, 2}
			if diff := cmp.Diff(want, instancePlanIndices(test.instances, 2)); diff != "" {
				t.Fatalf("non-retiring selection changed (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInstancePlanIndices_SinglePodSurgeNotPinnedAsPair(t *testing.T) {
	// A single-pod surge carries Op.Step=Surge but NO SurgeIndex (it
	// toggles ActiveOrdinal in place). It must NOT be treated as a gang
	// surge pair — replicas=1, one Updating instance → plan stays {0}, no
	// phantom surge index.
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: "Surge"}},
	}
	got := instancePlanIndices(instances, 1)
	if len(got) != 1 || got[0] != 0 {
		t.Errorf("single-pod surge must plan exactly {0}; got %v", got)
	}
}

func TestAllocateSurgeIndex(t *testing.T) {
	// Smoke: with statuses {0, 1, 3}, surge should land at 2.
	instances := []types.InstanceStatus{
		{Index: 0}, {Index: 1}, {Index: 3},
	}
	if got := types.AllocateSurgeIndex(instances); got != 2 {
		t.Errorf("got %d want 2", got)
	}
	// Empty status returns 0.
	if got := types.AllocateSurgeIndex(nil); got != 0 {
		t.Errorf("empty: got %d want 0", got)
	}
	// Shared-surgeIndex regression: an in-flight surge slot
	// recorded in Operation.SurgeIndex must be excluded even though no Instance
	// with that Index exists yet. Without this, every Instance surging in one
	// reconcile pass collides on the same lowest-free index, and when that single
	// shared surge becomes Ready the drain-on-ready logic releases ALL sharing
	// sources at once — the full-fleet wipe. With instances {0(surge→2), 1},
	// indices {0,1,2} are taken, so the next surge must land at 3, not 2.
	surgeAt2 := int32(2)
	withInflightSurge := []types.InstanceStatus{
		{Index: 0, Operation: &types.InstanceOperation{SurgeIndex: &surgeAt2}},
		{Index: 1},
	}
	if got := types.AllocateSurgeIndex(withInflightSurge); got != 3 {
		t.Errorf("in-flight surge slot at 2 must be excluded: got %d want 3", got)
	}
}

// TestPartitionHeldIndices pins the canary hold-membership rule: the
// lowest-indexed `Partition` Instances observed OFF the target revision
// are held; Instances converging to target (mid-update, preserved
// Update op — including gang-surge target markers), transient migration
// surges, and unobserved Instances are never hold candidates. Holding a
// mid-surge Instance would strand its surge pod (never promoted to
// serving) and permanently consume the maxSurge budget, deadlocking the
// roll — the hold falls to the next-lowest old-revision Instance
// instead.
func TestPartitionHeldIndices(t *testing.T) {
	p := func(v int32) *int32 { return &v }
	planFor := func(indices ...int32) []types.InstancePlan {
		out := make([]types.InstancePlan, 0, len(indices))
		for _, idx := range indices {
			out = append(out, types.InstancePlan{Index: idx})
		}
		return out
	}
	const target = "owner-engine-target"
	cases := []struct {
		name      string
		partition *int32
		observed  []types.InstanceStatus
		planned   []types.InstancePlan
		want      map[int32]bool
	}{
		{
			name:      "nil partition holds nothing",
			partition: nil,
			planned:   planFor(0, 1),
			observed: []types.InstanceStatus{
				{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: "old"},
				{Index: 1, Phase: types.InstancePhaseReady, RunningRevision: "old"},
			},
			want: map[int32]bool{},
		},
		{
			name:      "lowest old-revision Instances held, count = Partition",
			partition: p(2),
			planned:   planFor(0, 1, 2),
			observed: []types.InstanceStatus{
				{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: "old"},
				{Index: 1, Phase: types.InstancePhaseReady, RunningRevision: "old"},
				{Index: 2, Phase: types.InstancePhaseReady, RunningRevision: "old"},
			},
			want: map[int32]bool{0: true, 1: true},
		},
		{
			name:      "on-target Instance is past holding; hold keys to revision, not position",
			partition: p(1),
			planned:   planFor(0, 1),
			observed: []types.InstanceStatus{
				{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: target},
				{Index: 1, Phase: types.InstancePhaseReady, RunningRevision: "old"},
			},
			want: map[int32]bool{1: true},
		},
		{
			name:      "mid-update Instance must finish — hold falls to next old-revision Instance",
			partition: p(1),
			planned:   planFor(0, 1),
			observed: []types.InstanceStatus{
				{Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: "old",
					Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: "Surge"}},
				{Index: 1, Phase: types.InstancePhaseReady, RunningRevision: "old"},
			},
			want: map[int32]bool{1: true},
		},
		{
			name:      "Failed Update continuation and gang-surge target marker are never held",
			partition: p(2),
			planned:   planFor(0, 1, 2),
			observed: []types.InstanceStatus{
				{Index: 0, Phase: types.InstancePhaseCreating, RunningRevision: "",
					Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget}},
				{Index: 1, Phase: types.InstancePhaseFailed, RunningRevision: "old",
					Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: "Drain"}},
				{Index: 2, Phase: types.InstancePhaseReady, RunningRevision: "old"},
			},
			want: map[int32]bool{2: true},
		},
		{
			name:      "migration surge target excluded; Migrating source stays a candidate",
			partition: p(1),
			planned:   planFor(1, 5),
			observed: []types.InstanceStatus{
				{Index: 1, Phase: types.InstancePhaseMigrating, RunningRevision: "old",
					Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate}},
				{Index: 5, Phase: types.InstancePhaseCreating, RunningRevision: "",
					Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate}},
			},
			want: map[int32]bool{1: true},
		},
		{
			name:      "unobserved planned Instances are not candidates",
			partition: p(2),
			planned:   planFor(0, 1),
			observed:  []types.InstanceStatus{{Index: 1, Phase: types.InstancePhaseReady, RunningRevision: "old"}},
			want:      map[int32]bool{1: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := escalation.PartitionHeldIndices(tc.partition, types.ReconcileInput{ObservedState: types.WorkloadObservedState{InstanceStatuses: tc.observed}}, tc.planned, target)
			if len(got) != len(tc.want) {
				t.Fatalf("held = %v, want %v", got, tc.want)
			}
			for idx := range tc.want {
				if !got[idx] {
					t.Errorf("held = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// Surge-pair pinning vs replica drops: a replica count lowered while a
// surge pair (migration or gang update) is in flight must keep BOTH
// pair members in the plan — the pair is released only through its own
// state machine, never by scale-down — and shed the budget overflow
// from the unpinned siblings instead.

func TestInstancePlanIndices_ReplicaDropKeepsMigrationPair(t *testing.T) {
	surge := int32(3)
	back := int32(0)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseMigrating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, SurgeIndex: &surge}},
		{Index: 1, Phase: types.InstancePhaseReady},
		{Index: 3, Phase: types.InstancePhaseCreating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, SurgeIndex: &back}},
	}
	if diff := cmp.Diff([]int32{0, 3}, instancePlanIndices(instances, 1)); diff != "" {
		t.Fatalf("replica drop must keep the in-flight migration pair and shed the Ready sibling (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_ReplicaDropKeepsGangSurgePair(t *testing.T) {
	surge := int32(3)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseUpdating, TargetRevision: "comp-rev-v2bbbbbb",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: "Surge",
				SurgeIndex: &surge, TargetRevision: "comp-rev-v2bbbbbb"}},
		{Index: 1, Phase: types.InstancePhaseReady},
		{Index: 3, Phase: types.InstancePhaseCreating, TargetRevision: "comp-rev-v2bbbbbb",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget,
				TargetRevision: "comp-rev-v2bbbbbb"}},
	}
	if diff := cmp.Diff([]int32{0, 3}, instancePlanIndices(instances, 1)); diff != "" {
		t.Fatalf("replica drop must keep the in-flight gang surge pair and shed the Ready sibling (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_MigrationTargetNotReadyKeepsSourcePinned(t *testing.T) {
	// The migration-side release branch demands a Ready target before
	// the source leaves the plan; a target still Creating keeps both
	// pinned even though the source references it.
	surge := int32(1)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseMigrating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate, SurgeIndex: &surge}},
		{Index: 1, Phase: types.InstancePhaseCreating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationMigrate}},
	}
	if diff := cmp.Diff([]int32{0, 1}, instancePlanIndices(instances, 1)); diff != "" {
		t.Fatalf("a not-yet-Ready migration target must keep the source pinned (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_GangSurgeDrainReleaseDemandsExactTargetProof(t *testing.T) {
	// A gang source in the durable drain step is released only when the
	// referenced target is a settled Ready instance promoted on the
	// operation's EXACT pinned revision. Any weaker target state keeps
	// the pair pinned.
	newRev, oldRev := "comp-rev-newaaaa", "comp-rev-oldbbbb"
	zero := int32(0)
	source := types.InstanceStatus{Index: 1, Phase: types.InstancePhaseUpdating,
		RunningRevision: oldRev, TargetRevision: newRev,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain,
			SurgeIndex: &zero, TargetRevision: newRev}}
	sibling := types.InstanceStatus{Index: 2, Phase: types.InstancePhaseReady, RunningRevision: newRev}

	tests := []struct {
		name   string
		target types.InstanceStatus
	}{
		{
			name: "target promoted on a different revision",
			target: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady,
				RunningRevision: "comp-rev-otherccc"},
		},
		{
			name: "target still carrying an operation",
			target: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: newRev,
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget,
					TargetRevision: newRev}},
		},
		{
			name: "target with a pending TargetRevision",
			target: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady,
				RunningRevision: newRev, TargetRevision: newRev},
		},
		{
			name: "target not yet Ready",
			target: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseCreating,
				RunningRevision: newRev},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			instances := []types.InstanceStatus{test.target, source, sibling}
			if diff := cmp.Diff([]int32{0, 1, 2}, instancePlanIndices(instances, 2)); diff != "" {
				t.Fatalf("unproven target must keep the drain-step source pinned (-want +got):\n%s", diff)
			}
		})
	}
}

func TestInstancePlanIndices_GangSurgeDrainMissingTargetKeepsSourcePinned(t *testing.T) {
	// A drain-step source whose referenced target status vanished has no
	// promotion proof: the source must stay in the plan so the update
	// state machine can recover, and the dangling reference must not
	// materialize a phantom index.
	missing := int32(5)
	instances := []types.InstanceStatus{
		{Index: 1, Phase: types.InstancePhaseUpdating,
			RunningRevision: "comp-rev-oldbbbb", TargetRevision: "comp-rev-newaaaa",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain,
				SurgeIndex: &missing, TargetRevision: "comp-rev-newaaaa"}},
	}
	if diff := cmp.Diff([]int32{1}, instancePlanIndices(instances, 1)); diff != "" {
		t.Fatalf("missing target must keep the source pinned without inventing its index (-want +got):\n%s", diff)
	}
}

func TestInstancePlanIndices_OccupiedReferencedTargetStaysPinned(t *testing.T) {
	// Conflict recovery: a fresh gang-surge claim can reference an index
	// occupied by an unrelated settled instance. Both the source and the
	// occupant stay in the plan until the update state machine resets the
	// claim — neither may fall into scale-down while the pair is ambiguous.
	occupied := int32(1)
	instances := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseUpdating, TargetRevision: "comp-rev-newaaaa",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: "Surge",
				SurgeIndex: &occupied, TargetRevision: "comp-rev-newaaaa"}},
		{Index: 1, Phase: types.InstancePhaseReady, RunningRevision: "comp-rev-otherccc"},
		{Index: 2, Phase: types.InstancePhaseReady, RunningRevision: "comp-rev-newaaaa"},
	}
	if diff := cmp.Diff([]int32{0, 1, 2}, instancePlanIndices(instances, 2)); diff != "" {
		t.Fatalf("occupied referenced target must stay pinned at replicas=2 (-want +got):\n%s", diff)
	}
	// Under a replica drop the pinned pair still wins; the unreferenced
	// sibling is the scale-down extra.
	if diff := cmp.Diff([]int32{0, 1}, instancePlanIndices(instances, 1)); diff != "" {
		t.Fatalf("occupied referenced target must stay pinned at replicas=1 (-want +got):\n%s", diff)
	}
}
func TestBuildPlan_CarriesMinReadySeconds(t *testing.T) {
	desired := singlePodDesired(1, types.Lifecycle{})
	desired.MinReadySeconds = 20
	plan, err := BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.MinReadySeconds != 20 {
		t.Fatalf("MinReadySeconds: got %d want 20", plan.MinReadySeconds)
	}

	// Unset projects to 0 (Available as soon as Ready); a negative value
	// from a hand-edited source cannot widen the window below zero.
	desired.MinReadySeconds = -7
	plan, err = BuildPlan(types.ComponentEngine, desired, types.WorkloadObservedState{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.MinReadySeconds != 0 {
		t.Fatalf("MinReadySeconds: got %d want 0", plan.MinReadySeconds)
	}
}

// TestResolveInstanceReadyTimeout pins the precedence of the readiness
// backstop: the per-resource lifecycle value wins over the operator's
// configured window, the configured window covers a resource that sets
// none, and zero — neither level supplying one — is the honest
// "unconfigured" answer rather than a number baked into the binary.
func TestResolveInstanceReadyTimeout(t *testing.T) {
	tests := []struct {
		name       string
		spec       *metav1.Duration
		configured time.Duration
		want       time.Duration
	}{
		{"spec wins over config", &metav1.Duration{Duration: 5 * time.Minute}, 30 * time.Minute, 5 * time.Minute},
		{"spec alone", &metav1.Duration{Duration: 5 * time.Minute}, 0, 5 * time.Minute},
		{"config covers an unset spec", nil, 30 * time.Minute, 30 * time.Minute},
		{"neither is set", nil, 0, 0},
		{"non-positive spec falls through to config", &metav1.Duration{}, 30 * time.Minute, 30 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResolveInstanceReadyTimeout(tt.spec, tt.configured); got != tt.want {
				t.Errorf("ResolveInstanceReadyTimeout: got %v want %v", got, tt.want)
			}
		})
	}
}
