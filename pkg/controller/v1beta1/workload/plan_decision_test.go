package workload_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Plan is the pure decision layer: the tests below assert the Decision
// it produces for each precedence branch — which pass-level actions are
// selected, in what order — and that producing it performs ZERO
// mutations (every ReconcileInput mutation callback is wired to a
// recorder that fails the test on invocation).

// forbidMutations wires every mutation callback on input to a t.Error
// recorder, so any write attempted during Plan fails the test.
func forbidMutations(t *testing.T, input *types.ReconcileInput) {
	t.Helper()
	input.MutateInstance = func(_ context.Context, idx int32, _ func(*types.InstanceStatus) bool) error {
		t.Errorf("Plan must not call MutateInstance (idx=%d)", idx)
		return nil
	}
	input.ApplyInstanceMutations = func(_ context.Context, muts []types.InstanceMutation) error {
		t.Errorf("Plan must not call ApplyInstanceMutations (%d mutations)", len(muts))
		return nil
	}
	input.RemoveInstance = func(_ context.Context, idx int32) (bool, error) {
		t.Errorf("Plan must not call RemoveInstance (idx=%d)", idx)
		return false, nil
	}
	input.WriteAggregateCondition = func(_ context.Context, cond metav1.Condition) error {
		t.Errorf("Plan must not call WriteAggregateCondition (%s)", cond.Type)
		return nil
	}
	input.WarnInstanceFailed = func(idx int32, _, _ string) {
		t.Errorf("Plan must not call WarnInstanceFailed (idx=%d)", idx)
	}
	input.WarnRetryHeld = func(rev string, _ int32, _ string) {
		t.Errorf("Plan must not call WarnRetryHeld (%s)", rev)
	}
	input.MutateMigration = func(_ context.Context, uuid string, _ func(*types.MigrationRecord) bool) error {
		t.Errorf("Plan must not call MutateMigration (%s)", uuid)
		return nil
	}
	input.AppendMigration = func(_ context.Context, rec types.MigrationRecord) error {
		t.Errorf("Plan must not call AppendMigration (%s)", rec.RequestUUID)
		return nil
	}
	input.MutateRetryBlock = func(_ context.Context, rev string, _ func(*types.RetryBlock) types.RetryBlockDisposition) error {
		t.Errorf("Plan must not call MutateRetryBlock (%s)", rev)
		return nil
	}
	input.UpdateGate = func(_ types.UpdateStrategyType, _, _ int32) (bool, types.RolloutHoldGate, string) {
		t.Error("Plan must not consult UpdateGate (Execute owns the consult)")
		return true, "", ""
	}
}

// planSnapshot builds a snapshot over pre-bucketed pods (both read
// sources) so Plan needs no client.
func planSnapshot(input types.ReconcileInput, byIdx map[int32][]*corev1.Pod) *workload.ObservedSnapshot {
	return workload.SnapshotWithPodsForTest(input, byIdx)
}

// planSnapshotWithRevisions is planSnapshot with the running revisions'
// recorded PodSpecs supplied by name, for a Plan that judges a single-pod
// in-place start's diff.
func planSnapshotWithRevisions(input types.ReconcileInput, byIdx map[int32][]*corev1.Pod, specs map[string]*corev1.PodSpec) *workload.ObservedSnapshot {
	return workload.SnapshotWithRevisionsForTest(input, byIdx, specs)
}

// planOrFail runs Plan (nil target) and fails the test on error.
func planOrFail(t *testing.T, input types.ReconcileInput, plan types.ComponentPlan, snapshot *workload.ObservedSnapshot) workload.Decision {
	t.Helper()
	return planTargetOrFail(t, input, plan, nil, snapshot)
}

// planTargetOrFail runs Plan against a target ControllerRevision and
// fails the test on error.
func planTargetOrFail(t *testing.T, input types.ReconcileInput, plan types.ComponentPlan, target *appsv1.ControllerRevision, snapshot *workload.ObservedSnapshot) workload.Decision {
	t.Helper()
	d, err := workload.Plan(context.Background(), input, plan, target, snapshot)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	return d
}

// actionKinds projects the Decision's action kinds in order.
func actionKinds(d workload.Decision) []workload.ActionKind {
	kinds := make([]workload.ActionKind, 0, len(d.Actions))
	for _, a := range d.Actions {
		kinds = append(kinds, a.Kind)
	}
	return kinds
}

// kindsEqual compares two ordered ActionKind slices.
func kindsEqual(got, want []workload.ActionKind) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// findAction returns the first action of the given kind, or nil.
func findAction(d workload.Decision, kind workload.ActionKind) *workload.PlannedAction {
	for i := range d.Actions {
		if d.Actions[i].Kind == kind {
			return &d.Actions[i]
		}
	}
	return nil
}

// TestPlan_ScaleDown_SelectsExtras asserts an observed index the plan
// no longer covers is selected for scale-down, and nothing is selected
// when observation matches the plan.
func TestPlan_ScaleDown_SelectsExtras(t *testing.T) {
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseReady},
		{Index: 1, Phase: types.InstancePhaseReady}, // extra
	}
	plan := minimalPlan() // covers only index 0

	d := planOrFail(t, in, plan, planSnapshot(in, nil))
	sd := findAction(d, workload.ActionScaleDown)
	if sd == nil {
		t.Fatalf("expected a ScaleDown action, got %v", actionKinds(d))
	}
	if len(sd.Extras) != 1 || sd.Extras[0] != 1 {
		t.Errorf("ScaleDown extras = %v, want [1]", sd.Extras)
	}

	// Converged shape: no extras → no ScaleDown action.
	in.ObservedState.InstanceStatuses = in.ObservedState.InstanceStatuses[:1]
	d = planOrFail(t, in, plan, planSnapshot(in, nil))
	if findAction(d, workload.ActionScaleDown) != nil {
		t.Errorf("converged plan must not select ScaleDown, got %v", actionKinds(d))
	}
}

func TestPlan_DeleteOwnedDesiredIndexFinishesBeforeRecreate(t *testing.T) {
	input := minimalInput(t)
	forbidMutations(t, &input)
	plan := minimalPlan()
	input.ObservedState.InstanceStatuses = []types.InstanceStatus{{
		Index: 0, Incarnation: 3, Phase: types.InstancePhaseDeleting,
		Operation: &types.InstanceOperation{ID: "delete-0", Type: types.InstanceOperationDelete, Step: "Drain"},
	}}
	decision := planOrFail(t, input, plan, planSnapshot(input, nil))
	if len(decision.Actions) == 0 || decision.Actions[0].Kind != workload.ActionScaleDown {
		t.Fatalf("actions = %v, want ScaleDown first", actionKinds(decision))
	}
	if len(decision.Actions[0].Extras) != 0 {
		t.Fatalf("desired Delete-owned index must not be reclassified as a fresh extra: %v", decision.Actions[0].Extras)
	}
}

// TestPlan_Restart_SelectsTriggeredInstances asserts the restart
// selection: a Ready Instance whose live pod count is below desired is
// selected (with the trigger reason), healthy Instances are not, and
// the pass is skipped entirely for non-RecreateInstance policies.
func TestPlan_Restart_SelectsTriggeredInstances(t *testing.T) {
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady},
	}
	plan := types.ComponentPlan{
		Component:     types.ComponentEngine,
		Replicas:      2,
		RestartPolicy: types.RestartPolicyRecreateInstance,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	// Index 0 has its pod; index 1 lost its pod (restart trigger).
	pods := map[int32][]*corev1.Pod{
		0: {enginePod("llama-70b", "prod", 0)},
	}

	d := planOrFail(t, in, plan, planSnapshot(in, pods))
	ra := findAction(d, workload.ActionRestart)
	if ra == nil {
		t.Fatalf("expected a Restart action, got %v", actionKinds(d))
	}
	if len(ra.Restarts) != 1 || ra.Restarts[0].Instance.Index != 1 {
		t.Fatalf("restart selection = %+v, want index 1 only", ra.Restarts)
	}
	if ra.Restarts[0].Reason == "" {
		t.Errorf("restart selection must carry the trigger reason")
	}

	// Same shape without the RecreateInstance policy: no restart pass.
	plan.RestartPolicy = ""
	d = planOrFail(t, in, plan, planSnapshot(in, pods))
	if findAction(d, workload.ActionRestart) != nil {
		t.Errorf("restart must not be planned without RestartPolicyRecreateInstance, got %v", actionKinds(d))
	}
}

// TestPlan_ScaleDownPrecedesRestart asserts the precedence ordering:
// when both an extra index and a restart trigger exist, ScaleDown is
// planned before Restart.
func TestPlan_ScaleDownPrecedesRestart(t *testing.T) {
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady}, // pod lost → restart
		{Index: 5, Incarnation: 1, Phase: types.InstancePhaseReady}, // extra → scale-down
	}
	plan := minimalPlan()
	plan.RestartPolicy = types.RestartPolicyRecreateInstance

	d := planOrFail(t, in, plan, planSnapshot(in, nil))
	want := []workload.ActionKind{workload.ActionScaleDown, workload.ActionRestart, workload.ActionCreate}
	if !kindsEqual(actionKinds(d), want) {
		t.Errorf("decision = %v, want %v", actionKinds(d), want)
	}
}

// TestPlan_FullPrecedenceOrder pins the complete precedence: with
// every pass triggered at once, the Decision lists scale-down >
// restart > migration expiry > migration > update > create, in that
// order. Selection is not execution — the executor stops at the first
// action whose op outcome requires a requeue, so a later-listed
// selection may not run this reconcile.
func TestPlan_FullPrecedenceOrder(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	target := updateTarget()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		// Prior revision → update trigger. Its pod is present, so nothing
		// else claims it.
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		// Runs the target and lost its pod → restart trigger: a repair is
		// selected only for a row whose revision is still the target.
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: target.Name},
		// Extra → scale-down.
		{Index: 9, Incarnation: 1, Phase: types.InstancePhaseReady},
	}
	in.ObservedState.Migrations = []types.MigrationRecord{
		{RequestUUID: "u-expired", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseAccepted, SourceInstance: 0,
			Deadline: metav1.NewTime(now.Add(-time.Minute))},
		{RequestUUID: "u-drive", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseAccepted, SourceInstance: 0,
			StartedAt: metav1.NewTime(now.Add(-time.Hour))},
	}
	plan := types.ComponentPlan{
		Component:     types.ComponentEngine,
		Replicas:      2,
		RestartPolicy: types.RestartPolicyRecreateInstance,
		MigrationMode: types.MigrationModeAuto,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	pods := map[int32][]*corev1.Pod{0: {enginePod("llama-70b", "prod", 0)}}

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, pods))
	want := []workload.ActionKind{
		workload.ActionScaleDown,
		workload.ActionRestart,
		workload.ActionMigrateExpiry,
		workload.ActionMigrate,
		workload.ActionUpdate,
		workload.ActionCreate,
	}
	if !kindsEqual(actionKinds(d), want) {
		t.Errorf("decision = %v, want %v", actionKinds(d), want)
	}
	if ra := findAction(d, workload.ActionRestart); len(ra.Restarts) != 1 || ra.Restarts[0].Instance.Index != 1 {
		t.Errorf("restart selection = %+v, want index 1 only", ra.Restarts)
	}
	if !d.Escalate {
		t.Errorf("non-paused decision must enable escalation")
	}
}

// TestPlan_Create_AlwaysPlannedUnlessPaused asserts the create pass
// closes every non-paused decision, last in precedence.
func TestPlan_Create_AlwaysPlannedUnlessPaused(t *testing.T) {
	in := minimalInput(t)
	forbidMutations(t, &in)
	plan := minimalPlan()

	d := planOrFail(t, in, plan, planSnapshot(in, nil))
	kinds := actionKinds(d)
	if len(kinds) == 0 || kinds[len(kinds)-1] != workload.ActionCreate {
		t.Errorf("decision = %v, want Create last", kinds)
	}

	plan.Paused = true
	d = planOrFail(t, in, plan, planSnapshot(in, nil))
	if findAction(d, workload.ActionCreate) != nil {
		t.Errorf("paused decision must not plan Create, got %v", actionKinds(d))
	}
}

// TestPlan_MigrateExpiry_SelectedFromDeadline asserts the expiry
// selection: a non-terminal Manual record past its Deadline plans
// MigrateExpiry even when MigrationMode=Never (a mode flip must not
// strand the record), ordered before the drive action; a record within
// its Deadline plans no expiry.
func TestPlan_MigrateExpiry_SelectedFromDeadline(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseReady},
	}
	in.ObservedState.Migrations = []types.MigrationRecord{{
		RequestUUID: "u-expired", Trigger: types.MigrationTriggerManual,
		Phase: types.MigrationPhaseAccepted, SourceInstance: 0,
		Deadline: metav1.NewTime(now.Add(-time.Minute)),
	}}
	plan := minimalPlan()
	plan.MigrationMode = types.MigrationModeNever

	d := planOrFail(t, in, plan, planSnapshot(in, nil))
	if findAction(d, workload.ActionMigrateExpiry) == nil {
		t.Errorf("expired record must plan MigrateExpiry, got %v", actionKinds(d))
	}
	if findAction(d, workload.ActionMigrate) != nil {
		t.Errorf("MigrationMode=Never must not plan a Migrate drive, got %v", actionKinds(d))
	}

	// Same record still within its Deadline: no expiry.
	in.ObservedState.Migrations[0].Deadline = metav1.NewTime(now.Add(time.Minute))
	d = planOrFail(t, in, plan, planSnapshot(in, nil))
	if findAction(d, workload.ActionMigrateExpiry) != nil {
		t.Errorf("unexpired record must not plan MigrateExpiry, got %v", actionKinds(d))
	}
}

// TestPlan_Migrate_SelectsOldestManualRecord asserts the drive
// selection: terminal and Auto records are never work; the oldest
// non-terminal Manual record is selected; expiry precedes the drive.
func TestPlan_Migrate_SelectsOldestManualRecord(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseReady},
	}
	in.ObservedState.Migrations = []types.MigrationRecord{
		{RequestUUID: "u-done", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseCompleted, SourceInstance: 0},
		{RequestUUID: "u-auto", Trigger: types.MigrationTriggerAuto,
			Phase: types.MigrationPhaseRelocated, SourceInstance: 0},
		{RequestUUID: "u-newer", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseAccepted, SourceInstance: 0,
			StartedAt: metav1.NewTime(now.Add(-time.Minute)),
			// Expired: also plans the expiry action ahead of the drive.
			Deadline: metav1.NewTime(now.Add(-time.Second))},
		{RequestUUID: "u-older", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseSurgePending, SourceInstance: 0,
			StartedAt: metav1.NewTime(now.Add(-time.Hour))},
	}
	plan := minimalPlan()
	plan.MigrationMode = types.MigrationModeAuto

	d := planOrFail(t, in, plan, planSnapshot(in, nil))
	kinds := actionKinds(d)
	wantPrefix := []workload.ActionKind{workload.ActionMigrateExpiry, workload.ActionMigrate}
	if len(kinds) < 2 || !kindsEqual(kinds[:2], wantPrefix) {
		t.Fatalf("decision = %v, want prefix %v", kinds, wantPrefix)
	}
	ma := findAction(d, workload.ActionMigrate)
	if ma.Migration == nil || ma.Migration.Head == nil || ma.Migration.Head.RequestUUID != "u-older" || len(ma.Migration.Parked) != 0 {
		t.Errorf("drive selection = %+v, want oldest non-terminal Manual record u-older as the head", ma.Migration)
	}

	// Only terminal/Auto records: nothing to drive.
	in.ObservedState.Migrations = in.ObservedState.Migrations[:2]
	d = planOrFail(t, in, plan, planSnapshot(in, nil))
	if findAction(d, workload.ActionMigrate) != nil || findAction(d, workload.ActionMigrateExpiry) != nil {
		t.Errorf("terminal/Auto records must plan no migration work, got %v", actionKinds(d))
	}
}

// TestPlan_Migrate_ParkedDrainYieldsTheHead: a Draining record whose
// every live source pod is Terminating past its own deletion deadline
// has nothing left for the drive to do but wait on the kubelet, so it
// is tended without holding the dispatch head — the oldest record that
// is not parked is the head, driven in the same pass. A source pod
// still inside its own grace, or not Terminating at all, keeps the head
// as any in-flight record does.
func TestPlan_Migrate_ParkedDrainYieldsTheHead(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	surgeIdx := int32(1)
	migratePin := func(uuid string) *types.InstanceOperation {
		return &types.InstanceOperation{Type: types.InstanceOperationMigrate, Step: "CreateSurge", RequestUUID: uuid}
	}
	records := func() []types.MigrationRecord {
		return []types.MigrationRecord{
			{RequestUUID: "u-queued", Trigger: types.MigrationTriggerManual,
				Phase: types.MigrationPhaseAccepted, SourceInstance: 2, FromNode: "node-b",
				StartedAt: metav1.NewTime(now.Add(-time.Minute)), Deadline: metav1.NewTime(now.Add(30 * time.Minute))},
			{RequestUUID: "u-parked", Trigger: types.MigrationTriggerManual,
				Phase: types.MigrationPhaseDraining, SourceInstance: 0, SurgeInstance: &surgeIdx, FromNode: "node-a",
				StartedAt: metav1.NewTime(now.Add(-time.Hour)), Deadline: metav1.NewTime(now.Add(30 * time.Minute))},
		}
	}
	plan := types.ComponentPlan{
		Component:     types.ComponentEngine,
		Replicas:      2,
		MigrationMode: types.MigrationModeAuto,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	sourcePod := func(deletedAt *time.Time) *corev1.Pod {
		pod := enginePod("llama-70b", "prod", 0)
		pod.Spec.NodeName = "node-a"
		if deletedAt != nil {
			pod.Finalizers = []string{"example.com/hold"}
			ts := metav1.NewTime(*deletedAt)
			pod.DeletionTimestamp = &ts
		}
		return pod
	}
	overdue, inGrace := now.Add(-time.Minute), now.Add(time.Minute)

	for _, tc := range []struct {
		name       string
		pod        *corev1.Pod
		wantParked []string
		wantHead   string
	}{
		{name: "source Terminating past its deadline yields the head", pod: sourcePod(&overdue),
			wantParked: []string{"u-parked"}, wantHead: "u-queued"},
		{name: "source Terminating inside its grace holds the head", pod: sourcePod(&inGrace),
			wantHead: "u-parked"},
		{name: "source not Terminating holds the head", pod: sourcePod(nil),
			wantHead: "u-parked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.Clock = clocktesting.NewFakeClock(now)
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseMigrating, RunningRevision: "prior-rev", Operation: migratePin("u-parked")},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseCreating, Operation: migratePin("u-parked")},
				{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
			}
			in.ObservedState.Migrations = records()
			surgePod := enginePod("llama-70b", "prod", 1)
			queuedSource := enginePod("llama-70b", "prod", 2)
			queuedSource.Spec.NodeName = "node-b"
			pods := map[int32][]*corev1.Pod{0: {tc.pod}, 1: {surgePod}, 2: {queuedSource}}

			d := planOrFail(t, in, plan, planSnapshot(in, pods))
			ma := findAction(d, workload.ActionMigrate)
			if ma == nil || ma.Migration == nil {
				t.Fatalf("no Migrate action planned; actions %v", actionKinds(d))
			}
			var parked []string
			for _, r := range ma.Migration.Parked {
				parked = append(parked, r.RequestUUID)
			}
			if !reflect.DeepEqual(parked, tc.wantParked) {
				t.Fatalf("parked records = %v, want %v", parked, tc.wantParked)
			}
			if ma.Migration.Head == nil || ma.Migration.Head.RequestUUID != tc.wantHead {
				t.Fatalf("head = %+v, want %s", ma.Migration.Head, tc.wantHead)
			}
		})
	}
}

func TestPlan_MigrationSurgeWaitsForUpdateSurge(t *testing.T) {
	minusOne := int32(-1)
	allocated := int32(3)
	tests := []struct {
		name          string
		updateStep    string
		surgeInstance *int32
		wantMigrate   bool
	}{
		{name: "fresh record waits", updateStep: "Surge", wantMigrate: false},
		{name: "legacy fresh record waits", updateStep: "Surge", surgeInstance: &minusOne, wantMigrate: false},
		{name: "allocated record resumes", updateStep: "Surge", surgeInstance: &allocated, wantMigrate: true},
		{name: "non-surge update does not block", updateStep: "InPlace", wantMigrate: true},
		{name: "no update operation", wantMigrate: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := minimalInput(t)
			forbidMutations(t, &input)
			statuses := []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
				{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
			}
			if test.updateStep != "" {
				statuses[0].Phase = types.InstancePhaseUpdating
				statuses[0].Operation = &types.InstanceOperation{
					Type: types.InstanceOperationUpdate,
					Step: test.updateStep,
				}
			}
			input.ObservedState.InstanceStatuses = statuses
			phase := types.MigrationPhaseAccepted
			if test.surgeInstance != nil && *test.surgeInstance >= 0 {
				phase = types.MigrationPhaseSurgePending
			}
			input.ObservedState.Migrations = []types.MigrationRecord{{
				RequestUUID:    "migration-during-update",
				Trigger:        types.MigrationTriggerManual,
				Phase:          phase,
				SourceInstance: 2,
				SurgeInstance:  test.surgeInstance,
			}}
			plan := types.ComponentPlan{
				Component:      types.ComponentEngine,
				Replicas:       3,
				MigrationMode:  types.MigrationModeAuto,
				UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain},
				Instances: []types.InstancePlan{
					{Index: 0, Incarnation: 1},
					{Index: 1, Incarnation: 1},
					{Index: 2, Incarnation: 1},
				},
			}

			decision := planTargetOrFail(t, input, plan, updateTarget(), planSnapshot(input, nil))
			if got := findAction(decision, workload.ActionMigrate) != nil; got != test.wantMigrate {
				t.Errorf("Migrate selected = %v, want %v; actions=%v", got, test.wantMigrate, actionKinds(decision))
			}
			if test.updateStep != "" && findAction(decision, workload.ActionUpdate) == nil {
				t.Errorf("Update continuation missing; actions=%v", actionKinds(decision))
			}
		})
	}
}

// updateTarget is the target ControllerRevision the update-selection
// tests roll toward.
func updateTarget() *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newtarget", Namespace: "prod"},
	}
}

func TestPlan_RemovableInlineFieldsDoNotChangeDecision(t *testing.T) {
	target := updateTarget()
	plan := types.ComponentPlan{
		Component:     types.ComponentEngine,
		Replicas:      1,
		RestartPolicy: types.RestartPolicyRecreateInstance,
		Instances: []types.InstancePlan{{
			Index: 0, Incarnation: 1,
			Runners: []types.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		}},
	}
	podA := enginePod("llama-70b", "prod", 0)
	podB := enginePod("llama-70b", "prod", 0)
	podB.Name += "-worker"
	staleCachedPod := podA.DeepCopy()
	staleCachedPod.Spec.Containers[0].Image = "test:v0"
	cached := map[int32][]*corev1.Pod{0: {staleCachedPod}}

	tests := []struct {
		name         string
		live         map[int32][]*corev1.Pod
		inlineFields types.InstanceStatus
		wantActions  []workload.ActionKind
	}{
		{
			name: "persisted observation leads partial live gang",
			live: map[int32][]*corev1.Pod{0: {podA}},
			inlineFields: types.InstanceStatus{
				ReadyPodCount: 2, ScheduledPodCount: 2, NodesOccupied: []string{"node-a", "node-b"},
			},
			wantActions: []workload.ActionKind{workload.ActionRestart, workload.ActionUpdate, workload.ActionCreate},
		},
		{
			name: "persisted observation trails complete live gang",
			live: map[int32][]*corev1.Pod{0: {podA, podB}},
			inlineFields: types.InstanceStatus{
				ReadyPodCount: 1, ScheduledPodCount: 1, NodesOccupied: []string{"node-a"},
			},
			wantActions: []workload.ActionKind{workload.ActionUpdate, workload.ActionCreate},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			withoutFields := minimalInput(t)
			forbidMutations(t, &withoutFields)
			withoutFields.ObservedState.InstanceStatuses = []types.InstanceStatus{{
				Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 2,
			}}
			withFields := minimalInput(t)
			forbidMutations(t, &withFields)
			row := withoutFields.ObservedState.InstanceStatuses[0]
			row.ReadyPodCount = test.inlineFields.ReadyPodCount
			row.ScheduledPodCount = test.inlineFields.ScheduledPodCount
			row.NodesOccupied = test.inlineFields.NodesOccupied
			withFields.ObservedState.InstanceStatuses = []types.InstanceStatus{row}

			withoutDecision := planTargetOrFail(t, withoutFields, plan, target,
				workload.SnapshotWithDistinctPodsForTest(withoutFields, test.live, cached))
			withDecision := planTargetOrFail(t, withFields, plan, target,
				workload.SnapshotWithDistinctPodsForTest(withFields, test.live, cached))
			if !reflect.DeepEqual(withoutDecision, withDecision) {
				t.Fatalf("removable fields changed the full decision\nwithout values: %#v\nwith values:    %#v", withoutDecision, withDecision)
			}
			if got := actionKinds(withoutDecision); !kindsEqual(got, test.wantActions) {
				t.Fatalf("ordered actions = %v, want %v", got, test.wantActions)
			}
		})
	}
}

func TestPlanObservationReadFailureFailsClosed(t *testing.T) {
	wantErr := errors.New("pod observation failed")
	funcs := interceptor.Funcs{
		List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
			return wantErr
		},
	}
	failing := fake.NewClientBuilder().WithScheme(makeScheme(t)).WithInterceptorFuncs(funcs).Build()
	healthy := fake.NewClientBuilder().WithScheme(makeScheme(t)).Build()
	tests := []struct {
		name     string
		liveFail bool
		target   *appsv1.ControllerRevision
	}{
		{name: "live restart observation", liveFail: true},
		{name: "cached update observation", target: updateTarget()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := minimalInput(t)
			forbidMutations(t, &input)
			input.ObservedState.InstanceStatuses = []types.InstanceStatus{{
				Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-revision",
			}}
			plan := minimalPlan()
			deps := types.Deps{Client: failing, APIReader: healthy}
			if test.liveFail {
				plan.RestartPolicy = types.RestartPolicyRecreateInstance
				deps.Client, deps.APIReader = healthy, failing
			}
			snapshot := workload.NewObservedSnapshot(deps, input, plan.Component, input.ObservedState.InstanceStatuses)

			decision, err := workload.Plan(context.Background(), input, plan, test.target, snapshot)
			if !errors.Is(err, wantErr) {
				t.Fatalf("Plan error = %v, want %v", err, wantErr)
			}
			if !reflect.DeepEqual(decision, workload.Decision{}) {
				t.Fatalf("failed observation returned a partial decision: %#v", decision)
			}
		})
	}
}

// TestPlan_Update_SelectsTriggeredInstances asserts the update
// selection: an Instance on a prior revision is a fresh start, a
// mid-update Instance is a continuation, an Instance already on the
// target is not listed, and a nil target plans no update pass at all.
func TestPlan_Update_SelectsTriggeredInstances(t *testing.T) {
	target := updateTarget()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "prior-rev"},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: target.Name},
	}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  3,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil {
		t.Fatalf("expected an Update action, got %v", actionKinds(d))
	}
	items := ua.Update.Items
	if len(items) != 2 ||
		items[0].Instance.Index != 0 || !items[0].StartingFresh ||
		items[1].Instance.Index != 1 || items[1].StartingFresh {
		t.Errorf("update items = %+v, want [{0 fresh} {1 continuation}]", items)
	}
	// Empty strategy Type resolves to the SurgeThenDrain default; nil
	// RollingUpdate leaves both per-Component budgets uncapped.
	if ua.Update.Strategy != types.UpdateStrategySurgeThenDrain {
		t.Errorf("strategy = %q, want SurgeThenDrain default", ua.Update.Strategy)
	}
	if ua.Update.SurgeBudget != escalation.BudgetNoLimit || ua.Update.UnavailBudget != escalation.BudgetNoLimit {
		t.Errorf("budgets = (%d, %d), want BudgetNoLimit for nil RollingUpdate",
			ua.Update.SurgeBudget, ua.Update.UnavailBudget)
	}

	// Nil target: the update pass is not planned.
	d = planOrFail(t, in, plan, planSnapshot(in, nil))
	if findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("nil target must not plan an update pass, got %v", actionKinds(d))
	}
}

// TestPlan_Update_RetryBlockWait asserts a not-yet-due Backoff
// RetryBlock for the target denies the fresh start and surfaces the
// wake-up as Decision.RequeueAfter.
func TestPlan_Update_RetryBlockWait(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	target := updateTarget()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	wake := metav1.NewTime(now.Add(30 * time.Second))
	in.ObservedState.RetryBlocks = []types.RetryBlock{{
		TargetRevision: target.Name,
		State:          types.RetryBlockBackoff,
		NextRetryAt:    &wake,
	}}
	plan := minimalPlan()

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	if findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("a not-yet-due Backoff block must deny the fresh start, got %v", actionKinds(d))
	}
	if d.RequeueAfter != 30*time.Second {
		t.Errorf("RequeueAfter = %v, want 30s (the block's wake-up)", d.RequeueAfter)
	}
}

// TestPlan_Update_IdleRollReportsNoHold pins when Plan marks the update
// stage idle (Decision.UpdateIdle), which is what clears a recorded hold
// once the roll has nothing left to do: every planned Instance runs the
// target with nothing in flight, a parked Instance included, or a
// partition holds every candidate. A fresh start plans the pass instead;
// a same-target block the ladder still holds, whether a candidate reaches
// the trigger or a partition holds every one back, leaves the ladder's
// hold to the status writer; a pause and a nil target leave the hold
// standing. None of those is idle.
func TestPlan_Update_IdleRollReportsNoHold(t *testing.T) {
	target := updateTarget()
	plan := minimalPlan()
	rows := func(phase types.InstancePhase, revision string) types.ReconcileInput {
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: phase, RunningRevision: revision},
		}
		return in
	}

	in := rows(types.InstancePhaseReady, target.Name)
	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	if !d.UpdateIdle || findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("every Instance on the target with nothing in flight must be idle with no update pass: idle=%t actions=%v", d.UpdateIdle, actionKinds(d))
	}

	in = rows(types.InstancePhaseFailed, target.Name)
	d = planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	if !d.UpdateIdle || findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("an Instance parked on the target leaves the roll idle: idle=%t actions=%v", d.UpdateIdle, actionKinds(d))
	}

	partition := int32(1)
	staged := minimalPlan()
	staged.UpdateStrategy = types.UpdateStrategy{
		Type:          types.UpdateStrategySurgeThenDrain,
		RollingUpdate: &types.RollingUpdate{Partition: &partition},
	}
	in = rows(types.InstancePhaseReady, "prior-rev")
	d = planTargetOrFail(t, in, staged, target, planSnapshot(in, nil))
	if !d.UpdateIdle || findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("a partition holding every candidate leaves the roll idle: idle=%t actions=%v", d.UpdateIdle, actionKinds(d))
	}

	// The same partition under a same-target block the ladder still
	// holds: no candidate reaches the trigger, but the ladder gave up on
	// the revision and the hold must keep saying so. Not idle for a Held
	// block or a Backoff not yet due; idle once the Backoff is due, since
	// the ladder then admits a start and holds nothing.
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	in = rows(types.InstancePhaseReady, "prior-rev")
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.RetryBlocks = []types.RetryBlock{{TargetRevision: target.Name, State: types.RetryBlockHeld}}
	d = planTargetOrFail(t, in, staged, target, planSnapshot(in, nil))
	if d.UpdateIdle || findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("a partition holding every candidate under a same-target Held block is the ladder's to report, not idle: idle=%t actions=%v", d.UpdateIdle, actionKinds(d))
	}
	notDue := metav1.NewTime(now.Add(time.Minute))
	in.ObservedState.RetryBlocks = []types.RetryBlock{{TargetRevision: target.Name, State: types.RetryBlockBackoff, NextRetryAt: &notDue}}
	d = planTargetOrFail(t, in, staged, target, planSnapshot(in, nil))
	if d.UpdateIdle || findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("a partition holding every candidate under a Backoff not yet due is the ladder's to report, not idle: idle=%t actions=%v", d.UpdateIdle, actionKinds(d))
	}
	due := metav1.NewTime(now.Add(-time.Second))
	in.ObservedState.RetryBlocks = []types.RetryBlock{{TargetRevision: target.Name, State: types.RetryBlockBackoff, NextRetryAt: &due}}
	d = planTargetOrFail(t, in, staged, target, planSnapshot(in, nil))
	if !d.UpdateIdle || findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("a partition holding every candidate under a due Backoff leaves the roll idle: idle=%t actions=%v", d.UpdateIdle, actionKinds(d))
	}

	in = rows(types.InstancePhaseReady, "prior-rev")
	d = planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	if d.UpdateIdle || findAction(d, workload.ActionUpdate) == nil {
		t.Errorf("a fresh start plans the update pass and is not idle: idle=%t actions=%v", d.UpdateIdle, actionKinds(d))
	}

	in.ObservedState.RetryBlocks = []types.RetryBlock{{TargetRevision: target.Name, State: types.RetryBlockHeld}}
	d = planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	if d.UpdateIdle || findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("a start the target's Held block denies is the ladder's to report, not idle: idle=%t actions=%v", d.UpdateIdle, actionKinds(d))
	}

	paused := minimalPlan()
	paused.Paused = true
	in = rows(types.InstancePhaseReady, target.Name)
	d = planTargetOrFail(t, in, paused, target, planSnapshot(in, nil))
	if d.UpdateIdle {
		t.Errorf("a paused Component plans no update pass and is not idle: actions=%v", actionKinds(d))
	}

	in = rows(types.InstancePhaseReady, target.Name)
	d = planOrFail(t, in, plan, planSnapshot(in, nil))
	if d.UpdateIdle {
		t.Errorf("a nil target plans no update pass and is not idle: actions=%v", actionKinds(d))
	}
}

// TestPlan_Update_CrashedSetBackInRotationLeavesTheRollIdle pins the
// standing hold's boundary on a roll with nothing left to start: an
// Instance on the target whose promoted set crashed and was rebuilt holds
// the roll while the rebuilt set is out of rotation, and holds nothing
// once every pod of it is Ready and serving, inside the window it has yet
// to hold Ready for. The window paces a further start at the admission; a
// roll with none to start has nothing to stand behind, so the update
// stage reads idle and a recorded hold clears. A block the ladder holds
// for the revision is the ladder's hold either way. Under SurgeThenDrain
// maxUnavailable 0 and RecreatePod maxUnavailable 1.
func TestPlan_Update_CrashedSetBackInRotationLeavesTheRollIdle(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	target := updateTarget()
	failedAt := metav1.NewTime(now.Add(-40 * time.Second))
	readyAgain := metav1.NewTime(now.Add(-10 * time.Second))
	// rebuilt is the pod the row built right after its set's failure, Ready
	// again for less than the window or still out of rotation.
	rebuilt := func(inRotation bool) *corev1.Pod {
		pod := liveEnginePod("llama-70b", "prod", 0, 0, "newtarget", failedAt.Add(5*time.Second))
		pod.Status.Phase = corev1.PodRunning
		ready := corev1.ConditionTrue
		if !inRotation {
			ready = corev1.ConditionFalse
		}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "main", Ready: inRotation,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(readyAgain.Add(-5 * time.Second))}},
		}}
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: ready, LastTransitionTime: readyAgain},
			{Type: corev1.PodReady, Status: ready, LastTransitionTime: readyAgain},
			{Type: query.ServingConditionType, Status: ready},
		}
		return pod
	}
	one, zero := intstr.FromInt32(1), intstr.FromInt32(0)
	strategies := []struct {
		name     string
		strategy types.UpdateStrategy
	}{
		{name: "SurgeThenDrain maxUnavailable 0", strategy: types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain, RollingUpdate: &types.RollingUpdate{MaxSurge: &one, MaxUnavailable: &zero}}},
		{name: "RecreatePod maxUnavailable 1", strategy: types.UpdateStrategy{Type: types.UpdateStrategyRecreatePod, RollingUpdate: &types.RollingUpdate{MaxSurge: &zero, MaxUnavailable: &one}}},
	}
	for _, sc := range strategies {
		t.Run(sc.name, func(t *testing.T) {
			plan := minimalPlan()
			plan.UpdateStrategy = sc.strategy
			observe := func(pod *corev1.Pod, blocks ...types.RetryBlock) (types.ReconcileInput, *workload.ObservedSnapshot) {
				in := minimalInput(t)
				forbidMutations(t, &in)
				in.Clock = clocktesting.NewFakeClock(now)
				in.StuckPodGrace = time.Minute
				in.ObservedState.InstanceStatuses = []types.InstanceStatus{{
					Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: target.Name,
					ReadySince: &readyAgain, PodCount: 1, ServingPodCount: 1,
					LastFailure: &types.InstanceTermination{PodName: pod.Name, ContainerName: "main", Reason: "Error", Time: failedAt},
				}}
				in.ObservedState.RetryBlocks = blocks
				return in, planSnapshot(in, map[int32][]*corev1.Pod{0: {pod}})
			}

			in, snapshot := observe(rebuilt(true))
			d := planTargetOrFail(t, in, plan, target, snapshot)
			if !d.UpdateIdle || d.StandingHold != nil || findAction(d, workload.ActionUpdate) != nil {
				t.Errorf("a rebuilt set back in rotation inside its window leaves a roll with nothing to start idle: idle=%t standing=%+v actions=%v", d.UpdateIdle, d.StandingHold, actionKinds(d))
			}

			in, snapshot = observe(rebuilt(false))
			d = planTargetOrFail(t, in, plan, target, snapshot)
			if d.UpdateIdle || d.StandingHold == nil || d.StandingHold.Gate != types.RolloutHoldGateBudget || d.StandingHold.Target != target.Name || !strings.Contains(d.StandingHold.Reason, "Instance 0") {
				t.Errorf("a rebuilt set out of rotation holds the roll at the budget and names itself: idle=%t standing=%+v", d.UpdateIdle, d.StandingHold)
			}

			in, snapshot = observe(rebuilt(true), types.RetryBlock{TargetRevision: target.Name, State: types.RetryBlockRetryInProgress, AttemptsStarted: 1, Reason: "Error"})
			d = planTargetOrFail(t, in, plan, target, snapshot)
			if d.UpdateIdle || d.LadderHold == nil || d.LadderHold.Gate != types.RolloutHoldGateRetryBlock || d.LadderHold.Target != target.Name {
				t.Errorf("a rebuilt set proving itself under the ladder's attempt is the ladder's to report: idle=%t ladder=%+v", d.UpdateIdle, d.LadderHold)
			}
		})
	}
}

// TestPlan_Update_HeldByPartition asserts the canary hold: an Instance
// below the partition is not selected, unless it is already mid-surge
// (it must finish, not strand its surge pod).
func TestPlan_Update_HeldByPartition(t *testing.T) {
	target := updateTarget()
	partition := int32(1)
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{
			Type:          types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{Partition: &partition},
		},
	}

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil {
		t.Fatalf("expected an Update action, got %v", actionKinds(d))
	}
	if len(ua.Update.Items) != 1 || ua.Update.Items[0].Instance.Index != 1 {
		t.Errorf("update items = %+v, want index 1 only (index 0 held)", ua.Update.Items)
	}

	// An Instance ALREADY mid-surge is never a hold candidate — it is
	// selected so it can finish (a held mid-surge Instance would strand
	// its surge pod and pin the surge budget). The hold falls to the
	// next old-revision Instance, preserving the Partition count: index
	// 1 is held instead, so exactly one Instance stays on the old
	// revision when the roll settles.
	in.ObservedState.InstanceStatuses[0].Phase = types.InstancePhaseUpdating
	d = planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	ua = findAction(d, workload.ActionUpdate)
	if ua == nil || len(ua.Update.Items) != 1 || ua.Update.Items[0].Instance.Index != 0 {
		t.Errorf("mid-surge Instance must stay selected and the hold must fall to index 1; got %+v", ua)
	}
}

// TestPlan_Update_PacingPartitionPrecedence asserts where the hold reads
// its partition: the projected pacing partition (a canary step) holds
// even when the user's rollingUpdate carries none, an explicit pacing 0
// releases every Instance over a user partition, and a nil pacing
// partition defers to the user's rollingUpdate partition.
func TestPlan_Update_PacingPartitionPrecedence(t *testing.T) {
	target := updateTarget()
	part := func(n int32) *int32 { return &n }
	newPlan := func(ru *types.RollingUpdate) types.ComponentPlan {
		return types.ComponentPlan{
			Component: types.ComponentEngine,
			Replicas:  2,
			Instances: []types.InstancePlan{
				{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
				{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			},
			UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain, RollingUpdate: ru},
		}
	}
	cases := []struct {
		name        string
		pacing      *types.WorkloadPacing
		ru          *types.RollingUpdate
		wantIndices []int32
	}{
		{"pacing holds with no user partition", &types.WorkloadPacing{Partition: part(1)}, nil, []int32{1}},
		{"pacing 0 releases over user partition", &types.WorkloadPacing{Partition: part(0)}, &types.RollingUpdate{Partition: part(1)}, []int32{0, 1}},
		{"nil pacing defers to user partition", &types.WorkloadPacing{}, &types.RollingUpdate{Partition: part(1)}, []int32{1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.DesiredSpec.Pacing = tc.pacing
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
			}
			d := planTargetOrFail(t, in, newPlan(tc.ru), target, planSnapshot(in, nil))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil {
				t.Fatalf("expected an Update action, got %v", actionKinds(d))
			}
			var got []int32
			for _, item := range ua.Update.Items {
				got = append(got, item.Instance.Index)
			}
			if len(got) != len(tc.wantIndices) {
				t.Fatalf("update items = %v, want %v", got, tc.wantIndices)
			}
			for i := range got {
				if got[i] != tc.wantIndices[i] {
					t.Fatalf("update items = %v, want %v", got, tc.wantIndices)
				}
			}
		})
	}
}

// TestPlan_Update_PartitionHoldsCountOnSparseIndices asserts the
// partition hold on a SPARSE index set (migration / lowest-unused
// surge allocation leave gaps): Partition counts Instances to hold —
// the lowest-indexed old-revision Instances — not raw index values.
// With old-revision indices {1,2} and Partition=1, exactly the lowest
// (index 1) is held; raw-index comparison would hold nothing and roll
// both.
func TestPlan_Update_PartitionHoldsCountOnSparseIndices(t *testing.T) {
	target := updateTarget()
	partition := int32(1)
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{
			Type:          types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{Partition: &partition},
		},
	}

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil {
		t.Fatalf("expected an Update action, got %v", actionKinds(d))
	}
	if len(ua.Update.Items) != 1 || ua.Update.Items[0].Instance.Index != 2 {
		t.Errorf("update items = %+v, want index 2 only (index 1 held as rank 0)", ua.Update.Items)
	}

	// Partition == replicas holds the whole steady set: no update items.
	partition = 2
	d = planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	if findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("Partition=replicas must hold every steady Instance, got %v", actionKinds(d))
	}
}

// TestPlan_Update_PartitionHoldSurvivesGangSurgeReindex asserts the
// hold is keyed to revision membership, not index position. Gang surge
// allocates the lowest unused index for its replacement, so a roll of
// {1,2} with Partition=1 promotes the target-revision replacement at
// index 0 — BELOW the held Instance at index 1. A positional hold
// would let the replacement steal the held slot and un-hold index 1,
// rolling it too: 100% blast radius under Partition=1 and the staged
// shape (Partition Instances Ready on the old revision) permanently
// unreachable. The on-target Instance is past holding; index 1 must
// stay held.
func TestPlan_Update_PartitionHoldSurvivesGangSurgeReindex(t *testing.T) {
	target := updateTarget()
	partition := int32(1)
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		// The promoted gang-surge replacement: landed at the freed
		// lowest index, already on the target revision.
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: target.Name},
		// The canary hold: still on the old revision.
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
	}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{
			Type:          types.UpdateStrategySurgeThenDrain,
			RollingUpdate: &types.RollingUpdate{Partition: &partition},
		},
	}

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	if ua := findAction(d, workload.ActionUpdate); ua != nil {
		t.Errorf("index 1 must stay held after the replacement re-indexed below it; got update items %+v", ua.Update.Items)
	}
}

// TestPlan_Update_StartingFresh_FailedByOperationType asserts the
// Failed-phase budget exemption is scoped to a preserved UPDATE
// operation — the only shape CurrentUnavailableInFlight charges as
// in-flight. Failed with a non-Update operation (e.g. an expired
// Restart) is not charged, so it must start fresh (budget-gated).
func TestPlan_Update_StartingFresh_FailedByOperationType(t *testing.T) {
	target := updateTarget()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: "Drain"}},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: "Drain"}},
	}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  2,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil {
		t.Fatalf("expected an Update action, got %v", actionKinds(d))
	}
	items := ua.Update.Items
	if len(items) != 2 ||
		items[0].Instance.Index != 0 || !items[0].StartingFresh ||
		items[1].Instance.Index != 1 || items[1].StartingFresh {
		t.Errorf("update items = %+v, want [{0 fresh (Failed+Restart)} {1 continuation (Failed+Update)}]", items)
	}
}

// TestPlan_Update_CoordGateExempt_FailedZeroServing asserts the
// coordination-gate exemption selection on a non-surge strategy: a
// Failed Instance with zero serving pods starts fresh but skips the
// gate consult (its outage is already inside the gate's serving-based
// unavailability count), while a Failed Instance still contributing
// serving pods keeps the consult (its recreate takes real capacity
// offline).
func TestPlan_Update_CoordGateExempt_FailedZeroServing(t *testing.T) {
	target := updateTarget()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: "Drain"}},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			PodCount: 1, ServingPodCount: 1},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev",
			PodCount: 1, ServingPodCount: 1},
	}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  3,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategyRecreatePod},
	}

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil {
		t.Fatalf("expected an Update action, got %v", actionKinds(d))
	}
	items := ua.Update.Items
	if len(items) != 3 {
		t.Fatalf("update items = %+v, want 3", items)
	}
	if !items[0].StartingFresh || !items[0].CoordGateExempt {
		t.Errorf("item 0 (Failed+Restart, zero serving) = %+v, want fresh + gate-exempt", items[0])
	}
	if !items[1].StartingFresh || items[1].CoordGateExempt {
		t.Errorf("item 1 (Failed but serving) = %+v, want fresh + gate-consulted", items[1])
	}
	if !items[2].StartingFresh || items[2].CoordGateExempt {
		t.Errorf("item 2 (healthy) = %+v, want fresh + gate-consulted", items[2])
	}
}

// TestPlan_UpdateCoordGateExemptSurgeKeepsConsult asserts the
// exemption never applies under SurgeThenDrain: the surge-side gates
// count surge pods, not serving loss, and a Failed Instance's
// recreate-via-surge genuinely adds one.
func TestPlan_UpdateCoordGateExemptSurgeKeepsConsult(t *testing.T) {
	target := updateTarget()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: "Drain"}},
	}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  1,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
		UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain},
	}

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil {
		t.Fatalf("expected an Update action, got %v", actionKinds(d))
	}
	if len(ua.Update.Items) != 1 || ua.Update.Items[0].CoordGateExempt {
		t.Errorf("update items = %+v, want one gate-consulted item", ua.Update.Items)
	}
}

// TestPlan_Update_RecreateFallback_GangInPlace asserts the selection
// marks the fresh starts whose in-place strategy runs as a recreate on a
// multi-pod Instance, under either in-place variant, and nothing else
// when the running revision differs from the target by an image only.
// The executor consults the coordination gate for such a start as a
// RecreatePod start: it takes the pods out of rotation before anything
// returns, the capacity loss the gate waives only for a same-pod patch.
func TestPlan_Update_RecreateFallback_GangInPlace(t *testing.T) {
	gang := []types.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	single := []types.RunnerPlan{{Name: "default", Size: 1}}
	cases := []struct {
		name     string
		strategy types.UpdateStrategyType
		runners  []types.RunnerPlan
		want     bool
	}{
		{"gang InPlaceIfPossible", types.UpdateStrategyInPlaceIfPossible, gang, true},
		{"gang InPlaceOnly", types.UpdateStrategyInPlaceOnly, gang, true},
		{"single-pod InPlaceIfPossible", types.UpdateStrategyInPlaceIfPossible, single, false},
		{"single-pod InPlaceOnly", types.UpdateStrategyInPlaceOnly, single, false},
		{"gang RecreatePod", types.UpdateStrategyRecreatePod, gang, false},
		{"gang SurgeThenDrain", types.UpdateStrategySurgeThenDrain, gang, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := updateTarget()
			in := minimalInput(t)
			forbidMutations(t, &in)
			podCount := int32(len(tc.runners))
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev",
					PodCount: podCount, ServingPodCount: podCount},
			}
			plan := types.ComponentPlan{
				Component:      types.ComponentEngine,
				Replicas:       1,
				Instances:      []types.InstancePlan{{Index: 0, Incarnation: 1, Runners: tc.runners}},
				UpdateStrategy: types.UpdateStrategy{Type: tc.strategy},
			}
			running := in.DesiredSpec.PodSpec.DeepCopy()
			running.Containers[0].Image = "test:v0"

			d := planTargetOrFail(t, in, plan, target, planSnapshotWithRevisions(in, nil, map[string]*corev1.PodSpec{"prior-rev": running}))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil {
				t.Fatalf("expected an Update action, got %v", actionKinds(d))
			}
			items := ua.Update.Items
			if len(items) != 1 || !items[0].StartingFresh {
				t.Fatalf("update items = %+v, want one fresh start", items)
			}
			if items[0].RecreateFallback != tc.want {
				t.Errorf("RecreateFallback = %v, want %v", items[0].RecreateFallback, tc.want)
			}
		})
	}
}

// TestPlan_Update_RecreateFallback_SinglePodDiff asserts the selection
// marks a fresh single-pod InPlaceIfPossible start whose diff against the
// running revision exceeds regular-container images, or whose running
// revision is gone, as a recreate fallback: the update op resolves such a
// start to a recreate, which drains the pod before anything returns, so
// the executor consults the coordination gate for it as RecreatePod. An
// image-only diff stays an in-place start, and InPlaceOnly rejects the
// diff rather than falling back. The classification is a snapshot read:
// Plan writes nothing.
func TestPlan_Update_RecreateFallback_SinglePodDiff(t *testing.T) {
	cases := []struct {
		name     string
		strategy types.UpdateStrategyType
		running  func(target *corev1.PodSpec) *corev1.PodSpec // nil: the revision is gone
		want     bool
	}{
		{"InPlaceIfPossible image-only diff", types.UpdateStrategyInPlaceIfPossible, func(target *corev1.PodSpec) *corev1.PodSpec {
			running := target.DeepCopy()
			running.Containers[0].Image = "test:v0"
			return running
		}, false},
		{"InPlaceIfPossible diff beyond images", types.UpdateStrategyInPlaceIfPossible, func(target *corev1.PodSpec) *corev1.PodSpec {
			running := target.DeepCopy()
			running.Containers[0].Env = []corev1.EnvVar{{Name: "MODE", Value: "batch"}}
			return running
		}, true},
		{"InPlaceIfPossible gone running revision", types.UpdateStrategyInPlaceIfPossible, nil, true},
		{"InPlaceOnly diff beyond images", types.UpdateStrategyInPlaceOnly, func(target *corev1.PodSpec) *corev1.PodSpec {
			running := target.DeepCopy()
			running.Containers[0].Env = []corev1.EnvVar{{Name: "MODE", Value: "batch"}}
			return running
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := updateTarget()
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev",
					PodCount: 1, ServingPodCount: 1},
			}
			plan := types.ComponentPlan{
				Component:      types.ComponentEngine,
				Replicas:       1,
				Instances:      []types.InstancePlan{{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}}},
				UpdateStrategy: types.UpdateStrategy{Type: tc.strategy},
			}
			specs := map[string]*corev1.PodSpec{}
			if tc.running != nil {
				specs["prior-rev"] = tc.running(in.DesiredSpec.PodSpec)
			}

			d := planTargetOrFail(t, in, plan, target, planSnapshotWithRevisions(in, nil, specs))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil {
				t.Fatalf("expected an Update action, got %v", actionKinds(d))
			}
			items := ua.Update.Items
			if len(items) != 1 || !items[0].StartingFresh {
				t.Fatalf("update items = %+v, want one fresh start", items)
			}
			if items[0].RecreateFallback != tc.want {
				t.Errorf("RecreateFallback = %v, want %v", items[0].RecreateFallback, tc.want)
			}
		})
	}
}

// TestPlan_Update_AdoptRevision asserts the empty-RunningRevision
// adoption selection: runtime-ready pods already carrying the target
// revision's hash select the backfill stamp as an Item (the write
// itself belongs to the executor — forbidMutations proves Plan never
// performs it).
func TestPlan_Update_AdoptRevision(t *testing.T) {
	target := updateTarget()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady},
	}
	plan := minimalPlan()
	pod := enginePod("llama-70b", "prod", 0)
	pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(target.Name)
	pod.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.ContainersReady, Status: corev1.ConditionTrue,
	}}

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {pod}}))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil {
		t.Fatalf("expected an Update action carrying the adoption, got %v", actionKinds(d))
	}
	if len(ua.Update.Items) != 1 || !ua.Update.Items[0].AdoptRevision {
		t.Errorf("update items = %+v, want a single AdoptRevision item", ua.Update.Items)
	}

	// Same shape with a NOT-runtime-ready pod: no adoption (Ready is only
	// stamped on proof) — the row takes the ordinary roll instead, which
	// is the recovery its strategy resolves.
	pod.Status.Conditions = nil
	d = planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {pod}}))
	ua = findAction(d, workload.ActionUpdate)
	if ua == nil {
		t.Fatalf("an unproven pod set must select the ordinary roll, got %v", actionKinds(d))
	}
	if len(ua.Update.Items) != 1 || ua.Update.Items[0].AdoptRevision {
		t.Errorf("update items = %+v, want a single non-adopting item", ua.Update.Items)
	}
}

// TestPlan_Update_CleanupOnly_RollBackWreckage asserts the wreckage
// scan: an instance the revision-diff trigger declines (target
// == RunningRevision, the corrective roll-back) but that carries a
// live pod on a THIRD revision is selected CleanupOnly — never
// StartingFresh, so it is neither budget-charged nor gated. The same
// instance with only running-revision pods selects nothing.
func TestPlan_Update_CleanupOnly_RollBackWreckage(t *testing.T) {
	target := updateTarget()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: target.Name},
	}
	plan := minimalPlan()

	runningPod := enginePod("llama-70b", "prod", 0)
	runningPod.Labels[query.LabelRevisionHash] = query.RevisionFromName(target.Name).Hash()
	alienPod := enginePod("llama-70b", "prod", 0)
	alienPod.Name += "-alien"
	alienPod.Labels[query.LabelRevisionHash] = "deadrev1"

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {runningPod, alienPod}}))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil {
		t.Fatalf("expected an Update action carrying the cleanup, got %v", actionKinds(d))
	}
	if len(ua.Update.Items) != 1 || !ua.Update.Items[0].CleanupOnly ||
		ua.Update.Items[0].StartingFresh || ua.Update.Items[0].Instance.Index != 0 {
		t.Errorf("update items = %+v, want a single CleanupOnly (non-fresh) item for index 0", ua.Update.Items)
	}

	// No alien pod → nothing to clean, no Update action.
	d = planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {runningPod}}))
	if findAction(d, workload.ActionUpdate) != nil {
		t.Errorf("no wreckage must select no update items, got %v", actionKinds(d))
	}

	// Gang shape: a Failed source whose preserved gang-surge operation
	// targets a superseded revision is wreckage even with no alien pod
	// in its own bucket — the abandon continuation must dispatch.
	k := int32(1)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: target.Name,
			Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: "Surge", SurgeIndex: &k,
				TargetRevision: "llama-70b-engine-deadrev1",
			}},
	}
	d = planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {runningPod}}))
	ua = findAction(d, workload.ActionUpdate)
	if ua == nil || len(ua.Update.Items) != 1 || !ua.Update.Items[0].CleanupOnly {
		t.Fatalf("expected a CleanupOnly item for the stranded gang continuation, got %v", actionKinds(d))
	}
}

// TestPlan_LiveSurgeUntouchedByCleanup pins the mid-flight safety of
// the wreckage-cleanup machinery: a LIVE (non-Failed) gang surge with its
// marker correctly pinned — source Op.SurgeIndex referencing the
// marker, surge pods on the pinned revision — is selected as a normal
// update continuation, never CleanupOnly, and the marker is neither
// scale-downed nor selected. Holds both while the surge target IS the
// roll target and after the target moved on mid-surge (the superseded
// redirect owns that, not the wreckage scan).
func TestPlan_LiveSurgeUntouchedByCleanup(t *testing.T) {
	for _, tc := range []struct {
		name       string
		liveTarget *appsv1.ControllerRevision
	}{
		{name: "surge toward the current target", liveTarget: updateTarget()},
		{name: "target moved on mid-surge", liveTarget: &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-newerrev", Namespace: "prod"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pinned := updateTarget()
			in := minimalInput(t)
			forbidMutations(t, &in)
			k := int32(1)
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseUpdating,
					RunningRevision: "llama-70b-engine-priorrev",
					Operation: &types.InstanceOperation{
						Type: types.InstanceOperationUpdate, Step: "Surge",
						SurgeIndex: &k, TargetRevision: pinned.Name,
					}},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseCreating,
					Operation: &types.InstanceOperation{
						Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget,
						TargetRevision: pinned.Name,
					}},
			}
			plan, err := workload.BuildPlan(types.ComponentEngine, in.DesiredSpec, in.ObservedState)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}

			sourcePod := enginePod("llama-70b", "prod", 0)
			surgePod := enginePod("llama-70b", "prod", 1)
			surgePod.Labels[query.LabelRevisionHash] = query.RevisionFromName(pinned.Name).Hash()

			d := planTargetOrFail(t, in, plan, tc.liveTarget, planSnapshot(in, map[int32][]*corev1.Pod{0: {sourcePod}, 1: {surgePod}}))
			if sd := findAction(d, workload.ActionScaleDown); sd != nil {
				t.Fatalf("a live surge pair must not be scale-downed, got extras %v", sd.Extras)
			}
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil {
				t.Fatalf("expected the in-flight surge continuation, got %v", actionKinds(d))
			}
			if len(ua.Update.Items) != 1 {
				t.Fatalf("update items = %+v, want exactly the source continuation", ua.Update.Items)
			}
			item := ua.Update.Items[0]
			if item.Instance.Index != 0 || item.CleanupOnly || item.StartingFresh || item.AdoptRevision {
				t.Errorf("live surge source must be a plain continuation, got %+v", item)
			}
		})
	}
}

// TestPlan_Paused_RepairRunsFleetChangesDoNot pins the pause matrix.
// A standard pause keeps the scale-down selection AND the restart pass
// (repair of existing Instances at their current revision) while
// planning no MigrateExpiry / Migrate / Update / Create work and
// suspending escalation. A frozen pause (PauseFreeze) truncates after
// scale-down — repair is suspended too. RestartPolicy None plans no
// repair under either depth.
func TestPlan_Paused_RepairRunsFleetChangesDoNot(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	// The all-passes fixture: pod-lost + prior-revision Instance
	// (restart + update triggers), an extra index (scale-down), an
	// expired and a drivable Manual migration record. The lost pod is
	// repaired at the prior revision while paused: the roll that would
	// otherwise take the row is what the pause withholds.
	build := func(t *testing.T) (types.ReconcileInput, types.ComponentPlan) {
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.Clock = clocktesting.NewFakeClock(now)
		in.ObservedState.InstanceStatuses = []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
			{Index: 9, Incarnation: 1, Phase: types.InstancePhaseReady}, // extra
		}
		in.ObservedState.Migrations = []types.MigrationRecord{
			{RequestUUID: "u-expired", Trigger: types.MigrationTriggerManual,
				Phase: types.MigrationPhaseAccepted, SourceInstance: 0,
				Deadline: metav1.NewTime(now.Add(-time.Minute))},
			{RequestUUID: "u-drive", Trigger: types.MigrationTriggerManual,
				Phase: types.MigrationPhaseAccepted, SourceInstance: 0,
				StartedAt: metav1.NewTime(now.Add(-time.Hour))},
		}
		plan := minimalPlan()
		plan.Paused = true
		plan.RestartPolicy = types.RestartPolicyRecreateInstance
		plan.MigrationMode = types.MigrationModeAuto
		return in, plan
	}

	t.Run("standard pause plans scale-down and repair only", func(t *testing.T) {
		in, plan := build(t)
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		want := []workload.ActionKind{workload.ActionScaleDown, workload.ActionRestart}
		if !kindsEqual(actionKinds(d), want) {
			t.Errorf("paused decision = %v, want %v", actionKinds(d), want)
		}
		ra := findAction(d, workload.ActionRestart)
		if len(ra.Restarts) != 1 || ra.Restarts[0].Instance.Index != 0 {
			t.Errorf("restart selection = %+v, want index 0 only", ra.Restarts)
		}
		if d.Escalate {
			t.Errorf("paused decision must suspend escalation")
		}
	})

	t.Run("frozen pause truncates after scale-down and the truth pass", func(t *testing.T) {
		in, plan := build(t)
		plan.PauseFreeze = true
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		// Index 0 is Ready with no pods: the freeze suspends the repair
		// that a plain pause would open, so the truth pass demotes it.
		want := []workload.ActionKind{workload.ActionScaleDown, workload.ActionDemote}
		if !kindsEqual(actionKinds(d), want) {
			t.Errorf("frozen decision = %v, want %v", actionKinds(d), want)
		}
		if d.Escalate {
			t.Errorf("frozen decision must suspend escalation")
		}
	})

	t.Run("standard pause with RestartPolicy None plans truth but no repair", func(t *testing.T) {
		in, plan := build(t)
		plan.RestartPolicy = ""
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		// Index 0 is Ready with no pods and no operation: with no restart
		// pass to own it, the truth pass demotes it — even while paused.
		want := []workload.ActionKind{workload.ActionScaleDown, workload.ActionDemote}
		if !kindsEqual(actionKinds(d), want) {
			t.Errorf("paused None-policy decision = %v, want %v", actionKinds(d), want)
		}
		da := findAction(d, workload.ActionDemote)
		if len(da.Demotions) != 1 || da.Demotions[0].Index != 0 {
			t.Errorf("demotions = %+v, want index 0 only (the extra belongs to scale-down)", da.Demotions)
		}
	})

	t.Run("unpause restores escalation and the full pipeline", func(t *testing.T) {
		in, plan := build(t)
		plan.Paused = false
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		if !d.Escalate {
			t.Errorf("non-paused decision must enable escalation")
		}
		for _, k := range []workload.ActionKind{workload.ActionMigrateExpiry, workload.ActionMigrate, workload.ActionUpdate, workload.ActionCreate} {
			if findAction(d, k) == nil {
				t.Errorf("non-paused decision missing %s, got %v", k, actionKinds(d))
			}
		}
	})
}

// maximalPlanInput builds the all-passes-triggerable fixture: an extra
// index (scale-down), a pod-lost Instance (restart), an expired plus a
// drivable Manual migration record, prior-revision Instances (update),
// and a covering plan (create closes every non-paused decision). Every
// mutation callback is wired to the forbidMutations recorder.
func maximalPlanInput(t *testing.T, now time.Time) (types.ReconcileInput, types.ComponentPlan) {
	t.Helper()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.Clock = clocktesting.NewFakeClock(now)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		// Prior revision → update trigger. Index 1 runs the target and lost
		// its pod → restart trigger; a repair is selected only for a row
		// whose revision is still the target.
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: updateTarget().Name},
		// Extra → scale-down.
		{Index: 9, Incarnation: 1, Phase: types.InstancePhaseReady},
	}
	in.ObservedState.Migrations = []types.MigrationRecord{
		{RequestUUID: "u-expired", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseAccepted, SourceInstance: 0,
			Deadline: metav1.NewTime(now.Add(-time.Minute))},
		{RequestUUID: "u-drive", Trigger: types.MigrationTriggerManual,
			Phase: types.MigrationPhaseAccepted, SourceInstance: 0,
			StartedAt: metav1.NewTime(now.Add(-time.Hour))},
	}
	plan := types.ComponentPlan{
		Component:     types.ComponentEngine,
		Replicas:      2,
		RestartPolicy: types.RestartPolicyRecreateInstance,
		MigrationMode: types.MigrationModeAuto,
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	return in, plan
}

// maximalPlanKinds is the complete precedence order the maximal fixture
// must produce — a guard that the fixture really triggers every pass,
// so the purity/determinism assertions on it are not vacuous.
var maximalPlanKinds = []workload.ActionKind{
	workload.ActionScaleDown,
	workload.ActionRestart,
	workload.ActionMigrateExpiry,
	workload.ActionMigrate,
	workload.ActionUpdate,
	workload.ActionCreate,
}

// TestPlan_Determinism_IdenticalInputs asserts Plan is a pure function
// of its inputs: two calls over identical inputs (fake clock, same
// snapshot content) produce deep-equal Decisions, with every pass
// triggered.
func TestPlan_Determinism_IdenticalInputs(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	target := updateTarget()
	in, plan := maximalPlanInput(t, now)
	pods := map[int32][]*corev1.Pod{0: {enginePod("llama-70b", "prod", 0)}}

	d1 := planTargetOrFail(t, in, plan, target, planSnapshot(in, pods))
	d2 := planTargetOrFail(t, in, plan, target, planSnapshot(in, pods))

	if !kindsEqual(actionKinds(d1), maximalPlanKinds) {
		t.Fatalf("maximal fixture decision = %v, want %v (fixture must trigger every pass)",
			actionKinds(d1), maximalPlanKinds)
	}
	if !reflect.DeepEqual(d1, d2) {
		t.Errorf("Plan is not deterministic over identical inputs:\n first = %+v\nsecond = %+v", d1, d2)
	}
}

// TestPlan_MaximalFixture_NoWrites proves the purity contract over the
// full decision surface at once: with every pass triggered, Plan invokes
// ZERO ReconcileInput mutation callbacks (forbidMutations, wired by
// maximalPlanInput) and issues ZERO client writes — the snapshot's pod
// reads route through a write-intercepting client that fails the test on
// any Create/Update/Patch/Delete, plain or subresource.
func TestPlan_MaximalFixture_NoWrites(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	target := updateTarget()
	in, plan := maximalPlanInput(t, now)

	podLists := 0
	forbidden := func(op string, obj client.Object) {
		t.Errorf("Plan must not issue client writes (%s %T %s/%s)", op, obj, obj.GetNamespace(), obj.GetName())
	}
	funcs := interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
			forbidden("Create", obj)
			return nil
		},
		Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
			forbidden("Update", obj)
			return nil
		},
		Delete: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.DeleteOption) error {
			forbidden("Delete", obj)
			return nil
		},
		DeleteAllOf: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.DeleteAllOfOption) error {
			forbidden("DeleteAllOf", obj)
			return nil
		},
		Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
			forbidden("Patch", obj)
			return nil
		},
		SubResourceCreate: func(_ context.Context, _ client.Client, sub string, obj client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
			forbidden("SubResource("+sub+").Create", obj)
			return nil
		},
		SubResourceUpdate: func(_ context.Context, _ client.Client, sub string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
			forbidden("SubResource("+sub+").Update", obj)
			return nil
		},
		SubResourcePatch: func(_ context.Context, _ client.Client, sub string, obj client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
			forbidden("SubResource("+sub+").Patch", obj)
			return nil
		},
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				podLists++
			}
			return cl.List(ctx, list, opts...)
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(makeScheme(t)).
		WithObjects(enginePod("llama-70b", "prod", 0)).
		WithInterceptorFuncs(funcs).
		Build()
	deps := types.Deps{Client: c, APIReader: c}
	snapshot := workload.NewObservedSnapshot(deps, in, plan.Component, in.ObservedState.InstanceStatuses)

	d, err := workload.Plan(context.Background(), in, plan, target, snapshot)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if !kindsEqual(actionKinds(d), maximalPlanKinds) {
		t.Fatalf("maximal fixture decision = %v, want %v (fixture must trigger every pass)",
			actionKinds(d), maximalPlanKinds)
	}
	// Sanity: the snapshot's live (restart) and cached (update) reads
	// both routed through the intercepted client — the zero-writes
	// assertion above actually covered the client surface Plan touches.
	if podLists < 2 {
		t.Errorf("pod List calls = %d, want >= 2 (live + cached read sources must go through the intercepted client)", podLists)
	}
}

// TestPlan_Demote_UnbackedReadyInstances pins the truth pass: a Ready
// Instance with no live pods and no in-flight Operation is demoted in
// every pause depth when no op pass will act; the pass never fires for
// RecreateInstanceOnPodRestart components (their restart pass owns
// Ready-with-pod-loss), never for Instances with pods or an Operation,
// and never for scale-down extras.
func TestPlan_Demote_UnbackedReadyInstances(t *testing.T) {
	unbacked := func(t *testing.T) types.ReconcileInput {
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady},
		}
		return in
	}

	t.Run("frozen pause still tells the truth", func(t *testing.T) {
		in := unbacked(t)
		plan := minimalPlan()
		plan.Paused = true
		plan.PauseFreeze = true
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		if !kindsEqual(actionKinds(d), []workload.ActionKind{workload.ActionDemote}) {
			t.Errorf("frozen decision = %v, want [Demote] only", actionKinds(d))
		}
	})

	t.Run("plain pause demotes", func(t *testing.T) {
		in := unbacked(t)
		plan := minimalPlan()
		plan.Paused = true
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		if !kindsEqual(actionKinds(d), []workload.ActionKind{workload.ActionDemote}) {
			t.Errorf("paused decision = %v, want [Demote] only", actionKinds(d))
		}
	})

	t.Run("unpaused leaves the correction to Create", func(t *testing.T) {
		in := unbacked(t)
		plan := minimalPlan()
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		if findAction(d, workload.ActionDemote) != nil {
			t.Errorf("unpaused reconciles must not demote (Create recovers and re-stamps), got %v", actionKinds(d))
		}
		if findAction(d, workload.ActionCreate) == nil {
			t.Errorf("Create must be planned to re-materialize, got %v", actionKinds(d))
		}
	})

	t.Run("a loss the restart pass repairs this reconcile is not demoted", func(t *testing.T) {
		in := unbacked(t)
		plan := minimalPlan()
		plan.Paused = true
		plan.RestartPolicy = types.RestartPolicyRecreateInstance
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		if findAction(d, workload.ActionDemote) != nil {
			t.Errorf("a plain pause keeps the restart pass running, so the loss is its to repair, got %v", actionKinds(d))
		}
		if findAction(d, workload.ActionRestart) == nil {
			t.Errorf("the restart pass must own the recovery, got %v", actionKinds(d))
		}
	})

	t.Run("a frozen pause demotes the loss the recreate policy cannot repair", func(t *testing.T) {
		in := unbacked(t)
		in.ObservedState.InstanceStatuses[0].RunningRevision = "rev-a"
		plan := minimalPlan()
		plan.Paused = true
		plan.PauseFreeze = true
		plan.RestartPolicy = types.RestartPolicyRecreateInstance
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		if !kindsEqual(actionKinds(d), []workload.ActionKind{workload.ActionDemote}) {
			t.Errorf("frozen recreate-policy decision = %v, want [Demote] only: freeze suspends the repair, never the truth", actionKinds(d))
		}
	})

	t.Run("a demoted recreate-policy row is repaired once the restart pass runs", func(t *testing.T) {
		in := unbacked(t)
		in.ObservedState.InstanceStatuses[0].Phase = types.InstancePhasePending
		in.ObservedState.InstanceStatuses[0].RunningRevision = "rev-a"
		plan := minimalPlan()
		plan.Paused = true
		plan.RestartPolicy = types.RestartPolicyRecreateInstance
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		ra := findAction(d, workload.ActionRestart)
		if ra == nil || len(ra.Restarts) != 1 || ra.Restarts[0].Instance.Index != 0 {
			t.Fatalf("restart selection = %+v, want the demoted row repaired at its running revision", ra)
		}
		if ra.Restarts[0].Reason != "pod count 0 below desired 1" {
			t.Errorf("restart reason = %q, want the pod-count trigger", ra.Restarts[0].Reason)
		}
		if ra.Restarts[0].OpensUnavailability {
			t.Errorf("recovering lost capacity must not be put to the unavailability budget")
		}
		if findAction(d, workload.ActionDemote) != nil {
			t.Errorf("a Pending row has nothing left to demote, got %v", actionKinds(d))
		}
	})

	t.Run("a demoted recreate-policy row waits out a frozen pause", func(t *testing.T) {
		in := unbacked(t)
		in.ObservedState.InstanceStatuses[0].Phase = types.InstancePhasePending
		in.ObservedState.InstanceStatuses[0].RunningRevision = "rev-a"
		plan := minimalPlan()
		plan.Paused = true
		plan.PauseFreeze = true
		plan.RestartPolicy = types.RestartPolicyRecreateInstance
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		if len(d.Actions) != 0 {
			t.Errorf("frozen decision = %v, want nothing: no repair starts and nothing is left to demote", actionKinds(d))
		}
	})

	t.Run("a Pending row that never ran a revision stays with Create", func(t *testing.T) {
		in := unbacked(t)
		in.ObservedState.InstanceStatuses[0].Phase = types.InstancePhasePending
		plan := minimalPlan()
		plan.RestartPolicy = types.RestartPolicyRecreateInstance
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		if findAction(d, workload.ActionRestart) != nil {
			t.Errorf("a row with no running revision is a first materialization, got %v", actionKinds(d))
		}
		if findAction(d, workload.ActionCreate) == nil {
			t.Errorf("Create must be planned to materialize it, got %v", actionKinds(d))
		}
	})

	t.Run("live pods veto the demotion", func(t *testing.T) {
		in := unbacked(t)
		plan := minimalPlan()
		plan.Paused = true
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-0-default-0", Namespace: "prod"}}
		d := planOrFail(t, in, plan, planSnapshot(in, map[int32][]*corev1.Pod{0: {pod}}))
		if findAction(d, workload.ActionDemote) != nil {
			t.Errorf("an Instance with live pods must not be demoted, got %v", actionKinds(d))
		}
	})

	t.Run("an in-flight Operation keeps ownership", func(t *testing.T) {
		in := unbacked(t)
		in.ObservedState.InstanceStatuses[0].Operation = &types.InstanceOperation{
			ID: "u-1", Type: types.InstanceOperationUpdate, Step: "Surge",
		}
		plan := minimalPlan()
		plan.Paused = true
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		if findAction(d, workload.ActionDemote) != nil {
			t.Errorf("an Instance with an Operation must not be demoted, got %v", actionKinds(d))
		}
	})

	t.Run("non-Ready phases are never touched", func(t *testing.T) {
		in := unbacked(t)
		in.ObservedState.InstanceStatuses[0].Phase = types.InstancePhasePending
		plan := minimalPlan()
		plan.Paused = true
		d := planOrFail(t, in, plan, planSnapshot(in, nil))
		if findAction(d, workload.ActionDemote) != nil {
			t.Errorf("only Ready demotes, got %v", actionKinds(d))
		}
	})
}

func TestPlan_Paused_AdvancesInFlightAttemptsOnly(t *testing.T) {
	build := func(t *testing.T) (types.ReconcileInput, types.ComponentPlan) {
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []types.InstanceStatus{
			// Recreate in flight: the old pods are gone and the new ones
			// are not created yet.
			{Index: 0, Incarnation: 2, Phase: types.InstancePhaseUpdating, RunningRevision: "prior-rev",
				Operation: &types.InstanceOperation{
					ID: "update-0", Type: types.InstanceOperationUpdate, Step: "Drain",
					TargetRevision: updateTarget().Name,
				}},
			// Off-target but idle: a fresh start.
			{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
			// Create committed to this target: the set is half materialized.
			{Index: 2, Incarnation: 1, Phase: types.InstancePhaseCreating,
				Operation: &types.InstanceOperation{
					ID: "create-2", Type: types.InstanceOperationCreate, Step: "CreatePods",
					TargetRevision: updateTarget().Name,
				}},
		}
		plan := minimalPlan()
		plan.Replicas = 3
		plan.Instances = append(plan.Instances,
			types.InstancePlan{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			types.InstancePlan{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		)
		return in, plan
	}
	// Index 1 is backed, so the truth pass has nothing to correct and the
	// decision is exactly the lifecycle selection under test.
	pods := func(in types.ReconcileInput) map[int32][]*corev1.Pod {
		return map[int32][]*corev1.Pod{1: {enginePod(in.Key.OwnerName, in.Key.Namespace, 1)}}
	}

	t.Run("paused selects the open attempts only", func(t *testing.T) {
		in, plan := build(t)
		plan.Paused = true
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		if !kindsEqual(actionKinds(d), []workload.ActionKind{workload.ActionUpdate, workload.ActionCreate}) {
			t.Fatalf("paused decision = %v, want [Update Create]", actionKinds(d))
		}
		ua := findAction(d, workload.ActionUpdate)
		if len(ua.Update.Items) != 1 || ua.Update.Items[0].Instance.Index != 0 {
			t.Errorf("update selection = %+v, want the in-flight index 0 only", ua.Update.Items)
		}
		if ua.Update.Items[0].StartingFresh {
			t.Errorf("a paused selection must never start fresh: %+v", ua.Update.Items[0])
		}
		if d.Escalate {
			t.Errorf("paused decision must suspend escalation")
		}
	})

	t.Run("no committed create means no Create pass", func(t *testing.T) {
		in, plan := build(t)
		plan.Paused = true
		in.ObservedState.InstanceStatuses = in.ObservedState.InstanceStatuses[:2]
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		if findAction(d, workload.ActionCreate) != nil {
			t.Errorf("paused decision with nothing committed must not plan Create, got %v", actionKinds(d))
		}
	})

	t.Run("unpause restores the fresh start", func(t *testing.T) {
		in, plan := build(t)
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		ua := findAction(d, workload.ActionUpdate)
		if ua == nil || len(ua.Update.Items) != 2 {
			t.Fatalf("unpaused update selection = %+v, want both indices", ua)
		}
		if !ua.Update.Items[1].StartingFresh {
			t.Errorf("index 1 = %+v, want a fresh start once the pause is cleared", ua.Update.Items[1])
		}
	})
}

// TestPlan_Paused_ReDrivesAFailedUpdateContinuation: a paused Component
// whose only Update row is Failed with the operation preserved — the
// deadline backstop's stamp on a multi-pod roll — is still selected, and
// selected as a continuation. The claim decides, not the owner: nobody
// may advance a Failed row, but the operation it carries is an Update
// half-done, and a pause parks new work rather than leaving a roll
// wedged until the unpause.
func TestPlan_Paused_ReDrivesAFailedUpdateContinuation(t *testing.T) {
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{
				ID: "update-0", Type: types.InstanceOperationUpdate, Step: "Drain",
				TargetRevision: updateTarget().Name,
			}},
	}
	plan := minimalPlan()
	plan.Paused = true
	d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil || len(ua.Update.Items) != 1 || ua.Update.Items[0].Instance.Index != 0 {
		t.Fatalf("paused decision = %v, want the Failed row's Update continuation selected", actionKinds(d))
	}
	if ua.Update.Items[0].StartingFresh {
		t.Errorf("a Failed row with a preserved Update is a continuation, got %+v", ua.Update.Items[0])
	}
}

// TestPlan_PauseFreeze_ReDrivesAFailedUpdateContinuation: a frozen pause
// deepens the hold onto the restart pass and nothing else, so a Failed
// row whose preserved operation is an Update is still selected as a
// continuation under it, exactly as under the default pause.
func TestPlan_PauseFreeze_ReDrivesAFailedUpdateContinuation(t *testing.T) {
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{
		{Index: 0, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "prior-rev",
			Operation: &types.InstanceOperation{
				ID: "update-0", Type: types.InstanceOperationUpdate, Step: "Drain",
				TargetRevision: updateTarget().Name,
			}},
	}
	plan := minimalPlan()
	plan.Paused = true
	plan.PauseFreeze = true
	d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
	ua := findAction(d, workload.ActionUpdate)
	if ua == nil || len(ua.Update.Items) != 1 || ua.Update.Items[0].Instance.Index != 0 {
		t.Fatalf("frozen decision = %v, want the Failed row's Update continuation selected", actionKinds(d))
	}
	if ua.Update.Items[0].StartingFresh {
		t.Errorf("a Failed row with a preserved Update is a continuation, got %+v", ua.Update.Items[0])
	}
}

// TestPlan_PauseFreeze_AdvancesOpenRepairOnly: a frozen pause suspends
// the repair pass — a fresh pod-loss trigger is not selected — but a
// repair already under way is still driven, so the pods it deleted are
// recreated instead of staying missing for the length of the hold. A
// standard pause keeps starting repairs.
func TestPlan_PauseFreeze_AdvancesOpenRepairOnly(t *testing.T) {
	build := func(t *testing.T) (types.ReconcileInput, types.ComponentPlan) {
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []types.InstanceStatus{
			{Index: 0, Incarnation: 2, Phase: types.InstancePhaseRestarting,
				Operation: &types.InstanceOperation{
					ID: "restart-0", Type: types.InstanceOperationRestart, Step: "Drain",
				}},
			// Ready with no pods: a fresh pod-loss repair trigger.
			{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady},
		}
		plan := minimalPlan()
		plan.Replicas = 2
		plan.RestartPolicy = types.RestartPolicyRecreateInstance
		plan.Paused = true
		plan.Instances = append(plan.Instances,
			types.InstancePlan{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		)
		return in, plan
	}

	t.Run("frozen pause drives the open repair only", func(t *testing.T) {
		in, plan := build(t)
		plan.PauseFreeze = true
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		ra := findAction(d, workload.ActionRestart)
		if ra == nil || len(ra.Restarts) != 1 || ra.Restarts[0].Instance.Index != 0 {
			t.Fatalf("frozen restart selection = %+v, want the open repair on index 0 only", ra)
		}
	})

	t.Run("frozen pause with nothing open plans no repair", func(t *testing.T) {
		in, plan := build(t)
		plan.PauseFreeze = true
		in.ObservedState.InstanceStatuses = in.ObservedState.InstanceStatuses[1:]
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		if findAction(d, workload.ActionRestart) != nil {
			t.Errorf("frozen decision must not start a repair, got %v", actionKinds(d))
		}
	})

	t.Run("standard pause keeps repairing", func(t *testing.T) {
		in, plan := build(t)
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, nil))
		ra := findAction(d, workload.ActionRestart)
		if ra == nil || len(ra.Restarts) != 2 {
			t.Fatalf("paused restart selection = %+v, want both the open repair and the fresh one", ra)
		}
	})
}

// TestPlan_PauseFreeze_AdvancesTheOpenRollOnly: a freeze suspends repair
// on top of what a standard pause withholds, but neither withholds a
// step already under way. A recreate mid-gap and an in-place patch
// mid-flight are both still selected as continuations — never as fresh
// starts — and clearing the pause adds the fresh start back without
// disturbing either of them.
func TestPlan_PauseFreeze_AdvancesTheOpenRollOnly(t *testing.T) {
	build := func(t *testing.T) (types.ReconcileInput, types.ComponentPlan) {
		t.Helper()
		now := time.Now()
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []types.InstanceStatus{
			rollInFlight(0, "Drain", 2, now.Add(30*time.Minute)),
			rollInFlight(1, "InPlace", 1, now.Add(30*time.Minute)),
			// Off-target with no attempt open: the fresh start a pause
			// of either kind withholds.
			{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		}
		plan := minimalPlan()
		plan.Replicas = 3
		plan.Instances = append(plan.Instances,
			types.InstancePlan{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			types.InstancePlan{Index: 2, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		)
		return in, plan
	}
	// Index 2 is backed, so the truth pass has nothing to correct and the
	// decision is exactly the lifecycle selection under test.
	pods := func(in types.ReconcileInput) map[int32][]*corev1.Pod {
		return map[int32][]*corev1.Pod{2: {enginePod(in.Key.OwnerName, in.Key.Namespace, 2)}}
	}

	openIndices := func(t *testing.T, d workload.Decision) []int32 {
		t.Helper()
		ua := findAction(d, workload.ActionUpdate)
		if ua == nil {
			t.Fatalf("no Update action selected; got %v", actionKinds(d))
		}
		var out []int32
		for _, item := range ua.Update.Items {
			out = append(out, item.Instance.Index)
		}
		return out
	}

	t.Run("frozen pause drives both open rolls and starts nothing", func(t *testing.T) {
		in, plan := build(t)
		plan.Paused, plan.PauseFreeze = true, true
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		got := openIndices(t, d)
		if len(got) != 2 || got[0] != 0 || got[1] != 1 {
			t.Fatalf("frozen update selection = %v, want the two open rolls [0 1]", got)
		}
		for _, item := range findAction(d, workload.ActionUpdate).Update.Items {
			if item.StartingFresh {
				t.Errorf("index %d = %+v, want a continuation under a freeze", item.Instance.Index, item)
			}
		}
		if d.Escalate {
			t.Errorf("a frozen decision must suspend escalation")
		}
	})

	t.Run("unpause adds the fresh start and leaves the open rolls alone", func(t *testing.T) {
		in, plan := build(t)
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		got := openIndices(t, d)
		if len(got) != 3 {
			t.Fatalf("unpaused update selection = %v, want all three indices", got)
		}
		for _, item := range findAction(d, workload.ActionUpdate).Update.Items {
			fresh := item.Instance.Index == 2
			if item.StartingFresh != fresh {
				t.Errorf("index %d StartingFresh = %v, want %v (the unpause restamps no open roll)",
					item.Instance.Index, item.StartingFresh, fresh)
			}
		}
	})
}

// TestPlan_PauseFreeze_AdvancesTheOpenDrain: a drain step is past the
// point where a hold is free — the source is already out of rotation and
// the replacement is the Instance's only capacity — so neither a standard
// pause nor a freeze withholds it. It is selected as a continuation, and
// clearing the pause adds back the fresh start it was withholding without
// disturbing the drain.
func TestPlan_PauseFreeze_AdvancesTheOpenDrain(t *testing.T) {
	build := func(t *testing.T) (types.ReconcileInput, types.ComponentPlan) {
		t.Helper()
		now := time.Now()
		in := minimalInput(t)
		forbidMutations(t, &in)
		in.ObservedState.InstanceStatuses = []types.InstanceStatus{
			rollInFlight(0, types.UpdateStepSurgeDrain, 1, now.Add(30*time.Minute)),
			// Off-target with no attempt open: the fresh start a pause of
			// either kind withholds.
			{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
		}
		plan := minimalPlan()
		plan.Replicas = 2
		plan.Instances = append(plan.Instances,
			types.InstancePlan{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}})
		return in, plan
	}
	pods := func(in types.ReconcileInput) map[int32][]*corev1.Pod {
		return map[int32][]*corev1.Pod{1: {enginePod(in.Key.OwnerName, in.Key.Namespace, 1)}}
	}
	openIndices := func(t *testing.T, d workload.Decision) []int32 {
		t.Helper()
		ua := findAction(d, workload.ActionUpdate)
		if ua == nil {
			t.Fatalf("no Update action selected; got %v", actionKinds(d))
		}
		var out []int32
		for _, item := range ua.Update.Items {
			out = append(out, item.Instance.Index)
		}
		return out
	}

	t.Run("frozen pause drives the open drain and starts nothing", func(t *testing.T) {
		in, plan := build(t)
		plan.Paused, plan.PauseFreeze = true, true
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		got := openIndices(t, d)
		if len(got) != 1 || got[0] != 0 {
			t.Fatalf("frozen update selection = %v, want the open drain [0]", got)
		}
		if findAction(d, workload.ActionUpdate).Update.Items[0].StartingFresh {
			t.Errorf("the open drain was restamped as a fresh attempt under a freeze")
		}
		if d.Escalate {
			t.Errorf("a frozen decision must suspend escalation")
		}
	})

	t.Run("unpause adds the fresh start and leaves the drain alone", func(t *testing.T) {
		in, plan := build(t)
		d := planTargetOrFail(t, in, plan, updateTarget(), planSnapshot(in, pods(in)))
		got := openIndices(t, d)
		if len(got) != 2 {
			t.Fatalf("unpaused update selection = %v, want both indices", got)
		}
		for _, item := range findAction(d, workload.ActionUpdate).Update.Items {
			fresh := item.Instance.Index == 1
			if item.StartingFresh != fresh {
				t.Errorf("index %d StartingFresh = %v, want %v (the unpause restamps no open roll)",
					item.Instance.Index, item.StartingFresh, fresh)
			}
		}
	})
}

// podOnRevision stamps the pod with the hash of the named revision, the
// label every pass reads the pod's revision from.
func podOnRevision(pod *corev1.Pod, revision string) *corev1.Pod {
	pod.Labels[query.LabelRevisionHash] = query.RevisionFromName(revision).Hash()
	return pod
}

// offTargetRepairPlan is the two-Instance plan the yield tests run:
// single-pod Instances, or a leader+worker gang when gang is set.
func offTargetRepairPlan(gang bool, strategy types.UpdateStrategyType) types.ComponentPlan {
	runners := []types.RunnerPlan{{Name: "default", Size: 1}}
	if gang {
		runners = []types.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	}
	return types.ComponentPlan{
		Component:      types.ComponentEngine,
		Replicas:       2,
		RestartPolicy:  types.RestartPolicyRecreateInstance,
		UpdateStrategy: types.UpdateStrategy{Type: strategy},
		Instances: []types.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: runners},
			{Index: 1, Incarnation: 1, Runners: runners},
		},
	}
}

// TestPlan_Restart_YieldsARowOffTheTargetToTheUpdatePass pins the
// ownership rule between the two passes: an Instance whose running
// revision is not the roll target belongs to the update pass, whatever
// evidence the restart trigger holds against it — a crash loop, a lost
// pod or a lost gang member — because the roll replaces the pod set at
// the target and a repair would rebuild a revision the Component does not want. The
// same evidence on a row that still runs the target opens the repair.
func TestPlan_Restart_YieldsARowOffTheTargetToTheUpdatePass(t *testing.T) {
	target := updateTarget()
	cases := []struct {
		name string
		gang bool
		// pods of the row under test, stamped with the revision it runs
		pods func(running string) []*corev1.Pod
	}{
		{name: "crash loop", pods: func(running string) []*corev1.Pod {
			return []*corev1.Pod{podOnRevision(wedgedEnginePod(1), running)}
		}},
		{name: "pod lost", pods: func(string) []*corev1.Pod { return nil }},
		{name: "gang member lost", gang: true, pods: func(running string) []*corev1.Pod {
			leader := podOnRevision(enginePod("llama-70b", "prod", 1), running)
			leader.Labels[query.LabelRunner] = "leader"
			return []*corev1.Pod{leader}
		}},
	}
	for _, tc := range cases {
		for _, onTarget := range []bool{false, true} {
			name := tc.name + " off the target"
			if onTarget {
				name = tc.name + " on the target"
			}
			t.Run(name, func(t *testing.T) {
				in := minimalInput(t)
				forbidMutations(t, &in)
				in.StuckPodGrace = time.Minute
				running := crashLoopRepairRevision
				if onTarget {
					running = target.Name
				}
				// The Component's promoted revision is the one the row under
				// test runs; the crash-loop evidence is scoped to it.
				in.ObservedState.CurrentRevision = running
				in.ObservedState.InstanceStatuses = []types.InstanceStatus{
					{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, RunningRevision: target.Name},
					{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, RunningRevision: running},
				}
				plan := offTargetRepairPlan(tc.gang, types.UpdateStrategyRecreatePod)
				healthy := []*corev1.Pod{podOnRevision(enginePod("llama-70b", "prod", 0), target.Name)}
				if tc.gang {
					in.ObservedState.InstanceStatuses[0].PodCount = 2
					in.ObservedState.InstanceStatuses[1].PodCount = 2
					healthy[0].Labels[query.LabelRunner] = "leader"
					worker := podOnRevision(enginePod("llama-70b", "prod", 0), target.Name)
					worker.Name += "-worker"
					worker.Labels[query.LabelRunner] = "worker"
					healthy = append(healthy, worker)
				}
				pods := map[int32][]*corev1.Pod{0: healthy, 1: tc.pods(running)}
				d := planTargetOrFail(t, in, plan, target, planSnapshot(in, pods))

				ra := findAction(d, workload.ActionRestart)
				ua := findAction(d, workload.ActionUpdate)
				if onTarget {
					if ra == nil || len(ra.Restarts) != 1 || ra.Restarts[0].Instance.Index != 1 {
						t.Fatalf("a row on the target is repaired; restart action = %+v (actions %v)", ra, actionKinds(d))
					}
					if ua != nil {
						t.Fatalf("nothing is off the target, so no update is selected; got %+v", ua.Update.Items)
					}
					return
				}
				if ra != nil {
					t.Fatalf("a row off the target is never repaired; restart selection = %+v", ra.Restarts)
				}
				if ua == nil || len(ua.Update.Items) != 1 || ua.Update.Items[0].Instance.Index != 1 || !ua.Update.Items[0].StartingFresh {
					t.Fatalf("the update pass must start on the off-target row; update action = %+v (actions %v)", ua, actionKinds(d))
				}
			})
		}
	}
}

// TestPlan_Restart_OpenRepairIsDrivenWhateverTheTarget: a Restart
// already in flight is driven to completion even though the target has
// moved on; the roll starts once the repair ends. Only a repair that has
// not opened yields.
func TestPlan_Restart_OpenRepairIsDrivenWhateverTheTarget(t *testing.T) {
	target := updateTarget()
	in := minimalInput(t)
	forbidMutations(t, &in)
	in.ObservedState.InstanceStatuses = []types.InstanceStatus{{
		Index: 0, Incarnation: 2, Phase: types.InstancePhaseRestarting, RunningRevision: crashLoopRepairRevision,
		Operation: &types.InstanceOperation{ID: "restart-0", Type: types.InstanceOperationRestart, Step: types.RestartStepDrain},
	}}
	plan := minimalPlan()
	plan.RestartPolicy = types.RestartPolicyRecreateInstance

	d := planTargetOrFail(t, in, plan, target, planSnapshot(in, nil))
	ra := findAction(d, workload.ActionRestart)
	if ra == nil || len(ra.Restarts) != 1 || ra.Restarts[0].Instance.Index != 0 {
		t.Fatalf("an open repair must keep advancing; actions = %v", actionKinds(d))
	}
	if ua := findAction(d, workload.ActionUpdate); ua != nil {
		t.Fatalf("the roll waits for the open repair; update action = %+v", ua.Update.Items)
	}
}

// TestPlan_Restart_DemotedRowOffTarget: a row demoted for losing every
// pod keeps the revision it ran. Off the target with no pod left it is
// the Create pass's, which materializes it at the target; one that holds
// survivors again is still rebuilt by the repair at the revision it
// records, because no update trigger reads a Pending row.
func TestPlan_Restart_DemotedRowOffTarget(t *testing.T) {
	target := updateTarget()
	build := func(t *testing.T, gang bool) (types.ReconcileInput, types.ComponentPlan) {
		in := minimalInput(t)
		forbidMutations(t, &in)
		podCount := int32(1)
		if gang {
			podCount = 2
		}
		in.ObservedState.InstanceStatuses = []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: podCount, RunningRevision: target.Name},
			{Index: 1, Incarnation: 1, Phase: types.InstancePhasePending, PodCount: podCount, RunningRevision: crashLoopRepairRevision},
		}
		return in, offTargetRepairPlan(gang, types.UpdateStrategyRecreatePod)
	}

	t.Run("no pod left is materialized by Create at the target", func(t *testing.T) {
		in, plan := build(t, false)
		d := planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {podOnRevision(enginePod("llama-70b", "prod", 0), target.Name)}}))
		if ra := findAction(d, workload.ActionRestart); ra != nil {
			t.Fatalf("a demoted row off the target with no pod is not repaired; restart selection = %+v", ra.Restarts)
		}
		if findAction(d, workload.ActionCreate) == nil {
			t.Fatalf("the Create pass must be planned to materialize the row; actions = %v", actionKinds(d))
		}
	})

	t.Run("a survivor keeps the gang with the repair", func(t *testing.T) {
		in, plan := build(t, true)
		leader := podOnRevision(enginePod("llama-70b", "prod", 0), target.Name)
		leader.Labels[query.LabelRunner] = "leader"
		worker := podOnRevision(enginePod("llama-70b", "prod", 0), target.Name)
		worker.Name += "-worker"
		worker.Labels[query.LabelRunner] = "worker"
		survivor := enginePod("llama-70b", "prod", 1)
		survivor.Labels[query.LabelRunner] = "leader"
		d := planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {leader, worker}, 1: {survivor}}))
		ra := findAction(d, workload.ActionRestart)
		if ra == nil || len(ra.Restarts) != 1 || ra.Restarts[0].Instance.Index != 1 {
			t.Fatalf("a demoted gang holding a survivor is rebuilt by the repair; actions = %v", actionKinds(d))
		}
	})
}

// servingEnginePod is an engine pod in rotation: Ready with the serving
// gate set.
func servingEnginePod(idx int32) *corev1.Pod {
	pod := enginePod("llama-70b", "prod", idx)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}
	return pod
}

// TestPlan_Update_CoordGateExempt_ReadyRowServingNothing: a fresh update
// start on a Ready row whose pod set is provably out of service takes
// nothing further offline, so it skips the coordination gate consult on
// a non-surge strategy exactly as a dark Failed row does. A surge start
// keeps the consult, and so does a row that still serves or whose pod is
// merely not yet Ready.
func TestPlan_Update_CoordGateExempt_ReadyRowServingNothing(t *testing.T) {
	target := updateTarget()
	notReady := enginePod("llama-70b", "prod", 0)
	notReady.Status.Phase = corev1.PodRunning
	notReady.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}
	cases := []struct {
		name     string
		strategy types.UpdateStrategyType
		pods     []*corev1.Pod
		exempt   bool
	}{
		{name: "crash loop, recreate", strategy: types.UpdateStrategyRecreatePod, pods: []*corev1.Pod{wedgedEnginePod(0)}, exempt: true},
		{name: "crash loop, in-place", strategy: types.UpdateStrategyInPlaceIfPossible, pods: []*corev1.Pod{wedgedEnginePod(0)}, exempt: true},
		{name: "crash loop, surge", strategy: types.UpdateStrategySurgeThenDrain, pods: []*corev1.Pod{wedgedEnginePod(0)}, exempt: false},
		{name: "serving pod, recreate", strategy: types.UpdateStrategyRecreatePod, pods: []*corev1.Pod{servingEnginePod(0)}, exempt: false},
		{name: "running not ready, recreate", strategy: types.UpdateStrategyRecreatePod, pods: []*corev1.Pod{notReady}, exempt: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, RunningRevision: crashLoopRepairRevision},
			}
			plan := minimalPlan()
			plan.UpdateStrategy = types.UpdateStrategy{Type: tc.strategy}
			// The running revision's template differs from the desired one by
			// its image alone, the diff an in-place strategy keeps in place.
			running := in.DesiredSpec.PodSpec.DeepCopy()
			running.Containers[0].Image = "test:v0"
			d := planTargetOrFail(t, in, plan, target, planSnapshotWithRevisions(in,
				map[int32][]*corev1.Pod{0: tc.pods}, map[string]*corev1.PodSpec{crashLoopRepairRevision: running}))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil || len(ua.Update.Items) != 1 || !ua.Update.Items[0].StartingFresh {
				t.Fatalf("the off-target row must be a fresh update start; actions = %v", actionKinds(d))
			}
			if got := ua.Update.Items[0].CoordGateExempt; got != tc.exempt {
				t.Fatalf("CoordGateExempt = %v, want %v", got, tc.exempt)
			}
		})
	}
}

// TestPlan_Update_DarkInstancesListedFirst: the update selection lists
// the fresh starts that replace an Instance serving nothing — a Ready
// row whose pod set is provably out of service, a Failed row with no
// serving pod — ahead of the starts that take a serving Instance
// offline, each group in plan order, under a surge and a drain-first
// strategy alike; a continuation keeps its place among the rest. The
// budget the executor spends in that order restores the dark Instances
// before it touches a serving one.
func TestPlan_Update_DarkInstancesListedFirst(t *testing.T) {
	target := updateTarget()
	for _, strategy := range []types.UpdateStrategyType{types.UpdateStrategySurgeThenDrain, types.UpdateStrategyRecreatePod} {
		t.Run(string(strategy), func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, ServingPodCount: 1, RunningRevision: crashLoopRepairRevision},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseUpdating, PodCount: 1, RunningRevision: crashLoopRepairRevision},
				{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, ServingPodCount: 1, RunningRevision: crashLoopRepairRevision},
				{Index: 3, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, RunningRevision: crashLoopRepairRevision},
				{Index: 4, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: crashLoopRepairRevision},
			}
			plan := types.ComponentPlan{
				Component:      types.ComponentEngine,
				Replicas:       5,
				UpdateStrategy: types.UpdateStrategy{Type: strategy},
			}
			for idx := int32(0); idx < 5; idx++ {
				plan.Instances = append(plan.Instances, types.InstancePlan{Index: idx, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}})
			}
			pods := map[int32][]*corev1.Pod{
				0: {servingEnginePod(0)}, 1: {servingEnginePod(1)}, 2: {servingEnginePod(2)}, 3: {wedgedEnginePod(3)},
			}
			d := planTargetOrFail(t, in, plan, target, planSnapshot(in, pods))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil {
				t.Fatalf("expected an Update action, got %v", actionKinds(d))
			}
			want := []int32{3, 4, 0, 1, 2}
			var got []int32
			for _, item := range ua.Update.Items {
				got = append(got, item.Instance.Index)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("update items = %v, want %v: the dark Instances first, then plan order", got, want)
			}
			for _, item := range ua.Update.Items {
				dark := item.Instance.Index == 3 || item.Instance.Index == 4
				if item.ReplacesDarkPodSet != dark {
					t.Errorf("instance %d: ReplacesDarkPodSet = %v, want %v", item.Instance.Index, item.ReplacesDarkPodSet, dark)
				}
			}
		})
	}
}

// TestPlan_Update_DarkGangIsReadOnItsLeader: a gang serves through its
// leader, so the preference for an Instance that serves nothing reads the
// gang's routed member. Two leader+worker gangs on the running revision
// under RecreatePod: a gang whose leader is parked in CrashLoopBackOff
// beside a Ready worker is dark and listed first; a gang whose worker is
// parked beside a serving leader still serves and keeps its place in plan
// order. Either gang is already out of the coordination gate's serving
// count, so both starts skip the gate consult.
func TestPlan_Update_DarkGangIsReadOnItsLeader(t *testing.T) {
	target := updateTarget()
	gangPod := func(idx int32, runner string, parked bool) *corev1.Pod {
		pod := servingEnginePod(idx)
		if parked {
			pod = wedgedEnginePod(idx)
		}
		pod.Name += "-" + runner
		pod.Labels[query.LabelRunner] = runner
		return pod
	}
	for _, tc := range []struct {
		name   string
		parked string
		dark   bool
	}{
		{name: "leader parked beside a Ready worker", parked: "leader", dark: true},
		{name: "worker parked beside a serving leader", parked: "worker", dark: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 2, ServingPodCount: 2, RunningRevision: crashLoopRepairRevision},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 2, ServingPodCount: 1, RunningRevision: crashLoopRepairRevision},
			}
			plan := types.ComponentPlan{
				Component:      types.ComponentEngine,
				Replicas:       2,
				UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategyRecreatePod},
			}
			for idx := int32(0); idx < 2; idx++ {
				plan.Instances = append(plan.Instances, types.InstancePlan{Index: idx, Incarnation: 1,
					Runners: []types.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}})
			}
			pods := map[int32][]*corev1.Pod{
				0: {gangPod(0, "leader", false), gangPod(0, "worker", false)},
				1: {gangPod(1, "leader", tc.parked == "leader"), gangPod(1, "worker", tc.parked == "worker")},
			}
			d := planTargetOrFail(t, in, plan, target, planSnapshot(in, pods))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil || len(ua.Update.Items) != 2 {
				t.Fatalf("expected both gangs selected for the update, got %v", actionKinds(d))
			}
			want := []int32{0, 1}
			if tc.dark {
				want = []int32{1, 0}
			}
			var got []int32
			for _, item := range ua.Update.Items {
				got = append(got, item.Instance.Index)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("update items = %v, want %v", got, want)
			}
			for _, item := range ua.Update.Items {
				dark := tc.dark && item.Instance.Index == 1
				exempt := item.Instance.Index == 1
				if item.ReplacesDarkPodSet != dark || item.CoordGateExempt != exempt {
					t.Errorf("instance %d: ReplacesDarkPodSet = %v, CoordGateExempt = %v, want %v and %v",
						item.Instance.Index, item.ReplacesDarkPodSet, item.CoordGateExempt, dark, exempt)
				}
			}
		})
	}
}

// TestPlan_Update_CoordGateExempt_GangWithParkedMember: the coordination
// gate counts a gang as serving only when every member is, so a gang with
// a member parked in a terminal waiting reason is already inside the
// gate's unavailability whichever member it is. A fresh drain-first start
// on such a gang takes nothing further out of the gate's count and skips
// the consult; the start keeps its place in plan order when the leader
// still serves, because the per-Component budget still pays for the
// leader leaving rotation. A gang whose member is merely not yet Ready,
// a gang fully in rotation, and any start on a surge strategy keep the
// consult.
func TestPlan_Update_CoordGateExempt_GangWithParkedMember(t *testing.T) {
	target := updateTarget()
	member := func(pod *corev1.Pod, runner string) *corev1.Pod {
		pod.Name += "-" + runner
		pod.Labels[query.LabelRunner] = runner
		return pod
	}
	bootingWorker := func() *corev1.Pod {
		pod := enginePod("llama-70b", "prod", 0)
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: constants.MainContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}}
		return member(pod, "worker")
	}
	for _, tc := range []struct {
		name     string
		strategy types.UpdateStrategyType
		pods     []*corev1.Pod
		serving  int32
		exempt   bool
		dark     bool
	}{
		{name: "worker parked beside a serving leader, recreate", strategy: types.UpdateStrategyRecreatePod,
			pods: []*corev1.Pod{member(servingEnginePod(0), "leader"), member(wedgedEnginePod(0), "worker")}, serving: 1, exempt: true},
		{name: "worker parked beside a serving leader, in-place", strategy: types.UpdateStrategyInPlaceIfPossible,
			pods: []*corev1.Pod{member(servingEnginePod(0), "leader"), member(wedgedEnginePod(0), "worker")}, serving: 1, exempt: true},
		{name: "worker parked beside a serving leader, surge", strategy: types.UpdateStrategySurgeThenDrain,
			pods: []*corev1.Pod{member(servingEnginePod(0), "leader"), member(wedgedEnginePod(0), "worker")}, serving: 1, exempt: false},
		{name: "leader parked beside a serving worker, recreate", strategy: types.UpdateStrategyRecreatePod,
			pods: []*corev1.Pod{member(wedgedEnginePod(0), "leader"), member(servingEnginePod(0), "worker")}, serving: 1, exempt: true, dark: true},
		{name: "worker booting beside a serving leader, recreate", strategy: types.UpdateStrategyRecreatePod,
			pods: []*corev1.Pod{member(servingEnginePod(0), "leader"), bootingWorker()}, serving: 1, exempt: false},
		{name: "both members serving, recreate", strategy: types.UpdateStrategyRecreatePod,
			pods: []*corev1.Pod{member(servingEnginePod(0), "leader"), member(servingEnginePod(0), "worker")}, serving: 2, exempt: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 2, ServingPodCount: tc.serving, RunningRevision: crashLoopRepairRevision},
			}
			plan := types.ComponentPlan{
				Component:      types.ComponentEngine,
				Replicas:       1,
				UpdateStrategy: types.UpdateStrategy{Type: tc.strategy},
				Instances: []types.InstancePlan{{Index: 0, Incarnation: 1,
					Runners: []types.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}}},
			}
			d := planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: tc.pods}))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil || len(ua.Update.Items) != 1 || !ua.Update.Items[0].StartingFresh {
				t.Fatalf("the off-target gang must be a fresh update start; actions = %v", actionKinds(d))
			}
			item := ua.Update.Items[0]
			if item.CoordGateExempt != tc.exempt || item.ReplacesDarkPodSet != tc.dark {
				t.Fatalf("CoordGateExempt = %v, ReplacesDarkPodSet = %v, want %v and %v", item.CoordGateExempt, item.ReplacesDarkPodSet, tc.exempt, tc.dark)
			}
		})
	}
}

// unreadyEnginePod is an engine pod that was promoted and then stopped
// passing readiness unreadyFor ago: still Running, the serving gate True,
// ContainersReady and Ready False since then.
func unreadyEnginePod(idx int32, unreadyFor time.Duration) *corev1.Pod {
	pod := servingEnginePod(idx)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Hour))
	pod.Status.ContainerStatuses[0].Ready = false
	since := metav1.NewTime(time.Now().Add(-unreadyFor))
	for i := range pod.Status.Conditions {
		switch pod.Status.Conditions[i].Type {
		case corev1.ContainersReady, corev1.PodReady:
			pod.Status.Conditions[i].Status = corev1.ConditionFalse
			pod.Status.Conditions[i].LastTransitionTime = since
		}
	}
	return pod
}

// TestPlan_Update_CoordGateExempt_ReadyRowUnreadyPastTheWindow: a fresh
// update start on a Ready row whose promoted pod has failed readiness for
// longer than the stuck-pod grace replaces an Instance that serves
// nothing: it skips the coordination gate consult on a drain-first
// strategy, as a parked pod does, and is listed as a dark start under
// every strategy. Inside the grace, with no grace configured, or on a pod
// that never served, the start is consulted and charged like any other.
// A gang reads the same way through its routed leader, and a worker
// unready past the grace skips the consult as a parked worker does.
func TestPlan_Update_CoordGateExempt_ReadyRowUnreadyPastTheWindow(t *testing.T) {
	target := updateTarget()
	member := func(pod *corev1.Pod, runner string) *corev1.Pod {
		pod.Name += "-" + runner
		pod.Labels[query.LabelRunner] = runner
		return pod
	}
	gang := []types.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	single := []types.RunnerPlan{{Name: "default", Size: 1}}
	never := enginePod("llama-70b", "prod", 0)
	never.Status.Phase = corev1.PodRunning
	never.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: constants.MainContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}}
	for _, tc := range []struct {
		name     string
		strategy types.UpdateStrategyType
		grace    time.Duration
		runners  []types.RunnerPlan
		pods     []*corev1.Pod
		serving  int32
		exempt   bool
		dark     bool
	}{
		{name: "past the window, recreate", strategy: types.UpdateStrategyRecreatePod, grace: time.Minute, runners: single,
			pods: []*corev1.Pod{unreadyEnginePod(0, 2*time.Minute)}, exempt: true, dark: true},
		{name: "past the window, in-place", strategy: types.UpdateStrategyInPlaceIfPossible, grace: time.Minute, runners: single,
			pods: []*corev1.Pod{unreadyEnginePod(0, 2*time.Minute)}, exempt: true, dark: true},
		{name: "past the window, surge", strategy: types.UpdateStrategySurgeThenDrain, grace: time.Minute, runners: single,
			pods: []*corev1.Pod{unreadyEnginePod(0, 2*time.Minute)}, dark: true},
		{name: "inside the window, recreate", strategy: types.UpdateStrategyRecreatePod, grace: time.Minute, runners: single,
			pods: []*corev1.Pod{unreadyEnginePod(0, 10*time.Second)}},
		{name: "no window configured, recreate", strategy: types.UpdateStrategyRecreatePod, runners: single,
			pods: []*corev1.Pod{unreadyEnginePod(0, 2*time.Hour)}},
		{name: "never promoted, recreate", strategy: types.UpdateStrategyRecreatePod, grace: time.Minute, runners: single,
			pods: []*corev1.Pod{never}},
		{name: "leader past the window beside a serving worker, recreate", strategy: types.UpdateStrategyRecreatePod, grace: time.Minute, runners: gang,
			pods: []*corev1.Pod{member(unreadyEnginePod(0, 2*time.Minute), "leader"), member(servingEnginePod(0), "worker")}, serving: 1, exempt: true, dark: true},
		{name: "worker past the window beside a serving leader, recreate", strategy: types.UpdateStrategyRecreatePod, grace: time.Minute, runners: gang,
			pods: []*corev1.Pod{member(servingEnginePod(0), "leader"), member(unreadyEnginePod(0, 2*time.Minute), "worker")}, serving: 1, exempt: true},
		{name: "worker inside the window beside a serving leader, recreate", strategy: types.UpdateStrategyRecreatePod, grace: time.Minute, runners: gang,
			pods: []*corev1.Pod{member(servingEnginePod(0), "leader"), member(unreadyEnginePod(0, 10*time.Second), "worker")}, serving: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.StuckPodGrace = tc.grace
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: int32(len(tc.runners)), ServingPodCount: tc.serving, RunningRevision: crashLoopRepairRevision},
			}
			plan := types.ComponentPlan{
				Component:      types.ComponentEngine,
				Replicas:       1,
				UpdateStrategy: types.UpdateStrategy{Type: tc.strategy},
				Instances:      []types.InstancePlan{{Index: 0, Incarnation: 1, Runners: tc.runners}},
			}
			// The running revision's template differs from the desired one by
			// its image alone, the diff an in-place strategy keeps in place.
			running := in.DesiredSpec.PodSpec.DeepCopy()
			running.Containers[0].Image = "test:v0"
			d := planTargetOrFail(t, in, plan, target, planSnapshotWithRevisions(in,
				map[int32][]*corev1.Pod{0: tc.pods}, map[string]*corev1.PodSpec{crashLoopRepairRevision: running}))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil || len(ua.Update.Items) != 1 || !ua.Update.Items[0].StartingFresh {
				t.Fatalf("the off-target row must be a fresh update start; actions = %v", actionKinds(d))
			}
			item := ua.Update.Items[0]
			if item.CoordGateExempt != tc.exempt || item.ReplacesDarkPodSet != tc.dark {
				t.Fatalf("CoordGateExempt = %v, ReplacesDarkPodSet = %v, want %v and %v", item.CoordGateExempt, item.ReplacesDarkPodSet, tc.exempt, tc.dark)
			}
		})
	}
}

// TestPlan_Update_InstancesOutOfRotationListedFirst: among the fresh
// starts a pass may admit, those whose pod set has no routed pod in
// rotation are listed first, in plan order among themselves, and the
// rest keep plan order, under a surge, a recreate and an in-place
// strategy alike. An Instance whose promoted pod stopped passing
// readiness is listed first from the moment it left rotation: inside its
// grace the start is still charged and consulted like any other, neither
// dark nor gate-exempt; past it the start is dark as well. Two such
// Instances keep their index order ahead of every serving peer, a serving
// fleet keeps plain index order, and a gang is read on its routed leader:
// a worker out of rotation beside a serving leader keeps the gang's place.
func TestPlan_Update_InstancesOutOfRotationListedFirst(t *testing.T) {
	target := updateTarget()
	member := func(pod *corev1.Pod, runner string) *corev1.Pod {
		pod.Name += "-" + runner
		pod.Labels[query.LabelRunner] = runner
		return pod
	}
	gang := []types.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}}
	single := []types.RunnerPlan{{Name: "default", Size: 1}}
	shapes := []struct {
		name    string
		runners []types.RunnerPlan
		pods    map[int32][]*corev1.Pod
		want    []int32
		dark    map[int32]bool
	}{
		{name: "one Instance unready inside the grace", runners: single,
			pods: map[int32][]*corev1.Pod{3: {unreadyEnginePod(3, 10*time.Second)}}, want: []int32{3, 0, 1, 2}},
		{name: "one Instance unready past the grace", runners: single,
			pods: map[int32][]*corev1.Pod{3: {unreadyEnginePod(3, 2*time.Minute)}}, want: []int32{3, 0, 1, 2}, dark: map[int32]bool{3: true}},
		{name: "two Instances out of rotation", runners: single,
			pods: map[int32][]*corev1.Pod{1: {unreadyEnginePod(1, 10*time.Second)}, 3: {unreadyEnginePod(3, 2*time.Minute)}}, want: []int32{1, 3, 0, 2}, dark: map[int32]bool{3: true}},
		{name: "every Instance serving", runners: single, want: []int32{0, 1, 2, 3}},
		{name: "gang leader out of rotation", runners: gang,
			pods: map[int32][]*corev1.Pod{3: {member(unreadyEnginePod(3, 10*time.Second), "leader"), member(servingEnginePod(3), "worker")}}, want: []int32{3, 0, 1, 2}},
		{name: "gang worker out of rotation beside a serving leader", runners: gang,
			pods: map[int32][]*corev1.Pod{3: {member(servingEnginePod(3), "leader"), member(unreadyEnginePod(3, 10*time.Second), "worker")}}, want: []int32{0, 1, 2, 3}},
	}
	for _, sc := range []struct {
		strategy       types.UpdateStrategyType
		surge, unavail int32
	}{
		{types.UpdateStrategySurgeThenDrain, 1, 0},
		{types.UpdateStrategyRecreatePod, 0, 1},
		{types.UpdateStrategyInPlaceIfPossible, 0, 1},
	} {
		for _, shape := range shapes {
			t.Run(string(sc.strategy)+"/"+shape.name, func(t *testing.T) {
				in := minimalInput(t)
				forbidMutations(t, &in)
				in.StuckPodGrace = time.Minute
				maxSurge, maxUnavailable := intstr.FromInt32(sc.surge), intstr.FromInt32(sc.unavail)
				plan := types.ComponentPlan{
					Component: types.ComponentEngine,
					Replicas:  4,
					UpdateStrategy: types.UpdateStrategy{Type: sc.strategy,
						RollingUpdate: &types.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable}},
				}
				pods := map[int32][]*corev1.Pod{}
				for idx := int32(0); idx < 4; idx++ {
					plan.Instances = append(plan.Instances, types.InstancePlan{Index: idx, Incarnation: 1, Runners: shape.runners})
					if set, ok := shape.pods[idx]; ok {
						pods[idx] = set
					} else if len(shape.runners) > 1 {
						pods[idx] = []*corev1.Pod{member(servingEnginePod(idx), "leader"), member(servingEnginePod(idx), "worker")}
					} else {
						pods[idx] = []*corev1.Pod{servingEnginePod(idx)}
					}
					serving := int32(0)
					for _, pod := range pods[idx] {
						if podreadiness.ReadyAndServing(pod) {
							serving++
						}
					}
					in.ObservedState.InstanceStatuses = append(in.ObservedState.InstanceStatuses, types.InstanceStatus{
						Index: idx, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: int32(len(shape.runners)), ServingPodCount: serving, RunningRevision: crashLoopRepairRevision,
					})
				}
				// The running revision's template differs from the desired one
				// by its image alone, the diff an in-place strategy keeps in place.
				running := in.DesiredSpec.PodSpec.DeepCopy()
				running.Containers[0].Image = "test:v0"
				d := planTargetOrFail(t, in, plan, target, planSnapshotWithRevisions(in, pods, map[string]*corev1.PodSpec{crashLoopRepairRevision: running}))
				ua := findAction(d, workload.ActionUpdate)
				if ua == nil {
					t.Fatalf("expected an Update action, got %v", actionKinds(d))
				}
				var got []int32
				for _, item := range ua.Update.Items {
					got = append(got, item.Instance.Index)
					if !item.StartingFresh {
						t.Errorf("instance %d: every off-target Ready row is a fresh start", item.Instance.Index)
					}
					if dark := shape.dark[item.Instance.Index]; item.ReplacesDarkPodSet != dark {
						t.Errorf("instance %d: ReplacesDarkPodSet = %v, want %v: the order is read on rotation, the budget on the grace", item.Instance.Index, item.ReplacesDarkPodSet, dark)
					}
					if exempt := shape.dark[item.Instance.Index] && sc.strategy != types.UpdateStrategySurgeThenDrain; item.CoordGateExempt != exempt {
						t.Errorf("instance %d: CoordGateExempt = %v, want %v: a start inside the grace is consulted like any other", item.Instance.Index, item.CoordGateExempt, exempt)
					}
				}
				if !reflect.DeepEqual(got, shape.want) {
					t.Fatalf("update items = %v, want %v: the Instances out of rotation first, then plan order", got, shape.want)
				}
			})
		}
	}
}

// TestPlan_Update_InstanceOutOfRotationKeepsThePartition: the preference
// for an Instance out of rotation orders the starts the partition admits
// and never pulls in one it holds. Four Instances under a partition of
// two, from the user's rollingUpdate and from a canary step's pacing
// alike: an Instance out of rotation among the two held is not listed,
// and one among the two released is listed ahead of its released peer.
func TestPlan_Update_InstanceOutOfRotationKeepsThePartition(t *testing.T) {
	target := updateTarget()
	for _, src := range []struct {
		name   string
		pacing bool
	}{{"rollingUpdate partition", false}, {"canary step partition", true}} {
		for _, tc := range []struct {
			name    string
			unready int32
			want    []int32
		}{
			{name: "held Instance out of rotation stays held", unready: 1, want: []int32{2, 3}},
			{name: "released Instance out of rotation goes first", unready: 3, want: []int32{3, 2}},
		} {
			t.Run(src.name+"/"+tc.name, func(t *testing.T) {
				in := minimalInput(t)
				forbidMutations(t, &in)
				in.StuckPodGrace = time.Minute
				partition := int32(2)
				maxSurge := intstr.FromInt32(1)
				plan := types.ComponentPlan{
					Component: types.ComponentEngine,
					Replicas:  4,
					UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain,
						RollingUpdate: &types.RollingUpdate{MaxSurge: &maxSurge}},
				}
				if src.pacing {
					in.DesiredSpec.Pacing = &types.WorkloadPacing{Partition: &partition}
				} else {
					plan.UpdateStrategy.RollingUpdate.Partition = &partition
				}
				pods := map[int32][]*corev1.Pod{}
				for idx := int32(0); idx < 4; idx++ {
					plan.Instances = append(plan.Instances, types.InstancePlan{Index: idx, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}})
					serving := int32(1)
					if idx == tc.unready {
						pods[idx] = []*corev1.Pod{unreadyEnginePod(idx, 10*time.Second)}
						serving = 0
					} else {
						pods[idx] = []*corev1.Pod{servingEnginePod(idx)}
					}
					in.ObservedState.InstanceStatuses = append(in.ObservedState.InstanceStatuses, types.InstanceStatus{
						Index: idx, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, ServingPodCount: serving, RunningRevision: crashLoopRepairRevision,
					})
				}
				d := planTargetOrFail(t, in, plan, target, planSnapshot(in, pods))
				ua := findAction(d, workload.ActionUpdate)
				if ua == nil {
					t.Fatalf("expected an Update action, got %v", actionKinds(d))
				}
				var got []int32
				for _, item := range ua.Update.Items {
					got = append(got, item.Instance.Index)
				}
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("update items = %v, want %v: the partition chooses who is listed, the rotation reading orders them", got, tc.want)
				}
			})
		}
	}
}

// TestPlan_Update_DarkInstanceOnTheTargetHoldsNoSlot: an Instance the roll
// already moved onto the target whose promoted pod served and then failed
// readiness for the stuck-pod grace is not among the rolled Instances the
// roll waits on: it holds no slot in either arm's budget and no floor
// stands on its account, so the roll's next fresh start is selected with
// nothing named against it, under a drain-first and a surge strategy
// alike. Inside the grace the Instance is listed, and the pass wakes for
// the grace left rather than for the next tick.
func TestPlan_Update_DarkInstanceOnTheTargetHoldsNoSlot(t *testing.T) {
	target := updateTarget()
	const grace = time.Minute
	strategies := []struct {
		name           string
		strategy       types.UpdateStrategyType
		surge, unavail int
	}{
		{"recreate", types.UpdateStrategyRecreatePod, 0, 1},
		{"in-place", types.UpdateStrategyInPlaceIfPossible, 0, 1},
		{"surge", types.UpdateStrategySurgeThenDrain, 1, 0},
	}
	windows := []struct {
		name       string
		unreadyFor time.Duration
		listed     bool
	}{
		{"past the grace", 2 * grace, false},
		{"inside the grace", grace / 4, true},
	}
	for _, sc := range strategies {
		for _, tc := range windows {
			t.Run(sc.name+"/"+tc.name, func(t *testing.T) {
				in := minimalInput(t)
				forbidMutations(t, &in)
				in.StuckPodGrace = grace
				wake := &types.PassWake{}
				in.PassWake = wake
				in.DesiredSpec.Replicas = 2
				readySince := metav1.NewTime(time.Now().Add(-time.Hour))
				in.ObservedState.InstanceStatuses = []types.InstanceStatus{
					{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, ServingPodCount: 0, RunningRevision: target.Name, ReadySince: &readySince},
					{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, ServingPodCount: 1, RunningRevision: crashLoopRepairRevision},
				}
				plan := types.ComponentPlan{
					Component: types.ComponentEngine,
					Replicas:  2,
					UpdateStrategy: types.UpdateStrategy{
						Type:          sc.strategy,
						RollingUpdate: &types.RollingUpdate{MaxSurge: intOrStringInt(sc.surge), MaxUnavailable: intOrStringInt(sc.unavail)},
					},
					Instances: []types.InstancePlan{
						{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
						{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
					},
				}
				dark := unreadyEnginePod(0, tc.unreadyFor)
				dark.Labels[query.LabelRevisionHash] = query.RevisionFromName(target.Name).Hash()
				// The running revision's template differs from the desired one
				// by its image alone, the diff an in-place strategy keeps in place.
				running := in.DesiredSpec.PodSpec.DeepCopy()
				running.Containers[0].Image = "test:v0"
				d := planTargetOrFail(t, in, plan, target, planSnapshotWithRevisions(in,
					map[int32][]*corev1.Pod{0: {dark}, 1: {servingEnginePod(1)}}, map[string]*corev1.PodSpec{crashLoopRepairRevision: running}))
				ua := findAction(d, workload.ActionUpdate)
				if ua == nil {
					t.Fatalf("the roll must select the fresh start on Instance 1; actions = %v", actionKinds(d))
				}
				if len(ua.Update.Items) != 1 || ua.Update.Items[0].Instance.Index != 1 || !ua.Update.Items[0].StartingFresh {
					t.Fatalf("items = %+v, want the one fresh start on Instance 1", ua.Update.Items)
				}
				if tc.listed {
					if len(ua.Update.RolledNotServing) != 1 || ua.Update.RolledNotServing[0] != 0 {
						t.Fatalf("rolledNotServing = %v, want Instance 0 while its pod is inside the grace", ua.Update.RolledNotServing)
					}
					if left := wake.Pending(); left <= 0 || left > grace-tc.unreadyFor {
						t.Fatalf("wake = %s, want the grace left (at most %s)", left, grace-tc.unreadyFor)
					}
					return
				}
				if len(ua.Update.RolledNotServing) != 0 {
					t.Fatalf("rolledNotServing = %v, want none: a dark Instance on the target holds no slot", ua.Update.RolledNotServing)
				}
				if d.StandingHold != nil {
					t.Fatalf("no hold stands on a dark Instance, got %+v", d.StandingHold)
				}
				if left := wake.Pending(); left != 0 {
					t.Fatalf("no wake is owed for a dark Instance; got %s", left)
				}
			})
		}
	}
}

// TestPlan_Update_InPlaceStartOverADarkPodIsAFallbackRecreate: a fresh
// InPlaceIfPossible start whose diff against the running revision replaces
// no container, over an Instance whose pod has failed readiness for the
// stuck-pod grace, is selected as a fallback recreate: the mechanism the
// admission consults, since the patch would leave the pod as dark as it
// found it. A diff that changes the image keeps the in-place mechanism,
// because the kubelet's restart is what the step waits on, and so does a
// pod still inside its grace.
func TestPlan_Update_InPlaceStartOverADarkPodIsAFallbackRecreate(t *testing.T) {
	target := updateTarget()
	const grace = time.Minute
	cases := []struct {
		name         string
		runningImage string
		unreadyFor   time.Duration
		recreate     bool
	}{
		{"metadata diff over a dark pod", "test:v1", 2 * grace, true},
		{"image diff over a dark pod", "test:v0", 2 * grace, false},
		{"metadata diff inside the grace", "test:v1", grace / 4, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.StuckPodGrace = grace
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, PodCount: 1, ServingPodCount: 0, RunningRevision: crashLoopRepairRevision},
			}
			plan := types.ComponentPlan{
				Component: types.ComponentEngine,
				Replicas:  1,
				UpdateStrategy: types.UpdateStrategy{
					Type:          types.UpdateStrategyInPlaceIfPossible,
					RollingUpdate: &types.RollingUpdate{MaxSurge: intOrStringInt(0), MaxUnavailable: intOrStringInt(1)},
				},
				Instances: []types.InstancePlan{
					{Index: 0, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
				},
			}
			running := in.DesiredSpec.PodSpec.DeepCopy()
			running.Containers[0].Image = tc.runningImage
			dark := unreadyEnginePod(0, tc.unreadyFor)
			dark.Spec.Containers[0].Image = tc.runningImage
			d := planTargetOrFail(t, in, plan, target, planSnapshotWithRevisions(in,
				map[int32][]*corev1.Pod{0: {dark}}, map[string]*corev1.PodSpec{crashLoopRepairRevision: running}))
			ua := findAction(d, workload.ActionUpdate)
			if ua == nil || len(ua.Update.Items) != 1 || !ua.Update.Items[0].StartingFresh {
				t.Fatalf("want the one fresh start on Instance 0; actions = %v", actionKinds(d))
			}
			if got := ua.Update.Items[0].RecreateFallback; got != tc.recreate {
				t.Fatalf("RecreateFallback = %v, want %v", got, tc.recreate)
			}
		})
	}
}
