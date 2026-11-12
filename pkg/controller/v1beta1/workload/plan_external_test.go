package workload_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestPlan_MigrationRequestWaitsForAGangSurgeToResolve: a fresh
// migration record adds one more surge Instance, so it is not driven
// while another surge is already counted. The source carries the Surge
// step for as long as the replacement gang exists — through the
// marker's own retirement — so the request waits for the pair to
// resolve rather than stacking a second lot of extra capacity, and the
// plan selects it as waiting for the roll so the pass names the wait.
func TestPlan_MigrationRequestWaitsForAGangSurgeToResolve(t *testing.T) {
	for _, step := range []string{
		types.UpdateStepGangSurgeTarget,
		types.UpdateStepGangSurgeTargetCleanup,
	} {
		t.Run(step, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.ObservedState.InstanceStatuses = gangSurgePair(time.Now(), 1, step)
			in.ObservedState.Migrations = []types.MigrationRecord{{
				RequestUUID: "request-a",
				Trigger:     types.MigrationTriggerManual,
				Phase:       types.MigrationPhaseAccepted,
			}}
			plan, err := workload.BuildPlan(types.ComponentEngine, in.DesiredSpec, in.ObservedState)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			plan.MigrationMode = types.MigrationModeSurge

			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: gangPairTarget,
			}}
			sourcePod := enginePod("llama-70b", "prod", 0)
			surgePod := enginePod("llama-70b", "prod", 1)
			surgePod.Labels[query.LabelRevisionHash] = query.RevisionFromName(gangPairTarget).Hash()
			d := planTargetOrFail(t, in, plan, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {sourcePod}, 1: {surgePod}}))

			if a := findAction(d, workload.ActionMigrate); a == nil || a.Migration == nil || a.Migration.Head != nil ||
				a.Migration.WaitingForRoll == nil || a.Migration.WaitingForRoll.RequestUUID != "request-a" {
				t.Fatalf("the record must wait for the roll, not be driven, while the gang surge is in flight: %+v", a)
			}

			// Counter-case: the same record with no surge counted is
			// driven, so the deferral above is the surge's doing and not
			// a record the selector would have skipped anyway.
			in.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: gangPairTarget},
			}
			free, err := workload.BuildPlan(types.ComponentEngine, in.DesiredSpec, in.ObservedState)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			free.MigrationMode = types.MigrationModeSurge
			d = planTargetOrFail(t, in, free, target, planSnapshot(in, map[int32][]*corev1.Pod{0: {sourcePod}}))
			if a := findAction(d, workload.ActionMigrate); a == nil || a.Migration == nil || a.Migration.Head == nil {
				t.Fatalf("with no surge counted the same record must be driven; got %v", actionKinds(d))
			}
		})
	}
}

// TestPlan_MigrationRequestWaitsForASurgeCycleToResolve: a fresh
// migration adds one more surge Instance, so it is not driven while a
// SurgeThenDrain cycle is already counted — at either of its steps. The
// request waits in its record rather than stacking a second lot of extra
// capacity on the Component, selected as waiting for the roll so the
// pass names the wait on it.
func TestPlan_MigrationRequestWaitsForASurgeCycleToResolve(t *testing.T) {
	for _, step := range []string{types.UpdateStepSurge, types.UpdateStepSurgeDrain} {
		t.Run(step, func(t *testing.T) {
			in := minimalInput(t)
			forbidMutations(t, &in)
			in.ObservedState.InstanceStatuses = surgeSourceRow(time.Now(), step, 1)
			in.ObservedState.Migrations = []types.MigrationRecord{{
				RequestUUID: "request-a",
				Trigger:     types.MigrationTriggerManual,
				Phase:       types.MigrationPhaseAccepted,
			}}
			plan, err := workload.BuildPlan(types.ComponentEngine, in.DesiredSpec, in.ObservedState)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			plan.MigrationMode = types.MigrationModeSurge

			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
				Namespace: "prod", Name: gangPairTarget,
			}}
			sourcePod := enginePod("llama-70b", "prod", 0)
			surgePod := enginePod("llama-70b", "prod", 1)
			surgePod.Labels[query.LabelRevisionHash] = query.RevisionFromName(gangPairTarget).Hash()
			d := planTargetOrFail(t, in, plan, target,
				planSnapshot(in, map[int32][]*corev1.Pod{0: {sourcePod}, 1: {surgePod}}))

			if a := findAction(d, workload.ActionMigrate); a == nil || a.Migration == nil || a.Migration.Head != nil ||
				a.Migration.WaitingForRoll == nil || a.Migration.WaitingForRoll.RequestUUID != "request-a" {
				t.Fatalf("the record must wait for the roll, not be driven, while the surge is in flight: %+v", a)
			}
		})
	}
}

// A scale-down keeps the rows on a sound revision ahead of the rows on a
// revision the retry ladder reads as failing, whatever the phases say: a
// Ready row there is the pause between crashes, and keeping it over the
// running revision's row would land the push by attrition and leave the
// Component on a revision the ladder has given up on. Within each class
// the serving rows are kept first, then the lowest index.
func TestBuildPlan_ScaleDownKeepsARunningRevisionRowOverAFailingRevisionOne(t *testing.T) {
	const running, pushed = "svc-engine-aaaaaaaa", "svc-engine-bbbbbbbb"
	now := metav1.Now()
	earlier := metav1.NewTime(now.Add(-time.Minute))
	held := []types.RetryBlock{{TargetRevision: pushed, State: types.RetryBlockHeld}}
	backoff := []types.RetryBlock{{TargetRevision: pushed, State: types.RetryBlockBackoff, NextRetryAt: &now}}
	readyOn := func(idx int32, rev string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseReady, RunningRevision: rev, PodCount: 1, ServingPodCount: 1, ReadySince: &earlier}
	}
	restartingOn := func(idx int32, rev string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseRestarting, RunningRevision: rev, PodCount: 1,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}}
	}
	remembering := func(idx int32, rev string) types.InstanceStatus {
		row := readyOn(idx, rev)
		row.LastFailure = &types.InstanceTermination{Reason: "CrashLoopBackOff", Time: now}
		return row
	}
	failedToward := func(idx int32, from, to string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseFailed, RunningRevision: from, TargetRevision: to, PodCount: 1}
	}
	cases := []struct {
		name   string
		rows   []types.InstanceStatus
		blocks []types.RetryBlock
		want   []int32
	}{
		{"a Ready row on a Held revision leaves before a Restarting row on the running revision",
			[]types.InstanceStatus{readyOn(0, pushed), restartingOn(1, running)}, held, []int32{1}},
		{"a Ready row on a Held revision leaves before a Ready row on the running revision whatever the index order",
			[]types.InstanceStatus{readyOn(0, pushed), readyOn(1, running)}, held, []int32{1}},
		{"a Backoff block reads the revision the same way",
			[]types.InstanceStatus{readyOn(0, pushed), restartingOn(1, running)}, backoff, []int32{1}},
		{"a Ready row remembering the crash a retry in flight answers leaves before one that does not",
			[]types.InstanceStatus{remembering(0, running), readyOn(1, running)}, []types.RetryBlock{{TargetRevision: running, State: types.RetryBlockRetryInProgress}}, []int32{1}},
		{"a failure record on a revision with no block does not rank the row down",
			[]types.InstanceStatus{remembering(0, running), readyOn(1, running)}, nil, []int32{0}},
		{"a Failed row pinned to the failing revision leaves before a serving row on it",
			[]types.InstanceStatus{failedToward(0, running, pushed), readyOn(1, pushed)}, held, []int32{1}},
		{"with every row on the failing revision the serving row is kept",
			[]types.InstanceStatus{readyOn(0, pushed), restartingOn(1, pushed)}, held, []int32{0}},
		{"rows on a sound revision keep the lowest index",
			[]types.InstanceStatus{readyOn(0, running), readyOn(1, running)}, held, []int32{0}},
		{"an attempt in flight on the revision does not read it as failing",
			[]types.InstanceStatus{readyOn(0, pushed), readyOn(1, running)}, []types.RetryBlock{{TargetRevision: pushed, State: types.RetryBlockRetryInProgress}}, []int32{0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			observed := types.WorkloadObservedState{InstanceStatuses: tc.rows, RetryBlocks: tc.blocks}
			plan, err := workload.BuildPlan(types.ComponentEngine, types.WorkloadDesiredSpec{Replicas: 1}, observed)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			got := make([]int32, 0, len(plan.Instances))
			for _, inst := range plan.Instances {
				got = append(got, inst.Index)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Fatalf("the plan must keep the running revision's row over a failing revision's (-want +got):\n%s", diff)
			}
		})
	}
}

// A scale-down reads the cordoned Instances the adapter projects: the
// Instance with a pod on a cordoned node leaves first even when it is the
// healthiest and lowest index, and the extras are exactly the rows the
// plan dropped.
func TestBuildPlan_ScaleDownRemovesTheCordonedInstanceFirst(t *testing.T) {
	healthy := func(idx int32) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseReady, PodCount: 1, ServingPodCount: 1}
	}
	creating := types.InstanceStatus{Index: 1, Phase: types.InstancePhaseCreating, PodCount: 1,
		Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate, Step: "CreatePods"}}
	rows := []types.InstanceStatus{healthy(0), creating, healthy(2), healthy(3)}
	cases := []struct {
		name     string
		rows     []types.InstanceStatus
		cordoned map[int32]struct{}
		replicas int32
		extras   []int32
	}{
		{"no cordoned node: the Creating row and then the highest index leave", rows, nil, 2, []int32{1, 3}},
		{"the healthy lowest index on a cordoned node leaves first", rows, map[int32]struct{}{0: {}}, 3, []int32{0}},
		{"then the usual order among the rest", rows, map[int32]struct{}{0: {}}, 2, []int32{0, 1}},
		{"a Creating row with no operation leaves before the cordoned row: nothing drives it",
			[]types.InstanceStatus{healthy(0), {Index: 1, Phase: types.InstancePhaseCreating, PodCount: 1}, healthy(2), healthy(3)},
			map[int32]struct{}{0: {}}, 3, []int32{1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			observed := types.WorkloadObservedState{InstanceStatuses: tc.rows, CordonedInstances: tc.cordoned}
			plan, err := workload.BuildPlan(types.ComponentEngine, types.WorkloadDesiredSpec{Replicas: tc.replicas}, observed)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			if diff := cmp.Diff(tc.extras, workload.ScaleDownExtras(tc.rows, plan)); diff != "" {
				t.Fatalf("scale-down extras (-want +got):\n%s", diff)
			}
		})
	}
}

// A move that expires restores its source and releases its replacement
// index, leaving that row Creating with no operation. The plan that
// follows reads the released row as the one extra to retire, whatever
// else stands beside it: a Failed attempt on a revision in backoff keeps
// its place in the count, and with it the pod that could serve again.
func TestBuildPlan_ExpiredMoveReleasesTheExtraBesideAFailedAttempt(t *testing.T) {
	const running, pushed = "comp-rev-v1aaaaaa", "comp-rev-v2bbbbbb"
	serving := func(idx int32) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseReady, RunningRevision: running, PodCount: 1, ServingPodCount: 1}
	}
	rows := []types.InstanceStatus{
		{Index: 0, Phase: types.InstancePhaseFailed, RunningRevision: running, TargetRevision: pushed, PodCount: 1},
		serving(1),
		serving(2),
		{Index: 3, Phase: types.InstancePhaseCreating, PodCount: 1},
	}
	observed := types.WorkloadObservedState{
		InstanceStatuses: rows,
		RetryBlocks:      []types.RetryBlock{{TargetRevision: pushed, State: types.RetryBlockBackoff}},
	}
	plan, err := workload.BuildPlan(types.ComponentEngine, types.WorkloadDesiredSpec{Replicas: 3}, observed)
	if err != nil {
		t.Fatalf("BuildPlan: %v", err)
	}
	if diff := cmp.Diff([]int32{3}, workload.ScaleDownExtras(rows, plan)); diff != "" {
		t.Fatalf("the released replacement must be the extra, not the Failed attempt whose pod still runs (-want +got):\n%s", diff)
	}
}

// A scale-down keeps a source whose SurgeThenDrain replacement is coming
// up ahead of every steady row: removing it throws the replacement's
// progress away, and the steady Instance kept in its place still owes a
// surge and a drain. The highest Instances with no replacement coming up
// leave instead, and the source stays until its surge finishes. A source
// whose target revision the retry schedule holds has no replacement
// coming up and ranks as any other row.
func TestBuildPlan_ScaleDownKeepsTheSourceWhoseReplacementIsComingUp(t *testing.T) {
	const running, pushed = "svc-engine-aaaaaaaa", "svc-engine-bbbbbbbb"
	ready := func(idx int32, rev string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseReady, RunningRevision: rev, PodCount: 1, ServingPodCount: 1}
	}
	surging := func(idx int32, step string) types.InstanceStatus {
		return types.InstanceStatus{Index: idx, Phase: types.InstancePhaseUpdating, RunningRevision: running, TargetRevision: pushed,
			PodCount: 2, ServingPodCount: 1,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: step, TargetRevision: pushed, Strategy: types.UpdateStrategySurgeThenDrain}}
	}
	fleet := func(count int32, rolling map[int32]string) []types.InstanceStatus {
		rows := make([]types.InstanceStatus, 0, count)
		for idx := int32(0); idx < count; idx++ {
			if step, surges := rolling[idx]; surges {
				rows = append(rows, surging(idx, step))
				continue
			}
			rows = append(rows, ready(idx, running))
		}
		return rows
	}
	indices := func(from, to int32) []int32 {
		out := make([]int32, 0, to-from+1)
		for idx := from; idx <= to; idx++ {
			out = append(out, idx)
		}
		return out
	}
	held := []types.RetryBlock{{TargetRevision: pushed, State: types.RetryBlockHeld}}
	cases := []struct {
		name     string
		rows     []types.InstanceStatus
		blocks   []types.RetryBlock
		cordoned map[int32]struct{}
		replicas int32
		extras   []int32
		kept     []int32
	}{
		{name: "8 engines, the replacement of Instance 7 is coming up, the count drops to 6: Instances 5 and 6 leave",
			rows: fleet(8, map[int32]string{7: types.UpdateStepSurge}), replicas: 6, extras: []int32{5, 6}, kept: []int32{0, 1, 2, 3, 4, 7}},
		{name: "the replacement of Instance 7 is serving and its old pod drains: the same two leave",
			rows: fleet(8, map[int32]string{7: types.UpdateStepSurgeDrain}), replicas: 6, extras: []int32{5, 6}, kept: []int32{0, 1, 2, 3, 4, 7}},
		{name: "16 engines, the replacements of Instances 2 and 3 are coming up, the count drops to 8: Instances 8 to 15 leave",
			rows: fleet(16, map[int32]string{2: types.UpdateStepSurge, 3: types.UpdateStepSurge}), replicas: 8, extras: indices(8, 15), kept: []int32{0, 1, 2, 3, 4, 5, 6, 7}},
		{name: "the surge finished after the drop: Instance 7 stays on the new revision and the count holds at 6",
			rows: append(fleet(5, nil), ready(7, pushed)), replicas: 6, kept: []int32{0, 1, 2, 3, 4, 7}},
		{name: "the count drops below the sources whose replacement is coming up: the lowest of them stays",
			rows: fleet(8, map[int32]string{6: types.UpdateStepSurge, 7: types.UpdateStepSurge}), replicas: 1, extras: []int32{0, 1, 2, 3, 4, 5, 7}, kept: []int32{6}},
		{name: "a source whose target revision is held has no replacement coming up: it leaves before a steady row",
			rows: fleet(8, map[int32]string{7: types.UpdateStepSurge}), blocks: held, replicas: 6, extras: []int32{6, 7}, kept: []int32{0, 1, 2, 3, 4, 5}},
		{name: "a source on a cordoned node keeps its place while its replacement is coming up",
			rows: fleet(8, map[int32]string{7: types.UpdateStepSurge}), cordoned: map[int32]struct{}{7: {}}, replicas: 6, extras: []int32{5, 6}, kept: []int32{0, 1, 2, 3, 4, 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			observed := types.WorkloadObservedState{InstanceStatuses: tc.rows, RetryBlocks: tc.blocks, CordonedInstances: tc.cordoned}
			plan, err := workload.BuildPlan(types.ComponentEngine, types.WorkloadDesiredSpec{Replicas: tc.replicas}, observed)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			if diff := cmp.Diff(tc.extras, workload.ScaleDownExtras(tc.rows, plan)); diff != "" {
				t.Fatalf("the scale-down must remove the highest Instances with no replacement coming up (-want +got):\n%s", diff)
			}
			kept := make([]int32, 0, len(plan.Instances))
			for _, inst := range plan.Instances {
				kept = append(kept, inst.Index)
			}
			if diff := cmp.Diff(tc.kept, kept); diff != "" {
				t.Fatalf("the plan must keep the source whose replacement is coming up (-want +got):\n%s", diff)
			}
		})
	}
}
