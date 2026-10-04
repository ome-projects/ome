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
// resolve rather than stacking a second lot of extra capacity.
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

			if a := findAction(d, workload.ActionMigrate); a != nil {
				t.Fatalf("a migration was driven while the gang surge is in flight: %+v", a.Migration)
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
			if findAction(d, workload.ActionMigrate) == nil {
				t.Fatalf("with no surge counted the same record must be driven; got %v", actionKinds(d))
			}
		})
	}
}

// TestPlan_MigrationRequestWaitsForASurgeCycleToResolve: a fresh
// migration adds one more surge Instance, so it is not driven while a
// SurgeThenDrain cycle is already counted — at either of its steps. The
// request waits in its record rather than stacking a second lot of extra
// capacity on the Component.
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

			if a := findAction(d, workload.ActionMigrate); a != nil {
				t.Fatalf("a migration was driven while the surge is in flight: %+v", a.Migration)
			}
		})
	}
}

// A scale-down keeps the rows on a sound revision ahead of the rows on a
// revision the retry ladder reads as failing, whatever the phases say: a
// Ready row there is the pause between crashes, and keeping it over the
// running revision's row would land the push by attrition and leave the
// Component on a revision the ladder has given up on. Within each class
// the serving rows are kept first, then the oldest index.
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
		{"rows on a sound revision keep the oldest index",
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
