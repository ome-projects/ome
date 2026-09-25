package workload_test

import (
	"testing"
	"time"

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
