package canary

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// rebuiltFleetFixture is a four-engine unit at its first manual step, the
// split serving, whose engine replica was deleted and projected again: the
// projection created the new replica pointed at the stable revision, and
// its four Instances are being built for the first time, no revision run
// and no pod yet. The revisions the deleted replica minted are the
// service's and stay.
func rebuiltFleetFixture(t *testing.T) (*v1beta1.InferenceService, client.Client) {
	t.Helper()
	isvc, _ := fixtureServingManual(t)
	ns, n4 := "default", 4
	isvc.Namespace = ns
	isvc.UID = types.UID(isvc.Name + "-isvc-uid")
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"}}
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).TargetID = activeCanaryTargetID(isvc, rollout.CanaryGroup(isvc, rollout.Policies{}))

	replica := ir(ns, isvc.Name, v1beta1.EngineComponent, "new")
	replica.UID = canaryIRUID(replica.Name) + "-2"
	replica.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))}
	replica.Spec.ParentRef = &v1beta1.ParentReference{Name: isvc.Name}
	replica.Spec.Replicas = ptr.To(int32(4))
	replica.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: ptr.To(int32(2)), RollbackToRevision: ptr.To(isvc.Name + "-engine-old")}
	for i := int32(0); i < 4; i++ {
		replica.Status.InstanceStatuses = append(replica.Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{
			Index: i, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: isvc.Name + "-engine-old",
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate, TargetRevision: isvc.Name + "-engine-old"},
		})
	}
	replica.Status.Replicas = 4
	objs := []runtime.Object{isvc, replica,
		serviceOwned(canaryControllerRevision(ns, isvc.Name, "engine", "old", 1), isvc),
		serviceOwned(canaryControllerRevision(ns, isvc.Name, "engine", "new", 2), isvc),
	}
	return isvc, fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).WithStatusSubresource(replica).Build()
}

// landOnStable stands every Instance of the engine's new replica Ready on
// the stable revision, with one serving pod each, as the replica controller
// publishes a fleet whose first build finished.
func landOnStable(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
	t.Helper()
	ctx := context.Background()
	replica := mustReplica(t, c, isvc, v1beta1.EngineComponent)
	for i := range replica.Status.InstanceStatuses {
		row := &replica.Status.InstanceStatuses[i]
		row.Phase, row.RunningRevision, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceReady, isvc.Name+"-engine-old", "", nil
		row.PodCount, row.ServingPodCount, row.AvailablePodCount = 1, 1, 1
		if err := c.Create(ctx, canaryPod(isvc.Namespace, isvc.Name, "engine", "old", "old-"+string(rune('0'+row.Index)))); err != nil {
			t.Fatal(err)
		}
	}
	replica.Status.ReadyReplicas, replica.Status.ServingReplicas, replica.Status.AvailableReplicas = 4, 4, 4
	if err := c.Status().Update(ctx, replica); err != nil {
		t.Fatal(err)
	}
}

func rebuiltFleetTarget(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) string {
	t.Helper()
	replica := mustReplica(t, c, isvc, v1beta1.EngineComponent)
	if replica.Spec.Pacing == nil || replica.Spec.Pacing.RollbackToRevision == nil {
		return ""
	}
	return *replica.Spec.Pacing.RollbackToRevision
}

// expectTargetKeptForTwoPasses runs two passes and expects the new replica
// to stay pointed at the stable revision and the unit at its first step;
// why names what the target is waiting for.
func expectTargetKeptForTwoPasses(t *testing.T, deps DispatchDeps, c client.Client, isvc *v1beta1.InferenceService, stable, why string) {
	t.Helper()
	for pass := 0; pass < 2; pass++ {
		if _, err := Dispatch(context.Background(), deps); err != nil {
			t.Fatalf("Dispatch pass %d: %v", pass, err)
		}
		if got := rebuiltFleetTarget(t, c, isvc); got != stable {
			t.Fatalf("pass %d: the new replica must stay pointed at %s while %s, got %q", pass, stable, why, got)
		}
		cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
		if got := rollout.StateOf(cs, phaseOf(isvc), 2); got != rollout.CanaryStateServing || cs.CurrentStep != 0 {
			t.Fatalf("pass %d: the unit must keep its first step, got %s at step %d", pass, got, cs.CurrentStep)
		}
	}
}

// A replica projected again under a live step was created pointed at the
// stable revision. The dispatch keeps that target while an Instance of the
// new replica has still to run a revision, and clears it, as it clears any
// target no rollback holds, once every Instance runs the stable revision:
// from then the step's partition holds the Instances on the revision they
// run and the step stages its share. The unit keeps its step throughout.
func TestDispatch_AFleetRebuiltUnderAStepKeepsTheStableTargetUntilEveryInstanceRan(t *testing.T) {
	isvc, c := rebuiltFleetFixture(t)
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	stable := isvc.Name + "-engine-old"
	expectTargetKeptForTwoPasses(t, deps, c, isvc, stable, "its Instances are built")
	landOnStable(t, c, isvc)
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch after the fleet landed: %v", err)
	}
	if got := rebuiltFleetTarget(t, c, isvc); got != "" {
		t.Fatalf("every Instance runs the stable revision, so the target must be cleared for the step to stage its share, got %q", got)
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if got := rollout.StateOf(cs, phaseOf(isvc), 2); got != rollout.CanaryStateServing || cs.CurrentStep != 0 {
		t.Fatalf("after the fleet landed the unit must keep its first step, got %s at step %d", got, cs.CurrentStep)
	}
}

// failedFirstBuild stands Instances 0, 2 and 3 of the engine's new replica
// Ready on the stable revision and leaves Instance 1 Failed with no revision
// run: its first build ended in a failure the replica controller recorded,
// and its retry has not started.
func failedFirstBuild(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
	t.Helper()
	ctx := context.Background()
	replica := mustReplica(t, c, isvc, v1beta1.EngineComponent)
	for i := range replica.Status.InstanceStatuses {
		row := &replica.Status.InstanceStatuses[i]
		if row.Index == 1 {
			row.Phase, row.RunningRevision, row.Operation = v1beta1.OMENativeInstanceFailed, "", nil
			row.LastFailure = &v1beta1.InstanceTermination{PodName: isvc.Name + "-engine-1-0", Reason: "ImagePullBackOff"}
			continue
		}
		row.Phase, row.RunningRevision, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceReady, isvc.Name+"-engine-old", "", nil
		row.PodCount, row.ServingPodCount, row.AvailablePodCount = 1, 1, 1
		if err := c.Create(ctx, canaryPod(isvc.Namespace, isvc.Name, "engine", "old", "old-"+string(rune('0'+row.Index)))); err != nil {
			t.Fatal(err)
		}
	}
	replica.Status.ReadyReplicas, replica.Status.ServingReplicas, replica.Status.AvailableReplicas = 3, 3, 3
	if err := c.Status().Update(ctx, replica); err != nil {
		t.Fatal(err)
	}
}

// A first build that failed has run no revision either: the retry the
// replica controller opens for it is a fresh start, built on the revision
// the replica is pointed at, so the target stays until that Instance has
// run the stable revision too. Without it the retry would be built at the
// canary revision, one more on the canary side than the step stages.
func TestDispatch_AFailedFirstBuildKeepsTheStableTargetUntilItsRetryLands(t *testing.T) {
	isvc, c := rebuiltFleetFixture(t)
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	stable := isvc.Name + "-engine-old"
	failedFirstBuild(t, c, isvc)
	expectTargetKeptForTwoPasses(t, deps, c, isvc, stable, "a failed first build awaits its retry")
	replica := mustReplica(t, c, isvc, v1beta1.EngineComponent)
	row := &replica.Status.InstanceStatuses[1]
	row.Phase, row.RunningRevision, row.LastFailure = v1beta1.OMENativeInstanceReady, stable, nil
	row.PodCount, row.ServingPodCount, row.AvailablePodCount = 1, 1, 1
	if err := c.Create(context.Background(), canaryPod(isvc.Namespace, isvc.Name, "engine", "old", "old-1")); err != nil {
		t.Fatal(err)
	}
	replica.Status.ReadyReplicas, replica.Status.ServingReplicas, replica.Status.AvailableReplicas = 4, 4, 4
	if err := c.Status().Update(context.Background(), replica); err != nil {
		t.Fatal(err)
	}
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch after the retry landed: %v", err)
	}
	if got := rebuiltFleetTarget(t, c, isvc); got != "" {
		t.Fatalf("every Instance runs the stable revision, so the target must be cleared, got %q", got)
	}
}

// Only a target that names the unit's stable revision is kept. A replica
// pointed at any other revision while its Instances are built is cleared
// at once, as any revert target no rollback holds.
func TestDispatch_ATargetNamingAnotherRevisionIsNotKeptWhileTheFleetBuilds(t *testing.T) {
	isvc, c := rebuiltFleetFixture(t)
	replica := mustReplica(t, c, isvc, v1beta1.EngineComponent)
	replica.Spec.Pacing.RollbackToRevision = ptr.To(isvc.Name + "-engine-other")
	if err := c.Update(context.Background(), replica); err != nil {
		t.Fatal(err)
	}
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	if _, err := Dispatch(context.Background(), deps); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got := rebuiltFleetTarget(t, c, isvc); got != "" {
		t.Fatalf("a target naming a revision other than the stable one must be cleared while the fleet builds, got %q", got)
	}
}
