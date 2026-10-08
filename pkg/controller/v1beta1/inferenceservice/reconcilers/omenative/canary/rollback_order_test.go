package canary

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A rollback moves traffic onto the stable revision only once that revision
// has serving capacity. With the held stable instance not Ready, the split
// that serves stays programmed while the revert brings stable capacity back;
// the first Ready stable instance takes the traffic.
func TestReconcile_RollbackWaitsForServingStableCapacity(t *testing.T) {
	isvc := pinActiveRun(canaryISVC(twoStep(), i32(3600)))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	mustReconcile(t, isvc, in) // final step: 100% on the canary revision, draining
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")

	// The held stable instance is not serving when the rollback lands.
	in.PerRevisionPods = map[string]int32{"new": 3}
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	res := mustReconcile(t, isvc, in)
	if !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the revert proceeds, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "new")

	// One instance is back on stable and Ready: traffic moves onto it.
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
	if res := mustReconcile(t, isvc, in); !res.RolledBack || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the revert is still in flight, got phase=%q res=%+v", phaseOf(isvc), res)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
}

// A secondary's rollback write follows the same rule on its own stable
// revision: a member whose stable revision has no serving capacity keeps its
// programmed split until it does. That split was written on the member's
// serving capacity alone, its target, because a stable revision with no
// serving pod carries no weight.
func TestReconcile_SecondaryRollbackWaitsForItsServingStableCapacity(t *testing.T) {
	isvc, in := pdInputs(map[string]int32{"new": 2, "old": 2}, bumpedDecoder())
	in.GroupReadyPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{
		v1beta1.EngineComponent:  {"new": 2, "old": 2},
		v1beta1.DecoderComponent: {"decnew": 2},
	}
	mustReconcile(t, isvc, in)
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	if res := mustReconcile(t, isvc, in); !res.RolledBack {
		t.Fatalf("the request rolls the unit back, got %+v", res)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"old": {percent: 100, protocol: pdStableProtocol},
	})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true},
	})
	in.GroupReadyPerRevisionPods[v1beta1.DecoderComponent] = map[string]int32{"decnew": 1, "decold": 1}
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decold": {percent: 100, protocol: pdStableProtocol},
	})
}

// The dispatcher-level rollback onto a stable revision with no serving pod:
// the split stays programmed, and the replica is still signaled to revert,
// since the revert is what brings stable capacity back.
func TestDispatch_RollbackKeepsTrafficOffAnEmptyStableRevision(t *testing.T) {
	ns := "default"
	n4 := 4
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace, isvc.Name = ns, "rollback-empty"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	g := rollout.CanaryGroup(isvc, rollout.Policies{})
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		TargetID:              activeCanaryTargetID(isvc, g),
		CanaryRevisionHash:    "new",
		StableRevisionHash:    "old",
		StepEnteredTime:       &metav1.Time{Time: metav1.Now().Time},
		ObservedTrafficWeight: 50,
	})
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhasePaused)
	setTraffic(isvc, v1beta1.EngineComponent, coordination.BuildTrafficTargets(isvc.Name, v1beta1.EngineComponent,
		canaryWeights("new", "old", "", "", 50)))
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-old"
	// The two held stable instances exist but neither is Ready.
	unready := func(name string) runtime.Object {
		pod := canaryPod(ns, isvc.Name, "engine", "old", name)
		pod.Status.Conditions = nil
		return pod
	}
	objs := []runtime.Object{isvc, engineIR,
		unready(isvc.Name + "-engine-0"),
		unready(isvc.Name + "-engine-1"),
		canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-2"),
		canaryPod(ns, isvc.Name, "engine", "new", isvc.Name+"-engine-3"),
		canaryControllerRevision(ns, isvc.Name, "engine", "old", 1),
		canaryControllerRevision(ns, isvc.Name, "engine", "new", 2),
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
	ctx := context.Background()
	annotateStored(t, c, isvc, constants.RolloutRollbackAnnotation, "true")
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: g}

	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch (rollback): %v", err)
	}
	if got := isvc.Status.Components[v1beta1.EngineComponent].RolloutPhase; got != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rejected pods still serve, got phase %q", got)
	}
	if tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic; len(tr) != 2 {
		t.Fatalf("traffic must not move onto a stable revision with no serving pod, got %+v", tr)
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: engineIR.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil || *got.Spec.Pacing.RollbackToRevision != isvc.Name+"-engine-old" {
		t.Fatalf("the revert must be signaled, got pacing=%+v", got.Spec.Pacing)
	}

	// A stable instance comes back Ready: traffic moves onto it.
	if err := c.Delete(ctx, canaryPod(ns, isvc.Name, "engine", "old", isvc.Name+"-engine-0")); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, canaryPod(ns, isvc.Name, "engine", "old", isvc.Name+"-engine-0")); err != nil {
		t.Fatal(err)
	}
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatalf("Dispatch (stable serving): %v", err)
	}
	expectSoleTarget(t, isvc, v1beta1.EngineComponent, "old")
}
