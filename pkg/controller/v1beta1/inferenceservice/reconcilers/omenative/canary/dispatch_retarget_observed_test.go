package canary

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A new target that lands while the rejected revision's pod is still the
// live source of an in-flight revert, with the revert's stable-revision
// replacement abandoned and Terminating at the surge slot, re-arms the unit
// at the new target and stages it on capacity. The Terminating replacement
// is neither capacity nor a stable identity, the rejected pod draws no
// traffic, and no later pass rolls the new target back; the ladder moves as
// soon as the new revision's capacity lands.
func TestDispatch_RetargetWhileRollingBackWithTerminatingReplacement(t *testing.T) {
	ns := "default"
	n2 := 2
	isvc := canaryISVC(twoStep(), nil)
	isvc.Namespace = ns
	isvc.Name = "retarget-terminating"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n2}}
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-old"
	irKey := types.NamespacedName{Namespace: ns, Name: engineIR.Name}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc, engineIR,
		canaryPod(ns, isvc.Name, "engine", "old", "old-0"),
		canaryPod(ns, isvc.Name, "engine", "new", "new-1"),
		canaryControllerRevision(ns, isvc.Name, "engine", "old", 1),
		canaryControllerRevision(ns, isvc.Name, "engine", "new", 2),
	).Build()
	ctx := context.Background()
	deps := DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{})}
	dispatch := func(pass string) Outcome {
		t.Helper()
		out, err := Dispatch(ctx, deps)
		if err != nil {
			t.Fatalf("Dispatch (%s): %v", pass, err)
		}
		return out
	}
	status := func() *v1beta1.CanaryStatus { return rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent) }
	rollbackTarget := func() *string {
		t.Helper()
		got := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, irKey, got); err != nil {
			t.Fatal(err)
		}
		if got.Spec.Pacing == nil {
			return nil
		}
		return got.Spec.Pacing.RollbackToRevision
	}
	stableTraffic := func() bool {
		tr := isvc.Status.Components[v1beta1.EngineComponent].Traffic
		return len(tr) == 1 && tr[0].Percent == 100 && tr[0].RevisionName == isvc.Name+"-engine-rev-old"
	}

	dispatch("serve")
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("step 0 must serve its split before the rollback, got %q", phaseOf(isvc))
	}
	annotateStored(t, c, isvc, constants.RolloutRollbackAnnotation, "true")
	dispatch("rollback")
	if cs := status(); cs.RolledBackRevisionHash != "new" || phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("the rollback must reject the canary revision, got %+v phase=%q", cs, phaseOf(isvc))
	}

	// The revert's stable replacement was created at the rejected pod's
	// other slot and abandoned when the new target superseded it: it is
	// Terminating and was never Ready, while the rejected pod still runs.
	abandoned := canaryPod(ns, isvc.Name, "engine", "old", "old-abandoned-1")
	abandoned.Finalizers = []string{"example.com/terminating"}
	abandoned.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	if err := c.Create(ctx, abandoned); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, abandoned); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: abandoned.Name}, got); err != nil || got.DeletionTimestamp == nil {
		t.Fatalf("the abandoned replacement must be Terminating, got err=%v pod=%+v", err, got.ObjectMeta)
	}

	reopenRun(isvc, v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "v3", StableRevision: "old"})
	deps.Group = rollout.CanaryGroup(isvc, rollout.Policies{})
	updateIR(t, c, irKey, func(ir *v1beta1.InferenceReplica) { ir.Status.UpdateRevision = isvc.Name + "-engine-v3" })
	if err := c.Create(ctx, canaryControllerRevision(ns, isvc.Name, "engine", "v3", 3)); err != nil {
		t.Fatal(err)
	}
	if err := BindRun(ctx, c, isvc, deps.Group, false); err != nil {
		t.Fatal(err)
	}
	for _, pass := range []string{"reopen", "wait", "wait-again"} {
		out := dispatch(pass)
		cs := status()
		if cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "v3" || cs.StableRevisionHash != "old" || cs.CurrentStep != 0 {
			t.Fatalf("%s: the new target must be armed at step 0 with no rejection carried, got %+v", pass, cs)
		}
		if phaseOf(isvc) != v1beta1.RolloutPhasePending {
			t.Fatalf("%s: the new target has no capacity and must stage, got %q", pass, phaseOf(isvc))
		}
		if !stableTraffic() {
			t.Fatalf("%s: traffic must rest on stable, got %+v", pass, isvc.Status.Components[v1beta1.EngineComponent].Traffic)
		}
		if got := rollbackTarget(); got != nil {
			t.Fatalf("%s: the IR's rollback target must be clear, got %q", pass, *got)
		}
		if _, ok := storedAnnotations(t, c, isvc)[constants.RolloutRollbackAnnotation]; ok {
			t.Fatalf("%s: the spent rollback request is still stored", pass)
		}
		if out.RequeueAfter == 0 && !out.Requeue {
			t.Fatalf("%s: a staging step must ask to come back, got %+v", pass, out)
		}
	}

	// The abandoned replacement is gone and the new revision's pod lands
	// next to the still-running rejected pod: the step serves.
	got.Finalizers = nil
	if err := c.Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, canaryPod(ns, isvc.Name, "engine", "v3", "v3-1")); err != nil {
		t.Fatal(err)
	}
	dispatch("landed")
	if cs := status(); cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "v3" || phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("the new target must serve its first step once its capacity lands, got %+v phase=%q", cs, phaseOf(isvc))
	}
	if err := c.Delete(ctx, canaryPod(ns, isvc.Name, "engine", "new", "new-1")); err != nil {
		t.Fatal(err)
	}
	dispatch("drained")
	if cs := status(); cs.RolledBackRevisionHash != "" || phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("draining the rejected pod must not disturb the served step, got %+v phase=%q", cs, phaseOf(isvc))
	}
}
