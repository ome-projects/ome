package canary

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/rollout"
)

// twoUnitISVC is one service with a canary group per unit: the engine's
// ladder and the router's ladder are independent.
func twoUnitISVC() *v1beta1.InferenceService {
	isvc := canaryISVC(twoStep(), nil)
	ordering := v1beta1.RolloutGroupOrderingConcurrent
	isvc.Spec.Rollout.GroupOrdering = &ordering
	isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
		Canary:     &v1beta1.GroupCanary{Steps: twoStep()},
	})
	return isvc
}

// holdEngineRejected records the engine unit's hold on the revision its
// ladder rejected, bound to the target of the run that presented it.
func holdEngineRejected(isvc *v1beta1.InferenceService, phase v1beta1.RolloutPhase) {
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		TargetID:               "engine-new",
		CanaryRevisionHash:     "new",
		StableRevisionHash:     "old",
		RolledBackRevisionHash: "new",
	})
	setPhase(isvc, v1beta1.EngineComponent, phase)
}

// A run opened by the router's retarget pins the engine unit at its stable
// revision in place of the revision it rejected. That pin changes the unit's
// TargetID but offers it no target: the engine's IR still points at the
// rejected revision, and the unit keeps its hold rather than re-arm toward it.
func TestReconcile_AnotherGroupsRunKeepsTheRejectedHold(t *testing.T) {
	cases := []struct {
		name  string
		phase v1beta1.RolloutPhase
		park  bool
		pods  map[string]int32
		state rollout.CanaryState
		// rolledBack is the revert signal the pass reports; a park reports none.
		rolledBack bool
	}{
		{name: "settled hold", phase: v1beta1.RolloutPhaseRolledBack, pods: map[string]int32{"old": 4}, state: rollout.CanaryStateRolledBack, rolledBack: true},
		{name: "revert in flight", phase: v1beta1.RolloutPhaseRollingBack, pods: map[string]int32{"new": 2, "old": 2}, state: rollout.CanaryStateRollingBack, rolledBack: true},
		{name: "parked without a stable revision", phase: v1beta1.RolloutPhaseFailed, park: true, pods: map[string]int32{"old": 4}, state: rollout.CanaryStateFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := twoUnitISVC()
			holdEngineRejected(isvc, tc.phase)
			if tc.park {
				rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).Failed = &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureStableRevisionMissing}
			}
			runWithTargets(isvc,
				v1beta1.RolloutRunTarget{Component: v1beta1.RouterComponent, Revision: "rnew", StableRevision: "rold"},
				v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "old", StableRevision: "old"},
			)
			in := baseInputs(isvc, tc.pods)
			in.Group = rollout.CanaryGroupFor(isvc, rollout.Policies{}, v1beta1.EngineComponent)
			in.CanaryRevisionHash, in.StableRevisionHash = "new", "old"
			in.TargetID = activeCanaryTargetID(isvc, in.Group)
			if in.TargetID == "" || in.TargetID == "engine-new" {
				t.Fatalf("the fresh run must hand the unit a different target id, got %q", in.TargetID)
			}

			res, err := Reconcile(context.Background(), in)
			if err != nil {
				t.Fatal(err)
			}
			cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
			if cs.RolledBackRevisionHash != "new" || cs.CanaryRevisionHash != "new" {
				t.Fatalf("the hold on the rejected revision was dropped: %+v", cs)
			}
			if got := rollout.StateOf(cs, phaseOf(isvc), 2); got != tc.state {
				t.Fatalf("state %q, want %q (phase %q, result %+v)", got, tc.state, phaseOf(isvc), res)
			}
			if res.RolledBack != tc.rolledBack {
				t.Fatalf("revert signal %v, want %v", res.RolledBack, tc.rolledBack)
			}
			if tc.park && cs.Failed == nil {
				t.Fatal("the park was lifted")
			}
		})
	}
}

// The same TargetID change made by the unit's own retarget is a new target:
// the run pins the unit at a revision other than its stable, and the hold
// re-arms toward it.
func TestReconcile_OwnRetargetRearmsTheRejectedHold(t *testing.T) {
	isvc := twoUnitISVC()
	holdEngineRejected(isvc, v1beta1.RolloutPhaseRolledBack)
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.RouterComponent, Revision: "rold", StableRevision: "rold"},
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "v3", StableRevision: "old"},
	)
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.Group = rollout.CanaryGroupFor(isvc, rollout.Policies{}, v1beta1.EngineComponent)
	in.CanaryRevisionHash, in.StableRevisionHash = "v3", "old"
	in.TargetID = activeCanaryTargetID(isvc, in.Group)

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "v3" || cs.StableRevisionHash != "old" || cs.CurrentStep != 0 {
		t.Fatalf("the unit's own retarget must arm a fresh canary at step 0, got %+v", cs)
	}
	if got := rollout.StateOf(cs, phaseOf(isvc), 2); got != rollout.CanaryStateStaging || res.RolledBack {
		t.Fatalf("state %q rolledBack=%v, want Staging with no revert signal", got, res.RolledBack)
	}
}

// The run layer opens one run for the whole service. A retarget of the
// router unit alone opens a fresh run in which the rolled-back engine unit
// is pinned at its stable revision; bound to that run and dispatched, the
// engine unit must keep its hold and its revert signal while the router unit
// arms toward its new revision.
func TestDispatch_AnotherGroupsRunKeepsTheRolledBackUnitHeld(t *testing.T) {
	ns := "default"
	n4, n1 := 4, 1
	isvc := twoUnitISVC()
	isvc.Namespace = ns
	isvc.Name = "twounit"
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n1}}
	holdEngineRejected(isvc, v1beta1.RolloutPhaseRolledBack)
	// The run that presented the rejected revision closed on the engine's rollback.
	isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome: v1beta1.RolloutRunRolledBack,
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
			{Component: v1beta1.RouterComponent, Revision: "rold", StableRevision: "rold"},
		},
	}}

	// The rejected revision stays the engine's spec target through the hold;
	// the router's spec moved to a new revision.
	engineIR := ir(ns, isvc.Name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = isvc.Name + "-engine-old"
	routerIR := ir(ns, isvc.Name, v1beta1.RouterComponent, "rnew")
	routerIR.Status.CurrentRevision = isvc.Name + "-router-rold"
	for _, r := range []*v1beta1.InferenceReplica{engineIR, routerIR} {
		r.Generation, r.Status.ObservedGeneration = 1, 1
	}
	engineKey := types.NamespacedName{Namespace: ns, Name: engineIR.Name}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc, engineIR, routerIR,
		canaryPod(ns, isvc.Name, "engine", "old", "old-0"),
		canaryPod(ns, isvc.Name, "engine", "old", "old-1"),
		canaryPod(ns, isvc.Name, "engine", "old", "old-2"),
		canaryPod(ns, isvc.Name, "engine", "old", "old-3"),
		canaryPod(ns, isvc.Name, "router", "rold", "rold-0"),
		canaryControllerRevision(ns, isvc.Name, "engine", "old", 1),
		canaryControllerRevision(ns, isvc.Name, "engine", "new", 2),
		canaryControllerRevision(ns, isvc.Name, "router", "rold", 1),
		canaryControllerRevision(ns, isvc.Name, "router", "rnew", 2),
	).Build()
	ctx := context.Background()

	out, err := rolloutrun.Reconcile(ctx, rolloutrun.Inputs{Client: c, Reader: c, ISVC: isvc, Now: time.Unix(1000, 0), FeatureEnabled: true})
	if err != nil {
		t.Fatalf("run open: %v", err)
	}
	if !out.Opened || out.Adopted {
		t.Fatalf("the router's retarget must open a fresh run, got %+v", out)
	}
	for _, target := range isvc.Status.Rollout.ActiveRun.TargetRevisions {
		if target.Component == v1beta1.EngineComponent && (target.Revision != "old" || target.StableRevision != "old") {
			t.Fatalf("the run must pin the rolled-back engine at its stable revision, got %+v", target)
		}
	}
	for _, g := range rollout.CanaryGroups(isvc, rollout.Policies{}) {
		if err := BindRun(ctx, c, isvc, g, out.Adopted); err != nil {
			t.Fatalf("bind %v: %v", g.Components, err)
		}
	}
	if rcs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.RouterComponent); rcs == nil || rcs.CanaryRevisionHash != "rnew" {
		t.Fatalf("the router unit must arm toward its new revision, got %+v", rcs)
	}

	group := rollout.CanaryGroupFor(isvc, rollout.Policies{}, v1beta1.EngineComponent)
	if _, err := Dispatch(ctx, DispatchDeps{Client: c, Reader: c, ISVC: isvc, ComponentRunnerPorts: canaryRunnerPorts(), Group: group}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs.RolledBackRevisionHash != "new" || phaseOf(isvc) != v1beta1.RolloutPhaseRolledBack {
		t.Fatalf("the engine unit must keep its hold on the rejected revision, got %+v phase=%q", cs, phaseOf(isvc))
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, engineKey, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Pacing == nil || got.Spec.Pacing.RollbackToRevision == nil || *got.Spec.Pacing.RollbackToRevision != isvc.Name+"-engine-old" {
		t.Fatalf("the engine's revert signal must keep naming its stable revision, got %+v", got.Spec.Pacing)
	}
}
