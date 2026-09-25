package canary

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// mixedUnitCanaryISVC is the one-group PD shape: the router unit and the
// engine unit share a single canary ladder, driven through the router.
func mixedUnitCanaryISVC(steps []v1beta1.RolloutGroupStep) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc"},
		Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent},
			Canary:     &v1beta1.GroupCanary{Steps: steps},
		}}}},
	}
	return pinActiveRun(isvc)
}

// The group's run state lives under its primary. Every member's partition
// must follow that state, or the members of the other unit stay at step 0's
// capacity while the primary advances and the next step's capacity gate never
// passes.
func TestEffectivePartition_MembersFollowTheGroupPrimaryStep(t *testing.T) {
	isvc := mixedUnitCanaryISVC(twoStep())
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.RouterComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new",
		StableRevisionHash: "old",
		CurrentStep:        1,
	})
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		p, ok := EffectivePartition(isvc, c, 8)
		if !ok || p == nil {
			t.Fatalf("%s: expected a canary partition", c)
		}
		if *p != 0 {
			t.Errorf("%s: partition %d at the 100%% step, want 0; the member is held at step 0's capacity", c, *p)
		}
	}
}

// A rollback recorded on the group's primary releases every member's hold.
func TestEffectivePartition_MembersFollowTheGroupRollback(t *testing.T) {
	isvc := mixedUnitCanaryISVC(twoStep())
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.RouterComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash:     "new",
		StableRevisionHash:     "old",
		RolledBackRevisionHash: "new",
	})
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		p, ok := EffectivePartition(isvc, c, 8)
		if !ok || p == nil || *p != 0 {
			t.Errorf("%s: partition during the group's rollback = %v, want 0", c, p)
		}
	}
}

// With no stable revision there is nothing to shift traffic from, so a unit
// seen for the first time does not arm a ladder: a fresh service rolls out
// and reads Stable rather than parking on its first step's gate.
func TestReconcile_NoStableRevisionDoesNotArm(t *testing.T) {
	isvc := canaryISVC(twoStep(), nil)
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "new"},
	)
	in := baseInputs(isvc, map[string]int32{"new": 4})
	in.StableRevisionHash = ""
	in.TargetID = "creation-target"

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if res.Active {
		t.Fatal("a unit with no stable revision must not arm a canary")
	}
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs != nil {
		t.Fatalf("no canary state may be written without a stable revision, got %+v", cs)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseStable {
		t.Fatalf("phase = %q, want Stable", phaseOf(isvc))
	}
}
