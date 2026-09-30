package rollout

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// A canary group owns every one of its members, whether the spec declares it
// inline or by policyRef, or the pinned plan alone still carries it; a
// blueGreen group owns none.
func TestCanaryOwnedComponentsCoverEveryMember(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	if got := CanaryOwnedComponents(isvc); len(got) != 0 {
		t.Fatalf("no rollout block owns nothing, got %v", got)
	}

	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{
		{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
			Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Traffic: 100}}},
		},
		{
			Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
			BlueGreen:  &v1beta1.GroupBlueGreen{},
		},
	}}
	got := CanaryOwnedComponents(isvc)
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		if _, ok := got[c]; !ok {
			t.Fatalf("%s is a member of the declared canary group, got %v", c, got)
		}
	}
	if _, ok := got[v1beta1.RouterComponent]; ok {
		t.Fatalf("a blueGreen member is not canary-owned, got %v", got)
	}

	// A policyRef canary group has no inline ladder yet still governs its
	// members.
	isvc.Spec.Rollout.Groups[0] = v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		PolicyRef:  &v1beta1.RolloutPolicyRef{Name: "p", Progression: v1beta1.RolloutProgressionCanary},
	}
	if got := CanaryOwnedComponents(isvc); len(got) != 2 {
		t.Fatalf("a policyRef canary group owns its members, got %v", got)
	}

	// The spec no longer carries the group; the open run still pins it.
	pinned := v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
		Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Traffic: 100}}},
	}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
		Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Source: v1beta1.RolloutPlanSourceInline, Group: pinned}}},
	}}
	got = CanaryOwnedComponents(isvc)
	if _, ok := got[v1beta1.RouterComponent]; !ok || len(got) != 2 {
		t.Fatalf("a pinned canary group owns its members for the life of the run, got %v", got)
	}
}
