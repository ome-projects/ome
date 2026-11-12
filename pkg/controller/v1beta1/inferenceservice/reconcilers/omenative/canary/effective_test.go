package canary

import (
	"context"
	"fmt"
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
// passes. At the 100% step each member holds its last stable instance, the
// held floor, until the sentinel releases it.
func TestEffectivePartition_MembersFollowTheGroupPrimaryStep(t *testing.T) {
	isvc := mixedUnitCanaryISVC(twoStep())
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.RouterComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new",
		StableRevisionHash: "old",
		CurrentStep:        1,
	})
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		p, ok := EffectivePartition(isvc, rollout.Policies{}, c, 8)
		if !ok || p == nil {
			t.Fatalf("%s: expected a canary partition", c)
		}
		if *p != 1 {
			t.Errorf("%s: partition %d at the 100%% step, want 1; the member is held at step 0's capacity", c, *p)
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
		p, ok := EffectivePartition(isvc, rollout.Policies{}, c, 8)
		if !ok || p == nil || *p != 0 {
			t.Errorf("%s: partition during the group's rollback = %v, want 0", c, p)
		}
	}
}

// A push the operator took back while an Instance still ran the canary
// revision: the run records the unit at its stable revision, the record is
// gone and the unit reads RollingBack while that Instance is still off the
// stable revision. The unit holds nothing, so the Instance rolls back; step
// 0's partition would keep it where it stands, with the run open on it for
// good. A unit that sits such a run out whole reads Stable and keeps step
// 0's partition, as it does with no run open and under a run that offers
// it a target it has no record for yet.
func TestEffectivePartition_AUnitTheRunRecordsAtItsStableHoldsNothing(t *testing.T) {
	show := func(p *int32, ok bool) string {
		if !ok || p == nil {
			return fmt.Sprintf("(%v, ok=%t)", p, ok)
		}
		return fmt.Sprint(*p)
	}
	isvc := canaryISVC(twoStep(), nil)
	runWithTargets(isvc, v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "old", StableRevision: "old"})
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRollingBack)
	if p, ok := EffectivePartition(isvc, rollout.Policies{}, v1beta1.EngineComponent, 4); !ok || p == nil || *p != 0 {
		t.Fatalf("partition under a run that records the unit at its stable revision, an Instance still off it = %s, want 0", show(p, ok))
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseStable)
	if p, ok := EffectivePartition(isvc, rollout.Policies{}, v1beta1.EngineComponent, 4); !ok || p == nil || *p != 2 {
		t.Fatalf("partition of a unit that sits the run out whole = %s, want step 0's 2", show(p, ok))
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseRollingBack)
	isvc.Status.Rollout.ActiveRun = nil
	if p, ok := EffectivePartition(isvc, rollout.Policies{}, v1beta1.EngineComponent, 4); !ok || p == nil || *p != 2 {
		t.Fatalf("partition with no run open = %s, want step 0's 2", show(p, ok))
	}
	runWithTargets(isvc, v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: ""})
	if p, ok := EffectivePartition(isvc, rollout.Policies{}, v1beta1.EngineComponent, 4); !ok || p == nil || *p != 2 {
		t.Fatalf("partition under a run that offers a target with no record yet = %s, want step 0's 2", show(p, ok))
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

// A negative step is an external write. The partition is read at the step
// the record's own traffic places the ladder at: a drain whose 100% shift
// is recorded keeps the final step's held floor, a unit that already read
// Stable keeps the sentinel's release, and a split keeps its own step.
func TestEffectivePartition_NegativeStepReadsTheRecord(t *testing.T) {
	cases := []struct {
		name   string
		phase  v1beta1.RolloutPhase
		weight int32
		want   int32
	}{
		{"drain at 100%", v1beta1.RolloutPhasePromoting, 100, 1},
		{"held final gate at 100%", v1beta1.RolloutPhasePaused, 100, 1},
		{"completed cutover", v1beta1.RolloutPhaseStable, 100, 0},
		{"first split", v1beta1.RolloutPhasePaused, 50, 4},
		{"staging before any split", v1beta1.RolloutPhasePending, 0, 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isvc := mixedUnitCanaryISVC(twoStep())
			rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.RouterComponent, &v1beta1.CanaryStatus{
				CanaryRevisionHash:    "new",
				StableRevisionHash:    "old",
				CurrentStep:           -1,
				ObservedTrafficWeight: c.weight,
			})
			setPhase(isvc, v1beta1.RouterComponent, c.phase)
			for _, member := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
				p, ok := EffectivePartition(isvc, rollout.Policies{}, member, 8)
				if !ok || p == nil {
					t.Fatalf("%s: expected a canary partition", member)
				}
				if *p != c.want {
					t.Errorf("%s: partition %d, want %d", member, *p, c.want)
				}
			}
		})
	}
}
