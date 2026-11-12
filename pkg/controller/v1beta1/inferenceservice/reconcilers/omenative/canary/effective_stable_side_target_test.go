package canary

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The revision a replica created for a Component comes up on is the name of
// the stable revision's ControllerRevision, read from the rolled-back run's
// record, the canary record, or a member's own rolled-out record; it is
// empty when the rollback stopped for want of a stable revision, when no
// canary governs the Component, and when no stable revision is known. A
// unit whose step holds a side names it too (the table below has every
// state).
func TestStableSideTarget(t *testing.T) {
	held := rolledBackPair
	policies := rollout.Policies{}

	isvc := held()
	if got := StableSideTarget(isvc, policies, v1beta1.EngineComponent); got != "svc-engine-old" {
		t.Fatalf("engine under the hold: %q, want svc-engine-old", got)
	}
	if got := StableSideTarget(isvc, policies, v1beta1.DecoderComponent); got != "svc-decoder-decold" {
		t.Fatalf("decoder under the hold: %q, want svc-decoder-decold", got)
	}
	if got := StableSideTarget(isvc, policies, v1beta1.RouterComponent); got != "" {
		t.Fatalf("a Component outside the group: %q, want none", got)
	}

	noRun := held()
	noRun.Status.Rollout = nil
	if got := StableSideTarget(noRun, policies, v1beta1.EngineComponent); got != "svc-engine-old" {
		t.Fatalf("the canary record names the primary's stable revision: %q", got)
	}
	decoder := noRun.Status.Components[v1beta1.DecoderComponent]
	decoder.LatestRolledoutRevision = coordination.PerRevisionServiceName("svc", v1beta1.DecoderComponent, "decold")
	noRun.Status.Components[v1beta1.DecoderComponent] = decoder
	if got := StableSideTarget(noRun, policies, v1beta1.DecoderComponent); got != "svc-decoder-decold" {
		t.Fatalf("a secondary's rolled-out record names its stable revision: %q", got)
	}

	unknown := held()
	unknown.Status.Rollout = nil
	if got := StableSideTarget(unknown, policies, v1beta1.DecoderComponent); got != "" {
		t.Fatalf("a member with no stable revision known: %q, want none", got)
	}

	heldUnit := held()
	cs := rollout.CanaryStatusFor(&heldUnit.Status, v1beta1.EngineComponent)
	cs.Failed = &v1beta1.CanaryFailure{Reason: v1beta1.CanaryFailureStableRevisionMissing, Time: &metav1.Time{Time: metav1.Now().Time}}
	if got := StableSideTarget(heldUnit, policies, v1beta1.EngineComponent); got != "" {
		t.Fatalf("a rollback stopped for want of a stable revision: %q, want none", got)
	}

	live := held()
	rollout.CanaryStatusFor(&live.Status, v1beta1.EngineComponent).RolledBackRevisionHash = ""
	if got := StableSideTarget(live, policies, v1beta1.EngineComponent); got != "svc-engine-old" {
		t.Fatalf("a unit whose step holds a side and records no rollback: %q, want svc-engine-old", got)
	}

	unmoved := pinActiveRun(held())
	rollout.CanaryStatusFor(&unmoved.Status, v1beta1.EngineComponent).RolledBackRevisionHash = ""
	unmoved.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		{Component: v1beta1.DecoderComponent, Revision: "decold", StableRevision: "decold"},
	}
	if got := StableSideTarget(unmoved, policies, v1beta1.DecoderComponent); got != "" {
		t.Fatalf("a member the run left at its stable revision: %q, want none (its spec is that revision)", got)
	}
	if got := StableSideTarget(unmoved, policies, v1beta1.EngineComponent); got != "svc-engine-old" {
		t.Fatalf("the member the run moves, beside one it left alone: %q, want svc-engine-old", got)
	}

	ungoverned := held()
	ungoverned.Spec.Rollout = nil
	if got := StableSideTarget(ungoverned, policies, v1beta1.EngineComponent); got != "" {
		t.Fatalf("a Component no canary governs: %q, want none", got)
	}
}

// rolledBackPair is a two-member canary group whose engine rolled back to
// "old" and holds, and whose run record names "decold" as the decoder's
// stable revision.
func rolledBackPair() *v1beta1.InferenceService {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Name = "svc"
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.EngineComponent, &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new", StableRevisionHash: "old", RolledBackRevisionHash: "new",
	})
	engine := isvc.Status.Components[v1beta1.EngineComponent]
	engine.RolloutPhase = v1beta1.RolloutPhaseRolledBack
	isvc.Status.Components[v1beta1.EngineComponent] = engine
	isvc.Status.Rollout = &v1beta1.RolloutStatus{LastRun: &v1beta1.RolloutRunRecord{
		Outcome: v1beta1.RolloutRunRolledBack,
		TargetRevisions: []v1beta1.RolloutRunTarget{
			{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
			{Component: v1beta1.DecoderComponent, Revision: "decnew", StableRevision: "decold"},
		},
	}}
	return isvc
}

// stableSideCases is one row per canary state: the fixture that drives the
// unit there and the revision a replica of the engine created in that state
// comes up on. A step short of the last, a stop and a rollback keep a stable
// side. The final step keeps none: its share is the whole fleet, and a member
// rebuilt on the stable side there would break the unit's canary path and
// rest the other member's traffic on its one held Instance for two load
// rounds. A unit with no record, a cutover past its release and a stop with
// no stable revision leave the created replica to the spec as well.
func stableSideCases() []struct {
	state   string
	fixture func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs)
	want    string
} {
	const stable = "svc-engine-old"
	return []struct {
		state   string
		fixture func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs)
		want    string
	}{
		{"Idle", fixtureIdle, ""},
		{"Staging", fixtureStaging, stable},
		{"Staging/Final", fixtureStagingFinal, ""},
		{"Serving/Manual", fixtureServingManual, stable},
		{"Serving/Timed", fixtureServingTimed, stable},
		{"Serving/Analysis", fixtureServingAnalysis, stable},
		{"PreHold", fixturePreHold, stable},
		{"Draining", fixtureDraining, ""},
		{"Draining/Held", fixtureDrainingHeld, ""},
		{"Draining/ShortInWindow", fixtureDrainingShortInWindow, ""},
		{"Draining/ShortElapsed", fixtureDrainingShort, ""},
		{"Done/Rolling", fixtureDoneRolling, ""},
		{"Done", fixtureDone, ""},
		{"RollingBack", fixtureRollingBack, stable},
		{"RolledBack", fixtureRolledBack, stable},
		{"Failed", fixtureFailed, stable},
		{"Failed/StableMissing", fixtureFailedStableMissing, ""},
	}
}

// A replica of a canary member deleted and projected again is created on
// the stable side its unit keeps: under a step short of the last, a stop or
// a rollback it comes up on the stable revision, so the step's partition has
// a revision to hold its Instances on and the step stages its share from
// there; at the final step, with no record, after the cutover's release, and
// under a stop with no stable revision it comes up on the spec's revision.
func TestStableSideTarget_AReplicaCreatedUnderEachUnitState(t *testing.T) {
	for _, tc := range stableSideCases() {
		t.Run(tc.state, func(t *testing.T) {
			isvc, _ := tc.fixture(t)
			got := StableSideTarget(isvc, rollout.Policies{}, v1beta1.EngineComponent)
			if got != tc.want {
				t.Fatalf("a replica created at %s comes up on %q, want %q", tc.state, got, tc.want)
			}
		})
	}
}
