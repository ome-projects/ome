package canary

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

func canaryRunFixture() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Rollout: &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
				RunID:    "whole-run",
				OpenedAt: metav1.NewTime(time.Unix(1000, 0)),
				TargetRevisions: []v1beta1.RolloutRunTarget{
					{Component: v1beta1.RouterComponent, Revision: "router-a", StableRevision: "router-a"},
					{Component: v1beta1.EngineComponent, Revision: "engine-b", StableRevision: "engine-a"},
					{Component: v1beta1.DecoderComponent, Revision: "decoder-a", StableRevision: "decoder-a"},
				},
				Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{
					Group: v1beta1.RolloutGroup{
						Components: []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent},
						Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Traffic: 25}}},
					},
				}}},
			}},
		},
	}
}

func TestActiveCanaryTargetIDIsGroupScoped(t *testing.T) {
	isvc := canaryRunFixture()
	want := activeCanaryTargetID(isvc, rollout.CanaryGroup(isvc))
	if want == "" {
		t.Fatal("target ID must be populated")
	}

	isvc.Status.Rollout.ActiveRun.TargetRevisions[2].Revision = "decoder-b"
	if got := activeCanaryTargetID(isvc, rollout.CanaryGroup(isvc)); got != want {
		t.Fatalf("unrelated target changed canary identity: %q -> %q", want, got)
	}

	isvc.Status.Rollout.ActiveRun.TargetRevisions[1].Revision = "engine-c"
	if got := activeCanaryTargetID(isvc, rollout.CanaryGroup(isvc)); got == want {
		t.Fatalf("canary-group target change did not change identity: %q", got)
	}
}

func TestBindRunAtomicallyResetsFreshRun(t *testing.T) {
	isvc := canaryRunFixture()
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		TargetID:               "old-target",
		CanaryRevisionHash:     "router-old",
		StableRevisionHash:     "router-original",
		CurrentStep:            2,
		RolledBackRevisionHash: "router-rejected",
	}

	BindRun(isvc, rollout.CanaryGroup(isvc), false)
	cs := isvc.Status.Canary
	if cs.TargetID == "" || cs.TargetID == "old-target" {
		t.Fatalf("fresh run target ID was not bound: %+v", cs)
	}
	if cs.CanaryRevisionHash != "router-a" || cs.StableRevisionHash != "router-a" {
		t.Fatalf("primary identities = canary %q stable %q, want router-a/router-a", cs.CanaryRevisionHash, cs.StableRevisionHash)
	}
	if cs.CurrentStep != 0 || cs.RolledBackRevisionHash != "" {
		t.Fatalf("fresh run state was not reset: %+v", cs)
	}
	if cs.StepEnteredTime == nil || !cs.StepEnteredTime.Time.Equal(isvc.Status.Rollout.ActiveRun.OpenedAt.Time) {
		t.Fatalf("step time = %v, want run open time %v", cs.StepEnteredTime, isvc.Status.Rollout.ActiveRun.OpenedAt)
	}
}

func TestBindRunAdoptsWithoutRestart(t *testing.T) {
	isvc := canaryRunFixture()
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "router-old",
		StableRevisionHash: "router-stable",
		CurrentStep:        1,
	}

	BindRun(isvc, rollout.CanaryGroup(isvc), true)
	cs := isvc.Status.Canary
	if cs.TargetID == "" {
		t.Fatal("adopted state must be bound to the canary target set")
	}
	if cs.CurrentStep != 1 || cs.CanaryRevisionHash != "router-old" || cs.StableRevisionHash != "router-stable" {
		t.Fatalf("adoption restarted canary state: %+v", cs)
	}
}
