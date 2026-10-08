package canary

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
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

// bindRun drives the run-open seam with no client: a spent request is then
// removed in-memory only, which cannot fail.
func bindRun(t *testing.T, isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup, adopting bool) {
	t.Helper()
	if err := BindRun(context.Background(), nil, isvc, g, adopting); err != nil {
		t.Fatal(err)
	}
}

func TestActiveCanaryTargetIDIsGroupScoped(t *testing.T) {
	isvc := canaryRunFixture()
	want := activeCanaryTargetID(isvc, rollout.CanaryGroup(isvc, rollout.Policies{}))
	if want == "" {
		t.Fatal("target ID must be populated")
	}

	isvc.Status.Rollout.ActiveRun.TargetRevisions[2].Revision = "decoder-b"
	if got := activeCanaryTargetID(isvc, rollout.CanaryGroup(isvc, rollout.Policies{})); got != want {
		t.Fatalf("unrelated target changed canary identity: %q -> %q", want, got)
	}

	isvc.Status.Rollout.ActiveRun.TargetRevisions[1].Revision = "engine-c"
	if got := activeCanaryTargetID(isvc, rollout.CanaryGroup(isvc, rollout.Policies{})); got == want {
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

	bindRun(t, isvc, rollout.CanaryGroup(isvc, rollout.Policies{}), false)
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

	bindRun(t, isvc, rollout.CanaryGroup(isvc, rollout.Policies{}), true)
	cs := isvc.Status.Canary
	if cs.TargetID == "" {
		t.Fatal("adopted state must be bound to the canary target set")
	}
	if cs.CurrentStep != 1 || cs.CanaryRevisionHash != "router-old" || cs.StableRevisionHash != "router-stable" {
		t.Fatalf("adoption restarted canary state: %+v", cs)
	}
}

// twoUnitRunFixture is one run over two canary groups, one per unit, in which
// only the router retargeted; the engine unit's targets equal its stable.
func twoUnitRunFixture() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Rollout: &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
				RunID:    "whole-run",
				OpenedAt: metav1.NewTime(time.Unix(1000, 0)),
				TargetRevisions: []v1beta1.RolloutRunTarget{
					{Component: v1beta1.RouterComponent, Revision: "router-b", StableRevision: "router-a"},
					{Component: v1beta1.EngineComponent, Revision: "engine-a", StableRevision: "engine-a"},
					{Component: v1beta1.DecoderComponent, Revision: "decoder-a", StableRevision: "decoder-a"},
				},
				Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{
					{Group: v1beta1.RolloutGroup{
						Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
						Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Traffic: 50}, {Traffic: 100}}},
					}},
					{Group: v1beta1.RolloutGroup{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
						Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Traffic: 25}, {Traffic: 100}}},
					}},
				}},
			}},
		},
	}
}

// A unit whose targets all equal their stable revision has no work in the
// run. Binding must leave it without state: state is what arms the step
// machine, which would then walk the unit's ladder over a rollout that never
// happened.
func TestBindRunLeavesAnIdleUnitWithoutState(t *testing.T) {
	isvc := twoUnitRunFixture()
	for _, g := range rollout.CanaryGroups(isvc, rollout.Policies{}) {
		bindRun(t, isvc, g, false)
	}
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent); cs != nil {
		t.Fatalf("the idle engine unit was handed run state: %+v", cs)
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.RouterComponent)
	if cs == nil || cs.CanaryRevisionHash != "router-b" || cs.StableRevisionHash != "router-a" || cs.TargetID == "" {
		t.Fatalf("the retargeted router unit must be bound to the run, got %+v", cs)
	}
}

// A unit whose run records no stable revision has nothing to shift traffic
// from: its first rollout is not a canary. Binding must not create state for
// it; the step machine arms later only if a stable revision becomes known.
func TestBindRunSkipsAUnitWithoutAStableRevision(t *testing.T) {
	isvc := twoUnitRunFixture()
	isvc.Status.Rollout.ActiveRun.TargetRevisions[0].StableRevision = ""
	bindRun(t, isvc, rollout.CanaryGroups(isvc, rollout.Policies{})[0], false)
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.RouterComponent); cs != nil {
		t.Fatalf("a unit with no stable revision was handed run state: %+v", cs)
	}
}

// A rollback request is recorded against one target. A run opened toward a
// new target re-arms the rolled-back unit and removes the request with the
// rejected hash, stored and in memory, so it cannot roll the new canary back
// on its first pass.
func TestBindRunRemovesTheRequestOfTheRolledBackTarget(t *testing.T) {
	isvc := canaryRunFixture()
	isvc.Namespace, isvc.Name = "ns", "svc"
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		TargetID:               "old-target",
		CanaryRevisionHash:     "router-rejected",
		StableRevisionHash:     "router-a",
		RolledBackRevisionHash: "router-rejected",
	}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	if err := BindRun(context.Background(), c, isvc, rollout.CanaryGroup(isvc, rollout.Policies{}), false); err != nil {
		t.Fatal(err)
	}
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.RouterComponent)
	if cs == nil || cs.RolledBackRevisionHash != "" || cs.CanaryRevisionHash != "router-a" || cs.CurrentStep != 0 {
		t.Fatalf("the unit was not re-armed toward the run's target: %+v", cs)
	}
	if _, ok := isvc.Annotations[constants.RolloutRollbackAnnotation]; ok {
		t.Fatal("the spent rollback request is still visible to this pass")
	}
	if _, ok := storedAnnotations(t, c, isvc)[constants.RolloutRollbackAnnotation]; ok {
		t.Fatal("the spent rollback request is still stored on the object")
	}
}

// A request found on a unit that holds no rejection has not been applied to
// any target: run open leaves it in place, and the executor applies it to
// the target the run presents.
func TestBindRunKeepsAnUnappliedRollbackRequest(t *testing.T) {
	isvc := canaryRunFixture()
	isvc.Namespace, isvc.Name = "ns", "svc"
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithObjects(isvc).Build()

	if err := BindRun(context.Background(), c, isvc, rollout.CanaryGroup(isvc, rollout.Policies{}), false); err != nil {
		t.Fatal(err)
	}
	if cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.RouterComponent); cs == nil || cs.CanaryRevisionHash != "router-a" {
		t.Fatalf("the unit was not armed toward the run's target: %+v", cs)
	}
	if isvc.Annotations[constants.RolloutRollbackAnnotation] != "true" ||
		storedAnnotations(t, c, isvc)[constants.RolloutRollbackAnnotation] != "true" {
		t.Fatal("an unapplied request must ride into the new run")
	}
}
