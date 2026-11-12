package canary

import (
	"context"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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
	if err := BindRun(context.Background(), nil, isvc, g, RunOpen{Adopted: adopting}); err != nil {
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

	if err := BindRun(context.Background(), c, isvc, rollout.CanaryGroup(isvc, rollout.Policies{}), RunOpen{}); err != nil {
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

	if err := BindRun(context.Background(), c, isvc, rollout.CanaryGroup(isvc, rollout.Policies{}), RunOpen{}); err != nil {
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

// collectedRecordRunFixture is a fresh run over one canary group whose
// engine moves from version 2 (its stable) to version 3 while its
// rolled-out record still names version 1, a revision no Instance runs; a
// second group member, when asked for, sits the run out at version 2 with
// the same stale record. Hashes are short so the record names parse back.
func collectedRecordRunFixture(members ...v1beta1.ComponentType) *v1beta1.InferenceService {
	targets := []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "e3", StableRevision: "e2"}}
	for _, c := range members {
		if c != v1beta1.EngineComponent {
			targets = append(targets, v1beta1.RolloutRunTarget{Component: c, Revision: hashOf(c, 2), StableRevision: hashOf(c, 2)})
		}
	}
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Rollout: &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
				RunID:           "whole-run",
				OpenedAt:        metav1.NewTime(time.Unix(1000, 0)),
				TargetRevisions: targets,
				Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{
					Group: v1beta1.RolloutGroup{
						Components: members,
						Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Traffic: 10}, {Traffic: 100}}},
					},
				}}},
			}},
		},
	}
	isvc.Namespace, isvc.Name = "ns", "svc"
	for _, c := range members {
		setRolledOutRecord(isvc, c, hashOf(c, 1), "")
	}
	return isvc
}

// hashOf is a member's revision hash for version n: e2, d1.
func hashOf(c v1beta1.ComponentType, n int) string {
	return string(c)[:1] + strconv.Itoa(n)
}

// setRolledOutRecord names one member's rolled-out revision fields the way
// the executor writes them, under the member's replica prefix, with
// latestReady level with latest.
func setRolledOutRecord(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, latest, previous string) {
	prefix := irprojector.RoleReplicaPrefix(isvc, c)
	cs := componentStatus(isvc, c)
	cs.LatestRolledoutRevision = coordination.PerRevisionServiceName(prefix, c, latest)
	cs.LatestReadyRevision = cs.LatestRolledoutRevision
	cs.PreviousRolledoutRevision = ""
	if previous != "" {
		cs.PreviousRolledoutRevision = coordination.PerRevisionServiceName(prefix, c, previous)
	}
	isvc.Status.Components[c] = cs
}

// rolledOutHashes reads one member's record back as hashes: ready, latest, previous.
func rolledOutHashes(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) [3]string {
	cs := isvc.Status.Components[c]
	return [3]string{
		query.RevisionFromName(cs.LatestReadyRevision).Hash(),
		query.RevisionFromName(cs.LatestRolledoutRevision).Hash(),
		query.RevisionFromName(cs.PreviousRolledoutRevision).Hash(),
	}
}

// revisionHistory is a client holding the ControllerRevisions each member's
// history still carries, by version number; every other version of the
// member is collected.
func revisionHistory(t *testing.T, isvc *v1beta1.InferenceService, retained map[v1beta1.ComponentType][]int) client.Client {
	t.Helper()
	var objs []runtime.Object
	for c, versions := range retained {
		for _, n := range versions {
			objs = append(objs, canaryControllerRevision(isvc.Namespace, isvc.Name, string(c), hashOf(c, n), int64(n)))
		}
	}
	return fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(objs...).Build()
}

// A member's record that names a revision outside the run's target and
// stable, whose ControllerRevision the sweep collected, names one no
// Instance of the member runs, and the executor writes the record next at
// the cutover or the revert. The bind writes the run's stable over it,
// keeps the stale name as previous and brings latestReady level, for every
// member of the unit it binds, each from its own run target. A record
// naming a revision the history still carries is kept: right after a
// cutover the stable, read from the replica's current revision, trails the
// pods, and the record then names the revision the Instances run. A record
// naming the stable or the target, an empty record, an adopted run, a unit
// the run did not retarget and a bind with no reader are left as they are.
// bindRunRecordCase is one row of TestBindRunRecordsTheStableOverARecordNoInstanceRuns.
type bindRunRecordCase struct {
	name     string
	members  []v1beta1.ComponentType
	retained map[v1beta1.ComponentType][]int
	noReader bool
	arrange  func(isvc *v1beta1.InferenceService)
	adopting bool
	want     map[v1beta1.ComponentType][3]string
}

// bindRunRecordWriteCases are the rows where the bind writes the record, and the
// push inside the window after a cutover, where it keeps it.
func bindRunRecordWriteCases() []bindRunRecordCase {
	engine, decoder := v1beta1.EngineComponent, v1beta1.DecoderComponent
	stableAndTarget := map[v1beta1.ComponentType][]int{engine: {2, 3}, decoder: {2}}
	everyVersion := map[v1beta1.ComponentType][]int{engine: {1, 2, 3}}
	return []bindRunRecordCase{
		{
			name:     "a record naming a collected revision outside the run's pair is written to the stable with the stale name as previous",
			members:  []v1beta1.ComponentType{engine},
			retained: stableAndTarget,
			want:     map[v1beta1.ComponentType][3]string{engine: {"e2", "e2", "e1"}},
		},
		{
			name:     "every member of the unit is written from its own run target",
			members:  []v1beta1.ComponentType{engine, decoder},
			retained: stableAndTarget,
			want: map[v1beta1.ComponentType][3]string{
				engine:  {"e2", "e2", "e1"},
				decoder: {"d2", "d2", "d1"},
			},
		},
		{
			name:     "a push one pass after a cutover, before the replica's current revision moves, keeps the record naming the revision the Instances run",
			members:  []v1beta1.ComponentType{engine},
			retained: everyVersion,
			arrange: func(isvc *v1beta1.InferenceService) {
				isvc.Status.Rollout.ActiveRun.TargetRevisions[0].StableRevision = "e1"
				setRolledOutRecord(isvc, engine, "e2", "e1")
			},
			want: map[v1beta1.ComponentType][3]string{engine: {"e2", "e2", "e1"}},
		},
	}
}

// bindRunRecordKeepCases are the rows where the bind leaves the record as it is.
func bindRunRecordKeepCases() []bindRunRecordCase {
	engine, decoder := v1beta1.EngineComponent, v1beta1.DecoderComponent
	stableAndTarget := map[v1beta1.ComponentType][]int{engine: {2, 3}, decoder: {2}}
	everyVersion := map[v1beta1.ComponentType][]int{engine: {1, 2, 3}}
	return []bindRunRecordCase{
		{
			name:     "a record naming a revision the history still carries waits for the cutover or the revert",
			members:  []v1beta1.ComponentType{engine},
			retained: everyVersion,
			want:     map[v1beta1.ComponentType][3]string{engine: {"e1", "e1", ""}},
		},
		{
			name:     "a record naming the stable is left as it is",
			members:  []v1beta1.ComponentType{engine},
			retained: stableAndTarget,
			arrange:  func(isvc *v1beta1.InferenceService) { setRolledOutRecord(isvc, engine, "e2", "e1") },
			want:     map[v1beta1.ComponentType][3]string{engine: {"e2", "e2", "e1"}},
		},
		{
			name:     "a record naming the target is left as it is",
			members:  []v1beta1.ComponentType{engine},
			retained: stableAndTarget,
			arrange:  func(isvc *v1beta1.InferenceService) { setRolledOutRecord(isvc, engine, "e3", "") },
			want:     map[v1beta1.ComponentType][3]string{engine: {"e3", "e3", ""}},
		},
		{
			name:     "an empty record stays empty",
			members:  []v1beta1.ComponentType{engine},
			retained: stableAndTarget,
			arrange:  func(isvc *v1beta1.InferenceService) { isvc.Status.Components[engine] = v1beta1.ComponentStatusSpec{} },
			want:     map[v1beta1.ComponentType][3]string{engine: {"", "", ""}},
		},
		{
			name:     "an adopted run leaves the record to the executor",
			members:  []v1beta1.ComponentType{engine},
			retained: stableAndTarget,
			adopting: true,
			want:     map[v1beta1.ComponentType][3]string{engine: {"e1", "e1", ""}},
		},
		{
			name:     "a unit the run did not retarget keeps its record for the settled write",
			members:  []v1beta1.ComponentType{engine},
			retained: stableAndTarget,
			arrange: func(isvc *v1beta1.InferenceService) {
				isvc.Status.Rollout.ActiveRun.TargetRevisions[0].Revision = "e2"
			},
			want: map[v1beta1.ComponentType][3]string{engine: {"e1", "e1", ""}},
		},
		{
			name:     "with no reader the history is unknown and the record is kept",
			members:  []v1beta1.ComponentType{engine},
			noReader: true,
			want:     map[v1beta1.ComponentType][3]string{engine: {"e1", "e1", ""}},
		},
	}
}

func TestBindRunRecordsTheStableOverARecordNoInstanceRuns(t *testing.T) {
	cases := append(bindRunRecordWriteCases(), bindRunRecordKeepCases()...)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := collectedRecordRunFixture(tc.members...)
			if tc.arrange != nil {
				tc.arrange(isvc)
			}
			var reads client.Client
			if !tc.noReader {
				reads = revisionHistory(t, isvc, tc.retained)
			}
			if err := BindRun(context.Background(), reads, isvc, rollout.CanaryGroup(isvc, rollout.Policies{}), RunOpen{Adopted: tc.adopting}); err != nil {
				t.Fatal(err)
			}
			for c, want := range tc.want {
				if got := rolledOutHashes(isvc, c); got != want {
					t.Errorf("%s record (ready, latest, previous) = %v, want %v", c, got, want)
				}
			}
		})
	}
}

// stepsAt is a step list whose steps serve the given traffic shares, each
// at a capacity of the same share, with a bare pause on every step but the
// last.
func stepsAt(traffics ...int32) []v1beta1.RolloutGroupStep {
	out := make([]v1beta1.RolloutGroupStep, 0, len(traffics))
	for i, traffic := range traffics {
		step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString(strconv.Itoa(int(traffic)) + "%"), Traffic: traffic}
		if i < len(traffics)-1 {
			step.Pause = &v1beta1.RolloutPause{}
		}
		out = append(out, step)
	}
	return out
}

// gatedSteps is stepsAt with a metric gate of the given failure limit on
// every step but the last.
func gatedSteps(failureLimit int32, traffics ...int32) []v1beta1.RolloutGroupStep {
	out := stepsAt(traffics...)
	for i := range out[:len(out)-1] {
		out[i].Analysis = analysisGate(nil)
		out[i].Analysis.FailureLimit = failureLimit
	}
	return out
}

// twoUnitPlan is a router canary group beside the engine's group over the
// given members with the given steps.
func twoUnitPlan(engineSteps []v1beta1.RolloutGroupStep, members ...v1beta1.ComponentType) []v1beta1.RolloutGroup {
	return []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: &v1beta1.GroupCanary{Steps: twoStep()}},
		{Components: members, Canary: &v1beta1.GroupCanary{Steps: engineSteps}},
	}
}

// plainRouterPlan is a router group with no canary beside the engine's
// canary group.
func plainRouterPlan(engineSteps []v1beta1.RolloutGroupStep) []v1beta1.RolloutGroup {
	return []v1beta1.RolloutGroup{
		{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, RollingUpdate: &v1beta1.GroupRollingUpdate{}},
		{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{Steps: engineSteps}},
	}
}

// runOverBothUnits is a run opened over the whole service by a router
// push, with the plan given: the router is recorded toward a newer
// revision and the engine unit at the target it already holds.
func runOverBothUnits(groups []v1beta1.RolloutGroup) *v1beta1.InferenceService {
	isvc := pinActiveRun(&v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "svc"},
		Spec:       v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: groups}},
	})
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.RouterComponent, Revision: "rnewer", StableRevision: "rold"},
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		{Component: v1beta1.DecoderComponent, Revision: "new", StableRevision: "old"},
	}
	return isvc
}

// A push to one unit closes the run and a fresh run opens over the whole
// service; the other unit is recorded at the target it already holds and
// keeps its record. When the same change edits that unit's step list, the
// bind reads the kept step into the new list the way a plan verb's clamp
// does: the index stays inside the list, a finished unit stays finished,
// and a kept step above the traffic on record sets the pre-step hold.
func TestBindRunReadsAKeptRecordIntoAnEditedStepList(t *testing.T) {
	engine, decoder := v1beta1.EngineComponent, v1beta1.DecoderComponent
	four := stepsAt(10, 30, 60, 100)
	entered := metav1.NewTime(time.Unix(1000, 0))
	second := v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 30, StepEnteredTime: &entered}
	for _, tc := range []struct {
		name     string
		ranUnder []v1beta1.RolloutGroup
		fresh    []v1beta1.RolloutGroup
		noClosed bool
		record   v1beta1.CanaryStatus
		phase    v1beta1.RolloutPhase
		wantStep int32
		wantHold bool
	}{
		{"a shorter list lands the second step on the final step and holds the raise", twoUnitPlan(four, engine), twoUnitPlan(stepsAt(10, 100), engine), false, second, v1beta1.RolloutPhasePaused, 1, true},
		{"a gate edited alone keeps the step and its split", twoUnitPlan(gatedSteps(3, 10, 30, 60, 100), engine), twoUnitPlan(gatedSteps(5, 10, 30, 60, 100), engine), false, second, v1beta1.RolloutPhasePaused, 1, false},
		{"a longer list keeps the index on a step at no more traffic", twoUnitPlan(four, engine), twoUnitPlan(stepsAt(10, 20, 30, 60, 80, 100), engine), false, second, v1beta1.RolloutPhasePaused, 1, false},
		{"a finished unit stays finished under a longer list", twoUnitPlan(four, engine), twoUnitPlan(stepsAt(10, 20, 30, 60, 80, 100), engine), false, v1beta1.CanaryStatus{CurrentStep: 4, ObservedTrafficWeight: 100, StepEnteredTime: &entered}, v1beta1.RolloutPhasePromoting, 6, false},
		{"the same list leaves the record alone", twoUnitPlan(four, engine), twoUnitPlan(four, engine), false, second, v1beta1.RolloutPhasePaused, 1, false},
		{"with no closed run in hand the record stands", twoUnitPlan(four, engine), twoUnitPlan(stepsAt(10, 100), engine), true, second, v1beta1.RolloutPhasePaused, 1, false},
		{"a prefill/decode unit beside the router is read the same way", twoUnitPlan(four, engine, decoder), twoUnitPlan(stepsAt(10, 100), engine, decoder), false, second, v1beta1.RolloutPhasePaused, 1, true},
		{"a router group with no canary beside the unit brings the same reopen", plainRouterPlan(four), plainRouterPlan(stepsAt(10, 100)), false, second, v1beta1.RolloutPhasePaused, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			closed := runOverBothUnits(tc.ranUnder).Status.Rollout.ActiveRun
			if tc.noClosed {
				closed = nil
			}
			isvc := runOverBothUnits(tc.fresh)
			g := rollout.CanaryGroupFor(isvc, rollout.Policies{}, engine)
			cs := tc.record
			cs.TargetID, cs.CanaryRevisionHash, cs.StableRevisionHash = activeCanaryTargetID(isvc, g), "new", "old"
			rollout.SetCanaryStatusFor(&isvc.Status, engine, &cs)
			setPhase(isvc, engine, tc.phase)
			if err := BindRun(context.Background(), nil, isvc, g, RunOpen{Closed: closed}); err != nil {
				t.Fatal(err)
			}
			got := rollout.CanaryStatusFor(&isvc.Status, engine)
			if got.CurrentStep != tc.wantStep || got.PreStepHold != tc.wantHold {
				t.Fatalf("step %d hold %v, want step %d hold %v", got.CurrentStep, got.PreStepHold, tc.wantStep, tc.wantHold)
			}
			if got.CanaryRevisionHash != "new" || !got.StepEnteredTime.Equal(&entered) || isvc.Status.Components[engine].RolloutPhase != tc.phase {
				t.Fatalf("the kept record must keep its target, its clock and its phase: %+v phase=%q", got, isvc.Status.Components[engine].RolloutPhase)
			}
		})
	}
}
