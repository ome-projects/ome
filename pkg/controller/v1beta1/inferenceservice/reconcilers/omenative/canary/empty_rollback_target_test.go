package canary

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A rollback returns every Instance of the unit to its stable revision. A
// stable revision with no live Instance while the ladder holds a floor for
// it is not one the unit stood on in this ladder, so a rollback the
// controller decides on its own parks instead of rebuilding the fleet onto
// it; a rollback the operator requests proceeds, with the same warning.

// breachingStep is an analysis-gated step whose first failing sample rolls
// the ladder back.
func breachingStep() v1beta1.RolloutGroupStep {
	return v1beta1.RolloutGroupStep{
		Capacity: intstr.FromString("50%"), Traffic: 50,
		Analysis: &v1beta1.RolloutAnalysis{
			Interval:     metav1.Duration{Duration: time.Minute},
			FailureLimit: 1,
			Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
		},
	}
}

// servingLadder seeds a single-engine ladder serving its first step toward
// "new" from stable "old", with the unit's pods per revision as given, and
// returns the inputs of a pass whose analysis sample fails.
func servingLadder(t *testing.T, pods map[string]int32, desired int32) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	t.Helper()
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{breachingStep(), {Capacity: intstr.FromString("100%"), Traffic: 100}}, nil)
	t0 := time.Unix(100000, 0)
	isvc.Status.Canary = &v1beta1.CanaryStatus{
		CanaryRevisionHash: "new",
		StableRevisionHash: "old",
		StepEnteredTime:    &metav1.Time{Time: t0},
	}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseCanarying)
	rec := record.NewFakeRecorder(8)
	in := baseInputs(isvc, pods)
	in.Now = t0
	in.Recorder = rec
	in.DesiredReplicas = desired
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{v1beta1.EngineComponent: pods}
	in.Sampler = &countingSample{outcome: analysis.Fail, at: t0}
	return isvc, in, rec
}

func emptyTargetEvents(rec *record.FakeRecorder) []string {
	var out []string
	for _, e := range eventsFrom(rec) {
		if strings.Contains(e, EventReasonCanaryStableRevisionEmpty) {
			out = append(out, e)
		}
	}
	return out
}

// The gate fails on a unit whose stable revision runs no Instance: the ladder
// parks where it stands, no revert is signaled, and the warning names the
// revision and the Component.
func TestReconcile_AutomaticRollbackToAnEmptyStableParks(t *testing.T) {
	isvc, in, rec := servingLadder(t, map[string]int32{"new": 4}, 4)

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureStableRevisionMissing {
		t.Fatalf("the ladder must park Failed with the stable revision unavailable, got %+v", cs)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseFailed {
		t.Fatalf("phase %q, want Failed", phaseOf(isvc))
	}
	if cs.RolledBackRevisionHash != "" || res.RolledBack {
		t.Fatalf("no revert may be signaled toward an empty stable revision, got hash %q rolledBack=%v", cs.RolledBackRevisionHash, res.RolledBack)
	}
	if res.RequeueAfter != failedRequeue {
		t.Fatalf("a park keeps the parked cadence %v, got %v", failedRequeue, res.RequeueAfter)
	}
	events := emptyTargetEvents(rec)
	if len(events) != 1 || !strings.HasPrefix(events[0], "Warning") || !strings.Contains(events[0], "old") || !strings.Contains(events[0], string(v1beta1.EngineComponent)) {
		t.Fatalf("want one Warning naming the stable revision and the Component, got %v", events)
	}
	if cs.CurrentStep != 0 {
		t.Fatalf("the park keeps the step, got %d", cs.CurrentStep)
	}
	// The split is left on serving capacity: the empty stable revision
	// carries no weight, the canary revision alone is named.
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, latest: true}})
}

// The park is a terminal hold: later passes neither re-sample nor revert.
func TestReconcile_EmptyStableParkHoldsAcrossPasses(t *testing.T) {
	isvc, in, rec := servingLadder(t, map[string]int32{"new": 4}, 4)
	if _, err := Reconcile(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	eventsFrom(rec)
	in.Now = in.Now.Add(10 * time.Minute)
	sampler := &countingSample{outcome: analysis.Pass, at: in.Now}
	in.Sampler = sampler
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if sampler.calls != 0 || res.RolledBack || isvc.Status.Canary.Failed == nil {
		t.Fatalf("a parked ladder must hold: calls=%d rolledBack=%v failed=%+v", sampler.calls, res.RolledBack, isvc.Status.Canary.Failed)
	}
	if got := emptyTargetEvents(rec); len(got) != 0 {
		t.Fatalf("the warning is recorded once, at the park, got %v", got)
	}
}

// A stable revision with a live Instance is a target the unit can return
// to: the automatic rollback proceeds as before.
func TestReconcile_AutomaticRollbackToAServedStableProceeds(t *testing.T) {
	isvc, in, rec := servingLadder(t, map[string]int32{"new": 2, "old": 2}, 4)

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if cs.RolledBackRevisionHash != "new" || !res.RolledBack || cs.Failed != nil {
		t.Fatalf("the revert must start, got %+v rolledBack=%v", cs, res.RolledBack)
	}
	if phaseOf(isvc) != v1beta1.RolloutPhaseRollingBack {
		t.Fatalf("phase %q, want RollingBack", phaseOf(isvc))
	}
	if got := emptyTargetEvents(rec); len(got) != 0 {
		t.Fatalf("a served stable revision warns of nothing, got %v", got)
	}
}

// A single-Instance member stages in place: its ladder holds no floor, so
// a stable revision with no Instance is the ladder's own doing and the
// rollback proceeds.
func TestReconcile_AutomaticRollbackOfASingleInstanceUnitProceeds(t *testing.T) {
	isvc, in, rec := servingLadder(t, map[string]int32{"new": 1}, 1)

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if isvc.Status.Canary.RolledBackRevisionHash != "new" || !res.RolledBack || isvc.Status.Canary.Failed != nil {
		t.Fatalf("a single-Instance unit rolls back, got %+v rolledBack=%v", isvc.Status.Canary, res.RolledBack)
	}
	if got := emptyTargetEvents(rec); len(got) != 0 {
		t.Fatalf("no warning for a unit that holds no floor, got %v", got)
	}
}

// The unit reverts as a whole: a secondary member whose stable revision runs
// no Instance parks the unit too, and the warning names that member.
func TestReconcile_AutomaticRollbackParksOnASecondarysEmptyStable(t *testing.T) {
	isvc, in, rec := servingLadder(t, map[string]int32{"new": 2, "old": 2}, 4)
	in.Secondaries = map[v1beta1.ComponentType]MemberRevisions{
		v1beta1.DecoderComponent: {CanaryRevisionHash: "dnew", StableRevisionHash: "dold", ReadyCanaryCapacity: 4, DesiredReplicas: 4},
	}
	in.GroupTotalPerRevisionPods[v1beta1.DecoderComponent] = map[string]int32{"dnew": 4}
	in.GroupReadyPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{
		v1beta1.EngineComponent:  {"new": 2, "old": 2},
		v1beta1.DecoderComponent: {"dnew": 4},
	}

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if isvc.Status.Canary.Failed == nil || res.RolledBack || isvc.Status.Canary.RolledBackRevisionHash != "" {
		t.Fatalf("the unit must park on the decoder's empty stable revision, got %+v rolledBack=%v", isvc.Status.Canary, res.RolledBack)
	}
	events := emptyTargetEvents(rec)
	if len(events) != 1 || !strings.Contains(events[0], "dold") || !strings.Contains(events[0], string(v1beta1.DecoderComponent)) {
		t.Fatalf("want one Warning naming the decoder and its stable revision, got %v", events)
	}
	// A member whose stable serves keeps the step's split; the member whose
	// stable is empty names its target alone.
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 50, latest: true}, "old": {percent: 50}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"dnew": {percent: 100, latest: true}})
}

// The operator's rollback is the operator's decision: it proceeds toward
// the empty stable revision, and the same warning says what it returns to.
func TestReconcile_OperatorRollbackToAnEmptyStableProceedsWithTheWarning(t *testing.T) {
	isvc, in, rec := servingLadder(t, map[string]int32{"new": 4}, 4)
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	in.Sampler = nil

	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	cs := isvc.Status.Canary
	if cs.RolledBackRevisionHash != "new" || !res.RolledBack || cs.Failed != nil {
		t.Fatalf("the operator's rollback must proceed, got %+v rolledBack=%v", cs, res.RolledBack)
	}
	events := emptyTargetEvents(rec)
	if len(events) != 1 || !strings.HasPrefix(events[0], "Warning") || !strings.Contains(events[0], "old") || !strings.Contains(events[0], string(v1beta1.EngineComponent)) {
		t.Fatalf("want one Warning naming the stable revision and the Component, got %v", events)
	}
}

// The operator's rollback toward an empty stable revision proceeds the same
// way from every live state the request can land in: a split serving under
// a manual, timed or metric gate, the final step waiting for its capacity,
// and the pre-step hold. The request runs ahead of the step's gate, so the
// gate kind plays no part. Traffic moves only onto stable capacity that
// serves: with none, the split the state held stands until the revert
// brings a stable pod back, so a share keeps naming the stable revision
// although no pod serves it.
func TestReconcile_OperatorRollbackToAnEmptyStableFromEveryLiveState(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fixture func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs)
	}{
		{"serving under a manual pause", fixtureServingManual},
		{"serving under a timed pause", fixtureServingTimed},
		{"serving under analysis", fixtureServingAnalysis},
		{"the final step staging", fixtureStagingFinal},
		{"the pre-step hold", fixturePreHold},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc, in := tc.fixture(t)
			rec := record.NewFakeRecorder(8)
			in.Recorder = rec
			evRollbackStableEmpty(isvc, &in)

			res := mustReconcile(t, isvc, in)
			cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
			steps := len(isvc.Spec.Rollout.Groups[0].Canary.Steps)
			if got := rollout.StateOf(cs, phaseOf(isvc), steps); got != rollout.CanaryStateRollingBack {
				t.Fatalf("state %q, want %q (status %+v)", got, rollout.CanaryStateRollingBack, cs)
			}
			if cs.RolledBackRevisionHash != in.CanaryRevisionHash || !res.RolledBack || cs.Failed != nil {
				t.Fatalf("the operator's rollback must proceed, got %+v rolledBack=%v", cs, res.RolledBack)
			}
			events := emptyTargetEvents(rec)
			if len(events) != 1 || !strings.HasPrefix(events[0], "Warning") || !strings.Contains(events[0], in.StableRevisionHash) || !strings.Contains(events[0], string(v1beta1.EngineComponent)) {
				t.Fatalf("want one Warning naming the stable revision and the Component, got %v", events)
			}
			stableShare := int32(0)
			for _, target := range isvc.Status.Components[v1beta1.EngineComponent].Traffic {
				if query.RevisionFromName(target.RevisionName).Hash() == in.StableRevisionHash {
					stableShare += target.Percent
				}
			}
			if stableShare <= 0 || in.PerRevisionPods[in.StableRevisionHash] != 0 {
				t.Fatalf("the split held before the request stands until a stable pod serves again, got %+v with pods %v",
					isvc.Status.Components[v1beta1.EngineComponent].Traffic, in.PerRevisionPods)
			}
		})
	}
}

// Through the dispatcher: the gate's failing sample parks the unit and the
// InferenceReplica is never pointed at the empty stable revision, although
// that revision's ControllerRevision is retained.
func TestDispatch_AutomaticRollbackToAnEmptyStableLeavesTheReplicaUnsignaled(t *testing.T) {
	ns, name := "default", "empty-stable"
	n4 := 4
	isvc := canaryISVC([]v1beta1.RolloutGroupStep{breachingStep(), {Capacity: intstr.FromString("100%"), Traffic: 100}}, nil)
	isvc.Namespace, isvc.Name = ns, name
	isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: &n4}}
	isvc.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "new", StableRevisionHash: "old", StepEnteredTime: &metav1.Time{Time: time.Now()}}
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseCanarying)
	pinActiveRun(isvc)
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	engineIR := ir(ns, name, v1beta1.EngineComponent, "new")
	engineIR.Status.CurrentRevision = name + "-engine-new"
	c := fake.NewClientBuilder().WithScheme(canaryScheme(t)).WithRuntimeObjects(
		isvc, engineIR,
		canaryPod(ns, name, "engine", "new", "new-0"), canaryPod(ns, name, "engine", "new", "new-1"),
		canaryPod(ns, name, "engine", "new", "new-2"), canaryPod(ns, name, "engine", "new", "new-3"),
		canaryControllerRevision(ns, name, "engine", "old", 1),
		canaryControllerRevision(ns, name, "engine", "new", 2),
	).Build()
	events := make(chan event.GenericEvent, 8)
	sampler := NewSampler(func(context.Context, SampleRequest) analysis.Result {
		return analysis.Result{Outcome: analysis.Fail, Metrics: []analysis.MetricResult{{Name: "err"}}}
	}, events, 1, time.Hour)
	rec := record.NewFakeRecorder(16)
	deps := DispatchDeps{Client: c, Reader: c, Recorder: rec, ISVC: isvc, Sampler: sampler,
		ComponentRunnerPorts: canaryRunnerPorts(), Group: rollout.CanaryGroup(isvc, rollout.Policies{}), ParkedRequeue: failedRequeue}
	ctx := context.Background()

	// The first pass kicks the query and holds; the sample lands for the next.
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatal(err)
	}
	waitEvent(t, events)
	if _, err := Dispatch(ctx, deps); err != nil {
		t.Fatal(err)
	}

	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if cs == nil || cs.Failed == nil || cs.RolledBackRevisionHash != "" {
		t.Fatalf("the failing gate must park the unit without a revert, got %+v", cs)
	}
	if len(emptyTargetEvents(rec)) != 1 {
		t.Fatalf("want the empty-stable warning once, got %v", eventsFrom(rec))
	}
	got := &v1beta1.InferenceReplica{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name + "-engine"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Pacing != nil && got.Spec.Pacing.RollbackToRevision != nil {
		t.Fatalf("the replica must not be pointed at the empty stable revision, got %+v", got.Spec.Pacing)
	}
}
