package canary

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The canary state × event grid, one row per authored cell. A row's id is
// canary/<State>/<event>; its fixture drives Reconcile into the state, its
// event is one input change, and its want is the decided outcome. A row
// that disagrees with the code is a finding about one or the other, never
// something to paper over in the fixture.
type cell struct {
	id      string
	fixture func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs)
	event   func(isvc *v1beta1.InferenceService, in *ReconcileInputs)
	want    cellWant
}

type cellWant struct {
	state    rollout.CanaryState
	phase    v1beta1.RolloutPhase // empty: not asserted
	step     int32
	active   bool
	rolledBk bool
	complete bool
	consume  []string // annotation keys the pass hands back for removal
}

const cellReadyTimeout = 15 * time.Minute

func mustReconcile(t *testing.T, isvc *v1beta1.InferenceService, in ReconcileInputs) *Result {
	t.Helper()
	res, err := Reconcile(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// twoStepTimed is a timed soak then full traffic; twoStep() is the manual
// ladder.
func twoStepTimed(d time.Duration) []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: d}}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// fixtureStagingApplied is an intermediate step staging short of its
// capacity after a forced advance from the step before, with the force's
// value recorded in PromotedThrough and the annotation already taken.
func fixtureStagingApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(threeStep(), nil))
	in := baseInputs(isvc, map[string]int32{"new": 0, "old": 4})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in) // arms and parks on the capacity gate
	isvc.Annotations = map[string]string{constants.RolloutPromoteForceAnnotation: "new"}
	mustReconcile(t, isvc, in) // the force advances to step 1, still short
	return isvc, in
}

// fixtureStagingFinalApplied is the final step staging short of its capacity
// after a forced advance from the first step: the force's value is recorded
// in PromotedThrough and the annotation already taken.
func fixtureStagingFinalApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureStaging(t)
	isvc.Annotations = map[string]string{constants.RolloutPromoteForceAnnotation: "new"}
	mustReconcile(t, isvc, in) // the force advances to the final step, still short
	return isvc, in
}

// threeStepTimedSecond is threeStep with a timed soak on its second step.
func threeStepTimedSecond(d time.Duration) []v1beta1.RolloutGroupStep {
	steps := threeStep()
	steps[1].Pause = &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: d}}
	return steps
}

// servingApplied walks a three-step ladder to its second step serving under
// a promote whose value the record still carries: the promote advanced step
// 0, a lagging cache showed it again, and the pass that met the second
// step's capacity took the copy and kept PromotedThrough.
func servingApplied(t *testing.T, steps []v1beta1.RolloutGroupStep, prep func(*ReconcileInputs)) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(steps, nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	if prep != nil {
		prep(&in)
	}
	mustReconcile(t, isvc, in) // the first split serves under its manual hold
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the promote advances to step 1
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the copy is taken again; the second split serves
	return isvc, in
}

func fixtureServingManualApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return servingApplied(t, threeStep(), nil)
}

func fixtureServingTimedApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return servingApplied(t, threeStepTimedSecond(time.Hour), nil)
}

// analysisGate is a metric gate sampling every minute with a failure limit
// of three, under the given inconclusive policy.
func analysisGate(policy *v1beta1.OnInconclusive) *v1beta1.RolloutAnalysis {
	return &v1beta1.RolloutAnalysis{
		Interval:       metav1.Duration{Duration: time.Minute},
		FailureLimit:   3,
		OnInconclusive: policy,
		Metrics:        []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
	}
}

// twoStepAnalysis is an analysis-gated first step baking for an hour, then
// full traffic.
func twoStepAnalysis(policy *v1beta1.OnInconclusive) []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: time.Hour}}, Analysis: analysisGate(policy)},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// missingSample is a sampler with no fresh sample: every read kicks a query
// that has not landed.
type missingSample struct{}

func (missingSample) Get(SampleRequest, time.Time) (analysis.Result, time.Time, bool) {
	return analysis.Result{}, time.Time{}, false
}

// withSampler wires a sampler that answers every read with outcome, dated
// at the pass's own clock, over a bundled metrics source.
func withSampler(outcome analysis.Outcome) func(*ReconcileInputs) {
	return func(in *ReconcileInputs) {
		in.Sampler = &countingSample{outcome: outcome, at: in.Now}
		in.BundledPrometheusAddress = "http://prometheus.example.com:9090"
	}
}

// servingAnalysis is the analysis-gated split serving inside its bake window
// with one passing sample consumed.
func servingAnalysis(t *testing.T, policy *v1beta1.OnInconclusive) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(twoStepAnalysis(policy), nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	withSampler(analysis.Pass)(&in)
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixtureServingAnalysis(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return servingAnalysis(t, nil)
}

func fixtureServingAnalysisRollback(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return servingAnalysis(t, onInconclusivePtr(v1beta1.OnInconclusiveRollback))
}

func fixtureServingAnalysisRollbackOnStall(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return servingAnalysis(t, onInconclusivePtr(v1beta1.OnInconclusiveRollbackOnStall))
}

// fixtureServingAnalysisApplied is servingApplied with the second step
// analysis-gated inside its bake window.
func fixtureServingAnalysisApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	steps := threeStepTimedSecond(time.Hour)
	steps[1].Analysis = analysisGate(nil)
	return servingApplied(t, steps, withSampler(analysis.Pass))
}

// fixturePreHoldApplied is a pre-step hold on a step whose record still
// carries an applied promote: the clamp landed while a lagging cache showed
// the promote that advanced the step before, so the record kept it.
func fixturePreHoldApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureServingManualApplied(t)
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).PreStepHold = true
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the copy is taken again; the hold stands
	return isvc, in
}

// drainingApplied walks a two-step ladder into its final step under a
// promote whose value the record still carries, as servingApplied does for
// an intermediate step; the final step shifts 100% on the last pass.
func drainingApplied(t *testing.T, steps []v1beta1.RolloutGroupStep, scaleDownDelay *int32) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(steps, scaleDownDelay))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the promote advances to the final step
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	mustReconcile(t, isvc, in) // the copy is taken again; 100% shifts
	return isvc, in
}

func fixtureDrainingApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return drainingApplied(t, twoStep(), i32(3600))
}

// twoStepHeldFinal is a manual step then a final step under a bare pause.
func twoStepHeldFinal() []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{}},
	}
}

func fixtureDrainingHeldApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return drainingApplied(t, twoStepHeldFinal(), nil)
}

// drainingShortApplied is fixtureDrainingApplied with a canary pod lost
// after d, the copy still showing so the record keeps the applied promote.
func drainingShortApplied(t *testing.T, d time.Duration) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDrainingApplied(t)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	in.Now = in.Now.Add(d)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixtureDrainingShortApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return drainingShortApplied(t, 2*time.Hour)
}

func fixtureDrainingShortInWindowApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return drainingShortApplied(t, time.Minute)
}

// heldFinal walks a ladder whose final step carries the given gate to 100%
// traffic behind that gate, inside a three-hour drain window.
func heldFinal(t *testing.T, gate v1beta1.RolloutGroupStep, prep func(*ReconcileInputs)) (*v1beta1.InferenceService, ReconcileInputs) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		gate,
	}
	isvc := pinActiveRun(canaryISVC(steps, i32(10800)))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	if prep != nil {
		prep(&in)
	}
	mustReconcile(t, isvc, in)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	mustReconcile(t, isvc, in) // 100% traffic, the final gate holds
	return isvc, in
}

// fixtureDrainingHeldTimed is the final step at 100% behind a one-hour soak.
func fixtureDrainingHeldTimed(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return heldFinal(t, v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: time.Hour}}}, nil)
}

// drainingHeldAnalysis is the final step at 100% behind the metric gate,
// baking for an hour with one passing sample consumed.
func drainingHeldAnalysis(t *testing.T, policy *v1beta1.OnInconclusive) (*v1beta1.InferenceService, ReconcileInputs) {
	gate := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: time.Hour}}, Analysis: analysisGate(policy)}
	return heldFinal(t, gate, withSampler(analysis.Pass))
}

func fixtureDrainingHeldAnalysis(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return drainingHeldAnalysis(t, nil)
}

func fixtureDrainingHeldAnalysisRollback(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return drainingHeldAnalysis(t, onInconclusivePtr(v1beta1.OnInconclusiveRollback))
}

func fixtureDrainingHeldAnalysisRollbackOnStall(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return drainingHeldAnalysis(t, onInconclusivePtr(v1beta1.OnInconclusiveRollbackOnStall))
}

// fixtureDoneRollingApplied is the done sentinel with the released instance
// still rolling, after a release that recorded a live promote: the record
// carries it and the annotation is taken.
func fixtureDoneRollingApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDraining(t)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	in.Now = in.Now.Add(2 * time.Hour)
	mustReconcile(t, isvc, in) // the release records the promote
	return isvc, in
}

// fixtureDoneApplied is the completed cutover whose record still carries
// the recorded promote, the copy having shown on every pass since.
func fixtureDoneApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDoneRollingApplied(t)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	in.PerRevisionPods = map[string]int32{"new": 4}
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureRollingBackApplied is a revert in flight whose record still carries
// an applied promote from the step before the rollback.
func fixtureRollingBackApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureServingManualApplied(t)
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixtureRolledBackApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureRollingBackApplied(t)
	in.PerRevisionPods = map[string]int32{"old": 4}
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureFailedApplied is a park on the ready timeout whose record still
// carries an applied promote from the step before.
func fixtureFailedApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureStagingApplied(t)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the copy is taken; the capacity wait is clocked
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
	mustReconcile(t, isvc, in) // the copy is taken again; the wait outlives the timeout
	return isvc, in
}

// fixtureFailedStableMissingApplied is the stable-missing park whose record
// still carries an applied promote from the step before the rollback.
func fixtureFailedStableMissingApplied(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureRollingBackApplied(t)
	parkFailed(isvc, v1beta1.EngineComponent, rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent), v1beta1.CanaryFailureStableRevisionMissing, in.Now)
	delete(isvc.Annotations, constants.RolloutRollbackAnnotation)
	return isvc, in
}

func fixtureIdle(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := canaryISVC(twoStep(), nil)
	runWithTargets(isvc, v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "old", StableRevision: "old"})
	in := baseInputs(isvc, map[string]int32{"old": 4})
	in.CanaryRevisionHash, in.StableRevisionHash, in.TargetID = "old", "old", "t-idle"
	return isvc, in
}

func fixtureStaging(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(twoStep(), nil))
	in := baseInputs(isvc, map[string]int32{"new": 0, "old": 4})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in) // arms and parks on the capacity gate
	return isvc, in
}

func fixtureServingManual(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureStaging(t)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
	mustReconcile(t, isvc, in) // gate met, split serving, manual hold
	return isvc, in
}

func fixtureServingTimed(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(twoStepTimed(time.Hour), nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixturePreHold(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureServingManual(t)
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).PreStepHold = true
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureDraining is the final step at 100% traffic inside its drain window.
// The step holds the last stable instance, so three instances are Ready on
// the canary revision and one still serves the stable one.
func fixtureDraining(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(twoStep(), i32(3600)))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the promote advances to the final step
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	mustReconcile(t, isvc, in) // 100% traffic, drain window open
	return isvc, in
}

// fixtureDrainingHeld is the final step at 100% traffic with a manual gate
// still to open: the drain waits on the operator.
func fixtureDrainingHeld(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{}},
	}
	isvc := pinActiveRun(canaryISVC(steps, nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in)
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in)
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	mustReconcile(t, isvc, in) // 100% traffic, the final gate holds
	return isvc, in
}

// fixtureStagingFinal is the final step of a manual ladder short of its
// capacity before it ever served: the first step's canary pods are gone by
// the time the final step stages, so no canary pod serves while it waits.
func fixtureStagingFinal(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{}},
	}
	isvc := pinActiveRun(canaryISVC(steps, nil))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in) // arms, gate met, split serving, manual hold
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the promote advances to the final step
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 4}
	mustReconcile(t, isvc, in) // the record clears; the final step waits on its capacity
	return isvc, in
}

// fixtureDrainingShort is the final step held on capacity past its drain
// window: 100% traffic landed on three canary instances, the window elapsed,
// and a canary pod is gone when the release would be read. The stable
// instance stays held until the capacity is back.
func fixtureDrainingShort(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDraining(t)
	in.Now = in.Now.Add(2 * time.Hour)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureDrainingShortInWindow is the final step held on capacity inside its
// drain window: 100% traffic landed and a canary pod went before the window
// elapsed.
func fixtureDrainingShortInWindow(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDraining(t)
	in.Now = in.Now.Add(time.Minute)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureFailedAtTheRelease is the final step parked on the ready timeout of
// its capacity wait: the stable instance is still held and carries the
// traffic again.
func fixtureFailedAtTheRelease(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDrainingShort(t)
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureDoneRolling is the done sentinel with the released stable instance
// still rolling: the drain elapsed, the floor is gone, one instance still
// serves the stable revision.
func fixtureDoneRolling(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDraining(t)
	in.Now = in.Now.Add(2 * time.Hour)
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureDone is the completed cutover: every instance serves the canary
// revision and the unit reads Stable.
func fixtureDone(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDoneRolling(t)
	in.PerRevisionPods = map[string]int32{"new": 4}
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixtureRollingBack(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureServingManual(t)
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixtureRolledBack(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureRollingBack(t)
	in.PerRevisionPods = map[string]int32{"old": 4}
	mustReconcile(t, isvc, in)
	return isvc, in
}

func fixtureFailed(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureStaging(t)
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureFailedStableMissing is a rollback that found no stable revision to
// return to. The park is decided where the revision history is read, in
// Dispatch, so the fixture records it the way Dispatch does; the request is
// gone the way the controller's removal after the park's flush leaves it.
func fixtureFailedStableMissing(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureRollingBack(t)
	parkFailed(isvc, v1beta1.EngineComponent, rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent), v1beta1.CanaryFailureStableRevisionMissing, in.Now)
	delete(isvc.Annotations, constants.RolloutRollbackAnnotation)
	return isvc, in
}

// fixtureRollingBackPD is a rollback in a two-member unit (engine primary,
// decoder secondary) with both members still serving their rejected
// revisions. The primary view carries the engine's ready pods; the group view
// carries every pod of both members, ready or not, and their stable identities.
func fixtureRollingBackPD(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := canaryISVC(twoStep(), nil)
	isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	runWithTargets(isvc,
		v1beta1.RolloutRunTarget{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
		v1beta1.RolloutRunTarget{Component: v1beta1.DecoderComponent, Revision: "dec-new", StableRevision: "dec-old"},
	)
	in := baseInputs(isvc, nil)
	in.TargetID = "t1"
	in.GroupStableRevisionHashes = map[v1beta1.ComponentType]string{v1beta1.EngineComponent: "old", v1beta1.DecoderComponent: "dec-old"}
	setGroupPods(&in, map[string]int32{"new": 2, "old": 2}, nil, map[string]int32{"dec-new": 2, "dec-old": 2})
	mustReconcile(t, isvc, in) // arms, gate met, split serving, manual hold
	isvc.Annotations = map[string]string{constants.RolloutRollbackAnnotation: "true"}
	mustReconcile(t, isvc, in)
	return isvc, in
}

// setGroupPods writes the engine's ready pods (the primary view) and every
// pod of the engine and the decoder (the group view). A nil engineTotal means
// every engine pod is ready.
func setGroupPods(in *ReconcileInputs, engineReady, engineTotal, decoderTotal map[string]int32) {
	if engineTotal == nil {
		engineTotal = engineReady
	}
	in.PerRevisionPods = engineReady
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{
		v1beta1.EngineComponent:  engineTotal,
		v1beta1.DecoderComponent: decoderTotal,
	}
}

func annotate(isvc *v1beta1.InferenceService, key, value string) {
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	isvc.Annotations[key] = value
}

func evPause(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.PausedRolloutAnnotation, "true")
}
func evPromoteMatch(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	annotate(isvc, constants.RolloutPromoteAnnotation, in.CanaryRevisionHash)
}
func evPromoteStale(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.RolloutPromoteAnnotation, "stale")
}
func evForceMatch(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	annotate(isvc, constants.RolloutPromoteForceAnnotation, in.CanaryRevisionHash)
}
func evForceStale(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.RolloutPromoteForceAnnotation, "stale")
}

// evForcePastReadyTimeout: the force lands once the capacity wait has already
// outlived the ready timeout.
func evForcePastReadyTimeout(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evForceMatch(isvc, in)
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
}
func evRollback(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
}

// evResumeOwn names the unit's own canary revision; evResumeStale names the
// stable revision, which no canary carries.
func evResumeOwn(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.RolloutResumeAnnotation, rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).CanaryRevisionHash)
}
func evResumeStale(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	annotate(isvc, constants.RolloutResumeAnnotation, "old")
}
func evNewTarget(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.CanaryRevisionHash, in.TargetID = "newer", "t2"
	in.PerRevisionPods = map[string]int32{"old": 4}
}

// evNewTargetAtRunOpen presents the new target the way the controller does:
// the run layer reopens the run toward it and BindRun re-arms the unit before
// the executor's pass, so the pass sees the run's target as its own.
func evNewTargetAtRunOpen(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "newer", StableRevision: "old"},
	}
	g := rollout.CanaryGroup(isvc, rollout.Policies{})
	if err := BindRun(context.Background(), nil, isvc, g, false); err != nil {
		panic(err)
	}
	in.CanaryRevisionHash, in.TargetID = "newer", activeCanaryTargetID(isvc, g)
	in.PerRevisionPods = map[string]int32{"old": 4}
}

// The rejected revision is still draining when the new target appears.
func evNewTargetWhileDraining(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evNewTarget(isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
}
func evNewTargetAtRunOpenWhileDraining(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evNewTargetAtRunOpen(isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
}

// evOtherGroupRun: another group's retarget opened a fresh run for the whole
// service. The run layer pins a unit holding a rejected revision at its
// stable revision, so the unit's TargetID changes while its IR target is
// still the rejected revision and BindRun leaves it alone.
func evOtherGroupRun(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.RouterComponent, Revision: "rnew", StableRevision: "rold"},
		{Component: v1beta1.EngineComponent, Revision: "old", StableRevision: "old"},
	}
	in.TargetID = activeCanaryTargetID(isvc, rollout.CanaryGroup(isvc, rollout.Policies{}))
}
func evReplicasUp(_ *v1beta1.InferenceService, in *ReconcileInputs) { in.DesiredReplicas = 8 }
func evCapacityDropped(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 2}
}
func evDrainingCapacityDropped(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
}

// evDrainingCapacityDroppedPastTheWindow: the drain window has elapsed and a
// canary pod is gone when the release would be read.
func evDrainingCapacityDroppedPastTheWindow(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evClockElapsed(isvc, in)
	evDrainingCapacityDropped(isvc, in)
}

// evDrainingCapacityReached: every staged canary instance serves again.
func evDrainingCapacityReached(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
}

// evReplacementReady: the released instance's replacement is Ready on the
// canary revision while the stable instance it replaces still serves.
func evReplacementReady(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 4, "old": 1}
}

// evLastInstanceRolled: nothing serves the stable revision any more.
func evLastInstanceRolled(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 4}
}

// evCanaryPodLost: a canary pod is lost after the cutover completed.
func evCanaryPodLost(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 3}
}
func evCapacityReached(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
}
func evReadyTimeout(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
}
func evClockElapsed(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Hour)
}

// evRestartInSoak: the soak since the split first served has elapsed, but a
// canary pod died and came back half a soak ago.
func evRestartInSoak(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Hour)
	in.CanaryRestartedAt = in.Now.Add(-30 * time.Minute)
}

// evRestartSoaked: a canary pod died and came back inside the step, and has
// served a full soak since.
func evRestartSoaked(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Hour)
	in.CanaryRestartedAt = in.Now.Add(-61 * time.Minute)
}

// evPromoteAfterRestartInSoak: the operator promotes while the restarted
// canary pod is still inside its soak.
func evPromoteAfterRestartInSoak(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evRestartInSoak(isvc, in)
	evPromoteMatch(isvc, in)
}

// evCanaryRestarted: a canary pod died and came back a minute into the
// step; the kubelet has it serving again.
func evCanaryRestarted(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.CanaryRestartedAt = in.Now
	in.Now = in.Now.Add(time.Minute)
}
func evRejectedPodsGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"old": 4}
}

// evPrimaryReverted: the engine is back on stable while a decoder pod still
// exists on the decoder's rejected revision; the group view counts it whether
// or not it is ready.
func evPrimaryReverted(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	setGroupPods(in, map[string]int32{"old": 4}, nil, map[string]int32{"dec-old": 3, "dec-new": 1})
}
func evGroupReverted(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	setGroupPods(in, map[string]int32{"old": 4}, nil, map[string]int32{"dec-old": 4})
}

// evPrimaryRejectedPodUnready: every ready engine pod is on stable, but an
// engine pod on the rejected revision still exists, not ready.
func evPrimaryRejectedPodUnready(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	setGroupPods(in, map[string]int32{"old": 4}, map[string]int32{"old": 4, "new": 1}, map[string]int32{"dec-old": 4})
}

// evMemberStableUnknown: the decoder still has a rejected pod but no stable
// identity on record, so it is sent no revert signal and is not waited on.
func evMemberStableUnknown(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evPrimaryReverted(isvc, in)
	in.GroupStableRevisionHashes[v1beta1.DecoderComponent] = ""
}

// evCanaryCrashed: a canary pod is in a crash loop after its Instance was
// serving. The kubelet has it back up for the moment, so the capacity counts
// are unchanged.
func evCanaryCrashed(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Crash = &CanaryCrash{Component: v1beta1.EngineComponent, PodName: "svc-engine-3-default-0", Detail: "Error (exit 1)"}
}

// evCanaryPodDeleted: a canary pod is deleted by hand a minute into the
// step. The pass reads the pod on its way out, its runner stopped by the
// kubelet, and sees the dip.
func evCanaryPodDeleted(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(time.Minute)
	in.Crash = deletedCanaryPodReading(isvc, in.Now)
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 2}
}
func evRunLost(_ *v1beta1.InferenceService, in *ReconcileInputs) { in.RunActive = false }
func evRunLostAfterSoak(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.RunActive = false
	in.Now = in.Now.Add(2 * time.Hour)
}
func evNothing(*v1beta1.InferenceService, *ReconcileInputs) {}

// evUnpause: the rollout was paused for a pass and the pause is cleared; the
// pass under test is the first one after it.
func evUnpause(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evPause(isvc, in)
	if _, err := Reconcile(context.Background(), *in); err != nil {
		panic(err)
	}
	delete(isvc.Annotations, constants.PausedRolloutAnnotation)
}

// evResumeOtherUnit: the resume names the revision another unit's canary
// carries. The router gets its own canary group in the pinned plan and a
// record on that revision, so the engine's pass must leave the verb to it.
func evResumeOtherUnit(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	group := v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, Canary: &v1beta1.GroupCanary{Steps: twoStep()}}
	isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, group)
	run := isvc.Status.Rollout.ActiveRun
	run.Plan.Groups = append(run.Plan.Groups, v1beta1.RolloutRunGroup{Source: v1beta1.RolloutPlanSourceInline, Group: group})
	rollout.SetCanaryStatusFor(&isvc.Status, v1beta1.RouterComponent, &v1beta1.CanaryStatus{TargetID: "rt1", CanaryRevisionHash: "rnew", StableRevisionHash: "rold"})
	annotate(isvc, constants.RolloutResumeAnnotation, "rnew")
}

// evPlanDrift: the spec's ladder is edited under the pinned run; the pinned
// plan is what executes, so the edit changes nothing until a repin.
func evPlanDrift(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Traffic = 30
}

// reconcileOnce runs one pass inside an event, for the verbs whose cell is
// the pass after the one that applied them.
func reconcileOnce(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	if _, err := Reconcile(context.Background(), *in); err != nil {
		panic(err)
	}
}

// evPromoteReshown / evForceReshown: the verb lands, the pass applies it, and
// a lagging cache shows the same value again on the next pass.
func evPromoteReshown(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evPromoteMatch(isvc, in)
	reconcileOnce(isvc, in)
	evPromoteMatch(isvc, in)
}
func evForceReshown(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evForceMatch(isvc, in)
	reconcileOnce(isvc, in)
	evForceMatch(isvc, in)
}

// evRollbackReshown: the request lands and is applied; the controller has
// not removed it yet when the next pass reads it.
func evRollbackReshown(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evRollback(isvc, in)
	reconcileOnce(isvc, in)
}

// evRollbackStableEmpty: the request lands while the stable revision runs
// no live Instance under the held floor.
func evRollbackStableEmpty(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 1}
	evRollback(isvc, in)
}

// evDrainingRollbackStableEmpty: the request lands in the drain while the
// held stable instance is gone.
func evDrainingRollbackStableEmpty(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 2}
	evRollback(isvc, in)
}

// evRunAdopted: the run layer adopts the in-flight ladder, binding a record
// that carries no target id to the run's id without a reset.
func evRunAdopted(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).TargetID = ""
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: in.CanaryRevisionHash, StableRevision: "old"},
	}
	g := rollout.CanaryGroup(isvc, rollout.Policies{})
	if err := BindRun(context.Background(), nil, isvc, g, true); err != nil {
		panic(err)
	}
	in.TargetID = activeCanaryTargetID(isvc, g)
}

// Record shapes the controller repairs in place: a status missing a field,
// or one written by another writer.
func evRecordNoTargetID(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).TargetID = ""
}
func evRecordStableEqualsCanary(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	cs.StableRevisionHash = cs.CanaryRevisionHash
}
func evRecordNoStable(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).StableRevisionHash = ""
}
func evRecordNegativeStep(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).CurrentStep = -1
}
func evPhaseFailedNoMarker(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).Failed = nil
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseFailed)
}

// evCapacityBack: the split dipped for a pass and the count is back at the
// staged number.
func evCapacityBack(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evCapacityDropped(isvc, in)
	reconcileOnce(isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 2}
}

// evDipThenReadyTimeout: the split dipped and stays short past the ready
// timeout, clocked from the dip.
func evDipThenReadyTimeout(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evCapacityDropped(isvc, in)
	reconcileOnce(isvc, in)
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
}

// evUnpauseAfterSoak: the pause outlasted the step's soak.
func evUnpauseAfterSoak(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evPause(isvc, in)
	in.Now = in.Now.Add(2 * time.Hour)
	reconcileOnce(isvc, in)
	delete(isvc.Annotations, constants.PausedRolloutAnnotation)
}
func evReplicasDown(_ *v1beta1.InferenceService, in *ReconcileInputs) { in.DesiredReplicas = 2 }

// evSample: after d, the gate reads one sample with the given outcome.
func evSample(outcome analysis.Outcome, d time.Duration) func(*v1beta1.InferenceService, *ReconcileInputs) {
	return func(_ *v1beta1.InferenceService, in *ReconcileInputs) {
		in.Now = in.Now.Add(d)
		withSampler(outcome)(in)
	}
}

// evSampleAtLimit: the step already counted two failing checks when the
// third breach lands.
func evSampleAtLimit(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).AnalysisFailedChecks = 2
	evSample(analysis.Fail, 2*time.Minute)(isvc, in)
}

// evSampleAtLimitStableEmpty: the breach at the limit lands while the stable
// revision runs no Instance under the held floor.
func evSampleAtLimitStableEmpty(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evSampleAtLimit(isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 2}
}

// The three ways the gate cannot read: no sampler wired, a source that
// cannot be built, and no fresh sample.
func evNoSampler(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Minute)
	in.Sampler = nil
}
func evSourceError(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Minute)
	in.Prometheus = &v1beta1.AnalysisPrometheus{AuthRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "token"}, Key: "token"}}
}
func evNoFreshSample(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Minute)
	in.Sampler = missingSample{}
}

// evDrainingCanaryPodDeleted: a canary pod of the drain is deleted by hand a
// minute in; evDrainingCanaryPodDeletedDeeper takes a second one from a
// drain already short.
func evDrainingCanaryPodDeleted(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(time.Minute)
	in.Crash = deletedCanaryPodReading(isvc, in.Now)
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
}
func evDrainingCanaryPodDeletedDeeper(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evDrainingCanaryPodDeleted(isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 1, "old": 1}
}

// evRetargetObserved: the pinned run retargets the unit and the dispatch
// observes the new target; evRetargetWithoutRun observes it with no run
// pinned.
func evRetargetObserved(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "new", StableRevision: "old"},
	}
	in.CanaryRevisionHash, in.TargetID = "new", "t1"
}
func evRetargetWithoutRun(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evRetargetObserved(isvc, in)
	in.RunActive = false
}

// The verbs that reach a park with no run pinned.
func evResumeOwnRunLost(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evRunLost(isvc, in)
	evResumeOwn(isvc, in)
}
func evRollbackRunLost(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evRunLost(isvc, in)
	evRollback(isvc, in)
}

// evDoneCanaryPodDeleted: a canary pod is deleted by hand after the cutover
// completed.
func evDoneCanaryPodDeleted(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(time.Minute)
	in.Crash = deletedCanaryPodReading(isvc, in.Now)
	in.PerRevisionPods = map[string]int32{"new": 3}
}

// evSampleAtLimitStableEmptyDraining: the breach at the limit lands at 100%
// while the held stable instance is gone.
func evSampleAtLimitStableEmptyDraining(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evSampleAtLimit(isvc, in)
	in.PerRevisionPods = map[string]int32{"new": 3}
}

// evBakeElapsedNoSample: the bake window elapsed with no fresh sample to read
// it with.
func evBakeElapsedNoSample(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.Now = in.Now.Add(2 * time.Hour)
	in.Sampler = missingSample{}
}

// repinLadder replaces the pinned ladder with steps, as a repin renders the
// spec into the pin, and clamps the record the way the run layer's clamp
// does: a unit done under the old ladder stays done at the new ladder's
// length, an index past the new ladder's end lands on its last step, and a
// clamped step whose traffic exceeds the recorded weight arms the pre-step
// hold.
func repinLadder(isvc *v1beta1.InferenceService, steps []v1beta1.RolloutGroupStep) {
	old := len(isvc.Spec.Rollout.Groups[0].Canary.Steps)
	isvc.Spec.Rollout.Groups[0].Canary.Steps = steps
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps = append([]v1beta1.RolloutGroupStep(nil), steps...)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if old > 0 && int(cs.CurrentStep) >= old {
		cs.CurrentStep = int32(len(steps))
		return
	}
	if int(cs.CurrentStep) >= len(steps) {
		cs.CurrentStep = int32(len(steps)) - 1
	}
	if steps[cs.CurrentStep].Traffic > cs.ObservedTrafficWeight {
		cs.PreStepHold = true
	}
}

// evRepinShorter: the ladder is repinned to its final step alone, so the
// index clamps onto a step at the recorded weight; evRepinShorterHeld is the
// same with a bare pause on that step.
func evRepinShorter(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	repinLadder(isvc, []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}})
}
func evRepinShorterHeld(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	repinLadder(isvc, []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{}}})
}

// evRepinLonger: a longer ladder whose step at the record's index carries no
// more traffic than the recorded weight, so the clamp arms no hold.
func evRepinLonger(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	repinLadder(isvc, []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("25%"), Traffic: 0, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	})
}

// evRepinRaise: the clamped step would raise traffic above the recorded
// weight, so the clamp arms the pre-step hold.
func evRepinRaise(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	repinLadder(isvc, []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}})
}

// evRepinPastDone: a unit that finished under the old ladder is repinned
// onto a longer one; done stays done at the new ladder's length.
func evRepinPastDone(isvc *v1beta1.InferenceService, _ *ReconcileInputs) {
	repinLadder(isvc, threeStep())
}

// evResumeReshownAcrossSecondPark: the resume lands and re-arms the park, the
// re-armed step's capacity wait outlives the ready timeout and parks the
// unit again, and a lagging cache then shows the same resume once more.
func evResumeReshownAcrossSecondPark(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evResumeOwn(isvc, in)
	reconcileOnce(isvc, in) // the resume re-arms the unit at step 0
	in.PerRevisionPods = map[string]int32{"old": 4}
	reconcileOnce(isvc, in) // the re-armed step clocks its capacity wait
	in.Now = in.Now.Add(cellReadyTimeout + time.Second)
	reconcileOnce(isvc, in) // the wait outlives the timeout: a second park
	evResumeOwn(isvc, in)
}

func TestReconcileCells(t *testing.T) {
	const (
		idle        = rollout.CanaryStateIdle
		staging     = rollout.CanaryStateStaging
		serving     = rollout.CanaryStateServing
		preHold     = rollout.CanaryStatePreHold
		draining    = rollout.CanaryStateDraining
		done        = rollout.CanaryStateDone
		rollingBack = rollout.CanaryStateRollingBack
		rolledBack  = rollout.CanaryStateRolledBack
		failed      = rollout.CanaryStateFailed
	)
	cells := []cell{
		// pause: the executor observes and writes nothing.
		{"canary/Idle/pause", fixtureIdle, evPause, cellWant{state: idle}},
		{"canary/Staging/pause", fixtureStaging, evPause, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/pause", fixtureServingManual, evPause, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/pause", fixturePreHold, evPause, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Draining/pause", fixtureDraining, evPause, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/pause", fixtureDrainingShort, evPause, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Failed[release]/pause", fixtureFailedAtTheRelease, evPause, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Done/pause", fixtureDone, evPause, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done[rolling]/pause", fixtureDoneRolling, evPause, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2}},
		{"canary/RollingBack/pause", fixtureRollingBack, evPause, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RolledBack/pause", fixtureRolledBack, evPause, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/pause", fixtureFailed, evPause, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// promote
		{"canary/Idle/promote", fixtureIdle, evPromoteMatch, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Staging/promote", fixtureStaging, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/promote[match]", fixtureServingManual, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving/promote[stale]", fixtureServingManual, evPromoteStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/promote", fixtureServingTimed, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/PreHold/promote", fixturePreHold, evPromoteMatch, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		// An ungated final step has no gate for a promote to open: the hold on capacity keeps
		// it live, unconsumed, for the release to record.
		{"canary/Draining[capacityShort]/promote", fixtureDrainingShort, evPromoteMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Done/promote", fixtureDone, evPromoteMatch, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/RolledBack/promote", fixtureRolledBack, evPromoteMatch, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/promote", fixtureFailed, evPromoteMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[release]/promote", fixtureFailedAtTheRelease, evPromoteMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// A final gate short of its capacity is not reached by a promote: the capacity
		// gate in front of it holds, and the promote stays for the pass that meets it.
		{"canary/Staging[final]/promote", fixtureStagingFinal, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		// promoteForce: a promote that also lifts the step's capacity wait. It applies
		// at every step of an armed ladder, capacity met or not; where the ladder is
		// held (a park, a rollback, a pause) it is consumed with the reason.
		{"canary/Idle/promoteForce", fixtureIdle, evForceMatch, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Staging/promoteForce", fixtureStaging, evForceMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Staging/promoteForce[stale]", fixtureStaging, evForceStale, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/promoteForce[match]", fixtureServingManual, evForceMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Serving/promoteForce[stale]", fixtureServingManual, evForceStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/promoteForce", fixtureServingTimed, evForceMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/PreHold/promoteForce", fixturePreHold, evForceMatch, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		// An ungated final step keeps its drain open under the force: no clock, no
		// park, and the force stays live until the completion edge records it.
		{"canary/Draining/promoteForce", fixtureDraining, evForceMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/promoteForce", fixtureDrainingShort, evForceMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/promoteForce[pastTimeout]", fixtureDrainingShort, evForcePastReadyTimeout, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[M]/promoteForce", fixtureDrainingHeld, evForceMatch, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		// A forced final gate opens into its drain with no canary pod serving: the stable
		// revision carries everything, and the force stays live until the release.
		{"canary/Staging[final]/promoteForce", fixtureStagingFinal, evForceMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Done/promoteForce", fixtureDone, evForceMatch, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done[rolling]/promoteForce", fixtureDoneRolling, evForceMatch, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/RollingBack/promoteForce", fixtureRollingBack, evForceMatch, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/RolledBack/promoteForce", fixtureRolledBack, evForceMatch, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Failed/promoteForce", fixtureFailed, evForceMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Failed[stableMissing]/promoteForce", fixtureFailedStableMissing, evForceMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Failed[release]/promoteForce", fixtureFailedAtTheRelease, evForceMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		// rollback
		{"canary/Idle/rollback", fixtureIdle, evRollback, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable, consume: []string{constants.RolloutRollbackAnnotation}}},
		{"canary/Staging/rollback", fixtureStaging, evRollback, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Serving/rollback", fixtureServingManual, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/PreHold/rollback", fixturePreHold, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Draining/rollback", fixtureDraining, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[capacityShort]/rollback", fixtureDrainingShort, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Done/rollback", fixtureDone, evRollback, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, consume: []string{constants.RolloutRollbackAnnotation}}},
		{"canary/Done[rolling]/rollback", fixtureDoneRolling, evRollback, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutRollbackAnnotation}}},
		{"canary/RolledBack/rollback", fixtureRolledBack, evRollback, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/rollback", fixtureFailed, evRollback, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		// the park at the release still has canary pods to drain, so its rollback drains first
		{"canary/Failed[release]/rollback", fixtureFailedAtTheRelease, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		// A park for a missing stable revision already carries the rejection the
		// request asked for: the request is refused and handed back, the park unmoved.
		{"canary/Failed[stableMissing]/rollback", fixtureFailedStableMissing, evRollback, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, consume: []string{constants.RolloutRollbackAnnotation}}},
		// resume: honored at a park it names; everywhere else, and for a hash
		// no canary carries, consumed and ignored.
		{"canary/Idle/resume", fixtureIdle, evResumeStale, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Staging/resume", fixtureStaging, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Staging/resume[stale]", fixtureStaging, evResumeStale, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Serving/resume", fixtureServingManual, evResumeOwn, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Serving/resume[stale]", fixtureServingManual, evResumeStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/PreHold/resume", fixturePreHold, evResumeOwn, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining/resume", fixtureDraining, evResumeOwn, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining[capacityShort]/resume", fixtureDrainingShort, evResumeOwn, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Done/resume", fixtureDone, evResumeOwn, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Done/resume[stale]", fixtureDone, evResumeStale, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/RollingBack/resume", fixtureRollingBack, evResumeOwn, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/RolledBack/resume", fixtureRolledBack, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/RolledBack/resume[stale]", fixtureRolledBack, evResumeStale, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed/resume", fixtureFailed, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed/resume[stale]", fixtureFailed, evResumeStale, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed[stableMissing]/resume", fixtureFailedStableMissing, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed[release]/resume", fixtureFailedAtTheRelease, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		// new target
		{"canary/Staging/newTarget", fixtureStaging, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/newTarget", fixtureServingManual, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/PreHold/newTarget", fixturePreHold, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining/newTarget", fixtureDraining, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining[capacityShort]/newTarget", fixtureDrainingShort, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Done/newTarget", fixtureDone, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Done[rolling]/newTarget", fixtureDoneRolling, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/RollingBack/newTarget", fixtureRollingBack, evNewTargetWhileDraining, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/RollingBack/newTarget[runOpen]", fixtureRollingBack, evNewTargetAtRunOpenWhileDraining, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/RolledBack/newTarget", fixtureRolledBack, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/RolledBack/newTarget[runOpen]", fixtureRolledBack, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		// A run another group opened offers the held unit no target: the hold stays.
		{"canary/RollingBack/newTarget[otherGroup]", fixtureRollingBack, evOtherGroupRun, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RolledBack/newTarget[otherGroup]", fixtureRolledBack, evOtherGroupRun, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/newTarget", fixtureFailed, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Failed[stableMissing]/newTarget", fixtureFailedStableMissing, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Failed[release]/newTarget", fixtureFailedAtTheRelease, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		// replicas: more capacity to stage holds the step where it is.
		{"canary/Serving/replicasUp", fixtureServingManual, evReplicasUp, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		// run lost: the canary holds where it is; even an elapsed soak does not advance it.
		// A revert keeps draining (the run layer adopts it in place) and a settled hold stays settled.
		{"canary/Serving/runLost", fixtureServingManual, evRunLost, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/runLost", fixtureServingTimed, evRunLostAfterSoak, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Draining[capacityShort]/runLost", fixtureDrainingShort, evRunLost, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/RollingBack/runLost", fixtureRollingBack, evRunLost, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RolledBack/runLost", fixtureRolledBack, evRunLost, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		// capacity reached
		{"canary/Staging/capacityReached", fixtureStaging, evCapacityReached, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Failed/capacityReached", fixtureFailed, evCapacityReached, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// the capacity a held release waited for is back: the pass reads it and releases
		{"canary/Draining[capacityShort]/capacityReached", fixtureDrainingShort, evDrainingCapacityReached, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		// inside the window the capacity back resumes the drain; a park is not re-evaluated by it
		{"canary/Draining[capacityShort,inWindow]/capacityReached", fixtureDrainingShortInWindow, evDrainingCapacityReached, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Failed[release]/capacityReached", fixtureFailedAtTheRelease, evDrainingCapacityReached, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// capacity dropped: a serving split keeps its phase and weight; a drain runs on, and past
		// its window the release waits for the capacity with the stable instance still held.
		{"canary/Serving/capacityDropped", fixtureServingManual, evCapacityDropped, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/capacityDropped", fixturePreHold, evCapacityDropped, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Draining/capacityDropped", fixtureDraining, evDrainingCapacityDropped, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/capacityDropped[drainElapsed]", fixtureDraining, evDrainingCapacityDroppedPastTheWindow, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		// a canary pod lost after the cutover completed is the workload's repair, not a canary state
		{"canary/Done/capacityDropped", fixtureDone, evCanaryPodLost, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		// canary crashed: a pod that served and keeps dying parks a live ladder at its step, whatever
		// the counts say now; a hold, a revert and a finished cutover are not moved by it.
		{"canary/Idle/canaryCrashed", fixtureIdle, evCanaryCrashed, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Staging/canaryCrashed", fixtureStaging, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving/canaryCrashed", fixtureServingManual, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving[T]/canaryCrashed", fixtureServingTimed, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/PreHold/canaryCrashed", fixturePreHold, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Draining/canaryCrashed", fixtureDraining, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Draining[capacityShort]/canaryCrashed", fixtureDrainingShort, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Failed[release]/canaryCrashed", fixtureFailedAtTheRelease, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Done/canaryCrashed", fixtureDone, evCanaryCrashed, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done[rolling]/canaryCrashed", fixtureDoneRolling, evCanaryCrashed, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/RollingBack/canaryCrashed", fixtureRollingBack, evCanaryCrashed, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RolledBack/canaryCrashed", fixtureRolledBack, evCanaryCrashed, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/canaryCrashed", fixtureFailed, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// canary pod deleted: a pod on its way out is a loss, so the step keeps its phase and
		// weight like any dip, and a held split stays held.
		{"canary/Staging/canaryPodDeleted", fixtureStaging, evCanaryPodDeleted, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/canaryPodDeleted", fixtureServingManual, evCanaryPodDeleted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/canaryPodDeleted", fixturePreHold, evCanaryPodDeleted, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		// rejected pods gone
		{"canary/RollingBack/rejectedPodsGone", fixtureRollingBack, evRejectedPodsGone, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		// rejected pods gone in a two-member unit: the rollback completes for the group, not the primary
		// alone, and a rejected pod holds it for as long as it exists, ready or not.
		{"canary/RollingBack[PD]/resync", fixtureRollingBackPD, evNothing, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack[PD]/primaryReverted", fixtureRollingBackPD, evPrimaryReverted, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack[PD]/primaryRejectedPodUnready", fixtureRollingBackPD, evPrimaryRejectedPodUnready, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack[PD]/groupReverted", fixtureRollingBackPD, evGroupReverted, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RollingBack[PD]/memberStableUnknown", fixtureRollingBackPD, evMemberStableUnknown, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		// ready timeout
		{"canary/Staging/readyTimeout", fixtureStaging, evReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// a held release's capacity wait runs the same ready timeout, from the wait, inside or past
		// the drain window: the park keeps the final step and the held stable instance, and returns
		// the traffic to it
		{"canary/Draining[capacityShort]/readyTimeout", fixtureDrainingShort, evReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/readyTimeout", fixtureDrainingShortInWindow, evReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// pause elapsed
		{"canary/Serving[T]/pauseElapsed", fixtureServingTimed, evClockElapsed, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		// a canary pod died and came back: a timed soak measures from the newest restart, so the
		// step moves only once the restarted pod has served a full soak; a manual hold is unmoved
		// by it, and a promote is the operator's decision on any gate.
		{"canary/Serving[T]/pauseElapsed[restartInSoak]", fixtureServingTimed, evRestartInSoak, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/pauseElapsed[restartSoaked]", fixtureServingTimed, evRestartSoaked, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Serving[T]/promote[restartInSoak]", fixtureServingTimed, evPromoteAfterRestartInSoak, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving/canaryRestarted", fixtureServingManual, evCanaryRestarted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Staging/canaryRestarted", fixtureStaging, evCanaryRestarted, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		// a held final gate: the drain waits, the phase says so
		{"canary/Draining[M]/gateHolds", fixtureDrainingHeld, evNothing, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/promote", fixtureDrainingHeld, evPromoteMatch, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		// drain elapsed: the done sentinel releases the held stable instance; the unit is not
		// Stable until that instance has rolled
		{"canary/Draining/drainElapsed", fixtureDraining, evClockElapsed, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		// the released instance rolls: the replacement serving is not enough while the stable
		// instance still serves; the pass that finds nothing on the stable revision completes
		{"canary/Done[rolling]/replacementReady", fixtureDoneRolling, evReplacementReady, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/lastInstanceRolled", fixtureDoneRolling, evLastInstanceRolled, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, active: true, complete: true}},
		// nothing changed
		{"canary/Staging/resync", fixtureStaging, evNothing, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/resync", fixtureServingManual, evNothing, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Draining/resync", fixtureDraining, evNothing, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/resync", fixtureDrainingShort, evNothing, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/resync", fixtureDrainingShortInWindow, evNothing, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Failed[release]/resync", fixtureFailedAtTheRelease, evNothing, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Done/resync", fixtureDone, evNothing, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done[rolling]/resync", fixtureDoneRolling, evNothing, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/RolledBack/resync", fixtureRolledBack, evNothing, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/resync", fixtureFailed, evNothing, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/resync", fixtureFailedStableMissing, evNothing, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// --- Idle: the pass has no record to act on; a verb no canary carries
		// is left in place, one another unit carries is left to it, and an
		// edit to the spec or the replica count arms nothing.
		{"canary/Idle/unpause", fixtureIdle, evUnpause, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Idle/promote[stale]", fixtureIdle, evPromoteStale, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Idle/promoteForce[stale]", fixtureIdle, evForceStale, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Idle/resume[otherUnit]", fixtureIdle, evResumeOtherUnit, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Idle/planDrift", fixtureIdle, evPlanDrift, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Idle/replicasUp", fixtureIdle, evReplicasUp, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Idle/resync", fixtureIdle, evNothing, cellWant{state: idle, phase: v1beta1.RolloutPhaseStable}},
		{"canary/Idle/newTarget", fixtureIdle, evRetargetObserved, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Idle/newTarget[runOpen]", fixtureIdle, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Idle/runLost", fixtureIdle, evRetargetWithoutRun, cellWant{state: idle}},
		// --- Staging: the capacity gate holds in front of everything the step
		// could do; the records of applied verbs keep their re-shown copies
		// inert, and an incomplete record shape is repaired in place.
		{"canary/Staging/unpause", fixtureStaging, evUnpause, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/promote[stale]", fixtureStaging, evPromoteStale, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/promote[applied]", fixtureStagingApplied, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Staging/promoteForce[applied]", fixtureStagingApplied, evForceMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Staging/promoteForce[reshown]", fixtureStaging, evForceReshown, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Staging/rollback[reshown]", fixtureStaging, evRollbackReshown, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Staging/rollback[stableEmpty]", fixtureStaging, evRollbackStableEmpty, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Staging/resume[otherUnit]", fixtureStaging, evResumeOtherUnit, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/planDrift", fixtureStaging, evPlanDrift, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/runLost", fixtureStaging, evRunLost, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/runAdopted", fixtureStaging, evRunAdopted, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/newTarget[runOpen]", fixtureStaging, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/replicasUp", fixtureStaging, evReplicasUp, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/record[noTargetID]", fixtureStaging, evRecordNoTargetID, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/record[stableEqualsCanary]", fixtureStaging, evRecordStableEqualsCanary, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/record[noStable]", fixtureStaging, evRecordNoStable, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/record[negativeStep]", fixtureStaging, evRecordNegativeStep, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging/record[failedPhaseNoMarker]", fixtureStaging, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// --- Staging/Final: the last step short of its capacity before it ever
		// served; no canary pod serves, so a rollback reads complete at once and
		// a gated final step opens into its held drain when the capacity lands.
		{"canary/Staging[final]/pause", fixtureStagingFinal, evPause, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/unpause", fixtureStagingFinal, evUnpause, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/promote[stale]", fixtureStagingFinal, evPromoteStale, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/promote[applied]", fixtureStagingFinalApplied, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Staging[final]/promoteForce[reshown]", fixtureStagingFinal, evForceReshown, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Staging[final]/promoteForce[stale]", fixtureStagingFinal, evForceStale, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/promoteForce[applied]", fixtureStagingFinalApplied, evForceMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Staging[final]/rollback", fixtureStagingFinal, evRollback, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, step: 1, active: true, rolledBk: true}},
		{"canary/Staging[final]/rollback[reshown]", fixtureStagingFinal, evRollbackReshown, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, step: 1, active: true, rolledBk: true}},
		{"canary/Staging[final]/resume", fixtureStagingFinal, evResumeOwn, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Staging[final]/resume[stale]", fixtureStagingFinal, evResumeStale, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Staging[final]/resume[otherUnit]", fixtureStagingFinal, evResumeOtherUnit, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/planDrift", fixtureStagingFinal, evPlanDrift, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/runLost", fixtureStagingFinal, evRunLost, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/runAdopted", fixtureStagingFinal, evRunAdopted, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/newTarget", fixtureStagingFinal, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging[final]/newTarget[runOpen]", fixtureStagingFinal, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging[final]/capacityReached", fixtureStagingFinal, evDrainingCapacityReached, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Staging[final]/replicasUp", fixtureStagingFinal, evReplicasUp, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/readyTimeout", fixtureStagingFinal, evReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Staging[final]/canaryCrashed", fixtureStagingFinal, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Staging[final]/canaryRestarted", fixtureStagingFinal, evCanaryRestarted, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/canaryPodDeleted", fixtureStagingFinal, evCanaryPodDeleted, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/record[noTargetID]", fixtureStagingFinal, evRecordNoTargetID, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/record[stableEqualsCanary]", fixtureStagingFinal, evRecordStableEqualsCanary, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/record[noStable]", fixtureStagingFinal, evRecordNoStable, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Staging[final]/record[negativeStep]", fixtureStagingFinal, evRecordNegativeStep, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Staging[final]/record[failedPhaseNoMarker]", fixtureStagingFinal, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Staging[final]/resync", fixtureStagingFinal, evNothing, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		// --- Serving/Manual: the split serves under a bare pause. The applied
		// promote's record keeps its copies inert, a dip keeps the phase and
		// parks only past the ready timeout, and the record is repaired in place.
		{"canary/Serving/unpause", fixtureServingManual, evUnpause, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/promote[reshown]", fixtureServingManual, evPromoteReshown, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving/promote[applied]", fixtureServingManualApplied, evPromoteMatch, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving/promoteForce[reshown]", fixtureServingManual, evForceReshown, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Serving/promoteForce[applied]", fixtureServingManualApplied, evForceMatch, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Serving/rollback[reshown]", fixtureServingManual, evRollbackReshown, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Serving/resume[otherUnit]", fixtureServingManual, evResumeOtherUnit, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/planDrift", fixtureServingManual, evPlanDrift, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/runAdopted", fixtureServingManual, evRunAdopted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/newTarget[runOpen]", fixtureServingManual, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving/capacityReached", fixtureServingManual, evCapacityBack, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/replicasDown", fixtureServingManual, evReplicasDown, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/readyTimeout", fixtureServingManual, evDipThenReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving/record[noTargetID]", fixtureServingManual, evRecordNoTargetID, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/record[stableEqualsCanary]", fixtureServingManual, evRecordStableEqualsCanary, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/record[noStable]", fixtureServingManual, evRecordNoStable, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/record[negativeStep]", fixtureServingManual, evRecordNegativeStep, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving/record[failedPhaseNoMarker]", fixtureServingManual, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// --- Serving/Timed: the same rows on the timed fixture, whose soak is
		// the one input that differs; the soak keeps counting under a pause.
		{"canary/Serving[T]/pause", fixtureServingTimed, evPause, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/unpause", fixtureServingTimed, evUnpause, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/unpause[soaked]", fixtureServingTimed, evUnpauseAfterSoak, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Serving[T]/promote[reshown]", fixtureServingTimed, evPromoteReshown, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving[T]/promote[stale]", fixtureServingTimed, evPromoteStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/promote[applied]", fixtureServingTimedApplied, evPromoteMatch, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving[T]/promoteForce[reshown]", fixtureServingTimed, evForceReshown, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Serving[T]/promoteForce[stale]", fixtureServingTimed, evForceStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/promoteForce[applied]", fixtureServingTimedApplied, evForceMatch, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Serving[T]/rollback", fixtureServingTimed, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Serving[T]/rollback[reshown]", fixtureServingTimed, evRollbackReshown, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Serving[T]/resume", fixtureServingTimed, evResumeOwn, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Serving[T]/resume[stale]", fixtureServingTimed, evResumeStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Serving[T]/resume[otherUnit]", fixtureServingTimed, evResumeOtherUnit, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/planDrift", fixtureServingTimed, evPlanDrift, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/runAdopted", fixtureServingTimed, evRunAdopted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/newTarget", fixtureServingTimed, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving[T]/newTarget[runOpen]", fixtureServingTimed, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving[T]/capacityReached", fixtureServingTimed, evCapacityBack, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/capacityDropped", fixtureServingTimed, evCapacityDropped, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/replicasUp", fixtureServingTimed, evReplicasUp, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/readyTimeout", fixtureServingTimed, evDipThenReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving[T]/canaryRestarted", fixtureServingTimed, evCanaryRestarted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/canaryPodDeleted", fixtureServingTimed, evCanaryPodDeleted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/record[noTargetID]", fixtureServingTimed, evRecordNoTargetID, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/record[stableEqualsCanary]", fixtureServingTimed, evRecordStableEqualsCanary, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/record[noStable]", fixtureServingTimed, evRecordNoStable, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/record[negativeStep]", fixtureServingTimed, evRecordNegativeStep, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[T]/record[failedPhaseNoMarker]", fixtureServingTimed, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving[T]/resync", fixtureServingTimed, evNothing, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		// --- Serving/Analysis: the split serves under the metric gate inside
		// its bake window. A promote overrides the gate; the gate itself holds,
		// advances, rolls back, parks or cannot read, as the sample says.
		{"canary/Serving[A]/pause", fixtureServingAnalysis, evPause, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/unpause", fixtureServingAnalysis, evUnpause, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/promote", fixtureServingAnalysis, evPromoteMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving[A]/promote[reshown]", fixtureServingAnalysis, evPromoteReshown, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving[A]/promote[stale]", fixtureServingAnalysis, evPromoteStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/promote[applied]", fixtureServingAnalysisApplied, evPromoteMatch, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Serving[A]/promoteForce", fixtureServingAnalysis, evForceMatch, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Serving[A]/promoteForce[reshown]", fixtureServingAnalysis, evForceReshown, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Serving[A]/promoteForce[stale]", fixtureServingAnalysis, evForceStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/promoteForce[applied]", fixtureServingAnalysisApplied, evForceMatch, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Serving[A]/rollback", fixtureServingAnalysis, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Serving[A]/rollback[reshown]", fixtureServingAnalysis, evRollbackReshown, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Serving[A]/resume", fixtureServingAnalysis, evResumeOwn, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Serving[A]/resume[stale]", fixtureServingAnalysis, evResumeStale, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Serving[A]/resume[otherUnit]", fixtureServingAnalysis, evResumeOtherUnit, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/planDrift", fixtureServingAnalysis, evPlanDrift, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/runLost", fixtureServingAnalysis, evRunLost, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/runAdopted", fixtureServingAnalysis, evRunAdopted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/newTarget", fixtureServingAnalysis, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving[A]/newTarget[runOpen]", fixtureServingAnalysis, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Serving[A]/capacityReached", fixtureServingAnalysis, evCapacityBack, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/capacityDropped", fixtureServingAnalysis, evCapacityDropped, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/replicasUp", fixtureServingAnalysis, evReplicasUp, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/readyTimeout", fixtureServingAnalysis, evDipThenReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving[A]/pauseElapsed", fixtureServingAnalysis, evBakeElapsedNoSample, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/sample[pass]", fixtureServingAnalysis, evSample(analysis.Pass, 2*time.Minute), cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/sample[passBaked]", fixtureServingAnalysis, evSample(analysis.Pass, 2*time.Hour), cellWant{state: staging, phase: v1beta1.RolloutPhasePending, step: 1, active: true}},
		{"canary/Serving[A]/sample[fail]", fixtureServingAnalysis, evSample(analysis.Fail, 2*time.Minute), cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/sample[failAtLimit]", fixtureServingAnalysis, evSampleAtLimit, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Serving[A]/sample[failAtLimitStableEmpty]", fixtureServingAnalysis, evSampleAtLimitStableEmpty, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving[A]/sample[inconclusive]", fixtureServingAnalysis, evSample(analysis.Inconclusive, 2*time.Minute), cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/sample[inconclusiveRollbackPolicy]", fixtureServingAnalysisRollback, evSample(analysis.Inconclusive, 2*time.Minute), cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Serving[A]/sample[stalled]", fixtureServingAnalysis, evSample(analysis.Inconclusive, 20*time.Minute), cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving[A]/sample[stalledRollbackOnStall]", fixtureServingAnalysisRollbackOnStall, evSample(analysis.Inconclusive, 20*time.Minute), cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Serving[A]/unavailable[noSampler]", fixtureServingAnalysis, evNoSampler, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/unavailable[sourceError]", fixtureServingAnalysis, evSourceError, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/unavailable[noFreshSample]", fixtureServingAnalysis, evNoFreshSample, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/canaryCrashed", fixtureServingAnalysis, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving[A]/canaryRestarted", fixtureServingAnalysis, evCanaryRestarted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/canaryPodDeleted", fixtureServingAnalysis, evCanaryPodDeleted, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/record[noTargetID]", fixtureServingAnalysis, evRecordNoTargetID, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/record[stableEqualsCanary]", fixtureServingAnalysis, evRecordStableEqualsCanary, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/record[noStable]", fixtureServingAnalysis, evRecordNoStable, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/record[negativeStep]", fixtureServingAnalysis, evRecordNegativeStep, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/Serving[A]/record[failedPhaseNoMarker]", fixtureServingAnalysis, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Serving[A]/resync", fixtureServingAnalysis, evNothing, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true}},
		// --- PreHold: the traffic raise waits for a promote naming the canary
		// revision. The release spends the verb, so a copy re-shown afterwards
		// is inert; another value, a dip or a lost run leave the hold standing.
		{"canary/PreHold/unpause", fixturePreHold, evUnpause, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/promote[reshown]", fixturePreHold, evPromoteReshown, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/PreHold/promote[stale]", fixturePreHold, evPromoteStale, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/promote[applied]", fixturePreHoldApplied, evPromoteMatch, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/PreHold/promoteForce[reshown]", fixturePreHold, evForceReshown, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/PreHold/promoteForce[stale]", fixturePreHold, evForceStale, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/promoteForce[applied]", fixturePreHoldApplied, evForceMatch, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/PreHold/rollback[reshown]", fixturePreHold, evRollbackReshown, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/PreHold/resume[stale]", fixturePreHold, evResumeStale, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/PreHold/resume[otherUnit]", fixturePreHold, evResumeOtherUnit, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/planDrift", fixturePreHold, evPlanDrift, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/runLost", fixturePreHold, evRunLost, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/runAdopted", fixturePreHold, evRunAdopted, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/newTarget[runOpen]", fixturePreHold, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/PreHold/capacityReached", fixturePreHold, evCapacityBack, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/replicasUp", fixturePreHold, evReplicasUp, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/readyTimeout", fixturePreHold, evDipThenReadyTimeout, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/PreHold/canaryRestarted", fixturePreHold, evCanaryRestarted, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/record[noTargetID]", fixturePreHold, evRecordNoTargetID, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/record[stableEqualsCanary]", fixturePreHold, evRecordStableEqualsCanary, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/record[noStable]", fixturePreHold, evRecordNoStable, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/record[negativeStep]", fixturePreHold, evRecordNegativeStep, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		{"canary/PreHold/record[failedPhaseNoMarker]", fixturePreHold, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/PreHold/resync", fixturePreHold, evNothing, cellWant{state: preHold, phase: v1beta1.RolloutPhasePaused, active: true}},
		// --- Draining: the ungated final step at 100% inside its window. A
		// promote has no gate to open and stays live; an applied copy is taken;
		// a lost run, a spec edit, a restart or a replica change leave the
		// drain running, and a deleted pod is a dip inside it.
		{"canary/Draining/unpause", fixtureDraining, evUnpause, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/promote", fixtureDraining, evPromoteMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/promote[stale]", fixtureDraining, evPromoteStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/promote[applied]", fixtureDrainingApplied, evPromoteMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Draining/promoteForce[stale]", fixtureDraining, evForceStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/promoteForce[applied]", fixtureDrainingApplied, evForceMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Draining/rollback[reshown]", fixtureDraining, evRollbackReshown, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining/rollback[stableEmpty]", fixtureDraining, evDrainingRollbackStableEmpty, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining/resume[stale]", fixtureDraining, evResumeStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining/resume[otherUnit]", fixtureDraining, evResumeOtherUnit, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/planDrift", fixtureDraining, evPlanDrift, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/runLost", fixtureDraining, evRunLost, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/runAdopted", fixtureDraining, evRunAdopted, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/newTarget[runOpen]", fixtureDraining, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining/replicasUp", fixtureDraining, evReplicasUp, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/canaryRestarted", fixtureDraining, evCanaryRestarted, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/canaryPodDeleted", fixtureDraining, evDrainingCanaryPodDeleted, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/record[noTargetID]", fixtureDraining, evRecordNoTargetID, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/record[stableEqualsCanary]", fixtureDraining, evRecordStableEqualsCanary, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/record[noStable]", fixtureDraining, evRecordNoStable, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining/record[failedPhaseNoMarker]", fixtureDraining, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// --- Draining/Held: 100% behind a final gate. The gate opens on a
		// promote, an elapsed soak or a baked pass and the drain follows; a
		// window elapsing, a dip, a restart or a lost run leave the gate holding.
		{"canary/Draining[M]/pause", fixtureDrainingHeld, evPause, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/unpause", fixtureDrainingHeld, evUnpause, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/promote[reshown]", fixtureDrainingHeld, evPromoteReshown, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Draining[M]/promote[stale]", fixtureDrainingHeld, evPromoteStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/promote[applied]", fixtureDrainingHeldApplied, evPromoteMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Draining[M]/promoteForce[reshown]", fixtureDrainingHeld, evForceReshown, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Draining[M]/promoteForce[stale]", fixtureDrainingHeld, evForceStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/promoteForce[applied]", fixtureDrainingHeldApplied, evForceMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Draining[M]/rollback", fixtureDrainingHeld, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[M]/rollback[reshown]", fixtureDrainingHeld, evRollbackReshown, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[M]/rollback[stableEmpty]", fixtureDrainingHeld, evDrainingRollbackStableEmpty, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[M]/resume", fixtureDrainingHeld, evResumeOwn, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining[M]/resume[stale]", fixtureDrainingHeld, evResumeStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining[M]/resume[otherUnit]", fixtureDrainingHeld, evResumeOtherUnit, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/planDrift", fixtureDrainingHeld, evPlanDrift, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/runLost", fixtureDrainingHeld, evRunLost, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/runAdopted", fixtureDrainingHeld, evRunAdopted, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/newTarget", fixtureDrainingHeld, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining[M]/newTarget[runOpen]", fixtureDrainingHeld, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining[M]/capacityDropped", fixtureDrainingHeld, evDrainingCapacityDropped, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/replicasUp", fixtureDrainingHeld, evReplicasUp, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[T]/pauseElapsed", fixtureDrainingHeldTimed, evClockElapsed, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[T]/pauseElapsed[restartInSoak]", fixtureDrainingHeldTimed, evRestartInSoak, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[T]/pauseElapsed[restartSoaked]", fixtureDrainingHeldTimed, evRestartSoaked, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[M]/drainElapsed", fixtureDrainingHeld, evClockElapsed, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[A]/sample[pass]", fixtureDrainingHeldAnalysis, evSample(analysis.Pass, 2*time.Minute), cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[A]/sample[passBaked]", fixtureDrainingHeldAnalysis, evSample(analysis.Pass, 2*time.Hour), cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[A]/sample[fail]", fixtureDrainingHeldAnalysis, evSample(analysis.Fail, 2*time.Minute), cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[A]/sample[failAtLimit]", fixtureDrainingHeldAnalysis, evSampleAtLimit, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[A]/sample[failAtLimitStableEmpty]", fixtureDrainingHeldAnalysis, evSampleAtLimitStableEmptyDraining, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Draining[A]/sample[inconclusive]", fixtureDrainingHeldAnalysis, evSample(analysis.Inconclusive, 2*time.Minute), cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[A]/sample[inconclusiveRollbackPolicy]", fixtureDrainingHeldAnalysisRollback, evSample(analysis.Inconclusive, 2*time.Minute), cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[A]/sample[stalled]", fixtureDrainingHeldAnalysis, evSample(analysis.Inconclusive, 20*time.Minute), cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Draining[A]/sample[stalledRollbackOnStall]", fixtureDrainingHeldAnalysisRollbackOnStall, evSample(analysis.Inconclusive, 20*time.Minute), cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[A]/unavailable[noSampler]", fixtureDrainingHeldAnalysis, evNoSampler, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[A]/unavailable[sourceError]", fixtureDrainingHeldAnalysis, evSourceError, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[A]/unavailable[noFreshSample]", fixtureDrainingHeldAnalysis, evNoFreshSample, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/canaryCrashed", fixtureDrainingHeld, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Draining[M]/canaryRestarted", fixtureDrainingHeld, evCanaryRestarted, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/canaryPodDeleted", fixtureDrainingHeld, evDrainingCanaryPodDeleted, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/record[noTargetID]", fixtureDrainingHeld, evRecordNoTargetID, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/record[stableEqualsCanary]", fixtureDrainingHeld, evRecordStableEqualsCanary, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/record[noStable]", fixtureDrainingHeld, evRecordNoStable, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 1, active: true}},
		{"canary/Draining[M]/record[failedPhaseNoMarker]", fixtureDrainingHeld, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// --- Draining/ShortInWindow: a canary pod lost inside the window. The
		// drain runs on under every verb that opens no gate; the release is not
		// read before the window elapses.
		{"canary/Draining[capacityShort,inWindow]/pause", fixtureDrainingShortInWindow, evPause, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/unpause", fixtureDrainingShortInWindow, evUnpause, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/promote", fixtureDrainingShortInWindow, evPromoteMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/promote[stale]", fixtureDrainingShortInWindow, evPromoteStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/promote[applied]", fixtureDrainingShortInWindowApplied, evPromoteMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Draining[capacityShort,inWindow]/promoteForce", fixtureDrainingShortInWindow, evForceMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/promoteForce[stale]", fixtureDrainingShortInWindow, evForceStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/promoteForce[applied]", fixtureDrainingShortInWindowApplied, evForceMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Draining[capacityShort,inWindow]/rollback", fixtureDrainingShortInWindow, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[capacityShort,inWindow]/rollback[reshown]", fixtureDrainingShortInWindow, evRollbackReshown, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[capacityShort,inWindow]/rollback[stableEmpty]", fixtureDrainingShortInWindow, evDrainingRollbackStableEmpty, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[capacityShort,inWindow]/resume", fixtureDrainingShortInWindow, evResumeOwn, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining[capacityShort,inWindow]/resume[stale]", fixtureDrainingShortInWindow, evResumeStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining[capacityShort,inWindow]/resume[otherUnit]", fixtureDrainingShortInWindow, evResumeOtherUnit, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/planDrift", fixtureDrainingShortInWindow, evPlanDrift, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/runLost", fixtureDrainingShortInWindow, evRunLost, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/runAdopted", fixtureDrainingShortInWindow, evRunAdopted, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/newTarget", fixtureDrainingShortInWindow, evNewTarget, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining[capacityShort,inWindow]/newTarget[runOpen]", fixtureDrainingShortInWindow, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining[capacityShort,inWindow]/replicasUp", fixtureDrainingShortInWindow, evReplicasUp, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/canaryCrashed", fixtureDrainingShortInWindow, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/canaryRestarted", fixtureDrainingShortInWindow, evCanaryRestarted, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/canaryPodDeleted", fixtureDrainingShortInWindow, evDrainingCanaryPodDeletedDeeper, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/record[noTargetID]", fixtureDrainingShortInWindow, evRecordNoTargetID, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/record[stableEqualsCanary]", fixtureDrainingShortInWindow, evRecordStableEqualsCanary, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/record[noStable]", fixtureDrainingShortInWindow, evRecordNoStable, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort,inWindow]/record[failedPhaseNoMarker]", fixtureDrainingShortInWindow, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// --- Draining/ShortElapsed: the window elapsed with a canary pod
		// missing; the release waits for the capacity with the stable instance
		// held, under the same verbs.
		{"canary/Draining[capacityShort]/unpause", fixtureDrainingShort, evUnpause, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/promote[stale]", fixtureDrainingShort, evPromoteStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/promote[applied]", fixtureDrainingShortApplied, evPromoteMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Draining[capacityShort]/promoteForce[stale]", fixtureDrainingShort, evForceStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/promoteForce[applied]", fixtureDrainingShortApplied, evForceMatch, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Draining[capacityShort]/rollback[reshown]", fixtureDrainingShort, evRollbackReshown, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[capacityShort]/rollback[stableEmpty]", fixtureDrainingShort, evDrainingRollbackStableEmpty, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/Draining[capacityShort]/resume[stale]", fixtureDrainingShort, evResumeStale, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Draining[capacityShort]/resume[otherUnit]", fixtureDrainingShort, evResumeOtherUnit, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/planDrift", fixtureDrainingShort, evPlanDrift, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/runAdopted", fixtureDrainingShort, evRunAdopted, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/newTarget[runOpen]", fixtureDrainingShort, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Draining[capacityShort]/replicasUp", fixtureDrainingShort, evReplicasUp, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/canaryRestarted", fixtureDrainingShort, evCanaryRestarted, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/canaryPodDeleted", fixtureDrainingShort, evDrainingCanaryPodDeletedDeeper, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/record[noTargetID]", fixtureDrainingShort, evRecordNoTargetID, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/record[stableEqualsCanary]", fixtureDrainingShort, evRecordStableEqualsCanary, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/record[noStable]", fixtureDrainingShort, evRecordNoStable, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 1, active: true}},
		{"canary/Draining[capacityShort]/record[failedPhaseNoMarker]", fixtureDrainingShort, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		// --- Done/Rolling: the sentinel with the released instance rolling. A
		// done canary evaluates no gate, so verbs are left in place or consumed
		// with nothing to act on; the applied promote's copy is taken; the
		// cutover finishes with or without a run and waits out every dip.
		{"canary/Done[rolling]/unpause", fixtureDoneRolling, evUnpause, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/promote", fixtureDoneRolling, evPromoteMatch, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/promote[stale]", fixtureDoneRolling, evPromoteStale, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/promote[applied]", fixtureDoneRollingApplied, evPromoteMatch, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Done[rolling]/promoteForce[stale]", fixtureDoneRolling, evForceStale, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/promoteForce[applied]", fixtureDoneRollingApplied, evForceMatch, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Done[rolling]/resume", fixtureDoneRolling, evResumeOwn, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Done[rolling]/resume[stale]", fixtureDoneRolling, evResumeStale, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Done[rolling]/resume[otherUnit]", fixtureDoneRolling, evResumeOtherUnit, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/planDrift", fixtureDoneRolling, evPlanDrift, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/runLost", fixtureDoneRolling, evRunLost, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/newTarget[runOpen]", fixtureDoneRolling, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Done[rolling]/capacityDropped", fixtureDoneRolling, evDrainingCapacityDropped, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/replicasUp", fixtureDoneRolling, evReplicasUp, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/canaryRestarted", fixtureDoneRolling, evCanaryRestarted, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/canaryPodDeleted", fixtureDoneRolling, evDrainingCanaryPodDeleted, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/record[noTargetID]", fixtureDoneRolling, evRecordNoTargetID, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/record[stableEqualsCanary]", fixtureDoneRolling, evRecordStableEqualsCanary, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 2, active: true}},
		{"canary/Done[rolling]/record[noStable]", fixtureDoneRolling, evRecordNoStable, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done[rolling]/record[failedPhaseNoMarker]", fixtureDoneRolling, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 2, active: true}},
		// --- Done: the completed cutover reads Stable and is inactive under
		// everything but a retarget; the applied promote's copy is still taken.
		{"canary/Done/unpause", fixtureDone, evUnpause, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/promote[stale]", fixtureDone, evPromoteStale, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/promote[applied]", fixtureDoneApplied, evPromoteMatch, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, consume: []string{constants.RolloutPromoteAnnotation}}},
		{"canary/Done/promoteForce[stale]", fixtureDone, evForceStale, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/promoteForce[applied]", fixtureDoneApplied, evForceMatch, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, consume: []string{constants.RolloutPromoteForceAnnotation}}},
		{"canary/Done/resume[otherUnit]", fixtureDone, evResumeOtherUnit, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/planDrift", fixtureDone, evPlanDrift, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/runLost", fixtureDone, evRunLost, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/newTarget[runOpen]", fixtureDone, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Done/replicasUp", fixtureDone, evReplicasUp, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/canaryRestarted", fixtureDone, evCanaryRestarted, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/canaryPodDeleted", fixtureDone, evDoneCanaryPodDeleted, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/record[noTargetID]", fixtureDone, evRecordNoTargetID, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/record[noStable]", fixtureDone, evRecordNoStable, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2}},
		{"canary/Done/record[failedPhaseNoMarker]", fixtureDone, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 2, active: true}},
		// --- RollingBack: the revert drains. The hold reads no promote and
		// consumes no copy, a further request is inert, and the record is
		// adopted or repaired in place while the rejected pods leave.
		{"canary/RollingBack/unpause", fixtureRollingBack, evUnpause, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/promote", fixtureRollingBack, evPromoteMatch, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/promote[stale]", fixtureRollingBack, evPromoteStale, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/promote[applied]", fixtureRollingBackApplied, evPromoteMatch, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/RollingBack/promoteForce[stale]", fixtureRollingBack, evForceStale, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/promoteForce[applied]", fixtureRollingBackApplied, evForceMatch, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, step: 1, active: true, rolledBk: true}},
		{"canary/RollingBack/rollback", fixtureRollingBack, evRollback, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/resume[stale]", fixtureRollingBack, evResumeStale, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/RollingBack/resume[otherUnit]", fixtureRollingBack, evResumeOtherUnit, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/planDrift", fixtureRollingBack, evPlanDrift, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/runAdopted", fixtureRollingBack, evRunAdopted, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/replicasUp", fixtureRollingBack, evReplicasUp, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/canaryRestarted", fixtureRollingBack, evCanaryRestarted, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/record[noTargetID]", fixtureRollingBack, evRecordNoTargetID, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/record[stableEqualsCanary]", fixtureRollingBack, evRecordStableEqualsCanary, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/record[noStable]", fixtureRollingBack, evRecordNoStable, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/record[negativeStep]", fixtureRollingBack, evRecordNegativeStep, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/record[failedPhaseNoMarker]", fixtureRollingBack, evPhaseFailedNoMarker, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		// --- RolledBack: the settled hold. Nothing is read but a resume, a new
		// target or a run-open retarget; the record is adopted or repaired in
		// place and a lost phase is re-projected from the revert.
		{"canary/RolledBack/unpause", fixtureRolledBack, evUnpause, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/promote[stale]", fixtureRolledBack, evPromoteStale, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/promote[applied]", fixtureRolledBackApplied, evPromoteMatch, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, step: 1, active: true, rolledBk: true}},
		{"canary/RolledBack/promoteForce[stale]", fixtureRolledBack, evForceStale, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/promoteForce[applied]", fixtureRolledBackApplied, evForceMatch, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, step: 1, active: true, rolledBk: true}},
		{"canary/RolledBack/resume[otherUnit]", fixtureRolledBack, evResumeOtherUnit, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/planDrift", fixtureRolledBack, evPlanDrift, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/replicasUp", fixtureRolledBack, evReplicasUp, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/record[noTargetID]", fixtureRolledBack, evRecordNoTargetID, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/record[stableEqualsCanary]", fixtureRolledBack, evRecordStableEqualsCanary, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/record[noStable]", fixtureRolledBack, evRecordNoStable, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/record[negativeStep]", fixtureRolledBack, evRecordNegativeStep, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/record[failedPhaseNoMarker]", fixtureRolledBack, evPhaseFailedNoMarker, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		// --- Failed: the park. Verbs with nothing to act on are left in place,
		// a rollback proceeds from the park with or without a run, a resume
		// without a run is rejected, and no count, dip, restart or record
		// repair moves the park.
		{"canary/Failed/unpause", fixtureFailed, evUnpause, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/promote[stale]", fixtureFailed, evPromoteStale, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/promote[applied]", fixtureFailedApplied, evPromoteMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Failed/promoteForce[stale]", fixtureFailed, evForceStale, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/promoteForce[applied]", fixtureFailedApplied, evForceMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Failed/rollback[reshown]", fixtureFailed, evRollbackReshown, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/rollback[stableEmpty]", fixtureFailed, evRollbackStableEmpty, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/Failed/rollback[runLost]", fixtureFailed, evRollbackRunLost, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/resume[runLost]", fixtureFailed, evResumeOwnRunLost, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed/resume[otherUnit]", fixtureFailed, evResumeOtherUnit, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/planDrift", fixtureFailed, evPlanDrift, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/runLost", fixtureFailed, evRunLost, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/runAdopted", fixtureFailed, evRunAdopted, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/newTarget[runOpen]", fixtureFailed, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Failed/capacityDropped", fixtureFailed, evCapacityDropped, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/replicasUp", fixtureFailed, evReplicasUp, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/canaryRestarted", fixtureFailed, evCanaryRestarted, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/canaryPodDeleted", fixtureFailed, evCanaryPodDeleted, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/record[noTargetID]", fixtureFailed, evRecordNoTargetID, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/record[stableEqualsCanary]", fixtureFailed, evRecordStableEqualsCanary, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/record[noStable]", fixtureFailed, evRecordNoStable, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/record[negativeStep]", fixtureFailed, evRecordNegativeStep, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/record[failedPhaseNoMarker]", fixtureFailed, evPhaseFailedNoMarker, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// --- Failed/StableMissing: the park that carries a rejection it cannot
		// carry out. The pause echoes the rejection; everything else leaves the
		// park standing, and a resume without a run is rejected.
		{"canary/Failed[stableMissing]/pause", fixtureFailedStableMissing, evPause, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, rolledBk: true}},
		{"canary/Failed[stableMissing]/unpause", fixtureFailedStableMissing, evUnpause, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/promote", fixtureFailedStableMissing, evPromoteMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/promote[stale]", fixtureFailedStableMissing, evPromoteStale, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/promote[applied]", fixtureFailedStableMissingApplied, evPromoteMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Failed[stableMissing]/promoteForce[stale]", fixtureFailedStableMissing, evForceStale, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/promoteForce[applied]", fixtureFailedStableMissingApplied, evForceMatch, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, step: 1, active: true}},
		{"canary/Failed[stableMissing]/resume[stale]", fixtureFailedStableMissing, evResumeStale, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed[stableMissing]/resume[runLost]", fixtureFailedStableMissing, evResumeOwnRunLost, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed[stableMissing]/resume[otherUnit]", fixtureFailedStableMissing, evResumeOtherUnit, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/planDrift", fixtureFailedStableMissing, evPlanDrift, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/runLost", fixtureFailedStableMissing, evRunLost, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/runAdopted", fixtureFailedStableMissing, evRunAdopted, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/newTarget[runOpen]", fixtureFailedStableMissing, evNewTargetAtRunOpen, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true}},
		{"canary/Failed[stableMissing]/capacityReached", fixtureFailedStableMissing, evCapacityReached, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/capacityDropped", fixtureFailedStableMissing, evCapacityDropped, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/replicasUp", fixtureFailedStableMissing, evReplicasUp, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/canaryCrashed", fixtureFailedStableMissing, evCanaryCrashed, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/canaryRestarted", fixtureFailedStableMissing, evCanaryRestarted, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/canaryPodDeleted", fixtureFailedStableMissing, evCanaryPodDeleted, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/record[noTargetID]", fixtureFailedStableMissing, evRecordNoTargetID, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/record[stableEqualsCanary]", fixtureFailedStableMissing, evRecordStableEqualsCanary, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/record[noStable]", fixtureFailedStableMissing, evRecordNoStable, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/record[negativeStep]", fixtureFailedStableMissing, evRecordNegativeStep, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		// --- Incomplete record shapes and repins the fixtures above do not drive:
		// a negative step on a record past step 0 is clamped to 0, which
		// re-runs step 0 as a live step; an equal pair at the sentinel is
		// repaired and the cutover re-recorded; a phase-only Failed over a
		// recorded rejection reads as the revert; a repin clamps the index
		// under every hold without moving it, and done stays done.
		{"canary/Draining/record[negativeStep]", fixtureDraining, evRecordNegativeStep, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 0, active: true}},
		{"canary/Draining/repin[holdOrTighten]", fixtureDraining, evRepinShorter, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 0, active: true}},
		{"canary/Draining[M]/record[negativeStep]", fixtureDrainingHeld, evRecordNegativeStep, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 0, active: true}},
		{"canary/Draining[M]/repin[holdOrTighten]", fixtureDrainingHeld, evRepinShorterHeld, cellWant{state: draining, phase: v1beta1.RolloutPhasePaused, step: 0, active: true}},
		{"canary/Draining[capacityShort,inWindow]/record[negativeStep]", fixtureDrainingShortInWindow, evRecordNegativeStep, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 0, active: true}},
		{"canary/Draining[capacityShort,inWindow]/repin[holdOrTighten]", fixtureDrainingShortInWindow, evRepinShorter, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 0, active: true}},
		{"canary/Draining[capacityShort]/record[negativeStep]", fixtureDrainingShort, evRecordNegativeStep, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 0, active: true}},
		{"canary/Draining[capacityShort]/repin[holdOrTighten]", fixtureDrainingShort, evRepinShorter, cellWant{state: draining, phase: v1beta1.RolloutPhasePromoting, step: 0, active: true}},
		{"canary/Done[rolling]/record[negativeStep]", fixtureDoneRolling, evRecordNegativeStep, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 0, active: true}},
		{"canary/Done[rolling]/repin[pastDone]", fixtureDoneRolling, evRepinPastDone, cellWant{state: done, phase: v1beta1.RolloutPhasePromoting, step: 3, active: true}},
		{"canary/Done/record[negativeStep]", fixtureDone, evRecordNegativeStep, cellWant{state: serving, phase: v1beta1.RolloutPhasePaused, step: 0, active: true}},
		{"canary/Done/record[stableEqualsCanary]", fixtureDone, evRecordStableEqualsCanary, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 2, active: true, complete: true}},
		{"canary/Done/repin[pastDone]", fixtureDone, evRepinPastDone, cellWant{state: done, phase: v1beta1.RolloutPhaseStable, step: 3}},
		{"canary/RollingBack/repin[holdOrTighten]", fixtureRollingBack, evRepinLonger, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RollingBack/repin[raise]", fixtureRollingBack, evRepinRaise, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		{"canary/RolledBack/repin[holdOrTighten]", fixtureRolledBack, evRepinLonger, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/RolledBack/repin[raise]", fixtureRolledBack, evRepinRaise, cellWant{state: rolledBack, phase: v1beta1.RolloutPhaseRolledBack, active: true, rolledBk: true}},
		{"canary/Failed/repin[holdOrTighten]", fixtureFailed, evRepinLonger, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed/repin[raise]", fixtureFailed, evRepinRaise, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/repin[holdOrTighten]", fixtureFailedStableMissing, evRepinLonger, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/repin[raise]", fixtureFailedStableMissing, evRepinRaise, cellWant{state: failed, phase: v1beta1.RolloutPhaseFailed, active: true}},
		{"canary/Failed[stableMissing]/record[failedPhaseNoMarker]", fixtureFailedStableMissing, evPhaseFailedNoMarker, cellWant{state: rollingBack, phase: v1beta1.RolloutPhaseRollingBack, active: true, rolledBk: true}},
		// A resume leaves no record in status: a copy re-shown after the
		// re-armed ladder parks again re-arms it a second time.
		{"canary/Failed/resume[reshown]", fixtureFailed, evResumeReshownAcrossSecondPark, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/Failed[stableMissing]/resume[reshown]", fixtureFailedStableMissing, evResumeReshownAcrossSecondPark, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
		{"canary/RolledBack/resume[reshown]", fixtureRolledBack, evResumeReshownAcrossSecondPark, cellWant{state: staging, phase: v1beta1.RolloutPhasePending, active: true, consume: []string{constants.RolloutResumeAnnotation}}},
	}
	for _, c := range cells {
		t.Run(c.id, func(t *testing.T) {
			isvc, in := c.fixture(t)
			c.event(isvc, &in)
			res := mustReconcile(t, isvc, in)
			cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
			steps := len(isvc.Spec.Rollout.Groups[0].Canary.Steps)
			if got := rollout.StateOf(cs, phaseOf(isvc), steps); got != c.want.state {
				t.Errorf("state %q, want %q", got, c.want.state)
			}
			if c.want.phase != "" && phaseOf(isvc) != c.want.phase {
				t.Errorf("phase %q, want %q", phaseOf(isvc), c.want.phase)
			}
			if cs != nil && cs.CurrentStep != c.want.step {
				t.Errorf("step %d, want %d", cs.CurrentStep, c.want.step)
			}
			if len(c.want.consume) > 0 || len(res.Consume) > 0 {
				got := append([]string(nil), res.Consume...)
				if len(got) != len(c.want.consume) {
					t.Errorf("consume %v, want %v", got, c.want.consume)
				} else {
					for i := range got {
						if got[i] != c.want.consume[i] {
							t.Errorf("consume %v, want %v", got, c.want.consume)
							break
						}
					}
				}
			}
			if res.Active != c.want.active || res.RolledBack != c.want.rolledBk || res.Complete != c.want.complete {
				t.Errorf("result active=%v rolledBack=%v complete=%v, want active=%v rolledBack=%v complete=%v",
					res.Active, res.RolledBack, res.Complete, c.want.active, c.want.rolledBk, c.want.complete)
			}
			// Whatever the cell decided, every weighted target it leaves behind
			// names a revision with a serving pod in the pass's own view.
			expectTrafficServes(t, isvc, v1beta1.EngineComponent, in.PerRevisionPods)
			// RollingBack is a wait on the drain: the pass must ask to come back.
			if c.want.state == rollingBack && res.RequeueAfter == 0 && !res.Requeue {
				t.Errorf("RollingBack must requeue, got %+v", res)
			}
			// A released cutover still rolling is a wait on the workload, which moves no
			// annotation and no step: the pass must ask to come back, at partition 0.
			if c.want.state == done && c.want.active && !c.want.complete {
				if res.RequeueAfter == 0 && !res.Requeue {
					t.Errorf("a rolling Done must requeue, got %+v", res)
				}
				if res.Partition != 0 {
					t.Errorf("a rolling Done projects partition 0, got %d", res.Partition)
				}
			}
		})
	}
}
