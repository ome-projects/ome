package canary

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A metric gate that cannot read health stops the unit once the stall has
// outlived the ready timeout. Where the stop lands decides the traffic. At
// the final step the drain had shifted everything onto the canary revision
// and kept one stable Instance of each member for exactly this case, so the
// stop hands the traffic back to it. At an earlier step the stable side
// still carries its share, so the stop leaves the step's split as it stands.

func init() {
	for id, reader := range map[string]func(t *testing.T, isvc *v1beta1.InferenceService){
		"canary/Draining[A]/sample[stalled]": splitReturnedToStable,
		"canary/Serving[A]/sample[stalled]":  splitStandsAtHalf,
	} {
		cellAlso[id] = reader
	}
}

// stallGate is a metric gate sampled every minute with a failure limit of
// three, under the default policy for a sample that cannot be read.
func stallGate() *v1beta1.RolloutAnalysis {
	return &v1beta1.RolloutAnalysis{
		Interval:     metav1.Duration{Duration: time.Minute},
		FailureLimit: 3,
		Metrics:      []v1beta1.AnalysisMetric{{Name: "err", Query: "q", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}},
	}
}

// twoOfEachUnit is two engines beside two decoders in one canary group, the
// run recorded toward new/decnew over old/decold, one Instance of each
// member serving the canary revision beside one on the stable revision,
// under a ten-minute ready timeout.
func twoOfEachUnit(steps []v1beta1.RolloutGroupStep) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := pdUnit(steps, nil, map[string]int32{"new": 1, "old": 1})
	in.DesiredReplicas = 2
	in.DefaultReadyTimeout = 10 * time.Minute
	decoderServes(&in, 1, 1, true)
	m := in.Secondaries[v1beta1.DecoderComponent]
	m.DesiredReplicas = 2
	in.Secondaries[v1beta1.DecoderComponent] = m
	return isvc, in
}

// sampleInconclusive dates one sample the gate cannot read d after the pass
// before.
func sampleInconclusive(in *ReconcileInputs, d time.Duration) {
	in.Now = in.Now.Add(d)
	withSampler(analysis.Inconclusive)(in)
}

// expectStoppedOnStall reads the stop a stall records: Failed with the
// reason AnalysisStalled at step, every Instance of the primary held where
// it stands.
func expectStoppedOnStall(t *testing.T, isvc *v1beta1.InferenceService, res *Result, step int32) {
	t.Helper()
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	state := rollout.StateOf(cs, phaseOf(isvc), len(isvc.Spec.Rollout.Groups[0].Canary.Steps))
	if state != rollout.CanaryStateFailed || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureAnalysisStalled {
		t.Fatalf("a stall past the ready timeout stops the unit Failed with AnalysisStalled, got state %q failed %+v", state, cs.Failed)
	}
	if cs.CurrentStep != step {
		t.Fatalf("the stop keeps the step the stall landed on: step %d, want %d", cs.CurrentStep, step)
	}
	if res.Partition != 2 {
		t.Fatalf("the stop holds every Instance where it stands: partition %d, want 2", res.Partition)
	}
}

// splitAtTenBothMembers reads the first step's split standing on both
// members: 10 to the canary revision, 90 to the stable one.
func splitAtTenBothMembers(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 10, protocol: pdCanaryProtocol, latest: true},
		"old": {percent: 90, protocol: pdStableProtocol},
	})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 10, protocol: pdCanaryProtocol, latest: true},
		"decold": {percent: 90, protocol: pdStableProtocol},
	})
}

// splitReturnedToStable reads a one-member unit's traffic back on its stable
// revision alone after a stop from the drain.
func splitReturnedToStable(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
}

// Two engines and two decoders under a 10 percent first step held for a
// promote and a final step behind the metric gate. The promote shifts 100
// percent onto version 2 on both members and the gate keeps the one version
// 1 Instance of each. Every sample reads inconclusive. The one that lands
// eleven minutes after the shift stops the unit at the final step, and the
// stop hands both members' traffic back to their kept version 1 Instance.
func TestReconcile_StallAtTheFinalStepReturnsTrafficToTheKeptStableInstances(t *testing.T) {
	isvc, in := twoOfEachUnit([]v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 5 * time.Minute}}, Analysis: stallGate()},
	})
	mustReconcile(t, isvc, in) // the first split serves at 10 percent under the manual hold
	isvc.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "new"}
	mustReconcile(t, isvc, in) // the promote advances to the final step
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	withSampler(analysis.Inconclusive)(&in)
	mustReconcile(t, isvc, in) // 100 percent shifts and the gate holds on an unreadable sample
	sampleInconclusive(&in, time.Minute)
	mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("an unreadable sample inside the ready timeout holds the gate, got phase %q", phaseOf(isvc))
	}
	splitAllOnCanaryBothMembers(t, isvc)

	sampleInconclusive(&in, 10*time.Minute)
	res := mustReconcile(t, isvc, in)
	expectStoppedOnStall(t, isvc, res, 1)
	splitRestsOnStable(t, isvc)
	if w := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).ObservedTrafficWeight; w != 0 {
		t.Fatalf("the record carries the returned split: weight %d, want 0", w)
	}
}

// The same two engines and two decoders with the metric gate on the first
// step, serving 10 percent. Every sample reads inconclusive. The one that
// lands eleven minutes after the split first served stops the unit at step
// 0, where the stable side still carries 90 percent, so the stop leaves the
// step's split as it stands on both members.
func TestReconcile_StallAtAnEarlierStepKeepsTheStepSplit(t *testing.T) {
	isvc, in := twoOfEachUnit([]v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 10, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 5 * time.Minute}}, Analysis: stallGate()},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	})
	withSampler(analysis.Inconclusive)(&in)
	mustReconcile(t, isvc, in) // the split serves at 10 percent and the gate holds on an unreadable sample
	sampleInconclusive(&in, time.Minute)
	mustReconcile(t, isvc, in)
	if phaseOf(isvc) != v1beta1.RolloutPhasePaused {
		t.Fatalf("an unreadable sample inside the ready timeout holds the gate, got phase %q", phaseOf(isvc))
	}
	splitAtTenBothMembers(t, isvc)

	sampleInconclusive(&in, 10*time.Minute)
	res := mustReconcile(t, isvc, in)
	expectStoppedOnStall(t, isvc, res, 0)
	splitAtTenBothMembers(t, isvc)
	if w := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).ObservedTrafficWeight; w != 10 {
		t.Fatalf("the record keeps the step's weight: weight %d, want 10", w)
	}
}
