package canary

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The fixtures, events and readers of the cases about where a resumed
// unit continues, on the reference prefill/decode shape: 24 engines beside
// 8 decoders under a 10/30/60/100 canary with a manual gate on every step
// but the last. A unit stopped Failed and resumed continues at the step the
// stop interrupted, or at the first later step whose staged count covers the
// Instances each member already runs on the canary revision. The split
// stands as the stop left it until the resumed step writes its own, and the
// capacity wait is clocked again by the first short pass after the resume.
// A stop that holds a rejection starts its steps again at step 0, raised
// only to cover Instances still on the rejected revision.

func init() {
	for id, reader := range map[string]func(t *testing.T, isvc *v1beta1.InferenceService){
		"canary/Failed[stableMissing,final]/resume[nextPass]": readers(splitAllOnStable, firstStepStagesTwoOfFour),
		"canary/Failed[pd24,stepTwo]/resume":                  readers(splitOnBothMembers(30), recordedWeight(30), waitCleared),
		"canary/Failed[pd24,stepTwo]/resume[nextPass]":        readers(splitOnBothMembers(60), recordedWeight(60)),
		"canary/Failed[pd24,stepTwo,short]/resume":            readers(splitOnBothMembers(30), recordedWeight(30), waitCleared),
		"canary/Failed[pd24,stepTwo,short]/resume[nextPass]":  readers(splitOnBothMembers(30), recordedWeight(30), waitClocked),
		"canary/Failed[pd24,scaledUp]/resume":                 readers(splitOnBothMembers(10), recordedWeight(10), waitCleared),
		"canary/Failed[pd24,scaledUp]/resume[nextPass]":       readers(splitOnBothMembers(10), recordedWeight(10), waitClocked),
	} {
		cellAlso[id] = reader
	}
}

// pd24FinalStep is the reference shape's last step: 100 percent with no gate.
func pd24FinalStep() v1beta1.RolloutGroupStep {
	return v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
}

// pd24StoppedAtStepTwo walks the unit to its third step staging short (12
// engines and 4 decoders of the 15 and 5 it stages serve the canary
// revision, the split still 30 to it and 70 to the stable revision), pauses
// the rollout past the ready timeout, ends the pause, and lets a whole ready
// timeout pass after it: the unit is stopped Failed at step 2 with
// CapacityTimeout.
func pd24StoppedAtStepTwo(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := pd24Unit(pd24FinalStep(), nil)
	mustReconcile(t, isvc, in) // step 0 serves under its manual gate
	promotePD24(t, isvc, &in, 8, 16, 3, 5, true)
	promotePD24(t, isvc, &in, 12, 12, 4, 4, false)
	evUnpausePastReadyTimeout(isvc, &in)
	mustReconcile(t, isvc, in) // the first pass after the pause
	evReadyTimeout(isvc, &in)
	mustReconcile(t, isvc, in) // a whole ready timeout after the pause
	return isvc, in
}

// fixtureFailedPD24AtStepTwo is the stop at step 2 with the fleet at the
// step's staged count: 15 engines and 5 decoders serve the canary revision,
// more than the first step's 3 and 1.
func fixtureFailedPD24AtStepTwo(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := pd24StoppedAtStepTwo(t)
	in.PerRevisionPods = map[string]int32{"new": 15, "old": 9}
	decoderOfEightServes(&in, 5, 3, true)
	return isvc, in
}

// fixtureFailedPD24ShortOfStepTwo is the stop at step 2 with the fleet
// short of the step's staged count: 12 engines and 4 decoders serve the
// canary revision.
func fixtureFailedPD24ShortOfStepTwo(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return pd24StoppedAtStepTwo(t)
}

// fixtureFailedPD24ScaledUp is step 1 stopped on the ready timeout before
// any of its Instances came up (3 engines and 1 decoder on the canary
// revision from step 0), after which the engine was scaled from 24 to 32 and
// its 8 new Instances came up on the canary revision: 11 of 32 engines run
// it, more than step 1 stages for 32 (10) and within step 2's 20.
func fixtureFailedPD24ScaledUp(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := pd24Unit(pd24FinalStep(), nil)
	mustReconcile(t, isvc, in) // step 0 serves under its manual gate
	promotePD24(t, isvc, &in, 3, 21, 1, 7, false)
	evReadyTimeout(isvc, &in)
	mustReconcile(t, isvc, in) // nothing came up: the unit stops Failed at step 1
	in.DesiredReplicas = 32
	in.PerRevisionPods = map[string]int32{"new": 11, "old": 21}
	decoderOfEightServes(&in, 1, 7, false)
	return isvc, in
}

// fixtureFailedStableMissingAtTheFinalStep is a rollback requested at the
// final step of a two-step plan whose revert returned every Instance to the
// stable revision before that revision's ControllerRevision went missing:
// the stop holds the rejection at step 1 with nothing on the rejected
// revision, the picture the RolledBack hold would show with the revision
// retained.
func fixtureFailedStableMissingAtTheFinalStep(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureDraining(t)
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	mustReconcile(t, isvc, in) // the revert starts at the final step
	in.PerRevisionPods = map[string]int32{"old": 4}
	mustReconcile(t, isvc, in) // every Instance is back on the stable revision
	delete(isvc.Annotations, constants.RolloutRollbackAnnotation)
	parkFailed(in, rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent), v1beta1.CanaryFailureStableRevisionMissing)
	return isvc, in
}

// firstStepStagesTwoOfFour reads the first step's partition on four
// Instances: two staged, two kept.
func firstStepStagesTwoOfFour(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectProjectedPartition(t, isvc, v1beta1.EngineComponent, 4, 2)
}

// readers runs several readers on one row.
func readers(all ...func(t *testing.T, isvc *v1beta1.InferenceService)) func(t *testing.T, isvc *v1beta1.InferenceService) {
	return func(t *testing.T, isvc *v1beta1.InferenceService) {
		t.Helper()
		for _, read := range all {
			read(t, isvc)
		}
	}
}

// splitOnBothMembers reads the same split on the engine and the decoder:
// weight to the canary revision, the remainder to the stable one.
func splitOnBothMembers(weight int32) func(t *testing.T, isvc *v1beta1.InferenceService) {
	return func(t *testing.T, isvc *v1beta1.InferenceService) {
		t.Helper()
		expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
			"new": {percent: weight, protocol: pdCanaryProtocol, latest: true},
			"old": {percent: 100 - weight, protocol: pdStableProtocol},
		})
		expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
			"decnew": {percent: weight, protocol: pdCanaryProtocol, latest: true},
			"decold": {percent: 100 - weight, protocol: pdStableProtocol},
		})
	}
}

// recordedWeight reads the weight the record carries.
func recordedWeight(want int32) func(t *testing.T, isvc *v1beta1.InferenceService) {
	return func(t *testing.T, isvc *v1beta1.InferenceService) {
		t.Helper()
		if got := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).ObservedTrafficWeight; got != want {
			t.Errorf("the record carries weight %d, want %d", got, want)
		}
	}
}

// TestResumeRowsStartFromAStopAtTheirStep reads the stops the resume rows
// start from: each is stopped Failed with CapacityTimeout at its step, with
// the split and the record's weight as the stop left them.
func TestResumeRowsStartFromAStopAtTheirStep(t *testing.T) {
	cases := []struct {
		name    string
		fixture func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs)
		step    int32
		weight  int32
	}{
		{"at step two", fixtureFailedPD24AtStepTwo, 2, 30},
		{"short of step two", fixtureFailedPD24ShortOfStepTwo, 2, 30},
		{"scaled up", fixtureFailedPD24ScaledUp, 1, 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isvc, _ := c.fixture(t)
			cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
			if got := rollout.StateOf(cs, phaseOf(isvc), len(pd24Steps(pd24FinalStep()))); got != rollout.CanaryStateFailed || cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailureCapacityTimeout {
				t.Fatalf("the fixture must be stopped Failed with CapacityTimeout, got %q (%+v)", got, cs.Failed)
			}
			if cs.CurrentStep != c.step {
				t.Fatalf("the fixture must be stopped at step %d, got %d", c.step, cs.CurrentStep)
			}
			splitOnBothMembers(c.weight)(t, isvc)
			recordedWeight(c.weight)(t, isvc)
		})
	}
}
