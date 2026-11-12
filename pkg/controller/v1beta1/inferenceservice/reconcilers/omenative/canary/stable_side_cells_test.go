package canary

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The fixtures, events and readers of the cases about the stable side of
// a split: a stable pod retired by the staging or lost, a pod lost while the
// operator's pause holds the unit, a resume while the fleet already stands
// past the first step (it continues at the final step, whose next pass
// opens the drain), and a push landing before the cutover. The unit has
// one stable side: a member whose stable revision has no serving pod left
// moves the whole unit's remainder onto the canary revision, the other
// member's with it, while the canary revision serves on every member.
// Whether a split's weight should move with the capacity that serves it
// while a stable pod still serves is left open; those rows record the
// current behaviour.

func init() {
	for id, reader := range map[string]func(t *testing.T, isvc *v1beta1.InferenceService){
		"canary/Staging/stableGone":                                noSplitWritten,
		"canary/Staging[serving]/stableGone":                       splitStandsAtQuarter,
		"canary/Staging[serving]/stableGone[last]":                 splitAllOnCanary,
		"canary/Staging[final,serving]/stableGone":                 splitStandsAtHalf,
		"canary/Staging[final,serving]/stableGone[last]":           splitAllOnCanary,
		"canary/Staging[final,pd]/stableGone":                      splitKeeps,
		"canary/Staging[final,pd]/stableGone[last]":                unitAllOnCanary,
		"canary/Staging[final,pd12]/stableGone":                    splitStandsAtSixtyForty,
		"canary/Staging[final,pd12]/stableGone[last]":              unitAllOnCanary,
		"canary/Staging[final,pd12]/stableGone[decoderLast]":       unitAllOnCanary,
		"canary/Serving/stableGone":                                splitStandsAtHalf,
		"canary/Serving/stableGone[last]":                          splitAllOnCanary,
		"canary/Serving[T]/stableGone":                             splitStandsAtHalf,
		"canary/Serving[T]/stableGone[last]":                       splitAllOnCanary,
		"canary/Serving[A]/stableGone":                             splitStandsAtHalf,
		"canary/Serving[A]/stableGone[last]":                       splitAllOnCanary,
		"canary/Serving[pd]/stableGone[last]":                      unitAllOnCanary,
		"canary/Serving[pd]/stableGone[decoderLast]":               unitAllOnCanary,
		"canary/PreHold/stableGone":                                splitStandsAtHalf,
		"canary/PreHold/stableGone[last]":                          splitAllOnCanary,
		"canary/Draining[retiring]/stableGone":                     splitAllOnCanary,
		"canary/Draining/stableGone[last]":                         splitAllOnCanary,
		"canary/Draining[M,retiring]/stableGone":                   splitAllOnCanary,
		"canary/Draining[M]/stableGone[last]":                      splitAllOnCanary,
		"canary/RollingBack/stableGone":                            splitAllOnStable,
		"canary/RollingBack/stableGone[last]":                      splitAllOnCanary,
		"canary/RolledBack/stableGone":                             splitAllOnStable,
		"canary/Failed/stableGone[last]":                           splitAllOnCanary,
		"canary/Failed[stableMissing]/stableGone[last]":            splitAllOnCanary,
		"canary/Serving[paused]/capacityDropped":                   splitStandsAtHalfUnclocked,
		"canary/Serving[T,paused]/capacityDropped":                 splitStandsAtHalfUnclocked,
		"canary/Serving[A,paused]/capacityDropped":                 splitStandsAtHalfUnclocked,
		"canary/PreHold[paused]/capacityDropped":                   splitStandsAtHalfUnclocked,
		"canary/Staging[final,paused]/capacityDropped":             splitStandsAtHalf,
		"canary/Serving[pd,paused]/capacityDropped":                splitKeeps,
		"canary/Draining[paused]/capacityDropped":                  splitAllOnCanaryUnclocked,
		"canary/Draining[M,paused]/capacityDropped":                splitAllOnCanaryUnclocked,
		"canary/Failed[fleetAhead]/resume[nextPass]":               splitAllOnCanary,
		"canary/Failed[stableMissing,fleetAhead]/resume[nextPass]": splitAllOnCanary,
		"canary/Draining[pd]/newTarget":                            splitsOnTheHeldStable,
	} {
		cellAlso[id] = reader
	}
}

// pausedFixture holds a fixture under the operator's pause for one pass with
// every pod serving, so the split stands as the step wrote it.
func pausedFixture(fixture func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs)) func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
		isvc, in := fixture(t)
		evPause(isvc, &in)
		mustReconcile(t, isvc, in)
		return isvc, in
	}
}

var (
	fixtureServingManualPaused       = pausedFixture(fixtureServingManual)
	fixtureServingTimedPaused        = pausedFixture(fixtureServingTimed)
	fixtureServingAnalysisPaused     = pausedFixture(fixtureServingAnalysis)
	fixturePreHoldPaused             = pausedFixture(fixturePreHold)
	fixtureStagingFinalServingPaused = pausedFixture(fixtureStagingFinalServing)
	fixtureServingManualPDPaused     = pausedFixture(fixtureServingManualPD)
	fixtureDrainingPaused            = pausedFixture(fixtureDraining)
	fixtureDrainingHeldPaused        = pausedFixture(fixtureDrainingHeld)
)

// drainingRetiring walks the given final step to 100 percent with an old pod
// still serving beside the held Instance, as SurgeThenDrain keeps one until
// its replacement serves: three canary Instances meet the step and two pods
// still serve the stable revision.
func drainingRetiring(t *testing.T, steps []v1beta1.RolloutGroupStep, scaleDownDelay *int32) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc := pinActiveRun(canaryISVC(steps, scaleDownDelay))
	in := baseInputs(isvc, map[string]int32{"new": 2, "old": 2})
	in.TargetID = "t1"
	mustReconcile(t, isvc, in) // the first split serves under its manual hold
	annotate(isvc, "ome.io/rollout-promote", "new")
	mustReconcile(t, isvc, in) // the promote advances to the final step
	delete(isvc.Annotations, "ome.io/rollout-promote")
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 2}
	mustReconcile(t, isvc, in) // 100 percent on the canary revision; an old pod still retiring
	return isvc, in
}

func fixtureDrainingRetiring(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return drainingRetiring(t, twoStep(), i32(3600))
}

func fixtureDrainingHeldRetiring(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	return drainingRetiring(t, twoStepHeldFinal(), nil)
}

// fixtureFailedFleetAhead is the final step stopped on the ready timeout of
// its capacity wait after the lost canary pod came back: three of four
// Instances serve the canary revision, more than the first step stages.
func fixtureFailedFleetAhead(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureFailedAtTheRelease(t)
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	return isvc, in
}

// fixtureFailedStableMissingFleetAhead is the stable-missing stop with three
// of four Instances on the rejected revision, more than the first step stages.
func fixtureFailedStableMissingFleetAhead(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureFailedStableMissing(t)
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
	return isvc, in
}

// A stable pod leaves rotation. The staging and serving fixtures run two
// canary pods beside two stable ones; the drain fixtures three beside the
// held one.
func evStablePodGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 2, "old": 1}
}
func evStablePodsGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 2}
}
func evStagingStablePodGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 3}
}
func evDrainStablePodGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 3, "old": 1}
}
func evDrainStablePodsGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 3}
}
func evRolledBackStablePodGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"old": 3}
}
func evRolledBackStablePodsGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{}
}

// fixtureStagingFinalPD12 is the reference shape at its final step's
// staging: 12 prefill and 8 decode under 10/30/60/100 manual steps, the
// first three steps served and promoted in turn. The engine has 9
// Instances serving version 2, one rebuilding and two still on version 1
// (the held one and one the roll has not taken yet); the decoder has 5 on
// version 2, two rebuilding and its held one on version 1. The step before
// left 60 percent on version 2 and 40 on version 1 on both members.
func fixtureStagingFinalPD12(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := pdUnit(fourStepManual(), nil, map[string]int32{"new": 2, "old": 10})
	in.DesiredReplicas = 12
	decoderServesAt(&in, 1, 7, 8, true)
	mustReconcile(t, isvc, in) // step 0 serves 10 percent under its manual hold
	for _, staged := range []struct{ engineNew, engineOld, decoderNew, decoderOld int32 }{{4, 8, 3, 5}, {8, 4, 5, 3}} {
		annotate(isvc, constants.RolloutPromoteAnnotation, "new")
		mustReconcile(t, isvc, in) // the promote advances; the next step stages
		delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
		in.PerRevisionPods = map[string]int32{"new": staged.engineNew, "old": staged.engineOld}
		decoderServesAt(&in, staged.decoderNew, staged.decoderOld, 8, true)
		mustReconcile(t, isvc, in) // the step's capacity stands and its split serves
	}
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in) // the promote advances to the final step
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": 9, "old": 2}
	decoderServesAt(&in, 5, 1, 8, false)
	mustReconcile(t, isvc, in) // the final step waits on both members; the 60/40 split stands
	return isvc, in
}

// decoderServesAt is decoderServes for a decoder scaled to desired.
func decoderServesAt(in *ReconcileInputs, canary, stable, desired int32, met bool) {
	decoderServes(in, canary, stable, met)
	m := in.Secondaries[v1beta1.DecoderComponent]
	m.DesiredReplicas = desired
	in.Secondaries[v1beta1.DecoderComponent] = m
}

// The reference shape's stable pods at its final step, read off each
// member's own replica: one of the engine's two version 1 pods leaves,
// both leave, or the decoder's held version 1 pod leaves.
func evEngineStablePodGonePD12(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 9, "old": 1}
	decoderServesAt(in, 5, 1, 8, false)
}
func evEngineStablePodsGonePD12(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 9}
	decoderServesAt(in, 5, 1, 8, false)
}
func evDecoderStablePodsGonePD12(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	decoderServesAt(in, 5, 0, 8, false)
}

// The prefill/decode unit's stable pods, read off each member's own replica.
func evEngineStablePodsGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 2}
}
func evEngineStablePodsGoneAtTheFinalStep(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 3}
}
func evDecoderStablePodsGone(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	decoderServes(in, 2, 0, true)
}
func evDecoderStablePodGoneShort(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	decoderServes(in, 2, 1, false)
}

// evResumeOwnThenPass: the resume re-enters the steps and the pass under
// test is the first one the step machine runs from the new record.
func evResumeOwnThenPass(isvc *v1beta1.InferenceService, in *ReconcileInputs) {
	evResumeOwn(isvc, in)
	reconcileOnce(isvc, in)
}

// evNewTargetOverTheDrainPD: version 3 is pushed on both members while the
// drain holds one Instance per member on version 1 and the other three serve
// version 2; no pod of version 3 exists yet.
func evNewTargetOverTheDrainPD(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.CanaryRevisionHash, in.TargetID = "newer", "t2"
	in.Secondaries[v1beta1.DecoderComponent] = MemberRevisions{
		CanaryRevisionHash: "decnewer", StableRevisionHash: "decold",
		CanaryPairingProtocol: pdCanaryProtocol, StablePairingProtocol: pdStableProtocol,
		DesiredReplicas: 4,
	}
	in.SecondaryCapacityReady = false
}

func noSplitWritten(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	if traffic := isvc.Status.Components[v1beta1.EngineComponent].Traffic; len(traffic) != 0 {
		t.Errorf("step 0 has written no split yet, got %+v", traffic)
	}
}

// splitStandsAtHalf reads the manual step's 50/50 split standing.
func splitStandsAtHalf(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 50, latest: true},
		"old": {percent: 50},
	})
}

// splitStandsAtQuarter reads the widening step list's first split standing.
func splitStandsAtQuarter(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 25, latest: true},
		"old": {percent: 75},
	})
}

// splitAllOnCanary reads the whole split on the canary revision: a stable
// revision with no serving pod carries no weight.
func splitAllOnCanary(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, latest: true}})
}

func splitAllOnStable(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100}})
}

func splitStandsAtHalfUnclocked(t *testing.T, isvc *v1beta1.InferenceService) {
	splitStandsAtHalf(t, isvc)
	waitCleared(t, isvc)
}

func splitAllOnCanaryUnclocked(t *testing.T, isvc *v1beta1.InferenceService) {
	splitAllOnCanary(t, isvc)
	waitCleared(t, isvc)
}

// unitAllOnCanary reads both members' splits wholly on their canary
// revision: the unit has one stable side, so a member whose stable revision
// has no serving pod moves the whole unit's remainder, the other member's
// with it, and no request is split between a version 2 engine and a
// version 1 decoder.
func unitAllOnCanary(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
}

// splitStandsAtSixtyForty reads the reference shape's third step split
// standing on both members.
func splitStandsAtSixtyForty(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 60, protocol: pdCanaryProtocol, latest: true},
		"old": {percent: 40, protocol: pdStableProtocol},
	})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 60, protocol: pdCanaryProtocol, latest: true},
		"decold": {percent: 40, protocol: pdStableProtocol},
	})
}

// splitsOnTheHeldStable reads both members' splits wholly on their stable
// revision after a push before the cutover: version 1 is still the stable
// identity, so each member's one version 1 pod carries everything while the
// three version 2 pods beside it carry nothing.
func splitsOnTheHeldStable(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100, protocol: pdStableProtocol}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})
}

// noServingPodCase is one grid row about a split and a revision with no
// serving pod: the fixture and event that make the picture, the member and
// revision that must serve no pod, the percent the member's traffic names
// for that revision after the pass, and the percent on readOn when the row
// names one.
type noServingPodCase struct {
	id      string
	fixture func(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs)
	event   func(isvc *v1beta1.InferenceService, in *ReconcileInputs)
	state   rollout.CanaryState
	comp    v1beta1.ComponentType
	hash    string
	weight  int32
	why     string
	readOn  string
	readsAs int32
}

func runNoServingPodCase(t *testing.T, c noServingPodCase) {
	t.Helper()
	isvc, in := c.fixture(t)
	c.event(isvc, &in)
	mustReconcile(t, isvc, in)
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	steps := len(isvc.Spec.Rollout.Groups[0].Canary.Steps)
	if got := rollout.StateOf(cs, phaseOf(isvc), steps); got != c.state {
		t.Fatalf("state %q, want %q", got, c.state)
	}
	serving := in.PerRevisionPods
	if c.comp != v1beta1.EngineComponent {
		serving = in.GroupReadyPerRevisionPods[c.comp]
	}
	if serving[c.hash] != 0 {
		t.Fatalf("the picture is wrong: %s must serve no pod of %q, got %v", c.comp, c.hash, serving)
	}
	if got := weightOn(isvc, c.comp, c.hash); got != c.weight {
		t.Fatalf("%s sends %d percent to %q, which has no serving pod; the decided reading is %d: %s", c.comp, got, c.hash, c.weight, c.why)
	}
	if c.readOn != "" {
		if got := weightOn(isvc, c.comp, c.readOn); got != c.readsAs {
			t.Fatalf("%s sends %d percent to %q, want %d: %s", c.comp, got, c.readOn, c.readsAs, c.why)
		}
	}
}

// TestTheSplitStandsOnARevisionWithNoServingPod reads the cells where the
// executor leaves a weighted target on a revision none of the member's pods
// serve, which the grid's own pass check refuses: a step's split under the
// operator's pause with every canary pod lost, where the paused pass writes
// nothing and whether the share should move is left open, and a
// settled revert with nothing serving on either side, where there is
// nothing to move the traffic onto. The paused cases record the current
// behaviour.
func TestTheSplitStandsOnARevisionWithNoServingPod(t *testing.T) {
	cases := []noServingPodCase{
		{id: "Serving[paused]/capacityDropped[allLost]", fixture: fixtureServingManualPaused, event: evCanaryPodsAllLost, state: rollout.CanaryStateServing,
			comp: v1beta1.EngineComponent, hash: "new", weight: 50, why: "the paused pass writes nothing, so the step's share stays on the canary revision while no pod of it serves"},
		{id: "Serving[pd,paused]/capacityDropped[allLost]", fixture: fixtureServingManualPDPaused, event: evDecoderCapacityLost, state: rollout.CanaryStateServing,
			comp: v1beta1.DecoderComponent, hash: "decnew", weight: 50, why: "the paused pass writes nothing, so the decoder's share stays on its canary revision while no pod of it serves"},
		{id: "RolledBack/stableGone[last]", fixture: fixtureRolledBack, event: evRolledBackStablePodsGone, state: rollout.CanaryStateRolledBack,
			comp: v1beta1.EngineComponent, hash: "old", weight: 100, why: "nothing serves on either side after the revert, so the settled hold has nothing to move the traffic onto and the 100 percent stands until a stable pod serves again"},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) { runNoServingPodCase(t, c) })
	}
}

// TestTheSplitFollowsTheServingPods reads the cells where a stable revision
// loses its last serving pod while the other revision serves, under a stop
// and under a revert: the member's traffic goes to the revision that
// serves, so no share names a revision with nothing behind it.
func TestTheSplitFollowsTheServingPods(t *testing.T) {
	cases := []noServingPodCase{
		{id: "Failed[release]/stableGone[last]", fixture: fixtureFailedAtTheRelease, event: evStablePodsGone, state: rollout.CanaryStateFailed,
			comp: v1beta1.EngineComponent, hash: "old", weight: 0, readOn: "new", readsAs: 100, why: "the stopped unit's traffic goes to the canary pods that serve when the last stable pod leaves"},
		{id: "RollingBack/stableGone[last]", fixture: fixtureRollingBack, event: evStablePodsGone, state: rollout.CanaryStateRollingBack,
			comp: v1beta1.EngineComponent, hash: "old", weight: 0, readOn: "new", readsAs: 100, why: "the revert rests the traffic on the rejected pods that serve while the stable revision has none"},
		{id: "Failed[stableMissing]/stableGone[last]", fixture: fixtureFailedStableMissing, event: evStablePodsGone, state: rollout.CanaryStateFailed,
			comp: v1beta1.EngineComponent, hash: "old", weight: 0, readOn: "new", readsAs: 100, why: "the stopped unit's traffic goes to the rejected pods that serve when the last stable pod leaves"},
	}
	for _, c := range cases {
		t.Run(c.id, func(t *testing.T) { runNoServingPodCase(t, c) })
	}
}

// evCanaryPodsAllLost: every canary pod of the step is lost at once while the
// stable pods serve.
func evCanaryPodsAllLost(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 0, "old": 2}
}
