package canary

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The fixtures, events and readers of the cases about a stop on the
// reference prefill/decode shape: 24 engines beside 8 decoders under a
// 10/30/60/100 canary. A unit that stops Failed holds the roll where it
// stands: the partition projected for each member is its whole count, so no
// further Instance moves to the stopped revision, and the split stays as the
// stop left it. A rollback, a resume or a new revision ends the hold.

func init() {
	for id, reader := range map[string]func(t *testing.T, isvc *v1beta1.InferenceService){
		"canary/Staging[final,pd24]/readyTimeout": stopHoldsTheRollUnderTheHeldSplit,
		"canary/Draining[A,pd24]/sample[stalled]": stopReturnsTheSplitToStableBothMembers,
		"canary/Failed[pd24]/resync":              stopHoldsTheRollUnderTheHeldSplit,
		"canary/Failed[pd24]/stableGone":          stopHoldsTheRollUnderTheHeldSplit,
		"canary/Failed[pd24]/stableGone[last]":    stopHoldsTheRollWithTheUnitOnTheCanaryRevision,
		"canary/Failed[pd24]/replicasUp":          stopHoldsTheScaledEngineWhole,
		"canary/Failed[pd24]/resume":              resumeContinuesAtTheFinalStep,
		"canary/Failed[pd24]/rollback":            rollbackReleasesThePartition,
	} {
		cellAlso[id] = reader
	}
}

const (
	pd24Engines  int32 = 24
	pd24Decoders int32 = 8
)

// pd24Steps is the 10/30/60/100 list with a manual gate on every step but
// the last, which is the step given.
func pd24Steps(final v1beta1.RolloutGroupStep) []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("10%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("30%"), Traffic: 30, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("60%"), Traffic: 60, Pause: &v1beta1.RolloutPause{}},
		final,
	}
}

// decoderOfEightServes is decoderServes for a decoder scaled to 8.
func decoderOfEightServes(in *ReconcileInputs, canary, stable int32, met bool) {
	decoderServes(in, canary, stable, met)
	m := in.Secondaries[v1beta1.DecoderComponent]
	m.DesiredReplicas = pd24Decoders
	in.Secondaries[v1beta1.DecoderComponent] = m
}

// pd24Unit is the reference shape at step 0, both members staged for it: 3
// of 24 engines and 1 of 8 decoders serve the canary revision.
func pd24Unit(final v1beta1.RolloutGroupStep, prep func(*ReconcileInputs)) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := pdUnit(pd24Steps(final), nil, map[string]int32{"new": 3, "old": 21})
	in.DesiredReplicas = pd24Engines
	decoderOfEightServes(&in, 1, 7, true)
	if prep != nil {
		prep(&in)
	}
	return isvc, in
}

// promotePD24 promotes the unit one step and stands both members at the
// counts given; met says whether the decoder's count meets the new step.
func promotePD24(t *testing.T, isvc *v1beta1.InferenceService, in *ReconcileInputs, engineNew, engineOld, decoderNew, decoderOld int32, met bool) {
	t.Helper()
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, *in) // the promote advances the step
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	in.PerRevisionPods = map[string]int32{"new": engineNew, "old": engineOld}
	decoderOfEightServes(in, decoderNew, decoderOld, met)
	mustReconcile(t, isvc, *in) // the step serves, or stages short
}

// fixtureStagingFinalPD24 is the final step staging with the roll part-way:
// the three gated steps served whole (3, 8 and 15 engines; 1, 3 and 5
// decoders), the final step asks for 23 and 7, 18 engines and 6 decoders
// serve the canary revision so far, and the wait is clocked. The split is
// the step before's, 60 to the canary revision and 40 to the stable one.
func fixtureStagingFinalPD24(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := pd24Unit(v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}, nil)
	mustReconcile(t, isvc, in) // step 0 serves under its manual gate
	promotePD24(t, isvc, &in, 8, 16, 3, 5, true)
	promotePD24(t, isvc, &in, 15, 9, 5, 3, true)
	promotePD24(t, isvc, &in, 18, 6, 6, 2, false)
	return isvc, in
}

// fixtureFailedPD24 is fixtureStagingFinalPD24 stopped on the ready timeout
// of its capacity wait.
func fixtureFailedPD24(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	isvc, in := fixtureStagingFinalPD24(t)
	evReadyTimeout(isvc, &in)
	mustReconcile(t, isvc, in)
	return isvc, in
}

// fixtureDrainingHeldAnalysisPD24 is the final step at 100 percent behind
// its metric gate, both members staged whole (23 engines and 7 decoders on
// the canary revision beside the one held Instance each), baking for an
// hour with one passing sample consumed.
func fixtureDrainingHeldAnalysisPD24(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs) {
	gate := v1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: time.Hour}}, Analysis: analysisGate(nil)}
	isvc, in := pd24Unit(gate, withSampler(analysis.Pass))
	mustReconcile(t, isvc, in)
	promotePD24(t, isvc, &in, 8, 16, 3, 5, true)
	promotePD24(t, isvc, &in, 15, 9, 5, 3, true)
	promotePD24(t, isvc, &in, 23, 1, 7, 1, true) // 100 percent on both members; the gate holds
	return isvc, in
}

// evEngineStablePodGonePD24: one of the engine's six stable pods leaves
// rotation while the unit is stopped.
func evEngineStablePodGonePD24(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 18, "old": 5}
	decoderOfEightServes(in, 6, 2, false)
}

// evEngineStablePodsGonePD24: all six of the engine's stable pods leave
// rotation while the unit is stopped and the decoder keeps its two.
func evEngineStablePodsGonePD24(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.PerRevisionPods = map[string]int32{"new": 18}
	decoderOfEightServes(in, 6, 2, false)
}

// evEngineReplicasUpPD24: the engine is scaled from 24 to 26 while the unit
// is stopped.
func evEngineReplicasUpPD24(_ *v1beta1.InferenceService, in *ReconcileInputs) {
	in.DesiredReplicas = 26
}

// expectProjectedPartition reads the partition the projector would write on
// a member scaled to desired.
func expectProjectedPartition(t *testing.T, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, desired, want int32) {
	t.Helper()
	resolver := PartitionResolver(isvc, rollout.Policies{}, comp)
	if resolver == nil {
		t.Fatalf("no canary governs %s", comp)
	}
	if p := resolver(desired); p == nil || *p != want {
		t.Errorf("%s at %d Instances projects partition %s, want %d", comp, desired, fmtPartition(p), want)
	}
}

// rollHeldWhole reads the stop's hold: each member's partition is its whole
// count, so no further Instance of either moves to the stopped revision.
func rollHeldWhole(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectProjectedPartition(t, isvc, v1beta1.EngineComponent, pd24Engines, pd24Engines)
	expectProjectedPartition(t, isvc, v1beta1.DecoderComponent, pd24Decoders, pd24Decoders)
}

// splitStandsAtSixtyBothMembers reads the step before's split standing on
// both members: 60 to the canary revision, 40 to the stable one.
func splitStandsAtSixtyBothMembers(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	splitOnBothMembers(60)(t, isvc)
}

// splitAllOnCanaryBothMembers reads the cutover's split standing on both
// members: everything on the canary revision.
func splitAllOnCanaryBothMembers(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
}

func stopHoldsTheRollUnderTheHeldSplit(t *testing.T, isvc *v1beta1.InferenceService) {
	rollHeldWhole(t, isvc)
	splitStandsAtSixtyBothMembers(t, isvc)
}

// stopHoldsTheRollWithTheUnitOnTheCanaryRevision reads the stop holding
// the roll whole while both members' traffic rests on the canary revision:
// the unit has one stable side, so the engine's stable pods all lost move
// the decoder's share with the engine's.
func stopHoldsTheRollWithTheUnitOnTheCanaryRevision(t *testing.T, isvc *v1beta1.InferenceService) {
	rollHeldWhole(t, isvc)
	unitAllOnCanary(t, isvc)
}

// stopReturnsTheSplitToStableBothMembers reads a stop from the drain: the
// cutover's split is handed back to the one stable Instance each member
// kept, so the stable revision carries everything again.
func stopReturnsTheSplitToStableBothMembers(t *testing.T, isvc *v1beta1.InferenceService) {
	rollHeldWhole(t, isvc)
	splitRestsOnStable(t, isvc)
}

// stopHoldsTheScaledEngineWhole reads the hold re-sized to the new count: the
// engine scaled to 26 is held at 26, the decoder at 8.
func stopHoldsTheScaledEngineWhole(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectProjectedPartition(t, isvc, v1beta1.EngineComponent, 26, 26)
	expectProjectedPartition(t, isvc, v1beta1.DecoderComponent, pd24Decoders, pd24Decoders)
	splitStandsAtSixtyBothMembers(t, isvc)
}

// resumeContinuesAtTheFinalStep reads the hold ended by a resume: the record
// continues at the final step, the step the stop interrupted, whose
// partition releases every Instance but the one held per member, and the
// split stands as the stop left it until the step writes its own.
func resumeContinuesAtTheFinalStep(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectProjectedPartition(t, isvc, v1beta1.EngineComponent, pd24Engines, 1)
	expectProjectedPartition(t, isvc, v1beta1.DecoderComponent, pd24Decoders, 1)
	splitStandsAtSixtyBothMembers(t, isvc)
}

// rollbackReleasesThePartition reads the hold ended by a rollback: the revert
// is driven by the replica's rollback target, so the partition is 0.
func rollbackReleasesThePartition(t *testing.T, isvc *v1beta1.InferenceService) {
	t.Helper()
	expectProjectedPartition(t, isvc, v1beta1.EngineComponent, pd24Engines, 0)
	expectProjectedPartition(t, isvc, v1beta1.DecoderComponent, pd24Decoders, 0)
}

// A stop is reported once, on the pass that records it, and the message
// says how far each member's roll had come and what ends the hold.
func TestParkFailed_ReportsTheHeldRollOnce(t *testing.T) {
	isvc, in := fixtureStagingFinalPD24(t)
	rec := record.NewFakeRecorder(8)
	in.Recorder = rec
	evReadyTimeout(isvc, &in)
	mustReconcile(t, isvc, in)
	var held []string
	for len(rec.Events) > 0 {
		if ev := <-rec.Events; strings.Contains(ev, EventReasonCanaryRollHeld) {
			held = append(held, ev)
		}
	}
	if len(held) != 1 {
		t.Fatalf("the stop must be reported once, got %v", held)
	}
	for _, want := range []string{
		"engine canary stopped at step 3 (CapacityTimeout)",
		"the roll is held where it stands",
		"engine 18 of 24 Instances on revision new, decoder 6 of 8 Instances on revision decnew",
		"the traffic split stays as written",
		constants.RolloutRollbackAnnotation, constants.RolloutResumeAnnotation,
	} {
		if !strings.Contains(held[0], want) {
			t.Errorf("the report must say %q, got %q", want, held[0])
		}
	}
	in.Now = in.Now.Add(time.Minute)
	mustReconcile(t, isvc, in)
	for len(rec.Events) > 0 {
		if ev := <-rec.Events; strings.Contains(ev, EventReasonCanaryRollHeld) {
			t.Fatalf("a stopped unit reports nothing again, got %q", ev)
		}
	}
}
