package canary

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A rollout phase written Failed from outside the controller, with the
// record's failure reason empty, is read by where the record stands. A unit
// short of its last step keeps the stop: the reason PhaseWritten is recorded
// and the hold is reported once, so the operator sees why the roll holds.
// A unit at or past its done mark keeps finishing: the phase is projected
// from the record again (Promoting, then Stable), nothing is held and no
// partition is projected again. The grid rows of that write in
// TestReconcileCells read the recorded reason, or its absence, on every
// state; the tests here walk the two stories through whole passes.

// quarterThenAll is the step list of the 4:4 shape: a quarter of the
// capacity at 10 percent of the traffic under a manual gate, then everything.
func quarterThenAll() []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("25%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// halfThenAll is the step list of the 2:2 shape: half of the capacity at 10
// percent of the traffic under a manual gate, then everything.
func halfThenAll() []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("50%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// decoderOfServes is decoderServes for a decoder scaled to desired.
func decoderOfServes(in *ReconcileInputs, desired, canary, stable int32, met bool) {
	decoderServes(in, canary, stable, met)
	m := in.Secondaries[v1beta1.DecoderComponent]
	m.DesiredReplicas = desired
	in.Secondaries[v1beta1.DecoderComponent] = m
}

// pdUnitOf is pdUnit with both members scaled to the counts given under a
// ten-minute drain window, before its first pass: the engine's pods as
// given, decoderCanary decoder Instances serving the canary revision beside
// the rest on the stable one, and the decoder's step met. The run's target
// id is the one the bind computes from the members' targets, so a record
// read after the run closed still names the targets the members observe.
func pdUnitOf(steps []v1beta1.RolloutGroupStep, engines, decoders int32, enginePods map[string]int32, decoderCanary int32) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	isvc, in := pdUnit(steps, i32(600), enginePods)
	in.TargetID = activeCanaryTargetID(isvc, &isvc.Spec.Rollout.Groups[0])
	in.DesiredReplicas = engines
	decoderOfServes(&in, decoders, decoderCanary, decoders-decoderCanary, true)
	rec := record.NewFakeRecorder(32)
	in.Recorder = rec
	return isvc, in, rec
}

// writeFailedPhase is the write under test, as a tool or a hand edit makes
// it: the primary's phase set to Failed with the record's failure reason
// empty.
func writeFailedPhase(isvc *v1beta1.InferenceService) {
	rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent).Failed = nil
	setPhase(isvc, v1beta1.EngineComponent, v1beta1.RolloutPhaseFailed)
}

// heldRollEvents drains the recorder and keeps the CanaryRollHeld events.
func heldRollEvents(rec *record.FakeRecorder) []string {
	var held []string
	for _, ev := range drainEvents(rec) {
		if strings.Contains(ev, EventReasonCanaryRollHeld) {
			held = append(held, ev)
		}
	}
	return held
}

// expectAllTrafficOn reads one target on the Component: all of it on hash.
func expectAllTrafficOn(t *testing.T, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) {
	t.Helper()
	traffic := isvc.Status.Components[comp].Traffic
	if len(traffic) != 1 || query.RevisionFromName(traffic[0].RevisionName).Hash() != hash || traffic[0].Percent != 100 {
		t.Errorf("%s sends everything to %s, got %+v", comp, hash, traffic)
	}
}

// expectUnitState reads the unit's state and the primary's phase together.
func expectUnitReads(t *testing.T, isvc *v1beta1.InferenceService, steps int, state rollout.CanaryState, phase v1beta1.RolloutPhase) *v1beta1.CanaryStatus {
	t.Helper()
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	if got := rollout.StateOf(cs, phaseOf(isvc), steps); got != state || phaseOf(isvc) != phase {
		t.Fatalf("state %q phase %q, want %q %q (record %+v)", got, phaseOf(isvc), state, phase, cs)
	}
	return cs
}

// Four engines and four decoders at a 25 percent manual step serve 10
// percent on version 2; two minutes in, a phase written Failed with no
// failure reason stops the roll. The pass records the reason PhaseWritten at
// the write's time, reports the hold once with each member's count on
// version 2, keeps the split and holds every Instance where it stands; the
// pass after reports nothing again and comes back at the stopped cadence.
func TestApplyHolds_PhaseWrittenFailedInFlightIsRecordedAndReportedOnce(t *testing.T) {
	isvc, in, rec := pdUnitOf(quarterThenAll(), 4, 4, map[string]int32{"new": 1, "old": 3}, 1)
	mustReconcile(t, isvc, in) // the first step's split serves under its manual gate
	expectUnitReads(t, isvc, 2, rollout.CanaryStateServing, v1beta1.RolloutPhasePaused)
	drainEvents(rec)

	in.Now = in.Now.Add(2 * time.Minute)
	writeFailedPhase(isvc)
	res := mustReconcile(t, isvc, in)

	cs := expectUnitReads(t, isvc, 2, rollout.CanaryStateFailed, v1beta1.RolloutPhaseFailed)
	if cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailurePhaseWritten || cs.Failed.Time == nil || !cs.Failed.Time.Time.Equal(in.Now) {
		t.Fatalf("the stop records PhaseWritten at the write's pass, got %+v", cs.Failed)
	}
	if !res.Active {
		t.Errorf("the stop keeps the unit active, got %+v", res)
	}
	expectProjectedPartition(t, isvc, v1beta1.EngineComponent, 4, 4)
	expectProjectedPartition(t, isvc, v1beta1.DecoderComponent, 4, 4)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 10, protocol: pdCanaryProtocol, latest: true},
		"old": {percent: 90, protocol: pdStableProtocol},
	})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 10, protocol: pdCanaryProtocol, latest: true},
		"decold": {percent: 90, protocol: pdStableProtocol},
	})
	held := heldRollEvents(rec)
	if len(held) != 1 {
		t.Fatalf("the stop is reported once, got %v", held)
	}
	for _, want := range []string{
		"engine canary stopped at step 0 (PhaseWritten)",
		"the roll is held where it stands",
		"engine 1 of 4 Instances on revision new, decoder 1 of 4 Instances on revision decnew",
		"the traffic split stays as written",
		constants.RolloutRollbackAnnotation, constants.RolloutResumeAnnotation,
	} {
		if !strings.Contains(held[0], want) {
			t.Errorf("the CanaryRollHeld event must say %q, got %q", want, held[0])
		}
	}

	in.Now = in.Now.Add(time.Minute)
	res = mustReconcile(t, isvc, in)
	cs = expectUnitReads(t, isvc, 2, rollout.CanaryStateFailed, v1beta1.RolloutPhaseFailed)
	if cs.Failed == nil || cs.Failed.Reason != v1beta1.CanaryFailurePhaseWritten {
		t.Errorf("the recorded reason stands, got %+v", cs.Failed)
	}
	if got := heldRollEvents(rec); len(got) != 0 {
		t.Errorf("a stopped unit reports nothing again, got %v", got)
	}
	if res.RequeueAfter != failedRequeue {
		t.Errorf("the stop comes back at the stopped cadence %s, got %s", failedRequeue, res.RequeueAfter)
	}
}

// releasedPDUnit walks two engines and two decoders through a two-step
// manual canary to its done mark: the first step serves at t+0, the promote
// at t+1m shifts everything to version 2, the ten-minute window elapses and
// at t+12m the held version 1 Instance of each member is released. The
// released Instances are still rolling when it returns.
func releasedPDUnit(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	t.Helper()
	isvc, in, rec := pdUnitOf(halfThenAll(), 2, 2, map[string]int32{"new": 1, "old": 1}, 1)
	mustReconcile(t, isvc, in) // the first step's split serves under its manual gate
	in.Now = in.Now.Add(time.Minute)
	annotate(isvc, constants.RolloutPromoteAnnotation, "new")
	mustReconcile(t, isvc, in) // the promote advances to the final step
	delete(isvc.Annotations, constants.RolloutPromoteAnnotation)
	mustReconcile(t, isvc, in) // everything shifts to version 2; the window opens
	in.Now = in.Now.Add(11 * time.Minute)
	res := mustReconcile(t, isvc, in) // the window elapsed: the held Instances are released
	expectUnitReads(t, isvc, 2, rollout.CanaryStateDone, v1beta1.RolloutPhasePromoting)
	if res.Partition != 0 {
		t.Fatalf("the release projects partition 0, got %d", res.Partition)
	}
	drainEvents(rec)
	return isvc, in, rec
}

// completedPDUnit is releasedPDUnit once the released Instances rolled and
// the cutover completed at t+12m: every Instance serves version 2, the
// promoted revisions are recorded, the unit reads Stable and the run closed.
func completedPDUnit(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	t.Helper()
	isvc, in, rec := releasedPDUnit(t)
	in.PerRevisionPods = map[string]int32{"new": 2}
	decoderOfServes(&in, 2, 2, 0, true)
	res := mustReconcile(t, isvc, in)
	expectUnitReads(t, isvc, 2, rollout.CanaryStateDone, v1beta1.RolloutPhaseStable)
	if !res.Complete {
		t.Fatalf("the pass that counts every Instance on version 2 completes, got %+v", res)
	}
	isvc.Status.Rollout.ActiveRun = nil
	in.RunActive = false
	in.TargetID = ""
	drainEvents(rec)
	return isvc, in, rec
}

// Two engines and two decoders finished their canary and the run closed at
// t+12m; at t+14m a phase written Failed with no failure reason lands. The
// done mark outranks the phase: the pass reads Stable again, records no
// failure, holds nothing, projects partition 0 as the release did and
// reports nothing.
func TestApplyHolds_PhaseWrittenFailedAfterTheCutoverKeepsTheUnitStable(t *testing.T) {
	isvc, in, rec := completedPDUnit(t)
	in.Now = in.Now.Add(2 * time.Minute)
	writeFailedPhase(isvc)
	res := mustReconcile(t, isvc, in)

	cs := expectUnitReads(t, isvc, 2, rollout.CanaryStateDone, v1beta1.RolloutPhaseStable)
	if cs.Failed != nil {
		t.Errorf("a finished unit records no failure, got %+v", cs.Failed)
	}
	if res.Active || res.Partition != 0 {
		t.Errorf("a finished unit is inactive at partition 0, got active=%v partition=%d", res.Active, res.Partition)
	}
	expectProjectedPartition(t, isvc, v1beta1.EngineComponent, 2, 0)
	expectProjectedPartition(t, isvc, v1beta1.DecoderComponent, 2, 0)
	expectAllTrafficOn(t, isvc, v1beta1.EngineComponent, "new")
	expectAllTrafficOn(t, isvc, v1beta1.DecoderComponent, "decnew")
	if held := heldRollEvents(rec); len(held) != 0 {
		t.Errorf("nothing is held on a finished unit, got %v", held)
	}
}

// Two engines and two decoders released their held version 1 Instances at
// t+12m and those Instances still roll when, at t+14m, a phase written
// Failed with no failure reason lands. The done mark outranks the phase: the
// pass keeps Promoting at partition 0, records no failure, keeps everything
// on version 2 and reports nothing; once the released Instances serve
// version 2 the cutover completes as if nothing had been written.
func TestApplyHolds_PhaseWrittenFailedWhileTheReleasedInstancesRollKeepsPromoting(t *testing.T) {
	isvc, in, rec := releasedPDUnit(t)
	in.Now = in.Now.Add(2 * time.Minute)
	writeFailedPhase(isvc)
	res := mustReconcile(t, isvc, in)

	cs := expectUnitReads(t, isvc, 2, rollout.CanaryStateDone, v1beta1.RolloutPhasePromoting)
	if cs.Failed != nil {
		t.Errorf("a finishing unit records no failure, got %+v", cs.Failed)
	}
	if !res.Active || res.Partition != 0 || (res.RequeueAfter == 0 && !res.Requeue) {
		t.Errorf("the cutover goes on at partition 0 and comes back, got %+v", res)
	}
	expectProjectedPartition(t, isvc, v1beta1.EngineComponent, 2, 0)
	expectProjectedPartition(t, isvc, v1beta1.DecoderComponent, 2, 0)
	expectAllTrafficOn(t, isvc, v1beta1.EngineComponent, "new")
	expectAllTrafficOn(t, isvc, v1beta1.DecoderComponent, "decnew")
	if held := heldRollEvents(rec); len(held) != 0 {
		t.Errorf("nothing is held while the released Instances roll, got %v", held)
	}

	in.Now = in.Now.Add(time.Minute)
	in.PerRevisionPods = map[string]int32{"new": 2}
	decoderOfServes(&in, 2, 2, 0, true)
	res = mustReconcile(t, isvc, in)
	expectUnitReads(t, isvc, 2, rollout.CanaryStateDone, v1beta1.RolloutPhaseStable)
	if !res.Complete {
		t.Errorf("the released Instances serving version 2 complete the cutover, got %+v", res)
	}
}
