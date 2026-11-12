package canary

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// A split never names a revision with no serving pod while the other
// revision serves: the share of a stable revision whose pods are all lost
// goes to the revision that serves, under a revert and under a stop as at
// a step, with a Warning naming the member, and comes back with a Normal
// when a stable pod serves again. With nothing serving on either side the
// split stands: a loss with nothing to move onto is the workload's repair.
// The shape is 8 engines beside 8 decoders under a four-step canary.

// fourSteps is the reference step list: 10, 30, 60 and 100 percent.
func fourSteps() []v1beta1.RolloutGroupStep {
	return []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("10%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("30%"), Traffic: 30, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("60%"), Traffic: 60, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
}

// eightByEight is a prefill/decode unit of 8 engines beside 8 decoders,
// the engine primary, its record opened at step 0 under fourSteps before
// its first pass, with the engine's pods as given and the decoder serving
// as decoderOf says.
func eightByEight(steps []v1beta1.RolloutGroupStep, enginePods map[string]int32) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	isvc, in := pdUnit(steps, nil, enginePods)
	in.DesiredReplicas = 8
	rec := record.NewFakeRecorder(32)
	in.Recorder = rec
	return isvc, in, rec
}

// decoderOf is the decoder's pods as the pass reads them: canary Instances
// serving decnew beside stable ones serving decold, out of 8, and whether
// the serving canary Instances meet the step. total is every pod of each
// revision, ready or not, the view the revert reads; nil reads the serving
// pods as the total.
func decoderOf(in *ReconcileInputs, canary, stable int32, met bool, total map[string]int32) {
	pods := map[string]int32{"decnew": canary, "decold": stable}
	if total == nil {
		total = pods
	}
	in.GroupReadyPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{v1beta1.DecoderComponent: pods}
	in.GroupTotalPerRevisionPods = map[v1beta1.ComponentType]map[string]int32{v1beta1.EngineComponent: in.PerRevisionPods, v1beta1.DecoderComponent: total}
	m := in.Secondaries[v1beta1.DecoderComponent]
	m.ReadyCanaryCapacity, m.DesiredReplicas = canary, 8
	in.Secondaries[v1beta1.DecoderComponent] = m
	in.SecondaryCapacityReady = met
}

// enginePods sets the engine's serving pods and, when given, every pod of
// each revision ready or not.
func enginePods(in *ReconcileInputs, serving, total map[string]int32) {
	in.PerRevisionPods = serving
	if total == nil {
		total = serving
	}
	in.GroupTotalPerRevisionPods[v1beta1.EngineComponent] = total
}

// servingFirstStep walks the unit to its first split serving under the
// manual hold: one engine and one decoder on the canary revision beside
// seven on the stable one, 10 percent to the canary revision on both.
func servingFirstStep(t *testing.T) (*v1beta1.InferenceService, ReconcileInputs, *record.FakeRecorder) {
	t.Helper()
	isvc, in, rec := eightByEight(fourSteps(), map[string]int32{"new": 1, "old": 7})
	decoderOf(&in, 1, 7, true, nil)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{
		"new": {percent: 10, protocol: pdCanaryProtocol, latest: true},
		"old": {percent: 90, protocol: pdStableProtocol},
	})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{
		"decnew": {percent: 10, protocol: pdCanaryProtocol, latest: true},
		"decold": {percent: 90, protocol: pdStableProtocol},
	})
	eventsFrom(rec)
	return isvc, in, rec
}

func engineState(isvc *v1beta1.InferenceService) rollout.CanaryState {
	cs := rollout.CanaryStatusFor(&isvc.Status, v1beta1.EngineComponent)
	return rollout.StateOf(cs, phaseOf(isvc), len(isvc.Spec.Rollout.Groups[0].Canary.Steps))
}

// expectOneEvent asserts the recorder holds exactly one event of the reason
// whose message carries every one of the given words.
func expectOneEvent(t *testing.T, rec *record.FakeRecorder, reason string, words ...string) {
	t.Helper()
	events := eventsFrom(rec)
	if len(events) != 1 || !strings.Contains(events[0], reason) {
		t.Fatalf("want one %s event, got %v", reason, events)
	}
	for _, w := range words {
		if !strings.Contains(events[0], w) {
			t.Fatalf("the %s event must say %q, got %q", reason, w, events[0])
		}
	}
}

func expectNoEvents(t *testing.T, rec *record.FakeRecorder) {
	t.Helper()
	if events := eventsFrom(rec); len(events) != 0 {
		t.Fatalf("a pass that changes nothing repeats nothing, got %v", events)
	}
}

// The operator rolls back at the first step and the revert writes 100
// percent onto version 1 while its pods serve. Then the node under every
// version 1 pod stops: no stable pod serves on either member while the
// rejected pods still do. The traffic follows the pods that serve, with one
// Warning naming both members; a pass that changes nothing repeats nothing;
// when the version 1 pods serve again the traffic is back on them with one
// Normal.
func TestRevert_RestsOnTheRejectedRevisionWhileNoStablePodServes(t *testing.T) {
	isvc, in, rec := servingFirstStep(t)
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	mustReconcile(t, isvc, in)
	if got := engineState(isvc); got != rollout.CanaryStateRollingBack {
		t.Fatalf("state %q, want RollingBack", got)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100, protocol: pdStableProtocol}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})
	eventsFrom(rec)

	enginePods(&in, map[string]int32{"new": 1, "old": 0}, map[string]int32{"new": 1, "old": 7})
	decoderOf(&in, 1, 0, true, map[string]int32{"decnew": 1, "decold": 7})
	mustReconcile(t, isvc, in)
	if got := engineState(isvc); got != rollout.CanaryStateRollingBack {
		t.Fatalf("state %q, want RollingBack: the revert goes on", got)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectOneEvent(t, rec, EventReasonCanaryTrafficOffStable, "engine stable revision old", "decoder stable revision decold", "rollback")

	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectNoEvents(t, rec)

	enginePods(&in, map[string]int32{"new": 1, "old": 7}, nil)
	decoderOf(&in, 1, 7, true, nil)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100, protocol: pdStableProtocol}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})
	expectOneEvent(t, rec, EventReasonCanaryTrafficBackOnStable, "engine stable revision old", "decoder stable revision decold")
}

// A revert that finds no pod serving on either side leaves the split as it
// stands: there is nothing to move the traffic onto.
func TestRevert_KeepsItsSplitWhileNothingServes(t *testing.T) {
	isvc, in, rec := servingFirstStep(t)
	annotate(isvc, constants.RolloutRollbackAnnotation, "true")
	mustReconcile(t, isvc, in)
	eventsFrom(rec)

	enginePods(&in, map[string]int32{}, map[string]int32{"new": 1, "old": 7})
	decoderOf(&in, 0, 0, false, map[string]int32{"decnew": 1, "decold": 7})
	mustReconcile(t, isvc, in)
	if got := engineState(isvc); got != rollout.CanaryStateRollingBack {
		t.Fatalf("state %q, want RollingBack", got)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100, protocol: pdStableProtocol}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})
	expectNoEvents(t, rec)
}

// The canary engine pod is lost at the first step and stays lost past the
// ready timeout: the unit stops Failed with the traffic on version 1. The
// pod serving again gives the stopped canary nothing back. Then every
// version 1 pod is lost: the traffic goes to the pods that serve, with one
// Warning; when a version 1 pod serves again the traffic is back on it,
// the stop's resting place, with one Normal.
func TestStopped_TrafficFollowsTheServingPodsUnderTheStop(t *testing.T) {
	isvc, in, rec := servingFirstStep(t)
	enginePods(&in, map[string]int32{"new": 0, "old": 7}, nil)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100, protocol: pdStableProtocol}})
	in.Now = in.Now.Add(in.DefaultReadyTimeout + 1)
	mustReconcile(t, isvc, in)
	if got := engineState(isvc); got != rollout.CanaryStateFailed {
		t.Fatalf("state %q, want Failed: the pod stayed lost past the ready timeout", got)
	}
	eventsFrom(rec)

	enginePods(&in, map[string]int32{"new": 1, "old": 7}, nil)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100, protocol: pdStableProtocol}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})
	expectNoEvents(t, rec)

	enginePods(&in, map[string]int32{"new": 1, "old": 0}, nil)
	decoderOf(&in, 1, 0, true, nil)
	mustReconcile(t, isvc, in)
	if got := engineState(isvc); got != rollout.CanaryStateFailed {
		t.Fatalf("state %q, want Failed: the stop stands", got)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectOneEvent(t, rec, EventReasonCanaryTrafficOffStable, "engine stable revision old", "decoder stable revision decold", "stopped")

	mustReconcile(t, isvc, in)
	expectNoEvents(t, rec)

	enginePods(&in, map[string]int32{"new": 1, "old": 7}, nil)
	decoderOf(&in, 1, 7, true, nil)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100, protocol: pdStableProtocol}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})
	expectOneEvent(t, rec, EventReasonCanaryTrafficBackOnStable, "engine stable revision old", "decoder stable revision decold")
}

// A capacity-ahead first step (every Instance but the held one on the
// canary revision at 0 percent) over a service whose last cutover left 100
// percent on version 1: seven engines serve version 2, the one held
// version 1 engine does not, and the decoder's canary Instances are still
// coming up. The engine's traffic goes to its seven serving pods, with one
// Warning, instead of naming version 1 with no pod behind it; the decoder,
// whose stable pods serve, keeps everything on them.
func TestCapacityAheadStep_EngineTrafficGoesToItsServingPodsWhileItsHeldStablePodIsDown(t *testing.T) {
	steps := []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("100%"), Traffic: 0, Pause: &v1beta1.RolloutPause{}},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}
	isvc, in, rec := eightByEight(steps, map[string]int32{"new": 7, "old": 0})
	decoderOf(&in, 0, 8, false, nil)
	writeTraffic(isvc, v1beta1.EngineComponent, "", "old", "", pdStableProtocol, 0)
	writeTraffic(isvc, v1beta1.DecoderComponent, "", "decold", "", pdStableProtocol, 0)
	mustReconcile(t, isvc, in) // opens the record; the decoder is short, so the step waits
	mustReconcile(t, isvc, in)
	if got := engineState(isvc); got != rollout.CanaryStateStaging {
		t.Fatalf("state %q, want Staging: the decoder's canary Instances are still coming up", got)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})
	expectOneEvent(t, rec, EventReasonCanaryTrafficOffStable, "engine stable revision old")
}
