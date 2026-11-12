package canary

import (
	"strings"
	"testing"

	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// The stable side of a prefill/decode unit's split is one side for the
// whole unit, as the canary path is. 12 prefill and 8 decode under
// 10/30/60/100 are at the final step's staging with the step before having
// left 60 percent on version 2 and 40 on version 1 on both members.

// TestStableSide_OneMembersLastStablePodMovesTheWholeUnit: the engine's
// last version 1 pod is lost while the decoder still has one. Both members'
// splits move wholly onto version 2 on the pass that reads the loss, so no
// request is split between a version 2 engine and a version 1 decoder, and
// one Warning names the engine's loss and the decoder's share that went
// with it; a pass with nothing changed repeats nothing; the engine's
// version 1 pod serving again puts the 60/40 split back on both members
// with one Normal that speaks for the unit.
func TestStableSide_OneMembersLastStablePodMovesTheWholeUnit(t *testing.T) {
	isvc, in := fixtureStagingFinalPD12(t)
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec
	splitStandsAtSixtyForty(t, isvc)

	evEngineStablePodsGonePD12(isvc, &in)
	mustReconcile(t, isvc, in)
	unitAllOnCanary(t, isvc)
	expectOneEvent(t, rec, EventReasonCanaryTrafficOffStable, "Warning", "engine canary step 3",
		"no pod serves engine stable revision old, so its 40% share of the traffic goes to engine revision new (9 pods serving)",
		"the unit has one stable side, so the decoder's 40% share on stable revision decold goes to decoder revision decnew (5 pods serving) with it",
		"serves on every member again")

	mustReconcile(t, isvc, in)
	unitAllOnCanary(t, isvc)
	expectNoEvents(t, rec)

	in.PerRevisionPods = map[string]int32{"new": 9, "old": 1}
	decoderServesAt(&in, 5, 1, 8, false)
	mustReconcile(t, isvc, in)
	splitStandsAtSixtyForty(t, isvc)
	expectOneEvent(t, rec, EventReasonCanaryTrafficBackOnStable, "Normal", "engine canary step 3",
		"a pod serves the stable revision on every member again",
		"the 40% share of the traffic is back on engine stable revision old",
		"the 40% share of the traffic is back on decoder stable revision decold")
}

// TestStableSide_TheDecodersLastStablePodMovesTheEngineToo: the decoder's
// held version 1 pod is lost while the engine still has two; the engine's
// split follows the decoder's onto version 2 and the Warning names the
// decoder's loss first, then the engine's share that went with it.
func TestStableSide_TheDecodersLastStablePodMovesTheEngineToo(t *testing.T) {
	isvc, in := fixtureStagingFinalPD12(t)
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec

	evDecoderStablePodsGonePD12(isvc, &in)
	mustReconcile(t, isvc, in)
	unitAllOnCanary(t, isvc)
	expectOneEvent(t, rec, EventReasonCanaryTrafficOffStable, "Warning",
		"no pod serves decoder stable revision decold, so its 40% share of the traffic goes to decoder revision decnew (5 pods serving)",
		"the unit has one stable side, so the engine's 40% share on stable revision old goes to engine revision new (9 pods serving) with it")
}

// TestStableSide_WithTheCanaryPathBrokenEachMemberRestsOnItsOwnPods: the
// engine has no version 1 pod and the decoder no version 2 pod, so no one
// version serves on both members and the unit cannot move together. Each
// member rests on the pods it has: the engine on version 2 alone, the
// decoder on version 1 alone. The Warning names the engine's own loss and
// no share that followed it.
func TestStableSide_WithTheCanaryPathBrokenEachMemberRestsOnItsOwnPods(t *testing.T) {
	isvc, in := fixtureStagingFinalPD12(t)
	rec := record.NewFakeRecorder(16)
	in.Recorder = rec

	in.PerRevisionPods = map[string]int32{"new": 9}
	decoderServesAt(&in, 0, 1, 8, false)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})
	events := eventsFrom(rec)
	if len(events) != 1 || !strings.Contains(events[0], EventReasonCanaryTrafficOffStable) || !strings.Contains(events[0], "no pod serves engine stable revision old") {
		t.Fatalf("want one Warning naming the engine's loss, got %v", events)
	}
	if strings.Contains(events[0], "one stable side") {
		t.Fatalf("no share followed the engine's loss while the canary path is broken, got %q", events[0])
	}
}

// TestStableSide_AStoppedUnitKeepsOneStableSideToo: 8 engines beside 8
// decoders stop Failed at the first step with the traffic on version 1.
// Then every version 1 engine pod is lost while the decoder keeps its seven
// and a version 2 pod serves on both members: the engine's traffic goes to
// its version 2 pod and the decoder's goes with it, so the stopped unit's
// requests stay within one version; the next pass holds it there; when a
// version 1 engine pod serves again both members return to version 1,
// where the stop rests them.
func TestStableSide_AStoppedUnitKeepsOneStableSideToo(t *testing.T) {
	isvc, in, rec := servingFirstStep(t)
	enginePods(&in, map[string]int32{"new": 0, "old": 7}, nil)
	mustReconcile(t, isvc, in)
	in.Now = in.Now.Add(in.DefaultReadyTimeout + 1)
	mustReconcile(t, isvc, in)
	if got := engineState(isvc); got != rollout.CanaryStateFailed {
		t.Fatalf("state %q, want Failed: the pod stayed lost past the ready timeout", got)
	}
	eventsFrom(rec)

	enginePods(&in, map[string]int32{"new": 1, "old": 0}, nil)
	decoderOf(&in, 1, 7, true, nil)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectOneEvent(t, rec, EventReasonCanaryTrafficOffStable, "stopped at step 0",
		"no pod serves engine stable revision old, so its 100% share of the traffic goes to engine revision new (1 pod serving)",
		"the unit has one stable side, so the decoder's 100% share on stable revision decold goes to decoder revision decnew (1 pod serving) with it")

	mustReconcile(t, isvc, in)
	if got := engineState(isvc); got != rollout.CanaryStateFailed {
		t.Fatalf("state %q, want Failed: the stop stands", got)
	}
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"new": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decnew": {percent: 100, protocol: pdCanaryProtocol, latest: true}})
	expectNoEvents(t, rec)

	enginePods(&in, map[string]int32{"new": 1, "old": 7}, nil)
	mustReconcile(t, isvc, in)
	expectTargets(t, isvc, v1beta1.EngineComponent, map[string]trafficWant{"old": {percent: 100, protocol: pdStableProtocol}})
	expectTargets(t, isvc, v1beta1.DecoderComponent, map[string]trafficWant{"decold": {percent: 100, protocol: pdStableProtocol}})
	expectOneEvent(t, rec, EventReasonCanaryTrafficBackOnStable, "stopped at step 0",
		"a pod serves the stable revision on every member again",
		"the 100% share of the traffic is back on engine stable revision old",
		"the 100% share of the traffic is back on decoder stable revision decold")
}
