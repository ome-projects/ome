package coordination

import (
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Two protocol changes in a row: the protocol went from v1 to v2, and
// before that roll finished it went on to v3. Three cohorts are then in
// the picture at once: pods still on v1, pods on v2 (the first change's
// replacements, some already promoted) and the first v3 pods starting.
// Both sides surge and neither may lose a pairable Instance.
//
// secondChangeFleet is that picture for one engine (prefill) count and two
// decoders, or one decoder at the 1x1 shape. The last engine Instance has
// finished its move to v2 and runs a v2 pod alone; the other engines still
// serve v1, the first of them with a v3 replacement that is not Ready yet.
// The last decoder Instance is mid-surge: its v1 source and its v2
// replacement both serve, set to the v2 revision. engineOnV2 false keeps
// that last engine on v1, so no engine serves v2 at all.
func secondChangeFleet(isvc *v1beta1.InferenceService, engines, decoders int, engineOnV2 bool) []client.Object {
	lastEngine, lastDecoder := engines-1, decoders-1
	engineRows := []v1beta1.OMENativeInstanceStatus{surgingRow(isvc.Name, v1beta1.EngineComponent, 0, "v1", "v3", workloadtypes.UpdateStepSurge, 1)}
	for i := 1; i < lastEngine; i++ {
		engineRows = append(engineRows, settledRow(isvc.Name, v1beta1.EngineComponent, i, "v1"))
	}
	lastEngineCohort := "v1"
	if engineOnV2 {
		lastEngineCohort = "v2"
	}
	if lastEngine > 0 {
		engineRows = append(engineRows, settledRow(isvc.Name, v1beta1.EngineComponent, lastEngine, lastEngineCohort))
	} else {
		engineRows = []v1beta1.OMENativeInstanceStatus{surgingRow(isvc.Name, v1beta1.EngineComponent, 0, lastEngineCohort, "v3", workloadtypes.UpdateStepSurge, 1)}
	}
	var decoderRows []v1beta1.OMENativeInstanceStatus
	for i := 0; i < lastDecoder; i++ {
		decoderRows = append(decoderRows, settledRow(isvc.Name, v1beta1.DecoderComponent, i, "v1"))
	}
	decoderRows = append(decoderRows, surgingRow(isvc.Name, v1beta1.DecoderComponent, lastDecoder, "v1", "v2", workloadtypes.UpdateStepSurge, 2))

	objs := []client.Object{
		withRows(sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, engines, 0), engineRows...),
		withRows(sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategySurgeThenDrain, decoders, 0), decoderRows...),
	}
	for _, comp := range pairingComponents {
		for _, proto := range []string{"v1", "v2", "v3"} {
			objs = append(objs, pairingCR(isvc.Namespace, isvc.Name, comp, proto, proto))
		}
	}
	// Engines: the first Instance's v1 source with a v3 replacement still
	// starting, the middle Instances on v1, the last one on v2 alone.
	if lastEngine > 0 {
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, lastEngine, 0, "v1", true)...)
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 1, 1, "v3", false)...)
		objs = append(objs, fleetPods(v1beta1.EngineComponent, lastEngine, 1, 1, lastEngineCohort, true)...)
	} else {
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 1, 1, lastEngineCohort, true)...)
		objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 1, 0, "v3", false)...)
	}
	// Decoders: the settled ones on v1, the last one serving v1 and v2.
	if lastDecoder > 0 {
		objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, lastDecoder, 0, "v1", true)...)
	}
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, lastDecoder, 1, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, lastDecoder, 1, 1, "v2", true)...)
	return objs
}

// decoderPod names the last decoder Instance's pod at ordinal 0 (the v1
// source) or 1 (the v2 replacement).
func decoderPod(decoders int, ordinal int) string {
	return fmt.Sprintf("llama-decoder-%d-default-%d", decoders-1, ordinal)
}

// The drain that finishes the first change on the last decoder, after the
// second change moved the roll target to v3: its v1 source leaves and its
// v2 replacement, already serving, stays. The replacement is traded for
// the v2 cohort, which the engine on v2 pairs with, so the drain proceeds
// at both shapes. A gate that reads the replacement as a v3 pod would hold
// it for a v3 engine that nothing brings up before this decoder has moved.
// With no engine on v2 at all the drain waits, as on a first change, for
// an engine of the cohort the replacement runs.
func TestEvaluatePairingDrain_SecondChange_HandOverIsJudgedOnThePinnedCohort(t *testing.T) {
	for _, shape := range []struct {
		name              string
		engines, decoders int
	}{
		{name: "1 prefill and 1 decode", engines: 1, decoders: 1},
		{name: "6 prefill and 2 decode", engines: 6, decoders: 2},
	} {
		t.Run(shape.name, func(t *testing.T) {
			isvc := pairingISVC("v3")
			source := []string{decoderPod(shape.decoders, 0)}
			if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, source, secondChangeFleet(isvc, shape.engines, shape.decoders, true)...); !allowed {
				t.Fatalf("the v2 replacement pairs with the engine on v2, so the v1 source may leave: %s", reason)
			}
			allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, source, secondChangeFleet(isvc, shape.engines, shape.decoders, false)...)
			requireHeld(t, allowed, gate, reason, "no engine serves the cohort the replacement runs")
			if !strings.Contains(reason, `"v2"`) {
				t.Errorf("the hold must name the replacement's own cohort, v2, as the one no engine serves: %s", reason)
			}
		})
	}
}

// The other question the second change raises: the v2 replacement itself
// leaving rotation, with its v1 source staying, which is what a roll
// retargeted to v3 does to a surge it abandons. At 1x1 the engine on v2
// pairs with that replacement and nothing else, so taking it out leaves no
// pair at all and the drain is held. At 6x2 the engine on v2 is the sixth
// pairable engine Instance and loses its only partner: five of six, under
// a pairable minimum of six, so the drain is held on it. When no engine serves
// v2 the replacement partners nothing yet and may leave.
func TestEvaluatePairingDrain_SecondChange_WithdrawingThePartnerReplacementIsHeld(t *testing.T) {
	isvc := pairingISVC("v3")

	oneByOne := secondChangeFleet(isvc, 1, 1, true)
	allowed, gate, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{decoderPod(1, 1)}, oneByOne...)
	if allowed || gate != v1beta1.RolloutHoldGatePairing {
		t.Fatalf("1x1: the v2 decoder is the engine's only partner; withdrawing it must be held: allowed=%v gate=%q reason=%s", allowed, gate, reason)
	}
	if !strings.Contains(reason, "no pairable serving engine+decoder pair") {
		t.Errorf("1x1: the hold must say no pair would be left: %s", reason)
	}

	sixByTwo := secondChangeFleet(isvc, 6, 2, true)
	allowed, gate, reason = drainGate(t, isvc, v1beta1.DecoderComponent, []string{decoderPod(2, 1)}, sixByTwo...)
	requireFloorHeld(t, allowed, gate, reason, "6x2: the engine on v2 would lose its only partner")
	if !strings.Contains(reason, "engine") || !strings.Contains(reason, "5 pairable") {
		t.Errorf("6x2: the hold must say the engine falls to five pairable Instances: %s", reason)
	}

	noEngineOnV2 := secondChangeFleet(isvc, 6, 2, false)
	if allowed, _, reason := drainGate(t, isvc, v1beta1.DecoderComponent, []string{decoderPod(2, 1)}, noEngineOnV2...); !allowed {
		t.Fatalf("6x2 with no engine on v2: the v2 decoder partners nothing yet, so withdrawing it costs no pair: %s", reason)
	}
}
