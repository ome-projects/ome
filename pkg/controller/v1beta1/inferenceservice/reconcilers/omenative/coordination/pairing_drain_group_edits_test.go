package coordination

import (
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A group edit while a run is open never switches the pairing gates off in
// the middle of a pairing-protocol change. The gates read the members the
// run recorded, so a member added to or removed from the group changes
// nothing for them until the next run, and a group that shares no member
// with any spec group any more is still read by the two pairing gates from
// the recorded plan until the run closes.

// groupEdits are the operator's edits of the rollout block after the run
// recorded a group over the engine and the decoder.
var groupEdits = []struct {
	name  string
	apply func(spec *v1beta1.RolloutSpec)
}{
	{name: "the router added to the group", apply: func(spec *v1beta1.RolloutSpec) {
		spec.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}
	}},
	{name: "the decoder removed from the group", apply: func(spec *v1beta1.RolloutSpec) {
		spec.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
	}},
	{name: "the group removed", apply: func(spec *v1beta1.RolloutSpec) { spec.Groups = nil }},
}

// pairingGroupKinds are the two progressions a group over the engine and
// the decoder rolls under while the pairing gates pace a protocol change.
var pairingGroupKinds = []struct {
	name    string
	rolling bool
}{{name: "blueGreen"}, {name: "rollingUpdate", rolling: true}}

// pairingServiceUnder is pairingISVC with its group declared under the
// given progression and the run recorded over it.
func pairingServiceUnder(rolling bool, protocol string) *v1beta1.InferenceService {
	isvc := pairingISVC(protocol)
	if rolling {
		isvc.Spec.Rollout.Groups[0].BlueGreen = nil
		isvc.Spec.Rollout.Groups[0].RollingUpdate = &v1beta1.GroupRollingUpdate{}
		isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group = *isvc.Spec.Rollout.Groups[0].DeepCopy()
	}
	return isvc
}

// sixTwoLastOldDecoder is the picture of six engines and two decoders, both
// surging with none allowed out of pairing, half way through a protocol
// change from v1 to v2: the first three engines and the first decoder are
// on v2, the other three engines still run v1 with no replacement serving,
// and the second decoder's v2 replacement is Ready. Retiring that decoder's
// v1 pod would leave the three v1 engines without a partner, 3 of 6
// pairable under a pairable minimum of 6.
func sixTwoLastOldDecoder(isvc *v1beta1.InferenceService) []client.Object {
	objs := []client.Object{
		sizedReplica(isvc, v1beta1.EngineComponent, v1beta1.UpdateStrategySurgeThenDrain, 6, 0),
		sizedReplica(isvc, v1beta1.DecoderComponent, v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
	}
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 0, 3, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.EngineComponent, 3, 3, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 0, 1, 1, "v2", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 1, 1, 0, "v1", true)...)
	objs = append(objs, fleetPods(v1beta1.DecoderComponent, 1, 1, 1, "v2", true)...)
	return objs
}

// requireMinimumHeld reads a drain answer as the Pairing hold that keeps the
// engine side at its pairable minimum: 3 of 6 engines pairable under a
// minimum of 6.
func requireMinimumHeld(t *testing.T, allowed bool, gate v1beta1.RolloutHoldGate, reason, why string) {
	t.Helper()
	if allowed {
		t.Fatalf("%s: drain allowed (gate=%q reason=%q)", why, gate, reason)
	}
	if gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "engine with 3 pairable serving Instance(s), below its") || !strings.Contains(reason, "of 6 (desired 6 minus maxUnavailable 0)") {
		t.Errorf("%s: the hold must name the engine's shortfall against the recorded minimum under the Pairing gate; got gate=%q reason=%q", why, gate, reason)
	}
}

func TestEvaluatePairingDrain_AGroupEditMidRunKeepsThePairableMinimumOfAProtocolChange(t *testing.T) {
	dec := v1beta1.DecoderComponent
	source := []string{"llama-decoder-1-default-0"}
	for _, kind := range pairingGroupKinds {
		for _, edit := range groupEdits {
			t.Run(kind.name+"/"+edit.name, func(t *testing.T) {
				isvc := pairingServiceUnder(kind.rolling, "v2")
				fleet := sixTwoLastOldDecoder(isvc)
				allowed, gate, reason := drainGate(t, isvc, dec, source, fleet...)
				requireMinimumHeld(t, allowed, gate, reason, "the last v1 decoder under the recorded group")

				edit.apply(isvc.Spec.Rollout)
				allowed, gate, reason = drainGate(t, isvc, dec, source, fleet...)
				requireMinimumHeld(t, allowed, gate, reason, "the same drain after "+edit.name)

				gateCtx := ResolveGateContextWithDefaults(context.Background(), fakeclient.New(t, fakeclient.WithObjects(fleet...)), isvc, dec, GroupDefaults{})
				if gateCtx.ShortCircuit || gateCtx.Hold {
					t.Fatalf("after %s the decoder must still read a group for the pairing gates; got short=%v (%q) hold=%v", edit.name, gateCtx.ShortCircuit, gateCtx.ShortReason, gateCtx.Hold)
				}
				if got := gateCtx.Group.Components; len(got) != 2 || !groupPairsEngineAndDecoder(gateCtx.Group) {
					t.Fatalf("after %s the gates must read the members the run recorded; got %v", edit.name, got)
				}
				wantScope := CoordinationGroup
				if isvc.Spec.Rollout == nil || len(isvc.Spec.Rollout.Groups) == 0 {
					wantScope = RecordedGroupPairing
				}
				if gateCtx.Scope != wantScope {
					t.Fatalf("after %s the group's scope reads %d, want %d", edit.name, gateCtx.Scope, wantScope)
				}
			})
		}
	}

	t.Run("the recorded reading ends with the run", func(t *testing.T) {
		isvc := pairingServiceUnder(false, "v2")
		fleet := sixTwoLastOldDecoder(isvc)
		isvc.Spec.Rollout.Groups = nil
		isvc.Status.Rollout = nil
		allowed, _, reason := drainGate(t, isvc, dec, source, fleet...)
		if !allowed || reason != "no coordination groups" {
			t.Fatalf("with the run closed and no group declared nothing pairs the sides; got allowed=%v reason=%q", allowed, reason)
		}
	})
}

func TestCheckPairing_AGroupEditMidRunKeepsTheStartTimePairableMinimumOfAProtocolChange(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	for _, kind := range pairingGroupKinds {
		for _, edit := range groupEdits {
			t.Run(kind.name+"/"+edit.name, func(t *testing.T) {
				isvc := pairingServiceUnder(kind.rolling, "proto-b")
				halfway := []client.Object{
					pairingCR("prod", "llama", eng, "ea", "proto-a"),
					pairingCR("prod", "llama", eng, "eb", "proto-b"),
					pairingCR("prod", "llama", dec, "da", "proto-a"),
					pairingCR("prod", "llama", dec, "db", "proto-b"),
					sizedPairingIR("prod", "llama", eng, servingInstances("llama", eng, "eb", "ea"), v1beta1.UpdateStrategySurgeThenDrain, 2, 0),
					sizedPairingIR("prod", "llama", dec, servingInstances("llama", dec, "db", "da"), v1beta1.UpdateStrategyRecreatePod, 2, 1),
				}
				gate := pairingGate(t, isvc, dec, halfway...)
				if ok, reason := gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0); ok || !strings.Contains(reason, "engine with 1 pairable") {
					t.Fatalf("recreating the last proto-a decoder must wait while an engine still speaks only proto-a; got ok=%v reason=%q", ok, reason)
				}
				edit.apply(isvc.Spec.Rollout)
				gate = pairingGate(t, isvc, dec, halfway...)
				ok, reason := gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
				if ok || !strings.Contains(reason, "engine with 1 pairable") {
					t.Fatalf("after %s the same start must still wait on the engine's pairable minimum; got ok=%v reason=%q", edit.name, ok, reason)
				}
			})
		}
	}
}
