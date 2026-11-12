package coordination

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// canaryPairingISVC declares the engine and the decoder in one canary group
// with the given target protocol, the other shape admission lets a
// pairing-protocol change roll in; the active run records that group, since
// the gates read the recorded plan.
func canaryPairingISVC(protocol string) *v1beta1.InferenceService {
	isvc := pairingISVC(protocol)
	isvc.Spec.Rollout.Groups[0].BlueGreen = nil
	isvc.Spec.Rollout.Groups[0].Canary = &v1beta1.GroupCanary{}
	return pinActiveRun(isvc)
}

// withLifecycle gives a pairing replica its desired count, its update
// strategy and its own maxUnavailable, the budget a canary unit member's
// pairable minimum is built from.
func withLifecycle(ir *v1beta1.InferenceReplica, desired int32, strategy v1beta1.UpdateStrategyType, maxUnavailable int32) *v1beta1.InferenceReplica {
	ir.Spec.Replicas = ptr.To(desired)
	ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
		Type:          strategy,
		RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: ptr.To(intstr.FromInt32(maxUnavailable))},
	}}
	return ir
}

// pairingGateWith builds the gate for a forward-roll step of component under
// the operator's group defaults.
func pairingGateWith(t *testing.T, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, defaults GroupDefaults, objs ...client.Object) GateContext {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).Build()
	gate := ResolveGateContextWithDefaults(context.Background(), c, isvc, component, defaults)
	gate.TargetRevision = forwardTarget(isvc, component)
	return gate
}

// A pairing-protocol change under a canary group that lists engine and
// decoder keeps the pair the way a blueGreen group does: the last
// old-protocol engine is not drained while no new-protocol decoder serves.
func TestCheckPairing_CanaryGroupHoldsTheLastOldEngineUntilATargetDecoderServes(t *testing.T) {
	isvc := canaryPairingISVC("proto-b")
	objs := []client.Object{
		pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "eb")),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
		pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	gate := pairingGate(t, isvc, v1beta1.EngineComponent, objs...)
	ok, reason := gate.CheckPairing("", workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if ok {
		t.Fatalf("canary group: draining the last proto-a engine with no proto-b decoder must be denied; got allowed (%s)", reason)
	}
	if !strings.Contains(reason, `"proto-a"`) || !strings.Contains(reason, `"proto-b"`) {
		t.Errorf("denial must name the dying cohort and the target: %s", reason)
	}
}

// The drain-time counterpart: under the same canary group the engine's old pod
// stays in rotation while its replacement serves proto-b and no decoder
// does, and leaves once one serves.
func TestEvaluatePairingDrain_CanaryGroupKeepsTheOldEnginePodUntilATargetDecoderServes(t *testing.T) {
	isvc := canaryPairingISVC("proto-b")
	oldEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-default-0", "default", "proto-a", true)
	newEngine := drainPod("llama", v1beta1.EngineComponent, "llama-engine-0-default-1", "default", "proto-b", true)
	oldDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-0", "default", "proto-a", true)

	allowed, gate, reason := drainGate(t, isvc, v1beta1.EngineComponent, []string{oldEngine.Name}, oldEngine, newEngine, oldDecoder)
	if allowed {
		t.Fatalf("canary group: the old proto-a engine pod must stay in rotation while no decoder serves proto-b; got allowed (%s)", reason)
	}
	if gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, `no serving decoder runs cohort "proto-b" yet`) {
		t.Errorf("the hold must be the Pairing gate's, naming the missing partner; got gate=%q reason=%q", gate, reason)
	}
	newDecoder := drainPod("llama", v1beta1.DecoderComponent, "llama-decoder-0-default-1", "default", "proto-b", true)
	if allowed, _, reason := drainGate(t, isvc, v1beta1.EngineComponent, []string{oldEngine.Name}, oldEngine, newEngine, oldDecoder, newDecoder); !allowed {
		t.Fatalf("with a proto-b decoder serving the replacement pairs; the drain must proceed: %s", reason)
	}
}

// A canary group carries no budget of its own, so a member's pairable
// minimum is its desired count less its own maxUnavailable. Two engines of four serve
// the old protocol beside the last old decoder: an engine that may let two
// Instances go admits the decoder's rebuild, one that may let none go holds
// it.
func TestCheckPairing_CanaryGroupPairableMinimumIsTheComponentsOwnBudget(t *testing.T) {
	for _, tc := range []struct {
		name           string
		maxUnavailable int32
		wantHold       bool
	}{
		{name: "maxUnavailable 0 keeps every engine paired", maxUnavailable: 0, wantHold: true},
		{name: "maxUnavailable 2 keeps two engines paired", maxUnavailable: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := canaryPairingISVC("proto-b")
			objs := []client.Object{
				withLifecycle(pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea", "ea", "eb", "eb")), 4, v1beta1.UpdateStrategySurgeThenDrain, tc.maxUnavailable),
				pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
				pairingCR("prod", "llama", v1beta1.EngineComponent, "eb", "proto-b"),
				withLifecycle(pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da", "db", "db", "db")), 4, v1beta1.UpdateStrategyRecreatePod, 1),
				pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
				pairingCR("prod", "llama", v1beta1.DecoderComponent, "db", "proto-b"),
			}
			gate := pairingGate(t, isvc, v1beta1.DecoderComponent, objs...)
			ok, reason := gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
			if ok == tc.wantHold {
				t.Fatalf("allowed=%v, want hold=%v (%s)", ok, tc.wantHold, reason)
			}
			if tc.wantHold && !strings.Contains(reason, "below its floor of 4 (desired 4 minus maxUnavailable 0)") {
				t.Errorf("the hold must name the engine's own budget: %s", reason)
			}
		})
	}
}

// A pairing-protocol change under a canary group that lists engine and
// decoder is ordered the way a blueGreen group's is: the batch step moves
// the named side first inside every batch, so the pairing gate lets the
// named decoder's rebuild go first while the engine's surge keeps the pair,
// under the chart's decoder-first setting, in both group kinds. With no
// side named the gate orders a pair down to one Instance each itself: the
// decoder's rebuild waits while the engine's surge keeps the pair.
func TestCheckPairing_CanaryGroupReadsTheFirstSideLikeABlueGreenGroup(t *testing.T) {
	objs := []client.Object{
		withLifecycle(pairingIR("prod", "llama", v1beta1.EngineComponent, servingInstances("llama", v1beta1.EngineComponent, "ea")), 1, v1beta1.UpdateStrategySurgeThenDrain, 0),
		pairingCR("prod", "llama", v1beta1.EngineComponent, "ea", "proto-a"),
		withLifecycle(pairingIR("prod", "llama", v1beta1.DecoderComponent, servingInstances("llama", v1beta1.DecoderComponent, "da")), 1, v1beta1.UpdateStrategyRecreatePod, 1),
		pairingCR("prod", "llama", v1beta1.DecoderComponent, "da", "proto-a"),
	}
	ask := func(isvc *v1beta1.InferenceService, defaults GroupDefaults) (bool, string) {
		gate := pairingGateWith(t, isvc, v1beta1.DecoderComponent, defaults, objs...)
		return gate.CheckPairing("llama-decoder-da", workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	}
	for name, isvc := range map[string]*v1beta1.InferenceService{"blueGreen group": pairingISVC("proto-b"), "canary group": canaryPairingISVC("proto-b")} {
		if ok, reason := ask(isvc, keepRatioDecoderFirst(5)); !ok {
			t.Fatalf("%s: the batch step orders the sides, so the named decoder goes first; got held (%s)", name, reason)
		}
	}
	ok, reason := ask(canaryPairingISVC("proto-b"), keepRatio(5))
	if ok {
		t.Fatalf("canary group, no side named: the decoder's rebuild must wait for the engine's surge; got allowed (%s)", reason)
	}
	if !strings.Contains(reason, "the engine surges, keeping its pod in rotation") {
		t.Errorf("the hold must say the engine's surge keeps the pair: %s", reason)
	}
}
