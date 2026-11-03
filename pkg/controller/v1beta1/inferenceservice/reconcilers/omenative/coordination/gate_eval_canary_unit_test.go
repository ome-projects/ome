package coordination

import (
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A member of a canary unit, a canary group that lists the engine and the
// decoder, is paced by its canary step and its own budget. Of the
// coordination gates only the pairing gate reads the unit: the batch step,
// the ratio band, the group budgets and the Sequential gate do not. The
// same picture under a blueGreen group is held by the batch step.
func TestEvaluateUpdateGate_CanaryUnitConsultsThePairingGateAlone(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	members := map[v1beta1.ComponentType]batchMemberFixture{
		eng: {desired: 2, surge: "1", strategy: v1beta1.UpdateStrategySurgeThenDrain},
		dec: {desired: 1, surge: "1", strategy: v1beta1.UpdateStrategySurgeThenDrain},
	}
	grouped := batchService(batchGroupFixture{blueGreen: true}, members)
	unit := grouped.DeepCopy()
	unit.Spec.Rollout.Groups[0] = v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{eng, dec}, Canary: &v1beta1.GroupCanary{}}
	unit = pinActiveRun(unit)
	c := batchClient(t, members)

	if allowed, gate, reason := batchGateWith(t, grouped, c, eng, workloadtypes.UpdateStrategySurgeThenDrain, 0, keepRatioDecoderFirst(5)); allowed || gate != v1beta1.RolloutHoldGateRatio {
		t.Fatalf("blueGreen group: the engine's first surge waits for the decoder's batch; got allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}
	if allowed, gate, reason := batchGateWith(t, unit, c, eng, workloadtypes.UpdateStrategySurgeThenDrain, 0, keepRatioDecoderFirst(5)); !allowed {
		t.Fatalf("canary unit: the batch step and the group budgets are not consulted; held by gate=%q reason=%q", gate, reason)
	}
	// A second surge beside the open one would spend the group's surge
	// budget of one; the unit's member answers to its own budget alone.
	if allowed, gate, reason := batchGateWith(t, unit, c, eng, workloadtypes.UpdateStrategySurgeThenDrain, 1, keepRatioDecoderFirst(5)); !allowed {
		t.Fatalf("canary unit: the group surge budget is not consulted; held by gate=%q reason=%q", gate, reason)
	}

	// A pairing-protocol change is paced through the same decision site:
	// the last proto-a engine waits for a proto-b decoder.
	changing := []client.Object{
		pairingIR("prod", "llama", eng, servingInstances("llama", eng, "ea", "eb")),
		pairingCR("prod", "llama", eng, "ea", "proto-a"),
		pairingCR("prod", "llama", eng, "eb", "proto-b"),
		pairingIR("prod", "llama", dec, servingInstances("llama", dec, "da")),
		pairingCR("prod", "llama", dec, "da", "proto-a"),
	}
	isvc := canaryPairingISVC("proto-b")
	reads := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(changing...).Build()
	allowed, gate, reason := EvaluateUpdateGate(context.Background(), reads, isvc, eng, "", forwardTarget(isvc, eng), nil, keepRatioDecoderFirst(5), workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "would leave no pairable serving engine+decoder pair") {
		t.Fatalf("canary unit: the pairing gate holds the last proto-a engine; got allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}
}
