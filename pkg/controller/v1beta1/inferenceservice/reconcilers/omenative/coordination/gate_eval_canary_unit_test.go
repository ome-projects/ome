package coordination

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A member of a canary unit, a canary group that lists the engine and the
// decoder, is staged by its canary step and paced by its own budget. Of
// the coordination gates the pairing gate and the batch step read the
// unit, the way they read a blueGreen group: the ratio band, the group
// budgets and the Sequential gate do not.
func TestEvaluateUpdateGate_CanaryUnitConsultsThePairingGateAndTheBatchStep(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	members := map[v1beta1.ComponentType]batchMemberFixture{
		eng: {desired: 2, surge: "1", strategy: v1beta1.UpdateStrategySurgeThenDrain},
		dec: {desired: 1, surge: "1", strategy: v1beta1.UpdateStrategySurgeThenDrain},
	}
	grouped := batchService(batchGroupFixture{blueGreen: true}, members)
	unit := canaryUnitService()
	c := batchClient(t, members)

	// The engine's first surge waits for the decoder's batch under both
	// group kinds, with the same words.
	wait := "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)"
	for name, isvc := range map[string]*v1beta1.InferenceService{"blueGreen group": grouped, "canary unit": unit} {
		allowed, gate, reason := batchGateWith(t, isvc, c, eng, workloadtypes.UpdateStrategySurgeThenDrain, 0, keepRatioDecoderFirst(5))
		if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != wait {
			t.Fatalf("%s: the engine's first surge waits for the decoder's batch; got allowed=%v gate=%q reason=%q", name, allowed, gate, reason)
		}
	}
	// The decoder's first batch opens. A second surge beside an open one
	// would spend the group's surge budget of one: the blueGreen group holds
	// it under Budget, the unit's member answers to its own budget alone.
	if allowed, gate, reason := batchGateWith(t, grouped, c, dec, workloadtypes.UpdateStrategySurgeThenDrain, 1, keepRatioDecoderFirst(5)); allowed || gate != v1beta1.RolloutHoldGateBudget {
		t.Fatalf("blueGreen group: a second surge spends the group surge budget; got allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}
	if allowed, gate, reason := batchGateWith(t, unit, c, dec, workloadtypes.UpdateStrategySurgeThenDrain, 1, keepRatioDecoderFirst(5)); !allowed {
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

// A prefill/decode canary unit is paced by the batch step. The canary
// executor holds each member's partition and waits for the step's capacity;
// inside the step, and inside a revert, the two sides' starts are batched
// under the chart's settings, the decoder first, over the Instances the
// partition lets move. The pictures below are the reference shapes 30:10
// and 12:4 at 10 percent under RecreatePod, and the revert of 30:10 from
// its final step.

// canaryUnitService is a service whose engine and decoder roll as one
// canary unit, with the run that records the plan open.
func canaryUnitService(components ...v1beta1.ComponentType) *v1beta1.InferenceService {
	if len(components) == 0 {
		components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: batchServiceName, Namespace: batchNamespace},
		Spec: v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
			Components: components,
			Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("10%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			}},
		}}}},
	}
	return pinActiveRun(isvc)
}

// canaryStaged is one member's replica under a canary step: its own
// maxUnavailable of 10 percent under RecreatePod, the step's partition in
// spec.pacing, promoted Instances on the step's revision and attempts
// toward it in flight.
func canaryStaged(comp v1beta1.ComponentType, desired, partition, promoted, inFlight int32) *v1beta1.InferenceReplica {
	ir := batchReplica(comp, batchMemberFixture{desired: desired, step: "10%", strategy: v1beta1.UpdateStrategyRecreatePod, promoted: promoted, inFlight: inFlight})
	ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: ptr.To(partition)}
	return ir
}

// canaryReverting is one member's replica under a canary rollback: the
// partition at zero, the stable revision set as the rollback target,
// stillNew Instances on the rejected revision, back Instances on the stable
// one, and reverts in flight toward it.
func canaryReverting(comp v1beta1.ComponentType, desired, back, reverting int32) *v1beta1.InferenceReplica {
	ir := batchReplica(comp, batchMemberFixture{desired: desired, step: "10%", strategy: v1beta1.UpdateStrategyRecreatePod})
	stable, rejected := batchRevision(comp, "old"), batchRevision(comp, "target")
	ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{Partition: ptr.To(int32(0)), RollbackToRevision: ptr.To(stable)}
	rows := ir.Status.InstanceStatuses
	for i := range rows {
		switch {
		case int32(i) < back:
			rows[i].RunningRevision = stable
		case int32(i) < back+reverting:
			rows[i].Phase, rows[i].RunningRevision, rows[i].TargetRevision = v1beta1.OMENativeInstanceUpdating, rejected, stable
			rows[i].PodCount, rows[i].ServingPodCount = 1, 0
			rows[i].Operation = &v1beta1.InstanceOperation{ID: "revert", Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepDrain, TargetRevision: stable}
		default:
			rows[i].RunningRevision = rejected
		}
	}
	return ir
}

func canaryUnitClient(t *testing.T, replicas ...*v1beta1.InferenceReplica) client.Client {
	t.Helper()
	objs := make([]client.Object, 0, len(replicas))
	for _, ir := range replicas {
		objs = append(objs, ir)
	}
	return fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).Build()
}

// canaryUnitGate asks the production gate about one more recreate of comp
// toward revision under the chart's settings at the default tolerance of 5.
func canaryUnitGate(t *testing.T, isvc *v1beta1.InferenceService, c client.Client, comp v1beta1.ComponentType, revision string) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: batchRevision(comp, revision)}}
	return EvaluateUpdateGate(context.Background(), c, isvc, comp, batchRevision(comp, "old"), target, nil, keepRatioDecoderFirst(5), workloadtypes.UpdateStrategyRecreatePod, 0, 0)
}

func TestEvaluateUpdateGate_ACanaryStepMovesTheDecoderBeforeTheEngines(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	cases := []struct {
		name                    string
		engine, decoder         *v1beta1.InferenceReplica
		engineHeld, decoderHeld bool
		engineWait, decoderWait string
	}{
		{
			name:   "30:10 at 10 percent, nothing started: the decoder moves first",
			engine: canaryStaged(eng, 30, 27, 0, 0), decoder: canaryStaged(dec, 10, 9, 0, 0),
			engineHeld: true, engineWait: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving, 1 of 10 movable)",
		},
		{
			name:   "30:10, the decoder's Instance serves: the engines start",
			engine: canaryStaged(eng, 30, 27, 0, 0), decoder: canaryStaged(dec, 10, 9, 1, 0),
			decoderHeld: true, decoderWait: "decoder batch 2 waits for engine batch 1 to serve (0 of 3 serving, 3 of 30 movable)",
		},
		{
			name:   "30:10, two engines moving beside the served decoder: the third starts",
			engine: canaryStaged(eng, 30, 27, 0, 2), decoder: canaryStaged(dec, 10, 9, 1, 0),
			decoderHeld: true, decoderWait: "decoder batch 2 waits for engine batch 1 to serve (0 of 3 serving, 3 of 30 movable)",
		},
		{
			name:   "30:10, three engines moving: the step is staged, nothing more opens",
			engine: canaryStaged(eng, 30, 27, 0, 3), decoder: canaryStaged(dec, 10, 9, 1, 0),
			engineHeld: true, engineWait: "engine batch 2 waits for engine batch 1 to serve (0 of 3 serving, 3 of 30 movable)",
			decoderHeld: true, decoderWait: "decoder batch 2 waits for engine batch 1 to serve (0 of 3 serving, 3 of 30 movable)",
		},
		{
			name:   "12:4 at 10 percent, nothing started: the decoder moves first",
			engine: canaryStaged(eng, 12, 10, 0, 0), decoder: canaryStaged(dec, 4, 3, 0, 0),
			engineHeld: true, engineWait: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving, 1 of 4 movable)",
		},
		{
			name:   "12:4, the decoder's Instance serves: the two engines start",
			engine: canaryStaged(eng, 12, 10, 0, 0), decoder: canaryStaged(dec, 4, 3, 1, 0),
			decoderHeld: true, decoderWait: "decoder batch 2 waits for engine batch 1 to serve (0 of 2 serving, 2 of 12 movable)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := canaryUnitService()
			c := canaryUnitClient(t, tc.engine, tc.decoder)
			for _, side := range []struct {
				comp v1beta1.ComponentType
				held bool
				wait string
			}{{eng, tc.engineHeld, tc.engineWait}, {dec, tc.decoderHeld, tc.decoderWait}} {
				allowed, gate, reason := canaryUnitGate(t, isvc, c, side.comp, "target")
				if allowed == side.held {
					t.Errorf("%s: allowed=%v gate=%s reason=%q; wanted held=%v", side.comp, allowed, gate, reason, side.held)
					continue
				}
				if side.held && (gate != v1beta1.RolloutHoldGateRatio || reason != side.wait) {
					t.Errorf("%s: held under gate %s with %q; wanted the Ratio gate with %q", side.comp, gate, reason, side.wait)
				}
			}
		})
	}
}

func TestEvaluateUpdateGate_ACanaryRevertMovesBothSidesTogether(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	cases := []struct {
		name                    string
		engine, decoder         *v1beta1.InferenceReplica
		engineHeld, decoderHeld bool
		engineWait, decoderWait string
	}{
		{
			name:   "30:10 rolled back at the final step, one Instance per side back: two more engines revert first",
			engine: canaryReverting(eng, 30, 1, 0), decoder: canaryReverting(dec, 10, 1, 0),
			decoderHeld: true, decoderWait: "decoder batch 2 waits for engine batch 1 to serve (1 of 3 serving)",
		},
		{
			name:   "three engines back: the decoder's second revert opens and the engines wait for it",
			engine: canaryReverting(eng, 30, 3, 0), decoder: canaryReverting(dec, 10, 1, 0),
			engineHeld: true, engineWait: "engine batch 2 waits for decoder batch 2 to serve first (1 of 2 serving)",
		},
		{
			name:   "three engines and two decoders back: the next three engines revert",
			engine: canaryReverting(eng, 30, 3, 0), decoder: canaryReverting(dec, 10, 2, 0),
			decoderHeld: true, decoderWait: "decoder batch 3 waits for engine batch 2 to serve (3 of 6 serving)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := canaryUnitService()
			c := canaryUnitClient(t, tc.engine, tc.decoder)
			for _, side := range []struct {
				comp v1beta1.ComponentType
				held bool
				wait string
			}{{eng, tc.engineHeld, tc.engineWait}, {dec, tc.decoderHeld, tc.decoderWait}} {
				allowed, gate, reason := canaryUnitGate(t, isvc, c, side.comp, "old")
				if allowed == side.held {
					t.Errorf("%s: allowed=%v gate=%s reason=%q; wanted held=%v", side.comp, allowed, gate, reason, side.held)
					continue
				}
				if side.held && (gate != v1beta1.RolloutHoldGateRatio || reason != side.wait) {
					t.Errorf("%s: held under gate %s with %q; wanted the Ratio gate with %q", side.comp, gate, reason, side.wait)
				}
			}
		})
	}
}
