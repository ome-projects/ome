package coordination

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// A pairing-protocol change, pair-v1 to pair-v2, on a service whose
// prefill side rebuilds its Instances (RecreatePod) while its decode side
// surges, under the operator's decoder-first setting. The decode side
// moves first, so its first replacement serves before any prefill pod of
// the new protocol exists. The old decode pod stays in rotation until a
// prefill pod of the new protocol serves, so every decoder keeps a partner
// at every moment; the prefill's first start opens beside the kept pod,
// because the start-time gate reads the decoder as held still and the
// batch step does not wait on it; once the first prefill serves pair-v2
// the replacement pairs and the old pod leaves. Three shapes: 2 prefill
// beside 2 decode with no decoder allowed out of pairing, 2 prefill beside
// 6 decode surging three at a time, and 12 prefill rebuilt three at a time
// beside 8 decode surging two at a time with none allowed out.
func TestEvaluatePairingDrain_TheOldDecodePodStaysUntilAPrefillOfTheNewProtocolServes(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge, recreate := v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod
	decoderFirst := GroupDefaults{PrefillDecode: PrefillDecodeRules{FirstSide: dec}}
	// surging sets a surging replica's own maxSurge and marks the first
	// Instances as surges whose replacements serve beside their old pods.
	surging := func(ir *v1beta1.InferenceReplica, maxSurge int, handOvers int) *v1beta1.InferenceReplica {
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(maxSurge))
		for i := 0; i < handOvers; i++ {
			ir.Status.InstanceStatuses[i] = surgeReplacementServingRow(dec, int32(i))
		}
		return ir
	}
	// decodePods are the decode side's pods: the first handOvers Instances
	// serve pair-v1 and pair-v2 at once, the rest pair-v1.
	decodePods := func(total, handOvers int) []client.Object {
		out := cohortPods(dec, nil, indexes(handOvers, total-1), -1)
		for i := 0; i < handOvers; i++ {
			out = append(out, cohortPods(dec, nil, nil, i)...)
		}
		return out
	}
	cases := []struct {
		name         string
		prefill      int
		step         int // the prefill's own maxUnavailable
		decode       int
		surgeStep    int // the decode's own maxSurge
		outOfPairing int // the decode's own maxUnavailable
		handOvers    int // decode Instances whose replacement serves
	}{
		{name: "2 prefill rebuilt one at a time beside 2 decode with none allowed out of pairing", prefill: 2, step: 1, decode: 2, surgeStep: 1, outOfPairing: 0, handOvers: 1},
		{name: "2 prefill rebuilt one at a time beside 6 decode surging three at a time with one allowed out", prefill: 2, step: 1, decode: 6, surgeStep: 3, outOfPairing: 1, handOvers: 3},
		{name: "12 prefill rebuilt three at a time beside 8 decode surging two at a time with none allowed out", prefill: 12, step: 3, decode: 8, surgeStep: 2, outOfPairing: 0, handOvers: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := pairingISVC("pair-v2")
			source := []string{"llama-decoder-0-default-0"}
			beforeAnyPrefillServes := append(threeCohortCRs("pair-v2"),
				rolledPairingIR(eng, recreate, tc.prefill, tc.step, revisionsOf(0, tc.prefill)...),
				surging(rolledPairingIR(dec, surge, tc.decode, tc.outOfPairing, revisionsOf(0, tc.decode)...), tc.surgeStep, tc.handOvers))
			pods := append(cohortPods(eng, nil, indexes(0, tc.prefill-1), -1), decodePods(tc.decode, tc.handOvers)...)

			allowed, gate, reason := drainGate(t, isvc, dec, source, append(append([]client.Object{}, beforeAnyPrefillServes...), pods...)...)
			if allowed || gate != v1beta1.RolloutHoldGatePairing {
				t.Fatalf("no prefill serves pair-v2 yet, so the old decode pod must stay in rotation: allowed=%v gate=%q reason=%q", allowed, gate, reason)
			}
			if !strings.Contains(reason, `the replacement decoder cannot pair because no serving engine runs cohort "pair-v2" yet`) {
				t.Fatalf("the hold must say the replacement waits for a prefill of the new protocol: %s", reason)
			}

			allowed, gate, reason = decoderFirstStart(t, isvc, decoderFirst, eng, workloadtypes.UpdateStrategyRecreatePod, beforeAnyPrefillServes...)
			if !allowed {
				t.Fatalf("the prefill's first rebuild must open beside the kept decode pod, which leaves only once a prefill serves pair-v2; held under %s: %s", gate, reason)
			}

			afterOnePrefillServes := append(threeCohortCRs("pair-v2"),
				rolledPairingIR(eng, recreate, tc.prefill, tc.step, revisionsOf(1, tc.prefill)...),
				surging(rolledPairingIR(dec, surge, tc.decode, tc.outOfPairing, revisionsOf(0, tc.decode)...), tc.surgeStep, tc.handOvers))
			pods = append(cohortPods(eng, []int{0}, indexes(1, tc.prefill-1), -1), decodePods(tc.decode, tc.handOvers)...)
			if allowed, _, reason := drainGate(t, isvc, dec, source, append(append([]client.Object{}, afterOnePrefillServes...), pods...)...); !allowed {
				t.Fatalf("once a prefill serves pair-v2 the replacement pairs and the old decode pod may leave: %s", reason)
			}
		})
	}
}

// decoderFirstStart asks the whole gate stack, under the given operator
// defaults, about the next start of component from the pair-v1 revision.
func decoderFirstStart(t *testing.T, isvc *v1beta1.InferenceService, defaults GroupDefaults, component v1beta1.ComponentType, strategy workloadtypes.UpdateStrategyType, objs ...client.Object) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithObjects(objs...))
	return EvaluateUpdateGate(context.Background(), c, isvc, component, "llama-"+string(component)+"-v1", forwardTarget(isvc, component), nil, defaults, strategy, 0, 0)
}
