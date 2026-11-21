package coordination

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A service of one prefill patched in place beside one decoder that
// surges changes its pairing protocol twice: pair-v1 to pair-v2, and to
// pair-v3 the moment the first new decode pod serves. The prefill's patch
// flips its only pod to the newest protocol in one step, so it may run
// only when a decoder already serves that protocol; a prefill on pair-v3
// beside a decoder on pair-v2 would leave the service with no pair until
// the decoder's next replacement serves. The old decode pod is what keeps
// the pair through both changes: the drain gate holds it while no
// prefill serves the protocol its replacement speaks, and the start-time
// gate holds the prefill's patch while the patch would leave no pair.
//
// The walk, picture by picture: the first replacement serves pair-v2
// beside the kept pod and the prefill's patch to pair-v2 is admitted; the
// second change lands first, so the prefill's patch, now to pair-v3, is
// held, the old pod stays, and the replacement on pair-v2, which pairs with
// nothing, may leave; the decoder's surge toward pair-v3 is admitted on the
// settled pair; the pair-v3 replacement serves beside the kept pod and the
// prefill's patch is admitted while the old pod still stays; the prefill
// serves pair-v3 and the old pod may leave. On the settled pair, with
// nothing in flight, the decoder's surge is the one start that keeps the
// pair, so the prefill's patch waits for it: behind the decoder's batch
// when the decoder is named first, and under the pairing gate when no side
// is.
func TestPairingGates_SecondChangeOnOneByOne_PrefillWaitsForADecoderOfTheFinalProtocol(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	inPlace, surge := v1beta1.UpdateStrategyInPlaceIfPossible, v1beta1.UpdateStrategySurgeThenDrain
	oldPod, replacement := "llama-decoder-0-default-0", "llama-decoder-0-default-1"
	chart := GroupDefaults{RatioTolerancePercent: ptr.To(int32(5)), PrefillDecode: PrefillDecodeRules{KeepRatio: true, FirstSide: dec}}

	// decoder is the surging side's replica: one Instance, maxSurge 1, none
	// allowed out of pairing, its row as given.
	decoder := func(row v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplica {
		ir := rolledPairingIR(dec, surge, 1, 0, "v1")
		ir.Spec.Lifecycle.UpdateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt(1))
		ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{row}
		return ir
	}
	// prefill is the in-place side's replica: one Instance serving the
	// given revision, maxUnavailable 1.
	prefill := func(hash string) *v1beta1.InferenceReplica {
		return rolledPairingIR(eng, inPlace, 1, 1, hash)
	}
	pods := func(prefillProto, replacementProto string) []client.Object {
		out := []client.Object{
			drainPod("llama", eng, "llama-engine-0-default-0", "default", prefillProto, true),
			drainPod("llama", dec, oldPod, "default", "pair-v1", true),
		}
		if replacementProto != "" {
			out = append(out, drainPod("llama", dec, replacement, "default", replacementProto, true))
		}
		return out
	}
	picture := func(isvc *v1beta1.InferenceService, prefillHash, prefillProto string, row v1beta1.OMENativeInstanceStatus, replacementProto string) []client.Object {
		objs := append(threeCohortCRs(isvc.Spec.RolloutPairingProtocol()), prefill(prefillHash), decoder(row))
		return append(objs, pods(prefillProto, replacementProto)...)
	}
	requireNoPairHold := func(allowed bool, gate v1beta1.RolloutHoldGate, reason, why string) {
		t.Helper()
		if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "would leave no pairable serving engine+decoder pair") {
			t.Fatalf("%s: allowed=%v gate=%q reason=%q", why, allowed, gate, reason)
		}
	}

	// The first change: the pair-v2 replacement serves beside the kept pod.
	first := pairingISVC("pair-v2")
	replacementOnV2 := surgingRow("llama", dec, 0, "v1", "v2", workloadtypes.UpdateStepSurge, 2)
	objs := picture(first, "v1", "pair-v1", replacementOnV2, "pair-v2")
	allowed, gate, reason := drainGate(t, first, dec, []string{oldPod}, objs...)
	requireHeld(t, allowed, gate, reason, "first change: no prefill serves pair-v2, so the old decode pod stays")
	if allowed, gate, reason := decoderFirstStart(t, first, chart, eng, workloadtypes.UpdateStrategyInPlaceIfPossible, objs...); !allowed {
		t.Fatalf("first change: a decoder serves pair-v2, so the prefill's patch to pair-v2 may run beside the kept pod; held under %s: %s", gate, reason)
	}

	// The second change lands before the prefill's patch: the prefill is
	// held, the old pod stays, and the replacement may leave. A replacement
	// the roll leaves behind does not count, so the patch is answered on the
	// pair that stays, as on the settled pair below: it waits behind the
	// decoder's batch when the decoder is named first, and under the pairing
	// gate when no side is.
	second := pairingISVC("pair-v3")
	objs = picture(second, "v1", "pair-v1", replacementOnV2, "pair-v2")
	if allowed, gate, reason := decoderFirstStart(t, second, chart, eng, workloadtypes.UpdateStrategyInPlaceIfPossible, objs...); allowed || gate != v1beta1.RolloutHoldGateRatio {
		t.Fatalf("second change: no decoder serves pair-v3, so the prefill's patch to pair-v3 must wait for the decoder's first batch: allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}
	allowed, gate, reason = decoderFirstStart(t, second, GroupDefaults{}, eng, workloadtypes.UpdateStrategyInPlaceIfPossible, objs...)
	requireNoPairHold(allowed, gate, reason, "second change with no side named first: no decoder serves pair-v3, so the prefill's patch to pair-v3 must wait")
	allowed, gate, reason = drainGate(t, second, dec, []string{oldPod}, objs...)
	requireHeld(t, allowed, gate, reason, "second change: the replacement on pair-v2 has no partner, so the old decode pod stays")
	if !strings.Contains(reason, `"pair-v2"`) {
		t.Errorf("the hold must name the replacement's own protocol, pair-v2, as the one no prefill serves: %s", reason)
	}
	if allowed, _, reason := drainGate(t, second, dec, []string{replacement}, objs...); !allowed {
		t.Fatalf("second change: the pair-v2 replacement pairs with nothing and the pair-v1 pods keep the only pair, so the surge may remove it: %s", reason)
	}

	// The replacement is gone and the decoder surges again, toward pair-v3,
	// from the settled pair; the prefill still waits.
	settled := settledRow("llama", dec, 0, "v1")
	objs = picture(second, "v1", "pair-v1", settled, "")
	if allowed, gate, reason := decoderFirstStart(t, second, chart, dec, workloadtypes.UpdateStrategySurgeThenDrain, objs...); !allowed {
		t.Fatalf("second change: the decoder's surge toward pair-v3 keeps the pair-v1 pod serving until its replacement pairs, so it may open; held under %s: %s", gate, reason)
	}
	// On the settled pair the pairing gate's one-by-one escape admits either
	// side's step, and the first-side rule is what orders them: the decoder
	// named first, the prefill's patch waits for the decoder's batch.
	if allowed, gate, reason := decoderFirstStart(t, second, chart, eng, workloadtypes.UpdateStrategyInPlaceIfPossible, objs...); allowed || gate != v1beta1.RolloutHoldGateRatio {
		t.Fatalf("second change: still no decoder serves pair-v3, so the prefill's patch must wait for the decoder's first batch: allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}
	if allowed, gate, reason := decoderFirstStart(t, second, GroupDefaults{}, eng, workloadtypes.UpdateStrategyInPlaceIfPossible, objs...); allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "the decoder surges") {
		t.Fatalf("second change with no side named first: the prefill's patch waits under the Pairing gate for the decoder's surge, which keeps the pair: allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}

	// The pair-v3 replacement serves beside the kept pod: the prefill's
	// patch may run, and the old pod stays until it has.
	replacementOnV3 := surgingRow("llama", dec, 0, "v1", "target", workloadtypes.UpdateStepSurge, 2)
	objs = picture(second, "v1", "pair-v1", replacementOnV3, "pair-v3")
	if allowed, gate, reason := decoderFirstStart(t, second, chart, eng, workloadtypes.UpdateStrategyInPlaceIfPossible, objs...); !allowed {
		t.Fatalf("second change: a decoder serves pair-v3, so the prefill's patch to pair-v3 may run beside the kept pod; held under %s: %s", gate, reason)
	}
	allowed, gate, reason = drainGate(t, second, dec, []string{oldPod}, objs...)
	requireHeld(t, allowed, gate, reason, "second change: no prefill serves pair-v3 yet, so the old decode pod stays")
	if !strings.Contains(reason, `"pair-v3"`) {
		t.Errorf("the hold must name pair-v3 as the protocol no prefill serves: %s", reason)
	}

	// The prefill serves pair-v3: the old pod may leave.
	objs = picture(second, "target", "pair-v3", replacementOnV3, "pair-v3")
	if allowed, _, reason := drainGate(t, second, dec, []string{oldPod}, objs...); !allowed {
		t.Fatalf("second change: the prefill serves pair-v3, so the replacement pairs and the old decode pod may leave: %s", reason)
	}
}
