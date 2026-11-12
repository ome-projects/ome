package coordination

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A resize while the ratio rule paces a prefill/decode roll. The paired
// plan is made again on every pass from the counts the specs ask for now,
// so a side scaled mid-roll reads its batches off the new plan, and the
// side named first keeps leading inside each batch of that plan.

// resizeAnswer is what the gate answers one side's next start.
type resizeAnswer struct {
	gate   v1beta1.RolloutHoldGate // empty: admitted
	reason string
}

func (a resizeAnswer) check(t *testing.T, side string, allowed bool, gate v1beta1.RolloutHoldGate, reason string) {
	t.Helper()
	if a.gate == "" {
		if !allowed {
			t.Errorf("%s: held under %q with %q, want its next start admitted", side, gate, reason)
		}
		return
	}
	if allowed || gate != a.gate || reason != a.reason {
		t.Errorf("%s: allowed=%v gate=%q reason=%q, want a %s hold %q", side, allowed, gate, reason, a.gate, a.reason)
	}
}

// TestEvaluateUpdateGate_KeepRatioReplansTheBatchesFromTheResizedCounts is
// a resize between two batches of the reference roll under the ratio rule
// with the decoder named first: 12 prefill beside 8 decode, both RecreatePod
// at 10 percent and a tolerance of 10, the prefill side having served two
// batches of two and the decode side two batches of one. The plan over 12:8
// reads 2:1, 4:2, 5:3, 7:4, 8:5, 10:6, 11:7, 12:8, so before the resize the
// prefill's next start waits for the third new decode and the decode's
// third drain is admitted. Each side's count is read from its spec and the
// plan is made again from both counts and both steps:
//
//   - the prefill side scaled up to 24 (12 Instances still being created):
//     the step becomes 3 and the plan over 24:8 at 3 and 1 reads 3:1, 6:2,
//     9:3 and so on, so the 4 started prefill are one full batch and one of
//     the second; the prefill's next start opens the second batch, which the
//     batch step admits since the second new decode serves, and the group's
//     unavailability budget, 3 of 24 at 10 percent, holds it while the 12 new
//     Instances come up, counted as unavailable; the decode's third drain
//     waits for the second prefill batch to serve, 4 of 6;
//   - the prefill side scaled down to 8 (4 old Instances not yet retired):
//     the plan over 8:8 at 1 and 1 reads one of each per batch, the 4
//     started prefill are four batches, and the prefill's next start waits
//     for the decode's fourth, 2 of 4; the decode's third drain is admitted;
//   - the decode side scaled down to 4 (4 old Instances not yet retired):
//     the step stays 1 and the plan over 12:4 at 2 and 1 reads 2:1, 4:1,
//     6:2, 8:3, 10:3, 12:4, so the 2 started decode are three batches and
//     the 4 started prefill two; the prefill's next start opens its third
//     batch, whose decode share already serves, so it is admitted and the
//     picture heads for 6:2, half of each side; the decode's next drain waits
//     for the third prefill batch to serve, 4 of 6;
//   - the decode side scaled up to 16 (8 Instances still being created):
//     the step becomes 2 and the plan over 12:16 at 2 and 2 reads 2:2, 4:4,
//     5:6, 7:8, 8:10, 10:12, 11:14, 12:16, so the 2 started decode are one
//     batch; the prefill's next start waits for the decode's second batch to
//     serve, 2 of 4, and the decode's next drain opens that batch, which the
//     unavailability budget, 2 of 16 at 10 percent, holds while the 8 new
//     Instances come up.
//
// Under RollingUpdate the group reads ScalingDown throughout; under BlueGreen
// the same picture reads Shifting.
func TestEvaluateUpdateGate_KeepRatioReplansTheBatchesFromTheResizedCounts(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	rules := keepRatioDecoderFirst(10)
	engine := batchMemberFixture{desired: 12, step: "10%", strategy: recreate, promoted: 4}
	decoder := batchMemberFixture{desired: 8, step: "10%", strategy: recreate, promoted: 2}
	cases := []struct {
		name            string
		engine, decoder batchMemberFixture
		prefill, decode resizeAnswer
	}{
		{
			name: "before-the-resize", engine: engine, decoder: decoder,
			prefill: resizeAnswer{v1beta1.RolloutHoldGateRatio, "engine batch 3 waits for decoder batch 3 to serve first (2 of 3 serving)"},
		},
		{
			name:   "prefill-scaled-up-to-24",
			engine: batchMemberFixture{desired: 24, step: "10%", strategy: recreate, promoted: 4, creating: 12}, decoder: decoder,
			prefill: resizeAnswer{v1beta1.RolloutHoldGateBudget, "unavailable budget 3 exhausted (current 12, in-flight 0, would become 13); counted as unavailable: Instances 12 to 23 still being created"},
			decode:  resizeAnswer{v1beta1.RolloutHoldGateRatio, "decoder batch 3 waits for engine batch 2 to serve (4 of 6 serving)"},
		},
		{
			name:   "prefill-scaled-down-to-8",
			engine: batchMemberFixture{desired: 8, step: "10%", strategy: recreate, promoted: 4, retiring: 4}, decoder: decoder,
			prefill: resizeAnswer{v1beta1.RolloutHoldGateRatio, "engine batch 5 waits for decoder batch 4 to serve (2 of 4 serving)"},
		},
		{
			name:   "decode-scaled-down-to-4",
			engine: engine, decoder: batchMemberFixture{desired: 4, step: "10%", strategy: recreate, promoted: 2, retiring: 4},
			decode: resizeAnswer{v1beta1.RolloutHoldGateRatio, "decoder batch 4 waits for engine batch 3 to serve (4 of 6 serving)"},
		},
		{
			name:   "decode-scaled-up-to-16",
			engine: engine, decoder: batchMemberFixture{desired: 16, step: "10%", strategy: recreate, promoted: 2, creating: 8},
			prefill: resizeAnswer{v1beta1.RolloutHoldGateRatio, "engine batch 3 waits for decoder batch 2 to serve (2 of 4 serving)"},
			decode:  resizeAnswer{v1beta1.RolloutHoldGateBudget, "unavailable budget 2 exhausted (current 8, in-flight 0, would become 9); counted as unavailable: Instances 8 to 15 still being created"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
			if got := batchGroupPhaseUnder(v1beta1.CoordinationPolicyRollingUpdate, members, eng, dec); got != v1beta1.CoordinationPhaseScalingDown {
				t.Fatalf("RollingUpdate group phase %q, want %q", got, v1beta1.CoordinationPhaseScalingDown)
			}
			if got := batchGroupPhaseUnder(v1beta1.CoordinationPolicyBlueGreen, members, eng, dec); got != v1beta1.CoordinationPhaseShifting {
				t.Fatalf("BlueGreen group phase %q, want %q", got, v1beta1.CoordinationPhaseShifting)
			}
			isvc := batchService(batchGroupFixture{maxUnavailable: "10%"}, members)
			c := batchClient(t, members)
			allowed, gate, reason := batchGateWith(t, isvc, c, eng, workloadtypes.UpdateStrategyRecreatePod, 0, rules)
			tc.prefill.check(t, "prefill", allowed, gate, reason)
			allowed, gate, reason = batchGateWith(t, isvc, c, dec, workloadtypes.UpdateStrategyRecreatePod, 0, rules)
			tc.decode.check(t, "decode", allowed, gate, reason)
		})
	}
}

// TestEvaluateUpdateGate_KeepRatioReplansTheFirstBatchFromTheResizedCounts
// is a resize while the first batch is open under the same rules: the
// decode side's first drain is in flight and nothing is promoted, so the
// group reads Surging under either policy. The prefill's first start waits
// for the first new decode whatever the counts; the decode's second drain
// waits for the first prefill batch of the plan made from the new counts:
// 2 prefill before the resize, 3 once the prefill side is scaled to 24, and
// 1 once it is scaled to 8.
func TestEvaluateUpdateGate_KeepRatioReplansTheFirstBatchFromTheResizedCounts(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	rules := keepRatioDecoderFirst(10)
	decoder := batchMemberFixture{desired: 8, step: "10%", strategy: recreate, inFlight: 1}
	cases := []struct {
		name   string
		engine batchMemberFixture
		decode resizeAnswer
	}{
		{
			name:   "before-the-resize",
			engine: batchMemberFixture{desired: 12, step: "10%", strategy: recreate},
			decode: resizeAnswer{v1beta1.RolloutHoldGateRatio, "decoder batch 2 waits for engine batch 1 to serve (0 of 2 serving)"},
		},
		{
			name:   "prefill-scaled-up-to-24",
			engine: batchMemberFixture{desired: 24, step: "10%", strategy: recreate, creating: 12},
			decode: resizeAnswer{v1beta1.RolloutHoldGateRatio, "decoder batch 2 waits for engine batch 1 to serve (0 of 3 serving)"},
		},
		{
			name:   "prefill-scaled-down-to-8",
			engine: batchMemberFixture{desired: 8, step: "10%", strategy: recreate, retiring: 4},
			decode: resizeAnswer{v1beta1.RolloutHoldGateRatio, "decoder batch 2 waits for engine batch 1 to serve (0 of 1 serving)"},
		},
	}
	prefill := resizeAnswer{v1beta1.RolloutHoldGateRatio, "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: decoder}
			for _, policy := range []v1beta1.CoordinationPolicy{v1beta1.CoordinationPolicyRollingUpdate, v1beta1.CoordinationPolicyBlueGreen} {
				if got := batchGroupPhaseUnder(policy, members, eng, dec); got != v1beta1.CoordinationPhaseSurging {
					t.Fatalf("%s group phase %q, want %q", policy, got, v1beta1.CoordinationPhaseSurging)
				}
			}
			isvc := batchService(batchGroupFixture{maxUnavailable: "10%"}, members)
			c := batchClient(t, members)
			allowed, gate, reason := batchGateWith(t, isvc, c, eng, workloadtypes.UpdateStrategyRecreatePod, 0, rules)
			prefill.check(t, "prefill", allowed, gate, reason)
			allowed, gate, reason = batchGateWith(t, isvc, c, dec, workloadtypes.UpdateStrategyRecreatePod, 0, rules)
			tc.decode.check(t, "decode", allowed, gate, reason)
		})
	}
}
