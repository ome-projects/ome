package coordination

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestEvaluateUpdateGate_HeldNamedSideSettlesItsBatchesByServedInstancesOnly:
// 3 prefill beside 3 decode roll under SurgeThenDrain at a maxSurge of 1
// with the decoder named to move first, and neither image can be pulled.
// The decoder's record reads Held after three failed attempts. While it
// is Held, only decode Instances that serve the new version settle a
// decode batch for the prefill side: an ended decode attempt no retry
// will re-open settles nothing, so a prefill start of the same batch
// reads Held with the decoder's record and the two ways out, whether the
// prefill side has started nothing, retries its own batch, or both sides
// were asked for one Instance meanwhile. An Instance the decoder served
// before the record was held settles its batch; a released record lets
// the decoder start the ended attempt again at once, so the prefill side
// waits on it instead of opening beside it; and a Held side that is not
// the named one is waited on by its ended attempts as before. The
// prefill side's own failed attempt is one its schedule counted.
func TestEvaluateUpdateGate_HeldNamedSideSettlesItsBatchesByServedInstancesOnly(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := v1beta1.UpdateStrategySurgeThenDrain
	three := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.surge, m.strategy = 3, "1", surge
		return m
	}
	one := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.surge, m.strategy = 1, "1", surge
		return m
	}
	decoderFirst := GroupDefaults{PrefillDecode: PrefillDecodeRules{FirstSide: dec}}
	engineFirst := GroupDefaults{PrefillDecode: PrefillDecodeRules{FirstSide: eng}}
	decoderHeld := func(wait string) string {
		return wait + ", and the decoder is held: update to svc-decoder-target held after 3 failed attempt(s): the image cannot be pulled; " +
			"a corrected revision, or a release of the held revision on the decoder, resumes both sides"
	}
	engineHeld := func(wait string) string {
		return wait + ", and the engine is held: update to svc-engine-target held after 3 failed attempt(s): the image cannot be pulled; " +
			"a corrected revision, or a release of the held revision on the engine, resumes both sides"
	}
	cases := []struct {
		name       string
		engine     batchMemberFixture
		decoder    batchMemberFixture
		comp       v1beta1.ComponentType
		defaults   GroupDefaults
		wantGate   v1beta1.RolloutHoldGate
		wantReason string
	}{
		{
			name:   "the engine has started nothing: its first start reads Held, the ended decode attempt settles nothing",
			engine: three(batchMemberFixture{}), decoder: three(batchMemberFixture{failedSurge: 1, held: 3}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: decoderHeld("engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving, 1 failed)"),
		},
		{
			name:   "the engine's own attempt failed once: its retry reads Held",
			engine: three(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: three(batchMemberFixture{failedSurge: 1, held: 3}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: decoderHeld("engine batch 2 waits for decoder batch 2 to serve first (0 of 2 serving, 1 failed)"),
		},
		{
			name:   "both sides asked for one Instance: the engine's retry of its own batch reads Held, not a retry countdown",
			engine: one(batchMemberFixture{failedSurge: 1, backoff: 1}), decoder: one(batchMemberFixture{failedSurge: 1, held: 3}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: decoderHeld("engine batch 2 waits for decoder batch 2 to serve first (0 of 1 serving, 1 failed)"),
		},
		{
			name:   "the decoder served one Instance before its record was held: the engine's first batch opens on it",
			engine: three(batchMemberFixture{}), decoder: three(batchMemberFixture{promoted: 1, failedSurge: 1, held: 3}), comp: eng, defaults: decoderFirst,
		},
		{
			name:   "the decoder served one Instance before its record was held: the engine's second batch waits on the served count alone",
			engine: three(batchMemberFixture{promoted: 1}), decoder: three(batchMemberFixture{promoted: 1, failedSurge: 1, held: 3}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: decoderHeld("engine batch 2 waits for decoder batch 2 to serve first (1 of 2 serving, 1 failed)"),
		},
		{
			name:   "the record released: the decoder starts the ended attempt again at once and the engine's first start waits on it",
			engine: three(batchMemberFixture{}), decoder: three(batchMemberFixture{failedSurge: 1}), comp: eng, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateRatio, wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving; decoder Instance 0 starts again)",
		},
		{
			name:   "the engine named and held: the decoder's first start reads Held with the engine's record",
			engine: three(batchMemberFixture{failedSurge: 1, held: 3}), decoder: three(batchMemberFixture{}), comp: dec, defaults: engineFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: engineHeld("decoder batch 1 waits for engine batch 1 to serve first (0 of 1 serving, 1 failed)"),
		},
		{
			name:   "no side named: the held decoder's ended attempt settles its batch for the engine's next batch as before",
			engine: three(batchMemberFixture{promoted: 1}), decoder: three(batchMemberFixture{failedSurge: 1, held: 3}), comp: eng,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
			isvc := batchService(batchGroupFixture{maxUnavailable: "0", maxSurge: "1"}, members)
			allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), tc.comp, workloadtypes.UpdateStrategySurgeThenDrain, 0, tc.defaults)
			if wantHold := tc.wantGate != ""; wantHold == allowed {
				t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, wantHold, gate, reason)
			}
			if gate != tc.wantGate {
				t.Errorf("gate=%q, want %q (reason=%q)", gate, tc.wantGate, reason)
			}
			if reason != tc.wantReason {
				t.Errorf("reason=%q, want %q", reason, tc.wantReason)
			}
		})
	}
}
