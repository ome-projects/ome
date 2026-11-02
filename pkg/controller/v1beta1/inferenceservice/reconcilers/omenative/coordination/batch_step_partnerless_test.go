package coordination

import (
	"strings"
	"testing"

	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A side whose version has lost its last partner. 6 prefill beside 2 decode
// roll under SurgeThenDrain at 10 percent of each side, one Instance at a
// time, the decoder named first: decode Instance 0 serves version 2 and
// prefill 0 follows; then the last old decode pod is lost and rebuilt onto
// version 2 at once, so the five old prefill have no old decode left and
// answer nothing. With no ratio tolerance configured the batch step holds
// none of their starts from that pass on and the prefill side's own surge
// budget paces them. Under a configured tolerance the ratio rule governs and
// the batch pacing stands. "No tolerance configured" is the setting's
// absence: the group names no maintainRatio tolerance (none written, none
// filled from the operator's default) and, under keepRatio, the operator
// configured no default; a tolerance of 100, the band's opt-out, is a
// configured tolerance.

// sixTwoSurge is the shape, both sides surging at a 10 percent budget of
// their own count, one Instance each.
func sixTwoSurge() (engine, decoder batchMemberFixture) {
	surge := v1beta1.UpdateStrategySurgeThenDrain
	return batchMemberFixture{desired: 6, surge: "10%", strategy: surge}, batchMemberFixture{desired: 2, surge: "10%", strategy: surge}
}

// sixTwoGroup is the rollingUpdate group at the same 10 percent surge
// budget, naming no maintainRatio.
var sixTwoGroup = batchGroupFixture{maxUnavailable: "0", maxSurge: "10%"}

// withTolerance is the group under maintainRatio at the given tolerance.
func withTolerance(g batchGroupFixture, tolerance int32) batchGroupFixture {
	g.tolerance = ptr.To(tolerance)
	return g
}

// partnerlessPicture is the moment after the loss: prefill 0 serves
// version 2, both decoders run it (Instance 1 rebuilt onto it), and open
// prefill replacements are starting.
func partnerlessPicture(open int32) map[v1beta1.ComponentType]batchMemberFixture {
	engine, decoder := sixTwoSurge()
	engine.promoted, engine.surging = 1, open
	decoder.promoted = 2
	return map[v1beta1.ComponentType]batchMemberFixture{v1beta1.EngineComponent: engine, v1beta1.DecoderComponent: decoder}
}

// noToleranceSettings are the operator settings under which the group has
// no ratio tolerance, the group naming no maintainRatio in every case.
var noToleranceSettings = []struct {
	name     string
	defaults GroupDefaults
}{
	{"keepRatio and firstSide on, no default tolerance configured", GroupDefaults{PrefillDecode: PrefillDecodeRules{KeepRatio: true, FirstSide: v1beta1.DecoderComponent}}},
	{"firstSide alone", GroupDefaults{PrefillDecode: PrefillDecodeRules{FirstSide: v1beta1.DecoderComponent}}},
	{"no setting at all", GroupDefaults{}},
}

// drivePartnerlessPrefill rolls the five old prefill of the picture through
// the gate one round at a time: the side's next start is asked with nothing
// of it open and must be admitted, a second start is asked beside the open
// one and must be held under wantGate (the batch step's hold names a batch,
// the budget's does not), then the open replacement serves. It returns the
// rounds the roll took.
func drivePartnerlessPrefill(t *testing.T, group batchGroupFixture, defaults GroupDefaults, wantGate v1beta1.RolloutHoldGate) int {
	t.Helper()
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	engine, decoder := sixTwoSurge()
	engine.promoted, decoder.promoted = 1, 2
	rounds := 0
	for engine.promoted < 6 {
		rounds++
		if rounds > 6 {
			t.Fatalf("the five old prefill did not roll in 6 rounds")
		}
		members := map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder}
		allowed, gate, reason := batchGateWith(t, batchService(group, members), batchClient(t, members), eng, surge, 0, defaults)
		if !allowed {
			t.Fatalf("round %d: with nothing of the prefill side open its next start must be admitted; got gate=%s reason=%q", rounds, gate, reason)
		}
		engine.surging = 1
		members = map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder}
		allowed, gate, reason = batchGateWith(t, batchService(group, members), batchClient(t, members), eng, surge, 0, defaults)
		if allowed || gate != wantGate || strings.Contains(reason, "batch") != (wantGate == v1beta1.RolloutHoldGateRatio) {
			t.Fatalf("round %d: a second start beside the open one must be held under the %s gate; got allowed=%v gate=%s reason=%q", rounds, wantGate, allowed, gate, reason)
		}
		engine.promoted, engine.surging = engine.promoted+1, 0
	}
	return rounds
}

// TestEvaluateUpdateGate_PartnerlessSideRollsAtItsOwnBudgetWhenNoToleranceIsConfigured
// holds the gate to the release: once both decoders run version 2 and the
// prefill side still has Instances on version 1, no answer to a prefill
// start comes from the batch step. Its next start opens whenever nothing of
// it is open, a start beside an open one is the surge budget's to hold,
// and the five old prefill roll one per round at that budget. The mirror
// holds for old decoders whose prefill side is wholly on version 2. A peer
// the run did not move is not a lost partner: in an engine-only change the
// decoder's one revision serves both prefill versions and the batch pacing
// stands.
func TestEvaluateUpdateGate_PartnerlessSideRollsAtItsOwnBudgetWhenNoToleranceIsConfigured(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	budgetHold := "surge budget 1 exhausted (current 1, in-flight 0, would become 2)"
	for _, s := range noToleranceSettings {
		t.Run(s.name, func(t *testing.T) {
			t.Run("the next prefill start opens once the previous serves", func(t *testing.T) {
				members := partnerlessPicture(0)
				allowed, gate, reason := batchGateWith(t, batchService(sixTwoGroup, members), batchClient(t, members), eng, surge, 0, s.defaults)
				if !allowed {
					t.Fatalf("the next prefill start must be admitted; got gate=%s reason=%q", gate, reason)
				}
			})
			t.Run("a start beside the open one is the surge budget's to hold, not the batch step's", func(t *testing.T) {
				members := partnerlessPicture(1)
				allowed, gate, reason := batchGateWith(t, batchService(sixTwoGroup, members), batchClient(t, members), eng, surge, 0, s.defaults)
				if allowed || gate != v1beta1.RolloutHoldGateBudget || reason != budgetHold {
					t.Fatalf("allowed=%v gate=%s reason=%q; want the surge budget's hold %q", allowed, gate, reason, budgetHold)
				}
			})
			t.Run("the five old prefill roll one per round at the budget", func(t *testing.T) {
				if rounds := drivePartnerlessPrefill(t, sixTwoGroup, s.defaults, v1beta1.RolloutHoldGateBudget); rounds != 5 {
					t.Fatalf("the five old prefill took %d rounds, want 5", rounds)
				}
			})
			t.Run("the mirror: old decoders whose prefill side is wholly on version 2 roll at the decode budget", func(t *testing.T) {
				engine, decoder := sixTwoSurge()
				engine.promoted, decoder.surging = 6, 1
				members := map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder}
				allowed, gate, reason := batchGateWith(t, batchService(sixTwoGroup, members), batchClient(t, members), dec, surge, 0, s.defaults)
				if allowed || gate != v1beta1.RolloutHoldGateBudget || reason != budgetHold {
					t.Fatalf("allowed=%v gate=%s reason=%q; want the surge budget's hold %q", allowed, gate, reason, budgetHold)
				}
			})
			t.Run("a peer the run did not move is not a lost partner: an engine-only change keeps the pacing", func(t *testing.T) {
				members := partnerlessPicture(1)
				isvc := batchService(sixTwoGroup, members)
				for i := range isvc.Status.Rollout.ActiveRun.TargetRevisions {
					if recorded := &isvc.Status.Rollout.ActiveRun.TargetRevisions[i]; recorded.Component == dec {
						recorded.StableRevision = recorded.Revision
					}
				}
				var objs []client.Object
				for comp, m := range members {
					ir := batchReplica(comp, m)
					if comp == dec {
						ir.Status.CurrentRevision = ir.Status.UpdateRevision
					}
					objs = append(objs, ir)
				}
				c := fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).Build()
				allowed, gate, reason := batchGateWith(t, isvc, c, eng, surge, 0, s.defaults)
				want := "engine batch 3 waits for engine batch 2 to serve (1 of 2 serving)"
				if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != want {
					t.Fatalf("allowed=%v gate=%s reason=%q; want the batch step's hold %q", allowed, gate, reason, want)
				}
			})
		})
	}
}

// TestEvaluateUpdateGate_PartnerlessSideKeepsTheBatchPacingUnderAConfiguredTolerance
// holds the same picture to the batch step whenever the group has a ratio
// tolerance: the chart's settings (keepRatio with the operator's default of
// 5), maintainRatio on the group with no prefill/decode setting, and a
// tolerance of 100 written on the group or configured as the default. A
// prefill start beside an open one waits for the prefill's own previous
// batch to serve, and the five old prefill roll one per batch.
func TestEvaluateUpdateGate_PartnerlessSideKeepsTheBatchPacingUnderAConfiguredTolerance(t *testing.T) {
	eng := v1beta1.EngineComponent
	surge := workloadtypes.UpdateStrategySurgeThenDrain
	want := "engine batch 3 waits for engine batch 2 to serve (1 of 2 serving)"
	cases := []struct {
		name     string
		group    batchGroupFixture
		defaults GroupDefaults
	}{
		{"the chart's settings: keepRatio with the operator's default tolerance of 5", sixTwoGroup, keepRatioDecoderFirst(5)},
		{"maintainRatio tolerance 5 on the group with no prefill/decode setting", withTolerance(sixTwoGroup, 5), GroupDefaults{}},
		{"maintainRatio tolerance 100 on the group, the band's opt-out, is a configured tolerance", withTolerance(sixTwoGroup, 100), keepRatioDecoderFirst(5)},
		{"the operator's default of 100 under keepRatio is a configured tolerance", sixTwoGroup, keepRatioDecoderFirst(100)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := partnerlessPicture(1)
			allowed, gate, reason := batchGateWith(t, batchService(tc.group, members), batchClient(t, members), eng, surge, 0, tc.defaults)
			if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != want {
				t.Fatalf("allowed=%v gate=%s reason=%q; want the batch step's hold %q", allowed, gate, reason, want)
			}
			if rounds := drivePartnerlessPrefill(t, tc.group, tc.defaults, v1beta1.RolloutHoldGateRatio); rounds != 5 {
				t.Fatalf("the five old prefill took %d rounds, want 5", rounds)
			}
		})
	}
}
