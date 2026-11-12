package coordination

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestEvaluateUpdateGate_AStoppedAttemptIsEndedOnlyWhileItsRecordStands:
// the escalation stops a recreate or in-place attempt whose pod set still
// runs and keeps it on the row at the step it records for that, and the
// failure counts on the side's retry record. The batch step reads such an attempt as
// every other ended attempt: settled for its peers while a record for the
// target stands (Backoff, RetryInProgress or Held), and still to start
// once the record is gone, as the operator's release of a Held record or a
// promote on the same side that pruned it leaves it. With no record the
// side's next pass starts the attempt again at once, so a peer keeps
// waiting on the batch with the Instance and its last failure named, and
// the side's own next start, that attempt, asks as the batch it belongs to
// and is admitted. The pictures: 6 prefill at 3 beside 2 decode at 1 under
// RecreatePod and as in-place gangs, and 12 prefill beside 8 decode at 10
// percent with the decoder named to move first, before and after the
// release. A surge attempt is never stopped this way, since its operation
// is cleared instead, so no surging picture is asked.
func TestEvaluateUpdateGate_AStoppedAttemptIsEndedOnlyWhileItsRecordStands(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	for _, tc := range stoppedAttemptCases() {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
			isvc := batchService(batchGroupFixture{maxUnavailable: tc.group}, members)
			allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), tc.comp, tc.mechanism, 0, tc.defaults)
			if wantHold := tc.wantReason != ""; wantHold == allowed {
				t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, wantHold, gate, reason)
			}
			if tc.wantReason == "" {
				return
			}
			wantGate := tc.wantGate
			if wantGate == "" {
				wantGate = v1beta1.RolloutHoldGateRatio
			}
			if gate != wantGate {
				t.Errorf("gate=%q, want %q (reason=%q)", gate, wantGate, reason)
			}
			if reason != tc.wantReason {
				t.Errorf("reason=%q, want %q", reason, tc.wantReason)
			}
		})
	}
}

// stoppedAttemptCase is one question to the gate: the two sides as their
// replicas publish them, the group's unavailability budget (empty for the
// default fill), the side and mechanism asked about, the operator's
// settings, and the hold expected, empty when the start is admitted; the
// gate is Ratio unless named.
type stoppedAttemptCase struct {
	name       string
	group      string
	engine     batchMemberFixture
	decoder    batchMemberFixture
	comp       v1beta1.ComponentType
	mechanism  workloadtypes.UpdateStrategyType
	defaults   GroupDefaults
	wantGate   v1beta1.RolloutHoldGate
	wantReason string
}

// stoppedAttemptCases are the pictures of
// TestEvaluateUpdateGate_AStoppedAttemptIsEndedOnlyWhileItsRecordStands.
func stoppedAttemptCases() []stoppedAttemptCase {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategyInPlaceIfPossible
	six := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 6, "3", recreate
		return m
	}
	two := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 2, "1", recreate
		return m
	}
	sixGangs := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy, m.gang = 6, "3", inPlace, true
		return m
	}
	twoGangs := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy, m.gang = 2, "1", inPlace, true
		return m
	}
	twelve := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 12, "10%", recreate
		return m
	}
	eight := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 8, "10%", recreate
		return m
	}
	decoderFirst := GroupDefaults{PrefillDecode: PrefillDecodeRules{FirstSide: dec}}
	decoderHeld := func(wait string) string {
		return wait + ", and the decoder is held: update to svc-decoder-target held after 3 failed attempt(s): the image cannot be pulled; " +
			"a corrected revision, or a release of the held revision on the decoder, resumes both sides"
	}
	return []stoppedAttemptCase{
		{
			name: "the record stands in backoff: the stopped prefill attempt settles its batch and the decoder goes on", group: "3",
			engine: six(batchMemberFixture{promoted: 2, stopped: 1, backoff: 1}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name: "the record is Held and no side is named: the stopped prefill attempt settles its batch as before", group: "3",
			engine: six(batchMemberFixture{promoted: 2, stopped: 1, held: 3}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name: "the record gone after a promote pruned it: the decoder's second batch waits on the stopped Instance by name", group: "3",
			engine: six(batchMemberFixture{promoted: 2, stopped: 1}), decoder: two(batchMemberFixture{promoted: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
			wantReason: "decoder batch 2 waits for engine batch 1 to serve (2 of 3 serving; engine Instance 2 ImagePullBackOff, starts again)",
		},
		{
			name: "the record gone: the prefill's own next start is the stopped attempt again, asked as its first batch, and is admitted", group: "3",
			engine: six(batchMemberFixture{promoted: 2, stopped: 1}), decoder: two(batchMemberFixture{}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name: "an in-place gang stopped over its serving pods, the record gone: the decoder waits on it by name", group: "3",
			engine: sixGangs(batchMemberFixture{promoted: 2, parked: 1}), decoder: twoGangs(batchMemberFixture{promoted: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
			wantReason: "decoder batch 2 waits for engine batch 1 to serve (2 of 3 serving; engine Instance 2 ImagePullBackOff, starts again)",
		},
		{
			name: "an in-place gang stopped over its serving pods, the record in backoff: the decoder goes on", group: "3",
			engine: sixGangs(batchMemberFixture{promoted: 2, parked: 1, backoff: 1}), decoder: twoGangs(batchMemberFixture{promoted: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name:   "the decoder named first, its stopped attempt under a Held record: the prefill's first start reads Held",
			engine: twelve(batchMemberFixture{}), decoder: eight(batchMemberFixture{stopped: 1, held: 3}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, defaults: decoderFirst,
			wantGate: v1beta1.RolloutHoldGateHeld, wantReason: decoderHeld("engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving, 1 failed)"),
		},
		{
			name:   "the decoder named first, its stopped attempt in backoff: the settled batch opens the prefill's first batch",
			engine: twelve(batchMemberFixture{}), decoder: eight(batchMemberFixture{stopped: 1, backoff: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, defaults: decoderFirst,
		},
		{
			name:   "the Held record released: the stopped decoder attempt starts again at once and the prefill's first batch waits on it by name",
			engine: twelve(batchMemberFixture{}), decoder: eight(batchMemberFixture{stopped: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, defaults: decoderFirst,
			wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving; decoder Instance 0 ImagePullBackOff, starts again)",
		},
		{
			name:   "the Held record released: the decoder's own next start is the stopped attempt again and is admitted",
			engine: twelve(batchMemberFixture{}), decoder: eight(batchMemberFixture{stopped: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod, defaults: decoderFirst,
		},
	}
}
