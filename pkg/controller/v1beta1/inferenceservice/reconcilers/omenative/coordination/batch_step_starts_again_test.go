package coordination

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestEvaluateUpdateGate_AnAttemptStartedAgainAtOnceIsStillWaitedOn: 12
// prefill beside 8 decode under RecreatePod at 10 percent, so the steps
// are 2 and 1. The scheduler cannot place the decoder's first new pod and
// the placement grace ends the attempt: the row reads Failed with nothing
// counted on the decoder's retry record, and the decoder's next pass
// starts the attempt again. The batch step reads such an Instance as
// still to start, so the prefill side keeps waiting on the decoder's
// first batch, the hold names the Instance and why it waits, and the
// decoder's own next start, that attempt, asks as the batch it belongs
// to and is admitted. An attempt the record counts (a backoff, a Held
// record) waits on the record and settles its batch as before. An open
// attempt the scheduler holds is waited on with its reason named. Under
// SurgeThenDrain the same reading keeps the two sides from waiting on
// each other when both first surges end this way.
func TestEvaluateUpdateGate_AnAttemptStartedAgainAtOnceIsStillWaitedOn(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	for _, tc := range startsAgainCases() {
		t.Run(tc.name, func(t *testing.T) {
			members := map[v1beta1.ComponentType]batchMemberFixture{eng: tc.engine, dec: tc.decoder}
			isvc := batchService(batchGroupFixture{}, members)
			allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), tc.comp, tc.mechanism, 0, tc.defaults)
			if wantHold := tc.wantReason != ""; wantHold == allowed {
				t.Fatalf("allowed=%v, want hold=%v (gate=%q reason=%q)", allowed, wantHold, gate, reason)
			}
			if tc.wantReason == "" {
				return
			}
			if gate != v1beta1.RolloutHoldGateRatio {
				t.Errorf("gate=%q, want %q (reason=%q)", gate, v1beta1.RolloutHoldGateRatio, reason)
			}
			if reason != tc.wantReason {
				t.Errorf("reason=%q, want %q", reason, tc.wantReason)
			}
		})
	}
}

// startsAgainCase is one question to the gate: the two sides as their
// replicas publish them, the side and mechanism asked about, the
// operator's settings, and the hold expected, empty when the start is
// admitted.
type startsAgainCase struct {
	name       string
	engine     batchMemberFixture
	decoder    batchMemberFixture
	comp       v1beta1.ComponentType
	mechanism  workloadtypes.UpdateStrategyType
	defaults   GroupDefaults
	wantReason string
}

// startsAgainCases are the pictures of
// TestEvaluateUpdateGate_AnAttemptStartedAgainAtOnceIsStillWaitedOn.
func startsAgainCases() []startsAgainCase {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	twelve := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 12, "10%", recreate
		return m
	}
	eight := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.step, m.strategy = 8, "10%", recreate
		return m
	}
	three := func(m batchMemberFixture) batchMemberFixture {
		m.desired, m.surge, m.strategy = 3, "1", surge
		return m
	}
	decoderFirst := GroupDefaults{PrefillDecode: PrefillDecodeRules{FirstSide: dec}}
	return []startsAgainCase{
		{
			name:   "the prefill's second batch waits on the decoder attempt the grace ended",
			engine: twelve(batchMemberFixture{promoted: 2}), decoder: eight(batchMemberFixture{graceEnded: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod,
			wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving; decoder Instance 0 Unschedulable, starts again)",
		},
		{
			name:   "the decoder named first: the prefill's first batch waits on it",
			engine: twelve(batchMemberFixture{}), decoder: eight(batchMemberFixture{graceEnded: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod, defaults: decoderFirst,
			wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving; decoder Instance 0 Unschedulable, starts again)",
		},
		{
			name:   "the decoder's next start is that attempt again, asked as its first batch, and is admitted",
			engine: twelve(batchMemberFixture{}), decoder: eight(batchMemberFixture{graceEnded: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod, defaults: decoderFirst,
		},
		{
			name:   "a prefill attempt the grace ended: the prefill's next start is the attempt again and is admitted",
			engine: twelve(batchMemberFixture{promoted: 1, graceEnded: 1}), decoder: eight(batchMemberFixture{promoted: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name:   "a prefill attempt the grace ended: the decoder's second batch waits on it and names it",
			engine: twelve(batchMemberFixture{promoted: 1, graceEnded: 1}), decoder: eight(batchMemberFixture{promoted: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategyRecreatePod,
			wantReason: "decoder batch 2 waits for engine batch 1 to serve (1 of 2 serving; engine Instance 1 Unschedulable, starts again)",
		},
		{
			name:   "a failure the record counts waits on its backoff and settles its batch: the prefill goes on",
			engine: twelve(batchMemberFixture{promoted: 2}), decoder: eight(batchMemberFixture{graceEnded: 1, backoff: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name:   "a Held record: the ended attempt settles its batch for a side that is not the named one",
			engine: twelve(batchMemberFixture{promoted: 2}), decoder: eight(batchMemberFixture{graceEnded: 1, held: 3}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod,
		},
		{
			name:   "an open attempt the scheduler holds is waited on with its reason",
			engine: twelve(batchMemberFixture{promoted: 2}), decoder: eight(batchMemberFixture{unplaceable: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod,
			wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving; decoder Instance 0 Unschedulable)",
		},
		{
			name:   "a Failed row with no reason recorded and no record: the Instance starts again",
			engine: twelve(batchMemberFixture{promoted: 2}), decoder: eight(batchMemberFixture{failed: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategyRecreatePod,
			wantReason: "engine batch 2 waits for decoder batch 1 to serve (0 of 1 serving; decoder Instance 0 starts again)",
		},
		{
			name:   "3:3 surging, both first surges ended on the grace: the engine's retry is admitted",
			engine: three(batchMemberFixture{graceEnded: 1}), decoder: three(batchMemberFixture{graceEnded: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain,
		},
		{
			name:   "3:3 surging, both ended on the grace, the decoder named: the engine's start waits on the decoder's retry",
			engine: three(batchMemberFixture{graceEnded: 1}), decoder: three(batchMemberFixture{graceEnded: 1}), comp: eng, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, defaults: decoderFirst,
			wantReason: "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving; decoder Instance 0 Unschedulable, starts again)",
		},
		{
			name:   "3:3 surging, both ended on the grace, the decoder named: the decoder's retry is admitted, so the sides never wait on each other",
			engine: three(batchMemberFixture{graceEnded: 1}), decoder: three(batchMemberFixture{graceEnded: 1}), comp: dec, mechanism: workloadtypes.UpdateStrategySurgeThenDrain, defaults: decoderFirst,
		},
	}
}
