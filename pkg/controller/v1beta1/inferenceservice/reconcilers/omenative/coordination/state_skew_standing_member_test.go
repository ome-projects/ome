package coordination

import (
	"slices"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The group's skew signal names a member only while the ratio gate may still
// be asked about a start of it. A member that keeps its template, one whose
// movable Instances are all on the target and one resting at its partition
// have no next start, so a change of one member alone, or the tail of a roll
// one side finished first, writes no surgeHeldByRatio and records no
// RatioSkewRejected.

// prefillDecodeSurge is the reference pair the band and the lockstep pace
// on their own: 12 prefill beside 8 decode, both SurgeThenDrain at maxSurge
// 10 percent under maintainRatio at tolerance 20, with the given Instances
// on version 2 on each side.
func prefillDecodeSurge(enginePromoted, decoderPromoted int32) map[v1beta1.ComponentType]batchMemberFixture {
	surge := v1beta1.UpdateStrategySurgeThenDrain
	return map[v1beta1.ComponentType]batchMemberFixture{
		v1beta1.EngineComponent:  {desired: 12, surge: "10%", strategy: surge, promoted: enginePromoted},
		v1beta1.DecoderComponent: {desired: 8, surge: "10%", strategy: surge, promoted: decoderPromoted},
	}
}

// withRouter adds 4 routers, SurgeThenDrain at maxSurge 10 percent, with
// the given Instances on version 2.
func withRouter(members map[v1beta1.ComponentType]batchMemberFixture, promoted int32) map[v1beta1.ComponentType]batchMemberFixture {
	members[v1beta1.RouterComponent] = batchMemberFixture{desired: 4, surge: "10%", strategy: v1beta1.UpdateStrategySurgeThenDrain, promoted: promoted}
	return members
}

// standing marks the named members as keeping their template, the way the
// pass observes a member whose replica rests: no roll in flight and every
// Instance on the one revision it runs and serving.
func standing(comps ...v1beta1.ComponentType) func(map[v1beta1.ComponentType]*ComponentObservation) {
	return func(m map[v1beta1.ComponentType]*ComponentObservation) {
		for _, c := range comps {
			co := m[c]
			co.RolloutInFlight, co.AtDesiredShape = false, true
			co.NewRevisionPods, co.NewRevisionReadyPods, co.ServingPods = co.TotalPods, co.TotalPods, co.TotalPods
		}
	}
}

// standingScaledUp marks the engine as keeping its template with a scale-up
// of two Instances pending: 14 asked for, 12 on the one revision it runs.
func standingScaledUp(m map[v1beta1.ComponentType]*ComponentObservation) {
	standing(v1beta1.EngineComponent)(m)
	m[v1beta1.EngineComponent].SpecReplicas = 14
}

// restingAtPartition marks the engine as resting at a partition of four:
// eight Instances on version 2, the four the partition holds on version 1,
// the replica's current revision still version 1.
func restingAtPartition(m map[v1beta1.ComponentType]*ComponentObservation) {
	m[v1beta1.EngineComponent].Partition, m[v1beta1.EngineComponent].AtDesiredShape = 4, true
}

// TestComputeTransition_AMemberWithNoStartLeftIsNeverNamedHeld is the
// operator's story: 12 prefill beside 8 decode under maintainRatio at
// tolerance 20, both SurgeThenDrain, and the decode image alone is pushed.
// The prefill side keeps its template, so its progress reads 12 of 12 and
// the lockstep would hold its next surge start against a decoder at 0 of 8,
// a start no replica will ever ask for. The group names no held start and
// the service writes no surgeHeldByRatio, whichever member stands and under
// either policy; the same holds for a side that finished while its peer
// trails, for one resting at its partition and for a standing side with a
// scale-up pending. A member with a start left that runs ahead is still
// named.
func TestComputeTransition_AMemberWithNoStartLeftIsNeverNamedHeld(t *testing.T) {
	rolling, blueGreen := v1beta1.CoordinationPolicyRollingUpdate, v1beta1.CoordinationPolicyBlueGreen
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	pair := []v1beta1.ComponentType{eng, dec}
	trio := []v1beta1.ComponentType{eng, dec, rtr}
	engineSurge := []HeldStart{{Component: eng, Strategy: workloadtypes.UpdateStrategySurgeThenDrain}}
	cases := []struct {
		name       string
		policy     v1beta1.CoordinationPolicy
		components []v1beta1.ComponentType
		members    map[v1beta1.ComponentType]batchMemberFixture
		edit       func(map[v1beta1.ComponentType]*ComponentObservation)
		phase      v1beta1.CoordinationPhase
		held       []HeldStart
	}{
		{"decoder-changes-alone-under-rollingupdate", rolling, pair, prefillDecodeSurge(12, 0), standing(eng), v1beta1.CoordinationPhaseSurging, nil},
		{"decoder-changes-alone-under-bluegreen", blueGreen, pair, prefillDecodeSurge(12, 0), standing(eng), v1beta1.CoordinationPhaseSurging, nil},
		{"engine-changes-alone-the-decoder-standing", rolling, pair, prefillDecodeSurge(0, 8), standing(dec), v1beta1.CoordinationPhaseSurging, nil},
		{"engine-standing-with-a-scale-up-pending", rolling, pair, prefillDecodeSurge(12, 0), standingScaledUp, v1beta1.CoordinationPhaseSurging, nil},
		{"router-changes-alone-in-a-three-member-group", rolling, trio, withRouter(prefillDecodeSurge(12, 8), 0), standing(eng, dec), v1beta1.CoordinationPhaseSurging, nil},
		{"engine-finished-while-the-decoder-trails-under-rollingupdate", rolling, pair, prefillDecodeSurge(12, 1), nil, v1beta1.CoordinationPhaseScalingDown, nil},
		{"engine-finished-while-the-decoder-trails-under-bluegreen", blueGreen, pair, prefillDecodeSurge(12, 1), nil, v1beta1.CoordinationPhaseShifting, nil},
		{"engine-resting-at-its-partition-beside-the-trailing-decoder", rolling, pair, prefillDecodeSurge(8, 1), restingAtPartition, v1beta1.CoordinationPhaseScalingDown, nil},
		{"engine-ahead-with-starts-left-is-named", rolling, pair, prefillDecodeSurge(4, 0), nil, v1beta1.CoordinationPhaseSurging, engineSurge},
		{"a-standing-router-hides-no-lead-of-the-pair", rolling, trio, withRouter(prefillDecodeSurge(4, 0), 4), standing(rtr), v1beta1.CoordinationPhaseSurging, engineSurge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := skewTransitionOver(t, tc.policy, tc.components, tc.members, tc.edit)
			if tr.Phase != tc.phase {
				t.Fatalf("phase %s, want %s", tr.Phase, tc.phase)
			}
			if tr.RatioSkewRejected != (len(tc.held) > 0) {
				t.Errorf("RatioSkewRejected=%v in %s, want %v: the signal names a member only while a start of it may still be asked", tr.RatioSkewRejected, tr.Phase, len(tc.held) > 0)
			}
			if !slices.Equal(tr.RatioHeldStarts, tc.held) {
				t.Errorf("held starts %v in %s, want %v", tr.RatioHeldStarts, tr.Phase, tc.held)
			}
		})
	}
}
