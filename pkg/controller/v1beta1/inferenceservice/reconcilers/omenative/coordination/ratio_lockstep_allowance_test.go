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

// The lockstep half of the ratio rule projects every start it is asked
// about on the moves the side already has under way. The story: engine
// Instances patched in place beside decode Instances recreated, in one
// rollingUpdate group with maintainRatio and group budgets wide enough that
// only the ratio rule paces the pair; the engine's single-pod patches keep
// the batch step off, so the lockstep and the band decide. A side may stand
// ahead of its peer's share by the tolerance of its own count, rounded up
// to a whole Instance and at least one: 1 of 8 at tolerance 10, 2 at 20
// and at 25, 4 at 50. A start that would carry the side past that
// allowance is held with the lockstep's reason, whether the moves under
// way were started in an earlier pass or in this one (every start is
// written on its row before the next is consulted), and whatever the
// serving counters say. The side behind always proceeds.

// lockstepMember is one side of the story: its Instance count (8 when
// unset), how many Instances have landed on version 2, how many attempts
// are open on its rows, and the serving count its status carries (nil
// leaves the fixture's own).
type lockstepMember struct {
	count    int32
	promoted int32
	open     int32
	serving  *int32
}

func (m lockstepMember) desired() int32 {
	if m.count > 0 {
		return m.count
	}
	return 8
}

// lockstepPair builds the service and the replicas of the story at the
// given tolerance. The engine patches single pods in place and the decode
// side recreates; the group's budgets admit every Instance at once, so the
// ratio rule alone paces the pair.
func lockstepPair(t *testing.T, tolerance int32, engine, decode lockstepMember) (*v1beta1.InferenceService, client.Client) {
	t.Helper()
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	members := map[v1beta1.ComponentType]batchMemberFixture{
		eng: {desired: engine.desired(), strategy: v1beta1.UpdateStrategyInPlaceIfPossible, promoted: engine.promoted, inPlace: engine.open},
		dec: {desired: decode.desired(), strategy: v1beta1.UpdateStrategyRecreatePod, promoted: decode.promoted, inFlight: decode.open},
	}
	isvc := batchService(batchGroupFixture{tolerance: int32Ptr(tolerance), maxUnavailable: "100%", maxSurge: "100%"}, members)
	var objs []client.Object
	for comp, m := range members {
		ir := batchReplica(comp, m)
		serving := engine.serving
		if comp == dec {
			serving = decode.serving
		}
		if serving != nil {
			ir.Status.ServingReplicas = *serving
			ir.Status.ReadyReplicas = *serving
		}
		objs = append(objs, ir)
	}
	return isvc, fake.NewClientBuilder().WithScheme(pairingScheme(t)).WithObjects(objs...).Build()
}

const (
	inPlaceLockstepReason = "in-place update would outpace peer new-revision rollout past tolerance"
	drainLockstepReason   = "drain would outpace peer new-revision rollout past tolerance"
)

// TestEvaluateUpdateGate_LockstepProjectsTheStartItAdmits asks the
// production decision site about one more start of a side, picture by
// picture. The side's progress after the start counts the Instances that
// have landed, the attempts open on its rows (the starts of this pass
// among them) and the start itself; the peer is credited only with what
// has landed, and the pods the dispatcher reports out of rotation this
// pass are the stale counters' correction, not a second count of the
// rows. A start that would carry the side past its allowance ahead of a
// still-rolling peer is held with the lockstep's reason, whatever the
// serving counts say; a start that lands exactly on the allowance is
// admitted.
func TestEvaluateUpdateGate_LockstepProjectsTheStartItAdmits(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	inPlace, recreate := workloadtypes.UpdateStrategyInPlaceIfPossible, workloadtypes.UpdateStrategyRecreatePod
	eight := int32Ptr(8)
	cases := []struct {
		name           string
		tolerance      int32
		engine, decode lockstepMember
		side           v1beta1.ComponentType
		mechanism      workloadtypes.UpdateStrategyType
		already        int32 // pods the dispatcher reports out of rotation this pass, beside the rows
		wantAllowed    bool
		wantReason     string
	}{
		{
			name:      "8:8 at 10, level at none: the engine's first patch is admitted although one Instance is wider than the tolerance",
			tolerance: 10, side: eng, mechanism: inPlace, wantAllowed: true,
		},
		{
			name:      "8:8 at 10, one patch written on its row earlier in the pass and reported out of rotation: the second would stand two ahead of a decode side at none, past the allowance of one",
			tolerance: 10, engine: lockstepMember{open: 1, serving: eight},
			side: eng, mechanism: inPlace, already: 1,
			wantAllowed: false, wantReason: inPlaceLockstepReason,
		},
		{
			name:      "8:8 at 10, the next pass with two patches under way and nothing landed, the counters still carrying both pods as serving: a third patch is held",
			tolerance: 10, engine: lockstepMember{open: 2, serving: eight},
			side: eng, mechanism: inPlace,
			wantAllowed: false, wantReason: inPlaceLockstepReason,
		},
		{
			name:      "8:8 at 10, one patch landed and one under way beside a decode side at none: held",
			tolerance: 10, engine: lockstepMember{promoted: 1, open: 1},
			side: eng, mechanism: inPlace,
			wantAllowed: false, wantReason: inPlaceLockstepReason,
		},
		{
			name:      "8:8 at 10, one patch under way beside one decode Instance landed: level, so the engine's next patch is admitted",
			tolerance: 10, engine: lockstepMember{open: 1}, decode: lockstepMember{promoted: 1},
			side: eng, mechanism: inPlace, wantAllowed: true,
		},
		{
			name:      "8:8 at 10, the engine behind a decode side two Instances ahead: its patch brings the pair back together and is admitted",
			tolerance: 10, decode: lockstepMember{promoted: 2},
			side: eng, mechanism: inPlace, wantAllowed: true,
		},
		{
			name:      "8:8 at 10, the decode side's second drain of the same pass is held by the lockstep, not by the band: the drain written on its row already stands at the allowance",
			tolerance: 10, decode: lockstepMember{open: 1, serving: eight},
			side: dec, mechanism: recreate, already: 1,
			wantAllowed: false, wantReason: drainLockstepReason,
		},
		{
			name:      "8:8 at 10, the decode side with one drain under way, the counters one step behind, beside an engine at none: its next drain is held",
			tolerance: 10, decode: lockstepMember{open: 1, serving: eight},
			side: dec, mechanism: recreate,
			wantAllowed: false, wantReason: drainLockstepReason,
		},
		{
			name:      "8:8 at 10, the decode side behind an engine with two patches landed: its drain is admitted; the lockstep never holds the side behind",
			tolerance: 10, engine: lockstepMember{promoted: 2},
			side: dec, mechanism: recreate, wantAllowed: true,
		},
		{
			name:      "8:8 at 25, one patch under way beside a decode side at none: the second lands exactly on the allowance of two and is admitted",
			tolerance: 25, engine: lockstepMember{open: 1},
			side: eng, mechanism: inPlace, wantAllowed: true,
		},
		{
			name:      "8:8 at 25, one patch written on its row this pass and reported out of rotation: one move, not two, so the second patch is admitted",
			tolerance: 25, engine: lockstepMember{open: 1, serving: eight},
			side: eng, mechanism: inPlace, already: 1, wantAllowed: true,
		},
		{
			name:      "8:8 at 25, two patches under way beside a decode side at none: a third would stand three ahead, past the allowance of two",
			tolerance: 25, engine: lockstepMember{open: 2},
			side: eng, mechanism: inPlace,
			wantAllowed: false, wantReason: inPlaceLockstepReason,
		},
		{
			name:      "8:4 at 50, three patches under way beside a decode side at none: the fourth lands on the engine's allowance of four and is admitted",
			tolerance: 50, engine: lockstepMember{open: 3}, decode: lockstepMember{count: 4},
			side: eng, mechanism: inPlace, wantAllowed: true,
		},
		{
			name:      "8:4 at 50, four patches under way beside a decode side at none: a fifth would stand five ahead, past the allowance of four",
			tolerance: 50, engine: lockstepMember{open: 4}, decode: lockstepMember{count: 4},
			side: eng, mechanism: inPlace,
			wantAllowed: false, wantReason: inPlaceLockstepReason,
		},
		{
			name:      "8:4 at 50, five patches under way beside one decode Instance landed, two engine Instances' worth: the sixth lands four ahead and is admitted",
			tolerance: 50, engine: lockstepMember{open: 5}, decode: lockstepMember{count: 4, promoted: 1},
			side: eng, mechanism: inPlace, wantAllowed: true,
		},
		{
			name:      "8:4 at 50, six patches under way beside one decode Instance landed: the seventh would stand five ahead and is held",
			tolerance: 50, engine: lockstepMember{open: 6}, decode: lockstepMember{count: 4, promoted: 1},
			side: eng, mechanism: inPlace,
			wantAllowed: false, wantReason: inPlaceLockstepReason,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc, c := lockstepPair(t, tc.tolerance, tc.engine, tc.decode)
			allowed, gate, reason := batchGate(t, isvc, c, tc.side, tc.mechanism, tc.already)
			if allowed != tc.wantAllowed {
				t.Fatalf("allowed=%v want %v (gate=%q reason=%q)", allowed, tc.wantAllowed, gate, reason)
			}
			if tc.wantAllowed {
				return
			}
			if gate != v1beta1.RolloutHoldGateRatio {
				t.Errorf("hold gate=%q want %q (reason=%q)", gate, v1beta1.RolloutHoldGateRatio, reason)
			}
			if reason != tc.wantReason {
				t.Errorf("hold reason=%q want %q", reason, tc.wantReason)
			}
		})
	}
}

// TestEvaluateUpdateGate_LockstepStopsAPassAtTheSidesAllowance drives the
// story's engine side through the gate the way the dispatcher does within
// one pass, with nothing of the decode side moving and the group's budgets
// out of the way: every admitted start is written on its row and reported
// out of rotation before the next is consulted. The pass admits exactly
// the engine's allowance of patches, the tolerance of its own count
// rounded up to a whole Instance and at least one, and holds the next with
// the lockstep's reason. At tolerance 5 and 10 one Instance of eight
// already exceeds the tolerance, so one patch is admitted; at 20 and at 25
// two; at 50 four.
func TestEvaluateUpdateGate_LockstepStopsAPassAtTheSidesAllowance(t *testing.T) {
	eng := v1beta1.EngineComponent
	eight := int32Ptr(8)
	for _, tc := range []struct {
		tolerance  int32
		wantStarts int32
	}{{tolerance: 5, wantStarts: 1}, {tolerance: 10, wantStarts: 1}, {tolerance: 20, wantStarts: 2}, {tolerance: 25, wantStarts: 2}, {tolerance: 50, wantStarts: 4}} {
		var starts int32
		for {
			isvc, c := lockstepPair(t, tc.tolerance, lockstepMember{open: starts, serving: eight}, lockstepMember{})
			allowed, gate, reason := EvaluateUpdateGate(context.Background(), c, isvc, eng, "", forwardTarget(isvc, eng), nil, GroupDefaults{},
				workloadtypes.UpdateStrategyInPlaceIfPossible, 0, starts)
			if allowed {
				starts++
				if starts > 8 {
					t.Fatalf("tolerance %d: the pass admitted more patches than the engine has Instances", tc.tolerance)
				}
				continue
			}
			if gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "outpace") {
				t.Errorf("tolerance %d: the pass must end on the lockstep's hold, ended on gate=%q reason=%q", tc.tolerance, gate, reason)
			}
			break
		}
		if starts != tc.wantStarts {
			t.Errorf("tolerance %d: the pass admitted %d engine patches beside a decode side at none, want %d", tc.tolerance, starts, tc.wantStarts)
		}
	}
}
