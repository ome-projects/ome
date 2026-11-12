package coordination

import (
	"context"
	"slices"
	"strings"
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The group's skew signal, the RatioSkewRejected event and the status field
// surgeHeldByRatio, in every phase of a roll. The replica-side ratio gate
// runs for every start, whatever phase the group reads, so the group
// computes the signal wherever a start may still be asked: while a rolling
// member keeps a pod on an old revision. A group with nothing left to
// start, or at rest, raises none.

// skewPair is the pair the band and the lockstep pace on their own, with no
// batch step: 12 engines beside 8 routers, both SurgeThenDrain under
// maintainRatio at tolerance 20, with the given Instances on version 2 on
// each side and routerFailed router Instances whose surge ended Failed.
func skewPair(enginePromoted, routerPromoted, routerFailed int32) map[v1beta1.ComponentType]batchMemberFixture {
	surge := v1beta1.UpdateStrategySurgeThenDrain
	return map[v1beta1.ComponentType]batchMemberFixture{
		v1beta1.EngineComponent: {desired: 12, surge: "10%", strategy: surge, promoted: enginePromoted},
		v1beta1.RouterComponent: {desired: 8, surge: "10%", strategy: surge, promoted: routerPromoted, failed: routerFailed},
	}
}

// skewTransition runs the group machine under policy over the engine and
// the router as the coordination pass observes them from their replicas
// and pods, the group under maintainRatio at tolerance 20 with its
// reference at the members' counts; edit changes the observations before
// the pass.
func skewTransition(t *testing.T, policy v1beta1.CoordinationPolicy, members map[v1beta1.ComponentType]batchMemberFixture, edit func(map[v1beta1.ComponentType]*ComponentObservation)) GroupTransition {
	t.Helper()
	return skewTransitionOver(t, policy, []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent}, members, edit)
}

// skewTransitionOver is skewTransition over the listed members, in the
// group's order.
func skewTransitionOver(t *testing.T, policy v1beta1.CoordinationPolicy, components []v1beta1.ComponentType, members map[v1beta1.ComponentType]batchMemberFixture, edit func(map[v1beta1.ComponentType]*ComponentObservation)) GroupTransition {
	t.Helper()
	group := ResolvedGroup{
		Name: "0", Policy: policy, Components: components,
		Pacing: v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced, RatioTolerancePercent: ptr.To(int32(20))},
	}
	isvc := batchService(batchGroupFixture{tolerance: ptr.To(int32(20)), components: group.Components, blueGreen: policy == v1beta1.CoordinationPolicyBlueGreen}, members)
	pods := map[v1beta1.ComponentType]map[string]int32{}
	for comp, m := range members {
		pods[comp] = map[string]int32{batchRevision(comp, "old"): m.desired - m.promoted, batchRevision(comp, "target"): m.promoted}
	}
	obs, err := buildGroupObservation(context.Background(), batchClient(t, members), isvc, group, pods)
	if err != nil {
		t.Fatalf("observe the group: %v", err)
	}
	obs.OriginalReplicas = map[v1beta1.ComponentType]int32{}
	byComponent := map[v1beta1.ComponentType]*ComponentObservation{}
	for comp, m := range members {
		co := obs.Components[comp]
		byComponent[comp] = &co
		obs.OriginalReplicas[comp] = m.desired
	}
	if edit != nil {
		edit(byComponent)
	}
	for comp, co := range byComponent {
		obs.Components[comp] = *co
	}
	return ComputeTransition(obs)
}

// TestComputeTransition_SkewSignalFollowsTheRatioGateIntoEveryPhase is the
// operator's story: 12 engines beside 8 routers under maintainRatio at
// tolerance 20, both SurgeThenDrain, 4 engines and 1 router on version 2,
// every new pod Ready and the old pods still serving. The engine runs ahead
// of the router by more than the tolerance, so the ratio gate holds its next
// surge whatever the group reads: ScalingDown under RollingUpdate, Shifting
// under BlueGreen, and Failed beside a router Instance whose surge ended
// Failed. The group's skew signal says so in each of them, naming the
// engine's surge start as the held move, so the event RatioSkewRejected and
// the status field surgeHeldByRatio follow the hold as they do in Surging
// and Waiting.
func TestComputeTransition_SkewSignalFollowsTheRatioGateIntoEveryPhase(t *testing.T) {
	rolling, blueGreen := v1beta1.CoordinationPolicyRollingUpdate, v1beta1.CoordinationPolicyBlueGreen
	cases := []struct {
		name    string
		policy  v1beta1.CoordinationPolicy
		members map[v1beta1.ComponentType]batchMemberFixture
		phase   v1beta1.CoordinationPhase
	}{
		{"between-two-steps-under-rollingupdate", rolling, skewPair(4, 1, 0), v1beta1.CoordinationPhaseScalingDown},
		{"between-two-steps-under-bluegreen", blueGreen, skewPair(4, 1, 0), v1beta1.CoordinationPhaseShifting},
		{"beside-a-failed-router-under-rollingupdate", rolling, skewPair(4, 1, 1), v1beta1.CoordinationPhaseFailed},
		{"beside-a-failed-router-under-bluegreen", blueGreen, skewPair(4, 1, 1), v1beta1.CoordinationPhaseFailed},
	}
	engineHeld := []HeldStart{{Component: v1beta1.EngineComponent, Strategy: workloadtypes.UpdateStrategySurgeThenDrain}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := batchService(batchGroupFixture{tolerance: ptr.To(int32(20)), components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent}, blueGreen: tc.policy == blueGreen}, tc.members)
			allowed, gate, reason := batchGate(t, isvc, batchClient(t, tc.members), v1beta1.EngineComponent, workloadtypes.UpdateStrategySurgeThenDrain, 0)
			if allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "outpace") {
				t.Fatalf("the gate answers allowed=%v gate=%q reason=%q, want the engine held by the lockstep", allowed, gate, reason)
			}
			tr := skewTransition(t, tc.policy, tc.members, nil)
			if tr.Phase != tc.phase {
				t.Fatalf("phase %s, want %s", tr.Phase, tc.phase)
			}
			if !tr.RatioSkewRejected {
				t.Errorf("the group reads %s with the engine held by the ratio gate and raises no skew signal; RatioSkewRejected and surgeHeldByRatio would not be recorded", tr.Phase)
			}
			if !slices.Equal(tr.RatioHeldStarts, engineHeld) {
				t.Errorf("the group names %v as held in %s, want the engine's surge start alone", tr.RatioHeldStarts, tr.Phase)
			}
		})
	}
}

// TestComputeTransition_NoSkewSignalWhereNoStartCanBeAsked holds the signal
// to the starts the gate can still be asked for. In Surging a level pair at
// the start of the roll raises none; in Waiting the same lead raises it
// while a router's new pod is not yet Ready. Where no start can be asked it
// is never raised: a RollingUpdate group reading Shifting with every pod of
// both members on version 2 and nothing converged yet, the same picture
// under BlueGreen reading ScalingDown, a group at rest on a partition
// (Staged), an Idle group, and a paused one. The held starts the signal
// names are empty exactly when it is not raised.
func TestComputeTransition_NoSkewSignalWhereNoStartCanBeAsked(t *testing.T) {
	rolling, blueGreen := v1beta1.CoordinationPolicyRollingUpdate, v1beta1.CoordinationPolicyBlueGreen
	routerNewPodNotReady := func(m map[v1beta1.ComponentType]*ComponentObservation) {
		m[v1beta1.RouterComponent].NewRevisionReadyPods--
	}
	allOnVersionTwo := func(m map[v1beta1.ComponentType]*ComponentObservation) {
		for _, co := range m {
			co.NewRevisionPods, co.NewRevisionReadyPods = co.TotalPods, co.TotalPods
		}
	}
	staged := func(m map[v1beta1.ComponentType]*ComponentObservation) {
		for _, co := range m {
			co.Partition, co.AtDesiredShape = 1, true
		}
	}
	cases := []struct {
		name   string
		policy v1beta1.CoordinationPolicy
		edit   func(map[v1beta1.ComponentType]*ComponentObservation)
		phase  v1beta1.CoordinationPhase
		skew   bool
	}{
		{"surging-level-pair-no-skew", rolling, nil, v1beta1.CoordinationPhaseSurging, false},
		{"waiting-engine-ahead-skew", rolling, routerNewPodNotReady, v1beta1.CoordinationPhaseWaiting, true},
		{"shifting-nothing-left-to-start", rolling, allOnVersionTwo, v1beta1.CoordinationPhaseShifting, false},
		{"scalingdown-nothing-left-to-start-under-bluegreen", blueGreen, allOnVersionTwo, v1beta1.CoordinationPhaseScalingDown, false},
		{"staged-under-rollingupdate", rolling, staged, v1beta1.CoordinationPhaseStaged, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members := skewPair(4, 1, 0)
			if tc.phase == v1beta1.CoordinationPhaseSurging {
				members = skewPair(0, 0, 0)
			}
			tr := skewTransition(t, tc.policy, members, tc.edit)
			if tr.Phase != tc.phase {
				t.Fatalf("phase %s, want %s", tr.Phase, tc.phase)
			}
			if tr.RatioSkewRejected != tc.skew {
				t.Errorf("RatioSkewRejected=%v in %s, want %v", tr.RatioSkewRejected, tr.Phase, tc.skew)
			}
			if (len(tr.RatioHeldStarts) > 0) != tr.RatioSkewRejected {
				t.Errorf("held starts %v beside RatioSkewRejected=%v in %s; the signal names a held start exactly when it is raised", tr.RatioHeldStarts, tr.RatioSkewRejected, tr.Phase)
			}
		})
	}
	t.Run("idle-and-paused-raise-none", func(t *testing.T) {
		members := map[v1beta1.ComponentType]batchMemberFixture{
			v1beta1.EngineComponent: {desired: 12, surge: "10%", strategy: v1beta1.UpdateStrategySurgeThenDrain, promoted: 12},
			v1beta1.RouterComponent: {desired: 8, surge: "10%", strategy: v1beta1.UpdateStrategySurgeThenDrain, promoted: 8},
		}
		settled := func(m map[v1beta1.ComponentType]*ComponentObservation) {
			for _, co := range m {
				co.RolloutInFlight, co.AtDesiredShape = false, true
			}
		}
		if tr := skewTransition(t, rolling, members, settled); tr.Phase != v1beta1.CoordinationPhaseIdle || tr.RatioSkewRejected || len(tr.RatioHeldStarts) > 0 {
			t.Errorf("settled pair reads %s with RatioSkewRejected=%v held=%v, want Idle and no signal", tr.Phase, tr.RatioSkewRejected, tr.RatioHeldStarts)
		}
		obs := GroupObservation{PausedGlobal: true, Group: ResolvedGroup{Name: "0", Policy: rolling}}
		if tr := ComputeTransition(obs); tr.Phase != v1beta1.CoordinationPhasePaused || tr.RatioSkewRejected || len(tr.RatioHeldStarts) > 0 {
			t.Errorf("paused group reads %s with RatioSkewRejected=%v held=%v, want Paused and no signal", tr.Phase, tr.RatioSkewRejected, tr.RatioHeldStarts)
		}
	})
}
