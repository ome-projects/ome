package coordination

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestEvaluateSurge_CapacityRecoveryLadders(t *testing.T) {
	engine := v1beta1.EngineComponent
	decoder := v1beta1.DecoderComponent
	router := v1beta1.RouterComponent
	tolerance := int32(25)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tolerance,
	}
	tests := []struct {
		name     string
		original map[v1beta1.ComponentType]int32
		desired  map[v1beta1.ComponentType]int32
	}{
		{
			name: "symmetric",
			desired: map[v1beta1.ComponentType]int32{
				engine:  8,
				decoder: 8,
			},
		},
		{
			name: "asymmetric",
			desired: map[v1beta1.ComponentType]int32{
				engine:  8,
				decoder: 4,
			},
		},
		{
			name: "asymmetric indivisible steps",
			desired: map[v1beta1.ComponentType]int32{
				engine:  3,
				decoder: 2,
			},
		},
		{
			name: "three components",
			desired: map[v1beta1.ComponentType]int32{
				engine:  8,
				decoder: 4,
				router:  2,
			},
		},
		{
			name: "desired reshape differs from original anchor",
			original: map[v1beta1.ComponentType]int32{
				engine:  8,
				decoder: 8,
			},
			desired: map[v1beta1.ComponentType]int32{
				engine:  8,
				decoder: 4,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.original
			if original == nil {
				original = tc.desired
			}
			components := make([]v1beta1.ComponentType, 0, len(tc.desired))
			state := RatioState{
				Original:         original,
				Current:          make(map[v1beta1.ComponentType]int32, len(tc.desired)),
				Desired:          make(map[v1beta1.ComponentType]int32, len(tc.desired)),
				NewPods:          make(map[v1beta1.ComponentType]int32, len(tc.desired)),
				Serving:          make(map[v1beta1.ComponentType]int32, len(tc.desired)),
				RecoveryEligible: make(map[v1beta1.ComponentType]bool, len(tc.desired)),
				InFlightSurge:    map[v1beta1.ComponentType]int32{},
			}
			var wantSteps int32
			for _, component := range []v1beta1.ComponentType{engine, decoder, router} {
				target, ok := tc.desired[component]
				if !ok {
					continue
				}
				components = append(components, component)
				state.Current[component] = target
				state.Desired[component] = target
				state.RecoveryEligible[component] = true
				wantSteps += target
			}
			var steps int32

			for steps < wantSteps {
				progressed := false
				for _, component := range components {
					state.RecoveryEligible[component] = state.Serving[component] < state.Desired[component]
					if !state.RecoveryEligible[component] {
						continue
					}
					decision := EvaluateSurge(pacing, state, component, 1)
					if decision.AllowedSurgeDelta == 0 {
						continue
					}
					if decision.AllowedSurgeDelta != 1 {
						t.Fatalf("recovery must advance one replica at a time: got %+v", decision)
					}
					state.Serving[component]++
					state.NewPods[component]++
					steps++
					progressed = true
					break
				}
				if !progressed {
					t.Fatalf("capacity recovery deadlocked after %d steps at serving=%v", steps, state.Serving)
				}
			}

			for _, component := range components {
				if state.Serving[component] != tc.desired[component] {
					t.Fatalf("recovery did not reach desired shape: serving=%v desired=%v", state.Serving, tc.desired)
				}
			}
		})
	}
}

func TestEvaluateSurge_CapacityRecoveryUsesDesiredRatio(t *testing.T) {
	tolerance := int32(25)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tolerance,
	}
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  8,
			v1beta1.DecoderComponent: 8,
		},
		Current: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  8,
			v1beta1.DecoderComponent: 8,
		},
		Desired: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  8,
			v1beta1.DecoderComponent: 4,
		},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  2,
			v1beta1.DecoderComponent: 2,
		},
		RecoveryEligible: map[v1beta1.ComponentType]bool{
			v1beta1.EngineComponent:  true,
			v1beta1.DecoderComponent: true,
		},
		InFlightSurge: map[v1beta1.ComponentType]int32{},
	}

	if decision := EvaluateSurge(pacing, state, v1beta1.DecoderComponent, 1); decision.AllowedSurgeDelta != 0 {
		t.Fatalf("decoder is ahead in the desired 8:4 ratio and must wait: %+v", decision)
	}
	if decision := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 1); decision.AllowedSurgeDelta != 1 {
		t.Fatalf("engine is behind in the desired 8:4 ratio and must advance: %+v", decision)
	}
}

func TestEvaluateSurge_CapacityDeficitPreservesStrictMultiDelta(t *testing.T) {
	tolerance := int32(50)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tolerance,
	}
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  8,
			v1beta1.DecoderComponent: 8,
		},
		Current: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  8,
			v1beta1.DecoderComponent: 8,
		},
		Desired: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  8,
			v1beta1.DecoderComponent: 8,
		},
		NewPods: map[v1beta1.ComponentType]int32{},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  4,
			v1beta1.DecoderComponent: 4,
		},
		RecoveryEligible: map[v1beta1.ComponentType]bool{
			v1beta1.EngineComponent:  true,
			v1beta1.DecoderComponent: true,
		},
	}

	decision := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 2)
	if decision.AllowedSurgeDelta != 2 || decision.Reason != "within tolerance" {
		t.Fatalf("recovery fallback must preserve an ordinary in-band multi-surge decision: %+v", decision)
	}
}

func TestEvaluateSurge_CapacityRecoverySerializesAndKeepsDrainsStrict(t *testing.T) {
	tolerance := int32(25)
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: &tolerance,
	}
	state := RatioState{
		Original: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  8,
			v1beta1.DecoderComponent: 8,
		},
		Current: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  8,
			v1beta1.DecoderComponent: 8,
		},
		Desired: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  8,
			v1beta1.DecoderComponent: 8,
		},
		Serving: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent:  0,
			v1beta1.DecoderComponent: 0,
		},
		RecoveryEligible: map[v1beta1.ComponentType]bool{
			v1beta1.EngineComponent:  true,
			v1beta1.DecoderComponent: true,
		},
		InFlightSurge: map[v1beta1.ComponentType]int32{
			v1beta1.EngineComponent: 1,
		},
	}

	if decision := EvaluateSurge(pacing, state, v1beta1.EngineComponent, 4); decision.AllowedSurgeDelta != 0 {
		t.Fatalf("an active recovery surge must serialize later starts: %+v", decision)
	}
	if decision := EvaluateSurge(pacing, state, v1beta1.DecoderComponent, 4); decision.AllowedSurgeDelta != 1 {
		t.Fatalf("an independent zero-serving peer must retain its one-pod bootstrap: %+v", decision)
	}
	if decision := EvaluateSurge(pacing, state, v1beta1.DecoderComponent, -1); decision.AllowedSurgeDelta != 0 {
		t.Fatalf("capacity recovery must not relax the strict drain path: %+v", decision)
	}
}

func mkZeroServingRatioFixture() *v1beta1.InferenceService {
	isvc := mkSymmetricRatioFixture(25)
	maxSurge := intstr.FromInt32(4)
	isvc.Spec.Rollout.Groups[0].RollingUpdate.MaxSurge = &maxSurge
	pinActiveRun(isvc) // re-pin so the recorded plan carries the surge budget the fixture declares
	isvc.Status.RolloutCoordination.Groups[0].ObservedRatio.Original = map[v1beta1.ComponentType]int32{
		v1beta1.EngineComponent:  8,
		v1beta1.DecoderComponent: 8,
	}
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 8, ServingReplicas: 0},
		},
		v1beta1.DecoderComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 8, ServingReplicas: 0},
		},
	}
	return isvc
}

// The pair is an engine beside a router, which the batch step does not
// govern, so the band's recovery ladder answers through the full stack.
func TestCheckRatio_CapacityRecoveryUsesAuthoritativeIRState(t *testing.T) {
	isvc := withRouterForDecoder(mkZeroServingRatioFixture())
	failed := make([]v1beta1.OMENativeInstanceStatus, 8)
	for i := range failed {
		failed[i] = v1beta1.OMENativeInstanceStatus{
			Index: int32(i),
			Phase: v1beta1.OMENativeInstanceFailed,
		}
	}
	client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{
		v1beta1.EngineComponent: failed,
		v1beta1.RouterComponent: failed,
	})

	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, "", forwardTarget(isvc, v1beta1.EngineComponent), nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Fatalf("an all-zero serving group with positive authoritative shape must start recovery: %s", reason)
	}
	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, "", forwardTarget(isvc, v1beta1.EngineComponent), nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 1, 0); allowed {
		t.Fatalf("same-wakeup surge accounting must prevent repeated bootstrap even with MaxSurge greater than one: %s", reason)
	}
}

func TestCheckRatio_CapacityRecoveryCountsPersistedSurge(t *testing.T) {
	isvc := mkZeroServingRatioFixture()
	engineInstances := []v1beta1.OMENativeInstanceStatus{{
		Index: 0,
		Phase: v1beta1.OMENativeInstanceUpdating,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationUpdate,
			Step: workloadtypes.UpdateStepSurge,
		},
	}}
	client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{
		v1beta1.EngineComponent: engineInstances,
	})

	engineCtx := ResolveGateContext(context.Background(), client, isvc, v1beta1.EngineComponent)
	if allowed, reason := engineCtx.CheckRatio(0, 0, +1); allowed {
		t.Fatalf("persisted surge must prevent a second recovery bootstrap: %s", reason)
	}
	decoderCtx := ResolveGateContext(context.Background(), client, isvc, v1beta1.DecoderComponent)
	if allowed, reason := decoderCtx.CheckRatio(0, 0, +1); !allowed {
		t.Fatalf("a peer without an active surge must retain its bootstrap: %s", reason)
	}
}

func TestCheckRatio_CapacityRecoveryRequiresCompletePositiveObservation(t *testing.T) {
	t.Run("missing member IR uses strict ratio path", func(t *testing.T) {
		isvc := mkZeroServingRatioFixture()
		delete(isvc.Status.Components, v1beta1.DecoderComponent)
		ctx := ResolveGateContext(context.Background(), fakeClientForISVC(isvc), isvc, v1beta1.EngineComponent)
		if allowed, reason := ctx.CheckRatio(0, 0, +1); allowed {
			t.Fatalf("missing member IR must not activate recovery relaxation: %s", reason)
		}
	})

	t.Run("missing member IR preserves nil-tolerance pass-through", func(t *testing.T) {
		isvc := mkZeroServingRatioFixture()
		isvc.Spec.Rollout.Groups[0].MaintainRatio.Tolerance = nil
		pinActiveRun(isvc) // re-pin: the tolerance edit must be part of the pinned plan
		delete(isvc.Status.Components, v1beta1.DecoderComponent)
		ctx := ResolveGateContext(context.Background(), fakeClientForISVC(isvc), isvc, v1beta1.EngineComponent)
		if allowed, reason := ctx.CheckRatio(0, 0, +1); !allowed || !strings.Contains(reason, "no ratio tolerance") {
			t.Fatalf("missing IR must preserve the strict path's nil-tolerance decision: allowed=%t reason=%q", allowed, reason)
		}
	})

	t.Run("non-positive member shape disables recovery", func(t *testing.T) {
		isvc := mkZeroServingRatioFixture()
		isvc.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 0, ServingReplicas: 0},
		}
		ctx := ResolveGateContext(context.Background(), fakeClientForISVC(isvc), isvc, v1beta1.EngineComponent)
		if allowed, reason := ctx.CheckRatio(0, 0, +1); allowed {
			t.Fatalf("an incompletely observed group must not use recovery relaxation: %s", reason)
		}
	})
}

func TestCheckRatio_CapacityRecoveryDistinguishesDeficitFromScaleDown(t *testing.T) {
	t.Run("zero-serving positive shape bootstraps", func(t *testing.T) {
		isvc := mkSymmetricRatioFixture(25)
		isvc.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 0},
		}
		ctx := ResolveGateContext(context.Background(), fakeClientForISVC(isvc), isvc, v1beta1.DecoderComponent)
		if allowed, reason := ctx.CheckRatio(0, 0, +1); !allowed {
			t.Fatalf("zero serving below a positive authoritative shape must bootstrap: %s", reason)
		}
	})

	t.Run("fully serving smaller shape stays strict", func(t *testing.T) {
		isvc := mkSymmetricRatioFixture(25)
		isvc.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 2},
		}
		client := fakeClientForISVCWithDesiredAndInstances(isvc, map[v1beta1.ComponentType]*int32{
			v1beta1.DecoderComponent: ptr.To(int32(2)),
		}, nil)
		ctx := ResolveGateContext(context.Background(), client, isvc, v1beta1.DecoderComponent)
		if allowed, reason := ctx.CheckRatio(0, 0, +1); allowed {
			t.Fatalf("serving at desired scale with stale extra status Instances has no recovery deficit: %s", reason)
		}
	})

	t.Run("explicit zero desired shape does not recover", func(t *testing.T) {
		isvc := mkSymmetricRatioFixture(25)
		isvc.Status.Components[v1beta1.DecoderComponent] = v1beta1.ComponentStatusSpec{
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 0},
		}
		client := fakeClientForISVCWithDesiredAndInstances(isvc, map[v1beta1.ComponentType]*int32{
			v1beta1.DecoderComponent: ptr.To(int32(0)),
		}, nil)
		ctx := ResolveGateContext(context.Background(), client, isvc, v1beta1.DecoderComponent)
		if allowed, reason := ctx.CheckRatio(0, 0, +1); allowed {
			t.Fatalf("scale-to-zero must not be mistaken for a serving deficit: %s", reason)
		}
	})

	t.Run("explicit zero desired shape keeps strict drains", func(t *testing.T) {
		isvc := mkSymmetricRatioFixture(25)
		client := fakeClientForISVCWithDesiredAndInstances(isvc, map[v1beta1.ComponentType]*int32{
			v1beta1.DecoderComponent: ptr.To(int32(0)),
		}, nil)
		ctx := ResolveGateContext(context.Background(), client, isvc, v1beta1.DecoderComponent)
		if allowed, reason := ctx.CheckRatio(0, 0, -1); !allowed {
			t.Fatalf("scale-to-zero must keep the existing strict drain decision: %s", reason)
		}
	})

	t.Run("nil desired shape uses API default", func(t *testing.T) {
		isvc := mkZeroServingRatioFixture()
		client := fakeClientForISVCWithDesiredAndInstances(isvc, map[v1beta1.ComponentType]*int32{
			v1beta1.EngineComponent:  nil,
			v1beta1.DecoderComponent: nil,
		}, nil)
		ctx := ResolveGateContext(context.Background(), client, isvc, v1beta1.EngineComponent)
		if allowed, reason := ctx.CheckRatio(0, 0, +1); !allowed {
			t.Fatalf("omitted spec.replicas defaults to one and may recover its missing serving slot: %s", reason)
		}
	})
}

// mkScaleUpRatioState is the picture of a group scaled up while its roll
// settles, in the pass before the ISVC pass rewrites the reference to the
// new counts: 6 engines and 2 decoders behind 2 routers in one RatioBalanced
// group, every member SurgeThenDrain, scaled to 7 engines and 3 decoders. The
// new Instances do not serve yet, so the engine and the decoder each serve
// fewer pods than they desire and read as recovery-eligible, while each still
// serves its recorded count. The decoder has rolled 1 of its 3 desired
// Instances, the engine newE of its 7 and the routers newR of their 2.
func mkScaleUpRatioState(newE, newR int32) RatioState {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	return RatioState{
		Original:         map[v1beta1.ComponentType]int32{eng: 6, dec: 2, rtr: 2},
		Current:          map[v1beta1.ComponentType]int32{eng: 7, dec: 3, rtr: 2},
		Desired:          map[v1beta1.ComponentType]int32{eng: 7, dec: 3, rtr: 2},
		Serving:          map[v1beta1.ComponentType]int32{eng: 6, dec: 2, rtr: 2},
		NewPods:          map[v1beta1.ComponentType]int32{eng: newE, dec: 1, rtr: newR},
		RecoveryEligible: map[v1beta1.ComponentType]bool{eng: true, dec: true},
		InFlightSurge:    map[v1beta1.ComponentType]int32{},
	}
}

// TestEvaluateSurge_ScaleUpKeepsLockstepArmed pins that the recovery
// fallback cannot disarm the new-revision lockstep. With the group scaled up
// mid-roll every member is short of its desired count, but none has lost a
// pod: each still serves its recorded count. The decoder's second surge (1 of
// 3 rolled, the engine at 0 of 7, a lead of 33 points at a tolerance of 10)
// is refused by the lockstep and stays refused through the fallback; the
// engine, behind, proceeds; once the engine is level the decoder's surge is
// admitted again, so the pair converges.
func TestEvaluateSurge_ScaleUpKeepsLockstepArmed(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: int32Ptr(10),
	}
	const lockstepReason = "surge would outpace peer new-revision rollout past tolerance"

	if d := EvaluateSurge(pacing, mkScaleUpRatioState(0, 0), dec, 1); d.AllowedSurgeDelta != 0 || !d.SkewRejected || d.Reason != lockstepReason {
		t.Fatalf("decoder 1 of 3 against engine 0 of 7 at tol 10 must stay held by the lockstep: %+v", d)
	}
	if d := EvaluateSurge(pacing, mkScaleUpRatioState(1, 1), dec, 1); d.AllowedSurgeDelta != 0 || d.Reason != lockstepReason {
		t.Fatalf("decoder 1 of 3 against engine 1 of 7 (a lead of 19 points) must stay held: %+v", d)
	}
	if d := EvaluateSurge(pacing, mkScaleUpRatioState(0, 0), eng, 1); d.AllowedSurgeDelta != 1 {
		t.Fatalf("the engine, behind, must proceed: %+v", d)
	}
	if d := EvaluateSurge(pacing, mkScaleUpRatioState(3, 1), dec, 1); d.AllowedSurgeDelta != 1 {
		t.Fatalf("decoder 1 of 3 behind engine 3 of 7 and routers 1 of 2 must proceed again: %+v", d)
	}
	// The hold is the lockstep's, whatever the member's recovery standing:
	// a surge the fallback could otherwise serialize is refused the same way.
	state := mkScaleUpRatioState(0, 0)
	state.InFlightSurge[dec] = 1
	if d := EvaluateSurge(pacing, state, dec, 1); d.AllowedSurgeDelta != 0 || d.Reason != lockstepReason {
		t.Fatalf("an in-flight surge changes nothing about the lockstep's hold: %+v", d)
	}
}

// TestEvaluateSurge_LostServingPodRecovers pins what the fallback does: a
// member that lost a serving pod on the running revision refills
// it, with a level peer through the band's rejection and, below its anchor,
// even while the lockstep would hold it. The same lead at the anchor count
// stays held, so the anchor is the boundary.
func TestEvaluateSurge_LostServingPodRecovers(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := func(tol int32) v1beta1.CoordinationPacing {
		return v1beta1.CoordinationPacing{
			Type:                  v1beta1.CoordinationPacingRatioBalanced,
			RatioTolerancePercent: int32Ptr(tol),
		}
	}
	state := func(servingE, desiredE, newE int32) RatioState {
		return RatioState{
			Original:         map[v1beta1.ComponentType]int32{eng: 8, dec: 8},
			Current:          map[v1beta1.ComponentType]int32{eng: 8, dec: 8},
			Desired:          map[v1beta1.ComponentType]int32{eng: desiredE, dec: 8},
			Serving:          map[v1beta1.ComponentType]int32{eng: servingE, dec: 8},
			NewPods:          map[v1beta1.ComponentType]int32{eng: newE, dec: 0},
			RecoveryEligible: map[v1beta1.ComponentType]bool{eng: servingE < desiredE},
			InFlightSurge:    map[v1beta1.ComponentType]int32{},
		}
	}
	const recoveryReason = "capacity recovery advances a lagging Component"

	// Four engine pods lost, nobody ahead: the band refuses 5:8 at tol 25 and
	// the fallback refills.
	if d := EvaluateSurge(pacing(25), state(4, 8, 0), eng, 1); d.AllowedSurgeDelta != 1 || d.Reason != recoveryReason {
		t.Fatalf("an engine at 4 of 8 serving with a level peer must recover: %+v", d)
	}
	// Two engine pods lost while the engine is 3 of 8 rolled against a
	// decoder at none: the lockstep would hold a lead of 37 points at tol 20,
	// but the engine serves below its anchor, so the refill goes through.
	if d := EvaluateSurge(pacing(20), state(6, 8, 3), eng, 1); d.AllowedSurgeDelta != 1 || d.Reason != recoveryReason {
		t.Fatalf("an engine below its anchor must keep recovering past the lockstep: %+v", d)
	}
	// Every recorded pod serving and one more desired (a scale-up the ISVC
	// pass has not yet written into the reference): nothing was lost, and 3
	// of 9 against none is still a lead of 33 points, so the lockstep holds.
	if d := EvaluateSurge(pacing(20), state(8, 9, 3), eng, 1); d.AllowedSurgeDelta != 0 || !d.SkewRejected {
		t.Fatalf("an engine at its recorded count that only desires more must stay held: %+v", d)
	}
}

// TestEvaluateSurge_ScaleUpWithNoRollingPeerSurgesFreely pins that the
// rule only bites while a peer is still rolling: a member alone in its group
// surges freely, and a member whose peer has finished its roll is admitted
// by the surge tiebreaker.
func TestEvaluateSurge_ScaleUpWithNoRollingPeerSurgesFreely(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pacing := v1beta1.CoordinationPacing{
		Type:                  v1beta1.CoordinationPacingRatioBalanced,
		RatioTolerancePercent: int32Ptr(10),
	}
	alone := RatioState{
		Original:         map[v1beta1.ComponentType]int32{eng: 6},
		Current:          map[v1beta1.ComponentType]int32{eng: 7},
		Desired:          map[v1beta1.ComponentType]int32{eng: 7},
		Serving:          map[v1beta1.ComponentType]int32{eng: 6},
		NewPods:          map[v1beta1.ComponentType]int32{eng: 4},
		RecoveryEligible: map[v1beta1.ComponentType]bool{eng: true},
	}
	if d := EvaluateSurge(pacing, alone, eng, 1); d.AllowedSurgeDelta != 1 {
		t.Fatalf("a single-member group scaled up mid-roll must keep surging: %+v", d)
	}
	finished := mkScaleUpRatioState(6, 2)
	if d := EvaluateSurge(pacing, finished, dec, 1); d.AllowedSurgeDelta != 1 {
		t.Fatalf("peers fully rolled no longer pace the decoder: %+v", d)
	}
}

// mkScaleUpRatioFixture is the scale-up picture of mkScaleUpRatioState as the
// production gate reads it, on a pair the batch step does not govern: an
// engine beside a router with an anchor of 6:2, the two desiring 7 and 3
// with their new Instances not serving yet, the router 1 of 2 rolled and the
// engine at none.
func mkScaleUpRatioFixture() *v1beta1.InferenceService {
	eng, rtr := v1beta1.EngineComponent, v1beta1.RouterComponent
	isvc := mkRatioProgressFixture(10, 6, 0, 2, 1)
	isvc.Status.Components[eng] = v1beta1.ComponentStatusSpec{Lifecycle: &v1beta1.LifecycleStatus{
		Replicas: 7, ServingReplicas: 6, UpdatedReplicas: 0,
	}}
	isvc.Status.Components[rtr] = v1beta1.ComponentStatusSpec{Lifecycle: &v1beta1.LifecycleStatus{
		Replicas: 3, ServingReplicas: 2, UpdatedReplicas: 1,
	}}
	return isvc
}

// TestEvaluateUpdateGate_ScaleUpKeepsLockstepArmed reads the scale-up picture
// through the production decision site, where recovery eligibility is derived
// from the authoritative InferenceReplica shape: the router's second surge
// is held on gate Ratio by the lockstep, the engine proceeds, and a router
// that really lost a pod is still refilled.
func TestEvaluateUpdateGate_ScaleUpKeepsLockstepArmed(t *testing.T) {
	eng, rtr := v1beta1.EngineComponent, v1beta1.RouterComponent
	isvc := mkScaleUpRatioFixture()
	client := fakeClientForISVC(isvc)
	allowed, gate, reason := EvaluateUpdateGate(context.Background(), client, isvc, rtr, "", forwardTarget(isvc, rtr), nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0)
	if allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "outpace") {
		t.Fatalf("router 1 of 2 against engine 0 of 6 must be held on gate Ratio by the lockstep after the scale-up: allowed=%v gate=%q reason=%q", allowed, gate, reason)
	}
	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, eng, "", forwardTarget(isvc, eng), nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Fatalf("the engine, behind, must proceed: %s", reason)
	}

	lost := mkScaleUpRatioFixture()
	lost.Status.Components[rtr] = v1beta1.ComponentStatusSpec{Lifecycle: &v1beta1.LifecycleStatus{
		Replicas: 3, ServingReplicas: 1, UpdatedReplicas: 1,
	}}
	routerCtx := ResolveGateContext(context.Background(), fakeClientForISVC(lost), lost, rtr)
	if allowed, reason := routerCtx.CheckRatio(0, 0, +1); !allowed || !strings.Contains(reason, "capacity recovery") {
		t.Fatalf("a router that lost a serving pod below its anchor must still be refilled: allowed=%v reason=%q", allowed, reason)
	}
}
