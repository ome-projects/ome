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
	pinActiveRun(isvc) // re-pin: the surge-budget edit must be part of the pinned plan
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

func TestCheckRatio_CapacityRecoveryUsesAuthoritativeIRState(t *testing.T) {
	isvc := mkZeroServingRatioFixture()
	failed := make([]v1beta1.OMENativeInstanceStatus, 8)
	for i := range failed {
		failed[i] = v1beta1.OMENativeInstanceStatus{
			Index: int32(i),
			Phase: v1beta1.OMENativeInstanceFailed,
		}
	}
	client := fakeClientForISVCWithInstances(isvc, map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{
		v1beta1.EngineComponent:  failed,
		v1beta1.DecoderComponent: failed,
	})

	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, nil, GroupDefaults{},
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Fatalf("an all-zero serving group with positive authoritative shape must start recovery: %s", reason)
	}
	if allowed, _, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, nil, GroupDefaults{},
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
