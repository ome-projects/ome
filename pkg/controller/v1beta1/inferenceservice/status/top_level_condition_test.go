package status

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func lifecycleWithMaxUnavailable(value intstr.IntOrString) *v1beta1.LifecycleSpec {
	return &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &value},
		},
	}
}

func replicas(i int32) *int32 { return &i }

// TestTopLevelComponentReadyFromLifecycle locks the Instance-level readiness
// contract: only lifecycle rollingUpdate.maxUnavailable relaxes the strict
// all-Instances-serving floor.
//
// The floor scales against the desired count, so rollout surge cannot raise it.
func TestTopLevelComponentReadyFromLifecycle(t *testing.T) {
	tests := []struct {
		name       string
		component  v1beta1.ComponentType
		cs         *v1beta1.LifecycleStatus
		lifecycle  *v1beta1.LifecycleSpec
		desired    *int32
		wantType   apis.ConditionType
		wantStatus corev1.ConditionStatus
		wantReason string
		wantNil    bool
	}{
		{
			name:       "engine fully ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 2, ReadyReplicas: 2, ServingReplicas: 2},
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:       "encoder fully ready",
			component:  v1beta1.EncoderComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 3, ReadyReplicas: 3, ServingReplicas: 3},
			wantType:   v1beta1.EncoderReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:       "engine partial, no budget: strict → not ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 2, ReadyReplicas: 1, ServingReplicas: 1},
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			name:       "engine 9/10 within lifecycle 25% maxUnavailable → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 9, ServingReplicas: 9},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			name:       "engine 6/10 below lifecycle 25% maxUnavailable → not ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 6, ServingReplicas: 6},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			name:       "engine 8/10 within lifecycle integer maxUnavailable → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 8, ServingReplicas: 8},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromInt(2)),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			name:       "engine 7/10 below lifecycle integer maxUnavailable → not ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 7, ServingReplicas: 7},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromInt(2)),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			name:       "engine lifecycle 25% maxUnavailable → ready at 3/4",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 4, ReadyReplicas: 3, ServingReplicas: 3},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			name:       "engine zero replicas",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 0},
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "NoReplicas",
		},
		{
			name:       "engine nil status",
			component:  v1beta1.EngineComponent,
			cs:         nil,
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "NoReplicas",
		},
		{
			name:       "decoder fully ready",
			component:  v1beta1.DecoderComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 3, ReadyReplicas: 3, ServingReplicas: 3},
			wantType:   v1beta1.DecoderReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:       "router fully ready",
			component:  v1beta1.RouterComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1},
			wantType:   v1beta1.RouterReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			// Held Instances: containers ready, none promoted into rotation.
			name:       "router containers ready but no Instance serving → not ready",
			component:  v1beta1.RouterComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 8, ReadyReplicas: 8},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			wantType:   v1beta1.RouterReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			// 25% of one Instance rounds up to the whole component, so the
			// budget alone would let it read ready with nothing serving.
			name:       "router 1/1 containers ready, none serving, budget covers every Instance → not ready",
			component:  v1beta1.RouterComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 1, ReadyReplicas: 1},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			wantType:   v1beta1.RouterReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			// An in-place update drains one Instance while the other serves.
			name:       "engine containers ready with one Instance drained → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 2, ReadyReplicas: 2, ServingReplicas: 1},
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:      "unknown component returns nil",
			component: v1beta1.ComponentType("predictor"),
			cs:        &v1beta1.LifecycleStatus{Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1},
			wantNil:   true,
		},

		// --- rollout surge: the floor must follow `desired`, not the surged live count ---
		{
			// 16 desired surged to 32 live, all 16 serving. Floor is 25% of 16 = 12,
			// not of 32 (= 24).
			name:       "surged 16->32, all desired serving → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 16, ServingReplicas: 16},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// Real unavailability during a surge must still be caught: 8 < floor 12.
			name:       "surged 16->32, serving below desired floor → not ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 8, ServingReplicas: 8},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			// Mid-drain: new Instances ready on top of the old set.
			name:       "surged 16->32, 24 serving → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 24, ServingReplicas: 24},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// Converged: surge drained.
			name:       "post-drain 16/16 ready → fully ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 16, ReadyReplicas: 16, ServingReplicas: 16},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "Ready",
		},
		{
			name:       "surged with lifecycle maxUnavailable → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 8, ReadyReplicas: 4, ServingReplicas: 4},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    replicas(4),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// Clamp: floor is 25% of the 8 live, not of the not-yet-real 16.
			name:       "scale-up 8 live of 16 desired → clamped to live floor, ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 8, ReadyReplicas: 6, ServingReplicas: 6},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// nil desired keeps the live-count floor.
			name:       "surged with nil desired → live-count floor",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 16, ServingReplicas: 16},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    nil,
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionFalse,
			wantReason: "InsufficientAvailable",
		},
		{
			name:       "zero desired → falls back to live count",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 10, ReadyReplicas: 9, ServingReplicas: 9},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    replicas(0),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			name:       "surged with lifecycle percentage maxUnavailable → ready",
			component:  v1beta1.EngineComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 32, ReadyReplicas: 16, ServingReplicas: 16},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    replicas(16),
			wantType:   v1beta1.EngineReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
		{
			// Router surges too (MaxSurge=25%).
			name:       "router surged 3->4, all desired serving → ready",
			component:  v1beta1.RouterComponent,
			cs:         &v1beta1.LifecycleStatus{Replicas: 4, ReadyReplicas: 3, ServingReplicas: 3},
			lifecycle:  lifecycleWithMaxUnavailable(intstr.FromString("25%")),
			desired:    replicas(3),
			wantType:   v1beta1.RouterReady,
			wantStatus: corev1.ConditionTrue,
			wantReason: "MinimumAvailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TopLevelComponentReadyFromLifecycle(tt.component, tt.cs, tt.lifecycle, tt.desired)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("want nil condition, got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("want condition, got nil")
			}
			if got.Type != tt.wantType {
				t.Errorf("Type: got %q want %q", got.Type, tt.wantType)
			}
			if got.Status != tt.wantStatus {
				t.Errorf("Status: got %q want %q", got.Status, tt.wantStatus)
			}
			if got.Reason != tt.wantReason {
				t.Errorf("Reason: got %q want %q", got.Reason, tt.wantReason)
			}
		})
	}
}

// TestSurgeDoesNotInflateAvailabilityFloor verifies that rollout surge does not
// raise the lifecycle availability floor above the desired Instance count.
func TestSurgeDoesNotInflateAvailabilityFloor(t *testing.T) {
	cs := &v1beta1.LifecycleStatus{
		Replicas:        32, // 16 desired + 100% surge
		ReadyReplicas:   16,
		ServingReplicas: 16,
		CurrentRevision: "engine-ed3867a3",
		UpdateRevision:  "engine-66ed9807",
	}
	lifecycle := lifecycleWithMaxUnavailable(intstr.FromString("25%"))
	if got := InstanceAvailabilityFloor(cs.Replicas, replicas(16), lifecycle); got != 12 {
		t.Fatalf("InstanceAvailabilityFloor() = %d, want 12", got)
	}

	got := TopLevelComponentReadyFromLifecycle(v1beta1.EngineComponent, cs, lifecycle, replicas(16))
	if got == nil {
		t.Fatal("want condition, got nil")
	}
	if got.Status != corev1.ConditionTrue {
		t.Errorf("Status: got %q want %q (all 16 desired Instances are serving)", got.Status, corev1.ConditionTrue)
	}
	if got.Reason != "MinimumAvailable" {
		t.Errorf("Reason: got %q want %q", got.Reason, "MinimumAvailable")
	}
	if want := "16/32 Instances serving (min 12)"; got.Message != want {
		t.Errorf("Message: got %q want %q", got.Message, want)
	}
}

func TestInstanceAvailabilityFloor(t *testing.T) {
	lifecycle := lifecycleWithMaxUnavailable(intstr.FromString("25%"))
	tests := []struct {
		name    string
		actual  int32
		desired *int32
		policy  *v1beta1.LifecycleSpec
		want    int32
	}{
		{name: "surge scales against desired", actual: 32, desired: replicas(16), policy: lifecycle, want: 12},
		{name: "nil desired scales against live", actual: 32, policy: lifecycle, want: 24},
		{name: "absent lifecycle is strict", actual: 32, desired: replicas(16), want: 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InstanceAvailabilityFloor(tt.actual, tt.desired, tt.policy); got != tt.want {
				t.Fatalf("InstanceAvailabilityFloor() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestFloorBasis(t *testing.T) {
	tests := []struct {
		name    string
		actual  int32
		desired *int32
		want    int32
	}{
		{name: "surge: desired below live count wins", actual: 32, desired: replicas(16), want: 16},
		{name: "steady state: equal", actual: 16, desired: replicas(16), want: 16},
		{name: "scale-up: clamped to live count", actual: 8, desired: replicas(16), want: 8},
		{name: "nil desired falls back to live", actual: 32, desired: nil, want: 32},
		{name: "zero desired falls back to live", actual: 32, desired: replicas(0), want: 32},
		{name: "negative desired falls back to live", actual: 32, desired: replicas(-1), want: 32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := floorBasis(tt.actual, tt.desired); got != tt.want {
				t.Errorf("floorBasis(%d, %v) = %d, want %d", tt.actual, tt.desired, got, tt.want)
			}
		})
	}
}
