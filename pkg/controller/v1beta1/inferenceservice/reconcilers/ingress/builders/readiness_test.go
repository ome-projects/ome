package builders

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestComponentCanBackRoute(t *testing.T) {
	for _, tt := range []struct {
		name           string
		ready          corev1.ConditionStatus
		scaleTargetRef *v1beta1.ScaleTargetRef
		lifecycle      *v1beta1.LifecycleStatus
		wantAllowed    bool
	}{
		{name: "ready condition", ready: corev1.ConditionTrue, wantAllowed: true},
		{
			name:  "serving while below readiness floor",
			ready: corev1.ConditionFalse,
			scaleTargetRef: &v1beta1.ScaleTargetRef{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: inferenceReplicaScaleTargetKind, Name: "test-engine",
			},
			lifecycle: &v1beta1.LifecycleStatus{
				Replicas: 10, ReadyReplicas: 4, ServingReplicas: 4,
			},
			wantAllowed: true,
		},
		{
			name:  "ready replicas are out of rotation",
			ready: corev1.ConditionFalse,
			scaleTargetRef: &v1beta1.ScaleTargetRef{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: inferenceReplicaScaleTargetKind, Name: "test-engine",
			},
			lifecycle: &v1beta1.LifecycleStatus{
				Replicas: 10, ReadyReplicas: 4,
			},
			wantAllowed: false,
		},
		{
			name:           "stale lifecycle from another deployment mode",
			ready:          corev1.ConditionFalse,
			scaleTargetRef: &v1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "test-engine"},
			lifecycle: &v1beta1.LifecycleStatus{
				Replicas: 10, ReadyReplicas: 4, ServingReplicas: 4,
			},
			wantAllowed: false,
		},
		{
			name:  "wrong inference replica",
			ready: corev1.ConditionFalse,
			scaleTargetRef: &v1beta1.ScaleTargetRef{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: inferenceReplicaScaleTargetKind, Name: "other-engine",
			},
			lifecycle: &v1beta1.LifecycleStatus{
				Replicas: 10, ReadyReplicas: 4, ServingReplicas: 4,
			},
			wantAllowed: false,
		},
		{
			name:  "unknown readiness",
			ready: corev1.ConditionUnknown,
			scaleTargetRef: &v1beta1.ScaleTargetRef{
				APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: inferenceReplicaScaleTargetKind, Name: "test-engine",
			},
			lifecycle: &v1beta1.LifecycleStatus{
				Replicas: 10, ReadyReplicas: 4, ServingReplicas: 4,
			},
			wantAllowed: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Name: "test"},
			}
			isvc.Status.SetCondition(v1beta1.EngineReady, &apis.Condition{
				Type:   v1beta1.EngineReady,
				Status: tt.ready,
			})
			if tt.lifecycle != nil {
				isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.EngineComponent: {
						ScaleTargetRef: tt.scaleTargetRef,
						Lifecycle:      tt.lifecycle,
					},
				}
			}

			assert.Equal(t, tt.wantAllowed,
				componentCanBackRoute(isvc, v1beta1.EngineComponent, v1beta1.EngineReady))
		})
	}
}
