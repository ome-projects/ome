package components

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// TestDispatchIRAutoscaler_PendingSwitchRequeues pins that a switch from the
// InferenceReplica's HPA to KEDA, which waits for KEDA to take the HPA over,
// requeues the pass instead of failing it, so the component status is still
// written.
func TestDispatchIRAutoscaler_PendingSwitchRequeues(t *testing.T) {
	ext := v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1), MaxReplicas: 4}
	isvc := holdTestISVC(ext)
	ir := holdTestIR("llm-a-engine", nil)
	working := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ir.Namespace,
			Name:      ir.Name,
			UID:       "working-hpa-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       ir.Name,
				UID:        ir.UID,
				Controller: ptr.To(true),
			}},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       ir.Name,
			},
			MaxReplicas: 4,
		},
	}
	cl := componentClient(t, fakeclient.WithObjects(working))
	b := &BaseComponentFields{Client: cl, Scheme: cl.Scheme()}

	result, err := dispatchIRAutoscaler(context.Background(), b, isvc, ir, &ext,
		ComponentAutoscaling{Resolved: storedIRBlock(), Source: autoscaler.SpecSourceISVC})
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{Requeue: true}, result)

	so := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ir.Namespace, Name: isvcutils.GetScaledObjectName(ir.Name)}, so))
	assert.Equal(t, "true", so.Annotations[kedav1.ScaledObjectTransferHpaOwnershipAnnotation])
	kept := &autoscalingv2.HorizontalPodAutoscaler{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: ir.Namespace, Name: ir.Name}, kept))
	assert.Equal(t, working.UID, kept.UID, "the HPA keeps scaling until KEDA takes it over")
}

// TestRawComponentDeployment_PendingSwitchRequeues pins the RawDeployment
// side: switching a Deployment's HPA to KEDA requeues while KEDA takes the
// HPA over, and the HPA keeps scaling meanwhile.
func TestRawComponentDeployment_PendingSwitchRequeues(t *testing.T) {
	minReplicas := 1
	ext := v1beta1.ComponentExtensionSpec{
		MinReplicas: &minReplicas,
		MaxReplicas: 4,
		Autoscaler:  storedIRBlock(),
	}
	isvc := rawAutoscalerTestISVC(v1beta1.EngineComponent, ext)
	componentMeta := metav1.ObjectMeta{Name: isvc.Name + "-engine", Namespace: isvc.Namespace, Annotations: map[string]string{}}
	working := &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       componentMeta.Namespace,
			Name:            componentMeta.Name,
			UID:             "working-hpa-uid",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService"))},
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: componentMeta.Name},
			MaxReplicas:    4,
		},
	}
	cl := componentClient(t, fakeclient.WithObjects(working))
	deps := &ComponentDeps{
		Client:    cl,
		Clientset: rawAutoscalerTestClientset(),
		Scheme:    cl.Scheme(),
		Config:    &controllerconfig.InferenceServicesConfig{},
	}
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "example.com/runtime:test"}}}

	engine := NewEngine(deps, ComponentInputs{DeploymentMode: constants.RawDeployment}, isvc.Spec.Engine).(*Engine)
	request := mustComponentPDBRequest(t, &engine.BaseComponentFields, isvc, constants.RawDeployment, v1beta1.EngineComponent, componentMeta, &isvc.Spec.Engine.ComponentExtensionSpec)
	result, err := engine.reconcileDeployment(context.Background(), isvc, componentMeta, podSpec, 0, nil, request)
	require.NoError(t, err)
	assert.True(t, result.Requeue, "the pass requeues while KEDA takes the HPA over")

	so := &kedav1.ScaledObject{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: componentMeta.Namespace, Name: isvcutils.GetScaledObjectName(componentMeta.Name)}, so))
	assert.Equal(t, "true", so.Annotations[kedav1.ScaledObjectTransferHpaOwnershipAnnotation])
	kept := &autoscalingv2.HorizontalPodAutoscaler{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Namespace: componentMeta.Namespace, Name: componentMeta.Name}, kept))
	assert.Equal(t, working.UID, kept.UID)
}
