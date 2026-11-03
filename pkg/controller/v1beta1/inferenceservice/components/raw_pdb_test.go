package components

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestRawPDBUsesConfiguredFallbackForEveryComponent(t *testing.T) {
	configured := intstr.FromInt(2)
	config := &controllerconfig.InferenceServicesConfig{
		PodDisruptionBudget: controllerconfig.PodDisruptionBudgetConfig{
			RawDeployment: &controllerconfig.PodDisruptionBudgetPolicy{MaxUnavailable: &configured},
		},
	}

	for _, component := range []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	} {
		t.Run(string(component), func(t *testing.T) {
			cl := componentClient(t)
			ext := rawPDBTestExtension()
			isvc := rawAutoscalerTestISVC(component, ext)
			componentMeta := rawPDBTestObjectMeta(isvc, component)

			require.NoError(t, reconcileRawPDBTestComponent(
				context.Background(), component, cl, config, isvc, componentMeta,
			))

			budget := &policyv1.PodDisruptionBudget{}
			require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
				Namespace: componentMeta.Namespace,
				Name:      componentMeta.Name,
			}, budget))
			require.NotNil(t, budget.Spec.MaxUnavailable)
			assert.Equal(t, configured, *budget.Spec.MaxUnavailable)
			assert.Nil(t, budget.Spec.MinAvailable)
		})
	}
}

func TestRawPDBMergedComponentPolicyOverridesConfiguredFallback(t *testing.T) {
	configured := intstr.FromInt(2)
	minimum := intstr.FromInt(1)
	config := &controllerconfig.InferenceServicesConfig{
		PodDisruptionBudget: controllerconfig.PodDisruptionBudgetConfig{
			RawDeployment: &controllerconfig.PodDisruptionBudgetPolicy{MaxUnavailable: &configured},
		},
	}
	ext := rawPDBTestExtension()
	ext.MinAvailable = &minimum
	cl := componentClient(t)
	isvc := rawAutoscalerTestISVC(v1beta1.EngineComponent, ext)
	componentMeta := rawPDBTestObjectMeta(isvc, v1beta1.EngineComponent)

	require.NoError(t, reconcileRawPDBTestComponent(
		context.Background(), v1beta1.EngineComponent, cl, config, isvc, componentMeta,
	))

	budget := &policyv1.PodDisruptionBudget{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      componentMeta.Name,
	}, budget))
	require.NotNil(t, budget.Spec.MinAvailable)
	assert.Equal(t, minimum, *budget.Spec.MinAvailable)
	assert.Nil(t, budget.Spec.MaxUnavailable)
}

func TestRawPDBAbsentPolicyDoesNotCreateAndDeletesOwned(t *testing.T) {
	tests := []struct {
		name         string
		config       *controllerconfig.InferenceServicesConfig
		seedOwnedPDB bool
	}{
		{name: "nil config"},
		{name: "empty config deletes owned PDB", config: &controllerconfig.InferenceServicesConfig{}, seedOwnedPDB: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext := rawPDBTestExtension()
			isvc := rawAutoscalerTestISVC(v1beta1.EngineComponent, ext)
			componentMeta := rawPDBTestObjectMeta(isvc, v1beta1.EngineComponent)
			var opts []fakeclient.Option
			if tt.seedOwnedPDB {
				stale := intstr.FromInt(3)
				opts = append(opts, fakeclient.WithObjects(&policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{
						Name:            componentMeta.Name,
						Namespace:       componentMeta.Namespace,
						UID:             types.UID("stale-pdb-uid"),
						ResourceVersion: "1",
						OwnerReferences: []metav1.OwnerReference{
							*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
						},
					},
					Spec: policyv1.PodDisruptionBudgetSpec{
						MaxUnavailable: &stale,
						Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
							constants.RawDeploymentAppLabel: constants.GetRawServiceLabel(componentMeta.Name),
						}},
					},
				}))
			}
			cl := componentClient(t, opts...)

			require.NoError(t, reconcileRawPDBTestComponent(
				context.Background(), v1beta1.EngineComponent, cl, tt.config, isvc, componentMeta,
			))

			budget := &policyv1.PodDisruptionBudget{}
			err := cl.Get(context.Background(), types.NamespacedName{
				Namespace: componentMeta.Namespace,
				Name:      componentMeta.Name,
			}, budget)
			assert.True(t, apierrors.IsNotFound(err), "PodDisruptionBudget must be absent, got %v", err)
		})
	}
}

func TestRawPDBInvalidPolicyFailsBeforeChildMutation(t *testing.T) {
	minimum := intstr.FromInt(1)
	maximum := intstr.FromInt(2)
	ext := rawPDBTestExtension()
	ext.MinAvailable = &minimum
	ext.MaxUnavailable = &maximum
	cl := componentClient(t)
	isvc := rawAutoscalerTestISVC(v1beta1.EngineComponent, ext)
	componentMeta := rawPDBTestObjectMeta(isvc, v1beta1.EngineComponent)

	err := reconcileRawPDBTestComponent(
		context.Background(), v1beta1.EngineComponent, cl, &controllerconfig.InferenceServicesConfig{}, isvc, componentMeta,
	)
	require.ErrorContains(t, err, "exactly one of minAvailable or maxUnavailable")

	key := types.NamespacedName{Namespace: componentMeta.Namespace, Name: componentMeta.Name}
	for name, object := range map[string]client.Object{
		"Deployment":          &appsv1.Deployment{},
		"Service":             &corev1.Service{},
		"PodDisruptionBudget": &policyv1.PodDisruptionBudget{},
	} {
		t.Run(name, func(t *testing.T) {
			err := cl.Get(context.Background(), key, object)
			assert.True(t, apierrors.IsNotFound(err), "%s must not be created, got %v", name, err)
		})
	}
}

func rawPDBTestExtension() v1beta1.ComponentExtensionSpec {
	return v1beta1.ComponentExtensionSpec{
		Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
	}
}

func rawPDBTestObjectMeta(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:        isvc.Name + "-" + string(component),
		Namespace:   isvc.Namespace,
		Annotations: map[string]string{},
	}
}

func reconcileRawPDBTestComponent(
	ctx context.Context,
	component v1beta1.ComponentType,
	cl client.Client,
	config *controllerconfig.InferenceServicesConfig,
	isvc *v1beta1.InferenceService,
	componentMeta metav1.ObjectMeta,
) error {
	deps := &ComponentDeps{
		Client:    cl,
		Clientset: rawAutoscalerTestClientset(),
		Scheme:    cl.Scheme(),
		Config:    config,
	}
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "example.com/runtime:test"}},
	}

	switch component {
	case v1beta1.EngineComponent:
		engine := NewEngine(deps, ComponentInputs{DeploymentMode: constants.RawDeployment}, isvc.Spec.Engine).(*Engine)
		request, err := resolveComponentPDBRequest(&engine.BaseComponentFields, isvc, constants.RawDeployment, component, componentMeta, &isvc.Spec.Engine.ComponentExtensionSpec)
		if err == nil {
			err = preflightComponentPDB(ctx, &engine.BaseComponentFields, request)
		}
		if err != nil {
			return err
		}
		_, err = engine.reconcileDeployment(ctx, isvc, componentMeta, podSpec, 0, nil, request)
		return err
	case v1beta1.DecoderComponent:
		decoder := NewDecoder(deps, ComponentInputs{DeploymentMode: constants.RawDeployment}, isvc.Spec.Decoder).(*Decoder)
		request, err := resolveComponentPDBRequest(&decoder.BaseComponentFields, isvc, constants.RawDeployment, component, componentMeta, &isvc.Spec.Decoder.ComponentExtensionSpec)
		if err == nil {
			err = preflightComponentPDB(ctx, &decoder.BaseComponentFields, request)
		}
		if err != nil {
			return err
		}
		_, err = decoder.reconcileDeployment(ctx, isvc, componentMeta, podSpec, 0, nil, request)
		return err
	case v1beta1.RouterComponent:
		router := NewRouter(deps, ComponentInputs{DeploymentMode: constants.RawDeployment}, isvc.Spec.Router).(*Router)
		request, err := resolveComponentPDBRequest(&router.BaseComponentFields, isvc, constants.RawDeployment, component, componentMeta, &isvc.Spec.Router.ComponentExtensionSpec)
		if err == nil {
			err = preflightComponentPDB(ctx, &router.BaseComponentFields, request)
		}
		if err != nil {
			return err
		}
		_, err = router.reconcileDeployment(ctx, isvc, componentMeta, podSpec, request)
		return err
	default:
		return assert.AnError
	}
}
