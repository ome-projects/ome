package components

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/pdb"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/render"
)

func TestReconcileOMENativePDBUsesConfiguredFallback(t *testing.T) {
	scheme := omeNativePDBTestScheme(t)
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	isvc, meta := omeNativePDBTestObjects(v1beta1.EngineComponent)
	maximum := intstr.FromInt(1)
	b := omeNativePDBTestBase(cl, scheme, &controllerconfig.InferenceServicesConfig{
		PodDisruptionBudget: controllerconfig.PodDisruptionBudgetConfig{
			OMENative: &controllerconfig.PodDisruptionBudgetPolicy{MaxUnavailable: &maximum},
		},
	})
	ir := omeNativePDBTestIR(isvc, v1beta1.EngineComponent, 3,
		v1beta1.Runner{Name: v1beta1.RunnerNameDefault, Size: 1})

	require.NoError(t, reconcileOMENativePDBForTest(
		context.Background(), b, isvc, v1beta1.EngineComponent,
		&v1beta1.ComponentExtensionSpec{}, meta, ir,
	))

	budget := getOMENativePDB(t, cl, meta)
	require.NotNil(t, budget.Spec.MinAvailable)
	assert.Equal(t, intstr.FromInt(2), *budget.Spec.MinAvailable)
	assert.Nil(t, budget.Spec.MaxUnavailable)
	assert.Equal(t, map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}, budget.Spec.Selector.MatchLabels)
	assert.NotContains(t, budget.Spec.Selector.MatchLabels, query.LabelRunner)
	assert.NotContains(t, budget.Spec.Selector.MatchLabels, query.LabelPodOrdinal)
	controller := metav1.GetControllerOf(budget)
	require.NotNil(t, controller)
	assert.Equal(t, isvc.UID, controller.UID)
}

func TestReconcileOMENativePDBComponentPolicyOverridesFallback(t *testing.T) {
	scheme := omeNativePDBTestScheme(t)
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	isvc, meta := omeNativePDBTestObjects(v1beta1.EngineComponent)
	fallback := intstr.FromInt(1)
	componentMinimum := intstr.FromString("50%")
	b := omeNativePDBTestBase(cl, scheme, &controllerconfig.InferenceServicesConfig{
		PodDisruptionBudget: controllerconfig.PodDisruptionBudgetConfig{
			OMENative: &controllerconfig.PodDisruptionBudgetPolicy{MaxUnavailable: &fallback},
		},
	})
	ir := omeNativePDBTestIR(isvc, v1beta1.EngineComponent, 3,
		v1beta1.Runner{Name: v1beta1.RunnerNameDefault, Size: 1})

	require.NoError(t, reconcileOMENativePDBForTest(
		context.Background(), b, isvc, v1beta1.EngineComponent,
		&v1beta1.ComponentExtensionSpec{MinAvailable: &componentMinimum}, meta, ir,
	))

	budget := getOMENativePDB(t, cl, meta)
	require.NotNil(t, budget.Spec.MinAvailable)
	assert.Equal(t, intstr.FromInt(2), *budget.Spec.MinAvailable)
	assert.Nil(t, budget.Spec.MaxUnavailable)
}

func TestReconcileOMENativePDBNormalizesMultiPodMaximum(t *testing.T) {
	scheme := omeNativePDBTestScheme(t)
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	isvc, meta := omeNativePDBTestObjects(v1beta1.EngineComponent)
	maximum := intstr.FromString("25%")
	ir := omeNativePDBTestIR(isvc, v1beta1.EngineComponent, 2,
		v1beta1.Runner{Name: v1beta1.RunnerNameLeader, Size: 1},
		v1beta1.Runner{Name: v1beta1.RunnerNameWorker, Size: 3},
	)

	require.NoError(t, reconcileOMENativePDBForTest(
		context.Background(), omeNativePDBTestBase(cl, scheme, nil), isvc,
		v1beta1.EngineComponent,
		&v1beta1.ComponentExtensionSpec{MaxUnavailable: &maximum}, meta, ir,
	))

	budget := getOMENativePDB(t, cl, meta)
	require.NotNil(t, budget.Spec.MinAvailable)
	assert.Equal(t, intstr.FromInt(6), *budget.Spec.MinAvailable)
	assert.Nil(t, budget.Spec.MaxUnavailable)
}

func TestReconcileOMENativePDBNormalizesZeroReplicas(t *testing.T) {
	scheme := omeNativePDBTestScheme(t)
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	isvc, meta := omeNativePDBTestObjects(v1beta1.EngineComponent)
	maximum := intstr.FromInt(1)
	ir := omeNativePDBTestIR(isvc, v1beta1.EngineComponent, 0,
		v1beta1.Runner{Name: v1beta1.RunnerNameDefault, Size: 1})

	require.NoError(t, reconcileOMENativePDBForTest(
		context.Background(), omeNativePDBTestBase(cl, scheme, nil), isvc,
		v1beta1.EngineComponent,
		&v1beta1.ComponentExtensionSpec{MaxUnavailable: &maximum}, meta, ir,
	))

	budget := getOMENativePDB(t, cl, meta)
	require.NotNil(t, budget.Spec.MinAvailable)
	assert.Equal(t, intstr.FromInt(0), *budget.Spec.MinAvailable)
}

func TestReconcileOMENativePDBAbsentPolicyDeletesOwnedWithoutIR(t *testing.T) {
	scheme := omeNativePDBTestScheme(t)
	isvc, meta := omeNativePDBTestObjects(v1beta1.EngineComponent)
	existing := omeNativePDBTestExisting(isvc, meta, types.UID("owned-pdb"))
	existing.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: omeNativeComponentSelector(isvc, v1beta1.EngineComponent),
	}
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

	require.NoError(t, reconcileOMENativePDBForTest(
		context.Background(), omeNativePDBTestBase(cl, scheme, nil), isvc,
		v1beta1.EngineComponent, &v1beta1.ComponentExtensionSpec{}, meta, nil,
	))

	err := cl.Get(context.Background(), client.ObjectKeyFromObject(existing), &policyv1.PodDisruptionBudget{})
	assert.True(t, apierrors.IsNotFound(err), "owned PodDisruptionBudget must be deleted, got %v", err)
}

func TestReconcileOMENativePDBConfiguredPolicyRequiresCommittedIR(t *testing.T) {
	scheme := omeNativePDBTestScheme(t)
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	isvc, meta := omeNativePDBTestObjects(v1beta1.EngineComponent)
	maximum := intstr.FromInt(1)

	err := reconcileOMENativePDBForTest(
		context.Background(), omeNativePDBTestBase(cl, scheme, nil), isvc,
		v1beta1.EngineComponent,
		&v1beta1.ComponentExtensionSpec{MaxUnavailable: &maximum}, meta, nil,
	)
	require.ErrorContains(t, err, "InferenceReplica is required")
}

func TestReconcileOMENativePDBDefersRawSelectorCutoverUntilIRReady(t *testing.T) {
	scheme := omeNativePDBTestScheme(t)
	isvc, meta := omeNativePDBTestObjects(v1beta1.EngineComponent)
	existing := omeNativePDBTestExisting(isvc, meta, types.UID("raw-pdb"))
	minimum := intstr.FromInt(2)
	ir := omeNativePDBTestIR(isvc, v1beta1.EngineComponent, 3,
		v1beta1.Runner{Name: v1beta1.RunnerNameDefault, Size: 1})
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(existing, ir).Build()

	require.NoError(t, reconcileOMENativePDBForTest(
		context.Background(), omeNativePDBTestBase(cl, scheme, nil), isvc,
		v1beta1.EngineComponent,
		&v1beta1.ComponentExtensionSpec{MinAvailable: &minimum}, meta, ir,
	))

	budget := getOMENativePDB(t, cl, meta)
	assert.Equal(t, types.UID("raw-pdb"), budget.UID)
	require.NotNil(t, budget.Spec.MaxUnavailable)
	assert.Equal(t, intstr.FromInt(1), *budget.Spec.MaxUnavailable)
	assert.Equal(t, map[string]string{
		constants.RawDeploymentAppLabel: constants.GetRawServiceLabel(meta.Name),
	}, budget.Spec.Selector.MatchLabels)

	markOMENativePDBTestIRReady(ir)
	require.NoError(t, cl.Update(context.Background(), ir))
	require.NoError(t, reconcileOMENativePDBForTest(
		context.Background(), omeNativePDBTestBase(cl, scheme, nil), isvc,
		v1beta1.EngineComponent,
		&v1beta1.ComponentExtensionSpec{MinAvailable: &minimum}, meta, ir,
	))

	budget = getOMENativePDB(t, cl, meta)
	assert.Equal(t, types.UID("raw-pdb"), budget.UID)
	require.NotNil(t, budget.Spec.MinAvailable)
	assert.Equal(t, minimum, *budget.Spec.MinAvailable)
	assert.Nil(t, budget.Spec.MaxUnavailable)
	assert.Equal(t, omeNativeComponentSelector(isvc, v1beta1.EngineComponent), budget.Spec.Selector.MatchLabels)
}

func TestReconcileOMENativePDBAbsentPolicyPreservesForeignObject(t *testing.T) {
	scheme := omeNativePDBTestScheme(t)
	isvc, meta := omeNativePDBTestObjects(v1beta1.EngineComponent)
	existing := omeNativePDBTestExisting(isvc, meta, types.UID("foreign-pdb"))
	existing.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       "foreign-controller",
		UID:        types.UID("foreign-controller-uid"),
		Controller: boolPointer(true),
	}}
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(existing).Build()

	require.NoError(t, reconcileOMENativePDBForTest(
		context.Background(), omeNativePDBTestBase(cl, scheme, nil), isvc,
		v1beta1.EngineComponent, &v1beta1.ComponentExtensionSpec{}, meta, nil,
	))

	budget := getOMENativePDB(t, cl, meta)
	assert.Equal(t, types.UID("foreign-pdb"), budget.UID)
	controller := metav1.GetControllerOf(budget)
	require.NotNil(t, controller)
	assert.Equal(t, types.UID("foreign-controller-uid"), controller.UID)
}

func TestOMENativeComponentPathsReconcilePDBAfterIRCreation(t *testing.T) {
	for _, component := range []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	} {
		t.Run(string(component), func(t *testing.T) {
			scheme := omeNativePDBTestScheme(t)
			cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
				Create: func(ctx context.Context, inner client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if ir, ok := obj.(*v1beta1.InferenceReplica); ok {
						ir.UID = types.UID("committed-" + string(component) + "-uid")
					}
					return inner.Create(ctx, obj, opts...)
				},
			}).Build()
			isvc, meta := omeNativePDBTestObjects(component)
			minimumReplicas := 1
			ext := v1beta1.ComponentExtensionSpec{
				MinReplicas: &minimumReplicas,
				Autoscaler:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			}
			setOMENativePDBTestComponent(isvc, component, ext)
			maximum := intstr.FromInt(1)
			config := &controllerconfig.InferenceServicesConfig{
				PodDisruptionBudget: controllerconfig.PodDisruptionBudgetConfig{
					OMENative: &controllerconfig.PodDisruptionBudgetPolicy{MaxUnavailable: &maximum},
				},
			}
			deps := &ComponentDeps{Client: cl, Scheme: scheme, Config: config}
			podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "example.com/runtime:test"}}}

			switch component {
			case v1beta1.EngineComponent:
				engine := NewEngine(deps, ComponentInputs{DeploymentMode: constants.OMENative}, isvc.Spec.Engine).(*Engine)
				request := mustComponentPDBRequest(t, &engine.BaseComponentFields, isvc, constants.OMENative, component, meta, &isvc.Spec.Engine.ComponentExtensionSpec)
				_, err := engine.reconcileDeployment(context.Background(), isvc, meta, podSpec, 0, nil, request)
				require.NoError(t, err)
			case v1beta1.DecoderComponent:
				decoder := NewDecoder(deps, ComponentInputs{DeploymentMode: constants.OMENative}, isvc.Spec.Decoder).(*Decoder)
				request := mustComponentPDBRequest(t, &decoder.BaseComponentFields, isvc, constants.OMENative, component, meta, &isvc.Spec.Decoder.ComponentExtensionSpec)
				_, err := decoder.reconcileDeployment(context.Background(), isvc, meta, podSpec, 0, nil, request)
				require.NoError(t, err)
			case v1beta1.RouterComponent:
				router := NewRouter(deps, ComponentInputs{DeploymentMode: constants.OMENative}, isvc.Spec.Router).(*Router)
				request := mustComponentPDBRequest(t, &router.BaseComponentFields, isvc, constants.OMENative, component, meta, &isvc.Spec.Router.ComponentExtensionSpec)
				_, err := router.reconcileDeployment(context.Background(), isvc, meta, podSpec, request)
				require.NoError(t, err)
			}

			ir := &v1beta1.InferenceReplica{}
			require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
				Namespace: isvc.Namespace,
				Name:      meta.Name,
			}, ir))
			budget := getOMENativePDB(t, cl, meta)
			require.NotNil(t, budget.Spec.MinAvailable)
			assert.Equal(t, intstr.FromInt(0), *budget.Spec.MinAvailable)
		})
	}
}

func TestOMENativeComponentPathsRejectInvalidPDBBeforeChildMutation(t *testing.T) {
	for _, component := range []v1beta1.ComponentType{
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
		v1beta1.RouterComponent,
	} {
		t.Run(string(component), func(t *testing.T) {
			scheme := omeNativePDBTestScheme(t)
			cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
			isvc, meta := omeNativePDBTestObjects(component)
			minimumReplicas := 1
			minimum := intstr.FromInt(1)
			maximum := intstr.FromInt(1)
			ext := v1beta1.ComponentExtensionSpec{
				MinReplicas:    &minimumReplicas,
				MinAvailable:   &minimum,
				MaxUnavailable: &maximum,
				Autoscaler:     &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
			}
			setOMENativePDBTestComponent(isvc, component, ext)
			deps := &ComponentDeps{
				Client: cl,
				Scheme: scheme,
				Config: &controllerconfig.InferenceServicesConfig{},
			}
			var err error
			switch component {
			case v1beta1.EngineComponent:
				engine := NewEngine(deps, ComponentInputs{DeploymentMode: constants.OMENative}, isvc.Spec.Engine).(*Engine)
				_, err = resolveComponentPDBRequest(&engine.BaseComponentFields, isvc, constants.OMENative, component, meta, &isvc.Spec.Engine.ComponentExtensionSpec)
			case v1beta1.DecoderComponent:
				decoder := NewDecoder(deps, ComponentInputs{DeploymentMode: constants.OMENative}, isvc.Spec.Decoder).(*Decoder)
				_, err = resolveComponentPDBRequest(&decoder.BaseComponentFields, isvc, constants.OMENative, component, meta, &isvc.Spec.Decoder.ComponentExtensionSpec)
			case v1beta1.RouterComponent:
				router := NewRouter(deps, ComponentInputs{DeploymentMode: constants.OMENative}, isvc.Spec.Router).(*Router)
				_, err = resolveComponentPDBRequest(&router.BaseComponentFields, isvc, constants.OMENative, component, meta, &isvc.Spec.Router.ComponentExtensionSpec)
			}
			require.ErrorContains(t, err, "exactly one of minAvailable or maxUnavailable")

			key := types.NamespacedName{Namespace: meta.Namespace, Name: meta.Name}
			for name, object := range map[string]client.Object{
				"InferenceReplica":    &v1beta1.InferenceReplica{},
				"PodDisruptionBudget": &policyv1.PodDisruptionBudget{},
			} {
				t.Run(name, func(t *testing.T) {
					err := cl.Get(context.Background(), key, object)
					assert.True(t, apierrors.IsNotFound(err), "%s must not be created, got %v", name, err)
				})
			}
			hpas := &autoscalingv2.HorizontalPodAutoscalerList{}
			require.NoError(t, cl.List(context.Background(), hpas))
			assert.Empty(t, hpas.Items)
			scaledObjects := &kedav1.ScaledObjectList{}
			require.NoError(t, cl.List(context.Background(), scaledObjects))
			assert.Empty(t, scaledObjects.Items)
		})
	}
}

func reconcileOMENativePDBForTest(
	ctx context.Context,
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	componentType v1beta1.ComponentType,
	componentExt *v1beta1.ComponentExtensionSpec,
	objectMeta metav1.ObjectMeta,
	ir *v1beta1.InferenceReplica,
) error {
	request, err := resolveComponentPDBRequest(
		b, isvc, constants.OMENative, componentType, objectMeta, componentExt,
	)
	if err != nil {
		return err
	}
	return ReconcileOMENativePDB(ctx, b, componentType, ir, request)
}

func mustComponentPDBRequest(
	t *testing.T,
	b *BaseComponentFields,
	isvc *v1beta1.InferenceService,
	mode constants.DeploymentModeType,
	component v1beta1.ComponentType,
	meta metav1.ObjectMeta,
	ext *v1beta1.ComponentExtensionSpec,
) pdb.Request {
	t.Helper()
	request, err := resolveComponentPDBRequest(b, isvc, mode, component, meta, ext)
	require.NoError(t, err)
	return request
}

func omeNativePDBTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, addToScheme := range []func(*runtime.Scheme) error{
		v1beta1.AddToScheme,
		autoscalingv2.AddToScheme,
		corev1.AddToScheme,
		policyv1.AddToScheme,
		kedav1.AddToScheme,
	} {
		require.NoError(t, addToScheme(scheme))
	}
	return scheme
}

func omeNativePDBTestBase(cl client.Client, scheme *runtime.Scheme, config *controllerconfig.InferenceServicesConfig) *BaseComponentFields {
	return &BaseComponentFields{Piece: render.Piece{InferenceServiceConfig: config}, Client: cl, Scheme: scheme}
}

func omeNativePDBTestObjects(component v1beta1.ComponentType) (*v1beta1.InferenceService, metav1.ObjectMeta) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name:      "pdb-test",
		Namespace: "default",
		UID:       types.UID("pdb-test-uid"),
	}}
	return isvc, metav1.ObjectMeta{
		Name:      isvc.Name + "-" + string(component),
		Namespace: isvc.Namespace,
		Labels:    map[string]string{"example.com/fixture": "true"},
	}
}

func omeNativePDBTestIR(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, replicas int32, runners ...v1beta1.Runner) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      isvc.Name + "-" + string(component),
			Namespace: isvc.Namespace,
			UID:       types.UID("pdb-test-ir-uid"),
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: isvc.Name},
			Component: component,
			Replicas:  &replicas,
			Runners:   runners,
		},
	}
}

func markOMENativePDBTestIRReady(ir *v1beta1.InferenceReplica) {
	ir.Generation = 1
	ir.Status.ObservedGeneration = ir.Generation
	ir.Status.ReadyReplicas = *ir.Spec.Replicas
	ir.Status.AvailableReplicas = *ir.Spec.Replicas
	ir.Status.UpdatedReadyReplicas = *ir.Spec.Replicas
	ir.Status.CurrentRevision = ir.Name + "-target"
	ir.Status.UpdateRevision = ir.Status.CurrentRevision
}

func omeNativePDBTestExisting(isvc *v1beta1.InferenceService, meta metav1.ObjectMeta, uid types.UID) *policyv1.PodDisruptionBudget {
	maximum := intstr.FromInt(1)
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:            meta.Name,
			Namespace:       meta.Namespace,
			UID:             uid,
			ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maximum,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				constants.RawDeploymentAppLabel: constants.GetRawServiceLabel(meta.Name),
			}},
		},
	}
}

func getOMENativePDB(t *testing.T, cl client.Client, meta metav1.ObjectMeta) *policyv1.PodDisruptionBudget {
	t.Helper()
	budget := &policyv1.PodDisruptionBudget{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Namespace: meta.Namespace,
		Name:      meta.Name,
	}, budget))
	return budget
}

func setOMENativePDBTestComponent(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, ext v1beta1.ComponentExtensionSpec) {
	switch component {
	case v1beta1.EngineComponent:
		isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: ext}
	case v1beta1.DecoderComponent:
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: ext}
	case v1beta1.RouterComponent:
		isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: ext}
	}
}

func boolPointer(value bool) *bool {
	return &value
}
