package components

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/render"
)

func TestRouterReconcileRejectsInvalidPDBBeforeChildMutation(t *testing.T) {
	for _, mode := range []constants.DeploymentModeType{
		constants.RawDeployment,
		constants.OMENative,
	} {
		t.Run(string(mode), func(t *testing.T) {
			scheme := routerPDBTestScheme(t)
			cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
			minimum := intstr.FromInt(1)
			maximum := intstr.FromInt(1)
			ext := rawPDBTestExtension()
			ext.MinAvailable = &minimum
			ext.MaxUnavailable = &maximum
			isvc := rawAutoscalerTestISVC(v1beta1.RouterComponent, ext)
			isvc.Spec.Router.PodSpec.Containers = []corev1.Container{{
				Name: constants.MainContainerName, Image: "example.com/runtime:test",
			}}
			deps := &ComponentDeps{
				Client:    cl,
				Clientset: rawAutoscalerTestClientset(),
				Scheme:    scheme,
				Config:    &controllerconfig.InferenceServicesConfig{},
			}
			router := NewRouter(deps, ComponentInputs{DeploymentMode: mode}, isvc.Spec.Router).(*Router)

			_, err := router.Reconcile(context.Background(), isvc)
			require.ErrorContains(t, err, "exactly one of minAvailable or maxUnavailable")

			key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-router"}
			for name, object := range map[string]client.Object{
				"ServiceAccount":      &corev1.ServiceAccount{},
				"Role":                &rbacv1.Role{},
				"RoleBinding":         &rbacv1.RoleBinding{},
				"Deployment":          &appsv1.Deployment{},
				"InferenceReplica":    &v1beta1.InferenceReplica{},
				"PodDisruptionBudget": &policyv1.PodDisruptionBudget{},
			} {
				t.Run(name, func(t *testing.T) {
					err := cl.Get(context.Background(), key, object)
					assert.True(t, apierrors.IsNotFound(err), "%s must not be created, got %v", name, err)
				})
			}
		})
	}
}

func TestRouterReconcilePreflightsForeignPDBBeforeRBAC(t *testing.T) {
	for _, mode := range []constants.DeploymentModeType{
		constants.RawDeployment,
		constants.OMENative,
	} {
		t.Run(string(mode), func(t *testing.T) {
			scheme := routerPDBTestScheme(t)
			maximum := intstr.FromInt(1)
			ext := rawPDBTestExtension()
			isvc := rawAutoscalerTestISVC(v1beta1.RouterComponent, ext)
			isvc.Spec.Router.PodSpec.Containers = []corev1.Container{{
				Name: constants.MainContainerName, Image: "example.com/runtime:test",
			}}
			key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-router"}
			controller := true
			foreign := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{
				Name: key.Name, Namespace: key.Namespace, UID: types.UID("foreign-pdb"),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "Deployment", Name: "foreign",
					UID: types.UID("foreign-owner"), Controller: &controller,
				}},
			}}
			cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
			liveReader := ctrlclientfake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()
			config := &controllerconfig.InferenceServicesConfig{}
			if mode == constants.RawDeployment {
				config.PodDisruptionBudget.RawDeployment = &controllerconfig.PodDisruptionBudgetPolicy{MaxUnavailable: &maximum}
			} else {
				config.PodDisruptionBudget.OMENative = &controllerconfig.PodDisruptionBudgetPolicy{MaxUnavailable: &maximum}
			}
			deps := &ComponentDeps{
				Client: cl, Clientset: rawAutoscalerTestClientset(), APIReader: liveReader, Scheme: scheme, Config: config,
			}
			router := NewRouter(deps, ComponentInputs{DeploymentMode: mode}, isvc.Spec.Router).(*Router)

			_, err := router.Reconcile(context.Background(), isvc)
			assert.True(t, apierrors.IsConflict(err), "error = %v", err)

			for name, object := range map[string]client.Object{
				"ServiceAccount":   &corev1.ServiceAccount{},
				"Role":             &rbacv1.Role{},
				"RoleBinding":      &rbacv1.RoleBinding{},
				"Deployment":       &appsv1.Deployment{},
				"InferenceReplica": &v1beta1.InferenceReplica{},
			} {
				t.Run(name, func(t *testing.T) {
					err := cl.Get(context.Background(), key, object)
					assert.True(t, apierrors.IsNotFound(err), "%s must not be created, got %v", name, err)
				})
			}
		})
	}
}

func TestRouterReconcileWithoutPDBPolicyPreservesRawBehavior(t *testing.T) {
	scheme := routerPDBTestScheme(t)
	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	ext := rawPDBTestExtension()
	isvc := rawAutoscalerTestISVC(v1beta1.RouterComponent, ext)
	isvc.Spec.Router.PodSpec.Containers = []corev1.Container{{
		Name: constants.MainContainerName, Image: "example.com/runtime:test",
	}}
	deps := &ComponentDeps{
		Client:    cl,
		Clientset: rawAutoscalerTestClientset(),
		Scheme:    scheme,
	}
	router := NewRouter(deps, ComponentInputs{DeploymentMode: constants.RawDeployment}, isvc.Spec.Router).(*Router)

	_, err := router.Reconcile(context.Background(), isvc)
	require.NoError(t, err)

	key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name + "-router"}
	for name, object := range map[string]client.Object{
		"ServiceAccount": &corev1.ServiceAccount{},
		"Role":           &rbacv1.Role{},
		"RoleBinding":    &rbacv1.RoleBinding{},
		"Deployment":     &appsv1.Deployment{},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, cl.Get(context.Background(), key, object))
		})
	}
	err = cl.Get(context.Background(), key, &policyv1.PodDisruptionBudget{})
	assert.True(t, apierrors.IsNotFound(err), "PodDisruptionBudget must be absent, got %v", err)
}

func TestResolveComponentPDBBudgetForMode(t *testing.T) {
	rawMinimum := intstr.FromInt(1)
	omeMaximum := intstr.FromInt(2)
	config := &controllerconfig.InferenceServicesConfig{
		PodDisruptionBudget: controllerconfig.PodDisruptionBudgetConfig{
			RawDeployment: &controllerconfig.PodDisruptionBudgetPolicy{MinAvailable: &rawMinimum},
			OMENative:     &controllerconfig.PodDisruptionBudgetPolicy{MaxUnavailable: &omeMaximum},
		},
	}
	tests := []struct {
		name    string
		base    *BaseComponentFields
		mode    constants.DeploymentModeType
		wantMin *intstr.IntOrString
		wantMax *intstr.IntOrString
	}{
		{name: "nil base", mode: constants.RawDeployment},
		{name: "nil config", base: &BaseComponentFields{}, mode: constants.OMENative},
		{name: "raw fallback", base: &BaseComponentFields{Piece: render.Piece{InferenceServiceConfig: config}}, mode: constants.RawDeployment, wantMin: &rawMinimum},
		{name: "OMENative fallback", base: &BaseComponentFields{Piece: render.Piece{InferenceServiceConfig: config}}, mode: constants.OMENative, wantMax: &omeMaximum},
		{name: "unsupported mode", base: &BaseComponentFields{Piece: render.Piece{InferenceServiceConfig: config}}, mode: constants.PDDisaggregated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			budget, err := resolveComponentPDBBudgetForMode(tt.base, tt.mode, &v1beta1.ComponentExtensionSpec{})
			require.NoError(t, err)
			if tt.wantMin == nil && tt.wantMax == nil {
				assert.Nil(t, budget)
				return
			}
			require.NotNil(t, budget)
			assert.Equal(t, tt.wantMin, budget.MinAvailable)
			assert.Equal(t, tt.wantMax, budget.MaxUnavailable)
		})
	}
}

func routerPDBTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := rawAutoscalerTestScheme(t)
	require.NoError(t, rbacv1.AddToScheme(scheme))
	return scheme
}

// TestConfigEnvVars_SortedDeterministic pins the env order derived from
// the router config map: map iteration order is randomized per run, and
// an order change churns the pod template hash, rolling the router
// deployment for no spec change.
func TestConfigEnvVars_SortedDeterministic(t *testing.T) {
	config := map[string]string{
		"ZED":       "3",
		"ALPHA":     "1",
		"MIDDLE":    "2",
		"OME_DEBUG": "true",
		"B_FLAG":    "x",
	}

	want := []string{"ALPHA", "B_FLAG", "MIDDLE", "OME_DEBUG", "ZED"}
	first := render.ConfigEnvVars(config)
	if len(first) != len(want) {
		t.Fatalf("ConfigEnvVars returned %d vars, want %d", len(first), len(want))
	}
	for i, name := range want {
		if first[i].Name != name {
			t.Errorf("env[%d].Name = %q, want %q (sorted key order)", i, first[i].Name, name)
		}
		if first[i].Value != config[name] {
			t.Errorf("env[%d].Value = %q, want %q", i, first[i].Value, config[name])
		}
	}

	// Repeated conversion of the same map must produce the identical
	// sequence — the property the pod template hash depends on.
	for run := 0; run < 20; run++ {
		again := render.ConfigEnvVars(config)
		for i := range want {
			if again[i] != first[i] {
				t.Fatalf("run %d: env[%d] = %+v, want %+v (order must be stable)", run, i, again[i], first[i])
			}
		}
	}
}

// TestConfigEnvVars_Empty covers nil and empty maps.
func TestConfigEnvVars_Empty(t *testing.T) {
	if got := render.ConfigEnvVars(nil); len(got) != 0 {
		t.Errorf("render.ConfigEnvVars(nil) = %v, want empty", got)
	}
	if got := render.ConfigEnvVars(map[string]string{}); len(got) != 0 {
		t.Errorf("render.ConfigEnvVars(empty) = %v, want empty", got)
	}
}
