package isvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/validation"
)

// routeCapabilityValidator builds a validator whose runtime lookup succeeds,
// so the only rule left to decide a canary's fate is the route-provider gate.
func routeCapabilityValidator(cfg *controllerconfig.IngressConfig) *InferenceServiceValidator {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)
	csr := &v1beta1.ClusterServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "test-runtime"}}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(csr).Build()
	return &InferenceServiceValidator{
		Client:          fakeClient,
		RuntimeSelector: runtimeselector.New(fakeClient),
		IngressConfig:   cfg,
	}
}

// canaryISVC is an OMENative engine canary with a 10% traffic step; router
// adds a router in front of the engine, annotations are applied verbatim.
func canaryISVC(router bool, annotations map[string]string) *v1beta1.InferenceService {
	mode := constants.OMENative
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", Annotations: annotations},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
			Engine:         &v1beta1.EngineSpec{},
			Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
				Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 10, Pause: &v1beta1.RolloutPause{}},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}}},
		},
	}
	if router {
		isvc.Spec.Router = &v1beta1.RouterSpec{}
	}
	return isvc
}

// gatewayAPICluster renders Gateway API routes and declares that provider
// able to weight revisions, while an InferenceService that disables its own
// ingress creation resolves to the None provider, declared unable.
func gatewayAPICluster() *controllerconfig.IngressConfig {
	return &controllerconfig.IngressConfig{
		EnableGatewayAPI: true,
		RouteProviderCapabilities: map[string][]string{
			string(controllerconfig.RouteProviderGatewayAPI): {constants.RouteCapabilityWeightedRevisionRouting},
			string(controllerconfig.RouteProviderNone):       {},
		},
	}
}

func TestValidateCreate_RouteProviderCapabilityGate(t *testing.T) {
	ingressDisabled := map[string]string{constants.IngressDisableCreation: "true"}

	t.Run("canary rejected when the provider declares it cannot weight revisions", func(t *testing.T) {
		v := routeCapabilityValidator(&controllerconfig.IngressConfig{
			RouteProviderCapabilities: map[string][]string{string(controllerconfig.RouteProviderKubernetesIngress): {}},
		})
		_, err := v.ValidateCreate(context.Background(), canaryISVC(false, nil))
		require.Error(t, err)
		assert.Contains(t, err.Error(), validation.ReasonRouteProviderUnsupported)
		assert.Contains(t, err.Error(), `"KubernetesIngress"`)
		assert.Contains(t, err.Error(), constants.RouteCapabilityWeightedRevisionRouting)
		assert.Contains(t, err.Error(), "spec.rollout.groups[0].canary.steps[].traffic")
	})

	t.Run("canary rejected when the InferenceService itself disables ingress creation and None is declared unable", func(t *testing.T) {
		v := routeCapabilityValidator(gatewayAPICluster())
		_, err := v.ValidateCreate(context.Background(), canaryISVC(false, ingressDisabled))
		require.Error(t, err)
		assert.Contains(t, err.Error(), validation.ReasonRouteProviderUnsupported)
		assert.Contains(t, err.Error(), `"None"`)
	})

	t.Run("canary admitted when the provider declares weighted revision routing", func(t *testing.T) {
		v := routeCapabilityValidator(gatewayAPICluster())
		_, err := v.ValidateCreate(context.Background(), canaryISVC(false, nil))
		assert.NoError(t, err)
	})

	t.Run("canary admitted when the provider's capabilities are not declared", func(t *testing.T) {
		v := routeCapabilityValidator(&controllerconfig.IngressConfig{})
		_, err := v.ValidateCreate(context.Background(), canaryISVC(false, nil))
		assert.NoError(t, err)
	})

	t.Run("canary admitted when the validator has no ingress config", func(t *testing.T) {
		v := routeCapabilityValidator(nil)
		_, err := v.ValidateCreate(context.Background(), canaryISVC(false, ingressDisabled))
		assert.NoError(t, err)
	})

	t.Run("engine canary behind a router admitted on a provider that cannot weight revisions", func(t *testing.T) {
		v := routeCapabilityValidator(gatewayAPICluster())
		_, err := v.ValidateCreate(context.Background(), canaryISVC(true, ingressDisabled))
		assert.NoError(t, err)
	})
}

func TestValidateUpdate_RouteProviderCapabilityGateRatchets(t *testing.T) {
	v := routeCapabilityValidator(&controllerconfig.IngressConfig{
		RouteProviderCapabilities: map[string][]string{string(controllerconfig.RouteProviderKubernetesIngress): {}},
	})
	stored := canaryISVC(false, nil)

	t.Run("an edit that leaves the intent and provider alone is admitted", func(t *testing.T) {
		scaled := stored.DeepCopy()
		one := 1
		scaled.Spec.Engine.MinReplicas = &one
		_, err := v.ValidateUpdate(context.Background(), stored, scaled)
		assert.NoError(t, err)
	})

	t.Run("an edit that changes the rollout is gated", func(t *testing.T) {
		edited := stored.DeepCopy()
		edited.Spec.Rollout.Groups[0].Canary.Steps[0].Traffic = 20
		_, err := v.ValidateUpdate(context.Background(), stored, edited)
		require.Error(t, err)
		assert.Contains(t, err.Error(), validation.ReasonRouteProviderUnsupported)
	})

	t.Run("adding the canary is gated", func(t *testing.T) {
		plain := stored.DeepCopy()
		plain.Spec.Rollout = nil
		_, err := v.ValidateUpdate(context.Background(), plain, stored)
		require.Error(t, err)
		assert.Contains(t, err.Error(), validation.ReasonRouteProviderUnsupported)
	})
}
