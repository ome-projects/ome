package builders

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	knativeapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
)

func TestIngressBuilder_Build(t *testing.T) {
	builder := createIngressBuilder()
	isvc := createTestInferenceServiceIngress("test-isvc", "default")
	setEngineReadyIngress(isvc)

	ingress, err := builder.BuildIngress(context.Background(), isvc)
	assert.NoError(t, err)
	assert.NotNil(t, ingress)

	k8sIngress, ok := ingress.(*netv1.Ingress)
	assert.True(t, ok)
	assert.Equal(t, "test-isvc", k8sIngress.Name)
	assert.Equal(t, "default", k8sIngress.Namespace)
}

func TestIngressBuilder_BuildIngress(t *testing.T) {
	tests := []struct {
		name           string
		isvc           *v1beta1.InferenceService
		expectedError  bool
		expectNil      bool
		expectedRules  int
		expectedEngine bool
	}{
		{
			name:           "engine only",
			isvc:           createTestInferenceServiceIngress("test-isvc", "default"),
			expectedError:  false,
			expectedRules:  1, // engine-only (no duplicate)
			expectedEngine: true,
		},
		{
			name:           "with router",
			isvc:           createTestInferenceServiceWithRouterIngress("test-isvc", "default"),
			expectedError:  false,
			expectedRules:  3, // router rules: default→router, router→router, engine→engine
			expectedEngine: true,
		},
		{
			name:           "with decoder",
			isvc:           createTestInferenceServiceWithDecoderIngress("test-isvc", "default"),
			expectedError:  false,
			expectedRules:  3, // decoder rules: default→engine, engine→engine, decoder→decoder
			expectedEngine: true,
		},
		{
			name:          "predictor not ready",
			isvc:          createTestInferenceServiceIngress("test-isvc", "default"),
			expectNil:     true,
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createIngressBuilder()

			if !tt.expectNil {
				setEngineReadyIngress(tt.isvc)
				if tt.isvc.Spec.Router != nil {
					setRouterReadyIngress(tt.isvc)
				}
				if tt.isvc.Spec.Decoder != nil {
					setDecoderReadyIngress(tt.isvc)
				}
			}

			result, err := builder.BuildIngress(context.Background(), tt.isvc)

			if tt.expectNil {
				var notReady *interfaces.ComponentNotReadyError
				require.ErrorAs(t, err, &notReady)
				assert.Nil(t, result)
				return
			}
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			require.NotNil(t, result)
			ingress, ok := result.(*netv1.Ingress)
			require.True(t, ok)

			assert.Equal(t, tt.isvc.Name, ingress.Name)
			assert.Equal(t, tt.isvc.Namespace, ingress.Namespace)
			assert.Len(t, ingress.Spec.Rules, tt.expectedRules)

			// Check that engine rule is always present
			if tt.expectedEngine {
				engineRuleFound := false
				for _, rule := range ingress.Spec.Rules {
					if rule.Host == "test-isvc.default.example.com" {
						engineRuleFound = true
						break
					}
				}
				assert.True(t, engineRuleFound, "Engine rule should always be present")
			}
		})
	}
}

func TestIngressBuilder_BuildRouterRules(t *testing.T) {
	tests := []struct {
		name          string
		isvc          *v1beta1.InferenceService
		expectedRules int
		expectedError bool
	}{
		{
			name:          "router only",
			isvc:          createTestInferenceServiceWithRouterIngress("test-isvc", "default"),
			expectedRules: 3, // default→router, router→router, engine→engine
			expectedError: false,
		},
		{
			name:          "router with decoder",
			isvc:          createTestInferenceServiceWithRouterAndDecoderIngress("test-isvc", "default"),
			expectedRules: 4, // default→router, router→router, engine→engine, decoder→decoder
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createIngressBuilder()

			rules, err := builder.buildRouterRules(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, rules, tt.expectedRules)
			}
		})
	}
}

func TestIngressBuilder_BuildDecoderRules(t *testing.T) {
	tests := []struct {
		name          string
		isvc          *v1beta1.InferenceService
		expectedRules int
		expectedError bool
	}{
		{
			name:          "decoder only",
			isvc:          createTestInferenceServiceWithDecoderIngress("test-isvc", "default"),
			expectedRules: 3, // default→engine, engine→engine, decoder→decoder
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createIngressBuilder()

			rules, err := builder.buildDecoderRules(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, rules, tt.expectedRules)
			}
		})
	}
}

func TestIngressBuilder_BuildEngineOnlyRules(t *testing.T) {
	tests := []struct {
		name          string
		isvc          *v1beta1.InferenceService
		expectedRules int
		expectedError bool
	}{
		{
			name:          "engine only",
			isvc:          createTestInferenceServiceIngress("test-isvc", "default"),
			expectedRules: 1, // top-level engine
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createIngressBuilder()

			rules, err := builder.buildEngineOnlyRules(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Len(t, rules, tt.expectedRules)
			}
		})
	}
}

// TestIngressBuilder_BackendUsesComponentServicePort verifies the raw
// Ingress backend port follows each component Service's real port rather
// than the hardcoded CommonISVCPort. A runtime on a non-default port
// otherwise produced a backend pointing at a port the Service doesn't
// expose.
func TestIngressBuilder_BackendUsesComponentServicePort(t *testing.T) {
	const customPort int32 = 8000

	isvc := createTestInferenceServiceWithRouterAndDecoderIngress("test-isvc", "default")
	setEngineReadyIngress(isvc)
	setRouterReadyIngress(isvc)
	setDecoderReadyIngress(isvc)

	builder := createIngressBuilder()
	builder.client = fakeClientWithServices(t,
		newServicePort("test-isvc-engine", "default", customPort),
		newServicePort("test-isvc-router", "default", customPort),
		newServicePort("test-isvc-decoder", "default", customPort),
	)

	result, err := builder.BuildIngress(context.Background(), isvc)
	require.NoError(t, err)
	require.NotNil(t, result)
	ingress := result.(*netv1.Ingress)

	require.NotEmpty(t, ingress.Spec.Rules)
	for _, rule := range ingress.Spec.Rules {
		for _, p := range rule.HTTP.Paths {
			assert.Equal(t, customPort, p.Backend.Service.Port.Number,
				"backend for %s must use the component Service port", rule.Host)
		}
	}
}

// TestIngressBuilder_BackendFallsBackToDefaultPort verifies the backend
// port falls back to constants.CommonISVCPort when the Service can't be
// resolved (nil client).
func TestIngressBuilder_BackendFallsBackToDefaultPort(t *testing.T) {
	isvc := createTestInferenceServiceIngress("test-isvc", "default")
	setEngineReadyIngress(isvc)

	// createIngressBuilder leaves client nil -> fallback path.
	builder := createIngressBuilder()

	result, err := builder.BuildIngress(context.Background(), isvc)
	require.NoError(t, err)
	require.NotNil(t, result)
	ingress := result.(*netv1.Ingress)

	require.NotEmpty(t, ingress.Spec.Rules)
	for _, rule := range ingress.Spec.Rules {
		for _, p := range rule.HTTP.Paths {
			assert.Equal(t, int32(constants.CommonISVCPort), p.Backend.Service.Port.Number)
		}
	}
}

func TestIngressBuilder_GenerateRule(t *testing.T) {
	builder := createIngressBuilder()

	rule := builder.generateRule("test-host.example.com", "test-service", "/test-path", 8080)

	assert.Equal(t, "test-host.example.com", rule.Host)
	assert.NotNil(t, rule.HTTP)
	assert.Len(t, rule.HTTP.Paths, 1)

	path := rule.HTTP.Paths[0]
	assert.Equal(t, "/test-path", path.Path)
	assert.Equal(t, netv1.PathTypePrefix, *path.PathType)
	assert.Equal(t, "test-service", path.Backend.Service.Name)
	assert.Equal(t, int32(8080), path.Backend.Service.Port.Number)
}

func TestIngressBuilder_GenerateMetadata(t *testing.T) {
	builder := createIngressBuilder()
	isvc := createTestInferenceServiceIngress("test-isvc", "default")
	isvc.Labels = map[string]string{
		"app":     "test-app",
		"version": "v1",
	}
	isvc.Annotations = map[string]string{
		"nginx.ingress.kubernetes.io/rewrite-target": "/",
		"example.com/annotation":                     "value",
	}

	metadata := builder.generateMetadata(isvc, "engine", "test-engine")

	assert.Equal(t, "test-engine", metadata.Name)
	assert.Equal(t, "default", metadata.Namespace)
	assert.Contains(t, metadata.Labels, "app")
	assert.Contains(t, metadata.Labels, "version")
	assert.Contains(t, metadata.Annotations, "nginx.ingress.kubernetes.io/rewrite-target")
	assert.Contains(t, metadata.Annotations, "example.com/annotation")
}

func TestIngressBuilder_GenerateIngressHost(t *testing.T) {
	tests := []struct {
		name          string
		componentType string
		topLevelFlag  bool
		serviceName   string
		expectedHost  string
		expectedError bool
	}{
		{
			name:          "top level host",
			componentType: "engine",
			topLevelFlag:  true,
			serviceName:   "test-service",
			expectedHost:  "test-isvc.default.example.com",
			expectedError: false,
		},
		{
			name:          "component specific host",
			componentType: "router",
			topLevelFlag:  false,
			serviceName:   "test-service-router",
			expectedHost:  "test-service-router.default.example.com",
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createIngressBuilder()
			isvc := createTestInferenceServiceIngress("test-isvc", "default")

			host, err := builder.generateIngressHost(tt.componentType, tt.topLevelFlag, tt.serviceName, isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedHost, host)
			}
		})
	}
}

// A Component that cannot back a rule yet yields no Ingress and a
// *interfaces.ComponentNotReadyError naming it; the builder writes no
// condition, that is the strategy's job.
func TestIngressBuilder_ComponentReadiness(t *testing.T) {
	tests := []struct {
		name           string
		isvc           *v1beta1.InferenceService
		setupReadiness func(*v1beta1.InferenceService)
		notReady       v1beta1.ComponentType
	}{
		{
			name: "router not ready",
			isvc: createTestInferenceServiceWithRouterIngress("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) {
				setEngineReadyIngress(isvc)
			},
			notReady: v1beta1.RouterComponent,
		},
		{
			name: "decoder not ready",
			isvc: createTestInferenceServiceWithDecoderIngress("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) {
				setEngineReadyIngress(isvc)
			},
			notReady: v1beta1.DecoderComponent,
		},
		{
			name: "engine not ready",
			isvc: createTestInferenceServiceIngress("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) {
			},
			notReady: v1beta1.EngineComponent,
		},
		{
			name: "all components ready",
			isvc: createTestInferenceServiceWithRouterAndDecoderIngress("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) {
				setEngineReadyIngress(isvc)
				setRouterReadyIngress(isvc)
				setDecoderReadyIngress(isvc)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createIngressBuilder()
			tt.setupReadiness(tt.isvc)

			result, err := builder.BuildIngress(context.Background(), tt.isvc)

			if tt.notReady == "" {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				return
			}
			var notReady *interfaces.ComponentNotReadyError
			require.ErrorAs(t, err, &notReady)
			assert.Equal(t, tt.notReady, notReady.Component)
			assert.Nil(t, result)
			assert.Nil(t, tt.isvc.Status.GetCondition(v1beta1.IngressReady), "the builder writes no condition")
		})
	}
}

func TestIngressBuilder_IngressClassName(t *testing.T) {
	tests := []struct {
		name         string
		className    *string
		expectedName *string
	}{
		{
			name:         "with ingress class",
			className:    stringPtrIngress("nginx"),
			expectedName: stringPtrIngress("nginx"),
		},
		{
			name:         "without ingress class",
			className:    nil,
			expectedName: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := &IngressBuilder{
				scheme: func() *runtime.Scheme {
					scheme := runtime.NewScheme()
					_ = v1beta1.AddToScheme(scheme)
					_ = netv1.AddToScheme(scheme)
					return scheme
				}(),
				ingressConfig: &controllerconfig.IngressConfig{
					IngressClassName: tt.className,
					IngressDomain:    "example.com",
					DomainTemplate:   "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				},
				isvcConfig:    &controllerconfig.InferenceServicesConfig{},
				domainService: services.NewDomainService(),
				pathService:   services.NewPathService(),
			}

			isvc := createTestInferenceServiceIngress("test-isvc", "default")
			setEngineReadyIngress(isvc)

			result, err := builder.BuildIngress(context.Background(), isvc)

			assert.NoError(t, err)
			require.NotNil(t, result)

			ingress, ok := result.(*netv1.Ingress)
			require.True(t, ok)

			assert.Equal(t, tt.expectedName, ingress.Spec.IngressClassName)
		})
	}
}

// Helper functions
func createIngressBuilder() *IngressBuilder {
	scheme := runtime.NewScheme()
	_ = v1beta1.AddToScheme(scheme)
	_ = netv1.AddToScheme(scheme)

	return &IngressBuilder{
		scheme: scheme,
		ingressConfig: &controllerconfig.IngressConfig{
			IngressDomain:    "example.com",
			DomainTemplate:   "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			IngressClassName: stringPtrIngress("nginx"),
		},
		isvcConfig:    &controllerconfig.InferenceServicesConfig{},
		domainService: services.NewDomainService(),
		pathService:   services.NewPathService(),
	}
}

func createTestInferenceServiceIngress(name, namespace string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{},
		},
		Status: v1beta1.InferenceServiceStatus{
			Status: duckv1.Status{
				Conditions: []knativeapis.Condition{},
			},
		},
	}
}

func createTestInferenceServiceWithRouterIngress(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceIngress(name, namespace)
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	return isvc
}

func createTestInferenceServiceWithDecoderIngress(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceIngress(name, namespace)
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

func createTestInferenceServiceWithRouterAndDecoderIngress(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceIngress(name, namespace)
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

func setEngineReadyIngress(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.EngineReady, &knativeapis.Condition{
		Type:   v1beta1.EngineReady,
		Status: corev1.ConditionTrue,
	})
}

func setRouterReadyIngress(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.RouterReady, &knativeapis.Condition{
		Type:   v1beta1.RouterReady,
		Status: corev1.ConditionTrue,
	})
}

func setDecoderReadyIngress(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.DecoderReady, &knativeapis.Condition{
		Type:   v1beta1.DecoderReady,
		Status: corev1.ConditionTrue,
	})
}

func stringPtrIngress(s string) *string {
	return &s
}
