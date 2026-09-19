package builders

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
)

func TestHTTPRouteBuilder_GetResourceType(t *testing.T) {
	builder := createHTTPRouteBuilder()
	assert.Equal(t, "HTTPRoute", builder.GetResourceType())
}

func TestHTTPRouteBuilder_Build(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	httpRoute, err := builder.Build(context.Background(), isvc)
	assert.NoError(t, err)
	assert.NotNil(t, httpRoute)

	route, ok := httpRoute.(*gatewayapiv1.HTTPRoute)
	assert.True(t, ok)
	// engine HTTPRoute is named "<isvc>-engine", distinct from the
	// top-level "<isvc>" route, and backs the "<isvc>-engine" Service
	// (matching engine.go's defaultEngineName).
	assert.Equal(t, "test-isvc-engine", route.Name)
	assert.Equal(t, "default", route.Namespace)
}

func TestHTTPRouteBuilder_BuildHTTPRoute_EngineComponent(t *testing.T) {
	tests := []struct {
		name            string
		isvc            *v1beta1.InferenceService
		expectedName    string
		expectedError   bool
		expectedRules   int
		expectedHosts   int
		expectNil       bool
		expectedTimeout *gatewayapiv1.Duration
	}{
		{
			name: "engine component ready",
			isvc: createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			// engine HTTPRoute is "<isvc>-engine", not "<isvc>".
			expectedName:    "test-isvc-engine",
			expectedError:   false,
			expectedRules:   1,
			expectedHosts:   1,
			expectedTimeout: toGatewayAPIDuration(1800), // cluster default from IngressConfig
		},
		{
			name:          "engine component not ready",
			isvc:          createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			expectNil:     true,
			expectedError: false,
		},
		{
			name:            "engine component with custom timeout",
			isvc:            createTestInferenceServiceHTTPRouteWithTimeout("test-isvc", "default", 120),
			expectedName:    "test-isvc-engine",
			expectedError:   false,
			expectedRules:   1,
			expectedHosts:   1,
			expectedTimeout: toGatewayAPIDuration(120),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()

			if !tt.expectNil {
				setEngineReady(tt.isvc)
			}

			result, err := builder.BuildHTTPRoute(context.Background(), tt.isvc, EngineComponent)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			httpRoute, ok := result.(*gatewayapiv1.HTTPRoute)
			require.True(t, ok)

			assert.Equal(t, tt.expectedName, httpRoute.Name)
			assert.Len(t, httpRoute.Spec.Rules, tt.expectedRules)
			assert.Len(t, httpRoute.Spec.Hostnames, tt.expectedHosts)

			if tt.expectedTimeout != nil {
				assert.Equal(t, tt.expectedTimeout, httpRoute.Spec.Rules[0].Timeouts.Request)
			}
		})
	}
}

func TestHTTPRouteBuilder_BuildHTTPRoute_RouterComponent(t *testing.T) {
	tests := []struct {
		name            string
		isvc            *v1beta1.InferenceService
		expectedName    string
		expectedError   bool
		expectNil       bool
		expectedTimeout *gatewayapiv1.Duration
	}{
		{
			name:            "router component ready",
			isvc:            createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			expectedName:    "test-isvc-router",
			expectedError:   false,
			expectedTimeout: toGatewayAPIDuration(1800),
		},
		{
			name:          "router component not ready",
			isvc:          createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			expectNil:     true,
			expectedError: false,
		},
		{
			name:            "router component with custom timeout",
			isvc:            createTestInferenceServiceWithRouterHTTPRouteTimeout("test-isvc", "default", 90),
			expectedName:    "test-isvc-router",
			expectedError:   false,
			expectedTimeout: toGatewayAPIDuration(90),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()

			if !tt.expectNil {
				setEngineReady(tt.isvc)
				setRouterReady(tt.isvc)
			}

			result, err := builder.BuildHTTPRoute(context.Background(), tt.isvc, RouterComponent)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			httpRoute, ok := result.(*gatewayapiv1.HTTPRoute)
			require.True(t, ok)

			assert.Equal(t, tt.expectedName, httpRoute.Name)
			if tt.expectedTimeout != nil {
				assert.Equal(t, tt.expectedTimeout, httpRoute.Spec.Rules[0].Timeouts.Request)
			}
		})
	}
}

func TestHTTPRouteBuilder_BuildHTTPRoute_DecoderComponent(t *testing.T) {
	tests := []struct {
		name            string
		isvc            *v1beta1.InferenceService
		expectedName    string
		expectedError   bool
		expectNil       bool
		expectedTimeout *gatewayapiv1.Duration
	}{
		{
			name:            "decoder component ready",
			isvc:            createTestInferenceServiceWithDecoderHTTPRoute("test-isvc", "default"),
			expectedName:    "test-isvc-decoder",
			expectedError:   false,
			expectedTimeout: toGatewayAPIDuration(1800),
		},
		{
			name:          "decoder component not ready",
			isvc:          createTestInferenceServiceWithDecoderHTTPRoute("test-isvc", "default"),
			expectNil:     true,
			expectedError: false,
		},
		{
			name:            "decoder component with custom timeout",
			isvc:            createTestInferenceServiceWithDecoderHTTPRouteTimeout("test-isvc", "default", 45),
			expectedName:    "test-isvc-decoder",
			expectedError:   false,
			expectedTimeout: toGatewayAPIDuration(45),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()

			if !tt.expectNil {
				setEngineReady(tt.isvc)
				setDecoderReady(tt.isvc)
			}

			result, err := builder.BuildHTTPRoute(context.Background(), tt.isvc, DecoderComponent)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			httpRoute, ok := result.(*gatewayapiv1.HTTPRoute)
			require.True(t, ok)

			assert.Equal(t, tt.expectedName, httpRoute.Name)
			if tt.expectedTimeout != nil {
				assert.Equal(t, tt.expectedTimeout, httpRoute.Spec.Rules[0].Timeouts.Request)
			}
		})
	}
}

func TestHTTPRouteBuilder_BuildHTTPRoute_TopLevelComponent(t *testing.T) {
	tests := []struct {
		name           string
		isvc           *v1beta1.InferenceService
		setupReadiness func(*v1beta1.InferenceService)
		expectedName   string
		expectedError  bool
		expectedRules  int
		expectNil      bool
	}{
		{
			name:           "top level with engine only",
			isvc:           createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) { setEngineReady(isvc) },
			expectedName:   "test-isvc",
			expectedRules:  1,
		},
		{
			name: "top level with router",
			isvc: createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) {
				setEngineReady(isvc)
				setRouterReady(isvc)
			},
			expectedName:  "test-isvc",
			expectedRules: 1,
		},
		{
			name: "top level with router and decoder",
			isvc: createTestInferenceServiceWithRouterAndDecoderHTTPRoute("test-isvc", "default"),
			setupReadiness: func(isvc *v1beta1.InferenceService) {
				setEngineReady(isvc)
				setRouterReady(isvc)
				setDecoderReady(isvc)
			},
			expectedName:  "test-isvc",
			expectedRules: 1,
		},
		{
			name:           "top level engine not ready returns nil",
			isvc:           createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			setupReadiness: func(_ *v1beta1.InferenceService) {},
			expectNil:      true,
		},
		{
			name:           "top level router not ready still creates route",
			isvc:           createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			setupReadiness: func(_ *v1beta1.InferenceService) {},
			expectedName:   "test-isvc",
			expectedRules:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()
			tt.setupReadiness(tt.isvc)

			result, err := builder.BuildHTTPRoute(context.Background(), tt.isvc, TopLevelComponent)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			httpRoute, ok := result.(*gatewayapiv1.HTTPRoute)
			require.True(t, ok)

			assert.Equal(t, tt.expectedName, httpRoute.Name)
			assert.Len(t, httpRoute.Spec.Rules, tt.expectedRules)
		})
	}
}

// ----- Weighted backendRefs from status.traffic[] -----

// TestHTTPRouteBuilder_WeightedBackends_FromStatusTraffic verifies the
// HTTPRoute builder reads status.components[<c>].traffic[] and emits
// weighted backendRefs. Single-entry status collapses to one
// backendRef (today's behavior). Multi-entry status produces weighted
// backendRefs in the same order. Per-mode reconcilers populate the
// status; this commit only wires the builder to consume it.
func TestHTTPRouteBuilder_TopLevelRoutesToRouterEvenWhenNotReady(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default")

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
	require.NoError(t, err)
	require.NotNil(t, result)
	route := result.(*gatewayapiv1.HTTPRoute)

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	assert.Equal(t, "test-isvc-router", string(route.Spec.Rules[0].BackendRefs[0].Name),
		"top-level HTTPRoute must point to router service even when RouterReady is false")
	assert.Equal(t, int32(30080), int32(*route.Spec.Rules[0].BackendRefs[0].Port),
		"top-level HTTPRoute must use router port even when RouterReady is false")
}

// TestHTTPRouteBuilder_EngineAndTopLevel_DistinctNames pins route
// naming: when both engine and top-level routes are reconciled for the
// same ISVC, they must have distinct names so the top-level reconcile
// doesn't overwrite the engine route. If both were named "<isvc>"
// (PredictorServiceName == isvc.Name) the top-level would always win,
// leaving the engine externally unaddressable.
func TestHTTPRouteBuilder_EngineAndTopLevel_DistinctNames(t *testing.T) {
	builder := createHTTPRouteBuilder()

	// Engine-only ISVC: engine route is "<isvc>-engine", top-level is "<isvc>".
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	engineResult, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
	require.NoError(t, err)
	require.NotNil(t, engineResult)
	engineRoute := engineResult.(*gatewayapiv1.HTTPRoute)

	topLevelResult, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
	require.NoError(t, err)
	require.NotNil(t, topLevelResult)
	topLevelRoute := topLevelResult.(*gatewayapiv1.HTTPRoute)

	assert.NotEqual(t, engineRoute.Name, topLevelRoute.Name,
		"engine and top-level HTTPRoute names must differ so the top-level reconcile does not overwrite engine")
	assert.Equal(t, "test-isvc-engine", engineRoute.Name)
	assert.Equal(t, "test-isvc", topLevelRoute.Name)
}

// TestHTTPRouteBuilder_EngineBackendUsesEngineService pins the engine
// route's backend: the engine Service is named "<isvc>-engine"
// (defaultEngineName in engine.go), not "<isvc>" (PredictorServiceName);
// a route referencing the latter points at a non-existent backend.
func TestHTTPRouteBuilder_EngineBackendUsesEngineService(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
	require.NoError(t, err)
	require.NotNil(t, result)
	route := result.(*gatewayapiv1.HTTPRoute)

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	backendName := string(route.Spec.Rules[0].BackendRefs[0].Name)
	assert.Equal(t, "test-isvc-engine", backendName,
		"engine HTTPRoute must back the <isvc>-engine Service (matches defaultEngineName in engine.go)")
}

// TestHTTPRouteBuilder_TopLevelEngineFallback_UsesEngineService
// verifies that when there is no router, the top-level HTTPRoute's
// engine fallback backend is the "<isvc>-engine" Service, not the
// legacy "<isvc>" backend that doesn't exist.
func TestHTTPRouteBuilder_TopLevelEngineFallback_UsesEngineService(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
	require.NoError(t, err)
	require.NotNil(t, result)
	route := result.(*gatewayapiv1.HTTPRoute)

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	backendName := string(route.Spec.Rules[0].BackendRefs[0].Name)
	assert.Equal(t, "test-isvc-engine", backendName,
		"top-level HTTPRoute (engine fallback) must back the <isvc>-engine Service when there is no router")
}

// TestHTTPRouteBuilder_BuildHTTPRoute_ReturnsInterfaceNilWhenComponentNotReady
// pins the dispatcher-side guard against a typed-nil-through-interface panic.
//
// A per-component builder returns (nil, nil) for the "component not ready"
// branch. Returned directly through the client.Object interface, Go wraps
// the typed-nil pointer — the interface itself is non-nil (carries type
// info), only the inner value is nil. A caller checking `if obj == nil`
// sees FALSE and dereferences the typed-nil pointer in
// controllerutil.SetControllerReference, crashing the reconciler.
//
// The dispatcher detects the typed-nil and explicitly returns untyped nil
// so `desired == nil` in the caller is true.
//
// NOTE: testify's `assert.Nil` uses reflect.IsNil under the hood and would
// pass even for a typed-nil wrapped in an interface — so this test uses the
// raw `== nil` comparison, which is the actual check the strategy performs.
func TestHTTPRouteBuilder_BuildHTTPRoute_ReturnsInterfaceNilWhenComponentNotReady(t *testing.T) {
	cases := []struct {
		name          string
		componentType string
		isvc          *v1beta1.InferenceService
	}{
		{
			name:          "engine not ready",
			componentType: EngineComponent,
			isvc:          createTestInferenceServiceHTTPRoute("test-isvc", "default"),
		},
		{
			name:          "router not ready",
			componentType: RouterComponent,
			isvc:          createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
		},
		{
			name:          "decoder not ready",
			componentType: DecoderComponent,
			isvc:          createTestInferenceServiceWithDecoderHTTPRoute("test-isvc", "default"),
		},
		{
			name:          "top-level engine fallback not ready",
			componentType: TopLevelComponent,
			isvc:          createTestInferenceServiceHTTPRoute("test-isvc", "default"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := createHTTPRouteBuilder()

			// Deliberately do NOT mark any condition Ready, so the inner
			// per-component builders take the "component not ready" branch
			// and return (nil, nil).
			result, err := builder.BuildHTTPRoute(context.Background(), tc.isvc, tc.componentType)
			require.NoError(t, err)

			// A (*HTTPRoute)(nil) wrapped in a non-nil client.Object
			// interface makes `result == nil` false and the strategy panic
			// on the subsequent SetControllerReference. The dispatcher
			// returns interface-nil instead, so this assertion holds.
			//
			// We deliberately use bare `== nil` here, NOT assert.Nil, since
			// the former is the exact check the strategy performs.
			if result != nil {
				t.Fatalf("BuildHTTPRoute(%s) must return interface-nil (== nil) when the component is not ready, "+
					"but returned a typed-nil-wrapped-in-interface (%T). This is the typed-nil-through-interface bug "+
					"that crashed the controller's reconcile loop on PD-disaggregated ISVCs.",
					tc.componentType, result)
			}
		})
	}
}

func TestHTTPRouteBuilder_BuildHTTPRoute_UnsupportedComponent(t *testing.T) {
	builder := createHTTPRouteBuilder()
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, "unsupported")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported component type")
	assert.Nil(t, result)
}

func TestHTTPRouteBuilder_CreatePathPrefixMatch(t *testing.T) {
	builder := createHTTPRouteBuilder()

	match := builder.createPathPrefixMatch("/playground/test-isvc/")

	assert.NotNil(t, match.Path)
	assert.Equal(t, gatewayapiv1.PathMatchPathPrefix, *match.Path.Type)
	assert.Equal(t, "/playground/test-isvc/", *match.Path.Value)
}

func TestHTTPRouteBuilder_AddIsvcHeaders(t *testing.T) {
	builder := createHTTPRouteBuilder()

	filter := builder.addIsvcHeaders("test-service", "test-namespace")

	assert.Equal(t, gatewayapiv1.HTTPRouteFilterRequestHeaderModifier, filter.Type)
	assert.NotNil(t, filter.RequestHeaderModifier)
	assert.Len(t, filter.RequestHeaderModifier.Set, 2)

	headers := make(map[string]string)
	for _, header := range filter.RequestHeaderModifier.Set {
		headers[string(header.Name)] = header.Value
	}

	assert.Equal(t, "test-service", headers["OMe-Isvc-Name"])
	assert.Equal(t, "test-namespace", headers["OME-Isvc-Namespace"])
}

func TestHTTPRouteBuilder_CreateHTTPRouteRule(t *testing.T) {
	builder := createHTTPRouteBuilder()

	matches := []gatewayapiv1.HTTPRouteMatch{
		builder.createPathPrefixMatch("/playground/test-service/"),
	}
	filters := []gatewayapiv1.HTTPRouteFilter{
		builder.addIsvcHeaders("test-service", "test-namespace"),
	}
	timeout := toGatewayAPIDuration(30)

	rule := builder.createHTTPRouteRule(matches, filters, "test-service", "test-namespace", 8080, timeout)

	assert.Len(t, rule.Matches, 1)
	assert.Len(t, rule.Filters, 1)
	assert.Len(t, rule.BackendRefs, 1)
	assert.Equal(t, timeout, rule.Timeouts.Request)

	backend := rule.BackendRefs[0]
	assert.Equal(t, gatewayapiv1.ObjectName("test-service"), backend.BackendRef.BackendObjectReference.Name)
	assert.Equal(t, "test-namespace", string(*backend.BackendRef.BackendObjectReference.Namespace))
	assert.Equal(t, int32(8080), int32(*backend.BackendRef.BackendObjectReference.Port))
}

func TestHTTPRouteBuilder_ToGatewayAPIDuration(t *testing.T) {
	tests := []struct {
		name     string
		seconds  int64
		expected string
	}{
		{
			name:     "30 seconds",
			seconds:  30,
			expected: "30s",
		},
		{
			name:     "60 seconds",
			seconds:  60,
			expected: "60s",
		},
		{
			name:     "120 seconds",
			seconds:  120,
			expected: "120s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duration := toGatewayAPIDuration(tt.seconds)
			assert.NotNil(t, duration)
			assert.Equal(t, tt.expected, string(*duration))
		})
	}
}

// ----- backendRef ports follow the component Service's real port -----

// TestHTTPRouteBuilder_BackendRefUsesComponentServicePort verifies the
// HTTPRoute backendRef targets each component Service's actual port (its
// runner's containerPort) rather than the hardcoded default. For a runtime
// on a non-default port (e.g. 8000), a backendRef on the default port would
// match no Service port and yield no endpoints.
func TestHTTPRouteBuilder_BackendRefUsesComponentServicePort(t *testing.T) {
	const customPort int32 = 8000

	isvc := createTestInferenceServiceWithRouterAndDecoderHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)
	setRouterReady(isvc)
	setDecoderReady(isvc)

	c := fakeClientWithServices(t,
		newServicePort("test-isvc-engine", "default", customPort),
		newServicePort("test-isvc-router", "default", customPort),
		newServicePort("test-isvc-decoder", "default", customPort),
	)
	builder := createHTTPRouteBuilderWithClient(c)

	for _, tc := range []struct {
		component string
	}{
		{EngineComponent},
		{RouterComponent},
		{DecoderComponent},
		{TopLevelComponent}, // routes to router here; router Service is on customPort
	} {
		t.Run(tc.component, func(t *testing.T) {
			result, err := builder.BuildHTTPRoute(context.Background(), isvc, tc.component)
			require.NoError(t, err)
			require.NotNil(t, result)
			route := result.(*gatewayapiv1.HTTPRoute)

			require.Len(t, route.Spec.Rules, 1)
			require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
			assert.Equal(t, customPort, int32(*route.Spec.Rules[0].BackendRefs[0].Port),
				"%s backendRef must use the component Service port, not the default", tc.component)
		})
	}
}

// TestHTTPRouteBuilder_TopLevelEngineFallbackUsesEngineServicePort covers
// the no-router top-level route: its engine fallback backendRef must use
// the engine Service's real port.
func TestHTTPRouteBuilder_TopLevelEngineFallbackUsesEngineServicePort(t *testing.T) {
	const customPort int32 = 8000

	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	c := fakeClientWithServices(t, newServicePort("test-isvc-engine", "default", customPort))
	builder := createHTTPRouteBuilderWithClient(c)

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
	require.NoError(t, err)
	require.NotNil(t, result)
	route := result.(*gatewayapiv1.HTTPRoute)

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	assert.Equal(t, customPort, int32(*route.Spec.Rules[0].BackendRefs[0].Port))
}

// TestHTTPRouteBuilder_BackendRefFallsBackToDefaultPort verifies that when
// the component Service can't be resolved (nil client / missing Service),
// the backendRef uses the default port consts (preserves prior behavior).
func TestHTTPRouteBuilder_BackendRefFallsBackToDefaultPort(t *testing.T) {
	for _, tc := range []struct {
		name        string
		component   string
		setup       func(*v1beta1.InferenceService)
		isvc        *v1beta1.InferenceService
		expportPort int32
	}{
		{
			name:        "engine",
			component:   EngineComponent,
			isvc:        createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			setup:       func(i *v1beta1.InferenceService) { setEngineReady(i) },
			expportPort: EngineDefaultPort,
		},
		{
			name:      "router",
			component: RouterComponent,
			isvc:      createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			setup: func(i *v1beta1.InferenceService) {
				setEngineReady(i)
				setRouterReady(i)
			},
			expportPort: RouterDefaultPort,
		},
		{
			name:      "decoder",
			component: DecoderComponent,
			isvc:      createTestInferenceServiceWithDecoderHTTPRoute("test-isvc", "default"),
			setup: func(i *v1beta1.InferenceService) {
				setEngineReady(i)
				setDecoderReady(i)
			},
			expportPort: DecoderDefaultPort,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(tc.isvc)
			// nil-client builder -> port resolution falls back to defaults.
			builder := createHTTPRouteBuilder()

			result, err := builder.BuildHTTPRoute(context.Background(), tc.isvc, tc.component)
			require.NoError(t, err)
			require.NotNil(t, result)
			route := result.(*gatewayapiv1.HTTPRoute)

			require.Len(t, route.Spec.Rules, 1)
			require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
			assert.Equal(t, tc.expportPort, int32(*route.Spec.Rules[0].BackendRefs[0].Port))
		})
	}
}

// ----- per-ISVC subdomain ingress scheme (opt-in) -----

// TestHTTPRouteBuilder_SharedHostScheme_Default pins the default (flag OFF)
// scheme: every component route uses the shared "llm.<IngressDomain>" host,
// a "/<namespace>/<service>/" path prefix, and a urlRewrite that strips it.
// This guards existing clusters from a silent change when PerISVCSubdomain
// is unset.
func TestHTTPRouteBuilder_SharedHostScheme_Default(t *testing.T) {
	for _, tc := range []struct {
		name         string
		component    string
		setup        func(*v1beta1.InferenceService)
		isvc         *v1beta1.InferenceService
		expectedPath string
	}{
		{
			name:         "engine",
			component:    EngineComponent,
			isvc:         createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			setup:        func(i *v1beta1.InferenceService) { setEngineReady(i) },
			expectedPath: "/default/test-isvc-engine/",
		},
		{
			name:      "router",
			component: RouterComponent,
			isvc:      createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			setup: func(i *v1beta1.InferenceService) {
				setEngineReady(i)
				setRouterReady(i)
			},
			expectedPath: "/default/test-isvc-router/",
		},
		{
			name:      "decoder",
			component: DecoderComponent,
			isvc:      createTestInferenceServiceWithDecoderHTTPRoute("test-isvc", "default"),
			setup: func(i *v1beta1.InferenceService) {
				setEngineReady(i)
				setDecoderReady(i)
			},
			expectedPath: "/default/test-isvc-decoder/",
		},
		{
			name:         "toplevel",
			component:    TopLevelComponent,
			isvc:         createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			setup:        func(i *v1beta1.InferenceService) { setEngineReady(i) },
			expectedPath: "/default/test-isvc/",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(tc.isvc)
			builder := createHTTPRouteBuilder()

			result, err := builder.BuildHTTPRoute(context.Background(), tc.isvc, tc.component)
			require.NoError(t, err)
			require.NotNil(t, result)
			route := result.(*gatewayapiv1.HTTPRoute)

			require.Len(t, route.Spec.Hostnames, 1)
			assert.Equal(t, gatewayapiv1.Hostname("llm.example.com"), route.Spec.Hostnames[0],
				"default scheme must use the shared llm.<domain> host")

			require.Len(t, route.Spec.Rules, 1)
			require.Len(t, route.Spec.Rules[0].Matches, 1)
			assert.Equal(t, tc.expectedPath, *route.Spec.Rules[0].Matches[0].Path.Value,
				"default scheme must use the /<namespace>/<service>/ path prefix")
			assert.True(t, hasURLRewriteFilter(route.Spec.Rules[0]),
				"default scheme must strip the path prefix via urlRewrite")
		})
	}
}

// TestHTTPRouteBuilder_SharedHostPrefix verifies the shared-host scheme honors
// the config-supplied SharedHostPrefix (there is no hardcoded "llm"): a custom
// prefix is used verbatim, and an empty prefix yields the bare IngressDomain.
func TestHTTPRouteBuilder_SharedHostPrefix(t *testing.T) {
	for _, tc := range []struct {
		name     string
		prefix   string
		wantHost gatewayapiv1.Hostname
	}{
		{name: "custom prefix", prefix: "serving", wantHost: "serving.example.com"},
		{name: "default llm", prefix: "llm", wantHost: "llm.example.com"},
		{name: "empty prefix yields bare domain", prefix: "", wantHost: "example.com"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
			setEngineReady(isvc)
			builder := createHTTPRouteBuilder()
			builder.ingressConfig.SharedHostPrefix = tc.prefix

			result, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
			require.NoError(t, err)
			route := result.(*gatewayapiv1.HTTPRoute)
			require.Len(t, route.Spec.Hostnames, 1)
			assert.Equal(t, tc.wantHost, route.Spec.Hostnames[0],
				"shared-host scheme must use the config-supplied prefix, not a hardcoded value")
		})
	}
}

// TestHTTPRouteBuilder_AdditionalIngressGateways verifies a route attaches to the
// primary gateway plus each additional gateway, with one parentRef and one
// hostname per gateway (each host rendered from that gateway's own domain).
func TestHTTPRouteBuilder_AdditionalIngressGateways(t *testing.T) {
	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	builder := createHTTPRouteBuilder()
	builder.ingressConfig.PerISVCSubdomain = true
	builder.ingressConfig.OmeIngressGateway = "envoy-gateway-system/int-gw"
	builder.ingressConfig.IngressDomain = "int.example.com"
	builder.ingressConfig.AdditionalIngressGateways = []controllerconfig.IngressGatewaySpec{
		{OmeIngressGateway: "envoy-gateway-system/ext-gw", IngressDomain: "ext.example.com"},
	}

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
	require.NoError(t, err)
	route := result.(*gatewayapiv1.HTTPRoute)

	// Two gateways => two parentRefs + two hostnames, index-aligned.
	require.Len(t, route.Spec.ParentRefs, 2, "one parentRef per gateway")
	require.Len(t, route.Spec.Hostnames, 2, "one hostname per gateway")
	assert.Equal(t, gatewayapiv1.ObjectName("int-gw"), route.Spec.ParentRefs[0].Name)
	assert.Equal(t, gatewayapiv1.ObjectName("ext-gw"), route.Spec.ParentRefs[1].Name)
	assert.Equal(t, gatewayapiv1.Hostname("test-isvc.default.int.example.com"), route.Spec.Hostnames[0])
	assert.Equal(t, gatewayapiv1.Hostname("test-isvc.default.ext.example.com"), route.Spec.Hostnames[1])
}

// TestHTTPRouteBuilder_NamespaceIngressGateways verifies the per-namespace gateway
// override: an ISVC in a namespace listed in NamespaceIngressGateways attaches to
// that namespace's gateway(s) instead of the cluster default (the host stays
// namespace-scoped via DomainTemplate); an ISVC in an unlisted namespace keeps the
// cluster default; and a per-ISVC gateway annotation still wins for the primary.
func TestHTTPRouteBuilder_NamespaceIngressGateways(t *testing.T) {
	nsGateways := map[string]controllerconfig.NamespaceIngressGateway{
		"prod": {
			Primary: controllerconfig.IngressGatewaySpec{
				OmeIngressGateway: "envoy-gateway-system/prod-int-gw",
				IngressDomain:     "int.example.com",
			},
			Additional: []controllerconfig.IngressGatewaySpec{
				{OmeIngressGateway: "envoy-gateway-system/prod-ext-gw", IngressDomain: "ext.example.com"},
			},
		},
	}

	newBuilder := func() *HTTPRouteBuilder {
		b := createHTTPRouteBuilder()
		b.ingressConfig.PerISVCSubdomain = true
		b.ingressConfig.OmeIngressGateway = "envoy-gateway-system/tenant-int-gw"
		b.ingressConfig.IngressDomain = "int.example.com"
		b.ingressConfig.NamespaceIngressGateways = nsGateways
		return b
	}
	buildTopLevel := func(t *testing.T, b *HTTPRouteBuilder, isvc *v1beta1.InferenceService) *gatewayapiv1.HTTPRoute {
		result, err := b.BuildHTTPRoute(context.Background(), isvc, TopLevelComponent)
		require.NoError(t, err)
		return result.(*gatewayapiv1.HTTPRoute)
	}

	t.Run("namespace in map overrides to that namespace's gateways", func(t *testing.T) {
		isvc := createTestInferenceServiceHTTPRoute("test-isvc", "prod")
		setEngineReady(isvc)

		route := buildTopLevel(t, newBuilder(), isvc)

		require.Len(t, route.Spec.ParentRefs, 2, "primary + additional prod gateways")
		assert.Equal(t, gatewayapiv1.ObjectName("prod-int-gw"), route.Spec.ParentRefs[0].Name)
		assert.Equal(t, gatewayapiv1.ObjectName("prod-ext-gw"), route.Spec.ParentRefs[1].Name)
		// Host is still namespace-scoped (rendered from DomainTemplate), unchanged.
		assert.Equal(t, gatewayapiv1.Hostname("test-isvc.prod.int.example.com"), route.Spec.Hostnames[0])
		assert.Equal(t, gatewayapiv1.Hostname("test-isvc.prod.ext.example.com"), route.Spec.Hostnames[1])
	})

	t.Run("namespace not in map keeps cluster default", func(t *testing.T) {
		isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
		setEngineReady(isvc)

		route := buildTopLevel(t, newBuilder(), isvc)

		require.Len(t, route.Spec.ParentRefs, 1, "cluster-default primary only")
		assert.Equal(t, gatewayapiv1.ObjectName("tenant-int-gw"), route.Spec.ParentRefs[0].Name)
	})

	t.Run("per-ISVC gateway annotation wins over namespace default", func(t *testing.T) {
		isvc := createTestInferenceServiceHTTPRoute("test-isvc", "prod")
		setEngineReady(isvc)
		if isvc.Annotations == nil {
			isvc.Annotations = map[string]string{}
		}
		isvc.Annotations[constants.IngressGatewayOverride] = "envoy-gateway-system/special-gw"

		b := newBuilder()
		// ResolveIngressConfig applies the annotation to the primary before the
		// builder runs; simulate that resolved state here.
		b.ingressConfig.OmeIngressGateway = "envoy-gateway-system/special-gw"

		route := buildTopLevel(t, b, isvc)

		// Annotation controls the primary parentRef, overriding the namespace map.
		assert.Equal(t, gatewayapiv1.ObjectName("special-gw"), route.Spec.ParentRefs[0].Name)
	})
}

// TestHTTPRouteBuilder_PerISVCSubdomainScheme verifies the opt-in (flag ON)
// scheme for engine, router, decoder, and top-level routes: the route host
// equals the ISVC's per-ISVC subdomain (the same host status.url advertises,
// rendered from DomainTemplate), the match is at root "/", and there is no
// urlRewrite filter (the host identifies the ISVC, so requests pass through).
func TestHTTPRouteBuilder_PerISVCSubdomainScheme(t *testing.T) {
	for _, tc := range []struct {
		name      string
		component string
		setup     func(*v1beta1.InferenceService)
		isvc      *v1beta1.InferenceService
	}{
		{
			name:      "engine",
			component: EngineComponent,
			isvc:      createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			setup:     func(i *v1beta1.InferenceService) { setEngineReady(i) },
		},
		{
			name:      "router",
			component: RouterComponent,
			isvc:      createTestInferenceServiceWithRouterHTTPRoute("test-isvc", "default"),
			setup: func(i *v1beta1.InferenceService) {
				setEngineReady(i)
				setRouterReady(i)
			},
		},
		{
			name:      "decoder",
			component: DecoderComponent,
			isvc:      createTestInferenceServiceWithDecoderHTTPRoute("test-isvc", "default"),
			setup: func(i *v1beta1.InferenceService) {
				setEngineReady(i)
				setDecoderReady(i)
			},
		},
		{
			name:      "toplevel",
			component: TopLevelComponent,
			isvc:      createTestInferenceServiceHTTPRoute("test-isvc", "default"),
			setup:     func(i *v1beta1.InferenceService) { setEngineReady(i) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(tc.isvc)
			builder := createHTTPRouteBuilderPerISVC()

			// The expected host is whatever the domainService renders for
			// status.url — reuse the exact same call so the test can't drift
			// from the builder either.
			expectedHost, err := builder.domainService.GenerateDomainName(tc.isvc.Name, tc.isvc.ObjectMeta, builder.ingressConfig)
			require.NoError(t, err)
			require.Equal(t, "test-isvc.default.example.com", expectedHost)

			result, err := builder.BuildHTTPRoute(context.Background(), tc.isvc, tc.component)
			require.NoError(t, err)
			require.NotNil(t, result)
			route := result.(*gatewayapiv1.HTTPRoute)

			require.Len(t, route.Spec.Hostnames, 1)
			assert.Equal(t, gatewayapiv1.Hostname(expectedHost), route.Spec.Hostnames[0],
				"per-ISVC host must equal the status.url host (domainService.GenerateDomainName)")

			require.Len(t, route.Spec.Rules, 1)
			require.Len(t, route.Spec.Rules[0].Matches, 1)
			assert.Equal(t, "/", *route.Spec.Rules[0].Matches[0].Path.Value,
				"per-ISVC scheme matches at root path")
			assert.False(t, hasURLRewriteFilter(route.Spec.Rules[0]),
				"per-ISVC scheme must not rewrite the path (no prefix to strip)")
		})
	}
}

// TestHTTPRouteBuilder_PerISVCSubdomain_KeepsBackendPortResolution confirms the
// opt-in scheme still resolves the backendRef port from the live Service,
// independent of host/path changes.
func TestHTTPRouteBuilder_PerISVCSubdomain_KeepsBackendPortResolution(t *testing.T) {
	const customPort int32 = 8000

	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	c := fakeClientWithServices(t, newServicePort("test-isvc-engine", "default", customPort))
	builder := createHTTPRouteBuilderWithClient(c)
	builder.ingressConfig.PerISVCSubdomain = true

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
	require.NoError(t, err)
	require.NotNil(t, result)
	route := result.(*gatewayapiv1.HTTPRoute)

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	assert.Equal(t, customPort, int32(*route.Spec.Rules[0].BackendRefs[0].Port),
		"per-ISVC scheme must keep resolving the backendRef port from the Service")
}

// Helper functions
// TestHTTPRouteBuilder_NoDefaultTimeout_OmitsRouteTimeout verifies the
// graceful-degradation contract: when IngressConfig.DefaultRouteTimeoutSeconds
// is unset (nil), OME imposes no route timeout (Timeouts left nil) so the
// gateway's own default governs — no hardcoded fallback.
func TestHTTPRouteBuilder_NoDefaultTimeout_OmitsRouteTimeout(t *testing.T) {
	builder := createHTTPRouteBuilder()
	builder.ingressConfig.DefaultRouteTimeoutSeconds = nil // operator did not configure one

	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
	require.NoError(t, err)
	httpRoute, ok := result.(*gatewayapiv1.HTTPRoute)
	require.True(t, ok)
	require.Len(t, httpRoute.Spec.Rules, 1)
	assert.Nil(t, httpRoute.Spec.Rules[0].Timeouts, "no default configured => no route timeout imposed by OME")
}

// TestHTTPRouteBuilder_ZeroDefaultTimeout_DisablesRouteTimeout verifies a
// configured value of 0 emits an explicit "0s", which Gateway API / Envoy treat
// as DISABLING the request timeout. Omitting the timeout instead would fall back
// to Envoy's built-in 15s route default (verified live), which truncates
// long-running inference — so 0 must produce "0s", not be dropped.
func TestHTTPRouteBuilder_ZeroDefaultTimeout_DisablesRouteTimeout(t *testing.T) {
	builder := createHTTPRouteBuilder()
	builder.ingressConfig.DefaultRouteTimeoutSeconds = ptr.To(int64(0))

	isvc := createTestInferenceServiceHTTPRoute("test-isvc", "default")
	setEngineReady(isvc)

	result, err := builder.BuildHTTPRoute(context.Background(), isvc, EngineComponent)
	require.NoError(t, err)
	httpRoute := result.(*gatewayapiv1.HTTPRoute)
	require.Len(t, httpRoute.Spec.Rules, 1)
	require.NotNil(t, httpRoute.Spec.Rules[0].Timeouts)
	require.NotNil(t, httpRoute.Spec.Rules[0].Timeouts.Request)
	assert.Equal(t, gatewayapiv1.Duration("0s"), *httpRoute.Spec.Rules[0].Timeouts.Request,
		"0 => explicit \"0s\" (Gateway API: disable request timeout)")
}

func createHTTPRouteBuilder() *HTTPRouteBuilder {
	return &HTTPRouteBuilder{
		ingressConfig: &controllerconfig.IngressConfig{
			IngressDomain:              "example.com",
			DomainTemplate:             "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			OmeIngressGateway:          "istio-system/gateway",
			SharedHostPrefix:           "llm",
			DefaultRouteTimeoutSeconds: ptr.To(int64(1800)),
		},
		isvcConfig:    &controllerconfig.InferenceServicesConfig{},
		domainService: services.NewDomainService(),
		pathService:   services.NewPathService(),
	}
}

func createHTTPRouteBuilderWithClient(c client.Client) *HTTPRouteBuilder {
	b := createHTTPRouteBuilder()
	b.client = c
	return b
}

// createHTTPRouteBuilderPerISVC is createHTTPRouteBuilder with the opt-in
// per-ISVC subdomain scheme enabled.
func createHTTPRouteBuilderPerISVC() *HTTPRouteBuilder {
	b := createHTTPRouteBuilder()
	b.ingressConfig.PerISVCSubdomain = true
	return b
}

// hasURLRewriteFilter reports whether the rule carries a urlRewrite filter.
func hasURLRewriteFilter(rule gatewayapiv1.HTTPRouteRule) bool {
	for _, f := range rule.Filters {
		if f.Type == gatewayapiv1.HTTPRouteFilterURLRewrite {
			return true
		}
	}
	return false
}

func newServicePort(name, namespace string, port int32) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       port,
				TargetPort: intstr.FromInt32(port),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
}

func fakeClientWithServices(t *testing.T, svcs ...*corev1.Service) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	objs := make([]client.Object, 0, len(svcs))
	for _, s := range svcs {
		objs = append(objs, s)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func createTestInferenceServiceHTTPRoute(name, namespace string) *v1beta1.InferenceService {
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
				Conditions: []apis.Condition{},
			},
		},
	}
}

func createTestInferenceServiceHTTPRouteWithTimeout(name, namespace string, timeoutSeconds int64) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceHTTPRoute(name, namespace)
	isvc.Spec.Engine.TimeoutSeconds = &timeoutSeconds
	return isvc
}

func createTestInferenceServiceWithRouterHTTPRoute(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceHTTPRoute(name, namespace)
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	return isvc
}

func createTestInferenceServiceWithRouterHTTPRouteTimeout(name, namespace string, timeoutSeconds int64) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceWithRouterHTTPRoute(name, namespace)
	isvc.Spec.Router.TimeoutSeconds = &timeoutSeconds
	return isvc
}

func createTestInferenceServiceWithDecoderHTTPRoute(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceHTTPRoute(name, namespace)
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

func createTestInferenceServiceWithDecoderHTTPRouteTimeout(name, namespace string, timeoutSeconds int64) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceWithDecoderHTTPRoute(name, namespace)
	isvc.Spec.Decoder.TimeoutSeconds = &timeoutSeconds
	return isvc
}

func createTestInferenceServiceWithRouterAndDecoderHTTPRoute(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceHTTPRoute(name, namespace)
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

func setEngineReady(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.EngineReady, &apis.Condition{
		Type:   v1beta1.EngineReady,
		Status: corev1.ConditionTrue,
	})
}

func setRouterReady(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.RouterReady, &apis.Condition{
		Type:   v1beta1.RouterReady,
		Status: corev1.ConditionTrue,
	})
}

func setDecoderReady(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.DecoderReady, &apis.Condition{
		Type:   v1beta1.DecoderReady,
		Status: corev1.ConditionTrue,
	})
}
