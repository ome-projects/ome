package strategies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/services"
)

func TestGatewayAPIStrategy_GetName(t *testing.T) {
	strategy := createGatewayAPIStrategy(t)
	assert.Equal(t, "GatewayAPI", strategy.GetName())
}

func TestGatewayAPIStrategy_Reconcile(t *testing.T) {
	tests := []struct {
		name                    string
		isvc                    *v1beta1.InferenceService
		ingressConfig           *controllerconfig.IngressConfig
		existingHTTPRoutes      []client.Object
		expectedError           bool
		expectedIngressReady    corev1.ConditionStatus
		expectedHTTPRoutesCount int
	}{
		{
			// Routes are created this pass but the gateway has not programmed
			// them yet (no parent status) — IngressReady must stay False, not
			// be overwritten to True.
			name: "fresh routes pending gateway programming",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionFalse,
			expectedHTTPRoutesCount: 2, // engine + toplevel
		},
		{
			name: "fresh routes with router pending gateway programming",
			isvc: createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionFalse,
			expectedHTTPRoutesCount: 3, // engine + router + toplevel
		},
		{
			name: "fresh routes with decoder pending gateway programming",
			isvc: createTestInferenceServiceWithDecoderGateway("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionFalse,
			expectedHTTPRoutesCount: 3, // engine + decoder + toplevel
		},
		{
			name: "routes already programmed by the gateway",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			existingHTTPRoutes: []client.Object{
				createReadyHTTPRoute("test-isvc-engine", "default"), // engine
				createReadyHTTPRoute("test-isvc", "default"),        // toplevel
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionTrue,
			expectedHTTPRoutesCount: 2,
		},
		{
			name: "ingress creation disabled",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: true,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionTrue,
			expectedHTTPRoutesCount: 0,
		},
		{
			name: "cluster local visibility pending gateway programming",
			isvc: createTestInferenceServiceWithClusterLocal("test-isvc", "default"),
			ingressConfig: &controllerconfig.IngressConfig{
				EnableGatewayAPI:       true,
				IngressDomain:          "example.com",
				OmeIngressGateway:      "istio-system/gateway",
				DomainTemplate:         "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
				UrlScheme:              "https",
				DisableIngressCreation: false,
			},
			expectedError:           false,
			expectedIngressReady:    corev1.ConditionFalse,
			expectedHTTPRoutesCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			// Set component statuses to ready
			setComponentStatusReadyGateway(tt.isvc)

			objs := []client.Object{tt.isvc}
			objs = append(objs, tt.existingHTTPRoutes...)

			// Status subresource so spec updates don't clobber seeded
			// route programming status (matches apiserver semantics).
			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				WithStatusSubresource(&gatewayapiv1.HTTPRoute{}).
				Build()

			// Use the proper constructor to create the strategy with builder
			opts := interfaces.ReconcilerOptions{
				Client:        fakeClient,
				Scheme:        scheme,
				IngressConfig: tt.ingressConfig,
				IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
			}
			strategy := NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService())

			err := strategy.Reconcile(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedIngressReady, tt.isvc.Status.GetCondition(v1beta1.IngressReady).Status)
			}
		})
	}
}

func TestGatewayAPIStrategy_Reconcile_GateOnTrafficIntent(t *testing.T) {
	// The default BackendTrafficPolicy created by this strategy and the
	// per-ISVC BTP created by the traffic.Reconciler write the same
	// resource name. To prevent the two loops from fighting, this
	// strategy must skip BTP create/check when the operator has declared
	// traffic intent (spec.traffic or any ome.io/* annotation).
	//
	// Cases:
	//   - no intent  -> default BTP created (preserves prior behavior)
	//   - intent set -> default BTP NOT created (traffic.Reconciler owns it)
	alg := v1beta1.LoadBalancingTypeRoundRobin

	tests := []struct {
		name             string
		mutateIsvc       func(isvc *v1beta1.InferenceService)
		expectBTPCreated bool
	}{
		{
			name:             "no traffic intent: default BTP is created",
			mutateIsvc:       func(_ *v1beta1.InferenceService) {},
			expectBTPCreated: true,
		},
		{
			name: "traffic intent via spec.traffic: default BTP skipped",
			mutateIsvc: func(isvc *v1beta1.InferenceService) {
				isvc.Spec.Traffic = &v1beta1.TrafficSpec{Algorithm: &alg}
			},
			expectBTPCreated: false,
		},
		{
			name: "traffic intent via ome.io annotation: default BTP skipped",
			mutateIsvc: func(isvc *v1beta1.InferenceService) {
				if isvc.Annotations == nil {
					isvc.Annotations = map[string]string{}
				}
				isvc.Annotations["ome.io/circuit-breaker-max-connections"] = "1024"
			},
			expectBTPCreated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			isvc := createTestInferenceServiceGateway("test-isvc", "default")
			setComponentStatusReadyGateway(isvc)
			tt.mutateIsvc(isvc)

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(isvc).
				Build()

			opts := interfaces.ReconcilerOptions{
				Client: fakeClient,
				Scheme: scheme,
				IngressConfig: &controllerconfig.IngressConfig{
					EnableGatewayAPI:      true,
					IngressDomain:         "example.com",
					OmeIngressGateway:     "istio-system/gateway",
					DomainTemplate:        "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
					UrlScheme:             "https",
					ConsistentHashHeaders: []string{"x-routing-key"},
				},
				IsvcConfig: &controllerconfig.InferenceServicesConfig{},
			}
			strategy := NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService())

			require.NoError(t, strategy.Reconcile(context.Background(), isvc))

			policy := &unstructured.Unstructured{}
			policy.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
			err := fakeClient.Get(context.Background(), types.NamespacedName{
				Name: isvc.Name, Namespace: isvc.Namespace,
			}, policy)

			if tt.expectBTPCreated {
				require.NoError(t, err, "expected default BTP to be created")
				// The created route BTP must merge into the Gateway-level parent
				// so it inherits its settings (e.g. circuitBreaker, timeout).
				spec, ok := policy.Object["spec"].(map[string]interface{})
				require.True(t, ok)
				assert.Equal(t, "StrategicMerge", spec["mergeType"])
			} else {
				assert.True(t, apierr.IsNotFound(err),
					"expected default BTP NOT to be created (traffic.Reconciler owns it); got err=%v", err)
			}
		})
	}
}

func TestGatewayAPIStrategy_ReconcileComponentHTTPRoute(t *testing.T) {
	tests := []struct {
		name           string
		isvc           *v1beta1.InferenceService
		componentType  string
		existingRoute  *gatewayapiv1.HTTPRoute
		expectedError  bool
		expectedCreate bool
		expectedUpdate bool
	}{
		{
			name:           "create new engine HTTPRoute",
			isvc:           createTestInferenceServiceGateway("test-isvc", "default"),
			componentType:  "engine",
			expectedError:  false,
			expectedCreate: true,
		},
		{
			name:           "create new router HTTPRoute",
			isvc:           createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			componentType:  "router",
			expectedError:  false,
			expectedCreate: true,
		},
		{
			name:           "create new decoder HTTPRoute",
			isvc:           createTestInferenceServiceWithDecoderGateway("test-isvc", "default"),
			componentType:  "decoder",
			expectedError:  false,
			expectedCreate: true,
		},
		{
			name:          "update existing HTTPRoute",
			isvc:          createTestInferenceServiceGateway("test-isvc", "default"),
			componentType: "engine",
			existingRoute: &gatewayapiv1.HTTPRoute{
				// Engine routes are named "<isvc>-engine"; the name must
				// match for the update path to be exercised.
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc-engine",
					Namespace: "default",
				},
			},
			expectedError:  false,
			expectedUpdate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			setComponentStatusReadyGateway(tt.isvc)

			objs := []client.Object{tt.isvc}
			if tt.existingRoute != nil {
				objs = append(objs, tt.existingRoute)
			}

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

			ready, err := strategy.(*GatewayAPIStrategy).reconcileComponentHTTPRoute(context.Background(), tt.isvc, tt.componentType)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.expectedCreate {
					assert.False(t, ready, "a just-created route has no verified gateway status and must not count as ready")
					cond := tt.isvc.Status.GetCondition(v1beta1.IngressReady)
					require.NotNil(t, cond)
					assert.Equal(t, corev1.ConditionFalse, cond.Status)
				} else {
					assert.True(t, ready, "an existing route defers readiness to the status check")
				}
			}
		})
	}
}

// typedNilHTTPRouteBuilder is a stub HTTPRouteBuilder that always returns
// a (*gatewayapiv1.HTTPRoute)(nil) wrapped in a non-nil client.Object — a
// shape a bare `== nil` check does not catch. The strategy must defend
// against it even if a dispatcher regression reintroduces it.
type typedNilHTTPRouteBuilder struct{}

func (typedNilHTTPRouteBuilder) GetResourceType() string { return "HTTPRoute" }

func (typedNilHTTPRouteBuilder) Build(_ context.Context, _ *v1beta1.InferenceService) (client.Object, error) {
	return (*gatewayapiv1.HTTPRoute)(nil), nil
}

func (typedNilHTTPRouteBuilder) BuildHTTPRoute(_ context.Context, _ *v1beta1.InferenceService, _ string) (client.Object, error) {
	// Wrap a typed-nil *HTTPRoute in the interface return. `result == nil`
	// returns FALSE for this value because the interface header carries
	// non-nil type info; only the inner value is nil. The strategy's
	// typed-nil guard must catch this.
	return (*gatewayapiv1.HTTPRoute)(nil), nil
}

func (typedNilHTTPRouteBuilder) Endpoints(_ *v1beta1.InferenceService, _ string) ([]interfaces.Endpoint, error) {
	return nil, nil
}

// TestGatewayAPIStrategy_ReconcileComponentHTTPRoute_TypedNilBuilderDoesNotPanic
// pins the strategy-side defense against a typed-nil-through-interface
// panic.
//
// A typed-nil *HTTPRoute wrapped in a non-nil client.Object interface
// defeats the `desired == nil` check at the top of
// reconcileComponentHTTPRoute (the interface is non-nil), the type
// assertion succeeds with ok=true, and the subsequent
// controllerutil.SetControllerReference call dereferences the typed-nil
// pointer.
//
// The BuildHTTPRoute dispatcher returns interface-nil for the not-ready
// branch, which closes the usual source. This test exercises the second
// layer — a defensive nil check inside the strategy that catches any
// caller (test stub, dispatcher regression, third-party builder) that
// still hands the strategy a typed-nil-wrapped-in-interface. The stub
// builder is injected directly into the strategy struct, bypassing the
// dispatcher, so this test pins the defense regardless of any change to
// BuildHTTPRoute.
func TestGatewayAPIStrategy_ReconcileComponentHTTPRoute_TypedNilBuilderDoesNotPanic(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	isvc := createTestInferenceServiceWithDecoderGateway("test-isvc", "default")
	// Deliberately do NOT call setComponentStatusReadyGateway — even though
	// our stub builder ignores readiness anyway, leaving it not-ready mirrors
	// the production trigger (decoder component not yet ready).

	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(isvc).Build()

	opts := interfaces.ReconcilerOptions{
		Client:        fakeClient,
		Scheme:        scheme,
		IngressConfig: &controllerconfig.IngressConfig{EnableGatewayAPI: true},
		IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
	}
	strategy := NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService()).(*GatewayAPIStrategy)

	// Swap in the stub builder. This bypasses the layer-1 dispatcher fix so
	// the strategy sees the bad shape directly and we exercise the layer-2
	// defense in isolation.
	strategy.httpRouteBuilder = typedNilHTTPRouteBuilder{}

	// Without the guard this panics at SetControllerReference. Wrap in a
	// recover() check to give a precise diagnostic if the defense regresses.
	var err error
	var ready bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reconcileComponentHTTPRoute panicked on a typed-nil HTTPRoute (this is the bug we are guarding against): %v", r)
			}
		}()
		ready, err = strategy.reconcileComponentHTTPRoute(context.Background(), isvc, builders.DecoderComponent)
	}()

	require.NoError(t, err, "strategy must return nil error (not a panic and not a real failure) when builder hands it a typed-nil HTTPRoute")
	assert.False(t, ready, "a typed-nil route means the component is not ready")

	cond := isvc.Status.GetCondition(v1beta1.IngressReady)
	require.NotNil(t, cond, "IngressReady condition must be set by the typed-nil guard")
	assert.Equal(t, corev1.ConditionFalse, cond.Status, "IngressReady must be False when component is not ready")
	assert.Equal(t, "ComponentNotReady", cond.Reason, "IngressReady reason must identify the not-ready component cause")
}

func TestGatewayAPIStrategy_CheckHTTPRouteStatuses(t *testing.T) {
	tests := []struct {
		name              string
		isvc              *v1beta1.InferenceService
		httpRoutes        []client.Object
		expectedError     bool
		expectedCondition corev1.ConditionStatus
	}{
		{
			// engine HTTPRoute is "<isvc>-engine"; top-level is "<isvc>".
			// Both lookups must succeed.
			name: "all HTTPRoutes ready",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			httpRoutes: []client.Object{
				createReadyHTTPRoute("test-isvc-engine", "default"), // engine
				createReadyHTTPRoute("test-isvc", "default"),        // toplevel
			},
			expectedError:     false,
			expectedCondition: corev1.ConditionTrue,
		},
		{
			name: "HTTPRoute not ready",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			httpRoutes: []client.Object{
				createNotReadyHTTPRoute("test-isvc-engine", "default"), // engine
				createReadyHTTPRoute("test-isvc", "default"),           // toplevel
			},
			expectedError:     false,
			expectedCondition: corev1.ConditionFalse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))

			objs := []client.Object{tt.isvc}
			objs = append(objs, tt.httpRoutes...)

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

			ready, err := strategy.(*GatewayAPIStrategy).checkHTTPRouteStatuses(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedCondition == corev1.ConditionTrue, ready)
				if tt.expectedCondition == corev1.ConditionFalse {
					condition := tt.isvc.Status.GetCondition(v1beta1.IngressReady)
					require.NotNil(t, condition)
					assert.Equal(t, corev1.ConditionFalse, condition.Status)
				}
			}
		})
	}
}

// TestGatewayAPIStrategy_CheckHTTPRouteStatuses_NotFoundIsTolerated pins
// that checkHTTPRouteStatuses treats IsNotFound as a tolerable absence,
// not a hard error. reconcileComponentHTTPRoute skips create when a
// component isn't Ready (Builder returns nil), so during PD-disaggregated
// bring-up some routes do not exist yet; a hard error there would crash
// the reconcile every loop. The status-check must continue past the
// missing routes rather than failing.
func TestGatewayAPIStrategy_CheckHTTPRouteStatuses_NotFoundIsTolerated(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	// ISVC declares Engine + Router + Decoder so checkHTTPRouteStatuses
	// will iterate all four name slots ("<isvc>-engine", "<isvc>-router",
	// "<isvc>-decoder", "<isvc>"). None of them exist in the client.
	isvc := createTestInferenceServiceWithRouterGateway("test-isvc", "default")
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}

	// Pre-seed an IngressReady condition the way reconcileComponentHTTPRoute
	// would when it skipped a component (False / ComponentNotReady). The
	// status-check must not corrupt this.
	isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
		Type:    v1beta1.IngressReady,
		Status:  corev1.ConditionFalse,
		Reason:  "ComponentNotReady",
		Message: "engine component not ready for HTTPRoute creation",
	})

	// Fake client with NO HTTPRoutes pre-created.
	fakeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		Build()

	strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

	ready, err := strategy.(*GatewayAPIStrategy).checkHTTPRouteStatuses(context.Background(), isvc)
	assert.NoError(t, err, "checkHTTPRouteStatuses must tolerate NotFound and not return an error")
	assert.True(t, ready, "absent routes are the create-path's signal, not the status-check's")

	// The pre-seeded condition from the create-path is preserved
	// (status-check did not touch it).
	condition := isvc.Status.GetCondition(v1beta1.IngressReady)
	require.NotNil(t, condition)
	assert.Equal(t, corev1.ConditionFalse, condition.Status,
		"pre-existing IngressReady=False set by reconcileComponentHTTPRoute must be preserved")
	assert.Equal(t, "ComponentNotReady", condition.Reason)
}

// TestGatewayAPIStrategy_Reconcile_CreatePassInformerLagNotReady pins the
// create-pass informer-lag race: routes Created this pass are typically not
// visible to the cache-backed client yet, so checkHTTPRouteStatuses sees
// NotFound for them. That NotFound must not let IngressReady conclude True
// on the very pass that created un-programmed routes — readiness is decided
// on a later pass, once the watch observes the route and its real status.
func TestGatewayAPIStrategy_Reconcile_CreatePassInformerLagNotReady(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	isvc := createTestInferenceServiceGateway("test-isvc", "default")
	setComponentStatusReadyGateway(isvc)

	// Simulate informer lag: HTTPRoutes created during this pass stay
	// invisible to subsequent Gets, exactly like a cache that has not
	// caught up with the apiserver.
	createdRoutes := map[string]bool{}
	fakeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		WithStatusSubresource(&gatewayapiv1.HTTPRoute{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*gatewayapiv1.HTTPRoute); ok {
					createdRoutes[obj.GetName()] = true
				}
				return c.Create(ctx, obj, opts...)
			},
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*gatewayapiv1.HTTPRoute); ok && createdRoutes[key.Name] {
					return apierr.NewNotFound(gatewayapiv1.Resource("httproutes"), key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	strategy := createGatewayAPIStrategyWithClient(t, fakeClient)
	require.NoError(t, strategy.Reconcile(context.Background(), isvc))

	// Routes were created on the server ...
	routes := &gatewayapiv1.HTTPRouteList{}
	require.NoError(t, fakeClient.List(context.Background(), routes, client.InNamespace("default")))
	assert.Len(t, routes.Items, 2, "engine + toplevel routes are created on the server")

	// ... but IngressReady must not be True until they are observed with
	// real gateway status.
	cond := isvc.Status.GetCondition(v1beta1.IngressReady)
	require.NotNil(t, cond)
	assert.Equal(t, corev1.ConditionFalse, cond.Status,
		"a route created this pass must not count toward IngressReady=True")
	assert.Equal(t, HTTPRouteParentStatusNotAvailable, cond.Reason)
}

// TestGatewayAPIStrategy_Reconcile_AlreadyExistsCacheLagNotReady pins the
// sibling lag shape: the route already exists on the server while the
// cache still reports NotFound, so the strategy's Create races into
// AlreadyExists. That is a routine cache-lag outcome, not a failure — the
// pass must report not-ready (no error) and let the watch-driven requeue
// re-check once the cache observes the route.
func TestGatewayAPIStrategy_Reconcile_AlreadyExistsCacheLagNotReady(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	isvc := createTestInferenceServiceGateway("test-isvc", "default")
	setComponentStatusReadyGateway(isvc)

	// Routes exist server-side (seeded), but every HTTPRoute Get returns
	// NotFound — a cache that lags a Create from a previous pass.
	fakeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc,
			createReadyHTTPRoute("test-isvc-engine", "default"),
			createReadyHTTPRoute("test-isvc", "default")).
		WithStatusSubresource(&gatewayapiv1.HTTPRoute{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*gatewayapiv1.HTTPRoute); ok {
					return apierr.NewNotFound(gatewayapiv1.Resource("httproutes"), key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	strategy := createGatewayAPIStrategyWithClient(t, fakeClient)
	require.NoError(t, strategy.Reconcile(context.Background(), isvc),
		"AlreadyExists from a cache-lagged Create is not an error")

	cond := isvc.Status.GetCondition(v1beta1.IngressReady)
	require.NotNil(t, cond)
	assert.Equal(t, corev1.ConditionFalse, cond.Status,
		"an unobserved route must not count toward IngressReady=True")
}

// gatewayStrategyWithConfig builds a GatewayAPIStrategy over an explicit
// IngressConfig so each status case pins the exact routing knobs.
func gatewayStrategyWithConfig(t *testing.T, cfg *controllerconfig.IngressConfig) *GatewayAPIStrategy {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	opts := interfaces.ReconcilerOptions{
		Client:        fakeClient,
		Scheme:        scheme,
		IngressConfig: cfg,
		IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
	}
	return NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService()).(*GatewayAPIStrategy)
}

func addressName(a duckv1.Addressable) string {
	if a.Name == nil {
		return ""
	}
	return *a.Name
}

// TestGatewayAPIStrategy_setStatusEndpoints is the status-endpoints contract:
// status.addresses carries one entry per gateway (tagged with the declared
// class) plus the cluster-local address, status.url projects the primary gateway
// endpoint, and status.address projects the cluster-local one — all derived from
// the same builder as the HTTPRoutes.
func TestGatewayAPIStrategy_setStatusEndpoints(t *testing.T) {
	base := func() *controllerconfig.IngressConfig {
		return &controllerconfig.IngressConfig{
			EnableGatewayAPI:  true,
			IngressDomain:     "int-gw-https.cluster.example",
			DomainTemplate:    "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			OmeIngressGateway: "istio-system/int-gw",
			UrlScheme:         "https",
		}
	}

	t.Run("subdomain primary+external is discoverable and url is primary", func(t *testing.T) {
		cfg := base()
		cfg.PerISVCSubdomain = true
		cfg.OmeIngressGatewayClass = "internal"
		cfg.AdditionalIngressGateways = []controllerconfig.IngressGatewaySpec{{
			OmeIngressGateway: "istio-system/ext-gw",
			IngressDomain:     "ext-gw-https.cluster.example",
			Class:             "external",
		}}
		strategy := gatewayStrategyWithConfig(t, cfg)
		isvc := createTestInferenceServiceGateway("svc", "ns")

		require.NoError(t, strategy.setStatusEndpoints(isvc))

		require.Len(t, isvc.Status.Addresses, 3)
		assert.Equal(t, "internal", addressName(isvc.Status.Addresses[0]))
		assert.Equal(t, "https://svc.ns.int-gw-https.cluster.example/", isvc.Status.Addresses[0].URL.String())
		assert.Equal(t, "external", addressName(isvc.Status.Addresses[1]))
		assert.Equal(t, "https://svc.ns.ext-gw-https.cluster.example/", isvc.Status.Addresses[1].URL.String())
		assert.Equal(t, interfaces.ClusterLocalEndpointClass, addressName(isvc.Status.Addresses[2]))
		assert.Equal(t, "https://svc-engine.ns.svc.cluster.local", isvc.Status.Addresses[2].URL.String())

		// status.url projects the primary gateway; status.address the cluster-local.
		assert.Equal(t, "https://svc.ns.int-gw-https.cluster.example/", isvc.Status.URL.String())
		require.NotNil(t, isvc.Status.Address)
		assert.Equal(t, "https://svc-engine.ns.svc.cluster.local", isvc.Status.Address.URL.String())
	})

	t.Run("shared-host url carries the load-bearing path prefix", func(t *testing.T) {
		cfg := base()
		cfg.PerISVCSubdomain = false
		cfg.SharedHostPrefix = "llm"
		strategy := gatewayStrategyWithConfig(t, cfg)
		isvc := createTestInferenceServiceGateway("svc", "ns")

		require.NoError(t, strategy.setStatusEndpoints(isvc))

		require.Len(t, isvc.Status.Addresses, 2)
		assert.Equal(t, "", addressName(isvc.Status.Addresses[0]))
		assert.Equal(t, "https://llm.int-gw-https.cluster.example/ns/svc/", isvc.Status.Addresses[0].URL.String())
		assert.Equal(t, interfaces.ClusterLocalEndpointClass, addressName(isvc.Status.Addresses[1]))

		// The corrected status.url — the shared host WITH the /ns/svc/ prefix,
		// not the old non-routable per-ISVC subdomain.
		assert.Equal(t, "https://llm.int-gw-https.cluster.example/ns/svc/", isvc.Status.URL.String())
	})
}

func TestGatewayAPIStrategy_GetRawServiceHost(t *testing.T) {
	tests := []struct {
		name         string
		isvc         *v1beta1.InferenceService
		expectedHost string
	}{
		{
			name:         "with router",
			isvc:         createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			expectedHost: "test-isvc-router.default.svc.cluster.local",
		},
		{
			// Engine Service is named "<isvc>-engine" (constants.EngineServiceName);
			// "<isvc>" resolves to no Service.
			name:         "without router",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedHost: "test-isvc-engine.default.svc.cluster.local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createGatewayAPIStrategy(t)
			host := strategy.(*GatewayAPIStrategy).getRawServiceHost(tt.isvc)
			assert.Equal(t, tt.expectedHost, host)
		})
	}
}

func TestGatewayAPIStrategy_IsHTTPRouteReady(t *testing.T) {
	tests := []struct {
		name            string
		status          gatewayapiv1.HTTPRouteStatus
		expectedReady   bool
		expectedReason  *string
		expectedMessage *string
	}{
		{
			name:          "empty status",
			status:        gatewayapiv1.HTTPRouteStatus{},
			expectedReady: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createGatewayAPIStrategy(t)
			ready, _, _ := strategy.(*GatewayAPIStrategy).isHTTPRouteReady(tt.status)
			assert.Equal(t, tt.expectedReady, ready)
		})
	}
}

func TestGatewayAPIStrategy_GetComponentType(t *testing.T) {
	tests := []struct {
		name         string
		serviceName  string
		isvc         *v1beta1.InferenceService
		expectedType string
	}{
		{
			// engine HTTPRoute is "<isvc>-engine", distinct from the
			// top-level "<isvc>".
			name:         "engine component",
			serviceName:  "test-isvc-engine",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedType: "Engine",
		},
		{
			name:         "router component",
			serviceName:  "test-isvc-router",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedType: "Router",
		},
		{
			name:         "decoder component",
			serviceName:  "test-isvc-decoder",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedType: "Decoder",
		},
		{
			// Top-level HTTPRoute is named "<isvc>"; distinct from the
			// engine route.
			name:         "toplevel component",
			serviceName:  "test-isvc",
			isvc:         createTestInferenceServiceGateway("test-isvc", "default"),
			expectedType: "TopLevel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createGatewayAPIStrategy(t)
			componentType := strategy.(*GatewayAPIStrategy).getComponentType(tt.serviceName, tt.isvc)
			assert.Equal(t, tt.expectedType, componentType)
		})
	}
}

// Helper functions
func createGatewayAPIStrategy(t *testing.T) interfaces.IngressStrategy {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).Build()

	opts := interfaces.ReconcilerOptions{
		Client: fakeClient,
		Scheme: scheme,
		IngressConfig: &controllerconfig.IngressConfig{
			EnableGatewayAPI:  true,
			IngressDomain:     "example.com",
			OmeIngressGateway: "istio-system/gateway",
			DomainTemplate:    "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			UrlScheme:         "https",
		},
		IsvcConfig: &controllerconfig.InferenceServicesConfig{},
	}

	return NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService())
}

func createGatewayAPIStrategyWithClient(t *testing.T, client client.Client) interfaces.IngressStrategy {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	opts := interfaces.ReconcilerOptions{
		Client: client,
		Scheme: scheme,
		IngressConfig: &controllerconfig.IngressConfig{
			EnableGatewayAPI:  true,
			IngressDomain:     "example.com",
			OmeIngressGateway: "istio-system/gateway",
			DomainTemplate:    "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
			UrlScheme:         "https",
		},
		IsvcConfig: &controllerconfig.InferenceServicesConfig{},
	}

	return NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService())
}

func createTestInferenceServiceGateway(name, namespace string) *v1beta1.InferenceService {
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

func createTestInferenceServiceWithRouterGateway(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceGateway(name, namespace)
	isvc.Spec.Router = &v1beta1.RouterSpec{}
	return isvc
}

func createTestInferenceServiceWithDecoderGateway(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceGateway(name, namespace)
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	return isvc
}

func createTestInferenceServiceWithClusterLocal(name, namespace string) *v1beta1.InferenceService {
	isvc := createTestInferenceServiceGateway(name, namespace)
	isvc.Labels = map[string]string{
		constants.VisibilityLabel: constants.ClusterLocalVisibility,
	}
	return isvc
}

func setComponentStatusReadyGateway(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.EngineReady, &apis.Condition{
		Type:   v1beta1.EngineReady,
		Status: corev1.ConditionTrue,
	})

	if isvc.Spec.Router != nil {
		isvc.Status.SetCondition(v1beta1.RouterReady, &apis.Condition{
			Type:   v1beta1.RouterReady,
			Status: corev1.ConditionTrue,
		})
	}

	if isvc.Spec.Decoder != nil {
		isvc.Status.SetCondition(v1beta1.DecoderReady, &apis.Condition{
			Type:   v1beta1.DecoderReady,
			Status: corev1.ConditionTrue,
		})
	}
}

func createReadyHTTPRoute(name, namespace string) *gatewayapiv1.HTTPRoute {
	return &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: gatewayapiv1.HTTPRouteStatus{
			RouteStatus: gatewayapiv1.RouteStatus{
				Parents: []gatewayapiv1.RouteParentStatus{{
					ParentRef:      gatewayapiv1.ParentReference{Name: "gateway"},
					ControllerName: "test.controller/gateway",
					Conditions: []metav1.Condition{{
						Type:               string(gatewayapiv1.RouteConditionAccepted),
						Status:             metav1.ConditionTrue,
						Reason:             string(gatewayapiv1.RouteReasonAccepted),
						LastTransitionTime: metav1.Now(),
					}},
				}},
			},
		},
	}
}

func createNotReadyHTTPRoute(name, namespace string) *gatewayapiv1.HTTPRoute {
	return &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: gatewayapiv1.HTTPRouteStatus{
			RouteStatus: gatewayapiv1.RouteStatus{
				Parents: []gatewayapiv1.RouteParentStatus{{
					ParentRef:      gatewayapiv1.ParentReference{Name: "gateway"},
					ControllerName: "test.controller/gateway",
					Conditions: []metav1.Condition{{
						Type:               string(gatewayapiv1.RouteConditionAccepted),
						Status:             metav1.ConditionFalse,
						Reason:             string(gatewayapiv1.RouteReasonNotAllowedByListeners),
						Message:            "route not accepted",
						LastTransitionTime: metav1.Now(),
					}},
				}},
			},
		},
	}
}

func TestGatewayAPIStrategy_ReconcileBackendTrafficPolicy(t *testing.T) {
	tests := []struct {
		name           string
		isvc           *v1beta1.InferenceService
		existingPolicy bool
		expectedError  bool
	}{
		{
			name:          "create new policy",
			isvc:          createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			expectedError: false,
		},
		{
			name:           "update existing policy",
			isvc:           createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			existingPolicy: true,
			expectedError:  false,
		},
		{
			name:          "create policy for non-PD",
			isvc:          createTestInferenceServiceGateway("test-isvc", "default"),
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			setComponentStatusReadyGateway(tt.isvc)

			objs := []client.Object{tt.isvc}
			if tt.existingPolicy {
				existing := &unstructured.Unstructured{}
				existing.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
				existing.SetName(tt.isvc.Name)
				existing.SetNamespace(tt.isvc.Namespace)
				existing.Object["spec"] = map[string]interface{}{}
				objs = append(objs, existing)
			}

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

			err := strategy.(*GatewayAPIStrategy).reconcileBackendTrafficPolicy(context.Background(), tt.isvc)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Verify the policy was created/updated
				policy := &unstructured.Unstructured{}
				policy.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
				err := fakeClient.Get(context.Background(), types.NamespacedName{
					Name:      tt.isvc.Name,
					Namespace: tt.isvc.Namespace,
				}, policy)
				assert.NoError(t, err)
				assert.Equal(t, tt.isvc.Name, policy.GetName())
			}
		})
	}
}

func TestGatewayAPIStrategy_CheckBackendTrafficPolicyStatus(t *testing.T) {
	tests := []struct {
		name              string
		isvc              *v1beta1.InferenceService
		policy            *unstructured.Unstructured
		expectedCondition corev1.ConditionStatus
	}{
		{
			name: "policy not found - no error",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
		},
		{
			name: "policy accepted",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			policy: func() *unstructured.Unstructured {
				p := &unstructured.Unstructured{}
				p.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
				p.SetName("test-isvc")
				p.SetNamespace("default")
				p.Object["status"] = map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Accepted",
							"status": "True",
							"reason": "Accepted",
						},
					},
				}
				return p
			}(),
			expectedCondition: corev1.ConditionTrue,
		},
		{
			name: "policy rejected",
			isvc: createTestInferenceServiceGateway("test-isvc", "default"),
			policy: func() *unstructured.Unstructured {
				p := &unstructured.Unstructured{}
				p.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
				p.SetName("test-isvc")
				p.SetNamespace("default")
				p.Object["status"] = map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "Accepted",
							"status":  "False",
							"reason":  "Invalid",
							"message": "target not found",
						},
					},
				}
				return p
			}(),
			expectedCondition: corev1.ConditionFalse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			setComponentStatusReadyGateway(tt.isvc)

			objs := []client.Object{tt.isvc}
			if tt.policy != nil {
				objs = append(objs, tt.policy)
			}

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

			ready, err := strategy.(*GatewayAPIStrategy).checkBackendTrafficPolicyStatus(context.Background(), tt.isvc)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedCondition != corev1.ConditionFalse, ready)

			if tt.expectedCondition == corev1.ConditionFalse {
				condition := tt.isvc.Status.GetCondition(v1beta1.IngressReady)
				require.NotNil(t, condition)
				assert.Equal(t, corev1.ConditionFalse, condition.Status)
				assert.Contains(t, condition.Message, "BackendTrafficPolicy")
			}
		})
	}
}
