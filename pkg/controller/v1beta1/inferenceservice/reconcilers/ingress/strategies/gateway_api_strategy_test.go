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
			makeHTTPRouteSpecsCurrent(t, tt.isvc, tt.ingressConfig, tt.existingHTTPRoutes)

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

func TestGatewayAPIStrategy_Reconcile_AcceptedRoutesDuringScaleUp(t *testing.T) {
	for _, tt := range []struct {
		name          string
		readyReplicas int32
		serving       int32
		want          corev1.ConditionStatus
	}{
		{name: "serving capacity remains", readyReplicas: 4, serving: 4, want: corev1.ConditionTrue},
		{name: "ready replicas are out of rotation", readyReplicas: 4, serving: 0, want: corev1.ConditionFalse},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v1beta1.AddToScheme(scheme))
			require.NoError(t, gatewayapiv1.Install(scheme))
			require.NoError(t, corev1.AddToScheme(scheme))

			isvc := createTestInferenceServiceWithRouterGateway("test-isvc", "default")
			isvc.Generation = 2
			setComponentStatusReadyGateway(isvc)
			existingHTTPRoutes := []client.Object{
				createReadyHTTPRoute("test-isvc-engine", "default"),
				createReadyHTTPRoute("test-isvc-router", "default"),
				createReadyHTTPRoute("test-isvc", "default"),
			}
			makeHTTPRouteSpecsCurrent(t, isvc, gatewayTestIngressConfig(), existingHTTPRoutes)
			isvc.Status.SetCondition(v1beta1.EngineReady, &apis.Condition{
				Type:    v1beta1.EngineReady,
				Status:  corev1.ConditionFalse,
				Reason:  "InsufficientAvailable",
				Message: "component is converging to its desired replica count",
			})
			isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
				Type:   v1beta1.IngressReady,
				Status: corev1.ConditionTrue,
			})
			isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					ScaleTargetRef: &v1beta1.ScaleTargetRef{
						APIVersion: v1beta1.SchemeGroupVersion.String(),
						Kind:       "InferenceReplica",
						Name:       "test-isvc-engine",
					},
					Lifecycle: &v1beta1.LifecycleStatus{
						ObservedGeneration: 1,
						Replicas:           10,
						ReadyReplicas:      tt.readyReplicas,
						ServingReplicas:    tt.serving,
					},
				},
			}

			fakeClient := fakeclient.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(append([]client.Object{isvc}, existingHTTPRoutes...)...).
				WithStatusSubresource(&gatewayapiv1.HTTPRoute{}).
				Build()
			strategy := createGatewayAPIStrategyWithClient(t, fakeClient)

			require.NoError(t, strategy.Reconcile(context.Background(), isvc))
			condition := isvc.Status.GetCondition(v1beta1.IngressReady)
			require.NotNil(t, condition)
			assert.Equal(t, tt.want, condition.Status)
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
				} else if tt.expectedUpdate {
					assert.False(t, ready, "an updated route must await status for its new generation")
					cond := tt.isvc.Status.GetCondition(v1beta1.IngressReady)
					require.NotNil(t, cond)
					assert.Equal(t, corev1.ConditionFalse, cond.Status)
				} else {
					assert.True(t, ready)
				}
			}
		})
	}
}

// typedNilHTTPRouteBuilder is a stub HTTPRouteBuilder that always returns
// a (*gatewayapiv1.HTTPRoute)(nil) wrapped in a non-nil client.Object, a
// shape a bare `== nil` check does not catch.
type typedNilHTTPRouteBuilder struct{}

func (typedNilHTTPRouteBuilder) BuildHTTPRoute(_ context.Context, _ *v1beta1.InferenceService, _ string) (client.Object, error) {
	return (*gatewayapiv1.HTTPRoute)(nil), nil
}

func (typedNilHTTPRouteBuilder) Endpoints(_ *v1beta1.InferenceService, _ string) ([]interfaces.Endpoint, error) {
	return nil, nil
}

// A builder that hands back a typed-nil *HTTPRoute inside a non-nil
// client.Object, with no error, broke its contract: the strategy refuses it
// with an error instead of dereferencing the nil route, and writes no
// condition it cannot explain. The stub is injected into the strategy struct,
// so the production builder's own answer is not in play.
func TestGatewayAPIStrategy_ReconcileComponentHTTPRoute_TypedNilBuilderDoesNotPanic(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	isvc := createTestInferenceServiceWithDecoderGateway("test-isvc", "default")

	fakeClient := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(isvc).Build()

	opts := interfaces.ReconcilerOptions{
		Client:        fakeClient,
		Scheme:        scheme,
		IngressConfig: &controllerconfig.IngressConfig{EnableGatewayAPI: true},
		IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
	}
	strategy := NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService()).(*GatewayAPIStrategy)

	strategy.httpRouteBuilder = typedNilHTTPRouteBuilder{}

	var err error
	var ready bool
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reconcileComponentHTTPRoute panicked on a typed-nil HTTPRoute: %v", r)
			}
		}()
		ready, err = strategy.reconcileComponentHTTPRoute(context.Background(), isvc, builders.DecoderComponent)
	}()

	require.Error(t, err, "a typed-nil route with no error is a broken builder contract")
	assert.False(t, ready)
	assert.Nil(t, isvc.Status.GetCondition(v1beta1.IngressReady), "the strategy writes no condition for a builder answer it cannot explain")
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
// component isn't Ready (the builder answers not ready), so during PD-disaggregated
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

// TestGatewayAPIStrategy_RoutesCreatedInOnePass_TopLevelRouteFirst checks the
// creation order when every component is Ready before any route exists. Under
// per-ISVC subdomains the four routes match the same host at "/", and Gateway
// API gives that tie to the oldest route, so the top-level route comes first.
func TestGatewayAPIStrategy_RoutesCreatedInOnePass_TopLevelRouteFirst(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	isvc := createTestInferenceServiceWithRouterGateway("test-isvc", "default")
	isvc.Spec.Decoder = &v1beta1.DecoderSpec{}
	setComponentStatusReadyGateway(isvc)

	var created []string
	fakeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(isvc).
		WithStatusSubresource(&gatewayapiv1.HTTPRoute{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*gatewayapiv1.HTTPRoute); ok {
					created = append(created, obj.GetName())
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	ingressConfig := gatewayTestIngressConfig()
	ingressConfig.PerISVCSubdomain = true
	strategy := NewGatewayAPIStrategy(interfaces.ReconcilerOptions{
		Client:        fakeClient,
		Scheme:        scheme,
		IngressConfig: ingressConfig,
		IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
	}, services.NewDomainService(), services.NewPathService())

	require.NoError(t, strategy.Reconcile(context.Background(), isvc))

	assert.Equal(t, []string{"test-isvc", "test-isvc-engine", "test-isvc-router", "test-isvc-decoder"}, created)
	routes := &gatewayapiv1.HTTPRouteList{}
	require.NoError(t, fakeClient.List(context.Background(), routes, client.InNamespace("default")))
	require.Len(t, routes.Items, 4)
	for _, route := range routes.Items {
		assert.Equal(t, []gatewayapiv1.Hostname{"test-isvc.default.example.com"}, route.Spec.Hostnames, route.Name)
		require.NotEmpty(t, route.Spec.Rules, route.Name)
		assert.Equal(t, "/", *route.Spec.Rules[0].Matches[0].Path.Value, route.Name)
	}
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
			ClusterDomain:     "cluster.local",
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
		name          string
		isvc          *v1beta1.InferenceService
		clusterDomain string
		expectedHost  string
	}{
		{
			name:          "with router",
			isvc:          createTestInferenceServiceWithRouterGateway("test-isvc", "default"),
			clusterDomain: "cluster.local",
			expectedHost:  "test-isvc-router.default.svc.cluster.local",
		},
		{
			// Engine Service is named "<isvc>-engine" (constants.EngineServiceName);
			// "<isvc>" resolves to no Service.
			name:          "without router",
			isvc:          createTestInferenceServiceGateway("test-isvc", "default"),
			clusterDomain: "cluster.local",
			expectedHost:  "test-isvc-engine.default.svc.cluster.local",
		},
		{
			// The suffix is the cluster domain the ingress block names, not a
			// value of the binary's own.
			name:          "configured cluster domain",
			isvc:          createTestInferenceServiceGateway("test-isvc", "default"),
			clusterDomain: "example.org",
			expectedHost:  "test-isvc-engine.default.svc.example.org",
		},
		{
			name:          "no cluster domain renders the short name",
			isvc:          createTestInferenceServiceGateway("test-isvc", "default"),
			clusterDomain: "",
			expectedHost:  "test-isvc-engine.default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createGatewayAPIStrategy(t).(*GatewayAPIStrategy)
			strategy.ingressConfig.ClusterDomain = tt.clusterDomain
			assert.Equal(t, tt.expectedHost, strategy.getRawServiceHost(tt.isvc))
		})
	}
}

func TestGatewayAPIStrategy_IsHTTPRouteReady(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*gatewayapiv1.HTTPRoute)
		expectedReady bool
	}{
		{name: "current generation accepted", expectedReady: true},
		{
			name: "empty status",
			mutate: func(route *gatewayapiv1.HTTPRoute) {
				route.Status.Parents = nil
			},
			expectedReady: false,
		},
		{
			name: "accepted condition missing",
			mutate: func(route *gatewayapiv1.HTTPRoute) {
				route.Status.Parents[0].Conditions = nil
			},
			expectedReady: false,
		},
		{
			name: "accepted condition unknown",
			mutate: func(route *gatewayapiv1.HTTPRoute) {
				route.Status.Parents[0].Conditions[0].Status = metav1.ConditionUnknown
			},
			expectedReady: false,
		},
		{
			name: "accepted condition is stale",
			mutate: func(route *gatewayapiv1.HTTPRoute) {
				route.Status.Parents[0].Conditions[0].ObservedGeneration--
			},
			expectedReady: false,
		},
		{
			name: "configured parent has no matching status",
			mutate: func(route *gatewayapiv1.HTTPRoute) {
				route.Status.Parents[0].ParentRef.Name = "another-gateway"
			},
			expectedReady: false,
		},
		{
			name: "second configured parent has no status",
			mutate: func(route *gatewayapiv1.HTTPRoute) {
				route.Spec.ParentRefs = append(route.Spec.ParentRefs, gatewayapiv1.ParentReference{
					Name: "second-gateway",
				})
			},
			expectedReady: false,
		},
		{
			name: "current generation condition is false",
			mutate: func(route *gatewayapiv1.HTTPRoute) {
				route.Status.Parents[0].Conditions = append(route.Status.Parents[0].Conditions, metav1.Condition{
					Type:               string(gatewayapiv1.RouteConditionResolvedRefs),
					Status:             metav1.ConditionFalse,
					ObservedGeneration: route.Generation,
					Reason:             string(gatewayapiv1.RouteReasonInvalidKind),
				})
			},
			expectedReady: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := createGatewayAPIStrategy(t)
			route := createReadyHTTPRoute("test-isvc", "default")
			if tt.mutate != nil {
				tt.mutate(route)
			}
			ready, _, _ := strategy.(*GatewayAPIStrategy).isHTTPRouteReady(route)
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
			ClusterDomain:     "cluster.local",
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
		Client:        client,
		Scheme:        scheme,
		IngressConfig: gatewayTestIngressConfig(),
		IsvcConfig:    &controllerconfig.InferenceServicesConfig{},
	}

	return NewGatewayAPIStrategy(opts, services.NewDomainService(), services.NewPathService())
}

func gatewayTestIngressConfig() *controllerconfig.IngressConfig {
	return &controllerconfig.IngressConfig{
		EnableGatewayAPI:  true,
		IngressDomain:     "example.com",
		ClusterDomain:     "cluster.local",
		OmeIngressGateway: "istio-system/gateway",
		DomainTemplate:    "{{.Name}}.{{.Namespace}}.{{.IngressDomain}}",
		UrlScheme:         "https",
	}
}

func makeHTTPRouteSpecsCurrent(
	t *testing.T,
	isvc *v1beta1.InferenceService,
	ingressConfig *controllerconfig.IngressConfig,
	routes []client.Object,
) {
	t.Helper()
	builder := builders.NewHTTPRouteBuilder(
		nil,
		ingressConfig,
		&controllerconfig.InferenceServicesConfig{},
		services.NewDomainService(),
		services.NewPathService(),
	)
	for _, obj := range routes {
		route, ok := obj.(*gatewayapiv1.HTTPRoute)
		require.True(t, ok)
		var component string
		switch route.Name {
		case constants.EngineServiceName(isvc.Name):
			component = builders.EngineComponent
		case constants.RouterServiceName(isvc.Name):
			component = builders.RouterComponent
		case constants.DecoderServiceName(isvc.Name):
			component = builders.DecoderComponent
		case isvc.Name:
			component = builders.TopLevelComponent
		default:
			t.Fatalf("unknown HTTPRoute %q", route.Name)
		}
		desired, err := builder.BuildHTTPRoute(context.Background(), isvc, component)
		require.NoError(t, err)
		require.NotNil(t, desired)
		route.Spec = desired.(*gatewayapiv1.HTTPRoute).Spec
		markHTTPRouteAccepted(route)
	}
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
	parentRef := gatewayapiv1.ParentReference{Name: "gateway"}
	route := &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Generation: 1,
		},
		Spec: gatewayapiv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{
				ParentRefs: []gatewayapiv1.ParentReference{parentRef},
			},
		},
	}
	markHTTPRouteAccepted(route)
	return route
}

func markHTTPRouteAccepted(route *gatewayapiv1.HTTPRoute) {
	parents := make([]gatewayapiv1.RouteParentStatus, 0, len(route.Spec.ParentRefs))
	for _, parentRef := range route.Spec.ParentRefs {
		parents = append(parents, gatewayapiv1.RouteParentStatus{
			ParentRef:      parentRef,
			ControllerName: "test.controller/gateway",
			Conditions: []metav1.Condition{{
				Type:               string(gatewayapiv1.RouteConditionAccepted),
				Status:             metav1.ConditionTrue,
				ObservedGeneration: route.Generation,
				Reason:             string(gatewayapiv1.RouteReasonAccepted),
				LastTransitionTime: metav1.Now(),
			}},
		})
	}
	route.Status.Parents = parents
}

func createNotReadyHTTPRoute(name, namespace string) *gatewayapiv1.HTTPRoute {
	route := createReadyHTTPRoute(name, namespace)
	route.Status.Parents[0].Conditions[0].Status = metav1.ConditionFalse
	route.Status.Parents[0].Conditions[0].Reason = string(gatewayapiv1.RouteReasonNotAllowedByListeners)
	route.Status.Parents[0].Conditions[0].Message = "route not accepted"
	return route
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

// TestGatewayAPIStrategy_Reconcile_RoutesFollowTrafficTargets holds the
// data-plane side of a rollout: when a Component's traffic targets change, the
// routes that already exist are re-rendered with that split as weighted
// backendRefs on the per-revision Services, and collapse back onto the
// aggregate Service once the targets are gone.
func TestGatewayAPIStrategy_Reconcile_RoutesFollowTrafficTargets(t *testing.T) {
	const (
		stableRev = "test-isvc-engine-rev-a1b2c3d4"
		canaryRev = "test-isvc-engine-rev-e5f6a7b8"
	)
	isvc := createTestInferenceServiceGateway("test-isvc", "default")
	setComponentStatusReadyGateway(isvc)
	setEngineTraffic := func(targets ...v1beta1.ComponentTrafficTarget) {
		if isvc.Status.Components == nil {
			isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{}
		}
		cs := isvc.Status.Components[v1beta1.EngineComponent]
		cs.Traffic = targets
		isvc.Status.Components[v1beta1.EngineComponent] = cs
	}

	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, gatewayapiv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	routes := []client.Object{
		createReadyHTTPRoute(constants.EngineServiceName(isvc.Name), isvc.Namespace),
		createReadyHTTPRoute(isvc.Name, isvc.Namespace),
	}
	makeHTTPRouteSpecsCurrent(t, isvc, gatewayTestIngressConfig(), routes)
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(routes...).Build()
	strategy := createGatewayAPIStrategyWithClient(t, c)

	type backend struct {
		name   string
		weight *int32
	}
	backendsOf := func(name string) []backend {
		route := &gatewayapiv1.HTTPRoute{}
		require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: isvc.Namespace}, route))
		require.Len(t, route.Spec.Rules, 1)
		out := make([]backend, 0, len(route.Spec.Rules[0].BackendRefs))
		for _, ref := range route.Spec.Rules[0].BackendRefs {
			out = append(out, backend{name: string(ref.Name), weight: ref.Weight})
		}
		return out
	}
	weight := func(w int32) *int32 { return &w }

	require.NoError(t, strategy.Reconcile(context.Background(), isvc))
	assert.Equal(t, []backend{{name: "test-isvc-engine"}}, backendsOf("test-isvc-engine"),
		"premise: with no targets the route backs onto the aggregate Service")
	assert.Equal(t, corev1.ConditionTrue, isvc.Status.GetCondition(v1beta1.IngressReady).Status,
		"premise: the accepted routes are current")

	setEngineTraffic(
		v1beta1.ComponentTrafficTarget{RevisionName: stableRev, Percent: 90},
		v1beta1.ComponentTrafficTarget{RevisionName: canaryRev, Percent: 10, LatestRevision: true},
	)
	require.NoError(t, strategy.Reconcile(context.Background(), isvc))
	for _, name := range []string{"test-isvc-engine", "test-isvc"} {
		assert.Equal(t, []backend{{name: stableRev, weight: weight(90)}, {name: canaryRev, weight: weight(10)}},
			backendsOf(name), "%s must carry the step's split", name)
	}
	assert.Equal(t, corev1.ConditionFalse, isvc.Status.GetCondition(v1beta1.IngressReady).Status,
		"a reprogrammed route is not ready until its gateway accepts the new generation")

	setEngineTraffic(v1beta1.ComponentTrafficTarget{RevisionName: canaryRev, Percent: 100, LatestRevision: true})
	require.NoError(t, strategy.Reconcile(context.Background(), isvc))
	for _, name := range []string{"test-isvc-engine", "test-isvc"} {
		assert.Equal(t, []backend{{name: canaryRev, weight: weight(100)}}, backendsOf(name),
			"%s must follow the cutover", name)
	}

	setEngineTraffic()
	require.NoError(t, strategy.Reconcile(context.Background(), isvc))
	for _, name := range []string{"test-isvc-engine", "test-isvc"} {
		assert.Equal(t, []backend{{name: "test-isvc-engine"}}, backendsOf(name),
			"%s must return to the aggregate Service once no targets are published", name)
	}
}
