package strategies

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/builders"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
)

const (
	HTTPRouteNotReady                 = "HttpRouteNotReady"
	HTTPRouteParentStatusNotAvailable = "ParentStatusNotAvailable"
)

// GatewayAPIStrategy handles ingress for Gateway API (raw deployment mode)
type GatewayAPIStrategy struct {
	client               client.Client
	scheme               *runtime.Scheme
	ingressConfig        *controllerconfig.IngressConfig
	isvcConfig           *controllerconfig.InferenceServicesConfig
	domainService        interfaces.DomainService
	pathService          interfaces.PathService
	httpRouteBuilder     interfaces.HTTPRouteBuilder
	trafficPolicyBuilder *builders.BackendTrafficPolicyBuilder
}

// NewGatewayAPIStrategy creates a new Gateway API strategy
func NewGatewayAPIStrategy(opts interfaces.ReconcilerOptions, domainService interfaces.DomainService, pathService interfaces.PathService) interfaces.IngressStrategy {
	httpRouteBuilder := builders.NewHTTPRouteBuilder(opts.Client, opts.IngressConfig, opts.IsvcConfig, domainService, pathService)
	trafficPolicyBuilder := builders.NewBackendTrafficPolicyBuilder(opts.IngressConfig)

	return &GatewayAPIStrategy{
		client:               opts.Client,
		scheme:               opts.Scheme,
		ingressConfig:        opts.IngressConfig,
		isvcConfig:           opts.IsvcConfig,
		domainService:        domainService,
		pathService:          pathService,
		httpRouteBuilder:     httpRouteBuilder,
		trafficPolicyBuilder: trafficPolicyBuilder,
	}
}

func (g *GatewayAPIStrategy) GetName() string {
	return "GatewayAPI"
}

func (g *GatewayAPIStrategy) Reconcile(ctx context.Context, isvc *v1beta1.InferenceService) error {
	if !g.ingressConfig.DisableIngressCreation {
		// Track readiness across the pass: each reconcile/check below sets
		// IngressReady=False itself when it finds a problem, and True is
		// only stamped at the end when nothing did — an unconditional True
		// here would overwrite those False conditions every pass.
		ingressReady := true

		// Reconcile component HTTPRoutes
		ready, err := g.reconcileComponentHTTPRoute(ctx, isvc, builders.EngineComponent)
		if err != nil {
			return err
		}
		ingressReady = ingressReady && ready
		if irprojector.RoleDeclared(isvc, v1beta1.RouterComponent) {
			ready, err = g.reconcileComponentHTTPRoute(ctx, isvc, builders.RouterComponent)
			if err != nil {
				return err
			}
			ingressReady = ingressReady && ready
		}
		if irprojector.RoleDeclared(isvc, v1beta1.DecoderComponent) {
			ready, err = g.reconcileComponentHTTPRoute(ctx, isvc, builders.DecoderComponent)
			if err != nil {
				return err
			}
			ingressReady = ingressReady && ready
		}
		ready, err = g.reconcileComponentHTTPRoute(ctx, isvc, builders.TopLevelComponent)
		if err != nil {
			return err
		}
		ingressReady = ingressReady && ready

		// The default BTP (ConsistentHash on configured headers) is
		// emitted here only when the operator has NOT declared any
		// traffic intent. When intent is declared, the per-ISVC
		// traffic.Reconciler owns the BTP — both reconcilers writing
		// the same resource name would fight on every loop.
		ownsBackendTrafficPolicy := !traffic.Resolve(isvc).HasIntent()
		if ownsBackendTrafficPolicy {
			if err := g.reconcileBackendTrafficPolicy(ctx, isvc); err != nil {
				return err
			}
		}

		// Check HTTPRoute and (optionally) BackendTrafficPolicy statuses
		routesProgrammed, err := g.checkHTTPRouteStatuses(ctx, isvc)
		if err != nil {
			return err
		}
		ingressReady = ingressReady && routesProgrammed

		if ownsBackendTrafficPolicy {
			policyAccepted, err := g.checkBackendTrafficPolicyStatus(ctx, isvc)
			if err != nil {
				return err
			}
			ingressReady = ingressReady && policyAccepted
		}

		if ingressReady {
			isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
				Type:   v1beta1.IngressReady,
				Status: corev1.ConditionTrue,
			})
		}
	} else {
		// Ingress creation is disabled. We set it to true as the isvc condition depends on it.
		isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionTrue,
		})
	}

	// Set status URL, Address, and Addresses — all derived from the same builder
	// that produces the HTTPRoutes, so they cannot drift from routing.
	return g.setStatusEndpoints(isvc)
}

// setStatusEndpoints derives status.addresses (one entry per gateway the route
// attaches to, tagged with the operator-declared class, plus the cluster-local
// address) from the shared endpoint builder, and projects the two
// backward-compatible fields from it: status.url = the primary gateway endpoint,
// status.address = the cluster-local endpoint. Because all three come from one
// renderer, they cannot disagree with the HTTPRoutes.
func (g *GatewayAPIStrategy) setStatusEndpoints(isvc *v1beta1.InferenceService) error {
	endpoints, err := g.httpRouteBuilder.Endpoints(isvc, isvc.Name)
	if err != nil {
		return err
	}

	addresses := make([]duckv1.Addressable, 0, len(endpoints)+1)
	var primary *knapis.URL
	for _, ep := range endpoints {
		addr := duckv1.Addressable{
			URL: &knapis.URL{Scheme: ep.Scheme, Host: ep.Host, Path: ep.Path},
		}
		if ep.Class != "" {
			addr.Name = ptr.To(ep.Class)
		}
		addresses = append(addresses, addr)
		if ep.Primary {
			primary = addr.URL
		}
	}

	// Cluster-local endpoint (unchanged semantics) — projected to status.address
	// and also listed in status.addresses, tagged "cluster-local".
	clusterLocal := duckv1.Addressable{
		Name: ptr.To(interfaces.ClusterLocalEndpointClass),
		URL: &knapis.URL{
			Scheme: g.ingressConfig.UrlScheme,
			Host:   g.getRawServiceHost(isvc),
		},
	}
	addresses = append(addresses, clusterLocal)

	isvc.Status.Addresses = addresses
	isvc.Status.Address = &duckv1.Addressable{URL: clusterLocal.URL}
	if primary != nil {
		isvc.Status.URL = primary
	}
	return nil
}

// reconcileComponentHTTPRoute ensures the component's HTTPRoute exists and
// matches the desired spec. It returns ready=false (with IngressReady=False
// already set on the ISVC) when the component is not yet ready for a route.
func (g *GatewayAPIStrategy) reconcileComponentHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService, componentType string) (bool, error) {
	// Use builder to create the HTTPRoute
	desired, err := g.httpRouteBuilder.BuildHTTPRoute(ctx, isvc, componentType)
	if err != nil {
		return false, err
	}
	if desired == nil {
		// Set ingress condition to indicate component not ready
		isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
			Type:    v1beta1.IngressReady,
			Status:  corev1.ConditionFalse,
			Reason:  "ComponentNotReady",
			Message: fmt.Sprintf("%s component not ready for HTTPRoute creation", componentType),
		})
		klog.V(1).InfoS("Builder returned nil HTTPRoute; component not ready", "isvc", isvc.Name, "component", componentType)
		return false, nil
	}

	httpRoute, ok := desired.(*gatewayapiv1.HTTPRoute)
	if !ok {
		return false, fmt.Errorf("builder returned unexpected type %T, expected *gatewayapiv1.HTTPRoute", desired)
	}
	if httpRoute == nil {
		// Defensive: BuildHTTPRoute now returns interface-nil for not-ready
		// components, but guard here too so any future caller that hands us
		// a typed-nil pointer (e.g. via a stub builder in tests, or a future
		// dispatcher regression) does not crash the reconciler in
		// controllerutil.SetControllerReference below.
		isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
			Type:    v1beta1.IngressReady,
			Status:  corev1.ConditionFalse,
			Reason:  "ComponentNotReady",
			Message: fmt.Sprintf("%s component not ready for HTTPRoute creation", componentType),
		})
		klog.V(1).InfoS("Builder returned typed-nil HTTPRoute; component not ready", "isvc", isvc.Name, "component", componentType)
		return false, nil
	}

	if err := controllerutil.SetControllerReference(isvc, httpRoute, g.scheme); err != nil {
		return false, fmt.Errorf("failed to set controller reference for %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
	}

	existing := &gatewayapiv1.HTTPRoute{}
	err = g.client.Get(ctx, types.NamespacedName{Name: httpRoute.Name, Namespace: isvc.Namespace}, existing)
	if err != nil {
		if apierr.IsNotFound(err) {
			createErr := g.client.Create(ctx, httpRoute)
			if createErr != nil && !apierr.IsAlreadyExists(createErr) {
				return false, fmt.Errorf("failed to create %s HttpRoute %s: %w", componentType, httpRoute.Name, createErr)
			}
			// A route created this pass (or one the cache-backed client
			// cannot see yet: AlreadyExists on a NotFound Get means the
			// informer lags the server) has no verified gateway status,
			// so it must not count toward IngressReady=True this pass.
			// The HTTPRoute watch re-enqueues the ISVC once the route is
			// observed, and readiness is decided from its real status then.
			isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
				Type:    v1beta1.IngressReady,
				Status:  corev1.ConditionFalse,
				Reason:  HTTPRouteParentStatusNotAvailable,
				Message: fmt.Sprintf("%s HTTPRoute awaiting gateway programming", componentType),
			})
			return false, nil
		}
		return false, err
	} else {
		// Set ResourceVersion which is required for update operation.
		httpRoute.ResourceVersion = existing.ResourceVersion
		// Do a dry-run update to avoid diffs generated by default values.
		if err := g.client.Update(ctx, httpRoute, client.DryRunAll); err != nil {
			return false, fmt.Errorf("failed to perform dry-run update for %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
		}
		if !g.semanticHttpRouteEquals(httpRoute, existing) {
			if err := g.client.Update(ctx, httpRoute); err != nil {
				return false, fmt.Errorf("failed to update %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
			}
			// The existing parent status describes the previous route
			// generation. Do not treat it as proof that the updated route has
			// been accepted; the HTTPRoute watch will re-enqueue the ISVC after
			// the gateway reports status for the new generation.
			isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
				Type:    v1beta1.IngressReady,
				Status:  corev1.ConditionFalse,
				Reason:  HTTPRouteParentStatusNotAvailable,
				Message: fmt.Sprintf("%s HTTPRoute awaiting gateway reprogramming", componentType),
			})
			return false, nil
		}
	}
	return true, nil
}

func (g *GatewayAPIStrategy) reconcileBackendTrafficPolicy(ctx context.Context, isvc *v1beta1.InferenceService) error {
	policy := g.trafficPolicyBuilder.Build(isvc)

	if err := controllerutil.SetControllerReference(isvc, policy, g.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference for BackendTrafficPolicy %s: %w", isvc.Name, err)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
	err := g.client.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, existing)
	if err != nil {
		if apierr.IsNotFound(err) {
			if err := g.client.Create(ctx, policy); err != nil {
				return fmt.Errorf("failed to create BackendTrafficPolicy %s: %w", isvc.Name, err)
			}
		} else {
			return err
		}
	} else {
		policy.SetResourceVersion(existing.GetResourceVersion())
		if err := g.client.Update(ctx, policy); err != nil {
			return fmt.Errorf("failed to update BackendTrafficPolicy %s: %w", isvc.Name, err)
		}
	}
	return nil
}

// checkHTTPRouteStatuses returns ready=false (with IngressReady=False set)
// when any existing HTTPRoute has not been programmed by its gateway yet.
func (g *GatewayAPIStrategy) checkHTTPRouteStatuses(ctx context.Context, isvc *v1beta1.InferenceService) (bool, error) {
	// The engine HTTPRoute is always emitted as "<isvc>-engine" (distinct
	// from the top-level "<isvc>"); these lookups must match the
	// builder-produced names.
	components := []struct {
		name      string
		condition func() bool
	}{
		{constants.EngineServiceName(isvc.Name), func() bool { return true }},                                     // Engine: "<isvc>-engine"
		{isvc.Name + "-router", func() bool { return irprojector.RoleDeclared(isvc, v1beta1.RouterComponent) }},   // Router
		{isvc.Name + "-decoder", func() bool { return irprojector.RoleDeclared(isvc, v1beta1.DecoderComponent) }}, // Decoder
		{isvc.Name, func() bool { return true }},                                                                  // Top level: "<isvc>"
	}

	for _, comp := range components {
		if !comp.condition() {
			continue
		}

		httpRoute := &gatewayapiv1.HTTPRoute{}
		if err := g.client.Get(ctx, types.NamespacedName{
			Name:      comp.name,
			Namespace: isvc.Namespace,
		}, httpRoute); err != nil {
			if apierr.IsNotFound(err) {
				// NotFound here means reconcileComponentHTTPRoute either
				// skipped the route (component not Ready) or created it so
				// recently the cache has not observed it yet — and in both
				// cases it already set IngressReady=False and reported
				// not-ready, so this pass cannot conclude True. Keep
				// iterating so any component with an existing HTTPRoute
				// still has its status checked; the HTTPRoute watch
				// re-enqueues once the route materializes.
				continue
			}
			return false, err
		}

		if ready, reason, message := g.isHTTPRouteReady(httpRoute); !ready {
			componentType := g.getComponentType(comp.name, isvc)
			isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
				Type:    v1beta1.IngressReady,
				Status:  corev1.ConditionFalse,
				Reason:  *reason,
				Message: fmt.Sprintf("%s %s", componentType, *message),
			})
			return false, nil
		}
	}
	return true, nil
}

// checkBackendTrafficPolicyStatus returns ready=false (with
// IngressReady=False set) when the default BTP reports a False condition.
func (g *GatewayAPIStrategy) checkBackendTrafficPolicyStatus(ctx context.Context, isvc *v1beta1.InferenceService) (bool, error) {
	policy := &unstructured.Unstructured{}
	policy.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
	if err := g.client.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, policy); err != nil {
		if apierr.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}

	status, ok := policy.Object["status"].(map[string]interface{})
	if !ok {
		return true, nil
	}
	conditions, ok := status["conditions"].([]interface{})
	if !ok {
		return true, nil
	}
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if cond["status"] == "False" {
			reason, _ := cond["reason"].(string)
			message, _ := cond["message"].(string)
			isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
				Type:    v1beta1.IngressReady,
				Status:  corev1.ConditionFalse,
				Reason:  reason,
				Message: fmt.Sprintf("BackendTrafficPolicy: %s", message),
			})
			return false, nil
		}
	}
	return true, nil
}

func (g *GatewayAPIStrategy) getRawServiceHost(isvc *v1beta1.InferenceService) string {
	if irprojector.RoleDeclared(isvc, v1beta1.RouterComponent) {
		routerName := isvc.Name + "-router" // Actual router service name
		return routerName + "." + isvc.Namespace + ".svc.cluster.local"
	}
	// Engine Service is named "<isvc>-engine" (constants.EngineServiceName).
	engineName := constants.EngineServiceName(isvc.Name)
	return engineName + "." + isvc.Namespace + ".svc.cluster.local"
}

func (g *GatewayAPIStrategy) semanticHttpRouteEquals(desired, existing *gatewayapiv1.HTTPRoute) bool {
	return equality.Semantic.DeepEqual(desired.Spec, existing.Spec)
}

// isHTTPRouteReady checks that every configured parent has accepted the
// current HTTPRoute generation and has no current False condition.
func (g *GatewayAPIStrategy) isHTTPRouteReady(httpRoute *gatewayapiv1.HTTPRoute) (bool, *string, *string) {
	if httpRoute == nil || len(httpRoute.Spec.ParentRefs) == 0 || len(httpRoute.Status.Parents) == 0 {
		return false, ptr.To(HTTPRouteParentStatusNotAvailable), ptr.To(HTTPRouteNotReady)
	}
	for _, parentRef := range httpRoute.Spec.ParentRefs {
		accepted := false
		for _, parentStatus := range httpRoute.Status.Parents {
			if !equality.Semantic.DeepEqual(parentRef, parentStatus.ParentRef) {
				continue
			}
			for i := range parentStatus.Conditions {
				condition := &parentStatus.Conditions[i]
				if condition.ObservedGeneration != httpRoute.Generation {
					continue
				}
				if condition.Status == metav1.ConditionFalse {
					return false, &condition.Reason, &condition.Message
				}
				if condition.Type == string(gatewayapiv1.RouteConditionAccepted) &&
					condition.Status == metav1.ConditionTrue {
					accepted = true
				}
			}
		}
		if !accepted {
			return false, ptr.To(HTTPRouteParentStatusNotAvailable), ptr.To(HTTPRouteNotReady)
		}
	}
	return true, nil, nil
}

// getComponentType returns the component type name for display purposes.
// Engine HTTPRoute is "<isvc>-engine", top-level is "<isvc>".
func (g *GatewayAPIStrategy) getComponentType(name string, isvc *v1beta1.InferenceService) string {
	switch name {
	case constants.EngineServiceName(isvc.Name): // "<isvc>-engine"
		return "Engine"
	case isvc.Name + "-router":
		return "Router"
	case isvc.Name + "-decoder":
		return "Decoder"
	case isvc.Name:
		return "TopLevel"
	default:
		return "Component"
	}
}
