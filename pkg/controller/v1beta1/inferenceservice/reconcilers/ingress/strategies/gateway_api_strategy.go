package strategies

import (
	"context"
	"errors"
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
	if g.ingressConfig.DisableIngressCreation {
		// Nothing is created, and the service's readiness depends on the
		// condition, so it reads True.
		markIngressReady(isvc)
		return g.setStatusEndpoints(isvc)
	}
	ready, err := g.reconcileRoutes(ctx, isvc)
	if err != nil {
		return err
	}
	// Each step below writes IngressReady=False itself when it finds a
	// problem; True is written only when none did, so an earlier False of
	// this pass is never overwritten.
	if ready {
		markIngressReady(isvc)
	}
	// Set status URL, Address, and Addresses — all derived from the same builder
	// that produces the HTTPRoutes, so they cannot drift from routing.
	return g.setStatusEndpoints(isvc)
}

// reconcileRoutes writes the HTTPRoute of every routed Component and, when the
// operator declares no traffic intent, the default BackendTrafficPolicy, then
// reads the gateway's answer for each. It reports whether every route is
// programmed and the policy accepted.
func (g *GatewayAPIStrategy) reconcileRoutes(ctx context.Context, isvc *v1beta1.InferenceService) (bool, error) {
	ready := true
	for _, component := range routedComponents(isvc) {
		routeReady, err := g.reconcileComponentHTTPRoute(ctx, isvc, component)
		if err != nil {
			return false, err
		}
		ready = ready && routeReady
	}

	// The default policy (ConsistentHash on the configured headers) is
	// written only when the operator declares no traffic intent; with an
	// intent the per-service traffic reconciler owns the policy, and two
	// writers of one object would fight on every pass.
	ownsBackendTrafficPolicy := !traffic.Resolve(isvc).HasIntent()
	if ownsBackendTrafficPolicy {
		if err := g.reconcileBackendTrafficPolicy(ctx, isvc); err != nil {
			return false, err
		}
	}

	routesProgrammed, err := g.checkHTTPRouteStatuses(ctx, isvc)
	if err != nil {
		return false, err
	}
	ready = ready && routesProgrammed

	if ownsBackendTrafficPolicy {
		policyAccepted, err := g.checkBackendTrafficPolicyStatus(ctx, isvc)
		if err != nil {
			return false, err
		}
		ready = ready && policyAccepted
	}
	return ready, nil
}

// routedComponents lists the routes of a service in the order they are
// written: the top-level route first, then the engine's, then the router's
// and the decoder's when declared. Under per-service subdomains every route
// matches the same host at "/", and Gateway API gives that tie to the oldest
// route, so the top-level route must be the oldest.
func routedComponents(isvc *v1beta1.InferenceService) []string {
	components := []string{builders.TopLevelComponent, builders.EngineComponent}
	if irprojector.RoleDeclared(isvc, v1beta1.RouterComponent) {
		components = append(components, builders.RouterComponent)
	}
	if irprojector.RoleDeclared(isvc, v1beta1.DecoderComponent) {
		components = append(components, builders.DecoderComponent)
	}
	return components
}

// markIngressReady writes IngressReady=True.
func markIngressReady(isvc *v1beta1.InferenceService) {
	isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
		Type:   v1beta1.IngressReady,
		Status: corev1.ConditionTrue,
	})
}

// markIngressNotReady writes IngressReady=False with the reason and message.
func markIngressNotReady(isvc *v1beta1.InferenceService, reason, message string) {
	isvc.Status.SetCondition(v1beta1.IngressReady, &knapis.Condition{
		Type:    v1beta1.IngressReady,
		Status:  corev1.ConditionFalse,
		Reason:  reason,
		Message: message,
	})
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

// reconcileComponentHTTPRoute brings the Component's HTTPRoute to the desired
// spec. It reports false, with IngressReady=False written here, when the
// Component cannot back a route yet, when the route was created this pass or
// when it was rewritten, because in each case the gateway has not programmed
// the route as it now stands; the builders write no condition themselves.
func (g *GatewayAPIStrategy) reconcileComponentHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService, componentType string) (bool, error) {
	httpRoute, err := g.desiredHTTPRoute(ctx, isvc, componentType)
	if err != nil || httpRoute == nil {
		return false, err
	}

	existing := &gatewayapiv1.HTTPRoute{}
	err = g.client.Get(ctx, types.NamespacedName{Name: httpRoute.Name, Namespace: isvc.Namespace}, existing)
	switch {
	case apierr.IsNotFound(err):
		return g.createHTTPRoute(ctx, isvc, componentType, httpRoute)
	case err != nil:
		return false, err
	}
	return g.updateHTTPRoute(ctx, isvc, componentType, httpRoute, existing)
}

// desiredHTTPRoute asks the builder for the Component's route and sets the
// service as its controller. A Component that cannot back a route yet writes
// IngressReady=False and yields no route and no error.
func (g *GatewayAPIStrategy) desiredHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService, componentType string) (*gatewayapiv1.HTTPRoute, error) {
	desired, err := g.httpRouteBuilder.BuildHTTPRoute(ctx, isvc, componentType)
	var notReady *interfaces.ComponentNotReadyError
	if errors.As(err, &notReady) {
		markIngressNotReady(isvc, "ComponentNotReady", fmt.Sprintf("%s component not ready for HTTPRoute creation", componentType))
		klog.V(1).InfoS("Component not ready; HTTPRoute not built", "isvc", isvc.Name, "component", componentType)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// A builder that hands back no route without saying why broke its
	// contract; refuse it instead of dereferencing a nil route below.
	httpRoute, ok := desired.(*gatewayapiv1.HTTPRoute)
	if !ok || httpRoute == nil {
		return nil, fmt.Errorf("builder returned %T for the %s HTTPRoute, expected a non-nil *gatewayapiv1.HTTPRoute", desired, componentType)
	}

	if err := controllerutil.SetControllerReference(isvc, httpRoute, g.scheme); err != nil {
		return nil, fmt.Errorf("failed to set controller reference for %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
	}
	return httpRoute, nil
}

// createHTTPRoute creates a route the cache does not hold. A route created
// this pass, or one the cache-backed client cannot see yet (AlreadyExists on
// a NotFound Get means the informer lags the server), has no verified gateway
// status, so it does not count toward IngressReady=True this pass; the
// HTTPRoute watch re-enqueues the service once the route is observed.
func (g *GatewayAPIStrategy) createHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService, componentType string, httpRoute *gatewayapiv1.HTTPRoute) (bool, error) {
	if err := g.client.Create(ctx, httpRoute); err != nil && !apierr.IsAlreadyExists(err) {
		return false, fmt.Errorf("failed to create %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
	}
	markIngressNotReady(isvc, HTTPRouteParentStatusNotAvailable, fmt.Sprintf("%s HTTPRoute awaiting gateway programming", componentType))
	return false, nil
}

// updateHTTPRoute rewrites a stored route whose spec differs from the desired
// one. The stored parent status describes the previous route generation, so
// a rewritten route does not count as accepted; the HTTPRoute watch
// re-enqueues the service after the gateway reports on the new generation.
func (g *GatewayAPIStrategy) updateHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService, componentType string, httpRoute, existing *gatewayapiv1.HTTPRoute) (bool, error) {
	// Set ResourceVersion which is required for update operation.
	httpRoute.ResourceVersion = existing.ResourceVersion
	// Do a dry-run update to avoid diffs generated by default values.
	if err := g.client.Update(ctx, httpRoute, client.DryRunAll); err != nil {
		return false, fmt.Errorf("failed to perform dry-run update for %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
	}
	if g.semanticHttpRouteEquals(httpRoute, existing) {
		return true, nil
	}
	if err := g.client.Update(ctx, httpRoute); err != nil {
		return false, fmt.Errorf("failed to update %s HttpRoute %s: %w", componentType, httpRoute.Name, err)
	}
	markIngressNotReady(isvc, HTTPRouteParentStatusNotAvailable, fmt.Sprintf("%s HTTPRoute awaiting gateway reprogramming", componentType))
	return false, nil
}

// reconcileBackendTrafficPolicy creates the default policy or rewrites the
// stored one to the desired spec.
func (g *GatewayAPIStrategy) reconcileBackendTrafficPolicy(ctx context.Context, isvc *v1beta1.InferenceService) error {
	policy := g.trafficPolicyBuilder.Build(isvc)

	if err := controllerutil.SetControllerReference(isvc, policy, g.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference for BackendTrafficPolicy %s: %w", isvc.Name, err)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(builders.BackendTrafficPolicyGVK)
	err := g.client.Get(ctx, types.NamespacedName{Name: isvc.Name, Namespace: isvc.Namespace}, existing)
	switch {
	case apierr.IsNotFound(err):
		if err := g.client.Create(ctx, policy); err != nil {
			return fmt.Errorf("failed to create BackendTrafficPolicy %s: %w", isvc.Name, err)
		}
		return nil
	case err != nil:
		return err
	}
	policy.SetResourceVersion(existing.GetResourceVersion())
	if err := g.client.Update(ctx, policy); err != nil {
		return fmt.Errorf("failed to update BackendTrafficPolicy %s: %w", isvc.Name, err)
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
			markIngressNotReady(isvc, *reason, fmt.Sprintf("%s %s", g.getComponentType(comp.name, isvc), *message))
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
			markIngressNotReady(isvc, reason, fmt.Sprintf("BackendTrafficPolicy: %s", message))
			return false, nil
		}
	}
	return true, nil
}

// getRawServiceHost is the in-cluster hostname of the Service that fronts
// the InferenceService: the router's when one is declared, else the engine's
// ("<isvc>-engine", constants.EngineServiceName). The domain suffix comes
// from ingress.clusterDomain.
func (g *GatewayAPIStrategy) getRawServiceHost(isvc *v1beta1.InferenceService) string {
	if irprojector.RoleDeclared(isvc, v1beta1.RouterComponent) {
		return constants.ServiceHostname(constants.RouterServiceName(isvc.Name), isvc.Namespace, g.ingressConfig.ClusterDomain)
	}
	return constants.ServiceHostname(constants.EngineServiceName(isvc.Name), isvc.Namespace, g.ingressConfig.ClusterDomain)
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
		accepted, failed := parentAcceptance(httpRoute, parentRef)
		if failed != nil {
			return false, &failed.Reason, &failed.Message
		}
		if !accepted {
			return false, ptr.To(HTTPRouteParentStatusNotAvailable), ptr.To(HTTPRouteNotReady)
		}
	}
	return true, nil, nil
}

// parentAcceptance reads one parent's answer for the route's current
// generation: the first False condition it reports, else whether it reports
// the route Accepted. Conditions for an older generation say nothing.
func parentAcceptance(httpRoute *gatewayapiv1.HTTPRoute, parentRef gatewayapiv1.ParentReference) (accepted bool, failed *metav1.Condition) {
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
				return false, condition
			}
			if condition.Type == string(gatewayapiv1.RouteConditionAccepted) &&
				condition.Status == metav1.ConditionTrue {
				accepted = true
			}
		}
	}
	return accepted, nil
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
