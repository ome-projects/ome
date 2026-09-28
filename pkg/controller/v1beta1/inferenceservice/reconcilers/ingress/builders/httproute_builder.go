package builders

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress/interfaces"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/utils"
)

const (
	EngineComponent   = "engine"
	RouterComponent   = "router"
	DecoderComponent  = "decoder"
	TopLevelComponent = "toplevel"

	// Fallback backend ports used only when the component's Service can't
	// be resolved (e.g. early reconcile before the Service exists). The
	// authoritative port is the component Service's first port.
	EngineDefaultPort  int32 = 30000
	DecoderDefaultPort int32 = 30000
	RouterDefaultPort  int32 = 30080
)

// HTTPRouteBuilder builds Gateway API HTTPRoute resources
type HTTPRouteBuilder struct {
	client        client.Client
	ingressConfig *controllerconfig.IngressConfig
	isvcConfig    *controllerconfig.InferenceServicesConfig
	domainService interfaces.DomainService
	pathService   interfaces.PathService
}

// NewHTTPRouteBuilder creates a new HTTPRoute builder. The client resolves
// each component's real backend port from its Service; it may be nil, in
// which case the default port consts are used.
func NewHTTPRouteBuilder(c client.Client, ingressConfig *controllerconfig.IngressConfig, isvcConfig *controllerconfig.InferenceServicesConfig,
	domainService interfaces.DomainService, pathService interfaces.PathService) interfaces.HTTPRouteBuilder {
	return &HTTPRouteBuilder{
		client:        c,
		ingressConfig: ingressConfig,
		isvcConfig:    isvcConfig,
		domainService: domainService,
		pathService:   pathService,
	}
}

// resolveServicePort returns the named Service's first port, falling back
// to defaultPort when the Service can't be resolved. This matches the
// port OME sets on the component Service (its runner's containerPort).
func (b *HTTPRouteBuilder) resolveServicePort(ctx context.Context, namespace, serviceName string, defaultPort int32) int32 {
	return isvcutils.ResolveServicePort(ctx, b.client, namespace, serviceName, defaultPort)
}

func (b *HTTPRouteBuilder) GetResourceType() string {
	return "HTTPRoute"
}

func (b *HTTPRouteBuilder) Build(ctx context.Context, isvc *v1beta1.InferenceService) (client.Object, error) {
	return b.BuildHTTPRoute(ctx, isvc, EngineComponent)
}

func (b *HTTPRouteBuilder) BuildHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService, componentType string) (client.Object, error) {
	var route *gatewayapiv1.HTTPRoute
	var err error
	switch componentType {
	case EngineComponent:
		route, err = b.buildEngineHTTPRoute(ctx, isvc)
	case RouterComponent:
		route, err = b.buildRouterHTTPRoute(ctx, isvc)
	case DecoderComponent:
		route, err = b.buildDecoderHTTPRoute(ctx, isvc)
	case TopLevelComponent:
		route, err = b.buildTopLevelHTTPRoute(ctx, isvc)
	default:
		return nil, fmt.Errorf("unsupported component type: %s", componentType)
	}
	if route == nil {
		// Return an explicit interface-nil rather than letting Go wrap a
		// typed-nil (*HTTPRoute)(nil) into a non-nil client.Object. The
		// caller checks `desired == nil`, which is false for a typed-nil
		// inside an interface — that gap caused a panic when a per-
		// component builder returned (nil, nil) for the "component not
		// ready" branch and the strategy then tried to call
		// controllerutil.SetControllerReference on the typed-nil pointer.
		return nil, err
	}
	return route, err
}

// toGatewayAPIDuration converts seconds to gatewayapiv1.Duration
func toGatewayAPIDuration(seconds int64) *gatewayapiv1.Duration {
	duration := gatewayapiv1.Duration(fmt.Sprintf("%ds", seconds))
	return &duration
}

// defaultTimeout returns the cluster-default HTTPRoute request timeout from
// IngressConfig.DefaultRouteTimeoutSeconds. A configured value of 0 yields "0s",
// which Gateway API / Envoy Gateway treat as an explicit DISABLE of the request
// timeout (verified live). This distinction matters: an *omitted* timeout is NOT
// "unbounded" — Envoy falls back to its built-in 15s route default, which would
// truncate long-running inference. So 0 must emit "0s", not be dropped. When the
// value is nil (unset) or negative we return nil (timeout left unset → Envoy's
// 15s default applies). The value is config-driven (inferenceservice-config
// ConfigMap / chart), never a hardcoded literal. A per-ISVC
// spec.<component>.timeoutSeconds still overrides.
func (b *HTTPRouteBuilder) defaultTimeout() *gatewayapiv1.Duration {
	if b.ingressConfig != nil && b.ingressConfig.DefaultRouteTimeoutSeconds != nil && *b.ingressConfig.DefaultRouteTimeoutSeconds >= 0 {
		return toGatewayAPIDuration(*b.ingressConfig.DefaultRouteTimeoutSeconds)
	}
	return nil
}

// gatewayHostnameForDomain renders the HTTPRoute host for a given gateway domain.
// In the shared-host scheme it is "<SharedHostPrefix>.<domain>" (the prefix is
// config-supplied, not hardcoded; empty yields the bare domain). When
// PerISVCSubdomain is set it returns this ISVC's own subdomain via the same
// domainService.GenerateDomainName the strategy uses for status.url — so the
// route host and status.url can't drift.
func (b *HTTPRouteBuilder) gatewayHostnameForDomain(isvc *v1beta1.InferenceService, domain string) (gatewayapiv1.Hostname, error) {
	if b.ingressConfig.PerISVCSubdomain {
		// Render with this gateway's domain so each gateway's host follows the
		// same template as status.url.
		cfg := *b.ingressConfig
		cfg.IngressDomain = domain
		host, err := b.domainService.GenerateDomainName(isvc.Name, isvc.ObjectMeta, &cfg)
		if err != nil {
			return "", err
		}
		return gatewayapiv1.Hostname(host), nil
	}
	if b.ingressConfig.SharedHostPrefix != "" {
		return gatewayapiv1.Hostname(b.ingressConfig.SharedHostPrefix + "." + domain), nil
	}
	return gatewayapiv1.Hostname(domain), nil
}

// resolvedGateway pairs a resolved gateway spec with its endpoint class and
// whether it is the config-designated primary gateway. It is the single source
// both gatewaysForRoute (routing) and Endpoints (status) iterate, so the two
// cannot drift.
type resolvedGateway struct {
	spec    controllerconfig.IngressGatewaySpec
	class   string
	primary bool
}

// resolvedGateways returns the gateways a route attaches to, in host/parentRef
// order: the primary gateway (OmeIngressGateway + IngressDomain, class
// OmeIngressGatewayClass) first, then every AdditionalIngressGateways entry with
// its own class. A per-namespace override (NamespaceIngressGateways) replaces the
// primary and/or additional gateways for ISVCs in that namespace, carrying that
// override's own class. A per-ISVC gateway annotation, already applied to
// b.ingressConfig by ResolveIngressConfig, wins over the namespace default, so
// the corresponding override is skipped when set.
func (b *HTTPRouteBuilder) resolvedGateways(isvc *v1beta1.InferenceService) []resolvedGateway {
	primary := controllerconfig.IngressGatewaySpec{
		OmeIngressGateway: b.ingressConfig.OmeIngressGateway,
		IngressDomain:     b.ingressConfig.IngressDomain,
	}
	primaryClass := b.ingressConfig.OmeIngressGatewayClass
	additional := b.ingressConfig.AdditionalIngressGateways

	if nsGW, ok := b.ingressConfig.NamespaceIngressGateways[isvc.Namespace]; ok {
		if _, annotated := isvc.Annotations[constants.IngressGatewayOverride]; !annotated && nsGW.Primary.OmeIngressGateway != "" {
			primary = nsGW.Primary
			primaryClass = nsGW.Primary.Class
		}
		if _, annotated := isvc.Annotations[constants.IngressAdditionalGateways]; !annotated && nsGW.Additional != nil {
			additional = nsGW.Additional
		}
	}

	gateways := make([]resolvedGateway, 0, 1+len(additional))
	gateways = append(gateways, resolvedGateway{spec: primary, class: primaryClass, primary: true})
	for _, gw := range additional {
		gateways = append(gateways, resolvedGateway{spec: gw, class: gw.Class})
	}
	return gateways
}

// gatewaysForRoute returns the parentRefs and hostnames a route attaches to: the
// primary gateway plus every additional gateway (see resolvedGateways), each with
// a hostname rendered from its own domain. This lets one route serve, e.g., both
// an internal and an external gateway. The hostnames and parentRefs are
// index-aligned per gateway.
func (b *HTTPRouteBuilder) gatewaysForRoute(isvc *v1beta1.InferenceService) ([]gatewayapiv1.ParentReference, []gatewayapiv1.Hostname, error) {
	gateways := b.resolvedGateways(isvc)

	parentRefs := make([]gatewayapiv1.ParentReference, 0, len(gateways))
	hosts := make([]gatewayapiv1.Hostname, 0, len(gateways))
	for _, gw := range gateways {
		host, err := b.gatewayHostnameForDomain(isvc, gw.spec.IngressDomain)
		if err != nil {
			return nil, nil, err
		}
		hosts = append(hosts, host)
		slice := strings.Split(gw.spec.OmeIngressGateway, "/")
		ns := slice[0]
		name := slice[len(slice)-1]
		parentRefs = append(parentRefs, gatewayapiv1.ParentReference{
			Group:     (*gatewayapiv1.Group)(&gatewayapiv1.GroupVersion.Group),
			Kind:      (*gatewayapiv1.Kind)(ptr.To(constants.GatewayKind)),
			Namespace: (*gatewayapiv1.Namespace)(&ns),
			Name:      gatewayapiv1.ObjectName(name),
		})
	}
	return parentRefs, hosts, nil
}

// pathPrefix returns the route's PathPrefix match. By default this is
// "/<namespace>/<service>/" (the shared host disambiguates ISVCs by path).
// When PerISVCSubdomain is set, the host already identifies the ISVC, so the
// route matches at root "/" and no prefix rewrite is needed.
func (b *HTTPRouteBuilder) pathPrefix(isvc *v1beta1.InferenceService, serviceName string) string {
	if b.ingressConfig.PerISVCSubdomain {
		return "/"
	}
	return "/" + isvc.Namespace + "/" + serviceName + "/"
}

// componentFilters builds the per-route filters: ISVC headers plus, under the
// default shared-host scheme, the urlRewrite that strips the path prefix. With
// PerISVCSubdomain the match is already at root, so the rewrite is omitted.
func (b *HTTPRouteBuilder) componentFilters(isvc *v1beta1.InferenceService) []gatewayapiv1.HTTPRouteFilter {
	filters := []gatewayapiv1.HTTPRouteFilter{b.addIsvcHeaders(isvc.Name, isvc.Namespace)}
	if !b.ingressConfig.PerISVCSubdomain {
		filters = append(filters, b.urlRewriteFilter())
	}
	return filters
}

func (b *HTTPRouteBuilder) createPathPrefixMatch(prefix string) gatewayapiv1.HTTPRouteMatch {
	return gatewayapiv1.HTTPRouteMatch{
		Path: &gatewayapiv1.HTTPPathMatch{
			Type:  ptr.To(gatewayapiv1.PathMatchPathPrefix),
			Value: ptr.To(prefix),
		},
	}
}

func (b *HTTPRouteBuilder) urlRewriteFilter() gatewayapiv1.HTTPRouteFilter {
	return gatewayapiv1.HTTPRouteFilter{
		Type: gatewayapiv1.HTTPRouteFilterURLRewrite,
		URLRewrite: &gatewayapiv1.HTTPURLRewriteFilter{
			Path: &gatewayapiv1.HTTPPathModifier{
				Type:               gatewayapiv1.PrefixMatchHTTPPathModifier,
				ReplacePrefixMatch: ptr.To("/"),
			},
		},
	}
}

func (b *HTTPRouteBuilder) addIsvcHeaders(name string, namespace string) gatewayapiv1.HTTPRouteFilter {
	return gatewayapiv1.HTTPRouteFilter{
		Type: gatewayapiv1.HTTPRouteFilterRequestHeaderModifier,
		RequestHeaderModifier: &gatewayapiv1.HTTPHeaderFilter{
			Set: []gatewayapiv1.HTTPHeader{
				{
					Name:  constants.IsvcNameHeader,
					Value: name,
				},
				{
					Name:  constants.IsvcNamespaceHeader,
					Value: namespace,
				},
			},
		},
	}
}

func (b *HTTPRouteBuilder) createHTTPRouteRule(routeMatches []gatewayapiv1.HTTPRouteMatch, filters []gatewayapiv1.HTTPRouteFilter,
	serviceName, namespace string, port int32, timeout *gatewayapiv1.Duration,
) gatewayapiv1.HTTPRouteRule {
	var backendRefs []gatewayapiv1.HTTPBackendRef
	if serviceName != "" {
		backendRefs = []gatewayapiv1.HTTPBackendRef{
			{
				BackendRef: gatewayapiv1.BackendRef{
					BackendObjectReference: gatewayapiv1.BackendObjectReference{
						Kind:      ptr.To(gatewayapiv1.Kind(constants.ServiceKind)),
						Name:      gatewayapiv1.ObjectName(serviceName),
						Namespace: (*gatewayapiv1.Namespace)(&namespace),
						Port:      (*gatewayapiv1.PortNumber)(&port),
					},
				},
			},
		}
	}
	rule := gatewayapiv1.HTTPRouteRule{
		Matches:     routeMatches,
		Filters:     filters,
		BackendRefs: backendRefs,
	}
	// Only set a request timeout when one is configured (cluster default via
	// IngressConfig.DefaultRouteTimeoutSeconds, or a per-ISVC override). A "0s"
	// timeout explicitly DISABLES it (Gateway API). A nil timeout leaves Timeouts
	// unset — note that means Envoy's built-in 15s route default applies, NOT
	// "unbounded". OME never imposes a hardcoded timeout value.
	if timeout != nil {
		rule.Timeouts = &gatewayapiv1.HTTPRouteTimeouts{Request: timeout}
	}
	return rule
}

func (b *HTTPRouteBuilder) buildEngineHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService) (*gatewayapiv1.HTTPRoute, error) {
	if !componentCanBackRoute(isvc, v1beta1.EngineComponent, v1beta1.EngineReady) {
		isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionFalse,
			Reason: "Engine ingress not created",
		})
		return nil, nil
	}

	// The engine HTTPRoute is always named "<isvc>-engine", distinct from the
	// top-level "<isvc>" HTTPRoute. The legacy engine route was named via
	// PredictorServiceName which returns "<isvc>" — colliding with the
	// top-level route's name and pointing at a backend Service that no longer
	// exists (the engine Service is named "<isvc>-engine"). Using
	// EngineServiceName for both the route name and the backend fixes both
	// bugs and makes the engine externally addressable on its own subdomain
	// even when a router is present.
	engineName := constants.EngineServiceName(isvc.Name)
	prefix := b.pathPrefix(isvc, engineName)
	filters := b.componentFilters(isvc)

	timeout := b.defaultTimeout()
	if isvc.Spec.Engine != nil && isvc.Spec.Engine.TimeoutSeconds != nil {
		timeout = toGatewayAPIDuration(*isvc.Spec.Engine.TimeoutSeconds)
	}

	port := b.resolveServicePort(ctx, isvc.Namespace, engineName, EngineDefaultPort)
	routeMatch := []gatewayapiv1.HTTPRouteMatch{b.createPathPrefixMatch(prefix)}
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createHTTPRouteRule(routeMatch, filters, engineName, isvc.Namespace, port, timeout),
	}

	return b.buildHTTPRouteResource(isvc, engineName, httpRouteRules)
}

func (b *HTTPRouteBuilder) buildRouterHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService) (*gatewayapiv1.HTTPRoute, error) {
	if !componentCanBackRoute(isvc, v1beta1.RouterComponent, v1beta1.RouterReady) {
		isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionFalse,
			Reason: "Router ingress not created",
		})
		return nil, nil
	}

	routerName := constants.RouterServiceName(isvc.Name)
	prefix := b.pathPrefix(isvc, routerName)
	filters := b.componentFilters(isvc)

	timeout := b.defaultTimeout()
	if isvc.Spec.Router.TimeoutSeconds != nil {
		timeout = toGatewayAPIDuration(*isvc.Spec.Router.TimeoutSeconds)
	}

	port := b.resolveServicePort(ctx, isvc.Namespace, routerName, RouterDefaultPort)
	routeMatch := []gatewayapiv1.HTTPRouteMatch{b.createPathPrefixMatch(prefix)}
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createHTTPRouteRule(routeMatch, filters, routerName, isvc.Namespace, port, timeout),
	}

	return b.buildHTTPRouteResource(isvc, constants.RouterServiceName(isvc.Name), httpRouteRules)
}

func (b *HTTPRouteBuilder) buildDecoderHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService) (*gatewayapiv1.HTTPRoute, error) {
	if !componentCanBackRoute(isvc, v1beta1.DecoderComponent, v1beta1.DecoderReady) {
		isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
			Type:   v1beta1.IngressReady,
			Status: corev1.ConditionFalse,
			Reason: "Decoder ingress not created",
		})
		return nil, nil
	}

	decoderName := constants.DecoderServiceName(isvc.Name)
	prefix := b.pathPrefix(isvc, decoderName)
	filters := b.componentFilters(isvc)

	timeout := b.defaultTimeout()
	if isvc.Spec.Decoder.TimeoutSeconds != nil {
		timeout = toGatewayAPIDuration(*isvc.Spec.Decoder.TimeoutSeconds)
	}

	port := b.resolveServicePort(ctx, isvc.Namespace, decoderName, DecoderDefaultPort)
	routeMatch := []gatewayapiv1.HTTPRouteMatch{b.createPathPrefixMatch(prefix)}
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createHTTPRouteRule(routeMatch, filters, decoderName, isvc.Namespace, port, timeout),
	}

	return b.buildHTTPRouteResource(isvc, constants.DecoderServiceName(isvc.Name), httpRouteRules)
}

func (b *HTTPRouteBuilder) buildTopLevelHTTPRoute(ctx context.Context, isvc *v1beta1.InferenceService) (*gatewayapiv1.HTTPRoute, error) {
	filters := b.componentFilters(isvc)

	var serviceName string
	var port int32
	var timeout *gatewayapiv1.Duration

	if isvc.Spec.Router != nil {
		serviceName = constants.RouterServiceName(isvc.Name)
		port = b.resolveServicePort(ctx, isvc.Namespace, serviceName, RouterDefaultPort)
		timeout = b.defaultTimeout()
		if isvc.Spec.Router.TimeoutSeconds != nil {
			timeout = toGatewayAPIDuration(*isvc.Spec.Router.TimeoutSeconds)
		}
	} else {
		if !componentCanBackRoute(isvc, v1beta1.EngineComponent, v1beta1.EngineReady) {
			isvc.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{
				Type:   v1beta1.IngressReady,
				Status: corev1.ConditionFalse,
				Reason: "Engine not ready",
			})
			return nil, nil
		}
		// Align with the engine Service name produced by engine.go
		// (defaultEngineName = isvc.Name + "-engine") rather than the legacy
		// PredictorServiceName (returns isvc.Name). The PredictorServiceName
		// path pointed at a backend Service that no longer exists in the
		// Engine-architecture post-Predictor world.
		serviceName = constants.EngineServiceName(isvc.Name)
		port = b.resolveServicePort(ctx, isvc.Namespace, serviceName, EngineDefaultPort)
		timeout = b.defaultTimeout()
		if isvc.Spec.Engine != nil && isvc.Spec.Engine.TimeoutSeconds != nil {
			timeout = toGatewayAPIDuration(*isvc.Spec.Engine.TimeoutSeconds)
		}
	}

	prefix := b.pathPrefix(isvc, isvc.Name)
	routeMatch := []gatewayapiv1.HTTPRouteMatch{b.createPathPrefixMatch(prefix)}
	httpRouteRules := []gatewayapiv1.HTTPRouteRule{
		b.createHTTPRouteRule(routeMatch, filters, serviceName, isvc.Namespace, port, timeout),
	}

	return b.buildHTTPRouteResource(isvc, isvc.Name, httpRouteRules)
}

func (b *HTTPRouteBuilder) buildHTTPRouteResource(isvc *v1beta1.InferenceService, name string, httpRouteRules []gatewayapiv1.HTTPRouteRule) (*gatewayapiv1.HTTPRoute, error) {
	annotations := utils.Filter(isvc.Annotations, func(key string) bool {
		return !utils.Includes(constants.ServiceAnnotationDisallowedList, key)
	})
	labels := utils.Filter(isvc.Labels, func(key string) bool {
		return !utils.Includes(constants.RevisionTemplateLabelDisallowedList, key)
	})

	parentRefs, hosts, err := b.gatewaysForRoute(isvc)
	if err != nil {
		return nil, err
	}

	return &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   isvc.Namespace,
			Annotations: annotations,
			Labels:      labels,
		},
		Spec: gatewayapiv1.HTTPRouteSpec{
			Hostnames: hosts,
			Rules:     httpRouteRules,
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{
				ParentRefs: parentRefs,
			},
		},
	}, nil
}
