package endpoint

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	// GatewayAPITrafficMapPublisherName is the durable publisher identity stored
	// in each TrafficMap cleanup journal.
	GatewayAPITrafficMapPublisherName = "gatewayapi"

	gatewayAPIHTTPRouteClaimPrefix = "v1:httproute:"
	gatewayAPIHTTPRouteKind        = "HTTPRoute"

	// Gateway API limits one HTTPRoute rule to 16 backendRefs.
	maxGatewayAPIBackendRefs = 16
	// Gateway API constrains each backendRef weight to this upper bound.
	maxGatewayAPIBackendWeight int32 = 1_000_000
)

// GatewayAPITrafficMapPublisher realizes TrafficMaps through the portable
// Gateway API resources managed by GatewayAPIPublisher. It is a separate type
// because the TrafficMap and legacy endpoint lifecycles have different cleanup
// contracts.
type GatewayAPITrafficMapPublisher struct {
	publisher *GatewayAPIPublisher
}

var _ TrafficMapPublisher = (*GatewayAPITrafficMapPublisher)(nil)
var _ trafficMapPublisherPreflighter = (*GatewayAPITrafficMapPublisher)(nil)

// NewGatewayAPITrafficMapPublisher constructs the TrafficMap-owned Gateway API
// adapter. The client addresses the cluster that hosts the shared Gateway.
func NewGatewayAPITrafficMapPublisher(
	c client.Client,
	apiReader client.Reader,
	cfg Config,
	opts ...GatewayAPIPublisherOption,
) *GatewayAPITrafficMapPublisher {
	cfg.Labels = cloneGatewayAPILabels(cfg.Labels)
	gatewayOptions := append([]GatewayAPIPublisherOption(nil), opts...)
	gatewayOptions = append(gatewayOptions, WithGatewayAPIReader(apiReader))
	gatewayOptions = append(gatewayOptions, withStrictResourceOwnership())
	gatewayOptions = append(gatewayOptions, withTrafficMapBackendNaming())
	return &GatewayAPITrafficMapPublisher{
		publisher: NewGatewayAPIPublisher(c, cfg, gatewayOptions...),
	}
}

// Name implements TrafficMapPublisher.
func (p *GatewayAPITrafficMapPublisher) Name() string {
	return GatewayAPITrafficMapPublisherName
}

// Stateful implements TrafficMapPublisher.
func (*GatewayAPITrafficMapPublisher) Stateful() bool { return true }

// ResolveOptions implements TrafficMapPublisher. Gateway selection and backend
// behavior are installation-wide Config, so an InferenceService cannot alter
// the target scope through publisher options.
func (*GatewayAPITrafficMapPublisher) ResolveOptions(global, inline map[string]string) (map[string]string, error) {
	if len(inline) != 0 {
		return nil, unsupportedGatewayAPIOptions("per-InferenceService", inline)
	}
	if len(global) != 0 {
		return nil, unsupportedGatewayAPIOptions("global", global)
	}
	return map[string]string{}, nil
}

func unsupportedGatewayAPIOptions(scope string, options map[string]string) error {
	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return fmt.Errorf("Gateway API publisher does not accept %s options: %s", scope, strings.Join(keys, ", "))
}

type gatewayAPITrafficMapPlan struct {
	owner    *v1beta1.InferenceService
	target   Target
	routeKey types.NamespacedName
	publish  bool
}

func (p *gatewayAPITrafficMapPlan) Claims() []string {
	if p == nil {
		return nil
	}
	return []string{gatewayAPIHTTPRouteClaim(p.routeKey)}
}

// Plan implements TrafficMapPublisher. The returned plan owns all mutable
// values it contains, so later cache or caller mutations cannot alter Apply.
func (p *GatewayAPITrafficMapPublisher) Plan(
	owner *v1beta1.InferenceService,
	trafficMap *v1beta1.TrafficMap,
	effectiveOptions map[string]string,
) (TrafficMapPublishPlan, error) {
	if p == nil || p.publisher == nil {
		return nil, errors.New("Gateway API publisher is not configured")
	}
	if owner == nil {
		return nil, errors.New("InferenceService owner must not be nil")
	}
	if trafficMap == nil {
		return nil, errors.New("TrafficMap must not be nil")
	}
	if len(effectiveOptions) != 0 {
		return nil, unsupportedGatewayAPIOptions("effective", effectiveOptions)
	}
	ownerKey := client.ObjectKeyFromObject(owner)
	if err := validateGatewayAPISourceKey(ownerKey); err != nil {
		return nil, err
	}
	for _, finalizer := range owner.Finalizers {
		if finalizer == EndpointFinalizer {
			return nil, fmt.Errorf(
				"InferenceService %s still has legacy endpoint finalizer %q; complete its cleanup before publishing its TrafficMap",
				ownerKey, EndpointFinalizer,
			)
		}
	}
	if trafficMap.Namespace != owner.Namespace || trafficMap.Name != owner.Name || trafficMap.Spec.Service != owner.Name {
		return nil, fmt.Errorf("TrafficMap %s/%s does not identify InferenceService %s/%s",
			trafficMap.Namespace, trafficMap.Name, owner.Namespace, owner.Name)
	}

	ownerCopy := owner.DeepCopy()
	routeKey := types.NamespacedName{
		Namespace: p.publisher.routeNamespace(ownerCopy),
		Name:      p.publisher.routeName(ownerCopy),
	}
	if err := validateGatewayAPIRouteKey(routeKey); err != nil {
		return nil, err
	}

	target := Target{
		Service:              trafficMap.Spec.Service,
		WeightsAuthoritative: true,
	}
	if !p.publisher.config.IsEnabled() {
		return newGatewayAPIWithdrawalPlan(ownerCopy, target, routeKey), nil
	}
	globalHost, err := p.publisher.config.GlobalHostFor(ownerCopy)
	if err != nil {
		return nil, err
	}
	if globalHost == "" {
		return newGatewayAPIWithdrawalPlan(ownerCopy, target, routeKey), nil
	}
	if err := validateGatewayAPIHostname(globalHost, true); err != nil {
		return nil, fmt.Errorf("global hostname: %w", err)
	}
	if err := validateGatewayAPIPublishConfig(p.publisher.config); err != nil {
		return nil, err
	}
	if err := validateGatewayAPIResourceLabels(p.publisher.baseLabels(ownerCopy)); err != nil {
		return nil, err
	}
	if len(trafficMap.Spec.Entries) > maxGatewayAPIBackendRefs {
		return nil, fmt.Errorf("TrafficMap has %d entries, Gateway API HTTPRoute rules support at most %d backendRefs",
			len(trafficMap.Spec.Entries), maxGatewayAPIBackendRefs)
	}

	target.GlobalHost = globalHost
	target.Homes = make([]Home, 0, len(trafficMap.Spec.Entries))
	seenClusters := make(map[string]struct{}, len(trafficMap.Spec.Entries))
	seenServices := make(map[string]string, len(trafficMap.Spec.Entries))
	for _, entry := range trafficMap.Spec.Entries {
		if entry.Cluster == "" {
			return nil, errors.New("TrafficMap entry cluster must not be empty")
		}
		if problems := validation.IsDNS1123Subdomain(entry.Cluster); len(problems) != 0 {
			return nil, fmt.Errorf("TrafficMap entry cluster %q is invalid: %s",
				entry.Cluster, strings.Join(problems, "; "))
		}
		if _, duplicate := seenClusters[entry.Cluster]; duplicate {
			return nil, fmt.Errorf("TrafficMap contains more than one entry for cluster %q", entry.Cluster)
		}
		seenClusters[entry.Cluster] = struct{}{}
		if entry.Endpoint == nil || entry.Endpoint.Host == "" {
			return nil, fmt.Errorf("TrafficMap entry %q has no valid endpoint", entry.Cluster)
		}
		if entry.Weight < 0 || entry.Weight > maxGatewayAPIBackendWeight {
			return nil, fmt.Errorf("TrafficMap entry %q weight %d must be between 0 and %d",
				entry.Cluster, entry.Weight, maxGatewayAPIBackendWeight)
		}
		backendHost := hostOnly(entry.Endpoint.Host)
		if err := validateGatewayAPIHostname(backendHost, false); err != nil {
			return nil, fmt.Errorf("TrafficMap entry %q backend hostname: %w", entry.Cluster, err)
		}
		serviceName := p.publisher.serviceName(ownerCopy, entry.Cluster)
		if problems := validation.IsDNS1035Label(serviceName); len(problems) != 0 {
			return nil, fmt.Errorf("TrafficMap entry %q generates invalid backend Service name %q: %s",
				entry.Cluster, serviceName, strings.Join(problems, "; "))
		}
		if err := validateGatewayAPIResourceLabels(p.publisher.serviceLabels(ownerCopy, entry.Cluster)); err != nil {
			return nil, fmt.Errorf("TrafficMap entry %q: %w", entry.Cluster, err)
		}
		if otherCluster, duplicate := seenServices[serviceName]; duplicate {
			return nil, fmt.Errorf("TrafficMap entries %q and %q generate the same backend Service name %q",
				otherCluster, entry.Cluster, serviceName)
		}
		seenServices[serviceName] = entry.Cluster
		target.Homes = append(target.Homes, Home{
			Cluster:     entry.Cluster,
			Endpoint:    entry.Endpoint.String(),
			BackendHost: backendHost,
			Weight:      entry.Weight,
		})
	}
	sort.Slice(target.Homes, func(i, j int) bool { return target.Homes[i].Cluster < target.Homes[j].Cluster })
	return &gatewayAPITrafficMapPlan{
		owner:    ownerCopy,
		target:   target,
		routeKey: routeKey,
		publish:  true,
	}, nil
}

func newGatewayAPIWithdrawalPlan(
	owner *v1beta1.InferenceService,
	target Target,
	routeKey types.NamespacedName,
) *gatewayAPITrafficMapPlan {
	return &gatewayAPITrafficMapPlan{owner: owner, target: target, routeKey: routeKey}
}

// Preflight rejects an HTTPRoute hostname collision before Drain or Apply can
// change external state. The shared Gateway is a semantic target: two distinct
// catch-all routes for the same hostname are resolved by route precedence, not
// combined, so allowing both would silently leave one TrafficMap ineffective.
func (p *GatewayAPITrafficMapPublisher) Preflight(ctx context.Context, plan TrafficMapPublishPlan) error {
	if p == nil || p.publisher == nil {
		return errors.New("Gateway API publisher is not configured")
	}
	gatewayPlan, ok := plan.(*gatewayAPITrafficMapPlan)
	if !ok || gatewayPlan == nil || gatewayPlan.owner == nil {
		return fmt.Errorf("Gateway API publisher received plan of type %T", plan)
	}
	if !gatewayPlan.publish {
		return nil
	}
	if err := p.publisher.validateIO(); err != nil {
		return err
	}

	desiredRoute := &gatewayapiv1.HTTPRoute{}
	err := p.publisher.apiReader.Get(ctx, gatewayPlan.routeKey, desiredRoute)
	switch {
	case apierrors.IsNotFound(err):
		// The exact desired route is available.
	case err != nil:
		return fmt.Errorf("read desired global HTTPRoute %s: %w", gatewayPlan.routeKey, err)
	case !p.publisher.ownsResource(desiredRoute, gatewayPlan.owner):
		return &terminalPublisherError{err: fmt.Errorf(
			"global HTTPRoute %s belongs to another source", gatewayPlan.routeKey)}
	default:
		if err := requireActiveGatewayResource(desiredRoute, "HTTPRoute"); err != nil {
			return err
		}
	}

	routes := &gatewayapiv1.HTTPRouteList{}
	if err := p.publisher.apiReader.List(ctx, routes); err != nil {
		return fmt.Errorf("list HTTPRoutes for Gateway hostname ownership: %w", err)
	}
	gatewayKey := effectiveGatewayAPIKey(p.publisher.config.GlobalGateway, gatewayPlan.routeKey.Namespace)
	for i := range routes.Items {
		route := &routes.Items[i]
		routeKey := client.ObjectKeyFromObject(route)
		if routeKey == gatewayPlan.routeKey {
			// Never exclude the desired key solely by name: an unrelated object at
			// that key is a collision even when its hostname happens to differ.
			if !p.publisher.ownsResource(route, gatewayPlan.owner) {
				return &terminalPublisherError{err: fmt.Errorf(
					"global HTTPRoute %s belongs to another source", routeKey)}
			}
			continue
		}
		if gatewayAPIHTTPRoutePublishes(route, gatewayKey, gatewayPlan.target.GlobalHost) {
			return &terminalPublisherError{err: fmt.Errorf(
				"Gateway hostname %q on %s is already published by HTTPRoute %s",
				gatewayPlan.target.GlobalHost, gatewayKey, routeKey)}
		}
	}

	if err := p.publisher.preflightResourceKeys(ctx, gatewayPlan.owner, gatewayPlan.target); err != nil {
		var ownership *gatewayResourceOwnershipError
		if errors.As(err, &ownership) {
			return &terminalPublisherError{err: ownership}
		}
		return err
	}
	return nil
}

func effectiveGatewayAPIKey(globalGateway, routeNamespace string) types.NamespacedName {
	namespace, name := splitGatewayRef(globalGateway)
	if namespace == "" {
		namespace = routeNamespace
	}
	return types.NamespacedName{Namespace: namespace, Name: name}
}

func gatewayAPIHTTPRoutePublishes(
	route *gatewayapiv1.HTTPRoute,
	gatewayKey types.NamespacedName,
	hostname string,
) bool {
	if route == nil {
		return false
	}
	hostMatched := false
	for _, candidate := range route.Spec.Hostnames {
		if string(candidate) == hostname {
			hostMatched = true
			break
		}
	}
	if !hostMatched {
		return false
	}
	for _, parent := range route.Spec.ParentRefs {
		if parent.Group != nil && *parent.Group != gatewayapiv1.Group(gatewayapiv1.GroupVersion.Group) {
			continue
		}
		if parent.Kind != nil && *parent.Kind != gatewayapiv1.Kind(constants.GatewayKind) {
			continue
		}
		namespace := route.Namespace
		if parent.Namespace != nil && *parent.Namespace != "" {
			namespace = string(*parent.Namespace)
		}
		if (types.NamespacedName{Namespace: namespace, Name: string(parent.Name)}) == gatewayKey {
			return true
		}
	}
	return false
}

// Drain implements TrafficMapPublisher. Retired routes remain claimed and
// explicitly carry zero weights until final cleanup.
func (p *GatewayAPITrafficMapPublisher) Drain(
	ctx context.Context,
	sourceKey types.NamespacedName,
	claims []string,
) error {
	if p == nil || p.publisher == nil {
		return errors.New("Gateway API publisher is not configured")
	}
	if err := validateGatewayAPISourceKey(sourceKey); err != nil {
		return fmt.Errorf("validate Gateway API drain source: %w", err)
	}
	routeKeys, err := validateGatewayAPIHTTPRouteClaims(claims)
	if err != nil {
		return fmt.Errorf("validate stale Gateway API route claims: %w", err)
	}
	var errs []error
	for _, routeKey := range routeKeys {
		if err := p.publisher.zeroClaimedHTTPRoute(ctx, routeKey, sourceKey); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Apply implements TrafficMapPublisher.
func (p *GatewayAPITrafficMapPublisher) Apply(
	ctx context.Context,
	plan TrafficMapPublishPlan,
) (TrafficMapPublishResult, error) {
	if p == nil || p.publisher == nil {
		return TrafficMapPublishResult{}, errors.New("Gateway API publisher is not configured")
	}
	gatewayPlan, ok := plan.(*gatewayAPITrafficMapPlan)
	if !ok || gatewayPlan == nil || gatewayPlan.owner == nil {
		return TrafficMapPublishResult{}, fmt.Errorf("Gateway API publisher received plan of type %T", plan)
	}
	claims, err := validateGatewayAPIHTTPRouteClaims(gatewayPlan.Claims())
	if err != nil {
		return TrafficMapPublishResult{}, fmt.Errorf("validate Gateway API plan claim: %w", err)
	}
	if len(claims) != 1 || claims[0] != gatewayPlan.routeKey {
		return TrafficMapPublishResult{}, errors.New("Gateway API plan does not contain its route claim")
	}
	sourceKey := client.ObjectKeyFromObject(gatewayPlan.owner)
	expectedRouteKey := types.NamespacedName{
		Namespace: p.publisher.routeNamespace(gatewayPlan.owner),
		Name:      p.publisher.routeName(gatewayPlan.owner),
	}
	if expectedRouteKey != gatewayPlan.routeKey {
		return TrafficMapPublishResult{}, fmt.Errorf(
			"Gateway API plan route %s does not match configured route %s", gatewayPlan.routeKey, expectedRouteKey)
	}
	if !gatewayPlan.publish {
		if err := p.publisher.deleteClaimedHTTPRoute(ctx, gatewayPlan.routeKey, sourceKey); err != nil {
			return TrafficMapPublishResult{}, err
		}
		if err := p.publisher.deleteSourceResources(ctx, gatewayPlan.routeKey.Namespace, sourceKey); err != nil {
			return TrafficMapPublishResult{}, err
		}
		return TrafficMapPublishResult{Withdrawn: true}, nil
	}

	target := cloneGatewayAPITarget(gatewayPlan.target)
	if err := p.publisher.Publish(ctx, gatewayPlan.owner.DeepCopy(), target); err != nil {
		return TrafficMapPublishResult{}, err
	}
	return TrafficMapPublishResult{GatewayRef: &v1beta1.TrafficMapGatewayRef{
		Group:     gatewayapiv1.GroupVersion.Group,
		Kind:      gatewayAPIHTTPRouteKind,
		Namespace: gatewayPlan.routeKey.Namespace,
		Name:      gatewayPlan.routeKey.Name,
	}}, nil
}

// Unpublish implements TrafficMapPublisher. The source key and durable journal
// are sufficient to remove every published object after its owners disappear.
func (p *GatewayAPITrafficMapPublisher) Unpublish(
	ctx context.Context,
	sourceKey types.NamespacedName,
	journal v1beta1.TrafficMapPublisherStatus,
) error {
	if p == nil || p.publisher == nil {
		return errors.New("Gateway API publisher is not configured")
	}
	if journal.PublisherName != "" && journal.PublisherName != p.Name() {
		return fmt.Errorf("Gateway API publisher cannot clean journal owned by %q", journal.PublisherName)
	}
	if err := validateGatewayAPISourceKey(sourceKey); err != nil {
		return fmt.Errorf("validate Gateway API cleanup source: %w", err)
	}
	routeKeys, err := validateGatewayAPIHTTPRouteClaims(journal.ClaimedTargets)
	if err != nil {
		return fmt.Errorf("validate Gateway API cleanup journal: %w", err)
	}

	var routeErrs []error
	routeNamespaces := make(map[string]struct{}, len(routeKeys))
	for _, routeKey := range routeKeys {
		routeNamespaces[routeKey.Namespace] = struct{}{}
		if err := p.publisher.deleteClaimedHTTPRoute(ctx, routeKey, sourceKey); err != nil {
			routeErrs = append(routeErrs, err)
		}
	}
	if len(routeErrs) != 0 {
		return errors.Join(routeErrs...)
	}

	var errs []error
	namespaces := make([]string, 0, len(routeNamespaces))
	for routeNamespace := range routeNamespaces {
		namespaces = append(namespaces, routeNamespace)
	}
	sort.Strings(namespaces)
	for _, routeNamespace := range namespaces {
		if err := p.publisher.deleteSourceResources(ctx, routeNamespace, sourceKey); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func cloneGatewayAPITarget(target Target) Target {
	cloned := target
	cloned.Homes = append([]Home(nil), target.Homes...)
	return cloned
}

func gatewayAPIHTTPRouteClaim(key types.NamespacedName) string {
	return gatewayAPIHTTPRouteClaimPrefix + key.Namespace + "/" + key.Name
}

func validateGatewayAPIHTTPRouteClaims(claims []string) ([]types.NamespacedName, error) {
	if len(claims) > v1beta1.MaxTrafficMapPublisherTargets {
		return nil, fmt.Errorf("journal has %d targets, maximum is %d",
			len(claims), v1beta1.MaxTrafficMapPublisherTargets)
	}
	routeKeys := make([]types.NamespacedName, 0, len(claims))
	seen := make(map[string]struct{}, len(claims))
	for _, claim := range claims {
		if len(claim) > v1beta1.MaxTrafficMapPublisherTargetLength {
			return nil, fmt.Errorf("target %q has %d bytes, maximum is %d",
				claim, len(claim), v1beta1.MaxTrafficMapPublisherTargetLength)
		}
		if !strings.HasPrefix(claim, gatewayAPIHTTPRouteClaimPrefix) {
			return nil, fmt.Errorf("target %q has an unsupported claim version or kind", claim)
		}
		remainder := strings.TrimPrefix(claim, gatewayAPIHTTPRouteClaimPrefix)
		if strings.Count(remainder, "/") != 1 {
			return nil, fmt.Errorf("target %q must contain exactly one namespace/name separator", claim)
		}
		namespace, name, _ := strings.Cut(remainder, "/")
		routeKey := types.NamespacedName{Namespace: namespace, Name: name}
		if err := validateGatewayAPIRouteKey(routeKey); err != nil {
			return nil, fmt.Errorf("target %q: %w", claim, err)
		}
		if canonical := gatewayAPIHTTPRouteClaim(routeKey); canonical != claim {
			return nil, fmt.Errorf("target %q is not canonical", claim)
		}
		if _, duplicate := seen[claim]; duplicate {
			return nil, fmt.Errorf("target %q appears more than once", claim)
		}
		seen[claim] = struct{}{}
		routeKeys = append(routeKeys, routeKey)
	}
	sort.Slice(routeKeys, func(i, j int) bool {
		if routeKeys[i].Namespace == routeKeys[j].Namespace {
			return routeKeys[i].Name < routeKeys[j].Name
		}
		return routeKeys[i].Namespace < routeKeys[j].Namespace
	})
	return routeKeys, nil
}

func validateGatewayAPIRouteKey(key types.NamespacedName) error {
	if problems := validation.IsDNS1123Label(key.Namespace); len(problems) != 0 {
		return fmt.Errorf("HTTPRoute namespace %q is invalid: %s", key.Namespace, strings.Join(problems, "; "))
	}
	if problems := validation.IsDNS1123Subdomain(key.Name); len(problems) != 0 {
		return fmt.Errorf("HTTPRoute name %q is invalid: %s", key.Name, strings.Join(problems, "; "))
	}
	return nil
}

func validateGatewayAPISourceKey(key types.NamespacedName) error {
	if problems := validation.IsDNS1123Label(key.Namespace); len(problems) != 0 {
		return fmt.Errorf("source namespace %q is invalid: %s", key.Namespace, strings.Join(problems, "; "))
	}
	if problems := validation.IsDNS1123Subdomain(key.Name); len(problems) != 0 {
		return fmt.Errorf("source name %q is invalid: %s", key.Name, strings.Join(problems, "; "))
	}
	if problems := validation.IsValidLabelValue(key.Namespace); len(problems) != 0 {
		return fmt.Errorf("source namespace %q cannot be used as a label value: %s",
			key.Namespace, strings.Join(problems, "; "))
	}
	if problems := validation.IsValidLabelValue(key.Name); len(problems) != 0 {
		return fmt.Errorf("source name %q cannot be used as a label value: %s",
			key.Name, strings.Join(problems, "; "))
	}
	return nil
}

func validateGatewayAPIHostname(host string, wildcardAllowed bool) error {
	if wildcardAllowed && strings.HasPrefix(host, "*.") {
		host = strings.TrimPrefix(host, "*.")
	}
	if net.ParseIP(host) != nil {
		return fmt.Errorf("hostname %q must not be an IP address", host)
	}
	if problems := validation.IsDNS1123Subdomain(host); len(problems) != 0 {
		return fmt.Errorf("hostname %q is invalid: %s", host, strings.Join(problems, "; "))
	}
	return nil
}

func validateGatewayAPIPublishConfig(cfg Config) error {
	if problems := validation.IsValidPortNum(int(cfg.BackendPort)); len(problems) != 0 {
		return fmt.Errorf("Gateway API backend port %d is invalid: %s", cfg.BackendPort, strings.Join(problems, "; "))
	}
	gatewayRef := strings.TrimSpace(cfg.GlobalGateway)
	if gatewayRef != cfg.GlobalGateway {
		return fmt.Errorf("Gateway API global gateway reference %q is not canonical", cfg.GlobalGateway)
	}
	if strings.Count(gatewayRef, "/") > 1 {
		return fmt.Errorf("Gateway API global gateway reference %q must be a name or namespace/name", gatewayRef)
	}
	namespace, name := splitGatewayRef(gatewayRef)
	if strings.Contains(gatewayRef, "/") {
		if problems := validation.IsDNS1123Label(namespace); len(problems) != 0 {
			return fmt.Errorf("Gateway API global gateway namespace %q is invalid: %s",
				namespace, strings.Join(problems, "; "))
		}
	}
	if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
		return fmt.Errorf("Gateway API global gateway name %q is invalid: %s", name, strings.Join(problems, "; "))
	}
	if cfg.GatewayBackend.TLS.Enabled &&
		cfg.GatewayBackend.TLS.WellKnownCACertificates != string(gatewayapiv1.WellKnownCACertificatesSystem) {
		return fmt.Errorf("Gateway API backend TLS well-known CA certificates %q are unsupported",
			cfg.GatewayBackend.TLS.WellKnownCACertificates)
	}
	return nil
}

func validateGatewayAPIResourceLabels(labels map[string]string) error {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if problems := validation.IsQualifiedName(key); len(problems) != 0 {
			return fmt.Errorf("Gateway API resource label key %q is invalid: %s", key, strings.Join(problems, "; "))
		}
		if problems := validation.IsValidLabelValue(labels[key]); len(problems) != 0 {
			return fmt.Errorf("Gateway API resource label %q value %q is invalid: %s",
				key, labels[key], strings.Join(problems, "; "))
		}
	}
	return nil
}

func cloneGatewayAPILabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}
