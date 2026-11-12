package endpoint

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	// resourceSuffix names the backing resources the Gateway API publisher
	// creates for an InferenceService. The HTTPRoute is "<base>-global"; each
	// per-cluster ExternalName Service is "<base>-global-<cluster>" — distinct from
	// the per-cluster ingress resources OME's normal ingress reconciler emits
	// ("<isvc>", "<isvc>-engine", ...) so the two never collide.
	resourceSuffix = "-global"

	// TrafficMap backend names use a digest of the structured source identity.
	// The prefix is an internal naming-protocol discriminator, not a routing
	// value. Keeping the base short enough for an address-family suffix avoids a
	// second lossy transformation for EndpointSlices.
	trafficMapBackendNamePrefix = "tmb-"
	trafficMapBackendHashLength = 52
	trafficMapBackendNameDomain = "gateway-api-trafficmap-backend/v2"
)

// GatewayAPIPublisher implements EndpointPublisher by programming, on the
// control-plane cluster, one Gateway API HTTPRoute per InferenceService whose
// backends are Services representing each serving workload cluster's ingress
// host. In the portable default mode those Services are ExternalName aliases.
// Optional gateway-backend settings add per-cluster hostname rewrites, backend TLS,
// and direct addresses through EndpointSlices without a vendor-specific
// external-backend CRD.
//
// Single mode yields one backend Service and a single-backendRef route; All/Split
// yield one Service per cluster and a route that load-balances across all of them
// using their resolved traffic weights. As clusters come and go the Service set is
// reconciled — new clusters get a Service, departed clusters' Services are
// garbage-collected — and the route's backendRefs track the current set.
// Teardown removes the route and every per-cluster backing resource.
type GatewayAPIPublisher struct {
	client                 client.Client
	apiReader              client.Reader
	config                 Config
	addressResolver        BackendAddressResolver
	strictOwnership        bool
	trafficMapBackendNames bool
}

var _ EndpointPublisher = (*GatewayAPIPublisher)(nil)

// GatewayAPIPublisherOption configures an optional publisher dependency.
type GatewayAPIPublisherOption func(*GatewayAPIPublisher)

// WithBackendAddressResolver supplies the resolver used when gateway-backend
// publication is enabled.
func WithBackendAddressResolver(resolver BackendAddressResolver) GatewayAPIPublisherOption {
	return func(p *GatewayAPIPublisher) { p.addressResolver = resolver }
}

// withStrictResourceOwnership requires complete publication labels on every
// existing resource. TrafficMap claims use this mode to reject collisions.
func withStrictResourceOwnership() GatewayAPIPublisherOption {
	return func(p *GatewayAPIPublisher) { p.strictOwnership = true }
}

// withTrafficMapBackendNaming selects collision-resistant child-resource names
// for the TrafficMap lifecycle. HTTPRoute naming remains lifecycle-independent.
func withTrafficMapBackendNaming() GatewayAPIPublisherOption {
	return func(p *GatewayAPIPublisher) { p.trafficMapBackendNames = true }
}

// WithGatewayAPIReader supplies the uncached reader used for collision checks
// and cleanup discovery.
func WithGatewayAPIReader(apiReader client.Reader) GatewayAPIPublisherOption {
	return func(p *GatewayAPIPublisher) { p.apiReader = apiReader }
}

// NewGatewayAPIPublisher constructs the Gateway API backend. The client is the
// control-plane cluster client (the global gateway and the published resources
// live there).
func NewGatewayAPIPublisher(c client.Client, cfg Config, opts ...GatewayAPIPublisherOption) *GatewayAPIPublisher {
	p := &GatewayAPIPublisher{client: c, apiReader: c, config: cfg}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *GatewayAPIPublisher) Name() string { return "GatewayAPI" }

func (p *GatewayAPIPublisher) validateIO() error {
	if p == nil {
		return errors.New("Gateway API publisher is not configured")
	}
	if p.client == nil {
		return errors.New("Gateway API publisher client is not configured")
	}
	if p.apiReader == nil {
		return errors.New("Gateway API publisher API reader is not configured")
	}
	return nil
}

// deleteObservedObject fences deletion to the exact object version returned by
// the API reader. Kubernetes objects without server identity cannot be deleted
// safely because a same-key replacement may already exist.
func (p *GatewayAPIPublisher) deleteObservedObject(ctx context.Context, object client.Object) error {
	key := client.ObjectKeyFromObject(object)
	uid := object.GetUID()
	if uid == "" {
		return fmt.Errorf("delete observed %T %s: UID is empty", object, key)
	}
	resourceVersion := object.GetResourceVersion()
	if resourceVersion == "" {
		return fmt.Errorf("delete observed %T %s: resourceVersion is empty", object, key)
	}
	if err := p.client.Delete(ctx, object, client.Preconditions{
		UID:             &uid,
		ResourceVersion: &resourceVersion,
	}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// resourceBaseName is the stable base every published resource name is built
// from. A shared RouteNamespace co-locates resources from many source
// namespaces, so the base carries a digest of the fully-qualified source key
// there. A plain namespace-name join would not keep same-named sources apart:
// the hyphen is legal inside both, so (a, b-c) and (a-b, c) would collide.
func (p *GatewayAPIPublisher) resourceBaseName(isvc *v1beta1.InferenceService) string {
	if p.routeNamespace(isvc) != isvc.Namespace {
		sum := sha256.Sum256([]byte(isvc.Namespace + "/" + isvc.Name))
		return fmt.Sprintf("%s-%x", isvc.Name, sum[:4])
	}
	return isvc.Name
}

// boundedResourceName renders a published resource name as a valid DNS-1035
// label: an overflowing name collapses to a deterministic hashed form, and the
// result always starts with a letter, which Service names require.
func boundedResourceName(name string) string {
	bounded := constants.TruncateNameWithMaxLength(name, validation.DNS1035LabelMaxLength)
	if bounded != "" && (bounded[0] < 'a' || bounded[0] > 'z') {
		bounded = "a" + bounded
		bounded = constants.TruncateNameWithMaxLength(bounded, validation.DNS1035LabelMaxLength)
	}
	return bounded
}

// trafficMapBackendResourceName hashes a NUL-delimited identity tuple. Kubernetes
// source keys and cluster names cannot contain NUL, so tuple boundaries remain
// unambiguous without depending on punctuation that is valid inside a name.
func trafficMapBackendResourceName(isvc *v1beta1.InferenceService, cluster string) string {
	parts := []string{trafficMapBackendNameDomain, isvc.Namespace, isvc.Name, cluster}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	digest := fmt.Sprintf("%x", sum[:])
	return trafficMapBackendNamePrefix + digest[:trafficMapBackendHashLength]
}

// routeName is the HTTPRoute name for an InferenceService.
func (p *GatewayAPIPublisher) routeName(isvc *v1beta1.InferenceService) string {
	return boundedResourceName(p.resourceBaseName(isvc) + resourceSuffix)
}

// serviceName is the per-cluster ExternalName Service name for a cluster.
func (p *GatewayAPIPublisher) serviceName(isvc *v1beta1.InferenceService, cluster string) string {
	if p.trafficMapBackendNames {
		return trafficMapBackendResourceName(isvc, cluster)
	}
	return boundedResourceName(p.resourceBaseName(isvc) + resourceSuffix + "-" + cluster)
}

// routeNamespace resolves the namespace the published resources live in: the
// configured RouteNamespace, or the ISVC's own namespace when unset.
func (p *GatewayAPIPublisher) routeNamespace(isvc *v1beta1.InferenceService) string {
	if ns := strings.TrimSpace(p.config.RouteNamespace); ns != "" {
		return ns
	}
	return isvc.Namespace
}

// Publish reconciles the per-cluster backing resources and the HTTPRoute so the
// global host load-balances across exactly target.Clusters.
func (p *GatewayAPIPublisher) Publish(ctx context.Context, isvc *v1beta1.InferenceService, target Target) error {
	if err := p.validateIO(); err != nil {
		return err
	}
	addresses, err := p.resolveBackendAddresses(ctx, isvc, target)
	if err != nil {
		return err
	}
	if p.strictOwnership {
		if err := p.preflightResourceOwnership(ctx, isvc, target, addresses); err != nil {
			return err
		}
	}
	desired, err := p.ensureServices(ctx, isvc, target)
	if err != nil {
		return err
	}
	desiredSlices, desiredPolicies, err := p.ensureGatewayBackendResources(ctx, isvc, target, addresses)
	if err != nil {
		return err
	}
	if err := applyOwned(ctx, p, isvc, ownedHTTPRoute, p.buildHTTPRoute(isvc, target)); err != nil {
		return err
	}
	if err := p.pruneGatewayBackendResources(ctx, isvc, desiredSlices, desiredPolicies); err != nil {
		return err
	}
	return p.pruneServices(ctx, isvc, desired)
}

// Unpublish drains and removes the HTTPRoute before deleting its per-cluster
// resources. Missing resources are tolerated so repeated teardown is a no-op.
func (p *GatewayAPIPublisher) Unpublish(ctx context.Context, isvc *v1beta1.InferenceService) error {
	if err := p.validateIO(); err != nil {
		return err
	}
	ns := p.routeNamespace(isvc)
	sourceKey := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}
	routeKey := types.NamespacedName{Namespace: ns, Name: p.routeName(isvc)}
	// Delete the route only if it is ours. A shared RouteNamespace holds routes
	// for many sources, so a name match alone must not authorize a delete.
	existing := &gatewayapiv1.HTTPRoute{}
	switch err := p.apiReader.Get(ctx, routeKey, existing); {
	case apierrors.IsNotFound(err):
		// nothing to remove
	case err != nil:
		return fmt.Errorf("read global HTTPRoute %s/%s: %w", routeKey.Namespace, routeKey.Name, err)
	case !p.ownsResource(existing, isvc):
		return fmt.Errorf("global HTTPRoute %s/%s belongs to another InferenceService", routeKey.Namespace, routeKey.Name)
	default:
		if err := p.drainAndDeleteHTTPRoute(ctx, routeKey, existing, "global"); err != nil {
			return err
		}
	}
	return p.deleteSourceResources(ctx, ns, sourceKey)
}

// zeroClaimedHTTPRoute sets every backend weight on an explicitly claimed
// route to zero. Exact publication labels prevent a stale claim from mutating
// an unrelated route at the same key.
func (p *GatewayAPIPublisher) zeroClaimedHTTPRoute(
	ctx context.Context,
	routeKey types.NamespacedName,
	sourceKey types.NamespacedName,
) error {
	if err := p.validateIO(); err != nil {
		return err
	}
	route, err := p.getClaimedHTTPRoute(ctx, routeKey, sourceKey)
	if err != nil || route == nil {
		return err
	}
	if !zeroHTTPRouteBackendWeights(route) {
		return nil
	}
	if err := p.client.Update(ctx, route); err != nil {
		return fmt.Errorf("zero claimed HTTPRoute %s/%s backend weights: %w", routeKey.Namespace, routeKey.Name, err)
	}
	return nil
}

// deleteClaimedHTTPRoute deletes an explicitly claimed route when its complete
// publication labels identify the expected source. Missing routes are already
// clean.
func (p *GatewayAPIPublisher) deleteClaimedHTTPRoute(
	ctx context.Context,
	routeKey types.NamespacedName,
	sourceKey types.NamespacedName,
) error {
	if err := p.validateIO(); err != nil {
		return err
	}
	route, err := p.getClaimedHTTPRoute(ctx, routeKey, sourceKey)
	if err != nil || route == nil {
		return err
	}
	return p.drainAndDeleteHTTPRoute(ctx, routeKey, route, "claimed")
}

// drainAndDeleteHTTPRoute removes traffic before requesting deletion and waits
// for authoritative absence before subordinate resources may be removed.
func (p *GatewayAPIPublisher) drainAndDeleteHTTPRoute(
	ctx context.Context,
	routeKey types.NamespacedName,
	route *gatewayapiv1.HTTPRoute,
	description string,
) error {
	if zeroHTTPRouteBackendWeights(route) {
		if err := p.client.Update(ctx, route); err != nil {
			return fmt.Errorf("zero %s HTTPRoute %s/%s before deletion: %w",
				description, routeKey.Namespace, routeKey.Name, err)
		}
	}
	if route.DeletionTimestamp.IsZero() {
		if err := p.deleteObservedObject(ctx, route); err != nil {
			return fmt.Errorf("delete %s HTTPRoute %s/%s: %w", description, routeKey.Namespace, routeKey.Name, err)
		}
	}
	remaining := &gatewayapiv1.HTTPRoute{}
	if err := p.apiReader.Get(ctx, routeKey, remaining); apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("confirm %s HTTPRoute %s/%s deletion: %w", description, routeKey.Namespace, routeKey.Name, err)
	}
	return fmt.Errorf("%s HTTPRoute %s/%s deletion is pending", description, routeKey.Namespace, routeKey.Name)
}

func zeroHTTPRouteBackendWeights(route *gatewayapiv1.HTTPRoute) bool {
	changed := false
	for ruleIndex := range route.Spec.Rules {
		for backendIndex := range route.Spec.Rules[ruleIndex].BackendRefs {
			weight := route.Spec.Rules[ruleIndex].BackendRefs[backendIndex].Weight
			if weight != nil && *weight == 0 {
				continue
			}
			route.Spec.Rules[ruleIndex].BackendRefs[backendIndex].Weight = ptr.To(int32(0))
			changed = true
		}
	}
	return changed
}

func (p *GatewayAPIPublisher) getClaimedHTTPRoute(
	ctx context.Context,
	routeKey types.NamespacedName,
	sourceKey types.NamespacedName,
) (*gatewayapiv1.HTTPRoute, error) {
	route := &gatewayapiv1.HTTPRoute{}
	err := p.apiReader.Get(ctx, routeKey, route)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read claimed HTTPRoute %s/%s: %w", routeKey.Namespace, routeKey.Name, err)
	}
	labels := route.GetLabels()
	managedBy, hasManagedBy := labels[ManagedByLabel]
	sourceName, hasSourceName := labels[PlacementEndpointISVCLabel]
	sourceNamespace, hasSourceNamespace := labels[PlacementEndpointISVCNamespaceLabel]
	if !hasManagedBy || managedBy != ManagedByValue ||
		!hasSourceName || sourceName != sourceKey.Name ||
		!hasSourceNamespace || sourceNamespace != sourceKey.Namespace {
		return nil, fmt.Errorf(
			"claimed HTTPRoute %s/%s is not owned by source %s/%s",
			routeKey.Namespace, routeKey.Name, sourceKey.Namespace, sourceKey.Name,
		)
	}
	return route, nil
}

// deleteSourceResources removes subordinate resources selected by the complete
// publication label set for a source within one route namespace.
func (p *GatewayAPIPublisher) deleteSourceResources(
	ctx context.Context,
	routeNamespace string,
	sourceKey types.NamespacedName,
) error {
	if err := p.validateIO(); err != nil {
		return err
	}
	var errs []error
	if err := p.deleteGatewayBackendResourcesForSource(ctx, routeNamespace, sourceKey); err != nil {
		errs = append(errs, err)
	}
	owned, err := p.sourceServices(ctx, routeNamespace, sourceKey)
	if err != nil {
		errs = append(errs, err)
		return errors.Join(errs...)
	}
	for i := range owned {
		s := &owned[i]
		if err := p.deleteObservedObject(ctx, s); err != nil {
			errs = append(errs, fmt.Errorf("delete global backend Service %s/%s: %w", s.Namespace, s.Name, err))
		}
	}
	return errors.Join(errs...)
}

// ownedServices lists the per-cluster backend Services this publisher created for
// the ISVC (by the managed-by + per-ISVC name/namespace labels), so teardown and
// stale-cluster GC find them all regardless of how many clusters there are, and never
// reach a same-named ISVC from another namespace.
func (p *GatewayAPIPublisher) ownedServices(ctx context.Context, isvc *v1beta1.InferenceService) ([]corev1.Service, error) {
	return p.sourceServices(
		ctx,
		p.routeNamespace(isvc),
		types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name},
	)
}

func (p *GatewayAPIPublisher) sourceServices(
	ctx context.Context,
	routeNamespace string,
	sourceKey types.NamespacedName,
) ([]corev1.Service, error) {
	list := &corev1.ServiceList{}
	if err := p.apiReader.List(ctx, list, sourceListOptions(routeNamespace, sourceKey)...); err != nil {
		return nil, fmt.Errorf("list global backend Services for %s/%s: %w", sourceKey.Namespace, sourceKey.Name, err)
	}
	return list.Items, nil
}

// ensureServices creates or updates one ExternalName Service per cluster, before
// the route is changed, and returns the desired names for the prune step.
func (p *GatewayAPIPublisher) ensureServices(ctx context.Context, isvc *v1beta1.InferenceService, target Target) (map[string]struct{}, error) {
	desired := make(map[string]ServingCluster, len(target.Clusters))
	for _, h := range target.Clusters {
		desired[p.serviceName(isvc, h.Cluster)] = h
	}
	// Apply in a stable order so behavior is deterministic.
	names := make([]string, 0, len(desired))
	for n := range desired {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		service := p.buildExternalNameService(isvc, name, desired[name])
		if err := applyOwned(ctx, p, isvc, ownedBackendService, service); err != nil {
			return nil, err
		}
	}
	keep := make(map[string]struct{}, len(desired))
	for name := range desired {
		keep[name] = struct{}{}
	}
	return keep, nil
}

// pruneServices deletes owned Services no longer backing a cluster. It runs after
// the route drops their backendRefs, so the route never points at a Service that
// is already gone.
func (p *GatewayAPIPublisher) pruneServices(ctx context.Context, isvc *v1beta1.InferenceService, desired map[string]struct{}) error {
	owned, err := p.ownedServices(ctx, isvc)
	if err != nil {
		return err
	}
	for i := range owned {
		s := &owned[i]
		if _, keep := desired[s.Name]; keep {
			continue
		}
		if err := p.deleteObservedObject(ctx, s); err != nil {
			return fmt.Errorf("delete stale global backend Service %s/%s: %w", s.Namespace, s.Name, err)
		}
	}
	return nil
}

// ownedKind says how the publisher rewrites one kind of object it owns: the
// name its errors use, an empty object to read the live state into, the
// cluster-assigned fields it keeps from the live object because it does not
// own them, and the owned fields whose difference means an update.
type ownedKind[T client.Object] struct {
	name  string
	empty func() T
	keep  func(desired, live T)
	equal func(desired, live T) bool
}

var (
	ownedBackendService = ownedKind[*corev1.Service]{
		name:  "backend Service",
		empty: func() *corev1.Service { return &corev1.Service{} },
		keep: func(desired, live *corev1.Service) {
			desired.Spec.ClusterIP = live.Spec.ClusterIP
			desired.Spec.ClusterIPs = live.Spec.ClusterIPs
		},
		equal: func(desired, live *corev1.Service) bool {
			return equality.Semantic.DeepEqual(desired.Spec, live.Spec)
		},
	}
	ownedHTTPRoute = ownedKind[*gatewayapiv1.HTTPRoute]{
		name:  "HTTPRoute",
		empty: func() *gatewayapiv1.HTTPRoute { return &gatewayapiv1.HTTPRoute{} },
		equal: func(desired, live *gatewayapiv1.HTTPRoute) bool {
			return equality.Semantic.DeepEqual(desired.Spec, live.Spec)
		},
	}
)

// applyOwned creates desired when no object has its key. Otherwise it updates
// the live object in place, but only when this InferenceService owns it, it is
// not terminating, and its labels or owned fields differ. The live
// ResourceVersion, finalizers and kept fields go into the write.
func applyOwned[T client.Object](
	ctx context.Context,
	p *GatewayAPIPublisher,
	isvc *v1beta1.InferenceService,
	kind ownedKind[T],
	desired T,
) error {
	key := client.ObjectKeyFromObject(desired)
	live := kind.empty()
	err := p.apiReader.Get(ctx, key, live)
	if apierrors.IsNotFound(err) {
		if err := p.client.Create(ctx, desired); err != nil {
			return fmt.Errorf("create global %s %s/%s: %w", kind.name, key.Namespace, key.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read global %s %s/%s: %w", kind.name, key.Namespace, key.Name, err)
	}
	if !p.ownsResource(live, isvc) {
		return fmt.Errorf("global %s %s/%s belongs to another InferenceService", kind.name, key.Namespace, key.Name)
	}
	if err := requireActiveGatewayResource(live, kind.name); err != nil {
		return err
	}
	desired.SetResourceVersion(live.GetResourceVersion())
	desired.SetFinalizers(append([]string(nil), live.GetFinalizers()...))
	if kind.keep != nil {
		kind.keep(desired, live)
	}
	if kind.equal(desired, live) && maps.Equal(desired.GetLabels(), live.GetLabels()) {
		return nil
	}
	if err := p.client.Update(ctx, desired); err != nil {
		return fmt.Errorf("update global %s %s/%s: %w", kind.name, key.Namespace, key.Name, err)
	}
	return nil
}

// ownsResource checks publication ownership. Compatibility mode may adopt an
// object only when both source labels are absent; strict mode requires complete
// publisher and source labels.
func (p *GatewayAPIPublisher) ownsResource(obj metav1.Object, isvc *v1beta1.InferenceService) bool {
	l := obj.GetLabels()
	managedBy, hasManagedBy := l[ManagedByLabel]
	name, hasName := l[PlacementEndpointISVCLabel]
	ns, hasNS := l[PlacementEndpointISVCNamespaceLabel]
	if p.strictOwnership {
		return hasManagedBy && managedBy == ManagedByValue &&
			hasName && name == isvc.Name && hasNS && ns == isvc.Namespace
	}
	if !hasName && !hasNS {
		return true
	}
	return hasName && name == isvc.Name && hasNS && ns == isvc.Namespace
}

// preflightResourceOwnership checks every exact object the publish may create
// or update before any write, with the EndpointSlices the resolved addresses
// produce.
func (p *GatewayAPIPublisher) preflightResourceOwnership(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	target Target,
	addresses map[string][]BackendAddress,
) error {
	return p.preflightPublishedObjects(ctx, isvc, target, func(serviceName string, cluster ServingCluster) ([]string, error) {
		slices, err := p.buildEndpointSlices(isvc, serviceName, cluster, addresses[cluster.Cluster])
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(slices))
		for _, endpointSlice := range slices {
			names = append(names, endpointSlice.Name)
		}
		return names, nil
	})
}

// preflightResourceKeys checks every object key an active publication can use
// without resolving backend addresses. Both possible EndpointSlice family keys
// are reserved so this check stays independent of a changing resolver result.
func (p *GatewayAPIPublisher) preflightResourceKeys(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	target Target,
) error {
	return p.preflightPublishedObjects(ctx, isvc, target, func(serviceName string, _ ServingCluster) ([]string, error) {
		return []string{
			boundedResourceName(serviceName + ipv4EndpointSliceSuffix),
			boundedResourceName(serviceName + ipv6EndpointSliceSuffix),
		}, nil
	})
}

// preflightPublishedObjects reads every object a publication of target may
// create or update and fails on the first that belongs to another source or is
// terminating: the HTTPRoute, then per cluster in name order the backend
// Service, the EndpointSlices sliceNames returns for it, and its
// BackendTLSPolicy. Per-object update checks remain authoritative if another
// actor creates or replaces an object after this read-only pass.
func (p *GatewayAPIPublisher) preflightPublishedObjects(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	target Target,
	sliceNames func(serviceName string, cluster ServingCluster) ([]string, error),
) error {
	route := p.buildHTTPRoute(isvc, target)
	if err := p.requireOwnedOrAbsent(ctx, route, "HTTPRoute", isvc); err != nil {
		return err
	}
	for _, cluster := range sortedClusters(target) {
		if err := p.preflightClusterObjects(ctx, isvc, cluster, sliceNames); err != nil {
			return err
		}
	}
	return nil
}

// preflightClusterObjects reads one cluster's backend Service, then the
// EndpointSlices sliceNames returns for it when direct addresses are
// enabled, then its BackendTLSPolicy when backend TLS is enabled, and fails
// on the first that belongs to another source or is terminating.
func (p *GatewayAPIPublisher) preflightClusterObjects(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	cluster ServingCluster,
	sliceNames func(serviceName string, cluster ServingCluster) ([]string, error),
) error {
	namespace := p.routeNamespace(isvc)
	serviceName := p.serviceName(isvc, cluster.Cluster)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: serviceName}}
	if err := p.requireOwnedOrAbsent(ctx, service, "backend Service", isvc); err != nil {
		return err
	}
	if p.config.GatewayBackend.EndpointSlices.Enabled {
		names, err := sliceNames(serviceName, cluster)
		if err != nil {
			return err
		}
		for _, name := range names {
			endpointSlice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
			if err := p.requireOwnedOrAbsent(ctx, endpointSlice, "backend EndpointSlice", isvc); err != nil {
				return err
			}
		}
	}
	if p.config.GatewayBackend.TLS.Enabled {
		policy := &gatewayapiv1.BackendTLSPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: serviceName}}
		if err := p.requireOwnedOrAbsent(ctx, policy, "BackendTLSPolicy", isvc); err != nil {
			return err
		}
	}
	return nil
}

func (p *GatewayAPIPublisher) requireOwnedOrAbsent(
	ctx context.Context,
	object client.Object,
	kind string,
	isvc *v1beta1.InferenceService,
) error {
	key := client.ObjectKeyFromObject(object)
	if err := p.apiReader.Get(ctx, key, object); apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("read global %s %s/%s: %w", kind, key.Namespace, key.Name, err)
	}
	if !p.ownsResource(object, isvc) {
		return &gatewayResourceOwnershipError{kind: kind, key: key}
	}
	return requireActiveGatewayResource(object, kind)
}

type gatewayResourceOwnershipError struct {
	kind string
	key  types.NamespacedName
}

func (e *gatewayResourceOwnershipError) Error() string {
	return fmt.Sprintf("global %s %s/%s belongs to another source", e.kind, e.key.Namespace, e.key.Name)
}

func requireActiveGatewayResource(object metav1.Object, kind string) error {
	timestamp := object.GetDeletionTimestamp()
	if timestamp == nil || timestamp.IsZero() {
		return nil
	}
	return fmt.Errorf("global %s %s/%s is terminating", kind, object.GetNamespace(), object.GetName())
}

// buildExternalNameService renders the ExternalName Service that aliases one
// cluster's ingress host. The externalName is repointed if the cluster's
// backend changes; the Service name is stable per cluster so the HTTPRoute
// backendRef for that cluster never has to change.
func (p *GatewayAPIPublisher) buildExternalNameService(isvc *v1beta1.InferenceService, name string, cluster ServingCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: p.routeNamespace(isvc),
			Labels:    p.serviceLabels(isvc, cluster.Cluster),
		},
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeExternalName,
			ExternalName:    cluster.BackendHost,
			SessionAffinity: corev1.ServiceAffinityNone,
			Ports: []corev1.ServicePort{{
				Port:       p.config.BackendPort,
				Protocol:   corev1.ProtocolTCP,
				TargetPort: intstr.FromInt32(p.config.BackendPort),
			}},
		},
	}
}

// buildHTTPRoute renders the global HTTPRoute: it attaches to the configured
// global gateway, matches the global host at root, and forwards to one backend
// per cluster (each the cluster's ExternalName Service), equal weight. Clusters are
// sorted by cluster so the backendRef order is deterministic (stable idempotency
// comparisons).
func (p *GatewayAPIPublisher) buildHTTPRoute(isvc *v1beta1.InferenceService, target Target) *gatewayapiv1.HTTPRoute {
	gwNS, gwName := splitGatewayRef(p.config.GlobalGateway)
	backendNS := p.routeNamespace(isvc)
	port := gatewayapiv1.PortNumber(p.config.BackendPort)
	parentRef := gatewayapiv1.ParentReference{
		Group: (*gatewayapiv1.Group)(&gatewayapiv1.GroupVersion.Group),
		Kind:  (*gatewayapiv1.Kind)(ptr.To(constants.GatewayKind)),
		Name:  gatewayapiv1.ObjectName(gwName),
	}
	if gwNS != "" {
		parentRef.Namespace = ptr.To(gatewayapiv1.Namespace(gwNS))
	}

	clusters := sortedClusters(target)

	// Traffic weight per cluster, resolved upstream: the TrafficMap's capacity-aware
	// weight when routing is on, else the reactive ready-replica count (a cluster
	// with 0 ready gets 0 — no traffic until it is serving). When no cluster carries
	// a weight (Single/All, or a Split placement before any cluster is ready) every
	// weight is zero; Gateway API sends no traffic if ALL backendRef weights are
	// zero, so legacy inputs fall back to equal weight (1 each). TrafficMap
	// weights are authoritative and can intentionally keep every arm at zero.
	var totalWeight int32
	for _, h := range clusters {
		totalWeight += h.Weight
	}
	refs := make([]gatewayapiv1.HTTPBackendRef, 0, len(clusters))
	for _, h := range clusters {
		weight := int32(1)
		if totalWeight > 0 || target.WeightsAuthoritative {
			weight = h.Weight
		}
		ref := gatewayapiv1.HTTPBackendRef{
			BackendRef: gatewayapiv1.BackendRef{
				BackendObjectReference: gatewayapiv1.BackendObjectReference{
					Group:     ptr.To(gatewayapiv1.Group(corev1.GroupName)),
					Kind:      ptr.To(gatewayapiv1.Kind(constants.ServiceKind)),
					Name:      gatewayapiv1.ObjectName(p.serviceName(isvc, h.Cluster)),
					Namespace: (*gatewayapiv1.Namespace)(&backendNS),
					Port:      &port,
				},
				Weight: ptr.To(weight),
			},
		}
		if p.config.GatewayBackend.RewriteHostname {
			ref.Filters = []gatewayapiv1.HTTPRouteFilter{{
				Type: gatewayapiv1.HTTPRouteFilterURLRewrite,
				URLRewrite: &gatewayapiv1.HTTPURLRewriteFilter{
					Hostname: ptr.To(gatewayapiv1.PreciseHostname(h.BackendHost)),
				},
			}}
		}
		refs = append(refs, ref)
	}

	return &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.routeName(isvc),
			Namespace: backendNS,
			Labels:    p.baseLabels(isvc),
		},
		Spec: gatewayapiv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{
				ParentRefs: []gatewayapiv1.ParentReference{parentRef},
			},
			Hostnames: []gatewayapiv1.Hostname{gatewayapiv1.Hostname(target.GlobalHost)},
			Rules: []gatewayapiv1.HTTPRouteRule{{
				Matches: []gatewayapiv1.HTTPRouteMatch{{
					Path: &gatewayapiv1.HTTPPathMatch{
						Type:  ptr.To(gatewayapiv1.PathMatchPathPrefix),
						Value: ptr.To("/"),
					},
				}},
				BackendRefs: refs,
			}},
		},
	}
}

// baseLabels are the ownership markers stamped on every published resource
// (route + Services): the managed-by marker plus the per-ISVC name/namespace
// grouping labels, merged over the operator-configured Labels (owned keys win).
func (p *GatewayAPIPublisher) baseLabels(isvc *v1beta1.InferenceService) map[string]string {
	out := map[string]string{}
	maps.Copy(out, p.config.Labels)
	out[ManagedByLabel] = ManagedByValue
	out[PlacementEndpointISVCLabel] = isvc.Name
	out[PlacementEndpointISVCNamespaceLabel] = isvc.Namespace
	return out
}

// serviceLabels are baseLabels plus the cluster label (a backend Service
// points at exactly one cluster).
func (p *GatewayAPIPublisher) serviceLabels(isvc *v1beta1.InferenceService, cluster string) map[string]string {
	out := p.baseLabels(isvc)
	if cluster != "" {
		out[PlacementClusterLabel] = cluster
	}
	return out
}

// splitGatewayRef parses a "namespace/name" gateway reference. A bare "name"
// (no slash) resolves to an empty namespace, which Gateway API treats as the
// route's own namespace.
func splitGatewayRef(ref string) (namespace, name string) {
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}
