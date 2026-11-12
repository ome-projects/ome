package endpoint

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sort"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	ipv4EndpointSliceSuffix = "-ipv4"
	ipv6EndpointSliceSuffix = "-ipv6"
)

func (p *GatewayAPIPublisher) resolveBackendAddresses(ctx context.Context, isvc *v1beta1.InferenceService, target Target) (map[string][]BackendAddress, error) {
	if !p.config.GatewayBackend.EndpointSlices.Enabled {
		return nil, nil
	}
	if p.addressResolver == nil {
		return nil, errors.New("gateway backend publication is enabled but no backend address resolver is configured")
	}
	clusters := sortedClusters(target)
	addresses := make(map[string][]BackendAddress, len(clusters))
	for _, cluster := range clusters {
		resolved, err := p.addressResolver.Resolve(ctx, isvc, cluster)
		if err != nil {
			return nil, fmt.Errorf("resolve backend addresses for cluster %q: %w", cluster.Cluster, err)
		}
		if len(resolved) == 0 {
			return nil, fmt.Errorf("resolve backend addresses for cluster %q: resolver returned no addresses", cluster.Cluster)
		}
		addresses[cluster.Cluster] = resolved
	}
	return addresses, nil
}

// sortedClusters returns the target's clusters in name order; the target's
// own list is left as it is.
func sortedClusters(target Target) []ServingCluster {
	clusters := append([]ServingCluster(nil), target.Clusters...)
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].Cluster < clusters[j].Cluster })
	return clusters
}

// ensureGatewayBackendResources creates or updates the direct EndpointSlices
// and TLS policies before the HTTPRoute starts referencing their Services.
// It returns the names of the EndpointSlices and the policies the target
// wants, so the prune step can tell them from stale ones.
func (p *GatewayAPIPublisher) ensureGatewayBackendResources(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	target Target,
	addresses map[string][]BackendAddress,
) (map[string]struct{}, map[string]struct{}, error) {
	desiredSlices := map[string]struct{}{}
	desiredPolicies := map[string]struct{}{}
	if !p.config.GatewayBackend.EndpointSlices.Enabled && !p.config.GatewayBackend.TLS.Enabled {
		return desiredSlices, desiredPolicies, nil
	}
	for _, cluster := range sortedClusters(target) {
		if err := p.ensureClusterBackend(ctx, isvc, cluster, addresses[cluster.Cluster], desiredSlices, desiredPolicies); err != nil {
			return nil, nil, err
		}
	}
	return desiredSlices, desiredPolicies, nil
}

// ensureClusterBackend writes one cluster's EndpointSlices and its
// BackendTLSPolicy, each when its configuration block is enabled, and adds
// the names it wrote to desiredSlices and desiredPolicies.
func (p *GatewayAPIPublisher) ensureClusterBackend(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	cluster ServingCluster,
	addresses []BackendAddress,
	desiredSlices, desiredPolicies map[string]struct{},
) error {
	serviceName := p.serviceName(isvc, cluster.Cluster)
	if p.config.GatewayBackend.EndpointSlices.Enabled {
		endpointSlices, err := p.buildEndpointSlices(isvc, serviceName, cluster, addresses)
		if err != nil {
			return err
		}
		for _, endpointSlice := range endpointSlices {
			if err := applyOwned(ctx, p, isvc, ownedEndpointSlice, endpointSlice); err != nil {
				return err
			}
			desiredSlices[endpointSlice.Name] = struct{}{}
		}
	}
	if p.config.GatewayBackend.TLS.Enabled {
		policy := p.buildBackendTLSPolicy(isvc, serviceName, cluster)
		if err := applyOwned(ctx, p, isvc, ownedBackendTLSPolicy, policy); err != nil {
			return err
		}
		desiredPolicies[policy.Name] = struct{}{}
	}
	return nil
}

// buildEndpointSlices builds one EndpointSlice per address family the
// cluster's addresses use, IPv4 before IPv6; a cluster with no usable address
// is an error, as is one address of the wrong shape.
func (p *GatewayAPIPublisher) buildEndpointSlices(
	isvc *v1beta1.InferenceService,
	serviceName string,
	cluster ServingCluster,
	addresses []BackendAddress,
) ([]*discoveryv1.EndpointSlice, error) {
	byFamily, err := backendAddressesByFamily(cluster.Cluster, addresses)
	if err != nil {
		return nil, err
	}
	var endpointSlices []*discoveryv1.EndpointSlice
	for _, addressType := range []discoveryv1.AddressType{discoveryv1.AddressTypeIPv4, discoveryv1.AddressTypeIPv6} {
		if len(byFamily[addressType]) == 0 {
			continue
		}
		endpointSlices = append(endpointSlices, p.endpointSliceFor(isvc, serviceName, cluster.Cluster, addressType, byFamily[addressType]))
	}
	if len(endpointSlices) == 0 {
		return nil, fmt.Errorf("build EndpointSlice for cluster %q: no IPv4 or IPv6 addresses", cluster.Cluster)
	}
	return endpointSlices, nil
}

// backendAddressesByFamily checks every address and groups the distinct
// canonical addresses by family, each family's list sorted.
func backendAddressesByFamily(cluster string, addresses []BackendAddress) (map[discoveryv1.AddressType][]string, error) {
	byFamily := map[discoveryv1.AddressType]map[string]struct{}{}
	for _, address := range addresses {
		parsed, err := parseBackendAddress(cluster, address)
		if err != nil {
			return nil, err
		}
		if byFamily[address.Type] == nil {
			byFamily[address.Type] = map[string]struct{}{}
		}
		byFamily[address.Type][parsed.String()] = struct{}{}
	}
	sorted := make(map[discoveryv1.AddressType][]string, len(byFamily))
	for addressType, familyAddressSet := range byFamily {
		familyAddresses := make([]string, 0, len(familyAddressSet))
		for address := range familyAddressSet {
			familyAddresses = append(familyAddresses, address)
		}
		sort.Strings(familyAddresses)
		sorted[addressType] = familyAddresses
	}
	return sorted, nil
}

// parseBackendAddress reads one backend address: its family is IPv4 or
// IPv6, the address parses without a zone, and its form matches the family
// once an IPv4-mapped IPv6 address is unmapped.
func parseBackendAddress(cluster string, address BackendAddress) (netip.Addr, error) {
	switch address.Type {
	case discoveryv1.AddressTypeIPv4, discoveryv1.AddressTypeIPv6:
	default:
		return netip.Addr{}, fmt.Errorf("build EndpointSlice for cluster %q: unsupported address type %q", cluster, address.Type)
	}
	parsed, err := netip.ParseAddr(address.Address)
	if err != nil || parsed.Zone() != "" {
		return netip.Addr{}, fmt.Errorf("build EndpointSlice for cluster %q: invalid %s address %q", cluster, address.Type, address.Address)
	}
	parsed = parsed.Unmap()
	if (address.Type == discoveryv1.AddressTypeIPv4) != parsed.Is4() {
		return netip.Addr{}, fmt.Errorf("build EndpointSlice for cluster %q: address %q does not match type %q", cluster, address.Address, address.Type)
	}
	return parsed, nil
}

// endpointSliceFor is the EndpointSlice of one address family: a ready,
// serving endpoint per address, the backend port, and the Service and
// managed-by labels beside the publication labels.
func (p *GatewayAPIPublisher) endpointSliceFor(
	isvc *v1beta1.InferenceService,
	serviceName string,
	cluster string,
	addressType discoveryv1.AddressType,
	addresses []string,
) *discoveryv1.EndpointSlice {
	endpoints := make([]discoveryv1.Endpoint, 0, len(addresses))
	for _, address := range addresses {
		endpoints = append(endpoints, discoveryv1.Endpoint{
			Addresses: []string{address},
			Conditions: discoveryv1.EndpointConditions{
				Ready:       ptr.To(true),
				Serving:     ptr.To(true),
				Terminating: ptr.To(false),
			},
		})
	}
	suffix := ipv6EndpointSliceSuffix
	if addressType == discoveryv1.AddressTypeIPv4 {
		suffix = ipv4EndpointSliceSuffix
	}
	labels := p.serviceLabels(isvc, cluster)
	labels[discoveryv1.LabelServiceName] = serviceName
	labels[discoveryv1.LabelManagedBy] = ManagedByValue
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      boundedResourceName(serviceName + suffix),
			Namespace: p.routeNamespace(isvc),
			Labels:    labels,
		},
		AddressType: addressType,
		Endpoints:   endpoints,
		Ports: []discoveryv1.EndpointPort{{
			Protocol: ptr.To(corev1.ProtocolTCP),
			Port:     ptr.To(p.config.BackendPort),
		}},
	}
}

func (p *GatewayAPIPublisher) buildBackendTLSPolicy(
	isvc *v1beta1.InferenceService,
	serviceName string,
	cluster ServingCluster,
) *gatewayapiv1.BackendTLSPolicy {
	caSet := gatewayapiv1.WellKnownCACertificatesType(p.config.GatewayBackend.TLS.WellKnownCACertificates)
	return &gatewayapiv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: p.routeNamespace(isvc),
			Labels:    p.serviceLabels(isvc, cluster.Cluster),
		},
		Spec: gatewayapiv1.BackendTLSPolicySpec{
			TargetRefs: []gatewayapiv1.LocalPolicyTargetReferenceWithSectionName{{
				LocalPolicyTargetReference: gatewayapiv1.LocalPolicyTargetReference{
					Group: gatewayapiv1.Group(corev1.GroupName),
					Kind:  gatewayapiv1.Kind(constants.ServiceKind),
					Name:  gatewayapiv1.ObjectName(serviceName),
				},
			}},
			Validation: gatewayapiv1.BackendTLSPolicyValidation{
				WellKnownCACertificates: &caSet,
				Hostname:                gatewayapiv1.PreciseHostname(cluster.BackendHost),
			},
		},
	}
}

var (
	ownedEndpointSlice = ownedKind[*discoveryv1.EndpointSlice]{
		name:  "backend EndpointSlice",
		empty: func() *discoveryv1.EndpointSlice { return &discoveryv1.EndpointSlice{} },
		equal: func(desired, live *discoveryv1.EndpointSlice) bool {
			return desired.AddressType == live.AddressType &&
				equality.Semantic.DeepEqual(desired.Endpoints, live.Endpoints) &&
				equality.Semantic.DeepEqual(desired.Ports, live.Ports)
		},
	}
	ownedBackendTLSPolicy = ownedKind[*gatewayapiv1.BackendTLSPolicy]{
		name:  "BackendTLSPolicy",
		empty: func() *gatewayapiv1.BackendTLSPolicy { return &gatewayapiv1.BackendTLSPolicy{} },
		equal: func(desired, live *gatewayapiv1.BackendTLSPolicy) bool {
			return equality.Semantic.DeepEqual(desired.Spec, live.Spec)
		},
	}
)

func (p *GatewayAPIPublisher) sourceEndpointSlices(
	ctx context.Context,
	routeNamespace string,
	sourceKey types.NamespacedName,
) ([]discoveryv1.EndpointSlice, error) {
	list := &discoveryv1.EndpointSliceList{}
	if err := p.apiReader.List(ctx, list, sourceListOptions(routeNamespace, sourceKey)...); err != nil {
		return nil, fmt.Errorf("list global backend EndpointSlices for %s/%s: %w", sourceKey.Namespace, sourceKey.Name, err)
	}
	return list.Items, nil
}

func (p *GatewayAPIPublisher) sourceBackendTLSPolicies(
	ctx context.Context,
	routeNamespace string,
	sourceKey types.NamespacedName,
) ([]gatewayapiv1.BackendTLSPolicy, error) {
	list := &gatewayapiv1.BackendTLSPolicyList{}
	if err := p.apiReader.List(ctx, list, sourceListOptions(routeNamespace, sourceKey)...); err != nil {
		// The policy CRD is optional. Absence means there is no policy object to
		// clean up. The enabled create path reports a missing CRD explicitly.
		if apimeta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return []gatewayapiv1.BackendTLSPolicy{}, nil
		}
		return nil, fmt.Errorf("list global BackendTLSPolicies for %s/%s: %w", sourceKey.Namespace, sourceKey.Name, err)
	}
	return list.Items, nil
}

func sourceListOptions(routeNamespace string, sourceKey types.NamespacedName) []client.ListOption {
	return []client.ListOption{
		client.InNamespace(routeNamespace),
		client.MatchingLabels{
			ManagedByLabel:                      ManagedByValue,
			PlacementEndpointISVCLabel:          sourceKey.Name,
			PlacementEndpointISVCNamespaceLabel: sourceKey.Namespace,
		},
	}
}

func (p *GatewayAPIPublisher) pruneGatewayBackendResources(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	desiredSlices map[string]struct{},
	desiredPolicies map[string]struct{},
) error {
	return p.pruneGatewayBackendResourcesForSource(
		ctx,
		p.routeNamespace(isvc),
		types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name},
		desiredSlices,
		desiredPolicies,
	)
}

func (p *GatewayAPIPublisher) pruneGatewayBackendResourcesForSource(
	ctx context.Context,
	routeNamespace string,
	sourceKey types.NamespacedName,
	desiredSlices map[string]struct{},
	desiredPolicies map[string]struct{},
) error {
	var errs []error
	slices, err := p.sourceEndpointSlices(ctx, routeNamespace, sourceKey)
	if err != nil {
		errs = append(errs, err)
	} else {
		for i := range slices {
			if _, keep := desiredSlices[slices[i].Name]; keep {
				continue
			}
			if err := p.deleteObservedObject(ctx, &slices[i]); err != nil {
				errs = append(errs, fmt.Errorf("delete stale global backend EndpointSlice %s/%s: %w", slices[i].Namespace, slices[i].Name, err))
			}
		}
	}
	policies, err := p.sourceBackendTLSPolicies(ctx, routeNamespace, sourceKey)
	if err != nil {
		errs = append(errs, err)
	} else {
		for i := range policies {
			if _, keep := desiredPolicies[policies[i].Name]; keep {
				continue
			}
			if err := p.deleteObservedObject(ctx, &policies[i]); err != nil {
				errs = append(errs, fmt.Errorf("delete stale global BackendTLSPolicy %s/%s: %w", policies[i].Namespace, policies[i].Name, err))
			}
		}
	}
	return errors.Join(errs...)
}

func (p *GatewayAPIPublisher) deleteGatewayBackendResourcesForSource(
	ctx context.Context,
	routeNamespace string,
	sourceKey types.NamespacedName,
) error {
	return p.pruneGatewayBackendResourcesForSource(
		ctx,
		routeNamespace,
		sourceKey,
		map[string]struct{}{},
		map[string]struct{}{},
	)
}
