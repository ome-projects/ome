package endpoint

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	homes := append([]Home(nil), target.Homes...)
	sort.Slice(homes, func(i, j int) bool { return homes[i].Cluster < homes[j].Cluster })
	addresses := make(map[string][]BackendAddress, len(homes))
	for _, home := range homes {
		resolved, err := p.addressResolver.Resolve(ctx, isvc, home)
		if err != nil {
			return nil, fmt.Errorf("resolve backend addresses for cluster %q: %w", home.Cluster, err)
		}
		if len(resolved) == 0 {
			return nil, fmt.Errorf("resolve backend addresses for cluster %q: resolver returned no addresses", home.Cluster)
		}
		addresses[home.Cluster] = resolved
	}
	return addresses, nil
}

// ensureGatewayBackendResources creates or updates the direct EndpointSlices
// and TLS policies before the HTTPRoute starts referencing their Services.
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

	homes := append([]Home(nil), target.Homes...)
	sort.Slice(homes, func(i, j int) bool { return homes[i].Cluster < homes[j].Cluster })
	for _, home := range homes {
		serviceName := p.serviceName(isvc, home.Cluster)
		if p.config.GatewayBackend.EndpointSlices.Enabled {
			slices, err := p.buildEndpointSlices(isvc, serviceName, home, addresses[home.Cluster])
			if err != nil {
				return nil, nil, err
			}
			for _, endpointSlice := range slices {
				if err := p.applyEndpointSlice(ctx, isvc, endpointSlice); err != nil {
					return nil, nil, err
				}
				desiredSlices[endpointSlice.Name] = struct{}{}
			}
		}

		if p.config.GatewayBackend.TLS.Enabled {
			policy := p.buildBackendTLSPolicy(isvc, serviceName, home)
			if err := p.applyBackendTLSPolicy(ctx, isvc, policy); err != nil {
				return nil, nil, err
			}
			desiredPolicies[policy.Name] = struct{}{}
		}
	}
	return desiredSlices, desiredPolicies, nil
}

func (p *GatewayAPIPublisher) buildEndpointSlices(
	isvc *v1beta1.InferenceService,
	serviceName string,
	home Home,
	addresses []BackendAddress,
) ([]*discoveryv1.EndpointSlice, error) {
	byFamily := map[discoveryv1.AddressType]map[string]struct{}{}
	for _, address := range addresses {
		switch address.Type {
		case discoveryv1.AddressTypeIPv4, discoveryv1.AddressTypeIPv6:
		default:
			return nil, fmt.Errorf("build EndpointSlice for cluster %q: unsupported address type %q", home.Cluster, address.Type)
		}
		parsed, err := netip.ParseAddr(address.Address)
		if err != nil || parsed.Zone() != "" {
			return nil, fmt.Errorf("build EndpointSlice for cluster %q: invalid %s address %q", home.Cluster, address.Type, address.Address)
		}
		parsed = parsed.Unmap()
		if (address.Type == discoveryv1.AddressTypeIPv4) != parsed.Is4() {
			return nil, fmt.Errorf("build EndpointSlice for cluster %q: address %q does not match type %q", home.Cluster, address.Address, address.Type)
		}
		if byFamily[address.Type] == nil {
			byFamily[address.Type] = map[string]struct{}{}
		}
		byFamily[address.Type][parsed.String()] = struct{}{}
	}

	var slices []*discoveryv1.EndpointSlice
	for _, addressType := range []discoveryv1.AddressType{discoveryv1.AddressTypeIPv4, discoveryv1.AddressTypeIPv6} {
		familyAddressSet := byFamily[addressType]
		if len(familyAddressSet) == 0 {
			continue
		}
		familyAddresses := make([]string, 0, len(familyAddressSet))
		for address := range familyAddressSet {
			familyAddresses = append(familyAddresses, address)
		}
		sort.Strings(familyAddresses)
		endpoints := make([]discoveryv1.Endpoint, 0, len(familyAddresses))
		for _, address := range familyAddresses {
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
		labels := p.serviceLabels(isvc, home.Cluster)
		labels[discoveryv1.LabelServiceName] = serviceName
		labels[discoveryv1.LabelManagedBy] = ManagedByValue
		slices = append(slices, &discoveryv1.EndpointSlice{
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
		})
	}
	if len(slices) == 0 {
		return nil, fmt.Errorf("build EndpointSlice for cluster %q: no IPv4 or IPv6 addresses", home.Cluster)
	}
	return slices, nil
}

func (p *GatewayAPIPublisher) buildBackendTLSPolicy(
	isvc *v1beta1.InferenceService,
	serviceName string,
	home Home,
) *gatewayapiv1.BackendTLSPolicy {
	caSet := gatewayapiv1.WellKnownCACertificatesType(p.config.GatewayBackend.TLS.WellKnownCACertificates)
	return &gatewayapiv1.BackendTLSPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: p.routeNamespace(isvc),
			Labels:    p.serviceLabels(isvc, home.Cluster),
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
				Hostname:                gatewayapiv1.PreciseHostname(home.BackendHost),
			},
		},
	}
}

func (p *GatewayAPIPublisher) applyEndpointSlice(ctx context.Context, isvc *v1beta1.InferenceService, desired *discoveryv1.EndpointSlice) error {
	existing := &discoveryv1.EndpointSlice{}
	err := p.client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := p.client.Create(ctx, desired); err != nil {
			return fmt.Errorf("create global backend EndpointSlice %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read global backend EndpointSlice %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if !p.ownsResource(existing, isvc) {
		return fmt.Errorf("global backend EndpointSlice %s/%s belongs to another InferenceService", desired.Namespace, desired.Name)
	}
	desired.ResourceVersion = existing.ResourceVersion
	if desired.AddressType == existing.AddressType &&
		equality.Semantic.DeepEqual(desired.Endpoints, existing.Endpoints) &&
		equality.Semantic.DeepEqual(desired.Ports, existing.Ports) &&
		maps.Equal(desired.Labels, existing.Labels) {
		return nil
	}
	if err := p.client.Update(ctx, desired); err != nil {
		return fmt.Errorf("update global backend EndpointSlice %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

func (p *GatewayAPIPublisher) applyBackendTLSPolicy(ctx context.Context, isvc *v1beta1.InferenceService, desired *gatewayapiv1.BackendTLSPolicy) error {
	existing := &gatewayapiv1.BackendTLSPolicy{}
	err := p.client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		if err := p.client.Create(ctx, desired); err != nil {
			return fmt.Errorf("create global BackendTLSPolicy %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read global BackendTLSPolicy %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	if !p.ownsResource(existing, isvc) {
		return fmt.Errorf("global BackendTLSPolicy %s/%s belongs to another InferenceService", desired.Namespace, desired.Name)
	}
	desired.ResourceVersion = existing.ResourceVersion
	if equality.Semantic.DeepEqual(desired.Spec, existing.Spec) && maps.Equal(desired.Labels, existing.Labels) {
		return nil
	}
	if err := p.client.Update(ctx, desired); err != nil {
		return fmt.Errorf("update global BackendTLSPolicy %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

func (p *GatewayAPIPublisher) ownedEndpointSlices(ctx context.Context, isvc *v1beta1.InferenceService) ([]discoveryv1.EndpointSlice, error) {
	list := &discoveryv1.EndpointSliceList{}
	if err := p.client.List(ctx, list, p.ownedListOptions(isvc)...); err != nil {
		return nil, fmt.Errorf("list global backend EndpointSlices for %s/%s: %w", isvc.Namespace, isvc.Name, err)
	}
	return list.Items, nil
}

func (p *GatewayAPIPublisher) ownedBackendTLSPolicies(ctx context.Context, isvc *v1beta1.InferenceService) ([]gatewayapiv1.BackendTLSPolicy, error) {
	list := &gatewayapiv1.BackendTLSPolicyList{}
	if err := p.client.List(ctx, list, p.ownedListOptions(isvc)...); err != nil {
		// The policy CRD is optional. Absence means there is no policy object to
		// clean up. The enabled create path reports a missing CRD explicitly.
		if apimeta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return []gatewayapiv1.BackendTLSPolicy{}, nil
		}
		return nil, fmt.Errorf("list global BackendTLSPolicies for %s/%s: %w", isvc.Namespace, isvc.Name, err)
	}
	return list.Items, nil
}

func (p *GatewayAPIPublisher) ownedListOptions(isvc *v1beta1.InferenceService) []client.ListOption {
	return []client.ListOption{
		client.InNamespace(p.routeNamespace(isvc)),
		client.MatchingLabels{
			ManagedByLabel:                      ManagedByValue,
			PlacementEndpointISVCLabel:          isvc.Name,
			PlacementEndpointISVCNamespaceLabel: isvc.Namespace,
		},
	}
}

func (p *GatewayAPIPublisher) pruneGatewayBackendResources(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	desiredSlices map[string]struct{},
	desiredPolicies map[string]struct{},
) error {
	var errs []error
	slices, err := p.ownedEndpointSlices(ctx, isvc)
	if err != nil {
		errs = append(errs, err)
	} else {
		for i := range slices {
			if _, keep := desiredSlices[slices[i].Name]; keep {
				continue
			}
			if err := p.client.Delete(ctx, &slices[i]); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("delete stale global backend EndpointSlice %s/%s: %w", slices[i].Namespace, slices[i].Name, err))
			}
		}
	}
	policies, err := p.ownedBackendTLSPolicies(ctx, isvc)
	if err != nil {
		errs = append(errs, err)
	} else {
		for i := range policies {
			if _, keep := desiredPolicies[policies[i].Name]; keep {
				continue
			}
			if err := p.client.Delete(ctx, &policies[i]); err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, fmt.Errorf("delete stale global BackendTLSPolicy %s/%s: %w", policies[i].Namespace, policies[i].Name, err))
			}
		}
	}
	return errors.Join(errs...)
}

func (p *GatewayAPIPublisher) deleteGatewayBackendResources(ctx context.Context, isvc *v1beta1.InferenceService) error {
	return p.pruneGatewayBackendResources(ctx, isvc, map[string]struct{}{}, map[string]struct{}{})
}
