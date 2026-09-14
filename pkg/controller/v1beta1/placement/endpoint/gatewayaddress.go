package endpoint

import (
	"context"
	"fmt"
	"net/netip"
	"sort"
	"strings"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

// BackendAddress is one directly-routable address advertised by a workload
// cluster Gateway, paired with the EndpointSlice address family it belongs to.
type BackendAddress struct {
	Address string
	Type    discoveryv1.AddressType
}

// BackendAddressResolver discovers the network addresses for one serving home.
// Implementations must return addresses reachable from the global Gateway.
type BackendAddressResolver interface {
	Resolve(context.Context, *v1beta1.InferenceService, Home) ([]BackendAddress, error)
}

// RemoteClusterClients is the workload-cluster client seam used by the address
// resolver. workloadcluster.Manager implements it; tests provide a small fake.
type RemoteClusterClients interface {
	ClientFor(string) (workloadcluster.SelectivelyCachingClient, bool)
}

// GatewayAddressResolver reads the generated HTTPRoute on a serving workload
// cluster, selects the parent Gateway corresponding to the home's hostname,
// and returns that Gateway's IP status addresses.
type GatewayAddressResolver struct {
	clusters RemoteClusterClients
}

// NewGatewayAddressResolver constructs a resolver backed by OME's existing
// workload-cluster connections.
func NewGatewayAddressResolver(clusters RemoteClusterClients) *GatewayAddressResolver {
	return &GatewayAddressResolver{clusters: clusters}
}

// Resolve returns only literal IPv4/IPv6 addresses. Hostname addresses are not
// resolved here: doing so would make EndpointSlices stale independently of the
// authoritative Gateway status and can produce cluster-local synthetic DNS
// answers instead of the advertised data-plane address.
func (r *GatewayAddressResolver) Resolve(ctx context.Context, isvc *v1beta1.InferenceService, home Home) ([]BackendAddress, error) {
	if r == nil || r.clusters == nil {
		return nil, fmt.Errorf("resolve backend addresses for cluster %q: workload-cluster clients are not configured", home.Cluster)
	}
	remote, ok := r.clusters.ClientFor(home.Cluster)
	if !ok {
		return nil, fmt.Errorf("resolve backend addresses for cluster %q: workload cluster is not connected", home.Cluster)
	}

	route := &gatewayapiv1.HTTPRoute{}
	key := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}
	if err := remote.Get(ctx, key, route); err != nil {
		return nil, fmt.Errorf("read serving HTTPRoute %s/%s on cluster %q: %w", key.Namespace, key.Name, home.Cluster, err)
	}
	parent, err := gatewayParentForHost(route, home.BackendHost)
	if err != nil {
		return nil, fmt.Errorf("resolve serving Gateway for host %q on cluster %q: %w", home.BackendHost, home.Cluster, err)
	}

	gwNamespace := route.Namespace
	if parent.Namespace != nil && *parent.Namespace != "" {
		gwNamespace = string(*parent.Namespace)
	}
	gateway := &gatewayapiv1.Gateway{}
	gwKey := types.NamespacedName{Namespace: gwNamespace, Name: string(parent.Name)}
	if err := remote.Get(ctx, gwKey, gateway); err != nil {
		return nil, fmt.Errorf("read serving Gateway %s/%s on cluster %q: %w", gwKey.Namespace, gwKey.Name, home.Cluster, err)
	}

	addresses := make([]BackendAddress, 0, len(gateway.Status.Addresses))
	seen := make(map[BackendAddress]struct{}, len(gateway.Status.Addresses))
	for _, statusAddress := range gateway.Status.Addresses {
		if statusAddress.Type != nil && *statusAddress.Type != gatewayapiv1.IPAddressType {
			continue
		}
		addr, err := netip.ParseAddr(strings.TrimSpace(statusAddress.Value))
		if err != nil {
			continue
		}
		addressType := discoveryv1.AddressTypeIPv6
		if addr.Is4() {
			addressType = discoveryv1.AddressTypeIPv4
		}
		resolved := BackendAddress{Address: addr.String(), Type: addressType}
		if _, exists := seen[resolved]; exists {
			continue
		}
		seen[resolved] = struct{}{}
		addresses = append(addresses, resolved)
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("serving Gateway %s/%s on cluster %q has no IP status addresses", gwKey.Namespace, gwKey.Name, home.Cluster)
	}
	sort.Slice(addresses, func(i, j int) bool {
		if addresses[i].Type != addresses[j].Type {
			return addresses[i].Type < addresses[j].Type
		}
		return addresses[i].Address < addresses[j].Address
	})
	return addresses, nil
}

// gatewayParentForHost selects the generated route's parent Gateway for a
// backend hostname. OME's workload-cluster HTTPRoute builder emits hostnames
// and parentRefs in matching order. A single parent is unambiguous regardless
// of the number of hostnames.
func gatewayParentForHost(route *gatewayapiv1.HTTPRoute, host string) (gatewayapiv1.ParentReference, error) {
	if len(route.Spec.ParentRefs) == 0 {
		return gatewayapiv1.ParentReference{}, fmt.Errorf("HTTPRoute %s/%s has no parentRefs", route.Namespace, route.Name)
	}
	hostIndexes := make([]int, 0, 1)
	want := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	for i, candidate := range route.Spec.Hostnames {
		got := strings.TrimSuffix(strings.ToLower(string(candidate)), ".")
		if got == want {
			hostIndexes = append(hostIndexes, i)
		}
	}
	if len(hostIndexes) == 0 {
		return gatewayapiv1.ParentReference{}, fmt.Errorf("HTTPRoute %s/%s does not publish hostname %q", route.Namespace, route.Name, host)
	}

	parentIndex := 0
	if len(route.Spec.ParentRefs) > 1 {
		if len(route.Spec.ParentRefs) != len(route.Spec.Hostnames) {
			return gatewayapiv1.ParentReference{}, fmt.Errorf("HTTPRoute %s/%s has %d parentRefs and %d hostnames; backend parent is ambiguous", route.Namespace, route.Name, len(route.Spec.ParentRefs), len(route.Spec.Hostnames))
		}
		if len(hostIndexes) != 1 {
			return gatewayapiv1.ParentReference{}, fmt.Errorf("HTTPRoute %s/%s publishes hostname %q for multiple parents", route.Namespace, route.Name, host)
		}
		parentIndex = hostIndexes[0]
	}
	parent := route.Spec.ParentRefs[parentIndex]
	if parent.Group != nil && *parent.Group != gatewayapiv1.Group(gatewayapiv1.GroupVersion.Group) {
		return gatewayapiv1.ParentReference{}, fmt.Errorf("parentRef %q has unsupported group %q", parent.Name, *parent.Group)
	}
	if parent.Kind != nil && *parent.Kind != "Gateway" {
		return gatewayapiv1.ParentReference{}, fmt.Errorf("parentRef %q has unsupported kind %q", parent.Name, *parent.Kind)
	}
	return parent, nil
}
