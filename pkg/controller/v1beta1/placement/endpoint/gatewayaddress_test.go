package endpoint

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

type fakeRemoteClusterClients map[string]workloadcluster.SelectivelyCachingClient

func (f fakeRemoteClusterClients) ClientFor(cluster string) (workloadcluster.SelectivelyCachingClient, bool) {
	c, ok := f[cluster]
	return c, ok
}

func remoteClient(t *testing.T, objects ...client.Object) workloadcluster.SelectivelyCachingClient {
	t.Helper()
	return workloadcluster.NewNeverCachingClient(fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install), fakeclient.WithObjects(objects...)))
}

func TestGatewayAddressResolver_SelectsGatewayForBackendHost(t *testing.T) {
	gwGroup := gatewayapiv1.Group(gatewayapiv1.GroupVersion.Group)
	gwKind := gatewayapiv1.Kind("Gateway")
	gwNamespace := gatewayapiv1.Namespace("gateway-system")
	route := &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
		Spec: gatewayapiv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{ParentRefs: []gatewayapiv1.ParentReference{
				{Group: &gwGroup, Kind: &gwKind, Namespace: &gwNamespace, Name: "internal-gw"},
				{Group: &gwGroup, Kind: &gwKind, Namespace: &gwNamespace, Name: "external-gw"},
			}},
			Hostnames: []gatewayapiv1.Hostname{"svc.internal.example", "svc.external.example"},
		},
	}
	hostnameType := gatewayapiv1.HostnameAddressType
	ipType := gatewayapiv1.IPAddressType
	gateway := &gatewayapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "external-gw", Namespace: "gateway-system"},
		Status: gatewayapiv1.GatewayStatus{Addresses: []gatewayapiv1.GatewayStatusAddress{
			{Type: &ipType, Value: "2001:0db8:0:0::20"},
			{Value: "192.0.2.20"},
			{Type: &ipType, Value: "192.0.2.20"},
			{Type: &hostnameType, Value: "gateway.example"},
		}},
	}
	resolver := NewGatewayAddressResolver(fakeRemoteClusterClients{
		"cluster-a": remoteClient(t, route, gateway),
	})

	addresses, err := resolver.Resolve(context.Background(), testISVC(), ServingCluster{
		Cluster: "cluster-a", BackendHost: "svc.external.example",
	})
	require.NoError(t, err)
	assert.Equal(t, []BackendAddress{
		{Address: "192.0.2.20", Type: discoveryv1.AddressTypeIPv4},
		{Address: "2001:db8::20", Type: discoveryv1.AddressTypeIPv6},
	}, addresses, "addresses are canonicalized, deduplicated, and family-sorted")
}

func TestGatewayAddressResolver_DefaultsParentNamespaceAndAddressType(t *testing.T) {
	route := &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
		Spec: gatewayapiv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{ParentRefs: []gatewayapiv1.ParentReference{{Name: "gw"}}},
			Hostnames:       []gatewayapiv1.Hostname{"svc.example"},
		},
	}
	gateway := &gatewayapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "prod"},
		Status:     gatewayapiv1.GatewayStatus{Addresses: []gatewayapiv1.GatewayStatusAddress{{Value: "192.0.2.1"}}},
	}
	resolver := NewGatewayAddressResolver(fakeRemoteClusterClients{
		"cluster-a": remoteClient(t, route, gateway),
	})

	addresses, err := resolver.Resolve(context.Background(), testISVC(), ServingCluster{
		Cluster: "cluster-a", BackendHost: "SVC.EXAMPLE.",
	})
	require.NoError(t, err)
	assert.Equal(t, []BackendAddress{{Address: "192.0.2.1", Type: discoveryv1.AddressTypeIPv4}}, addresses)
}

func TestGatewayAddressResolver_RejectsAmbiguousRoute(t *testing.T) {
	route := &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
		Spec: gatewayapiv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{ParentRefs: []gatewayapiv1.ParentReference{
				{Name: "gw-a"}, {Name: "gw-b"},
			}},
			Hostnames: []gatewayapiv1.Hostname{"svc.example"},
		},
	}
	resolver := NewGatewayAddressResolver(fakeRemoteClusterClients{
		"cluster-a": remoteClient(t, route),
	})

	_, err := resolver.Resolve(context.Background(), testISVC(), ServingCluster{
		Cluster: "cluster-a", BackendHost: "svc.example",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous")
}

func TestGatewayAddressResolver_DisconnectedCluster(t *testing.T) {
	resolver := NewGatewayAddressResolver(fakeRemoteClusterClients{})
	_, err := resolver.Resolve(context.Background(), testISVC(), ServingCluster{Cluster: "missing", BackendHost: "svc.example"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestGatewayParentForHost_ParentOfUnsupportedGroup_ReturnsError(t *testing.T) {
	group := gatewayapiv1.Group("example.io")
	route := &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
		Spec: gatewayapiv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{ParentRefs: []gatewayapiv1.ParentReference{{
				Group: &group, Name: "not-a-gateway",
			}}},
			Hostnames: []gatewayapiv1.Hostname{"svc.example"},
		},
	}

	_, err := gatewayParentForHost(route, "svc.example")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported group")
}

// resolveGatewayAddresses resolves the backend host of a route whose one
// parent names gateway, both held by cluster-a.
func resolveGatewayAddresses(t *testing.T, host string, parent gatewayapiv1.ParentReference, gateway *gatewayapiv1.Gateway) ([]BackendAddress, error) {
	t.Helper()
	route := &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
		Spec: gatewayapiv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{ParentRefs: []gatewayapiv1.ParentReference{parent}},
			Hostnames:       []gatewayapiv1.Hostname{gatewayapiv1.Hostname(host)},
		},
	}
	resolver := NewGatewayAddressResolver(fakeRemoteClusterClients{"cluster-a": remoteClient(t, route, gateway)})
	return resolver.Resolve(context.Background(), testISVC(), ServingCluster{Cluster: "cluster-a", BackendHost: host})
}

// The resolver keeps the Gateway's IP status addresses and nothing else: a
// value under the Hostname or Named type, a value that is no address and a
// repeat are dropped, padding is trimmed, and a Gateway without one IP
// address is refused.
func TestGatewayAddressResolver_AddressShapes_KeepsOnlyDistinctIPAddresses(t *testing.T) {
	ipType, hostnameType, namedType := gatewayapiv1.IPAddressType, gatewayapiv1.HostnameAddressType, gatewayapiv1.NamedAddressType
	ipv4 := BackendAddress{Address: "192.0.2.21", Type: discoveryv1.AddressTypeIPv4}
	for name, tt := range map[string]struct {
		statuses []gatewayapiv1.GatewayStatusAddress
		want     []BackendAddress
		wantErr  string
	}{
		"padding and the long IPv6 form": {
			statuses: []gatewayapiv1.GatewayStatusAddress{{Value: " 192.0.2.21 "}, {Type: &ipType, Value: "2001:0db8:0:0::20"}},
			want:     []BackendAddress{ipv4, {Address: "2001:db8::20", Type: discoveryv1.AddressTypeIPv6}},
		},
		"an IP under the Hostname type": {
			statuses: []gatewayapiv1.GatewayStatusAddress{{Type: &hostnameType, Value: "192.0.2.20"}, {Value: "192.0.2.21"}},
			want:     []BackendAddress{ipv4},
		},
		"an IP under the Named type": {
			statuses: []gatewayapiv1.GatewayStatusAddress{{Type: &namedType, Value: "192.0.2.20"}, {Value: "192.0.2.21"}},
			want:     []BackendAddress{ipv4},
		},
		"values that are no address": {
			statuses: []gatewayapiv1.GatewayStatusAddress{{Value: "300.1.1.1"}, {Value: "gateway.example"}, {Value: ""}, {Value: "192.0.2.21"}},
			want:     []BackendAddress{ipv4},
		},
		"the same address twice": {
			statuses: []gatewayapiv1.GatewayStatusAddress{{Value: "192.0.2.21"}, {Type: &ipType, Value: "192.0.2.21"}},
			want:     []BackendAddress{ipv4},
		},
		"only a hostname": {
			statuses: []gatewayapiv1.GatewayStatusAddress{{Type: &hostnameType, Value: "gateway.example"}},
			wantErr:  `serving Gateway prod/gw on cluster "cluster-a" has no IP status addresses`,
		},
		"no status address": {
			wantErr: `serving Gateway prod/gw on cluster "cluster-a" has no IP status addresses`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			gateway := &gatewayapiv1.Gateway{
				ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "prod"},
				Status:     gatewayapiv1.GatewayStatus{Addresses: tt.statuses},
			}
			got, err := resolveGatewayAddresses(t, "svc.example", gatewayapiv1.ParentReference{Name: "gw"}, gateway)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("Resolve() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A parent reference whose namespace is the empty string names a Gateway in
// the route's namespace, as a parent without a namespace does.
func TestGatewayAddressResolver_EmptyParentNamespace_ReadsTheRouteNamespace(t *testing.T) {
	empty := gatewayapiv1.Namespace("")
	gateway := &gatewayapiv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "gw", Namespace: "prod"},
		Status:     gatewayapiv1.GatewayStatus{Addresses: []gatewayapiv1.GatewayStatusAddress{{Value: "192.0.2.1"}}},
	}
	got, err := resolveGatewayAddresses(t, "svc.example", gatewayapiv1.ParentReference{Name: "gw", Namespace: &empty}, gateway)
	require.NoError(t, err)
	assert.Equal(t, []BackendAddress{{Address: "192.0.2.1", Type: discoveryv1.AddressTypeIPv4}}, got)
}

// A resolver without workload-cluster clients, and a nil resolver, refuse
// every cluster with a text that says the clients are not configured.
func TestGatewayAddressResolver_NoClusterClients_RefusesWithText(t *testing.T) {
	var nilResolver *GatewayAddressResolver
	want := `resolve backend addresses for cluster "cluster-a": workload-cluster clients are not configured`
	for name, resolver := range map[string]*GatewayAddressResolver{"no clients": NewGatewayAddressResolver(nil), "nil resolver": nilResolver} {
		t.Run(name, func(t *testing.T) {
			_, err := resolver.Resolve(context.Background(), testISVC(), ServingCluster{Cluster: "cluster-a", BackendHost: "svc.example"})
			if err == nil || err.Error() != want {
				t.Fatalf("Resolve() error = %v, want %q", err, want)
			}
		})
	}
}
