package endpoint

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

type fakeRemoteClusterClients map[string]workloadcluster.SelectivelyCachingClient

func (f fakeRemoteClusterClients) ClientFor(cluster string) (workloadcluster.SelectivelyCachingClient, bool) {
	c, ok := f[cluster]
	return c, ok
}

func remoteClient(t *testing.T, scheme *runtime.Scheme, objects ...runtime.Object) workloadcluster.SelectivelyCachingClient {
	t.Helper()
	builder := fakeclient.NewClientBuilder().WithScheme(scheme)
	for _, object := range objects {
		builder = builder.WithRuntimeObjects(object)
	}
	return workloadcluster.NewNeverCachingClient(builder.Build())
}

func TestGatewayAddressResolver_SelectsGatewayForBackendHost(t *testing.T) {
	scheme := pubScheme(t)
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
		"cluster-a": remoteClient(t, scheme, route, gateway),
	})

	addresses, err := resolver.Resolve(context.Background(), testISVC(), Home{
		Cluster: "cluster-a", BackendHost: "svc.external.example",
	})
	require.NoError(t, err)
	assert.Equal(t, []BackendAddress{
		{Address: "192.0.2.20", Type: discoveryv1.AddressTypeIPv4},
		{Address: "2001:db8::20", Type: discoveryv1.AddressTypeIPv6},
	}, addresses, "addresses are canonicalized, deduplicated, and family-sorted")
}

func TestGatewayAddressResolver_DefaultsParentNamespaceAndAddressType(t *testing.T) {
	scheme := pubScheme(t)
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
		"cluster-a": remoteClient(t, scheme, route, gateway),
	})

	addresses, err := resolver.Resolve(context.Background(), testISVC(), Home{
		Cluster: "cluster-a", BackendHost: "SVC.EXAMPLE.",
	})
	require.NoError(t, err)
	assert.Equal(t, []BackendAddress{{Address: "192.0.2.1", Type: discoveryv1.AddressTypeIPv4}}, addresses)
}

func TestGatewayAddressResolver_RejectsAmbiguousRoute(t *testing.T) {
	scheme := pubScheme(t)
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
		"cluster-a": remoteClient(t, scheme, route),
	})

	_, err := resolver.Resolve(context.Background(), testISVC(), Home{
		Cluster: "cluster-a", BackendHost: "svc.example",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous")
}

func TestGatewayAddressResolver_DisconnectedCluster(t *testing.T) {
	resolver := NewGatewayAddressResolver(fakeRemoteClusterClients{})
	_, err := resolver.Resolve(context.Background(), testISVC(), Home{Cluster: "missing", BackendHost: "svc.example"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not connected")
}

func TestGatewayParentForHostRejectsUnsupportedParent(t *testing.T) {
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
