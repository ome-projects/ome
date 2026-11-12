package endpoint

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var gatewayFakeUIDCounter atomic.Uint64

// gatewayClient builds the fake client of the Gateway API tests: the
// gateway.networking.k8s.io group beside the built-ins and ome.io, and a UID
// on every object created through it, as the apiserver sets one.
func gatewayClient(t *testing.T, opts ...fakeclient.Option) client.WithWatch {
	t.Helper()
	return fakeclient.New(t, append([]fakeclient.Option{
		fakeclient.WithTypes(gatewayapiv1.Install),
		fakeclient.WithInterceptors(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
				setGatewayFakeObjectIdentity(object)
				return c.Create(ctx, object, opts...)
			},
		}),
	}, opts...)...)
}

func setGatewayFakeObjectIdentity(objects ...client.Object) {
	for _, object := range objects {
		if object.GetUID() == "" {
			object.SetUID(types.UID("gateway-test-" + strconv.FormatUint(gatewayFakeUIDCounter.Add(1), 10)))
		}
	}
}

// routeUpdatesRefused fails every HTTPRoute update, so a Publish that cannot
// repoint the route is observable.
func routeUpdatesRefused() fakeclient.Option {
	return fakeclient.FailingVerb(fakeclient.Update, fakeclient.EveryCall, errors.New("route update failed")).
		OnKind(&gatewayapiv1.HTTPRoute{})
}

// blindCache reads as a controller-runtime cache that has observed nothing of
// what the direct API reader can see: every Get is NotFound and every List is
// empty. Writes reach the store.
func blindCache() fakeclient.Option {
	return fakeclient.WithInterceptors(interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
			return apierrors.NewNotFound(schema.GroupResource{Resource: "objects"}, key.Name)
		},
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return nil
		},
	})
}

func testISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
		Spec: v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{
			Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle,
		}},
	}
}

func baseConfig() Config {
	return Config{
		GlobalHostTemplate: "{{.Name}}.{{.Namespace}}.global.example",
		GlobalGateway:      "ome-system/global-gw",
		BackendPort:        8080,
		Labels:             map[string]string{"team": "platform"},
	}
}

func gatewayBackendConfig() Config {
	cfg := baseConfig()
	cfg.BackendPort = 443
	cfg.GatewayBackend = GatewayBackendConfig{
		RewriteHostname: true,
		TLS: GatewayBackendTLSConfig{
			Enabled:                 true,
			WellKnownCACertificates: string(gatewayapiv1.WellKnownCACertificatesSystem),
		},
		EndpointSlices: GatewayBackendEndpointSliceConfig{Enabled: true},
	}
	return cfg
}

type staticBackendAddressResolver map[string][]BackendAddress

func (r staticBackendAddressResolver) Resolve(_ context.Context, _ *v1beta1.InferenceService, cluster ServingCluster) ([]BackendAddress, error) {
	return r[cluster.Cluster], nil
}

// oneCluster builds a single-cluster Target (Single mode shape).
func oneCluster(globalHost, cluster, backendHost string) Target {
	return Target{GlobalHost: globalHost, Clusters: []ServingCluster{{Cluster: cluster, BackendHost: backendHost}}}
}

func TestBuildExternalNameService_OneCluster_NamesLabelsAndAliasesTheHost(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	isvc := testISVC()
	cluster := ServingCluster{Cluster: "cluster-a", BackendHost: "svc.prod.cloud-a.example"}

	svc := p.buildExternalNameService(isvc, p.serviceName(isvc, "cluster-a"), cluster)

	assert.Equal(t, "svc-global-cluster-a", svc.Name, "per-cluster Service name carries the cluster")
	assert.Equal(t, "prod", svc.Namespace, "namespace falls back to the ISVC namespace")
	assert.Equal(t, corev1.ServiceTypeExternalName, svc.Spec.Type)
	assert.Equal(t, "svc.prod.cloud-a.example", svc.Spec.ExternalName, "ExternalName aliases the cluster ingress host")
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(8080), svc.Spec.Ports[0].Port)
	assert.Equal(t, ManagedByValue, svc.Labels[ManagedByLabel])
	assert.Equal(t, "cluster-a", svc.Labels[PlacementClusterLabel])
	assert.Equal(t, "svc", svc.Labels[PlacementEndpointISVCLabel], "per-ISVC grouping label for GC/teardown")
	assert.Equal(t, "prod", svc.Labels[PlacementEndpointISVCNamespaceLabel])
	assert.Equal(t, "platform", svc.Labels["team"], "operator labels are merged in")
}

func TestBuildHTTPRoute_SingleCluster(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())

	route := p.buildHTTPRoute(testISVC(), oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example"))

	assert.Equal(t, "svc-global", route.Name)
	assert.Equal(t, "prod", route.Namespace)
	require.Len(t, route.Spec.Hostnames, 1)
	assert.Equal(t, gatewayapiv1.Hostname("svc.prod.global.example"), route.Spec.Hostnames[0])
	assert.Equal(t, "svc", route.Labels[PlacementEndpointISVCLabel])
	assert.Equal(t, "prod", route.Labels[PlacementEndpointISVCNamespaceLabel])

	require.Len(t, route.Spec.ParentRefs, 1)
	pr := route.Spec.ParentRefs[0]
	require.NotNil(t, pr.Namespace)
	assert.Equal(t, "ome-system", string(*pr.Namespace), "gateway namespace parsed from namespace/name")
	assert.Equal(t, "global-gw", string(pr.Name))
	require.NotNil(t, pr.Kind)
	assert.Equal(t, constants.GatewayKind, string(*pr.Kind))

	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	br := route.Spec.Rules[0].BackendRefs[0]
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-a"), br.Name, "route forwards to the cluster's ExternalName Service")
	require.NotNil(t, br.Kind)
	assert.Equal(t, constants.ServiceKind, string(*br.Kind))
	require.NotNil(t, br.Port)
	assert.Equal(t, gatewayapiv1.PortNumber(8080), *br.Port)
	require.NotNil(t, br.Weight)
	assert.Equal(t, int32(1), *br.Weight)
	assert.Empty(t, br.Filters, "portable ExternalName mode keeps backend filters optional")

	require.Len(t, route.Spec.Rules[0].Matches, 1)
	require.NotNil(t, route.Spec.Rules[0].Matches[0].Path)
	assert.Equal(t, "/", *route.Spec.Rules[0].Matches[0].Path.Value)
}

func TestBuildHTTPRoute_MultiClusterEqualWeightSorted(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	// Clusters passed out of order; the route's backendRefs must be sorted by cluster.
	target := Target{GlobalHost: "h", Clusters: []ServingCluster{
		{Cluster: "cluster-b", BackendHost: "b.example"},
		{Cluster: "cluster-a", BackendHost: "a.example"},
	}}

	route := p.buildHTTPRoute(testISVC(), target)

	refs := route.Spec.Rules[0].BackendRefs
	require.Len(t, refs, 2, "one backendRef per cluster")
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-a"), refs[0].Name)
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-b"), refs[1].Name)
	require.NotNil(t, refs[0].Weight)
	require.NotNil(t, refs[1].Weight)
	assert.Equal(t, int32(1), *refs[0].Weight, "equal weight across clusters")
	assert.Equal(t, int32(1), *refs[1].Weight)
}

func TestBuildHTTPRoute_GatewayBackendRewritesEachClusterHostname(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, gatewayBackendConfig())
	target := Target{GlobalHost: "h", Clusters: []ServingCluster{
		{Cluster: "cluster-b", BackendHost: "b.example"},
		{Cluster: "cluster-a", BackendHost: "a.example"},
	}}

	refs := p.buildHTTPRoute(testISVC(), target).Spec.Rules[0].BackendRefs
	require.Len(t, refs, 2)
	for i, hostname := range []gatewayapiv1.PreciseHostname{"a.example", "b.example"} {
		require.Len(t, refs[i].Filters, 1)
		assert.Equal(t, gatewayapiv1.HTTPRouteFilterURLRewrite, refs[i].Filters[0].Type)
		require.NotNil(t, refs[i].Filters[0].URLRewrite)
		require.NotNil(t, refs[i].Filters[0].URLRewrite.Hostname)
		assert.Equal(t, hostname, *refs[i].Filters[0].URLRewrite.Hostname)
	}
}

func TestBuildResources_RouteNamespaceOverride(t *testing.T) {
	cfg := baseConfig()
	cfg.RouteNamespace = "ome-gateways"
	p := NewGatewayAPIPublisher(nil, cfg)
	isvc := testISVC()

	svc := p.buildExternalNameService(isvc, p.serviceName(isvc, "c"), ServingCluster{Cluster: "c", BackendHost: "b"})
	route := p.buildHTTPRoute(isvc, oneCluster("h", "c", "b"))

	assert.Equal(t, "ome-gateways", svc.Namespace)
	assert.Equal(t, "ome-gateways", route.Namespace)
	require.NotNil(t, route.Spec.Rules[0].BackendRefs[0].Namespace)
	assert.Equal(t, "ome-gateways", string(*route.Spec.Rules[0].BackendRefs[0].Namespace),
		"backendRef namespace tracks the route namespace")
}

func TestBuildHTTPRoute_BareGatewayName(t *testing.T) {
	cfg := baseConfig()
	cfg.GlobalGateway = "global-gw" // no namespace
	p := NewGatewayAPIPublisher(nil, cfg)

	route := p.buildHTTPRoute(testISVC(), oneCluster("h", "c", "b"))

	pr := route.Spec.ParentRefs[0]
	assert.Nil(t, pr.Namespace, "bare gateway name uses the route-local namespace default")
	assert.Equal(t, "global-gw", string(pr.Name))
}

func TestPublish_SingleClusterCreatesResources(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()

	require.NoError(t, p.Publish(context.Background(), isvc, oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))

	svc := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, svc))
	assert.Equal(t, "svc.prod.cloud-a.example", svc.Spec.ExternalName)

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	assert.Equal(t, gatewayapiv1.Hostname("svc.prod.global.example"), route.Spec.Hostnames[0])
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
}

func TestPublish_LegacyAdoptsUnlabeledResources(t *testing.T) {
	isvc := testISVC()
	target := oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
	p := NewGatewayAPIPublisher(nil, baseConfig())
	service := p.buildExternalNameService(isvc, p.serviceName(isvc, "cluster-a"), target.Clusters[0])
	service.Labels = nil
	service.Spec.ExternalName = "old.example"
	route := p.buildHTTPRoute(isvc, target)
	route.Labels = nil
	route.Spec.Hostnames = []gatewayapiv1.Hostname{"old.example"}
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install), fakeclient.WithObjects(service, route))
	p = NewGatewayAPIPublisher(c, baseConfig())

	require.NoError(t, p.Publish(context.Background(), isvc, target))
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(service), service))
	assert.Equal(t, target.Clusters[0].BackendHost, service.Spec.ExternalName)
	assert.Equal(t, ManagedByValue, service.Labels[ManagedByLabel])
	assert.Equal(t, isvc.Name, service.Labels[PlacementEndpointISVCLabel])
	assert.Equal(t, isvc.Namespace, service.Labels[PlacementEndpointISVCNamespaceLabel])
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(route), route))
	assert.Equal(t, []gatewayapiv1.Hostname{gatewayapiv1.Hostname(target.GlobalHost)}, route.Spec.Hostnames)
	assert.Equal(t, ManagedByValue, route.Labels[ManagedByLabel])
}

func TestPublish_LegacyRejectsPartialSourceLabels(t *testing.T) {
	isvc := testISVC()
	target := oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
	p := NewGatewayAPIPublisher(nil, baseConfig())
	service := p.buildExternalNameService(isvc, p.serviceName(isvc, "cluster-a"), target.Clusters[0])
	service.Labels = map[string]string{PlacementEndpointISVCLabel: isvc.Name}
	service.Spec.ExternalName = "old.example"
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install), fakeclient.WithObjects(service))
	p = NewGatewayAPIPublisher(c, baseConfig())

	err := p.Publish(context.Background(), isvc, target)
	require.ErrorContains(t, err, "belongs to another InferenceService")
	live := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(service), live))
	assert.Equal(t, "old.example", live.Spec.ExternalName)
	assert.NotContains(t, live.Labels, PlacementEndpointISVCNamespaceLabel)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), types.NamespacedName{
		Namespace: p.routeNamespace(isvc), Name: p.routeName(isvc),
	}, &gatewayapiv1.HTTPRoute{})))
}

func TestPublish_StrictOwnershipPreflightsEveryDesiredResource(t *testing.T) {
	for _, resourceKind := range []string{"HTTPRoute", "Service", "EndpointSlice", "BackendTLSPolicy"} {
		t.Run(resourceKind, func(t *testing.T) {
			isvc := testISVC()
			target := oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
			cfg := baseConfig()
			addresses := map[string][]BackendAddress{
				"cluster-a": {{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4}},
			}
			if resourceKind == "EndpointSlice" || resourceKind == "BackendTLSPolicy" {
				cfg = gatewayBackendConfig()
			}
			builder := NewGatewayAPIPublisher(nil, cfg)
			serviceName := builder.serviceName(isvc, target.Clusters[0].Cluster)
			var collision client.Object
			switch resourceKind {
			case "HTTPRoute":
				collision = builder.buildHTTPRoute(isvc, target)
			case "Service":
				collision = builder.buildExternalNameService(isvc, serviceName, target.Clusters[0])
			case "EndpointSlice":
				slices, err := builder.buildEndpointSlices(isvc, serviceName, target.Clusters[0], addresses["cluster-a"])
				require.NoError(t, err)
				require.Len(t, slices, 1)
				collision = slices[0]
			case "BackendTLSPolicy":
				collision = builder.buildBackendTLSPolicy(isvc, serviceName, target.Clusters[0])
			}
			collision.SetLabels(nil)
			collisionKey := client.ObjectKeyFromObject(collision)
			c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install), fakeclient.WithObjects(collision))
			p := NewGatewayAPIPublisher(
				c,
				cfg,
				WithBackendAddressResolver(staticBackendAddressResolver(addresses)),
				withStrictResourceOwnership(),
			)

			err := p.Publish(context.Background(), isvc, target)
			require.ErrorContains(t, err, "belongs to another source")
			if resourceKind != "Service" {
				assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), types.NamespacedName{
					Namespace: p.routeNamespace(isvc), Name: p.serviceName(isvc, target.Clusters[0].Cluster),
				}, &corev1.Service{})), "preflight must not create a backend Service")
			}
			if resourceKind != "HTTPRoute" {
				assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), types.NamespacedName{
					Namespace: p.routeNamespace(isvc), Name: p.routeName(isvc),
				}, &gatewayapiv1.HTTPRoute{})), "preflight must not create an HTTPRoute")
			}
			live := collision.DeepCopyObject().(client.Object)
			live.SetResourceVersion("")
			require.NoError(t, c.Get(context.Background(), collisionKey, live))
			assert.Empty(t, live.GetLabels(), "the colliding object must not be adopted")
		})
	}
}

func TestGatewayAPIPublisher_NoClientOrAPIReader_PublishReturnsError(t *testing.T) {
	target := oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
	err := NewGatewayAPIPublisher(nil, baseConfig()).Publish(context.Background(), testISVC(), target)
	require.ErrorContains(t, err, "client is not configured")

	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, baseConfig(), WithGatewayAPIReader(nil))
	err = p.Publish(context.Background(), testISVC(), target)
	require.ErrorContains(t, err, "API reader is not configured")
}

func TestPublish_MultiClusterCreatesPerClusterServices(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	target := Target{GlobalHost: "svc.prod.global.example", Clusters: []ServingCluster{
		{Cluster: "cluster-a", BackendHost: "a.example"},
		{Cluster: "cluster-b", BackendHost: "b.example"},
	}}

	require.NoError(t, p.Publish(context.Background(), isvc, target))

	a := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, a))
	assert.Equal(t, "a.example", a.Spec.ExternalName)
	b := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-b", Namespace: "prod"}, b))
	assert.Equal(t, "b.example", b.Spec.ExternalName)

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	require.Len(t, route.Spec.Rules[0].BackendRefs, 2, "route load-balances across both clusters")
}

func TestPublish_GatewayBackendCreatesEndpointSlicesAndTLSPolicy(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig(), WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {
			{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4},
			{Address: "2001:db8::10", Type: discoveryv1.AddressTypeIPv6},
		},
	}))
	isvc := testISVC()

	require.NoError(t, p.Publish(context.Background(), isvc,
		oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))

	for name, addressType := range map[string]discoveryv1.AddressType{
		"svc-global-cluster-a-ipv4": discoveryv1.AddressTypeIPv4,
		"svc-global-cluster-a-ipv6": discoveryv1.AddressTypeIPv6,
	} {
		endpointSlice := &discoveryv1.EndpointSlice{}
		require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "prod"}, endpointSlice))
		assert.Equal(t, addressType, endpointSlice.AddressType)
		assert.Equal(t, "svc-global-cluster-a", endpointSlice.Labels[discoveryv1.LabelServiceName])
		assert.Equal(t, ManagedByValue, endpointSlice.Labels[discoveryv1.LabelManagedBy])
		require.Len(t, endpointSlice.Ports, 1)
		require.NotNil(t, endpointSlice.Ports[0].Port)
		assert.Equal(t, int32(443), *endpointSlice.Ports[0].Port)
		require.Len(t, endpointSlice.Endpoints, 1)
		require.NotNil(t, endpointSlice.Endpoints[0].Conditions.Ready)
		assert.True(t, *endpointSlice.Endpoints[0].Conditions.Ready)
		require.NotNil(t, endpointSlice.Endpoints[0].Conditions.Serving)
		assert.True(t, *endpointSlice.Endpoints[0].Conditions.Serving)
		require.NotNil(t, endpointSlice.Endpoints[0].Conditions.Terminating)
		assert.False(t, *endpointSlice.Endpoints[0].Conditions.Terminating)
	}

	policy := &gatewayapiv1.BackendTLSPolicy{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, policy))
	require.Len(t, policy.Spec.TargetRefs, 1)
	assert.Empty(t, policy.Spec.TargetRefs[0].Group)
	assert.Equal(t, gatewayapiv1.Kind(constants.ServiceKind), policy.Spec.TargetRefs[0].Kind)
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-a"), policy.Spec.TargetRefs[0].Name)
	assert.Equal(t, gatewayapiv1.PreciseHostname("svc.prod.cloud-a.example"), policy.Spec.Validation.Hostname)
	require.NotNil(t, policy.Spec.Validation.WellKnownCACertificates)
	assert.Equal(t, gatewayapiv1.WellKnownCACertificatesSystem, *policy.Spec.Validation.WellKnownCACertificates)
}

func TestPublish_GatewayBackendUpdatesAddressesAndPrunesStaleFamily(t *testing.T) {
	c := gatewayClient(t)
	resolver := staticBackendAddressResolver{
		"cluster-a": {
			{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4},
			{Address: "2001:db8::10", Type: discoveryv1.AddressTypeIPv6},
		},
	}
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig(), WithBackendAddressResolver(resolver))
	isvc := testISVC()
	target := oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
	require.NoError(t, p.Publish(context.Background(), isvc, target))

	resolver["cluster-a"] = []BackendAddress{{Address: "2001:db8::20", Type: discoveryv1.AddressTypeIPv6}}
	require.NoError(t, p.Publish(context.Background(), isvc, target))

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a-ipv4", Namespace: "prod"}, &discoveryv1.EndpointSlice{})
	assert.True(t, apierrors.IsNotFound(err), "address-family slice is pruned after the Gateway stops advertising that family")
	endpointSlice := &discoveryv1.EndpointSlice{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a-ipv6", Namespace: "prod"}, endpointSlice))
	require.Len(t, endpointSlice.Endpoints, 1)
	assert.Equal(t, []string{"2001:db8::20"}, endpointSlice.Endpoints[0].Addresses)
}

func TestPublish_GatewayBackendIsIdempotent(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig(), WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {{Address: "2001:db8::10", Type: discoveryv1.AddressTypeIPv6}},
	}))
	isvc := testISVC()
	target := oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
	require.NoError(t, p.Publish(context.Background(), isvc, target))

	endpointSlice := &discoveryv1.EndpointSlice{}
	sliceKey := types.NamespacedName{Name: "svc-global-cluster-a-ipv6", Namespace: "prod"}
	require.NoError(t, c.Get(context.Background(), sliceKey, endpointSlice))
	sliceVersion := endpointSlice.ResourceVersion
	policy := &gatewayapiv1.BackendTLSPolicy{}
	policyKey := types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}
	require.NoError(t, c.Get(context.Background(), policyKey, policy))
	policyVersion := policy.ResourceVersion

	require.NoError(t, p.Publish(context.Background(), isvc, target))
	require.NoError(t, c.Get(context.Background(), sliceKey, endpointSlice))
	assert.Equal(t, sliceVersion, endpointSlice.ResourceVersion)
	require.NoError(t, c.Get(context.Background(), policyKey, policy))
	assert.Equal(t, policyVersion, policy.ResourceVersion)
}

func TestPublish_DisablingEndpointSlicesKeepsExternalNameTLSRouting(t *testing.T) {
	c := gatewayClient(t)
	cfg := gatewayBackendConfig()
	p := NewGatewayAPIPublisher(c, cfg, WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4}},
	}))
	isvc := testISVC()
	target := oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
	require.NoError(t, p.Publish(context.Background(), isvc, target))

	p.config.GatewayBackend.EndpointSlices.Enabled = false
	require.NoError(t, p.Publish(context.Background(), isvc, target))

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a-ipv4", Namespace: "prod"}, &discoveryv1.EndpointSlice{})
	assert.True(t, apierrors.IsNotFound(err), "the direct-address fallback is removed")
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &gatewayapiv1.BackendTLSPolicy{}),
		"backend TLS remains configured")
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	require.Len(t, route.Spec.Rules[0].BackendRefs[0].Filters, 1, "the hostname rewrite remains configured")
}

func TestPublish_ExternalNameTLSDoesNotRequireAddressResolver(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	cfg := gatewayBackendConfig()
	cfg.GatewayBackend.EndpointSlices.Enabled = false
	p := NewGatewayAPIPublisher(c, cfg)

	require.NoError(t, p.Publish(context.Background(), testISVC(),
		oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &gatewayapiv1.BackendTLSPolicy{}))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a-ipv4", Namespace: "prod"}, &discoveryv1.EndpointSlice{})))
}

func TestPublish_GatewayBackendRequiresAddressResolver(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig())

	err := p.Publish(context.Background(), testISVC(),
		oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no backend address resolver")
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})),
		"resolution fails before changing the existing publication")
}

func TestPublish_Idempotent(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	target := oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")

	require.NoError(t, p.Publish(context.Background(), isvc, target))
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	rvBefore := route.ResourceVersion

	require.NoError(t, p.Publish(context.Background(), isvc, target))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	assert.Equal(t, rvBefore, route.ResourceVersion, "identical re-publish must be a no-op")
}

func TestPublish_ClusterLeavesGCsStaleService(t *testing.T) {
	c := gatewayClient(t)
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()

	// Two clusters, then one leaves.
	require.NoError(t, p.Publish(context.Background(), isvc, Target{GlobalHost: "h", Clusters: []ServingCluster{
		{Cluster: "cluster-a", BackendHost: "a.example"},
		{Cluster: "cluster-b", BackendHost: "b.example"},
	}}))
	require.NoError(t, p.Publish(context.Background(), isvc, oneCluster("h", "cluster-a", "a.example")))

	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &corev1.Service{}),
		"surviving cluster's Service kept")
	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-b", Namespace: "prod"}, &corev1.Service{})
	assert.True(t, apierrors.IsNotFound(err), "departed cluster's Service garbage-collected")

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1, "route drops the departed cluster's backendRef")
}

func TestPublish_SingleModeMovesToAnotherCluster_ReplacesServiceAndBackend(t *testing.T) {
	c := gatewayClient(t)
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()

	require.NoError(t, p.Publish(context.Background(), isvc, oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))
	// Single re-placement onto a different cluster: old cluster's Service is GC'd, new one created.
	require.NoError(t, p.Publish(context.Background(), isvc, oneCluster("svc.prod.global.example", "cluster-b", "svc.prod.cloud-b.example")))

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &corev1.Service{})
	assert.True(t, apierrors.IsNotFound(err), "old winner's Service GC'd")
	b := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-b", Namespace: "prod"}, b))
	assert.Equal(t, "svc.prod.cloud-b.example", b.Spec.ExternalName)

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-b"), route.Spec.Rules[0].BackendRefs[0].Name)
}

func TestUnpublish_DeletesRouteAndAllServices(t *testing.T) {
	c := gatewayClient(t)
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	require.NoError(t, p.Publish(context.Background(), isvc, Target{GlobalHost: "h", Clusters: []ServingCluster{
		{Cluster: "cluster-a", BackendHost: "a.example"},
		{Cluster: "cluster-b", BackendHost: "b.example"},
	}}))

	require.NoError(t, p.Unpublish(context.Background(), isvc))

	for _, name := range []string{"svc-global-cluster-a", "svc-global-cluster-b"} {
		err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "prod"}, &corev1.Service{})
		assert.True(t, apierrors.IsNotFound(err), "%s deleted", name)
	}
	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})
	assert.True(t, apierrors.IsNotFound(err), "HTTPRoute deleted")
}

func TestUnpublish_DeletesGatewayBackendResources(t *testing.T) {
	c := gatewayClient(t)
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig(), WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {{Address: "2001:db8::10", Type: discoveryv1.AddressTypeIPv6}},
	}))
	isvc := testISVC()
	require.NoError(t, p.Publish(context.Background(), isvc,
		oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))

	require.NoError(t, p.Unpublish(context.Background(), isvc))

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a-ipv6", Namespace: "prod"}, &discoveryv1.EndpointSlice{})
	assert.True(t, apierrors.IsNotFound(err), "EndpointSlice deleted")
	err = c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &gatewayapiv1.BackendTLSPolicy{})
	assert.True(t, apierrors.IsNotFound(err), "BackendTLSPolicy deleted")
}

func TestUnpublish_MissingIsNoOp(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, baseConfig())
	require.NoError(t, p.Unpublish(context.Background(), testISVC()))
}

func TestZeroClaimedHTTPRoute_RouteWithSeveralRules_ZerosEveryBackendIdempotently(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	require.NoError(t, p.Publish(context.Background(), isvc, Target{GlobalHost: "h", Clusters: []ServingCluster{
		{Cluster: "cluster-a", BackendHost: "a.example", Weight: 3},
		{Cluster: "cluster-b", BackendHost: "b.example", Weight: 1},
	}}))

	routeKey := types.NamespacedName{Namespace: "prod", Name: "svc-global"}
	sourceKey := types.NamespacedName{Namespace: "prod", Name: "svc"}
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	route.Spec.Rules = append(route.Spec.Rules, gatewayapiv1.HTTPRouteRule{
		BackendRefs: []gatewayapiv1.HTTPBackendRef{{
			BackendRef: gatewayapiv1.BackendRef{Weight: nil},
		}},
	})
	require.NoError(t, c.Update(context.Background(), route))

	require.NoError(t, p.zeroClaimedHTTPRoute(context.Background(), routeKey, sourceKey))
	require.NoError(t, p.zeroClaimedHTTPRoute(context.Background(), routeKey, sourceKey), "an already drained route is a no-op")
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	for _, rule := range route.Spec.Rules {
		for _, backend := range rule.BackendRefs {
			require.NotNil(t, backend.Weight)
			assert.Zero(t, *backend.Weight)
		}
	}
}

func TestClaimedHTTPRouteHelpers_LabelsNotExactlyOurs_RefuseAndLeaveWeights(t *testing.T) {
	for _, tc := range []struct {
		name   string
		labels map[string]string
	}{
		{name: "unlabeled"},
		{
			name: "wrong managed by",
			labels: map[string]string{
				ManagedByLabel:                      "another-controller",
				PlacementEndpointISVCLabel:          "svc",
				PlacementEndpointISVCNamespaceLabel: "prod",
			},
		},
		{
			name:   "missing source",
			labels: map[string]string{ManagedByLabel: ManagedByValue},
		},
		{
			name: "foreign source",
			labels: map[string]string{
				ManagedByLabel:                      ManagedByValue,
				PlacementEndpointISVCLabel:          "other",
				PlacementEndpointISVCNamespaceLabel: "prod",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routeKey := types.NamespacedName{Namespace: "routes", Name: "claimed"}
			sourceKey := types.NamespacedName{Namespace: "prod", Name: "svc"}
			route := &gatewayapiv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Namespace: routeKey.Namespace, Name: routeKey.Name, Labels: tc.labels},
				Spec: gatewayapiv1.HTTPRouteSpec{Rules: []gatewayapiv1.HTTPRouteRule{{
					BackendRefs: []gatewayapiv1.HTTPBackendRef{{
						BackendRef: gatewayapiv1.BackendRef{Weight: ptr.To(int32(7))},
					}},
				}}},
			}
			c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install), fakeclient.WithObjects(route))
			p := NewGatewayAPIPublisher(c, baseConfig())

			require.Error(t, p.zeroClaimedHTTPRoute(context.Background(), routeKey, sourceKey))
			require.Error(t, p.deleteClaimedHTTPRoute(context.Background(), routeKey, sourceKey))
			live := &gatewayapiv1.HTTPRoute{}
			require.NoError(t, c.Get(context.Background(), routeKey, live))
			require.NotNil(t, live.Spec.Rules[0].BackendRefs[0].Weight)
			assert.Equal(t, int32(7), *live.Spec.Rules[0].BackendRefs[0].Weight)
		})
	}
}

func TestDeleteClaimedHTTPRoute_CalledTwice_DeletesOnceThenNoOp(t *testing.T) {
	isvc := testISVC()
	p := NewGatewayAPIPublisher(nil, baseConfig())
	route := p.buildHTTPRoute(isvc, oneCluster("h", "cluster-a", "a.example"))
	setGatewayFakeObjectIdentity(route)
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install), fakeclient.WithObjects(route))
	p = NewGatewayAPIPublisher(c, baseConfig())
	routeKey := types.NamespacedName{Namespace: route.Namespace, Name: route.Name}
	sourceKey := types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name}

	require.NoError(t, p.deleteClaimedHTTPRoute(context.Background(), routeKey, sourceKey))
	require.NoError(t, p.deleteClaimedHTTPRoute(context.Background(), routeKey, sourceKey))
	err := c.Get(context.Background(), routeKey, &gatewayapiv1.HTTPRoute{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestDeleteClaimedHTTPRoute_RouteHeldByFinalizer_ZerosWeightsAndWaits(t *testing.T) {
	isvc := testISVC()
	p := NewGatewayAPIPublisher(nil, baseConfig())
	route := p.buildHTTPRoute(isvc, oneCluster("h", "cluster-a", "a.example"))
	route.Finalizers = []string{"example.com/hold"}
	route.Spec.Rules[0].BackendRefs[0].Weight = nil
	setGatewayFakeObjectIdentity(route)
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install), fakeclient.WithObjects(route))
	p = NewGatewayAPIPublisher(c, baseConfig())
	routeKey := client.ObjectKeyFromObject(route)
	sourceKey := client.ObjectKeyFromObject(isvc)

	err := p.deleteClaimedHTTPRoute(context.Background(), routeKey, sourceKey)
	require.ErrorContains(t, err, "deletion is pending")
	live := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, live))
	require.NotNil(t, live.DeletionTimestamp)
	assert.Equal(t, []string{"example.com/hold"}, live.Finalizers)
	require.NotNil(t, live.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Zero(t, *live.Spec.Rules[0].BackendRefs[0].Weight)

	live.Finalizers = nil
	require.NoError(t, c.Update(context.Background(), live))
	require.NoError(t, p.deleteClaimedHTTPRoute(context.Background(), routeKey, sourceKey))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), routeKey, &gatewayapiv1.HTTPRoute{})))
}

func TestDeleteClaimedHTTPRoute_RouteReplacedMeanwhile_LeavesReplacement(t *testing.T) {
	isvc := testISVC()
	p := NewGatewayAPIPublisher(nil, baseConfig())
	route := p.buildHTTPRoute(isvc, oneCluster("h", "cluster-a", "a.example"))
	route.UID = types.UID("owned-route-uid")
	route.ResourceVersion = "1"
	routeKey := client.ObjectKeyFromObject(route)
	sourceKey := client.ObjectKeyFromObject(isvc)

	deleteCalls := 0
	c := fakeclient.New(t,
		fakeclient.WithTypes(gatewayapiv1.Install),
		fakeclient.WithObjects(route),
		fakeclient.WithInterceptors(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				deleteCalls++
				deleteOptions := (&client.DeleteOptions{}).ApplyOptions(opts)
				require.NotNil(t, deleteOptions.Preconditions)
				require.NotNil(t, deleteOptions.Preconditions.UID)
				require.NotNil(t, deleteOptions.Preconditions.ResourceVersion)
				assert.Equal(t, obj.GetUID(), *deleteOptions.Preconditions.UID)
				assert.Equal(t, obj.GetResourceVersion(), *deleteOptions.Preconditions.ResourceVersion)

				foreign := &gatewayapiv1.HTTPRoute{}
				require.NoError(t, cl.Get(ctx, routeKey, foreign))
				foreign.UID = types.UID("foreign-route-uid")
				foreign.Labels[PlacementEndpointISVCLabel] = "other"
				foreign.Spec.Rules[0].BackendRefs[0].Weight = ptr.To(int32(9))
				require.NoError(t, cl.Update(ctx, foreign))
				return apierrors.NewConflict(
					gatewayapiv1.Resource("httproutes"),
					obj.GetName(),
					apierrors.NewBadRequest("delete preconditions no longer match"),
				)
			},
		}),
	)
	p = NewGatewayAPIPublisher(c, baseConfig())

	err := p.deleteClaimedHTTPRoute(context.Background(), routeKey, sourceKey)
	require.Error(t, err)
	assert.True(t, apierrors.IsConflict(errors.Unwrap(err)))
	assert.Equal(t, 1, deleteCalls)
	foreign := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, foreign))
	assert.Equal(t, types.UID("foreign-route-uid"), foreign.UID)
	assert.Equal(t, "other", foreign.Labels[PlacementEndpointISVCLabel])
	require.NotNil(t, foreign.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Equal(t, int32(9), *foreign.Spec.Rules[0].BackendRefs[0].Weight)
}

func TestDeleteObservedObject_UIDOrResourceVersionMissing_FailsWithoutDelete(t *testing.T) {
	for _, tc := range []struct {
		name              string
		uid               types.UID
		resourceVersion   string
		expectedErrorText string
	}{
		{
			name:              "missing UID",
			resourceVersion:   "1",
			expectedErrorText: "UID is empty",
		},
		{
			name:              "missing resource version",
			uid:               types.UID("service-uid"),
			expectedErrorText: "resourceVersion is empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deleteCalls := 0
			c := fakeclient.New(t,
				fakeclient.WithTypes(gatewayapiv1.Install),
				fakeclient.WithInterceptors(interceptor.Funcs{
					Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
						deleteCalls++
						return nil
					},
				}),
			)
			p := NewGatewayAPIPublisher(c, baseConfig())
			service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
				Namespace:       "prod",
				Name:            "backend",
				UID:             tc.uid,
				ResourceVersion: tc.resourceVersion,
			}}

			err := p.deleteObservedObject(context.Background(), service)
			require.ErrorContains(t, err, tc.expectedErrorText)
			assert.Zero(t, deleteCalls)
		})
	}
}

func TestDeleteObservedObject_ServerSaysNotFound_ReturnsNoError(t *testing.T) {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Namespace:       "prod",
		Name:            "backend",
		UID:             types.UID("service-uid"),
		ResourceVersion: "7",
	}}
	deleteCalls := 0
	c := fakeclient.New(t,
		fakeclient.WithTypes(gatewayapiv1.Install),
		fakeclient.WithInterceptors(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, object client.Object, opts ...client.DeleteOption) error {
				deleteCalls++
				deleteOptions := (&client.DeleteOptions{}).ApplyOptions(opts)
				require.NotNil(t, deleteOptions.Preconditions)
				require.Equal(t, service.UID, *deleteOptions.Preconditions.UID)
				require.Equal(t, service.ResourceVersion, *deleteOptions.Preconditions.ResourceVersion)
				return apierrors.NewNotFound(schema.GroupResource{Resource: "services"}, object.GetName())
			},
		}),
	)
	p := NewGatewayAPIPublisher(c, baseConfig())

	require.NoError(t, p.deleteObservedObject(context.Background(), service))
	assert.Equal(t, 1, deleteCalls)
}

func TestGatewayDeletionCallSites_ObjectReplacedBeforeDelete_LeaveTheReplacement(t *testing.T) {
	type invokeFunc func(context.Context, *GatewayAPIPublisher, *v1beta1.InferenceService) error
	for _, tc := range []struct {
		name       string
		objectKind string
		invoke     invokeFunc
	}{
		{
			name:       "legacy route cleanup",
			objectKind: "HTTPRoute",
			invoke: func(ctx context.Context, p *GatewayAPIPublisher, isvc *v1beta1.InferenceService) error {
				return p.Unpublish(ctx, isvc)
			},
		},
		{
			name:       "Service cleanup",
			objectKind: "Service",
			invoke: func(ctx context.Context, p *GatewayAPIPublisher, isvc *v1beta1.InferenceService) error {
				return p.deleteSourceResources(ctx, p.routeNamespace(isvc), client.ObjectKeyFromObject(isvc))
			},
		},
		{
			name:       "Service prune",
			objectKind: "Service",
			invoke: func(ctx context.Context, p *GatewayAPIPublisher, isvc *v1beta1.InferenceService) error {
				return p.pruneServices(ctx, isvc, map[string]struct{}{})
			},
		},
		{
			name:       "EndpointSlice cleanup",
			objectKind: "EndpointSlice",
			invoke: func(ctx context.Context, p *GatewayAPIPublisher, isvc *v1beta1.InferenceService) error {
				return p.deleteGatewayBackendResourcesForSource(ctx, p.routeNamespace(isvc), client.ObjectKeyFromObject(isvc))
			},
		},
		{
			name:       "EndpointSlice prune",
			objectKind: "EndpointSlice",
			invoke: func(ctx context.Context, p *GatewayAPIPublisher, isvc *v1beta1.InferenceService) error {
				return p.pruneGatewayBackendResourcesForSource(
					ctx, p.routeNamespace(isvc), client.ObjectKeyFromObject(isvc),
					map[string]struct{}{}, map[string]struct{}{},
				)
			},
		},
		{
			name:       "BackendTLSPolicy cleanup",
			objectKind: "BackendTLSPolicy",
			invoke: func(ctx context.Context, p *GatewayAPIPublisher, isvc *v1beta1.InferenceService) error {
				return p.deleteGatewayBackendResourcesForSource(ctx, p.routeNamespace(isvc), client.ObjectKeyFromObject(isvc))
			},
		},
		{
			name:       "BackendTLSPolicy prune",
			objectKind: "BackendTLSPolicy",
			invoke: func(ctx context.Context, p *GatewayAPIPublisher, isvc *v1beta1.InferenceService) error {
				return p.pruneGatewayBackendResourcesForSource(
					ctx, p.routeNamespace(isvc), client.ObjectKeyFromObject(isvc),
					map[string]struct{}{}, map[string]struct{}{},
				)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := testISVC()
			renderer := NewGatewayAPIPublisher(nil, baseConfig())
			labels := renderer.serviceLabels(isvc, "cluster-a")
			var observed client.Object
			switch tc.objectKind {
			case "HTTPRoute":
				observed = renderer.buildHTTPRoute(isvc, oneCluster("svc.global.example", "cluster-a", "backend.example"))
			case "Service":
				observed = &corev1.Service{ObjectMeta: metav1.ObjectMeta{
					Namespace: isvc.Namespace, Name: "backend", Labels: labels,
				}}
			case "EndpointSlice":
				observed = &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
					Namespace: isvc.Namespace, Name: "backend-ipv4", Labels: labels,
				}}
			case "BackendTLSPolicy":
				observed = &gatewayapiv1.BackendTLSPolicy{ObjectMeta: metav1.ObjectMeta{
					Namespace: isvc.Namespace, Name: "backend", Labels: labels,
				}}
			default:
				t.Fatalf("unknown resource kind %q", tc.objectKind)
			}
			observed.SetUID(types.UID("observed-uid"))
			observed.SetResourceVersion("1")
			key := client.ObjectKeyFromObject(observed)
			deleteCalls := 0
			c := fakeclient.New(t,
				fakeclient.WithTypes(gatewayapiv1.Install),
				fakeclient.WithObjects(observed),
				fakeclient.WithInterceptors(interceptor.Funcs{
					Delete: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.DeleteOption) error {
						deleteCalls++
						deleteOptions := (&client.DeleteOptions{}).ApplyOptions(opts)
						require.NotNil(t, deleteOptions.Preconditions)
						require.NotNil(t, deleteOptions.Preconditions.UID)
						require.NotNil(t, deleteOptions.Preconditions.ResourceVersion)
						assert.Equal(t, object.GetUID(), *deleteOptions.Preconditions.UID)
						assert.Equal(t, object.GetResourceVersion(), *deleteOptions.Preconditions.ResourceVersion)

						current := object.DeepCopyObject().(client.Object)
						require.NoError(t, c.Get(ctx, key, current))
						require.NoError(t, c.Delete(ctx, current))
						replacement := current.DeepCopyObject().(client.Object)
						replacement.SetUID(types.UID("replacement-uid"))
						replacement.SetResourceVersion("")
						replacementLabels := map[string]string{}
						for label, value := range replacement.GetLabels() {
							replacementLabels[label] = value
						}
						replacementLabels[PlacementEndpointISVCLabel] = "other"
						replacement.SetLabels(replacementLabels)
						require.NoError(t, c.Create(ctx, replacement))
						return apierrors.NewConflict(
							schema.GroupResource{Resource: "gateway-publications"},
							object.GetName(),
							errors.New("delete preconditions no longer match"),
						)
					},
				}),
			)
			p := NewGatewayAPIPublisher(c, baseConfig())

			err := tc.invoke(context.Background(), p, isvc)
			require.Error(t, err)
			var statusError *apierrors.StatusError
			require.ErrorAs(t, err, &statusError)
			assert.True(t, apierrors.IsConflict(statusError))
			assert.Equal(t, 1, deleteCalls)
			live := observed.DeepCopyObject().(client.Object)
			require.NoError(t, c.Get(context.Background(), key, live))
			assert.Equal(t, types.UID("replacement-uid"), live.GetUID())
			assert.Equal(t, "other", live.GetLabels()[PlacementEndpointISVCLabel])
		})
	}
}

func TestDeleteSourceResources_MixedOwnersAndNamespaces_DeletesOnlyExactMatches(t *testing.T) {
	sourceKey := types.NamespacedName{Namespace: "prod", Name: "svc"}
	ownedLabels := map[string]string{
		ManagedByLabel:                      ManagedByValue,
		PlacementEndpointISVCLabel:          sourceKey.Name,
		PlacementEndpointISVCNamespaceLabel: sourceKey.Namespace,
	}
	foreignLabels := map[string]string{
		ManagedByLabel:                      ManagedByValue,
		PlacementEndpointISVCLabel:          "other",
		PlacementEndpointISVCNamespaceLabel: sourceKey.Namespace,
	}
	objects := []client.Object{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "routes", Name: "owned-service", Labels: ownedLabels}},
		&discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: "routes", Name: "owned-slice", Labels: ownedLabels}},
		&gatewayapiv1.BackendTLSPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "routes", Name: "owned-policy", Labels: ownedLabels}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "routes", Name: "foreign-service", Labels: foreignLabels}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "elsewhere", Name: "owned-elsewhere", Labels: ownedLabels}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "routes", Name: "unlabeled-service"}},
	}
	setGatewayFakeObjectIdentity(objects...)
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install), fakeclient.WithObjects(objects...))
	p := NewGatewayAPIPublisher(c, baseConfig())

	require.NoError(t, p.deleteSourceResources(context.Background(), "routes", sourceKey))
	for _, key := range []types.NamespacedName{
		{Namespace: "routes", Name: "owned-service"},
		{Namespace: "routes", Name: "owned-slice"},
		{Namespace: "routes", Name: "owned-policy"},
	} {
		var obj client.Object
		switch key.Name {
		case "owned-slice":
			obj = &discoveryv1.EndpointSlice{}
		case "owned-policy":
			obj = &gatewayapiv1.BackendTLSPolicy{}
		default:
			obj = &corev1.Service{}
		}
		err := c.Get(context.Background(), key, obj)
		assert.True(t, apierrors.IsNotFound(err), "%s deleted", key)
	}
	for _, key := range []types.NamespacedName{
		{Namespace: "routes", Name: "foreign-service"},
		{Namespace: "elsewhere", Name: "owned-elsewhere"},
		{Namespace: "routes", Name: "unlabeled-service"},
	} {
		require.NoError(t, c.Get(context.Background(), key, &corev1.Service{}), "%s retained", key)
	}
}

func TestGatewayAPIPublisher_CacheReturnsNothing_CleanupAndPreflightReadLive(t *testing.T) {
	sourceKey := types.NamespacedName{Namespace: "prod", Name: "svc"}
	routeKey := types.NamespacedName{Namespace: sourceKey.Namespace, Name: "svc-global"}
	labels := func() map[string]string {
		return map[string]string{
			ManagedByLabel:                      ManagedByValue,
			PlacementEndpointISVCLabel:          sourceKey.Name,
			PlacementEndpointISVCNamespaceLabel: sourceKey.Namespace,
		}
	}
	objects := []client.Object{
		&gatewayapiv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
			Namespace: routeKey.Namespace, Name: routeKey.Name, Labels: labels(),
		}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Namespace: routeKey.Namespace, Name: "owned-service", Labels: labels(),
		}},
		&discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
			Namespace: routeKey.Namespace, Name: "owned-slice", Labels: labels(),
		}},
		&gatewayapiv1.BackendTLSPolicy{ObjectMeta: metav1.ObjectMeta{
			Namespace: routeKey.Namespace, Name: "owned-policy", Labels: labels(),
		}},
	}
	setGatewayFakeObjectIdentity(objects...)
	live := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install), fakeclient.WithObjects(objects...))
	stale := fakeclient.Wrap(t, live, blindCache())
	p := NewGatewayAPIPublisher(
		stale,
		baseConfig(),
		WithGatewayAPIReader(live),
		withStrictResourceOwnership(),
	)

	require.NoError(t, p.deleteClaimedHTTPRoute(context.Background(), routeKey, sourceKey))
	require.NoError(t, p.deleteSourceResources(context.Background(), routeKey.Namespace, sourceKey))
	for _, object := range objects {
		key := client.ObjectKeyFromObject(object)
		err := live.Get(context.Background(), key, object.DeepCopyObject().(client.Object))
		assert.True(t, apierrors.IsNotFound(err), "%T %s deleted", object, key)
	}

	foreignRoute := &gatewayapiv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
		Namespace: routeKey.Namespace, Name: routeKey.Name,
	}}
	require.NoError(t, live.Create(context.Background(), foreignRoute))
	isvc := testISVC()
	isvc.Namespace = sourceKey.Namespace
	isvc.Name = sourceKey.Name
	err := p.Publish(context.Background(), isvc,
		oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example"))
	require.ErrorContains(t, err, "belongs to another source")
	assert.True(t, apierrors.IsNotFound(live.Get(context.Background(), types.NamespacedName{
		Namespace: routeKey.Namespace, Name: p.serviceName(isvc, "cluster-a"),
	}, &corev1.Service{})), "live preflight must reject the route before any write")
}

func TestBuildHTTPRoute_WeightedByReadyReplicas(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	// Split: each cluster carries its ready-replica count -> proportional weights.
	target := Target{GlobalHost: "h", Clusters: []ServingCluster{
		{Cluster: "cluster-a", BackendHost: "a.example", Weight: 3},
		{Cluster: "cluster-b", BackendHost: "b.example", Weight: 1},
	}}

	refs := p.buildHTTPRoute(testISVC(), target).Spec.Rules[0].BackendRefs
	require.Len(t, refs, 2)
	require.NotNil(t, refs[0].Weight)
	require.NotNil(t, refs[1].Weight)
	assert.Equal(t, int32(3), *refs[0].Weight, "cluster-a (sorted first) weighted by its ready replicas")
	assert.Equal(t, int32(1), *refs[1].Weight, "cluster-b weighted by its ready replicas")
}

func TestBuildHTTPRoute_UnweightedFallsBackToEqual(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	// No weights (Single/All, or a Split placement before any cluster is ready):
	// every weight is 0, so fall back to equal (1) rather than black-holing.
	target := Target{GlobalHost: "h", Clusters: []ServingCluster{
		{Cluster: "cluster-a", BackendHost: "a.example"},
		{Cluster: "cluster-b", BackendHost: "b.example"},
	}}

	refs := p.buildHTTPRoute(testISVC(), target).Spec.Rules[0].BackendRefs
	require.Len(t, refs, 2)
	assert.Equal(t, int32(1), *refs[0].Weight)
	assert.Equal(t, int32(1), *refs[1].Weight)
}

func TestBuildHTTPRoute_AuthoritativeAllZeroIsPreserved(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	target := Target{GlobalHost: "h", WeightsAuthoritative: true, Clusters: []ServingCluster{
		{Cluster: "cluster-a", BackendHost: "a.example"},
		{Cluster: "cluster-b", BackendHost: "b.example"},
	}}

	refs := p.buildHTTPRoute(testISVC(), target).Spec.Rules[0].BackendRefs
	require.Len(t, refs, 2)
	assert.Equal(t, int32(0), *refs[0].Weight)
	assert.Equal(t, int32(0), *refs[1].Weight)
}

func TestPublish_RouteUpdateFailureKeepsOldBackendService(t *testing.T) {
	c := fakeclient.New(t, fakeclient.WithTypes(gatewayapiv1.Install))
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	require.NoError(t, p.Publish(context.Background(), isvc, oneCluster("h", "cluster-a", "a.example")))

	p.client = fakeclient.Wrap(t, c, routeUpdatesRefused())
	err := p.Publish(context.Background(), isvc, oneCluster("h", "cluster-b", "b.example"))
	require.Error(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &corev1.Service{}),
		"old Service remains while the route still references it")
}

func TestPublish_SharedRouteNamespaceIsolatesSameNameSources(t *testing.T) {
	c := gatewayClient(t)
	cfg := baseConfig()
	cfg.RouteNamespace = "ome-gateways"
	p := NewGatewayAPIPublisher(c, cfg)
	a := testISVC()
	a.Namespace = "team-a"
	b := testISVC()
	b.Namespace = "team-b"

	require.NoError(t, p.Publish(context.Background(), a, oneCluster("a.global.example", "cluster-a", "a.example")))
	require.NoError(t, p.Publish(context.Background(), b, oneCluster("b.global.example", "cluster-b", "b.example")))
	assert.NotEqual(t, p.routeName(a), p.routeName(b))
	assert.NotEqual(t, p.serviceName(a, "cluster-a"), p.serviceName(b, "cluster-b"))
	ownedA, err := p.ownedServices(context.Background(), a)
	require.NoError(t, err)
	require.Len(t, ownedA, 1)
	assert.Equal(t, "team-a", ownedA[0].Labels[PlacementEndpointISVCNamespaceLabel])

	require.NoError(t, p.Unpublish(context.Background(), a))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: p.routeName(b), Namespace: "ome-gateways"}, &gatewayapiv1.HTTPRoute{}))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: p.serviceName(b, "cluster-b"), Namespace: "ome-gateways"}, &corev1.Service{}))
}

func TestResourceNames_BoundedToDNSLabelLimit(t *testing.T) {
	cfg := baseConfig()
	cfg.RouteNamespace = "ome-gateways"
	p := NewGatewayAPIPublisher(nil, cfg)
	isvc := testISVC()
	isvc.Namespace = strings.Repeat("n", 60)
	isvc.Name = strings.Repeat("s", 60)

	assert.LessOrEqual(t, len(p.routeName(isvc)), validation.DNS1035LabelMaxLength)
	assert.LessOrEqual(t, len(p.serviceName(isvc, "cluster-a")), validation.DNS1035LabelMaxLength)
	assert.NotEqual(t, p.serviceName(isvc, "cluster-a"), p.serviceName(isvc, "cluster-b"),
		"per-cluster names stay distinct after truncation")
}

// Two distinct sources must never share a published name. A plain
// namespace-name join collides across the hyphen, which would let one source
// overwrite or delete the other's route in a shared RouteNamespace.
func TestRouteName_NamespaceNameJoinsThatCouldCollide_StayDistinct(t *testing.T) {
	p := &GatewayAPIPublisher{config: Config{RouteNamespace: "gw"}}

	pairs := [][2]string{
		{"a", "b-c"},
		{"a-b", "c"},
		{"gw", "x-y"},
		{"x", "y"},
	}
	seen := map[string][2]string{}
	for _, ns := range pairs {
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns[0], Name: ns[1]},
		}
		got := p.routeName(isvc)
		if other, dup := seen[got]; dup {
			t.Fatalf("route name %q collides: %s/%s and %s/%s", got, other[0], other[1], ns[0], ns[1])
		}
		seen[got] = ns
	}
}

// Service names must be DNS-1035 labels, which have to start with a letter.
func TestBoundedResourceName_InputStartsWithDigit_StartsWithLetterWithinLimit(t *testing.T) {
	for _, in := range []string{"9team-svc", "0", "abc"} {
		got := boundedResourceName(in)
		if got == "" || got[0] < 'a' || got[0] > 'z' {
			t.Fatalf("boundedResourceName(%q) = %q, must start with a lowercase letter", in, got)
		}
		if len(got) > validation.DNS1035LabelMaxLength {
			t.Fatalf("boundedResourceName(%q) = %q exceeds the DNS label limit", in, got)
		}
	}
}

// The EndpointSlices of one cluster: one per address family, IPv4 first,
// each address once in its canonical form; an address of the wrong shape or
// a cluster without one usable address refuses the build and names why.
func TestBuildEndpointSlices_AddressShapes_OneSlicePerFamilyOrARefusal(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, gatewayBackendConfig())
	isvc := testISVC()
	cluster := ServingCluster{Cluster: "cluster-a", BackendHost: "svc.prod.cloud-a.example"}
	v4 := func(a string) BackendAddress { return BackendAddress{Address: a, Type: discoveryv1.AddressTypeIPv4} }
	v6 := func(a string) BackendAddress { return BackendAddress{Address: a, Type: discoveryv1.AddressTypeIPv6} }
	for _, tc := range []struct {
		name      string
		addresses []BackendAddress
		// want lists each slice as "<name suffix> <addresses>", in build order.
		want    []string
		wantErr string
	}{
		{name: "both families, IPv4 first", addresses: []BackendAddress{v6("2001:db8::10"), v4("192.0.2.10")}, want: []string{"-ipv4 [192.0.2.10]", "-ipv6 [2001:db8::10]"}},
		{name: "repeats and letter case fold to one endpoint", addresses: []BackendAddress{v6("2001:DB8::10"), v6("2001:db8::10"), v4("192.0.2.10"), v4("192.0.2.10")}, want: []string{"-ipv4 [192.0.2.10]", "-ipv6 [2001:db8::10]"}},
		{name: "an IPv4-mapped IPv6 address is an IPv4 address", addresses: []BackendAddress{v4("::ffff:192.0.2.10")}, want: []string{"-ipv4 [192.0.2.10]"}},
		{name: "a family other than IPv4 or IPv6", addresses: []BackendAddress{{Address: "svc.example", Type: discoveryv1.AddressTypeFQDN}}, wantErr: `build EndpointSlice for cluster "cluster-a": unsupported address type "FQDN"`},
		{name: "a zone", addresses: []BackendAddress{v6("fe80::1%eth0")}, wantErr: `build EndpointSlice for cluster "cluster-a": invalid IPv6 address "fe80::1%eth0"`},
		{name: "not an address", addresses: []BackendAddress{v4("192.0.2.300")}, wantErr: `build EndpointSlice for cluster "cluster-a": invalid IPv4 address "192.0.2.300"`},
		{name: "an IPv6 address declared IPv4", addresses: []BackendAddress{v4("2001:db8::10")}, wantErr: `build EndpointSlice for cluster "cluster-a": address "2001:db8::10" does not match type "IPv4"`},
		{name: "no address", addresses: nil, wantErr: `build EndpointSlice for cluster "cluster-a": no IPv4 or IPv6 addresses`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			built, err := p.buildEndpointSlices(isvc, "svc-global-cluster-a", cluster, tc.addresses)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("buildEndpointSlices() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			require.NoError(t, err)
			var got []string
			for _, s := range built {
				var addresses []string
				for _, e := range s.Endpoints {
					addresses = append(addresses, e.Addresses...)
				}
				got = append(got, strings.TrimPrefix(s.Name, "svc-global-cluster-a")+" "+fmt.Sprint(addresses))
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// The preflight reads the published objects in the configured route
// namespace, not in the service's own: an object of another source under the
// backend name there stops the publication, one in the service's namespace
// does not.
func TestPreflightPublishedObjects_RouteNamespaceSet_ReadsTheObjectsThere(t *testing.T) {
	cfg := gatewayBackendConfig()
	cfg.RouteNamespace = "edge"
	isvc := testISVC()
	otherOwner := map[string]string{PlacementEndpointISVCLabel: "other", PlacementEndpointISVCNamespaceLabel: isvc.Namespace}
	for _, tc := range []struct {
		name      string
		namespace string
		wantErr   string
	}{
		{name: "in the route namespace", namespace: "edge", wantErr: `global backend Service edge/svc-5973a5bb-global-cluster-a belongs to another source`},
		{name: "in the service namespace", namespace: isvc.Namespace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			taken := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: tc.namespace, Name: "svc-5973a5bb-global-cluster-a", Labels: otherOwner}}
			p := NewGatewayAPIPublisher(gatewayClient(t, fakeclient.WithObjects(taken)), cfg)
			err := p.preflightResourceKeys(context.Background(), isvc, oneCluster("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example"))
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			if err == nil || err.Error() != tc.wantErr {
				t.Fatalf("preflightResourceKeys() error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}
