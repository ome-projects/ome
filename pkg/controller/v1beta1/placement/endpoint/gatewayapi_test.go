package endpoint

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func pubScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(s))
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, gatewayapiv1.Install(s))
	return s
}

func testISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
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

func (r staticBackendAddressResolver) Resolve(_ context.Context, _ *v1beta1.InferenceService, home Home) ([]BackendAddress, error) {
	return r[home.Cluster], nil
}

// oneHome builds a single-home Target (Single mode shape).
func oneHome(globalHost, cluster, backendHost string) Target {
	return Target{GlobalHost: globalHost, Homes: []Home{{Cluster: cluster, BackendHost: backendHost}}}
}

func TestBuildExternalNameService(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	isvc := testISVC()
	home := Home{Cluster: "cluster-a", BackendHost: "svc.prod.cloud-a.example"}

	svc := p.buildExternalNameService(isvc, p.serviceName(isvc, "cluster-a"), home)

	assert.Equal(t, "svc-global-cluster-a", svc.Name, "per-home Service name carries the cluster")
	assert.Equal(t, "prod", svc.Namespace, "namespace falls back to the ISVC namespace")
	assert.Equal(t, corev1.ServiceTypeExternalName, svc.Spec.Type)
	assert.Equal(t, "svc.prod.cloud-a.example", svc.Spec.ExternalName, "ExternalName aliases the home ingress host")
	require.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, int32(8080), svc.Spec.Ports[0].Port)
	assert.Equal(t, ManagedByValue, svc.Labels[ManagedByLabel])
	assert.Equal(t, "cluster-a", svc.Labels[PlacementClusterLabel])
	assert.Equal(t, "svc", svc.Labels[PlacementEndpointISVCLabel], "per-ISVC grouping label for GC/teardown")
	assert.Equal(t, "prod", svc.Labels[PlacementEndpointISVCNamespaceLabel])
	assert.Equal(t, "platform", svc.Labels["team"], "operator labels are merged in")
}

func TestBuildHTTPRoute_SingleHome(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())

	route := p.buildHTTPRoute(testISVC(), oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example"))

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
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-a"), br.Name, "route forwards to the home's ExternalName Service")
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

func TestBuildHTTPRoute_MultiHomeEqualWeightSorted(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	// Homes passed out of order; the route's backendRefs must be sorted by cluster.
	target := Target{GlobalHost: "h", Homes: []Home{
		{Cluster: "cluster-b", BackendHost: "b.example"},
		{Cluster: "cluster-a", BackendHost: "a.example"},
	}}

	route := p.buildHTTPRoute(testISVC(), target)

	refs := route.Spec.Rules[0].BackendRefs
	require.Len(t, refs, 2, "one backendRef per home")
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-a"), refs[0].Name)
	assert.Equal(t, gatewayapiv1.ObjectName("svc-global-cluster-b"), refs[1].Name)
	require.NotNil(t, refs[0].Weight)
	require.NotNil(t, refs[1].Weight)
	assert.Equal(t, int32(1), *refs[0].Weight, "equal weight across homes")
	assert.Equal(t, int32(1), *refs[1].Weight)
}

func TestBuildHTTPRoute_GatewayBackendRewritesEachHomeHostname(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, gatewayBackendConfig())
	target := Target{GlobalHost: "h", Homes: []Home{
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

	svc := p.buildExternalNameService(isvc, p.serviceName(isvc, "c"), Home{Cluster: "c", BackendHost: "b"})
	route := p.buildHTTPRoute(isvc, oneHome("h", "c", "b"))

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

	route := p.buildHTTPRoute(testISVC(), oneHome("h", "c", "b"))

	pr := route.Spec.ParentRefs[0]
	require.NotNil(t, pr.Namespace)
	assert.Equal(t, "", string(*pr.Namespace), "bare gateway name yields empty (route-local) namespace")
	assert.Equal(t, "global-gw", string(pr.Name))
}

// routeUpdateFailClient fails HTTPRoute updates so a Publish that cannot
// repoint the route is observable.
type routeUpdateFailClient struct {
	client.Client
}

func (c routeUpdateFailClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*gatewayapiv1.HTTPRoute); ok {
		return errors.New("route update failed")
	}
	return c.Client.Update(ctx, obj, opts...)
}

func TestPublish_SingleHomeCreatesResources(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()

	require.NoError(t, p.Publish(context.Background(), isvc, oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))

	svc := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, svc))
	assert.Equal(t, "svc.prod.cloud-a.example", svc.Spec.ExternalName)

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	assert.Equal(t, gatewayapiv1.Hostname("svc.prod.global.example"), route.Spec.Hostnames[0])
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1)
}

func TestPublish_MultiHomeCreatesPerHomeServices(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	target := Target{GlobalHost: "svc.prod.global.example", Homes: []Home{
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
	require.Len(t, route.Spec.Rules[0].BackendRefs, 2, "route load-balances across both homes")
}

func TestPublish_GatewayBackendCreatesEndpointSlicesAndTLSPolicy(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig(), WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {
			{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4},
			{Address: "2001:db8::10", Type: discoveryv1.AddressTypeIPv6},
		},
	}))
	isvc := testISVC()

	require.NoError(t, p.Publish(context.Background(), isvc,
		oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))

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
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	resolver := staticBackendAddressResolver{
		"cluster-a": {
			{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4},
			{Address: "2001:db8::10", Type: discoveryv1.AddressTypeIPv6},
		},
	}
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig(), WithBackendAddressResolver(resolver))
	isvc := testISVC()
	target := oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
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
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig(), WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {{Address: "2001:db8::10", Type: discoveryv1.AddressTypeIPv6}},
	}))
	isvc := testISVC()
	target := oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
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
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	cfg := gatewayBackendConfig()
	p := NewGatewayAPIPublisher(c, cfg, WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4}},
	}))
	isvc := testISVC()
	target := oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")
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
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	cfg := gatewayBackendConfig()
	cfg.GatewayBackend.EndpointSlices.Enabled = false
	p := NewGatewayAPIPublisher(c, cfg)

	require.NoError(t, p.Publish(context.Background(), testISVC(),
		oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &gatewayapiv1.BackendTLSPolicy{}))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a-ipv4", Namespace: "prod"}, &discoveryv1.EndpointSlice{})))
}

func TestPublish_GatewayBackendRequiresAddressResolver(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig())

	err := p.Publish(context.Background(), testISVC(),
		oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no backend address resolver")
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})),
		"resolution fails before changing the existing publication")
}

func TestPublish_Idempotent(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	target := oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")

	require.NoError(t, p.Publish(context.Background(), isvc, target))
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	rvBefore := route.ResourceVersion

	require.NoError(t, p.Publish(context.Background(), isvc, target))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	assert.Equal(t, rvBefore, route.ResourceVersion, "identical re-publish must be a no-op")
}

func TestPublish_HomeLeavesGCsStaleService(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()

	// Two homes, then one leaves.
	require.NoError(t, p.Publish(context.Background(), isvc, Target{GlobalHost: "h", Homes: []Home{
		{Cluster: "cluster-a", BackendHost: "a.example"},
		{Cluster: "cluster-b", BackendHost: "b.example"},
	}}))
	require.NoError(t, p.Publish(context.Background(), isvc, oneHome("h", "cluster-a", "a.example")))

	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &corev1.Service{}),
		"surviving home's Service kept")
	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-b", Namespace: "prod"}, &corev1.Service{})
	assert.True(t, apierrors.IsNotFound(err), "departed home's Service garbage-collected")

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	require.Len(t, route.Spec.Rules[0].BackendRefs, 1, "route drops the departed home's backendRef")
}

func TestPublish_WinnerMovesClustersInSingle(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()

	require.NoError(t, p.Publish(context.Background(), isvc, oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))
	// Single re-placement onto a different cluster: old home's Service is GC'd, new one created.
	require.NoError(t, p.Publish(context.Background(), isvc, oneHome("svc.prod.global.example", "cluster-b", "svc.prod.cloud-b.example")))

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
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	require.NoError(t, p.Publish(context.Background(), isvc, Target{GlobalHost: "h", Homes: []Home{
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
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig(), WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {{Address: "2001:db8::10", Type: discoveryv1.AddressTypeIPv6}},
	}))
	isvc := testISVC()
	require.NoError(t, p.Publish(context.Background(), isvc,
		oneHome("svc.prod.global.example", "cluster-a", "svc.prod.cloud-a.example")))

	require.NoError(t, p.Unpublish(context.Background(), isvc))

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a-ipv6", Namespace: "prod"}, &discoveryv1.EndpointSlice{})
	assert.True(t, apierrors.IsNotFound(err), "EndpointSlice deleted")
	err = c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &gatewayapiv1.BackendTLSPolicy{})
	assert.True(t, apierrors.IsNotFound(err), "BackendTLSPolicy deleted")
}

func TestUnpublish_MissingIsNoOp(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	require.NoError(t, p.Unpublish(context.Background(), testISVC()))
}

func TestBuildHTTPRoute_WeightedByReadyReplicas(t *testing.T) {
	p := NewGatewayAPIPublisher(nil, baseConfig())
	// Split: each home carries its ready-replica count -> proportional weights.
	target := Target{GlobalHost: "h", Homes: []Home{
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
	// No weights (Single/All, or a Split placement before any home is ready):
	// every weight is 0, so fall back to equal (1) rather than black-holing.
	target := Target{GlobalHost: "h", Homes: []Home{
		{Cluster: "cluster-a", BackendHost: "a.example"},
		{Cluster: "cluster-b", BackendHost: "b.example"},
	}}

	refs := p.buildHTTPRoute(testISVC(), target).Spec.Rules[0].BackendRefs
	require.Len(t, refs, 2)
	assert.Equal(t, int32(1), *refs[0].Weight)
	assert.Equal(t, int32(1), *refs[1].Weight)
}

func TestPublish_RouteUpdateFailureKeepsOldBackendService(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	require.NoError(t, p.Publish(context.Background(), isvc, oneHome("h", "cluster-a", "a.example")))

	p.client = routeUpdateFailClient{Client: c}
	err := p.Publish(context.Background(), isvc, oneHome("h", "cluster-b", "b.example"))
	require.Error(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &corev1.Service{}),
		"old Service remains while the route still references it")
}

func TestPublish_SharedRouteNamespaceIsolatesSameNameSources(t *testing.T) {
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).Build()
	cfg := baseConfig()
	cfg.RouteNamespace = "ome-gateways"
	p := NewGatewayAPIPublisher(c, cfg)
	a := testISVC()
	a.Namespace = "team-a"
	b := testISVC()
	b.Namespace = "team-b"

	require.NoError(t, p.Publish(context.Background(), a, oneHome("a.global.example", "cluster-a", "a.example")))
	require.NoError(t, p.Publish(context.Background(), b, oneHome("b.global.example", "cluster-b", "b.example")))
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
		"per-home names stay distinct after truncation")
}

// Two distinct sources must never share a published name. A plain
// namespace-name join collides across the hyphen, which would let one source
// overwrite or delete the other's route in a shared RouteNamespace.
func TestResourceBaseNameIsInjectiveAcrossNamespaces(t *testing.T) {
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
func TestBoundedResourceNameStartsWithLetter(t *testing.T) {
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
