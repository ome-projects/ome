package endpoint

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestGatewayAPIPublisherUnpublishWaitsForRouteDeletion(t *testing.T) {
	c := newGatewayFakeClientBuilder(t, pubScheme(t)).Build()
	p := NewGatewayAPIPublisher(c, gatewayBackendConfig(), WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4}},
		"cluster-b": {{Address: "2001:db8::10", Type: discoveryv1.AddressTypeIPv6}},
	}))
	isvc := testISVC()
	target := Target{GlobalHost: "svc.prod.global.example", Homes: []Home{
		{Cluster: "cluster-a", BackendHost: "a.example", Weight: 3},
		{Cluster: "cluster-b", BackendHost: "b.example", Weight: 1},
	}}
	require.NoError(t, p.Publish(context.Background(), isvc, target))

	routeKey := types.NamespacedName{Namespace: "prod", Name: "svc-global"}
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	route.Finalizers = []string{"example.com/hold"}
	route.Spec.Rules[0].BackendRefs[0].Weight = nil
	require.NoError(t, c.Update(context.Background(), route))

	err := p.Unpublish(context.Background(), isvc)
	require.ErrorContains(t, err, "deletion is pending")
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	require.NotNil(t, route.DeletionTimestamp)
	assert.Equal(t, []string{"example.com/hold"}, route.Finalizers)
	for _, rule := range route.Spec.Rules {
		for _, backend := range rule.BackendRefs {
			require.NotNil(t, backend.Weight)
			assert.Zero(t, *backend.Weight)
		}
	}
	assertLegacyGatewayChildren(t, p, isvc, 2, 2, 2)

	route.Finalizers = nil
	require.NoError(t, c.Update(context.Background(), route))
	require.NoError(t, p.Unpublish(context.Background(), isvc))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), routeKey, &gatewayapiv1.HTTPRoute{})))
	assertLegacyGatewayChildren(t, p, isvc, 0, 0, 0)
}

func TestGatewayAPIPublisherUnpublishRouteErrorsRetainChildren(t *testing.T) {
	for _, tc := range []struct {
		name      string
		publisher func(client.Client) *GatewayAPIPublisher
	}{
		{
			name: "read",
			publisher: func(c client.Client) *GatewayAPIPublisher {
				return NewGatewayAPIPublisher(c, baseConfig(), WithGatewayAPIReader(&httpRouteGetErrorReader{
					Reader: c, failAt: 1,
				}))
			},
		},
		{
			name: "update",
			publisher: func(c client.Client) *GatewayAPIPublisher {
				return NewGatewayAPIPublisher(routeUpdateFailClient{Client: c}, baseConfig(), WithGatewayAPIReader(c))
			},
		},
		{
			name: "delete",
			publisher: func(c client.Client) *GatewayAPIPublisher {
				return NewGatewayAPIPublisher(httpRouteDeleteErrorClient{Client: c}, baseConfig(), WithGatewayAPIReader(c))
			},
		},
		{
			name: "confirmation",
			publisher: func(c client.Client) *GatewayAPIPublisher {
				return NewGatewayAPIPublisher(c, baseConfig(), WithGatewayAPIReader(&httpRouteGetErrorReader{
					Reader: c, failAt: 2,
				}))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, isvc, serviceKey := publishLegacyGatewayFixture(t)
			err := tc.publisher(c).Unpublish(context.Background(), isvc)
			require.Error(t, err)
			require.NoError(t, c.Get(context.Background(), serviceKey, &corev1.Service{}),
				"route teardown failures must retain subordinate resources")
		})
	}
}

func TestGatewayAPIPublisherUnpublishFencesPostDrainRouteReplacement(t *testing.T) {
	isvc := testISVC()
	renderer := NewGatewayAPIPublisher(nil, baseConfig())
	route := renderer.buildHTTPRoute(isvc, oneHome("svc.prod.global.example", "cluster-a", "a.example"))
	service := renderer.buildExternalNameService(isvc, renderer.serviceName(isvc, "cluster-a"), Home{
		Cluster: "cluster-a", BackendHost: "a.example",
	})
	route.UID = types.UID("owned-route")
	route.ResourceVersion = "1"
	service.UID = types.UID("owned-service")
	service.ResourceVersion = "1"
	routeKey := client.ObjectKeyFromObject(route)
	serviceKey := client.ObjectKeyFromObject(service)
	initialResourceVersion := route.ResourceVersion
	var postDrainResourceVersion string

	c := fakeclient.NewClientBuilder().
		WithScheme(pubScheme(t)).
		WithObjects(route, service).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.UpdateOption) error {
				if updated, ok := object.(*gatewayapiv1.HTTPRoute); ok {
					for _, rule := range updated.Spec.Rules {
						for _, backend := range rule.BackendRefs {
							require.NotNil(t, backend.Weight)
							assert.Zero(t, *backend.Weight)
						}
					}
				}
				if err := c.Update(ctx, object, opts...); err != nil {
					return err
				}
				if _, ok := object.(*gatewayapiv1.HTTPRoute); ok {
					postDrainResourceVersion = object.GetResourceVersion()
				}
				return nil
			},
			Delete: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.DeleteOption) error {
				deleted, ok := object.(*gatewayapiv1.HTTPRoute)
				require.True(t, ok)
				deleteOptions := (&client.DeleteOptions{}).ApplyOptions(opts)
				require.NotNil(t, deleteOptions.Preconditions)
				require.NotNil(t, deleteOptions.Preconditions.UID)
				require.NotNil(t, deleteOptions.Preconditions.ResourceVersion)
				assert.Equal(t, deleted.UID, *deleteOptions.Preconditions.UID)
				assert.Equal(t, postDrainResourceVersion, *deleteOptions.Preconditions.ResourceVersion)

				current := &gatewayapiv1.HTTPRoute{}
				require.NoError(t, c.Get(ctx, routeKey, current))
				require.NoError(t, c.Delete(ctx, current))
				replacement := current.DeepCopy()
				replacement.UID = types.UID("replacement-route")
				replacement.ResourceVersion = ""
				replacement.DeletionTimestamp = nil
				replacement.Labels[PlacementEndpointISVCLabel] = "other"
				replacement.Spec.Rules[0].BackendRefs[0].Weight = ptr.To(int32(9))
				require.NoError(t, c.Create(ctx, replacement))
				return apierrors.NewConflict(
					gatewayapiv1.Resource("httproutes"), routeKey.Name, errors.New("delete preconditions no longer match"),
				)
			},
		}).
		Build()
	p := NewGatewayAPIPublisher(c, baseConfig(), WithGatewayAPIReader(c))

	err := p.Unpublish(context.Background(), isvc)
	require.Error(t, err)
	assert.NotEmpty(t, postDrainResourceVersion)
	assert.NotEqual(t, initialResourceVersion, postDrainResourceVersion)
	replacement := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, replacement))
	assert.Equal(t, types.UID("replacement-route"), replacement.UID)
	assert.Equal(t, "other", replacement.Labels[PlacementEndpointISVCLabel])
	require.NoError(t, c.Get(context.Background(), serviceKey, &corev1.Service{}))
}

func TestGatewayAPIPublisherUnpublishUsesLiveAPIReader(t *testing.T) {
	isvc := testISVC()
	renderer := NewGatewayAPIPublisher(nil, baseConfig())
	route := renderer.buildHTTPRoute(isvc, oneHome("svc.prod.global.example", "cluster-a", "a.example"))
	route.Finalizers = []string{"example.com/hold"}
	service := renderer.buildExternalNameService(isvc, renderer.serviceName(isvc, "cluster-a"), Home{
		Cluster: "cluster-a", BackendHost: "a.example",
	})
	setGatewayFakeObjectIdentity(route, service)
	live := fakeclient.NewClientBuilder().WithScheme(pubScheme(t)).WithObjects(route, service).Build()
	stale := blindGatewayCacheClient{Client: live}
	p := NewGatewayAPIPublisher(stale, baseConfig(), WithGatewayAPIReader(live))

	err := p.Unpublish(context.Background(), isvc)
	require.ErrorContains(t, err, "deletion is pending")
	current := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(route), current))
	require.NotNil(t, current.DeletionTimestamp)
	require.NotNil(t, current.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Zero(t, *current.Spec.Rules[0].BackendRefs[0].Weight)
	require.NoError(t, live.Get(context.Background(), client.ObjectKeyFromObject(service), &corev1.Service{}))
}

func TestGatewayAPIPublisherUnpublishPreservesLegacyUnlabeledRouteOwnership(t *testing.T) {
	c, isvc, serviceKey := publishLegacyGatewayFixture(t)
	p := NewGatewayAPIPublisher(c, baseConfig())
	routeKey := types.NamespacedName{Namespace: "prod", Name: "svc-global"}
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	route.Labels = nil
	require.NoError(t, c.Update(context.Background(), route))

	require.NoError(t, p.Unpublish(context.Background(), isvc))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), routeKey, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), serviceKey, &corev1.Service{})))
}

func TestGatewayAPIPublisherUnpublishRejectsForeignRouteBeforeChildCleanup(t *testing.T) {
	c, isvc, serviceKey := publishLegacyGatewayFixture(t)
	p := NewGatewayAPIPublisher(c, baseConfig())
	routeKey := types.NamespacedName{Namespace: "prod", Name: "svc-global"}
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	route.Labels[PlacementEndpointISVCLabel] = "other"
	require.NoError(t, c.Update(context.Background(), route))

	require.ErrorContains(t, p.Unpublish(context.Background(), isvc), "belongs to another InferenceService")
	require.NoError(t, c.Get(context.Background(), serviceKey, &corev1.Service{}))
}

func TestGatewayAPIPublisherUnpublishAlreadyTerminatingRoute(t *testing.T) {
	c, isvc, serviceKey := publishLegacyGatewayFixture(t)
	routeKey := types.NamespacedName{Namespace: "prod", Name: "svc-global"}
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	route.Finalizers = []string{"example.com/hold"}
	require.NoError(t, c.Update(context.Background(), route))
	require.NoError(t, c.Delete(context.Background(), route))

	deleteCalls := 0
	countingClient := &httpRouteDeleteCountingClient{Client: c, calls: &deleteCalls}
	p := NewGatewayAPIPublisher(countingClient, baseConfig(), WithGatewayAPIReader(c))
	require.ErrorContains(t, p.Unpublish(context.Background(), isvc), "deletion is pending")
	assert.Zero(t, deleteCalls, "an already terminating route must not receive another delete request")
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	require.NotNil(t, route.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Zero(t, *route.Spec.Rules[0].BackendRefs[0].Weight)
	require.NoError(t, c.Get(context.Background(), serviceKey, &corev1.Service{}))
}

func TestGatewayAPIPublisherUnpublishAbsentRouteCleansChildren(t *testing.T) {
	c, isvc, serviceKey := publishLegacyGatewayFixture(t)
	routeKey := types.NamespacedName{Namespace: "prod", Name: "svc-global"}
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	require.NoError(t, c.Delete(context.Background(), route))

	require.NoError(t, NewGatewayAPIPublisher(c, baseConfig()).Unpublish(context.Background(), isvc))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), serviceKey, &corev1.Service{})))
}

type httpRouteGetErrorReader struct {
	client.Reader
	reads  int
	failAt int
}

func (r *httpRouteGetErrorReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if _, ok := object.(*gatewayapiv1.HTTPRoute); ok {
		r.reads++
		if r.reads == r.failAt {
			return errors.New("injected HTTPRoute read failure")
		}
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

type httpRouteDeleteErrorClient struct {
	client.Client
}

func (c httpRouteDeleteErrorClient) Delete(
	ctx context.Context,
	object client.Object,
	opts ...client.DeleteOption,
) error {
	if _, ok := object.(*gatewayapiv1.HTTPRoute); ok {
		return apierrors.NewConflict(
			schema.GroupResource{Group: gatewayapiv1.GroupVersion.Group, Resource: "httproutes"},
			object.GetName(), errors.New("injected HTTPRoute delete failure"),
		)
	}
	return c.Client.Delete(ctx, object, opts...)
}

type httpRouteDeleteCountingClient struct {
	client.Client
	calls *int
}

func (c httpRouteDeleteCountingClient) Delete(
	ctx context.Context,
	object client.Object,
	opts ...client.DeleteOption,
) error {
	if _, ok := object.(*gatewayapiv1.HTTPRoute); ok {
		(*c.calls)++
	}
	return c.Client.Delete(ctx, object, opts...)
}

func publishLegacyGatewayFixture(
	t *testing.T,
) (client.Client, *v1beta1.InferenceService, types.NamespacedName) {
	t.Helper()
	c := newGatewayFakeClientBuilder(t, pubScheme(t)).Build()
	p := NewGatewayAPIPublisher(c, baseConfig())
	isvc := testISVC()
	require.NoError(t, p.Publish(context.Background(), isvc,
		oneHome("svc.prod.global.example", "cluster-a", "a.example")))
	return c, isvc, types.NamespacedName{Namespace: "prod", Name: "svc-global-cluster-a"}
}

func assertLegacyGatewayChildren(
	t *testing.T,
	p *GatewayAPIPublisher,
	isvc *v1beta1.InferenceService,
	serviceCount int,
	endpointSliceCount int,
	policyCount int,
) {
	t.Helper()
	sourceKey := client.ObjectKeyFromObject(isvc)
	namespace := p.routeNamespace(isvc)
	services, err := p.sourceServices(context.Background(), namespace, sourceKey)
	require.NoError(t, err)
	assert.Len(t, services, serviceCount)
	endpointSlices, err := p.sourceEndpointSlices(context.Background(), namespace, sourceKey)
	require.NoError(t, err)
	assert.Len(t, endpointSlices, endpointSliceCount)
	policies, err := p.sourceBackendTLSPolicies(context.Background(), namespace, sourceKey)
	require.NoError(t, err)
	assert.Len(t, policies, policyCount)
}
