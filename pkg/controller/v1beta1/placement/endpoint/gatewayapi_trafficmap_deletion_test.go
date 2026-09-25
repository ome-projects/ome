package endpoint

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestGatewayAPITrafficMapWithdrawalWaitsForRouteDeletion(t *testing.T) {
	scheme := pubScheme(t)
	c := newGatewayFakeClientBuilder(t, scheme).Build()
	cfg := baseConfig()
	cfg.GlobalHostTemplate = ""
	p := NewGatewayAPITrafficMapPublisher(c, c, cfg)
	owner := testISVC()
	owner.Annotations = map[string]string{GlobalHostAnnotation: "svc.global.example"}
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 5))

	publishPlan, err := p.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	_, err = p.Apply(context.Background(), publishPlan)
	require.NoError(t, err)
	routeKey := types.NamespacedName{Namespace: "prod", Name: "svc-global"}
	serviceKey := types.NamespacedName{Namespace: "prod", Name: p.publisher.serviceName(owner, "cluster-a")}
	addGatewayRouteTestFinalizer(t, c, routeKey)

	delete(owner.Annotations, GlobalHostAnnotation)
	withdrawPlan, err := p.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	result, err := p.Apply(context.Background(), withdrawPlan)
	require.ErrorContains(t, err, "deletion is pending")
	assert.False(t, result.Withdrawn)
	assertGatewayRouteDeletingAtZero(t, c, routeKey)
	require.NoError(t, c.Get(context.Background(), serviceKey, &corev1.Service{}),
		"subordinate resources remain journaled until the route is gone")

	releaseGatewayRouteTestFinalizer(t, c, routeKey)
	result, err = p.Apply(context.Background(), withdrawPlan)
	require.NoError(t, err)
	assert.True(t, result.Withdrawn)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), routeKey, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), serviceKey, &corev1.Service{})))
}

func TestGatewayAPITrafficMapUnpublishWaitsForRouteDeletion(t *testing.T) {
	scheme := pubScheme(t)
	c := newGatewayFakeClientBuilder(t, scheme).Build()
	p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 5))
	plan, err := p.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	_, err = p.Apply(context.Background(), plan)
	require.NoError(t, err)

	routeKey := types.NamespacedName{Namespace: "prod", Name: p.publisher.routeName(owner)}
	serviceKey := types.NamespacedName{Namespace: "prod", Name: p.publisher.serviceName(owner, "cluster-a")}
	addGatewayRouteTestFinalizer(t, c, routeKey)
	journal := v1beta1.TrafficMapPublisherStatus{
		PublisherName:  p.Name(),
		ClaimedTargets: plan.Claims(),
	}
	err = p.Unpublish(context.Background(), client.ObjectKeyFromObject(owner), journal)
	require.ErrorContains(t, err, "deletion is pending")
	assertGatewayRouteDeletingAtZero(t, c, routeKey)
	require.NoError(t, c.Get(context.Background(), serviceKey, &corev1.Service{}),
		"subordinate resources must remain until every claimed route is gone")

	releaseGatewayRouteTestFinalizer(t, c, routeKey)
	require.NoError(t, p.Unpublish(context.Background(), client.ObjectKeyFromObject(owner), journal))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), routeKey, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), serviceKey, &corev1.Service{})))
}

func addGatewayRouteTestFinalizer(
	t *testing.T,
	c client.Client,
	key types.NamespacedName,
) {
	t.Helper()
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), key, route))
	route.Finalizers = append(route.Finalizers, "example.com/hold")
	require.NoError(t, c.Update(context.Background(), route))
}

func releaseGatewayRouteTestFinalizer(
	t *testing.T,
	c client.Client,
	key types.NamespacedName,
) {
	t.Helper()
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), key, route))
	route.Finalizers = nil
	require.NoError(t, c.Update(context.Background(), route))
}

func assertGatewayRouteDeletingAtZero(
	t *testing.T,
	c client.Client,
	key types.NamespacedName,
) {
	t.Helper()
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), key, route))
	require.NotNil(t, route.DeletionTimestamp)
	for _, rule := range route.Spec.Rules {
		for _, backend := range rule.BackendRefs {
			require.NotNil(t, backend.Weight)
			assert.Zero(t, *backend.Weight)
		}
	}
	assert.Equal(t, []string{"example.com/hold"}, route.Finalizers)
}
