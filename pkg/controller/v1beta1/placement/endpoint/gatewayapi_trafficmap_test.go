package endpoint

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func gatewayTrafficMap(owner *v1beta1.InferenceService, entries ...v1beta1.TrafficMapEntry) *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: owner.Name, Namespace: owner.Namespace},
		Spec: v1beta1.TrafficMapSpec{
			Service: owner.Name,
			Entries: entries,
		},
		Status: v1beta1.TrafficMapStatus{SourceUID: owner.UID},
	}
}

func gatewayTrafficMapEntry(cluster, host string, weight int32) v1beta1.TrafficMapEntry {
	return v1beta1.TrafficMapEntry{
		Cluster:  cluster,
		Endpoint: &apis.URL{Scheme: "https", Host: host},
		Weight:   weight,
	}
}

type staleGatewayAPIReadClient struct {
	client.Client
}

func (c staleGatewayAPIReadClient) Get(
	_ context.Context,
	key client.ObjectKey,
	_ client.Object,
	_ ...client.GetOption,
) error {
	return apierrors.NewNotFound(schema.GroupResource{Group: gatewayapiv1.GroupVersion.Group, Resource: "objects"}, key.Name)
}

func (c staleGatewayAPIReadClient) List(
	_ context.Context,
	_ client.ObjectList,
	_ ...client.ListOption,
) error {
	return nil
}

type failingGatewayAPIReader struct {
	client.Reader
	getErr    error
	listErr   error
	getCalls  int
	listCalls int
}

func (r *failingGatewayAPIReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	r.getCalls++
	if r.getErr != nil {
		return r.getErr
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func (r *failingGatewayAPIReader) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	r.listCalls++
	if r.listErr != nil {
		return r.listErr
	}
	return r.Reader.List(ctx, list, opts...)
}

func gatewayCollisionRoute(
	namespace,
	name,
	hostname string,
	parent gatewayapiv1.ParentReference,
) *gatewayapiv1.HTTPRoute {
	return &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: gatewayapiv1.HTTPRouteSpec{
			CommonRouteSpec: gatewayapiv1.CommonRouteSpec{
				ParentRefs: []gatewayapiv1.ParentReference{parent},
			},
			Hostnames: []gatewayapiv1.Hostname{gatewayapiv1.Hostname(hostname)},
		},
	}
}

func gatewayTrafficMapWithJournal(
	owner *v1beta1.InferenceService,
	routeKey types.NamespacedName,
	entries ...v1beta1.TrafficMapEntry,
) *v1beta1.TrafficMap {
	trafficMap := gatewayTrafficMap(owner, entries...)
	trafficMap.UID = types.UID("map-" + owner.Name)
	trafficMap.Generation = 1
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(owner, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
	}
	trafficMap.Spec.ObservedISVCGeneration = owner.Generation
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
		PublisherName:  GatewayAPITrafficMapPublisherName,
		ClaimedTargets: []string{gatewayAPIHTTPRouteClaim(routeKey)},
	}
	return trafficMap
}

func TestGatewayAPITrafficMapPublisherIdentityAndOptions(t *testing.T) {
	p := NewGatewayAPITrafficMapPublisher(nil, nil, baseConfig())

	assert.Equal(t, GatewayAPITrafficMapPublisherName, p.Name())
	assert.Equal(t, "gatewayapi", p.Name())
	assert.True(t, p.Stateful())
	resolved, err := p.ResolveOptions(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, resolved)

	_, err = p.ResolveOptions(nil, map[string]string{"z": "1", "a": "2"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "per-InferenceService options: a, z")
	_, err = p.ResolveOptions(map[string]string{"unused": "value"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "global options: unused")
}

func TestGatewayAPITrafficMapPreflightRejectsExactGatewayHostnameCollisions(t *testing.T) {
	const hostname = "shared.global.example"
	explicitGatewayNamespace := gatewayapiv1.Namespace("ome-system")
	desiredNamespace := gatewayapiv1.Namespace("prod")
	tests := []struct {
		name   string
		config Config
		route  *gatewayapiv1.HTTPRoute
	}{
		{
			name:   "foreign route",
			config: baseConfig(),
			route: gatewayCollisionRoute("foreign", "occupied", hostname, gatewayapiv1.ParentReference{
				Name: "global-gw", Namespace: &explicitGatewayNamespace,
			}),
		},
		{
			name:   "terminating route",
			config: baseConfig(),
			route: gatewayCollisionRoute("foreign", "terminating", hostname, gatewayapiv1.ParentReference{
				Name: "global-gw", Namespace: &explicitGatewayNamespace,
			}),
		},
		{
			name:   "bare existing parent matches explicit configured parent",
			config: baseConfig(),
			route: gatewayCollisionRoute("ome-system", "bare-parent", hostname, gatewayapiv1.ParentReference{
				Name: "global-gw",
			}),
		},
		{
			name: "explicit existing parent matches bare configured parent",
			config: func() Config {
				cfg := baseConfig()
				cfg.GlobalGateway = "global-gw"
				return cfg
			}(),
			route: gatewayCollisionRoute("foreign", "explicit-parent", hostname, gatewayapiv1.ParentReference{
				Name: "global-gw", Namespace: &desiredNamespace,
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner := testISVC()
			owner.Annotations = map[string]string{GlobalHostAnnotation: hostname}
			if tt.name == "terminating route" {
				now := metav1.Now()
				tt.route.DeletionTimestamp = &now
				tt.route.Finalizers = []string{"test.example/finalizer"}
			}
			c := fakeclient.NewClientBuilder().WithScheme(pubScheme(t)).WithObjects(tt.route).Build()
			publisher := NewGatewayAPITrafficMapPublisher(c, c, tt.config)
			plan, err := publisher.Plan(owner, gatewayTrafficMap(owner), nil)
			require.NoError(t, err)

			err = publisher.Preflight(context.Background(), plan)
			var terminal *terminalPublisherError
			require.ErrorAs(t, err, &terminal)
			assert.ErrorContains(t, terminal.err, "already published")
		})
	}
}

func TestGatewayAPITrafficMapPreflightAllowsDistinctTargets(t *testing.T) {
	const hostname = "shared.global.example"
	gatewayNamespace := gatewayapiv1.Namespace("ome-system")
	routes := []client.Object{
		gatewayCollisionRoute("foreign", "different-host", "other.global.example", gatewayapiv1.ParentReference{
			Name: "global-gw", Namespace: &gatewayNamespace,
		}),
		gatewayCollisionRoute("foreign", "different-gateway", hostname, gatewayapiv1.ParentReference{
			Name: "other-gw", Namespace: &gatewayNamespace,
		}),
	}
	owner := testISVC()
	owner.Annotations = map[string]string{GlobalHostAnnotation: hostname}
	c := fakeclient.NewClientBuilder().WithScheme(pubScheme(t)).WithObjects(routes...).Build()
	publisher := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	plan, err := publisher.Plan(owner, gatewayTrafficMap(owner), nil)
	require.NoError(t, err)
	require.NoError(t, publisher.Preflight(context.Background(), plan))
}

func TestGatewayAPITrafficMapPreflightVerifiesDesiredRouteOwnership(t *testing.T) {
	owner := testISVC()
	foreign := gatewayCollisionRoute("prod", "svc-global", "unrelated.example", gatewayapiv1.ParentReference{
		Name: "other-gateway",
	})
	c := fakeclient.NewClientBuilder().WithScheme(pubScheme(t)).WithObjects(foreign).Build()
	publisher := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	plan, err := publisher.Plan(owner, gatewayTrafficMap(owner), nil)
	require.NoError(t, err)

	err = publisher.Preflight(context.Background(), plan)
	var terminal *terminalPublisherError
	require.ErrorAs(t, err, &terminal)
	assert.ErrorContains(t, terminal.err, "belongs to another source")
}

func TestGatewayAPITrafficMapPreflightUsesUncachedReader(t *testing.T) {
	const hostname = "shared.global.example"
	gatewayNamespace := gatewayapiv1.Namespace("ome-system")
	collision := gatewayCollisionRoute("foreign", "occupied", hostname, gatewayapiv1.ParentReference{
		Name: "global-gw", Namespace: &gatewayNamespace,
	})
	live := fakeclient.NewClientBuilder().WithScheme(pubScheme(t)).WithObjects(collision).Build()
	owner := testISVC()
	owner.Annotations = map[string]string{GlobalHostAnnotation: hostname}
	publisher := NewGatewayAPITrafficMapPublisher(staleGatewayAPIReadClient{Client: live}, live, baseConfig())
	plan, err := publisher.Plan(owner, gatewayTrafficMap(owner), nil)
	require.NoError(t, err)

	err = publisher.Preflight(context.Background(), plan)
	var terminal *terminalPublisherError
	require.ErrorAs(t, err, &terminal)
	assert.ErrorContains(t, terminal.err, "already published")
}

func TestGatewayAPITrafficMapPreflightSkipsWithdrawal(t *testing.T) {
	reader := &failingGatewayAPIReader{
		getErr:  errors.New("unexpected get"),
		listErr: errors.New("unexpected list"),
	}
	cfg := baseConfig()
	cfg.BackendPort = 0
	owner := testISVC()
	publisher := NewGatewayAPITrafficMapPublisher(nil, reader, cfg)
	plan, err := publisher.Plan(owner, gatewayTrafficMap(owner), nil)
	require.NoError(t, err)

	require.NoError(t, publisher.Preflight(context.Background(), plan))
	assert.Zero(t, reader.getCalls)
	assert.Zero(t, reader.listCalls)
}

func TestGatewayAPITrafficMapPreflightReadFailuresAreRetryable(t *testing.T) {
	tests := []struct {
		name    string
		getErr  error
		listErr error
		want    string
	}{
		{name: "get", getErr: errors.New("get failed"), want: "get failed"},
		{name: "list", listErr: errors.New("list failed"), want: "list failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			live := fakeclient.NewClientBuilder().WithScheme(pubScheme(t)).Build()
			reader := &failingGatewayAPIReader{Reader: live, getErr: tt.getErr, listErr: tt.listErr}
			owner := testISVC()
			publisher := NewGatewayAPITrafficMapPublisher(live, reader, baseConfig())
			plan, err := publisher.Plan(owner, gatewayTrafficMap(owner), nil)
			require.NoError(t, err)

			err = publisher.Preflight(context.Background(), plan)
			require.ErrorContains(t, err, tt.want)
			var terminal *terminalPublisherError
			assert.False(t, errors.As(err, &terminal))
		})
	}
}

func TestGatewayAPITrafficMapHostnameChangeCollisionPreservesPublishedRoute(t *testing.T) {
	const (
		oldHostname = "h1.global.example"
		newHostname = "h2.global.example"
	)
	cfg := baseConfig()
	owner := testISVC()
	owner.Generation = 1
	owner.Annotations = map[string]string{GlobalHostAnnotation: newHostname}
	renderer := NewGatewayAPITrafficMapPublisher(nil, nil, cfg)
	routeKey := types.NamespacedName{
		Namespace: renderer.publisher.routeNamespace(owner),
		Name:      renderer.publisher.routeName(owner),
	}
	trafficMap := gatewayTrafficMapWithJournal(owner, routeKey,
		gatewayTrafficMapEntry("cluster-a", "new-backend.example", 99),
	)
	trafficMap.Status.Published = true
	trafficMap.Status.ObservedTrafficMapGeneration = trafficMap.Generation
	oldRoute := renderer.publisher.buildHTTPRoute(owner, Target{
		GlobalHost:           oldHostname,
		WeightsAuthoritative: true,
		Homes: []Home{{
			Cluster: "cluster-a", BackendHost: "old-backend.example", Weight: 37,
		}},
	})
	gatewayNamespace := gatewayapiv1.Namespace("ome-system")
	occupied := gatewayCollisionRoute("foreign", "occupied-h2", newHostname, gatewayapiv1.ParentReference{
		Name: "global-gw", Namespace: &gatewayNamespace,
	})
	scheme := pubScheme(t)
	kubeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.TrafficMap{}).
		WithObjects(owner, trafficMap, oldRoute, occupied).
		Build()
	publisher := NewGatewayAPITrafficMapPublisher(kubeClient, kubeClient, cfg)
	reconciler := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)

	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionFalse, condition.Status)
	assert.Equal(t, trafficMapPublisherReasonClaimRejected, condition.Reason)
	require.NotNil(t, current.Status.Publisher)
	assert.Equal(t, []string{gatewayAPIHTTPRouteClaim(routeKey)}, current.Status.Publisher.ClaimedTargets)

	gotRoute := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, kubeClient.Get(context.Background(), routeKey, gotRoute))
	assert.Equal(t, []gatewayapiv1.Hostname{oldHostname}, gotRoute.Spec.Hostnames)
	require.Len(t, gotRoute.Spec.Rules, 1)
	require.Len(t, gotRoute.Spec.Rules[0].BackendRefs, 1)
	assert.Equal(t, int32(37), *gotRoute.Spec.Rules[0].BackendRefs[0].Weight,
		"the old route must not be drained or updated before collision rejection")
	routes := &gatewayapiv1.HTTPRouteList{}
	require.NoError(t, kubeClient.List(context.Background(), routes))
	assert.Len(t, routes.Items, 2, "collision rejection must not create another HTTPRoute")
	assert.True(t, apierrors.IsNotFound(kubeClient.Get(context.Background(), types.NamespacedName{
		Namespace: routeKey.Namespace,
		Name:      renderer.publisher.serviceName(owner, "cluster-a"),
	}, &corev1.Service{})), "collision rejection must not apply backend Services")
}

func TestGatewayAPITrafficMapLegacyDuplicateRoutesBothReportCollision(t *testing.T) {
	const hostname = "duplicate.global.example"
	cfg := baseConfig()
	renderer := NewGatewayAPITrafficMapPublisher(nil, nil, cfg)
	owners := []*v1beta1.InferenceService{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "svc-a", Namespace: "prod", UID: "owner-a", Generation: 1,
				Annotations: map[string]string{GlobalHostAnnotation: hostname},
			},
			Spec: v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{
				Mode: v1beta1.PlacementModeSingle, Requirements: "accelerator=test",
			}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "svc-b", Namespace: "prod", UID: "owner-b", Generation: 1,
				Annotations: map[string]string{GlobalHostAnnotation: hostname},
			},
			Spec: v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{
				Mode: v1beta1.PlacementModeSingle, Requirements: "accelerator=test",
			}},
		},
	}
	objects := make([]client.Object, 0, 6)
	maps := make([]*v1beta1.TrafficMap, 0, len(owners))
	for _, owner := range owners {
		routeKey := types.NamespacedName{
			Namespace: renderer.publisher.routeNamespace(owner),
			Name:      renderer.publisher.routeName(owner),
		}
		trafficMap := gatewayTrafficMapWithJournal(owner, routeKey)
		route := renderer.publisher.buildHTTPRoute(owner, Target{
			GlobalHost: hostname, WeightsAuthoritative: true,
		})
		maps = append(maps, trafficMap)
		objects = append(objects, owner, trafficMap, route)
	}
	scheme := pubScheme(t)
	kubeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.TrafficMap{}).
		WithObjects(objects...).
		Build()
	publisher := NewGatewayAPITrafficMapPublisher(kubeClient, kubeClient, cfg)
	reconciler := &TrafficMapPublisherReconciler{
		Client: kubeClient, APIReader: kubeClient, Log: logr.Discard(), Publisher: publisher, Active: true,
	}

	for _, trafficMap := range maps {
		_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
		require.NoError(t, err)
		current := getPublisherTrafficMap(t, kubeClient, trafficMap)
		condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
		require.NotNil(t, condition)
		assert.Equal(t, metav1.ConditionFalse, condition.Status)
		assert.Equal(t, trafficMapPublisherReasonClaimRejected, condition.Reason)
		require.NotNil(t, current.Status.Publisher)
		assert.Equal(t, []string{gatewayAPIHTTPRouteClaim(types.NamespacedName{
			Namespace: "prod", Name: trafficMap.Name + resourceSuffix,
		})}, current.Status.Publisher.ClaimedTargets,
			"legacy route-only journals remain valid while both duplicates are diagnosed")
	}
}

func TestGatewayAPITrafficMapPlanIsImmutableAndAppliesWeights(t *testing.T) {
	scheme := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	cfg := baseConfig()
	p := NewGatewayAPITrafficMapPublisher(c, c, cfg)
	owner := testISVC()
	owner.Annotations = map[string]string{GlobalHostAnnotation: "original.global.example"}
	trafficMap := gatewayTrafficMap(owner,
		gatewayTrafficMapEntry("cluster-b", "b.example:443", 0),
		gatewayTrafficMapEntry("cluster-a", "a.example:443", 7),
	)

	plan, err := p.Plan(owner, trafficMap, map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, []string{"v1:httproute:prod/svc-global"}, plan.Claims())

	owner.Annotations[GlobalHostAnnotation] = "mutated.global.example"
	trafficMap.Spec.Entries[0].Weight = 99
	cfg.Labels["team"] = "mutated"
	claims := plan.Claims()
	claims[0] = "mutated"
	assert.Equal(t, []string{"v1:httproute:prod/svc-global"}, plan.Claims())

	result, err := p.Apply(context.Background(), plan)
	require.NoError(t, err)
	require.NotNil(t, result.GatewayRef)
	assert.Equal(t, gatewayapiv1.GroupVersion.Group, result.GatewayRef.Group)
	assert.Equal(t, gatewayAPIHTTPRouteKind, result.GatewayRef.Kind)
	assert.Equal(t, "prod", result.GatewayRef.Namespace)
	assert.Equal(t, "svc-global", result.GatewayRef.Name)

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, route))
	assert.Equal(t, []gatewayapiv1.Hostname{"original.global.example"}, route.Spec.Hostnames)
	assert.Equal(t, "platform", route.Labels["team"])
	require.Len(t, route.Spec.Rules, 1)
	require.Len(t, route.Spec.Rules[0].BackendRefs, 2)
	assert.Equal(t, gatewayapiv1.ObjectName(p.publisher.serviceName(owner, "cluster-a")), route.Spec.Rules[0].BackendRefs[0].Name)
	assert.Equal(t, int32(7), *route.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Equal(t, gatewayapiv1.ObjectName(p.publisher.serviceName(owner, "cluster-b")), route.Spec.Rules[0].BackendRefs[1].Name)
	assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[1].Weight)
}

func TestGatewayAPITrafficMapBackendNamesUseStructuredIdentities(t *testing.T) {
	legacy := NewGatewayAPIPublisher(nil, baseConfig())
	trafficMap := NewGatewayAPITrafficMapPublisher(nil, nil, baseConfig())
	assert.Equal(t, "svc-global", legacy.routeName(testISVC()))
	assert.Equal(t, "svc-global-cluster-a", legacy.serviceName(testISVC(), "cluster-a"))

	dotted := testISVC()
	dotted.Name = "9model.team"
	cluster := "cluster.us"
	backendName := trafficMap.publisher.serviceName(dotted, cluster)
	assert.Empty(t, validation.IsDNS1035Label(backendName))
	assert.LessOrEqual(t, len(backendName+ipv4EndpointSliceSuffix), validation.DNS1035LabelMaxLength)
	assert.Equal(t, backendName, trafficMap.publisher.serviceName(dotted, cluster), "names are deterministic")
	assert.Equal(t, legacy.routeName(dotted), trafficMap.publisher.routeName(dotted),
		"TrafficMap route keys retain the durable legacy naming contract")
	_, err := trafficMap.Plan(dotted, gatewayTrafficMap(dotted,
		gatewayTrafficMapEntry(cluster, "backend.example", 1),
	), nil)
	require.NoError(t, err)
	maximal := testISVC()
	maximal.Name = strings.Repeat("s", 63)
	maximalCluster := strings.Repeat("c", 63)
	maximalBackendName := trafficMap.publisher.serviceName(maximal, maximalCluster)
	assert.Empty(t, validation.IsDNS1035Label(maximalBackendName))
	for _, suffix := range []string{ipv4EndpointSliceSuffix, ipv6EndpointSliceSuffix} {
		derivedName := maximalBackendName + suffix
		assert.Empty(t, validation.IsDNS1123Label(derivedName))
		assert.LessOrEqual(t, len(derivedName), validation.DNS1123LabelMaxLength)
	}
	_, err = trafficMap.Plan(maximal, gatewayTrafficMap(maximal,
		gatewayTrafficMapEntry(maximalCluster, "backend.example", 1),
	), nil)
	require.NoError(t, err)

	numeric := testISVC()
	numeric.Name = "9model"
	letterPrefixed := testISVC()
	letterPrefixed.Name = "a9model"
	assert.Equal(t, legacy.routeName(numeric), trafficMap.publisher.routeName(numeric))
	assert.Equal(t, legacy.routeName(letterPrefixed), trafficMap.publisher.routeName(letterPrefixed))
	assert.Equal(t, trafficMap.publisher.routeName(numeric), trafficMap.publisher.routeName(letterPrefixed),
		"the legacy-compatible route collision remains visible to durable claim ownership")
	assert.NotEqual(t,
		trafficMap.publisher.serviceName(numeric, "cluster-a"),
		trafficMap.publisher.serviceName(letterPrefixed, "cluster-a"),
	)

	first := testISVC()
	first.Name = "model"
	second := testISVC()
	second.Name = "model-global-blue"
	assert.Equal(t,
		legacy.serviceName(first, "blue-global-west"),
		legacy.serviceName(second, "west"),
		"the legacy flattened tuples demonstrate the collision fixture",
	)
	assert.NotEqual(t,
		trafficMap.publisher.serviceName(first, "blue-global-west"),
		trafficMap.publisher.serviceName(second, "west"),
	)
}

func TestGatewayAPITrafficMapMigratesLegacyBackendResourcesAfterRouteSwitch(t *testing.T) {
	cfg := gatewayBackendConfig()
	owner := testISVC()
	home := Home{Cluster: "cluster-a", BackendHost: "backend.example", Weight: 5}
	target := Target{
		GlobalHost:           "svc.prod.global.example",
		Homes:                []Home{home},
		WeightsAuthoritative: true,
	}
	addresses := []BackendAddress{{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4}}
	resolver := staticBackendAddressResolver{home.Cluster: addresses}

	legacy := NewGatewayAPIPublisher(nil, cfg, WithBackendAddressResolver(resolver))
	legacyServiceName := legacy.serviceName(owner, home.Cluster)
	legacyService := legacy.buildExternalNameService(owner, legacyServiceName, home)
	legacySlices, err := legacy.buildEndpointSlices(owner, legacyServiceName, home, addresses)
	require.NoError(t, err)
	require.Len(t, legacySlices, 1)
	legacyPolicy := legacy.buildBackendTLSPolicy(owner, legacyServiceName, home)
	legacyRoute := legacy.buildHTTPRoute(owner, target)

	objects := []client.Object{legacyService, legacySlices[0], legacyPolicy, legacyRoute}
	setGatewayFakeObjectIdentity(objects...)
	kubeClient := fakeclient.NewClientBuilder().WithScheme(pubScheme(t)).WithObjects(objects...).Build()
	publisher := NewGatewayAPITrafficMapPublisher(
		kubeClient,
		kubeClient,
		cfg,
		WithBackendAddressResolver(resolver),
	)
	plan, err := publisher.Plan(owner, gatewayTrafficMap(owner,
		gatewayTrafficMapEntry(home.Cluster, home.BackendHost, home.Weight),
	), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"v1:httproute:prod/svc-global"}, plan.Claims())
	assert.Equal(t, "svc-global", legacyRoute.Name)

	v2ServiceName := publisher.publisher.serviceName(owner, home.Cluster)
	require.NotEqual(t, legacyServiceName, v2ServiceName)
	v2Slices, err := publisher.publisher.buildEndpointSlices(owner, v2ServiceName, home, addresses)
	require.NoError(t, err)
	require.Len(t, v2Slices, 1)

	publisher.publisher.client = routeUpdateFailClient{Client: kubeClient}
	_, err = publisher.Apply(context.Background(), plan)
	require.ErrorContains(t, err, "route update failed")

	routeKey := client.ObjectKeyFromObject(legacyRoute)
	currentRoute := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, kubeClient.Get(context.Background(), routeKey, currentRoute))
	require.Len(t, currentRoute.Spec.Rules[0].BackendRefs, 1)
	assert.Equal(t, gatewayapiv1.ObjectName(legacyServiceName), currentRoute.Spec.Rules[0].BackendRefs[0].Name)
	for _, object := range []client.Object{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: legacyServiceName}},
		&discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: legacySlices[0].Name}},
		&gatewayapiv1.BackendTLSPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: legacyPolicy.Name}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: v2ServiceName}},
		&discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: v2Slices[0].Name}},
		&gatewayapiv1.BackendTLSPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: v2ServiceName}},
	} {
		require.NoError(t, kubeClient.Get(context.Background(), client.ObjectKeyFromObject(object), object),
			"both generations remain until the route switches")
	}

	publisher.publisher.client = kubeClient
	_, err = publisher.Apply(context.Background(), plan)
	require.NoError(t, err)
	require.NoError(t, kubeClient.Get(context.Background(), routeKey, currentRoute))
	require.Len(t, currentRoute.Spec.Rules[0].BackendRefs, 1)
	assert.Equal(t, gatewayapiv1.ObjectName(v2ServiceName), currentRoute.Spec.Rules[0].BackendRefs[0].Name)
	v2Service := &corev1.Service{}
	require.NoError(t, kubeClient.Get(context.Background(), types.NamespacedName{
		Namespace: owner.Namespace,
		Name:      v2ServiceName,
	}, v2Service))
	v2Slice := &discoveryv1.EndpointSlice{}
	require.NoError(t, kubeClient.Get(context.Background(), types.NamespacedName{
		Namespace: owner.Namespace,
		Name:      v2Slices[0].Name,
	}, v2Slice))
	assert.Equal(t, v2ServiceName, v2Slice.Labels[discoveryv1.LabelServiceName])
	v2Policy := &gatewayapiv1.BackendTLSPolicy{}
	require.NoError(t, kubeClient.Get(context.Background(), types.NamespacedName{
		Namespace: owner.Namespace,
		Name:      v2ServiceName,
	}, v2Policy))
	require.Len(t, v2Policy.Spec.TargetRefs, 1)
	assert.Equal(t, gatewayapiv1.ObjectName(v2ServiceName), v2Policy.Spec.TargetRefs[0].Name)
	for _, object := range []client.Object{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: legacyServiceName}},
		&discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: legacySlices[0].Name}},
		&gatewayapiv1.BackendTLSPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: owner.Namespace, Name: legacyPolicy.Name}},
	} {
		assert.True(t, apierrors.IsNotFound(kubeClient.Get(context.Background(), client.ObjectKeyFromObject(object), object)),
			"legacy children are pruned only after the route references v2")
	}
}

func TestGatewayAPITrafficMapApplyPreservesEmptyAndAllZeroMaps(t *testing.T) {
	scheme := pubScheme(t)
	c := newGatewayFakeClientBuilder(t, scheme).Build()
	p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	owner := testISVC()

	nonEmpty, err := p.Plan(owner, gatewayTrafficMap(owner,
		gatewayTrafficMapEntry("cluster-a", "a.example", 1),
	), nil)
	require.NoError(t, err)
	_, err = p.Apply(context.Background(), nonEmpty)
	require.NoError(t, err)

	empty, err := p.Plan(owner, gatewayTrafficMap(owner), nil)
	require.NoError(t, err)
	result, err := p.Apply(context.Background(), empty)
	require.NoError(t, err)
	require.NotNil(t, result.GatewayRef)
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, route))
	require.Len(t, route.Spec.Rules, 1)
	assert.Empty(t, route.Spec.Rules[0].BackendRefs)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: p.publisher.serviceName(owner, "cluster-a")}, &corev1.Service{})))

	allZero, err := p.Plan(owner, gatewayTrafficMap(owner,
		gatewayTrafficMapEntry("cluster-a", "a.example", 0),
		gatewayTrafficMapEntry("cluster-b", "b.example", 0),
	), nil)
	require.NoError(t, err)
	_, err = p.Apply(context.Background(), allZero)
	require.NoError(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, route))
	require.Len(t, route.Spec.Rules[0].BackendRefs, 2)
	assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[1].Weight)
}

func TestGatewayAPITrafficMapApplyWithdrawsWhenHostIsRemoved(t *testing.T) {
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

	delete(owner.Annotations, GlobalHostAnnotation)
	trafficMap.Spec.Entries = make([]v1beta1.TrafficMapEntry, maxGatewayAPIBackendRefs+1)
	withdrawPlan, err := p.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	assert.Equal(t, publishPlan.Claims(), withdrawPlan.Claims())
	result, err := p.Apply(context.Background(), withdrawPlan)
	require.NoError(t, err)
	assert.Nil(t, result.GatewayRef)
	assert.True(t, result.Withdrawn)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: p.publisher.serviceName(owner, "cluster-a")}, &corev1.Service{})))
}

func TestGatewayAPITrafficMapApplyWithdrawsWhenConfigIsDisabled(t *testing.T) {
	scheme := pubScheme(t)
	c := newGatewayFakeClientBuilder(t, scheme).Build()
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 5))

	enabled := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	plan, err := enabled.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	_, err = enabled.Apply(context.Background(), plan)
	require.NoError(t, err)

	disabledConfig := baseConfig()
	disabledConfig.BackendPort = 0
	disabled := NewGatewayAPITrafficMapPublisher(c, c, disabledConfig)
	plan, err = disabled.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	result, err := disabled.Apply(context.Background(), plan)
	require.NoError(t, err)
	assert.Nil(t, result.GatewayRef)
	assert.True(t, result.Withdrawn)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})))
}

func TestGatewayAPITrafficMapApplyRejectsUnownedResourcesBeforeMutation(t *testing.T) {
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 5))

	t.Run("route", func(t *testing.T) {
		scheme := pubScheme(t)
		foreign := &gatewayapiv1.HTTPRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "svc-global"},
			Spec: gatewayapiv1.HTTPRouteSpec{
				Hostnames: []gatewayapiv1.Hostname{"foreign.example"},
			},
		}
		c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()
		p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
		plan, err := p.Plan(owner, trafficMap, nil)
		require.NoError(t, err)

		_, err = p.Apply(context.Background(), plan)
		require.ErrorContains(t, err, "belongs to another source")
		got := &gatewayapiv1.HTTPRoute{}
		require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, got))
		assert.Equal(t, []gatewayapiv1.Hostname{"foreign.example"}, got.Spec.Hostnames)
		assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
			types.NamespacedName{Namespace: "prod", Name: p.publisher.serviceName(owner, "cluster-a")}, &corev1.Service{})),
			"strict preflight must reject the route before creating a backend Service")
	})

	t.Run("service", func(t *testing.T) {
		scheme := pubScheme(t)
		p := NewGatewayAPITrafficMapPublisher(nil, nil, baseConfig())
		serviceName := p.publisher.serviceName(owner, "cluster-a")
		foreign := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: serviceName},
			Spec: corev1.ServiceSpec{
				Type:         corev1.ServiceTypeExternalName,
				ExternalName: "foreign.example",
			},
		}
		c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()
		p = NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
		plan, err := p.Plan(owner, trafficMap, nil)
		require.NoError(t, err)

		_, err = p.Apply(context.Background(), plan)
		require.ErrorContains(t, err, "belongs to another source")
		got := &corev1.Service{}
		require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: serviceName}, got))
		assert.Equal(t, "foreign.example", got.Spec.ExternalName)
		assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
			types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})),
			"strict preflight must reject the Service before creating an HTTPRoute")
	})
}

func TestGatewayAPITrafficMapPlanRejectsInvalidInputs(t *testing.T) {
	p := NewGatewayAPITrafficMapPublisher(nil, nil, baseConfig())
	owner := testISVC()

	entries := make([]v1beta1.TrafficMapEntry, maxGatewayAPIBackendRefs+1)
	for i := range entries {
		entries[i] = gatewayTrafficMapEntry(fmt.Sprintf("cluster-%02d", i), fmt.Sprintf("%d.example", i), 1)
	}
	_, err := p.Plan(owner, gatewayTrafficMap(owner, entries...), nil)
	require.ErrorContains(t, err, "support at most 16 backendRefs")

	tests := []struct {
		name    string
		entries []v1beta1.TrafficMapEntry
		want    string
	}{
		{name: "empty cluster", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("", "a.example", 1)}, want: "cluster must not be empty"},
		{name: "duplicate cluster", entries: []v1beta1.TrafficMapEntry{
			gatewayTrafficMapEntry("cluster-a", "a.example", 1),
			gatewayTrafficMapEntry("cluster-a", "b.example", 1),
		}, want: "more than one entry"},
		{name: "missing endpoint", entries: []v1beta1.TrafficMapEntry{{Cluster: "cluster-a", Weight: 1}}, want: "has no valid endpoint"},
		{name: "negative weight", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("cluster-a", "a.example", -1)}, want: "must be between"},
		{name: "weight above Gateway limit", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("cluster-a", "a.example", maxGatewayAPIBackendWeight+1)}, want: "must be between"},
		{name: "invalid backend hostname", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("cluster-a", "UPPER.example", 1)}, want: "backend hostname"},
		{name: "IP backend hostname", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("cluster-a", "192.0.2.1", 1)}, want: "must not be an IP address"},
		{name: "invalid cluster name", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry("INVALID", "a.example", 1)}, want: "cluster \"INVALID\" is invalid"},
		{name: "cluster cannot fit label", entries: []v1beta1.TrafficMapEntry{gatewayTrafficMapEntry(strings.Repeat("a", 64), "a.example", 1)}, want: "label"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.Plan(owner, gatewayTrafficMap(owner, tt.entries...), nil)
			require.ErrorContains(t, err, tt.want)
		})
	}
	collisionSuffix := strings.Repeat("a", 54)
	plan, err := p.Plan(owner, gatewayTrafficMap(owner,
		gatewayTrafficMapEntry("c13303-"+collisionSuffix, "a.example", 1),
		gatewayTrafficMapEntry("c82303-"+collisionSuffix, "b.example", 1),
	), nil)
	require.NoError(t, err)
	assert.Len(t, plan.(*gatewayAPITrafficMapPlan).target.Homes, 2)

	_, err = p.Plan(owner, gatewayTrafficMap(owner), map[string]string{"unexpected": "value"})
	require.ErrorContains(t, err, "effective options")

	owner.Finalizers = []string{EndpointFinalizer}
	_, err = p.Plan(owner, gatewayTrafficMap(owner), nil)
	require.ErrorContains(t, err, "legacy endpoint finalizer")
	owner.Finalizers = nil

	owner.Annotations = map[string]string{GlobalHostAnnotation: "INVALID.example"}
	_, err = p.Plan(owner, gatewayTrafficMap(owner), nil)
	require.ErrorContains(t, err, "global hostname")
	owner.Annotations[GlobalHostAnnotation] = "*.example.com"
	_, err = p.Plan(owner, gatewayTrafficMap(owner), nil)
	require.NoError(t, err, "Gateway API permits a wildcard in the first hostname label")
}

func TestGatewayAPITrafficMapPlanValidatesPublishedResourceConfig(t *testing.T) {
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner)
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{
			name:   "port above protocol limit",
			mutate: func(cfg *Config) { cfg.BackendPort = 65_536 },
			want:   "backend port",
		},
		{
			name:   "gateway has extra separator",
			mutate: func(cfg *Config) { cfg.GlobalGateway = "system/edge/extra" },
			want:   "name or namespace/name",
		},
		{
			name:   "gateway has invalid namespace",
			mutate: func(cfg *Config) { cfg.GlobalGateway = "INVALID/edge" },
			want:   "gateway namespace",
		},
		{
			name:   "gateway has invalid name",
			mutate: func(cfg *Config) { cfg.GlobalGateway = "system/INVALID" },
			want:   "gateway name",
		},
		{
			name:   "label has invalid key",
			mutate: func(cfg *Config) { cfg.Labels = map[string]string{"bad key": "value"} },
			want:   "label key",
		},
		{
			name: "label has invalid value",
			mutate: func(cfg *Config) {
				cfg.Labels = map[string]string{"example.com/team": strings.Repeat("x", 64)}
			},
			want: "label",
		},
		{
			name: "TLS well-known CA is empty",
			mutate: func(cfg *Config) {
				cfg.GatewayBackend.TLS.Enabled = true
			},
			want: "well-known CA certificates",
		},
		{
			name: "TLS well-known CA is unsupported",
			mutate: func(cfg *Config) {
				cfg.GatewayBackend.TLS.Enabled = true
				cfg.GatewayBackend.TLS.WellKnownCACertificates = "Custom"
			},
			want: "well-known CA certificates",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
			p := NewGatewayAPITrafficMapPublisher(nil, nil, cfg)
			_, err := p.Plan(owner, trafficMap, nil)
			require.ErrorContains(t, err, tt.want)
		})
	}

	longNameOwner := testISVC()
	longNameOwner.Name = strings.Repeat("a", 64)
	p := NewGatewayAPITrafficMapPublisher(nil, nil, baseConfig())
	_, err := p.Plan(longNameOwner, gatewayTrafficMap(longNameOwner), nil)
	require.ErrorContains(t, err, "cannot be used as a label value")
}

func TestValidateGatewayAPIHTTPRouteClaims(t *testing.T) {
	valid := "v1:httproute:gateway-system/service-global"
	routeKeys, err := validateGatewayAPIHTTPRouteClaims([]string{
		"v1:httproute:z-system/z-route",
		valid,
	})
	require.NoError(t, err)
	assert.Equal(t, []types.NamespacedName{
		{Namespace: "gateway-system", Name: "service-global"},
		{Namespace: "z-system", Name: "z-route"},
	}, routeKeys)

	tooMany := make([]string, v1beta1.MaxTrafficMapPublisherTargets+1)
	for i := range tooMany {
		tooMany[i] = valid
	}
	tests := []struct {
		name   string
		claims []string
		want   string
	}{
		{name: "unknown version", claims: []string{"v2:httproute:gateway-system/service-global"}, want: "unsupported claim version or kind"},
		{name: "unknown kind", claims: []string{"v1:service:gateway-system/service-global"}, want: "unsupported claim version or kind"},
		{name: "missing separator", claims: []string{"v1:httproute:gateway-system"}, want: "exactly one namespace/name separator"},
		{name: "extra separator", claims: []string{"v1:httproute:gateway-system/a/b"}, want: "exactly one namespace/name separator"},
		{name: "invalid namespace", claims: []string{"v1:httproute:Gateway/service-global"}, want: "namespace"},
		{name: "invalid name", claims: []string{"v1:httproute:gateway-system/Service"}, want: "name"},
		{name: "noncanonical", claims: []string{"v1:httproute:gateway-system/service-global "}, want: "invalid"},
		{name: "duplicate", claims: []string{valid, valid}, want: "appears more than once"},
		{name: "too many", claims: tooMany, want: "maximum"},
		{name: "too long", claims: []string{gatewayAPIHTTPRouteClaimPrefix + "ns/" + strings.Repeat("a", v1beta1.MaxTrafficMapPublisherTargetLength)}, want: "bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateGatewayAPIHTTPRouteClaims(tt.claims)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestGatewayAPITrafficMapDrainValidatesAllClaimsBeforeMutation(t *testing.T) {
	scheme := pubScheme(t)
	route := &gatewayapiv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod",
			Name:      "svc-global",
			Labels: map[string]string{
				ManagedByLabel:                      ManagedByValue,
				PlacementEndpointISVCLabel:          "svc",
				PlacementEndpointISVCNamespaceLabel: "prod",
			},
		},
		Spec: gatewayapiv1.HTTPRouteSpec{Rules: []gatewayapiv1.HTTPRouteRule{{
			BackendRefs: []gatewayapiv1.HTTPBackendRef{{
				BackendRef: gatewayapiv1.BackendRef{Weight: ptr.To(int32(9))},
			}},
		}}},
	}
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()
	p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	validClaim := "v1:httproute:prod/svc-global"

	invalidSourceKey := types.NamespacedName{Namespace: "Prod", Name: "svc"}
	err := p.Drain(context.Background(), invalidSourceKey, []string{validClaim})
	require.ErrorContains(t, err, "drain source")
	got := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, got))
	assert.Equal(t, int32(9), *got.Spec.Rules[0].BackendRefs[0].Weight)

	sourceKey := types.NamespacedName{Namespace: "prod", Name: "svc"}
	err = p.Drain(context.Background(), sourceKey, []string{validClaim, "v2:httproute:prod/other"})
	require.Error(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, got))
	assert.Equal(t, int32(9), *got.Spec.Rules[0].BackendRefs[0].Weight)

	require.NoError(t, p.Drain(context.Background(), sourceKey, []string{validClaim}))
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, got))
	assert.Equal(t, int32(0), *got.Spec.Rules[0].BackendRefs[0].Weight)
}

func TestGatewayAPITrafficMapUnpublishUsesOnlyJournalAndSourceKey(t *testing.T) {
	scheme := pubScheme(t)
	c := newGatewayFakeClientBuilder(t, scheme).Build()
	p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 3))
	plan, err := p.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	_, err = p.Apply(context.Background(), plan)
	require.NoError(t, err)

	journal := v1beta1.TrafficMapPublisherStatus{
		PublisherName:  p.Name(),
		ClaimedTargets: plan.Claims(),
	}
	require.NoError(t, p.Unpublish(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, journal))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: p.publisher.serviceName(owner, "cluster-a")}, &corev1.Service{})))
}

func TestGatewayAPITrafficMapCleanupUsesUncachedReader(t *testing.T) {
	scheme := pubScheme(t)
	live := newGatewayFakeClientBuilder(t, scheme).Build()
	cfg := baseConfig()
	owner := testISVC()
	trafficMap := gatewayTrafficMap(owner, gatewayTrafficMapEntry("cluster-a", "a.example", 3))
	publisher := NewGatewayAPITrafficMapPublisher(live, live, cfg)
	plan, err := publisher.Plan(owner, trafficMap, nil)
	require.NoError(t, err)
	_, err = publisher.Apply(context.Background(), plan)
	require.NoError(t, err)

	publisher = NewGatewayAPITrafficMapPublisher(staleGatewayAPIReadClient{Client: live}, live, cfg)
	sourceKey := types.NamespacedName{Namespace: owner.Namespace, Name: owner.Name}
	require.NoError(t, publisher.Drain(context.Background(), sourceKey, plan.Claims()))
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, live.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, route))
	assert.Equal(t, int32(0), *route.Spec.Rules[0].BackendRefs[0].Weight)

	require.NoError(t, publisher.Unpublish(context.Background(), sourceKey, v1beta1.TrafficMapPublisherStatus{
		PublisherName:  publisher.Name(),
		ClaimedTargets: plan.Claims(),
	}))
	assert.True(t, apierrors.IsNotFound(live.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(live.Get(context.Background(),
		types.NamespacedName{Namespace: "prod", Name: publisher.publisher.serviceName(owner, "cluster-a")}, &corev1.Service{})))
}

func TestGatewayAPITrafficMapUnpublishValidatesWholeJournalBeforeMutation(t *testing.T) {
	scheme := pubScheme(t)
	route := &gatewayapiv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod",
		Name:      "svc-global",
		Labels: map[string]string{
			ManagedByLabel:                      ManagedByValue,
			PlacementEndpointISVCLabel:          "svc",
			PlacementEndpointISVCNamespaceLabel: "prod",
		},
	}}
	c := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()
	p := NewGatewayAPITrafficMapPublisher(c, c, baseConfig())
	sourceKey := types.NamespacedName{Namespace: "prod", Name: "svc"}

	err := p.Unpublish(context.Background(), types.NamespacedName{Namespace: "Prod", Name: "svc"}, v1beta1.TrafficMapPublisherStatus{
		PublisherName:  p.Name(),
		ClaimedTargets: []string{"v1:httproute:prod/svc-global"},
	})
	require.ErrorContains(t, err, "cleanup source")
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{}))

	err = p.Unpublish(context.Background(), sourceKey, v1beta1.TrafficMapPublisherStatus{
		PublisherName: p.Name(),
		ClaimedTargets: []string{
			"v1:httproute:prod/svc-global",
			"v2:httproute:prod/invalid",
		},
	})
	require.Error(t, err)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{}))

	err = p.Unpublish(context.Background(), sourceKey, v1beta1.TrafficMapPublisherStatus{
		PublisherName:  "other",
		ClaimedTargets: []string{"v1:httproute:prod/svc-global"},
	})
	require.ErrorContains(t, err, "owned by")
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc-global"}, &gatewayapiv1.HTTPRoute{}))
}
