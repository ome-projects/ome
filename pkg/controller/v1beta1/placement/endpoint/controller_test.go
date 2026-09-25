package endpoint

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	placementcontroller "sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
)

// placedISVC builds a control-plane ISVC whose placement reports a winner with
// an addressable endpoint (the publishable state).
func placedISVC(cluster, backendHost string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
		Status: v1beta1.InferenceServiceStatus{
			Placement: &v1beta1.PlacementStatus{
				Phase:    v1beta1.PlacementPhasePlaced,
				Cluster:  cluster,
				Endpoint: apis.HTTPS(backendHost),
			},
		},
	}
}

func newReconciler(t *testing.T, cfg Config, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	s := pubScheme(t)
	c := newGatewayFakeClientBuilder(t, s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(objs...).Build()
	return &Reconciler{
		Client:    c,
		APIReader: c,
		Log:       log.Log,
		Publisher: NewGatewayAPIPublisher(c, cfg),
		Config:    cfg,
	}, c
}

type recordingEndpointPublisher struct {
	published   []Target
	unpublished int
}

func (p *recordingEndpointPublisher) Publish(_ context.Context, _ *v1beta1.InferenceService, target Target) error {
	p.published = append(p.published, cloneGatewayAPITarget(target))
	return nil
}

func (p *recordingEndpointPublisher) Unpublish(context.Context, *v1beta1.InferenceService) error {
	p.unpublished++
	return nil
}

func (*recordingEndpointPublisher) Name() string { return "recording" }

// trafficMapBlindClient simulates an informer cache that has not observed a
// TrafficMap which is already visible through the direct API reader.
type trafficMapBlindClient struct {
	client.Client
}

func (c trafficMapBlindClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if _, ok := object.(*v1beta1.TrafficMap); ok {
		return apierrors.NewNotFound(schema.GroupResource{
			Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps",
		}, key.Name)
	}
	return c.Client.Get(ctx, key, object, opts...)
}

// trafficMapAppearingReader simulates a TrafficMap publisher acquiring its
// durable handoff state between the legacy reconciler's initial check and its
// external publish or unpublish call.
type trafficMapAppearingReader struct {
	client.Reader
	trafficMapReads int
}

func (r *trafficMapAppearingReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	opts ...client.GetOption,
) error {
	if _, ok := object.(*v1beta1.TrafficMap); ok {
		r.trafficMapReads++
		if r.trafficMapReads == 1 {
			return apierrors.NewNotFound(schema.GroupResource{
				Group: v1beta1.SchemeGroupVersion.Group, Resource: "trafficmaps",
			}, key.Name)
		}
	}
	return r.Reader.Get(ctx, key, object, opts...)
}

func reconcile(t *testing.T, r *Reconciler) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "svc", Namespace: "prod"}})
	require.NoError(t, err)
}

func updateEvent(old, nw *v1beta1.InferenceService) event.UpdateEvent {
	return event.UpdateEvent{ObjectOld: old, ObjectNew: nw}
}

func trafficMapUpdateEvent(old, nw *v1beta1.TrafficMap) event.UpdateEvent {
	return event.UpdateEvent{ObjectOld: old, ObjectNew: nw}
}

func TestReconcile_PlacedPublishesAndFinalizes(t *testing.T) {
	r, c := newReconciler(t, baseConfig(), placedISVC("cluster-a", "svc.prod.cloud-a.example"))

	reconcile(t, r)

	// HTTPRoute + ExternalName Service created.
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	assert.Equal(t, gatewayapiv1.Hostname("svc.prod.global.example"), route.Spec.Hostnames[0])
	svc := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, svc))
	assert.Equal(t, "svc.prod.cloud-a.example", svc.Spec.ExternalName)

	// Finalizer added so teardown can run before the ISVC is removed.
	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, got))
	assert.True(t, controllerutil.ContainsFinalizer(got, EndpointFinalizer))
}

func TestReconcile_GatewayBackendSchedulesAddressRefresh(t *testing.T) {
	cfg := gatewayBackendConfig()
	cfg.GatewayBackend.EndpointSlices.AddressRefreshInterval = 45 * time.Second
	s := pubScheme(t)
	c := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(placedISVC("cluster-a", "svc.prod.cloud-a.example")).Build()
	publisher := NewGatewayAPIPublisher(c, cfg, WithBackendAddressResolver(staticBackendAddressResolver{
		"cluster-a": {{Address: "192.0.2.10", Type: "IPv4"}},
	}))
	r := &Reconciler{Client: c, APIReader: c, Log: log.Log, Publisher: publisher, Config: cfg}

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "svc", Namespace: "prod"},
	})
	require.NoError(t, err)
	assert.Equal(t, 45*time.Second, result.RequeueAfter)
}

func TestReconcile_RepointsOnReplacement(t *testing.T) {
	r, c := newReconciler(t, baseConfig(), placedISVC("cluster-a", "svc.prod.cloud-a.example"))
	reconcile(t, r)

	// Simulate the placement controller re-homing onto a new cluster.
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, cur))
	cur.Status.Placement.Cluster = "cluster-b"
	cur.Status.Placement.Endpoint = apis.HTTPS("svc.prod.cloud-b.example")
	require.NoError(t, c.Status().Update(context.Background(), cur))

	reconcile(t, r)

	svc := &corev1.Service{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-b", Namespace: "prod"}, svc))
	assert.Equal(t, "svc.prod.cloud-b.example", svc.Spec.ExternalName, "ExternalName repointed to new winner")
	assert.Equal(t, "cluster-b", svc.Labels[PlacementClusterLabel])
	// The old winner's per-home Service is garbage-collected.
	errOld := c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &corev1.Service{})
	assert.True(t, apierrors.IsNotFound(errOld), "old winner's Service GC'd on re-home")
}

func TestReconcile_UnplacedTearsDownAndDropsFinalizer(t *testing.T) {
	r, c := newReconciler(t, baseConfig(), placedISVC("cluster-a", "svc.prod.cloud-a.example"))
	reconcile(t, r) // publish + finalizer

	// Placement regresses to Admitting (winner lost). Publisher must tear down.
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, cur))
	cur.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
	cur.Status.Placement.Cluster = ""
	cur.Status.Placement.Endpoint = nil
	require.NoError(t, c.Status().Update(context.Background(), cur))

	reconcile(t, r)

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})
	assert.True(t, apierrors.IsNotFound(err), "stale route removed when winner lost")
	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, got))
	assert.False(t, controllerutil.ContainsFinalizer(got, EndpointFinalizer), "finalizer dropped after teardown")
}

func TestReconcile_UnpublishRetainsFinalizerUntilRouteIsGone(t *testing.T) {
	r, c := newReconciler(t, baseConfig(), placedISVC("cluster-a", "svc.prod.cloud-a.example"))
	reconcile(t, r)

	routeKey := types.NamespacedName{Name: "svc-global", Namespace: "prod"}
	serviceKey := types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}
	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	route.Finalizers = []string{"example.com/hold"}
	require.NoError(t, c.Update(context.Background(), route))

	isvc := &v1beta1.InferenceService{}
	isvcKey := types.NamespacedName{Name: "svc", Namespace: "prod"}
	require.NoError(t, c.Get(context.Background(), isvcKey, isvc))
	require.NoError(t, c.Delete(context.Background(), isvc))

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: isvcKey})
	require.ErrorContains(t, err, "deletion is pending")
	require.NoError(t, c.Get(context.Background(), isvcKey, isvc))
	require.NotNil(t, isvc.DeletionTimestamp)
	assert.Contains(t, isvc.Finalizers, EndpointFinalizer)
	require.NoError(t, c.Get(context.Background(), routeKey, route))
	require.NotNil(t, route.DeletionTimestamp)
	require.NotNil(t, route.Spec.Rules[0].BackendRefs[0].Weight)
	assert.Zero(t, *route.Spec.Rules[0].BackendRefs[0].Weight)
	require.NoError(t, c.Get(context.Background(), serviceKey, &corev1.Service{}))

	route.Finalizers = nil
	require.NoError(t, c.Update(context.Background(), route))
	_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: isvcKey})
	require.NoError(t, err)
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), routeKey, &gatewayapiv1.HTTPRoute{})))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), serviceKey, &corev1.Service{})))
	assert.True(t, apierrors.IsNotFound(c.Get(context.Background(), isvcKey, &v1beta1.InferenceService{})))
}

func TestReconcile_LegacyPublisherIgnoresTrafficMapWeights(t *testing.T) {
	// A Split ISVC with two admitted homes whose reactive ready-replica ratio is
	// 5:2, plus a TrafficMap the routing controller published carrying a
	// capacity-aware 1:3 split. Once the publisher handoff is released, the
	// legacy route must use the reactive placement weights.
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
		Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
			Phase: v1beta1.PlacementPhasePlaced,
			Candidates: []v1beta1.CandidatePlacement{
				{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("a.example"), ReadyReplicas: 5},
				{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("b.example"), ReadyReplicas: 2},
			},
		}},
	}
	tm := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
		Spec: v1beta1.TrafficMapSpec{
			Service: "svc",
			Entries: []v1beta1.TrafficMapEntry{
				{Cluster: "a", Weight: 1, Healthy: true},
				{Cluster: "b", Weight: 3, Healthy: true},
			},
		},
	}
	r, c := newReconciler(t, baseConfig(), isvc, tm)

	reconcile(t, r)

	route := &gatewayapiv1.HTTPRoute{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, route))
	refs := route.Spec.Rules[0].BackendRefs
	require.Len(t, refs, 2)
	require.NotNil(t, refs[0].Weight)
	require.NotNil(t, refs[1].Weight)
	// backendRefs are cluster-sorted: a then b.
	assert.Equal(t, int32(5), *refs[0].Weight)
	assert.Equal(t, int32(2), *refs[1].Weight)
}

func TestReconcile_AuthoritativeTrafficMapStateBlocksLegacyEffects(t *testing.T) {
	tests := []struct {
		name       string
		trafficMap *v1beta1.TrafficMap
	}{
		{
			name: "publisher finalizer",
			trafficMap: &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
				Name: "svc", Namespace: "prod", Finalizers: []string{TrafficMapPublisherFinalizer},
			}},
		},
		{
			name: "durable publisher status",
			trafficMap: &v1beta1.TrafficMap{
				ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
				Status: v1beta1.TrafficMapStatus{Publisher: &v1beta1.TrafficMapPublisherStatus{
					PublisherName: "gatewayapi", ClaimedTargets: []string{"v1:httproute:prod/svc-global"},
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := placedISVC("cluster-a", "svc.prod.cloud-a.example")
			r, apiClient := newReconciler(t, baseConfig(), isvc, tt.trafficMap)
			publisher := &recordingEndpointPublisher{}
			r.Client = trafficMapBlindClient{Client: apiClient}
			r.APIReader = apiClient
			r.Publisher = publisher

			reconcile(t, r)

			assert.Empty(t, publisher.published)
			assert.Zero(t, publisher.unpublished)
			got := &v1beta1.InferenceService{}
			require.NoError(t, apiClient.Get(context.Background(), client.ObjectKeyFromObject(isvc), got))
			assert.NotContains(t, got.Finalizers, EndpointFinalizer)
		})
	}
}

func TestReconcile_OwnerlessTrafficMapHandoffReleaseEnqueuesFinalizerCleanup(t *testing.T) {
	isvc := placedISVC("cluster-a", "svc.prod.cloud-a.example")
	isvc.Finalizers = []string{EndpointFinalizer, placementcontroller.PlacementFinalizer}
	trafficMap := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:       isvc.Name,
			Namespace:  isvc.Namespace,
			Finalizers: []string{TrafficMapPublisherFinalizer, "example.com/hold"},
		},
		Status: v1beta1.TrafficMapStatus{
			SourceUID: isvc.UID,
			Publisher: &v1beta1.TrafficMapPublisherStatus{
				PublisherName:  "gatewayapi",
				ClaimedTargets: []string{"v1:httproute:prod/svc-global"},
			},
		},
	}
	r, c := newReconciler(t, baseConfig(), isvc, trafficMap)
	publisher := &recordingEndpointPublisher{}
	r.Publisher = publisher
	require.NoError(t, c.Delete(context.Background(), isvc))
	require.NoError(t, c.Delete(context.Background(), trafficMap))

	reconcile(t, r)
	assert.Zero(t, publisher.unpublished)
	blockedSource := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), blockedSource))
	assert.Contains(t, blockedSource.Finalizers, EndpointFinalizer)

	blockedMap := &v1beta1.TrafficMap{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(trafficMap), blockedMap))
	releasedMap := blockedMap.DeepCopy()
	releasedMap.Finalizers = []string{"example.com/hold"}
	releasedMap.Status.Publisher = nil
	require.NoError(t, c.Update(context.Background(), releasedMap))
	require.True(t, trafficMapPublishChange.Update(trafficMapUpdateEvent(blockedMap, releasedMap)))
	requests := enqueueTrafficMapISVC(context.Background(), releasedMap)
	require.Len(t, requests, 1, "ownerless handoff release must enqueue the same-key ISVC")
	_, err := r.Reconcile(context.Background(), requests[0])
	require.NoError(t, err)

	assert.Equal(t, 1, publisher.unpublished)
	releasedSource := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(isvc), releasedSource))
	assert.NotContains(t, releasedSource.Finalizers, EndpointFinalizer)
	assert.Contains(t, releasedSource.Finalizers, placementcontroller.PlacementFinalizer)
}

func TestReconcile_RechecksTrafficMapImmediatelyBeforeEffects(t *testing.T) {
	trafficMap := &v1beta1.TrafficMap{ObjectMeta: metav1.ObjectMeta{
		Name: "svc", Namespace: "prod", Finalizers: []string{TrafficMapPublisherFinalizer},
	}}

	t.Run("publish", func(t *testing.T) {
		isvc := placedISVC("cluster-a", "svc.prod.cloud-a.example")
		r, apiClient := newReconciler(t, baseConfig(), isvc, trafficMap.DeepCopy())
		publisher := &recordingEndpointPublisher{}
		reader := &trafficMapAppearingReader{Reader: apiClient}
		r.APIReader = reader
		r.Publisher = publisher

		reconcile(t, r)

		assert.Empty(t, publisher.published)
		assert.Equal(t, 2, reader.trafficMapReads)
	})

	t.Run("unpublish", func(t *testing.T) {
		isvc := placedISVC("cluster-a", "svc.prod.cloud-a.example")
		isvc.Finalizers = []string{EndpointFinalizer}
		cfg := baseConfig()
		cfg.GlobalGateway = ""
		r, apiClient := newReconciler(t, cfg, isvc, trafficMap.DeepCopy())
		publisher := &recordingEndpointPublisher{}
		reader := &trafficMapAppearingReader{Reader: apiClient}
		r.APIReader = reader
		r.Publisher = publisher

		reconcile(t, r)

		assert.Zero(t, publisher.unpublished)
		assert.Equal(t, 2, reader.trafficMapReads)
		got := &v1beta1.InferenceService{}
		require.NoError(t, apiClient.Get(context.Background(), client.ObjectKeyFromObject(isvc), got))
		assert.Contains(t, got.Finalizers, EndpointFinalizer)
	})
}

func TestReconcile_ReleasedTerminatingTrafficMapUsesReactiveWeights(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
		Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
			Phase: v1beta1.PlacementPhasePlaced,
			Candidates: []v1beta1.CandidatePlacement{
				{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("a.example"), ReadyReplicas: 5},
				{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("b.example"), ReadyReplicas: 2},
			},
		}},
	}
	now := metav1.Now()
	trafficMap := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "svc", Namespace: "prod", DeletionTimestamp: &now,
			Finalizers: []string{"example.com/unrelated-cleanup"},
		},
		Spec: v1beta1.TrafficMapSpec{Entries: []v1beta1.TrafficMapEntry{
			{Cluster: "a", Weight: 1}, {Cluster: "b", Weight: 3},
		}},
	}
	r, _ := newReconciler(t, baseConfig(), isvc, trafficMap)
	publisher := &recordingEndpointPublisher{}
	r.Publisher = publisher

	reconcile(t, r)

	require.Len(t, publisher.published, 1)
	require.Len(t, publisher.published[0].Homes, 2)
	assert.Equal(t, int32(5), publisher.published[0].Homes[0].Weight)
	assert.Equal(t, int32(2), publisher.published[0].Homes[1].Weight)
	assert.False(t, publisher.published[0].WeightsAuthoritative)
}

func TestReconcile_PendingNeverPublishes(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
		Status:     v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{Phase: v1beta1.PlacementPhasePending}},
	}
	r, c := newReconciler(t, baseConfig(), isvc)

	reconcile(t, r)

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})
	assert.True(t, apierrors.IsNotFound(err))
	got := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, got))
	assert.False(t, controllerutil.ContainsFinalizer(got, EndpointFinalizer),
		"an ISVC that never publishes is not held by a finalizer")
}

func TestReconcile_PlacedButNoEndpointYet(t *testing.T) {
	// Winner declared, but the worker has not reported a URL yet -> nothing
	// concrete to point at, so we do not publish (and do not strand a finalizer).
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", UID: "uid-1"},
		Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
			Phase: v1beta1.PlacementPhasePlaced, Cluster: "cluster-a", Endpoint: nil,
		}},
	}
	r, c := newReconciler(t, baseConfig(), isvc)

	reconcile(t, r)

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestReconcile_DeletionTearsDown(t *testing.T) {
	r, c := newReconciler(t, baseConfig(), placedISVC("cluster-a", "svc.prod.cloud-a.example"))
	reconcile(t, r) // publish + finalizer

	// Delete the ISVC; with the finalizer present it sticks around for teardown.
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, cur))
	require.NoError(t, c.Delete(context.Background(), cur))

	reconcile(t, r)

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &corev1.Service{})
	assert.True(t, apierrors.IsNotFound(err), "backend Service deleted on ISVC deletion")
	// Finalizer removed -> the fake client now actually deletes the ISVC.
	err = c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, &v1beta1.InferenceService{})
	assert.True(t, apierrors.IsNotFound(err), "ISVC removed after finalizer dropped")
}

func TestReconcile_BackendDisabledIsNoOp(t *testing.T) {
	cfg := baseConfig()
	cfg.GlobalGateway = "" // backend not configured
	r, c := newReconciler(t, cfg, placedISVC("cluster-a", "svc.prod.cloud-a.example"))

	reconcile(t, r)

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})
	assert.True(t, apierrors.IsNotFound(err), "no backend programmed when gateway unconfigured")
}

func TestReconcile_BackendDisabledCleansPublishedResources(t *testing.T) {
	cfg := baseConfig()
	r, c := newReconciler(t, cfg, placedISVC("cluster-a", "svc.prod.cloud-a.example"))
	reconcile(t, r)

	r.Config.GlobalGateway = ""
	reconcile(t, r)

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})
	assert.True(t, apierrors.IsNotFound(err), "disabled backend removes the published route")
	err = c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &corev1.Service{})
	assert.True(t, apierrors.IsNotFound(err), "disabled backend removes published Services")
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, cur))
	assert.NotContains(t, cur.Finalizers, EndpointFinalizer)
}

func TestReconcile_BackendDisabledReleasesDeletedISVC(t *testing.T) {
	cfg := baseConfig()
	r, c := newReconciler(t, cfg, placedISVC("cluster-a", "svc.prod.cloud-a.example"))
	reconcile(t, r) // publish + finalizer

	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, cur))
	require.NoError(t, c.Delete(context.Background(), cur))
	r.Config.GlobalGateway = ""

	reconcile(t, r)

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, &v1beta1.InferenceService{})
	assert.True(t, apierrors.IsNotFound(err), "an ISVC deleted while the backend is disabled is still released")
	err = c.Get(context.Background(), types.NamespacedName{Name: "svc-global-cluster-a", Namespace: "prod"}, &corev1.Service{})
	assert.True(t, apierrors.IsNotFound(err), "its published Service is torn down first")
}

func TestReconcile_NoGlobalHostNeverPublishes(t *testing.T) {
	cfg := baseConfig()
	cfg.GlobalHostTemplate = "" // no template, ISVC has no annotation -> no host
	r, c := newReconciler(t, cfg, placedISVC("cluster-a", "svc.prod.cloud-a.example"))

	reconcile(t, r)

	err := c.Get(context.Background(), types.NamespacedName{Name: "svc-global", Namespace: "prod"}, &gatewayapiv1.HTTPRoute{})
	assert.True(t, apierrors.IsNotFound(err), "no host resolvable -> nothing published (no magic default)")
}

func TestResolveTarget(t *testing.T) {
	r := &Reconciler{Config: baseConfig()}

	t.Run("placed + endpoint -> ok", func(t *testing.T) {
		tgt, ok, err := r.resolveTarget(placedISVC("cloud-a", "h.example"), nil)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "svc.prod.global.example", tgt.GlobalHost)
		require.Len(t, tgt.Homes, 1)
		assert.Equal(t, "cloud-a", tgt.Homes[0].Cluster)
		assert.Equal(t, "h.example", tgt.Homes[0].BackendHost)
	})

	t.Run("nil placement -> not ok", func(t *testing.T) {
		_, ok, err := r.resolveTarget(&v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"}}, nil)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("All: admitted candidates -> one home each, sorted", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
			Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
				Phase: v1beta1.PlacementPhasePlaced, // no top-level winner in All
				Candidates: []v1beta1.CandidatePlacement{
					{Cluster: "workload-2", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("b.example")},
					{Cluster: "workload-1", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("a.example")},
					{Cluster: "workload-3", Phase: v1beta1.CandidatePhaseAdmitting}, // gated: not a home
				},
			}},
		}
		tgt, ok, err := r.resolveTarget(isvc, nil)
		require.NoError(t, err)
		require.True(t, ok)
		require.Len(t, tgt.Homes, 2, "only admitted+addressable candidates are homes")
		assert.Equal(t, "workload-1", tgt.Homes[0].Cluster, "sorted by cluster")
		assert.Equal(t, "a.example", tgt.Homes[0].BackendHost)
		assert.Equal(t, "workload-2", tgt.Homes[1].Cluster)
		assert.Equal(t, "b.example", tgt.Homes[1].BackendHost)
	})

	t.Run("placed but no addressable home -> not ok", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
			Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
				Phase:      v1beta1.PlacementPhasePlaced,
				Candidates: []v1beta1.CandidatePlacement{{Cluster: "workload-1", Phase: v1beta1.CandidatePhaseAdmitting}},
			}},
		}
		_, ok, err := r.resolveTarget(isvc, nil)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("endpoint host with a port -> port stripped for the backend", func(t *testing.T) {
		// A URL Host is "host:port" when the endpoint carries a port; the backend
		// ExternalName Service needs a BARE host (port comes from Config.BackendPort).
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
			Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
				Phase: v1beta1.PlacementPhasePlaced,
				Candidates: []v1beta1.CandidatePlacement{
					{Cluster: "cloud-a", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("svc.inf-prod.svc.cluster.local:8000")},
				},
			}},
		}
		tgt, ok, err := r.resolveTarget(isvc, nil)
		require.NoError(t, err)
		require.True(t, ok)
		require.Len(t, tgt.Homes, 1)
		assert.Equal(t, "svc.inf-prod.svc.cluster.local", tgt.Homes[0].BackendHost,
			"port stripped so spec.externalName stays RFC-1123 valid")
	})

	t.Run("Split: per-home ready replicas become home weights", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
			Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
				Phase: v1beta1.PlacementPhasePlaced,
				Candidates: []v1beta1.CandidatePlacement{
					{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("a.example"), ReadyReplicas: 5},
					{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("b.example"), ReadyReplicas: 2},
				},
			}},
		}
		tgt, ok, err := r.resolveTarget(isvc, nil)
		require.NoError(t, err)
		require.True(t, ok)
		require.Len(t, tgt.Homes, 2)
		assert.Equal(t, int32(5), tgt.Homes[0].Weight, "home a weight = its ready replicas")
		assert.Equal(t, int32(2), tgt.Homes[1].Weight, "home b weight = its ready replicas")
	})

	t.Run("TrafficMap weights override the reactive ready-replica split", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
			Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
				Phase: v1beta1.PlacementPhasePlaced,
				Candidates: []v1beta1.CandidatePlacement{
					{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("a.example"), ReadyReplicas: 5},
					{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("b.example"), ReadyReplicas: 2},
				},
			}},
		}
		// The routing controller published a capacity-aware split (e.g. b has a
		// faster accelerator) that differs from the raw ready-replica ratio.
		tgt, ok, err := r.resolveTarget(isvc, map[string]int32{"a": 1, "b": 3})
		require.NoError(t, err)
		require.True(t, ok)
		require.Len(t, tgt.Homes, 2)
		assert.Equal(t, int32(1), tgt.Homes[0].Weight, "home a takes the TrafficMap weight, not its 5 ready replicas")
		assert.Equal(t, int32(3), tgt.Homes[1].Weight, "home b takes the TrafficMap weight, not its 2 ready replicas")
		assert.True(t, tgt.WeightsAuthoritative)
	})

	t.Run("home absent from the TrafficMap keeps its reactive weight", func(t *testing.T) {
		isvc := &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
			Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{
				Phase: v1beta1.PlacementPhasePlaced,
				Candidates: []v1beta1.CandidatePlacement{
					{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("a.example"), ReadyReplicas: 5},
					{Cluster: "b", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("b.example"), ReadyReplicas: 2},
				},
			}},
		}
		// TrafficMap lags placement: it only knows about home a. Home b falls back
		// to its live ready-replica count rather than dropping to zero.
		tgt, ok, err := r.resolveTarget(isvc, map[string]int32{"a": 9})
		require.NoError(t, err)
		require.True(t, ok)
		require.Len(t, tgt.Homes, 2)
		assert.Equal(t, int32(9), tgt.Homes[0].Weight, "home a takes its TrafficMap weight")
		assert.Equal(t, int32(2), tgt.Homes[1].Weight, "home b, absent from the map, keeps its reactive weight")
		assert.False(t, tgt.WeightsAuthoritative, "a partial map cannot authorize an all-zero route")
	})
}

func TestPlacementPublishChange(t *testing.T) {
	base := placedISVC("cloud-a", "h.example")

	t.Run("phase change passes", func(t *testing.T) {
		old := base.DeepCopy()
		nw := base.DeepCopy()
		nw.Status.Placement.Phase = v1beta1.PlacementPhaseAdmitting
		assert.True(t, placementPublishChange.Update(updateEvent(old, nw)))
	})

	t.Run("endpoint host change passes", func(t *testing.T) {
		old := base.DeepCopy()
		nw := base.DeepCopy()
		nw.Status.Placement.Endpoint = apis.HTTPS("other.example")
		assert.True(t, placementPublishChange.Update(updateEvent(old, nw)))
	})

	t.Run("unrelated spec churn is dropped", func(t *testing.T) {
		old := base.DeepCopy()
		nw := base.DeepCopy()
		nw.Labels = map[string]string{"unrelated": "x"}
		assert.False(t, placementPublishChange.Update(updateEvent(old, nw)))
	})

	t.Run("global-host annotation change passes", func(t *testing.T) {
		old := base.DeepCopy()
		nw := base.DeepCopy()
		nw.Annotations = map[string]string{GlobalHostAnnotation: "pinned.example"}
		assert.True(t, placementPublishChange.Update(updateEvent(old, nw)))
	})

	t.Run("placement eligibility removal passes", func(t *testing.T) {
		old := base.DeepCopy()
		old.Annotations = map[string]string{
			placementcontroller.AcceleratorRequirementsAnnotation: "gpu=tpu",
		}
		nw := old.DeepCopy()
		delete(nw.Annotations, placementcontroller.AcceleratorRequirementsAnnotation)
		assert.True(t, placementPublishChange.Update(updateEvent(old, nw)))
	})

	t.Run("routing opt-out transition passes", func(t *testing.T) {
		old := base.DeepCopy()
		old.Spec.Placement = &v1beta1.PlacementSpec{Requirements: "gpu=tpu"}
		nw := old.DeepCopy()
		disabled := false
		nw.Spec.Routing = &v1beta1.RoutingSpec{Enabled: &disabled}
		assert.True(t, placementPublishChange.Update(updateEvent(old, nw)))
	})

	t.Run("deletion entering passes", func(t *testing.T) {
		old := base.DeepCopy()
		nw := base.DeepCopy()
		now := metav1.Now()
		nw.DeletionTimestamp = &now
		assert.True(t, placementPublishChange.Update(updateEvent(old, nw)))
	})

	t.Run("candidate-only change passes", func(t *testing.T) {
		old := base.DeepCopy()
		nw := base.DeepCopy()
		old.Status.Placement.Cluster = ""
		old.Status.Placement.Endpoint = nil
		old.Status.Placement.Candidates = []v1beta1.CandidatePlacement{{
			Cluster: "cloud-a", Phase: v1beta1.CandidatePhaseAdmitted, Endpoint: apis.HTTPS("a.example"), ReadyReplicas: 1,
		}}
		nw.Status.Placement = old.Status.Placement.DeepCopy()
		nw.Status.Placement.Candidates[0].ReadyReplicas = 2
		assert.True(t, placementPublishChange.Update(updateEvent(old, nw)))
	})
}

func TestTrafficMapPublishChange(t *testing.T) {
	base := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", Generation: 1},
		Spec: v1beta1.TrafficMapSpec{
			Service: "svc",
			Mode:    v1beta1.PlacementModeSplit,
			Entries: []v1beta1.TrafficMapEntry{
				{
					Cluster:  "cluster-a",
					Endpoint: apis.HTTPS("a.example"),
					Weight:   3,
					Healthy:  true,
				},
				{
					Cluster:  "cluster-b",
					Endpoint: apis.HTTPS("b.example:8443"),
					Weight:   7,
					Healthy:  true,
				},
			},
			ObservedISVCGeneration: 4,
		},
	}

	t.Run("create and delete pass", func(t *testing.T) {
		assert.True(t, trafficMapPublishChange.Create(event.CreateEvent{Object: base}))
		assert.True(t, trafficMapPublishChange.Delete(event.DeleteEvent{Object: base}))
	})

	nonPublicationChanges := []struct {
		name   string
		mutate func(*v1beta1.TrafficMap)
	}{
		{
			name: "entry order",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Entries[0], tm.Spec.Entries[1] = tm.Spec.Entries[1], tm.Spec.Entries[0]
			},
		},
		{
			name: "metadata",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Generation++
				tm.Labels = map[string]string{"unrelated": "metadata"}
			},
		},
		{
			name: "routing intent provenance",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Mode = v1beta1.PlacementModeAll
				tm.Spec.ObservedISVCGeneration++
			},
		},
		{
			name: "health provenance",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Entries[0].Healthy = false
			},
		},
		{
			name: "capacity provenance",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Entries[0].Capacity = &v1beta1.TrafficMapCapacity{Allocated: 11}
			},
		},
		{
			name: "probe provenance",
			mutate: func(tm *v1beta1.TrafficMap) {
				probeTime := metav1.Now()
				tm.Spec.Entries[0].Probe = &v1beta1.TrafficMapProbe{
					Result:              v1beta1.ProbeResultFailing,
					Gated:               true,
					LastProbeTime:       &probeTime,
					ConsecutiveFailures: 2,
					Message:             "probe failed",
				}
			},
		},
	}
	for _, tt := range nonPublicationChanges {
		t.Run(tt.name+" is dropped", func(t *testing.T) {
			old := base.DeepCopy()
			nw := base.DeepCopy()
			tt.mutate(nw)
			assert.False(t, trafficMapPublishChange.Update(trafficMapUpdateEvent(old, nw)))
		})
	}

	t.Run("handoff entering passes", func(t *testing.T) {
		old := base.DeepCopy()
		nw := base.DeepCopy()
		nw.Status.Published = true
		assert.True(t, trafficMapPublishChange.Update(trafficMapUpdateEvent(old, nw)))
	})

	t.Run("handoff release passes while an unrelated finalizer keeps deletion pending", func(t *testing.T) {
		old := base.DeepCopy()
		now := metav1.Now()
		old.DeletionTimestamp = &now
		old.Finalizers = []string{TrafficMapPublisherFinalizer, "example.com/unrelated-cleanup"}
		old.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
			PublisherName: "gatewayapi", ClaimedTargets: []string{"v1:httproute:prod/svc-global"},
		}
		nw := old.DeepCopy()
		nw.Finalizers = []string{"example.com/unrelated-cleanup"}
		nw.Status.Publisher = nil

		assert.True(t, trafficMapPublishChange.Update(trafficMapUpdateEvent(old, nw)))
	})

	t.Run("publisher status churn while handoff remains pending is dropped", func(t *testing.T) {
		old := base.DeepCopy()
		old.Finalizers = []string{TrafficMapPublisherFinalizer}
		old.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{
			PublisherName: "gatewayapi", ClaimedTargets: []string{"v1:httproute:prod/svc-global"},
		}
		nw := old.DeepCopy()
		nw.Status.Published = true
		nw.Status.ObservedTrafficMapGeneration = 2

		assert.False(t, trafficMapPublishChange.Update(trafficMapUpdateEvent(old, nw)))
	})

	t.Run("deletion transition passes", func(t *testing.T) {
		old := base.DeepCopy()
		nw := base.DeepCopy()
		now := metav1.Now()
		nw.DeletionTimestamp = &now

		assert.True(t, trafficMapPublishChange.Update(trafficMapUpdateEvent(old, nw)))
	})

	tests := []struct {
		name   string
		mutate func(*v1beta1.TrafficMap)
	}{
		{
			name: "service change passes",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Service = "other-service"
			},
		},
		{
			name: "cluster change passes",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Entries[0].Cluster = "cluster-c"
			},
		},
		{
			name: "endpoint change passes",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Entries[0].Endpoint = apis.HTTPS("new.example")
			},
		},
		{
			name: "weight change passes",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Entries[0].Weight++
			},
		},
		{
			name: "entry addition passes",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Entries = append(tm.Spec.Entries, v1beta1.TrafficMapEntry{
					Cluster: "cluster-c", Endpoint: apis.HTTPS("c.example"), Weight: 5,
				})
			},
		},
		{
			name: "entry removal passes",
			mutate: func(tm *v1beta1.TrafficMap) {
				tm.Spec.Entries = tm.Spec.Entries[:1]
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := base.DeepCopy()
			nw := base.DeepCopy()
			tt.mutate(nw)
			assert.True(t, trafficMapPublishChange.Update(trafficMapUpdateEvent(old, nw)))
		})
	}

	t.Run("unexpected update type passes", func(t *testing.T) {
		assert.True(t, trafficMapPublishChange.Update(event.UpdateEvent{
			ObjectOld: &corev1.Service{}, ObjectNew: &corev1.Service{},
		}))
	})
}

func TestSetupWithManagerRequiresAPIReader(t *testing.T) {
	err := (&Reconciler{}).SetupWithManager(nil)
	require.ErrorContains(t, err, "API reader is not configured")
}
