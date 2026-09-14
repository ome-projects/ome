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
	c := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(objs...).Build()
	return &Reconciler{
		Client:    c,
		Log:       log.Log,
		Publisher: NewGatewayAPIPublisher(c, cfg),
		Config:    cfg,
	}, c
}

func reconcile(t *testing.T, r *Reconciler) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "svc", Namespace: "prod"}})
	require.NoError(t, err)
}

func updateEvent(old, nw *v1beta1.InferenceService) event.UpdateEvent {
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
	r := &Reconciler{Client: c, Log: log.Log, Publisher: publisher, Config: cfg}

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

	// Placement regresses to Racing (winner lost). Publisher must tear down.
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "svc", Namespace: "prod"}, cur))
	cur.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
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

func TestReconcile_PublishesTrafficMapWeights(t *testing.T) {
	// A Split ISVC with two admitted homes whose reactive ready-replica ratio is
	// 5:2, plus a TrafficMap the routing controller published carrying a
	// capacity-aware 1:3 split. The route must honor the TrafficMap.
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
	assert.Equal(t, int32(1), *refs[0].Weight, "home a takes the TrafficMap weight, not its 5 ready replicas")
	assert.Equal(t, int32(3), *refs[1].Weight, "home b takes the TrafficMap weight, not its 2 ready replicas")
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
					{Cluster: "workload-3", Phase: v1beta1.CandidatePhasePlaced}, // gated: not a home
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
				Candidates: []v1beta1.CandidatePlacement{{Cluster: "workload-1", Phase: v1beta1.CandidatePhasePlaced}},
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
	})
}

func TestPlacementPublishChange(t *testing.T) {
	base := placedISVC("cloud-a", "h.example")

	t.Run("phase change passes", func(t *testing.T) {
		old := base.DeepCopy()
		nw := base.DeepCopy()
		nw.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
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
