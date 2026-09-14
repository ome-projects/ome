package routing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	testName = "svc"
	testNS   = "prod"
)

func routingScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(s))
	require.NoError(t, clientgoscheme.AddToScheme(s))
	return s
}

// splitISVC builds a Placed, Split-mode ISVC with the given per-home admitted
// and ready replica counts. clusters, admitted, and ready are index-aligned.
func splitISVC(clusters []string, admitted, ready []int32) *v1beta1.InferenceService {
	cands := make([]v1beta1.CandidatePlacement, len(clusters))
	for i, c := range clusters {
		cands[i] = v1beta1.CandidatePlacement{
			Cluster:          c,
			Phase:            v1beta1.CandidatePhaseAdmitted,
			Endpoint:         apis.HTTPS(c + ".example"),
			AdmittedReplicas: admitted[i],
			ReadyReplicas:    ready[i],
		}
	}
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNS, UID: "uid-1", Generation: 7},
		Spec: v1beta1.InferenceServiceSpec{
			Placement: &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSplit},
		},
		Status: v1beta1.InferenceServiceStatus{
			Placement: &v1beta1.PlacementStatus{
				Phase:      v1beta1.PlacementPhasePlaced,
				Candidates: cands,
			},
		},
	}
}

func newReconciler(t *testing.T, cfg Config, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	c := fakeclient.NewClientBuilder().WithScheme(routingScheme(t)).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.TrafficMap{}).
		WithObjects(objs...).Build()
	return &Reconciler{Client: c, Log: log.Log, Config: cfg}, c
}

func reconcile(t *testing.T, r *Reconciler) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: testName, Namespace: testNS},
	})
	require.NoError(t, err)
}

func getTrafficMap(t *testing.T, c client.Client) (*v1beta1.TrafficMap, bool) {
	t.Helper()
	tm := &v1beta1.TrafficMap{}
	err := c.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, tm)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	require.NoError(t, err)
	return tm, true
}

func TestReconcile_SplitGeneratesWeightedTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"b", "a"}, []int32{2, 5}, []int32{2, 5})
	r, c := newReconciler(t, Config{Enabled: true}, isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok, "TrafficMap should be generated")
	assert.Equal(t, testName, tm.Spec.Service)
	assert.Equal(t, v1beta1.PlacementModeSplit, tm.Spec.Mode)
	assert.Equal(t, int64(7), tm.Spec.ObservedISVCGeneration)

	require.Len(t, tm.Spec.Entries, 2)
	// Entries are cluster-sorted: "a" (5 replicas) then "b" (2 replicas) -> 5:2.
	assert.Equal(t, "a", tm.Spec.Entries[0].Cluster)
	assert.Equal(t, int32(5), tm.Spec.Entries[0].Weight)
	assert.True(t, tm.Spec.Entries[0].Healthy)
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	assert.Equal(t, int32(5), tm.Spec.Entries[0].Capacity.Allocated)
	assert.Equal(t, int32(5), tm.Spec.Entries[0].Capacity.Ready)
	require.NotNil(t, tm.Spec.Entries[0].Endpoint)
	assert.Equal(t, "a.example", tm.Spec.Entries[0].Endpoint.Host)

	assert.Equal(t, "b", tm.Spec.Entries[1].Cluster)
	assert.Equal(t, int32(2), tm.Spec.Entries[1].Weight)

	// Owner-ref'd to the ISVC for garbage collection.
	require.True(t, metav1.IsControlledBy(tm, isvc), "TrafficMap should be controlled by its ISVC")
}

func TestReconcile_UnhealthyHomeGatedToZero(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{6, 4}, []int32{6, 0})
	r, c := newReconciler(t, Config{Enabled: true}, isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight, "healthy home keeps weight")
	assert.True(t, tm.Spec.Entries[0].Healthy)
	assert.Equal(t, int32(0), tm.Spec.Entries[1].Weight, "unhealthy home gated to 0")
	assert.False(t, tm.Spec.Entries[1].Healthy)
}

func TestReconcile_DisabledReapsExistingTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	// Pre-existing owned TrafficMap that should be reaped when the feature is off.
	existing := &v1beta1.TrafficMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testName,
			Namespace: testNS,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, v1beta1.SchemeGroupVersion.WithKind("InferenceService")),
			},
		},
	}
	r, c := newReconciler(t, Config{Enabled: false}, isvc, existing)

	reconcile(t, r)

	_, ok := getTrafficMap(t, c)
	assert.False(t, ok, "disabled controller should reap the TrafficMap it owns")
}

// routableCond returns the Routable condition, or nil when absent.
func routableCond(tm *v1beta1.TrafficMap) *metav1.Condition {
	return apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapRoutable)
}

// An ISVC that is not Placed keeps its map, emptied, with the reason. Deleting
// it would make "not placed yet" indistinguishable from a wedged controller.
func TestReconcile_NotPlacedKeepsEmptyTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
	r, c := newReconciler(t, Config{Enabled: true}, isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok, "an unroutable ISVC must still have a TrafficMap")
	assert.Empty(t, tm.Spec.Entries)
	assert.Equal(t, testName, tm.Spec.Service, "identity is populated even with no entries")
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonNotPlaced, cond.Reason)
}

// Placed, but no candidate publishes an endpoint: empty table, distinct reason.
// The reason is what separates this from NotPlaced, since both tables are empty.
func TestReconcile_NoAddressableHomeKeepsEmptyTrafficMap(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	isvc.Status.Placement.Candidates[0].Endpoint = nil
	r, c := newReconciler(t, Config{Enabled: true}, isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	assert.Empty(t, tm.Spec.Entries)
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonNoAddressableHome, cond.Reason)
}

// Every home addressable but none ready is Routable=TRUE: the weight fallback
// spreads traffic rather than dropping it, so the map is degraded, not dead.
func TestReconcile_AllHomesUnreadyIsRoutableButDegraded(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{3, 3}, []int32{0, 0})
	r, c := newReconciler(t, Config{Enabled: true}, isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	for _, e := range tm.Spec.Entries {
		assert.False(t, e.Healthy)
		assert.Equal(t, int32(1), e.Weight, "equal-weight fallback, not black-holed")
	}
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonAllHomesUnready, cond.Reason)
}

func TestReconcile_ServingHomeReportsRoutable(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	r, c := newReconciler(t, Config{Enabled: true}, isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	cond := routableCond(tm)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, cond.Reason)
}

// The publisher owns Programmed on the same subresource, so writing Routable
// must not drop it.
func TestReconcile_RoutableDoesNotClobberProgrammed(t *testing.T) {
	isvc := splitISVC([]string{"a"}, []int32{3}, []int32{3})
	r, c := newReconciler(t, Config{Enabled: true}, isvc)
	reconcile(t, r)
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)

	apimeta.SetStatusCondition(&tm.Status.Conditions, metav1.Condition{
		Type: v1beta1.TrafficMapProgrammed, Status: metav1.ConditionTrue,
		Reason: "Published", Message: "publisher realized the map",
	})
	require.NoError(t, c.Status().Update(context.Background(), tm))

	// A later pass that flips the ISVC unroutable must rewrite only our condition.
	isvc.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
	require.NoError(t, c.Status().Update(context.Background(), isvc))
	reconcile(t, r)

	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.NotNil(t, apimeta.FindStatusCondition(tm.Status.Conditions, v1beta1.TrafficMapProgrammed),
		"the publisher's condition must survive a Routable write")
	assert.Equal(t, v1beta1.TrafficMapReasonNotPlaced, routableCond(tm).Reason)
}

func TestReconcile_UpdatesExistingTrafficMapOnCapacityChange(t *testing.T) {
	isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
	r, c := newReconciler(t, Config{Enabled: true}, isvc)
	reconcile(t, r)
	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)

	// Home "a" scales up: 8 vs 2 -> 4:1.
	cur := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: testName, Namespace: testNS}, cur))
	cur.Status.Placement.Candidates[0].AdmittedReplicas = 8
	cur.Status.Placement.Candidates[0].ReadyReplicas = 8
	cur.Status.Placement.Candidates[1].AdmittedReplicas = 2
	cur.Status.Placement.Candidates[1].ReadyReplicas = 2
	require.NoError(t, c.Status().Update(context.Background(), cur))

	reconcile(t, r)
	tm, ok = getTrafficMap(t, c)
	require.True(t, ok)
	assert.Equal(t, int32(4), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
}

func TestReconcile_CapacityFactorWeightsHomes(t *testing.T) {
	// Two homes with equal admitted+ready replicas: without factors they would
	// weight 1:1. A per-cluster factor of 2 on "a" doubles its share -> 2:1.
	isvc := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})
	isvc.Spec.Placement.CapacityFactors = map[string]resource.Quantity{
		"a": resource.MustParse("2"),
	}
	r, c := newReconciler(t, Config{Enabled: true}, isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)

	assert.Equal(t, "a", tm.Spec.Entries[0].Cluster)
	assert.Equal(t, int32(2), tm.Spec.Entries[0].Weight, "factor-2 home carries double the share")
	require.NotNil(t, tm.Spec.Entries[0].Capacity)
	require.NotNil(t, tm.Spec.Entries[0].Capacity.Factor, "factor recorded as provenance")
	assert.Equal(t, "2", tm.Spec.Entries[0].Capacity.Factor.String())

	assert.Equal(t, "b", tm.Spec.Entries[1].Cluster)
	assert.Equal(t, int32(1), tm.Spec.Entries[1].Weight)
	require.NotNil(t, tm.Spec.Entries[1].Capacity)
	assert.Nil(t, tm.Spec.Entries[1].Capacity.Factor, "home absent from map uses the identity factor")
}

func TestReconcile_FractionalCapacityFactor(t *testing.T) {
	// "500m" halves a home's per-replica capacity: a(4 replicas)*0.5 vs b(4)*1
	// -> 2:4 -> reduced 1:2.
	isvc := splitISVC([]string{"a", "b"}, []int32{4, 4}, []int32{4, 4})
	isvc.Spec.Placement.CapacityFactors = map[string]resource.Quantity{
		"a": resource.MustParse("500m"),
	}
	r, c := newReconciler(t, Config{Enabled: true}, isvc)

	reconcile(t, r)

	tm, ok := getTrafficMap(t, c)
	require.True(t, ok)
	require.Len(t, tm.Spec.Entries, 2)
	assert.Equal(t, int32(1), tm.Spec.Entries[0].Weight)
	assert.Equal(t, int32(2), tm.Spec.Entries[1].Weight)
}

func TestRoutingTableChangePredicate(t *testing.T) {
	base := splitISVC([]string{"a", "b"}, []int32{5, 5}, []int32{5, 5})

	t.Run("no relevant change is dropped", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Annotations = map[string]string{"unrelated": "x"}
		assert.False(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("capacity change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Status.Placement.Candidates[0].ReadyReplicas = 1
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("routable-ness change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Status.Placement.Phase = v1beta1.PlacementPhaseRacing
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("deletion transition is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		now := metav1.Now()
		nw.DeletionTimestamp = &now
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})

	t.Run("capacity factor change is admitted", func(t *testing.T) {
		nw := base.DeepCopy()
		nw.Spec.Placement.CapacityFactors = map[string]resource.Quantity{"a": resource.MustParse("2")}
		assert.True(t, routingTableChange.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: nw}))
	})
}
