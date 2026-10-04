package inferenceservice

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// endpointSliceToISVC admits exactly the OMENative headless Services the
// name parse recognizes and, among those, enqueues the InferenceService
// that controls the Service over the parsed prefix; a Service it cannot
// read, or that another kind controls, keeps the parsed name.
func TestEndpointSliceToISVC(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, discoveryv1.AddToScheme, v1beta1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	isController := true
	ownedBy := func(name, apiVersion, kind, owner string) *corev1.Service {
		return &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "team-a",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: apiVersion, Kind: kind,
				Name: owner, UID: types.UID(owner + "-uid"), Controller: &isController,
			}},
		}}
	}
	r := &InferenceServiceReconciler{Client: fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(
		ownedBy("pool-a-engine-headless", v1beta1.SchemeGroupVersion.String(), "InferenceService", "svc"),
		ownedBy("pool-d-decoder-headless", v1beta1.SchemeGroupVersion.String(), "InferenceReplica", "pool-d"),
		ownedBy("other-engine-headless", "serving.example.com/v1", "InferenceService", "foreign"),
		ownedBy("pool-a-engine-rev-abc", v1beta1.SchemeGroupVersion.String(), "InferenceService", "svc"),
	).Build()}
	slice := func(service string) *discoveryv1.EndpointSlice {
		return &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
			Name: service + "-x7k2p", Namespace: "team-a",
			Labels: map[string]string{discoveryv1.LabelServiceName: service},
		}}
	}
	want := func(isvc string) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: isvc}}}
	}
	for _, tc := range []struct {
		name    string
		service string
		want    []reconcile.Request
	}{
		{"headless Service controlled by an InferenceService, by owner", "pool-a-engine-headless", want("svc")},
		{"headless Service controlled by a replica, by name", "pool-d-decoder-headless", want("pool-d")},
		{"headless Service controlled by a same-kind owner of another group, by name", "other-engine-headless", want("other")},
		{"headless Service that cannot be read, by name", "svc-engine-headless", want("svc")},
		{"per-revision Service", "pool-a-engine-rev-abc", nil},
		{"foreign Service", "kube-dns", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := r.endpointSliceToISVC(context.Background(), slice(tc.service))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("endpointSliceToISVC(%s) = %v, want %v", tc.service, got, tc.want)
			}
		})
	}
	if got := r.endpointSliceToISVC(context.Background(), &corev1.Pod{}); got != nil {
		t.Fatalf("non-slice object: got %v, want nil", got)
	}
}

// TestISVCReconcileTriggerPredicate_DropsStatusOnlyUpdate pins the For()
// watch filter that breaks the HPA-status reconcile loop: a status-only ISVC
// update (resourceVersion bumped, generation/labels/annotations unchanged)
// must be dropped, while a generation change AND an annotation change must
// pass. The annotation case is load-bearing — OME drives canary
// promote/rollback through annotations that do NOT bump generation, so a
// blanket GenerationChangedPredicate would silently break rollouts.
func TestISVCReconcileTriggerPredicate_DropsStatusOnlyUpdate(t *testing.T) {
	g := gomega.NewWithT(t)
	p := isvcReconcileTriggerPredicate()

	base := func() *v1beta1.InferenceService {
		return &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "ns1",
				Name:        "isvc1",
				Generation:  3,
				Labels:      map[string]string{"app": "x"},
				Annotations: map[string]string{"ome.io/canary": "stable"},
			},
		}
	}

	cases := []struct {
		name     string
		mutate   func(newObj *v1beta1.InferenceService)
		wantPass bool
	}{
		{
			name: "status-only update (rv bump, no gen/meta change) → dropped",
			mutate: func(o *v1beta1.InferenceService) {
				o.ResourceVersion = "999"
				o.Status.URL = nil // status churn
			},
			wantPass: false,
		},
		{
			name: "generation change (spec) → passes",
			mutate: func(o *v1beta1.InferenceService) {
				o.Generation = 4
			},
			wantPass: true,
		},
		{
			name: "annotation change (no gen bump) → passes",
			mutate: func(o *v1beta1.InferenceService) {
				o.Annotations["ome.io/canary"] = "promote"
			},
			wantPass: true,
		},
		{
			name: "label change (no gen bump) → passes",
			mutate: func(o *v1beta1.InferenceService) {
				o.Labels["app"] = "y"
			},
			wantPass: true,
		},
		{
			// Deleting an ISVC that carries the controller finalizer is an
			// UPDATE (deletionTimestamp set), not a Delete event. Dropping
			// it stalls finalizer teardown until an unrelated event wakes
			// the reconciler.
			name: "deletionTimestamp set (finalizer-held delete) → passes",
			mutate: func(o *v1beta1.InferenceService) {
				now := metav1.Now()
				o.DeletionTimestamp = &now
				o.Finalizers = []string{"ome.io/finalizer"}
			},
			wantPass: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			oldObj := base()
			newObj := base()
			newObj.ResourceVersion = "2"
			tc.mutate(newObj)
			got := p.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj})
			g.Expect(got).To(gomega.Equal(tc.wantPass))
		})
	}

	// Create / Delete / Generic always pass — lifecycle transitions must
	// reach the reconciler.
	g.Expect(p.Create(event.CreateEvent{Object: base()})).To(gomega.BeTrue())
	g.Expect(p.Delete(event.DeleteEvent{Object: base()})).To(gomega.BeTrue())
	g.Expect(p.Generic(event.GenericEvent{Object: base()})).To(gomega.BeTrue())
}

// TestOwnedStatusIgnoringPredicate_DropsHPAStatusOnly pins the Owns(HPA)
// filter: an HPA status-only update (the metrics controller rewriting
// .status.conditions, no generation/metadata change) must be dropped so it
// does not re-reconcile the owning ISVC; an HPA spec change (generation bump)
// must pass.
func TestOwnedStatusIgnoringPredicate_DropsHPAStatusOnly(t *testing.T) {
	g := gomega.NewWithT(t)
	p := ownedStatusIgnoringPredicate()

	base := func() *autoscalingv2.HorizontalPodAutoscaler {
		return &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "hpa1", Generation: 1},
		}
	}

	// Status-only update: generation unchanged, only .status churns → drop.
	oldHPA := base()
	newHPA := base()
	newHPA.ResourceVersion = "2"
	newHPA.Status.CurrentReplicas = 5
	newHPA.Status.Conditions = []autoscalingv2.HorizontalPodAutoscalerCondition{
		{Type: autoscalingv2.ScalingActive, Reason: "FailedGetResourceMetric"},
	}
	g.Expect(p.Update(event.UpdateEvent{ObjectOld: oldHPA, ObjectNew: newHPA})).To(gomega.BeFalse(),
		"HPA status-only churn must not re-reconcile the ISVC")

	// Spec change: generation bumped → pass.
	specOld := base()
	specNew := base()
	specNew.Generation = 2
	g.Expect(p.Update(event.UpdateEvent{ObjectOld: specOld, ObjectNew: specNew})).To(gomega.BeTrue(),
		"HPA spec change must re-reconcile the ISVC")

	// Metadata change with no generation bump → pass.
	metaOld := base()
	metaNew := base()
	metaNew.Annotations = map[string]string{"k": "v"}
	g.Expect(p.Update(event.UpdateEvent{ObjectOld: metaOld, ObjectNew: metaNew})).To(gomega.BeTrue())

	// Create / Delete / Generic always pass.
	g.Expect(p.Create(event.CreateEvent{Object: base()})).To(gomega.BeTrue())
	g.Expect(p.Delete(event.DeleteEvent{Object: base()})).To(gomega.BeTrue())
}

// isvcWatchFixture is an InferenceService as the For() watch sees it once the
// controller finalizer has landed.
func isvcWatchFixture() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "ns1",
			Name:            "isvc1",
			UID:             types.UID("isvc1-uid"),
			ResourceVersion: "1",
			Generation:      3,
			Labels:          map[string]string{"app": "x"},
			Annotations:     map[string]string{"ome.io/canary": "stable"},
			Finalizers:      []string{inferenceServiceFinalizer},
		},
	}
}

// markDeleting stamps the fixture the way an accepted delete does: the
// deletionTimestamp is set and the finalizers stay.
func markDeleting(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	ts := metav1.NewTime(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	isvc.DeletionTimestamp = &ts
	return isvc
}

// isvcReq is the reconcile key the ISVC fixture must enqueue under.
var isvcReq = reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns1", Name: "isvc1"}}

func newWatchTestQueue() workqueue.TypedRateLimitingInterface[reconcile.Request] {
	return workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
		workqueue.TypedRateLimitingQueueConfig[reconcile.Request]{})
}

// drainRequests returns every request the handler put on the queue.
func drainRequests(q workqueue.TypedRateLimitingInterface[reconcile.Request]) []reconcile.Request {
	var out []reconcile.Request
	for q.Len() > 0 {
		item, shutdown := q.Get()
		if shutdown {
			break
		}
		q.Done(item)
		out = append(out, item)
	}
	return out
}

// TestISVCReconcileTriggerPredicate_AdmitsEveryUpdateOfDeletingISVC pins the
// deletion contract of the For() watch: an InferenceService whose
// deletionTimestamp is set passes on every update, not only on the update
// that set it. The delete of a finalizer-bearing object is an UPDATE, and the
// reconciler's finalizer pass is what the deletion waits on, so the watch
// stays level-triggered while the object is deleting: a later write by any
// other actor (a foreign finalizer dropping, a status write, a resync replay)
// re-enqueues it. The status-only filter still holds for a live object.
func TestISVCReconcileTriggerPredicate_AdmitsEveryUpdateOfDeletingISVC(t *testing.T) {
	deleting := func() *v1beta1.InferenceService { return markDeleting(isvcWatchFixture()) }

	cases := []struct {
		name     string
		oldObj   func() *v1beta1.InferenceService
		newObj   func() *v1beta1.InferenceService
		wantPass bool
	}{
		{
			name:   "delete accepted: deletionTimestamp appears, finalizer held",
			oldObj: isvcWatchFixture,
			newObj: func() *v1beta1.InferenceService {
				o := deleting()
				o.ResourceVersion = "2"
				return o
			},
			wantPass: true,
		},
		{
			name: "already deleting: a foreign finalizer is removed",
			oldObj: func() *v1beta1.InferenceService {
				o := deleting()
				o.Finalizers = append(o.Finalizers, "example.com/other")
				return o
			},
			newObj: func() *v1beta1.InferenceService {
				o := deleting()
				o.ResourceVersion = "2"
				return o
			},
			wantPass: true,
		},
		{
			name:   "already deleting: status-only write",
			oldObj: deleting,
			newObj: func() *v1beta1.InferenceService {
				o := deleting()
				o.ResourceVersion = "2"
				o.Status.PinnedRevisionName = "rev-1"
				return o
			},
			wantPass: true,
		},
		{
			name:     "already deleting: resync replays the same object",
			oldObj:   deleting,
			newObj:   deleting,
			wantPass: true,
		},
		{
			name:   "live object: status-only write stays filtered",
			oldObj: isvcWatchFixture,
			newObj: func() *v1beta1.InferenceService {
				o := isvcWatchFixture()
				o.ResourceVersion = "2"
				o.Status.PinnedRevisionName = "rev-1"
				return o
			},
			wantPass: false,
		},
	}

	p := isvcReconcileTriggerPredicate()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			got := p.Update(event.UpdateEvent{ObjectOld: tc.oldObj(), ObjectNew: tc.newObj()})
			g.Expect(got).To(gomega.Equal(tc.wantPass))
		})
	}
}

// TestISVCForWatch_DeletionUpdateEnqueuesObject drives the For() watch the way
// the controller wires it — the trigger predicate, then the object handler —
// and asserts the deleting InferenceService's own key lands on the queue,
// both when the deletionTimestamp appears and on a later update of the
// already-deleting object.
func TestISVCForWatch_DeletionUpdateEnqueuesObject(t *testing.T) {
	deliver := func(oldObj, newObj client.Object) []reconcile.Request {
		q := newWatchTestQueue()
		defer q.ShutDown()
		evt := event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
		if isvcReconcileTriggerPredicate().Update(evt) {
			(&handler.EnqueueRequestForObject{}).Update(context.Background(), evt, q)
		}
		return drainRequests(q)
	}

	t.Run("deletionTimestamp appears", func(t *testing.T) {
		g := gomega.NewWithT(t)
		newObj := markDeleting(isvcWatchFixture())
		newObj.ResourceVersion = "2"
		g.Expect(deliver(isvcWatchFixture(), newObj)).To(gomega.ConsistOf(isvcReq))
	})

	t.Run("already deleting, foreign finalizer removed", func(t *testing.T) {
		g := gomega.NewWithT(t)
		oldObj := markDeleting(isvcWatchFixture())
		oldObj.Finalizers = append(oldObj.Finalizers, "example.com/other")
		newObj := markDeleting(isvcWatchFixture())
		newObj.ResourceVersion = "2"
		g.Expect(deliver(oldObj, newObj)).To(gomega.ConsistOf(isvcReq))
	})
}

// TestISVCOwnsWatch_ChildEventsEnqueueDeletingParent drives the Owns() handler
// the controller registers — owner-reference mapping restricted to the
// controller owner — with children whose controller owner is an
// InferenceService under deletion. The mapping reads only the child's owner
// reference, so create, update and delete of every owned kind enqueue the
// parent whatever the parent's own deletion state; a child under deletion
// (finalizer held, deletionTimestamp set) maps the same way.
func TestISVCOwnsWatch_ChildEventsEnqueueDeletingParent(t *testing.T) {
	g := gomega.NewWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).To(gomega.Succeed())
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{v1beta1.SchemeGroupVersion})
	isvcGVK := v1beta1.SchemeGroupVersion.WithKind("InferenceService")
	mapper.Add(isvcGVK, meta.RESTScopeNamespace)
	ownerHandler := handler.EnqueueRequestForOwner(scheme, mapper, &v1beta1.InferenceService{}, handler.OnlyControllerOwner())

	parent := markDeleting(isvcWatchFixture())
	ownedMeta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Namespace:       parent.Namespace,
			Name:            name,
			ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(parent, isvcGVK)},
		}
	}
	// deletingChild is a child mid-teardown: its own finalizer holds it while
	// its deletionTimestamp is set.
	deletingChild := func(obj client.Object) client.Object {
		obj.SetResourceVersion("2")
		obj.SetFinalizers([]string{"example.com/child-finalizer"})
		ts := metav1.NewTime(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
		obj.SetDeletionTimestamp(&ts)
		return obj
	}

	children := []struct {
		name      string
		live      func() client.Object
		predicate predicate.Predicate
	}{
		{
			name: "InferenceReplica",
			live: func() client.Object {
				return &v1beta1.InferenceReplica{ObjectMeta: ownedMeta("isvc1-engine")}
			},
		},
		{
			name: "Deployment",
			live: func() client.Object {
				return &appsv1.Deployment{ObjectMeta: ownedMeta("isvc1-engine")}
			},
		},
		{
			name: "ControllerRevision",
			live: func() client.Object {
				return &appsv1.ControllerRevision{ObjectMeta: ownedMeta("isvc1-engine-rev-1")}
			},
		},
		{
			// The HPA watch carries the status-ignoring predicate; a child
			// deletion is a metadata transition it admits.
			name: "HorizontalPodAutoscaler",
			live: func() client.Object {
				return &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: ownedMeta("isvc1-engine")}
			},
			predicate: ownedStatusIgnoringPredicate(),
		},
	}

	for _, child := range children {
		t.Run(child.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()

			q := newWatchTestQueue()
			defer q.ShutDown()

			create := event.CreateEvent{Object: child.live()}
			if child.predicate == nil || child.predicate.Create(create) {
				ownerHandler.Create(ctx, create, q)
			}
			g.Expect(drainRequests(q)).To(gomega.ConsistOf(isvcReq), "create must enqueue the parent")

			update := event.UpdateEvent{ObjectOld: child.live(), ObjectNew: deletingChild(child.live())}
			if child.predicate == nil || child.predicate.Update(update) {
				ownerHandler.Update(ctx, update, q)
			}
			g.Expect(drainRequests(q)).To(gomega.ConsistOf(isvcReq), "a child's deletion update must enqueue the parent")

			del := event.DeleteEvent{Object: deletingChild(child.live())}
			if child.predicate == nil || child.predicate.Delete(del) {
				ownerHandler.Delete(ctx, del, q)
			}
			g.Expect(drainRequests(q)).To(gomega.ConsistOf(isvcReq), "delete must enqueue the parent")
		})
	}
}
