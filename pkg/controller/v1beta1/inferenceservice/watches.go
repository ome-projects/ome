package inferenceservice

import (
	"context"
	"reflect"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative"
)

// isvcGroupKind identifies an InferenceService controller owner reference,
// matched by group and kind so a same-kind owner from another API group does
// not count.
var isvcGroupKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService").GroupKind()

// endpointSliceToISVC maps an EndpointSlice of an OMENative headless Service
// to the InferenceService the Service belongs to. The name parse selects the
// Services (`<prefix>-<component>-headless`, prefix being a replica's name
// prefix); the Service's controller owner, when it is an InferenceService,
// names the service, since the prefix is the service name only for a
// projected replica. A Service that cannot be read, or that another kind
// controls, keeps the parsed name. Slices of any other Service map to
// nothing.
func (r *InferenceServiceReconciler) endpointSliceToISVC(ctx context.Context, obj client.Object) []reconcile.Request {
	reqs := omenative.EndpointSliceToISVC(ctx, obj)
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if len(reqs) == 0 || !ok {
		return nil
	}
	svc := &v1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: slice.Namespace, Name: slice.Labels[discoveryv1.LabelServiceName]}, svc); err == nil {
		if ref := metav1.GetControllerOf(svc); ref != nil &&
			schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).GroupKind() == isvcGroupKind {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: slice.Namespace, Name: ref.Name}}}
		}
	}
	return reqs
}

// isvcReconcileTriggerPredicate gates the controller's For(&InferenceService{})
// watch so the reconciler reacts to spec and metadata changes but NOT to its
// own status writes.
//
// Why this exists: the autoscaler-status mirror copies the live HPA's
// conditions onto status.components.<comp>.autoscaler.conditions. When a
// Component's HPA can't read a metric (e.g. a CPU-utilization HPA on pods with
// no CPU request) the HPA controller perpetually rewrites a
// ScalingActive=False / FailedGetResourceMetric condition. Even after we
// normalize the rotating pod name out of the message, any status write the
// reconciler makes still bumps resourceVersion — and an unfiltered For() watch
// re-enqueues on every such self-write, producing a reconcile + status-write
// storm.
//
// We deliberately do NOT use predicate.GenerationChangedPredicate: OME drives
// rollouts (canary promote/rollback, etc.) through annotations that do not bump
// .metadata.generation. Dropping those would break rollouts. Instead we enqueue
// when generation changed OR labels/annotations changed, and drop updates that
// only touched status / resourceVersion / managedFields.
//
// An InferenceService under deletion passes on every update, whatever the
// update touched. The delete of a finalizer-bearing object is an UPDATE
// (deletionTimestamp set) and the reconciler's finalizer pass is what the
// deletion waits on, so the filter keys on the deletion state itself, not
// on the transition that set it: any later write to the object — a foreign
// finalizer dropping, a status write, a resync replay — re-enqueues it. A
// deleting object's pass writes no status and is a no-op once the controller
// finalizer is gone, so this reopens no churn.
//
// Create / Delete / Generic always pass — they are not status-churn sources and
// the reconciler must see object lifecycle transitions.
func isvcReconcileTriggerPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return true
			}
			if e.ObjectNew.GetDeletionTimestamp() != nil {
				return true
			}
			return metaOrSpecChanged(e.ObjectOld, e.ObjectNew)
		},
	}
}

// ownedStatusIgnoringPredicate gates an Owns() watch so the reconciler reacts
// to spec/generation and metadata changes on the owned object but ignores
// status-only churn.
//
// This is used for the HPA (and KEDA ScaledObject) Owns() watches. The HPA
// controller continuously rewrites the HPA's .status.conditions when it can't
// fetch a metric; without this filter every such rewrite re-enqueues the owning
// ISVC. We still react to HPA spec changes (e.g. min/max replicas) and metadata
// changes via the generation-or-metadata comparison below.
//
// Create / Delete / Generic always pass.
func ownedStatusIgnoringPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return true
			}
			return metaOrSpecChanged(e.ObjectOld, e.ObjectNew)
		},
	}
}

// metaOrSpecChanged reports whether an old→new object transition touched
// .metadata.generation (a proxy for spec changes on a CRD/built-in with a
// status subresource), .metadata.deletionTimestamp, labels, or annotations.
// Status-only writes leave all of these untouched (generation only bumps on
// spec changes; the apiserver does not advance generation for status
// subresource writes), so they return false and are dropped.
//
// The deletionTimestamp check is load-bearing: deleting an object that
// carries a finalizer surfaces as an UPDATE event (deletionTimestamp set),
// not a Delete event — the Delete event only fires when the object is
// actually removed, after every finalizer drops. Without the check the
// reconciler never learns the deletion started and finalizer processing
// stalls until some unrelated event happens to wake it.
func metaOrSpecChanged(oldObj, newObj client.Object) bool {
	if oldObj.GetGeneration() != newObj.GetGeneration() {
		return true
	}
	if (oldObj.GetDeletionTimestamp() == nil) != (newObj.GetDeletionTimestamp() == nil) {
		return true
	}
	if !reflect.DeepEqual(oldObj.GetLabels(), newObj.GetLabels()) {
		return true
	}
	if !reflect.DeepEqual(oldObj.GetAnnotations(), newObj.GetAnnotations()) {
		return true
	}
	return false
}
