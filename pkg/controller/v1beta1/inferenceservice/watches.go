package inferenceservice

import (
	"context"
	"reflect"
	"strings"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// isvcGroupKind identifies an InferenceService controller owner reference,
// matched by group and kind so a same-kind owner from another API group does
// not count.
var isvcGroupKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService").GroupKind()

// endpointSliceToISVC maps an EndpointSlice to the InferenceService whose
// Service it belongs to: the per-Component headless Service, the routed
// per-revision Services and their headless twins alike. A Service the
// InferenceService controls names it through the owner reference, whatever
// the Service is called; a Service that cannot be read, or that another
// kind controls, is parsed by name. The coordination pass counts a pod for
// the traffic split only once its routed Service lists it, so the slice
// that lists the pod must start the pass that reads it.
func (r *InferenceServiceReconciler) endpointSliceToISVC(ctx context.Context, obj client.Object) []reconcile.Request {
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		return nil
	}
	serviceName := slice.Labels[discoveryv1.LabelServiceName]
	if serviceName == "" {
		return nil
	}
	svc := &v1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: slice.Namespace, Name: serviceName}, svc); err == nil {
		if ref := metav1.GetControllerOf(svc); ref != nil &&
			schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind).GroupKind() == isvcGroupKind {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: slice.Namespace, Name: ref.Name}}}
		}
	}
	return EndpointSliceToISVC(ctx, obj)
}

// headlessServiceSuffix is the trailing component of the per-Component
// headless Service name (`<prefix>-<component>-headless`). The
// EndpointSlice mapper uses it to recognise an OMENative-owned slice
// and reverse-lookup the name prefix.
const headlessServiceSuffix = "-headless"

// perRevisionServiceInfix is the segment between the `<prefix>-<component>`
// prefix and the revision hash in a per-revision Service name
// (`<prefix>-<component>-rev-<hash>`, with the headless suffix on its twin).
const perRevisionServiceInfix = "-rev-"

// EndpointSliceToISVC maps an EndpointSlice of an OMENative Service to the
// InferenceService reconcile key its name prefix spells. Returns an empty
// slice for any event that doesn't target an OMENative-managed Service —
// the EndpointSlice watch is unfiltered, so the mapper does all the
// filtering.
//
// EndpointSlice carries the `kubernetes.io/service-name` label pointing
// at its parent Service. OMENative names its Services
// `<prefix>-<component>-headless`, `<prefix>-<component>-rev-<hash>` and
// `<prefix>-<component>-rev-<hash>-headless`, prefix being the replica's
// name prefix: the InferenceService name for a projected replica, which is
// what the parse recovers. The controller's mapper asks the Service's owner
// first and parses only a Service it cannot read.
func EndpointSliceToISVC(ctx context.Context, obj client.Object) []reconcile.Request {
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		return nil
	}
	serviceName := slice.Labels[discoveryv1.LabelServiceName]
	isvcName, _, ok := isvcFromOMENativeServiceName(serviceName)
	if !ok {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: slice.Namespace,
			Name:      isvcName,
		},
	}}
}

// isvcFromOMENativeServiceName parses the three names OMENative gives a
// Service after an InferenceService, `<isvc>-<component>-headless`,
// `<isvc>-<component>-rev-<hash>` and `<isvc>-<component>-rev-<hash>-headless`,
// into (isvc, component, ok). A per-revision name is read from its last
// revision infix, so an InferenceService whose own name carries one still
// parses.
func isvcFromOMENativeServiceName(name string) (string, v1beta1.ComponentType, bool) {
	if isvc, component, ok := isvcFromHeadlessServiceName(name); ok {
		return isvc, component, true
	}
	trimmed := strings.TrimSuffix(name, headlessServiceSuffix)
	i := strings.LastIndex(trimmed, perRevisionServiceInfix)
	if i <= 0 {
		return "", "", false
	}
	return isvcFromHeadlessServiceName(trimmed[:i] + headlessServiceSuffix)
}

// isvcFromHeadlessServiceName parses `<isvc>-<component>-headless`
// into (isvc, component, ok). The component segment is matched
// against the v1beta1 component-type set so an arbitrary
// `-foo-headless` Service in the same namespace doesn't masquerade
// as OMENative-owned.
func isvcFromHeadlessServiceName(name string) (string, v1beta1.ComponentType, bool) {
	if name == "" || !strings.HasSuffix(name, headlessServiceSuffix) {
		return "", "", false
	}
	trimmed := strings.TrimSuffix(name, headlessServiceSuffix)
	for _, component := range v1beta1.RoleTypes() {
		suffix := "-" + string(component)
		if strings.HasSuffix(trimmed, suffix) {
			isvc := strings.TrimSuffix(trimmed, suffix)
			if isvc == "" {
				return "", "", false
			}
			return isvc, component, true
		}
	}
	return "", "", false
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
// A status update that changes a canary record (status.components[*].canary
// or the status.canary alias) is admitted too: the record is the canary
// executor's state, and a write to it from any writer is read only by a pass.
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
			return metaOrSpecChanged(e.ObjectOld, e.ObjectNew) || canaryRecordChanged(e.ObjectOld, e.ObjectNew)
		},
	}
}

// canaryRecordChanged reports whether an update touched a canary record. The
// executor stamps a record only on a transition or a sample, never on every
// pass, so its own flush of a changed record wakes one pass, not a loop.
func canaryRecordChanged(oldObj, newObj client.Object) bool {
	before, ok := oldObj.(*v1beta1.InferenceService)
	if !ok {
		return false
	}
	after, ok := newObj.(*v1beta1.InferenceService)
	if !ok {
		return false
	}
	if !equality.Semantic.DeepEqual(before.Status.Canary, after.Status.Canary) {
		return true
	}
	for c, cs := range after.Status.Components {
		if !equality.Semantic.DeepEqual(before.Status.Components[c].Canary, cs.Canary) {
			return true
		}
	}
	for c, cs := range before.Status.Components {
		if _, kept := after.Status.Components[c]; !kept && cs.Canary != nil {
			return true
		}
	}
	return false
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
