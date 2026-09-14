package servingruntime

import (
	"reflect"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// inheritanceTriggerPredicate gates the inheritance controller's watches on
// the two things that can change a resolved chain: the spec (via generation)
// and the annotations that declare the parent.
//
// Generation alone is not enough. inherit-from is an annotation, and both
// runtime CRDs enable the status subresource, so the apiserver leaves
// .metadata.generation untouched when a runtime is repointed at a different
// parent — the edit that most needs re-resolution is exactly the one a
// generation-only filter drops.
//
// Status-only writes touch neither, so the controller's own status updates
// are still dropped and cannot feed back into its queue.
//
// Create / Delete / Generic always pass: they are not status-churn sources
// and each changes the graph.
func inheritanceTriggerPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return true
			}
			return specOrAnnotationsChanged(e.ObjectOld, e.ObjectNew)
		},
	}
}

// specOrAnnotationsChanged reports whether an old→new transition touched
// .metadata.generation or .metadata.annotations. The annotation comparison
// is whole-map rather than inherit-from only: an unrelated annotation edit
// then costs one re-resolve that finds no status diff and writes nothing.
func specOrAnnotationsChanged(oldObj, newObj client.Object) bool {
	if oldObj.GetGeneration() != newObj.GetGeneration() {
		return true
	}
	return !reflect.DeepEqual(oldObj.GetAnnotations(), newObj.GetAnnotations())
}
