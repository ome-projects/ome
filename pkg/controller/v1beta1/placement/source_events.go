package placement

import (
	"maps"
	"slices"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Source intent and deletion drive placement. Member events and scheduled
// retries refresh observations without a feedback loop from placement outputs.
var placementRelevantSourceChange = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldSource, oldOK := e.ObjectOld.(*v1beta1.InferenceService)
		newSource, newOK := e.ObjectNew.(*v1beta1.InferenceService)
		if !oldOK || !newOK || oldSource == nil || newSource == nil {
			return true
		}
		return oldSource.UID != newSource.UID || oldSource.Generation != newSource.Generation ||
			!oldSource.DeletionTimestamp.Equal(newSource.DeletionTimestamp) ||
			!maps.Equal(oldSource.Labels, newSource.Labels) || !maps.Equal(oldSource.Annotations, newSource.Annotations) ||
			(slices.Contains(oldSource.Finalizers, PlacementFinalizer) && !slices.Contains(newSource.Finalizers, PlacementFinalizer))
	},
}
