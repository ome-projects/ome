package placement

import (
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	// TrafficMapPublisherFinalizer preserves a TrafficMap and its durable
	// publication journal until all external effects have been removed.
	TrafficMapPublisherFinalizer = "ome.io/trafficmap-publisher"
)

// TrafficMapHasController reports whether any owner reference claims controller
// ownership. It deliberately scans every reference so a malformed second
// controller cannot be hidden behind an otherwise exact first reference.
func TrafficMapHasController(trafficMap *v1beta1.TrafficMap) bool {
	if trafficMap == nil {
		return false
	}
	for i := range trafficMap.OwnerReferences {
		if controller := trafficMap.OwnerReferences[i].Controller; controller != nil && *controller {
			return true
		}
	}
	return false
}

// TrafficMapHasExactController reports whether at least one controller
// reference exists and every controller reference identifies source exactly.
func TrafficMapHasExactController(
	trafficMap *v1beta1.TrafficMap,
	source *v1beta1.InferenceService,
) bool {
	if trafficMap == nil || source == nil || source.UID == "" {
		return false
	}
	found := false
	for i := range trafficMap.OwnerReferences {
		owner := &trafficMap.OwnerReferences[i]
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		found = true
		if owner.APIVersion != v1beta1.SchemeGroupVersion.String() ||
			owner.Kind != "InferenceService" || owner.Name != source.Name || owner.UID != source.UID {
			return false
		}
	}
	return found
}

// trafficMapHasPublicationEvidence recognizes durable state that may represent
// an external publication even when a legacy map lost its controller reference
// before routing established sourceUID.
func trafficMapHasPublicationEvidence(trafficMap *v1beta1.TrafficMap) bool {
	if trafficMap == nil {
		return false
	}
	return controllerutil.ContainsFinalizer(trafficMap, TrafficMapPublisherFinalizer) ||
		trafficMap.Status.Publisher != nil || trafficMap.Status.Published ||
		trafficMap.Status.GatewayRef != nil || trafficMap.Status.ObservedTrafficMapGeneration != 0 ||
		apimeta.FindStatusCondition(trafficMap.Status.Conditions, v1beta1.TrafficMapPublished) != nil
}

// trafficMapBlocksSourceTeardown applies the deletion identity matrix. Exact
// owner identity or durable sourceUID provenance blocks teardown. A legacy map
// with neither remains ambiguous only when publisher-owned state shows that
// external publication may still exist.
func trafficMapBlocksSourceTeardown(
	trafficMap *v1beta1.TrafficMap,
	source *v1beta1.InferenceService,
) bool {
	if trafficMap == nil || source == nil || source.UID == "" {
		return true
	}
	if TrafficMapHasExactController(trafficMap, source) {
		return true
	}
	if trafficMap.Status.SourceUID == source.UID {
		return true
	}
	if TrafficMapHasController(trafficMap) || trafficMap.Status.SourceUID != "" {
		return false
	}
	return trafficMapHasPublicationEvidence(trafficMap)
}
