package coordination

import (
	"strconv"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// StatusObservedTemplates reports whether a replica's status was written for
// the pod templates its spec holds now, so the roll target it records
// (updateRevision) is the one the spec asks for. The projector writes the
// template hash on the spec in the same write as the templates and the
// replica repeats it in status once updateRevision was computed from them;
// the two agree exactly when the status describes the spec in hand. A spec
// write that changes no template (an annotation value the revision leaves
// out, the replica count, a pause, a partition, a rollback target) moves
// the generation but not the hash, so it leaves the status fresh. A
// replica with no template hash (one no projector writes) is read by its
// generation instead.
func StatusObservedTemplates(ir *v1beta1.InferenceReplica) bool {
	if ir == nil {
		return false
	}
	if hash := ir.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]; hash != "" {
		return ir.Status.ObservedTemplateHash == hash
	}
	return ir.Status.ObservedGeneration == ir.Generation
}

// SpecTrailsService reports whether a replica's spec was written from an
// older generation of its service than the one in hand. The projector
// writes the service's generation onto the replica on every pass. A
// written generation below the service's means the service changed and
// the change has not reached this replica: its roll target is the one
// the service had before. A replica that carries no such record, or one
// that does not parse, is read as current.
func SpecTrailsService(isvc *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) bool {
	if isvc == nil || ir == nil {
		return false
	}
	raw, ok := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	if !ok {
		return false
	}
	written, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return false
	}
	return written < isvc.Generation
}
