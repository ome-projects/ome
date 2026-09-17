// Package irstatus is the codec for the per-Instance representation union of
// InferenceReplica status: the dense instanceStatuses list (DenseV1) and the
// grouped instanceStatusColumns payload (ColumnarV2). Both decode to the same
// logical rows in the same order; every failure carries a fixed-catalog
// reason and never returns a partial row set.
package irstatus

import "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"

// Encoding identifies the representation that carries the per-Instance rows
// of an InferenceReplica status.
type Encoding string

const (
	// EncodingDenseV1 is the dense instanceStatuses list, marked by an absent
	// instanceStatusEncoding.
	EncodingDenseV1 Encoding = "DenseV1"
	// EncodingColumnarV2 is the instanceStatusColumns payload under the
	// ColumnarV2 marker.
	EncodingColumnarV2 Encoding = "ColumnarV2"
)

// ObservedEncoding reports which representation status carries. It fails
// closed on an unknown marker and on every mixed spelling of the union: an
// unmarked status with columns, or a ColumnarV2 status without columns or
// with dense rows. A nil status is an empty DenseV1 status.
func ObservedEncoding(status *v1beta1.InferenceReplicaStatus) (Encoding, error) {
	if status == nil {
		return EncodingDenseV1, nil
	}
	if status.InstanceStatusEncoding == nil {
		if status.InstanceStatusColumns != nil {
			return "", newCodecError(ErrorReasonRepresentationUnion)
		}
		return EncodingDenseV1, nil
	}
	switch *status.InstanceStatusEncoding {
	case v1beta1.InstanceStatusEncodingColumnarV2:
		if status.InstanceStatusColumns == nil || len(status.InstanceStatuses) != 0 {
			return "", newCodecError(ErrorReasonRepresentationUnion)
		}
		return EncodingColumnarV2, nil
	default:
		return "", newCodecError(ErrorReasonUnknownEncoding)
	}
}

// DecodeStatus returns the logical rows of status in dense order together
// with the representation they were read from. DenseV1 rows are the stored
// slice itself and receive no new cardinality check; ColumnarV2 rows are
// validated and expanded under maxInstances into a fresh slice. On error no
// rows are returned.
func DecodeStatus(status *v1beta1.InferenceReplicaStatus, maxInstances uint64) ([]v1beta1.OMENativeInstanceStatus, Encoding, error) {
	encoding, err := ObservedEncoding(status)
	if err != nil {
		return nil, "", err
	}
	if encoding == EncodingDenseV1 {
		if status == nil {
			return nil, EncodingDenseV1, nil
		}
		return status.InstanceStatuses, EncodingDenseV1, nil
	}
	rows, err := DecodeColumns(status.InstanceStatusColumns, maxInstances)
	if err != nil {
		return nil, "", err
	}
	return rows, EncodingColumnarV2, nil
}

// ClearPodDerivedObservations zeroes the three row fields that DenseV1 does
// not persist because workload decisions derive them from current Pods.
// Neither representation stores them, so a write copy clears them before
// either candidate is built.
func ClearPodDerivedObservations(rows []v1beta1.OMENativeInstanceStatus) {
	for i := range rows {
		rows[i].ReadyPodCount = 0
		rows[i].ScheduledPodCount = 0
		rows[i].NodesOccupied = nil
	}
}

// supportedPhase reports whether phase is one of the Instance phases the
// column schema enumerates.
func supportedPhase(phase v1beta1.OMENativeInstancePhase) bool {
	switch phase {
	case v1beta1.OMENativeInstancePending, v1beta1.OMENativeInstanceCreating, v1beta1.OMENativeInstanceReady,
		v1beta1.OMENativeInstanceUpdating, v1beta1.OMENativeInstanceRestarting, v1beta1.OMENativeInstanceMigrating,
		v1beta1.OMENativeInstanceFailed, v1beta1.OMENativeInstanceDeleting:
		return true
	default:
		return false
	}
}

// entryHasContent reports whether an entry carries at least one exceptional
// record. Reused nested records keep their DenseV1 normalization, so an
// explicitly empty conditions list counts as absent.
func entryHasContent(entry *v1beta1.InstanceStatusEntry) bool {
	return len(entry.Conditions) > 0 || entry.ReadySince != nil || entry.Operation != nil || entry.LastFailure != nil
}
