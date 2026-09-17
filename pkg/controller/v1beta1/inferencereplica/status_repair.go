package inferencereplica

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// ErrInstanceStatusRepairRefused wraps every refusal of the break-glass
// repair entry point: an invalid or non-DenseV1 replacement, a replacement
// that carries anything besides rows, a missing precondition, or a live
// object that moved past the supplied resourceVersion.
var ErrInstanceStatusRepairRefused = errors.New("InferenceReplica status repair refused")

// InstanceStatusRepair is the operator-supplied request to replace the stored
// per-Instance representation of one InferenceReplica with a complete,
// independently validated DenseV1 row set. It is the only input the repair
// entry point takes: no lifecycle mutation can be expressed through it.
type InstanceStatusRepair struct {
	// Live is the object as read directly from the API server with its stored
	// representation untouched. Only its resourceVersion and the status
	// fields outside the per-Instance representation are used; the stored
	// per-Instance payload is never decoded or trusted.
	Live *v1beta1.InferenceReplica
	// ResourceVersion is the write precondition: the live object must still
	// carry it, otherwise the write is refused as a conflict and nothing
	// changes.
	ResourceVersion string
	// Replacement carries the complete DenseV1 row set in instanceStatuses
	// and nothing else; a marker, columns, or any other status field is
	// refused.
	Replacement *v1beta1.InferenceReplicaStatus
}

// InstanceStatusRepairOutcome describes what the repair would write or wrote.
type InstanceStatusRepairOutcome struct {
	// StoredEncoding is the representation the live object carries, or
	// "undecodable (<reason>)" when the codec refuses it.
	StoredEncoding string
	// StoredRows is the live row count, or -1 when it cannot be decoded.
	StoredRows int
	// StoredBytes is the serialized size of the live status as stored.
	StoredBytes int
	// ReplacementRows is the number of rows in the validated replacement.
	ReplacementRows int
	// SelectedEncoding and SelectedBytes describe the candidate the
	// configured target selects for the replacement.
	SelectedEncoding irstatus.Encoding
	SelectedBytes    int
	// Applied reports whether the write was performed.
	Applied bool
	// ResourceVersion is the object's resourceVersion after the write, or the
	// precondition when nothing was written.
	ResourceVersion string
}

// RepairInstanceStatus is the break-glass entry on the status writer
// boundary. It is not registered with any reconciler. It validates the
// replacement with the codec, installs the rows on a copy of the live object
// whose other status fields are preserved, selects the representation the
// configured target selects for any write, and, when apply is set, persists
// it once with the resourceVersion precondition and no conflict retry. When
// apply is false nothing is written and c is not used.
func RepairInstanceStatus(ctx context.Context, c client.Client, target irstatus.Encoding, bound uint64, repair InstanceStatusRepair, apply bool) (*InstanceStatusRepairOutcome, error) {
	if repair.Live == nil {
		return nil, fmt.Errorf("%w: the live object is required", ErrInstanceStatusRepairRefused)
	}
	if repair.ResourceVersion == "" {
		return nil, fmt.Errorf("%w: a live resourceVersion precondition is required", ErrInstanceStatusRepairRefused)
	}
	rows, err := validateRepairReplacement(repair.Replacement, bound)
	if err != nil {
		return nil, err
	}

	storedBytes, err := json.Marshal(&repair.Live.Status)
	if err != nil {
		return nil, fmt.Errorf("serialize stored status: %w", err)
	}
	outcome := &InstanceStatusRepairOutcome{
		StoredBytes:     len(storedBytes),
		ReplacementRows: len(rows),
		ResourceVersion: repair.ResourceVersion,
	}
	outcome.StoredEncoding, outcome.StoredRows = describeStoredRepresentation(&repair.Live.Status, bound)

	repaired := repair.Live.DeepCopy()
	repaired.ResourceVersion = repair.ResourceVersion
	repaired.Status.InstanceStatuses = rows
	repaired.Status.InstanceStatusEncoding = nil
	repaired.Status.InstanceStatusColumns = nil
	writer := statusWriter{Client: c, decoder: irstatus.NewDecoder(bound), target: target}
	candidate, err := writer.selectCandidate(repaired)
	if err != nil {
		return nil, err
	}
	outcome.SelectedEncoding, outcome.SelectedBytes = candidate.Encoding, candidate.Size
	if !apply {
		return outcome, nil
	}
	if c == nil {
		return nil, fmt.Errorf("%w: a client is required to apply", ErrInstanceStatusRepairRefused)
	}
	if err := persistInferenceReplicaStatus(ctx, writer, repaired, candidate); err != nil {
		if apierrors.IsConflict(err) {
			return nil, fmt.Errorf("%w: %s/%s changed since resourceVersion %s was read; repeat the repair from a fresh live read: %w",
				ErrInstanceStatusRepairRefused, repaired.Namespace, repaired.Name, repair.ResourceVersion, err)
		}
		return nil, err
	}
	outcome.Applied = true
	outcome.ResourceVersion = repaired.ResourceVersion
	return outcome, nil
}

// validateRepairReplacement accepts exactly one shape: a status whose only
// populated field is a nonempty DenseV1 instanceStatuses list within the
// configured row bound. Everything else is refused before any read or write.
func validateRepairReplacement(replacement *v1beta1.InferenceReplicaStatus, bound uint64) ([]v1beta1.OMENativeInstanceStatus, error) {
	if replacement == nil {
		return nil, fmt.Errorf("%w: a replacement is required", ErrInstanceStatusRepairRefused)
	}
	rows, encoding, err := irstatus.DecodeStatus(replacement, bound)
	if err != nil {
		return nil, fmt.Errorf("%w: the replacement is not a valid per-Instance representation: %w", ErrInstanceStatusRepairRefused, err)
	}
	if encoding != irstatus.EncodingDenseV1 {
		return nil, fmt.Errorf("%w: the replacement is stored as %s; the repair accepts a DenseV1 row set only", ErrInstanceStatusRepairRefused, encoding)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: the replacement carries no rows; the repair never clears the per-Instance representation", ErrInstanceStatusRepairRefused)
	}
	if bound > 0 && uint64(len(rows)) > bound {
		return nil, fmt.Errorf("%w: the replacement carries %d rows, above the configured maxDecodedInstances %d", ErrInstanceStatusRepairRefused, len(rows), bound)
	}
	rest := replacement.DeepCopy()
	rest.InstanceStatuses = nil
	if !reflect.DeepEqual(*rest, v1beta1.InferenceReplicaStatus{}) {
		return nil, fmt.Errorf("%w: the replacement must carry only instanceStatuses; every other status field is preserved from the live object", ErrInstanceStatusRepairRefused)
	}
	copied := make([]v1beta1.OMENativeInstanceStatus, len(rows))
	for i := range rows {
		rows[i].DeepCopyInto(&copied[i])
	}
	return copied, nil
}

// describeStoredRepresentation classifies the live representation for the
// outcome without letting a corrupt payload stop the repair.
func describeStoredRepresentation(status *v1beta1.InferenceReplicaStatus, bound uint64) (string, int) {
	rows, encoding, err := irstatus.DecodeStatus(status, bound)
	if err != nil {
		reason := err.Error()
		if codecReason, ok := irstatus.ErrorReasonOf(err); ok {
			reason = string(codecReason)
		}
		return "undecodable (" + reason + ")", -1
	}
	return string(encoding), len(rows)
}
