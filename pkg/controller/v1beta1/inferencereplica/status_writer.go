package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
)

// ErrColumnarV2StatusWrite reports a logical status mutation refused because
// the object still carries a ColumnarV2 representation the writer does not
// accept: a write copy that was never decoded into the logical shape, or,
// under a DenseV1 target, an object read from ColumnarV2. The transition gate
// rewrites such an object before any lifecycle pass runs, so a ColumnarV2
// source reaching a DenseV1-target mutation is an ownership violation rather
// than a conversion request.
var ErrColumnarV2StatusWrite = errors.New("InferenceReplica status write refused: the object carries a stored ColumnarV2 representation that must be converted or decoded before it is mutated")

// ErrStatusConversionConverged reports a conversion requested for an object
// whose stored representation is already the one the configured target
// selects; a representation-only write of a converged object is never
// performed.
var ErrStatusConversionConverged = errors.New("InferenceReplica status conversion refused: the stored representation is already the selected one")

// ErrInstanceStatusTargetUnset reports a writer built without a write target.
// The target is loaded from the omenativeStatus configuration at startup and
// every construction sets it; the writer never assumes one.
var ErrInstanceStatusTargetUnset = errors.New("InferenceReplica status write refused: no per-Instance status write target is configured")

// EventReasonStatusSizeExceeded is the Warning emitted when the API server
// rejects an InferenceReplica status write as too large. No lifecycle
// transition can be persisted for that IR until its status shrinks.
const EventReasonStatusSizeExceeded = "StatusSizeExceeded"

// tooLargeInternalErrorMarker is the substring the storage backend puts in the
// Internal error it returns for an oversized request; the API server does not
// translate it into a 413.
const tooLargeInternalErrorMarker = "request is too large"

// statusWriter is the single InferenceReplica status persistence boundary:
// the client that performs the status write, the decoder that returns the
// API-confirmed object to the dense logical shape, the configured write
// target that selects the representation of every write, and the recorder
// for write-rejection events.
type statusWriter struct {
	client.Client
	decoder  irstatus.Decoder
	target   irstatus.Encoding
	recorder record.EventRecorder
}

// statusWriter binds this reconciler's client, row decoder, write target, and
// recorder into the persistence boundary every status mutation closure
// writes through.
func (r *Reconciler) statusWriter() statusWriter {
	return statusWriter{Client: r.Client, decoder: r.InstanceStatusDecoder, target: r.InstanceStatusTarget, recorder: r.Recorder}
}

// updateInferenceReplicaStatus persists a logical status mutation. source is
// the encoding the object was decoded from. Under a DenseV1 target it must be
// DenseV1; under a ColumnarV2 target either source is accepted and the write
// may switch the stored representation as part of the mutation. A conflict
// is returned unchanged so the caller re-reads and rebuilds the candidate
// from the fresh object.
func updateInferenceReplicaStatus(ctx context.Context, w statusWriter, ir *v1beta1.InferenceReplica, source irstatus.Encoding) error {
	if w.target == irstatus.EncodingDenseV1 && source == irstatus.EncodingColumnarV2 {
		return fmt.Errorf("%s/%s (source %q, target %q): %w", ir.Namespace, ir.Name, source, w.target, ErrColumnarV2StatusWrite)
	}
	candidate, err := w.selectCandidate(ir)
	if err != nil {
		return err
	}
	return persistInferenceReplicaStatus(ctx, w, ir, candidate)
}

// convertInferenceReplicaStatus is the transition gate's representation-only
// write: it persists an object decoded from source in the representation the
// configured target selects, with its logical rows unchanged, and counts the
// conversion once it is committed. An object whose selected representation
// equals source is refused with ErrStatusConversionConverged before any
// request.
func convertInferenceReplicaStatus(ctx context.Context, w statusWriter, ir *v1beta1.InferenceReplica, source irstatus.Encoding) error {
	candidate, err := w.selectCandidate(ir)
	if err != nil {
		return err
	}
	if candidate.Encoding == source {
		return fmt.Errorf("%s/%s (stored as %s): %w", ir.Namespace, ir.Name, source, ErrStatusConversionConverged)
	}
	if err := persistInferenceReplicaStatus(ctx, w, ir, candidate); err != nil {
		return err
	}
	obsmetrics.RecordIRStatusConversion(encodingLabel(source), encodingLabel(candidate.Encoding))
	return nil
}

// selectCandidate builds the write candidate for the logical status of ir
// under the configured target and installs the selected representation on
// ir. A DenseV1 target normalizes the dense list in place; a ColumnarV2
// target builds both complete candidates and keeps columns only when they
// are strictly smaller. ir must be in the decoded logical shape.
func (w statusWriter) selectCandidate(ir *v1beta1.InferenceReplica) (*irstatus.Candidate, error) {
	switch w.target {
	case irstatus.EncodingDenseV1:
		candidate, err := irstatus.DenseCandidate(&ir.Status)
		if err != nil {
			return nil, undecodedWriteError(ir, err)
		}
		return candidate, nil
	case irstatus.EncodingColumnarV2:
		selection, err := irstatus.SelectCandidate(&ir.Status, w.decoder.MaxDecodedInstances())
		if err != nil {
			return nil, undecodedWriteError(ir, err)
		}
		candidate := selection.SelectedCandidate()
		ir.Status = *candidate.Status
		return candidate, nil
	default:
		return nil, fmt.Errorf("%s/%s (target %q): %w", ir.Namespace, ir.Name, w.target, ErrInstanceStatusTargetUnset)
	}
}

// undecodedWriteError classifies a candidate build failure: the write copy
// was not in the logical shape, which only happens when an object bypassed
// the decode boundary.
func undecodedWriteError(ir *v1beta1.InferenceReplica, err error) error {
	if reason, ok := irstatus.ErrorReasonOf(err); ok {
		obsmetrics.RecordIRStatusCodecError(string(reason))
	}
	return fmt.Errorf("%s/%s (object carries a ColumnarV2 marker or columns): %w: %w", ir.Namespace, ir.Name, ErrColumnarV2StatusWrite, err)
}

// persistInferenceReplicaStatus is the single InferenceReplica status write.
// ir carries exactly the representation of candidate; it is written, and the
// object the API server returned is decoded in place so mirrors and OnCommit
// callbacks observe the same logical shape a fresh read would. The bytes
// gauge and write counters carry the encoding that was written.
func persistInferenceReplicaStatus(ctx context.Context, w statusWriter, ir *v1beta1.InferenceReplica, candidate *irstatus.Candidate) error {
	encoding := encodingLabel(candidate.Encoding)
	obsmetrics.SetIRStatusBytes(ir.Namespace, ir.Name, string(ir.Spec.Component), encoding, candidate.Size)
	obsmetrics.RecordIRStatusWrite(encoding, obsmetrics.IRStatusWriteAttempt)

	if err := w.Status().Update(ctx, ir); err != nil {
		switch {
		case apierrors.IsConflict(err):
			obsmetrics.RecordIRStatusWrite(encoding, obsmetrics.IRStatusWriteConflict)
		case isStatusTooLarge(err):
			obsmetrics.RecordIRStatusWrite(encoding, obsmetrics.IRStatusWriteRejected)
			w.warnStatusSizeExceeded(ctx, ir, candidate)
		default:
			obsmetrics.RecordIRStatusWrite(encoding, obsmetrics.IRStatusWriteError)
		}
		return err
	}
	obsmetrics.RecordIRStatusWrite(encoding, obsmetrics.IRStatusWriteCommitted)

	if _, err := w.decoder.Decode(ir); err != nil {
		if reason, ok := irstatus.ErrorReasonOf(err); ok {
			obsmetrics.RecordIRStatusCodecError(string(reason))
		}
		return fmt.Errorf("%s/%s: decode committed status: %w", ir.Namespace, ir.Name, err)
	}
	return nil
}

// isStatusTooLarge reports whether the API server refused a write for its
// size: an explicit 413, or the storage backend's oversized-request failure
// surfaced as an Internal error.
func isStatusTooLarge(err error) bool {
	if apierrors.IsRequestEntityTooLargeError(err) {
		return true
	}
	return apierrors.IsInternalError(err) && strings.Contains(err.Error(), tooLargeInternalErrorMarker)
}

// warnStatusSizeExceeded surfaces a size rejection where operators look
// first: on the parent InferenceService when it can be resolved, otherwise on
// the IR itself. The message names the encoded size, never the payload.
func (w statusWriter) warnStatusSizeExceeded(ctx context.Context, ir *v1beta1.InferenceReplica, candidate *irstatus.Candidate) {
	if w.recorder == nil {
		return
	}
	var target client.Object = ir
	if w.Client != nil && ir.Spec.ParentRef.Name != "" {
		parent := &v1beta1.InferenceService{}
		if err := w.Get(ctx, types.NamespacedName{Namespace: ir.Namespace, Name: ir.Spec.ParentRef.Name}, parent); err == nil {
			target = parent
		}
	}
	w.recorder.Eventf(target, corev1.EventTypeWarning, EventReasonStatusSizeExceeded,
		"InferenceReplica %s/%s status write of %d bytes (%s) was rejected by the API server as too large; no lifecycle transition can be persisted until the status shrinks",
		ir.Namespace, ir.Name, candidate.Size, candidate.Encoding)
}

// encodingLabel maps a codec encoding onto the fixed metric label vocabulary.
func encodingLabel(encoding irstatus.Encoding) string {
	switch encoding {
	case irstatus.EncodingDenseV1:
		return obsmetrics.IRStatusEncodingDenseV1
	case irstatus.EncodingColumnarV2:
		return obsmetrics.IRStatusEncodingColumnarV2
	default:
		return ""
	}
}
