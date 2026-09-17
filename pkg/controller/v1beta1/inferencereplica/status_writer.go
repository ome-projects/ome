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
// the object was read from, or still carries, a ColumnarV2 representation.
// The transition gate rewrites a stored ColumnarV2 object as DenseV1 before
// any lifecycle pass runs, so a ColumnarV2 source reaching a mutation is an
// ownership violation rather than a conversion request.
var ErrColumnarV2StatusWrite = errors.New("InferenceReplica status write refused: the object is stored as ColumnarV2 and this manager writes DenseV1 only")

// ErrDenseV1StatusConversion reports a conversion requested for an object
// that is already stored as DenseV1; a representation-only write of a
// converged object is never performed.
var ErrDenseV1StatusConversion = errors.New("InferenceReplica status conversion refused: the object is already stored as DenseV1")

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
// API-confirmed object to the dense logical shape, and the recorder for
// write-rejection events. The manager refuses to start under any configured
// write target other than DenseV1, so the writer emits DenseV1 alone.
type statusWriter struct {
	client.Client
	decoder  irstatus.Decoder
	recorder record.EventRecorder
}

// statusWriter binds this reconciler's client, row decoder, and recorder into
// the persistence boundary every status mutation closure writes through.
func (r *Reconciler) statusWriter() statusWriter {
	return statusWriter{Client: r.Client, decoder: r.InstanceStatusDecoder, recorder: r.Recorder}
}

// updateInferenceReplicaStatus persists a logical status mutation. source is
// the encoding the object was decoded from and must be DenseV1. A conflict is
// returned unchanged so the caller re-reads and rebuilds the candidate from
// the fresh object.
func updateInferenceReplicaStatus(ctx context.Context, w statusWriter, ir *v1beta1.InferenceReplica, source irstatus.Encoding) error {
	if source != irstatus.EncodingDenseV1 {
		return fmt.Errorf("%s/%s (source %q): %w", ir.Namespace, ir.Name, source, ErrColumnarV2StatusWrite)
	}
	return persistInferenceReplicaStatus(ctx, w, ir)
}

// convertInferenceReplicaStatus is the transition gate's representation-only
// write: it persists an object decoded from ColumnarV2 as DenseV1 with its
// logical rows unchanged and counts the conversion once it is committed.
func convertInferenceReplicaStatus(ctx context.Context, w statusWriter, ir *v1beta1.InferenceReplica, source irstatus.Encoding) error {
	if source != irstatus.EncodingColumnarV2 {
		return fmt.Errorf("%s/%s (source %q): %w", ir.Namespace, ir.Name, source, ErrDenseV1StatusConversion)
	}
	if err := persistInferenceReplicaStatus(ctx, w, ir); err != nil {
		return err
	}
	obsmetrics.RecordIRStatusConversion(encodingLabel(source), obsmetrics.IRStatusEncodingDenseV1)
	return nil
}

// persistInferenceReplicaStatus is the single InferenceReplica status write.
// The status is normalized into the DenseV1 candidate by the codec, written,
// and the object the API server returned is decoded in place so mirrors and
// OnCommit callbacks observe the same logical shape a fresh read would.
func persistInferenceReplicaStatus(ctx context.Context, w statusWriter, ir *v1beta1.InferenceReplica) error {
	candidate, err := irstatus.DenseCandidate(&ir.Status)
	if err != nil {
		if reason, ok := irstatus.ErrorReasonOf(err); ok {
			obsmetrics.RecordIRStatusCodecError(string(reason))
		}
		return fmt.Errorf("%s/%s (object carries a ColumnarV2 marker or columns): %w: %w", ir.Namespace, ir.Name, ErrColumnarV2StatusWrite, err)
	}
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
