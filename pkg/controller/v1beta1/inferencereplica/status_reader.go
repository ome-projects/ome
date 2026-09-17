package inferencereplica

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
)

// EventReasonInstanceStatusDecodeFailed is the Warning emitted when a stored
// per-Instance representation cannot be decoded. The reconcile stops before
// any effect; the message carries only the fixed-catalog reason.
const EventReasonInstanceStatusDecodeFailed = "InstanceStatusDecodeFailed"

// liveReader is the uncached reader paired with this reconciler's row
// decoder. Every conflict-retried status mutation re-reads through it so the
// fresh object is decoded under the configured bound before it is mutated.
func (r *Reconciler) liveReader() client.Reader {
	return irstatus.NewReader(r.APIReader, r.InstanceStatusDecoder)
}

// cachedReader is the informer-backed reader paired with the row decoder,
// used for the reconcile entry read.
func (r *Reconciler) cachedReader() client.Reader {
	return irstatus.NewReader(r.Client, r.InstanceStatusDecoder)
}

// instanceStatusDecodeError classifies a failed reconcile entry read: a
// codec failure is reported as a Warning on the object and returned wrapped
// so the reconcile fails closed; any other error is returned unchanged.
func (r *Reconciler) instanceStatusDecodeError(ir *v1beta1.InferenceReplica, err error) error {
	reason, ok := irstatus.ErrorReasonOf(err)
	if !ok {
		return err
	}
	obsmetrics.RecordIRStatusCodecError(string(reason))
	if r.Recorder != nil {
		r.Recorder.Eventf(ir, corev1.EventTypeWarning, EventReasonInstanceStatusDecodeFailed,
			"InferenceReplica %s/%s per-Instance status cannot be decoded (%s); reconciliation is suspended until the status is repaired",
			ir.Namespace, ir.Name, reason)
	}
	return fmt.Errorf("InferenceReplica %s/%s: decode per-Instance status: %w", ir.Namespace, ir.Name, err)
}
