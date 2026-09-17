package inferencereplica

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// EventReasonInstanceStatusConverted is the Normal event emitted when the
// transition gate rewrote a stored per-Instance status representation.
const EventReasonInstanceStatusConverted = "InstanceStatusConverted"

// reconcileStoredRepresentation is the transition gate for an object whose
// entry read decoded from a representation other than the configured write
// target. It reports whether the gate owns this pass: when it does, the
// caller returns its result without any lifecycle effect.
//
// ColumnarV2 under a DenseV1 target always converts. DenseV1 under a
// ColumnarV2 target builds both candidates from the decoded object and
// converts only when columns are strictly smaller; otherwise nothing is
// written, no conversion is counted, and the pass continues on the dense
// object. Equal logical input always selects the same representation, so
// an unchanged object never alternates.
func (r *Reconciler) reconcileStoredRepresentation(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, source irstatus.Encoding) (bool, ctrl.Result, error) {
	if source == r.InstanceStatusTarget {
		return false, ctrl.Result{}, nil
	}
	if r.InstanceStatusTarget == irstatus.EncodingColumnarV2 && source == irstatus.EncodingDenseV1 {
		selection, err := irstatus.SelectCandidate(&ir.Status, r.InstanceStatusDecoder.MaxDecodedInstances())
		if err != nil {
			return true, ctrl.Result{}, r.instanceStatusDecodeError(ir, err)
		}
		if selection.Selected == irstatus.EncodingDenseV1 {
			return false, ctrl.Result{}, nil
		}
	}
	result, err := r.convertStoredRepresentation(ctx, log, ir, source)
	return true, result, err
}

// convertStoredRepresentation rewrites the stored representation of ir once,
// with its logical rows unchanged and no lifecycle effect on this pass, and
// requeues so the next pass starts from the converged object. The write
// retries from an uncached decoded read on conflict; an object that was
// replaced, already converged, or whose fresh selection no longer differs
// from its stored representation is left alone.
func (r *Reconciler) convertStoredRepresentation(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, source irstatus.Encoding) (ctrl.Result, error) {
	key := client.ObjectKeyFromObject(ir)
	writer := r.statusWriter()
	converted := false
	rows := 0
	var to irstatus.Encoding
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		freshSource, err := irstatus.GetDecoded(ctx, r.liveReader(), key, fresh)
		if err != nil {
			return err
		}
		if fresh.UID != ir.UID || freshSource == r.InstanceStatusTarget {
			return nil
		}
		rows = len(fresh.Status.InstanceStatuses)
		if err := convertInferenceReplicaStatus(ctx, writer, fresh, freshSource); err != nil {
			if errors.Is(err, ErrStatusConversionConverged) {
				return nil
			}
			return err
		}
		source, to = freshSource, r.InstanceStatusTarget
		converted = true
		return nil
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		if _, codec := irstatus.ErrorReasonOf(err); codec {
			return ctrl.Result{}, r.instanceStatusDecodeError(ir, err)
		}
		return ctrl.Result{}, fmt.Errorf("InferenceReplica %s/%s: rewrite stored status as %s: %w", ir.Namespace, ir.Name, r.InstanceStatusTarget, err)
	}
	if converted {
		log.Info("Rewrote the stored per-Instance status representation", "from", source, "to", to, "instances", rows)
		if r.Recorder != nil {
			r.Recorder.Eventf(ir, corev1.EventTypeNormal, EventReasonInstanceStatusConverted,
				"InferenceReplica %s/%s per-Instance status was rewritten from %s to %s (%d Instances); lifecycle reconciliation resumes on the next pass",
				ir.Namespace, ir.Name, source, to, rows)
		}
	}
	return ctrl.Result{Requeue: true}, nil
}
