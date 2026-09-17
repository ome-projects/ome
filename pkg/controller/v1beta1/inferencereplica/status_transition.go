package inferencereplica

import (
	"context"
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
// transition gate rewrote a stored ColumnarV2 per-Instance status as DenseV1.
const EventReasonInstanceStatusConverted = "InstanceStatusConverted"

// convertStoredRepresentation is the transition gate for an object whose
// entry read decoded from ColumnarV2. This manager's write target is DenseV1,
// so the object is rewritten as DenseV1 once, with its logical rows unchanged
// and no lifecycle effect on this pass, and the reconcile is requeued so the
// next pass starts from the converged object. The write retries from an
// uncached decoded read on conflict; an object that was replaced or already
// converged since the entry read is left alone.
func (r *Reconciler) convertStoredRepresentation(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica) (ctrl.Result, error) {
	key := client.ObjectKeyFromObject(ir)
	writer := r.statusWriter()
	converted := false
	rows := 0
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		source, err := irstatus.GetDecoded(ctx, r.liveReader(), key, fresh)
		if err != nil {
			return err
		}
		if fresh.UID != ir.UID || source == irstatus.EncodingDenseV1 {
			return nil
		}
		rows = len(fresh.Status.InstanceStatuses)
		if err := convertInferenceReplicaStatus(ctx, writer, fresh, source); err != nil {
			return err
		}
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
		return ctrl.Result{}, fmt.Errorf("InferenceReplica %s/%s: rewrite stored status as %s: %w", ir.Namespace, ir.Name, irstatus.EncodingDenseV1, err)
	}
	if converted {
		log.Info("Rewrote the stored per-Instance status representation",
			"from", irstatus.EncodingColumnarV2, "to", irstatus.EncodingDenseV1, "instances", rows)
		if r.Recorder != nil {
			r.Recorder.Eventf(ir, corev1.EventTypeNormal, EventReasonInstanceStatusConverted,
				"InferenceReplica %s/%s per-Instance status was rewritten from %s to %s (%d Instances); lifecycle reconciliation resumes on the next pass",
				ir.Namespace, ir.Name, irstatus.EncodingColumnarV2, irstatus.EncodingDenseV1, rows)
		}
	}
	return ctrl.Result{Requeue: true}, nil
}
