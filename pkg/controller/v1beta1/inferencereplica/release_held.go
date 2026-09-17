package inferencereplica

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// consumeReleaseHeldRequest is the operator release mailbox for Held
// RetryBlocks: the ome.io/release-held-revision annotation on the IR
// names a revision — full ControllerRevision name or bare revision
// hash — whose Held block the operator wants removed. It is the manual
// exit from the terminal Held state (one `kubectl annotate` instead of
// a status-subresource patch).
//
// Mailbox discipline mirrors consumeMigrationRequests: every present
// request is answered — a matching Held block is removed (status write
// through the MutateRetryBlock seam, which also mirrors the committed
// slice onto the in-memory IR) with a Normal RetryBlockReleased event;
// a non-Held match or an unknown revision is a no-op answered with a
// RetryBlockReleaseSkipped event — and the annotation is then deleted
// (consume = ack) against a fresh IR read under conflict retry, with
// the deletion mirrored onto the caller's in-memory IR. Write order:
// the block removal commits BEFORE the annotation delete, so a crash
// between the two re-delivers into the no-op branch and the annotation
// is cleaned up idempotently. Only the matched block is touched.
//
// Exhausted conflict retries on the annotation delete return
// requeue=true; the request re-answers next pass. Events land on the
// parent ISVC when resolvable (the user-facing stream), else the IR.
func (r *Reconciler) consumeReleaseHeldRequest(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService) (requeue bool, err error) {
	val, present := ir.Annotations[constants.ReleaseHeldRevisionAnnotationKey]
	if !present {
		return false, nil
	}
	eventTarget := client.Object(ir)
	if parent != nil {
		eventTarget = parent
	}

	match := findRetryBlockByRevisionOrHash(ir.Status.RetryBlocks, val)
	switch {
	case match == nil:
		log.Info("RetryBlock release requested for a revision with no block; consuming as no-op",
			"revision", val)
		if r.Recorder != nil {
			r.Recorder.Eventf(eventTarget, corev1.EventTypeNormal, string(workload.EventReasonRetryBlockReleaseSkipped),
				"InferenceReplica %s/%s component=%s: release requested for revision %q but no retryBlock exists for it; nothing to release",
				ir.Namespace, ir.Name, ir.Spec.Component, val)
		}
	case match.State != v1beta1.RetryBlockHeld:
		log.Info("RetryBlock release requested for a non-Held block; consuming as no-op",
			"revision", match.TargetRevision, "state", match.State)
		if r.Recorder != nil {
			r.Recorder.Eventf(eventTarget, corev1.EventTypeNormal, string(workload.EventReasonRetryBlockReleaseSkipped),
				"InferenceReplica %s/%s component=%s: retryBlock for revision %s is %s, not Held; nothing to release",
				ir.Namespace, ir.Name, ir.Spec.Component, match.TargetRevision, match.State)
		}
	default:
		rev := match.TargetRevision
		if rerr := buildMutateRetryBlock(r.statusWriter(), r.liveReader(), ir)(ctx, rev, func(*workload.RetryBlock) workload.RetryBlockDisposition {
			return workload.RetryBlockRemove
		}); rerr != nil {
			// Annotation NOT consumed: the request re-drives the removal
			// next pass.
			return false, fmt.Errorf("release Held retry block (rev=%s): %w", rev, rerr)
		}
		log.Info("Held RetryBlock released at operator request", "revision", rev)
		if r.Recorder != nil {
			r.Recorder.Eventf(eventTarget, corev1.EventTypeNormal, string(workload.EventReasonRetryBlockReleased),
				"InferenceReplica %s/%s component=%s: Held retryBlock for revision %s removed at operator request (%s annotation)",
				ir.Namespace, ir.Name, ir.Spec.Component, rev, constants.ReleaseHeldRevisionAnnotationKey)
		}
	}

	// Consume the annotation against a FRESH IR read under conflict
	// retry — same discipline as the migration mailbox delete.
	key := client.ObjectKeyFromObject(ir)
	uerr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.APIReader.Get(ctx, key, fresh); err != nil {
			return err
		}
		if _, ok := fresh.Annotations[constants.ReleaseHeldRevisionAnnotationKey]; !ok {
			return nil
		}
		delete(fresh.Annotations, constants.ReleaseHeldRevisionAnnotationKey)
		return r.Update(ctx, fresh)
	})
	if uerr != nil {
		if apierrors.IsConflict(uerr) {
			// Retries exhausted on a hot IR; the request re-answers next
			// pass (every branch above is idempotent on re-delivery).
			return true, nil
		}
		if apierrors.IsNotFound(uerr) {
			return false, nil
		}
		return false, fmt.Errorf("delete consumed release-held annotation: %w", uerr)
	}
	// Mirror the deletion onto the caller's in-memory IR so this pass
	// observes the consumed mailbox.
	delete(ir.Annotations, constants.ReleaseHeldRevisionAnnotationKey)
	return false, nil
}

// findRetryBlockByRevisionOrHash returns the RetryBlock whose
// TargetRevision equals val (full ControllerRevision name) or whose
// revision hash — the trailing name segment, via query.RevisionFromName
// — equals val (bare hash). Empty val matches nothing.
func findRetryBlockByRevisionOrHash(blocks []v1beta1.RetryBlock, val string) *v1beta1.RetryBlock {
	if val == "" {
		return nil
	}
	for i := range blocks {
		if blocks[i].TargetRevision == val || query.RevisionFromName(blocks[i].TargetRevision).Hash() == val {
			return &blocks[i]
		}
	}
	return nil
}
