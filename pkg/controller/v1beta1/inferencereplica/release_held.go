package inferencereplica

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// consumeReleaseHeldRequest is the operator release mailbox for Held
// RetryBlocks: the ome.io/release-held-revision annotation on the IR
// names a revision — full ControllerRevision name or bare revision
// hash — whose Held block the operator wants removed. It is the manual
// exit from the terminal Held state (one `kubectl annotate` instead of
// a status-subresource patch).
//
// Every present request is answered: a matching Held block is removed
// (status write through the MutateRetryBlock seam, which also mirrors
// the committed slice onto the in-memory IR) with a Normal
// RetryBlockReleased event; a non-Held match or an unknown revision is
// a no-op answered with a RetryBlockReleaseSkipped event. The block
// removal commits before acknowledgeRequest removes the annotation, so
// a crash between the two re-delivers into the no-op branch. Only the
// matched block is touched. Events land on the parent ISVC when
// resolvable (the user-facing stream), else the IR.
func (r *Reconciler) consumeReleaseHeldRequest(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService, retryBlockHistoryLimit *int32) (requeue bool, err error) {
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
			r.Recorder.Eventf(eventTarget, corev1.EventTypeNormal, string(workloadtypes.EventReasonRetryBlockReleaseSkipped),
				"InferenceReplica %s/%s component=%s: release requested for revision %q but no retryBlock exists for it; nothing to release",
				ir.Namespace, ir.Name, ir.Spec.Component, val)
		}
	case match.State != v1beta1.RetryBlockHeld:
		log.Info("RetryBlock release requested for a non-Held block; consuming as no-op",
			"revision", match.TargetRevision, "state", match.State)
		if r.Recorder != nil {
			r.Recorder.Eventf(eventTarget, corev1.EventTypeNormal, string(workloadtypes.EventReasonRetryBlockReleaseSkipped),
				"InferenceReplica %s/%s component=%s: retryBlock for revision %s is %s, not Held; nothing to release",
				ir.Namespace, ir.Name, ir.Spec.Component, match.TargetRevision, match.State)
		}
	default:
		rev := match.TargetRevision
		if rerr := buildMutateRetryBlock(r.statusWriter(), r.liveReader(), ir, retryBlockHistoryLimit)(ctx, rev, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
			return workloadtypes.RetryBlockRemove
		}); rerr != nil {
			// Annotation NOT consumed: the request re-drives the removal
			// next pass.
			return false, fmt.Errorf("release Held retry block (rev=%s): %w", rev, rerr)
		}
		log.Info("Held RetryBlock released at operator request", "revision", rev)
		if r.Recorder != nil {
			r.Recorder.Eventf(eventTarget, corev1.EventTypeNormal, string(workloadtypes.EventReasonRetryBlockReleased),
				"InferenceReplica %s/%s component=%s: Held retryBlock for revision %s removed at operator request (%s annotation)",
				ir.Namespace, ir.Name, ir.Spec.Component, rev, constants.ReleaseHeldRevisionAnnotationKey)
		}
	}

	key := client.ObjectKeyFromObject(ir)
	return r.acknowledgeRequest(ctx, ir, "release-held annotation", func(ctx context.Context) (client.Object, error) {
		fresh := &v1beta1.InferenceReplica{}
		return fresh, r.APIReader.Get(ctx, key, fresh)
	}, constants.ReleaseHeldRevisionAnnotationKey)
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
