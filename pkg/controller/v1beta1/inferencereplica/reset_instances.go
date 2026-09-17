package inferencereplica

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// resetInstancesRequest is the parsed ome.io/reset-instances value:
// every Failed Instance, or an explicit index list in request order.
type resetInstancesRequest struct {
	all     bool
	indices []int32
}

// resetSkip records one requested Instance the reset left alone and why.
type resetSkip struct {
	index  int32
	reason string
}

// consumeResetInstancesRequest is the operator rebuild mailbox for
// Failed Instances: the ome.io/reset-instances annotation on the IR
// names the Instances — "all" or a comma-separated index list — whose
// Failed materialization the operator wants torn down and rebuilt. It
// is the manual exit for an Instance parked at Phase=Failed behind a
// preserved repair attempt (a deadline-expired Restart or Create): its
// pods are still present, so no Create/Restart trigger ever fires, and
// the only other way out is a pod-template edit that rolls every
// Instance.
//
// Two guards keep the verb from doing the rollout machinery's work. An
// Instance any of whose live pods is still in the serving rotation
// (podreadiness.IsServing — a gang-surge source whose replacement failed
// keeps serving on its old pods) is skipped untouched: draining a
// serving source is the rollout machinery's job, not this verb's. And
// only repair-owned parked attempts are in scope (no Operation, Create,
// Restart — workload.ResetOwnsOperation): an Instance parked behind an
// Update or Migrate continuation is skipped, because the gang abandon,
// wreckage cleanup, release-held, and migration-expiry paths own those
// continuations and clearing one here would orphan its surge marker or
// migration record.
//
// Mailbox discipline mirrors consumeReleaseHeldRequest: every present
// request is answered. A candidate has every pod deleted (live list,
// expectations recorded before each delete) and its Operation cleared
// through the MutateInstance seam — Phase stays Failed and LastFailure
// is preserved, which is exactly the fresh-start shape the Create pass
// rebuilds (Failed with no Operation and no pods). A target that is not
// Failed, does not exist, is guarded as above, or has nothing left to
// tear down is skipped; a malformed value resets nothing. Each outcome
// gets one event, then the annotation is deleted (consume = ack) against
// a fresh IR read under conflict retry, with the deletion mirrored onto
// the caller's in-memory IR. Write order: pod deletes and the status
// write commit BEFORE the annotation delete, so a crash between the two
// re-delivers into the nothing-to-reset branch and the annotation is
// cleaned up idempotently.
//
// A pod delete or status write error leaves the annotation in place and
// returns the error; the request re-drives next pass. Exhausted conflict
// retries on the annotation delete return requeue=true. Events land on
// the parent ISVC when resolvable (the user-facing stream), else the IR.
func (r *Reconciler) consumeResetInstancesRequest(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, parent *v1beta1.InferenceService) (requeue bool, err error) {
	val, present := ir.Annotations[constants.ResetInstancesAnnotationKey]
	if !present {
		return false, nil
	}
	eventTarget := client.Object(ir)
	if parent != nil {
		eventTarget = parent
	}

	req, perr := parseResetInstancesValue(val)
	if perr != nil {
		log.Info("Instance reset requested with a malformed value; consuming as no-op",
			"value", val, "error", perr.Error())
		if r.Recorder != nil {
			r.Recorder.Eventf(eventTarget, corev1.EventTypeWarning, string(workload.EventReasonInstancesResetRejected),
				"InferenceReplica %s/%s component=%s: %s=%q rejected: %v; nothing reset",
				ir.Namespace, ir.Name, ir.Spec.Component, constants.ResetInstancesAnnotationKey, val, perr)
		}
	} else {
		reset, skipped, rerr := r.resetInstances(ctx, log, ir, req)
		if rerr != nil {
			// Annotation NOT consumed: the request re-drives the reset
			// next pass; every step above is idempotent on re-delivery.
			return false, fmt.Errorf("reset instances (%s=%q): %w", constants.ResetInstancesAnnotationKey, val, rerr)
		}
		if r.Recorder != nil {
			if len(reset) > 0 {
				r.Recorder.Eventf(eventTarget, corev1.EventTypeNormal, string(workload.EventReasonInstancesReset),
					"InferenceReplica %s/%s component=%s: reset instance(s) %s at operator request (%s annotation): pods deleted and preserved operation cleared; the lifecycle passes rebuild them",
					ir.Namespace, ir.Name, ir.Spec.Component, formatIndices(reset), constants.ResetInstancesAnnotationKey)
			}
			switch {
			case len(skipped) > 0:
				r.Recorder.Eventf(eventTarget, corev1.EventTypeNormal, string(workload.EventReasonInstancesResetSkipped),
					"InferenceReplica %s/%s component=%s: reset skipped for instance(s) %s",
					ir.Namespace, ir.Name, ir.Spec.Component, formatSkips(skipped))
			case len(reset) == 0:
				r.Recorder.Eventf(eventTarget, corev1.EventTypeNormal, string(workload.EventReasonInstancesResetSkipped),
					"InferenceReplica %s/%s component=%s: reset requested for all instances but none is Failed; nothing to reset",
					ir.Namespace, ir.Name, ir.Spec.Component)
			}
		}
	}

	// Consume the annotation against a FRESH IR read under conflict
	// retry — same discipline as the release-held mailbox delete.
	key := client.ObjectKeyFromObject(ir)
	uerr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &v1beta1.InferenceReplica{}
		if err := r.APIReader.Get(ctx, key, fresh); err != nil {
			return err
		}
		if _, ok := fresh.Annotations[constants.ResetInstancesAnnotationKey]; !ok {
			return nil
		}
		delete(fresh.Annotations, constants.ResetInstancesAnnotationKey)
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
		return false, fmt.Errorf("delete consumed reset-instances annotation: %w", uerr)
	}
	// Mirror the deletion onto the caller's in-memory IR so this pass
	// observes the consumed mailbox.
	delete(ir.Annotations, constants.ResetInstancesAnnotationKey)
	return false, nil
}

// resetInstances tears down every requested Instance that is Failed,
// repair-owned, and out of the serving rotation, and reports the indices
// reset plus the requested ones it skipped. Per candidate: live-list its
// pods, skip it untouched if any pod is still serving, otherwise delete
// each pod not already Terminating (ExpectDeletes before the Delete,
// ObservedDelete on error, NotFound tolerated), then clear the preserved
// Operation through MutateInstance, which mirrors the committed status
// onto the caller's IR. Phase and LastFailure are untouched. An Instance
// with no pod to delete and no Operation to clear is already in the
// rebuild shape and is skipped — the branch a re-delivered request lands
// in. Stops at the first error; Instances already handled stay
// consistent because every step is idempotent.
func (r *Reconciler) resetInstances(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, req resetInstancesRequest) (reset []int32, skipped []resetSkip, err error) {
	statuses := make(map[int32]*v1beta1.OMENativeInstanceStatus, len(ir.Status.InstanceStatuses))
	for i := range ir.Status.InstanceStatuses {
		statuses[ir.Status.InstanceStatuses[i].Index] = &ir.Status.InstanceStatuses[i]
	}

	// classify admits a Failed Instance whose parked attempt the reset
	// owns; a continuation owned by the rollout or migration machinery is
	// skipped with its owner named.
	var candidates []int32
	classify := func(idx int32, s *v1beta1.OMENativeInstanceStatus) {
		if !workload.ResetOwnsOperation(v1beta1convert.InstanceOperationToWorkload(s.Operation)) {
			skipped = append(skipped, resetSkip{index: idx, reason: "owned by " + string(s.Operation.Type)})
			return
		}
		candidates = append(candidates, idx)
	}
	if req.all {
		failed := make([]int32, 0, len(statuses))
		for idx, s := range statuses {
			if s.Phase == v1beta1.OMENativeInstanceFailed {
				failed = append(failed, idx)
			}
		}
		sort.Slice(failed, func(i, j int) bool { return failed[i] < failed[j] })
		for _, idx := range failed {
			classify(idx, statuses[idx])
		}
	} else {
		for _, idx := range req.indices {
			s, ok := statuses[idx]
			switch {
			case !ok:
				skipped = append(skipped, resetSkip{index: idx, reason: "no such instance"})
			case s.Phase != v1beta1.OMENativeInstanceFailed:
				skipped = append(skipped, resetSkip{index: idx, reason: "Phase=" + string(s.Phase)})
			default:
				classify(idx, s)
			}
		}
	}
	if len(candidates) == 0 {
		return nil, skipped, nil
	}

	key := buildKey(ir)
	expectations := r.Expectations
	if expectations == nil {
		expectations = workload.DefaultExpectations
	}
	mutateInstance := buildMutateInstance(r.Client, r.APIReader, ir)
	for _, idx := range candidates {
		// Live list: a stale cache view of "pods gone" must not skip a
		// pod the apiserver still holds.
		pods, lerr := query.LiveListPodsForInstance(ctx, r.APIReader, key.Namespace, key.OwnerName, key.Component, idx)
		if lerr != nil {
			return reset, skipped, fmt.Errorf("list pods (instance=%d): %w", idx, lerr)
		}
		// Serving capacity is never removed here: a Failed Instance still
		// in rotation is the rollout machinery's to drain.
		if anyPodServing(pods) {
			skipped = append(skipped, resetSkip{index: idx, reason: "still serving"})
			continue
		}
		deleted := 0
		for _, pod := range pods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			expectations.ExpectDeletes(key.Namespace, key.OwnerName, key.Component, idx, 1)
			if derr := r.Delete(ctx, pod); derr != nil {
				expectations.ObservedDelete(key.Namespace, key.OwnerName, key.Component, idx)
				if apierrors.IsNotFound(derr) {
					continue
				}
				return reset, skipped, fmt.Errorf("delete pod %s/%s (instance=%d): %w", pod.Namespace, pod.Name, idx, derr)
			}
			deleted++
		}

		// The transition write itself belongs to the workload layer; the
		// seam re-checks Phase=Failed and the Operation owner on the
		// fresh read.
		cleared, merr := workload.ClearFailedInstanceOperation(ctx, mutateInstance, idx)
		if merr != nil {
			return reset, skipped, fmt.Errorf("clear preserved operation (instance=%d): %w", idx, merr)
		}

		if deleted == 0 && !cleared {
			skipped = append(skipped, resetSkip{index: idx, reason: "nothing to reset"})
			continue
		}
		log.Info("Failed Instance reset at operator request",
			"instance", idx, "podsDeleted", deleted, "operationCleared", cleared)
		reset = append(reset, idx)
	}
	return reset, skipped, nil
}

// anyPodServing reports whether any pod is in the serving rotation
// (ome.io/serving=True), the same predicate the Restart drain reads.
func anyPodServing(pods []*corev1.Pod) bool {
	for _, pod := range pods {
		if podreadiness.IsServing(pod) {
			return true
		}
	}
	return false
}

// parseResetInstancesValue parses the annotation value: "all", or a
// comma-separated list of non-negative Instance indices (surrounding
// whitespace tolerated, duplicates collapsed in first-seen order).
// Anything else — including an empty value or an empty list entry — is
// malformed.
func parseResetInstancesValue(val string) (resetInstancesRequest, error) {
	trimmed := strings.TrimSpace(val)
	if trimmed == constants.ResetInstancesAll {
		return resetInstancesRequest{all: true}, nil
	}
	if trimmed == "" {
		return resetInstancesRequest{}, fmt.Errorf("expected %q or a comma-separated list of instance indices", constants.ResetInstancesAll)
	}
	seen := make(map[int32]struct{})
	var indices []int32
	for _, part := range strings.Split(trimmed, ",") {
		part = strings.TrimSpace(part)
		n, err := strconv.ParseInt(part, 10, 32)
		if err != nil || n < 0 {
			return resetInstancesRequest{}, fmt.Errorf("%q is not a non-negative instance index", part)
		}
		idx := int32(n)
		if _, dup := seen[idx]; dup {
			continue
		}
		seen[idx] = struct{}{}
		indices = append(indices, idx)
	}
	return resetInstancesRequest{indices: indices}, nil
}

// formatIndices renders indices as a comma-separated list ("13,14").
func formatIndices(indices []int32) string {
	parts := make([]string, len(indices))
	for i, idx := range indices {
		parts[i] = strconv.FormatInt(int64(idx), 10)
	}
	return strings.Join(parts, ",")
}

// formatSkips renders skipped indices with their reasons
// ("7 (Phase=Ready), 99 (no such instance)").
func formatSkips(skips []resetSkip) string {
	parts := make([]string, len(skips))
	for i, s := range skips {
		parts[i] = fmt.Sprintf("%d (%s)", s.index, s.reason)
	}
	return strings.Join(parts, ", ")
}
