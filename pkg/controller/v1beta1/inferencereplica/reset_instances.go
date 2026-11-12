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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
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
// Restart, and the recreate the gang verdict ended — status.ResetOwnsRow):
// an Instance parked behind any other Update or a Migrate continuation is
// skipped, because the gang abandon, wreckage cleanup, release-held, and
// migration-expiry paths own those continuations and clearing one here
// would orphan its surge marker or migration record.
//
// Every present request is answered. A candidate has every pod deleted
// (live list, expectations recorded before each delete) and its
// Operation cleared through the MutateInstance seam — Phase stays Failed
// and LastFailure is preserved, which is exactly the fresh-start shape
// the Create pass rebuilds (Failed with no Operation and no pods). A
// target that is not Failed, does not exist, is guarded as above, or has
// nothing left to tear down is skipped; a malformed value resets
// nothing. Each outcome gets one event. The pod deletes and the status
// write commit before acknowledgeRequest removes the annotation, so a
// crash between the two re-delivers into the nothing-to-reset branch.
//
// A pod delete or status write error leaves the annotation in place and
// returns the error; the request re-drives next pass. Events land on
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
		r.recordResetRejected(eventTarget, ir, val, perr)
	} else {
		reset, skipped, rerr := r.resetInstances(ctx, log, ir, req)
		if rerr != nil {
			// Annotation NOT consumed: the request re-drives the reset
			// next pass; every step above is idempotent on re-delivery.
			return false, fmt.Errorf("reset instances (%s=%q): %w", constants.ResetInstancesAnnotationKey, val, rerr)
		}
		r.recordResetOutcome(eventTarget, ir, reset, skipped)
	}

	key := client.ObjectKeyFromObject(ir)
	return r.acknowledgeRequest(ctx, ir, "reset-instances annotation", func(ctx context.Context) (client.Object, error) {
		fresh := &v1beta1.InferenceReplica{}
		return fresh, r.APIReader.Get(ctx, key, fresh)
	}, constants.ResetInstancesAnnotationKey)
}

// recordResetRejected publishes the one Warning a malformed request earns.
func (r *Reconciler) recordResetRejected(target client.Object, ir *v1beta1.InferenceReplica, val string, perr error) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(target, corev1.EventTypeWarning, string(workloadtypes.EventReasonInstancesResetRejected),
		"InferenceReplica %s/%s component=%s: %s=%q rejected: %v; nothing reset",
		ir.Namespace, ir.Name, ir.Spec.Component, constants.ResetInstancesAnnotationKey, val, perr)
}

// recordResetOutcome publishes what a well-formed request did: one event
// naming the Instances reset, and one naming the Instances skipped or, when
// nothing was reset and nothing skipped, that no Instance was Failed.
func (r *Reconciler) recordResetOutcome(target client.Object, ir *v1beta1.InferenceReplica, reset []int32, skipped []resetSkip) {
	if r.Recorder == nil {
		return
	}
	if len(reset) > 0 {
		r.Recorder.Eventf(target, corev1.EventTypeNormal, string(workloadtypes.EventReasonInstancesReset),
			"InferenceReplica %s/%s component=%s: reset instance(s) %s at operator request (%s annotation): pods deleted and preserved operation cleared; the lifecycle passes rebuild them",
			ir.Namespace, ir.Name, ir.Spec.Component, formatIndices(reset), constants.ResetInstancesAnnotationKey)
	}
	switch {
	case len(skipped) > 0:
		r.Recorder.Eventf(target, corev1.EventTypeNormal, string(workloadtypes.EventReasonInstancesResetSkipped),
			"InferenceReplica %s/%s component=%s: reset skipped for instance(s) %s",
			ir.Namespace, ir.Name, ir.Spec.Component, formatSkips(skipped))
	case len(reset) == 0:
		r.Recorder.Eventf(target, corev1.EventTypeNormal, string(workloadtypes.EventReasonInstancesResetSkipped),
			"InferenceReplica %s/%s component=%s: reset requested for all instances but none is Failed; nothing to reset",
			ir.Namespace, ir.Name, ir.Spec.Component)
	}
}

// resetInstances tears down every requested Instance that is Failed,
// repair-owned, and out of the serving rotation, and reports the indices
// reset plus the requested ones it skipped. Per candidate: live-list its
// pods, skip it untouched if any pod is still serving, otherwise delete
// each pod not already Terminating (ExpectDeletes before the Delete,
// ObservedDelete on error, NotFound tolerated), then clear the preserved
// Operation through MutateInstance, which mirrors the committed status
// onto the caller's IR. LastFailure is untouched, and so is Phase, except
// that a parked Update attempt read Updating reads Failed once its set is
// deleted: the mailbox addresses a parked attempt in either phase. An Instance
// with no pod to delete and no Operation to clear is already in the
// rebuild shape and is skipped — the branch a re-delivered request lands
// in. Stops at the first error; Instances already handled stay
// consistent because every step is idempotent.
func (r *Reconciler) resetInstances(ctx context.Context, log logr.Logger, ir *v1beta1.InferenceReplica, req resetInstancesRequest) (reset []int32, skipped []resetSkip, err error) {
	var candidates []int32
	candidates, skipped = resetCandidates(ir, req)
	if len(candidates) == 0 {
		return nil, skipped, nil
	}

	key := buildKey(ir)
	expectations := r.Expectations
	if expectations == nil {
		expectations = workloadtypes.DefaultExpectations
	}
	mutateInstance := buildMutateInstance(r.statusWriter(), r.liveReader(), ir)
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
		deleted, derr := r.deleteInstancePods(ctx, expectations, key, idx, pods)
		if derr != nil {
			return reset, skipped, derr
		}

		// The transition write itself belongs to the workload layer; the
		// seam re-checks Phase=Failed and the Operation owner on the
		// fresh read.
		cleared, merr := status.ClearFailedInstanceOperation(ctx, mutateInstance, idx)
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

// resetCandidates sorts the requested Instances into the ones the reset may
// tear down and the ones it leaves alone with the reason: an index no row
// carries, a row that is neither Failed nor an Update attempt waiting in
// place, and an attempt another owner continues. "all" walks the Failed rows
// in index order; a list keeps the request's order.
func resetCandidates(ir *v1beta1.InferenceReplica, req resetInstancesRequest) (candidates []int32, skipped []resetSkip) {
	statuses := make(map[int32]*v1beta1.OMENativeInstanceStatus, len(ir.Status.InstanceStatuses))
	for i := range ir.Status.InstanceStatuses {
		statuses[ir.Status.InstanceStatuses[i].Index] = &ir.Status.InstanceStatuses[i]
	}

	// classify admits a Failed Instance whose parked attempt the reset
	// owns; a continuation owned by the rollout or migration machinery is
	// skipped with its owner named.
	classify := func(idx int32, s *v1beta1.OMENativeInstanceStatus) {
		row := v1beta1convert.InstanceStatusToWorkload(*s)
		if !status.ResetOwnsRow(&row) {
			skipped = append(skipped, resetSkip{index: idx, reason: "owned by " + string(s.Operation.Type)})
			return
		}
		candidates = append(candidates, idx)
	}
	// addressable is the Failed row, and the Update attempt parked after
	// its disposition in whichever phase its pod set gives it: the row it
	// stands for is Failed.
	addressable := func(s *v1beta1.OMENativeInstanceStatus) bool {
		return s.Phase == v1beta1.OMENativeInstanceFailed ||
			workloadtypes.OperationParked(v1beta1convert.InstanceOperationToWorkload(s.Operation))
	}
	if req.all {
		failed := make([]int32, 0, len(statuses))
		for idx, s := range statuses {
			if addressable(s) {
				failed = append(failed, idx)
			}
		}
		sort.Slice(failed, func(i, j int) bool { return failed[i] < failed[j] })
		for _, idx := range failed {
			classify(idx, statuses[idx])
		}
		return candidates, skipped
	}
	for _, idx := range req.indices {
		s, ok := statuses[idx]
		switch {
		case !ok:
			skipped = append(skipped, resetSkip{index: idx, reason: "no such instance"})
		case !addressable(s):
			skipped = append(skipped, resetSkip{index: idx, reason: "Phase=" + string(s.Phase)})
		default:
			classify(idx, s)
		}
	}
	return candidates, skipped
}

// deleteInstancePods deletes every pod of the Instance that is not already
// terminating and counts the deletes that landed. The expectation is
// recorded before each delete and taken back when the delete fails; a pod
// that was already gone is not counted and stops nothing.
func (r *Reconciler) deleteInstancePods(ctx context.Context, expectations *workloadtypes.Expectations, key workloadtypes.Key, idx int32, pods []*corev1.Pod) (int, error) {
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
			return deleted, fmt.Errorf("delete pod %s/%s (instance=%d): %w", pod.Namespace, pod.Name, idx, derr)
		}
		deleted++
	}
	return deleted, nil
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
