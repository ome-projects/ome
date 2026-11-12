package ops

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// occupantsThat returns the pods among existing that hold one of the desired
// target names and that the predicate reports as never running again.
func occupantsThat(existing []*corev1.Pod, targets []podTarget, neverRunsAgain func(*corev1.Pod) bool) []*corev1.Pod {
	if len(existing) == 0 || len(targets) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		wanted[t.Name] = struct{}{}
	}
	var occupants []*corev1.Pod
	for _, pod := range existing {
		if !neverRunsAgain(pod) {
			continue
		}
		if _, ok := wanted[pod.Name]; ok {
			occupants = append(occupants, pod)
		}
	}
	return occupants
}

// terminalTargetPods returns the terminal pods among existing that occupy
// one of the desired target names — the dead occupants that block
// recreating those targets under their stable names.
func terminalTargetPods(existing []*corev1.Pod, targets []podTarget) []*corev1.Pod {
	return occupantsThat(existing, targets, query.IsTerminalPod)
}

// admissionRejectedTargetPods returns the pods occupying one of the
// desired target names that the kubelet refused to admit. A narrower set
// than terminalTargetPods: a pod the node rejected never ran, and the
// refusal is the node's capacity accounting rather than anything the
// operation placed there, so the name can be rebuilt immediately even on
// the paths that otherwise leave a terminal occupant to the deadline.
func admissionRejectedTargetPods(existing []*corev1.Pod, targets []podTarget) []*corev1.Pod {
	return occupantsThat(existing, targets, func(pod *corev1.Pod) bool {
		_, rejected := query.PodAdmissionRejected(pod)
		return rejected
	})
}

// recycleTerminalTargets frees the target names a terminal pod is holding
// so the owning attempt can place them again. Every update attempt that
// puts a replacement down — the per-pod surge, the recreate's rebuild, the
// gang surge, the in-place patch — replaces a dead pod at once, as the
// Create and Restart passes do, rather than reading it as "present but
// not ready" to the operation deadline.
//
// statusIdx owns the operation and takes the bookkeeping; podIdx buckets
// the pods and their expectations. They differ for a gang surge, whose
// pods live under the surge index while the governing operation stays on
// the source. Reports true when the caller must not create this pass.
func recycleTerminalTargets(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, statusIdx, podIdx int32, opType workload.InstanceOperationType, existing []*corev1.Pod, targets []podTarget) (bool, error) {
	return recycleTerminalPods(ctx, deps, input, statusIdx, podIdx, opType,
		terminalTargetPods(existing, targets))
}

// recycleAdmissionRejectedTargets is recycleTerminalTargets narrowed to
// the pods a node refused: the migration surge frees only those, and
// leaves any other terminal surge pod to its record's deadline.
func recycleAdmissionRejectedTargets(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, statusIdx, podIdx int32, opType workload.InstanceOperationType, existing []*corev1.Pod, targets []podTarget) (bool, error) {
	return recycleTerminalPods(ctx, deps, input, statusIdx, podIdx, opType,
		admissionRejectedTargetPods(existing, targets))
}

// recycleTerminalPods deletes the terminal pods occupying an Instance's
// desired names so the owning operation (named by opType) can recreate
// them on a later pass, once the watch has observed the deletes. Reports
// true when the caller must not create this pass: the recycle is paced,
// is waiting on expectations, or was just issued.
//
// statusIdx owns the operation and takes the bookkeeping; podIdx buckets
// the pods and their expectations. The two are the same index for every
// path whose pods live on the row that owns the operation.
//
// Pacing: repeated recycles within one attempt follow the same-target
// update retry ladder (initialDelay × multiplier^n, capped at maxDelay),
// indexed by Operation.RetryCount and anchored on Operation.LastProgressAt.
// The first recycle of an attempt is immediate. With no policy configured
// there is no ladder to follow, so the next recycle happens on the next
// pass. Either way the attempt's Operation.Deadline remains the outer
// bound: past it nothing is recycled, and escalation owns the attempt.
// A Failed row carries no deadline, so a terminal replacement member of
// a gang source that failed at its drain step is replaced under the
// pacing alone.
//
// The bookkeeping (RetryCount, LastProgressAt, LastFailure from the dead
// pod's diagnostics) is written ahead of the deletes so a crash between
// the two can only delay the next recycle, never skip the pacing.
func recycleTerminalPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, statusIdx, podIdx int32, opType workload.InstanceOperationType, dead []*corev1.Pod) (bool, error) {
	if len(dead) == 0 {
		return false, nil
	}
	now := input.Now()
	if replacementHeldBack(input.ObservedState.Instance(statusIdx), opType, input.UpdateRetryPolicy, now) {
		return true, nil
	}
	if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, podIdx) {
		return true, nil
	}

	termination := terminalPodTermination(dead[0], metav1.NewTime(now))
	recycle, err := status.RecordRecycleAttempt(ctx, input, statusIdx, opType, metav1.NewTime(now), termination, dead[0].CreationTimestamp)
	if err != nil {
		return true, fmt.Errorf("record terminal pod recycle (instance=%d): %w", statusIdx, err)
	}
	if err := deleteTerminalPods(ctx, deps, input, podIdx, dead); err != nil {
		return true, err
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonTerminalPodRecycled,
		"OMENative %s deleted terminal pod(s) %s for recreate (%s recycle %d)",
		workload.InstanceKey(input.Key.Component, podIdx), describeTerminalPods(dead), opType, recycle)
	return true, nil
}

// replacementHeldBack reports whether the attempt that owns opType on row s
// must not delete its terminal pods now: its deadline has passed, so
// escalation owns the attempt, or the retry schedule's delay since its last
// attempt has not elapsed. An attempt's first delete, an attempt with no
// schedule configured and a row under another operation are never held back.
func replacementHeldBack(s *workload.InstanceStatus, opType workload.InstanceOperationType, schedule *workload.RetryPolicy, now time.Time) bool {
	if s == nil || s.Operation == nil || s.Operation.Type != opType {
		return false
	}
	op := s.Operation
	if !op.Deadline.IsZero() && now.After(op.Deadline.Time) {
		return true
	}
	if op.RetryCount > 0 && schedule != nil {
		return now.Before(op.LastProgressAt.Add(schedule.NextRetryDelay(op.RetryCount)))
	}
	return false
}

// deleteTerminalPods deletes every pod not already on its way out. Each
// delete is recorded in the expectations before the request and taken back
// when the request fails, because a failed request fires no event to
// release it; a pod already gone counts as deleted.
func deleteTerminalPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, podIdx int32, pods []*corev1.Pod) error {
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, podIdx, 1)
		if err := deps.Client.Delete(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, podIdx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("delete terminal pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// terminalPodTermination captures the diagnostics of a terminal pod for
// LastFailure. A pod rejected before any container ran carries its cause
// only in the pod-level status reason, which replaces the bare PodFailed
// placeholder the container-level fallback records.
func terminalPodTermination(pod *corev1.Pod, now metav1.Time) *workload.InstanceTermination {
	t := workload.PodTermination(pod, now)
	if t == nil {
		return nil
	}
	if t.ContainerName == "" && pod.Status.Reason != "" {
		t.Reason = pod.Status.Reason
	}
	return t
}

// describeTerminalPods formats "name (phase[/reason])" for each pod.
func describeTerminalPods(pods []*corev1.Pod) string {
	parts := make([]string, 0, len(pods))
	for _, pod := range pods {
		desc := pod.Name + " (" + string(pod.Status.Phase)
		if pod.Status.Reason != "" {
			desc += "/" + pod.Status.Reason
		}
		parts = append(parts, desc+")")
	}
	return strings.Join(parts, ", ")
}
