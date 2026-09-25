package ops

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// terminalTargetPods returns the terminal pods among existing that occupy
// one of the desired target names — the dead occupants that block
// recreating those targets under their stable names.
func terminalTargetPods(existing []*corev1.Pod, targets []podTarget) []*corev1.Pod {
	if len(existing) == 0 || len(targets) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		wanted[t.Name] = struct{}{}
	}
	var dead []*corev1.Pod
	for _, pod := range existing {
		if !query.IsTerminalPod(pod) {
			continue
		}
		if _, ok := wanted[pod.Name]; ok {
			dead = append(dead, pod)
		}
	}
	return dead
}

// admissionRejectedTargetPods returns the pods occupying one of the
// desired target names that the kubelet refused to admit. A narrower set
// than terminalTargetPods: a pod the node rejected never ran, and the
// refusal is the node's capacity accounting rather than anything the
// operation placed there, so the name can be rebuilt immediately even on
// the paths that otherwise leave a terminal occupant to the deadline.
func admissionRejectedTargetPods(existing []*corev1.Pod, targets []podTarget) []*corev1.Pod {
	if len(existing) == 0 || len(targets) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(targets))
	for _, t := range targets {
		wanted[t.Name] = struct{}{}
	}
	var dead []*corev1.Pod
	for _, pod := range existing {
		if _, rejected := query.PodAdmissionRejected(pod); !rejected {
			continue
		}
		if _, ok := wanted[pod.Name]; ok {
			dead = append(dead, pod)
		}
	}
	return dead
}

// recycleAdmissionRejectedTargets frees the target names an admission
// rejection is holding so the owning operation can place them again.
// Shared by every path that puts a REPLACEMENT pod down — the per-pod
// surge, the recreate's rebuild, the gang surge — which otherwise read a
// rejected pod as "present but not ready" and hold to the operation
// deadline.
//
// statusIdx owns the operation and takes the bookkeeping; podIdx buckets
// the pods and their expectations. They differ for a gang surge, whose
// pods live under the surge index while the governing operation stays on
// the source. Reports true when the caller must not create this pass.
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
//
// The bookkeeping (RetryCount, LastProgressAt, LastFailure from the dead
// pod's diagnostics) is written ahead of the deletes so a crash between
// the two can only delay the next recycle, never skip the pacing.
func recycleTerminalPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, statusIdx, podIdx int32, opType workload.InstanceOperationType, dead []*corev1.Pod) (bool, error) {
	if len(dead) == 0 {
		return false, nil
	}
	now := input.Now()
	if s := input.ObservedState.Instance(statusIdx); s != nil && s.Operation != nil && s.Operation.Type == opType {
		op := s.Operation
		if !op.Deadline.IsZero() && now.After(op.Deadline.Time) {
			return true, nil
		}
		if op.RetryCount > 0 && input.UpdateRetryPolicy != nil {
			notBefore := op.LastProgressAt.Add(input.UpdateRetryPolicy.NextRetryDelay(op.RetryCount))
			if now.Before(notBefore) {
				return true, nil
			}
		}
	}
	if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, podIdx) {
		return true, nil
	}

	termination := terminalPodTermination(dead[0], metav1.NewTime(now))
	recycle, err := status.RecordRecycleAttempt(ctx, input, statusIdx, opType, metav1.NewTime(now), termination)
	if err != nil {
		return true, fmt.Errorf("record terminal pod recycle (instance=%d): %w", statusIdx, err)
	}

	// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
	// ObservedDelete on error — a failed RPC fires no event to decrement.
	for _, pod := range dead {
		if pod.DeletionTimestamp != nil {
			continue
		}
		deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, podIdx, 1)
		if err := deps.Client.Delete(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, podIdx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return true, fmt.Errorf("delete terminal pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonTerminalPodRecycled,
		"OMENative %s deleted terminal pod(s) %s for recreate (%s recycle %d)",
		workload.InstanceKey(input.Key.Component, podIdx), describeTerminalPods(dead), opType, recycle)
	return true, nil
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
