package ops

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
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

// recycleTerminalPods deletes the terminal pods occupying an Instance's
// desired names so the owning operation (Create or Restart, named by
// opType) can recreate them on a later pass, once the watch has observed
// the deletes. Reports true when the caller must not create this pass:
// the recycle is paced, is waiting on expectations, or was just issued.
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
func recycleTerminalPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, opType workload.InstanceOperationType, dead []*corev1.Pod) (bool, error) {
	if len(dead) == 0 {
		return false, nil
	}
	now := input.Now()
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, idx); s != nil && s.Operation != nil && s.Operation.Type == opType {
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
	if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx) {
		return true, nil
	}

	termination := terminalPodTermination(dead[0], metav1.NewTime(now))
	var recycle int32
	err := input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Operation == nil || s.Operation.Type != opType {
			return false
		}
		op := *s.Operation
		op.RetryCount++
		op.LastProgressAt = metav1.NewTime(now)
		s.Operation = &op
		recycle = op.RetryCount
		if termination != nil {
			captured := *termination
			s.LastFailure = &captured
		}
		return true
	})
	if err != nil {
		return true, fmt.Errorf("record terminal pod recycle (instance=%d): %w", idx, err)
	}

	// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
	// ObservedDelete on error — a failed RPC fires no event to decrement.
	for _, pod := range dead {
		if pod.DeletionTimestamp != nil {
			continue
		}
		deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx, 1)
		if err := deps.Client.Delete(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return true, fmt.Errorf("delete terminal pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonTerminalPodRecycled,
		"OMENative %s deleted terminal pod(s) %s for recreate (%s recycle %d)",
		instanceKey(input.Key.Component, idx), describeTerminalPods(dead), opType, recycle)
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
