package ops

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Wreckage is per-instance rollout debris keyed to a SUPERSEDED
// revision: state that no revision-diff update trigger can ever reach,
// because the trigger's predicate is "this instance must move to a
// different revision" while the wreckage sits at zero revision distance
// (a corrective roll-back) or on a third-party revision. Two shapes:
//
//   - GANG: a Failed source still carrying its gang-surge continuation
//     (Op{Update, SurgeIndex}) toward a revision that is no longer the
//     roll target. When the corrective target equals the source's
//     RunningRevision the trigger never fires, so the abandon
//     continuation must be dispatched explicitly.
//   - ALIEN PODS: live pods whose revision-hash label matches neither
//     the instance's RunningRevision nor the current roll target — the
//     dead pod an exhausted attempt toward a superseded revision left
//     behind (e.g. parked at the surge ordinal).
//
// Cleanup restores the invariant that nothing keyed to a superseded
// revision gates, steers, or occupies reconciliation.

// EvaluateWreckage is the pure wreckage predicate for one Instance.
// Plan consults it only for instances the update trigger declined
// (in-flight and re-triggered instances clean their own debris through
// the update machinery). instancePods must already be filtered to the
// instance's index; the clock is not read.
//
// Excluded by design: migrate-owned statuses (the migration record owns
// them), gang surge-target markers (a live marker is owned by its
// source; an orphaned one is collected by the plan's marker-liveness
// scale-down), and transient phases (Creating / Deleting / Restarting
// are not interruptible).
func EvaluateWreckage(s *workload.InstanceStatus, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) bool {
	if s == nil || target == nil {
		return false
	}
	if isMigrateOwnedStatus(s) || isGangSurgeTargetMarker(s) {
		return false
	}
	if s.Phase != workload.InstancePhaseReady && s.Phase != workload.InstancePhaseFailed {
		return false
	}
	if failedGangSurgeContinuation(s, target.Name) {
		return true
	}
	return len(alienRevisionPods(s, target.Name, instancePods)) > 0
}

// failedGangSurgeContinuation reports the gang wreckage shape: a Failed
// source whose preserved gang-surge operation targets a revision other
// than the current roll target.
func failedGangSurgeContinuation(s *workload.InstanceStatus, targetName string) bool {
	return s.Phase == workload.InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
		s.Operation.SurgeIndex != nil &&
		s.Operation.TargetRevision != targetName
}

// alienRevisionPods returns the instance's live pods labeled with a
// revision that matches neither the instance's RunningRevision nor the
// current roll target. Unlabeled pods are never selected (legacy pods
// are the ordinal partition's business). An empty RunningRevision
// yields no aliens: with no recorded baseline, alienness is
// undecidable — that state is owned by the trigger's per-pod diff /
// adoption path, and deleting an unproven-but-innocent pod there would
// destroy the very pod adoption is waiting on.
func alienRevisionPods(s *workload.InstanceStatus, targetName string, pods []*corev1.Pod) []*corev1.Pod {
	if s.RunningRevision == "" {
		return nil
	}
	running := query.RevisionFromName(s.RunningRevision)
	target := query.RevisionFromName(targetName)
	var out []*corev1.Pod
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		hash, ok := pod.Labels[query.LabelRevisionHash]
		if !ok || hash == "" {
			continue
		}
		rev := query.RevisionFromPod(pod)
		if rev.Same(running) || rev.Same(target) {
			continue
		}
		out = append(out, pod)
	}
	return out
}

// CleanupWreckage abandons one instance's superseded-revision wreckage
// toward the CURRENT desired state — legal when target equals the
// instance's RunningRevision (the corrective roll-back), where the
// update machinery is unreachable. Effects route through the existing
// machinery:
//
//   - gang continuation → abandonFailedGangSurge (deletes the dead
//     replacement gang, drops its marker, records the failure on the
//     superseded revision's RetryBlock, resets the source Ready on its
//     running revision);
//   - alien-revision pods → direct delete, the same eviction the
//     superseded-surge redirect applies to a not-yet-promoted surge pod.
//
// The serving source is never touched: no serving-gate flip, no status
// stamp — after the debris is gone the Create pass re-proves readiness
// and promotes. done=true means no wreckage remains for this instance.
func CleanupWreckage(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("CleanupWreckage: nil client")
	}
	if target == nil {
		return true, nil
	}
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
	if s == nil {
		return true, nil
	}

	if failedGangSurgeContinuation(s, target.Name) {
		return abandonFailedGangSurge(ctx, deps, input, plan, inst.Index, *s.Operation.SurgeIndex,
			s.RunningRevision, s.Operation.TargetRevision,
			instanceFailureReason(s, "gang surge abandoned after a corrective edit"), instanceFailureWorkloadCaused(s))
	}

	aliens := alienRevisionPods(s, target.Name, instancePods)
	if len(aliens) == 0 {
		return true, nil
	}
	if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
		return false, nil
	}
	for _, pod := range aliens {
		deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index, 1)
		if err := deps.Client.Delete(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index)
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("delete superseded-revision pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonSupersededWreckageCleaned,
		"OMENative %s deleted %d superseded-revision pod(s); current target %s",
		instanceKey(input.Key.Component, inst.Index), len(aliens), target.Name)
	return false, nil
}
