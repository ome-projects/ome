package ops

import (
	"context"
	"fmt"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// surgeDrainKey identifies a SurgeThenDrain drain writer entry on the
// old pod's ome.io/serving gate. Indexed by (instance, surge ordinal)
// so a parallel surge across instances doesn't collide. Removed when
// the old pod is deleted at the end of Phase 2.
func surgeDrainKey(idx int32, newOrdinal int32) string {
	return fmt.Sprintf("update-surge-drain-%d-%d", idx, newOrdinal)
}

const createContainerErrorReason = "CreateContainerError"

// recycleFailedCreateContainerTarget removes a failed, non-serving surge
// target after disposition has authorized relocation away from its node. The
// active source remains untouched. Returning handled=true parks the normal
// surge state machine until the failed pod is gone or a retry is authorized.
func recycleFailedCreateContainerTarget(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	inst workload.InstancePlan,
	target *appsv1.ControllerRevision,
	status *workload.InstanceStatus,
	oldPods, surgePods []*corev1.Pod,
) (bool, error) {
	if status == nil || status.Phase != workload.InstancePhaseFailed || status.Operation != nil ||
		status.LastFailure == nil || status.LastFailure.Reason != createContainerErrorReason ||
		target == nil || status.TargetRevision == "" || status.TargetRevision != target.Name {
		return false, nil
	}

	if len(oldPods) != 1 || oldPods[0] == nil {
		return true, nil
	}
	liveSource := &corev1.Pod{}
	if err := deps.Reader().Get(ctx, client.ObjectKeyFromObject(oldPods[0]), liveSource); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return true, fmt.Errorf("read serving source before failed-target cleanup %s/%s: %w", oldPods[0].Namespace, oldPods[0].Name, err)
	}
	// Cleanup is safe only while the canonical source is still healthy and in
	// rotation. A missing or unhealthy source requires operator attention rather
	// than another automatic target recycle.
	if liveSource.DeletionTimestamp != nil || !podreadiness.IsContainersReady(liveSource) || !podreadiness.IsServing(liveSource) {
		return true, nil
	}
	sourceRev := query.RevisionFromName(status.RunningRevision)
	activePodRev := query.RevisionFromPod(liveSource)
	if sourceRev.IsZero() || activePodRev.IsZero() || !activePodRev.Same(sourceRev) {
		return true, nil
	}

	// An empty surge slot means the prior delete completed. Let the ordinary
	// Phase 1 path stamp a fresh operation and create the replacement.
	if len(surgePods) == 0 {
		return false, nil
	}
	if len(surgePods) != 1 {
		return true, nil
	}
	observedTarget := surgePods[0]
	if observedTarget == nil {
		return true, nil
	}
	pod := &corev1.Pod{}
	if err := deps.Reader().Get(ctx, client.ObjectKeyFromObject(observedTarget), pod); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return true, fmt.Errorf("read failed surge target %s/%s: %w", observedTarget.Namespace, observedTarget.Name, err)
	}
	if pod.DeletionTimestamp != nil {
		return true, nil
	}
	if status.LastFailure.PodName != "" && status.LastFailure.PodName != pod.Name {
		return true, nil
	}
	failedRev := query.RevisionFromName(status.TargetRevision)
	podRev := query.RevisionFromPod(pod)
	if failedRev.IsZero() || podRev.IsZero() || !podRev.Same(failedRev) {
		return false, nil
	}
	if podreadiness.IsServing(pod) || podreadiness.IsPodReady(pod) || !podHasWaitingReason(pod, createContainerErrorReason) {
		return false, nil
	}

	// ExcludedNodes is projected from persisted AutoRecover directives. The
	// failed pod's node appearing here proves that disposition authorized this
	// relocation attempt. Once the configured budget is exhausted, a new node is
	// not recorded and this branch parks the Instance instead of churning pods.
	if pod.Spec.NodeName == "" || !slices.Contains(inst.ExcludedNodes, pod.Spec.NodeName) {
		return true, nil
	}
	if pod.UID == "" {
		return true, fmt.Errorf("recycle failed surge target %s/%s without an observed UID", pod.Namespace, pod.Name)
	}
	if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
		return true, nil
	}

	uid := pod.UID
	deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index, 1)
	if err := deps.Client.Delete(ctx, pod, client.Preconditions{UID: &uid}); err != nil {
		deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index)
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return true, nil
		}
		return true, fmt.Errorf("delete failed surge target %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonFailedSurgeTargetRecycled,
		"OMENative %s deleted failed non-serving surge target %s on excluded node %s; retry will create a fresh pod",
		instanceKey(input.Key.Component, inst.Index), pod.Name, pod.Spec.NodeName)
	return true, nil
}

func podHasWaitingReason(pod *corev1.Pod, reason string) bool {
	if pod == nil {
		return false
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, status := range statuses {
			if status.State.Waiting != nil && status.State.Waiting.Reason == reason {
				return true
			}
		}
	}
	return false
}

// surgeUpdate runs the SurgeThenDrain rollout for one Instance.
//
// State machine (per Instance):
//
//	Recovery:          A failed CreateContainerError target with an
//	                   authorized node exclusion is deleted while the active
//	                   source remains serving. The empty slot then re-enters
//	                   Phase 1.
//	Phase 1 (Surge):   Op{Step=Surge}. Create the surge pod at
//	                   ordinal=1-ActiveOrdinal with target revision.
//	                   Wait ContainersReady, mark it serving, then wait
//	                   for PodReady.
//	Phase 2 (Drain):   Op.Step=SurgeDrain. Mark the old pod serving=False
//	                   (leaves rotation). Wait drain.IsPodDrained on the
//	                   per-revision routed Service.
//	Phase 3 (Settle):  Op.Step=SurgeDrainSettle. Keep the old pod alive
//	                   for the configured grace period so persistent
//	                   connections age out, then delete it.
//	Phase 4 (Promote): Old pod gone. Advance InstanceStatus.
//	                   ActiveOrdinal to the new slot, set Phase=Ready,
//	                   RunningRevision=target, clear Operation.
//
// Invariants:
//   - serving count never dips below desired — surge always rotates IN
//     before old rotates OUT.
//   - alive count peaks at desired+1 per Instance during the surge
//     window. The surge gate (coordination GateContext.CheckSurge)
//     caps concurrent surges across Instances.
//   - no in-place patching — different pod NAMES throughout.
//   - target stability across reconciles: once an Operation is recorded
//     with a surge lifecycle Step and TargetRevision=X, the in-flight
//     surge is committed to driving to X. Spec bumps mid-surge are
//     picked up by the NEXT reconcile after this surge promotes —
//     detectUpdateTrigger fires because RunningRevision=X != target=Y.
//     Without pinning, the surge would silently drift the in-flight
//     pod (still on X) to "RunningRevision=Y" in status.
//
// Single-pod path (Runner.Size == 1). Multi-pod (gang) SurgeThenDrain
// branches to gangSurgeUpdate at the top of this function.
func surgeUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, pods []*corev1.Pod) (bool, error) {
	// Multi-pod (gang) Instances surge a whole replacement gang at a
	// fresh instance index (gangSurgeUpdate); single-pod Instances toggle
	// the ActiveOrdinal slot in place below.
	if inst.TotalPods() > 1 {
		return gangSurgeUpdate(ctx, deps, input, plan, inst, target)
	}
	if len(inst.Runners) == 0 || inst.Runners[0].Size != 1 {
		return false, fmt.Errorf("surgeUpdate: unexpected single-pod runner layout (instance=%d)", inst.Index)
	}
	runner := inst.Runners[0]

	// Where ActiveOrdinal lives today (0 by default). The surge pod
	// goes to the opposite slot.
	var oldOrdinal int32
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil {
		oldOrdinal = s.ActiveOrdinal
	}
	newOrdinal := int32(1) - oldOrdinal

	// Partition observed pods by ordinal slot.
	oldPods, surgePods, stragglers := partitionPodsBySurgeOrdinal(pods, oldOrdinal, newOrdinal)
	if len(stragglers) > 0 {
		// Pods at neither slot — leftover from a future ordinal scheme
		// or label corruption. Refuse to proceed (same shape recreate
		// uses for unknownPods).
		for _, pod := range stragglers {
			recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonFoundOrphan,
				"OMENative %s found pod %s/%s with unexpected ordinal label; refusing to surge-update",
				instanceKey(input.Key.Component, inst.Index), pod.Namespace, pod.Name)
		}
		return false, nil
	}

	status := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
	if handled, err := recycleFailedCreateContainerTarget(ctx, deps, input, inst, target, status, oldPods, surgePods); err != nil || handled {
		return false, err
	}

	// Uncommitted-surge redirect (level-triggered "desired wins"): the in-flight
	// surge is committed to something the desired state no longer asks for — a
	// superseded target revision, or SurgeThenDrain itself once the strategy is
	// edited away from it — AND is not about to promote (surge pod not yet
	// Ready). Abandon JUST the stuck surge pod — the source at oldOrdinal keeps
	// serving, so capacity never drops — and reset the Instance to Ready on its
	// running revision. The NEXT reconcile re-enters through the normal gated
	// path under whatever the desired state now is, so maxSurge /
	// maxUnavailable / ratio / canary all re-apply. Without this, a surge that
	// never becomes Ready and never escalates pins itself to a dead rev and
	// holds the maxSurge budget until instanceReadyTimeout, and a strategy edit
	// made to escape a surge that cannot fit on a full cluster is ignored. A
	// surge that IS about to promote (Ready) skips this and keeps the X-2 pin
	// below so its promote stamps the rev its pods actually run. Only
	// Step=Surge — later surge steps are past the point of no return (source
	// already draining) and finish their cycle.
	supersededTarget := status != nil && status.Operation != nil &&
		status.Operation.TargetRevision != "" && status.Operation.TargetRevision != target.Name
	strategyLeftSurge := plan.UpdateStrategy.Type != "" &&
		plan.UpdateStrategy.Type != workload.UpdateStrategySurgeThenDrain
	if s := status; s != nil &&
		s.Phase != workload.InstancePhaseFailed && s.Operation != nil &&
		s.Operation.Type == workload.InstanceOperationUpdate &&
		s.Operation.Step == updateStepSurge &&
		(supersededTarget || strategyLeftSurge) &&
		!(len(surgePods) > 0 && query.AllPodsRuntimeReady(surgePods)) {
		if len(surgePods) > 0 {
			if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
				return false, nil
			}
			for _, pod := range surgePods {
				if pod.DeletionTimestamp != nil {
					continue
				}
				deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index, 1)
				if derr := deps.Client.Delete(ctx, pod); derr != nil {
					deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index)
					if apierrors.IsNotFound(derr) {
						continue
					}
					return false, fmt.Errorf("delete abandoned surge pod %s/%s: %w", pod.Namespace, pod.Name, derr)
				}
			}
			return false, nil
		}
		// Surge pod gone — reset to Ready on the running rev (source unchanged); the
		// next reconcile re-enters via DetectUpdateTrigger + the budget /
		// coordination gates, under the currently desired strategy.
		//
		// The reset clears Operation, and the InstanceStatus the caller handed
		// in may alias the storage MutateInstance writes through. Read the
		// abandoned revision out before the write, not after.
		abandonedRev := s.Operation.TargetRevision
		if err := patchInstanceStatusReadyOnRevision(ctx, input, inst.Index, s.RunningRevision); err != nil {
			return false, fmt.Errorf("reset abandoned surge source (instance=%d): %w", inst.Index, err)
		}
		if strategyLeftSurge && !supersededTarget {
			recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateStarted,
				"OMENative %s abandoned uncommitted surge to %s; rolling under %s instead",
				instanceKey(input.Key.Component, inst.Index), abandonedRev, plan.UpdateStrategy.Type)
		} else {
			recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateStarted,
				"OMENative %s abandoned superseded surge to %s; re-surging toward %s",
				instanceKey(input.Key.Component, inst.Index), abandonedRev, target.Name)
		}
		return false, nil
	}

	// surgeTargetName pins the revision this surge is committed to
	// driving to. On the FIRST pass (no in-flight Op) it equals
	// target.Name — the rev the dispatcher just decided to roll to. On
	// SUBSEQUENT passes (an in-flight surge Op from a prior
	// reconcile) it is the rev that was recorded when the surge was
	// stamped — even if `target` has since moved because of a fresh
	// spec bump. Pinning prevents the state machine from declaring the
	// in-flight pod "on the new target" when it is actually on the
	// previous target, which is the bump-during-bump corruption mode
	// (status says vN but the pods are still on vN-1). The next surge
	// cycle for the newer target fires
	// naturally after this surge promotes, because detectUpdateTrigger
	// observes RunningRevision != target.Name.
	surgeTargetName := target.Name
	// instanceFailed: this surge already escalated to Phase=Failed. Recovery drops
	// the pin below so a corrective target can classify the old target as stale;
	// same-target CreateContainerError relocation is handled before this point.
	instanceFailed := false
	if s := status; s != nil {
		instanceFailed = s.Phase == workload.InstancePhaseFailed
		if !instanceFailed && s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
			isSurgeUpdateStep(s.Operation.Step) &&
			s.Operation.TargetRevision != "" {
			// Normal in-flight surge: stay pinned to the committed rev (X-2
			// anti-corruption). A FAILED surge instead re-commits to the current
			// target so a corrective revision can supersede the failed one.
			surgeTargetName = s.Operation.TargetRevision
		}
	}
	surgeTargetRev := query.RevisionFromName(surgeTargetName)

	// Re-route any pod stranded at the surge ordinal whose revision-hash
	// label is NOT the pinned target through the drain path — leftovers
	// from a previous cycle's promote, or a dead pod from an exhausted
	// attempt toward a superseded revision. Without the recheck the
	// ordinal partition treats such a pod as the valid surge and either
	// promotes the wrong revision (X-2) or waits forever on a pod that
	// can never become Ready. See reclassifyByRevisionHash.
	surgePods, oldPods = reclassifyByRevisionHash(surgePods, oldPods, surgeTargetRev)

	// Phase 1 entry: stamp Phase=Updating + Op.Step=Surge if not
	// already there. Idempotent on the second pass. Writes
	// surgeTargetName so the recorded TargetRevision stays pinned to
	// the in-flight surge's commitment, not the latest target.
	wasNotSurging := true
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil &&
		s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
		isSurgeUpdateStep(s.Operation.Step) {
		wasNotSurging = false
	}
	if err := patchInstanceStatusSurgingForUpdate(ctx, input, inst.Index, surgeTargetName, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("stamp surge step (instance=%d): %w", inst.Index, err)
	}
	if wasNotSurging {
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateStarted,
			"OMENative %s surge to revision %s (newOrdinal=%d)",
			instanceKey(input.Key.Component, inst.Index), surgeTargetName, newOrdinal)
	}

	// Phase 1: create surge pod if missing. The created pod's
	// ome.io/revision-hash label is stamped from surgeTargetName so it
	// matches the in-flight commitment — the pinned-target equivalent of
	// rendering against the live target revision.
	//
	// Skip the create when the surge ordinal slot is already occupied
	// by a pod the reclassify just moved into the drain set. That's
	// the recovery path where a stale-rev pod lives at the surge slot
	// — the Create would AlreadyExists against it, leaving the stale
	// pod in place forever. The stale-slot branch below evicts it; the
	// NEXT reconcile pass enters Phase 1 with an empty surge slot and
	// creates the correct-rev pod.
	staleAtSurgeSlot := false
	for _, pod := range oldPods {
		if ord, ok := query.PodOrdinalFromLabels(pod); ok && ord == newOrdinal {
			staleAtSurgeSlot = true
			break
		}
	}
	if len(surgePods) == 0 && !staleAtSurgeSlot {
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		targets := []podTarget{{
			Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, newOrdinal),
			Runner:  runner,
			Ordinal: newOrdinal,
		}}
		if _, err := createMissingPods(ctx, deps, input, plan, inst, inst.Index, targets, query.RevisionFromName(surgeTargetName)); err != nil {
			if createRejectionHandled(err) {
				return false, nil
			}
			return false, fmt.Errorf("create surge pod (instance=%d, ordinal=%d): %w", inst.Index, newOrdinal, err)
		}
		return false, nil
	}
	// Stale-slot eviction: the surge ordinal holds a wrong-revision pod
	// and no valid surge is alive. Delete ONLY the pod(s) at the surge
	// slot — the same direct eviction the superseded-surge redirect
	// applies to a not-yet-promoted surge pod — and leave the canonical
	// pod at the old ordinal serving untouched. Draining the source here
	// would take the instance's only healthy pod out of rotation before
	// a replacement exists (a per-instance outage for the whole recovery
	// window); the real drain happens in Phase 2, after the correct-rev
	// surge pod is Ready and in rotation.
	if staleAtSurgeSlot && len(surgePods) == 0 {
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		for _, pod := range oldPods {
			if ord, ok := query.PodOrdinalFromLabels(pod); !ok || ord != newOrdinal {
				continue
			}
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete stale-slot pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Phase 1 (cont): wait surge pod ContainersReady. Don't flip serving
	// or touch the old pod until the surge has containers up — that's the
	// no-downtime guarantee.
	if !query.AllPodsRuntimeReady(surgePods) {
		return false, nil
	}

	// ContainersReady permits enabling the serving gate. PodReady is
	// observed below before the source leaves rotation.
	for _, pod := range surgePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark surge serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	if len(oldPods) > 0 {
		// Keep the source in rotation until kubelet has incorporated the
		// replacement's serving gate into PodReady.
		for _, pod := range surgePods {
			if !podreadiness.IsPodReady(pod) {
				return false, nil
			}
		}
		// ...and, while the source is still in rotation, until the
		// replacement has stayed Ready for the minReadySeconds window. The
		// surge stays in flight (budget slot held) for the whole wait.
		if surgeWindowApplies(findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)) &&
			!podsAvailable(surgePods, plan.MinReadySeconds, input.Now()) {
			return false, nil
		}
		// Transition Step Surge → Drain once. Subsequent passes idempotency-
		// skip inside the helper.
		if err := patchInstanceStatusSurgeStepDrain(ctx, input, inst.Index); err != nil {
			return false, fmt.Errorf("transition surge step to drain (instance=%d): %w", inst.Index, err)
		}
		for _, pod := range oldPods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterUpdateSurgeDrain, surgeDrainKey(inst.Index, newOrdinal)); err != nil {
				return false, fmt.Errorf("mark old not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		// Live drain check via per-revision routed Service — the
		// headless slice would lie because of PublishNotReadyAddresses.
		for _, pod := range oldPods {
			serviceName := drainServiceForPod(input, plan, pod)
			if serviceName == "" {
				continue
			}
			drained, err := drain.IsPodDrained(ctx, deps.Reader(), input.Key.Namespace, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}
		settled, err := waitForSurgeDrainSettle(ctx, input, plan, inst.Index)
		if err != nil {
			return false, err
		}
		if !settled {
			return false, nil
		}
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDelete BEFORE Delete, rollback via
		// ObservedDelete on error — matches recreateUpdate's contract.
		for _, pod := range oldPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete old pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Phase 3: old gone, surge is canonical. Advance ActiveOrdinal,
	// promote to Ready on the pinned target revision (the rev the
	// surge actually drove the pod to — NOT the latest target if a
	// mid-surge bump moved it). Using target.Name here would write
	// "RunningRevision=<latest-target>" while the pod is on the prior
	// rev — the X-2 corruption mode. detectUpdateTrigger picks up the
	// drift on the next reconcile and fires a fresh surge cycle.
	if err := patchInstanceStatusReadyOnRevisionWithOrdinal(ctx, input, inst.Index, surgeTargetName, newOrdinal); err != nil {
		return false, fmt.Errorf("promote surge (instance=%d): %w", inst.Index, err)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateCompleted,
		"OMENative %s surge to revision %s complete (activeOrdinal=%d)",
		instanceKey(input.Key.Component, inst.Index), surgeTargetName, newOrdinal)
	return true, nil
}

func waitForSurgeDrainSettle(ctx context.Context, input workload.ReconcileInput, plan workload.ComponentPlan, idx int32) (bool, error) {
	grace := 30 * time.Second
	if strategy := plan.UpdateStrategy.InPlaceUpdateStrategy; strategy != nil && strategy.GracePeriodSeconds != nil {
		grace = time.Duration(*strategy.GracePeriodSeconds) * time.Second
	}
	if grace <= 0 {
		return true, nil
	}

	status := findInstanceStatus(input.ObservedState.InstanceStatuses, idx)
	if status == nil || status.Operation == nil || status.Operation.Step != updateStepSurgeDrainSettle {
		if err := patchInstanceStatusSurgeStepSettle(ctx, input, idx); err != nil {
			return false, fmt.Errorf("transition surge step to settle (instance=%d): %w", idx, err)
		}
		return false, nil
	}
	if status.Operation.LastProgressAt.IsZero() {
		return false, nil
	}
	return !input.Now().Before(status.Operation.LastProgressAt.Add(grace)), nil
}

// partitionPodsBySurgeOrdinal buckets pods by the LabelPodOrdinal label
// for SurgeThenDrain's three-way decision (old/surge/stragglers).
// Stragglers are pods at neither slot — should never happen in normal
// operation; bailing out is safer than risking a wrong action.
func partitionPodsBySurgeOrdinal(pods []*corev1.Pod, oldOrdinal, newOrdinal int32) (old, surge, stragglers []*corev1.Pod) {
	for _, pod := range pods {
		ord, ok := query.PodOrdinalFromLabels(pod)
		if !ok {
			stragglers = append(stragglers, pod)
			continue
		}
		switch ord {
		case oldOrdinal:
			old = append(old, pod)
		case newOrdinal:
			surge = append(surge, pod)
		default:
			stragglers = append(stragglers, pod)
		}
	}
	return
}

// reclassifyByRevisionHash moves any pod bucketed as a surge candidate
// into the drain set unless its revision-hash label matches the PINNED
// surge target. The ordinal partition is load-bearing for the
// no-downtime invariant (the surge slot is the ALTERNATE ordinal), but
// it classifies by slot alone, and the surge slot can hold a pod on the
// wrong revision:
//
//   - a leftover from the previous cycle's promote (labeled with the
//     just-promoted RunningRevision) — keeping it as the "valid surge"
//     would let Phase 3 promote RunningRevision to the pinned target
//     while the actual pod runs a prior rev (the X-2 corruption mode);
//   - a dead pod from an exhausted attempt toward a superseded revision
//     (labeled with a rev that is neither RunningRevision nor the
//     current target) — keeping it parks the rollout forever on
//     AllPodsRuntimeReady of a pod that can never become Ready, and
//     both escalation paths skip it as superseded.
//
// Keep-in-surge is therefore exactly: the label matches the pinned
// target. A missing/empty label falls back to the ordinal
// classification so legacy pods aren't churned (partition tests pin
// this). Production pods always carry the hash createMissingPods
// stamped from the pinned target, so a mid-flight healthy surge always
// matches and is untouched.
func reclassifyByRevisionHash(surge, drainPods []*corev1.Pod, target query.RevisionID) (newSurge, newDrain []*corev1.Pod) {
	newSurge = surge[:0]
	newDrain = drainPods
	for _, pod := range surge {
		hash, ok := pod.Labels[query.LabelRevisionHash]
		if !ok || hash == "" {
			newSurge = append(newSurge, pod)
			continue
		}
		if query.RevisionFromPod(pod).Same(target) {
			newSurge = append(newSurge, pod)
			continue
		}
		newDrain = append(newDrain, pod)
	}
	return
}
