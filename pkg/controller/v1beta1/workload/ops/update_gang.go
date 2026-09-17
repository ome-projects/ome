package ops

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// gangSurgeDrainKey is the serving-gate writer key for draining a gang
// surge's SOURCE Instance. Stable per source index so the drain flip is
// idempotent across the multi-pass drain window.
func gangSurgeDrainKey(sourceIdx int32) string {
	return "gang-surge-source-" + strconv.Itoa(int(sourceIdx))
}

// eventReasonGangSurgeAbandoned distinguishes a retired replacement from a
// successfully promoted one.
const eventReasonGangSurgeAbandoned workload.EventReason = "GangSurgeAbandoned"

// gangSurgeUpdate creates a replacement gang at a fresh Instance index, waits
// for it to serve, then drains and finalizes the source. Source and target
// status entries retain the pinned revision and cleanup ownership across
// reconciles.
func gangSurgeUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision) (bool, error) {
	sourceIdx := inst.Index
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, plan.Component

	// 1. Recover the surge index from the in-flight Op, or allocate one
	// and stamp the source + target markers.
	src := findInstanceStatus(input.ObservedState.InstanceStatuses, sourceIdx)
	surging := src != nil && src.Operation != nil &&
		src.Operation.Type == workload.InstanceOperationUpdate &&
		isSurgeUpdateStep(src.Operation.Step) && src.Operation.SurgeIndex != nil
	startingSurge := surging && src.Operation.Step == updateStepSurge

	// An in-flight operation stays pinned to the revision stamped at start.
	// A newer desired revision is handled by a subsequent update.
	surgeTargetName := target.Name
	if surging && src.Operation.TargetRevision != "" {
		surgeTargetName = src.Operation.TargetRevision
	}

	var surgeIdx int32
	if surging {
		surgeIdx = *src.Operation.SurgeIndex
	}
	promotedSurgeTarget := false
	var surgeMarker *workload.InstanceStatus
	if surging {
		surgeMarker = findInstanceStatus(input.ObservedState.InstanceStatuses, surgeIdx)
		if surgeMarker == nil {
			cleanup := startingSurge && src.Phase == workload.InstancePhaseFailed
			if startingSurge && !cleanup && src.Operation.TargetRevision != target.Name {
				surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, surgeIdx)
				if err != nil {
					return false, fmt.Errorf("list missing surge target pods (instance=%d): %w", surgeIdx, err)
				}
				cleanup = int32(len(surgePods)) < inst.TotalPods() || !query.AllPodsRuntimeReady(surgePods)
			}
			if cleanup && (input.ApplyInstanceMutationsWithRetryBlock != nil || input.FinalizeInstanceResources != nil) {
				if _, err := restoreGangSurgeTargetCleanup(ctx, input, src, surgeIdx, surgeTargetName, plan.InstanceReadyTimeout); err != nil {
					return false, fmt.Errorf("restore gang surge target cleanup marker (instance=%d): %w", surgeIdx, err)
				}
			} else if _, err := restoreInstanceStatusGangSurgeTarget(ctx, input, src, surgeIdx, surgeTargetName, plan.InstanceReadyTimeout); err != nil {
				return false, fmt.Errorf("restore gang surge target marker (instance=%d): %w", surgeIdx, err)
			}
			return false, nil
		}
		promotedTarget := gangSurgePromotedTargetMatches(surgeMarker, surgeTargetName)
		if !gangSurgeTargetClaimMatches(surgeMarker, surgeTargetName) && !promotedTarget {
			if _, err := resetGangSurgeSourceAfterTargetConflict(ctx, input, src, surgeMarker); err != nil {
				return false, fmt.Errorf("reset gang surge with occupied target (source=%d target=%d): %w", sourceIdx, surgeIdx, err)
			}
			return false, nil
		}
		if promotedTarget {
			// A promoted target is safe to accept only after the source pods are
			// gone. Nonzero source pods make an operation-less Ready slot
			// insufficient proof that it belongs to this surge.
			sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, sourceIdx)
			if err != nil {
				return false, fmt.Errorf("list source gang pods for promoted target recovery (instance=%d): %w", sourceIdx, err)
			}
			if len(sourcePods) != 0 {
				if !startingSurge {
					return false, nil
				}
				if _, err := resetGangSurgeSourceAfterTargetConflict(ctx, input, src, surgeMarker); err != nil {
					return false, fmt.Errorf("reset gang surge with occupied promoted target (source=%d target=%d): %w", sourceIdx, surgeIdx, err)
				}
				return false, nil
			}
			if input.ApplyInstanceMutationsWithRetryBlock != nil {
				confirmed, err := confirmPromotedGangSurgeTarget(ctx, input, src, surgeMarker)
				if err != nil {
					return false, fmt.Errorf("confirm promoted gang surge target (instance=%d): %w", surgeIdx, err)
				}
				if !confirmed {
					return false, nil
				}
				if err := pruneRetryBlockOnPromote(ctx, input, surgeTargetName); err != nil {
					return false, fmt.Errorf("prune retry block after gang promotion (revision=%s): %w", surgeTargetName, err)
				}
			}
			promotedSurgeTarget = true
		}
		if startingSurge && !promotedTarget && surgeMarker.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup {
			failedTargetRev, failureReason, workloadCaused := "", "", false
			if src.Phase == workload.InstancePhaseFailed {
				failedTargetRev = src.Operation.TargetRevision
				failureReason = instanceFailureReason(src, "gang surge abandoned before the target became Ready")
				workloadCaused = instanceFailureWorkloadCaused(src)
			}
			return abandonFailedGangSurge(ctx, deps, input, plan, sourceIdx, surgeIdx, src.RunningRevision, failedTargetRev, failureReason, workloadCaused)
		}
		if !promotedTarget && (input.ApplyInstanceMutationsWithRetryBlock != nil || input.FinalizeInstanceResources != nil) {
			resolution, err := restoreInstanceStatusGangSurgeTarget(ctx, input, src, surgeIdx, surgeTargetName, plan.InstanceReadyTimeout)
			if err != nil {
				return false, fmt.Errorf("confirm gang surge target marker (instance=%d): %w", surgeIdx, err)
			}
			if resolution == gangSurgeTargetMarkerCleanup {
				if !startingSurge {
					return false, nil
				}
				failedTargetRev, failureReason, workloadCaused := "", "", false
				if src.Phase == workload.InstancePhaseFailed {
					failedTargetRev = src.Operation.TargetRevision
					failureReason = instanceFailureReason(src, "gang surge abandoned before the target became Ready")
					workloadCaused = instanceFailureWorkloadCaused(src)
				}
				return abandonFailedGangSurge(ctx, deps, input, plan, sourceIdx, surgeIdx, src.RunningRevision, failedTargetRev, failureReason, workloadCaused)
			}
			if resolution != gangSurgeTargetMarkerActive {
				return false, nil
			}
		}
	}

	// A failed replacement is retired while the source remains on its running
	// revision. Retry policy controls the next attempt.
	if startingSurge && !promotedSurgeTarget && src.Phase == workload.InstancePhaseFailed {
		// Record failure against the revision owned by this operation.
		return abandonFailedGangSurge(ctx, deps, input, plan, sourceIdx, *src.Operation.SurgeIndex, src.RunningRevision,
			src.Operation.TargetRevision, instanceFailureReason(src, "gang surge abandoned before the target became Ready"),
			instanceFailureWorkloadCaused(src))
	}

	// Retire an incomplete replacement when the desired revision changes. A
	// ready replacement finishes on its pinned revision first.
	if startingSurge && !promotedSurgeTarget && src.Operation.TargetRevision != target.Name {
		surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, *src.Operation.SurgeIndex)
		if err != nil {
			return false, fmt.Errorf("list surge gang pods for supersede check (instance=%d): %w", *src.Operation.SurgeIndex, err)
		}
		if int32(len(surgePods)) < inst.TotalPods() || !query.AllPodsRuntimeReady(surgePods) {
			// A spec change is not a failure of the retired revision.
			return abandonFailedGangSurge(ctx, deps, input, plan, sourceIdx, *src.Operation.SurgeIndex, src.RunningRevision, "", "", false)
		}
	}

	if !surging {
		surgeIdx = workload.AllocateSurgeIndex(input.ObservedState.InstanceStatuses)
		claimed, err := startGangSurge(ctx, input, src, surgeIdx, surgeTargetName, plan.InstanceReadyTimeout)
		if err != nil {
			return false, fmt.Errorf("claim gang surge pair (source=%d target=%d): %w", sourceIdx, surgeIdx, err)
		}
		if !claimed {
			return false, nil
		}
		// Reserve the index in this reconcile's snapshot so sibling updates cannot
		// allocate the same replacement slot.
		if src != nil {
			si := surgeIdx
			src.Operation = &workload.InstanceOperation{
				Type:           workload.InstanceOperationUpdate,
				Step:           updateStepSurge,
				SurgeIndex:     &si,
				TargetRevision: surgeTargetName,
			}
		}
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateStarted,
			"OMENative %s gang surge to revision %s (surge-index=%d)",
			instanceKey(comp, sourceIdx), surgeTargetName, surgeIdx)
		// Requeue so the next pass sees k in the plan — EnsurePodGroups
		// creates k's PodGroup before we create its pods.
		return false, nil
	}

	// 2. Create the replacement gang at k on the target revision (leader
	// + workers; createMissingPods picks the per-runner spec).
	surgeInst := workload.InstancePlan{
		Index:         surgeIdx,
		Incarnation:   1,
		Runners:       append([]workload.RunnerPlan(nil), inst.Runners...),
		ExcludedNodes: inst.ExcludedNodes, // Exclusion memory follows the instance through surge replacement.
	}
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list surge gang pods (instance=%d): %w", surgeIdx, err)
	}
	existingByName := query.IndexPodsByName(surgePods)
	missing := make([]podTarget, 0)
	for _, t := range expectedPodNamesForInstance(input, plan, surgeInst) {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !deps.ExpectationsCache().Satisfied(ns, owner, comp, surgeIdx) {
			return false, nil
		}
		// Announce the surge gang's PodGroup before its pods. The top-level
		// EnsurePodGroups pass keys off the plan, which only pins the surge
		// index once its GangSurgeTarget status round-trips into ObservedState
		// — a lag a gang-aware scheduler (e.g. coscheduler) turns into
		// "PodGroup not found" on the freshly-created surge pods. Ensuring it
		// here makes PodGroup-before-pods hold regardless of that timing. The
		// callback is wired by the caller (gang package owns the podgroup dep,
		// keeping ops free of it); nil callback / single-pod surge / absent
		// CRD all no-op inside EnsureGangPodGroup.
		renderPlan := plan
		if deps.EnsureGangPodGroup != nil {
			effectiveTopology, pgErr := deps.EnsureGangPodGroup(ctx, input, plan, surgeInst)
			if pgErr != nil {
				return false, fmt.Errorf("ensure surge gang PodGroup (instance=%d): %w", surgeIdx, pgErr)
			}
			renderPlan.InstanceTopologyKeys = cloneInstanceTopologyKeys(plan.InstanceTopologyKeys)
			renderPlan.InstanceTopologyKeys[surgeIdx] = effectiveTopology
		}
		// Stamp the pinned in-flight rev hash (NOT the latest target) so
		// the gang's pods match the revision this surge committed to and
		// the per-revision drain Service selects them. See surgeTargetName.
		if _, cerr := createMissingPods(ctx, deps, input, renderPlan, surgeInst, surgeIdx, missing, query.RevisionFromName(surgeTargetName)); cerr != nil {
			if createRejectionHandled(cerr) {
				return false, nil
			}
			return false, fmt.Errorf("create surge gang (instance=%d): %w", surgeIdx, cerr)
		}
		return false, nil
	}

	// ContainersReady permits setting the serving gate; waiting for PodReady
	// here would deadlock because PodReady includes that gate.
	if int32(len(surgePods)) < inst.TotalPods() || !query.AllPodsRuntimeReady(surgePods) {
		return false, nil
	}
	for _, pod := range surgePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("serving=True on surge pod %s: %w", pod.Name, err)
		}
	}

	// 4. Drain the source gang once the replacement is in rotation.
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, sourceIdx)
	if err != nil {
		return false, fmt.Errorf("list source gang pods (instance=%d): %w", sourceIdx, err)
	}
	if len(sourcePods) > 0 {
		// Do not drain until kubelet has incorporated the serving gate into
		// PodReady; this preserves overlap with the source.
		for _, pod := range surgePods {
			if !podreadiness.IsPodReady(pod) {
				return false, nil
			}
		}
		// Nor, while the source is still in rotation, until the whole
		// replacement gang has stayed Ready for the minReadySeconds window;
		// the surge budget slot is held meanwhile.
		if surgeWindowApplies(src) && !podsAvailable(surgePods, plan.MinReadySeconds, input.Now()) {
			return false, nil
		}
	}
	if !promotedSurgeTarget && input.ApplyInstanceMutationsWithRetryBlock != nil {
		claimed, err := claimGangSurgeDrain(ctx, input, src, surgeMarker)
		if err != nil {
			return false, fmt.Errorf("claim source gang drain (instance=%d): %w", sourceIdx, err)
		}
		if !claimed {
			return false, nil
		}
		src.Operation.Step = updateStepSurgeDrain
	} else if !promotedSurgeTarget && input.FinalizeInstanceResources != nil &&
		(src.Operation.Step == updateStepSurge || src.Operation.Step == updateStepSurgeDrain) {
		claimed, err := transitionTerminalOperationStep(ctx, input, src, updateStepSurgeDrain, true)
		if err != nil {
			return false, fmt.Errorf("stamp source gang drain (instance=%d): %w", sourceIdx, err)
		}
		if !claimed {
			return false, nil
		}
		src.Operation.Step = updateStepSurgeDrain
	}
	if len(sourcePods) > 0 {
		// Remove the source from rotation before deletion so termination grace
		// does not leave both generations serving.
		for _, pod := range sourcePods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod,
				podreadiness.WriterUpdateSurgeDrain, gangSurgeDrainKey(sourceIdx)); err != nil {
				return false, fmt.Errorf("drain source gang pod %s: %w", pod.Name, err)
			}
		}
		if !deps.ExpectationsCache().Satisfied(ns, owner, comp, sourceIdx) {
			return false, nil
		}
		for _, pod := range sourcePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(ns, owner, comp, sourceIdx, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(ns, owner, comp, sourceIdx)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete source pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Promote the replacement on its pinned revision. A newer desired revision
	// remains visible as RunningRevision != target and starts another update.
	if !promotedSurgeTarget {
		if input.ApplyInstanceMutationsWithRetryBlock != nil {
			promoted, err := promoteGangSurgeTarget(ctx, input, src, surgeMarker, surgeTargetName)
			if err != nil {
				return false, fmt.Errorf("promote surge gang (instance=%d): %w", surgeIdx, err)
			}
			if !promoted {
				return false, nil
			}
		} else if err := patchInstanceStatusReadyOnRevision(ctx, input, surgeIdx, surgeTargetName); err != nil {
			return false, fmt.Errorf("promote surge gang (instance=%d): %w", surgeIdx, err)
		}
	}
	if !gangSurgeSourceOwnsRemoval(src, surgeIdx) {
		return false, nil
	}
	removed, rerr := finalizeAndRemoveInstance(ctx, deps, input, sourceIdx, src)
	if rerr != nil {
		return false, fmt.Errorf("finalize source Instance (instance=%d): %w", sourceIdx, rerr)
	}
	if !removed {
		return false, nil
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateCompleted,
		"OMENative %s gang surge complete: instance %d promoted to revision %s",
		instanceKey(comp, sourceIdx), surgeIdx, surgeTargetName)
	return true, nil
}

func gangSurgeSourceOwnsRemoval(status *workload.InstanceStatus, surgeIdx int32) bool {
	return status != nil && status.Operation != nil &&
		status.Operation.Type == workload.InstanceOperationUpdate &&
		isSurgeUpdateStep(status.Operation.Step) &&
		status.Operation.SurgeIndex != nil && *status.Operation.SurgeIndex == surgeIdx
}

func gangSurgePromotedTargetMatches(status *workload.InstanceStatus, targetRevision string) bool {
	return status != nil && status.Incarnation == 1 && status.ActiveOrdinal == 0 &&
		status.Phase == workload.InstancePhaseReady && status.RunningRevision == targetRevision &&
		status.TargetRevision == "" && status.Operation == nil
}

func confirmPromotedGangSurgeTarget(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	target *workload.InstanceStatus,
) (bool, error) {
	if source == nil || target == nil || !gangSurgeSourceOwnsRemoval(source, target.Index) ||
		!gangSurgePromotedTargetMatches(target, source.Operation.TargetRevision) {
		return false, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}

	sourceIdentity := captureTerminalInstanceIdentity(source)
	targetIdentity := captureTerminalInstanceIdentity(target)
	ownerUID := input.OwnerObject.GetUID()
	confirmed := false
	preflight := workload.InstanceMutation{
		Index:  source.Index,
		Mutate: func(*workload.InstanceStatus) bool { return false },
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			confirmed = false
			if snapshot.OwnerUID != ownerUID {
				return false
			}
			currentSource, sourceFound := snapshot.Instances[source.Index]
			currentTarget, targetFound := snapshot.Instances[target.Index]
			if !sourceFound || !targetFound ||
				!sourceIdentity.matches(currentSource) || !targetIdentity.matches(currentTarget) {
				return false
			}
			confirmed = true
			return true
		},
	}
	err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{preflight})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return confirmed, nil
}

func claimGangSurgeDrain(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	target *workload.InstanceStatus,
) (bool, error) {
	if source == nil || target == nil || source.Operation == nil ||
		(source.Operation.Step != updateStepSurge && source.Operation.Step != updateStepSurgeDrain) ||
		!gangSurgeSourceOwnsRemoval(source, target.Index) ||
		!gangSurgeActiveTargetClaimMatches(target, source.Operation.TargetRevision) {
		return false, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}

	before := captureTerminalInstanceIdentity(source)
	after := before
	after.operation.step = updateStepSurgeDrain
	targetIdentity := captureTerminalInstanceIdentity(target)
	ownerUID := input.OwnerObject.GetUID()
	confirmed := false
	committed := false
	mutation := workload.InstanceMutation{
		Index: source.Index,
		Mutate: func(status *workload.InstanceStatus) bool {
			if after.matches(*status) {
				confirmed = true
				return false
			}
			if !before.matches(*status) {
				return false
			}
			status.Operation.Step = updateStepSurgeDrain
			status.Operation.LastProgressAt = metav1.NewTime(input.Now())
			return true
		},
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			confirmed = false
			if snapshot.OwnerUID != ownerUID {
				return false
			}
			currentSource, sourceFound := snapshot.Instances[source.Index]
			currentTarget, targetFound := snapshot.Instances[target.Index]
			if !sourceFound || !targetFound || !targetIdentity.matches(currentTarget) ||
				!gangSurgeActiveTargetClaimMatches(&currentTarget, source.Operation.TargetRevision) {
				return false
			}
			if after.matches(currentSource) {
				confirmed = true
				return true
			}
			return before.matches(currentSource)
		},
		Postcondition: func(status *workload.InstanceStatus) bool {
			return status != nil && after.matches(*status)
		},
		OnCommit: func(_, _ *workload.InstanceStatus) {
			committed = true
		},
	}
	err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{mutation})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return committed || confirmed, nil
}

func promoteGangSurgeTarget(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	target *workload.InstanceStatus,
	targetRevision string,
) (bool, error) {
	if source == nil || target == nil || source.Operation == nil ||
		source.Operation.Step != updateStepSurgeDrain ||
		!gangSurgeSourceOwnsRemoval(source, target.Index) ||
		!gangSurgeActiveTargetClaimMatches(target, targetRevision) {
		return false, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}

	sourceIdentity := captureTerminalInstanceIdentity(source)
	targetIdentity := captureTerminalInstanceIdentity(target)
	ownerUID := input.OwnerObject.GetUID()
	promoted := false
	mutation := createStatusReadyOnRevisionMutation(target.Index, targetRevision, input.Now())
	mutation.BatchPrecondition = func(snapshot workload.InstanceMutationSnapshot) bool {
		if snapshot.OwnerUID != ownerUID {
			return false
		}
		currentSource, sourceFound := snapshot.Instances[source.Index]
		currentTarget, targetFound := snapshot.Instances[target.Index]
		return sourceFound && targetFound &&
			sourceIdentity.matches(currentSource) && targetIdentity.matches(currentTarget) &&
			gangSurgeActiveTargetClaimMatches(&currentTarget, targetRevision)
	}
	mutation.Postcondition = func(status *workload.InstanceStatus) bool {
		return status != nil && status.Index == target.Index &&
			status.Incarnation == target.Incarnation &&
			status.ActiveOrdinal == target.ActiveOrdinal &&
			status.Phase == workload.InstancePhaseReady &&
			status.RunningRevision == targetRevision &&
			status.TargetRevision == "" && status.Operation == nil
	}
	mutation.OnCommit = func(_, _ *workload.InstanceStatus) {
		promoted = true
	}
	err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{mutation})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !promoted {
		return false, nil
	}
	if err := pruneRetryBlockOnPromote(ctx, input, targetRevision); err != nil {
		return false, err
	}
	return true, nil
}

// resetGangSurgeSourceAfterTargetConflict releases a source claim without
// changing the occupied target. The strong path guards both lifecycle
// identities in one authoritative snapshot; compatibility adapters confirm
// the target and source independently through their fresh mutation reads.
func resetGangSurgeSourceAfterTargetConflict(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	target *workload.InstanceStatus,
) (bool, error) {
	if source == nil || target == nil || !gangSurgeSourceOwnsRemoval(source, target.Index) ||
		gangSurgeTargetClaimMatches(target, source.Operation.TargetRevision) {
		return false, nil
	}

	sourceIdentity := captureTerminalInstanceIdentity(source)
	targetIdentity := captureTerminalInstanceIdentity(target)
	reset := createStatusReadyOnRevisionMutation(source.Index, source.RunningRevision, input.Now())
	reset.Postcondition = func(status *workload.InstanceStatus) bool {
		return status != nil && status.Index == source.Index &&
			status.Incarnation == source.Incarnation &&
			status.Phase == workload.InstancePhaseReady &&
			status.RunningRevision == source.RunningRevision &&
			status.TargetRevision == "" && status.Operation == nil &&
			status.ActiveOrdinal == source.ActiveOrdinal
	}

	if input.ApplyInstanceMutationsWithRetryBlock != nil {
		if err := validateTerminalMutationOwner(input); err != nil {
			return false, err
		}
		ownerUID := input.OwnerObject.GetUID()
		committed := false
		reset.BatchPrecondition = func(snapshot workload.InstanceMutationSnapshot) bool {
			if snapshot.OwnerUID != ownerUID {
				return false
			}
			currentSource, sourceFound := snapshot.Instances[source.Index]
			currentTarget, targetFound := snapshot.Instances[target.Index]
			return sourceFound && targetFound &&
				sourceIdentity.matches(currentSource) && targetIdentity.matches(currentTarget)
		}
		reset.OnCommit = func(_, _ *workload.InstanceStatus) {
			committed = true
		}
		err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{reset})
		if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return committed, nil
	}

	if input.MutateInstance == nil {
		return false, fmt.Errorf("gang surge target conflict requires a status mutation adapter")
	}
	targetMatched := false
	if err := input.MutateInstance(ctx, target.Index, func(current *workload.InstanceStatus) bool {
		targetMatched = targetIdentity.matches(*current)
		return false
	}); err != nil {
		return false, err
	}
	if !targetMatched {
		return false, nil
	}
	sourceMatched := false
	if err := input.MutateInstance(ctx, source.Index, func(current *workload.InstanceStatus) bool {
		sourceMatched = sourceIdentity.matches(*current)
		if !sourceMatched {
			return false
		}
		return reset.Mutate(current)
	}); err != nil {
		return false, err
	}
	return sourceMatched, nil
}

func cloneInstanceTopologyKeys(in map[int32]string) map[int32]string {
	out := make(map[int32]string, len(in)+1)
	for index, key := range in {
		out[index] = key
	}
	return out
}

// abandonFailedGangSurge drains and finalizes a retired replacement, then
// atomically removes its marker and resets the source. A failed revision also
// records its RetryBlock in that status transition: workloadCaused (the
// source's LastFailure evidence, see instanceFailureWorkloadCaused) decides
// whether the wave charges the revision's retry ladder or only paces the
// next attempt, and both abandon tails apply that one verdict.
func abandonFailedGangSurge(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx, surgeIdx int32, sourceRunningRev, failedTargetRev, failureReason string, workloadCaused bool) (bool, error) {
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, plan.Component
	guardTerminalMarker := input.ApplyInstanceMutationsWithRetryBlock != nil || input.FinalizeInstanceResources != nil
	source := findInstanceStatus(input.ObservedState.InstanceStatuses, sourceIdx)
	if guardTerminalMarker && !gangSurgeSourceOwnsRemoval(source, surgeIdx) {
		return false, nil
	}
	marker := findInstanceStatus(input.ObservedState.InstanceStatuses, surgeIdx)
	if guardTerminalMarker {
		if marker == nil {
			if _, err := restoreGangSurgeTargetCleanup(
				ctx, input, source, surgeIdx, source.Operation.TargetRevision, plan.InstanceReadyTimeout,
			); err != nil {
				return false, fmt.Errorf("restore gang surge target cleanup marker (instance=%d): %w", surgeIdx, err)
			}
			return false, nil
		}
		claimed, err := transitionGangSurgeTargetCleanup(ctx, input, source, marker)
		if err != nil {
			return false, fmt.Errorf("stamp failed surge cleanup (instance=%d): %w", surgeIdx, err)
		}
		if !claimed {
			return false, nil
		}
		marker.Operation.Step = workload.UpdateStepGangSurgeTargetCleanup
	}
	if marker != nil && (!gangSurgeTargetOwnsRemoval(marker, guardTerminalMarker) ||
		guardTerminalMarker && !gangSurgeTargetClaimMatches(marker, source.Operation.TargetRevision)) {
		return false, nil
	}

	// 1. Delete the wedged surge gang's pods.
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list failed surge gang pods (instance=%d): %w", surgeIdx, err)
	}
	if len(surgePods) > 0 {
		if !deps.ExpectationsCache().Satisfied(ns, owner, comp, surgeIdx) {
			return false, nil
		}
		for _, pod := range surgePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(ns, owner, comp, surgeIdx, 1)
			if err := deps.Client.Delete(ctx, pod); err != nil {
				deps.ExpectationsCache().ObservedDelete(ns, owner, comp, surgeIdx)
				if apierrors.IsNotFound(err) {
					continue
				}
				return false, fmt.Errorf("delete failed surge pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// 2. Finalize the surge index and reset its source. The strong path commits
	// both status changes atomically so target-marker absence can never be
	// mistaken for an interrupted target-stamp and restored as active work.
	if guardTerminalMarker {
		complete, err := finalizeAndResetAbandonedGangSurge(
			ctx, deps, input, source, marker, surgeIdx, sourceRunningRev, failedTargetRev, failureReason, workloadCaused,
		)
		if err != nil {
			return false, fmt.Errorf("finalize abandoned gang surge (instance=%d): %w", surgeIdx, err)
		}
		if !complete {
			return false, nil
		}
	} else {
		removed, err := finalizeAndRemoveInstance(ctx, deps, input, surgeIdx, marker)
		if err != nil {
			return false, fmt.Errorf("finalize failed surge Instance (instance=%d): %w", surgeIdx, err)
		}
		if !removed {
			return false, nil
		}
		if err := recordUpdateFailureInRetryBlock(ctx, input, failedTargetRev, failureReason, workloadCaused); err != nil {
			return false, fmt.Errorf("record retry block for failed gang surge (rev=%s): %w", failedTargetRev, err)
		}
		if err := patchInstanceStatusReadyOnRevision(ctx, input, sourceIdx, sourceRunningRev); err != nil {
			return false, fmt.Errorf("reset failed gang surge source (instance=%d): %w", sourceIdx, err)
		}
	}
	if failedTargetRev != "" {
		recordWarning(deps.Recorder, eventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned failed gang surge (surge-index=%d); resetting to revision %s for a fresh rollout",
			instanceKey(comp, sourceIdx), surgeIdx, sourceRunningRev)
	} else {
		recordNormal(deps.Recorder, eventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned superseded gang surge (surge-index=%d); resetting to revision %s for a fresh rollout",
			instanceKey(comp, sourceIdx), surgeIdx, sourceRunningRev)
	}
	return false, nil
}

func transitionGangSurgeTargetCleanup(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	marker *workload.InstanceStatus,
) (bool, error) {
	if source == nil || marker == nil || !gangSurgeSourceOwnsRemoval(source, marker.Index) ||
		!gangSurgeTargetClaimMatches(marker, source.Operation.TargetRevision) {
		return false, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}

	sourceIdentity := captureTerminalInstanceIdentity(source)
	before := captureTerminalInstanceIdentity(marker)
	after := before
	after.operation.step = workload.UpdateStepGangSurgeTargetCleanup
	ownerUID := input.OwnerObject.GetUID()
	confirmed := false
	committed := false
	mutation := workload.InstanceMutation{
		Index: marker.Index,
		Mutate: func(status *workload.InstanceStatus) bool {
			if after.matches(*status) {
				confirmed = true
				return false
			}
			if !before.matches(*status) {
				return false
			}
			status.Operation.Step = workload.UpdateStepGangSurgeTargetCleanup
			return true
		},
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			confirmed = false
			if snapshot.OwnerUID != ownerUID {
				return false
			}
			currentSource, found := snapshot.Instances[source.Index]
			if !found || !sourceIdentity.matches(currentSource) {
				return false
			}
			currentMarker, found := snapshot.Instances[marker.Index]
			if !found {
				return false
			}
			if after.matches(currentMarker) {
				confirmed = true
				return true
			}
			return before.matches(currentMarker)
		},
		Postcondition: func(status *workload.InstanceStatus) bool {
			return status != nil && after.matches(*status)
		},
		OnCommit: func(_, _ *workload.InstanceStatus) {
			committed = true
		},
	}
	err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{mutation})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return committed || confirmed, nil
}

func restoreGangSurgeTargetCleanup(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	surgeIdx int32,
	targetRevision string,
	timeout time.Duration,
) (bool, error) {
	if source == nil || !gangSurgeSourceOwnsRemoval(source, surgeIdx) ||
		source.Operation.TargetRevision != targetRevision {
		return false, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}

	now := metav1.NewTime(input.Now())
	desired := workload.InstanceStatus{
		Index:          surgeIdx,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		TargetRevision: targetRevision,
		Operation: &workload.InstanceOperation{
			ID:             fmt.Sprintf("gangsurgetarget-%d-%d", surgeIdx, now.Unix()),
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepGangSurgeTargetCleanup,
			TargetRevision: targetRevision,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
		},
	}
	sourceIdentity := captureTerminalInstanceIdentity(source)
	ownerUID := input.OwnerObject.GetUID()
	confirmed := false
	committed := false
	mutation := workload.InstanceMutation{
		Index: surgeIdx,
		Mutate: func(status *workload.InstanceStatus) bool {
			if gangSurgeCleanupTargetClaimMatches(status, targetRevision) {
				confirmed = true
				return false
			}
			if gangSurgeActiveTargetClaimMatches(status, targetRevision) {
				status.Operation.Step = workload.UpdateStepGangSurgeTargetCleanup
				return true
			}
			if !emptyGangSurgeTargetSlot(status) {
				return false
			}
			*status = cloneTerminalStatusValue(desired)
			return true
		},
		BatchPrecondition: func(snapshot workload.InstanceMutationSnapshot) bool {
			confirmed = false
			if snapshot.OwnerUID != ownerUID {
				return false
			}
			currentSource, found := snapshot.Instances[source.Index]
			if !found || !sourceIdentity.matches(currentSource) {
				return false
			}
			currentTarget, found := snapshot.Instances[surgeIdx]
			if !found {
				return true
			}
			if gangSurgeCleanupTargetClaimMatches(&currentTarget, targetRevision) {
				confirmed = true
				return true
			}
			return gangSurgeActiveTargetClaimMatches(&currentTarget, targetRevision)
		},
		Postcondition: func(status *workload.InstanceStatus) bool {
			return status != nil && status.Index == surgeIdx &&
				gangSurgeCleanupTargetClaimMatches(status, targetRevision)
		},
		OnCommit: func(_, _ *workload.InstanceStatus) {
			committed = true
		},
	}
	err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{mutation})
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return committed || confirmed, nil
}

func finalizeAndResetAbandonedGangSurge(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	marker *workload.InstanceStatus,
	surgeIdx int32,
	sourceRunningRev string,
	failedTargetRev string,
	failureReason string,
	workloadCaused bool,
) (bool, error) {
	if source == nil || !gangSurgeSourceOwnsRemoval(source, surgeIdx) {
		return false, nil
	}
	if marker != nil && (!gangSurgeTargetOwnsRemoval(marker, true) ||
		!gangSurgeTargetClaimMatches(marker, source.Operation.TargetRevision)) {
		return false, nil
	}
	if err := validateTerminalMutationOwner(input); err != nil {
		return false, err
	}

	ownerUID := input.OwnerObject.GetUID()
	sourceIdentity := captureTerminalInstanceIdentity(source)
	if marker == nil {
		return false, nil
	}
	markerIdentity := captureTerminalInstanceIdentity(marker)
	guard := func(snapshot workload.InstanceMutationSnapshot) bool {
		if snapshot.OwnerUID != ownerUID {
			return false
		}
		currentSource, found := snapshot.Instances[source.Index]
		if !found || !sourceIdentity.matches(currentSource) {
			return false
		}
		currentMarker, found := snapshot.Instances[surgeIdx]
		return found && markerIdentity.matches(currentMarker)
	}

	preflight := workload.InstanceMutation{
		Index:             source.Index,
		Mutate:            func(*workload.InstanceStatus) bool { return false },
		BatchPrecondition: guard,
	}
	if err := applyTerminalInstanceMutations(ctx, input, []workload.InstanceMutation{preflight}); err != nil {
		if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
			return false, nil
		}
		return false, err
	}
	if sourceRunningRev != "" && workload.FindRetryBlock(input.ObservedState.RetryBlocks, sourceRunningRev) != nil {
		if err := pruneRetryBlockOnPromote(ctx, input, sourceRunningRev); err != nil {
			return false, err
		}
	}

	if input.FinalizeInstanceResources != nil {
		complete, err := input.FinalizeInstanceResources(ctx, surgeIdx)
		if err != nil {
			return false, err
		}
		if !complete {
			return false, nil
		}
	}

	committed := false
	reset := createStatusReadyOnRevisionMutation(source.Index, sourceRunningRev, input.Now())
	reset.BatchPrecondition = guard
	reset.Postcondition = func(status *workload.InstanceStatus) bool {
		return status != nil && status.Index == source.Index &&
			status.Incarnation == source.Incarnation &&
			status.Phase == workload.InstancePhaseReady &&
			status.RunningRevision == sourceRunningRev &&
			status.TargetRevision == "" && status.Operation == nil &&
			status.ActiveOrdinal == source.ActiveOrdinal
	}
	reset.OnCommit = func(_, _ *workload.InstanceStatus) {
		committed = true
		deps.ExpectationsCache().Forget(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, surgeIdx)
	}
	removeMarker := workload.InstanceMutation{Index: surgeIdx, Remove: true}

	var retryRevision string
	var mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition
	heldAttempts := int32(0)
	if input.MutateRetryBlock != nil && failedTargetRev != "" {
		retryRevision = failedTargetRev
		now := metav1.NewTime(input.Now())
		mutateRetryBlock = func(block *workload.RetryBlock) workload.RetryBlockDisposition {
			var disposition workload.RetryBlockDisposition
			disposition, heldAttempts = workload.ApplyUpdateFailureToRetryBlock(
				block, input.UpdateRetryPolicy, now, failureReason, workloadCaused,
			)
			return disposition
		}
	}
	priorOnCommit := reset.OnCommit
	reset.OnCommit = func(previous, current *workload.InstanceStatus) {
		priorOnCommit(previous, current)
		if heldAttempts > 0 && input.WarnRetryHeld != nil {
			input.WarnRetryHeld(failedTargetRev, heldAttempts, failureReason)
		}
	}
	err := input.ApplyInstanceMutationsWithRetryBlock(
		ctx,
		[]workload.InstanceMutation{reset, removeMarker},
		retryRevision,
		mutateRetryBlock,
	)
	if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return committed, nil
}

func gangSurgeTargetOwnsRemoval(status *workload.InstanceStatus, requireCleanupMarker bool) bool {
	if status == nil || status.Operation == nil || status.Operation.Type != workload.InstanceOperationUpdate {
		return false
	}
	if requireCleanupMarker {
		return status.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup
	}
	return status.Operation.Step == workload.UpdateStepGangSurgeTarget
}

func gangSurgeTargetMatches(status *workload.InstanceStatus, targetRevision string) bool {
	return gangSurgeActiveTargetClaimMatches(status, targetRevision) &&
		status.Phase == workload.InstancePhaseCreating &&
		status.Operation.Step == workload.UpdateStepGangSurgeTarget
}

func gangSurgeTargetClaimMatches(status *workload.InstanceStatus, targetRevision string) bool {
	return gangSurgeActiveTargetClaimMatches(status, targetRevision) ||
		gangSurgeCleanupTargetClaimMatches(status, targetRevision)
}

func gangSurgeActiveTargetClaimMatches(status *workload.InstanceStatus, targetRevision string) bool {
	return status != nil && status.TargetRevision == targetRevision && status.Operation != nil &&
		status.Operation.Type == workload.InstanceOperationUpdate &&
		status.Operation.Step == workload.UpdateStepGangSurgeTarget &&
		status.Operation.TargetRevision == targetRevision
}

func gangSurgeCleanupTargetClaimMatches(status *workload.InstanceStatus, targetRevision string) bool {
	return status != nil && status.TargetRevision == targetRevision && status.Operation != nil &&
		status.Operation.Type == workload.InstanceOperationUpdate &&
		status.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup &&
		status.Operation.TargetRevision == targetRevision
}
