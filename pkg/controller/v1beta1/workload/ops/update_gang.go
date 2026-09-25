package ops

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
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
// for it to serve, then drains and finalizes the source. The drain is two
// steps: the source leaves rotation, and only once the endpoints confirm it is
// gone are its pods deleted. Source and target status entries retain the
// pinned revision and cleanup ownership across reconciles.
func gangSurgeUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision) (bool, error) {
	sourceIdx := inst.Index
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, plan.Component

	// 1. Recover the surge index from the in-flight Op, or allocate one
	// and stamp the source + target markers.
	src := input.ObservedState.Instance(sourceIdx)
	surging := gangSurgeSourceClaim(src)
	startingSurge := surging && src.Operation.Step == workload.UpdateStepSurge

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
		surgeMarker = input.ObservedState.Instance(surgeIdx)
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
			} else if _, err := status.RestoreGangSurgeTarget(ctx, input, src, surgeIdx, surgeTargetName, plan.InstanceReadyTimeout); err != nil {
				return false, fmt.Errorf("restore gang surge target marker (instance=%d): %w", surgeIdx, err)
			}
			return false, nil
		}
		promotedTarget := gangSurgePromotedTargetMatches(surgeMarker, surgeTargetName)
		if !status.GangSurgeTargetClaimMatches(surgeMarker, surgeTargetName) && !promotedTarget {
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
				if err := status.RetryBlockPruneOnPromote(ctx, input, surgeTargetName); err != nil {
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
			resolution, err := status.RestoreGangSurgeTarget(ctx, input, src, surgeIdx, surgeTargetName, plan.InstanceReadyTimeout)
			if err != nil {
				return false, fmt.Errorf("confirm gang surge target marker (instance=%d): %w", surgeIdx, err)
			}
			if resolution == status.GangSurgeTargetMarkerCleanup {
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
			if resolution != status.GangSurgeTargetMarkerActive {
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
		claimed, err := status.StartGangSurge(ctx, input, src, surgeIdx, surgeTargetName, plan.UpdateStrategy.Type, plan.InstanceReadyTimeout)
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
				Step:           workload.UpdateStepSurge,
				SurgeIndex:     &si,
				TargetRevision: surgeTargetName,
				Strategy:       plan.UpdateStrategy.Type,
			}
		}
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonRecreateUpdateStarted,
			"OMENative %s gang surge to revision %s (surge-index=%d)",
			workload.InstanceKey(comp, sourceIdx), surgeTargetName, surgeIdx)
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
	surgeTargets := expectedPodNamesForInstance(input, plan, surgeInst)
	// A gang member the kubelet refused to admit never ran and never
	// will, yet it still holds its name, so the gang can never complete.
	// Free it now rather than holding the surge to the operation
	// deadline. The bookkeeping belongs to the SOURCE, which owns the
	// operation; the pods and their expectations bucket on the surge.
	if recycling, rerr := recycleAdmissionRejectedTargets(ctx, deps, input, sourceIdx, surgeIdx,
		workload.InstanceOperationUpdate, surgePods, surgeTargets); rerr != nil {
		return false, fmt.Errorf("recycle rejected surge gang member (instance=%d): %w", surgeIdx, rerr)
	} else if recycling {
		return false, nil
	}
	// A member whose kubelet has stopped reporting is not dead evidence:
	// its name is freed only by the force-delete sweep, on proven node
	// death, and the source gang keeps serving meanwhile. The sweep reads
	// and events under the surge index, where the pods live.
	if holding, _, herr := recoverUnknownPhaseTargets(ctx, deps, input, surgeIdx, surgePods, surgeTargets); herr != nil {
		return false, fmt.Errorf("recover unknown-phase surge gang member (instance=%d): %w", surgeIdx, herr)
	} else if holding {
		return false, nil
	}
	existingByName := query.IndexPodsByName(surgePods)
	missing := make([]podTarget, 0)
	for _, t := range surgeTargets {
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
				if errors.Is(pgErr, workload.ErrGangNameUnusable) {
					// The surge's PodGroup name is held by another controller
					// or by an object still being collected. That is
					// classified evidence about one row, not a pass failure:
					// the escalation pass owns what happens to it, and
					// erroring out here would stall every other Instance too.
					return false, nil
				}
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
		// Do not drain until the whole replacement gang clears the shared
		// promote bar: kubelet has incorporated the serving gate into PodReady
		// and the gang has held Ready for the availability window. That
		// preserves overlap with the source, whose budget slot is held
		// meanwhile. Past Step=Surge the source is already draining.
		window := plan.MinReadySeconds
		if !surgeWindowApplies(src) {
			window = 0
		}
		if promotable, wait := query.PodSetPromotable(surgePods, window, input.Now()); !promotable {
			input.PromoteWindow.Observe(wait)
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
		src.Operation.Step = workload.UpdateStepSurgeDrain
	} else if !promotedSurgeTarget && input.FinalizeInstanceResources != nil &&
		(src.Operation.Step == workload.UpdateStepSurge || src.Operation.Step == workload.UpdateStepSurgeDrain) {
		claimed, err := status.StampStep(ctx, input, src, workload.UpdateStepSurgeDrain, true)
		if err != nil {
			return false, fmt.Errorf("stamp source gang drain (instance=%d): %w", sourceIdx, err)
		}
		if !claimed {
			return false, nil
		}
		src.Operation.Step = workload.UpdateStepSurgeDrain
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
		// Flipping the gate only starts the drain: the endpoint controller has
		// to publish the withdrawal before kube-proxy stops routing. Deleting
		// on the same pass would kill a leader the data plane still sends
		// requests to. Wait until every routed source pod has left its
		// per-revision Service's EndpointSlices, then delete the whole gang.
		// The batcher lists each Service's slices once for the gang.
		drainer := drain.NewBatcher(deps.Reader(), ns)
		drained, derr := podsDrainedFromRouting(ctx, drainer, input, plan, sourceIdx, sourcePods)
		if derr != nil {
			return false, derr
		}
		if !drained {
			return false, nil
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
		} else if err := status.StampReadyOnRevision(ctx, input, surgeIdx, surgeTargetName); err != nil {
			return false, fmt.Errorf("promote surge gang (instance=%d): %w", surgeIdx, err)
		}
	}
	if !gangSurgeSourceOwnsRemoval(src, surgeIdx) {
		return false, nil
	}
	removed, rerr := status.FinalizeAndRemove(ctx, deps, input, sourceIdx, src)
	if rerr != nil {
		return false, fmt.Errorf("finalize source Instance (instance=%d): %w", sourceIdx, rerr)
	}
	if !removed {
		return false, nil
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonRecreateUpdateCompleted,
		"OMENative %s gang surge complete: instance %d promoted to revision %s",
		workload.InstanceKey(comp, sourceIdx), surgeIdx, surgeTargetName)
	return true, nil
}

func gangSurgeSourceOwnsRemoval(row *workload.InstanceStatus, surgeIdx int32) bool {
	return gangSurgeSourceClaim(row) && *row.Operation.SurgeIndex == surgeIdx
}

func gangSurgePromotedTargetMatches(row *workload.InstanceStatus, targetRevision string) bool {
	return row != nil && row.Incarnation == 1 && row.ActiveOrdinal == 0 &&
		row.Phase == workload.InstancePhaseReady && row.RunningRevision == targetRevision &&
		row.TargetRevision == "" && row.Operation == nil
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
	if err := status.RequireOwner(input); err != nil {
		return false, err
	}

	sourceIdentity := status.Capture(source)
	targetIdentity := status.Capture(target)
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
				!sourceIdentity.Matches(currentSource) || !targetIdentity.Matches(currentTarget) {
				return false
			}
			confirmed = true
			return true
		},
	}
	err := status.Apply(ctx, input, []workload.InstanceMutation{preflight})
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
		(source.Operation.Step != workload.UpdateStepSurge && source.Operation.Step != workload.UpdateStepSurgeDrain) ||
		!gangSurgeSourceOwnsRemoval(source, target.Index) ||
		!status.GangSurgeActiveTargetClaimMatches(target, source.Operation.TargetRevision) {
		return false, nil
	}
	return status.StampGangSurgeDrainStep(ctx, input, source, target)
}

func promoteGangSurgeTarget(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	target *workload.InstanceStatus,
	targetRevision string,
) (bool, error) {
	if source == nil || target == nil || source.Operation == nil ||
		source.Operation.Step != workload.UpdateStepSurgeDrain ||
		!gangSurgeSourceOwnsRemoval(source, target.Index) ||
		!status.GangSurgeActiveTargetClaimMatches(target, targetRevision) {
		return false, nil
	}
	return status.StampGangSurgeTargetReady(ctx, input, source, target, targetRevision)
}

// resetGangSurgeSourceAfterTargetConflict releases a source claim whose
// target slot is occupied by something other than its own marker; the
// occupied target is left as it is.
func resetGangSurgeSourceAfterTargetConflict(
	ctx context.Context,
	input workload.ReconcileInput,
	source *workload.InstanceStatus,
	target *workload.InstanceStatus,
) (bool, error) {
	if source == nil || target == nil || !gangSurgeSourceOwnsRemoval(source, target.Index) ||
		status.GangSurgeTargetClaimMatches(target, source.Operation.TargetRevision) {
		return false, nil
	}
	return status.ResetGangSurgeSource(ctx, input, source, target)
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
	source := input.ObservedState.Instance(sourceIdx)
	if guardTerminalMarker && !gangSurgeSourceOwnsRemoval(source, surgeIdx) {
		return false, nil
	}
	marker := input.ObservedState.Instance(surgeIdx)
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
		guardTerminalMarker && !status.GangSurgeTargetClaimMatches(marker, source.Operation.TargetRevision)) {
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
		removed, err := status.FinalizeAndRemove(ctx, deps, input, surgeIdx, marker)
		if err != nil {
			return false, fmt.Errorf("finalize failed surge Instance (instance=%d): %w", surgeIdx, err)
		}
		if !removed {
			return false, nil
		}
		if err := recordUpdateFailureInRetryBlock(ctx, input, failedTargetRev, failureReason, workloadCaused); err != nil {
			return false, fmt.Errorf("record retry block for failed gang surge (rev=%s): %w", failedTargetRev, err)
		}
		if err := status.StampReadyOnRevision(ctx, input, sourceIdx, sourceRunningRev); err != nil {
			return false, fmt.Errorf("reset failed gang surge source (instance=%d): %w", sourceIdx, err)
		}
	}
	if failedTargetRev != "" {
		workload.RecordWarning(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned failed gang surge (surge-index=%d); resetting to revision %s for a fresh rollout",
			workload.InstanceKey(comp, sourceIdx), surgeIdx, sourceRunningRev)
	} else {
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned superseded gang surge (surge-index=%d); resetting to revision %s for a fresh rollout",
			workload.InstanceKey(comp, sourceIdx), surgeIdx, sourceRunningRev)
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
		!status.GangSurgeTargetClaimMatches(marker, source.Operation.TargetRevision) {
		return false, nil
	}
	return status.StampGangSurgeTargetCleanupStep(ctx, input, source, marker)
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
	return status.RestoreGangSurgeTargetCleanup(ctx, input, source, surgeIdx, targetRevision, timeout)
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
		!status.GangSurgeTargetClaimMatches(marker, source.Operation.TargetRevision)) {
		return false, nil
	}
	return status.ResetGangSurgeSourceAndRemoveMarker(
		ctx, deps, input, source, marker, surgeIdx, sourceRunningRev, failedTargetRev, failureReason, workloadCaused,
	)
}

func gangSurgeTargetOwnsRemoval(row *workload.InstanceStatus, requireCleanupMarker bool) bool {
	if row == nil || row.Operation == nil || row.Operation.Type != workload.InstanceOperationUpdate {
		return false
	}
	if requireCleanupMarker {
		return row.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup
	}
	return row.Operation.Step == workload.UpdateStepGangSurgeTarget
}
