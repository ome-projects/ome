package ops

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

// ReplacementLostReason is the failure a gang surge source records when its
// replacement gang loses members after the hand-over.
const ReplacementLostReason = "ReplacementLost"

// eventReasonGangSurgeRebuilt announces a replacement gang rebuilt after it
// was lost past the hand-over; eventReasonGangSurgeHeld a pair ended because
// the pinned revision admits no further attempt.
const (
	eventReasonGangSurgeRebuilt workload.EventReason = "GangSurgeRebuilt"
	eventReasonGangSurgeHeld    workload.EventReason = "GangSurgeHeld"
)

// gangSurgeEnd is how an abandon leaves the source: Ready on its running
// revision, or, when held is set, Failed with no attempt and that record,
// because the pinned revision admits no further attempt. withdrawn marks
// a pair ended past the hand-over because the roll target moved off the
// pinned revision while the replacement was short of members.
type gangSurgeEnd struct {
	held      *workload.InstanceTermination
	withdrawn bool
}

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
				keep, err := supersededGangSurgeKeepsPin(ctx, deps, input, plan, inst, surgeIdx)
				if err != nil {
					return false, err
				}
				cleanup = !keep
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
		// Past the Surge step the marker reaches its cleanup step only
		// through the end of a lost replacement, which finishes here: the
		// hold of its ladder, or the withdrawal of its revision.
		if !startingSurge && !promotedTarget && surgeMarker.Operation != nil &&
			surgeMarker.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup {
			if surgeTargetName != target.Name {
				return endWithdrawnGangSurge(ctx, deps, input, plan, src, surgeIdx)
			}
			return holdLostGangSurge(ctx, deps, input, plan, src, surgeIdx, surgeTargetName)
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
			failedTargetRev, failureReason, cause := "", "", workload.CauseUnattributed
			if src.Phase == workload.InstancePhaseFailed {
				failedTargetRev = src.Operation.TargetRevision
				failureReason = instanceFailureReason(src, "gang surge abandoned before the target became Ready")
				cause = instanceFailureCause(src)
			}
			return abandonFailedGangSurge(ctx, deps, input, plan, sourceIdx, surgeIdx, src.RunningRevision, failedTargetRev, failureReason, cause, gangSurgeEnd{})
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
				failedTargetRev, failureReason, cause := "", "", workload.CauseUnattributed
				if src.Phase == workload.InstancePhaseFailed {
					failedTargetRev = src.Operation.TargetRevision
					failureReason = instanceFailureReason(src, "gang surge abandoned before the target became Ready")
					cause = instanceFailureCause(src)
				}
				return abandonFailedGangSurge(ctx, deps, input, plan, sourceIdx, surgeIdx, src.RunningRevision, failedTargetRev, failureReason, cause, gangSurgeEnd{})
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
			instanceFailureCause(src), gangSurgeEnd{})
	}

	// Retire the replacement when the desired revision moves away from the
	// pinned one while the source gang still stands; only a Ready
	// replacement whose source is gone finishes on its pin.
	if startingSurge && !promotedSurgeTarget && src.Operation.TargetRevision != target.Name {
		keep, err := supersededGangSurgeKeepsPin(ctx, deps, input, plan, inst, *src.Operation.SurgeIndex)
		if err != nil {
			return false, err
		}
		if !keep {
			// A spec change is not a failure of the retired revision.
			return abandonFailedGangSurge(ctx, deps, input, plan, sourceIdx, *src.Operation.SurgeIndex, src.RunningRevision, "", "", workload.CauseUnattributed, gangSurgeEnd{})
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
	// A member in a terminal phase still holds its name, so the gang can
	// never complete; free it now. The bookkeeping belongs to the SOURCE,
	// which owns the operation; the pods and expectations bucket on the surge.
	if recycling, rerr := recycleTerminalTargets(ctx, deps, input, sourceIdx, surgeIdx,
		workload.InstanceOperationUpdate, surgePods, surgeTargets); rerr != nil {
		return false, fmt.Errorf("recycle terminal surge gang member (instance=%d): %w", surgeIdx, rerr)
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
		// Past the hand-over a replacement that lost members died behind a
		// source out of rotation: the rebuild is the next attempt, not this
		// one, and only at a revision the roll still wants.
		rearm := false
		if !startingSurge && !promotedSurgeTarget {
			if surgeTargetName != target.Name {
				return endWithdrawnGangSurge(ctx, deps, input, plan, src, surgeIdx)
			}
			recycled := recycledMembers(src, missing)
			create, after, err := rebuildLostReplacement(ctx, deps, input, plan, src, surgeMarker, surgeIdx, surgeTargetName, recycled, len(surgePods), len(surgeTargets))
			if err != nil || !create {
				return false, err
			}
			rearm = after
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
		// the per-revision drain Service selects them, and render that
		// revision's template. See surgeTargetName.
		tmpl, found, terr := pinnedTemplate(ctx, deps.Reader(), input, renderPlan, target, surgeTargetName)
		if terr != nil {
			return false, fmt.Errorf("resolve surge gang template (instance=%d): %w", surgeIdx, terr)
		}
		if !found {
			return false, announceRevisionGone(ctx, deps, input, sourceIdx, surgeTargetName)
		}
		if _, cerr := createMissingPods(ctx, deps, input, renderPlan, surgeInst, surgeIdx, missing, tmpl); cerr != nil {
			if createRejectionHandled(cerr) {
				return false, nil
			}
			return false, fmt.Errorf("create surge gang (instance=%d): %w", surgeIdx, cerr)
		}
		if rearm {
			if err := rearmGangSurgeRebuild(ctx, deps, input, plan, src, surgeIdx, surgeTargetName, len(missing), len(surgeTargets)); err != nil {
				return false, err
			}
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
		// The drain is also where a cross-Component gate can act with the
		// replacement gang already serving; a held source keeps serving at
		// Step=Surge, exactly as under a pause.
		if !promotedSurgeTarget && src.Operation != nil && src.Operation.Step == workload.UpdateStepSurge &&
			!drainAdmitted(input, sourcePods, surgeTargetName) {
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
		// does not leave both generations serving; a member already on its
		// way out, or gone since the list, is out of rotation by other means.
		if _, err := unroutePodsUnder(ctx, deps, sourcePods, podreadiness.WriterUpdateSurgeDrain, gangSurgeDrainKey(sourceIdx), nil); err != nil {
			return false, fmt.Errorf("drain source gang pod %w", err)
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
	// The Instance goes on under the replacement's index: a move still
	// pending on the source's index follows it there before the index is
	// released, so the record never names an Instance that is gone.
	if err := followHandoffForPendingMigrations(ctx, deps, input, sourceIdx, surgeIdx); err != nil {
		return false, fmt.Errorf("rebind pending migrations of the promoted gang (source=%d target=%d): %w", sourceIdx, surgeIdx, err)
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

// supersededGangSurgeKeepsPin reports whether a gang surge pinned to a
// revision the desired state has withdrawn finishes on its pin instead of
// being abandoned: only when the source gang is gone and the complete
// replacement gang is runtime-ready, so the replacement is the Instance's
// only pod set. While the source stands the surge is abandoned whether or
// not the replacement is Ready, for the reasons surgeUpdate's redirect gives.
func supersededGangSurgeKeepsPin(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, surgeIdx int32) (bool, error) {
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, plan.Component
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, inst.Index)
	if err != nil {
		return false, fmt.Errorf("list source gang pods for supersede check (instance=%d): %w", inst.Index, err)
	}
	if anyPodStanding(sourcePods) {
		return false, nil
	}
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list surge gang pods for supersede check (instance=%d): %w", surgeIdx, err)
	}
	return int32(len(surgePods)) >= inst.TotalPods() && query.AllPodsRuntimeReady(surgePods), nil
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
// atomically removes its marker and ends the source as end says: Ready on
// its running revision, the fresh-start Failed shape when it never ran one,
// or Failed with no attempt when the pinned revision holds. A failed revision also
// records its RetryBlock in that status transition: cause (the source's
// LastFailure evidence, see instanceFailureCause) decides how the wave
// reaches the revision's retry ladder, and both abandon tails apply that
// one verdict.
func abandonFailedGangSurge(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx, surgeIdx int32, sourceRunningRev, failedTargetRev, failureReason string, cause workload.FailureCause, end gangSurgeEnd) (bool, error) {
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, plan.Component
	guardTerminalMarker := input.ApplyInstanceMutationsWithRetryBlock != nil || input.FinalizeInstanceResources != nil
	source := input.ObservedState.Instance(sourceIdx)
	if guardTerminalMarker && !gangSurgeSourceOwnsRemoval(source, surgeIdx) {
		return false, nil
	}
	marker := input.ObservedState.Instance(surgeIdx)
	pinned := ""
	if source != nil && source.Operation != nil {
		pinned = source.Operation.TargetRevision
	}
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

	// 1. Delete the wedged surge gang's pods. The source stays at its surge
	// step, holding the budget, until they are gone, so a member that never
	// carried the serving gate goes on the abandoned-replacement grace.
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list failed surge gang pods (instance=%d): %w", surgeIdx, err)
	}
	if len(surgePods) > 0 {
		// A marker whose member write the apiserver refused is left as the
		// rejection left it until the revision in force admits a fresh
		// attempt; the source keeps its surge step and budget meanwhile, as
		// for any member still standing. With no member left there is
		// nothing to refuse, and the retirement finishes below.
		if parked, _ := rejectionParked(input, marker); parked {
			return false, nil
		}
		if !deps.ExpectationsCache().Satisfied(ns, owner, comp, surgeIdx) {
			return false, nil
		}
		// A member in rotation leaves it a pass ahead of its deletion.
		refused := func(pod *corev1.Pod, err error) (bool, error) {
			return disposeRejectedRowWrite(ctx, deps, input, surgeIdx, pod.Name, err)
		}
		if unrouted, err := unrouteServingPods(ctx, deps, surgeIdx, surgePods, refused); err != nil {
			return false, fmt.Errorf("unroute abandoned surge gang member (instance=%d): %w", surgeIdx, err)
		} else if unrouted {
			return false, nil
		}
		for _, pod := range surgePods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			deps.ExpectationsCache().ExpectDeletes(ns, owner, comp, surgeIdx, 1)
			if err := deps.Client.Delete(ctx, pod, abandonedReplacementDeleteOptions(input, pod)...); err != nil {
				deps.ExpectationsCache().ObservedDelete(ns, owner, comp, surgeIdx)
				if apierrors.IsNotFound(err) {
					continue
				}
				if handled, derr := disposeRejectedRowWrite(ctx, deps, input, surgeIdx, pod.Name, err); handled {
					return false, derr
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
			ctx, deps, input, source, marker, surgeIdx, sourceRunningRev, failedTargetRev, failureReason, cause, end,
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
		if end.held != nil {
			if err := status.StampFailed(ctx, input, sourceIdx, end.held); err != nil {
				return false, fmt.Errorf("hold failed gang surge source (instance=%d): %w", sourceIdx, err)
			}
		} else {
			if err := recordUpdateFailureInRetryBlock(ctx, input, failedTargetRev, failureReason, cause); err != nil {
				return false, fmt.Errorf("record retry block for failed gang surge (rev=%s): %w", failedTargetRev, err)
			}
			if err := status.StampAbandonedSurgeSource(ctx, input, sourceIdx, sourceRunningRev); err != nil {
				return false, fmt.Errorf("reset failed gang surge source (instance=%d): %w", sourceIdx, err)
			}
		}
	}
	reset := fmt.Sprintf("resetting to revision %s for a fresh rollout", sourceRunningRev)
	switch {
	case end.withdrawn && sourceRunningRev != "":
		reset = fmt.Sprintf("the source returns to rotation on revision %s", sourceRunningRev)
	case end.withdrawn:
		reset = "the source's pods are gone and it re-enters as a fresh start at the roll target"
	case sourceRunningRev == "":
		reset = "the source never ran a revision and re-enters as a fresh start"
	}
	if end.held != nil {
		workload.RecordWarning(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeHeld,
			"OMENative %s held: revision %s admits no further attempt after the replacement gang (surge-index=%d) lost members once it had taken over serving; the Instance stays Failed until a corrected revision or a reset",
			workload.InstanceKey(comp, sourceIdx), pinned, surgeIdx)
	} else if failedTargetRev != "" {
		workload.RecordWarning(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned failed gang surge (surge-index=%d); %s",
			workload.InstanceKey(comp, sourceIdx), surgeIdx, reset)
	} else if end.withdrawn {
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned gang surge (surge-index=%d): the roll target moved off revision %s while the replacement gang was short of members, so nothing is rebuilt at it; %s",
			workload.InstanceKey(comp, sourceIdx), surgeIdx, pinned, reset)
	} else {
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned superseded gang surge (surge-index=%d); %s",
			workload.InstanceKey(comp, sourceIdx), surgeIdx, reset)
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
	cause workload.FailureCause,
	end gangSurgeEnd,
) (bool, error) {
	if source == nil || !gangSurgeSourceOwnsRemoval(source, surgeIdx) {
		return false, nil
	}
	if marker != nil && (!gangSurgeTargetOwnsRemoval(marker, true) ||
		!status.GangSurgeTargetClaimMatches(marker, source.Operation.TargetRevision)) {
		return false, nil
	}
	if end.held != nil {
		return status.HoldGangSurgeSourceAndRemoveMarker(ctx, deps, input, source, marker, surgeIdx, end.held)
	}
	return status.ResetGangSurgeSourceAndRemoveMarker(
		ctx, deps, input, source, marker, surgeIdx, sourceRunningRev, failedTargetRev, failureReason, cause,
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

// rebuildLostReplacement decides the drain-step continuation's rebuild of a
// replacement gang short of members. A loss is a drop in the replacement's
// member count against the count the status publication wrote from the
// previous pass, less the members the recycle took: a dead member the
// controller removed itself is the recycle's to recreate, at once and
// under the attempt as it stands, and members already missing at the
// published count are a rebuild still pending, created and charged
// nothing. A loss closes the attempt and counts it once on the pinned
// revision's ladder; the next attempt rebuilds the set under a fresh
// deadline once the ladder admits it, re-armed after its members are
// created; a ladder that holds the revision ends the pair with the source
// Failed and no attempt. Reports whether the caller may create the missing
// members this pass, and whether the source re-arms once they are created.
func rebuildLostReplacement(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, src, marker *workload.InstanceStatus, surgeIdx int32, pin string, recycled, observed, size int) (create, rearm bool, err error) {
	// A rebuild the cluster is holding back is not a loss: its create waits
	// on the hold, as every attempt's does.
	if workload.OperationExternallyHeld(src.Operation) || (marker != nil && workload.OperationExternallyHeld(marker.Operation)) {
		return true, false, nil
	}
	switch src.Phase {
	case workload.InstancePhaseUpdating, workload.InstancePhaseFailed:
	default:
		return true, false, nil
	}
	published, _ := workload.AdapterPublished(marker)
	lost := int(published) - observed - recycled
	block := workload.RetryBlock{TargetRevision: pin}
	if standing := workload.FindRetryBlock(input.ObservedState.RetryBlocks, pin); standing != nil {
		block = *standing
	}
	switch {
	case lost > 0:
		block, err = closeLostGangSurgeAttempt(ctx, input, src, surgeIdx, pin, lost, size)
		if err != nil {
			return false, false, err
		}
		if block.State == workload.RetryBlockHeld {
			// The hold's fences read the row as this pass closed it.
			src.Phase = workload.InstancePhaseFailed
			_, err = holdLostGangSurge(ctx, deps, input, plan, src, surgeIdx, pin)
			return false, false, err
		}
		if src.Phase == workload.InstancePhaseUpdating {
			return false, false, nil
		}
	case src.Phase == workload.InstancePhaseUpdating:
		return true, false, nil
	case block.State == workload.RetryBlockHeld:
		_, err = holdLostGangSurge(ctx, deps, input, plan, src, surgeIdx, pin)
		return false, false, err
	}
	if block.State == workload.RetryBlockBackoff && block.NextRetryAt != nil && input.Now().Before(block.NextRetryAt.Time) {
		input.PassWake.Observe(block.NextRetryAt.Time.Sub(input.Now()))
		return false, false, nil
	}
	return true, true, nil
}

// recycledMembers counts the missing members the terminal recycle took: the
// row's failure record names the member and is dated no later than the
// attempt's last progress, the two the recycle writes together. Such a
// member's removal is the controller's own, not a loss of the replacement.
// An escalation's record is dated after the last progress, and a loss
// names no pod, so neither reads as the recycle's.
func recycledMembers(src *workload.InstanceStatus, missing []podTarget) int {
	if src == nil || src.Operation == nil || src.Operation.RetryCount == 0 || src.LastFailure == nil ||
		src.LastFailure.PodName == "" || src.LastFailure.Time.After(src.Operation.LastProgressAt.Time) {
		return 0
	}
	for _, t := range missing {
		if t.Name == src.LastFailure.PodName {
			return 1
		}
	}
	return 0
}

// rearmGangSurgeRebuild opens the next attempt once a lost replacement's
// members are created again: the source back to Updating at the drain step
// with the retry counted and a fresh deadline, the ladder's block made the
// attempt in progress.
func rearmGangSurgeRebuild(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, src *workload.InstanceStatus, surgeIdx int32, pin string, rebuilt, size int) error {
	rearmed, err := status.RearmGangSurgeRebuild(ctx, input, src, plan.InstanceReadyTimeout)
	if err != nil || !rearmed {
		return err
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeRebuilt,
		"OMENative %s rebuilt %d of %d member(s) of the replacement gang (surge-index=%d) at revision %s; retry %d under a fresh deadline",
		workload.InstanceKey(plan.Component, src.Index), rebuilt, size, surgeIdx, pin, src.Operation.RetryCount+1)
	return nil
}

// closeLostGangSurgeAttempt ends the attempt whose replacement died past the
// hand-over: the loss counts once on the pinned revision's ladder, by the
// ladder's own reading (a revision with no block or an attempt in progress
// counts the wave, a Backoff or Held block is already counting it), and is
// recorded on the source, Failed with its surge kept. Returns the ladder's
// block as it stands afterwards.
func closeLostGangSurgeAttempt(ctx context.Context, input workload.ReconcileInput, src *workload.InstanceStatus, surgeIdx int32, pin string, lost, size int) (workload.RetryBlock, error) {
	termination := replacementLostTermination(surgeIdx, lost, size, metav1.NewTime(input.Now()))
	block := workload.RetryBlock{TargetRevision: pin}
	if observed := workload.FindRetryBlock(input.ObservedState.RetryBlocks, pin); observed != nil {
		block = *observed
	}
	if block.State == "" || block.State == workload.RetryBlockRetryInProgress {
		after, err := recordLostAttemptWave(ctx, input, pin, termination.Message)
		if err != nil {
			return block, fmt.Errorf("count lost replacement gang on the ladder (rev=%s): %w", pin, err)
		}
		block = after
	}
	committed, err := status.ApplyStamp(ctx, input, src.Index, status.StampReplacementLost(termination))
	if err != nil {
		return block, fmt.Errorf("record lost replacement gang (instance=%d): %w", src.Index, err)
	}
	if committed && input.WarnInstanceFailed != nil {
		input.WarnInstanceFailed(src.Index, "", termination.Message)
	}
	return block, nil
}

// recordLostAttemptWave applies one lost attempt to the pinned revision's
// ladder and returns the block as written; an unconfigured ladder leaves
// the wave unrecorded and the block empty.
func recordLostAttemptWave(ctx context.Context, input workload.ReconcileInput, pin, reason string) (workload.RetryBlock, error) {
	after := workload.RetryBlock{TargetRevision: pin}
	if input.MutateRetryBlock == nil {
		return after, nil
	}
	now := metav1.NewTime(input.Now())
	var held int32
	err := input.MutateRetryBlock(ctx, pin, func(b *workload.RetryBlock) workload.RetryBlockDisposition {
		disposition, heldAttempts := workload.ApplyUpdateFailureToRetryBlock(b, input.UpdateRetryPolicy, now, reason, workload.CauseUnattributed)
		after, held = *b, heldAttempts
		return disposition
	})
	if err != nil {
		return after, err
	}
	if held > 0 && input.WarnRetryHeld != nil {
		input.WarnRetryHeld(pin, held, reason)
	}
	return after, nil
}

// replacementLostTermination is the record of a replacement gang that lost
// members after the hand-over.
func replacementLostTermination(surgeIdx int32, lost, size int, now metav1.Time) *workload.InstanceTermination {
	return &workload.InstanceTermination{
		Reason:  ReplacementLostReason,
		Message: fmt.Sprintf("replacement gang (surge-index=%d) lost %d of %d member(s) after it took over serving", surgeIdx, lost, size),
		Time:    now,
	}
}

// holdLostGangSurge ends a gang surge whose pinned revision admits no
// further attempt after its replacement was lost past the hand-over: the
// replacement's leftovers go, the marker is removed and the source is left
// Failed with no attempt, so the Instance holds until a corrected revision
// or a reset. Multi-pass, through the abandon's own steps.
func holdLostGangSurge(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, src *workload.InstanceStatus, surgeIdx int32, pin string) (bool, error) {
	attempts := int32(0)
	if b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, pin); b != nil {
		attempts = b.AttemptsStarted
	}
	held := &workload.InstanceTermination{
		Reason:  ReplacementLostReason,
		Message: fmt.Sprintf("replacement gang (surge-index=%d) lost members after it took over serving; revision %s holds after %d attempt(s)", surgeIdx, pin, attempts),
		Time:    metav1.NewTime(input.Now()),
	}
	return abandonFailedGangSurge(ctx, deps, input, plan, src.Index, surgeIdx, src.RunningRevision, "", "", workload.CauseUnattributed, gangSurgeEnd{held: held})
}

// endWithdrawnGangSurge ends a gang surge past the hand-over whose
// replacement is short of members while the roll target has moved off the
// pinned revision: a replacement is rebuilt only for a revision the roll
// still wants. The source gang's drain hold is lifted so it returns to
// rotation on the revision the roll is back on; with no source pod standing
// it re-enters as the Create pass's fresh start at the target. Multi-pass,
// through the abandon's own steps.
func endWithdrawnGangSurge(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, src *workload.InstanceStatus, surgeIdx int32) (bool, error) {
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, plan.Component
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, src.Index)
	if err != nil {
		return false, fmt.Errorf("list source gang pods for the withdrawn surge (instance=%d): %w", src.Index, err)
	}
	hold := podreadiness.Message{UserAgent: podreadiness.WriterUpdateSurgeDrain, Key: gangSurgeDrainKey(src.Index)}
	standing := 0
	for _, pod := range sourcePods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		standing++
		if err := podreadiness.RemoveNotReadyKeyIgnoreNotFound(ctx, deps.Client, deps.Reader(), pod, hold); err != nil {
			return false, fmt.Errorf("lift the drain hold of source pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	sourceRunningRev := src.RunningRevision
	if standing == 0 {
		sourceRunningRev = ""
	}
	return abandonFailedGangSurge(ctx, deps, input, plan, src.Index, surgeIdx, sourceRunningRev, "", "", workload.CauseUnattributed, gangSurgeEnd{withdrawn: true})
}
