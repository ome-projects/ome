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

// gangSurgeDrainKey is the serving-gate writer key under which a gang surge
// drains its source Instance. It depends on the source index alone, so the
// gate write repeats without effect across the passes a drain takes.
func gangSurgeDrainKey(sourceIdx int32) string {
	return "gang-surge-source-" + strconv.Itoa(int(sourceIdx))
}

// eventReasonGangSurgeAbandoned reports a pair that ended with its
// replacement retired, as against one that ended with it promoted.
const eventReasonGangSurgeAbandoned workload.EventReason = "GangSurgeAbandoned"

// ReplacementLostReason is the failure a gang surge source records when its
// replacement gang loses members after it took over serving.
const ReplacementLostReason = "ReplacementLost"

// eventReasonGangSurgeRebuilt reports a replacement gang rebuilt after it
// lost members once it had taken over serving; eventReasonGangSurgeHeld a
// pair ended because the revision it is held to admits no further attempt.
const (
	eventReasonGangSurgeRebuilt workload.EventReason = "GangSurgeRebuilt"
	eventReasonGangSurgeHeld    workload.EventReason = "GangSurgeHeld"
)

// gangSurgeEnd is how an ended pair leaves the source: Ready on its running
// revision, or, when held is set, Failed with no attempt and that record,
// because the pair's revision admits no further attempt. withdrawn marks a
// pair ended after its replacement took over serving, because the roll
// target moved off the pair's revision while the replacement was short of
// members.
type gangSurgeEnd struct {
	held      *workload.InstanceTermination
	withdrawn bool
}

// gangSurgePhase is one step of a gang surge pass. It reports whether the
// pass goes on to the next step; a step that ends the pass returns false
// with the error that ended it, if any.
type gangSurgePhase func(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (next bool, err error)

// runGangSurgePhases runs the phases in order and reports whether every one
// of them went on. The first phase that ends the pass ends it, with its error.
func runGangSurgePhases(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, phases []gangSurgePhase) (bool, error) {
	for _, phase := range phases {
		if next, err := phase(ctx, deps, input); err != nil || !next {
			return false, err
		}
	}
	return true, nil
}

// gangSurgeRoll is a gang surge as one pass reads it: the source row, the
// row at the surge index the replacement gang is built under, and the
// revision the pair is held to. The phases fill in what the pass learns.
type gangSurgeRoll struct {
	plan workload.ComponentPlan
	inst workload.InstancePlan
	// target is the roll's target revision; the pair's revision may be an
	// older one the roll has since left.
	target *appsv1.ControllerRevision

	sourceIdx int32
	surgeIdx  int32
	source    *workload.InstanceStatus
	// replacement is the row at surgeIdx: nil until the pair is written, or
	// when the row is missing and has to be written back.
	replacement *workload.InstanceStatus
	// revision is the revision the pair is held to: the operation's once the
	// pair is written, else the roll's target.
	revision string
	// claimed: the source's operation names a surge index, so the pair is open.
	claimed bool
	// startingSurge: the source is at step Surge, not yet claimed for the drain.
	startingSurge bool
	// promoted: the replacement's row already reads Ready on the pair's
	// revision with no operation.
	promoted bool
	// keptForPairing is the pairing gate's reason when a pair the roll has
	// left goes on because the gate keeps the replacement in rotation.
	keptForPairing string

	replacementPlan workload.InstancePlan
	replacementPods []*corev1.Pod
	sourcePods      []*corev1.Pod
}

// readGangSurgeRoll reads the pair from the rows the pass observed.
func readGangSurgeRoll(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision) *gangSurgeRoll {
	r := &gangSurgeRoll{plan: plan, inst: inst, target: target, sourceIdx: inst.Index, revision: target.Name}
	r.source = input.ObservedState.Instance(r.sourceIdx)
	r.claimed = gangSurgeSourceClaim(r.source)
	if !r.claimed {
		return r
	}
	r.surgeIdx = *r.source.Operation.SurgeIndex
	r.startingSurge = r.source.Operation.Step == workload.UpdateStepSurge
	// An open pair stays held to the revision written at its start; a newer
	// target is handled by a later update.
	if r.source.Operation.TargetRevision != "" {
		r.revision = r.source.Operation.TargetRevision
	}
	r.replacement = input.ObservedState.Instance(r.surgeIdx)
	r.promoted = gangSurgePromotedTargetMatches(r.replacement, r.revision)
	return r
}

// gangSurgeUpdate rolls a multi-pod Instance by replacement. A whole new
// gang is built at a free index on the revision the pair is held to; the
// source gang leaves rotation only once the new gang serves and the drain
// gate allows it, and is deleted only once its endpoints are gone; the new
// gang is promoted once the source's pods are gone. The pair's two rows keep
// the revision and the cleanup ownership across passes. The phase list is
// the whole decision of what runs next: each phase does one job and says
// whether the pass goes on.
func gangSurgeUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision) (bool, error) {
	roll := readGangSurgeRoll(input, plan, inst, target)
	if !roll.claimed {
		return roll.openPair(ctx, deps, input)
	}
	return runGangSurgePhases(ctx, deps, input, []gangSurgePhase{
		roll.resumeFromRows,
		roll.endPairOfFailedSource,
		roll.endPairTheRollLeft,
		roll.buildReplacement,
		roll.markReplacementServing,
		roll.waitForPromoteBar,
		roll.askDrainGate,
		roll.claimSourceDrain,
		roll.deleteDrainedSource,
		roll.promoteReplacement,
		roll.removeSource,
	})
}

// openPair claims the pair in one write: the source goes to Update/Surge
// with the replacement's index and the roll's target revision, and the
// replacement's row is written at that index. The pass stops so the next one
// sees the index in the plan and the PodGroup pass creates the replacement's
// PodGroup before its pods.
func (r *gangSurgeRoll) openPair(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	surges := input.SurgeIndexAllocator()
	r.surgeIdx = surges.Next()
	claimed, err := status.StartGangSurge(ctx, input, r.source, r.surgeIdx, r.revision, r.plan.UpdateStrategy.Type, r.plan.InstanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("claim gang surge pair (source=%d target=%d): %w", r.sourceIdx, r.surgeIdx, err)
	}
	if !claimed {
		return false, nil
	}
	// Reserve the index for the rest of the pass: with the allocator, so
	// the next surge takes the one after it, and in this reconcile's
	// snapshot, so sibling updates cannot allocate the same replacement
	// slot.
	surges.Claim(r.surgeIdx)
	if r.source != nil {
		surgeIdx := r.surgeIdx
		r.source.Operation = &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           workload.UpdateStepSurge,
			SurgeIndex:     &surgeIdx,
			TargetRevision: r.revision,
			Strategy:       r.plan.UpdateStrategy.Type,
		}
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonRecreateUpdateStarted,
		"OMENative %s gang surge to revision %s (surge-index=%d)",
		workload.InstanceKey(r.plan.Component, r.sourceIdx), r.revision, r.surgeIdx)
	return false, nil
}

// resumeFromRows picks the pair up where its two rows left it. A missing
// replacement row is written back; a surge index another row holds releases
// the source's claim; a pair that is already ending (a lost replacement whose
// revision holds or that the roll has left, a retired replacement at its
// cleanup step) is taken on to its end; a replacement already promoted is
// accepted once the source's pods are gone. On the batch path the
// replacement's row is confirmed live before the pass goes on.
func (r *gangSurgeRoll) resumeFromRows(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if r.replacement == nil {
		return r.restoreReplacementRow(ctx, deps, input)
	}
	if !status.GangSurgeTargetClaimMatches(r.replacement, r.revision) && !r.promoted {
		if _, err := resetGangSurgeSourceAfterTargetConflict(ctx, input, r.source, r.replacement); err != nil {
			return false, fmt.Errorf("reset gang surge with occupied target (source=%d target=%d): %w", r.sourceIdx, r.surgeIdx, err)
		}
		return false, nil
	}
	// Past the Surge step the replacement's row reaches its cleanup step only
	// through the end of a lost replacement, which finishes here: its
	// revision's retry record holds, or the roll has left its revision.
	if !r.startingSurge && !r.promoted && r.replacement.Operation != nil &&
		r.replacement.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup {
		if r.revision != r.target.Name {
			_, err := endWithdrawnGangSurge(ctx, deps, input, r.plan, r.source, r.surgeIdx)
			return false, err
		}
		_, err := holdLostGangSurge(ctx, deps, input, r.plan, r.source, r.surgeIdx, r.revision)
		return false, err
	}
	if r.promoted {
		if next, err := r.acceptPromotedReplacement(ctx, deps, input); err != nil || !next {
			return false, err
		}
	}
	if r.startingSurge && !r.promoted && r.replacement.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup {
		return r.endFailedPair(ctx, deps, input)
	}
	if !r.promoted && (input.ApplyInstanceMutationsWithRetryBlock != nil || input.FinalizeInstanceResources != nil) {
		resolution, err := status.RestoreGangSurgeTarget(ctx, input, r.source, r.surgeIdx, r.revision, r.plan.InstanceReadyTimeout)
		if err != nil {
			return false, fmt.Errorf("confirm gang surge target marker (instance=%d): %w", r.surgeIdx, err)
		}
		if resolution == status.GangSurgeTargetMarkerCleanup {
			if !r.startingSurge {
				return false, nil
			}
			return r.endFailedPair(ctx, deps, input)
		}
		if resolution != status.GangSurgeTargetMarkerActive {
			return false, nil
		}
	}
	return true, nil
}

// restoreReplacementRow writes the replacement's row back when the surge
// index holds none: in its cleanup form when the pair is ending (the source
// Failed at the Surge step, or the roll gone from the pair's revision with
// nothing that keeps the pair), else in its live form. The pass stops so the
// next one reads the row it wrote.
func (r *gangSurgeRoll) restoreReplacementRow(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	cleanup := r.startingSurge && r.source.Phase == workload.InstancePhaseFailed
	if r.startingSurge && !cleanup && r.source.Operation.TargetRevision != r.target.Name {
		keep, _, err := supersededGangSurgeKeepsPin(ctx, deps, input, r.plan, r.inst, r.surgeIdx, r.source.Operation.TargetRevision)
		if err != nil {
			return false, err
		}
		cleanup = !keep
	}
	if cleanup && (input.ApplyInstanceMutationsWithRetryBlock != nil || input.FinalizeInstanceResources != nil) {
		if _, err := restoreGangSurgeTargetCleanup(ctx, input, r.source, r.surgeIdx, r.revision, r.plan.InstanceReadyTimeout); err != nil {
			return false, fmt.Errorf("restore gang surge target cleanup marker (instance=%d): %w", r.surgeIdx, err)
		}
	} else if _, err := status.RestoreGangSurgeTarget(ctx, input, r.source, r.surgeIdx, r.revision, r.plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("restore gang surge target marker (instance=%d): %w", r.surgeIdx, err)
	}
	return false, nil
}

// acceptPromotedReplacement takes a replacement whose row already reads
// promoted as this pair's once the source's pods are gone. Pods still
// standing make an operation-less Ready row no proof that it is this pair's:
// a source still at the Surge step releases its claim, a source past it
// waits. On the batch path both rows are confirmed on the adapter's fresh
// copy and the revision's retry record is pruned.
func (r *gangSurgeRoll) acceptPromotedReplacement(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, r.plan.Component, r.sourceIdx)
	if err != nil {
		return false, fmt.Errorf("list source gang pods for promoted target recovery (instance=%d): %w", r.sourceIdx, err)
	}
	if len(sourcePods) != 0 {
		if !r.startingSurge {
			return false, nil
		}
		if _, err := resetGangSurgeSourceAfterTargetConflict(ctx, input, r.source, r.replacement); err != nil {
			return false, fmt.Errorf("reset gang surge with occupied promoted target (source=%d target=%d): %w", r.sourceIdx, r.surgeIdx, err)
		}
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock != nil {
		confirmed, err := confirmPromotedGangSurgeTarget(ctx, input, r.source, r.replacement)
		if err != nil {
			return false, fmt.Errorf("confirm promoted gang surge target (instance=%d): %w", r.surgeIdx, err)
		}
		if !confirmed {
			return false, nil
		}
		if err := status.RetryBlockPruneOnPromote(ctx, input, r.revision); err != nil {
			return false, fmt.Errorf("prune retry block after gang promotion (revision=%s): %w", r.revision, err)
		}
	}
	return true, nil
}

// endPairOfFailedSource ends the pair when the escalation failed the source
// while its replacement was still being built: the source stays on its
// running revision and the pair's revision is charged with the failure.
func (r *gangSurgeRoll) endPairOfFailedSource(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if !r.startingSurge || r.promoted || r.source.Phase != workload.InstancePhaseFailed {
		return true, nil
	}
	return r.endFailedPair(ctx, deps, input)
}

// endPairTheRollLeft ends a pair whose revision the roll has moved off while
// the source gang still stands, charging nothing: a spec change is not a
// failure of the revision it retires. The pair goes on when the replacement
// is the Instance's only pod set, or when the pairing gate keeps it in
// rotation; the gate's reason is kept for the drain.
func (r *gangSurgeRoll) endPairTheRollLeft(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if !r.startingSurge || r.promoted || r.source.Operation.TargetRevision == r.target.Name {
		return true, nil
	}
	keep, reason, err := supersededGangSurgeKeepsPin(ctx, deps, input, r.plan, r.inst, r.surgeIdx, r.source.Operation.TargetRevision)
	if err != nil {
		return false, err
	}
	if !keep {
		return r.endPair(ctx, deps, input, "", "", workload.CauseUnattributed, gangSurgeEnd{})
	}
	r.keptForPairing = reason
	return true, nil
}

// endFailedPair ends a pair whose replacement never became Ready. A Failed
// source charges the pair's revision with its failure; any other source
// charges nothing.
func (r *gangSurgeRoll) endFailedPair(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	revision, reason, cause := "", "", workload.CauseUnattributed
	if r.source.Phase == workload.InstancePhaseFailed {
		revision = r.source.Operation.TargetRevision
		reason = instanceFailureReason(r.source, "gang surge abandoned before the target became Ready")
		cause = instanceFailureCause(r.source)
	}
	return r.endPair(ctx, deps, input, revision, reason, cause, gangSurgeEnd{})
}

// endPair ends the pair through abandonFailedGangSurge: the replacement's
// pods go, the source goes back to its running revision, and the revision
// named is charged with the failure given. The pass stops.
func (r *gangSurgeRoll) endPair(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, chargedRevision, reason string, cause workload.FailureCause, end gangSurgeEnd) (bool, error) {
	_, err := abandonFailedGangSurge(ctx, deps, input, r.plan, r.sourceIdx, r.surgeIdx, r.source.RunningRevision, chargedRevision, reason, cause, end)
	return false, err
}

// buildReplacement lists the replacement gang's pods at the surge index and
// creates the members that are missing, on the pair's revision at the width
// that revision records. A member in a terminal phase is removed first, and
// a member on a node that stopped reporting keeps its name; both stop the
// pass. The pass goes on only when every member exists.
func (r *gangSurgeRoll) buildReplacement(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, r.plan.Component
	r.replacementPlan = workload.InstancePlan{
		Index:         r.surgeIdx,
		Incarnation:   1,
		Runners:       r.plan.RunnersFor(r.inst, r.revision),
		ExcludedNodes: r.inst.ExcludedNodes, // Exclusion memory follows the instance through surge replacement.
	}
	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, r.surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list surge gang pods (instance=%d): %w", r.surgeIdx, err)
	}
	r.replacementPods = pods
	targets := expectedPodNamesForInstance(input, r.plan, r.replacementPlan)
	// A member in a terminal phase still holds its name, so the gang can
	// never complete; free it now. The bookkeeping belongs to the SOURCE,
	// which owns the operation; the pods and expectations bucket on the surge.
	if recycling, rerr := recycleTerminalTargets(ctx, deps, input, r.sourceIdx, r.surgeIdx,
		workload.InstanceOperationUpdate, pods, targets); rerr != nil {
		return false, fmt.Errorf("recycle terminal surge gang member (instance=%d): %w", r.surgeIdx, rerr)
	} else if recycling {
		return false, nil
	}
	// A member whose kubelet has stopped reporting is not known to be gone:
	// its name is freed only by the force-delete sweep, once the policy
	// proves the node gone, and the source gang keeps serving meanwhile. The
	// sweep reads and events under the surge index, where the pods live.
	if holding, _, herr := recoverUnknownPhaseTargets(ctx, deps, input, r.surgeIdx, pods, targets); herr != nil {
		return false, fmt.Errorf("recover unknown-phase surge gang member (instance=%d): %w", r.surgeIdx, herr)
	} else if holding {
		return false, nil
	}
	// Once the replacement has taken over serving, an attempt the placement
	// grace ended leaves the member the scheduler refused standing; the next
	// attempt builds its own, so the refused one goes first.
	if !r.startingSurge && !r.promoted && placementGraceEndedAttempt(r.source) {
		if retiring, rerr := retireRefusedMembers(ctx, deps, input, r.surgeIdx, pods); rerr != nil {
			return false, fmt.Errorf("retire refused surge gang member (instance=%d): %w", r.surgeIdx, rerr)
		} else if retiring {
			return false, nil
		}
	}
	existingByName := query.IndexPodsByName(pods)
	missing := make([]podTarget, 0)
	for _, t := range targets {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) == 0 {
		return true, nil
	}
	return r.createMissingMembers(ctx, deps, input, missing, len(targets))
}

// createMissingMembers creates the replacement's missing members once the
// expectations cache is settled. Past the drain, a replacement that lost
// members died behind a source that is out of rotation: the loss ends this
// attempt, the rebuild is the next one, and only at a revision the roll
// still wants. The pass stops.
func (r *gangSurgeRoll) createMissingMembers(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, missing []podTarget, size int) (bool, error) {
	if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, r.plan.Component, r.surgeIdx) {
		return false, nil
	}
	rearm := false
	if !r.startingSurge && !r.promoted {
		if r.revision != r.target.Name {
			_, err := endWithdrawnGangSurge(ctx, deps, input, r.plan, r.source, r.surgeIdx)
			return false, err
		}
		recycled := recycledMembers(r.source, missing)
		create, after, err := rebuildLostReplacement(ctx, deps, input, r.plan, r.source, r.replacement, r.surgeIdx, r.revision, recycled, len(r.replacementPods), size)
		if err != nil || !create {
			return false, err
		}
		rearm = after
	}
	created, err := r.createReplacementPods(ctx, deps, input, missing)
	if err != nil || !created {
		return false, err
	}
	if rearm {
		if err := rearmGangSurgeRebuild(ctx, deps, input, r.plan, r.source, r.replacement, r.surgeIdx, r.revision, len(missing), size); err != nil {
			return false, err
		}
	}
	return false, nil
}

// createReplacementPods ensures the replacement's PodGroup, then creates
// the missing members from the template of the pair's revision, so the pods
// carry the revision the surge committed to and the per-revision drain
// Service selects them. It reports whether the members were created.
//
// The PodGroup is ensured here, before the pods, because the PodGroup pass
// keys off the plan, which carries the surge index only once the
// replacement's row has round-tripped through the status; a gang-aware
// scheduler reads that lag as "PodGroup not found" on the fresh pods. The
// callback belongs to the gang package; a nil callback, a single-pod surge
// or an absent CRD does nothing inside it.
func (r *gangSurgeRoll) createReplacementPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, missing []podTarget) (bool, error) {
	renderPlan := r.plan
	if deps.EnsureGangPodGroup != nil {
		effectiveTopology, pgErr := deps.EnsureGangPodGroup(ctx, input, r.plan, r.replacementPlan)
		if pgErr != nil {
			if errors.Is(pgErr, workload.ErrGangNameUnusable) {
				// The PodGroup name is held by another controller or by an
				// object still being collected: a reading about one row that
				// the escalation pass owns, not a pass failure, which would
				// stall every other Instance too.
				return false, nil
			}
			return false, fmt.Errorf("ensure surge gang PodGroup (instance=%d): %w", r.surgeIdx, pgErr)
		}
		renderPlan.InstanceTopologyKeys = cloneInstanceTopologyKeys(r.plan.InstanceTopologyKeys)
		renderPlan.InstanceTopologyKeys[r.surgeIdx] = effectiveTopology
	}
	tmpl, found, terr := pinnedTemplate(ctx, deps.Reader(), input, renderPlan, r.target, r.revision)
	if terr != nil {
		return false, fmt.Errorf("resolve surge gang template (instance=%d): %w", r.surgeIdx, terr)
	}
	if !found {
		return false, announceRevisionGone(ctx, deps, input, r.sourceIdx, r.revision)
	}
	if _, cerr := createMissingPods(ctx, deps, input, renderPlan, r.replacementPlan, r.surgeIdx, missing, tmpl); cerr != nil {
		if createRejectionHandled(cerr) {
			return false, nil
		}
		return false, fmt.Errorf("create surge gang (instance=%d): %w", r.surgeIdx, cerr)
	}
	return true, nil
}

// markReplacementServing waits until every member of the replacement gang
// is ContainersReady, then writes the serving gate on each. Waiting for
// PodReady here would wait forever, because PodReady includes that gate.
func (r *gangSurgeRoll) markReplacementServing(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if int32(len(r.replacementPods)) < r.replacementPlan.TotalPods() || !query.AllPodsRuntimeReady(r.replacementPods) {
		return false, nil
	}
	for _, pod := range r.replacementPods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("serving=True on surge pod %s: %w", pod.Name, err)
		}
	}
	return true, nil
}

// waitForPromoteBar lists the source gang's pods and, while any stands,
// holds the pass until the whole replacement gang has passed the promote
// bar: kubelet has folded the serving gate into PodReady and the gang has
// held Ready for the availability window. The source keeps serving and its
// budget slot meanwhile. Past the Surge step the source is already draining
// and the window no longer applies.
func (r *gangSurgeRoll) waitForPromoteBar(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, r.plan.Component, r.sourceIdx)
	if err != nil {
		return false, fmt.Errorf("list source gang pods (instance=%d): %w", r.sourceIdx, err)
	}
	r.sourcePods = pods
	if len(pods) == 0 {
		return true, nil
	}
	window := r.plan.MinReadySeconds
	if !surgeWindowApplies(r.source) {
		window = 0
	}
	if promotable, wait := query.PodSetPromotable(r.replacementPods, window, input.Now()); !promotable {
		input.PromoteWindow.Observe(wait)
		return false, nil
	}
	return true, nil
}

// askDrainGate holds a source still at the Surge step until the drain gate
// (the cross-Component pairing, ratio and minimum rules) lets it leave
// rotation; a held source keeps serving, as under a pause. A pair the
// pairing gate keeps says so once, here.
func (r *gangSurgeRoll) askDrainGate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if len(r.sourcePods) == 0 {
		return true, nil
	}
	if !r.promoted && r.source.Operation != nil && r.source.Operation.Step == workload.UpdateStepSurge &&
		!drainAdmitted(input, r.sourcePods, r.revision) {
		return false, nil
	}
	if r.keptForPairing != "" && r.source.Operation != nil && r.source.Operation.Step == workload.UpdateStepSurge {
		recordSurgeKeptForPairing(deps, input, r.sourceIdx, r.revision, r.target.Name, r.keptForPairing)
	}
	return true, nil
}

// claimSourceDrain moves the source's operation from Surge to SurgeDrain
// before the source leaves rotation: on the batch path fenced on both rows,
// on the finalizer path as a step write. A pair whose replacement is already
// promoted has nothing to claim.
func (r *gangSurgeRoll) claimSourceDrain(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if r.promoted {
		return true, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock != nil {
		claimed, err := claimGangSurgeDrain(ctx, input, r.source, r.replacement)
		if err != nil {
			return false, fmt.Errorf("claim source gang drain (instance=%d): %w", r.sourceIdx, err)
		}
		if !claimed {
			return false, nil
		}
		r.source.Operation.Step = workload.UpdateStepSurgeDrain
	} else if input.FinalizeInstanceResources != nil &&
		(r.source.Operation.Step == workload.UpdateStepSurge || r.source.Operation.Step == workload.UpdateStepSurgeDrain) {
		claimed, err := status.StampStep(ctx, input, r.source, workload.UpdateStepSurgeDrain, true)
		if err != nil {
			return false, fmt.Errorf("stamp source gang drain (instance=%d): %w", r.sourceIdx, err)
		}
		if !claimed {
			return false, nil
		}
		r.source.Operation.Step = workload.UpdateStepSurgeDrain
	}
	return true, nil
}

// deleteDrainedSource takes the source gang out of rotation, waits until
// every routed member has left its per-revision Service's EndpointSlices,
// then deletes the whole gang. The gate flip only starts the drain: the
// endpoint controller has to publish the change before kube-proxy stops
// routing, so a delete on the same pass would remove a leader the data plane
// still sends requests to. A member already on its way out is out of
// rotation by other means. The pass stops while any source pod stands.
func (r *gangSurgeRoll) deleteDrainedSource(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if len(r.sourcePods) == 0 {
		return true, nil
	}
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, r.plan.Component
	if _, err := unroutePodsUnder(ctx, deps, r.sourcePods, podreadiness.WriterUpdateSurgeDrain, gangSurgeDrainKey(r.sourceIdx), nil); err != nil {
		return false, fmt.Errorf("drain source gang pod %w", err)
	}
	// The batcher lists each Service's slices once for the gang.
	drainer := drain.NewBatcher(deps.Reader(), ns)
	drained, derr := podsDrainedFromRouting(ctx, drainer, input, r.plan, r.sourceIdx, r.sourcePods)
	if derr != nil {
		return false, derr
	}
	if !drained {
		return false, nil
	}
	if !deps.ExpectationsCache().Satisfied(ns, owner, comp, r.sourceIdx) {
		return false, nil
	}
	for _, pod := range r.sourcePods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		deps.ExpectationsCache().ExpectDeletes(ns, owner, comp, r.sourceIdx, 1)
		if err := deps.Client.Delete(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedDelete(ns, owner, comp, r.sourceIdx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("delete source pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return false, nil
}

// promoteReplacement writes the replacement Ready on the pair's revision
// once the source's pods are gone. A newer target revision stays visible as
// a RunningRevision that differs from it, and starts another update.
func (r *gangSurgeRoll) promoteReplacement(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if r.promoted {
		return true, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock != nil {
		promoted, err := promoteGangSurgeTarget(ctx, input, r.source, r.replacement, r.revision)
		if err != nil {
			return false, fmt.Errorf("promote surge gang (instance=%d): %w", r.surgeIdx, err)
		}
		return promoted, nil
	}
	if err := status.StampReadyOnRevision(ctx, input, r.surgeIdx, r.revision); err != nil {
		return false, fmt.Errorf("promote surge gang (instance=%d): %w", r.surgeIdx, err)
	}
	return true, nil
}

// removeSource ends the roll: the Instance goes on under the replacement's
// index, so a move still pending on the source's index follows it there
// before the index is released and the record never names an Instance that
// is gone; then the source's row is removed and the completed roll is
// published.
func (r *gangSurgeRoll) removeSource(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if !gangSurgeSourceOwnsRemoval(r.source, r.surgeIdx) {
		return false, nil
	}
	if err := followHandoffForPendingMigrations(ctx, deps, input, r.sourceIdx, r.surgeIdx); err != nil {
		return false, fmt.Errorf("rebind pending migrations of the promoted gang (source=%d target=%d): %w", r.sourceIdx, r.surgeIdx, err)
	}
	removed, rerr := status.FinalizeAndRemove(ctx, deps, input, r.sourceIdx, r.source)
	if rerr != nil {
		return false, fmt.Errorf("finalize source Instance (instance=%d): %w", r.sourceIdx, rerr)
	}
	if !removed {
		return false, nil
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonRecreateUpdateCompleted,
		"OMENative %s gang surge complete: instance %d promoted to revision %s",
		workload.InstanceKey(r.plan.Component, r.sourceIdx), r.surgeIdx, r.revision)
	return true, nil
}

// supersededGangSurgeKeepsPin reports whether a gang surge held to a
// revision the roll has left finishes on that revision instead of ending:
// yes when the source gang is gone and the whole replacement gang is
// runtime-ready, so the replacement is the Instance's only pod set, and yes
// when the replacement gang is in rotation and the pairing drain gate keeps
// it there, with the gate's reason returned. Otherwise the surge ends while
// the source stands, whether or not the replacement is Ready, for the
// reasons surgeUpdate's redirect gives.
func supersededGangSurgeKeepsPin(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, surgeIdx int32, pinnedRevision string) (bool, string, error) {
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, plan.Component
	sourcePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, inst.Index)
	if err != nil {
		return false, "", fmt.Errorf("list source gang pods for supersede check (instance=%d): %w", inst.Index, err)
	}
	surgePods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, surgeIdx)
	if err != nil {
		return false, "", fmt.Errorf("list surge gang pods for supersede check (instance=%d): %w", surgeIdx, err)
	}
	if anyPodStanding(sourcePods) {
		reason, kept := replacementKeptForPairing(input, surgePods)
		return kept, reason, nil
	}
	// The replacement is complete at the width the pair's revision records.
	pinned := workload.InstancePlan{Runners: plan.RunnersFor(inst, pinnedRevision)}
	return int32(len(surgePods)) >= pinned.TotalPods() && query.AllPodsRuntimeReady(surgePods), "", nil
}

// gangSurgeSourceOwnsRemoval reports whether the row is a surge source whose
// operation names surgeIdx as its replacement's index.
func gangSurgeSourceOwnsRemoval(row *workload.InstanceStatus, surgeIdx int32) bool {
	return gangSurgeSourceClaim(row) && *row.Operation.SurgeIndex == surgeIdx
}

// gangSurgePromotedTargetMatches reports whether the row at the surge index
// already reads as a promoted replacement: Ready on targetRevision, created
// once and never recreated, in its first pod slot, with no operation and no
// target of its own.
func gangSurgePromotedTargetMatches(row *workload.InstanceStatus, targetRevision string) bool {
	return row != nil && row.Incarnation == 1 && row.ActiveOrdinal == 0 &&
		row.Phase == workload.InstancePhaseReady && row.RunningRevision == targetRevision &&
		row.TargetRevision == "" && row.Operation == nil
}

// confirmPromotedGangSurgeTarget proves, with a write that changes nothing,
// that both rows of the pair still read on the adapter's fresh copy as this
// pass saw them.
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

// claimGangSurgeDrain moves the source from Surge to SurgeDrain while the
// pair's rows still match.
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

// promoteGangSurgeTarget writes the replacement Ready on targetRevision once
// the source is at SurgeDrain and the pair's rows still match.
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

// resetGangSurgeSourceAfterTargetConflict releases a source's claim when the
// surge index holds a row other than the pair's replacement; the row at the
// index is left as it is.
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

// gangSurgeEnding is one gang surge pair as the pass ends it: the pair's
// rows, the revision the source goes back to, the revision and failure the
// end charges, and how the source is left.
type gangSurgeEnding struct {
	plan        workload.ComponentPlan
	sourceIdx   int32
	surgeIdx    int32
	source      *workload.InstanceStatus
	replacement *workload.InstanceStatus
	// revision is the revision the pair was held to, read from the source's
	// operation before any write; the events name it.
	revision string
	// sourceRunningRev is the revision the source goes back to; empty when
	// it never ran one and re-enters as a fresh start.
	sourceRunningRev string
	// failedTargetRev, failureReason and cause are what the end charges to
	// the revision's retry record; an empty failedTargetRev charges nothing.
	failedTargetRev string
	failureReason   string
	cause           workload.FailureCause
	end             gangSurgeEnd
	// guarded: the batch writer or the finalizer is wired, so the
	// replacement's row is moved to its cleanup step before any outside
	// effect and the pair ends in one write.
	guarded bool
}

// abandonFailedGangSurge ends a pair whose replacement is retired: the
// replacement's row goes to its cleanup step, its pods leave rotation and
// are deleted, then one write removes the row and leaves the source as end
// says: Ready on its running revision, Failed as a fresh start when it never
// ran one, or Failed with no attempt when the pair's revision holds. The
// failure given is charged to failedTargetRev's retry record in that same
// write, and cause decides how it counts (see instanceFailureCause). The
// pass stops at each step that has to wait; the phase list is the whole
// order of the end.
func abandonFailedGangSurge(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, sourceIdx, surgeIdx int32, sourceRunningRev, failedTargetRev, failureReason string, cause workload.FailureCause, end gangSurgeEnd) (bool, error) {
	ending := &gangSurgeEnding{
		plan:             plan,
		sourceIdx:        sourceIdx,
		surgeIdx:         surgeIdx,
		source:           input.ObservedState.Instance(sourceIdx),
		replacement:      input.ObservedState.Instance(surgeIdx),
		sourceRunningRev: sourceRunningRev,
		failedTargetRev:  failedTargetRev,
		failureReason:    failureReason,
		cause:            cause,
		end:              end,
		guarded:          input.ApplyInstanceMutationsWithRetryBlock != nil || input.FinalizeInstanceResources != nil,
	}
	if ending.source != nil && ending.source.Operation != nil {
		ending.revision = ending.source.Operation.TargetRevision
	}
	ended, err := runGangSurgePhases(ctx, deps, input, []gangSurgePhase{
		ending.claimReplacementCleanup,
		ending.deleteReplacementPods,
		ending.writePairEnd,
	})
	if err != nil || !ended {
		return false, err
	}
	ending.publishEnd(deps, input)
	return false, nil
}

// claimReplacementCleanup puts the replacement's row at its cleanup step
// before any outside effect. On the guarded path the source must still own
// the pair, a missing row is written back in its cleanup form and the pass
// stops, and the row must be this pair's; on the plain path the row must be
// the pair's live claim.
func (e *gangSurgeEnding) claimReplacementCleanup(ctx context.Context, _ workload.Deps, input workload.ReconcileInput) (bool, error) {
	if e.guarded {
		if !gangSurgeSourceOwnsRemoval(e.source, e.surgeIdx) {
			return false, nil
		}
		if e.replacement == nil {
			if _, err := restoreGangSurgeTargetCleanup(
				ctx, input, e.source, e.surgeIdx, e.source.Operation.TargetRevision, e.plan.InstanceReadyTimeout,
			); err != nil {
				return false, fmt.Errorf("restore gang surge target cleanup marker (instance=%d): %w", e.surgeIdx, err)
			}
			return false, nil
		}
		claimed, err := transitionGangSurgeTargetCleanup(ctx, input, e.source, e.replacement)
		if err != nil {
			return false, fmt.Errorf("stamp failed surge cleanup (instance=%d): %w", e.surgeIdx, err)
		}
		if !claimed {
			return false, nil
		}
		e.replacement.Operation.Step = workload.UpdateStepGangSurgeTargetCleanup
	}
	if e.replacement != nil && (!gangSurgeTargetOwnsRemoval(e.replacement, e.guarded) ||
		e.guarded && !status.GangSurgeTargetClaimMatches(e.replacement, e.source.Operation.TargetRevision)) {
		return false, nil
	}
	return true, nil
}

// deleteReplacementPods takes the retired replacement's members out of
// rotation and deletes them under the retired-replacement grace. The source
// keeps its surge step and its budget slot until they are gone. A member
// whose write the apiserver refused is left as the refusal left it until the
// revision in force admits a fresh attempt. The pass stops while any member
// stands.
func (e *gangSurgeEnding) deleteReplacementPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, e.plan.Component
	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), ns, owner, comp, e.surgeIdx)
	if err != nil {
		return false, fmt.Errorf("list failed surge gang pods (instance=%d): %w", e.surgeIdx, err)
	}
	if len(pods) == 0 {
		return true, nil
	}
	if waiting, _ := rejectionParked(input, e.replacement); waiting {
		return false, nil
	}
	if !deps.ExpectationsCache().Satisfied(ns, owner, comp, e.surgeIdx) {
		return false, nil
	}
	// A member in rotation leaves it a pass ahead of its deletion.
	refused := func(pod *corev1.Pod, err error) (bool, error) {
		return disposeRejectedRowWrite(ctx, deps, input, e.surgeIdx, pod.Name, err)
	}
	if unrouted, err := unrouteServingPods(ctx, deps, e.surgeIdx, pods, refused); err != nil {
		return false, fmt.Errorf("unroute abandoned surge gang member (instance=%d): %w", e.surgeIdx, err)
	} else if unrouted {
		return false, nil
	}
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		deps.ExpectationsCache().ExpectDeletes(ns, owner, comp, e.surgeIdx, 1)
		if err := deps.Client.Delete(ctx, pod, abandonedReplacementDeleteOptions(input, pod)...); err != nil {
			deps.ExpectationsCache().ObservedDelete(ns, owner, comp, e.surgeIdx)
			if apierrors.IsNotFound(err) {
				continue
			}
			if handled, derr := disposeRejectedRowWrite(ctx, deps, input, e.surgeIdx, pod.Name, err); handled {
				return false, derr
			}
			return false, fmt.Errorf("delete failed surge pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return false, nil
}

// writePairEnd removes the replacement's row and leaves the source as the
// ending says. The guarded path commits both in one write, so a missing row
// can never be read as an interrupted claim and written back as live work.
// The plain path removes the row, then writes the source Failed when the
// revision holds, else charges the revision and resets the source.
func (e *gangSurgeEnding) writePairEnd(ctx context.Context, deps workload.Deps, input workload.ReconcileInput) (bool, error) {
	if e.guarded {
		complete, err := finalizeAndResetAbandonedGangSurge(ctx, deps, input, e)
		if err != nil {
			return false, fmt.Errorf("finalize abandoned gang surge (instance=%d): %w", e.surgeIdx, err)
		}
		return complete, nil
	}
	removed, err := status.FinalizeAndRemove(ctx, deps, input, e.surgeIdx, e.replacement)
	if err != nil {
		return false, fmt.Errorf("finalize failed surge Instance (instance=%d): %w", e.surgeIdx, err)
	}
	if !removed {
		return false, nil
	}
	if e.end.held != nil {
		if err := status.StampFailed(ctx, input, e.sourceIdx, e.end.held); err != nil {
			return false, fmt.Errorf("hold failed gang surge source (instance=%d): %w", e.sourceIdx, err)
		}
		return true, nil
	}
	if err := recordUpdateFailureInRetryBlock(ctx, input, e.failedTargetRev, e.failureReason, e.cause); err != nil {
		return false, fmt.Errorf("record retry block for failed gang surge (rev=%s): %w", e.failedTargetRev, err)
	}
	if err := status.StampAbandonedSurgeSource(ctx, input, e.sourceIdx, e.sourceRunningRev); err != nil {
		return false, fmt.Errorf("reset failed gang surge source (instance=%d): %w", e.sourceIdx, err)
	}
	return true, nil
}

// publishEnd records how the pair ended and what the source goes back to.
func (e *gangSurgeEnding) publishEnd(deps workload.Deps, input workload.ReconcileInput) {
	reset := fmt.Sprintf("resetting to revision %s for a fresh rollout", e.sourceRunningRev)
	switch {
	case e.end.withdrawn && e.sourceRunningRev != "":
		reset = fmt.Sprintf("the source returns to rotation on revision %s", e.sourceRunningRev)
	case e.end.withdrawn:
		reset = "the source's pods are gone and it re-enters as a fresh start at the roll target"
	case e.sourceRunningRev == "":
		reset = "the source never ran a revision and re-enters as a fresh start"
	}
	source := workload.InstanceKey(e.plan.Component, e.sourceIdx)
	switch {
	case e.end.held != nil:
		workload.RecordWarning(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeHeld,
			"OMENative %s held: revision %s admits no further attempt after the replacement gang (surge-index=%d) lost members once it had taken over serving; the Instance stays Failed until a corrected revision or a reset",
			source, e.revision, e.surgeIdx)
	case e.failedTargetRev != "":
		workload.RecordWarning(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned failed gang surge (surge-index=%d); %s",
			source, e.surgeIdx, reset)
	case e.end.withdrawn:
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned gang surge (surge-index=%d): the roll target moved off revision %s while the replacement gang was short of members, so nothing is rebuilt at it; %s",
			source, e.surgeIdx, e.revision, reset)
	default:
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeAbandoned,
			"OMENative %s abandoned superseded gang surge (surge-index=%d); %s",
			source, e.surgeIdx, reset)
	}
}

// transitionGangSurgeTargetCleanup moves the replacement's row to its cleanup
// step while the pair's rows still match.
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

// restoreGangSurgeTargetCleanup writes the replacement's row back in its
// cleanup form while the source still claims surgeIdx at targetRevision.
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

// finalizeAndResetAbandonedGangSurge is the guarded path's one write: the
// replacement's row removed and the source left as the ending says, both
// fenced on the rows the pass read. The source must still own the pair, and
// the replacement's row, when present, must be the pair's at its cleanup step.
func finalizeAndResetAbandonedGangSurge(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, e *gangSurgeEnding) (bool, error) {
	if e.source == nil || !gangSurgeSourceOwnsRemoval(e.source, e.surgeIdx) {
		return false, nil
	}
	if e.replacement != nil && (!gangSurgeTargetOwnsRemoval(e.replacement, true) ||
		!status.GangSurgeTargetClaimMatches(e.replacement, e.source.Operation.TargetRevision)) {
		return false, nil
	}
	if e.end.held != nil {
		return status.HoldGangSurgeSourceAndRemoveMarker(ctx, deps, input, e.source, e.replacement, e.surgeIdx, e.end.held)
	}
	return status.ResetGangSurgeSourceAndRemoveMarker(
		ctx, deps, input, e.source, e.replacement, e.surgeIdx, e.sourceRunningRev, e.failedTargetRev, e.failureReason, e.cause,
	)
}

// gangSurgeTargetOwnsRemoval reports whether the replacement's row is at the
// step the end expects: the cleanup step when requireCleanupMarker is set,
// else the live claim.
func gangSurgeTargetOwnsRemoval(row *workload.InstanceStatus, requireCleanupMarker bool) bool {
	if row == nil || row.Operation == nil || row.Operation.Type != workload.InstanceOperationUpdate {
		return false
	}
	if requireCleanupMarker {
		return row.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup
	}
	return row.Operation.Step == workload.UpdateStepGangSurgeTarget
}

// rebuildLostReplacement decides, for a source past the drain step, whether
// a replacement gang short of members is rebuilt this pass. A loss is a drop
// in the replacement's member count against the count the status
// publication wrote from the previous pass, less the members the controller's
// own terminal removal took: such a member is recreated at once under the
// attempt as it stands, and members already missing at the published count
// are a rebuild still pending, created and charged nothing. A loss closes
// the attempt and counts it once on the revision's retry record; the next
// pass rebuilds the set at once as the next attempt, under a fresh deadline
// once its members are created, with no wait on the record's backoff: the
// source is out of rotation behind the replacement, so the Instance serves
// nothing until the members are back. A record that holds the revision ends
// the pair with the source Failed and no attempt. Reports whether the caller
// may create the missing members this pass, and whether the source opens its
// next attempt once they are created.
func rebuildLostReplacement(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, src, marker *workload.InstanceStatus, surgeIdx int32, pin string, recycled, observed, size int) (create, rearm bool, err error) {
	// An attempt the placement grace ended rebuilds the members it retired
	// as the next attempt: nothing was lost and nothing is counted, and a
	// token the ended attempt left on the pair holds nothing back.
	if placementGraceEndedAttempt(src) {
		return true, true, nil
	}
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
	return true, true, nil
}

// placementGraceEndedAttempt reports a drain-step continuation whose
// attempt the placement grace ended and no rebuild has opened since: the
// source reads Failed on the scheduler's record, dated after the
// attempt's last progress. The write that opens the next attempt moves
// the progress past the record.
func placementGraceEndedAttempt(src *workload.InstanceStatus) bool {
	return src != nil && src.Phase == workload.InstancePhaseFailed && src.Operation != nil &&
		src.Operation.Step == workload.UpdateStepSurgeDrain &&
		src.LastFailure != nil && src.LastFailure.Reason == workload.WaitingReasonUnschedulable &&
		src.LastFailure.Time.After(src.Operation.LastProgressAt.Time)
}

// retireRefusedMembers deletes the replacement gang's members the
// scheduler still refuses, on the grace a retired replacement is given,
// since none of them ever ran. Reports true while a refused member stands
// or is on its way out, so the caller builds nothing this pass; the
// members already placed keep their placement.
func retireRefusedMembers(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, surgeIdx int32, members []*corev1.Pod) (bool, error) {
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, input.Key.Component
	var refused []*corev1.Pod
	retiring := false
	for _, pod := range members {
		if pod.DeletionTimestamp != nil {
			retiring = true
			continue
		}
		if _, _, ok := workload.PodUnschedulable(pod); ok {
			refused = append(refused, pod)
		}
	}
	if len(refused) == 0 {
		return retiring, nil
	}
	if !deps.ExpectationsCache().Satisfied(ns, owner, comp, surgeIdx) {
		return true, nil
	}
	for _, pod := range refused {
		deps.ExpectationsCache().ExpectDeletes(ns, owner, comp, surgeIdx, 1)
		if err := deps.Client.Delete(ctx, pod, abandonedReplacementDeleteOptions(input, pod)...); err != nil {
			deps.ExpectationsCache().ObservedDelete(ns, owner, comp, surgeIdx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return true, fmt.Errorf("delete refused pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return true, nil
}

// recycledMembers counts the missing members the controller's own terminal
// removal took: the row's failure record names the member and is dated no
// later than the attempt's last progress, the two that removal writes
// together. Such a member's absence is not a loss of the replacement. An
// escalation's record is dated after the last progress, and a loss names no
// pod, so neither reads as the controller's removal.
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
// members are created again: the source goes back to Updating at the drain
// step with the retry counted and a fresh deadline, and the revision's retry
// record reads an attempt in progress. The same write clears the placement
// wait the ended attempt left on the replacement's row, when the row the
// pass read still carries one.
func rearmGangSurgeRebuild(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, src, replacement *workload.InstanceStatus, surgeIdx int32, pin string, rebuilt, size int) error {
	rearmed, err := status.RearmGangSurgeRebuild(ctx, input, src, replacement, plan.InstanceReadyTimeout)
	if err != nil || !rearmed {
		return err
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), eventReasonGangSurgeRebuilt,
		"OMENative %s rebuilt %d of %d member(s) of the replacement gang (surge-index=%d) at revision %s; retry %d under a fresh deadline",
		workload.InstanceKey(plan.Component, src.Index), rebuilt, size, surgeIdx, pin, src.Operation.RetryCount+1)
	return nil
}

// closeLostGangSurgeAttempt ends the attempt whose replacement died after it
// took over serving: the loss counts once on the revision's retry record, by
// the record's own reading (a revision with no record or an attempt in
// progress counts the failure, a Backoff or Held record is already counting
// it), and is recorded on the source, Failed with its surge kept. Returns the
// record as it stands afterwards.
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

// recordLostAttemptWave applies one lost attempt to the revision's retry
// record and returns the record as written; with no retry-record writer the
// loss is not recorded and the record returned is empty.
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
// members after it took over serving.
func replacementLostTermination(surgeIdx int32, lost, size int, now metav1.Time) *workload.InstanceTermination {
	return &workload.InstanceTermination{
		Reason:  ReplacementLostReason,
		Message: fmt.Sprintf("replacement gang (surge-index=%d) lost %d of %d member(s) after it took over serving", surgeIdx, lost, size),
		Time:    now,
	}
}

// holdLostGangSurge ends a gang surge whose revision admits no further
// attempt after its replacement lost members once it had taken over serving:
// the replacement's leftovers go, its row is removed and the source is left
// Failed with no attempt, so the Instance holds until a corrected revision
// or a reset. Multi-pass, through the ending's own phases.
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

// endWithdrawnGangSurge ends a gang surge whose replacement, after it took
// over serving, is short of members while the roll target has moved off the
// pair's revision: a replacement is rebuilt only for a revision the roll
// still wants. The source gang's drain hold is removed so it returns to
// rotation on the revision the roll is back on; with no source pod standing
// it re-enters as the Create pass's fresh start at the target. Multi-pass,
// through the ending's own phases.
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
