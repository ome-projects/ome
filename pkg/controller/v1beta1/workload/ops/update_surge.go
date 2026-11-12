package ops

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// exclusionCoversPod reports whether one of the Instance's recorded
// exclusions names the node pod sits on, for the revision pod carries.
func exclusionCoversPod(exclusions []workload.NodeExclusion, pod *corev1.Pod) bool {
	podRev := query.RevisionFromPod(pod)
	for _, exclusion := range exclusions {
		if exclusion.Node == pod.Spec.NodeName && query.RevisionFromName(exclusion.Revision).Same(podRev) {
			return true
		}
	}
	return false
}

// surgeDrainKey names the serving-gate entry the single-pod surge's drain
// step writes on the source pod, keyed by Instance and replacement
// ordinal so parallel surges never collide. The entry goes with the
// source pod when it is deleted.
func surgeDrainKey(idx int32, newOrdinal int32) string {
	return fmt.Sprintf("update-surge-drain-%d-%d", idx, newOrdinal)
}

const createContainerErrorReason = "CreateContainerError"

// recycleFailedCreateContainerTarget deletes a failed, non-serving
// replacement once the deadline decision has recorded its node as excluded
// for the attempt's revision, so the next attempt builds it elsewhere. The
// source is not touched. handled=true holds the roll on this Instance until
// the failed pod is gone or a retry is admitted; false lets the roll go on.
func recycleFailedCreateContainerTarget(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	inst workload.InstancePlan,
	target *appsv1.ControllerRevision,
	row *workload.InstanceStatus,
	oldPods, surgePods []*corev1.Pod,
) (bool, error) {
	if !failedToCreateAContainerToward(row, target) {
		return false, nil
	}
	if serves, err := sourceServesOn(ctx, deps, oldPods, row.RunningRevision); err != nil || !serves {
		return true, err
	}
	// An empty replacement slot means the earlier delete completed: the roll
	// opens a fresh attempt and creates the replacement.
	if len(surgePods) == 0 {
		return false, nil
	}
	pod, holdsRoll, err := failedReplacementOf(ctx, deps, row, surgePods)
	if pod == nil {
		return holdsRoll, err
	}
	return removeFailedReplacement(ctx, deps, input, inst, pod)
}

// failedToCreateAContainerToward reports the row this step acts on: a
// Failed row with no operation whose last failure is a container that
// could not be created, toward target, the revision the row still names.
func failedToCreateAContainerToward(row *workload.InstanceStatus, target *appsv1.ControllerRevision) bool {
	return row != nil && row.Phase == workload.InstancePhaseFailed && row.Operation == nil &&
		row.LastFailure != nil && row.LastFailure.Reason == createContainerErrorReason &&
		target != nil && row.TargetRevision != "" && row.TargetRevision == target.Name
}

// sourceServesOn reports whether the Instance's one source pod, read live,
// still serves on revision: in rotation, not on its way out, and labelled
// with the revision's hash. Removing the replacement is safe only then; a
// missing or unhealthy source needs an operator, not another removal. A
// source that cannot be read is an error, except one that is gone.
func sourceServesOn(ctx context.Context, deps workload.Deps, oldPods []*corev1.Pod, revision string) (bool, error) {
	if len(oldPods) != 1 || oldPods[0] == nil {
		return false, nil
	}
	liveSource := &corev1.Pod{}
	if err := deps.Reader().Get(ctx, client.ObjectKeyFromObject(oldPods[0]), liveSource); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read serving source before failed-target cleanup %s/%s: %w", oldPods[0].Namespace, oldPods[0].Name, err)
	}
	if liveSource.DeletionTimestamp != nil || !podreadiness.ReadyAndServing(liveSource) {
		return false, nil
	}
	sourceRev := query.RevisionFromName(revision)
	activePodRev := query.RevisionFromPod(liveSource)
	return !sourceRev.IsZero() && !activePodRev.IsZero() && activePodRev.Same(sourceRev), nil
}

// failedReplacementOf reads the replacement the surge slot holds, live:
// the pod to remove when it is the one the row's failure names, on the
// failed revision, neither serving nor ready, and waiting on the container
// that could not be created; nil otherwise, with holdsRoll saying whether
// the roll waits on this Instance (a slot this step cannot read, a pod on
// its way out, or one the failure does not name) or goes on (a pod of
// another revision, or one that serves, is ready or waits in another
// reason). A pod that cannot be read is an error, except one that is gone.
func failedReplacementOf(ctx context.Context, deps workload.Deps, row *workload.InstanceStatus, surgePods []*corev1.Pod) (pod *corev1.Pod, holdsRoll bool, err error) {
	if len(surgePods) != 1 || surgePods[0] == nil {
		return nil, true, nil
	}
	observedTarget := surgePods[0]
	pod = &corev1.Pod{}
	if err := deps.Reader().Get(ctx, client.ObjectKeyFromObject(observedTarget), pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, true, nil
		}
		return nil, true, fmt.Errorf("read failed surge target %s/%s: %w", observedTarget.Namespace, observedTarget.Name, err)
	}
	if pod.DeletionTimestamp != nil || (row.LastFailure.PodName != "" && row.LastFailure.PodName != pod.Name) {
		return nil, true, nil
	}
	failedRev := query.RevisionFromName(row.TargetRevision)
	podRev := query.RevisionFromPod(pod)
	if failedRev.IsZero() || podRev.IsZero() || !podRev.Same(failedRev) {
		return nil, false, nil
	}
	if podreadiness.IsServing(pod) || podreadiness.IsPodReady(pod) || !podHasWaitingReason(pod, createContainerErrorReason) {
		return nil, false, nil
	}
	return pod, true, nil
}

// removeFailedReplacement deletes the failed replacement once the node
// exclusions the deadline decision persisted name the pod's node for the
// pod's own revision (the decision asked for a rebuild elsewhere) and the
// Instance's earlier deletes are seen. The delete goes under the pod's UID
// and is recorded on the expectations first, and the operator is told.
// Until the exclusion is recorded, and once the exclusion budget is spent
// so no new node is recorded, the Instance waits here instead of churning
// pods.
func removeFailedReplacement(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, inst workload.InstancePlan, pod *corev1.Pod) (bool, error) {
	if pod.Spec.NodeName == "" || !exclusionCoversPod(inst.ExcludedNodes, pod) {
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
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonFailedSurgeTargetRecycled,
		"OMENative %s deleted failed non-serving surge target %s on excluded node %s; retry will create a fresh pod",
		workload.InstanceKey(input.Key.Component, inst.Index), pod.Name, pod.Spec.NodeName)
	return true, nil
}

// retireRefusedReplacement removes the replacement an attempt the
// placement grace ended left at the surge slot while the scheduler still
// refuses it. Every attempt builds its own replacement: opened over the
// refused pod, the next attempt would be ended by the same refusal on its
// first pass, once per pass for as long as the pod stands. The pod goes
// on the grace a retired replacement is given, since it never ran, and
// the source keeps serving. Reports true until the slot is free.
func retireRefusedReplacement(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, inst workload.InstancePlan, row *workload.InstanceStatus, surgePods []*corev1.Pod) (bool, error) {
	if row == nil || row.Phase != workload.InstancePhaseFailed || row.Operation != nil ||
		row.LastFailure == nil || row.LastFailure.Reason != workload.WaitingReasonUnschedulable {
		return false, nil
	}
	ns, owner, comp := input.Key.Namespace, input.Key.OwnerName, input.Key.Component
	var refused []*corev1.Pod
	retiring := false
	for _, pod := range surgePods {
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
	if !deps.ExpectationsCache().Satisfied(ns, owner, comp, inst.Index) {
		return true, nil
	}
	for _, pod := range refused {
		deps.ExpectationsCache().ExpectDeletes(ns, owner, comp, inst.Index, 1)
		if err := deps.Client.Delete(ctx, pod, abandonedReplacementDeleteOptions(input, pod)...); err != nil {
			deps.ExpectationsCache().ObservedDelete(ns, owner, comp, inst.Index)
			if apierrors.IsNotFound(err) {
				continue
			}
			return true, fmt.Errorf("delete refused surge pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return true, nil
}

func podHasWaitingReason(pod *corev1.Pod, reason string) bool {
	if pod == nil {
		return false
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, row := range statuses {
			if row.State.Waiting != nil && row.State.Waiting.Reason == reason {
				return true
			}
		}
	}
	return false
}

// podWaitsInTerminalReason reports whether a container of the pod,
// regular or init, waits in a kubelet reason it cannot get itself out of
// (workload.IsTerminalWaitingReason): the pod still shows the failure
// that ended an attempt on it. The reason alone decides, so a replacement
// whose create the runtime never finished is rebuilt too, not waited on
// by a second attempt.
func podWaitsInTerminalReason(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses} {
		for _, row := range statuses {
			if row.State.Waiting != nil && workload.IsTerminalWaitingReason(row.State.Waiting.Reason) {
				return true
			}
		}
	}
	return false
}

// surgeUpdate runs the SurgeThenDrain roll for one Instance. A multi-pod
// Instance replaces its whole gang at a fresh Instance index
// (gangSurgeUpdate). A single-pod Instance runs the phases below in a
// fixed order, from the top on every pass; the row's step and the pods
// found decide how far a pass gets:
//
//	refuse a pod at neither slot; remove a failed replacement from a node
//	the deadline decision excluded; redirect when the roll target moved;
//	open the attempt; remove the replacement the previous attempt failed
//	on; free the replacement's name; create the replacement; wait for it;
//	drain the source; delete the source; promote.
//
// Invariants:
//   - the serving count never drops below the desired count: the
//     replacement is in rotation before the source leaves it;
//   - the live count peaks at desired plus one per Instance while the
//     replacement stands beside the source; the surge budget caps how
//     many Instances do this at once;
//   - no pod is patched in place: the replacement has its own name;
//   - the revision an attempt opened with is the revision it finishes on.
//     A spec change at the first step ends the attempt while the source
//     still serves, unless the pairing gate keeps a serving replacement;
//     past that step the attempt promotes onto its own revision and the
//     next pass rolls on toward the newer target;
//   - every attempt builds its own replacement: a pod the previous attempt
//     failed on is removed before this one waits on anything, so one pod's
//     failure counts once on the retry schedule.
func surgeUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, pods []*corev1.Pod) (bool, error) {
	if inst.TotalPods() > 1 {
		return gangSurgeUpdate(ctx, deps, input, plan, inst, target)
	}
	roll, err := newSinglePodSurge(deps, input, plan, inst, target, pods)
	if err != nil {
		return false, err
	}
	phases := []func(context.Context) (bool, error){
		roll.refusePodsAtNeitherSlot,
		roll.removeFailedReplacementOnExcludedNode,
		roll.removeRefusedReplacement,
		roll.redirectWhenTargetMoved,
		roll.openAttempt,
		roll.removeReplacementOfTheEndedAttempt,
		roll.freeReplacementName,
		roll.createReplacement,
		roll.waitForReplacement,
		roll.drainSource,
		roll.deleteSource,
	}
	for _, phase := range phases {
		if stop, err := phase(ctx); stop || err != nil {
			return false, err
		}
	}
	return roll.promote(ctx)
}

// singlePodSurge is one pass of the replacement roll of a single-pod
// Instance: what the pass read at its start, and what its phases decide
// as they run. The source is the pod the Instance serves from, at the
// slot the row's ActiveOrdinal names; the replacement is the pod the roll
// builds at the other slot. A phase reports stop when the pass ends with
// it; the next pass runs the phases again from the top.
type singlePodSurge struct {
	deps   workload.Deps
	input  workload.ReconcileInput
	plan   workload.ComponentPlan
	inst   workload.InstancePlan
	target *appsv1.ControllerRevision
	row    *workload.InstanceStatus

	// drainer answers every drain question of the pass from one
	// EndpointSlice read per routed Service, so the rotation report and
	// the drain check cannot disagree halfway through.
	drainer *drain.Batcher

	sourceOrdinal      int32
	replacementOrdinal int32
	// replacementTargets is the one pod name the attempt asks for, at the
	// replacement slot.
	replacementTargets []podTarget

	sourcePods        []*corev1.Pod
	replacementPods   []*corev1.Pod
	podsAtNeitherSlot []*corev1.Pod

	// attemptRevision is the revision this attempt drives to, set when the
	// attempt opens: the one an open attempt recorded, else the roll target.
	attemptRevision string
	// attemptOpenedAt is when the attempt this pass drives opened, read
	// when the attempt opens: the recorded start of an attempt already
	// open, else this pass. A replacement built before it is a previous
	// attempt's.
	attemptOpenedAt time.Time
	// keptForPairing is the pairing gate's reason for keeping a serving
	// replacement whose revision the roll target moved past; empty when the
	// gate was not asked or let it go.
	keptForPairing string
}

// newSinglePodSurge reads what one pass of the roll needs: the two slots,
// the pods at each, and the name the replacement takes.
func newSinglePodSurge(deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, pods []*corev1.Pod) (*singlePodSurge, error) {
	if len(inst.Runners) == 0 || inst.Runners[0].Size != 1 {
		return nil, fmt.Errorf("surgeUpdate: unexpected single-pod runner layout (instance=%d)", inst.Index)
	}
	runner := inst.Runners[0]
	row := input.ObservedState.Instance(inst.Index)
	var sourceOrdinal int32
	if row != nil {
		sourceOrdinal = row.ActiveOrdinal
	}
	replacementOrdinal := int32(1) - sourceOrdinal
	roll := &singlePodSurge{
		deps:               deps,
		input:              input,
		plan:               plan,
		inst:               inst,
		target:             target,
		row:                row,
		drainer:            drain.NewBatcher(deps.Reader(), input.Key.Namespace),
		sourceOrdinal:      sourceOrdinal,
		replacementOrdinal: replacementOrdinal,
		replacementTargets: []podTarget{{
			Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, replacementOrdinal),
			Runner:  runner,
			Ordinal: replacementOrdinal,
		}},
	}
	roll.sourcePods, roll.replacementPods, roll.podsAtNeitherSlot = partitionPodsBySurgeOrdinal(pods, sourceOrdinal, replacementOrdinal)
	return roll, nil
}

// refusePodsAtNeitherSlot stops the roll while a pod of the Instance sits
// at neither pod-name slot: the roll cannot tell which pod serves, so it
// warns for each such pod and does nothing.
func (s *singlePodSurge) refusePodsAtNeitherSlot(context.Context) (bool, error) {
	if len(s.podsAtNeitherSlot) == 0 {
		return false, nil
	}
	for _, pod := range s.podsAtNeitherSlot {
		workload.RecordWarning(s.deps.Recorder, workload.EventTarget(s.input), workload.EventReasonFoundOrphan,
			"OMENative %s found pod %s/%s with unexpected ordinal label; refusing to surge-update",
			workload.InstanceKey(s.input.Key.Component, s.inst.Index), pod.Namespace, pod.Name)
	}
	return true, nil
}

// removeFailedReplacementOnExcludedNode deletes a replacement that could
// not start a container on a node the deadline decision has since
// excluded for the attempt's revision, so the next attempt builds it
// elsewhere while the source keeps serving
// (recycleFailedCreateContainerTarget).
func (s *singlePodSurge) removeFailedReplacementOnExcludedNode(ctx context.Context) (bool, error) {
	return recycleFailedCreateContainerTarget(ctx, s.deps, s.input, s.inst, s.target, s.row, s.sourcePods, s.replacementPods)
}

// removeRefusedReplacement deletes the replacement an attempt the
// placement grace ended left at the replacement slot while the scheduler
// still refuses it, so the next attempt builds its own instead of being
// ended by the same refusal; the source keeps serving
// (retireRefusedReplacement).
func (s *singlePodSurge) removeRefusedReplacement(ctx context.Context) (bool, error) {
	return retireRefusedReplacement(ctx, s.deps, s.input, s.inst, s.row, s.replacementPods)
}

// redirectWhenTargetMoved ends an attempt still at its first step whose
// revision the spec no longer asks for. The desired state wins while the
// source still serves: the replacement leaves rotation on one pass and is
// deleted on the next, then the row returns to Ready on the revision it
// runs, and the next pass starts toward the new target through the
// budget and coordination gates again. Capacity never drops, because the
// source serves throughout. A replacement the pairing gate keeps in
// rotation is not removed: the attempt finishes onto its own revision and
// the roll moves on from there. A strategy edit is not a redirect: the
// attempt keeps the mechanism it opened with.
func (s *singlePodSurge) redirectWhenTargetMoved(ctx context.Context) (bool, error) {
	if !s.targetMovedAtSurgeStep() {
		return false, nil
	}
	if reason, kept := replacementKeptForPairing(s.input, s.replacementPods); kept {
		s.keptForPairing = reason
		return false, nil
	}
	if len(s.replacementPods) > 0 {
		return true, s.removeReplacementOfMovedTarget(ctx)
	}
	return true, s.resetSourceAfterMovedTarget(ctx)
}

// targetMovedAtSurgeStep reports an attempt at the Surge step whose
// recorded revision is not the roll target, on a row that is not Failed.
// A Ready replacement whose source is already gone is the Instance's only
// pod set and is not redirected: it promotes onto its own revision and a
// newer target rolls from there. Past the Surge step the source is
// already draining and the attempt finishes.
func (s *singlePodSurge) targetMovedAtSurgeStep() bool {
	row := s.row
	if row == nil || row.Phase == workload.InstancePhaseFailed || row.Operation == nil ||
		row.Operation.Type != workload.InstanceOperationUpdate || row.Operation.Step != workload.UpdateStepSurge {
		return false
	}
	if row.Operation.TargetRevision == "" || row.Operation.TargetRevision == s.target.Name {
		return false
	}
	replacementIsAllThatStands := !anyPodStanding(s.sourcePods) &&
		len(s.replacementPods) > 0 && query.AllPodsRuntimeReady(s.replacementPods)
	return !replacementIsAllThatStands
}

// removeReplacementOfMovedTarget takes the replacement of a redirected
// attempt out of rotation, and deletes it on the pass after, as every
// routed pod the roll removes. A replacement that never carried the
// serving gate goes on the abandoned-replacement grace: it owes no work,
// and its own grace would hold the slot that long.
func (s *singlePodSurge) removeReplacementOfMovedTarget(ctx context.Context) error {
	if !s.expectationsSatisfied() {
		return nil
	}
	unrouted, err := unrouteServingPods(ctx, s.deps, s.inst.Index, s.replacementPods, nil)
	if err != nil {
		return fmt.Errorf("unroute abandoned surge pod (instance=%d): %w", s.inst.Index, err)
	}
	if unrouted {
		return nil
	}
	return s.deletePods(ctx, s.replacementPods, "abandoned surge pod", s.removedReplacementGrace)
}

// resetSourceAfterMovedTarget returns a redirected attempt's row to Ready
// on the revision it runs once the replacement is gone. The next pass
// starts toward the roll target through the trigger and the gates, under
// the strategy the spec asks for now.
func (s *singlePodSurge) resetSourceAfterMovedTarget(ctx context.Context) error {
	// The reset clears the operation, and the row may alias the storage
	// the write lands on: read the revisions out before the write.
	abandonedRev, runningRev := s.row.Operation.TargetRevision, s.row.RunningRevision
	if err := status.StampReadyOnRevision(ctx, s.input, s.inst.Index, runningRev); err != nil {
		return fmt.Errorf("reset abandoned surge source (instance=%d): %w", s.inst.Index, err)
	}
	if s.target.Name == runningRev {
		workload.RecordNormal(s.deps.Recorder, workload.EventTarget(s.input), workload.EventReasonSurgeAbandoned,
			"OMENative %s abandoned surge to withdrawn revision %s; the Instance stays on revision %s",
			workload.InstanceKey(s.input.Key.Component, s.inst.Index), abandonedRev, runningRev)
		return nil
	}
	workload.RecordNormal(s.deps.Recorder, workload.EventTarget(s.input), workload.EventReasonSurgeAbandoned,
		"OMENative %s abandoned superseded surge to %s; re-surging toward %s",
		workload.InstanceKey(s.input.Key.Component, s.inst.Index), abandonedRev, s.target.Name)
	return nil
}

// openAttempt records which revision this attempt drives to, sorts the
// pods by it, and writes the row Updating at the Surge step; a later pass
// finds the row there and the write is a no-op. An open attempt keeps the
// revision it recorded even when the roll target has moved, so a promote
// never writes a revision the pod does not run; a Failed attempt commits
// to the current target again, so a corrective revision replaces the one
// that failed.
func (s *singlePodSurge) openAttempt(ctx context.Context) (bool, error) {
	s.attemptRevision = s.target.Name
	if s.row != nil && s.row.Phase != workload.InstancePhaseFailed && surgeClaim(s.row) && s.row.Operation.TargetRevision != "" {
		s.attemptRevision = s.row.Operation.TargetRevision
	}
	// A pod at the replacement slot that runs another revision is a
	// leftover to drain, not the replacement (reclassifyByRevisionHash).
	s.replacementPods, s.sourcePods = reclassifyByRevisionHash(s.replacementPods, s.sourcePods, query.RevisionFromName(s.attemptRevision))
	observed := s.input.ObservedState.Instance(s.inst.Index)
	wasNotSurging := !surgeClaim(observed)
	s.attemptOpenedAt = s.input.Now()
	if !wasNotSurging && observed.Phase != workload.InstancePhaseFailed {
		s.attemptOpenedAt = observed.Operation.StartedAt.Time
	}
	if err := status.StampSurging(ctx, s.input, s.inst.Index, s.attemptRevision, s.plan.UpdateStrategy.Type, s.plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("stamp surge step (instance=%d): %w", s.inst.Index, err)
	}
	if wasNotSurging {
		workload.RecordNormal(s.deps.Recorder, workload.EventTarget(s.input), workload.EventReasonRecreateUpdateStarted,
			"OMENative %s surge to revision %s (newOrdinal=%d)",
			workload.InstanceKey(s.input.Key.Component, s.inst.Index), s.attemptRevision, s.replacementOrdinal)
	}
	return false, nil
}

// removeReplacementOfTheEndedAttempt deletes the replacement a previous
// attempt failed on once this attempt has opened over it: a pod at the
// replacement slot built before this attempt opened and still waiting in
// a terminal kubelet reason is the one that failed, and a retry builds
// its own replacement, so one pod's failure counts once on the retry
// schedule instead of once per attempt that finds it standing. A pod
// that has started, or waits on nothing terminal, is left to come up
// whichever attempt built it; one in a terminal phase is the next
// phase's to remove; one already on its way out is waited for. The pass
// ends with the delete, and the create waits for the name to be free.
func (s *singlePodSurge) removeReplacementOfTheEndedAttempt(ctx context.Context) (bool, error) {
	openedAt := s.attemptOpenedAt.Truncate(time.Second)
	var failedOn []*corev1.Pod
	for _, pod := range s.replacementPods {
		if pod == nil || pod.DeletionTimestamp != nil || query.IsTerminalPod(pod) ||
			pod.CreationTimestamp.IsZero() || !pod.CreationTimestamp.Time.Before(openedAt) {
			continue
		}
		if podWaitsInTerminalReason(pod) {
			failedOn = append(failedOn, pod)
		}
	}
	if len(failedOn) == 0 {
		return false, nil
	}
	if !s.expectationsSatisfied() {
		return true, nil
	}
	return true, s.deletePods(ctx, failedOn, "replacement of the ended attempt", s.removedReplacementGrace)
}

// freeReplacementName stops the pass while the replacement's pod name is
// held by a pod that will never clear the promote bar: a replacement in a
// terminal phase is deleted now rather than polled to the deadline, and
// one whose kubelet has stopped reporting waits for the force-delete
// sweep, which frees the name only once the node is known to be gone.
// The source keeps serving meanwhile.
func (s *singlePodSurge) freeReplacementName(ctx context.Context) (bool, error) {
	recycling, err := recycleTerminalTargets(ctx, s.deps, s.input, s.inst.Index, s.inst.Index,
		workload.InstanceOperationUpdate, s.replacementPods, s.replacementTargets)
	if err != nil {
		return false, fmt.Errorf("recycle terminal surge target (instance=%d): %w", s.inst.Index, err)
	}
	if recycling {
		return true, nil
	}
	holding, _, err := recoverUnknownPhaseTargets(ctx, s.deps, s.input, s.inst.Index, s.replacementPods, s.replacementTargets)
	if err != nil {
		return false, fmt.Errorf("recover unknown-phase surge target (instance=%d): %w", s.inst.Index, err)
	}
	return holding, nil
}

// createReplacement builds the replacement when its slot is empty and
// ends the pass. A pod of another revision at the slot is deleted first,
// so the create does not fail against its name; the source keeps serving
// the whole time, since a drain before a replacement exists would be an
// outage. An attempt whose source has already left rotation, and whose
// revision the roll target moved past, rebuilds nothing and ends instead
// (endWithdrawnSurge).
func (s *singlePodSurge) createReplacement(ctx context.Context) (bool, error) {
	if len(s.replacementPods) > 0 {
		return false, nil
	}
	if stale := s.sourcePodsAtReplacementSlot(); len(stale) > 0 {
		return true, s.removeStalePodsAtReplacementSlot(ctx, stale)
	}
	if !s.expectationsSatisfied() {
		return true, nil
	}
	if s.attemptRevision != s.target.Name && surgeStepIs(s.input, s.inst.Index, workload.UpdateStepSurgeDrain) {
		return true, endWithdrawnSurge(ctx, s.deps, s.input, s.inst, s.row, s.sourcePods, s.sourceOrdinal, s.attemptRevision, s.target.Name)
	}
	// An attempt at the roll target renders the current template; one the
	// target moved away from renders its stored one.
	tmpl, found, err := pinnedTemplate(ctx, s.deps.Reader(), s.input, s.plan, s.target, s.attemptRevision)
	if err != nil {
		return false, fmt.Errorf("resolve surge template (instance=%d): %w", s.inst.Index, err)
	}
	if !found {
		return true, announceRevisionGone(ctx, s.deps, s.input, s.inst.Index, s.attemptRevision)
	}
	if _, err := createMissingPods(ctx, s.deps, s.input, s.plan, s.inst, s.inst.Index, s.replacementTargets, tmpl); err != nil {
		if createRejectionHandled(err) {
			return true, nil
		}
		return false, fmt.Errorf("create surge pod (instance=%d, ordinal=%d): %w", s.inst.Index, s.replacementOrdinal, err)
	}
	return true, nil
}

// sourcePodsAtReplacementSlot lists the pods sorted into the source set
// that sit at the replacement slot: pods of another revision the attempt
// must clear before it can create there.
func (s *singlePodSurge) sourcePodsAtReplacementSlot() []*corev1.Pod {
	var stale []*corev1.Pod
	for _, pod := range s.sourcePods {
		if ord, ok := query.PodOrdinalFromLabels(pod); ok && ord == s.replacementOrdinal {
			stale = append(stale, pod)
		}
	}
	return stale
}

// removeStalePodsAtReplacementSlot deletes the pods of another revision at
// the replacement slot, on the abandoned-replacement grace of a pod that
// never served, and leaves the source at its own slot untouched.
func (s *singlePodSurge) removeStalePodsAtReplacementSlot(ctx context.Context, stale []*corev1.Pod) error {
	if !s.expectationsSatisfied() {
		return nil
	}
	return s.deletePods(ctx, stale, "stale-slot pod", s.removedReplacementGrace)
}

// waitForReplacement stops the pass until every container of the
// replacement runs, then lets it serve: the serving gate goes true and
// the kubelet folds it into PodReady. The source is not touched before
// this point, which is the no-downtime rule.
func (s *singlePodSurge) waitForReplacement(ctx context.Context) (bool, error) {
	if !query.AllPodsRuntimeReady(s.replacementPods) {
		return true, nil
	}
	for _, pod := range s.replacementPods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, s.deps.Client, s.deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark surge serving (instance=%d, pod=%s): %w", s.inst.Index, pod.Name, err)
		}
	}
	return false, nil
}

// drainSource takes the source out of rotation once the replacement has
// held Ready for the availability window and its routed Service lists it
// for new traffic, and the step may move, then stops the pass until the
// source has left its routed Service. The step
// moves from Surge to SurgeDrain once. At the Surge step the source keeps
// serving while the Component is paused or the drain gate holds, the one
// place in the roll where a hold costs the Instance nothing. Past that
// step the window no longer applies: a replacement that flaps Ready must
// not keep a draining source out of service for another window.
func (s *singlePodSurge) drainSource(ctx context.Context) (bool, error) {
	if len(s.sourcePods) == 0 {
		return false, nil
	}
	window := s.plan.MinReadySeconds
	if !surgeWindowApplies(s.input.ObservedState.Instance(s.inst.Index)) {
		window = 0
	}
	if promotable, wait := query.PodSetPromotable(s.replacementPods, window, s.input.Now()); !promotable {
		s.input.PromoteWindow.Observe(wait)
		return true, nil
	}
	listed, err := s.replacementListedForTraffic(ctx)
	if err != nil {
		return false, err
	}
	if !listed {
		return true, nil
	}
	if surgeStepIs(s.input, s.inst.Index, workload.UpdateStepSurge) {
		if s.plan.Paused || !drainAdmitted(s.input, s.sourcePods, s.attemptRevision) {
			return true, nil
		}
		if s.keptForPairing != "" {
			recordSurgeKeptForPairing(s.deps, s.input, s.inst.Index, s.attemptRevision, s.target.Name, s.keptForPairing)
		}
	}
	if err := status.StampSurgeDrainStep(ctx, s.input, s.inst.Index); err != nil {
		return false, fmt.Errorf("transition surge step to drain (instance=%d): %w", s.inst.Index, err)
	}
	// The flip reads the pass's pod list, which the force-delete sweep ran
	// ahead of: a source it removed is out of rotation already and is
	// skipped like any pod that has gone.
	if _, err := unroutePodsUnder(ctx, s.deps, s.sourcePods, podreadiness.WriterUpdateSurgeDrain, surgeDrainKey(s.inst.Index, s.replacementOrdinal), nil); err != nil {
		return false, fmt.Errorf("mark old not serving (instance=%d): %w", s.inst.Index, err)
	}
	// The per-revision routed Service is the one to ask: the headless
	// Service publishes not-ready addresses and would read as routed.
	drained, err := podsDrainedFromRouting(ctx, s.drainer, s.input, s.plan, s.inst.Index, s.sourcePods)
	if err != nil {
		return false, err
	}
	return !drained, nil
}

// replacementListedForTraffic reports whether the routed Service of the
// attempt's revision publishes the replacement as an endpoint eligible for
// new traffic, or has no Service for it to be published in. The source
// leaves rotation, and is deleted, only behind a replacement that is
// listed: the serving gate the kubelet folds into PodReady reaches the
// Service after a delay of its own, and no request lands on the
// replacement before then. A Service that exists and does not list the
// replacement, or lists it as terminating, holds the pass with the source
// where it stands; the EndpointSlice watch brings the next pass.
func (s *singlePodSurge) replacementListedForTraffic(ctx context.Context) (bool, error) {
	for _, pod := range s.replacementPods {
		serviceName := drainServiceForPod(s.input, s.plan, pod)
		if serviceName == "" {
			continue
		}
		listed, err := s.drainer.IsPodInRotation(ctx, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check rotation on surge replacement %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		if listed {
			continue
		}
		routesNothing, err := s.drainer.RoutesNothing(ctx, serviceName)
		if err != nil {
			return false, fmt.Errorf("check routed Service %s of surge replacement %s/%s: %w", serviceName, pod.Namespace, pod.Name, err)
		}
		if !routesNothing {
			return false, nil
		}
	}
	return true, nil
}

// deleteSource deletes the source once it has left rotation and ends the
// pass; its own termination grace and preStop cover the requests still
// in flight to it. The promote lands on the pass that finds it gone.
func (s *singlePodSurge) deleteSource(ctx context.Context) (bool, error) {
	if len(s.sourcePods) == 0 {
		return false, nil
	}
	if !s.expectationsSatisfied() {
		return true, nil
	}
	return true, s.deletePods(ctx, s.sourcePods, "old pod", s.ownTerminationGrace)
}

// promote makes the replacement the Instance's pod: the row reads Ready
// on the attempt's revision, with ActiveOrdinal at the replacement slot
// and no operation. The attempt's own revision is written, never the
// roll target: when the target moved mid-attempt, the next pass reads the
// difference and opens a fresh attempt toward it.
func (s *singlePodSurge) promote(ctx context.Context) (bool, error) {
	if err := status.StampReadyAtOrdinal(ctx, s.input, s.inst.Index, s.attemptRevision, s.replacementOrdinal); err != nil {
		return false, fmt.Errorf("promote surge (instance=%d): %w", s.inst.Index, err)
	}
	workload.RecordNormal(s.deps.Recorder, workload.EventTarget(s.input), workload.EventReasonRecreateUpdateCompleted,
		"OMENative %s surge to revision %s complete (activeOrdinal=%d)",
		workload.InstanceKey(s.input.Key.Component, s.inst.Index), s.attemptRevision, s.replacementOrdinal)
	return true, nil
}

// expectationsSatisfied reports whether every create and delete the roll
// issued for the Instance has been observed; no phase creates or deletes
// before then.
func (s *singlePodSurge) expectationsSatisfied() bool {
	return s.deps.ExpectationsCache().Satisfied(s.input.Key.Namespace, s.input.Key.OwnerName, s.input.Key.Component, s.inst.Index)
}

// deletePods deletes every pod of pods that is not already on its way
// out, recording the expectation before each call and giving it back
// when the call fails; a pod already gone is skipped. what names the pods
// in an error; grace is the termination grace each delete carries.
func (s *singlePodSurge) deletePods(ctx context.Context, pods []*corev1.Pod, what string, grace func(*corev1.Pod) []client.DeleteOption) error {
	cache := s.deps.ExpectationsCache()
	key := s.input.Key
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		cache.ExpectDeletes(key.Namespace, key.OwnerName, key.Component, s.inst.Index, 1)
		if err := s.deps.Client.Delete(ctx, pod, grace(pod)...); err != nil {
			cache.ObservedDelete(key.Namespace, key.OwnerName, key.Component, s.inst.Index)
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("delete %s %s/%s: %w", what, pod.Namespace, pod.Name, err)
		}
	}
	return nil
}

// removedReplacementGrace is the termination grace of a replacement the
// roll removes before it promoted: the configured abandoned-replacement
// grace when it never served, else its own.
func (s *singlePodSurge) removedReplacementGrace(pod *corev1.Pod) []client.DeleteOption {
	return abandonedReplacementDeleteOptions(s.input, pod)
}

// ownTerminationGrace leaves a pod its own termination grace: a source
// still owes the requests in flight to it.
func (*singlePodSurge) ownTerminationGrace(*corev1.Pod) []client.DeleteOption {
	return nil
}

// surgeDrainHolds are the serving-gate entries a surge's drain step writes
// on the source pods of Instance idx: the single-pod key, named for the
// replacement's ordinal, and the gang key.
func surgeDrainHolds(idx, activeOrdinal int32) []podreadiness.Message {
	return []podreadiness.Message{
		{UserAgent: podreadiness.WriterUpdateSurgeDrain, Key: surgeDrainKey(idx, 1-activeOrdinal)},
		{UserAgent: podreadiness.WriterUpdateSurgeDrain, Key: gangSurgeDrainKey(idx)},
	}
}

// liftSurgeDrainHolds returns pods a surge's drain step took out of
// rotation to it once the attempt that held them is over: a pod that keeps
// serving its revision must be routable before its row reads Ready. A pod
// on its way out, or one carrying no such hold, is left alone.
func liftSurgeDrainHolds(ctx context.Context, deps workload.Deps, idx, activeOrdinal int32, pods []*corev1.Pod) error {
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		for _, hold := range surgeDrainHolds(idx, activeOrdinal) {
			if !podreadiness.ContainsNotReadyKey(pod, hold) {
				continue
			}
			if err := podreadiness.RemoveNotReadyKeyIgnoreNotFound(ctx, deps.Client, deps.Reader(), pod, hold); err != nil {
				return fmt.Errorf("lift the drain hold of pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
	}
	return nil
}

// RestoreSourceRotation returns the source of a single-pod surge attempt
// that is ending to rotation: the drain step took it out behind a serving
// replacement, and an attempt that ends past that step leaves no
// replacement to serve for it, so the source serves again on the revision
// it runs while the next attempt, or the operator, decides what comes
// next. The hold goes before the row is written, so a row that reads
// Failed never stands over a healthy source nothing routes. Any other
// attempt took no source out and nothing is written; a source already on
// its way out is left alone.
func RestoreSourceRotation(ctx context.Context, deps workload.Deps, row workload.InstanceStatus, pods []*corev1.Pod) error {
	op := row.Operation
	if op == nil || op.Type != workload.InstanceOperationUpdate || op.SurgeIndex != nil || !status.SurgeUpdateStep(op.Step) {
		return nil
	}
	return liftSurgeDrainHolds(ctx, deps, row.Index, row.ActiveOrdinal, pods)
}

// endWithdrawnSurge ends a single-pod surge whose replacement is gone after
// the source left rotation, while the roll target has moved off the
// attempt's revision: a replacement is rebuilt only for a revision the roll
// still wants. The source's drain hold is removed so it returns to rotation
// on the revision it runs; with no source pod standing the Instance
// re-enters as the Create pass's fresh start at the target, without a
// repair.
func endWithdrawnSurge(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, inst workload.InstancePlan, row *workload.InstanceStatus, sourcePods []*corev1.Pod, activeOrdinal int32, attemptRevision, targetName string) error {
	if err := liftSurgeDrainHolds(ctx, deps, inst.Index, activeOrdinal, sourcePods); err != nil {
		return err
	}
	runningRev := ""
	if row != nil && anyPodStanding(sourcePods) {
		runningRev = row.RunningRevision
	}
	if err := status.StampAbandonedSurgeSource(ctx, input, inst.Index, runningRev); err != nil {
		return fmt.Errorf("end withdrawn surge (instance=%d): %w", inst.Index, err)
	}
	if runningRev != "" {
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonSurgeAbandoned,
			"OMENative %s abandoned surge to withdrawn revision %s: the replacement was lost after it took over serving and nothing is rebuilt at it; the source returns to rotation on revision %s",
			workload.InstanceKey(input.Key.Component, inst.Index), attemptRevision, runningRev)
	} else {
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonSurgeAbandoned,
			"OMENative %s abandoned surge to withdrawn revision %s: the replacement was lost after it took over serving and no source pod stands, so the Instance re-enters as a fresh start at revision %s",
			workload.InstanceKey(input.Key.Component, inst.Index), attemptRevision, targetName)
	}
	return nil
}

// surgeStepIs reports whether the Instance's in-flight surge attempt is
// on step. A row with no surge step recorded is on the entry step: the
// stamp that opens the cycle writes it there.
func surgeStepIs(input workload.ReconcileInput, idx int32, step string) bool {
	s := input.ObservedState.Instance(idx)
	if s == nil || s.Operation == nil || !status.SurgeUpdateStep(s.Operation.Step) {
		return step == workload.UpdateStepSurge
	}
	return s.Operation.Step == step
}

// partitionPodsBySurgeOrdinal sorts pods by their LabelPodOrdinal label
// into the pods at oldOrdinal, the pods at newOrdinal, and the pods at
// neither slot or with no label, which the roll refuses to act on.
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

// reclassifyByRevisionHash moves a pod found at the replacement slot into
// the source set unless its revision-hash label is the attempt's revision.
// The slot partition keeps the no-downtime rule (the replacement is the
// other slot) but classifies by slot alone, and the replacement slot can
// hold a pod on the wrong revision: a leftover of the previous cycle's
// promote, labelled with the revision just promoted, which a promote
// would otherwise write onto the row while the pod runs an older one; or
// the pod of a spent attempt at a revision the roll replaced, which will
// never become Ready and would hold the roll forever while both
// escalation paths skip it. A pod with no revision-hash label keeps its
// slot, so pods that predate the label are not churned (the partition
// tests hold this). createMissingPods labels every pod it creates with
// the attempt's hash, so a healthy replacement in flight is never moved.
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
