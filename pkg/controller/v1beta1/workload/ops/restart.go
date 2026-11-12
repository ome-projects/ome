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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// RestartRequeueInterval is the wait between passes while a Restart is
// in flight, from the operator's lifecycle.requeue.operation. Exported
// so the dispatcher's pacing stays in lockstep. Zero means
// unconfigured: the caller requeues on the controller's rate-limited
// backoff instead.
func RestartRequeueInterval(input workload.ReconcileInput) time.Duration {
	return input.Requeue.Operation
}

// DetectRestartTrigger fires when the Instance is mid-restart, when
// Phase=Ready and a pod is Failed / the live pod count is below
// desired, when a row demoted for losing every pod (types.DemotedReady)
// is still short of its pod set, or when a materialized Instance has
// lost a gang member in any phase (see instanceLostGangMember). A
// Migrate-owned Instance is suppressed because Migrate's source-pod
// deletion would otherwise trip the "pod count below desired" trigger on
// the source.
//
// The restart policy is read here rather than by the caller: all of the
// above is RestartPolicyRecreateInstance's, while driving an
// open repair and the operation-free crash-loop repair
// (crashLoopRepairReason) belong to every policy.
//
// Ownership: Create materializes an Instance, Restart repairs one that
// was already materialized. Gating repair on Phase=Ready is right for a
// single-pod Instance and backwards for a gang — a gang that loses a
// member before it ever reaches Ready is the case that can never recover
// on its own, because the recovery is gated on a Ready it can no longer
// attain. Only Restart bumps the Incarnation, and only the bump drains
// the surviving members, so any other pass that fills the gap leaves the
// survivors pinned to a topology domain the replacement cannot enter.
//
// Returns (needsRestart, reason, err). When needsRestart is true the
// reason lands on Operation.Reason on the first pass through Restart.
// The trigger reads the row against its own revision; whether that
// revision is still the Component's target is the selection's question
// (RepairYieldsToTarget), asked with the roll target in hand.
//
// Self-lists this Instance's pods live. The dispatcher's per-reconcile
// restart pass instead calls DetectRestartTriggerWithPods after a single
// live List + bucket, so a Component with N gangs costs one live List per
// reconcile instead of N — see that variant.
func DetectRestartTrigger(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) (bool, string, error) {
	s := input.ObservedState.Instance(inst.Index)
	// Restarting and the absent/Migrate-owned cases decide without reading
	// pods at all, so the live List is wasted there. Every other phase can
	// reach a pod-set comparison.
	if s == nil || isMigrateOwnedStatus(s) || s.Phase == workload.InstancePhaseRestarting {
		needs, reason := DetectRestartTriggerWithPods(input, plan, inst, nil)
		return needs, reason, nil
	}

	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index)
	if err != nil {
		return false, "", err
	}
	needs, reason := DetectRestartTriggerWithPods(input, plan, inst, pods)
	return needs, reason, nil
}

// DetectRestartTriggerWithPods is DetectRestartTrigger with this
// Instance's pods supplied by the caller — the dispatcher does a single
// per-Component live List + bucket once per reconcile and threads each
// Instance's slice here, instead of one live List per Instance.
// instancePods must already be filtered to inst.Index. Semantics are
// identical to DetectRestartTrigger; only the read source differs.
func DetectRestartTriggerWithPods(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, instancePods []*corev1.Pod) (bool, string) {
	s := input.ObservedState.Instance(inst.Index)
	if s == nil || isMigrateOwnedStatus(s) {
		return false, ""
	}
	// An open Restart is driven to completion whatever the policy says: the
	// policy governs whether a repair starts, and an Instance left half
	// drained is worse than one never repaired.
	if s.Phase == workload.InstancePhaseRestarting {
		return true, ""
	}
	expected := inst.TotalPods()
	if needs, reason, decided := stoppedRepairDecision(input, s, expected, instancePods); decided {
		return needs, reason
	}
	if reason, fires := crashLoopTrigger(input, plan, s, expected, instancePods); fires {
		return true, reason
	}
	if plan.RestartPolicy != workload.RestartPolicyRecreateInstance {
		return false, ""
	}
	if s.Phase != workload.InstancePhaseReady {
		// Below Ready, the loss of a gang member triggers. Pod-level failure
		// stays Ready-gated: a container that dies while the Instance is
		// still forming is the ordinary boot path.
		if reason, lost := instanceLostGangMember(input, plan, s, expected, instancePods); lost {
			return true, reason
		}
		if !rebuildsAtRunningRevision(input, s) {
			return false, ""
		}
	} else if reason, fires := readyPodTrigger(input, s, expected, instancePods); fires {
		return true, reason
	}
	return podCountTrigger(expected, instancePods)
}

// stoppedRepairDecision decides a repair stopped at Failed, whatever the
// policy says: the policy governs whether a repair starts, and this one did.
// A rebuild the apiserver refused waits for the repair's own schedule, its
// pod set aside. With no live pod left the repair closes and the row is the
// Create pass's fresh start, as every total loss is. A broken set still
// there is retried once the schedule is due. Any other stopped repair is
// left to the readings that follow.
func stoppedRepairDecision(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (needs bool, reason string, decided bool) {
	if !SpentRepair(s) {
		return false, "", false
	}
	if repairRebuildRefused(s) {
		reason, fires := refusedRepairTrigger(input, s)
		return fires, reason, true
	}
	if spentRepairPodSetGone(s, pods) {
		return true, repairCloseReason(s), true
	}
	if reason, fires := spentRepairTrigger(input, s, expected, pods); fires {
		return true, reason, true
	}
	return false, "", false
}

// crashLoopTrigger reads the crash loops a repair answers under any policy.
// A stopped repair whose set serves again, or a serving set whose loop must
// be named or cleared, is an episode of the row. A pod held in a terminal
// kubelet reason past the grace cannot recover on its own and no other pass
// owns an operation-free Ready row. The first crash of the promoted set
// inside its window is remembered on the row, so a later sighting still
// reads as this set's crash once the kubelet's record has rolled past the
// first restart; it counts an attempt only where a rebuild is warranted.
func crashLoopTrigger(input workload.ReconcileInput, plan workload.ComponentPlan, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	if episode := readCrashLoopEpisode(input, s, expected, pods); episode.step != crashLoopNothing {
		return episode.reason, true
	}
	if reason, pastGrace := crashLoopRepairReason(input, plan, s, expected, pods); pastGrace {
		return reason, true
	}
	if l := readCrashLadder(input, s, expected, pods, crashRebuildWarranted(input, plan, s, expected, pods)); l != nil && l.remember {
		return l.crash.restartReason(), true
	}
	return "", false
}

// rebuildsAtRunningRevision reports a row set back to Pending for losing
// every pod whose running revision is not held: its pod count is read as a
// Ready row's is, the repair rebuilds it at the revision it kept, and pods
// that turn out to be present return it to Ready through the promote path.
// Every other row below Ready is its own pass's to build.
func rebuildsAtRunningRevision(input workload.ReconcileInput, s *workload.InstanceStatus) bool {
	return workload.DemotedReady(s) && !rebuildRetryBlockDenies(input, s)
}

// readyPodTrigger reads a Ready Instance's pods: a runner restarted in place
// since the row went Ready, or a pod the kubelet marked Failed, named by its
// container's termination so the event carries the cause (an OOM kill with
// its exit code, a terminal waiting reason) and by the pod's name when the
// pod carries no detail.
func readyPodTrigger(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	if reason, restarted := runnerRestartTrigger(input, s, expected, pods); restarted {
		return reason, true
	}
	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodFailed {
			continue
		}
		if t := workload.PodTermination(pod, metav1.NewTime(input.Now())); t != nil {
			return t.ShortString(), true
		}
		return fmt.Sprintf("pod %s Failed", pod.Name), true
	}
	return "", false
}

// podCountTrigger reads an Instance short of its pod set. Terminal pods are
// absent for the count: a Succeeded pod, or a Failed one that carried no
// termination detail, still leaves the Instance short of its desired set.
func podCountTrigger(expected int32, pods []*corev1.Pod) (bool, string) {
	if live := query.LivePodCount(pods); int32(live) < expected {
		return true, fmt.Sprintf("pod count %d below desired %d", live, expected)
	}
	return false, ""
}

// instanceLostGangMember reports whether a committed multi-pod Instance is
// missing a member and no other operation owns its pod churn. Create commits
// CreatePods before issuing Pod creates, so an interrupted partial create is
// safely repaired as a whole Instance. Failed also proves that an attempt ran;
// unrecognized or operation-free states use PodCount as a legacy fallback.
//
// Requiring a survivor (len(pods) > 0) confines this to partial loss —
// the shape where the survivors hold capacity the replacement needs.
// Total loss stays with Create's fresh-start path, which already honors
// the RetryBlock recorded against a bad revision. It also excludes
// single-pod Instances without a policy check, since 0 < n < 1 is
// unsatisfiable.
//
// Nothing here reads readiness, container state, deadlines or operation
// age: a slow boot must not be expressible in the inputs, or a gang that
// merely takes hours to load weights would be recycled underneath itself.
func instanceLostGangMember(input workload.ReconcileInput, plan workload.ComponentPlan, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	if s == nil || expected <= 0 {
		return "", false
	}
	if plan.RestartPolicy != workload.RestartPolicyRecreateInstance {
		return "", false
	}
	// Only the phases where Create or Restart owns the pod set. Updating,
	// Migrating, Restarting and Deleting are mid-flight through another
	// pass's own drain/recreate, and their transient pod sets are not loss.
	switch s.Phase {
	case workload.InstancePhaseEmpty, workload.InstancePhasePending,
		workload.InstancePhaseCreating, workload.InstancePhaseFailed,
		workload.InstancePhaseReady:
	case workload.InstancePhaseUpdating:
		// A roll attempt parked after its disposition is settled in the
		// phase its pods give it; an attempt in flight owns its set.
		if workload.StateOf(s) != workload.StateUpdateParked {
			return "", false
		}
	default:
		return "", false
	}
	if isMigrateOwnedStatus(s) {
		return "", false
	}
	// A preserved Update or Migrate claim keeps its own pass as owner
	// whatever the phase says. A Restart operation parked at Failed is a
	// spent attempt, not an owner: its deadline elapsed with a member still
	// missing, and only another Restart can rebuild the gang as a whole
	// (incarnation bump, survivors drained), so it may re-arm. Each cycle
	// is bounded by the attempt deadline. A roll attempt parked after its
	// disposition claims nothing and is spent the same way.
	if claim := workload.ClaimOf(s); claim != workload.OwnerNone && claim != workload.OwnerCreate &&
		!(claim == workload.OwnerRestart && s.Phase == workload.InstancePhaseFailed) {
		return "", false
	}
	createCommitted := s.Operation != nil && s.Operation.Step == status.CreateStepCreatePods
	publishedPods, _ := workload.AdapterPublished(s)
	if !createCommitted && s.Phase != workload.InstancePhaseFailed && publishedPods < expected {
		return "", false
	}
	// A terminal pod is not a survivor: it holds no capacity and pins no
	// topology, so it counts as lost, not live.
	live := query.LivePodCount(pods)
	if live == 0 || int32(live) >= expected {
		return "", false
	}
	if rebuildRetryBlockDenies(input, s) {
		return "", false
	}
	return fmt.Sprintf("gang member lost: %d of %d pods present", live, expected), true
}

// rebuildRetryBlockDenies reports whether the retry record of the
// revision a rebuild of this Instance would run forbids it right now. The
// record paces the push's attempts at its revision. A rebuild at the
// revision the Instance already served is no attempt of the push, so a
// record in Backoff, or one whose attempt is in flight, does not hold it;
// a Held record stops every rebuild of its revision until an operator
// releases it. A rebuild of an Instance that never served a revision
// materializes the roll target, a fresh attempt the record paces as it
// paces a Create.
func rebuildRetryBlockDenies(input workload.ReconcileInput, s *workload.InstanceStatus) bool {
	if rev := s.RunningRevision; rev != "" {
		b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev)
		return b != nil && b.State == workload.RetryBlockHeld
	}
	rev := s.TargetRevision
	if rev == "" {
		rev = input.ObservedState.UpdateRevision
	}
	if rev == "" {
		return false
	}
	b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev)
	if b == nil {
		return false
	}
	denied, _ := evaluateRetryBlockGate(b, input.Now(), anyInFlightCreateAttempt(input, allInstances))
	return denied
}

// restartDrainKey identifies a Restart drain writer against one Instance
// materialization. Same shape as the Update drain key, but the writer
// userAgent differs so the two can't cancel each other out.
func restartDrainKey(idx int32, incarnation int64) string {
	return strconv.Itoa(int(idx)) + "-" + strconv.FormatInt(incarnation, 10)
}

// RestartBatch owns what every Restart advanced in one reconcile pass
// shares: the drain observations (EndpointSlices are listed once per
// routed Service) and the earliest moment a node under a rebuilt set
// comes of age under the force-delete policy. Not safe for concurrent
// use.
type RestartBatch struct {
	drainer *drain.Batcher
	// policyBoundaryAt is the earliest moment a node under a pod of a
	// rebuilt set, left in place this pass, comes of age under the
	// force-delete policy; zero while no repair of the pass waits on one.
	policyBoundaryAt time.Time
}

// PolicyBoundaryIn reports how long after now the earliest node under a
// pod of a rebuilt set comes of age under the force-delete policy, zero
// when no repair of the pass waits on one. The reading turns actionable
// on a clock and a node that has stopped reporting emits no event, so
// the dispatcher folds the wait into the pass's requeue.
func (b *RestartBatch) PolicyBoundaryIn(now time.Time) time.Duration {
	if b.policyBoundaryAt.IsZero() {
		return 0
	}
	return b.policyBoundaryAt.Sub(now)
}

// notePolicyBoundary keeps at when it comes before the boundary noted so
// far; a zero at names none.
func (b *RestartBatch) notePolicyBoundary(at time.Time) {
	b.policyBoundaryAt = earlierTime(b.policyBoundaryAt, at)
}

// NewRestartBatch returns the pass-scoped Restart executor. Callers should
// reuse one batch for every selected Instance in the pass.
func NewRestartBatch(deps workload.Deps, input workload.ReconcileInput) *RestartBatch {
	return &RestartBatch{
		drainer: drain.NewBatcher(deps.Reader(), input.Key.Namespace),
	}
}

// Restart drives one Instance through its repair: the old pod set leaves
// traffic and is deleted, the set is rebuilt at the next incarnation (the
// ome.io/instance-incarnation label tells the two apart), and the rebuilt
// set is promoted once it serves. Multi-pass and idempotent; Advance lists
// the phases.
//
// reason lands on Operation.Reason on the first pass only; later passes
// preserve it via the skip-write guard.
//
// Returns done=true once the Instance is back at Phase=Ready with
// Operation=nil; done=false means the caller should requeue and resume.
//
// target is the Component's roll target, nil when it has none. A rebuild
// that has created no pod yet follows it when the row is off it
// (rebuildFollowsTarget), and one whose set carries a revision the roll
// has since moved off replaces that set at the target before it serves
// (replaceRebuiltSetOffTheTarget); every other rebuild renders the
// revision the lost pods ran, which the row records before its drain
// deletes them (revisionPodsRan), unless the row runs the Component's
// current revision or no lost pod is left to read, when it renders the
// revision its row records.
//
// Source-agnostic: callers construct workload.ReconcileInput from their
// own source-of-truth types — today the InferenceReplica adapter
// (inferencereplica.buildReconcileInput) wires this entry point.
func Restart(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, reason string) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Restart: nil client")
	}

	pods, err := query.LiveListPodsForInstance(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Restart: list pods (instance=%d): %w", inst.Index, err)
	}
	return NewRestartBatch(deps, input).Advance(ctx, deps, input, plan, inst, target, reason, pods)
}

// Advance drives Restart from the Instance bucket of an authoritative,
// Component-wide Pod observation supplied by the caller. The dispatcher uses
// this entry point so one decision snapshot drives every selected Instance and
// every restart phase, including the death a newly opened repair records; the
// compatibility Restart entry point above performs a narrow API-server read
// itself. The phases run in a fixed order, from the top on every pass; the
// row and the pods found decide how far a pass gets:
//
//	settle a crash-loop episode; resolve the attempt; sort the pods by
//	incarnation; replace a rebuilt set the roll has moved off; record the
//	revision the lost pods ran; drain the old set; delete the old set;
//	plan the rebuilt set; free the rebuilt set's names; stop when its
//	revision turned Held; create the missing members; mark the rebuilt
//	set serving; wait at the promote bar; promote.
func (b *RestartBatch) Advance(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, reason string, pods []*corev1.Pod) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Restart: nil client")
	}
	if b == nil || b.drainer == nil {
		return false, fmt.Errorf("Restart: nil restart batch")
	}
	repair := b.newInstanceRepair(deps, input, plan, inst, target, reason, pods)
	if settled, err := repair.settleCrashLoop(ctx); settled || err != nil {
		return settled, err
	}
	phases := []func(context.Context) (bool, error){
		repair.resolveAttempt,
		repair.sortPodsByIncarnation,
		repair.replaceRebuiltSetOffTheTarget,
		repair.recordRevisionTheLostPodsRan,
		repair.drainOldSet,
		repair.deleteOldSet,
		repair.planRebuiltSet,
		repair.freeRebuiltSetNames,
		repair.stopIfRevisionHeld,
		repair.createMissingMembers,
		repair.markRebuiltSetServing,
		repair.waitForPromoteBar,
	}
	for _, phase := range phases {
		if stop, err := phase(ctx); stop || err != nil {
			return false, err
		}
	}
	return repair.promote(ctx)
}

// instanceRepair is one pass of the repair of one Instance: what the pass
// read at its start, and what its phases decide as they run. The old set
// is the pods at an incarnation before the attempt's; the rebuilt set is
// the pods at the attempt's. A phase reports stop when the pass ends with
// it; the next pass runs the phases again from the top.
type instanceRepair struct {
	deps   workload.Deps
	input  workload.ReconcileInput
	plan   workload.ComponentPlan
	inst   workload.InstancePlan
	target *appsv1.ControllerRevision
	reason string
	// pods is the Instance's pod set as the caller read it. The trigger
	// selected the row on this read and the repair acts on the same one:
	// the informer can lag the kubelet's report of the death whose only
	// copy the drain deletes, and one read keeps the pass to one Pod List.
	pods []*corev1.Pod
	// row is the row as the pass read it, before any write. A phase that
	// needs the row after the attempt's writes reads it again.
	row *workload.InstanceStatus
	// rowWasRestarting reports whether an earlier pass opened the attempt:
	// the row already read Restarting. The pass that opens it records the
	// failure and publishes the event.
	rowWasRestarting bool
	// drainer answers the drain questions of every Instance in the batch
	// from one EndpointSlice read per routed Service.
	drainer *drain.Batcher
	// batch is the pass-scoped executor the repair runs under; it carries
	// the force-delete policy boundary the repair's waits report.
	batch *RestartBatch

	// incarnation is the one the rebuilt set carries, known once the
	// attempt is resolved: a resumed repair keeps the row's, an opened one
	// takes the next. Never inst.Incarnation, which BuildPlan read earlier.
	incarnation int64
	// oldSet and rebuiltSet are the pods below and at or above the
	// attempt's incarnation, by their incarnation label.
	oldSet, rebuiltSet []*corev1.Pod

	// rebuilt is the Instance plan the rebuilt set renders from: the
	// attempt's incarnation and the runners of the one revision it
	// rebuilds.
	rebuilt workload.InstancePlan
	// followsTarget reports whether the rebuild renders the roll target
	// rather than the revision the row records (rebuildFollowsTarget).
	followsTarget bool
	// rebuiltTargets are the pod names the rebuilt set asks for.
	rebuiltTargets []podTarget
}

// newInstanceRepair reads what one pass of the repair needs: the row, and
// whether it already reads Restarting.
func (b *RestartBatch) newInstanceRepair(deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, reason string, pods []*corev1.Pod) *instanceRepair {
	row := input.ObservedState.Instance(inst.Index)
	return &instanceRepair{
		deps:             deps,
		input:            input,
		plan:             plan,
		inst:             inst,
		target:           target,
		reason:           reason,
		pods:             pods,
		row:              row,
		rowWasRestarting: row != nil && row.Phase == workload.InstancePhaseRestarting,
		drainer:          b.drainer,
		batch:            b,
	}
}

// settleCrashLoop writes the step an operation-free row owes for a crash
// loop of its promoted set, if it owes one, and ends the pass as done: a
// row held Failed whose set serves again returns to Ready, a Ready row on
// a held revision gets the loop named on it, and a note gone stale is
// cleared (readCrashLoopEpisode). Nothing opens and nothing is
// left in flight.
func (r *instanceRepair) settleCrashLoop(ctx context.Context) (bool, error) {
	episode := readCrashLoopEpisode(r.input, r.row, r.inst.TotalPods(), r.pods)
	if episode.step == crashLoopNothing {
		return false, nil
	}
	if err := settleCrashLoopEpisode(ctx, r.input, r.inst.Index, episode); err != nil {
		return false, err
	}
	return true, nil
}

// resolveAttempt decides which attempt the pass drives, from the row and
// the pods found. A repair held at Failed with no live pod left closes,
// and the Create pass rebuilds the Instance as a fresh start. One whose
// rebuilt set came up on its own resumes at the incarnation the pods
// already carry, and ends with the promote. One not yet due on the retry
// schedule stays as it is: the trigger declined the row, and a pass that
// reaches it anyway leaves it held. Every other row opens an attempt at
// the next incarnation (openAttempt).
func (r *instanceRepair) resolveAttempt(ctx context.Context) (bool, error) {
	switch {
	case spentRepairPodSetGone(r.row, r.pods):
		return true, r.closeSpentRepair(ctx)
	case RepairResumes(r.row, r.inst.TotalPods(), r.pods):
		r.incarnation = r.row.Incarnation
		return false, nil
	case spentRepairParked(r.input, r.row, r.inst.TotalPods(), r.pods):
		return true, nil
	default:
		return r.openAttempt(ctx)
	}
}

// closeSpentRepair closes the operation of a repair held at Failed whose
// pods are all gone or terminal: nothing is left to resume or rebuild
// under it, and the row becomes the fresh start the Create pass owns,
// which deletes any pod of it left in a terminal phase and rebuilds under
// the row's incarnation. The pass after reads the closed row.
func (r *instanceRepair) closeSpentRepair(ctx context.Context) error {
	closed, err := status.CloseSpentRepair(ctx, r.input, r.inst.Index, r.row.Operation.ID)
	if err != nil {
		return fmt.Errorf("Restart: close spent repair (instance=%d): %w", r.inst.Index, err)
	}
	if closed {
		workload.RecordNormal(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonRepairClosed,
			"OMENative %s %s; the Create pass rebuilds the Instance as a fresh start",
			workload.InstanceKey(r.input.Key.Component, r.inst.Index), repairCloseReason(r.row))
	}
	return nil
}

// openAttempt writes the row Restarting at the next incarnation, with the
// failure that opened the repair, and publishes RestartTriggered on the
// pass that opens it. A crash of a promoted pod set is a failed attempt
// of the revision that promoted it, whatever trigger read it: the running
// revision's retry schedule counts it first, paces the rebuild, and holds
// the row Failed once it is spent (countCrashOnLadder); an attempt that
// goes on is recorded on that schedule. A pass that found the attempt
// open on its copy of the row goes on with it instead (resumeAttempt).
func (r *instanceRepair) openAttempt(ctx context.Context) (bool, error) {
	if r.rowWasRestarting {
		return r.resumeAttempt(ctx)
	}
	retryRevision := ""
	if r.row != nil && r.row.Phase == workload.InstancePhaseReady {
		held, rev, err := countCrashOnLadder(ctx, r.input, r.plan, r.inst, r.row, r.pods)
		if err != nil || held {
			return true, err
		}
		retryRevision = rev
	}
	incarnation, err := status.StampRestarting(ctx, r.input, r.inst.Index, r.reason, r.plan.InstanceReadyTimeout, r.failureToRecord())
	if err != nil {
		return false, fmt.Errorf("patch status Restarting (instance=%d): %w", r.inst.Index, err)
	}
	r.incarnation = incarnation
	if retryRevision != "" {
		if err := status.RetryBlockAttemptStarted(ctx, r.input, retryRevision); err != nil {
			return false, fmt.Errorf("Restart: start retry ladder attempt (instance=%d): %w", r.inst.Index, err)
		}
	}
	workload.RecordWarning(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonRestartTriggered,
		"OMENative %s restart triggered: %s (incarnation=%d)",
		workload.InstanceKey(r.input.Key.Component, r.inst.Index), r.reason, incarnation)
	return false, nil
}

// resumeAttempt goes on with the attempt an earlier pass opened, at the
// set number the live row carries. The copy the pass read comes from a
// cache that can be a write behind the row: a row the pass before
// promoted or ended carries no attempt, and this pass leaves it alone,
// so the next pass reads the row as it is and the retry schedule, not a
// copy, decides whether a new attempt opens.
func (r *instanceRepair) resumeAttempt(ctx context.Context) (bool, error) {
	set, open, err := status.ResumeRestarting(ctx, r.input, r.inst.Index)
	if err != nil {
		return false, fmt.Errorf("Restart: read the open attempt (instance=%d): %w", r.inst.Index, err)
	}
	if !open {
		return true, nil
	}
	r.incarnation = set
	return false, nil
}

// failureToRecord is the termination the attempt records on the pass
// that opens it, read before the drain deletes the pod and its kubelet
// diagnostics with it, so a gang that keeps restarting leaves a trace.
// For a promoted set whose runner restarted after Ready it is that exit,
// ahead of any member that died beside it: the rebuilt set's death is
// read against a record naming the runner's container, and a member the
// kubelet refused or evicted says nothing about why the set died. Else
// it is the first failed pod's. A pass that found no failing pod (a pod
// count below desired) records nothing and leaves any earlier record as
// it is. Nil after the pass that opened the attempt.
func (r *instanceRepair) failureToRecord() *workload.InstanceTermination {
	if r.rowWasRestarting {
		return nil
	}
	now := metav1.NewTime(r.input.Now())
	if r.row != nil && r.row.Phase == workload.InstancePhaseReady {
		if exit := runnerExitAfterReady(r.pods, r.row.ReadySince, now); exit != nil {
			return exit
		}
	}
	return firstFailedPodTermination(r.pods, now)
}

// sortPodsByIncarnation splits the pods into the old set and the rebuilt
// set by their incarnation label. A pod with no such label stops the
// repair: a stripped label, or a pod another controller labeled as
// managed by OMENative, would otherwise be deleted with the old set. The
// repair warns for each such pod and does nothing until the operator
// relabels or deletes it.
func (r *instanceRepair) sortPodsByIncarnation(context.Context) (bool, error) {
	var unlabelled []*corev1.Pod
	r.oldSet, r.rebuiltSet, unlabelled = query.PartitionPodsByIncarnation(r.pods, r.incarnation)
	if len(unlabelled) == 0 {
		return false, nil
	}
	for _, pod := range unlabelled {
		workload.RecordWarning(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonFoundOrphan,
			"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to delete",
			workload.InstanceKey(r.input.Key.Component, r.inst.Index), pod.Namespace, pod.Name)
	}
	return true, nil
}

// replaceRebuiltSetOffTheTarget replaces the set the attempt built when
// the roll has moved off the revision it carries before the set came to
// serve: the row's set number moves on in one write
// (status.RestartReplacesItsRebuiltSet), so the set reads as the old set
// the phases after this one drain and delete, and the pass that finds it
// gone rebuilds the Instance on the target, as the roll replaces its own
// attempt when the target moves under it. The pods are sorted again under
// the new number, so the drain runs in this pass. A row another pass has
// moved on ends this one, which reads it again.
func (r *instanceRepair) replaceRebuiltSetOffTheTarget(ctx context.Context) (bool, error) {
	built, replace := r.rebuiltSetOffTheTarget()
	if !replace {
		return false, nil
	}
	incarnation, moved, err := status.RestartReplacesItsRebuiltSet(ctx, r.input, r.inst.Index, r.incarnation, r.plan.InstanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("Restart: replace the rebuilt set the roll moved off (instance=%d): %w", r.inst.Index, err)
	}
	if !moved {
		return true, nil
	}
	workload.RecordNormal(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonRestartRetargeted,
		"OMENative %s restart replaces the pod set built on %s at %s (incarnation=%d)",
		workload.InstanceKey(r.input.Key.Component, r.inst.Index), built, r.target.Name, incarnation)
	r.incarnation = incarnation
	return r.sortPodsByIncarnation(ctx)
}

// rebuiltSetOffTheTarget reports whether the set the attempt built is one
// the repair replaces at the roll target now, and names the revision it
// carries: live pods of it exist and agree on a revision other than a
// target the roll takes a rebuild toward (rollTakesTheRebuild: the roll
// could start on it, is not held on it by its retry schedule, and its
// partition does not hold the Instance), and the set is short of
// serving, a member missing or a pod not yet runtime-ready. A set one
// promote from serving is kept: it promotes on the revision it carries and
// the roll replaces it from Ready under its budget. A paused Component, a
// Held target, a roll held on its target by its retry schedule or a held
// Instance keeps the set as well, as it keeps a rebuild that has created
// no pod on the revision the row records.
func (r *instanceRepair) rebuiltSetOffTheTarget() (string, bool) {
	live := query.ExcludeTerminalPods(r.rebuiltSet)
	if len(live) == 0 || r.target == nil || !rollTakesTheRebuild(r.input, r.plan, r.inst.Index, r.target) {
		return "", false
	}
	built := revisionPodsRan(r.input, r.plan, live)
	if built == "" || built == r.target.Name {
		return "", false
	}
	set := r.inst
	set.Incarnation = r.incarnation
	set.Runners = r.plan.RunnersFor(r.inst, built)
	complete := len(missingPodTargets(podSetNamesOf(r.input, r.plan, set), live)) == 0
	if complete && query.AllPodsRuntimeReady(live) {
		return "", false
	}
	return built, true
}

// recordRevisionTheLostPodsRan writes on the row the revision the old set
// ran when the row records another: a roll writes the row only at
// promote, so the row can lag its pods, and the rebuild renders what the
// pods ran. A row on the Component's current revision keeps it; a held
// target returns there.
func (r *instanceRepair) recordRevisionTheLostPodsRan(ctx context.Context) (bool, error) {
	rev := revisionPodsRan(r.input, r.plan, r.oldSet)
	if rev == "" || rev == rebuildRevision(r.row) || recordsCurrentRevision(r.input, r.row) {
		return false, nil
	}
	if err := status.StampRestartRevision(ctx, r.input, r.inst.Index, rev); err != nil {
		return false, fmt.Errorf("Restart: record the revision the lost pods ran (instance=%d): %w", r.inst.Index, err)
	}
	return false, nil
}

// drainOldSet takes the old set out of traffic and stops the pass until
// it has left its routed Services. An old pod its kubelet has stopped
// reporting is not drained: no kubelet is there to honor a graceful
// delete, and a pod deleted that way holds its name for its whole grace.
// The force-delete sweep frees it once the node is known to be gone, and
// the drain of the set waits while such a pod is still held.
func (r *instanceRepair) drainOldSet(ctx context.Context) (bool, error) {
	if len(r.oldSet) == 0 {
		return false, nil
	}
	// A repair cannot drain the serving members without a named version
	// to rebuild. Preserve them while revision identity is unresolved.
	rev := query.RevisionFromName(rebuildRevision(r.input.ObservedState.Instance(r.inst.Index)))
	if rebuildFollowsTarget(r.input, r.plan, r.inst.Index, r.target, r.rebuiltSet) {
		rev = query.RevisionOf(r.target)
	}
	if err := query.RequireNamedRevision(rev); err != nil {
		return false, fmt.Errorf("instance %d cannot drain for restart: %w", r.inst.Index, err)
	}
	holding, _, err := recoverUnknownPhaseTargets(ctx, r.deps, r.input, r.inst.Index, r.oldSet, occupiedTargets(r.oldSet))
	if err != nil {
		return false, fmt.Errorf("Restart: recover unknown-phase pods (instance=%d): %w", r.inst.Index, err)
	}
	if holding {
		return true, nil
	}
	for _, pod := range r.oldSet {
		if !podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodNotServing(ctx, r.deps.Client, r.deps.Reader(), pod, podreadiness.WriterRestartDrain, restartDrainKey(r.inst.Index, r.incarnation)); err != nil {
			if apierrors.IsNotFound(err) || errors.Is(err, podreadiness.ErrPodIdentityChanged) {
				return true, nil
			}
			return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
	}
	drained, err := r.oldSetDrained(ctx)
	if err != nil {
		return false, err
	}
	return !drained, nil
}

// oldSetDrained reports whether every old pod has left the routed Service
// of its revision, named by the pod's revision-hash label, from the
// batch's EndpointSlice read. The per-revision routed Service is the one
// to ask: the per-Component headless Service publishes not-ready
// endpoints for peer discovery and would never read as drained. A pod
// with no revision-hash label is not asked about.
func (r *instanceRepair) oldSetDrained(ctx context.Context) (bool, error) {
	for _, pod := range r.oldSet {
		hash := pod.Labels[query.LabelRevisionHash]
		if hash == "" {
			continue
		}
		serviceName := query.PerRevisionServiceName(r.input.Key.OwnerName, r.plan.Component, hash)
		drained, err := r.drainer.IsPodDrained(ctx, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
		if !drained {
			return false, nil
		}
	}
	return true, nil
}

// deleteOldSet deletes the drained old set and ends the pass; the rebuild
// starts on the pass that finds the set gone. Every delete records its
// expectation first and gives it back when the call fails, since a failed
// call fires no event to count it down. The delete carries the pod's
// observed UID, so a same-name pod created since is left alone; a pod
// already gone or changed ends the pass for a fresh read.
func (r *instanceRepair) deleteOldSet(ctx context.Context) (bool, error) {
	if len(r.oldSet) == 0 {
		return false, nil
	}
	if !r.expectationsSatisfied() {
		return true, nil
	}
	cache := r.deps.ExpectationsCache()
	key := r.input.Key
	for _, pod := range r.oldSet {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if pod.UID == "" {
			return false, fmt.Errorf("delete pod %s/%s without an observed UID", pod.Namespace, pod.Name)
		}
		uid := pod.UID
		cache.ExpectDeletes(key.Namespace, key.OwnerName, key.Component, r.inst.Index, 1)
		if err := r.deps.Client.Delete(ctx, pod, client.Preconditions{UID: &uid}); err != nil {
			cache.ObservedDelete(key.Namespace, key.OwnerName, key.Component, r.inst.Index)
			if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
				return true, nil
			}
			return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
	}
	return true, nil
}

// planRebuiltSet decides the pod set the attempt builds: the Instance plan
// at the attempt's incarnation, so Render labels the new pods with it,
// with the runners of the one revision the rebuild renders, the roll
// target when the rebuild follows it, else the revision the row records.
func (r *instanceRepair) planRebuiltSet(context.Context) (bool, error) {
	r.rebuilt = r.inst
	r.rebuilt.Incarnation = r.incarnation
	r.followsTarget = rebuildFollowsTarget(r.input, r.plan, r.inst.Index, r.target, r.rebuiltSet)
	if r.followsTarget {
		r.rebuilt.Runners = r.plan.RunnersFor(r.inst, r.target.Name)
	} else {
		r.rebuilt.Runners = r.plan.RunnersFor(r.inst, rebuildRevision(r.input.ObservedState.Instance(r.inst.Index)))
	}
	r.rebuiltTargets = expectedPodNamesForInstance(r.input, r.plan, r.rebuilt)
	return false, nil
}

// freeRebuiltSetNames stops the pass while a name of the rebuilt set is
// held by a pod that will never run. A pod of the new incarnation that
// died (rejected at admission, evicted) holds its stable name and is
// deleted now; the name is created on a later pass. A pod whose node
// stopped reporting it may still run: its name stays occupied until the
// force-delete sweep proves the node gone, and the repair reports that
// wait instead of polling a name no kubelet answers for. A pod of the set
// that never ran a container on such a node served nothing and holds no
// state, so once the node is proven gone under the configured policy it
// is removed and the name is created again on a live node; the roll's
// own starts and every pod that ran keep the wait. An unfinished Restart
// requeues at RestartRequeueInterval, or sooner when a node under the
// set comes of age under the policy before that: the batch carries the
// earliest such moment to the dispatcher, so the reading is taken when
// it turns actionable rather than on the pass after.
func (r *instanceRepair) freeRebuiltSetNames(ctx context.Context) (bool, error) {
	nameHeld, err := recycleTerminalPods(ctx, r.deps, r.input, r.inst.Index, r.inst.Index, workload.InstanceOperationRestart, terminalTargetPods(r.rebuiltSet, r.rebuiltTargets))
	if err != nil {
		return false, fmt.Errorf("Restart: recycle terminal pods (instance=%d): %w", r.inst.Index, err)
	}
	if nameHeld {
		return true, nil
	}
	holding, holdAt, err := recoverUnknownPhaseTargets(ctx, r.deps, r.input, r.inst.Index, r.rebuiltSet, r.rebuiltTargets)
	if err != nil {
		return false, fmt.Errorf("Restart: recover unknown-phase pods (instance=%d): %w", r.inst.Index, err)
	}
	r.batch.notePolicyBoundary(holdAt)
	removed, removeAt, err := forceDeleteNeverRanPodsOnNodeDeath(ctx, r.deps, r.input, r.inst.Index, r.rebuiltSet)
	if err != nil {
		return false, fmt.Errorf("Restart: remove pods that never ran on a lost node (instance=%d): %w", r.inst.Index, err)
	}
	r.batch.notePolicyBoundary(removeAt)
	return holding || removed, nil
}

// stopIfRevisionHeld ends the repair before its first create when the
// retry record of the revision the rebuilt set would run turned Held
// while the repair was open: another Instance's crash spent the record's
// last attempt between the trigger, which read a record that admitted
// the rebuild, and the pass that reaches the create. A held revision
// admits no new set, so the row is written Failed with no operation and
// the failure that opened the repair kept, and the InstanceFailed warning
// names the hold, as the crash of a serving set under a held revision is
// written. The Create pass owns the row from there: it rebuilds the
// Instance once the operator releases the revision or pushes another,
// and a push that corrects the revision takes the row first, as it takes
// every Instance that serves nothing. A rebuilt set with a live pod is
// finished: the record admitted that set when it was created, and a
// gang left half built serves nobody. A rebuild that follows the roll
// target is not stopped: it follows a target whose record admits a
// start, or renders the target's template for a row that never recorded
// a revision, which is kept whatever the hold says.
func (r *instanceRepair) stopIfRevisionHeld(ctx context.Context) (bool, error) {
	if r.followsTarget || len(query.ExcludeTerminalPods(r.rebuiltSet)) > 0 {
		return false, nil
	}
	rev := rebuildRevision(r.input.ObservedState.Instance(r.inst.Index))
	block := workload.FindRetryBlock(r.input.ObservedState.RetryBlocks, rev)
	if block == nil || block.State != workload.RetryBlockHeld || !heldSinceTheRepairOpened(block, r.row) {
		return false, nil
	}
	if err := status.StampFailed(ctx, r.input, r.inst.Index, nil); err != nil {
		return false, fmt.Errorf("Restart: stop the rebuild on a held revision (instance=%d): %w", r.inst.Index, err)
	}
	recorded, err := status.Announce(ctx, r.input, r.inst.Index, workload.EventReasonInstanceFailed)
	if err != nil {
		return false, fmt.Errorf("Restart: record that the rebuild stopped on a held revision (instance=%d): %w", r.inst.Index, err)
	}
	if recorded && r.input.WarnInstanceFailed != nil {
		podName, reason := "", "repair"
		if r.row != nil && r.row.LastFailure != nil {
			podName, reason = r.row.LastFailure.PodName, r.row.LastFailure.Reason
		}
		r.input.WarnInstanceFailed(r.inst.Index, podName,
			fmt.Sprintf("%s: revision %s held after %d failed attempts, so the repair builds no set on it", reason, rev, block.AttemptsStarted))
	}
	return true, nil
}

// heldSinceTheRepairOpened reports whether the record's last counted
// failure, the one that holds it, landed once the repair had started. A
// record held before the repair opened was read by the trigger, which
// admits the rebuild in that case: a set waiting at Failed
// on a held rollback target that lost a member is rebuilt there. A record
// or an attempt with no time recorded reads as held before.
func heldSinceTheRepairOpened(block *workload.RetryBlock, row *workload.InstanceStatus) bool {
	if block.LastFailureAt == nil || row == nil || row.Operation == nil || row.Operation.StartedAt.IsZero() {
		return false
	}
	return !block.LastFailureAt.Before(&row.Operation.StartedAt)
}

// createMissingMembers creates the pods of the rebuilt set that no live
// pod holds the name of, and ends the pass. The pass that reaches here
// found no old pod; a same-name pod created since makes the create return
// AlreadyExists, which createMissingPods treats as converged. A revision
// that is gone creates nothing and says so (announceRevisionGone).
func (r *instanceRepair) createMissingMembers(ctx context.Context) (bool, error) {
	missing := r.missingMembers()
	if len(missing) == 0 {
		return false, nil
	}
	if !r.expectationsSatisfied() {
		return true, nil
	}
	tmpl, found, err := r.rebuiltSetTemplate(ctx)
	if err != nil {
		return false, err
	}
	if !found {
		return true, announceRevisionGone(ctx, r.deps, r.input, r.inst.Index, rebuildRevision(r.input.ObservedState.Instance(r.inst.Index)))
	}
	if _, err := createMissingPods(ctx, r.deps, r.input, r.plan, r.rebuilt, r.inst.Index, missing, tmpl); err != nil {
		if createRejectionHandled(err) {
			return true, nil
		}
		return false, err
	}
	return true, nil
}

// missingMembers lists the names of the rebuilt set no live pod holds.
func (r *instanceRepair) missingMembers() []podTarget {
	existingByName := query.IndexPodsByName(query.ExcludeTerminalPods(r.rebuiltSet))
	missing := make([]podTarget, 0)
	for _, target := range r.rebuiltTargets {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}
	return missing
}

// rebuiltSetTemplate is the template the rebuilt set renders from, paired
// with the revision its pods are labeled with. A rebuild that has
// created no pod yet and follows the roll target records that revision
// on the row before the first create, so a pass interrupted between the
// two still renders what the row says, and the update trigger reads the
// Instance as on target once it is Ready; it renders the target's current
// template. Every other rebuild renders the stored template of the
// revision its row records: a repair under a pause or a Held target, and
// one whose pods already exist on that revision. found is false when that
// revision is gone.
func (r *instanceRepair) rebuiltSetTemplate(ctx context.Context) (podTemplate, bool, error) {
	if !r.followsTarget {
		tmpl, found, err := rebuildTemplate(ctx, r.deps, r.input, r.plan, r.target, r.inst.Index)
		if err != nil {
			return podTemplate{}, false, fmt.Errorf("Restart: resolve rebuild template (instance=%d): %w", r.inst.Index, err)
		}
		// The rebuild renders the revision the row records, the one the
		// Instance ran: a refusal of it is the cluster's answer, retried
		// on the repair's schedule with no revision charged. A rebuild
		// that follows the roll target is the push's first create at that
		// revision and charges it as any push does.
		tmpl.refusal = refusalIsTheRepairsToRetry
		return tmpl, found, nil
	}
	if err := status.StampRestartRevision(ctx, r.input, r.inst.Index, r.target.Name); err != nil {
		return podTemplate{}, false, fmt.Errorf("Restart: record rebuild revision (instance=%d): %w", r.inst.Index, err)
	}
	return desiredTemplate(r.input, r.plan, query.RevisionOf(r.target)), true, nil
}

// markRebuiltSetServing stops the pass until every container of the
// rebuilt set runs, then lets the set serve: the serving gate goes true
// under the lifecycle key, and the kubelet folds it into PodReady. The
// gate is written before the promote bar is asked because the rebuilt
// pods carry the lifecycle hold, and PodReady cannot become true until it
// is released. The restart-drain key lived only on the old pods, which
// are gone.
func (r *instanceRepair) markRebuiltSetServing(ctx context.Context) (bool, error) {
	if !query.AllPodsRuntimeReady(r.rebuiltSet) {
		return true, nil
	}
	for _, pod := range r.rebuiltSet {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, r.deps.Client, r.deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			if apierrors.IsNotFound(err) || errors.Is(err, podreadiness.ErrPodIdentityChanged) {
				return true, nil
			}
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
	}
	return false, nil
}

// waitForPromoteBar stops the pass until the rebuilt set has held Ready
// for the availability window (query.PodSetPromotable), the bar every
// promote in the engine waits at, and records how long until it should
// look again.
func (r *instanceRepair) waitForPromoteBar(context.Context) (bool, error) {
	promotable, wait := query.PodSetPromotable(r.rebuiltSet, r.plan.MinReadySeconds, r.input.Now())
	if promotable {
		return false, nil
	}
	r.input.PromoteWindow.Observe(wait)
	return true, nil
}

// promote records the verified pod version, writes the row Ready with no
// operation and publishes RestartCompleted: the Instance is back in traffic
// at the attempt's incarnation.
func (r *instanceRepair) promote(ctx context.Context) (bool, error) {
	rev := podSetRevision(r.rebuiltSet, r.input.ObservedState.Instance(r.inst.Index), r.target)
	if rev == "" {
		return false, fmt.Errorf("instance %d cannot finish its restart from its pod labels: %w", r.inst.Index, query.ErrRevisionUnknown)
	}
	mutation := status.ReadyOnRevisionMutation(r.inst.Index, rev, r.input.Now())
	if err := r.input.MutateInstance(ctx, mutation.Index, mutation.Mutate); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", r.inst.Index, err)
	}
	workload.RecordNormal(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonRestartCompleted,
		"OMENative %s restart complete (incarnation=%d)",
		workload.InstanceKey(r.input.Key.Component, r.inst.Index), r.incarnation)
	return true, nil
}

// expectationsSatisfied reports whether every create and delete the
// repair issued for the Instance has been observed; no phase creates or
// deletes before then.
func (r *instanceRepair) expectationsSatisfied() bool {
	return r.deps.ExpectationsCache().Satisfied(r.input.Key.Namespace, r.input.Key.OwnerName, r.input.Key.Component, r.inst.Index)
}

// firstFailedPodTermination returns the InstanceTermination of the first
// pod that is Phase=Failed or carries any container-termination diagnostics
// worth preserving across the recreate. Returns nil when no pod yields a
// signal (e.g. the failing pod already vanished — the "pod count below
// desired" trigger). Order-deterministic by the input slice.
func firstFailedPodTermination(pods []*corev1.Pod, now metav1.Time) *workload.InstanceTermination {
	// Prefer an explicitly Failed pod — that's the pod the trigger fired on.
	for _, pod := range pods {
		if pod != nil && pod.Status.Phase == corev1.PodFailed {
			if t := workload.PodTermination(pod, now); t != nil {
				return t
			}
		}
	}
	// Otherwise capture any pod showing a crash / wedge in its container
	// states (a not-yet-Failed-phase pod mid-CrashLoopBackOff still counts
	// as the cause when Restart was triggered for it).
	for _, pod := range pods {
		if pod == nil || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if t := workload.PodTermination(pod, now); t != nil {
			return t
		}
	}
	return nil
}

// runnerExitAfterReady is the failure record of a promoted pod set whose
// runner restarted after the row entered Ready: the kubelet's termination
// for the run that ended when it shows one, else the runner named with
// its exit unobserved, known from the new run's start alone. Either names
// a container, which is what lets the rebuilt set's death be read against
// it. Nil when no runner restarted after Ready.
func runnerExitAfterReady(pods []*corev1.Pod, readySince *metav1.Time, now metav1.Time) *workload.InstanceTermination {
	for _, pod := range pods {
		if _, restarted := RunnerRestartedSinceReady(pod, readySince); !restarted {
			continue
		}
		if t := workload.PodTermination(pod, now); t != nil && t.ContainerName != "" {
			return t
		}
		return runnerExitUnobserved(pod, now)
	}
	return nil
}

// runnerExitUnobserved names pod's runner as the container that died,
// with the exit unobserved, dated now. The record names a container so
// the rebuilt set's death is read against it (RebuiltAfterFailure).
func runnerExitUnobserved(pod *corev1.Pod, now metav1.Time) *workload.InstanceTermination {
	return &workload.InstanceTermination{
		PodName:       pod.Name,
		ContainerName: constants.MainContainerName,
		Reason:        workload.RunnerExitUnobservedReason,
		Message:       workload.RunnerExitUnobservedMessage,
		Time:          now,
	}
}

// runnerCrashTermination is the record of a promoted set's crash: the
// kubelet's termination or waiting reason when it shows one, else the
// runner named with its exit unobserved, since the crash was read from
// the new run's start alone.
func runnerCrashTermination(crash runnerCrash, now metav1.Time) *workload.InstanceTermination {
	if t := workload.PodTerminationWithReason(crash.pod, crash.reason, now); t.ContainerName != "" {
		return t
	}
	return runnerExitUnobserved(crash.pod, now)
}

// rebuildTemplate is the template a repair of Instance idx renders:
// the stored template of the revision it rebuilds at (rebuildRevision),
// stamped with that revision. target is the roll target the stored
// template is composed against. found is false when that revision's
// ControllerRevision is gone. A row that records no revision has an unknown
// template identity. The create guard withholds new members instead of
// mixing labeled pods into an unidentified legacy pod set.
func rebuildTemplate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, idx int32) (podTemplate, bool, error) {
	rev := rebuildRevision(input.ObservedState.Instance(idx))
	if rev == "" {
		return desiredTemplate(input, plan, query.RevisionID{}), true, nil
	}
	return storedTemplate(ctx, deps.Reader(), input, target, rev)
}

// revisionPodsRan names the revision a lost pod set ran: the revision label
// of its pods at their newest incarnation, as the ControllerRevision the
// adapter minted it under. "" when no such pod carries the label or those
// pods disagree.
func revisionPodsRan(input workload.ReconcileInput, plan workload.ComponentPlan, lost []*corev1.Pod) string {
	var newest int64
	hash := ""
	for _, pod := range lost {
		inc, ok := query.InstanceIncarnationFromLabels(pod)
		if !ok {
			continue
		}
		podHash := pod.Labels[query.LabelRevisionHash]
		if podHash == "" {
			continue
		}
		switch {
		case hash == "" || inc > newest:
			newest, hash = inc, podHash
		case inc == newest && podHash != hash:
			return ""
		}
	}
	if hash == "" {
		return ""
	}
	return revisionNamed(input, plan.Component, hash)
}

// recordsCurrentRevision reports whether the row runs the Component's
// current revision: the revision a repair returns the Instance to while the
// roll's target is held or paused, whatever set the lost pods were.
func recordsCurrentRevision(input workload.ReconcileInput, s *workload.InstanceStatus) bool {
	return s != nil && s.RunningRevision != "" && s.RunningRevision == input.ObservedState.CurrentRevision
}

// rebuildRevision names the revision a repair of this row would rebuild
// at: the revision the Instance runs, else the one it was converging to,
// else the one its attempt pinned — an Instance that never ran a revision
// (a gang rebuilt after losing a member while still forming) has only the
// pin on its Operation. Empty when nothing records a revision.
func rebuildRevision(s *workload.InstanceStatus) string {
	if s == nil {
		return ""
	}
	if s.RunningRevision != "" {
		return s.RunningRevision
	}
	if s.TargetRevision != "" {
		return s.TargetRevision
	}
	if s.Operation != nil {
		return s.Operation.TargetRevision
	}
	return ""
}

// RepairYieldsToTarget reports whether a restart selection for inst is
// left to the pass that rebuilds the Instance at the Component's roll
// target instead of opening a repair: the revision the repair would
// rebuild (rebuildRevision) is not target. A crash loop, a lost pod or a
// lost gang member on such a row is unavailability its roll must cross
// anyway, so the update pass replacing the pod set at the target opens
// no further outage, while a repair would rebuild a revision the
// Component does not want and the roll would tear it down again.
// The update pass takes a Ready or Failed row, the Create pass retires a
// first materialization pinned to a superseded revision and materializes
// a demoted row that has no pod left.
//
// Four rows are never yielded: an open Restart, which is driven on — its
// rebuild follows the target itself until its set serves
// (rebuildFollowsTarget, replaceRebuiltSetOffTheTarget); a Migrate-owned
// row, which its record owns; a
// row demoted for losing every pod that holds pods again, which no
// update trigger reads, so its repair rebuilds the revision it records and
// the roll follows once it is Ready; and a row owed a crash-loop episode
// (readCrashLoopEpisode), which is status truth about the set the row
// holds and opens nothing, so the park follows its set through the roll
// that waits to take it. A row that records no revision, or a pass with
// no target, has nothing to compare.
//
// A row whose pods are a superseded revision's leftovers (EvaluateWreckage)
// is yielded whatever revision it records: the update pass's wreckage
// cleanup removes those pods, and while its deletes land one at a time the
// half-gone set reads as a loss that is not the row's own. The row is the
// cleanup's until they are gone and the Create pass's fresh start after,
// both of which rebuild it at the target. A pause withholds the cleanup,
// so under one the repair stands.
//
// The roll the row is left to must be one that can start (rollTargetOpen)
// and one that may take the row (RollHeldIndices), and for a row no
// attempt owns one that is not held on its target by its retry schedule
// (rollHeldOnItsTarget): an Instance kept dark for the length of a pause,
// a Held target, a partition hold or a retry schedule that admits no start
// is worse than one rebuilt at the revision it runs, and a canary step
// holds its stable side for as long as a manual gate waits. The roll takes
// the row once the pause, the hold, the partition or the schedule clears.
func RepairYieldsToTarget(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) bool {
	s := input.ObservedState.Instance(inst.Index)
	if s == nil || s.Phase == workload.InstancePhaseRestarting || isMigrateOwnedStatus(s) {
		return false
	}
	if readCrashLoopEpisode(input, s, inst.TotalPods(), instancePods).step != crashLoopNothing {
		return false
	}
	if !plan.Paused && EvaluateWreckage(s, target, instancePods) {
		return true
	}
	if !rollTargetOpen(input, plan, target) {
		return false
	}
	rev := rebuildRevision(s)
	if rev == "" || rev == target.Name {
		return false
	}
	if RollHeldIndices(input, plan, target)[inst.Index] {
		return false
	}
	if workload.DemotedReady(s) && query.LivePodCount(instancePods) > 0 {
		return false
	}
	// A lost Instance that no attempt owns stays with the repair while the
	// roll is held on its target by its retry schedule: the roll cannot take
	// it now and may never reach the target. A row that carries the roll's
	// own attempt, kept for the retry the schedule admits, stays the
	// roll's: that retry replaces its set, lost members included.
	if s.Operation == nil && rollHeldOnItsTarget(input, target) {
		return false
	}
	return true
}

// rollTargetOpen reports whether target is a roll target the roll could
// start on: the Component has one, is not paused, and the RetryBlock does
// not hold it. A pause withholds the roll, and a Held target admits no
// fresh attempt until an operator releases it; a Backoff block merely
// paces the start.
func rollTargetOpen(input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) bool {
	if target == nil || plan.Paused {
		return false
	}
	block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name)
	return block == nil || block.State != workload.RetryBlockHeld
}

// rollHeldOnItsTarget reports whether the roll is held on target by its
// retry schedule: the last attempt at the target failed and the schedule
// admits no fresh start this pass (RetryBlockDenyingFreshStart) - a
// backoff not yet due, an attempt already under way, or the record Held.
// Such a roll may never reach its target, and an Instance it cannot take
// now would stay down for as long as it waits, so a lost Instance that no
// attempt owns is rebuilt on the revision it runs instead of being left to
// the roll. A backoff that is due with nothing in flight admits a start,
// and the roll takes the row as it takes any row off the target.
func rollHeldOnItsTarget(input workload.ReconcileInput, target *appsv1.ControllerRevision) bool {
	return RetryBlockDenyingFreshStart(input, target) != nil
}

// rollTakesTheRebuild reports whether a rebuild that serves nothing yet
// renders the roll target: the roll could start on it (rollTargetOpen) and
// is not held on it by its retry schedule (rollHeldOnItsTarget), and the
// roll's partition does not hold the Instance (RollHeldIndices).
func rollTakesTheRebuild(input workload.ReconcileInput, plan workload.ComponentPlan, idx int32, target *appsv1.ControllerRevision) bool {
	return rollTargetOpen(input, plan, target) && !rollHeldOnItsTarget(input, target) && !RollHeldIndices(input, plan, target)[idx]
}

// RollHeldIndices is the set of Instances the roll holds on the revision
// they run this pass, read as the update pass reads it: the canary step's
// projected partition, else the user's rollingUpdate partition, over the
// plan's Instances as the pass observed them. The roll takes none of them
// while the hold stands, so a repair of one rebuilds the revision it runs
// and the create pass refills one on it, or rebuilds it there when no pod
// of it is left. Empty when the pass has no roll target. The set is read once per pass (ReconcileInput.Held): reading it
// costs one lookup per planned Instance, and the restart selection asks
// for every row that needs a repair while a roll is open.
func RollHeldIndices(input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) map[int32]bool {
	if target == nil {
		return nil
	}
	if held, ok := input.Held.Recorded(target.Name); ok {
		return held
	}
	partition := EffectivePartition(input.DesiredSpec.Pacing, plan.UpdateStrategy.RollingUpdate)
	held := PartitionHeldIndices(partition, input, plan.Instances, target.Name)
	input.Held.Record(target.Name, held)
	return held
}

// rebuildFollowsTarget reports whether the rebuild of an open repair
// renders the roll target instead of the revision its row records: no pod
// of the rebuild exists yet, the row is off a target the roll could start
// on and is not held on by its retry schedule, and the roll's partition
// does not hold the Instance (rollTakesTheRebuild). The Instance
// serves nothing, so the rebuild is the pass that puts it on the target and
// no pod set is torn down for it; a held Instance is the step's stable
// side, which the step machine alone promotes. A rebuild whose pods exist
// renders the revision they carry; while the roll has moved off that
// revision and the set is short of serving, the repair replaces the set at
// the target (replaceRebuiltSetOffTheTarget), and a set one promote from
// serving promotes on its revision, for the roll to replace from Ready
// under its budget.
//
// A row that records no revision follows any target it has, whatever the
// pause, the hold or the canary step say: its rebuild renders the current
// template, which is the target's, so following changes only the label.
// Ending a failed first start leaves such a row, and a pod without the
// label is invisible to everything that counts pods by revision.
func rebuildFollowsTarget(input workload.ReconcileInput, plan workload.ComponentPlan, idx int32, target *appsv1.ControllerRevision, rebuiltPods []*corev1.Pod) bool {
	if target == nil || query.LivePodCount(rebuiltPods) > 0 {
		return false
	}
	rev := rebuildRevision(input.ObservedState.Instance(idx))
	if rev == "" {
		return true
	}
	return rev != target.Name && rollTakesTheRebuild(input, plan, idx, target)
}

// RunnerRestartedSinceReady reports whether pod's runner container carries
// restart evidence dated after readySince: a current run that began after the
// Instance entered Ready, or a termination that finished after it. Either
// proves the process group formed at Ready has lost this member's original
// process, even though kubelet's in-place restart left the Pod Running under
// the same UID. Sidecar and init containers never count — kubelet's in-place
// restart is their recovery path and does not break the Instance's process
// group. A nil readySince (Instance was last promoted before the field
// existed) reports false; the anchor appears on the next Ready transition.
// A pod created after readySince is not one of the pods that were Ready
// then: its container's first run began after readySince by construction,
// so it reports false too (podCreatedAfterReady).
func RunnerRestartedSinceReady(pod *corev1.Pod, readySince *metav1.Time) (string, bool) {
	if pod == nil || readySince == nil || readySince.IsZero() || pod.DeletionTimestamp != nil || podCreatedAfterReady(pod, readySince) {
		return "", false
	}
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.Name != constants.MainContainerName {
			continue
		}
		if t := cs.LastTerminationState.Terminated; t != nil && t.FinishedAt.After(readySince.Time) {
			reason := t.Reason
			if reason == "" {
				reason = "Error"
			}
			return fmt.Sprintf("pod %s container %s restarted after Ready: %s (exit %d)",
				pod.Name, cs.Name, reason, t.ExitCode), true
		}
		if r := cs.State.Running; r != nil && r.StartedAt.After(readySince.Time) {
			return fmt.Sprintf("pod %s container %s restarted after Ready", pod.Name, cs.Name), true
		}
	}
	return "", false
}

// RunnerRestartedAgainSinceReady reports whether pod's runner container
// has restarted more than once since readySince: the run its last
// termination records had itself begun after the Instance entered Ready,
// so it was already a restart's run, and it ended too. The kubelet keeps
// only the latest termination, and that run's start is what dates the
// restart before it. A container that keeps dying under load reads this
// way from its second crash on; one the kubelet restarted once never
// does, because the run that crashed is the run that was promoted. The
// nil and zero readySince rules of RunnerRestartedSinceReady apply, and so
// does its reading of a pod created after readySince.
func RunnerRestartedAgainSinceReady(pod *corev1.Pod, readySince *metav1.Time) bool {
	if pod == nil || readySince == nil || readySince.IsZero() || pod.DeletionTimestamp != nil || podCreatedAfterReady(pod, readySince) {
		return false
	}
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.Name != constants.MainContainerName {
			continue
		}
		t := cs.LastTerminationState.Terminated
		return t != nil && t.StartedAt.After(readySince.Time)
	}
	return false
}

// RunnerRestartedOnRecord reports whether pod's runner container has
// restarted at all in this pod's life: the kubelet's restart count is
// above zero or a last termination is on record. It is the reading for a
// row that carries no Ready anchor, where RunnerRestartedSinceReady cannot
// place a restart against the entry into Ready: the budgets then prove
// such a runner by its current run alone, so a crash loop never frees a
// slot between two of its crashes for want of an anchor. A pod with no
// restart on record is read off its serving state, as any fresh pod is.
func RunnerRestartedOnRecord(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.Name != constants.MainContainerName {
			continue
		}
		return cs.RestartCount > 0 || cs.LastTerminationState.Terminated != nil
	}
	return false
}

// podCreatedAfterReady reports whether pod was created after the Instance
// entered Ready. readySince names the pods that were Ready at that time, so
// a younger pod is a new pod, built after that set was lost or replaced: a
// run of its container that began after readySince is its first run, not a
// restart. A pod with no creation time recorded counts as one of that set.
func podCreatedAfterReady(pod *corev1.Pod, readySince *metav1.Time) bool {
	return !pod.CreationTimestamp.IsZero() && pod.CreationTimestamp.After(readySince.Time)
}

// Crash-loop repair: a wedge no other pass owns.
//
// No pass owns an operation-free Ready row, so this trigger does: the
// stuck-pod fast escalator qualifies such a row only through the
// wedged-pod shape, which is a revision-hash disagreement, so a pod
// crash-looping on the revision the Instance is supposed to be running
// reaches no escalation path. The trigger belongs to every restart
// policy; RestartPolicy=None keeps its meaning for mere container
// restarts, because a container that died and came back is not a wedge.
//
// The repair is a Restart, not a Failed stamp: the pod set can be
// rebuilt. Routing it through the restart pass means a rebuild that
// takes a serving pod offline is admitted by the per-Component
// unavailability budget before it opens, while a rebuild of a pod set
// that serves nothing removes no serving capacity and is not. The
// coordination gate is not asked either way: the parked member that
// makes the wedge already puts the Instance inside the gate's
// serving-based unavailability. The running revision's retry ladder
// bounds the rebuilds: a crash of the runner after the row entered Ready
// is a failed attempt at that revision, the rebuild waits for the
// ladder's backoff and is its next attempt, and once the ladder holds
// the row parks Failed naming the crash while its set is out of serving
// (a serving set keeps it Ready naming the loop), so a revision that
// keeps crash-looping is held and said so rather than recycled forever. A
// wedge with no such restart answers to the RetryBlock as a repair of a
// never-Ready Instance does.

// ladderReadsUnready reports whether the running revision's ladder reads a
// promoted pod unready past the stuck-pod grace as the wedge of the crash it
// counts: the set came up after a restart the policy answers and went dark.
func ladderReadsUnready(plan workload.ComponentPlan) bool {
	return plan.RestartPolicy == workload.RestartPolicyRecreateInstance
}

// crashLoopRepairReason reports whether this row holds a crash-loop
// wedge the restart pass acts on: the wedge itself is
// evidence.CrashLoopWedge, and the running revision's retry ladder says
// whether the rebuild opens, waits, or the row parks (readCrashLoopRepair).
func crashLoopRepairReason(input workload.ReconcileInput, plan workload.ComponentPlan, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	r := readCrashLoopRepair(input, plan, s, expected, pods)
	if !r.fires {
		return "", false
	}
	return r.restartReason(), true
}

// crashLoopRepair is the restart pass's reading of a crash-loop wedge on
// a Ready row: whether the trigger selects the row this pass, whether the
// selection opens the rebuild, and what the running revision's retry
// ladder says about the crash (nil when the ladder does not read it). A
// selection that fires without opening counts the crash on the ladder or
// parks the row Failed under a ladder that holds.
type crashLoopRepair struct {
	pod    *corev1.Pod
	reason string
	fires  bool
	opens  bool
	ladder *crashLoopLadder
}

func (r crashLoopRepair) restartReason() string {
	return evidence.WedgeReason(r.pod, r.reason)
}

// runnerCrash is the crash of a promoted pod set: a pod carrying the
// row's running revision, still running that revision's own image, whose
// runner restarted after the row entered Ready, with the reason the
// kubelet reports for it (the terminal waiting reason it is parked in,
// else the termination's).
type runnerCrash struct {
	pod    *corev1.Pod
	reason string
}

func (c runnerCrash) restartReason() string {
	return fmt.Sprintf("pod %s crashed after promotion: %s", c.pod.Name, c.reason)
}

// readRunnerCrash finds the crash of a complete pod set on a Ready row
// that is the revision's own. One restart of a runner after Ready is not
// a crash: the kubelet starts it again and the set serves on, or the
// policy rebuilds it; a crash is a loop (runnerLoops). A set short of a
// pod is a loss, not a crash: its repair recovers capacity already gone
// and is not the ladder's to pace. A pod whose runner no longer runs the
// revision's image (podRunsDesiredRunnerImage) is a break made under the
// row, not the revision's failure: its repair re-renders the revision at
// once.
func readRunnerCrash(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (runnerCrash, bool) {
	if s == nil || s.Phase != workload.InstancePhaseReady || s.RunningRevision == "" {
		return runnerCrash{}, false
	}
	live := query.ExcludeTerminalPods(pods)
	if expected <= 0 || int32(len(live)) < expected {
		return runnerCrash{}, false
	}
	running := query.RevisionFromName(s.RunningRevision)
	for _, pod := range live {
		if pod.DeletionTimestamp != nil {
			continue
		}
		if podRev := query.RevisionFromPod(pod); !podRev.IsZero() && !running.IsZero() && !podRev.Same(running) {
			continue
		}
		if _, restarted := RunnerRestartedSinceReady(pod, s.ReadySince); !restarted {
			continue
		}
		if !podRunsDesiredRunnerImage(input, s, pod) || !runnerLoops(input, s, pod) {
			continue
		}
		reason := ""
		if parked, stuck := evidence.PodStuckInTerminalWaiting(pod, input.Now(), 0); stuck {
			reason = parked
		} else if t := workload.PodTermination(pod, metav1.NewTime(input.Now())); t != nil {
			reason = t.Reason
		}
		if reason == "" {
			reason = "Error"
		}
		return runnerCrash{pod: pod, reason: reason}, true
	}
	return runnerCrash{}, false
}

// runnerLoops reports whether a runner that restarted after the row
// entered Ready is crash-looping rather than restarted once: the row
// already remembers this set's crash, the runner restarted again since
// Ready, it is parked in a terminal waiting state such as
// CrashLoopBackOff, it is the first restart of a set that replaced one
// the row rebuilt for a failure just before this set was created (the
// policy's rebuild of a restarted runner, or the repair of a wedge), or
// the running revision's ladder already reads the revision as failing
// where the set stands (revisionFailing), so the revision crashes on set
// after set.
func runnerLoops(input workload.ReconcileInput, s *workload.InstanceStatus, pod *corev1.Pod) bool {
	if crashRemembered(s) || RunnerRestartedAgainSinceReady(pod, s.ReadySince) {
		return true
	}
	if _, stuck := evidence.PodStuckInTerminalWaiting(pod, input.Now(), 0); stuck {
		return true
	}
	return rebuiltAfterFailure(input, s, pod) || revisionFailing(input, s, pod)
}

// revisionFailing reports whether the running revision's retry ladder
// already reads the revision as failing where pod stands: the ladder
// holds, so no restart on the revision is rebuilt and the row parks
// Failed; or the set was created after the ladder first recorded a
// failure, so it is an attempt the ladder admitted, or one built while
// the revision was already failing, and its crash is the revision's next
// failure however the row's own record reads. The first failure is the
// anchor, not the latest: the ladder's record moves on every failure of
// any Instance, so a set built between two of them is still a set built
// on a failing revision.
func revisionFailing(input workload.ReconcileInput, s *workload.InstanceStatus, pod *corev1.Pod) bool {
	block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, s.RunningRevision)
	if block == nil {
		return false
	}
	if block.State == workload.RetryBlockHeld {
		return true
	}
	if block.FirstFailureAt == nil || pod.CreationTimestamp.IsZero() {
		return false
	}
	return !pod.CreationTimestamp.Time.Before(block.FirstFailureAt.Time)
}

// rebuiltAfterFailure reports whether pod belongs to a set the row built
// right after a failure of the set before it, as the roll reads it
// (RebuiltAfterFailure): the row's failure record predates this set's
// Ready and this pod was created within the crash window of that
// failure, and the failure is this revision's own, not one of a set the
// row ran on a superseded revision. With no window configured nothing
// proves a rebuilt set by time, so every set built after the record
// answers it (RebuildAnswersFailure), however long it served.
func rebuiltAfterFailure(input workload.ReconcileInput, s *workload.InstanceStatus, pod *corev1.Pod) bool {
	if s.LastFailure == nil || s.ReadySince == nil || s.LastFailure.Time.After(s.ReadySince.Time) {
		return false
	}
	window := ProvenWindowSeconds(input.DesiredSpec.MinReadySeconds, input.StuckPodGrace)
	if window <= 0 {
		return RebuildAnswersFailure(pod, s.LastFailure, input.RevisionBorn)
	}
	return RebuiltAfterFailure(pod, s.LastFailure, input.RevisionBorn, window)
}

// podRunsDesiredRunnerImage reports whether pod's runner container runs
// the image the row's running revision renders for it. The desired
// template renders the spec target, so only a row running that revision
// can be compared; any other row, and a pod whose runner image differs
// from the template's, is read as not the revision's own.
func podRunsDesiredRunnerImage(input workload.ReconcileInput, s *workload.InstanceStatus, pod *corev1.Pod) bool {
	if s.RunningRevision == "" || s.RunningRevision != input.ObservedState.UpdateRevision {
		return false
	}
	tmpl := input.DesiredSpec.PodSpec
	if pod.Labels[query.LabelRunner] == workload.RunnerWorker && input.DesiredSpec.WorkerPodSpec != nil {
		tmpl = input.DesiredSpec.WorkerPodSpec
	}
	if tmpl == nil {
		return false
	}
	want, got := "", ""
	for i := range tmpl.Containers {
		if tmpl.Containers[i].Name == constants.MainContainerName {
			want = tmpl.Containers[i].Image
		}
	}
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == constants.MainContainerName {
			got = pod.Spec.Containers[i].Image
		}
	}
	return want != "" && want == got
}

// crashWindow is the window after a row enters Ready inside which a
// crash of its promoted pod set is the attempt's failure: the
// Component's minReadySeconds, and no shorter than the stuck-pod grace,
// the age below which a crash is not yet read as a wedge. A pod set that
// held Ready for the window and breaks later is a steady-state break,
// repaired through the ordinary restart path and never counted. A zero
// window, neither knob configured, bounds nothing: no set ever holds it,
// so every crash of a promoted set is the attempt's failure.
func crashWindow(input workload.ReconcileInput) time.Duration {
	return time.Duration(ProvenWindowSeconds(input.DesiredSpec.MinReadySeconds, input.StuckPodGrace)) * time.Second
}

// firstRestartInsideWindow reports whether the runner's first restart
// after the row entered Ready came within the crash window, as far as
// the kubelet's record shows. The kubelet keeps only the latest
// termination, so the first restart is bounded from above: by that
// termination's end, or by the start of the run that died when that run
// itself began after Ready. The bound drifts as a set keeps crashing,
// which is why the row remembers the first sighting (crashRemembered).
// With no window configured every restart after Ready is inside it.
func firstRestartInsideWindow(input workload.ReconcileInput, s *workload.InstanceStatus, pod *corev1.Pod) bool {
	if s.ReadySince == nil || s.ReadySince.IsZero() {
		return false
	}
	window := crashWindow(input)
	if window <= 0 {
		return true
	}
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.Name != constants.MainContainerName {
			continue
		}
		var first time.Time
		if t := cs.LastTerminationState.Terminated; t != nil && t.FinishedAt.After(s.ReadySince.Time) {
			first = t.FinishedAt.Time
			if t.StartedAt.After(s.ReadySince.Time) {
				first = t.StartedAt.Time
			}
		} else if r := cs.State.Running; r != nil && r.StartedAt.After(s.ReadySince.Time) {
			first = r.StartedAt.Time
		} else {
			return false
		}
		return !first.After(s.ReadySince.Add(window))
	}
	return false
}

// crashRemembered reports whether the row records a crash of its current
// promoted pod set: a LastFailure dated after the row entered Ready. A
// rebuild enters Ready anew, so the record of the set it replaced reads
// as none.
func crashRemembered(s *workload.InstanceStatus) bool {
	return s.LastFailure != nil && s.ReadySince != nil && s.LastFailure.Time.After(s.ReadySince.Time)
}

// crashOfPromotedSet reports whether the crash the row remembers is its
// promoted set's: the record names a pod of that set, at the row's
// incarnation on the running revision. A surge replacement's failure is
// recorded on its source row as well and names the other generation's
// pod; the source's set cannot serve its way out of that record.
func crashOfPromotedSet(s *workload.InstanceStatus, pods []*corev1.Pod) bool {
	if !crashRemembered(s) {
		return false
	}
	running := query.RevisionFromName(s.RunningRevision)
	for _, pod := range pods {
		if pod == nil || pod.Name != s.LastFailure.PodName {
			continue
		}
		if inc, ok := query.InstanceIncarnationFromLabels(pod); ok && inc != s.Incarnation {
			return false
		}
		podRev := query.RevisionFromPod(pod)
		return podRev.IsZero() || running.IsZero() || podRev.Same(running)
	}
	return false
}

// crashCountedOnBlock reports whether the ladder already counts the
// remembered crash of this promoted set: the row's record is dated no
// earlier than the block's first failure, and no later than the block's
// last failure or its retry time. The record and the block's failure
// carry the one time of the pass that counted the crash (rememberCrash),
// never the kubelet's earlier date, and the block's last failure only
// moves forward, so a set counted once is never counted again, however
// the block's state moves under other attempts. A crash remembered
// while the block waited for its retry is the wave the block counts. A
// crash remembered after the block's retry time, under an attempt the
// record had admitted, with no repair answering it, is dated after every
// failure the block records, so it is still owed its count once a repair
// opens.
func crashCountedOnBlock(s *workload.InstanceStatus, block *workload.RetryBlock) bool {
	if !crashRemembered(s) || block == nil || block.FirstFailureAt == nil {
		return false
	}
	at := s.LastFailure.Time.Time
	if at.Before(block.FirstFailureAt.Time) {
		return false
	}
	if block.LastFailureAt != nil && !at.After(block.LastFailureAt.Time) {
		return true
	}
	return block.NextRetryAt != nil && !at.After(block.NextRetryAt.Time)
}

// rememberCrash records the crash of the row's promoted pod set on the
// row, dated at the pass that counts it with the time the block's failure
// carries, so later passes read the window and the count off the row
// rather than off a kubelet record that drifts or predates the count.
func rememberCrash(ctx context.Context, input workload.ReconcileInput, idx int32, termination *workload.InstanceTermination, at metav1.Time) error {
	dated := *termination
	dated.Time = at
	return status.RecordDatedFailure(ctx, input, idx, &dated)
}

// crashRebuildWarranted reports whether a trigger of the restart pass
// would rebuild the row's pod set for its crash: the pod set is wedged
// past the stuck-pod grace, or the policy rebuilds on a runner restart.
// A crash that warrants no rebuild is only remembered. The crash is the
// promoted set's on the revision the row runs, so its wedge is read
// there: a roll promotes that revision ahead of the Component's current
// one, and a pushed set rebuilds on its ladder as a current one does.
func crashRebuildWarranted(input workload.ReconcileInput, plan workload.ComponentPlan, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) bool {
	if pod, _, _ := evidence.CrashLoopWedgedPodOn(input, s, expected, pods, s.RunningRevision, ladderReadsUnready(plan)); pod != nil {
		return true
	}
	return plan.RestartPolicy == workload.RestartPolicyRecreateInstance && runnerRestartedSinceReady(pods, s.ReadySince)
}

// runnerRestartedSinceReady reports whether any pod's runner restarted
// after the row entered Ready (RunnerRestartedSinceReady).
func runnerRestartedSinceReady(pods []*corev1.Pod, readySince *metav1.Time) bool {
	for _, pod := range pods {
		if _, restarted := RunnerRestartedSinceReady(pod, readySince); restarted {
			return true
		}
	}
	return false
}

// crashLoopLadder is the running revision's retry ladder read against a
// crash of a promoted pod set. The crash is the failure of the attempt
// that promoted the set and the rebuild is the ladder's next attempt, so
// the one ladder every attempt at the revision is paced by bounds the
// rebuilds too: a loop exhausts it, where a crash that never comes back
// costs one attempt the Component's rows outlive.
type crashLoopLadder struct {
	rev   string
	crash runnerCrash
	// remember marks the first sighting of this promoted set's crash: the
	// row records it, dated, before anything else reads it.
	remember bool
	// record marks a crash the block does not count yet: this set's first
	// count, outside a wave the block already records, where a repair
	// answers it.
	record bool
	// held marks a ladder that holds once the crash is counted.
	held bool
	// parks marks a held ladder whose park lands this pass: the set is out
	// of serving. A held ladder parks nothing a serving set sits on.
	parks bool
	// open marks a ladder that admits the rebuild this pass; wait is how
	// long until it would, when the backoff is not due.
	open     bool
	wait     time.Duration
	attempts int32
}

// readCrashLoopRepair reads the wedge and the ladder for one row. A pod
// parked inside the grace is not a wedge yet, and the grace ending raises
// no watch event, so the pass deposits the grace left and comes back when
// the repair can open; a ladder whose backoff is not due deposits that
// wait the same way. A crash the ladder counts is the promoted set's on
// the revision the row runs, and its wedge is read there; a wedge no
// counted crash is behind is repaired on the Component's current revision
// alone, and an off-current one is the escalation's. A pod that runs but
// fails readiness is read as a wedge only with the ladder (ladderReadsUnready):
// on an idle row it is the runtime's own out-of-rotation signal, dark for
// the budgets and the roll and rebuilt by no policy.
func readCrashLoopRepair(input workload.ReconcileInput, plan workload.ComponentPlan, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) crashLoopRepair {
	ladder := readCrashLadder(input, s, expected, pods, crashRebuildWarranted(input, plan, s, expected, pods))
	var pod *corev1.Pod
	var reason string
	var graceLeft time.Duration
	if ladder != nil {
		pod, reason, graceLeft = evidence.CrashLoopWedgedPodOn(input, s, expected, pods, s.RunningRevision, ladderReadsUnready(plan))
	} else {
		pod, reason, graceLeft = evidence.CrashLoopWedgedPod(input, s, expected, pods, false)
	}
	if pod == nil {
		if graceLeft > 0 {
			input.PassWake.Observe(graceLeft)
		}
		return crashLoopRepair{}
	}
	r := crashLoopRepair{pod: pod, reason: reason}
	if ladder != nil {
		r.ladder = ladder
		r.opens = ladder.open
		r.fires = ladder.open || ladder.record || ladder.parks || ladder.remember
		return r
	}
	if rebuildRetryBlockDenies(input, s) {
		return r
	}
	r.fires, r.opens = true, true
	return r
}

// runnerRestartTrigger is the RecreateInstanceOnPodRestart trigger for a
// runner that restarted after Ready, paced by the running revision's
// ladder: a ladder whose backoff is not due, or whose attempt is in
// flight, leaves the row until it admits the rebuild, and a ladder that
// holds fires so the row parks Failed.
func runnerRestartTrigger(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	reason, restarted := "", false
	for _, pod := range pods {
		if reason, restarted = RunnerRestartedSinceReady(pod, s.ReadySince); restarted {
			break
		}
	}
	if !restarted {
		return "", false
	}
	if ladder := readCrashLadder(input, s, expected, pods, true); ladder != nil && !(ladder.open || ladder.record || ladder.parks || ladder.remember) {
		return "", false
	}
	return reason, true
}

// readCrashLadder is the running revision's ladder once the crash of the
// row's promoted pod set is counted against it, computed on a copy of
// the block with the same transition the writer applies. Nil when there
// is no such crash, the crash came after the pod set had held the crash
// window on a revision whose ladder does not hold (a steady-state break,
// rebuilt without the ladder counting it; with no window configured no
// set holds it and every crash counts), or the ladder records nothing
// for it: no ladder is configured, or the cause is the environment's. A
// set is counted once: a later sighting of the same set, with the block
// moved on under other attempts, records nothing. While the block's
// attempt is in progress, only the death of a set built once the block
// came due counts, since that set is the attempt's own; a set built
// before then, the policy's rebuild of a runner that died once, was never
// admitted by the block, so its death is remembered on the row and
// answered inside the attempt under way without moving the count.
// rebuilds says whether a trigger of the restart pass would rebuild the
// set for the crash (crashRebuildWarranted); the record counts the
// attempts the engine makes, so only such a crash is counted, and a crash
// no repair answers, a runner the kubelet restarted in place under
// restart policy None, is remembered on the row while the block stands
// as it is, whichever set died: the attempt's own set spends its count
// at the pass the repair opens for it. A ladder whose backoff is not due
// deposits that wait on the pass, which no watch event ends.
func readCrashLadder(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod, rebuilds bool) *crashLoopLadder {
	crash, ok := readRunnerCrash(input, s, expected, pods)
	if !ok {
		return nil
	}
	rev := s.RunningRevision
	now := input.Now()
	cur := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev)
	// A revision whose ladder holds admits no rebuild of its sets, inside
	// the window or after it: a restart on it is the hold's to park once
	// the set is out of serving.
	heldRevision := cur != nil && cur.State == workload.RetryBlockHeld
	remembered := crashRemembered(s)
	if !remembered && !heldRevision && !firstRestartInsideWindow(input, s, crash.pod) {
		return nil
	}
	block := workload.RetryBlock{TargetRevision: rev}
	if cur != nil {
		block = *cur
	}
	l := &crashLoopLadder{rev: rev, crash: crash, remember: !remembered}
	counts := false
	switch block.State {
	case workload.RetryBlockHeld, workload.RetryBlockBackoff:
		// Held is terminal, and a Backoff block already counts this wave
		// and paces the rebuild.
	case workload.RetryBlockRetryInProgress:
		counts = !crashCountedOnBlock(s, cur) && setBuiltOnceDue(s, cur, pods)
	default:
		counts = !crashCountedOnBlock(s, cur)
	}
	if counts && !rebuilds {
		// No repair answers the crash this pass, so the block stands as it
		// is and the row remembers the crash: a block that does not exist
		// yet is not written for it.
		if cur == nil {
			return l
		}
		counts = false
	}
	if counts {
		l.record = true
		workload.ApplyUpdateFailureToRetryBlock(&block, input.UpdateRetryPolicy, metav1.NewTime(now), crash.reason, workload.FailureCauseOf(crash.reason))
	}
	l.attempts = block.AttemptsStarted
	switch block.State {
	case workload.RetryBlockHeld:
		l.held = true
		l.parks = !query.PodSetFullyServing(pods, expected)
	case "":
		return nil
	default:
		denied, wait := evaluateRetryBlockGate(&block, now, anyInFlightAttemptAt(input, rev))
		l.open, l.wait = !denied, wait
		if !l.open && wait > 0 {
			input.PassWake.Observe(wait)
		}
	}
	return l
}

// setBuiltOnceDue reports whether the row's live pod set on its running
// revision was built once the block's retry time had passed, as a set
// the block's attempt rebuilds is: no pod of the set was created before
// nextRetryAt. A block with no retry time, or a set with no creation
// time, cannot be placed and reads as the attempt's own. The creation
// time is the API server's clock and nextRetryAt the controller's: a
// skew between them wider than the gap between the pass that found the
// block due and the create reads the attempt's own set as built before
// then, so that wave rebuilds one set more without counting it, and the
// set it builds counts.
func setBuiltOnceDue(s *workload.InstanceStatus, block *workload.RetryBlock, pods []*corev1.Pod) bool {
	if block == nil || block.NextRetryAt == nil {
		return true
	}
	running := query.RevisionFromName(s.RunningRevision)
	for _, pod := range query.ExcludeTerminalPods(pods) {
		if pod.DeletionTimestamp != nil || pod.CreationTimestamp.IsZero() {
			continue
		}
		if podRev := query.RevisionFromPod(pod); !podRev.IsZero() && !running.IsZero() && !podRev.Same(running) {
			continue
		}
		if pod.CreationTimestamp.Time.Before(block.NextRetryAt.Time) {
			return false
		}
	}
	return true
}

// anyInFlightAttemptAt reports whether any row carries an attempt at rev
// the ladder must let finish before it admits another: a Create or Update
// pinned to it, or a Restart rebuilding it.
func anyInFlightAttemptAt(input workload.ReconcileInput, rev string) bool {
	statuses := input.ObservedState.InstanceStatuses
	if anyInFlightCreateAttempt(input, allInstances) || anyInFlightUpdateAt(statuses, rev) {
		return true
	}
	for i := range statuses {
		s := &statuses[i]
		if s.Phase == workload.InstancePhaseRestarting && rebuildRevision(s) == rev {
			return true
		}
	}
	return false
}

// countCrashOnLadder reads the crash of a Ready row's promoted pod set
// against the running revision's retry ladder before a repair opens,
// whatever trigger selected the row. The crash is remembered on the row
// at its first sighting under every restart policy, and counted on the
// block at the first pass a trigger of the restart pass would rebuild
// the set for it (crashRebuildWarranted), the record and the row dated
// alike at that pass: the record counts the attempts the engine makes,
// so a runner the kubelet restarts in place under restart policy None
// spends no attempt until its pod waits in a terminal kubelet reason
// past the grace, while a crash the policy answers with a rebuild counts
// when it is first seen. The death of a set the block's open attempt did
// not build is remembered the same way and not counted: the attempt
// under way answers it. Where the repair would rebuild the set, a
// ladder that holds parks the row Failed naming the crash, with
// an InstanceFailed warning once per pod set, only while the set is out
// of serving, and a ladder whose backoff is not due leaves
// the row until it is. parked is true when no rebuild opens this pass.
// rev names the revision whose ladder attempt the rebuild is, "" when
// the ladder does not read the row.
func countCrashOnLadder(ctx context.Context, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, s *workload.InstanceStatus, pods []*corev1.Pod) (bool, string, error) {
	rebuilds := crashRebuildWarranted(input, plan, s, inst.TotalPods(), pods)
	l := readCrashLadder(input, s, inst.TotalPods(), pods, rebuilds)
	if l == nil {
		return false, "", nil
	}
	now := metav1.NewTime(input.Now())
	termination := runnerCrashTermination(l.crash, now)
	// The block is written before the row: a pass that ends between the
	// two re-enters here with the crash not yet remembered, and the block
	// records nothing twice while it counts the wave.
	if l.record {
		if err := workload.RecordUpdateFailureInRetryBlockAt(ctx, input, l.rev, l.crash.reason, workload.FailureCauseOf(l.crash.reason), now); err != nil {
			return false, "", fmt.Errorf("count crash on retry ladder (instance=%d, rev=%s): %w", inst.Index, l.rev, err)
		}
	}
	if l.remember || l.record {
		if err := rememberCrash(ctx, input, inst.Index, termination, now); err != nil {
			return false, "", fmt.Errorf("remember crash on the row (instance=%d): %w", inst.Index, err)
		}
	}
	if !rebuilds {
		return true, "", nil
	}
	if l.held {
		if !l.parks {
			if err := status.RecordCrashLoopNote(ctx, input, inst.Index, CrashLoopServingNote); err != nil {
				return false, "", fmt.Errorf("name the crash loop on the row (instance=%d): %w", inst.Index, err)
			}
			return true, "", nil
		}
		if err := status.StampFailed(ctx, input, inst.Index, termination); err != nil {
			return false, "", fmt.Errorf("stamp Failed under held retry ladder (instance=%d): %w", inst.Index, err)
		}
		// The park lands again on every crash of a set that serves between
		// them; the warning is owed once per pod set.
		announced, err := status.Announce(ctx, input, inst.Index, workload.EventReasonInstanceFailed)
		if err != nil {
			return false, "", fmt.Errorf("announce the park under held retry ladder (instance=%d): %w", inst.Index, err)
		}
		if announced && input.WarnInstanceFailed != nil {
			input.WarnInstanceFailed(inst.Index, l.crash.pod.Name,
				fmt.Sprintf("%s: crash after promotion; revision %s held after %d failed attempts", l.crash.reason, l.rev, l.attempts))
		}
		return true, "", nil
	}
	if !l.open {
		return true, "", nil
	}
	return false, l.rev, nil
}

// RestartOpensRepair reports whether a restart selection for inst would
// OPEN a fresh crash-loop repair this pass, as opposed to driving one
// already in flight or recovering capacity that is already gone: a
// Restart already in flight must be driven to completion, and the
// pod-loss and lost-member triggers repair an outage rather than causing
// one. Only a fresh open counts against the per-pass repair batch.
func RestartOpensRepair(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, instancePods []*corev1.Pod) bool {
	return readCrashLoopRepair(input, plan, input.ObservedState.Instance(inst.Index), inst.TotalPods(), instancePods).opens
}

// RestartOpensLadderAttempt names the revision whose retry ladder admits
// the rebuild a restart selection for inst opens this pass, "" when the
// open is not a ladder attempt. The ladder authorizes one attempt at a
// revision at a time, so the pass opens one such rebuild per revision
// and leaves the rest for the pass after.
func RestartOpensLadderAttempt(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, instancePods []*corev1.Pod) string {
	s := input.ObservedState.Instance(inst.Index)
	rebuilds := crashRebuildWarranted(input, plan, s, inst.TotalPods(), instancePods)
	if l := readCrashLadder(input, s, inst.TotalPods(), instancePods, rebuilds); l != nil && l.open && rebuilds {
		return l.rev
	}
	return ""
}

// LadderAttemptRebuilds reports whether the attempt of rev's retry
// ladder rebuilds this row's crashed set: the row runs rev, its crash
// counts on the ladder, a trigger of the restart pass would rebuild the
// set, and the ladder has not held — the rebuild waits on the backoff or
// on another attempt at rev, or the ladder admits it this pass — or the
// row's own rebuild is the attempt under way (a Restart rendering rev
// while rev's block is RetryInProgress). Every such rebuild is part of
// the ladder's attempt, so the hold that names the attempt names the row.
func LadderAttemptRebuilds(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, rev string, instancePods []*corev1.Pod) bool {
	s := input.ObservedState.Instance(inst.Index)
	if s == nil || rev == "" {
		return false
	}
	if s.Phase == workload.InstancePhaseRestarting {
		block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev)
		return rebuildRevision(s) == rev && block != nil && block.State == workload.RetryBlockRetryInProgress
	}
	if s.RunningRevision != rev {
		return false
	}
	rebuilds := crashRebuildWarranted(input, plan, s, inst.TotalPods(), instancePods)
	l := readCrashLadder(input, s, inst.TotalPods(), instancePods, rebuilds)
	return l != nil && !l.held && rebuilds
}

// RestartOpensUnavailability reports whether a restart selection for
// inst would OPEN a fresh crash-loop repair this pass that takes serving
// capacity offline. Only such an open is put to the per-Component
// unavailability budget: a wedged pod set that serves nothing
// (evidence.PodSetServesNothing, the reading the update pass makes for a
// dark row) removes no serving capacity, so its rebuild is neither
// admitted nor charged.
func RestartOpensUnavailability(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, instancePods []*corev1.Pod) bool {
	return RestartOpensRepair(input, plan, inst, instancePods) && !evidence.PodSetServesNothing(instancePods, input.Now(), input.StuckPodGrace)
}

// A crash-loop park follows its pod set: Failed while the set is out of
// serving, Ready naming the loop while it serves between crashes, with
// ReadySince and the failure record kept so the ladder and roll read on.

// CrashLoopServingNote is what a Ready row names on its failure message
// while the running revision's ladder holds and its set serves between
// crashes; it clears once the set holds Ready for the proven window.
const CrashLoopServingNote = "crash loop on a held revision: the pod set serves between crashes, and the Instance reads Failed while it is down"

// crashLoopStep is what the restart pass owes an operation-free row for
// its crash-loop episode this pass.
type crashLoopStep int

const (
	crashLoopNothing crashLoopStep = iota
	// crashLoopUnpark returns a park whose set serves to Ready.
	crashLoopUnpark
	// crashLoopName names the loop on a Ready row the hold parks nothing on.
	crashLoopName
	// crashLoopClear drops a note that is stale: the set proved itself or
	// the hold is gone.
	crashLoopClear
)

// crashLoopEpisode is the step owed to a row this pass, the reason the
// selection carries, and the note the unpark writes: the loop's naming
// when the ladder holds and the set has not proven itself, else none.
type crashLoopEpisode struct {
	step   crashLoopStep
	reason string
	note   string
}

// readCrashLoopEpisode reads the step owed to an operation-free row that
// remembers a crash of its promoted set: unpark a park whose set serves,
// name the loop on a held serving row, clear a note that has gone stale.
func readCrashLoopEpisode(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) crashLoopEpisode {
	if s == nil || s.Operation != nil || s.RunningRevision == "" || !crashOfPromotedSet(s, pods) {
		return crashLoopEpisode{}
	}
	held := revisionHeld(input, s.RunningRevision)
	switch s.Phase {
	case workload.InstancePhaseFailed:
		if !crashLoopParkServes(s, expected, pods) {
			return crashLoopEpisode{}
		}
		note := ""
		if held && !podSetProven(input, pods) {
			note = CrashLoopServingNote
		}
		return crashLoopEpisode{step: crashLoopUnpark, note: note,
			reason: fmt.Sprintf("crash-loop park on revision %s serves again", s.RunningRevision)}
	case workload.InstancePhaseReady:
		noted := status.FailureNoted(s, CrashLoopServingNote)
		switch {
		case !noted && held && query.PodSetFullyServing(pods, expected) && !podSetProven(input, pods):
			return crashLoopEpisode{step: crashLoopName,
				reason: fmt.Sprintf("crash loop on held revision %s serves between crashes", s.RunningRevision)}
		case noted && (!held || podSetProven(input, pods)):
			return crashLoopEpisode{step: crashLoopClear,
				reason: fmt.Sprintf("crash loop on revision %s is over", s.RunningRevision)}
		}
	}
	return crashLoopEpisode{}
}

// settleCrashLoopEpisode writes the owed step. Nothing opens and nothing
// is left in flight.
func settleCrashLoopEpisode(ctx context.Context, input workload.ReconcileInput, idx int32, episode crashLoopEpisode) error {
	var err error
	switch episode.step {
	case crashLoopUnpark:
		err = status.UnparkServing(ctx, input, idx, episode.note)
	case crashLoopName:
		err = status.RecordCrashLoopNote(ctx, input, idx, CrashLoopServingNote)
	case crashLoopClear:
		err = status.ClearCrashLoopNote(ctx, input, idx, CrashLoopServingNote)
	}
	if err != nil {
		return fmt.Errorf("settle crash-loop episode (instance=%d): %w", idx, err)
	}
	return nil
}

// crashLoopParkServes reports whether a park's set is back: complete,
// every pod at the row's incarnation on the running revision, and fully
// serving; a set of another incarnation or revision is another pass's.
func crashLoopParkServes(s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) bool {
	if expected <= 0 {
		return false
	}
	set := repairPodSet(pods)
	if int32(len(set)) < expected {
		return false
	}
	running := query.RevisionFromName(s.RunningRevision)
	for _, pod := range set {
		if inc, ok := query.InstanceIncarnationFromLabels(pod); !ok || inc != s.Incarnation {
			return false
		}
		if podRev := query.RevisionFromPod(pod); !podRev.IsZero() && !running.IsZero() && !podRev.Same(running) {
			return false
		}
	}
	return query.PodSetFullyServing(set, expected)
}

// revisionHeld reports whether rev's retry ladder holds.
func revisionHeld(input workload.ReconcileInput, rev string) bool {
	block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, rev)
	return block != nil && block.State == workload.RetryBlockHeld
}

// podSetProven reports whether every live pod of the set has held Ready
// for the proven window (ProvenWindowSeconds), the roll's bar for a set
// that restarted since Ready; an empty set proves nothing, and with no
// window configured no set does, so a held revision's serving row keeps
// naming the loop until the hold is released.
func podSetProven(input workload.ReconcileInput, pods []*corev1.Pod) bool {
	window := ProvenWindowSeconds(input.DesiredSpec.MinReadySeconds, input.StuckPodGrace)
	if window <= 0 {
		return false
	}
	set := repairPodSet(pods)
	for _, pod := range set {
		if available, _ := podreadiness.IsPodAvailable(pod, window, input.Now()); !available {
			return false
		}
	}
	return len(set) > 0
}

// A spent repair: a Restart parked at Failed by the deadline or a wedged
// pod. No pass drives the row, but the repair it records keeps its exits
// here. A rebuilt set that comes up on its own — the kubelet's retry
// succeeded once a missing image tag or ConfigMap key was back — resumes
// the promote at the incarnation the pods already carry, so the Instance
// returns to rotation on its running revision with no operator action. A
// set that stays wedged is rebuilt again on the operator's retry ladder
// (lifecycle.updateRetry, the one ladder every attempt is paced by),
// measured from the recorded failure and bounded by its attempt count, so
// a cause that needs a fresh pod set to clear — a crash loop, a gang
// whose members hung, a node-local fault — heals once it is gone. A park
// whose recorded cause is workload-caused (workload.RepairWaitsOnWorkload:
// the container config or image the pod template names) is owed no
// re-arm at all: the kubelet retries that cause in place and a fresh set
// would wedge on it identically, so the row stays Failed with its reason
// until the configuration or image is fixed and takes the first exit
// when the pods come up. No ladder configured means no re-arm either:
// the row waits for the reset mailbox or a new revision. A set that is
// entirely gone or terminal leaves nothing to resume or rebuild under the
// spent operation, whatever the cause or the ladder: the operation closes
// (spentRepairPodSetGone) and the row is the fresh start the Create pass
// rebuilds, as every total loss is.

// SpentRepair reports whether the row is a Restart parked at Failed.
func SpentRepair(s *workload.InstanceStatus) bool {
	return s != nil && s.Phase == workload.InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == workload.InstanceOperationRestart
}

// repairPodSet is the pods a spent repair's exits are judged on: live,
// not deleting, not terminal. A pod still terminating holds its name and
// belongs to no exit until it is gone.
func repairPodSet(pods []*corev1.Pod) []*corev1.Pod {
	set := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range query.ExcludeTerminalPods(pods) {
		if pod.DeletionTimestamp != nil {
			continue
		}
		set = append(set, pod)
	}
	return set
}

// spentRepairPodSetGone reports whether a spent repair has no live pod
// left to judge: every pod of its set is gone or in a terminal phase. A
// pod still terminating is live until it is gone, so the name it holds
// is not rebuilt under it. A repair whose rebuild create the apiserver
// refused has no pod by construction and is not a total loss: its own
// schedule retries the create (repairRebuildRefused).
func spentRepairPodSetGone(s *workload.InstanceStatus, pods []*corev1.Pod) bool {
	return SpentRepair(s) && !repairRebuildRefused(s) && query.LivePodCount(pods) == 0
}

// repairRebuildRefused reports whether the row is a spent repair whose
// rebuild create the apiserver refused as an invalid pod spec, at the
// revision the Instance already ran. The refusal is recorded as the
// failure with no pod named, since the create made none. Such a repair
// keeps its attempt and retries the create on its own schedule: it is
// not closed for having no pod, and a member it does hold is not read as
// a loss to rebuild at once.
func repairRebuildRefused(s *workload.InstanceStatus) bool {
	return SpentRepair(s) && s.LastFailure != nil && s.LastFailure.Reason == workload.RejectionReasonInvalidPodSpec
}

// refusedRepairTrigger decides the exits of a repair whose rebuild create
// the apiserver refused: the create is issued again when the repair's
// schedule comes due, measured from the refusal, whatever its pod set
// holds (repairRetryDue).
func refusedRepairTrigger(input workload.ReconcileInput, s *workload.InstanceStatus) (string, bool) {
	return repairRetryDue(input, s)
}

// repairCloseReason names the close of a spent repair: the failure that
// parked it and the loss that ends it.
func repairCloseReason(s *workload.InstanceStatus) string {
	return fmt.Sprintf("repair parked after %s closed: no live pod left to resume or rebuild", RepairFailureSummary(s))
}

// RepairResumes reports whether a spent repair's rebuilt set is one
// promote away: complete, every pod at the row's incarnation, and every
// pod ContainersReady. A set carrying an older incarnation is a drain the
// attempt never finished, not a set that came up, and is rebuilt instead.
func RepairResumes(s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) bool {
	if !SpentRepair(s) || expected <= 0 {
		return false
	}
	set := repairPodSet(pods)
	if int32(len(set)) < expected {
		return false
	}
	for _, pod := range set {
		if inc, ok := query.InstanceIncarnationFromLabels(pod); !ok || inc != s.Incarnation {
			return false
		}
	}
	return query.AllPodsRuntimeReady(set)
}

// RepairRetryAt returns the instant a spent repair may re-arm — the
// recorded failure plus the ladder's delay for the next attempt — and
// false when no re-arm is owed: no spent repair on the row, a park on a
// cause the kubelet retries in place (workload.RepairWaitsOnWorkload),
// no ladder configured, or every re-arm spent.
func RepairRetryAt(input workload.ReconcileInput, s *workload.InstanceStatus) (time.Time, bool) {
	policy := input.UpdateRetryPolicy
	if !SpentRepair(s) || workload.RepairWaitsOnWorkload(s) || policy == nil || policy.Exhausted(s.Operation.RetryCount) {
		return time.Time{}, false
	}
	return repairParkedAt(s).Add(policy.NextRetryDelay(s.Operation.RetryCount + 1)), true
}

// repairParkedAt is when the repair parked: the recorded failure, which
// the Failed stamps write in the same transaction as the phase, else the
// operation's own timestamps for a row that recorded no failure.
func repairParkedAt(s *workload.InstanceStatus) time.Time {
	if s.LastFailure != nil && !s.LastFailure.Time.IsZero() {
		return s.LastFailure.Time.Time
	}
	if !s.Operation.LastProgressAt.IsZero() {
		return s.Operation.LastProgressAt.Time
	}
	return s.Operation.StartedAt.Time
}

// RepairRetriesExhausted reports whether a spent repair has spent every
// re-arm the operator's ladder allows, so the row stays parked until an
// operator resets it or a new revision arrives. A park on a cause the
// kubelet retries in place is not held by the ladder — it waits for the
// configuration or image and resumes on its own — so it is never read as
// exhausted, whatever its attempt count.
func RepairRetriesExhausted(input workload.ReconcileInput, s *workload.InstanceStatus) bool {
	return SpentRepair(s) && !workload.RepairWaitsOnWorkload(s) && input.UpdateRetryPolicy != nil &&
		input.UpdateRetryPolicy.Exhausted(s.Operation.RetryCount)
}

// RepairWaitingNote is what a park on a workload-caused failure records
// on its failure message and says in its event: the cause is the
// configuration or image the pod template names, which the kubelet
// retries in place, so the row waits for the fix and no re-arm is owed.
const RepairWaitingNote = "waiting for the configuration or image the pod needs; the kubelet retries in place"

// spentRepairTrigger decides the exits of a spent repair whose pod set is
// complete. An incomplete set is not decided here: a gang short of a
// member re-arms through the lost-member trigger, and a row with no live
// pod left closes its repair first (spentRepairPodSetGone) and is then
// the Create pass's fresh start.
//
// A resume stamps no operation, so it carries no reason. A re-arm that is
// owed but not yet due deposits its wake-up on the pass: the ladder
// coming due raises no watch event.
func spentRepairTrigger(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) (string, bool) {
	if expected <= 0 || int32(len(repairPodSet(pods))) < expected {
		return "", false
	}
	if RepairResumes(s, expected, pods) {
		return "", true
	}
	return repairRetryDue(input, s)
}

// repairRetryDue decides whether a spent repair's next attempt starts on
// this pass: not while a Held block on its revision denies every rebuild
// of it, not while nothing automatic retries it, and not before its
// schedule comes due, in which case the remaining wait is deposited on the
// pass, since the schedule coming due raises no watch event. Due, it
// reports the reason the new attempt opens with.
func repairRetryDue(input workload.ReconcileInput, s *workload.InstanceStatus) (string, bool) {
	if rebuildRetryBlockDenies(input, s) {
		return "", false
	}
	at, owed := RepairRetryAt(input, s)
	if !owed {
		return "", false
	}
	if now := input.Now(); now.Before(at) {
		input.PassWake.Observe(at.Sub(now))
		return "", false
	}
	return repairRearmReason(s), true
}

// spentRepairParked reports whether a spent repair with a complete pod set
// has nothing to do this pass: the set is wedged and the ladder has not
// come due, is spent, owes nothing to a park the kubelet retries in
// place, or is denied by the revision's RetryBlock.
func spentRepairParked(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) bool {
	if !SpentRepair(s) || expected <= 0 || int32(len(repairPodSet(pods))) < expected {
		return false
	}
	if RepairResumes(s, expected, pods) {
		return false
	}
	if rebuildRetryBlockDenies(input, s) {
		return true
	}
	at, owed := RepairRetryAt(input, s)
	return !owed || input.Now().Before(at)
}

// repairRearmReason names the re-arm on the new attempt: which attempt it
// is and the failure that parked the last one.
func repairRearmReason(s *workload.InstanceStatus) string {
	return fmt.Sprintf("repair re-armed (attempt %d) after %s", s.Operation.RetryCount+1, RepairFailureSummary(s))
}

// RepairFailureSummary names what parked the repair, for events and the
// re-armed attempt's reason: the recorded failure when there is one — by
// pod when it names one, by its own message otherwise, as an elapsed
// deadline does — else the reason the repair opened with.
func RepairFailureSummary(s *workload.InstanceStatus) string {
	switch {
	case s == nil:
		return ""
	case s.LastFailure == nil:
		if s.Operation != nil {
			return s.Operation.Reason
		}
		return ""
	case s.LastFailure.PodName != "":
		return s.LastFailure.ShortString()
	case s.LastFailure.Message != "":
		return s.LastFailure.Message
	default:
		return s.LastFailure.Reason
	}
}
