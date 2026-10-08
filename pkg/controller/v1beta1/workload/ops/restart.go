package ops

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
	if s == nil {
		return false, ""
	}
	if isMigrateOwnedStatus(s) {
		return false, ""
	}
	// An open Restart is driven to completion whatever the policy says:
	// the policy governs whether a repair STARTS, and a half-drained
	// Instance abandoned mid-repair is worse than one never repaired.
	if s.Phase == workload.InstancePhaseRestarting {
		return true, ""
	}
	expected := inst.TotalPods()
	// A repair parked at Failed has its own exits, whatever the policy
	// says: the policy governs whether a repair STARTS, and this one did.
	// With no live pod left there is nothing to resume or rebuild under
	// the spent operation, so it closes and the row is the Create pass's
	// fresh start, as every total loss is.
	if SpentRepair(s) {
		if spentRepairPodSetGone(s, instancePods) {
			return true, repairCloseReason(s)
		}
		if reason, fires := spentRepairTrigger(input, s, expected, instancePods); fires {
			return true, reason
		}
	}
	// A crash-loop park follows its pod set, and a Ready row the hold parks
	// nothing on names the loop it serves through, whatever the policy says
	// (readCrashLoopEpisode).
	if episode := readCrashLoopEpisode(input, s, expected, instancePods); episode.step != crashLoopNothing {
		return true, episode.reason
	}
	// A pod wedged in a terminal kubelet waiting reason cannot recover on
	// its own and no other pass owns an operation-free Ready row, so the
	// repair starts regardless of the restart policy; a pod that runs but
	// fails readiness is no wedge of an idle row under any policy
	// (readCrashLoopRepair).
	if reason, wedged := crashLoopRepairReason(input, plan, s, expected, instancePods); wedged {
		return true, reason
	}
	// A crash of the promoted pod set inside its window is remembered on
	// the row whatever the policy says, so a later sighting whose kubelet
	// record no longer dates the first restart still reads as this set's
	// crash. The ladder counts it only where the wedge or the policy's
	// trigger would rebuild the set.
	if l := readCrashLadder(input, s, expected, instancePods); l != nil && l.remember {
		return true, l.crash.restartReason()
	}
	if plan.RestartPolicy != workload.RestartPolicyRecreateInstance {
		return false, ""
	}
	if s.Phase != workload.InstancePhaseReady {
		// Below Ready, gang-member loss triggers. Pod-level failure
		// evidence stays Ready-gated: a container that dies while the
		// Instance is still forming is the ordinary boot path.
		if reason, lost := instanceLostGangMember(input, plan, s, expected, instancePods); lost {
			return true, reason
		}
		// A row demoted for losing every pod (types.DemotedReady) is read
		// by its pod count below, as Ready is: the running revision it
		// kept is what the repair rebuilds at, and pods that turn out to
		// be present return it to Ready through the promote path. Below
		// Ready the rebuild answers to that revision's RetryBlock, as
		// every re-materialization does. Every other row below Ready is
		// its own pass's to materialize.
		if !workload.DemotedReady(s) || rebuildRetryBlockDenies(input, s) {
			return false, ""
		}
	} else {
		if reason, restarted := runnerRestartTrigger(input, s, expected, instancePods); restarted {
			return true, reason
		}
		for _, pod := range instancePods {
			if pod.Status.Phase == corev1.PodFailed {
				// Build a richer reason from the failed pod's container
				// termination so the RestartTriggered event names the actual
				// cause (OOMKilled exit 137, CrashLoopBackOff, ...) instead of
				// the bare "pod X Failed". Falls back to the pod name when no
				// per-container detail is available.
				if t := workload.PodTermination(pod, metav1.NewTime(input.Now())); t != nil {
					return true, t.ShortString()
				}
				return true, fmt.Sprintf("pod %s Failed", pod.Name)
			}
		}
	}
	// Terminal pods are absent for the count: a Succeeded pod, or a Failed
	// one that carried no termination detail, still leaves the Instance
	// short of its desired set.
	if live := len(query.ExcludeTerminalPods(instancePods)); int32(live) < expected {
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
	live := len(query.ExcludeTerminalPods(pods))
	if live == 0 || int32(live) >= expected {
		return "", false
	}
	if rebuildRetryBlockDenies(input, s) {
		return "", false
	}
	return fmt.Sprintf("gang member lost: %d of %d pods present", live, expected), true
}

// rebuildRetryBlockDenies reports whether a RetryBlock forbids rebuilding
// this Instance right now. Restart has no RetryBlock gate of its own
// because a Ready Instance is by definition running a revision that
// works; repairing a never-Ready one can re-materialize a revision the
// disposition held, so it has to answer to the same authority Create does.
func rebuildRetryBlockDenies(input workload.ReconcileInput, s *workload.InstanceStatus) bool {
	rev := s.RunningRevision
	if rev == "" {
		rev = s.TargetRevision
	}
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

// Restart drives one Instance through Restart-on-pod-loss. Multi-pass,
// idempotent. Three phases:
//
//	A. Drain + delete every pod at the old incarnation.
//	B. Recreate the pod set at the bumped incarnation
//	   (distinguished by the ome.io/instance-incarnation label).
//	C. Wait for runtime ready, flip ome.io/serving=True, promote to Ready.
//
// reason lands on Operation.Reason on the first pass only; later passes
// preserve it via the skip-write guard.
//
// Returns done=true once the Instance is back at Phase=Ready with
// Operation=nil; done=false means the caller should requeue and resume.
//
// target is the Component's roll target, nil when it has none. A rebuild
// that has created no pod yet follows it when the row is off it
// (rebuildFollowsTarget); every other rebuild renders the revision its
// row records.
//
// Source-agnostic: callers construct workload.ReconcileInput from their
// own source-of-truth types — today the InferenceReplica adapter
// (inferencereplica.buildReconcileInput) wires this entry point.
func Restart(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, reason string) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Restart: nil client")
	}

	pods, err := query.LiveListPodsForInstance(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Restart: list pods (instance=%d): %w", inst.Index, err)
	}

	// Use the post-patch Incarnation, not inst.Incarnation — BuildPlan
	// ran against an earlier read.
	observed := input.ObservedState.Instance(inst.Index)
	// A crash-loop park follows its pod set and a Ready row the hold parks
	// nothing on names the loop; neither opens a repair or leaves one in
	// flight.
	if episode := readCrashLoopEpisode(input, observed, inst.TotalPods(), pods); episode.step != crashLoopNothing {
		if err := settleCrashLoopEpisode(ctx, input, inst.Index, episode); err != nil {
			return false, err
		}
		return true, nil
	}
	wasNotRestarting := observed == nil || observed.Phase != workload.InstancePhaseRestarting
	var newInc int64
	switch {
	case spentRepairPodSetGone(observed, pods):
		// Nothing is left to resume or rebuild under the spent operation:
		// it closes, and the row is the fresh start the Create pass owns,
		// which recycles any dead occupant and rebuilds under the row's
		// incarnation. The pass after reads the closed row.
		closed, err := status.CloseSpentRepair(ctx, input, inst.Index, observed.Operation.ID)
		if err != nil {
			return false, fmt.Errorf("Restart: close spent repair (instance=%d): %w", inst.Index, err)
		}
		if closed {
			workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonRepairClosed,
				"OMENative %s %s; the Create pass rebuilds the Instance as a fresh start",
				workload.InstanceKey(input.Key.Component, inst.Index), repairCloseReason(observed))
		}
		return false, nil
	case RepairResumes(observed, inst.TotalPods(), pods):
		// The rebuilt set of a spent repair came up on its own. No new
		// attempt opens: the pods already carry the row's incarnation, and
		// the repair ends with the promote below.
		newInc = observed.Incarnation
	case spentRepairParked(input, observed, inst.TotalPods(), pods):
		// Wedged and not yet due on the retry ladder: the trigger declined
		// this row, and a pass that reaches it anyway leaves it parked.
		return false, nil
	default:
		// A crash of a promoted pod set is the failure of the attempt that
		// promoted it, whatever trigger reads it: the running revision's
		// retry ladder counts it, paces the rebuild, and parks the row
		// Failed once it holds.
		ladderRev := ""
		if wasNotRestarting && observed != nil && observed.Phase == workload.InstancePhaseReady {
			parked, rev, err := countCrashOnLadder(ctx, input, plan, inst, observed, pods)
			if err != nil || parked {
				return false, err
			}
			ladderRev = rev
		}
		// The failing pod's diagnostics travel in the stamp's write: the
		// drain below deletes the pod, and its termination reason and exit
		// code with it, so a gang that keeps restarting would otherwise
		// leave no failure trace. A restart for a pod count below desired
		// finds nothing to capture and leaves any prior record intact.
		var failure *workload.InstanceTermination
		if wasNotRestarting {
			failure = firstFailedPodTermination(pods, metav1.NewTime(input.Now()))
		}
		newInc, err = status.StampRestarting(ctx, input, inst.Index, reason, plan.InstanceReadyTimeout, failure)
		if err != nil {
			return false, fmt.Errorf("patch status Restarting (instance=%d): %w", inst.Index, err)
		}
		if ladderRev != "" {
			if err := status.RetryBlockAttemptStarted(ctx, input, ladderRev); err != nil {
				return false, fmt.Errorf("Restart: start retry ladder attempt (instance=%d): %w", inst.Index, err)
			}
		}
		if wasNotRestarting {
			workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonRestartTriggered,
				"OMENative %s restart triggered: %s (incarnation=%d)",
				workload.InstanceKey(input.Key.Component, inst.Index), reason, newInc)
		}
	}

	oldPods, newPods, unknownPods := query.PartitionPodsByIncarnation(pods, newInc)

	// Surface orphan pods (label-missing) as warnings and refuse to delete.
	// A stripped label or a third-party operator stamping the OMENative
	// managed-by label would otherwise get torn down by Phase A. Operator
	// must re-classify or delete the pod for Restart to proceed.
	if len(unknownPods) > 0 {
		for _, pod := range unknownPods {
			workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonFoundOrphan,
				"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to delete",
				workload.InstanceKey(input.Key.Component, inst.Index), pod.Namespace, pod.Name)
		}
		return false, nil
	}

	// Phase A: drain + delete old pods.
	if len(oldPods) > 0 {
		for _, pod := range oldPods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterRestartDrain, restartDrainKey(inst.Index, newInc)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}

		// Live reader on drain check so kube-proxy isn't still routing.
		// Drain target is the per-revision *routed* Service. Read from
		// the pod's ome.io/revision-hash label (stamped by Render). The
		// per-Component headless Service publishes not-ready endpoints
		// so peer-discovery DNS resolves during gang init — using it
		// here would make IsPodDrained wait forever.
		//
		// Batcher lists each per-revision Service's EndpointSlices once
		// and reuses them across the gang, avoiding the N+1 per-pod LIST.
		drainer := drain.NewBatcher(deps.Reader(), input.Key.Namespace)
		for _, pod := range oldPods {
			hash := pod.Labels[query.LabelRevisionHash]
			if hash == "" {
				continue
			}
			serviceName := query.PerRevisionServiceName(input.Key.OwnerName, plan.Component, hash)
			drained, err := drainer.IsPodDrained(ctx, serviceName, pod)
			if err != nil {
				return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			if !drained {
				return false, nil
			}
		}

		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		// EXPECT-ORDER: per-pod ExpectDeletes BEFORE Delete, rollback via
		// ObservedDelete on error — a failed RPC fires no event to decrement.
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
				return false, fmt.Errorf("delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
		return false, nil
	}

	// Phase B: recreate at the bumped Incarnation.
	// Live-read the OLD pod set first — a stable-name Create with the
	// cache stale would AlreadyExists against the still-terminating
	// previous incarnation. Matches K8s foreground-deletion propagation.
	clear, err := query.LiveOldPodsClearedForRecreate(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index, newInc)
	if err != nil {
		return false, fmt.Errorf("Restart: live-check Phase A done (instance=%d): %w", inst.Index, err)
	}
	if !clear {
		return false, nil
	}

	// Override Incarnation on the ad-hoc plan so Render stamps the new label.
	newInst := inst
	newInst.Incarnation = newInc

	desired := expectedPodNamesForInstance(input, plan, newInst)
	// A new-incarnation pod that died (rejected at admission, evicted) is
	// absent: it holds the stable name but will never run. Recycle it so
	// the name frees up; its target is created on a later pass.
	recycling, err := recycleTerminalPods(ctx, deps, input, inst.Index, inst.Index, workload.InstanceOperationRestart, terminalTargetPods(newPods, desired))
	if err != nil {
		return false, fmt.Errorf("Restart: recycle terminal pods (instance=%d): %w", inst.Index, err)
	}
	if recycling {
		return false, nil
	}
	// A pod whose node stopped reporting it is not dead evidence: the
	// name stays occupied until the force-delete sweep proves the node
	// gone, and the repair reports that wait instead of polling a name no
	// kubelet is answering for. The policy boundary that wait ends on
	// needs no plumbing here: an unfinished Restart requeues at
	// RestartRequeueInterval, well inside any node-death threshold.
	holding, _, err := recoverUnknownPhaseTargets(ctx, deps, input, inst.Index, newPods, desired)
	if err != nil {
		return false, fmt.Errorf("Restart: recover unknown-phase pods (instance=%d): %w", inst.Index, err)
	}
	if holding {
		return false, nil
	}
	existingByName := query.IndexPodsByName(query.ExcludeTerminalPods(newPods))
	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}
	if len(missing) > 0 {
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		// The rebuild renders the revision it stamps. A rebuild that has
		// created no pod yet follows a roll target the row is off: the row
		// records that revision before the first create, so a pass
		// interrupted between the two still renders what the row says, and
		// the update trigger reads the Instance as on target once it is
		// Ready. Every other rebuild renders the stored template of the
		// revision its row records: a repair under a pause or a Held
		// target, and one whose pods already exist, which the roll replaces
		// from Ready.
		var tmpl podTemplate
		found := true
		if rebuildFollowsTarget(input, plan, inst.Index, target, newPods) {
			if err := status.StampRestartRevision(ctx, input, inst.Index, target.Name); err != nil {
				return false, fmt.Errorf("Restart: record rebuild revision (instance=%d): %w", inst.Index, err)
			}
			tmpl = desiredTemplate(input, plan, query.RevisionOf(target))
		} else {
			tmpl, found, err = rebuildTemplate(ctx, deps, input, plan, target, inst.Index)
			if err != nil {
				return false, fmt.Errorf("Restart: resolve rebuild template (instance=%d): %w", inst.Index, err)
			}
		}
		if !found {
			return false, announceRevisionGone(ctx, deps, input, inst.Index, rebuildRevision(input.ObservedState.Instance(inst.Index)))
		}
		if _, err := createMissingPods(ctx, deps, input, plan, newInst, inst.Index, missing, tmpl); err != nil {
			if createRejectionHandled(err) {
				return false, nil
			}
			return false, err
		}
		return false, nil
	}

	// Phase C: flip serving, then hold at the shared promote bar. The gate
	// write comes first because the rebuilt pods carry the lifecycle hold, so
	// PodReady cannot become true until it is released.
	if !query.AllPodsRuntimeReady(newPods) {
		return false, nil
	}
	for _, pod := range newPods {
		if podreadiness.IsServing(pod) {
			continue
		}
		// Lifecycle key on fresh pods; the Restart-drain key only lived
		// on the now-deleted OLD pods.
		if err := podreadiness.MarkPodServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}
	if promotable, wait := query.PodSetPromotable(newPods, plan.MinReadySeconds, input.Now()); !promotable {
		input.PromoteWindow.Observe(wait)
		return false, nil
	}

	if err := status.StampReady(ctx, input, inst.Index); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonRestartCompleted,
		"OMENative %s restart complete (incarnation=%d)",
		workload.InstanceKey(input.Key.Component, inst.Index), newInc)
	return true, nil
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

// rebuildTemplate is the template a repair of Instance idx renders:
// the stored template of the revision it rebuilds at (rebuildRevision),
// stamped with that revision. target is the roll target the stored
// template is composed against. found is false when that revision's
// ControllerRevision is gone. A row that records no revision is rebuilt
// from the current template with no revision label, as it was created.
func rebuildTemplate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, idx int32) (podTemplate, bool, error) {
	rev := rebuildRevision(input.ObservedState.Instance(idx))
	if rev == "" {
		return desiredTemplate(input, plan, query.RevisionID{}), true, nil
	}
	return storedTemplate(ctx, deps.Reader(), input, target, rev)
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
// rebuild follows the target itself while it has created no pod
// (rebuildFollowsTarget); a Migrate-owned row, which its record owns; a
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
// The roll the row is left to must be one that can start (rollTargetOpen):
// an Instance kept dark for the length of a pause or a hold is worse than
// one rebuilt at the revision the roll is held on. The roll takes the row
// once the pause or the hold clears.
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
	if workload.DemotedReady(s) && len(query.ExcludeTerminalPods(instancePods)) > 0 {
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

// rebuildFollowsTarget reports whether the rebuild of an open repair
// renders the roll target instead of the revision its row records: no pod
// of the rebuild exists yet, the row is off a target the roll could start
// on, and the canary step does not hold the Instance. The Instance serves
// nothing, so the rebuild is the pass that puts it on the target and no pod
// set is torn down for it; a held Instance is the step's stable side, which
// the step machine alone promotes. A rebuild whose pods exist finishes on
// the revision they carry: replacing a starting pod set is the roll's job,
// under its budget.
func rebuildFollowsTarget(input workload.ReconcileInput, plan workload.ComponentPlan, idx int32, target *appsv1.ControllerRevision, rebuiltPods []*corev1.Pod) bool {
	if len(query.ExcludeTerminalPods(rebuiltPods)) > 0 || !rollTargetOpen(input, plan, target) {
		return false
	}
	rev := rebuildRevision(input.ObservedState.Instance(idx))
	return rev != "" && rev != target.Name && !canaryStepHeldIndices(input, plan, target)[idx]
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
func RunnerRestartedSinceReady(pod *corev1.Pod, readySince *metav1.Time) (string, bool) {
	if pod == nil || readySince == nil || readySince.IsZero() || pod.DeletionTimestamp != nil {
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
// nil and zero readySince rules of RunnerRestartedSinceReady apply.
func RunnerRestartedAgainSinceReady(pod *corev1.Pod, readySince *metav1.Time) bool {
	if pod == nil || readySince == nil || readySince.IsZero() || pod.DeletionTimestamp != nil {
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
// Failed; or the set was created after the ladder recorded a failure,
// so it is an attempt the ladder admitted, or one built while the
// revision was already failing, and its crash is the revision's next
// failure however the row's own record reads.
func revisionFailing(input workload.ReconcileInput, s *workload.InstanceStatus, pod *corev1.Pod) bool {
	block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, s.RunningRevision)
	if block == nil {
		return false
	}
	if block.State == workload.RetryBlockHeld {
		return true
	}
	if block.LastFailureAt == nil || pod.CreationTimestamp.IsZero() {
		return false
	}
	return !pod.CreationTimestamp.Time.Before(block.LastFailureAt.Time)
}

// rebuiltAfterFailure reports whether pod belongs to a set the row built
// right after a failure of the set before it, as the roll reads it
// (RebuiltAfterFailure): the row's failure record predates this set's
// Ready and this pod was created within the crash window of that
// failure, and the failure is this revision's own, not one of a set the
// row ran on a superseded revision.
func rebuiltAfterFailure(input workload.ReconcileInput, s *workload.InstanceStatus, pod *corev1.Pod) bool {
	if s.LastFailure == nil || s.ReadySince == nil || s.LastFailure.Time.After(s.ReadySince.Time) {
		return false
	}
	return RebuiltAfterFailure(pod, s.LastFailure, input.RevisionBorn, ProvenWindowSeconds(input.DesiredSpec.MinReadySeconds, input.StuckPodGrace))
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
// repaired through the ordinary restart path and never counted.
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
func firstRestartInsideWindow(input workload.ReconcileInput, s *workload.InstanceStatus, pod *corev1.Pod) bool {
	if s.ReadySince == nil || s.ReadySince.IsZero() {
		return false
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
		return !first.After(s.ReadySince.Add(crashWindow(input)))
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
// earlier than the block's first failure. The record and the block's
// failure carry the one time of the pass that counted the crash
// (rememberCrash), never the kubelet's earlier date, so a set counted
// once is never counted again, however the block's state moves under
// other attempts.
func crashCountedOnBlock(s *workload.InstanceStatus, block *workload.RetryBlock) bool {
	return crashRemembered(s) && block != nil && block.FirstFailureAt != nil &&
		!s.LastFailure.Time.Time.Before(block.FirstFailureAt.Time)
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
// A crash that warrants no rebuild is only counted. The crash is the
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
	// count, outside a wave the block already records.
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
	ladder := readCrashLadder(input, s, expected, pods)
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
	if ladder := readCrashLadder(input, s, expected, pods); ladder != nil && !(ladder.open || ladder.record || ladder.parks || ladder.remember) {
		return "", false
	}
	return reason, true
}

// readCrashLadder is the running revision's ladder once the crash of the
// row's promoted pod set is counted against it, computed on a copy of
// the block with the same transition the writer applies. Nil when there
// is no such crash, the crash came after the pod set had held the crash
// window on a revision whose ladder does not hold (a steady-state break,
// rebuilt without the ladder counting it), or the ladder records nothing for it: no ladder
// is configured, or the cause is the environment's. A set is counted once: a later sighting of the same
// set, with the block moved on under other attempts, records nothing. A
// ladder whose backoff is not due deposits that wait on the pass, which
// no watch event ends.
func readCrashLadder(input workload.ReconcileInput, s *workload.InstanceStatus, expected int32, pods []*corev1.Pod) *crashLoopLadder {
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
	switch block.State {
	case workload.RetryBlockHeld, workload.RetryBlockBackoff:
		// Held is terminal, and a Backoff block already counts this wave
		// and paces the rebuild.
	default:
		if !crashCountedOnBlock(s, cur) {
			l.record = true
			workload.ApplyUpdateFailureToRetryBlock(&block, input.UpdateRetryPolicy, metav1.NewTime(now), crash.reason, workload.FailureCauseOf(crash.reason))
		}
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
// whatever trigger selected the row. A crash the block does not count
// yet is counted at its first sighting and remembered on the row, dated
// alike, whether or not a rebuild opens: the loop is the revision's
// failure, and the ladder's backoff is what the roll waits on and
// announces. Where the wedge or the policy's trigger would rebuild the
// set, a ladder that holds parks the row Failed naming the crash, with
// an InstanceFailed warning once per pod set, only while the set is out
// of serving, and a ladder whose backoff is not due leaves
// the row until it is. parked is true when no rebuild opens this pass.
// rev names the revision whose ladder attempt the rebuild is, "" when
// the ladder does not read the row.
func countCrashOnLadder(ctx context.Context, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, s *workload.InstanceStatus, pods []*corev1.Pod) (bool, string, error) {
	l := readCrashLadder(input, s, inst.TotalPods(), pods)
	if l == nil {
		return false, "", nil
	}
	now := metav1.NewTime(input.Now())
	termination := workload.PodTerminationWithReason(l.crash.pod, l.crash.reason, now)
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
	if !crashRebuildWarranted(input, plan, s, inst.TotalPods(), pods) {
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
	if l := readCrashLadder(input, s, inst.TotalPods(), instancePods); l != nil && l.open && crashRebuildWarranted(input, plan, s, inst.TotalPods(), instancePods) {
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
	l := readCrashLadder(input, s, inst.TotalPods(), instancePods)
	return l != nil && !l.held && crashRebuildWarranted(input, plan, s, inst.TotalPods(), instancePods)
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
// that restarted since Ready; an empty set proves nothing.
func podSetProven(input workload.ReconcileInput, pods []*corev1.Pod) bool {
	window := ProvenWindowSeconds(input.DesiredSpec.MinReadySeconds, input.StuckPodGrace)
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
// is not rebuilt under it.
func spentRepairPodSetGone(s *workload.InstanceStatus, pods []*corev1.Pod) bool {
	return SpentRepair(s) && len(query.ExcludeTerminalPods(pods)) == 0
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
