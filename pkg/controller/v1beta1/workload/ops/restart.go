package ops

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// RestartRequeueInterval is the wait between passes while a Restart
// is in flight. Exported so the dispatcher's pacing stays in
// lockstep.
const RestartRequeueInterval = 5 * time.Second

// DetectRestartTrigger fires when the Instance is mid-restart, when
// Phase=Ready and a pod is Failed / the live pod count is below
// desired, or when a materialized Instance has lost a gang member in
// any phase (see instanceLostGangMember). A Migrate-owned Instance is
// suppressed because Migrate's source-pod deletion would otherwise trip
// the "pod count below desired" trigger on the source.
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
//
// Self-lists this Instance's pods live. The dispatcher's per-reconcile
// restart pass instead calls DetectRestartTriggerWithPods after a single
// live List + bucket, so a Component with N gangs costs one live List per
// reconcile instead of N — see that variant.
func DetectRestartTrigger(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) (bool, string, error) {
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
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
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
	if s == nil {
		return false, ""
	}
	if isMigrateOwnedStatus(s) {
		return false, ""
	}
	if s.Phase == workload.InstancePhaseRestarting {
		return true, ""
	}
	expected := inst.TotalPods()
	if s.Phase != workload.InstancePhaseReady {
		// Below Ready only gang-member loss triggers. Pod-level failure
		// evidence stays Ready-gated: a container that dies while the
		// Instance is still forming is the ordinary boot path.
		if reason, lost := instanceLostGangMember(input, plan, s, expected, instancePods); lost {
			return true, reason
		}
		return false, ""
	}

	for _, pod := range instancePods {
		if reason, restarted := runnerRestartedSinceReady(pod, s.ReadySince); restarted {
			return true, reason
		}
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
	default:
		return "", false
	}
	if isMigrateOwnedStatus(s) {
		return "", false
	}
	// A preserved Update or Migrate operation keeps its own pass as owner
	// whatever the phase says. A Restart operation parked at Failed is a
	// spent attempt, not an owner: its deadline elapsed with a member still
	// missing, and only another Restart can rebuild the gang as a whole
	// (incarnation bump, survivors drained), so it may re-arm. Each cycle
	// is bounded by the attempt deadline.
	if s.Operation != nil && s.Operation.Type != workload.InstanceOperationCreate &&
		!(s.Operation.Type == workload.InstanceOperationRestart && s.Phase == workload.InstancePhaseFailed) {
		return "", false
	}
	createCommitted := s.Operation != nil && s.Operation.Step == createStepCreatePods
	if !createCommitted && s.Phase != workload.InstancePhaseFailed && s.PodCount < expected {
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
	denied, _ := evaluateRetryBlockGate(b, input.Now(), anyInFlightCreateAttempt(input.ObservedState.InstanceStatuses))
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
// Source-agnostic: callers construct workload.ReconcileInput from their
// own source-of-truth types — today the InferenceReplica adapter
// (inferencereplica.buildReconcileInput) wires this entry point.
func Restart(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, reason string) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Restart: nil client")
	}

	// Use the post-patch Incarnation, not inst.Incarnation — BuildPlan
	// ran against an earlier read.
	wasNotRestarting := true
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil && s.Phase == workload.InstancePhaseRestarting {
		wasNotRestarting = false
	}
	newInc, err := patchInstanceStatusRestarting(ctx, input, inst.Index, reason, plan.InstanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("patch status Restarting (instance=%d): %w", inst.Index, err)
	}
	if wasNotRestarting {
		recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonRestartTriggered,
			"OMENative %s restart triggered: %s (incarnation=%d)",
			instanceKey(input.Key.Component, inst.Index), reason, newInc)
	}

	pods, err := query.LiveListPodsForInstance(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Restart: list pods (instance=%d): %w", inst.Index, err)
	}

	// On the first pass, preserve the failing pod's diagnostics into
	// InstanceStatus.LastFailure BEFORE Phase A drains and deletes it —
	// otherwise the container termination reason / exit code vanish with
	// the pod and a gang that keeps restarting leaves no failure trace.
	// Only a genuine pod-failure restart populates this; a "pod count
	// below desired" restart (the pod already vanished) finds nothing to
	// capture and leaves any prior LastFailure intact.
	if wasNotRestarting {
		if t := firstFailedPodTermination(pods); t != nil {
			if err := patchInstanceLastFailure(ctx, input, inst.Index, t); err != nil {
				return false, fmt.Errorf("record LastFailure (instance=%d): %w", inst.Index, err)
			}
		}
	}

	oldPods, newPods, unknownPods := query.PartitionPodsByIncarnation(pods, newInc)

	// Surface orphan pods (label-missing) as warnings and refuse to delete.
	// A stripped label or a third-party operator stamping the OMENative
	// managed-by label would otherwise get torn down by Phase A. Operator
	// must re-classify or delete the pod for Restart to proceed.
	if len(unknownPods) > 0 {
		for _, pod := range unknownPods {
			recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonFoundOrphan,
				"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to delete",
				instanceKey(input.Key.Component, inst.Index), pod.Namespace, pod.Name)
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
	recycling, err := recycleTerminalPods(ctx, deps, input, inst.Index, workload.InstanceOperationRestart, terminalTargetPods(newPods, desired))
	if err != nil {
		return false, fmt.Errorf("Restart: recycle terminal pods (instance=%d): %w", inst.Index, err)
	}
	if recycling {
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
		if _, err := createMissingPods(ctx, deps, input, plan, newInst, inst.Index, missing, revisionForInstance(input, inst.Index)); err != nil {
			if createRejectionHandled(err) {
				return false, nil
			}
			return false, err
		}
		return false, nil
	}

	// Phase C: flip serving, promote Ready.
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

	if err := patchInstanceStatusReady(ctx, input, inst.Index); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRestartCompleted,
		"OMENative %s restart complete (incarnation=%d)",
		instanceKey(input.Key.Component, inst.Index), newInc)
	return true, nil
}

// patchInstanceStatusRestarting idempotently stamps Phase=Restarting +
// Restart/Drain with the given reason and increments Incarnation by one.
// Returns the post-write Incarnation; if a previous pass already moved
// into Restart, the existing Incarnation is preserved.
func patchInstanceStatusRestarting(ctx context.Context, input workload.ReconcileInput, idx int32, reason string, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	err := input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseRestarting &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationRestart {
			observedIncarnation = s.Incarnation
			return false
		}
		// An Instance that never ran a revision has only the revision its
		// interrupted attempt pinned; the rebuilt pods must carry it so the
		// per-revision Service selects them.
		pinned := ""
		if s.RunningRevision == "" && s.TargetRevision == "" && s.Operation != nil {
			pinned = s.Operation.TargetRevision
		}
		// Bump first so old pods can be distinguished from the new set.
		if s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Incarnation++
		observedIncarnation = s.Incarnation
		s.Phase = workload.InstancePhaseRestarting
		now := metav1.NewTime(input.Now())
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("restart-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationRestart,
			Step:           "Drain",
			Reason:         reason,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
			TargetRevision: pinned,
		}
		return true
	})
	return observedIncarnation, err
}

// firstFailedPodTermination returns the InstanceTermination of the first
// pod that is Phase=Failed or carries any container-termination diagnostics
// worth preserving across the recreate. Returns nil when no pod yields a
// signal (e.g. the failing pod already vanished — the "pod count below
// desired" trigger). Order-deterministic by the input slice.
func firstFailedPodTermination(pods []*corev1.Pod) *workload.InstanceTermination {
	now := metav1.Now()
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

// patchInstanceLastFailure stamps InstanceStatus.LastFailure with the
// captured termination. Idempotent: a no-op when an identical record is
// already stored (same pod + reason + exit code), so a repeated first-pass
// (status conflict retry) doesn't churn the field.
func patchInstanceLastFailure(ctx context.Context, input workload.ReconcileInput, idx int32, t *workload.InstanceTermination) error {
	if t == nil {
		return nil
	}
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if sameTermination(s.LastFailure, t) {
			return false
		}
		captured := *t
		s.LastFailure = &captured
		return true
	})
}

// sameTermination reports whether two termination records carry the same
// operator-relevant identity (pod, container, reason, exit code). Time and
// Message are excluded so a re-capture that differs only in the recorded
// timestamp doesn't trigger a redundant status write.
func sameTermination(a, b *workload.InstanceTermination) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.PodName != b.PodName || a.ContainerName != b.ContainerName || a.Reason != b.Reason {
		return false
	}
	switch {
	case a.ExitCode == nil && b.ExitCode == nil:
		return true
	case a.ExitCode == nil || b.ExitCode == nil:
		return false
	default:
		return *a.ExitCode == *b.ExitCode
	}
}

// revisionForInstance returns the revision to stamp on pods
// being recreated for one Instance. Restart keeps the Instance on its
// existing revision, so we read RunningRevision from the observed
// status. An Instance that never ran a revision (a gang rebuilt after
// losing a member while still forming) has only the revision its attempt
// pinned on the Operation. Returns the zero RevisionID when nothing
// records a revision.
func revisionForInstance(input workload.ReconcileInput, idx int32) query.RevisionID {
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, idx)
	if s == nil {
		return query.RevisionID{}
	}
	if s.RunningRevision != "" {
		return query.RevisionFromName(s.RunningRevision)
	}
	if s.TargetRevision != "" {
		return query.RevisionFromName(s.TargetRevision)
	}
	if s.Operation != nil && s.Operation.TargetRevision != "" {
		return query.RevisionFromName(s.Operation.TargetRevision)
	}
	return query.RevisionID{}
}

// runnerRestartedSinceReady reports whether pod's runner container carries
// restart evidence dated after readySince: a current run that began after the
// Instance entered Ready, or a termination that finished after it. Either
// proves the process group formed at Ready has lost this member's original
// process, even though kubelet's in-place restart left the Pod Running under
// the same UID. Sidecar and init containers never count — kubelet's in-place
// restart is their recovery path and does not break the Instance's process
// group. A nil readySince (Instance was last promoted before the field
// existed) reports false; the anchor appears on the next Ready transition.
func runnerRestartedSinceReady(pod *corev1.Pod, readySince *metav1.Time) (string, bool) {
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
