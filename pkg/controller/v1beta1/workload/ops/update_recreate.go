package ops

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// recreateRevisionCause formats the "revision <from> → <to>" cause string
// for a recreate-driven rollout. "from" is the Instance's recorded
// RunningRevision; when it's empty (first-ever rollout, or a backfill gap)
// the string degrades to "revision -> <to>" so the target is always named.
func recreateRevisionCause(input workload.ReconcileInput, idx int32, targetRev string) string {
	from := ""
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, idx); s != nil {
		from = s.RunningRevision
	}
	return fmt.Sprintf("revision %s -> %s", from, targetRev)
}

// recreateUpdate runs the recreate rollout: drain + delete every
// current-incarnation pod, recreate at the bumped Incarnation. Shares
// the drain/delete/recreate skeleton with Restart but pivots on
// template-change rather than pod-loss. Multi-pass; idempotent.
func recreateUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, pods []*corev1.Pod) (bool, error) {
	// "First pass of recreate" = no in-flight Update or Step != Drain;
	// the recreate stamp sets Step=Drain.
	wasNotRecreating := true
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil &&
		s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
		s.Operation.Step == updateStepDrain {
		wasNotRecreating = false
	}
	// Cause string for the recreate: "revision <from> → <to>". Recorded on
	// Operation.Reason so a revision-roll recreate is distinguishable in
	// status from a pod-failure Restart (Type=Update vs Type=Restart, and
	// a revision-pair reason vs a termination reason). "from" is the
	// recorded RunningRevision; empty on the first-ever rollout.
	cause := recreateRevisionCause(input, inst.Index, target.Name)
	newInc, err := patchInstanceStatusRecreatingForUpdate(ctx, input, inst.Index, target.Name, cause, plan.InstanceReadyTimeout)
	if err != nil {
		return false, fmt.Errorf("bump Incarnation (instance=%d): %w", inst.Index, err)
	}
	if wasNotRecreating {
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateStarted,
			"OMENative %s recreate (%s, incarnation=%d)",
			instanceKey(input.Key.Component, inst.Index), cause, newInc)
	}

	oldPods, newPods, unknownPods := query.PartitionPodsByIncarnation(pods, newInc)

	// Refuse to proceed on orphan pods (label-missing). Without this guard
	// a stripped label or third-party reuse of managed-by would get torn
	// down by Phase A. Same rationale as Restart.
	if len(unknownPods) > 0 {
		for _, pod := range unknownPods {
			recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonFoundOrphan,
				"OMENative %s found orphan pod %s/%s without ome.io/instance-incarnation; refusing to recreate-update",
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
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterUpdateRecreateDrain, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		// Live reader on drain check so kube-proxy isn't still routing.
		// Batcher lists each per-revision Service's EndpointSlices once
		// and reuses them across the gang, avoiding the N+1 per-pod LIST.
		drainer := drain.NewBatcher(deps.Reader(), input.Key.Namespace)
		for _, pod := range oldPods {
			serviceName := drainServiceForPod(input, plan, pod)
			if serviceName == "" {
				continue
			}
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
	// Live-read the OLD set first — a stable-name Create with the cache
	// stale would AlreadyExists against a still-terminating previous
	// incarnation. Matches K8s foreground-deletion propagation.
	clear, err := query.LiveOldPodsClearedForRecreate(ctx, deps.Reader(), input.Key.Namespace, input.Key.OwnerName, plan.Component, inst.Index, newInc)
	if err != nil {
		return false, fmt.Errorf("recreateUpdate: live-check Phase A done (instance=%d): %w", inst.Index, err)
	}
	if !clear {
		return false, nil
	}

	newInst := inst
	newInst.Incarnation = newInc

	desired := expectedPodNamesForInstance(input, plan, newInst)
	existingByName := query.IndexPodsByName(newPods)
	missing := make([]podTarget, 0, len(desired))
	for _, t := range desired {
		if _, ok := existingByName[t.Name]; !ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		if _, err := createMissingPods(ctx, deps, input, plan, newInst, inst.Index, missing, query.RevisionOf(target)); err != nil {
			if createRejectionHandled(err) {
				return false, nil
			}
			return false, err
		}
		return false, nil
	}

	// Phase C: flip serving, then wait for kubelet to fold the readiness gate
	// into PodReady before promoting. ContainersReady only proves the runtime
	// probe passed; a pod is not eligible for its Service until PodReady is
	// true, and promotion releases the slot that holds the next Instance's drain.
	if !query.AllPodsRuntimeReady(newPods) {
		return false, nil
	}
	for _, pod := range newPods {
		if podreadiness.IsServing(pod) {
			continue
		}
		// Lifecycle key on fresh pods; the Update-recreate-drain key only
		// lived on the now-deleted old pods.
		if err := podreadiness.MarkPodServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}
	for _, pod := range newPods {
		if !podreadiness.IsPodReady(pod) {
			return false, nil
		}
	}
	// Under a minReadySeconds window promotion additionally waits until every
	// new pod has stayed Ready for that long.
	if plan.MinReadySeconds > 0 && !podsAvailable(newPods, plan.MinReadySeconds, input.Now()) {
		return false, nil
	}
	if err := patchInstanceStatusReadyOnRevision(ctx, input, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonRecreateUpdateCompleted,
		"OMENative %s recreate to revision %s complete",
		instanceKey(input.Key.Component, inst.Index), target.Name)
	return true, nil
}
