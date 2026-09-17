package ops

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// inPlaceUpdate runs the in-place rollout: drain (optional), patch
// container images, reconcile pod-template annotations from the
// target revision's PodMeta, wait for runtime ready on the target
// image, flip serving back, promote Ready. Multi-pass; idempotent.
//
// Annotation handling adds / updates keys present in the new spec and
// deletes keys the previous revision once authored; foreign
// (third-party) annotations are left alone. Recreate-on-annotation-
// change was rejected because annotations are not load-bearing for
// runtime image semantics and would defeat the zero-downtime
// contract.
func inPlaceUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec, pods []*corev1.Pod) (bool, error) {
	// In-place keeps the same Incarnation — only the container image rolls.
	wasNotUpdating := true
	if s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index); s != nil && s.Phase == workload.InstancePhaseUpdating {
		wasNotUpdating = false
	}
	if err := patchInstanceStatusUpdating(ctx, input, inst.Index, target.Name, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("patch status Updating (instance=%d): %w", inst.Index, err)
	}
	if wasNotUpdating {
		recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonInPlaceUpdateStarted,
			"OMENative %s in-place update to revision %s",
			instanceKey(input.Key.Component, inst.Index), target.Name)
	}

	markNotReady := true
	if plan.UpdateStrategy.InPlaceUpdateStrategy != nil &&
		plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle != nil {
		markNotReady = *plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle
	}

	// A pod already relabeled to the target revision has been through the
	// drain + patch steps and is back in (or returning to) rotation; draining
	// it again would remove converged capacity and restart its PodReady age
	// on every pass while promotion waits out the minReadySeconds window.
	targetRev := query.RevisionOf(target)
	onTarget := func(pod *corev1.Pod) bool { return query.RevisionFromPod(pod).Same(targetRev) }

	// Drain first when the strategy requires it.
	if markNotReady {
		for _, pod := range pods {
			if !podreadiness.IsServing(pod) || onTarget(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
				return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
		}
		// Live reader on drain check so kube-proxy isn't still routing.
		for _, pod := range pods {
			if onTarget(pod) {
				continue
			}
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
	}

	// Pre-load the target + running revision metadata once, before the
	// per-pod patch loop. The CR's PodMeta is the source of truth for the
	// annotation set the new revision authored; the running revision's
	// PodMeta tells us which keys the previous spec owned (so we can
	// delete the ones the user just removed without clobbering keys set
	// by other controllers / pod-mutating webhooks). Both can be nil on
	// edge cases (CR with no PodMeta yet, first in-place update with no
	// recorded running CR) — annotationsDiff handles nil-as-empty.
	targetPayload, err := loadControllerRevisionPayload(target)
	if err != nil {
		return false, fmt.Errorf("load target CR data (instance=%d): %w", inst.Index, err)
	}
	runningPayload, err := loadRunningRevisionPayload(ctx, deps.Reader(), input, inst.Index)
	if err != nil {
		return false, fmt.Errorf("load running CR data (instance=%d): %w", inst.Index, err)
	}
	var targetAnnotations, previousAnnotations map[string]string
	var runningSpec *corev1.PodSpec
	if targetPayload != nil && targetPayload.PodMeta != nil {
		targetAnnotations = targetPayload.PodMeta.Annotations
	}
	if runningPayload != nil {
		runningSpec = runningPayload.PodSpec
		if runningPayload.PodMeta != nil {
			previousAnnotations = runningPayload.PodMeta.Annotations
		}
	}

	// Pod mutation and convergence both start from a live read. Kubelet status
	// writes advance resourceVersion independently of the cached observation;
	// using that observation for an optimistic image patch can conflict until
	// the cache catches up. The same live object closes the window where a pod
	// becomes serving again after the drain check. The metadata patches
	// piggyback on this loop. Any issued mutation ends the pass so convergence
	// and status promotion use another live pod observation.
	livePods := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		fresh := &corev1.Pod{}
		if gerr := deps.Reader().Get(ctx, client.ObjectKeyFromObject(pod), fresh); gerr != nil {
			if apierrors.IsNotFound(gerr) {
				return false, nil
			}
			return false, fmt.Errorf("re-read pod before in-place update (instance=%d, pod=%s): %w", inst.Index, pod.Name, gerr)
		}
		livePods = append(livePods, fresh)
	}
	// A pod mutation the apiserver PERMANENTLY rejects is a statement
	// about the revision, not a transient hiccup: the object this
	// revision asks for is not admissible, so no number of retries
	// produces a different answer. Dispose exactly as a rejected create
	// does — the corrected revision is then admitted immediately.
	rejected := func(pod *corev1.Pod, err error) (bool, error) {
		rejection := workload.ClassifyAPIError(err)
		if !rejection.Class.Permanent() {
			return false, nil
		}
		if derr := disposeRejectedAttempt(ctx, deps, input, inst.Index, target.Name, pod.Name, rejection, true); derr != nil {
			return true, fmt.Errorf("dispose rejected in-place patch (instance=%d, pod=%s): %w", inst.Index, pod.Name, derr)
		}
		return true, nil
	}

	mutated := false
	for _, pod := range livePods {
		imagePatches := imagePatchTargets(pod, targetSpec)
		needsImagePatch := len(imagePatches) > 0
		if markNotReady && needsImagePatch && podreadiness.IsServing(pod) {
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
				return false, fmt.Errorf("re-mark not serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
			}
			return false, nil
		}
		markerReady, merr := ensureInPlaceImageTransition(ctx, deps.Client, pod, targetSpec, imagePatches)
		if merr != nil {
			if disposed, derr := rejected(pod, merr); disposed {
				return false, derr
			}
			return false, fmt.Errorf("record image transition (instance=%d, pod=%s): %w", inst.Index, pod.Name, merr)
		}
		if !markerReady {
			return false, nil
		}
		if needsImagePatch {
			issued, perr := patchPodImages(ctx, deps.Client, pod, targetSpec)
			if perr != nil {
				if disposed, derr := rejected(pod, perr); disposed {
					return false, derr
				}
				return false, fmt.Errorf("patch images (instance=%d, pod=%s): %w", inst.Index, pod.Name, perr)
			}
			if issued {
				mutated = true
			}
		}
		annotationsPatched, err := patchPodAnnotations(ctx, deps.Client, pod, previousAnnotations, targetAnnotations)
		if err != nil {
			if disposed, derr := rejected(pod, err); disposed {
				return false, derr
			}
			return false, fmt.Errorf("patch annotations (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
		mutated = mutated || annotationsPatched
		// Restamp the revision-owned labels (revision hash + pairing
		// protocol) so the in-place-rolled pod is recognized as the target
		// revision by per-revision Service routing, drain, and stuck-pod
		// detection, and reports the target revision's pairing cohort.
		// Idempotent.
		targetProtocol := ""
		if targetPayload != nil && targetPayload.PairingProtocol != nil {
			targetProtocol = *targetPayload.PairingProtocol
		}
		revisionLabelsPatched, err := patchPodRevisionLabels(ctx, deps.Client, pod, query.RevisionOf(target).Hash(), targetProtocol)
		if err != nil {
			if disposed, derr := rejected(pod, err); disposed {
				return false, derr
			}
			return false, fmt.Errorf("patch revision labels (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
		mutated = mutated || revisionLabelsPatched
	}
	if mutated {
		return false, nil
	}

	// ContainersReady can remain true in a stale observation after an image
	// patch. Runtime image confirmation is therefore required for every image
	// that differs between the immutable running and target revisions. An empty
	// changed-image set is already satisfied; runtime aliases for unchanged
	// containers are not evidence about this rollout.
	if !query.AllPodsRuntimeReady(livePods) {
		return false, nil
	}
	for _, pod := range livePods {
		if !podImagesMatch(pod, targetSpec) {
			return false, nil
		}
		if !podRuntimeImageChangesMatch(pod, runningSpec, targetSpec) {
			return false, nil
		}
		transition, present, valid := inPlaceImageTransitionFromPod(pod)
		valid = valid && inPlaceImageTransitionMatchesTarget(transition, targetSpec)
		if present && !valid {
			return false, nil
		}
		if valid && !inPlaceImageTransitionRuntimeMatches(pod, transition) {
			return false, nil
		}
	}
	for _, pod := range livePods {
		_, present, _ := inPlaceImageTransitionFromPod(pod)
		if !present {
			continue
		}
		if err := removeInPlaceImageTransition(ctx, deps.Client, pod); err != nil {
			return false, fmt.Errorf("clear image transition (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
		return false, nil
	}

	for _, pod := range livePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	// Promotion releases this Instance's unavailability budget slot, so under
	// a minReadySeconds window it waits until every patched pod has stayed
	// Ready for that long. A zero window keeps the runtime-image gate above.
	if plan.MinReadySeconds > 0 && !podsAvailable(livePods, plan.MinReadySeconds, input.Now()) {
		return false, nil
	}
	if err := patchInstanceStatusReadyOnRevision(ctx, input, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonInPlaceUpdateCompleted,
		"OMENative %s in-place update to revision %s complete",
		instanceKey(input.Key.Component, inst.Index), target.Name)
	return true, nil
}
