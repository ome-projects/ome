package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
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
	// An in-place step patches pods it does not create, so an empty pod
	// set has no repair anywhere in the pipeline: the update pass owns the
	// index, so the Create pass is skipped and fresh-index creation
	// excludes an Updating row, and the restart trigger declines below
	// Ready. Re-resolve the roll to recreate instead, which does render a
	// replacement at the target revision.
	if len(pods) == 0 {
		// An empty set is evidence only once this Instance's own creates
		// and deletes have been observed. Before that it can be a read
		// that predates them, and re-resolving on one would bump the
		// Incarnation out from under a pod that does exist.
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, inst.Index) {
			return false, nil
		}
		reresolved, err := status.StampRecreateFromInPlace(ctx, input, inst.Index, target.Name)
		if err != nil {
			return false, fmt.Errorf("re-resolve in-place roll to recreate (instance=%d): %w", inst.Index, err)
		}
		if reresolved {
			workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonInPlaceUpdateNotPossible,
				"OMENative %s lost the pod its in-place update to revision %s was patching; recreating instead",
				workload.InstanceKey(input.Key.Component, inst.Index), target.Name)
		}
		return recreateUpdate(ctx, deps, input, plan, inst, target, pods)
	}

	// In-place keeps the same Incarnation — only the container image rolls.
	// A row the update pass owns is mid-update; one in Updating only
	// because a parked attempt's pods serve is not, and the patch over it
	// is a first pass.
	wasNotUpdating := workload.Owner(input.ObservedState.Instance(inst.Index)) != workload.OwnerUpdate
	if err := status.StampUpdatingInPlace(ctx, input, inst.Index, target.Name, plan.UpdateStrategy.Type, plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("patch status Updating (instance=%d): %w", inst.Index, err)
	}
	if wasNotUpdating {
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonInPlaceUpdateStarted,
			"OMENative %s in-place update to revision %s",
			workload.InstanceKey(input.Key.Component, inst.Index), target.Name)
	}

	// A pod in a terminal phase can never report the new image: free it
	// now rather than patch and poll it to the deadline; the empty set then
	// re-resolves the roll to recreate, above.
	if recycling, err := recycleTerminalTargets(ctx, deps, input, inst.Index, inst.Index,
		workload.InstanceOperationUpdate, pods, expectedPodNamesForInstance(input, plan, inst)); err != nil {
		return false, fmt.Errorf("recycle terminal pod (instance=%d): %w", inst.Index, err)
	} else if recycling {
		return false, nil
	}

	markNotReady := true
	if plan.UpdateStrategy.InPlaceUpdateStrategy != nil &&
		plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle != nil {
		markNotReady = *plan.UpdateStrategy.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle
	}

	// A pod mutation the apiserver could not take to admission is a wait,
	// not a verdict: the row records it in the apiserver's words and the
	// pass ends quietly for the ordinary retry, exactly as a refused
	// create does.
	admissionWait := func(pod *corev1.Pod, err error) (bool, error) {
		rejection := evidence.ClassifyAPIError(err)
		if rejection.Class != workload.APIRejectionAdmissionUnavailable {
			return false, nil
		}
		if rerr := recordAdmissionWait(ctx, deps, input, plan, inst.Index, pod.Name, rejection); rerr != nil {
			return true, fmt.Errorf("record admission wait (instance=%d, pod=%s): %w", inst.Index, pod.Name, rerr)
		}
		return true, nil
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
				if handled, herr := admissionWait(pod, err); handled {
					return false, herr
				}
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
	// recorded running CR) — evidence.AnnotationsDiff handles nil-as-empty.
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
		if handled, herr := admissionWait(pod, err); handled {
			return true, herr
		}
		rejection := evidence.ClassifyAPIError(err)
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
				if handled, herr := admissionWait(pod, err); handled {
					return false, herr
				}
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
	// Every patch went through admission: an admission wait a refused
	// pass left on the row is over.
	if err := releaseAdmissionWait(ctx, input, inst.Index); err != nil {
		return false, fmt.Errorf("release admission wait (instance=%d): %w", inst.Index, err)
	}
	if mutated {
		return false, nil
	}

	// ContainersReady can remain true in a stale observation after an image
	// patch. Every container the roll re-images must therefore be confirmed
	// at runtime before the pod returns to rotation: the kubelet reports the
	// target image for it, or has replaced it since the patch. An empty
	// changed-image set is already satisfied; runtime aliases for unchanged
	// containers are not evidence about this rollout.
	if !query.AllPodsRuntimeReady(livePods) {
		return false, nil
	}
	transitions := make(map[string]*inPlaceImageTransition, len(livePods))
	for _, pod := range livePods {
		if !evidence.PodImagesMatch(pod, targetSpec) {
			return false, nil
		}
		transition, present, valid := inPlaceImageTransitionFromPod(pod)
		valid = valid && inPlaceImageTransitionMatchesTarget(transition, targetSpec)
		if present && !valid {
			return false, nil
		}
		if !inPlaceImageRollConfirmed(pod, runningSpec, targetSpec, transition) {
			return false, nil
		}
		transitions[pod.Name] = transition
	}

	for _, pod := range livePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterUpdateInPlace, updateDrainKey(inst.Index, inst.Incarnation)); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}

	// The shared promote bar with the in-place extra: on top of PodReady past
	// the availability window, every container whose image the roll changed
	// must be confirmed at runtime. Promotion releases this Instance's
	// unavailability budget slot, so a pod the serving-gate flip has not yet
	// carried back into rotation holds it.
	promotable, wait := query.PodSetPromotable(livePods, plan.MinReadySeconds, input.Now(),
		func(pod *corev1.Pod) bool {
			return inPlaceImageRollConfirmed(pod, runningSpec, targetSpec, transitions[pod.Name])
		})
	if !promotable {
		input.PromoteWindow.Observe(wait)
		return false, nil
	}
	if err := status.StampReadyOnRevision(ctx, input, inst.Index, target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", inst.Index, err)
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonInPlaceUpdateCompleted,
		"OMENative %s in-place update to revision %s complete",
		workload.InstanceKey(input.Key.Component, inst.Index), target.Name)
	// The transition outlives the stamp: it carries the restart counts the
	// confirmation compares against, and a pass that finds it gone before
	// the row is Ready cannot confirm a container the kubelet reports
	// under another name. A marker left by a failed clear is re-recorded
	// by the next roll's patch.
	for _, pod := range livePods {
		if err := removeInPlaceImageTransition(ctx, deps.Client, pod); err != nil {
			return false, fmt.Errorf("clear image transition (instance=%d, pod=%s): %w", inst.Index, pod.Name, err)
		}
	}
	return true, nil
}

var inPlaceImageTransitionAnnotation = constants.InferenceServiceInPlaceImageTransitionAnnotationKey

// inPlaceImageTransition is the write-ahead record of an image patch: the
// images the patch moves each container to, and the restart count each
// container had when its patch was issued, which is how the roll tells a
// container the kubelet has since replaced from one it has not.
type inPlaceImageTransition struct {
	TargetImages  map[string]string `json:"targetImages"`
	RestartCounts map[string]int32  `json:"restartCounts,omitempty"`
}

// replacedSincePatch reports whether the kubelet has restarted the named
// container since its image patch was issued: its restart count rose past
// the count the transition recorded then. Unknown without a recorded count.
func (t *inPlaceImageTransition) replacedSincePatch(pod *corev1.Pod, name string) bool {
	if t == nil || pod == nil {
		return false
	}
	recorded, known := t.RestartCounts[name]
	return known && containerRestartCount(pod, name) > recorded
}

// containerRestartCount is the kubelet's restart count for the named
// container; zero before the kubelet has reported it.
func containerRestartCount(pod *corev1.Pod, name string) int32 {
	if cs := containerStatusNamed(pod, name); cs != nil {
		return cs.RestartCount
	}
	return 0
}

// containerReady reports whether the kubelet reports the named container
// as having passed its readiness probe.
func containerReady(pod *corev1.Pod, name string) bool {
	cs := containerStatusNamed(pod, name)
	return cs != nil && cs.Ready
}

func containerStatusNamed(pod *corev1.Pod, name string) *corev1.ContainerStatus {
	if pod == nil {
		return nil
	}
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == name {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	return nil
}

// inPlaceImageRollConfirmed reports whether every container the roll
// re-images — those whose image differs between the running and target
// revisions, and those the transition is still rolling — runs the target
// image: the kubelet reports that image for it, or has replaced it since
// the patch. A node holds an image under whichever references put it
// there (a pull, a side-load, a second tag on the same image) and reports
// one of them, so the name alone is not required once the kubelet has
// acted on the patch. A container with neither proof keeps the pod out of
// rotation and the Instance at Updating.
func inPlaceImageRollConfirmed(pod *corev1.Pod, runningSpec, targetSpec *corev1.PodSpec, transition *inPlaceImageTransition) bool {
	if pod == nil || targetSpec == nil {
		return false
	}
	for name, image := range evidence.ChangedContainerImages(runningSpec, targetSpec) {
		if !containerImageConfirmed(pod, name, image, transition) {
			return false
		}
	}
	return transition == nil || inPlaceImageTransitionRuntimeMatches(pod, transition)
}

// containerImageConfirmed is the per-container proof the roll accepts:
// the kubelet reports image for the container, or has replaced the
// container since the patch the transition records.
func containerImageConfirmed(pod *corev1.Pod, name, image string, transition *inPlaceImageTransition) bool {
	return evidence.ContainerRuntimeImageIs(pod, name, image) || transition.replacedSincePatch(pod, name)
}

func inPlaceImageTransitionFromPod(pod *corev1.Pod) (*inPlaceImageTransition, bool, bool) {
	if pod == nil || pod.Annotations == nil {
		return nil, false, false
	}
	raw, present := pod.Annotations[inPlaceImageTransitionAnnotation]
	if !present {
		return nil, false, false
	}
	var transition inPlaceImageTransition
	if raw == "" || json.Unmarshal([]byte(raw), &transition) != nil || len(transition.TargetImages) == 0 {
		return nil, true, false
	}
	for name, image := range transition.TargetImages {
		if name == "" || image == "" {
			return nil, true, false
		}
	}
	return &transition, true, true
}

func imagePatchTargets(pod *corev1.Pod, target *corev1.PodSpec) map[string]string {
	if pod == nil || target == nil {
		return nil
	}
	currentImages := make(map[string]string, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		currentImages[container.Name] = container.Image
	}
	patches := make(map[string]string)
	for _, container := range target.Containers {
		if current, found := currentImages[container.Name]; found && current != container.Image {
			patches[container.Name] = container.Image
		}
	}
	if len(patches) == 0 {
		return nil
	}
	return patches
}

// ensureInPlaceImageTransition persists, before any image patch is issued,
// the images the patch moves each container to and the restart count the
// container has at that point. Pending container names survive a retarget,
// with their expected values rewritten from the current target and their
// count re-recorded. A recorded count is refreshed only for a Ready
// container whose count has moved since: its readiness could promote the
// row, so its baseline must be the count the patch is issued against. A
// container that is not Ready keeps its baseline, so a crash loop that
// restarts it between passes never defers the patch.
func ensureInPlaceImageTransition(ctx context.Context, c client.Client, pod *corev1.Pod, target *corev1.PodSpec, patches map[string]string) (bool, error) {
	if pod == nil || target == nil {
		return false, nil
	}
	current, present, valid := inPlaceImageTransitionFromPod(pod)
	if !present && len(patches) == 0 {
		return true, nil
	}
	targetImages := make(map[string]string, len(target.Containers))
	for _, container := range target.Containers {
		targetImages[container.Name] = container.Image
	}
	desired := &inPlaceImageTransition{TargetImages: make(map[string]string), RestartCounts: make(map[string]int32)}
	if present && valid {
		for name := range current.TargetImages {
			image, found := targetImages[name]
			if !found {
				valid = false
				break
			}
			desired.TargetImages[name] = image
			if count, recorded := current.RestartCounts[name]; recorded {
				desired.RestartCounts[name] = count
			}
		}
	}
	if present && !valid {
		desired.TargetImages = maps.Clone(targetImages)
		desired.RestartCounts = make(map[string]int32)
	}
	for name, image := range patches {
		live := containerRestartCount(pod, name)
		recorded, known := desired.RestartCounts[name]
		sameTarget := known && desired.TargetImages[name] == image
		if !sameTarget || (recorded != live && containerReady(pod, name)) {
			desired.RestartCounts[name] = live
		}
		desired.TargetImages[name] = image
	}
	if present && valid && maps.Equal(current.TargetImages, desired.TargetImages) && maps.Equal(current.RestartCounts, desired.RestartCounts) {
		return true, nil
	}
	if err := patchInPlaceImageTransition(ctx, c, pod, desired); err != nil {
		return false, err
	}
	return false, nil
}

func inPlaceImageTransitionMatchesTarget(transition *inPlaceImageTransition, target *corev1.PodSpec) bool {
	if transition == nil || target == nil {
		return false
	}
	targetImages := make(map[string]string, len(target.Containers))
	for _, container := range target.Containers {
		targetImages[container.Name] = container.Image
	}
	for name, image := range transition.TargetImages {
		if targetImages[name] != image {
			return false
		}
	}
	return true
}

// inPlaceImageTransitionRuntimeMatches reports whether every container the
// transition is rolling is confirmed at runtime: the kubelet reports its
// target image, or has replaced it since the patch the transition records.
func inPlaceImageTransitionRuntimeMatches(pod *corev1.Pod, transition *inPlaceImageTransition) bool {
	if pod == nil || transition == nil {
		return false
	}
	for name, targetImage := range transition.TargetImages {
		if !containerImageConfirmed(pod, name, targetImage, transition) {
			return false
		}
	}
	return true
}

func patchInPlaceImageTransition(ctx context.Context, c client.Client, pod *corev1.Pod, transition *inPlaceImageTransition) error {
	raw, err := json.Marshal(transition)
	if err != nil {
		return fmt.Errorf("marshal in-place image transition for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	base := pod.DeepCopy()
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[inPlaceImageTransitionAnnotation] = string(raw)
	patch := client.StrategicMergeFrom(base, client.MergeFromWithOptimisticLock{})
	if err := c.Patch(ctx, pod, patch); err != nil {
		return fmt.Errorf("patch in-place image transition for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// removeInPlaceImageTransition clears the transition marker. The key is
// written by this roll alone, so the removal names only it and takes no
// resource-version lock: the serving-gate patch issued earlier in the
// same pass, and the kubelet's own status writes, move the pod's version
// without touching the marker, and a clear that conflicted on them would
// leave the marker behind with no later pass to retry it.
func removeInPlaceImageTransition(ctx context.Context, c client.Client, pod *corev1.Pod) error {
	if pod == nil || pod.Annotations == nil {
		return nil
	}
	if _, present := pod.Annotations[inPlaceImageTransitionAnnotation]; !present {
		return nil
	}
	raw, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{inPlaceImageTransitionAnnotation: nil}},
	})
	if err != nil {
		return fmt.Errorf("marshal in-place image transition removal for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, raw)); err != nil {
		return fmt.Errorf("remove in-place image transition from pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// The in-place writers: the strategic-merge patches
// that move a live pod onto the target revision's images, its
// revision-owned labels and its annotations. What the pod still needs
// patched is evidence.PodImagesMatch / evidence.AnnotationsDiff; this
// file only issues the writes.

// patchPodImages strategic-merge-patches each changed container image with an
// optimistic resource-version precondition. Strategic merge keys by container
// name, and the precondition rejects a concurrent pod or status mutation after
// the caller's live read.
// Returns whether a patch was actually issued so callers only requeue
// for a kubelet roll when one is coming.
func patchPodImages(ctx context.Context, c client.Client, pod *corev1.Pod, target *corev1.PodSpec) (bool, error) {
	if pod == nil || target == nil {
		return false, fmt.Errorf("patchPodImages: nil pod or target")
	}
	wantByName := make(map[string]string, len(target.Containers))
	for _, t := range target.Containers {
		wantByName[t.Name] = t.Image
	}
	base := pod.DeepCopy()
	changed := false
	for i := range pod.Spec.Containers {
		current := &pod.Spec.Containers[i]
		newImage, ok := wantByName[current.Name]
		if !ok || newImage == current.Image {
			continue
		}
		current.Image = newImage
		changed = true
	}
	if !changed {
		return false, nil
	}
	patch := client.StrategicMergeFrom(base, client.MergeFromWithOptimisticLock{})
	if err := c.Patch(ctx, pod, patch); err != nil {
		return false, fmt.Errorf("patch pod %s/%s images: %w", pod.Namespace, pod.Name, err)
	}
	return true, nil
}

// patchPodRevisionLabels strategic-merge-patches the pod's revision-owned
// labels (ome.io/revision-hash, ome.io/pairing-protocol) to the target
// revision's values. An in-place update changes a pod's revision (image /
// annotations) WITHOUT recreating it, but those labels are stamped only at
// pod create time (render.go). Without restamping them here, an
// in-place-rolled pod keeps its OLD revision-hash label while
// InstanceStatus.RunningRevision advances — so per-revision Service
// routing, drain (drain.IsPodDrained), and stuck-pod detection
// (HasWedgedPodAgainstCurrent), which all key on this label, treat the
// rolled pod as the PREVIOUS revision — and a stale pairing-protocol label
// would misreport the pod's cohort. An empty pairingProtocol deletes the
// label (the target revision pairs with anything). Returns whether a patch
// was issued; matching labels and empty target hashes are no-ops.
func patchPodRevisionLabels(ctx context.Context, c client.Client, pod *corev1.Pod, targetHash, pairingProtocol string) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("patchPodRevisionLabels: nil pod")
	}
	if targetHash == "" {
		return false, nil
	}
	labelsPatch := map[string]any{}
	if pod.Labels[query.LabelRevisionHash] != targetHash {
		labelsPatch[query.LabelRevisionHash] = targetHash
	}
	current, hasCurrent := pod.Labels[query.LabelPairingProtocol]
	switch {
	case pairingProtocol != "" && current != pairingProtocol:
		labelsPatch[query.LabelPairingProtocol] = pairingProtocol
	case pairingProtocol == "" && hasCurrent:
		// Strategic-merge null deletes the key.
		labelsPatch[query.LabelPairingProtocol] = nil
	}
	if len(labelsPatch) == 0 {
		return false, nil
	}
	raw, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"labels": labelsPatch},
	})
	if err != nil {
		return false, fmt.Errorf("marshal revision label patch for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, raw)); err != nil {
		return false, fmt.Errorf("patch pod %s/%s revision labels: %w", pod.Namespace, pod.Name, err)
	}
	return true, nil
}

// patchPodAnnotations applies a strategic-merge patch on the pod's
// metadata.annotations to reconcile it toward the target revision's
// PodMeta. previousTargetAnnotations is the running revision's PodMeta
// (used to compute which keys OMENative owns and is allowed to
// delete). Returns whether a patch was issued; an empty diff is a no-op.
// Without this reconcile, in-place updates would leave
// pod.metadata.annotations stuck at the value the pod was created
// with, so spec.{component}.annotations edits during an
// in-place-eligible rollout would never reach the pod.
func patchPodAnnotations(ctx context.Context, c client.Client, pod *corev1.Pod, previousTargetAnnotations, newTargetAnnotations map[string]string) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("patchPodAnnotations: nil pod")
	}
	diff := evidence.AnnotationsDiff(pod.Annotations, previousTargetAnnotations, newTargetAnnotations)
	if len(diff) == 0 {
		return false, nil
	}
	raw, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": diff},
	})
	if err != nil {
		return false, fmt.Errorf("marshal annotation patch for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, raw)); err != nil {
		return false, fmt.Errorf("patch pod %s/%s annotations: %w", pod.Namespace, pod.Name, err)
	}
	return true, nil
}
