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

// inPlacePhase is one step of an in-place pass. It reports whether the pass
// goes on to the next step; a step that ends the pass returns false with
// the error that ended it, if any.
type inPlacePhase func(ctx context.Context) (next bool, err error)

// runInPlacePhases runs the phases in order and reports whether every one
// of them went on, which is the roll finishing. The first phase that ends
// the pass ends it, with its error.
func runInPlacePhases(ctx context.Context, phases []inPlacePhase) (bool, error) {
	for _, phase := range phases {
		if next, err := phase(ctx); err != nil || !next {
			return false, err
		}
	}
	return true, nil
}

// inPlaceRoll is one pass of the in-place roll of an Instance: what the
// pass read at its start, and what its phases learn as they run. The pods
// keep their names and their generation number; only their images, template
// metadata and revision labels move, in one patch per pod.
type inPlaceRoll struct {
	deps       workload.Deps
	input      workload.ReconcileInput
	plan       workload.ComponentPlan
	inst       workload.InstancePlan
	target     *appsv1.ControllerRevision
	targetSpec *corev1.PodSpec
	// pods are the Instance's pods as the pass listed them.
	pods []*corev1.Pod

	targetRevision query.RevisionID
	// markNotReady: the strategy takes a pod out of rotation before its
	// images move and returns it once the kubelet runs the new images.
	markNotReady bool

	// template is what the patch moves a pod onto (loadTemplate).
	template podTemplateChanges
	// runningSpec is the running revision's pod template, nil when no
	// running revision is recorded (loadTemplate).
	runningSpec *corev1.PodSpec
	// carried widens the template's deletable keys with the templates of
	// the revisions the pods carry besides the target and the running
	// revision (loadTemplate, readLivePods).
	carried carriedTemplates
	// livePods are the pods re-read from the apiserver before the patch
	// (readLivePods); the phases from the patch on work on these.
	livePods []*corev1.Pod
	// transitions is each live pod's image transition record by pod name,
	// once every re-imaged container is confirmed (confirmImageTransition).
	transitions map[string]*inPlaceImageTransition
}

// newInPlaceRoll reads what one pass of the roll needs from its inputs.
func newInPlaceRoll(deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec, pods []*corev1.Pod) *inPlaceRoll {
	roll := &inPlaceRoll{
		deps:           deps,
		input:          input,
		plan:           plan,
		inst:           inst,
		target:         target,
		targetSpec:     targetSpec,
		pods:           pods,
		targetRevision: query.RevisionOf(target),
		markNotReady:   true,
	}
	if strategy := plan.UpdateStrategy.InPlaceUpdateStrategy; strategy != nil && strategy.MarkNotReadyDuringLifecycle != nil {
		roll.markNotReady = *strategy.MarkNotReadyDuringLifecycle
	}
	return roll
}

// inPlaceUpdate runs the in-place roll of one Instance. The pods keep their
// names; one patch per pod moves its images, template metadata and revision
// labels together. Metadata keys the new revision names are written and
// keys only the running revision, or a revision a pod was patched onto
// before the row recorded it, authored are deleted; labels and
// annotations from other owners are left alone, and a metadata change
// alone never forces a rebuild. With no pod standing the roll is
// re-resolved to recreate, which does render a replacement. Otherwise the
// phases below run in order, from the top on every pass; the row and the
// pods found decide how far a pass gets:
//
//	open the attempt; remove a pod in a terminal phase; load the template;
//	relabel a pod already on the target template; take the pods out of
//	rotation; wait for them to drain; re-read the pods; patch them onto
//	the target; confirm the kubelet runs the new images; mark the pods
//	serving; wait for the promote bar; promote.
func inPlaceUpdate(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec, pods []*corev1.Pod) (bool, error) {
	roll := newInPlaceRoll(deps, input, plan, inst, target, targetSpec, pods)
	if len(pods) == 0 {
		return roll.recreateWhenNoPodStands(ctx)
	}
	return runInPlacePhases(ctx, []inPlacePhase{
		roll.openAttempt,
		roll.removeTerminalPods,
		roll.loadTemplate,
		roll.relabelPodsOnTargetTemplate,
		roll.takePodsOutOfRotation,
		roll.waitForPodsToDrain,
		roll.readLivePods,
		roll.patchPods,
		roll.confirmImageTransition,
		roll.markPodsServing,
		roll.waitForPromoteBar,
		roll.promote,
	})
}

// recreateWhenNoPodStands re-resolves the roll to recreate when the
// Instance has no pod: an in-place step patches pods it does not create,
// and no other pass rebuilds an Instance the update pass owns. The empty
// set counts only once this Instance's own creates and deletes have been
// observed; before that it can be a read that predates them, and
// re-resolving on one would raise the Incarnation under a pod that does
// exist.
func (r *inPlaceRoll) recreateWhenNoPodStands(ctx context.Context) (bool, error) {
	if !r.deps.ExpectationsCache().Satisfied(r.input.Key.Namespace, r.input.Key.OwnerName, r.input.Key.Component, r.inst.Index) {
		return false, nil
	}
	reresolved, err := status.StampRecreateFromInPlace(ctx, r.input, r.inst.Index, r.target.Name)
	if err != nil {
		return false, fmt.Errorf("re-resolve in-place roll to recreate (instance=%d): %w", r.inst.Index, err)
	}
	if reresolved {
		workload.RecordWarning(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonInPlaceUpdateNotPossible,
			"OMENative %s lost the pod its in-place update to revision %s was patching; recreating instead",
			workload.InstanceKey(r.input.Key.Component, r.inst.Index), r.target.Name)
	}
	return recreateUpdate(ctx, r.deps, r.input, r.plan, r.inst, r.target, r.pods)
}

// openAttempt writes the row Updating at the InPlace step and records the
// started event on the first pass; a later pass finds the row there and
// the write is a no-op. The Incarnation stays: only the pods' images and
// metadata move. A row the update pass owns is mid-roll; a row in Updating
// only because a held attempt's pods serve is not, and the patch over it
// is a first pass.
func (r *inPlaceRoll) openAttempt(ctx context.Context) (bool, error) {
	wasNotUpdating := workload.Owner(r.input.ObservedState.Instance(r.inst.Index)) != workload.OwnerUpdate
	if err := status.StampUpdatingInPlace(ctx, r.input, r.inst.Index, r.target.Name, r.plan.UpdateStrategy.Type, r.plan.InstanceReadyTimeout); err != nil {
		return false, fmt.Errorf("patch status Updating (instance=%d): %w", r.inst.Index, err)
	}
	if wasNotUpdating {
		workload.RecordNormal(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonInPlaceUpdateStarted,
			"OMENative %s in-place update to revision %s",
			workload.InstanceKey(r.input.Key.Component, r.inst.Index), r.target.Name)
	}
	return true, nil
}

// removeTerminalPods deletes a pod in a terminal phase and ends the pass:
// such a pod can never report the new image, so it is freed now rather
// than patched and polled to the deadline. The empty set then re-resolves
// the roll to recreate (recreateWhenNoPodStands).
func (r *inPlaceRoll) removeTerminalPods(ctx context.Context) (bool, error) {
	recycling, err := recycleTerminalTargets(ctx, r.deps, r.input, r.inst.Index, r.inst.Index,
		workload.InstanceOperationUpdate, r.pods, expectedPodNamesForInstance(r.input, r.plan, r.inst))
	if err != nil {
		return false, fmt.Errorf("recycle terminal pod (instance=%d): %w", r.inst.Index, err)
	}
	return !recycling, nil
}

// loadTemplate reads what the patch moves a pod onto: the target
// revision's images, template labels and annotations and revision labels,
// the running revision's template, whose keys the previous spec owned and
// the patch may delete, and the template of any other revision a pod's
// label names, whose keys the patch may delete too. Either payload may be
// missing (a revision with no PodMeta yet, a first in-place update with no
// recorded running revision); the diffs read nil as empty.
func (r *inPlaceRoll) loadTemplate(ctx context.Context) (bool, error) {
	targetPayload, err := loadControllerRevisionPayload(r.target)
	if err != nil {
		return false, fmt.Errorf("load target CR data (instance=%d): %w", r.inst.Index, err)
	}
	runningPayload, err := loadRunningRevisionPayload(ctx, r.deps.Reader(), r.input, r.inst.Index)
	if err != nil {
		return false, fmt.Errorf("load running CR data (instance=%d): %w", r.inst.Index, err)
	}
	r.template = podTemplateChanges{spec: r.targetSpec, revisionHash: r.targetRevision.Hash()}
	if targetPayload != nil && targetPayload.PodMeta != nil {
		r.template.targetLabels = targetPayload.PodMeta.Labels
		r.template.targetAnnotations = targetPayload.PodMeta.Annotations
	}
	if targetPayload != nil && targetPayload.PairingProtocol != nil {
		r.template.pairingProtocol = *targetPayload.PairingProtocol
	}
	if runningPayload != nil {
		r.runningSpec = runningPayload.PodSpec
		if runningPayload.PodMeta != nil {
			r.template.previousLabels = runningPayload.PodMeta.Labels
			r.template.previousAnnotations = runningPayload.PodMeta.Annotations
		}
	}
	// A pod patched onto another revision before the row recorded it still
	// carries that revision's template, which its revision label names. Its
	// keys are deletable too, or a revert to the running revision would
	// leave them on the pod.
	r.carried = newCarriedTemplates(r.deps.Reader(), r.input, r.plan.Component, r.inst.Index)
	if err := r.carried.widen(ctx, &r.template, r.pods); err != nil {
		return false, fmt.Errorf("load the revisions the pods carry (instance=%d): %w", r.inst.Index, err)
	}
	return true, nil
}

// relabelPodsOnTargetTemplate moves a pod that already runs the target
// template under another revision's label onto the target's labels before
// anything else, the drain included: that label is what the previous
// revision's routing Service selects by. One relabel ends the pass; a pod
// gone since the list ends it too.
func (r *inPlaceRoll) relabelPodsOnTargetTemplate(ctx context.Context) (bool, error) {
	for _, pod := range r.pods {
		if r.onTarget(pod) || !r.template.carriedBy(pod) {
			continue
		}
		fresh := &corev1.Pod{}
		if gerr := r.deps.Reader().Get(ctx, client.ObjectKeyFromObject(pod), fresh); gerr != nil {
			if apierrors.IsNotFound(gerr) {
				return false, nil
			}
			return false, fmt.Errorf("re-read pod before relabel (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, gerr)
		}
		if r.onTarget(fresh) || !r.template.carriedBy(fresh) {
			continue
		}
		if _, perr := patchPodTemplate(ctx, r.deps.Client, fresh, r.template); perr != nil {
			if handled, herr := r.recordRefusedPatch(ctx, fresh, perr); handled {
				return false, herr
			}
			return false, fmt.Errorf("relabel pod on the target template (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, perr)
		}
		return false, nil
	}
	return true, nil
}

// takePodsOutOfRotation sets the serving gate false on every pod still in
// rotation that is not yet on the target revision, under the strategy that
// drains before the patch. A pod already relabelled to the target has been
// through the drain and the patch and is back in, or returning to,
// rotation: draining it again would remove converged capacity and restart
// its PodReady age on every pass while the promote waits out the
// availability window.
func (r *inPlaceRoll) takePodsOutOfRotation(ctx context.Context) (bool, error) {
	if !r.markNotReady {
		return true, nil
	}
	for _, pod := range r.pods {
		if !podreadiness.IsServing(pod) || r.onTarget(pod) {
			continue
		}
		if err := podreadiness.MarkPodNotServing(ctx, r.deps.Client, r.deps.Reader(), pod, podreadiness.WriterUpdateInPlace, updateDrainKey(r.inst.Index, r.inst.Incarnation)); err != nil {
			if handled, herr := r.waitForAdmission(ctx, pod, err); handled {
				return false, herr
			}
			return false, fmt.Errorf("mark not serving (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
	}
	return true, nil
}

// waitForPodsToDrain ends the pass while a pod not yet on the target
// revision still has a Ready endpoint in its routed Service, read live so
// the patch never lands on a pod the proxies still route to. Only the
// strategy that drains before the patch waits.
func (r *inPlaceRoll) waitForPodsToDrain(ctx context.Context) (bool, error) {
	if !r.markNotReady {
		return true, nil
	}
	for _, pod := range r.pods {
		if r.onTarget(pod) {
			continue
		}
		serviceName := drainServiceForPod(r.input, r.plan, pod)
		if serviceName == "" {
			continue
		}
		drained, err := drain.IsPodDrained(ctx, r.deps.Reader(), r.input.Key.Namespace, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("check drain (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
		if !drained {
			return false, nil
		}
	}
	return true, nil
}

// readLivePods re-reads every pod of the Instance from the apiserver. The
// kubelet's status writes advance a pod's resourceVersion ahead of the
// cached copy, so a patch locked to the cached copy conflicts until the
// cache catches up; the live copy also closes the window in which a pod
// returned to rotation after the drain check. A pod gone since the list
// ends the pass.
func (r *inPlaceRoll) readLivePods(ctx context.Context) (bool, error) {
	r.livePods = make([]*corev1.Pod, 0, len(r.pods))
	for _, pod := range r.pods {
		fresh := &corev1.Pod{}
		if gerr := r.deps.Reader().Get(ctx, client.ObjectKeyFromObject(pod), fresh); gerr != nil {
			if apierrors.IsNotFound(gerr) {
				return false, nil
			}
			return false, fmt.Errorf("re-read pod before in-place update (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, gerr)
		}
		r.livePods = append(r.livePods, fresh)
	}
	// The live read can show a pod on a revision the observation did not
	// carry yet.
	if err := r.carried.widen(ctx, &r.template, r.livePods); err != nil {
		return false, fmt.Errorf("load the revisions the live pods carry (instance=%d): %w", r.inst.Index, err)
	}
	return true, nil
}

// patchPods moves every live pod onto the target template and ends the
// pass when any patch was issued, so the next pass reads the pods again
// before it confirms them. When every patch went through admission, an
// admission wait a refused pass left on the row is over.
func (r *inPlaceRoll) patchPods(ctx context.Context) (bool, error) {
	mutated := false
	for _, pod := range r.livePods {
		issued, next, err := r.patchPod(ctx, pod)
		if err != nil || !next {
			return false, err
		}
		mutated = mutated || issued
	}
	if err := releaseAdmissionWait(ctx, r.input, r.inst.Index); err != nil {
		return false, fmt.Errorf("release admission wait (instance=%d): %w", r.inst.Index, err)
	}
	return !mutated, nil
}

// patchPod moves one live pod onto the target template. Under the strategy
// that drains first, a pod that returned to rotation after the drain check
// is taken out again before its images move, and the pass ends. The image
// transition record is written before the patch that moves the images, so
// a later pass can tell which containers the kubelet has replaced since;
// writing it ends the pass. One write then moves the pod onto the target
// template and the target revision's labels together, so a manager that
// stops right after it leaves no pod running one revision under another's
// label. Reports whether a write was issued and whether the pass goes on
// to the next pod.
func (r *inPlaceRoll) patchPod(ctx context.Context, pod *corev1.Pod) (issued, next bool, err error) {
	imagePatches := imagePatchTargets(pod, r.targetSpec)
	if r.markNotReady && len(imagePatches) > 0 && podreadiness.IsServing(pod) {
		if merr := podreadiness.MarkPodNotServing(ctx, r.deps.Client, r.deps.Reader(), pod, podreadiness.WriterUpdateInPlace, updateDrainKey(r.inst.Index, r.inst.Incarnation)); merr != nil {
			if handled, herr := r.waitForAdmission(ctx, pod, merr); handled {
				return false, false, herr
			}
			return false, false, fmt.Errorf("re-mark not serving (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, merr)
		}
		return false, false, nil
	}
	transitionRecorded, terr := ensureInPlaceImageTransition(ctx, r.deps.Client, pod, r.targetSpec, imagePatches)
	if terr != nil {
		if handled, herr := r.recordRefusedPatch(ctx, pod, terr); handled {
			return false, false, herr
		}
		return false, false, fmt.Errorf("record image transition (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, terr)
	}
	if !transitionRecorded {
		return false, false, nil
	}
	issued, perr := patchPodTemplate(ctx, r.deps.Client, pod, r.template)
	if perr != nil {
		if handled, herr := r.recordRefusedPatch(ctx, pod, perr); handled {
			return false, false, herr
		}
		return false, false, fmt.Errorf("patch pod onto the target revision (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, perr)
	}
	return issued, true, nil
}

// confirmImageTransition ends the pass until the kubelet runs the target
// image in every container the roll re-images. ContainersReady can stay
// true in a stale observation after an image patch, so every live pod's
// containers must be runtime-ready and its images must match the target
// spec, and each changed container must be confirmed: the kubelet reports
// the target image for it, or has replaced it since the patch, which the
// transition record's restart counts tell. A record that does not parse
// or names another target ends the pass; an empty changed-image set is
// already satisfied. Each pod's record is kept for the promote bar.
func (r *inPlaceRoll) confirmImageTransition(context.Context) (bool, error) {
	if !query.AllPodsRuntimeReady(r.livePods) {
		return false, nil
	}
	r.transitions = make(map[string]*inPlaceImageTransition, len(r.livePods))
	for _, pod := range r.livePods {
		if !evidence.PodImagesMatch(pod, r.targetSpec) {
			return false, nil
		}
		transition, present, valid := inPlaceImageTransitionFromPod(pod)
		valid = valid && inPlaceImageTransitionMatchesTarget(transition, r.targetSpec)
		if present && !valid {
			return false, nil
		}
		if !inPlaceImageRollConfirmed(pod, r.runningSpec, r.targetSpec, transition) {
			return false, nil
		}
		r.transitions[pod.Name] = transition
	}
	return true, nil
}

// markPodsServing sets the serving gate true on every live pod that is out
// of rotation; the kubelet folds it into PodReady and the pod returns to
// its routed Service on the new images.
func (r *inPlaceRoll) markPodsServing(ctx context.Context) (bool, error) {
	for _, pod := range r.livePods {
		if podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodServing(ctx, r.deps.Client, r.deps.Reader(), pod, podreadiness.WriterUpdateInPlace, updateDrainKey(r.inst.Index, r.inst.Incarnation)); err != nil {
			return false, fmt.Errorf("mark serving (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
	}
	return true, nil
}

// waitForPromoteBar ends the pass until every live pod has held PodReady
// for the availability window with every re-imaged container confirmed at
// runtime, the in-place extra over the shared promote bar. The promote
// releases this Instance's unavailability budget slot, so a pod the
// serving-gate write has not yet carried back into rotation holds it.
func (r *inPlaceRoll) waitForPromoteBar(context.Context) (bool, error) {
	promotable, wait := query.PodSetPromotable(r.livePods, r.plan.MinReadySeconds, r.input.Now(),
		func(pod *corev1.Pod) bool {
			return inPlaceImageRollConfirmed(pod, r.runningSpec, r.targetSpec, r.transitions[pod.Name])
		})
	if !promotable {
		r.input.PromoteWindow.Observe(wait)
		return false, nil
	}
	return true, nil
}

// promote writes the row Ready on the target revision, records the
// completed event, and clears each pod's transition record. The record
// outlives the Ready write: it carries the restart counts the confirmation
// compares against, and a pass that found it gone before the row was Ready
// could not confirm a container the kubelet reports under another name. A
// record a failed clear leaves behind is rewritten by the next roll's patch.
func (r *inPlaceRoll) promote(ctx context.Context) (bool, error) {
	if err := status.StampReadyOnRevision(ctx, r.input, r.inst.Index, r.target.Name); err != nil {
		return false, fmt.Errorf("patch status Ready (instance=%d): %w", r.inst.Index, err)
	}
	workload.RecordNormal(r.deps.Recorder, workload.EventTarget(r.input), workload.EventReasonInPlaceUpdateCompleted,
		"OMENative %s in-place update to revision %s complete",
		workload.InstanceKey(r.input.Key.Component, r.inst.Index), r.target.Name)
	for _, pod := range r.livePods {
		if err := removeInPlaceImageTransition(ctx, r.deps.Client, pod); err != nil {
			return false, fmt.Errorf("clear image transition (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, err)
		}
	}
	return true, nil
}

// onTarget reports whether pod already carries the target revision's
// label: it has been through the drain and the patch.
func (r *inPlaceRoll) onTarget(pod *corev1.Pod) bool {
	return query.RevisionFromPod(pod).Same(r.targetRevision)
}

// waitForAdmission records, when the apiserver could not take the pod write
// to admission, that the row waits on it, in the apiserver's words, and
// reports the write handled: the pass ends with no error for the ordinary
// retry, as a refused create does. Any other refusal is left to the caller.
func (r *inPlaceRoll) waitForAdmission(ctx context.Context, pod *corev1.Pod, err error) (bool, error) {
	rejection := evidence.ClassifyAPIError(err)
	if rejection.Class != workload.APIRejectionAdmissionUnavailable {
		return false, nil
	}
	if rerr := recordAdmissionWait(ctx, r.deps, r.input, r.plan, r.inst.Index, pod.Name, rejection); rerr != nil {
		return true, fmt.Errorf("record admission wait (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, rerr)
	}
	return true, nil
}

// recordRefusedPatch settles a refused pod patch. An admission outage is a
// wait on the row (waitForAdmission). A refusal the apiserver gives again
// however often the patch is retried is a statement about the revision: the
// attempt is written Failed and the revision charged, exactly as a rejected
// create is, so the corrected revision is admitted at once. Reports whether
// the refusal was handled; any other error is left to the caller.
func (r *inPlaceRoll) recordRefusedPatch(ctx context.Context, pod *corev1.Pod, err error) (bool, error) {
	if handled, herr := r.waitForAdmission(ctx, pod, err); handled {
		return true, herr
	}
	rejection := evidence.ClassifyAPIError(err)
	if !rejection.Class.Permanent() {
		return false, nil
	}
	if derr := disposeRejectedAttempt(ctx, r.deps, r.input, r.inst.Index, r.target.Name, pod.Name, rejection, refusalEndsTheAttempt); derr != nil {
		return true, fmt.Errorf("dispose rejected in-place patch (instance=%d, pod=%s): %w", r.inst.Index, pod.Name, derr)
	}
	return true, nil
}

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
	raw, present := pod.Annotations[constants.InferenceServiceInPlaceImageTransitionAnnotationKey]
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
	currentImages := containerImages(&pod.Spec)
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
// container has at that point. The record pod carries is carried over to
// the target (carriedImageTransition) or started over, the patched
// containers are added (recordPatches), and the record is written when it
// differs from the one the pod carries. Reports whether the record is in
// place: a write ends the pass, and the next pass finds it.
func ensureInPlaceImageTransition(ctx context.Context, c client.Client, pod *corev1.Pod, target *corev1.PodSpec, patches map[string]string) (bool, error) {
	if pod == nil || target == nil {
		return false, nil
	}
	current, present, valid := inPlaceImageTransitionFromPod(pod)
	if !present && len(patches) == 0 {
		return true, nil
	}
	targetImages := containerImages(target)
	carried := carriedImageTransition(current, present && valid, targetImages)
	desired := carried
	if desired == nil {
		desired = startedOverImageTransition(present, targetImages)
	}
	desired.recordPatches(pod, patches)
	if carried != nil && current.equal(desired) {
		return true, nil
	}
	if err := patchInPlaceImageTransition(ctx, c, pod, desired); err != nil {
		return false, err
	}
	return false, nil
}

// carriedImageTransition is the readable record a pod carries, carried
// over to the target: each container it names at the target's image for
// it, with its recorded restart count kept, so a pending container
// survives a retarget. nil when the record is not readable or names a
// container the target no longer has; the record then starts over.
func carriedImageTransition(current *inPlaceImageTransition, readable bool, targetImages map[string]string) *inPlaceImageTransition {
	if !readable {
		return nil
	}
	carried := &inPlaceImageTransition{TargetImages: make(map[string]string), RestartCounts: make(map[string]int32)}
	for name := range current.TargetImages {
		image, found := targetImages[name]
		if !found {
			return nil
		}
		carried.TargetImages[name] = image
		if count, recorded := current.RestartCounts[name]; recorded {
			carried.RestartCounts[name] = count
		}
	}
	return carried
}

// startedOverImageTransition is the record of a pod whose record cannot be
// carried over: every target image with no count when the pod carried an
// unreadable record, empty when it carried none.
func startedOverImageTransition(present bool, targetImages map[string]string) *inPlaceImageTransition {
	fresh := &inPlaceImageTransition{TargetImages: make(map[string]string), RestartCounts: make(map[string]int32)}
	if present {
		fresh.TargetImages = maps.Clone(targetImages)
	}
	return fresh
}

// recordPatches adds each patched container to the record: the image it
// moves to and, unless the record already names that image for it with
// the same count or the container is not Ready, the restart count it has
// now. A Ready container's readiness could promote the row, so its
// baseline must be the count the patch is issued against; a container
// that is not Ready keeps its baseline, so a crash loop that restarts it
// between passes never defers the patch.
func (t *inPlaceImageTransition) recordPatches(pod *corev1.Pod, patches map[string]string) {
	for name, image := range patches {
		live := containerRestartCount(pod, name)
		recorded, known := t.RestartCounts[name]
		sameTarget := known && t.TargetImages[name] == image
		if !sameTarget || (recorded != live && containerReady(pod, name)) {
			t.RestartCounts[name] = live
		}
		t.TargetImages[name] = image
	}
}

// equal reports whether two records name the same images and counts.
func (t *inPlaceImageTransition) equal(other *inPlaceImageTransition) bool {
	return maps.Equal(t.TargetImages, other.TargetImages) && maps.Equal(t.RestartCounts, other.RestartCounts)
}

// containerImages is each container's image in spec, by container name.
func containerImages(spec *corev1.PodSpec) map[string]string {
	images := make(map[string]string, len(spec.Containers))
	for _, container := range spec.Containers {
		images[container.Name] = container.Image
	}
	return images
}

func inPlaceImageTransitionMatchesTarget(transition *inPlaceImageTransition, target *corev1.PodSpec) bool {
	if transition == nil || target == nil {
		return false
	}
	targetImages := containerImages(target)
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
	pod.Annotations[constants.InferenceServiceInPlaceImageTransitionAnnotationKey] = string(raw)
	patch := client.StrategicMergeFrom(base, client.MergeFromWithOptimisticLock{})
	if err := c.Patch(ctx, pod, patch); err != nil {
		return fmt.Errorf("patch in-place image transition for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// removeInPlaceImageTransition clears the pod's transition record. The
// annotation is written by this roll alone, so the removal names only it
// and takes no resource-version lock: the serving-gate patch issued
// earlier in the same pass, and the kubelet's own status writes, move the
// pod's version without touching the record, and a clear that conflicted
// on them would leave the record behind with no later pass to retry it.
func removeInPlaceImageTransition(ctx context.Context, c client.Client, pod *corev1.Pod) error {
	if pod == nil || pod.Annotations == nil {
		return nil
	}
	if _, present := pod.Annotations[constants.InferenceServiceInPlaceImageTransitionAnnotationKey]; !present {
		return nil
	}
	raw, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{constants.InferenceServiceInPlaceImageTransitionAnnotationKey: nil}},
	})
	if err != nil {
		return fmt.Errorf("marshal in-place image transition removal for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if err := c.Patch(ctx, pod, client.RawPatch(types.StrategicMergePatchType, raw)); err != nil {
		return fmt.Errorf("remove in-place image transition from pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

// The in-place writer: one strategic-merge patch moves a live pod onto the
// target revision's images, template metadata and revision-owned labels.
// evidence.PodImagesMatch, LabelsDiff and AnnotationsDiff say what it owes.

// podTemplateChanges is what the in-place step moves a live pod onto: the
// target revision's images, template labels and annotations, and the labels
// the revision owns. The metadata of the running revision, and of any
// revision a pod was patched onto before the row recorded it, names the
// deletable keys.
type podTemplateChanges struct {
	spec                                   *corev1.PodSpec
	previousLabels, targetLabels           map[string]string
	previousAnnotations, targetAnnotations map[string]string
	revisionHash, pairingProtocol          string
}

// carriedTemplates widens a pass's deletable key sets with the template
// metadata of every revision the Instance's pods carry besides the target
// and the running revision. Each revision is read once per pass; one that
// is gone adds nothing.
type carriedTemplates struct {
	reads     client.Reader
	input     workload.ReconcileInput
	component workload.ComponentType
	running   query.RevisionID
	loaded    map[string]bool
}

func newCarriedTemplates(reads client.Reader, input workload.ReconcileInput, component workload.ComponentType, idx int32) carriedTemplates {
	c := carriedTemplates{reads: reads, input: input, component: component}
	if row := input.ObservedState.Instance(idx); row != nil {
		c.running = query.RevisionFromName(row.RunningRevision)
	}
	return c
}

// widen adds the template keys of the revisions pods carry to template's
// deletable sets.
func (c *carriedTemplates) widen(ctx context.Context, template *podTemplateChanges, pods []*corev1.Pod) error {
	target := query.RevisionFromHash(template.revisionHash)
	for _, pod := range pods {
		carried := query.RevisionFromPod(pod)
		if carried.IsZero() || carried.Same(target) || carried.Same(c.running) || c.loaded[carried.Hash()] {
			continue
		}
		if c.loaded == nil {
			c.loaded = map[string]bool{}
		}
		c.loaded[carried.Hash()] = true
		payload, err := loadRevisionPayload(ctx, c.reads, c.input.Key.Namespace, revisionNamed(c.input, c.component, carried.Hash()))
		if err != nil {
			return fmt.Errorf("load revision %s carried by pod %s: %w", carried, pod.Name, err)
		}
		if payload == nil || payload.PodMeta == nil {
			continue
		}
		template.previousLabels = withKeys(template.previousLabels, payload.PodMeta.Labels)
		template.previousAnnotations = withKeys(template.previousAnnotations, payload.PodMeta.Annotations)
	}
	return nil
}

// withKeys returns previous widened with every key of more; a value already
// in previous wins.
func withKeys(previous, more map[string]string) map[string]string {
	if len(more) == 0 {
		return previous
	}
	merged := make(map[string]string, len(previous)+len(more))
	maps.Copy(merged, more)
	maps.Copy(merged, previous)
	return merged
}

// carriedBy reports whether pod already runs the template, whatever its
// revision labels say: no image, template label or annotation differs.
func (t podTemplateChanges) carriedBy(pod *corev1.Pod) bool {
	return len(imagePatchTargets(pod, t.spec)) == 0 &&
		len(evidence.AnnotationsDiff(pod.Annotations, t.previousAnnotations, t.targetAnnotations)) == 0 &&
		len(evidence.LabelsDiff(pod.Labels, t.previousLabels, t.targetLabels)) == 0
}

// apply moves pod onto the template in memory and reports whether anything
// changed. The caller issues the write.
func (t podTemplateChanges) apply(pod *corev1.Pod) bool {
	changed := applyPodImages(pod, t.spec)
	changed = applyMetadata(&pod.Annotations, evidence.AnnotationsDiff(pod.Annotations, t.previousAnnotations, t.targetAnnotations)) || changed
	changed = applyTemplateLabels(pod, t.previousLabels, t.targetLabels) || changed
	return applyRevisionLabels(pod, t.revisionHash, t.pairingProtocol) || changed
}

// patchPodTemplate moves a live pod onto the template in one strategic-merge
// patch locked to the caller's read, so no write leaves the pod running one
// revision's template under another's label. Reports whether it wrote.
func patchPodTemplate(ctx context.Context, c client.Client, pod *corev1.Pod, template podTemplateChanges) (bool, error) {
	if pod == nil || template.spec == nil {
		return false, fmt.Errorf("patchPodTemplate: nil pod or target")
	}
	base := pod.DeepCopy()
	if !template.apply(pod) {
		return false, nil
	}
	patch := client.StrategicMergeFrom(base, client.MergeFromWithOptimisticLock{})
	if err := c.Patch(ctx, pod, patch); err != nil {
		return false, fmt.Errorf("patch pod %s/%s onto revision %s: %w", pod.Namespace, pod.Name, template.revisionHash, err)
	}
	return true, nil
}

// applyPodImages rewrites each changed container image in memory; a
// container the target does not name is left alone.
func applyPodImages(pod *corev1.Pod, target *corev1.PodSpec) bool {
	wantByName := make(map[string]string, len(target.Containers))
	for _, t := range target.Containers {
		wantByName[t.Name] = t.Image
	}
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
	return changed
}

// applyRevisionLabels sets the labels the revision owns: the revision hash
// the routed Services, drain and stuck-pod detection key on, and the pairing
// protocol, which an empty protocol deletes. An empty hash changes nothing.
func applyRevisionLabels(pod *corev1.Pod, targetHash, pairingProtocol string) bool {
	if targetHash == "" {
		return false
	}
	changed := false
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	if pod.Labels[query.LabelRevisionHash] != targetHash {
		pod.Labels[query.LabelRevisionHash] = targetHash
		changed = true
	}
	current, hasCurrent := pod.Labels[query.LabelPairingProtocol]
	switch {
	case pairingProtocol != "" && current != pairingProtocol:
		pod.Labels[query.LabelPairingProtocol] = pairingProtocol
		changed = true
	case pairingProtocol == "" && hasCurrent:
		delete(pod.Labels, query.LabelPairingProtocol)
		changed = true
	}
	return changed
}

// applyMetadata applies a label or annotation diff in memory: a nil value
// deletes the key, a string sets it. Reports whether anything changed.
func applyMetadata(m *map[string]string, diff map[string]any) bool {
	if len(diff) == 0 {
		return false
	}
	if *m == nil {
		*m = make(map[string]string, len(diff))
	}
	for key, value := range diff {
		text, set := value.(string)
		if !set {
			delete(*m, key)
			continue
		}
		(*m)[key] = text
	}
	return true
}
