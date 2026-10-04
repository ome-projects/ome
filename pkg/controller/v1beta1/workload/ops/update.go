package ops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// updateDrainKey identifies an in-place or recreate Update writer
// against a specific Instance materialization. Same key on Add and
// Remove so an in-place flow ends with the gate flipping back True.
func updateDrainKey(idx int32, incarnation int64) string {
	return strconv.Itoa(int(idx)) + "-" + strconv.FormatInt(incarnation, 10)
}

// UpdateRequeueInterval is the wait between passes while an Update is
// in flight, from the operator's lifecycle.requeue.operation. Exported
// so the dispatcher's requeue cadence stays in lockstep with the
// per-Instance state machine. Zero means unconfigured: the caller
// requeues on the controller's rate-limited backoff instead.
func UpdateRequeueInterval(input workload.ReconcileInput) time.Duration {
	return input.Requeue.Operation
}

// surgeWindowApplies reports whether the minReadySeconds window still gates a
// surge: the Operation has not advanced past Step=Surge, so the source is in
// rotation and the replacement's Ready age decides when it may leave. Past
// that step the source is already draining, and a replacement that flaps
// Ready must not hold the drained source out of service for another window.
// drainAdmitted asks the adapter's DrainGate whether the source pods may
// leave rotation now that their replacement serves. A denial keeps the
// source at Step=Surge for this pass and records the hold; the next pass
// asks again. No gate means the drain proceeds.
func drainAdmitted(input workload.ReconcileInput, sourcePods []*corev1.Pod, target string) bool {
	if input.DrainGate == nil {
		return true
	}
	names := make([]string, 0, len(sourcePods))
	for _, pod := range sourcePods {
		if pod != nil && pod.DeletionTimestamp == nil {
			names = append(names, pod.Name)
		}
	}
	allowed, gate, reason := input.DrainGate(names)
	if allowed {
		return true
	}
	input.DrainHolds.Observe(workload.RolloutHold{Gate: gate, Reason: reason, Target: target})
	return false
}

func surgeWindowApplies(s *workload.InstanceStatus) bool {
	return s == nil || s.Operation == nil || s.Operation.Step == workload.UpdateStepSurge
}

// abandonedReplacementDeleteOptions bounds the termination grace of a
// replacement the rollout abandons before it ever carried the serving
// gate: it has never been in rotation and owes no in-flight work, so
// waiting out its own grace would only hold its surge slot, and with it
// the budget, against the revision that supersedes it. A pod that has
// carried the gate in either direction, and an unconfigured bound, keep
// the pod's own grace.
func abandonedReplacementDeleteOptions(input workload.ReconcileInput, pod *corev1.Pod) []client.DeleteOption {
	if input.AbandonedReplacementGrace <= 0 || podHasServingCondition(pod) {
		return nil
	}
	seconds := int64(math.Ceil(input.AbandonedReplacementGrace.Seconds()))
	return []client.DeleteOption{client.GracePeriodSeconds(seconds)}
}

// podHasServingCondition reports whether any writer has set the serving
// gate on the pod. The renderer only declares the gate on the spec; the
// condition appears the first time the rollout marks the pod serving or
// holds it out of rotation, so its absence proves the pod never served.
func podHasServingCondition(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == podreadiness.ConditionType {
			return true
		}
	}
	return false
}

// Update drives one Instance toward the target ControllerRevision.
// Mode is chosen per Instance:
//
//   - RecreatePod: always drain + delete + recreate at bumped
//     Incarnation.
//   - InPlaceIfPossible: image-patch when the diff is container images
//     only; otherwise fall through to recreate.
//   - InPlaceOnly: image-patch when eligible; error otherwise.
//
// Under either in-place strategy, a recreate already in flight on the
// Instance completes as a recreate whatever the diff against the target
// now says (recreateInFlight).
//
// done=true once Phase=Ready with RunningRevision=target.Name.
//
// Self-lists the Component's pods (cached) then filters to this Instance.
// The dispatcher's per-reconcile update pass instead calls UpdateWithPods
// after a single cached List + bucket, so a Component with N Instances
// costs one List per reconcile instead of N.
func Update(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Update: nil client")
	}
	pods, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component, true)
	if err != nil {
		return false, fmt.Errorf("Update: list pods (instance=%d): %w", inst.Index, err)
	}
	return UpdateWithPods(ctx, deps, input, plan, inst, target, targetSpec, filterPodsByInstance(pods, inst.Index))
}

// UpdateWithPods is Update with this Instance's pods supplied by the
// caller — the dispatcher does a single per-Component cached List + bucket
// once per reconcile and threads each Instance's slice here, instead of
// one cached List per Instance. instancePods must already be filtered to
// inst.Index. Semantics are identical to Update; only the read source
// differs.
func UpdateWithPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec, instancePods []*corev1.Pod) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("Update: nil client")
	}
	if target == nil || targetSpec == nil {
		return false, fmt.Errorf("Update: nil target revision or spec (instance=%d)", inst.Index)
	}

	pods := instancePods

	// Every update mode below, the surge machine included, tears pods down
	// and then waits for them to go away. On a dead node that wait never
	// ends, so clear the ones the force-delete policy proves are
	// unrecoverable before any mode runs.
	if err := escalateStuckTerminatingPods(ctx, deps, input, pods, inst.Index); err != nil {
		return false, fmt.Errorf("Update: force-delete stuck-Terminating pods (instance=%d): %w", inst.Index, err)
	}

	// UpdateStrategy is not part of the revision payload, so a strategy edit
	// retargets nothing — and every mode leaves state the others cannot see:
	// a patch already applied to a live pod, a second pod at the alternate
	// ordinal slot, a drain hold released only by the source's deletion.
	// An attempt therefore runs to its end on the mechanism it opened with,
	// and the edit reaches this Instance at its next admitted attempt.
	observed := input.ObservedState.Instance(inst.Index)
	plan.UpdateStrategy.Type = effectiveUpdateStrategy(observed, plan.UpdateStrategy.Type)

	// A surge-owned status resumes on the surge machine whatever the plan
	// says: it holds the ordinal slot and the source's drain hold, which no
	// other mode knows how to release.
	if isSurgeOwnedStatus(observed) {
		return surgeUpdate(ctx, deps, input, plan, inst, target, pods)
	}

	// Eligibility compares against the recorded running revision, NOT the
	// live pod. Live pods carry apiserver-defaulted fields and Render
	// overlays (hostname/subdomain/serving gate); byte-comparing those
	// against the freshly-rendered target would always declare ineligible.
	// The recorded revision's PodSpec is the pre-defaulted canonical form
	// the target hashes against.
	runningSpec, err := loadRunningRevisionPodSpec(ctx, deps.Reader(), input, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Update: load running revision (instance=%d): %w", inst.Index, err)
	}

	multiPod := inst.TotalPods() > 1
	// Strategy on the plan is the workload-mirror UpdateStrategyType;
	// the chooser compares against the workload constants directly. It is
	// the attempt's pinned strategy once one is in flight.
	strategy := plan.UpdateStrategy.Type
	var mode updateMode
	if inPlaceStrategy(strategy) && recreateInFlight(observed) {
		// An in-place strategy resolves its mechanism from the diff between
		// the running revision and the target, and a recreate it fell back
		// to leaves that diff stale: the pod set was rendered from the
		// revision the attempt opened for, so the running revision's PodSpec
		// describes none of the live pods and offers nothing to patch from.
		// The attempt completes as the recreate it is. A target that
		// moves meanwhile, back onto the running revision included, is
		// reached by tearing that set down and rebuilding it at the target,
		// which the recreate stamp does on a retarget.
		mode = updateModeRecreate
	} else {
		mode, err = chooseUpdateModeForInstance(strategy, runningSpec, targetSpec, multiPod)
		if err != nil {
			// InPlaceOnly + ineligible diff is the only error path here.
			workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonInPlaceUpdateNotPossible,
				"OMENative %s rejected update: %v",
				workload.InstanceKey(input.Key.Component, inst.Index), err)
			return false, fmt.Errorf("choose update mode (instance=%d): %w", inst.Index, err)
		}
	}

	// A surge keeps the serving source in rotation while its replacement
	// comes up. An Instance that was never promoted has no such source, and
	// a surge beside the pod set its disposed attempt left behind would only
	// add a second replacement; its re-drive rebuilds the set in place. The
	// recreate stamp pins that mechanism on the attempt.
	if mode == updateModeSurge && SurgeHasNoSource(observed) {
		mode = updateModeRecreate
		plan.UpdateStrategy.Type = workload.UpdateStrategyRecreatePod
	}

	// Status stamping is per-mode: recreate bumps Incarnation atomically
	// with the Updating transition; in-place must NOT bump (same
	// materialization); surge advances ActiveOrdinal (single-pod) or
	// promotes the surge index (gang) on completion.
	switch mode {
	case updateModeInPlace:
		return inPlaceUpdate(ctx, deps, input, plan, inst, target, targetSpec, pods)
	case updateModeRecreate:
		return recreateUpdate(ctx, deps, input, plan, inst, target, pods)
	case updateModeSurge:
		return surgeUpdate(ctx, deps, input, plan, inst, target, pods)
	}
	return false, fmt.Errorf("update mode unknown (instance=%d)", inst.Index)
}

// filterPodsByInstance drops pods whose ome.io/instance-index label
// doesn't match idx.
func filterPodsByInstance(pods []*corev1.Pod, idx int32) []*corev1.Pod {
	out := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if i, ok := query.InstanceIdxFromLabels(pod); ok && i == idx {
			out = append(out, pod)
		}
	}
	return out
}

// updateMode is the resolved per-Instance dispatch decision.
type updateMode int

const (
	updateModeRecreate updateMode = iota
	updateModeInPlace
	// updateModeSurge runs the SurgeThenDrain rollout — create a new
	// pod at the other ordinal slot, wait Ready, drain + delete the
	// old. Zero-downtime per Instance.
	updateModeSurge
)

// RunningPodSpecFunc loads the PodSpec recorded by an Instance's running
// revision on demand. nil with no error is no recorded baseline.
type RunningPodSpecFunc func() (*corev1.PodSpec, error)

// recreateInFlight reports whether the row carries a recreate attempt: an
// Update on its Drain step, which tears the pod set down and rebuilds it
// at a bumped Incarnation.
func recreateInFlight(s *workload.InstanceStatus) bool {
	return workload.StateOf(s) == workload.StateUpdateDrain
}

// inPlaceStrategy reports whether strategy is one of the two that patch
// pods where they run and resolve a recreate from the diff instead.
func inPlaceStrategy(strategy workload.UpdateStrategyType) bool {
	return strategy == workload.UpdateStrategyInPlaceIfPossible || strategy == workload.UpdateStrategyInPlaceOnly
}

// SurgeHasNoSource reports whether a fresh start on the row has nothing a
// surge could keep in rotation: the Instance was never promoted, so no pod
// of its is a serving source, and the attempt that built its pod set has
// been disposed. Such a start runs as a recreate under every strategy, so
// the update pass reads it here to consult the admission gate for a
// rebuild rather than a surge.
func SurgeHasNoSource(s *workload.InstanceStatus) bool {
	return s != nil && s.RunningRevision == "" && s.Phase == workload.InstancePhaseFailed && s.Operation == nil
}

// InPlaceFallsBackToRecreate reports whether an in-place strategy runs as
// a recreate on this Instance. A multi-pod Instance always rebuilds under
// either in-place variant: inPlaceEligible compares only the leader's
// PodSpec, so a worker-only image bump patched in place would leave the
// workers on the old image (split-brain). A single-pod InPlaceIfPossible
// start rebuilds when its diff against the running revision's PodSpec
// exceeds regular-container images, or when no baseline is recorded; the
// running PodSpec is loaded only for that case. InPlaceOnly's strict
// rejection of such a diff on a single-pod Instance is not a fallback:
// nothing starts. The update pass reads this ahead of the coordination
// gate consult, since a fallback recreate takes capacity out of rotation
// the way any drain-first start does.
func InPlaceFallsBackToRecreate(strategy workload.UpdateStrategyType, multiPod bool, target *corev1.PodSpec, running RunningPodSpecFunc) (bool, error) {
	switch strategy {
	case workload.UpdateStrategyInPlaceOnly:
		return multiPod, nil
	case workload.UpdateStrategyInPlaceIfPossible:
		if multiPod {
			return true, nil
		}
		runningSpec, err := running()
		if err != nil {
			return false, err
		}
		return !inPlaceEligible(runningSpec, target), nil
	}
	return false, nil
}

// chooseUpdateModeForInstance is chooseUpdateMode with the fallback rule
// layered on top: an in-place strategy routes to recreate wherever
// InPlaceFallsBackToRecreate says it rebuilds. SurgeThenDrain keeps
// updateModeSurge for gangs — surgeUpdate performs a per-gang index surge
// (a whole replacement gang at a fresh instance index, gang-scheduled via
// its own PodGroup, then the source gang is drained). RecreatePod is
// already recreate; multiPod has no additional effect on it.
func chooseUpdateModeForInstance(strategy workload.UpdateStrategyType, running, target *corev1.PodSpec, multiPod bool) (updateMode, error) {
	fallback, err := InPlaceFallsBackToRecreate(strategy, multiPod, target, func() (*corev1.PodSpec, error) { return running, nil })
	if err != nil {
		return 0, err
	}
	if fallback {
		return updateModeRecreate, nil
	}
	return chooseUpdateMode(strategy, running, target)
}

// chooseUpdateMode resolves strategy + eligibility into a mode. nil
// running spec is treated as ineligible, forcing recreate from a
// known baseline. InPlaceOnly + ineligible returns an error.
// Pod-count-agnostic; production callers route through
// chooseUpdateModeForInstance.
func chooseUpdateMode(strategy workload.UpdateStrategyType, running, target *corev1.PodSpec) (updateMode, error) {
	eligible := inPlaceEligible(running, target)
	switch strategy {
	case workload.UpdateStrategySurgeThenDrain:
		return updateModeSurge, nil
	case workload.UpdateStrategyRecreatePod:
		return updateModeRecreate, nil
	case workload.UpdateStrategyInPlaceOnly:
		if !eligible {
			return 0, fmt.Errorf("InPlaceOnly strategy but diff exceeds container images")
		}
		return updateModeInPlace, nil
	case workload.UpdateStrategyInPlaceIfPossible:
		if eligible {
			return updateModeInPlace, nil
		}
		return updateModeRecreate, nil
	case "":
		// An unset type runs as SurgeThenDrain, the same reading
		// BuildPlan applies.
		return updateModeSurge, nil
	default:
		return 0, fmt.Errorf("unknown UpdateStrategy.Type %q", strategy)
	}
}

// inPlaceEligible reports whether the diff is regular-container images
// only. Both sides are stripped of regular-container images then
// byte-compared; init images are preserved so an init-image diff routes
// to recreate (kubelet can't re-run init containers after an image patch).
// nil on either side returns false — can't prove image-only.
func inPlaceEligible(running, target *corev1.PodSpec) bool {
	if running == nil || target == nil {
		return false
	}
	runningStripped, err := podSpecWithoutImages(running)
	if err != nil {
		return false
	}
	targetStripped, err := podSpecWithoutImages(target)
	if err != nil {
		return false
	}
	return string(runningStripped) == string(targetStripped)
}

// loadRunningRevisionPodSpec returns the PodSpec from the CR named by
// InstanceStatus.RunningRevision. Returns (nil, nil) when no status
// exists, no RunningRevision is recorded, or the CR is gone — callers
// treat that as no baseline and route to recreate.
func loadRunningRevisionPodSpec(ctx context.Context, reads client.Reader, input workload.ReconcileInput, idx int32) (*corev1.PodSpec, error) {
	payload, err := loadRunningRevisionPayload(ctx, reads, input, idx)
	if err != nil || payload == nil {
		return nil, err
	}
	return payload.PodSpec, nil
}

// loadRunningRevisionPayload returns the full revision.DataPayload
// (PodSpec + PodMeta + WorkerPodSpec). Used by inPlaceUpdate's
// annotation-reconciliation pass.
func loadRunningRevisionPayload(ctx context.Context, reads client.Reader, input workload.ReconcileInput, idx int32) (*revision.DataPayload, error) {
	s := input.ObservedState.Instance(idx)
	if s == nil || s.RunningRevision == "" {
		return nil, nil
	}
	return loadRevisionPayload(ctx, reads, input.Key.Namespace, s.RunningRevision)
}

// RevisionPodSpec returns the PodSpec the named ControllerRevision records,
// read through reads: the baseline a single-pod in-place start's diff is
// judged against. (nil, nil) when the revision is gone — no baseline,
// which the mode choice reads as a recreate.
func RevisionPodSpec(ctx context.Context, reads client.Reader, namespace, name string) (*corev1.PodSpec, error) {
	payload, err := loadRevisionPayload(ctx, reads, namespace, name)
	if err != nil || payload == nil {
		return nil, err
	}
	return payload.PodSpec, nil
}

// loadRevisionPayload reads the named ControllerRevision through reads and
// decodes its payload. (nil, nil) when the revision is gone.
func loadRevisionPayload(ctx context.Context, reads client.Reader, namespace, name string) (*revision.DataPayload, error) {
	cr := &appsv1.ControllerRevision{}
	if err := reads.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get running CR %s: %w", name, err)
	}
	var payload revision.DataPayload
	if err := json.Unmarshal(cr.Data.Raw, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal running CR %s data: %w", name, err)
	}
	return &payload, nil
}

// loadControllerRevisionPayload unmarshals a CR's raw data into a
// revision.DataPayload. Returns (nil, nil) when the CR is nil or its
// data is empty (benign mid-write edge case).
func loadControllerRevisionPayload(cr *appsv1.ControllerRevision) (*revision.DataPayload, error) {
	if cr == nil || len(cr.Data.Raw) == 0 {
		return nil, nil
	}
	var payload revision.DataPayload
	if err := json.Unmarshal(cr.Data.Raw, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal CR %s data: %w", cr.Name, err)
	}
	return &payload, nil
}

// podSpecWithoutImages returns the JSON form with regular-container
// images stripped; init-container images stay so an init-image-only diff
// fails inPlaceEligible and routes to recreate.
func podSpecWithoutImages(spec *corev1.PodSpec) ([]byte, error) {
	clone := spec.DeepCopy()
	for i := range clone.Containers {
		clone.Containers[i].Image = ""
	}
	return json.Marshal(clone)
}

// DetectUpdateTrigger fires when the Instance is mid-update, or when
// Phase=Ready and a pod's PodSpec hashes to a non-target revision.
// Creating / Deleting / Restarting / Migrating are not interruptible.
// The Migrate-owned suppression is explicit because a just-promoted
// surge briefly carries RunningRevision=sourceRev while the post-
// promote scale-down pass runs — without the guard the spec-bump that
// triggered the migration would race the surge promote.
//
// Exported for the workload reconcile loop (workload/reconcile.go)
// which iterates Instances and consults this before invoking Update.
//
// Self-lists only on the slow path (empty RunningRevision). The
// reconcile loop's per-Instance update pass calls DetectUpdateTriggerWithPods
// with the Instance's pods from a single per-Component cached List +
// bucket, so the rare per-pod fallback doesn't cost one List per Instance.
//
// retryAfter > 0 means the trigger was denied by a not-yet-due Backoff
// RetryBlock for the current target; re-evaluate then. 0 otherwise
// (including Held, which has no time bound).
func DetectUpdateTrigger(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision) (trigger bool, retryAfter time.Duration, err error) {
	return DetectUpdateTriggerWithPods(ctx, deps, input, plan, inst, target, nil)
}

// DetectUpdateTriggerWithPods is DetectUpdateTrigger with this Instance's
// pods supplied by the caller for the empty-RunningRevision fallback path.
// instancePods must already be filtered to inst.Index. A nil slice means
// "not pre-listed"; the function then self-lists on the fallback path,
// preserving the standalone DetectUpdateTrigger contract. Semantics are
// otherwise identical.
//
// Composition of the pure evaluation (EvaluateUpdateTrigger) with its
// two effects: the fallback self-list and the AdoptRevision backfill
// write (BackfillRunningRevision).
func DetectUpdateTriggerWithPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) (trigger bool, retryAfter time.Duration, err error) {
	dec, needPods := evaluateUpdateTriggerFast(input, inst, target)
	if needPods {
		pods := instancePods
		if pods == nil {
			all, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component, true)
			if err != nil {
				return false, 0, err
			}
			pods = filterPodsByInstance(all, inst.Index)
		}
		dec = evaluateUpdateTriggerPods(pods, target)
	}
	if dec.AdoptRevision {
		if err := BackfillRunningRevision(ctx, input, inst.Index, target.Name); err != nil {
			return false, 0, err
		}
		return false, 0, nil
	}
	return dec.Trigger, dec.RetryAfter, nil
}

// UpdateTriggerDecision is the pure outcome of the update-trigger
// evaluation for one Instance.
type UpdateTriggerDecision struct {
	// Trigger: the Instance needs the Update op this pass.
	Trigger bool
	// RetryAfter > 0: fresh re-triggering was denied by a not-yet-due
	// Backoff RetryBlock for the current target; re-evaluate then. 0
	// otherwise (including Held, which has no time bound).
	RetryAfter time.Duration
	// RetryBlockDenied: fresh re-triggering was denied by the current
	// target's RetryBlock — Held, an attempt already in flight, or a
	// Backoff not yet due (RetryAfter carries its wake-up).
	RetryBlockDenied bool
	// AdoptRevision: RunningRevision is empty but the Instance's
	// runtime-ready pods already match the target — stamp
	// Ready-on-target (BackfillRunningRevision) so future evaluations
	// take the fast path. The stamp is an EFFECT; the evaluation only
	// selects it.
	AdoptRevision bool
}

// EvaluateUpdateTrigger is the pure update-trigger evaluation: no
// client reads, no status writes. instancePods must already be
// filtered to inst.Index (nil is an empty pod set); they are consulted
// only on the empty-RunningRevision fallback path. The clock is read
// through input.Now.
func EvaluateUpdateTrigger(input workload.ReconcileInput, inst workload.InstancePlan, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) UpdateTriggerDecision {
	dec, needPods := evaluateUpdateTriggerFast(input, inst, target)
	if !needPods {
		return dec
	}
	return evaluateUpdateTriggerPods(instancePods, target)
}

// evaluateUpdateTriggerFast runs the status-only portion of the
// update-trigger evaluation. needPods=true means the fast paths did not
// decide (empty RunningRevision) and the caller must run
// evaluateUpdateTriggerPods over the Instance's pods.
func evaluateUpdateTriggerFast(input workload.ReconcileInput, inst workload.InstancePlan, target *appsv1.ControllerRevision) (dec UpdateTriggerDecision, needPods bool) {
	s := input.ObservedState.Instance(inst.Index)
	if s == nil {
		return UpdateTriggerDecision{}, false
	}
	if isMigrateOwnedStatus(s) {
		return UpdateTriggerDecision{}, false
	}
	// A gang surge-target MARKER is the replacement gang's index, created and
	// driven by its SOURCE instance's gangSurgeUpdate — not an independent
	// update target. Triggering it here mis-drives it as a fresh surge source
	// (allocating a phantom surge index), which corrupts the rollout when a
	// corrective edit moves the target while the marker still runs the old
	// surge revision. Skip; the source owns the marker's lifecycle.
	if isGangSurgeTargetMarker(s) {
		return UpdateTriggerDecision{}, false
	}
	if s.Phase == workload.InstancePhaseUpdating {
		return UpdateTriggerDecision{Trigger: true}, false
	}
	// A Failed Instance must still re-trigger toward a NEW target. Once a
	// rollout escalates an Instance to Phase=Failed (e.g. a bad image →
	// ImagePullBackOff), a corrective revision (fixed runtime or ISVC edit)
	// would otherwise never roll — the Instance stays wedged on the old
	// revision forever. Treat Failed like Ready for the revision comparison so a
	// changed target re-drives it (the reconciler's budget path lets a
	// mid-operation Failed Instance continue rather than start fresh).
	if s.Phase != workload.InstancePhaseReady && s.Phase != workload.InstancePhaseFailed {
		return UpdateTriggerDecision{}, false
	}

	// Same-target retry gate: a persisted RetryBlock for the CURRENT
	// target denies FRESH re-triggering. A different target revision is
	// a different RetrySubject and passes. A continuation (the teardown
	// or abandon of a failed candidate) is exempt, by the same predicate
	// the dispatcher admits and charges with.
	if b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name); b != nil && !workload.UpdateContinuation(s) {
		denied, retryAfter := evaluateRetryBlockGate(b, input.Now(),
			anyInFlightUpdateAt(input.ObservedState.InstanceStatuses, target.Name))
		if denied {
			return UpdateTriggerDecision{RetryAfter: retryAfter, RetryBlockDenied: true}, false
		}
	}

	// RunningRevision is the cheap fast-path comparison.
	if s.RunningRevision != "" && s.RunningRevision != target.Name {
		return UpdateTriggerDecision{Trigger: true}, false
	}
	if s.RunningRevision == target.Name {
		return UpdateTriggerDecision{}, false
	}
	// Empty RunningRevision — fall back to the per-pod diff.
	return UpdateTriggerDecision{}, true
}

// evaluateUpdateTriggerPods runs the empty-RunningRevision fallback:
// the per-pod revision check against the target, and the AdoptRevision
// selection.
//
// A pod's revision is its ome.io/revision-hash label — the identity
// every other pass reads, and the only one that can be right here. The
// renderer stamps hostname, subdomain and the serving readiness gate
// onto each pod it writes, so a live pod's PodSpec never equals the
// unrendered desired template and a spec comparison can only ever say
// "different". A pod carrying no label cannot be proven on target and
// is rolled.
//
// Exactly one shape is adopted: every pod on the target revision and
// runtime-ready. Every other shape takes the roll, so a Failed row keeps
// the recovery its strategy and disposition give it.
func evaluateUpdateTriggerPods(pods []*corev1.Pod, target *appsv1.ControllerRevision) UpdateTriggerDecision {
	// An empty pod set proves nothing in either direction; the Create
	// and demotion passes own a row with no pods.
	if len(pods) == 0 {
		return UpdateTriggerDecision{}
	}
	if !existingPodsMatchTargetRevision(pods, target) {
		return UpdateTriggerDecision{Trigger: true}
	}
	// Carrying the target revision proves nothing about health: a wedged
	// pod (e.g. ImagePullBackOff) is labelled with the very revision that
	// broke it, and the adoption stamp prunes that revision's RetryBlock —
	// Ready is only stamped on proof. An unhealthy pod set on the target is
	// a row to recover, not a row to stamp: it takes the ordinary roll, which
	// is the retry, recreate or relocation its strategy and the disposition's
	// node-exclusion overlay resolve. Leaving it alone would end the row's
	// recovery — no further attempt is opened, so the RetryBlock ladder never
	// advances and a stuck pod is never recreated off its suspect node.
	// Adoption is the narrow case: every pod on the target AND runtime-ready.
	// It backfills the revision an already running pod set carries, so it is
	// not one of the promote paths and does not consult the availability
	// window.
	if !query.AllPodsRuntimeReady(pods) {
		return UpdateTriggerDecision{Trigger: true}
	}
	return UpdateTriggerDecision{AdoptRevision: true}
}

// BackfillRunningRevision stamps the Instance Ready on rev
// (Phase=Ready, RunningRevision=rev, Operation cleared) — the
// AdoptRevision effect, so future trigger evaluations take the
// RunningRevision fast path.
func BackfillRunningRevision(ctx context.Context, input workload.ReconcileInput, idx int32, rev string) error {
	if err := status.StampReadyOnRevision(ctx, input, idx, rev); err != nil {
		return fmt.Errorf("backfill RunningRevision (instance=%d): %w", idx, err)
	}
	return nil
}

// isMigrateOwnedStatus reports whether the migration record has a hold
// on the row: the Migrating pin on the phase, or a Migrate operation
// whatever the phase — a pair row that escalated keeps its claim while
// it reads Failed. No trigger starts an Update or a Restart on such a
// row; the record ends both pair rows through itself.
func isMigrateOwnedStatus(s *workload.InstanceStatus) bool {
	return s != nil && (s.Phase == workload.InstancePhaseMigrating ||
		workload.ClaimOf(s) == workload.OwnerMigrate)
}

// surgeClaim reports whether the row's operation is a surge cycle's.
//
// Read from the operation, not from the ownership table: a surge that
// escalated keeps its claim — the pinned revision, the replacement index
// — while the row reads Failed, and the table answers OwnerNone for
// every Failed row. Who drives the row and what the row still claims are
// different questions, and the recovery paths ask the second one.
func surgeClaim(s *workload.InstanceStatus) bool {
	return s != nil && s.Operation != nil &&
		s.Operation.Type == workload.InstanceOperationUpdate &&
		status.SurgeUpdateStep(s.Operation.Step)
}

// gangSurgeSourceClaim is surgeClaim plus the replacement index a gang
// source pins for the duration of its cycle.
func gangSurgeSourceClaim(s *workload.InstanceStatus) bool {
	return surgeClaim(s) && s.Operation.SurgeIndex != nil
}

// isSurgeOwnedStatus reports whether the SurgeThenDrain sub-machine is
// running on this row, so a mode resolved from a since-edited strategy must
// not be dispatched over it.
//
// Two halves. Ownership: the update pass drives the row at all — which a
// Failed row is not, since a failed surge has already escalated to operator
// attention and editing the strategy is one of the levers used to rescue it.
// Mechanism: of the update pass's sub-machines, the one in flight is the
// surge cycle.
func isSurgeOwnedStatus(s *workload.InstanceStatus) bool {
	switch workload.StateOf(s) {
	case workload.StateUpdateSurge, workload.StateUpdateSurgeDrain:
		return true
	}
	return false
}

// effectiveUpdateStrategy returns the strategy an attempt runs under: the
// one pinned on the operation in flight, or the desired one when no attempt
// owns the Instance. A Failed row holds no attempt — editing the strategy is
// the operator's way out of a mode that cannot make progress — so its
// preserved operation does not pin anything.
func effectiveUpdateStrategy(s *workload.InstanceStatus, desired workload.UpdateStrategyType) workload.UpdateStrategyType {
	if workload.Owner(s) != workload.OwnerUpdate || s.Operation.Strategy == "" {
		return desired
	}
	return s.Operation.Strategy
}

// isGangSurgeTargetMarker reports whether s is a gang surge-target marker —
// the transient replacement-gang index stamped by status.StampGangSurgeTarget
// (Op{Update, Step=GangSurgeTarget}). Its lifecycle is owned by the source
// instance's gangSurgeUpdate, so the update trigger must not treat it as an
// independent target.
//
// Read from the operation and not from the ownership table: a marker whose
// gang escalated keeps the claim on its index while the row reads Failed,
// and the table answers OwnerNone for every Failed row. The claim is what
// this asks about, not who drives the row.
func isGangSurgeTargetMarker(s *workload.InstanceStatus) bool {
	return s != nil && s.Operation != nil &&
		s.Operation.Type == workload.InstanceOperationUpdate &&
		(s.Operation.Step == workload.UpdateStepGangSurgeTarget ||
			s.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup)
}

// RetryBlockDenyingFreshStart returns the target's RetryBlock when it
// denies a fresh start this pass — a Held ladder, an attempt already in
// flight under RetryInProgress, or a Backoff not yet due — and nil when
// the target carries no block or one that admits a start. It is the
// trigger gate's own reading (evaluateRetryBlockGate), so a caller that
// asks about the Component as a whole, rather than one Instance that
// reached the trigger, agrees with the trigger's per-Instance verdict.
func RetryBlockDenyingFreshStart(input workload.ReconcileInput, target *appsv1.ControllerRevision) *workload.RetryBlock {
	if target == nil {
		return nil
	}
	b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name)
	if b == nil {
		return nil
	}
	if denied, _ := evaluateRetryBlockGate(b, input.Now(), anyInFlightUpdateAt(input.ObservedState.InstanceStatuses, target.Name)); !denied {
		return nil
	}
	return b
}

// anyInFlightUpdateAt reports whether any Instance carries an in-flight
// Update Operation targeting rev. Used by the retry gate to distinguish
// a live RetryInProgress authorization from one leaked by a superseded
// or interrupted attempt.
func anyInFlightUpdateAt(statuses []workload.InstanceStatus, rev string) bool {
	for i := range statuses {
		op := statuses[i].Operation
		if op != nil && op.Type == workload.InstanceOperationUpdate && op.TargetRevision == rev {
			return true
		}
	}
	return false
}

// ProvenWindowSeconds is how long, in whole seconds, a promoted pod that
// restarted since its Instance entered Ready must hold Ready again
// before the roll reads it as serving: the Component's minReadySeconds
// floored by the stuck-pod grace, the span the workload already grants
// a pod to prove it is not crash-looping. With neither configured, Ready
// is Available, as it is for a pod that never restarted.
func ProvenWindowSeconds(minReadySeconds int32, stuckPodGrace time.Duration) int32 {
	grace := math.Ceil(stuckPodGrace.Seconds())
	if grace > math.MaxInt32 {
		grace = math.MaxInt32
	}
	return max(minReadySeconds, int32(grace))
}

// RebuiltAfterFailure reports whether pod is the rebuild answering a death
// of the set the roll is landing: the Instance's recorded failure names a
// container of the set that died or wedged, it is younger than the
// revision being rolled, and pod was created no earlier than it and
// inside the proven window of it. The failure outlives the pod that
// failed, so such a pod is the set's comeback, and the roll reads it as
// serving only once it has held Ready for the window since its readiness
// moved, exactly as a pod the kubelet restarted in place. The window is
// measured from the recorded failure, so every rebuild created inside it
// is bounded by it, however many answer the same record.
//
// A failure older than the revision being rolled belongs to a set of a
// superseded revision, so a push that corrects a crashing revision rolls
// its fresh sets unheld; a revision whose creation is not known cannot
// be placed and does not exclude. A record with no container names a
// wait the pod never served through, not a death of the set. A pod
// created with no such failure recorded, or past the window of one, is a
// fresh start, read by its serving state alone.
func RebuiltAfterFailure(pod *corev1.Pod, failure *workload.InstanceTermination, revisionBorn time.Time, window int32) bool {
	if pod == nil || failure == nil || failure.Time.IsZero() || failure.ContainerName == "" ||
		pod.CreationTimestamp.IsZero() || window <= 0 {
		return false
	}
	failedAt := failure.Time.Time
	if !revisionBorn.IsZero() && !failedAt.After(revisionBorn) {
		return false
	}
	created := pod.CreationTimestamp.Time
	return !created.Before(failedAt) && created.Before(failedAt.Add(time.Duration(window)*time.Second))
}

// RolledInstanceNotServing reports whether row is an Instance the roll
// already moved onto target that does not serve it: its pod set on that
// revision is short of fully serving, or its runner container restarted
// since the row entered Ready and has not served since. Nothing is left
// to retry for such an Instance and nothing but a repair or a corrected
// revision changes it, so it is the roll's open work: the update pass
// counts it against the budget of the strategy's arm ahead of any fresh
// start, and under SurgeThenDrain keeps every further source in
// rotation while the count exceeds the unavailability budget. A row with
// an attempt in flight is anchored by the in-flight counts instead, and
// a row a migration owns is the record's.
//
// A restart after Ready holds the slot while the container is down, and
// while it is Ready again but has not held Ready for window seconds:
// the Component's minReadySeconds floored by the stuck-pod grace
// (ProvenWindowSeconds). A comeback shorter than the span the workload
// grants a pod to prove it is not crash-looping proves nothing, so the
// set stays unproven and the roll starts nothing further on its
// account. The kubelet moves the Ready condition's transition time when
// a pod leaves and re-enters Ready across a restart, so Available on
// the pod (podreadiness.IsPodAvailable) reads "Ready again and held it
// for the window" off that clock. The duration is how long until a pod
// Ready again clears the window, zero when only a watch event changes
// the verdict. A rebuild is the set's restart as much as a restart in
// place: the failure that opened it outlives the pod that failed, on
// the row's LastFailure, and a pod created inside the window after a
// death of the set on the revision being rolled is the rebuild answering
// it (RebuiltAfterFailure), unproven until it has held Ready for the
// window since its readiness moved. The hold belongs to sets of the
// revision being rolled alone: a failure of the set the Instance ran on
// a superseded revision never holds the new roll, so a corrected push
// lands as a fresh roll. A pod that never restarted since Ready and
// answers no such failure is read off its serving state alone:
// minReadySeconds already paced its promotion. A container that has
// restarted more than once since Ready (RunnerRestartedAgainSinceReady)
// keeps restarting: it is up only between its crashes, so it keeps the
// slot through those windows whatever their length, and the roll never
// creeps forward on them. The reason names what holds the slot, for the
// pass to log; it is empty when nothing does.
func RolledInstanceNotServing(row *workload.InstanceStatus, target string, revisionBorn time.Time, pods []*corev1.Pod, desired, window int32, now time.Time) (bool, time.Duration, string) {
	if row == nil || target == "" || row.RunningRevision != target {
		return false, 0, ""
	}
	if workload.UpdateContinuation(row) || isMigrateOwnedStatus(row) {
		return false, 0, ""
	}
	targetRev := query.RevisionFromName(target)
	own := make([]*corev1.Pod, 0, len(pods))
	held, wait, reason := false, time.Duration(0), ""
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		if podRev := query.RevisionFromPod(pod); !podRev.IsZero() && !podRev.Same(targetRev) {
			continue
		}
		comeback := ""
		if RebuiltAfterFailure(pod, row.LastFailure, revisionBorn, window) {
			comeback = fmt.Sprintf("pod %s rebuilt after the failure recorded at %s", pod.Name, row.LastFailure.Time.UTC().Format(time.RFC3339))
		}
		if _, restarted := RunnerRestartedSinceReady(pod, row.ReadySince); restarted {
			if RunnerRestartedAgainSinceReady(pod, row.ReadySince) {
				return true, 0, fmt.Sprintf("pod %s restarted again since Ready", pod.Name)
			}
			comeback = fmt.Sprintf("pod %s restarted since Ready", pod.Name)
		}
		if comeback != "" {
			if available, remaining := podreadiness.IsPodAvailable(pod, window, now); !available {
				held = true
				wait = max(wait, remaining)
				reason = fmt.Sprintf("%s, Ready again for less than the %ds window (%s left)", comeback, window, remaining.Round(time.Second))
			}
		}
		own = append(own, pod)
	}
	if held {
		return true, wait, reason
	}
	if !query.PodSetFullyServing(own, desired) {
		return true, 0, fmt.Sprintf("pod set not fully serving (%d of %d pods on the revision)", len(own), desired)
	}
	return false, 0, ""
}

// Wreckage is per-instance rollout debris keyed to a SUPERSEDED
// revision: state that no revision-diff update trigger can ever reach,
// because the trigger's predicate is "this instance must move to a
// different revision" while the wreckage sits at zero revision distance
// (a corrective roll-back) or on a third-party revision. Two shapes:
//
//   - GANG: a Failed source still carrying its gang-surge continuation
//     (Op{Update, SurgeIndex}) toward a revision that is no longer the
//     roll target. When the corrective target equals the source's
//     RunningRevision the trigger never fires, so the abandon
//     continuation must be dispatched explicitly.
//   - ALIEN PODS: live pods whose revision-hash label matches neither
//     the instance's RunningRevision nor the current roll target — the
//     dead pod an exhausted attempt toward a superseded revision left
//     behind (e.g. parked at the surge ordinal).
//
// Cleanup restores the invariant that nothing keyed to a superseded
// revision gates, steers, or occupies reconciliation.

// EvaluateWreckage is the pure wreckage predicate for one Instance.
// Plan consults it only for instances the update trigger declined
// (in-flight and re-triggered instances clean their own debris through
// the update machinery). instancePods must already be filtered to the
// instance's index; the clock is not read.
//
// Excluded by design: migrate-owned statuses (the migration record owns
// them), gang surge-target markers (a live marker is owned by its
// source; an orphaned one is collected by the plan's marker-liveness
// scale-down), and transient phases (Creating / Deleting / Restarting
// are not interruptible).
func EvaluateWreckage(s *workload.InstanceStatus, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) bool {
	if s == nil || target == nil {
		return false
	}
	if isMigrateOwnedStatus(s) || isGangSurgeTargetMarker(s) {
		return false
	}
	if s.Phase != workload.InstancePhaseReady && s.Phase != workload.InstancePhaseFailed {
		return false
	}
	if failedGangSurgeContinuation(s, target.Name) {
		return true
	}
	return len(alienRevisionPods(s, target.Name, instancePods)) > 0
}

// failedGangSurgeContinuation reports the gang wreckage shape: a Failed
// source whose preserved gang-surge operation targets a revision other
// than the current roll target.
func failedGangSurgeContinuation(s *workload.InstanceStatus, targetName string) bool {
	return s.Phase == workload.InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
		s.Operation.SurgeIndex != nil &&
		s.Operation.TargetRevision != targetName
}

// alienRevisionPods returns the instance's live pods labeled with a
// revision that matches neither the instance's RunningRevision nor the
// current roll target. Unlabeled pods are never selected (legacy pods
// are the ordinal partition's business). An empty RunningRevision
// yields no aliens: with no recorded baseline, alienness is
// undecidable — that state is owned by the trigger's per-pod diff /
// adoption path, and deleting an unproven-but-innocent pod there would
// destroy the very pod adoption is waiting on.
func alienRevisionPods(s *workload.InstanceStatus, targetName string, pods []*corev1.Pod) []*corev1.Pod {
	if s.RunningRevision == "" {
		return nil
	}
	running := query.RevisionFromName(s.RunningRevision)
	target := query.RevisionFromName(targetName)
	var out []*corev1.Pod
	for _, pod := range pods {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		hash, ok := pod.Labels[query.LabelRevisionHash]
		if !ok || hash == "" {
			continue
		}
		rev := query.RevisionFromPod(pod)
		if rev.Same(running) || rev.Same(target) {
			continue
		}
		out = append(out, pod)
	}
	return out
}

// CleanupWreckage abandons one instance's superseded-revision wreckage
// toward the CURRENT desired state — legal when target equals the
// instance's RunningRevision (the corrective roll-back), where the
// update machinery is unreachable. Effects route through the existing
// machinery:
//
//   - gang continuation → abandonFailedGangSurge (deletes the dead
//     replacement gang, drops its marker, records the failure on the
//     superseded revision's RetryBlock, resets the source Ready on its
//     running revision);
//   - alien-revision pods → taken out of rotation, then deleted on a later
//     pass, the same eviction the superseded-surge redirect applies to a
//     not-yet-promoted surge pod.
//
// The serving source is never touched: no serving-gate flip, no status
// stamp — after the debris is gone the Create pass re-proves readiness
// and promotes. done=true means no wreckage remains for this instance.
func CleanupWreckage(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, instancePods []*corev1.Pod) (bool, error) {
	if deps.Client == nil {
		return false, fmt.Errorf("CleanupWreckage: nil client")
	}
	if target == nil {
		return true, nil
	}
	s := input.ObservedState.Instance(inst.Index)
	if s == nil {
		return true, nil
	}

	if failedGangSurgeContinuation(s, target.Name) {
		return abandonFailedGangSurge(ctx, deps, input, plan, inst.Index, *s.Operation.SurgeIndex,
			s.RunningRevision, s.Operation.TargetRevision,
			instanceFailureReason(s, "gang surge abandoned after a corrective edit"), instanceFailureCause(s))
	}

	aliens := alienRevisionPods(s, target.Name, instancePods)
	if len(aliens) == 0 {
		return true, nil
	}
	return deleteSupersededRevisionPods(ctx, deps, input, inst.Index, aliens, target.Name)
}

// deleteSupersededRevisionPods evicts an Instance's pods keyed to a revision
// it is not converging to — the wreckage sweep's aliens, and the pods a
// retired Create attempt still holds the names of. A pod that is still
// routed is taken out of rotation and left for the next pass: deleting it in
// the same pass gives the endpoint controllers no window to act on the
// withdrawal, and a bare delete drops the connections it is carrying.
// Deletes are counted on the expectations cache before they are issued, and
// compensated when the apiserver refuses, so the next pass reads a pod set
// it can trust.
//
// done=false whenever there is still work here: outstanding expectations,
// a pod just unrouted, or pods deleted that the next pass has to re-read.
func deleteSupersededRevisionPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, pods []*corev1.Pod, targetName string) (bool, error) {
	cache := deps.ExpectationsCache()
	ns, owner, component := input.Key.Namespace, input.Key.OwnerName, input.Key.Component
	if !cache.Satisfied(ns, owner, component, idx) {
		return false, nil
	}
	unrouted := false
	for _, pod := range pods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod,
			podreadiness.WriterDeleteDrain, deleteDrainKey(idx)); err != nil {
			// A pod that has vanished, or come back under a new UID, is out
			// of rotation by other means already; the delete resolves it.
			if !apierrors.IsNotFound(err) && !errors.Is(err, podreadiness.ErrPodIdentityChanged) {
				return false, fmt.Errorf("unroute superseded-revision pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
			continue
		}
		unrouted = true
	}
	// Deleting in the pass that unrouted gives the endpoint controllers no
	// window to act on the withdrawal, so a pod that was still carrying
	// traffic goes on the next pass. One that was already out of rotation
	// has nothing to wait for and goes now.
	if unrouted {
		return false, nil
	}
	deleted := 0
	for _, pod := range pods {
		cache.ExpectDeletes(ns, owner, component, idx, 1)
		if err := deps.Client.Delete(ctx, pod); err != nil {
			cache.ObservedDelete(ns, owner, component, idx)
			if apierrors.IsNotFound(err) {
				continue
			}
			return false, fmt.Errorf("delete superseded-revision pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		deleted++
	}
	if deleted > 0 {
		workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonSupersededWreckageCleaned,
			"OMENative %s deleted %d superseded-revision pod(s); current target %s",
			workload.InstanceKey(component, idx), deleted, targetName)
	}
	return false, nil
}
