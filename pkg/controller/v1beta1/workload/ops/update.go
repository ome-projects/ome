package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// updateDrainKey identifies an in-place or recreate Update writer
// against a specific Instance materialization. Same key on Add and
// Remove so an in-place flow ends with the gate flipping back True.
func updateDrainKey(idx int32, incarnation int64) string {
	return strconv.Itoa(int(idx)) + "-" + strconv.FormatInt(incarnation, 10)
}

// drainServiceForPod is declared in migrate.go — Update and Migrate
// share the same per-revision routed Service lookup.

// UpdateRequeueInterval is the wait between passes while an Update is
// in flight. Exported so the dispatcher's requeue cadence stays in
// lockstep with the per-Instance state machine.
const UpdateRequeueInterval = 5 * time.Second

// podsAvailable reports whether every pod has been Ready for the
// Component's minReadySeconds window (podreadiness.IsPodAvailable). A zero
// window reduces to every pod being PodReady. Rollouts pace on this rather
// than on Ready: a drain or promotion that ran the moment a pod flipped
// Ready would release its budget slot before the pod proved it stays up.
// The in-flight Update keeps the dispatcher polling at UpdateRequeueInterval
// until the window elapses.
func podsAvailable(pods []*corev1.Pod, minReadySeconds int32, now time.Time) bool {
	for _, pod := range pods {
		if available, _ := podreadiness.IsPodAvailable(pod, minReadySeconds, now); !available {
			return false
		}
	}
	return true
}

// surgeWindowApplies reports whether the minReadySeconds window still gates a
// surge: the Operation has not advanced past Step=Surge, so the source is in
// rotation and the replacement's Ready age decides when it may leave. Past
// that step the source is already draining, and a replacement that flaps
// Ready must not hold the drained source out of service for another window.
func surgeWindowApplies(s *workload.InstanceStatus) bool {
	return s == nil || s.Operation == nil || s.Operation.Step == updateStepSurge
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
	// retargets nothing: the same roll continues under a different mechanism,
	// and the mode resolved below can differ from the one that opened the
	// Operation. Only SurgeThenDrain leaves state another mode cannot see —
	// a second pod at the alternate ordinal slot, the ActiveOrdinal advance
	// that ends the cycle, and a drain hold on the source released only by
	// the source's deletion. Dispatching another mode over that drains or
	// deletes the source while its replacement is still coming up, and
	// strands the surge pod. Keep the surge machine in control; it decides
	// whether to unwind (still uncommitted) or finish the cycle.
	if isSurgeOwnedStatus(findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)) {
		return surgeUpdate(ctx, deps, input, plan, inst, target, pods)
	}

	// Eligibility compares against the recorded running revision, NOT the
	// live pod. Live pods carry apiserver-defaulted fields and Render
	// overlays (hostname/subdomain/serving gate); byte-comparing those
	// against the freshly-rendered target would always declare ineligible.
	// The recorded revision's PodSpec is the pre-defaulted canonical form
	// the target hashes against.
	runningSpec, err := loadRunningRevisionPodSpec(ctx, deps.Reader(), input, input.Key.Component, inst.Index)
	if err != nil {
		return false, fmt.Errorf("Update: load running revision (instance=%d): %w", inst.Index, err)
	}

	multiPod := inst.TotalPods() > 1
	// Strategy on the plan is the workload-mirror UpdateStrategyType;
	// the chooser compares against the workload constants directly.
	strategy := plan.UpdateStrategy.Type
	mode, err := chooseUpdateModeForInstance(strategy, runningSpec, targetSpec, multiPod)
	if err != nil {
		// InPlaceOnly + ineligible diff is the only error path here.
		recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonInPlaceUpdateNotPossible,
			"OMENative %s rejected update: %v",
			instanceKey(input.Key.Component, inst.Index), err)
		return false, fmt.Errorf("choose update mode (instance=%d): %w", inst.Index, err)
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

// chooseUpdateModeForInstance is chooseUpdateMode with the multi-pod
// adjustment layered on top.
//
// Multi-pod adjustment:
//   - In-place modes → recreate: inPlaceEligible compares only the
//     leader's PodSpec, so a worker-only image bump would produce an
//     in-place rollout that patches only the leader, leaving workers
//     on the old image (split-brain).
//
// SurgeThenDrain keeps updateModeSurge for gangs — surgeUpdate performs a
// per-gang index surge (a whole replacement gang at a fresh instance
// index, gang-scheduled via its own PodGroup, then the source gang is
// drained). RecreatePod is already recreate; multiPod has no additional
// effect on it.
func chooseUpdateModeForInstance(strategy workload.UpdateStrategyType, running, target *corev1.PodSpec, multiPod bool) (updateMode, error) {
	mode, err := chooseUpdateMode(strategy, running, target)
	if err != nil {
		// InPlaceOnly + ineligible diff is the only error path. For
		// multi-pod fall back to recreate: in-place cannot safely roll
		// a worker-only change. Single-pod keeps the strict semantic.
		if multiPod && strategy == workload.UpdateStrategyInPlaceOnly {
			return updateModeRecreate, nil
		}
		return mode, err
	}
	if multiPod && mode == updateModeInPlace {
		return updateModeRecreate, nil
	}
	return mode, nil
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
		// Default matches SurgeThenDrain so direct callers (tests,
		// fuzz) skipping the defaulter get safe behavior.
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
//
// component is unused (the InstanceStatus lookup keys on idx alone)
// but kept for symmetry and future evolution toward multi-Component
// input payloads.
func loadRunningRevisionPodSpec(ctx context.Context, reads client.Reader, input workload.ReconcileInput, _ workload.ComponentType, idx int32) (*corev1.PodSpec, error) {
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
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, idx)
	if s == nil || s.RunningRevision == "" {
		return nil, nil
	}
	cr := &appsv1.ControllerRevision{}
	key := client.ObjectKey{Namespace: input.Key.Namespace, Name: s.RunningRevision}
	if err := reads.Get(ctx, key, cr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get running CR %s: %w", s.RunningRevision, err)
	}
	var payload revision.DataPayload
	if err := json.Unmarshal(cr.Data.Raw, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal running CR %s data: %w", s.RunningRevision, err)
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
func DetectUpdateTrigger(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec) (trigger bool, retryAfter time.Duration, err error) {
	return DetectUpdateTriggerWithPods(ctx, deps, input, plan, inst, target, targetSpec, nil)
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
func DetectUpdateTriggerWithPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec, instancePods []*corev1.Pod) (trigger bool, retryAfter time.Duration, err error) {
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
		dec = evaluateUpdateTriggerPods(pods, targetSpec)
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
func EvaluateUpdateTrigger(input workload.ReconcileInput, inst workload.InstancePlan, target *appsv1.ControllerRevision, targetSpec *corev1.PodSpec, instancePods []*corev1.Pod) UpdateTriggerDecision {
	dec, needPods := evaluateUpdateTriggerFast(input, inst, target)
	if !needPods {
		return dec
	}
	return evaluateUpdateTriggerPods(instancePods, targetSpec)
}

// evaluateUpdateTriggerFast runs the status-only portion of the
// update-trigger evaluation. needPods=true means the fast paths did not
// decide (empty RunningRevision) and the caller must run
// evaluateUpdateTriggerPods over the Instance's pods.
func evaluateUpdateTriggerFast(input workload.ReconcileInput, inst workload.InstancePlan, target *appsv1.ControllerRevision) (dec UpdateTriggerDecision, needPods bool) {
	s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
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

	// Same-target retry gate: a persisted RetryBlock for the
	// CURRENT target denies FRESH re-triggering. A different target
	// revision is a different RetrySubject and passes (the Failed-wedge
	// fix stands). Phase=Failed with an in-flight Update Operation is a
	// CONTINUATION (teardown/abandon of the failed candidate) and is
	// exempt — mirrors the dispatcher's startingFresh carve-out.
	failedContinuation := s.Phase == workload.InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate
	if b := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name); b != nil && !failedContinuation {
		denied, retryAfter := evaluateRetryBlockGate(b, input.Now(),
			anyInFlightUpdateAt(input.ObservedState.InstanceStatuses, target.Name))
		if denied {
			return UpdateTriggerDecision{RetryAfter: retryAfter}, false
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
// per-pod diff against the target spec, and the AdoptRevision
// selection.
func evaluateUpdateTriggerPods(pods []*corev1.Pod, targetSpec *corev1.PodSpec) UpdateTriggerDecision {
	for _, pod := range pods {
		if !podMatchesTarget(pod, targetSpec) {
			return UpdateTriggerDecision{Trigger: true}
		}
	}
	// Spec-match alone proves nothing about health: a wedged pod (e.g.
	// ImagePullBackOff) spec-matches the very revision that broke it, and
	// the adoption stamp prunes that revision's RetryBlock — Ready is
	// only stamped on proof. Require the same runtime-readiness the
	// canonical Ready promotions gate on before adopting; unproven pods
	// are simply left alone (a corrective target re-triggers via the
	// per-pod diff above, and pods that later become ready are adopted
	// on a subsequent pass).
	if !query.AllPodsRuntimeReady(pods) {
		return UpdateTriggerDecision{}
	}
	return UpdateTriggerDecision{AdoptRevision: true}
}

// BackfillRunningRevision stamps the Instance Ready on rev
// (Phase=Ready, RunningRevision=rev, Operation cleared) — the
// AdoptRevision effect, so future trigger evaluations take the
// RunningRevision fast path.
func BackfillRunningRevision(ctx context.Context, input workload.ReconcileInput, idx int32, rev string) error {
	if err := patchInstanceStatusReadyOnRevision(ctx, input, idx, rev); err != nil {
		return fmt.Errorf("backfill RunningRevision (instance=%d): %w", idx, err)
	}
	return nil
}

// isMigrateOwnedStatus reports whether the InstanceStatus carries an
// in-flight Migrate operation (either source-side or surge-side). The
// post-promote scale-down pass briefly leaves RunningRevision pointing
// at the source rev while the surge has already been promoted; without
// this guard a spec-bump arriving during that window would race the
// surge promote.
func isMigrateOwnedStatus(s *workload.InstanceStatus) bool {
	if s == nil {
		return false
	}
	if s.Phase == workload.InstancePhaseMigrating {
		return true
	}
	if s.Operation != nil && s.Operation.Type == workload.InstanceOperationMigrate {
		return true
	}
	return false
}

// isSurgeOwnedStatus reports whether the SurgeThenDrain state machine owns
// this Instance's in-flight Operation, so a mode resolved from a since-edited
// strategy must not be dispatched over it.
//
// Phase=Failed is excluded. A failed surge has already escalated to operator
// attention, and editing the strategy is one of the levers used to rescue it;
// holding the Instance on the surge machine would take that lever away.
func isSurgeOwnedStatus(s *workload.InstanceStatus) bool {
	return s != nil && s.Phase != workload.InstancePhaseFailed &&
		s.Operation != nil && s.Operation.Type == workload.InstanceOperationUpdate &&
		isSurgeUpdateStep(s.Operation.Step)
}

// isGangSurgeTargetMarker reports whether s is a gang surge-target marker —
// the transient replacement-gang index stamped by patchInstanceStatusGangSurgeTarget
// (Op{Update, Step=GangSurgeTarget}). Its lifecycle is owned by the source
// instance's gangSurgeUpdate, so the update trigger must not treat it as an
// independent target.
func isGangSurgeTargetMarker(s *workload.InstanceStatus) bool {
	return s != nil && s.Operation != nil &&
		s.Operation.Type == workload.InstanceOperationUpdate &&
		(s.Operation.Step == workload.UpdateStepGangSurgeTarget ||
			s.Operation.Step == workload.UpdateStepGangSurgeTargetCleanup)
}

// podMatchesTarget compares pod's runtime PodSpec to target — same
// image-stripped JSON AND matching container images. Used when
// RunningRevision is missing.
func podMatchesTarget(pod *corev1.Pod, target *corev1.PodSpec) bool {
	if pod == nil || target == nil {
		return false
	}
	gotStripped, err := podSpecWithoutImages(&pod.Spec)
	if err != nil {
		return false
	}
	wantStripped, err := podSpecWithoutImages(target)
	if err != nil {
		return false
	}
	if string(gotStripped) != string(wantStripped) {
		return false
	}
	return podImagesMatch(pod, target)
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
