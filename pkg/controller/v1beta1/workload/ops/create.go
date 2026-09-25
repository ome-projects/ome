package ops

import (
	"context"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/holds"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// CreateRequeueInterval is how long to wait before re-checking pod
// readiness when an Instance is still coming up, from the operator's
// lifecycle.requeue.operation. Zero means unconfigured: the caller
// requeues on the controller's rate-limited backoff instead.
func CreateRequeueInterval(input workload.ReconcileInput) time.Duration {
	return input.Requeue.Operation
}

// Create drives Create / Scale-Up toward each desired Instance.
// Multi-pass:
//
//  1. Commit selected InstanceStatus entries as Phase=Creating, then render
//     and create their missing pods with ome.io/serving=False.
//  2. Wait for runtime Ready on every pod, flip ome.io/serving=True,
//     promote InstanceStatus to Phase=Ready (Operation=nil).
//
// When target != nil the Ready promote records
// RunningRevision=target.Name so detectUpdateTrigger's fast-path can
// short-circuit. Without it, per-pod podMatchesTarget false-positives
// against post-Render pod mutations and triggers a spurious recreate.
func Create(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	return createFiltered(ctx, deps, input, plan, target, func(int32) bool { return true })
}

// CreateFreshIndices runs the Create pass for ONLY surge-free Instance
// indices — those with no in-flight surge state the Update pass owns. It
// lets a scale-up of brand-new gangs proceed even while a rollout is
// mid-flight on another index, instead of starving scale-out behind the
// rollout.
//
// A surge-free index is immune to both corruption modes the dispatcher's
// skip-Create-while-updating gate guards against: its ActiveOrdinal is a
// genuine 0 (never existed, not a stale snapshot of a flipped ordinal),
// and it has no RunningRevision to mis-stamp — its pods are created
// carrying the target rev-hash, so existingPodsMatchTargetRevision
// promotes correctly. See the surgeFreeIndex predicate.
//
// No-op (returns ctrl.Result{}, nil) when no index qualifies, so pure
// rollouts are unaffected.
func CreateFreshIndices(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	eligible := false
	for _, inst := range plan.Instances {
		if surgeFreeIndex(input, inst.Index) {
			eligible = true
			break
		}
	}
	if !eligible {
		return ctrl.Result{}, nil
	}
	return createFiltered(ctx, deps, input, plan, target, func(idx int32) bool {
		return surgeFreeIndex(input, idx)
	})
}

// CreateCommittedIndices runs the Create pass for ONLY the Instance
// indices whose committed create this pass would FINISH. It is the pass
// a paused Component runs: the set a committed create started
// materializing is completed and promoted, while an index that never
// opened one stays empty until the pause is cleared.
//
// No-op (returns ctrl.Result{}, nil) when no index qualifies.
func CreateCommittedIndices(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) (ctrl.Result, error) {
	if !HasCommittedCreate(input, plan, target) {
		return ctrl.Result{}, nil
	}
	return createFiltered(ctx, deps, input, plan, target, func(idx int32) bool {
		return createFinishableAt(input.ObservedState.Instance(idx), target)
	})
}

// HasCommittedCreate reports whether any planned index carries a create
// this pass would finish rather than replace. The decision layer asks it
// to decide whether the pass is worth planning and the pass asks it again
// to decide whether it has work, so both read one predicate.
func HasCommittedCreate(input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision) bool {
	for _, inst := range plan.Instances {
		if createFinishableAt(input.ObservedState.Instance(inst.Index), target) {
			return true
		}
	}
	return false
}

// createFinishableAt reports whether the row carries a create the pass
// would FINISH: the Creating phase and the Create operation the
// write-ahead stamp records before the first pod of the set is created,
// on an attempt the retirement rule would leave in place.
//
// The distinction matters because the pass does not only finish an
// attempt. One whose pods it OWNS — a first materialization, which is
// what an empty RunningRevision marks — committed to a revision the
// Component no longer converges to is retired and replaced by a fresh
// attempt at the current target, with a new identity, a new deadline and
// re-rendered pods. That is a new operation, so a paused Component
// leaves such a row alone and retires it on the unpause. An attempt
// naming no revision is in the same position: the retirement rule tells
// it apart from one to finish by weighing pod evidence, which is not a
// question a hold should be answering.
//
// A row that was serving before — a promoted Instance rebuilding a
// member it lost — is never retired, whatever revision its attempt
// names, so a pause finishes it: the pass adds the missing pods under
// the same operation, and holding it instead would leave the Instance
// short of its set for the length of the hold.
func createFinishableAt(s *workload.InstanceStatus, target *appsv1.ControllerRevision) bool {
	if workload.StateOf(s) != workload.StateCreateCreatePods {
		return false
	}
	// Nothing to retarget to, so nothing can be retired: every committed
	// set is one to finish.
	if target == nil {
		return true
	}
	return !createAttemptOwnsPods(s) || s.Operation.TargetRevision == target.Name
}

// surgeFreeIndex reports whether the Instance at idx has NO in-flight
// surge state — i.e. its ObservedState entry is absent (never created)
// or mid-Create (Phase=Creating with no operation, or a Create one).
// Updating / Migrating carry an in-flight surge (stale ActiveOrdinal /
// RunningRevision hazard); Ready-but-degraded is Restart/Recreate
// territory. A Creating entry carrying an Update or Migrate operation
// is a surge TARGET marker owned by another index's op — overwriting it
// with a Create stamp unpins the in-flight replacement gang from the
// plan and scale-down deletes it mid-surge. All excluded.
func surgeFreeIndex(input workload.ReconcileInput, idx int32) bool {
	s := input.ObservedState.Instance(idx)
	if s == nil {
		return true
	}
	return s.Phase == workload.InstancePhaseCreating &&
		(s.Operation == nil || s.Operation.Type == workload.InstanceOperationCreate)
}

// createFiltered is Create restricted to the Instance indices for which
// keep returns true. Create delegates with an always-true keep;
// CreateFreshIndices delegates with the surge-free predicate. Indices
// that keep rejects are left entirely untouched this pass (not listed,
// not promoted, not duplicated).
func createFiltered(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, keep func(idx int32) bool) (ctrl.Result, error) {
	if deps.Client == nil {
		return ctrl.Result{}, fmt.Errorf("Create: nil client")
	}

	existing, err := query.ListOMENativePodsByName(ctx, deps.Client, input.Key.Namespace, input.Key.OwnerName, plan.Component, true)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("Create: list pods: %w", err)
	}
	byInstance := query.BucketPodsByInstanceIdx(existing)
	return createFilteredBatched(ctx, deps, input, plan, target, keep, byInstance)
}

type createStartAction struct {
	instance workload.InstancePlan
	missing  []podTarget
	// terminal holds the dead pods still occupying missing targets' names;
	// they are recycled before the targets can be created.
	terminal       []*corev1.Pod
	firstCreate    bool
	statusChanges  bool
	transition     statusTransition
	statusMutation workload.InstanceMutation
}

type createReadyAction struct {
	instance workload.InstancePlan
	existing []*corev1.Pod
	// promote is false while the pod set is below the shared promote bar:
	// the action then only writes the serving gate, because a fresh pod
	// carries the lifecycle hold and cannot reach PodReady until it is
	// released. The Ready stamp waits for the pass that observes PodReady.
	promote        bool
	wasReady       bool
	statusChanges  bool
	markedServing  []*corev1.Pod
	transition     statusTransition
	statusMutation workload.InstanceMutation
	pruneRevision  string
}

type statusTransition struct {
	index    int32
	previous *workload.InstanceStatus
	current  *workload.InstanceStatus
}

type createPassAction struct {
	start *createStartAction
	ready *createReadyAction
}

// createFilteredBatched is the status-coalesced Create path. It preserves plan
// order while batching adjacent actions behind two ordering barriers:
//
//  1. Each adjacent group of selected Creating intents is committed in one
//     status update before any corresponding Pod create.
//  2. Serving conditions are written before each adjacent Ready group is
//     committed in one status update.
//
// ScaleUpPodBatchSize bounds missing Pods selected by a pass. Each Instance is
// atomic: all of its missing Pods are selected together or the Instance is
// deferred. The first eligible Instance may exceed a positive budget and
// proceeds alone. Ready promotions do not consume the budget.
func createFilteredBatched(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	target *appsv1.ControllerRevision,
	keep func(idx int32) bool,
	byInstance map[int32][]*corev1.Pod,
) (ctrl.Result, error) {
	// A pod in phase Unknown still occupies its stable name, so the
	// missing-pod diff below reads that target as present and nothing else
	// in the pass would free it. Route it the same way the rebuild paths
	// do — node-death evidence decides when the name is freed, and the
	// attempt reports the wait meanwhile — before any Instance is selected.
	// A held Instance is dropped from the selection below, which would
	// otherwise leave the pass looking converged: not-converged keeps the
	// ordinary poll, and nodeDeathAt carries the exact policy boundary,
	// because the node this waits on emits no event to wake anyone on.
	held, nodeDeathAt, err := recoverUnknownPhaseInstances(ctx, deps, input, plan, keep, byInstance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(held) > 0 {
		admitted := keep
		keep = func(idx int32) bool { return admitted(idx) && !held[idx] }
	}

	allReady := len(held) == 0
	var retryBlockWait time.Duration
	actions := make([]createPassAction, 0, len(plan.Instances))
	retirements := make([]retiredCreateAttempt, 0)
	selectedPods := int32(0)
	// Keep each wave as a stable Instance-order prefix. Once an eligible gang
	// does not fit, it leads the next wave instead of being bypassed by smaller
	// later Instances.
	selectionClosed := false

	for _, inst := range plan.Instances {
		if !keep(inst.Index) {
			continue
		}
		existing := byInstance[inst.Index]
		missing := missingPodTargets(input, plan, inst, existing)
		// An attempt that is fully materialized and runtime-ready is one
		// promote away from serving: its pods are already routed, so retiring
		// it now drops live traffic and throws away a warmed accelerator. Let
		// it promote; the update machinery then rolls the Instance to the new
		// target through a surge, without a gap.
		onePromoteAway := len(missing) == 0 && len(existing) > 0 && query.AllPodsRuntimeReady(existing)
		if retired, ok := retireSupersededCreateMutation(input, inst.Index, existing, target, plan.InstanceReadyTimeout); ok && !onePromoteAway {
			// Replacing an attempt starts a fresh one, so the new target's
			// RetryBlock gates it as it gates a first attempt. Retire only
			// once the replacement would be allowed to build: otherwise the
			// old attempt is torn down and nothing takes its place.
			if wait, denied := retargetDeniedByRetryBlock(input, inst.Index, target); denied {
				if wait > 0 && (retryBlockWait == 0 || wait < retryBlockWait) {
					retryBlockWait = wait
				}
				allReady = false
				continue
			}
			retirements = append(retirements, retired)
			allReady = false
			continue
		}
		// A replacement attempt owns whatever the attempt it retired left
		// behind: those pods hold the names it needs and carry a revision
		// it is not converging to.
		if stale := retiredAttemptPods(input, inst.Index, existing, target); len(stale) > 0 {
			if _, err := deleteSupersededRevisionPods(ctx, deps, input, inst.Index, stale, target.Name); err != nil {
				return ctrl.Result{}, err
			}
			allReady = false
			continue
		}
		if len(missing) > 0 {
			allowed, treatedReady, retryAfter := allowCreateForMissingInstance(input, plan, inst, existing, target)
			if retryAfter > 0 && (retryBlockWait == 0 || retryAfter < retryBlockWait) {
				retryBlockWait = retryAfter
			}
			if !allowed {
				if !treatedReady {
					allReady = false
				}
				continue
			}

			podCost := int32(len(missing))
			if selectionClosed || !scaleUpPodBatchAdmits(input.ScaleUpPodBatchSize, selectedPods, podCost) {
				selectionClosed = true
				allReady = false
				continue
			}
			now := metav1.NewTime(input.Now())
			targetRevision := ""
			if target != nil {
				targetRevision = target.Name
			}
			mutation := status.CreatingMutation(inst.Index, inst.Incarnation, plan.InstanceReadyTimeout, targetRevision, now)
			observed := input.ObservedState.Instance(inst.Index)
			probe := workload.InstanceStatus{Index: inst.Index}
			if observed != nil {
				probe = *observed
			}
			action := &createStartAction{
				instance:      inst,
				missing:       missing,
				terminal:      terminalTargetPods(existing, missing),
				firstCreate:   observed == nil,
				statusChanges: mutation.Mutate(&probe),
				transition:    statusTransition{index: inst.Index},
			}
			mutation.OnCommit = action.transition.capture
			action.statusMutation = mutation
			actions = append(actions, createPassAction{start: action})
			selectedPods += podCost
			if input.ScaleUpPodBatchSize != nil && selectedPods >= *input.ScaleUpPodBatchSize {
				selectionClosed = true
			}
			allReady = false
			continue
		}

		// ContainersReady permits writing the serving gate; the Ready stamp
		// itself waits for the shared promote bar, which the gate write is a
		// precondition of (kubelet folds it into PodReady).
		if !query.AllPodsRuntimeReady(existing) {
			allReady = false
			continue
		}

		observed := input.ObservedState.Instance(inst.Index)
		wasReady := observed != nil && observed.Phase == workload.InstancePhaseReady
		promotable, wait := query.PodSetPromotable(existing, plan.MinReadySeconds, input.Now())
		action := &createReadyAction{
			instance: inst,
			existing: existing,
			promote:  promotable,
			wasReady: wasReady,
		}
		if !promotable {
			// A set waiting out its availability window needs no poll: it
			// becomes promotable at a computable instant. One still waiting
			// for kubelet to fold the gate into PodReady has no such instant.
			input.PromoteWindow.Observe(wait)
			if wait == 0 {
				allReady = false
			}
			actions = append(actions, createPassAction{ready: action})
			continue
		}
		var mutation workload.InstanceMutation
		if target != nil && existingPodsMatchTargetRevision(existing, target) {
			mutation = status.ReadyOnRevisionMutation(inst.Index, target.Name, input.Now())
			action.pruneRevision = target.Name
		} else {
			mutation = status.ReadyMutation(inst.Index, input.Now())
		}
		probe := workload.InstanceStatus{Index: inst.Index}
		if observed != nil {
			probe = *observed
		}
		action.statusChanges = mutation.Mutate(&probe)
		action.transition.index = inst.Index
		mutation.OnCommit = action.transition.capture
		action.statusMutation = mutation
		actions = append(actions, createPassAction{ready: action})
	}
	if len(retirements) > 0 {
		mutations := make([]workload.InstanceMutation, 0, len(retirements))
		for _, retired := range retirements {
			mutations = append(mutations, retired.mutation)
		}
		ownerPresent, err := applyInstanceMutationsWithOutcome(ctx, input, mutations)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("batch retire superseded Create attempts: %w", err)
		}
		if !ownerPresent {
			return ctrl.Result{}, nil
		}
		for _, retired := range retirements {
			workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonCreateAttemptSuperseded,
				"OMENative %s retired the Create attempt pinned to %s and restarted it at %s",
				workload.InstanceKey(plan.Component, retired.index), retired.from, target.Name)
		}
		return workload.RequeueNow(), nil
	}

	readyStatusBatchingAvailable := input.ApplyInstanceMutations != nil || input.ApplyInstanceMutationsWithRetryBlock != nil
	// A revision-targeted start also transitions its RetryBlock. Group it only
	// when the adapter can commit both changes atomically.
	startStatusBatchingAvailable := input.ApplyInstanceMutationsWithRetryBlock != nil ||
		(target == nil && input.ApplyInstanceMutations != nil)
	for i := 0; i < len(actions); {
		j := i + 1
		if actions[i].start != nil {
			starts := []*createStartAction{actions[i].start}
			for startStatusBatchingAvailable && j < len(actions) && actions[j].start != nil &&
				actions[j].start.statusChanges == actions[i].start.statusChanges {
				starts = append(starts, actions[j].start)
				j++
			}
			ownerPresent, err := processStartActions(ctx, deps, input, plan, target, starts)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !ownerPresent {
				return ctrl.Result{}, nil
			}
		} else {
			ready := []*createReadyAction{actions[i].ready}
			for readyStatusBatchingAvailable && j < len(actions) && actions[j].ready != nil &&
				actions[j].ready.statusChanges == actions[i].ready.statusChanges {
				ready = append(ready, actions[j].ready)
				j++
			}
			ownerPresent, err := processReadyActions(ctx, deps, input, ready)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !ownerPresent {
				return ctrl.Result{}, nil
			}
		}
		i = j
	}

	// One wake-up for the pass: the earliest of the cadence (only while
	// Instances are still coming up) and every explicit wait collected
	// above. PassRequeue owns the precedence — an explicit wait always
	// beats the bare rate-limited backoff. A pass with everything Ready
	// and nothing due asks for no wake-up at all and waits on watches.
	var res ctrl.Result
	promoteWait := input.PromoteWindow.Pending()
	nodeDeathWait := until(input.Now(), nodeDeathAt)
	if !allReady || retryBlockWait > 0 || promoteWait > 0 || nodeDeathWait > 0 {
		cadence := time.Duration(0)
		if !allReady {
			cadence = CreateRequeueInterval(input)
		}
		res = workload.PassRequeue(cadence, retryBlockWait, promoteWait, nodeDeathWait)
	}
	// A server-suggested throttle delay is a floor, not a preference:
	// waking earlier just re-earns the 429.
	return workload.NoSoonerThan(res, input.Pacing.Pending()), nil
}

func processStartActions(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	target *appsv1.ControllerRevision,
	actions []*createStartAction,
) (bool, error) {
	mutations := make([]workload.InstanceMutation, 0, len(actions))
	for _, action := range actions {
		mutations = append(mutations, action.statusMutation)
	}
	ownerPresent, err := applyCreateIntentMutations(ctx, input, mutations, target)
	if err != nil {
		return true, fmt.Errorf("batch patch status Creating: %w", err)
	}
	if !ownerPresent {
		return false, nil
	}

	for i, action := range actions {
		if err := ctx.Err(); err != nil {
			rollbackErr := rollbackStartActions(ctx, input, actions[i:])
			return true, errors.Join(err, rollbackErr)
		}
		if action.firstCreate {
			workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonInstanceCreated,
				"OMENative %s materialized; %d pod(s) requested",
				workload.InstanceKey(input.Key.Component, action.instance.Index), len(action.missing))
		}
		// A dead pod on a stable name must be gone before the name can be
		// reused; the create waits for a later pass.
		recycling, err := recycleTerminalPods(ctx, deps, input, action.instance.Index, action.instance.Index, workload.InstanceOperationCreate, action.terminal)
		if err != nil {
			rollbackErr := rollbackStartActions(ctx, input, actions[i+1:])
			return true, errors.Join(err, rollbackErr)
		}
		if recycling {
			continue
		}
		if !deps.ExpectationsCache().Satisfied(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, action.instance.Index) {
			continue
		}
		if _, err := createMissingPods(ctx, deps, input, plan, action.instance, action.instance.Index, action.missing, query.RevisionOf(target)); err != nil {
			rejection, classified := asPodRejection(err)
			switch {
			case classified && rejection.rejection.Class == workload.APIRejectionThrottled:
				// The server asked for room. Stop creating this pass, roll
				// back the intents whose pods were never attempted, and wake
				// on the server's suggested delay instead of burning the
				// caller's backoff on an error that is not a fault. The delay
				// itself is already on the pass pacing.
				if rollbackErr := rollbackStartActions(ctx, input, actions[i+1:]); rollbackErr != nil {
					return true, rollbackErr
				}
				return true, nil
			case classified:
				// Permanent (already disposed) or capacity-blocked (waiting
				// with its clock parked): either way this Instance is
				// settled for the pass and its neighbours are unaffected.
				continue
			}
			rollbackErr := rollbackStartActions(ctx, input, actions[i+1:])
			return true, errors.Join(err, rollbackErr)
		}
	}
	return true, nil
}

func processReadyActions(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, actions []*createReadyAction) (bool, error) {
	completed := make([]*createReadyAction, 0, len(actions))
	for _, action := range actions {
		if err := ctx.Err(); err != nil {
			ownerPresent, commitErr := commitReadyActions(ctx, deps, input, completed)
			if !ownerPresent {
				return false, nil
			}
			if commitErr != nil {
				return true, commitErr
			}
			return true, err
		}
		for _, pod := range action.existing {
			if podreadiness.IsServing(pod) {
				continue
			}
			changed, err := podreadiness.MarkPodServingWithChange(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady)
			if err != nil {
				ownerPresent, commitErr := commitReadyActions(ctx, deps, input, completed)
				if !ownerPresent {
					return false, nil
				}
				markErr := fmt.Errorf("mark serving (instance=%d, pod=%s): %w", action.instance.Index, pod.Name, err)
				if commitErr != nil {
					compensateErr := rollbackReadyServing(ctx, deps, []*createReadyAction{action})
					return true, errors.Join(commitErr, compensateErr)
				}
				return true, markErr
			}
			if changed {
				action.markedServing = append(action.markedServing, pod)
			}
		}
		// Below the promote bar the gate write above is the whole action:
		// there is no status mutation to commit until a later pass observes
		// the set PodReady past its availability window.
		if action.promote {
			completed = append(completed, action)
		}
	}
	return commitReadyActions(ctx, deps, input, completed)
}

func commitReadyActions(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, actions []*createReadyAction) (bool, error) {
	if len(actions) == 0 {
		return true, nil
	}
	mutations := make([]workload.InstanceMutation, 0, len(actions))
	for _, action := range actions {
		mutations = append(mutations, action.statusMutation)
	}
	ownerPresent, err := applyInstanceMutationsWithOutcome(ctx, input, mutations)
	if err != nil {
		compensateErr := rollbackReadyServing(ctx, deps, actions[1:])
		return true, errors.Join(fmt.Errorf("batch patch status Ready: %w", err), compensateErr)
	}
	if !ownerPresent {
		return false, nil
	}
	prunedRevisions := make(map[string]struct{})
	for i, action := range actions {
		if action.pruneRevision != "" {
			if _, pruned := prunedRevisions[action.pruneRevision]; !pruned {
				if err := status.RetryBlockPruneOnPromote(ctx, input, action.pruneRevision); err != nil {
					rollbackStatusErr := rollbackReadyStatuses(ctx, input, actions[i+1:])
					rollbackServingErr := rollbackReadyServing(ctx, deps, actions[i+1:])
					return true, errors.Join(err, rollbackStatusErr, rollbackServingErr)
				}
				prunedRevisions[action.pruneRevision] = struct{}{}
			}
		}
		if !action.wasReady {
			workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonInstanceReady,
				"OMENative %s is Ready (%d pod(s) serving)",
				workload.InstanceKey(input.Key.Component, action.instance.Index), len(action.existing))
			if earliest := earliestPodCreation(action.existing); !earliest.IsZero() {
				obsmetrics.RecordPodCreateToReady(input.Key.Namespace, input.Key.OwnerName,
					string(input.Key.Component), time.Since(earliest).Seconds())
			}
		}
	}
	return true, nil
}

func rollbackReadyStatuses(ctx context.Context, input workload.ReconcileInput, actions []*createReadyAction) error {
	mutations := make([]workload.InstanceMutation, 0, len(actions))
	for _, action := range actions {
		if mutation, ok := action.transition.rollbackMutation(); ok {
			mutations = append(mutations, mutation)
		}
	}
	if len(mutations) == 0 {
		return nil
	}
	_, err := applyInstanceMutationsWithOutcome(ctx, input, mutations)
	if err != nil {
		return fmt.Errorf("rollback unprocessed Ready status: %w", err)
	}
	return nil
}

// rollbackReadyServing takes back the serving-gate writes an action issued
// before it failed part-way, so a pod is not left in rotation for an Instance
// the pass could not record. Only the gate writes this pass made are undone:
// a promoted Instance's pods were already in rotation before the pass (the
// promote bar reads PodReady, which the gate is a precondition of), so for
// those callers this is a no-op.
func rollbackReadyServing(ctx context.Context, deps workload.Deps, actions []*createReadyAction) error {
	var rollbackErrs []error
	for _, action := range actions {
		for _, pod := range action.markedServing {
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod, podreadiness.WriterLifecycle, podreadiness.KeyLifecycleInstanceReady); err != nil {
				rollbackErrs = append(rollbackErrs,
					fmt.Errorf("rollback serving (instance=%d, pod=%s): %w", action.instance.Index, pod.Name, err))
			}
		}
	}
	return errors.Join(rollbackErrs...)
}

func rollbackStartActions(ctx context.Context, input workload.ReconcileInput, actions []*createStartAction) error {
	mutations := make([]workload.InstanceMutation, 0, len(actions))
	for _, action := range actions {
		if mutation, ok := action.transition.rollbackMutation(); ok {
			mutations = append(mutations, mutation)
		}
	}
	if len(mutations) == 0 {
		return nil
	}
	_, err := applyInstanceMutationsWithOutcome(ctx, input, mutations)
	if err != nil {
		return fmt.Errorf("rollback unattempted Creating status: %w", err)
	}
	return nil
}

func (transition *statusTransition) capture(previous, current *workload.InstanceStatus) {
	transition.previous = previous
	transition.current = current
}

func (transition *statusTransition) rollbackMutation() (workload.InstanceMutation, bool) {
	return status.CreateRollbackMutation(transition.index, transition.previous, transition.current)
}

func scaleUpPodBatchAdmits(limit *int32, selectedPods, candidatePods int32) bool {
	if limit == nil {
		return true
	}
	if *limit <= 0 {
		return false
	}
	if selectedPods == 0 {
		return true
	}
	remaining := *limit - selectedPods
	return remaining > 0 && candidatePods <= remaining
}

func applyInstanceMutations(ctx context.Context, input workload.ReconcileInput, mutations []workload.InstanceMutation) error {
	if len(mutations) == 0 {
		return nil
	}
	if input.ApplyInstanceMutations != nil {
		return input.ApplyInstanceMutations(ctx, mutations)
	}
	for _, mutation := range mutations {
		if mutation.Remove {
			if mutation.Mutate != nil {
				return fmt.Errorf("instance mutation for index %d sets both Remove and Mutate", mutation.Index)
			}
			if input.RemoveInstance == nil {
				return fmt.Errorf("instance status removal is not configured")
			}
			if _, err := input.RemoveInstance(ctx, mutation.Index); err != nil {
				return err
			}
			continue
		}
		if mutation.Mutate == nil {
			return fmt.Errorf("instance mutation for index %d has no operation", mutation.Index)
		}
		if input.MutateInstance == nil {
			return fmt.Errorf("instance status mutation is not configured")
		}
		if err := input.MutateInstance(ctx, mutation.Index, mutation.Mutate); err != nil {
			return err
		}
	}
	return nil
}

func applyInstanceMutationsWithOutcome(ctx context.Context, input workload.ReconcileInput, mutations []workload.InstanceMutation) (bool, error) {
	if len(mutations) == 0 {
		return true, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock != nil {
		err := input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, "", nil)
		if errors.Is(err, workload.ErrStatusOwnerGone) {
			return false, nil
		}
		return true, err
	}
	return true, applyInstanceMutations(ctx, input, mutations)
}

func applyCreateIntentMutations(ctx context.Context, input workload.ReconcileInput, mutations []workload.InstanceMutation, target *appsv1.ControllerRevision) (bool, error) {
	if len(mutations) == 0 {
		return true, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock != nil {
		var targetRevision string
		var mutateRetryBlock func(*workload.RetryBlock) workload.RetryBlockDisposition
		if target != nil {
			targetRevision = target.Name
			mutateRetryBlock = status.RetryBlockStartAttempt
		}
		err := input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, targetRevision, mutateRetryBlock)
		if errors.Is(err, workload.ErrStatusOwnerGone) {
			return false, nil
		}
		return true, err
	}
	if err := applyInstanceMutations(ctx, input, mutations); err != nil {
		return true, err
	}
	if target == nil {
		return true, nil
	}
	return true, status.RetryBlockAttemptStarted(ctx, input, target.Name)
}

// missingPodTargets diffs the desired pod names against the live pods. A
// terminal pod is absent for this purpose: it still holds its stable name,
// but nothing will ever run in it again, so the target must be recreated.
func missingPodTargets(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, existing []*corev1.Pod) []podTarget {
	desired := expectedPodNamesForInstance(input, plan, inst)
	existingByName := query.IndexPodsByName(query.ExcludeTerminalPods(existing))
	missing := make([]podTarget, 0)
	for _, target := range desired {
		if _, ok := existingByName[target.Name]; !ok {
			missing = append(missing, target)
		}
	}
	return missing
}

// allowCreateForMissingInstance evaluates the gates that precede a Creating
// intent. A denied revision is intentionally skipped rather than reported as
// an unready create.
//
// Every pod the pass would create carries the target revision, so every one
// of them answers to that revision's RetryBlock: a Held block denies the
// whole revision, not merely the first attempt at it, and a row rebuilding a
// member it lost waits on the record exactly as a fresh start does. The
// milder states are asked of a fresh start only, because they govern OPENING
// an attempt — a backoff still running, and the one-attempt-at-a-time
// authorization — and asking them of a row already materializing would deny
// the very attempt they authorized.
//
// A denied fresh start is settled: it holds no pods and runs no deadline, so
// the pass reports it ready and waits on the block's own wake-up. A denied
// rebuild is not — it is short of its pod set, and while it carries an
// attempt that attempt's deadline is the backstop that ends the wait — so the
// pass keeps its cadence for it.
func allowCreateForMissingInstance(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, existing []*corev1.Pod, target *appsv1.ControllerRevision) (allowed, treatedReady bool, retryAfter time.Duration) {
	if target != nil {
		s := input.ObservedState.Instance(inst.Index)
		if block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name); block != nil {
			if createFreshStart(s) {
				attemptInFlight := anyInFlightUpdateAt(input.ObservedState.InstanceStatuses, target.Name) ||
					anyInFlightCreateAttempt(input, allInstances)
				if denied, wait := evaluateRetryBlockGate(block, input.Now(), attemptInFlight); denied {
					return false, true, wait
				}
			} else if block.State == workload.RetryBlockHeld {
				return false, false, 0
			}
		}
	}
	if plan.RestartPolicy == workload.RestartPolicyRecreateInstance && len(existing) > 0 {
		s := input.ObservedState.Instance(inst.Index)
		if s != nil && s.Phase == workload.InstancePhaseReady {
			return false, false, 0
		}
		if _, lost := instanceLostGangMember(input, plan, s, inst.TotalPods(), existing); lost {
			return false, false, 0
		}
	}
	return true, false, 0
}

// createFreshStart reports whether the row is one the Create pass would open
// a first attempt on: no row at all, or the Failed-with-no-Operation shape a
// disposed attempt and the operator reset mailbox both leave behind. Any
// other row is rebuilding — it holds pods, an attempt, or both.
func createFreshStart(s *workload.InstanceStatus) bool {
	return s == nil || (s.Phase == workload.InstancePhaseFailed && s.Operation == nil)
}

// createAttemptOwnsPods reports whether an Instance's pods belong to a Create
// attempt that the Create pass may retire and rebuild.
//
// Two halves. Ownership: the row is the Create pass's committed attempt —
// not an empty slot it has yet to materialize, and not a row another machine
// stamped Creating for its own replacement generation. Promotion history: a
// recorded RunningRevision excludes the row, because its pods belong to a
// serving materialization that a repair pass happens to be rebuilding, and
// rolling those to a new revision is the update machinery's business.
// Retiring one here would announce a retirement the pod cleanup then
// declines forever.
func createAttemptOwnsPods(row *workload.InstanceStatus) bool {
	return workload.StateOf(row) == workload.StateCreateCreatePods &&
		row.RunningRevision == ""
}

// retargetDeniedByRetryBlock reports whether the revision a retirement would
// pin its replacement to is currently denied, and how long until it is due.
// The in-flight question is asked exactly as the first-attempt gate asks it —
// an in-flight Create may carry no TargetRevision at all, so a
// revision-scoped reading would answer "nobody is attempting this" and let a
// second attempt start at a one-at-a-time revision. Only the row being
// retired is excluded: it is pinned to the revision being left behind, and
// counting it would deny its own retarget forever.
func retargetDeniedByRetryBlock(input workload.ReconcileInput, idx int32, target *appsv1.ControllerRevision) (time.Duration, bool) {
	block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name)
	if block == nil {
		return 0, false
	}
	others := make([]workload.InstanceStatus, 0, len(input.ObservedState.InstanceStatuses))
	for _, s := range input.ObservedState.InstanceStatuses {
		if s.Index != idx {
			others = append(others, s)
		}
	}
	attemptInFlight := anyInFlightUpdateAt(others, target.Name) || anyInFlightCreateAttempt(input, idx)
	denied, wait := evaluateRetryBlockGate(block, input.Now(), attemptInFlight)
	return wait, denied
}

// retiredCreateAttempt is one superseded Create attempt: the status write
// that replaces it and the revision it was pinned to, so the pass can name
// both revisions once the write lands.
type retiredCreateAttempt struct {
	index    int32
	from     string
	mutation workload.InstanceMutation
}

// retireSupersededCreateMutation replaces a Create attempt pinned to some
// revision other than the current target with a fresh attempt at that
// target: new operation identity, the target pinned, the deadline re-armed,
// all in the same write. A spec move is not a failure of the retired revision, so
// the row never passes through Failed and no RetryBlock is recorded against
// the revision it was building.
//
// An unpinned attempt is retired only when a pod label proves that the
// attempt belongs to another revision. The mutation is preconditioned on the
// retired attempt's exact identity, so it no-ops once the replacement has
// landed.
func retireSupersededCreateMutation(input workload.ReconcileInput, idx int32, existing []*corev1.Pod, target *appsv1.ControllerRevision, timeout time.Duration) (retiredCreateAttempt, bool) {
	if target == nil {
		return retiredCreateAttempt{}, false
	}
	row := input.ObservedState.Instance(idx)
	if !createAttemptOwnsPods(row) {
		return retiredCreateAttempt{}, false
	}
	if row.Operation.TargetRevision == target.Name {
		return retiredCreateAttempt{}, false
	}
	if row.Operation.TargetRevision == "" {
		targetRevision := query.RevisionOf(target)
		provenDifferent := false
		for _, pod := range existing {
			podRevision := query.RevisionFromPod(pod)
			if !podRevision.IsZero() && !podRevision.Same(targetRevision) {
				provenDifferent = true
				break
			}
		}
		if !provenDifferent {
			return retiredCreateAttempt{}, false
		}
	}

	now := metav1.NewTime(input.Now())
	replacement := workload.InstanceOperation{
		ID:             replacementCreateOperationID(idx, target, now),
		Type:           workload.InstanceOperationCreate,
		Step:           status.CreateStepCreatePods,
		StartedAt:      now,
		LastProgressAt: now,
		Deadline:       workload.DeadlineAt(now, timeout),
		TargetRevision: target.Name,
	}
	from := row.Operation.TargetRevision
	if from == "" {
		from = query.RevisionFromPod(firstRevisionLabeledPod(existing)).String()
	}
	return retiredCreateAttempt{index: idx, from: from, mutation: status.CreateReplacementMutation(row, replacement)}, true
}

// replacementCreateOperationID names the attempt that replaces a superseded
// one. The pinned revision is part of the identity because a replacement
// written in the same second as the attempt it retires would otherwise be
// indistinguishable from it, and every precondition in this pass reads the
// operation id.
func replacementCreateOperationID(idx int32, target *appsv1.ControllerRevision, now metav1.Time) string {
	return fmt.Sprintf("create-%d-%d-%s", idx, now.Unix(), query.RevisionOf(target).Hash())
}

// firstRevisionLabeledPod returns the first pod carrying a revision-hash
// label, or nil when none does.
func firstRevisionLabeledPod(pods []*corev1.Pod) *corev1.Pod {
	for _, pod := range pods {
		if !query.RevisionFromPod(pod).IsZero() {
			return pod
		}
	}
	return nil
}

// retiredAttemptPods returns the live pods an Instance's in-flight Create
// attempt cannot converge: they carry a revision other than the one the
// attempt is pinned to, which is what a retired attempt leaves behind.
// Restricted to an Instance that has never been promoted — a recorded
// RunningRevision means the pods belong to a serving materialization, and
// rolling those is the update machinery's business, not Create's.
func retiredAttemptPods(input workload.ReconcileInput, idx int32, existing []*corev1.Pod, target *appsv1.ControllerRevision) []*corev1.Pod {
	if target == nil {
		return nil
	}
	row := input.ObservedState.Instance(idx)
	if !createAttemptOwnsPods(row) || row.Operation.TargetRevision != target.Name {
		return nil
	}
	pinned := query.RevisionOf(target)
	if pinned.IsZero() {
		return nil
	}
	var stale []*corev1.Pod
	for _, pod := range existing {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		rev := query.RevisionFromPod(pod)
		if rev.IsZero() || rev.Same(pinned) {
			continue
		}
		stale = append(stale, pod)
	}
	return stale
}

// earliestPodCreation returns the oldest CreationTimestamp across the
// Instance's pods, or the zero time when the slice is empty / unstamped.
func earliestPodCreation(pods []*corev1.Pod) time.Time {
	var earliest time.Time
	for _, p := range pods {
		ct := p.CreationTimestamp.Time
		if ct.IsZero() {
			continue
		}
		if earliest.IsZero() || ct.Before(earliest) {
			earliest = ct
		}
	}
	return earliest
}

// existingPodsMatchTargetRevision reports whether every existing pod
// for an Instance carries the rev-hash label of `target`. Used by the
// Create pass's Ready promote to avoid stamping
// RunningRevision=target.Name on pods that are actually on a different
// (typically intermediate) revision after a bump during an in-flight
// surge.
//
// Returns true when:
//   - existing is non-empty AND
//   - every pod's `ome.io/revision-hash` label equals
//     RevisionHashFromControllerRevisionName(target.Name).
//
// Returns false otherwise. An empty existing slice returns false (no
// pods to promote against; caller short-circuits earlier on the
// no-missing-pods branch anyway, so this only safeguards the path
// where the slice is unexpectedly empty).
//
// A pod missing the rev-hash label is treated as a mismatch — we
// can't prove it's on target, so refuse to promote. Legacy pods
// (pre-LabelRevisionHash) would hit this path and stay on their
// prior RunningRevision; a fresh recreate would label them and the
// next pass would promote correctly.
func existingPodsMatchTargetRevision(existing []*corev1.Pod, target *appsv1.ControllerRevision) bool {
	if len(existing) == 0 || target == nil {
		return false
	}
	tgt := query.RevisionOf(target)
	if tgt.IsZero() {
		return false
	}
	for _, pod := range existing {
		if pod == nil {
			return false
		}
		if !query.RevisionFromPod(pod).Same(tgt) {
			return false
		}
	}
	return true
}

// podTarget describes one pod to create within an Instance.
type podTarget struct {
	Name    string
	Runner  workload.RunnerPlan
	Ordinal int32
}

// expectedPodNamesForInstance enumerates every (Runner, ordinal)
// tuple the Instance plan asks for.
//
// Single-pod Runners (Size==1) read the ordinal slot from the
// Instance's recorded ActiveOrdinal — SurgeThenDrain alternates
// between slots 0 and 1 across rollouts. Without this lookup the
// post-surge Create pass would hard-code ordinal 0 and create a
// duplicate pod alongside the (now ordinal-1) canonical pod.
//
// Multi-pod Runners (Size>1) keep enumerating every 0..Size-1
// ordinal; per-gang-member surge allocation isn't yet plumbed.
func expectedPodNamesForInstance(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan) []podTarget {
	var targets []podTarget
	for _, runner := range inst.Runners {
		if runner.Size == 1 {
			ordinal := activeOrdinalForInstance(input, inst.Index)
			targets = append(targets, podTarget{
				Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, ordinal),
				Runner:  runner,
				Ordinal: ordinal,
			})
			continue
		}
		for o := int32(0); o < runner.Size; o++ {
			targets = append(targets, podTarget{
				Name:    query.PodName(input.Key.OwnerName, plan.Component, inst.Index, runner.Name, o),
				Runner:  runner,
				Ordinal: o,
			})
		}
	}
	return targets
}

// activeOrdinalForInstance returns the InstanceStatus.ActiveOrdinal
// for idx, or 0 when no status exists yet. Single-pod
// Create / Restart / recreateUpdate paths consult this to address the
// post-surge canonical slot (which may be 1, not 0).
func activeOrdinalForInstance(input workload.ReconcileInput, idx int32) int32 {
	if s := input.ObservedState.Instance(idx); s != nil {
		return s.ActiveOrdinal
	}
	return 0
}

// createMissingPods renders and creates targets, owning the per-pod
// ExpectCreates bookkeeping. idx is the expectations bucket — pass
// inst.Index for steady-state Create / Restart Phase B. Callers must
// NOT call ExpectCreates themselves. target, when non-zero, stamps
// ome.io/revision-hash on every created pod so per-revision Services can
// select it, and names the revision a rejection is charged against.
//
// Every create path in the engine funnels through here, so this is where
// an apiserver rejection is READ rather than passed up opaquely. The
// classified outcome is returned as a *podRejectionError; only an
// unclassified (transient) rejection stays a *podCreateError, which callers
// propagate.
func createMissingPods(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, idx int32, targets []podTarget, target query.RevisionID) (int, error) {
	// A gang whose deterministic PodGroup name this owner cannot write —
	// held by another controller, or by an object still being collected —
	// gets no members: every pod is stamped with that name, so creating
	// one would enlist it in a group this owner does not control. Nothing
	// created means no progress, which is exactly the wait the caller
	// reports; the escalation pass owns the row's fate.
	if input.Gangs.BlocksPods(idx) {
		return 0, nil
	}
	created := 0
	revisionHash := target.Hash()
	// Prime the gang's peer-DNS host list once. It's identical for every
	// pod in the Instance, so caching it here turns the per-pod O(gangsize)
	// rebuild inside Render into O(gangsize) total per gang. inst is a value
	// copy, so this mutation stays loop-local. Single-pod Instances get no
	// peer list (Render's len<2 gate makes it a no-op anyway).
	if inst.PeerHostnames == nil && inst.TotalPods() > 1 {
		inst.PeerHostnames = buildInstancePeerHostnames(input.Key.OwnerName, plan.Component, inst)
	}
	for _, t := range targets {
		template := input.DesiredSpec.PodSpec
		if t.Runner.Name == workload.RunnerWorker && input.DesiredSpec.WorkerPodSpec != nil {
			template = input.DesiredSpec.WorkerPodSpec
		}

		pod, err := RenderWithRevision(
			input.OwnerObject,
			input.OwnerGVK,
			input.Key,
			template,
			input.DesiredSpec.PodTemplateObjectMeta,
			plan,
			inst,
			t.Runner,
			t.Ordinal,
			revisionHash,
			deps.RenderHook,
		)
		if err != nil {
			return created, fmt.Errorf("render pod %s: %w", t.Name, err)
		}

		// Advisory (non-blocking): a multi-node gang worker rendered with no
		// co-location podAffinity — no resolved topologyKey and no
		// operator-supplied term — may schedule across topology domains and
		// break the runtime's collectives. Warn once per episode per row.
		if err := maybeWarnGangSplitRisk(ctx, deps, input, plan, inst, t.Runner, pod); err != nil {
			return created, err
		}

		// EXPECT-ORDER: expectation before RPC, rollback via ObservedCreate
		// on error — a failed Create fires no watch event to decrement it.
		deps.ExpectationsCache().ExpectCreates(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx, 1)
		if err := deps.Client.Create(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedCreate(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx)
			if apierrors.IsAlreadyExists(err) {
				// Prior reconcile created it; cache hasn't caught up yet.
				continue
			}
			rejection := evidence.ClassifyAPIError(err)
			switch rejection.Class {
			case workload.APIRejectionPermanentWorkload, workload.APIRejectionPermanentEnvironment:
				// A migration surge's pod carries the request's placement
				// overlay, so its rejection may indict the overlay rather
				// than the revision — see disposeRejectedAttempt.
				blameRevision := inst.MigrationOverlay == nil
				if derr := disposeRejectedAttempt(ctx, deps, input, idx, target.Name(), t.Name, rejection, blameRevision); derr != nil {
					return created, fmt.Errorf("dispose rejected create (instance=%d, pod=%s): %w", idx, t.Name, derr)
				}
				return created, &podRejectionError{podName: t.Name, rejection: rejection, disposed: true, err: err}
			case workload.APIRejectionCapacityBlocked:
				recorded, incumbent, merr := status.RecordCapacityRefusal(ctx, input, idx)
				if merr != nil {
					return created, fmt.Errorf("record quota refusal (instance=%d, pod=%s): %w", idx, t.Name, merr)
				}
				// Announced when the refusal becomes the row's reported
				// wait, which the hold pass decides by precedence: a row
				// already queued behind the scheduler or a silent node keeps
				// reporting that, and hears nothing new.
				if recorded && holds.Enters(workload.RejectionReasonQuotaExceeded, incumbent) {
					workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonInstanceQuotaBlocked,
						"OMENative %s waiting on capacity: %s", workload.InstanceKey(input.Key.Component, idx), rejection.Message)
				}
				return created, &podRejectionError{podName: t.Name, rejection: rejection, err: err}
			case workload.APIRejectionThrottled:
				// The server named its own delay; deposit it on the pass so
				// whichever operation issued this create wakes no earlier.
				input.Pacing.Observe(rejection.RetryAfter)
				return created, &podRejectionError{podName: t.Name, rejection: rejection, err: err}
			}
			return created, &podCreateError{podName: t.Name, err: err}
		}
		created++
	}
	// Reaching here means every target was placed or already existed and
	// no quota refusal was seen: retire the record so the parked deadline
	// re-arms from this moment.
	if err := clearCapacityRefusal(ctx, input, idx); err != nil {
		return created, fmt.Errorf("clear quota refusal (instance=%d): %w", idx, err)
	}
	return created, nil
}

// until converts an absolute policy boundary into the delay a requeue
// takes. A zero or already-passed boundary is no delay at all — there is
// nothing left to wait for, and asking for a zero RequeueAfter would
// read as "no requeue requested".
func until(now, at time.Time) time.Duration {
	if at.IsZero() {
		return 0
	}
	if wait := at.Sub(now); wait > 0 {
		return wait
	}
	return 0
}

// maybeWarnGangSplitRisk emits a one-shot Warning when a multi-node gang
// WORKER pod is about to be created with no co-location podAffinity at
// all. Such a gang's pods may land in separate network / NVLink / TPU
// topology domains, which breaks the tightly-coupled collectives a
// multi-node runtime needs (NCCL/RCCL/NIXL all-reduce, multi-host TPU
// sessions). Advisory only — it never blocks the create.
//
// It reads the ALREADY-RENDERED pod, so it inherits injectGangDomainAffinity's
// exact decision instead of re-deriving it (no logic drift):
//   - a resolved Component topologyKey means the injector added a required
//     term -> not at risk;
//   - a hand-written operator podAffinity is preserved on the pod → not
//     at risk;
//   - only a worker left with zero required podAffinity trips the warning.
//
// Accelerator-agnostic by construction: it inspects podAffinity, never the
// requested resource kind, so GPU/RDMA/TPU/any future gang are covered the
// same way. Announced once per episode on the Instance's own row, so the
// warning reaches an operator once per workload rather than once per
// controller process. nil-safe.
func maybeWarnGangSplitRisk(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, pod *corev1.Pod) error {
	// Only a multi-node gang WORKER can split. The leader is the domain
	// anchor (carries no co-location term by design); single-pod Instances
	// have nothing to co-locate.
	if runner.Name != workload.RunnerWorker || !instanceHasLeader(inst) {
		return nil
	}
	// A required podAffinity term — OME-injected or operator-supplied —
	// means the gang is already pinned to one domain.
	if pod == nil || hasAnyRequiredPodAffinity(pod) {
		return nil
	}
	target := workload.EventTarget(input)
	if target == nil || deps.Recorder == nil {
		return nil
	}
	announced, err := status.Announce(ctx, input, inst.Index, workload.EventReasonGangSplitRisk)
	if err != nil || !announced {
		return err
	}
	deps.Recorder.Eventf(target, corev1.EventTypeWarning, string(workload.EventReasonGangSplitRisk),
		"OMENative component=%s is a multi-node gang but its worker pods have no co-location podAffinity: "+
			"the gang may be scheduled across separate network/NVLink/TPU topology domains, breaking the "+
			"tightly-coupled collectives a multi-node runtime needs (NCCL/RCCL/NIXL all-reduce, multi-host "+
			"TPU sessions). Set %s.topologyKey to the node label that identifies the required topology "+
			"domain, or declare a worker podAffinity.",
		plan.Component, plan.Component)
	return nil
}

// hasAnyRequiredPodAffinity reports whether pod declares at least one
// required (hard) podAffinity term — the signal that some co-location
// constraint, OME-injected or operator-supplied, is in force.
func hasAnyRequiredPodAffinity(pod *corev1.Pod) bool {
	return pod.Spec.Affinity != nil &&
		pod.Spec.Affinity.PodAffinity != nil &&
		len(pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution) > 0
}
