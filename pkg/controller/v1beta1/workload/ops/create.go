package ops

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// CreateRequeueInterval is how long to wait before re-checking pod
// readiness when an Instance is still coming up.
const CreateRequeueInterval = 5 * time.Second

// createStepCreatePods is the write-ahead marker for a committed Instance
// materialization. The status mutation is persisted before any Pod create.
const createStepCreatePods = "CreatePods"

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
// rollout (a 20-40 min cold-compile on TPU).
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
	observed := input.ObservedState.InstanceStatuses
	eligible := false
	for _, inst := range plan.Instances {
		if surgeFreeIndex(observed, inst.Index) {
			eligible = true
			break
		}
	}
	if !eligible {
		return ctrl.Result{}, nil
	}
	return createFiltered(ctx, deps, input, plan, target, func(idx int32) bool {
		return surgeFreeIndex(observed, idx)
	})
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
func surgeFreeIndex(observed []workload.InstanceStatus, idx int32) bool {
	s := findInstanceStatus(observed, idx)
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
	instance       workload.InstancePlan
	existing       []*corev1.Pod
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
	allReady := true
	var retryBlockWait time.Duration
	actions := make([]createPassAction, 0, len(plan.Instances))
	retirements := make([]workload.InstanceMutation, 0)
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
		if mutation, ok := retireSupersededCreateMutation(input, inst.Index, existing, target); ok {
			retirements = append(retirements, mutation)
			allReady = false
			continue
		}
		missing := missingPodTargets(input, plan, inst, existing)
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
			mutation := createStatusCreatingMutation(inst.Index, inst.Incarnation, plan.InstanceReadyTimeout, targetRevision, now)
			observed := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
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

		if !query.AllPodsRuntimeReady(existing) {
			allReady = false
			continue
		}

		observed := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
		wasReady := observed != nil && observed.Phase == workload.InstancePhaseReady
		action := &createReadyAction{
			instance: inst,
			existing: existing,
			wasReady: wasReady,
		}
		var mutation workload.InstanceMutation
		if target != nil && existingPodsMatchTargetRevision(existing, target) {
			mutation = createStatusReadyOnRevisionMutation(inst.Index, target.Name, input.Now())
			action.pruneRevision = target.Name
		} else {
			mutation = createStatusReadyMutation(inst.Index, input.Now())
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
		ownerPresent, err := applyInstanceMutationsWithOutcome(ctx, input, retirements)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("batch retire superseded Create attempts: %w", err)
		}
		if !ownerPresent {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{Requeue: true}, nil
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

	res := ctrl.Result{}
	if !allReady {
		res.RequeueAfter = CreateRequeueInterval
	}
	if retryBlockWait > 0 && (res.RequeueAfter == 0 || retryBlockWait < res.RequeueAfter) {
		res.RequeueAfter = retryBlockWait
	}
	// A server-suggested throttle delay is a floor, not a preference:
	// waking earlier just re-earns the 429.
	if throttle := input.Pacing.Throttle(); throttle > res.RequeueAfter {
		res.RequeueAfter = throttle
	}
	return res, nil
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
			recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonInstanceCreated,
				"OMENative %s materialized; %d pod(s) requested",
				instanceKey(input.Key.Component, action.instance.Index), len(action.missing))
		}
		// A dead pod on a stable name must be gone before the name can be
		// reused; the create waits for a later pass.
		recycling, err := recycleTerminalPods(ctx, deps, input, action.instance.Index, workload.InstanceOperationCreate, action.terminal)
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
		completed = append(completed, action)
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
				if err := pruneRetryBlockOnPromote(ctx, input, action.pruneRevision); err != nil {
					rollbackStatusErr := rollbackReadyStatuses(ctx, input, actions[i+1:])
					rollbackServingErr := rollbackReadyServing(ctx, deps, actions[i+1:])
					return true, errors.Join(err, rollbackStatusErr, rollbackServingErr)
				}
				prunedRevisions[action.pruneRevision] = struct{}{}
			}
		}
		if !action.wasReady {
			recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonInstanceReady,
				"OMENative %s is Ready (%d pod(s) serving)",
				instanceKey(input.Key.Component, action.instance.Index), len(action.existing))
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
	if transition.current == nil {
		return workload.InstanceMutation{}, false
	}
	committed := *transition.current
	matchesCommitted := func(status *workload.InstanceStatus) bool {
		return sameCreateTransitionOwnerState(*status, committed)
	}
	if transition.previous == nil {
		return workload.InstanceMutation{
			Index:        transition.index,
			Remove:       true,
			Precondition: matchesCommitted,
		}, true
	}
	previous := *transition.previous
	return workload.InstanceMutation{
		Index: transition.index,
		Mutate: func(status *workload.InstanceStatus) bool {
			if !matchesCommitted(status) {
				return false
			}
			restoreCreateTransitionState(status, previous)
			return true
		},
	}, true
}

// sameCreateTransitionOwnerState compares the state that can authorize a
// Create rollback. Publication-only Pod observations do not own the lifecycle
// transition and may advance independently.
func sameCreateTransitionOwnerState(current, committed workload.InstanceStatus) bool {
	current.ReadyPodCount = 0
	current.ScheduledPodCount = 0
	current.NodesOccupied = nil
	committed.ReadyPodCount = 0
	committed.ScheduledPodCount = 0
	committed.NodesOccupied = nil
	return reflect.DeepEqual(current, committed)
}

func restoreCreateTransitionState(status *workload.InstanceStatus, previous workload.InstanceStatus) {
	previous.ReadyPodCount = status.ReadyPodCount
	previous.ScheduledPodCount = status.ScheduledPodCount
	if status.NodesOccupied != nil {
		previous.NodesOccupied = append([]string(nil), status.NodesOccupied...)
	} else {
		previous.NodesOccupied = nil
	}
	*status = previous
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
			mutateRetryBlock = markRetryBlockAttemptStarted
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
	return true, flipRetryBlockOnAttemptStart(ctx, input, target.Name)
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
func allowCreateForMissingInstance(input workload.ReconcileInput, plan workload.ComponentPlan, inst workload.InstancePlan, existing []*corev1.Pod, target *appsv1.ControllerRevision) (allowed, treatedReady bool, retryAfter time.Duration) {
	if target != nil {
		s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
		freshStart := s == nil || (s.Phase == workload.InstancePhaseFailed && s.Operation == nil)
		if freshStart {
			if block := workload.FindRetryBlock(input.ObservedState.RetryBlocks, target.Name); block != nil {
				attemptInFlight := anyInFlightUpdateAt(input.ObservedState.InstanceStatuses, target.Name) ||
					anyInFlightCreateAttempt(input.ObservedState.InstanceStatuses)
				if denied, wait := evaluateRetryBlockGate(block, input.Now(), attemptInFlight); denied {
					return false, true, wait
				}
			}
		}
	}
	if plan.RestartPolicy == workload.RestartPolicyRecreateInstance && len(existing) > 0 {
		s := findInstanceStatus(input.ObservedState.InstanceStatuses, inst.Index)
		if s != nil && s.Phase == workload.InstancePhaseReady {
			return false, false, 0
		}
		if _, lost := instanceLostGangMember(input, plan, s, inst.TotalPods(), existing); lost {
			return false, false, 0
		}
	}
	return true, false, 0
}

// retireSupersededCreateMutation ends a Create when the live target changes.
// An unpinned attempt is retired only when a pod label proves that the attempt
// belongs to another revision.
func retireSupersededCreateMutation(input workload.ReconcileInput, idx int32, existing []*corev1.Pod, target *appsv1.ControllerRevision) (workload.InstanceMutation, bool) {
	if target == nil {
		return workload.InstanceMutation{}, false
	}
	status := findInstanceStatus(input.ObservedState.InstanceStatuses, idx)
	if status == nil || status.Phase != workload.InstancePhaseCreating ||
		status.Operation == nil || status.Operation.Type != workload.InstanceOperationCreate {
		return workload.InstanceMutation{}, false
	}
	if status.Operation.TargetRevision == target.Name {
		return workload.InstanceMutation{}, false
	}
	if status.Operation.TargetRevision == "" {
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
			return workload.InstanceMutation{}, false
		}
	}

	expectedIncarnation := status.Incarnation
	expectedID := status.Operation.ID
	expectedTarget := status.Operation.TargetRevision
	return workload.InstanceMutation{Index: idx, Mutate: func(current *workload.InstanceStatus) bool {
		if current.Incarnation != expectedIncarnation || current.Phase != workload.InstancePhaseCreating ||
			current.Operation == nil || current.Operation.Type != workload.InstanceOperationCreate ||
			current.Operation.ID != expectedID || current.Operation.TargetRevision != expectedTarget {
			return false
		}
		current.Phase = workload.InstancePhaseFailed
		current.Operation = nil
		return true
	}}, true
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
// (typically intermediate) revision — the X-2 bump-during-bump
// corruption mode.
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
			ordinal := activeOrdinalForInstance(input.ObservedState.InstanceStatuses, inst.Index)
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
func activeOrdinalForInstance(observed []workload.InstanceStatus, idx int32) int32 {
	if s := findInstanceStatus(observed, idx); s != nil {
		return s.ActiveOrdinal
	}
	return 0
}

// findInstanceStatus returns a pointer to the matching InstanceStatus
// in the observed slice, or nil if absent. Callers must NOT mutate
// the returned pointer — it aliases ObservedState.
func findInstanceStatus(observed []workload.InstanceStatus, idx int32) *workload.InstanceStatus {
	for i := range observed {
		if observed[i].Index == idx {
			return &observed[i]
		}
	}
	return nil
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
		if t.Runner.Name == "worker" && input.DesiredSpec.WorkerPodSpec != nil {
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
		// break the runtime's collectives. Warn once per (owner, Component).
		maybeWarnGangSplitRisk(deps.Recorder, input, plan, inst, t.Runner, pod)

		// EXPECT-ORDER: expectation before RPC, rollback via ObservedCreate
		// on error — a failed Create fires no watch event to decrement it.
		deps.ExpectationsCache().ExpectCreates(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx, 1)
		if err := deps.Client.Create(ctx, pod); err != nil {
			deps.ExpectationsCache().ObservedCreate(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx)
			if apierrors.IsAlreadyExists(err) {
				// Prior reconcile created it; cache hasn't caught up yet.
				continue
			}
			rejection := workload.ClassifyAPIError(err)
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
				entered, merr := markCapacityBlocked(ctx, input, idx)
				if merr != nil {
					return created, fmt.Errorf("record quota wait (instance=%d, pod=%s): %w", idx, t.Name, merr)
				}
				if entered {
					recordNormal(deps.Recorder, eventTarget(input), workload.EventReasonInstanceQuotaBlocked,
						"OMENative %s waiting on capacity: %s", instanceKey(input.Key.Component, idx), rejection.Message)
				}
				return created, &podRejectionError{podName: t.Name, rejection: rejection, err: err}
			case workload.APIRejectionThrottled:
				// The server named its own delay; deposit it on the pass so
				// whichever operation issued this create wakes no earlier.
				input.Pacing.ObserveThrottle(rejection.RetryAfter)
				return created, &podRejectionError{podName: t.Name, rejection: rejection, err: err}
			}
			return created, &podCreateError{podName: t.Name, err: err}
		}
		created++
	}
	// Reaching here means every target was placed or already existed and
	// no quota refusal was seen: release the waiting token so the parked
	// deadline re-arms from this moment.
	if err := clearCapacityBlock(ctx, input, idx); err != nil {
		return created, fmt.Errorf("clear quota wait (instance=%d): %w", idx, err)
	}
	return created, nil
}

// createStatusCreatingMutation stamps Phase=Creating with a durable Create
// operation. Existing nonzero incarnations are preserved across retries.
func createStatusCreatingMutation(idx int32, incarnation int64, timeout time.Duration, targetRev string, now metav1.Time) workload.InstanceMutation {
	return workload.InstanceMutation{Index: idx, Mutate: func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseCreating &&
			s.Operation != nil && s.Operation.Type == workload.InstanceOperationCreate &&
			s.Incarnation > 0 {
			if s.Operation.TargetRevision == "" && targetRev != "" {
				op := *s.Operation
				op.TargetRevision = targetRev
				s.Operation = &op
				return true
			}
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = incarnation
		}
		s.Phase = workload.InstancePhaseCreating
		s.Operation = &workload.InstanceOperation{
			ID:             fmt.Sprintf("create-%d-%d", idx, now.Unix()),
			Type:           workload.InstanceOperationCreate,
			Step:           createStepCreatePods,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       metav1.NewTime(now.Add(timeout)),
			TargetRevision: targetRev,
		}
		return true
	}}
}

// patchInstanceStatusReady idempotently moves to Phase=Ready, clears Operation.
func patchInstanceStatusReady(ctx context.Context, input workload.ReconcileInput, idx int32) error {
	mutation := createStatusReadyMutation(idx, input.Now())
	return input.MutateInstance(ctx, mutation.Index, mutation.Mutate)
}

// markReadyTransition flips s into Ready and stamps ReadySince when this is
// an entry into Ready rather than an idempotent re-apply. Container restarts
// that finished before the stamp (boot crashes, in-place update restarts)
// therefore never read as post-Ready failures.
func markReadyTransition(s *workload.InstanceStatus, now time.Time) {
	if s.Phase != workload.InstancePhaseReady {
		t := metav1.NewTime(now)
		s.ReadySince = &t
	}
	s.Phase = workload.InstancePhaseReady
}

func createStatusReadyMutation(idx int32, now time.Time) workload.InstanceMutation {
	return workload.InstanceMutation{Index: idx, Mutate: func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseReady && s.Operation == nil {
			return false
		}
		markReadyTransition(s, now)
		s.Operation = nil
		return true
	}}
}

// patchInstanceStatusReadyOnRevision idempotently moves to Phase=Ready
// with RunningRevision=rev; clears Operation + TargetRevision. The
// post-promote anchor that lets detectUpdateTrigger's fast-path
// short-circuit on revision-equality before per-pod diffs. Success at
// rev also prunes rev's RetryBlock — a converged subject leaves no
// active block.
func patchInstanceStatusReadyOnRevision(ctx context.Context, input workload.ReconcileInput, idx int32, rev string) error {
	mutation := createStatusReadyOnRevisionMutation(idx, rev, input.Now())
	err := input.MutateInstance(ctx, mutation.Index, mutation.Mutate)
	if err != nil {
		return err
	}
	return pruneRetryBlockOnPromote(ctx, input, rev)
}

func createStatusReadyOnRevisionMutation(idx int32, rev string, now time.Time) workload.InstanceMutation {
	return workload.InstanceMutation{Index: idx, Mutate: func(s *workload.InstanceStatus) bool {
		if s.Phase == workload.InstancePhaseReady &&
			s.Operation == nil &&
			s.RunningRevision == rev &&
			s.TargetRevision == "" {
			return false
		}
		markReadyTransition(s, now)
		s.RunningRevision = rev
		s.TargetRevision = ""
		s.Operation = nil
		return true
	}}
}
