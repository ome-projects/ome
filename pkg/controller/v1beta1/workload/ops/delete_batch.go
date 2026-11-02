package ops

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// deleteDrainKey is shared by every Pod in an Instance so Pod deletion clears
// the Instance's scale-down serving hold.
func deleteDrainKey(idx int32) string {
	return strconv.Itoa(int(idx))
}

// DeleteBatchResult describes whether the scale-down action still owns the
// pipeline and whether a fresh status commit requires an immediate replan.
type DeleteBatchResult struct {
	InProgress        bool
	ImmediateRequeue  bool
	SelectedPodCost   int32
	Deferred          int
	Oversized         bool
	RequeueAfter      time.Duration
	PolicyDeadlineDue bool
}

type deleteBatchCandidate struct {
	status workload.InstanceStatus
	pods   []*corev1.Pod
	cost   int32
}

type deleteBatchSelection struct {
	candidates []deleteBatchCandidate
	deferred   int
	oversized  bool
	fresh      bool
}

// DeleteBatch advances one durable, gang-atomic scale-down wave: the
// Instances the plan no longer covers leave in waves sized by the pod
// budget. A fresh wave is written Deleting in one status transaction and
// takes no external effect in the pass that admits it. A wave already
// Delete-owned resumes from the authoritative Pod snapshot: every serving
// pod leaves rotation, each row waits for its drain and deletes its pods,
// and the rows with no pod left are removed in one transaction.
func DeleteBatch(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	extras []int32,
	podsByInstance map[int32][]*corev1.Pod,
) (DeleteBatchResult, error) {
	if deps.Client == nil {
		return DeleteBatchResult{}, fmt.Errorf("DeleteBatch: nil client")
	}
	if input.OwnerObject == nil || input.OwnerObject.GetUID() == "" {
		return DeleteBatchResult{}, fmt.Errorf("DeleteBatch: owner with UID is required")
	}
	wave, err := selectScaleDownWave(deps, input, plan, extras, podsByInstance)
	if err != nil {
		return DeleteBatchResult{}, err
	}
	defer wave.log(ctx)
	if len(wave.selection.candidates) == 0 {
		return wave.result, nil
	}
	steps := []func(context.Context) (bool, error){
		wave.writeDeleting,
		wave.confirmResumedRows,
		wave.driveDeleteBatch,
		wave.reportOverdueDrains,
		wave.removeRows,
	}
	for _, step := range steps {
		stop, err := step(ctx)
		if err != nil {
			return DeleteBatchResult{}, err
		}
		if stop {
			return wave.result, nil
		}
	}
	return wave.result, nil
}

// scaleDownWave is one pass of the scale-down over one Component: the rows
// the pass selected, the budget they were selected under, and what the
// steps find out as they run. A step reports stop when the pass ends with
// it; the next pass selects again from the top.
type scaleDownWave struct {
	deps           workload.Deps
	input          workload.ReconcileInput
	plan           workload.ComponentPlan
	podsByInstance map[int32][]*corev1.Pod

	// budget is the pod count one wave may take, nil when unbounded;
	// footprint is the pod count of every observed Instance it was
	// resolved against.
	budget    *int32
	footprint int32
	selection deleteBatchSelection
	result    DeleteBatchResult
	// admitted counts the Instances a fresh wave wrote Deleting this pass.
	admitted int

	// expectationsSatisfied records, per selected row, whether the
	// expectations cache has seen every delete the row expected.
	expectationsSatisfied map[int32]bool
	// refused records the rows whose own write the apiserver refused for
	// good mid-wave; each is left as the refusal left it, out of this wave.
	refused map[int32]struct{}
	// completed is the rows with no pod left, removed at the end of the pass.
	completed []deleteBatchCandidate
	// policyDeadlineAt is the earliest time the force-delete policy wants
	// the pass back for a pod already Terminating; zero when none does.
	policyDeadlineAt time.Time
}

// selectScaleDownWave resolves the pod budget, picks the rows this pass
// drives (selectDeleteBatch) and records what it picked in the pass's
// counters and metrics.
func selectScaleDownWave(deps workload.Deps, input workload.ReconcileInput, plan workload.ComponentPlan, extras []int32, podsByInstance map[int32][]*corev1.Pod) (*scaleDownWave, error) {
	footprint := workload.ScaleDownPodFootprint(input.ObservedState.InstanceStatuses, podsByInstance)
	budget, err := workload.ResolveScaleDownPodBatchSize(input.ScaleDownPodBatchSize, footprint)
	if err != nil {
		return nil, fmt.Errorf("DeleteBatch: resolve scale-down Pod batch size: %w", err)
	}
	selection, err := selectDeleteBatch(input.ObservedState.InstanceStatuses, extrasNotParked(input, extras, podsByInstance), podsByInstance, budget, input.ObservedState.CordonedInstances)
	if err != nil {
		return nil, err
	}
	wave := &scaleDownWave{
		deps:                  deps,
		input:                 input,
		plan:                  plan,
		podsByInstance:        podsByInstance,
		budget:                budget,
		footprint:             footprint,
		selection:             selection,
		expectationsSatisfied: make(map[int32]bool, len(selection.candidates)),
		refused:               make(map[int32]struct{}),
	}
	wave.result = DeleteBatchResult{
		InProgress: len(selection.candidates) > 0 || selection.deferred > 0,
		Deferred:   selection.deferred,
		Oversized:  selection.oversized,
	}
	for _, candidate := range selection.candidates {
		wave.result.SelectedPodCost += candidate.cost
	}
	obsmetrics.SetScaleDownActivePods(input.Key.Namespace, input.Key.OwnerName, string(plan.Component), wave.result.SelectedPodCost)
	obsmetrics.SetScaleDownDeferredInstances(input.Key.Namespace, input.Key.OwnerName, string(plan.Component), wave.result.Deferred)
	return wave, nil
}

// log reports the wave as the pass leaves it, whatever ended the pass.
func (w *scaleDownWave) log(ctx context.Context) {
	budgetValue := any("unbounded")
	if w.budget != nil {
		budgetValue = *w.budget
	}
	configuredBudget := any("unbounded")
	if w.input.ScaleDownPodBatchSize != nil {
		configuredBudget = w.input.ScaleDownPodBatchSize.String()
	}
	logf.FromContext(ctx).Info("OMENative scale-down wave",
		"namespace", w.input.Key.Namespace,
		"isvc", w.input.Key.OwnerName,
		"component", w.plan.Component,
		"podBudget", budgetValue,
		"configuredPodBudget", configuredBudget,
		"podFootprint", w.footprint,
		"activePodCost", w.result.SelectedPodCost,
		"activeInstances", len(w.selection.candidates),
		"admittedInstances", w.admitted,
		"deferredInstances", w.result.Deferred)
}

// staleWrite reports whether a status write failed because the rows moved
// under the pass or the owner is gone; the pass then asks for a fresh plan
// in place of an error.
func staleWrite(err error) bool {
	return errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone)
}

// selectDeleteBatch picks the wave this pass drives. Delete-owned rows come
// first, oldest operation first; only when none remain are fresh extras
// admitted, those with a pod on a cordoned node first, then the highest
// index, so a wave the budget splits empties the cordoned nodes first.
func selectDeleteBatch(
	statuses []workload.InstanceStatus,
	extras []int32,
	podsByInstance map[int32][]*corev1.Pod,
	budget *int32,
	cordoned map[int32]struct{},
) (deleteBatchSelection, error) {
	if budget != nil && *budget <= 0 {
		return deleteBatchSelection{}, fmt.Errorf("DeleteBatch: scale-down Pod batch size must be positive")
	}
	extraSet := make(map[int32]struct{}, len(extras))
	for _, index := range extras {
		extraSet[index] = struct{}{}
	}

	owned := make([]deleteBatchCandidate, 0)
	fresh := make([]deleteBatchCandidate, 0, len(extras))
	for _, original := range statuses {
		row := status.CloneDeleteInstanceStatus(original)
		candidate := deleteBatchCandidate{
			status: row,
			pods:   append([]*corev1.Pod(nil), podsByInstance[row.Index]...),
		}
		sort.SliceStable(candidate.pods, func(i, j int) bool {
			left, right := candidate.pods[i], candidate.pods[j]
			if left == nil || right == nil {
				return left == nil && right != nil
			}
			if left.Namespace != right.Namespace {
				return left.Namespace < right.Namespace
			}
			return left.Name < right.Name
		})
		candidate.cost = int32(len(candidate.pods))
		if candidate.cost == 0 {
			candidate.cost = 1
		}
		if deleteOwned(row) {
			owned = append(owned, candidate)
			continue
		}
		if _, ok := extraSet[row.Index]; ok {
			fresh = append(fresh, candidate)
		}
	}

	pool := owned
	selection := deleteBatchSelection{}
	blockedFresh := 0
	if len(pool) == 0 {
		pool = fresh
		selection.fresh = true
		sort.Slice(pool, func(i, j int) bool {
			_, iCordoned := cordoned[pool[i].status.Index]
			_, jCordoned := cordoned[pool[j].status.Index]
			if iCordoned != jCordoned {
				return iCordoned
			}
			return pool[i].status.Index > pool[j].status.Index
		})
	} else {
		blockedFresh = len(fresh)
		sort.SliceStable(pool, func(i, j int) bool {
			iStarted := pool[i].status.Operation.StartedAt.Time
			jStarted := pool[j].status.Operation.StartedAt.Time
			if iStarted.Equal(jStarted) {
				return pool[i].status.Index > pool[j].status.Index
			}
			return iStarted.Before(jStarted)
		})
	}

	var selectedCost int32
	for _, candidate := range pool {
		if budget != nil && len(selection.candidates) > 0 && selectedCost+candidate.cost > *budget {
			break
		}
		if budget != nil && len(selection.candidates) == 0 && candidate.cost > *budget {
			selection.oversized = true
		}
		selection.candidates = append(selection.candidates, candidate)
		selectedCost += candidate.cost
	}
	selection.deferred = len(pool) - len(selection.candidates) + blockedFresh
	return selection, nil
}

func admitDeleteBatch(ctx context.Context, input workload.ReconcileInput, plan workload.ComponentPlan, candidates []deleteBatchCandidate) (bool, error) {
	failure := retiredAttemptFailure(input.ObservedState, candidates)
	committed, heldAttempts, err := status.StampDeletingBatch(ctx, input, plan.InstanceReadyTimeout, candidateStatuses(candidates), failure)
	if committed && heldAttempts > 0 && input.WarnRetryHeld != nil {
		input.WarnRetryHeld(failure.TargetRevision, heldAttempts, failure.Reason)
	}
	return committed, err
}

// retiredAttemptFailure is the failed wave a scale-down owes the ladder
// for an attempt it retires. A candidate carrying a single-Instance
// update attempt — a recreate, an in-place patch or a single-pod surge —
// whose pods on the attempt's revision show a workload-caused failure is
// an attempt failing on its own; the wave is what ends it, so the wave
// records it the way the escalation would, with the pod's reason. A
// failed gang surge pair the plan let go is read through its source
// (retiredGangSurgeFailure), against the observed rows, which hold the
// replacement the source names. One record per wave: the ladder counts
// waves, not rows. nil when the wave retires no such attempt — an attempt
// that showed no failure is left to the gate, which reads an
// authorization with no attempt behind it as interrupted and lets the
// roll reopen it.
func retiredAttemptFailure(observed workload.WorkloadObservedState, candidates []deleteBatchCandidate) *status.AttemptFailure {
	for _, candidate := range candidates {
		row := candidate.status
		if failure := retiredGangSurgeFailure(&row, candidates, observed); failure != nil {
			return failure
		}
		if !retiresUpdateAttempt(&row) {
			continue
		}
		pods := evidence.AttemptStuckPods(row, candidate.pods, row.Operation.TargetRevision)
		pod, reason := evidence.FirstWorkloadCausedPod(pods)
		if pod == nil {
			continue
		}
		return &status.AttemptFailure{
			TargetRevision: row.Operation.TargetRevision,
			Reason:         reason,
			Cause:          workload.FailureCauseOf(reason),
		}
	}
	return nil
}

// retiresUpdateAttempt reports whether a wave candidate carries an
// in-flight single-Instance update attempt at a pinned revision. A gang
// surge source names its replacement through SurgeIndex and is read by
// retiredGangSurgeFailure; a surge target marker carries the gang-target
// steps and records nothing on its own — its attempt is its source's.
func retiresUpdateAttempt(row *workload.InstanceStatus) bool {
	if !workload.UpdateContinuation(row) || row.Operation == nil {
		return false
	}
	op := row.Operation
	return op.Type == workload.InstanceOperationUpdate && op.TargetRevision != "" &&
		op.SurgeIndex == nil && !isGangSurgeTargetMarker(row)
}

// retiresGangSurgeAttempt reports whether a wave candidate is the source
// of a gang surge attempt at a pinned revision: an update continuation
// that names its replacement through SurgeIndex. The plan pins the pair
// while the source rolls, so the wave meets one once the attempt has
// failed and the pair is retired together in place of the gang abandon.
func retiresGangSurgeAttempt(row *workload.InstanceStatus) bool {
	if !workload.UpdateContinuation(row) || row.Operation == nil {
		return false
	}
	op := row.Operation
	return op.Type == workload.InstanceOperationUpdate && op.TargetRevision != "" && op.SurgeIndex != nil
}

// retiredGangSurgeFailure is the record a wave owes for a gang surge
// source it retires. A source whose replacement is already Ready on the
// revision the surge rolled to ended a successful attempt and owes
// nothing, whatever failure its own row still records: a replacement
// member that exited and was rebuilt during the drain is written there,
// and this removal only clears the row the finished surge left standing.
// Otherwise the attempt's pods live under the replacement's index: the
// marker in the same wave whose pods show a workload-caused failure
// gives the reason; else the failure the escalation recorded on the
// source's row does, classified the way the gang abandon classifies it.
// nil when neither says anything.
func retiredGangSurgeFailure(row *workload.InstanceStatus, candidates []deleteBatchCandidate, observed workload.WorkloadObservedState) *status.AttemptFailure {
	if !retiresGangSurgeAttempt(row) {
		return nil
	}
	target := row.Operation.TargetRevision
	if gangSurgePromotedTargetMatches(observed.Instance(*row.Operation.SurgeIndex), target) {
		return nil
	}
	for _, candidate := range candidates {
		if candidate.status.Index != *row.Operation.SurgeIndex || !isGangSurgeTargetMarker(&candidate.status) {
			continue
		}
		pods := evidence.AttemptStuckPods(candidate.status, candidate.pods, target)
		if _, reason := evidence.FirstWorkloadCausedPod(pods); reason != "" {
			return &status.AttemptFailure{TargetRevision: target, Reason: reason, Cause: workload.CauseWorkload}
		}
	}
	if row.LastFailure == nil || row.LastFailure.Reason == "" {
		return nil
	}
	// The record must be this attempt's: a failure dated before the
	// attempt started belongs to an attempt the row made at a revision
	// since withdrawn, and charging it here would hold the revision the
	// row is moving to for a failure it never had.
	if row.LastFailure.Time.Before(&row.Operation.StartedAt) {
		return nil
	}
	reason := row.LastFailure.Reason
	return &status.AttemptFailure{TargetRevision: target, Reason: reason, Cause: workload.FailureCauseOf(reason)}
}

// candidateStatuses is the rows of a wave, in selection order.
func candidateStatuses(candidates []deleteBatchCandidate) []workload.InstanceStatus {
	rows := make([]workload.InstanceStatus, 0, len(candidates))
	for _, candidate := range candidates {
		rows = append(rows, candidate.status)
	}
	return rows
}

// writeDeleting admits a fresh wave: every selected row goes to Deleting
// under one Delete operation in a single status transaction, with the
// retry record of an attempt the wave retires (admitDeleteBatch). The pass
// ends with the write, committed or not: a wave takes no external effect
// in the pass that admits it, and a write the rows moved under asks for a
// fresh plan.
func (w *scaleDownWave) writeDeleting(ctx context.Context) (bool, error) {
	if !w.selection.fresh {
		return false, nil
	}
	committed, err := admitDeleteBatch(ctx, w.input, w.plan, w.selection.candidates)
	if err != nil {
		if staleWrite(err) {
			w.result.ImmediateRequeue = true
			return true, nil
		}
		return true, fmt.Errorf("DeleteBatch: admit wave: %w", err)
	}
	w.result.ImmediateRequeue = committed
	if committed {
		w.admitted = len(w.selection.candidates)
		obsmetrics.RecordScaleDownBatchPods(string(w.plan.Component), w.result.SelectedPodCost)
		if w.result.Oversized {
			obsmetrics.RecordScaleDownOversizedBatch(string(w.plan.Component))
		}
	}
	return true, nil
}

// confirmResumedRows checks, before any external effect, that every row of
// a resumed wave still carries the wave's own Delete operation
// (preflightDeleteOwnedBatch). A row already removed has its expectations
// forgotten, and the pass asks for a fresh plan.
func (w *scaleDownWave) confirmResumedRows(ctx context.Context) (bool, error) {
	absent, err := preflightDeleteOwnedBatch(ctx, w.input, w.selection.candidates)
	if err != nil {
		if staleWrite(err) {
			w.result.ImmediateRequeue = true
			return true, nil
		}
		return true, fmt.Errorf("DeleteBatch: verify owned wave: %w", err)
	}
	if len(absent) == 0 {
		return false, nil
	}
	for index := range absent {
		w.deps.ExpectationsCache().Forget(w.input.Key.Namespace, w.input.Key.OwnerName, w.input.Key.Component, index)
	}
	w.result.ImmediateRequeue = true
	return true, nil
}

func preflightDeleteOwnedBatch(ctx context.Context, input workload.ReconcileInput, candidates []deleteBatchCandidate) (map[int32]struct{}, error) {
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return nil, fmt.Errorf("DeleteBatch: owner-aware atomic status adapter is required")
	}
	absent := make(map[int32]struct{})
	mutations := make([]workload.InstanceMutation, 0, len(candidates))
	for _, candidate := range candidates {
		mutations = append(mutations, workload.InstanceMutation{
			Index: candidate.status.Index,
			Mutate: func(*workload.InstanceStatus) bool {
				return false
			},
		})
	}
	if len(mutations) == 0 {
		return absent, nil
	}
	ownerUID := input.OwnerObject.GetUID()
	mutations[0].BatchPrecondition = func(snapshot workload.InstanceMutationSnapshot) bool {
		clear(absent)
		if snapshot.OwnerUID != ownerUID {
			return false
		}
		for _, candidate := range candidates {
			current, found := snapshot.Instances[candidate.status.Index]
			if !found {
				absent[candidate.status.Index] = struct{}{}
				continue
			}
			if current.Incarnation != candidate.status.Incarnation || current.Phase != workload.InstancePhaseDeleting ||
				!status.SameDeleteOperation(current.Operation, candidate.status.Operation) {
				return false
			}
		}
		return true
	}
	if err := input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, "", nil); err != nil {
		return nil, err
	}
	return absent, nil
}

// driveDeleteBatch moves a resumed wave's pods toward gone, in one listed
// order: every pod must carry an observed UID; the pods already
// Terminating run the force-delete clock; every serving pod leaves
// rotation before any pod is deleted; then each row waits for its drain
// and deletes its pods, and a row with no pod left completes. A pod whose
// identity changed under the pass asks for a fresh plan.
func (w *scaleDownWave) driveDeleteBatch(ctx context.Context) (bool, error) {
	steps := []func(context.Context) error{
		w.requirePodUIDs,
		w.readTerminatingPods,
		w.takeOutOfRotation,
		w.deleteDrainedRows,
	}
	for _, step := range steps {
		if err := step(ctx); err != nil {
			if errors.Is(err, podreadiness.ErrPodIdentityChanged) {
				w.result.ImmediateRequeue = true
				return true, nil
			}
			return true, err
		}
	}
	w.recordRequeue()
	return false, nil
}

// requirePodUIDs refuses to drive a wave over a pod observed without a
// UID: every delete is preconditioned on the UID, and a pod without one
// cannot be told from a replacement at the same name.
func (w *scaleDownWave) requirePodUIDs(context.Context) error {
	for _, candidate := range w.selection.candidates {
		for _, pod := range candidate.pods {
			if pod.UID == "" {
				return fmt.Errorf("DeleteBatch: refuse to delete pod %s/%s without an observed UID", pod.Namespace, pod.Name)
			}
		}
	}
	return nil
}

// readTerminatingPods runs the force-delete clock on every pod of the wave
// that is already Terminating, keeping the earliest time the policy wants
// the pass back, and reads per row whether the expectations cache has
// seen every delete the row expected. A clock that cannot be read ends the
// pass only when the scale-down has no poll interval to come back on.
func (w *scaleDownWave) readTerminatingPods(ctx context.Context) error {
	for _, candidate := range w.selection.candidates {
		for _, pod := range candidate.pods {
			if pod.DeletionTimestamp == nil {
				continue
			}
			next, err := escalateStuckTerminatingWithDeadline(ctx, w.deps, w.input, pod, candidate.status.Index)
			if err != nil {
				if w.input.ScaleDownRequeueInterval <= 0 {
					return fmt.Errorf("DeleteBatch: evaluate stuck-Terminating pod %s: %w", pod.Name, err)
				}
				logf.FromContext(ctx).V(1).Info("stuck-Terminating escalation deferred", "pod", pod.Name, "error", err.Error())
			}
			w.policyDeadlineAt = earlierTime(w.policyDeadlineAt, next)
		}
		w.expectationsSatisfied[candidate.status.Index] = w.deps.ExpectationsCache().Satisfied(
			w.input.Key.Namespace, w.input.Key.OwnerName, w.input.Key.Component, candidate.status.Index)
	}
	return nil
}

// takeOutOfRotation writes the serving gate false on every serving pod of
// the wave before any pod is deleted, so a gang leaves traffic whole
// across its Instances.
func (w *scaleDownWave) takeOutOfRotation(ctx context.Context) error {
	for _, candidate := range w.selection.candidates {
		refused, err := w.markRowNotServing(ctx, candidate)
		if err != nil {
			return err
		}
		if refused {
			w.refused[candidate.status.Index] = struct{}{}
		}
	}
	return nil
}

// markRowNotServing takes one row's serving pods out of rotation. It
// reports true when the apiserver refused the write for good and the
// refusal was recorded on the row; the row's other pods are left alone.
func (w *scaleDownWave) markRowNotServing(ctx context.Context, candidate deleteBatchCandidate) (bool, error) {
	index := candidate.status.Index
	for _, pod := range candidate.pods {
		if !podreadiness.IsServing(pod) {
			continue
		}
		err := podreadiness.MarkPodNotServing(ctx, w.deps.Client, w.deps.Reader(), pod, podreadiness.WriterDeleteDrain, deleteDrainKey(index))
		if err == nil {
			continue
		}
		handled, derr := disposeRejectedRowWrite(ctx, w.deps, w.input, index, pod.Name, err)
		if !handled {
			return false, fmt.Errorf("DeleteBatch: mark not serving (instance=%d, pod=%s): %w", index, pod.Name, err)
		}
		if derr != nil {
			return false, fmt.Errorf("DeleteBatch: dispose rejected drain (instance=%d, pod=%s): %w", index, pod.Name, derr)
		}
		return true, nil
	}
	return false, nil
}

// deleteDrainedRows walks every row the wave still drives: a row with no
// pod left completes once its resources are released; a row whose deletes
// the expectations cache has not yet seen waits; a row still routed waits
// for its drain; a drained row has its pods deleted.
func (w *scaleDownWave) deleteDrainedRows(ctx context.Context) error {
	drainer := drain.NewBatcher(w.deps.Reader(), w.input.Key.Namespace)
	w.completed = make([]deleteBatchCandidate, 0)
	for _, candidate := range w.selection.candidates {
		index := candidate.status.Index
		if _, skip := w.refused[index]; skip {
			continue
		}
		if len(candidate.pods) == 0 {
			complete, err := w.finalizeEmptyRow(ctx, index)
			if err != nil {
				return err
			}
			if complete {
				w.completed = append(w.completed, candidate)
			}
			continue
		}
		if !w.expectationsSatisfied[index] {
			continue
		}
		drained, err := w.rowDrained(ctx, drainer, candidate)
		if err != nil {
			return err
		}
		if !drained {
			continue
		}
		refused, err := w.deleteRowPods(ctx, candidate)
		if err != nil {
			return err
		}
		if refused {
			w.refused[index] = struct{}{}
		}
	}
	return nil
}

// finalizeEmptyRow releases the resources of a row with no pod left
// through the adapter, when the adapter has any to release; the row
// completes once they are reported gone.
func (w *scaleDownWave) finalizeEmptyRow(ctx context.Context, index int32) (bool, error) {
	if w.input.FinalizeInstanceResources == nil {
		return true, nil
	}
	complete, err := w.input.FinalizeInstanceResources(ctx, index)
	if err != nil {
		return false, fmt.Errorf("DeleteBatch: finalize resources (instance=%d): %w", index, err)
	}
	return complete, nil
}

// rowDrained reports whether every pod of the row has left its
// per-revision Service, each read through the pass's one EndpointSlice
// observation per Service. A pod with no revision label is routed by no
// per-revision Service and is read as drained.
func (w *scaleDownWave) rowDrained(ctx context.Context, drainer *drain.Batcher, candidate deleteBatchCandidate) (bool, error) {
	drained := true
	for _, pod := range candidate.pods {
		hash := pod.Labels[query.LabelRevisionHash]
		if hash == "" {
			continue
		}
		serviceName := query.PerRevisionServiceName(w.input.Key.OwnerName, w.plan.Component, hash)
		podDrained, err := drainer.IsPodDrained(ctx, serviceName, pod)
		if err != nil {
			return false, fmt.Errorf("DeleteBatch: check drain (instance=%d, pod=%s): %w", candidate.status.Index, pod.Name, err)
		}
		if !podDrained {
			drained = false
		}
	}
	return drained, nil
}

// deleteRowPods deletes the row's pods that are not yet Terminating, each
// under its observed UID, with the delete expected before the call and the
// expectation rolled back when the call fails. A pod already gone, or
// replaced at its name, needs nothing. It reports true when the apiserver
// refused a delete for good and the refusal was recorded on the row.
func (w *scaleDownWave) deleteRowPods(ctx context.Context, candidate deleteBatchCandidate) (bool, error) {
	index := candidate.status.Index
	for _, pod := range candidate.pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		uid := pod.UID
		w.deps.ExpectationsCache().ExpectDeletes(w.input.Key.Namespace, w.input.Key.OwnerName, w.input.Key.Component, index, 1)
		err := w.deps.Client.Delete(ctx, pod, client.Preconditions{UID: &uid})
		if err == nil {
			continue
		}
		w.deps.ExpectationsCache().ObservedDelete(w.input.Key.Namespace, w.input.Key.OwnerName, w.input.Key.Component, index)
		// Absence and UID mismatch both prove the observed Pod identity
		// does not occupy its name.
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			continue
		}
		handled, derr := disposeRejectedRowWrite(ctx, w.deps, w.input, index, pod.Name, err)
		if !handled {
			return false, fmt.Errorf("DeleteBatch: delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}
		if derr != nil {
			return false, fmt.Errorf("DeleteBatch: dispose rejected delete (instance=%d, pod=%s): %w", index, pod.Name, derr)
		}
		return true, nil
	}
	return false, nil
}

// recordRequeue settles what the pass reports after the drive: a wave
// whose every row the apiserver refused holds nothing open (the rows are
// admitted again once the retry record admits them or the refused pod is
// gone), and the force-delete clock sets when the pass runs again.
func (w *scaleDownWave) recordRequeue() {
	if len(w.refused) > 0 && len(w.refused) == len(w.selection.candidates) && w.result.Deferred == 0 {
		w.result.InProgress = false
	}
	if w.policyDeadlineAt.IsZero() {
		return
	}
	remaining := w.policyDeadlineAt.Sub(w.input.Now())
	if remaining <= 0 {
		w.result.PolicyDeadlineDue = true
	} else {
		w.result.RequeueAfter = remaining
	}
}

// reportOverdueDrains tells the operator about every Delete-owned row
// whose drain is past its deadline. It runs after the drive so it reports
// the wave's own effects, and it changes nothing the removal reads: the
// record it writes is not part of the delete identity the removal guard
// tests.
func (w *scaleDownWave) reportOverdueDrains(ctx context.Context) (bool, error) {
	err := announceOverdueDrains(ctx, w.deps, w.input, w.plan, w.podsByInstance)
	if err == nil {
		return false, nil
	}
	if staleWrite(err) {
		w.result.ImmediateRequeue = true
		return true, nil
	}
	return true, err
}

// removeRows completes the wave: every row with no pod left is removed in
// one status transaction fenced on the wave's own operation
// (completeDeleteBatch). A pass that completed no row ends here; a
// committed removal asks for a fresh plan.
func (w *scaleDownWave) removeRows(ctx context.Context) (bool, error) {
	if len(w.completed) == 0 {
		return true, nil
	}
	committed, err := completeDeleteBatch(ctx, w.deps, w.input, w.completed)
	if err != nil {
		if staleWrite(err) {
			w.result.ImmediateRequeue = true
			return true, nil
		}
		return true, fmt.Errorf("DeleteBatch: complete wave: %w", err)
	}
	w.result.ImmediateRequeue = committed
	return true, nil
}

// extrasNotParked drops the leaving rows whose own write the apiserver
// refused: a row a permanent rejection ended, while the revision in force
// denies a fresh attempt and the pod the rejection names still stands in
// the row's set. Such a row holds no wave open and is admitted again once
// the retry record admits or that pod is gone. A row failed by a refused
// create names no standing pod and is taken as any extra.
func extrasNotParked(input workload.ReconcileInput, extras []int32, podsByInstance map[int32][]*corev1.Pod) []int32 {
	kept := make([]int32, 0, len(extras))
	for _, index := range extras {
		row := input.ObservedState.Instance(index)
		if parked, _ := rejectionParked(input, row); parked && refusedPodStands(row, podsByInstance[index]) {
			continue
		}
		kept = append(kept, index)
	}
	return kept
}

// refusedPodStands reports whether the pod row's failure names is in the
// row's current pod set.
func refusedPodStands(row *workload.InstanceStatus, pods []*corev1.Pod) bool {
	if row == nil || row.LastFailure == nil || row.LastFailure.PodName == "" {
		return false
	}
	for _, pod := range pods {
		if pod != nil && pod.Name == row.LastFailure.PodName {
			return true
		}
	}
	return false
}

func earlierTime(current, candidate time.Time) time.Time {
	if candidate.IsZero() {
		return current
	}
	if current.IsZero() || candidate.Before(current) {
		return candidate
	}
	return current
}

func completeDeleteBatch(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, candidates []deleteBatchCandidate) (bool, error) {
	return status.RemoveDeletedBatch(ctx, deps, input, candidateStatuses(candidates))
}

func deleteOwned(status workload.InstanceStatus) bool {
	return workload.Owner(&status) == workload.OwnerDelete
}

// What an elapsed drain deadline does.
//
// A Delete operation carries the same deadline every other operation
// does, but a drain has no backstop to hand it to: the phase must not
// move (the row is on its way out), the delete wave keeps the index,
// and force-deleting a wedged pod is gated on the configured policy
// plus node evidence. What is missing without this pass is the operator
// ever being told. So the deadline is consumed for visibility: one
// Warning naming the Instance, its pods and how far past the deadline
// the drain is, plus a record on the row that readers of the status can
// aggregate. Announced once per episode, keyed by the deadline itself.

// announceOverdueDrains records and reports every delete-owned row
// whose drain is past its operation deadline. The record lands first
// and the event fires from the commit callback, so an announcement
// survives exactly as long as the write that earned it: a failed write
// emits nothing and the next pass re-announces.
//
// Every owned row, not only the wave this pass drives. A row deferred
// behind the pod budget is the one an operator is least likely to
// notice and most likely to be waiting on, and reporting it costs
// nothing: the announcement reads the observation the wave was selected
// from and takes no external effect.
func announceOverdueDrains(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	podsByInstance map[int32][]*corev1.Pod,
) error {
	now := input.Now()
	statuses := input.ObservedState.InstanceStatuses
	mutations := make([]workload.InstanceMutation, 0, len(statuses))
	for _, row := range statuses {
		pods := sortedInstancePods(podsByInstance[row.Index])
		overdue, ok := overdueDrain(row, pods, now)
		if !ok {
			continue
		}
		deadline := row.Operation.Deadline
		index := row.Index
		detail := overdueDrainPodDetail(pods)
		record := &workload.InstanceTermination{
			PodName: overdueDrainPodName(pods),
			Reason:  workload.DrainOverdueReason,
			Message: detail,
			Time:    deadline,
		}
		mutations = append(mutations, workload.InstanceMutation{
			Index:  index,
			Mutate: status.AnnounceDrainOverdue(deadline, record),
			OnCommit: func(*workload.InstanceStatus, *workload.InstanceStatus) {
				workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonDrainOverdue,
					"OMENative %s: drain overdue by %s (deadline %s); %s",
					workload.InstanceKey(plan.Component, index), overdue.Round(time.Second),
					deadline.UTC().Format(time.RFC3339), detail)
			},
		})
	}
	if len(mutations) == 0 {
		return nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return fmt.Errorf("DeleteBatch: owner-aware atomic status adapter is required")
	}
	if err := input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, "", nil); err != nil {
		return fmt.Errorf("DeleteBatch: announce overdue drain: %w", err)
	}
	return nil
}

// sortedInstancePods copies a row's pod bucket into namespace/name
// order, so the names an announcement reports are stable across passes
// whatever order the observation arrived in. The authoritative bucket
// is left untouched.
func sortedInstancePods(pods []*corev1.Pod) []*corev1.Pod {
	out := append([]*corev1.Pod(nil), pods...)
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left == nil || right == nil {
			return left == nil && right != nil
		}
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		return left.Name < right.Name
	})
	return out
}

// overdueDrain reports how far past its deadline a Delete-owned row's
// drain is, and whether it is overdue at all. A row with no pods left
// has nothing to wait on — it is completing this pass — and a zero
// deadline means the operation was never given one.
func overdueDrain(s workload.InstanceStatus, pods []*corev1.Pod, now time.Time) (time.Duration, bool) {
	if !deleteOwned(s) || len(pods) == 0 {
		return 0, false
	}
	deadline := s.Operation.Deadline
	if deadline.IsZero() || !now.After(deadline.Time) {
		return 0, false
	}
	if status.Announced(s, workload.EventReasonDrainOverdue) {
		return 0, false
	}
	return now.Sub(deadline.Time), true
}

// overdueDrainPodDetail names what the drain is still waiting on, as
// the pass observed it. Pods already on their way out are the common
// wedge and are reported as such; pods the wave has not asked to
// terminate are still in rotation, so an operator can tell a stuck
// kubelet from a stuck drain gate.
func overdueDrainPodDetail(pods []*corev1.Pod) string {
	terminating := make([]string, 0, len(pods))
	pending := make([]string, 0, len(pods))
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		if pod.DeletionTimestamp != nil {
			terminating = append(terminating, pod.Name)
			continue
		}
		pending = append(pending, pod.Name)
	}
	parts := make([]string, 0, 2)
	if len(terminating) > 0 {
		parts = append(parts, fmt.Sprintf("%d pod(s) still Terminating (%s)", len(terminating), strings.Join(terminating, ", ")))
	}
	if len(pending) > 0 {
		parts = append(parts, fmt.Sprintf("%d pod(s) still draining (%s)", len(pending), strings.Join(pending, ", ")))
	}
	if len(parts) == 0 {
		return "no pods observed"
	}
	return strings.Join(parts, "; ")
}

// overdueDrainPodName picks the record's representative pod: the first
// pod still Terminating, else the first pod of the row. The slice is
// already sorted by namespace and name, so the choice is stable.
func overdueDrainPodName(pods []*corev1.Pod) string {
	for _, pod := range pods {
		if pod != nil && pod.DeletionTimestamp != nil {
			return pod.Name
		}
	}
	for _, pod := range pods {
		if pod != nil {
			return pod.Name
		}
	}
	return ""
}
