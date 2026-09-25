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

// DeleteBatch advances one durable, gang-atomic scale-down wave. Fresh
// candidates are committed as one status transaction and never receive an
// external effect in the admission pass. Persisted Delete-owned candidates
// resume from the authoritative Pod snapshot on later passes.
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
	selection, err := selectDeleteBatch(input.ObservedState.InstanceStatuses, extras, podsByInstance, input.ScaleDownPodBatchSize)
	if err != nil {
		return DeleteBatchResult{}, err
	}
	result := DeleteBatchResult{
		InProgress: len(selection.candidates) > 0 || selection.deferred > 0,
		Deferred:   selection.deferred,
		Oversized:  selection.oversized,
	}
	for _, candidate := range selection.candidates {
		result.SelectedPodCost += candidate.cost
	}
	obsmetrics.SetScaleDownActivePods(input.Key.Namespace, input.Key.OwnerName, string(plan.Component), result.SelectedPodCost)
	obsmetrics.SetScaleDownDeferredInstances(input.Key.Namespace, input.Key.OwnerName, string(plan.Component), result.Deferred)
	admittedInstances := 0
	defer func() {
		budgetValue := any("unbounded")
		if input.ScaleDownPodBatchSize != nil {
			budgetValue = *input.ScaleDownPodBatchSize
		}
		logf.FromContext(ctx).Info("OMENative scale-down wave",
			"namespace", input.Key.Namespace,
			"isvc", input.Key.OwnerName,
			"component", plan.Component,
			"podBudget", budgetValue,
			"activePodCost", result.SelectedPodCost,
			"activeInstances", len(selection.candidates),
			"admittedInstances", admittedInstances,
			"deferredInstances", result.Deferred)
	}()
	if len(selection.candidates) == 0 {
		return result, nil
	}
	if selection.fresh {
		committed, err := admitDeleteBatch(ctx, input, plan, selection.candidates)
		if err != nil {
			if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
				result.ImmediateRequeue = true
				return result, nil
			}
			return DeleteBatchResult{}, fmt.Errorf("DeleteBatch: admit wave: %w", err)
		}
		result.ImmediateRequeue = committed
		if committed {
			admittedInstances = len(selection.candidates)
			obsmetrics.RecordScaleDownBatchPods(string(plan.Component), result.SelectedPodCost)
			if result.Oversized {
				obsmetrics.RecordScaleDownOversizedBatch(string(plan.Component))
			}
		}
		if result.ImmediateRequeue || !committed {
			return result, nil
		}
	} else {
		absent, err := preflightDeleteOwnedBatch(ctx, input, selection.candidates)
		if err != nil {
			if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
				result.ImmediateRequeue = true
				return result, nil
			}
			return DeleteBatchResult{}, fmt.Errorf("DeleteBatch: verify owned wave: %w", err)
		}
		if len(absent) > 0 {
			for index := range absent {
				deps.ExpectationsCache().Forget(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, index)
			}
			result.ImmediateRequeue = true
			return result, nil
		}
	}

	completed, requeueAt, err := driveDeleteBatch(ctx, deps, input, plan, selection.candidates)
	if err != nil {
		if errors.Is(err, podreadiness.ErrPodIdentityChanged) {
			result.ImmediateRequeue = true
			return result, nil
		}
		return DeleteBatchResult{}, err
	}
	if !requeueAt.IsZero() {
		remaining := requeueAt.Sub(input.Now())
		if remaining <= 0 {
			result.PolicyDeadlineDue = true
		} else {
			result.RequeueAfter = remaining
		}
	}
	// Visibility for the rows this wave is still waiting on. Runs after
	// the drive so it reports the wave's own effects, and it changes
	// nothing the completion below reads: the record it writes is not
	// part of the delete identity the completion guard tests.
	if err := announceOverdueDrains(ctx, deps, input, plan, podsByInstance); err != nil {
		if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
			result.ImmediateRequeue = true
			return result, nil
		}
		return DeleteBatchResult{}, err
	}
	if len(completed) == 0 {
		return result, nil
	}
	committed, err := completeDeleteBatch(ctx, deps, input, completed)
	if err != nil {
		if errors.Is(err, workload.ErrStatusMutationPrecondition) || errors.Is(err, workload.ErrStatusOwnerGone) {
			result.ImmediateRequeue = true
			return result, nil
		}
		return DeleteBatchResult{}, fmt.Errorf("DeleteBatch: complete wave: %w", err)
	}
	result.ImmediateRequeue = committed
	return result, nil
}

func selectDeleteBatch(
	statuses []workload.InstanceStatus,
	extras []int32,
	podsByInstance map[int32][]*corev1.Pod,
	budget *int32,
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
		sort.Slice(pool, func(i, j int) bool { return pool[i].status.Index > pool[j].status.Index })
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
	return status.StampDeletingBatch(ctx, input, plan.InstanceReadyTimeout, candidateStatuses(candidates))
}

// candidateStatuses is the rows of a wave, in selection order.
func candidateStatuses(candidates []deleteBatchCandidate) []workload.InstanceStatus {
	rows := make([]workload.InstanceStatus, 0, len(candidates))
	for _, candidate := range candidates {
		rows = append(rows, candidate.status)
	}
	return rows
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

func driveDeleteBatch(
	ctx context.Context,
	deps workload.Deps,
	input workload.ReconcileInput,
	plan workload.ComponentPlan,
	candidates []deleteBatchCandidate,
) ([]deleteBatchCandidate, time.Time, error) {
	for _, candidate := range candidates {
		for _, pod := range candidate.pods {
			if pod.UID == "" {
				return nil, time.Time{}, fmt.Errorf("DeleteBatch: refuse to delete pod %s/%s without an observed UID", pod.Namespace, pod.Name)
			}
		}
	}
	expectationsSatisfied := make(map[int32]bool, len(candidates))
	var requeueAt time.Time
	for _, candidate := range candidates {
		for _, pod := range candidate.pods {
			if pod.DeletionTimestamp != nil {
				next, err := escalateStuckTerminatingWithDeadline(ctx, deps, input, pod, candidate.status.Index)
				if err != nil {
					if input.ScaleDownRequeueInterval <= 0 {
						return nil, time.Time{}, fmt.Errorf("DeleteBatch: evaluate stuck-Terminating pod %s: %w", pod.Name, err)
					}
					logf.FromContext(ctx).V(1).Info("stuck-Terminating escalation deferred", "pod", pod.Name, "error", err.Error())
				}
				requeueAt = earlierTime(requeueAt, next)
			}
		}
		expectationsSatisfied[candidate.status.Index] = deps.ExpectationsCache().Satisfied(
			input.Key.Namespace, input.Key.OwnerName, input.Key.Component, candidate.status.Index)
	}

	// Every serving hold in the selected wave lands before any member Pod can
	// be deleted, preserving gang drain atomicity across Instance boundaries.
	for _, candidate := range candidates {
		for _, pod := range candidate.pods {
			if !podreadiness.IsServing(pod) {
				continue
			}
			if err := podreadiness.MarkPodNotServing(ctx, deps.Client, deps.Reader(), pod,
				podreadiness.WriterDeleteDrain, deleteDrainKey(candidate.status.Index)); err != nil {
				return nil, time.Time{}, fmt.Errorf("DeleteBatch: mark not serving (instance=%d, pod=%s): %w", candidate.status.Index, pod.Name, err)
			}
		}
	}

	drainer := drain.NewBatcher(deps.Reader(), input.Key.Namespace)
	completed := make([]deleteBatchCandidate, 0)
	for _, candidate := range candidates {
		if len(candidate.pods) == 0 {
			if input.FinalizeInstanceResources != nil {
				complete, err := input.FinalizeInstanceResources(ctx, candidate.status.Index)
				if err != nil {
					return nil, time.Time{}, fmt.Errorf("DeleteBatch: finalize resources (instance=%d): %w", candidate.status.Index, err)
				}
				if !complete {
					continue
				}
			}
			completed = append(completed, candidate)
			continue
		}
		if !expectationsSatisfied[candidate.status.Index] {
			continue
		}
		drained := true
		for _, pod := range candidate.pods {
			hash := pod.Labels[query.LabelRevisionHash]
			if hash == "" {
				continue
			}
			serviceName := query.PerRevisionServiceName(input.Key.OwnerName, plan.Component, hash)
			podDrained, err := drainer.IsPodDrained(ctx, serviceName, pod)
			if err != nil {
				return nil, time.Time{}, fmt.Errorf("DeleteBatch: check drain (instance=%d, pod=%s): %w", candidate.status.Index, pod.Name, err)
			}
			if !podDrained {
				drained = false
			}
		}
		if !drained {
			continue
		}
		for _, pod := range candidate.pods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			uid := pod.UID
			deps.ExpectationsCache().ExpectDeletes(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, candidate.status.Index, 1)
			if err := deps.Client.Delete(ctx, pod, client.Preconditions{UID: &uid}); err != nil {
				deps.ExpectationsCache().ObservedDelete(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, candidate.status.Index)
				// Absence and UID mismatch both prove the observed Pod identity
				// does not occupy its name.
				if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
					continue
				}
				return nil, time.Time{}, fmt.Errorf("DeleteBatch: delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
	}
	return completed, requeueAt, nil
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
