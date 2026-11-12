package status

import (
	"context"
	"fmt"
	"reflect"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/obsmetrics"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The scale-down stamps. A delete wave is written twice: the admission
// puts every selected row into Deleting under one Delete operation in a
// single transaction, and the completion removes the rows whose pods
// are gone, again in one transaction. Both are fenced on the wave the
// pass selected — the admission on the whole observed table, so a plan
// that moved under the selection admits nothing; the completion on the
// rows still carrying the wave's own operation.

// AttemptFailure is the ladder record a scale-down wave commits with its
// admission. The wave retires a row carrying an in-flight update attempt
// whose own pods already show a workload-caused failure; the attempt ends
// with its row, so the revision's RetryBlock takes the failed wave in the
// same write — once the pods are gone the evidence is too, and a block
// left RetryInProgress with no attempt behind it would admit the next
// start on a survivor as the same attempt.
type AttemptFailure struct {
	TargetRevision string
	Reason         string
	Cause          types.FailureCause
}

// countedFailure is the retry-record half of a write that ends a failed
// attempt with its row: the revision's record takes the failure under the
// policy, in the same status write as the rows. All methods are nil-safe:
// a nil countedFailure names no revision and writes nothing.
type countedFailure struct {
	failure AttemptFailure
	policy  *types.RetryPolicy
	now     metav1.Time
	// held is the attempt count a transition into Held carries, zero
	// otherwise, as the last try of the write left it.
	held int32
}

// counted is the failure as the write counts it on the retry record at
// now, under the policy input carries.
func (f AttemptFailure) counted(input types.ReconcileInput, now metav1.Time) *countedFailure {
	return &countedFailure{failure: f, policy: input.UpdateRetryPolicy, now: now}
}

// revision is the revision whose record takes the failure.
func (c *countedFailure) revision() string {
	if c == nil {
		return ""
	}
	return c.failure.TargetRevision
}

// write is the retry-record mutation of the status write, nil when there
// is no failure to count.
func (c *countedFailure) write() func(*types.RetryBlock) types.RetryBlockDisposition {
	if c == nil {
		return nil
	}
	return func(block *types.RetryBlock) types.RetryBlockDisposition {
		var decision types.RetryBlockDisposition
		decision, c.held = types.ApplyUpdateFailureToRetryBlock(block, c.policy, c.now, c.failure.Reason, c.failure.Cause)
		return decision
	}
}

// heldAttempts is the attempt count a transition into Held carried, read
// after the write landed; zero otherwise.
func (c *countedFailure) heldAttempts() int32 {
	if c == nil {
		return 0
	}
	return c.held
}

// StampDeletingBatch admits one scale-down wave: every row goes to
// Phase=Deleting with a fresh Delete operation at Step=Drain and the
// deadline timeout gives, committed together or not at all. The guard is
// the whole observed table as the pass planned from it — owner UID and
// generation, the same row set, and every row still the row it was. A
// partial commit is an error. A non-nil failure is applied to its
// revision's RetryBlock in the same write, one failed wave on the ladder.
// Reports whether the wave was committed and, when it was, the attempt
// count a new transition into Held carries (zero otherwise).
func StampDeletingBatch(ctx context.Context, input types.ReconcileInput, timeout time.Duration, rows []types.InstanceStatus, failure *AttemptFailure) (bool, int32, error) {
	mutations := make([]types.InstanceMutation, 0, len(rows))
	expected := make(map[int32]types.InstanceStatus, len(rows))
	for _, row := range rows {
		expected[row.Index] = CloneDeleteInstanceStatus(row)
		mutations = append(mutations, deletingMutation(row, metav1.NewTime(input.Now()), timeout))
	}
	mutations[0].BatchPrecondition = deleteAdmissionGuard(input, expected, input.ObservedState.InstanceStatuses)
	if failure == nil {
		committed, err := applyDeleteBatch(ctx, input, mutations, "", nil)
		return committed, 0, err
	}
	counted := failure.counted(input, metav1.NewTime(input.Now()))
	committed, err := applyDeleteBatch(ctx, input, mutations, counted.revision(), counted.write())
	if !committed {
		return false, 0, err
	}
	return true, counted.heldAttempts(), err
}

// deletingMutation admits one row of a wave: Deleting under a fresh Delete
// operation at its drain step, opened at now with the deadline timeout
// gives, and the postcondition that the row written is the row read under
// that operation.
func deletingMutation(row types.InstanceStatus, now metav1.Time, timeout time.Duration) types.InstanceMutation {
	operation := types.InstanceOperation{
		ID:             fmt.Sprintf("delete-%d-%d", row.Index, now.Unix()),
		Type:           types.InstanceOperationDelete,
		Step:           types.DeleteStepDrain,
		StartedAt:      now,
		LastProgressAt: now,
		Deadline:       types.DeadlineAt(now, timeout),
	}
	index, incarnation := row.Index, row.Incarnation
	return types.InstanceMutation{
		Index: index,
		Mutate: func(status *types.InstanceStatus) bool {
			status.Phase = types.InstancePhaseDeleting
			status.Operation = cloneDeleteOperation(&operation)
			return true
		},
		Postcondition: func(status *types.InstanceStatus) bool {
			return status != nil && status.Index == index && status.Incarnation == incarnation &&
				status.Phase == types.InstancePhaseDeleting && status.Operation != nil &&
				status.Operation.ID == operation.ID && status.Operation.Type == operation.Type && status.Operation.Step == operation.Step
		},
	}
}

// RemoveDeletedBatch completes one scale-down wave: every row is removed,
// its delete expectations forgotten and its time-in-wave recorded, in one
// transaction fenced on each row still carrying the wave's own Delete
// operation at its incarnation. Reports whether the wave was removed.
func RemoveDeletedBatch(ctx context.Context, deps types.Deps, input types.ReconcileInput, rows []types.InstanceStatus) (bool, error) {
	expected := make(map[int32]types.InstanceStatus, len(rows))
	mutations := make([]types.InstanceMutation, 0, len(rows))
	for _, row := range rows {
		expected[row.Index] = CloneDeleteInstanceStatus(row)
		index := row.Index
		mutations = append(mutations, types.InstanceMutation{
			Index:  index,
			Remove: true,
			OnCommit: func(previous, _ *types.InstanceStatus) {
				deps.ExpectationsCache().Forget(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, index)
				if previous != nil && previous.Operation != nil && !previous.Operation.StartedAt.IsZero() {
					seconds := input.Now().Sub(previous.Operation.StartedAt.Time).Seconds()
					if seconds >= 0 {
						obsmetrics.RecordScaleDownInstanceDuration(string(input.Key.Component), seconds)
					}
				}
			},
		})
	}
	mutations[0].BatchPrecondition = deleteCompletionGuard(input, expected)
	return applyDeleteBatch(ctx, input, mutations, "", nil)
}

// applyDeleteBatch commits a wave's mutations, with the RetryBlock
// mutation for targetRevision when one is given, and requires every row
// mutation to commit: a wave is gang-atomic, so an adapter that confirmed
// only part of it has broken the contract, not partially succeeded.
func applyDeleteBatch(ctx context.Context, input types.ReconcileInput, mutations []types.InstanceMutation, targetRevision string, mutateRetryBlock func(*types.RetryBlock) types.RetryBlockDisposition) (bool, error) {
	if len(mutations) == 0 {
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return false, fmt.Errorf("DeleteBatch: owner-aware atomic status adapter is required")
	}
	committed := 0
	for i := range mutations {
		callback := mutations[i].OnCommit
		mutations[i].OnCommit = func(previous, current *types.InstanceStatus) {
			committed++
			if callback != nil {
				callback(previous, current)
			}
		}
	}
	if err := input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, targetRevision, mutateRetryBlock); err != nil {
		return false, err
	}
	if committed != len(mutations) {
		return false, fmt.Errorf("DeleteBatch: status adapter confirmed %d of %d mutations", committed, len(mutations))
	}
	return true, nil
}

func deleteAdmissionGuard(input types.ReconcileInput, expected map[int32]types.InstanceStatus, planned []types.InstanceStatus) func(types.InstanceMutationSnapshot) bool {
	uid := input.OwnerObject.GetUID()
	generation := input.OwnerObject.GetGeneration()
	plannedIdentities := make(map[int32]types.InstanceStatus, len(planned))
	for _, status := range planned {
		plannedIdentities[status.Index] = CloneDeleteInstanceStatus(status)
	}
	return func(snapshot types.InstanceMutationSnapshot) bool {
		if snapshot.OwnerUID != uid || snapshot.OwnerGeneration != generation {
			return false
		}
		if len(snapshot.Instances) != len(plannedIdentities) {
			return false
		}
		for index, plannedStatus := range plannedIdentities {
			current, found := snapshot.Instances[index]
			if !found || !sameDeleteCandidate(current, plannedStatus) {
				return false
			}
		}
		for index, planned := range expected {
			current, found := snapshot.Instances[index]
			if !found || !sameDeleteCandidate(current, planned) {
				return false
			}
		}
		return true
	}
}

func deleteCompletionGuard(input types.ReconcileInput, expected map[int32]types.InstanceStatus) func(types.InstanceMutationSnapshot) bool {
	uid := input.OwnerObject.GetUID()
	return func(snapshot types.InstanceMutationSnapshot) bool {
		if snapshot.OwnerUID != uid {
			return false
		}
		for index, planned := range expected {
			current, found := snapshot.Instances[index]
			if !found || current.Incarnation != planned.Incarnation || current.Phase != types.InstancePhaseDeleting ||
				!SameDeleteOperation(current.Operation, planned.Operation) {
				return false
			}
		}
		return true
	}
}

func sameDeleteCandidate(current, planned types.InstanceStatus) bool {
	return current.Index == planned.Index && current.Incarnation == planned.Incarnation && current.Phase == planned.Phase &&
		reflect.DeepEqual(current.Operation, planned.Operation)
}

// SameDeleteOperation reports whether current still carries the Delete
// operation planned: the wave's identity is the operation id.
func SameDeleteOperation(current, planned *types.InstanceOperation) bool {
	return current != nil && planned != nil && current.Type == types.InstanceOperationDelete && planned.Type == types.InstanceOperationDelete &&
		current.ID == planned.ID
}

// CloneDeleteInstanceStatus copies a row far enough for the delete pass
// to hold it across a write: the row and its operation, with the node
// hints the operation carries.
func CloneDeleteInstanceStatus(status types.InstanceStatus) types.InstanceStatus {
	copy := status
	copy.Operation = cloneDeleteOperation(status.Operation)
	return copy
}

func cloneDeleteOperation(operation *types.InstanceOperation) *types.InstanceOperation {
	if operation == nil {
		return nil
	}
	copy := *operation
	copy.HintTargetNodes = append([]string(nil), operation.HintTargetNodes...)
	return &copy
}
