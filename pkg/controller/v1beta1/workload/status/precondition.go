package status

import (
	"context"
	"fmt"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Identity is the row identity a write was decided against: the attempt
// it read (index, incarnation, phase, the revisions it runs between, the
// active ordinal) and the operation that owns the row (id, type, step,
// request, retry count, reason, node hints, surge index). Runtime
// counters, conditions, node history and operation timestamps are
// outside it: they move without transferring ownership of the action
// the write completes.
//
// One type, one comparison: a write that lands on a row that reads to a
// different identity than the one it was decided against comes from a
// pass that was overtaken.
type Identity struct {
	rowIdentity
	operation operationIdentity
}

// rowIdentity is the part of the identity read off the row itself.
type rowIdentity struct {
	index           int32
	incarnation     int64
	phase           types.InstancePhase
	runningRevision string
	targetRevision  string
	activeOrdinal   int32
}

// operationIdentity is the part read off the row's operation. The node
// hints sit beside the comparable fields because a slice has no equality
// operator. A row without an operation reads to the zero value.
type operationIdentity struct {
	fields          operationFields
	hintTargetNodes []string
}

type operationFields struct {
	present        bool
	id             string
	operationType  types.InstanceOperationType
	step           string
	requestUUID    string
	targetRevision string
	retryCount     int32
	reason         string
	fromNode       string
	surgeIndex     int32
	hasSurgeIndex  bool
}

// Capture reads the identity off a row and keeps its own copy of the node
// hints. A nil row captures the zero identity, which matches only an
// equally empty row.
func Capture(status *types.InstanceStatus) Identity {
	identity := readIdentity(status)
	identity.operation.hintTargetNodes = append([]string(nil), identity.operation.hintTargetNodes...)
	return identity
}

// readIdentity reads the identity fields off a row, sharing the row's
// node hints. It is the one list of the fields an identity is made of.
func readIdentity(status *types.InstanceStatus) Identity {
	if status == nil {
		return Identity{}
	}
	identity := Identity{rowIdentity: rowIdentity{
		index:           status.Index,
		incarnation:     status.Incarnation,
		phase:           status.Phase,
		runningRevision: status.RunningRevision,
		targetRevision:  status.TargetRevision,
		activeOrdinal:   status.ActiveOrdinal,
	}}
	if status.Operation == nil {
		return identity
	}
	identity.operation = operationIdentity{
		fields: operationFields{
			present:        true,
			id:             status.Operation.ID,
			operationType:  status.Operation.Type,
			step:           status.Operation.Step,
			requestUUID:    status.Operation.RequestUUID,
			targetRevision: status.Operation.TargetRevision,
			retryCount:     status.Operation.RetryCount,
			reason:         status.Operation.Reason,
			fromNode:       status.Operation.FromNode,
		},
		hintTargetNodes: status.Operation.HintTargetNodes,
	}
	if status.Operation.SurgeIndex != nil {
		identity.operation.fields.hasSurgeIndex = true
		identity.operation.fields.surgeIndex = *status.Operation.SurgeIndex
	}
	return identity
}

// Matches reports whether status still answers to this identity: the row
// reads to the same identity, field for field.
func (identity Identity) Matches(status types.InstanceStatus) bool {
	current := readIdentity(&status)
	return identity.rowIdentity == current.rowIdentity &&
		identity.operation.fields == current.operation.fields &&
		slices.Equal(identity.operation.hintTargetNodes, current.operation.hintTargetNodes)
}

// holds reports whether the fresh rows still carry, at this identity's
// index, the row it was read from.
func (identity Identity) holds(rows types.InstanceMutationSnapshot) bool {
	current, found := rows.Instances[identity.index]
	return found && identity.Matches(current)
}

// WithStep is this identity with the operation's step replaced, the shape
// a step transition expects to find after its own write. An identity
// without an operation has no step to replace.
func (identity Identity) WithStep(step string) Identity {
	if !identity.operation.fields.present {
		return identity
	}
	after := identity
	after.operation.fields.step = step
	return after
}

// GuardState is what a batch precondition observed about the row it
// guarded: whether the row still matched, and whether it was gone. A
// row that is gone is not a failed precondition — the action the stamp
// was completing has already been completed by its own removal.
type GuardState struct {
	Matched bool
	Absent  bool
}

// Guard builds the batch precondition for a stamp against index: the
// owner must still be the owner the pass read, and the row must either
// still match expected or be gone.
func Guard(
	input types.ReconcileInput,
	index int32,
	expected *types.InstanceStatus,
) (func(types.InstanceMutationSnapshot) bool, *GuardState) {
	identity := Capture(expected)
	ownerUID := input.OwnerObject.GetUID()
	state := &GuardState{}
	return func(snapshot types.InstanceMutationSnapshot) bool {
		state.Matched = false
		state.Absent = false
		if snapshot.OwnerUID != ownerUID {
			return false
		}
		current, found := snapshot.Instances[index]
		if !found {
			state.Absent = true
			return true
		}
		state.Matched = expected != nil && identity.Matches(current)
		return state.Matched
	}, state
}

// Apply commits a batch of row mutations atomically. Every stamp that
// carries an identity precondition goes through here: the precondition
// and the write must land in the same call for the guard to mean
// anything.
func Apply(ctx context.Context, input types.ReconcileInput, mutations []types.InstanceMutation) error {
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return fmt.Errorf("row stamp requires the owner-aware atomic status adapter")
	}
	return input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, "", nil)
}

// RequireOwner reports whether the adapter can carry an owner-fenced
// stamp at all. A stamp with no owner UID to fence on would commit
// against a recreated owner's rows.
func RequireOwner(input types.ReconcileInput) error {
	if input.OwnerObject == nil || input.OwnerObject.GetUID() == "" {
		return fmt.Errorf("row stamp requires a non-empty status owner UID")
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		return fmt.Errorf("row stamp requires the owner-aware atomic status adapter")
	}
	return nil
}

// Clone deep-copies the parts of a row a stamp may hand to another
// writer: the operation and the two slices hanging off the row.
func Clone(status types.InstanceStatus) types.InstanceStatus {
	out := status
	if status.Operation != nil {
		operation := *status.Operation
		operation.HintTargetNodes = append([]string(nil), status.Operation.HintTargetNodes...)
		if status.Operation.SurgeIndex != nil {
			surgeIndex := *status.Operation.SurgeIndex
			operation.SurgeIndex = &surgeIndex
		}
		out.Operation = &operation
	}
	out.NodesOccupied = append([]string(nil), status.NodesOccupied...)
	out.Conditions = append([]metav1.Condition(nil), status.Conditions...)
	out.Announced = append([]string(nil), status.Announced...)
	return out
}
