package status

import (
	"context"
	"fmt"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Identity is the row identity a stamp was decided against: the
// incarnation it read, the operation that owns it (id, type, step,
// request, retry count, reason, node hints, surge pointer) and the
// revisions it runs between. Runtime counters, conditions, node history
// and operation timestamps are deliberately outside it — they move
// without transferring ownership of the action a stamp is completing.
//
// One type, one comparison, stated here rather than re-typed at every
// call site: a stamp that lands on a row no longer answering to the
// identity it was decided against is a write from a pass that has been
// overtaken.
type Identity struct {
	index           int32
	incarnation     int64
	phase           types.InstancePhase
	runningRevision string
	targetRevision  string
	activeOrdinal   int32
	operation       operationIdentity
}

type operationIdentity struct {
	present         bool
	id              string
	operationType   types.InstanceOperationType
	step            string
	requestUUID     string
	targetRevision  string
	retryCount      int32
	reason          string
	fromNode        string
	hintTargetNodes []string
	surgeIndex      int32
	hasSurgeIndex   bool
}

// Capture reads the identity off a row. A nil row captures the zero
// identity, which matches only an equally empty row.
func Capture(status *types.InstanceStatus) Identity {
	identity := Identity{}
	if status == nil {
		return identity
	}
	identity.index = status.Index
	identity.incarnation = status.Incarnation
	identity.phase = status.Phase
	identity.runningRevision = status.RunningRevision
	identity.targetRevision = status.TargetRevision
	identity.activeOrdinal = status.ActiveOrdinal
	if status.Operation == nil {
		return identity
	}
	identity.operation = operationIdentity{
		present:         true,
		id:              status.Operation.ID,
		operationType:   status.Operation.Type,
		step:            status.Operation.Step,
		requestUUID:     status.Operation.RequestUUID,
		targetRevision:  status.Operation.TargetRevision,
		retryCount:      status.Operation.RetryCount,
		reason:          status.Operation.Reason,
		fromNode:        status.Operation.FromNode,
		hintTargetNodes: append([]string(nil), status.Operation.HintTargetNodes...),
	}
	if status.Operation.SurgeIndex != nil {
		identity.operation.hasSurgeIndex = true
		identity.operation.surgeIndex = *status.Operation.SurgeIndex
	}
	return identity
}

// Matches reports whether status still answers to this identity.
func (identity Identity) Matches(status types.InstanceStatus) bool {
	if status.Index != identity.index ||
		status.Incarnation != identity.incarnation ||
		status.Phase != identity.phase ||
		status.RunningRevision != identity.runningRevision ||
		status.TargetRevision != identity.targetRevision ||
		status.ActiveOrdinal != identity.activeOrdinal {
		return false
	}
	if status.Operation == nil {
		return !identity.operation.present
	}
	if !identity.operation.present ||
		status.Operation.ID != identity.operation.id ||
		status.Operation.Type != identity.operation.operationType ||
		status.Operation.Step != identity.operation.step ||
		status.Operation.RequestUUID != identity.operation.requestUUID ||
		status.Operation.TargetRevision != identity.operation.targetRevision ||
		status.Operation.RetryCount != identity.operation.retryCount ||
		status.Operation.Reason != identity.operation.reason ||
		status.Operation.FromNode != identity.operation.fromNode ||
		!slices.Equal(status.Operation.HintTargetNodes, identity.operation.hintTargetNodes) {
		return false
	}
	if status.Operation.SurgeIndex == nil {
		return !identity.operation.hasSurgeIndex
	}
	return identity.operation.hasSurgeIndex && *status.Operation.SurgeIndex == identity.operation.surgeIndex
}

// WithStep is this identity with the operation's step replaced — the
// shape a step transition expects to find after its own write.
func (identity Identity) WithStep(step string) Identity {
	after := identity
	after.operation.step = step
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
