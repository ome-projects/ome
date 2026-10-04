package status

import (
	"context"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ResetOwnsOperation reports whether an operator reset may clear the
// Operation a Failed Instance preserved: none, or a Create / Restart
// attempt — the repair passes' own work, parked when its deadline
// expired. Update and Migrate continuations (and a Delete) belong to the
// rollout and migration machinery: the gang abandon, wreckage cleanup,
// release-held, and migration-expiry paths own them, and clearing one
// from a reset would orphan its surge marker or migration record.
func ResetOwnsOperation(op *types.InstanceOperation) bool {
	if op == nil {
		return true
	}
	switch op.Type {
	case types.InstanceOperationCreate, types.InstanceOperationRestart:
		return true
	}
	return false
}

// ClearFailedInstanceOperation drops the Operation a Failed Instance kept
// when its attempt expired, returning the Instance to the fresh-start
// shape — Failed with no Operation — that the Create pass rebuilds once
// its pods are gone. Phase and LastFailure are untouched. It is the
// operator-reset counterpart of StampFailed: that one parks an attempt
// at Failed, this one releases the parked attempt.
//
// mutate is the adapter's MutateInstance seam. Reports whether a write
// happened: a non-Failed Instance, a Failed Instance with no Operation,
// a fresh-empty slot (Phase=="") from the seam's append path — a status
// deleted out from under the caller — and an Operation the reset does
// not own (ResetOwnsOperation) all write nothing; the ownership check is
// enforced here on the fresh read, whatever the caller classified.
func ClearFailedInstanceOperation(ctx context.Context, mutate func(context.Context, int32, func(*types.InstanceStatus) bool) error, idx int32) (bool, error) {
	cleared := false
	err := mutate(ctx, idx, func(s *types.InstanceStatus) bool {
		cleared = false
		if s.Phase != types.InstancePhaseFailed || s.Operation == nil || !ResetOwnsOperation(s.Operation) {
			return false
		}
		s.Operation = nil
		cleared = true
		return true
	})
	if err != nil {
		return false, err
	}
	return cleared, nil
}

// CloseSpentRepair drops the Restart operation a Failed Instance kept once
// the repair has no live pod left to resume or rebuild, returning the row
// to the fresh-start shape the Create pass rebuilds. Phase, Incarnation
// and LastFailure stay. opID names the attempt the caller read: a row
// whose operation has moved on since — re-armed, reset, or taken by
// another pass — is left alone. Reports whether a write happened.
func CloseSpentRepair(ctx context.Context, input types.ReconcileInput, idx int32, opID string) (bool, error) {
	closed := false
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		closed = false
		if s.Phase != types.InstancePhaseFailed || s.Operation == nil ||
			s.Operation.Type != types.InstanceOperationRestart || s.Operation.ID != opID {
			return false
		}
		s.Operation = nil
		closed = true
		return true
	})
	if err != nil {
		return false, err
	}
	return closed, nil
}
