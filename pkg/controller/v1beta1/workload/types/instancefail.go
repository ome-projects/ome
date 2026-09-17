package types

import (
	"context"
)

// FailInstanceClearingOperation stamps Phase=Failed AND clears the
// in-flight Operation in one MutateInstance call — the abandon-analogue
// for single-pod / create attempts. Failed-with-no-Operation hands control
// back to operation-specific recovery on a later reconcile; clearing the
// Operation prevents the current attempt's stamper from extending it. A
// fresh-empty slot (Phase=="") from MutateInstance's append path is a
// sentinel for a slot deleted out from under us — don't resurrect.
// termination, when non-nil, is recorded on LastFailure in the same write.
//
// Lives in the leaf types package so both the workload-root dispositions
// and workload/ops (the apiserver-rejection path) share ONE implementation
// without closing the workload → workload/ops import cycle.
func FailInstanceClearingOperation(ctx context.Context, input ReconcileInput, idx int32, termination *InstanceTermination) error {
	return input.MutateInstance(ctx, idx, func(s *InstanceStatus) bool {
		if s.Phase == "" {
			return false
		}
		if s.Phase == InstancePhaseFailed && s.Operation == nil {
			return false
		}
		s.Phase = InstancePhaseFailed
		s.Operation = nil
		if termination != nil {
			captured := *termination
			s.LastFailure = &captured
		}
		return true
	})
}
