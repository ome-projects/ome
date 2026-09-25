package status

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The writes that end a row, or that a pass repeats while trying to end
// one: the removal itself, the bookkeeping a recycle leaves behind, and
// the announcement an overdue drain earns. All three are preconditioned
// on the identity the pass decided from, because all three are decided
// from a pass-start observation that a concurrent writer may have
// overtaken.

// FinalizeAndRemove applies the shared terminal ordering for lifecycle
// paths outside ordinary scale-down: prove the row is still the row the
// pass selected, let the adapter finalize the Instance's own resources,
// then remove the row. Deployment modes with no per-Instance resources
// keep the plain removal path.
func FinalizeAndRemove(
	ctx context.Context,
	deps types.Deps,
	input types.ReconcileInput,
	index int32,
	expected *types.InstanceStatus,
) (bool, error) {
	if expected != nil && expected.Index != index {
		return false, fmt.Errorf("terminal finalization identity index %d does not match target %d", expected.Index, index)
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil && input.FinalizeInstanceResources == nil {
		if input.RemoveInstance == nil {
			return false, fmt.Errorf("terminal lifecycle requires a status removal adapter")
		}
		if _, err := input.RemoveInstance(ctx, index); err != nil {
			return false, err
		}
		deps.ExpectationsCache().Forget(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, index)
		return true, nil
	}
	if err := RequireOwner(input); err != nil {
		return false, err
	}

	if input.FinalizeInstanceResources != nil {
		guard, state := Guard(input, index, expected)
		preflight := types.InstanceMutation{
			Index:             index,
			Mutate:            func(*types.InstanceStatus) bool { return false },
			BatchPrecondition: guard,
		}
		err := Apply(ctx, input, []types.InstanceMutation{preflight})
		if errors.Is(err, types.ErrStatusMutationPrecondition) || errors.Is(err, types.ErrStatusOwnerGone) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if state.Absent && expected != nil {
			deps.ExpectationsCache().Forget(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, index)
			return true, nil
		}
		if !state.Absent && !state.Matched {
			return false, nil
		}
		complete, err := input.FinalizeInstanceResources(ctx, index)
		if err != nil {
			return false, err
		}
		if !complete {
			return false, nil
		}
	}

	guard, state := Guard(input, index, expected)
	committed := false
	mutation := types.InstanceMutation{
		Index:             index,
		Remove:            true,
		BatchPrecondition: guard,
		OnCommit: func(_, _ *types.InstanceStatus) {
			committed = true
			deps.ExpectationsCache().Forget(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, index)
		},
	}

	err := Apply(ctx, input, []types.InstanceMutation{mutation})
	if errors.Is(err, types.ErrStatusMutationPrecondition) || errors.Is(err, types.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if state.Absent {
		deps.ExpectationsCache().Forget(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, index)
	}
	return committed || state.Absent, nil
}

// RecordRecycleAttempt is the bookkeeping a terminal-pod recycle leaves
// on the row before it deletes anything: the retry count the pacing
// ladder is indexed by, the progress stamp it is anchored on, and the
// dead pod's diagnostics. Written ahead of the deletes so a crash
// between the two can only delay the next recycle, never skip the
// pacing. Reports the recycle number the caller announces.
//
// The operation type is the precondition: a row whose attempt has been
// replaced since the pass read it is not the row this recycle belongs
// to.
func RecordRecycleAttempt(
	ctx context.Context,
	input types.ReconcileInput,
	idx int32,
	opType types.InstanceOperationType,
	now metav1.Time,
	termination *types.InstanceTermination,
) (int32, error) {
	var recycle int32
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Operation == nil || s.Operation.Type != opType {
			return false
		}
		op := *s.Operation
		op.RetryCount++
		op.LastProgressAt = now
		s.Operation = &op
		recycle = op.RetryCount
		if termination != nil {
			captured := *termination
			s.LastFailure = &captured
		}
		return true
	})
	return recycle, err
}

// AnnounceDrainOverdue is the announcement an overdue drain earns: the
// once-only marker that lets the caller emit its Warning exactly once
// per episode, and the record on LastFailure that readers of the status
// can aggregate. Nothing else moves — the phase must not, the row is on
// its way out.
//
// Eligibility is re-tested against the row the write lands on: the
// pass-start observation can be stale, and a row readmitted under a new
// delete operation has a fresh deadline and a fresh announcement due.
func AnnounceDrainOverdue(deadline metav1.Time, record *types.InstanceTermination) func(*types.InstanceStatus) bool {
	return func(s *types.InstanceStatus) bool {
		if s == nil || s.Operation == nil || s.Operation.Type != types.InstanceOperationDelete ||
			!s.Operation.Deadline.Equal(&deadline) {
			return false
		}
		if !markAnnounced(s, types.EventReasonDrainOverdue) {
			return false
		}
		if record != nil {
			captured := *record
			s.LastFailure = &captured
		}
		return true
	}
}
