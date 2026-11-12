package status

import (
	"context"
	"errors"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The guarded write. A pass decides a write from rows it read at its
// start, and by the time the write lands another pass may have moved
// them. So every write here carries a guard, the identities it was
// decided from, and the adapter re-checks the guard on its fresh copy of
// the rows before it applies anything, again on every conflict retry.
// The write then answers one of three ways: it landed, an earlier pass
// had already landed it, or the rows moved and nothing was written. The
// first two are "done"; the third is not an error.

// guardedWrite is one such write: the owner every guard starts from and
// the two ways the write can be done.
type guardedWrite struct {
	ownerUID  k8stypes.UID
	committed bool
	confirmed bool
}

// newGuardedWrite checks that the adapter can carry a guarded write at
// all and takes the owner every guard starts from.
func newGuardedWrite(input types.ReconcileInput) (*guardedWrite, error) {
	if err := RequireOwner(input); err != nil {
		return nil, err
	}
	return &guardedWrite{ownerUID: input.OwnerObject.GetUID()}, nil
}

// guard is the batch precondition: the owner must still be the owner the
// pass read, then check runs on the fresh rows. A confirmation an
// earlier try recorded is dropped first, so check records it afresh.
func (w *guardedWrite) guard(check func(rows types.InstanceMutationSnapshot) bool) func(types.InstanceMutationSnapshot) bool {
	return func(rows types.InstanceMutationSnapshot) bool {
		w.confirmed = false
		if rows.OwnerUID != w.ownerUID {
			return false
		}
		return check(rows)
	}
}

// recordCommit is the OnCommit callback of the mutation the write is
// named for; the adapter calls it once the status write has landed.
func (w *guardedWrite) recordCommit(_, _ *types.InstanceStatus) {
	w.committed = true
}

// stepTransition is the write that moves one row's operation to step and
// nothing else. The row must still read as before, the identity the pass
// decided from; a row already at the step was moved by an earlier pass
// and is confirmed instead of written; any other row is left alone.
// pairHolds, when given, is the rest of the guard, the other row of a
// pair, and is checked first. progressAt, when given, dates the move on
// LastProgressAt.
func (w *guardedWrite) stepTransition(
	before Identity,
	step string,
	pairHolds func(rows types.InstanceMutationSnapshot) bool,
	progressAt func() time.Time,
) types.InstanceMutation {
	after := before.WithStep(step)
	return types.InstanceMutation{
		Index: before.index,
		Mutate: func(row *types.InstanceStatus) bool {
			if after.Matches(*row) {
				w.confirmed = true
				return false
			}
			if !before.Matches(*row) {
				return false
			}
			row.Operation.Step = step
			if progressAt != nil {
				row.Operation.LastProgressAt = metav1.NewTime(progressAt())
			}
			return true
		},
		BatchPrecondition: w.guard(func(rows types.InstanceMutationSnapshot) bool {
			if pairHolds != nil && !pairHolds(rows) {
				return false
			}
			current, found := rows.Instances[before.index]
			if !found {
				return false
			}
			if after.Matches(current) {
				w.confirmed = true
				return true
			}
			return before.Matches(current)
		}),
		Postcondition: func(row *types.InstanceStatus) bool {
			return row != nil && after.Matches(*row)
		},
		OnCommit: w.recordCommit,
	}
}

// commit applies mutations in one status write, with the retry-record
// mutation for retryRevision when one is given, and reports whether the
// write is done: landed by this call, or confirmed already in place.
func (w *guardedWrite) commit(
	ctx context.Context,
	input types.ReconcileInput,
	mutations []types.InstanceMutation,
	retryRevision string,
	mutateRetryBlock func(*types.RetryBlock) types.RetryBlockDisposition,
) (bool, error) {
	err := input.ApplyInstanceMutationsWithRetryBlock(ctx, mutations, retryRevision, mutateRetryBlock)
	return writeOutcome(err, w.committed || w.confirmed)
}

// promote is the write that ends a replacement roll: mutation moves the
// replacement's row Ready on revision under the guard the caller set, and
// once that write has landed the revision's retry record is pruned, as
// every promote onto a revision prunes it. Reports whether this pass
// promoted; a write another pass got ahead of promotes nothing and prunes
// nothing.
func (w *guardedWrite) promote(ctx context.Context, input types.ReconcileInput, mutation types.InstanceMutation, revision string) (bool, error) {
	mutation.OnCommit = w.recordCommit
	promoted, err := w.commit(ctx, input, []types.InstanceMutation{mutation}, "", nil)
	if err != nil || !promoted {
		return false, err
	}
	if err := RetryBlockPruneOnPromote(ctx, input, revision); err != nil {
		return false, err
	}
	return true, nil
}

// promotedRow reports whether row is read, promoted onto revision: the
// same identity as the row read, Ready on revision, with no
// target revision and no operation left.
func promotedRow(row, read *types.InstanceStatus, revision string) bool {
	return row != nil && row.Index == read.Index &&
		row.Incarnation == read.Incarnation &&
		row.ActiveOrdinal == read.ActiveOrdinal &&
		row.Phase == types.InstancePhaseReady &&
		row.RunningRevision == revision &&
		row.TargetRevision == "" && row.Operation == nil
}

// guardHolds runs guard against the fresh rows, writes nothing, and
// reports whether the guard passed. A write with an effect outside the
// status between its decision and its commit asks this first, so the
// effect never runs for rows another pass has moved.
func guardHolds(ctx context.Context, input types.ReconcileInput, index int32, guard func(types.InstanceMutationSnapshot) bool) (bool, error) {
	preflight := types.InstanceMutation{
		Index:             index,
		Mutate:            func(*types.InstanceStatus) bool { return false },
		BatchPrecondition: guard,
	}
	err := input.ApplyInstanceMutationsWithRetryBlock(ctx, []types.InstanceMutation{preflight}, "", nil)
	return writeOutcome(err, true)
}

// writeOutcome turns the adapter's answer into the write's. A guard that
// no longer holds, or an owner that is gone, means another pass got there
// first: the write is not done and that is not an error. Any other error
// is the caller's. Otherwise the write reports done.
func writeOutcome(err error, done bool) (bool, error) {
	if errors.Is(err, types.ErrStatusMutationPrecondition) || errors.Is(err, types.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return done, nil
}
