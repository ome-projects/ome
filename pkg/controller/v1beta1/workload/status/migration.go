package status

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The migration stamps. A migration writes a PAIR of rows: the SOURCE
// goes to Migrating pinned to its surge, the SURGE is seeded Creating
// pinned back to its source, and both carry the request's UUID so every
// later write can prove it is acting on the pair it read. The pin is the
// only fact on the row; migration facts live on the owner's
// status.migrations record.

// MigrationRole picks which side of the migration pair a pin is written
// for — source goes to Phase=Migrating with sibling=surge; surge goes to
// Phase=Creating with sibling=source and Incarnation seeded.
type MigrationRole int

const (
	MigrationRoleSource MigrationRole = iota
	MigrationRoleSurge
)

// StampMigrationPin writes one side of the migration pair. Idempotent on
// (Phase, Operation.Type, RequestUUID).
func StampMigrationPin(ctx context.Context, input types.ReconcileInput, idx, siblingIdx int32, role MigrationRole, uuid string, timeout time.Duration) error {
	mutation := MigrationPinMutation(input, idx, siblingIdx, role, uuid, timeout)
	return input.MutateInstance(ctx, mutation.Index, mutation.Mutate)
}

// MigrationPinMutation is the migration pin as a mutation, for the pass
// that commits both sides of the pair in one batch. The Operation it
// writes is a pin — Type + RequestUUID (+ SurgeIndex for pair
// correlation) plus the timing fields the deadline machinery reads;
// migration facts (FromNode, hints, reason) live on the owner's
// status.migrations record, not on the row.
func MigrationPinMutation(input types.ReconcileInput, idx, siblingIdx int32, role MigrationRole, uuid string, timeout time.Duration) types.InstanceMutation {
	now := metav1.NewTime(input.Now())
	siblingPtr := siblingIdx
	wantPhase := types.InstancePhaseMigrating
	idSuffix := ""
	if role == MigrationRoleSurge {
		wantPhase = types.InstancePhaseCreating
		idSuffix = "-surge"
	}
	return types.InstanceMutation{Index: idx, Mutate: func(s *types.InstanceStatus) bool {
		if role == MigrationRoleSource && s.Phase == "" {
			// Fresh-empty slot from the append path: the source status
			// was removed out from under us — don't resurrect it. The
			// surge role legitimately seeds a fresh slot.
			return false
		}
		if s.Phase == wantPhase &&
			s.Operation != nil && s.Operation.Type == types.InstanceOperationMigrate &&
			s.Operation.RequestUUID == uuid {
			return false
		}
		if role == MigrationRoleSurge && s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Phase = wantPhase
		s.Operation = &types.InstanceOperation{
			ID:             fmt.Sprintf("migrate-%s%s-%d", uuid, idSuffix, now.Unix()),
			Type:           types.InstanceOperationMigrate,
			Step:           "CreateSurge",
			RequestUUID:    uuid,
			SurgeIndex:     &siblingPtr,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       types.DeadlineAt(now, timeout),
		}
		return true
	}}
}

// StartMigration commits both sides of the pair in one batch: the
// source pin and the surge pin, fenced on the identities the pass
// decided from. Reports whether the pair is established afterwards —
// by this write, or because a previous pass already wrote it.
func StartMigration(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	surge *types.InstanceStatus,
	requestUUID string,
	surgeIdx int32,
	timeout time.Duration,
) (bool, error) {
	if err := RequireOwner(input); err != nil {
		return false, err
	}
	ownerUID := input.OwnerObject.GetUID()
	sourceGuard, sourceState := Guard(input, source.Index, source)
	surgeGuard, surgeState := Guard(input, surgeIdx, surge)
	confirmed := false
	batchGuard := func(snapshot types.InstanceMutationSnapshot) bool {
		if snapshot.OwnerUID != ownerUID {
			confirmed = false
			return false
		}
		currentSource, sourceFound := snapshot.Instances[source.Index]
		currentSurge, surgeFound := snapshot.Instances[surgeIdx]
		currentSourceOwned := sourceFound && MigrationSourceOwnsRemoval(&currentSource, requestUUID, surgeIdx)
		currentSurgeOwned := surgeFound && MigrationSurgeOwnsPromotion(&currentSurge, requestUUID, source.Index)
		if (currentSourceOwned && currentSurgeOwned) ||
			(surge == nil && currentSourceOwned && !surgeFound) {
			confirmed = true
			return true
		}
		confirmed = sourceGuard(snapshot) && sourceState.Matched && surgeGuard(snapshot)
		if surge == nil {
			confirmed = confirmed && surgeState.Absent
		} else {
			confirmed = confirmed && surgeState.Matched
		}
		return confirmed
	}

	sourceMutation := MigrationPinMutation(input, source.Index, surgeIdx, MigrationRoleSource, requestUUID, timeout)
	sourceMutation.BatchPrecondition = batchGuard
	sourceMutation.Postcondition = func(row *types.InstanceStatus) bool {
		return MigrationSourceOwnsRemoval(row, requestUUID, surgeIdx)
	}
	surgeMutation := MigrationPinMutation(input, surgeIdx, source.Index, MigrationRoleSurge, requestUUID, timeout)
	surgeMutation.Postcondition = func(row *types.InstanceStatus) bool {
		return MigrationSurgeOwnsPromotion(row, requestUUID, source.Index)
	}
	err := Apply(ctx, input, []types.InstanceMutation{sourceMutation, surgeMutation})
	if errors.Is(err, types.ErrStatusMutationPrecondition) || errors.Is(err, types.ErrStatusOwnerGone) {
		return false, nil
	}
	return confirmed, err
}

// StampMigrationSurgeReady is the migration's promote terminator: the
// surge goes Ready on targetRevision with its pin cleared, fenced on
// both sides of the pair still being the pair the pass read. Success at
// targetRevision also prunes its RetryBlock. Reports whether this pass
// promoted.
func StampMigrationSurgeReady(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	surge *types.InstanceStatus,
	requestUUID string,
	targetRevision string,
) (bool, error) {
	if err := RequireOwner(input); err != nil {
		return false, err
	}
	sourceGuard, sourceState := Guard(input, source.Index, source)
	surgeGuard, surgeState := Guard(input, surge.Index, surge)
	mutation := ReadyOnRevisionMutation(surge.Index, targetRevision, input.Now())
	promoted := false
	mutation.BatchPrecondition = func(snapshot types.InstanceMutationSnapshot) bool {
		if !sourceGuard(snapshot) || !sourceState.Matched ||
			!surgeGuard(snapshot) || !surgeState.Matched {
			return false
		}
		currentSource := snapshot.Instances[source.Index]
		currentSurge := snapshot.Instances[surge.Index]
		return MigrationSourceOwnsRemoval(&currentSource, requestUUID, surge.Index) &&
			MigrationSurgeOwnsPromotion(&currentSurge, requestUUID, source.Index)
	}
	mutation.Postcondition = func(row *types.InstanceStatus) bool {
		return row != nil && row.Index == surge.Index &&
			row.Incarnation == surge.Incarnation && row.ActiveOrdinal == surge.ActiveOrdinal &&
			MigrationPromotedSurgeMatches(row, targetRevision)
	}
	mutation.OnCommit = func(_, _ *types.InstanceStatus) {
		promoted = true
	}
	err := Apply(ctx, input, []types.InstanceMutation{mutation})
	if errors.Is(err, types.ErrStatusMutationPrecondition) || errors.Is(err, types.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil || !promoted {
		return false, err
	}

	if err := RetryBlockPruneOnPromote(ctx, input, targetRevision); err != nil {
		return false, err
	}
	return true, nil
}

// ClearMigrationPin drops the Migrate pin for uuid from the row at idx,
// leaving the rest of the status untouched. No-op when the slot is gone
// or the pin is absent, another type, or another request's.
func ClearMigrationPin(ctx context.Context, input types.ReconcileInput, idx int32, uuid string) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" {
			return false
		}
		if s.Operation == nil || s.Operation.Type != types.InstanceOperationMigrate || s.Operation.RequestUUID != uuid {
			return false
		}
		s.Operation = nil
		return true
	})
}

// StampMigrationSourceRestored returns an expired migration's source to
// the phase its live pods justify — Ready when every pod is runtime-ready,
// Failed with the given record otherwise — and drops the request's pin
// in the same write. Whichever phase the row already reads is left as
// it is, so a repeated restore writes nothing. A fresh-empty slot is a
// source removed out from under the pass and is not resurrected.
func StampMigrationSourceRestored(ctx context.Context, input types.ReconcileInput, sourceIdx int32, uuid string, healthy bool, failureReason, failureMessage string) error {
	now := metav1.NewTime(input.Now())
	return input.MutateInstance(ctx, sourceIdx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" {
			// Fresh-empty slot from the append path: the status was
			// deleted out from under us — don't resurrect.
			return false
		}
		changed := false
		if s.Operation != nil && s.Operation.Type == types.InstanceOperationMigrate && s.Operation.RequestUUID == uuid {
			s.Operation = nil
			changed = true
		}
		want := types.InstancePhaseReady
		if !healthy {
			want = types.InstancePhaseFailed
		}
		if s.Phase != want {
			s.Phase = want
			if !healthy {
				s.LastFailure = &types.InstanceTermination{
					Reason:  failureReason,
					Message: failureMessage,
					Time:    now,
				}
			}
			changed = true
		}
		return changed
	})
}

// MigrationSourceOwnsRemoval reports the source side of a live pair: a
// Migrating row on a recorded revision, pinned to requestUUID and to the
// surge at surgeIdx.
func MigrationSourceOwnsRemoval(row *types.InstanceStatus, requestUUID string, surgeIdx int32) bool {
	return row != nil && row.Phase == types.InstancePhaseMigrating &&
		row.RunningRevision != "" && row.TargetRevision == "" && row.Operation != nil &&
		row.Operation.Type == types.InstanceOperationMigrate &&
		row.Operation.RequestUUID == requestUUID &&
		row.Operation.SurgeIndex != nil && *row.Operation.SurgeIndex == surgeIdx
}

// MigrationSurgeOwnsPromotion reports the surge side of a live pair: a
// first-incarnation Creating row with no revision yet, pinned to
// requestUUID and back to the source at sourceIdx.
func MigrationSurgeOwnsPromotion(row *types.InstanceStatus, requestUUID string, sourceIdx int32) bool {
	return row != nil && row.Incarnation == 1 && row.ActiveOrdinal == 0 &&
		row.Phase == types.InstancePhaseCreating && row.RunningRevision == "" &&
		row.TargetRevision == "" && row.Operation != nil &&
		row.Operation.Type == types.InstanceOperationMigrate &&
		row.Operation.RequestUUID == requestUUID &&
		row.Operation.SurgeIndex != nil && *row.Operation.SurgeIndex == sourceIdx
}

// MigrationPromotedSurgeMatches reports a surge the promote terminator
// has already landed on: first incarnation, Ready on targetRevision,
// pin cleared.
func MigrationPromotedSurgeMatches(row *types.InstanceStatus, targetRevision string) bool {
	return row != nil && row.Incarnation == 1 && row.ActiveOrdinal == 0 &&
		row.Phase == types.InstancePhaseReady &&
		row.RunningRevision == targetRevision && row.TargetRevision == "" && row.Operation == nil
}
