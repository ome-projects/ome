package status

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The gang-surge stamps. A gang surge is the one update whose two rows
// are written together: the SOURCE carries the operation and its
// deadline, the TARGET index carries the marker that pins the surge slot
// in the plan. They commit in one batch so no pass can observe a claim
// with no marker behind it.

// stampGangSurging stamps the SOURCE row of a gang surge:
// Phase=Updating, Op{Update, Step=Surge, SurgeIndex=k}. Step=Surge is
// what makes surge accounting and the coordination surge gate count the
// operation against MaxSurge; the SurgeIndex pointer distinguishes a
// gang surge (a new index) from a single-pod surge (an ordinal toggle).
// Idempotent.
func stampGangSurging(ctx context.Context, input types.ReconcileInput, idx, surgeIdx int32, targetRev string, strategy types.UpdateStrategyType, timeout time.Duration) error {
	now := metav1.NewTime(input.Now())
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		return stampGangSurgeSource(s, idx, surgeIdx, targetRev, strategy, timeout, now)
	})
	if err != nil {
		return err
	}
	return RetryBlockAttemptStarted(ctx, input, targetRev)
}

// StampGangSurgeTarget stamps the TARGET index of a gang surge:
// Phase=Creating, Op{Update, Step=GangSurgeTarget}. The same attempt
// stampGangSurging already flipped the RetryBlock for, so there is no
// flip here — once per attempt, on the source side. The marker pins the
// index in the plan so the gang pass creates its PodGroup and scale-down
// will not delete it. Creates the row if absent. Idempotent.
func StampGangSurgeTarget(ctx context.Context, input types.ReconcileInput, surgeIdx int32, targetRev string, timeout time.Duration) error {
	now := metav1.NewTime(input.Now())
	return input.MutateInstance(ctx, surgeIdx, func(s *types.InstanceStatus) bool {
		return stampGangSurgeTarget(s, surgeIdx, targetRev, timeout, now)
	})
}

func stampGangSurgeSource(s *types.InstanceStatus, idx, surgeIdx int32, targetRev string, strategy types.UpdateStrategyType, timeout time.Duration, now metav1.Time) bool {
	if s.Phase == types.InstancePhaseUpdating && s.Operation != nil &&
		s.Operation.Type == types.InstanceOperationUpdate &&
		s.Operation.Step == types.UpdateStepSurge && s.Operation.SurgeIndex != nil &&
		*s.Operation.SurgeIndex == surgeIdx && s.TargetRevision == targetRev {
		return false
	}
	k := surgeIdx
	s.Phase = types.InstancePhaseUpdating
	s.TargetRevision = targetRev
	s.Operation = &types.InstanceOperation{
		ID:             fmt.Sprintf("gangsurge-%d-%d", idx, now.Unix()),
		Type:           types.InstanceOperationUpdate,
		Step:           types.UpdateStepSurge,
		SurgeIndex:     &k,
		TargetRevision: targetRev,
		Strategy:       strategy,
		StartedAt:      now,
		LastProgressAt: now,
		Deadline:       types.DeadlineAt(now, timeout),
	}
	return true
}

func stampGangSurgeTarget(s *types.InstanceStatus, surgeIdx int32, targetRev string, timeout time.Duration, now metav1.Time) bool {
	if GangSurgeTargetClaimMatches(s, targetRev) || !EmptyGangSurgeTargetSlot(s) {
		return false
	}
	claim := gangSurgeTargetRow(surgeIdx, targetRev, types.UpdateStepGangSurgeTarget, timeout, now)
	s.Incarnation = claim.Incarnation
	s.Phase = claim.Phase
	s.TargetRevision = claim.TargetRevision
	s.Operation = claim.Operation
	return true
}

// gangSurgeTargetRow is the row a gang surge's target index carries while
// the slot is claimed: first incarnation, Creating toward targetRevision,
// under an Update operation at step with the deadline timeout gives.
func gangSurgeTargetRow(surgeIdx int32, targetRevision, step string, timeout time.Duration, now metav1.Time) types.InstanceStatus {
	return types.InstanceStatus{
		Index:          surgeIdx,
		Incarnation:    1,
		Phase:          types.InstancePhaseCreating,
		TargetRevision: targetRevision,
		Operation: &types.InstanceOperation{
			ID:             fmt.Sprintf("gangsurgetarget-%d-%d", surgeIdx, now.Unix()),
			Type:           types.InstanceOperationUpdate,
			Step:           step,
			TargetRevision: targetRevision,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       types.DeadlineAt(now, timeout),
		},
	}
}

// StartGangSurge commits the source claim and the target marker in one
// batch, fenced on the source identity the pass decided from. Reports
// whether this pass is the one that opened the surge.
func StartGangSurge(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	surgeIdx int32,
	targetRev string,
	strategy types.UpdateStrategyType,
	timeout time.Duration,
) (bool, error) {
	if source == nil {
		return false, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		if err := stampGangSurging(ctx, input, source.Index, surgeIdx, targetRev, strategy, timeout); err != nil {
			return false, err
		}
		if err := StampGangSurgeTarget(ctx, input, surgeIdx, targetRev, timeout); err != nil {
			return false, err
		}
		return true, nil
	}
	write, err := newGuardedWrite(input)
	if err != nil {
		return false, err
	}

	now := metav1.NewTime(input.Now())
	sourceIdentity := Capture(source)
	desiredSource := Clone(*source)
	stampGangSurgeSource(&desiredSource, source.Index, surgeIdx, targetRev, strategy, timeout, now)
	desiredSourceIdentity := Capture(&desiredSource)
	desiredTarget := types.InstanceStatus{Index: surgeIdx}
	stampGangSurgeTarget(&desiredTarget, surgeIdx, targetRev, timeout, now)
	desiredTargetIdentity := Capture(&desiredTarget)
	sourceMutation := types.InstanceMutation{
		Index: source.Index,
		Mutate: func(status *types.InstanceStatus) bool {
			return stampGangSurgeSource(status, source.Index, surgeIdx, targetRev, strategy, timeout, now)
		},
		BatchPrecondition: write.guard(func(rows types.InstanceMutationSnapshot) bool {
			currentTarget, found := rows.Instances[surgeIdx]
			return sourceIdentity.holds(rows) && (!found || EmptyGangSurgeTargetSlot(&currentTarget))
		}),
		Postcondition: func(status *types.InstanceStatus) bool {
			return status != nil && desiredSourceIdentity.Matches(*status)
		},
		OnCommit: write.recordCommit,
	}
	targetMutation := types.InstanceMutation{
		Index: surgeIdx,
		Mutate: func(status *types.InstanceStatus) bool {
			return stampGangSurgeTarget(status, surgeIdx, targetRev, timeout, now)
		},
		Postcondition: func(status *types.InstanceStatus) bool {
			return status != nil && desiredTargetIdentity.Matches(*status)
		},
	}
	return write.commit(ctx, input, []types.InstanceMutation{sourceMutation, targetMutation}, targetRev, RetryBlockStartAttempt)
}

// StampGangSurgeDrainStep moves a gang surge's SOURCE from Step=Surge to
// Step=SurgeDrain, fenced on both rows of the pair still being the pair
// the pass read: the source identity, and the target still carrying the
// live marker for the source's revision. Reports whether the source is
// on SurgeDrain afterwards — by this write or a previous one.
func StampGangSurgeDrainStep(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	target *types.InstanceStatus,
) (bool, error) {
	write, err := newGuardedWrite(input)
	if err != nil {
		return false, err
	}

	targetIdentity := Capture(target)
	targetHolds := func(rows types.InstanceMutationSnapshot) bool {
		currentTarget := rows.Instances[target.Index]
		return targetIdentity.holds(rows) &&
			GangSurgeActiveTargetClaimMatches(&currentTarget, source.Operation.TargetRevision)
	}
	step := write.stepTransition(Capture(source), types.UpdateStepSurgeDrain, targetHolds, input.Now)
	return write.commit(ctx, input, []types.InstanceMutation{step}, "", nil)
}

// StampGangSurgeTargetReady is the gang surge's promote terminator: the
// TARGET goes Ready on targetRevision with its marker cleared, fenced on
// the source identity, the target identity and the live marker. Success
// at targetRevision also prunes its RetryBlock. Reports whether this
// pass promoted.
func StampGangSurgeTargetReady(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	target *types.InstanceStatus,
	targetRevision string,
) (bool, error) {
	write, err := newGuardedWrite(input)
	if err != nil {
		return false, err
	}

	sourceIdentity := Capture(source)
	targetIdentity := Capture(target)
	mutation := ReadyOnRevisionMutation(target.Index, targetRevision, input.Now())
	mutation.BatchPrecondition = write.guard(func(rows types.InstanceMutationSnapshot) bool {
		currentTarget := rows.Instances[target.Index]
		return sourceIdentity.holds(rows) && targetIdentity.holds(rows) &&
			GangSurgeActiveTargetClaimMatches(&currentTarget, targetRevision)
	})
	mutation.Postcondition = func(row *types.InstanceStatus) bool {
		return promotedRow(row, target, targetRevision)
	}
	return write.promote(ctx, input, mutation, targetRevision)
}

// ResetGangSurgeSource releases a source claim whose target slot turned
// out to be occupied: the source goes back to Ready on its running
// revision, or to the fresh-start Failed shape when it never ran one, and
// the occupied target is not touched. The strong path
// guards both lifecycle identities in one authoritative snapshot;
// compatibility adapters confirm the target and source independently
// through their fresh mutation reads. Reports whether this pass reset.
func ResetGangSurgeSource(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	target *types.InstanceStatus,
) (bool, error) {
	sourceIdentity := Capture(source)
	targetIdentity := Capture(target)
	reset := stoppedSurgeSourceReset(source, source.RunningRevision, input.Now())

	if input.ApplyInstanceMutationsWithRetryBlock != nil {
		write, err := newGuardedWrite(input)
		if err != nil {
			return false, err
		}
		reset.BatchPrecondition = write.guard(func(rows types.InstanceMutationSnapshot) bool {
			return sourceIdentity.holds(rows) && targetIdentity.holds(rows)
		})
		reset.OnCommit = write.recordCommit
		return write.commit(ctx, input, []types.InstanceMutation{reset}, "", nil)
	}

	if input.MutateInstance == nil {
		return false, fmt.Errorf("gang surge target conflict requires a status mutation adapter")
	}
	targetMatched := false
	if err := input.MutateInstance(ctx, target.Index, func(current *types.InstanceStatus) bool {
		targetMatched = targetIdentity.Matches(*current)
		return false
	}); err != nil {
		return false, err
	}
	if !targetMatched {
		return false, nil
	}
	sourceMatched := false
	if err := input.MutateInstance(ctx, source.Index, func(current *types.InstanceStatus) bool {
		sourceMatched = sourceIdentity.Matches(*current)
		if !sourceMatched {
			return false
		}
		return reset.Mutate(current)
	}); err != nil {
		return false, err
	}
	return sourceMatched, nil
}

// StampGangSurgeTargetCleanupStep moves a gang surge's TARGET marker
// from its live step to Step=GangSurgeTargetCleanup: the slot is being
// collected, not claimed. Fenced on the source identity and the marker
// identity. Reports whether the marker is on the cleanup step afterwards
// — by this write or a previous one.
func StampGangSurgeTargetCleanupStep(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	marker *types.InstanceStatus,
) (bool, error) {
	write, err := newGuardedWrite(input)
	if err != nil {
		return false, err
	}

	sourceIdentity := Capture(source)
	step := write.stepTransition(Capture(marker), types.UpdateStepGangSurgeTargetCleanup, sourceIdentity.holds, nil)
	return write.commit(ctx, input, []types.InstanceMutation{step}, "", nil)
}

// RestoreGangSurgeTargetCleanup closes the resumable gap between a
// source whose surge is being abandoned and the cleanup claim its
// target should carry: an absent slot gets the cleanup claim, a live
// claim is moved to the cleanup step, and an existing cleanup claim
// is confirmed. Fenced on the source identity. Reports whether the
// cleanup claim is in place afterwards.
func RestoreGangSurgeTargetCleanup(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	surgeIdx int32,
	targetRevision string,
	timeout time.Duration,
) (bool, error) {
	write, err := newGuardedWrite(input)
	if err != nil {
		return false, err
	}
	claim, err := write.restoreGangSurgeSlot(ctx, input, gangSurgeSlotRestore{
		source:   source,
		surgeIdx: surgeIdx,
		revision: targetRevision,
		step:     types.UpdateStepGangSurgeTargetCleanup,
		timeout:  timeout,
		moveClaim: func(row *types.InstanceStatus) {
			row.Operation.Step = types.UpdateStepGangSurgeTargetCleanup
		},
		settled: func(row, _ *types.InstanceStatus) bool {
			return row != nil && row.Index == surgeIdx &&
				GangSurgeCleanupTargetClaimMatches(row, targetRevision)
		},
	})
	return claim != GangSurgeTargetMarkerUnclaimed, err
}

// ResetGangSurgeSourceAndRemoveMarker ends an abandoned gang surge in
// one batch: the SOURCE goes back to Ready on sourceRunningRev, or to the
// fresh-start Failed shape when it never ran a revision, and the
// TARGET's cleanup marker is removed, with the failed revision's
// RetryBlock disposition committed in the same write. Fenced on both
// identities, proved once before the adapter finalizes the surge index's
// own resources and again on the write. Reports whether this pass
// committed the reset.
func ResetGangSurgeSourceAndRemoveMarker(
	ctx context.Context,
	deps types.Deps,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	marker *types.InstanceStatus,
	surgeIdx int32,
	sourceRunningRev string,
	failedTargetRev string,
	failureReason string,
	cause types.FailureCause,
) (bool, error) {
	if source == nil {
		return false, nil
	}
	reset := stoppedSurgeSourceReset(source, sourceRunningRev, input.Now())
	var counted *countedFailure
	if input.MutateRetryBlock != nil && failedTargetRev != "" {
		failure := AttemptFailure{TargetRevision: failedTargetRev, Reason: failureReason, Cause: cause}
		counted = failure.counted(input, metav1.NewTime(input.Now()))
	}
	committed, err := endGangSurgePair(ctx, deps, input, source, marker, surgeIdx, sourceRunningRev, reset, counted.revision(), counted.write())
	if err == nil && committed && counted.heldAttempts() > 0 && input.WarnRetryHeld != nil {
		input.WarnRetryHeld(failedTargetRev, counted.heldAttempts(), failureReason)
	}
	return committed, err
}

// stoppedSurgeSourceReset is the write that returns the source of a gang surge
// that stopped to runningRevision, with the postcondition that the row written
// is the source read, settled there: the same identity, Ready on the revision,
// or Failed with no operation when it ran none.
func stoppedSurgeSourceReset(source *types.InstanceStatus, runningRevision string, now time.Time) types.InstanceMutation {
	reset := AbandonedSurgeSourceMutation(source.Index, runningRevision, now)
	reset.Postcondition = func(row *types.InstanceStatus) bool {
		return row != nil && row.Index == source.Index &&
			row.Incarnation == source.Incarnation &&
			AbandonedSurgeSourceSettled(row, runningRevision) &&
			row.ActiveOrdinal == source.ActiveOrdinal
	}
	return reset
}

// HoldGangSurgeSourceAndRemoveMarker ends a gang surge whose pinned
// revision admits no further attempt, in one batch: the SOURCE is left
// Failed with no operation and termination recorded, on the revision it
// ran, and the TARGET's cleanup marker is removed. The ladder already
// holds, so no RetryBlock is written. Fenced like the reset. Reports
// whether this pass committed the hold.
func HoldGangSurgeSourceAndRemoveMarker(
	ctx context.Context,
	deps types.Deps,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	marker *types.InstanceStatus,
	surgeIdx int32,
	termination *types.InstanceTermination,
) (bool, error) {
	if source == nil {
		return false, nil
	}
	hold := FailedWithoutOperationMutation(source.Index, termination)
	hold.Postcondition = func(row *types.InstanceStatus) bool {
		return row != nil && row.Index == source.Index &&
			row.Incarnation == source.Incarnation &&
			row.Phase == types.InstancePhaseFailed && row.Operation == nil
	}
	return endGangSurgePair(ctx, deps, input, source, marker, surgeIdx, "", hold, "", nil)
}

// FailedWithoutOperationMutation ends the attempt a row carries where it
// stands: Phase Failed, the operation released, termination recorded. The
// revisions the row ran and converged toward are left as they were.
func FailedWithoutOperationMutation(idx int32, termination *types.InstanceTermination) types.InstanceMutation {
	return types.InstanceMutation{Index: idx, Mutate: func(s *types.InstanceStatus) bool {
		if s.Phase == types.InstancePhaseFailed && s.Operation == nil && sameFailureIdentity(s.LastFailure, termination) {
			return false
		}
		s.Phase = types.InstancePhaseFailed
		s.Operation = nil
		if termination != nil {
			captured := *termination
			s.LastFailure = &captured
		}
		return true
	}}
}

// endGangSurgePair commits the end of a gang surge pair: the source's end
// mutation and the removal of the marker, fenced on both identities,
// proved once before the adapter finalizes the surge index's resources
// and again on the write, with the retry-block mutation, when there is
// one, in the same write. A block standing against prunedRevision is
// pruned first, the way a promote onto it would. Reports whether this
// pass committed the end.
func endGangSurgePair(
	ctx context.Context,
	deps types.Deps,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	marker *types.InstanceStatus,
	surgeIdx int32,
	prunedRevision string,
	end types.InstanceMutation,
	retryRevision string,
	mutateRetryBlock func(*types.RetryBlock) types.RetryBlockDisposition,
) (bool, error) {
	write, err := newGuardedWrite(input)
	if err != nil {
		return false, err
	}
	if marker == nil {
		return false, nil
	}

	sourceIdentity := Capture(source)
	markerIdentity := Capture(marker)
	guard := write.guard(func(rows types.InstanceMutationSnapshot) bool {
		currentMarker, found := rows.Instances[surgeIdx]
		return sourceIdentity.holds(rows) && found && markerIdentity.Matches(currentMarker)
	})
	if held, err := guardHolds(ctx, input, source.Index, guard); !held {
		return false, err
	}
	if prunedRevision != "" && types.FindRetryBlock(input.ObservedState.RetryBlocks, prunedRevision) != nil {
		if err := RetryBlockPruneOnPromote(ctx, input, prunedRevision); err != nil {
			return false, err
		}
	}

	if input.FinalizeInstanceResources != nil {
		complete, err := input.FinalizeInstanceResources(ctx, surgeIdx)
		if err != nil {
			return false, err
		}
		if !complete {
			return false, nil
		}
	}

	end.BatchPrecondition = guard
	end.OnCommit = func(_, _ *types.InstanceStatus) {
		write.committed = true
		deps.ExpectationsCache().Forget(input.Key.Namespace, input.Key.OwnerName, input.Key.Component, surgeIdx)
	}
	removeMarker := types.InstanceMutation{Index: surgeIdx, Remove: true}
	return write.commit(ctx, input, []types.InstanceMutation{end, removeMarker}, retryRevision, mutateRetryBlock)
}

// GangSurgeTargetMarker reports which marker state a target slot was
// found in.
type GangSurgeTargetMarker uint8

const (
	// GangSurgeTargetMarkerUnclaimed: no marker, and none written.
	GangSurgeTargetMarkerUnclaimed GangSurgeTargetMarker = iota
	// GangSurgeTargetMarkerActive: the live marker is already there.
	GangSurgeTargetMarkerActive
	// GangSurgeTargetMarkerRestored: this pass wrote the marker, so a
	// fresh reconcile must observe it before any external effect.
	GangSurgeTargetMarkerRestored
	// GangSurgeTargetMarkerCleanup: the marker is there, in its cleanup
	// step — the slot is being collected, not claimed.
	GangSurgeTargetMarkerCleanup
)

// RestoreGangSurgeTarget closes the resumable gap between the source's
// committed surge claim and its target claim. The result identifies the
// authoritative claim state. The strong path binds target creation to
// the exact source operation on every conflict retry.
func RestoreGangSurgeTarget(
	ctx context.Context,
	input types.ReconcileInput,
	source *types.InstanceStatus,
	surgeIdx int32,
	targetRev string,
	timeout time.Duration,
) (GangSurgeTargetMarker, error) {
	if source == nil || source.Operation == nil {
		return GangSurgeTargetMarkerUnclaimed, nil
	}
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		if input.FinalizeInstanceResources != nil {
			return GangSurgeTargetMarkerUnclaimed, fmt.Errorf("gang surge target recovery requires the owner-aware atomic status adapter")
		}
		if err := StampGangSurgeTarget(ctx, input, surgeIdx, targetRev, timeout); err != nil {
			return GangSurgeTargetMarkerUnclaimed, err
		}
		return GangSurgeTargetMarkerRestored, nil
	}
	write, err := newGuardedWrite(input)
	if err != nil {
		return GangSurgeTargetMarkerUnclaimed, err
	}
	return write.restoreGangSurgeSlot(ctx, input, gangSurgeSlotRestore{
		source:   source,
		surgeIdx: surgeIdx,
		revision: targetRev,
		step:     types.UpdateStepGangSurgeTarget,
		timeout:  timeout,
		settled:  sameRestoredGangSurgeTarget,
	})
}

// gangSurgeSlotRestore names the form a pair's replacement row takes at
// the surge index: the claim at step for revision, under the source as
// the pass read it, with the deadline timeout gives. moveClaim is what
// the write does with the pair's claim found at the other step; nil
// leaves such a claim as it stands and counts it done. settled is the
// postcondition, asked with the row an empty slot is claimed with.
type gangSurgeSlotRestore struct {
	source    *types.InstanceStatus
	surgeIdx  int32
	revision  string
	step      string
	timeout   time.Duration
	moveClaim func(*types.InstanceStatus)
	settled   func(row, desired *types.InstanceStatus) bool
}

// restoreGangSurgeSlot is the write both restores share: it puts the
// pair's replacement row at the surge index into the form r names,
// fenced on the source identity. An empty slot is claimed; the pair's
// claim already at r.step is confirmed; the pair's claim at the other
// step is moved by r.moveClaim or, with none, confirmed as it stands;
// any other occupant stops the write. Reports what the slot holds once
// the write is done (the live claim, the cleanup claim, or the row this
// pass wrote), Unclaimed when the rows moved under the write or it
// failed.
func (w *guardedWrite) restoreGangSurgeSlot(ctx context.Context, input types.ReconcileInput, r gangSurgeSlotRestore) (GangSurgeTargetMarker, error) {
	desired := gangSurgeTargetRow(r.surgeIdx, r.revision, r.step, r.timeout, metav1.NewTime(input.Now()))
	sourceIdentity := Capture(r.source)
	wanted := gangSurgeClaimAt(r.step)
	holds := GangSurgeTargetMarkerUnclaimed
	// confirmed records that the slot already carries claim: the write is
	// done without writing.
	confirmed := func(claim GangSurgeTargetMarker) {
		holds, w.confirmed = claim, true
	}
	// answer is what the write does with the row in the slot. Reports
	// whether the row changed.
	answer := func(row *types.InstanceStatus) bool {
		switch claim := gangSurgeClaimOf(row, r.revision); {
		case claim == GangSurgeTargetMarkerUnclaimed:
			if !EmptyGangSurgeTargetSlot(row) {
				return false
			}
			*row = Clone(desired)
			return true
		case claim != wanted && r.moveClaim != nil:
			r.moveClaim(row)
			return true
		default:
			confirmed(claim)
			return false
		}
	}
	mutation := types.InstanceMutation{
		Index:  r.surgeIdx,
		Mutate: answer,
		BatchPrecondition: w.guard(func(rows types.InstanceMutationSnapshot) bool {
			holds = GangSurgeTargetMarkerUnclaimed
			if !sourceIdentity.holds(rows) {
				return false
			}
			current, found := rows.Instances[r.surgeIdx]
			if !found {
				return true
			}
			claim := gangSurgeClaimOf(&current, r.revision)
			if claim == GangSurgeTargetMarkerUnclaimed {
				return false
			}
			if claim == wanted || r.moveClaim == nil {
				confirmed(claim)
			}
			return true
		}),
		Postcondition: func(row *types.InstanceStatus) bool {
			return r.settled(row, &desired)
		},
		OnCommit: func(_, _ *types.InstanceStatus) {
			holds, w.committed = GangSurgeTargetMarkerRestored, true
		},
	}
	done, err := w.commit(ctx, input, []types.InstanceMutation{mutation}, "", nil)
	if err != nil || !done {
		return GangSurgeTargetMarkerUnclaimed, err
	}
	return holds, nil
}

// gangSurgeClaimOf reports the pair's claim row carries for revision:
// the live claim, the cleanup claim, or Unclaimed for anything else.
func gangSurgeClaimOf(row *types.InstanceStatus, revision string) GangSurgeTargetMarker {
	switch {
	case GangSurgeActiveTargetClaimMatches(row, revision):
		return GangSurgeTargetMarkerActive
	case GangSurgeCleanupTargetClaimMatches(row, revision):
		return GangSurgeTargetMarkerCleanup
	}
	return GangSurgeTargetMarkerUnclaimed
}

// gangSurgeClaimAt is the claim a target row at step reads as.
func gangSurgeClaimAt(step string) GangSurgeTargetMarker {
	if step == types.UpdateStepGangSurgeTargetCleanup {
		return GangSurgeTargetMarkerCleanup
	}
	return GangSurgeTargetMarkerActive
}

// EmptyGangSurgeTargetSlot reports whether a slot carries nothing at all
// — the only shape a surge may claim.
func EmptyGangSurgeTargetSlot(status *types.InstanceStatus) bool {
	return status != nil && status.Incarnation == 0 && status.Phase == types.InstancePhaseEmpty &&
		status.RunningRevision == "" && status.TargetRevision == "" && status.Operation == nil &&
		status.PodCount == 0 && status.ServingPodCount == 0 && status.AvailablePodCount == 0 &&
		!status.Admitted && len(status.Conditions) == 0 && status.LastFailure == nil
}

// gangSurgeTargetMatches reports the live marker in full: the active
// claim, on a row still in Creating at the GangSurgeTarget step.
func gangSurgeTargetMatches(status *types.InstanceStatus, targetRevision string) bool {
	return GangSurgeActiveTargetClaimMatches(status, targetRevision) &&
		status.Phase == types.InstancePhaseCreating &&
		status.Operation.Step == types.UpdateStepGangSurgeTarget
}

// GangSurgeTargetClaimMatches reports either marker for targetRevision:
// the live claim or the cleanup one the collection leaves behind.
func GangSurgeTargetClaimMatches(status *types.InstanceStatus, targetRevision string) bool {
	return GangSurgeActiveTargetClaimMatches(status, targetRevision) ||
		GangSurgeCleanupTargetClaimMatches(status, targetRevision)
}

// GangSurgeActiveTargetClaimMatches reports the live target claim.
func GangSurgeActiveTargetClaimMatches(status *types.InstanceStatus, targetRevision string) bool {
	return gangSurgeTargetClaim(status, targetRevision, types.UpdateStepGangSurgeTarget)
}

// GangSurgeCleanupTargetClaimMatches reports the cleanup claim: the
// marker a collection of the surge slot is running under.
func GangSurgeCleanupTargetClaimMatches(status *types.InstanceStatus, targetRevision string) bool {
	return gangSurgeTargetClaim(status, targetRevision, types.UpdateStepGangSurgeTargetCleanup)
}

// gangSurgeTargetClaim reports a target slot's claim for targetRevision at
// step: an Update operation at that step toward targetRevision, on a row
// converging toward it.
func gangSurgeTargetClaim(status *types.InstanceStatus, targetRevision, step string) bool {
	return status != nil && status.TargetRevision == targetRevision && status.Operation != nil &&
		status.Operation.Type == types.InstanceOperationUpdate &&
		status.Operation.Step == step &&
		status.Operation.TargetRevision == targetRevision
}

func sameRestoredGangSurgeTarget(current, desired *types.InstanceStatus) bool {
	return current != nil && desired != nil && gangSurgeTargetMatches(current, desired.TargetRevision) &&
		current.Index == desired.Index && current.Incarnation == desired.Incarnation &&
		current.Operation.ID == desired.Operation.ID
}

// StampReplacementLost records on a gang surge SOURCE that its replacement
// gang lost members after the hand-over: Phase Failed with the operation
// kept and its deadline cleared (keepOperationWithoutDeadline), termination
// on LastFailure. The attempt that died is closed for the retry record to
// count; a later pass starts the next attempt under a deadline of its own,
// or holds the pair. A row already Failed on the same loss, or one with no
// operation left, is a no-op.
func StampReplacementLost(termination *types.InstanceTermination) func(*types.InstanceStatus) bool {
	return func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil {
			return false
		}
		changed := false
		if s.Phase != types.InstancePhaseFailed {
			s.Phase = types.InstancePhaseFailed
			keepOperationWithoutDeadline(s)
			changed = true
		}
		if termination != nil && !sameFailureIdentity(s.LastFailure, termination) {
			captured := *termination
			s.LastFailure = &captured
			changed = true
		}
		return changed
	}
}

// RearmGangSurgeRebuild opens the next attempt of a gang surge whose
// replacement died after the hand-over: the SOURCE goes back to Updating
// at its drain step with the retry counted on the operation and a fresh
// deadline from now, and the pinned revision's due backoff becomes the
// attempt in progress in the same write. Fenced on the source identity.
// The same write clears the placement wait the ended attempt left on the
// replacement's row (endedAttemptWaitClear), so the next attempt never
// starts from it, even when the hold pass's write that clears it did not
// land. Reports whether this pass re-armed.
func RearmGangSurgeRebuild(
	ctx context.Context,
	input types.ReconcileInput,
	source, replacement *types.InstanceStatus,
	timeout time.Duration,
) (bool, error) {
	if source == nil || source.Operation == nil || source.Operation.TargetRevision == "" {
		return false, nil
	}
	clears := endedAttemptWaitClear(replacement, source.Operation.TargetRevision)
	now := metav1.NewTime(input.Now())
	identity := Capture(source)
	retry := source.Operation.RetryCount + 1
	rearm := func(s *types.InstanceStatus) bool {
		if !identity.Matches(*s) {
			return false
		}
		s.Phase = types.InstancePhaseUpdating
		op := *s.Operation
		op.RetryCount = retry
		op.StartedAt = now
		op.LastProgressAt = now
		op.Deadline = types.DeadlineAt(now, timeout)
		s.Operation = &op
		return true
	}
	pin := source.Operation.TargetRevision
	if input.ApplyInstanceMutationsWithRetryBlock == nil {
		// With no batched write, the wait goes first: the source opens the
		// next attempt only once the replacement's row no longer carries it.
		for _, clear := range clears {
			if err := input.MutateInstance(ctx, clear.Index, clear.Mutate); err != nil {
				return false, err
			}
		}
		committed := false
		if err := input.MutateInstance(ctx, source.Index, func(s *types.InstanceStatus) bool {
			committed = rearm(s)
			return committed
		}); err != nil || !committed {
			return false, err
		}
		return true, RetryBlockAttemptStarted(ctx, input, pin)
	}
	write, err := newGuardedWrite(input)
	if err != nil {
		return false, err
	}
	mutation := types.InstanceMutation{
		Index:             source.Index,
		Mutate:            rearm,
		BatchPrecondition: write.guard(identity.holds),
		Postcondition: func(row *types.InstanceStatus) bool {
			return row != nil && row.Phase == types.InstancePhaseUpdating &&
				row.Operation != nil && row.Operation.RetryCount == retry
		},
		OnCommit: write.recordCommit,
	}
	return write.commit(ctx, input, append([]types.InstanceMutation{mutation}, clears...), pin, RetryBlockStartAttempt)
}

// endedAttemptWaitClear is the mutation that retires the placement wait a
// gang surge replacement's row recorded for the attempt that ended, for
// the write that opens the next attempt to carry. It is empty when the
// row the pass read records none, so that write names no row it would
// leave unchanged. The row is cleared only while it still holds the
// pair's live claim on targetRevision.
func endedAttemptWaitClear(replacement *types.InstanceStatus, targetRevision string) []types.InstanceMutation {
	if replacement == nil || replacement.Operation == nil || replacement.Operation.PlacementWaitSince == nil {
		return nil
	}
	return []types.InstanceMutation{{
		Index: replacement.Index,
		Mutate: func(s *types.InstanceStatus) bool {
			return GangSurgeActiveTargetClaimMatches(s, targetRevision) && clearPlacementWait(s)
		},
		Postcondition: func(row *types.InstanceStatus) bool {
			return row != nil && (!GangSurgeActiveTargetClaimMatches(row, targetRevision) || row.Operation.PlacementWaitSince == nil)
		},
	}}
}
