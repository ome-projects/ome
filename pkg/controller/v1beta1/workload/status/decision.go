package status

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The decision stamps: phase, operation, step, target revision and
// deadline. What the engine decided, as opposed to what it observed.
// Edge triggers key on these fields. The three Failed stamps carry the
// termination record onto LastFailure in the same write, because the
// decision and its cause must land together or a crash between them
// leaves a Failed row that says nothing; no other stamp here touches an
// evidence field.

// StampFailed stamps Phase=Failed AND clears the in-flight
// Operation in one write: the abandon-analogue for single-pod and
// create attempts. Failed-with-no-Operation hands control back to
// operation-specific recovery on a later reconcile, and clearing the
// Operation is what stops the current attempt's stamper from extending
// it. termination, when non-nil, is recorded on LastFailure in the same
// write.
//
// A fresh-empty slot (Phase=="") from the writer's append path is a
// sentinel for a slot deleted out from under us — don't resurrect.
func StampFailed(ctx context.Context, input types.ReconcileInput, idx int32, termination *types.InstanceTermination) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" {
			return false
		}
		if s.Phase == types.InstancePhaseFailed && s.Operation == nil {
			return false
		}
		s.Phase = types.InstancePhaseFailed
		s.Operation = nil
		if termination != nil {
			captured := *termination
			s.LastFailure = &captured
		}
		return true
	})
}

// StampFailedKeepingOperation is the deadline backstop's stamp: Phase=Failed
// with the Operation left in place, because an operator reading the row
// wants to see WHAT was in flight when the deadline elapsed.
//
// A slot whose Operation is already gone is a no-op — the attempt
// concluded, or was disposed, since the deadline was observed, so there
// is nothing in flight left to expire. An already-Failed slot is a no-op
// for the same reason the clearing stamp treats it as one.
func StampFailedKeepingOperation(termination *types.InstanceTermination) func(*types.InstanceStatus) bool {
	return func(s *types.InstanceStatus) bool {
		if s.Phase == "" {
			return false
		}
		if s.Phase == types.InstancePhaseFailed {
			return false
		}
		if s.Operation == nil {
			return false
		}
		s.Phase = types.InstancePhaseFailed
		captured := *termination
		s.LastFailure = &captured
		return true
	}
}

// StampFailedOnStuckPod is the stuck-pod escalation's stamp: Phase=Failed with
// whatever operation the row carries left in place, so an operator sees
// what was in flight when the kubelet wedge surfaced. Unlike
// StampFailedKeepingOperation it needs no operation: the wedged-pod recovery
// shape — a pod whose revision disagrees with the current one under a
// settled row — has none. termination, when non-nil, is recorded on
// LastFailure in the same write so the wedged pod's diagnostics survive
// the recreate or teardown that follows; nil leaves LastFailure alone.
//
// A fresh-empty slot (Phase=="") from the writer's append path is a
// sentinel for a slot deleted out from under us — don't resurrect. An
// already-Failed slot is a no-op.
func StampFailedOnStuckPod(termination *types.InstanceTermination) func(*types.InstanceStatus) bool {
	return func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Phase == types.InstancePhaseFailed {
			return false
		}
		s.Phase = types.InstancePhaseFailed
		if termination != nil {
			captured := *termination
			s.LastFailure = &captured
		}
		return true
	}
}

// EnterReady flips s into Ready and stamps ReadySince when this is an
// entry into Ready rather than an idempotent re-apply. Container
// restarts that finished before the stamp — boot crashes, in-place
// update restarts — therefore never read as post-Ready failures.
func EnterReady(s *types.InstanceStatus, now time.Time) {
	if s.Phase != types.InstancePhaseReady {
		t := metav1.NewTime(now)
		s.ReadySince = &t
	}
	s.Phase = types.InstancePhaseReady
}

// DemoteUnbacked applies the truth pass to one row: a status-only
// Ready→Pending transition for an Instance the decision layer proved
// unbacked. Counters, revisions and Incarnation are preserved —
// recovery stays with the ordinary passes, which re-materialize a
// Pending Instance's pods exactly as they would a Ready one's.
//
// The precondition is re-checked on the fresh row, so an operation that
// claimed the row since selection keeps it and the demotion no-ops.
func DemoteUnbacked(ctx context.Context, input types.ReconcileInput, idx int32) (bool, error) {
	demoted := false
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase != types.InstancePhaseReady || s.Operation != nil {
			return false
		}
		s.Phase = types.InstancePhasePending
		demoted = true
		return true
	})
	return demoted, err
}

// StampDeadline writes Operation.Deadline and nothing else. A zero
// deadline is the "never expires" sentinel the deadline backstop
// honors, so parking and restarting the clock are the same stamp. No-op
// when the slot is gone, has no operation, or already holds the target.
func StampDeadline(ctx context.Context, input types.ReconcileInput, idx int32, deadline metav1.Time) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil {
			return false
		}
		if s.Operation.Deadline.Time.Equal(deadline.Time) {
			return false
		}
		s.Operation.Deadline = deadline
		return true
	})
}

// StampUpdatingInPlace idempotently stamps Phase=Updating with the
// Update operation in Step=InPlace at targetRev. The skip-write guard
// requires Step==InPlace so a previous recreate-step write does not
// short-circuit it.
//
// An attempt already in flight toward this exact target keeps its
// identity and its deadline; only the step moves. The update mode is
// re-resolved on every pass, so a roll can change mechanism mid-flight
// without the attempt ending — and an attempt that restarted its clock
// on each of those turns would never be bounded by InstanceReadyTimeout
// at all.
func StampUpdatingInPlace(ctx context.Context, input types.ReconcileInput, idx int32, targetRev string, strategy types.UpdateStrategyType, timeout time.Duration) error {
	now := metav1.NewTime(input.Now())
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == types.InstancePhaseUpdating &&
			s.Operation != nil && s.Operation.Type == types.InstanceOperationUpdate &&
			s.Operation.Step == types.UpdateStepInPlace &&
			s.TargetRevision == targetRev {
			return false
		}
		if s.Phase == types.InstancePhaseUpdating &&
			s.Operation != nil && s.Operation.Type == types.InstanceOperationUpdate &&
			s.Operation.TargetRevision == targetRev && s.TargetRevision == targetRev {
			op := *s.Operation
			op.Step = types.UpdateStepInPlace
			op.LastProgressAt = now
			s.Operation = &op
			return true
		}
		s.Phase = types.InstancePhaseUpdating
		s.TargetRevision = targetRev
		s.Operation = &types.InstanceOperation{
			ID:             fmt.Sprintf("update-%d-%d", idx, now.Unix()),
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepInPlace,
			TargetRevision: targetRev,
			Strategy:       strategy,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       types.DeadlineAt(now, timeout),
		}
		return true
	})
	if err != nil {
		return err
	}
	return RetryBlockAttemptStarted(ctx, input, targetRev)
}

// StampRecreating is the recreate entry point: bumps Incarnation (like
// Restart) and stamps Phase=Updating with Step=Drain. reason is the
// "revision <from> → <to>" cause string recorded on Operation.Reason so
// a revision-roll recreate is distinguishable in status from a
// pod-failure Restart. Returns the post-write Incarnation. The
// skip-write guard requires Step==Drain so a prior in-place pass at the
// same target does not block the bump.
func StampRecreating(ctx context.Context, input types.ReconcileInput, idx int32, targetRev, reason string, strategy types.UpdateStrategyType, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == types.InstancePhaseUpdating &&
			s.Operation != nil && s.Operation.Type == types.InstanceOperationUpdate &&
			s.Operation.Step == types.UpdateStepDrain &&
			s.TargetRevision == targetRev && s.Incarnation > 0 {
			observedIncarnation = s.Incarnation
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Incarnation++
		observedIncarnation = s.Incarnation
		s.Phase = types.InstancePhaseUpdating
		s.TargetRevision = targetRev
		now := metav1.NewTime(input.Now())
		s.Operation = &types.InstanceOperation{
			ID:             fmt.Sprintf("update-%d-%d", idx, now.Unix()),
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepDrain,
			TargetRevision: targetRev,
			Strategy:       strategy,
			Reason:         reason,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       types.DeadlineAt(now, timeout),
		}
		return true
	})
	if err != nil {
		return observedIncarnation, err
	}
	return observedIncarnation, RetryBlockAttemptStarted(ctx, input, targetRev)
}

// StampRestarting idempotently stamps Phase=Restarting + Restart/Drain
// with the trigger reason and bumps Incarnation by one, so the rebuilt
// pods are distinguishable from the set being drained. Returns the
// post-write Incarnation; a pass that already moved the row into Restart
// preserves it.
//
// An Instance that never ran a revision has only the revision its
// interrupted attempt pinned; the rebuilt pods carry it so the
// per-revision Service selects them.
func StampRestarting(ctx context.Context, input types.ReconcileInput, idx int32, reason string, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == types.InstancePhaseRestarting &&
			s.Operation != nil && s.Operation.Type == types.InstanceOperationRestart {
			observedIncarnation = s.Incarnation
			return false
		}
		pinned := ""
		if s.RunningRevision == "" && s.TargetRevision == "" && s.Operation != nil {
			pinned = s.Operation.TargetRevision
		}
		if s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Incarnation++
		observedIncarnation = s.Incarnation
		s.Phase = types.InstancePhaseRestarting
		now := metav1.NewTime(input.Now())
		s.Operation = &types.InstanceOperation{
			ID:             fmt.Sprintf("restart-%d-%d", idx, now.Unix()),
			Type:           types.InstanceOperationRestart,
			Step:           types.RestartStepDrain,
			Reason:         reason,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       types.DeadlineAt(now, timeout),
			TargetRevision: pinned,
		}
		return true
	})
	return observedIncarnation, err
}

// StampRecreateFromInPlace converts an in-flight in-place roll into
// the recreate roll for the same target: Incarnation bumped so the
// rebuilt pod is distinguishable, Step moved to Drain so the recreate's
// skip-write guard recognizes its own state. Reports whether it
// converted anything.
//
// The Operation is edited, not replaced. Identity and deadline belong to
// the attempt, and the attempt has not ended — it is converging to the
// same target by another mechanism — so a new id would make the row read
// as a retry and a new deadline would hand it a second full window.
func StampRecreateFromInPlace(ctx context.Context, input types.ReconcileInput, idx int32, targetRev string) (bool, error) {
	converted := false
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase != types.InstancePhaseUpdating || s.Operation == nil ||
			s.Operation.Type != types.InstanceOperationUpdate ||
			s.Operation.Step != types.UpdateStepInPlace {
			return false
		}
		if s.Incarnation == 0 {
			s.Incarnation = 1
		}
		s.Incarnation++
		s.TargetRevision = targetRev
		op := *s.Operation
		op.Step = types.UpdateStepDrain
		op.TargetRevision = targetRev
		op.Strategy = types.UpdateStrategyRecreatePod
		op.LastProgressAt = metav1.NewTime(input.Now())
		s.Operation = &op
		converted = true
		return true
	})
	return converted, err
}

// StampSurging is the SurgeThenDrain entry point. Stamps Phase=Updating
// + Op{Step=Surge, TargetRevision} without bumping Incarnation — a
// different pod NAME (the other ordinal slot) keeps old and new distinct
// without reusing identity. ActiveOrdinal stays on the old slot until
// promote completes.
//
// The skip-write guard accepts every surge lifecycle step so the helper
// does not regress an in-flight drain or settle back to Surge when the
// surge pass re-runs the entry stamp on a later pass; it does require
// Type==Update so a prior recreate or in-place write at the same target
// does not short-circuit it (those writes use the same TargetRevision
// but a different Step namespace).
func StampSurging(ctx context.Context, input types.ReconcileInput, idx int32, targetRev string, strategy types.UpdateStrategyType, timeout time.Duration) error {
	now := metav1.NewTime(input.Now())
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		// Already surging toward this target — no-op, whether the row is
		// still Updating or the escalator has since failed it. A Failed
		// row is not resurrected toward the same target; only a new
		// TargetRevision re-surges.
		if s.Operation != nil && s.Operation.Type == types.InstanceOperationUpdate &&
			SurgeUpdateStep(s.Operation.Step) &&
			s.TargetRevision == targetRev &&
			(s.Phase == types.InstancePhaseUpdating || s.Phase == types.InstancePhaseFailed) {
			return false
		}
		s.Phase = types.InstancePhaseUpdating
		s.TargetRevision = targetRev
		s.Operation = &types.InstanceOperation{
			ID:             fmt.Sprintf("update-%d-%d", idx, now.Unix()),
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepSurge,
			TargetRevision: targetRev,
			Strategy:       strategy,
			StartedAt:      now,
			LastProgressAt: now,
			Deadline:       types.DeadlineAt(now, timeout),
		}
		return true
	})
	if err != nil {
		return err
	}
	return RetryBlockAttemptStarted(ctx, input, targetRev)
}

// StampSurgeDrainStep moves the surge operation from Step=Surge to
// Step=SurgeDrain once the surge pod is Ready and the serving gates are
// about to flip. Distinct from Step=Drain (which recreate uses) so
// surge-budget accounting can identify in-flight surges by step name.
// Touches LastProgressAt for the stuck-operation timeout machinery.
// Idempotent — a no-op once the row is on SurgeDrain.
func StampSurgeDrainStep(ctx context.Context, input types.ReconcileInput, idx int32) error {
	now := metav1.NewTime(input.Now())
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Operation == nil ||
			s.Operation.Type != types.InstanceOperationUpdate {
			return false
		}
		if s.Operation.Step == types.UpdateStepSurgeDrain || s.Operation.Step == types.UpdateStepSurgeDrainSettle {
			return false
		}
		s.Operation.Step = types.UpdateStepSurgeDrain
		s.Operation.LastProgressAt = now
		return true
	})
}

// StampReadyOnRevision is the promote terminator: Phase=Ready with
// RunningRevision=rev, Operation and TargetRevision cleared. It is the
// post-promote anchor that lets an update trigger short-circuit on
// revision-equality before it diffs per-pod. Success at rev also prunes
// rev's RetryBlock — a converged subject leaves no active block.
func StampReadyOnRevision(ctx context.Context, input types.ReconcileInput, idx int32, rev string) error {
	mutation := ReadyOnRevisionMutation(idx, rev, input.Now())
	if err := input.MutateInstance(ctx, mutation.Index, mutation.Mutate); err != nil {
		return err
	}
	return RetryBlockPruneOnPromote(ctx, input, rev)
}

// ReadyOnRevisionMutation is the promote terminator as a mutation, for
// the pass that commits it alongside other rows in one batch. The
// RetryBlock prune is the stamp's other half and stays with the caller
// that owns the batch.
func ReadyOnRevisionMutation(idx int32, rev string, now time.Time) types.InstanceMutation {
	return types.InstanceMutation{Index: idx, Mutate: func(s *types.InstanceStatus) bool {
		if s.Phase == types.InstancePhaseReady &&
			s.Operation == nil &&
			s.RunningRevision == rev &&
			s.TargetRevision == "" {
			return false
		}
		EnterReady(s, now)
		s.RunningRevision = rev
		s.TargetRevision = ""
		s.Operation = nil
		return true
	}}
}

// StampReadyAtOrdinal is the surge-promote terminator: clears
// Operation, advances ActiveOrdinal to the new slot, stamps
// RunningRevision=rev and Phase=Ready. Surge needs the ordinal advance
// that the plain promote does not — the old pod is gone, the surge pod
// is the new canonical pod, and future restart and scale-up paths must
// address that slot. Success at rev also prunes rev's RetryBlock.
func StampReadyAtOrdinal(ctx context.Context, input types.ReconcileInput, idx int32, rev string, newOrdinal int32) error {
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == types.InstancePhaseReady &&
			s.Operation == nil &&
			s.RunningRevision == rev &&
			s.TargetRevision == "" &&
			s.ActiveOrdinal == newOrdinal {
			return false
		}
		EnterReady(s, input.Now())
		s.RunningRevision = rev
		s.TargetRevision = ""
		s.Operation = nil
		s.ActiveOrdinal = newOrdinal
		return true
	})
	if err != nil {
		return err
	}
	return RetryBlockPruneOnPromote(ctx, input, rev)
}

// SurgeUpdateStep reports whether step is one of the surge lifecycle's
// own — the three steps surge-budget accounting recognizes an in-flight
// surge by.
func SurgeUpdateStep(step string) bool {
	return step == types.UpdateStepSurge ||
		step == types.UpdateStepSurgeDrain ||
		step == types.UpdateStepSurgeDrainSettle
}

// StampStep persists a step transition against the exact
// lifecycle identity that selected the work. Timestamp normalization and
// derived status changes do not invalidate that ownership, which is why
// the precondition is the identity rather than the whole row.
func StampStep(
	ctx context.Context,
	input types.ReconcileInput,
	expected *types.InstanceStatus,
	step string,
	updateProgress bool,
) (bool, error) {
	if expected == nil || expected.Operation == nil {
		return false, nil
	}
	if err := RequireOwner(input); err != nil {
		return false, err
	}
	before := Capture(expected)
	after := before.WithStep(step)
	ownerUID := input.OwnerObject.GetUID()
	confirmed := false
	committed := false
	mutate := func(status *types.InstanceStatus) bool {
		if after.Matches(*status) {
			confirmed = true
			return false
		}
		if !before.Matches(*status) {
			return false
		}
		status.Operation.Step = step
		if updateProgress {
			status.Operation.LastProgressAt = metav1.NewTime(input.Now())
		}
		return true
	}
	mutation := types.InstanceMutation{
		Index:  expected.Index,
		Mutate: mutate,
		BatchPrecondition: func(snapshot types.InstanceMutationSnapshot) bool {
			confirmed = false
			if snapshot.OwnerUID != ownerUID {
				return false
			}
			current, found := snapshot.Instances[expected.Index]
			if !found {
				return false
			}
			if after.Matches(current) {
				confirmed = true
				return true
			}
			return before.Matches(current)
		},
		Postcondition: func(status *types.InstanceStatus) bool {
			return status != nil && after.Matches(*status)
		},
		OnCommit: func(_, _ *types.InstanceStatus) {
			committed = true
		},
	}

	err := Apply(ctx, input, []types.InstanceMutation{mutation})
	if errors.Is(err, types.ErrStatusMutationPrecondition) || errors.Is(err, types.ErrStatusOwnerGone) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return committed || confirmed, nil
}

// RetryBlockAttemptStarted marks an existing due-Backoff RetryBlock
// RetryInProgress once a fresh attempt actually starts — i.e. after
// every dispatcher budget and coordination gate admitted it. Flipping at
// detect time would strand RetryInProgress when a budget denies the
// start. No block, or any non-Backoff state, records nothing: a fresh
// start with no prior failure needs no block. Idempotent across passes.
func RetryBlockAttemptStarted(ctx context.Context, input types.ReconcileInput, targetRev string) error {
	if input.MutateRetryBlock == nil {
		return nil
	}
	if err := input.MutateRetryBlock(ctx, targetRev, RetryBlockStartAttempt); err != nil {
		return fmt.Errorf("mark retry in progress (rev=%s): %w", targetRev, err)
	}
	return nil
}

// RetryBlockStartAttempt is the RetryBlock half of an attempt
// start: a due block moves to RetryInProgress, anything else is left
// alone.
func RetryBlockStartAttempt(rb *types.RetryBlock) types.RetryBlockDisposition {
	if rb.State != types.RetryBlockBackoff {
		return types.RetryBlockUnchanged
	}
	rb.State = types.RetryBlockRetryInProgress
	return types.RetryBlockPersist
}

// RetryBlockPruneOnPromote removes the RetryBlock for rev once the
// subject converges there: a converged subject leaves no active block.
// No-op when the adapter did not wire MutateRetryBlock, or rev is empty.
func RetryBlockPruneOnPromote(ctx context.Context, input types.ReconcileInput, rev string) error {
	if input.MutateRetryBlock == nil || rev == "" {
		return nil
	}
	if err := input.MutateRetryBlock(ctx, rev, func(_ *types.RetryBlock) types.RetryBlockDisposition {
		return types.RetryBlockRemove
	}); err != nil {
		return fmt.Errorf("prune retry block (rev=%s): %w", rev, err)
	}
	return nil
}
