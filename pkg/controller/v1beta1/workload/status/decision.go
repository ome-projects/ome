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

// StampFailed stamps Phase=Failed AND clears whatever Operation the row
// carries in one write, for a failure decided on the row itself rather
// than on an attempt the pass observed: a rejected write of this pass, a
// crash the retry ladder parks. Failed-with-no-Operation hands control
// back to operation-specific recovery on a later reconcile, and clearing
// the Operation is what stops the current attempt's stamper from
// extending it. termination, when non-nil, is recorded on LastFailure in
// the same write.
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

// StampFailedEndingAttempt is the disposition's clearing stamp: Phase=Failed
// with the Operation cleared and termination recorded, landing only while
// the fresh row still carries the attempt the pass observed (attemptOnRow).
// It reports whether the row changed.
func StampFailedEndingAttempt(ctx context.Context, input types.ReconcileInput, idx int32, attempt types.InstanceOperation, termination *types.InstanceTermination) (bool, error) {
	return ApplyStamp(ctx, input, idx, func(s *types.InstanceStatus) bool {
		if !attemptOnRow(s, attempt) {
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

// StampRejected records a permanent apiserver rejection as the row's
// failure: Phase=Failed, the operation cleared unless keepOperation, and
// LastFailure the rejection, naming the pod the refused write was for
// when it made one. A row already Failed on this same rejection of the
// same pod is left as it is, so a repeated disposition writes nothing.
func StampRejected(ctx context.Context, input types.ReconcileInput, idx int32, termination *types.InstanceTermination, keepOperation bool) (bool, error) {
	return ApplyStamp(ctx, input, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" {
			return false
		}
		same := s.LastFailure != nil && s.LastFailure.PodName == termination.PodName &&
			s.LastFailure.Reason == termination.Reason && s.LastFailure.Message == termination.Message
		if s.Phase == types.InstancePhaseFailed && same && (keepOperation || s.Operation == nil) {
			return false
		}
		s.Phase = types.InstancePhaseFailed
		if !keepOperation {
			s.Operation = nil
		}
		captured := *termination
		s.LastFailure = &captured
		return true
	})
}

// AttemptStanding is how the fresh row relates to an attempt a pass
// observed: still carrying it, re-opened under another attempt, or
// concluded with no attempt left (an appended slot reads as concluded).
type AttemptStanding int

const (
	AttemptOnRow AttemptStanding = iota
	AttemptReopened
	AttemptConcluded
)

// AttemptStandingOnFreshRow reads the row through the single-row seam, as
// a write landing now would find it, and reports the attempt's standing
// on it; nothing is written.
func AttemptStandingOnFreshRow(ctx context.Context, input types.ReconcileInput, idx int32, attempt types.InstanceOperation) (AttemptStanding, error) {
	standing := AttemptConcluded
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		switch {
		case attemptOnRow(s, attempt):
			standing = AttemptOnRow
		case s.Phase != "" && s.Operation != nil:
			standing = AttemptReopened
		}
		return false
	})
	return standing, err
}

// attemptOnRow reports whether the fresh row still carries the attempt a
// pass observed and decided on. A row with no operation, or with another
// attempt (SameAttempt), concluded or was re-opened since the observation
// and is not that decision's to end; an appended slot (Phase=="") is a
// deleted row and is never resurrected.
func attemptOnRow(s *types.InstanceStatus, attempt types.InstanceOperation) bool {
	return s.Phase != "" && s.Operation != nil && SameAttempt(*s.Operation, attempt)
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
// blamedIncarnation is the incarnation label of the pod the stamp blames,
// zero when it carries none. The stamp is decided on the pass's
// observation and applied to the fresh row, and only a Restart bumps a
// row's incarnation: a blamed pod below the fresh row's incarnation is
// the set a repair claimed since the observation. The repair owns that
// set until its replacement runs, so the stamp is withheld.
//
// A fresh-empty slot (Phase=="") from the writer's append path is a
// sentinel for a slot deleted out from under us — don't resurrect. An
// already-Failed slot is a no-op.
func StampFailedOnStuckPod(termination *types.InstanceTermination, blamedIncarnation int64) func(*types.InstanceStatus) bool {
	return func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Phase == types.InstancePhaseFailed {
			return false
		}
		if blamedIncarnation > 0 && s.Incarnation > blamedIncarnation {
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

// StampFailedParkingAttempt is the disposition's stamp for an attempt
// whose pod set is still alive: Phase=Failed for the set that is down
// now, with the Update operation kept on the parked step — waiting
// names the wait the next attempt stands behind and the deadline is
// parked, since the clock that ended the attempt must not end it again.
// termination is recorded on LastFailure in the same write.
//
// attempt is the operation the disposition observed. The park lands only
// while the fresh row still carries that Update attempt (attemptOnRow): a
// row carrying another attempt, or none, has nothing to park. An already
// parked Failed row is a no-op. It reports whether the row changed.
func StampFailedParkingAttempt(ctx context.Context, input types.ReconcileInput, idx int32, attempt types.InstanceOperation, termination *types.InstanceTermination, waiting string) (bool, error) {
	return ApplyStamp(ctx, input, idx, func(s *types.InstanceStatus) bool {
		if !attemptOnRow(s, attempt) || s.Operation.Type != types.InstanceOperationUpdate {
			return false
		}
		if s.Phase == types.InstancePhaseFailed && types.OperationParked(s.Operation) {
			return false
		}
		s.Phase = types.InstancePhaseFailed
		op := *s.Operation
		op.Step = types.UpdateStepParked
		op.Waiting = waiting
		op.Deadline = metav1.Time{}
		s.Operation = &op
		if termination != nil {
			captured := *termination
			s.LastFailure = &captured
		}
		return true
	})
}

// SameAttempt reports whether two operations are the same attempt: the
// same ID, or the same start when neither carries one.
func SameAttempt(a, b types.InstanceOperation) bool {
	if a.ID != "" || b.ID != "" {
		return a.ID == b.ID
	}
	return a.StartedAt.Equal(&b.StartedAt)
}

// FollowParkedAttempt gives a parked attempt the phase its pod set earns:
// Updating while the full set serves, Failed otherwise. waiting, when
// non-empty, replaces the wait the row names. A row whose operation is
// not a parked attempt has moved on and is left alone.
func FollowParkedAttempt(serving bool, waiting string) func(*types.InstanceStatus) bool {
	return func(s *types.InstanceStatus) bool {
		if !types.OperationParked(s.Operation) {
			return false
		}
		if s.Phase != types.InstancePhaseFailed && s.Phase != types.InstancePhaseUpdating {
			return false
		}
		phase := types.InstancePhaseFailed
		if serving {
			phase = types.InstancePhaseUpdating
		}
		changed := false
		if s.Phase != phase {
			s.Phase = phase
			changed = true
		}
		if waiting != "" && s.Operation.Waiting != waiting {
			op := *s.Operation
			op.Waiting = waiting
			s.Operation = &op
			changed = true
		}
		return changed
	}
}

// ParkedAttemptWaits names waiting as the wait a parked attempt stands
// behind, and touches nothing else. A row whose operation is not a parked
// attempt is left alone, as is one already naming it.
func ParkedAttemptWaits(waiting string) func(*types.InstanceStatus) bool {
	return func(s *types.InstanceStatus) bool {
		if !types.OperationParked(s.Operation) || !types.ParkedWaitingReason(waiting) || s.Operation.Waiting == waiting {
			return false
		}
		op := *s.Operation
		op.Waiting = waiting
		s.Operation = &op
		return true
	}
}

// ApplyStamp runs one stamp through the single-row seam and reports
// whether it changed the row. The seam consumes the stamp's own answer,
// and a caller that announces the stamp has nothing to announce for one
// the fresh row withheld.
func ApplyStamp(ctx context.Context, input types.ReconcileInput, idx int32, stamp func(*types.InstanceStatus) bool) (bool, error) {
	committed := false
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		committed = stamp(s)
		return committed
	})
	return committed, err
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
// recovery stays with the ordinary passes: Create re-materializes a
// Pending Instance's pods exactly as it would a Ready one's, and under
// RecreateInstanceOnPodRestart the restart pass reads the kept running
// revision (types.DemotedReady) and rebuilds at it.
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

// UnparkServing returns a crash-loop park, a Failed row with no operation
// whose promoted set serves again, to Ready. ReadySince and the failure
// record keep their times; note, when given, is appended to the message.
func UnparkServing(ctx context.Context, input types.ReconcileInput, idx int32, note string) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase != types.InstancePhaseFailed || s.Operation != nil || s.LastFailure == nil {
			return false
		}
		s.Phase = types.InstancePhaseReady
		if note != "" {
			noteFailure(s, note)
		}
		return true
	})
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
// at all. An attempt parked after its disposition is nobody's and not in
// flight: a new attempt opens over it with its own identity and deadline.
func StampUpdatingInPlace(ctx context.Context, input types.ReconcileInput, idx int32, targetRev string, strategy types.UpdateStrategyType, timeout time.Duration) error {
	now := metav1.NewTime(input.Now())
	var openSince *metav1.Time
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		openSince = nil
		inFlight := types.Owner(s) == types.OwnerUpdate && s.Operation.Type == types.InstanceOperationUpdate
		if inFlight && s.Operation.Step == types.UpdateStepInPlace && s.TargetRevision == targetRev {
			started := s.Operation.StartedAt
			openSince = &started
			return false
		}
		if inFlight && s.Operation.TargetRevision == targetRev && s.TargetRevision == targetRev {
			started := s.Operation.StartedAt
			openSince = &started
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
	return entryStampAttemptStarted(ctx, input, targetRev, openSince)
}

// StampRecreating is the recreate entry point: bumps Incarnation (like
// Restart) and stamps Phase=Updating with Step=Drain. reason is the
// "revision <from> → <to>" cause string recorded on Operation.Reason so
// a revision-roll recreate is distinguishable in status from a
// pod-failure Restart. Returns the post-write Incarnation. The
// skip-write guard requires Step==Drain so a prior in-place pass at the
// same target does not block the bump; an attempt parked after its
// disposition sits on its own step, so a new attempt opens over it and
// the bump makes the parked set the one it drains.
func StampRecreating(ctx context.Context, input types.ReconcileInput, idx int32, targetRev, reason string, strategy types.UpdateStrategyType, timeout time.Duration) (int64, error) {
	var observedIncarnation int64
	var openSince *metav1.Time
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		openSince = nil
		if s.Phase == types.InstancePhaseUpdating &&
			s.Operation != nil && s.Operation.Type == types.InstanceOperationUpdate &&
			s.Operation.Step == types.UpdateStepDrain &&
			s.TargetRevision == targetRev && s.Incarnation > 0 {
			observedIncarnation = s.Incarnation
			started := s.Operation.StartedAt
			openSince = &started
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
	return observedIncarnation, entryStampAttemptStarted(ctx, input, targetRev, openSince)
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
//
// A Restart stamped over a Failed row that already carries a Restart is
// that repair's next attempt: the new operation's RetryCount continues
// the spent one's, which is what the retry ladder is indexed by.
//
// failure, when given, is the termination of the pod set the restart
// drains, captured into LastFailure in the same write, dated as
// captured: the drain deletes the pod and its record with it, and a
// rebuilt pod that keeps its name and dies the same way is a new failure
// a reader anchored on the time must see. A stored record that differs
// in nothing is left as it is.
func StampRestarting(ctx context.Context, input types.ReconcileInput, idx int32, reason string, timeout time.Duration, failure *types.InstanceTermination) (int64, error) {
	var observedIncarnation int64
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == types.InstancePhaseRestarting &&
			s.Operation != nil && s.Operation.Type == types.InstanceOperationRestart {
			observedIncarnation = s.Incarnation
			return false
		}
		if failure != nil && !(sameFailureIdentity(s.LastFailure, failure) && s.LastFailure.Time.Equal(&failure.Time)) {
			captured := *failure
			s.LastFailure = &captured
		}
		pinned := ""
		retries := int32(0)
		if s.Operation != nil {
			if s.RunningRevision == "" && s.TargetRevision == "" {
				pinned = s.Operation.TargetRevision
			}
			if s.Phase == types.InstancePhaseFailed && s.Operation.Type == types.InstanceOperationRestart {
				retries = s.Operation.RetryCount + 1
			}
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
			RetryCount:     retries,
			TargetRevision: pinned,
		}
		return true
	})
	return observedIncarnation, err
}

// StampRestartRevision records rev as the revision an open repair rebuilds
// at: RunningRevision on a Restarting row, written before the first pod of
// the rebuild is created, so the row's revision names the pod set the
// repair builds and every later pass renders and promotes that revision.
// Any other row is left alone: the repair ended, or another pass owns the
// row.
func StampRestartRevision(ctx context.Context, input types.ReconcileInput, idx int32, rev string) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase != types.InstancePhaseRestarting || s.Operation == nil ||
			s.Operation.Type != types.InstanceOperationRestart || s.RunningRevision == rev {
			return false
		}
		s.RunningRevision = rev
		return true
	})
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
	var openSince *metav1.Time
	err := input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		openSince = nil
		// Already surging toward this target — no-op, whether the row is
		// still Updating or the escalator has since failed it. A Failed
		// row is not resurrected toward the same target; only a new
		// TargetRevision re-surges.
		if s.Operation != nil && s.Operation.Type == types.InstanceOperationUpdate &&
			SurgeUpdateStep(s.Operation.Step) &&
			s.TargetRevision == targetRev &&
			(s.Phase == types.InstancePhaseUpdating || s.Phase == types.InstancePhaseFailed) {
			started := s.Operation.StartedAt
			openSince = &started
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
	return entryStampAttemptStarted(ctx, input, targetRev, openSince)
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

// AbandonedSurgeSourceMutation resets the source of an abandoned gang
// surge: Ready on the revision it runs, or the fresh-start Failed shape
// when it never ran one, since Ready with no revision would count a gang
// that never served as capacity. LastFailure is kept either way.
func AbandonedSurgeSourceMutation(idx int32, rev string, now time.Time) types.InstanceMutation {
	if rev != "" {
		return ReadyOnRevisionMutation(idx, rev, now)
	}
	return types.InstanceMutation{Index: idx, Mutate: func(s *types.InstanceStatus) bool {
		if s.Phase == "" || AbandonedSurgeSourceSettled(s, rev) {
			return false
		}
		s.Phase = types.InstancePhaseFailed
		s.TargetRevision = ""
		s.Operation = nil
		return true
	}}
}

// AbandonedSurgeSourceSettled reports whether the row already reads as
// the reset of an abandoned surge's source leaves it for rev.
func AbandonedSurgeSourceSettled(s *types.InstanceStatus, rev string) bool {
	if s == nil || s.Operation != nil || s.TargetRevision != "" {
		return false
	}
	if rev == "" {
		return s.Phase == types.InstancePhaseFailed
	}
	return s.Phase == types.InstancePhaseReady && s.RunningRevision == rev
}

// StampAbandonedSurgeSource writes AbandonedSurgeSourceMutation through
// the single-row seam and prunes rev's RetryBlock when there is one.
func StampAbandonedSurgeSource(ctx context.Context, input types.ReconcileInput, idx int32, rev string) error {
	mutation := AbandonedSurgeSourceMutation(idx, rev, input.Now())
	if err := input.MutateInstance(ctx, mutation.Index, mutation.Mutate); err != nil {
		return err
	}
	return RetryBlockPruneOnPromote(ctx, input, rev)
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

// entryStampAttemptStarted is the RetryBlock half of an update entry
// stamp, which runs again on every pass of its attempt. openSince is nil
// on the pass that opened the attempt, and otherwise the time it opened.
// An attempt open since before the block was due belongs to the wave the
// block already counts and starts nothing on the ladder; one opened once
// it was due is the attempt the ladder admitted, so a later pass lands
// the flip an earlier one did not.
func entryStampAttemptStarted(ctx context.Context, input types.ReconcileInput, targetRev string, openSince *metav1.Time) error {
	if openSince == nil {
		return RetryBlockAttemptStarted(ctx, input, targetRev)
	}
	if input.MutateRetryBlock == nil {
		return nil
	}
	since := *openSince
	err := input.MutateRetryBlock(ctx, targetRev, func(rb *types.RetryBlock) types.RetryBlockDisposition {
		if rb.NextRetryAt != nil && since.Before(rb.NextRetryAt) {
			return types.RetryBlockUnchanged
		}
		return RetryBlockStartAttempt(rb)
	})
	if err != nil {
		return fmt.Errorf("mark retry in progress (rev=%s): %w", targetRev, err)
	}
	return nil
}

// RowRemembersCrashOn reports whether a row Ready on rev records a
// failure of its current promoted pod set (types.RemembersCrash).
func RowRemembersCrashOn(rows []types.InstanceStatus, rev string) bool {
	for i := range rows {
		row := &rows[i]
		if row.Phase == types.InstancePhaseReady && row.RunningRevision == rev && types.RemembersCrash(row) {
			return true
		}
	}
	return false
}

// RetryBlockPruneOnPromote removes the RetryBlock for rev once the
// subject converges there: a converged subject leaves no active block.
// A subject has not converged while a row Ready on rev remembers a crash
// of its promoted pod set (RowRemembersCrashOn): the block counts that
// crash and paces its rebuild, and the prune of a block its rows outlived
// clears it once they do. A Held block is never pruned here: one row
// converging on a revision whose ladder holds, on an attempt admitted
// before the hold, does not make the revision sound, and the hold lasts
// until a different target revision arrives or an operator releases it.
// No-op when the adapter did not wire MutateRetryBlock, or rev is empty.
func RetryBlockPruneOnPromote(ctx context.Context, input types.ReconcileInput, rev string) error {
	if input.MutateRetryBlock == nil || rev == "" || RowRemembersCrashOn(input.ObservedState.InstanceStatuses, rev) {
		return nil
	}
	if err := input.MutateRetryBlock(ctx, rev, func(b *types.RetryBlock) types.RetryBlockDisposition {
		if b.State == types.RetryBlockHeld {
			return types.RetryBlockUnchanged
		}
		return types.RetryBlockRemove
	}); err != nil {
		return fmt.Errorf("prune retry block (rev=%s): %w", rev, err)
	}
	return nil
}
