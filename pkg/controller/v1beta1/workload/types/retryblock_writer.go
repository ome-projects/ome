package types

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RecordUpdateFailureInRetryBlock upserts the RetryBlock for targetRev
// after a terminal same-target attempt failure (failed update rollout,
// deadline-disposed create/update attempt). cause is the failure's
// attribution (FailureCauseOf over its evidence) and selects the
// transition ApplyUpdateFailureToRetryBlock applies. Wave counting: an
// existing Backoff block means this wave already recorded — refresh
// evidence only. No-op when the adapter did not wire MutateRetryBlock.
// Callers hold the writer-ordering invariant: the failed attempt's
// Operation is cleared in the same transition (block write first — a
// crash between the two re-enters the caller's failed branch, where the
// wave dedup refreshes without recounting).
//
// Lives in the leaf types package so both workload/ops (gang abandon)
// and the workload-root disposition share ONE implementation without
// closing the workload → workload/ops import cycle.
func RecordUpdateFailureInRetryBlock(ctx context.Context, input ReconcileInput, targetRev, reason string, cause FailureCause) error {
	return RecordUpdateFailureInRetryBlockAt(ctx, input, targetRev, reason, cause, metav1.NewTime(input.Now()))
}

// RecordUpdateFailureInRetryBlockAt is RecordUpdateFailureInRetryBlock
// with the wave dated at now, for a caller that dates its own record of
// the same failure alike.
func RecordUpdateFailureInRetryBlockAt(ctx context.Context, input ReconcileInput, targetRev, reason string, cause FailureCause, now metav1.Time) error {
	if input.MutateRetryBlock == nil || targetRev == "" {
		return nil
	}
	// heldAttempts captures the Held transition inside the mutate; the
	// warning is emitted only after the write COMMITS so RMW conflict
	// retries cannot duplicate the event.
	var heldAttempts int32
	err := input.MutateRetryBlock(ctx, targetRev, func(b *RetryBlock) RetryBlockDisposition {
		var disposition RetryBlockDisposition
		disposition, heldAttempts = ApplyUpdateFailureToRetryBlock(b, input.UpdateRetryPolicy, now, reason, cause)
		return disposition
	})
	if err == nil && heldAttempts > 0 && input.WarnRetryHeld != nil {
		input.WarnRetryHeld(targetRev, heldAttempts, reason)
	}
	return err
}

// ApplyUpdateFailureToRetryBlock applies one terminal failure wave to a
// RetryBlock value. It is pure apart from mutating b, so callers can compose
// the transition into a larger atomic owner-status update. heldAttempts is
// non-zero only for a new transition into Held.
//
// There is one ladder: every failed attempt at the revision advances
// AttemptsStarted and the policy decides Backoff (persisted NextRetryAt)
// or Held at MaxAttempts, whether the revision is blamed for the failure
// (a pull failure) or not (a crash loop, a runtime start rejection,
// readiness never reached, an elapsed deadline). The cause decides only
// what happens where the ladder cannot: an environment cause — a
// scheduler hold, a gang name another controller owns — says nothing
// about the revision and never touches the block; with a nil policy a
// workload-caused wave Holds at once (fail-safe) while an unattributed
// wave is left unrecorded; and a Held block keeps the evidence that
// justified the hold unless the wave blames the revision outright.
func ApplyUpdateFailureToRetryBlock(b *RetryBlock, policy *RetryPolicy, now metav1.Time, reason string, cause FailureCause) (RetryBlockDisposition, int32) {
	if b == nil || cause == CauseEnvironment {
		return RetryBlockUnchanged, 0
	}
	if cause != CauseWorkload && (policy == nil || b.State == RetryBlockHeld) {
		return RetryBlockUnchanged, 0
	}
	if b.FirstFailureAt == nil {
		b.FirstFailureAt = &now
	}
	b.LastFailureAt = &now
	b.Reason = reason
	switch b.State {
	case RetryBlockBackoff, RetryBlockHeld:
		// This wave is already recorded or terminally held; refresh only the
		// failure evidence.
		return RetryBlockPersist, 0
	}
	b.AttemptsStarted++
	if policy.Exhausted(b.AttemptsStarted) {
		b.State = RetryBlockHeld
		b.NextRetryAt = nil
		return RetryBlockPersist, b.AttemptsStarted
	}
	b.State = RetryBlockBackoff
	next := metav1.NewTime(now.Add(policy.NextRetryDelay(b.AttemptsStarted)))
	b.NextRetryAt = &next
	return RetryBlockPersist, 0
}
