package types

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RecordUpdateFailureInRetryBlock upserts the RetryBlock for targetRev
// after a terminal same-target attempt failure (failed update rollout,
// deadline-disposed create/update attempt). workloadCaused is the
// failure's cause attribution (IsWorkloadCausedReason over its
// evidence) and selects the transition ApplyUpdateFailureToRetryBlock
// applies. Wave counting: an existing Backoff block means this wave
// already recorded — refresh evidence only. No-op when the adapter did
// not wire MutateRetryBlock. Callers hold the writer-ordering invariant:
// the failed attempt's Operation is cleared in the same transition
// (block write first — a crash between the two re-enters the caller's
// failed branch, where the wave dedup refreshes without recounting).
//
// Lives in the leaf types package so both workload/ops (gang abandon)
// and the workload-root disposition share ONE implementation without
// closing the workload → workload/ops import cycle.
func RecordUpdateFailureInRetryBlock(ctx context.Context, input ReconcileInput, targetRev, reason string, workloadCaused bool) error {
	if input.MutateRetryBlock == nil || targetRev == "" {
		return nil
	}
	now := metav1.NewTime(input.Now())
	// heldAttempts captures the Held transition inside the mutate; the
	// warning is emitted only after the write COMMITS so RMW conflict
	// retries cannot duplicate the event.
	var heldAttempts int32
	err := input.MutateRetryBlock(ctx, targetRev, func(b *RetryBlock) RetryBlockDisposition {
		var disposition RetryBlockDisposition
		disposition, heldAttempts = ApplyUpdateFailureToRetryBlock(b, input.UpdateRetryPolicy, now, reason, workloadCaused)
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
// A workload-caused wave charges the ladder: AttemptsStarted advances and
// the policy decides Backoff (persisted NextRetryAt) or Held; a nil policy
// is always exhausted, so it Holds. Any other wave — an elapsed deadline,
// an ambiguous kubelet reason, anything the revision cannot be blamed
// for — only paces the next attempt: the block enters Backoff for the
// delay of its current ladder rung while AttemptsStarted stays put, so no
// number of such waves reaches Held. With a nil policy there is no pacing
// ladder to apply, and a Held block keeps the revision-blaming evidence
// that justified the hold; an uncharged wave leaves both untouched.
func ApplyUpdateFailureToRetryBlock(b *RetryBlock, policy *RetryPolicy, now metav1.Time, reason string, workloadCaused bool) (RetryBlockDisposition, int32) {
	if b == nil {
		return RetryBlockUnchanged, 0
	}
	if !workloadCaused && (policy == nil || b.State == RetryBlockHeld) {
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
	if !workloadCaused {
		// Pace at the current rung — the delay the next charged attempt
		// would wait — without moving the count that reaches Held.
		b.State = RetryBlockBackoff
		next := metav1.NewTime(now.Add(policy.NextRetryDelay(max(b.AttemptsStarted, 1))))
		b.NextRetryAt = &next
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
