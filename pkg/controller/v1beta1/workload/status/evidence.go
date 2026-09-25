package status

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The evidence writes: what the engine observed, as opposed to what it
// decided. LastFailure is the diagnostic record, Operation.Waiting is
// the authority currently reporting the row's wait, and
// Operation.CapacityRefusedAt is the one refusal no later pass can read
// off the cluster. None of the three is an edge trigger — the decision
// fields are — and no stamp here writes a phase or an operation.

// RecordWaiting records token as the operation's Waiting token, with the
// authority's evidence alongside it when there is any.
//
// mayOwn is the authority's own rule about which rows its wait can be
// true of, and takes is its precedence rule against an incumbent token.
// Both are evaluated INSIDE the mutation, against the row the write will
// actually land on: the pass decided from an observation that can be
// stale by the time the callback runs, and a row that has since entered
// teardown must not be given a hold — or have its own termination record
// overwritten — on the strength of a read that predates it.
//
// Reports two things about the row the write landed on: entered, the
// transition INTO the hold (the edge a caller announces), and held,
// whether the row carries THIS token afterwards.
//
// Evidence is compared by which pod is held, why, and since when —
// deliberately not by its message: quota names current usage, the
// scheduler names current node counts, and both move continuously, so
// storing either would rewrite status on every pass for as long as the
// wait lasts.
func RecordWaiting(
	ctx context.Context,
	input types.ReconcileInput,
	idx int32,
	token string,
	mayOwn func(types.RowOwner) bool,
	takes func(token, current string) bool,
	evidence *types.InstanceTermination,
) (entered, held bool, err error) {
	err = input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil {
			return false
		}
		if !mayOwn(types.OwnerOfOperation(s.Operation)) {
			return false
		}
		current := s.Operation.Waiting
		if !takes(token, current) {
			return false
		}
		held = true
		if current == token && sameEvidence(s.LastFailure, evidence) {
			return false
		}
		op := *s.Operation
		op.Waiting = token
		s.Operation = &op
		entered = current != token
		if evidence != nil {
			captured := *evidence
			s.LastFailure = &captured
		}
		return true
	})
	return entered, held, err
}

// ReleaseWaiting retires token once its authority reports the wait over.
// Another authority's token, or none at all, is left untouched: clearing
// unconditionally would let one authority's completion re-arm a deadline
// another authority is still holding.
func ReleaseWaiting(ctx context.Context, input types.ReconcileInput, idx int32, token string) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil || s.Operation.Waiting != token {
			return false
		}
		op := *s.Operation
		op.Waiting = ""
		s.Operation = &op
		return true
	})
}

// RecordLastFailure stamps the row's LastFailure with the captured
// termination. Idempotent: a no-op when an identical record is already
// stored, so a repeated first pass — a status conflict retried — does
// not churn the field.
func RecordLastFailure(ctx context.Context, input types.ReconcileInput, idx int32, t *types.InstanceTermination) error {
	if t == nil {
		return nil
	}
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if sameFailureIdentity(s.LastFailure, t) {
			return false
		}
		captured := *t
		s.LastFailure = &captured
		return true
	})
}

// RecordRefreshedFailure replaces a Failed row's LastFailure with
// termination when the recorded reason is still the generic one, and
// touches nothing else. The recorded time survives: what changes is the
// cause, not when the row failed, and moving the stamp forward would
// both misdate the failure and make a row that is merely wedged look
// like it keeps failing anew.
//
// The eligibility test is evaluated against the row the write actually
// lands on: the pass-start observation can be stale, and a row that has
// since left Failed or recorded a concrete cause of its own must keep
// what it has. A concrete reason is the FIRST named cause, and an
// operator reading it must not have it replaced by whatever state the
// pod happens to be in later.
func RecordRefreshedFailure(termination *types.InstanceTermination, generic string) func(*types.InstanceStatus) bool {
	return func(row *types.InstanceStatus) bool {
		if row.Phase != types.InstancePhaseFailed {
			return false
		}
		if row.LastFailure == nil || row.LastFailure.Reason != generic {
			return false
		}
		captured := *termination
		captured.Time = row.LastFailure.Time
		row.LastFailure = &captured
		return true
	}
}

// sameFailureIdentity reports whether two termination records name the
// same operator-relevant failure: pod, container, reason, exit code.
// Time and Message are excluded so a re-capture differing only in when
// it was taken writes nothing.
//
// Narrower than sameEvidence, which asks whether the evidence an
// escalation recorded still matches what the pass observes; this asks
// whether the row already names this failure.
func sameFailureIdentity(a, b *types.InstanceTermination) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.PodName != b.PodName || a.ContainerName != b.ContainerName || a.Reason != b.Reason {
		return false
	}
	switch {
	case a.ExitCode == nil && b.ExitCode == nil:
		return true
	case a.ExitCode == nil || b.ExitCode == nil:
		return false
	default:
		return *a.ExitCode == *b.ExitCode
	}
}

// sameEvidence reports whether the recorded evidence already states this
// hold: the same pod, held for the same reason, since the same moment.
// Including the moment is what lets a same-name pod recreated later
// start a fresh record instead of inheriting the old episode's. A caller
// supplying no evidence has nothing to compare, so the token alone
// decides.
//
// Not the same predicate as sameFailureIdentity, and the two may not be
// folded: this one keys on the moment, so the same pod held again later
// opens a new episode, and it reads a nil fresh record as "nothing to
// say". sameFailureIdentity asks the narrower question of whether the
// row already names this failure — container and exit code, never the
// moment, since a re-capture of one failure must not churn the field.
func sameEvidence(recorded, fresh *types.InstanceTermination) bool {
	if fresh == nil {
		return true
	}
	return recorded != nil &&
		recorded.Reason == fresh.Reason &&
		recorded.PodName == fresh.PodName &&
		recorded.Time.Equal(&fresh.Time)
}

// RecordCapacityRefusal persists an admission quota refusal on the
// operation. It reports whether this write recorded it, and the token
// the row was reporting when the write landed: the caller announces the
// refusal only when it will become the row's reported wait, so a row
// already queued behind another authority's fact is not announced twice.
//
// A refusal is the apiserver's answer to the create this pass just made:
// no later pass can read it off the cluster, so it is recorded here for
// the hold pass to report as a token and for the deadline-parking step
// to park on. The record carries the moment and nothing else — a quota
// message names current usage and the pod that lost the race, so it
// differs on every pass and would rewrite status for as long as the wait
// lasts.
func RecordCapacityRefusal(ctx context.Context, input types.ReconcileInput, idx int32) (recorded bool, incumbent string, err error) {
	err = input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil || s.Operation.CapacityRefusedAt != nil {
			return false
		}
		op := *s.Operation
		at := metav1.NewTime(input.Now())
		op.CapacityRefusedAt = &at
		s.Operation = &op
		recorded, incumbent = true, op.Waiting
		return true
	})
	return recorded, incumbent, err
}

// ClearCapacityRefusal retires the record once the create path completes
// without a quota refusal — the edge that re-arms the parked deadline
// from that moment, and the edge the hold pass releases its token on.
func ClearCapacityRefusal(ctx context.Context, input types.ReconcileInput, idx int32) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil || s.Operation.CapacityRefusedAt == nil {
			return false
		}
		op := *s.Operation
		op.CapacityRefusedAt = nil
		s.Operation = &op
		return true
	})
}
