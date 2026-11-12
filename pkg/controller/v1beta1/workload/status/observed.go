package status

import (
	"context"
	"strings"

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
//
// The reading a wait supplies never replaces a record that names a
// container. Such a record is the death of the pod set before this
// attempt, written when the repair opened, and the retry schedule reads
// it to tell the rebuilt set's death from a first death
// (ops.RebuiltAfterFailure); the wait is a state of the attempt in
// flight, reported by the token, while the exit is a fact about the set
// that died. The two sit side by side on the row, and observing the same
// wait again on such a row writes nothing.
//
// placementSince, when set, is also recorded as the attempt's placement
// wait (Operation.PlacementWaitSince) if the operation holds none yet, in
// the same write, so entering a placement wait costs one status write. It
// is recorded even when another authority holds the token: the wait
// belongs to the attempt, whichever authority reports it.
func RecordWaiting(
	ctx context.Context,
	input types.ReconcileInput,
	idx int32,
	token string,
	mayOwn func(types.RowOwner) bool,
	takes func(token, current string) bool,
	evidence *types.InstanceTermination,
	placementSince *metav1.Time,
) (entered, held bool, err error) {
	err = input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil {
			return false
		}
		if !mayOwn(types.OwnerOfOperation(s.Operation)) {
			return false
		}
		placing := placementSince != nil && s.Operation.PlacementWaitSince == nil
		current := s.Operation.Waiting
		if !takes(token, current) {
			if !placing {
				return false
			}
			op := *s.Operation
			op.PlacementWaitSince = placementSince.DeepCopy()
			s.Operation = &op
			return true
		}
		held = true
		kept := deathRecordKept(s.LastFailure, evidence)
		unchanged := current == token && (kept || sameEvidence(s.LastFailure, evidence))
		if unchanged && !placing {
			return false
		}
		op := *s.Operation
		op.Waiting = token
		if placing {
			op.PlacementWaitSince = placementSince.DeepCopy()
		}
		s.Operation = &op
		entered = current != token
		if evidence != nil && !kept && !unchanged {
			captured := *evidence
			s.LastFailure = &captured
		}
		return true
	})
	return entered, held, err
}

// deathRecordKept reports whether a wait's reading leaves the row's
// recorded failure in place: the record names a container, so it is the
// death of a pod set, and the reading names none, so it is a wait of the
// attempt in flight and not a death of its own. A record naming no
// container is an earlier wait or a pod-level failure, which the fresh
// wait replaces as before.
func deathRecordKept(recorded, reading *types.InstanceTermination) bool {
	return reading != nil && reading.ContainerName == "" &&
		recorded != nil && recorded.ContainerName != ""
}

// ReleaseWaiting retires token once its authority reports the wait over.
// Another authority's token, or none at all, is left untouched: clearing
// unconditionally would let one authority's completion re-arm a deadline
// another authority is still holding. clearPlacement also retires the
// attempt's placement wait in the same write, once every pod the attempt
// asks for runs on a node.
func ReleaseWaiting(ctx context.Context, input types.ReconcileInput, idx int32, token string, clearPlacement bool) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil || s.Operation.Waiting != token {
			return false
		}
		op := *s.Operation
		op.Waiting = ""
		if clearPlacement {
			op.PlacementWaitSince = nil
		}
		s.Operation = &op
		return true
	})
}

// RecordDatedFailure stamps the row's LastFailure with the captured
// termination, dated as captured: a stored record that differs only in
// its time is rewritten, so a reader anchored on the time (a crash of
// the row's promoted pod set remembered against the row's Ready and the
// block's first failure) reads this sighting. Idempotent within a pass:
// the same record at the same time writes once.
func RecordDatedFailure(ctx context.Context, input types.ReconcileInput, idx int32, t *types.InstanceTermination) error {
	if t == nil {
		return nil
	}
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if sameFailureIdentity(s.LastFailure, t) && s.LastFailure.Time.Equal(&t.Time) {
			return false
		}
		captured := *t
		s.LastFailure = &captured
		return true
	})
}

// RecordCrashLoopNote appends note once to the failure message of a
// Ready, operation-free row, beside the kubelet's own message; a row
// already carrying it, or in any other shape, is left untouched.
func RecordCrashLoopNote(ctx context.Context, input types.ReconcileInput, idx int32, note string) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.Phase != types.InstancePhaseReady || s.Operation != nil || s.LastFailure == nil || note == "" {
			return false
		}
		return noteFailure(s, note)
	})
}

// ClearCrashLoopNote removes note from the row's failure message and
// touches nothing else: the record stays as the history of the crash,
// dated as it was. A row that does not carry the note is left untouched.
func ClearCrashLoopNote(ctx context.Context, input types.ReconcileInput, idx int32, note string) error {
	return input.MutateInstance(ctx, idx, func(s *types.InstanceStatus) bool {
		if s.LastFailure == nil || note == "" {
			return false
		}
		return unnoteFailure(s, note)
	})
}

// FailureNoted reports whether the row's failure message carries note.
func FailureNoted(s *types.InstanceStatus, note string) bool {
	return s != nil && s.LastFailure != nil && note != "" && strings.Contains(s.LastFailure.Message, note)
}

// noteFailure appends note to the row's failure message on a copy of the
// record, so the observed row the pass decided from is not aliased.
// Reports whether it changed the row.
func noteFailure(s *types.InstanceStatus, note string) bool {
	if FailureNoted(s, note) {
		return false
	}
	failure := *s.LastFailure
	if failure.Message == "" {
		failure.Message = note
	} else {
		failure.Message += "; " + note
	}
	s.LastFailure = &failure
	return true
}

// unnoteFailure removes note, and the separator that joined it, from the
// row's failure message on a copy of the record. Reports whether it
// changed the row.
func unnoteFailure(s *types.InstanceStatus, note string) bool {
	if !FailureNoted(s, note) {
		return false
	}
	failure := *s.LastFailure
	switch {
	case failure.Message == note:
		failure.Message = ""
	case strings.HasSuffix(failure.Message, "; "+note):
		failure.Message = strings.TrimSuffix(failure.Message, "; "+note)
	case strings.HasPrefix(failure.Message, note+"; "):
		failure.Message = strings.TrimPrefix(failure.Message, note+"; ")
	default:
		failure.Message = strings.Replace(failure.Message, "; "+note, "", 1)
	}
	s.LastFailure = &failure
	return true
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

// ClearPlacementWait retires the moment once every pod the attempt asks
// for runs on a node, or once the attempt ends.
func ClearPlacementWait(ctx context.Context, input types.ReconcileInput, idx int32) error {
	return input.MutateInstance(ctx, idx, ClearPlacementWaitMutation())
}

// ClearPlacementWaitMutation is ClearPlacementWait as a row mutation, for
// a pass that batches its writes. A row holding no moment is left alone.
func ClearPlacementWaitMutation() func(*types.InstanceStatus) bool {
	return func(s *types.InstanceStatus) bool {
		return s.Phase != "" && clearPlacementWait(s)
	}
}
