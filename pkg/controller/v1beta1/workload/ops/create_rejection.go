package ops

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Apiserver-rejection handling for the shared create path. Every create
// in the engine funnels through createMissingPods, so a rejection is read
// here — once — into the outcome the whole machine agrees on, instead of
// being handed back opaquely and retried on blind controller-runtime
// backoff until the operation deadline reports a misleading timeout.

type podCreateError struct {
	podName string
	err     error
}

func (e *podCreateError) Error() string {
	return fmt.Sprintf("create pod %s: %v", e.podName, e.err)
}

func (e *podCreateError) Unwrap() error {
	return e.err
}

// podRejectionError reports that the apiserver CLASSIFIED its refusal of a
// pod create, so the caller must not treat it as an opaque failure to
// retry on blind backoff. It is returned for every non-transient class:
//
//   - a permanent class (disposed==true): the Instance has ALREADY been
//     disposed Failed, with its RetryBlock recorded when the rejection
//     blames the revision. The caller stops working that Instance and
//     moves on; the pass itself has not failed.
//   - capacity-blocked: the refusal is recorded on the Instance's
//     operation, which is still in flight. The caller keeps going with
//     the other Instances and retries on the ordinary create interval.
//   - throttled: nothing was written. The caller stops creating this pass
//     and wakes after the server's suggested delay, already deposited on
//     the pass pacing.
type podRejectionError struct {
	podName   string
	rejection workload.APIRejection
	// disposed reports that the Instance was already failed by this
	// rejection, so no further action is owed for it this pass.
	disposed bool
	err      error
}

func (e *podRejectionError) Error() string {
	return fmt.Sprintf("create pod %s rejected (%s): %v", e.podName, e.rejection.Reason, e.err)
}

func (e *podRejectionError) Unwrap() error {
	return e.err
}

// asPodRejection extracts the classified rejection a create path attached
// to err, if any.
func asPodRejection(err error) (*podRejectionError, bool) {
	var rejected *podRejectionError
	if errors.As(err, &rejected) {
		return rejected, true
	}
	return nil, false
}

// createRejectionHandled reports whether err is a create rejection the
// shared create path already acted on. Callers that drive one Instance
// end their pass quietly on it, WITHOUT surfacing an error:
//
//   - permanent: the Instance is disposed Failed; nothing further is
//     possible until a corrected revision (or a repaired environment).
//   - capacity-blocked: the quota refusal is recorded on the Operation
//     and its deadline parks; the operation's own requeue interval
//     retries.
//   - throttled: the server's delay is on the pass pacing, which floors
//     that requeue.
//
// Surfacing any of these as an error instead would hand the retry to
// controller-runtime's escalating backoff — the blind wait this whole
// classification exists to remove.
func createRejectionHandled(err error) bool {
	_, ok := asPodRejection(err)
	return ok
}

// disposeRejectedAttempt ends the Instance's attempt on a permanent
// apiserver rejection of one of its pods — a create or an in-place
// patch: LastFailure from the rejection, the Operation cleared,
// Phase=Failed, plus a RetryBlock against the target revision when
// blameRevision allows it. Emits the operator-facing Warning naming the
// pod and the apiserver's own explanation.
//
// blameRevision is false where the rejected pod is not a faithful render
// of the target revision — a migration surge carries a placement overlay
// the revision never asked for, so a 422 there may be the overlay's
// fault and must not hold an otherwise-good revision.
func disposeRejectedAttempt(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, targetRevision string, podName string, rejection workload.APIRejection, blameRevision bool) error {
	inst := workload.InstanceStatus{Index: idx}
	if observed := input.ObservedState.Instance(idx); observed != nil {
		inst = *observed
	}
	// The in-flight attempt's pin is the authority on what is being
	// converged toward; the observation predates this pass's stamp, so the
	// caller's target fills the gap for a fresh create.
	if (inst.Operation == nil || inst.Operation.TargetRevision == "") && targetRevision != "" {
		op := workload.InstanceOperation{}
		if inst.Operation != nil {
			op = *inst.Operation
		}
		op.TargetRevision = targetRevision
		inst.Operation = &op
	}
	held, err := disposeAPIRejection(ctx, input, inst, rejection, blameRevision)
	if err != nil {
		return err
	}
	detail := "no revision is blamed"
	if held != "" {
		detail = fmt.Sprintf("revision %s held for retry", held)
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonInstanceRejected,
		"OMENative %s: apiserver rejected pod %s (%s: %s); %s",
		workload.InstanceKey(input.Key.Component, idx), podName, rejection.Reason, rejection.Message, detail)
	return nil
}

// clearCapacityRefusal retires the record once the create path completes
// without a quota refusal — the edge that re-arms the parked deadline
// from that moment, and the edge the hold pass releases its token on.
// Completion, not the count of pods created, is the signal: an Instance
// whose targets all came back AlreadyExists creates nothing yet is
// plainly no longer blocked.
//
// Edge-triggered off the observation as well as the write, so a create
// that was never refused costs no mutation at all.
func clearCapacityRefusal(ctx context.Context, input workload.ReconcileInput, idx int32) error {
	if !observedCapacityRefused(input, idx) {
		return nil
	}
	return status.ClearCapacityRefusal(ctx, input, idx)
}

// observedCapacityRefused reports whether this reconcile's observation
// of the row already carries a refusal, which is the only state a
// release can act on.
func observedCapacityRefused(input workload.ReconcileInput, idx int32) bool {
	row := input.ObservedState.Instance(idx)
	return row != nil && workload.OperationCapacityRefused(row.Operation)
}

// disposeAPIRejection ends an attempt the apiserver permanently rejected.
// There is no pod — the rejection itself is the evidence — so LastFailure
// is built from the classified rejection rather than from container
// status, and the Operation is cleared + Phase=Failed in the same single
// mutation every other terminal disposition uses.
//
// A workload-caused rejection (RejectionReasonInvalidPodSpec) additionally
// records a RetryBlock against the attempt's target revision BEFORE the
// clear, so the same revision is not re-admitted while a corrected one is.
// Writer ordering matches the deadline disposition: block first, clear
// second, so a crash between the two re-enters here and the writer's wave
// dedup refreshes the block without recounting. An environment-caused
// rejection blames no revision and records no block.
//
// blameRevision lets a caller withhold that blame when the rejected pod
// is not a faithful render of the target revision — a migration surge
// carries a placement overlay the revision never asked for, so holding
// the revision for the overlay's fault would wedge an innocent rollout.
//
// The target revision is the attempt's Operation.TargetRevision, falling
// back to the owner's UpdateRevision for an unpinned Create (the revision
// the rejected pod was rendered from). heldRevision names the revision
// that was blocked, empty when none was.
//
// Non-permanent classes are a no-op: they carry their own pacing at the
// call site and the attempt is still alive.
func disposeAPIRejection(ctx context.Context, input workload.ReconcileInput, inst workload.InstanceStatus, rejection workload.APIRejection, blameRevision bool) (heldRevision string, err error) {
	if !rejection.Class.Permanent() {
		return "", nil
	}
	if rejection.Class == workload.APIRejectionPermanentWorkload && blameRevision {
		heldRevision = rejectionTargetRevision(input, inst)
		if heldRevision != "" {
			if err := workload.RecordUpdateFailureInRetryBlock(ctx, input, heldRevision, rejection.Reason, true); err != nil {
				return "", fmt.Errorf("record retry block for rejected attempt (instance=%d rev=%s): %w", inst.Index, heldRevision, err)
			}
		}
	}
	termination := &workload.InstanceTermination{
		Reason:  rejection.Reason,
		Message: rejection.Message,
		Time:    metav1.NewTime(input.Now()),
	}
	if err := status.StampFailed(ctx, input, inst.Index, termination); err != nil {
		return heldRevision, fmt.Errorf("clear operation + stamp Failed (instance=%d): %w", inst.Index, err)
	}
	return heldRevision, nil
}

// rejectionTargetRevision resolves the revision a rejected attempt was
// converging toward: the pin the operation carries, else the owner's
// current UpdateRevision (an unpinned Create renders from it).
func rejectionTargetRevision(input workload.ReconcileInput, inst workload.InstanceStatus) string {
	if inst.Operation != nil && inst.Operation.TargetRevision != "" {
		return inst.Operation.TargetRevision
	}
	return input.ObservedState.UpdateRevision
}

// The RetryBlock is create's admission: one gate consulted at every
// create site, at the revision each pod is for, so a Held record denies a
// fresh start, the fill of a lost member, and the remainder of an attempt
// already materializing alike. The update trigger and the restart rebuild
// consult it at the same revision, because a repair that re-materializes
// a held revision is the create the block exists to deny.

// evaluateRetryBlockGate is the single deny/allow evaluation of a
// persisted RetryBlock, shared by the update trigger gate and the
// create pass. One implementation on purpose: a deadline-disposed
// attempt leaves its instance Failed-with-no-Operation — a fresh start
// — and an ungated create would re-materialize pods at the same bad
// revision forever, bypassing the block the disposition recorded.
//
// attemptInFlight reports whether an authorized attempt at the block's
// revision is currently in flight. It distinguishes a live
// RetryInProgress authorization (deny — exactly one attempt at a time)
// from a leaked one (superseded surge, scale-down, crash), which is
// treated as due so the revision is not silently denied forever; the
// attempt stamp re-confirms the state.
//
// Returns denied plus retryAfter: >0 only for a not-yet-due Backoff
// block (re-evaluate then). Held has no time bound. A nil NextRetryAt
// is immediately due.
//
// A due Backoff allows WITHOUT flipping state — the RetryInProgress
// flip belongs to attempt-stamp time (status.RetryBlockAttemptStarted),
// after the dispatcher's budget/coordination gates admit the start.
// Flipping here would strand RetryInProgress when a budget denies the
// pass. Unrecognized states fall through un-gated (fail-open
// forward-compat).
func evaluateRetryBlockGate(b *workload.RetryBlock, now time.Time, attemptInFlight bool) (denied bool, retryAfter time.Duration) {
	if b == nil {
		return false, 0
	}
	switch b.State {
	case workload.RetryBlockHeld:
		return true, 0
	case workload.RetryBlockRetryInProgress:
		if attemptInFlight {
			return true, 0
		}
	case workload.RetryBlockBackoff:
		if b.NextRetryAt != nil && now.Before(b.NextRetryAt.Time) {
			return true, b.NextRetryAt.Time.Sub(now)
		}
	}
	return false, 0
}

// allInstances is the "no row excluded" index a caller passes when it is
// asking about the whole Component rather than re-arming one row.
const allInstances int32 = -1

// anyInFlightCreateAttempt reports whether any Instance other than except
// carries an in-flight Create attempt, read from the pass's owner grouping
// rather than re-derived here. Pass a negative index to ask about every row;
// a row re-arming its own attempt passes its index so it does not count
// itself. TargetRevision is deliberately ignored because an empty value is a
// supported persisted state and the gate must remain conservative.
func anyInFlightCreateAttempt(input workload.ReconcileInput, except int32) bool {
	return input.OwnedRows().AnyInFlight(workload.OwnerCreate, except)
}

// recordUpdateFailureInRetryBlock delegates to the shared writer in the
// types package (workload.RecordUpdateFailureInRetryBlock) — one
// implementation for the gang abandon here and the workload-root
// deadline disposition, under the package-local name the ops-side call
// sites and invariant tests use.
func recordUpdateFailureInRetryBlock(ctx context.Context, input workload.ReconcileInput, targetRev, reason string, workloadCaused bool) error {
	return workload.RecordUpdateFailureInRetryBlock(ctx, input, targetRev, reason, workloadCaused)
}

// instanceFailureReason summarizes the failure evidence the escalators
// already stamped on the instance (LastFailure) for RetryBlock.Reason:
// the kubelet detail message when present, else the compact termination
// fragment, else fallback.
func instanceFailureReason(s *workload.InstanceStatus, fallback string) string {
	if s == nil || s.LastFailure == nil {
		return fallback
	}
	if s.LastFailure.Message != "" {
		return s.LastFailure.Message
	}
	if short := s.LastFailure.ShortString(); short != "" {
		return short
	}
	return fallback
}

// instanceFailureWorkloadCaused reports whether the failure evidence the
// escalators stamped on the instance (LastFailure.Reason) blames the
// revision itself. Only that evidence charges the revision's retry
// ladder; an elapsed deadline or an ambiguous kubelet reason paces the
// next attempt without counting toward Held.
func instanceFailureWorkloadCaused(s *workload.InstanceStatus) bool {
	return s != nil && s.LastFailure != nil && workload.IsWorkloadCausedReason(s.LastFailure.Reason)
}
