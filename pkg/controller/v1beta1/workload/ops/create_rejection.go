package ops

import (
	"context"
	"errors"
	"fmt"

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
//   - capacity-blocked: the Instance's operation carries the quota
//     waiting token and is still in flight. The caller keeps going with
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
//   - capacity-blocked: the quota wait is recorded on the Operation and
//     its deadline parks; the operation's own requeue interval retries.
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
	if observed := findInstanceStatus(input.ObservedState.InstanceStatuses, idx); observed != nil {
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
	held, err := workload.DisposeAPIRejection(ctx, input, inst, rejection, blameRevision)
	if err != nil {
		return err
	}
	detail := "no revision is blamed"
	if held != "" {
		detail = fmt.Sprintf("revision %s held for retry", held)
	}
	recordWarning(deps.Recorder, eventTarget(input), workload.EventReasonInstanceRejected,
		"OMENative %s: apiserver rejected pod %s (%s: %s); %s",
		instanceKey(input.Key.Component, idx), podName, rejection.Reason, rejection.Message, detail)
	return nil
}

// markCapacityBlocked records an admission quota refusal as the
// operation's waiting token. The token is what parks the
// InstanceReadyTimeout clock (see workload.ReconcileGatedDeadlines): the
// Instance is queued behind capacity an operator controls, not stuck.
//
// Edge-triggered on the blocked STATE, not on the message: a quota
// message names the current usage and the pod that lost the race, so it
// differs on every pass. Storing it would rewrite status and re-announce
// the same episode for as long as the namespace stays full. The message
// reaches operators once, through the event the caller emits when this
// reports entered=true.
func markCapacityBlocked(ctx context.Context, input workload.ReconcileInput, idx int32) (bool, error) {
	entered := false
	err := input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil || workload.OperationCapacityBlocked(s.Operation) {
			return false
		}
		s.Operation.Waiting = workload.RejectionReasonQuotaExceeded
		entered = true
		return true
	})
	return entered, err
}

// clearCapacityBlock releases the quota waiting token once the create
// path completes without a quota refusal — the edge that re-arms the
// parked deadline from that moment. Completion, not the count of pods
// created, is the signal: an Instance whose targets all came back
// AlreadyExists creates nothing yet is plainly no longer blocked.
//
// Edge-triggered off the observation, so a pass that was never blocked
// writes nothing.
func clearCapacityBlock(ctx context.Context, input workload.ReconcileInput, idx int32) error {
	observed := findInstanceStatus(input.ObservedState.InstanceStatuses, idx)
	if observed == nil || !workload.OperationCapacityBlocked(observed.Operation) {
		return nil
	}
	return input.MutateInstance(ctx, idx, func(s *workload.InstanceStatus) bool {
		if s.Phase == "" || s.Operation == nil || !workload.OperationCapacityBlocked(s.Operation) {
			return false
		}
		s.Operation.Waiting = ""
		return true
	})
}
