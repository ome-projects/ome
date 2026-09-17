package types

import (
	"errors"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// APIRejectionClass is how the workload engine reads an apiserver
// rejection of a pod create or patch. The class — not the HTTP code —
// decides whether the attempt is over, paced, or simply retried.
type APIRejectionClass int

const (
	// APIRejectionTransient is the default: the rejection carries no
	// machine-usable meaning (Conflict, NotFound, timeouts, anything
	// unrecognized). The caller returns the error opaquely and
	// controller-runtime retries with its own backoff.
	APIRejectionTransient APIRejectionClass = iota
	// APIRejectionThrottled — the apiserver asked the client to slow
	// down (429 / 503). The attempt is intact: no failure accounting,
	// no deadline change, retry after the server's suggested delay.
	APIRejectionThrottled
	// APIRejectionCapacityBlocked — admission refused the object for
	// lack of quota. The workload is correct and the attempt is intact;
	// it is waiting on capacity an operator or another workload
	// controls, so the operation's deadline parks while it waits.
	APIRejectionCapacityBlocked
	// APIRejectionPermanentWorkload — the pod template itself is
	// unacceptable (422 Invalid). Retrying the same revision reproduces
	// the rejection byte for byte, so the attempt ends and the revision
	// is held for retry until a corrected one is published.
	APIRejectionPermanentWorkload
	// APIRejectionPermanentEnvironment — the environment refuses the
	// object for a reason no revision can fix (the namespace is being
	// terminated). The attempt ends, but the revision is blameless and
	// keeps a clean retry ladder.
	APIRejectionPermanentEnvironment
)

// Permanent reports whether the class ends the attempt. Permanent
// classes dispose the Instance (Operation cleared, Phase=Failed);
// the rest leave it in flight.
func (c APIRejectionClass) Permanent() bool {
	return c == APIRejectionPermanentWorkload || c == APIRejectionPermanentEnvironment
}

// Rejection reasons recorded on InstanceTermination.Reason (permanent
// classes) or InstanceOperation.Waiting (the capacity-blocked waiting
// token). They live beside the kubelet waiting-reason set in
// failurecause.go for the same reason: they are fixed Kubernetes API
// semantics, identical on every cluster, not operator-tunable behavior.
const (
	// RejectionReasonInvalidPodSpec marks a 422 Invalid on a pod create
	// or patch. WORKLOAD-CAUSED: the rejected field belongs to the pod
	// template, which travels with the revision, so it charges the
	// revision's retry ladder (see workloadCausedWaitingReasons).
	RejectionReasonInvalidPodSpec = "InvalidPodSpec"
	// RejectionReasonNamespaceTerminating marks a create refused because
	// the namespace is being deleted. ENVIRONMENT-CAUSED: no revision
	// can satisfy it, so it never charges a retry ladder.
	RejectionReasonNamespaceTerminating = "NamespaceTerminating"
	// RejectionReasonQuotaExceeded marks a ResourceQuota admission
	// refusal. Recorded as the operation's Waiting token, never
	// as a failure.
	RejectionReasonQuotaExceeded = "QuotaExceeded"
	// RejectionReasonThrottled marks a 429 / 503. Diagnostic only — the
	// throttled class writes no status.
	RejectionReasonThrottled = "Throttled"
)

// quotaExceededMarker is the phrase the apiserver's ResourceQuota
// admission plugin puts in the Forbidden message it returns. Matching on
// it is what separates "out of quota" (capacity, transient) from an RBAC
// refusal (which stays an opaque error). Fixed upstream wording, like
// the kubelet waiting reasons — not a tunable.
const quotaExceededMarker = "exceeded quota"

// APIRejection is the classified form of one apiserver rejection.
type APIRejection struct {
	Class APIRejectionClass
	// Reason is the grep-stable token recorded on status (one of the
	// RejectionReason* constants). Empty for the transient class, which
	// records nothing.
	Reason string
	// Message is the apiserver's own explanation, carried verbatim so
	// `kubectl describe` shows the operator which field or quota the
	// apiserver objected to.
	Message string
	// RetryAfter is the delay the server suggested (Retry-After). Zero
	// when the server suggested none; the caller then keeps its own
	// pacing.
	RetryAfter time.Duration
}

// ClassifyAPIError reads one apiserver error into the policy the
// workload engine applies to it. Pure: every branch is a statement about
// Kubernetes API semantics, so the same error classifies identically on
// every cluster.
//
// Order matters. A namespace-terminating refusal is also a 403 Forbidden,
// so it is recognized by its status cause before the quota-message match
// can claim it.
func ClassifyAPIError(err error) APIRejection {
	if err == nil {
		return APIRejection{}
	}
	message := apiErrorMessage(err)
	switch {
	case apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause):
		return APIRejection{
			Class:   APIRejectionPermanentEnvironment,
			Reason:  RejectionReasonNamespaceTerminating,
			Message: message,
		}
	case apierrors.IsInvalid(err):
		return APIRejection{
			Class:   APIRejectionPermanentWorkload,
			Reason:  RejectionReasonInvalidPodSpec,
			Message: message,
		}
	case apierrors.IsForbidden(err) && strings.Contains(message, quotaExceededMarker):
		return APIRejection{
			Class:   APIRejectionCapacityBlocked,
			Reason:  RejectionReasonQuotaExceeded,
			Message: message,
		}
	case apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err):
		rejection := APIRejection{
			Class:   APIRejectionThrottled,
			Reason:  RejectionReasonThrottled,
			Message: message,
		}
		if seconds, ok := apierrors.SuggestsClientDelay(err); ok && seconds > 0 {
			rejection.RetryAfter = time.Duration(seconds) * time.Second
		}
		return rejection
	}
	return APIRejection{}
}

// OperationCapacityBlocked reports whether an operation is parked on the
// capacity-blocked waiting token — the create path could not place its
// pods because admission refused them for lack of quota. The deadline
// parking step and the escalation pass read this the way they read an
// admission scheduling gate: the wait is external, so the
// InstanceReadyTimeout clock must not run.
//
// The token lives in Operation.Waiting, never in Operation.Reason:
// Reason names why the operation exists and is part of the
// terminal-finalize identity tuple, so a quota blip written there would
// both erase a restart/revision-roll cause and read as the operation
// having been replaced.
func OperationCapacityBlocked(op *InstanceOperation) bool {
	return op != nil && op.Waiting == RejectionReasonQuotaExceeded
}

// APIPacing is the pass-scoped sink for pacing the apiserver asked for:
// the longest Retry-After it suggested while refusing a write. It exists
// because the op state machines report progress as (done, error) — a
// throttled write is neither — so the delay is deposited here and the
// dispatcher floors the pass's requeue with it. All methods are nil-safe.
type APIPacing struct {
	throttle time.Duration
}

// ObserveThrottle records a server-suggested delay, keeping the longest
// one seen this pass: waking before the slowest request's suggestion just
// re-earns its rejection.
func (p *APIPacing) ObserveThrottle(retryAfter time.Duration) {
	if p == nil || retryAfter <= p.throttle {
		return
	}
	p.throttle = retryAfter
}

// Throttle returns the delay the pass must wait at minimum, or zero when
// the apiserver never asked for room.
func (p *APIPacing) Throttle() time.Duration {
	if p == nil {
		return 0
	}
	return p.throttle
}

// apiErrorMessage returns the apiserver's Status message when the error
// carries one, falling back to the Go error text. Trimmed so the value
// is safe to embed in a status field or an event.
func apiErrorMessage(err error) string {
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		if msg := strings.TrimSpace(status.Status().Message); msg != "" {
			return msg
		}
	}
	return strings.TrimSpace(err.Error())
}
