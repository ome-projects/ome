package evidence

import (
	"errors"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// quotaExceededMarker is the phrase the apiserver's ResourceQuota
// admission plugin puts in the Forbidden message it returns. Matching on
// it is what separates "out of quota" (capacity, transient) from an RBAC
// refusal (which stays an opaque error). Fixed upstream wording, like
// the kubelet waiting reasons — not a tunable.
const quotaExceededMarker = "exceeded quota"

// ClassifyAPIError reads one apiserver error into the policy the
// workload engine applies to it. Pure: every branch is a statement about
// Kubernetes API semantics, so the same error classifies identically on
// every cluster.
//
// Order matters. A namespace-terminating refusal is also a 403 Forbidden,
// so it is recognized by its status cause before the quota-message match
// can claim it.
func ClassifyAPIError(err error) types.APIRejection {
	if err == nil {
		return types.APIRejection{}
	}
	message := apiErrorMessage(err)
	switch {
	case apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause):
		return types.APIRejection{
			Class:   types.APIRejectionPermanentEnvironment,
			Reason:  types.RejectionReasonNamespaceTerminating,
			Message: message,
		}
	case apierrors.IsInvalid(err):
		return types.APIRejection{
			Class:   types.APIRejectionPermanentWorkload,
			Reason:  types.RejectionReasonInvalidPodSpec,
			Message: message,
		}
	case apierrors.IsForbidden(err) && strings.Contains(message, quotaExceededMarker):
		return types.APIRejection{
			Class:   types.APIRejectionCapacityBlocked,
			Reason:  types.RejectionReasonQuotaExceeded,
			Message: message,
		}
	case apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err):
		rejection := types.APIRejection{
			Class:   types.APIRejectionThrottled,
			Reason:  types.RejectionReasonThrottled,
			Message: message,
		}
		if seconds, ok := apierrors.SuggestsClientDelay(err); ok && seconds > 0 {
			rejection.RetryAfter = time.Duration(seconds) * time.Second
		}
		return rejection
	}
	return types.APIRejection{}
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
