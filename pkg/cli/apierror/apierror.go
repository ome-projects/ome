// Package apierror translates raw Kubernetes API errors into bounded messages
// that identify an operator action without repeating server, transport, or
// kubeconfig text.
package apierror

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/cli/safetext"
)

const (
	installationURL          = "https://github.com/ome-projects/ome#installation"
	maxRootErrorDisplayWidth = 80
	rootErrorPrefix          = "error: "
	genericMissingMessage    = "the server could not find the requested resource"
)

// ReadOperation is an allowlisted read-side operation. Unknown values are
// rendered as the fixed word "read" and are never reflected to the terminal.
type ReadOperation string

const (
	ReadOperationGet     ReadOperation = "get"
	ReadOperationList    ReadOperation = "list"
	ReadOperationResolve ReadOperation = "resolve"
)

// ReadTarget describes the safe identity of a Kubernetes read. All fields are
// validated again before display; callers must not pre-format a target string.
type ReadTarget struct {
	Operation ReadOperation
	Resource  string
	Namespace string
	Name      string
}

type safeReadError struct {
	target string
	reason string
	hint   string
	cause  error
}

func (e *safeReadError) Error() string {
	if e.hint == "" {
		return fmt.Sprintf("%s: %s", e.target, e.reason)
	}
	return fmt.Sprintf("%s: %s (%s)", e.target, e.reason, e.hint)
}

func (e *safeReadError) Unwrap() error { return e.cause }

type missingAPIError struct {
	cause error
}

func (e *missingAPIError) Error() string {
	return "OME does not appear to be installed on this cluster (the ome.io API is not available; install OME first — see " + installationURL + ")"
}

func (e *missingAPIError) Unwrap() error { return e.cause }

// SafeRead replaces untrusted error text with a fixed classification while
// retaining the original error in the unwrap chain. This preserves
// errors.Is/errors.As and Kubernetes apierrors predicates without exposing the
// cause through Error().
func SafeRead(err error, target ReadTarget) error {
	if err == nil {
		return nil
	}
	reason, hint := classify(err)
	return &safeReadError{
		target: formatReadTarget(target, readTargetWidth(reason, hint)),
		reason: reason,
		hint:   hint,
		cause:  err,
	}
}

// Friendly retains the historical package entry point. It translates a
// missing OME API into fixed actionable guidance and otherwise preserves the
// caller's error unchanged. New read paths should use SafeRead so every error
// classification is safe to render.
func Friendly(err error) error {
	if err == nil || !missingOMEAPI(err) {
		return err
	}
	return &missingAPIError{cause: err}
}

func classify(err error) (string, string) {
	switch {
	case errors.Is(err, context.Canceled):
		return "Canceled", ""
	case errors.Is(err, context.DeadlineExceeded):
		return "TimedOut", ""
	case missingOMEAPI(err):
		return "OMEAPIMissing", "install OME first"
	case kerrors.IsForbidden(err):
		return "Forbidden", ""
	case kerrors.IsUnauthorized(err):
		return "Unauthorized", ""
	case kerrors.IsNotFound(err):
		return "NotFound", ""
	case kerrors.IsConflict(err):
		return "Conflict", ""
	case kerrors.IsInvalid(err), kerrors.IsBadRequest(err):
		return "Invalid", ""
	case kerrors.IsTooManyRequests(err):
		return "TooManyRequests", ""
	case kerrors.IsTimeout(err), kerrors.IsServerTimeout(err):
		return "TimedOut", ""
	case kerrors.IsServiceUnavailable(err):
		return "ServiceUnavailable", ""
	case kerrors.IsNotAcceptable(err), kerrors.IsUnsupportedMediaType(err), kerrors.IsMethodNotSupported(err):
		return "UnsupportedAPI", ""
	case kerrors.IsGone(err), kerrors.IsResourceExpired(err):
		return "Expired", ""
	case kerrors.IsRequestEntityTooLargeError(err):
		return "ResponseTooLarge", ""
	case kerrors.IsInternalError(err):
		return "ServerError", ""
	case kerrors.IsUnexpectedServerError(err), kerrors.IsUnexpectedObjectError(err):
		return "MalformedResponse", ""
	default:
		return "Unavailable", ""
	}
}

func missingOMEAPI(err error) bool {
	if !kerrors.IsNotFound(err) {
		return false
	}
	var status kerrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	apiStatus := status.Status()
	details := apiStatus.Details
	return details == nil || details.Name == "" || clientGenericMissingResource(apiStatus)
}

func clientGenericMissingResource(status metav1.Status) bool {
	details := status.Details
	if status.Code != http.StatusNotFound || status.Reason != metav1.StatusReasonNotFound || details == nil || details.Name == "" || details.Kind == "" {
		return false
	}

	unexpectedResponse := false
	for _, cause := range details.Causes {
		if cause.Type == metav1.CauseTypeUnexpectedServerResponse {
			unexpectedResponse = true
			break
		}
	}
	if !unexpectedResponse {
		return false
	}

	resource := details.Kind
	if details.Group != "" {
		resource += "." + details.Group
	}
	expected := fmt.Sprintf("%s (get %s %s)", genericMissingMessage, resource, details.Name)
	return status.Message == expected
}

func readTargetWidth(reason, hint string) int {
	width := maxRootErrorDisplayWidth - len(rootErrorPrefix) - len(": ") - len(reason)
	if hint != "" {
		width -= len(" (") + len(hint) + len(")")
	}
	return width
}

func formatReadTarget(target ReadTarget, maxWidth int) string {
	operation := "read"
	switch target.Operation {
	case ReadOperationGet, ReadOperationList, ReadOperationResolve:
		operation = string(target.Operation)
	}

	if target.Operation == "" && target.Resource == "" {
		return "read OME API"
	}
	if !safeDNSSubdomain(target.Resource) {
		return operation + " OME resource"
	}
	base := operation + " " + target.Resource
	if len(base) > maxWidth {
		return operation + " OME resource"
	}

	if target.Name != "" {
		if !safeDNSSubdomain(target.Name) {
			return base
		}
		if target.Namespace == "" {
			return withinWidth(base+" "+target.Name, base, maxWidth)
		}
		if !safeDNSLabel(target.Namespace) {
			return base
		}
		return withinWidth(base+" "+target.Namespace+"/"+target.Name, base, maxWidth)
	}
	if target.Namespace != "" {
		if !safeDNSLabel(target.Namespace) {
			return base
		}
		return withinWidth(base+" "+target.Namespace, base, maxWidth)
	}
	return base
}

func withinWidth(value, fallback string, maxWidth int) string {
	if len(value) > maxWidth {
		return fallback
	}
	return value
}

func safeDNSLabel(value string) bool {
	return value != "" && len(validation.IsDNS1123Label(value)) == 0 && safetext.Sanitize(value, 63) == value
}

func safeDNSSubdomain(value string) bool {
	return value != "" && len(validation.IsDNS1123Subdomain(value)) == 0 && safetext.Sanitize(value, 253) == value
}
