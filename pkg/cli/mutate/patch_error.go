package mutate

import (
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
)

// GuardedPatchError distinguishes guarded rejection from ordinary admission
// failure without exporting arbitrary API messages or claiming a failed test.
func GuardedPatchError(err error) error {
	return GuardedAnnotationPatchError(err, "rollout status")
}

// GuardedAnnotationPatchError recognizes only the native guarded rejection;
// arbitrary admission messages never establish a stale CAS.
func GuardedAnnotationPatchError(err error, followUp string) error {
	conflict := apierrors.IsConflict(err)
	var status apierrors.APIStatus
	if errors.As(err, &status) && status.Status().Code == 422 {
		s := status.Status()
		d := s.Details
		conflict = s.Status == metav1.StatusFailure && s.Reason == metav1.StatusReasonInvalid && s.Message == "the server rejected our request due to an error in our request" && d != nil && d.Name == "" && d.Group == "" && d.Kind == "" && d.UID == "" && d.RetryAfterSeconds == 0 && len(d.Causes) == 0
	}
	if conflict {
		return &exitcode.PreconditionError{Err: errors.New("guarded annotation patch rejected; refresh " + followUp + " and retry explicitly")}
	}
	return SafeAPIError(err)
}
