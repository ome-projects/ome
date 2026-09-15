package mutate

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
)

func TestSharedGuardedPatchClassifierExact(t *testing.T) {
	canonical := apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "private-hidden", 0, false)
	require.Equal(t, 3, exitcode.FromError(GuardedPatchError(canonical)))
	require.Equal(t, 3, exitcode.FromError(GuardedPatchError(apierrors.NewConflict(schema.GroupResource{}, "", errors.New("private-hidden")))))
	for _, edit := range []func(*metav1.Status){func(s *metav1.Status) { s.Details = nil }, func(s *metav1.Status) { s.Status = "Success" }, func(s *metav1.Status) { s.Reason = metav1.StatusReasonForbidden }, func(s *metav1.Status) { s.Message = "testing value /metadata/uid failed private-hidden" }, func(s *metav1.Status) { s.Details.Name = "chat" }, func(s *metav1.Status) { s.Details.Group = "ome.io" }, func(s *metav1.Status) { s.Details.Kind = "InferenceService" }, func(s *metav1.Status) { s.Details.UID = "uid" }, func(s *metav1.Status) { s.Details.RetryAfterSeconds = 1 }, func(s *metav1.Status) { s.Details.Causes = []metav1.StatusCause{{Message: "private-hidden"}} }} {
		s := canonical.ErrStatus.DeepCopy()
		edit(s)
		err := GuardedPatchError(&apierrors.StatusError{ErrStatus: *s})
		require.Equal(t, 1, exitcode.FromError(err))
		require.NotContains(t, err.Error(), "private-hidden")
	}
}
