package migration

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/printers"
)

func TestStartGuardedRejectionUsesMigrationFollowUp(t *testing.T) {
	for _, failure := range []*apierrors.StatusError{
		apierrors.NewConflict(schema.GroupResource{}, "", errors.New("PRIVATE_API_MESSAGE")),
		apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "PRIVATE_API_MESSAGE", 0, false),
	} {
		t.Run(string(failure.ErrStatus.Reason), func(t *testing.T) {
			fixture := newStartFixture()
			fixture.patchStatus = failure.ErrStatus.DeepCopy()
			err, result, patches, preview := runStartWire(t, fixture, "-o=json")
			require.Error(t, err)
			require.Equal(t, exitcode.MutationConflict, exitcode.FromError(err))
			require.Contains(t, err.Error(), "migration status")
			require.Contains(t, err.Error(), "preview UUID")
			require.NotContains(t, err.Error(), "rollout status")
			require.NotContains(t, err.Error()+preview, "PRIVATE")
			for _, line := range strings.Split("error: "+err.Error(), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
			require.Empty(t, result.Kind)
			require.Equal(t, 1, patches)
		})
	}
}
