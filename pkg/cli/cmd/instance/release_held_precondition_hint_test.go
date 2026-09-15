package instance

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/printers"
)

func TestReleaseHeldGuardedRejectionUsesInstanceFollowUp(t *testing.T) {
	for _, code := range []int{http.StatusConflict, http.StatusUnprocessableEntity} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			h := newHeldWireHarness(t)
			h.patchReply = func(w http.ResponseWriter, _ *http.Request) {
				status := metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(code), Reason: metav1.StatusReasonConflict, Message: "PRIVATE_API_MESSAGE"}
				if code == http.StatusUnprocessableEntity {
					status.Reason = metav1.StatusReasonInvalid
					status.Message = "the server rejected our request due to an error in our request"
					status.Details = &metav1.StatusDetails{}
				}
				w.WriteHeader(code)
				require.NoError(t, json.NewEncoder(w).Encode(status))
			}
			out, preview, err := h.execute(t, "chat", "--component=engine", "--revision=aaaaaaaa", "--yes", "-o=json")
			require.Error(t, err)
			require.Equal(t, exitcode.MutationConflict, exitcode.FromError(err))
			require.Contains(t, err.Error(), "instance retry-blocks")
			require.NotContains(t, err.Error(), "rollout status")
			require.NotContains(t, err.Error()+preview, "PRIVATE")
			for _, line := range strings.Split("error: "+err.Error(), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
			require.Empty(t, out)
			require.Equal(t, 1, h.patches)
		})
	}
}
