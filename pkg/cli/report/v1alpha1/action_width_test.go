package v1alpha1_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

func TestActionResultCompactFitsTerminalAndShowsExpansionHint(t *testing.T) {
	result := actionResult()
	result.Target.Name = strings.Repeat("n", 253)
	result.FollowUp = "kubectl ome rollout status " + result.Target.Name + " -n prod"
	result.Message = "accepted; convergence not observed"
	result.RequestID = strings.Repeat("id", 64)
	var out bytes.Buffer
	require.NoError(t, report.Write(&out, report.FormatTable, result))
	for _, line := range strings.Split(out.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
	require.Contains(t, out.String(), "-o json")
	require.Contains(t, out.String(), "...")
}
