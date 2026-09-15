package trafficprojection_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/trafficprojection"
)

// Each fixture is a small API snapshot paired with independently specified,
// checked-in expected output. Tests never regenerate the expected documents.
// The matrix catches derived-state, provenance, omission and serialization bugs
// at the boundary from raw controller evidence to all four public formats.
func TestProjectExplainExactOutputMatrix(t *testing.T) {
	for _, scenario := range []string{
		"healthy", "partial", "unavailable", "unsupported", "mismatch",
		"stale", "stale-unsupported", "malformed", "absent-translator",
		"noop-translator", "hostile-secret", "canary100", "canary-only", "completed-canary",
	} {
		t.Run(scenario, func(t *testing.T) {
			base := filepath.Join("testdata", "explain", scenario)
			input, err := os.ReadFile(base + ".input.json")
			require.NoError(t, err)
			var isvc omev1beta1.InferenceService
			require.NoError(t, json.Unmarshal(input, &isvc))
			got, err := trafficprojection.ProjectExplain(&isvc, reportv1alpha1.ClockFunc(func() time.Time {
				return explainProjectionNow
			}))
			require.NoError(t, err)
			for _, format := range []string{"compact", "wide", "json", "yaml"} {
				t.Run(format, func(t *testing.T) {
					var output bytes.Buffer
					switch format {
					case "compact":
						require.NoError(t, got.Table().Write(&output))
					case "wide":
						require.NoError(t, got.WideTable().Write(&output))
					default:
						require.NoError(t, report.Write(&output, report.Format(format), got))
					}
					want, err := os.ReadFile(base + "." + format + ".golden")
					require.NoError(t, err)
					assert.Equal(t, string(want), output.String())
					if format == "compact" {
						for _, line := range strings.Split(output.String(), "\n") {
							assert.LessOrEqual(t, len([]rune(line)), 80)
						}
					}
					for _, forbidden := range []string{"SECRET", "secret-user", "resourceVersion", "annotations", "message"} {
						assert.NotContains(t, output.String(), forbidden)
					}
				})
			}
		})
	}
}
