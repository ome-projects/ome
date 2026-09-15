package quotatreeprojection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/yaml"
)

// These literal tree lines catch accidental flattening, wrong tenancy and
// suppression of defects independently of the report's rendering helpers.
func TestLiteralScenarioOutputAllFormats(t *testing.T) {
	tests := []struct {
		name, target string
		quotas       []api.AcceleratorQuota
		rows         string
		problems     string
		names        string
	}{
		{"healthy", "", []api.AcceleratorQuota{node("root", "", "Cohort", "2"), node("team", "root", "ClusterQueue", "2")},
			"root [Cohort]\n  budget nvidia.com/gpu/a100=2 (Declared)\n  team [ClusterQueue] tenant=team\n    budget nvidia.com/gpu/a100=2 (Declared)\n", `[]`, `["root","team"]`},
		{"named leaf", "team", []api.AcceleratorQuota{node("root", "", "Cohort", ""), node("team", "root", "ClusterQueue", "2"), node("other", "root", "ClusterQueue", "1")},
			"Selected ancestry and descendants: team\nroot [Cohort]\n  team [ClusterQueue] tenant=team\n    budget nvidia.com/gpu/a100=2 (Declared)\n", `[]`, `["root","team"]`},
		{"missing", "", []api.AcceleratorQuota{node("team", "absent", "ClusterQueue", "2")},
			"team [ClusterQueue] tenant=team (unresolved)\n  budget nvidia.com/gpu/a100=2 (Declared)\n! root: RootMissing (Computed; whole snapshot)\n! team: ParentMissing (Computed; whole snapshot)\n", `[{"node":"root","reason":"RootMissing","evidence":"Computed"},{"node":"team","reason":"ParentMissing","evidence":"Computed"}]`, `["team"]`},
		{"cyclic", "", []api.AcceleratorQuota{node("a", "b", "Cohort", ""), node("b", "a", "Cohort", "")},
			"a [Cohort] (unresolved)\nb [Cohort] (unresolved)\n! a: ParentCycle (Computed; whole snapshot)\n! b: ParentCycle (Computed; whole snapshot)\n! root: RootMissing (Computed; whole snapshot)\n", `[{"node":"a","reason":"ParentCycle","evidence":"Computed"},{"node":"b","reason":"ParentCycle","evidence":"Computed"},{"node":"root","reason":"RootMissing","evidence":"Computed"}]`, `["a","b"]`},
		{"multi-root", "", []api.AcceleratorQuota{node("root", "", "Cohort", ""), node("other", "", "Cohort", "")},
			"root [Cohort]\nother [Cohort] (unresolved)\n! other: ParentMissing (Computed; whole snapshot)\n", `[{"node":"other","reason":"ParentMissing","evidence":"Computed"}]`, `["root","other"]`},
		{"budget-invalid", "", []api.AcceleratorQuota{node("root", "", "Cohort", "-1")},
			"root [Cohort]\n  budget nvidia.com/gpu/a100=-1 (Declared)\n! root: BudgetInvalid (Computed; whole snapshot)\n", `[{"node":"root","reason":"BudgetInvalid","evidence":"Computed"}]`, `["root"]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := input(tt.quotas...)
			in.Target = tt.target
			got, err := Project(in, fixed)
			require.NoError(t, err)
			var table bytes.Buffer
			require.NoError(t, report.Write(&table, report.FormatTable, got))
			prefix := "QUOTA TREE (advisory)\nDeclared budgets; computed ancestry; advisory only.\nNo admission, enforcement, controller, Kueue, or capacity claim.\n"
			lines := strings.Split(table.String(), "\n")
			require.Greater(t, len(lines), 4)
			state := "NoProblemsDetected"
			if tt.problems != "[]" {
				state = "ProblemsDetected"
			}
			assert.Equal(t, prefix+fmt.Sprintf("Snapshot: Complete; %d items / 1 pages; %s\n", len(tt.quotas), state)+tt.rows, table.String())
			for _, line := range lines {
				assert.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
			for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
				var out bytes.Buffer
				require.NoError(t, report.Write(&out, format, got))
				var decoded v.Envelope[v.QuotaTreeContent]
				if format == report.FormatJSON {
					d := json.NewDecoder(&out)
					d.DisallowUnknownFields()
					require.NoError(t, d.Decode(&decoded))
				} else {
					require.NoError(t, yaml.UnmarshalStrict(out.Bytes(), &decoded))
				}
				data, _ := json.Marshal(decoded.Content.Problems)
				assert.JSONEq(t, tt.problems, string(data))
				names := []string{}
				for _, n := range decoded.Content.Nodes {
					names = append(names, n.Name)
				}
				data, _ = json.Marshal(names)
				assert.JSONEq(t, tt.names, string(data))
				assert.Equal(t, v.QuotaTreeSnapshot{Scope: "Cluster", Completeness: "Complete", ObservedPages: 1, ObservedItems: len(tt.quotas), Evidence: v.EvidenceObserved}, decoded.Content.Snapshot)
				assert.Equal(t, got, decoded)
			}
			wide := v.QuotaTreeWideTable(got)
			assert.Equal(t, []string{"FIELD", "VALUE"}, wide.Headers)
			require.GreaterOrEqual(t, len(wide.Rows), 5)
			assert.Equal(t, []string{"report", "QuotaTreeReport"}, wide.Rows[0])
			assert.Equal(t, []string{"collectedAt", "2026-09-15T12:00:00Z"}, wide.Rows[1])
			if tt.name == "healthy" {
				assert.Equal(t, []string{"node", `{"name":"root","role":"Cohort","computedPath":"/root","depth":0,"selected":false,"position":"Resolved","topologyEvidence":"Declared","positionEvidence":"Computed","distribution":"Unavailable/NotConfigured","budgets":[{"resourceName":"nvidia.com/gpu","resourceFlavor":"a100","nominal":"2","policy":"Unavailable/NotConfigured","policySource":"Unavailable/NotConfigured","perCluster":[],"evidence":"Declared"}]}`}, wide.Rows[5])
				assert.Equal(t, []string{"node", `{"name":"team","role":"ClusterQueue","tenant":"team","parentName":"root","computedPath":"/root/team","depth":1,"selected":false,"position":"Resolved","topologyEvidence":"Declared","positionEvidence":"Computed","distribution":"Unavailable/NotConfigured","budgets":[{"resourceName":"nvidia.com/gpu","resourceFlavor":"a100","nominal":"2","policy":"Unavailable/NotConfigured","policySource":"Unavailable/NotConfigured","perCluster":[],"evidence":"Declared"}]}`}, wide.Rows[6])
			}
		})
	}
}
