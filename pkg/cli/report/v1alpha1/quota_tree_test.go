package v1alpha1

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// Identical primary identities with different trailing values must be ordered
// totally, including unknown values passed directly to the exported canonical API.
func TestQuotaCanonicalTotalOrderingDeepCopyAndBoundaries(t *testing.T) {
	b := QuotaTreeBudget{ResourceName: "gpu", ResourceFlavor: "flavor", Nominal: "1", Policy: "future", PolicySource: "future", PerCluster: []QuotaTreeClusterShare{{Cluster: "z", Nominal: "2"}, {Cluster: "a", Nominal: "1"}}}
	c := QuotaTreeContent{Snapshot: QuotaTreeSnapshot{Completeness: "Unknown"}, Structure: "Unknown", Nodes: []QuotaTreeNode{
		{Name: "same", ComputedPath: "/same", Depth: 0, Budgets: []QuotaTreeBudget{b}, PositionEvidence: "FutureB"},
		{Name: "same", ComputedPath: "/same", Depth: 0, Budgets: []QuotaTreeBudget{b}, PositionEvidence: "FutureA"},
	}, Problems: []QuotaTreeProblem{{Node: "same", Reason: "future", Evidence: "B"}, {Node: "same", Reason: "future", Evidence: "A"}}}
	before, _ := json.Marshal(c)
	canonical := c.Canonical()
	after, _ := json.Marshal(c)
	assert.Equal(t, string(before), string(after))
	assert.Equal(t, EvidenceLevel("FutureA"), canonical.Nodes[0].PositionEvidence)
	assert.Equal(t, "a", canonical.Nodes[0].Budgets[0].PerCluster[0].Cluster)
	canonical.Nodes[0].Budgets[0].PerCluster[0].Nominal = "mutated"
	assert.Equal(t, "2", c.Nodes[0].Budgets[0].PerCluster[0].Nominal)
	expected, _ := json.Marshal(c.Canonical())
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 50; i++ {
		rng.Shuffle(len(c.Nodes), func(i, j int) { c.Nodes[i], c.Nodes[j] = c.Nodes[j], c.Nodes[i] })
		rng.Shuffle(len(c.Problems), func(i, j int) { c.Problems[i], c.Problems[j] = c.Problems[j], c.Problems[i] })
		got, _ := json.Marshal(c.Canonical())
		assert.Equal(t, string(expected), string(got))
	}
	for _, length := range []int{0, 1, 70, 71, 79, 80, 81, 200, 2000} {
		for _, value := range []string{strings.Repeat("a", length), strings.Repeat("界", length), strings.Repeat("e\u0301", length), strings.Repeat("\x1b\n", length)} {
			clipped := quotaClip(value)
			assert.LessOrEqual(t, printers.CellDisplayWidth(clipped), 80)
			if strings.HasPrefix(value, "a") && length <= 80 {
				assert.Equal(t, value, clipped)
			}
		}
	}
	for _, depth := range []int{-1, 0, 8, 9, 9999} {
		c.Nodes = []QuotaTreeNode{{Name: strings.Repeat("n", 200), Depth: depth, Role: "Future", Tenant: "tenant", Position: "Unresolved", Budgets: []QuotaTreeBudget{b}}}
		var out bytes.Buffer
		require.NoError(t, c.Table().Write(&out))
		for _, line := range strings.Split(out.String(), "\n") {
			assert.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
		}
	}
}

func TestQuotaReportWriterFailures(t *testing.T) {
	value := NewEnvelope(QuotaTreeReportKind, Metadata{Name: "root"}, QuotaTreeContent{}, ClockFunc(func() time.Time { return time.Unix(0, 0) }))
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		require.Error(t, report.Write(quotaShortWriter{}, format, value))
	}
	require.Error(t, QuotaTreeWideTable(value).Write(quotaShortWriter{}))
}

// The indentation cap must not make successive deep descendants look like
// siblings. Actual depth stays visible before even a clipped node identity.
func TestQuotaCompactLabelsCompressedDepth(t *testing.T) {
	content := QuotaTreeContent{
		Snapshot:  QuotaTreeSnapshot{Completeness: "Complete", ObservedItems: 3, ObservedPages: 1},
		Structure: "NoProblemsDetected",
		Nodes: []QuotaTreeNode{
			{Name: "node-08", Role: "Cohort", Depth: 8},
			{Name: "node-09", Role: "Cohort", Depth: 9},
			{Name: "node-10", Role: "Cohort", Depth: 10},
		},
	}
	var output bytes.Buffer
	require.NoError(t, content.Table().Write(&output))
	assert.Equal(t, "QUOTA TREE (advisory)\n"+
		"Declared budgets; computed ancestry; advisory only.\n"+
		"No admission, enforcement, controller, Kueue, or capacity claim.\n"+
		"Snapshot: Complete; 3 items / 1 pages; NoProblemsDetected\n"+
		"                node-08 [Cohort]\n"+
		"                depth=9 node-09 [Cohort]\n"+
		"                depth=10 node-10 [Cohort]\n", output.String())
	for _, line := range strings.Split(output.String(), "\n") {
		assert.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
	content.Nodes = []QuotaTreeNode{{Name: strings.Repeat("n", 63), Role: "Cohort", Depth: 999}}
	output.Reset()
	require.NoError(t, content.Table().Write(&output))
	assert.Contains(t, output.String(), "                depth=999 ")
	for _, line := range strings.Split(output.String(), "\n") {
		assert.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
}

type quotaShortWriter struct{}

func (quotaShortWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }
