package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/printers"
)

func TestQuotaStatusCompactNestedEvidenceAndWidth(t *testing.T) {
	c := QuotaStatusContent{Snapshot: QuotaTreeSnapshot{Completeness: "Complete", ObservedItems: 1, ObservedPages: 1}, Objects: []QuotaStatusObject{{Name: strings.Repeat("a", 63), Generation: 7, ObservedGeneration: 7, Freshness: "Current", Budgets: []QuotaStatusBudget{{ResourceName: "gpu", ResourceFlavor: "a100", Usage: QuotaStatusUsage{Nominal: "8", Admitted: "3", Reserved: "5", Borrowed: "Unknown/Defaulted"}, PerCluster: []QuotaStatusClusterUsage{{Cluster: "west", Usage: QuotaStatusUsage{Nominal: "8", Admitted: "3", Reserved: "5", Borrowed: "Unknown/Defaulted"}}}}}, Capacity: []QuotaStatusCapacity{{ResourceName: "gpu", ResourceFlavor: "a100", PerClusterGroup: QuotaStatusGroup{State: "Invalid", Reason: "MalformedPayload"}}}}}}
	var out bytes.Buffer
	require.NoError(t, c.Table().Write(&out))
	assert.Contains(t, out.String(), "cluster west nominal=8 admitted=3 reserved=5")
	assert.Contains(t, out.String(), "clusters=Invalid/MalformedPayload")
	for _, line := range strings.Split(out.String(), "\n") {
		assert.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
	assert.Contains(t, out.String(), "-o wide retains safe fields")
}

func TestQuotaStatusCanonicalDeepCopyAndCompleteTupleTies(t *testing.T) {
	a := QuotaStatusObject{Name: "same", Budgets: []QuotaStatusBudget{{ResourceName: "a|b", ResourceFlavor: "c", PerCluster: []QuotaStatusClusterUsage{{Cluster: "b", Usage: QuotaStatusUsage{Nominal: "2"}}, {Cluster: "a", Usage: QuotaStatusUsage{Nominal: "1"}}}}}, Conditions: []QuotaStatusCondition{{Type: "Unknown", Reason: "future-b"}, {Type: "Unknown", Reason: "future-a"}}, Materialization: &QuotaStatusMaterialization{Reason: "b"}}
	b := a
	b.Budgets = append([]QuotaStatusBudget{}, a.Budgets...)
	b.Budgets[0].PerCluster = append([]QuotaStatusClusterUsage{}, a.Budgets[0].PerCluster...)
	b.Budgets[0].PerCluster[0], b.Budgets[0].PerCluster[1] = b.Budgets[0].PerCluster[1], b.Budgets[0].PerCluster[0]
	b.Conditions = append([]QuotaStatusCondition{}, a.Conditions...)
	b.Conditions[0], b.Conditions[1] = b.Conditions[1], b.Conditions[0]
	c := QuotaStatusContent{Objects: []QuotaStatusObject{a, b}}
	before, _ := json.Marshal(c)
	got := c.Canonical()
	again := QuotaStatusContent{Objects: []QuotaStatusObject{b, a}}.Canonical()
	assert.Equal(t, got, again)
	after, _ := json.Marshal(c)
	assert.Equal(t, string(before), string(after))
	got.Objects[0].Materialization.Reason = "changed"
	assert.Equal(t, "b", a.Materialization.Reason)
	assert.NotNil(t, got.Objects[0].Capacity)
	assert.NotNil(t, got.Objects[0].Projections)
}

func TestQuotaWideIncludesEverySafeTypedField(t *testing.T) {
	c := QuotaStatusContent{Target: "team", Objects: []QuotaStatusObject{{Name: "team", SourceGeneration: 8, SourceGenerationState: "Reported/Unverifiable", Materialization: &QuotaStatusMaterialization{FrozenAt: "2026-09-15T10:00:00Z", LastAppliedTime: "2026-09-15T09:00:00Z"}}}}
	e := NewEnvelope(QuotaStatusReportKind, Metadata{Name: "team"}, c, ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) }))
	e.Sources = []SourceReference{{Name: "team", Evidence: EvidenceReported}}
	e.Warnings = []Warning{{Code: "ReportedOnly", Message: "safe"}}
	var out bytes.Buffer
	require.NoError(t, QuotaStatusWideTable(e).Write(&out))
	assert.Contains(t, out.String(), `"sourceGeneration":8`)
	assert.Contains(t, out.String(), "target")
	assert.Contains(t, out.String(), "cli.ome.io/v1alpha1")
	assert.Contains(t, out.String(), "2026-09-15T09:00:00Z")
	assert.Contains(t, out.String(), "ReportedOnly")
	v := NewEnvelope(QuotaValidationReportKind, Metadata{Name: "root"}, QuotaValidationContent{Valid: true, Topology: QuotaTreeContent{Structure: "NoProblemsDetected"}}, nil)
	out.Reset()
	require.NoError(t, QuotaValidationWideTable(v).Write(&out))
	assert.Contains(t, out.String(), "valid")
	assert.Contains(t, out.String(), "true")
	out.Reset()
	require.NoError(t, v.Table().Write(&out))
	assert.Contains(t, out.String(), "NoProblemsDetected; complete fetched snapshot only.")
}
