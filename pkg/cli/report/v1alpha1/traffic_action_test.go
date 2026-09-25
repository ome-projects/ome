package v1alpha1_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	v1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestActionResultTrafficDetailsAreTypedCanonicalAndClear(t *testing.T) {
	result := v1alpha1.ActionResult{
		Action:      "traffic drain",
		Target:      v1alpha1.ActionTarget{Kind: "InferenceService", Namespace: "prod", Name: "chat", UID: "uid-1", ResourceVersion: "42"},
		CollectedAt: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC),
		DryRun:      v1alpha1.DryRunNone,
	}
	result.Accepted = true
	result.Applied = true
	result.Message = "API accepted traffic annotation request; not TrafficMap convergence."
	result.FollowUp = "kubectl ome traffic status chat -n prod --context=moirai"
	result.Traffic = &v1alpha1.TrafficActionDetails{
		OverrideID: "maintenance-a", Cluster: "worker-a", OverridesBefore: 1, OverridesAfter: 2,
	}
	canonical := result.Canonical()
	require.NotSame(t, result.Traffic, canonical.Traffic)
	assert.Equal(t, "maintenance-a", canonical.Traffic.OverrideID)
	result.Traffic.OverrideID = "changed"
	assert.Equal(t, "maintenance-a", canonical.Traffic.OverrideID)

	var table bytes.Buffer
	require.NoError(t, canonical.Table().Write(&table))
	for _, expected := range []string{"override-id", "maintenance-a", "cluster", "worker-a", "overrides", "1 -> 2"} {
		assert.Contains(t, table.String(), expected)
	}
	assert.NotContains(t, table.String(), "request-id")
	assert.NotContains(t, table.String(), "resource-version")
	for _, line := range strings.Split(strings.TrimSuffix(table.String(), "\n"), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}

	var wide bytes.Buffer
	require.NoError(t, canonical.WideTable().Write(&wide))
	for _, expected := range []string{"uid", "uid-1", "resource-version", "42"} {
		assert.Contains(t, wide.String(), expected)
	}
	for _, line := range strings.Split(strings.TrimSuffix(wide.String(), "\n"), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}

	var machine bytes.Buffer
	require.NoError(t, report.Write(&machine, report.FormatJSON, canonical))
	assert.JSONEq(t, `{
		"apiVersion":"cli.ome.io/v1alpha1",
		"kind":"ActionResult",
		"collectedAt":"2026-08-31T18:30:00Z",
		"action":"traffic drain",
		"target":{"kind":"InferenceService","namespace":"prod","name":"chat","uid":"uid-1","resourceVersion":"42"},
		"dryRun":"none",
		"accepted":true,
		"applied":true,
		"message":"API accepted traffic annotation request; not TrafficMap convergence.",
		"followUp":"kubectl ome traffic status chat -n prod --context=moirai",
		"traffic":{"overrideID":"maintenance-a","cluster":"worker-a","overridesBefore":1,"overridesAfter":2}
	}`, machine.String())
}

func TestTrafficActionTableBoundsHostileValues(t *testing.T) {
	result := v1alpha1.NewActionResult("traffic drain", v1alpha1.ActionTarget{
		Kind: "InferenceService", Namespace: "prod", Name: strings.Repeat("n", 256),
	}, v1alpha1.DryRunClient, nil)
	result.Traffic = &v1alpha1.TrafficActionDetails{
		OverrideID: strings.Repeat("id", 128) + "\n\x1b[31m", Cluster: strings.Repeat("cluster", 64),
	}
	var output bytes.Buffer
	require.NoError(t, result.Table().Write(&output))
	assert.NotContains(t, output.String(), "\x1b")
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}
}
