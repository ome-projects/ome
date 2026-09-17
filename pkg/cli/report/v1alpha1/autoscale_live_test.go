package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestAutoscaleLiveScaleIsVisibleInBoundedTableAndTypedJSON(t *testing.T) {
	spec, current := int32(3), int32(2)
	r := v1alpha1.NewAutoscaleStatusReport(v1alpha1.Metadata{Namespace: "prod", Name: "chat"}, v1alpha1.AutoscaleStatusContent{
		Summary: v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStateReported},
		Components: []v1alpha1.AutoscaleComponentStatus{{Type: v1alpha1.RuntimeComponentEngine,
			LiveScale: &v1alpha1.AutoscaleLiveScale{Evidence: v1alpha1.AutoscaleLiveScaleReported,
				CountComparison: v1alpha1.AutoscaleLiveScaleDrift, SpecReplicas: &spec, CurrentReplicas: &current}}},
	}, nil)
	var out bytes.Buffer
	require.NoError(t, report.Write(&out, report.FormatTable, r))
	for _, want := range []string{"LIVE-EVIDENCE", "Reported", "LIVE-SPEC", "3", "LIVE-CURRENT", "2", "LIVE-COUNT", "Drift"} {
		require.Contains(t, out.String(), want)
	}
	for _, line := range strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
	data, err := json.Marshal(r.Canonical())
	require.NoError(t, err)
	require.Contains(t, string(data), `"liveScale":{"evidence":"Reported","countComparison":"Drift","specReplicas":3,"currentReplicas":2}`)

	// The optional field does not alter the legacy default contract.
	r.Content.Components[0].LiveScale = nil
	data, err = json.Marshal(r.Canonical())
	require.NoError(t, err)
	require.NotContains(t, string(data), "liveScale")
}

func TestAutoscaleLiveScaleParticipatesInCanonicalOrderingAndCopiesCounts(t *testing.T) {
	one, two := int32(1), int32(2)
	first := v1alpha1.AutoscaleComponentStatus{Type: v1alpha1.RuntimeComponentEngine,
		LiveScale: &v1alpha1.AutoscaleLiveScale{Evidence: v1alpha1.AutoscaleLiveScaleReported,
			CountComparison: v1alpha1.AutoscaleLiveScaleDrift, SpecReplicas: &two}}
	second := v1alpha1.AutoscaleComponentStatus{Type: v1alpha1.RuntimeComponentEngine,
		LiveScale: &v1alpha1.AutoscaleLiveScale{Evidence: v1alpha1.AutoscaleLiveScaleReported,
			CountComparison: v1alpha1.AutoscaleLiveScaleDrift, SpecReplicas: &one}}
	r := v1alpha1.NewAutoscaleStatusReport(v1alpha1.Metadata{}, v1alpha1.AutoscaleStatusContent{
		Components: []v1alpha1.AutoscaleComponentStatus{first, second},
	}, nil)
	ordered := r.Canonical()
	require.Equal(t, int32(1), *ordered.Content.Components[0].LiveScale.SpecReplicas)
	*ordered.Content.Components[0].LiveScale.SpecReplicas = 99
	require.Equal(t, int32(1), *r.Content.Components[0].LiveScale.SpecReplicas)
}
