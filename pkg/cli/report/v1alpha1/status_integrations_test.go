package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

func TestStatusIntegratedSummariesCanonicalizeUnsafeValues(t *testing.T) {
	r := NewStatusReport(Metadata{Name: "chat", Namespace: "prod"}, StatusContent{
		Traffic: StatusTraffic{State: "PRIVATE", Algorithm: "PRIVATE", Translator: "PRIVATE",
			PolicyReady: "PRIVATE", PolicyFreshness: "PRIVATE", Evidence: "PRIVATE"},
		RuntimeSummary: StatusRuntimeSummary{State: "PRIVATE", Reason: "PRIVATE", ActiveName: "Bearer PRIVATE",
			ActiveKind: "PRIVATE", ActiveOrigin: "PRIVATE", ActiveState: "PRIVATE",
			PinState: "PRIVATE", Freshness: "PRIVATE", Evidence: "PRIVATE"},
		Accelerator: StatusAccelerator{State: "PRIVATE", Reason: "PRIVATE", Freshness: "PRIVATE", Evidence: "PRIVATE",
			Components: []StatusAcceleratorComponent{{Type: "PRIVATE", DeclaredClass: "Bearer PRIVATE", SelectedClass: "Bearer PRIVATE"}}},
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	var out bytes.Buffer
	require.NoError(t, report.Write(&out, report.FormatJSON, r))
	require.NotContains(t, out.String(), "PRIVATE")
	require.Equal(t, StatusSummaryUnavailable, r.Content.RuntimeSummary.State)
	require.Equal(t, StatusSummaryInvalid, r.Content.Accelerator.State)
	require.Empty(t, r.Content.Accelerator.Components)
}

func TestStatusIntegratedSummariesStayWithinEightyColumns(t *testing.T) {
	r := NewStatusReport(Metadata{Name: "chat", Namespace: "prod"}, StatusContent{
		Traffic: StatusTraffic{State: TrafficStateReported, Algorithm: TrafficAlgorithmRoundRobin,
			Translator: TrafficTranslatorIstio, PolicyReady: TrafficConditionTrue,
			PolicyFreshness: TrafficFreshnessCurrent, Evidence: EvidenceReported},
		RuntimeSummary: StatusRuntimeSummary{State: StatusSummaryReported, ActiveName: "runtime-a",
			ActiveKind: RuntimeKindClusterServingRuntime, ActiveOrigin: ConfigurationOriginControllerRevision,
			ActiveState: ConfigurationStateAvailable, PinState: RuntimePinStateResolved,
			Freshness: StatusFreshnessCurrent, Evidence: EvidenceComputed},
		Accelerator: StatusAccelerator{State: StatusSummaryPartial, Freshness: StatusFreshnessCurrent,
			Evidence: EvidenceComputed, Components: []StatusAcceleratorComponent{{Type: RuntimeComponentEngine,
				Intent: AcceleratorIntentClass, DeclaredClass: "gpu-a", Selection: AcceleratorSelectionReported,
				SelectedClass: "gpu-a", Class: AcceleratorClassForbidden}}},
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	for _, table := range []report.Table{r.Table(), r.WideTable()} {
		var out bytes.Buffer
		require.NoError(t, table.Write(&out))
		for _, line := range strings.Split(out.String(), "\n") {
			require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
		}
		require.Contains(t, out.String(), "Traffic")
		require.Contains(t, out.String(), "Accelerator")
	}
}

func TestStatusIntegratedSummariesRejectContradictoryEvidence(t *testing.T) {
	r := NewStatusReport(Metadata{Name: "chat", Namespace: "prod"}, StatusContent{
		Traffic: StatusTraffic{State: TrafficStateReported, Evidence: EvidenceUnavailable},
		RuntimeSummary: StatusRuntimeSummary{State: StatusSummaryReported,
			ActiveName: "runtime-a", ActiveKind: RuntimeKindClusterServingRuntime,
			ActiveOrigin: ConfigurationOriginLiveRuntime,
			ActiveState:  ConfigurationStateAvailable, Evidence: EvidenceUnavailable},
		Accelerator: StatusAccelerator{State: StatusSummaryReported, Evidence: EvidenceUnavailable,
			Components: []StatusAcceleratorComponent{{Type: RuntimeComponentEngine,
				Intent: AcceleratorIntentClass, DeclaredClass: "gpu-a",
				Selection: AcceleratorSelectionReported, SelectedClass: "gpu-a",
				Class: AcceleratorClassObserved}}},
	}, nil)
	require.Equal(t, TrafficStateInvalid, r.Content.Traffic.State)
	require.Equal(t, StatusSummaryInvalid, r.Content.RuntimeSummary.State)
	require.Equal(t, ConfigurationStateUnavailable, r.Content.RuntimeSummary.ActiveState)
	require.Equal(t, StatusSummaryInvalid, r.Content.Accelerator.State)
	require.Empty(t, r.Content.Accelerator.Components)
}

func TestUnavailableTrafficDoesNotRetainPolicyDetails(t *testing.T) {
	r := NewStatusReport(Metadata{Name: "chat", Namespace: "prod"}, StatusContent{
		Traffic: StatusTraffic{State: TrafficStateUnavailable, Translator: TrafficTranslatorIstio,
			Algorithm: TrafficAlgorithmRoundRobin, PolicyReady: TrafficConditionTrue,
			PolicyFreshness: TrafficFreshnessCurrent, Evidence: EvidenceReported},
	}, nil)
	require.Equal(t, EvidenceUnavailable, r.Content.Traffic.Evidence)
	require.Empty(t, r.Content.Traffic.Translator)
	require.Empty(t, r.Content.Traffic.Algorithm)
	require.Empty(t, r.Content.Traffic.PolicyReady)
	require.Empty(t, r.Content.Traffic.PolicyFreshness)

	data, err := json.Marshal(r)
	require.NoError(t, err)
	for _, field := range []string{`"translator"`, `"algorithm"`, `"policyReady"`, `"policyFreshness"`} {
		require.NotContains(t, string(data), field)
	}
	var out bytes.Buffer
	require.NoError(t, report.Write(&out, report.FormatYAML, r))
	for _, field := range []string{"translator:", "algorithm:", "policyReady:", "policyFreshness:"} {
		require.NotContains(t, out.String(), field)
	}
	out.Reset()
	require.NoError(t, r.WideTable().Write(&out))
	for _, field := range []string{"Traffic policy Ready", "Traffic translator", "Traffic algorithm", "Istio", "RoundRobin"} {
		require.NotContains(t, out.String(), field)
	}
}

func TestNotConfiguredAcceleratorDoesNotRetainReportedDetails(t *testing.T) {
	value := NewStatusReport(Metadata{Name: "chat", Namespace: "prod"}, StatusContent{
		Accelerator: StatusAccelerator{
			State: StatusSummaryNotConfigured, Reason: StatusReasonReadFailed,
			Freshness: StatusFreshnessCurrent, Evidence: EvidenceComputed,
			Components: []StatusAcceleratorComponent{{
				Type: RuntimeComponentEngine, Intent: AcceleratorIntentClass,
				DeclaredClass: "gpu-private", Selection: AcceleratorSelectionReported,
				SelectedClass: "gpu-private", Class: AcceleratorClassObserved,
			}},
		},
	}, nil)
	require.Equal(t, StatusSummaryNotConfigured, value.Content.Accelerator.State)
	require.Empty(t, value.Content.Accelerator.Components)
	require.Empty(t, value.Content.Accelerator.Reason)
	require.Empty(t, value.Content.Accelerator.Freshness)
	require.Equal(t, EvidenceUnavailable, value.Content.Accelerator.Evidence)

	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		require.NoError(t, report.Write(&out, format, value))
		require.NotContains(t, out.String(), "gpu-private")
		require.NotContains(t, out.String(), "ReadFailed")
		require.NotContains(t, out.String(), "Current")
	}
	for _, table := range []report.Table{value.Table(), value.WideTable()} {
		var out bytes.Buffer
		require.NoError(t, table.Write(&out))
		require.Contains(t, out.String(), "Accelerator")
		require.NotContains(t, out.String(), "Accelerator engine")
		require.NotContains(t, out.String(), "gpu-private")
		require.NotContains(t, out.String(), "ReadFailed")
	}
}
