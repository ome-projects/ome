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

func TestAcceleratorExplainCanonicalAndCompactTable(t *testing.T) {
	collectedAt := time.Date(2026, 9, 14, 20, 30, 0, 0, time.UTC)
	content := AcceleratorExplainContent{
		Summary: AcceleratorExplainSummary{State: AcceleratorExplainReported},
		Components: []AcceleratorExplainComponent{
			{
				Type: RuntimeComponentDecoder,
				Intent: AcceleratorIntent{
					State: AcceleratorIntentPolicy, Policy: AcceleratorPolicyMostCapable,
					PolicySource: AcceleratorSelectorSourceService,
				},
				Selection: AcceleratorSelectionObservation{
					State: AcceleratorSelectionReported, Class: "nvidia-h100-80gb",
					Reason: AcceleratorReason{State: AcceleratorReasonReported, Digest: "rs1:bbbbbbbbbbbb"},
				},
				Class: AcceleratorClassObservation{
					State: AcceleratorClassObserved, Name: "nvidia-h100-80gb",
				},
				Requests: AcceleratorRequestObservation{
					State:     AcceleratorRequestsReported,
					Base:      []AcceleratorResourceRequest{{Name: "cpu", Quantity: "2"}},
					Effective: []AcceleratorResourceRequest{{Name: "nvidia.com/gpu", Quantity: "1"}},
				},
				Issues: []AcceleratorExplainIssueCode{},
			},
			{
				Type: RuntimeComponentEngine,
				Intent: AcceleratorIntent{
					State: AcceleratorIntentClass, DeclaredClass: "nvidia-a100-80gb",
					ClassSource: AcceleratorSelectorSourceComponent,
				},
				Selection: AcceleratorSelectionObservation{
					State: AcceleratorSelectionReported, Class: "nvidia-a100-80gb",
					Reason: AcceleratorReason{State: AcceleratorReasonNotReported},
				},
				Class: AcceleratorClassObservation{
					State: AcceleratorClassObserved, Name: "nvidia-a100-80gb",
				},
				Requests: AcceleratorRequestObservation{
					State: AcceleratorRequestsReported,
					Base: []AcceleratorResourceRequest{
						{Name: "memory", Quantity: "8Gi"}, {Name: "cpu", Quantity: "1"},
					},
					Effective: []AcceleratorResourceRequest{{Name: "nvidia.com/gpu", Quantity: "1"}},
				},
				Issues: []AcceleratorExplainIssueCode{},
			},
		},
		Issues: []AcceleratorExplainIssue{},
	}
	reportValue := NewAcceleratorExplainReport(
		Metadata{Namespace: "prod", Name: "chat"}, content,
		ClockFunc(func() time.Time { return collectedAt }),
	)
	reportValue.Sources = []AcceleratorSourceReference{
		{Kind: "InferenceService", Namespace: "prod", Name: "chat", Evidence: EvidenceObserved},
		{Kind: "AcceleratorClass", Name: "nvidia-h100-80gb", Evidence: EvidenceObserved},
	}

	canonical := reportValue.Canonical()
	require.Equal(t, []RuntimeComponentType{RuntimeComponentEngine, RuntimeComponentDecoder}, []RuntimeComponentType{
		canonical.Content.Components[0].Type, canonical.Content.Components[1].Type,
	})
	assert.Equal(t, []AcceleratorResourceRequest{{Name: "cpu", Quantity: "1"}, {Name: "memory", Quantity: "8Gi"}},
		canonical.Content.Components[0].Requests.Base)
	assert.NotSame(t, &reportValue.Content.Components[0], &canonical.Content.Components[0])

	var output bytes.Buffer
	require.NoError(t, canonical.Table().Write(&output))
	assert.Equal(t, `COMP      POLICY        CLASS              SEL        REQUESTS           ISS
engine    Explicit      nvidia-a100-80gb   Reported   nvidia.com/gpu=1   0
decoder   MostCapable   nvidia-h100-80gb   Reported   nvidia.com/gpu=1   0
`, output.String())
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.Equal(t, line, printers.BoundedCell(line, 80), "line exceeds 80 terminal columns")
	}
}

func TestAcceleratorExplainMachineSchemaNeverCarriesRawReason(t *testing.T) {
	const secret = "ghp_secret-controller-reason"
	reportValue := NewAcceleratorExplainReport(Metadata{Name: "chat"}, AcceleratorExplainContent{
		Summary: AcceleratorExplainSummary{State: AcceleratorExplainPartial},
		Components: []AcceleratorExplainComponent{{
			Type: RuntimeComponentEngine,
			Intent: AcceleratorIntent{State: AcceleratorIntentPolicy, Policy: AcceleratorPolicyCheapest,
				PolicySource: AcceleratorSelectorSourceService},
			Selection: AcceleratorSelectionObservation{State: AcceleratorSelectionUnavailable,
				Reason: AcceleratorReason{State: AcceleratorReasonUnavailable}},
			Class:    AcceleratorClassObservation{State: AcceleratorClassNotRequested},
			Requests: AcceleratorRequestObservation{State: AcceleratorRequestsUnavailable},
			Issues:   []AcceleratorExplainIssueCode{AcceleratorIssueStatusStale},
		}},
		Issues: []AcceleratorExplainIssue{{Code: AcceleratorIssueStatusStale, Component: RuntimeComponentEngine}},
	}, ClockFunc(func() time.Time { return time.Unix(0, 0) }))

	data, err := json.Marshal(reportValue)
	require.NoError(t, err)
	assert.NotContains(t, string(data), secret)
	assert.NotContains(t, string(data), "message")
	assert.Contains(t, string(data), `"state":"Unavailable"`)
}

func TestAcceleratorExplainWideTableIsCompleteAndDeterministic(t *testing.T) {
	component := AcceleratorExplainComponent{
		Type: RuntimeComponentEngine,
		Intent: AcceleratorIntent{
			State: AcceleratorIntentPolicy, Policy: AcceleratorPolicyFirstAvailable,
			PolicySource: AcceleratorSelectorSourceComponent,
		},
		Selection: AcceleratorSelectionObservation{
			State: AcceleratorSelectionReported, Class: "gpu-a",
			Reason: AcceleratorReason{State: AcceleratorReasonReported, Digest: "rs1:0123456789ab"},
		},
		Class: AcceleratorClassObservation{State: AcceleratorClassForbidden, Name: "gpu-a"},
		Requests: AcceleratorRequestObservation{
			State:     AcceleratorRequestsReported,
			Base:      []AcceleratorResourceRequest{{Name: "cpu", Quantity: "1"}},
			Effective: []AcceleratorResourceRequest{{Name: "example.com/gpu", Quantity: "2"}},
		},
		Issues: []AcceleratorExplainIssueCode{AcceleratorIssueClassForbidden},
	}
	content := AcceleratorExplainContent{
		Summary:    AcceleratorExplainSummary{State: AcceleratorExplainPartial},
		Components: []AcceleratorExplainComponent{component},
		Issues:     []AcceleratorExplainIssue{{Code: AcceleratorIssueClassForbidden, Component: RuntimeComponentEngine}},
	}
	reversed := content
	reversed.Components = []AcceleratorExplainComponent{component}
	reversed.Components[0].Requests.Base = []AcceleratorResourceRequest{{Name: "cpu", Quantity: "1"}}

	assert.Equal(t, content.WideTable(), reversed.WideTable())
	assert.Equal(t, [][]string{
		{"SUMMARY", "-", "STATE", "Partial", "Computed"},
		{"INTENT", "engine", "MODE", "Policy", "Declared"},
		{"INTENT", "engine", "POLICY", "FirstAvailable", "Declared/Component"},
		{"SELECTION", "engine", "STATE", "Reported", "Reported"},
		{"SELECTION", "engine", "CLASS", "gpu-a", "Reported"},
		{"SELECTION", "engine", "REASON", "rs1:0123456789ab", "Reported/Redacted"},
		{"CLASS", "engine", "STATE", "Forbidden", "Unavailable"},
		{"REQUEST", "engine", "BASE", "cpu=1", "Computed"},
		{"REQUEST", "engine", "EFFECTIVE", "example.com/gpu=2", "Reported"},
		{"ISSUE", "engine", "ClassForbidden", "-", "Computed"},
	}, content.WideTable().Rows)
}

func TestAcceleratorExplainCompactTableNeverExceedsEightyColumns(t *testing.T) {
	reportValue := NewAcceleratorExplainReport(Metadata{Name: "chat"}, AcceleratorExplainContent{
		Summary: AcceleratorExplainSummary{State: AcceleratorExplainPartial},
		Components: []AcceleratorExplainComponent{{
			Type: RuntimeComponentDecoder,
			Intent: AcceleratorIntent{
				State: AcceleratorIntentPolicy, Policy: AcceleratorPolicyMostCapable,
				PolicySource: AcceleratorSelectorSourceComponent,
			},
			Selection: AcceleratorSelectionObservation{
				State:  AcceleratorSelectionReported,
				Class:  "a-very-long-but-valid-accelerator-class-name-that-must-be-clipped",
				Reason: AcceleratorReason{State: AcceleratorReasonNotReported},
			},
			Class: AcceleratorClassObservation{State: AcceleratorClassObserved},
			Requests: AcceleratorRequestObservation{
				State: AcceleratorRequestsUnavailable,
				Base: []AcceleratorResourceRequest{
					{Name: "an.example.com/extremely-long-accelerator-resource-name", Quantity: "999999999999m"},
					{Name: "memory", Quantity: "256Gi"},
				},
			},
			Issues: []AcceleratorExplainIssueCode{
				AcceleratorIssueClassUnreadable, AcceleratorIssueReportedClassMismatch,
			},
		}},
	}, ClockFunc(func() time.Time { return time.Unix(0, 0) }))

	var output bytes.Buffer
	require.NoError(t, reportValue.Table().Write(&output))
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.Equal(t, line, printers.BoundedCell(line, 80), "line exceeds 80 terminal columns")
	}
}
