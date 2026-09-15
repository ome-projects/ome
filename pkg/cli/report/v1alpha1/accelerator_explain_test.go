package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
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
					BaseState:      AcceleratorBaseRequestsAvailable,
					Base:           []AcceleratorResourceRequest{{Name: "cpu", Quantity: "2"}},
					EffectiveState: AcceleratorRequestsReported,
					Effective:      []AcceleratorResourceRequest{{Name: "nvidia.com/gpu", Quantity: "1"}},
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
					BaseState: AcceleratorBaseRequestsAvailable,
					Base: []AcceleratorResourceRequest{
						{Name: "memory", Quantity: "8Gi"}, {Name: "cpu", Quantity: "1"},
					},
					EffectiveState: AcceleratorRequestsReported,
					Effective:      []AcceleratorResourceRequest{{Name: "nvidia.com/gpu", Quantity: "1"}},
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

func TestAcceleratorExplainCanonicalComponentOrderIsTotal(t *testing.T) {
	componentA := AcceleratorExplainComponent{
		Type: RuntimeComponentType("unknown-b"),
		Intent: AcceleratorIntent{
			State: AcceleratorIntentClass, DeclaredClass: "class-b",
		},
		Selection: AcceleratorSelectionObservation{
			State:  AcceleratorSelectionNotReported,
			Reason: AcceleratorReason{State: AcceleratorReasonNotReported},
		},
		Class: AcceleratorClassObservation{State: AcceleratorClassNotRequested},
		Requests: AcceleratorRequestObservation{
			BaseState:      AcceleratorBaseRequestsAvailable,
			Base:           []AcceleratorResourceRequest{{Name: "memory", Quantity: "2Gi"}},
			EffectiveState: AcceleratorRequestsNotReported,
		},
		Issues: []AcceleratorExplainIssueCode{AcceleratorIssueSelectionNotReported},
	}
	componentB := componentA
	componentB.Type = RuntimeComponentType("unknown-a")
	componentB.Intent.DeclaredClass = "class-a"
	componentB.Requests.Base = []AcceleratorResourceRequest{{Name: "cpu", Quantity: "2"}}
	componentC := componentA
	componentC.Intent.DeclaredClass = "class-a"
	componentC.Requests.Base = []AcceleratorResourceRequest{{Name: "cpu", Quantity: "1"}}

	forward := AcceleratorExplainContent{Components: []AcceleratorExplainComponent{
		componentA, componentB, componentC,
	}}.Canonical()
	reversed := AcceleratorExplainContent{Components: []AcceleratorExplainComponent{
		componentC, componentB, componentA,
	}}.Canonical()

	assert.Equal(t, forward.Components, reversed.Components)
	require.Len(t, forward.Components, 3)
	assert.Equal(t, RuntimeComponentType("unknown-a"), forward.Components[0].Type)
	assert.Equal(t, "class-a", forward.Components[1].Intent.DeclaredClass)
	assert.Equal(t, "class-b", forward.Components[2].Intent.DeclaredClass)
}

func TestAcceleratorExplainCanonicalFramesCollectionBoundaries(t *testing.T) {
	// Caller inputs are not validated by Canonical. These literal values move
	// the same tokens across the effective-request/issue boundary.
	requestComponent := AcceleratorExplainComponent{
		Type: RuntimeComponentEngine,
		Requests: AcceleratorRequestObservation{
			EffectiveState: AcceleratorRequestsReported,
			Effective:      []AcceleratorResourceRequest{{Name: "ClassForbidden", Quantity: "ClassInvalid"}},
		},
	}
	issueComponent := AcceleratorExplainComponent{
		Type:     RuntimeComponentEngine,
		Requests: AcceleratorRequestObservation{EffectiveState: AcceleratorRequestsReported},
		Issues:   []AcceleratorExplainIssueCode{AcceleratorIssueClassForbidden, AcceleratorIssueClassInvalid},
	}
	forward := AcceleratorExplainContent{Components: []AcceleratorExplainComponent{
		requestComponent, issueComponent,
	}}.Canonical()
	reversed := AcceleratorExplainContent{Components: []AcceleratorExplainComponent{
		issueComponent, requestComponent,
	}}.Canonical()

	assert.Equal(t, forward.Components, reversed.Components)
	require.Len(t, forward.Components, 2)
	assert.Empty(t, forward.Components[0].Requests.Effective)
	assert.Equal(t, []AcceleratorExplainIssueCode{AcceleratorIssueClassForbidden, AcceleratorIssueClassInvalid},
		forward.Components[0].Issues)
	assert.Equal(t, []AcceleratorResourceRequest{{Name: "ClassForbidden", Quantity: "ClassInvalid"}},
		forward.Components[1].Requests.Effective)
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
			Class: AcceleratorClassObservation{State: AcceleratorClassNotRequested},
			Requests: AcceleratorRequestObservation{
				BaseState:      AcceleratorBaseRequestsUnavailable,
				EffectiveState: AcceleratorRequestsUnavailable,
			},
			Issues: []AcceleratorExplainIssueCode{AcceleratorIssueStatusStale},
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
	collectedAt := time.Date(2026, 9, 14, 20, 30, 0, 0, time.UTC)
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
			BaseState:      AcceleratorBaseRequestsAvailable,
			Base:           []AcceleratorResourceRequest{{Name: "cpu", Quantity: "1"}},
			EffectiveState: AcceleratorRequestsReported,
			Effective:      []AcceleratorResourceRequest{{Name: "example.com/gpu", Quantity: "2"}},
		},
		Issues: []AcceleratorExplainIssueCode{AcceleratorIssueClassForbidden},
	}
	content := AcceleratorExplainContent{
		Summary: AcceleratorExplainSummary{
			State: AcceleratorExplainPartial, StatusFreshness: StatusFreshnessStale,
		},
		Components: []AcceleratorExplainComponent{component},
		Issues: []AcceleratorExplainIssue{
			{Code: AcceleratorIssueUnexpectedComponentEvidence, Component: RuntimeComponentRouter},
			{Code: AcceleratorIssueStatusStale},
			{Code: AcceleratorIssueClassForbidden, Component: RuntimeComponentEngine},
		},
	}
	reportValue := NewAcceleratorExplainReport(
		Metadata{Namespace: "prod", Name: "chat"}, content,
		ClockFunc(func() time.Time { return collectedAt }),
	)
	reportValue.Sources = []AcceleratorSourceReference{
		{Kind: "InferenceService", Namespace: "prod", Name: "chat",
			Generation: 4, Evidence: EvidenceObserved},
		{Kind: "AcceleratorClass", Name: "gpu-a", Evidence: EvidenceUnavailable,
			UnavailableReason: UnavailableForbidden},
	}
	reportValue.Warnings = []AcceleratorWarning{
		{Code: AcceleratorWarningStaleEvidence},
		{Code: AcceleratorWarningSourceUnavailable},
		{Code: AcceleratorWarningPartialData},
	}
	reversed := reportValue
	reversed.Sources = []AcceleratorSourceReference{
		reportValue.Sources[1], reportValue.Sources[0],
	}
	reversed.Warnings = []AcceleratorWarning{
		reportValue.Warnings[2], reportValue.Warnings[1], reportValue.Warnings[0],
	}
	reversed.Content.Issues = []AcceleratorExplainIssue{
		content.Issues[2], content.Issues[1], content.Issues[0],
	}

	assert.Equal(t, reportValue.WideTable(), reversed.WideTable())
	assert.Equal(t, [][]string{
		{"SUMMARY", "-", "STATE", "Partial", "Computed"},
		{"SUMMARY", "-", "STATUS_FRESHNESS", "Stale", "Computed"},
		{"INTENT", "engine", "MODE", "Policy", "Declared"},
		{"INTENT", "engine", "POLICY", "FirstAvailable", "Declared/Component"},
		{"SELECTION", "engine", "STATE", "Reported", "Reported"},
		{"SELECTION", "engine", "CLASS", "gpu-a", "Reported"},
		{"SELECTION", "engine", "REASON_STATE", "Reported", "Reported"},
		{"SELECTION", "engine", "REASON_DIGEST", "rs1:0123456789ab", "Reported/Redacted"},
		{"CLASS", "engine", "STATE", "Forbidden", "Unavailable"},
		{"CLASS", "engine", "NAME", "gpu-a", "Unavailable"},
		{"REQUEST", "engine", "BASE_STATE", "Available", "Computed"},
		{"REQUEST", "engine", "BASE", "cpu=1", "Computed"},
		{"REQUEST", "engine", "EFFECTIVE_STATE", "Reported", "Reported"},
		{"REQUEST", "engine", "EFFECTIVE", "example.com/gpu=2", "Reported"},
		{"ISSUE", "engine", "ClassForbidden", "-", "Computed"},
		{"ISSUE", "router", "UnexpectedComponentEvidence", "-", "Computed"},
		{"ISSUE", "-", "StatusStale", "-", "Computed"},
		{"SOURCE", "AcceleratorClass", "NAME", "gpu-a", "Unavailable"},
		{"SOURCE", "AcceleratorClass", "GENERATION", "-", "Unavailable"},
		{"SOURCE", "AcceleratorClass", "COLLECTED_AT", "2026-09-14T20:30:00Z", "Unavailable"},
		{"SOURCE", "AcceleratorClass", "UNAVAILABLE_REASON", "Forbidden", "Unavailable"},
		{"SOURCE", "InferenceService", "NAME", "prod/chat", "Observed"},
		{"SOURCE", "InferenceService", "GENERATION", "4", "Observed"},
		{"SOURCE", "InferenceService", "COLLECTED_AT", "2026-09-14T20:30:00Z", "Observed"},
		{"WARNING", "-", "CODE", "PartialData", "Computed"},
		{"WARNING", "-", "CODE", "SourceUnavailable", "Computed"},
		{"WARNING", "-", "CODE", "StaleEvidence", "Computed"},
	}, reportValue.WideTable().Rows)
}

func TestAcceleratorExplainWideTableAlwaysShowsReasonAndRequestStates(t *testing.T) {
	content := AcceleratorExplainContent{
		Summary: AcceleratorExplainSummary{
			State: AcceleratorExplainPartial, StatusFreshness: StatusFreshnessCurrent,
		},
		Components: []AcceleratorExplainComponent{{
			Type: RuntimeComponentEngine,
			Intent: AcceleratorIntent{
				State: AcceleratorIntentPolicy, Policy: AcceleratorPolicyCheapest,
				PolicySource: AcceleratorSelectorSourceService,
			},
			Selection: AcceleratorSelectionObservation{
				State: AcceleratorSelectionReported, Class: "gpu-a",
				Reason: AcceleratorReason{State: AcceleratorReasonNotReported},
			},
			Class: AcceleratorClassObservation{State: AcceleratorClassObserved, Name: "gpu-a"},
			Requests: AcceleratorRequestObservation{
				BaseState:      AcceleratorBaseRequestsAvailable,
				Base:           []AcceleratorResourceRequest{{Name: "cpu", Quantity: "2"}},
				EffectiveState: AcceleratorRequestsNotReported,
			},
			Issues: []AcceleratorExplainIssueCode{AcceleratorIssueRequestsNotReported},
		}},
		Issues: []AcceleratorExplainIssue{{
			Code: AcceleratorIssueRequestsNotReported, Component: RuntimeComponentEngine,
		}},
	}

	rows := content.WideTable().Rows
	assert.Contains(t, rows, []string{
		"SELECTION", "engine", "REASON_STATE", "NotReported", "Unavailable",
	})
	assert.Contains(t, rows, []string{
		"REQUEST", "engine", "BASE_STATE", "Available", "Computed",
	})
	assert.Contains(t, rows, []string{
		"REQUEST", "engine", "EFFECTIVE_STATE", "NotReported", "Unavailable",
	})
	assert.NotContains(t, rows, []string{
		"REQUEST", "engine", "EFFECTIVE", "None", "Reported",
	})
}

func TestAcceleratorExplainNotReportedRequestsMachineOutputIsExact(t *testing.T) {
	reportValue := NewAcceleratorExplainReport(Metadata{Namespace: "prod", Name: "chat"},
		AcceleratorExplainContent{
			Summary: AcceleratorExplainSummary{
				State: AcceleratorExplainPartial, StatusFreshness: StatusFreshnessCurrent,
			},
			Components: []AcceleratorExplainComponent{{
				Type: RuntimeComponentEngine,
				Intent: AcceleratorIntent{
					State: AcceleratorIntentPolicy, Policy: AcceleratorPolicyCheapest,
					PolicySource: AcceleratorSelectorSourceService,
				},
				Selection: AcceleratorSelectionObservation{
					State: AcceleratorSelectionReported, Class: "gpu-a",
					Reason: AcceleratorReason{State: AcceleratorReasonNotReported},
				},
				Class: AcceleratorClassObservation{State: AcceleratorClassObserved, Name: "gpu-a"},
				Requests: AcceleratorRequestObservation{
					BaseState:      AcceleratorBaseRequestsAvailable,
					Base:           []AcceleratorResourceRequest{{Name: "cpu", Quantity: "2"}},
					EffectiveState: AcceleratorRequestsNotReported,
				},
				Issues: []AcceleratorExplainIssueCode{AcceleratorIssueRequestsNotReported},
			}},
			Issues: []AcceleratorExplainIssue{{
				Code: AcceleratorIssueRequestsNotReported, Component: RuntimeComponentEngine,
			}},
		}, ClockFunc(func() time.Time { return time.Unix(0, 0) }))
	reportValue.Sources = []AcceleratorSourceReference{{
		Kind: "InferenceService", Namespace: "prod", Name: "chat",
		Generation: 4, Evidence: EvidenceObserved,
	}}
	reportValue.Warnings = []AcceleratorWarning{{Code: AcceleratorWarningPartialData}}

	tests := []struct {
		name   string
		format report.Format
		want   string
	}{
		{name: "json", format: report.FormatJSON, want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "AcceleratorExplainReport",
  "metadata": {
    "namespace": "prod",
    "name": "chat"
  },
  "collectedAt": "1970-01-01T00:00:00Z",
  "sources": [
    {
      "kind": "InferenceService",
      "namespace": "prod",
      "name": "chat",
      "generation": 4,
      "evidence": "Observed",
      "collectedAt": "1970-01-01T00:00:00Z"
    }
  ],
  "content": {
    "summary": {
      "state": "Partial",
      "statusFreshness": "Current"
    },
    "components": [
      {
        "type": "engine",
        "intent": {
          "state": "Policy",
          "policy": "Cheapest",
          "policySource": "Service"
        },
        "selection": {
          "state": "Reported",
          "class": "gpu-a",
          "reason": {
            "state": "NotReported"
          }
        },
        "class": {
          "state": "Observed",
          "name": "gpu-a"
        },
        "requests": {
          "baseState": "Available",
          "base": [
            {
              "name": "cpu",
              "quantity": "2"
            }
          ],
          "effectiveState": "NotReported",
          "effective": []
        },
        "issues": [
          "RequestsNotReported"
        ]
      }
    ],
    "issues": [
      {
        "code": "RequestsNotReported",
        "component": "engine"
      }
    ]
  },
  "warnings": [
    {
      "code": "PartialData"
    }
  ]
}
`},
		{name: "yaml", format: report.FormatYAML, want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "1970-01-01T00:00:00Z"
content:
  components:
  - class:
      name: gpu-a
      state: Observed
    intent:
      policy: Cheapest
      policySource: Service
      state: Policy
    issues:
    - RequestsNotReported
    requests:
      base:
      - name: cpu
        quantity: "2"
      baseState: Available
      effective: []
      effectiveState: NotReported
    selection:
      class: gpu-a
      reason:
        state: NotReported
      state: Reported
    type: engine
  issues:
  - code: RequestsNotReported
    component: engine
  summary:
    state: Partial
    statusFreshness: Current
kind: AcceleratorExplainReport
metadata:
  name: chat
  namespace: prod
sources:
- collectedAt: "1970-01-01T00:00:00Z"
  evidence: Observed
  generation: 4
  kind: InferenceService
  name: chat
  namespace: prod
warnings:
- code: PartialData
`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			require.NoError(t, report.Write(&output, test.format, reportValue))
			assert.Equal(t, test.want, output.String())
			assert.NotContains(t, output.String(), "resourceVersion")
			assert.NotContains(t, strings.ToLower(output.String()), "uid")
		})
	}
}

func TestAcceleratorExplainCompactIssueCountIncludesGlobalDiagnostics(t *testing.T) {
	content := AcceleratorExplainContent{
		Components: []AcceleratorExplainComponent{{
			Type:   RuntimeComponentEngine,
			Issues: []AcceleratorExplainIssueCode{AcceleratorIssueClassForbidden},
		}},
		Issues: []AcceleratorExplainIssue{
			{Code: AcceleratorIssueClassForbidden, Component: RuntimeComponentEngine},
			{Code: AcceleratorIssueStatusStale},
			{Code: AcceleratorIssueUnexpectedComponentEvidence, Component: RuntimeComponentRouter},
		},
	}

	require.Len(t, content.Table().Rows, 1)
	assert.Equal(t, "3", content.Table().Rows[0][5])
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
				BaseState: AcceleratorBaseRequestsAvailable,
				Base: []AcceleratorResourceRequest{
					{Name: "an.example.com/extremely-long-accelerator-resource-name", Quantity: "999999999999m"},
					{Name: "memory", Quantity: "256Gi"},
				},
				EffectiveState: AcceleratorRequestsUnavailable,
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

func TestAcceleratorExplainCompactTablePreservesBaseEvidenceFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		baseState AcceleratorBaseRequestState
		want      string
	}{
		{name: "unavailable", baseState: AcceleratorBaseRequestsUnavailable, want: "base:Unknown"},
		{name: "invalid", baseState: AcceleratorBaseRequestsInvalid, want: "base:Invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			content := AcceleratorExplainContent{Components: []AcceleratorExplainComponent{{
				Type: RuntimeComponentEngine,
				Requests: AcceleratorRequestObservation{
					BaseState: test.baseState, EffectiveState: AcceleratorRequestsReported,
					Effective: []AcceleratorResourceRequest{{Name: "example.com/gpu", Quantity: "1"}},
				},
			}}}

			require.Len(t, content.Table().Rows, 1)
			assert.Equal(t, test.want, content.Table().Rows[0][4])
			rows := content.WideTable().Rows
			assert.Contains(t, rows, []string{
				"REQUEST", "engine", "BASE_STATE", string(test.baseState), "Unavailable",
			})
			assert.Contains(t, rows, []string{
				"REQUEST", "engine", "EFFECTIVE_STATE", "Reported", "Reported",
			})
		})
	}
}

func TestAcceleratorRequestObservationMachineSchemaIsLossless(t *testing.T) {
	requests := AcceleratorRequestObservation{
		BaseState:      AcceleratorBaseRequestsUnavailable,
		Base:           []AcceleratorResourceRequest{},
		EffectiveState: AcceleratorRequestsReported,
		Effective:      []AcceleratorResourceRequest{{Name: "example.com/gpu", Quantity: "1"}},
	}

	encodedJSON, err := json.Marshal(requests)
	require.NoError(t, err)
	assert.Equal(t,
		`{"baseState":"Unavailable","base":[],"effectiveState":"Reported",`+
			`"effective":[{"name":"example.com/gpu","quantity":"1"}]}`,
		string(encodedJSON),
	)
	encodedYAML, err := yaml.Marshal(requests)
	require.NoError(t, err)
	assert.Equal(t, `base: []
baseState: Unavailable
effective:
- name: example.com/gpu
  quantity: "1"
effectiveState: Reported
`, string(encodedYAML))
}
