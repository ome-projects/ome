package v1alpha1

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/yaml"
)

func TestStatusReportCanonicalPrivacyAndRepresentations(t *testing.T) {
	secret := "ghp_0123456789abcdefghijklmnopqrstuvwxyz"
	content := StatusContent{
		Ready:   StatusReady{Status: "HOSTILE", Validity: "HOSTILE", Reason: secret, Message: "https://private.example/token", Inspection: StatusInspection{State: "HOSTILE", Warnings: []StatusIssueCode{"HOSTILE"}}},
		Runtime: secret, Model: "model-a", Generation: -1, ObservedGeneration: 8, GenerationFreshness: "Current",
		Components: []StatusComponent{{Type: RuntimeComponentRouter, Ready: "False"}, {Type: RuntimeComponentEngine, Ready: "True"}},
		Pods:       StatusCollection{State: "HOSTILE", Reason: "HOSTILE"}, Events: StatusCollection{State: "Reported"},
		RecentEvents: []StatusEvent{{Kind: "Pod", Name: "pod-a", Reason: "FailedMount", Message: secret, Count: 2}},
		Issues:       []StatusIssueCode{"HOSTILE"}, Rollout: StatusRollout{Summary: RolloutSummary{State: "HOSTILE"}, Issues: []RolloutIssueCode{"HOSTILE"}, Warnings: []WarningCode{"HOSTILE"}},
	}
	clock := ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) })
	r := NewStatusReport(Metadata{Name: secret, Namespace: "prod"}, content, clock)
	r.Sources = []SourceReference{{Kind: "HOSTILE", UID: secret, ResourceVersion: secret}}
	r.Warnings = []Warning{{Code: WarningPartialData, Message: secret}}
	r = r.Canonical()
	require.Equal(t, "StatusReport", r.Kind)
	require.Equal(t, "[REDACTED]", r.Metadata.Name)
	require.Equal(t, "NotRecorded", string(r.Content.Ready.Status))
	require.Equal(t, "Invalid", string(r.Content.Ready.Validity))
	require.Equal(t, "Unverifiable", r.Content.GenerationFreshness)
	require.Equal(t, RuntimeComponentEngine, r.Content.Components[0].Type)
	require.Empty(t, r.Sources)
	require.Empty(t, r.Warnings)
	require.Equal(t, r, r.Canonical())
	var jsonOut, yamlOut bytes.Buffer
	require.NoError(t, report.Write(&jsonOut, report.FormatJSON, r))
	require.NoError(t, report.Write(&yamlOut, report.FormatYAML, r))
	var jsonValue, yamlValue any
	decoder := json.NewDecoder(&jsonOut)
	require.NoError(t, decoder.Decode(&jsonValue))
	require.ErrorIs(t, decoder.Decode(&yamlValue), io.EOF)
	require.NoError(t, yaml.Unmarshal(yamlOut.Bytes(), &yamlValue))
	require.Equal(t, jsonValue, yamlValue)
	encoded, err := json.Marshal(r)
	require.NoError(t, err)
	for _, forbidden := range []string{secret, "private.example", "HOSTILE", "resourceVersion", "uid"} {
		require.NotContains(t, string(encoded), forbidden)
	}
	for _, table := range []report.Table{r.Table(), r.WideTable()} {
		var out bytes.Buffer
		require.NoError(t, table.Write(&out))
		require.Contains(t, out.String(), "Unverifiable")
		require.Contains(t, out.String(), "Use -o json or -o yaml")
		for _, line := range strings.Split(out.String(), "\n") {
			require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
		}
	}
	copy := r.Canonical()
	copy.Content.Components[0].Ready = "False"
	copy.Content.RecentEvents[0].Reason = "changed"
	copy.Content.Ready.Inspection.Warnings = append(copy.Content.Ready.Inspection.Warnings, "PodMalformed")
	require.Equal(t, StatusReadyState("True"), r.Content.Components[0].Ready)
	require.Equal(t, "FailedMount", r.Content.RecentEvents[0].Reason)
}

func TestStatusReportWholeBoundsAndUnicodeWidth(t *testing.T) {
	c := StatusContent{Ready: StatusReady{Status: "True", Validity: "Valid", Message: strings.Repeat("測", 500)}, Components: make([]StatusComponent, 4), RecentEvents: make([]StatusEvent, 101)}
	r := NewStatusReport(Metadata{Name: "chat", Namespace: "prod"}, c, nil)
	require.Empty(t, r.Content.Components)
	require.Empty(t, r.Content.RecentEvents)
	require.Contains(t, r.Content.Issues, StatusIssueCode("CollectionLimitExceeded"))
	for _, table := range []report.Table{r.Table(), r.WideTable()} {
		var out bytes.Buffer
		require.NoError(t, table.Write(&out))
		for _, line := range strings.Split(out.String(), "\n") {
			require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
		}
	}
	zero := NewStatusReport(Metadata{Name: "chat", Namespace: "prod"}, StatusContent{}, nil)
	encoded, err := json.Marshal(zero)
	require.NoError(t, err)
	for _, array := range []string{`"components":[]`, `"recentEvents":[]`, `"issues":[]`, `"sources":[]`, `"warnings":[]`} {
		require.Contains(t, string(encoded), array)
	}
}

func TestStatusReportRejectedCollectionsDoNotAllocateCopies(t *testing.T) {
	value := StatusReport{Envelope: Envelope[StatusContent]{Metadata: Metadata{Name: "chat", Namespace: "prod"}, Sources: make([]SourceReference, 50000), Warnings: make([]Warning, 50000), Content: StatusContent{Issues: make([]StatusIssueCode, 50000)}}}
	measurement := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			value.Canonical()
		}
	})
	require.Less(t, measurement.AllocedBytesPerOp(), int64(128*1024), "whole rejected collections must be checked before copying/sorting")
}

func TestStatusCanonicalMalformedRecordsAndWideFacts(t *testing.T) {
	stamp := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	value := NewStatusReport(Metadata{Name: "chat", Namespace: "prod"}, StatusContent{Ready: StatusReady{Status: "NotRecorded", Validity: "Valid", Inspection: StatusInspection{State: "Complete", Total: 1, Inspected: 2}}, Pods: StatusCollection{State: "Reported", Observed: 1001}, Events: StatusCollection{State: "Reported", Truncated: true}, Components: []StatusComponent{{Type: RuntimeComponentEngine, Pods: StatusPodCounts{Total: 1}}, {Type: "hostile"}, {Type: RuntimeComponentRouter}}, RecentEvents: []StatusEvent{{Kind: "Secret", Name: "never-copy"}, {Kind: "Pod", Name: "chat-engine-0", Count: 1, LastSeen: &stamp}}, Rollout: StatusRollout{Issues: make([]RolloutIssueCode, 33), Warnings: make([]WarningCode, 9)}}, ClockFunc(func() time.Time { return stamp }))
	require.Equal(t, StatusValidity("Unavailable"), value.Content.Ready.Validity)
	require.Equal(t, StatusInspectionState("LimitExceeded"), value.Content.Ready.Inspection.State)
	require.Equal(t, 1, value.Content.Ready.Inspection.Inspected)
	require.Equal(t, StatusCollectionState("Partial"), value.Content.Pods.State)
	require.Equal(t, StatusCollectionState("Partial"), value.Content.Events.State)
	require.Contains(t, value.Content.Issues, StatusIssueCode("PodMalformed"))
	require.Contains(t, value.Content.Issues, StatusIssueCode("EventMalformed"))
	require.Contains(t, value.Content.Issues, StatusIssueCode("UnsupportedComponent"))
	require.Contains(t, value.Content.Issues, StatusIssueCode("CollectionLimitExceeded"))
	require.Len(t, value.Content.RecentEvents, 1)
	duplicate := StatusContent{Components: []StatusComponent{{Type: RuntimeComponentDecoder}, {Type: RuntimeComponentDecoder}}}.Canonical()
	require.Empty(t, duplicate.Components)
	require.Contains(t, duplicate.Issues, StatusIssueCode("UnsupportedComponent"))
	value.Content.Components = []StatusComponent{{Type: RuntimeComponentEngine, Ready: "False", Validity: "Valid", Pods: StatusPodCounts{Total: 1000, Ready: 1000, Running: 1000, Restarts: 412316860224000}, Evidence: EvidenceObserved}}
	value.Content.Pods = StatusCollection{State: "Partial", SkippedTargets: 7}
	value.Sources = []SourceReference{{Kind: "InferenceService", Name: "chat", Namespace: "prod", Generation: 4, Evidence: EvidenceObserved}}
	var wide bytes.Buffer
	require.NoError(t, value.WideTable().Write(&wide))
	require.Contains(t, wide.String(), "412316860224000")
	require.Contains(t, wide.String(), "Pod targets skipped")
	require.Contains(t, wide.String(), "2026-09-15T12:00:00Z")
	require.Contains(t, wide.String(), "Source generation")
	require.NotEmpty(t, value.Content.Table().Rows)
}

func TestStatusReportConstructorOwnsSafeMetadata(t *testing.T) {
	secret := "ghp_0123456789abcdefghijklmnopqrstuvwxyz"
	value := NewStatusReport(Metadata{Name: secret, Namespace: secret}, StatusContent{}, nil)
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.NotContains(t, string(data), secret)
	require.Equal(t, "[REDACTED]", value.Metadata.Name)
	require.Equal(t, "[REDACTED]", value.Metadata.Namespace)
}

func TestStatusReportCredentialShapesNeverReachAnyRepresentation(t *testing.T) {
	for _, secret := range []string{"ghp_0123456789abcdefghijklmnopqrstuvwxyz", "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJl", "AKIAIOSFODNN7EXAMPLE", "context_Bearer secret-token", "password=private"} {
		value := NewStatusReport(Metadata{Name: "chat", Namespace: "prod"}, StatusContent{Ready: StatusReady{Status: "False", Validity: "Valid", Reason: "Waiting", Message: secret}, RecentEvents: []StatusEvent{{Kind: "Pod", Name: "chat-engine-0", Reason: "BackOff", Message: secret}}}, nil)
		for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML, "wide"} {
			var out bytes.Buffer
			var err error
			if format == "wide" {
				err = value.WideTable().Write(&out)
			} else {
				err = report.Write(&out, format, value)
			}
			require.NoError(t, err)
			require.NotContains(t, out.String(), secret)
			if format != "table" {
				require.Contains(t, out.String(), "[REDACTED]")
			}
			require.Contains(t, out.String(), "Waiting")
			require.Contains(t, out.String(), "BackOff")
		}
	}
}

func TestStatusCountsRejectOverflowShapedCanonicalInput(t *testing.T) {
	for _, counts := range []StatusPodCounts{{Total: 1, Running: 1, Restarts: math.MaxInt64}, {Total: 1, Running: math.MaxInt, Pending: math.MaxInt, Succeeded: 3}} {
		value := StatusContent{Components: []StatusComponent{{Type: RuntimeComponentEngine, Pods: counts, Evidence: EvidenceObserved}}}.Canonical()
		require.Zero(t, value.Components[0].Pods.Total)
		require.Equal(t, EvidenceUnavailable, value.Components[0].Evidence)
		require.Contains(t, value.Issues, StatusIssueCode("PodMalformed"))
	}
}
