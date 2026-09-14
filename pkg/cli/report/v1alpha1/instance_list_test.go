package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestInstanceListReportCanonicalizesAndRendersDeterministically(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.FixedZone("PDT", -7*60*60))
	reportValue := v1alpha1.NewInstanceListReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.InstanceListContent{
			Summary: v1alpha1.InstanceListSummary{
				State:      v1alpha1.InstanceListStatePartial,
				Components: 2,
				Instances:  2,
			},
			Components: []v1alpha1.InstanceListComponent{
				{
					Type:               v1alpha1.RuntimeComponentDecoder,
					State:              v1alpha1.InstanceEvidenceStale,
					InferenceReplica:   "chat-decoder",
					Generation:         4,
					ObservedGeneration: 3,
					IndexSet:           v1alpha1.InstanceIndexSetSparse,
					CurrentRevision:    "chat-decoder-old",
					UpdateRevision:     "chat-decoder-new",
				},
				{
					Type:               v1alpha1.RuntimeComponentEngine,
					State:              v1alpha1.InstanceEvidenceReported,
					InferenceReplica:   "chat-engine",
					Generation:         1,
					ObservedGeneration: 1,
					IndexSet:           v1alpha1.InstanceIndexSetDense,
					CurrentRevision:    "chat-engine-a",
					UpdateRevision:     "chat-engine-a",
				},
			},
			Instances: []v1alpha1.InstanceListInstance{
				{
					Component:        v1alpha1.RuntimeComponentDecoder,
					InferenceReplica: "chat-decoder",
					Index:            3,
					Incarnation:      8,
					Phase:            v1alpha1.InstancePhaseUpdating,
					RunningRevision:  "chat-decoder-old",
					TargetRevision:   "chat-decoder-new",
					Pods: v1alpha1.InstancePodCounts{
						Total: 4, Serving: 1, Available: 1,
					},
					Admitted:           true,
					OperationPresent:   true,
					LastFailurePresent: false,
					Evidence:           v1alpha1.InstanceEvidenceStale,
				},
				{
					Component:        v1alpha1.RuntimeComponentEngine,
					InferenceReplica: "chat-engine",
					Index:            0,
					Incarnation:      1,
					Phase:            v1alpha1.InstancePhaseReady,
					RunningRevision:  "chat-engine-a",
					Pods: v1alpha1.InstancePodCounts{
						Total: 1, Serving: 1, Available: 1,
					},
					Admitted: true,
					Evidence: v1alpha1.InstanceEvidenceReported,
				},
			},
			Issues: []v1alpha1.InstanceListIssue{{
				Code:             v1alpha1.InstanceIssueSparseIndices,
				Component:        v1alpha1.RuntimeComponentDecoder,
				InferenceReplica: "chat-decoder",
			}},
		},
		v1alpha1.ClockFunc(func() time.Time { return now }),
	)
	reportValue.Sources = []v1alpha1.SourceReference{
		{Kind: "InferenceReplica", Namespace: "prod", Name: "chat-decoder", Evidence: v1alpha1.EvidenceReported},
		{Kind: "InferenceService", Namespace: "prod", Name: "chat", Evidence: v1alpha1.EvidenceReported},
	}
	reportValue.Warnings = []v1alpha1.InstanceListWarning{{Code: v1alpha1.WarningStaleEvidence}}

	canonical := reportValue.Canonical()
	require.Equal(t, v1alpha1.APIVersion, canonical.APIVersion)
	require.Equal(t, v1alpha1.InstanceListReportKind, canonical.Kind)
	assert.Equal(t, "2026-09-14T19:00:00Z", canonical.CollectedAt.Format(time.RFC3339))
	assert.Equal(t, v1alpha1.RuntimeComponentEngine, canonical.Content.Components[0].Type)
	assert.Equal(t, v1alpha1.RuntimeComponentEngine, canonical.Content.Instances[0].Component)
	assert.Equal(t, canonical.CollectedAt, canonical.Sources[0].CollectedAt)

	var table bytes.Buffer
	require.NoError(t, report.Write(&table, report.FormatTable, reportValue))
	assert.Equal(t, strings.Join([]string{
		"COMP      IDX/INC   PHASE      PODS    REVS          AOF   EVIDENCE",
		"engine    0/1       Ready      1/1/1   chat...ne-a   A--   OK",
		"decoder   3/8       Updating   1/1/4   chat...-new   AO-   STALE:3/4",
		"",
	}, "\n"), table.String())

	var machine bytes.Buffer
	require.NoError(t, report.Write(&machine, report.FormatJSON, reportValue))
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(machine.Bytes(), &decoded))
	assert.Equal(t, v1alpha1.APIVersion, decoded["apiVersion"])
	assert.Equal(t, v1alpha1.InstanceListReportKind, decoded["kind"])
	assert.NotContains(t, machine.String(), "DenseV1")
	assert.NotContains(t, machine.String(), "ColumnarV2")
}

func TestInstanceListTableKeepsUnavailableComponentsAndGlobalPartialEvidenceVisible(t *testing.T) {
	t.Parallel()

	reportValue := v1alpha1.NewInstanceListReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.InstanceListContent{
			Summary: v1alpha1.InstanceListSummary{
				State: v1alpha1.InstanceListStatePartial, Components: 2, Instances: 1, Truncated: true,
			},
			Components: []v1alpha1.InstanceListComponent{
				{
					Type: v1alpha1.RuntimeComponentEngine, State: v1alpha1.InstanceEvidenceReported,
					InferenceReplica: "chat-engine", IndexSet: v1alpha1.InstanceIndexSetDense,
				},
				{
					Type: v1alpha1.RuntimeComponentDecoder, State: v1alpha1.InstanceEvidenceUnavailable,
					InferenceReplica: "chat-decoder", Generation: 2,
					IndexSet: v1alpha1.InstanceIndexSetNotReported,
				},
			},
			Instances: []v1alpha1.InstanceListInstance{{
				Component: v1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
				Index: 0, Phase: v1alpha1.InstancePhaseReady, Evidence: v1alpha1.InstanceEvidenceReported,
			}},
			Issues: []v1alpha1.InstanceListIssue{
				{
					Code: v1alpha1.InstanceIssueStatusUnobserved, Component: v1alpha1.RuntimeComponentDecoder,
					InferenceReplica: "chat-decoder",
				},
				{Code: v1alpha1.InstanceIssueCollectionTruncated},
			},
		},
		v1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }),
	)

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))

	assert.Equal(t, strings.Join([]string{
		"COMP      IDX/INC   PHASE   PODS    REVS   AOF   EVIDENCE",
		"engine    0/0       Ready   0/0/0   -      ---   OK",
		"decoder   -         -       -       -      ---   NOT-OBSERVED",
		"summary   -         -       -       -      ---   TRUNCATED",
		"",
	}, "\n"), output.String())
}

func TestInstanceListTableNamesGlobalEvidenceWhenNoReplicaIsAccepted(t *testing.T) {
	t.Parallel()

	content := v1alpha1.InstanceListContent{
		Summary: v1alpha1.InstanceListSummary{State: v1alpha1.InstanceListStatePartial},
		Issues:  []v1alpha1.InstanceListIssue{{Code: v1alpha1.InstanceIssueIdentityRejected}},
	}
	var output bytes.Buffer

	require.NoError(t, content.Table().Write(&output))

	assert.Contains(t, output.String(), "REJECTED")
}

func TestInstanceListTablePrioritizesParentProjectionEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		instances []v1alpha1.InstanceListInstance
		issues    []v1alpha1.InstanceListIssue
		want      string
	}{
		{
			name: "stale parent with rows",
			instances: []v1alpha1.InstanceListInstance{{
				Component: v1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
				Index: 0, Phase: v1alpha1.InstancePhaseReady, Evidence: v1alpha1.InstanceEvidenceStale,
			}},
			issues: []v1alpha1.InstanceListIssue{{
				Code:      v1alpha1.InstanceIssueParentGenerationStale,
				Component: v1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
			}},
			want: "STALE:PARENT",
		},
		{
			name: "missing rows outrank stale parent",
			issues: []v1alpha1.InstanceListIssue{
				{
					Code:      v1alpha1.InstanceIssueParentGenerationStale,
					Component: v1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
				},
				{
					Code:      v1alpha1.InstanceIssueStatusesNotReported,
					Component: v1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
				},
			},
			want: "NOT-REPORTED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			content := v1alpha1.InstanceListContent{
				Summary: v1alpha1.InstanceListSummary{State: v1alpha1.InstanceListStatePartial, Components: 1},
				Components: []v1alpha1.InstanceListComponent{{
					Type: v1alpha1.RuntimeComponentEngine, State: v1alpha1.InstanceEvidenceStale,
					InferenceReplica: "chat-engine", Generation: 1, ObservedGeneration: 1,
					IndexSet: v1alpha1.InstanceIndexSetDense,
				}},
				Instances: test.instances,
				Issues:    test.issues,
			}
			var output bytes.Buffer
			require.NoError(t, content.Table().Write(&output))
			assert.Contains(t, output.String(), test.want)
			assert.NotContains(t, output.String(), "STALE:1/1")
		})
	}
}

func TestInstanceListTablePrioritizesInvalidObservedGenerationRegardlessOfIssueOrder(t *testing.T) {
	t.Parallel()

	baseIssues := []v1alpha1.InstanceListIssue{
		{
			Code:      v1alpha1.InstanceIssueParentGenerationStale,
			Component: v1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
		},
		{
			Code:      v1alpha1.InstanceIssueObservedGenerationInvalid,
			Component: v1alpha1.RuntimeComponentEngine, InferenceReplica: "chat-engine",
		},
	}
	render := func(issues []v1alpha1.InstanceListIssue) string {
		content := v1alpha1.InstanceListContent{
			Summary: v1alpha1.InstanceListSummary{State: v1alpha1.InstanceListStatePartial, Components: 1},
			Components: []v1alpha1.InstanceListComponent{{
				Type: v1alpha1.RuntimeComponentEngine, State: v1alpha1.InstanceEvidenceMalformed,
				InferenceReplica: "chat-engine", Generation: 1, ObservedGeneration: 2,
				IndexSet: v1alpha1.InstanceIndexSetMalformed,
			}},
			Issues: issues,
		}
		var output bytes.Buffer
		require.NoError(t, content.Table().Write(&output))
		return output.String()
	}

	forward := render(baseIssues)
	reversed := render([]v1alpha1.InstanceListIssue{baseIssues[1], baseIssues[0]})
	assert.Equal(t, forward, reversed)
	assert.Contains(t, forward, "BAD:GEN")
	assert.NotContains(t, forward, "STALE:PARENT")
}

func TestInstanceListTableStaysWithin80ColumnsForEveryWriter(t *testing.T) {
	t.Parallel()

	reportValue := v1alpha1.InstanceListReport{Content: v1alpha1.InstanceListContent{
		Summary: v1alpha1.InstanceListSummary{State: v1alpha1.InstanceListStatePartial},
		Components: []v1alpha1.InstanceListComponent{{
			Type: v1alpha1.RuntimeComponentDecoder, State: v1alpha1.InstanceEvidenceStale,
			InferenceReplica: "component-with-long-name", Generation: 9_999_999,
			ObservedGeneration: 1, IndexSet: v1alpha1.InstanceIndexSetSparse,
		}},
		Instances: []v1alpha1.InstanceListInstance{{
			Component: v1alpha1.RuntimeComponentDecoder, InferenceReplica: "component-with-long-name",
			Index: 2_147_483_647, Incarnation: 9_223_372_036_854_775_807,
			Phase:           v1alpha1.InstancePhaseRestarting,
			RunningRevision: "component-old-revision", TargetRevision: "component-new-revision",
			Pods: v1alpha1.InstancePodCounts{
				Total:   2_147_483_647,
				Serving: 2_147_483_647, Available: 2_147_483_647,
			},
			Admitted: true, OperationPresent: true, LastFailurePresent: true,
			Evidence: v1alpha1.InstanceEvidenceStale,
		}},
	}}
	writers := []interface {
		Write([]byte) (int, error)
		String() string
	}{
		&bytes.Buffer{},
		&instanceTerminalBuffer{width: 80},
		&instanceTerminalBuffer{width: 120},
	}

	for _, writer := range writers {
		require.NoError(t, report.Write(writer, report.FormatTable, reportValue))
		lines := strings.Split(strings.TrimSuffix(writer.String(), "\n"), "\n")
		require.Len(t, lines, 2, "%q", writer.String())
		for _, line := range lines {
			assert.LessOrEqual(t, len(line), 80, "%q", line)
		}
	}
}

type instanceTerminalBuffer struct {
	bytes.Buffer
	width int
}

func (w *instanceTerminalBuffer) TerminalWidth() (int, bool) { return w.width, true }
