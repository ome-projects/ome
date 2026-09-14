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

func TestInstanceStatusCanonicalIsDeterministicSanitizedAndImmutable(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC)
	unsafe := "Bearer secret-token\n\x1b[31m" + strings.Repeat("界", 300)
	input := InstanceStatusContent{
		Summary: InstanceStatusSummary{State: InstanceStatusStateReported, Component: RuntimeComponentEngine, Index: 2},
		Instance: &InstanceStatusInstance{
			InferenceReplica: "chat-engine", Index: 2, Incarnation: 7, Phase: InstancePhaseReady,
			RunningRevision: "chat-engine-a", TargetRevision: "chat-engine-b",
			Conditions: []InstanceStatusCondition{
				{Type: "Zeta", Status: "True", Reason: unsafe},
				{Type: "AllPodsReady", Status: "True", Reason: "Ready"},
			},
			Operation:   &InstanceStatusOperation{ID: unsafe, Type: "Update", Step: unsafe, Reason: unsafe, TargetNodeHints: []string{"node-b", "node-a"}},
			LastFailure: &InstanceStatusFailure{PodName: "chat-engine-2-0", ContainerName: "runner", Reason: unsafe},
		},
		Pods: []InstanceStatusPod{
			{Name: "pod-z", Runner: "worker", Revision: "rev-b", Incarnation: 7, Phase: "Running", Ready: true, ServingReady: true},
			{Name: "pod-a", Runner: "leader", Revision: "rev-a", Incarnation: 7, Phase: "Pending"},
		},
		Events: []InstanceStatusEvent{
			{TargetKind: "Pod", TargetName: "pod-z", Reason: unsafe, Count: 2},
			{TargetKind: "InferenceReplica", TargetName: "chat-engine", Reason: "FailedUpdate", Count: 1},
		},
		Issues: []InstanceStatusIssue{{Code: InstanceStatusIssueEventsTruncated}, {Code: InstanceStatusIssuePodsTruncated}},
	}
	report := NewInstanceStatusReport(Metadata{Namespace: "prod", Name: "chat"}, input, ClockFunc(func() time.Time { return now }))
	report.Sources = []SourceReference{
		{Kind: "PodList", Namespace: "prod", Name: "chat", Evidence: EvidenceObserved},
		{Kind: "InferenceService", Namespace: "prod", Name: "chat", Evidence: EvidenceReported},
	}
	report.Warnings = []InstanceStatusWarning{{Code: WarningTruncated}, {Code: WarningPartialData}}

	canonical := report.Canonical()

	require.NotNil(t, canonical.Content.Instance)
	assert.Equal(t, "AllPodsReady", canonical.Content.Instance.Conditions[0].Type)
	assert.Equal(t, "pod-a", canonical.Content.Pods[0].Name)
	assert.Equal(t, "InferenceReplica", canonical.Content.Events[0].TargetKind)
	assert.Equal(t, []string{"node-a", "node-b"}, canonical.Content.Instance.Operation.TargetNodeHints)
	assert.Equal(t, "[REDACTED]", canonical.Content.Instance.Operation.ID)
	assert.Equal(t, "[REDACTED]", canonical.Content.Instance.Operation.Step)
	assert.Equal(t, "[REDACTED]", canonical.Content.Instance.Conditions[1].Reason)
	assert.Equal(t, "[REDACTED]", canonical.Content.Events[1].Reason)
	assert.Equal(t, "[REDACTED]", canonical.Content.Instance.LastFailure.Reason)
	assert.Equal(t, "pod-z", input.Pods[0].Name, "Canonical must not mutate the caller")
	assert.Equal(t, unsafe, input.Instance.Operation.ID)
	report.Content.Instance.Operation.TargetNodeHints[0] = "mutated"
	assert.Equal(t, "node-a", canonical.Content.Instance.Operation.TargetNodeHints[0])
}

func TestInstanceStatusJSONKeepsEmptyCollectionsAsArrays(t *testing.T) {
	t.Parallel()

	report := NewInstanceStatusReport(Metadata{Name: "chat"}, InstanceStatusContent{
		Summary: InstanceStatusSummary{State: InstanceStatusStateNotProjected, Component: RuntimeComponentEngine},
	}, ClockFunc(func() time.Time { return time.Unix(0, 0) }))
	data, err := json.Marshal(report)
	require.NoError(t, err)
	text := string(data)
	for _, want := range []string{`"apiVersion":"cli.ome.io/v1alpha1"`, `"kind":"InstanceStatusReport"`, `"sources":[]`, `"pods":[]`, `"events":[]`, `"issues":[]`, `"warnings":[]`} {
		assert.Contains(t, text, want)
	}
}

func TestInstanceStatusTableIsUsefulAndAtMost80DisplayColumns(t *testing.T) {
	t.Parallel()

	exit := int32(137)
	failureTime := time.Date(2026, 9, 14, 23, 0, 0, 0, time.UTC)
	report := NewInstanceStatusReport(Metadata{Namespace: "prod", Name: "chat"}, InstanceStatusContent{
		Summary: InstanceStatusSummary{State: InstanceStatusStatePartial, Component: RuntimeComponentEngine, Index: 2147483647},
		Instance: &InstanceStatusInstance{
			InferenceReplica: "chat-engine", Index: 2147483647, Incarnation: 9223372036854775807,
			Phase: InstancePhaseUpdating, RunningRevision: strings.Repeat("revision-", 40),
			Operation:   &InstanceStatusOperation{ID: "op-1", Type: "Update", Step: strings.Repeat("界", 100), Reason: "rolling", FromNode: "node-old", TargetNodeHints: []string{"node-new"}},
			LastFailure: &InstanceStatusFailure{PodName: "failed-pod", ContainerName: "runner", Reason: "OOMKilled", ExitCode: &exit, Time: &failureTime},
		},
		Pods:   []InstanceStatusPod{{Name: "chat-engine-2147483647-worker-0", Runner: "worker", Phase: "Running", Node: strings.Repeat("node-", 30)}},
		Events: []InstanceStatusEvent{{TargetKind: "Pod", TargetName: "chat-engine-2147483647-worker-0", Reason: strings.Repeat("Failed", 30)}},
		Issues: []InstanceStatusIssue{{Code: InstanceStatusIssuePodsTruncated}},
	}, ClockFunc(time.Now))

	var out bytes.Buffer
	require.NoError(t, report.Table().Write(&out))
	text := out.String()
	for _, want := range []string{"FIELD", "VALUE", "state", "instance", "pod", "event", "issue"} {
		assert.Contains(t, text, want)
	}
	assert.Contains(t, text, "op-1")
	assert.Contains(t, text, "exit=137")
	for _, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		assert.LessOrEqual(t, printers.CellDisplayWidth(line), 80, "line %q", line)
	}
}
