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
			{Name: "pod-z", Runner: "worker", Revision: "rev-b", Incarnation: 7, Phase: "Running", Ready: "True", ServingReady: "True"},
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

func TestInstanceStatusCanonicalRedactsCredentialShapesWithoutKeywordFalsePositives(t *testing.T) {
	t.Parallel()

	shapes := []string{
		"ghp_0123456789abcdefghijklmnopqrstuvwxyz",
		"sk-proj-0123456789abcdefghijklmnopqrstuvwxyz",
		"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJl",
		"AKIAIOSFODNN7EXAMPLE",
	}
	for _, shape := range shapes {
		report := NewInstanceStatusReport(Metadata{Name: "chat"}, InstanceStatusContent{
			Summary: InstanceStatusSummary{State: InstanceStatusStateReported, Component: RuntimeComponentEngine},
			Instance: &InstanceStatusInstance{Conditions: []InstanceStatusCondition{{
				Type: "Ready", Status: "False", Reason: "failure-" + shape,
			}}},
			Events: []InstanceStatusEvent{{TargetKind: "Pod", TargetName: "pod", Reason: "reason-" + shape}},
		}, ClockFunc(func() time.Time { return time.Unix(0, 0) })).Canonical()
		require.NotNil(t, report.Content.Instance)
		assert.Equal(t, "[REDACTED]", report.Content.Instance.Conditions[0].Reason)
		assert.Equal(t, "[REDACTED]", report.Content.Events[0].Reason)
	}

	report := NewInstanceStatusReport(Metadata{Name: "chat"}, InstanceStatusContent{
		Summary: InstanceStatusSummary{State: InstanceStatusStateReported, Component: RuntimeComponentEngine},
		Instance: &InstanceStatusInstance{Conditions: []InstanceStatusCondition{
			{Type: "Ready", Status: "False", Reason: "TokenExpired"},
			{Type: "Healthy", Status: "False", Reason: "SecretNotFound"},
		}},
	}, ClockFunc(func() time.Time { return time.Unix(0, 0) })).Canonical()
	require.NotNil(t, report.Content.Instance)
	assert.Equal(t, "SecretNotFound", report.Content.Instance.Conditions[0].Reason)
	assert.Equal(t, "TokenExpired", report.Content.Instance.Conditions[1].Reason)
}

func TestInstanceStatusEveryOutputRedactsEmbeddedCredentialShapes(t *testing.T) {
	t.Parallel()
	secret := "failure_ghp_0123456789abcdefghijklmnopqrstuvwxyz"
	password := "context_password=hunter2"
	report := NewInstanceStatusReport(Metadata{Name: "chat"}, InstanceStatusContent{
		Summary:  InstanceStatusSummary{State: InstanceStatusStateReported, Component: RuntimeComponentEngine},
		Instance: &InstanceStatusInstance{Conditions: []InstanceStatusCondition{{Type: "Ready", Status: "False", Reason: secret}}, Migrations: []InstanceStatusMigration{{RequestUUID: "request", Role: "Source", Reason: password, TargetNodeHints: []string{}}}},
	}, ClockFunc(func() time.Time { return time.Unix(0, 0) })).Canonical()
	var table, wide bytes.Buffer
	require.NoError(t, report.Table().Write(&table))
	require.NoError(t, report.WideTable().Write(&wide))
	jsonData, err := json.Marshal(report)
	require.NoError(t, err)
	yamlData, err := yaml.Marshal(report)
	require.NoError(t, err)
	for _, output := range []string{table.String(), wide.String(), string(jsonData), string(yamlData)} {
		assert.NotContains(t, output, "ghp_")
		assert.NotContains(t, output, "hunter2")
		assert.Contains(t, output, "[REDACTED]")
	}
}

func TestInstanceStatusCanonicalUsesTotalOrderingForHostileDuplicates(t *testing.T) {
	t.Parallel()
	first := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	second := first.Add(time.Minute)
	content := InstanceStatusContent{
		Summary: InstanceStatusSummary{State: InstanceStatusStateReported, Component: RuntimeComponentEngine},
		Instance: &InstanceStatusInstance{
			Conditions: []InstanceStatusCondition{
				{Type: "Ready", Status: "False", Reason: "Same", LastTransitionTime: &second},
				{Type: "Ready", Status: "False", Reason: "Same", LastTransitionTime: &first},
			},
			Migrations: []InstanceStatusMigration{
				{RequestUUID: "same", Role: "Source", Phase: "Accepted", Reason: "z", TargetNodeHints: []string{}},
				{RequestUUID: "same", Role: "Source", Phase: "Accepted", Reason: "a", TargetNodeHints: []string{}},
			},
		},
		Pods: []InstanceStatusPod{
			{Name: "duplicate", Runner: "worker", Revision: "z"},
			{Name: "duplicate", Runner: "worker", Revision: "a"},
		},
		Events: []InstanceStatusEvent{
			{TargetKind: "Pod", TargetName: "duplicate", Reason: "Same", Count: 1, LastSeen: &second},
			{TargetKind: "Pod", TargetName: "duplicate", Reason: "Same", Count: 1, LastSeen: &first},
		},
	}
	reversed := content
	instanceCopy := *content.Instance
	reversed.Instance = &instanceCopy
	reversed.Instance.Migrations = append([]InstanceStatusMigration{}, content.Instance.Migrations...)
	slicesReverseStatus(reversed.Instance.Migrations)
	reversed.Instance.Conditions = append([]InstanceStatusCondition{}, content.Instance.Conditions...)
	reversed.Pods = append([]InstanceStatusPod{}, content.Pods...)
	reversed.Events = append([]InstanceStatusEvent{}, content.Events...)
	slicesReverseStatus(reversed.Instance.Conditions)
	slicesReverseStatus(reversed.Pods)
	slicesReverseStatus(reversed.Events)
	left := NewInstanceStatusReport(Metadata{Name: "chat"}, content, ClockFunc(func() time.Time { return first })).Canonical()
	right := NewInstanceStatusReport(Metadata{Name: "chat"}, reversed, ClockFunc(func() time.Time { return first })).Canonical()
	assert.Equal(t, left, right)
}

func slicesReverseStatus[T any](values []T) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
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

func TestInstanceStatusWideTableRetainsCompleteBoundedOperationalFields(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 14, 23, 1, 2, 0, time.UTC)
	revision := strings.Repeat("r", 200)
	report := NewInstanceStatusReport(Metadata{Name: "chat"}, InstanceStatusContent{
		Summary:    InstanceStatusSummary{State: InstanceStatusStateReported, Component: RuntimeComponentEngine},
		Deployment: InstanceStatusDeployment{Mode: DeploymentModeOMENative, Source: DeploymentModeSourceComponentAnnotation, Evidence: EvidenceReported},
		Instance: &InstanceStatusInstance{InferenceReplica: "chat-engine", RunningRevision: revision, TargetRevision: revision,
			Operation: &InstanceStatusOperation{ID: "operation-id", Type: "Migrate", Step: "Move", FromNode: "node-old", TargetNodeHints: []string{"node-new"}, StartedAt: &now, LastProgressAt: &now, Deadline: &now}},
		Pods: []InstanceStatusPod{{Name: "pod", Revision: revision, Node: "node-new"}},
	}, ClockFunc(func() time.Time { return now }))
	var out bytes.Buffer
	require.NoError(t, report.WideTable().Write(&out))
	text := out.String()
	for _, want := range []string{revision, "OMENative", "ComponentAnnotation", "operation-id", "node-old", "node-new", now.Format(time.RFC3339)} {
		assert.Contains(t, text, want)
	}
}

func TestInstanceStatusWideTableDoesNotJoinOrTruncateSafeFields(t *testing.T) {
	t.Parallel()
	first := strings.Repeat("a", 253)
	second := strings.Repeat("b", 253)
	target := strings.Repeat("p", 253)
	report := NewInstanceStatusReport(Metadata{Name: "chat"}, InstanceStatusContent{
		Summary:  InstanceStatusSummary{State: InstanceStatusStateReported, Component: RuntimeComponentEngine},
		Instance: &InstanceStatusInstance{Operation: &InstanceStatusOperation{TargetNodeHints: []string{second, first}}},
		Events:   []InstanceStatusEvent{{TargetKind: "Pod", TargetName: target, Reason: "Failed"}},
	}, ClockFunc(func() time.Time { return time.Unix(0, 0) }))
	var out bytes.Buffer
	require.NoError(t, report.WideTable().Write(&out))
	text := out.String()
	assert.Contains(t, text, first)
	assert.Contains(t, text, second)
	assert.Contains(t, text, target)
	assert.NotContains(t, text, first+","+second)
}

func TestInstanceStatusWideTableRetainsEnvelopeAndProvenanceFields(t *testing.T) {
	t.Parallel()
	collected := time.Date(2026, 9, 14, 23, 1, 2, 0, time.UTC)
	sourceTime := collected.Add(-time.Minute)
	report := NewInstanceStatusReport(Metadata{Namespace: "prod", Name: "chat"}, InstanceStatusContent{
		Summary:  InstanceStatusSummary{State: InstanceStatusStatePartial, Component: RuntimeComponentEngine, Index: 2, Evidence: InstanceEvidenceStale, Truncated: true},
		Instance: &InstanceStatusInstance{Index: 2, Conditions: []InstanceStatusCondition{}, Migrations: []InstanceStatusMigration{}},
		Issues:   []InstanceStatusIssue{{Code: InstanceStatusIssueEventsUnavailable, UnavailableReason: UnavailableForbidden}},
	}, ClockFunc(func() time.Time { return collected }))
	report.Sources = []SourceReference{{
		Kind: "PodList", Namespace: "prod", Name: "chat/engine/2", UID: "source-uid",
		Generation: 7, Evidence: EvidenceUnavailable, CollectedAt: sourceTime,
		UnavailableReason: UnavailableForbidden,
	}}
	report.Warnings = []InstanceStatusWarning{{Code: WarningSourceUnavailable}}

	wide := report.WideTable()
	for _, row := range [][]string{
		{"apiVersion", APIVersion},
		{"kind", InstanceStatusReportKind},
		{"metadata namespace", "prod"},
		{"metadata name", "chat"},
		{"collected at", collected.Format(time.RFC3339)},
		{"source[0] kind", "PodList"},
		{"source[0] namespace", "prod"},
		{"source[0] name", "chat/engine/2"},
		{"source[0] uid", "source-uid"},
		{"source[0] generation", "7"},
		{"source[0] evidence", string(EvidenceUnavailable)},
		{"source[0] collected at", sourceTime.Format(time.RFC3339)},
		{"source[0] unavailable", string(UnavailableForbidden)},
		{"instance index", "2"},
		{"issue[0] code", string(InstanceStatusIssueEventsUnavailable)},
		{"issue[0] unavailable", string(UnavailableForbidden)},
		{"warning[0] code", string(WarningSourceUnavailable)},
	} {
		assert.Contains(t, wide.Rows, row)
	}
}
