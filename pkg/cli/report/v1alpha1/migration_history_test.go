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
	"sigs.k8s.io/ome/pkg/cli/report"
)

func TestMigrationHistoryReportCanonicalIsDeterministicAndOwned(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 14, 20, 0, 0, 0, time.UTC)
	earlier := now.Add(-time.Hour)
	completed := now.Add(-time.Minute)
	records := []MigrationHistoryRecord{
		{
			Evidence: MigrationHistoryEvidenceAudit, SourceName: "chat-ome-migration-audit",
			RequestID: "bbbbbbbb-bbbb", Component: RuntimeComponentEngine, SourceInstance: 2,
			Phase: MigrationHistoryPhaseCompleted, State: MigrationHistoryStateTerminal,
			Freshness: MigrationHistoryFreshnessUnverifiable, StartedAt: &earlier, CompletedAt: &completed,
			Outcome: MigrationOutcomeCompleted, Message: "done\n\x1b[31m" + strings.Repeat("界", 300),
			Issues: []MigrationHistoryIssueCode{MigrationHistoryIssueChronologyConflict, MigrationHistoryIssueChronologyConflict},
		},
		{
			Evidence: MigrationHistoryEvidenceAuthoritative, SourceName: "chat-engine",
			RequestID: "aaaaaaaa-aaaa", Component: RuntimeComponentEngine, SourceInstance: 1,
			Trigger: MigrationTriggerManual, Phase: MigrationHistoryPhaseAccepted,
			State: MigrationHistoryStateActive, Freshness: MigrationHistoryFreshnessCurrent,
			StartedAt: &earlier, Outcome: MigrationOutcomeInProgress, TargetNodeHints: []string{"node-b", "node-a", "node-a"},
		},
	}
	reportValue := MigrationHistoryReport{
		APIVersion: "wrong", Kind: "wrong", Metadata: Metadata{Namespace: "prod", Name: "chat"},
		CollectedAt: now,
		Sources: []MigrationHistorySource{
			{Kind: MigrationHistorySourceConfigMap, Namespace: "prod", Name: "chat-ome-migration-audit", Evidence: MigrationHistoryEvidenceAudit, Availability: MigrationHistoryAvailabilityAvailable, Freshness: MigrationHistoryFreshnessUnverifiable, CollectedAt: now},
			{Kind: MigrationHistorySourceInferenceReplica, Namespace: "prod", Name: "related-inferencereplicas", Evidence: MigrationHistoryEvidenceAuthoritative, Availability: MigrationHistoryAvailabilityAvailable, Freshness: MigrationHistoryFreshnessCurrent, CollectedAt: now},
		},
		Content: MigrationHistoryContent{
			Summary: MigrationHistorySummary{State: MigrationHistoryReportPartial},
			Records: records,
			Issues:  []MigrationHistoryIssue{{Code: MigrationHistoryIssueAuditMalformed}, {Code: MigrationHistoryIssueAuditMalformed}},
		},
		Warnings: []MigrationHistoryWarning{{Code: MigrationHistoryWarningPartial}, {Code: MigrationHistoryWarningPartial}},
	}

	canonical := reportValue.Canonical()

	assert.Equal(t, APIVersion, canonical.APIVersion)
	assert.Equal(t, MigrationHistoryReportKind, canonical.Kind)
	assert.Equal(t, MigrationHistoryEvidenceAuthoritative, canonical.Sources[0].Evidence)
	assert.Equal(t, "bbbbbbbb-bbbb", canonical.Content.Records[0].RequestID)
	assert.Equal(t, []MigrationHistoryIssueCode{MigrationHistoryIssueChronologyConflict}, canonical.Content.Records[0].Issues)
	assert.Equal(t, "aaaaaaaa-aaaa", canonical.Content.Records[1].RequestID)
	assert.Equal(t, []string{"node-a", "node-b"}, canonical.Content.Records[1].TargetNodeHints)
	assert.Equal(t, []MigrationHistoryIssue{{Code: MigrationHistoryIssueAuditMalformed}}, canonical.Content.Issues)
	assert.Equal(t, []MigrationHistoryWarning{{Code: MigrationHistoryWarningPartial}}, canonical.Warnings)
	assert.NotEmpty(t, canonical.Content.Records[0].Message)
	assert.LessOrEqual(t, printers.CellDisplayWidth(canonical.Content.Records[0].Message), MigrationHistoryMessageMaxDisplayWidth)
	assert.NotContains(t, canonical.Content.Records[0].Message, "\n")
	assert.NotContains(t, canonical.Content.Records[0].Message, "\x1b")

	// Canonicalization never mutates caller-owned slices or timestamps.
	assert.Equal(t, "bbbbbbbb-bbbb", records[0].RequestID)
	assert.Equal(t, []string{"node-b", "node-a", "node-a"}, records[1].TargetNodeHints)
	assert.Len(t, reportValue.Content.Issues, 2)
	assert.Len(t, reportValue.Warnings, 2)
}

func TestMigrationHistoryReportCanonicalRecordOrderUsesEverySafeField(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 14, 20, 0, 0, 0, time.UTC)
	earlier := now.Add(-time.Minute)
	replacement := int32(3)
	base := MigrationHistoryRecord{
		Evidence: MigrationHistoryEvidenceParent, SourceName: "chat", RequestID: "same-request",
		Component: RuntimeComponentEngine, SourceInstance: 1, Phase: MigrationHistoryPhaseCompleted,
		CompletedAt: &now, TargetNodeHints: []string{}, AuthoritativeIssues: []MigrationIssueCode{},
		Issues: []MigrationHistoryIssueCode{},
	}
	tests := []struct {
		name   string
		mutate func(*MigrationHistoryRecord)
	}{
		{name: "replacement instance", mutate: func(record *MigrationHistoryRecord) { record.ReplacementInstance = &replacement }},
		{name: "trigger", mutate: func(record *MigrationHistoryRecord) { record.Trigger = MigrationTriggerManual }},
		{name: "mode", mutate: func(record *MigrationHistoryRecord) { record.Mode = MigrationHistoryModeSurge }},
		{name: "state", mutate: func(record *MigrationHistoryRecord) { record.State = MigrationHistoryStateTerminal }},
		{name: "freshness", mutate: func(record *MigrationHistoryRecord) { record.Freshness = MigrationHistoryFreshnessCurrent }},
		{name: "from node", mutate: func(record *MigrationHistoryRecord) { record.FromNode = "node-a" }},
		{name: "target nodes", mutate: func(record *MigrationHistoryRecord) { record.TargetNodeHints = []string{"node-a"} }},
		{name: "requested at", mutate: func(record *MigrationHistoryRecord) { record.RequestedAt = &earlier }},
		{name: "started at", mutate: func(record *MigrationHistoryRecord) { record.StartedAt = &earlier }},
		{name: "allocated at", mutate: func(record *MigrationHistoryRecord) { record.AllocatedAt = &earlier }},
		{name: "deadline", mutate: func(record *MigrationHistoryRecord) { record.Deadline = &earlier }},
		{name: "completed at", mutate: func(record *MigrationHistoryRecord) { record.CompletedAt = &earlier }},
		{name: "event count", mutate: func(record *MigrationHistoryRecord) { record.EventCount = 1 }},
		{name: "outcome", mutate: func(record *MigrationHistoryRecord) { record.Outcome = MigrationOutcomeCompleted }},
		{name: "message", mutate: func(record *MigrationHistoryRecord) { record.Message = "AAA" }},
		{name: "authoritative issues", mutate: func(record *MigrationHistoryRecord) {
			record.AuthoritativeIssues = []MigrationIssueCode{MigrationIssueTimestampInvalid}
		}},
		{name: "record issues", mutate: func(record *MigrationHistoryRecord) {
			record.Issues = []MigrationHistoryIssueCode{MigrationHistoryIssueTimestampInvalid}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			variant := base
			test.mutate(&variant)
			forward := NewMigrationHistoryReport(Metadata{Namespace: "prod", Name: "chat"}, MigrationHistoryContent{
				Records: []MigrationHistoryRecord{base, variant},
			}, ClockFunc(func() time.Time { return now }))
			reverse := NewMigrationHistoryReport(Metadata{Namespace: "prod", Name: "chat"}, MigrationHistoryContent{
				Records: []MigrationHistoryRecord{variant, base},
			}, ClockFunc(func() time.Time { return now }))

			assert.Equal(t, forward.Content.Records, reverse.Content.Records,
				"canonical output must not depend on a permutation of %s", test.name)
		})
	}
}

func TestMigrationHistoryReportDefensivelyRedactsUnknownTypedValues(t *testing.T) {
	t.Parallel()

	secret := "SECRET_CANARY_DO_NOT_EMIT/value"
	value := NewMigrationHistoryReport(Metadata{Name: "chat"}, MigrationHistoryContent{
		Records: []MigrationHistoryRecord{{
			Evidence: MigrationHistoryEvidence(secret), SourceName: secret, RequestID: secret,
			Component: RuntimeComponentType(secret), Trigger: MigrationTrigger(secret),
			Mode: MigrationHistoryMode(secret), Phase: MigrationHistoryPhase(secret),
			State: MigrationHistoryState(secret), Freshness: MigrationHistoryFreshness(secret),
			Outcome: MigrationOutcome(secret), Issues: []MigrationHistoryIssueCode{MigrationHistoryIssueCode(secret)},
		}},
		Issues: []MigrationHistoryIssue{{Code: MigrationHistoryIssueCode(secret), RequestID: secret}},
	}, ClockFunc(func() time.Time { return time.Unix(0, 0) }))
	value.Sources = []MigrationHistorySource{{
		Kind: MigrationHistorySourceKind(secret), Name: secret, Evidence: MigrationHistoryEvidence(secret),
		Availability: MigrationHistoryAvailability(secret), Freshness: MigrationHistoryFreshness(secret),
	}}

	encoded, err := json.Marshal(value.Canonical())
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), secret)
	assert.NotContains(t, string(encoded), `"evidence":"Authoritative"`,
		"an unknown evidence value must never be promoted to work authority")
	assert.Contains(t, string(encoded), `"evidence":"Unknown"`)
}

func TestMigrationHistoryCompactAndWideTablesAreExactAndSafe(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 14, 20, 0, 0, 0, time.UTC)
	started := now.Add(-time.Hour)
	completed := now.Add(-time.Minute)
	reportValue := NewMigrationHistoryReport(
		Metadata{Namespace: "prod", Name: "chat"},
		MigrationHistoryContent{
			Summary: MigrationHistorySummary{State: MigrationHistoryReportPartial, Requests: 2, Records: 2, Authoritative: 1, Audit: 1, ActiveAuthoritative: 1, TerminalAuthoritative: 0, Invalid: 0, Truncated: false},
			Records: []MigrationHistoryRecord{
				{Evidence: MigrationHistoryEvidenceAudit, SourceName: "chat-ome-migration-audit", RequestID: "bbbbbbbb-bbbb", Component: RuntimeComponentEngine, SourceInstance: 2, Phase: MigrationHistoryPhaseCompleted, State: MigrationHistoryStateTerminal, Freshness: MigrationHistoryFreshnessUnverifiable, StartedAt: &started, CompletedAt: &completed, Outcome: MigrationOutcomeCompleted, Message: "replacement became ready"},
				{Evidence: MigrationHistoryEvidenceAuthoritative, SourceName: "chat-engine", RequestID: "aaaaaaaa-aaaa", Component: RuntimeComponentEngine, SourceInstance: 1, Trigger: MigrationTriggerManual, Phase: MigrationHistoryPhaseAccepted, State: MigrationHistoryStateActive, Freshness: MigrationHistoryFreshnessCurrent, StartedAt: &started, Outcome: MigrationOutcomeInProgress},
			},
			Issues: []MigrationHistoryIssue{{Code: MigrationHistoryIssueAuditUnavailable}},
		},
		ClockFunc(func() time.Time { return now }),
	)
	reportValue.Sources = []MigrationHistorySource{
		{Kind: MigrationHistorySourceInferenceReplica, Namespace: "prod", Name: "related-inferencereplicas", Evidence: MigrationHistoryEvidenceAuthoritative, Availability: MigrationHistoryAvailabilityAvailable, Freshness: MigrationHistoryFreshnessCurrent, ObservedPages: 1, ObservedItems: 1, CollectedAt: now},
		{Kind: MigrationHistorySourceConfigMap, Namespace: "prod", Name: "chat-ome-migration-audit", Evidence: MigrationHistoryEvidenceAudit, Availability: MigrationHistoryAvailabilityAvailable, Freshness: MigrationHistoryFreshnessUnverifiable, Bounded: true, CollectedAt: now},
	}

	var compact bytes.Buffer
	require.NoError(t, reportValue.Table().Write(&compact))
	assert.Equal(t,
		"EVID     REQUEST/COMP   PHASE/STATE   WHEN           DETAIL\n"+
			"AUDIT    bbbbbbbb/E     Completed/T   09-14T19:59Z   replacement becam...\n"+
			"AUTH     aaaaaaaa/E     Accepted/A    09-14T19:00Z   -\n"+
			"REPORT   -/-            Partial/?     -              AuditUnavailable\n",
		compact.String(),
	)
	for _, line := range strings.Split(strings.TrimSuffix(compact.String(), "\n"), "\n") {
		assert.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}

	var wide bytes.Buffer
	require.NoError(t, reportValue.WideTable().Write(&wide))
	assert.Equal(t,
		"EVIDENCE        AVAILABILITY   WINDOW      SOURCE                     REQUEST         COMPONENT   SOURCE-INSTANCE   REPLACEMENT   TRIGGER   MODE      PHASE       STATE      FRESHNESS      FROM-NODE   TARGET-NODES   REQUESTED   STARTED                ALLOCATED   DEADLINE   COMPLETED              EVENTS   OUTCOME      DETAIL                     ISSUES\n"+
			"AuditHistory    Available      0p/0i/B/C   chat-ome-migration-audit   bbbbbbbb-bbbb   engine      2                 -             Unknown   Unknown   Completed   Terminal   Unverifiable   -           -              -           2026-09-14T19:00:00Z   -           -          2026-09-14T19:59:00Z   0        Completed    replacement became ready   -\n"+
			"Authoritative   Available      1p/1i/C     chat-engine                aaaaaaaa-aaaa   engine      1                 -             Manual    Unknown   Accepted    Active     Current        -           -              -           2026-09-14T19:00:00Z   -           -          -                      0        InProgress   -                          -\n"+
			"Report          -              -           -                          -               -           -                 -             -         -         -           Partial    -              -           -              -           -                      -           -          -                      -        -            -                          AuditUnavailable\n",
		wide.String(),
	)
}

func TestMigrationHistoryMachineOutputsAreExact(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 14, 20, 0, 0, 0, time.UTC)
	reportValue := NewMigrationHistoryReport(Metadata{Namespace: "prod", Name: "chat"}, MigrationHistoryContent{
		Summary: MigrationHistorySummary{State: MigrationHistoryReportEmpty},
		Records: []MigrationHistoryRecord{}, Issues: []MigrationHistoryIssue{},
	}, ClockFunc(func() time.Time { return now }))
	reportValue.Sources = []MigrationHistorySource{}
	reportValue.Warnings = []MigrationHistoryWarning{}

	var jsonOut bytes.Buffer
	require.NoError(t, report.Write(&jsonOut, report.FormatJSON, reportValue))
	assert.Equal(t, `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "MigrationHistoryReport",
  "metadata": {
    "namespace": "prod",
    "name": "chat"
  },
  "collectedAt": "2026-09-14T20:00:00Z",
  "sources": [],
  "content": {
    "summary": {
      "state": "Empty",
      "requests": 0,
      "records": 0,
      "authoritative": 0,
      "parentSummary": 0,
      "audit": 0,
      "activeAuthoritative": 0,
      "terminalAuthoritative": 0,
      "invalid": 0,
      "truncated": false
    },
    "records": [],
    "issues": []
  },
  "warnings": []
}
`, jsonOut.String())

	var yamlOut bytes.Buffer
	require.NoError(t, report.Write(&yamlOut, report.FormatYAML, reportValue))
	assert.Equal(t, `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-09-14T20:00:00Z"
content:
  issues: []
  records: []
  summary:
    activeAuthoritative: 0
    audit: 0
    authoritative: 0
    invalid: 0
    parentSummary: 0
    records: 0
    requests: 0
    state: Empty
    terminalAuthoritative: 0
    truncated: false
kind: MigrationHistoryReport
metadata:
  name: chat
  namespace: prod
sources: []
warnings: []
`, yamlOut.String())
}

func TestMigrationHistoryCompactTableIsBoundedByDisplayColumns(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 14, 20, 0, 0, 0, time.UTC)
	reportValue := NewMigrationHistoryReport(Metadata{Namespace: "prod", Name: "chat"}, MigrationHistoryContent{
		Summary: MigrationHistorySummary{State: MigrationHistoryReportPartial},
		Records: []MigrationHistoryRecord{{
			Evidence: MigrationHistoryEvidenceAudit, SourceName: "chat-ome-migration-audit",
			RequestID: "long-request-identifier", Component: RuntimeComponentEngine,
			Phase: MigrationHistoryPhaseSurgePending, State: MigrationHistoryStateActive,
			Freshness: MigrationHistoryFreshnessUnverifiable, StartedAt: &now,
			Outcome: MigrationOutcomeInProgress, Message: strings.Repeat("界", 100),
		}},
		Issues: []MigrationHistoryIssue{{Code: MigrationHistoryIssueCrossSourceIdentityConflict, RequestID: "long-request-identifier"}},
	}, ClockFunc(func() time.Time { return now }))

	var output bytes.Buffer
	require.NoError(t, reportValue.Table().Write(&output))
	assert.NotContains(t, output.String(), string(MigrationHistoryIssueCrossSourceIdentityConflict))
	assert.Contains(t, output.String(), "CrossSourceIdenti...")
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
	}
	var wide bytes.Buffer
	require.NoError(t, reportValue.WideTable().Write(&wide))
	assert.Contains(t, wide.String(), string(MigrationHistoryIssueCrossSourceIdentityConflict))
	encoded, err := json.Marshal(reportValue.Canonical())
	require.NoError(t, err)
	assert.Contains(t, string(encoded), string(MigrationHistoryIssueCrossSourceIdentityConflict))
}

func TestMigrationHistoryCompactTablePrioritizesInvalidEvidence(t *testing.T) {
	t.Parallel()

	reportValue := NewMigrationHistoryReport(Metadata{Namespace: "prod", Name: "chat"}, MigrationHistoryContent{
		Summary: MigrationHistorySummary{State: MigrationHistoryReportPartial},
		Records: []MigrationHistoryRecord{{
			Evidence: MigrationHistoryEvidenceAudit, SourceName: "chat-ome-migration-audit",
			RequestID: "bad-record", Component: RuntimeComponentEngine,
			Phase: MigrationHistoryPhaseCompleted, State: MigrationHistoryStateInvalid,
			Freshness: MigrationHistoryFreshnessUnverifiable, Outcome: MigrationOutcomeCompleted,
			Message: "misleading success detail", Issues: []MigrationHistoryIssueCode{MigrationHistoryIssueTimestampInvalid},
		}},
	}, ClockFunc(func() time.Time { return time.Unix(0, 0) }))

	var output bytes.Buffer
	require.NoError(t, reportValue.Table().Write(&output))
	assert.Contains(t, output.String(), "TimestampInvalid")
	assert.NotContains(t, output.String(), "misleading success")
}
