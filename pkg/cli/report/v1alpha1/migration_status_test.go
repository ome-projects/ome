package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
)

func TestMigrationStatusReportCanonicalizesWithoutMutatingInput(t *testing.T) {
	t.Parallel()

	collected := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.FixedZone("west", -7*60*60))
	started := time.Date(2026, time.September, 14, 18, 1, 0, 0, time.UTC)
	report := MigrationStatusReport{
		APIVersion:  "wrong",
		Kind:        "wrong",
		Metadata:    Metadata{Namespace: "prod", Name: "chat"},
		CollectedAt: collected,
		Sources: []MigrationSourceReference{
			{Kind: MigrationSourceInferenceReplica, Namespace: "prod", Name: "chat-router", UID: "ir-r", Generation: 2, ObservedGeneration: 1, Freshness: StatusFreshnessStale, CollectedAt: collected},
			{Kind: MigrationSourceInferenceService, Namespace: "prod", Name: "chat", UID: "isvc", Generation: 8, Freshness: StatusFreshnessCurrent, CollectedAt: collected},
		},
		Content: MigrationStatusContent{
			Summary:  MigrationSummary{State: MigrationReportStatePartial, Records: 2, Active: 1, Terminal: 1},
			Capacity: MigrationCapacityEvidence{Evidence: EvidenceComputed, ActiveAllocated: 1, ConcurrencyLimit: EvidenceUnavailable, RateLimit: EvidenceUnavailable},
			Migrations: []MigrationRecord{
				{RequestID: "bbbbbbbb-bbbb", SourceName: "chat-router", Component: RuntimeComponentRouter, SourceInstance: 2, Trigger: MigrationTriggerManual, Phase: MigrationPhaseAccepted, Classification: MigrationClassificationActive, Freshness: StatusFreshnessStale, RequestedAtEvidence: EvidenceUnavailable, StartedAt: &started, ReasonEvidence: MigrationReasonOperatorSupplied, MessageEvidence: MigrationMessagePresent, Outcome: MigrationOutcomeInProgress, TargetNodeHints: []string{"node-z", "node-a"}, Issues: []MigrationIssueCode{MigrationIssueSourceStale, MigrationIssueTimestampInvalid}},
				{RequestID: "aaaaaaaa-aaaa", SourceName: "chat-engine", Component: RuntimeComponentEngine, SourceInstance: 1, Trigger: MigrationTriggerAuto, Phase: MigrationPhaseRelocated, Classification: MigrationClassificationTerminal, Freshness: StatusFreshnessCurrent, RequestedAtEvidence: EvidenceUnavailable, ReasonEvidence: MigrationReasonAutoRecover, MessageEvidence: MigrationMessagePresent, Outcome: MigrationOutcomeRelocated, TargetNodeHints: nil, Issues: nil},
			},
			Issues: []MigrationIssue{
				{Code: MigrationIssueTimestampInvalid, SourceName: "chat-router", RequestID: "bbbbbbbb-bbbb", Component: RuntimeComponentRouter},
				{Code: MigrationIssueSourceStale, SourceName: "chat-router", Component: RuntimeComponentRouter},
			},
		},
		Warnings: nil,
	}

	got := report.Canonical()

	assert.Equal(t, APIVersion, got.APIVersion)
	assert.Equal(t, MigrationStatusReportKind, got.Kind)
	assert.Equal(t, collected.UTC(), got.CollectedAt)
	require.Len(t, got.Sources, 2)
	assert.Equal(t, MigrationSourceInferenceService, got.Sources[0].Kind)
	assert.Equal(t, "aaaaaaaa-aaaa", got.Content.Migrations[0].RequestID)
	assert.Equal(t, []string{"node-a", "node-z"}, got.Content.Migrations[1].TargetNodeHints)
	assert.Equal(t, []MigrationIssueCode{MigrationIssueSourceStale, MigrationIssueTimestampInvalid}, got.Content.Migrations[1].Issues)
	assert.NotNil(t, got.Warnings)
	assert.NotNil(t, got.Content.Migrations[0].TargetNodeHints)
	assert.NotNil(t, got.Content.Migrations[0].Issues)

	// Canonicalization owns every slice and pointer it returns.
	got.Sources[0].Name = "changed"
	got.Content.Migrations[1].TargetNodeHints[0] = "changed"
	*got.Content.Migrations[1].StartedAt = time.Time{}
	assert.Equal(t, "chat-router", report.Sources[0].Name)
	assert.Equal(t, []string{"node-z", "node-a"}, report.Content.Migrations[0].TargetNodeHints)
	assert.Equal(t, started, *report.Content.Migrations[0].StartedAt)
}

func TestMigrationStatusReportCanonicalRecordOrderUsesAllFields(t *testing.T) {
	t.Parallel()

	early := time.Date(2026, time.September, 14, 17, 0, 0, 0, time.UTC)
	late := early.Add(time.Minute)
	base := MigrationRecord{
		RequestID: "same-request", SourceName: "chat-engine", Component: RuntimeComponentEngine,
		SourceInstance: 1, Trigger: MigrationTriggerManual, Phase: MigrationPhaseAccepted,
		Classification: MigrationClassificationActive, Freshness: StatusFreshnessCurrent,
		RequestedAtEvidence: EvidenceUnavailable, Deadline: &late,
		ReasonEvidence: MigrationReasonUnavailable, MessageEvidence: MigrationMessageAbsent,
		Outcome: MigrationOutcomeInProgress, TargetNodeHints: []string{}, Issues: []MigrationIssueCode{},
	}
	first := base
	first.StartedAt = &early
	second := base
	second.StartedAt = &late
	left := MigrationStatusReport{Content: MigrationStatusContent{Migrations: []MigrationRecord{second, first}}}.Canonical()
	right := MigrationStatusReport{Content: MigrationStatusContent{Migrations: []MigrationRecord{first, second}}}.Canonical()

	leftJSON, err := json.Marshal(left)
	require.NoError(t, err)
	rightJSON, err := json.Marshal(right)
	require.NoError(t, err)
	assert.Equal(t, string(leftJSON), string(rightJSON))
}

func TestMigrationStatusTableIsCompactAndExplicitWhenEmpty(t *testing.T) {
	t.Parallel()

	empty := MigrationStatusReport{Content: MigrationStatusContent{
		Summary:    MigrationSummary{State: MigrationReportStateEmpty},
		Migrations: []MigrationRecord{},
		Issues:     []MigrationIssue{},
	}}
	assert.Equal(t, [][]string{{"-/-", "Empty/Current", "-"}}, empty.Table().Rows)

	report := MigrationStatusReport{Content: MigrationStatusContent{
		Summary: MigrationSummary{State: MigrationReportStateReported},
		Migrations: []MigrationRecord{{
			RequestID: "12345678-1234-1234-1234-123456789abc", Component: RuntimeComponentDecoder,
			SourceInstance: 2_000_000, SurgeInstance: int32Pointer(2_000_001), Trigger: MigrationTriggerManual,
			Phase: MigrationPhaseSurgePending, Classification: MigrationClassificationActive, Freshness: StatusFreshnessCurrent,
			Issues: []MigrationIssueCode{MigrationIssueTimestampInvalid, MigrationIssueSourceStale},
		}},
		Issues: []MigrationIssue{},
	}}
	table := report.Table()
	assert.Equal(t, []string{"REQUEST/COMP", "STATUS", "ISSUE"}, table.Headers)
	assert.Equal(t, [][]string{
		{"12345678/decoder", "SurgePending/Active/Current", "SourceGenerationStale"},
		{"12345678/decoder", "SurgePending/Active/Current", "TimestampInvalid"},
	}, table.Rows)

	// Maximum cell widths plus two 3-column gaps are exactly 80 columns.
	widths := []int{16, 32, 26}
	total := 2 * 3
	for _, width := range widths {
		total += width
	}
	assert.Equal(t, 80, total)
}

func TestMigrationStatusTableNamesFreshnessAndIssueCodes(t *testing.T) {
	t.Parallel()

	document := MigrationStatusReport{Content: MigrationStatusContent{
		Summary: MigrationSummary{State: MigrationReportStatePartial},
		Migrations: []MigrationRecord{{
			RequestID: "12345678-1234-1234-1234-123456789abc", Component: RuntimeComponentDecoder,
			Phase: MigrationPhaseAccepted, Classification: MigrationClassificationActive,
			Freshness: StatusFreshnessStale,
			Issues:    []MigrationIssueCode{MigrationIssueTimestampInvalid, MigrationIssueSourceStale},
		}},
		Issues: []MigrationIssue{{Code: MigrationIssueSourcesTruncated}},
	}}

	table := document.Table()
	assert.Equal(t, []string{"REQUEST/COMP", "STATUS", "ISSUE"}, table.Headers)
	assert.Equal(t, [][]string{
		{"12345678/decoder", "Accepted/Active/Stale", "SourceGenerationStale"},
		{"12345678/decoder", "Accepted/Active/Stale", "TimestampInvalid"},
		{"REPORT/-", "Partial/Stale", "SourcesTruncated"},
	}, table.Rows)
}

func TestMigrationStatusTableDeduplicatesOnlyExactScopedIssues(t *testing.T) {
	t.Parallel()

	document := MigrationStatusReport{Content: MigrationStatusContent{
		Summary: MigrationSummary{State: MigrationReportStatePartial},
		Issues: []MigrationIssue{
			{Code: MigrationIssueSourceOwnerMismatch, SourceName: "chat-engine", Component: RuntimeComponentEngine},
			{Code: MigrationIssueSourceOwnerMismatch, SourceName: "chat-engine", Component: RuntimeComponentEngine},
			{Code: MigrationIssueSourceOwnerMismatch, SourceName: "chat-engine-shadow", Component: RuntimeComponentEngine},
			{Code: MigrationIssueSourceOwnerMismatch, SourceName: "chat-router", Component: RuntimeComponentRouter},
		},
	}}

	table := document.Table()

	assert.Equal(t, [][]string{
		{"REPORT/engine", "Partial/Current", "SourceOwnerMismatch"},
		{"REPORT/engine", "Partial/Current", "SourceOwnerMismatch"},
		{"REPORT/router", "Partial/Current", "SourceOwnerMismatch"},
	}, table.Rows)
}

func TestMigrationStatusTableStaysWithin80ColumnsAtCommonTerminalWidths(t *testing.T) {
	t.Parallel()

	document := MigrationStatusReport{Content: MigrationStatusContent{
		Summary: MigrationSummary{State: MigrationReportStateReported},
		Migrations: []MigrationRecord{
			{
				RequestID: "12345678-1234-1234-1234-123456789abc", Component: RuntimeComponentDecoder,
				SourceInstance: 2_000_000, SurgeInstance: int32Pointer(2_000_001), Trigger: MigrationTriggerUnknown,
				Phase: MigrationPhaseSurgePending, Classification: MigrationClassificationTerminal,
				Freshness: StatusFreshnessUnobserved, Issues: []MigrationIssueCode{MigrationIssueSourceUnobserved},
			},
			{
				RequestID: "surge-record", Component: RuntimeComponentEngine,
				SourceInstance: 1, SurgeInstance: int32Pointer(2), Trigger: MigrationTriggerManual,
				Phase: MigrationPhaseSurgePending, Classification: MigrationClassificationActive,
			},
			{
				RequestID: "done-record", Component: RuntimeComponentRouter,
				SourceInstance: 3, Trigger: MigrationTriggerAuto,
				Phase: MigrationPhaseRelocated, Classification: MigrationClassificationTerminal,
				Issues: []MigrationIssueCode{MigrationIssueTimestampInvalid},
			},
		},
	}}
	for _, width := range []int{80, 120} {
		writer := &migrationTerminalBuffer{width: width}
		require.NoError(t, report.Write(writer, report.FormatTable, document))
		lines := strings.Split(strings.TrimSuffix(writer.String(), "\n"), "\n")
		assert.Len(t, lines, 4, "the natural table must not wrap at width %d: %q", width, writer.String())
		for _, line := range lines {
			assert.LessOrEqual(t, len(line), 80, "terminal width %d: %q", width, line)
		}
	}
}

func TestMigrationStatusTableSanitizesDefensiveTypedValues(t *testing.T) {
	t.Parallel()

	document := MigrationStatusReport{Content: MigrationStatusContent{
		Summary: MigrationSummary{State: MigrationReportState("hostile-state-" + strings.Repeat("x", 100))},
		Migrations: []MigrationRecord{{
			RequestID: "bad\nrequest", Component: RuntimeComponentType("engine\n\x1b\u202e" + strings.Repeat("界", 80)),
			Trigger: MigrationTrigger("Manual\rvalue"), Phase: MigrationPhase("Future\tphase" + strings.Repeat("x", 100)),
			Classification: MigrationClassification("Invalid" + strings.Repeat("x", 100)),
		}},
	}}
	for _, writer := range []interface {
		Write([]byte) (int, error)
		String() string
	}{&bytes.Buffer{}, &migrationTerminalBuffer{width: 120}} {
		require.NoError(t, report.Write(writer, report.FormatTable, document))
		output := writer.String()
		assert.NotContains(t, output, "\x1b")
		assert.NotContains(t, output, "\u202e")
		assert.NotContains(t, output, "engine")
		assert.NotContains(t, output, "Future")
		assert.NotContains(t, output, "Invalidx")
		assert.NotContains(t, output, "界")
		lines := strings.Split(strings.TrimSuffix(output, "\n"), "\n")
		assert.Len(t, lines, 2)
		for _, line := range lines {
			assert.LessOrEqual(t, len(line), 80, "%q", line)
		}
	}

	empty := MigrationStatusReport{Content: MigrationStatusContent{
		Summary: MigrationSummary{State: MigrationReportState(strings.Repeat("x", 200))},
	}}
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, empty))
	assert.NotContains(t, output.String(), strings.Repeat("x", 20))
	assert.LessOrEqual(t, len(strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")[1]), 80)
}

func int32Pointer(value int32) *int32 { return &value }

type migrationTerminalBuffer struct {
	bytes.Buffer
	width int
}

func (w *migrationTerminalBuffer) TerminalWidth() (int, bool) { return w.width, true }
