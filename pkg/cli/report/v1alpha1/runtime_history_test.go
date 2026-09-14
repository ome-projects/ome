package v1alpha1_test

import (
	"bytes"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestRuntimeHistoryCanonicalReturnsDeepOrderedUTCCopy(t *testing.T) {
	content := runtimeHistoryContent()
	wantOriginal := runtimeHistoryContent()
	originalRuntime := content.Runtime
	originalSource := content.Revisions[1].Source

	got := content.Canonical()

	assert.Equal(t, []string{"revision-a", "revision-b", "revision-old"}, revisionNames(got.Revisions))
	assert.Equal(t, []v1alpha1.RuntimeRevisionRole{
		v1alpha1.RuntimeRevisionRoleActive,
		v1alpha1.RuntimeRevisionRoleRequested,
		v1alpha1.RuntimeRevisionRoleReported,
		v1alpha1.RuntimeRevisionRoleHistory,
	}, got.Revisions[0].Roles)
	assert.Equal(t, []v1alpha1.RuntimeIssueCode{
		v1alpha1.RuntimeIssueRevisionHashInvalid,
		v1alpha1.RuntimeIssueRevisionNameMismatch,
	}, got.Revisions[0].Issues)
	assert.Equal(t, []v1alpha1.RuntimeIssue{
		{Code: v1alpha1.RuntimeIssueHistoryTruncated},
		{Code: v1alpha1.RuntimeIssueRevisionHashCollision, Revision: "revision-b"},
	}, got.Issues)
	for _, revision := range got.Revisions {
		assert.Equal(t, time.UTC, revision.Revision.CreatedAt.Location())
	}

	require.NotSame(t, originalRuntime, got.Runtime)
	require.NotSame(t, originalSource, got.Revisions[1].Source)
	got.Runtime.Name = "returned-runtime"
	got.Revisions[0].Revision.Name = "returned-revision-identity"
	got.Revisions[1].Source.Name = "returned-revision-source"
	got.Revisions[0].Roles[0] = v1alpha1.RuntimeRevisionRole("ReturnedRole")
	got.Revisions[0].Issues[0] = v1alpha1.RuntimeIssueHistoryUnavailable
	got.Issues[0].Revision = "returned-history-issue"

	assert.Equal(t, wantOriginal, content)
}

func TestRuntimeHistoryCanonicalIsDeterministicForConflictingEntries(t *testing.T) {
	createdAt := time.Date(2026, time.August, 31, 18, 20, 0, 0, time.UTC)
	source := runtimeObject(v1alpha1.RuntimeKindServingRuntime, "prod", "vllm")
	base := v1alpha1.RuntimeRevisionEntry{
		Revision: v1alpha1.RuntimeRevisionReference{
			Namespace: "ome", Name: "revision", UID: "uid", CreatedAt: runtimeReportTime(createdAt),
		},
		Source: &source, Hash: "aaaaaaaa",
		Roles:          []v1alpha1.RuntimeRevisionRole{v1alpha1.RuntimeRevisionRoleHistory},
		Consistency:    v1alpha1.RevisionConsistencyConsistent,
		RelationToLive: v1alpha1.RevisionRelationMatchesLive,
		Issues:         []v1alpha1.RuntimeIssueCode{v1alpha1.RuntimeIssueRevisionHashInvalid},
	}

	tests := []struct {
		name   string
		early  func(v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry
		late   func(v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry
		assert func(*testing.T, v1alpha1.RuntimeRevisionEntry)
	}{
		{
			name: "revision namespace",
			early: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Revision.Namespace = "a"
				return entry
			},
			late: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Revision.Namespace = "z"
				return entry
			},
			assert: func(t *testing.T, entry v1alpha1.RuntimeRevisionEntry) {
				assert.Equal(t, "a", entry.Revision.Namespace)
			},
		},
		{
			name: "revision name",
			early: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Revision.Name = "a"
				return entry
			},
			late: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Revision.Name = "z"
				return entry
			},
			assert: func(t *testing.T, entry v1alpha1.RuntimeRevisionEntry) { assert.Equal(t, "a", entry.Revision.Name) },
		},
		{
			name: "revision uid",
			early: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Revision.UID = "a"
				return entry
			},
			late: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Revision.UID = "z"
				return entry
			},
			assert: func(t *testing.T, entry v1alpha1.RuntimeRevisionEntry) { assert.Equal(t, "a", entry.Revision.UID) },
		},
		{
			name: "hash",
			early: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Hash = "aaaaaaaa"
				return entry
			},
			late: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Hash = "zzzzzzzz"
				return entry
			},
			assert: func(t *testing.T, entry v1alpha1.RuntimeRevisionEntry) { assert.Equal(t, "aaaaaaaa", entry.Hash) },
		},
		{
			name: "source identity",
			early: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				sourceCopy := *entry.Source
				sourceCopy.Name = "a"
				entry.Source = &sourceCopy
				return entry
			},
			late: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				sourceCopy := *entry.Source
				sourceCopy.Name = "z"
				entry.Source = &sourceCopy
				return entry
			},
			assert: func(t *testing.T, entry v1alpha1.RuntimeRevisionEntry) { assert.Equal(t, "a", entry.Source.Name) },
		},
		{
			name: "roles",
			early: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Roles = []v1alpha1.RuntimeRevisionRole{v1alpha1.RuntimeRevisionRoleActive}
				return entry
			},
			late: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Roles = []v1alpha1.RuntimeRevisionRole{v1alpha1.RuntimeRevisionRoleHistory}
				return entry
			},
			assert: func(t *testing.T, entry v1alpha1.RuntimeRevisionEntry) {
				assert.Equal(t, []v1alpha1.RuntimeRevisionRole{v1alpha1.RuntimeRevisionRoleActive}, entry.Roles)
			},
		},
		{
			name: "consistency",
			early: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Consistency = v1alpha1.RevisionConsistencyConsistent
				return entry
			},
			late: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Consistency = v1alpha1.RevisionConsistencyUnknown
				return entry
			},
			assert: func(t *testing.T, entry v1alpha1.RuntimeRevisionEntry) {
				assert.Equal(t, v1alpha1.RevisionConsistencyConsistent, entry.Consistency)
			},
		},
		{
			name: "relation",
			early: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.RelationToLive = v1alpha1.RevisionRelationDiffersFromLive
				return entry
			},
			late: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.RelationToLive = v1alpha1.RevisionRelationUnknown
				return entry
			},
			assert: func(t *testing.T, entry v1alpha1.RuntimeRevisionEntry) {
				assert.Equal(t, v1alpha1.RevisionRelationDiffersFromLive, entry.RelationToLive)
			},
		},
		{
			name: "issues",
			early: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Issues = []v1alpha1.RuntimeIssueCode{v1alpha1.RuntimeIssueRevisionHashInvalid}
				return entry
			},
			late: func(entry v1alpha1.RuntimeRevisionEntry) v1alpha1.RuntimeRevisionEntry {
				entry.Issues = []v1alpha1.RuntimeIssueCode{v1alpha1.RuntimeIssueRevisionNameMismatch}
				return entry
			},
			assert: func(t *testing.T, entry v1alpha1.RuntimeRevisionEntry) {
				assert.Equal(t, []v1alpha1.RuntimeIssueCode{v1alpha1.RuntimeIssueRevisionHashInvalid}, entry.Issues)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			early, late := tt.early(base), tt.late(base)
			forward := (v1alpha1.RuntimeHistoryContent{Revisions: []v1alpha1.RuntimeRevisionEntry{early, late}}).Canonical()
			reverse := (v1alpha1.RuntimeHistoryContent{Revisions: []v1alpha1.RuntimeRevisionEntry{late, early}}).Canonical()

			require.Len(t, forward.Revisions, 2)
			require.True(t, slices.EqualFunc(forward.Revisions, reverse.Revisions, func(a, b v1alpha1.RuntimeRevisionEntry) bool {
				return reflect.DeepEqual(a, b)
			}), "input order must not affect canonical history")
			tt.assert(t, forward.Revisions[0])
		})
	}
}

func TestRuntimeHistoryCanonicalOrdersEverySourceIdentityField(t *testing.T) {
	createdAt := time.Date(2026, time.August, 31, 18, 20, 0, 0, time.UTC)
	baseSource := v1alpha1.RuntimeObjectReference{
		APIVersion: "ome.io/v1beta1", Kind: v1alpha1.RuntimeKindServingRuntime,
		Namespace: "prod", Name: "vllm", UID: "uid", Generation: 2,
	}
	baseEntry := v1alpha1.RuntimeRevisionEntry{
		Revision: v1alpha1.RuntimeRevisionReference{
			Namespace: "ome", Name: "revision", UID: "uid", CreatedAt: runtimeReportTime(createdAt),
		},
		Hash: "aaaaaaaa", Roles: []v1alpha1.RuntimeRevisionRole{}, Issues: []v1alpha1.RuntimeIssueCode{},
	}

	tests := []struct {
		name  string
		early *v1alpha1.RuntimeObjectReference
		late  *v1alpha1.RuntimeObjectReference
	}{
		{name: "nil", early: nil, late: runtimeSourceCopy(baseSource, func(*v1alpha1.RuntimeObjectReference) {})},
		{
			name:  "api version",
			early: runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.APIVersion = "a" }),
			late:  runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.APIVersion = "z" }),
		},
		{
			name:  "kind",
			early: runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.Kind = v1alpha1.RuntimeKindClusterServingRuntime }),
			late:  runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.Kind = v1alpha1.RuntimeKindServingRuntime }),
		},
		{
			name:  "namespace",
			early: runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.Namespace = "a" }),
			late:  runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.Namespace = "z" }),
		},
		{
			name:  "name",
			early: runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.Name = "a" }),
			late:  runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.Name = "z" }),
		},
		{
			name:  "uid",
			early: runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.UID = "a" }),
			late:  runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.UID = "z" }),
		},
		{
			name:  "generation",
			early: runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.Generation = 1 }),
			late:  runtimeSourceCopy(baseSource, func(source *v1alpha1.RuntimeObjectReference) { source.Generation = 3 }),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			early, late := baseEntry, baseEntry
			early.Source, late.Source = tt.early, tt.late
			forward := (v1alpha1.RuntimeHistoryContent{Revisions: []v1alpha1.RuntimeRevisionEntry{early, late}}).Canonical()
			reverse := (v1alpha1.RuntimeHistoryContent{Revisions: []v1alpha1.RuntimeRevisionEntry{late, early}}).Canonical()

			require.Len(t, forward.Revisions, 2)
			assert.True(t, reflect.DeepEqual(forward.Revisions, reverse.Revisions), "input order must not affect source ordering")
			assert.Equal(t, tt.early, forward.Revisions[0].Source)
		})
	}
}

func TestRuntimeHistoryAllowsUnavailableRuntimeIdentity(t *testing.T) {
	runtimeField, ok := reflect.TypeOf(v1alpha1.RuntimeHistoryContent{}).FieldByName("Runtime")
	require.True(t, ok)
	assert.Equal(t, reflect.Pointer, runtimeField.Type.Kind())
	assert.Contains(t, runtimeField.Tag.Get("json"), "omitempty")

	reportValue := v1alpha1.NewRuntimeHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RuntimeHistoryContent{
			Observation:  v1alpha1.HistoryObservationStateUnavailable,
			Completeness: v1alpha1.HistoryCompletenessIncomplete,
			Issues:       []v1alpha1.RuntimeIssue{{Code: v1alpha1.RuntimeIssueHistoryUnavailable}},
		},
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatJSON, reportValue))
	assert.NotContains(t, output.String(), `"runtime"`)
}

func TestRuntimeHistoryCanonicalNormalizesNilSlices(t *testing.T) {
	content := v1alpha1.RuntimeHistoryContent{
		Revisions: []v1alpha1.RuntimeRevisionEntry{{}},
	}

	got := content.Canonical()

	assert.NotNil(t, got.Revisions)
	assert.NotNil(t, got.Revisions[0].Roles)
	assert.NotNil(t, got.Revisions[0].Issues)
	assert.NotNil(t, got.Issues)
}

func TestNewRuntimeHistoryReportUsesFixedKindAndUTCClock(t *testing.T) {
	now := time.Date(2026, time.August, 31, 11, 30, 0, 0, time.FixedZone("test", -7*60*60))

	got := v1alpha1.NewRuntimeHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RuntimeHistoryContent{},
		fixedClock{now: now},
	)

	assert.Equal(t, v1alpha1.RuntimeHistoryReportKind, got.Kind)
	assert.Equal(t, "2026-08-31T18:30:00Z", got.CollectedAt.Format(time.RFC3339))
	assert.NotNil(t, got.Content.Revisions)
	assert.NotNil(t, got.Content.Issues)
}

func TestRuntimeHistoryTableContract(t *testing.T) {
	reportValue := v1alpha1.NewRuntimeHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		runtimeHistoryContent(),
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)

	assert.Equal(t, report.Table{
		Headers: []string{"WINDOW", "REVISION", "CREATED", "ROLES", "CHECK", "LIVE", "ISSUES"},
		Rows: [][]string{
			{"P/I/2/3", "revision-a", "26-08-31T18:20Z", "AQRH", "BAD", "MATCH", "R2/G2"},
			{"P/I/2/3", "revision-b", "26-08-31T18:20Z", "H", "BAD", "DIFF", "R1/G2"},
			{"P/I/2/3", "revision-old", "26-08-31T18:10Z", "-", "?", "?", "R0/G2"},
		},
	}, reportValue.Table())

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 80, "compact output line %q", line)
	}
}

func TestRuntimeHistoryCompactTableBoundsSanitizedUnicodeByDisplayWidth(t *testing.T) {
	name := "\t\n\r\u202e" + strings.Repeat("界", 100)
	content := v1alpha1.RuntimeHistoryContent{
		Observation:  v1alpha1.HistoryObservationStateComplete,
		Completeness: v1alpha1.HistoryCompletenessRetentionBounded,
		Revisions: []v1alpha1.RuntimeRevisionEntry{{
			Revision: v1alpha1.RuntimeRevisionReference{Name: name},
		}},
	}

	table := content.Table()
	require.Len(t, table.Rows, 1)
	assert.NotContains(t, table.Rows[0][1], "\t")
	assert.NotContains(t, table.Rows[0][1], "\n")
	assert.NotContains(t, table.Rows[0][1], "\r")
	assert.NotContains(t, table.Rows[0][1], "\u202e")
	assert.Equal(t, table.Rows[0][1], printers.BoundedMiddleCell(table.Rows[0][1], 14))

	var output bytes.Buffer
	require.NoError(t, table.Write(&output))
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.Equal(t, line, printers.BoundedCell(line, 80), "display width of line %q", line)
	}
}

func TestRuntimeHistoryCompactTableDisambiguatesBeyondSafetyClip(t *testing.T) {
	prefix := strings.Repeat("a", 511)
	suffix := strings.Repeat("z", 510)
	firstName := prefix + "first-distinct-middle" + suffix
	secondName := prefix + "second-distinct-middle" + suffix
	content := v1alpha1.RuntimeHistoryContent{
		Revisions: []v1alpha1.RuntimeRevisionEntry{
			{Revision: v1alpha1.RuntimeRevisionReference{Name: firstName}},
			{Revision: v1alpha1.RuntimeRevisionReference{Name: secondName}},
		},
	}

	first := content.Table()
	second := content.Table()
	require.Len(t, first.Rows, 2)
	require.Len(t, second.Rows, 2)
	assert.Equal(t, "aa...#4a16bd72", first.Rows[0][1])
	assert.Equal(t, "aa...#7933716b", first.Rows[1][1])
	assert.Equal(t, first, second)
	for _, row := range first.Rows {
		assert.Equal(t, row[1], printers.BoundedMiddleCell(row[1], 14))
	}
}

func TestRuntimeHistoryCompactTableMapsUnknownAndEmptyStatesClosed(t *testing.T) {
	zero := time.Time{}
	tests := []struct {
		name        string
		consistency v1alpha1.RevisionConsistency
		relation    v1alpha1.RevisionRelation
		wantCheck   string
		wantLive    string
	}{
		{name: "empty", wantCheck: "-", wantLive: "-"},
		{
			name: "ambiguous live relation", consistency: v1alpha1.RevisionConsistencyConsistent,
			relation: v1alpha1.RevisionRelationAmbiguous, wantCheck: "OK", wantLive: "AMB",
		},
		{
			name: "future values", consistency: v1alpha1.RevisionConsistency("FutureConsistency"),
			relation: v1alpha1.RevisionRelation("FutureRelation"), wantCheck: "OTHER", wantLive: "OTHER",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := v1alpha1.RuntimeHistoryContent{
				Observation:    v1alpha1.HistoryObservationState("FutureObservation"),
				Completeness:   v1alpha1.HistoryCompleteness("FutureCompleteness"),
				ObservedPages:  -1,
				RequestedPages: 100,
				Revisions: []v1alpha1.RuntimeRevisionEntry{{
					Revision:       v1alpha1.RuntimeRevisionReference{Name: "revision", CreatedAt: &zero},
					Consistency:    tt.consistency,
					RelationToLive: tt.relation,
				}},
			}

			row := content.Table().Rows[0]
			assert.Equal(t, "?/?/?/99+", row[0])
			assert.Equal(t, "-", row[2])
			assert.Equal(t, tt.wantCheck, row[4])
			assert.Equal(t, tt.wantLive, row[5])
		})
	}
}

func TestRuntimeHistoryWideTablePreservesCompleteLegacyView(t *testing.T) {
	reportValue := v1alpha1.NewRuntimeHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		runtimeHistoryContent(),
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)

	assert.Equal(t, report.Table{
		Headers: []string{
			"OBSERVATION", "COMPLETENESS", "PAGES", "REVISION", "CREATED", "HASH", "ROLES", "SOURCE",
			"CONSISTENCY", "RELATION", "REVISION-ISSUES", "REPORT-ISSUES",
		},
		Rows: [][]string{
			{"Partial", "Incomplete", "2/3", "revision-a", "2026-08-31T18:20:00Z", "aaaaaaaa", "Active,Requested,Reported,History", "ServingRuntime/prod/vllm", "Inconsistent", "MatchesLive", "RevisionHashInvalid,RevisionNameMismatch", "HistoryTruncated,RevisionHashCollision(revision-b)"},
			{"Partial", "Incomplete", "2/3", "revision-b", "2026-08-31T18:20:00Z", "bbbbbbbb", "History", "ServingRuntime/prod/vllm", "Inconsistent", "DiffersFromLive", "RevisionSourceMismatch", "HistoryTruncated,RevisionHashCollision(revision-b)"},
			{"Partial", "Incomplete", "2/3", "revision-old", "2026-08-31T18:10:00Z", "-", "-", "-", "Unknown", "Unknown", "-", "HistoryTruncated,RevisionHashCollision(revision-b)"},
		},
	}, reportValue.Content.WideTable())
}

func TestRuntimeHistoryCompactTableBoundsAndDisambiguatesHostileValues(t *testing.T) {
	createdAt := time.Date(2026, time.August, 31, 18, 20, 0, 0, time.UTC)
	content := v1alpha1.RuntimeHistoryContent{
		Observation:    v1alpha1.HistoryObservationStatePartial,
		Completeness:   v1alpha1.HistoryCompletenessIncomplete,
		ObservedPages:  100,
		RequestedPages: 101,
		Revisions: []v1alpha1.RuntimeRevisionEntry{
			{
				Revision: v1alpha1.RuntimeRevisionReference{
					Name: "shared-prefix-first-distinct-middle-shared-suffix", CreatedAt: runtimeReportTime(createdAt),
				},
				Roles: []v1alpha1.RuntimeRevisionRole{
					v1alpha1.RuntimeRevisionRoleActive, v1alpha1.RuntimeRevisionRoleRequested,
					v1alpha1.RuntimeRevisionRoleReported, v1alpha1.RuntimeRevisionRoleHistory,
					v1alpha1.RuntimeRevisionRole("FutureRole"),
				},
				Consistency:    v1alpha1.RevisionConsistency("FutureConsistency"),
				RelationToLive: v1alpha1.RevisionRelation("FutureRelation"),
				Issues:         make([]v1alpha1.RuntimeIssueCode, 10),
			},
			{
				Revision: v1alpha1.RuntimeRevisionReference{
					Name: "shared-prefix-second-distinct-middle-shared-suffix", CreatedAt: runtimeReportTime(createdAt),
				},
			},
		},
		Issues: make([]v1alpha1.RuntimeIssue, 10),
	}

	table := content.Table()
	require.Len(t, table.Rows, 2)
	assert.Equal(t, "P/I/99+/99+", table.Rows[0][0])
	assert.Equal(t, "AQRH?", table.Rows[0][3])
	assert.Equal(t, "OTHER", table.Rows[0][4])
	assert.Equal(t, "OTHER", table.Rows[0][5])
	assert.Equal(t, "R9+/G9+", table.Rows[0][6])
	assert.NotEqual(t, table.Rows[0][1], table.Rows[1][1])
	assert.Contains(t, table.Rows[0][1], "#")
	assert.Contains(t, table.Rows[1][1], "#")

	var output bytes.Buffer
	require.NoError(t, table.Write(&output))
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 80, "compact output line %q", line)
	}
}

func TestRuntimeHistoryEmptyFormsHaveDiagnosticSummaryRow(t *testing.T) {
	tests := []struct {
		content v1alpha1.RuntimeHistoryContent
		want    []string
	}{
		{
			content: v1alpha1.RuntimeHistoryContent{
				Observation: v1alpha1.HistoryObservationStateNotRequested, Completeness: v1alpha1.HistoryCompletenessNotRequested,
			},
			want: []string{"N/N/0/0", "-", "-", "-", "-", "-", "R0/G0"},
		},
		{
			content: v1alpha1.RuntimeHistoryContent{
				Observation: v1alpha1.HistoryObservationStateComplete, Completeness: v1alpha1.HistoryCompletenessRetentionBounded,
				RequestedPages: 1, ObservedPages: 1,
			},
			want: []string{"C/B/1/1", "-", "-", "-", "-", "-", "R0/G0"},
		},
		{
			content: v1alpha1.RuntimeHistoryContent{
				Observation:    v1alpha1.HistoryObservationStateUnavailable,
				Completeness:   v1alpha1.HistoryCompletenessIncomplete,
				RequestedPages: 1,
				Issues:         []v1alpha1.RuntimeIssue{{Code: v1alpha1.RuntimeIssueHistoryUnavailable}},
			},
			want: []string{"U/I/0/1", "-", "-", "-", "-", "-", "R0/G1"},
		},
	}
	for _, tt := range tests {
		canonical := tt.content.Canonical()
		assert.NotNil(t, canonical.Revisions)
		assert.Equal(t, [][]string{tt.want}, canonical.Table().Rows)
	}

	assert.Equal(t, [][]string{{
		"Unavailable", "Incomplete", "0/1", "-", "-", "-", "-", "-", "-", "-", "-", "HistoryUnavailable",
	}}, tests[2].content.WideTable().Rows)
}

func TestRuntimeHistoryMachineOutputContract(t *testing.T) {
	reportValue := v1alpha1.NewRuntimeHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		runtimeHistoryMachineContent(),
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)
	tests := []struct {
		name   string
		format report.Format
		want   string
	}{
		{
			name:   "json",
			format: report.FormatJSON,
			want: "{\n" +
				`  "apiVersion": "cli.ome.io/v1alpha1",` + "\n" +
				`  "kind": "RuntimeHistoryReport",` + "\n" +
				`  "metadata": {` + "\n" +
				`    "namespace": "prod",` + "\n" +
				`    "name": "chat"` + "\n" +
				"  },\n" +
				`  "collectedAt": "2026-08-31T18:30:00Z",` + "\n" +
				`  "sources": [],` + "\n" +
				`  "content": {` + "\n" +
				`    "runtime": {` + "\n" +
				`      "apiVersion": "ome.io/v1beta1",` + "\n" +
				`      "kind": "ServingRuntime",` + "\n" +
				`      "namespace": "prod",` + "\n" +
				`      "name": "vllm"` + "\n" +
				"    },\n" +
				`    "observation": "Complete",` + "\n" +
				`    "completeness": "RetentionBounded",` + "\n" +
				`    "requestedPages": 2,` + "\n" +
				`    "observedPages": 2,` + "\n" +
				`    "revisions": [` + "\n" +
				"      {\n" +
				`        "revision": {` + "\n" +
				`          "namespace": "ome",` + "\n" +
				`          "name": "revision-a",` + "\n" +
				`          "createdAt": "2026-08-31T18:20:00Z"` + "\n" +
				"        },\n" +
				`        "source": {` + "\n" +
				`          "apiVersion": "ome.io/v1beta1",` + "\n" +
				`          "kind": "ServingRuntime",` + "\n" +
				`          "namespace": "prod",` + "\n" +
				`          "name": "vllm"` + "\n" +
				"        },\n" +
				`        "hash": "aaaaaaaa",` + "\n" +
				`        "roles": [` + "\n" +
				`          "Active",` + "\n" +
				`          "History"` + "\n" +
				"        ],\n" +
				`        "consistency": "Consistent",` + "\n" +
				`        "relationToLive": "MatchesLive",` + "\n" +
				`        "issues": []` + "\n" +
				"      }\n" +
				"    ],\n" +
				`    "issues": []` + "\n" +
				"  },\n" +
				`  "warnings": []` + "\n" +
				"}\n",
		},
		{
			name:   "yaml",
			format: report.FormatYAML,
			want: "apiVersion: cli.ome.io/v1alpha1\n" +
				"collectedAt: \"2026-08-31T18:30:00Z\"\n" +
				"content:\n" +
				"  completeness: RetentionBounded\n" +
				"  issues: []\n" +
				"  observation: Complete\n" +
				"  observedPages: 2\n" +
				"  requestedPages: 2\n" +
				"  revisions:\n" +
				"  - consistency: Consistent\n" +
				"    hash: aaaaaaaa\n" +
				"    issues: []\n" +
				"    relationToLive: MatchesLive\n" +
				"    revision:\n" +
				"      createdAt: \"2026-08-31T18:20:00Z\"\n" +
				"      name: revision-a\n" +
				"      namespace: ome\n" +
				"    roles:\n" +
				"    - Active\n" +
				"    - History\n" +
				"    source:\n" +
				"      apiVersion: ome.io/v1beta1\n" +
				"      kind: ServingRuntime\n" +
				"      name: vllm\n" +
				"      namespace: prod\n" +
				"  runtime:\n" +
				"    apiVersion: ome.io/v1beta1\n" +
				"    kind: ServingRuntime\n" +
				"    name: vllm\n" +
				"    namespace: prod\n" +
				"kind: RuntimeHistoryReport\n" +
				"metadata:\n" +
				"  name: chat\n" +
				"  namespace: prod\n" +
				"sources: []\n" +
				"warnings: []\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			require.NoError(t, report.Write(&output, tt.format, reportValue))
			assert.Equal(t, tt.want, output.String())
		})
	}
}

func TestRuntimeHistorySchemaIsStrictlyAllowlisted(t *testing.T) {
	assertRuntimeReportSchema(t, reflect.TypeOf(v1alpha1.RuntimeEnvelope[v1alpha1.RuntimeHistoryContent]{}), map[reflect.Type]bool{})
}

func runtimeHistoryContent() v1alpha1.RuntimeHistoryContent {
	location := time.FixedZone("test", -7*60*60)
	newest := time.Date(2026, time.August, 31, 11, 20, 0, 0, location)
	older := time.Date(2026, time.August, 31, 11, 10, 0, 0, location)
	source := runtimeObject(v1alpha1.RuntimeKindServingRuntime, "prod", "vllm")
	return v1alpha1.RuntimeHistoryContent{
		Runtime:        &source,
		Observation:    v1alpha1.HistoryObservationStatePartial,
		Completeness:   v1alpha1.HistoryCompletenessIncomplete,
		RequestedPages: 3,
		ObservedPages:  2,
		Revisions: []v1alpha1.RuntimeRevisionEntry{
			{
				Revision:    v1alpha1.RuntimeRevisionReference{Namespace: "ome", Name: "revision-old", CreatedAt: runtimeReportTime(older)},
				Consistency: v1alpha1.RevisionConsistencyUnknown, RelationToLive: v1alpha1.RevisionRelationUnknown,
			},
			{
				Revision: v1alpha1.RuntimeRevisionReference{Namespace: "ome", Name: "revision-b", CreatedAt: runtimeReportTime(newest)},
				Source:   &source, Hash: "bbbbbbbb", Roles: []v1alpha1.RuntimeRevisionRole{v1alpha1.RuntimeRevisionRoleHistory},
				Consistency: v1alpha1.RevisionConsistencyInconsistent, RelationToLive: v1alpha1.RevisionRelationDiffersFromLive,
				Issues: []v1alpha1.RuntimeIssueCode{v1alpha1.RuntimeIssueRevisionSourceMismatch},
			},
			{
				Revision: v1alpha1.RuntimeRevisionReference{Namespace: "ome", Name: "revision-a", CreatedAt: runtimeReportTime(newest)},
				Source:   &source, Hash: "aaaaaaaa",
				Roles: []v1alpha1.RuntimeRevisionRole{
					v1alpha1.RuntimeRevisionRoleHistory, v1alpha1.RuntimeRevisionRoleReported,
					v1alpha1.RuntimeRevisionRoleRequested, v1alpha1.RuntimeRevisionRoleActive,
				},
				Consistency: v1alpha1.RevisionConsistencyInconsistent, RelationToLive: v1alpha1.RevisionRelationMatchesLive,
				Issues: []v1alpha1.RuntimeIssueCode{
					v1alpha1.RuntimeIssueRevisionNameMismatch, v1alpha1.RuntimeIssueRevisionHashInvalid,
				},
			},
		},
		Issues: []v1alpha1.RuntimeIssue{
			{Code: v1alpha1.RuntimeIssueRevisionHashCollision, Revision: "revision-b"},
			{Code: v1alpha1.RuntimeIssueHistoryTruncated},
		},
	}
}

func runtimeHistoryMachineContent() v1alpha1.RuntimeHistoryContent {
	source := runtimeObject(v1alpha1.RuntimeKindServingRuntime, "prod", "vllm")
	return v1alpha1.RuntimeHistoryContent{
		Runtime:        &source,
		Observation:    v1alpha1.HistoryObservationStateComplete,
		Completeness:   v1alpha1.HistoryCompletenessRetentionBounded,
		RequestedPages: 2,
		ObservedPages:  2,
		Revisions: []v1alpha1.RuntimeRevisionEntry{{
			Revision: v1alpha1.RuntimeRevisionReference{
				Namespace: "ome", Name: "revision-a", CreatedAt: runtimeReportTime(time.Date(2026, time.August, 31, 18, 20, 0, 0, time.UTC)),
			},
			Source: &source, Hash: "aaaaaaaa",
			Roles:          []v1alpha1.RuntimeRevisionRole{v1alpha1.RuntimeRevisionRoleHistory, v1alpha1.RuntimeRevisionRoleActive},
			Consistency:    v1alpha1.RevisionConsistencyConsistent,
			RelationToLive: v1alpha1.RevisionRelationMatchesLive,
			Issues:         nil,
		}},
		Issues: nil,
	}
}

func revisionNames(revisions []v1alpha1.RuntimeRevisionEntry) []string {
	result := make([]string, 0, len(revisions))
	for _, revision := range revisions {
		result = append(result, revision.Revision.Name)
	}
	return result
}

func runtimeSourceCopy(
	source v1alpha1.RuntimeObjectReference,
	mutate func(*v1alpha1.RuntimeObjectReference),
) *v1alpha1.RuntimeObjectReference {
	mutate(&source)
	return &source
}
