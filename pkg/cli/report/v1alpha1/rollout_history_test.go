package v1alpha1_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestNewRolloutHistoryReportBuildsTypedEmptyContract(t *testing.T) {
	clock := v1alpha1.ClockFunc(func() time.Time {
		return time.Date(2026, time.September, 14, 18, 30, 0, 0, time.FixedZone("fixture", -7*60*60))
	})

	reportValue := v1alpha1.NewRolloutHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutHistoryContent{},
		clock,
	)

	assert.Equal(t, v1alpha1.APIVersion, reportValue.APIVersion)
	assert.Equal(t, v1alpha1.RolloutHistoryReportKind, reportValue.Kind)
	assert.Equal(t, time.Date(2026, time.September, 15, 1, 30, 0, 0, time.UTC), reportValue.CollectedAt)
	assert.Equal(t, v1alpha1.Metadata{Namespace: "prod", Name: "chat"}, reportValue.Metadata)
	require.NotNil(t, reportValue.Sources)
	require.NotNil(t, reportValue.Content.Runs)
	require.NotNil(t, reportValue.Content.Provenance)
	require.NotNil(t, reportValue.Content.Revisions)
	require.NotNil(t, reportValue.Content.StatusIssues)
	require.NotNil(t, reportValue.Content.Issues)
	require.NotNil(t, reportValue.Warnings)
}

func TestRolloutHistoryCanonicalIsDeterministicDeepAndClosed(t *testing.T) {
	opened := time.Date(2026, time.September, 14, 18, 0, 0, 0, time.FixedZone("fixture", 2*60*60))
	closed := opened.Add(time.Hour)
	lastOpened := opened.Add(-2 * time.Hour)
	lastClosed := opened.Add(-time.Hour)
	groupOne := 1
	groupZero := 0
	reportValue := v1alpha1.RolloutHistoryReport{
		Metadata:    v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		CollectedAt: opened,
		Sources: []v1alpha1.RolloutHistorySourceReference{
			{Kind: v1alpha1.RolloutSourceInferenceService, Namespace: "prod", Name: "z", Evidence: v1alpha1.EvidenceObserved},
			{Kind: v1alpha1.RolloutSourceInferenceService, Namespace: "prod", Name: "chat", Evidence: v1alpha1.EvidenceReported},
		},
		Content: v1alpha1.RolloutHistoryContent{
			Summary: v1alpha1.RolloutHistorySummary{
				State: v1alpha1.RolloutHistoryStateReported, Completeness: v1alpha1.RolloutHistoryRetentionBounded,
				CurrentState: v1alpha1.RolloutStateInProgress, CurrentEvidence: v1alpha1.EvidenceReported,
				CurrentEpoch: v1alpha1.RolloutEpochUnverifiable,
			},
			Runs: []v1alpha1.RolloutHistoryRun{
				{Slot: v1alpha1.RolloutHistoryRunLast, Outcome: v1alpha1.RolloutHistoryRunCompleted, OpenedAt: &lastOpened, ClosedAt: &lastClosed},
				{Slot: v1alpha1.RolloutHistoryRunActive, Outcome: v1alpha1.RolloutHistoryRunActiveState,
					RunID: "chat-0123456789ab", OpenedAt: &opened, PinnedAt: &closed,
					Targets: []v1alpha1.RolloutHistoryTarget{
						{Component: v1alpha1.RuntimeComponentRouter, RevisionHash: "bbbbbbbb", Evidence: v1alpha1.EvidenceReported},
						{Component: v1alpha1.RuntimeComponentEngine, RevisionHash: "aaaaaaaa", Evidence: v1alpha1.EvidenceReported},
					}},
			},
			Provenance: []v1alpha1.RolloutHistoryProvenance{
				{View: v1alpha1.RolloutHistoryViewCurrent, Group: 1, Source: v1alpha1.RolloutPlanSourcePolicy,
					PortableDigest: "rp1:bbbbbbbbbbbb", Policy: &v1alpha1.RolloutPolicyReference{
						Kind: "RolloutPolicy", Name: "slow", Progression: "canary", Evidence: v1alpha1.EvidenceReported,
					}},
				{View: v1alpha1.RolloutHistoryViewActive, Group: 0, Source: v1alpha1.RolloutPlanSourceInline,
					PortableDigest: "rp1:aaaaaaaaaaaa"},
			},
			Revisions: []v1alpha1.RolloutHistoryRevision{
				{Component: v1alpha1.RuntimeComponentRouter, Role: v1alpha1.RolloutRevisionPrevious,
					RevisionHash: "dddddddd", Phase: v1alpha1.RolloutPhasePaused},
				{Component: v1alpha1.RuntimeComponentEngine, Role: v1alpha1.RolloutRevisionCurrent,
					RevisionHash: "cccccccc", Phase: v1alpha1.RolloutPhaseStable},
			},
			StatusIssues: []v1alpha1.RolloutIssue{
				{Code: v1alpha1.RolloutIssueTrafficInvalid, Group: &groupOne, Component: v1alpha1.RuntimeComponentRouter},
				{Code: v1alpha1.RolloutIssueSpecMalformed, Group: &groupZero},
			},
			Issues: []v1alpha1.RolloutHistoryIssue{
				{Code: v1alpha1.RolloutHistoryIssueCurrentResolutionMalformed, View: v1alpha1.RolloutHistoryViewCurrent, Group: &groupOne},
				{Code: v1alpha1.RolloutHistoryIssueActiveRunMalformed, View: v1alpha1.RolloutHistoryViewActive, Group: &groupZero},
			},
		},
		Warnings: []v1alpha1.RolloutWarning{{Code: v1alpha1.WarningTruncated}, {Code: v1alpha1.WarningPartialData}},
	}

	canonical := reportValue.Canonical()

	assert.Equal(t, v1alpha1.APIVersion, canonical.APIVersion)
	assert.Equal(t, v1alpha1.RolloutHistoryReportKind, canonical.Kind)
	assert.Equal(t, time.UTC, canonical.CollectedAt.Location())
	assert.Equal(t, v1alpha1.RolloutHistoryRunActive, canonical.Content.Runs[0].Slot)
	assert.Equal(t, v1alpha1.RuntimeComponentEngine, canonical.Content.Runs[0].Targets[0].Component)
	assert.Equal(t, v1alpha1.RolloutHistoryViewActive, canonical.Content.Provenance[0].View)
	assert.Equal(t, v1alpha1.RuntimeComponentEngine, canonical.Content.Revisions[0].Component)
	assert.Equal(t, v1alpha1.RolloutIssueSpecMalformed, canonical.Content.StatusIssues[0].Code)
	assert.Equal(t, v1alpha1.RolloutHistoryIssueActiveRunMalformed, canonical.Content.Issues[0].Code)
	assert.Equal(t, v1alpha1.WarningPartialData, canonical.Warnings[0].Code)
	assert.Equal(t, 1, canonical.Content.Summary.ActiveRuns)
	assert.Equal(t, 1, canonical.Content.Summary.RetainedRuns)
	assert.Equal(t, 2, canonical.Content.Summary.Revisions)

	canonical.Content.Runs[0].Targets[0].RevisionHash = "ffffffff"
	canonical.Content.Runs[0].OpenedAt = nil
	canonical.Content.Provenance[1].Policy.Name = "mutated"
	canonical.Content.StatusIssues[0].Group = nil
	canonical.Content.Issues[0].Group = nil
	assert.Equal(t, "aaaaaaaa", reportValue.Content.Runs[1].Targets[1].RevisionHash)
	require.NotNil(t, reportValue.Content.Runs[1].OpenedAt)
	assert.Equal(t, "slow", reportValue.Content.Provenance[0].Policy.Name)
	require.NotNil(t, reportValue.Content.StatusIssues[1].Group)
	require.NotNil(t, reportValue.Content.Issues[1].Group)
}

func TestRolloutHistoryCanonicalRejectsHostileOpenFields(t *testing.T) {
	secret := "SECRET_token_header_query_template"
	zero := time.Time{}
	reportValue := v1alpha1.RolloutHistoryReport{
		Metadata: v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		Content: v1alpha1.RolloutHistoryContent{
			Summary: v1alpha1.RolloutHistorySummary{
				State: v1alpha1.RolloutHistoryState(secret), Completeness: v1alpha1.RolloutHistoryCompleteness(secret), CurrentState: v1alpha1.RolloutState(secret),
				CurrentEvidence: v1alpha1.EvidenceLevel(secret), CurrentEpoch: v1alpha1.RolloutEpochState(secret),
			},
			Runs: []v1alpha1.RolloutHistoryRun{{
				Slot: v1alpha1.RolloutHistoryRunSlot(secret), Outcome: v1alpha1.RolloutHistoryRunOutcome(secret), RunID: secret, OpenedAt: &zero,
				Targets: []v1alpha1.RolloutHistoryTarget{{Component: v1alpha1.RuntimeComponentType(secret), RevisionHash: secret}},
			}},
			Provenance: []v1alpha1.RolloutHistoryProvenance{{
				View: v1alpha1.RolloutHistoryView(secret), Group: -1, Source: v1alpha1.RolloutPlanSource(secret), PortableDigest: secret,
				DigestEvidence: v1alpha1.EvidenceLevel(secret),
				Policy:         &v1alpha1.RolloutPolicyReference{Kind: secret, Name: secret, Progression: secret, Digest: secret, Evidence: v1alpha1.EvidenceLevel(secret)},
			}},
			Revisions: []v1alpha1.RolloutHistoryRevision{{
				Component: v1alpha1.RuntimeComponentType(secret), Role: v1alpha1.RolloutRevisionRole(secret), RevisionHash: secret, Phase: v1alpha1.RolloutPhase(secret),
			}},
			StatusIssues: []v1alpha1.RolloutIssue{{Code: v1alpha1.RolloutIssueCode(secret), Component: v1alpha1.RuntimeComponentType(secret)}},
			Issues:       []v1alpha1.RolloutHistoryIssue{{Code: v1alpha1.RolloutHistoryIssueCode(secret), View: v1alpha1.RolloutHistoryView(secret), Component: v1alpha1.RuntimeComponentType(secret)}},
		},
	}

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatJSON, reportValue))
	assert.NotContains(t, output.String(), secret)
	assert.NotContains(t, output.String(), "token")
	assert.Contains(t, output.String(), `"state": "Unknown"`)
	assert.Contains(t, output.String(), `"runs": []`)
}

func TestRolloutHistoryCanonicalRetainsDigestlessCurrentResolution(t *testing.T) {
	reportValue := v1alpha1.NewRolloutHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutHistoryContent{Provenance: []v1alpha1.RolloutHistoryProvenance{{
			View: v1alpha1.RolloutHistoryViewCurrent, Group: 0,
			Source: v1alpha1.RolloutPlanSourceInline,
		}}},
		v1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }),
	)

	require.Len(t, reportValue.Content.Provenance, 1)
	assert.Empty(t, reportValue.Content.Provenance[0].PortableDigest)
	assert.Equal(t, v1alpha1.EvidenceUnavailable, reportValue.Content.Provenance[0].DigestEvidence)
	assert.Equal(t, "-", reportValue.Table().Rows[0][3])
}

func TestRolloutHistoryCanonicalFailsClosedOnCrossSlotChronology(t *testing.T) {
	activeOpened := time.Date(2026, 9, 14, 18, 0, 0, 0, time.UTC)
	activePinned := activeOpened.Add(30 * time.Minute)
	lastOpened := activeOpened.Add(-time.Hour)
	lastClosed := activeOpened.Add(time.Hour)
	reportValue := v1alpha1.NewRolloutHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutHistoryContent{Runs: []v1alpha1.RolloutHistoryRun{
			{Slot: v1alpha1.RolloutHistoryRunActive, Outcome: v1alpha1.RolloutHistoryRunActiveState,
				RunID: "chat-0123456789ab", OpenedAt: &activeOpened, PinnedAt: &activePinned,
				GroupCount: 1, Targets: []v1alpha1.RolloutHistoryTarget{}},
			{Slot: v1alpha1.RolloutHistoryRunLast, Outcome: v1alpha1.RolloutHistoryRunCompleted,
				OpenedAt: &lastOpened, ClosedAt: &lastClosed, GroupCount: 1,
				Targets: []v1alpha1.RolloutHistoryTarget{}},
		}, Provenance: []v1alpha1.RolloutHistoryProvenance{{
			View: v1alpha1.RolloutHistoryViewLast, Group: 0, Source: v1alpha1.RolloutPlanSourceInline,
			PortableDigest: "rp1:aaaaaaaaaaaa",
		}}}, v1alpha1.ClockFunc(func() time.Time { return activePinned }),
	)

	require.Len(t, reportValue.Content.Runs, 1)
	assert.Equal(t, v1alpha1.RolloutHistoryRunActive, reportValue.Content.Runs[0].Slot)
	assert.Empty(t, reportValue.Content.Provenance)
	assert.Equal(t, v1alpha1.RolloutHistoryStatePartial, reportValue.Content.Summary.State)
	assert.Contains(t, reportValue.Content.Issues, v1alpha1.RolloutHistoryIssue{
		Code: v1alpha1.RolloutHistoryIssueRunChronologyMalformed,
		View: v1alpha1.RolloutHistoryViewLast,
	})
	assert.Contains(t, reportValue.Warnings, v1alpha1.RolloutWarning{Code: v1alpha1.WarningPartialData})
}

func TestRolloutHistoryCompactTableIsCompleteAndAtMostEightyColumns(t *testing.T) {
	reportValue := rolloutHistoryReportFixture()
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))

	assert.Equal(t, "TYPE     STATE        COMP     IDENT          TIME          DETAIL       ISS\n"+
		"WINDOW   Reported     -        -              -             bounded      0\n"+
		"CURR     InProgress   -        Reported       -             Unverified   0\n"+
		"ACTIVE   Active       -        0123456789ab   09-14T18:00   G:2 T:2      0\n"+
		"TARGET   Reported     engine   aaaaaaaa       -             run-target   0\n"+
		"TARGET   Reported     router   bbbbbbbb       -             run-target   0\n"+
		"LAST     Completed    -        -              09-14T17:00   G:1          0\n"+
		"PROV-A   Inline       g0       aaaaaaaaaaaa   09-14T18:30   -            0\n"+
		"PROV-C   Policy       g1       bbbbbbbbbbbb   -             slow         0\n"+
		"REV      Stable       engine   cccccccc       -             Current      0\n"+
		"REV      RolledBack   router   dddddddd       -             Previous     0\n", output.String())
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, line)
	}
}

func TestRolloutHistoryCompactTableBoundsMaximumFieldDomains(t *testing.T) {
	reportValue := rolloutHistoryReportFixture()
	reportValue.Content.Summary.CurrentState = v1alpha1.RolloutStateNotConfigured
	reportValue.Content.Summary.CurrentEvidence = v1alpha1.EvidenceUnavailable
	reportValue.Content.Summary.CurrentEpoch = v1alpha1.RolloutEpochNotApplicable
	reportValue.Content.Runs[0].Targets[0].RevisionHash = ""
	reportValue.Content.Provenance[1].Policy.Name = strings.Repeat("a", 253)
	for range 105 {
		reportValue.Content.StatusIssues = append(reportValue.Content.StatusIssues, v1alpha1.RolloutIssue{
			Code: v1alpha1.RolloutIssueStatusMalformed,
		})
	}
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))

	assert.Contains(t, output.String(), "a#")
	assert.Contains(t, output.String(), "99+")
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 80, line)
	}
}

func TestRolloutHistoryWideTableShowsOnlyFullAllowlistedEvidence(t *testing.T) {
	table := rolloutHistoryReportFixture().WideTable()
	var output bytes.Buffer
	require.NoError(t, table.Write(&output))

	assert.Contains(t, output.String(), "chat-0123456789ab")
	assert.Contains(t, output.String(), "rp1:aaaaaaaaaaaa")
	assert.Contains(t, output.String(), "RolloutPolicy/slow")
	assert.Contains(t, output.String(), "RetentionBounded")
	assert.NotContains(t, output.String(), "serverAddress")
	assert.NotContains(t, output.String(), "query")
	assert.Equal(t, []string{
		"ROW", "NAMESPACE", "NAME", "COLLECTED-AT", "COMPLETENESS", "WINDOW-STATE",
		"CURRENT-STATE", "CURRENT-EVIDENCE", "CURRENT-EPOCH", "ACTIVE-RUNS",
		"RETAINED-RUNS", "REVISIONS", "SOURCE-KIND", "SOURCE-NAMESPACE", "SOURCE-NAME",
		"SOURCE-GENERATION", "SOURCE-EVIDENCE", "SOURCE-COLLECTED-AT", "RECORD", "OUTCOME",
		"RUN-ID", "GROUP-COUNT", "COMPONENT", "REVISION", "REVISION-EVIDENCE", "ROLE",
		"PHASE", "OPENED-AT", "PINNED-AT", "CLOSED-AT", "OBSERVED-AT", "VIEW", "GROUP",
		"SOURCE", "POLICY", "POLICY-PROGRESSION", "POLICY-GENERATION", "POLICY-EVIDENCE",
		"POLICY-DIGEST", "DIGEST", "DIGEST-EVIDENCE", "SHADOWED-POLICY", "SHADOWED-PROGRESSION",
		"SHADOWED-GENERATION", "SHADOWED-EVIDENCE", "SHADOWED-DIGEST", "ISSUE-CODE", "ISSUE-VIEW",
		"ISSUE-GROUP", "ISSUE-COMPONENT", "WARNING",
	}, table.Headers)
	assert.Equal(t, "Summary", table.Rows[0][0])
	assert.Equal(t, "Reported", table.Rows[0][5])
	assert.Equal(t, "InProgress", table.Rows[0][6])
	assert.Equal(t, "Reported", table.Rows[0][7])
	assert.Equal(t, "Unverifiable", table.Rows[0][8])
	assert.Equal(t, "1", table.Rows[0][9])
	assert.Equal(t, "1", table.Rows[0][10])
	assert.Equal(t, "2", table.Rows[0][11])
}

func TestRolloutHistoryWideTableDoesNotInventUnavailablePolicyGeneration(t *testing.T) {
	table := rolloutHistoryReportFixture().WideTable()
	var current []string
	viewColumn := historyWideColumn(t, table, "VIEW")
	for _, row := range table.Rows {
		if row[viewColumn] == string(v1alpha1.RolloutHistoryViewCurrent) {
			current = row
			break
		}
	}
	require.Len(t, current, len(table.Headers))
	assert.Equal(t, "-", current[historyWideColumn(t, table, "POLICY-GENERATION")])
}

func TestRolloutHistoryWideTableUsesDistinctPinnedAndObservedTimes(t *testing.T) {
	table := rolloutHistoryReportFixture().WideTable()
	var active, provenance []string
	recordColumn := historyWideColumn(t, table, "RECORD")
	viewColumn := historyWideColumn(t, table, "VIEW")
	for _, row := range table.Rows {
		switch {
		case row[0] == "Run" && row[recordColumn] == string(v1alpha1.RolloutHistoryRunActive):
			active = row
		case row[0] == "Provenance" && row[viewColumn] == string(v1alpha1.RolloutHistoryViewActive):
			provenance = row
		}
	}
	require.Len(t, active, len(table.Headers))
	require.Len(t, provenance, len(table.Headers))
	assert.Equal(t, "2026-09-14T18:30:00Z", active[historyWideColumn(t, table, "PINNED-AT")])
	assert.Equal(t, "-", active[historyWideColumn(t, table, "OBSERVED-AT")])
	assert.Equal(t, "-", provenance[historyWideColumn(t, table, "PINNED-AT")])
	assert.Equal(t, "2026-09-14T18:30:00Z", provenance[historyWideColumn(t, table, "OBSERVED-AT")])
}

func TestRolloutHistoryWideTableExactGoldens(t *testing.T) {
	group := 1
	partial := rolloutHistoryReportFixture()
	partial.Sources = []v1alpha1.RolloutHistorySourceReference{{
		Kind: v1alpha1.RolloutSourceInferenceService, Namespace: "prod", Name: "chat",
		Generation: 7, Evidence: v1alpha1.EvidenceObserved, CollectedAt: partial.CollectedAt,
	}}
	partial.Content.Summary.State = v1alpha1.RolloutHistoryStatePartial
	partial.Content.Provenance[1].Policy.Generation = 4
	partial.Content.Provenance[1].Policy.Digest = "rp1:bbbbbbbbbbbb"
	partial.Content.Provenance = append(partial.Content.Provenance, v1alpha1.RolloutHistoryProvenance{
		View: v1alpha1.RolloutHistoryViewCurrent, Group: 2,
		Source: v1alpha1.RolloutPlanSourceInline, PortableDigest: "rp1:ffffffffffff",
		DigestEvidence: v1alpha1.EvidenceComputed,
		ShadowedPolicy: &v1alpha1.RolloutPolicyReference{
			Kind: "RolloutPolicy", Name: "shadow", Progression: "blueGreen", Generation: 5,
			Digest: "rp1:eeeeeeeeeeee", Evidence: v1alpha1.EvidenceReported,
		},
	})
	partial.Content.StatusIssues = []v1alpha1.RolloutIssue{{
		Code: v1alpha1.RolloutIssueTrafficInvalid, Group: &group,
		Component: v1alpha1.RuntimeComponentRouter,
	}}
	partial.Content.Issues = []v1alpha1.RolloutHistoryIssue{{
		Code: v1alpha1.RolloutHistoryIssueCurrentResolutionMalformed,
		View: v1alpha1.RolloutHistoryViewCurrent, Group: &group,
		Component: v1alpha1.RuntimeComponentRouter,
	}}
	partial.Warnings = []v1alpha1.RolloutWarning{{Code: v1alpha1.WarningPartialData}}

	empty := v1alpha1.NewRolloutHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutHistoryContent{Summary: v1alpha1.RolloutHistorySummary{
			State: v1alpha1.RolloutHistoryStateEmpty, CurrentState: v1alpha1.RolloutStateNotConfigured,
			CurrentEvidence: v1alpha1.EvidenceDeclared, CurrentEpoch: v1alpha1.RolloutEpochNotApplicable,
		}}, v1alpha1.ClockFunc(func() time.Time { return time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC) }),
	)
	unavailable := empty
	unavailable.Content.Summary.State = v1alpha1.RolloutHistoryStateUnavailable
	unavailable.Content.Issues = []v1alpha1.RolloutHistoryIssue{{Code: v1alpha1.RolloutHistoryIssueRunStatusUnavailable}}
	unavailable.Warnings = []v1alpha1.RolloutWarning{{Code: v1alpha1.WarningSourceUnavailable}}

	tests := []struct {
		name  string
		value v1alpha1.RolloutHistoryReport
		want  string
	}{
		{name: "empty", value: empty, want: "Summary | NAMESPACE=prod | NAME=chat | COLLECTED-AT=2026-09-14T20:00:00Z | COMPLETENESS=RetentionBounded | WINDOW-STATE=Empty | CURRENT-STATE=NotConfigured | CURRENT-EVIDENCE=Declared | CURRENT-EPOCH=NotApplicable | ACTIVE-RUNS=0 | RETAINED-RUNS=0 | REVISIONS=0\n"},
		{name: "unavailable", value: unavailable, want: "Summary | NAMESPACE=prod | NAME=chat | COLLECTED-AT=2026-09-14T20:00:00Z | COMPLETENESS=RetentionBounded | WINDOW-STATE=Unavailable | CURRENT-STATE=NotConfigured | CURRENT-EVIDENCE=Declared | CURRENT-EPOCH=NotApplicable | ACTIVE-RUNS=0 | RETAINED-RUNS=0 | REVISIONS=0\n" +
			"HistoryIssue | ISSUE-CODE=RunStatusUnavailable\n" +
			"Warning | WARNING=SourceUnavailable\n"},
		{name: "partial", value: partial, want: "Summary | NAMESPACE=prod | NAME=chat | COLLECTED-AT=2026-09-14T19:00:00Z | COMPLETENESS=RetentionBounded | WINDOW-STATE=Partial | CURRENT-STATE=InProgress | CURRENT-EVIDENCE=Reported | CURRENT-EPOCH=Unverifiable | ACTIVE-RUNS=1 | RETAINED-RUNS=1 | REVISIONS=2\n" +
			"Source | SOURCE-KIND=InferenceService | SOURCE-NAMESPACE=prod | SOURCE-NAME=chat | SOURCE-GENERATION=7 | SOURCE-EVIDENCE=Observed | SOURCE-COLLECTED-AT=2026-09-14T19:00:00Z\n" +
			"Run | RECORD=Active | OUTCOME=Active | RUN-ID=chat-0123456789ab | GROUP-COUNT=2 | OPENED-AT=2026-09-14T18:00:00Z | PINNED-AT=2026-09-14T18:30:00Z\n" +
			"Target | RECORD=Active | COMPONENT=engine | REVISION=aaaaaaaa | REVISION-EVIDENCE=Reported | ROLE=RunTarget\n" +
			"Target | RECORD=Active | COMPONENT=router | REVISION=bbbbbbbb | REVISION-EVIDENCE=Reported | ROLE=RunTarget\n" +
			"Run | RECORD=Last | OUTCOME=Completed | GROUP-COUNT=1 | OPENED-AT=2026-09-14T16:00:00Z | CLOSED-AT=2026-09-14T17:00:00Z\n" +
			"Provenance | OBSERVED-AT=2026-09-14T18:30:00Z | VIEW=ActiveRun | GROUP=0 | SOURCE=Inline | DIGEST=rp1:aaaaaaaaaaaa | DIGEST-EVIDENCE=Computed\n" +
			"Provenance | VIEW=Current | GROUP=1 | SOURCE=Policy | POLICY=RolloutPolicy/slow | POLICY-PROGRESSION=canary | POLICY-GENERATION=4 | POLICY-EVIDENCE=Reported | POLICY-DIGEST=rp1:bbbbbbbbbbbb | DIGEST=rp1:bbbbbbbbbbbb | DIGEST-EVIDENCE=Reported\n" +
			"Provenance | VIEW=Current | GROUP=2 | SOURCE=Inline | DIGEST=rp1:ffffffffffff | DIGEST-EVIDENCE=Computed | SHADOWED-POLICY=RolloutPolicy/shadow | SHADOWED-PROGRESSION=blueGreen | SHADOWED-GENERATION=5 | SHADOWED-EVIDENCE=Reported | SHADOWED-DIGEST=rp1:eeeeeeeeeeee\n" +
			"Revision | RECORD=CurrentStatus | COMPONENT=engine | REVISION=cccccccc | REVISION-EVIDENCE=Reported | ROLE=Current | PHASE=Stable\n" +
			"Revision | RECORD=CurrentStatus | COMPONENT=router | REVISION=dddddddd | REVISION-EVIDENCE=Reported | ROLE=Previous | PHASE=RolledBack\n" +
			"StatusIssue | ISSUE-CODE=TrafficInvalid | ISSUE-GROUP=1 | ISSUE-COMPONENT=router\n" +
			"HistoryIssue | ISSUE-CODE=CurrentResolutionMalformed | ISSUE-VIEW=Current | ISSUE-GROUP=1 | ISSUE-COMPONENT=router\n" +
			"Warning | WARNING=PartialData\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, rolloutHistoryWideSnapshot(tt.value.WideTable()))
		})
	}
}

func rolloutHistoryWideSnapshot(table report.Table) string {
	var output strings.Builder
	for _, row := range table.Rows {
		values := []string{row[0]}
		for column := 1; column < len(table.Headers); column++ {
			if row[column] != "-" {
				values = append(values, table.Headers[column]+"="+row[column])
			}
		}
		output.WriteString(strings.Join(values, " | "))
		output.WriteByte('\n')
	}
	return output.String()
}

func historyWideColumn(t *testing.T, table report.Table, name string) int {
	t.Helper()
	for index, header := range table.Headers {
		if header == name {
			return index
		}
	}
	require.FailNow(t, "wide column missing", name)
	return -1
}

func rolloutHistoryReportFixture() v1alpha1.RolloutHistoryReport {
	opened := time.Date(2026, time.September, 14, 18, 0, 0, 0, time.UTC)
	pinned := opened.Add(30 * time.Minute)
	lastOpened := opened.Add(-2 * time.Hour)
	lastClosed := opened.Add(-time.Hour)
	collected := opened.Add(time.Hour)
	return v1alpha1.NewRolloutHistoryReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutHistoryContent{
			Summary: v1alpha1.RolloutHistorySummary{
				State: v1alpha1.RolloutHistoryStateReported, Completeness: v1alpha1.RolloutHistoryRetentionBounded,
				CurrentState: v1alpha1.RolloutStateInProgress, CurrentEvidence: v1alpha1.EvidenceReported,
				CurrentEpoch: v1alpha1.RolloutEpochUnverifiable,
			},
			Runs: []v1alpha1.RolloutHistoryRun{
				{Slot: v1alpha1.RolloutHistoryRunActive, Outcome: v1alpha1.RolloutHistoryRunActiveState,
					RunID: "chat-0123456789ab", OpenedAt: &opened, PinnedAt: &pinned,
					Targets: []v1alpha1.RolloutHistoryTarget{
						{Component: v1alpha1.RuntimeComponentEngine, RevisionHash: "aaaaaaaa", Evidence: v1alpha1.EvidenceReported},
						{Component: v1alpha1.RuntimeComponentRouter, RevisionHash: "bbbbbbbb", Evidence: v1alpha1.EvidenceReported},
					}, GroupCount: 2},
				{Slot: v1alpha1.RolloutHistoryRunLast, Outcome: v1alpha1.RolloutHistoryRunCompleted,
					OpenedAt: &lastOpened, ClosedAt: &lastClosed, GroupCount: 1},
			},
			Provenance: []v1alpha1.RolloutHistoryProvenance{
				{View: v1alpha1.RolloutHistoryViewActive, Group: 0, Source: v1alpha1.RolloutPlanSourceInline,
					PortableDigest: "rp1:aaaaaaaaaaaa", DigestEvidence: v1alpha1.EvidenceComputed, ObservedAt: &pinned},
				{View: v1alpha1.RolloutHistoryViewCurrent, Group: 1, Source: v1alpha1.RolloutPlanSourcePolicy,
					PortableDigest: "rp1:bbbbbbbbbbbb", DigestEvidence: v1alpha1.EvidenceReported, Policy: &v1alpha1.RolloutPolicyReference{
						Kind: "RolloutPolicy", Name: "slow", Progression: "canary", Evidence: v1alpha1.EvidenceReported,
					}},
			},
			Revisions: []v1alpha1.RolloutHistoryRevision{
				{Component: v1alpha1.RuntimeComponentEngine, Role: v1alpha1.RolloutRevisionCurrent,
					RevisionHash: "cccccccc", Phase: v1alpha1.RolloutPhaseStable},
				{Component: v1alpha1.RuntimeComponentRouter, Role: v1alpha1.RolloutRevisionPrevious,
					RevisionHash: "dddddddd", Phase: v1alpha1.RolloutPhaseRolledBack},
			},
		},
		v1alpha1.ClockFunc(func() time.Time { return collected }),
	)
}
