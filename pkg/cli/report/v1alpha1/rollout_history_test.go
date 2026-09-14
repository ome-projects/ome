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
	groupOne := 1
	groupZero := 0
	reportValue := v1alpha1.RolloutHistoryReport{
		Metadata:    v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		CollectedAt: opened,
		Sources: []v1alpha1.RolloutSourceReference{
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
				{Slot: v1alpha1.RolloutHistoryRunLast, Outcome: v1alpha1.RolloutHistoryRunCompleted, OpenedAt: &opened, ClosedAt: &closed},
				{Slot: v1alpha1.RolloutHistoryRunActive, Outcome: v1alpha1.RolloutHistoryRunActiveState,
					RunID: "chat-0123456789ab", OpenedAt: &opened, PinnedAt: &closed,
					Targets: []v1alpha1.RolloutHistoryTarget{
						{Component: v1alpha1.RuntimeComponentRouter, RevisionHash: "bbbbbbbb"},
						{Component: v1alpha1.RuntimeComponentEngine, RevisionHash: "aaaaaaaa"},
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

func TestRolloutHistoryCompactTableIsCompleteAndAtMostEightyColumns(t *testing.T) {
	reportValue := rolloutHistoryReportFixture()
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))

	assert.Equal(t, "TYPE     STATE        COMP     IDENT          TIME          DETAIL       ISS\n"+
		"WINDOW   Reported     -        -              -             bounded      0\n"+
		"ACTIVE   Active       -        0123456789ab   09-14T18:00   G:2 T:2      0\n"+
		"TARGET   Active       engine   aaaaaaaa       -             run-target   0\n"+
		"TARGET   Active       router   bbbbbbbb       -             run-target   0\n"+
		"LAST     Completed    -        -              09-14T19:00   G:1          0\n"+
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
	var output bytes.Buffer
	require.NoError(t, rolloutHistoryReportFixture().WideTable().Write(&output))

	assert.Contains(t, output.String(), "chat-0123456789ab")
	assert.Contains(t, output.String(), "rp1:aaaaaaaaaaaa")
	assert.Contains(t, output.String(), "RolloutPolicy/slow")
	assert.Contains(t, output.String(), "RetentionBounded")
	assert.NotContains(t, output.String(), "serverAddress")
	assert.NotContains(t, output.String(), "query")
}

func TestRolloutHistoryWideTableDoesNotInventUnavailablePolicyGeneration(t *testing.T) {
	table := rolloutHistoryReportFixture().WideTable()
	var current []string
	for _, row := range table.Rows {
		if row[10] == string(v1alpha1.RolloutHistoryViewCurrent) {
			current = row
			break
		}
	}
	require.Len(t, current, len(table.Headers))
	assert.Equal(t, "-", current[14])
}

func rolloutHistoryReportFixture() v1alpha1.RolloutHistoryReport {
	opened := time.Date(2026, time.September, 14, 18, 0, 0, 0, time.UTC)
	pinned := opened.Add(30 * time.Minute)
	closed := opened.Add(time.Hour)
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
						{Component: v1alpha1.RuntimeComponentEngine, RevisionHash: "aaaaaaaa"},
						{Component: v1alpha1.RuntimeComponentRouter, RevisionHash: "bbbbbbbb"},
					}, GroupCount: 2},
				{Slot: v1alpha1.RolloutHistoryRunLast, Outcome: v1alpha1.RolloutHistoryRunCompleted,
					OpenedAt: &opened, ClosedAt: &closed, GroupCount: 1},
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
		v1alpha1.ClockFunc(func() time.Time { return closed }),
	)
}
