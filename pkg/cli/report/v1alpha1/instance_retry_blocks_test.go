package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstanceRetryBlocksReportCanonicalizesWithoutMutatingInput(t *testing.T) {
	t.Parallel()

	next := time.Date(2026, 9, 15, 1, 2, 0, 0, time.FixedZone("offset", 2*60*60))
	last := time.Date(2026, 9, 14, 22, 0, 0, 0, time.UTC)
	content := InstanceRetryBlocksContent{
		Summary: InstanceRetryBlocksSummary{State: InstanceRetryBlocksReported, Blocks: 2, Held: 1, Eligible: 1},
		Blocks: []InstanceRetryBlock{
			{
				Component: RuntimeComponentDecoder, InferenceReplica: "chat-decoder",
				TargetRevision: "chat-decoder-bbbbbbbb", State: InstanceRetryBlockBackoff,
				AttemptsStarted: 2, NextRetryAt: &next, LastFailureAt: &last,
				Reason: "capacity unavailable", Freshness: StatusFreshnessCurrent,
			},
			{
				Component: RuntimeComponentEngine, InferenceReplica: "chat-engine",
				TargetRevision: "chat-engine-aaaaaaaa", State: InstanceRetryBlockHeld,
				AttemptsStarted: 3, LastFailureAt: &last, Reason: "image pull failed",
				ReleaseEligible: true, Freshness: StatusFreshnessCurrent,
			},
		},
		Issues: []InstanceRetryBlockIssue{},
	}
	reportValue := NewInstanceRetryBlocksReport(
		Metadata{Namespace: "prod", Name: "chat"}, content,
		ClockFunc(func() time.Time { return time.Date(2026, 9, 14, 23, 0, 0, 0, time.UTC) }),
	)
	reportValue.Sources = []SourceReference{
		{Kind: "InferenceService", Namespace: "prod", Name: "chat", Evidence: EvidenceReported},
		{Kind: "InferenceReplica", Namespace: "prod", Name: "chat-engine", Evidence: EvidenceReported},
	}
	reportValue.Warnings = []Warning{
		{Code: WarningTruncated}, {Code: WarningPartialData}, {Code: WarningTruncated},
	}

	canonical := reportValue.Canonical()

	require.Len(t, canonical.Content.Blocks, 2)
	assert.Equal(t, RuntimeComponentEngine, canonical.Content.Blocks[0].Component)
	assert.Equal(t, RuntimeComponentDecoder, canonical.Content.Blocks[1].Component)
	assert.Equal(t, "2026-09-14T23:02:00Z", canonical.Content.Blocks[1].NextRetryAt.Format(time.RFC3339))
	*canonical.Content.Blocks[1].NextRetryAt = time.Time{}
	assert.Equal(t, next, *content.Blocks[0].NextRetryAt)
	assert.Equal(t, RuntimeComponentDecoder, content.Blocks[0].Component)
	assert.Equal(t, []Warning{{Code: WarningPartialData}, {Code: WarningTruncated}}, canonical.Warnings)
	require.Len(t, canonical.Sources, 2)
	assert.Equal(t, "InferenceReplica", canonical.Sources[0].Kind)
	for _, source := range canonical.Sources {
		assert.Equal(t, reportValue.CollectedAt, source.CollectedAt)
	}
	assert.Zero(t, reportValue.Sources[0].CollectedAt)
	assert.Len(t, reportValue.Warnings, 3)
}

func TestInstanceRetryBlocksTableIsCompactAndOperational(t *testing.T) {
	t.Parallel()

	next := time.Date(2026, 9, 15, 1, 2, 0, 0, time.UTC)
	content := InstanceRetryBlocksContent{
		Summary: InstanceRetryBlocksSummary{State: InstanceRetryBlocksPartial, Blocks: 2, Held: 1, Eligible: 1},
		Blocks: []InstanceRetryBlock{
			{
				Component: RuntimeComponentEngine, TargetRevision: "chat-engine-aaaaaaaa",
				State: InstanceRetryBlockHeld, AttemptsStarted: 3, ReleaseEligible: true,
				Reason: "image pull failed", Freshness: StatusFreshnessCurrent,
			},
			{
				Component: RuntimeComponentDecoder, TargetRevision: "chat-decoder-bbbbbbbb",
				State: InstanceRetryBlockBackoff, AttemptsStarted: 2, NextRetryAt: &next,
				Reason: "waiting\nfor capacity", Freshness: StatusFreshnessCurrent,
			},
		},
		Issues: []InstanceRetryBlockIssue{{
			Code: InstanceRetryBlockIssueCollectionTruncated,
		}},
	}

	table := content.Table()

	assert.Equal(t, []string{"COMP", "STATE", "TARGET", "ATT", "NEXT", "REL", "REASON"}, table.Headers)
	require.Len(t, table.Rows, 3)
	assert.Equal(t, []string{"engine", "HELD", "chat-#a29b545d", "3", "-", "YES", "image pull..."}, table.Rows[0])
	assert.Equal(t, []string{"decoder", "BACKOFF", "chat-#ff03205a", "2", "26-09-15T01:02Z", "NO", `waiting\nf...`}, table.Rows[1])
	assert.Equal(t, []string{"-", "ISSUE", "-", "-", "-", "NO", "COLL_TRUNC"}, table.Rows[2])

	var output bytes.Buffer
	require.NoError(t, table.Write(&output))
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 80, line)
	}
}

func TestInstanceRetryBlocksTableUsesStableCollisionResistantRevisionAliases(t *testing.T) {
	t.Parallel()

	content := InstanceRetryBlocksContent{Blocks: []InstanceRetryBlock{
		{Component: RuntimeComponentEngine, TargetRevision: "same-prefix-one-aaaaaaaa", State: InstanceRetryBlockHeld},
		{Component: RuntimeComponentEngine, TargetRevision: "same-prefix-two-aaaaaaaa", State: InstanceRetryBlockHeld},
	}}

	first := content.Table()
	second := content.Table()
	require.Len(t, first.Rows, 2)
	assert.Equal(t, first, second)
	assert.NotEqual(t, first.Rows[0][2], first.Rows[1][2])
	for _, row := range first.Rows {
		assert.Len(t, row[2], 14)
		assert.Contains(t, row[2], "#")
	}
}

func TestInstanceRetryBlocksTableBoundsMalformedAndFutureValues(t *testing.T) {
	t.Parallel()

	content := InstanceRetryBlocksContent{
		Blocks: []InstanceRetryBlock{
			{
				Component: RuntimeComponentEngine, TargetRevision: "short-rev",
				State: InstanceRetryBlockRetryInProgress, AttemptsStarted: 100,
				Reason: strings.Repeat("\u0301", 200),
			},
			{
				Component: RuntimeComponentRouter, TargetRevision: "bad\n\u202eSECRET",
				State: "Future", AttemptsStarted: -1,
			},
		},
		Issues: []InstanceRetryBlockIssue{{
			Code: InstanceRetryBlockIssueCode("Future\nSECRET"),
		}},
	}

	table := content.Table()

	require.Len(t, table.Rows, 3)
	assert.Equal(t, "RUNNING", table.Rows[0][1])
	assert.Equal(t, "short-rev", table.Rows[0][2])
	assert.Equal(t, "99+", table.Rows[0][3])
	assert.Equal(t, "INVALID", table.Rows[1][1])
	assert.Equal(t, "?", table.Rows[1][3])
	assert.True(t, strings.HasPrefix(table.Rows[1][2], "!"))
	assert.NotContains(t, table.Rows[1][2], "SECRET")
	assert.True(t, strings.HasPrefix(table.Rows[2][6], "X#"))
	assert.NotContains(t, table.Rows[2][6], "SECRET")

	reportValue := NewInstanceRetryBlocksReport(
		Metadata{Name: "chat"}, content,
		ClockFunc(func() time.Time { return time.Unix(1, 0) }),
	)
	var output bytes.Buffer
	require.NoError(t, reportValue.Table().Write(&output))
	assert.NotContains(t, output.String(), "SECRET")
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 80, line)
	}
}

func TestInstanceRetryBlocksTableShowsExplicitEmptyState(t *testing.T) {
	t.Parallel()

	table := (InstanceRetryBlocksContent{
		Summary: InstanceRetryBlocksSummary{State: InstanceRetryBlocksEmpty},
		Blocks:  []InstanceRetryBlock{}, Issues: []InstanceRetryBlockIssue{},
	}).Table()

	assert.Equal(t, [][]string{{"-", "EMPTY", "-", "-", "-", "NO", "-"}}, table.Rows)
}

func TestInstanceRetryBlocksJSONKeepsTypedFullEvidence(t *testing.T) {
	t.Parallel()

	first := time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC)
	last := time.Date(2026, 9, 14, 21, 0, 0, 0, time.UTC)
	reportValue := NewInstanceRetryBlocksReport(
		Metadata{Namespace: "prod", Name: "chat"},
		InstanceRetryBlocksContent{Blocks: []InstanceRetryBlock{{
			Component: RuntimeComponentEngine, InferenceReplica: "chat-engine",
			TargetRevision: "chat-engine-complete-revision-aaaaaaaa",
			State:          InstanceRetryBlockHeld, AttemptsStarted: 3,
			FirstFailureAt: &first, LastFailureAt: &last,
			Reason: "image pull failed", ReleaseEligible: true,
			Freshness: StatusFreshnessCurrent,
		}}, Issues: []InstanceRetryBlockIssue{}},
		ClockFunc(func() time.Time { return last }),
	)

	encoded, err := json.Marshal(reportValue)

	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"kind":"InstanceRetryBlocksReport"`)
	assert.Contains(t, string(encoded), `"targetRevision":"chat-engine-complete-revision-aaaaaaaa"`)
	assert.Contains(t, string(encoded), `"releaseEligible":true`)
	assert.Contains(t, string(encoded), `"firstFailureAt":"2026-09-14T20:00:00Z"`)
}

func TestCompareInstanceRetryBlocksUsesEveryFieldAsTieBreaker(t *testing.T) {
	t.Parallel()

	earlier := time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Minute)
	base := InstanceRetryBlock{
		Component: RuntimeComponentEngine, InferenceReplica: "chat-engine",
		TargetRevision: "chat-engine-aaaaaaaa", State: InstanceRetryBlockHeld,
		AttemptsStarted: 1, NextRetryAt: &earlier, FirstFailureAt: &earlier,
		LastFailureAt: &earlier, Reason: "a", Freshness: StatusFreshnessCurrent,
	}
	tests := []struct {
		name   string
		mutate func(*InstanceRetryBlock)
	}{
		{name: "attempts", mutate: func(block *InstanceRetryBlock) { block.AttemptsStarted = 2 }},
		{name: "next retry", mutate: func(block *InstanceRetryBlock) { block.NextRetryAt = &later }},
		{name: "first failure", mutate: func(block *InstanceRetryBlock) { block.FirstFailureAt = &later }},
		{name: "last failure", mutate: func(block *InstanceRetryBlock) { block.LastFailureAt = &later }},
		{name: "reason", mutate: func(block *InstanceRetryBlock) { block.Reason = "b" }},
		{name: "reason truncated", mutate: func(block *InstanceRetryBlock) { block.ReasonTruncated = true }},
		{name: "release eligible", mutate: func(block *InstanceRetryBlock) { block.ReleaseEligible = true }},
		{name: "freshness", mutate: func(block *InstanceRetryBlock) { block.Freshness = StatusFreshnessInvalid }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			other := base
			test.mutate(&other)
			assert.Equal(t, -1, compareInstanceRetryBlocks(base, other))
			assert.Equal(t, 1, compareInstanceRetryBlocks(other, base))
		})
	}
}

func TestInstanceRetryBlocksCanonicalOutputIgnoresEquivalentInputOrder(t *testing.T) {
	t.Parallel()

	firstTime := time.Date(2026, 9, 14, 20, 0, 0, 0, time.UTC)
	lastTime := firstTime.Add(time.Hour)
	blocks := []InstanceRetryBlock{
		{
			Component: RuntimeComponentEngine, InferenceReplica: "chat-engine",
			TargetRevision: "chat-engine-aaaaaaaa", State: InstanceRetryBlockHeld,
			AttemptsStarted: 2, FirstFailureAt: &firstTime, LastFailureAt: &lastTime,
			Reason: "first", Freshness: StatusFreshnessCurrent,
		},
		{
			Component: RuntimeComponentEngine, InferenceReplica: "chat-engine",
			TargetRevision: "chat-engine-aaaaaaaa", State: InstanceRetryBlockHeld,
			AttemptsStarted: 3, FirstFailureAt: &firstTime, LastFailureAt: &lastTime,
			Reason: "second", ReasonTruncated: true, ReleaseEligible: true,
			Freshness: StatusFreshnessCurrent,
		},
	}
	content := InstanceRetryBlocksContent{Blocks: blocks, Issues: []InstanceRetryBlockIssue{}}
	reversed := InstanceRetryBlocksContent{
		Blocks: []InstanceRetryBlock{blocks[1], blocks[0]}, Issues: []InstanceRetryBlockIssue{},
	}

	firstJSON, err := json.Marshal(content.Canonical())
	require.NoError(t, err)
	secondJSON, err := json.Marshal(reversed.Canonical())
	require.NoError(t, err)
	assert.Equal(t, string(firstJSON), string(secondJSON))
	assert.Equal(t, content.Table(), reversed.Table())
}
