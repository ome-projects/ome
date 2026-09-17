package waitmigration

import (
	"testing"

	"github.com/stretchr/testify/require"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

const requestID = "12345678-1234-4234-8234-123456789abc"

func migrationReport(phase reportv1alpha1.MigrationPhase, classification reportv1alpha1.MigrationClassification) reportv1alpha1.MigrationStatusReport {
	return reportv1alpha1.MigrationStatusReport{
		Sources: []reportv1alpha1.MigrationSourceReference{{Kind: reportv1alpha1.MigrationSourceInferenceService, Name: "chat", UID: "parent-uid", Freshness: reportv1alpha1.StatusFreshnessCurrent}, {Kind: reportv1alpha1.MigrationSourceInferenceReplica, Name: "chat-engine", UID: "ir-uid-1", Freshness: reportv1alpha1.StatusFreshnessCurrent}},
		Content: reportv1alpha1.MigrationStatusContent{
			Summary:    reportv1alpha1.MigrationSummary{State: reportv1alpha1.MigrationReportStateReported},
			Migrations: []reportv1alpha1.MigrationRecord{{RequestID: requestID, SourceName: "chat-engine", Phase: phase, Classification: classification, Freshness: reportv1alpha1.StatusFreshnessCurrent, Outcome: reportv1alpha1.MigrationOutcomeCompleted, Component: reportv1alpha1.RuntimeComponentEngine}},
		},
	}
}

func TestEvaluatorPinsFirstObservedIRUIDAcrossPolls(t *testing.T) {
	b := &Evaluator{}
	active := migrationReport(reportv1alpha1.MigrationPhaseAccepted, reportv1alpha1.MigrationClassificationActive)
	active.Content.Migrations[0].Outcome = reportv1alpha1.MigrationOutcomeInProgress
	decision, _ := b.Evaluate(active, requestID)
	require.False(t, decision.Matched)

	replaced := migrationReport(reportv1alpha1.MigrationPhaseCompleted, reportv1alpha1.MigrationClassificationTerminal)
	replaced.Sources[1].UID = "ir-uid-2"
	decision, observed := b.Evaluate(replaced, requestID)
	require.False(t, decision.Matched)
	require.Equal(t, waitengine.ReasonInvalidMigration, decision.Reason)
	require.Equal(t, "Invalid", observed.Validity)

	original := migrationReport(reportv1alpha1.MigrationPhaseCompleted, reportv1alpha1.MigrationClassificationTerminal)
	decision, _ = b.Evaluate(original, requestID)
	require.True(t, decision.Matched)
}

func TestEvaluatorRefusesTerminalRecordWithoutUniqueNamedSource(t *testing.T) {
	r := migrationReport(reportv1alpha1.MigrationPhaseCompleted, reportv1alpha1.MigrationClassificationTerminal)
	r.Content.Migrations[0].SourceName = "other"
	decision, observed := (&Evaluator{}).Evaluate(r, requestID)
	require.False(t, decision.Matched)
	require.Equal(t, waitengine.ReasonInvalidMigration, decision.Reason)
	require.Equal(t, "Invalid", observed.Validity)
}

func TestEvaluateMatchesOnlyExactCurrentTerminalRequest(t *testing.T) {
	for _, tc := range []struct {
		phase   reportv1alpha1.MigrationPhase
		outcome reportv1alpha1.MigrationOutcome
	}{
		{reportv1alpha1.MigrationPhaseCompleted, reportv1alpha1.MigrationOutcomeCompleted},
		{reportv1alpha1.MigrationPhaseFailed, reportv1alpha1.MigrationOutcomeFailed},
		{reportv1alpha1.MigrationPhaseRelocated, reportv1alpha1.MigrationOutcomeRelocated},
	} {
		r := migrationReport(tc.phase, reportv1alpha1.MigrationClassificationTerminal)
		r.Content.Migrations[0].Outcome = tc.outcome
		decision, observed := Evaluate(r, requestID)
		require.Equal(t, waitengine.Decision{Matched: true, Reason: waitengine.ReasonMigrationMatched}, decision)
		require.Equal(t, requestID, observed.RequestID)
		require.Equal(t, tc.phase, observed.Phase)
		require.Equal(t, tc.outcome, observed.Outcome)
		require.Equal(t, "Valid", observed.Validity)
	}
}

func TestEvaluateNeverAttributesUnrelatedOrActiveMigration(t *testing.T) {
	r := migrationReport(reportv1alpha1.MigrationPhaseCompleted, reportv1alpha1.MigrationClassificationTerminal)
	decision, observed := Evaluate(r, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	require.Equal(t, waitengine.ReasonMigrationNotRecorded, decision.Reason)
	require.False(t, decision.Matched)
	require.Equal(t, reportv1alpha1.MigrationPhaseUnknown, observed.Phase)

	r.Content.Migrations[0].Phase = reportv1alpha1.MigrationPhaseAccepted
	r.Content.Migrations[0].Classification = reportv1alpha1.MigrationClassificationActive
	r.Content.Migrations[0].Outcome = reportv1alpha1.MigrationOutcomeInProgress
	decision, observed = Evaluate(r, requestID)
	require.Equal(t, waitengine.ReasonMigrationInProgress, decision.Reason)
	require.False(t, decision.Matched)
	require.Equal(t, reportv1alpha1.MigrationPhaseAccepted, observed.Phase)
}

func TestEvaluateFailsClosedForPartialOrInvalidEvidence(t *testing.T) {
	for _, mutate := range []func(*reportv1alpha1.MigrationStatusReport){
		func(r *reportv1alpha1.MigrationStatusReport) {
			r.Content.Summary.State = reportv1alpha1.MigrationReportStatePartial
		},
		func(r *reportv1alpha1.MigrationStatusReport) {
			r.Sources[1].Freshness = reportv1alpha1.StatusFreshnessStale
		},
		func(r *reportv1alpha1.MigrationStatusReport) {
			r.Content.Migrations[0].Classification = reportv1alpha1.MigrationClassificationInvalid
		},
		func(r *reportv1alpha1.MigrationStatusReport) {
			r.Content.Migrations = append(r.Content.Migrations, r.Content.Migrations[0])
		},
		func(r *reportv1alpha1.MigrationStatusReport) {
			r.Warnings = []reportv1alpha1.MigrationWarning{{Code: reportv1alpha1.MigrationWarningTruncated}}
		},
	} {
		r := migrationReport(reportv1alpha1.MigrationPhaseCompleted, reportv1alpha1.MigrationClassificationTerminal)
		mutate(&r)
		decision, observed := Evaluate(r, requestID)
		require.False(t, decision.Matched)
		require.Equal(t, waitengine.ReasonInvalidMigration, decision.Reason)
		require.Equal(t, "Invalid", observed.Validity)
	}
}
