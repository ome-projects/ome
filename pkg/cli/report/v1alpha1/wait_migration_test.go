package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
)

func TestWaitMigrationOutputIsRequestScopedAndBounded(t *testing.T) {
	r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedMigrationTerminal, Outcome: waitengine.OutcomeMatched,
		Reason: waitengine.ReasonMigrationMatched, Method: waitengine.MethodInitialGET,
		Migration: &WaitMigrationObservation{RequestID: "12345678-1234-4234-8234-123456789abc", Component: RuntimeComponentEngine, Phase: MigrationPhaseFailed, Outcome: MigrationOutcomeFailed, Validity: "Valid", InspectedSources: 2, InspectedRecords: 1},
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	require.Equal(t, WaitRequestedMigrationTerminal, r.Content.Requested)
	require.Nil(t, r.Content.Rollout)
	require.Equal(t, "NotRecorded", r.Content.Observed.Status)
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, r))
		require.Contains(t, output.String(), "12345678-1234-4234-8234-123456789abc")
		require.Contains(t, output.String(), "Failed")
		require.NotContains(t, output.String(), "Observed Ready")
		if format == report.FormatTable {
			for _, line := range strings.Split(output.String(), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
		}
	}
	var wide bytes.Buffer
	require.NoError(t, r.WideTable().Write(&wide))
	for _, line := range strings.Split(wide.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
}

func TestWaitMigrationCanonicalRejectsHostileUnselectedFields(t *testing.T) {
	r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{Requested: WaitRequestedMigrationTerminal,
		Migration: &WaitMigrationObservation{RequestID: "Bearer PRIVATE", Component: "PRIVATE", Phase: "PRIVATE", Outcome: "PRIVATE", Validity: "PRIVATE", InspectedSources: 999, InspectedRecords: 9999},
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	data, err := json.Marshal(r)
	require.NoError(t, err)
	require.NotContains(t, string(data), "PRIVATE")
	require.Empty(t, r.Content.Migration.RequestID)
	require.Equal(t, MigrationPhaseUnknown, r.Content.Migration.Phase)
	require.Equal(t, MigrationOutcomeUnknown, r.Content.Migration.Outcome)
	require.Equal(t, "Invalid", r.Content.Migration.Validity)
	require.LessOrEqual(t, r.Content.Migration.InspectedSources, 65)
	require.LessOrEqual(t, r.Content.Migration.InspectedRecords, 200)
	r.Content.Reason = waitengine.ReasonMatched
	require.Equal(t, waitengine.Reason("PredicateUnmet"), r.Canonical().Content.Reason)
}

func TestWaitMigrationTableDoesNotClaimAttributionWithoutValidEvidence(t *testing.T) {
	for _, tc := range []struct{ validity, want string }{
		{"Unavailable", "No verified matching IR record"},
		{"Invalid", "Unverifiable; incomplete IR evidence"},
	} {
		r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{Requested: WaitRequestedMigrationTerminal,
			Migration: &WaitMigrationObservation{RequestID: "12345678-1234-4234-8234-123456789abc", Phase: MigrationPhaseUnknown, Outcome: MigrationOutcomeUnknown, Validity: tc.validity},
		}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
		var out bytes.Buffer
		require.NoError(t, r.Table().Write(&out))
		require.Contains(t, out.String(), tc.want)
		require.NotContains(t, out.String(), "Exact request ID in live IR status")
	}
}

func TestWaitMigrationInvalidRequestIDCannotRetainValidAttribution(t *testing.T) {
	r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{Requested: WaitRequestedMigrationTerminal,
		Migration: &WaitMigrationObservation{RequestID: "Bearer PRIVATE", Phase: MigrationPhaseCompleted, Outcome: MigrationOutcomeCompleted, Validity: "Valid"},
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	require.Empty(t, r.Content.Migration.RequestID)
	require.Equal(t, "Invalid", r.Content.Migration.Validity)
	var out bytes.Buffer
	require.NoError(t, r.Table().Write(&out))
	require.NotContains(t, out.String(), "Exact request ID in live IR status")
	require.NotContains(t, out.String(), "PRIVATE")
}

func TestWaitMigrationMalformedValidObservationCannotClaimLiveIR(t *testing.T) {
	for _, tc := range []struct {
		name        string
		observation WaitMigrationObservation
	}{
		{name: "missing component", observation: WaitMigrationObservation{Phase: MigrationPhaseCompleted, Outcome: MigrationOutcomeCompleted}},
		{name: "unknown phase", observation: WaitMigrationObservation{Component: RuntimeComponentEngine, Phase: MigrationPhaseUnknown, Outcome: MigrationOutcomeCompleted}},
		{name: "mismatched terminal pair", observation: WaitMigrationObservation{Component: RuntimeComponentEngine, Phase: MigrationPhaseFailed, Outcome: MigrationOutcomeCompleted}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observation := tc.observation
			observation.RequestID = "12345678-1234-4234-8234-123456789abc"
			observation.Validity = "Valid"
			observation.InspectedSources = 2
			observation.InspectedRecords = 1
			r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
				Requested: WaitRequestedMigrationTerminal, Outcome: waitengine.OutcomeMatched,
				Reason: waitengine.ReasonMigrationMatched, Migration: &observation,
			}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
			require.Equal(t, "Invalid", r.Content.Migration.Validity)
			require.Equal(t, EvidenceUnavailable, r.Content.Evidence)
			require.NotEqual(t, waitengine.OutcomeMatched, r.Content.Outcome)
			require.Equal(t, waitengine.ReasonInvalidMigration, r.Content.Reason)
			var out bytes.Buffer
			require.NoError(t, r.Table().Write(&out))
			require.NotContains(t, out.String(), "Exact request ID in live IR status")
		})
	}
}

func TestWaitMigrationTimedOutLastRecordIsNotCalledLive(t *testing.T) {
	r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedMigrationTerminal, Outcome: waitengine.OutcomeTimedOut,
		Reason: waitengine.ReasonMigrationInProgress,
		Migration: &WaitMigrationObservation{RequestID: "12345678-1234-4234-8234-123456789abc",
			Component: RuntimeComponentEngine, Phase: MigrationPhaseAccepted,
			Outcome: MigrationOutcomeInProgress, Validity: "Valid",
			InspectedSources: 2, InspectedRecords: 1},
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	var out bytes.Buffer
	require.NoError(t, r.Table().Write(&out))
	require.Contains(t, out.String(), "Last observed exact IR record")
	require.NotContains(t, out.String(), "Exact request ID in live IR status")
}
