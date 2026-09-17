package v1alpha1

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitpredicate"
)

func TestWaitReportExplicitRolloutRequest(t *testing.T) {
	for _, requested := range []WaitRequested{"Rollout=Stable", "Rollout=Failed", "Rollout=RolledBack"} {
		r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{Requested: requested}, ClockFunc(func() time.Time { return time.Unix(1, 0) }))
		require.Equal(t, requested, r.Content.Requested)
		require.Equal(t, RolloutConditionUnobserved, r.Content.Rollout.Summary.CoordinationReady)
	}
	field, ok := reflect.TypeOf(WaitContent{}).FieldByName("Rollout")
	require.True(t, ok, "rollout observation must be a concrete additive field")
	require.Equal(t, "rollout,omitempty", field.Tag.Get("json"))
}

func TestWaitRolloutCanonicalSafetyAndCopies(t *testing.T) {
	makeReport := func() WaitReport {
		return WaitReport{Content: WaitContent{Requested: WaitRequestedRolloutStable, Rollout: &WaitRolloutObservation{Validity: "Valid", Summary: RolloutSummary{State: RolloutStateUnknown, ReportedState: RolloutStateSucceeded, Evidence: EvidenceReported, Epoch: RolloutEpochUnverifiable}, Issues: []RolloutIssue{{Code: RolloutIssueEpochUnverifiable}}, Warnings: []WaitRolloutWarning{"StaleReportedCondition"}}}}
	}
	t.Run("copy", func(t *testing.T) {
		r := makeReport()
		c := r.Canonical()
		c.Content.Rollout.Issues[0].Code = RolloutIssueStatusMalformed
		c.Content.Rollout.Warnings[0] = "changed"
		require.Equal(t, RolloutIssueEpochUnverifiable, r.Content.Rollout.Issues[0].Code)
		require.Equal(t, WaitRolloutWarning("StaleReportedCondition"), r.Content.Rollout.Warnings[0])
	})
	t.Run("unknown", func(t *testing.T) {
		r := makeReport()
		r.Content.Rollout.Validity = "SECRET"
		r.Content.Rollout.Summary.ReportedState = "SECRET"
		r.Content.Rollout.Warnings = []WaitRolloutWarning{"SECRET"}
		r.Content.Rollout.Issues = []RolloutIssue{{Code: "SECRET", Component: "SECRET"}}
		data, err := json.Marshal(r.Canonical())
		require.NoError(t, err)
		require.NotContains(t, string(data), "SECRET")
	})
	t.Run("caps", func(t *testing.T) {
		r := makeReport()
		r.Content.Rollout.Issues = make([]RolloutIssue, 65)
		c := r.Canonical()
		require.Equal(t, "Invalid", c.Content.Rollout.Validity)
		require.LessOrEqual(t, len(c.Content.Rollout.Issues), 64)
	})
	t.Run("ready unchanged", func(t *testing.T) {
		r := makeReport()
		r.Content.Requested = WaitRequestedTrue
		require.Nil(t, r.Canonical().Content.Rollout)
	})
	t.Run("selected evidence", func(t *testing.T) {
		r := makeReport()
		c := r.Canonical()
		require.Equal(t, EvidenceReported, c.Content.Evidence)
		require.Equal(t, "NotInspected", c.Content.Observed.Inspection.State)
		require.Equal(t, "Unavailable", c.Content.Observed.Validity)
		require.Equal(t, RolloutEpochUnverifiable, c.Content.Rollout.Summary.Epoch)
	})
	t.Run("empty arrays", func(t *testing.T) {
		r := makeReport()
		r.Content.Rollout.Issues = nil
		r.Content.Rollout.Warnings = nil
		data, err := json.Marshal(r.Canonical())
		require.NoError(t, err)
		require.Contains(t, string(data), `"issues":[]`)
		require.Contains(t, string(data), `"warnings":[]`)
	})
}

func TestWaitRolloutFourViewsAreUsefulAndBounded(t *testing.T) {
	r := NewWaitReport(Metadata{Name: strings.Repeat("界é🙂", 200), Namespace: "prod"}, WaitContent{Requested: WaitRequestedRolloutStable, Rollout: &WaitRolloutObservation{Validity: "Valid", Summary: RolloutSummary{State: RolloutStateUnknown, ReportedState: RolloutStateSucceeded, Evidence: EvidenceReported, Epoch: RolloutEpochUnverifiable}, Inspection: WaitRolloutInspection{State: "Complete"}}, Observed: waitpredicate.Observation{}}, ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	for _, table := range []report.Table{r.Table(), r.WideTable()} {
		var out bytes.Buffer
		require.NoError(t, table.Write(&out))
		require.Contains(t, out.String(), "Succeeded")
		require.Contains(t, out.String(), "Unverifiable")
		require.Contains(t, out.String(), "not current-spec")
		require.Contains(t, out.String(), "not action attribution")
		for _, line := range strings.Split(out.String(), "\n") {
			require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
		}
	}
	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		require.NoError(t, report.Write(&out, format, r))
		require.Contains(t, out.String(), "Succeeded")
	}
}

func TestWaitRolloutPreservesConcreteReasonsAndWideCounts(t *testing.T) {
	for _, reason := range []waitengine.Reason{waitengine.ReasonRolloutMatched, waitengine.ReasonRolloutNotMatched, waitengine.ReasonRolloutNotRecorded, waitengine.ReasonInvalidRollout} {
		r := NewWaitReport(Metadata{Name: "chat"}, WaitContent{Requested: WaitRequestedRolloutStable, Reason: reason, Rollout: &WaitRolloutObservation{Validity: "Valid", Inspection: WaitRolloutInspection{State: "Complete", Conditions: 64, Components: 3, Groups: 3, PinnedGroups: 3, Targets: 3}}}, ClockFunc(func() time.Time { return time.Unix(1, 0) }))
		require.Equal(t, reason, r.Content.Reason)
		var out bytes.Buffer
		require.NoError(t, r.WideTable().Write(&out))
		require.Contains(t, out.String(), "Inspected targets")
		require.Contains(t, out.String(), "Inspected conditions")
		require.Contains(t, out.String(), "Source method")
	}
}

func TestWaitRolloutTableShowsSelectedEvidenceWhenUnavailable(t *testing.T) {
	r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedRolloutStable,
		Rollout: &WaitRolloutObservation{
			Validity: "Unavailable",
			Summary:  RolloutSummary{Evidence: EvidenceReported},
		},
	}, ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.Equal(t, EvidenceUnavailable, r.Content.Evidence)
	for _, row := range r.Table().Rows {
		if row[0] == "Evidence" {
			require.Equal(t, string(EvidenceUnavailable), row[1])
			return
		}
	}
	t.Fatal("Evidence row missing")
}
