package v1alpha1

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/cli/waitscale"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func scaleWaitClock() Clock {
	return ClockFunc(func() time.Time { return time.Unix(1000, 0) })
}

func scaleWaitObservation() *WaitScaleObservation {
	return &WaitScaleObservation{
		Component: "engine", Requested: 3,
		SpecReplicas: scaleWaitCount(3), CurrentReplicas: scaleWaitCount(3), ReadyReplicas: scaleWaitCount(2),
		Encoding: string(irstatus.EncodingColumnarV2), Validity: "Valid", Freshness: "Current",
	}
}

func scaleWaitCount(value int32) *int32 { return &value }

func TestWaitScaleReportPreservesCurrentLogicalAndSeparateReadyCounts(t *testing.T) {
	r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedScaleCurrent, Outcome: waitengine.OutcomeMatched,
		Reason: waitscale.ReasonMatched, Method: waitengine.MethodInitialGET,
		Scale: scaleWaitObservation(), Counts: WaitCounts{Gets: 1, Observations: 1},
	}, scaleWaitClock())
	require.Equal(t, EvidenceReported, r.Content.Evidence)
	require.Equal(t, waitengine.OutcomeMatched, r.Content.Outcome)
	require.Nil(t, r.Content.ReadyReplicas)
	require.Equal(t, "NotRecorded", r.Content.Observed.Status)

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		require.NoError(t, report.Write(&out, format, r))
		if format == report.FormatTable {
			for _, fact := range []string{"Replicas=Current", "engine", "ColumnarV2", "Matched", "3", "2", "logical Instances", "Ready separate", "not action attribution"} {
				require.Contains(t, out.String(), fact)
			}
			require.NotContains(t, out.String(), "Observed Ready")
			for _, line := range strings.Split(out.String(), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
			continue
		}
		data := out.Bytes()
		if format == report.FormatYAML {
			var err error
			data, err = yaml.YAMLToJSONStrict(data)
			require.NoError(t, err)
		}
		var decoded WaitReport
		require.NoError(t, json.Unmarshal(data, &decoded))
		require.Equal(t, r.Content.Scale, decoded.Content.Scale)
		require.Equal(t, waitengine.OutcomeMatched, decoded.Content.Outcome)
	}
	var wide bytes.Buffer
	require.NoError(t, r.WideTable().Write(&wide))
	for _, line := range strings.Split(wide.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
}

func TestWaitScaleCanonicalRejectsForgedMatchAndScrubsUnverifiedCounters(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*WaitScaleObservation)
	}{
		{"missing spec", func(o *WaitScaleObservation) { o.SpecReplicas = nil }},
		{"wrong spec", func(o *WaitScaleObservation) { o.SpecReplicas = scaleWaitCount(2) }},
		{"wrong current", func(o *WaitScaleObservation) { o.CurrentReplicas = scaleWaitCount(2) }},
		{"too many ready", func(o *WaitScaleObservation) { o.ReadyReplicas = scaleWaitCount(4) }},
		{"invalid component", func(o *WaitScaleObservation) { o.Component = "PRIVATE-COMPONENT" }},
		{"invalid encoding", func(o *WaitScaleObservation) { o.Encoding = "PRIVATE-ENCODING" }},
		{"stale", func(o *WaitScaleObservation) { o.Freshness = "Stale" }},
		{"unavailable", func(o *WaitScaleObservation) { o.Validity = "Unavailable" }},
		{"invalid freshness", func(o *WaitScaleObservation) { o.Freshness = "PRIVATE-FRESHNESS" }},
		{"invalid validity", func(o *WaitScaleObservation) { o.Validity = "PRIVATE-VALIDITY" }},
		{"zero target", func(o *WaitScaleObservation) { o.Requested = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := scaleWaitObservation()
			tc.edit(o)
			r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
				Requested: WaitRequestedScaleCurrent, Outcome: waitengine.OutcomeMatched,
				Reason: waitscale.ReasonMatched, Scale: o,
			}, scaleWaitClock())
			require.NotEqual(t, waitengine.OutcomeMatched, r.Content.Outcome)
			require.Equal(t, EvidenceUnavailable, r.Content.Evidence)
			data, err := json.Marshal(r)
			require.NoError(t, err)
			require.NotContains(t, string(data), "PRIVATE")
		})
	}
}

func TestWaitScaleCanonicalRequiresMatchingReasonAndDetachesCountPointers(t *testing.T) {
	o := scaleWaitObservation()
	r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedScaleCurrent, Outcome: waitengine.OutcomeMatched,
		Reason: waitscale.ReasonNotMatched, Scale: o,
	}, scaleWaitClock())
	require.NotEqual(t, waitengine.OutcomeMatched, r.Content.Outcome)
	require.Nil(t, r.Content.Scale.SpecReplicas)

	valid := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedScaleCurrent, Outcome: waitengine.OutcomeMatched,
		Reason: waitscale.ReasonMatched, Scale: o,
	}, scaleWaitClock())
	*o.SpecReplicas = 99
	require.Equal(t, int32(3), *valid.Content.Scale.SpecReplicas)

	missing := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedScaleCurrent,
	}, scaleWaitClock())
	require.Equal(t, "Unavailable", missing.Content.Scale.Validity)
	require.Nil(t, missing.Content.Scale.CurrentReplicas)
	var missingTable bytes.Buffer
	require.NoError(t, missing.Table().Write(&missingTable))
	require.Contains(t, missingTable.String(), "<unavailable>")
	require.Contains(t, missingTable.String(), "No verified exact scale count")

	timedOut := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedScaleCurrent, Outcome: waitengine.OutcomeTimedOut,
		Reason: waitscale.ReasonNotMatched, Scale: scaleWaitObservation(),
	}, scaleWaitClock())
	var lastTable bytes.Buffer
	require.NoError(t, timedOut.Table().Write(&lastTable))
	require.Contains(t, lastTable.String(), "Last observation; currentness unverified")

	other := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedTrue, Scale: scaleWaitObservation(),
	}, scaleWaitClock())
	require.Nil(t, other.Content.Scale)
}
