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
	"sigs.k8s.io/ome/pkg/cli/waitruntime"
)

const reportSyncRequestID = "123e4567-e89b-42d3-a456-426614174000"

func TestWaitRuntimeSyncReportUsesAllowlistedEvidenceInEveryFormat(t *testing.T) {
	value := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedRuntimeSyncAcknowledged,
		Outcome:   waitengine.OutcomeMatched,
		Reason:    waitruntime.ReasonObserved,
		Method:    waitengine.MethodInitialGET,
		RuntimeSync: &WaitRuntimeSyncObservation{
			RequestID: reportSyncRequestID, TokenState: waitruntime.TokenAcknowledged,
			DriftState: waitruntime.DriftClear, PinState: waitruntime.PinManaged,
			PlacementState: waitruntime.PlacementDirect, Validity: "Valid",
			GenerationFreshness: "Unverifiable", InspectedConditions: 2,
		},
		Counts: WaitCounts{Gets: 1, Observations: 1},
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	require.Equal(t, EvidenceReported, value.Content.Evidence)
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		require.NoError(t, report.Write(&out, format, value))
		require.NotContains(t, out.String(), "PRIVATE")
		if format == report.FormatTable {
			require.Contains(t, out.String(), "Acknowledged")
			require.Contains(t, out.String(), "not live-runtime")
			require.Contains(t, out.String(), "not action attribution")
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
		require.Equal(t, value.Content.RuntimeSync, decoded.Content.RuntimeSync)
	}
	var wide bytes.Buffer
	require.NoError(t, value.WideTable().Write(&wide))
	for _, line := range strings.Split(wide.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
}

func TestWaitRuntimeSyncCanonicalCannotForgeMatchOrLeakArbitraryStates(t *testing.T) {
	missing := NewWaitReport(Metadata{Name: "chat"}, WaitContent{
		Requested: WaitRequestedRuntimeSyncAcknowledged,
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	require.Equal(t, "Unavailable", missing.Content.RuntimeSync.Validity)
	require.Equal(t, EvidenceUnavailable, missing.Content.Evidence)

	for _, tc := range []struct {
		name string
		edit func(*WaitRuntimeSyncObservation)
	}{
		{"pending", func(o *WaitRuntimeSyncObservation) { o.TokenState = waitruntime.TokenPending }},
		{"drift", func(o *WaitRuntimeSyncObservation) { o.DriftState = waitruntime.DriftReportedFalse }},
		{"pin", func(o *WaitRuntimeSyncObservation) { o.PinState = waitruntime.PinUnavailable }},
		{"placement", func(o *WaitRuntimeSyncObservation) { o.PlacementState = waitruntime.PlacementUnsupported }},
		{"invalid", func(o *WaitRuntimeSyncObservation) { o.Validity = "PRIVATE-VALIDITY" }},
		{"bad id", func(o *WaitRuntimeSyncObservation) { o.RequestID = "PRIVATE-REQUEST-ID" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := &WaitRuntimeSyncObservation{RequestID: reportSyncRequestID,
				TokenState: waitruntime.TokenAcknowledged, DriftState: waitruntime.DriftClear,
				PinState: waitruntime.PinManaged, PlacementState: waitruntime.PlacementDirect,
				Validity: "Valid", GenerationFreshness: "Unverifiable"}
			tc.edit(o)
			value := NewWaitReport(Metadata{Name: "chat"}, WaitContent{
				Requested: WaitRequestedRuntimeSyncAcknowledged,
				Outcome:   waitengine.OutcomeMatched, Reason: waitruntime.ReasonObserved,
				RuntimeSync: o,
			}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
			require.NotEqual(t, waitengine.OutcomeMatched, value.Content.Outcome)
			data, err := json.Marshal(value)
			require.NoError(t, err)
			require.NotContains(t, string(data), "PRIVATE")
		})
	}
}
