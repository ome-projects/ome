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
	"sigs.k8s.io/ome/pkg/cli/waitheld"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func validHeldWaitObservation() *WaitHeldRevisionObservation {
	return &WaitHeldRevisionObservation{
		Matched: true, Reason: waitheld.ReasonUnheld, Validity: waitheld.ValidityValid,
		TargetState: waitheld.TargetAbsent, MailboxState: waitheld.MailboxAbsent,
		Attribution: waitheld.AttributionUnverifiable, Component: "engine",
		Revision: "chat-engine-aaaaaaaa", InferenceReplica: "custom-engine", InferenceReplicaUID: "ir-uid",
		Encoding: irstatus.EncodingDenseV1, InspectedSources: 1, InspectedBlocks: 0,
	}
}

func TestWaitHeldRevisionReportRendersBoundedStateOnlyEvidence(t *testing.T) {
	value := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequested("HeldRevision=Unheld"), Outcome: waitengine.OutcomeMatched,
		Reason: waitengine.Reason(waitheld.ReasonUnheld), HeldRevision: validHeldWaitObservation(),
		Method: waitengine.MethodInitialGET, Counts: WaitCounts{Gets: 1, Observations: 1},
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	require.Equal(t, EvidenceReported, value.Content.Evidence)
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		require.NoError(t, report.Write(&out, format, value))
		if format == report.FormatTable {
			require.Contains(t, out.String(), "HeldRevision=Unheld")
			require.Contains(t, out.String(), "Unverifiable")
			require.Contains(t, out.String(), "not action attribution")
			require.Contains(t, out.String(), "Pruning/re-Held")
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
		require.Equal(t, value.Content.HeldRevision, decoded.Content.HeldRevision)
	}
	var wide bytes.Buffer
	require.NoError(t, value.WideTable().Write(&wide))
	for _, line := range strings.Split(wide.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
}

func TestWaitHeldRevisionCanonicalCannotForgeMatchOrLeakIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*WaitHeldRevisionObservation)
	}{
		{"still held", func(o *WaitHeldRevisionObservation) { o.TargetState = waitheld.TargetHeld }},
		{"mailbox pending", func(o *WaitHeldRevisionObservation) { o.MailboxState = waitheld.MailboxPending }},
		{"unavailable", func(o *WaitHeldRevisionObservation) { o.Validity = waitheld.ValidityUnavailable }},
		{"unsafe target", func(o *WaitHeldRevisionObservation) { o.InferenceReplica = "PRIVATE-NAME" }},
		{"unsafe revision", func(o *WaitHeldRevisionObservation) { o.Revision = "PRIVATE-REVISION" }},
		{"unsafe reason", func(o *WaitHeldRevisionObservation) { o.Reason = "PRIVATE-REASON" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observed := validHeldWaitObservation()
			tc.edit(observed)
			value := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
				Requested: WaitRequested("HeldRevision=Unheld"), Outcome: waitengine.OutcomeMatched,
				Reason: waitengine.Reason(waitheld.ReasonUnheld), HeldRevision: observed,
			}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
			require.NotEqual(t, waitengine.OutcomeMatched, value.Content.Outcome)
			data, err := json.Marshal(value)
			require.NoError(t, err)
			require.NotContains(t, string(data), "PRIVATE")
			require.NotContains(t, string(data), "ghp_0123456789abcdefghijklmnopqrst")
		})
	}
}

func TestWaitHeldRevisionCanonicalRedactsOperableCredentialShapedUID(t *testing.T) {
	observed := validHeldWaitObservation()
	observed.InferenceReplicaUID = "ghp_0123456789abcdefghijklmnopqrst"
	value := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequested("HeldRevision=Unheld"), Outcome: waitengine.OutcomeMatched,
		Reason: waitengine.Reason(waitheld.ReasonUnheld), HeldRevision: observed,
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
	require.Equal(t, "[REDACTED]", value.Content.HeldRevision.InferenceReplicaUID)
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.NotContains(t, string(data), "ghp_0123456789abcdefghijklmnopqrst")
}

func TestWaitHeldRevisionReportBindsRevisionToServiceMetadata(t *testing.T) {
	observed := validHeldWaitObservation()
	observed.Revision = "other-engine-aaaaaaaa"
	value := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedHeldRevisionUnheld, Outcome: waitengine.OutcomeMatched,
		Reason: waitengine.Reason(waitheld.ReasonUnheld), HeldRevision: observed,
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	require.NotEqual(t, waitengine.OutcomeMatched, value.Content.Outcome)
	require.False(t, value.Content.HeldRevision.Matched)
}
