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
	"sigs.k8s.io/yaml"
)

func TestWaitReadyReplicasReportPreservesVerifiedZeroInEveryFormat(t *testing.T) {
	zero := int32(0)
	r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
		Requested: WaitRequestedReadyReplicas, Outcome: waitengine.OutcomeMatched,
		Reason: waitengine.ReasonReplicaReadyMatched, Method: waitengine.MethodInitialGET,
		ReadyReplicas: &WaitReadyReplicasObservation{Component: RuntimeComponentEngine, Requested: 0, Observed: &zero, Validity: "Valid"},
		Counts:        WaitCounts{Gets: 1, Observations: 1},
	}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
	require.Equal(t, EvidenceReported, r.Content.Evidence)
	require.Nil(t, r.Content.Migration)
	require.Equal(t, "NotRecorded", r.Content.Observed.Status)
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		require.NoError(t, report.Write(&out, format, r))
		require.NotContains(t, out.String(), "Observed Ready")
		if format == report.FormatTable {
			require.Contains(t, out.String(), "engine")
			require.Contains(t, out.String(), "Matched")
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
		require.NotNil(t, decoded.Content.ReadyReplicas)
		require.NotNil(t, decoded.Content.ReadyReplicas.Observed)
		require.Zero(t, *decoded.Content.ReadyReplicas.Observed)
		if format == report.FormatJSON {
			require.Contains(t, out.String(), `"observed": 0`)
		}
	}
	var wide bytes.Buffer
	require.NoError(t, r.WideTable().Write(&wide))
	for _, line := range strings.Split(wide.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
}

func TestWaitReadyReplicasCanonicalCannotForgeMatchedOrLeakUnverifiedCount(t *testing.T) {
	for _, tc := range []struct {
		name      string
		component RuntimeComponentType
		requested int32
		observed  *int32
		validity  string
	}{
		{name: "missing observation", component: RuntimeComponentEngine, requested: 0, validity: "Valid"},
		{name: "wrong count", component: RuntimeComponentEngine, requested: 1, observed: ptrWaitCount(2), validity: "Valid"},
		{name: "invalid component", component: "PRIVATE-COMPONENT", requested: 0, observed: ptrWaitCount(0), validity: "Valid"},
		{name: "unavailable", component: RuntimeComponentEngine, requested: 0, observed: ptrWaitCount(0), validity: "Unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewWaitReport(Metadata{Name: "chat", Namespace: "prod"}, WaitContent{
				Requested: WaitRequestedReadyReplicas, Outcome: waitengine.OutcomeMatched,
				Reason:        waitengine.ReasonReplicaReadyMatched,
				ReadyReplicas: &WaitReadyReplicasObservation{Component: tc.component, Requested: tc.requested, Observed: tc.observed, Validity: tc.validity},
			}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
			require.NotEqual(t, waitengine.OutcomeMatched, r.Content.Outcome)
			require.Equal(t, EvidenceUnavailable, r.Content.Evidence)
			require.Nil(t, r.Content.ReadyReplicas.Observed)
			encoded, err := json.Marshal(r)
			require.NoError(t, err)
			require.NotContains(t, string(encoded), "PRIVATE-COMPONENT")
		})
	}
}

func ptrWaitCount(value int32) *int32 { return &value }
