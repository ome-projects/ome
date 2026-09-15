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
	"sigs.k8s.io/ome/pkg/cli/waitpredicate"
)

func waitFixture() WaitReport {
	return NewWaitReport(Metadata{Name: "service", Namespace: "work"}, WaitContent{Requested: WaitRequestedTrue, Outcome: waitengine.OutcomeMatched, Reason: waitengine.ReasonMatched, Observed: waitpredicate.Observation{Status: "True", Validity: "Valid", GenerationFreshness: "Unverifiable", Inspection: waitpredicate.Inspection{State: "Complete", Total: 1, Inspected: 1, Warnings: []waitpredicate.Warning{}}}, Counts: WaitCounts{Gets: 1, Observations: 1}, Method: waitengine.MethodInitialGET, ElapsedMilliseconds: 1}, ClockFunc(func() time.Time { return time.Unix(1000, 0) }))
}
func TestWaitAllFormatsPreserveFactsAndWidth(t *testing.T) {
	r := waitFixture()
	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML, report.FormatTable} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, r))
		if format == report.FormatTable {
			for _, line := range strings.Split(output.String(), "\n") {
				require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
			}
			for _, fact := range []string{"Ready=True", "Matched", "True", "Unverifiable", "Complete", "InitialGET"} {
				require.Contains(t, output.String(), fact)
			}
		} else {
			data := output.Bytes()
			if format == report.FormatYAML {
				var err error
				data, err = yaml.YAMLToJSONStrict(data)
				require.NoError(t, err)
			}
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.DisallowUnknownFields()
			var got WaitReport
			require.NoError(t, decoder.Decode(&got))
			require.Equal(t, r, got)
		}
	}
	var wide bytes.Buffer
	require.NoError(t, r.WideTable().Write(&wide))
	for _, line := range strings.Split(wide.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
}
func TestWaitCanonicalDetachesWarningsAndSanitizesUnknownEnums(t *testing.T) {
	r := waitFixture()
	r.Metadata.Name = "name\n\x1b[31m"
	r.Content.Observed.Inspection.Warnings = []waitpredicate.Warning{waitpredicate.WarningDuplicateReady}
	canonical := r.Canonical()
	canonical.Content.Observed.Inspection.Warnings[0] = waitpredicate.WarningInvalidRecord
	require.Equal(t, waitpredicate.WarningDuplicateReady, r.Content.Observed.Inspection.Warnings[0])
	require.NotContains(t, canonical.Metadata.Name, "\n")
	require.NotContains(t, canonical.Metadata.Name, "\x1b")
	r.Content.Outcome = "Bearer PRIVATE"
	r.Content.Reason = "password PRIVATE"
	r.Content.Method = "token PRIVATE"
	r.Content.Observed.Status = "PRIVATE"
	r.Content.Observed.Validity = "PRIVATE"
	r.Content.Observed.GenerationFreshness = "PRIVATE"
	r.Content.Observed.Inspection.State = "PRIVATE"
	r.Content.Observed.Inspection.Warnings = []waitpredicate.Warning{"PRIVATE"}
	r.Content.Requested = "PRIVATE"
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatJSON, r))
	require.NotContains(t, output.String(), "PRIVATE")
}

func TestWaitElapsedPreservesMeasuredCooperativeOverrun(t *testing.T) {
	r := waitFixture()
	r.Content.ElapsedMilliseconds = 86400001
	require.Equal(t, int64(86400001), r.Canonical().Content.ElapsedMilliseconds)
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatJSON, r))
	require.Contains(t, output.String(), `"elapsedMilliseconds": 86400001`)
	r.Content.ElapsedMilliseconds = -1
	require.Zero(t, r.Canonical().Content.ElapsedMilliseconds)
}

func TestWaitCredentialShapedMetadataIsRedactedInEveryView(t *testing.T) {
	const credential = "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	r := waitFixture()
	r.Metadata = Metadata{Name: credential, Namespace: credential}
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, r))
		require.NotContains(t, output.String(), credential)
		require.Contains(t, output.String(), "[REDACTED]")
	}
	var wide bytes.Buffer
	require.NoError(t, r.WideTable().Write(&wide))
	require.NotContains(t, wide.String(), credential)
	require.Contains(t, wide.String(), "[REDACTED]")
}
