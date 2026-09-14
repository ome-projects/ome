package v1alpha1_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestRuntimeEffectiveCanonicalReturnsDeepOrderedCopy(t *testing.T) {
	content := runtimeEffectiveContent()
	wantOriginal := runtimeEffectiveContent()
	originalSelectionRuntime := content.Selection.Runtime
	originalLiveSource := content.Live.Source
	originalActiveRevision := content.Active.Revision

	got := content.Canonical()

	assert.Equal(t, []v1alpha1.RuntimeObjectReference{
		content.Inheritance.Sources[0], content.Inheritance.Sources[1],
	}, got.Inheritance.Sources, "inheritance remains root-first")
	assert.Equal(t, []v1alpha1.RuntimeComponentType{
		v1alpha1.RuntimeComponentEngine, v1alpha1.RuntimeComponentDecoder, v1alpha1.RuntimeComponentRouter,
	}, componentTypes(got.Live.Components))
	assert.Equal(t, []v1alpha1.RuntimeIssue{
		{Code: v1alpha1.RuntimeIssueRevisionHashMismatch, Revision: "revision-a"},
		{Code: v1alpha1.RuntimeIssueStatusStale},
	}, got.Issues)
	assert.Equal(t, time.UTC, got.Active.Revision.CreatedAt.Location())

	require.NotSame(t, originalLiveSource, got.Live.Source)
	require.NotSame(t, originalSelectionRuntime, got.Selection.Runtime)
	require.NotSame(t, originalActiveRevision, got.Active.Revision)
	got.Inheritance.Sources[0].Name = "returned-inheritance-source"
	got.Selection.Runtime.Name = "returned-selection-runtime"
	got.Live.Source.Name = "returned-live-source"
	got.Active.Revision.Name = "returned-active-revision"
	got.Live.Components[0].DeploymentMode = v1alpha1.DeploymentMode("ReturnedLiveMode")
	got.Active.Components[0].Type = v1alpha1.RuntimeComponentType("returned-active-component")
	got.Issues[0].Revision = "returned-effective-issue"

	assert.Equal(t, wantOriginal, content)
}

func TestRuntimeEffectiveCanonicalNormalizesNilSlices(t *testing.T) {
	got := (v1alpha1.RuntimeEffectiveContent{}).Canonical()

	assert.NotNil(t, got.Inheritance.Sources)
	assert.NotNil(t, got.Live.Components)
	assert.NotNil(t, got.Active.Components)
	assert.NotNil(t, got.Issues)
}

func TestNewRuntimeEffectiveReportUsesFixedKindAndUTCClock(t *testing.T) {
	now := time.Date(2026, time.August, 31, 11, 30, 0, 0, time.FixedZone("test", -7*60*60))

	got := v1alpha1.NewRuntimeEffectiveReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RuntimeEffectiveContent{},
		fixedClock{now: now},
	)

	assert.Equal(t, v1alpha1.RuntimeEffectiveReportKind, got.Kind)
	assert.Equal(t, "2026-08-31T18:30:00Z", got.CollectedAt.Format(time.RFC3339))
	assert.NotNil(t, got.Content.Inheritance.Sources)
	assert.NotNil(t, got.Content.Live.Components)
	assert.NotNil(t, got.Content.Active.Components)
	assert.NotNil(t, got.Content.Issues)
}

func TestRuntimeEffectiveTableContract(t *testing.T) {
	reportValue := v1alpha1.NewRuntimeEffectiveReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		runtimeEffectiveContent(),
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)

	assert.Equal(t, report.Table{
		Headers: []string{"SCOPE", "FIELD", "VALUE"},
		Rows: [][]string{
			{"Live", "STATE", "Available"},
			{"Live", "RUNTIME", "SR/prod/vllm"},
			{"Live", "HASH", "11223344"},
			{"Live", "ENGINE", "RawDeployment (Default)"},
			{"Live", "DECODER", "MultiNode (LeaderWorkerShape)"},
			{"Live", "ROUTER", "VirtualDeployment (ServiceSpec)"},
			{"Active", "STATE", "Available"},
			{"Active", "RUNTIME", "CSR/cluster-vllm"},
			{"Active", "REVISION", "revision-a"},
			{"Active", "HASH", "aabbccdd"},
			{"Active", "ENGINE", "OMENative (ComponentAnnotation)"},
			{"Service", "PIN", "ManagedPin/Resolved"},
			{"Service", "SYNC", "Pending"},
			{"Service", "STATUS", "Stale"},
			{"Service", "DRIFT", "ReportedTrue/PinAdvanced"},
			{"Service", "LIVE-RELATION", "Different"},
			{"Service", "ISSUE", "RevisionHashMismatch(revision-a)"},
			{"Service", "ISSUE", "StatusStale"},
		},
	}, reportValue.Table())

	assert.Equal(t, report.Table{
		Headers: []string{
			"VIEW", "STATE", "REASON", "RUNTIME", "REVISION", "HASH", "COMPONENT", "MODE", "MODE-SOURCE",
			"PIN", "PIN-STATE", "SYNC", "STATUS", "DRIFT", "LIVE-RELATION", "ISSUES",
		},
		Rows: [][]string{
			{"Live", "Available", "-", "ServingRuntime/prod/vllm", "-", "11223344", "engine", "RawDeployment", "Default", "ManagedPin", "Resolved", "Pending", "Stale", "ReportedTrue/PinAdvanced", "Different", "RevisionHashMismatch(revision-a),StatusStale"},
			{"Live", "Available", "-", "ServingRuntime/prod/vllm", "-", "11223344", "decoder", "MultiNode", "LeaderWorkerShape", "ManagedPin", "Resolved", "Pending", "Stale", "ReportedTrue/PinAdvanced", "Different", "RevisionHashMismatch(revision-a),StatusStale"},
			{"Live", "Available", "-", "ServingRuntime/prod/vllm", "-", "11223344", "router", "VirtualDeployment", "ServiceSpec", "ManagedPin", "Resolved", "Pending", "Stale", "ReportedTrue/PinAdvanced", "Different", "RevisionHashMismatch(revision-a),StatusStale"},
			{"Active", "Available", "-", "ClusterServingRuntime/cluster-vllm", "revision-a", "aabbccdd", "engine", "OMENative", "ComponentAnnotation", "ManagedPin", "Resolved", "Pending", "Stale", "ReportedTrue/PinAdvanced", "Different", "RevisionHashMismatch(revision-a),StatusStale"},
		},
	}, reportValue.Content.WideTable())

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))
	assert.Contains(t, output.String(), "SCOPE")
	assert.Contains(t, output.String(), "RevisionHashMismatch(revision-a)")
}

func TestRuntimeEffectiveTableUsesDashRowForConfigurationWithoutComponents(t *testing.T) {
	content := v1alpha1.RuntimeEffectiveContent{
		Pin: v1alpha1.RuntimePin{
			Mode:      v1alpha1.RuntimePinModeAutoSync,
			State:     v1alpha1.RuntimePinStateNotApplicable,
			SyncState: v1alpha1.RuntimeSyncStateAbsent,
			Status:    v1alpha1.RuntimeStatusObservation{Freshness: v1alpha1.StatusFreshnessUnobserved},
		},
		Live: v1alpha1.RuntimeConfiguration{State: v1alpha1.ConfigurationStateUnavailable},
		Active: v1alpha1.RuntimeConfiguration{
			State:             v1alpha1.ConfigurationStateUnavailable,
			UnavailableReason: v1alpha1.UnavailableNotFound,
		},
	}

	assert.Equal(t, [][]string{
		{"Live", "STATE", "Unavailable"},
		{"Active", "STATE", "Unavailable"},
		{"Active", "REASON", "NotFound"},
		{"Service", "PIN", "AutoSync/NotApplicable"},
		{"Service", "SYNC", "Absent"},
		{"Service", "STATUS", "Unobserved"},
	}, content.Table().Rows)
}

func TestRuntimeEffectiveCompactTableBoundsAndDisambiguatesIdentities(t *testing.T) {
	liveName := "aa-" + strings.Repeat("x", 120) + "-shared-tail"
	activeName := "aa-" + strings.Repeat("y", 120) + "-shared-tail"
	namespace := "team-" + strings.Repeat("n", 60) + "-shared-tail"
	live := runtimeObject(v1alpha1.RuntimeKindServingRuntime, namespace, liveName)
	active := runtimeObject(v1alpha1.RuntimeKindServingRuntime, namespace, activeName)
	reportValue := v1alpha1.NewRuntimeEffectiveReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RuntimeEffectiveContent{
			Live: v1alpha1.RuntimeConfiguration{
				State: v1alpha1.ConfigurationStateAvailable, Source: &live,
				Components: []v1alpha1.RuntimeComponent{{
					Type:                 v1alpha1.RuntimeComponentEngine,
					DeploymentMode:       v1alpha1.DeploymentMode(strings.Repeat("界", 40)),
					DeploymentModeSource: v1alpha1.DeploymentModeSourceDefault,
				}},
			},
			Active: v1alpha1.RuntimeConfiguration{
				State: v1alpha1.ConfigurationStateAvailable, Source: &active,
			},
			LiveToActive: v1alpha1.RuntimeHashRelationDifferent,
		},
		fixedClock{},
	)

	table := reportValue.Table()
	var identities []string
	for _, row := range table.Rows {
		if row[1] == "RUNTIME" {
			identities = append(identities, row[2])
			assert.Equal(t, 2, strings.Count(row[2], "/"), row[2])
			assert.Equal(t, 2, strings.Count(row[2], "#"), row[2])
		}
	}
	require.Len(t, identities, 2)
	assert.NotEqual(t, identities[0], identities[1])

	var compact bytes.Buffer
	require.NoError(t, report.Write(&compact, report.FormatTable, reportValue))
	for _, line := range strings.Split(strings.TrimSuffix(compact.String(), "\n"), "\n") {
		assert.LessOrEqual(t, runtimeEffectiveFixtureWidth(line), 80, "line %q", line)
	}
	assert.NotContains(t, compact.String(), liveName)
	assert.NotContains(t, compact.String(), activeName)

	var wide bytes.Buffer
	require.NoError(t, reportValue.Content.WideTable().Write(&wide))
	assert.Contains(t, wide.String(), liveName)
	assert.Contains(t, wide.String(), activeName)

	var structured bytes.Buffer
	require.NoError(t, report.Write(&structured, report.FormatJSON, reportValue))
	assert.Contains(t, structured.String(), liveName)
	assert.Contains(t, structured.String(), activeName)
	assert.Contains(t, structured.String(), namespace)
}

// TestRuntimeEffectiveCompactIdentityRowsDisambiguateBeyondSafetyClip catches
// display tags being derived after a 1024-column safety clip. Each pair differs
// only in the discarded middle, so only a complete-identity hash can keep the
// compact runtime and revision rows distinct.
func TestRuntimeEffectiveCompactIdentityRowsDisambiguateBeyondSafetyClip(t *testing.T) {
	prefix := strings.Repeat("a", 511)
	suffix := strings.Repeat("z", 510)
	firstName := prefix + "first-distinct-middle" + suffix
	secondName := prefix + "second-distinct-middle" + suffix
	firstRuntime := runtimeObject(v1alpha1.RuntimeKindServingRuntime, "prod", firstName)
	secondRuntime := runtimeObject(v1alpha1.RuntimeKindServingRuntime, "prod", secondName)
	tests := []struct {
		name    string
		field   string
		content v1alpha1.RuntimeEffectiveContent
	}{
		{
			name:  "runtime rows",
			field: "RUNTIME",
			content: v1alpha1.RuntimeEffectiveContent{
				Live:   v1alpha1.RuntimeConfiguration{Source: &firstRuntime},
				Active: v1alpha1.RuntimeConfiguration{Source: &secondRuntime},
			},
		},
		{
			name:  "revision rows",
			field: "REVISION",
			content: v1alpha1.RuntimeEffectiveContent{
				Live: v1alpha1.RuntimeConfiguration{Revision: &v1alpha1.RuntimeRevisionReference{
					Namespace: "ome", Name: firstName,
				}},
				Active: v1alpha1.RuntimeConfiguration{Revision: &v1alpha1.RuntimeRevisionReference{
					Namespace: "ome", Name: secondName,
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			firstTable := tt.content.Table()
			secondTable := tt.content.Table()
			var identities []string
			for _, row := range firstTable.Rows {
				if row[1] == tt.field {
					identities = append(identities, row[2])
					assert.LessOrEqual(t, runtimeEffectiveFixtureWidth(row[2]), 54)
				}
			}
			require.Len(t, identities, 2)
			assert.NotEqual(t, identities[0], identities[1])
			assert.Equal(t, firstTable, secondTable)

			var compact bytes.Buffer
			require.NoError(t, firstTable.Write(&compact))
			for _, line := range strings.Split(strings.TrimSuffix(compact.String(), "\n"), "\n") {
				assert.LessOrEqual(t, runtimeEffectiveFixtureWidth(line), 80, "line %q", line)
			}

			var wide bytes.Buffer
			require.NoError(t, tt.content.WideTable().Write(&wide))
			assert.Contains(t, wide.String(), firstName)
			assert.Contains(t, wide.String(), secondName)

			for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
				var structured bytes.Buffer
				require.NoError(t, report.Write(&structured, format, tt.content))
				assert.Contains(t, structured.String(), firstName)
				assert.Contains(t, structured.String(), secondName)
			}
		})
	}
}

func TestRuntimeEffectiveCompactTableHandlesPartialAndUnknownValues(t *testing.T) {
	unknown := runtimeObject(v1alpha1.RuntimeKindUnknown, "team-a", "runtime")
	longRevision := "revision-" + strings.Repeat("x", 120)
	content := v1alpha1.RuntimeEffectiveContent{
		Live: v1alpha1.RuntimeConfiguration{
			Source: &unknown,
			Components: []v1alpha1.RuntimeComponent{
				{Type: v1alpha1.RuntimeComponentEngine, DeploymentMode: v1alpha1.DeploymentModeRawDeployment},
				{Type: v1alpha1.RuntimeComponentDecoder, DeploymentModeSource: v1alpha1.DeploymentModeSourceDefault},
				{Type: v1alpha1.RuntimeComponentRouter},
				{Type: v1alpha1.RuntimeComponentType("SECRET_COMPONENT")},
			},
		},
		Active: v1alpha1.RuntimeConfiguration{Revision: &v1alpha1.RuntimeRevisionReference{
			Namespace: "ome", Name: longRevision,
		}},
		Issues: []v1alpha1.RuntimeIssue{
			{Code: v1alpha1.RuntimeIssueStatusStale, Revision: longRevision},
			{Code: v1alpha1.RuntimeIssueCode("ExtremelyLongFutureIssueCode" + strings.Repeat("x", 60))},
		},
	}

	table := content.Table()
	rows := make(map[string][]string)
	var issues []string
	for _, row := range table.Rows {
		key := row[0] + "/" + row[1]
		rows[key] = row
		if key == "Service/ISSUE" {
			issues = append(issues, row[2])
		}
	}
	assert.Equal(t, "Unknown/team-a/runtime", rows["Live/RUNTIME"][2])
	assert.Equal(t, "RawDeployment", rows["Live/ENGINE"][2])
	assert.Equal(t, "Default", rows["Live/DECODER"][2])
	assert.Equal(t, "-", rows["Live/ROUTER"][2])
	assert.Equal(t, "Unsupported component omitted", rows["Live/OTHER"][2])
	assert.NotContains(t, strings.Join(rows["Live/OTHER"], " "), "SECRET_COMPONENT")
	assert.Contains(t, rows["Active/REVISION"][2], "#")
	require.Len(t, issues, 2)
	assert.Contains(t, strings.Join(issues, " "), "#")
	for _, issue := range issues {
		assert.LessOrEqual(t, runtimeEffectiveFixtureWidth(issue), 54)
	}

	var structured bytes.Buffer
	require.NoError(t, report.Write(&structured, report.FormatJSON, content))
	assert.Contains(t, structured.String(), "SECRET_COMPONENT")
	assert.Contains(t, structured.String(), longRevision)
}

func runtimeEffectiveFixtureWidth(value string) int {
	width := 0
	for _, char := range value {
		if char == '界' {
			width += 2
			continue
		}
		width++
	}
	return width
}

func TestRuntimeEffectiveSelectionAllowsMissingOrUnknownRuntimeIdentity(t *testing.T) {
	missing := v1alpha1.NewRuntimeEffectiveReport(
		v1alpha1.Metadata{Name: "chat"},
		v1alpha1.RuntimeEffectiveContent{
			Selection: v1alpha1.RuntimeSelection{Source: v1alpha1.RuntimeSelectionSourceSelected},
		},
		fixedClock{},
	)
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatJSON, missing))
	assert.Contains(t, output.String(), "\"selection\": {\n      \"source\": \"Selected\"\n    }")
	assert.NotContains(t, output.String(), `"kind": ""`)

	unknown := runtimeObject(v1alpha1.RuntimeKindUnknown, "", "unresolved")
	content := v1alpha1.RuntimeEffectiveContent{
		Selection: v1alpha1.RuntimeSelection{Source: v1alpha1.RuntimeSelectionSourceExplicit, Runtime: &unknown},
	}
	output.Reset()
	require.NoError(t, report.Write(&output, report.FormatJSON, content))
	assert.Contains(t, output.String(), `"kind": "Unknown"`)
}

func TestRuntimeEffectiveMachineOutputContract(t *testing.T) {
	reportValue := v1alpha1.NewRuntimeEffectiveReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		runtimeEffectiveMachineContent(),
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)
	tests := []struct {
		name   string
		format report.Format
		want   string
	}{
		{
			name:   "json",
			format: report.FormatJSON,
			want: "{\n" +
				`  "apiVersion": "cli.ome.io/v1alpha1",` + "\n" +
				`  "kind": "RuntimeEffectiveReport",` + "\n" +
				`  "metadata": {` + "\n" +
				`    "namespace": "prod",` + "\n" +
				`    "name": "chat"` + "\n" +
				"  },\n" +
				`  "collectedAt": "2026-08-31T18:30:00Z",` + "\n" +
				`  "sources": [],` + "\n" +
				`  "content": {` + "\n" +
				`    "selection": {` + "\n" +
				`      "source": "Explicit",` + "\n" +
				`      "runtime": {` + "\n" +
				`        "apiVersion": "ome.io/v1beta1",` + "\n" +
				`        "kind": "ServingRuntime",` + "\n" +
				`        "namespace": "prod",` + "\n" +
				`        "name": "vllm"` + "\n" +
				"      }\n" +
				"    },\n" +
				`    "inheritance": {` + "\n" +
				`      "state": "Unavailable",` + "\n" +
				`      "sources": [],` + "\n" +
				`      "unavailableReason": "NotFound"` + "\n" +
				"    },\n" +
				`    "pin": {` + "\n" +
				`      "mode": "ManagedPin",` + "\n" +
				`      "state": "RevisionMissing",` + "\n" +
				`      "reportedRevision": "revision-a",` + "\n" +
				`      "status": {` + "\n" +
				`        "generation": 7,` + "\n" +
				`        "observedGeneration": 7,` + "\n" +
				`        "freshness": "Current"` + "\n" +
				"      },\n" +
				`      "reportedDrift": {` + "\n" +
				`        "state": "ReportedFalse"` + "\n" +
				"      },\n" +
				`      "syncState": "Acknowledged"` + "\n" +
				"    },\n" +
				`    "live": {` + "\n" +
				`      "state": "Available",` + "\n" +
				`      "origin": "LiveRuntime",` + "\n" +
				`      "source": {` + "\n" +
				`        "apiVersion": "ome.io/v1beta1",` + "\n" +
				`        "kind": "ServingRuntime",` + "\n" +
				`        "namespace": "prod",` + "\n" +
				`        "name": "vllm"` + "\n" +
				"      },\n" +
				`      "hash": "11223344",` + "\n" +
				`      "components": [` + "\n" +
				"        {\n" +
				`          "type": "engine",` + "\n" +
				`          "deploymentMode": "RawDeployment",` + "\n" +
				`          "deploymentModeSource": "Default"` + "\n" +
				"        }\n" +
				"      ]\n" +
				"    },\n" +
				`    "active": {` + "\n" +
				`      "state": "Unavailable",` + "\n" +
				`      "components": [],` + "\n" +
				`      "unavailableReason": "NotFound"` + "\n" +
				"    },\n" +
				`    "liveToActive": "Unknown",` + "\n" +
				`    "issues": [` + "\n" +
				"      {\n" +
				`        "code": "ActiveRevisionUnavailable",` + "\n" +
				`        "revision": "revision-a"` + "\n" +
				"      }\n" +
				"    ]\n" +
				"  },\n" +
				`  "warnings": []` + "\n" +
				"}\n",
		},
		{
			name:   "yaml",
			format: report.FormatYAML,
			want: "apiVersion: cli.ome.io/v1alpha1\n" +
				"collectedAt: \"2026-08-31T18:30:00Z\"\n" +
				"content:\n" +
				"  active:\n" +
				"    components: []\n" +
				"    state: Unavailable\n" +
				"    unavailableReason: NotFound\n" +
				"  inheritance:\n" +
				"    sources: []\n" +
				"    state: Unavailable\n" +
				"    unavailableReason: NotFound\n" +
				"  issues:\n" +
				"  - code: ActiveRevisionUnavailable\n" +
				"    revision: revision-a\n" +
				"  live:\n" +
				"    components:\n" +
				"    - deploymentMode: RawDeployment\n" +
				"      deploymentModeSource: Default\n" +
				"      type: engine\n" +
				"    hash: \"11223344\"\n" +
				"    origin: LiveRuntime\n" +
				"    source:\n" +
				"      apiVersion: ome.io/v1beta1\n" +
				"      kind: ServingRuntime\n" +
				"      name: vllm\n" +
				"      namespace: prod\n" +
				"    state: Available\n" +
				"  liveToActive: Unknown\n" +
				"  pin:\n" +
				"    mode: ManagedPin\n" +
				"    reportedDrift:\n" +
				"      state: ReportedFalse\n" +
				"    reportedRevision: revision-a\n" +
				"    state: RevisionMissing\n" +
				"    status:\n" +
				"      freshness: Current\n" +
				"      generation: 7\n" +
				"      observedGeneration: 7\n" +
				"    syncState: Acknowledged\n" +
				"  selection:\n" +
				"    runtime:\n" +
				"      apiVersion: ome.io/v1beta1\n" +
				"      kind: ServingRuntime\n" +
				"      name: vllm\n" +
				"      namespace: prod\n" +
				"    source: Explicit\n" +
				"kind: RuntimeEffectiveReport\n" +
				"metadata:\n" +
				"  name: chat\n" +
				"  namespace: prod\n" +
				"sources: []\n" +
				"warnings: []\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			require.NoError(t, report.Write(&output, tt.format, reportValue))
			assert.Equal(t, tt.want, output.String())
		})
	}
}

func TestRuntimeEffectiveSchemaIsStrictlyAllowlisted(t *testing.T) {
	assertRuntimeReportSchema(t, reflect.TypeOf(v1alpha1.RuntimeEnvelope[v1alpha1.RuntimeEffectiveContent]{}), map[reflect.Type]bool{})
}

func runtimeEffectiveContent() v1alpha1.RuntimeEffectiveContent {
	live := runtimeObject(v1alpha1.RuntimeKindServingRuntime, "prod", "vllm")
	active := runtimeObject(v1alpha1.RuntimeKindClusterServingRuntime, "", "cluster-vllm")
	return v1alpha1.RuntimeEffectiveContent{
		Selection: v1alpha1.RuntimeSelection{Source: v1alpha1.RuntimeSelectionSourceExplicit, Runtime: &live},
		Inheritance: v1alpha1.RuntimeInheritance{
			State: v1alpha1.InheritanceStateObserved,
			Sources: []v1alpha1.RuntimeObjectReference{
				runtimeObject(v1alpha1.RuntimeKindClusterServingRuntime, "", "cluster-base"),
				live,
			},
		},
		Pin: v1alpha1.RuntimePin{
			Mode:              v1alpha1.RuntimePinModeManagedPin,
			State:             v1alpha1.RuntimePinStateResolved,
			RequestedRevision: "revision-b",
			ReportedRevision:  "revision-a",
			Status: v1alpha1.RuntimeStatusObservation{
				Generation: 7, ObservedGeneration: 6, Freshness: v1alpha1.StatusFreshnessStale,
			},
			ReportedDrift: v1alpha1.RuntimeDriftObservation{
				State: v1alpha1.DriftConditionStateReportedTrue, Cause: v1alpha1.RuntimeDriftCausePinAdvanced,
			},
			SyncState: v1alpha1.RuntimeSyncStatePending,
		},
		Live: v1alpha1.RuntimeConfiguration{
			State: v1alpha1.ConfigurationStateAvailable, Origin: v1alpha1.ConfigurationOriginLiveRuntime,
			Source: &live, Hash: "11223344",
			Components: []v1alpha1.RuntimeComponent{
				{Type: v1alpha1.RuntimeComponentRouter, DeploymentMode: v1alpha1.DeploymentModeVirtualDeployment, DeploymentModeSource: v1alpha1.DeploymentModeSourceServiceSpec},
				{Type: v1alpha1.RuntimeComponentEngine, DeploymentMode: v1alpha1.DeploymentModeRawDeployment, DeploymentModeSource: v1alpha1.DeploymentModeSourceDefault},
				{Type: v1alpha1.RuntimeComponentDecoder, DeploymentMode: v1alpha1.DeploymentModeMultiNode, DeploymentModeSource: v1alpha1.DeploymentModeSourceLeaderWorkerShape},
			},
		},
		Active: v1alpha1.RuntimeConfiguration{
			State: v1alpha1.ConfigurationStateAvailable, Origin: v1alpha1.ConfigurationOriginControllerRevision,
			Source: &active,
			Revision: &v1alpha1.RuntimeRevisionReference{
				Namespace: "ome", Name: "revision-a", CreatedAt: runtimeReportTime(time.Date(2026, time.August, 31, 11, 0, 0, 0, time.FixedZone("test", -7*60*60))),
			},
			Hash: "aabbccdd",
			Components: []v1alpha1.RuntimeComponent{
				{Type: v1alpha1.RuntimeComponentEngine, DeploymentMode: v1alpha1.DeploymentModeOMENative, DeploymentModeSource: v1alpha1.DeploymentModeSourceComponentAnnotation},
			},
		},
		LiveToActive: v1alpha1.RuntimeHashRelationDifferent,
		Issues: []v1alpha1.RuntimeIssue{
			{Code: v1alpha1.RuntimeIssueStatusStale},
			{Code: v1alpha1.RuntimeIssueRevisionHashMismatch, Revision: "revision-a"},
		},
	}
}

func runtimeEffectiveMachineContent() v1alpha1.RuntimeEffectiveContent {
	live := runtimeObject(v1alpha1.RuntimeKindServingRuntime, "prod", "vllm")
	return v1alpha1.RuntimeEffectiveContent{
		Selection:   v1alpha1.RuntimeSelection{Source: v1alpha1.RuntimeSelectionSourceExplicit, Runtime: &live},
		Inheritance: v1alpha1.RuntimeInheritance{State: v1alpha1.InheritanceStateUnavailable, UnavailableReason: v1alpha1.UnavailableNotFound},
		Pin: v1alpha1.RuntimePin{
			Mode: v1alpha1.RuntimePinModeManagedPin, State: v1alpha1.RuntimePinStateRevisionMissing, ReportedRevision: "revision-a",
			Status:        v1alpha1.RuntimeStatusObservation{Generation: 7, ObservedGeneration: 7, Freshness: v1alpha1.StatusFreshnessCurrent},
			ReportedDrift: v1alpha1.RuntimeDriftObservation{State: v1alpha1.DriftConditionStateReportedFalse},
			SyncState:     v1alpha1.RuntimeSyncStateAcknowledged,
		},
		Live: v1alpha1.RuntimeConfiguration{
			State: v1alpha1.ConfigurationStateAvailable, Origin: v1alpha1.ConfigurationOriginLiveRuntime,
			Source: &live, Hash: "11223344",
			Components: []v1alpha1.RuntimeComponent{{
				Type: v1alpha1.RuntimeComponentEngine, DeploymentMode: v1alpha1.DeploymentModeRawDeployment, DeploymentModeSource: v1alpha1.DeploymentModeSourceDefault,
			}},
		},
		Active:       v1alpha1.RuntimeConfiguration{State: v1alpha1.ConfigurationStateUnavailable, UnavailableReason: v1alpha1.UnavailableNotFound},
		LiveToActive: v1alpha1.RuntimeHashRelationUnknown,
		Issues: []v1alpha1.RuntimeIssue{{
			Code: v1alpha1.RuntimeIssueActiveRevisionUnavailable, Revision: "revision-a",
		}},
	}
}

func runtimeObject(kind v1alpha1.RuntimeKind, namespace, name string) v1alpha1.RuntimeObjectReference {
	return v1alpha1.RuntimeObjectReference{
		APIVersion: "ome.io/v1beta1", Kind: kind, Namespace: namespace, Name: name,
	}
}

func componentTypes(components []v1alpha1.RuntimeComponent) []v1alpha1.RuntimeComponentType {
	result := make([]v1alpha1.RuntimeComponentType, 0, len(components))
	for _, component := range components {
		result = append(result, component.Type)
	}
	return result
}
