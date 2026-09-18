package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestAutoscaleStatusEnumsAreClosedAndStable(t *testing.T) {
	got := []string{
		string(v1alpha1.AutoscaleSourceInferenceService),
		string(v1alpha1.AutoscaleStateReported), string(v1alpha1.AutoscaleStatePartial),
		string(v1alpha1.AutoscaleStateUnavailable), string(v1alpha1.AutoscaleStateInvalid),
		string(v1alpha1.AutoscaleComponentReported), string(v1alpha1.AutoscaleComponentPartial),
		string(v1alpha1.AutoscaleComponentNotReported), string(v1alpha1.AutoscaleComponentInvalid),
		string(v1alpha1.AutoscaleClassHPA), string(v1alpha1.AutoscaleClassKEDA),
		string(v1alpha1.AutoscaleClassExternal), string(v1alpha1.AutoscaleClassNone),
		string(v1alpha1.AutoscaleClassUnknown),
		string(v1alpha1.AutoscaleManagedByOME), string(v1alpha1.AutoscaleManagedByExternal),
		string(v1alpha1.AutoscaleManagedByNone), string(v1alpha1.AutoscaleManagedByUnknown),
		string(v1alpha1.AutoscaleSpecSourceISVC), string(v1alpha1.AutoscaleSpecSourcePolicy),
		string(v1alpha1.AutoscaleSpecSourceRuntime),
		string(v1alpha1.AutoscaleSpecSourceLegacy), string(v1alpha1.AutoscaleSpecSourceDefault),
		string(v1alpha1.AutoscaleSpecSourceUnknown),
		string(v1alpha1.AutoscaleTargetReported), string(v1alpha1.AutoscaleTargetNotReported),
		string(v1alpha1.AutoscaleTargetInvalid), string(v1alpha1.AutoscaleTargetDeployment),
		string(v1alpha1.AutoscaleTargetInferenceReplica),
		string(v1alpha1.AutoscaleReplicasReported), string(v1alpha1.AutoscaleReplicasAmbiguous),
		string(v1alpha1.AutoscaleReplicasNotReported), string(v1alpha1.AutoscaleReplicasUnavailable),
		string(v1alpha1.AutoscaleReplicasInvalid),
		string(v1alpha1.AutoscaleConditionAbleToScale), string(v1alpha1.AutoscaleConditionScalingActive),
		string(v1alpha1.AutoscaleConditionScalingLimited), string(v1alpha1.AutoscaleConditionReady),
		string(v1alpha1.AutoscaleConditionActive), string(v1alpha1.AutoscaleConditionFallback),
		string(v1alpha1.AutoscaleConditionPaused),
		string(v1alpha1.AutoscaleConditionTrue), string(v1alpha1.AutoscaleConditionFalse),
		string(v1alpha1.AutoscaleConditionUnknown),
		string(v1alpha1.AutoscaleConditionsReported), string(v1alpha1.AutoscaleConditionsNotReported),
		string(v1alpha1.AutoscaleConditionsUnavailable), string(v1alpha1.AutoscaleConditionsInvalid),
	}
	want := []string{
		"InferenceService",
		"Reported", "Partial", "Unavailable", "Invalid",
		"Reported", "Partial", "NotReported", "Invalid",
		"HPA", "KEDA", "External", "None", "Unknown",
		"ome", "external", "none", "Unknown",
		"isvc", "policy", "runtime", "legacy", "default", "Unknown",
		"Reported", "NotReported", "Invalid", "Deployment", "InferenceReplica",
		"Reported", "Ambiguous", "NotReported", "Unavailable", "Invalid",
		"AbleToScale", "ScalingActive", "ScalingLimited", "Ready", "Active", "Fallback", "Paused",
		"True", "False", "Unknown",
		"Reported", "NotReported", "Unavailable", "Invalid",
	}
	assert.Equal(t, want, got)
}

func TestAutoscaleStatusIssueAndWarningCodesAreStable(t *testing.T) {
	issues := []string{
		string(v1alpha1.AutoscaleIssueUnknownComponentStatus),
		string(v1alpha1.AutoscaleIssueAutoscalerNotReported),
		string(v1alpha1.AutoscaleIssueScaleTargetNotReported),
		string(v1alpha1.AutoscaleIssueClassInvalid),
		string(v1alpha1.AutoscaleIssueManagedByInvalid),
		string(v1alpha1.AutoscaleIssueOwnershipMismatch),
		string(v1alpha1.AutoscaleIssueSpecSourceInvalid),
		string(v1alpha1.AutoscaleIssueUnexpectedScalerEvidence),
		string(v1alpha1.AutoscaleIssueReplicaEvidenceAmbiguous),
		string(v1alpha1.AutoscaleIssueReplicaEvidenceInvalid),
		string(v1alpha1.AutoscaleIssueScaleTargetInvalid),
		string(v1alpha1.AutoscaleIssueConditionInvalid),
		string(v1alpha1.AutoscaleIssueConditionConflict),
	}
	assert.Equal(t, []string{
		"UnknownComponentStatus", "AutoscalerNotReported", "ScaleTargetNotReported",
		"ClassInvalid", "ManagedByInvalid", "OwnershipMismatch", "SpecSourceInvalid",
		"UnexpectedScalerEvidence", "ReplicaEvidenceAmbiguous", "ReplicaEvidenceInvalid",
		"ScaleTargetInvalid", "ConditionInvalid", "ConditionConflict",
	}, issues)

	warnings := []string{string(v1alpha1.AutoscaleWarningPartialData)}
	assert.Equal(t, []string{"PartialData"}, warnings)
}

func TestAutoscaleStatusSchemaIsTypedAndRedacted(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(v1alpha1.AutoscaleStatusReport{}),
		reflect.TypeOf(v1alpha1.AutoscaleSourceReference{}),
		reflect.TypeOf(v1alpha1.AutoscaleStatusContent{}),
		reflect.TypeOf(v1alpha1.AutoscaleComponentStatus{}),
		reflect.TypeOf(v1alpha1.AutoscaleTarget{}),
		reflect.TypeOf(v1alpha1.AutoscaleReplicaStatus{}),
		reflect.TypeOf(v1alpha1.AutoscaleConditionsStatus{}),
		reflect.TypeOf(v1alpha1.AutoscaleCondition{}),
		reflect.TypeOf(v1alpha1.AutoscaleIssue{}),
	} {
		assertAutoscaleSchema(t, typ, map[reflect.Type]bool{})
	}
	warningType := reflect.TypeOf(v1alpha1.AutoscaleWarning{})
	require.Equal(t, 1, warningType.NumField())
	assert.Equal(t, "Code", warningType.Field(0).Name)
	generation, ok := reflect.TypeOf(v1alpha1.AutoscaleSourceReference{}).FieldByName("Generation")
	require.True(t, ok)
	assert.Equal(t, "generation", generation.Tag.Get("json"), "source generation is required evidence, not optional freshness")
}

func TestNewAutoscaleStatusReportAcceptsDefaultSystemClock(t *testing.T) {
	before := time.Now().UTC()
	got := v1alpha1.NewAutoscaleStatusReport(
		v1alpha1.Metadata{Name: "chat"},
		v1alpha1.AutoscaleStatusContent{Components: []v1alpha1.AutoscaleComponentStatus{}, Issues: []v1alpha1.AutoscaleIssue{}},
		nil,
	)
	after := time.Now().UTC()

	assert.False(t, got.CollectedAt.Before(before))
	assert.False(t, got.CollectedAt.After(after))
	assert.Equal(t, time.UTC, got.CollectedAt.Location())
}

func TestAutoscaleStatusCanonicalizesWithoutMutatingCaller(t *testing.T) {
	collectedAt := time.Date(2026, time.August, 31, 11, 30, 0, 0, time.FixedZone("test", -7*60*60))
	lastScale := time.Date(2026, time.August, 31, 11, 20, 0, 0, time.FixedZone("test", -7*60*60))
	transition := time.Date(2026, time.August, 31, 11, 10, 0, 0, time.FixedZone("test", -7*60*60))
	current, desired := int32(2), int32(3)
	reportValue := v1alpha1.NewAutoscaleStatusReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.AutoscaleStatusContent{
			Summary: v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStatePartial},
			Components: []v1alpha1.AutoscaleComponentStatus{
				{Type: v1alpha1.RuntimeComponentRouter, State: v1alpha1.AutoscaleComponentNotReported,
					Class: v1alpha1.AutoscaleClassUnknown, ManagedBy: v1alpha1.AutoscaleManagedByUnknown,
					SpecSource: v1alpha1.AutoscaleSpecSourceUnknown,
					Target:     v1alpha1.AutoscaleTarget{State: v1alpha1.AutoscaleTargetNotReported},
					Replicas:   v1alpha1.AutoscaleReplicaStatus{State: v1alpha1.AutoscaleReplicasNotReported},
					Conditions: v1alpha1.AutoscaleConditionsStatus{State: v1alpha1.AutoscaleConditionsNotReported}},
				{Type: v1alpha1.RuntimeComponentEngine, State: v1alpha1.AutoscaleComponentReported,
					Class: v1alpha1.AutoscaleClassHPA, ManagedBy: v1alpha1.AutoscaleManagedByOME,
					SpecSource: v1alpha1.AutoscaleSpecSourceDefault,
					Target: v1alpha1.AutoscaleTarget{State: v1alpha1.AutoscaleTargetReported,
						APIVersion: "ome.io/v1beta1", Kind: v1alpha1.AutoscaleTargetInferenceReplica,
						Namespace: "prod", Name: "chat-engine"},
					Replicas: v1alpha1.AutoscaleReplicaStatus{State: v1alpha1.AutoscaleReplicasReported,
						CurrentReplicas: &current, DesiredReplicas: &desired, LastScaleTime: &lastScale},
					Conditions: v1alpha1.AutoscaleConditionsStatus{State: v1alpha1.AutoscaleConditionsReported, Items: []v1alpha1.AutoscaleCondition{
						{Type: v1alpha1.AutoscaleConditionScalingActive, Status: v1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition},
						{Type: v1alpha1.AutoscaleConditionAbleToScale, Status: v1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition},
						{Type: v1alpha1.AutoscaleConditionScalingActive, Status: v1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition},
					}}},
			},
			Issues: []v1alpha1.AutoscaleIssue{
				{Code: v1alpha1.AutoscaleIssueScaleTargetNotReported, Component: v1alpha1.RuntimeComponentRouter},
				{Code: v1alpha1.AutoscaleIssueAutoscalerNotReported, Component: v1alpha1.RuntimeComponentRouter},
			},
		},
		fixedClock{now: collectedAt},
	)
	reportValue.Sources = []v1alpha1.AutoscaleSourceReference{
		{Kind: v1alpha1.AutoscaleSourceInferenceService, Namespace: "prod", Name: "z", UID: "z", Generation: 8, Evidence: v1alpha1.EvidenceReported},
		{Kind: v1alpha1.AutoscaleSourceInferenceService, Namespace: "prod", Name: "chat", UID: "uid", Generation: 7, Evidence: v1alpha1.EvidenceReported},
	}
	reportValue.Warnings = []v1alpha1.AutoscaleWarning{{Code: v1alpha1.AutoscaleWarningPartialData}}

	canonical := reportValue.Canonical()

	assert.Equal(t, v1alpha1.APIVersion, canonical.APIVersion)
	assert.Equal(t, v1alpha1.AutoscaleStatusReportKind, canonical.Kind)
	assert.Equal(t, "2026-08-31T18:30:00Z", canonical.CollectedAt.Format(time.RFC3339))
	assert.Equal(t, "chat", canonical.Sources[0].Name)
	assert.Equal(t, canonical.CollectedAt, canonical.Sources[0].CollectedAt)
	assert.Equal(t, v1alpha1.RuntimeComponentEngine, canonical.Content.Components[0].Type)
	assert.Equal(t, int64(7), canonical.Sources[0].Generation)
	assert.Equal(t, []v1alpha1.AutoscaleCondition{
		{Type: v1alpha1.AutoscaleConditionAbleToScale, Status: v1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition.UTC()},
		{Type: v1alpha1.AutoscaleConditionScalingActive, Status: v1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition.UTC()},
	}, canonical.Content.Components[0].Conditions.Items)
	assert.Equal(t, time.UTC, canonical.Content.Components[0].Replicas.LastScaleTime.Location())
	assert.Equal(t, v1alpha1.AutoscaleIssueAutoscalerNotReported, canonical.Content.Issues[0].Code)
	assert.Equal(t, v1alpha1.AutoscaleWarningPartialData, canonical.Warnings[0].Code)

	*canonical.Content.Components[0].Replicas.CurrentReplicas = 99
	canonical.Content.Components[0].Conditions.Items[0].Status = v1alpha1.AutoscaleConditionFalse
	canonical.Sources[0].Name = "returned"
	assert.Equal(t, int32(2), *reportValue.Content.Components[0].Replicas.CurrentReplicas)
	assert.Equal(t, v1alpha1.AutoscaleConditionTrue, reportValue.Content.Components[0].Conditions.Items[0].Status)
	assert.Equal(t, "z", reportValue.Sources[0].Name)
}

func TestAutoscaleStatusCanonicalOrderingIsTotal(t *testing.T) {
	transitionA := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	transitionB := transitionA.Add(time.Second)
	componentA := v1alpha1.AutoscaleComponentStatus{
		Type: v1alpha1.RuntimeComponentEngine, State: v1alpha1.AutoscaleComponentReported,
		Class: v1alpha1.AutoscaleClassHPA, ManagedBy: v1alpha1.AutoscaleManagedByOME,
		SpecSource: v1alpha1.AutoscaleSpecSourceDefault,
		Target: v1alpha1.AutoscaleTarget{State: v1alpha1.AutoscaleTargetReported,
			APIVersion: "apps/v1", Kind: v1alpha1.AutoscaleTargetDeployment, Namespace: "prod", Name: "a"},
		Replicas: v1alpha1.AutoscaleReplicaStatus{State: v1alpha1.AutoscaleReplicasAmbiguous},
		Conditions: v1alpha1.AutoscaleConditionsStatus{State: v1alpha1.AutoscaleConditionsReported,
			Items: []v1alpha1.AutoscaleCondition{{Type: v1alpha1.AutoscaleConditionScalingActive,
				Status: v1alpha1.AutoscaleConditionTrue, LastTransitionTime: transitionA}}},
	}
	componentB := componentA
	componentB.Conditions.Items = []v1alpha1.AutoscaleCondition{{Type: v1alpha1.AutoscaleConditionScalingActive,
		Status: v1alpha1.AutoscaleConditionTrue, LastTransitionTime: transitionB}}
	left := v1alpha1.AutoscaleStatusContent{
		Components: []v1alpha1.AutoscaleComponentStatus{componentA, componentB},
	}.Canonical()
	right := v1alpha1.AutoscaleStatusContent{
		Components: []v1alpha1.AutoscaleComponentStatus{componentB, componentA},
	}.Canonical()
	assert.Equal(t, left, right)
}

func TestAutoscaleStatusCanonicalOrderingIncludesLiveScaler(t *testing.T) {
	componentA := v1alpha1.AutoscaleComponentStatus{
		Type: v1alpha1.RuntimeComponentEngine,
		LiveScaler: &v1alpha1.AutoscaleLiveScaler{
			Kind:     v1alpha1.AutoscaleClassHPA,
			Evidence: v1alpha1.AutoscaleLiveScalerForbidden,
		},
	}
	componentB := componentA
	componentB.LiveScaler = &v1alpha1.AutoscaleLiveScaler{
		Kind:     v1alpha1.AutoscaleClassHPA,
		Evidence: v1alpha1.AutoscaleLiveScalerNotFound,
	}
	left := v1alpha1.AutoscaleStatusContent{Components: []v1alpha1.AutoscaleComponentStatus{componentA, componentB}}.Canonical()
	right := v1alpha1.AutoscaleStatusContent{Components: []v1alpha1.AutoscaleComponentStatus{componentB, componentA}}.Canonical()
	assert.Equal(t, left, right)
}

func TestAutoscaleStatusTableUsesOnlyTypedReportedEvidence(t *testing.T) {
	reportValue := autoscaleStatusFixture()

	assert.Equal(t, report.Table{
		Headers: []string{"FIELD", "SERVICE", "ENGINE", "DECODER", "ROUTER"},
		Rows: [][]string{
			{"STATE", "Reported", "Reported", "-", "-"},
			{"CLASS", "-", "HPA", "-", "-"},
			{"MANAGED-BY", "-", "ome", "-", "-"},
			{"SPEC-SOURCE", "-", "default", "-", "-"},
			{"TARGET-KIND", "-", "IR", "-", "-"},
			{"TARGET-NAME", "-", "chat-engine", "-", "-"},
			{"TARGET-EVIDENCE", "-", "Reported", "-", "-"},
			{"CURRENT", "-", "2", "-", "-"},
			{"DESIRED", "-", "3", "-", "-"},
			{"REPLICA-EVIDENCE", "-", "Reported", "-", "-"},
			{"LAST-SCALE", "-", "Aug31 18:20Z", "-", "-"},
			{"COND-EVIDENCE", "-", "Reported", "-", "-"},
			{"ABLE-TO-SCALE", "-", "True", "-", "-"},
			{"SCALING-ACTIVE", "-", "True", "-", "-"},
		},
	}, reportValue.Table())

	assert.Equal(t, report.Table{
		Headers: []string{
			"STATE", "COMPONENT", "COMPONENT-STATE", "CLASS", "MANAGED-BY", "SPEC-SOURCE",
			"TARGET", "TARGET-EVIDENCE", "CURRENT", "DESIRED", "REPLICA-EVIDENCE", "LAST-SCALE", "CONDITION-EVIDENCE", "CONDITIONS", "ISSUES",
		},
		Rows: [][]string{{
			"Reported", "engine", "Reported", "HPA", "ome", "default",
			"InferenceReplica/prod/chat-engine", "Reported", "2", "3", "Reported", "2026-08-31T18:20:00Z",
			"Reported", "AbleToScale=True,ScalingActive=True", "-",
		}},
	}, reportValue.WideTable())

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))
	assert.Equal(t,
		"FIELD              SERVICE    ENGINE         DECODER   ROUTER\n"+
			"STATE              Reported   Reported       -         -\n"+
			"CLASS              -          HPA            -         -\n"+
			"MANAGED-BY         -          ome            -         -\n"+
			"SPEC-SOURCE        -          default        -         -\n"+
			"TARGET-KIND        -          IR             -         -\n"+
			"TARGET-NAME        -          chat-engine    -         -\n"+
			"TARGET-EVIDENCE    -          Reported       -         -\n"+
			"CURRENT            -          2              -         -\n"+
			"DESIRED            -          3              -         -\n"+
			"REPLICA-EVIDENCE   -          Reported       -         -\n"+
			"LAST-SCALE         -          Aug31 18:20Z   -         -\n"+
			"COND-EVIDENCE      -          Reported       -         -\n"+
			"ABLE-TO-SCALE      -          True           -         -\n"+
			"SCALING-ACTIVE     -          True           -         -\n",
		output.String())
}

func TestAutoscaleStatusTableSurfacesGlobalIssuesWithoutEchoingUnknownComponents(t *testing.T) {
	content := v1alpha1.AutoscaleStatusContent{
		Summary: v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStateInvalid},
		Components: []v1alpha1.AutoscaleComponentStatus{{
			Type: v1alpha1.RuntimeComponentEngine, State: v1alpha1.AutoscaleComponentPartial,
			Class: v1alpha1.AutoscaleClassHPA, ManagedBy: v1alpha1.AutoscaleManagedByOME,
			SpecSource: v1alpha1.AutoscaleSpecSourceDefault,
			Target:     v1alpha1.AutoscaleTarget{State: v1alpha1.AutoscaleTargetNotReported},
			Replicas:   v1alpha1.AutoscaleReplicaStatus{State: v1alpha1.AutoscaleReplicasAmbiguous},
			Conditions: v1alpha1.AutoscaleConditionsStatus{State: v1alpha1.AutoscaleConditionsNotReported, Items: []v1alpha1.AutoscaleCondition{}},
		}},
		Issues: []v1alpha1.AutoscaleIssue{
			{Code: v1alpha1.AutoscaleIssueUnknownComponentStatus},
			{Code: v1alpha1.AutoscaleIssueScaleTargetNotReported, Component: v1alpha1.RuntimeComponentEngine},
		},
	}

	table := content.Table()
	require.NotEmpty(t, table.Rows)
	issues := table.Rows[len(table.Rows)-1]
	assert.Equal(t, "ISSUES", issues[0])
	assert.Equal(t, "UnknownComp", issues[1])
	assert.Equal(t, "NoTarget", issues[2])
	assert.NotContains(t, strings.Join(issues, " "), "SECRET_COMPONENT")
}

func TestAutoscaleStatusCompactIssueAliasesAreDistinctAndBounded(t *testing.T) {
	tests := []struct {
		code v1alpha1.AutoscaleIssueCode
		want string
	}{
		{code: v1alpha1.AutoscaleIssueUnknownComponentStatus, want: "UnknownComp"},
		{code: v1alpha1.AutoscaleIssueAutoscalerNotReported, want: "NoAutoscaler"},
		{code: v1alpha1.AutoscaleIssueScaleTargetNotReported, want: "NoTarget"},
		{code: v1alpha1.AutoscaleIssueClassInvalid, want: "BadClass"},
		{code: v1alpha1.AutoscaleIssueManagedByInvalid, want: "BadManager"},
		{code: v1alpha1.AutoscaleIssueOwnershipMismatch, want: "OwnerMismatch"},
		{code: v1alpha1.AutoscaleIssueSpecSourceInvalid, want: "BadSpecSource"},
		{code: v1alpha1.AutoscaleIssueUnexpectedScalerEvidence, want: "UnexpectedEv"},
		{code: v1alpha1.AutoscaleIssueReplicaEvidenceAmbiguous, want: "ReplicaAmbig"},
		{code: v1alpha1.AutoscaleIssueReplicaEvidenceInvalid, want: "BadReplica"},
		{code: v1alpha1.AutoscaleIssueScaleTargetInvalid, want: "BadTarget"},
		{code: v1alpha1.AutoscaleIssueConditionInvalid, want: "BadCondition"},
		{code: v1alpha1.AutoscaleIssueConditionConflict, want: "CondConflict"},
	}

	seen := make(map[string]v1alpha1.AutoscaleIssueCode, len(tests))
	for _, test := range tests {
		t.Run(string(test.code), func(t *testing.T) {
			content := v1alpha1.AutoscaleStatusContent{
				Summary: v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStateInvalid},
				Issues:  []v1alpha1.AutoscaleIssue{{Code: test.code}},
			}

			table := content.Table()
			require.Len(t, table.Rows, 2)
			got := table.Rows[1][1]
			assert.Equal(t, test.want, got)
			assert.LessOrEqual(t, len(got), 13)
			if previous, found := seen[got]; found {
				t.Fatalf("compact alias %q collides for %q and %q", got, previous, test.code)
			}
			seen[got] = test.code
		})
	}
}

func TestAutoscaleStatusCompactUnknownIssueAliasesAreStableSafeAndDistinct(t *testing.T) {
	codes := []v1alpha1.AutoscaleIssueCode{
		"FutureIssueOne",
		"FutureIssueTwo",
		"future\n\u202eSECRET",
	}
	seen := make(map[string]v1alpha1.AutoscaleIssueCode, len(codes))
	for _, code := range codes {
		content := v1alpha1.AutoscaleStatusContent{
			Summary: v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStateInvalid},
			Issues:  []v1alpha1.AutoscaleIssue{{Code: code}},
		}

		first := content.Table().Rows[1][1]
		second := content.Table().Rows[1][1]
		assert.Equal(t, first, second)
		assert.Regexp(t, `^X#[0-9a-f]{10}$`, first)
		assert.NotContains(t, first, "Future")
		assert.NotContains(t, first, "SECRET")
		assert.LessOrEqual(t, len(first), 13)
		if previous, found := seen[first]; found {
			t.Fatalf("future issue alias %q collides for %q and %q", first, previous, code)
		}
		seen[first] = code
	}
}

func TestAutoscaleStatusCompactTableKeepsEveryIssueAliasVisible(t *testing.T) {
	content := v1alpha1.AutoscaleStatusContent{
		Summary: v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStateInvalid},
		Issues: []v1alpha1.AutoscaleIssue{
			{Code: v1alpha1.AutoscaleIssueScaleTargetNotReported},
			{Code: v1alpha1.AutoscaleIssueScaleTargetInvalid},
			{Code: v1alpha1.AutoscaleIssueReplicaEvidenceInvalid, Component: v1alpha1.RuntimeComponentEngine},
			{Code: v1alpha1.AutoscaleIssueReplicaEvidenceAmbiguous, Component: v1alpha1.RuntimeComponentEngine},
		},
	}

	rows := content.Table().Rows
	require.Len(t, rows, 3)
	assert.Equal(t, []string{"ISSUES", "BadTarget", "ReplicaAmbig", "-", "-"}, rows[1])
	assert.Equal(t, []string{"ISSUES", "NoTarget", "BadReplica", "-", "-"}, rows[2])
}

func TestAutoscaleStatusTableShowsUnavailableSummaryWithoutComponents(t *testing.T) {
	content := v1alpha1.AutoscaleStatusContent{
		Summary:    v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStateUnavailable},
		Components: []v1alpha1.AutoscaleComponentStatus{},
		Issues:     []v1alpha1.AutoscaleIssue{},
	}

	table := content.Table()
	assert.Equal(t, [][]string{{"STATE", "Unavailable", "-", "-", "-"}}, table.Rows)
}

func TestAutoscaleStatusCompactTableBoundsThreeComponents(t *testing.T) {
	reportValue := autoscaleStatusFixture()
	base := reportValue.Content.Components[0]
	reportValue.Content.Components = nil
	for _, componentType := range []v1alpha1.RuntimeComponentType{
		v1alpha1.RuntimeComponentEngine,
		v1alpha1.RuntimeComponentDecoder,
		v1alpha1.RuntimeComponentRouter,
	} {
		component := base
		component.Type = componentType
		component.Target.Name = strings.Repeat(string(componentType)+"-", 20) + "target"
		reportValue.Content.Components = append(reportValue.Content.Components, component)
		reportValue.Content.Issues = append(reportValue.Content.Issues, v1alpha1.AutoscaleIssue{
			Code: v1alpha1.AutoscaleIssueUnexpectedScalerEvidence, Component: componentType,
		})
	}

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, reportValue))
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, "line %q", line)
	}
	assert.NotContains(t, output.String(), strings.Repeat("engine-", 20))

	encoded, err := json.Marshal(reportValue.Canonical())
	require.NoError(t, err)
	assert.Contains(t, string(encoded), strings.Repeat("engine-", 20)+"target")
}

func TestAutoscaleStatusCompactTableCoversDeploymentAndConditionStates(t *testing.T) {
	content := v1alpha1.AutoscaleStatusContent{
		Summary: v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStatePartial},
		Components: []v1alpha1.AutoscaleComponentStatus{{
			Type:       v1alpha1.RuntimeComponentEngine,
			State:      v1alpha1.AutoscaleComponentPartial,
			Class:      v1alpha1.AutoscaleClassKEDA,
			ManagedBy:  v1alpha1.AutoscaleManagedByExternal,
			SpecSource: v1alpha1.AutoscaleSpecSourcePolicy,
			Target: v1alpha1.AutoscaleTarget{
				State: v1alpha1.AutoscaleTargetReported,
				Kind:  v1alpha1.AutoscaleTargetDeployment,
				Name:  "an-extremely-long-deployment-name",
			},
			Replicas: v1alpha1.AutoscaleReplicaStatus{State: v1alpha1.AutoscaleReplicasUnavailable},
			Conditions: v1alpha1.AutoscaleConditionsStatus{
				State: v1alpha1.AutoscaleConditionsReported,
				Items: []v1alpha1.AutoscaleCondition{
					{Type: v1alpha1.AutoscaleConditionScalingLimited, Status: v1alpha1.AutoscaleConditionFalse},
					{Type: v1alpha1.AutoscaleConditionReady, Status: v1alpha1.AutoscaleConditionTrue},
					{Type: v1alpha1.AutoscaleConditionActive, Status: v1alpha1.AutoscaleConditionUnknown},
					{Type: v1alpha1.AutoscaleConditionFallback, Status: v1alpha1.AutoscaleConditionFalse},
					{Type: v1alpha1.AutoscaleConditionPaused, Status: v1alpha1.AutoscaleConditionTrue},
				},
			},
		}},
	}

	rows := make(map[string][]string)
	for _, row := range content.Table().Rows {
		rows[row[0]] = row
	}
	assert.Equal(t, "Deployment", rows["TARGET-KIND"][2])
	assert.Equal(t, "an-ex...-name", rows["TARGET-NAME"][2])
	assert.Equal(t, "False", rows["SCALING-LIMITED"][2])
	assert.Equal(t, "True", rows["READY"][2])
	assert.Equal(t, "Unknown", rows["ACTIVE"][2])
	assert.Equal(t, "False", rows["FALLBACK"][2])
	assert.Equal(t, "True", rows["PAUSED"][2])
}

func TestAutoscaleStatusCompactTableDoesNotEchoUnknownComponentNames(t *testing.T) {
	content := v1alpha1.AutoscaleStatusContent{
		Summary: v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStateInvalid},
		Components: []v1alpha1.AutoscaleComponentStatus{{
			Type: v1alpha1.RuntimeComponentType("SECRET_COMPONENT"),
		}},
	}

	table := content.Table()
	require.Len(t, table.Rows, 2)
	assert.Equal(t, []string{"OTHER", "1 omitted", "-", "-", "-"}, table.Rows[1])
	assert.NotContains(t, strings.Join(table.Rows[1], " "), "SECRET_COMPONENT")
}

func autoscaleStatusFixture() v1alpha1.AutoscaleStatusReport {
	current, desired := int32(2), int32(3)
	lastScale := time.Date(2026, time.August, 31, 18, 20, 0, 0, time.UTC)
	transition := time.Date(2026, time.August, 31, 18, 15, 0, 0, time.UTC)
	result := v1alpha1.NewAutoscaleStatusReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.AutoscaleStatusContent{
			Summary: v1alpha1.AutoscaleSummary{State: v1alpha1.AutoscaleStateReported},
			Components: []v1alpha1.AutoscaleComponentStatus{{
				Type: v1alpha1.RuntimeComponentEngine, State: v1alpha1.AutoscaleComponentReported,
				Class: v1alpha1.AutoscaleClassHPA, ManagedBy: v1alpha1.AutoscaleManagedByOME,
				SpecSource: v1alpha1.AutoscaleSpecSourceDefault,
				Target: v1alpha1.AutoscaleTarget{State: v1alpha1.AutoscaleTargetReported,
					APIVersion: "ome.io/v1beta1", Kind: v1alpha1.AutoscaleTargetInferenceReplica,
					Namespace: "prod", Name: "chat-engine"},
				Replicas: v1alpha1.AutoscaleReplicaStatus{State: v1alpha1.AutoscaleReplicasReported,
					CurrentReplicas: &current, DesiredReplicas: &desired, LastScaleTime: &lastScale},
				Conditions: v1alpha1.AutoscaleConditionsStatus{State: v1alpha1.AutoscaleConditionsReported, Items: []v1alpha1.AutoscaleCondition{
					{Type: v1alpha1.AutoscaleConditionScalingActive, Status: v1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition},
					{Type: v1alpha1.AutoscaleConditionAbleToScale, Status: v1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition},
				}},
			}},
		},
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)
	result.Sources = []v1alpha1.AutoscaleSourceReference{{
		Kind: v1alpha1.AutoscaleSourceInferenceService, Namespace: "prod", Name: "chat",
		UID: "isvc-uid", Generation: 7, Evidence: v1alpha1.EvidenceReported,
		CollectedAt: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC),
	}}
	return result
}

func TestAutoscaleStatusMachineOutputIsExact(t *testing.T) {
	tests := []struct {
		name   string
		format report.Format
		want   string
	}{
		{name: "json", format: report.FormatJSON, want: `{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "AutoscaleStatusReport",
  "metadata": {
    "namespace": "prod",
    "name": "chat"
  },
  "collectedAt": "2026-08-31T18:30:00Z",
  "sources": [
    {
      "kind": "InferenceService",
      "namespace": "prod",
      "name": "chat",
      "uid": "isvc-uid",
      "generation": 7,
      "evidence": "Reported",
      "collectedAt": "2026-08-31T18:30:00Z"
    }
  ],
  "content": {
    "summary": {
      "state": "Reported"
    },
    "components": [
      {
        "type": "engine",
        "state": "Reported",
        "class": "HPA",
        "managedBy": "ome",
        "specSource": "default",
        "target": {
          "state": "Reported",
          "apiVersion": "ome.io/v1beta1",
          "kind": "InferenceReplica",
          "namespace": "prod",
          "name": "chat-engine"
        },
        "replicas": {
          "state": "Reported",
          "currentReplicas": 2,
          "desiredReplicas": 3,
          "lastScaleTime": "2026-08-31T18:20:00Z"
        },
        "conditions": {
          "state": "Reported",
          "items": [
            {
              "type": "AbleToScale",
              "status": "True",
              "lastTransitionTime": "2026-08-31T18:15:00Z"
            },
            {
              "type": "ScalingActive",
              "status": "True",
              "lastTransitionTime": "2026-08-31T18:15:00Z"
            }
          ]
        }
      }
    ],
    "issues": []
  },
  "warnings": []
}
`},
		{name: "yaml", format: report.FormatYAML, want: `apiVersion: cli.ome.io/v1alpha1
collectedAt: "2026-08-31T18:30:00Z"
content:
  components:
  - class: HPA
    conditions:
      items:
      - lastTransitionTime: "2026-08-31T18:15:00Z"
        status: "True"
        type: AbleToScale
      - lastTransitionTime: "2026-08-31T18:15:00Z"
        status: "True"
        type: ScalingActive
      state: Reported
    managedBy: ome
    replicas:
      currentReplicas: 2
      desiredReplicas: 3
      lastScaleTime: "2026-08-31T18:20:00Z"
      state: Reported
    specSource: default
    state: Reported
    target:
      apiVersion: ome.io/v1beta1
      kind: InferenceReplica
      name: chat-engine
      namespace: prod
      state: Reported
    type: engine
  issues: []
  summary:
    state: Reported
kind: AutoscaleStatusReport
metadata:
  name: chat
  namespace: prod
sources:
- collectedAt: "2026-08-31T18:30:00Z"
  evidence: Reported
  generation: 7
  kind: InferenceService
  name: chat
  namespace: prod
  uid: isvc-uid
warnings: []
`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			require.NoError(t, report.Write(&output, test.format, autoscaleStatusFixture()))
			assert.Equal(t, test.want, output.String())
		})
	}
}

func assertAutoscaleSchema(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.PkgPath() == "time" || seen[typ] {
		return
	}
	seen[typ] = true
	require.NotEqual(t, reflect.Interface, typ.Kind(), "schema contains interface field at %s", typ)
	require.NotEqual(t, reflect.Map, typ.Kind(), "schema contains map field at %s", typ)
	if typ.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.ToLower(field.Name + " " + strings.Split(field.Tag.Get("json"), ",")[0])
		for _, forbidden := range []string{
			"resourceversion", "annotation", "ownerreference", "managedfield", "observedgeneration",
			"trigger", "fallback", "metric", "behavior", "template", "reason", "message", "error",
			"freshness", "synctoken", "continuationtoken", "synchronizationtoken",
		} {
			assert.NotContains(t, name, forbidden, "unsafe field %s.%s", typ, field.Name)
		}
		assertAutoscaleSchema(t, field.Type, seen)
	}
}
