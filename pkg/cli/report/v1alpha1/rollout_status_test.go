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
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestRolloutStatusReportEnumValuesAreStable(t *testing.T) {
	got := []string{
		string(v1alpha1.RolloutSourceInferenceService),
		string(v1alpha1.RolloutStateUnknown), string(v1alpha1.RolloutStateNotConfigured),
		string(v1alpha1.RolloutStateSucceeded), string(v1alpha1.RolloutStateInProgress),
		string(v1alpha1.RolloutStatePaused), string(v1alpha1.RolloutStateStaged),
		string(v1alpha1.RolloutStateFailed), string(v1alpha1.RolloutStateRollingBack),
		string(v1alpha1.RolloutStateRolledBack),
		string(v1alpha1.RolloutEpochNotApplicable), string(v1alpha1.RolloutEpochUnverifiable),
		string(v1alpha1.RolloutStrategyUnknown), string(v1alpha1.RolloutStrategyIndependent),
		string(v1alpha1.RolloutStrategyCanary), string(v1alpha1.RolloutStrategyBlueGreen),
		string(v1alpha1.RolloutStrategyRollingUpdate), string(v1alpha1.RolloutStrategySequential),
		string(v1alpha1.RolloutPhaseUnknown), string(v1alpha1.RolloutPhaseStable),
		string(v1alpha1.RolloutPhaseCanarying), string(v1alpha1.RolloutPhaseBlueGreenStandby),
		string(v1alpha1.RolloutPhasePending), string(v1alpha1.RolloutPhasePaused),
		string(v1alpha1.RolloutPhasePromoting), string(v1alpha1.RolloutPhaseRollingBack),
		string(v1alpha1.RolloutPhaseRolledBack), string(v1alpha1.RolloutPhaseFailed),
		string(v1alpha1.RolloutPhaseIdle), string(v1alpha1.RolloutPhaseSurging),
		string(v1alpha1.RolloutPhaseWaiting), string(v1alpha1.RolloutPhaseShifting),
		string(v1alpha1.RolloutPhaseUpdating),
		string(v1alpha1.RolloutPhaseDraining), string(v1alpha1.RolloutPhaseScalingDown),
		string(v1alpha1.RolloutPhaseStaged), string(v1alpha1.RolloutPhaseAwaitingNextComponent),
		string(v1alpha1.RolloutGateUnknown), string(v1alpha1.RolloutGateImmediate),
		string(v1alpha1.RolloutGateManual), string(v1alpha1.RolloutGateTimed),
		string(v1alpha1.RolloutGateAnalysis),
		string(v1alpha1.RolloutAnalysisUnobserved), string(v1alpha1.RolloutAnalysisPassing),
		string(v1alpha1.RolloutAnalysisFailing), string(v1alpha1.RolloutAnalysisInconclusive),
		string(v1alpha1.RolloutConditionNotApplicable), string(v1alpha1.RolloutConditionUnobserved),
		string(v1alpha1.RolloutConditionTrue), string(v1alpha1.RolloutConditionFalse),
		string(v1alpha1.RolloutConditionUnknown), string(v1alpha1.RolloutConditionInvalid),
		string(v1alpha1.RolloutRevisionCurrent), string(v1alpha1.RolloutRevisionTarget),
		string(v1alpha1.RolloutRevisionPrevious), string(v1alpha1.RolloutRevisionOther),
	}
	want := []string{
		"InferenceService",
		"Unknown", "NotConfigured", "Succeeded", "InProgress", "Paused", "Staged", "Failed", "RollingBack", "RolledBack",
		"NotApplicable", "Unverifiable",
		"Unknown", "Independent", "Canary", "BlueGreen", "RollingUpdate", "Sequential",
		"Unknown", "Stable", "Canarying", "BlueGreenStandby", "Pending", "Paused", "Promoting", "RollingBack", "RolledBack", "Failed",
		"Idle", "Surging", "Waiting", "Shifting", "Updating", "Draining", "ScalingDown", "Staged", "AwaitingNextComponent",
		"Unknown", "Immediate", "Manual", "Timed", "Analysis",
		"Unobserved", "Passing", "Failing", "Inconclusive",
		"NotApplicable", "Unobserved", "True", "False", "Unknown", "Invalid",
		"Current", "Target", "Previous", "Other",
	}
	assert.Equal(t, want, got)
}

func TestRolloutStatusIssueCodesAreStable(t *testing.T) {
	got := []string{
		string(v1alpha1.RolloutIssueSpecMalformed), string(v1alpha1.RolloutIssueStatusMalformed),
		string(v1alpha1.RolloutIssueGroupStatusMissing),
		string(v1alpha1.RolloutIssueGroupStatusUnexpected), string(v1alpha1.RolloutIssueComponentStatusMissing),
		string(v1alpha1.RolloutIssueCanaryStatusMissing), string(v1alpha1.RolloutIssueCanaryStatusUnexpected),
		string(v1alpha1.RolloutIssueCanaryStepInvalid), string(v1alpha1.RolloutIssueRevisionNameInvalid),
		string(v1alpha1.RolloutIssueTrafficInvalid), string(v1alpha1.RolloutIssueAnalysisInconclusive),
		string(v1alpha1.RolloutIssueEpochUnverifiable),
	}
	assert.Equal(t, []string{
		"SpecMalformed", "StatusMalformed", "GroupStatusMissing", "GroupStatusUnexpected",
		"ComponentStatusMissing", "CanaryStatusMissing",
		"CanaryStatusUnexpected", "CanaryStepInvalid", "RevisionNameInvalid", "TrafficInvalid",
		"AnalysisInconclusive", "EpochUnverifiable",
	}, got)
}

func TestRolloutStatusReportSchemaIsTypedAndAllowlisted(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(v1alpha1.RolloutStatusReport{}),
		reflect.TypeOf(v1alpha1.RolloutSourceReference{}),
		reflect.TypeOf(v1alpha1.RolloutStatusContent{}),
		reflect.TypeOf(v1alpha1.RolloutGroupStatus{}),
		reflect.TypeOf(v1alpha1.RolloutComponentStatus{}),
		reflect.TypeOf(v1alpha1.RolloutIssue{}),
	} {
		assertRuntimeReportSchema(t, typ, map[reflect.Type]bool{})
	}
	warningType := reflect.TypeOf(v1alpha1.RolloutWarning{})
	require.Equal(t, 1, warningType.NumField())
	assert.Equal(t, "Code", warningType.Field(0).Name)
}

func TestNewRolloutStatusReportBuildsCanonicalTypedReport(t *testing.T) {
	collectedAt := time.Date(2026, time.August, 31, 11, 30, 0, 0, time.FixedZone("test", -7*60*60))
	transitionedAt := time.Date(2026, time.August, 31, 11, 20, 0, 0, time.FixedZone("test", -7*60*60))
	groupIndex := 0
	reportValue := v1alpha1.NewRolloutStatusReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutStatusContent{
			Summary: v1alpha1.RolloutSummary{
				State:             v1alpha1.RolloutStateUnknown,
				ReportedState:     v1alpha1.RolloutStateInProgress,
				Evidence:          v1alpha1.EvidenceReported,
				Epoch:             v1alpha1.RolloutEpochUnverifiable,
				CoordinationReady: v1alpha1.RolloutConditionFalse,
			},
			Groups: []v1alpha1.RolloutGroupStatus{{
				Index: 0, Strategy: v1alpha1.RolloutStrategyCanary, Phase: v1alpha1.RolloutPhaseCanarying,
				Components:         []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentEngine},
				StableRevisionHash: "aaaaaaaa", TargetRevisionHash: "bbbbbbbb",
				Step: &v1alpha1.RolloutStepStatus{
					Index: 1, Total: 3, Capacity: "50%", TargetTraffic: 20, ObservedTraffic: 20,
					Gate: v1alpha1.RolloutGateManual, EnteredAt: &transitionedAt,
				},
			}},
			Components: []v1alpha1.RolloutComponentStatus{{
				Type: v1alpha1.RuntimeComponentEngine, Strategy: v1alpha1.RolloutStrategyCanary,
				Group:                 &groupIndex,
				Phase:                 v1alpha1.RolloutPhaseCanarying,
				RolledOutRevisionHash: "aaaaaaaa", ReadyRevisionHash: "bbbbbbbb",
				Traffic: []v1alpha1.RolloutTrafficTarget{
					{RevisionHash: "bbbbbbbb", Percent: 20, Role: v1alpha1.RolloutRevisionTarget},
					{RevisionHash: "aaaaaaaa", Percent: 80, Role: v1alpha1.RolloutRevisionCurrent},
				},
			}},
			Issues: []v1alpha1.RolloutIssue{
				{Code: v1alpha1.RolloutIssueAnalysisInconclusive, Group: &groupIndex},
				{Code: v1alpha1.RolloutIssueTrafficInvalid},
			},
		},
		fixedClock{now: collectedAt},
	)
	reportValue.Sources = []v1alpha1.RolloutSourceReference{{
		Kind: "InferenceService", Namespace: "prod", Name: "chat", UID: "uid-1", Generation: 7,
		Evidence: v1alpha1.EvidenceObserved,
	}}
	reportValue.Warnings = []v1alpha1.RolloutWarning{{Code: v1alpha1.WarningPartialData}}

	canonical := reportValue.Canonical()

	assert.Equal(t, v1alpha1.APIVersion, canonical.APIVersion)
	assert.Equal(t, v1alpha1.RolloutStatusReportKind, canonical.Kind)
	assert.Equal(t, "2026-08-31T18:30:00Z", canonical.CollectedAt.Format(time.RFC3339))
	require.Len(t, canonical.Sources, 1)
	assert.Equal(t, canonical.CollectedAt, canonical.Sources[0].CollectedAt)
	assert.Equal(t, []v1alpha1.RolloutWarning{{Code: v1alpha1.WarningPartialData}}, canonical.Warnings)
	assert.Equal(t, []v1alpha1.RolloutTrafficTarget{
		{RevisionHash: "aaaaaaaa", Percent: 80, Role: v1alpha1.RolloutRevisionCurrent},
		{RevisionHash: "bbbbbbbb", Percent: 20, Role: v1alpha1.RolloutRevisionTarget},
	}, canonical.Content.Components[0].Traffic)
	assert.Equal(t, time.UTC, canonical.Content.Groups[0].Step.EnteredAt.Location())
	assert.Equal(t, v1alpha1.RolloutIssueAnalysisInconclusive, canonical.Content.Issues[0].Code)
	assert.Equal(t, v1alpha1.RolloutIssueTrafficInvalid, canonical.Content.Issues[1].Code)

	canonical.Content.Groups[0].Components[0] = v1alpha1.RuntimeComponentDecoder
	canonical.Content.Groups[0].Step.Capacity = "returned"
	canonical.Content.Components[0].Traffic[0].RevisionHash = "returned"
	*canonical.Content.Components[0].Group = 9
	*canonical.Content.Issues[0].Group = 9
	assert.Equal(t, v1alpha1.RuntimeComponentEngine, reportValue.Content.Groups[0].Components[0])
	assert.Equal(t, "50%", reportValue.Content.Groups[0].Step.Capacity)
	assert.Equal(t, "aaaaaaaa", reportValue.Content.Components[0].Traffic[0].RevisionHash)
	assert.Equal(t, 0, *reportValue.Content.Components[0].Group)
	assert.Equal(t, 0, *reportValue.Content.Issues[0].Group)
}

func TestRolloutStatusReportCanonicalBoundsEveryEnumField(t *testing.T) {
	group := 0
	reportValue := v1alpha1.RolloutStatusReport{
		Metadata:    v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		CollectedAt: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC),
		Sources: []v1alpha1.RolloutSourceReference{{
			Kind: v1alpha1.RolloutSourceKind("SECRET_SOURCE_KIND"),
			Name: "chat", Evidence: v1alpha1.EvidenceLevel("SECRET_SOURCE_EVIDENCE"),
		}},
		Content: v1alpha1.RolloutStatusContent{
			Summary: v1alpha1.RolloutSummary{
				State:             v1alpha1.RolloutState("SECRET_STATE"),
				ReportedState:     v1alpha1.RolloutState("SECRET_REPORTED_STATE"),
				Evidence:          v1alpha1.EvidenceLevel("SECRET_SUMMARY_EVIDENCE"),
				Epoch:             v1alpha1.RolloutEpochState("SECRET_EPOCH"),
				CoordinationReady: v1alpha1.RolloutConditionState("SECRET_CONDITION"),
			},
			Groups: []v1alpha1.RolloutGroupStatus{{
				Index:             group,
				Strategy:          v1alpha1.RolloutStrategy("SECRET_GROUP_STRATEGY"),
				Phase:             v1alpha1.RolloutPhase("SECRET_GROUP_PHASE"),
				Components:        []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentType("SECRET_GROUP_COMPONENT")},
				CurrentComponent:  v1alpha1.RuntimeComponentType("SECRET_CURRENT_COMPONENT"),
				PreviousComponent: v1alpha1.RuntimeComponentType("SECRET_PREVIOUS_COMPONENT"),
				Step: &v1alpha1.RolloutStepStatus{
					Gate:     v1alpha1.RolloutGate("SECRET_GATE"),
					Analysis: v1alpha1.RolloutAnalysisState("SECRET_ANALYSIS"),
				},
			}},
			Components: []v1alpha1.RolloutComponentStatus{
				{
					Type:     v1alpha1.RuntimeComponentEngine,
					Strategy: v1alpha1.RolloutStrategy("SECRET_COMPONENT_STRATEGY"),
					Group:    &group,
					Phase:    v1alpha1.RolloutPhase("SECRET_COMPONENT_PHASE"),
					Traffic: []v1alpha1.RolloutTrafficTarget{{
						RevisionHash: "aaaaaaaa", Percent: 100,
						Role: v1alpha1.RolloutRevisionRole("SECRET_REVISION_ROLE"),
					}},
				},
				{
					Type:     v1alpha1.RuntimeComponentType("SECRET_COMPONENT_TYPE"),
					Strategy: v1alpha1.RolloutStrategyIndependent,
					Phase:    v1alpha1.RolloutPhaseStable,
				},
			},
			Issues: []v1alpha1.RolloutIssue{{
				Code:      v1alpha1.RolloutIssueCode("SECRET_ISSUE_CODE"),
				Component: v1alpha1.RuntimeComponentType("SECRET_ISSUE_COMPONENT"),
			}},
		},
		Warnings: []v1alpha1.RolloutWarning{{Code: v1alpha1.WarningCode("SECRET_WARNING_CODE")}},
	}

	canonical := reportValue.Canonical()

	require.Len(t, canonical.Sources, 1)
	assert.Equal(t, v1alpha1.RolloutSourceInferenceService, canonical.Sources[0].Kind)
	assert.Equal(t, v1alpha1.EvidenceUnavailable, canonical.Sources[0].Evidence)
	assert.Equal(t, []v1alpha1.RolloutWarning{{Code: v1alpha1.WarningPartialData}}, canonical.Warnings)
	assert.Equal(t, v1alpha1.RolloutSummary{
		State:             v1alpha1.RolloutStateUnknown,
		ReportedState:     v1alpha1.RolloutStateUnknown,
		Evidence:          v1alpha1.EvidenceUnavailable,
		Epoch:             v1alpha1.RolloutEpochUnverifiable,
		CoordinationReady: v1alpha1.RolloutConditionInvalid,
	}, canonical.Content.Summary)
	require.Len(t, canonical.Content.Groups, 1)
	assert.Equal(t, v1alpha1.RolloutStrategyUnknown, canonical.Content.Groups[0].Strategy)
	assert.Equal(t, v1alpha1.RolloutPhaseUnknown, canonical.Content.Groups[0].Phase)
	assert.Empty(t, canonical.Content.Groups[0].Components)
	assert.Empty(t, canonical.Content.Groups[0].CurrentComponent)
	assert.Empty(t, canonical.Content.Groups[0].PreviousComponent)
	require.NotNil(t, canonical.Content.Groups[0].Step)
	assert.Equal(t, v1alpha1.RolloutGateUnknown, canonical.Content.Groups[0].Step.Gate)
	assert.Equal(t, v1alpha1.RolloutAnalysisUnobserved, canonical.Content.Groups[0].Step.Analysis)
	require.Len(t, canonical.Content.Components, 1)
	assert.Equal(t, v1alpha1.RolloutStrategyUnknown, canonical.Content.Components[0].Strategy)
	assert.Equal(t, v1alpha1.RolloutPhaseUnknown, canonical.Content.Components[0].Phase)
	require.Len(t, canonical.Content.Components[0].Traffic, 1)
	assert.Equal(t, v1alpha1.RolloutRevisionOther, canonical.Content.Components[0].Traffic[0].Role)
	assert.Equal(t, []v1alpha1.RolloutIssue{{
		Code: v1alpha1.RolloutIssueStatusMalformed,
	}}, canonical.Content.Issues)

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		t.Run(string(format), func(t *testing.T) {
			var output bytes.Buffer
			require.NoError(t, report.Write(&output, format, reportValue))
			assert.NotContains(t, output.String(), "SECRET_")
		})
	}
}

func TestRolloutStatusReportCanonicalPreservesAbsentOptionalAnalysis(t *testing.T) {
	reportValue := v1alpha1.NewRolloutStatusReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutStatusContent{
			Groups: []v1alpha1.RolloutGroupStatus{{
				Strategy: v1alpha1.RolloutStrategyCanary,
				Phase:    v1alpha1.RolloutPhaseCanarying,
				Step:     &v1alpha1.RolloutStepStatus{Gate: v1alpha1.RolloutGateImmediate},
			}},
		},
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)

	require.Len(t, reportValue.Content.Groups, 1)
	require.NotNil(t, reportValue.Content.Groups[0].Step)
	assert.Empty(t, reportValue.Content.Groups[0].Step.Analysis)
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatJSON, reportValue))
	assert.NotContains(t, output.String(), `"analysis"`)
}

func TestRolloutStatusReportWideTableUsesTypedContent(t *testing.T) {
	reportValue := rolloutStatusFixture()

	assert.Equal(t, report.Table{
		Headers: []string{
			"STATE", "REPORTED-STATE", "EVIDENCE", "EPOCH", "GROUP", "STRATEGY", "GROUP-PHASE",
			"CURRENT-COMPONENT", "PREVIOUS-COMPONENT", "COMPONENT", "COMPONENT-PHASE",
			"STEP", "GATE", "CAPACITY", "TARGET-TRAFFIC", "OBSERVED-TRAFFIC", "ROLLED-OUT", "READY", "PREVIOUS", "ISSUES",
		},
		Rows: [][]string{{
			"Unknown", "InProgress", "Reported", "Unverifiable", "0", "Canary", "Canarying", "-", "-", "engine", "Canarying",
			"2/3", "Manual", "50%", "20%", "20%", "aaaaaaaa", "bbbbbbbb", "-", "-",
		}}}, reportValue.WideTable())

	var output bytes.Buffer
	require.NoError(t, reportValue.WideTable().Write(&output))
	assert.Equal(t,
		"STATE     REPORTED-STATE   EVIDENCE   EPOCH          GROUP   STRATEGY   GROUP-PHASE   CURRENT-COMPONENT   PREVIOUS-COMPONENT   COMPONENT   COMPONENT-PHASE   STEP   GATE     CAPACITY   TARGET-TRAFFIC   OBSERVED-TRAFFIC   ROLLED-OUT   READY      PREVIOUS   ISSUES\n"+
			"Unknown   InProgress       Reported   Unverifiable   0       Canary     Canarying     -                   -                    engine      Canarying         2/3    Manual   50%        20%              20%                aaaaaaaa     bbbbbbbb   -          -\n",
		output.String())
}

func TestRolloutStatusReportTableUsesCompactComponentMatrix(t *testing.T) {
	reportValue := rolloutStatusFixture()

	assert.Equal(t, report.Table{
		Headers: []string{"FIELD", "SERVICE", "ENGINE", "DECODER", "ROUTER"},
		Rows: [][]string{
			{"STATE", "Unknown", "-", "-", "-"},
			{"REPORTED", "InProgress", "-", "-", "-"},
			{"EVIDENCE", "Reported", "-", "-", "-"},
			{"EPOCH", "Unverifiable", "-", "-", "-"},
			{"COORDINATION", "NotApplicable", "-", "-", "-"},
			{"GROUP", "-", "0", "-", "-"},
			{"STRATEGY", "-", "Canary", "-", "-"},
			{"GROUP-PHASE", "-", "Canarying", "-", "-"},
			{"PHASE", "-", "Canarying", "-", "-"},
			{"STEP", "-", "2/3", "-", "-"},
			{"GATE", "-", "Manual", "-", "-"},
			{"CAPACITY", "-", "50%", "-", "-"},
			{"TRAFFIC", "-", "20% -> 20%", "-", "-"},
			{"ROLLED-OUT", "-", "aaaaaaaa", "-", "-"},
			{"READY", "-", "bbbbbbbb", "-", "-"},
		},
	}, reportValue.Table())
}

func TestRolloutStatusReportTableAlignsAllComponentsAndListsIssuesOnce(t *testing.T) {
	groupZero, groupOne := 0, 1
	content := v1alpha1.RolloutStatusContent{
		Summary: v1alpha1.RolloutSummary{
			State:             v1alpha1.RolloutStateInProgress,
			ReportedState:     v1alpha1.RolloutStateInProgress,
			Evidence:          v1alpha1.EvidenceReported,
			Epoch:             v1alpha1.RolloutEpochUnverifiable,
			CoordinationReady: v1alpha1.RolloutConditionTrue,
		},
		Groups: []v1alpha1.RolloutGroupStatus{
			{
				Index: groupZero, Strategy: v1alpha1.RolloutStrategyCanary,
				Phase: v1alpha1.RolloutPhaseCanarying,
				Step: &v1alpha1.RolloutStepStatus{
					Index: 0, Total: 2, Capacity: "25%", TargetTraffic: 10,
					ObservedTraffic: 5, Gate: v1alpha1.RolloutGateManual,
				},
			},
			{
				Index: groupOne, Strategy: v1alpha1.RolloutStrategySequential,
				Phase:             v1alpha1.RolloutPhaseAwaitingNextComponent,
				CurrentComponent:  v1alpha1.RuntimeComponentRouter,
				PreviousComponent: v1alpha1.RuntimeComponentDecoder,
			},
		},
		Components: []v1alpha1.RolloutComponentStatus{
			{
				Type: v1alpha1.RuntimeComponentEngine, Strategy: v1alpha1.RolloutStrategyCanary,
				Group: &groupZero, Phase: v1alpha1.RolloutPhaseCanarying,
				RolledOutRevisionHash: "aaaaaaaa",
			},
			{
				Type: v1alpha1.RuntimeComponentDecoder, Strategy: v1alpha1.RolloutStrategySequential,
				Group: &groupOne, Phase: v1alpha1.RolloutPhaseStable,
				ReadyRevisionHash: "bbbbbbbb",
			},
			{
				Type: v1alpha1.RuntimeComponentRouter, Strategy: v1alpha1.RolloutStrategySequential,
				Group: &groupOne, Phase: v1alpha1.RolloutPhaseWaiting,
				PreviousRevisionHash: "cccccccc",
			},
		},
		Issues: []v1alpha1.RolloutIssue{
			{Code: v1alpha1.RolloutIssueEpochUnverifiable},
			{Code: v1alpha1.RolloutIssueTrafficInvalid, Group: &groupZero, Component: v1alpha1.RuntimeComponentEngine},
		},
	}

	table := content.Table()
	require.Equal(t, []string{"FIELD", "SERVICE", "ENGINE", "DECODER", "ROUTER"}, table.Headers)
	rows := make(map[string][]string, len(table.Rows))
	for _, row := range table.Rows {
		rows[row[0]] = row
	}
	assert.Equal(t, []string{"GROUP", "-", "0", "1", "1"}, rows["GROUP"])
	assert.Equal(t, []string{"STRATEGY", "-", "Canary", "Sequential", "Sequential"}, rows["STRATEGY"])
	assert.Equal(t, []string{"STEP", "-", "1/2", "-", "-"}, rows["STEP"])
	assert.Equal(t, []string{"TRAFFIC", "-", "10% -> 5%", "-", "-"}, rows["TRAFFIC"])
	assert.Equal(t, []string{"GROUP-CURRENT", "-", "-", "router", "router"}, rows["GROUP-CURRENT"])
	assert.Equal(t, []string{"ROLLED-OUT", "-", "aaaaaaaa", "-", "-"}, rows["ROLLED-OUT"])
	assert.Equal(t, []string{"READY", "-", "-", "bbbbbbbb", "-"}, rows["READY"])
	assert.Equal(t, []string{"PREVIOUS", "-", "-", "-", "cccccccc"}, rows["PREVIOUS"])
	require.Contains(t, rows, "ISSUES")
	assert.Equal(t, "EpochUnverifiable", rows["ISSUES"][1])
	assert.Equal(t, "TrafficInvalid(group=0)", rows["ISSUES"][2])
	assert.Equal(t, 1, strings.Count(strings.Join(rows["ISSUES"], ","), "EpochUnverifiable"))
	assert.Equal(t, 1, strings.Count(strings.Join(rows["ISSUES"], ","), "TrafficInvalid"))
}

func TestRolloutStatusReportComponentStrategyIsAuthoritativeAcrossFormats(t *testing.T) {
	tests := []struct {
		name     string
		strategy v1alpha1.RolloutStrategy
		want     v1alpha1.RolloutStrategy
	}{
		{name: "missing becomes bounded unknown", want: v1alpha1.RolloutStrategyUnknown},
		{name: "valid mismatch remains component value", strategy: v1alpha1.RolloutStrategyRollingUpdate, want: v1alpha1.RolloutStrategyRollingUpdate},
		{name: "invalid becomes bounded unknown", strategy: v1alpha1.RolloutStrategy("SECRET_STRATEGY"), want: v1alpha1.RolloutStrategyUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			group := 0
			reportValue := v1alpha1.NewRolloutStatusReport(
				v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
				v1alpha1.RolloutStatusContent{
					Summary: v1alpha1.RolloutSummary{
						State:             v1alpha1.RolloutStateUnknown,
						ReportedState:     v1alpha1.RolloutStateUnknown,
						Evidence:          v1alpha1.EvidenceReported,
						Epoch:             v1alpha1.RolloutEpochUnverifiable,
						CoordinationReady: v1alpha1.RolloutConditionUnobserved,
					},
					Groups: []v1alpha1.RolloutGroupStatus{{
						Index: group, Strategy: v1alpha1.RolloutStrategyCanary,
						Phase: v1alpha1.RolloutPhaseUnknown,
					}},
					Components: []v1alpha1.RolloutComponentStatus{{
						Type: v1alpha1.RuntimeComponentEngine, Strategy: tt.strategy,
						Group: &group, Phase: v1alpha1.RolloutPhaseUnknown,
					}},
				},
				fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
			)

			table := reportValue.WideTable()
			require.Len(t, table.Rows, 1)
			assert.Equal(t, string(tt.want), table.Rows[0][5])

			for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
				t.Run(string(format), func(t *testing.T) {
					var output bytes.Buffer
					require.NoError(t, report.Write(&output, format, reportValue))
					var decoded v1alpha1.RolloutStatusReport
					if format == report.FormatJSON {
						require.NoError(t, json.Unmarshal(output.Bytes(), &decoded))
					} else {
						require.NoError(t, yaml.Unmarshal(output.Bytes(), &decoded))
					}
					require.Len(t, decoded.Content.Components, 1)
					assert.Equal(t, tt.want, decoded.Content.Components[0].Strategy)
					assert.Equal(t, v1alpha1.RolloutStrategyCanary, decoded.Content.Groups[0].Strategy)
				})
			}
		})
	}
}

func TestRolloutStatusReportTableScopesIssuesToMatchingRows(t *testing.T) {
	groupZero, groupOne := 0, 1
	content := v1alpha1.RolloutStatusContent{
		Summary: v1alpha1.RolloutSummary{
			State:             v1alpha1.RolloutStateUnknown,
			ReportedState:     v1alpha1.RolloutStateInProgress,
			Evidence:          v1alpha1.EvidenceReported,
			Epoch:             v1alpha1.RolloutEpochUnverifiable,
			CoordinationReady: v1alpha1.RolloutConditionFalse,
		},
		Groups: []v1alpha1.RolloutGroupStatus{
			{Index: groupZero, Strategy: v1alpha1.RolloutStrategyCanary, Phase: v1alpha1.RolloutPhaseCanarying},
			{Index: groupOne, Strategy: v1alpha1.RolloutStrategyRollingUpdate, Phase: v1alpha1.RolloutPhaseSurging},
		},
		Components: []v1alpha1.RolloutComponentStatus{
			{Type: v1alpha1.RuntimeComponentEngine, Strategy: v1alpha1.RolloutStrategyCanary, Group: &groupZero, Phase: v1alpha1.RolloutPhaseCanarying},
			{Type: v1alpha1.RuntimeComponentDecoder, Strategy: v1alpha1.RolloutStrategyCanary, Group: &groupZero, Phase: v1alpha1.RolloutPhaseCanarying},
			{Type: v1alpha1.RuntimeComponentRouter, Strategy: v1alpha1.RolloutStrategyRollingUpdate, Group: &groupOne, Phase: v1alpha1.RolloutPhaseSurging},
			{Type: v1alpha1.RuntimeComponentEngine, Strategy: v1alpha1.RolloutStrategyIndependent, Phase: v1alpha1.RolloutPhaseUpdating},
		},
		Issues: []v1alpha1.RolloutIssue{
			{Code: v1alpha1.RolloutIssueSpecMalformed},
			{Code: v1alpha1.RolloutIssueGroupStatusMissing, Group: &groupZero},
			{Code: v1alpha1.RolloutIssueStatusMalformed, Group: &groupOne},
			{Code: v1alpha1.RolloutIssueRevisionNameInvalid, Component: v1alpha1.RuntimeComponentEngine},
			{Code: v1alpha1.RolloutIssueTrafficInvalid, Group: &groupZero, Component: v1alpha1.RuntimeComponentDecoder},
			{Code: v1alpha1.RolloutIssueCanaryStepInvalid, Group: &groupOne, Component: v1alpha1.RuntimeComponentEngine},
		},
	}
	original := content.Canonical()

	table := content.WideTable()
	reversed := content
	reversed.Components = append([]v1alpha1.RolloutComponentStatus{}, content.Components...)
	reversed.Issues = append([]v1alpha1.RolloutIssue{}, content.Issues...)
	for left, right := 0, len(reversed.Components)-1; left < right; left, right = left+1, right-1 {
		reversed.Components[left], reversed.Components[right] = reversed.Components[right], reversed.Components[left]
	}
	for left, right := 0, len(reversed.Issues)-1; left < right; left, right = left+1, right-1 {
		reversed.Issues[left], reversed.Issues[right] = reversed.Issues[right], reversed.Issues[left]
	}
	assert.Equal(t, table, reversed.WideTable(), "canonical table ordering must not depend on input order")
	assert.Equal(t, original, content.Canonical(), "Table must not mutate its input")

	issuesByRow := map[string]string{}
	for _, row := range table.Rows {
		issuesByRow[row[4]+"/"+row[9]] = row[19]
	}
	assert.Equal(t, map[string]string{
		"-/-":       "CanaryStepInvalid(group=1,component=engine)",
		"0/engine":  "GroupStatusMissing(group=0),RevisionNameInvalid(component=engine),SpecMalformed",
		"-/engine":  "RevisionNameInvalid(component=engine),SpecMalformed",
		"0/decoder": "GroupStatusMissing(group=0),SpecMalformed,TrafficInvalid(group=0,component=decoder)",
		"1/router":  "SpecMalformed,StatusMalformed(group=1)",
	}, issuesByRow)
}

func TestRolloutStatusReportTableRetainsUnmatchedScopedIssues(t *testing.T) {
	groupZero, groupSeven, groupNine := 0, 7, 9
	content := v1alpha1.RolloutStatusContent{
		Summary: v1alpha1.RolloutSummary{
			State:             v1alpha1.RolloutStateUnknown,
			ReportedState:     v1alpha1.RolloutStateUnknown,
			Evidence:          v1alpha1.EvidenceReported,
			Epoch:             v1alpha1.RolloutEpochUnverifiable,
			CoordinationReady: v1alpha1.RolloutConditionUnobserved,
		},
		Groups: []v1alpha1.RolloutGroupStatus{{
			Index: groupZero, Strategy: v1alpha1.RolloutStrategyCanary,
			Phase: v1alpha1.RolloutPhaseCanarying,
		}},
		Components: []v1alpha1.RolloutComponentStatus{
			{Type: v1alpha1.RuntimeComponentEngine, Strategy: v1alpha1.RolloutStrategyCanary, Group: &groupZero, Phase: v1alpha1.RolloutPhaseCanarying},
			{Type: v1alpha1.RuntimeComponentDecoder, Strategy: v1alpha1.RolloutStrategyIndependent, Phase: v1alpha1.RolloutPhaseStable},
		},
		Issues: []v1alpha1.RolloutIssue{
			{Code: v1alpha1.RolloutIssueSpecMalformed},
			{Code: v1alpha1.RolloutIssueStatusMalformed, Group: &groupZero},
			{Code: v1alpha1.RolloutIssueGroupStatusMissing, Group: &groupNine},
			{Code: v1alpha1.RolloutIssueRevisionNameInvalid, Component: v1alpha1.RuntimeComponentRouter},
			{Code: v1alpha1.RolloutIssueTrafficInvalid, Group: &groupSeven, Component: v1alpha1.RuntimeComponentDecoder},
		},
	}
	before, err := json.Marshal(content)
	require.NoError(t, err)

	table := content.WideTable()
	require.Len(t, table.Rows, 3)
	assert.Equal(t, "SpecMalformed,StatusMalformed(group=0)", table.Rows[0][19])
	assert.Equal(t, "SpecMalformed", table.Rows[1][19])
	assert.Equal(t, []string{
		"Unknown", "Unknown", "Reported", "Unverifiable", "-", "-", "-", "-", "-", "-",
		"-", "-", "-", "-", "-", "-", "-", "-", "-",
		"GroupStatusMissing(group=9),RevisionNameInvalid(component=router),TrafficInvalid(group=7,component=decoder)",
	}, table.Rows[2])

	reversed := content
	reversed.Components = append([]v1alpha1.RolloutComponentStatus{}, content.Components...)
	reversed.Issues = append([]v1alpha1.RolloutIssue{}, content.Issues...)
	for left, right := 0, len(reversed.Components)-1; left < right; left, right = left+1, right-1 {
		reversed.Components[left], reversed.Components[right] = reversed.Components[right], reversed.Components[left]
	}
	for left, right := 0, len(reversed.Issues)-1; left < right; left, right = left+1, right-1 {
		reversed.Issues[left], reversed.Issues[right] = reversed.Issues[right], reversed.Issues[left]
	}
	assert.Equal(t, table, reversed.WideTable())
	after, err := json.Marshal(content)
	require.NoError(t, err)
	assert.Equal(t, before, after, "Table must not mutate source content")

	allIssues := strings.Join([]string{table.Rows[0][19], table.Rows[1][19], table.Rows[2][19]}, ",")
	assert.Equal(t, 1, strings.Count(allIssues, "GroupStatusMissing(group=9)"))
	assert.Equal(t, 1, strings.Count(allIssues, "RevisionNameInvalid(component=router)"))
	assert.Equal(t, 1, strings.Count(allIssues, "TrafficInvalid(group=7,component=decoder)"))
	assert.Equal(t, 1, strings.Count(allIssues, "StatusMalformed(group=0)"))
	assert.Equal(t, 2, strings.Count(allIssues, "SpecMalformed"))
}

func TestRolloutStatusReportTableRetainsAllIssuesWithoutComponentRows(t *testing.T) {
	group := 1
	content := v1alpha1.RolloutStatusContent{
		Summary: v1alpha1.RolloutSummary{
			State:             v1alpha1.RolloutStateUnknown,
			ReportedState:     v1alpha1.RolloutStateUnknown,
			Evidence:          v1alpha1.EvidenceReported,
			Epoch:             v1alpha1.RolloutEpochUnverifiable,
			CoordinationReady: v1alpha1.RolloutConditionUnobserved,
		},
		Issues: []v1alpha1.RolloutIssue{
			{Code: v1alpha1.RolloutIssueStatusMalformed, Group: &group},
			{Code: v1alpha1.RolloutIssueRevisionNameInvalid, Component: v1alpha1.RuntimeComponentEngine},
		},
	}

	table := content.WideTable()
	require.Len(t, table.Rows, 1)
	assert.Equal(t,
		"RevisionNameInvalid(component=engine),StatusMalformed(group=1)",
		table.Rows[0][19],
	)
}

func TestRolloutStatusReportTableSeparatesCurrentAndReportedState(t *testing.T) {
	reportValue := v1alpha1.NewRolloutStatusReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutStatusContent{
			Summary: v1alpha1.RolloutSummary{
				State:             v1alpha1.RolloutStateUnknown,
				ReportedState:     v1alpha1.RolloutStateFailed,
				Evidence:          v1alpha1.EvidenceReported,
				Epoch:             v1alpha1.RolloutEpochUnverifiable,
				CoordinationReady: v1alpha1.RolloutConditionFalse,
			},
		},
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)

	table := reportValue.WideTable()
	assert.Equal(t, []string{
		"STATE", "REPORTED-STATE", "EVIDENCE", "EPOCH", "GROUP", "STRATEGY",
		"GROUP-PHASE", "CURRENT-COMPONENT", "PREVIOUS-COMPONENT", "COMPONENT",
		"COMPONENT-PHASE", "STEP", "GATE", "CAPACITY",
		"TARGET-TRAFFIC", "OBSERVED-TRAFFIC", "ROLLED-OUT", "READY", "PREVIOUS", "ISSUES",
	}, table.Headers)
	require.Len(t, table.Rows, 1)
	assert.Equal(t, []string{
		"Unknown", "Failed", "Reported", "Unverifiable", "-", "-", "-", "-", "-",
		"-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-",
	}, table.Rows[0])
}

func TestRolloutStatusReportTableShowsSequentialCursor(t *testing.T) {
	group := 0
	reportValue := v1alpha1.NewRolloutStatusReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutStatusContent{
			Summary: v1alpha1.RolloutSummary{
				State: v1alpha1.RolloutStateUnknown, ReportedState: v1alpha1.RolloutStateInProgress,
				Evidence: v1alpha1.EvidenceReported, Epoch: v1alpha1.RolloutEpochUnverifiable,
				CoordinationReady: v1alpha1.RolloutConditionTrue,
			},
			Groups: []v1alpha1.RolloutGroupStatus{{
				Index: 0, Strategy: v1alpha1.RolloutStrategySequential,
				Phase: v1alpha1.RolloutPhaseAwaitingNextComponent,
				Components: []v1alpha1.RuntimeComponentType{
					v1alpha1.RuntimeComponentDecoder, v1alpha1.RuntimeComponentEngine,
				},
				CurrentComponent:  v1alpha1.RuntimeComponentEngine,
				PreviousComponent: v1alpha1.RuntimeComponentDecoder,
			}},
			Components: []v1alpha1.RolloutComponentStatus{
				{Type: v1alpha1.RuntimeComponentEngine, Strategy: v1alpha1.RolloutStrategySequential, Group: &group, Phase: v1alpha1.RolloutPhaseUnknown, Traffic: []v1alpha1.RolloutTrafficTarget{}},
				{Type: v1alpha1.RuntimeComponentDecoder, Strategy: v1alpha1.RolloutStrategySequential, Group: &group, Phase: v1alpha1.RolloutPhaseUnknown, Traffic: []v1alpha1.RolloutTrafficTarget{}},
			},
		},
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)

	table := reportValue.WideTable()
	assert.Equal(t, []string{
		"STATE", "REPORTED-STATE", "EVIDENCE", "EPOCH", "GROUP", "STRATEGY",
		"GROUP-PHASE", "CURRENT-COMPONENT", "PREVIOUS-COMPONENT", "COMPONENT",
		"COMPONENT-PHASE", "STEP", "GATE", "CAPACITY", "TARGET-TRAFFIC",
		"OBSERVED-TRAFFIC", "ROLLED-OUT", "READY", "PREVIOUS", "ISSUES",
	}, table.Headers)
	require.Len(t, table.Rows, 2)
	for _, row := range table.Rows {
		assert.Equal(t, "engine", row[7])
		assert.Equal(t, "decoder", row[8])
	}
}

func TestRolloutStatusCanonicalOrderingIsTotal(t *testing.T) {
	groupA := v1alpha1.RolloutGroupStatus{
		Index: 1, Strategy: v1alpha1.RolloutStrategyCanary, Phase: v1alpha1.RolloutPhaseStable,
		Components: []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentRouter},
	}
	groupB := v1alpha1.RolloutGroupStatus{
		Index: 1, Strategy: v1alpha1.RolloutStrategyCanary, Phase: v1alpha1.RolloutPhaseFailed,
		Components: []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentEngine},
	}
	componentA := v1alpha1.RolloutComponentStatus{
		Type: v1alpha1.RuntimeComponentEngine, Phase: v1alpha1.RolloutPhaseStable,
		Traffic: []v1alpha1.RolloutTrafficTarget{{RevisionHash: "aaaaaaaa", Percent: 90, Role: v1alpha1.RolloutRevisionCurrent}},
	}
	componentB := v1alpha1.RolloutComponentStatus{
		Type: v1alpha1.RuntimeComponentEngine, Phase: v1alpha1.RolloutPhaseFailed,
		Traffic: []v1alpha1.RolloutTrafficTarget{{RevisionHash: "aaaaaaaa", Percent: 10, Role: v1alpha1.RolloutRevisionCurrent}},
	}
	left := v1alpha1.RolloutStatusContent{
		Groups:     []v1alpha1.RolloutGroupStatus{groupA, groupB},
		Components: []v1alpha1.RolloutComponentStatus{componentA, componentB},
	}.Canonical()
	right := v1alpha1.RolloutStatusContent{
		Groups:     []v1alpha1.RolloutGroupStatus{groupB, groupA},
		Components: []v1alpha1.RolloutComponentStatus{componentB, componentA},
	}.Canonical()

	assert.Equal(t, left, right)
}

func rolloutStatusFixture() v1alpha1.RolloutStatusReport {
	group := 0
	return v1alpha1.NewRolloutStatusReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutStatusContent{
			Summary: v1alpha1.RolloutSummary{
				State:             v1alpha1.RolloutStateUnknown,
				ReportedState:     v1alpha1.RolloutStateInProgress,
				Evidence:          v1alpha1.EvidenceReported,
				Epoch:             v1alpha1.RolloutEpochUnverifiable,
				CoordinationReady: v1alpha1.RolloutConditionNotApplicable,
			},
			Groups: []v1alpha1.RolloutGroupStatus{{
				Index: 0, Strategy: v1alpha1.RolloutStrategyCanary, Phase: v1alpha1.RolloutPhaseCanarying,
				Components: []v1alpha1.RuntimeComponentType{v1alpha1.RuntimeComponentEngine},
				Step: &v1alpha1.RolloutStepStatus{
					Index: 1, Total: 3, Capacity: "50%", TargetTraffic: 20, ObservedTraffic: 20,
					Gate: v1alpha1.RolloutGateManual,
				},
			}},
			Components: []v1alpha1.RolloutComponentStatus{{
				Type: v1alpha1.RuntimeComponentEngine, Strategy: v1alpha1.RolloutStrategyCanary,
				Group:                 &group,
				Phase:                 v1alpha1.RolloutPhaseCanarying,
				RolledOutRevisionHash: "aaaaaaaa", ReadyRevisionHash: "bbbbbbbb",
			}},
		},
		fixedClock{now: time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)},
	)
}
