package v1alpha1_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestRolloutValidationEnumValuesAreStable(t *testing.T) {
	got := []string{
		string(v1alpha1.RolloutValidationValid),
		string(v1alpha1.RolloutValidationInvalid),
		string(v1alpha1.RolloutValidationUnverifiable),
		string(v1alpha1.RolloutValidationResultValid),
		string(v1alpha1.RolloutValidationResultInvalid),
		string(v1alpha1.RolloutValidationResultUnverifiable),
		string(v1alpha1.RolloutValidationResultNotApplicable),
		string(v1alpha1.RolloutValidationFreshnessCurrent),
		string(v1alpha1.RolloutValidationFreshnessStale),
		string(v1alpha1.RolloutValidationFreshnessUnverifiable),
		string(v1alpha1.RolloutValidationFreshnessNotApplicable),
		string(v1alpha1.RolloutValidationCheckRolloutReferences),
		string(v1alpha1.RolloutValidationCheckRolloutPlan),
		string(v1alpha1.RolloutValidationCheckRolloutOrdering),
		string(v1alpha1.RolloutValidationCheckRolloutResolution),
		string(v1alpha1.RolloutValidationCheckTrafficSpec),
		string(v1alpha1.RolloutValidationCheckTrafficReadiness),
		string(v1alpha1.RolloutValidationCheckScalingPolicy),
		string(v1alpha1.RolloutValidationCheckAutoscalerSpec),
		string(v1alpha1.RolloutValidationCheckAutoscalerResolution),
		string(v1alpha1.RolloutValidationIssueRolloutReferenceInvalid),
		string(v1alpha1.RolloutValidationIssueRolloutPlanInvalid),
		string(v1alpha1.RolloutValidationIssueRolloutOrderingInvalid),
		string(v1alpha1.RolloutValidationIssueRolloutResolutionMissing),
		string(v1alpha1.RolloutValidationIssueRolloutResolutionStale),
		string(v1alpha1.RolloutValidationIssueRolloutResolutionMalformed),
		string(v1alpha1.RolloutValidationIssueRolloutResolutionFailed),
		string(v1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable),
		string(v1alpha1.RolloutValidationIssueRolloutPrerequisiteUnverifiable),
		string(v1alpha1.RolloutValidationIssueTrafficSpecInvalid),
		string(v1alpha1.RolloutValidationIssueTrafficAnnotationInvalid),
		string(v1alpha1.RolloutValidationIssueTrafficEvidenceMissing),
		string(v1alpha1.RolloutValidationIssueTrafficEvidenceStale),
		string(v1alpha1.RolloutValidationIssueTrafficEvidenceMalformed),
		string(v1alpha1.RolloutValidationIssueTrafficNotReady),
		string(v1alpha1.RolloutValidationIssueScalingPolicyInvalid),
		string(v1alpha1.RolloutValidationIssueAutoscalerSpecInvalid),
		string(v1alpha1.RolloutValidationIssueAutoscalerReferenceInvalid),
		string(v1alpha1.RolloutValidationIssueAutoscalerEvidenceMissing),
		string(v1alpha1.RolloutValidationIssueAutoscalerEvidenceStale),
		string(v1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed),
		string(v1alpha1.RolloutValidationIssueAutoscalerResolutionFailed),
		string(v1alpha1.RolloutValidationIssueChecksTruncated),
		string(v1alpha1.RolloutValidationIssueIssuesTruncated),
		string(v1alpha1.RolloutValidationIssueEvidenceMalformed),
	}
	assert.Equal(t, []string{
		"Valid", "Invalid", "Unverifiable",
		"Valid", "Invalid", "Unverifiable", "NotApplicable",
		"Current", "Stale", "Unverifiable", "NotApplicable",
		"RolloutReferences", "RolloutPlan", "RolloutOrdering", "RolloutResolution",
		"TrafficSpec", "TrafficReadiness", "ScalingPolicy", "AutoscalerSpec", "AutoscalerResolution",
		"RolloutReferenceInvalid", "RolloutPlanInvalid", "RolloutOrderingInvalid",
		"RolloutResolutionMissing", "RolloutResolutionStale", "RolloutResolutionMalformed",
		"RolloutResolutionFailed", "RolloutResolutionFreshnessUnverifiable",
		"RolloutPrerequisiteUnverifiable", "TrafficSpecInvalid", "TrafficAnnotationInvalid",
		"TrafficEvidenceMissing", "TrafficEvidenceStale", "TrafficEvidenceMalformed", "TrafficNotReady",
		"ScalingPolicyInvalid", "AutoscalerSpecInvalid", "AutoscalerReferenceInvalid",
		"AutoscalerEvidenceMissing", "AutoscalerEvidenceStale", "AutoscalerEvidenceMalformed",
		"AutoscalerResolutionFailed", "ChecksTruncated", "IssuesTruncated", "EvidenceMalformed",
	}, got)
}

func TestRolloutValidationIssueCapIsOrderIndependent(t *testing.T) {
	checks := []v1alpha1.RolloutValidationCheckName{
		v1alpha1.RolloutValidationCheckRolloutReferences,
		v1alpha1.RolloutValidationCheckRolloutPlan,
		v1alpha1.RolloutValidationCheckRolloutOrdering,
		v1alpha1.RolloutValidationCheckRolloutResolution,
		v1alpha1.RolloutValidationCheckTrafficSpec,
		v1alpha1.RolloutValidationCheckTrafficReadiness,
		v1alpha1.RolloutValidationCheckScalingPolicy,
		v1alpha1.RolloutValidationCheckAutoscalerSpec,
		v1alpha1.RolloutValidationCheckAutoscalerResolution,
	}
	components := []v1alpha1.RuntimeComponentType{
		"", v1alpha1.RuntimeComponentEngine, v1alpha1.RuntimeComponentRouter,
	}
	issues := make([]v1alpha1.RolloutValidationIssue, 0, len(checks)*len(components)*2)
	for _, check := range checks {
		for _, component := range components {
			issues = append(issues,
				v1alpha1.RolloutValidationIssue{
					Code: v1alpha1.RolloutValidationIssueTrafficNotReady, Check: check, Component: component,
				},
				v1alpha1.RolloutValidationIssue{
					Code: v1alpha1.RolloutValidationIssueAutoscalerSpecInvalid, Check: check, Component: component,
				},
			)
		}
	}
	reversed := append([]v1alpha1.RolloutValidationIssue{}, issues...)
	slices.Reverse(reversed)

	forward := v1alpha1.RolloutValidationContent{Issues: issues}.Canonical().Issues
	backward := v1alpha1.RolloutValidationContent{Issues: reversed}.Canonical().Issues
	assert.Equal(t, forward, backward)
	assert.Len(t, forward, 32)
	assert.Contains(t, forward, v1alpha1.RolloutValidationIssue{
		Code: v1alpha1.RolloutValidationIssueIssuesTruncated,
	})
}

func TestRolloutValidationDuplicateIssuesDoNotClaimTruncation(t *testing.T) {
	issues := make([]v1alpha1.RolloutValidationIssue, 40)
	for i := range issues {
		issues[i] = v1alpha1.RolloutValidationIssue{
			Code:  v1alpha1.RolloutValidationIssueTrafficNotReady,
			Check: v1alpha1.RolloutValidationCheckTrafficReadiness,
		}
	}

	got := v1alpha1.RolloutValidationContent{Issues: issues}.Canonical().Issues
	assert.Equal(t, []v1alpha1.RolloutValidationIssue{{
		Code:  v1alpha1.RolloutValidationIssueTrafficNotReady,
		Check: v1alpha1.RolloutValidationCheckTrafficReadiness,
	}}, got)
}

func TestRolloutValidationReportCanonicalIsDeterministicBoundedAndImmutable(t *testing.T) {
	now := time.Date(2026, time.September, 14, 17, 0, 0, 0, time.FixedZone("PDT", -7*60*60))
	checks := make([]v1alpha1.RolloutValidationCheck, 0, 20)
	for i := 0; i < 20; i++ {
		checks = append(checks, v1alpha1.RolloutValidationCheck{
			Check:     v1alpha1.RolloutValidationCheckAutoscalerSpec,
			Component: v1alpha1.RuntimeComponentEngine,
			Result:    v1alpha1.RolloutValidationResultValid,
			Evidence:  v1alpha1.EvidenceComputed,
			Freshness: v1alpha1.RolloutValidationFreshnessCurrent,
		})
	}
	issues := make([]v1alpha1.RolloutValidationIssue, 0, 40)
	for i := 0; i < 40; i++ {
		issues = append(issues, v1alpha1.RolloutValidationIssue{
			Code:  v1alpha1.RolloutValidationIssueTrafficNotReady,
			Check: v1alpha1.RolloutValidationCheckTrafficReadiness,
		})
	}
	reportValue := v1alpha1.RolloutValidationReport{
		Metadata:    v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		CollectedAt: now,
		Sources: []v1alpha1.RolloutSourceReference{
			{Name: "z", Evidence: v1alpha1.EvidenceReported},
			{Name: "a", Evidence: v1alpha1.EvidenceReported},
		},
		Content: v1alpha1.RolloutValidationContent{
			Summary: v1alpha1.RolloutValidationSummary{State: v1alpha1.RolloutValidationValid},
			Checks:  checks,
			Issues:  issues,
		},
	}
	original := reportValue
	original.Content.Checks = append([]v1alpha1.RolloutValidationCheck{}, reportValue.Content.Checks...)
	original.Content.Issues = append([]v1alpha1.RolloutValidationIssue{}, reportValue.Content.Issues...)

	first := reportValue.Canonical()
	second := reportValue.Canonical()
	assert.Equal(t, first, second)
	assert.Equal(t, original, reportValue)
	assert.Equal(t, v1alpha1.APIVersion, first.APIVersion)
	assert.Equal(t, v1alpha1.RolloutValidationReportKind, first.Kind)
	assert.Equal(t, time.UTC, first.CollectedAt.Location())
	assert.LessOrEqual(t, len(first.Content.Checks), 16)
	assert.LessOrEqual(t, len(first.Content.Issues), 32)
	assert.Equal(t, "a", first.Sources[0].Name)
	assert.Contains(t, first.Content.Issues, v1alpha1.RolloutValidationIssue{
		Code: v1alpha1.RolloutValidationIssueChecksTruncated,
	})
	assert.NotContains(t, first.Content.Issues, v1alpha1.RolloutValidationIssue{
		Code: v1alpha1.RolloutValidationIssueIssuesTruncated,
	})
}

func TestRolloutValidationReportBoundsEnumsAndNeverSerializesArbitraryFields(t *testing.T) {
	reportValue := v1alpha1.RolloutValidationReport{
		Metadata:    v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		CollectedAt: time.Date(2026, time.September, 15, 0, 0, 0, 0, time.UTC),
		Content: v1alpha1.RolloutValidationContent{
			Summary: v1alpha1.RolloutValidationSummary{State: v1alpha1.RolloutValidationState("SECRET_STATE")},
			Checks: []v1alpha1.RolloutValidationCheck{
				{
					Check:     v1alpha1.RolloutValidationCheckTrafficReadiness,
					Component: v1alpha1.RuntimeComponentType("SECRET_COMPONENT"),
					Result:    v1alpha1.RolloutValidationResult("SECRET_RESULT"),
					Evidence:  v1alpha1.EvidenceLevel("SECRET_EVIDENCE"),
					Freshness: v1alpha1.RolloutValidationFreshness("SECRET_FRESHNESS"),
				},
				{Check: v1alpha1.RolloutValidationCheckName("SECRET_CHECK")},
			},
			Issues: []v1alpha1.RolloutValidationIssue{{Code: v1alpha1.RolloutValidationIssueCode("SECRET_ISSUE")}},
		},
	}

	canonical := reportValue.Canonical()
	assert.Equal(t, v1alpha1.RolloutValidationUnverifiable, canonical.Content.Summary.State)
	require.Len(t, canonical.Content.Checks, 1)
	assert.Empty(t, canonical.Content.Checks[0].Component)
	assert.Equal(t, v1alpha1.RolloutValidationResultUnverifiable, canonical.Content.Checks[0].Result)
	assert.Equal(t, v1alpha1.EvidenceUnavailable, canonical.Content.Checks[0].Evidence)
	assert.Equal(t, v1alpha1.RolloutValidationFreshnessUnverifiable, canonical.Content.Checks[0].Freshness)
	assert.Equal(t, []v1alpha1.RolloutValidationIssue{{Code: v1alpha1.RolloutValidationIssueEvidenceMalformed}}, canonical.Content.Issues)

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, reportValue))
		assert.NotContains(t, output.String(), "SECRET_")
	}
}

func TestRolloutValidationTablesAreTypedDeterministicAndCompact(t *testing.T) {
	reportValue := validationReportFixture()
	before := reportValue.Canonical()

	assert.Equal(t, report.Table{
		Headers: []string{"CHECK", "COMP", "RESULT", "SOURCE"},
		Rows: [][]string{
			{"OVERALL", "-", "Unverifiable", "Computed/Current"},
			{"ROLLOUT-PLAN", "-", "Valid", "Computed/Current"},
			{"TRAFFIC-READY", "-", "Unverifiable", "Reported/Stale"},
			{"AUTOSCALER-SPEC", "engine", "Invalid", "Computed/Current"},
		},
	}, reportValue.Table())
	assert.Equal(t, report.Table{
		Headers: []string{"CHECK", "COMP", "RESULT", "EVIDENCE", "FRESHNESS", "ISSUES"},
		Rows: [][]string{
			{"OVERALL", "-", "Unverifiable", "Computed", "Current", "-"},
			{"ROLLOUT-PLAN", "-", "Valid", "Computed", "Current", "-"},
			{"TRAFFIC-READY", "-", "Unverifiable", "Reported", "Stale", "TrafficEvidenceStale"},
			{"AUTOSCALER-SPEC", "engine", "Invalid", "Computed", "Current", "AutoscalerSpecInvalid"},
		},
	}, reportValue.WideTable())
	assert.Equal(t, before, reportValue.Canonical(), "table rendering must not mutate the report")

	var compact bytes.Buffer
	require.NoError(t, reportValue.Table().Write(&compact))
	for _, line := range strings.Split(strings.TrimSuffix(compact.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 80, line)
	}
	var wide bytes.Buffer
	require.NoError(t, reportValue.WideTable().Write(&wide))
	for _, line := range strings.Split(strings.TrimSuffix(wide.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 120, line)
	}
}

func TestRolloutValidationWideTableBoundsDenseIssueRows(t *testing.T) {
	issues := []v1alpha1.RolloutValidationIssue{
		{Code: v1alpha1.RolloutValidationIssueTrafficSpecInvalid, Check: v1alpha1.RolloutValidationCheckTrafficSpec},
		{Code: v1alpha1.RolloutValidationIssueTrafficAnnotationInvalid, Check: v1alpha1.RolloutValidationCheckTrafficSpec},
		{Code: v1alpha1.RolloutValidationIssueTrafficEvidenceMalformed, Check: v1alpha1.RolloutValidationCheckTrafficSpec},
		{Code: v1alpha1.RolloutValidationIssueRolloutResolutionMalformed, Check: v1alpha1.RolloutValidationCheckTrafficSpec},
	}
	reportValue := v1alpha1.NewRolloutValidationReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutValidationContent{
			Summary: v1alpha1.RolloutValidationSummary{State: v1alpha1.RolloutValidationInvalid},
			Checks: []v1alpha1.RolloutValidationCheck{{
				Check: v1alpha1.RolloutValidationCheckTrafficSpec, Result: v1alpha1.RolloutValidationResultInvalid,
				Evidence: v1alpha1.EvidenceComputed, Freshness: v1alpha1.RolloutValidationFreshnessCurrent,
			}},
			Issues: issues,
		},
		fixedClock{now: time.Date(2026, time.September, 15, 0, 0, 0, 0, time.UTC)},
	)

	var output bytes.Buffer
	require.NoError(t, reportValue.WideTable().Write(&output))
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 120, line)
	}
	assert.Contains(t, output.String(), ",+3")
}

func TestRolloutValidationWideTableKeepsOneLongIssueCodeExact(t *testing.T) {
	reportValue := v1alpha1.NewRolloutValidationReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutValidationContent{
			Summary: v1alpha1.RolloutValidationSummary{State: v1alpha1.RolloutValidationUnverifiable},
			Checks: []v1alpha1.RolloutValidationCheck{{
				Check:     v1alpha1.RolloutValidationCheckRolloutResolution,
				Result:    v1alpha1.RolloutValidationResultUnverifiable,
				Evidence:  v1alpha1.EvidenceReported,
				Freshness: v1alpha1.RolloutValidationFreshnessUnverifiable,
			}},
			Issues: []v1alpha1.RolloutValidationIssue{{
				Code:  v1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable,
				Check: v1alpha1.RolloutValidationCheckRolloutResolution,
			}},
		},
		fixedClock{now: time.Date(2026, time.September, 15, 0, 0, 0, 0, time.UTC)},
	)

	rows := reportValue.WideTable().Rows
	require.Len(t, rows, 2)
	assert.Equal(t, "RolloutResolutionFreshnessUnverifiable", rows[1][5])
	assert.NotContains(t, rows[1][5], ",+0")

	var output bytes.Buffer
	require.NoError(t, reportValue.WideTable().Write(&output))
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len([]rune(line)), 120, line)
	}
}

func TestRolloutValidationMachineOutputIsStableAndUsesNonNilArrays(t *testing.T) {
	reportValue := v1alpha1.NewRolloutValidationReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutValidationContent{
			Summary: v1alpha1.RolloutValidationSummary{State: v1alpha1.RolloutValidationValid},
		},
		fixedClock{now: time.Date(2026, time.September, 15, 0, 0, 0, 0, time.UTC)},
	)

	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		t.Run(string(format), func(t *testing.T) {
			var first, second bytes.Buffer
			require.NoError(t, report.Write(&first, format, reportValue))
			require.NoError(t, report.Write(&second, format, reportValue))
			assert.Equal(t, first.String(), second.String())
			var decoded v1alpha1.RolloutValidationReport
			if format == report.FormatJSON {
				require.NoError(t, json.Unmarshal(first.Bytes(), &decoded))
			} else {
				require.NoError(t, yaml.Unmarshal(first.Bytes(), &decoded))
			}
			assert.NotNil(t, decoded.Sources)
			assert.NotNil(t, decoded.Warnings)
			assert.NotNil(t, decoded.Content.Checks)
			assert.NotNil(t, decoded.Content.Issues)
		})
	}
}

func validationReportFixture() v1alpha1.RolloutValidationReport {
	return v1alpha1.NewRolloutValidationReport(
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		v1alpha1.RolloutValidationContent{
			Summary: v1alpha1.RolloutValidationSummary{State: v1alpha1.RolloutValidationUnverifiable},
			Checks: []v1alpha1.RolloutValidationCheck{
				{Check: v1alpha1.RolloutValidationCheckAutoscalerSpec, Component: v1alpha1.RuntimeComponentEngine, Result: v1alpha1.RolloutValidationResultInvalid, Evidence: v1alpha1.EvidenceComputed, Freshness: v1alpha1.RolloutValidationFreshnessCurrent},
				{Check: v1alpha1.RolloutValidationCheckTrafficReadiness, Result: v1alpha1.RolloutValidationResultUnverifiable, Evidence: v1alpha1.EvidenceReported, Freshness: v1alpha1.RolloutValidationFreshnessStale},
				{Check: v1alpha1.RolloutValidationCheckRolloutPlan, Result: v1alpha1.RolloutValidationResultValid, Evidence: v1alpha1.EvidenceComputed, Freshness: v1alpha1.RolloutValidationFreshnessCurrent},
			},
			Issues: []v1alpha1.RolloutValidationIssue{
				{Code: v1alpha1.RolloutValidationIssueAutoscalerSpecInvalid, Check: v1alpha1.RolloutValidationCheckAutoscalerSpec, Component: v1alpha1.RuntimeComponentEngine},
				{Code: v1alpha1.RolloutValidationIssueTrafficEvidenceStale, Check: v1alpha1.RolloutValidationCheckTrafficReadiness},
			},
		},
		fixedClock{now: time.Date(2026, time.September, 15, 0, 0, 0, 0, time.UTC)},
	)
}

func TestRolloutValidationCanonicalReturnsDeepCopy(t *testing.T) {
	reportValue := validationReportFixture()
	canonical := reportValue.Canonical()
	canonical.Content.Checks[0].Result = v1alpha1.RolloutValidationResultInvalid
	canonical.Content.Issues[0].Code = v1alpha1.RolloutValidationIssueTrafficNotReady
	canonical.Sources = append(canonical.Sources, v1alpha1.RolloutSourceReference{Name: "other"})
	assert.False(t, reflect.DeepEqual(canonical, reportValue))
	assert.Equal(t, v1alpha1.RolloutValidationResultValid, reportValue.Content.Checks[0].Result)
}
