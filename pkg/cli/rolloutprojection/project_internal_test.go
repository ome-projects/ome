package rolloutprojection

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestComponentPhaseProjectionIsExhaustiveAndClosed(t *testing.T) {
	tests := []struct {
		input omev1beta1.RolloutPhase
		want  reportv1alpha1.RolloutPhase
	}{
		{omev1beta1.RolloutPhaseStable, reportv1alpha1.RolloutPhaseStable},
		{omev1beta1.RolloutPhaseCanarying, reportv1alpha1.RolloutPhaseCanarying},
		{omev1beta1.RolloutPhasePending, reportv1alpha1.RolloutPhasePending},
		{omev1beta1.RolloutPhasePaused, reportv1alpha1.RolloutPhasePaused},
		{omev1beta1.RolloutPhasePromoting, reportv1alpha1.RolloutPhasePromoting},
		{omev1beta1.RolloutPhaseRollingBack, reportv1alpha1.RolloutPhaseRollingBack},
		{omev1beta1.RolloutPhaseRolledBack, reportv1alpha1.RolloutPhaseRolledBack},
		{omev1beta1.RolloutPhaseFailed, reportv1alpha1.RolloutPhaseFailed},
		{"", reportv1alpha1.RolloutPhaseUnknown},
		{"Never serialize this", reportv1alpha1.RolloutPhaseUnknown},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, projectComponentPhase(tt.input), "input %q", tt.input)
	}
}

func TestCoordinationPhaseProjectionIsExhaustiveAndClosed(t *testing.T) {
	tests := []struct {
		input omev1beta1.CoordinationPhase
		want  reportv1alpha1.RolloutPhase
	}{
		{omev1beta1.CoordinationPhaseIdle, reportv1alpha1.RolloutPhaseIdle},
		{omev1beta1.CoordinationPhaseSurging, reportv1alpha1.RolloutPhaseSurging},
		{omev1beta1.CoordinationPhaseWaiting, reportv1alpha1.RolloutPhaseWaiting},
		{omev1beta1.CoordinationPhaseShifting, reportv1alpha1.RolloutPhaseShifting},
		{omev1beta1.CoordinationPhaseDraining, reportv1alpha1.RolloutPhaseDraining},
		{omev1beta1.CoordinationPhaseScalingDown, reportv1alpha1.RolloutPhaseScalingDown},
		{omev1beta1.CoordinationPhaseStaged, reportv1alpha1.RolloutPhaseStaged},
		{omev1beta1.CoordinationPhaseFailed, reportv1alpha1.RolloutPhaseFailed},
		{omev1beta1.CoordinationPhaseRollingBack, reportv1alpha1.RolloutPhaseRollingBack},
		{omev1beta1.CoordinationPhasePaused, reportv1alpha1.RolloutPhasePaused},
		{"", reportv1alpha1.RolloutPhaseUnknown},
		{"Never serialize this", reportv1alpha1.RolloutPhaseUnknown},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, projectCoordinationPhase(tt.input), "input %q", tt.input)
	}
}

func TestCanaryGateProjectionUsesShippedPrecedence(t *testing.T) {
	duration := metav1.Duration{Duration: time.Minute}
	tests := []struct {
		name string
		step omev1beta1.RolloutGroupStep
		want reportv1alpha1.RolloutGate
	}{
		{name: "immediate", want: reportv1alpha1.RolloutGateImmediate},
		{name: "manual", step: omev1beta1.RolloutGroupStep{Pause: &omev1beta1.RolloutPause{}}, want: reportv1alpha1.RolloutGateManual},
		{name: "timed", step: omev1beta1.RolloutGroupStep{Pause: &omev1beta1.RolloutPause{Duration: &duration}}, want: reportv1alpha1.RolloutGateTimed},
		{name: "analysis wins over duration", step: omev1beta1.RolloutGroupStep{Pause: &omev1beta1.RolloutPause{Duration: &duration}, Analysis: &omev1beta1.RolloutAnalysis{}}, want: reportv1alpha1.RolloutGateAnalysis},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { assert.Equal(t, tt.want, gateFor(&tt.step)) })
	}
}

func TestAnalysisProjectionNeverReturnsMetricPayloads(t *testing.T) {
	evaluated := metav1.NewTime(time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC))
	tests := []struct {
		name   string
		status *omev1beta1.CanaryStatus
		want   reportv1alpha1.RolloutAnalysisState
		valid  bool
	}{
		{name: "unobserved", status: &omev1beta1.CanaryStatus{}, want: reportv1alpha1.RolloutAnalysisUnobserved, valid: true},
		{name: "passing", status: &omev1beta1.CanaryStatus{
			LastEvaluationTime: &evaluated, LastConclusiveEvaluationTime: &evaluated,
			MetricResults: []omev1beta1.AnalysisMetricResult{{
				Name: "secret", Value: "1", Threshold: "1", Operator: omev1beta1.ComparisonLTE,
				Passed: true, Time: &evaluated,
			}},
		}, want: reportv1alpha1.RolloutAnalysisPassing, valid: true},
		{name: "failing", status: &omev1beta1.CanaryStatus{
			AnalysisFailedChecks: 1, LastEvaluationTime: &evaluated, LastConclusiveEvaluationTime: &evaluated,
			MetricResults: []omev1beta1.AnalysisMetricResult{{
				Name: "secret", Value: "2", Threshold: "1", Operator: omev1beta1.ComparisonLTE,
				Passed: false, Time: &evaluated,
			}},
		}, want: reportv1alpha1.RolloutAnalysisFailing, valid: true},
		{name: "inconclusive", status: &omev1beta1.CanaryStatus{
			LastEvaluationTime: &evaluated,
			MetricResults: []omev1beta1.AnalysisMetricResult{{
				Name: "secret", Threshold: "1", Operator: omev1beta1.ComparisonLTE,
				Passed: false, Message: "secret", Time: &evaluated,
			}},
		}, want: reportv1alpha1.RolloutAnalysisInconclusive, valid: true},
	}
	analysis := &omev1beta1.RolloutAnalysis{Metrics: []omev1beta1.AnalysisMetric{{
		Name: "secret", Threshold: "1", Operator: omev1beta1.ComparisonLTE,
	}}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, valid := analysisState(analysis, tt.status)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.valid, valid)
		})
	}
}

func TestCapacityAndRevisionExtractionAreBounded(t *testing.T) {
	for _, value := range []intstr.IntOrString{
		intstr.FromInt32(3), intstr.FromString("25%"), intstr.FromString("+10%"),
	} {
		assert.True(t, safeCapacity(value), "value %q", value.String())
	}
	for _, value := range []intstr.IntOrString{
		intstr.FromInt32(-1), intstr.FromString("3"), intstr.FromString("101%"),
		intstr.FromString("secret value"), intstr.FromString(""),
	} {
		assert.False(t, safeCapacity(value), "value %q", value.String())
	}

	assert.Equal(t, "aaaaaaaa", extractRevisionHash("chat", omev1beta1.EngineComponent, "chat-engine-rev-aaaaaaaa"))
	for _, value := range []string{
		"other-engine-rev-aaaaaaaa", "chat-decoder-rev-aaaaaaaa", "chat-engine-rev-",
		"chat-engine-rev-UPPERCASE", "chat-engine-rev-secret/value",
	} {
		assert.Empty(t, extractRevisionHash("chat", omev1beta1.EngineComponent, value), "value %q", value)
	}
}

func TestRevisionExtractionAcceptsOnlyExactControllerBoundedName(t *testing.T) {
	owner := strings.Repeat("long-owner-", 7) + "chat"
	raw := fmt.Sprintf("%s-%s-rev-%s", owner, omev1beta1.EngineComponent, "deadbeef")
	serviceName := constants.TruncateNameWithMaxLength(raw, validation.DNS1035LabelMaxLength)
	require.Len(t, serviceName, validation.DNS1035LabelMaxLength)

	assert.Equal(t, "deadbeef", extractRevisionHash(owner, omev1beta1.EngineComponent, serviceName))
	assert.Empty(t, extractRevisionHash(owner, omev1beta1.EngineComponent, "z"+serviceName[1:]))
	assert.Empty(t, extractRevisionHash(owner, omev1beta1.EngineComponent, serviceName[:len(serviceName)-8]+"DEADBEEF"))
}

func TestControllerRevisionExtractionAcceptsOnlyExactBoundedIdentity(t *testing.T) {
	assert.Equal(t, "deadbeef", extractControllerRevisionHash(
		"chat", omev1beta1.EngineComponent, "chat-engine-deadbeef",
	))
	for _, value := range []string{
		"other-engine-deadbeef",
		"chat-decoder-deadbeef",
		"chat-engine-rev-deadbeef",
		"chat-engine-DEADBEEF",
		"chat-engine-secret/value",
	} {
		assert.Empty(t, extractControllerRevisionHash(
			"chat", omev1beta1.EngineComponent, value,
		), "value %q", value)
	}
}

func TestCoordinationConditionProjectionIsBounded(t *testing.T) {
	tests := []struct {
		name       string
		conditions []apis.Condition
		phase      omev1beta1.CoordinationPhase
		want       reportv1alpha1.RolloutConditionState
		wantIssue  bool
	}{
		{name: "absent", phase: omev1beta1.CoordinationPhaseIdle, want: reportv1alpha1.RolloutConditionUnobserved},
		{name: "true", phase: omev1beta1.CoordinationPhaseIdle, conditions: []apis.Condition{{Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionTrue}}, want: reportv1alpha1.RolloutConditionTrue},
		{name: "false", phase: omev1beta1.CoordinationPhaseSurging, conditions: []apis.Condition{{Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionFalse}}, want: reportv1alpha1.RolloutConditionFalse},
		{name: "unknown", conditions: []apis.Condition{{Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionUnknown}}, want: reportv1alpha1.RolloutConditionUnknown},
		{name: "invalid", phase: omev1beta1.CoordinationPhaseIdle, conditions: []apis.Condition{{Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionStatus("Secret")}}, want: reportv1alpha1.RolloutConditionInvalid, wantIssue: true},
		{name: "duplicate", conditions: []apis.Condition{
			{Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionTrue},
			{Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionFalse},
		}, want: reportv1alpha1.RolloutConditionInvalid, wantIssue: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := coordinatedFixture()
			isvc.Status.RolloutCoordination.Groups[0].Phase = tt.phase
			isvc.Status.Conditions = tt.conditions
			got, err := Project(isvc, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Content.Summary.CoordinationReady)
			if tt.wantIssue {
				assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{Code: reportv1alpha1.RolloutIssueStatusMalformed})
			}
		})
	}
}

func TestMissingObservedGroupIsExplicitPartialData(t *testing.T) {
	isvc := coordinatedFixture()
	isvc.Status.RolloutCoordination = nil

	got, err := Project(isvc, reportv1alpha1.ClockFunc(func() time.Time { return time.Unix(1, 0) }))
	require.NoError(t, err)
	group := 0
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{Code: reportv1alpha1.RolloutIssueGroupStatusMissing, Group: &group})
	assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{Code: reportv1alpha1.WarningPartialData})
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
}

func coordinatedFixture() *omev1beta1.InferenceService {
	mode := constants.OMENative
	return &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "chat", UID: "isvc-uid", Generation: 7},
		Spec: omev1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Engine:         &omev1beta1.EngineSpec{},
			Rollout: &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			}}},
		},
		Status: omev1beta1.InferenceServiceStatus{
			Status: duckv1.Status{ObservedGeneration: 7},
			Components: map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: {},
			},
			RolloutCoordination: &omev1beta1.RolloutCoordinationStatus{Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
				Name: "0", Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Policy: omev1beta1.CoordinationPolicyBlueGreen, Phase: omev1beta1.CoordinationPhaseIdle,
			}}},
		},
	}
}
