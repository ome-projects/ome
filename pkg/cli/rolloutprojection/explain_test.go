package rolloutprojection_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestProjectExplainSeparatesDeclaredPinnedAndObservedEvidence(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Spec.Rollout.Groups[0].Canary.Steps[0] = omev1beta1.RolloutGroupStep{
		Capacity: intstr.FromString("25%"), Traffic: 10, Pause: &omev1beta1.RolloutPause{},
	}
	pinned := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 20, Analysis: validAnalysis("SECRET_METRIC")},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		RunID: "SECRET_RUN_ID",
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Kind: "RolloutPolicy", Name: "guarded", Progression: omev1beta1.RolloutProgressionCanary,
			},
			PolicyGeneration: 4, PortableDigest: "rp1:aaaaaaaaaaaa", Group: pinned,
		}}},
	}}
	isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), &apis.Condition{
		Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionTrue,
		Reason: omev1beta1.RolloutPlanReasonPinned, Message: "SECRET_READY_MESSAGE",
	})
	isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutPlanDriftCondition), &apis.Condition{
		Type: apis.ConditionType(omev1beta1.RolloutPlanDriftCondition), Status: corev1.ConditionTrue,
		Reason: omev1beta1.RolloutPlanDriftReasonSpecNewerThanRun, Message: "SECRET_DRIFT_MESSAGE",
	})
	isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true", "secret": "SECRET_ANNOTATION"}
	isvc.Status.Canary.PreStepHold = true
	before := isvc.DeepCopy()

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, before, isvc)
	assert.Equal(t, reportv1alpha1.RolloutPlanSelection{
		Mode: reportv1alpha1.RolloutPlanModePinned, Evidence: reportv1alpha1.EvidenceReported,
	}, got.Content.Summary.EffectivePlan)
	assert.Equal(t, reportv1alpha1.RolloutPlanCondition{
		State: reportv1alpha1.RolloutConditionTrue, Reason: reportv1alpha1.RolloutPlanReasonPinned,
		Evidence: reportv1alpha1.EvidenceReported,
	}, got.Content.Summary.PlanReady)
	assert.Equal(t, reportv1alpha1.RolloutPlanCondition{
		State: reportv1alpha1.RolloutConditionTrue, Reason: reportv1alpha1.RolloutPlanReasonSpecNewer,
		Evidence: reportv1alpha1.EvidenceReported,
	}, got.Content.Summary.PlanDrift)
	require.Len(t, got.Content.DeclaredGroups, 1)
	assert.Equal(t, reportv1alpha1.EvidenceDeclared, got.Content.DeclaredGroups[0].Evidence)
	assert.Equal(t, "25%", got.Content.DeclaredGroups[0].Steps[0].Capacity)
	require.Len(t, got.Content.EffectiveGroups, 1)
	effective := got.Content.EffectiveGroups[0]
	assert.Equal(t, reportv1alpha1.EvidenceReported, effective.Evidence)
	assert.Equal(t, reportv1alpha1.RolloutPlanSourcePolicy, effective.Source)
	require.NotNil(t, effective.Policy)
	assert.Equal(t, int64(4), effective.Policy.Generation)
	assert.Equal(t, "rp1:aaaaaaaaaaaa", effective.Policy.Digest)
	assert.Equal(t, "rp1:aaaaaaaaaaaa", effective.PortableDigest)
	assert.Equal(t, "50%", effective.Steps[0].Capacity)
	assert.Equal(t, reportv1alpha1.RolloutGateAnalysis, effective.Steps[0].Gate)
	assert.NotEmpty(t, got.Content.Observed.Components)
	assert.Contains(t, got.Content.Holds, reportv1alpha1.RolloutExplainHold{
		Kind: reportv1alpha1.RolloutHoldGlobalPause, Evidence: reportv1alpha1.EvidenceDeclared,
	})
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{"SECRET_RUN_ID", "SECRET_METRIC", "SECRET_READY_MESSAGE", "SECRET_DRIFT_MESSAGE", "SECRET_ANNOTATION"} {
		assert.NotContains(t, string(encoded), secret)
	}
}

func TestProjectExplainAcceptsDerivedPinnedPolicyNameOnly(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source:         omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef:      &omev1beta1.RolloutPolicyRef{Name: "guarded"},
			PortableDigest: "rp1:aaaaaaaaaaaa",
			Group:          *isvc.Spec.Rollout.Groups[0].DeepCopy(),
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.EffectiveGroups, 1)
	require.NotNil(t, got.Content.EffectiveGroups[0].Policy)
	assert.Equal(t, "RolloutPolicy", got.Content.EffectiveGroups[0].Policy.Kind)
	assert.Equal(t, "guarded", got.Content.EffectiveGroups[0].Policy.Name)
	assert.Equal(t, "canary", got.Content.EffectiveGroups[0].Policy.Progression)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainKeepsIntentionallyEmptyPinnedPlanAuthoritative(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
	}}}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{}},
	}}
	isvc.Status.Conditions = duckv1.Conditions{
		{Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionTrue, Reason: omev1beta1.RolloutPlanReasonPinned},
		{Type: apis.ConditionType(omev1beta1.RolloutPlanDriftCondition), Status: corev1.ConditionFalse, Reason: omev1beta1.RolloutPlanDriftReasonInSync},
	}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutPlanModePinned, got.Content.Summary.EffectivePlan.Mode)
	require.Len(t, got.Content.DeclaredGroups, 1)
	assert.Empty(t, got.Content.EffectiveGroups)
	assert.Empty(t, got.Content.Observed.Groups)
}

func TestProjectExplainRejectsPinnedGroupWithoutExactlyOneInlineProgression(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa",
			Group: omev1beta1.RolloutGroup{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}},
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.EffectiveGroups, 1)
	assert.Equal(t, reportv1alpha1.RolloutStrategyUnknown, got.Content.EffectiveGroups[0].Strategy)
	assert.False(t, got.Content.EffectiveGroups[0].Defaulted)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainRejectsPinnedPolicyProgressionMismatch(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Kind: "RolloutPolicy", Name: "guarded", Progression: omev1beta1.RolloutProgressionBlueGreen,
			},
			PortableDigest: "rp1:aaaaaaaaaaaa",
			Group:          *isvc.Spec.Rollout.Groups[0].DeepCopy(),
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.EffectiveGroups, 1)
	assert.Nil(t, got.Content.EffectiveGroups[0].Policy)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainClassifiesMalformedPinnedStepAsActiveRunMalformed(t *testing.T) {
	isvc := activeCanaryInferenceService()
	pinned := *isvc.Spec.Rollout.Groups[0].DeepCopy()
	pinned.Canary.Steps[0].Traffic = 101
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa", Group: pinned,
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainDoesNotTreatForbiddenPinnedInnerRefAsLivePolicy(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa",
			Group: omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				PolicyRef: &omev1beta1.RolloutPolicyRef{
					Name: "forbidden", Progression: omev1beta1.RolloutProgressionCanary,
				},
			},
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutStrategyUnknown, got.Content.EffectiveGroups[0].Strategy)
	assert.Nil(t, got.Content.EffectiveGroups[0].Policy)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssuePolicyBodyUnavailable,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainDoesNotInventUnresolvedPolicyBody(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		PolicyRef: &omev1beta1.RolloutPolicyRef{
			Kind: "RolloutPolicy", Name: "guarded", Progression: omev1beta1.RolloutProgressionRollingUpdate,
		},
	}}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutPlanModeLive, got.Content.Summary.EffectivePlan.Mode)
	require.Len(t, got.Content.EffectiveGroups, 1)
	group := got.Content.EffectiveGroups[0]
	assert.Equal(t, reportv1alpha1.RolloutPlanSourcePolicy, group.Source)
	assert.Equal(t, reportv1alpha1.RolloutStrategyRollingUpdate, group.Strategy)
	assert.Nil(t, group.RollingUpdate)
	assert.Empty(t, group.Steps)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssuePolicyBodyUnavailable,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainAcceptsReportedUnresolvedPolicyIdentityWithoutInventingBody(t *testing.T) {
	isvc := baseInferenceService()
	policyRef := &omev1beta1.RolloutPolicyRef{
		Kind: "RolloutPolicy", Name: "guarded", Progression: omev1beta1.RolloutProgressionRollingUpdate,
	}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}, PolicyRef: policyRef,
	}}}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{Groups: []omev1beta1.RolloutGroupResolution{{
		Index: 0, Source: omev1beta1.RolloutPlanSourcePolicy, PolicyRef: policyRef.DeepCopy(),
	}}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.NotNil(t, got.Content.EffectiveGroups[0].Policy)
	assert.Equal(t, reportv1alpha1.EvidenceReported, got.Content.EffectiveGroups[0].Policy.Evidence)
	assert.Empty(t, got.Content.EffectiveGroups[0].Policy.Digest)
	assert.Nil(t, got.Content.EffectiveGroups[0].RollingUpdate)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssuePolicyBodyUnavailable,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueResolutionMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainDoesNotInventGateHoldFromCurrentStep(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Pause = &omev1beta1.RolloutPause{}
	isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
		RolloutPhase: omev1beta1.RolloutPhasePending,
	}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.NotContains(t, got.Content.Holds, reportv1alpha1.RolloutExplainHold{
		Kind: reportv1alpha1.RolloutHoldManualGate, Evidence: reportv1alpha1.EvidenceReported,
		Group: ptrInt(0), Step: ptrInt32(0),
	})
}

func TestProjectExplainClosesMalformedStatusAndPlanValuesWithoutPanicking(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Canary.CurrentStep = -1
	bad := omev1beta1.OnInconclusive("SECRET_ON_INCONCLUSIVE")
	isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis = validAnalysis("SECRET_METRIC")
	isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis.OnInconclusive = &bad
	isvc.Spec.Rollout.Groups[0].Canary.Prometheus = &omev1beta1.AnalysisPrometheus{
		ServerAddress: "SECRET_SERVER", Headers: map[string]string{"SECRET_HEADER": "SECRET_VALUE"},
	}

	var got reportv1alpha1.RolloutExplainReport
	require.NotPanics(t, func() {
		var err error
		got, err = rolloutprojection.ProjectExplain(isvc, fixedClock())
		require.NoError(t, err)
	})
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{
		"SECRET_ON_INCONCLUSIVE", "SECRET_METRIC", "SECRET_SERVER", "SECRET_HEADER", "SECRET_VALUE",
	} {
		assert.NotContains(t, string(encoded), secret)
	}
}

func TestProjectExplainRejectsDuplicatePlanConditions(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.Conditions = duckv1.Conditions{
		{Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionTrue, Reason: omev1beta1.RolloutPlanReasonNoRun},
		{Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionFalse, Reason: omev1beta1.RolloutPlanReasonPolicyNotFound},
	}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutConditionInvalid, got.Content.Summary.PlanReady.State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssuePlanConditionMalformed,
	})
}

func TestProjectExplainRejectsPlanConditionStatusReasonMismatch(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.Conditions = duckv1.Conditions{{
		Type:   apis.ConditionType(omev1beta1.RolloutPlanReadyCondition),
		Status: corev1.ConditionFalse, Reason: omev1beta1.RolloutPlanReasonPinned,
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutPlanCondition{
		State: reportv1alpha1.RolloutConditionInvalid, Reason: reportv1alpha1.RolloutPlanReasonUnknown,
		Evidence: reportv1alpha1.EvidenceReported,
	}, got.Content.Summary.PlanReady)
	assert.NotContains(t, got.Content.Holds, reportv1alpha1.RolloutExplainHold{
		Kind: reportv1alpha1.RolloutHoldPlanParked, Evidence: reportv1alpha1.EvidenceReported,
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssuePlanConditionMalformed,
	})
}

func TestProjectExplainRejectsPlanConditionModeMismatch(t *testing.T) {
	tests := []struct {
		name           string
		active         bool
		conditionType  string
		conditionState corev1.ConditionStatus
		reason         string
	}{
		{name: "active plan reports no run", active: true, conditionType: omev1beta1.RolloutPlanReadyCondition, conditionState: corev1.ConditionTrue, reason: omev1beta1.RolloutPlanReasonNoRun},
		{name: "live plan reports pinned", conditionType: omev1beta1.RolloutPlanReadyCondition, conditionState: corev1.ConditionTrue, reason: omev1beta1.RolloutPlanReasonPinned},
		{name: "live plan reports drift", conditionType: omev1beta1.RolloutPlanDriftCondition, conditionState: corev1.ConditionTrue, reason: omev1beta1.RolloutPlanDriftReasonSpecNewerThanRun},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := baseInferenceService()
			if tt.active {
				isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{}}
			}
			isvc.Status.Conditions = duckv1.Conditions{{
				Type: apis.ConditionType(tt.conditionType), Status: tt.conditionState, Reason: tt.reason,
			}}

			got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
			require.NoError(t, err)
			condition := got.Content.Summary.PlanReady
			if tt.conditionType == omev1beta1.RolloutPlanDriftCondition {
				condition = got.Content.Summary.PlanDrift
			}
			assert.Equal(t, reportv1alpha1.RolloutConditionInvalid, condition.State)
			assert.Equal(t, reportv1alpha1.RolloutPlanReasonUnknown, condition.Reason)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
				Code: reportv1alpha1.RolloutExplainIssuePlanConditionMalformed,
			})
		})
	}
}

func TestProjectExplainReportsPlanParkedOnlyFromValidFailureCondition(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.Conditions = duckv1.Conditions{{
		Type:   apis.ConditionType(omev1beta1.RolloutPlanReadyCondition),
		Status: corev1.ConditionFalse, Reason: omev1beta1.RolloutPlanReasonPolicyNotFound,
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutConditionFalse, got.Content.Summary.PlanReady.State)
	assert.Contains(t, got.Content.Holds, reportv1alpha1.RolloutExplainHold{
		Kind: reportv1alpha1.RolloutHoldPlanParked, Evidence: reportv1alpha1.EvidenceReported,
	})
}

func TestProjectExplainBoundsMalformedStoredPlan(t *testing.T) {
	isvc := baseInferenceService()
	for range 5 {
		isvc.Spec.Rollout = ensureRollout(isvc.Spec.Rollout)
		isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, omev1beta1.RolloutGroup{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			Canary: &omev1beta1.GroupCanary{
				Steps: []omev1beta1.RolloutGroupStep{{
					Capacity: intstr.FromString("100%"), Traffic: 100,
				}},
			},
		})
	}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.LessOrEqual(t, len(got.Content.DeclaredGroups), 3)
	assert.LessOrEqual(t, len(got.Content.EffectiveGroups), 3)
	assert.LessOrEqual(t, len(got.Content.Observed.Groups), 3)
	assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{Code: reportv1alpha1.WarningTruncated})
}

func TestProjectExplainReportsInlineDigestAndShadowedPolicyPreview(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Spec.Rollout.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{
		Kind: "RolloutPolicy", Name: "next-policy", Progression: omev1beta1.RolloutProgressionCanary,
	}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{Groups: []omev1beta1.RolloutGroupResolution{{
		Index: 0, Source: omev1beta1.RolloutPlanSourceInline,
		PolicyRef: &omev1beta1.RolloutPolicyRef{
			Kind: "RolloutPolicy", Name: "next-policy", Progression: omev1beta1.RolloutProgressionCanary,
		},
		ObservedDigest: "rp1:111111111111",
		ShadowedPolicyRef: &omev1beta1.ShadowedRolloutPolicyRef{
			Name: "next-policy", WouldPinDigest: "rp1:222222222222",
		},
	}}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.DeclaredGroups, 1)
	require.NotNil(t, got.Content.DeclaredGroups[0].ShadowedPolicy)
	assert.Equal(t, reportv1alpha1.EvidenceDeclared, got.Content.DeclaredGroups[0].ShadowedPolicy.Evidence)
	require.Len(t, got.Content.EffectiveGroups, 1)
	effective := got.Content.EffectiveGroups[0]
	assert.Equal(t, "rp1:111111111111", effective.PortableDigest)
	require.NotNil(t, effective.ShadowedPolicy)
	assert.Equal(t, "rp1:222222222222", effective.ShadowedPolicy.Digest)
	assert.Equal(t, reportv1alpha1.EvidenceReported, effective.ShadowedPolicy.Evidence)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueResolutionMissing,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainRejectsContradictoryLiveResolution(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{Groups: []omev1beta1.RolloutGroupResolution{{
		Index: 0, Source: omev1beta1.RolloutPlanSourcePolicy,
		PolicyRef: &omev1beta1.RolloutPolicyRef{
			Kind: "RolloutPolicy", Name: "unexpected", Progression: omev1beta1.RolloutProgressionCanary,
		},
		ObservedDigest: "rp1:111111111111",
	}}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.EffectiveGroups, 1)
	assert.Equal(t, reportv1alpha1.RolloutPlanSourceInline, got.Content.EffectiveGroups[0].Source)
	assert.Nil(t, got.Content.EffectiveGroups[0].Policy)
	assert.Empty(t, got.Content.EffectiveGroups[0].PortableDigest)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueResolutionMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainValidatesLiveResolutionIdentityProgressionAndDigests(t *testing.T) {
	policyGroup := func() *omev1beta1.InferenceService {
		isvc := baseInferenceService()
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "guarded", Progression: omev1beta1.RolloutProgressionCanary,
			},
		}}}
		return isvc
	}
	inlineShadowGroup := func() *omev1beta1.InferenceService {
		isvc := activeCanaryInferenceService()
		isvc.Spec.Rollout.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{
			Name: "shadowed", Progression: omev1beta1.RolloutProgressionBlueGreen,
		}
		return isvc
	}
	tests := []struct {
		name       string
		isvc       func() *omev1beta1.InferenceService
		resolution omev1beta1.RolloutGroupResolution
	}{
		{
			name: "policy name", isvc: policyGroup,
			resolution: omev1beta1.RolloutGroupResolution{
				Index: 0, Source: omev1beta1.RolloutPlanSourcePolicy,
				PolicyRef:      &omev1beta1.RolloutPolicyRef{Name: "other", Progression: omev1beta1.RolloutProgressionCanary},
				ObservedDigest: "rp1:111111111111",
			},
		},
		{
			name: "policy progression", isvc: policyGroup,
			resolution: omev1beta1.RolloutGroupResolution{
				Index: 0, Source: omev1beta1.RolloutPlanSourcePolicy,
				PolicyRef:      &omev1beta1.RolloutPolicyRef{Name: "guarded", Progression: omev1beta1.RolloutProgressionBlueGreen},
				ObservedDigest: "rp1:111111111111",
			},
		},
		{
			name: "policy digest", isvc: policyGroup,
			resolution: omev1beta1.RolloutGroupResolution{
				Index: 0, Source: omev1beta1.RolloutPlanSourcePolicy,
				PolicyRef:      &omev1beta1.RolloutPolicyRef{Name: "guarded", Progression: omev1beta1.RolloutProgressionCanary},
				ObservedDigest: "SECRET_DIGEST",
			},
		},
		{
			name: "inline digest", isvc: activeCanaryInferenceService,
			resolution: omev1beta1.RolloutGroupResolution{
				Index: 0, Source: omev1beta1.RolloutPlanSourceInline,
			},
		},
		{
			name: "missing shadow", isvc: inlineShadowGroup,
			resolution: omev1beta1.RolloutGroupResolution{
				Index: 0, Source: omev1beta1.RolloutPlanSourceInline,
				PolicyRef:      &omev1beta1.RolloutPolicyRef{Name: "shadowed", Progression: omev1beta1.RolloutProgressionBlueGreen},
				ObservedDigest: "rp1:111111111111",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := tt.isvc()
			isvc.Status.Rollout = &omev1beta1.RolloutStatus{Groups: []omev1beta1.RolloutGroupResolution{tt.resolution}}

			got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
			require.NoError(t, err)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
				Code: reportv1alpha1.RolloutExplainIssueResolutionMalformed,
				View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
			})
			assert.Empty(t, got.Content.EffectiveGroups[0].PortableDigest)
		})
	}
}

func TestProjectExplainRejectsDuplicateAndOutOfRangeResolutions(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{Groups: []omev1beta1.RolloutGroupResolution{
		{Index: 0, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: "rp1:111111111111"},
		{Index: 0, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: "rp1:222222222222"},
		{Index: 7, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: "rp1:333333333333"},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.EffectiveGroups, 1)
	assert.Empty(t, got.Content.EffectiveGroups[0].PortableDigest)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueResolutionMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueResolutionMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective,
	})
}

func TestProjectExplainBoundsHostileResolutionIssues(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{}
	for index := range 100 {
		isvc.Status.Rollout.Groups = append(isvc.Status.Rollout.Groups, omev1beta1.RolloutGroupResolution{
			Index: int32(index + 1), Source: omev1beta1.RolloutPlanSourceInline,
		})
	}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.LessOrEqual(t, len(got.Content.Issues), 32)
}

func TestProjectExplainDuplicateIssueFloodDoesNotHideDistinctResolutionFailure(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{}
	for index := range 100 {
		isvc.Status.Rollout.Groups = append(isvc.Status.Rollout.Groups, omev1beta1.RolloutGroupResolution{
			Index: int32(index + 1), Source: omev1beta1.RolloutPlanSourceInline,
		})
	}
	isvc.Status.Rollout.Groups = append(isvc.Status.Rollout.Groups, omev1beta1.RolloutGroupResolution{
		Index: 0, Source: omev1beta1.RolloutPlanSourcePolicy,
	})

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueResolutionMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective,
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueResolutionMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainProjectsDefaultedBudgetsRatioAndGatePrecedence(t *testing.T) {
	t.Run("default blue green", func(t *testing.T) {
		isvc := baseInferenceService()
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		}}}
		got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
		require.NoError(t, err)
		group := got.Content.EffectiveGroups[0]
		assert.Equal(t, reportv1alpha1.RolloutStrategyBlueGreen, group.Strategy)
		assert.Equal(t, reportv1alpha1.RolloutPlanSourceDefaulted, group.Source)
		assert.True(t, group.Defaulted)
	})

	t.Run("rolling budgets and unresolved operator ratio", func(t *testing.T) {
		isvc := baseInferenceService()
		isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
			Components:    []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.DecoderComponent},
			RollingUpdate: &omev1beta1.GroupRollingUpdate{}, MaintainRatio: &omev1beta1.MaintainRatio{},
		}}}
		got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
		require.NoError(t, err)
		group := got.Content.EffectiveGroups[0]
		require.NotNil(t, group.RollingUpdate)
		assert.Equal(t, reportv1alpha1.RolloutSetting{
			Value: "25%", Source: reportv1alpha1.RolloutSettingDefaulted,
			Effect: reportv1alpha1.RolloutSettingEffectApplied,
		}, group.RollingUpdate.MaxSurge)
		assert.Equal(t, reportv1alpha1.RolloutSetting{
			Value: "25%", Source: reportv1alpha1.RolloutSettingDefaulted,
			Effect: reportv1alpha1.RolloutSettingEffectApplied,
		}, group.RollingUpdate.MaxUnavailable)
		require.NotNil(t, group.MaintainRatio)
		assert.Equal(t, reportv1alpha1.RolloutSettingUnresolved, group.MaintainRatio.Tolerance.Source)
	})

	t.Run("canary gate precedence", func(t *testing.T) {
		isvc := baseInferenceService()
		duration := metav1.Duration{Duration: time.Minute}
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("25%"), Traffic: 10},
				{Capacity: intstr.FromString("50%"), Traffic: 20, Pause: &omev1beta1.RolloutPause{}},
				{Capacity: intstr.FromString("75%"), Traffic: 50, Pause: &omev1beta1.RolloutPause{Duration: &duration}},
				{Capacity: intstr.FromString("100%"), Traffic: 100, Pause: &omev1beta1.RolloutPause{Duration: &duration}, Analysis: validAnalysis("redacted")},
			}},
		}}}
		got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
		require.NoError(t, err)
		assert.Equal(t, []reportv1alpha1.RolloutGate{
			reportv1alpha1.RolloutGateImmediate,
			reportv1alpha1.RolloutGateManual,
			reportv1alpha1.RolloutGateTimed,
			reportv1alpha1.RolloutGateAnalysis,
		}, []reportv1alpha1.RolloutGate{
			got.Content.EffectiveGroups[0].Steps[0].Gate,
			got.Content.EffectiveGroups[0].Steps[1].Gate,
			got.Content.EffectiveGroups[0].Steps[2].Gate,
			got.Content.EffectiveGroups[0].Steps[3].Gate,
		})
	})
}

func TestProjectExplainReportsIgnoredFinalSoakAndSingleComponentRatio(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	tolerance := int32(7)
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
		{
			Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
			BlueGreen:  &omev1beta1.GroupBlueGreen{},
			Soak:       &metav1.Duration{Duration: time.Minute},
		},
		{
			Components:    []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			BlueGreen:     &omev1beta1.GroupBlueGreen{},
			Soak:          &metav1.Duration{Duration: 2 * time.Minute},
			MaintainRatio: &omev1beta1.MaintainRatio{Tolerance: &tolerance},
		},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.EffectiveGroups, 2)
	require.NotNil(t, got.Content.EffectiveGroups[0].Soak)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectApplied, got.Content.EffectiveGroups[0].Soak.Effect)
	require.NotNil(t, got.Content.EffectiveGroups[1].Soak)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectIgnoredFinalGroup, got.Content.EffectiveGroups[1].Soak.Effect)
	require.NotNil(t, got.Content.EffectiveGroups[1].MaintainRatio)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectIgnoredSingleComponent, got.Content.EffectiveGroups[1].MaintainRatio.Tolerance.Effect)
}

func TestProjectExplainSoakApplicabilityMatchesResolutionAndInlinePrecedence(t *testing.T) {
	t.Run("inline blue green ignores shadowed policy", func(t *testing.T) {
		isvc := baseInferenceService()
		isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
			{
				Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
				BlueGreen:  &omev1beta1.GroupBlueGreen{},
				PolicyRef: &omev1beta1.RolloutPolicyRef{
					Name: "shadowed", Progression: omev1beta1.RolloutProgressionCanary,
				},
				Soak: &metav1.Duration{Duration: time.Minute},
			},
			{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				BlueGreen:  &omev1beta1.GroupBlueGreen{},
			},
		}}

		got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
		require.NoError(t, err)
		require.NotNil(t, got.Content.DeclaredGroups[0].Soak)
		assert.Equal(t, reportv1alpha1.RolloutSettingEffectApplied, got.Content.DeclaredGroups[0].Soak.Effect)
	})

	t.Run("unresolved policy body is not applied evidence", func(t *testing.T) {
		isvc := baseInferenceService()
		isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
			{
				Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
				PolicyRef: &omev1beta1.RolloutPolicyRef{
					Name: "first", Progression: omev1beta1.RolloutProgressionBlueGreen,
				},
				Soak: &metav1.Duration{Duration: time.Minute},
			},
			{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				PolicyRef: &omev1beta1.RolloutPolicyRef{
					Name: "second", Progression: omev1beta1.RolloutProgressionBlueGreen,
				},
			},
		}}

		got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
		require.NoError(t, err)
		require.NotNil(t, got.Content.EffectiveGroups[0].Soak)
		assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[0].Soak.Effect)
		assert.NotEqual(t, reportv1alpha1.RolloutSettingEffectApplied, got.Content.EffectiveGroups[0].Soak.Effect)
	})

	t.Run("unresolved policy plan does not apply ratio", func(t *testing.T) {
		isvc := baseInferenceService()
		isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
		tolerance := int32(5)
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
			Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent},
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "guarded", Progression: omev1beta1.RolloutProgressionBlueGreen,
			},
			MaintainRatio: &omev1beta1.MaintainRatio{Tolerance: &tolerance},
		}}}

		got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
		require.NoError(t, err)
		require.NotNil(t, got.Content.EffectiveGroups[0].MaintainRatio)
		assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[0].MaintainRatio.Tolerance.Effect)
	})

	t.Run("single component ratio is ignored even when policy is unresolved", func(t *testing.T) {
		isvc := baseInferenceService()
		tolerance := int32(5)
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "guarded", Progression: omev1beta1.RolloutProgressionBlueGreen,
			},
			MaintainRatio: &omev1beta1.MaintainRatio{Tolerance: &tolerance},
		}}}

		got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
		require.NoError(t, err)
		require.NotNil(t, got.Content.EffectiveGroups[0].MaintainRatio)
		assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[0].MaintainRatio.Tolerance.Effect)
	})
}

func TestProjectExplainSoakUsesFinalCoordinationGroupAndExcludesCanary(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Spec.Router = &omev1beta1.RouterSpec{}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
		{
			Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
			BlueGreen:  &omev1beta1.GroupBlueGreen{},
			Soak:       &metav1.Duration{Duration: time.Minute},
		},
		{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			BlueGreen:  &omev1beta1.GroupBlueGreen{},
			Soak:       &metav1.Duration{Duration: 2 * time.Minute},
		},
		{
			Components: []omev1beta1.ComponentType{omev1beta1.RouterComponent},
			Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("100%"), Traffic: 100,
			}}},
			Soak: &metav1.Duration{Duration: 3 * time.Minute},
		},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.DeclaredGroups, 3)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.DeclaredGroups[0].Soak.Effect)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.DeclaredGroups[1].Soak.Effect)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.DeclaredGroups[2].Soak.Effect)
}

func ensureRollout(value *omev1beta1.RolloutSpec) *omev1beta1.RolloutSpec {
	if value == nil {
		return &omev1beta1.RolloutSpec{}
	}
	return value
}

func ptrInt32(value int32) *int32 { return &value }

func TestProjectExplainKeepsLiveResolutionSeparateFromPinnedPlan(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Spec.Rollout.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{
		Name: "next-policy", Progression: omev1beta1.RolloutProgressionCanary,
	}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{
		Groups: []omev1beta1.RolloutGroupResolution{{
			Index: 0, Source: omev1beta1.RolloutPlanSourceInline,
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "next-policy", Progression: omev1beta1.RolloutProgressionCanary,
			},
			ObservedDigest: "rp1:111111111111",
			ShadowedPolicyRef: &omev1beta1.ShadowedRolloutPolicyRef{
				Name: "next-policy", WouldPinDigest: "rp1:222222222222",
			},
		}},
		ActiveRun: &omev1beta1.RolloutRun{Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "pinned-policy", Progression: omev1beta1.RolloutProgressionCanary,
			},
			PolicyGeneration: 3, PortableDigest: "rp1:aaaaaaaaaaaa",
			Group: *isvc.Spec.Rollout.Groups[0].DeepCopy(),
		}}}},
	}
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.PolicyRef = nil

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.LiveGroups, 1)
	live := got.Content.LiveGroups[0]
	assert.Equal(t, "rp1:111111111111", live.PortableDigest)
	require.NotNil(t, live.ShadowedPolicy)
	assert.Equal(t, "rp1:222222222222", live.ShadowedPolicy.Digest)
	require.Len(t, got.Content.EffectiveGroups, 1)
	effective := got.Content.EffectiveGroups[0]
	assert.Equal(t, "pinned-policy", effective.Policy.Name)
	assert.Equal(t, "rp1:aaaaaaaaaaaa", effective.PortableDigest)
	assert.NotEqual(t, live.PortableDigest, effective.PortableDigest)
}

func TestProjectExplainAttributesMalformedCurrentResolutionToLiveView(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{
		Groups: []omev1beta1.RolloutGroupResolution{{
			Index: 0, Source: omev1beta1.RolloutPlanSourceInline, ObservedDigest: "not-a-digest",
		}},
		ActiveRun: &omev1beta1.RolloutRun{Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa",
			Group: *isvc.Spec.Rollout.Groups[0].DeepCopy(),
		}}}},
	}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueResolutionMalformed,
		View: reportv1alpha1.RolloutPlanViewLive, Group: ptrInt(0),
	})
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueResolutionMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func TestProjectExplainDoesNotUseLivePolicyDigestAsBodyEvidence(t *testing.T) {
	for _, reason := range []string{
		omev1beta1.RolloutPlanReasonPolicyNotReady,
		omev1beta1.RolloutPlanReasonProgressionMismatch,
	} {
		t.Run(reason, func(t *testing.T) {
			isvc := baseInferenceService()
			isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
			tolerance := int32(5)
			isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
				{
					Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
					PolicyRef:  &omev1beta1.RolloutPolicyRef{Name: "first", Progression: omev1beta1.RolloutProgressionBlueGreen},
					Soak:       &metav1.Duration{Duration: time.Minute},
				},
				{
					Components:    []omev1beta1.ComponentType{omev1beta1.EngineComponent},
					PolicyRef:     &omev1beta1.RolloutPolicyRef{Name: "second", Progression: omev1beta1.RolloutProgressionBlueGreen},
					Soak:          &metav1.Duration{Duration: 2 * time.Minute},
					MaintainRatio: &omev1beta1.MaintainRatio{Tolerance: &tolerance},
				},
			}}
			isvc.Status.Rollout = &omev1beta1.RolloutStatus{Groups: []omev1beta1.RolloutGroupResolution{
				{Index: 0, Source: omev1beta1.RolloutPlanSourcePolicy, PolicyRef: isvc.Spec.Rollout.Groups[0].PolicyRef.DeepCopy(), ObservedDigest: "rp1:111111111111"},
				{Index: 1, Source: omev1beta1.RolloutPlanSourcePolicy, PolicyRef: isvc.Spec.Rollout.Groups[1].PolicyRef.DeepCopy(), ObservedDigest: "rp1:222222222222"},
			}}
			isvc.Status.Conditions = duckv1.Conditions{{
				Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionFalse, Reason: reason,
			}}

			got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
			require.NoError(t, err)
			require.Empty(t, got.Content.LiveGroups)
			require.Len(t, got.Content.EffectiveGroups, 2)
			require.NotNil(t, got.Content.EffectiveGroups[0].Soak)
			assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[0].Soak.Effect)
			require.NotNil(t, got.Content.EffectiveGroups[1].MaintainRatio)
			assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[1].MaintainRatio.Tolerance.Effect)
			require.NotNil(t, got.Content.EffectiveGroups[1].Soak)
			assert.Equal(t, reportv1alpha1.RolloutSettingEffectIgnoredFinalGroup, got.Content.EffectiveGroups[1].Soak.Effect)
		})
	}
}

func TestProjectExplainReportsHonestProgressionOrigin(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
	}}}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa",
			Group: omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				BlueGreen:  &omev1beta1.GroupBlueGreen{},
			},
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutProgressionOriginDefaulted, got.Content.DeclaredGroups[0].ProgressionOrigin)
	assert.Equal(t, reportv1alpha1.RolloutProgressionOriginDefaulted, got.Content.LiveGroups[0].ProgressionOrigin)
	assert.Equal(t, reportv1alpha1.RolloutProgressionOriginUnknown, got.Content.EffectiveGroups[0].ProgressionOrigin)
}

func TestProjectExplainRejectsControllerInvalidPlanBodies(t *testing.T) {
	zero := intstr.FromInt(0)
	badPercent := intstr.FromString("101%")
	tolerance := int32(101)
	tests := []struct {
		name   string
		mutate func(*omev1beta1.RolloutGroup)
	}{
		{name: "ratio tolerance", mutate: func(g *omev1beta1.RolloutGroup) {
			g.BlueGreen = &omev1beta1.GroupBlueGreen{}
			g.MaintainRatio = &omev1beta1.MaintainRatio{Tolerance: &tolerance}
		}},
		{name: "rolling malformed budget", mutate: func(g *omev1beta1.RolloutGroup) {
			g.RollingUpdate = &omev1beta1.GroupRollingUpdate{MaxSurge: &badPercent}
		}},
		{name: "rolling zero budget", mutate: func(g *omev1beta1.RolloutGroup) {
			g.RollingUpdate = &omev1beta1.GroupRollingUpdate{MaxSurge: &zero, MaxUnavailable: &zero}
		}},
		{name: "empty canary", mutate: func(g *omev1beta1.RolloutGroup) {
			g.Canary = &omev1beta1.GroupCanary{}
		}},
		{name: "nonmonotonic canary", mutate: func(g *omev1beta1.RolloutGroup) {
			g.Canary = &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 50},
				{Capacity: intstr.FromString("100%"), Traffic: 40},
			}}
		}},
		{name: "incomplete canary", mutate: func(g *omev1beta1.RolloutGroup) {
			g.Canary = &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("75%"), Traffic: 75,
			}}}
		}},
		{name: "invalid analysis interval", mutate: func(g *omev1beta1.RolloutGroup) {
			g.Canary = &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("100%"), Traffic: 100,
				Analysis: &omev1beta1.RolloutAnalysis{FailureLimit: 1, Metrics: validAnalysis("metric").Metrics},
			}}}
		}},
		{name: "invalid analysis failure limit", mutate: func(g *omev1beta1.RolloutGroup) {
			analysis := validAnalysis("metric")
			analysis.FailureLimit = 0
			g.Canary = &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: analysis}}}
		}},
		{name: "empty analysis metrics", mutate: func(g *omev1beta1.RolloutGroup) {
			analysis := validAnalysis("metric")
			analysis.Metrics = nil
			g.Canary = &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: analysis}}}
		}},
		{name: "too many analysis metrics", mutate: func(g *omev1beta1.RolloutGroup) {
			analysis := validAnalysis("metric")
			for i := 1; i < 11; i++ {
				metric := analysis.Metrics[0]
				metric.Name = fmt.Sprintf("metric-%d", i)
				analysis.Metrics = append(analysis.Metrics, metric)
			}
			g.Canary = &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100, Analysis: analysis}}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := baseInferenceService()
			group := omev1beta1.RolloutGroup{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}}
			tt.mutate(&group)
			isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{group}}
			isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
				Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
					Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa",
					Group: omev1beta1.RolloutGroup{
						Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
						BlueGreen:  &omev1beta1.GroupBlueGreen{},
					},
				}}},
			}}

			got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
			require.NoError(t, err)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
				Code: reportv1alpha1.RolloutExplainIssueDeclaredPlanMalformed,
				View: reportv1alpha1.RolloutPlanViewDeclared, Group: ptrInt(0),
			})
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
				Code: reportv1alpha1.RolloutExplainIssueLivePlanMalformed,
				View: reportv1alpha1.RolloutPlanViewLive, Group: ptrInt(0),
			})
			assertPlanSettingsNeverApplied(t, got.Content.LiveGroups[0])
		})
	}
}

func TestProjectExplainRejectsControllerInvalidPinnedBody(t *testing.T) {
	isvc := activeCanaryInferenceService()
	pinned := *isvc.Spec.Rollout.Groups[0].DeepCopy()
	pinned.Canary.Steps[1].Traffic = 90
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa", Group: pinned,
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueEffectivePlanMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
	assertPlanSettingsNeverApplied(t, got.Content.EffectiveGroups[0])
}

func TestProjectExplainRejectsControllerInvalidPinnedPlanShape(t *testing.T) {
	isvc := baseInferenceService()
	pinned := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
		Soak:       &metav1.Duration{Duration: time.Minute},
	}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa", Group: pinned,
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
	require.NotNil(t, got.Content.EffectiveGroups[0].Soak)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[0].Soak.Effect)
}

func TestProjectExplainEvaluatesValidatedPinnedPolicyBody(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	pinnedGroups := []omev1beta1.RolloutRunGroup{
		{
			Source: omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "first", Progression: omev1beta1.RolloutProgressionBlueGreen,
			},
			PolicyGeneration: 1, PortableDigest: "rp1:aaaaaaaaaaaa",
			Group: omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
				BlueGreen:  &omev1beta1.GroupBlueGreen{},
				Soak:       &metav1.Duration{Duration: time.Minute},
			},
		},
		{
			Source: omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "second", Progression: omev1beta1.RolloutProgressionBlueGreen,
			},
			PolicyGeneration: 1, PortableDigest: "rp1:bbbbbbbbbbbb",
			Group: omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				BlueGreen:  &omev1beta1.GroupBlueGreen{},
			},
		},
	}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: pinnedGroups},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.NotNil(t, got.Content.EffectiveGroups[0].Soak)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectApplied, got.Content.EffectiveGroups[0].Soak.Effect)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(0),
	})
}

func assertPlanSettingsNeverApplied(t *testing.T, group reportv1alpha1.RolloutPlanGroup) {
	t.Helper()
	if group.Soak != nil {
		assert.NotEqual(t, reportv1alpha1.RolloutSettingEffectApplied, group.Soak.Effect)
	}
	if group.RollingUpdate != nil {
		assert.NotEqual(t, reportv1alpha1.RolloutSettingEffectApplied, group.RollingUpdate.MaxSurge.Effect)
		assert.NotEqual(t, reportv1alpha1.RolloutSettingEffectApplied, group.RollingUpdate.MaxUnavailable.Effect)
	}
	if group.MaintainRatio != nil {
		assert.NotEqual(t, reportv1alpha1.RolloutSettingEffectApplied, group.MaintainRatio.Tolerance.Effect)
	}
}

func TestProjectExplainRejectsContextInvalidPinnedRollingUpdate(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa",
			Group: omev1beta1.RolloutGroup{
				Components:    []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
				RollingUpdate: &omev1beta1.GroupRollingUpdate{},
			},
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Observed.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueSpecMalformed,
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective,
	})
	require.Len(t, got.Content.EffectiveGroups, 1)
	require.NotNil(t, got.Content.EffectiveGroups[0].RollingUpdate)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[0].RollingUpdate.MaxSurge.Effect)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[0].RollingUpdate.MaxUnavailable.Effect)
}

func TestProjectExplainRejectsContextInvalidPinnedMaintainRatio(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{
		Annotations: map[string]string{constants.DeploymentMode: string(constants.RawDeployment)},
	}}
	tolerance := int32(5)
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: "rp1:aaaaaaaaaaaa",
			Group: omev1beta1.RolloutGroup{
				Components:    []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.DecoderComponent},
				BlueGreen:     &omev1beta1.GroupBlueGreen{},
				MaintainRatio: &omev1beta1.MaintainRatio{Tolerance: &tolerance},
			},
		}}},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Observed.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueSpecMalformed,
	})
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective,
	})
	require.Len(t, got.Content.EffectiveGroups, 1)
	require.NotNil(t, got.Content.EffectiveGroups[0].MaintainRatio)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[0].MaintainRatio.Tolerance.Effect)
}

func TestProjectExplainFinalPolicyOnlySoakIsProvablyIgnored(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
		{
			Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
			PolicyRef:  &omev1beta1.RolloutPolicyRef{Name: "first", Progression: omev1beta1.RolloutProgressionBlueGreen},
			Soak:       &metav1.Duration{Duration: time.Minute},
		},
		{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			PolicyRef:  &omev1beta1.RolloutPolicyRef{Name: "second", Progression: omev1beta1.RolloutProgressionBlueGreen},
			Soak:       &metav1.Duration{Duration: 2 * time.Minute},
		},
	}}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{Groups: []omev1beta1.RolloutGroupResolution{
		{
			Index: 0, Source: omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef: isvc.Spec.Rollout.Groups[0].PolicyRef.DeepCopy(), ObservedDigest: "rp1:111111111111",
		},
		{
			Index: 1, Source: omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef: isvc.Spec.Rollout.Groups[1].PolicyRef.DeepCopy(), ObservedDigest: "rp1:222222222222",
		},
	}}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.EffectiveGroups, 2)
	require.NotNil(t, got.Content.EffectiveGroups[0].Soak)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectUnknown, got.Content.EffectiveGroups[0].Soak.Effect)
	require.NotNil(t, got.Content.EffectiveGroups[1].Soak)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectIgnoredFinalGroup, got.Content.EffectiveGroups[1].Soak.Effect)
}
