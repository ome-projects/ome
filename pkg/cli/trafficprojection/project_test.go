package trafficprojection_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	knapis "knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/trafficprojection"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/validation"
)

var projectionClock = reportv1alpha1.ClockFunc(func() time.Time {
	return time.Date(2026, 9, 14, 17, 0, 0, 0, time.UTC)
})

func TestProjectCurrentTrafficEvidence(t *testing.T) {
	isvc := currentTrafficISVC(t)
	before := isvc.DeepCopy()

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, before, isvc, "projection must not mutate API evidence")
	assert.Equal(t, reportv1alpha1.TrafficStatePending, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficTranslatorEnvoyGateway, got.Content.Summary.Translator)
	assert.Equal(t, reportv1alpha1.TrafficAlgorithmRoundRobin, got.Content.Summary.Algorithm)
	assert.Equal(t, reportv1alpha1.TrafficConditionValue{Status: reportv1alpha1.TrafficConditionUnknown, Reason: reportv1alpha1.TrafficReasonPending}, got.Content.Summary.PolicyReady)
	assert.Equal(t, reportv1alpha1.TrafficUnsupportedNone, got.Content.Summary.Unsupported)
	assert.Equal(t, reportv1alpha1.TrafficFreshnessCurrent, got.Content.Summary.Source.PolicyReady.Freshness)
	require.NotNil(t, got.Content.Policy)
	assert.Equal(t, reportv1alpha1.TrafficPolicyBackendTrafficPolicy, got.Content.Policy.Kind)
	assert.Equal(t, []string{"chat", "chat-router"}, trafficRouteNames(got.Content.Routes))
	assert.Equal(t, []string{"http://chat-engine.prod.svc.cluster.local", "https://chat.prod.example/"}, trafficEndpointURLs(got.Content.Endpoints))
	require.NotNil(t, got.Content.Canary)
	assert.Equal(t, int32(0), got.Content.Canary.CurrentStep)
	assert.Equal(t, int32(2), got.Content.Canary.TotalSteps)
	assert.Equal(t, int32(20), got.Content.Canary.ObservedTraffic)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Canary.Component)
	assert.Equal(t, []reportv1alpha1.TrafficAllocationRole{reportv1alpha1.TrafficRoleStable, reportv1alpha1.TrafficRoleCanary}, trafficAllocationRoles(got.Content.Allocations))
	assert.Empty(t, got.Content.Issues)
	assert.Empty(t, got.Warnings)
	require.Len(t, got.Sources, 1)
	assert.Equal(t, "uid-chat", got.Sources[0].UID)
	assert.Equal(t, int64(7), got.Sources[0].Generation)
}

func TestProjectMapsControllerConditionsHonestly(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*omev1beta1.InferenceService)
		wantState   reportv1alpha1.TrafficState
		translator  reportv1alpha1.TrafficTranslator
		unsupported reportv1alpha1.TrafficUnsupportedState
		freshness   reportv1alpha1.TrafficFreshness
	}{
		{name: "accepted", mutate: func(isvc *omev1beta1.InferenceService) {
			setReadyCondition(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
		}, wantState: reportv1alpha1.TrafficStateReported, translator: reportv1alpha1.TrafficTranslatorEnvoyGateway, unsupported: reportv1alpha1.TrafficUnsupportedNone, freshness: reportv1alpha1.TrafficFreshnessCurrent},
		{name: "pending", mutate: func(*omev1beta1.InferenceService) {}, wantState: reportv1alpha1.TrafficStatePending, translator: reportv1alpha1.TrafficTranslatorEnvoyGateway, unsupported: reportv1alpha1.TrafficUnsupportedNone, freshness: reportv1alpha1.TrafficFreshnessCurrent},
		{name: "gateway rejected", mutate: func(isvc *omev1beta1.InferenceService) {
			setReadyCondition(isvc, metav1.ConditionFalse, omev1beta1.TrafficReasonGatewayRejected, 7)
		}, wantState: reportv1alpha1.TrafficStateDegraded, translator: reportv1alpha1.TrafficTranslatorEnvoyGateway, unsupported: reportv1alpha1.TrafficUnsupportedNone, freshness: reportv1alpha1.TrafficFreshnessCurrent},
		{name: "conflicting policy", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Traffic.BackendPolicyResource = nil
			isvc.Status.Traffic.TargetedHTTPRoutes = nil
			setReadyCondition(isvc, metav1.ConditionFalse, omev1beta1.TrafficReasonConflictingPolicy, 7)
		}, wantState: reportv1alpha1.TrafficStateDegraded, translator: reportv1alpha1.TrafficTranslatorUnavailable, unsupported: reportv1alpha1.TrafficUnsupportedUnknown, freshness: reportv1alpha1.TrafficFreshnessCurrent},
		{name: "unsupported fields", mutate: func(isvc *omev1beta1.InferenceService) {
			setReadyCondition(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
			isvc.Status.Traffic.Conditions = append(isvc.Status.Traffic.Conditions, trafficCondition(omev1beta1.TrafficConditionBackendPolicyUnsupportedFields, metav1.ConditionTrue, omev1beta1.TrafficReasonUnsupportedField, 7))
		}, wantState: reportv1alpha1.TrafficStatePartial, translator: reportv1alpha1.TrafficTranslatorEnvoyGateway, unsupported: reportv1alpha1.TrafficUnsupportedPresent, freshness: reportv1alpha1.TrafficFreshnessCurrent},
		{name: "no translator", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Traffic.BackendPolicyResource = nil
			isvc.Status.Traffic.TargetedHTTPRoutes = nil
			setReadyCondition(isvc, metav1.ConditionFalse, omev1beta1.TrafficReasonNoTranslatorAvailable, 7)
		}, wantState: reportv1alpha1.TrafficStateDegraded, translator: reportv1alpha1.TrafficTranslatorUnavailable, unsupported: reportv1alpha1.TrafficUnsupportedUnknown, freshness: reportv1alpha1.TrafficFreshnessCurrent},
		{name: "translation failed", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Traffic.BackendPolicyResource = nil
			isvc.Status.Traffic.TargetedHTTPRoutes = nil
			setReadyCondition(isvc, metav1.ConditionFalse, omev1beta1.TrafficReasonTranslationFailed, 7)
		}, wantState: reportv1alpha1.TrafficStateDegraded, translator: reportv1alpha1.TrafficTranslatorUnavailable, unsupported: reportv1alpha1.TrafficUnsupportedUnknown, freshness: reportv1alpha1.TrafficFreshnessCurrent},
		{name: "stale", mutate: func(isvc *omev1beta1.InferenceService) {
			setReadyCondition(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 6)
		}, wantState: reportv1alpha1.TrafficStatePartial, translator: reportv1alpha1.TrafficTranslatorEnvoyGateway, unsupported: reportv1alpha1.TrafficUnsupportedUnknown, freshness: reportv1alpha1.TrafficFreshnessStale},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			tt.mutate(isvc)
			got, err := trafficprojection.Project(isvc, projectionClock)
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, got.Content.Summary.State)
			assert.Equal(t, tt.translator, got.Content.Summary.Translator)
			assert.Equal(t, tt.unsupported, got.Content.Summary.Unsupported)
			assert.Equal(t, tt.freshness, got.Content.Summary.Source.PolicyReady.Freshness)
		})
	}
}

func TestProjectMissingTrafficStatusIsUnavailable(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Traffic = nil

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficStateUnavailable, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficUnsupportedUnknown, got.Content.Summary.Unsupported)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueTrafficStatusMissing})
}

func TestProjectMarksTranslatorUnavailableWithoutARecognizedPolicy(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Traffic.BackendPolicyResource = nil
	isvc.Status.Traffic.TargetedHTTPRoutes = nil
	setReadyCondition(isvc, metav1.ConditionFalse, omev1beta1.TrafficReasonConflictingPolicy, 7)

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficTranslatorUnavailable, got.Content.Summary.Translator)
	assert.Equal(t, reportv1alpha1.TrafficValueSource{
		Evidence:  reportv1alpha1.EvidenceComputed,
		Freshness: reportv1alpha1.TrafficFreshnessUnavailable,
	}, got.Content.Summary.Source.Translator)
}

func TestProjectKeepsNoTranslatorUnavailable(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Traffic.BackendPolicyResource = nil
	isvc.Status.Traffic.TargetedHTTPRoutes = nil
	setReadyCondition(isvc, metav1.ConditionFalse, omev1beta1.TrafficReasonNoTranslatorAvailable, 7)

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficTranslatorUnavailable, got.Content.Summary.Translator)
	assert.Equal(t, reportv1alpha1.TrafficValueSource{
		Evidence: reportv1alpha1.EvidenceComputed, Freshness: reportv1alpha1.TrafficFreshnessCurrent,
	}, got.Content.Summary.Source.Translator)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), `"translator":"noop"`)
}

func TestProjectDoesNotInferNoopFromInvalidReadyCondition(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*metav1.Condition)
	}{
		{
			name: "wrong status",
			mutate: func(condition *metav1.Condition) {
				condition.Status = metav1.ConditionUnknown
			},
		},
		{
			name: "missing transition time",
			mutate: func(condition *metav1.Condition) {
				condition.LastTransitionTime = metav1.Time{}
			},
		},
		{
			name: "negative observed generation",
			mutate: func(condition *metav1.Condition) {
				condition.ObservedGeneration = -1
			},
		},
		{
			name: "missing observed generation",
			mutate: func(condition *metav1.Condition) {
				condition.ObservedGeneration = 0
			},
		},
		{
			name: "future observed generation",
			mutate: func(condition *metav1.Condition) {
				condition.ObservedGeneration = 8
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			isvc.Status.Traffic.BackendPolicyResource = nil
			isvc.Status.Traffic.TargetedHTTPRoutes = nil
			setReadyCondition(isvc, metav1.ConditionFalse, omev1beta1.TrafficReasonNoTranslatorAvailable, 7)
			tt.mutate(&isvc.Status.Traffic.Conditions[0])

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.TrafficStateInvalid, got.Content.Summary.State)
			assert.Equal(t, reportv1alpha1.TrafficTranslatorUnavailable, got.Content.Summary.Translator)
			assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, got.Content.Summary.Source.Algorithm.Freshness)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueConditionInvalid})
		})
	}
}

func TestProjectMissingConditionGenerationCannotBindPresentTrafficFields(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Traffic.Conditions[0].ObservedGeneration = 0

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"STATE", "-", "Invalid", "Computed/Unverifiable"},
		{"TRANSLATOR", "-", "envoy-gateway", "Computed/Unverifiable"},
		{"ALGORITHM", "-", "RoundRobin", "Reported/Unverifiable"},
		{"POLICY-READY", "-", "Unknown/Pending", "Reported/Unverifiable"},
		{"UNSUPPORTED", "-", "Unknown", "Reported/Unverifiable"},
		{"ROUTES", "-", "2", "Reported/Unverifiable"},
		{"ENDPOINTS", "-", "2", "Reported/Unverifiable"},
		{"CANARY", "engine", "1/2 @ 20%", "Reported/Unverifiable"},
		{"WEIGHT", "engine", "stable:a1b2c3d4=80%", "Reported/Unverifiable"},
		{"WEIGHT", "engine", "canary:e5f6a7b8=20%", "Reported/Unverifiable"},
		{"ISSUE", "-", "ConditionInvalid", "Computed/Unverifiable"},
	}, got.Table().Rows)
	require.NotNil(t, got.Content.Policy)
	assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, got.Content.Policy.Source.Freshness)
	for _, route := range got.Content.Routes {
		assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, route.Source.Freshness)
	}
	var rendered bytes.Buffer
	require.NoError(t, report.Write(&rendered, report.FormatJSON, got))
	assert.Contains(t, rendered.String(), `"observedGeneration": 0`)
	assert.Contains(t, rendered.String(), `"code": "ConditionInvalid"`)
	assert.NotContains(t, rendered.String(), `"freshness": "Stale"`)
}

func TestProjectRejectsNoTranslatorConditionWithRecognizedPolicy(t *testing.T) {
	isvc := currentTrafficISVC(t)
	setReadyCondition(isvc, metav1.ConditionFalse, omev1beta1.TrafficReasonNoTranslatorAvailable, 7)

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"STATE", "-", "Invalid", "Computed/Unverifiable"},
		{"TRANSLATOR", "-", "Unavailable", "Computed/Unverifiable"},
		{"ALGORITHM", "-", "RoundRobin", "Reported/Current"},
		{"POLICY-READY", "-", "False/NoTranslatorAvailable", "Reported/Current"},
		{"UNSUPPORTED", "-", "Unknown", "Reported/Current"},
	}, got.Table().Rows[:5])
	assert.Equal(t, [][]string{{"ISSUE", "-", "StatusCombinationInvalid", "Computed/Unverifiable"}}, tableRows(got.Table().Rows, "ISSUE"))
	require.NotNil(t, got.Content.Policy, "retain the allowlisted reported policy for diagnosis")
	var rendered bytes.Buffer
	require.NoError(t, report.Write(&rendered, report.FormatJSON, got))
	assert.Contains(t, rendered.String(), `"translator": "Unavailable"`)
	assert.Contains(t, rendered.String(), `"code": "StatusCombinationInvalid"`)
	assert.NotContains(t, rendered.String(), `"translator": "noop"`)
}

func TestProjectDoesNotInferNoopFromConflictingReadyConditions(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Traffic.BackendPolicyResource = nil
	isvc.Status.Traffic.TargetedHTTPRoutes = nil
	setReadyCondition(isvc, metav1.ConditionFalse, omev1beta1.TrafficReasonNoTranslatorAvailable, 7)
	isvc.Status.Traffic.Conditions = append(
		isvc.Status.Traffic.Conditions,
		trafficCondition(
			omev1beta1.TrafficConditionBackendPolicyReady,
			metav1.ConditionTrue,
			omev1beta1.TrafficReasonAcceptedByGateway,
			7,
		),
	)

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficStateInvalid, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficTranslatorUnavailable, got.Content.Summary.Translator)
	assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, got.Content.Summary.Source.Algorithm.Freshness)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueConditionConflict})
}

func TestProjectMarksPresentTrafficFieldsUnverifiableWithoutReadyCondition(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Traffic.Conditions = nil

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficStateUnavailable, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, got.Content.Summary.Source.Algorithm.Freshness)
	assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, got.Content.Summary.Source.Translator.Freshness)
	require.NotNil(t, got.Content.Policy)
	assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, got.Content.Policy.Source.Freshness)
	require.NotEmpty(t, got.Content.Routes)
	for _, route := range got.Content.Routes {
		assert.Equal(t, reportv1alpha1.TrafficFreshnessUnverifiable, route.Source.Freshness)
	}
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssuePolicyConditionMissing})
}

func TestProjectAllowsAValidStatusBeforeEndpointsArePublished(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Addresses = nil
	isvc.Status.URL = nil
	isvc.Status.Address = nil

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficStatePending, got.Content.Summary.State)
	assert.Empty(t, got.Content.Endpoints)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueEndpointInvalid})
}

func TestProjectCanaryUsesPinnedActiveRunPlan(t *testing.T) {
	isvc := currentTrafficISVC(t)
	pinnedGroup := isvc.Spec.Rollout.Groups[0]
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{Group: pinnedGroup}}},
	}}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.RouterComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
			Capacity: intstr.FromString("100%"), Traffic: 100,
		}}},
	}}}

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	require.NotNil(t, got.Content.Canary)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Canary.Component)
	assert.Equal(t, int32(2), got.Content.Canary.TotalSteps)
}

func TestProjectUsesPerUnitCanaryInsteadOfLegacyAlias(t *testing.T) {
	isvc := currentTrafficISVC(t)
	engine := isvc.Status.Components[omev1beta1.EngineComponent]
	engine.Canary = isvc.Status.Canary.DeepCopy()
	isvc.Status.Components[omev1beta1.EngineComponent] = engine
	isvc.Status.Canary = &omev1beta1.CanaryStatus{CurrentStep: 99, ObservedTrafficWeight: 99}

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	require.NotNil(t, got.Content.Canary)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Canary.Component)
	assert.Equal(t, int32(20), got.Content.Canary.ObservedTraffic)
	assert.Empty(t, got.Content.Issues)
}

func TestProjectConcurrentCanariesKeepUnitAllocationsWithoutInventingGlobalStep(t *testing.T) {
	isvc := concurrentCanaryTrafficISVC(t)
	setReadyCondition(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
	before := isvc.DeepCopy()

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, before, isvc, "projection must not mutate API evidence")
	assert.Nil(t, got.Content.Canary, "one canary field cannot represent two independent steps")
	require.Len(t, got.Content.Canaries, 2)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Canaries[0].Component)
	assert.Equal(t, int32(0), got.Content.Canaries[0].CurrentStep)
	assert.Equal(t, int32(20), got.Content.Canaries[0].ObservedTraffic)
	assert.Equal(t, reportv1alpha1.RuntimeComponentRouter, got.Content.Canaries[1].Component)
	assert.Equal(t, int32(1), got.Content.Canaries[1].CurrentStep)
	assert.Equal(t, int32(50), got.Content.Canaries[1].ObservedTraffic)
	assert.Equal(t, reportv1alpha1.TrafficStateReported, got.Content.Summary.State)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueCanaryInvalid})
	assert.Empty(t, got.Warnings)
	assert.Equal(t, [][]string{
		{"CANARY", "engine", "1/2 @ 20%", "Reported/Unverifiable"},
		{"CANARY", "router", "2/3 @ 50%", "Reported/Unverifiable"},
	}, tableRows(got.Table().Rows, "CANARY"))
	for _, component := range []reportv1alpha1.RuntimeComponentType{reportv1alpha1.RuntimeComponentEngine, reportv1alpha1.RuntimeComponentRouter} {
		allocations := allocationsForComponent(got.Content.Allocations, component)
		require.Len(t, allocations, 2)
		assert.Equal(t, []reportv1alpha1.TrafficAllocationRole{reportv1alpha1.TrafficRoleStable, reportv1alpha1.TrafficRoleCanary},
			[]reportv1alpha1.TrafficAllocationRole{allocations[0].Role, allocations[1].Role})
	}
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, got))
	assert.NotContains(t, output.String(), "SECRET_")
	t.Logf("concurrent-canary traffic status (fixture):\n%s", output.String())
}

func TestProjectConcurrentCanariesDoNotReuseAliasForMissingUnit(t *testing.T) {
	isvc := concurrentCanaryTrafficISVC(t)
	router := isvc.Status.Components[omev1beta1.RouterComponent]
	router.Canary = nil
	isvc.Status.Components[omev1beta1.RouterComponent] = router

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Nil(t, got.Content.Canary)
	require.Len(t, got.Content.Canaries, 1)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Canaries[0].Component)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
		Code: reportv1alpha1.TrafficIssueCanaryInvalid, Component: reportv1alpha1.RuntimeComponentRouter,
	})
}

func TestProjectConcurrentCanariesKeepHealthyUnitWhenSiblingStatusIsMalformed(t *testing.T) {
	isvc := concurrentCanaryTrafficISVC(t)
	router := isvc.Status.Components[omev1beta1.RouterComponent]
	router.Canary.CurrentStep = 99
	isvc.Status.Components[omev1beta1.RouterComponent] = router

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Nil(t, got.Content.Canary)
	require.Len(t, got.Content.Canaries, 1)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Canaries[0].Component)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
		Code: reportv1alpha1.TrafficIssueCanaryInvalid, Component: reportv1alpha1.RuntimeComponentRouter,
	})
}

func TestProjectRejectsDuplicateCanaryUnitEvenWithoutStatus(t *testing.T) {
	isvc := concurrentCanaryTrafficISVC(t)
	isvc.Spec.Rollout.Groups[1].Components = []omev1beta1.ComponentType{omev1beta1.EngineComponent}
	engine := isvc.Status.Components[omev1beta1.EngineComponent]
	engine.Canary = nil
	engine.RolloutPhase = omev1beta1.RolloutPhaseStable
	isvc.Status.Components[omev1beta1.EngineComponent] = engine

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficStateInvalid, got.Content.Summary.State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
		Code:      reportv1alpha1.TrafficIssueCanaryInvalid,
		Component: reportv1alpha1.RuntimeComponentEngine,
	})
}

func concurrentCanaryTrafficISVC(t *testing.T) *omev1beta1.InferenceService {
	t.Helper()
	isvc := currentTrafficISVC(t)
	mode := constants.OMENative
	isvc.Spec.DeploymentMode = &mode
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	isvc.Spec.Router = &omev1beta1.RouterSpec{}
	concurrent := omev1beta1.RolloutGroupOrderingConcurrent
	isvc.Spec.Rollout.GroupOrdering = &concurrent
	isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.RouterComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("30%"), Traffic: 30},
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	})
	engine := isvc.Status.Components[omev1beta1.EngineComponent]
	engine.Canary = isvc.Status.Canary.DeepCopy()
	isvc.Status.Components[omev1beta1.EngineComponent] = engine
	routerCanary := &omev1beta1.CanaryStatus{
		CurrentStep: 1, ObservedTrafficWeight: 50,
		StableRevisionHash: "11112222", CanaryRevisionHash: "33334444",
	}
	isvc.Status.Components[omev1beta1.RouterComponent] = omev1beta1.ComponentStatusSpec{
		Canary:       routerCanary,
		RolloutPhase: omev1beta1.RolloutPhaseCanarying,
		Traffic: []omev1beta1.ComponentTrafficTarget{
			{RevisionName: "chat-router-rev-11112222", Percent: 50},
			{RevisionName: "chat-router-rev-33334444", Percent: 50, LatestRevision: true},
		},
	}
	isvc.Status.Canary = routerCanary.DeepCopy()
	require.NoError(t, validation.ValidateCanary(&isvc.Spec), "fixture must be a valid concurrent canary plan")
	return isvc
}

func TestProjectRejectsMissingCanaryStatusForEveryRequiredPrimaryPhase(t *testing.T) {
	for _, phase := range []omev1beta1.RolloutPhase{
		omev1beta1.RolloutPhasePending,
		omev1beta1.RolloutPhaseCanarying,
		omev1beta1.RolloutPhasePaused,
		omev1beta1.RolloutPhasePromoting,
		omev1beta1.RolloutPhaseRollingBack,
		omev1beta1.RolloutPhaseRolledBack,
		omev1beta1.RolloutPhaseFailed,
	} {
		t.Run(string(phase), func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			isvc.Status.Canary = nil
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = phase
			component.LatestRolledoutRevision = "chat-engine-rev-a1b2c3d4"
			isvc.Status.Components[omev1beta1.EngineComponent] = component

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.TrafficStateInvalid, got.Content.Summary.State)
			assert.Nil(t, got.Content.Canary)
			assert.Equal(t, []reportv1alpha1.TrafficIssue{{Code: reportv1alpha1.TrafficIssueCanaryInvalid}}, got.Content.Issues)
			assert.Equal(t, [][]string{{"ISSUE", "-", "CanaryInvalid", "Computed/Unverifiable"}}, tableRows(got.Table().Rows, "ISSUE"))
			var rendered bytes.Buffer
			require.NoError(t, report.Write(&rendered, report.FormatJSON, got))
			assert.Contains(t, rendered.String(), `"code": "CanaryInvalid"`)
			assert.NotContains(t, rendered.String(), `"canary":`)
		})
	}
}

func TestProjectAllowsMissingCanaryStatusOutsideRequiredPhases(t *testing.T) {
	for _, phase := range []omev1beta1.RolloutPhase{
		"",
		omev1beta1.RolloutPhaseStable,
		omev1beta1.RolloutPhaseBlueGreenStandby,
	} {
		t.Run(string(phase), func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			isvc.Status.Canary = nil
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = phase
			component.Traffic = []omev1beta1.ComponentTrafficTarget{{
				RevisionName: "chat-engine-rev-e5f6a7b8", Percent: 100, LatestRevision: true,
			}}
			isvc.Status.Components[omev1beta1.EngineComponent] = component

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueCanaryInvalid})
		})
	}
}

func TestProjectUsesRolledOutRevisionAsTheOnlyStableTargetDuringSplit(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Spec.Rollout = nil
	isvc.Status.Canary = nil
	component := isvc.Status.Components[omev1beta1.EngineComponent]
	component.RolloutPhase = omev1beta1.RolloutPhaseStable
	component.LatestRolledoutRevision = "chat-engine-rev-a1b2c3d4"
	component.Traffic = []omev1beta1.ComponentTrafficTarget{
		{RevisionName: "chat-engine-rev-e5f6a7b8", Percent: 50, LatestRevision: true},
		{RevisionName: "chat-engine-rev-a1b2c3d4", Percent: 50},
	}
	isvc.Status.Components[omev1beta1.EngineComponent] = component

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, []reportv1alpha1.TrafficAllocation{
		{
			Component: reportv1alpha1.RuntimeComponentEngine, Role: reportv1alpha1.TrafficRoleStable,
			RevisionName: "chat-engine-rev-a1b2c3d4", RevisionHash: "a1b2c3d4", Percent: 50,
			Source: reportv1alpha1.TrafficValueSource{Evidence: reportv1alpha1.EvidenceReported, Freshness: reportv1alpha1.TrafficFreshnessUnverifiable},
		},
		{
			Component: reportv1alpha1.RuntimeComponentEngine, Role: reportv1alpha1.TrafficRoleOther,
			RevisionName: "chat-engine-rev-e5f6a7b8", RevisionHash: "e5f6a7b8", Percent: 50,
			Source: reportv1alpha1.TrafficValueSource{Evidence: reportv1alpha1.EvidenceReported, Freshness: reportv1alpha1.TrafficFreshnessUnverifiable},
		},
	}, got.Content.Allocations)
	assert.Empty(t, got.Content.Issues)
	assert.Equal(t, [][]string{
		{"WEIGHT", "engine", "stable:a1b2c3d4=50%", "Reported/Unverifiable"},
		{"WEIGHT", "engine", "other:e5f6a7b8=50%", "Reported/Unverifiable"},
	}, tableRows(got.Table().Rows, "WEIGHT"))
	var rendered bytes.Buffer
	require.NoError(t, report.Write(&rendered, report.FormatJSON, got))
	assert.Equal(t, 1, strings.Count(rendered.String(), `"role": "stable"`))
	assert.Equal(t, 1, strings.Count(rendered.String(), `"role": "other"`))
}

func TestProjectRejectsAmbiguousOrMalformedStableRevisionEvidence(t *testing.T) {
	tests := []struct {
		name       string
		rolledOut  string
		latest     [2]bool
		wantRoles  []reportv1alpha1.TrafficAllocationRole
		secretText string
	}{
		{
			name: "split without rolled out identity", latest: [2]bool{false, true},
			wantRoles: []reportv1alpha1.TrafficAllocationRole{reportv1alpha1.TrafficRoleOther, reportv1alpha1.TrafficRoleOther},
		},
		{
			name: "multiple latest markers", rolledOut: "chat-engine-rev-a1b2c3d4", latest: [2]bool{true, true},
			wantRoles: []reportv1alpha1.TrafficAllocationRole{reportv1alpha1.TrafficRoleStable, reportv1alpha1.TrafficRoleOther},
		},
		{
			name: "malformed rolled out identity", rolledOut: "SECRET_ROLLED_OUT", latest: [2]bool{false, true}, secretText: "SECRET_ROLLED_OUT",
			wantRoles: []reportv1alpha1.TrafficAllocationRole{reportv1alpha1.TrafficRoleOther, reportv1alpha1.TrafficRoleOther},
		},
		{
			name: "rolled out identity absent from targets", rolledOut: "chat-engine-rev-cccccccc", latest: [2]bool{false, true},
			wantRoles: []reportv1alpha1.TrafficAllocationRole{reportv1alpha1.TrafficRoleOther, reportv1alpha1.TrafficRoleOther},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			isvc.Spec.Rollout = nil
			isvc.Status.Canary = nil
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = omev1beta1.RolloutPhaseStable
			component.LatestRolledoutRevision = tt.rolledOut
			component.Traffic = []omev1beta1.ComponentTrafficTarget{
				{RevisionName: "chat-engine-rev-a1b2c3d4", Percent: 50, LatestRevision: tt.latest[0]},
				{RevisionName: "chat-engine-rev-e5f6a7b8", Percent: 50, LatestRevision: tt.latest[1]},
			}
			isvc.Status.Components[omev1beta1.EngineComponent] = component

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.TrafficStateInvalid, got.Content.Summary.State)
			assert.Equal(t, tt.wantRoles, trafficAllocationRoles(got.Content.Allocations))
			assert.Equal(t, []reportv1alpha1.TrafficIssue{{
				Code: reportv1alpha1.TrafficIssueAllocationInvalid, Component: reportv1alpha1.RuntimeComponentEngine,
			}}, got.Content.Issues)
			assert.LessOrEqual(t, strings.Count(flattenRows(got.Table().Rows), "stable:"), 1)
			if tt.secretText != "" {
				var rendered bytes.Buffer
				require.NoError(t, report.Write(&rendered, report.FormatJSON, got))
				assert.NotContains(t, rendered.String(), tt.secretText)
			}
		})
	}
}

func TestProjectCanaryPrimaryFollowsControllerPriority(t *testing.T) {
	tests := []struct {
		name       string
		components []omev1beta1.ComponentType
	}{
		{
			name:       "router before engine",
			components: []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.RouterComponent},
		},
		{
			name:       "router before decoder",
			components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.RouterComponent},
		},
		{
			name: "router before engine and decoder",
			components: []omev1beta1.ComponentType{
				omev1beta1.DecoderComponent,
				omev1beta1.EngineComponent,
				omev1beta1.RouterComponent,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			isvc.Spec.Rollout.Groups[0].Components = tt.components
			isvc.Status.Components[omev1beta1.RouterComponent] = primaryCanaryComponent(
				omev1beta1.RouterComponent,
			)

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			require.NotNil(t, got.Content.Canary)
			assert.Equal(t, reportv1alpha1.RuntimeComponentRouter, got.Content.Canary.Component)
		})
	}
}

func TestProjectCanaryRolesAreScopedToThePrimaryComponent(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Spec.Rollout.Groups[0].Components = []omev1beta1.ComponentType{
		omev1beta1.EngineComponent,
		omev1beta1.RouterComponent,
	}
	isvc.Status.Components[omev1beta1.RouterComponent] = primaryCanaryComponent(
		omev1beta1.RouterComponent,
	)
	engine := isvc.Status.Components[omev1beta1.EngineComponent]
	engine.Traffic = []omev1beta1.ComponentTrafficTarget{{
		RevisionName:   "chat-engine-rev-e5f6a7b8",
		Percent:        100,
		LatestRevision: true,
	}}
	isvc.Status.Components[omev1beta1.EngineComponent] = engine

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	require.NotNil(t, got.Content.Canary)
	assert.Equal(t, reportv1alpha1.RuntimeComponentRouter, got.Content.Canary.Component)
	engineAllocations := allocationsForComponent(got.Content.Allocations, reportv1alpha1.RuntimeComponentEngine)
	require.Len(t, engineAllocations, 1)
	assert.Equal(t, reportv1alpha1.TrafficRoleStable, engineAllocations[0].Role)
}

func TestProjectRejectsContradictoryCanaryEpochEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{
			name: "traffic targets a different revision epoch",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CanaryRevisionHash = "cccccccc"
			},
		},
		{
			name: "paused phase reports a non-step weight",
			mutate: func(isvc *omev1beta1.InferenceService) {
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhasePaused
				isvc.Status.Components[omev1beta1.EngineComponent] = status
				isvc.Status.Canary.ObservedTrafficWeight = 35
			},
		},
		{
			name: "canary status under a blue green phase",
			mutate: func(isvc *omev1beta1.InferenceService) {
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhaseBlueGreenStandby
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
		},
		{
			name: "stable phase retains active canary status",
			mutate: func(isvc *omev1beta1.InferenceService) {
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhaseStable
				status.Traffic = []omev1beta1.ComponentTrafficTarget{{
					RevisionName: "chat-engine-rev-e5f6a7b8", Percent: 100, LatestRevision: true,
				}}
				isvc.Status.Components[omev1beta1.EngineComponent] = status
				isvc.Status.Canary.ObservedTrafficWeight = 100
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			status := isvc.Status.Components[omev1beta1.EngineComponent]
			status.RolloutPhase = omev1beta1.RolloutPhaseCanarying
			isvc.Status.Components[omev1beta1.EngineComponent] = status
			tt.mutate(isvc)

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.TrafficStateInvalid, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueCanaryInvalid})
		})
	}
}

func TestProjectAcceptsControllerRepinPreStepHold(t *testing.T) {
	tests := []struct {
		name               string
		phase              omev1beta1.RolloutPhase
		globalPause        string
		postRepinStepClock bool
	}{
		{name: "capacity pending after step clock refresh", phase: omev1beta1.RolloutPhasePending, postRepinStepClock: true},
		{name: "immediately persisted repin boundary", phase: omev1beta1.RolloutPhaseCanarying},
		{name: "globally paused repin boundary", phase: omev1beta1.RolloutPhaseCanarying, globalPause: "true"},
		{name: "ready and paused after step clock refresh", phase: omev1beta1.RolloutPhasePaused, postRepinStepClock: true},
		{name: "capacity wait failed after step clock refresh", phase: omev1beta1.RolloutPhaseFailed, postRepinStepClock: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			newSteps := []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("100%"), Traffic: 100,
			}}
			// clampCanary repinned a [20, 100] rollout while 20% was
			// programmed to the shorter, final-only [100] ladder. The
			// active run proves that this is a repin, rather than forged
			// stand-alone CanaryStatus residue.
			configureTrafficRepin(t, isvc, isvc.Spec.Rollout.Groups[0].Canary.Steps,
				newSteps, 0, 20, tt.phase, true)
			if tt.postRepinStepClock {
				entered := metav1.NewTime(isvc.Status.Rollout.ActiveRun.PinnedAt.Add(time.Minute))
				isvc.Status.Canary.StepEnteredTime = &entered
			}
			if tt.globalPause != "" {
				// A run-boundary repin is flushed before canary reconciliation.
				// Global pause then returns without replacing the pre-repin phase,
				// so this boundary remains durable until the operator resumes.
				isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: tt.globalPause}
			}

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			require.NotNil(t, got.Content.Canary)
			assert.Equal(t, int32(0), got.Content.Canary.CurrentStep)
			assert.Equal(t, int32(1), got.Content.Canary.TotalSteps)
			assert.Equal(t, int32(20), got.Content.Canary.ObservedTraffic)
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
				Code: reportv1alpha1.TrafficIssueCanaryInvalid,
			})
		})
	}
}

func TestProjectRejectsUnprovenRepinPreStepHold(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{name: "active run missing", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun = nil
		}},
		{name: "initial pin is not a repin", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.PinnedAt = isvc.Status.Rollout.ActiveRun.OpenedAt
		}},
		{name: "step entry time missing", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.StepEnteredTime = nil
		}},
		{name: "primary target differs", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.TargetRevisions[0].Revision = "cccccccc"
		}},
		{name: "source is invalid", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Source = "SECRET_SOURCE"
		}},
		{name: "policy progression differs", mutate: func(isvc *omev1beta1.InferenceService) {
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Source = omev1beta1.RolloutPlanSourcePolicy
			pinned.PolicyGeneration = 1
			pinned.PolicyRef = &omev1beta1.RolloutPolicyRef{
				Name: "guarded-canary", Progression: omev1beta1.RolloutProgressionBlueGreen,
			}
		}},
		{name: "portable digest differs", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest = "rp1:000000000000"
		}},
		{name: "closed rollback residue has no pinned plan", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun = nil
			isvc.Status.Canary.ObservedTrafficWeight = 0
			isvc.Status.Canary.RolledBackRevisionHash = isvc.Status.Canary.CanaryRevisionHash
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = omev1beta1.RolloutPhaseRolledBack
			component.Traffic = []omev1beta1.ComponentTrafficTarget{{
				RevisionName: "chat-engine-rev-a1b2c3d4", Percent: 100,
			}}
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			configureTrafficRepin(t, isvc, isvc.Spec.Rollout.Groups[0].Canary.Steps,
				[]omev1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}},
				0, 20, omev1beta1.RolloutPhaseCanarying, true)
			tt.mutate(isvc)

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			assert.Nil(t, got.Content.Canary)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
				Code: reportv1alpha1.TrafficIssueCanaryInvalid,
			})
		})
	}
}

func TestProjectValidatesGloballyPausedNonRaisingRepinBoundary(t *testing.T) {
	tests := []struct {
		name            string
		oldSteps        []omev1beta1.RolloutGroupStep
		steps           []omev1beta1.RolloutGroupStep
		currentStep     int32
		observedTraffic int32
		phase           omev1beta1.RolloutPhase
		promotedThrough string
		wantTarget      int32
		accepted        bool
		postRepinClock  bool
	}{
		{
			name: "lowering repin",
			oldSteps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("25%"), Traffic: 20},
				{Capacity: intstr.FromString("50%"), Traffic: 50},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
			steps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("25%"), Traffic: 10},
				{Capacity: intstr.FromString("50%"), Traffic: 30},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
			currentStep:     1,
			observedTraffic: 50,
			phase:           omev1beta1.RolloutPhasePaused,
			wantTarget:      30,
			accepted:        true,
		},
		{
			name: "non-raising boundary after step clock refresh",
			oldSteps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("25%"), Traffic: 20},
				{Capacity: intstr.FromString("50%"), Traffic: 50},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
			steps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("25%"), Traffic: 10},
				{Capacity: intstr.FromString("50%"), Traffic: 30},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
			currentStep:     1,
			observedTraffic: 50,
			phase:           omev1beta1.RolloutPhasePaused,
			wantTarget:      30,
			postRepinClock:  true,
		},
		{
			name: "equal repin with promotion residue",
			oldSteps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 20},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
			steps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
			currentStep:     0,
			observedTraffic: 100,
			phase:           omev1beta1.RolloutPhasePromoting,
			promotedThrough: "SECRET_PREVIOUS_PROMOTION",
			wantTarget:      100,
			accepted:        true,
		},
		{
			name: "promoting repin has incomplete traffic",
			oldSteps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("50%"), Traffic: 50},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
			steps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("100%"), Traffic: 80},
			},
			currentStep:     0,
			observedTraffic: 80,
			phase:           omev1beta1.RolloutPhasePromoting,
			promotedThrough: "SECRET_PREVIOUS_PROMOTION",
			wantTarget:      80,
		},
		{
			name: "digest-correct plan has nonterminal final traffic",
			oldSteps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("25%"), Traffic: 20},
				{Capacity: intstr.FromString("50%"), Traffic: 50},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			},
			steps: []omev1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("25%"), Traffic: 10},
				{Capacity: intstr.FromString("50%"), Traffic: 30},
				{Capacity: intstr.FromString("100%"), Traffic: 80},
			},
			currentStep:     1,
			observedTraffic: 50,
			phase:           omev1beta1.RolloutPhasePaused,
			wantTarget:      30,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			mode := constants.OMENative
			isvc.Spec.DeploymentMode = &mode
			isvc.Spec.Engine = &omev1beta1.EngineSpec{}
			// Repin replaces a controller-valid old ladder before the
			// executor runs: [10, 30, 100] leaves 50% above the new target,
			// while [100] clamps a completed 100% step to zero at equal
			// exposure and retains the prior promotion record.
			// Neither case arms PreStepHold; global pause preserves the
			// boundary until the executor is allowed to reconcile it.
			group := omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Canary:     &omev1beta1.GroupCanary{Steps: tt.steps},
			}
			isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{group}}
			entered := metav1.NewTime(time.Date(2026, 9, 14, 16, 50, 0, 0, time.UTC))
			isvc.Status.Canary.CurrentStep = tt.currentStep
			isvc.Status.Canary.ObservedTrafficWeight = tt.observedTraffic
			isvc.Status.Canary.StepEnteredTime = &entered
			isvc.Status.Canary.PromotedThrough = tt.promotedThrough
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = tt.phase
			component.Traffic = []omev1beta1.ComponentTrafficTarget{{
				RevisionName: "chat-engine-rev-e5f6a7b8", Percent: tt.observedTraffic, LatestRevision: true,
			}}
			if tt.observedTraffic < 100 {
				component.Traffic = append(component.Traffic, omev1beta1.ComponentTrafficTarget{
					RevisionName: "chat-engine-rev-a1b2c3d4", Percent: 100 - tt.observedTraffic,
				})
			}
			isvc.Status.Components[omev1beta1.EngineComponent] = component
			opened := metav1.NewTime(time.Date(2026, 9, 14, 16, 45, 0, 0, time.UTC))
			pinned := metav1.NewTime(time.Date(2026, 9, 14, 16, 55, 0, 0, time.UTC))
			oldGroup := group
			oldGroup.Canary = &omev1beta1.GroupCanary{Steps: tt.oldSteps}
			oldDigest, digestErr := rolloutpolicy.ProgressionDigest(&oldGroup)
			require.NoError(t, digestErr)
			pinnedDigest, digestErr := rolloutpolicy.ProgressionDigest(&group)
			require.NoError(t, digestErr)
			runIdentity := fmt.Sprintf(
				"%s=%s;%s%s", omev1beta1.EngineComponent, "e5f6a7b8",
				oldDigest, opened.Time.UTC().Format(time.RFC3339),
			)
			isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
				RunID: "chat-" + rolloutpolicy.ShortHash([]byte(runIdentity)), OpenedAt: opened, PinnedAt: pinned,
				TargetRevisions: []omev1beta1.RolloutRunTarget{{
					Component: omev1beta1.EngineComponent, Revision: "e5f6a7b8",
				}},
				Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
					Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: pinnedDigest, Group: group,
				}}},
			}}
			if tt.postRepinClock {
				entered := metav1.NewTime(pinned.Add(time.Minute))
				isvc.Status.Canary.StepEnteredTime = &entered
			}
			isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			if !tt.accepted {
				assert.Nil(t, got.Content.Canary)
				assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
					Code: reportv1alpha1.TrafficIssueCanaryInvalid,
				})
				return
			}
			require.NotNil(t, got.Content.Canary)
			assert.Equal(t, tt.currentStep, got.Content.Canary.CurrentStep)
			assert.Equal(t, int32(len(tt.steps)), got.Content.Canary.TotalSteps)
			assert.Equal(t, tt.observedTraffic, got.Content.Canary.ObservedTraffic)
			assert.Equal(t, tt.wantTarget, tt.steps[tt.currentStep].Traffic)
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
				Code: reportv1alpha1.TrafficIssueCanaryInvalid,
			})
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "SECRET_PREVIOUS_PROMOTION")
		})
	}
}

func TestProjectRejectsImpossiblePolicyRepinProvenance(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{name: "policy progression mismatch", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef.Progression = omev1beta1.RolloutProgressionBlueGreen
		}},
		{name: "policy kind invalid", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef.Kind = "ClusterRolloutPolicy"
		}},
		{name: "policy name invalid", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef.Name = "bad/name"
		}},
		{name: "policy progression invalid", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef.Progression = "SECRET_PROGRESSION"
		}},
		{name: "derived provenance carries progression", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyGeneration = 0
		}},
		{name: "derived provenance carries kind", mutate: func(isvc *omev1beta1.InferenceService) {
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.PolicyGeneration = 0
			pinned.PolicyRef.Progression = ""
			pinned.PolicyRef.Kind = "RolloutPolicy"
		}},
		{name: "local provenance omits progression", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].PolicyRef.Progression = ""
		}},
		{name: "policy capacity is absolute", mutate: func(isvc *omev1beta1.InferenceService) {
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Group.Canary.Steps[1].Capacity = intstr.FromInt(2)
			refreshTrafficPinnedDigest(t, pinned)
		}},
		{name: "policy server address is not portable", mutate: func(isvc *omev1beta1.InferenceService) {
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Group.Canary.Prometheus = &omev1beta1.AnalysisPrometheus{
				ServerAddress: "https://prometheus.internal.example",
			}
			refreshTrafficPinnedDigest(t, pinned)
		}},
		{name: "policy auth reference is not portable", mutate: func(isvc *omev1beta1.InferenceService) {
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Group.Canary.Prometheus = &omev1beta1.AnalysisPrometheus{
				AuthRef: &corev1.SecretKeySelector{Key: "token"},
			}
			refreshTrafficPinnedDigest(t, pinned)
		}},
		{name: "resolved group order is unsupported", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Order = []omev1beta1.ComponentType{
				omev1beta1.EngineComponent,
			}
		}},
		{name: "canary cannot coexist with another group", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
			group := omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
				BlueGreen:  &omev1beta1.GroupBlueGreen{},
			}
			digest, err := rolloutpolicy.ProgressionDigest(&group)
			require.NoError(t, err)
			isvc.Status.Rollout.ActiveRun.Plan.Groups = append(
				isvc.Status.Rollout.ActiveRun.Plan.Groups,
				omev1beta1.RolloutRunGroup{
					Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: group,
				},
			)
			isvc.Status.Rollout.ActiveRun.TargetRevisions = append(
				isvc.Status.Rollout.ActiveRun.TargetRevisions,
				omev1beta1.RolloutRunTarget{Component: omev1beta1.DecoderComponent, Revision: "cccccccc"},
			)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			configureTrafficRepin(t, isvc,
				[]omev1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("25%"), Traffic: 20},
					{Capacity: intstr.FromString("50%"), Traffic: 50},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				},
				[]omev1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("25%"), Traffic: 10},
					{Capacity: intstr.FromString("50%"), Traffic: 30},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				},
				1, 50, omev1beta1.RolloutPhasePaused, false)
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Source = omev1beta1.RolloutPlanSourcePolicy
			pinned.PolicyGeneration = 7
			pinned.PolicyRef = &omev1beta1.RolloutPolicyRef{
				Name: "guarded-canary", Progression: omev1beta1.RolloutProgressionCanary,
			}
			tt.mutate(isvc)

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			assert.Nil(t, got.Content.Canary)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
				Code: reportv1alpha1.TrafficIssueCanaryInvalid,
			})
		})
	}
}

func TestProjectAcceptsPolicyRepinWithReorderedTargets(t *testing.T) {
	isvc := currentTrafficISVC(t)
	configureTrafficRepin(t, isvc,
		[]omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 20},
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		},
		[]omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 10},
			{Capacity: intstr.FromString("50%"), Traffic: 30},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		},
		1, 50, omev1beta1.RolloutPhasePaused, false)
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
	pinned.Source = omev1beta1.RolloutPlanSourcePolicy
	pinned.PolicyGeneration = 7
	pinned.PolicyRef = &omev1beta1.RolloutPolicyRef{
		Name: "guarded-canary", Progression: omev1beta1.RolloutProgressionCanary,
	}
	pinned.Group.Components = []omev1beta1.ComponentType{
		omev1beta1.DecoderComponent, omev1beta1.EngineComponent,
	}
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []omev1beta1.RolloutRunTarget{
		{Component: omev1beta1.EngineComponent, Revision: "e5f6a7b8"},
		{Component: omev1beta1.DecoderComponent, Revision: "cccccccc"},
	}

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	require.NotNil(t, got.Content.Canary)
	assert.Equal(t, int32(1), got.Content.Canary.CurrentStep)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
		Code: reportv1alpha1.TrafficIssueCanaryInvalid,
	})

	// A derived service pins the same policy body from name-only provenance;
	// no local policy object means there is no generation, kind, or progression.
	pinned.PolicyGeneration = 0
	pinned.PolicyRef.Kind = ""
	pinned.PolicyRef.Progression = ""
	got, err = trafficprojection.Project(isvc, projectionClock)
	require.NoError(t, err)
	require.NotNil(t, got.Content.Canary)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
		Code: reportv1alpha1.TrafficIssueCanaryInvalid,
	})
}

func TestProjectAcceptsRollbackRepinPreStepHold(t *testing.T) {
	for _, phase := range []omev1beta1.RolloutPhase{
		omev1beta1.RolloutPhaseRollingBack,
		omev1beta1.RolloutPhaseRolledBack,
	} {
		t.Run(string(phase), func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			// The run layer handles repin before closed-outcome detection. A
			// one-step replacement therefore clamps the rolled-back status and
			// persists its hold before the canary reconciler runs again.
			newSteps := []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("100%"), Traffic: 100,
			}}
			configureTrafficRepin(t, isvc, isvc.Spec.Rollout.Groups[0].Canary.Steps,
				newSteps, 0, 0, phase, true)
			isvc.Status.Canary.RolledBackRevisionHash = isvc.Status.Canary.CanaryRevisionHash
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = phase
			component.Traffic = []omev1beta1.ComponentTrafficTarget{{
				RevisionName: "chat-engine-rev-a1b2c3d4", Percent: 100,
			}}
			isvc.Status.Components[omev1beta1.EngineComponent] = component

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			require.NotNil(t, got.Content.Canary)
			assert.Equal(t, int32(0), got.Content.Canary.CurrentStep)
			assert.Equal(t, int32(0), got.Content.Canary.ObservedTraffic)
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
				Code: reportv1alpha1.TrafficIssueCanaryInvalid,
			})
		})
	}
}

func TestProjectAcceptsPromotedThroughAfterBackwardRepinClamp(t *testing.T) {
	isvc := currentTrafficISVC(t)
	newSteps := []omev1beta1.RolloutGroupStep{{
		Capacity: intstr.FromString("100%"), Traffic: 100,
	}}
	configureTrafficRepin(t, isvc, isvc.Spec.Rollout.Groups[0].Canary.Steps,
		newSteps, 0, 20, omev1beta1.RolloutPhaseCanarying, true)
	isvc.Status.Canary.PromotedThrough = "SECRET_OLD_PROMOTE"
	// A global pause lets the phase and durable promotion record from the
	// repin boundary remain visible until the executor resumes.
	isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	require.NotNil(t, got.Content.Canary)
	assert.Equal(t, int32(0), got.Content.Canary.CurrentStep)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
		Code: reportv1alpha1.TrafficIssueCanaryInvalid,
	})
	var rendered bytes.Buffer
	require.NoError(t, report.Write(&rendered, report.FormatJSON, got))
	assert.NotContains(t, rendered.String(), "SECRET_OLD_PROMOTE")
}

func TestProjectRejectsMalformedPreStepHold(t *testing.T) {
	tests := []struct {
		name   string
		phase  omev1beta1.RolloutPhase
		mutate func(*omev1beta1.InferenceService)
	}{
		{name: "impossible phase", mutate: func(isvc *omev1beta1.InferenceService) {
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = omev1beta1.RolloutPhasePromoting
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
		{name: "target does not raise traffic", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Traffic = 20
		}},
		{name: "missing typed traffic", mutate: func(isvc *omev1beta1.InferenceService) {
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.Traffic = nil
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
		{name: "incoherent typed traffic", mutate: func(isvc *omev1beta1.InferenceService) {
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.Traffic[0].Percent = 30
			component.Traffic[1].Percent = 70
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
		{name: "negative observed traffic", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.ObservedTrafficWeight = -1
		}},
		{name: "out of range observed traffic", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.ObservedTrafficWeight = 101
		}},
		{name: "contradictory current step", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.CurrentStep = 1
		}},
		{name: "canarying boundary target does not raise traffic", phase: omev1beta1.RolloutPhaseCanarying, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Traffic = 20
		}},
		{name: "canarying boundary missing typed traffic", phase: omev1beta1.RolloutPhaseCanarying, mutate: func(isvc *omev1beta1.InferenceService) {
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.Traffic = nil
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
		{name: "canarying boundary typed traffic disagrees", phase: omev1beta1.RolloutPhaseCanarying, mutate: func(isvc *omev1beta1.InferenceService) {
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.Traffic[0].Percent = 30
			component.Traffic[1].Percent = 70
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
		{name: "canarying boundary observed traffic is unsafe", phase: omev1beta1.RolloutPhaseCanarying, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.ObservedTrafficWeight = 101
		}},
		{name: "canarying boundary step is completed", phase: omev1beta1.RolloutPhaseCanarying, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.CurrentStep = 1
			isvc.Status.Canary.ObservedTrafficWeight = 100
			isvc.Status.Canary.StableRevisionHash = ""
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			isvc.Spec.Rollout.Groups[0].Canary.Steps = []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("100%"), Traffic: 100,
			}}
			isvc.Status.Canary.PreStepHold = true
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = tt.phase
			if component.RolloutPhase == "" {
				component.RolloutPhase = omev1beta1.RolloutPhasePending
			}
			isvc.Status.Components[omev1beta1.EngineComponent] = component
			tt.mutate(isvc)

			got, err := trafficprojection.Project(isvc, projectionClock)

			require.NoError(t, err)
			assert.Nil(t, got.Content.Canary)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
				Code: reportv1alpha1.TrafficIssueCanaryInvalid,
			})
		})
	}
}

func TestProjectDoesNotPromoteAnUnmatchedPrimaryCanaryTargetToStable(t *testing.T) {
	isvc := currentTrafficISVC(t)
	status := isvc.Status.Components[omev1beta1.EngineComponent]
	status.RolloutPhase = omev1beta1.RolloutPhasePending
	isvc.Status.Components[omev1beta1.EngineComponent] = status
	isvc.Status.Canary.CanaryRevisionHash = "cccccccc"
	isvc.Status.Canary.ObservedTrafficWeight = 0

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficStatePending, got.Content.Summary.State)
	require.Len(t, got.Content.Allocations, 2)
	assert.Equal(t, reportv1alpha1.TrafficRoleStable, got.Content.Allocations[0].Role)
	assert.Equal(t, "a1b2c3d4", got.Content.Allocations[0].RevisionHash)
	assert.Equal(t, reportv1alpha1.TrafficRoleOther, got.Content.Allocations[1].Role)
	assert.Equal(t, "e5f6a7b8", got.Content.Allocations[1].RevisionHash)
}

func TestProjectRejectsEqualStableAndCanaryRevisionHashes(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Canary.CanaryRevisionHash = isvc.Status.Canary.StableRevisionHash

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficStateInvalid, got.Content.Summary.State)
	assert.Nil(t, got.Content.Canary)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueCanaryInvalid})
}

func TestProjectAcceptsCompletedCanarySentinel(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Canary.CurrentStep = int32(len(isvc.Spec.Rollout.Groups[0].Canary.Steps))
	isvc.Status.Canary.ObservedTrafficWeight = 100
	isvc.Status.Canary.StableRevisionHash = ""
	component := isvc.Status.Components[omev1beta1.EngineComponent]
	component.RolloutPhase = omev1beta1.RolloutPhaseStable
	component.Traffic = []omev1beta1.ComponentTrafficTarget{{
		RevisionName: "chat-engine-rev-e5f6a7b8", Percent: 100, LatestRevision: true,
	}}
	isvc.Status.Components[omev1beta1.EngineComponent] = component

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	require.NotNil(t, got.Content.Canary)
	assert.Equal(t, int32(2), got.Content.Canary.CurrentStep)
	assert.Equal(t, int32(2), got.Content.Canary.TotalSteps)
	assert.Equal(t, int32(100), got.Content.Canary.ObservedTraffic)
	require.Len(t, got.Content.Allocations, 1)
	assert.Equal(t, reportv1alpha1.TrafficRoleStable, got.Content.Allocations[0].Role)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: reportv1alpha1.TrafficIssueCanaryInvalid})
	assert.Contains(t, flattenRows(got.Table().Rows), "CANARY|engine|2/2 @ 100%|Reported/Unverifiable")
}

func TestProjectAcceptsControllerBoundedRevisionServiceNames(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Name = strings.Repeat("long-", 10) + "chat"
	component := isvc.Status.Components[omev1beta1.EngineComponent]
	for i := range component.Traffic {
		hash := component.Traffic[i].RevisionName[len(component.Traffic[i].RevisionName)-8:]
		raw := fmt.Sprintf("%s-%s-rev-%s", isvc.Name, omev1beta1.EngineComponent, hash)
		component.Traffic[i].RevisionName = constants.TruncateNameWithMaxLength(raw, utilvalidation.DNS1035LabelMaxLength)
		require.Len(t, component.Traffic[i].RevisionName, utilvalidation.DNS1035LabelMaxLength)
	}
	isvc.Status.Components[omev1beta1.EngineComponent] = component

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Len(t, got.Content.Allocations, 2)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
		Code: reportv1alpha1.TrafficIssueAllocationInvalid, Component: reportv1alpha1.RuntimeComponentEngine,
	})
	for _, allocation := range got.Content.Allocations {
		assert.Len(t, allocation.RevisionName, utilvalidation.DNS1035LabelMaxLength)
	}
}

func TestProjectNeverReportsUnsupportedNoneForMalformedEvidence(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Traffic.Conditions = append(isvc.Status.Traffic.Conditions,
		trafficCondition(omev1beta1.TrafficConditionBackendPolicyUnsupportedFields, metav1.ConditionFalse, omev1beta1.TrafficReasonUnsupportedField, 7),
	)

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficStateInvalid, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.TrafficUnsupportedUnknown, got.Content.Summary.Unsupported)
}

func TestProjectRejectsMalformedStatusWithoutEchoingHostileValues(t *testing.T) {
	secretValues := []string{
		"SECRET_ALGORITHM", "secret.policy.invalid", "SECRET_KIND", "Bad_Route_SECRET",
		"user:password", "SECRET_TAG", "SECRET_MESSAGE", "SECRET_ANNOTATION",
		"bad-revision-SECRET",
	}
	isvc := currentTrafficISVC(t)
	isvc.Annotations = map[string]string{"secret": "SECRET_ANNOTATION"}
	isvc.Status.Traffic.Algorithm = secretValues[0]
	isvc.Status.Traffic.BackendPolicyResource = &omev1beta1.BackendPolicyRef{APIVersion: secretValues[1], Kind: secretValues[2], Name: "SECRET_POLICY_NAME"}
	isvc.Status.Traffic.TargetedHTTPRoutes = append(isvc.Status.Traffic.TargetedHTTPRoutes, secretValues[3])
	isvc.Status.Traffic.Conditions[0].Message = secretValues[6]
	isvc.Status.Traffic.Conditions = append(isvc.Status.Traffic.Conditions, isvc.Status.Traffic.Conditions[0])
	isvc.Status.Addresses = append(isvc.Status.Addresses, duckv1.Addressable{URL: mustURL(t, "https://user:password@secret.invalid/path?token=SECRET#fragment")})
	component := isvc.Status.Components[omev1beta1.EngineComponent]
	component.Traffic = append(component.Traffic, omev1beta1.ComponentTrafficTarget{RevisionName: secretValues[8], Percent: 1, Tag: secretValues[5]})
	isvc.Status.Components[omev1beta1.EngineComponent] = component

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.TrafficStateInvalid, got.Content.Summary.State)
	require.NotEmpty(t, got.Content.Issues)
	var rendered bytes.Buffer
	require.NoError(t, report.Write(&rendered, report.FormatJSON, got))
	for _, secret := range secretValues {
		assert.NotContains(t, rendered.String(), secret)
	}
	assert.NotContains(t, rendered.String(), "SECRET_POLICY_NAME")
}

func TestProjectBoundsAndSortsAllRepeatedEvidence(t *testing.T) {
	isvc := currentTrafficISVC(t)
	isvc.Status.Traffic.TargetedHTTPRoutes = nil
	for i := 5; i >= 0; i-- {
		isvc.Status.Traffic.TargetedHTTPRoutes = append(isvc.Status.Traffic.TargetedHTTPRoutes, fmt.Sprintf("chat-route-%d", i))
	}
	isvc.Status.Addresses = nil
	for i := 17; i >= 0; i-- {
		isvc.Status.Addresses = append(isvc.Status.Addresses, duckv1.Addressable{URL: mustURL(t, fmt.Sprintf("https://chat-%02d.prod.example/", i))})
	}
	for _, componentType := range []omev1beta1.ComponentType{
		omev1beta1.EngineComponent, omev1beta1.DecoderComponent, omev1beta1.RouterComponent,
	} {
		component := isvc.Status.Components[componentType]
		component.Traffic = nil
		for i := 9; i >= 0; i-- {
			component.Traffic = append(component.Traffic, omev1beta1.ComponentTrafficTarget{
				RevisionName: fmt.Sprintf("chat-%s-rev-%08x", componentType, i), Percent: 10,
			})
		}
		isvc.Status.Components[componentType] = component
	}
	for i := 0; i < 100; i++ {
		isvc.Status.Traffic.Conditions = append(isvc.Status.Traffic.Conditions, trafficCondition(fmt.Sprintf("Unknown-%03d", i), metav1.ConditionTrue, "SECRET_REASON", 7))
	}

	got, err := trafficprojection.Project(isvc, projectionClock)

	require.NoError(t, err)
	assert.Len(t, got.Content.Routes, 4)
	assert.Equal(t, []string{"chat-route-0", "chat-route-1", "chat-route-2", "chat-route-3"}, trafficRouteNames(got.Content.Routes))
	assert.Len(t, got.Content.Endpoints, 16)
	assert.Equal(t, "https://chat-00.prod.example/", got.Content.Endpoints[0].URL)
	assert.Equal(t, "https://chat-15.prod.example/", got.Content.Endpoints[15].URL)
	assert.Len(t, got.Content.Allocations, 24)
	for _, component := range []reportv1alpha1.RuntimeComponentType{
		reportv1alpha1.RuntimeComponentEngine,
		reportv1alpha1.RuntimeComponentDecoder,
		reportv1alpha1.RuntimeComponentRouter,
	} {
		assert.Len(t, allocationsForComponent(got.Content.Allocations, component), 8)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{
			Code: reportv1alpha1.TrafficIssueAllocationsTruncated, Component: component,
		})
	}
	assert.Len(t, got.Content.Conditions, 1, "unrecognized condition types must not expand output")
	for _, code := range []reportv1alpha1.TrafficIssueCode{reportv1alpha1.TrafficIssueRoutesTruncated, reportv1alpha1.TrafficIssueEndpointsTruncated} {
		assert.Contains(t, got.Content.Issues, reportv1alpha1.TrafficIssue{Code: code, Component: issueComponent(code)})
	}
	assert.Contains(t, got.Warnings, reportv1alpha1.TrafficWarning{Code: reportv1alpha1.WarningTruncated})
}

func TestProjectIsDeterministicAcrossSourceOrder(t *testing.T) {
	left := currentTrafficISVC(t)
	right := left.DeepCopy()
	slices.Reverse(right.Status.Traffic.TargetedHTTPRoutes)
	slices.Reverse(right.Status.Addresses)
	rightComponent := right.Status.Components[omev1beta1.EngineComponent]
	slices.Reverse(rightComponent.Traffic)
	right.Status.Components[omev1beta1.EngineComponent] = rightComponent
	slices.Reverse(right.Status.Traffic.Conditions)

	leftReport, err := trafficprojection.Project(left, projectionClock)
	require.NoError(t, err)
	rightReport, err := trafficprojection.Project(right, projectionClock)
	require.NoError(t, err)

	assert.Equal(t, leftReport, rightReport)
}

func TestProjectConditionConflictIsDeterministicAcrossSourceOrder(t *testing.T) {
	left := currentTrafficISVC(t)
	left.Status.Traffic.Conditions = append(left.Status.Traffic.Conditions,
		trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady, metav1.ConditionFalse, omev1beta1.TrafficReasonGatewayRejected, 7),
	)
	right := left.DeepCopy()
	slices.Reverse(right.Status.Traffic.Conditions)

	leftReport, err := trafficprojection.Project(left, projectionClock)
	require.NoError(t, err)
	rightReport, err := trafficprojection.Project(right, projectionClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.TrafficStateInvalid, leftReport.Content.Summary.State)
	assert.Equal(t, leftReport, rightReport)
}

func TestProjectAllocationConflictIsDeterministicAcrossSourceOrder(t *testing.T) {
	left := currentTrafficISVC(t)
	left.Status.Canary = nil
	component := left.Status.Components[omev1beta1.EngineComponent]
	component.Traffic = []omev1beta1.ComponentTrafficTarget{
		{RevisionName: "chat-engine-rev-a1b2c3d4", Percent: 50},
		{RevisionName: "chat-engine-rev-a1b2c3d4", Percent: 50, LatestRevision: true},
	}
	left.Status.Components[omev1beta1.EngineComponent] = component
	right := left.DeepCopy()
	rightComponent := right.Status.Components[omev1beta1.EngineComponent]
	slices.Reverse(rightComponent.Traffic)
	right.Status.Components[omev1beta1.EngineComponent] = rightComponent

	leftReport, err := trafficprojection.Project(left, projectionClock)
	require.NoError(t, err)
	rightReport, err := trafficprojection.Project(right, projectionClock)
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.TrafficStateInvalid, leftReport.Content.Summary.State)
	assert.Equal(t, leftReport, rightReport)
}

func TestProjectRecognizesOnlyExactPolicyGVKs(t *testing.T) {
	tests := []struct {
		apiVersion string
		kind       string
		want       reportv1alpha1.TrafficTranslator
		invalid    bool
	}{
		{apiVersion: "gateway.envoyproxy.io/v1alpha1", kind: "BackendTrafficPolicy", want: reportv1alpha1.TrafficTranslatorEnvoyGateway},
		{apiVersion: "networking.istio.io/v1", kind: "DestinationRule", want: reportv1alpha1.TrafficTranslatorIstio},
		{apiVersion: "gateway.envoyproxy.io/v1beta1", kind: "BackendTrafficPolicy", want: reportv1alpha1.TrafficTranslatorUnavailable, invalid: true},
		{apiVersion: "networking.istio.io/v1", kind: "BackendTrafficPolicy", want: reportv1alpha1.TrafficTranslatorUnavailable, invalid: true},
	}
	for _, tt := range tests {
		t.Run(tt.apiVersion+"/"+tt.kind, func(t *testing.T) {
			isvc := currentTrafficISVC(t)
			isvc.Status.Traffic.BackendPolicyResource.APIVersion = tt.apiVersion
			isvc.Status.Traffic.BackendPolicyResource.Kind = tt.kind
			got, err := trafficprojection.Project(isvc, projectionClock)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Content.Summary.Translator)
			assert.Equal(t, tt.invalid, got.Content.Summary.State == reportv1alpha1.TrafficStateInvalid)
		})
	}
}

func TestProjectValidatesSubjectIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
		want   error
	}{
		{name: "nil", want: trafficprojection.ErrInferenceServiceRequired},
		{name: "name", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Name = "" }, want: trafficprojection.ErrInferenceServiceNameRequired},
		{name: "namespace", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Namespace = "" }, want: trafficprojection.ErrInferenceServiceNamespaceRequired},
		{name: "uid", mutate: func(isvc *omev1beta1.InferenceService) { isvc.UID = "" }, want: trafficprojection.ErrInferenceServiceUIDRequired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var isvc *omev1beta1.InferenceService
			if tt.mutate != nil {
				isvc = currentTrafficISVC(t)
				tt.mutate(isvc)
			}
			_, err := trafficprojection.Project(isvc, projectionClock)
			assert.ErrorIs(t, err, tt.want)
		})
	}
}

func currentTrafficISVC(t *testing.T) *omev1beta1.InferenceService {
	t.Helper()
	class := "SECRET_CLASS_MUST_NOT_LEAK"
	return &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 7},
		Spec: omev1beta1.InferenceServiceSpec{
			Traffic: &omev1beta1.TrafficSpec{},
			Rollout: &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("50%"), Traffic: 20},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			}}},
		},
		Status: omev1beta1.InferenceServiceStatus{
			Addresses: []duckv1.Addressable{
				{Name: &class, URL: mustURL(t, "https://chat.prod.example/")},
				{URL: mustURL(t, "http://chat-engine.prod.svc.cluster.local")},
			},
			Traffic: &omev1beta1.TrafficStatus{
				Algorithm:             "RoundRobin",
				BackendPolicyResource: &omev1beta1.BackendPolicyRef{APIVersion: "gateway.envoyproxy.io/v1alpha1", Kind: "BackendTrafficPolicy", Name: "chat"},
				TargetedHTTPRoutes:    []string{"chat-router", "chat"},
				Conditions:            []metav1.Condition{trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady, metav1.ConditionUnknown, omev1beta1.TrafficReasonPending, 7)},
			},
			Canary: &omev1beta1.CanaryStatus{CurrentStep: 0, ObservedTrafficWeight: 20, StableRevisionHash: "a1b2c3d4", CanaryRevisionHash: "e5f6a7b8"},
			Components: map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: {RolloutPhase: omev1beta1.RolloutPhaseCanarying, Traffic: []omev1beta1.ComponentTrafficTarget{
					{RevisionName: "chat-engine-rev-e5f6a7b8", Percent: 20, Tag: "SECRET_TAG", LatestRevision: true},
					{RevisionName: "chat-engine-rev-a1b2c3d4", Percent: 80},
				}},
			},
		},
	}
}

func configureTrafficRepin(
	t *testing.T,
	isvc *omev1beta1.InferenceService,
	oldSteps, newSteps []omev1beta1.RolloutGroupStep,
	currentStep, observedTraffic int32,
	phase omev1beta1.RolloutPhase,
	preStepHold bool,
) {
	t.Helper()
	mode := constants.OMENative
	isvc.Spec.DeploymentMode = &mode
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	group := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{
			Steps: append([]omev1beta1.RolloutGroupStep{}, newSteps...),
		},
	}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{group}}
	opened := metav1.NewTime(time.Date(2026, time.September, 14, 16, 40, 0, 0, time.UTC))
	entered := metav1.NewTime(time.Date(2026, time.September, 14, 16, 45, 0, 0, time.UTC))
	pinnedAt := metav1.NewTime(time.Date(2026, time.September, 14, 16, 50, 0, 0, time.UTC))
	isvc.Status.Canary.CurrentStep = currentStep
	isvc.Status.Canary.ObservedTrafficWeight = observedTraffic
	isvc.Status.Canary.PreStepHold = preStepHold
	isvc.Status.Canary.StepEnteredTime = &entered
	component := isvc.Status.Components[omev1beta1.EngineComponent]
	component.RolloutPhase = phase
	component.Traffic = []omev1beta1.ComponentTrafficTarget{{
		RevisionName: "chat-engine-rev-e5f6a7b8", Percent: observedTraffic, LatestRevision: true,
	}}
	if observedTraffic == 0 {
		component.Traffic = nil
	}
	if observedTraffic < 100 {
		component.Traffic = append(component.Traffic, omev1beta1.ComponentTrafficTarget{
			RevisionName: "chat-engine-rev-a1b2c3d4", Percent: 100 - observedTraffic,
		})
	}
	isvc.Status.Components[omev1beta1.EngineComponent] = component
	oldGroup := group
	oldGroup.Canary = &omev1beta1.GroupCanary{
		Steps: append([]omev1beta1.RolloutGroupStep{}, oldSteps...),
	}
	oldDigest, err := rolloutpolicy.ProgressionDigest(&oldGroup)
	require.NoError(t, err)
	pinnedDigest, err := rolloutpolicy.ProgressionDigest(&group)
	require.NoError(t, err)
	runIdentity := fmt.Sprintf("%s=%s;%s%s", omev1beta1.EngineComponent,
		"e5f6a7b8", oldDigest, opened.Time.UTC().Format(time.RFC3339))
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		RunID: "chat-" + rolloutpolicy.ShortHash([]byte(runIdentity)), OpenedAt: opened, PinnedAt: pinnedAt,
		TargetRevisions: []omev1beta1.RolloutRunTarget{{
			Component: omev1beta1.EngineComponent, Revision: "e5f6a7b8",
		}},
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: pinnedDigest, Group: group,
		}}},
	}}
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	isvc.Annotations[constants.PausedRolloutAnnotation] = "true"
}

func refreshTrafficPinnedDigest(t *testing.T, pinned *omev1beta1.RolloutRunGroup) {
	t.Helper()
	digest, err := rolloutpolicy.ProgressionDigest(&pinned.Group)
	require.NoError(t, err)
	pinned.PortableDigest = digest
}

func primaryCanaryComponent(component omev1beta1.ComponentType) omev1beta1.ComponentStatusSpec {
	return omev1beta1.ComponentStatusSpec{
		RolloutPhase: omev1beta1.RolloutPhaseCanarying,
		Traffic: []omev1beta1.ComponentTrafficTarget{
			{RevisionName: "chat-" + string(component) + "-rev-e5f6a7b8", Percent: 20, LatestRevision: true},
			{RevisionName: "chat-" + string(component) + "-rev-a1b2c3d4", Percent: 80},
		},
	}
}

func trafficCondition(conditionType string, status metav1.ConditionStatus, reason string, generation int64) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: "SECRET_MESSAGE", ObservedGeneration: generation, LastTransitionTime: metav1.NewTime(time.Date(2026, 9, 14, 16, 59, 0, 0, time.UTC))}
}

func setReadyCondition(isvc *omev1beta1.InferenceService, status metav1.ConditionStatus, reason string, generation int64) {
	isvc.Status.Traffic.Conditions[0] = trafficCondition(omev1beta1.TrafficConditionBackendPolicyReady, status, reason, generation)
}

func mustURL(t *testing.T, value string) *knapis.URL {
	t.Helper()
	result, err := knapis.ParseURL(value)
	require.NoError(t, err)
	return result
}

func trafficRouteNames(values []reportv1alpha1.TrafficRoute) []string {
	result := make([]string, len(values))
	for i := range values {
		result[i] = values[i].Name
	}
	return result
}

func trafficEndpointURLs(values []reportv1alpha1.TrafficEndpoint) []string {
	result := make([]string, len(values))
	for i := range values {
		result[i] = values[i].URL
	}
	return result
}

func trafficAllocationRoles(values []reportv1alpha1.TrafficAllocation) []reportv1alpha1.TrafficAllocationRole {
	result := make([]reportv1alpha1.TrafficAllocationRole, len(values))
	for i := range values {
		result[i] = values[i].Role
	}
	return result
}

func allocationsForComponent(values []reportv1alpha1.TrafficAllocation, component reportv1alpha1.RuntimeComponentType) []reportv1alpha1.TrafficAllocation {
	result := make([]reportv1alpha1.TrafficAllocation, 0, len(values))
	for _, value := range values {
		if value.Component == component {
			result = append(result, value)
		}
	}
	return result
}

func flattenRows(rows [][]string) string {
	values := make([]string, len(rows))
	for i := range rows {
		values[i] = strings.Join(rows[i], "|")
	}
	return strings.Join(values, "\n")
}

func tableRows(rows [][]string, field string) [][]string {
	result := make([][]string, 0)
	for _, row := range rows {
		if len(row) > 0 && row[0] == field {
			result = append(result, row)
		}
	}
	return result
}

func issueComponent(code reportv1alpha1.TrafficIssueCode) reportv1alpha1.RuntimeComponentType {
	if code == reportv1alpha1.TrafficIssueAllocationsTruncated {
		return reportv1alpha1.RuntimeComponentEngine
	}
	return ""
}
