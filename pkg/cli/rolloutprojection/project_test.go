package rolloutprojection_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func TestProjectCanaryProducesSafeFaithfulReport(t *testing.T) {
	entered := metav1.NewTime(time.Date(2026, time.August, 31, 17, 0, 0, 0, time.UTC))
	evaluated := metav1.NewTime(time.Date(2026, time.August, 31, 17, 1, 0, 0, time.UTC))
	analysis := validAnalysis("SECRET_METRIC")
	analysis.Metrics[0].Query = "SECRET_QUERY"
	isvc := baseInferenceService()
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 5},
			{
				Capacity: intstr.FromString("50%"), Traffic: 20,
				Pause:    &omev1beta1.RolloutPause{},
				Analysis: analysis,
			},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	isvc.Status.ObservedGeneration = 7
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			RolloutPhase:              omev1beta1.RolloutPhaseCanarying,
			LatestRolledoutRevision:   "chat-engine-rev-aaaaaaaa",
			LatestReadyRevision:       "chat-engine-rev-bbbbbbbb",
			PreviousRolledoutRevision: "chat-engine-rev-cccccccc",
			Traffic: []omev1beta1.ComponentTrafficTarget{
				{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 80, Tag: "SECRET_TAG", LatestRevision: true},
				{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 20},
			},
		},
	}
	isvc.Status.Canary = &omev1beta1.CanaryStatus{
		CanaryRevisionHash: "bbbbbbbb", StableRevisionHash: "aaaaaaaa",
		CurrentStep: 1, StepEnteredTime: &entered, ObservedTrafficWeight: 20,
		PromotedThrough: "SECRET_MAILBOX", AnalysisFailedChecks: 1,
		LastEvaluationTime: &evaluated, LastConclusiveEvaluationTime: &evaluated,
		MetricResults: []omev1beta1.AnalysisMetricResult{{
			Name: "SECRET_METRIC", Value: "2", Threshold: "1",
			Operator: omev1beta1.ComparisonLTE, Passed: false, Message: "SECRET_MESSAGE",
			Time: &evaluated,
		}},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.RolloutSummary{
		State:             reportv1alpha1.RolloutStateUnknown,
		ReportedState:     reportv1alpha1.RolloutStateInProgress,
		Evidence:          reportv1alpha1.EvidenceReported,
		Epoch:             reportv1alpha1.RolloutEpochUnverifiable,
		CoordinationReady: reportv1alpha1.RolloutConditionNotApplicable,
	}, got.Content.Summary)
	require.Len(t, got.Sources, 1)
	assert.Equal(t, reportv1alpha1.RolloutSourceReference{
		Kind: "InferenceService", Namespace: "prod", Name: "chat", UID: "isvc-uid",
		Generation: 7, Evidence: reportv1alpha1.EvidenceObserved,
		CollectedAt: fixedClock().Now(),
	}, got.Sources[0])
	require.Len(t, got.Content.Groups, 1)
	assert.Equal(t, reportv1alpha1.RolloutStrategyCanary, got.Content.Groups[0].Strategy)
	assert.Equal(t, reportv1alpha1.RolloutPhaseCanarying, got.Content.Groups[0].Phase)
	assert.Equal(t, "aaaaaaaa", got.Content.Groups[0].StableRevisionHash)
	assert.Equal(t, "bbbbbbbb", got.Content.Groups[0].TargetRevisionHash)
	require.NotNil(t, got.Content.Groups[0].Step)
	assert.Equal(t, reportv1alpha1.RolloutStepStatus{
		Index: 1, Total: 3, Capacity: "50%", TargetTraffic: 20, ObservedTraffic: 20,
		Gate: reportv1alpha1.RolloutGateAnalysis, Analysis: reportv1alpha1.RolloutAnalysisFailing,
		EnteredAt: ptrTime(entered.Time),
	}, *got.Content.Groups[0].Step)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, []reportv1alpha1.RolloutTrafficTarget{
		{RevisionHash: "aaaaaaaa", Percent: 80, Role: reportv1alpha1.RolloutRevisionCurrent},
		{RevisionHash: "bbbbbbbb", Percent: 20, Role: reportv1alpha1.RolloutRevisionTarget},
	}, got.Content.Components[0].Traffic)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateInProgress)

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{
		"SECRET_TAG", "SECRET_MAILBOX", "SECRET_METRIC", "SECRET_VALUE",
		"SECRET_QUERY", "SECRET_MESSAGE", "resourceVersion",
	} {
		assert.NotContains(t, string(encoded), secret)
	}
}

func TestProjectUsesPinnedCanaryPlanForPolicyOnlyLiveGroup(t *testing.T) {
	isvc := activeCanaryInferenceService()
	pinnedGroup := isvc.Spec.Rollout.Groups[0]
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		PolicyRef: &omev1beta1.RolloutPolicyRef{
			Name:        "guarded-canary",
			Progression: omev1beta1.RolloutProgressionCanary,
		},
	}}}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourcePolicy,
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name:        "guarded-canary",
				Progression: omev1beta1.RolloutProgressionCanary,
			},
			Group: pinnedGroup,
		}}},
	}}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Groups, 1)
	assert.Equal(t, reportv1alpha1.RolloutStrategyCanary, got.Content.Groups[0].Strategy)
	require.NotNil(t, got.Content.Groups[0].Step)
	assert.Equal(t, int32(2), got.Content.Groups[0].Step.Total)
	assert.Equal(t, "50%", got.Content.Groups[0].Step.Capacity)
	assert.Equal(t, int32(50), got.Content.Groups[0].Step.TargetTraffic)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateInProgress)
}

func TestProjectKeepsPinnedPlanWhenLiveSpecDriftsMidRun(t *testing.T) {
	isvc := activeCanaryInferenceService()
	pinnedGroup := isvc.Spec.Rollout.Groups[0]
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline,
			Group:  pinnedGroup,
		}}},
	}}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 10},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Groups, 1)
	require.NotNil(t, got.Content.Groups[0].Step)
	assert.Equal(t, "50%", got.Content.Groups[0].Step.Capacity)
	assert.Equal(t, int32(50), got.Content.Groups[0].Step.TargetTraffic)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateInProgress)
}

func TestProjectTreatsEmptyPinnedPlanAsAuthoritative(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{},
	}}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Empty(t, got.Content.Groups)
}

func TestProjectValidatesPinnedPlanInsteadOfLiveSpec(t *testing.T) {
	isvc := activeCanaryInferenceService()
	pinnedGroup := isvc.Spec.Rollout.Groups[0]
	pinnedGroup.Canary = &omev1beta1.GroupCanary{}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline,
			Group:  pinnedGroup,
		}}},
	}}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueSpecMalformed,
	})
}

func TestProjectMarksStatusDerivedSummaryEpochUnverifiable(t *testing.T) {
	isvc := activeCanaryInferenceService()

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.RolloutSummary{
		State:             reportv1alpha1.RolloutStateUnknown,
		ReportedState:     reportv1alpha1.RolloutStateInProgress,
		Evidence:          reportv1alpha1.EvidenceReported,
		Epoch:             reportv1alpha1.RolloutEpochUnverifiable,
		CoordinationReady: reportv1alpha1.RolloutConditionNotApplicable,
	}, got.Content.Summary)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueEpochUnverifiable,
	})
	assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{
		Code: reportv1alpha1.WarningPartialData,
	})
}

func TestProjectKeepsNonRolloutSummaryDeclared(t *testing.T) {
	isvc := baseInferenceService()
	mode := constants.RawDeployment
	isvc.Spec.DeploymentMode = &mode
	isvc.Spec.Engine = nil

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.RolloutSummary{
		State:             reportv1alpha1.RolloutStateNotConfigured,
		ReportedState:     reportv1alpha1.RolloutStateNotConfigured,
		Evidence:          reportv1alpha1.EvidenceDeclared,
		Epoch:             reportv1alpha1.RolloutEpochNotApplicable,
		CoordinationReady: reportv1alpha1.RolloutConditionNotApplicable,
	}, got.Content.Summary)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueEpochUnverifiable,
	})
	assert.Empty(t, got.Warnings)
}

func TestProjectImplicitIndependentStableFromLifecycle(t *testing.T) {
	isvc := independentLifecycleInferenceService(
		"chat-engine-aaaaaaaa",
		"chat-engine-aaaaaaaa",
	)

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.RolloutStrategyIndependent, got.Content.Components[0].Strategy)
	assert.Nil(t, got.Content.Components[0].Group)
	assert.Equal(t, reportv1alpha1.RolloutPhaseStable, got.Content.Components[0].Phase)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateSucceeded)
}

func TestProjectImplicitIndependentInProgressFromLifecycle(t *testing.T) {
	isvc := independentLifecycleInferenceService(
		"chat-engine-aaaaaaaa",
		"chat-engine-bbbbbbbb",
	)

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.RolloutStrategyIndependent, got.Content.Components[0].Strategy)
	assert.Equal(t, reportv1alpha1.RolloutPhaseUpdating, got.Content.Components[0].Phase)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateInProgress)
}

func TestProjectIncludesUngroupedIndependentAlongsideDeclaredGroup(t *testing.T) {
	isvc := coordinationBoundInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Status.Components[omev1beta1.DecoderComponent] = omev1beta1.ComponentStatusSpec{
		Lifecycle: &omev1beta1.LifecycleStatus{
			CurrentRevision: "chat-decoder-cccccccc",
			UpdateRevision:  "chat-decoder-dddddddd",
		},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Components, 2)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Components[0].Type)
	assert.Equal(t, reportv1alpha1.RolloutStrategyBlueGreen, got.Content.Components[0].Strategy)
	require.NotNil(t, got.Content.Components[0].Group)
	assert.Equal(t, 0, *got.Content.Components[0].Group)
	assert.Equal(t, reportv1alpha1.RuntimeComponentDecoder, got.Content.Components[1].Type)
	assert.Equal(t, reportv1alpha1.RolloutStrategyIndependent, got.Content.Components[1].Strategy)
	assert.Nil(t, got.Content.Components[1].Group)
	assert.Equal(t, reportv1alpha1.RolloutPhaseUpdating, got.Content.Components[1].Phase)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateInProgress)
}

func TestProjectExplicitOMENativeIndependentContributesUnknownBeforeLifecycleArrives(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Engine.Annotations = map[string]string{
		constants.DeploymentMode: string(constants.OMENative),
	}
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
	}}}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.DecoderComponent: {},
	}
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{
		Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
			Name:       "0",
			Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
			Policy:     omev1beta1.CoordinationPolicyBlueGreen,
			Phase:      omev1beta1.CoordinationPhaseIdle,
		}},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Components, 2)
	engine := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, engine.Type)
	assert.Equal(t, reportv1alpha1.RolloutStrategyIndependent, engine.Strategy)
	assert.Nil(t, engine.Group)
	assert.Equal(t, reportv1alpha1.RolloutPhaseUnknown, engine.Phase)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code:      reportv1alpha1.RolloutIssueComponentStatusMissing,
		Component: reportv1alpha1.RuntimeComponentEngine,
	})
	assertStatusDerivedSummary(t, got, reportv1alpha1.RolloutStateUnknown)
}

func TestProjectNonOMENativeWithoutRolloutRemainsNotConfigured(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Engine.Annotations = map[string]string{
		constants.DeploymentMode: string(constants.RawDeployment),
	}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.RolloutStrategyUnknown, got.Content.Components[0].Strategy)
	assert.Equal(t, reportv1alpha1.RolloutPhaseUnknown, got.Content.Components[0].Phase)
	assert.Equal(t, reportv1alpha1.RolloutSummary{
		State:             reportv1alpha1.RolloutStateNotConfigured,
		ReportedState:     reportv1alpha1.RolloutStateNotConfigured,
		Evidence:          reportv1alpha1.EvidenceDeclared,
		Epoch:             reportv1alpha1.RolloutEpochNotApplicable,
		CoordinationReady: reportv1alpha1.RolloutConditionNotApplicable,
	}, got.Content.Summary)
	assert.Empty(t, got.Content.Issues)
	assert.Empty(t, got.Warnings)
}

func TestProjectUnprovenModeWithoutLifecycleIsUnknown(t *testing.T) {
	isvc := baseInferenceService()
	mode := constants.RawDeployment
	isvc.Spec.DeploymentMode = &mode
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.RolloutStrategyUnknown, got.Content.Components[0].Strategy)
	assert.Equal(t, reportv1alpha1.RolloutPhaseUnknown, got.Content.Components[0].Phase)
	assertStatusDerivedSummary(t, got, reportv1alpha1.RolloutStateUnknown)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code:      reportv1alpha1.RolloutIssueComponentStatusMissing,
		Component: reportv1alpha1.RuntimeComponentEngine,
	})
}

func TestProjectRequiresExplicitNonOMENativeProofForEveryDeclaredComponent(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Spec.Engine.Annotations = map[string]string{
		constants.DeploymentMode: string(constants.RawDeployment),
	}

	partial, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assertStatusDerivedSummary(t, partial, reportv1alpha1.RolloutStateUnknown)
	assert.Contains(t, partial.Content.Issues, reportv1alpha1.RolloutIssue{
		Code:      reportv1alpha1.RolloutIssueComponentStatusMissing,
		Component: reportv1alpha1.RuntimeComponentDecoder,
	})

	isvc.Spec.Decoder.Annotations = map[string]string{
		constants.DeploymentMode: string(constants.MultiNode),
	}
	complete, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutSummary{
		State:             reportv1alpha1.RolloutStateNotConfigured,
		ReportedState:     reportv1alpha1.RolloutStateNotConfigured,
		Evidence:          reportv1alpha1.EvidenceDeclared,
		Epoch:             reportv1alpha1.RolloutEpochNotApplicable,
		CoordinationReady: reportv1alpha1.RolloutConditionNotApplicable,
	}, complete.Content.Summary)
	assert.Empty(t, complete.Content.Issues)
	assert.Empty(t, complete.Warnings)
}

func TestProjectBoundsInvalidIndependentLifecycleEvidence(t *testing.T) {
	tests := []struct {
		name     string
		current  string
		update   string
		phase    omev1beta1.RolloutPhase
		wantCode reportv1alpha1.RolloutIssueCode
	}{
		{
			name: "current revision absent", update: "chat-engine-bbbbbbbb",
			wantCode: reportv1alpha1.RolloutIssueComponentStatusMissing,
		},
		{
			name: "update revision absent", current: "chat-engine-aaaaaaaa",
			wantCode: reportv1alpha1.RolloutIssueComponentStatusMissing,
		},
		{
			name: "current revision identity is foreign", current: "other-engine-SECRET000",
			update: "chat-engine-bbbbbbbb", wantCode: reportv1alpha1.RolloutIssueRevisionNameInvalid,
		},
		{
			name:    "reported stable phase contradicts revision skew",
			current: "chat-engine-aaaaaaaa", update: "chat-engine-bbbbbbbb",
			phase: omev1beta1.RolloutPhaseStable, wantCode: reportv1alpha1.RolloutIssueStatusMalformed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := independentLifecycleInferenceService(tt.current, tt.update)
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = tt.phase
			isvc.Status.Components[omev1beta1.EngineComponent] = component

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			require.Len(t, got.Content.Components, 1)
			assert.Equal(t, reportv1alpha1.RolloutStrategyIndependent, got.Content.Components[0].Strategy)
			assert.Equal(t, reportv1alpha1.RolloutPhaseUnknown, got.Content.Components[0].Phase)
			assertStatusDerivedSummary(t, got, reportv1alpha1.RolloutStateUnknown)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: tt.wantCode, Component: reportv1alpha1.RuntimeComponentEngine,
			})
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "SECRET")
		})
	}
}

func TestProjectCollapsesShippedSequentialShape(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
		{Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent}},
		{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}, BlueGreen: &omev1beta1.GroupBlueGreen{}},
	}}
	isvc.Status.ObservedGeneration = 7
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.DecoderComponent: {},
		omev1beta1.EngineComponent:  {},
	}
	transitioned := metav1.NewTime(time.Date(2026, time.August, 31, 16, 0, 0, 0, time.UTC))
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
		Name: "0", Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent},
		Order:  []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent},
		Policy: omev1beta1.CoordinationPolicySequential, Phase: omev1beta1.CoordinationPhaseWaiting,
		CurrentComponent: omev1beta1.DecoderComponent, ObservedSurge: ptrIntOrString(intstr.FromString("25%")),
		LastTransitionTime: &transitioned, CompositePhase: "decoder.Waiting", Message: "SECRET_REASON",
	}}}
	isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutCoordinationReady), &apis.Condition{
		Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionFalse,
		Reason: "SECRET_CONDITION_REASON", Message: "SECRET_CONDITION_MESSAGE",
	})

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Groups, 1)
	assert.Equal(t, reportv1alpha1.RolloutGroupStatus{
		Index: 0, Strategy: reportv1alpha1.RolloutStrategySequential,
		Phase: reportv1alpha1.RolloutPhaseWaiting,
		Components: []reportv1alpha1.RuntimeComponentType{
			reportv1alpha1.RuntimeComponentDecoder, reportv1alpha1.RuntimeComponentEngine,
		},
		CurrentComponent: reportv1alpha1.RuntimeComponentDecoder,
		ObservedSurge:    "25%", TransitionedAt: ptrTime(transitioned.Time),
	}, got.Content.Groups[0])
	assert.Equal(t, reportv1alpha1.RolloutConditionFalse, got.Content.Summary.CoordinationReady)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateInProgress)
	require.Len(t, got.Content.Components, 2)
	for _, component := range got.Content.Components {
		require.NotNil(t, component.Group)
		assert.Equal(t, 0, *component.Group)
	}
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "SECRET_")
}

func TestProjectStrategyAndSummaryMatrix(t *testing.T) {
	tests := []struct {
		name      string
		group     omev1beta1.RolloutGroup
		status    *omev1beta1.RolloutCoordinationGroupStatus
		want      reportv1alpha1.RolloutStrategy
		wantState reportv1alpha1.RolloutState
		wantPhase reportv1alpha1.RolloutPhase
	}{
		{
			name: "blue green default", group: omev1beta1.RolloutGroup{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}},
			status: &omev1beta1.RolloutCoordinationGroupStatus{Name: "0", Policy: omev1beta1.CoordinationPolicyBlueGreen, Phase: omev1beta1.CoordinationPhaseIdle},
			want:   reportv1alpha1.RolloutStrategyBlueGreen, wantState: reportv1alpha1.RolloutStateSucceeded, wantPhase: reportv1alpha1.RolloutPhaseIdle,
		},
		{
			name: "blue green active", group: omev1beta1.RolloutGroup{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}, BlueGreen: &omev1beta1.GroupBlueGreen{}},
			status: &omev1beta1.RolloutCoordinationGroupStatus{Name: "0", Policy: omev1beta1.CoordinationPolicyBlueGreen, Phase: omev1beta1.CoordinationPhaseShifting},
			want:   reportv1alpha1.RolloutStrategyBlueGreen, wantState: reportv1alpha1.RolloutStateInProgress, wantPhase: reportv1alpha1.RolloutPhaseShifting,
		},
		{
			name: "rolling update active", group: omev1beta1.RolloutGroup{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}, RollingUpdate: &omev1beta1.GroupRollingUpdate{}},
			status: &omev1beta1.RolloutCoordinationGroupStatus{Name: "0", Policy: omev1beta1.CoordinationPolicyRollingUpdate, Phase: omev1beta1.CoordinationPhaseSurging},
			want:   reportv1alpha1.RolloutStrategyRollingUpdate, wantState: reportv1alpha1.RolloutStateInProgress, wantPhase: reportv1alpha1.RolloutPhaseSurging,
		},
		{
			name: "rolling update staged", group: omev1beta1.RolloutGroup{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}, RollingUpdate: &omev1beta1.GroupRollingUpdate{}},
			status: &omev1beta1.RolloutCoordinationGroupStatus{Name: "0", Policy: omev1beta1.CoordinationPolicyRollingUpdate, Phase: omev1beta1.CoordinationPhaseStaged},
			want:   reportv1alpha1.RolloutStrategyRollingUpdate, wantState: reportv1alpha1.RolloutStateStaged, wantPhase: reportv1alpha1.RolloutPhaseStaged,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := baseInferenceService()
			isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{tt.group}}
			isvc.Status.ObservedGeneration = 7
			isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: {},
			}
			status := *tt.status
			status.Components = append([]omev1beta1.ComponentType{}, tt.group.Components...)
			isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{Groups: []omev1beta1.RolloutCoordinationGroupStatus{status}}
			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			require.Len(t, got.Content.Groups, 1)
			assert.Equal(t, tt.want, got.Content.Groups[0].Strategy)
			assert.Equal(t, tt.wantPhase, got.Content.Groups[0].Phase)
			assertOnlyEpochBoundary(t, got, tt.wantState)
		})
	}
}

func TestProjectRejectsMalformedStoredRolloutSpecs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
		canary bool
	}{
		{
			name: "group member is not declared",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Engine = nil
			},
		},
		{
			name: "group member is not OMENative",
			mutate: func(isvc *omev1beta1.InferenceService) {
				mode := constants.RawDeployment
				isvc.Spec.DeploymentMode = &mode
			},
		},
		{
			name: "group order is not shipped",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Order = []omev1beta1.ComponentType{omev1beta1.EngineComponent}
			},
		},
		{
			name: "progression selectors are not one of",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].BlueGreen = &omev1beta1.GroupBlueGreen{}
				isvc.Spec.Rollout.Groups[0].RollingUpdate = &omev1beta1.GroupRollingUpdate{}
			},
		},
		{
			name: "rolling budgets both resolve to zero",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].BlueGreen = nil
				isvc.Spec.Rollout.Groups[0].RollingUpdate = &omev1beta1.GroupRollingUpdate{
					MaxSurge:       ptrIntOrString(intstr.FromInt(0)),
					MaxUnavailable: ptrIntOrString(intstr.FromString("0%")),
				}
			},
		},
		{
			name: "component lifecycle budgets both resolve to zero",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Engine.Lifecycle = lifecycleWithRollingBudgets(
					ptrIntOrString(intstr.FromString("0%")),
					ptrIntOrString(intstr.FromInt(0)),
				)
			},
		},
		{
			name: "component lifecycle budget is negative",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Engine.Lifecycle = lifecycleWithRollingBudgets(
					ptrIntOrString(intstr.FromInt(-1)), nil,
				)
			},
		},
		{
			name: "component lifecycle budget is a bare numeric string",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Engine.Lifecycle = lifecycleWithRollingBudgets(
					ptrIntOrString(intstr.FromString("3")), nil,
				)
			},
		},
		{
			name: "soak is not honored by one group",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Soak = &metav1.Duration{Duration: time.Minute}
			},
		},
		{
			name:   "canary steps are empty",
			canary: true,
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Canary.Steps = nil
			},
		},
		{
			name:   "canary traffic decreases",
			canary: true,
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Canary.Steps = []omev1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("60%"), Traffic: 60},
					{Capacity: intstr.FromString("80%"), Traffic: 50},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}
			},
		},
		{
			name:   "canary final traffic is partial",
			canary: true,
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Canary.Steps[1].Traffic = 90
			},
		},
		{
			name:   "canary final percent capacity is partial",
			canary: true,
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Canary.Steps[1].Capacity = intstr.FromString("50%")
			},
		},
		{
			name:   "analysis shape is incomplete",
			canary: true,
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis = &omev1beta1.RolloutAnalysis{}
			},
		},
		{
			name:   "analysis policy enum bypassed schema",
			canary: true,
			mutate: func(isvc *omev1beta1.InferenceService) {
				analysis := validAnalysis("latency")
				invalid := omev1beta1.OnInconclusive("SECRET_POLICY")
				analysis.OnInconclusive = &invalid
				isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis = analysis
			},
		},
		{
			name:   "step count bypassed schema",
			canary: true,
			mutate: func(isvc *omev1beta1.InferenceService) {
				steps := make([]omev1beta1.RolloutGroupStep, 21)
				for i := range steps {
					steps[i] = omev1beta1.RolloutGroupStep{Capacity: intstr.FromString("100%"), Traffic: 100}
				}
				isvc.Spec.Rollout.Groups[0].Canary.Steps = steps
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := coordinationBoundInferenceService()
			if tt.canary {
				isvc = activeCanaryInferenceService()
			}
			tt.mutate(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueSpecMalformed,
			})
			assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{
				Code: reportv1alpha1.WarningPartialData,
			})
		})
	}
}

func TestProjectUsesRolloutEvidenceWhenTopLevelObservedGenerationIsZero(t *testing.T) {
	isvc := baseInferenceService()
	mode := constants.OMENative
	isvc.Spec.DeploymentMode = &mode
	isvc.Spec.Engine = &omev1beta1.EngineSpec{}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
	}}}
	isvc.Status.ObservedGeneration = 0
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {},
	}
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
		Name: "0", Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Policy: omev1beta1.CoordinationPolicyBlueGreen, Phase: omev1beta1.CoordinationPhaseIdle,
	}}}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateSucceeded)

	summaryJSON, err := json.Marshal(got.Content.Summary)
	require.NoError(t, err)
	assert.NotContains(t, string(summaryJSON), "generation")
	assert.NotContains(t, string(summaryJSON), "freshness")
}

func TestProjectAcceptsCompletedCanarySentinel(t *testing.T) {
	isvc := completedCanaryInferenceService()

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateSucceeded)
	require.Len(t, got.Content.Groups, 1)
	assert.Equal(t, reportv1alpha1.RolloutPhaseStable, got.Content.Groups[0].Phase)
	assert.Equal(t, "bbbbbbbb", got.Content.Groups[0].TargetRevisionHash)
	assert.Empty(t, got.Content.Groups[0].StableRevisionHash)
	assert.Empty(t, got.Content.Groups[0].RejectedRevisionHash)
	assert.Nil(t, got.Content.Groups[0].Step)
}

func TestProjectRejectsInconsistentCompletedCanarySentinel(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{name: "step beyond sentinel", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Status.Canary.CurrentStep = 2 }},
		{name: "traffic weight is not complete", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Status.Canary.ObservedTrafficWeight = 99 }},
		{name: "old stable identity remains", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Status.Canary.StableRevisionHash = "aaaaaaaa" }},
		{name: "rollback identity remains", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Status.Canary.RolledBackRevisionHash = "cccccccc" }},
		{name: "promotion mailbox remains", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Status.Canary.PromotedThrough = "SECRET_MAILBOX" }},
		{name: "final spec traffic is not terminal", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Traffic = 50
		}},
		{name: "final spec capacity is not shipped form", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Capacity = intstr.FromString("3")
		}},
		{name: "final percentage capacity is not full", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Capacity = intstr.FromString("50%")
		}},
		{name: "final absolute capacity is zero", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Capacity = intstr.FromInt(0)
		}},
		{name: "target traffic absent", mutate: func(isvc *omev1beta1.InferenceService) {
			status := isvc.Status.Components[omev1beta1.EngineComponent]
			status.Traffic = nil
			isvc.Status.Components[omev1beta1.EngineComponent] = status
		}},
		{name: "traffic targets another revision", mutate: func(isvc *omev1beta1.InferenceService) {
			status := isvc.Status.Components[omev1beta1.EngineComponent]
			status.Traffic = []omev1beta1.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100}}
			isvc.Status.Components[omev1beta1.EngineComponent] = status
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := completedCanaryInferenceService()
			tt.mutate(isvc)
			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueCanaryStepInvalid, Group: ptrInt(0),
			})
		})
	}
}

func TestProjectAcceptsCompletedCanaryWithAbsoluteFinalCapacity(t *testing.T) {
	isvc := completedCanaryInferenceService()
	isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Capacity = intstr.FromInt(3)

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateSucceeded)
}

func TestProjectCanaryPhaseUsesShippedPrimarySelection(t *testing.T) {
	isvc := multiComponentCanaryInferenceService()

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Groups, 1)
	assert.Equal(t, reportv1alpha1.RolloutPhaseCanarying, got.Content.Groups[0].Phase)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateInProgress)
}

func TestProjectAcceptsControllerStepAdvanceBeforeNextTrafficProgramming(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*omev1beta1.InferenceService)
		wantIndex  int32
		wantTarget int32
	}{
		{
			name: "first intermediate advance",
			configure: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CurrentStep = 1
			},
			wantIndex: 1, wantTarget: 100,
		},
		{
			name: "later intermediate advance",
			configure: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Canary.Steps = []omev1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("25%"), Traffic: 25},
					{Capacity: intstr.FromString("50%"), Traffic: 50},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}
				isvc.Status.Canary.CurrentStep = 2
			},
			wantIndex: 2, wantTarget: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			// The controller applies step N's traffic and then advances
			// CurrentStep in the same status update. The next reconcile applies
			// step N+1's traffic.
			tt.configure(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateInProgress)
			require.Len(t, got.Content.Groups, 1)
			require.NotNil(t, got.Content.Groups[0].Step)
			assert.Equal(t, tt.wantIndex, got.Content.Groups[0].Step.Index)
			assert.Equal(t, tt.wantTarget, got.Content.Groups[0].Step.TargetTraffic)
			assert.Equal(t, int32(50), got.Content.Groups[0].Step.ObservedTraffic)
		})
	}
}

func TestProjectRejectsMalformedCanaryStepAdvanceEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{
			name: "arbitrary matching traffic is not a plan epoch",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CurrentStep = 1
				setActiveCanaryTraffic(isvc, 25)
			},
		},
		{
			name: "two-step-old traffic is not the advance edge",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Canary.Steps = []omev1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("25%"), Traffic: 25},
					{Capacity: intstr.FromString("50%"), Traffic: 50},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}
				isvc.Status.Canary.CurrentStep = 2
				setActiveCanaryTraffic(isvc, 25)
			},
		},
		{
			name: "paused phase remains bound to the current step",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CurrentStep = 1
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhasePaused
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
		},
		{
			name: "promoting phase remains bound to the current step",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CurrentStep = 1
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhasePromoting
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
		},
		{
			name: "primary traffic must match observed previous-step weight",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CurrentStep = 1
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.Traffic = []omev1beta1.ComponentTrafficTarget{{
					RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100,
				}}
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
		},
		{
			name: "primary traffic must use the bound revision epoch",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CurrentStep = 1
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.Traffic = []omev1beta1.ComponentTrafficTarget{
					{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 50},
					{RevisionName: "chat-engine-rev-cccccccc", Percent: 50},
				}
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			tt.mutate(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
			assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{
				Code: reportv1alpha1.WarningPartialData,
			})
		})
	}
}

func TestProjectRejectsControllerImpossibleCanaryStateMatrix(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{
			name: "intermediate promoting",
			mutate: func(isvc *omev1beta1.InferenceService) {
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhasePromoting
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
		},
		{
			name: "final paused",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CurrentStep = 1
				setActiveCanaryTraffic(isvc, 100)
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhasePaused
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
		},
		{
			name: "final canarying already at current traffic",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CurrentStep = 1
				setActiveCanaryTraffic(isvc, 100)
			},
		},
		{
			name: "step zero promoted through",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.PromotedThrough = "SECRET_PROMOTION"
			},
		},
		{
			name: "pending retains rollback identity",
			mutate: func(isvc *omev1beta1.InferenceService) {
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhasePending
				isvc.Status.Components[omev1beta1.EngineComponent] = status
				isvc.Status.Canary.RolledBackRevisionHash = "bbbbbbbb"
			},
		},
		{
			name: "failed retains rollback identity",
			mutate: func(isvc *omev1beta1.InferenceService) {
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhaseFailed
				isvc.Status.Components[omev1beta1.EngineComponent] = status
				isvc.Status.Canary.RolledBackRevisionHash = "bbbbbbbb"
			},
		},
		{
			name: "manual advance records a foreign promotion value",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Pause = &omev1beta1.RolloutPause{}
				isvc.Status.Canary.CurrentStep = 1
				isvc.Status.Canary.PromotedThrough = "SECRET_FOREIGN_PROMOTION"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			tt.mutate(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "SECRET_")
		})
	}
}

func TestProjectAcceptsControllerRepinPreStepHold(t *testing.T) {
	tests := []struct {
		name               string
		phase              omev1beta1.RolloutPhase
		globalPause        string
		wantState          reportv1alpha1.RolloutState
		postRepinStepClock bool
	}{
		{name: "capacity pending after step clock refresh", phase: omev1beta1.RolloutPhasePending, wantState: reportv1alpha1.RolloutStateInProgress, postRepinStepClock: true},
		{name: "immediately persisted repin boundary", phase: omev1beta1.RolloutPhaseCanarying, wantState: reportv1alpha1.RolloutStateInProgress},
		{name: "globally paused repin boundary", phase: omev1beta1.RolloutPhaseCanarying, globalPause: "true", wantState: reportv1alpha1.RolloutStateInProgress},
		{name: "ready and paused after step clock refresh", phase: omev1beta1.RolloutPhasePaused, wantState: reportv1alpha1.RolloutStatePaused, postRepinStepClock: true},
		{name: "capacity wait failed after step clock refresh", phase: omev1beta1.RolloutPhaseFailed, wantState: reportv1alpha1.RolloutStateFailed, postRepinStepClock: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			newSteps := []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("100%"), Traffic: 100,
			}}
			// clampCanary repinned a [50, 100] rollout while 50% was
			// programmed to the shorter, final-only [100] ladder. The
			// active run proves that this is a repin, rather than forged
			// stand-alone CanaryStatus residue.
			configureRolloutRepin(t, isvc, isvc.Spec.Rollout.Groups[0].Canary.Steps,
				newSteps, 0, 50, tt.phase, true)
			if tt.postRepinStepClock {
				entered := metav1.NewTime(isvc.Status.Rollout.ActiveRun.PinnedAt.Add(time.Minute))
				isvc.Status.Canary.StepEnteredTime = &entered
			}
			if tt.globalPause != "" {
				// Repin boundaries flush before the canary executor. A global
				// pause makes the old Canarying phase durable until resume.
				isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: tt.globalPause}
			}

			got, err := rolloutprojection.Project(isvc, fixedClock())

			require.NoError(t, err)
			assertStatusDerivedSummary(t, got, tt.wantState)
			require.Len(t, got.Content.Groups, 1)
			require.NotNil(t, got.Content.Groups[0].Step)
			assert.Equal(t, int32(0), got.Content.Groups[0].Step.Index)
			assert.Equal(t, int32(1), got.Content.Groups[0].Step.Total)
			assert.Equal(t, int32(100), got.Content.Groups[0].Step.TargetTraffic)
			assert.Equal(t, int32(50), got.Content.Groups[0].Step.ObservedTraffic)
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
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
				RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100,
			}}
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			configureRolloutRepin(t, isvc, isvc.Spec.Rollout.Groups[0].Canary.Steps,
				[]omev1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}},
				0, 50, omev1beta1.RolloutPhaseCanarying, true)
			tt.mutate(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())

			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
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
				{Capacity: intstr.FromString("50%"), Traffic: 50},
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
			isvc := activeCanaryInferenceService()
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
			entered := metav1.NewTime(time.Date(2026, time.August, 31, 18, 20, 0, 0, time.UTC))
			isvc.Status.Canary.CurrentStep = tt.currentStep
			isvc.Status.Canary.ObservedTrafficWeight = tt.observedTraffic
			isvc.Status.Canary.StepEnteredTime = &entered
			isvc.Status.Canary.PromotedThrough = tt.promotedThrough
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = tt.phase
			component.Traffic = []omev1beta1.ComponentTrafficTarget{{
				RevisionName: "chat-engine-rev-bbbbbbbb", Percent: tt.observedTraffic,
			}}
			if tt.observedTraffic < 100 {
				component.Traffic = append(component.Traffic, omev1beta1.ComponentTrafficTarget{
					RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100 - tt.observedTraffic,
				})
			}
			isvc.Status.Components[omev1beta1.EngineComponent] = component
			opened := metav1.NewTime(time.Date(2026, time.August, 31, 18, 15, 0, 0, time.UTC))
			pinned := metav1.NewTime(time.Date(2026, time.August, 31, 18, 25, 0, 0, time.UTC))
			oldGroup := group
			oldGroup.Canary = &omev1beta1.GroupCanary{Steps: tt.oldSteps}
			oldDigest, digestErr := rolloutpolicy.ProgressionDigest(&oldGroup)
			require.NoError(t, digestErr)
			pinnedDigest, digestErr := rolloutpolicy.ProgressionDigest(&group)
			require.NoError(t, digestErr)
			runIdentity := fmt.Sprintf(
				"%s=%s;%s%s", omev1beta1.EngineComponent, "bbbbbbbb",
				oldDigest, opened.Time.UTC().Format(time.RFC3339),
			)
			isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
				RunID: "chat-" + rolloutpolicy.ShortHash([]byte(runIdentity)), OpenedAt: opened, PinnedAt: pinned,
				TargetRevisions: []omev1beta1.RolloutRunTarget{{
					Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb",
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

			got, err := rolloutprojection.Project(isvc, fixedClock())

			require.NoError(t, err)
			require.Len(t, got.Content.Groups, 1)
			if !tt.accepted {
				assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
					Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
				})
				return
			}
			require.NotNil(t, got.Content.Groups[0].Step)
			assert.Equal(t, tt.currentStep, got.Content.Groups[0].Step.Index)
			assert.Equal(t, int32(len(tt.steps)), got.Content.Groups[0].Step.Total)
			assert.Equal(t, tt.wantTarget, got.Content.Groups[0].Step.TargetTraffic)
			assert.Equal(t, tt.observedTraffic, got.Content.Groups[0].Step.ObservedTraffic)
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
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
			refreshRolloutPinnedDigest(t, pinned)
		}},
		{name: "policy server address is not portable", mutate: func(isvc *omev1beta1.InferenceService) {
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Group.Canary.Prometheus = &omev1beta1.AnalysisPrometheus{
				ServerAddress: "https://prometheus.internal.example",
			}
			refreshRolloutPinnedDigest(t, pinned)
		}},
		{name: "policy auth reference is not portable", mutate: func(isvc *omev1beta1.InferenceService) {
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Group.Canary.Prometheus = &omev1beta1.AnalysisPrometheus{
				AuthRef: &corev1.SecretKeySelector{Key: "token"},
			}
			refreshRolloutPinnedDigest(t, pinned)
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
			isvc := activeCanaryInferenceService()
			configureRolloutRepin(t, isvc,
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

			got, err := rolloutprojection.Project(isvc, fixedClock())

			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
		})
	}
}

func TestProjectAcceptsPolicyRepinWithReorderedTargets(t *testing.T) {
	isvc := activeCanaryInferenceService()
	configureRolloutRepin(t, isvc,
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
		{Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb"},
		{Component: omev1beta1.DecoderComponent, Revision: "cccccccc"},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())

	require.NoError(t, err)
	require.Len(t, got.Content.Groups, 1)
	require.NotNil(t, got.Content.Groups[0].Step)
	assert.Equal(t, int32(1), got.Content.Groups[0].Step.Index)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
	})

	// A derived service pins the same policy body from name-only provenance;
	// no local policy object means there is no generation, kind, or progression.
	pinned.PolicyGeneration = 0
	pinned.PolicyRef.Kind = ""
	pinned.PolicyRef.Progression = ""
	got, err = rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	require.NotNil(t, got.Content.Groups[0].Step)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
	})
}

func TestProjectAcceptsRollbackRepinPreStepHold(t *testing.T) {
	tests := []struct {
		phase     omev1beta1.RolloutPhase
		wantState reportv1alpha1.RolloutState
	}{
		{phase: omev1beta1.RolloutPhaseRollingBack, wantState: reportv1alpha1.RolloutStateRollingBack},
		{phase: omev1beta1.RolloutPhaseRolledBack, wantState: reportv1alpha1.RolloutStateRolledBack},
	}
	for _, tt := range tests {
		t.Run(string(tt.phase), func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			// Repin runs before closed-outcome detection, so clampCanary can
			// persist a hold while the rejected revision is still rolling back
			// or already held rolled back.
			newSteps := []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("100%"), Traffic: 100,
			}}
			configureRolloutRepin(t, isvc, isvc.Spec.Rollout.Groups[0].Canary.Steps,
				newSteps, 0, 0, tt.phase, true)
			isvc.Status.Canary.RolledBackRevisionHash = isvc.Status.Canary.CanaryRevisionHash
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = tt.phase
			component.Traffic = []omev1beta1.ComponentTrafficTarget{{
				RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100,
			}}
			isvc.Status.Components[omev1beta1.EngineComponent] = component

			got, err := rolloutprojection.Project(isvc, fixedClock())

			require.NoError(t, err)
			assertStatusDerivedSummary(t, got, tt.wantState)
			require.Len(t, got.Content.Groups, 1)
			require.NotNil(t, got.Content.Groups[0].Step)
			assert.Equal(t, int32(0), got.Content.Groups[0].Step.Index)
			assert.Equal(t, int32(0), got.Content.Groups[0].Step.ObservedTraffic)
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
		})
	}
}

func TestProjectAcceptsPromotedThroughAfterBackwardRepinClamp(t *testing.T) {
	isvc := activeCanaryInferenceService()
	newSteps := []omev1beta1.RolloutGroupStep{{
		Capacity: intstr.FromString("100%"), Traffic: 100,
	}}
	configureRolloutRepin(t, isvc, isvc.Spec.Rollout.Groups[0].Canary.Steps,
		newSteps, 0, 50, omev1beta1.RolloutPhaseCanarying, true)
	isvc.Status.Canary.PromotedThrough = "SECRET_OLD_PROMOTE"
	// The globally paused executor preserves the phase written before repin,
	// while the run boundary has already persisted the clamped step and hold.
	isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}

	got, err := rolloutprojection.Project(isvc, fixedClock())

	require.NoError(t, err)
	assertStatusDerivedSummary(t, got, reportv1alpha1.RolloutStateInProgress)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
	})
	encoded, marshalErr := json.Marshal(got)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(encoded), "SECRET_OLD_PROMOTE")
}

func TestProjectPreStepHoldRequiresExactTypedTrafficEvidence(t *testing.T) {
	tests := []struct {
		name     string
		phase    omev1beta1.RolloutPhase
		mutate   func(*omev1beta1.InferenceService)
		wantCode reportv1alpha1.RolloutIssueCode
	}{
		{name: "pending missing", phase: omev1beta1.RolloutPhasePending, mutate: func(isvc *omev1beta1.InferenceService) {
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.Traffic = nil
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
		{name: "canarying missing", phase: omev1beta1.RolloutPhaseCanarying, mutate: func(isvc *omev1beta1.InferenceService) {
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.Traffic = nil
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
		{name: "canarying different split", phase: omev1beta1.RolloutPhaseCanarying, mutate: func(isvc *omev1beta1.InferenceService) {
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.Traffic[0].Percent = 60
			component.Traffic[1].Percent = 40
			isvc.Status.Components[omev1beta1.EngineComponent] = component
		}},
		{name: "canarying target does not raise exposure", phase: omev1beta1.RolloutPhaseCanarying, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Traffic = 50
		}},
		{name: "canarying unsafe observed weight", phase: omev1beta1.RolloutPhaseCanarying, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.ObservedTrafficWeight = 101
		}},
		{name: "canarying completed step", phase: omev1beta1.RolloutPhaseCanarying, wantCode: reportv1alpha1.RolloutIssueCanaryStepInvalid, mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.CurrentStep = 1
			isvc.Status.Canary.ObservedTrafficWeight = 100
			isvc.Status.Canary.StableRevisionHash = ""
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			isvc.Spec.Rollout.Groups[0].Canary.Steps = []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("100%"), Traffic: 100,
			}}
			isvc.Status.Canary.PreStepHold = true
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = tt.phase
			isvc.Status.Components[omev1beta1.EngineComponent] = component
			tt.mutate(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())

			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			wantCode := tt.wantCode
			if wantCode == "" {
				wantCode = reportv1alpha1.RolloutIssueStatusMalformed
			}
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: wantCode, Group: ptrInt(0),
			})
		})
	}
}

func TestProjectAcceptsEqualWeightFinalStepAdvanceEdge(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Traffic = 100
	isvc.Status.Canary.CurrentStep = 1
	isvc.Status.Canary.ObservedTrafficWeight = 100
	component := isvc.Status.Components[omev1beta1.EngineComponent]
	component.Traffic = []omev1beta1.ComponentTrafficTarget{{
		RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100,
	}}
	isvc.Status.Components[omev1beta1.EngineComponent] = component

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Groups, 1)
	require.NotNil(t, got.Content.Groups[0].Step)
	assert.Equal(t, int32(1), got.Content.Groups[0].Step.Index)
	assert.Equal(t, int32(100), got.Content.Groups[0].Step.ObservedTraffic)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
	})
}

func TestProjectAcceptsAutomaticPromotedThroughResidue(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Status.Canary.CurrentStep = 1
	isvc.Status.Canary.PromotedThrough = "SECRET_AUTOMATIC_PROMOTION"

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
	})
	encoded, marshalErr := json.Marshal(got)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(encoded), "SECRET_AUTOMATIC_PROMOTION")
}

func TestProjectBindsActiveCanaryStatusToPrimaryTraffic(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*omev1beta1.InferenceService)
		valid     bool
		wantState reportv1alpha1.RolloutState
	}{
		{
			name:      "canarying exact split",
			configure: func(*omev1beta1.InferenceService) {},
			valid:     true, wantState: reportv1alpha1.RolloutStateInProgress,
		},
		{
			name: "paused exact split",
			configure: func(isvc *omev1beta1.InferenceService) {
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhasePaused
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
			valid: true, wantState: reportv1alpha1.RolloutStatePaused,
		},
		{
			name: "promoting exact cutover",
			configure: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CurrentStep = 1
				isvc.Status.Canary.ObservedTrafficWeight = 100
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.RolloutPhase = omev1beta1.RolloutPhasePromoting
				status.Traffic = []omev1beta1.ComponentTrafficTarget{{
					RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100,
				}}
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
			valid: true, wantState: reportv1alpha1.RolloutStateInProgress,
		},
		{
			name: "rolling back exact stable traffic",
			configure: func(isvc *omev1beta1.InferenceService) {
				configureRollbackCanary(isvc, omev1beta1.RolloutPhaseRollingBack)
			},
			valid: true, wantState: reportv1alpha1.RolloutStateRollingBack,
		},
		{
			name: "rolled back exact stable traffic",
			configure: func(isvc *omev1beta1.InferenceService) {
				configureRollbackCanary(isvc, omev1beta1.RolloutPhaseRolledBack)
			},
			valid: true, wantState: reportv1alpha1.RolloutStateRolledBack,
		},
		{
			name: "observed weight from another epoch",
			configure: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.ObservedTrafficWeight = 25
			},
		},
		{
			name: "target hash from another epoch",
			configure: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.CanaryRevisionHash = "cccccccc"
			},
		},
		{
			name: "stable hash from another epoch",
			configure: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Canary.StableRevisionHash = "cccccccc"
			},
		},
		{
			name: "primary split from another epoch",
			configure: func(isvc *omev1beta1.InferenceService) {
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.Traffic = []omev1beta1.ComponentTrafficTarget{
					{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 60},
					{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 40},
				}
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
		},
		{
			name: "rollback status still routes rejected revision",
			configure: func(isvc *omev1beta1.InferenceService) {
				configureRollbackCanary(isvc, omev1beta1.RolloutPhaseRollingBack)
				status := isvc.Status.Components[omev1beta1.EngineComponent]
				status.Traffic = []omev1beta1.ComponentTrafficTarget{{
					RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100,
				}}
				isvc.Status.Components[omev1beta1.EngineComponent] = status
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			tt.configure(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			if tt.valid {
				assertStatusDerivedSummary(t, got, tt.wantState)
				assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
					Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
				})
				return
			}
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
		})
	}
}

func TestProjectAnalysisRequiresExactActiveStepMetricSet(t *testing.T) {
	tests := []struct {
		name    string
		results []omev1beta1.AnalysisMetricResult
		want    reportv1alpha1.RolloutAnalysisState
		issue   bool
	}{
		{
			name: "exact passing set",
			results: []omev1beta1.AnalysisMetricResult{
				exactMetricResult("latency", "1", true),
				exactMetricResult("errors", "0", true),
			},
			want: reportv1alpha1.RolloutAnalysisPassing,
		},
		{
			name: "exact failing set",
			results: []omev1beta1.AnalysisMetricResult{
				exactMetricResult("errors", "2", false),
				exactMetricResult("latency", "1", true),
			},
			want: reportv1alpha1.RolloutAnalysisFailing,
		},
		{
			name:    "missing result",
			results: []omev1beta1.AnalysisMetricResult{exactMetricResult("latency", "1", true)},
			want:    reportv1alpha1.RolloutAnalysisInconclusive, issue: true,
		},
		{
			name: "extra result",
			results: []omev1beta1.AnalysisMetricResult{
				exactMetricResult("latency", "1", true),
				exactMetricResult("errors", "0", true),
				exactMetricResult("foreign", "0", true),
			},
			want: reportv1alpha1.RolloutAnalysisInconclusive, issue: true,
		},
		{
			name: "duplicate result",
			results: []omev1beta1.AnalysisMetricResult{
				exactMetricResult("latency", "1", true),
				exactMetricResult("latency", "1", true),
			},
			want: reportv1alpha1.RolloutAnalysisInconclusive, issue: true,
		},
		{
			name: "threshold belongs to an earlier step",
			results: []omev1beta1.AnalysisMetricResult{
				{Name: "latency", Value: "1", Threshold: "2", Operator: omev1beta1.ComparisonLTE, Passed: true},
				exactMetricResult("errors", "0", true),
			},
			want: reportv1alpha1.RolloutAnalysisInconclusive, issue: true,
		},
		{
			name: "operator belongs to an earlier step",
			results: []omev1beta1.AnalysisMetricResult{
				{Name: "latency", Value: "1", Threshold: "1", Operator: omev1beta1.ComparisonGTE, Passed: true},
				exactMetricResult("errors", "0", true),
			},
			want: reportv1alpha1.RolloutAnalysisInconclusive, issue: true,
		},
		{
			name: "no results",
			want: reportv1alpha1.RolloutAnalysisUnobserved,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			analysis := validAnalysis("latency")
			analysis.Metrics = append(analysis.Metrics, omev1beta1.AnalysisMetric{
				Name: "errors", Query: "safe_query", Operator: omev1beta1.ComparisonLTE, Threshold: "1",
			})
			isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis = analysis
			setAnalysisResults(isvc, tt.results)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			require.NotNil(t, got.Content.Groups[0].Step)
			assert.Equal(t, tt.want, got.Content.Groups[0].Step.Analysis)
			issue := reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueAnalysisInconclusive, Group: ptrInt(0),
			}
			if tt.issue {
				assert.Contains(t, got.Content.Issues, issue)
				assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{Code: reportv1alpha1.WarningPartialData})
			} else {
				assert.NotContains(t, got.Content.Issues, issue)
			}
		})
	}
}

func TestProjectAnalysisFailTakesPrecedenceOverInconclusive(t *testing.T) {
	isvc := activeCanaryInferenceService()
	isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis = validAnalysis("latency")
	setAnalysisResults(isvc, []omev1beta1.AnalysisMetricResult{
		exactMetricResult("latency", "2", false),
		{Name: "auth", Passed: false, Message: "SECRET_AUTH_ERROR"},
	})

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.NotNil(t, got.Content.Groups[0].Step)
	assert.Equal(t, reportv1alpha1.RolloutAnalysisFailing, got.Content.Groups[0].Step.Analysis)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueAnalysisInconclusive, Group: ptrInt(0),
	})
	encoded, marshalErr := json.Marshal(got)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(encoded), "SECRET_AUTH_ERROR")
}

func TestProjectRejectsControllerImpossibleAnalysisEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{name: "nonnumeric value", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.MetricResults[0].Value = "SECRET_NOT_A_NUMBER"
		}},
		{name: "nan value", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.MetricResults[0].Value = "NaN"
		}},
		{name: "positive infinity value", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.MetricResults[0].Value = "+Inf"
		}},
		{name: "negative infinity value", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.MetricResults[0].Value = "-Inf"
		}},
		{name: "passed boolean contradicts comparison", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.MetricResults[0].Value = "2"
			isvc.Status.Canary.MetricResults[0].Passed = true
		}},
		{name: "empty value marked passed", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.MetricResults[0].Value = ""
			isvc.Status.Canary.MetricResults[0].Passed = true
		}},
		{name: "sample time absent", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.MetricResults[0].Time = nil
		}},
		{name: "sample time differs from evaluation", mutate: func(isvc *omev1beta1.InferenceService) {
			other := metav1.NewTime(isvc.Status.Canary.LastEvaluationTime.Time.Add(time.Second))
			isvc.Status.Canary.MetricResults[0].Time = &other
		}},
		{name: "sample time is zero", mutate: func(isvc *omev1beta1.InferenceService) {
			zero := metav1.Time{}
			isvc.Status.Canary.MetricResults[0].Time = &zero
		}},
		{name: "evaluation time is zero", mutate: func(isvc *omev1beta1.InferenceService) {
			zero := metav1.Time{}
			isvc.Status.Canary.LastEvaluationTime = &zero
		}},
		{name: "evaluation exists without results", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.MetricResults = nil
		}},
		{name: "conclusive time after evaluation", mutate: func(isvc *omev1beta1.InferenceService) {
			other := metav1.NewTime(isvc.Status.Canary.LastEvaluationTime.Time.Add(time.Second))
			isvc.Status.Canary.LastConclusiveEvaluationTime = &other
		}},
		{name: "negative failure counter", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.AnalysisFailedChecks = -1
		}},
		{name: "failing sample has zero counter", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.MetricResults[0].Value = "2"
			isvc.Status.Canary.MetricResults[0].Passed = false
			isvc.Status.Canary.AnalysisFailedChecks = 0
		}},
		{name: "positive counter has no conclusive history", mutate: func(isvc *omev1beta1.InferenceService) {
			isvc.Status.Canary.AnalysisFailedChecks = 1
			isvc.Status.Canary.LastConclusiveEvaluationTime = nil
		}},
		{name: "current pass lacks matching conclusive time", mutate: func(isvc *omev1beta1.InferenceService) {
			other := metav1.NewTime(isvc.Status.Canary.LastEvaluationTime.Time.Add(-time.Second))
			isvc.Status.Canary.LastConclusiveEvaluationTime = &other
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := analysisEvidenceInferenceService()
			tt.mutate(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "SECRET_NOT_A_NUMBER")
		})
	}
}

func TestProjectAcceptsLegitimateInconclusiveAnalysisEvidence(t *testing.T) {
	tests := []struct {
		name    string
		results []omev1beta1.AnalysisMetricResult
	}{
		{name: "synthetic auth result", results: []omev1beta1.AnalysisMetricResult{{
			Name: "auth", Passed: false, Message: "SECRET_AUTH_ERROR",
		}}},
		{name: "stale metric plan", results: []omev1beta1.AnalysisMetricResult{{
			Name: "latency", Value: "1", Threshold: "2", Operator: omev1beta1.ComparisonLTE, Passed: true,
		}}},
		{name: "matching no-data result", results: []omev1beta1.AnalysisMetricResult{{
			Name: "latency", Threshold: "1", Operator: omev1beta1.ComparisonLTE,
			Passed: false, Message: "SECRET_NO_DATA",
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis = validAnalysis("latency")
			setAnalysisResults(isvc, tt.results)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			require.NotNil(t, got.Content.Groups[0].Step)
			assert.Equal(t, reportv1alpha1.RolloutAnalysisInconclusive, got.Content.Groups[0].Step.Analysis)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueAnalysisInconclusive, Group: ptrInt(0),
			})
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "SECRET_")
		})
	}
}

func TestProjectAcceptsControllerRetargetBeforeCanaryTrafficIsApplied(t *testing.T) {
	tests := []struct {
		name      string
		phase     omev1beta1.RolloutPhase
		wantState reportv1alpha1.RolloutState
	}{
		{name: "capacity is pending", phase: omev1beta1.RolloutPhasePending, wantState: reportv1alpha1.RolloutStateInProgress},
		{name: "capacity wait failed", phase: omev1beta1.RolloutPhaseFailed, wantState: reportv1alpha1.RolloutStateFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := activeCanaryInferenceService()
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.RolloutPhase = tt.phase
			isvc.Status.Components[omev1beta1.EngineComponent] = component
			// resetCanaryStatus binds the new target and clears observed traffic.
			// While its capacity gate is unsatisfied, the controller writes Pending
			// (then possibly Failed) before applyTraffic, so the component traffic
			// can still truthfully describe the previous target epoch.
			isvc.Status.Canary.CanaryRevisionHash = "cccccccc"
			isvc.Status.Canary.ObservedTrafficWeight = 0

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assertStatusDerivedSummary(t, got, tt.wantState)
			assert.Equal(t, "cccccccc", got.Content.Groups[0].TargetRevisionHash)
			assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
		})
	}
}

func TestProjectRejectsContradictoryNonPrimaryCanaryPhase(t *testing.T) {
	isvc := multiComponentCanaryInferenceService()
	engine := isvc.Status.Components[omev1beta1.EngineComponent]
	engine.RolloutPhase = omev1beta1.RolloutPhaseFailed
	isvc.Status.Components[omev1beta1.EngineComponent] = engine

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
		Component: reportv1alpha1.RuntimeComponentEngine,
	})
}

func TestProjectRejectsCoordinationStatusFromAnotherSpec(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.RolloutCoordinationGroupStatus)
	}{
		{name: "policy absent", mutate: func(status *omev1beta1.RolloutCoordinationGroupStatus) { status.Policy = "" }},
		{name: "components absent", mutate: func(status *omev1beta1.RolloutCoordinationGroupStatus) { status.Components = nil }},
		{name: "components belong to old spec", mutate: func(status *omev1beta1.RolloutCoordinationGroupStatus) {
			status.Components = []omev1beta1.ComponentType{omev1beta1.DecoderComponent}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := coordinationBoundInferenceService()
			tt.mutate(&isvc.Status.RolloutCoordination.Groups[0])

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
		})
	}
}

func TestProjectRejectsSequentialStatusOrderFromOldSpec(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.ObservedGeneration = 7
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
		{Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent}},
		{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}},
	}}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.DecoderComponent: {},
		omev1beta1.EngineComponent:  {},
	}
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
		Name: "0", Policy: omev1beta1.CoordinationPolicySequential, Phase: omev1beta1.CoordinationPhaseIdle,
		Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent},
		Order:      []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.DecoderComponent},
	}}}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
	})
}

func TestProjectAggregatesGroupedComponentEvidence(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*omev1beta1.InferenceService)
		want      reportv1alpha1.RolloutState
		wantIssue reportv1alpha1.RolloutIssue
		hasIssue  bool
		useCanary bool
	}{
		{
			name: "missing coordination member status",
			mutate: func(isvc *omev1beta1.InferenceService) {
				delete(isvc.Status.Components, omev1beta1.EngineComponent)
			},
			want: reportv1alpha1.RolloutStateUnknown,
			wantIssue: reportv1alpha1.RolloutIssue{
				Code:  reportv1alpha1.RolloutIssueComponentStatusMissing,
				Group: ptrInt(0), Component: reportv1alpha1.RuntimeComponentEngine,
			},
			hasIssue: true,
		},
		{
			name: "empty coordination member phase is group-owned",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{}
			},
			want: reportv1alpha1.RolloutStateSucceeded,
		},
		{
			name: "failed coordination member contradicts idle group",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
					RolloutPhase: omev1beta1.RolloutPhaseFailed,
				}
			},
			want: reportv1alpha1.RolloutStateUnknown,
			wantIssue: reportv1alpha1.RolloutIssue{
				Code:  reportv1alpha1.RolloutIssueStatusMalformed,
				Group: ptrInt(0), Component: reportv1alpha1.RuntimeComponentEngine,
			},
			hasIssue: true,
		},
		{
			name: "rolling back coordination member contradicts idle group",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
					RolloutPhase: omev1beta1.RolloutPhaseRollingBack,
				}
			},
			want: reportv1alpha1.RolloutStateUnknown,
			wantIssue: reportv1alpha1.RolloutIssue{
				Code:  reportv1alpha1.RolloutIssueStatusMalformed,
				Group: ptrInt(0), Component: reportv1alpha1.RuntimeComponentEngine,
			},
			hasIssue: true,
		},
		{
			name: "failed coordination member agrees with failed group",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.RolloutCoordination.Groups[0].Phase = omev1beta1.CoordinationPhaseFailed
				isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
					RolloutPhase: omev1beta1.RolloutPhaseFailed,
				}
			},
			want: reportv1alpha1.RolloutStateFailed,
		},
		{
			name: "rolling back coordination member agrees with rolling back group",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.RolloutCoordination.Groups[0].Phase = omev1beta1.CoordinationPhaseRollingBack
				isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
					RolloutPhase: omev1beta1.RolloutPhaseRollingBack,
				}
			},
			want: reportv1alpha1.RolloutStateRollingBack,
		},
		{
			name: "paused coordination member agrees with paused group",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.RolloutCoordination.Groups[0].Phase = omev1beta1.CoordinationPhasePaused
				isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
					RolloutPhase: omev1beta1.RolloutPhasePaused,
				}
			},
			want: reportv1alpha1.RolloutStatePaused,
		},
		{
			name: "pending coordination member agrees with active group",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.RolloutCoordination.Groups[0].Phase = omev1beta1.CoordinationPhaseWaiting
				isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
					RolloutPhase: omev1beta1.RolloutPhasePending,
				}
			},
			want: reportv1alpha1.RolloutStateInProgress,
		},
		{
			name: "stable coordination member does not override active group",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.RolloutCoordination.Groups[0].Phase = omev1beta1.CoordinationPhaseWaiting
				isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
					RolloutPhase: omev1beta1.RolloutPhaseStable,
				}
			},
			want: reportv1alpha1.RolloutStateInProgress,
		},
		{
			name: "unknown coordination member phase is malformed",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
					RolloutPhase: omev1beta1.RolloutPhase("SECRET_PHASE"),
				}
			},
			want: reportv1alpha1.RolloutStateUnknown,
			wantIssue: reportv1alpha1.RolloutIssue{
				Code:  reportv1alpha1.RolloutIssueStatusMalformed,
				Group: ptrInt(0), Component: reportv1alpha1.RuntimeComponentEngine,
			},
			hasIssue: true,
		},
		{
			name:      "missing canary secondary is unknown",
			useCanary: true,
			mutate: func(isvc *omev1beta1.InferenceService) {
				delete(isvc.Status.Components, omev1beta1.EngineComponent)
			},
			want: reportv1alpha1.RolloutStateUnknown,
			wantIssue: reportv1alpha1.RolloutIssue{
				Code:  reportv1alpha1.RolloutIssueComponentStatusMissing,
				Group: ptrInt(0), Component: reportv1alpha1.RuntimeComponentEngine,
			},
			hasIssue: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := coordinationBoundInferenceService()
			if tt.useCanary {
				isvc = multiComponentCanaryInferenceService()
			}
			tt.mutate(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Equal(t, tt.want, got.Content.Summary.ReportedState)
			if tt.hasIssue {
				assert.Contains(t, got.Content.Issues, tt.wantIssue)
				assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{
					Code: reportv1alpha1.WarningPartialData,
				})
			} else {
				assertOnlyEpochBoundary(t, got, tt.want)
			}
		})
	}
}

func TestProjectBindsCoordinationReadyToObservedGroupPhases(t *testing.T) {
	tests := []struct {
		name      string
		phase     omev1beta1.CoordinationPhase
		condition corev1.ConditionStatus
		want      reportv1alpha1.RolloutConditionState
		malformed bool
	}{
		{name: "idle true", phase: omev1beta1.CoordinationPhaseIdle, condition: corev1.ConditionTrue, want: reportv1alpha1.RolloutConditionTrue},
		{name: "staged true", phase: omev1beta1.CoordinationPhaseStaged, condition: corev1.ConditionTrue, want: reportv1alpha1.RolloutConditionTrue},
		{name: "active false", phase: omev1beta1.CoordinationPhaseSurging, condition: corev1.ConditionFalse, want: reportv1alpha1.RolloutConditionFalse},
		{name: "failed false", phase: omev1beta1.CoordinationPhaseFailed, condition: corev1.ConditionFalse, want: reportv1alpha1.RolloutConditionFalse},
		{name: "unobserved phase unknown", phase: "", condition: corev1.ConditionUnknown, want: reportv1alpha1.RolloutConditionUnknown},
		{name: "idle cannot be false", phase: omev1beta1.CoordinationPhaseIdle, condition: corev1.ConditionFalse, want: reportv1alpha1.RolloutConditionInvalid, malformed: true},
		{name: "active cannot be true", phase: omev1beta1.CoordinationPhaseSurging, condition: corev1.ConditionTrue, want: reportv1alpha1.RolloutConditionInvalid, malformed: true},
		{name: "failed cannot be true", phase: omev1beta1.CoordinationPhaseFailed, condition: corev1.ConditionTrue, want: reportv1alpha1.RolloutConditionInvalid, malformed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := coordinationBoundInferenceService()
			isvc.Status.RolloutCoordination.Groups[0].Phase = tt.phase
			isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutCoordinationReady), &apis.Condition{
				Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: tt.condition,
			})

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Content.Summary.CoordinationReady)
			if tt.malformed {
				assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
				assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
					Code: reportv1alpha1.RolloutIssueStatusMalformed,
				})
			}
		})
	}
}

func TestProjectRejectsPolicyForeignCoordinationStatusFields(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.RolloutCoordinationGroupStatus)
	}{
		{
			name: "blue green order residue",
			mutate: func(status *omev1beta1.RolloutCoordinationGroupStatus) {
				status.Order = []omev1beta1.ComponentType{omev1beta1.EngineComponent}
			},
		},
		{
			name: "blue green current component residue",
			mutate: func(status *omev1beta1.RolloutCoordinationGroupStatus) {
				status.CurrentComponent = omev1beta1.EngineComponent
			},
		},
		{
			name: "blue green previous component residue",
			mutate: func(status *omev1beta1.RolloutCoordinationGroupStatus) {
				status.PreviousComponent = omev1beta1.EngineComponent
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := coordinationBoundInferenceService()
			tt.mutate(&isvc.Status.RolloutCoordination.Groups[0])

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
		})
	}
}

func TestProjectBindsSequentialCursorToExactOrder(t *testing.T) {
	tests := []struct {
		name      string
		phase     omev1beta1.CoordinationPhase
		composite string
		current   omev1beta1.ComponentType
		previous  omev1beta1.ComponentType
		valid     bool
	}{
		{name: "idle settled cursor", phase: omev1beta1.CoordinationPhaseIdle, valid: true},
		{name: "idle cannot await first component", phase: omev1beta1.CoordinationPhaseIdle, composite: omev1beta1.CompositePhaseSequentialAwaiting, current: omev1beta1.DecoderComponent},
		{name: "idle awaits next component", phase: omev1beta1.CoordinationPhaseIdle, composite: omev1beta1.CompositePhaseSequentialAwaiting, current: omev1beta1.EngineComponent, previous: omev1beta1.DecoderComponent, valid: true},
		{name: "active phase requires current component", phase: omev1beta1.CoordinationPhaseSurging},
		{name: "active first component has no predecessor", phase: omev1beta1.CoordinationPhaseSurging, current: omev1beta1.DecoderComponent, valid: true},
		{name: "active second component follows first", phase: omev1beta1.CoordinationPhaseWaiting, current: omev1beta1.EngineComponent, previous: omev1beta1.DecoderComponent, valid: true},
		{name: "failed phase requires current component", phase: omev1beta1.CoordinationPhaseFailed},
		{name: "global paused status has no cursor", phase: omev1beta1.CoordinationPhasePaused, valid: true},
		{name: "global paused status rejects stale cursor", phase: omev1beta1.CoordinationPhasePaused, current: omev1beta1.DecoderComponent},
		{name: "previous without current", phase: omev1beta1.CoordinationPhaseIdle, previous: omev1beta1.DecoderComponent},
		{name: "first component cannot have predecessor", phase: omev1beta1.CoordinationPhaseSurging, current: omev1beta1.DecoderComponent, previous: omev1beta1.EngineComponent},
		{name: "second component requires immediate predecessor", phase: omev1beta1.CoordinationPhaseWaiting, current: omev1beta1.EngineComponent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := sequentialInferenceService()
			status := &isvc.Status.RolloutCoordination.Groups[0]
			status.Phase = tt.phase
			status.CompositePhase = tt.composite
			status.CurrentComponent = tt.current
			status.PreviousComponent = tt.previous

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			if tt.valid {
				assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
					Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
				})
				return
			}
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
				Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
			})
		})
	}
}

func TestProjectSequentialAwaitingCursorIsInProgress(t *testing.T) {
	isvc := sequentialInferenceService()
	status := &isvc.Status.RolloutCoordination.Groups[0]
	status.Phase = omev1beta1.CoordinationPhaseIdle
	status.CompositePhase = omev1beta1.CompositePhaseSequentialAwaiting
	status.CurrentComponent = omev1beta1.EngineComponent
	status.PreviousComponent = omev1beta1.DecoderComponent

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	require.Len(t, got.Content.Groups, 1)
	assert.Equal(t, reportv1alpha1.RolloutPhaseAwaitingNextComponent, got.Content.Groups[0].Phase)
	assert.Equal(t, reportv1alpha1.RuntimeComponentEngine, got.Content.Groups[0].CurrentComponent)
	assert.Equal(t, reportv1alpha1.RuntimeComponentDecoder, got.Content.Groups[0].PreviousComponent)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateInProgress)
}

func TestProjectSequentialAwaitingPreservesControllerReadyTrue(t *testing.T) {
	isvc := sequentialInferenceService()
	status := &isvc.Status.RolloutCoordination.Groups[0]
	status.Phase = omev1beta1.CoordinationPhaseIdle
	status.CompositePhase = omev1beta1.CompositePhaseSequentialAwaiting
	status.CurrentComponent = omev1beta1.EngineComponent
	status.PreviousComponent = omev1beta1.DecoderComponent
	isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutCoordinationReady), &apis.Condition{
		Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionTrue,
	})

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.RolloutConditionTrue, got.Content.Summary.CoordinationReady)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed,
	})
}

func TestProjectRejectsContradictoryComponentDuringSequentialAwaiting(t *testing.T) {
	isvc := sequentialInferenceService()
	status := &isvc.Status.RolloutCoordination.Groups[0]
	status.Phase = omev1beta1.CoordinationPhaseIdle
	status.CompositePhase = omev1beta1.CompositePhaseSequentialAwaiting
	status.CurrentComponent = omev1beta1.EngineComponent
	status.PreviousComponent = omev1beta1.DecoderComponent
	isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{
		RolloutPhase: omev1beta1.RolloutPhaseFailed,
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
		Component: reportv1alpha1.RuntimeComponentEngine,
	})
}

func TestProjectRejectsIdleCursorWithoutExactAwaitingComposite(t *testing.T) {
	isvc := sequentialInferenceService()
	status := &isvc.Status.RolloutCoordination.Groups[0]
	status.Phase = omev1beta1.CoordinationPhaseIdle
	status.CompositePhase = "SECRET_NOT_AWAITING"
	status.CurrentComponent = omev1beta1.EngineComponent
	status.PreviousComponent = omev1beta1.DecoderComponent

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
	})
	encoded, marshalErr := json.Marshal(got)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(encoded), "SECRET_NOT_AWAITING")
}

func TestProjectRejectsAwaitingCompositeWithoutValidCursor(t *testing.T) {
	isvc := sequentialInferenceService()
	status := &isvc.Status.RolloutCoordination.Groups[0]
	status.Phase = omev1beta1.CoordinationPhaseIdle
	status.CompositePhase = omev1beta1.CompositePhaseSequentialAwaiting

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(0),
	})
}

func TestProjectSequentialSettledIdleIsSucceeded(t *testing.T) {
	for _, composite := range []string{"", string(omev1beta1.CoordinationPhaseIdle)} {
		t.Run("composite="+composite, func(t *testing.T) {
			isvc := sequentialInferenceService()
			status := &isvc.Status.RolloutCoordination.Groups[0]
			status.Phase = omev1beta1.CoordinationPhaseIdle
			status.CompositePhase = composite

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)

			require.Len(t, got.Content.Groups, 1)
			assert.Equal(t, reportv1alpha1.RolloutPhaseIdle, got.Content.Groups[0].Phase)
			assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateSucceeded)
		})
	}
}

func TestProjectRejectsActiveCanaryPhaseWhenNoRolloutIsConfigured(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.ObservedGeneration = 7
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {RolloutPhase: omev1beta1.RolloutPhaseCanarying},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code:      reportv1alpha1.RolloutIssueStatusMalformed,
		Component: reportv1alpha1.RuntimeComponentEngine,
	})
}

func TestProjectDoesNotTreatStableResidueAsProvenNotConfigured(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.ObservedGeneration = 7
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {RolloutPhase: omev1beta1.RolloutPhaseStable},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assertStatusDerivedSummary(t, got, reportv1alpha1.RolloutStateUnknown)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code:      reportv1alpha1.RolloutIssueComponentStatusMissing,
		Component: reportv1alpha1.RuntimeComponentEngine,
	})
}

func TestProjectTreatsEveryComponentRolloutFieldAsStatusResidue(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.ComponentStatusSpec)
	}{
		{
			name: "lifecycle",
			mutate: func(status *omev1beta1.ComponentStatusSpec) {
				status.Lifecycle = &omev1beta1.LifecycleStatus{
					CurrentRevision: "chat-engine-aaaaaaaa",
					UpdateRevision:  "chat-engine-aaaaaaaa",
				}
			},
		},
		{
			name: "rollout phase",
			mutate: func(status *omev1beta1.ComponentStatusSpec) {
				status.RolloutPhase = omev1beta1.RolloutPhaseStable
			},
		},
		{
			name: "latest ready revision",
			mutate: func(status *omev1beta1.ComponentStatusSpec) {
				status.LatestReadyRevision = "chat-engine-rev-aaaaaaaa"
			},
		},
		{
			name: "latest rolled out revision",
			mutate: func(status *omev1beta1.ComponentStatusSpec) {
				status.LatestRolledoutRevision = "chat-engine-rev-aaaaaaaa"
			},
		},
		{
			name: "previous rolled out revision",
			mutate: func(status *omev1beta1.ComponentStatusSpec) {
				status.PreviousRolledoutRevision = "chat-engine-rev-aaaaaaaa"
			},
		},
		{
			name: "traffic",
			mutate: func(status *omev1beta1.ComponentStatusSpec) {
				status.Traffic = []omev1beta1.ComponentTrafficTarget{{
					RevisionName: "chat-engine-rev-aaaaaaaa",
					Percent:      100,
				}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := baseInferenceService()
			isvc.Spec.Engine.Annotations = map[string]string{
				constants.DeploymentMode: string(constants.RawDeployment),
			}
			status := omev1beta1.ComponentStatusSpec{}
			tt.mutate(&status)
			isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: status,
			}

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.NotEqual(t, reportv1alpha1.RolloutStateNotConfigured, got.Content.Summary.ReportedState)
			assert.Equal(t, reportv1alpha1.EvidenceReported, got.Content.Summary.Evidence)
			assert.Equal(t, reportv1alpha1.RolloutEpochUnverifiable, got.Content.Summary.Epoch)
		})
	}
}

func TestProjectTreatsCoordinationConditionAsIndependentStatusResidue(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Engine.Annotations = map[string]string{
		constants.DeploymentMode: string(constants.OMENative),
	}
	isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutCoordinationReady), &apis.Condition{
		Type:   apis.ConditionType(omev1beta1.RolloutCoordinationReady),
		Status: corev1.ConditionUnknown,
	})

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assertStatusDerivedSummary(t, got, reportv1alpha1.RolloutStateUnknown)
}

func TestProjectTreatsRunModelEvidenceAsStatusResidue(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{
			name: "active run",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Rollout = &omev1beta1.RolloutStatus{
					ActiveRun: &omev1beta1.RolloutRun{},
				}
			},
		},
		{
			name: "last run",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Rollout = &omev1beta1.RolloutStatus{
					LastRun: &omev1beta1.RolloutRunRecord{},
				}
			},
		},
		{
			name: "resolution",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.Rollout = &omev1beta1.RolloutStatus{
					Groups: []omev1beta1.RolloutGroupResolution{{}},
				}
			},
		},
		{
			name: "plan ready condition",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), &apis.Condition{
					Type:    apis.ConditionType(omev1beta1.RolloutPlanReadyCondition),
					Status:  corev1.ConditionTrue,
					Reason:  omev1beta1.RolloutPlanReasonNoRun,
					Message: "SECRET_RUN_CONDITION",
				})
			},
		},
		{
			name: "plan drift condition",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutPlanDriftCondition), &apis.Condition{
					Type:    apis.ConditionType(omev1beta1.RolloutPlanDriftCondition),
					Status:  corev1.ConditionFalse,
					Reason:  omev1beta1.RolloutPlanDriftReasonInSync,
					Message: "SECRET_RUN_CONDITION",
				})
			},
		},
		{
			name: "retained no-run condition pair",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), &apis.Condition{
					Type:    apis.ConditionType(omev1beta1.RolloutPlanReadyCondition),
					Status:  corev1.ConditionTrue,
					Reason:  omev1beta1.RolloutPlanReasonNoRun,
					Message: "no rollout in progress",
				})
				isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutPlanDriftCondition), &apis.Condition{
					Type:   apis.ConditionType(omev1beta1.RolloutPlanDriftCondition),
					Status: corev1.ConditionFalse,
					Reason: omev1beta1.RolloutPlanDriftReasonInSync,
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := baseInferenceService()
			isvc.Spec.Engine.Annotations = map[string]string{
				constants.DeploymentMode: string(constants.RawDeployment),
			}
			tt.mutate(isvc)

			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assertStatusDerivedSummary(t, got, reportv1alpha1.RolloutStateUnknown)

			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "SECRET_RUN_CONDITION")
		})
	}
}

func TestProjectKeepsEmptyRunModelStatusNotConfigured(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Engine.Annotations = map[string]string{
		constants.DeploymentMode: string(constants.RawDeployment),
	}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutStateNotConfigured, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.RolloutStateNotConfigured, got.Content.Summary.ReportedState)
	assert.Equal(t, reportv1alpha1.EvidenceDeclared, got.Content.Summary.Evidence)
	assert.Equal(t, reportv1alpha1.RolloutEpochNotApplicable, got.Content.Summary.Epoch)
}

func TestProjectDoesNotUseTopLevelObservedGenerationAsRolloutFreshness(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Generation = 8
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
	}}}
	isvc.Status.ObservedGeneration = 7
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {},
	}
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
		Name: "0", Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Policy: omev1beta1.CoordinationPolicyBlueGreen, Phase: omev1beta1.CoordinationPhaseFailed,
	}}}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assertOnlyEpochBoundary(t, got, reportv1alpha1.RolloutStateFailed)
	assert.Equal(t, reportv1alpha1.RolloutPhaseFailed, got.Content.Groups[0].Phase)
}

func TestProjectFailsClosedForMalformedAndSecretBearingFields(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Annotations = map[string]string{"ome.io/rollout-promote": "SECRET_ANNOTATION"}
	isvc.ResourceVersion = "SECRET_RESOURCE_VERSION"
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.EngineComponent},
		Canary:     &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{Capacity: intstr.FromString("not-safe SECRET_CAPACITY"), Traffic: 101}}},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
	}}}
	isvc.Status.ObservedGeneration = 7
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			RolloutPhase: "SECRET_PHASE", LatestReadyRevision: "SECRET_FULL_OBJECT_NAME",
			Traffic: []omev1beta1.ComponentTrafficTarget{{RevisionName: "SECRET_BACKEND", Percent: 200, Tag: "SECRET_TAG"}},
		},
	}
	isvc.Status.Canary = &omev1beta1.CanaryStatus{
		CanaryRevisionHash: "SECRET_HASH", CurrentStep: 9, PromotedThrough: "SECRET_MAILBOX",
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
	assert.NotEmpty(t, got.Content.Issues)
	assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{Code: reportv1alpha1.WarningPartialData})
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{
		"SECRET_ANNOTATION", "SECRET_RESOURCE_VERSION", "SECRET_CAPACITY", "SECRET_PHASE",
		"SECRET_FULL_OBJECT_NAME", "SECRET_BACKEND", "SECRET_TAG", "SECRET_HASH", "SECRET_MAILBOX",
	} {
		assert.NotContains(t, string(encoded), secret)
	}
}

func TestProjectRejectsInvalidSubject(t *testing.T) {
	tests := []struct {
		name string
		isvc *omev1beta1.InferenceService
	}{
		{name: "nil", isvc: nil},
		{name: "empty name", isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod"}}},
		{name: "empty namespace", isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := rolloutprojection.Project(tt.isvc, fixedClock())
			require.Error(t, err)
		})
	}
}

func TestProjectRequiresUIDBeforeEmittingObservedEvidence(t *testing.T) {
	isvc := baseInferenceService()
	isvc.UID = ""

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.ErrorIs(t, err, rolloutprojection.ErrSubjectUIDRequired)
	assert.Empty(t, got.Sources)
}

func TestProjectDoesNotMutateInput(t *testing.T) {
	nonUTC := time.FixedZone("fixture", 5*60*60+30*60)
	canary := activeCanaryInferenceService()
	canary.Annotations = map[string]string{"private": "SECRET_ANNOTATION"}
	canary.Spec.Engine.Annotations = map[string]string{"private": "SECRET_COMPONENT_ANNOTATION"}
	canary.Status.Canary.StepEnteredTime = &metav1.Time{Time: time.Date(2026, 8, 31, 20, 0, 0, 0, nonUTC)}
	canary.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis = validAnalysis("latency")
	setAnalysisResults(canary, []omev1beta1.AnalysisMetricResult{
		exactMetricResult("latency", "1", true),
	})

	sequential := sequentialInferenceService()
	sequential.Annotations = map[string]string{"private": "SECRET_SEQUENTIAL_ANNOTATION"}
	sequential.Spec.Decoder.Annotations = map[string]string{"private": "SECRET_DECODER_ANNOTATION"}
	sequentialStatus := &sequential.Status.RolloutCoordination.Groups[0]
	sequentialStatus.Phase = omev1beta1.CoordinationPhaseIdle
	sequentialStatus.CompositePhase = omev1beta1.CompositePhaseSequentialAwaiting
	sequentialStatus.CurrentComponent = omev1beta1.EngineComponent
	sequentialStatus.PreviousComponent = omev1beta1.DecoderComponent
	sequentialStatus.ObservedSurge = ptrIntOrString(intstr.FromString("25%"))
	sequentialStatus.LastTransitionTime = &metav1.Time{Time: time.Date(2026, 8, 31, 21, 0, 0, 0, nonUTC)}
	sequential.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutCoordinationReady), &apis.Condition{
		Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionTrue,
	})

	for name, isvc := range map[string]*omev1beta1.InferenceService{
		"canary": canary, "sequential": sequential,
	} {
		t.Run(name, func(t *testing.T) {
			before := isvc.DeepCopy()
			_, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			assert.True(t, apiequality.Semantic.DeepEqual(before, isvc))
		})
	}
}

func TestProjectIsDeterministicAcrossTrafficPermutations(t *testing.T) {
	left := activeCanaryInferenceService()
	right := left.DeepCopy()
	component := right.Status.Components[omev1beta1.EngineComponent]
	component.Traffic[0], component.Traffic[1] = component.Traffic[1], component.Traffic[0]
	right.Status.Components[omev1beta1.EngineComponent] = component

	leftReport, err := rolloutprojection.Project(left, fixedClock())
	require.NoError(t, err)
	rightReport, err := rolloutprojection.Project(right, fixedClock())
	require.NoError(t, err)

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		t.Run(string(format), func(t *testing.T) {
			var leftOutput, rightOutput bytes.Buffer
			require.NoError(t, report.Write(&leftOutput, format, leftReport))
			require.NoError(t, report.Write(&rightOutput, format, rightReport))
			assert.Equal(t, leftOutput.Bytes(), rightOutput.Bytes())
		})
	}
}

func baseInferenceService() *omev1beta1.InferenceService {
	mode := constants.OMENative
	return &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "chat", UID: types.UID("isvc-uid"), Generation: 7,
		},
		Spec: omev1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Engine:         &omev1beta1.EngineSpec{},
		},
	}
}

func independentLifecycleInferenceService(current, update string) *omev1beta1.InferenceService {
	isvc := baseInferenceService()
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			Lifecycle: &omev1beta1.LifecycleStatus{
				CurrentRevision: current,
				UpdateRevision:  update,
			},
		},
	}
	return isvc
}

func coordinationBoundInferenceService() *omev1beta1.InferenceService {
	isvc := baseInferenceService()
	isvc.Status.ObservedGeneration = 7
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
	}}}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {},
	}
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
		Name: "0", Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Policy: omev1beta1.CoordinationPolicyBlueGreen, Phase: omev1beta1.CoordinationPhaseIdle,
	}}}
	return isvc
}

func sequentialInferenceService() *omev1beta1.InferenceService {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{
		{Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent}},
		{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}},
	}}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.DecoderComponent: {},
		omev1beta1.EngineComponent:  {},
	}
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
		Name:       "0",
		Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent},
		Order:      []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent},
		Policy:     omev1beta1.CoordinationPolicySequential,
		Phase:      omev1beta1.CoordinationPhaseIdle,
	}}}
	return isvc
}

func completedCanaryInferenceService() *omev1beta1.InferenceService {
	isvc := baseInferenceService()
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
			Capacity: intstr.FromString("100%"), Traffic: 100,
		}}},
	}}}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			RolloutPhase:            omev1beta1.RolloutPhaseStable,
			LatestRolledoutRevision: "chat-engine-rev-bbbbbbbb",
			LatestReadyRevision:     "chat-engine-rev-bbbbbbbb",
			Traffic: []omev1beta1.ComponentTrafficTarget{{
				RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100,
			}},
		},
	}
	isvc.Status.Canary = &omev1beta1.CanaryStatus{
		CanaryRevisionHash: "bbbbbbbb", CurrentStep: 1, ObservedTrafficWeight: 100,
	}
	return isvc
}

func activeCanaryInferenceService() *omev1beta1.InferenceService {
	isvc := baseInferenceService()
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("50%"), Traffic: 50},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			RolloutPhase:            omev1beta1.RolloutPhaseCanarying,
			LatestRolledoutRevision: "chat-engine-rev-aaaaaaaa",
			LatestReadyRevision:     "chat-engine-rev-bbbbbbbb",
			Traffic: []omev1beta1.ComponentTrafficTarget{
				{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 50},
				{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 50},
			},
		},
	}
	isvc.Status.Canary = &omev1beta1.CanaryStatus{
		StableRevisionHash: "aaaaaaaa", CanaryRevisionHash: "bbbbbbbb",
		CurrentStep: 0, ObservedTrafficWeight: 50,
	}
	return isvc
}

func configureRolloutRepin(
	t *testing.T,
	isvc *omev1beta1.InferenceService,
	oldSteps, newSteps []omev1beta1.RolloutGroupStep,
	currentStep, observedTraffic int32,
	phase omev1beta1.RolloutPhase,
	preStepHold bool,
) {
	t.Helper()
	group := omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{
			Steps: append([]omev1beta1.RolloutGroupStep{}, newSteps...),
		},
	}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{group}}
	opened := metav1.NewTime(time.Date(2026, time.August, 31, 18, 10, 0, 0, time.UTC))
	entered := metav1.NewTime(time.Date(2026, time.August, 31, 18, 15, 0, 0, time.UTC))
	pinnedAt := metav1.NewTime(time.Date(2026, time.August, 31, 18, 20, 0, 0, time.UTC))
	isvc.Status.Canary.CurrentStep = currentStep
	isvc.Status.Canary.ObservedTrafficWeight = observedTraffic
	isvc.Status.Canary.PreStepHold = preStepHold
	isvc.Status.Canary.StepEnteredTime = &entered
	component := isvc.Status.Components[omev1beta1.EngineComponent]
	component.RolloutPhase = phase
	component.Traffic = []omev1beta1.ComponentTrafficTarget{{
		RevisionName: "chat-engine-rev-bbbbbbbb", Percent: observedTraffic,
	}}
	if observedTraffic == 0 {
		component.Traffic = nil
	}
	if observedTraffic < 100 {
		component.Traffic = append(component.Traffic, omev1beta1.ComponentTrafficTarget{
			RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100 - observedTraffic,
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
		"bbbbbbbb", oldDigest, opened.Time.UTC().Format(time.RFC3339))
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		RunID: "chat-" + rolloutpolicy.ShortHash([]byte(runIdentity)), OpenedAt: opened, PinnedAt: pinnedAt,
		TargetRevisions: []omev1beta1.RolloutRunTarget{{
			Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb",
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

func refreshRolloutPinnedDigest(t *testing.T, pinned *omev1beta1.RolloutRunGroup) {
	t.Helper()
	digest, err := rolloutpolicy.ProgressionDigest(&pinned.Group)
	require.NoError(t, err)
	pinned.PortableDigest = digest
}

func analysisEvidenceInferenceService() *omev1beta1.InferenceService {
	isvc := activeCanaryInferenceService()
	isvc.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis = validAnalysis("latency")
	setAnalysisResults(isvc, []omev1beta1.AnalysisMetricResult{
		exactMetricResult("latency", "1", true),
	})
	return isvc
}

func setAnalysisResults(
	isvc *omev1beta1.InferenceService,
	results []omev1beta1.AnalysisMetricResult,
) {
	isvc.Status.Canary.MetricResults = append([]omev1beta1.AnalysisMetricResult{}, results...)
	isvc.Status.Canary.AnalysisFailedChecks = 0
	isvc.Status.Canary.LastEvaluationTime = nil
	isvc.Status.Canary.LastConclusiveEvaluationTime = nil
	if len(results) == 0 {
		return
	}
	evaluated := metav1.NewTime(time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC))
	isvc.Status.Canary.LastEvaluationTime = &evaluated
	conclusive := false
	for index := range isvc.Status.Canary.MetricResults {
		isvc.Status.Canary.MetricResults[index].Time = &evaluated
		if isvc.Status.Canary.MetricResults[index].Value != "" {
			conclusive = true
		}
		if isvc.Status.Canary.MetricResults[index].Value != "" &&
			!isvc.Status.Canary.MetricResults[index].Passed {
			isvc.Status.Canary.AnalysisFailedChecks = 1
		}
	}
	if conclusive {
		isvc.Status.Canary.LastConclusiveEvaluationTime = &evaluated
	}
}

func setActiveCanaryTraffic(isvc *omev1beta1.InferenceService, canaryWeight int32) {
	isvc.Status.Canary.ObservedTrafficWeight = canaryWeight
	status := isvc.Status.Components[omev1beta1.EngineComponent]
	status.Traffic = []omev1beta1.ComponentTrafficTarget{
		{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100 - canaryWeight},
		{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: canaryWeight},
	}
	isvc.Status.Components[omev1beta1.EngineComponent] = status
}

func configureRollbackCanary(isvc *omev1beta1.InferenceService, phase omev1beta1.RolloutPhase) {
	isvc.Status.Canary.ObservedTrafficWeight = 0
	isvc.Status.Canary.RolledBackRevisionHash = isvc.Status.Canary.CanaryRevisionHash
	status := isvc.Status.Components[omev1beta1.EngineComponent]
	status.RolloutPhase = phase
	status.Traffic = []omev1beta1.ComponentTrafficTarget{{
		RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100,
	}}
	isvc.Status.Components[omev1beta1.EngineComponent] = status
}

func multiComponentCanaryInferenceService() *omev1beta1.InferenceService {
	isvc := baseInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Spec.Router = &omev1beta1.RouterSpec{}
	isvc.Status.ObservedGeneration = 7
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{
			omev1beta1.EngineComponent, omev1beta1.DecoderComponent, omev1beta1.RouterComponent,
		},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("25%"), Traffic: 10},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}},
	}}}
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent:  {},
		omev1beta1.DecoderComponent: {},
		omev1beta1.RouterComponent: {
			RolloutPhase: omev1beta1.RolloutPhaseCanarying,
			Traffic: []omev1beta1.ComponentTrafficTarget{
				{RevisionName: "chat-router-rev-aaaaaaaa", Percent: 90},
				{RevisionName: "chat-router-rev-bbbbbbbb", Percent: 10},
			},
		},
	}
	isvc.Status.Canary = &omev1beta1.CanaryStatus{
		StableRevisionHash: "aaaaaaaa", CanaryRevisionHash: "bbbbbbbb",
		CurrentStep: 0, ObservedTrafficWeight: 10,
	}
	return isvc
}

func fixedClock() reportv1alpha1.Clock {
	return reportv1alpha1.ClockFunc(func() time.Time {
		return time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)
	})
}

func assertStatusDerivedSummary(
	t *testing.T,
	got reportv1alpha1.RolloutStatusReport,
	reported reportv1alpha1.RolloutState,
) {
	t.Helper()
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
	assert.Equal(t, reported, got.Content.Summary.ReportedState)
	assert.Equal(t, reportv1alpha1.EvidenceReported, got.Content.Summary.Evidence)
	assert.Equal(t, reportv1alpha1.RolloutEpochUnverifiable, got.Content.Summary.Epoch)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueEpochUnverifiable,
	})
	assert.Contains(t, got.Warnings, reportv1alpha1.RolloutWarning{
		Code: reportv1alpha1.WarningPartialData,
	})
}

func assertOnlyEpochBoundary(
	t *testing.T,
	got reportv1alpha1.RolloutStatusReport,
	reported reportv1alpha1.RolloutState,
) {
	t.Helper()
	assertStatusDerivedSummary(t, got, reported)
	assert.Equal(t, []reportv1alpha1.RolloutIssue{{
		Code: reportv1alpha1.RolloutIssueEpochUnverifiable,
	}}, got.Content.Issues)
	assert.Equal(t, []reportv1alpha1.RolloutWarning{{
		Code: reportv1alpha1.WarningPartialData,
	}}, got.Warnings)
}

func ptrTime(value time.Time) *time.Time { return &value }

func ptrInt(value int) *int { return &value }

func ptrIntOrString(value intstr.IntOrString) *intstr.IntOrString { return &value }

func lifecycleWithRollingBudgets(
	maxUnavailable, maxSurge *intstr.IntOrString,
) *omev1beta1.LifecycleSpec {
	return &omev1beta1.LifecycleSpec{UpdateStrategy: &omev1beta1.UpdateStrategy{
		Type: omev1beta1.UpdateStrategySurgeThenDrain,
		RollingUpdate: &omev1beta1.RollingUpdate{
			MaxUnavailable: maxUnavailable,
			MaxSurge:       maxSurge,
		},
	}}
}

func exactMetricResult(name, value string, passed bool) omev1beta1.AnalysisMetricResult {
	return omev1beta1.AnalysisMetricResult{
		Name: name, Value: value, Threshold: "1", Operator: omev1beta1.ComparisonLTE, Passed: passed,
	}
}

func validAnalysis(name string) *omev1beta1.RolloutAnalysis {
	return &omev1beta1.RolloutAnalysis{
		Interval:     metav1.Duration{Duration: time.Minute},
		FailureLimit: 1,
		Metrics: []omev1beta1.AnalysisMetric{{
			Name: name, Query: "safe_query", Operator: omev1beta1.ComparisonLTE, Threshold: "1",
		}},
	}
}
