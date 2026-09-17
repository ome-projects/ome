package rolloutprojection_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func concurrentCanaryInferenceService() *omev1beta1.InferenceService {
	isvc := activeCanaryInferenceService()
	isvc.Spec.Router = &omev1beta1.RouterSpec{}
	engineGroup := isvc.Spec.Rollout.Groups[0]
	ordering := omev1beta1.RolloutGroupOrderingConcurrent
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{
		GroupOrdering: &ordering,
		Groups: []omev1beta1.RolloutGroup{
			{
				Components: []omev1beta1.ComponentType{omev1beta1.RouterComponent},
				Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{
					{Capacity: intstr.FromString("25%"), Traffic: 25},
					{Capacity: intstr.FromString("100%"), Traffic: 100},
				}},
			},
			engineGroup,
		},
	}
	engine := isvc.Status.Components[omev1beta1.EngineComponent]
	engine.Canary = isvc.Status.Canary.DeepCopy()
	isvc.Status.Components[omev1beta1.EngineComponent] = engine
	routerCanary := &omev1beta1.CanaryStatus{
		StableRevisionHash: "cccccccc", CanaryRevisionHash: "dddddddd",
		CurrentStep: 0, ObservedTrafficWeight: 25,
	}
	isvc.Status.Components[omev1beta1.RouterComponent] = omev1beta1.ComponentStatusSpec{
		RolloutPhase:            omev1beta1.RolloutPhaseCanarying,
		LatestRolledoutRevision: "chat-router-rev-cccccccc",
		LatestReadyRevision:     "chat-router-rev-dddddddd",
		Traffic: []omev1beta1.ComponentTrafficTarget{
			{RevisionName: "chat-router-rev-cccccccc", Percent: 75},
			{RevisionName: "chat-router-rev-dddddddd", Percent: 25},
		},
		Canary: routerCanary,
	}
	// The legacy alias is the router's state, not an aggregate of both runs.
	isvc.Status.Canary = routerCanary.DeepCopy()
	return isvc
}

func pinnedConcurrentCanaryInferenceService(t *testing.T) *omev1beta1.InferenceService {
	t.Helper()
	isvc := concurrentCanaryInferenceService()
	opened := metav1.NewTime(time.Date(2026, time.September, 17, 19, 0, 0, 0, time.UTC))
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		RunID: "chat-0123456789ab", OpenedAt: opened, PinnedAt: opened,
		TargetRevisions: []omev1beta1.RolloutRunTarget{
			{Component: omev1beta1.RouterComponent, Revision: "dddddddd"},
			{Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb"},
		},
	}}
	for _, group := range isvc.Spec.Rollout.Groups {
		digest, err := rolloutpolicy.ProgressionDigest(&group)
		require.NoError(t, err)
		isvc.Status.Rollout.ActiveRun.Plan.Groups = append(isvc.Status.Rollout.ActiveRun.Plan.Groups,
			omev1beta1.RolloutRunGroup{
				Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: group,
			})
	}
	return isvc
}

func TestProjectConcurrentCanariesReadEachUnitStatus(t *testing.T) {
	isvc := concurrentCanaryInferenceService()

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Groups, 2)
	assert.Equal(t, reportv1alpha1.RolloutStrategyCanary, got.Content.Groups[0].Strategy)
	assert.Equal(t, "dddddddd", got.Content.Groups[0].TargetRevisionHash)
	require.NotNil(t, got.Content.Groups[0].Step)
	assert.Equal(t, int32(25), got.Content.Groups[0].Step.ObservedTraffic)
	assert.Equal(t, reportv1alpha1.RolloutStrategyCanary, got.Content.Groups[1].Strategy)
	assert.Equal(t, "bbbbbbbb", got.Content.Groups[1].TargetRevisionHash)
	require.NotNil(t, got.Content.Groups[1].Step)
	assert.Equal(t, int32(50), got.Content.Groups[1].Step.ObservedTraffic)
	assert.Equal(t, reportv1alpha1.RolloutStateInProgress, got.Content.Summary.ReportedState)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{Code: reportv1alpha1.RolloutIssueSpecMalformed})
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{Code: reportv1alpha1.RolloutIssueStatusMalformed, Group: ptrInt(1)})
	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatTable, got))
	t.Logf("rendered concurrent-canary rollout status (synthetic fixture):\n%s", output.String())
}

func TestProjectConcurrentCanaryDoesNotBorrowOtherUnitAlias(t *testing.T) {
	isvc := concurrentCanaryInferenceService()
	engine := isvc.Status.Components[omev1beta1.EngineComponent]
	engine.Canary = nil
	isvc.Status.Components[omev1beta1.EngineComponent] = engine

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Groups, 2)
	assert.Equal(t, "", got.Content.Groups[1].TargetRevisionHash)
	assert.Nil(t, got.Content.Groups[1].Step)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueCanaryStatusMissing, Group: ptrInt(1),
	})
}

func TestProjectConcurrentCanariesDoNotGuessLegacyAliasOwner(t *testing.T) {
	isvc := concurrentCanaryInferenceService()
	for _, name := range []omev1beta1.ComponentType{omev1beta1.RouterComponent, omev1beta1.EngineComponent} {
		component := isvc.Status.Components[name]
		component.Canary = nil
		isvc.Status.Components[name] = component
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Groups, 2)
	for index, group := range got.Content.Groups {
		assert.Empty(t, group.TargetRevisionHash)
		assert.Nil(t, group.Step)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
			Code: reportv1alpha1.RolloutIssueCanaryStatusMissing, Group: ptrInt(index),
		})
	}
}

func TestProjectRequiresConcurrentDeclarationForIndependentCanaries(t *testing.T) {
	isvc := concurrentCanaryInferenceService()
	isvc.Spec.Rollout.GroupOrdering = nil

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueSpecMalformed,
	})
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.ReportedState)
}

func TestProjectRejectsPerUnitCanaryWithoutConfiguredGroup(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {Canary: &omev1beta1.CanaryStatus{
			CanaryRevisionHash: "bbbbbbbb",
		}},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueCanaryStatusUnexpected,
	})
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.ReportedState)
}

func TestProjectRejectsPerUnitCanaryOnUnownedComponent(t *testing.T) {
	isvc := concurrentCanaryInferenceService()
	isvc.Status.Components[omev1beta1.DecoderComponent] = omev1beta1.ComponentStatusSpec{
		Canary: &omev1beta1.CanaryStatus{CanaryRevisionHash: "eeeeeeee"},
	}

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code:      reportv1alpha1.RolloutIssueCanaryStatusUnexpected,
		Component: reportv1alpha1.RuntimeComponentDecoder,
	})
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.ReportedState)
}

func TestProjectExplainKeepsConcurrentPinnedCanariesIndependent(t *testing.T) {
	isvc := pinnedConcurrentCanaryInferenceService(t)
	engine := isvc.Status.Components[omev1beta1.EngineComponent]
	engine.Canary.PreStepHold = true
	isvc.Status.Components[omev1beta1.EngineComponent] = engine

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.EffectiveGroups, 2)
	assert.Equal(t, reportv1alpha1.RolloutStrategyCanary, got.Content.EffectiveGroups[0].Strategy)
	assert.Equal(t, reportv1alpha1.RolloutStrategyCanary, got.Content.EffectiveGroups[1].Strategy)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective,
	})
	for _, group := range []int{0, 1} {
		assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
			Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
			View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(group),
		})
	}
	assert.Contains(t, got.Content.Holds, reportv1alpha1.RolloutExplainHold{
		Kind:     reportv1alpha1.RolloutHoldCanaryPreStep,
		Evidence: reportv1alpha1.EvidenceReported, Group: ptrInt(1),
	})
	assert.NotContains(t, got.Content.Holds, reportv1alpha1.RolloutExplainHold{
		Kind:     reportv1alpha1.RolloutHoldCanaryPreStep,
		Evidence: reportv1alpha1.EvidenceReported, Group: ptrInt(0),
	})
}

func TestProjectHistoryRetainsConcurrentPinnedCanaries(t *testing.T) {
	isvc := pinnedConcurrentCanaryInferenceService(t)

	got, err := rolloutprojection.ProjectHistory(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Runs, 1)
	assert.Equal(t, reportv1alpha1.RolloutHistoryRunActive, got.Content.Runs[0].Slot)
	assert.Equal(t, 2, got.Content.Runs[0].GroupCount)
	assert.Equal(t, 1, got.Content.Summary.ActiveRuns)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutHistoryIssue{
		Code: reportv1alpha1.RolloutHistoryIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutHistoryViewActive,
	})
	assert.Equal(t, reportv1alpha1.RolloutEpochUnverifiable, got.Content.Summary.CurrentEpoch)
}

func TestProjectPinnedCanariesDoNotClaimCurrentGenerationWhenLiveOrderDiffers(t *testing.T) {
	isvc := pinnedConcurrentCanaryInferenceService(t)
	isvc.Spec.Rollout.GroupOrdering = nil

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutStateInProgress, got.Content.Summary.ReportedState)
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.RolloutEpochUnverifiable, got.Content.Summary.Epoch)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutIssue{
		Code: reportv1alpha1.RolloutIssueSpecMalformed,
	})

	explained, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	assert.NotContains(t, explained.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective,
	})
}

func TestProjectExplainConcurrentCanaryKeepsIndependentSequentialSoak(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Spec.Router = &omev1beta1.RouterSpec{}
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	ordering := omev1beta1.RolloutGroupOrderingConcurrent
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{
		GroupOrdering: &ordering,
		Groups: []omev1beta1.RolloutGroup{
			{
				Components: []omev1beta1.ComponentType{omev1beta1.RouterComponent},
				Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
					Capacity: intstr.FromString("100%"), Traffic: 100,
				}}},
			},
			{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				BlueGreen:  &omev1beta1.GroupBlueGreen{},
				Soak:       &metav1.Duration{Duration: time.Minute},
			},
			{
				Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
				BlueGreen:  &omev1beta1.GroupBlueGreen{},
			},
		},
	}
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{}}
	for _, group := range isvc.Spec.Rollout.Groups {
		digest, err := rolloutpolicy.ProgressionDigest(&group)
		require.NoError(t, err)
		isvc.Status.Rollout.ActiveRun.Plan.Groups = append(isvc.Status.Rollout.ActiveRun.Plan.Groups,
			omev1beta1.RolloutRunGroup{
				Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: group,
			})
	}

	got, err := rolloutprojection.ProjectExplain(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.EffectiveGroups, 3)
	require.NotNil(t, got.Content.EffectiveGroups[1].Soak)
	assert.Equal(t, reportv1alpha1.RolloutSettingEffectApplied, got.Content.EffectiveGroups[1].Soak.Effect)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.RolloutExplainIssue{
		Code: reportv1alpha1.RolloutExplainIssueActiveRunMalformed,
		View: reportv1alpha1.RolloutPlanViewEffective, Group: ptrInt(1),
	})
}
