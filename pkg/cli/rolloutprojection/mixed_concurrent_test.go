package rolloutprojection_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/validation"
)

func mixedConcurrentInferenceService(canaryIndex int) *omev1beta1.InferenceService {
	isvc := concurrentCanaryInferenceService()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	canary := isvc.Spec.Rollout.Groups[0]
	blueGreen := []omev1beta1.RolloutGroup{
		{Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent}, BlueGreen: &omev1beta1.GroupBlueGreen{}},
		{Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent}, BlueGreen: &omev1beta1.GroupBlueGreen{}},
	}
	groups := make([]omev1beta1.RolloutGroup, 0, 3)
	for i := 0; i < 3; i++ {
		if i == canaryIndex {
			groups = append(groups, canary)
		} else {
			groups = append(groups, blueGreen[0])
			blueGreen = blueGreen[1:]
		}
	}
	ordering := omev1beta1.RolloutGroupOrderingConcurrent
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{GroupOrdering: &ordering, Groups: groups}
	isvc.Status.Components[omev1beta1.EngineComponent] = omev1beta1.ComponentStatusSpec{RolloutPhase: omev1beta1.RolloutPhaseStable}
	isvc.Status.Components[omev1beta1.DecoderComponent] = omev1beta1.ComponentStatusSpec{RolloutPhase: omev1beta1.RolloutPhaseStable}
	isvc.Status.RolloutCoordination = &omev1beta1.RolloutCoordinationStatus{Groups: []omev1beta1.RolloutCoordinationGroupStatus{{
		Name: "0", Policy: omev1beta1.CoordinationPolicySequential,
		Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent},
		Order:      []omev1beta1.ComponentType{omev1beta1.DecoderComponent, omev1beta1.EngineComponent},
		Phase:      omev1beta1.CoordinationPhaseIdle,
	}}}
	isvc.Status.SetCondition(apis.ConditionType(omev1beta1.RolloutCoordinationReady), &apis.Condition{
		Type: apis.ConditionType(omev1beta1.RolloutCoordinationReady), Status: corev1.ConditionTrue,
	})
	return isvc
}

func TestProjectMixedConcurrentCanaryAndCollapsedBlueGreen(t *testing.T) {
	for _, canaryIndex := range []int{0, 1, 2} {
		t.Run(string(rune('0'+canaryIndex)), func(t *testing.T) {
			isvc := mixedConcurrentInferenceService(canaryIndex)
			if canaryIndex == 2 {
				// An omitted progression defaults to blue-green in the controller.
				isvc.Spec.Rollout.Groups[0].BlueGreen = nil
			}
			require.NoError(t, validation.ValidateCoordination(&isvc.Spec))
			require.NoError(t, validation.ValidateCanary(&isvc.Spec))
			require.NoError(t, validation.ValidateRolloutOrderingEnforced(&isvc.Spec))
			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			require.Len(t, got.Content.Groups, 2)
			firstBlueGreen := 0
			if canaryIndex == 0 {
				firstBlueGreen = 1
			}
			assert.Contains(t, got.Content.Groups, reportv1alpha1.RolloutGroupStatus{
				Index: firstBlueGreen, Strategy: reportv1alpha1.RolloutStrategySequential,
				Phase: reportv1alpha1.RolloutPhaseIdle,
				Components: []reportv1alpha1.RuntimeComponentType{
					reportv1alpha1.RuntimeComponentDecoder, reportv1alpha1.RuntimeComponentEngine,
				},
			})
			var canary *reportv1alpha1.RolloutGroupStatus
			for i := range got.Content.Groups {
				if got.Content.Groups[i].Index == canaryIndex {
					canary = &got.Content.Groups[i]
				}
			}
			require.NotNil(t, canary)
			assert.Equal(t, reportv1alpha1.RolloutStrategyCanary, canary.Strategy)
			assert.Equal(t, reportv1alpha1.RolloutPhaseCanarying, canary.Phase)
			for _, component := range got.Content.Components {
				require.NotNil(t, component.Group)
				if component.Type == reportv1alpha1.RuntimeComponentRouter {
					assert.Equal(t, canaryIndex, *component.Group)
				} else {
					assert.Equal(t, firstBlueGreen, *component.Group)
				}
			}
			for _, issue := range got.Content.Issues {
				assert.Equal(t, reportv1alpha1.RolloutIssueEpochUnverifiable, issue.Code, "unexpected issue: %+v", issue)
			}
			assert.Equal(t, reportv1alpha1.RolloutStateInProgress, got.Content.Summary.ReportedState)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
			assert.Equal(t, reportv1alpha1.RolloutConditionTrue, got.Content.Summary.CoordinationReady)
			if canaryIndex == 0 {
				var output bytes.Buffer
				require.NoError(t, report.Write(&output, report.FormatTable, got))
				t.Logf("mixed concurrent rollout status (synthetic fixture):\n%s", output.String())
			}
		})
	}
}

func TestProjectMixedConcurrentPinnedPlanUsesCollapsedBlueGreenStatus(t *testing.T) {
	isvc := mixedConcurrentInferenceService(0)
	pinnedAt := metav1.NewTime(time.Date(2026, time.August, 31, 17, 0, 0, 0, time.UTC))
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		RunID: "chat-0123456789ab", OpenedAt: pinnedAt, PinnedAt: pinnedAt,
		TargetRevisions: []omev1beta1.RolloutRunTarget{
			{Component: omev1beta1.RouterComponent, Revision: "dddddddd", StableRevision: "cccccccc"},
			{Component: omev1beta1.DecoderComponent, Revision: "aaaaaaaa", StableRevision: "aaaaaaaa"},
			{Component: omev1beta1.EngineComponent, Revision: "bbbbbbbb", StableRevision: "bbbbbbbb"},
		},
	}}
	for _, group := range isvc.Spec.Rollout.Groups {
		digest, err := rolloutpolicy.ProgressionDigest(&group)
		require.NoError(t, err)
		isvc.Status.Rollout.ActiveRun.Plan.Groups = append(isvc.Status.Rollout.ActiveRun.Plan.Groups,
			omev1beta1.RolloutRunGroup{Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: group})
	}
	// The live declaration may change while the pinned run still owns status.
	isvc.Spec.Rollout.Groups[1], isvc.Spec.Rollout.Groups[2] = isvc.Spec.Rollout.Groups[2], isvc.Spec.Rollout.Groups[1]

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Groups, 2)
	assert.Equal(t, reportv1alpha1.RolloutStrategyCanary, got.Content.Groups[0].Strategy)
	assert.Equal(t, reportv1alpha1.RolloutStrategySequential, got.Content.Groups[1].Strategy)
	assert.Equal(t, []reportv1alpha1.RuntimeComponentType{
		reportv1alpha1.RuntimeComponentDecoder, reportv1alpha1.RuntimeComponentEngine,
	}, got.Content.Groups[1].Components)
	for _, issue := range got.Content.Issues {
		assert.Equal(t, reportv1alpha1.RolloutIssueEpochUnverifiable, issue.Code, "unexpected issue: %+v", issue)
	}
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.RolloutEpochUnverifiable, got.Content.Summary.Epoch)
}

func TestProjectMixedConcurrentSequentialSoakDoesNotHideCanary(t *testing.T) {
	isvc := mixedConcurrentInferenceService(1)
	isvc.Spec.Rollout.Groups[0].Soak = &metav1.Duration{Duration: time.Minute}
	coordination := &isvc.Status.RolloutCoordination.Groups[0]
	coordination.CompositePhase = omev1beta1.CompositePhaseSequentialAwaiting
	coordination.CurrentComponent = omev1beta1.EngineComponent
	coordination.PreviousComponent = omev1beta1.DecoderComponent

	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Groups, 2)
	assert.Equal(t, reportv1alpha1.RolloutPhaseAwaitingNextComponent, got.Content.Groups[0].Phase)
	assert.Equal(t, reportv1alpha1.RolloutPhaseCanarying, got.Content.Groups[1].Phase)
	assert.Equal(t, reportv1alpha1.RolloutConditionTrue, got.Content.Summary.CoordinationReady)
	assert.Equal(t, reportv1alpha1.RolloutStateInProgress, got.Content.Summary.ReportedState)
	for _, issue := range got.Content.Issues {
		assert.Equal(t, reportv1alpha1.RolloutIssueEpochUnverifiable, issue.Code, "unexpected issue: %+v", issue)
	}
}

func TestProjectMixedConcurrentRejectsMalformedCollapsedStatus(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutate   func(*omev1beta1.InferenceService)
		wantCode reportv1alpha1.RolloutIssueCode
	}{
		{
			name: "wrong policy",
			mutate: func(v *omev1beta1.InferenceService) {
				v.Status.RolloutCoordination.Groups[0].Policy = omev1beta1.CoordinationPolicyBlueGreen
			},
			wantCode: reportv1alpha1.RolloutIssueStatusMalformed,
		},
		{
			name: "old order",
			mutate: func(v *omev1beta1.InferenceService) {
				v.Status.RolloutCoordination.Groups[0].Order = []omev1beta1.ComponentType{
					omev1beta1.EngineComponent, omev1beta1.DecoderComponent,
				}
			},
			wantCode: reportv1alpha1.RolloutIssueStatusMalformed,
		},
		{
			name: "wrong components",
			mutate: func(v *omev1beta1.InferenceService) {
				v.Status.RolloutCoordination.Groups[0].Components = []omev1beta1.ComponentType{
					omev1beta1.DecoderComponent, omev1beta1.RouterComponent,
				}
			},
			wantCode: reportv1alpha1.RolloutIssueStatusMalformed,
		},
		{
			name: "duplicate collapsed group",
			mutate: func(v *omev1beta1.InferenceService) {
				extra := *v.Status.RolloutCoordination.Groups[0].DeepCopy()
				v.Status.RolloutCoordination.Groups = append(v.Status.RolloutCoordination.Groups, extra)
			},
			wantCode: reportv1alpha1.RolloutIssueStatusMalformed,
		},
		{
			name: "stale individual group",
			mutate: func(v *omev1beta1.InferenceService) {
				extra := *v.Status.RolloutCoordination.Groups[0].DeepCopy()
				extra.Name = "2"
				v.Status.RolloutCoordination.Groups = append(v.Status.RolloutCoordination.Groups, extra)
			},
			wantCode: reportv1alpha1.RolloutIssueGroupStatusUnexpected,
		},
		{
			name: "missing collapsed group",
			mutate: func(v *omev1beta1.InferenceService) {
				v.Status.RolloutCoordination.Groups = nil
			},
			wantCode: reportv1alpha1.RolloutIssueGroupStatusMissing,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isvc := mixedConcurrentInferenceService(0)
			tc.mutate(isvc)
			got, err := rolloutprojection.Project(isvc, fixedClock())
			require.NoError(t, err)
			var found bool
			for _, issue := range got.Content.Issues {
				found = found || issue.Code == tc.wantCode
			}
			assert.True(t, found, "missing %s in %+v", tc.wantCode, got.Content.Issues)
			assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
		})
	}
}

func TestProjectMixedConcurrentDoesNotCollapseDuplicateOwner(t *testing.T) {
	isvc := mixedConcurrentInferenceService(0)
	isvc.Spec.Rollout.Groups[1].Components = []omev1beta1.ComponentType{omev1beta1.RouterComponent}
	got, err := rolloutprojection.Project(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Groups, 3)
	var found bool
	for _, issue := range got.Content.Issues {
		found = found || issue.Code == reportv1alpha1.RolloutIssueSpecMalformed
	}
	assert.True(t, found, "overlapping groups must fail closed: %+v", got.Content.Issues)
	assert.Equal(t, reportv1alpha1.RolloutStateUnknown, got.Content.Summary.State)
}
