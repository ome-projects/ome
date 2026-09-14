package rolloutprojection_test

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
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

var validationNow = time.Date(2026, time.September, 15, 1, 2, 3, 0, time.UTC)

func TestProjectValidationRejectsNilInput(t *testing.T) {
	_, err := rolloutprojection.ProjectValidation(nil, reportv1alpha1.ClockFunc(func() time.Time { return validationNow }))
	require.ErrorIs(t, err, rolloutprojection.ErrNilInferenceService)
}

func TestProjectValidationRequiresBoundIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
		want   error
	}{
		{name: "name", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Name = "" }, want: rolloutprojection.ErrSubjectNameRequired},
		{name: "namespace", mutate: func(isvc *omev1beta1.InferenceService) { isvc.Namespace = "" }, want: rolloutprojection.ErrNamespaceRequired},
		{name: "uid", mutate: func(isvc *omev1beta1.InferenceService) { isvc.UID = "" }, want: rolloutprojection.ErrSubjectUIDRequired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := baseValidationISVC()
			tt.mutate(isvc)
			_, err := projectValidation(isvc)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestProjectValidationNoConfiguredFeaturesIsValid(t *testing.T) {
	isvc := baseValidationISVC()

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationValid, got.Content.Summary.State)
	assert.Equal(t, []reportv1alpha1.RolloutValidationCheck{
		validationCheck(reportv1alpha1.RolloutValidationCheckRolloutReferences, "", reportv1alpha1.RolloutValidationResultValid, reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessCurrent),
		validationCheck(reportv1alpha1.RolloutValidationCheckRolloutPlan, "", reportv1alpha1.RolloutValidationResultValid, reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessCurrent),
		validationCheck(reportv1alpha1.RolloutValidationCheckRolloutOrdering, "", reportv1alpha1.RolloutValidationResultValid, reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessCurrent),
		validationCheck(reportv1alpha1.RolloutValidationCheckRolloutResolution, "", reportv1alpha1.RolloutValidationResultNotApplicable, reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessNotApplicable),
		validationCheck(reportv1alpha1.RolloutValidationCheckTrafficSpec, "", reportv1alpha1.RolloutValidationResultValid, reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessCurrent),
		validationCheck(reportv1alpha1.RolloutValidationCheckTrafficReadiness, "", reportv1alpha1.RolloutValidationResultNotApplicable, reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessNotApplicable),
		validationCheck(reportv1alpha1.RolloutValidationCheckScalingPolicy, "", reportv1alpha1.RolloutValidationResultValid, reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessCurrent),
		validationCheck(reportv1alpha1.RolloutValidationCheckAutoscalerSpec, "", reportv1alpha1.RolloutValidationResultValid, reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessCurrent),
		validationCheck(reportv1alpha1.RolloutValidationCheckAutoscalerSpec, reportv1alpha1.RuntimeComponentEngine, reportv1alpha1.RolloutValidationResultValid, reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessCurrent),
	}, got.Content.Checks)
	assert.Empty(t, got.Content.Issues)
	require.Len(t, got.Sources, 1)
	assert.Equal(t, reportv1alpha1.RolloutSourceReference{
		Kind: reportv1alpha1.RolloutSourceInferenceService, Namespace: "prod", Name: "chat",
		UID: "uid-chat", Generation: 7, Evidence: reportv1alpha1.EvidenceObserved,
		CollectedAt: validationNow,
	}, got.Sources[0])
}

func TestProjectValidationProjectsObservablePrerequisitesWithoutOverclaimingRolloutFreshness(t *testing.T) {
	isvc := configuredValidationISVC()
	setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
	setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationUnverifiable, got.Content.Summary.State)
	for _, check := range got.Content.Checks {
		assert.NotEqual(t, reportv1alpha1.RolloutValidationResultInvalid, check.Result, check)
	}
	assert.Equal(t,
		validationCheck(reportv1alpha1.RolloutValidationCheckRolloutResolution, "", reportv1alpha1.RolloutValidationResultUnverifiable, reportv1alpha1.EvidenceReported, reportv1alpha1.RolloutValidationFreshnessUnverifiable),
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, ""),
	)
	assert.Contains(t, validationIssueCodes(got),
		reportv1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable)
	assert.Equal(t,
		validationCheck(reportv1alpha1.RolloutValidationCheckTrafficReadiness, "", reportv1alpha1.RolloutValidationResultValid, reportv1alpha1.EvidenceReported, reportv1alpha1.RolloutValidationFreshnessCurrent),
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficReadiness, ""),
	)
}

func TestProjectValidationDoesNotClaimUnobservedExternalRolloutPrerequisites(t *testing.T) {
	t.Run("inactive policy body", func(t *testing.T) {
		isvc := configuredValidationISVC()
		isvc.Spec.Rollout.Groups[0] = omev1beta1.RolloutGroup{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "blue-green-policy", Progression: omev1beta1.RolloutProgressionBlueGreen,
			},
		}
		setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
		setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
		assert.Contains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueRolloutPrerequisiteUnverifiable)
	})
	t.Run("analysis data source", func(t *testing.T) {
		isvc := configuredValidationISVC()
		isvc.Spec.Rollout.Groups[0] = omev1beta1.RolloutGroup{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
				Capacity: intstr.FromString("100%"), Traffic: 100,
				Analysis: &omev1beta1.RolloutAnalysis{
					Interval: metav1.Duration{Duration: time.Minute}, FailureLimit: 1,
					Metrics: []omev1beta1.AnalysisMetric{{
						Name: "errors", Query: "rate(errors[1m])",
						Operator: omev1beta1.ComparisonLTE, Threshold: "1",
					}},
				},
			}}},
		}
		setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
		setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
		assert.Contains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueRolloutPrerequisiteUnverifiable)
	})
}

func TestProjectValidationAcceptsCanonicalPinnedPlans(t *testing.T) {
	tests := []struct {
		name  string
		group omev1beta1.RolloutRunGroup
	}{
		{
			name: "inline",
			group: omev1beta1.RolloutRunGroup{
				Source: omev1beta1.RolloutPlanSourceInline,
				Group: omev1beta1.RolloutGroup{
					Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
					BlueGreen:  &omev1beta1.GroupBlueGreen{},
				},
			},
		},
		{
			name: "policy",
			group: omev1beta1.RolloutRunGroup{
				Source: omev1beta1.RolloutPlanSourcePolicy,
				PolicyRef: &omev1beta1.RolloutPolicyRef{
					Name: "blue-green-policy", Progression: omev1beta1.RolloutProgressionBlueGreen,
				},
				PolicyGeneration: 4,
				Group: omev1beta1.RolloutGroup{
					Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
					BlueGreen:  &omev1beta1.GroupBlueGreen{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := validationISVCWithPinnedRun(t, []omev1beta1.RolloutGroup{tt.group.Group}, true)
			pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
			pinned.Source = tt.group.Source
			pinned.PolicyRef = tt.group.PolicyRef
			pinned.PolicyGeneration = tt.group.PolicyGeneration
			isvc.Spec.Traffic = configuredValidationISVC().Spec.Traffic
			setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)

			got, err := projectValidation(isvc)
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
				findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
			assert.Contains(t, validationIssueCodes(got),
				reportv1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable)
		})
	}
}

func TestProjectValidationAcceptsMatchingActivePolicyResolution(t *testing.T) {
	ref := &omev1beta1.RolloutPolicyRef{
		Name: "blue-green-policy", Progression: omev1beta1.RolloutProgressionBlueGreen,
	}
	isvc := validationISVCWithPinnedRun(t, []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
	}}, false)
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		PolicyRef:  ref.DeepCopy(),
	}}}
	pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
	pinned.Source = omev1beta1.RolloutPlanSourcePolicy
	pinned.PolicyRef = ref.DeepCopy()
	pinned.PolicyGeneration = 4

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
	assert.NotContains(t, validationIssueCodes(got),
		reportv1alpha1.RolloutValidationIssueRolloutPrerequisiteUnverifiable)
	assert.Contains(t, validationIssueCodes(got),
		reportv1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable)

	isvc.Spec.Rollout.Groups[0].PolicyRef.Name = "different-policy"
	got, err = projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
	assert.Contains(t, validationIssueCodes(got),
		reportv1alpha1.RolloutValidationIssueRolloutPrerequisiteUnverifiable)
}

func TestProjectValidationHonorsPinnedRunAfterLiveGroupsAreRemoved(t *testing.T) {
	isvc := validationISVCWithPinnedRun(t, []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
	}}, false)

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
	assert.Contains(t, validationIssueCodes(got),
		reportv1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable)
}

func TestProjectValidationRejectsPinnedRunContractMismatches(t *testing.T) {
	blueGreenGroups := []omev1beta1.RolloutGroup{
		{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			BlueGreen:  &omev1beta1.GroupBlueGreen{},
		},
		{
			Components: []omev1beta1.ComponentType{omev1beta1.DecoderComponent},
			BlueGreen:  &omev1beta1.GroupBlueGreen{},
		},
	}
	tests := []struct {
		name   string
		mutate func(*omev1beta1.RolloutRun)
	}{
		{name: "empty run identity", mutate: func(run *omev1beta1.RolloutRun) { run.RunID = "" }},
		{name: "foreign run identity", mutate: func(run *omev1beta1.RolloutRun) { run.RunID = "other-0123456789ab" }},
		{name: "empty opened time", mutate: func(run *omev1beta1.RolloutRun) { run.OpenedAt = metav1.Time{} }},
		{name: "empty pinned time", mutate: func(run *omev1beta1.RolloutRun) { run.PinnedAt = metav1.Time{} }},
		{name: "pinned before opened", mutate: func(run *omev1beta1.RolloutRun) {
			run.PinnedAt = metav1.NewTime(run.OpenedAt.Add(-time.Second))
		}},
		{name: "empty targets", mutate: func(run *omev1beta1.RolloutRun) { run.TargetRevisions = nil }},
		{name: "empty target revision", mutate: func(run *omev1beta1.RolloutRun) {
			run.TargetRevisions[0].Revision = ""
		}},
		{name: "duplicate target", mutate: func(run *omev1beta1.RolloutRun) {
			run.TargetRevisions = append(run.TargetRevisions, run.TargetRevisions[0])
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := validationISVCWithPinnedRun(t, blueGreenGroups, true)
			tt.mutate(isvc.Status.Rollout.ActiveRun)

			got, err := projectValidation(isvc)
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
				findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
			assert.Contains(t, validationIssueCodes(got),
				reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
		})
	}
}

func TestProjectValidationAcceptsRepinnedComponentOrder(t *testing.T) {
	groups := []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{
			omev1beta1.EngineComponent, omev1beta1.DecoderComponent,
		},
		BlueGreen: &omev1beta1.GroupBlueGreen{},
	}}
	isvc := validationISVCWithPinnedRun(t, groups, true)
	pinned := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
	pinned.Group.Components[0], pinned.Group.Components[1] =
		pinned.Group.Components[1], pinned.Group.Components[0]
	digest, err := rolloutpolicy.ProgressionDigest(&pinned.Group)
	require.NoError(t, err)
	pinned.PortableDigest = digest

	got, err := projectValidation(isvc)

	require.NoError(t, err)
	assert.NotContains(t, validationIssueCodes(got),
		reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
	assert.Contains(t, validationIssueCodes(got),
		reportv1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable)
}

func TestProjectValidationRejectsPinnedDigestMismatchAfterLiveGroupsAreRemoved(t *testing.T) {
	isvc := validationISVCWithPinnedRun(t, []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
	}}, false)
	group := &isvc.Status.Rollout.ActiveRun.Plan.Groups[0]
	require.NotEqual(t, "rp1:000000000000", group.PortableDigest)
	group.PortableDigest = "rp1:000000000000"

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
	assert.Contains(t, validationIssueCodes(got),
		reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
}

func TestProjectValidationAppliesStoredShapeBoundsToPinnedPlans(t *testing.T) {
	tooManySteps := make([]omev1beta1.RolloutGroupStep, 21)
	for i := range tooManySteps {
		tooManySteps[i] = omev1beta1.RolloutGroupStep{
			Capacity: intstr.FromString("100%"), Traffic: 100,
		}
	}
	tooManyMetrics := make([]omev1beta1.AnalysisMetric, 11)
	for i := range tooManyMetrics {
		tooManyMetrics[i] = omev1beta1.AnalysisMetric{
			Name: "metric-" + strconv.Itoa(i), Query: "rate(requests[1m])",
			Operator: omev1beta1.ComparisonLTE, Threshold: "1",
		}
	}
	blueGreen := func(component omev1beta1.ComponentType) omev1beta1.RolloutGroup {
		return omev1beta1.RolloutGroup{
			Components: []omev1beta1.ComponentType{component},
			BlueGreen:  &omev1beta1.GroupBlueGreen{},
		}
	}
	tests := []struct {
		name   string
		groups []omev1beta1.RolloutGroup
	}{
		{
			name: "empty components and targets",
			groups: []omev1beta1.RolloutGroup{{
				BlueGreen: &omev1beta1.GroupBlueGreen{},
			}},
		},
		{
			name: "more than three groups",
			groups: []omev1beta1.RolloutGroup{
				blueGreen(omev1beta1.EngineComponent),
				blueGreen(omev1beta1.DecoderComponent),
				blueGreen(omev1beta1.RouterComponent),
				blueGreen(omev1beta1.EngineComponent),
			},
		},
		{
			name: "more than twenty canary steps",
			groups: []omev1beta1.RolloutGroup{{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Canary:     &omev1beta1.GroupCanary{Steps: tooManySteps},
			}},
		},
		{
			name: "more than ten analysis metrics",
			groups: []omev1beta1.RolloutGroup{{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
					Capacity: intstr.FromString("100%"), Traffic: 100,
					Analysis: &omev1beta1.RolloutAnalysis{
						Interval: metav1.Duration{Duration: time.Minute}, FailureLimit: 1,
						Metrics: tooManyMetrics,
					},
				}}},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := validationISVCWithPinnedRun(t, tt.groups, false)

			got, err := projectValidation(isvc)
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
				findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
			assert.Contains(t, validationIssueCodes(got),
				reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
		})
	}
}

func TestProjectValidationHonorsPinnedStoredShapeBoundaries(t *testing.T) {
	stepsAtCap := make([]omev1beta1.RolloutGroupStep, 20)
	for i := range stepsAtCap {
		stepsAtCap[i] = omev1beta1.RolloutGroupStep{
			Capacity: intstr.FromString("100%"), Traffic: 100,
		}
	}
	metricsAtCap := make([]omev1beta1.AnalysisMetric, 10)
	for i := range metricsAtCap {
		metricsAtCap[i] = omev1beta1.AnalysisMetric{
			Name: "metric-" + strconv.Itoa(i), Query: "rate(requests[1m])",
			Operator: omev1beta1.ComparisonLTE, Threshold: "1",
		}
	}
	blueGreen := func(component omev1beta1.ComponentType) omev1beta1.RolloutGroup {
		return omev1beta1.RolloutGroup{
			Components: []omev1beta1.ComponentType{component},
			BlueGreen:  &omev1beta1.GroupBlueGreen{},
		}
	}
	tests := []struct {
		name       string
		groups     []omev1beta1.RolloutGroup
		wantResult reportv1alpha1.RolloutValidationResult
	}{
		{
			name: "three groups",
			groups: []omev1beta1.RolloutGroup{
				blueGreen(omev1beta1.EngineComponent),
				blueGreen(omev1beta1.DecoderComponent),
				blueGreen(omev1beta1.RouterComponent),
			},
			wantResult: reportv1alpha1.RolloutValidationResultUnverifiable,
		},
		{
			name: "twenty canary steps",
			groups: []omev1beta1.RolloutGroup{{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Canary:     &omev1beta1.GroupCanary{Steps: stepsAtCap},
			}},
			wantResult: reportv1alpha1.RolloutValidationResultUnverifiable,
		},
		{
			name: "ten analysis metrics",
			groups: []omev1beta1.RolloutGroup{{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
					Capacity: intstr.FromString("100%"), Traffic: 100,
					Analysis: &omev1beta1.RolloutAnalysis{
						Interval: metav1.Duration{Duration: time.Minute}, FailureLimit: 1,
						Metrics: metricsAtCap,
					},
				}}},
			}},
			wantResult: reportv1alpha1.RolloutValidationResultUnverifiable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := validationISVCWithPinnedRun(t, tt.groups, false)

			got, err := projectValidation(isvc)
			require.NoError(t, err)
			assert.Equal(t, tt.wantResult,
				findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
			assert.NotContains(t, validationIssueCodes(got),
				reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
		})
	}
}

func TestProjectValidationCoversEveryDeclaredComponent(t *testing.T) {
	isvc := baseValidationISVC()
	isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
	isvc.Spec.Router = &omev1beta1.RouterSpec{}

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	for _, component := range []reportv1alpha1.RuntimeComponentType{
		reportv1alpha1.RuntimeComponentEngine,
		reportv1alpha1.RuntimeComponentDecoder,
		reportv1alpha1.RuntimeComponentRouter,
	} {
		assert.Equal(t, reportv1alpha1.RolloutValidationResultValid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerSpec, component).Result)
	}
}

func TestProjectValidationUsesCanonicalStaticValidators(t *testing.T) {
	isvc := configuredValidationISVC()
	proportional := omev1beta1.ScalingProportional
	isvc.Spec.ScalingPolicy.Mode = proportional
	isvc.Spec.Engine.Autoscaler = &omev1beta1.ComponentAutoscaler{Class: omev1beta1.AutoscalerKEDA}
	consistentHash := omev1beta1.LoadBalancingTypeConsistentHash
	isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &consistentHash}
	isvc.Annotations[constants.RetryAttemptsAnnotation] = "SECRET_NOT_AN_INTEGER"
	isvc.Spec.Rollout.Groups[0] = omev1beta1.RolloutGroup{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		Canary: &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
			Capacity: intstr.FromString("100%"), Traffic: 50,
		}}},
	}

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationInvalid, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutPlan, "").Result)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficSpec, "").Result)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckScalingPolicy, "").Result)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerSpec, reportv1alpha1.RuntimeComponentEngine).Result)
	assert.ElementsMatch(t, []reportv1alpha1.RolloutValidationIssueCode{
		reportv1alpha1.RolloutValidationIssueRolloutPlanInvalid,
		reportv1alpha1.RolloutValidationIssueTrafficSpecInvalid,
		reportv1alpha1.RolloutValidationIssueTrafficAnnotationInvalid,
		reportv1alpha1.RolloutValidationIssueScalingPolicyInvalid,
		reportv1alpha1.RolloutValidationIssueAutoscalerSpecInvalid,
		reportv1alpha1.RolloutValidationIssueRolloutResolutionMissing,
		reportv1alpha1.RolloutValidationIssueTrafficEvidenceMissing,
	}, validationIssueCodes(got))
}

func TestProjectValidationCoversLifecycleReplicaBoundsAndScaleToZero(t *testing.T) {
	t.Run("lifecycle rollout budget", func(t *testing.T) {
		isvc := baseValidationISVC()
		negative := int32(-1)
		isvc.Spec.Engine.Lifecycle = &omev1beta1.LifecycleSpec{MinReadySeconds: &negative}
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationInvalid, got.Content.Summary.State)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutPlan, "").Result)
	})
	t.Run("replica bounds", func(t *testing.T) {
		isvc := baseValidationISVC()
		negative := -1
		isvc.Spec.Engine.MinReplicas = &negative
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerSpec, reportv1alpha1.RuntimeComponentEngine).Result)
	})
	t.Run("scale to zero", func(t *testing.T) {
		isvc := baseValidationISVC()
		zero := 0
		isvc.Spec.Engine.MinReplicas = &zero
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerSpec, "").Result)
	})
}

func TestProjectValidationRejectsCRDOnlyStoredRolloutShapes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*omev1beta1.InferenceService)
	}{
		{
			name: "multiple progression arms",
			mutate: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.Rollout.Groups[0].Canary = &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{
					Capacity: intstr.FromString("100%"), Traffic: 100,
				}}}
			},
		},
		{
			name: "too many groups",
			mutate: func(isvc *omev1beta1.InferenceService) {
				group := isvc.Spec.Rollout.Groups[0]
				isvc.Spec.Rollout.Groups = []omev1beta1.RolloutGroup{group, group, group, group}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := configuredValidationISVC()
			tt.mutate(isvc)
			got, err := projectValidation(isvc)
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
				findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutPlan, "").Result)
			assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueRolloutPlanInvalid)
		})
	}
}

func TestProjectValidationRejectsStoredUnsupportedOrderingAndReferenceShapes(t *testing.T) {
	isvc := configuredValidationISVC()
	isvc.Spec.Rollout.Groups[0].Order = []omev1beta1.ComponentType{omev1beta1.EngineComponent}
	isvc.Spec.Rollout.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{
		Kind: "ClusterRolloutPolicy", Name: "SECRET_POLICY", Progression: omev1beta1.RolloutProgressionBlueGreen,
	}
	isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Kind: "ClusterAutoscalerPolicy", Name: "SECRET_AUTOSCALER"}

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationInvalid, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutReferences, "").Result)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutOrdering, "").Result)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueAutoscalerReferenceInvalid)

	var output bytes.Buffer
	require.NoError(t, report.Write(&output, report.FormatJSON, got))
	assert.NotContains(t, output.String(), "SECRET_POLICY")
	assert.NotContains(t, output.String(), "SECRET_AUTOSCALER")
}

func TestProjectValidationRejectsImpossiblePolicyReferenceNames(t *testing.T) {
	t.Run("rollout policy", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
			Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
			PolicyRef: &omev1beta1.RolloutPolicyRef{
				Name: "not_a_dns_name", Progression: omev1beta1.RolloutProgressionBlueGreen,
			},
		}}}

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutReferences, "").Result)
		assert.Contains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueRolloutReferenceInvalid)
	})

	t.Run("autoscaler policy", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "not_a_dns_name"}

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerSpec,
				reportv1alpha1.RuntimeComponentEngine).Result)
		assert.Contains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueAutoscalerReferenceInvalid)
	})
}

func TestProjectValidationDoesNotInferRolloutFreshnessFromWorkloadGeneration(t *testing.T) {
	const freshnessUnknown = reportv1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable
	tests := []struct {
		name      string
		active    bool
		configure func(*omev1beta1.InferenceService)
		observed  int64
	}{
		{
			name: "OMENative leaves top-level generation zero",
			configure: func(isvc *omev1beta1.InferenceService) {
				mode := constants.OMENative
				isvc.Spec.DeploymentMode = &mode
			},
			observed: 0,
		},
		{
			name:   "OMENative active pinned run leaves top-level generation zero",
			active: true,
			configure: func(isvc *omev1beta1.InferenceService) {
				mode := constants.OMENative
				isvc.Spec.DeploymentMode = &mode
			},
			observed: 0,
		},
		{
			name: "RawDeployment reports an unrelated Deployment generation",
			configure: func(isvc *omev1beta1.InferenceService) {
				mode := constants.RawDeployment
				isvc.Spec.DeploymentMode = &mode
			},
			observed: 3,
		},
		{
			name: "MultiNode reports an unrelated LeaderWorkerSet generation",
			configure: func(isvc *omev1beta1.InferenceService) {
				isvc.Spec.DeploymentMode = nil
				isvc.Spec.Engine.Annotations = map[string]string{
					constants.DeploymentMode: string(constants.MultiNode),
				}
			},
			observed: 99,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var isvc *omev1beta1.InferenceService
			if tt.active {
				isvc = validationISVCWithPinnedRun(t, []omev1beta1.RolloutGroup{{
					Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
					BlueGreen:  &omev1beta1.GroupBlueGreen{},
				}}, true)
			} else {
				isvc = baseValidationISVC()
				isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
					Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
					BlueGreen:  &omev1beta1.GroupBlueGreen{},
				}}}
				setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
			}
			tt.configure(isvc)
			isvc.Status.ObservedGeneration = tt.observed

			got, err := projectValidation(isvc)
			require.NoError(t, err)
			check := findValidationCheck(t, got,
				reportv1alpha1.RolloutValidationCheckRolloutResolution, "")
			assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable, check.Result)
			assert.Equal(t, reportv1alpha1.EvidenceReported, check.Evidence)
			assert.Equal(t, reportv1alpha1.RolloutValidationFreshnessUnverifiable, check.Freshness)
			assert.Contains(t, validationIssueCodes(got), freshnessUnknown)
			assert.NotContains(t, validationIssueCodes(got),
				reportv1alpha1.RolloutValidationIssueRolloutResolutionStale)
			assert.NotContains(t, validationIssueCodes(got),
				reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
		})
	}
}

func TestProjectValidationMarksGenerationStampedEvidenceStaleWithoutInferringRolloutFreshness(t *testing.T) {
	isvc := configuredValidationISVC()
	isvc.Spec.Rollout.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{Name: "rollout-policy", Progression: omev1beta1.RolloutProgressionBlueGreen}
	isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "autoscaler-policy"}
	isvc.Status.ObservedGeneration = 6
	setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
	setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 6)
	setAutoscalerResolved(isvc, metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy, 6)

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationUnverifiable, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.RolloutValidationFreshnessUnverifiable,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Freshness)
	assert.Equal(t, reportv1alpha1.RolloutValidationFreshnessStale,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficReadiness, "").Freshness)
	assert.Equal(t, reportv1alpha1.RolloutValidationFreshnessStale,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerResolution, reportv1alpha1.RuntimeComponentEngine).Freshness)
	assert.NotContains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueRolloutResolutionStale)
	assert.Contains(t, validationIssueCodes(got),
		reportv1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueTrafficEvidenceStale)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceStale)
}

func TestProjectValidationRejectsImpossibleGenerationStampedEvidenceAsMalformed(t *testing.T) {
	for _, observedGeneration := range []int64{-1, 8} {
		t.Run("traffic", func(t *testing.T) {
			isvc := configuredValidationISVC()
			setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
			setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, observedGeneration)

			got, err := projectValidation(isvc)
			require.NoError(t, err)
			check := findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficReadiness, "")
			assert.Equal(t, reportv1alpha1.RolloutValidationFreshnessUnverifiable, check.Freshness)
			assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueTrafficEvidenceMalformed)
		})
		t.Run("autoscaler", func(t *testing.T) {
			isvc := baseValidationISVC()
			isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
			setAutoscalerResolved(isvc, metav1.ConditionTrue,
				omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy, observedGeneration)

			got, err := projectValidation(isvc)
			require.NoError(t, err)
			check := findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerResolution,
				reportv1alpha1.RuntimeComponentEngine)
			assert.Equal(t, reportv1alpha1.RolloutValidationFreshnessUnverifiable, check.Freshness)
			assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
		})
	}
}

func TestProjectValidationTreatsReportedBlockedPrerequisitesAsUnverifiable(t *testing.T) {
	isvc := configuredValidationISVC()
	isvc.Spec.Rollout.Groups[0].PolicyRef = &omev1beta1.RolloutPolicyRef{Name: "rollout-policy", Progression: omev1beta1.RolloutProgressionBlueGreen}
	isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "autoscaler-policy"}
	setPlanReady(isvc, corev1.ConditionFalse, omev1beta1.RolloutPlanReasonPolicyNotFound)
	setTrafficReady(isvc, metav1.ConditionUnknown, omev1beta1.TrafficReasonPending, 7)
	setAutoscalerResolved(isvc, metav1.ConditionFalse, omev1beta1.AutoscalerResolvedReasonPolicyNotFound, 7)

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationUnverifiable, got.Content.Summary.State)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueRolloutResolutionFailed)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueTrafficNotReady)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueAutoscalerResolutionFailed)
}

func TestProjectValidationDetectsUnsupportedTrafficFields(t *testing.T) {
	isvc := configuredValidationISVC()
	setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
	setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
	isvc.Status.Traffic.Conditions = append(isvc.Status.Traffic.Conditions, metav1.Condition{
		Type:   omev1beta1.TrafficConditionBackendPolicyUnsupportedFields,
		Status: metav1.ConditionTrue, Reason: omev1beta1.TrafficReasonUnsupportedField,
		ObservedGeneration: 7, LastTransitionTime: metav1.NewTime(validationNow),
		Message: "SECRET_DROPPED_FIELDS",
	})

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationUnverifiable, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
		findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficReadiness, "").Result)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueTrafficNotReady)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "SECRET_DROPPED_FIELDS")
}

func TestProjectValidationBoundsTrafficSchemaAndReportedPolicyIdentity(t *testing.T) {
	t.Run("unknown algorithm is invalid stored schema", func(t *testing.T) {
		isvc := baseValidationISVC()
		unknown := omev1beta1.LoadBalancingType("SECRET_ALGORITHM")
		isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &unknown}
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultInvalid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficSpec, "").Result)
	})
	t.Run("reported algorithm mismatch is unverifiable", func(t *testing.T) {
		isvc := configuredValidationISVC()
		setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
		setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
		isvc.Status.Traffic.Algorithm = string(omev1beta1.LoadBalancingTypeLeastRequest)
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficReadiness, "").Result)
		assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueTrafficEvidenceMalformed)
	})
	t.Run("exact Istio GVK is recognized", func(t *testing.T) {
		isvc := configuredValidationISVC()
		setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
		setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
		isvc.Status.Traffic.BackendPolicyResource.APIVersion = "networking.istio.io/v1"
		isvc.Status.Traffic.BackendPolicyResource.Kind = "DestinationRule"
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultValid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficReadiness, "").Result)
	})
	t.Run("backend policy must belong to the subject", func(t *testing.T) {
		isvc := configuredValidationISVC()
		setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
		setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
		isvc.Status.Traffic.BackendPolicyResource.Name = "other-service"

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficReadiness, "").Result)
		assert.Contains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueTrafficEvidenceMalformed)
	})
	t.Run("annotation-only intent requires evidence", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Annotations[constants.RetryAttemptsAnnotation] = "2"
		setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
		isvc.Status.Traffic.Algorithm = "Default"
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultValid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficReadiness, "").Result)
	})
	t.Run("empty typed block is not traffic intent", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Traffic = &omev1beta1.TrafficSpec{}
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultNotApplicable,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckTrafficReadiness, "").Result)
	})
}

func TestProjectValidationMalformedEvidenceReturnsBoundedReport(t *testing.T) {
	isvc := configuredValidationISVC()
	isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
	isvc.Status.ObservedGeneration = 7
	isvc.Status.Conditions = append(isvc.Status.Conditions,
		apis.Condition{Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionTrue, Reason: "SECRET_REASON", Message: "SECRET_MESSAGE"},
		apis.Condition{Type: apis.ConditionType(omev1beta1.RolloutPlanReadyCondition), Status: corev1.ConditionTrue, Reason: omev1beta1.RolloutPlanReasonNoRun},
	)
	isvc.Status.Traffic = &omev1beta1.TrafficStatus{
		BackendPolicyResource: &omev1beta1.BackendPolicyRef{APIVersion: "SECRET_API", Kind: "SECRET_KIND", Name: "SECRET_NAME"},
		Conditions: []metav1.Condition{
			{Type: omev1beta1.TrafficConditionBackendPolicyReady, Status: metav1.ConditionTrue, Reason: omev1beta1.TrafficReasonAcceptedByGateway, ObservedGeneration: 7, Message: "SECRET_TRAFFIC"},
			{Type: omev1beta1.TrafficConditionBackendPolicyReady, Status: metav1.ConditionTrue, Reason: omev1beta1.TrafficReasonAcceptedByGateway, ObservedGeneration: 7},
		},
	}
	setAutoscalerResolved(isvc, metav1.ConditionTrue, "SECRET_AUTOSCALER_REASON", 7)
	isvc.Status.Components[omev1beta1.EngineComponent].Autoscaler.Conditions = append(
		isvc.Status.Components[omev1beta1.EngineComponent].Autoscaler.Conditions,
		metav1.Condition{Type: omev1beta1.AutoscalerResolvedCondition, Status: metav1.ConditionTrue, Reason: omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy, ObservedGeneration: 7},
	)

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationUnverifiable, got.Content.Summary.State)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueTrafficEvidenceMalformed)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, got))
		assert.NotContains(t, output.String(), "SECRET_")
	}
}

func TestProjectValidationRejectsMalformedPinnedPlanWithoutTrustingReadyCondition(t *testing.T) {
	isvc := configuredValidationISVC()
	setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonPinned)
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		RunID: "SECRET_RUN", Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{{
			Source: omev1beta1.RolloutPlanSourceInline,
			Group: omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				Canary:     &omev1beta1.GroupCanary{Steps: []omev1beta1.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 10}}},
			},
		}}},
	}}
	setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationUnverifiable, got.Content.Summary.State)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "SECRET_RUN")
}

func TestProjectValidationRejectsMalformedPinnedPlanProvenance(t *testing.T) {
	for _, mutate := range []func(*omev1beta1.RolloutRunGroup){
		func(group *omev1beta1.RolloutRunGroup) { group.Source = omev1beta1.RolloutPlanSource("SECRET_SOURCE") },
		func(group *omev1beta1.RolloutRunGroup) { group.PortableDigest = "SECRET_DIGEST" },
		func(group *omev1beta1.RolloutRunGroup) {
			group.Source = omev1beta1.RolloutPlanSourcePolicy
			group.PolicyRef = nil
		},
	} {
		isvc := configuredValidationISVC()
		setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonPinned)
		group := omev1beta1.RolloutRunGroup{
			Source:         omev1beta1.RolloutPlanSourceInline,
			PortableDigest: "rp1:aaaaaaaaaaaa",
			Group: omev1beta1.RolloutGroup{
				Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
				BlueGreen:  &omev1beta1.GroupBlueGreen{},
			},
		}
		mutate(&group)
		isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
			RunID: "run", Plan: omev1beta1.RolloutRunPlan{Groups: []omev1beta1.RolloutRunGroup{group}},
		}}
		setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckRolloutResolution, "").Result)
		assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), "SECRET_")
	}
}

func TestProjectValidationMissingPolicyEvidenceIsExplicitlyUnverifiable(t *testing.T) {
	isvc := baseValidationISVC()
	isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		PolicyRef:  &omev1beta1.RolloutPolicyRef{Name: "policy", Progression: omev1beta1.RolloutProgressionBlueGreen},
	}}}

	got, err := projectValidation(isvc)
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.RolloutValidationUnverifiable, got.Content.Summary.State)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueRolloutResolutionMissing)
	assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMissing)
}

func TestProjectValidationBindsAutoscalerResolutionToDeclaredPolicy(t *testing.T) {
	t.Run("rendered policy provenance matches", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
		setAutoscalerResolved(isvc, metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy, 7)
		status := isvc.Status.Components[omev1beta1.EngineComponent]
		status.Autoscaler.Policy = &omev1beta1.AutoscalerPolicyProvenance{
			Name: "policy", ObservedGeneration: 3,
			PortableDigest: "pv1:0123456789ab", ResolvedDigest: "rv1:0123456789ab",
		}
		isvc.Status.Components[omev1beta1.EngineComponent] = status
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultValid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerResolution, reportv1alpha1.RuntimeComponentEngine).Result)
	})
	t.Run("mismatched policy provenance is malformed", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
		setAutoscalerResolved(isvc, metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy, 7)
		status := isvc.Status.Components[omev1beta1.EngineComponent]
		status.Autoscaler.Policy = &omev1beta1.AutoscalerPolicyProvenance{Name: "other", ObservedGeneration: 3}
		isvc.Status.Components[omev1beta1.EngineComponent] = status
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerResolution, reportv1alpha1.RuntimeComponentEngine).Result)
		assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
	})
	t.Run("inline precedence binds shadow preview", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Engine.Autoscaler = &omev1beta1.ComponentAutoscaler{Class: omev1beta1.AutoscalerHPA}
		isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
		setAutoscalerResolved(isvc, metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonInlinePrecedence, 7)
		status := isvc.Status.Components[omev1beta1.EngineComponent]
		status.Autoscaler.ShadowedPolicyRef = &omev1beta1.ShadowedAutoscalerPolicy{Name: "policy"}
		isvc.Status.Components[omev1beta1.EngineComponent] = status
		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultValid,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerResolution, reportv1alpha1.RuntimeComponentEngine).Result)
	})
	t.Run("conflicting provenance is malformed", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
		setAutoscalerResolved(isvc, metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy, 7)
		status := isvc.Status.Components[omev1beta1.EngineComponent]
		status.Autoscaler.ShadowedPolicyRef = &omev1beta1.ShadowedAutoscalerPolicy{Name: "policy"}
		isvc.Status.Components[omev1beta1.EngineComponent] = status

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
			findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerResolution, reportv1alpha1.RuntimeComponentEngine).Result)
		assert.Contains(t, validationIssueCodes(got), reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
	})
}

func TestProjectValidationRejectsAutoscalerProvenanceContractMismatches(t *testing.T) {
	renderedTests := []struct {
		name   string
		mutate func(*omev1beta1.ComponentAutoscalerStatus)
	}{
		{name: "wrong source", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.SpecSource = "runtime"
		}},
		{name: "empty policy generation", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.Policy.ObservedGeneration = 0
		}},
		{name: "negative policy generation", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.Policy.ObservedGeneration = -1
		}},
		{name: "empty portable digest", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.Policy.PortableDigest = ""
		}},
		{name: "empty resolved digest", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.Policy.ResolvedDigest = ""
		}},
		{name: "malformed portable digest", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.Policy.PortableDigest = "pv1:not-a-digest"
		}},
		{name: "malformed resolved digest", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.Policy.ResolvedDigest = "rv1:not-a-digest"
		}},
	}
	for _, tt := range renderedTests {
		t.Run("rendered "+tt.name, func(t *testing.T) {
			isvc := baseValidationISVC()
			isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
			setAutoscalerResolved(isvc, metav1.ConditionTrue,
				omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy, 7)
			status := isvc.Status.Components[omev1beta1.EngineComponent]
			status.Autoscaler.SpecSource = "policy"
			status.Autoscaler.Policy = &omev1beta1.AutoscalerPolicyProvenance{
				Name: "policy", ObservedGeneration: 3,
				PortableDigest: "pv1:0123456789ab", ResolvedDigest: "rv1:0123456789ab",
			}
			tt.mutate(status.Autoscaler)
			isvc.Status.Components[omev1beta1.EngineComponent] = status

			got, err := projectValidation(isvc)
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
				findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerResolution,
					reportv1alpha1.RuntimeComponentEngine).Result)
			assert.Contains(t, validationIssueCodes(got),
				reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
		})
	}

	inlineTests := []struct {
		name   string
		mutate func(*omev1beta1.ComponentAutoscalerStatus)
	}{
		{name: "wrong source", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.SpecSource = "policy"
		}},
		{name: "partial shadow digest", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.ShadowedPolicyRef.WouldRenderDigest = ""
		}},
	}
	for _, tt := range inlineTests {
		t.Run("inline "+tt.name, func(t *testing.T) {
			isvc := baseValidationISVC()
			isvc.Spec.Engine.Autoscaler = &omev1beta1.ComponentAutoscaler{Class: omev1beta1.AutoscalerHPA}
			isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
			setAutoscalerResolved(isvc, metav1.ConditionTrue,
				omev1beta1.AutoscalerResolvedReasonInlinePrecedence, 7)
			status := isvc.Status.Components[omev1beta1.EngineComponent]
			status.Autoscaler.SpecSource = "isvc"
			status.Autoscaler.ShadowedPolicyRef = &omev1beta1.ShadowedAutoscalerPolicy{
				Name: "policy", PortableDigest: "pv1:0123456789ab",
				WouldRenderDigest: "rv1:0123456789ab",
			}
			tt.mutate(status.Autoscaler)
			isvc.Status.Components[omev1beta1.EngineComponent] = status

			got, err := projectValidation(isvc)
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.RolloutValidationResultUnverifiable,
				findValidationCheck(t, got, reportv1alpha1.RolloutValidationCheckAutoscalerResolution,
					reportv1alpha1.RuntimeComponentEngine).Result)
			assert.Contains(t, validationIssueCodes(got),
				reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
		})
	}
}

func TestProjectValidationValidatesAutoscalerFailureProvenance(t *testing.T) {
	t.Run("hold may preserve canonical prior policy", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "new-policy"}
		setAutoscalerResolved(isvc, metav1.ConditionFalse,
			omev1beta1.AutoscalerResolvedReasonPolicyNotFound, 7)
		status := isvc.Status.Components[omev1beta1.EngineComponent]
		status.Autoscaler.Policy = &omev1beta1.AutoscalerPolicyProvenance{
			Name: "prior-policy", ObservedGeneration: 2,
			PortableDigest: "pv1:0123456789ab", ResolvedDigest: "rv1:0123456789ab",
		}
		isvc.Status.Components[omev1beta1.EngineComponent] = status

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Contains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueAutoscalerResolutionFailed)
		assert.NotContains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
	})
	t.Run("hold rejects malformed prior policy", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
		setAutoscalerResolved(isvc, metav1.ConditionFalse,
			omev1beta1.AutoscalerResolvedReasonPolicyInvalid, 7)
		status := isvc.Status.Components[omev1beta1.EngineComponent]
		status.Autoscaler.Policy = &omev1beta1.AutoscalerPolicyProvenance{
			Name: "policy", ObservedGeneration: 2,
			PortableDigest: "pv1:bad", ResolvedDigest: "rv1:0123456789ab",
		}
		isvc.Status.Components[omev1beta1.EngineComponent] = status

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Contains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
	})
	t.Run("unsupported mode accepts ordinary source", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
		setAutoscalerResolved(isvc, metav1.ConditionFalse,
			omev1beta1.AutoscalerResolvedReasonUnsupportedMode, 7)

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Contains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueAutoscalerResolutionFailed)
		assert.NotContains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
	})
	t.Run("unsupported mode rejects policy source", func(t *testing.T) {
		isvc := baseValidationISVC()
		isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
		setAutoscalerResolved(isvc, metav1.ConditionFalse,
			omev1beta1.AutoscalerResolvedReasonUnsupportedMode, 7)
		status := isvc.Status.Components[omev1beta1.EngineComponent]
		status.Autoscaler.SpecSource = "policy"
		isvc.Status.Components[omev1beta1.EngineComponent] = status

		got, err := projectValidation(isvc)
		require.NoError(t, err)
		assert.Contains(t, validationIssueCodes(got),
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
	})
}

func TestProjectValidationIsDeterministicAndDoesNotMutateInput(t *testing.T) {
	isvc := configuredValidationISVC()
	isvc.Spec.Engine.AutoscalerPolicyRef = &omev1beta1.AutoscalerPolicyRef{Name: "policy"}
	setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonNoRun)
	setTrafficReady(isvc, metav1.ConditionTrue, omev1beta1.TrafficReasonAcceptedByGateway, 7)
	setAutoscalerResolved(isvc, metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonInlinePrecedence, 7)
	for i := 0; i < 100; i++ {
		isvc.Status.Conditions = append(isvc.Status.Conditions, apis.Condition{Type: apis.ConditionType("SECRET_CONDITION")})
		isvc.Status.Traffic.Conditions = append(isvc.Status.Traffic.Conditions, metav1.Condition{Type: "SECRET_TRAFFIC_CONDITION"})
		isvc.Status.Components[omev1beta1.EngineComponent].Autoscaler.Conditions = append(
			isvc.Status.Components[omev1beta1.EngineComponent].Autoscaler.Conditions,
			metav1.Condition{Type: "SECRET_AUTOSCALER_CONDITION"},
		)
	}
	before := isvc.DeepCopy()

	first, err := projectValidation(isvc)
	require.NoError(t, err)
	reversed := isvc.DeepCopy()
	sort.SliceStable(reversed.Status.Conditions, func(i, j int) bool { return i > j })
	sort.SliceStable(reversed.Status.Traffic.Conditions, func(i, j int) bool { return i > j })
	autoscaler := reversed.Status.Components[omev1beta1.EngineComponent].Autoscaler
	sort.SliceStable(autoscaler.Conditions, func(i, j int) bool { return i > j })
	second, err := projectValidation(reversed)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, before, isvc)
	assert.LessOrEqual(t, len(first.Content.Checks), 16)
	assert.LessOrEqual(t, len(first.Content.Issues), 32)
}

func projectValidation(isvc *omev1beta1.InferenceService) (reportv1alpha1.RolloutValidationReport, error) {
	return rolloutprojection.ProjectValidation(isvc, reportv1alpha1.ClockFunc(func() time.Time { return validationNow }))
}

func baseValidationISVC() *omev1beta1.InferenceService {
	mode := constants.OMENative
	return &omev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "chat", UID: types.UID("uid-chat"), Generation: 7,
			Annotations: map[string]string{},
		},
		Spec: omev1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Engine:         &omev1beta1.EngineSpec{},
		},
		Status: omev1beta1.InferenceServiceStatus{
			Components: map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{},
		},
	}
}

func configuredValidationISVC() *omev1beta1.InferenceService {
	isvc := baseValidationISVC()
	isvc.Status.ObservedGeneration = 7
	isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: []omev1beta1.RolloutGroup{{
		Components: []omev1beta1.ComponentType{omev1beta1.EngineComponent},
		BlueGreen:  &omev1beta1.GroupBlueGreen{},
	}}}
	algorithm := omev1beta1.LoadBalancingTypeRoundRobin
	isvc.Spec.Traffic = &omev1beta1.TrafficSpec{Algorithm: &algorithm}
	mode := omev1beta1.ScalingIndependent
	isvc.Spec.ScalingPolicy = &omev1beta1.ScalingPolicy{Mode: mode}
	isvc.Spec.Engine.Autoscaler = &omev1beta1.ComponentAutoscaler{Class: omev1beta1.AutoscalerHPA}
	return isvc
}

func setPlanReady(isvc *omev1beta1.InferenceService, status corev1.ConditionStatus, reason string) {
	isvc.Status.Conditions = append(isvc.Status.Conditions, apis.Condition{
		Type:   apis.ConditionType(omev1beta1.RolloutPlanReadyCondition),
		Status: status, Reason: reason, LastTransitionTime: apis.VolatileTime{Inner: metav1.NewTime(validationNow)},
	})
}

func setTrafficReady(isvc *omev1beta1.InferenceService, status metav1.ConditionStatus, reason string, generation int64) {
	isvc.Status.Traffic = &omev1beta1.TrafficStatus{
		Algorithm: string(omev1beta1.LoadBalancingTypeRoundRobin),
		BackendPolicyResource: &omev1beta1.BackendPolicyRef{
			APIVersion: "gateway.envoyproxy.io/v1alpha1", Kind: "BackendTrafficPolicy", Name: isvc.Name,
		},
		Conditions: []metav1.Condition{{
			Type: omev1beta1.TrafficConditionBackendPolicyReady, Status: status, Reason: reason,
			ObservedGeneration: generation, LastTransitionTime: metav1.NewTime(validationNow),
		}},
	}
}

func setAutoscalerResolved(isvc *omev1beta1.InferenceService, status metav1.ConditionStatus, reason string, generation int64) {
	component := isvc.Status.Components[omev1beta1.EngineComponent]
	component.Autoscaler = &omev1beta1.ComponentAutoscalerStatus{
		Conditions: []metav1.Condition{{
			Type: omev1beta1.AutoscalerResolvedCondition, Status: status, Reason: reason,
			ObservedGeneration: generation, LastTransitionTime: metav1.NewTime(validationNow),
		}},
	}
	if status == metav1.ConditionTrue && isvc.Spec.Engine != nil && isvc.Spec.Engine.AutoscalerPolicyRef != nil {
		switch reason {
		case omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy:
			component.Autoscaler.SpecSource = "policy"
			component.Autoscaler.Policy = &omev1beta1.AutoscalerPolicyProvenance{
				Name: isvc.Spec.Engine.AutoscalerPolicyRef.Name, ObservedGeneration: 3,
				PortableDigest: "pv1:0123456789ab", ResolvedDigest: "rv1:0123456789ab",
			}
		case omev1beta1.AutoscalerResolvedReasonInlinePrecedence:
			component.Autoscaler.SpecSource = "isvc"
			component.Autoscaler.ShadowedPolicyRef = &omev1beta1.ShadowedAutoscalerPolicy{
				Name:           isvc.Spec.Engine.AutoscalerPolicyRef.Name,
				PortableDigest: "pv1:0123456789ab", WouldRenderDigest: "rv1:0123456789ab",
			}
		}
	} else if status == metav1.ConditionFalse {
		switch reason {
		case omev1beta1.AutoscalerResolvedReasonPolicyNotFound,
			omev1beta1.AutoscalerResolvedReasonPolicyInvalid,
			omev1beta1.AutoscalerResolvedReasonAuthNotFound,
			omev1beta1.AutoscalerResolvedReasonClassUnavailable:
			component.Autoscaler.SpecSource = "policy"
		case omev1beta1.AutoscalerResolvedReasonUnsupportedMode:
			component.Autoscaler.SpecSource = "default"
		}
	}
	isvc.Status.Components[omev1beta1.EngineComponent] = component
}

func validationISVCWithPinnedRun(
	t *testing.T,
	groups []omev1beta1.RolloutGroup,
	keepLiveGroups bool,
) *omev1beta1.InferenceService {
	t.Helper()
	isvc := baseValidationISVC()
	if keepLiveGroups {
		isvc.Spec.Rollout = &omev1beta1.RolloutSpec{Groups: append([]omev1beta1.RolloutGroup{}, groups...)}
	}
	pinnedGroups := make([]omev1beta1.RolloutRunGroup, 0, len(groups))
	targets := make([]omev1beta1.RolloutRunTarget, 0, 3)
	seen := map[omev1beta1.ComponentType]bool{}
	revisions := map[omev1beta1.ComponentType]string{
		omev1beta1.EngineComponent:  "aaaaaaaa",
		omev1beta1.DecoderComponent: "bbbbbbbb",
		omev1beta1.RouterComponent:  "cccccccc",
	}
	for i := range groups {
		digest, err := rolloutpolicy.ProgressionDigest(&groups[i])
		require.NoError(t, err)
		pinnedGroups = append(pinnedGroups, omev1beta1.RolloutRunGroup{
			Source: omev1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: groups[i],
		})
		for _, component := range groups[i].Components {
			if seen[component] {
				continue
			}
			seen[component] = true
			targets = append(targets, omev1beta1.RolloutRunTarget{
				Component: component, Revision: revisions[component],
			})
			switch component {
			case omev1beta1.DecoderComponent:
				isvc.Spec.Decoder = &omev1beta1.DecoderSpec{}
			case omev1beta1.RouterComponent:
				isvc.Spec.Router = &omev1beta1.RouterSpec{}
			}
		}
	}
	opened := metav1.NewTime(validationNow.Add(-2 * time.Minute))
	pinned := metav1.NewTime(validationNow.Add(-time.Minute))
	isvc.Status.ObservedGeneration = isvc.Generation
	isvc.Status.Rollout = &omev1beta1.RolloutStatus{ActiveRun: &omev1beta1.RolloutRun{
		RunID: isvc.Name + "-0123456789ab", OpenedAt: opened, PinnedAt: pinned,
		TargetRevisions: targets, Plan: omev1beta1.RolloutRunPlan{Groups: pinnedGroups},
	}}
	setPlanReady(isvc, corev1.ConditionTrue, omev1beta1.RolloutPlanReasonPinned)
	return isvc
}

func validationCheck(
	name reportv1alpha1.RolloutValidationCheckName,
	component reportv1alpha1.RuntimeComponentType,
	result reportv1alpha1.RolloutValidationResult,
	evidence reportv1alpha1.EvidenceLevel,
	freshness reportv1alpha1.RolloutValidationFreshness,
) reportv1alpha1.RolloutValidationCheck {
	return reportv1alpha1.RolloutValidationCheck{
		Check: name, Component: component, Result: result, Evidence: evidence, Freshness: freshness,
	}
}

func findValidationCheck(
	t *testing.T,
	reportValue reportv1alpha1.RolloutValidationReport,
	name reportv1alpha1.RolloutValidationCheckName,
	component reportv1alpha1.RuntimeComponentType,
) reportv1alpha1.RolloutValidationCheck {
	t.Helper()
	for _, check := range reportValue.Content.Checks {
		if check.Check == name && check.Component == component {
			return check
		}
	}
	t.Fatalf("check %s/%s not found", name, component)
	return reportv1alpha1.RolloutValidationCheck{}
}

func validationIssueCodes(reportValue reportv1alpha1.RolloutValidationReport) []reportv1alpha1.RolloutValidationIssueCode {
	result := make([]reportv1alpha1.RolloutValidationIssueCode, 0, len(reportValue.Content.Issues))
	for _, issue := range reportValue.Content.Issues {
		result = append(result, issue.Code)
	}
	return result
}
