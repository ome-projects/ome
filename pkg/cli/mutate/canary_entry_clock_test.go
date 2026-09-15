package mutate

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func retainedAnalysisSample(v *v1beta1.InferenceService) {
	sample := metav1.NewTime(testNow.Add(-10 * time.Second))
	v.Status.Canary.LastEvaluationTime = &sample
	v.Status.Canary.LastConclusiveEvaluationTime = &sample
	v.Status.Canary.MetricResults = []v1beta1.AnalysisMetricResult{{
		Name: "PRIVATE_METRIC", Value: "0.01", Threshold: "0.05",
		Operator: v1beta1.ComparisonLTE, Passed: true, Time: &sample,
	}}
}

// The actor retains samples when it restamps the capacity-entry clock. A
// chronology check must not disable emergency rollback in that state.
func TestPrepareCanaryRollbackRetainedSampleAfterCapacityDip(t *testing.T) {
	for _, phase := range []v1beta1.RolloutPhase{v1beta1.RolloutPhasePending, v1beta1.RolloutPhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			v, state, work := analysisActionTarget(t)
			retainedAnalysisSample(v)
			_, err := PrepareCanaryRollout(v, state, work, "rollback", false, true, testClock)
			require.NoError(t, err, "serving sample baseline")
			entry := metav1.NewTime(testNow.Add(-5 * time.Second))
			v.Status.Canary.StepEnteredTime = &entry
			setCanaryPhase(v, phase)
			if phase == v1beta1.RolloutPhaseFailed {
				v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.ReadyTimeout = &metav1.Duration{Duration: time.Second}
				rehashCanary(t, v)
			}
			plan, err := PrepareCanaryRollout(v, state, work, "rollback", false, true, testClock)
			require.NoError(t, err, "rollback precedes capacity/parked Failed")
			require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-rollback","value":"true"}]`, string(plan.Patch()))
			var preview bytes.Buffer
			require.NoError(t, plan.WritePreview(&preview, "moirai", "ome", reportv1alpha1.DryRunClient))
			require.Contains(t, preview.String(), "Retained (before current entry)")
			require.NotContains(t, preview.String(), "PRIVATE_METRIC")
		})
	}
}

// Recovery also restamps serving entry. Explicit override precedes the actor's
// warm-up/sample logic, but must remain a strongly consented analysis override.
func TestPrepareCanaryOverrideRetainedSampleAfterRecovery(t *testing.T) {
	v, state, work := analysisActionTarget(t)
	retainedAnalysisSample(v)
	_, err := PrepareCanaryRollout(v, state, work, "promote", true, true, testClock)
	require.NoError(t, err, "serving sample baseline")
	entry := metav1.NewTime(testNow.Add(-5 * time.Second))
	v.Status.Canary.StepEnteredTime = &entry
	plan, err := PrepareCanaryRollout(v, state, work, "promote", true, true, testClock)
	require.NoError(t, err, "override precedes recovered serving warm-up")
	require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-promote","value":"bbbbbbbb"}]`, string(plan.Patch()))
	var preview bytes.Buffer
	require.NoError(t, plan.WritePreview(&preview, "moirai", "ome", reportv1alpha1.DryRunClient))
	require.Contains(t, preview.String(), "Retained (before current entry)")
	require.Contains(t, preview.String(), "ANALYSIS OVERRIDE")
	_, err = PrepareCanaryRollout(v, state, work, "promote", false, true, testClock)
	require.ErrorIs(t, err, ErrCanaryGate)
	_, err = PrepareCanaryRollout(v, state, work, "promote", true, false, testClock)
	require.ErrorIs(t, err, ErrAnalysisOverrideConfirmation)
}

func TestPrepareCanaryRetainedSamplesDoNotWaiveMalformedOrIdentityGuards(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1beta1.InferenceService)
	}{
		{"future evaluation", func(v *v1beta1.InferenceService) {
			x := metav1.NewTime(testNow.Add(time.Second))
			v.Status.Canary.LastEvaluationTime = &x
		}},
		{"zero entry", func(v *v1beta1.InferenceService) { v.Status.Canary.StepEnteredTime = &metav1.Time{} }},
		{"conclusive after evaluation", func(v *v1beta1.InferenceService) {
			x := metav1.NewTime(testNow.Add(-9 * time.Second))
			v.Status.Canary.LastConclusiveEvaluationTime = &x
		}},
		{"sample not evaluation", func(v *v1beta1.InferenceService) {
			x := metav1.NewTime(testNow.Add(-11 * time.Second))
			v.Status.Canary.MetricResults[0].Time = &x
		}},
		{"false comparison", func(v *v1beta1.InferenceService) { v.Status.Canary.MetricResults[0].Passed = false }},
		{"nonfinite value", func(v *v1beta1.InferenceService) { v.Status.Canary.MetricResults[0].Value = "NaN" }},
		{"target identity", func(v *v1beta1.InferenceService) { v.Status.Canary.TargetID = "ct1:aaaaaaaaaaaa" }},
		{"step identity", func(v *v1beta1.InferenceService) { v.Status.Canary.CurrentStep = 2 }},
	} {
		for _, action := range []string{"rollback", "promote"} {
			t.Run(tc.name+"/"+action, func(t *testing.T) {
				v, state, work := analysisActionTarget(t)
				retainedAnalysisSample(v)
				entry := metav1.NewTime(testNow.Add(-5 * time.Second))
				v.Status.Canary.StepEnteredTime = &entry
				tc.edit(v)
				plan, err := PrepareCanaryRollout(v, state, work, action, action == "promote", true, testClock)
				require.Error(t, err)
				require.Empty(t, plan.Patch())
				require.NotContains(t, err.Error(), "PRIVATE_")
			})
		}
	}
}
