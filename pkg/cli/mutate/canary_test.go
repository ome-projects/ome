package mutate

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func manualActionTarget(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState, ReplicaEvidence) {
	t.Helper()
	v, state := nativeTarget(t)
	pinnedCanary(t, v)
	v.Annotations = map[string]string{"private": "SECRET_PRIVATE_ANNOTATION"}
	v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[0].Pause = &v1beta1.RolloutPause{}
	v.Status.Rollout.ActiveRun.TargetRevisions[0].StableRevision = "aaaaaaaa"
	rehashCanary(t, v)
	setCanaryPhase(v, v1beta1.RolloutPhasePaused)
	entered := metav1.NewTime(testNow.Add(-20 * time.Second))
	v.Status.Canary.StepEnteredTime = &entered
	work, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(replicaFor(v)).OmeV1beta1(), v, []string{"engine"}, testClock)
	require.NoError(t, err)
	return v, state, work
}

func rehashCanary(t *testing.T, v *v1beta1.InferenceService) {
	t.Helper()
	pinned := &v.Status.Rollout.ActiveRun.Plan.Groups[0]
	digest, err := rolloutpolicy.ProgressionDigest(&pinned.Group)
	require.NoError(t, err)
	pinned.PortableDigest = digest
}

func setCanaryPhase(v *v1beta1.InferenceService, phase v1beta1.RolloutPhase) {
	c := v.Status.Components[v1beta1.EngineComponent]
	c.RolloutPhase = phase
	v.Status.Components[v1beta1.EngineComponent] = c
}

func analysisActionTarget(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState, ReplicaEvidence) {
	t.Helper()
	v, state, work := manualActionTarget(t)
	step := &v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[0]
	step.Analysis = &v1beta1.RolloutAnalysis{Interval: metav1.Duration{Duration: 10 * time.Second}, FailureLimit: 2, Metrics: []v1beta1.AnalysisMetric{{Name: "PRIVATE_METRIC", Query: "PRIVATE_QUERY", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}}}
	delay := metav1.Duration{Duration: time.Minute}
	step.Analysis.InitialDelay = &delay
	bake := metav1.Duration{Duration: 2 * time.Minute}
	step.Pause.Duration = &bake
	rehashCanary(t, v)
	return v, state, work
}

func TestPrepareCanaryExactManualAndRollbackPatch(t *testing.T) {
	for _, tc := range []struct{ action, key, value string }{{"promote", "promote", "bbbbbbbb"}, {"rollback", "rollback", "true"}} {
		t.Run(tc.action, func(t *testing.T) {
			v, state, work := manualActionTarget(t)
			v.Status.ObservedGeneration = 0 // Parent status is not child source acknowledgement.
			plan, err := PrepareCanaryRollout(v, state, work, tc.action, false, true, testClock)
			require.NoError(t, err)
			require.Equal(t, "bbbbbbbb", plan.RevisionHash())
			require.JSONEq(t, fmt.Sprintf(`[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-%s","value":"%s"}]`, tc.key, tc.value), string(plan.Patch()))
			copy := plan.Patch()
			copy[0] = 'x'
			require.Equal(t, byte('['), plan.Patch()[0])
			v.Annotations = nil
			plan, err = PrepareCanaryRollout(v, state, work, tc.action, false, true, testClock)
			require.NoError(t, err)
			require.Contains(t, string(plan.Patch()), `{"op":"add","path":"/metadata/annotations","value":{}}`)
		})
	}
}

func TestPrepareCanaryRefusesUnsafeEvidenceBeforePositiveWork(t *testing.T) {
	cases := []struct {
		name string
		edit func(*v1beta1.InferenceService, *ReplicaEvidence)
	}{
		{"absent run", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Status.Rollout = nil }},
		{"absent canary", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Status.Canary = nil }},
		{"digest", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			v.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest = "bad"
		}},
		{"run id", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Status.Rollout.ActiveRun.RunID = "forged" }},
		{"target id", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Status.Canary.TargetID = "ct1:aaaaaaaaaaaa" }},
		{"target hash", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Status.Canary.CanaryRevisionHash = "cccccccc" }},
		{"stable hash", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Status.Canary.StableRevisionHash = "cccccccc" }},
		{"missing stable", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			v.Status.Rollout.ActiveRun.TargetRevisions[0].StableRevision = ""
		}},
		{"unsafe stable", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			v.Status.Rollout.ActiveRun.TargetRevisions[0].StableRevision = "PRIVATE_STABLE"
		}},
		{"future pin", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			v.Status.Rollout.ActiveRun.PinnedAt = metav1.NewTime(testNow.Add(time.Hour))
		}},
		{"future enter", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			x := metav1.NewTime(testNow.Add(time.Hour))
			v.Status.Canary.StepEnteredTime = &x
		}},
		{"zero enter", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			v.Status.Canary.StepEnteredTime = &metav1.Time{}
		}},
		{"negative step", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Status.Canary.CurrentStep = -1 }},
		{"done step", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Status.Canary.CurrentStep = 2 }},
		{"invalid traffic", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			c := v.Status.Components[v1beta1.EngineComponent]
			c.Traffic[0].Percent = 51
			v.Status.Components[v1beta1.EngineComponent] = c
		}},
		{"unknown phase", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { setCanaryPhase(v, "PRIVATE_PHASE") }},
		{"paused", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			v.Annotations[constants.PausedRolloutAnnotation] = "true"
		}},
		{"frozen", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			v.Annotations[constants.PausedRolloutAnnotation] = "freeze"
		}},
		{"placement", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Finalizers = []string{"ome.io/placement"} }},
		{"deleting", func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
			x := metav1.NewTime(testNow)
			v.DeletionTimestamp = &x
		}},
		{"incomplete IR", func(_ *v1beta1.InferenceService, w *ReplicaEvidence) { w.complete = false }},
		{"IR UID", func(_ *v1beta1.InferenceService, w *ReplicaEvidence) { w.uid = "recreated" }},
		{"IR RV", func(_ *v1beta1.InferenceService, w *ReplicaEvidence) { w.resourceVersion = "41" }},
		{"missing IR", func(_ *v1beta1.InferenceService, w *ReplicaEvidence) { delete(w.sources, v1beta1.EngineComponent) }},
		{"IR target", func(_ *v1beta1.InferenceService, w *ReplicaEvidence) { w.sources[v1beta1.EngineComponent] = "cccccccc" }},
	}
	for _, tc := range cases {
		for _, action := range []string{"promote", "rollback"} {
			t.Run(tc.name+"/"+action, func(t *testing.T) {
				v, state, work := manualActionTarget(t)
				tc.edit(v, &work)
				plan, err := PrepareCanaryRollout(v, state, work, action, false, true, testClock)
				require.Error(t, err)
				require.Empty(t, plan.Patch())
				require.NotContains(t, err.Error(), "PRIVATE")
			})
		}
	}
	for _, key := range []string{constants.RolloutPromoteAnnotation, constants.RolloutRollbackAnnotation} {
		for _, value := range []string{"", "false", "true", "bbbbbbbb", "PRIVATE_VALUE"} {
			for _, action := range []string{"promote", "rollback"} {
				v, state, work := manualActionTarget(t)
				v.Annotations[key] = value
				plan, err := PrepareCanaryRollout(v, state, work, action, false, true, testClock)
				require.ErrorIs(t, err, ErrPending)
				require.Empty(t, plan.Patch())
			}
		}
	}
}

func TestPrepareCanaryPhaseAndGateApplicability(t *testing.T) {
	for _, phase := range []v1beta1.RolloutPhase{v1beta1.RolloutPhasePending, v1beta1.RolloutPhaseCanarying, v1beta1.RolloutPhasePaused, v1beta1.RolloutPhaseFailed} {
		v, state, work := manualActionTarget(t)
		setCanaryPhase(v, phase)
		if phase == v1beta1.RolloutPhasePending || phase == v1beta1.RolloutPhaseFailed {
			v.Status.Canary.StepEnteredTime = nil
			v.Status.Canary.ObservedTrafficWeight = 0
			c := v.Status.Components[v1beta1.EngineComponent]
			c.Traffic = nil
			v.Status.Components[v1beta1.EngineComponent] = c
		}
		_, err := PrepareCanaryRollout(v, state, work, "rollback", false, true, testClock)
		require.NoError(t, err, phase)
		_, err = PrepareCanaryRollout(v, state, work, "promote", false, true, testClock)
		if phase == v1beta1.RolloutPhasePaused {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
	for _, phase := range []v1beta1.RolloutPhase{v1beta1.RolloutPhaseStable, v1beta1.RolloutPhaseRollingBack, v1beta1.RolloutPhaseRolledBack} {
		v, state, work := manualActionTarget(t)
		setCanaryPhase(v, phase)
		_, err := PrepareCanaryRollout(v, state, work, "rollback", false, true, testClock)
		require.Error(t, err)
	}
	for _, gate := range []string{"immediate", "timed", "consumed", "hold", "no-enter", "rejected"} {
		v, state, work := manualActionTarget(t)
		s := &v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[0]
		switch gate {
		case "immediate":
			s.Pause = nil
		case "timed":
			s.Pause.Duration = &metav1.Duration{Duration: time.Second}
		case "consumed":
			v.Status.Canary.PromotedThrough = "bbbbbbbb"
		case "hold":
			v.Status.Canary.PreStepHold = true
		case "no-enter":
			v.Status.Canary.StepEnteredTime = nil
		case "rejected":
			v.Status.Canary.RolledBackRevisionHash = "bbbbbbbb"
		}
		rehashCanary(t, v)
		_, err := PrepareCanaryRollout(v, state, work, "promote", false, true, testClock)
		require.Error(t, err, gate)
	}
	v, state, work := manualActionTarget(t)
	v.Status.Canary.CurrentStep = 1
	v.Status.Canary.ObservedTrafficWeight = 100
	setCanaryPhase(v, v1beta1.RolloutPhasePromoting)
	c := v.Status.Components[v1beta1.EngineComponent]
	c.Traffic = []v1beta1.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100}}
	v.Status.Components[v1beta1.EngineComponent] = c
	v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[1].Pause = &v1beta1.RolloutPause{}
	rehashCanary(t, v)
	for _, action := range []string{"promote", "rollback"} {
		_, err := PrepareCanaryRollout(v, state, work, action, false, true, testClock)
		require.NoError(t, err, action)
	}
}

func TestPrepareCanaryAnalysisCannotBeOrdinaryPromotion(t *testing.T) {
	for _, sample := range []string{"unobserved", "pass", "fail", "empty", "unmatched", "duplicate", "mismatch", "missing"} {
		v, state, work := analysisActionTarget(t)
		if sample != "unobserved" {
			x := metav1.NewTime(testNow.Add(-10 * time.Second))
			v.Status.Canary.LastEvaluationTime = &x
			r := v1beta1.AnalysisMetricResult{Name: "PRIVATE_METRIC", Value: "0.01", Threshold: "0.05", Operator: v1beta1.ComparisonLTE, Passed: true, Time: &x, Message: "PRIVATE_MESSAGE"}
			switch sample {
			case "fail":
				r.Value = "0.1"
				r.Passed = false
				v.Status.Canary.AnalysisFailedChecks = 1
			case "empty":
				r.Value = ""
				r.Passed = false
			case "unmatched":
				r.Name = "PRIVATE_AUTH"
				r.Value = ""
				r.Passed = false
			case "mismatch":
				r.Threshold = "0.5"
			case "missing":
				v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[0].Analysis.Metrics = append(v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[0].Analysis.Metrics, v1beta1.AnalysisMetric{Name: "PRIVATE_MISSING", Query: "PRIVATE_QUERY_2", Operator: v1beta1.ComparisonGT, Threshold: "0"})
				rehashCanary(t, v)
			}
			v.Status.Canary.MetricResults = []v1beta1.AnalysisMetricResult{r}
			if sample == "duplicate" {
				v.Status.Canary.MetricResults = append(v.Status.Canary.MetricResults, r)
			}
			if sample == "pass" || sample == "fail" {
				v.Status.Canary.LastConclusiveEvaluationTime = &x
			}
		}
		_, err := PrepareCanaryRollout(v, state, work, "promote", false, true, testClock)
		require.Error(t, err, sample)
		_, err = PrepareCanaryRollout(v, state, work, "promote", true, false, testClock)
		require.ErrorIs(t, err, ErrAnalysisOverrideConfirmation)
		plan, err := PrepareCanaryRollout(v, state, work, "promote", true, true, testClock)
		require.NoError(t, err, sample)
		var preview bytes.Buffer
		require.NoError(t, plan.WritePreview(&preview, "moirai", "ome", reportv1alpha1.DryRunClient))
		require.Contains(t, preview.String(), "ANALYSIS OVERRIDE")
		require.Contains(t, preview.String(), "Metric 1")
		require.NotContains(t, preview.String(), "PRIVATE_")
		if sample == "unmatched" {
			require.Contains(t, preview.String(), "Unmatched")
		}
		if sample == "missing" {
			require.Contains(t, preview.String(), "Metric 2")
		}
		if sample == "duplicate" {
			require.Contains(t, preview.String(), "Result 2")
		}
	}
	for _, edit := range []func(*v1beta1.InferenceService){
		func(v *v1beta1.InferenceService) { v.Status.Canary.LastEvaluationTime = &metav1.Time{} },
		func(v *v1beta1.InferenceService) {
			x := metav1.NewTime(testNow.Add(-time.Minute))
			v.Status.Canary.LastEvaluationTime = &x
		},
		func(v *v1beta1.InferenceService) {
			x := metav1.NewTime(testNow.Add(time.Hour))
			v.Status.Canary.LastEvaluationTime = &x
		},
	} {
		v, state, work := analysisActionTarget(t)
		edit(v)
		plan, err := PrepareCanaryRollout(v, state, work, "promote", true, true, testClock)
		require.Error(t, err)
		require.Empty(t, plan.Patch())
	}
	v, state, work := manualActionTarget(t)
	_, err := PrepareCanaryRollout(v, state, work, "promote", true, true, testClock)
	require.Error(t, err)
}

func TestCanaryPreviewBindsExactRequestAndUsesActionWarnings(t *testing.T) {
	for _, action := range []string{"promote", "rollback"} {
		v, state, work := manualActionTarget(t)
		plan, err := PrepareCanaryRollout(v, state, work, action, false, true, testClock)
		require.NoError(t, err)
		var out bytes.Buffer
		require.NoError(t, plan.WritePreview(&out, "moirai", "ome", reportv1alpha1.DryRunNone))
		for _, value := range []string{"uid-chat", "42", "chat-0123456789ab", v.Status.Rollout.ActiveRun.Plan.Groups[0].PortableDigest, v.Status.Canary.TargetID, "bbbbbbbb", "aaaaaaaa", "Manual", "Paused"} {
			require.Contains(t, out.String(), value)
		}
		require.NotContains(t, out.String(), "SECRET")
		require.NotContains(t, out.String(), "RestartPolicy")
		for _, line := range strings.Split(out.String(), "\n") {
			require.LessOrEqual(t, len(line), 80, line)
		}
		if action == "rollback" {
			require.Contains(t, out.String(), "rejected target")
			require.Contains(t, out.String(), "Reported")
		}
		require.Error(t, plan.WritePreview(errorWriter{}, "moirai", "ome", reportv1alpha1.DryRunNone))
	}
}

func secondaryOnlyActionTarget(t *testing.T) (*v1beta1.InferenceService, *effective.RuntimeState, ReplicaEvidence) {
	t.Helper()
	v, _, _ := manualActionTarget(t)
	v.Spec.Router = &v1beta1.RouterSpec{}
	g := &v.Status.Rollout.ActiveRun.Plan.Groups[0].Group
	g.Components = []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent}
	rehashCanary(t, v)
	v.Status.Rollout.ActiveRun.TargetRevisions = append(v.Status.Rollout.ActiveRun.TargetRevisions, v1beta1.RolloutRunTarget{Component: v1beta1.RouterComponent, Revision: "cccccccc", StableRevision: "cccccccc"})
	v.Status.Canary.CanaryRevisionHash = "cccccccc"
	v.Status.Canary.StableRevisionHash = "cccccccc"
	v.Status.Canary.TargetID = "ct1:" + rolloutpolicy.ShortHash([]byte("engine=bbbbbbbb;router=cccccccc"))
	v.Status.Components[v1beta1.EngineComponent] = v1beta1.ComponentStatusSpec{}
	v.Status.Components[v1beta1.RouterComponent] = v1beta1.ComponentStatusSpec{RolloutPhase: v1beta1.RolloutPhasePaused, Traffic: []v1beta1.ComponentTrafficTarget{{RevisionName: "chat-router-rev-cccccccc", Percent: 100}}}
	rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: "simple", Namespace: "prod", UID: "uid-runtime", ResourceVersion: "99", Generation: 1}, Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox:1.36"}}}, RouterConfig: &v1beta1.RouterSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Image: "busybox:1.36"}}}}}
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	resolver, err := effective.NewRuntimePinResolver(kubefake.NewClientset().AppsV1(), effective.NewRuntimeResolver(clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build()), "ome", paging.Limits{PageSize: 16, MaxItems: 16, MaxPages: 2, RequestTimeout: time.Second})
	require.NoError(t, err)
	state, err := resolver.Resolve(context.Background(), v, effective.RuntimeResolveOptions{})
	require.NoError(t, err)
	ir := replicaFor(v)
	router := ir.DeepCopy()
	router.Name = "chat-router"
	router.UID = "uid-router"
	router.Spec.Component = v1beta1.RouterComponent
	router.Status.CurrentRevision = "chat-router-cccccccc"
	router.Status.UpdateRevision = "chat-router-cccccccc"
	work, err := CollectReplicaEvidence(context.Background(), omefake.NewSimpleClientset(ir, router).OmeV1beta1(), v, []string{"engine", "router"}, testClock)
	require.NoError(t, err)
	return v, state, work
}

func TestPrepareCanarySecondaryOnlyStillRequiresEveryNativeCurrentTarget(t *testing.T) {
	for _, action := range []string{"promote", "rollback"} {
		v, state, work := secondaryOnlyActionTarget(t)
		plan, err := PrepareCanaryRollout(v, state, work, action, false, true, testClock)
		require.NoError(t, err)
		require.Equal(t, "cccccccc", plan.RevisionHash())
		for _, edit := range []func(*v1beta1.InferenceService, *ReplicaEvidence){
			func(_ *v1beta1.InferenceService, w *ReplicaEvidence) { delete(w.sources, v1beta1.EngineComponent) },
			func(_ *v1beta1.InferenceService, w *ReplicaEvidence) { w.sources[v1beta1.EngineComponent] = "dddddddd" },
			func(v *v1beta1.InferenceService, _ *ReplicaEvidence) {
				v.Status.Rollout.ActiveRun.TargetRevisions[0].StableRevision = "bbbbbbbb"
			},
			func(v *v1beta1.InferenceService, _ *ReplicaEvidence) { v.Status.Canary.TargetID = "ct1:000000000000" },
		} {
			v, state, work := secondaryOnlyActionTarget(t)
			edit(v, &work)
			plan, err := PrepareCanaryRollout(v, state, work, action, false, true, testClock)
			require.Error(t, err)
			require.Empty(t, plan.Patch())
		}
	}
}

func TestPrepareCanaryEqualPrimaryCannotForgePendingOrFailedRollback(t *testing.T) {
	for _, phase := range []v1beta1.RolloutPhase{v1beta1.RolloutPhasePending, v1beta1.RolloutPhaseFailed} {
		v, state, work := manualActionTarget(t)
		v.Status.Rollout.ActiveRun.TargetRevisions[0].StableRevision = "bbbbbbbb"
		v.Status.Canary.StableRevisionHash = "bbbbbbbb"
		setCanaryPhase(v, phase)
		v.Status.Canary.StepEnteredTime = nil
		v.Status.Canary.ObservedTrafficWeight = 0
		c := v.Status.Components[v1beta1.EngineComponent]
		c.Traffic = nil
		v.Status.Components[v1beta1.EngineComponent] = c
		plan, err := PrepareCanaryRollout(v, state, work, "rollback", false, true, testClock)
		require.Error(t, err, phase)
		require.Empty(t, plan.Patch())
	}
}

func TestPrepareCanaryRuntimeShapesFailClosed(t *testing.T) {
	for _, edit := range []func(*v1beta1.InferenceService, *effective.RuntimeState){
		func(v *v1beta1.InferenceService, _ *effective.RuntimeState) { v.ResourceVersion = "43" },
		func(v *v1beta1.InferenceService, _ *effective.RuntimeState) { v.UID = "recreated" },
		func(v *v1beta1.InferenceService, _ *effective.RuntimeState) {
			kind := "UnknownRuntime"
			v.Spec.Runtime.Kind = &kind
		},
		func(v *v1beta1.InferenceService, _ *effective.RuntimeState) {
			group := "other.io"
			v.Spec.Runtime.APIGroup = &group
		},
		func(_ *v1beta1.InferenceService, s *effective.RuntimeState) {
			s.PinMode = effective.RuntimePinMode("Unsupported")
		},
	} {
		v, state, work := manualActionTarget(t)
		edit(v, state)
		plan, err := PrepareCanaryRollout(v, state, work, "promote", false, true, testClock)
		require.ErrorIs(t, err, ErrRuntime)
		require.Empty(t, plan.Patch())
	}
}

func TestPrepareCanaryRollbackOnStallRequiresExplicitOverride(t *testing.T) {
	v, state, work := analysisActionTarget(t)
	policy := v1beta1.OnInconclusiveRollbackOnStall
	v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[0].Analysis.OnInconclusive = &policy
	rehashCanary(t, v)
	plan, err := PrepareCanaryRollout(v, state, work, "promote", false, true, testClock)
	require.ErrorIs(t, err, ErrCanaryGate)
	require.Empty(t, plan.Patch())
	plan, err = PrepareCanaryRollout(v, state, work, "promote", true, false, testClock)
	require.ErrorIs(t, err, ErrAnalysisOverrideConfirmation)
	require.Empty(t, plan.Patch())
	plan, err = PrepareCanaryRollout(v, state, work, "promote", true, true, testClock)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Patch())
}

func TestPrepareCanaryRollbackMayAbortProvedRepinExposureHold(t *testing.T) {
	for _, secondaryOnly := range []bool{false, true} {
		v, state, work := manualActionTarget(t)
		if secondaryOnly {
			v, state, work = secondaryOnlyActionTarget(t)
		}
		v.Status.Canary.PreStepHold = true
		v.Status.Canary.ObservedTrafficWeight = 25
		if !secondaryOnly {
			c := v.Status.Components[v1beta1.EngineComponent]
			c.Traffic[0].Percent = 75
			c.Traffic[1].Percent = 25
			v.Status.Components[v1beta1.EngineComponent] = c
		}
		plan, err := PrepareCanaryRollout(v, state, work, "rollback", false, true, testClock)
		require.NoError(t, err, "rollback is consumed before capacity/hold; secondaryOnly=%v", secondaryOnly)
		require.NotEmpty(t, plan.Patch())
		plan, err = PrepareCanaryRollout(v, state, work, "promote", false, true, testClock)
		require.Error(t, err)
		require.Empty(t, plan.Patch())
	}
}

func TestPrepareCanaryRollbackAllowsOneWriteAdvanceAndSecondaryCapacityBoundaries(t *testing.T) {
	v, state, work := manualActionTarget(t)
	v.Status.Canary.CurrentStep = 1
	v.Status.Canary.PromotedThrough = "bbbbbbbb"
	setCanaryPhase(v, v1beta1.RolloutPhaseCanarying)
	_, err := PrepareCanaryRollout(v, state, work, "rollback", false, true, testClock)
	require.NoError(t, err)
	_, err = PrepareCanaryRollout(v, state, work, "promote", false, true, testClock)
	require.Error(t, err)
	for _, phase := range []v1beta1.RolloutPhase{v1beta1.RolloutPhasePending, v1beta1.RolloutPhaseFailed} {
		v, state, work := secondaryOnlyActionTarget(t)
		c := v.Status.Components[v1beta1.RouterComponent]
		c.RolloutPhase = phase
		c.Traffic = nil
		v.Status.Components[v1beta1.RouterComponent] = c
		v.Status.Canary.StepEnteredTime = nil
		v.Status.Canary.ObservedTrafficWeight = 0
		_, err := PrepareCanaryRollout(v, state, work, "rollback", false, true, testClock)
		require.NoError(t, err, phase)
	}
}

func TestPrepareCanaryAnalysisMalformedSamplesCannotBeOverridden(t *testing.T) {
	for _, raw := range []string{"NaN", "Inf", "-Inf", "PRIVATE_VALUE", "1e999"} {
		v, state, work := analysisActionTarget(t)
		x := metav1.NewTime(testNow.Add(-10 * time.Second))
		v.Status.Canary.LastEvaluationTime = &x
		v.Status.Canary.LastConclusiveEvaluationTime = &x
		v.Status.Canary.MetricResults = []v1beta1.AnalysisMetricResult{{Name: "PRIVATE_METRIC", Value: raw, Threshold: "0.05", Operator: v1beta1.ComparisonLTE, Passed: true, Time: &x}}
		plan, err := PrepareCanaryRollout(v, state, work, "promote", true, true, testClock)
		require.Error(t, err)
		require.Empty(t, plan.Patch())
		require.NotContains(t, err.Error(), raw)
	}
}

func TestCanaryPreviewShowsAllTenResultsWithBoundedLongIdentities(t *testing.T) {
	v, state, work := analysisActionTarget(t)
	x := metav1.NewTime(testNow.Add(-10 * time.Second))
	v.Status.Canary.LastEvaluationTime = &x
	v.Status.Canary.LastConclusiveEvaluationTime = &x
	analysis := v.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary.Steps[0].Analysis
	analysis.Metrics = nil
	for i := 1; i <= 10; i++ {
		name := fmt.Sprintf("PRIVATE_METRIC_%d", i)
		analysis.Metrics = append(analysis.Metrics, v1beta1.AnalysisMetric{Name: name, Query: "PRIVATE_QUERY", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"})
		v.Status.Canary.MetricResults = append(v.Status.Canary.MetricResults, v1beta1.AnalysisMetricResult{Name: name, Value: "0.01", Threshold: "0.05", Operator: v1beta1.ComparisonLTE, Passed: true, Message: "PRIVATE_MESSAGE", Time: &x})
	}
	rehashCanary(t, v)
	plan, err := PrepareCanaryRollout(v, state, work, "promote", true, true, testClock)
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, plan.WritePreview(&out, strings.Repeat("c", 256), strings.Repeat("n", 63), reportv1alpha1.DryRunClient))
	for i := 1; i <= 10; i++ {
		require.Contains(t, out.String(), fmt.Sprintf("Metric %d", i))
		require.Contains(t, out.String(), fmt.Sprintf("Result %d", i))
	}
	require.NotContains(t, out.String(), "PRIVATE_")
	for _, line := range strings.Split(out.String(), "\n") {
		require.LessOrEqual(t, len(line), 80, line)
	}
	counter := &canaryPreviewWriteCounter{}
	require.NoError(t, plan.WritePreview(counter, strings.Repeat("c", 256), strings.Repeat("n", 63), reportv1alpha1.DryRunClient))
	for n := 0; n < counter.calls; n++ {
		require.Error(t, plan.WritePreview(&failAfterWriter{remaining: n}, strings.Repeat("c", 256), strings.Repeat("n", 63), reportv1alpha1.DryRunClient))
	}
}

type canaryPreviewWriteCounter struct{ calls int }

func (w *canaryPreviewWriteCounter) Write(p []byte) (int, error) { w.calls++; return len(p), nil }
