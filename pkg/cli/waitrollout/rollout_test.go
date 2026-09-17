package waitrollout

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	report "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

var now = time.Date(2026, 9, 15, 22, 0, 0, 0, time.UTC)

func independent() *ome.InferenceService {
	mode := constants.OMENative
	v := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "private-uid", ResourceVersion: "private-rv", Generation: 7}, Spec: ome.InferenceServiceSpec{DeploymentMode: &mode, Engine: &ome.EngineSpec{}}}
	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {Lifecycle: &ome.LifecycleStatus{CurrentRevision: "chat-engine-aaaaaaaa", UpdateRevision: "chat-engine-aaaaaaaa"}}}
	return v
}

func coordinated(phase ome.CoordinationPhase) *ome.InferenceService {
	v := independent()
	v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Components: []ome.ComponentType{ome.EngineComponent}}}}
	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {}}
	v.Status.RolloutCoordination = &ome.RolloutCoordinationStatus{Groups: []ome.RolloutCoordinationGroupStatus{{Name: "0", Components: []ome.ComponentType{ome.EngineComponent}, Policy: ome.CoordinationPolicyBlueGreen, Phase: phase}}}
	return v
}

func canary(phase ome.RolloutPhase) *ome.InferenceService {
	v := independent()
	v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Components: []ome.ComponentType{ome.EngineComponent}, Canary: &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}}}}}
	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {RolloutPhase: phase, LatestRolledoutRevision: "chat-engine-rev-aaaaaaaa", Traffic: []ome.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100}}}}
	v.Status.Canary = &ome.CanaryStatus{CanaryRevisionHash: "bbbbbbbb", StableRevisionHash: "aaaaaaaa", CurrentStep: 0}
	if phase == ome.RolloutPhaseRolledBack {
		v.Status.Canary.RolledBackRevisionHash = "bbbbbbbb"
	}
	if phase == ome.RolloutPhaseStable {
		v.Status.Canary.StableRevisionHash = ""
		v.Status.Canary.CurrentStep = 1
		v.Status.Canary.ObservedTrafficWeight = 100
		s := v.Status.Components[ome.EngineComponent]
		s.Traffic[0].RevisionName = "chat-engine-rev-bbbbbbbb"
		s.LatestRolledoutRevision = "chat-engine-rev-bbbbbbbb"
		v.Status.Components[ome.EngineComponent] = s
	}
	return v
}

func TestQualifiedReportedRolloutMatching(t *testing.T) {
	for _, tt := range []struct {
		name      string
		v         *ome.InferenceService
		requested report.WaitRequested
		state     report.RolloutState
	}{
		{"independent", independent(), report.WaitRequestedRolloutStable, report.RolloutStateSucceeded},
		{"bluegreen", coordinated(ome.CoordinationPhaseIdle), report.WaitRequestedRolloutStable, report.RolloutStateSucceeded},
		{"completedcanary", canary(ome.RolloutPhaseStable), report.WaitRequestedRolloutStable, report.RolloutStateSucceeded},
		{"failed", canary(ome.RolloutPhaseFailed), report.WaitRequestedRolloutFailed, report.RolloutStateFailed},
		{"rolledback", canary(ome.RolloutPhaseRolledBack), report.WaitRequestedRolloutRolledBack, report.RolloutStateRolledBack},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, generation := range []int64{0, 6, 7, 8} {
				tt.v.Status.ObservedGeneration = generation
				d, o, err := Evaluate(tt.v, tt.requested, now)
				require.NoError(t, err)
				require.True(t, d.Matched)
				require.Equal(t, tt.state, o.Summary.ReportedState)
				require.Equal(t, report.RolloutStateUnknown, o.Summary.State)
				require.Equal(t, report.RolloutEpochUnverifiable, o.Summary.Epoch)
				require.Equal(t, "Valid", o.Validity)
			}
		})
	}
}

func TestRolloutPredicateRejectsInvalidSubjectAndRequest(t *testing.T) {
	_, _, err := Evaluate(nil, report.WaitRequestedRolloutStable, now)
	require.Error(t, err)
	_, _, err = Evaluate(independent(), report.WaitRequestedTrue, now)
	require.Error(t, err)
}

func pinned(v *ome.InferenceService) {
	g := v.Spec.Rollout.Groups[0]
	if g.Canary == nil && g.BlueGreen == nil && g.RollingUpdate == nil {
		g.BlueGreen = &ome.GroupBlueGreen{}
	}
	digest, err := rolloutpolicy.ProgressionDigest(&g)
	if err != nil {
		panic(err)
	}
	v.Status.Rollout = &ome.RolloutStatus{ActiveRun: &ome.RolloutRun{RunID: "chat-0123456789ab", OpenedAt: metav1.NewTime(now.Add(-10 * time.Second)), PinnedAt: metav1.NewTime(now.Add(-10 * time.Second)), Plan: ome.RolloutRunPlan{Groups: []ome.RolloutRunGroup{{Source: ome.RolloutPlanSourceInline, PortableDigest: digest, Group: g}}}, TargetRevisions: []ome.RolloutRunTarget{{Component: ome.EngineComponent, Revision: "aaaaaaaa"}}}}
}

func TestWholeRolloutAdmissionLimits(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*ome.InferenceService)
	}{
		{"conditions65", func(v *ome.InferenceService) { v.Status.Conditions = make(duckv1.Conditions, 65) }},
		{"groups4", func(v *ome.InferenceService) { v.Spec.Rollout = &ome.RolloutSpec{Groups: make([]ome.RolloutGroup, 4)} }},
		{"traffic17", func(v *ome.InferenceService) {
			s := v.Status.Components[ome.EngineComponent]
			s.Traffic = make([]ome.ComponentTrafficTarget, 17)
			v.Status.Components[ome.EngineComponent] = s
		}},
		{"lifecycleconditions65", func(v *ome.InferenceService) {
			v.Status.Components[ome.EngineComponent].Lifecycle.Conditions = make([]metav1.Condition, 65)
		}},
		{"componentmap4", func(v *ome.InferenceService) {
			for _, x := range []ome.ComponentType{"decoder", "router", "hostile"} {
				v.Status.Components[x] = ome.ComponentStatusSpec{}
			}
		}},
		{"freeform4097", func(v *ome.InferenceService) { v.Annotations = map[string]string{"opaque": strings.Repeat("x", 4097)} }},
		{"uid257", func(v *ome.InferenceService) { v.UID = types.UID(strings.Repeat("x", 257)) }},
		{"steps21", func(v *ome.InferenceService) {
			v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Components: []ome.ComponentType{ome.EngineComponent}, Canary: &ome.GroupCanary{Steps: make([]ome.RolloutGroupStep, 21)}}}}
		}},
		{"targets4", func(v *ome.InferenceService) {
			v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Components: []ome.ComponentType{ome.EngineComponent}}}}
			pinned(v)
			v.Status.Rollout.ActiveRun.TargetRevisions = make([]ome.RolloutRunTarget, 4)
		}},
		{"coordination4", func(v *ome.InferenceService) {
			v.Status.RolloutCoordination = &ome.RolloutCoordinationStatus{Groups: make([]ome.RolloutCoordinationGroupStatus, 4)}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v := independent()
			tt.edit(v)
			d, _, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
			require.ErrorAs(t, err, new(*waitengine.Error))
			require.False(t, d.Matched)
			require.Equal(t, string(waitengine.ReasonRolloutInspectionLimit), err.Error())
		})
	}
}

func TestMalformedRolloutCannotMatch(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*ome.InferenceService)
	}{
		{"negativecount", func(v *ome.InferenceService) { v.Status.Components[ome.EngineComponent].Lifecycle.ReadyReplicas = -1 }},
		{"negativeparentgeneration", func(v *ome.InferenceService) { v.Status.ObservedGeneration = -1 }},
		{"negativelifecyclegeneration", func(v *ome.InferenceService) {
			v.Status.Components[ome.EngineComponent].Lifecycle.ObservedGeneration = -1
		}},
		{"futurecondition", func(v *ome.InferenceService) {
			v.Status.Conditions = duckv1.Conditions{{Type: "Other", Status: "True", LastTransitionTime: apis.VolatileTime{Inner: metav1.NewTime(now.Add(time.Second))}}}
		}},
		{"unknowncomponent", func(v *ome.InferenceService) { v.Status.Components["hostile"] = ome.ComponentStatusSpec{} }},
		{"duplicateplan", func(v *ome.InferenceService) {
			v.Status.Conditions = duckv1.Conditions{{Type: ome.RolloutPlanReadyCondition, Status: "True"}, {Type: ome.RolloutPlanReadyCondition, Status: "True"}}
		}},
		{"futuretypedgeneration", func(v *ome.InferenceService) {
			v.Status.Components[ome.EngineComponent].Lifecycle.ObservedGeneration = 7
			v.Status.Components[ome.EngineComponent].Lifecycle.Conditions = []metav1.Condition{{Type: "Available", Status: "True", ObservedGeneration: 8, LastTransitionTime: metav1.NewTime(now.Add(-time.Second))}}
		}},
		{"negativeconditiongeneration", func(v *ome.InferenceService) {
			v.Status.Components[ome.EngineComponent].Lifecycle.Conditions = []metav1.Condition{{Type: "Available", Status: "True", ObservedGeneration: -1, LastTransitionTime: metav1.NewTime(now.Add(-time.Second))}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v := independent()
			tt.edit(v)
			d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
			require.NoError(t, err)
			require.False(t, d.Matched)
			require.Equal(t, "Invalid", o.Validity)
		})
	}
}

func TestInvalidPinnedProvenanceDoesNotMatch(t *testing.T) {
	for _, edit := range []func(*ome.RolloutRun){func(r *ome.RolloutRun) { r.Plan.Groups[0].PortableDigest = "wrong" }, func(r *ome.RolloutRun) { r.PinnedAt = metav1.NewTime(now.Add(time.Second)) }, func(r *ome.RolloutRun) { r.Plan.Groups = nil }} {
		v := coordinated(ome.CoordinationPhaseIdle)
		pinned(v)
		edit(v.Status.Rollout.ActiveRun)
		d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
		require.NoError(t, err)
		require.False(t, d.Matched)
		require.Equal(t, "Invalid", o.Validity)
	}
}

func TestMissingRolloutEvidenceIsUnavailable(t *testing.T) {
	v := coordinated(ome.CoordinationPhaseIdle)
	v.Status.RolloutCoordination = nil
	d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
	require.Equal(t, "Unavailable", o.Validity)
}

func TestInvalidUTF8ConditionIsInvalidNotAcquisitionFailure(t *testing.T) {
	v := independent()
	v.Status.Conditions = duckv1.Conditions{{Type: "Other", Status: "True", Reason: string([]byte{255})}}
	d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
	require.Equal(t, "Invalid", o.Validity)
}

func TestInvalidUTF8TypedConditionDoesNotMatch(t *testing.T) {
	v := independent()
	v.Status.Components[ome.EngineComponent].Lifecycle.Conditions = []metav1.Condition{{Type: "Available", Status: "True", ObservedGeneration: 7, LastTransitionTime: metav1.NewTime(now.Add(-time.Second)), Message: string([]byte{255})}}
	d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
	require.Equal(t, "Invalid", o.Validity)
}

func TestStableExcludesNonterminalAndNoConfiguration(t *testing.T) {
	for _, v := range []*ome.InferenceService{coordinated(ome.CoordinationPhaseStaged), coordinated(ome.CoordinationPhaseShifting), coordinated(ome.CoordinationPhasePaused), coordinated(ome.CoordinationPhaseRollingBack), canary(ome.RolloutPhaseFailed), canary(ome.RolloutPhaseRolledBack)} {
		d, _, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
		require.NoError(t, err)
		require.False(t, d.Matched)
	}
	v := independent()
	raw := constants.RawDeployment
	v.Spec.DeploymentMode = &raw
	v.Spec.Engine.Annotations = map[string]string{constants.DeploymentMode: string(raw)}
	v.Status.Components = nil
	d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
	require.Equal(t, report.RolloutStateNotConfigured, o.Summary.ReportedState)
}

func TestPinnedAndStaleConditionEvidenceRemainQualified(t *testing.T) {
	v := coordinated(ome.CoordinationPhaseIdle)
	pinned(v)
	d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.True(t, d.Matched)
	require.Equal(t, 1, o.Inspection.PinnedGroups)
	v = independent()
	v.Status.Components[ome.EngineComponent].Lifecycle.ObservedGeneration = 7
	v.Status.Components[ome.EngineComponent].Lifecycle.Conditions = []metav1.Condition{{Type: "Available", Status: "True", ObservedGeneration: 6, LastTransitionTime: metav1.NewTime(now.Add(-time.Second))}}
	d, o, err = Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.True(t, d.Matched)
	require.Contains(t, o.Warnings, report.WaitRolloutWarning("StaleReportedCondition"))
	copy := v.DeepCopy()
	copy.Status.Components[ome.EngineComponent].Lifecycle.Conditions = append(copy.Status.Components[ome.EngineComponent].Lifecycle.Conditions, copy.Status.Components[ome.EngineComponent].Lifecycle.Conditions[0])
	d, o, err = Evaluate(copy, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
	require.Equal(t, "Invalid", o.Validity)
}

func TestSupplementalShapesAndBounds(t *testing.T) {
	for _, edit := range []func(*ome.InferenceService){
		func(v *ome.InferenceService) {
			v.Status.RolloutCoordination.Groups[0].LastTransitionTime = &metav1.Time{}
		},
		func(v *ome.InferenceService) {
			v.Status.RolloutCoordination.Groups[0].LastTransitionTime = &metav1.Time{Time: now.Add(time.Second)}
		},
		func(v *ome.InferenceService) {
			v.Status.RolloutCoordination.Groups[0].ObservedRatio = &ome.RolloutCoordinationRatio{Original: map[ome.ComponentType]int32{"hostile": 1}}
		},
		func(v *ome.InferenceService) {
			v.Status.RolloutCoordination.Groups[0].ObservedRatio = &ome.RolloutCoordinationRatio{Current: map[ome.ComponentType]int32{ome.EngineComponent: -1}}
		},
	} {
		v := coordinated(ome.CoordinationPhaseIdle)
		edit(v)
		d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
		require.NoError(t, err)
		require.False(t, d.Matched)
		require.Equal(t, "Invalid", o.Validity)
	}
	for _, edit := range []func(*ome.InferenceService){
		func(v *ome.InferenceService) { v.Status.Canary.MetricResults = make([]ome.AnalysisMetricResult, 11) },
		func(v *ome.InferenceService) { v.Spec.Rollout.Groups[0].Components = make([]ome.ComponentType, 4) },
		func(v *ome.InferenceService) {
			v.Spec.Rollout.Groups[0].Canary.Steps[0].Analysis = &ome.RolloutAnalysis{Metrics: make([]ome.AnalysisMetric, 11)}
		},
		func(v *ome.InferenceService) {
			v.Spec.Rollout.Groups[0].Canary.Steps[0].Capacity = intstr.FromString(strings.Repeat("x", 65))
		},
	} {
		v := canary(ome.RolloutPhaseStable)
		edit(v)
		_, _, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
		require.EqualError(t, err, "RolloutInspectionLimit")
	}
}

func TestPerComponentCanaryMetricsRequireWholeAdmission(t *testing.T) {
	v := canary(ome.RolloutPhaseStable)
	component := v.Status.Components[ome.EngineComponent]
	component.Canary = v.Status.Canary.DeepCopy()
	component.Canary.MetricResults = make([]ome.AnalysisMetricResult, 11)
	v.Status.Components[ome.EngineComponent] = component

	d, _, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.False(t, d.Matched)
	require.EqualError(t, err, "RolloutInspectionLimit")
}

func TestPerComponentCanaryFutureTimestampCannotMatch(t *testing.T) {
	v := canary(ome.RolloutPhaseStable)
	component := v.Status.Components[ome.EngineComponent]
	component.Canary = v.Status.Canary.DeepCopy()
	component.Canary.StepEnteredTime = &metav1.Time{Time: now.Add(time.Second)}
	v.Status.Components[ome.EngineComponent] = component

	d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
	require.Equal(t, "Invalid", o.Validity)
}

func TestConflictingReadyInspectionCannotQualifyRollout(t *testing.T) {
	v := independent()
	v.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionReady, Status: corev1.ConditionTrue}, {Type: apis.ConditionReady, Status: corev1.ConditionFalse}}
	d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
	require.Equal(t, "Invalid", o.Validity)
	require.Contains(t, o.Warnings, report.WaitRolloutWarning("ConflictingReadyConditions"))
}

func TestPinnedEffectivePlanIgnoresLiveDriftButBoundsBoth(t *testing.T) {
	v := coordinated(ome.CoordinationPhaseIdle)
	pinned(v)
	v.Spec.Rollout.Groups[0].Canary = &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}}
	d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.True(t, d.Matched)
	require.Equal(t, report.RolloutStateSucceeded, o.Summary.ReportedState)
	v.Spec.Rollout.Groups[0].Canary.Steps = make([]ome.RolloutGroupStep, 21)
	_, _, err = Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.EqualError(t, err, "RolloutInspectionLimit")
}

func TestAggregatePriorityDoesNotInventSiblingLifecycleGate(t *testing.T) {
	v := coordinated(ome.CoordinationPhaseIdle)
	v.Spec.Decoder = &ome.DecoderSpec{}
	v.Spec.Rollout.Groups = append(v.Spec.Rollout.Groups, ome.RolloutGroup{Components: []ome.ComponentType{ome.DecoderComponent}})
	v.Status.Components[ome.DecoderComponent] = ome.ComponentStatusSpec{}
	// The producer collapses consecutive one-member blue-green groups into
	// one Sequential coordination record; two independent records are invalid.
	v.Status.RolloutCoordination.Groups = []ome.RolloutCoordinationGroupStatus{{Name: "0", Components: []ome.ComponentType{ome.EngineComponent, ome.DecoderComponent}, Order: []ome.ComponentType{ome.EngineComponent, ome.DecoderComponent}, CurrentComponent: ome.EngineComponent, Policy: ome.CoordinationPolicySequential, Phase: ome.CoordinationPhaseFailed}}
	d, o, err := Evaluate(v, report.WaitRequestedRolloutFailed, now)
	require.NoError(t, err)
	require.Equal(t, "Valid", o.Validity, "%+v", o)
	require.True(t, d.Matched)
	require.Equal(t, report.RolloutStateFailed, o.Summary.ReportedState)
	d, _, err = Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.False(t, d.Matched)
}

func TestRolloutPermutationAndImmutableObservation(t *testing.T) {
	v := independent()
	v.Status.Conditions = duckv1.Conditions{{Type: apis.ConditionReady, Status: corev1.ConditionTrue}, {Type: apis.ConditionType("Other"), Status: corev1.ConditionFalse}}
	before := v.DeepCopy()
	_, one, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.Equal(t, before, v)
	slices.Reverse(v.Status.Conditions)
	_, two, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.Equal(t, one, two)
	if len(one.Issues) > 0 {
		one.Issues[0].Code = report.RolloutIssueStatusMalformed
	}
	one.Warnings = append(one.Warnings, "InvalidPinnedRun")
	_, again, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
	require.NoError(t, err)
	require.Equal(t, two, again)
}

// IRStatusToComponentStatus copies IR observed generation and conditions 1:1.
// The IR controller stamps its own generation, not the ISVC's generation.
func TestCopiedIRConditionsUseOnlyTheirLifecycleGenerationDomain(t *testing.T) {
	for _, own := range []int64{6, 8} {
		for _, parent := range []int64{0, 7, 8, 9} {
			for _, global := range []int64{0, 6, 7, 8} {
				t.Run(fmt.Sprintf("own%d-parent%d-global%d", own, parent, global), func(t *testing.T) {
					v := independent()
					v.Generation = parent
					v.Status.ObservedGeneration = global
					ir := &ome.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Generation: own}, Status: ome.InferenceReplicaStatus{ObservedGeneration: own, CurrentRevision: "chat-engine-aaaaaaaa", UpdateRevision: "chat-engine-aaaaaaaa", Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: own, LastTransitionTime: metav1.NewTime(now.Add(-time.Second))}}}}
					component := v.Status.Components[ome.EngineComponent]
					component.Lifecycle = &ome.LifecycleStatus{ObservedGeneration: ir.Status.ObservedGeneration, CurrentRevision: ir.Status.CurrentRevision, UpdateRevision: ir.Status.UpdateRevision, Conditions: append([]metav1.Condition(nil), ir.Status.Conditions...)}
					v.Status.Components[ome.EngineComponent] = component
					projected, err := rolloutprojection.Project(v, report.ClockFunc(func() time.Time { return now }))
					require.NoError(t, err)
					require.Equal(t, report.RolloutStateSucceeded, projected.Content.Summary.ReportedState)
					d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
					require.NoError(t, err)
					require.True(t, d.Matched, "own=%d parent=%d global=%d", own, parent, global)
					require.Equal(t, "Valid", o.Validity)
					require.NotContains(t, o.Warnings, report.WaitRolloutWarning("StaleReportedCondition"), "own=%d parent=%d global=%d", own, parent, global)
					require.Equal(t, report.RolloutStateUnknown, o.Summary.State)
					require.Equal(t, report.RolloutEpochUnverifiable, o.Summary.Epoch)
				})
			}
		}
	}
}

func TestSameLifecycleGenerationOldAndFutureDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		condition      int64
		matched, stale bool
	}{{7, true, true}, {8, true, false}, {9, false, false}, {-1, false, false}} {
		t.Run(fmt.Sprintf("condition%d", tc.condition), func(t *testing.T) {
			v := independent()
			v.Generation = 100
			l := v.Status.Components[ome.EngineComponent].Lifecycle
			l.ObservedGeneration = 8
			l.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: tc.condition, LastTransitionTime: metav1.NewTime(now.Add(-time.Second))}}
			d, o, err := Evaluate(v, report.WaitRequestedRolloutStable, now)
			require.NoError(t, err)
			require.Equal(t, tc.matched, d.Matched)
			if tc.stale {
				require.Contains(t, o.Warnings, report.WaitRolloutWarning("StaleReportedCondition"))
			} else {
				require.NotContains(t, o.Warnings, report.WaitRolloutWarning("StaleReportedCondition"))
			}
			if !tc.matched {
				require.Equal(t, "Invalid", o.Validity)
			}
		})
	}
}
