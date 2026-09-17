package status

import (
	"bytes"
	"encoding/json"
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
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/rolloutprojection"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

var statusClock = r.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) })

func typedISVC() *ome.InferenceService {
	return &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "private-uid", ResourceVersion: "private-rv", Generation: 4}}
}
func observedReport(v *ome.InferenceService) *report {
	return &report{ISVC: v, Pods: map[ome.ComponentType][]corev1.Pod{}, PodObservation: r.StatusCollection{State: "Reported"}, EventObservation: r.StatusCollection{State: "Reported"}}
}
func condition(status corev1.ConditionStatus) apis.Condition {
	return apis.Condition{Type: apis.ConditionReady, Status: status, Reason: "Waiting", Message: "ordinary operational reason"}
}

func TestProjectStatusReadyUsesWholeConditionInspection(t *testing.T) {
	for _, tc := range []struct {
		name             string
		conditions       []apis.Condition
		status, validity string
	}{
		{"absent", nil, "NotRecorded", "Unavailable"},
		{"true", []apis.Condition{condition(corev1.ConditionTrue)}, "True", "Valid"},
		{"false", []apis.Condition{condition(corev1.ConditionFalse)}, "False", "Valid"},
		{"unknown", []apis.Condition{condition(corev1.ConditionUnknown)}, "Unknown", "Valid"},
		{"conflict", []apis.Condition{condition(corev1.ConditionTrue), condition(corev1.ConditionFalse)}, "NotRecorded", "Invalid"},
		{"malformed", []apis.Condition{condition("HOSTILE")}, "NotRecorded", "Invalid"},
		{"oversized", make([]apis.Condition, 65), "NotRecorded", "Invalid"},
		{"future", func() []apis.Condition {
			c := condition(corev1.ConditionTrue)
			c.LastTransitionTime = apis.VolatileTime{Inner: metav1.NewTime(statusClock.Now().Add(time.Second))}
			return []apis.Condition{c}
		}(), "True", "Invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := typedISVC()
			v.Status.Conditions = tc.conditions
			got, err := projectStatus(observedReport(v), statusClock)
			require.NoError(t, err)
			require.Equal(t, tc.status, string(got.Content.Ready.Status))
			require.Equal(t, tc.validity, string(got.Content.Ready.Validity))
			require.Equal(t, "Unverifiable", got.Content.GenerationFreshness)
		})
	}
	_, err := projectStatus(nil, statusClock)
	require.Error(t, err)
	v := typedISVC()
	v.ResourceVersion = ""
	_, err = projectStatus(observedReport(v), statusClock)
	require.Error(t, err)
}

func TestProjectStatusCanonicalSecondaryOnlyCanary(t *testing.T) {
	v := typedISVC()
	mode := constants.OMENative
	v.Spec.DeploymentMode = &mode
	v.Spec.Engine = &ome.EngineSpec{}
	v.Spec.Router = &ome.RouterSpec{}
	g := ome.RolloutGroup{Components: []ome.ComponentType{ome.RouterComponent, ome.EngineComponent}, Canary: &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Capacity: intstr.FromString("25%"), Traffic: 10, Pause: &ome.RolloutPause{}}, {Capacity: intstr.FromString("100%"), Traffic: 100}}}}
	v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{g}}
	digest, err := rolloutpolicy.ProgressionDigest(&g)
	require.NoError(t, err)
	now := statusClock.Now()
	v.Status.Rollout = &ome.RolloutStatus{ActiveRun: &ome.RolloutRun{RunID: "chat-0123456789ab", OpenedAt: metav1.NewTime(now.Add(-time.Minute)), PinnedAt: metav1.NewTime(now.Add(-30 * time.Second)), Plan: ome.RolloutRunPlan{Groups: []ome.RolloutRunGroup{{Source: ome.RolloutPlanSourceInline, PortableDigest: digest, Group: g}}}, TargetRevisions: []ome.RolloutRunTarget{{Component: ome.RouterComponent, Revision: "aaaaaaaa", StableRevision: "aaaaaaaa"}, {Component: ome.EngineComponent, Revision: "bbbbbbbb", StableRevision: "cccccccc"}}}}
	entered := metav1.NewTime(now.Add(-20 * time.Second))
	v.Status.Canary = &ome.CanaryStatus{StableRevisionHash: "aaaaaaaa", CanaryRevisionHash: "aaaaaaaa", TargetID: "ct1:" + rolloutpolicy.ShortHash([]byte("engine=bbbbbbbb;router=aaaaaaaa")), StepEnteredTime: &entered, CurrentStep: 0, ObservedTrafficWeight: 10}
	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {}, ome.RouterComponent: {RolloutPhase: ome.RolloutPhasePaused, Traffic: []ome.ComponentTrafficTarget{{RevisionName: "chat-router-rev-aaaaaaaa", Percent: 100}}}}
	expected, err := rolloutprojection.Project(v, statusClock)
	require.NoError(t, err)
	require.Equal(t, r.RolloutState("Paused"), expected.Content.Summary.ReportedState)
	for _, issue := range expected.Content.Issues {
		require.Equal(t, r.RolloutIssueEpochUnverifiable, issue.Code)
	}
	got, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, expected.Content.Summary, got.Content.Rollout.Summary)
	require.Equal(t, r.StatusReadyState("NotRecorded"), got.Content.Ready.Status)
}

func TestProjectStatusGlobalGenerationAlwaysAdvisoryAndOptionalUnavailableDistinct(t *testing.T) {
	for _, generation := range []int64{0, 3, 4, 5} {
		v := typedISVC()
		v.Status.ObservedGeneration = generation
		v.Status.Conditions = []apis.Condition{condition(corev1.ConditionTrue)}
		snapshot := observedReport(v)
		snapshot.PodObservation = r.StatusCollection{State: "Unavailable", Reason: "Forbidden"}
		got, err := projectStatus(snapshot, statusClock)
		require.NoError(t, err)
		require.Equal(t, generation, got.Content.ObservedGeneration)
		require.Equal(t, "Unverifiable", got.Content.GenerationFreshness)
		require.Equal(t, r.StatusCollectionState("Unavailable"), got.Content.Pods.State)
		require.Equal(t, r.StatusSourceReason("Forbidden"), got.Content.Pods.Reason)
		require.Equal(t, r.StatusCollectionState("Reported"), got.Content.Events.State)
	}
}

func TestProjectStatusReusesCanonicalRolloutSummary(t *testing.T) {
	for _, phase := range []ome.RolloutPhase{ome.RolloutPhasePaused, ome.RolloutPhasePromoting, ome.RolloutPhaseRollingBack, ome.RolloutPhaseRolledBack} {
		v := typedISVC()
		v.Spec.Engine = &ome.EngineSpec{}
		mode := constants.OMENative
		v.Spec.DeploymentMode = &mode
		v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Components: []ome.ComponentType{ome.EngineComponent}, Canary: &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Capacity: intstr.FromString("50%"), Traffic: 50}, {Capacity: intstr.FromString("100%"), Traffic: 100}}}}}}
		entered := metav1.NewTime(statusClock.Now().Add(-time.Minute))
		v.Status.Canary = &ome.CanaryStatus{CanaryRevisionHash: "bbbbbbbb", StableRevisionHash: "aaaaaaaa", StepEnteredTime: &entered, CurrentStep: 0, ObservedTrafficWeight: 50}
		traffic := []ome.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 50}, {RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 50}}
		if phase == ome.RolloutPhasePaused {
			v.Spec.Rollout.Groups[0].Canary.Steps[0].Pause = &ome.RolloutPause{}
		}
		if phase == ome.RolloutPhasePromoting {
			v.Status.Canary.CurrentStep = 1
			v.Status.Canary.ObservedTrafficWeight = 100
			traffic = []ome.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 100}}
		}
		if phase == ome.RolloutPhaseRollingBack || phase == ome.RolloutPhaseRolledBack {
			v.Status.Canary.ObservedTrafficWeight = 0
			v.Status.Canary.RolledBackRevisionHash = "bbbbbbbb"
			traffic = []ome.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 100}}
		}
		v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {RolloutPhase: phase, LatestRolledoutRevision: "chat-engine-rev-aaaaaaaa", LatestReadyRevision: "chat-engine-rev-bbbbbbbb", Traffic: traffic}}
		expected, err := rolloutprojection.Project(v, statusClock)
		require.NoError(t, err)
		require.NotEqual(t, r.RolloutState("Unknown"), expected.Content.Summary.ReportedState)
		got, err := projectStatus(observedReport(v), statusClock)
		require.NoError(t, err)
		require.Equal(t, expected.Content.Summary, got.Content.Rollout.Summary)
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		for _, private := range []string{"private-uid", "private-rv", "StepEnteredTime", "annotations"} {
			require.NotContains(t, string(encoded), private)
		}
	}
}

func TestProjectStatusComposesParentReportedAutoscaling(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &ome.EngineSpec{}
	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{
		ome.EngineComponent: {
			Autoscaler: &ome.ComponentAutoscalerStatus{
				Class: ome.AutoscalerHPA, ManagedBy: ome.AutoscalerManagedByOME,
				SpecSource: "isvc", CurrentReplicas: 2, DesiredReplicas: 3,
				Conditions: []metav1.Condition{{Type: "ScalingActive", Status: metav1.ConditionTrue,
					Reason: "Active", Message: "secret scaler message", LastTransitionTime: metav1.NewTime(statusClock.Now())}},
			},
			ScaleTargetRef: &ome.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine"},
		},
	}
	got, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	data, err := json.Marshal(got)
	require.NoError(t, err)
	require.Contains(t, string(data), `"autoscale"`)
	require.NotContains(t, string(data), "secret scaler message")
	var table bytes.Buffer
	require.NoError(t, got.Table().Write(&table))
	require.Contains(t, table.String(), "Autoscaling")
	require.Contains(t, table.String(), "2->3")
	require.Equal(t, r.AutoscaleStateReported, got.Content.Autoscale.Summary.State)
	require.Equal(t, r.EvidenceReported, got.Content.Autoscale.Evidence)
	require.Equal(t, r.AutoscaleClassHPA, got.Content.Autoscale.Components[0].Class)
	require.Equal(t, r.AutoscaleReplicasReported, got.Content.Autoscale.Components[0].ReplicaEvidence)
}

func TestProjectStatusAutoscaleUnavailableAndPartialAreDistinct(t *testing.T) {
	v := typedISVC()
	got, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, r.AutoscaleStateUnavailable, got.Content.Autoscale.Summary.State)
	require.Equal(t, r.EvidenceUnavailable, got.Content.Autoscale.Evidence)
	require.Empty(t, got.Content.Autoscale.Components)

	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {
		Autoscaler: &ome.ComponentAutoscalerStatus{Class: ome.AutoscalerHPA,
			ManagedBy: ome.AutoscalerManagedByOME, SpecSource: "isvc", CurrentReplicas: 2},
		ScaleTargetRef: &ome.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine"},
	}}
	got, err = projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, r.AutoscaleStatePartial, got.Content.Autoscale.Summary.State)
	require.Equal(t, r.EvidenceReported, got.Content.Autoscale.Evidence)
	require.Equal(t, r.AutoscaleReplicasAmbiguous, got.Content.Autoscale.Components[0].ReplicaEvidence)
	require.Contains(t, got.Content.Autoscale.Issues, r.AutoscaleIssue{Code: r.AutoscaleIssueReplicaEvidenceAmbiguous, Component: r.RuntimeComponentEngine})
	var wide bytes.Buffer
	require.NoError(t, got.WideTable().Write(&wide))
	require.Contains(t, wide.String(), "Scale conditions")
	require.Contains(t, wide.String(), "engine / NotReported")
}

func TestProjectStatusAutoscaleConditionPreflightRejectsHugePayload(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*ome.InferenceService)
	}{
		{"huge condition message", func(v *ome.InferenceService) {
			v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {
				Autoscaler: &ome.ComponentAutoscalerStatus{Conditions: []metav1.Condition{{Type: "ScalingActive", Message: strings.Repeat("x", 4097)}}},
			}}
		}},
		{"too many conditions", func(v *ome.InferenceService) {
			v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {
				Autoscaler: &ome.ComponentAutoscalerStatus{Conditions: make([]metav1.Condition, 65)},
			}}
		}},
		{"too many components", func(v *ome.InferenceService) {
			v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{
				ome.EngineComponent: {}, ome.DecoderComponent: {}, ome.RouterComponent: {}, "unknown": {},
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := typedISVC()
			tc.set(v)
			got, err := projectStatus(observedReport(v), statusClock)
			require.NoError(t, err)
			require.Equal(t, r.AutoscaleStateUnavailable, got.Content.Autoscale.Summary.State)
			require.Equal(t, r.EvidenceUnavailable, got.Content.Autoscale.Evidence)
			require.Empty(t, got.Content.Autoscale.Components)
			require.Contains(t, got.Content.Issues, r.StatusIssueCode("AutoscaleUnavailable"))
			require.Contains(t, got.Content.Issues, r.StatusIssueCode("CollectionLimitExceeded"))
		})
	}
}

func statusTestPod(name string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", UID: types.UID("uid-" + name), Labels: map[string]string{constants.InferenceServiceLabel: "chat", constants.OMEComponentLabel: "engine"}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{RestartCount: 2}}}}
}

func TestProjectStatusBoundedPodsAndWarningEvents(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &ome.EngineSpec{}
	v.Status.Conditions = []apis.Condition{{Type: "EngineReady", Status: corev1.ConditionFalse}}
	p := statusTestPod("chat-engine-0")
	bad := statusTestPod("hostile")
	bad.Namespace = "other"
	s := observedReport(v)
	s.Pods[ome.EngineComponent] = []corev1.Pod{p, bad}
	s.Events = []corev1.Event{{ObjectMeta: metav1.ObjectMeta{Name: "e", Namespace: "prod"}, Type: corev1.EventTypeWarning, InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: "prod", Name: p.Name, UID: p.UID}, Reason: "BackOff", Message: "ordinary reason", Count: 2, LastTimestamp: metav1.NewTime(statusClock.Now().Add(-time.Minute))}, {ObjectMeta: metav1.ObjectMeta{Namespace: "other"}, Type: corev1.EventTypeWarning, InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: p.Name, UID: p.UID}, Reason: "HOSTILE"}}
	got, err := projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	require.Equal(t, r.StatusReadyState("False"), got.Content.Components[0].Ready)
	require.Equal(t, 1, got.Content.Components[0].Pods.Total)
	require.Equal(t, 1, got.Content.Components[0].Pods.Ready)
	require.Equal(t, int64(2), got.Content.Components[0].Pods.Restarts)
	require.Len(t, got.Content.RecentEvents, 1)
	require.Equal(t, "BackOff", got.Content.RecentEvents[0].Reason)
	require.Contains(t, got.Content.Issues, r.StatusIssueCode("PodIdentityRejected"))
	s.Pods[ome.EngineComponent] = []corev1.Pod{bad, p}
	s.Events[0], s.Events[1] = s.Events[1], s.Events[0]
	reverse, err := projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Equal(t, got, reverse)
	p.Status.ContainerStatuses[0].RestartCount = -1
	s.Pods[ome.EngineComponent] = []corev1.Pod{p}
	s.Events = nil
	got, err = projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Zero(t, got.Content.Components[0].Pods.Total)
	require.Contains(t, got.Content.Issues, r.StatusIssueCode("PodMalformed"))
}

func TestProjectStatusRejectsWholeOversizedNestedCollections(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &ome.EngineSpec{}
	v.Spec.Rollout = &ome.RolloutSpec{Groups: make([]ome.RolloutGroup, 4)}
	s := observedReport(v)
	s.Pods[ome.EngineComponent] = make([]corev1.Pod, 1001)
	s.Events = make([]corev1.Event, 101)
	got, err := projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Contains(t, got.Content.Issues, r.StatusIssueCode("CollectionLimitExceeded"))
	require.Contains(t, got.Content.Issues, r.StatusIssueCode("RolloutUnavailable"))
	require.Empty(t, got.Content.RecentEvents)
	require.Zero(t, got.Content.Components[0].Pods.Total)
}

func TestProjectStatusEventChronologyAndSeriesCount(t *testing.T) {
	p := statusTestPod("chat-engine-0")
	s := observedReport(typedISVC())
	s.Pods[ome.EngineComponent] = []corev1.Pod{p}
	event := corev1.Event{ObjectMeta: metav1.ObjectMeta{Namespace: "prod"}, Type: corev1.EventTypeWarning, InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: p.Name, UID: p.UID}, Reason: "BackOff", Count: 1, EventTime: metav1.NewMicroTime(statusClock.Now().Add(-time.Minute)), Series: &corev1.EventSeries{Count: 7, LastObservedTime: metav1.NewMicroTime(statusClock.Now().Add(-time.Second))}}
	s.Events = []corev1.Event{event}
	got, err := projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Len(t, got.Content.RecentEvents, 1)
	require.Equal(t, int32(7), got.Content.RecentEvents[0].Count)
	for _, mutate := range []func(*corev1.Event){func(e *corev1.Event) { e.FirstTimestamp = metav1.NewTime(statusClock.Now().Add(time.Second)) }, func(e *corev1.Event) { e.Series.Count = -1 }, func(e *corev1.Event) { e.Series.LastObservedTime = metav1.NewMicroTime(e.EventTime.Add(-time.Second)) }} {
		copy := *event.DeepCopy()
		mutate(&copy)
		s.Events = []corev1.Event{copy}
		got, err = projectStatus(s, statusClock)
		require.NoError(t, err)
		require.Empty(t, got.Content.RecentEvents)
		require.Contains(t, got.Content.Issues, r.StatusIssueCode("EventMalformed"))
	}
}

func TestProjectStatusRejectsUnsafeUIDAndOversizedRolloutPrivateInput(t *testing.T) {
	s := observedReport(typedISVC())
	s.ISVC.Spec.Engine = &ome.EngineSpec{}
	p := statusTestPod("chat-engine-0")
	p.UID = "selector,injection"
	s.Pods[ome.EngineComponent] = []corev1.Pod{p}
	got, err := projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Zero(t, got.Content.Components[0].Pods.Total)
	require.Contains(t, got.Content.Issues, r.StatusIssueCode("PodIdentityRejected"))
	for _, edit := range []func(*ome.InferenceService){
		func(v *ome.InferenceService) {
			v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Canary: &ome.GroupCanary{Prometheus: &ome.AnalysisPrometheus{Headers: make(map[string]string, 17)}}}}}
			for i := 0; i < 17; i++ {
				v.Spec.Rollout.Groups[0].Canary.Prometheus.Headers[fmt.Sprint(i)] = "ordinary"
			}
		},
		func(v *ome.InferenceService) {
			v.Status.RolloutCoordination = &ome.RolloutCoordinationStatus{Groups: []ome.RolloutCoordinationGroupStatus{{Components: make([]ome.ComponentType, 4)}}}
		},
		func(v *ome.InferenceService) {
			v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Canary: &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Analysis: &ome.RolloutAnalysis{Metrics: []ome.AnalysisMetric{{Query: strings.Repeat("q", 4097)}}}}}}}}}
		},
	} {
		v := typedISVC()
		edit(v)
		got, err := projectStatus(observedReport(v), statusClock)
		require.NoError(t, err)
		require.Contains(t, got.Content.Issues, r.StatusIssueCode("RolloutUnavailable"))
	}
}

func TestProjectStatusPodPhasesConflictAndImmutability(t *testing.T) {
	v := typedISVC()
	v.Spec.Engine = &ome.EngineSpec{}
	v.Spec.Decoder = &ome.DecoderSpec{}
	v.Spec.Router = &ome.RouterSpec{}
	v.Status.Conditions = []apis.Condition{{Type: "DecoderReady", Status: corev1.ConditionTrue}, {Type: "RouterReady", Status: corev1.ConditionFalse}, {Type: "RouterReady", Status: corev1.ConditionTrue}}
	s := observedReport(v)
	for i, phase := range []corev1.PodPhase{corev1.PodRunning, corev1.PodPending, corev1.PodFailed, corev1.PodSucceeded, "unknown"} {
		p := statusTestPod(fmt.Sprintf("chat-engine-%d", i))
		p.Status.Phase = phase
		p.Status.InitContainerStatuses = []corev1.ContainerStatus{{RestartCount: 1}}
		p.Status.EphemeralContainerStatuses = []corev1.ContainerStatus{{RestartCount: 3}}
		if i == 1 {
			stamp := metav1.NewTime(statusClock.Now())
			p.DeletionTimestamp = &stamp
		}
		s.Pods[ome.EngineComponent] = append(s.Pods[ome.EngineComponent], p)
	}
	got, err := projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Len(t, got.Content.Components, 3)
	require.Equal(t, r.StatusPodCounts{Total: 5, Ready: 5, Restarts: 30, Running: 1, Pending: 1, Failed: 1, Succeeded: 1, Unknown: 1, Terminating: 1}, got.Content.Components[0].Pods)
	require.Equal(t, r.StatusReadyState("True"), got.Content.Components[1].Ready)
	require.Equal(t, r.StatusReadyState("NotRecorded"), got.Content.Components[2].Ready)
	require.Equal(t, r.StatusValidity("Invalid"), got.Content.Components[2].Validity)
	slices.Reverse(s.Pods[ome.EngineComponent])
	reverse, err := projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Equal(t, got, reverse)
	s.Pods[ome.EngineComponent][0].Status.ContainerStatuses[0].RestartCount = 100
	require.Equal(t, int64(30), got.Content.Components[0].Pods.Restarts)
	for _, edit := range []func(*corev1.Pod){func(p *corev1.Pod) { p.Status.Conditions = make([]corev1.PodCondition, 65) }, func(p *corev1.Pod) {
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse})
	}, func(p *corev1.Pod) { p.Status.Conditions[0].Status = "hostile" }, func(p *corev1.Pod) { p.UID = "" }, func(p *corev1.Pod) { p.Labels[constants.OMEComponentLabel] = "router" }} {
		p := statusTestPod("chat-engine-0")
		edit(&p)
		s.Pods[ome.EngineComponent] = []corev1.Pod{p}
		got, err = projectStatus(s, statusClock)
		require.NoError(t, err)
		require.Zero(t, got.Content.Components[0].Pods.Total)
	}
	p := statusTestPod("chat-engine-0")
	s.Pods[ome.EngineComponent] = []corev1.Pod{p, p}
	got, err = projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Zero(t, got.Content.Components[0].Pods.Total)
	require.Contains(t, got.Content.Issues, r.StatusIssueCode("PodIdentityRejected"))
	s.PodObservation.State = "Unavailable"
	got, err = projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Equal(t, r.EvidenceUnavailable, got.Content.Components[0].Evidence)
}

func TestProjectStatusEveryNestedRolloutWindow(t *testing.T) {
	for _, edit := range []func(*ome.InferenceService){
		func(v *ome.InferenceService) {
			v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{"a": {}, "b": {}, "c": {}, "d": {}}
		},
		func(v *ome.InferenceService) {
			v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {Traffic: make([]ome.ComponentTrafficTarget, 4)}}
		},
		func(v *ome.InferenceService) {
			v.Status.Canary = &ome.CanaryStatus{MetricResults: make([]ome.AnalysisMetricResult, 11)}
		},
		func(v *ome.InferenceService) {
			v.Status.RolloutCoordination = &ome.RolloutCoordinationStatus{Groups: make([]ome.RolloutCoordinationGroupStatus, 4)}
		},
		func(v *ome.InferenceService) {
			v.Status.Rollout = &ome.RolloutStatus{Groups: make([]ome.RolloutGroupResolution, 4)}
		},
		func(v *ome.InferenceService) {
			v.Status.Rollout = &ome.RolloutStatus{ActiveRun: &ome.RolloutRun{TargetRevisions: make([]ome.RolloutRunTarget, 4)}}
		},
		func(v *ome.InferenceService) {
			v.Status.Rollout = &ome.RolloutStatus{ActiveRun: &ome.RolloutRun{Plan: ome.RolloutRunPlan{Groups: []ome.RolloutRunGroup{{Group: ome.RolloutGroup{Components: make([]ome.ComponentType, 4)}}}}}}
		},
		func(v *ome.InferenceService) {
			v.Status.Rollout = &ome.RolloutStatus{LastRun: &ome.RolloutRunRecord{Groups: make([]ome.RolloutRunProvenance, 4)}}
		},
		func(v *ome.InferenceService) {
			v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Order: make([]ome.ComponentType, 4)}}}
		},
		func(v *ome.InferenceService) {
			v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Canary: &ome.GroupCanary{Steps: make([]ome.RolloutGroupStep, 21)}}}}
		},
		func(v *ome.InferenceService) {
			v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Canary: &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Analysis: &ome.RolloutAnalysis{Metrics: make([]ome.AnalysisMetric, 11)}}}}}}}
		},
	} {
		v := typedISVC()
		edit(v)
		got, err := projectStatus(observedReport(v), statusClock)
		require.NoError(t, err)
		require.Contains(t, got.Content.Issues, r.StatusIssueCode("RolloutUnavailable"))
	}
	s := observedReport(typedISVC())
	s.Pods = map[ome.ComponentType][]corev1.Pod{"a": nil, "b": nil, "c": nil, "d": nil}
	got, err := projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Contains(t, got.Content.Issues, r.StatusIssueCode("CollectionLimitExceeded"))
	s.Pods = map[ome.ComponentType][]corev1.Pod{"unknown": {statusTestPod("hostile")}}
	got, err = projectStatus(s, statusClock)
	require.NoError(t, err)
	require.Contains(t, got.Content.Issues, r.StatusIssueCode("UnsupportedComponent"))
	_, err = projectStatus(observedReport(typedISVC()), nil)
	require.NoError(t, err)
}
