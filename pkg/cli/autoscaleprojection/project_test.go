package autoscaleprojection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestProjectRejectsUnsafeInferenceServiceIdentity(t *testing.T) {
	tests := []struct {
		name string
		isvc *omev1beta1.InferenceService
		want error
	}{
		{name: "nil", want: ErrInferenceServiceRequired},
		{name: "name omitted", isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", UID: "uid"}}, want: ErrInferenceServiceNameRequired},
		{name: "namespace omitted", isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", UID: "uid"}}, want: ErrInferenceServiceNamespaceRequired},
		{name: "uid omitted", isvc: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod"}}, want: ErrInferenceServiceUIDRequired},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Project(test.isvc, fixedClock())
			assert.ErrorIs(t, err, test.want)
			assert.Equal(t, reportv1alpha1.AutoscaleStatusReport{}, got)
		})
	}

	assert.EqualError(t, ErrInferenceServiceRequired, "autoscale projection requires an InferenceService")
	assert.EqualError(t, ErrInferenceServiceNameRequired, "autoscale projection requires an InferenceService name")
	assert.EqualError(t, ErrInferenceServiceNamespaceRequired, "autoscale projection requires an InferenceService namespace")
	assert.EqualError(t, ErrInferenceServiceUIDRequired, "autoscale projection requires an InferenceService UID")
}

func TestProjectReportedHPAUsesOnlyParentReportedEvidence(t *testing.T) {
	lastScale := metav1.NewTime(time.Date(2026, time.August, 31, 18, 20, 0, 0, time.FixedZone("secret-zone", -7*60*60)))
	transition := metav1.NewTime(time.Date(2026, time.August, 31, 18, 15, 0, 0, time.FixedZone("secret-zone", -7*60*60)))
	isvc := baseInferenceService()
	isvc.Labels = map[string]string{"SECRET_OBJECT_LABEL": "SECRET_OBJECT_LABEL_VALUE"}
	isvc.OwnerReferences = []metav1.OwnerReference{{Name: "SECRET_OWNER_REFERENCE"}}
	isvc.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "SECRET_FIELD_MANAGER"}}
	isvc.Spec.Engine = &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{
		Labels:      map[string]string{"SECRET_LABEL_KEY": "SECRET_LABEL_VALUE"},
		Annotations: map[string]string{"SECRET_SPEC_ANNOTATION": "SECRET_SPEC_VALUE"},
		Autoscaler: &omev1beta1.ComponentAutoscaler{
			Class: omev1beta1.AutoscalerKEDA,
			Keda: &omev1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{
				Type: "SECRET_TRIGGER", Metadata: map[string]string{"token": "SECRET_TRIGGER_TOKEN"},
			}}},
			HPA: &omev1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
				Type: autoscalingv2.ExternalMetricSourceType,
				External: &autoscalingv2.ExternalMetricSource{
					Metric: autoscalingv2.MetricIdentifier{Name: "SECRET_METRIC"},
				},
			}}},
		},
	}}
	isvc.Status.ObservedGeneration = 999
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			RolloutPhase:              omev1beta1.RolloutPhase("SECRET_ROLLOUT_PHASE"),
			LatestReadyRevision:       "SECRET_READY_REVISION",
			LatestRolledoutRevision:   "SECRET_ROLLEDOUT_REVISION",
			PreviousRolledoutRevision: "SECRET_PREVIOUS_REVISION",
			Traffic: []omev1beta1.ComponentTrafficTarget{{
				RevisionName: "SECRET_TRAFFIC_REVISION", Tag: "SECRET_TRAFFIC_TAG", Percent: 100,
			}},
			Autoscaler: &omev1beta1.ComponentAutoscalerStatus{
				Class: omev1beta1.AutoscalerHPA, ManagedBy: omev1beta1.AutoscalerManagedByOME,
				SpecSource: "default", CurrentReplicas: 2, DesiredReplicas: 3,
				LastScaleTime: &lastScale,
				Conditions: []metav1.Condition{
					{Type: "ScalingActive", Status: metav1.ConditionTrue, Reason: "SECRET_REASON", Message: "SECRET_MESSAGE", ObservedGeneration: 998, LastTransitionTime: transition},
					{Type: "AbleToScale", Status: metav1.ConditionTrue, Reason: "SECRET_REASON_2", Message: "SECRET_MESSAGE_2", ObservedGeneration: 997, LastTransitionTime: transition},
				},
			},
			ScaleTargetRef: &omev1beta1.ScaleTargetRef{
				APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine",
			},
		},
	}

	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)

	current, desired := int32(2), int32(3)
	assert.Equal(t, reportv1alpha1.AutoscaleSummary{State: reportv1alpha1.AutoscaleStateReported}, got.Content.Summary)
	assert.Equal(t, []reportv1alpha1.AutoscaleSourceReference{{
		Kind: reportv1alpha1.AutoscaleSourceInferenceService, Namespace: "prod", Name: "chat",
		UID: "isvc-uid", Generation: 7, Evidence: reportv1alpha1.EvidenceReported,
		CollectedAt: fixedClock().Now(),
	}}, got.Sources)
	assert.Equal(t, []reportv1alpha1.AutoscaleComponentStatus{{
		Type: reportv1alpha1.RuntimeComponentEngine, State: reportv1alpha1.AutoscaleComponentReported,
		Class: reportv1alpha1.AutoscaleClassHPA, ManagedBy: reportv1alpha1.AutoscaleManagedByOME,
		SpecSource: reportv1alpha1.AutoscaleSpecSourceDefault,
		Target: reportv1alpha1.AutoscaleTarget{
			State: reportv1alpha1.AutoscaleTargetReported, APIVersion: "ome.io/v1beta1",
			Kind: reportv1alpha1.AutoscaleTargetInferenceReplica, Namespace: "prod", Name: "chat-engine",
		},
		Replicas: reportv1alpha1.AutoscaleReplicaStatus{
			State: reportv1alpha1.AutoscaleReplicasReported, CurrentReplicas: &current,
			DesiredReplicas: &desired, LastScaleTime: ptrTime(lastScale.Time.UTC()),
		},
		Conditions: reportv1alpha1.AutoscaleConditionsStatus{
			State: reportv1alpha1.AutoscaleConditionsReported,
			Items: []reportv1alpha1.AutoscaleCondition{
				{Type: reportv1alpha1.AutoscaleConditionAbleToScale, Status: reportv1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition.Time.UTC()},
				{Type: reportv1alpha1.AutoscaleConditionScalingActive, Status: reportv1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition.Time.UTC()},
			},
		},
	}}, got.Content.Components)
	assert.Empty(t, got.Content.Issues)
	assert.Empty(t, got.Warnings)

	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, got))
		for _, secret := range []string{
			"SECRET_RESOURCE_VERSION", "SECRET_ANNOTATION", "SECRET_STATUS_ANNOTATION",
			"SECRET_REASON", "SECRET_MESSAGE", "observedGeneration", "resourceVersion",
			"SECRET_LABEL", "SECRET_SPEC", "SECRET_TRIGGER", "SECRET_TRIGGER_TOKEN",
			"SECRET_OBJECT_LABEL", "SECRET_OWNER_REFERENCE", "SECRET_FIELD_MANAGER",
			"SECRET_METRIC", "metrics", "behavior",
			"SECRET_ROLLOUT", "SECRET_READY", "SECRET_PREVIOUS", "SECRET_TRAFFIC",
			"freshness", "syncToken", "lastRuntimeSyncToken", "continuationToken",
		} {
			assert.NotContains(t, output.String(), secret, "format %s", format)
		}
	}
}

func TestProjectNoComponentEvidenceIsUnavailable(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.Components = nil

	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)

	assert.Equal(t, reportv1alpha1.AutoscaleStateUnavailable, got.Content.Summary.State)
	assert.NotNil(t, got.Content.Components)
	assert.Empty(t, got.Content.Components)
	assert.NotNil(t, got.Content.Issues)
	assert.Empty(t, got.Content.Issues)
	assert.Equal(t, []reportv1alpha1.AutoscaleWarning{{Code: reportv1alpha1.AutoscaleWarningPartialData}}, got.Warnings)
}

func TestProjectKnownComponentWithoutAutoscalerIsNotReported(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			ScaleTargetRef: &omev1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine"},
		},
	}

	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AutoscaleComponentNotReported, component.State)
	assert.Equal(t, reportv1alpha1.AutoscaleClassUnknown, component.Class)
	assert.Equal(t, reportv1alpha1.AutoscaleManagedByUnknown, component.ManagedBy)
	assert.Equal(t, reportv1alpha1.AutoscaleSpecSourceUnknown, component.SpecSource)
	assert.Equal(t, reportv1alpha1.AutoscaleTargetReported, component.Target.State)
	assert.Equal(t, reportv1alpha1.AutoscaleReplicasNotReported, component.Replicas.State)
	assert.NotNil(t, component.Conditions.Items)
	assert.Equal(t, reportv1alpha1.AutoscaleConditionsNotReported, component.Conditions.State)
	assert.Equal(t, []reportv1alpha1.AutoscaleIssue{{Code: reportv1alpha1.AutoscaleIssueAutoscalerNotReported, Component: reportv1alpha1.RuntimeComponentEngine}}, got.Content.Issues)
	assert.Equal(t, reportv1alpha1.AutoscaleStateUnavailable, got.Content.Summary.State)
}

func TestProjectMalformedTargetCannotBeMaskedByMissingAutoscaler(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.EngineComponent: {
			ScaleTargetRef: &omev1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment"},
		},
	}

	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, got.Content.Components[0].State)
	assert.Equal(t, reportv1alpha1.AutoscaleStateInvalid, got.Content.Summary.State)
	assert.Equal(t, []reportv1alpha1.AutoscaleIssueCode{
		reportv1alpha1.AutoscaleIssueAutoscalerNotReported,
		reportv1alpha1.AutoscaleIssueScaleTargetInvalid,
	}, issueCodes(got.Content.Issues))
}

func TestProjectClassOwnershipMatrix(t *testing.T) {
	tests := []struct {
		name           string
		class          omev1beta1.AutoscalerClass
		managedBy      omev1beta1.AutoscalerManagedBy
		wantClass      reportv1alpha1.AutoscaleClass
		wantManagedBy  reportv1alpha1.AutoscaleManagedBy
		wantReplicas   reportv1alpha1.AutoscaleReplicaState
		wantConditions reportv1alpha1.AutoscaleConditionsState
		wantComponent  reportv1alpha1.AutoscaleComponentState
		wantIssueCodes []reportv1alpha1.AutoscaleIssueCode
	}{
		{name: "HPA", class: omev1beta1.AutoscalerHPA, managedBy: omev1beta1.AutoscalerManagedByOME, wantClass: reportv1alpha1.AutoscaleClassHPA, wantManagedBy: reportv1alpha1.AutoscaleManagedByOME, wantReplicas: reportv1alpha1.AutoscaleReplicasAmbiguous, wantConditions: reportv1alpha1.AutoscaleConditionsNotReported, wantComponent: reportv1alpha1.AutoscaleComponentPartial, wantIssueCodes: []reportv1alpha1.AutoscaleIssueCode{reportv1alpha1.AutoscaleIssueReplicaEvidenceAmbiguous}},
		{name: "KEDA", class: omev1beta1.AutoscalerKEDA, managedBy: omev1beta1.AutoscalerManagedByOME, wantClass: reportv1alpha1.AutoscaleClassKEDA, wantManagedBy: reportv1alpha1.AutoscaleManagedByOME, wantReplicas: reportv1alpha1.AutoscaleReplicasAmbiguous, wantConditions: reportv1alpha1.AutoscaleConditionsNotReported, wantComponent: reportv1alpha1.AutoscaleComponentPartial, wantIssueCodes: []reportv1alpha1.AutoscaleIssueCode{reportv1alpha1.AutoscaleIssueReplicaEvidenceAmbiguous}},
		{name: "External", class: omev1beta1.AutoscalerExternal, managedBy: omev1beta1.AutoscalerManagedByExternal, wantClass: reportv1alpha1.AutoscaleClassExternal, wantManagedBy: reportv1alpha1.AutoscaleManagedByExternal, wantReplicas: reportv1alpha1.AutoscaleReplicasUnavailable, wantConditions: reportv1alpha1.AutoscaleConditionsUnavailable, wantComponent: reportv1alpha1.AutoscaleComponentReported},
		{name: "None", class: omev1beta1.AutoscalerNone, managedBy: omev1beta1.AutoscalerManagedByNone, wantClass: reportv1alpha1.AutoscaleClassNone, wantManagedBy: reportv1alpha1.AutoscaleManagedByNone, wantReplicas: reportv1alpha1.AutoscaleReplicasUnavailable, wantConditions: reportv1alpha1.AutoscaleConditionsUnavailable, wantComponent: reportv1alpha1.AutoscaleComponentReported},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc := baseInferenceService()
			isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
				omev1beta1.EngineComponent: {
					Autoscaler:     &omev1beta1.ComponentAutoscalerStatus{Class: test.class, ManagedBy: test.managedBy, SpecSource: "isvc"},
					ScaleTargetRef: &omev1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine"},
				},
			}

			got, err := Project(isvc, fixedClock())
			require.NoError(t, err)
			require.Len(t, got.Content.Components, 1)
			component := got.Content.Components[0]
			assert.Equal(t, test.wantClass, component.Class)
			assert.Equal(t, test.wantManagedBy, component.ManagedBy)
			assert.Equal(t, test.wantReplicas, component.Replicas.State)
			assert.Equal(t, test.wantConditions, component.Conditions.State)
			assert.Equal(t, test.wantComponent, component.State)
			assert.Equal(t, append([]reportv1alpha1.AutoscaleIssueCode{}, test.wantIssueCodes...), issueCodes(got.Content.Issues))
		})
	}
}

func TestProjectReplicaEvidenceStates(t *testing.T) {
	tests := []struct {
		name          string
		current       int32
		desired       int32
		lastScaleTime *metav1.Time
		wantState     reportv1alpha1.AutoscaleReplicaState
		wantCurrent   *int32
		wantDesired   *int32
		wantLast      *time.Time
		wantIssue     reportv1alpha1.AutoscaleIssueCode
	}{
		{name: "current zero is explicitly preserved but ambiguous", current: 0, desired: 3, wantState: reportv1alpha1.AutoscaleReplicasAmbiguous, wantCurrent: ptrInt32(0), wantDesired: ptrInt32(3), wantIssue: reportv1alpha1.AutoscaleIssueReplicaEvidenceAmbiguous},
		{name: "desired zero is explicitly preserved but ambiguous", current: 2, desired: 0, wantState: reportv1alpha1.AutoscaleReplicasAmbiguous, wantCurrent: ptrInt32(2), wantDesired: ptrInt32(0), wantIssue: reportv1alpha1.AutoscaleIssueReplicaEvidenceAmbiguous},
		{name: "negative current is invalid and omitted", current: -1, desired: 3, wantState: reportv1alpha1.AutoscaleReplicasInvalid, wantIssue: reportv1alpha1.AutoscaleIssueReplicaEvidenceInvalid},
		{name: "negative desired is invalid and omitted", current: 2, desired: -1, wantState: reportv1alpha1.AutoscaleReplicasInvalid, wantIssue: reportv1alpha1.AutoscaleIssueReplicaEvidenceInvalid},
		{name: "zero last-scale timestamp is invalid and omitted", current: 2, desired: 3, lastScaleTime: &metav1.Time{}, wantState: reportv1alpha1.AutoscaleReplicasInvalid, wantIssue: reportv1alpha1.AutoscaleIssueReplicaEvidenceInvalid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, &omev1beta1.ComponentAutoscalerStatus{
				Class: omev1beta1.AutoscalerHPA, ManagedBy: omev1beta1.AutoscalerManagedByOME,
				SpecSource: "runtime", CurrentReplicas: test.current, DesiredReplicas: test.desired,
				LastScaleTime: test.lastScaleTime, Conditions: []metav1.Condition{validHPACondition("AbleToScale")},
			})

			got, err := Project(isvc, fixedClock())
			require.NoError(t, err)
			component := got.Content.Components[0]
			assert.Equal(t, test.wantState, component.Replicas.State)
			assert.Equal(t, test.wantCurrent, component.Replicas.CurrentReplicas)
			assert.Equal(t, test.wantDesired, component.Replicas.DesiredReplicas)
			assert.Equal(t, test.wantLast, component.Replicas.LastScaleTime)
			assert.Contains(t, issueCodes(got.Content.Issues), test.wantIssue)
			if test.wantState == reportv1alpha1.AutoscaleReplicasInvalid {
				assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, component.State)
			} else {
				assert.Equal(t, reportv1alpha1.AutoscaleComponentPartial, component.State)
			}
		})
	}
}

func TestProjectValidatesTargetsWithoutReadingOrGuessing(t *testing.T) {
	tests := []struct {
		name      string
		target    *omev1beta1.ScaleTargetRef
		wantState reportv1alpha1.AutoscaleTargetState
		wantKind  reportv1alpha1.AutoscaleTargetKind
		wantIssue reportv1alpha1.AutoscaleIssueCode
	}{
		{name: "deployment", target: &omev1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-engine"}, wantState: reportv1alpha1.AutoscaleTargetReported, wantKind: reportv1alpha1.AutoscaleTargetDeployment},
		{name: "inference replica", target: &omev1beta1.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "chat-engine"}, wantState: reportv1alpha1.AutoscaleTargetReported, wantKind: reportv1alpha1.AutoscaleTargetInferenceReplica},
		{name: "missing", wantState: reportv1alpha1.AutoscaleTargetNotReported, wantIssue: reportv1alpha1.AutoscaleIssueScaleTargetNotReported},
		{name: "partial", target: &omev1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment"}, wantState: reportv1alpha1.AutoscaleTargetInvalid, wantIssue: reportv1alpha1.AutoscaleIssueScaleTargetInvalid},
		{name: "wrong group", target: &omev1beta1.ScaleTargetRef{APIVersion: "evil.example/v1", Kind: "Deployment", Name: "chat-engine"}, wantState: reportv1alpha1.AutoscaleTargetInvalid, wantIssue: reportv1alpha1.AutoscaleIssueScaleTargetInvalid},
		{name: "wrong kind", target: &omev1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "chat-engine"}, wantState: reportv1alpha1.AutoscaleTargetInvalid, wantIssue: reportv1alpha1.AutoscaleIssueScaleTargetInvalid},
		{name: "invalid name", target: &omev1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "SECRET/target"}, wantState: reportv1alpha1.AutoscaleTargetInvalid, wantIssue: reportv1alpha1.AutoscaleIssueScaleTargetInvalid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, reportedHPA())
			component := isvc.Status.Components[omev1beta1.EngineComponent]
			component.ScaleTargetRef = test.target
			isvc.Status.Components[omev1beta1.EngineComponent] = component

			got, err := Project(isvc, fixedClock())
			require.NoError(t, err)
			projected := got.Content.Components[0]
			assert.Equal(t, test.wantState, projected.Target.State)
			assert.Equal(t, test.wantKind, projected.Target.Kind)
			if test.wantState == reportv1alpha1.AutoscaleTargetReported {
				assert.Equal(t, "prod", projected.Target.Namespace)
				assert.Equal(t, "chat-engine", projected.Target.Name)
				assert.NotContains(t, issueCodes(got.Content.Issues), test.wantIssue)
			} else {
				assert.Empty(t, projected.Target.APIVersion)
				assert.Empty(t, projected.Target.Kind)
				assert.Empty(t, projected.Target.Namespace)
				assert.Empty(t, projected.Target.Name)
				assert.Contains(t, issueCodes(got.Content.Issues), test.wantIssue)
			}
		})
	}
}

func TestProjectRejectsClosedEnumAndOwnershipContradictions(t *testing.T) {
	tests := []struct {
		name      string
		status    *omev1beta1.ComponentAutoscalerStatus
		wantClass reportv1alpha1.AutoscaleClass
		wantOwner reportv1alpha1.AutoscaleManagedBy
		wantSpec  reportv1alpha1.AutoscaleSpecSource
		wantIssue reportv1alpha1.AutoscaleIssueCode
	}{
		{name: "unknown class", status: &omev1beta1.ComponentAutoscalerStatus{Class: "SECRET_CLASS", ManagedBy: omev1beta1.AutoscalerManagedByOME, SpecSource: "isvc"}, wantClass: reportv1alpha1.AutoscaleClassUnknown, wantOwner: reportv1alpha1.AutoscaleManagedByOME, wantSpec: reportv1alpha1.AutoscaleSpecSourceISVC, wantIssue: reportv1alpha1.AutoscaleIssueClassInvalid},
		{name: "unknown manager", status: &omev1beta1.ComponentAutoscalerStatus{Class: omev1beta1.AutoscalerHPA, ManagedBy: "SECRET_MANAGER", SpecSource: "isvc"}, wantClass: reportv1alpha1.AutoscaleClassHPA, wantOwner: reportv1alpha1.AutoscaleManagedByUnknown, wantSpec: reportv1alpha1.AutoscaleSpecSourceISVC, wantIssue: reportv1alpha1.AutoscaleIssueManagedByInvalid},
		{name: "unknown spec source", status: &omev1beta1.ComponentAutoscalerStatus{Class: omev1beta1.AutoscalerHPA, ManagedBy: omev1beta1.AutoscalerManagedByOME, SpecSource: "SECRET_SOURCE"}, wantClass: reportv1alpha1.AutoscaleClassHPA, wantOwner: reportv1alpha1.AutoscaleManagedByOME, wantSpec: reportv1alpha1.AutoscaleSpecSourceUnknown, wantIssue: reportv1alpha1.AutoscaleIssueSpecSourceInvalid},
		{name: "ownership mismatch", status: &omev1beta1.ComponentAutoscalerStatus{Class: omev1beta1.AutoscalerKEDA, ManagedBy: omev1beta1.AutoscalerManagedByExternal, SpecSource: "runtime"}, wantClass: reportv1alpha1.AutoscaleClassKEDA, wantOwner: reportv1alpha1.AutoscaleManagedByExternal, wantSpec: reportv1alpha1.AutoscaleSpecSourceRuntime, wantIssue: reportv1alpha1.AutoscaleIssueOwnershipMismatch},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, test.status)
			got, err := Project(isvc, fixedClock())
			require.NoError(t, err)
			component := got.Content.Components[0]
			assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, component.State)
			assert.Equal(t, test.wantClass, component.Class)
			assert.Equal(t, test.wantOwner, component.ManagedBy)
			assert.Equal(t, test.wantSpec, component.SpecSource)
			assert.Contains(t, issueCodes(got.Content.Issues), test.wantIssue)
			for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
				var output bytes.Buffer
				require.NoError(t, report.Write(&output, format, got))
				assert.NotContains(t, output.String(), "SECRET_", "format %s", format)
			}
		})
	}
}

func TestProjectRejectsEveryKnownOwnershipMatrixMismatch(t *testing.T) {
	tests := []struct {
		name      string
		class     omev1beta1.AutoscalerClass
		managedBy omev1beta1.AutoscalerManagedBy
	}{
		{name: "HPA/external", class: omev1beta1.AutoscalerHPA, managedBy: omev1beta1.AutoscalerManagedByExternal},
		{name: "HPA/none", class: omev1beta1.AutoscalerHPA, managedBy: omev1beta1.AutoscalerManagedByNone},
		{name: "KEDA/external", class: omev1beta1.AutoscalerKEDA, managedBy: omev1beta1.AutoscalerManagedByExternal},
		{name: "KEDA/none", class: omev1beta1.AutoscalerKEDA, managedBy: omev1beta1.AutoscalerManagedByNone},
		{name: "External/ome", class: omev1beta1.AutoscalerExternal, managedBy: omev1beta1.AutoscalerManagedByOME},
		{name: "External/none", class: omev1beta1.AutoscalerExternal, managedBy: omev1beta1.AutoscalerManagedByNone},
		{name: "None/ome", class: omev1beta1.AutoscalerNone, managedBy: omev1beta1.AutoscalerManagedByOME},
		{name: "None/external", class: omev1beta1.AutoscalerNone, managedBy: omev1beta1.AutoscalerManagedByExternal},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := &omev1beta1.ComponentAutoscalerStatus{
				Class: test.class, ManagedBy: test.managedBy, SpecSource: "default",
			}
			got, err := Project(inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status), fixedClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, got.Content.Components[0].State)
			assert.Equal(t, []reportv1alpha1.AutoscaleIssueCode{
				reportv1alpha1.AutoscaleIssueOwnershipMismatch,
			}, issueCodes(got.Content.Issues))
		})
	}
}

func TestProjectAcceptsEveryKnownSpecSource(t *testing.T) {
	tests := []struct {
		raw  string
		want reportv1alpha1.AutoscaleSpecSource
	}{
		{raw: "isvc", want: reportv1alpha1.AutoscaleSpecSourceISVC},
		{raw: "policy", want: reportv1alpha1.AutoscaleSpecSourcePolicy},
		{raw: "runtime", want: reportv1alpha1.AutoscaleSpecSourceRuntime},
		{raw: "legacy", want: reportv1alpha1.AutoscaleSpecSourceLegacy},
		{raw: "default", want: reportv1alpha1.AutoscaleSpecSourceDefault},
	}

	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			status := reportedHPA()
			status.SpecSource = test.raw
			got, err := Project(inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status), fixedClock())
			require.NoError(t, err)
			assert.Equal(t, test.want, got.Content.Components[0].SpecSource)
			assert.NotContains(t, issueCodes(got.Content.Issues), reportv1alpha1.AutoscaleIssueSpecSourceInvalid)
		})
	}
}

func TestProjectPolicySpecSourceRemainsReported(t *testing.T) {
	status := reportedHPA()
	status.SpecSource = "policy"

	got, err := Project(inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status), fixedClock())
	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.AutoscaleComponentReported, got.Content.Components[0].State)
	assert.Equal(t, reportv1alpha1.AutoscaleSpecSourcePolicy, got.Content.Components[0].SpecSource)
	assert.NotContains(t, issueCodes(got.Content.Issues), reportv1alpha1.AutoscaleIssueSpecSourceInvalid)
	assert.Equal(t, reportv1alpha1.AutoscaleStateReported, got.Content.Summary.State)
	assert.Empty(t, got.Warnings)
}

func TestProjectExternalAndNoneRejectUnexpectedScalerEvidence(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC))
	tests := []struct {
		name           string
		mutate         func(*omev1beta1.ComponentAutoscalerStatus)
		wantReplica    reportv1alpha1.AutoscaleReplicaState
		wantConditions reportv1alpha1.AutoscaleConditionsState
	}{
		{name: "counter", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) { status.CurrentReplicas = 1 }, wantReplica: reportv1alpha1.AutoscaleReplicasInvalid, wantConditions: reportv1alpha1.AutoscaleConditionsUnavailable},
		{name: "last scale", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) { status.LastScaleTime = &transition }, wantReplica: reportv1alpha1.AutoscaleReplicasInvalid, wantConditions: reportv1alpha1.AutoscaleConditionsUnavailable},
		{name: "condition", mutate: func(status *omev1beta1.ComponentAutoscalerStatus) {
			status.Conditions = []metav1.Condition{validHPACondition("AbleToScale")}
		}, wantReplica: reportv1alpha1.AutoscaleReplicasUnavailable, wantConditions: reportv1alpha1.AutoscaleConditionsInvalid},
	}

	for _, classAndOwner := range []struct {
		class omev1beta1.AutoscalerClass
		owner omev1beta1.AutoscalerManagedBy
	}{{omev1beta1.AutoscalerExternal, omev1beta1.AutoscalerManagedByExternal}, {omev1beta1.AutoscalerNone, omev1beta1.AutoscalerManagedByNone}} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s/%s", classAndOwner.class, test.name), func(t *testing.T) {
				status := &omev1beta1.ComponentAutoscalerStatus{Class: classAndOwner.class, ManagedBy: classAndOwner.owner, SpecSource: "default"}
				test.mutate(status)
				got, err := Project(inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status), fixedClock())
				require.NoError(t, err)
				component := got.Content.Components[0]
				assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, component.State)
				assert.Equal(t, test.wantReplica, component.Replicas.State)
				assert.Nil(t, component.Replicas.CurrentReplicas)
				assert.Nil(t, component.Replicas.DesiredReplicas)
				assert.Nil(t, component.Replicas.LastScaleTime)
				assert.Equal(t, test.wantConditions, component.Conditions.State)
				assert.Empty(t, component.Conditions.Items)
				assert.Equal(t, []reportv1alpha1.AutoscaleIssueCode{reportv1alpha1.AutoscaleIssueUnexpectedScalerEvidence}, issueCodes(got.Content.Issues))
			})
		}
	}
}

func TestProjectExternalAndNoneContradictionsSurviveOwnershipErrors(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC))
	tests := []struct {
		name      string
		class     omev1beta1.AutoscalerClass
		managedBy omev1beta1.AutoscalerManagedBy
		want      []reportv1alpha1.AutoscaleIssueCode
	}{
		{
			name: "external with mismatched OME manager", class: omev1beta1.AutoscalerExternal,
			managedBy: omev1beta1.AutoscalerManagedByOME,
			want: []reportv1alpha1.AutoscaleIssueCode{
				reportv1alpha1.AutoscaleIssueOwnershipMismatch,
				reportv1alpha1.AutoscaleIssueUnexpectedScalerEvidence,
			},
		},
		{
			name: "none with mismatched external manager", class: omev1beta1.AutoscalerNone,
			managedBy: omev1beta1.AutoscalerManagedByExternal,
			want: []reportv1alpha1.AutoscaleIssueCode{
				reportv1alpha1.AutoscaleIssueOwnershipMismatch,
				reportv1alpha1.AutoscaleIssueUnexpectedScalerEvidence,
			},
		},
		{
			name: "external with invalid manager", class: omev1beta1.AutoscalerExternal,
			managedBy: "SECRET_MANAGER",
			want: []reportv1alpha1.AutoscaleIssueCode{
				reportv1alpha1.AutoscaleIssueManagedByInvalid,
				reportv1alpha1.AutoscaleIssueUnexpectedScalerEvidence,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := &omev1beta1.ComponentAutoscalerStatus{
				Class: test.class, ManagedBy: test.managedBy, SpecSource: "isvc",
				CurrentReplicas: 1, DesiredReplicas: 2, LastScaleTime: &transition,
				Conditions: []metav1.Condition{validHPACondition("AbleToScale")},
			}

			got, err := Project(inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status), fixedClock())
			require.NoError(t, err)
			component := got.Content.Components[0]
			assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, component.State)
			assert.Equal(t, reportv1alpha1.AutoscaleReplicasInvalid, component.Replicas.State)
			assert.Nil(t, component.Replicas.CurrentReplicas)
			assert.Nil(t, component.Replicas.DesiredReplicas)
			assert.Nil(t, component.Replicas.LastScaleTime)
			assert.Equal(t, reportv1alpha1.AutoscaleConditionsInvalid, component.Conditions.State)
			assert.Empty(t, component.Conditions.Items)
			assert.Equal(t, test.want, issueCodes(got.Content.Issues))
		})
	}
}

func TestProjectConditionsAreAllowlistedValidatedDedupedAndOrdered(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, 8, 31, 11, 0, 0, 0, time.FixedZone("input-zone", -7*60*60)))
	duplicate := metav1.Condition{Type: "ScalingActive", Status: metav1.ConditionFalse, LastTransitionTime: transition, Reason: "SECRET_REASON", Message: "SECRET_MESSAGE"}
	isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, &omev1beta1.ComponentAutoscalerStatus{
		Class: omev1beta1.AutoscalerHPA, ManagedBy: omev1beta1.AutoscalerManagedByOME, SpecSource: "isvc",
		CurrentReplicas: 2, DesiredReplicas: 3,
		Conditions: []metav1.Condition{
			duplicate,
			{Type: "ScalingLimited", Status: metav1.ConditionUnknown, LastTransitionTime: transition, Reason: "Limited"},
			{Type: "AbleToScale", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "Able"},
			duplicate,
		},
	})

	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)
	conditions := got.Content.Components[0].Conditions
	assert.Equal(t, reportv1alpha1.AutoscaleConditionsReported, conditions.State)
	assert.Equal(t, []reportv1alpha1.AutoscaleCondition{
		{Type: reportv1alpha1.AutoscaleConditionAbleToScale, Status: reportv1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition.Time.UTC()},
		{Type: reportv1alpha1.AutoscaleConditionScalingActive, Status: reportv1alpha1.AutoscaleConditionFalse, LastTransitionTime: transition.Time.UTC()},
		{Type: reportv1alpha1.AutoscaleConditionScalingLimited, Status: reportv1alpha1.AutoscaleConditionUnknown, LastTransitionTime: transition.Time.UTC()},
	}, conditions.Items)
}

func TestProjectKEDAConditionsUseTheirOwnFixedOrder(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC))
	isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, &omev1beta1.ComponentAutoscalerStatus{
		Class: omev1beta1.AutoscalerKEDA, ManagedBy: omev1beta1.AutoscalerManagedByOME,
		SpecSource: "runtime", CurrentReplicas: 2, DesiredReplicas: 3,
		Conditions: []metav1.Condition{
			{Type: "Paused", Status: metav1.ConditionFalse, LastTransitionTime: transition, Reason: "NotPaused"},
			{Type: "Fallback", Status: metav1.ConditionUnknown, LastTransitionTime: transition, Reason: "Unknown"},
			{Type: "Active", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "Active"},
			{Type: "Ready", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "Ready"},
		},
	})

	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, []reportv1alpha1.AutoscaleConditionType{
		reportv1alpha1.AutoscaleConditionReady,
		reportv1alpha1.AutoscaleConditionActive,
		reportv1alpha1.AutoscaleConditionFallback,
		reportv1alpha1.AutoscaleConditionPaused,
	}, conditionTypes(got.Content.Components[0].Conditions.Items))
}

func TestProjectIgnoresValidAutoscalerResolvedPolicyCondition(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC))
	status := reportedHPA()
	status.Conditions = append(status.Conditions, metav1.Condition{
		Type: omev1beta1.AutoscalerResolvedCondition, Status: metav1.ConditionTrue,
		Reason: omev1beta1.AutoscalerResolvedReasonInlinePrecedence, LastTransitionTime: transition,
	})
	isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status)
	isvc.Spec.Engine = &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{
		Autoscaler:          &omev1beta1.ComponentAutoscaler{Class: omev1beta1.AutoscalerHPA},
		AutoscalerPolicyRef: &omev1beta1.AutoscalerPolicyRef{Name: "shadowed-policy"},
	}}

	got, err := Project(isvc, fixedClock())

	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AutoscaleComponentReported, component.State)
	assert.Equal(t, reportv1alpha1.AutoscaleConditionsReported, component.Conditions.State)
	assert.Equal(t, []reportv1alpha1.AutoscaleConditionType{reportv1alpha1.AutoscaleConditionAbleToScale}, conditionTypes(component.Conditions.Items))
	assert.NotContains(t, issueCodes(got.Content.Issues), reportv1alpha1.AutoscaleIssueConditionInvalid)

	status.Conditions = status.Conditions[1:]
	got, err = Project(inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status), fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleConditionsNotReported, got.Content.Components[0].Conditions.State)
	assert.Empty(t, got.Content.Components[0].Conditions.Items)
	assert.NotContains(t, issueCodes(got.Content.Issues), reportv1alpha1.AutoscaleIssueConditionInvalid)
}

func TestProjectIgnoresValidAutoscalerResolvedForExternalAndNone(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC))
	for _, test := range []struct {
		name    string
		class   omev1beta1.AutoscalerClass
		managed omev1beta1.AutoscalerManagedBy
	}{
		{name: "External", class: omev1beta1.AutoscalerExternal, managed: omev1beta1.AutoscalerManagedByExternal},
		{name: "None", class: omev1beta1.AutoscalerNone, managed: omev1beta1.AutoscalerManagedByNone},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := &omev1beta1.ComponentAutoscalerStatus{
				Class: test.class, ManagedBy: test.managed, SpecSource: "isvc",
				Conditions: []metav1.Condition{{
					Type: omev1beta1.AutoscalerResolvedCondition, Status: metav1.ConditionTrue,
					Reason: omev1beta1.AutoscalerResolvedReasonInlinePrecedence, LastTransitionTime: transition,
				}},
			}
			isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status)
			isvc.Spec.Engine = &omev1beta1.EngineSpec{ComponentExtensionSpec: omev1beta1.ComponentExtensionSpec{
				Autoscaler:          &omev1beta1.ComponentAutoscaler{Class: test.class},
				AutoscalerPolicyRef: &omev1beta1.AutoscalerPolicyRef{Name: "shadowed-policy"},
			}}

			got, err := Project(isvc, fixedClock())

			require.NoError(t, err)
			require.Len(t, got.Content.Components, 1)
			component := got.Content.Components[0]
			assert.Equal(t, reportv1alpha1.AutoscaleComponentReported, component.State)
			assert.Equal(t, reportv1alpha1.AutoscaleConditionsUnavailable, component.Conditions.State)
			assert.Empty(t, component.Conditions.Items)
			assert.Empty(t, got.Content.Issues)
			assert.Equal(t, reportv1alpha1.AutoscaleStateReported, got.Content.Summary.State)
		})
	}
}

func TestProjectMalformedExternalConditionDegradesToPartialUnavailable(t *testing.T) {
	status := &omev1beta1.ComponentAutoscalerStatus{
		Class: omev1beta1.AutoscalerExternal, ManagedBy: omev1beta1.AutoscalerManagedByExternal,
		SpecSource: "isvc", Conditions: []metav1.Condition{{
			Type: omev1beta1.AutoscalerResolvedCondition, Status: metav1.ConditionTrue,
			Reason: "SECRET INVALID REASON", Message: "SECRET CONDITION MESSAGE",
		}},
	}

	got, err := Project(inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status), fixedClock())

	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, reportv1alpha1.AutoscaleComponentPartial, got.Content.Components[0].State)
	assert.Equal(t, reportv1alpha1.AutoscaleConditionsUnavailable, got.Content.Components[0].Conditions.State)
	assert.Equal(t, []reportv1alpha1.AutoscaleIssueCode{reportv1alpha1.AutoscaleIssueConditionInvalid}, issueCodes(got.Content.Issues))
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "SECRET")
}

func TestProjectInvalidAndConflictingConditionsAreOmitted(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC))
	later := metav1.NewTime(transition.Add(time.Minute))
	tests := []struct {
		name       string
		class      omev1beta1.AutoscalerClass
		conditions []metav1.Condition
		wantIssue  reportv1alpha1.AutoscaleIssueCode
		wantItems  []reportv1alpha1.AutoscaleCondition
	}{
		{name: "foreign HPA condition", class: omev1beta1.AutoscalerHPA, conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "Ready"}}, wantIssue: reportv1alpha1.AutoscaleIssueConditionInvalid},
		{name: "foreign KEDA condition", class: omev1beta1.AutoscalerKEDA, conditions: []metav1.Condition{{Type: "AbleToScale", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "Able"}}, wantIssue: reportv1alpha1.AutoscaleIssueConditionInvalid},
		{name: "unknown status", class: omev1beta1.AutoscalerHPA, conditions: []metav1.Condition{{Type: "AbleToScale", Status: "SECRET_STATUS", LastTransitionTime: transition, Reason: "Able"}}, wantIssue: reportv1alpha1.AutoscaleIssueConditionInvalid},
		{name: "zero transition", class: omev1beta1.AutoscalerHPA, conditions: []metav1.Condition{{Type: "AbleToScale", Status: metav1.ConditionTrue, Reason: "Able"}}, wantIssue: reportv1alpha1.AutoscaleIssueConditionInvalid},
		{name: "empty reason", class: omev1beta1.AutoscalerHPA, conditions: []metav1.Condition{{Type: "AbleToScale", Status: metav1.ConditionTrue, LastTransitionTime: transition}}, wantIssue: reportv1alpha1.AutoscaleIssueConditionInvalid},
		{name: "malformed reason", class: omev1beta1.AutoscalerHPA, conditions: []metav1.Condition{{Type: "AbleToScale", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "SECRET REASON"}}, wantIssue: reportv1alpha1.AutoscaleIssueConditionInvalid},
		{name: "oversized reason", class: omev1beta1.AutoscalerHPA, conditions: []metav1.Condition{{Type: "AbleToScale", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: strings.Repeat("S", 1025)}}, wantIssue: reportv1alpha1.AutoscaleIssueConditionInvalid},
		{name: "conflicting duplicate", class: omev1beta1.AutoscalerHPA, conditions: []metav1.Condition{
			{Type: "AbleToScale", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "Able"},
			{Type: "AbleToScale", Status: metav1.ConditionFalse, LastTransitionTime: later, Reason: "Unable"},
			{Type: "ScalingActive", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "Active"},
		}, wantIssue: reportv1alpha1.AutoscaleIssueConditionConflict, wantItems: []reportv1alpha1.AutoscaleCondition{{Type: reportv1alpha1.AutoscaleConditionScalingActive, Status: reportv1alpha1.AutoscaleConditionTrue, LastTransitionTime: transition.Time}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := reportedHPA()
			status.Class = test.class
			status.Conditions = test.conditions
			got, err := Project(inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status), fixedClock())
			require.NoError(t, err)
			component := got.Content.Components[0]
			assert.Equal(t, reportv1alpha1.AutoscaleConditionsUnavailable, component.Conditions.State)
			assert.Equal(t, append([]reportv1alpha1.AutoscaleCondition{}, test.wantItems...), component.Conditions.Items)
			assert.Contains(t, issueCodes(got.Content.Issues), test.wantIssue)
			assert.Equal(t, reportv1alpha1.AutoscaleComponentPartial, component.State)
		})
	}
}

func TestProjectReportsBothInvalidAndConflictingConditionAnomalies(t *testing.T) {
	transition := metav1.NewTime(time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC))
	later := metav1.NewTime(transition.Add(time.Minute))
	status := reportedHPA()
	status.Conditions = []metav1.Condition{
		{Type: "AbleToScale", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "Able"},
		{Type: "AbleToScale", Status: metav1.ConditionFalse, LastTransitionTime: later, Reason: "Unable"},
		{Type: "SECRET_CONDITION", Status: metav1.ConditionTrue, LastTransitionTime: transition, Reason: "Secret"},
	}

	got, err := Project(inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status), fixedClock())
	require.NoError(t, err)
	assert.Equal(t, []reportv1alpha1.AutoscaleIssueCode{
		reportv1alpha1.AutoscaleIssueConditionConflict,
		reportv1alpha1.AutoscaleIssueConditionInvalid,
	}, issueCodes(got.Content.Issues))
	encoded, marshalErr := json.Marshal(got)
	require.NoError(t, marshalErr)
	assert.NotContains(t, string(encoded), "SECRET_CONDITION")
}

func TestProjectUnknownComponentAndHostileFieldsCannotLeak(t *testing.T) {
	isvc := baseInferenceService()
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		omev1beta1.ComponentType("SECRET_COMPONENT"): {
			Autoscaler:     &omev1beta1.ComponentAutoscalerStatus{Class: "SECRET_CLASS", ManagedBy: "SECRET_OWNER", SpecSource: "SECRET_SOURCE"},
			ScaleTargetRef: &omev1beta1.ScaleTargetRef{APIVersion: "SECRET_API", Kind: "SECRET_KIND", Name: "SECRET_TARGET"},
		},
	}

	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleStateInvalid, got.Content.Summary.State)
	assert.Empty(t, got.Content.Components)
	assert.Equal(t, []reportv1alpha1.AutoscaleIssue{{Code: reportv1alpha1.AutoscaleIssueUnknownComponentStatus}}, got.Content.Issues)

	var compact bytes.Buffer
	require.NoError(t, report.Write(&compact, report.FormatTable, got))
	assert.Contains(t, compact.String(), "UnknownComp")
	assert.NotContains(t, compact.String(), "SECRET_")

	var wide bytes.Buffer
	require.NoError(t, got.WideTable().Write(&wide))
	assert.Contains(t, wide.String(), string(reportv1alpha1.AutoscaleIssueUnknownComponentStatus))
	assert.NotContains(t, wide.String(), "SECRET_")

	for _, format := range []report.Format{report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, got))
		assert.Contains(t, output.String(), string(reportv1alpha1.AutoscaleIssueUnknownComponentStatus))
		assert.NotContains(t, output.String(), "SECRET_")
	}
}

func TestProjectIsDeterministicAndDoesNotMutateInput(t *testing.T) {
	components := []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.DecoderComponent, omev1beta1.RouterComponent}
	var baseline reportv1alpha1.AutoscaleStatusReport
	for seed := int64(0); seed < 20; seed++ {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test permutation
		permutation := rng.Perm(len(components))
		isvc := baseInferenceService()
		isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{}
		for _, index := range permutation {
			component := components[index]
			autoscaler := reportedHPA()
			autoscaler.Conditions = []metav1.Condition{
				validHPACondition("ScalingLimited"),
				validHPACondition("AbleToScale"),
				validHPACondition("ScalingActive"),
			}
			rng.Shuffle(len(autoscaler.Conditions), func(left, right int) {
				autoscaler.Conditions[left], autoscaler.Conditions[right] = autoscaler.Conditions[right], autoscaler.Conditions[left]
			})
			isvc.Status.Components[component] = omev1beta1.ComponentStatusSpec{
				Autoscaler: autoscaler,
				ScaleTargetRef: &omev1beta1.ScaleTargetRef{
					APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-" + string(component),
				},
			}
		}
		before := isvc.DeepCopy()

		got, err := Project(isvc, fixedClock())
		require.NoError(t, err)
		assert.True(t, reflect.DeepEqual(before, isvc), "projection mutated input for seed %d", seed)
		if seed == 0 {
			baseline = got
		} else {
			assert.Equal(t, baseline, got)
		}
	}
	assert.Equal(t, []reportv1alpha1.RuntimeComponentType{
		reportv1alpha1.RuntimeComponentEngine,
		reportv1alpha1.RuntimeComponentDecoder,
		reportv1alpha1.RuntimeComponentRouter,
	}, []reportv1alpha1.RuntimeComponentType{
		baseline.Content.Components[0].Type,
		baseline.Content.Components[1].Type,
		baseline.Content.Components[2].Type,
	})
}

func TestProjectOutputDoesNotAliasInput(t *testing.T) {
	lastScale := metav1.NewTime(time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC))
	status := reportedHPA()
	status.LastScaleTime = &lastScale
	isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status)

	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)
	projected := &got.Content.Components[0]
	*projected.Replicas.CurrentReplicas = 99
	*projected.Replicas.DesiredReplicas = 100
	*projected.Replicas.LastScaleTime = projected.Replicas.LastScaleTime.Add(time.Hour)
	projected.Target.Name = "returned-target"
	projected.Conditions.Items[0].Status = reportv1alpha1.AutoscaleConditionFalse

	original := isvc.Status.Components[omev1beta1.EngineComponent]
	assert.Equal(t, int32(2), original.Autoscaler.CurrentReplicas)
	assert.Equal(t, int32(3), original.Autoscaler.DesiredReplicas)
	assert.Equal(t, lastScale, *original.Autoscaler.LastScaleTime)
	assert.Equal(t, "chat-engine", original.ScaleTargetRef.Name)
	assert.Equal(t, metav1.ConditionTrue, original.Autoscaler.Conditions[0].Status)
}

func TestProjectWarningsAreOnlyPartialData(t *testing.T) {
	isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, reportedHPA())
	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Empty(t, got.Warnings)

	component := isvc.Status.Components[omev1beta1.EngineComponent]
	component.ScaleTargetRef = nil
	isvc.Status.Components[omev1beta1.EngineComponent] = component
	got, err = Project(isvc, fixedClock())
	require.NoError(t, err)
	assert.Equal(t, []reportv1alpha1.AutoscaleWarning{{Code: reportv1alpha1.AutoscaleWarningPartialData}}, got.Warnings)
	for _, warning := range got.Warnings {
		assert.Equal(t, reportv1alpha1.AutoscaleWarningPartialData, warning.Code)
	}
}

func baseInferenceService() *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "chat", UID: types.UID("isvc-uid"), Generation: 7,
		ResourceVersion: "SECRET_RESOURCE_VERSION",
		Annotations:     map[string]string{"secret": "SECRET_ANNOTATION"},
	}}
}

func inferenceServiceWithAutoscaler(component omev1beta1.ComponentType, status *omev1beta1.ComponentAutoscalerStatus) *omev1beta1.InferenceService {
	isvc := baseInferenceService()
	isvc.Status.Components = map[omev1beta1.ComponentType]omev1beta1.ComponentStatusSpec{
		component: {
			Autoscaler:     status,
			ScaleTargetRef: &omev1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "chat-" + string(component)},
		},
	}
	return isvc
}

func reportedHPA() *omev1beta1.ComponentAutoscalerStatus {
	return &omev1beta1.ComponentAutoscalerStatus{
		Class: omev1beta1.AutoscalerHPA, ManagedBy: omev1beta1.AutoscalerManagedByOME,
		SpecSource: "default", CurrentReplicas: 2, DesiredReplicas: 3,
		Conditions: []metav1.Condition{validHPACondition("AbleToScale")},
	}
}

func validHPACondition(conditionType string) metav1.Condition {
	return metav1.Condition{
		Type: conditionType, Status: metav1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Date(2026, 8, 31, 18, 0, 0, 0, time.UTC)),
		Reason:             "Observed",
	}
}

func issueCodes(issues []reportv1alpha1.AutoscaleIssue) []reportv1alpha1.AutoscaleIssueCode {
	result := make([]reportv1alpha1.AutoscaleIssueCode, len(issues))
	for index := range issues {
		result[index] = issues[index].Code
	}
	return result
}

func conditionTypes(conditions []reportv1alpha1.AutoscaleCondition) []reportv1alpha1.AutoscaleConditionType {
	result := make([]reportv1alpha1.AutoscaleConditionType, len(conditions))
	for index := range conditions {
		result[index] = conditions[index].Type
	}
	return result
}

func ptrInt32(value int32) *int32 { return &value }

func fixedClock() reportv1alpha1.Clock {
	return reportv1alpha1.ClockFunc(func() time.Time {
		return time.Date(2026, time.August, 31, 18, 30, 0, 0, time.UTC)
	})
}

func ptrTime(value time.Time) *time.Time { return &value }
