package autoscaleprojection

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/paging"
	clireport "sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

func TestProjectExplainConsistentCurrentReport(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: reportedEngine(v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "default", "apps/v1", "Deployment", "svc-engine"),
	}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainConsistent, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.StatusFreshnessCurrent, got.Content.Summary.StatusFreshness)
	require.Len(t, got.Content.Components, 1)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.DeploymentModeSourceServiceSpec, component.DeploymentModeSource)
	assert.Equal(t, reportv1alpha1.AutoscaleDesiredAvailable, component.Desired.State)
	assert.Equal(t, reportv1alpha1.AutoscaleReportedAvailable, component.Reported.State)
	assert.Equal(t, reportv1alpha1.AutoscaleReconciliationConsistent, component.Reconciliation.State)
	require.NotNil(t, component.Reported.Target)
	assert.Equal(t, "svc-engine", component.Reported.Target.Name)
	assert.Empty(t, got.Warnings)
}

func TestProjectExplainHonorsServiceVirtualDeploymentEarlyExit(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Spec.DeploymentMode = nil
	isvc.Annotations = map[string]string{constants.DeploymentMode: string(constants.VirtualDeployment)}
	isvc.Spec.Engine.Annotations = map[string]string{constants.DeploymentMode: string(constants.RawDeployment)}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainUnsupported, got.Content.Summary.State)
	require.Len(t, got.Content.Components, 1)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.DeploymentModeVirtualDeployment, component.DeploymentMode)
	assert.Equal(t, reportv1alpha1.DeploymentModeSourceServiceAnnotation, component.DeploymentModeSource)
	assert.Equal(t, reportv1alpha1.AutoscaleDesiredUnsupported, component.Desired.State)
	assert.Nil(t, component.Desired.Target)
}

func TestProjectExplainReportsMismatchWithoutClaimingDrift(t *testing.T) {
	isvc := explainISVC(constants.OMENative)
	isvc.Spec.Engine.Autoscaler = kedaExplainAutoscaler()
	isvc.Spec.Engine.MinReplicas = ptr.To(1)
	isvc.Spec.Engine.MaxReplicas = 4
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: reportedEngine(v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "runtime", "apps/v1", "Deployment", "wrong"),
	}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())
	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainReportedMismatch, got.Content.Summary.State)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AutoscaleReconciliationReportedMismatch, component.Reconciliation.State)
	assert.ElementsMatch(t, []reportv1alpha1.AutoscaleExplainIssueCode{
		reportv1alpha1.AutoscaleExplainIssueReportedClassMismatch,
		reportv1alpha1.AutoscaleExplainIssueReportedSpecSourceMismatch,
		reportv1alpha1.AutoscaleExplainIssueReportedTargetMismatch,
	}, component.Reconciliation.Issues)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "Drift")
	assert.NotContains(t, string(encoded), "drift")
}

func TestReconcileAutoscalingPreservesProvableMismatchWhenTargetIsNotReported(t *testing.T) {
	desired := reportv1alpha1.AutoscaleDesiredConfiguration{
		State: reportv1alpha1.AutoscaleDesiredAvailable, Class: reportv1alpha1.AutoscaleClassHPA,
		ManagedBy: reportv1alpha1.AutoscaleManagedByOME, SpecSource: reportv1alpha1.AutoscaleSpecSourceDefault,
		Target: &reportv1alpha1.AutoscaleTargetIdentity{
			APIVersion: "apps/v1", Kind: reportv1alpha1.AutoscaleTargetDeployment,
			Namespace: "workloads", Name: "svc-engine",
		},
	}
	reported := reportv1alpha1.AutoscaleReportedConfiguration{
		State: reportv1alpha1.AutoscaleReportedAvailable, Class: reportv1alpha1.AutoscaleClassKEDA,
		ManagedBy: reportv1alpha1.AutoscaleManagedByExternal, SpecSource: reportv1alpha1.AutoscaleSpecSourceRuntime,
		TargetState: reportv1alpha1.AutoscaleTargetNotReported,
	}

	got := reconcileAutoscaling(desired, reported)

	assert.Equal(t, reportv1alpha1.AutoscaleReconciliationReportedMismatch, got.State)
	assert.ElementsMatch(t, []reportv1alpha1.AutoscaleExplainIssueCode{
		reportv1alpha1.AutoscaleExplainIssueReportedClassMismatch,
		reportv1alpha1.AutoscaleExplainIssueReportedOwnershipMismatch,
		reportv1alpha1.AutoscaleExplainIssueReportedSpecSourceMismatch,
	}, got.Issues)
}

func TestProjectExplainStaleAndUnobservedStatusAreUnavailable(t *testing.T) {
	tests := []struct {
		name               string
		observedGeneration int64
		wantFreshness      reportv1alpha1.StatusFreshness
		wantIssue          reportv1alpha1.AutoscaleExplainIssueCode
		wantWarnings       []reportv1alpha1.AutoscaleExplainWarning
	}{
		{
			name: "stale", observedGeneration: 3, wantFreshness: reportv1alpha1.StatusFreshnessStale,
			wantIssue:    reportv1alpha1.AutoscaleExplainIssueStatusStale,
			wantWarnings: []reportv1alpha1.AutoscaleExplainWarning{{Code: reportv1alpha1.AutoscaleExplainWarningPartialData}, {Code: reportv1alpha1.AutoscaleExplainWarningStaleEvidence}},
		},
		{
			name: "unobserved", observedGeneration: 0, wantFreshness: reportv1alpha1.StatusFreshnessUnobserved,
			wantIssue:    reportv1alpha1.AutoscaleExplainIssueStatusUnobserved,
			wantWarnings: []reportv1alpha1.AutoscaleExplainWarning{{Code: reportv1alpha1.AutoscaleExplainWarningPartialData}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := explainISVC(constants.RawDeployment)
			isvc.Status.ObservedGeneration = tt.observedGeneration
			isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: reportedEngine(v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "default", "apps/v1", "Deployment", "svc-engine"),
			}
			state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

			got, err := ProjectExplain(isvc, state, fixedExplainClock())
			require.NoError(t, err)
			assert.Equal(t, reportv1alpha1.AutoscaleExplainPartial, got.Content.Summary.State)
			assert.Equal(t, tt.wantFreshness, got.Content.Summary.StatusFreshness)
			component := got.Content.Components[0]
			assert.Equal(t, reportv1alpha1.AutoscaleReportedUnavailable, component.Reported.State)
			assert.Equal(t, reportv1alpha1.AutoscaleTargetUnavailable, component.Reported.TargetState)
			assert.Nil(t, component.Reported.Target)
			assert.Equal(t, reportv1alpha1.AutoscaleReconciliationUnavailable, component.Reconciliation.State)
			assert.Contains(t, component.Reconciliation.Issues, tt.wantIssue)
			assert.Equal(t, tt.wantWarnings, got.Warnings)
		})
	}
}

func TestProjectExplainStaleMalformedStatusRemainsUnavailableNotInvalid(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Status.ObservedGeneration = isvc.Generation - 1
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: reportedEngine("SECRET_CLASS", "SECRET_OWNER", "SECRET_SOURCE", "secret/v1", "SecretKind", "secret-target"),
	}
	isvc.Status.Components[v1beta1.EngineComponent].Autoscaler.Conditions[0].Reason = "SECRET REASON"
	isvc.Status.Components[v1beta1.EngineComponent].Autoscaler.Conditions[0].Message = "status-message-secret"
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainPartial, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.AutoscaleReportedUnavailable, got.Content.Components[0].Reported.State)
	assert.NotContains(t, got.Content.Issues, reportv1alpha1.AutoscaleExplainIssue{Code: reportv1alpha1.AutoscaleExplainIssueStatusInvalid})
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{"SECRET_CLASS", "SECRET_OWNER", "SECRET_SOURCE", "secret-target", "SecretKind", "SECRET REASON", "status-message-secret"} {
		assert.NotContains(t, string(encoded), secret)
	}
}

func TestProjectExplainCurrentMalformedConditionIsPartialAndRedacted(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	status := reportedEngine(v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "default", "apps/v1", "Deployment", "svc-engine")
	status.Autoscaler.Conditions[0].Reason = "SECRET REASON"
	status.Autoscaler.Conditions[0].Message = "status-message-secret"
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: status,
	}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainPartial, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.AutoscaleReportedAvailable, got.Content.Components[0].Reported.State)
	assert.Equal(t, reportv1alpha1.AutoscaleConditionsUnavailable, got.Content.Components[0].Reported.Conditions.State)
	assert.Equal(t, reportv1alpha1.AutoscaleReconciliationConsistent, got.Content.Components[0].Reconciliation.State)
	assert.Contains(t, got.Content.Components[0].Reconciliation.Issues, reportv1alpha1.AutoscaleExplainIssueReportedEvidencePartial)
	assert.Contains(t, got.Warnings, reportv1alpha1.AutoscaleExplainWarning{Code: reportv1alpha1.AutoscaleExplainWarningPartialData})
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "SECRET REASON")
	assert.NotContains(t, string(encoded), "status-message-secret")
}

func TestProjectExplainCurrentMalformedUnmatchedStatusHasBoundedGlobalIssue(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent:  reportedEngine(v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "default", "apps/v1", "Deployment", "svc-engine"),
		v1beta1.DecoderComponent: reportedEngine("SECRET_CLASS", "SECRET_OWNER", "SECRET_SOURCE", "secret/v1", "SecretKind", "secret-target"),
	}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainInvalid, got.Content.Summary.State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleExplainIssue{Code: reportv1alpha1.AutoscaleExplainIssueStatusInvalid})
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{"SECRET_CLASS", "SECRET_OWNER", "SECRET_SOURCE", "secret-target", "SecretKind"} {
		assert.NotContains(t, string(encoded), secret)
	}
}

func TestProjectExplainCurrentReportedOnlyComponentIsMismatch(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent:  reportedEngine(v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "default", "apps/v1", "Deployment", "svc-engine"),
		v1beta1.DecoderComponent: reportedEngine(v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "default", "apps/v1", "Deployment", "svc-decoder"),
	}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainReportedMismatch, got.Content.Summary.State)
	assert.Equal(t, []reportv1alpha1.AutoscaleExplainIssue{{
		Code: reportv1alpha1.AutoscaleExplainIssueCode("ReportedComponentUnexpected"), Component: reportv1alpha1.RuntimeComponentDecoder,
	}}, got.Content.Issues)
	require.Len(t, got.Content.Components, 1, "reported-only evidence must not invent a desired component")
	var table bytes.Buffer
	require.NoError(t, clireport.Write(&table, clireport.FormatTable, got))
	assert.Contains(t, table.String(), "unexpected-component")
}

func TestProjectExplainPreservesIndependentlyReportedTargetWithoutAutoscaler(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			ScaleTargetRef: &v1beta1.ScaleTargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: "svc-engine"},
		},
	}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AutoscaleReportedNotReported, component.Reported.State)
	assert.Equal(t, reportv1alpha1.AutoscaleTargetReported, component.Reported.TargetState)
	require.NotNil(t, component.Reported.Target)
	assert.Equal(t, "svc-engine", component.Reported.Target.Name)
	var table bytes.Buffer
	require.NoError(t, clireport.Write(&table, clireport.FormatTable, got))
	assert.Contains(t, table.String(), "Deployment/svc-engine")
}

func TestProjectExplainPolicyUnavailableAndInvalidConfiguration(t *testing.T) {
	t.Run("policy", func(t *testing.T) {
		isvc := explainISVC(constants.RawDeployment)
		isvc.Spec.Engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{Name: "fleet"}
		state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

		got, err := ProjectExplain(isvc, state, fixedExplainClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.AutoscaleExplainPartial, got.Content.Summary.State)
		assert.Equal(t, reportv1alpha1.AutoscaleDesiredUnavailable, got.Content.Components[0].Desired.State)
		assert.Nil(t, got.Content.Components[0].Desired.Target)
		assert.Equal(t, reportv1alpha1.AutoscaleSpecSourcePolicy, got.Content.Components[0].Desired.SpecSource)
		assert.Contains(t, got.Content.Components[0].Reconciliation.Issues, reportv1alpha1.AutoscaleExplainIssuePolicyResolutionUnavailable)
		encoded, err := json.Marshal(got.Content.Components[0].Desired)
		require.NoError(t, err)
		assert.Contains(t, string(encoded), `"metricCount":null`)
		assert.Contains(t, string(encoded), `"triggerCount":null`)
		for _, unsupportedClaim := range []string{"hold", "activeDecision", "lastRendered"} {
			assert.NotContains(t, string(encoded), unsupportedClaim)
		}
	})

	t.Run("invalid KEDA", func(t *testing.T) {
		isvc := explainISVC(constants.RawDeployment)
		isvc.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA, Keda: &v1beta1.KedaAutoscaler{}}
		state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

		got, err := ProjectExplain(isvc, state, fixedExplainClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.AutoscaleExplainInvalid, got.Content.Summary.State)
		assert.Equal(t, reportv1alpha1.AutoscaleDesiredInvalid, got.Content.Components[0].Desired.State)
		assert.Contains(t, got.Content.Components[0].Reconciliation.Issues, reportv1alpha1.AutoscaleExplainIssueKEDATriggersRequired)
	})

	t.Run("reserved policy kind", func(t *testing.T) {
		isvc := explainISVC(constants.RawDeployment)
		isvc.Spec.Engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{
			Name: "fleet", Kind: "ClusterAutoscalerPolicy",
		}
		state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

		got, err := ProjectExplain(isvc, state, fixedExplainClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.AutoscaleExplainInvalid, got.Content.Summary.State)
		assert.Equal(t, reportv1alpha1.AutoscaleDesiredInvalid, got.Content.Components[0].Desired.State)
		assert.Contains(t, got.Content.Components[0].Reconciliation.Issues,
			reportv1alpha1.AutoscaleExplainIssueCode("PolicyReferenceInvalid"))
		encoded, err := json.Marshal(got)
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), "ClusterAutoscalerPolicy")
	})

	t.Run("empty policy name", func(t *testing.T) {
		isvc := explainISVC(constants.RawDeployment)
		isvc.Spec.Engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{}
		state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

		got, err := ProjectExplain(isvc, state, fixedExplainClock())
		require.NoError(t, err)
		assert.Equal(t, reportv1alpha1.AutoscaleExplainInvalid, got.Content.Summary.State)
		component := got.Content.Components[0]
		assert.Equal(t, reportv1alpha1.AutoscaleDesiredInvalid, component.Desired.State)
		assert.Contains(t, component.Reconciliation.Issues, reportv1alpha1.AutoscaleExplainIssuePolicyReferenceInvalid)
		assert.NotContains(t, component.Reconciliation.Issues, reportv1alpha1.AutoscaleExplainIssuePolicyResolutionUnavailable)
	})
}

func TestProjectExplainUnknownTypedAutoscalerClassIsInvalidAndRedacted(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{Class: "SECRET_CLASS"}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainInvalid, got.Content.Summary.State)
	component := got.Content.Components[0]
	assert.Equal(t, reportv1alpha1.AutoscaleDesiredInvalid, component.Desired.State)
	assert.Equal(t, reportv1alpha1.AutoscaleClassUnknown, component.Desired.Class)
	assert.Nil(t, component.Desired.Target)
	assert.Contains(t, component.Reconciliation.Issues, reportv1alpha1.AutoscaleExplainIssueAutoscalerClassInvalid)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "SECRET_CLASS")
}

func TestProjectExplainRedactsUnknownScalingModeToClosedEnum(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Spec.ScalingPolicy = &v1beta1.ScalingPolicy{Mode: "SECRET_ELASTIC"}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainInvalid, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.AutoscaleScalingPolicyInvalid, got.Content.ScalingPolicy.State)
	assert.Equal(t, reportv1alpha1.AutoscaleScalingMode("Unknown"), got.Content.ScalingPolicy.Mode)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleExplainIssue{Code: reportv1alpha1.AutoscaleExplainIssueScalingPolicyInvalid})
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "SECRET_ELASTIC")
}

func TestProjectExplainSecurityAllowlist(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Annotations = map[string]string{"secret.example/token": "isvc-secret-canary"}
	isvc.Spec.Engine.Autoscaler = &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{
			Type: "prometheus", Metadata: map[string]string{"bearerToken": "trigger-secret-canary"},
		}}},
	}
	runtimeSpec := baseExplainRuntime(nil)
	runtimeSpec.EngineConfig.Annotations = map[string]string{"runtime.example/token": "runtime-secret-canary"}
	state := resolveExplainState(t, isvc, runtimeSpec)

	got, err := ProjectExplain(isvc, state, fixedExplainClock())
	require.NoError(t, err)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	output := string(encoded)
	for _, forbidden := range []string{
		"isvc-secret-canary", "trigger-secret-canary", "runtime-secret-canary",
		"bearerToken", "resourceVersion", "annotations", "triggers",
	} {
		assert.NotContains(t, output, forbidden)
	}
	assert.Contains(t, output, `"triggerCount":1`)
}

func TestProjectExplainRejectsUnboundEvidence(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))
	isvc.ResourceVersion = "different"
	_, err := ProjectExplain(isvc, state, fixedExplainClock())
	assert.ErrorIs(t, err, effective.ErrAutoscalingEvidenceInvalid)
}

func TestProjectAutoscaleExplainSourcesLabelsRevisionIdentityEvidenceHonestly(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	active := effective.AutoscalingActiveConfiguration{
		State:   effective.AutoscalingActiveConfigurationAvailable,
		Runtime: effective.AutoscalingRuntimeReference{Kind: runtimeselector.KindServingRuntime, Namespace: "workloads", Name: "runtime"},
		Inheritance: effective.AutoscalingInheritance{
			State: effective.InheritanceUnavailable, UnavailableReason: effective.InheritanceUnreadable,
			Sources: []effective.AutoscalingRuntimeReference{},
		},
		Revision: &effective.AutoscalingRevisionReference{
			Namespace: "ome-system", Name: "expected-revision", Role: effective.RuntimeRevisionRoleActive,
		},
	}

	resolution := effective.AutoscalingResolution{Active: active}
	sources := projectAutoscaleExplainSources(isvc, resolution, fixedExplainClock().Now())

	require.Len(t, sources, 3)
	assert.Equal(t, "ControllerRevision", sources[2].Kind)
	assert.Equal(t, "ome-system", sources[2].Namespace)
	assert.Equal(t, "expected-revision", sources[2].Name)
	assert.Equal(t, reportv1alpha1.EvidenceComputed, sources[2].Evidence)
	active.Revision.IdentityObserved = true
	active.Revision.UID = "revision-uid"
	resolution.Active = active
	sources = projectAutoscaleExplainSources(isvc, resolution, fixedExplainClock().Now())
	assert.Equal(t, reportv1alpha1.EvidenceObserved, sources[2].Evidence)
	assert.Equal(t, "revision-uid", sources[2].UID)
}

func TestProjectAutoscaleExplainSourcesNeverRebindsActiveRuntimeToInheritanceObservation(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	active := effective.AutoscalingActiveConfiguration{
		State: effective.AutoscalingActiveConfigurationAvailable,
		Runtime: effective.AutoscalingRuntimeReference{
			Kind: runtimeselector.KindServingRuntime, Namespace: "workloads", Name: "runtime",
			UID: "selected-snapshot-uid", Generation: 4, IdentityObserved: true,
		},
		Inheritance: effective.AutoscalingInheritance{
			State: effective.InheritanceObserved,
			Sources: []effective.AutoscalingRuntimeReference{{
				Kind: runtimeselector.KindServingRuntime, Namespace: "workloads", Name: "runtime",
				UID: "later-observation-uid", Generation: 5,
			}},
		},
	}

	sources := projectAutoscaleExplainSources(isvc, effective.AutoscalingResolution{Active: active}, fixedExplainClock().Now())

	var selected, later bool
	for _, source := range sources {
		if source.Kind != runtimeselector.KindServingRuntime || source.Name != "runtime" {
			continue
		}
		selected = selected || source.UID == "selected-snapshot-uid" && source.Generation == 4
		later = later || source.UID == "later-observation-uid" && source.Generation == 5
	}
	assert.True(t, selected, "the desired snapshot identity must remain a distinct source")
	assert.True(t, later, "the independently observed inheritance source remains visible")
}

func TestProjectAutoscaleActiveOmitsUnboundRuntimeMetadata(t *testing.T) {
	const secretUID = "unbound-runtime-uid-with-user-secret"
	active := effective.AutoscalingActiveConfiguration{
		State:       effective.AutoscalingActiveConfigurationAvailable,
		Origin:      effective.ConfigurationOriginLiveRuntime,
		Consistency: effective.RevisionConsistencyUnknown,
		Runtime: effective.AutoscalingRuntimeReference{
			Kind: runtimeselector.KindServingRuntime, Namespace: "workloads", Name: "runtime",
			UID: secretUID, Generation: 99, IdentityObserved: false,
		},
		Inheritance: effective.AutoscalingInheritance{
			State: effective.InheritanceUnavailable, UnavailableReason: effective.InheritanceUnreadable,
			Sources: []effective.AutoscalingRuntimeReference{},
		},
	}

	projected, err := projectAutoscaleActive(active)
	require.NoError(t, err)
	assert.Empty(t, projected.Runtime.UID)
	assert.Zero(t, projected.Runtime.Generation)

	sources := projectAutoscaleExplainSources(
		explainISVC(constants.RawDeployment), effective.AutoscalingResolution{Active: active}, fixedExplainClock().Now(),
	)
	for _, source := range sources {
		assert.NotEqual(t, secretUID, source.UID)
	}
}

func TestProjectAutoscaleExplainSourcesIncludesOnlyCausalAutoSelectionModel(t *testing.T) {
	tests := []struct {
		name     string
		model    effective.AutoscalingModelReference
		wantKind string
		wantNS   string
		wantUID  string
		wantGen  int64
	}{
		{
			name: "namespaced model",
			model: effective.AutoscalingModelReference{
				Kind: "BaseModel", Namespace: "workloads", Name: "model",
				UID: "model-uid", Generation: 6,
			},
			wantKind: "BaseModel", wantNS: "workloads", wantUID: "model-uid", wantGen: 6,
		},
		{
			name: "cluster model",
			model: effective.AutoscalingModelReference{
				Kind: "ClusterBaseModel", Name: "model", UID: "cluster-model-uid", Generation: 7,
			},
			wantKind: "ClusterBaseModel", wantUID: "cluster-model-uid", wantGen: 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := explainISVC(constants.RawDeployment)
			resolution := effective.AutoscalingResolution{
				Model: &tt.model,
				Active: effective.AutoscalingActiveConfiguration{
					State:  effective.AutoscalingActiveConfigurationAvailable,
					Origin: effective.ConfigurationOriginLiveRuntime,
					Runtime: effective.AutoscalingRuntimeReference{
						Kind: runtimeselector.KindServingRuntime, Namespace: isvc.Namespace, Name: "runtime",
					},
					Inheritance: effective.AutoscalingInheritance{
						State: effective.InheritanceUnavailable, UnavailableReason: effective.InheritanceUnreadable,
					},
				},
			}

			sources := projectAutoscaleExplainSources(isvc, resolution, fixedExplainClock().Now())

			var modelSource *reportv1alpha1.RuntimeSourceReference
			for i := range sources {
				if sources[i].Kind == tt.wantKind {
					modelSource = &sources[i]
				}
			}
			require.NotNil(t, modelSource)
			assert.Equal(t, tt.wantNS, modelSource.Namespace)
			assert.Equal(t, "model", modelSource.Name)
			assert.Equal(t, tt.wantUID, modelSource.UID)
			assert.Equal(t, tt.wantGen, modelSource.Generation)
			assert.Equal(t, reportv1alpha1.EvidenceObserved, modelSource.Evidence)
		})
	}

	isvc := explainISVC(constants.RawDeployment)
	pinned := effective.AutoscalingResolution{
		Model: &effective.AutoscalingModelReference{
			Kind: "BaseModel", Namespace: isvc.Namespace, Name: "live-model", UID: "live-model-uid",
		},
		Active: effective.AutoscalingActiveConfiguration{
			State:  effective.AutoscalingActiveConfigurationAvailable,
			Origin: effective.ConfigurationOriginControllerRevision,
			Runtime: effective.AutoscalingRuntimeReference{
				Kind: runtimeselector.KindServingRuntime, Namespace: isvc.Namespace, Name: "runtime",
			},
			Inheritance: effective.AutoscalingInheritance{State: effective.InheritanceNotRecorded},
		},
	}
	for _, source := range projectAutoscaleExplainSources(isvc, pinned, fixedExplainClock().Now()) {
		assert.NotEqual(t, "BaseModel", source.Kind, "a live model must not be attributed to a pinned desired configuration")
	}
}

func TestProjectExplainPinnedRevisionKeepsLiveInheritanceIndependent(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	autoSync := false
	pinnedSpec := baseExplainRuntime(kedaExplainAutoscaler())
	fullHash, shortHash, err := runtimerevision.Hash(pinnedSpec)
	require.NoError(t, err)
	assert.Len(t, fullHash, 64)
	revisionName := runtimerevision.Name(runtimerevision.KindServingRuntime, isvc.Namespace, "runtime", shortHash)
	isvc.Spec.Runtime.AutoSync = &autoSync
	isvc.Spec.Runtime.Revision = &revisionName
	isvc.Status.PinnedRevisionName = revisionName
	reported := reportedEngine(v1beta1.AutoscalerKEDA, v1beta1.AutoscalerManagedByOME, "runtime", "apps/v1", "Deployment", "svc-engine")
	reported.Autoscaler.Conditions[0].Type = "Ready"
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: reported}
	raw, err := json.Marshal(pinnedSpec)
	require.NoError(t, err)
	revision := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name: revisionName, Namespace: "ome", UID: types.UID("revision-uid"),
			Labels: map[string]string{
				constants.RuntimeRevisionOfLabelKey:          "runtime",
				constants.RuntimeRevisionOfKindLabelKey:      string(runtimerevision.KindServingRuntime),
				constants.RuntimeRevisionOfNamespaceLabelKey: isvc.Namespace,
				constants.RuntimeRevisionHashLabelKey:        shortHash,
			},
			Annotations: map[string]string{
				constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue,
			},
		},
		Data: runtime.RawExtension{Raw: raw}, Revision: 1,
	}
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	liveRuntime := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: isvc.Namespace, UID: types.UID("live-uid"), ResourceVersion: "8", Generation: 2},
		Spec:       *baseExplainRuntime(nil),
	}
	live := effective.NewRuntimeResolver(ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(liveRuntime).Build())
	pins, err := effective.NewRuntimePinResolver(fake.NewSimpleClientset(revision).AppsV1(), live, "ome", paging.Limits{
		PageSize: 500, MaxItems: 1000, MaxPages: 2, RequestTimeout: time.Second,
	})
	require.NoError(t, err)
	state, err := pins.Resolve(context.Background(), isvc, effective.RuntimeResolveOptions{IncludeHistory: false})
	require.NoError(t, err)

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	assert.Equal(t, reportv1alpha1.AutoscaleExplainConsistent, got.Content.Summary.State)
	assert.Equal(t, reportv1alpha1.ConfigurationOriginControllerRevision, got.Content.ActiveConfiguration.Origin)
	assert.Equal(t, reportv1alpha1.InheritanceStateNotRecorded, got.Content.ActiveConfiguration.Inheritance.State)
	assert.Empty(t, got.Content.ActiveConfiguration.Inheritance.Sources)
	require.NotNil(t, got.Content.ActiveConfiguration.Revision)
	assert.Equal(t, revisionName, got.Content.ActiveConfiguration.Revision.Name)
	assert.Equal(t, reportv1alpha1.RuntimeRevisionRoleActive, got.Content.ActiveConfiguration.Revision.Role)
	assert.Equal(t, reportv1alpha1.AutoscaleClassKEDA, got.Content.Components[0].Desired.Class, "desired must come from the pinned payload")
	assert.Empty(t, got.Warnings)
}

func explainISVC(mode constants.DeploymentModeType) *v1beta1.InferenceService {
	kind := runtimeselector.KindServingRuntime
	result := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "workloads", UID: types.UID("isvc-uid"), ResourceVersion: "17", Generation: 4},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: &mode,
			Runtime:        &v1beta1.ServingRuntimeRef{Name: "runtime", Kind: &kind},
			Engine:         &v1beta1.EngineSpec{},
		},
	}
	result.Status.ObservedGeneration = 4
	return result
}

func baseExplainRuntime(autoscaler *v1beta1.ComponentAutoscaler) *v1beta1.ServingRuntimeSpec {
	return &v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Autoscaler: autoscaler},
	}}
}

func resolveExplainState(t *testing.T, isvc *v1beta1.InferenceService, runtimeSpec *v1beta1.ServingRuntimeSpec) *effective.RuntimeState {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	runtimeObject := &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: isvc.Namespace, UID: types.UID("runtime-uid"), ResourceVersion: "8", Generation: 2},
		Spec:       *runtimeSpec.DeepCopy(),
	}
	runtimeClient := ctrlfake.NewClientBuilder().WithScheme(scheme).WithObjects(runtimeObject).Build()
	live := effective.NewRuntimeResolver(runtimeClient)
	pins, err := effective.NewRuntimePinResolver(fake.NewSimpleClientset().AppsV1(), live, "ome", paging.Limits{
		PageSize: 500, MaxItems: 1000, MaxPages: 2, RequestTimeout: time.Second,
	})
	require.NoError(t, err)
	state, err := pins.Resolve(context.Background(), isvc, effective.RuntimeResolveOptions{IncludeHistory: false})
	require.NoError(t, err)
	require.True(t, state.MatchesInferenceService(isvc))
	return state
}

func reportedEngine(class v1beta1.AutoscalerClass, owner v1beta1.AutoscalerManagedBy, source, apiVersion, kind, name string) v1beta1.ComponentStatusSpec {
	return v1beta1.ComponentStatusSpec{
		Autoscaler: &v1beta1.ComponentAutoscalerStatus{
			Class: class, ManagedBy: owner, SpecSource: source, CurrentReplicas: 1, DesiredReplicas: 1,
			Conditions: []metav1.Condition{{Type: "ScalingActive", Status: metav1.ConditionTrue, Reason: "Observed", LastTransitionTime: metav1.NewTime(time.Date(2026, time.September, 7, 18, 0, 0, 0, time.UTC))}},
		},
		ScaleTargetRef: &v1beta1.ScaleTargetRef{APIVersion: apiVersion, Kind: kind, Name: name},
	}
}

func kedaExplainAutoscaler() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerKEDA,
		Keda:  &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{Type: "prometheus"}}},
	}
}

func fixedExplainClock() reportv1alpha1.Clock {
	return reportv1alpha1.ClockFunc(func() time.Time {
		return time.Date(2026, time.September, 7, 19, 0, 0, 0, time.UTC)
	})
}
