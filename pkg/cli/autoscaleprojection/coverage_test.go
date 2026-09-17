package autoscaleprojection

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

func TestProjectVirtualExplainNeedsNoRuntimeEvidence(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Annotations = map[string]string{
		constants.DeploymentMode: "VirtualDeployment",
		"private.example/token":  "secret-virtual-token",
	}
	isvc.Spec.Runtime.Name = "runtime-that-does-not-exist"

	got, err := ProjectVirtualExplain(isvc, fixedExplainClock())

	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleExplainUnsupported, got.Content.Summary.State)
	require.Equal(t, reportv1alpha1.AutoscaleActiveConfigurationUnavailable, got.Content.ActiveConfiguration.State)
	require.Len(t, got.Content.Components, 1)
	component := got.Content.Components[0]
	require.Equal(t, reportv1alpha1.DeploymentModeVirtualDeployment, component.DeploymentMode)
	require.Equal(t, reportv1alpha1.DeploymentModeSourceServiceAnnotation, component.DeploymentModeSource)
	require.Equal(t, reportv1alpha1.AutoscaleDesiredUnsupported, component.Desired.State)
	require.Nil(t, component.Desired.Target)
	require.Contains(t, component.Reconciliation.Issues, reportv1alpha1.AutoscaleExplainIssueDeploymentModeUnsupported)
	require.Len(t, got.Sources, 1)
	require.Equal(t, "InferenceService", got.Sources[0].Kind)
	require.Equal(t, "isvc-uid", got.Sources[0].UID)
	require.Contains(t, got.Warnings, reportv1alpha1.AutoscaleExplainWarning{Code: reportv1alpha1.AutoscaleExplainWarningUnsupportedConfiguration})
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "secret-virtual-token")
	require.NotContains(t, string(encoded), "runtime-that-does-not-exist")
}

func TestProjectVirtualExplainRejectsNonVirtualInput(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)

	got, err := ProjectVirtualExplain(isvc, fixedExplainClock())

	require.ErrorIs(t, err, effective.ErrAutoscalingEvidenceInvalid)
	require.Equal(t, reportv1alpha1.AutoscaleExplainReport{}, got)
}

func TestProjectExplainFutureObservedGenerationInvalidatesReportedEvidence(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	isvc.Status.ObservedGeneration = isvc.Generation + 1
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: reportedEngine("secret-class", "secret-owner", "secret-source", "secret/v1", "Secret", "secret-target"),
	}
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))

	got, err := ProjectExplain(isvc, state, fixedExplainClock())

	require.NoError(t, err)
	require.Equal(t, reportv1alpha1.AutoscaleExplainInvalid, got.Content.Summary.State)
	require.Equal(t, reportv1alpha1.StatusFreshnessInvalid, got.Content.Summary.StatusFreshness)
	require.Len(t, got.Content.Components, 1)
	require.Equal(t, reportv1alpha1.AutoscaleReportedInvalid, got.Content.Components[0].Reported.State)
	require.Equal(t, reportv1alpha1.AutoscaleTargetInvalid, got.Content.Components[0].Reported.TargetState)
	require.Nil(t, got.Content.Components[0].Reported.Target)
	require.Contains(t, got.Content.Components[0].Reconciliation.Issues, reportv1alpha1.AutoscaleExplainIssueStatusInvalid)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{"secret-class", "secret-owner", "secret-source", "secret-target", "secret/v1"} {
		require.NotContains(t, string(encoded), secret)
	}
}

func TestProjectResolvedExplainUnavailableInheritanceIsBounded(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))
	base, err := effective.ResolveAutoscaling(isvc, state)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		reason effective.InheritanceUnavailableReason
		want   reportv1alpha1.UnavailableReason
	}{
		{"not found", effective.InheritanceNotFound, reportv1alpha1.UnavailableNotFound},
		{"forbidden", effective.InheritanceForbidden, reportv1alpha1.UnavailableForbidden},
		{"cycle", effective.InheritanceCycle, reportv1alpha1.UnavailableCycle},
		{"depth", effective.InheritanceMaxDepthExceeded, reportv1alpha1.UnavailableMaxDepthExceeded},
		{"malformed", effective.InheritanceMalformed, reportv1alpha1.UnavailableMalformedPayload},
		{"unreadable", effective.InheritanceUnreadable, reportv1alpha1.UnavailableUnreadable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolution := base
			resolution.Active.Inheritance = effective.AutoscalingInheritance{
				State: effective.InheritanceUnavailable, UnavailableReason: tc.reason,
			}
			resolution.Issues = []effective.AutoscalingIssueCode{effective.AutoscalingIssueInheritanceUnavailable}

			got, err := projectResolvedExplain(isvc, resolution, fixedExplainClock())

			require.NoError(t, err)
			require.Equal(t, reportv1alpha1.AutoscaleExplainPartial, got.Content.Summary.State)
			require.NotNil(t, got.Content.ActiveConfiguration.Inheritance)
			require.Equal(t, reportv1alpha1.InheritanceStateUnavailable, got.Content.ActiveConfiguration.Inheritance.State)
			require.Equal(t, tc.want, got.Content.ActiveConfiguration.Inheritance.UnavailableReason)
			require.Empty(t, got.Content.ActiveConfiguration.Inheritance.Sources)
			require.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleExplainIssue{Code: reportv1alpha1.AutoscaleExplainIssueInheritanceUnavailable})
			require.Contains(t, got.Warnings, reportv1alpha1.AutoscaleExplainWarning{Code: reportv1alpha1.AutoscaleExplainWarningPartialData})
			require.Len(t, got.Sources, 2, "unavailable inheritance must not invent observed ancestor sources")
			require.Equal(t, runtimeselector.KindServingRuntime, got.Sources[1].Kind)
		})
	}
}

func TestProjectResolvedExplainRejectsContradictoryInheritance(t *testing.T) {
	isvc := explainISVC(constants.RawDeployment)
	state := resolveExplainState(t, isvc, baseExplainRuntime(nil))
	base, err := effective.ResolveAutoscaling(isvc, state)
	require.NoError(t, err)

	for _, tc := range []struct {
		name        string
		inheritance effective.AutoscalingInheritance
	}{
		{"observed without sources", effective.AutoscalingInheritance{State: effective.InheritanceObserved}},
		{"unavailable with sources", effective.AutoscalingInheritance{
			State: effective.InheritanceUnavailable, UnavailableReason: effective.InheritanceNotFound,
			Sources: []effective.AutoscalingRuntimeReference{{Kind: runtimeselector.KindServingRuntime, Namespace: isvc.Namespace, Name: "secret-parent"}},
		}},
		{"unknown reason", effective.AutoscalingInheritance{State: effective.InheritanceUnavailable, UnavailableReason: "secret-reason"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolution := base
			resolution.Active.Inheritance = tc.inheritance

			got, err := projectResolvedExplain(isvc, resolution, fixedExplainClock())

			require.ErrorIs(t, err, ErrExplainProjectionInvalid)
			require.Equal(t, reportv1alpha1.AutoscaleExplainReport{}, got)
		})
	}
}
