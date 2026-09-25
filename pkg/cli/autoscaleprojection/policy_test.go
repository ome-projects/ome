package autoscaleprojection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestProjectPolicyCurrentProvenance(t *testing.T) {
	status := policyAutoscalerStatus("policy")
	status.Policy = validPolicyProvenance()
	status.Conditions = append(status.Conditions, autoscalerResolved(
		metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy,
	))

	got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, &reportv1alpha1.AutoscalePolicyStatus{
		State:          reportv1alpha1.AutoscalePolicyCurrent,
		Resolution:     reportv1alpha1.AutoscalePolicyRenderedFromPolicy,
		Name:           "request-activity-v1",
		Generation:     17,
		PortableDigest: "pv1:0123456789ab",
		RenderDigest:   "rv1:abcdef012345",
	}, got.Content.Components[0].Policy)
	assert.Empty(t, got.Content.Issues)
	assert.Equal(t, reportv1alpha1.AutoscaleStateReported, got.Content.Summary.State)

	status.Policy.Name = "mutated-after-projection"
	assert.Equal(t, "request-activity-v1", got.Content.Components[0].Policy.Name)
}

func TestProjectPolicyRejectsZeroGeneration(t *testing.T) {
	status := policyAutoscalerStatus("policy")
	status.Policy = validPolicyProvenance()
	status.Policy.ObservedGeneration = 0
	status.Conditions = append(status.Conditions, autoscalerResolved(
		metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy,
	))

	got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

	require.NoError(t, err)
	assert.Nil(t, got.Content.Components[0].Policy)
	assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, got.Content.Components[0].State)
	assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleIssue{
		Code:      reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid,
		Component: reportv1alpha1.RuntimeComponentEngine,
	})
}

func TestProjectPolicyRejectsStaleOrImpossibleConditionGeneration(t *testing.T) {
	for _, observedGeneration := range []int64{0, 6, 8} {
		t.Run(fmt.Sprintf("observed-%d", observedGeneration), func(t *testing.T) {
			status := policyAutoscalerStatus("policy")
			status.Policy = validPolicyProvenance()
			condition := autoscalerResolved(
				metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy,
			)
			condition.ObservedGeneration = observedGeneration
			status.Conditions = append(status.Conditions, condition)

			got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

			require.NoError(t, err)
			assert.Nil(t, got.Content.Components[0].Policy)
			assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, got.Content.Components[0].State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleIssue{
				Code:      reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid,
				Component: reportv1alpha1.RuntimeComponentEngine,
			})
		})
	}
}

func TestProjectPolicyBindsCurrentAndShadowedNamesToDeclaredReference(t *testing.T) {
	for _, test := range []struct {
		name   string
		status *omev1beta1.ComponentAutoscalerStatus
	}{
		{
			name: "current",
			status: policyStatusWith(
				"policy",
				&omev1beta1.AutoscalerPolicyProvenance{
					Name: "different-policy", ObservedGeneration: 17,
					PortableDigest: "pv1:0123456789ab", ResolvedDigest: "rv1:abcdef012345",
				},
				nil,
				autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy),
			),
		},
		{
			name: "shadowed",
			status: policyStatusWith(
				"isvc", nil, &omev1beta1.ShadowedAutoscalerPolicy{Name: "different-policy"},
				autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonInlinePrecedence),
			),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := Project(inferenceServiceWithDeclaredPolicy(test.status), fixedClock())

			require.NoError(t, err)
			assert.Nil(t, got.Content.Components[0].Policy)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleIssue{
				Code:      reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid,
				Component: reportv1alpha1.RuntimeComponentEngine,
			})
		})
	}
}

func TestProjectPolicyRequiresDeclaredReferenceAndMatchingInlineIntent(t *testing.T) {
	current := policyStatusWith(
		"policy", validPolicyProvenance(), nil,
		autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy),
	)
	shadowed := policyStatusWith(
		"isvc", nil, &omev1beta1.ShadowedAutoscalerPolicy{Name: "request-activity-v1"},
		autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonInlinePrecedence),
	)
	for _, test := range []struct {
		name string
		isvc *omev1beta1.InferenceService
	}{
		{name: "condition without reference", isvc: inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, current)},
		{name: "current with unexpected inline", isvc: inferenceServiceWithDeclaredPolicy(policyStatusWith(
			"isvc", validPolicyProvenance(), nil,
			autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy),
		))},
		{name: "shadowed without inline", isvc: func() *omev1beta1.InferenceService {
			isvc := inferenceServiceWithDeclaredPolicy(shadowed)
			isvc.Spec.Engine.Autoscaler = nil
			return isvc
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := Project(test.isvc, fixedClock())

			require.NoError(t, err)
			assert.Nil(t, got.Content.Components[0].Policy)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleIssue{
				Code:      reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid,
				Component: reportv1alpha1.RuntimeComponentEngine,
			})
		})
	}
}

func TestProjectPolicyRejectsMissingStatusEvidenceOrInvalidReference(t *testing.T) {
	missing := inferenceServiceWithDeclaredPolicy(reportedHPA())
	currentStatus := policyStatusWith(
		"policy", validPolicyProvenance(), nil,
		autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy),
	)
	invalidName := inferenceServiceWithDeclaredPolicy(currentStatus)
	invalidName.Spec.Engine.AutoscalerPolicyRef.Name = "SECRET_INVALID_POLICY"
	invalidKind := inferenceServiceWithDeclaredPolicy(currentStatus)
	invalidKind.Spec.Engine.AutoscalerPolicyRef.Kind = "ClusterAutoscalerPolicy"

	for _, test := range []struct {
		name string
		isvc *omev1beta1.InferenceService
	}{
		{name: "missing status evidence", isvc: missing},
		{name: "invalid reference name", isvc: invalidName},
		{name: "unsupported reference kind", isvc: invalidKind},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := Project(test.isvc, fixedClock())

			require.NoError(t, err)
			assert.Nil(t, got.Content.Components[0].Policy)
			assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, got.Content.Components[0].State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleIssue{
				Code:      reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid,
				Component: reportv1alpha1.RuntimeComponentEngine,
			})
			encoded, marshalErr := json.Marshal(got)
			require.NoError(t, marshalErr)
			assert.NotContains(t, string(encoded), "SECRET_INVALID_POLICY")
		})
	}
}

func TestProjectPolicyInlineShadowPreview(t *testing.T) {
	status := policyAutoscalerStatus("isvc")
	status.ShadowedPolicyRef = &omev1beta1.ShadowedAutoscalerPolicy{
		Name: "request-activity-v1", PortableDigest: "pv1:0123456789ab",
		WouldRenderDigest: "rv1:abcdef012345",
	}
	status.Conditions = append(status.Conditions, autoscalerResolved(
		metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonInlinePrecedence,
	))

	got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

	require.NoError(t, err)
	require.Len(t, got.Content.Components, 1)
	assert.Equal(t, &reportv1alpha1.AutoscalePolicyStatus{
		State:          reportv1alpha1.AutoscalePolicyShadowed,
		Resolution:     reportv1alpha1.AutoscalePolicyInlinePrecedence,
		Name:           "request-activity-v1",
		PortableDigest: "pv1:0123456789ab",
		RenderDigest:   "rv1:abcdef012345",
	}, got.Content.Components[0].Policy)
	assert.Empty(t, got.Content.Issues)
}

func TestProjectPolicyInlineShadowWithoutPreviewDigests(t *testing.T) {
	status := policyAutoscalerStatus("isvc")
	status.ShadowedPolicyRef = &omev1beta1.ShadowedAutoscalerPolicy{Name: "request-activity-v1"}
	status.Conditions = append(status.Conditions, autoscalerResolved(
		metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonInlinePrecedence,
	))

	got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

	require.NoError(t, err)
	require.NotNil(t, got.Content.Components[0].Policy)
	assert.Equal(t, reportv1alpha1.AutoscalePolicyShadowed, got.Content.Components[0].Policy.State)
	assert.Equal(t, "request-activity-v1", got.Content.Components[0].Policy.Name)
	assert.Empty(t, got.Content.Components[0].Policy.PortableDigest)
	assert.Empty(t, got.Content.Components[0].Policy.RenderDigest)
}

func TestProjectPolicyHeldLastKnownGood(t *testing.T) {
	for _, test := range []struct {
		name       string
		apiReason  string
		resolution reportv1alpha1.AutoscalePolicyResolution
	}{
		{name: "policy missing", apiReason: omev1beta1.AutoscalerResolvedReasonPolicyNotFound, resolution: reportv1alpha1.AutoscalePolicyNotFound},
		{name: "policy invalid", apiReason: omev1beta1.AutoscalerResolvedReasonPolicyInvalid, resolution: reportv1alpha1.AutoscalePolicyInvalid},
		{name: "auth missing", apiReason: omev1beta1.AutoscalerResolvedReasonAuthNotFound, resolution: reportv1alpha1.AutoscalePolicyAuthNotFound},
		{name: "class unavailable", apiReason: omev1beta1.AutoscalerResolvedReasonClassUnavailable, resolution: reportv1alpha1.AutoscalePolicyClassUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := policyAutoscalerStatus("policy")
			status.Policy = validPolicyProvenance()
			status.Conditions = append(status.Conditions, autoscalerResolved(metav1.ConditionFalse, test.apiReason))

			got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

			require.NoError(t, err)
			require.NotNil(t, got.Content.Components[0].Policy)
			assert.Equal(t, reportv1alpha1.AutoscalePolicyHeld, got.Content.Components[0].Policy.State)
			assert.Equal(t, test.resolution, got.Content.Components[0].Policy.Resolution)
			assert.Equal(t, "request-activity-v1", got.Content.Components[0].Policy.Name)
			assert.Equal(t, int64(17), got.Content.Components[0].Policy.Generation)
			assert.Empty(t, got.Content.Issues)
		})
	}
}

func TestProjectPolicyHeldMayRetainPriorPolicyName(t *testing.T) {
	status := policyAutoscalerStatus("policy")
	status.Policy = validPolicyProvenance()
	status.Policy.Name = "previous-policy"
	status.Conditions = append(status.Conditions, autoscalerResolved(
		metav1.ConditionFalse, omev1beta1.AutoscalerResolvedReasonPolicyNotFound,
	))

	got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

	require.NoError(t, err)
	require.NotNil(t, got.Content.Components[0].Policy)
	assert.Equal(t, reportv1alpha1.AutoscalePolicyHeld, got.Content.Components[0].Policy.State)
	assert.Equal(t, "previous-policy", got.Content.Components[0].Policy.Name)
	assert.Empty(t, got.Content.Issues)
}

func TestProjectPolicyResolutionWithoutRetainedProvenance(t *testing.T) {
	status := policyAutoscalerStatus("policy")
	status.Conditions = append(status.Conditions, autoscalerResolved(
		metav1.ConditionFalse, omev1beta1.AutoscalerResolvedReasonPolicyNotFound,
	))

	got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

	require.NoError(t, err)
	require.NotNil(t, got.Content.Components[0].Policy)
	assert.Equal(t, reportv1alpha1.AutoscalePolicyUnresolved, got.Content.Components[0].Policy.State)
	assert.Equal(t, reportv1alpha1.AutoscalePolicyNotFound, got.Content.Components[0].Policy.Resolution)
	assert.Equal(t, "request-activity-v1", got.Content.Components[0].Policy.Name)
	assert.Empty(t, got.Content.Issues)
}

func TestProjectPolicyUnsupportedDeploymentMode(t *testing.T) {
	status := policyAutoscalerStatus("default")
	status.Conditions = append(status.Conditions, autoscalerResolved(
		metav1.ConditionFalse, omev1beta1.AutoscalerResolvedReasonUnsupportedMode,
	))

	got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

	require.NoError(t, err)
	require.NotNil(t, got.Content.Components[0].Policy)
	assert.Equal(t, reportv1alpha1.AutoscalePolicyUnsupported, got.Content.Components[0].Policy.State)
	assert.Equal(t, reportv1alpha1.AutoscalePolicyUnsupportedMode, got.Content.Components[0].Policy.Resolution)
	assert.Equal(t, "request-activity-v1", got.Content.Components[0].Policy.Name)
	assert.Empty(t, got.Content.Issues)
}

func TestProjectPolicyRejectsMalformedProvenance(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*omev1beta1.AutoscalerPolicyProvenance)
	}{
		{name: "empty name", mutate: func(value *omev1beta1.AutoscalerPolicyProvenance) { value.Name = "" }},
		{name: "invalid name", mutate: func(value *omev1beta1.AutoscalerPolicyProvenance) { value.Name = "SECRET_NAME" }},
		{name: "negative generation", mutate: func(value *omev1beta1.AutoscalerPolicyProvenance) { value.ObservedGeneration = -1 }},
		{name: "portable digest prefix", mutate: func(value *omev1beta1.AutoscalerPolicyProvenance) { value.PortableDigest = "rv1:0123456789ab" }},
		{name: "portable digest uppercase", mutate: func(value *omev1beta1.AutoscalerPolicyProvenance) { value.PortableDigest = "pv1:0123456789AB" }},
		{name: "portable digest length", mutate: func(value *omev1beta1.AutoscalerPolicyProvenance) { value.PortableDigest = "pv1:0123" }},
		{name: "resolved digest prefix", mutate: func(value *omev1beta1.AutoscalerPolicyProvenance) { value.ResolvedDigest = "pv1:abcdef012345" }},
		{name: "resolved digest uppercase", mutate: func(value *omev1beta1.AutoscalerPolicyProvenance) { value.ResolvedDigest = "rv1:ABCDEF012345" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := policyAutoscalerStatus("policy")
			status.Policy = validPolicyProvenance()
			test.mutate(status.Policy)
			status.Conditions = append(status.Conditions, autoscalerResolved(
				metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy,
			))

			got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

			require.NoError(t, err)
			require.Len(t, got.Content.Components, 1)
			assert.Nil(t, got.Content.Components[0].Policy)
			assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, got.Content.Components[0].State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleIssue{
				Code: reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid, Component: reportv1alpha1.RuntimeComponentEngine,
			})
			assert.Equal(t, reportv1alpha1.AutoscaleStateInvalid, got.Content.Summary.State)
		})
	}
}

func TestProjectPolicyRejectsMalformedShadowPreview(t *testing.T) {
	for _, shadow := range []*omev1beta1.ShadowedAutoscalerPolicy{
		{Name: "SECRET_NAME"},
		{Name: "request-activity-v1", PortableDigest: "pv1:0123456789ab"},
		{Name: "request-activity-v1", WouldRenderDigest: "rv1:abcdef012345"},
		{Name: "request-activity-v1", PortableDigest: "pv1:0123456789zz", WouldRenderDigest: "rv1:abcdef012345"},
	} {
		status := policyAutoscalerStatus("isvc")
		status.ShadowedPolicyRef = shadow
		status.Conditions = append(status.Conditions, autoscalerResolved(
			metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonInlinePrecedence,
		))

		got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

		require.NoError(t, err)
		assert.Nil(t, got.Content.Components[0].Policy)
		assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleIssue{
			Code: reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid, Component: reportv1alpha1.RuntimeComponentEngine,
		})
	}
}

func TestProjectPolicyRejectsContradictoryEvidence(t *testing.T) {
	currentCondition := autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy)
	inlineCondition := autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonInlinePrecedence)
	heldCondition := autoscalerResolved(metav1.ConditionFalse, omev1beta1.AutoscalerResolvedReasonPolicyInvalid)
	unsupportedCondition := autoscalerResolved(metav1.ConditionFalse, omev1beta1.AutoscalerResolvedReasonUnsupportedMode)
	unknownCondition := autoscalerResolved(metav1.ConditionUnknown, omev1beta1.AutoscalerResolvedReasonPolicyInvalid)
	unknownReason := autoscalerResolved(metav1.ConditionFalse, "SECRET_REASON")
	shadow := &omev1beta1.ShadowedAutoscalerPolicy{Name: "request-activity-v1"}
	for _, test := range []struct {
		name   string
		status *omev1beta1.ComponentAutoscalerStatus
	}{
		{name: "live policy with inline source", status: policyStatusWith("isvc", validPolicyProvenance(), nil, currentCondition)},
		{name: "live policy without provenance", status: policyStatusWith("policy", nil, nil, currentCondition)},
		{name: "live and shadow together", status: policyStatusWith("policy", validPolicyProvenance(), shadow, currentCondition)},
		{name: "shadow with policy source", status: policyStatusWith("policy", nil, shadow, inlineCondition)},
		{name: "shadow and retained together", status: policyStatusWith("isvc", validPolicyProvenance(), shadow, inlineCondition)},
		{name: "held with shadow", status: policyStatusWith("policy", validPolicyProvenance(), shadow, heldCondition)},
		{name: "held with inline source", status: policyStatusWith("isvc", validPolicyProvenance(), nil, heldCondition)},
		{name: "unsupported with policy source", status: policyStatusWith("policy", nil, nil, unsupportedCondition)},
		{name: "unsupported with provenance", status: policyStatusWith("default", validPolicyProvenance(), nil, unsupportedCondition)},
		{name: "unknown condition status", status: policyStatusWith("policy", nil, nil, unknownCondition)},
		{name: "unknown condition reason", status: policyStatusWith("policy", nil, nil, unknownReason)},
		{name: "provenance without condition", status: policyStatusWith("policy", validPolicyProvenance(), nil)},
		{name: "policy source without condition", status: policyStatusWith("policy", nil, nil)},
		{name: "shadow without condition", status: policyStatusWith("isvc", nil, shadow)},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := Project(inferenceServiceWithDeclaredPolicy(test.status), fixedClock())

			require.NoError(t, err)
			assert.Nil(t, got.Content.Components[0].Policy)
			assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, got.Content.Components[0].State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleIssue{
				Code: reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid, Component: reportv1alpha1.RuntimeComponentEngine,
			})
		})
	}
}

func TestProjectPolicyRejectsDuplicateOrConflictingResolvedConditions(t *testing.T) {
	for _, test := range []struct {
		name       string
		conditions []metav1.Condition
	}{
		{name: "duplicate", conditions: []metav1.Condition{
			autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy),
			autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy),
		}},
		{name: "conflicting", conditions: []metav1.Condition{
			autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy),
			autoscalerResolved(metav1.ConditionFalse, omev1beta1.AutoscalerResolvedReasonPolicyNotFound),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := policyAutoscalerStatus("policy")
			status.Policy = validPolicyProvenance()
			status.Conditions = append(status.Conditions, test.conditions...)

			got, err := Project(inferenceServiceWithDeclaredPolicy(status), fixedClock())

			require.NoError(t, err)
			assert.Nil(t, got.Content.Components[0].Policy)
			assert.Equal(t, reportv1alpha1.AutoscaleComponentInvalid, got.Content.Components[0].State)
			assert.Contains(t, got.Content.Issues, reportv1alpha1.AutoscaleIssue{
				Code: reportv1alpha1.AutoscaleIssuePolicyConditionConflict, Component: reportv1alpha1.RuntimeComponentEngine,
			})
		})
	}
}

func TestProjectPolicyNeverLeaksMessagesOrUnboundedData(t *testing.T) {
	status := policyAutoscalerStatus("policy")
	status.Policy = validPolicyProvenance()
	condition := autoscalerResolved(metav1.ConditionTrue, omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy)
	condition.Message = "SECRET_POLICY_MESSAGE provider=https://secret.example token=SECRET_TOKEN"
	status.Conditions = append(status.Conditions, condition)
	isvc := inferenceServiceWithDeclaredPolicy(status)
	isvc.Spec.Engine.Annotations = map[string]string{"provider": "SECRET_PROVIDER_DATA"}

	got, err := Project(isvc, fixedClock())
	require.NoError(t, err)

	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	for _, secret := range []string{"SECRET_POLICY_MESSAGE", "secret.example", "SECRET_TOKEN", "SECRET_PROVIDER_DATA"} {
		assert.NotContains(t, string(encoded), secret)
	}
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var output bytes.Buffer
		require.NoError(t, report.Write(&output, format, got))
		assert.NotContains(t, output.String(), "SECRET_")
	}
}

func policyAutoscalerStatus(source string) *omev1beta1.ComponentAutoscalerStatus {
	status := reportedHPA()
	status.SpecSource = source
	return status
}

func inferenceServiceWithDeclaredPolicy(
	status *omev1beta1.ComponentAutoscalerStatus,
) *omev1beta1.InferenceService {
	isvc := inferenceServiceWithAutoscaler(omev1beta1.EngineComponent, status)
	extension := omev1beta1.ComponentExtensionSpec{
		AutoscalerPolicyRef: &omev1beta1.AutoscalerPolicyRef{Name: "request-activity-v1"},
	}
	if status.SpecSource == "isvc" {
		extension.Autoscaler = &omev1beta1.ComponentAutoscaler{Class: omev1beta1.AutoscalerHPA}
	}
	isvc.Spec.Engine = &omev1beta1.EngineSpec{ComponentExtensionSpec: extension}
	return isvc
}

func policyStatusWith(
	source string,
	policy *omev1beta1.AutoscalerPolicyProvenance,
	shadow *omev1beta1.ShadowedAutoscalerPolicy,
	conditions ...metav1.Condition,
) *omev1beta1.ComponentAutoscalerStatus {
	status := policyAutoscalerStatus(source)
	status.Policy = policy
	status.ShadowedPolicyRef = shadow
	status.Conditions = append(status.Conditions, conditions...)
	return status
}

func validPolicyProvenance() *omev1beta1.AutoscalerPolicyProvenance {
	return &omev1beta1.AutoscalerPolicyProvenance{
		Name: "request-activity-v1", ObservedGeneration: 17,
		PortableDigest: "pv1:0123456789ab", ResolvedDigest: "rv1:abcdef012345",
	}
}

func autoscalerResolved(status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{
		Type: omev1beta1.AutoscalerResolvedCondition, Status: status, Reason: reason,
		ObservedGeneration: 7,
		LastTransitionTime: metav1.NewTime(time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC)),
		Message:            "ignored by projection",
	}
}
