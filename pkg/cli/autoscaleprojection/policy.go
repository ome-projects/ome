package autoscaleprojection

import (
	"regexp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var (
	portablePolicyDigestPattern = regexp.MustCompile(`^pv1:[0-9a-f]{12}$`)
	renderPolicyDigestPattern   = regexp.MustCompile(`^rv1:[0-9a-f]{12}$`)
)

func projectPolicy(
	status *omev1beta1.ComponentAutoscalerStatus,
	isvcGeneration int64,
	extension *omev1beta1.ComponentExtensionSpec,
) (*reportv1alpha1.AutoscalePolicyStatus, reportv1alpha1.AutoscaleIssueCode) {
	resolved := make([]metav1.Condition, 0, 1)
	for _, condition := range status.Conditions {
		if condition.Type == omev1beta1.AutoscalerResolvedCondition {
			resolved = append(resolved, condition)
		}
	}
	if len(resolved) > 1 {
		return nil, reportv1alpha1.AutoscaleIssuePolicyConditionConflict
	}
	reference, inline, referenceOK := declaredPolicyIntent(extension)
	if !referenceOK {
		return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
	}
	if len(resolved) == 0 {
		if reference != nil || status.Policy != nil || status.ShadowedPolicyRef != nil || status.SpecSource == "policy" {
			return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
		}
		return nil, ""
	}

	condition := resolved[0]
	if reference == nil || isvcGeneration <= 0 || condition.ObservedGeneration != isvcGeneration ||
		len(metav1validation.ValidateCondition(condition, field.NewPath("condition"))) != 0 {
		return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
	}
	switch condition.Reason {
	case omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy:
		if condition.Status != metav1.ConditionTrue || inline || status.SpecSource != "policy" ||
			status.Policy == nil || status.ShadowedPolicyRef != nil {
			return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
		}
		policy, ok := projectPolicyProvenance(status.Policy, reference.Name)
		if !ok {
			return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
		}
		policy.State = reportv1alpha1.AutoscalePolicyCurrent
		policy.Resolution = reportv1alpha1.AutoscalePolicyRenderedFromPolicy
		return policy, ""

	case omev1beta1.AutoscalerResolvedReasonInlinePrecedence:
		if condition.Status != metav1.ConditionTrue || !inline || status.SpecSource != "isvc" ||
			status.Policy != nil || status.ShadowedPolicyRef == nil {
			return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
		}
		policy, ok := projectShadowedPolicy(status.ShadowedPolicyRef, reference.Name)
		if !ok {
			return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
		}
		return policy, ""

	case omev1beta1.AutoscalerResolvedReasonPolicyNotFound,
		omev1beta1.AutoscalerResolvedReasonPolicyInvalid,
		omev1beta1.AutoscalerResolvedReasonAuthNotFound,
		omev1beta1.AutoscalerResolvedReasonClassUnavailable:
		if condition.Status != metav1.ConditionFalse || inline || status.SpecSource != "policy" ||
			status.ShadowedPolicyRef != nil {
			return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
		}
		resolution := policyResolution(condition.Reason)
		if status.Policy == nil {
			return &reportv1alpha1.AutoscalePolicyStatus{
				State: reportv1alpha1.AutoscalePolicyUnresolved, Resolution: resolution, Name: reference.Name,
			}, ""
		}
		// A held render may legitimately come from the previously declared
		// policy when a newly declared policy cannot be resolved.
		policy, ok := projectPolicyProvenance(status.Policy, "")
		if !ok {
			return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
		}
		policy.State = reportv1alpha1.AutoscalePolicyHeld
		policy.Resolution = resolution
		return policy, ""

	case omev1beta1.AutoscalerResolvedReasonUnsupportedMode:
		if condition.Status != metav1.ConditionFalse || !nonPolicySpecSource(status.SpecSource) ||
			status.Policy != nil || status.ShadowedPolicyRef != nil {
			return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
		}
		return &reportv1alpha1.AutoscalePolicyStatus{
			State: reportv1alpha1.AutoscalePolicyUnsupported, Resolution: reportv1alpha1.AutoscalePolicyUnsupportedMode,
			Name: reference.Name,
		}, ""

	default:
		return nil, reportv1alpha1.AutoscaleIssuePolicyEvidenceInvalid
	}
}

func projectPolicyProvenance(
	provenance *omev1beta1.AutoscalerPolicyProvenance,
	expectedName string,
) (*reportv1alpha1.AutoscalePolicyStatus, bool) {
	if provenance == nil || provenance.Name == "" ||
		len(utilvalidation.IsDNS1123Subdomain(provenance.Name)) != 0 ||
		(expectedName != "" && provenance.Name != expectedName) ||
		provenance.ObservedGeneration <= 0 ||
		!portablePolicyDigestPattern.MatchString(provenance.PortableDigest) ||
		!renderPolicyDigestPattern.MatchString(provenance.ResolvedDigest) {
		return nil, false
	}
	return &reportv1alpha1.AutoscalePolicyStatus{
		Name: provenance.Name, Generation: provenance.ObservedGeneration,
		PortableDigest: provenance.PortableDigest, RenderDigest: provenance.ResolvedDigest,
	}, true
}

func projectShadowedPolicy(
	shadow *omev1beta1.ShadowedAutoscalerPolicy,
	expectedName string,
) (*reportv1alpha1.AutoscalePolicyStatus, bool) {
	if shadow == nil || shadow.Name != expectedName || len(utilvalidation.IsDNS1123Subdomain(shadow.Name)) != 0 {
		return nil, false
	}
	if (shadow.PortableDigest == "") != (shadow.WouldRenderDigest == "") {
		return nil, false
	}
	if shadow.PortableDigest != "" &&
		(!portablePolicyDigestPattern.MatchString(shadow.PortableDigest) ||
			!renderPolicyDigestPattern.MatchString(shadow.WouldRenderDigest)) {
		return nil, false
	}
	return &reportv1alpha1.AutoscalePolicyStatus{
		State: reportv1alpha1.AutoscalePolicyShadowed, Resolution: reportv1alpha1.AutoscalePolicyInlinePrecedence,
		Name: shadow.Name, PortableDigest: shadow.PortableDigest, RenderDigest: shadow.WouldRenderDigest,
	}, true
}

func declaredPolicyIntent(
	extension *omev1beta1.ComponentExtensionSpec,
) (*omev1beta1.AutoscalerPolicyRef, bool, bool) {
	if extension == nil {
		return nil, false, true
	}
	inline := extension.Autoscaler != nil
	reference := extension.AutoscalerPolicyRef
	if reference == nil {
		return nil, inline, true
	}
	if reference.Name == "" || len(utilvalidation.IsDNS1123Subdomain(reference.Name)) != 0 ||
		(reference.Kind != "" && reference.Kind != "AutoscalerPolicy") {
		return nil, inline, false
	}
	return reference, inline, true
}

func policyResolution(reason string) reportv1alpha1.AutoscalePolicyResolution {
	switch reason {
	case omev1beta1.AutoscalerResolvedReasonPolicyNotFound:
		return reportv1alpha1.AutoscalePolicyNotFound
	case omev1beta1.AutoscalerResolvedReasonPolicyInvalid:
		return reportv1alpha1.AutoscalePolicyInvalid
	case omev1beta1.AutoscalerResolvedReasonAuthNotFound:
		return reportv1alpha1.AutoscalePolicyAuthNotFound
	case omev1beta1.AutoscalerResolvedReasonClassUnavailable:
		return reportv1alpha1.AutoscalePolicyClassUnavailable
	default:
		return ""
	}
}

func nonPolicySpecSource(source string) bool {
	switch source {
	case "isvc", "runtime", "legacy", "default":
		return true
	default:
		return false
	}
}
