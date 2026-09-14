package rolloutprojection

import (
	"errors"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"knative.dev/pkg/apis"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/ome/pkg/validation"
)

var (
	autoscalerPortableDigestPattern = regexp.MustCompile(`^pv1:[0-9a-f]{12}$`)
	autoscalerResolvedDigestPattern = regexp.MustCompile(`^rv1:[0-9a-f]{12}$`)
	shortHashPattern                = regexp.MustCompile(`^[0-9a-f]{12}$`)
)

// ProjectValidation validates the stored rollout-related configuration and
// projects only first-party evidence already present on one InferenceService.
// It deliberately performs no API reads and treats cluster-dependent facts as
// unverifiable unless generation-current controller status proves them.
func ProjectValidation(
	isvc *omev1beta1.InferenceService,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.RolloutValidationReport, error) {
	if isvc == nil {
		return reportv1alpha1.RolloutValidationReport{}, ErrNilInferenceService
	}
	if isvc.Name == "" {
		return reportv1alpha1.RolloutValidationReport{}, ErrSubjectNameRequired
	}
	if isvc.Namespace == "" {
		return reportv1alpha1.RolloutValidationReport{}, ErrNamespaceRequired
	}
	if isvc.UID == "" {
		return reportv1alpha1.RolloutValidationReport{}, ErrSubjectUIDRequired
	}
	b := validationProjector{isvc: isvc}
	b.projectRollout()
	b.projectTraffic()
	b.projectAutoscaling()
	b.content.Summary.State = validationState(b.content.Checks)

	reportValue := reportv1alpha1.NewRolloutValidationReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name},
		b.content,
		clock,
	)
	reportValue.Sources = []reportv1alpha1.RolloutSourceReference{{
		Kind:       reportv1alpha1.RolloutSourceInferenceService,
		Namespace:  isvc.Namespace,
		Name:       isvc.Name,
		UID:        string(isvc.UID),
		Generation: isvc.Generation,
		Evidence:   reportv1alpha1.EvidenceObserved,
	}}
	if reportValue.Content.Summary.State == reportv1alpha1.RolloutValidationUnverifiable {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.RolloutWarning{Code: reportv1alpha1.WarningPartialData})
	}
	for _, check := range reportValue.Content.Checks {
		if check.Freshness == reportv1alpha1.RolloutValidationFreshnessStale {
			reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.RolloutWarning{Code: reportv1alpha1.WarningStaleEvidence})
			break
		}
	}
	return reportValue.Canonical(), nil
}

type validationProjector struct {
	isvc    *omev1beta1.InferenceService
	content reportv1alpha1.RolloutValidationContent
}

func (b *validationProjector) projectRollout() {
	b.addStaticCheck(
		reportv1alpha1.RolloutValidationCheckRolloutReferences,
		validateStoredRolloutReferences(&b.isvc.Spec),
		reportv1alpha1.RolloutValidationIssueRolloutReferenceInvalid,
		"",
	)
	var planErr error
	if !validStoredRolloutPlan(&b.isvc.Spec) {
		planErr = errInvalidRolloutPlan
	}
	b.addStaticCheck(
		reportv1alpha1.RolloutValidationCheckRolloutPlan,
		planErr,
		reportv1alpha1.RolloutValidationIssueRolloutPlanInvalid,
		"",
	)
	b.addStaticCheck(
		reportv1alpha1.RolloutValidationCheckRolloutOrdering,
		validation.ValidateRolloutOrderingEnforced(&b.isvc.Spec),
		reportv1alpha1.RolloutValidationIssueRolloutOrderingInvalid,
		"",
	)
	b.projectRolloutResolution()
}

func (b *validationProjector) projectRolloutResolution() {
	name := reportv1alpha1.RolloutValidationCheckRolloutResolution
	active := b.isvc.Status.Rollout != nil && b.isvc.Status.Rollout.ActiveRun != nil
	if (b.isvc.Spec.Rollout == nil || len(b.isvc.Spec.Rollout.Groups) == 0) && !active {
		b.addCheck(name, "", reportv1alpha1.RolloutValidationResultNotApplicable,
			reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessNotApplicable)
		return
	}
	conditions := findKnativeConditions(b.isvc.Status.Conditions, omev1beta1.RolloutPlanReadyCondition)
	if len(conditions) == 0 {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceUnavailable,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueRolloutResolutionMissing)
		return
	}
	if len(conditions) != 1 {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
		return
	}
	condition := conditions[0]
	if !validRolloutPlanCondition(condition, b.isvc.Status.Rollout) {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
		return
	}
	if condition.Status != corev1.ConditionTrue {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueRolloutResolutionFailed)
		return
	}
	if b.isvc.Status.Rollout != nil && b.isvc.Status.Rollout.ActiveRun != nil &&
		!validPinnedRolloutPlan(b.isvc) {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueRolloutResolutionMalformed)
		return
	}
	if hasUnverifiableRolloutPrerequisites(b.isvc) {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceUnavailable,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueRolloutPrerequisiteUnverifiable)
		return
	}
	// RolloutPlanReady is a Knative condition and has no observed-generation
	// field. InferenceService.status.observedGeneration cannot substitute for
	// one: RawDeployment and MultiNode copy a child workload generation into
	// it, while OMENative leaves it unset. The stored condition can therefore
	// be checked for shape, but its freshness cannot be bound to this ISVC
	// generation without overclaiming.
	b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
		reportv1alpha1.RolloutValidationFreshnessUnverifiable,
		reportv1alpha1.RolloutValidationIssueRolloutResolutionFreshnessUnverifiable)
}

func (b *validationProjector) projectTraffic() {
	trafficErr := validation.ValidateTrafficSpec(b.isvc.Spec.Traffic)
	if trafficErr == nil && !validStoredTrafficSchema(b.isvc.Spec.Traffic) {
		trafficErr = errInvalidTrafficSpec
	}
	_, annotationErr := validation.ValidateTrafficAnnotations(b.isvc.Annotations, b.isvc.Spec.Traffic)
	result := reportv1alpha1.RolloutValidationResultValid
	if trafficErr != nil || annotationErr != nil {
		result = reportv1alpha1.RolloutValidationResultInvalid
	}
	b.addCheck(reportv1alpha1.RolloutValidationCheckTrafficSpec, "", result,
		reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessCurrent)
	if trafficErr != nil {
		b.addIssue(reportv1alpha1.RolloutValidationIssueTrafficSpecInvalid,
			reportv1alpha1.RolloutValidationCheckTrafficSpec, "")
	}
	if annotationErr != nil {
		b.addIssue(reportv1alpha1.RolloutValidationIssueTrafficAnnotationInvalid,
			reportv1alpha1.RolloutValidationCheckTrafficSpec, "")
	}
	b.projectTrafficReadiness()
}

func (b *validationProjector) projectTrafficReadiness() {
	name := reportv1alpha1.RolloutValidationCheckTrafficReadiness
	if !hasTrafficIntent(b.isvc) {
		b.addCheck(name, "", reportv1alpha1.RolloutValidationResultNotApplicable,
			reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessNotApplicable)
		return
	}
	if b.isvc.Status.Traffic == nil {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceUnavailable,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueTrafficEvidenceMissing)
		return
	}
	ready := findMetaConditions(b.isvc.Status.Traffic.Conditions, omev1beta1.TrafficConditionBackendPolicyReady)
	unsupported := findMetaConditions(b.isvc.Status.Traffic.Conditions, omev1beta1.TrafficConditionBackendPolicyUnsupportedFields)
	if len(ready) != 1 || len(unsupported) > 1 {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueTrafficEvidenceMalformed)
		return
	}
	if impossibleObservedGeneration(ready[0].ObservedGeneration, b.isvc.Generation) ||
		(len(unsupported) == 1 && impossibleObservedGeneration(unsupported[0].ObservedGeneration, b.isvc.Generation)) {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueTrafficEvidenceMalformed)
		return
	}
	if ready[0].ObservedGeneration != b.isvc.Generation ||
		(len(unsupported) == 1 && unsupported[0].ObservedGeneration != b.isvc.Generation) {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessStale,
			reportv1alpha1.RolloutValidationIssueTrafficEvidenceStale)
		return
	}
	if !validTrafficReadyCondition(ready[0]) ||
		(len(unsupported) == 1 && !validUnsupportedTrafficCondition(unsupported[0])) ||
		!reportedTrafficAlgorithmMatches(b.isvc.Spec.Traffic, b.isvc.Status.Traffic.Algorithm) {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessCurrent,
			reportv1alpha1.RolloutValidationIssueTrafficEvidenceMalformed)
		return
	}
	if ready[0].Status != metav1.ConditionTrue || len(unsupported) == 1 {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessCurrent,
			reportv1alpha1.RolloutValidationIssueTrafficNotReady)
		return
	}
	if !validBackendPolicyReference(b.isvc.Status.Traffic.BackendPolicyResource, b.isvc.Name) {
		b.addUnverifiable(name, "", reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessCurrent,
			reportv1alpha1.RolloutValidationIssueTrafficEvidenceMalformed)
		return
	}
	b.addCheck(name, "", reportv1alpha1.RolloutValidationResultValid,
		reportv1alpha1.EvidenceReported, reportv1alpha1.RolloutValidationFreshnessCurrent)
}

func (b *validationProjector) projectAutoscaling() {
	b.addStaticCheck(
		reportv1alpha1.RolloutValidationCheckScalingPolicy,
		validation.ValidateScalingPolicy(b.isvc.Spec.ScalingPolicy, "spec.scalingPolicy"),
		reportv1alpha1.RolloutValidationIssueScalingPolicyInvalid,
		"",
	)
	globalErr := firstError(
		func() error { _, err := validation.ValidateAutoscalerConfig(b.isvc); return err }(),
		validation.ValidateAutoscalerAnnotationConflict(b.isvc),
		validation.ValidateAutoscalerTargetUtilizationPercentage(b.isvc),
		validation.ValidateScaleToZero(b.isvc),
	)
	b.addStaticCheck(
		reportv1alpha1.RolloutValidationCheckAutoscalerSpec,
		globalErr,
		reportv1alpha1.RolloutValidationIssueAutoscalerSpecInvalid,
		"",
	)
	for _, component := range declaredValidationComponents(b.isvc) {
		b.projectComponentAutoscaler(component)
	}
}

type validationComponent struct {
	typeName  reportv1alpha1.RuntimeComponentType
	extension *omev1beta1.ComponentExtensionSpec
}

func (b *validationProjector) projectComponentAutoscaler(component validationComponent) {
	var maxReplicas *int
	if component.extension.MaxReplicas != 0 {
		maxReplicas = &component.extension.MaxReplicas
	}
	shapeErr := firstError(
		validation.ValidateComponentAutoscaler(component.extension),
		validation.ValidateReplicaBounds(component.extension.MinReplicas, maxReplicas),
	)
	referenceErr := validateAutoscalerReference(component.extension.AutoscalerPolicyRef)
	result := reportv1alpha1.RolloutValidationResultValid
	if shapeErr != nil || referenceErr != nil {
		result = reportv1alpha1.RolloutValidationResultInvalid
	}
	b.addCheck(reportv1alpha1.RolloutValidationCheckAutoscalerSpec, component.typeName, result,
		reportv1alpha1.EvidenceComputed, reportv1alpha1.RolloutValidationFreshnessCurrent)
	if shapeErr != nil {
		b.addIssue(reportv1alpha1.RolloutValidationIssueAutoscalerSpecInvalid,
			reportv1alpha1.RolloutValidationCheckAutoscalerSpec, component.typeName)
	}
	if referenceErr != nil {
		b.addIssue(reportv1alpha1.RolloutValidationIssueAutoscalerReferenceInvalid,
			reportv1alpha1.RolloutValidationCheckAutoscalerSpec, component.typeName)
	}
	if component.extension.AutoscalerPolicyRef != nil {
		b.projectAutoscalerResolution(component)
	}
}

func (b *validationProjector) projectAutoscalerResolution(component validationComponent) {
	name := reportv1alpha1.RolloutValidationCheckAutoscalerResolution
	status, found := b.isvc.Status.Components[omev1beta1.ComponentType(component.typeName)]
	if !found || status.Autoscaler == nil {
		b.addUnverifiable(name, component.typeName, reportv1alpha1.EvidenceUnavailable,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMissing)
		return
	}
	conditions := findMetaConditions(status.Autoscaler.Conditions, omev1beta1.AutoscalerResolvedCondition)
	if len(conditions) == 0 {
		b.addUnverifiable(name, component.typeName, reportv1alpha1.EvidenceUnavailable,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMissing)
		return
	}
	if len(conditions) != 1 {
		b.addUnverifiable(name, component.typeName, reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
		return
	}
	condition := conditions[0]
	if impossibleObservedGeneration(condition.ObservedGeneration, b.isvc.Generation) {
		b.addUnverifiable(name, component.typeName, reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessUnverifiable,
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
		return
	}
	if condition.ObservedGeneration != b.isvc.Generation {
		b.addUnverifiable(name, component.typeName, reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessStale,
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceStale)
		return
	}
	if !validAutoscalerResolutionCondition(
		condition,
		status.Autoscaler,
		component.extension.AutoscalerPolicyRef,
		component.extension.Autoscaler != nil,
	) {
		b.addUnverifiable(name, component.typeName, reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessCurrent,
			reportv1alpha1.RolloutValidationIssueAutoscalerEvidenceMalformed)
		return
	}
	if condition.Status != metav1.ConditionTrue {
		b.addUnverifiable(name, component.typeName, reportv1alpha1.EvidenceReported,
			reportv1alpha1.RolloutValidationFreshnessCurrent,
			reportv1alpha1.RolloutValidationIssueAutoscalerResolutionFailed)
		return
	}
	b.addCheck(name, component.typeName, reportv1alpha1.RolloutValidationResultValid,
		reportv1alpha1.EvidenceReported, reportv1alpha1.RolloutValidationFreshnessCurrent)
}

func (b *validationProjector) addStaticCheck(
	name reportv1alpha1.RolloutValidationCheckName,
	err error,
	issue reportv1alpha1.RolloutValidationIssueCode,
	component reportv1alpha1.RuntimeComponentType,
) {
	result := reportv1alpha1.RolloutValidationResultValid
	if err != nil {
		result = reportv1alpha1.RolloutValidationResultInvalid
	}
	b.addCheck(name, component, result, reportv1alpha1.EvidenceComputed,
		reportv1alpha1.RolloutValidationFreshnessCurrent)
	if err != nil {
		b.addIssue(issue, name, component)
	}
}

func (b *validationProjector) addUnverifiable(
	name reportv1alpha1.RolloutValidationCheckName,
	component reportv1alpha1.RuntimeComponentType,
	evidence reportv1alpha1.EvidenceLevel,
	freshness reportv1alpha1.RolloutValidationFreshness,
	issue reportv1alpha1.RolloutValidationIssueCode,
) {
	b.addCheck(name, component, reportv1alpha1.RolloutValidationResultUnverifiable, evidence, freshness)
	b.addIssue(issue, name, component)
}

func (b *validationProjector) addCheck(
	name reportv1alpha1.RolloutValidationCheckName,
	component reportv1alpha1.RuntimeComponentType,
	result reportv1alpha1.RolloutValidationResult,
	evidence reportv1alpha1.EvidenceLevel,
	freshness reportv1alpha1.RolloutValidationFreshness,
) {
	b.content.Checks = append(b.content.Checks, reportv1alpha1.RolloutValidationCheck{
		Check: name, Component: component, Result: result, Evidence: evidence, Freshness: freshness,
	})
}

func (b *validationProjector) addIssue(
	code reportv1alpha1.RolloutValidationIssueCode,
	check reportv1alpha1.RolloutValidationCheckName,
	component reportv1alpha1.RuntimeComponentType,
) {
	b.content.Issues = append(b.content.Issues, reportv1alpha1.RolloutValidationIssue{
		Code: code, Check: check, Component: component,
	})
}

func validationState(checks []reportv1alpha1.RolloutValidationCheck) reportv1alpha1.RolloutValidationState {
	unverifiable := false
	for _, check := range checks {
		switch check.Result {
		case reportv1alpha1.RolloutValidationResultInvalid:
			return reportv1alpha1.RolloutValidationInvalid
		case reportv1alpha1.RolloutValidationResultUnverifiable:
			unverifiable = true
		}
	}
	if unverifiable {
		return reportv1alpha1.RolloutValidationUnverifiable
	}
	return reportv1alpha1.RolloutValidationValid
}

func findKnativeConditions(conditions []apis.Condition, conditionType string) []*apis.Condition {
	result := make([]*apis.Condition, 0, 1)
	for i := range conditions {
		if conditions[i].Type == apis.ConditionType(conditionType) {
			result = append(result, &conditions[i])
		}
	}
	return result
}

func findMetaConditions(conditions []metav1.Condition, conditionType string) []*metav1.Condition {
	result := make([]*metav1.Condition, 0, 1)
	for i := range conditions {
		if conditions[i].Type == conditionType {
			result = append(result, &conditions[i])
		}
	}
	return result
}

func validRolloutPlanCondition(condition *apis.Condition, status *omev1beta1.RolloutStatus) bool {
	active := status != nil && status.ActiveRun != nil
	switch condition.Reason {
	case omev1beta1.RolloutPlanReasonPinned:
		return condition.Status == corev1.ConditionTrue && active
	case omev1beta1.RolloutPlanReasonNoRun:
		return condition.Status == corev1.ConditionTrue && !active
	case omev1beta1.RolloutPlanReasonPolicyNotFound,
		omev1beta1.RolloutPlanReasonPolicyNotReady,
		omev1beta1.RolloutPlanReasonProgressionMismatch,
		omev1beta1.RolloutPlanReasonPlanInvalid,
		omev1beta1.RolloutPlanReasonProviderUnbound:
		return condition.Status == corev1.ConditionFalse
	default:
		return false
	}
}

func validPinnedRolloutPlan(isvc *omev1beta1.InferenceService) bool {
	active := isvc.Status.Rollout.ActiveRun
	if !validActiveRolloutRun(isvc.Name, active) {
		return false
	}
	copy := isvc.DeepCopy()
	groups := active.Plan.Groups
	copy.Spec.Rollout = active.Plan.AsRolloutSpec(copy.Spec.Rollout)
	if !validStoredRolloutPlan(&copy.Spec) ||
		validation.ValidateRolloutPolicyRefs(&copy.Spec, true) != nil ||
		validation.ValidateRolloutOrderingEnforced(&copy.Spec) != nil {
		return false
	}
	if len(invalidPinnedPlanGroups(groups)) != 0 {
		return false
	}
	for i := range groups {
		group := &groups[i]
		digest, err := rolloutpolicy.ProgressionDigest(&group.Group)
		strategy, strategyValid := pinnedGroupStrategy(&group.Group)
		if !strategyValid || group.Group.PolicyRef != nil ||
			err != nil || digest == "" || digest != group.PortableDigest {
			return false
		}
		switch group.Source {
		case omev1beta1.RolloutPlanSourceInline:
			if group.PolicyRef != nil || group.PolicyGeneration != 0 {
				return false
			}
		case omev1beta1.RolloutPlanSourcePolicy:
			if projectPinnedPolicyRef(group.PolicyRef, strategy) == nil ||
				group.PolicyGeneration < 0 || !validPinnedPolicyBody(&group.Group) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func validActiveRolloutRun(subjectName string, run *omev1beta1.RolloutRun) bool {
	prefix := subjectName + "-"
	if run == nil || !strings.HasPrefix(run.RunID, prefix) ||
		!shortHashPattern.MatchString(strings.TrimPrefix(run.RunID, prefix)) ||
		run.OpenedAt.IsZero() || run.PinnedAt.IsZero() ||
		run.PinnedAt.Time.Before(run.OpenedAt.Time) || len(run.Plan.Groups) == 0 {
		return false
	}

	expectedComponents := make([]omev1beta1.ComponentType, 0, 3)
	seen := make(map[omev1beta1.ComponentType]bool, 3)
	for i := range run.Plan.Groups {
		for _, component := range run.Plan.Groups[i].Group.Components {
			if seen[component] {
				continue
			}
			seen[component] = true
			expectedComponents = append(expectedComponents, component)
		}
	}
	if len(run.TargetRevisions) != len(expectedComponents) {
		return false
	}
	for i := range run.TargetRevisions {
		target := run.TargetRevisions[i]
		if target.Component != expectedComponents[i] || !safeRevisionHash(target.Revision) {
			return false
		}
	}
	return true
}

func validTrafficReadyCondition(condition *metav1.Condition) bool {
	switch condition.Reason {
	case omev1beta1.TrafficReasonAcceptedByGateway:
		return condition.Status == metav1.ConditionTrue
	case omev1beta1.TrafficReasonConflictingPolicy,
		omev1beta1.TrafficReasonUnsupportedField,
		omev1beta1.TrafficReasonNoTranslatorAvailable,
		omev1beta1.TrafficReasonGatewayRejected,
		omev1beta1.TrafficReasonTranslationFailed:
		return condition.Status == metav1.ConditionFalse
	case omev1beta1.TrafficReasonPending:
		return condition.Status == metav1.ConditionUnknown
	default:
		return false
	}
}

func validUnsupportedTrafficCondition(condition *metav1.Condition) bool {
	return condition.Status == metav1.ConditionTrue && condition.Reason == omev1beta1.TrafficReasonUnsupportedField
}

func validBackendPolicyReference(reference *omev1beta1.BackendPolicyRef, subjectName string) bool {
	if reference == nil || reference.Name != subjectName ||
		len(utilvalidation.IsDNS1123Subdomain(reference.Name)) > 0 {
		return false
	}
	return (reference.APIVersion == "gateway.envoyproxy.io/v1alpha1" && reference.Kind == "BackendTrafficPolicy") ||
		(reference.APIVersion == "networking.istio.io/v1" && reference.Kind == "DestinationRule")
}

func validStoredTrafficSchema(traffic *omev1beta1.TrafficSpec) bool {
	if traffic == nil || traffic.Algorithm == nil {
		return true
	}
	switch *traffic.Algorithm {
	case omev1beta1.LoadBalancingTypeRoundRobin,
		omev1beta1.LoadBalancingTypeLeastRequest,
		omev1beta1.LoadBalancingTypeRandom,
		omev1beta1.LoadBalancingTypeConsistentHash:
		return true
	default:
		return false
	}
}

func reportedTrafficAlgorithmMatches(traffic *omev1beta1.TrafficSpec, reported string) bool {
	if traffic == nil || traffic.Algorithm == nil {
		return reported == "Default"
	}
	return reported == string(*traffic.Algorithm)
}

func validAutoscalerResolutionCondition(
	condition *metav1.Condition,
	status *omev1beta1.ComponentAutoscalerStatus,
	reference *omev1beta1.AutoscalerPolicyRef,
	inline bool,
) bool {
	switch condition.Reason {
	case omev1beta1.AutoscalerResolvedReasonInlinePrecedence:
		return condition.Status == metav1.ConditionTrue && inline &&
			status.SpecSource == "isvc" && status.Policy == nil && reference != nil &&
			validShadowedAutoscalerPolicy(status.ShadowedPolicyRef, reference.Name)
	case omev1beta1.AutoscalerResolvedReasonRenderedFromPolicy:
		return condition.Status == metav1.ConditionTrue && !inline &&
			status.SpecSource == "policy" && status.ShadowedPolicyRef == nil && reference != nil &&
			validAutoscalerPolicyProvenance(status.Policy, reference.Name)
	case omev1beta1.AutoscalerResolvedReasonPolicyNotFound,
		omev1beta1.AutoscalerResolvedReasonPolicyInvalid,
		omev1beta1.AutoscalerResolvedReasonAuthNotFound,
		omev1beta1.AutoscalerResolvedReasonClassUnavailable:
		return condition.Status == metav1.ConditionFalse && status.SpecSource == "policy" &&
			status.ShadowedPolicyRef == nil &&
			(status.Policy == nil || validAutoscalerPolicyProvenance(status.Policy, ""))
	case omev1beta1.AutoscalerResolvedReasonUnsupportedMode:
		return condition.Status == metav1.ConditionFalse &&
			validNonPolicyAutoscalerSource(status.SpecSource) &&
			status.Policy == nil && status.ShadowedPolicyRef == nil
	default:
		return false
	}
}

func validAutoscalerPolicyProvenance(
	provenance *omev1beta1.AutoscalerPolicyProvenance,
	expectedName string,
) bool {
	if provenance == nil || provenance.Name == "" ||
		len(utilvalidation.IsDNS1123Subdomain(provenance.Name)) > 0 ||
		(expectedName != "" && provenance.Name != expectedName) ||
		provenance.ObservedGeneration <= 0 {
		return false
	}
	return autoscalerPortableDigestPattern.MatchString(provenance.PortableDigest) &&
		autoscalerResolvedDigestPattern.MatchString(provenance.ResolvedDigest)
}

func validShadowedAutoscalerPolicy(
	shadow *omev1beta1.ShadowedAutoscalerPolicy,
	expectedName string,
) bool {
	if shadow == nil || shadow.Name != expectedName ||
		len(utilvalidation.IsDNS1123Subdomain(shadow.Name)) > 0 {
		return false
	}
	if shadow.PortableDigest == "" && shadow.WouldRenderDigest == "" {
		return true
	}
	return autoscalerPortableDigestPattern.MatchString(shadow.PortableDigest) &&
		autoscalerResolvedDigestPattern.MatchString(shadow.WouldRenderDigest)
}

func validNonPolicyAutoscalerSource(source string) bool {
	switch source {
	case "isvc", "runtime", "legacy", "default":
		return true
	default:
		return false
	}
}

func validateAutoscalerReference(reference *omev1beta1.AutoscalerPolicyRef) error {
	if reference == nil {
		return nil
	}
	if reference.Name == "" || len(utilvalidation.IsDNS1123Subdomain(reference.Name)) != 0 {
		return errInvalidAutoscalerReference
	}
	if reference.Kind != "" && reference.Kind != constants.AutoscalerPolicyKind {
		return errInvalidAutoscalerReference
	}
	return nil
}

var (
	errInvalidRolloutPlan         = errors.New("rollout plan is invalid")
	errInvalidRolloutReference    = errors.New("rollout reference is invalid")
	errInvalidTrafficSpec         = errors.New("traffic spec is invalid")
	errInvalidAutoscalerReference = errors.New("autoscaler reference is invalid")
)

func validateStoredRolloutReferences(spec *omev1beta1.InferenceServiceSpec) error {
	if err := validation.ValidateRolloutPolicyRefs(spec, true); err != nil {
		return err
	}
	groups := spec.GetRolloutGroups()
	for i := range groups {
		reference := groups[i].PolicyRef
		if reference != nil && len(utilvalidation.IsDNS1123Subdomain(reference.Name)) != 0 {
			return errInvalidRolloutReference
		}
	}
	return nil
}

func declaredValidationComponents(isvc *omev1beta1.InferenceService) []validationComponent {
	result := make([]validationComponent, 0, 3)
	if isvc.Spec.Engine != nil {
		result = append(result, validationComponent{
			typeName:  reportv1alpha1.RuntimeComponentEngine,
			extension: &isvc.Spec.Engine.ComponentExtensionSpec,
		})
	}
	if isvc.Spec.Decoder != nil {
		result = append(result, validationComponent{
			typeName:  reportv1alpha1.RuntimeComponentDecoder,
			extension: &isvc.Spec.Decoder.ComponentExtensionSpec,
		})
	}
	if isvc.Spec.Router != nil {
		result = append(result, validationComponent{
			typeName:  reportv1alpha1.RuntimeComponentRouter,
			extension: &isvc.Spec.Router.ComponentExtensionSpec,
		})
	}
	return result
}

func hasTrafficIntent(isvc *omev1beta1.InferenceService) bool {
	traffic := isvc.Spec.Traffic
	if traffic != nil && (traffic.Algorithm != nil || traffic.ConsistentHash != nil || traffic.EndpointOverride != nil) {
		return true
	}
	knownKeys := []string{
		constants.CircuitBreakerMaxConnectionsAnnotation,
		constants.CircuitBreakerMaxParallelRequestsAnnotation,
		constants.CircuitBreakerMaxPendingRequestsAnnotation,
		constants.CircuitBreakerMaxParallelRetriesAnnotation,
		constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation,
		constants.RetryAttemptsAnnotation,
		constants.RetryOnAnnotation,
		constants.RetryPerTryTimeoutAnnotation,
		constants.TimeoutIdleAnnotation,
		constants.TimeoutMaxConnectionDurationAnnotation,
		constants.TimeoutTCPConnectAnnotation,
	}
	for _, key := range knownKeys {
		if _, found := isvc.Annotations[key]; found {
			return true
		}
	}
	for key := range isvc.Annotations {
		if strings.HasPrefix(key, constants.PassthroughEnvoyGatewayPrefix) ||
			strings.HasPrefix(key, constants.PassthroughIstioPrefix) {
			return true
		}
	}
	return false
}

func firstError(errors ...error) error {
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func impossibleObservedGeneration(observed, generation int64) bool {
	return observed < 0 || observed > generation
}

func hasUnverifiableRolloutPrerequisites(isvc *omev1beta1.InferenceService) bool {
	groups := isvc.Spec.GetRolloutGroups()
	var active *omev1beta1.RolloutRun
	if isvc.Status.Rollout != nil {
		active = isvc.Status.Rollout.ActiveRun
	}
	for i := range groups {
		group := &groups[i]
		if group.PolicyRef != nil && !hasInlineProgression(group) &&
			!activeRunBindsPolicyGroup(active, i, group) {
			return true
		}
		if canaryUsesAnalysis(group.Canary) {
			return true
		}
	}
	if active == nil {
		return false
	}
	for i := range active.Plan.Groups {
		if canaryUsesAnalysis(active.Plan.Groups[i].Group.Canary) {
			return true
		}
	}
	return false
}

func activeRunBindsPolicyGroup(
	active *omev1beta1.RolloutRun,
	index int,
	live *omev1beta1.RolloutGroup,
) bool {
	if active == nil || live == nil || live.PolicyRef == nil ||
		index < 0 || index >= len(active.Plan.Groups) {
		return false
	}
	pinned := &active.Plan.Groups[index]
	if pinned.Source != omev1beta1.RolloutPlanSourcePolicy || pinned.PolicyRef == nil ||
		!sameRolloutPolicyReference(live.PolicyRef, pinned.PolicyRef) ||
		len(live.Components) != len(pinned.Group.Components) {
		return false
	}
	for i := range live.Components {
		if live.Components[i] != pinned.Group.Components[i] {
			return false
		}
	}
	return true
}

func sameRolloutPolicyReference(left, right *omev1beta1.RolloutPolicyRef) bool {
	if left == nil || right == nil || left.Name != right.Name ||
		left.Progression != right.Progression {
		return false
	}
	leftKind := left.Kind
	if leftKind == "" {
		leftKind = constants.RolloutPolicyKind
	}
	rightKind := right.Kind
	if rightKind == "" {
		rightKind = constants.RolloutPolicyKind
	}
	return leftKind == rightKind
}

func canaryUsesAnalysis(canary *omev1beta1.GroupCanary) bool {
	if canary == nil {
		return false
	}
	for i := range canary.Steps {
		if canary.Steps[i].Analysis != nil {
			return true
		}
	}
	return false
}
