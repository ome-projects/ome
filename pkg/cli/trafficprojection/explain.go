package trafficprojection

import (
	"strings"
	"time"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	omerollout "sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/validation"
)

var trafficExtensionKeys = map[reportv1alpha1.TrafficExtensionKind][]string{
	reportv1alpha1.TrafficExtensionCircuitBreaker: {
		constants.CircuitBreakerMaxConnectionsAnnotation,
		constants.CircuitBreakerMaxParallelRequestsAnnotation,
		constants.CircuitBreakerMaxPendingRequestsAnnotation,
		constants.CircuitBreakerMaxParallelRetriesAnnotation,
		constants.CircuitBreakerPerEndpointMaxConnectionsAnnotation,
	},
	reportv1alpha1.TrafficExtensionRetry: {
		constants.RetryAttemptsAnnotation,
		constants.RetryOnAnnotation,
		constants.RetryPerTryTimeoutAnnotation,
	},
	reportv1alpha1.TrafficExtensionTimeout: {
		constants.TimeoutIdleAnnotation,
		constants.TimeoutMaxConnectionDurationAnnotation,
		constants.TimeoutTCPConnectAnnotation,
	},
}

// ProjectExplain combines declared traffic intent with the bounded status
// projection. It performs no API reads, never parses human status messages,
// and does not inspect emitted policy or data-plane objects.
func ProjectExplain(
	isvc *omev1beta1.InferenceService,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.TrafficExplainReport, error) {
	statusReport, err := Project(isvc, clock)
	if err != nil {
		return reportv1alpha1.TrafficExplainReport{}, err
	}

	intent, specInvalid, annotationsInvalid := projectDeclaredTraffic(isvc)
	support := projectTrafficSupport(intent.State, statusReport.Content)
	realization := projectTrafficRealization(statusReport.Content)
	comparisons := projectTrafficComparisons(isvc, intent, support, statusReport.Content)
	// Canary traffic is intent even when no backend-policy feature is declared.
	// Keep policy support/comparisons scoped to the original policy intent.
	if intent.State == reportv1alpha1.TrafficIntentAbsent && hasDeclaredCanary(isvc) {
		intent.State = reportv1alpha1.TrafficIntentDeclared
	}
	issues := projectTrafficExplainIssues(
		intent.State, specInvalid, annotationsInvalid, support,
		realization, statusReport.Content, comparisons,
	)
	state := projectTrafficExplainState(
		intent.State, support, realization, statusReport.Content, comparisons,
	)

	content := reportv1alpha1.TrafficExplainContent{
		Summary: reportv1alpha1.TrafficExplainSummary{
			State: state, Intent: intent.State, Support: support,
			Realization: realization,
		},
		Intent: intent, Reported: statusReport.Content,
		Comparisons: comparisons, Issues: issues,
	}
	content = content.Canonical()
	content.Summary.Source = trafficExplainSummarySource(content)
	// Project sampled the caller's clock. Reuse that timestamp so one report
	// represents one collection instant even when a stateful test clock is used.
	reportValue := reportv1alpha1.NewTrafficExplainReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name},
		content,
		reportv1alpha1.ClockFunc(func() time.Time { return statusReport.CollectedAt }),
	)
	reportValue.Sources = []reportv1alpha1.TrafficExplainSourceReference{{
		Kind:      reportv1alpha1.TrafficSourceInferenceService,
		Namespace: isvc.Namespace, Name: isvc.Name,
		Generation: isvc.Generation, Evidence: reportv1alpha1.EvidenceReported,
	}}
	reportValue.Warnings = append(reportValue.Warnings, statusReport.Warnings...)
	if intent.State != reportv1alpha1.TrafficIntentAbsent &&
		support == reportv1alpha1.TrafficSupportUnavailable {
		reportValue.Warnings = append(reportValue.Warnings,
			reportv1alpha1.TrafficWarning{Code: reportv1alpha1.WarningSourceUnavailable})
	}
	if state == reportv1alpha1.TrafficExplainPartial ||
		state == reportv1alpha1.TrafficExplainUnsupported {
		reportValue.Warnings = append(reportValue.Warnings,
			reportv1alpha1.TrafficWarning{Code: reportv1alpha1.WarningPartialData})
	}
	return reportValue.Canonical(), nil
}

func projectDeclaredTraffic(
	isvc *omev1beta1.InferenceService,
) (reportv1alpha1.TrafficDeclaredIntent, bool, bool) {
	filteredAnnotations, passthroughInvalid := declaredTrafficAnnotations(isvc.Annotations)
	specInvalid := validation.ValidateTrafficSpec(isvc.Spec.Traffic) != nil ||
		!declaredAlgorithmValid(isvc.Spec.Traffic)
	_, annotationsErr := validation.ValidateTrafficAnnotations(
		filteredAnnotations, isvc.Spec.Traffic,
	)
	annotationsInvalid := annotationsErr != nil || passthroughInvalid
	hasTypedIntent := isvc.Spec.Traffic != nil &&
		(isvc.Spec.Traffic.Algorithm != nil ||
			isvc.Spec.Traffic.ConsistentHash != nil ||
			isvc.Spec.Traffic.EndpointOverride != nil)
	hasIntent := hasTypedIntent || len(filteredAnnotations) > 0

	state := reportv1alpha1.TrafficIntentAbsent
	if hasIntent {
		state = reportv1alpha1.TrafficIntentDeclared
	}
	if specInvalid || annotationsInvalid {
		state = reportv1alpha1.TrafficIntentInvalid
	}
	intent := reportv1alpha1.TrafficDeclaredIntent{
		State: state, Algorithm: projectDeclaredAlgorithm(isvc.Spec.Traffic),
		ConsistentHash:   projectDeclaredHash(isvc.Spec.Traffic),
		EndpointOverride: projectDeclaredEndpointOverride(isvc.Spec.Traffic),
		Extensions:       projectDeclaredExtensions(filteredAnnotations),
		Source: reportv1alpha1.TrafficValueSource{
			Evidence:  reportv1alpha1.EvidenceDeclared,
			Freshness: reportv1alpha1.TrafficFreshnessCurrent,
		},
	}
	return intent, specInvalid, annotationsInvalid
}

func projectDeclaredAlgorithm(spec *omev1beta1.TrafficSpec) reportv1alpha1.TrafficAlgorithm {
	if spec == nil || spec.Algorithm == nil {
		return reportv1alpha1.TrafficAlgorithmDefault
	}
	switch *spec.Algorithm {
	case omev1beta1.LoadBalancingTypeRoundRobin:
		return reportv1alpha1.TrafficAlgorithmRoundRobin
	case omev1beta1.LoadBalancingTypeLeastRequest:
		return reportv1alpha1.TrafficAlgorithmLeastRequest
	case omev1beta1.LoadBalancingTypeRandom:
		return reportv1alpha1.TrafficAlgorithmRandom
	case omev1beta1.LoadBalancingTypeConsistentHash:
		return reportv1alpha1.TrafficAlgorithmConsistentHash
	default:
		return reportv1alpha1.TrafficAlgorithmUnknown
	}
}

func declaredAlgorithmValid(spec *omev1beta1.TrafficSpec) bool {
	if spec == nil || spec.Algorithm == nil {
		return true
	}
	return projectDeclaredAlgorithm(spec) != reportv1alpha1.TrafficAlgorithmUnknown
}

func projectDeclaredHash(spec *omev1beta1.TrafficSpec) reportv1alpha1.TrafficDeclaredFeature {
	if spec == nil || spec.ConsistentHash == nil {
		return absentTrafficFeature()
	}
	hash := spec.ConsistentHash
	feature := reportv1alpha1.TrafficDeclaredFeature{
		State:   reportv1alpha1.TrafficFeatureDeclared,
		Variant: reportv1alpha1.TrafficFeatureVariantUnknown,
	}
	switch hash.Type {
	case omev1beta1.HashTypeHeader:
		feature.Variant = reportv1alpha1.TrafficFeatureVariantHeader
		feature.InputCount = len(hash.Headers)
	case omev1beta1.HashTypeCookie:
		feature.Variant = reportv1alpha1.TrafficFeatureVariantCookie
		if hash.Cookie != nil {
			feature.InputCount = 1
		}
	case omev1beta1.HashTypeSourceIP:
		feature.Variant = reportv1alpha1.TrafficFeatureVariantSourceIP
	}
	algorithm := omev1beta1.LoadBalancingTypeConsistentHash
	if validation.ValidateTrafficSpec(&omev1beta1.TrafficSpec{
		Algorithm: &algorithm, ConsistentHash: hash,
	}) != nil {
		feature.State = reportv1alpha1.TrafficFeatureInvalid
	}
	return feature
}

func projectDeclaredEndpointOverride(spec *omev1beta1.TrafficSpec) reportv1alpha1.TrafficDeclaredFeature {
	if spec == nil || spec.EndpointOverride == nil {
		return absentTrafficFeature()
	}
	override := spec.EndpointOverride
	feature := reportv1alpha1.TrafficDeclaredFeature{
		State:      reportv1alpha1.TrafficFeatureDeclared,
		Variant:    reportv1alpha1.TrafficFeatureVariantUnknown,
		InputCount: len(override.Headers),
	}
	switch override.Type {
	case omev1beta1.EndpointOverrideTypeHeader:
		feature.Variant = reportv1alpha1.TrafficFeatureVariantHeader
	case omev1beta1.EndpointOverrideTypeMetadata:
		feature.Variant = reportv1alpha1.TrafficFeatureVariantMetadata
	}
	if validation.ValidateTrafficSpec(&omev1beta1.TrafficSpec{
		EndpointOverride: override,
	}) != nil {
		feature.State = reportv1alpha1.TrafficFeatureInvalid
	}
	return feature
}

func absentTrafficFeature() reportv1alpha1.TrafficDeclaredFeature {
	return reportv1alpha1.TrafficDeclaredFeature{
		State:   reportv1alpha1.TrafficFeatureAbsent,
		Variant: reportv1alpha1.TrafficFeatureVariantUnavailable,
	}
}

func declaredTrafficAnnotations(annotations map[string]string) (map[string]string, bool) {
	result := make(map[string]string)
	for _, keys := range trafficExtensionKeys {
		for _, key := range keys {
			if value, found := annotations[key]; found {
				result[key] = value
			}
		}
	}
	invalid := false
	for key, value := range annotations {
		for _, prefix := range []string{
			constants.PassthroughEnvoyGatewayPrefix,
			constants.PassthroughIstioPrefix,
		} {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			result[key] = value
			if strings.TrimPrefix(key, prefix) == "" {
				invalid = true
			}
		}
	}
	return result, invalid
}

func projectDeclaredExtensions(annotations map[string]string) []reportv1alpha1.TrafficDeclaredExtension {
	result := make([]reportv1alpha1.TrafficDeclaredExtension, 0, 5)
	for kind, keys := range trafficExtensionKeys {
		count := 0
		for _, key := range keys {
			if _, found := annotations[key]; found {
				count++
			}
		}
		if count > 0 {
			result = append(result, reportv1alpha1.TrafficDeclaredExtension{
				Kind: kind, Count: count,
			})
		}
	}
	for _, passthrough := range []struct {
		prefix string
		kind   reportv1alpha1.TrafficExtensionKind
	}{
		{constants.PassthroughEnvoyGatewayPrefix, reportv1alpha1.TrafficExtensionEnvoyPassthrough},
		{constants.PassthroughIstioPrefix, reportv1alpha1.TrafficExtensionIstioPassthrough},
	} {
		count := 0
		for key := range annotations {
			if strings.HasPrefix(key, passthrough.prefix) {
				count++
			}
		}
		if count > 0 {
			result = append(result, reportv1alpha1.TrafficDeclaredExtension{
				Kind: passthrough.kind, Count: count,
			})
		}
	}
	return result
}

func projectTrafficSupport(
	intent reportv1alpha1.TrafficIntentState,
	status reportv1alpha1.TrafficStatusContent,
) reportv1alpha1.TrafficSupportState {
	if intent == reportv1alpha1.TrafficIntentAbsent {
		return reportv1alpha1.TrafficSupportNotApplicable
	}
	if intent == reportv1alpha1.TrafficIntentInvalid {
		return reportv1alpha1.TrafficSupportInvalid
	}
	if trafficPolicyEvidenceInvalid(status) {
		return reportv1alpha1.TrafficSupportInvalid
	}
	if hasTrafficIssue(status.Issues, reportv1alpha1.TrafficIssueTrafficStatusMissing) {
		return reportv1alpha1.TrafficSupportUnavailable
	}
	if status.Summary.Source.PolicyReady.Freshness != reportv1alpha1.TrafficFreshnessCurrent {
		return reportv1alpha1.TrafficSupportPartial
	}
	switch status.Summary.PolicyReady.Status {
	case reportv1alpha1.TrafficConditionTrue:
		if status.Summary.Source.Unsupported.Freshness != reportv1alpha1.TrafficFreshnessCurrent {
			return reportv1alpha1.TrafficSupportPartial
		}
		switch status.Summary.Unsupported {
		case reportv1alpha1.TrafficUnsupportedNone:
			return reportv1alpha1.TrafficSupportHonored
		case reportv1alpha1.TrafficUnsupportedPresent:
			return reportv1alpha1.TrafficSupportPartial
		default:
			return reportv1alpha1.TrafficSupportPartial
		}
	case reportv1alpha1.TrafficConditionUnknown:
		return reportv1alpha1.TrafficSupportPending
	case reportv1alpha1.TrafficConditionFalse:
		return reportv1alpha1.TrafficSupportRejected
	default:
		return reportv1alpha1.TrafficSupportUnavailable
	}
}

func projectTrafficRealization(status reportv1alpha1.TrafficStatusContent) reportv1alpha1.TrafficRealizationState {
	for _, issue := range status.Issues {
		switch issue.Code {
		case reportv1alpha1.TrafficIssueRouteInvalid,
			reportv1alpha1.TrafficIssueEndpointInvalid,
			reportv1alpha1.TrafficIssueCanaryInvalid,
			reportv1alpha1.TrafficIssueAllocationInvalid,
			reportv1alpha1.TrafficIssueAllocationConflict,
			reportv1alpha1.TrafficIssueUnknownComponentStatus:
			return reportv1alpha1.TrafficRealizationInvalid
		}
	}
	count := len(status.Routes) + len(status.Endpoints) + len(status.Allocations) + len(status.Canaries)
	if status.Canary != nil {
		count++
	}
	if count == 0 {
		return reportv1alpha1.TrafficRealizationUnavailable
	}
	if hasPartialTrafficIssue(status.Issues) {
		return reportv1alpha1.TrafficRealizationPartial
	}
	return reportv1alpha1.TrafficRealizationReported
}

func projectTrafficComparisons(
	isvc *omev1beta1.InferenceService,
	intent reportv1alpha1.TrafficDeclaredIntent,
	support reportv1alpha1.TrafficSupportState,
	status reportv1alpha1.TrafficStatusContent,
) []reportv1alpha1.TrafficExplainComparison {
	var algorithm reportv1alpha1.TrafficComparisonState
	algorithmFreshness := status.Summary.Source.Algorithm.Freshness
	switch {
	case intent.State == reportv1alpha1.TrafficIntentAbsent:
		algorithm = reportv1alpha1.TrafficComparisonNotApplicable
		algorithmFreshness = reportv1alpha1.TrafficFreshnessCurrent
	case intent.State == reportv1alpha1.TrafficIntentInvalid ||
		hasTrafficIssue(status.Issues, reportv1alpha1.TrafficIssueAlgorithmInvalid):
		algorithm = reportv1alpha1.TrafficComparisonInvalid
		algorithmFreshness = reportv1alpha1.TrafficFreshnessUnverifiable
	case status.Summary.Source.Algorithm.Freshness != reportv1alpha1.TrafficFreshnessCurrent:
		algorithm = reportv1alpha1.TrafficComparisonUnverifiable
	case intent.Algorithm == status.Summary.Algorithm:
		algorithm = reportv1alpha1.TrafficComparisonMatch
	default:
		algorithm = reportv1alpha1.TrafficComparisonMismatch
	}

	policy := reportv1alpha1.TrafficComparisonUnverifiable
	policyFreshness := status.Summary.Source.PolicyReady.Freshness
	switch {
	case intent.State == reportv1alpha1.TrafficIntentAbsent:
		policy = reportv1alpha1.TrafficComparisonNotApplicable
		policyFreshness = reportv1alpha1.TrafficFreshnessCurrent
	case intent.State == reportv1alpha1.TrafficIntentInvalid:
		policy = reportv1alpha1.TrafficComparisonInvalid
		policyFreshness = reportv1alpha1.TrafficFreshnessUnverifiable
	case policyFreshness != reportv1alpha1.TrafficFreshnessCurrent:
		policy = reportv1alpha1.TrafficComparisonUnverifiable
	case support == reportv1alpha1.TrafficSupportInvalid:
		policy = reportv1alpha1.TrafficComparisonInvalid
		policyFreshness = reportv1alpha1.TrafficFreshnessUnverifiable
	case support == reportv1alpha1.TrafficSupportHonored ||
		support == reportv1alpha1.TrafficSupportPartial:
		if status.Policy != nil && status.Policy.Source.Freshness == reportv1alpha1.TrafficFreshnessCurrent {
			policy = reportv1alpha1.TrafficComparisonMatch
		} else if status.Policy != nil {
			policyFreshness = status.Policy.Source.Freshness
		} else {
			policyFreshness = reportv1alpha1.TrafficFreshnessUnavailable
		}
	case support == reportv1alpha1.TrafficSupportRejected:
		policy = reportv1alpha1.TrafficComparisonMismatch
	}

	canary := reportv1alpha1.TrafficComparisonNotApplicable
	canaryFreshness := reportv1alpha1.TrafficFreshnessCurrent
	if hasDeclaredCanary(isvc) {
		canary = reportv1alpha1.TrafficComparisonUnverifiable
		canaryFreshness = reportv1alpha1.TrafficFreshnessUnavailable
		if len(status.Canaries) > 0 {
			// Unit observations exist, but there is no single aggregate weight.
			canaryFreshness = reportv1alpha1.TrafficFreshnessUnverifiable
		}
		if hasTrafficIssue(status.Issues, reportv1alpha1.TrafficIssueCanaryInvalid) ||
			hasCanaryAllocationIssue(isvc, status.Issues) {
			canary = reportv1alpha1.TrafficComparisonInvalid
			canaryFreshness = reportv1alpha1.TrafficFreshnessUnverifiable
		} else if status.Canary != nil {
			canary = reportv1alpha1.TrafficComparisonMatch
			canaryFreshness = status.Canary.Source.Freshness
		}
	}

	return []reportv1alpha1.TrafficExplainComparison{
		{Field: reportv1alpha1.TrafficComparisonAlgorithm, State: algorithm,
			Source: computedTrafficSource(algorithmFreshness)},
		{Field: reportv1alpha1.TrafficComparisonPolicy, State: policy,
			Source: computedTrafficSource(policyFreshness)},
		{Field: reportv1alpha1.TrafficComparisonCanaryWeight, State: canary,
			Source: computedTrafficSource(canaryFreshness)},
	}
}

func projectTrafficExplainIssues(
	intent reportv1alpha1.TrafficIntentState,
	specInvalid bool,
	annotationsInvalid bool,
	support reportv1alpha1.TrafficSupportState,
	realization reportv1alpha1.TrafficRealizationState,
	status reportv1alpha1.TrafficStatusContent,
	comparisons []reportv1alpha1.TrafficExplainComparison,
) []reportv1alpha1.TrafficExplainIssue {
	issues := make([]reportv1alpha1.TrafficExplainIssue, 0, 8)
	add := func(code reportv1alpha1.TrafficExplainIssueCode) {
		issues = append(issues, reportv1alpha1.TrafficExplainIssue{Code: code})
	}
	if specInvalid {
		add(reportv1alpha1.TrafficExplainIssueDeclaredSpecInvalid)
	}
	if annotationsInvalid {
		add(reportv1alpha1.TrafficExplainIssueDeclaredAnnotationsInvalid)
	}
	if status.Summary.State == reportv1alpha1.TrafficStateInvalid {
		add(reportv1alpha1.TrafficExplainIssueReportedEvidenceInvalid)
	}
	if trafficStatusStale(status) {
		add(reportv1alpha1.TrafficExplainIssueReportedEvidenceStale)
	}
	if support == reportv1alpha1.TrafficSupportPartial &&
		currentUnsupportedFields(status) {
		add(reportv1alpha1.TrafficExplainIssueUnsupportedDeclaredFields)
	}
	if support == reportv1alpha1.TrafficSupportRejected {
		if status.Summary.PolicyReady.Reason == reportv1alpha1.TrafficReasonNoTranslatorAvailable {
			add(reportv1alpha1.TrafficExplainIssueNoTranslatorAvailable)
		} else {
			add(reportv1alpha1.TrafficExplainIssuePolicyRejected)
		}
	}
	if intent != reportv1alpha1.TrafficIntentAbsent &&
		support == reportv1alpha1.TrafficSupportUnavailable {
		add(reportv1alpha1.TrafficExplainIssueIntentNotReported)
	}
	if intent != reportv1alpha1.TrafficIntentAbsent &&
		realization == reportv1alpha1.TrafficRealizationUnavailable {
		add(reportv1alpha1.TrafficExplainIssueRealizationUnavailable)
	}
	for _, comparison := range comparisons {
		if comparison.Field == reportv1alpha1.TrafficComparisonAlgorithm &&
			comparison.State == reportv1alpha1.TrafficComparisonMismatch {
			add(reportv1alpha1.TrafficExplainIssueAlgorithmMismatch)
		}
	}
	return issues
}

func projectTrafficExplainState(
	intent reportv1alpha1.TrafficIntentState,
	support reportv1alpha1.TrafficSupportState,
	realization reportv1alpha1.TrafficRealizationState,
	status reportv1alpha1.TrafficStatusContent,
	comparisons []reportv1alpha1.TrafficExplainComparison,
) reportv1alpha1.TrafficExplainState {
	if intent == reportv1alpha1.TrafficIntentInvalid ||
		support == reportv1alpha1.TrafficSupportInvalid ||
		realization == reportv1alpha1.TrafficRealizationInvalid ||
		hasComparisonState(comparisons, reportv1alpha1.TrafficComparisonInvalid) {
		return reportv1alpha1.TrafficExplainInvalid
	}
	if intent == reportv1alpha1.TrafficIntentAbsent {
		return reportv1alpha1.TrafficExplainNoIntent
	}
	if support == reportv1alpha1.TrafficSupportPartial &&
		currentUnsupportedFields(status) ||
		(support == reportv1alpha1.TrafficSupportRejected &&
			status.Summary.PolicyReady.Reason == reportv1alpha1.TrafficReasonNoTranslatorAvailable) {
		return reportv1alpha1.TrafficExplainUnsupported
	}
	if hasComparisonState(comparisons, reportv1alpha1.TrafficComparisonMismatch) ||
		support == reportv1alpha1.TrafficSupportRejected {
		return reportv1alpha1.TrafficExplainMismatch
	}
	if support == reportv1alpha1.TrafficSupportPending {
		return reportv1alpha1.TrafficExplainPending
	}
	if support == reportv1alpha1.TrafficSupportUnavailable {
		return reportv1alpha1.TrafficExplainUnavailable
	}
	if support == reportv1alpha1.TrafficSupportPartial ||
		hasComparisonState(comparisons, reportv1alpha1.TrafficComparisonUnverifiable) ||
		realization == reportv1alpha1.TrafficRealizationPartial ||
		realization == reportv1alpha1.TrafficRealizationUnavailable {
		return reportv1alpha1.TrafficExplainPartial
	}
	return reportv1alpha1.TrafficExplainConsistent
}

func trafficExplainSummarySource(content reportv1alpha1.TrafficExplainContent) reportv1alpha1.TrafficValueSource {
	if content.Summary.State == reportv1alpha1.TrafficExplainNoIntent {
		return computedTrafficSource(reportv1alpha1.TrafficFreshnessCurrent)
	}
	freshness := reportv1alpha1.TrafficFreshnessCurrent
	consider := func(candidate reportv1alpha1.TrafficValueSource) {
		if explainFreshnessRank(candidate.Freshness) > explainFreshnessRank(freshness) {
			freshness = candidate.Freshness
		}
	}
	consider(content.Summary.SupportSource)
	consider(content.Summary.RealizationSource)
	for _, comparison := range content.Comparisons {
		consider(comparison.Source)
	}
	return computedTrafficSource(freshness)
}

func currentUnsupportedFields(status reportv1alpha1.TrafficStatusContent) bool {
	return status.Summary.Unsupported == reportv1alpha1.TrafficUnsupportedPresent &&
		status.Summary.Source.Unsupported.Freshness == reportv1alpha1.TrafficFreshnessCurrent
}

// Endpoint, route, allocation and canary errors belong to realization. They
// must not invalidate independent backend-policy evidence.
func trafficPolicyEvidenceInvalid(status reportv1alpha1.TrafficStatusContent) bool {
	for _, issue := range status.Issues {
		switch issue.Code {
		case reportv1alpha1.TrafficIssueAlgorithmInvalid,
			reportv1alpha1.TrafficIssueConditionInvalid,
			reportv1alpha1.TrafficIssueConditionConflict,
			reportv1alpha1.TrafficIssuePolicyReferenceInvalid,
			reportv1alpha1.TrafficIssuePolicyKindUnsupported,
			reportv1alpha1.TrafficIssueStatusCombinationInvalid:
			return true
		}
	}
	return false
}

func computedTrafficSource(freshness reportv1alpha1.TrafficFreshness) reportv1alpha1.TrafficValueSource {
	if freshness == "" {
		freshness = reportv1alpha1.TrafficFreshnessUnavailable
	}
	return reportv1alpha1.TrafficValueSource{
		Evidence: reportv1alpha1.EvidenceComputed, Freshness: freshness,
	}
}

func hasDeclaredCanary(isvc *omev1beta1.InferenceService) bool {
	rollout := omerollout.Effective(isvc)
	if rollout == nil {
		return false
	}
	for i := range rollout.Groups {
		if rollout.Groups[i].Canary != nil {
			return true
		}
	}
	return false
}

func hasCanaryAllocationIssue(isvc *omev1beta1.InferenceService, issues []reportv1alpha1.TrafficIssue) bool {
	rollout := omerollout.Effective(isvc)
	if rollout == nil {
		return false
	}
	for i := range rollout.Groups {
		group := &rollout.Groups[i]
		if group.Canary == nil {
			continue
		}
		primary, _, valid := canaryPrimary(group)
		if !valid {
			continue
		}
		for _, issue := range issues {
			if issue.Component == primary && (issue.Code == reportv1alpha1.TrafficIssueAllocationInvalid ||
				issue.Code == reportv1alpha1.TrafficIssueAllocationConflict) {
				return true
			}
		}
	}
	return false
}

func hasComparisonState(
	comparisons []reportv1alpha1.TrafficExplainComparison,
	want reportv1alpha1.TrafficComparisonState,
) bool {
	for _, comparison := range comparisons {
		if comparison.State == want {
			return true
		}
	}
	return false
}

func hasTrafficIssue(
	issues []reportv1alpha1.TrafficIssue,
	want reportv1alpha1.TrafficIssueCode,
) bool {
	for _, issue := range issues {
		if issue.Code == want {
			return true
		}
	}
	return false
}

func hasPartialTrafficIssue(issues []reportv1alpha1.TrafficIssue) bool {
	for _, issue := range issues {
		switch issue.Code {
		case reportv1alpha1.TrafficIssueRoutesTruncated,
			reportv1alpha1.TrafficIssueEndpointsTruncated,
			reportv1alpha1.TrafficIssueAllocationsTruncated:
			return true
		}
	}
	return false
}

func trafficStatusStale(status reportv1alpha1.TrafficStatusContent) bool {
	for _, condition := range status.Conditions {
		if condition.Source.Freshness == reportv1alpha1.TrafficFreshnessStale {
			return true
		}
	}
	return false
}

func explainFreshnessRank(value reportv1alpha1.TrafficFreshness) int {
	switch value {
	case reportv1alpha1.TrafficFreshnessCurrent:
		return 0
	case reportv1alpha1.TrafficFreshnessUnverifiable:
		return 1
	case reportv1alpha1.TrafficFreshnessStale:
		return 2
	default:
		return 3
	}
}
