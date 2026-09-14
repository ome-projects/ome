// Package acceleratorprojection turns bounded accelerator evidence into the
// allowlisted kubectl-ome report contract.
package acceleratorprojection

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

var (
	ErrInvalidEvidence      = errors.New("accelerator projection evidence is invalid")
	ErrInvalidClassEvidence = errors.New("accelerator class evidence is invalid")
)

// AcceleratorClassEvidence is constructed only through the functions below,
// so unavailable reads cannot accidentally retain a resource payload.
type AcceleratorClassEvidence struct {
	state             reportv1alpha1.AcceleratorClassState
	name              string
	uid               string
	resourceVersion   string
	generation        int64
	unavailableReason reportv1alpha1.UnavailableReason
}

func ObserveAcceleratorClass(class *omev1beta1.AcceleratorClass) (AcceleratorClassEvidence, error) {
	if class == nil || !validObjectName(class.Name) || class.Namespace != "" ||
		class.UID == "" || class.ResourceVersion == "" || class.Generation < 0 {
		return AcceleratorClassEvidence{}, ErrInvalidClassEvidence
	}
	return AcceleratorClassEvidence{
		state: reportv1alpha1.AcceleratorClassObserved, name: class.Name,
		uid: string(class.UID), resourceVersion: class.ResourceVersion,
		generation: class.Generation,
	}, nil
}

func UnavailableAcceleratorClass(
	name string,
	state reportv1alpha1.AcceleratorClassState,
) (AcceleratorClassEvidence, error) {
	if !validObjectName(name) {
		return AcceleratorClassEvidence{}, ErrInvalidClassEvidence
	}
	reason, ok := unavailableClassReason(state)
	if !ok {
		return AcceleratorClassEvidence{}, ErrInvalidClassEvidence
	}
	return AcceleratorClassEvidence{state: state, name: name, unavailableReason: reason}, nil
}

// ReportedAcceleratorClassNames returns at most the two current, structurally
// valid Engine/Decoder class references. It never returns stale or arbitrary
// component status and is used to bound exact AcceleratorClass GETs.
func ReportedAcceleratorClassNames(isvc *omev1beta1.InferenceService) []string {
	if isvc == nil || expectedFreshness(isvc) != effective.StatusFreshnessCurrent {
		return []string{}
	}
	names := make(map[string]struct{}, 2)
	for _, component := range []omev1beta1.ComponentType{
		omev1beta1.EngineComponent, omev1beta1.DecoderComponent,
	} {
		status, found := isvc.Status.Components[component]
		if !found || status.SelectedAccelerator == nil ||
			!validReportedSelection(status.SelectedAccelerator) {
			continue
		}
		names[status.SelectedAccelerator.AcceleratorClass] = struct{}{}
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func Project(
	isvc *omev1beta1.InferenceService,
	base effective.AcceleratorBaseResolution,
	classes map[string]AcceleratorClassEvidence,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.AcceleratorExplainReport, error) {
	if !validPrimaryEvidence(isvc, base) || !validClassEvidenceMap(classes) ||
		!completeClassEvidence(isvc, base, classes) {
		return reportv1alpha1.AcceleratorExplainReport{}, ErrInvalidEvidence
	}
	freshness, ok := mapFreshness(base.StatusFreshness)
	if !ok {
		return reportv1alpha1.AcceleratorExplainReport{}, ErrInvalidEvidence
	}
	content := reportv1alpha1.AcceleratorExplainContent{
		Summary:    reportv1alpha1.AcceleratorExplainSummary{StatusFreshness: freshness},
		Components: []reportv1alpha1.AcceleratorExplainComponent{},
		Issues:     []reportv1alpha1.AcceleratorExplainIssue{},
	}
	reportValue := reportv1alpha1.NewAcceleratorExplainReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name}, content, clock,
	)
	reportValue.Sources = append(reportValue.Sources, reportv1alpha1.AcceleratorSourceReference{
		Kind: "InferenceService", Namespace: isvc.Namespace, Name: isvc.Name,
		Generation: isvc.Generation, Evidence: reportv1alpha1.EvidenceObserved,
	})
	if base.ActiveSourceName != "" {
		reportValue.Sources = append(reportValue.Sources, reportv1alpha1.AcceleratorSourceReference{
			Kind: base.ActiveSourceKind, Namespace: base.ActiveSourceNamespace,
			Name:       base.ActiveSourceName,
			Generation: base.ActiveSourceGeneration, Evidence: reportv1alpha1.EvidenceObserved,
		})
	}

	baseByComponent := make(map[omev1beta1.ComponentType]effective.AcceleratorBaseComponent, len(base.Components))
	for _, component := range base.Components {
		baseByComponent[component.Type] = component
	}
	for _, componentType := range []omev1beta1.ComponentType{
		omev1beta1.EngineComponent, omev1beta1.DecoderComponent,
	} {
		baseComponent, found := baseByComponent[componentType]
		if !found {
			continue
		}
		component, usedClass := projectComponent(isvc, base, baseComponent, classes)
		reportValue.Content.Components = append(reportValue.Content.Components, component)
		for _, code := range component.Issues {
			reportValue.Content.Issues = append(reportValue.Content.Issues,
				reportv1alpha1.AcceleratorExplainIssue{Code: code, Component: component.Type},
			)
		}
		if usedClass != "" {
			evidence := classes[usedClass]
			reportValue.Sources = append(reportValue.Sources, classSource(evidence))
		}
	}
	appendUnexpectedStatusIssues(isvc, &reportValue)
	reportValue.Content.Summary.State = summarize(reportValue.Content)
	reportValue.Warnings = warningsFor(reportValue.Content)
	return reportValue.Canonical(), nil
}

func projectComponent(
	isvc *omev1beta1.InferenceService,
	base effective.AcceleratorBaseResolution,
	baseComponent effective.AcceleratorBaseComponent,
	classes map[string]AcceleratorClassEvidence,
) (reportv1alpha1.AcceleratorExplainComponent, string) {
	componentType, _ := mapComponent(baseComponent.Type)
	intent, intentIssues := projectIntent(isvc, baseComponent.Type)
	component := reportv1alpha1.AcceleratorExplainComponent{
		Type: componentType, Intent: intent,
		Selection: reportv1alpha1.AcceleratorSelectionObservation{
			State:  reportv1alpha1.AcceleratorSelectionUnavailable,
			Reason: reportv1alpha1.AcceleratorReason{State: reportv1alpha1.AcceleratorReasonUnavailable},
		},
		Class: reportv1alpha1.AcceleratorClassObservation{State: reportv1alpha1.AcceleratorClassNotRequested},
		Requests: reportv1alpha1.AcceleratorRequestObservation{
			BaseState:      mapBaseRequestState(baseComponent.State),
			Base:           mapBaseRequests(baseComponent.Requests),
			EffectiveState: reportv1alpha1.AcceleratorRequestsUnavailable,
			Effective:      []reportv1alpha1.AcceleratorResourceRequest{},
		},
		Issues: append([]reportv1alpha1.AcceleratorExplainIssueCode{}, intentIssues...),
	}
	configured := intent.State != reportv1alpha1.AcceleratorIntentNotConfigured
	status := isvc.Status.Components[baseComponent.Type].SelectedAccelerator
	switch baseComponent.State {
	case effective.AcceleratorBaseUnavailable:
		component.Requests.Base = []reportv1alpha1.AcceleratorResourceRequest{}
		if base.ActiveState != effective.AcceleratorActiveUnavailable {
			component.Issues = append(
				component.Issues,
				reportv1alpha1.AcceleratorIssueBaseRequestsUnavailable,
			)
		}
	case effective.AcceleratorBaseInvalid:
		component.Requests.Base = []reportv1alpha1.AcceleratorResourceRequest{}
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueRequestsInvalid)
	}
	if base.ActiveState == effective.AcceleratorActiveInconsistent {
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueActiveRevisionInconsistent)
	}
	if base.ActiveState == effective.AcceleratorActiveUnavailable {
		component.Issues = append(component.Issues,
			reportv1alpha1.AcceleratorIssueActiveConfigurationUnavailable)
	}
	if !configured && status == nil {
		component.Selection.State = reportv1alpha1.AcceleratorSelectionNotConfigured
		component.Selection.Reason.State = reportv1alpha1.AcceleratorReasonNotReported
		component.Requests.EffectiveState = reportv1alpha1.AcceleratorRequestsNotConfigured
		return canonicalComponent(component), ""
	}
	if base.ActiveState == effective.AcceleratorActiveUnavailable {
		if !configured && status != nil {
			component.Issues = append(component.Issues,
				reportv1alpha1.AcceleratorIssueSelectionUnexpected)
		}
		return canonicalComponent(component), ""
	}
	switch base.StatusFreshness {
	case effective.StatusFreshnessStale:
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueStatusStale)
		return canonicalComponent(component), ""
	case effective.StatusFreshnessUnknown:
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueStatusUnobserved)
		return canonicalComponent(component), ""
	case effective.StatusFreshnessInconsistent:
		component.Selection.State = reportv1alpha1.AcceleratorSelectionInvalid
		component.Selection.Reason.State = reportv1alpha1.AcceleratorReasonInvalid
		component.Requests.EffectiveState = reportv1alpha1.AcceleratorRequestsInvalid
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueStatusInvalid)
		return canonicalComponent(component), ""
	case effective.StatusFreshnessCurrent:
	default:
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueStatusInvalid)
		return canonicalComponent(component), ""
	}

	if status == nil {
		if !configured {
			component.Selection.State = reportv1alpha1.AcceleratorSelectionNotConfigured
			component.Selection.Reason.State = reportv1alpha1.AcceleratorReasonNotReported
			component.Requests.EffectiveState = reportv1alpha1.AcceleratorRequestsNotConfigured
			return canonicalComponent(component), ""
		}
		component.Selection.State = reportv1alpha1.AcceleratorSelectionNotReported
		component.Selection.Reason.State = reportv1alpha1.AcceleratorReasonNotReported
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueSelectionNotReported)
		return canonicalComponent(component), ""
	}
	requests, requestsValid := safeReportedRequests(status.ResourceRequests)
	classValid := validObjectName(status.AcceleratorClass)
	nodeSelectorValid := validNodeSelector(status.NodeSelector)
	if !classValid || !nodeSelectorValid || !requestsValid {
		component.Selection.State = reportv1alpha1.AcceleratorSelectionInvalid
		component.Selection.Reason.State = reportv1alpha1.AcceleratorReasonInvalid
		component.Requests.EffectiveState = reportv1alpha1.AcceleratorRequestsInvalid
		if !requestsValid {
			component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueRequestsInvalid)
		}
		if !classValid {
			component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueClassInvalid)
		}
		if !nodeSelectorValid {
			component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueNodeSelectorInvalid)
		}
		return canonicalComponent(component), ""
	}
	component.Selection.State = reportv1alpha1.AcceleratorSelectionReported
	component.Selection.Class = status.AcceleratorClass
	component.Selection.Reason = projectReason(status.Reason)
	if status.ResourceRequests == nil {
		component.Requests.EffectiveState = reportv1alpha1.AcceleratorRequestsNotReported
		component.Issues = append(
			component.Issues, reportv1alpha1.AcceleratorIssueRequestsNotReported,
		)
	} else {
		component.Requests.EffectiveState = reportv1alpha1.AcceleratorRequestsReported
		component.Requests.Effective = requests
	}
	if !configured {
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueSelectionUnexpected)
	}
	if intent.DeclaredClass != "" && intent.DeclaredClass != status.AcceleratorClass {
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueReportedClassMismatch)
	}
	evidence, found := classes[status.AcceleratorClass]
	if !found {
		component.Class = reportv1alpha1.AcceleratorClassObservation{
			State: reportv1alpha1.AcceleratorClassUnreadable, Name: status.AcceleratorClass,
		}
		component.Issues = append(component.Issues, reportv1alpha1.AcceleratorIssueClassUnreadable)
		return canonicalComponent(component), ""
	}
	component.Class = reportv1alpha1.AcceleratorClassObservation{State: evidence.state, Name: evidence.name}
	if issue := classIssue(evidence.state); issue != "" {
		component.Issues = append(component.Issues, issue)
	}
	return canonicalComponent(component), status.AcceleratorClass
}

func projectIntent(
	isvc *omev1beta1.InferenceService,
	component omev1beta1.ComponentType,
) (reportv1alpha1.AcceleratorIntent, []reportv1alpha1.AcceleratorExplainIssueCode) {
	intent := reportv1alpha1.AcceleratorIntent{State: reportv1alpha1.AcceleratorIntentNotConfigured}
	issues := []reportv1alpha1.AcceleratorExplainIssueCode{}
	componentSelector := componentAcceleratorSelector(isvc, component)
	serviceSelector := isvc.Spec.AcceleratorSelector

	classPointerSet := false
	if componentSelector != nil && componentSelector.AcceleratorClass != nil {
		classPointerSet = true
		if validObjectName(*componentSelector.AcceleratorClass) {
			intent.State = reportv1alpha1.AcceleratorIntentClass
			intent.DeclaredClass = *componentSelector.AcceleratorClass
			intent.ClassSource = reportv1alpha1.AcceleratorSelectorSourceComponent
		} else {
			intent.State = reportv1alpha1.AcceleratorIntentInvalid
			issues = append(issues, reportv1alpha1.AcceleratorIssueDeclaredClassInvalid)
		}
	} else if serviceSelector != nil && serviceSelector.AcceleratorClass != nil {
		classPointerSet = true
		if validObjectName(*serviceSelector.AcceleratorClass) {
			intent.State = reportv1alpha1.AcceleratorIntentClass
			intent.DeclaredClass = *serviceSelector.AcceleratorClass
			intent.ClassSource = reportv1alpha1.AcceleratorSelectorSourceService
		} else {
			intent.State = reportv1alpha1.AcceleratorIntentInvalid
			issues = append(issues, reportv1alpha1.AcceleratorIssueDeclaredClassInvalid)
		}
	}

	policy, source := effectivePolicy(componentSelector, serviceSelector)
	if policy != "" {
		mapped, ok := mapPolicy(policy)
		if !ok {
			intent.State = reportv1alpha1.AcceleratorIntentInvalid
			issues = append(issues, reportv1alpha1.AcceleratorIssuePolicyInvalid)
		} else {
			intent.Policy = mapped
			intent.PolicySource = source
			if !classPointerSet {
				intent.State = reportv1alpha1.AcceleratorIntentPolicy
			}
		}
	}
	return intent, dedupeIssueCodes(issues)
}

func componentAcceleratorSelector(
	isvc *omev1beta1.InferenceService,
	component omev1beta1.ComponentType,
) *omev1beta1.AcceleratorSelector {
	if isvc == nil {
		return nil
	}
	switch component {
	case omev1beta1.EngineComponent:
		if isvc.Spec.Engine != nil {
			return isvc.Spec.Engine.AcceleratorOverride
		}
	case omev1beta1.DecoderComponent:
		if isvc.Spec.Decoder != nil {
			return isvc.Spec.Decoder.AcceleratorOverride
		}
	}
	return nil
}

func effectivePolicy(
	component, service *omev1beta1.AcceleratorSelector,
) (omev1beta1.AcceleratorSelectionPolicy, reportv1alpha1.AcceleratorSelectorSource) {
	if component != nil && component.Policy != "" {
		return component.Policy, reportv1alpha1.AcceleratorSelectorSourceComponent
	}
	if service != nil && service.Policy != "" {
		return service.Policy, reportv1alpha1.AcceleratorSelectorSourceService
	}
	return "", ""
}

func projectReason(reason string) reportv1alpha1.AcceleratorReason {
	if reason == "" {
		return reportv1alpha1.AcceleratorReason{State: reportv1alpha1.AcceleratorReasonNotReported}
	}
	digest := sha256.Sum256([]byte(reason))
	return reportv1alpha1.AcceleratorReason{
		State:  reportv1alpha1.AcceleratorReasonReported,
		Digest: fmt.Sprintf("rs1:%x", digest[:6]),
	}
}

func safeReportedRequests(values map[string]string) ([]reportv1alpha1.AcceleratorResourceRequest, bool) {
	if len(values) > 64 {
		return []reportv1alpha1.AcceleratorResourceRequest{}, false
	}
	result := make([]reportv1alpha1.AcceleratorResourceRequest, 0, len(values))
	for name, value := range values {
		if len(validation.IsQualifiedName(name)) > 0 || len(value) > 128 {
			return []reportv1alpha1.AcceleratorResourceRequest{}, false
		}
		quantity, err := resource.ParseQuantity(value)
		if err != nil || quantity.Sign() < 0 {
			return []reportv1alpha1.AcceleratorResourceRequest{}, false
		}
		result = append(result, reportv1alpha1.AcceleratorResourceRequest{
			Name: name, Quantity: quantity.String(),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, true
}

func validReportedSelection(selection *omev1beta1.AcceleratorSelection) bool {
	if selection == nil || !validObjectName(selection.AcceleratorClass) ||
		!validNodeSelector(selection.NodeSelector) {
		return false
	}
	_, valid := safeReportedRequests(selection.ResourceRequests)
	return valid
}

func validNodeSelector(selector map[string]string) bool {
	if len(selector) > 64 {
		return false
	}
	for key, value := range selector {
		if len(validation.IsQualifiedName(key)) > 0 || len(validation.IsValidLabelValue(value)) > 0 {
			return false
		}
	}
	return true
}

func validPrimaryEvidence(
	isvc *omev1beta1.InferenceService,
	base effective.AcceleratorBaseResolution,
) bool {
	if isvc == nil || isvc.Name == "" || isvc.Namespace == "" || isvc.UID == "" ||
		isvc.ResourceVersion == "" || isvc.Generation <= 0 ||
		base.StatusFreshness != expectedFreshness(isvc) || !validBaseState(base) {
		return false
	}
	seen := map[omev1beta1.ComponentType]bool{}
	for _, component := range base.Components {
		if component.Type != omev1beta1.EngineComponent && component.Type != omev1beta1.DecoderComponent ||
			seen[component.Type] || !validBaseComponent(component) {
			return false
		}
		seen[component.Type] = true
	}
	return len(base.Components) > 0 && seen[omev1beta1.EngineComponent] == (isvc.Spec.Engine != nil) &&
		seen[omev1beta1.DecoderComponent] == (isvc.Spec.Decoder != nil)
}

func validBaseState(base effective.AcceleratorBaseResolution) bool {
	switch base.ActiveState {
	case effective.AcceleratorActiveUnavailable:
		return base.RuntimeName == "" && base.RuntimeKind == "" && base.RuntimeNamespace == "" &&
			base.ActiveOrigin == "" && base.ActiveConsistency == "" && base.ActiveRevisionName == "" &&
			base.ActiveSourceKind == "" && base.ActiveSourceName == "" &&
			base.ActiveSourceNamespace == "" && base.ActiveSourceUID == "" &&
			base.ActiveSourceResourceVersion == "" && base.ActiveSourceGeneration == 0 &&
			!base.RuntimeAcceleratorsConfigured && validComponentsForActiveState(base)
	case effective.AcceleratorActiveAvailable, effective.AcceleratorActiveInconsistent:
		if !validObjectName(base.RuntimeName) {
			return false
		}
		switch base.RuntimeKind {
		case runtimeselector.KindServingRuntime:
			if !validNamespace(base.RuntimeNamespace) {
				return false
			}
		case runtimeselector.KindClusterServingRuntime:
			if base.RuntimeNamespace != "" {
				return false
			}
		default:
			return false
		}
		return validActiveSource(base) && validComponentsForActiveState(base)
	default:
		return false
	}
}

func validComponentsForActiveState(base effective.AcceleratorBaseResolution) bool {
	if base.ActiveState == effective.AcceleratorActiveAvailable {
		return true
	}
	for _, component := range base.Components {
		if component.State != effective.AcceleratorBaseUnavailable || len(component.Requests) != 0 {
			return false
		}
	}
	return true
}

func validActiveSource(base effective.AcceleratorBaseResolution) bool {
	if base.ActiveSourceUID == "" || base.ActiveSourceResourceVersion == "" ||
		base.ActiveSourceGeneration < 0 {
		return false
	}
	switch base.ActiveOrigin {
	case effective.ConfigurationOriginLiveRuntime:
		return base.ActiveState == effective.AcceleratorActiveAvailable &&
			base.ActiveConsistency == effective.RevisionConsistencyUnknown &&
			base.ActiveRevisionName == "" && base.ActiveSourceKind == base.RuntimeKind &&
			base.ActiveSourceName == base.RuntimeName &&
			base.ActiveSourceNamespace == base.RuntimeNamespace
	case effective.ConfigurationOriginControllerRevision:
		if base.ActiveState == effective.AcceleratorActiveAvailable &&
			base.ActiveConsistency != effective.RevisionConsistencyConsistent {
			return false
		}
		if base.ActiveState == effective.AcceleratorActiveInconsistent &&
			base.ActiveConsistency == effective.RevisionConsistencyConsistent {
			return false
		}
		return validObjectName(base.ActiveRevisionName) &&
			base.ActiveSourceKind == "ControllerRevision" &&
			validObjectName(base.ActiveSourceName) &&
			validNamespace(base.ActiveSourceNamespace) &&
			base.ActiveSourceName == base.ActiveRevisionName && base.ActiveSourceGeneration == 0
	default:
		return false
	}
}

func completeClassEvidence(
	isvc *omev1beta1.InferenceService,
	base effective.AcceleratorBaseResolution,
	classes map[string]AcceleratorClassEvidence,
) bool {
	expected := []string{}
	if base.ActiveState != effective.AcceleratorActiveUnavailable {
		expected = ReportedAcceleratorClassNames(isvc)
	}
	if len(classes) != len(expected) {
		return false
	}
	for _, name := range expected {
		if _, found := classes[name]; !found {
			return false
		}
	}
	return true
}

func validBaseComponent(component effective.AcceleratorBaseComponent) bool {
	switch component.State {
	case effective.AcceleratorBaseAvailable:
		for _, request := range component.Requests {
			if !validSafeRequest(request.Name, request.Quantity) {
				return false
			}
		}
		return uniqueBaseRequestNames(component.Requests)
	case effective.AcceleratorBaseUnavailable, effective.AcceleratorBaseInvalid:
		return len(component.Requests) == 0
	default:
		return false
	}
}

func validSafeRequest(name, value string) bool {
	if len(validation.IsQualifiedName(name)) > 0 || len(value) > 128 {
		return false
	}
	quantity, err := resource.ParseQuantity(value)
	return err == nil && quantity.Sign() >= 0 && quantity.String() == value
}

func uniqueBaseRequestNames(requests []effective.AcceleratorBaseRequest) bool {
	seen := map[string]struct{}{}
	for _, request := range requests {
		if _, found := seen[request.Name]; found {
			return false
		}
		seen[request.Name] = struct{}{}
	}
	return len(requests) <= 64
}

func validClassEvidenceMap(classes map[string]AcceleratorClassEvidence) bool {
	if len(classes) > 2 {
		return false
	}
	for name, evidence := range classes {
		if name != evidence.name || !validObjectName(name) {
			return false
		}
		if evidence.state == reportv1alpha1.AcceleratorClassObserved {
			if evidence.uid == "" || evidence.resourceVersion == "" ||
				evidence.unavailableReason != "" || evidence.generation < 0 {
				return false
			}
			continue
		}
		reason, ok := unavailableClassReason(evidence.state)
		if !ok || evidence.uid != "" || evidence.resourceVersion != "" ||
			evidence.generation != 0 || evidence.unavailableReason != reason {
			return false
		}
	}
	return true
}

func unavailableClassReason(
	state reportv1alpha1.AcceleratorClassState,
) (reportv1alpha1.UnavailableReason, bool) {
	switch state {
	case reportv1alpha1.AcceleratorClassNotFound:
		return reportv1alpha1.UnavailableNotFound, true
	case reportv1alpha1.AcceleratorClassForbidden:
		return reportv1alpha1.UnavailableForbidden, true
	case reportv1alpha1.AcceleratorClassUnsupportedAPI:
		return reportv1alpha1.UnavailableUnsupportedAPI, true
	case reportv1alpha1.AcceleratorClassUnreadable:
		return reportv1alpha1.UnavailableUnreadable, true
	case reportv1alpha1.AcceleratorClassInvalid:
		return reportv1alpha1.UnavailableMalformedPayload, true
	default:
		return "", false
	}
}

func classSource(evidence AcceleratorClassEvidence) reportv1alpha1.AcceleratorSourceReference {
	level := reportv1alpha1.EvidenceObserved
	if evidence.state != reportv1alpha1.AcceleratorClassObserved {
		level = reportv1alpha1.EvidenceUnavailable
	}
	return reportv1alpha1.AcceleratorSourceReference{
		Kind: "AcceleratorClass", Name: evidence.name,
		Generation: evidence.generation, Evidence: level,
		UnavailableReason: evidence.unavailableReason,
	}
}

func mapBaseRequestState(
	state effective.AcceleratorBaseState,
) reportv1alpha1.AcceleratorBaseRequestState {
	switch state {
	case effective.AcceleratorBaseAvailable:
		return reportv1alpha1.AcceleratorBaseRequestsAvailable
	case effective.AcceleratorBaseUnavailable:
		return reportv1alpha1.AcceleratorBaseRequestsUnavailable
	case effective.AcceleratorBaseInvalid:
		return reportv1alpha1.AcceleratorBaseRequestsInvalid
	default:
		return reportv1alpha1.AcceleratorBaseRequestsInvalid
	}
}

func mapBaseRequests(items []effective.AcceleratorBaseRequest) []reportv1alpha1.AcceleratorResourceRequest {
	result := make([]reportv1alpha1.AcceleratorResourceRequest, len(items))
	for i := range items {
		result[i] = reportv1alpha1.AcceleratorResourceRequest{Name: items[i].Name, Quantity: items[i].Quantity}
	}
	return result
}

func mapPolicy(policy omev1beta1.AcceleratorSelectionPolicy) (reportv1alpha1.AcceleratorPolicy, bool) {
	switch policy {
	case omev1beta1.BestFitPolicy:
		return reportv1alpha1.AcceleratorPolicyBestFit, true
	case omev1beta1.CheapestPolicy:
		return reportv1alpha1.AcceleratorPolicyCheapest, true
	case omev1beta1.MostCapablePolicy:
		return reportv1alpha1.AcceleratorPolicyMostCapable, true
	case omev1beta1.FirstAvailablePolicy:
		return reportv1alpha1.AcceleratorPolicyFirstAvailable, true
	default:
		return "", false
	}
}

func mapFreshness(value effective.StatusFreshness) (reportv1alpha1.StatusFreshness, bool) {
	switch value {
	case effective.StatusFreshnessCurrent:
		return reportv1alpha1.StatusFreshnessCurrent, true
	case effective.StatusFreshnessStale:
		return reportv1alpha1.StatusFreshnessStale, true
	case effective.StatusFreshnessUnknown:
		return reportv1alpha1.StatusFreshnessUnobserved, true
	case effective.StatusFreshnessInconsistent:
		return reportv1alpha1.StatusFreshnessInvalid, true
	default:
		return "", false
	}
}

func expectedFreshness(isvc *omev1beta1.InferenceService) effective.StatusFreshness {
	if isvc == nil {
		return ""
	}
	switch {
	case isvc.Status.ObservedGeneration == 0:
		return effective.StatusFreshnessUnknown
	case isvc.Status.ObservedGeneration == isvc.Generation:
		return effective.StatusFreshnessCurrent
	case isvc.Status.ObservedGeneration < isvc.Generation:
		return effective.StatusFreshnessStale
	default:
		return effective.StatusFreshnessInconsistent
	}
}

func mapComponent(component omev1beta1.ComponentType) (reportv1alpha1.RuntimeComponentType, bool) {
	switch component {
	case omev1beta1.EngineComponent:
		return reportv1alpha1.RuntimeComponentEngine, true
	case omev1beta1.DecoderComponent:
		return reportv1alpha1.RuntimeComponentDecoder, true
	case omev1beta1.RouterComponent:
		return reportv1alpha1.RuntimeComponentRouter, true
	default:
		return "", false
	}
}

func appendUnexpectedStatusIssues(
	isvc *omev1beta1.InferenceService,
	reportValue *reportv1alpha1.AcceleratorExplainReport,
) {
	for component, status := range isvc.Status.Components {
		if status.SelectedAccelerator == nil ||
			component == omev1beta1.EngineComponent || component == omev1beta1.DecoderComponent {
			continue
		}
		mapped, ok := mapComponent(component)
		if !ok {
			mapped = ""
		}
		reportValue.Content.Issues = append(reportValue.Content.Issues,
			reportv1alpha1.AcceleratorExplainIssue{
				Code: reportv1alpha1.AcceleratorIssueUnexpectedComponentEvidence, Component: mapped,
			},
		)
	}
}

func classIssue(state reportv1alpha1.AcceleratorClassState) reportv1alpha1.AcceleratorExplainIssueCode {
	switch state {
	case reportv1alpha1.AcceleratorClassNotFound:
		return reportv1alpha1.AcceleratorIssueClassNotFound
	case reportv1alpha1.AcceleratorClassForbidden:
		return reportv1alpha1.AcceleratorIssueClassForbidden
	case reportv1alpha1.AcceleratorClassUnsupportedAPI:
		return reportv1alpha1.AcceleratorIssueClassUnsupportedAPI
	case reportv1alpha1.AcceleratorClassUnreadable:
		return reportv1alpha1.AcceleratorIssueClassUnreadable
	case reportv1alpha1.AcceleratorClassInvalid:
		return reportv1alpha1.AcceleratorIssueClassInvalid
	default:
		return ""
	}
}

func summarize(content reportv1alpha1.AcceleratorExplainContent) reportv1alpha1.AcceleratorExplainState {
	allNotConfigured := len(content.Components) > 0
	hasInvalid := false
	hasIssue := len(content.Issues) > 0
	for _, component := range content.Components {
		if component.Intent.State != reportv1alpha1.AcceleratorIntentNotConfigured ||
			component.Selection.State != reportv1alpha1.AcceleratorSelectionNotConfigured ||
			component.Requests.BaseState != reportv1alpha1.AcceleratorBaseRequestsAvailable ||
			component.Requests.EffectiveState != reportv1alpha1.AcceleratorRequestsNotConfigured {
			allNotConfigured = false
		}
		if component.Intent.State == reportv1alpha1.AcceleratorIntentInvalid ||
			component.Selection.State == reportv1alpha1.AcceleratorSelectionInvalid ||
			component.Requests.BaseState == reportv1alpha1.AcceleratorBaseRequestsInvalid ||
			component.Requests.EffectiveState == reportv1alpha1.AcceleratorRequestsInvalid ||
			component.Class.State == reportv1alpha1.AcceleratorClassInvalid {
			hasInvalid = true
		}
		if component.Requests.BaseState == reportv1alpha1.AcceleratorBaseRequestsUnavailable {
			hasIssue = true
		}
		if len(component.Issues) > 0 {
			hasIssue = true
		}
	}
	if hasInvalid {
		return reportv1alpha1.AcceleratorExplainInvalid
	}
	if allNotConfigured && !hasIssue {
		return reportv1alpha1.AcceleratorExplainNotConfigured
	}
	if hasIssue {
		return reportv1alpha1.AcceleratorExplainPartial
	}
	return reportv1alpha1.AcceleratorExplainReported
}

func warningsFor(content reportv1alpha1.AcceleratorExplainContent) []reportv1alpha1.AcceleratorWarning {
	warnings := []reportv1alpha1.AcceleratorWarning{}
	if content.Summary.State == reportv1alpha1.AcceleratorExplainPartial ||
		content.Summary.State == reportv1alpha1.AcceleratorExplainInvalid {
		warnings = append(warnings, reportv1alpha1.AcceleratorWarning{Code: reportv1alpha1.AcceleratorWarningPartialData})
	}
	for _, issue := range content.Issues {
		switch issue.Code {
		case reportv1alpha1.AcceleratorIssueStatusStale:
			warnings = append(warnings, reportv1alpha1.AcceleratorWarning{Code: reportv1alpha1.AcceleratorWarningStaleEvidence})
		case reportv1alpha1.AcceleratorIssueActiveConfigurationUnavailable,
			reportv1alpha1.AcceleratorIssueBaseRequestsUnavailable,
			reportv1alpha1.AcceleratorIssueClassForbidden,
			reportv1alpha1.AcceleratorIssueClassNotFound,
			reportv1alpha1.AcceleratorIssueClassUnreadable,
			reportv1alpha1.AcceleratorIssueClassUnsupportedAPI:
			warnings = append(warnings, reportv1alpha1.AcceleratorWarning{Code: reportv1alpha1.AcceleratorWarningSourceUnavailable})
		}
	}
	return warnings
}

func canonicalComponent(component reportv1alpha1.AcceleratorExplainComponent) reportv1alpha1.AcceleratorExplainComponent {
	component.Issues = dedupeIssueCodes(component.Issues)
	return component
}

func dedupeIssueCodes(items []reportv1alpha1.AcceleratorExplainIssueCode) []reportv1alpha1.AcceleratorExplainIssueCode {
	sort.Slice(items, func(i, j int) bool { return items[i] < items[j] })
	result := make([]reportv1alpha1.AcceleratorExplainIssueCode, 0, len(items))
	for _, item := range items {
		if len(result) == 0 || result[len(result)-1] != item {
			result = append(result, item)
		}
	}
	return result
}

func validObjectName(name string) bool {
	return name != "" && len(name) <= 253 && len(validation.IsDNS1123Subdomain(name)) == 0
}

func validNamespace(namespace string) bool {
	return namespace != "" && len(validation.IsDNS1123Label(namespace)) == 0
}
