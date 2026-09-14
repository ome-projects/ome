// Package autoscaleprojection projects controller-reported autoscaling status
// into the deliberately small, message-free CLI report contract.
package autoscaleprojection

import (
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var (
	ErrInferenceServiceRequired          = errors.New("autoscale projection requires an InferenceService")
	ErrInferenceServiceNameRequired      = errors.New("autoscale projection requires an InferenceService name")
	ErrInferenceServiceNamespaceRequired = errors.New("autoscale projection requires an InferenceService namespace")
	ErrInferenceServiceUIDRequired       = errors.New("autoscale projection requires an InferenceService UID")
)

var knownComponents = []struct {
	api    omev1beta1.ComponentType
	report reportv1alpha1.RuntimeComponentType
}{
	{api: omev1beta1.EngineComponent, report: reportv1alpha1.RuntimeComponentEngine},
	{api: omev1beta1.DecoderComponent, report: reportv1alpha1.RuntimeComponentDecoder},
	{api: omev1beta1.RouterComponent, report: reportv1alpha1.RuntimeComponentRouter},
}

// Project reports only evidence already mirrored onto the parent
// InferenceService. It performs no child-object reads and makes no freshness
// claim about that evidence.
func Project(
	isvc *omev1beta1.InferenceService,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.AutoscaleStatusReport, error) {
	if isvc == nil {
		return reportv1alpha1.AutoscaleStatusReport{}, ErrInferenceServiceRequired
	}
	if isvc.Name == "" {
		return reportv1alpha1.AutoscaleStatusReport{}, ErrInferenceServiceNameRequired
	}
	if isvc.Namespace == "" {
		return reportv1alpha1.AutoscaleStatusReport{}, ErrInferenceServiceNamespaceRequired
	}
	if isvc.UID == "" {
		return reportv1alpha1.AutoscaleStatusReport{}, ErrInferenceServiceUIDRequired
	}

	content := reportv1alpha1.AutoscaleStatusContent{
		Components: []reportv1alpha1.AutoscaleComponentStatus{},
		Issues:     []reportv1alpha1.AutoscaleIssue{},
	}
	unknownComponent := false
	for component := range isvc.Status.Components {
		if !isKnownComponent(component) {
			unknownComponent = true
			content.Issues = append(content.Issues, reportv1alpha1.AutoscaleIssue{
				Code: reportv1alpha1.AutoscaleIssueUnknownComponentStatus,
			})
		}
	}
	for _, component := range knownComponents {
		status, ok := isvc.Status.Components[component.api]
		if !ok {
			continue
		}
		projected, issues := projectComponent(isvc.Namespace, component.report, status)
		content.Components = append(content.Components, projected)
		content.Issues = append(content.Issues, issues...)
	}
	content.Summary.State = summarize(content.Components, unknownComponent)

	report := reportv1alpha1.NewAutoscaleStatusReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name},
		content,
		clock,
	)
	report.Sources = []reportv1alpha1.AutoscaleSourceReference{{
		Kind:        reportv1alpha1.AutoscaleSourceInferenceService,
		Namespace:   isvc.Namespace,
		Name:        isvc.Name,
		UID:         string(isvc.UID),
		Generation:  isvc.Generation,
		Evidence:    reportv1alpha1.EvidenceReported,
		CollectedAt: report.CollectedAt,
	}}
	if content.Summary.State != reportv1alpha1.AutoscaleStateReported {
		report.Warnings = []reportv1alpha1.AutoscaleWarning{{Code: reportv1alpha1.AutoscaleWarningPartialData}}
	}
	return report.Canonical(), nil
}

func projectComponent(
	namespace string,
	componentType reportv1alpha1.RuntimeComponentType,
	status omev1beta1.ComponentStatusSpec,
) (reportv1alpha1.AutoscaleComponentStatus, []reportv1alpha1.AutoscaleIssue) {
	component := reportv1alpha1.AutoscaleComponentStatus{
		Type:       componentType,
		Class:      reportv1alpha1.AutoscaleClassUnknown,
		ManagedBy:  reportv1alpha1.AutoscaleManagedByUnknown,
		SpecSource: reportv1alpha1.AutoscaleSpecSourceUnknown,
		Target:     reportv1alpha1.AutoscaleTarget{State: reportv1alpha1.AutoscaleTargetNotReported},
		Replicas:   reportv1alpha1.AutoscaleReplicaStatus{State: reportv1alpha1.AutoscaleReplicasNotReported},
		Conditions: reportv1alpha1.AutoscaleConditionsStatus{State: reportv1alpha1.AutoscaleConditionsNotReported, Items: []reportv1alpha1.AutoscaleCondition{}},
	}
	issues := []reportv1alpha1.AutoscaleIssue{}
	addIssue := func(code reportv1alpha1.AutoscaleIssueCode) {
		issues = append(issues, reportv1alpha1.AutoscaleIssue{Code: code, Component: componentType})
	}

	var targetIssue reportv1alpha1.AutoscaleIssueCode
	component.Target, targetIssue = projectTarget(namespace, status.ScaleTargetRef)
	if targetIssue != "" {
		addIssue(targetIssue)
	}

	if status.Autoscaler == nil {
		component.State = reportv1alpha1.AutoscaleComponentNotReported
		if component.Target.State == reportv1alpha1.AutoscaleTargetInvalid {
			component.State = reportv1alpha1.AutoscaleComponentInvalid
		}
		addIssue(reportv1alpha1.AutoscaleIssueAutoscalerNotReported)
		return component, issues
	}

	autoscaler := status.Autoscaler
	classOK := true
	switch autoscaler.Class {
	case omev1beta1.AutoscalerHPA:
		component.Class = reportv1alpha1.AutoscaleClassHPA
	case omev1beta1.AutoscalerKEDA:
		component.Class = reportv1alpha1.AutoscaleClassKEDA
	case omev1beta1.AutoscalerExternal:
		component.Class = reportv1alpha1.AutoscaleClassExternal
	case omev1beta1.AutoscalerNone:
		component.Class = reportv1alpha1.AutoscaleClassNone
	default:
		classOK = false
		addIssue(reportv1alpha1.AutoscaleIssueClassInvalid)
	}

	managedByOK := true
	switch autoscaler.ManagedBy {
	case omev1beta1.AutoscalerManagedByOME:
		component.ManagedBy = reportv1alpha1.AutoscaleManagedByOME
	case omev1beta1.AutoscalerManagedByExternal:
		component.ManagedBy = reportv1alpha1.AutoscaleManagedByExternal
	case omev1beta1.AutoscalerManagedByNone:
		component.ManagedBy = reportv1alpha1.AutoscaleManagedByNone
	default:
		managedByOK = false
		addIssue(reportv1alpha1.AutoscaleIssueManagedByInvalid)
	}

	specSourceOK := true
	switch autoscaler.SpecSource {
	case "isvc":
		component.SpecSource = reportv1alpha1.AutoscaleSpecSourceISVC
	case "policy":
		component.SpecSource = reportv1alpha1.AutoscaleSpecSourcePolicy
	case "runtime":
		component.SpecSource = reportv1alpha1.AutoscaleSpecSourceRuntime
	case "legacy":
		component.SpecSource = reportv1alpha1.AutoscaleSpecSourceLegacy
	case "default":
		component.SpecSource = reportv1alpha1.AutoscaleSpecSourceDefault
	default:
		specSourceOK = false
		addIssue(reportv1alpha1.AutoscaleIssueSpecSourceInvalid)
	}

	matrixOK := classOK && managedByOK && ownershipMatches(component.Class, component.ManagedBy)
	if classOK && managedByOK && !matrixOK {
		addIssue(reportv1alpha1.AutoscaleIssueOwnershipMismatch)
	}
	nonOMEClass := classOK && (component.Class == reportv1alpha1.AutoscaleClassExternal || component.Class == reportv1alpha1.AutoscaleClassNone)
	unexpectedReplicas := nonOMEClass && (autoscaler.CurrentReplicas != 0 || autoscaler.DesiredReplicas != 0 || autoscaler.LastScaleTime != nil)
	unexpectedConditions, malformedConditions := false, false
	if nonOMEClass {
		unexpectedConditions, malformedConditions = classifyNonOMEConditions(autoscaler.Conditions)
	}

	if !matrixOK {
		component.Replicas.State = reportv1alpha1.AutoscaleReplicasInvalid
		component.Conditions.State = reportv1alpha1.AutoscaleConditionsInvalid
	} else {
		switch component.ManagedBy {
		case reportv1alpha1.AutoscaleManagedByOME:
			component.Replicas, targetIssue = projectOMEReplicas(autoscaler)
			if targetIssue != "" {
				addIssue(targetIssue)
			}
			var conditionIssues []reportv1alpha1.AutoscaleIssueCode
			component.Conditions, conditionIssues = projectOMEConditions(component.Class, autoscaler.Conditions)
			for _, conditionIssue := range conditionIssues {
				addIssue(conditionIssue)
			}
		case reportv1alpha1.AutoscaleManagedByExternal, reportv1alpha1.AutoscaleManagedByNone:
			component.Replicas = reportv1alpha1.AutoscaleReplicaStatus{State: reportv1alpha1.AutoscaleReplicasUnavailable}
			component.Conditions = reportv1alpha1.AutoscaleConditionsStatus{State: reportv1alpha1.AutoscaleConditionsUnavailable, Items: []reportv1alpha1.AutoscaleCondition{}}
		}
	}
	if unexpectedReplicas {
		component.Replicas.State = reportv1alpha1.AutoscaleReplicasInvalid
		addIssue(reportv1alpha1.AutoscaleIssueUnexpectedScalerEvidence)
	}
	if malformedConditions {
		if matrixOK {
			component.Conditions = reportv1alpha1.AutoscaleConditionsStatus{
				State: reportv1alpha1.AutoscaleConditionsUnavailable,
				Items: []reportv1alpha1.AutoscaleCondition{},
			}
		}
		addIssue(reportv1alpha1.AutoscaleIssueConditionInvalid)
	}
	if unexpectedConditions {
		component.Conditions.State = reportv1alpha1.AutoscaleConditionsInvalid
		addIssue(reportv1alpha1.AutoscaleIssueUnexpectedScalerEvidence)
	}

	component.State = summarizeComponent(component, classOK && managedByOK && specSourceOK && matrixOK)
	if malformedConditions && component.State == reportv1alpha1.AutoscaleComponentReported {
		component.State = reportv1alpha1.AutoscaleComponentPartial
	}
	return component, issues
}

func classifyNonOMEConditions(conditions []metav1.Condition) (unexpected, malformed bool) {
	for _, condition := range conditions {
		if len(metav1validation.ValidateCondition(condition, field.NewPath("condition"))) != 0 {
			malformed = true
			continue
		}
		// AutoscalerResolved is policy-selection provenance written for every
		// class. It is not backend scaler evidence and its reason/message are
		// deliberately neither interpreted nor emitted.
		if condition.Type == omev1beta1.AutoscalerResolvedCondition {
			continue
		}
		unexpected = true
	}
	return unexpected, malformed
}

func projectTarget(
	namespace string,
	target *omev1beta1.ScaleTargetRef,
) (reportv1alpha1.AutoscaleTarget, reportv1alpha1.AutoscaleIssueCode) {
	if target == nil {
		return reportv1alpha1.AutoscaleTarget{State: reportv1alpha1.AutoscaleTargetNotReported}, reportv1alpha1.AutoscaleIssueScaleTargetNotReported
	}
	if target.Name == "" || len(validation.IsDNS1123Subdomain(target.Name)) != 0 {
		return reportv1alpha1.AutoscaleTarget{State: reportv1alpha1.AutoscaleTargetInvalid}, reportv1alpha1.AutoscaleIssueScaleTargetInvalid
	}

	projected := reportv1alpha1.AutoscaleTarget{
		State: reportv1alpha1.AutoscaleTargetReported, Namespace: namespace, Name: target.Name,
	}
	switch {
	case target.APIVersion == "apps/v1" && target.Kind == "Deployment":
		projected.APIVersion = "apps/v1"
		projected.Kind = reportv1alpha1.AutoscaleTargetDeployment
	case target.APIVersion == "ome.io/v1beta1" && target.Kind == "InferenceReplica":
		projected.APIVersion = "ome.io/v1beta1"
		projected.Kind = reportv1alpha1.AutoscaleTargetInferenceReplica
	default:
		return reportv1alpha1.AutoscaleTarget{State: reportv1alpha1.AutoscaleTargetInvalid}, reportv1alpha1.AutoscaleIssueScaleTargetInvalid
	}
	return projected, ""
}

func projectOMEReplicas(status *omev1beta1.ComponentAutoscalerStatus) (reportv1alpha1.AutoscaleReplicaStatus, reportv1alpha1.AutoscaleIssueCode) {
	if status.CurrentReplicas < 0 || status.DesiredReplicas < 0 || (status.LastScaleTime != nil && status.LastScaleTime.IsZero()) {
		return reportv1alpha1.AutoscaleReplicaStatus{State: reportv1alpha1.AutoscaleReplicasInvalid}, reportv1alpha1.AutoscaleIssueReplicaEvidenceInvalid
	}
	current := status.CurrentReplicas
	desired := status.DesiredReplicas
	projected := reportv1alpha1.AutoscaleReplicaStatus{
		State: reportv1alpha1.AutoscaleReplicasReported, CurrentReplicas: &current, DesiredReplicas: &desired,
	}
	if status.LastScaleTime != nil {
		lastScaleTime := status.LastScaleTime.Time.UTC()
		projected.LastScaleTime = &lastScaleTime
	}
	if current == 0 || desired == 0 {
		projected.State = reportv1alpha1.AutoscaleReplicasAmbiguous
		return projected, reportv1alpha1.AutoscaleIssueReplicaEvidenceAmbiguous
	}
	return projected, ""
}

func projectOMEConditions(
	class reportv1alpha1.AutoscaleClass,
	conditions []metav1.Condition,
) (reportv1alpha1.AutoscaleConditionsStatus, []reportv1alpha1.AutoscaleIssueCode) {
	if len(conditions) == 0 {
		return reportv1alpha1.AutoscaleConditionsStatus{
			State: reportv1alpha1.AutoscaleConditionsNotReported,
			Items: []reportv1alpha1.AutoscaleCondition{},
		}, nil
	}

	byType := map[reportv1alpha1.AutoscaleConditionType][]reportv1alpha1.AutoscaleCondition{}
	invalid := false
	for _, condition := range conditions {
		if len(metav1validation.ValidateCondition(condition, field.NewPath("condition"))) != 0 {
			invalid = true
			continue
		}
		// AutoscalerResolved is first-party policy provenance, not backend
		// scaler health. Its free-form reason/message stay out of this report;
		// structurally valid instances neither contaminate nor satisfy the
		// backend condition evidence set.
		if condition.Type == omev1beta1.AutoscalerResolvedCondition {
			continue
		}
		typeValue, ok := conditionType(class, condition.Type)
		if !ok {
			invalid = true
			continue
		}
		statusValue, ok := conditionStatus(condition.Status)
		if !ok {
			invalid = true
			continue
		}
		projected := reportv1alpha1.AutoscaleCondition{
			Type: typeValue, Status: statusValue, LastTransitionTime: condition.LastTransitionTime.Time.UTC(),
		}
		if !containsCondition(byType[typeValue], projected) {
			byType[typeValue] = append(byType[typeValue], projected)
		}
	}

	items := []reportv1alpha1.AutoscaleCondition{}
	conflict := false
	for _, conditionType := range conditionOrder(class) {
		values := byType[conditionType]
		switch len(values) {
		case 0:
			continue
		case 1:
			items = append(items, values[0])
		default:
			conflict = true
		}
	}
	state := reportv1alpha1.AutoscaleConditionsReported
	issues := []reportv1alpha1.AutoscaleIssueCode{}
	if len(items) == 0 && !conflict && !invalid {
		state = reportv1alpha1.AutoscaleConditionsNotReported
	}
	if conflict {
		state = reportv1alpha1.AutoscaleConditionsUnavailable
		issues = append(issues, reportv1alpha1.AutoscaleIssueConditionConflict)
	}
	if invalid {
		state = reportv1alpha1.AutoscaleConditionsUnavailable
		issues = append(issues, reportv1alpha1.AutoscaleIssueConditionInvalid)
	}
	return reportv1alpha1.AutoscaleConditionsStatus{State: state, Items: items}, issues
}

func conditionType(class reportv1alpha1.AutoscaleClass, value string) (reportv1alpha1.AutoscaleConditionType, bool) {
	if class == reportv1alpha1.AutoscaleClassHPA {
		switch value {
		case "AbleToScale":
			return reportv1alpha1.AutoscaleConditionAbleToScale, true
		case "ScalingActive":
			return reportv1alpha1.AutoscaleConditionScalingActive, true
		case "ScalingLimited":
			return reportv1alpha1.AutoscaleConditionScalingLimited, true
		}
	}
	if class == reportv1alpha1.AutoscaleClassKEDA {
		switch value {
		case "Ready":
			return reportv1alpha1.AutoscaleConditionReady, true
		case "Active":
			return reportv1alpha1.AutoscaleConditionActive, true
		case "Fallback":
			return reportv1alpha1.AutoscaleConditionFallback, true
		case "Paused":
			return reportv1alpha1.AutoscaleConditionPaused, true
		}
	}
	return "", false
}

func conditionStatus(value metav1.ConditionStatus) (reportv1alpha1.AutoscaleConditionStatus, bool) {
	switch value {
	case metav1.ConditionTrue:
		return reportv1alpha1.AutoscaleConditionTrue, true
	case metav1.ConditionFalse:
		return reportv1alpha1.AutoscaleConditionFalse, true
	case metav1.ConditionUnknown:
		return reportv1alpha1.AutoscaleConditionUnknown, true
	default:
		return "", false
	}
}

func conditionOrder(class reportv1alpha1.AutoscaleClass) []reportv1alpha1.AutoscaleConditionType {
	if class == reportv1alpha1.AutoscaleClassHPA {
		return []reportv1alpha1.AutoscaleConditionType{
			reportv1alpha1.AutoscaleConditionAbleToScale,
			reportv1alpha1.AutoscaleConditionScalingActive,
			reportv1alpha1.AutoscaleConditionScalingLimited,
		}
	}
	return []reportv1alpha1.AutoscaleConditionType{
		reportv1alpha1.AutoscaleConditionReady,
		reportv1alpha1.AutoscaleConditionActive,
		reportv1alpha1.AutoscaleConditionFallback,
		reportv1alpha1.AutoscaleConditionPaused,
	}
}

func containsCondition(values []reportv1alpha1.AutoscaleCondition, candidate reportv1alpha1.AutoscaleCondition) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func ownershipMatches(class reportv1alpha1.AutoscaleClass, managedBy reportv1alpha1.AutoscaleManagedBy) bool {
	switch class {
	case reportv1alpha1.AutoscaleClassHPA, reportv1alpha1.AutoscaleClassKEDA:
		return managedBy == reportv1alpha1.AutoscaleManagedByOME
	case reportv1alpha1.AutoscaleClassExternal:
		return managedBy == reportv1alpha1.AutoscaleManagedByExternal
	case reportv1alpha1.AutoscaleClassNone:
		return managedBy == reportv1alpha1.AutoscaleManagedByNone
	default:
		return false
	}
}

func summarizeComponent(component reportv1alpha1.AutoscaleComponentStatus, enumsValid bool) reportv1alpha1.AutoscaleComponentState {
	if !enumsValid ||
		component.Target.State == reportv1alpha1.AutoscaleTargetInvalid ||
		component.Replicas.State == reportv1alpha1.AutoscaleReplicasInvalid ||
		component.Conditions.State == reportv1alpha1.AutoscaleConditionsInvalid {
		return reportv1alpha1.AutoscaleComponentInvalid
	}
	if component.Target.State == reportv1alpha1.AutoscaleTargetNotReported ||
		component.Replicas.State == reportv1alpha1.AutoscaleReplicasAmbiguous ||
		component.Replicas.State == reportv1alpha1.AutoscaleReplicasNotReported ||
		component.Conditions.State == reportv1alpha1.AutoscaleConditionsNotReported ||
		(component.ManagedBy == reportv1alpha1.AutoscaleManagedByOME &&
			component.Conditions.State == reportv1alpha1.AutoscaleConditionsUnavailable) {
		return reportv1alpha1.AutoscaleComponentPartial
	}
	return reportv1alpha1.AutoscaleComponentReported
}

func summarize(components []reportv1alpha1.AutoscaleComponentStatus, unknownComponent bool) reportv1alpha1.AutoscaleState {
	if unknownComponent {
		return reportv1alpha1.AutoscaleStateInvalid
	}
	if len(components) == 0 {
		return reportv1alpha1.AutoscaleStateUnavailable
	}
	allNotReported := true
	partial := false
	for _, component := range components {
		switch component.State {
		case reportv1alpha1.AutoscaleComponentInvalid:
			return reportv1alpha1.AutoscaleStateInvalid
		case reportv1alpha1.AutoscaleComponentReported:
			allNotReported = false
		case reportv1alpha1.AutoscaleComponentPartial:
			allNotReported = false
			partial = true
		case reportv1alpha1.AutoscaleComponentNotReported:
			partial = true
		}
	}
	if allNotReported {
		return reportv1alpha1.AutoscaleStateUnavailable
	}
	if partial {
		return reportv1alpha1.AutoscaleStatePartial
	}
	return reportv1alpha1.AutoscaleStateReported
}

func isKnownComponent(component omev1beta1.ComponentType) bool {
	for _, known := range knownComponents {
		if component == known.api {
			return true
		}
	}
	return false
}
