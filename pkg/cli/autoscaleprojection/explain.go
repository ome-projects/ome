package autoscaleprojection

import (
	"errors"
	"fmt"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// ErrExplainProjectionInvalid rejects an internal enum or relationship that
// cannot be represented by the closed autoscale-explain report contract.
var ErrExplainProjectionInvalid = errors.New("autoscale explain projection input is invalid")

// ProjectExplain joins pin-aware desired autoscaling with the existing
// parent-status projection. It does not read child scaler objects and never
// treats a reported mismatch as proof of causal drift.
func ProjectExplain(
	isvc *v1beta1.InferenceService,
	state *effective.RuntimeState,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.AutoscaleExplainReport, error) {
	resolution, err := effective.ResolveAutoscaling(isvc, state)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainReport{}, fmt.Errorf("resolve effective autoscaling: %w", err)
	}
	return projectResolvedExplain(isvc, resolution, clock)
}

// ProjectVirtualExplain projects the controller's service-level
// VirtualDeployment early exit without requiring runtime evidence.
func ProjectVirtualExplain(
	isvc *v1beta1.InferenceService,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.AutoscaleExplainReport, error) {
	resolution, err := effective.ResolveVirtualAutoscaling(isvc)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainReport{}, fmt.Errorf("resolve virtual autoscaling: %w", err)
	}
	return projectResolvedExplain(isvc, resolution, clock)
}

func projectResolvedExplain(
	isvc *v1beta1.InferenceService,
	resolution effective.AutoscalingResolution,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.AutoscaleExplainReport, error) {
	statusReport, err := Project(isvc, clock)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainReport{}, fmt.Errorf("project reported autoscaling: %w", err)
	}

	freshness, err := mapExplainFreshness(resolution.StatusFreshness)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainReport{}, err
	}
	active, err := projectAutoscaleActive(resolution.Active)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainReport{}, err
	}
	policy, err := projectScalingPolicy(resolution.ScalingPolicy)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainReport{}, err
	}

	content := reportv1alpha1.AutoscaleExplainContent{
		Summary:             reportv1alpha1.AutoscaleExplainSummary{StatusFreshness: freshness},
		ActiveConfiguration: active,
		ScalingPolicy:       policy,
		Components:          []reportv1alpha1.AutoscaleExplainComponent{},
		Issues:              []reportv1alpha1.AutoscaleExplainIssue{},
	}
	flags := explainSummaryFlags{}
	globalIssues, err := projectEffectiveIssues(resolution.Issues)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainReport{}, err
	}
	if resolution.StatusFreshness == effective.StatusFreshnessCurrent &&
		statusReport.Content.Summary.State == reportv1alpha1.AutoscaleStateInvalid {
		globalIssues = append(globalIssues, reportv1alpha1.AutoscaleExplainIssueStatusInvalid)
	}
	for _, issue := range globalIssues {
		content.Issues = append(content.Issues, reportv1alpha1.AutoscaleExplainIssue{Code: issue})
	}
	classifyGlobalExplainEvidence(resolution, statusReport, &flags)

	desiredComponents := map[reportv1alpha1.RuntimeComponentType]struct{}{}
	for _, desired := range resolution.Components {
		component, err := projectExplainComponent(desired, statusReport, resolution.StatusFreshness, globalIssues)
		if err != nil {
			return reportv1alpha1.AutoscaleExplainReport{}, err
		}
		content.Components = append(content.Components, component)
		desiredComponents[component.Type] = struct{}{}
		for _, issue := range component.Reconciliation.Issues {
			content.Issues = append(content.Issues, reportv1alpha1.AutoscaleExplainIssue{Code: issue, Component: component.Type})
		}
		classifyExplainComponent(component, &flags)
	}
	if resolution.StatusFreshness == effective.StatusFreshnessCurrent {
		for _, reported := range statusReport.Content.Components {
			if _, found := desiredComponents[reported.Type]; found {
				continue
			}
			content.Issues = append(content.Issues, reportv1alpha1.AutoscaleExplainIssue{
				Code:      reportv1alpha1.AutoscaleExplainIssueReportedComponentUnexpected,
				Component: reported.Type,
			})
			flags.mismatch = true
		}
	}
	content.Summary.State = flags.state()
	reportValue := reportv1alpha1.NewAutoscaleExplainReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name}, content, clock,
	)
	reportValue.Sources = projectAutoscaleExplainSources(isvc, resolution, reportValue.CollectedAt)
	if flags.partial {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.AutoscaleExplainWarning{Code: reportv1alpha1.AutoscaleExplainWarningPartialData})
	}
	if resolution.StatusFreshness == effective.StatusFreshnessStale {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.AutoscaleExplainWarning{Code: reportv1alpha1.AutoscaleExplainWarningStaleEvidence})
	}
	if flags.unsupported {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.AutoscaleExplainWarning{Code: reportv1alpha1.AutoscaleExplainWarningUnsupportedConfiguration})
	}
	return reportValue.Canonical(), nil
}

type explainSummaryFlags struct {
	invalid     bool
	unsupported bool
	mismatch    bool
	partial     bool
}

func (f explainSummaryFlags) state() reportv1alpha1.AutoscaleExplainState {
	switch {
	case f.invalid:
		return reportv1alpha1.AutoscaleExplainInvalid
	case f.unsupported:
		return reportv1alpha1.AutoscaleExplainUnsupported
	case f.mismatch:
		return reportv1alpha1.AutoscaleExplainReportedMismatch
	case f.partial:
		return reportv1alpha1.AutoscaleExplainPartial
	default:
		return reportv1alpha1.AutoscaleExplainConsistent
	}
}

func classifyGlobalExplainEvidence(
	resolution effective.AutoscalingResolution,
	status reportv1alpha1.AutoscaleStatusReport,
	flags *explainSummaryFlags,
) {
	switch resolution.ScalingPolicy.State {
	case effective.ScalingPolicyUnsupported:
		flags.unsupported = true
	case effective.ScalingPolicyInvalid:
		flags.invalid = true
	}
	if resolution.Active.State == effective.AutoscalingActiveConfigurationAvailable &&
		(resolution.Active.Inheritance.State == effective.InheritanceUnavailable ||
			(resolution.Active.Origin == effective.ConfigurationOriginControllerRevision &&
				resolution.Active.Consistency != effective.RevisionConsistencyConsistent)) {
		flags.partial = true
	}
	switch resolution.StatusFreshness {
	case effective.StatusFreshnessStale, effective.StatusFreshnessUnknown:
		flags.partial = true
	case effective.StatusFreshnessInconsistent:
		flags.invalid = true
	}
	if resolution.StatusFreshness == effective.StatusFreshnessCurrent {
		switch status.Content.Summary.State {
		case reportv1alpha1.AutoscaleStateInvalid:
			flags.invalid = true
		case reportv1alpha1.AutoscaleStatePartial, reportv1alpha1.AutoscaleStateUnavailable:
			flags.partial = true
		}
	}
}

func classifyExplainComponent(component reportv1alpha1.AutoscaleExplainComponent, flags *explainSummaryFlags) {
	switch component.Desired.State {
	case reportv1alpha1.AutoscaleDesiredInvalid:
		flags.invalid = true
	case reportv1alpha1.AutoscaleDesiredUnsupported:
		flags.unsupported = true
	case reportv1alpha1.AutoscaleDesiredUnavailable:
		flags.partial = true
	}
	switch component.Desired.ScaleToZero {
	case reportv1alpha1.AutoscaleScaleToZeroInvalid:
		flags.invalid = true
	case reportv1alpha1.AutoscaleScaleToZeroUnsupported:
		flags.unsupported = true
	case reportv1alpha1.AutoscaleScaleToZeroUnavailable:
		flags.partial = true
	}
	switch component.Reported.State {
	case reportv1alpha1.AutoscaleReportedInvalid:
		flags.invalid = true
	case reportv1alpha1.AutoscaleReportedNotReported, reportv1alpha1.AutoscaleReportedUnavailable:
		flags.partial = true
	}
	switch component.Reconciliation.State {
	case reportv1alpha1.AutoscaleReconciliationInvalid:
		flags.invalid = true
	case reportv1alpha1.AutoscaleReconciliationReportedMismatch:
		flags.mismatch = true
	case reportv1alpha1.AutoscaleReconciliationNotReported, reportv1alpha1.AutoscaleReconciliationUnavailable:
		flags.partial = true
	}
	for _, issue := range component.Reconciliation.Issues {
		if issue == reportv1alpha1.AutoscaleExplainIssueReportedEvidencePartial {
			flags.partial = true
		}
	}
}

func projectExplainComponent(
	desired effective.EffectiveAutoscalingComponent,
	statusReport reportv1alpha1.AutoscaleStatusReport,
	freshness effective.StatusFreshness,
	globalIssues []reportv1alpha1.AutoscaleExplainIssueCode,
) (reportv1alpha1.AutoscaleExplainComponent, error) {
	componentType, err := mapExplainComponentType(desired.Type)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainComponent{}, err
	}
	mode, err := mapExplainDeploymentMode(desired.DeploymentMode)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainComponent{}, err
	}
	modeSource, err := mapExplainDeploymentModeSource(desired.DeploymentModeSource)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainComponent{}, err
	}
	projectedDesired, desiredIssues, err := projectDesiredAutoscaling(desired)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainComponent{}, err
	}
	reported, reportedIssues, err := projectReportedAutoscaling(componentType, statusReport, freshness)
	if err != nil {
		return reportv1alpha1.AutoscaleExplainComponent{}, err
	}
	reconciliation := reconcileAutoscaling(projectedDesired, reported)
	reconciliation.Issues = append(reconciliation.Issues, globalIssues...)
	reconciliation.Issues = append(reconciliation.Issues, desiredIssues...)
	reconciliation.Issues = append(reconciliation.Issues, reportedIssues...)
	return reportv1alpha1.AutoscaleExplainComponent{
		Type: componentType, DeploymentMode: mode, DeploymentModeSource: modeSource,
		Desired: projectedDesired, Reported: reported, Reconciliation: reconciliation,
	}, nil
}

func projectDesiredAutoscaling(
	value effective.EffectiveAutoscalingComponent,
) (reportv1alpha1.AutoscaleDesiredConfiguration, []reportv1alpha1.AutoscaleExplainIssueCode, error) {
	state, err := mapDesiredState(value.State)
	if err != nil {
		return reportv1alpha1.AutoscaleDesiredConfiguration{}, nil, err
	}
	class, err := mapDesiredClass(value.Class, value.State)
	if err != nil {
		return reportv1alpha1.AutoscaleDesiredConfiguration{}, nil, err
	}
	managedBy, err := mapDesiredManagedBy(value.ManagedBy, value.State)
	if err != nil {
		return reportv1alpha1.AutoscaleDesiredConfiguration{}, nil, err
	}
	source, err := mapDesiredSource(value.SpecSource, value.State)
	if err != nil {
		return reportv1alpha1.AutoscaleDesiredConfiguration{}, nil, err
	}
	bounds, err := mapDesiredBounds(value.Bounds)
	if err != nil {
		return reportv1alpha1.AutoscaleDesiredConfiguration{}, nil, err
	}
	zero, err := mapScaleToZero(value.ScaleToZero)
	if err != nil {
		return reportv1alpha1.AutoscaleDesiredConfiguration{}, nil, err
	}
	target, err := mapDesiredTarget(value.Target)
	if err != nil {
		return reportv1alpha1.AutoscaleDesiredConfiguration{}, nil, err
	}
	if state != reportv1alpha1.AutoscaleDesiredAvailable {
		target = nil
	}
	issues, err := projectEffectiveIssues(value.Issues)
	if err != nil {
		return reportv1alpha1.AutoscaleDesiredConfiguration{}, nil, err
	}
	var metricCount, triggerCount *int
	if value.SpecSource != effective.AutoscalingSpecSourcePolicy && value.SpecSource != "" {
		metricCount = copyExplainInt(value.MetricCount)
		triggerCount = copyExplainInt(value.TriggerCount)
	}
	return reportv1alpha1.AutoscaleDesiredConfiguration{
		State: state, Class: class, ManagedBy: managedBy, SpecSource: source,
		Target: target, Bounds: bounds, ScaleToZero: zero,
		MetricCount: metricCount, TriggerCount: triggerCount,
	}, issues, nil
}

func projectReportedAutoscaling(
	component reportv1alpha1.RuntimeComponentType,
	statusReport reportv1alpha1.AutoscaleStatusReport,
	freshness effective.StatusFreshness,
) (reportv1alpha1.AutoscaleReportedConfiguration, []reportv1alpha1.AutoscaleExplainIssueCode, error) {
	if freshness == effective.StatusFreshnessStale || freshness == effective.StatusFreshnessUnknown {
		issue := reportv1alpha1.AutoscaleExplainIssueStatusStale
		if freshness == effective.StatusFreshnessUnknown {
			issue = reportv1alpha1.AutoscaleExplainIssueStatusUnobserved
		}
		return unavailableReportedAutoscaling(), []reportv1alpha1.AutoscaleExplainIssueCode{issue}, nil
	}
	if freshness == effective.StatusFreshnessInconsistent {
		result := unavailableReportedAutoscaling()
		result.State = reportv1alpha1.AutoscaleReportedInvalid
		result.TargetState = reportv1alpha1.AutoscaleTargetInvalid
		result.Replicas.State = reportv1alpha1.AutoscaleReplicasInvalid
		result.Conditions.State = reportv1alpha1.AutoscaleConditionsInvalid
		return result, []reportv1alpha1.AutoscaleExplainIssueCode{reportv1alpha1.AutoscaleExplainIssueStatusInvalid}, nil
	}
	if freshness != effective.StatusFreshnessCurrent {
		return reportv1alpha1.AutoscaleReportedConfiguration{}, nil, ErrExplainProjectionInvalid
	}

	var status *reportv1alpha1.AutoscaleComponentStatus
	for i := range statusReport.Content.Components {
		if statusReport.Content.Components[i].Type == component {
			status = &statusReport.Content.Components[i]
			break
		}
	}
	if status == nil {
		return notReportedAutoscaling(), []reportv1alpha1.AutoscaleExplainIssueCode{reportv1alpha1.AutoscaleExplainIssueStatusNotReported}, nil
	}
	result := reportv1alpha1.AutoscaleReportedConfiguration{
		Class: status.Class, ManagedBy: status.ManagedBy, SpecSource: status.SpecSource,
		TargetState: status.Target.State, Replicas: status.Replicas, Conditions: status.Conditions,
	}
	if status.Target.State == reportv1alpha1.AutoscaleTargetReported {
		result.Target = &reportv1alpha1.AutoscaleTargetIdentity{
			APIVersion: status.Target.APIVersion, Kind: status.Target.Kind,
			Namespace: status.Target.Namespace, Name: status.Target.Name,
		}
	}
	switch status.State {
	case reportv1alpha1.AutoscaleComponentReported, reportv1alpha1.AutoscaleComponentPartial:
		result.State = reportv1alpha1.AutoscaleReportedAvailable
	case reportv1alpha1.AutoscaleComponentNotReported:
		result.State = reportv1alpha1.AutoscaleReportedNotReported
	case reportv1alpha1.AutoscaleComponentInvalid:
		result.State = reportv1alpha1.AutoscaleReportedInvalid
	default:
		return reportv1alpha1.AutoscaleReportedConfiguration{}, nil, ErrExplainProjectionInvalid
	}
	issues := []reportv1alpha1.AutoscaleExplainIssueCode{}
	if status.State == reportv1alpha1.AutoscaleComponentPartial {
		issues = append(issues, reportv1alpha1.AutoscaleExplainIssueReportedEvidencePartial)
	}
	if status.State == reportv1alpha1.AutoscaleComponentInvalid {
		issues = append(issues, reportv1alpha1.AutoscaleExplainIssueReportedEvidenceInvalid)
	}
	if status.State == reportv1alpha1.AutoscaleComponentNotReported || status.Target.State == reportv1alpha1.AutoscaleTargetNotReported {
		issues = append(issues, reportv1alpha1.AutoscaleExplainIssueStatusNotReported)
	}
	return result, issues, nil
}

func unavailableReportedAutoscaling() reportv1alpha1.AutoscaleReportedConfiguration {
	return reportv1alpha1.AutoscaleReportedConfiguration{
		State: reportv1alpha1.AutoscaleReportedUnavailable,
		Class: reportv1alpha1.AutoscaleClassUnknown, ManagedBy: reportv1alpha1.AutoscaleManagedByUnknown,
		SpecSource:  reportv1alpha1.AutoscaleSpecSourceUnknown,
		TargetState: reportv1alpha1.AutoscaleTargetUnavailable,
		Replicas:    reportv1alpha1.AutoscaleReplicaStatus{State: reportv1alpha1.AutoscaleReplicasUnavailable},
		Conditions:  reportv1alpha1.AutoscaleConditionsStatus{State: reportv1alpha1.AutoscaleConditionsUnavailable, Items: []reportv1alpha1.AutoscaleCondition{}},
	}
}

func notReportedAutoscaling() reportv1alpha1.AutoscaleReportedConfiguration {
	return reportv1alpha1.AutoscaleReportedConfiguration{
		State: reportv1alpha1.AutoscaleReportedNotReported,
		Class: reportv1alpha1.AutoscaleClassUnknown, ManagedBy: reportv1alpha1.AutoscaleManagedByUnknown,
		SpecSource:  reportv1alpha1.AutoscaleSpecSourceUnknown,
		TargetState: reportv1alpha1.AutoscaleTargetNotReported,
		Replicas:    reportv1alpha1.AutoscaleReplicaStatus{State: reportv1alpha1.AutoscaleReplicasNotReported},
		Conditions:  reportv1alpha1.AutoscaleConditionsStatus{State: reportv1alpha1.AutoscaleConditionsNotReported, Items: []reportv1alpha1.AutoscaleCondition{}},
	}
}

func reconcileAutoscaling(
	desired reportv1alpha1.AutoscaleDesiredConfiguration,
	reported reportv1alpha1.AutoscaleReportedConfiguration,
) reportv1alpha1.AutoscaleReconciliation {
	result := reportv1alpha1.AutoscaleReconciliation{Issues: []reportv1alpha1.AutoscaleExplainIssueCode{}}
	switch desired.State {
	case reportv1alpha1.AutoscaleDesiredInvalid:
		result.State = reportv1alpha1.AutoscaleReconciliationInvalid
		return result
	case reportv1alpha1.AutoscaleDesiredUnavailable, reportv1alpha1.AutoscaleDesiredUnsupported:
		result.State = reportv1alpha1.AutoscaleReconciliationUnavailable
		return result
	case reportv1alpha1.AutoscaleDesiredAvailable:
	default:
		result.State = reportv1alpha1.AutoscaleReconciliationInvalid
		return result
	}
	switch reported.State {
	case reportv1alpha1.AutoscaleReportedInvalid:
		result.State = reportv1alpha1.AutoscaleReconciliationInvalid
		return result
	case reportv1alpha1.AutoscaleReportedUnavailable:
		result.State = reportv1alpha1.AutoscaleReconciliationUnavailable
		return result
	case reportv1alpha1.AutoscaleReportedNotReported:
		result.State = reportv1alpha1.AutoscaleReconciliationNotReported
		return result
	case reportv1alpha1.AutoscaleReportedAvailable:
	default:
		result.State = reportv1alpha1.AutoscaleReconciliationInvalid
		return result
	}
	if desired.Class != reported.Class {
		result.Issues = append(result.Issues, reportv1alpha1.AutoscaleExplainIssueReportedClassMismatch)
	}
	if desired.ManagedBy != reported.ManagedBy {
		result.Issues = append(result.Issues, reportv1alpha1.AutoscaleExplainIssueReportedOwnershipMismatch)
	}
	if desired.SpecSource != reported.SpecSource {
		result.Issues = append(result.Issues, reportv1alpha1.AutoscaleExplainIssueReportedSpecSourceMismatch)
	}
	if reported.TargetState != reportv1alpha1.AutoscaleTargetReported || reported.Target == nil {
		if len(result.Issues) > 0 {
			result.State = reportv1alpha1.AutoscaleReconciliationReportedMismatch
		} else {
			result.State = reportv1alpha1.AutoscaleReconciliationNotReported
		}
		return result
	}
	if desired.Target == nil || *desired.Target != *reported.Target {
		result.Issues = append(result.Issues, reportv1alpha1.AutoscaleExplainIssueReportedTargetMismatch)
	}
	if len(result.Issues) > 0 {
		result.State = reportv1alpha1.AutoscaleReconciliationReportedMismatch
	} else {
		result.State = reportv1alpha1.AutoscaleReconciliationConsistent
	}
	return result
}

func projectAutoscaleActive(value effective.AutoscalingActiveConfiguration) (reportv1alpha1.AutoscaleActiveConfiguration, error) {
	if value.State == effective.AutoscalingActiveConfigurationUnavailable {
		return reportv1alpha1.AutoscaleActiveConfiguration{
			State: reportv1alpha1.AutoscaleActiveConfigurationUnavailable,
		}, nil
	}
	if value.State != effective.AutoscalingActiveConfigurationAvailable {
		return reportv1alpha1.AutoscaleActiveConfiguration{}, ErrExplainProjectionInvalid
	}
	origin, err := mapConfigurationOrigin(value.Origin)
	if err != nil {
		return reportv1alpha1.AutoscaleActiveConfiguration{}, err
	}
	consistency, err := mapRevisionConsistency(value.Consistency)
	if err != nil {
		return reportv1alpha1.AutoscaleActiveConfiguration{}, err
	}
	runtimeValue := value.Runtime
	if !runtimeValue.IdentityObserved {
		runtimeValue.UID = ""
		runtimeValue.Generation = 0
	}
	runtimeReference, err := mapAutoscalingRuntimeReference(runtimeValue)
	if err != nil {
		return reportv1alpha1.AutoscaleActiveConfiguration{}, err
	}
	inheritance, err := mapAutoscalingInheritance(value.Inheritance)
	if err != nil {
		return reportv1alpha1.AutoscaleActiveConfiguration{}, err
	}
	result := reportv1alpha1.AutoscaleActiveConfiguration{
		State:  reportv1alpha1.AutoscaleActiveConfigurationAvailable,
		Origin: origin, Consistency: consistency, Runtime: &runtimeReference, Inheritance: &inheritance,
	}
	if value.Revision != nil {
		role, err := mapRevisionRole(value.Revision.Role)
		if err != nil || role != reportv1alpha1.RuntimeRevisionRoleActive || value.Revision.Name == "" || value.Revision.Namespace == "" {
			return reportv1alpha1.AutoscaleActiveConfiguration{}, ErrExplainProjectionInvalid
		}
		result.Revision = &reportv1alpha1.AutoscaleActiveRevision{
			Namespace: value.Revision.Namespace, Name: value.Revision.Name, UID: value.Revision.UID, Role: role,
		}
	}
	if origin == reportv1alpha1.ConfigurationOriginControllerRevision && result.Revision == nil {
		return reportv1alpha1.AutoscaleActiveConfiguration{}, ErrExplainProjectionInvalid
	}
	if origin == reportv1alpha1.ConfigurationOriginLiveRuntime && result.Revision != nil {
		return reportv1alpha1.AutoscaleActiveConfiguration{}, ErrExplainProjectionInvalid
	}
	return result, nil
}

func projectScalingPolicy(value effective.EffectiveScalingPolicy) (reportv1alpha1.AutoscaleScalingPolicy, error) {
	result := reportv1alpha1.AutoscaleScalingPolicy{}
	switch value.State {
	case effective.ScalingPolicyAvailable:
		result.State = reportv1alpha1.AutoscaleScalingPolicyAvailable
	case effective.ScalingPolicyUnsupported:
		result.State = reportv1alpha1.AutoscaleScalingPolicyUnsupported
	case effective.ScalingPolicyInvalid:
		result.State = reportv1alpha1.AutoscaleScalingPolicyInvalid
	default:
		return result, ErrExplainProjectionInvalid
	}
	switch value.Source {
	case effective.ScalingPolicySourceISVC:
		result.Source = reportv1alpha1.AutoscaleScalingPolicySourceISVC
	case effective.ScalingPolicySourceRuntime:
		result.Source = reportv1alpha1.AutoscaleScalingPolicySourceRuntime
	case effective.ScalingPolicySourceDefault:
		result.Source = reportv1alpha1.AutoscaleScalingPolicySourceDefault
	default:
		return result, ErrExplainProjectionInvalid
	}
	switch value.Mode {
	case v1beta1.ScalingIndependent:
		result.Mode = reportv1alpha1.AutoscaleScalingIndependent
	case v1beta1.ScalingProportional:
		result.Mode = reportv1alpha1.AutoscaleScalingProportional
	case v1beta1.ScalingPinned:
		result.Mode = reportv1alpha1.AutoscaleScalingPinned
	default:
		if value.State != effective.ScalingPolicyInvalid {
			return result, ErrExplainProjectionInvalid
		}
		// Unknown raw values cannot be represented in the stable output. The
		// state and fixed issue preserve the invalidity without echoing input.
		result.Mode = reportv1alpha1.AutoscaleScalingUnknown
	}
	return result, nil
}

func mapExplainFreshness(value effective.StatusFreshness) (reportv1alpha1.StatusFreshness, error) {
	switch value {
	case effective.StatusFreshnessCurrent:
		return reportv1alpha1.StatusFreshnessCurrent, nil
	case effective.StatusFreshnessStale:
		return reportv1alpha1.StatusFreshnessStale, nil
	case effective.StatusFreshnessUnknown:
		return reportv1alpha1.StatusFreshnessUnobserved, nil
	case effective.StatusFreshnessInconsistent:
		return reportv1alpha1.StatusFreshnessInvalid, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}

func mapExplainComponentType(value v1beta1.ComponentType) (reportv1alpha1.RuntimeComponentType, error) {
	switch value {
	case v1beta1.EngineComponent:
		return reportv1alpha1.RuntimeComponentEngine, nil
	case v1beta1.DecoderComponent:
		return reportv1alpha1.RuntimeComponentDecoder, nil
	case v1beta1.RouterComponent:
		return reportv1alpha1.RuntimeComponentRouter, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}

func mapExplainDeploymentMode(value constants.DeploymentModeType) (reportv1alpha1.DeploymentMode, error) {
	switch value {
	case constants.RawDeployment:
		return reportv1alpha1.DeploymentModeRawDeployment, nil
	case constants.OMENative:
		return reportv1alpha1.DeploymentModeOMENative, nil
	case constants.MultiNode:
		return reportv1alpha1.DeploymentModeMultiNode, nil
	case constants.VirtualDeployment:
		return reportv1alpha1.DeploymentModeVirtualDeployment, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}

func mapExplainDeploymentModeSource(value effective.ComponentDeploymentModeSource) (reportv1alpha1.DeploymentModeSource, error) {
	switch value {
	case effective.DeploymentModeComponentAnnotation:
		return reportv1alpha1.DeploymentModeSourceComponentAnnotation, nil
	case effective.DeploymentModeServiceAnnotation:
		return reportv1alpha1.DeploymentModeSourceServiceAnnotation, nil
	case effective.DeploymentModeServiceSpec:
		return reportv1alpha1.DeploymentModeSourceServiceSpec, nil
	case effective.DeploymentModeLeaderWorkerShape:
		return reportv1alpha1.DeploymentModeSourceLeaderWorkerShape, nil
	case effective.DeploymentModeDefault:
		return reportv1alpha1.DeploymentModeSourceDefault, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}

func mapDesiredState(value effective.AutoscalingComponentState) (reportv1alpha1.AutoscaleDesiredState, error) {
	switch value {
	case effective.AutoscalingComponentAvailable:
		return reportv1alpha1.AutoscaleDesiredAvailable, nil
	case effective.AutoscalingComponentUnavailable:
		return reportv1alpha1.AutoscaleDesiredUnavailable, nil
	case effective.AutoscalingComponentUnsupported:
		return reportv1alpha1.AutoscaleDesiredUnsupported, nil
	case effective.AutoscalingComponentInvalid:
		return reportv1alpha1.AutoscaleDesiredInvalid, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}

func mapDesiredClass(value v1beta1.AutoscalerClass, state effective.AutoscalingComponentState) (reportv1alpha1.AutoscaleClass, error) {
	switch value {
	case v1beta1.AutoscalerHPA:
		return reportv1alpha1.AutoscaleClassHPA, nil
	case v1beta1.AutoscalerKEDA:
		return reportv1alpha1.AutoscaleClassKEDA, nil
	case v1beta1.AutoscalerExternal:
		return reportv1alpha1.AutoscaleClassExternal, nil
	case v1beta1.AutoscalerNone:
		return reportv1alpha1.AutoscaleClassNone, nil
	default:
		if state == effective.AutoscalingComponentUnavailable || state == effective.AutoscalingComponentUnsupported ||
			state == effective.AutoscalingComponentInvalid {
			return reportv1alpha1.AutoscaleClassUnknown, nil
		}
	}
	return "", ErrExplainProjectionInvalid
}

func mapDesiredManagedBy(value effective.AutoscalingManagedBy, state effective.AutoscalingComponentState) (reportv1alpha1.AutoscaleManagedBy, error) {
	switch value {
	case effective.AutoscalingManagedByOME:
		return reportv1alpha1.AutoscaleManagedByOME, nil
	case effective.AutoscalingManagedByExternal:
		return reportv1alpha1.AutoscaleManagedByExternal, nil
	case effective.AutoscalingManagedByNone:
		return reportv1alpha1.AutoscaleManagedByNone, nil
	case "":
		if state == effective.AutoscalingComponentUnavailable || state == effective.AutoscalingComponentUnsupported ||
			state == effective.AutoscalingComponentInvalid {
			return reportv1alpha1.AutoscaleManagedByUnknown, nil
		}
	}
	return "", ErrExplainProjectionInvalid
}

func mapDesiredSource(value effective.AutoscalingSpecSource, state effective.AutoscalingComponentState) (reportv1alpha1.AutoscaleSpecSource, error) {
	switch value {
	case effective.AutoscalingSpecSourceISVC:
		return reportv1alpha1.AutoscaleSpecSourceISVC, nil
	case effective.AutoscalingSpecSourcePolicy:
		return reportv1alpha1.AutoscaleSpecSourcePolicy, nil
	case effective.AutoscalingSpecSourceRuntime:
		return reportv1alpha1.AutoscaleSpecSourceRuntime, nil
	case effective.AutoscalingSpecSourceLegacy:
		return reportv1alpha1.AutoscaleSpecSourceLegacy, nil
	case effective.AutoscalingSpecSourceDefault:
		return reportv1alpha1.AutoscaleSpecSourceDefault, nil
	case "":
		if state == effective.AutoscalingComponentUnavailable || state == effective.AutoscalingComponentUnsupported ||
			state == effective.AutoscalingComponentInvalid {
			return reportv1alpha1.AutoscaleSpecSourceUnknown, nil
		}
	}
	return "", ErrExplainProjectionInvalid
}

func mapDesiredBounds(value effective.AutoscalingBounds) (reportv1alpha1.AutoscaleExplainBounds, error) {
	result := reportv1alpha1.AutoscaleExplainBounds{}
	switch value.State {
	case effective.AutoscalingBoundsAvailable:
		if value.MinReplicas == nil || value.MaxReplicas == nil {
			return result, ErrExplainProjectionInvalid
		}
		result.State = reportv1alpha1.AutoscaleBoundsAvailable
		result.MinReplicas = copyExplainInt32(value.MinReplicas)
		result.MaxReplicas = copyExplainInt32(value.MaxReplicas)
	case effective.AutoscalingBoundsUnavailable:
		if value.MinReplicas != nil || value.MaxReplicas != nil {
			return result, ErrExplainProjectionInvalid
		}
		result.State = reportv1alpha1.AutoscaleBoundsUnavailable
	case effective.AutoscalingBoundsInvalid:
		if value.MinReplicas != nil || value.MaxReplicas != nil {
			return result, ErrExplainProjectionInvalid
		}
		result.State = reportv1alpha1.AutoscaleBoundsInvalid
	default:
		return result, ErrExplainProjectionInvalid
	}
	return result, nil
}

func mapScaleToZero(value effective.ScaleToZeroState) (reportv1alpha1.AutoscaleScaleToZeroState, error) {
	switch value {
	case effective.ScaleToZeroNotRequested:
		return reportv1alpha1.AutoscaleScaleToZeroNotRequested, nil
	case effective.ScaleToZeroEligible:
		return reportv1alpha1.AutoscaleScaleToZeroEligible, nil
	case effective.ScaleToZeroUnsupported:
		return reportv1alpha1.AutoscaleScaleToZeroUnsupported, nil
	case effective.ScaleToZeroUnavailable:
		return reportv1alpha1.AutoscaleScaleToZeroUnavailable, nil
	case effective.ScaleToZeroInvalid:
		return reportv1alpha1.AutoscaleScaleToZeroInvalid, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}

func mapDesiredTarget(value *effective.AutoscalingTargetReference) (*reportv1alpha1.AutoscaleTargetIdentity, error) {
	if value == nil {
		return nil, nil
	}
	result := &reportv1alpha1.AutoscaleTargetIdentity{
		APIVersion: value.APIVersion, Namespace: value.Namespace, Name: value.Name,
	}
	switch value.Kind {
	case "Deployment":
		if value.APIVersion != "apps/v1" {
			return nil, ErrExplainProjectionInvalid
		}
		result.Kind = reportv1alpha1.AutoscaleTargetDeployment
	case "InferenceReplica":
		if value.APIVersion != v1beta1.SchemeGroupVersion.String() {
			return nil, ErrExplainProjectionInvalid
		}
		result.Kind = reportv1alpha1.AutoscaleTargetInferenceReplica
	default:
		return nil, ErrExplainProjectionInvalid
	}
	if value.Namespace == "" || value.Name == "" {
		return nil, ErrExplainProjectionInvalid
	}
	return result, nil
}

func projectEffectiveIssues(values []effective.AutoscalingIssueCode) ([]reportv1alpha1.AutoscaleExplainIssueCode, error) {
	result := make([]reportv1alpha1.AutoscaleExplainIssueCode, 0, len(values))
	for _, value := range values {
		mapped, ok := mapEffectiveIssue(value)
		if !ok {
			return nil, ErrExplainProjectionInvalid
		}
		result = append(result, mapped)
	}
	return result, nil
}

func mapEffectiveIssue(value effective.AutoscalingIssueCode) (reportv1alpha1.AutoscaleExplainIssueCode, bool) {
	mappings := map[effective.AutoscalingIssueCode]reportv1alpha1.AutoscaleExplainIssueCode{
		effective.AutoscalingIssuePolicyResolutionUnavailable: reportv1alpha1.AutoscaleExplainIssuePolicyResolutionUnavailable,
		effective.AutoscalingIssuePolicyReferenceInvalid:      reportv1alpha1.AutoscaleExplainIssuePolicyReferenceInvalid,
		effective.AutoscalingIssueDeploymentModeUnsupported:   reportv1alpha1.AutoscaleExplainIssueDeploymentModeUnsupported,
		effective.AutoscalingIssueAutoscalerClassInvalid:      reportv1alpha1.AutoscaleExplainIssueAutoscalerClassInvalid,
		effective.AutoscalingIssueKEDATriggersRequired:        reportv1alpha1.AutoscaleExplainIssueKEDATriggersRequired,
		effective.AutoscalingIssueKEDAConfigurationInvalid:    reportv1alpha1.AutoscaleExplainIssueKEDAConfigurationInvalid,
		effective.AutoscalingIssueHPAMetricMalformed:          reportv1alpha1.AutoscaleExplainIssueHPAMetricMalformed,
		effective.AutoscalingIssueKEDAIdleNotBelowMinimum:     reportv1alpha1.AutoscaleExplainIssueKEDAIdleNotBelowMinimum,
		effective.AutoscalingIssueReservedHPANameCollision:    reportv1alpha1.AutoscaleExplainIssueReservedHPANameCollision,
		effective.AutoscalingIssueLegacyAutoscalerInvalid:     reportv1alpha1.AutoscaleExplainIssueLegacyAutoscalerInvalid,
		effective.AutoscalingIssueReplicaBoundsInvalid:        reportv1alpha1.AutoscaleExplainIssueReplicaBoundsInvalid,
		effective.AutoscalingIssueScaleToZeroInvalid:          reportv1alpha1.AutoscaleExplainIssueScaleToZeroInvalid,
		effective.AutoscalingIssueScaleToZeroUnsupported:      reportv1alpha1.AutoscaleExplainIssueScaleToZeroUnsupported,
		effective.AutoscalingIssueScalingPolicyUnsupported:    reportv1alpha1.AutoscaleExplainIssueScalingPolicyUnsupported,
		effective.AutoscalingIssueScalingPolicyInvalid:        reportv1alpha1.AutoscaleExplainIssueScalingPolicyInvalid,
		effective.AutoscalingIssueInheritanceUnavailable:      reportv1alpha1.AutoscaleExplainIssueInheritanceUnavailable,
		effective.AutoscalingIssueActiveRevisionInconsistent:  reportv1alpha1.AutoscaleExplainIssueActiveRevisionInconsistent,
	}
	mapped, ok := mappings[value]
	return mapped, ok
}

func projectAutoscaleExplainSources(
	isvc *v1beta1.InferenceService,
	resolution effective.AutoscalingResolution,
	collectedAt time.Time,
) []reportv1alpha1.RuntimeSourceReference {
	active := resolution.Active
	sources := []reportv1alpha1.RuntimeSourceReference{{
		Kind: "InferenceService", Namespace: isvc.Namespace, Name: isvc.Name,
		UID: string(isvc.UID), Generation: isvc.Generation,
		Evidence: reportv1alpha1.EvidenceObserved, CollectedAt: collectedAt,
	}}
	if active.State != effective.AutoscalingActiveConfigurationAvailable {
		return sources
	}
	if active.Origin == effective.ConfigurationOriginLiveRuntime && validAutoscalingModelSource(resolution.Model, isvc.Namespace) {
		sources = append(sources, reportv1alpha1.RuntimeSourceReference{
			Kind: resolution.Model.Kind, Namespace: resolution.Model.Namespace, Name: resolution.Model.Name,
			UID: resolution.Model.UID, Generation: resolution.Model.Generation,
			Evidence: reportv1alpha1.EvidenceObserved, CollectedAt: collectedAt,
		})
	}
	for _, source := range active.Inheritance.Sources {
		sources = append(sources, reportv1alpha1.RuntimeSourceReference{
			Kind: source.Kind, Namespace: source.Namespace, Name: source.Name,
			UID: source.UID, Generation: source.Generation,
			Evidence: reportv1alpha1.EvidenceObserved, CollectedAt: collectedAt,
		})
	}
	runtimeEvidence := reportv1alpha1.EvidenceComputed
	if active.Runtime.IdentityObserved {
		runtimeEvidence = reportv1alpha1.EvidenceObserved
	}
	runtimeUID, runtimeGeneration := active.Runtime.UID, active.Runtime.Generation
	if !active.Runtime.IdentityObserved {
		runtimeUID, runtimeGeneration = "", 0
	}
	sources = append(sources, reportv1alpha1.RuntimeSourceReference{
		Kind: active.Runtime.Kind, Namespace: active.Runtime.Namespace, Name: active.Runtime.Name,
		UID: runtimeUID, Generation: runtimeGeneration,
		Evidence: runtimeEvidence, CollectedAt: collectedAt,
	})
	if active.Revision != nil {
		evidence := reportv1alpha1.EvidenceComputed
		if active.Revision.IdentityObserved {
			evidence = reportv1alpha1.EvidenceObserved
		}
		sources = append(sources, reportv1alpha1.RuntimeSourceReference{
			Kind: "ControllerRevision", Namespace: active.Revision.Namespace, Name: active.Revision.Name,
			UID: active.Revision.UID, Evidence: evidence, CollectedAt: collectedAt,
		})
	}
	return sources
}

func validAutoscalingModelSource(model *effective.AutoscalingModelReference, isvcNamespace string) bool {
	if model == nil || model.Name == "" {
		return false
	}
	switch model.Kind {
	case "BaseModel":
		return model.Namespace == isvcNamespace && model.Namespace != ""
	case "ClusterBaseModel":
		return model.Namespace == ""
	default:
		return false
	}
}

func copyExplainInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func copyExplainInt(value int) *int {
	copy := value
	return &copy
}

func mapConfigurationOrigin(value effective.ConfigurationOrigin) (reportv1alpha1.ConfigurationOrigin, error) {
	switch value {
	case effective.ConfigurationOriginLiveRuntime:
		return reportv1alpha1.ConfigurationOriginLiveRuntime, nil
	case effective.ConfigurationOriginControllerRevision:
		return reportv1alpha1.ConfigurationOriginControllerRevision, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}

func mapRevisionConsistency(value effective.RevisionConsistencyState) (reportv1alpha1.RevisionConsistency, error) {
	switch value {
	case effective.RevisionConsistencyConsistent:
		return reportv1alpha1.RevisionConsistencyConsistent, nil
	case effective.RevisionConsistencyInconsistent:
		return reportv1alpha1.RevisionConsistencyInconsistent, nil
	case effective.RevisionConsistencyUnknown:
		return reportv1alpha1.RevisionConsistencyUnknown, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}

func mapRevisionRole(value effective.RuntimeRevisionRole) (reportv1alpha1.RuntimeRevisionRole, error) {
	if value != effective.RuntimeRevisionRoleActive {
		return "", ErrExplainProjectionInvalid
	}
	return reportv1alpha1.RuntimeRevisionRoleActive, nil
}

func mapAutoscalingRuntimeReference(value effective.AutoscalingRuntimeReference) (reportv1alpha1.RuntimeObjectReference, error) {
	kind, err := mapRuntimeKind(value.Kind)
	if err != nil || value.Name == "" {
		return reportv1alpha1.RuntimeObjectReference{}, ErrExplainProjectionInvalid
	}
	if kind == reportv1alpha1.RuntimeKindServingRuntime && value.Namespace == "" {
		return reportv1alpha1.RuntimeObjectReference{}, ErrExplainProjectionInvalid
	}
	if kind == reportv1alpha1.RuntimeKindClusterServingRuntime && value.Namespace != "" {
		return reportv1alpha1.RuntimeObjectReference{}, ErrExplainProjectionInvalid
	}
	return reportv1alpha1.RuntimeObjectReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: kind, Namespace: value.Namespace,
		Name: value.Name, UID: value.UID, Generation: value.Generation,
	}, nil
}

func mapRuntimeKind(value string) (reportv1alpha1.RuntimeKind, error) {
	switch value {
	case runtimeselector.KindServingRuntime:
		return reportv1alpha1.RuntimeKindServingRuntime, nil
	case runtimeselector.KindClusterServingRuntime:
		return reportv1alpha1.RuntimeKindClusterServingRuntime, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}

func mapAutoscalingInheritance(value effective.AutoscalingInheritance) (reportv1alpha1.RuntimeInheritance, error) {
	result := reportv1alpha1.RuntimeInheritance{Sources: []reportv1alpha1.RuntimeObjectReference{}}
	switch value.State {
	case effective.InheritanceObserved:
		if value.UnavailableReason != "" || len(value.Sources) == 0 {
			return result, ErrExplainProjectionInvalid
		}
		result.State = reportv1alpha1.InheritanceStateObserved
		for _, source := range value.Sources {
			mapped, err := mapAutoscalingRuntimeReference(source)
			if err != nil {
				return result, err
			}
			result.Sources = append(result.Sources, mapped)
		}
	case effective.InheritanceNotRecorded:
		if value.UnavailableReason != "" || len(value.Sources) != 0 {
			return result, ErrExplainProjectionInvalid
		}
		result.State = reportv1alpha1.InheritanceStateNotRecorded
	case effective.InheritanceUnavailable:
		if len(value.Sources) != 0 {
			return result, ErrExplainProjectionInvalid
		}
		result.State = reportv1alpha1.InheritanceStateUnavailable
		reason, err := mapInheritanceUnavailableReason(value.UnavailableReason)
		if err != nil {
			return result, err
		}
		result.UnavailableReason = reason
	default:
		return result, ErrExplainProjectionInvalid
	}
	return result, nil
}

func mapInheritanceUnavailableReason(value effective.InheritanceUnavailableReason) (reportv1alpha1.UnavailableReason, error) {
	switch value {
	case effective.InheritanceNotFound:
		return reportv1alpha1.UnavailableNotFound, nil
	case effective.InheritanceForbidden:
		return reportv1alpha1.UnavailableForbidden, nil
	case effective.InheritanceCycle:
		return reportv1alpha1.UnavailableCycle, nil
	case effective.InheritanceMaxDepthExceeded:
		return reportv1alpha1.UnavailableMaxDepthExceeded, nil
	case effective.InheritanceMalformed:
		return reportv1alpha1.UnavailableMalformedPayload, nil
	case effective.InheritanceUnreadable:
		return reportv1alpha1.UnavailableUnreadable, nil
	default:
		return "", ErrExplainProjectionInvalid
	}
}
