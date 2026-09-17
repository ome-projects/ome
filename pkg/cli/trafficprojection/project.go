// Package trafficprojection projects traffic evidence already reported on one
// InferenceService. It performs no cluster reads and never claims that the
// data plane has realized controller-reported routes, endpoints, or weights.
package trafficprojection

import (
	"errors"
	"net"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	knapis "knative.dev/pkg/apis"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/canaryevidence"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omerollout "sigs.k8s.io/ome/pkg/rollout"
)

const (
	maxRoutes              = 4
	maxEndpoints           = 16
	maxCanaryGroups        = 3
	maxAllocationsPerClass = 8
	maxEndpointPathBytes   = 256
	maxEndpointBytes       = 512
)

var (
	ErrInferenceServiceRequired          = errors.New("inference service is required")
	ErrInferenceServiceNameRequired      = errors.New("inference service name is required")
	ErrInferenceServiceNamespaceRequired = errors.New("inference service namespace is required")
	ErrInferenceServiceUIDRequired       = errors.New("inference service UID is required")
)

// Project builds a bounded deterministic report from one observed parent.
func Project(isvc *omev1beta1.InferenceService, clock reportv1alpha1.Clock) (reportv1alpha1.TrafficStatusReport, error) {
	if isvc == nil {
		return reportv1alpha1.TrafficStatusReport{}, ErrInferenceServiceRequired
	}
	if isvc.Name == "" {
		return reportv1alpha1.TrafficStatusReport{}, ErrInferenceServiceNameRequired
	}
	if isvc.Namespace == "" {
		return reportv1alpha1.TrafficStatusReport{}, ErrInferenceServiceNamespaceRequired
	}
	if isvc.UID == "" {
		return reportv1alpha1.TrafficStatusReport{}, ErrInferenceServiceUIDRequired
	}

	b := projector{
		isvc:     isvc,
		issueSet: make(map[string]struct{}),
	}
	b.projectTrafficStatus()
	b.projectEndpoints()
	b.projectCanary()
	b.projectAllocations()
	b.finishSummary()

	reportValue := reportv1alpha1.NewTrafficStatusReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name},
		b.content,
		clock,
	)
	reportValue.Sources = []reportv1alpha1.TrafficSourceReference{{
		Kind: reportv1alpha1.TrafficSourceInferenceService, Namespace: isvc.Namespace,
		Name: isvc.Name, UID: string(isvc.UID), Generation: isvc.Generation,
		Evidence: reportv1alpha1.EvidenceReported,
	}}
	if b.invalid || b.partial {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.TrafficWarning{Code: reportv1alpha1.WarningPartialData})
	}
	if b.stale {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.TrafficWarning{Code: reportv1alpha1.WarningStaleEvidence})
	}
	if b.truncated {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.TrafficWarning{Code: reportv1alpha1.WarningTruncated})
	}
	return reportValue.Canonical(), nil
}

type projector struct {
	isvc             *omev1beta1.InferenceService
	content          reportv1alpha1.TrafficStatusContent
	ready            *reportv1alpha1.TrafficCondition
	readyValid       bool
	unsupported      *reportv1alpha1.TrafficCondition
	unsupportedValid bool
	issueSet         map[string]struct{}
	canaries         map[reportv1alpha1.RuntimeComponentType]*reportv1alpha1.TrafficCanary
	invalid          bool
	partial          bool
	stale            bool
	truncated        bool
}

type allocationCandidate struct {
	name    string
	hash    string
	percent int32
	latest  bool
}

func (b *projector) projectTrafficStatus() {
	unavailable := source(reportv1alpha1.EvidenceReported, reportv1alpha1.TrafficFreshnessUnavailable)
	b.content.Summary = reportv1alpha1.TrafficSummary{
		State:       reportv1alpha1.TrafficStateUnavailable,
		Translator:  reportv1alpha1.TrafficTranslatorUnavailable,
		Algorithm:   reportv1alpha1.TrafficAlgorithmUnknown,
		PolicyReady: reportv1alpha1.TrafficConditionValue{Status: reportv1alpha1.TrafficConditionUnknown, Reason: reportv1alpha1.TrafficReasonNotReported},
		Unsupported: reportv1alpha1.TrafficUnsupportedUnknown,
		Source: reportv1alpha1.TrafficSummarySources{
			State:      source(reportv1alpha1.EvidenceComputed, reportv1alpha1.TrafficFreshnessUnverifiable),
			Translator: source(reportv1alpha1.EvidenceComputed, reportv1alpha1.TrafficFreshnessUnavailable),
			Algorithm:  unavailable, PolicyReady: unavailable, Unsupported: unavailable,
		},
	}
	b.content.Routes = []reportv1alpha1.TrafficRoute{}
	b.content.Endpoints = []reportv1alpha1.TrafficEndpoint{}
	b.content.Allocations = []reportv1alpha1.TrafficAllocation{}
	b.content.Conditions = []reportv1alpha1.TrafficCondition{}
	b.content.Issues = []reportv1alpha1.TrafficIssue{}

	status := b.isvc.Status.Traffic
	if status == nil {
		b.addIssue(reportv1alpha1.TrafficIssueTrafficStatusMissing, "", false)
		return
	}

	b.projectConditions(status.Conditions)
	conditionFreshness := reportv1alpha1.TrafficFreshnessUnverifiable
	if b.ready != nil {
		b.content.Summary.PolicyReady = reportv1alpha1.TrafficConditionValue{Status: b.ready.Status, Reason: b.ready.Reason}
		b.content.Summary.Source.PolicyReady = b.ready.Source
		if b.readyValid {
			conditionFreshness = b.ready.Source.Freshness
		}
	} else {
		b.addIssue(reportv1alpha1.TrafficIssuePolicyConditionMissing, "", false)
		b.partial = true
	}
	b.content.Summary.Source.Algorithm = source(reportv1alpha1.EvidenceReported, conditionFreshness)

	algorithm, ok := projectAlgorithm(status.Algorithm)
	if !ok {
		b.addIssue(reportv1alpha1.TrafficIssueAlgorithmInvalid, "", true)
	}
	b.content.Summary.Algorithm = algorithm

	b.projectPolicy(status.BackendPolicyResource, conditionFreshness)
	b.projectRoutes(status.TargetedHTTPRoutes, conditionFreshness)
	if status.BackendPolicyResource == nil && len(status.TargetedHTTPRoutes) > 0 {
		b.addIssue(reportv1alpha1.TrafficIssueStatusCombinationInvalid, "", true)
	}
	if b.ready != nil && b.ready.Status == reportv1alpha1.TrafficConditionTrue && b.content.Policy == nil {
		b.addIssue(reportv1alpha1.TrafficIssueStatusCombinationInvalid, "", true)
	}

	switch {
	case b.unsupported != nil && !b.unsupportedValid:
		b.content.Summary.Unsupported = reportv1alpha1.TrafficUnsupportedUnknown
		b.content.Summary.Source.Unsupported = b.unsupported.Source
	case b.unsupportedValid && b.unsupported.Status == reportv1alpha1.TrafficConditionTrue:
		b.content.Summary.Unsupported = reportv1alpha1.TrafficUnsupportedPresent
		b.content.Summary.Source.Unsupported = b.unsupported.Source
		b.partial = true
	case b.unsupportedValid:
		b.content.Summary.Unsupported = reportv1alpha1.TrafficUnsupportedUnknown
		b.content.Summary.Source.Unsupported = b.unsupported.Source
	case b.readyValid && provesNoUnsupportedFields(b.ready):
		b.content.Summary.Unsupported = reportv1alpha1.TrafficUnsupportedNone
		b.content.Summary.Source.Unsupported = b.ready.Source
	default:
		b.content.Summary.Unsupported = reportv1alpha1.TrafficUnsupportedUnknown
		if b.unsupported != nil {
			b.content.Summary.Source.Unsupported = b.unsupported.Source
		} else if b.ready != nil {
			b.content.Summary.Source.Unsupported = b.ready.Source
		}
	}

	if b.readyValid && b.ready.Reason == reportv1alpha1.TrafficReasonNoTranslatorAvailable {
		switch {
		case b.content.Policy != nil:
			b.addIssue(reportv1alpha1.TrafficIssueStatusCombinationInvalid, "", true)
			b.content.Summary.Translator = reportv1alpha1.TrafficTranslatorUnavailable
			b.content.Summary.Source.Translator = source(reportv1alpha1.EvidenceComputed, reportv1alpha1.TrafficFreshnessUnverifiable)
		case status.BackendPolicyResource == nil:
			b.content.Summary.Translator = reportv1alpha1.TrafficTranslatorUnavailable
			b.content.Summary.Source.Translator = source(reportv1alpha1.EvidenceComputed, b.ready.Source.Freshness)
		}
	}
}

func (b *projector) projectConditions(conditions []metav1.Condition) {
	ordered := make([]metav1.Condition, 0, len(conditions))
	for i := range conditions {
		if _, recognized := projectConditionType(conditions[i].Type); recognized {
			ordered = append(ordered, conditions[i])
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, c := ordered[i], ordered[j]
		if a.Type != c.Type {
			return a.Type < c.Type
		}
		if a.Status != c.Status {
			return a.Status < c.Status
		}
		if a.Reason != c.Reason {
			return a.Reason < c.Reason
		}
		if a.ObservedGeneration != c.ObservedGeneration {
			return a.ObservedGeneration < c.ObservedGeneration
		}
		return a.LastTransitionTime.Time.Before(c.LastTransitionTime.Time)
	})

	seen := make(map[reportv1alpha1.TrafficConditionType]bool, 2)
	for i := range ordered {
		conditionType, _ := projectConditionType(ordered[i].Type)
		if seen[conditionType] {
			b.addIssue(reportv1alpha1.TrafficIssueConditionConflict, "", true)
			if conditionType == reportv1alpha1.TrafficConditionBackendPolicyReady {
				b.readyValid = false
			} else {
				b.unsupportedValid = false
			}
			continue
		}
		seen[conditionType] = true
		projected, valid := b.projectCondition(conditionType, &ordered[i])
		if !valid {
			b.addIssue(reportv1alpha1.TrafficIssueConditionInvalid, "", true)
		}
		b.content.Conditions = append(b.content.Conditions, projected)
		if conditionType == reportv1alpha1.TrafficConditionBackendPolicyReady {
			value := projected
			b.ready = &value
			b.readyValid = valid
		} else {
			value := projected
			b.unsupported = &value
			b.unsupportedValid = valid
		}
	}
}

func (b *projector) projectCondition(conditionType reportv1alpha1.TrafficConditionType, condition *metav1.Condition) (reportv1alpha1.TrafficCondition, bool) {
	generationValid := condition.ObservedGeneration > 0 && condition.ObservedGeneration <= b.isvc.Generation
	freshness := reportv1alpha1.TrafficFreshnessUnverifiable
	if generationValid && condition.ObservedGeneration == b.isvc.Generation {
		freshness = reportv1alpha1.TrafficFreshnessCurrent
	} else if generationValid {
		freshness = reportv1alpha1.TrafficFreshnessStale
		b.stale = true
		b.partial = true
	}
	status, statusOK := projectConditionStatus(condition.Status)
	reason, reasonOK := projectConditionReason(condition.Reason)
	valid := generationValid &&
		statusOK &&
		reasonOK &&
		validConditionCombination(conditionType, status, reason) &&
		!condition.LastTransitionTime.IsZero()
	if !valid {
		status = reportv1alpha1.TrafficConditionUnknown
		if !reasonOK {
			reason = reportv1alpha1.TrafficReasonUnknown
		}
	}
	return reportv1alpha1.TrafficCondition{
		Type: conditionType, Status: status, Reason: reason,
		ObservedGeneration: condition.ObservedGeneration,
		LastTransitionTime: condition.LastTransitionTime.Time.UTC(),
		Source:             source(reportv1alpha1.EvidenceReported, freshness),
	}, valid
}

func (b *projector) projectPolicy(ref *omev1beta1.BackendPolicyRef, freshness reportv1alpha1.TrafficFreshness) {
	if ref == nil {
		return
	}
	if problems := utilvalidation.IsDNS1123Subdomain(ref.Name); len(problems) > 0 {
		b.addIssue(reportv1alpha1.TrafficIssuePolicyReferenceInvalid, "", true)
		return
	}
	var kind reportv1alpha1.TrafficPolicyKind
	switch {
	case ref.APIVersion == "gateway.envoyproxy.io/v1alpha1" && ref.Kind == "BackendTrafficPolicy":
		kind = reportv1alpha1.TrafficPolicyBackendTrafficPolicy
		b.content.Summary.Translator = reportv1alpha1.TrafficTranslatorEnvoyGateway
	case ref.APIVersion == "networking.istio.io/v1" && ref.Kind == "DestinationRule":
		kind = reportv1alpha1.TrafficPolicyDestinationRule
		b.content.Summary.Translator = reportv1alpha1.TrafficTranslatorIstio
	default:
		b.addIssue(reportv1alpha1.TrafficIssuePolicyKindUnsupported, "", true)
		return
	}
	b.content.Summary.Source.Translator = source(reportv1alpha1.EvidenceComputed, freshness)
	b.content.Policy = &reportv1alpha1.TrafficPolicy{
		APIVersion: ref.APIVersion, Kind: kind, Namespace: b.isvc.Namespace,
		Name: ref.Name, Source: source(reportv1alpha1.EvidenceReported, freshness),
	}
}

func (b *projector) projectRoutes(routeNames []string, freshness reportv1alpha1.TrafficFreshness) {
	unique := make(map[string]struct{}, len(routeNames))
	for _, name := range routeNames {
		if problems := utilvalidation.IsDNS1123Subdomain(name); len(problems) > 0 {
			b.addIssue(reportv1alpha1.TrafficIssueRouteInvalid, "", true)
			continue
		}
		unique[name] = struct{}{}
	}
	names := make([]string, 0, len(unique))
	for name := range unique {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) > maxRoutes {
		names = names[:maxRoutes]
		b.addIssue(reportv1alpha1.TrafficIssueRoutesTruncated, "", false)
		b.markTruncated()
	}
	for _, name := range names {
		b.content.Routes = append(b.content.Routes, reportv1alpha1.TrafficRoute{
			Name: name, Source: source(reportv1alpha1.EvidenceReported, freshness),
		})
	}
}

func (b *projector) projectEndpoints() {
	values := make([]*knapis.URL, 0, len(b.isvc.Status.Addresses)+2)
	if len(b.isvc.Status.Addresses) > 0 {
		for i := range b.isvc.Status.Addresses {
			values = append(values, b.isvc.Status.Addresses[i].URL)
		}
	} else {
		if b.isvc.Status.URL != nil && !b.isvc.Status.URL.IsEmpty() {
			values = append(values, b.isvc.Status.URL)
		}
		if b.isvc.Status.Address != nil && b.isvc.Status.Address.URL != nil && !b.isvc.Status.Address.URL.IsEmpty() {
			values = append(values, b.isvc.Status.Address.URL)
		}
	}
	unique := make(map[string]struct{}, len(values))
	for _, value := range values {
		sanitized, ok := sanitizeEndpoint(value)
		if !ok {
			b.addIssue(reportv1alpha1.TrafficIssueEndpointInvalid, "", true)
			continue
		}
		unique[sanitized] = struct{}{}
	}
	endpoints := make([]string, 0, len(unique))
	for endpoint := range unique {
		endpoints = append(endpoints, endpoint)
	}
	sort.Strings(endpoints)
	if len(endpoints) > maxEndpoints {
		endpoints = endpoints[:maxEndpoints]
		b.addIssue(reportv1alpha1.TrafficIssueEndpointsTruncated, "", false)
		b.markTruncated()
	}
	for _, endpoint := range endpoints {
		b.content.Endpoints = append(b.content.Endpoints, reportv1alpha1.TrafficEndpoint{
			URL:    endpoint,
			Source: source(reportv1alpha1.EvidenceReported, reportv1alpha1.TrafficFreshnessUnverifiable),
		})
	}
}

func (b *projector) projectCanary() {
	groups := omerollout.CanaryGroups(b.isvc)
	if len(groups) == 0 {
		if b.isvc.Status.Canary != nil {
			b.addIssue(reportv1alpha1.TrafficIssueCanaryInvalid, "", true)
		}
		return
	}
	if len(groups) > maxCanaryGroups {
		b.addIssue(reportv1alpha1.TrafficIssueCanaryInvalid, "", true)
		return
	}
	// The report has one canary-step field. For concurrent runs, keep their
	// per-unit allocation roles but do not pretend one run describes them all.
	if len(groups) > 1 {
		b.partial = true
	}
	b.canaries = make(map[reportv1alpha1.RuntimeComponentType]*reportv1alpha1.TrafficCanary, len(groups))
	seenUnits := make(map[omev1beta1.ComponentType]struct{}, len(groups))
	for _, group := range groups {
		component, primary, primaryOK := canaryPrimary(group)
		issueComponent := reportv1alpha1.RuntimeComponentType("")
		if len(groups) > 1 {
			issueComponent = component
		}
		if !primaryOK || !canaryevidence.ValidCanaryPlan(group.Canary) || len(group.Canary.Steps) > 20 || b.canaries[component] != nil {
			b.addIssue(reportv1alpha1.TrafficIssueCanaryInvalid, issueComponent, true)
			continue
		}
		if _, duplicate := seenUnits[primary]; duplicate {
			b.addIssue(reportv1alpha1.TrafficIssueCanaryInvalid, issueComponent, true)
			continue
		}
		seenUnits[primary] = struct{}{}
		var status *omev1beta1.CanaryStatus
		if len(groups) == 1 {
			status = omerollout.CanaryStatusFor(&b.isvc.Status, primary)
		} else if entrypoint, found := b.isvc.Status.Components[omerollout.CanaryUnit(primary)]; found {
			// The legacy alias cannot identify which of several units owns it.
			status = entrypoint.Canary
		}
		if status == nil {
			if entrypoint, found := b.isvc.Status.Components[primary]; found &&
				canaryevidence.PhaseNeedsStatus(canaryevidence.ProjectPhase(entrypoint.RolloutPhase)) {
				b.addIssue(reportv1alpha1.TrafficIssueCanaryInvalid, issueComponent, true)
			}
			continue
		}
		if !validCanaryStatus(status, len(group.Canary.Steps)) || !b.validCanaryEpoch(group.Canary.Steps, primary, status) {
			b.addIssue(reportv1alpha1.TrafficIssueCanaryInvalid, issueComponent, true)
			continue
		}
		canary := &reportv1alpha1.TrafficCanary{
			Component: component, CurrentStep: status.CurrentStep,
			TotalSteps: int32(len(group.Canary.Steps)), ObservedTraffic: status.ObservedTrafficWeight,
			StableRevisionHash: status.StableRevisionHash, CanaryRevisionHash: status.CanaryRevisionHash,
			Source: source(reportv1alpha1.EvidenceReported, reportv1alpha1.TrafficFreshnessUnverifiable),
		}
		b.canaries[component] = canary
		if len(groups) == 1 {
			b.content.Canary = canary
		}
	}
}

func (b *projector) validCanaryEpoch(
	steps []omev1beta1.RolloutGroupStep,
	primary omev1beta1.ComponentType,
	status *omev1beta1.CanaryStatus,
) bool {
	component, found := b.isvc.Status.Components[primary]
	if !found {
		return false
	}
	phase := canaryevidence.ProjectPhase(component.RolloutPhase)
	if phase == reportv1alpha1.RolloutPhaseStable {
		return canaryevidence.CompletedStatusMatches(
			b.isvc.Name,
			primary,
			steps,
			status,
			component.Traffic,
		)
	}
	repinBoundary := canaryevidence.ValidRepinBoundary(
		b.isvc, primary, phase, steps, status, component.Traffic,
	)
	if !canaryevidence.PhaseNeedsStatus(phase) ||
		(!canaryevidence.ValidPhaseStepResidue(phase, steps, status) && !repinBoundary) {
		return false
	}
	if canaryevidence.StatusBindsTraffic(phase, status) &&
		!canaryevidence.ActiveTrafficMatches(b.isvc.Name, primary, phase, status, component.Traffic) {
		return false
	}
	return !canaryevidence.PhaseBindsStepTraffic(phase) ||
		canaryevidence.ObservedTrafficMatchesStep(phase, steps, status) ||
		repinBoundary
}

func (b *projector) projectAllocations() {
	components := []omev1beta1.ComponentType{omev1beta1.EngineComponent, omev1beta1.DecoderComponent, omev1beta1.RouterComponent}
	for component := range b.isvc.Status.Components {
		if !slices.Contains(components, component) {
			b.addIssue(reportv1alpha1.TrafficIssueUnknownComponentStatus, "", true)
		}
	}
	for _, component := range components {
		status, ok := b.isvc.Status.Components[component]
		if !ok || len(status.Traffic) == 0 {
			continue
		}
		b.projectComponentAllocations(component, status)
	}
}

func (b *projector) projectComponentAllocations(component omev1beta1.ComponentType, status omev1beta1.ComponentStatusSpec) {
	projectedComponent := projectComponent(component)
	candidates := make([]allocationCandidate, 0, len(status.Traffic))
	total := int32(0)
	for _, target := range status.Traffic {
		total += target.Percent
		hash, validName := revisionTargetHash(b.isvc.Name, component, target.RevisionName)
		if target.Percent < 0 || target.Percent > 100 || !validName {
			b.addIssue(reportv1alpha1.TrafficIssueAllocationInvalid, projectedComponent, true)
			continue
		}
		candidates = append(candidates, allocationCandidate{name: target.RevisionName, hash: hash, percent: target.Percent, latest: target.LatestRevision})
	}
	if total != 100 {
		b.addIssue(reportv1alpha1.TrafficIssueAllocationInvalid, projectedComponent, true)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].name != candidates[j].name {
			return candidates[i].name < candidates[j].name
		}
		if candidates[i].percent != candidates[j].percent {
			return candidates[i].percent < candidates[j].percent
		}
		return !candidates[i].latest && candidates[j].latest
	})
	unique := candidates[:0]
	for _, candidate := range candidates {
		if len(unique) > 0 && unique[len(unique)-1].name == candidate.name {
			b.addIssue(reportv1alpha1.TrafficIssueAllocationConflict, projectedComponent, true)
			continue
		}
		unique = append(unique, candidate)
	}
	stableName := ""
	if b.canaries[projectedComponent] == nil {
		stableName = b.stableAllocationName(component, projectedComponent, status.LatestRolledoutRevision, unique)
	}
	if len(unique) > maxAllocationsPerClass {
		unique = unique[:maxAllocationsPerClass]
		b.addIssue(reportv1alpha1.TrafficIssueAllocationsTruncated, projectedComponent, false)
		b.markTruncated()
	}
	for _, candidate := range unique {
		b.content.Allocations = append(b.content.Allocations, reportv1alpha1.TrafficAllocation{
			Component: projectedComponent, Role: b.allocationRole(projectedComponent, candidate.hash, candidate.name, stableName),
			RevisionName: candidate.name, RevisionHash: candidate.hash, Percent: candidate.percent,
			Source: source(reportv1alpha1.EvidenceReported, reportv1alpha1.TrafficFreshnessUnverifiable),
		})
	}
}

func (b *projector) stableAllocationName(
	component omev1beta1.ComponentType,
	projectedComponent reportv1alpha1.RuntimeComponentType,
	rolledOutName string,
	candidates []allocationCandidate,
) string {
	latestCount := 0
	for _, candidate := range candidates {
		if candidate.latest {
			latestCount++
		}
	}
	if latestCount > 1 {
		b.addIssue(reportv1alpha1.TrafficIssueAllocationInvalid, projectedComponent, true)
	}

	if rolledOutName != "" {
		if _, valid := revisionTargetHash(b.isvc.Name, component, rolledOutName); !valid {
			b.addIssue(reportv1alpha1.TrafficIssueAllocationInvalid, projectedComponent, true)
			return ""
		}
		for _, candidate := range candidates {
			if candidate.name == rolledOutName {
				return rolledOutName
			}
		}
		b.addIssue(reportv1alpha1.TrafficIssueAllocationInvalid, projectedComponent, true)
		return ""
	}

	if len(candidates) > 1 {
		b.addIssue(reportv1alpha1.TrafficIssueAllocationInvalid, projectedComponent, true)
		return ""
	}
	if len(candidates) == 1 && candidates[0].latest {
		return candidates[0].name
	}
	return ""
}

func (b *projector) allocationRole(component reportv1alpha1.RuntimeComponentType, hash, name, stableName string) reportv1alpha1.TrafficAllocationRole {
	if canary := b.canaries[component]; canary != nil {
		if canary.StableRevisionHash != "" && hash == canary.StableRevisionHash {
			return reportv1alpha1.TrafficRoleStable
		}
		if hash == canary.CanaryRevisionHash {
			if canary.CurrentStep == canary.TotalSteps {
				return reportv1alpha1.TrafficRoleStable
			}
			return reportv1alpha1.TrafficRoleCanary
		}
		return reportv1alpha1.TrafficRoleOther
	}
	if stableName != "" && name == stableName {
		return reportv1alpha1.TrafficRoleStable
	}
	return reportv1alpha1.TrafficRoleOther
}

func (b *projector) finishSummary() {
	if b.invalid {
		b.content.Summary.State = reportv1alpha1.TrafficStateInvalid
		return
	}
	if b.isvc.Status.Traffic == nil || b.ready == nil {
		b.content.Summary.State = reportv1alpha1.TrafficStateUnavailable
		return
	}
	if b.ready.Source.Freshness != reportv1alpha1.TrafficFreshnessCurrent {
		b.content.Summary.State = reportv1alpha1.TrafficStatePartial
		return
	}
	switch b.ready.Status {
	case reportv1alpha1.TrafficConditionFalse:
		b.content.Summary.State = reportv1alpha1.TrafficStateDegraded
	case reportv1alpha1.TrafficConditionUnknown:
		if b.partial {
			b.content.Summary.State = reportv1alpha1.TrafficStatePartial
		} else {
			b.content.Summary.State = reportv1alpha1.TrafficStatePending
		}
	case reportv1alpha1.TrafficConditionTrue:
		if b.partial {
			b.content.Summary.State = reportv1alpha1.TrafficStatePartial
		} else {
			b.content.Summary.State = reportv1alpha1.TrafficStateReported
		}
	default:
		b.content.Summary.State = reportv1alpha1.TrafficStateInvalid
	}
}

func (b *projector) addIssue(code reportv1alpha1.TrafficIssueCode, component reportv1alpha1.RuntimeComponentType, invalid bool) {
	key := string(code) + "\x00" + string(component)
	if _, exists := b.issueSet[key]; exists {
		if invalid {
			b.invalid = true
		}
		return
	}
	b.issueSet[key] = struct{}{}
	b.content.Issues = append(b.content.Issues, reportv1alpha1.TrafficIssue{Code: code, Component: component})
	if invalid {
		b.invalid = true
	}
}

func (b *projector) markTruncated() {
	b.truncated = true
	b.partial = true
}

func projectAlgorithm(value string) (reportv1alpha1.TrafficAlgorithm, bool) {
	switch value {
	case "Default":
		return reportv1alpha1.TrafficAlgorithmDefault, true
	case "RoundRobin":
		return reportv1alpha1.TrafficAlgorithmRoundRobin, true
	case "LeastRequest":
		return reportv1alpha1.TrafficAlgorithmLeastRequest, true
	case "Random":
		return reportv1alpha1.TrafficAlgorithmRandom, true
	case "ConsistentHash":
		return reportv1alpha1.TrafficAlgorithmConsistentHash, true
	default:
		return reportv1alpha1.TrafficAlgorithmUnknown, false
	}
}

func projectConditionType(value string) (reportv1alpha1.TrafficConditionType, bool) {
	switch value {
	case omev1beta1.TrafficConditionBackendPolicyReady:
		return reportv1alpha1.TrafficConditionBackendPolicyReady, true
	case omev1beta1.TrafficConditionBackendPolicyUnsupportedFields:
		return reportv1alpha1.TrafficConditionBackendPolicyUnsupportedFields, true
	default:
		return "", false
	}
}

func projectConditionStatus(value metav1.ConditionStatus) (reportv1alpha1.TrafficConditionStatus, bool) {
	switch value {
	case metav1.ConditionTrue:
		return reportv1alpha1.TrafficConditionTrue, true
	case metav1.ConditionFalse:
		return reportv1alpha1.TrafficConditionFalse, true
	case metav1.ConditionUnknown:
		return reportv1alpha1.TrafficConditionUnknown, true
	default:
		return reportv1alpha1.TrafficConditionUnknown, false
	}
}

func projectConditionReason(value string) (reportv1alpha1.TrafficConditionReason, bool) {
	switch value {
	case omev1beta1.TrafficReasonAcceptedByGateway:
		return reportv1alpha1.TrafficReasonAcceptedByGateway, true
	case omev1beta1.TrafficReasonConflictingPolicy:
		return reportv1alpha1.TrafficReasonConflictingPolicy, true
	case omev1beta1.TrafficReasonUnsupportedField:
		return reportv1alpha1.TrafficReasonUnsupportedField, true
	case omev1beta1.TrafficReasonNoTranslatorAvailable:
		return reportv1alpha1.TrafficReasonNoTranslatorAvailable, true
	case omev1beta1.TrafficReasonGatewayRejected:
		return reportv1alpha1.TrafficReasonGatewayRejected, true
	case omev1beta1.TrafficReasonPending:
		return reportv1alpha1.TrafficReasonPending, true
	case omev1beta1.TrafficReasonTranslationFailed:
		return reportv1alpha1.TrafficReasonTranslationFailed, true
	default:
		return reportv1alpha1.TrafficReasonUnknown, false
	}
}

func provesNoUnsupportedFields(condition *reportv1alpha1.TrafficCondition) bool {
	if condition == nil || condition.Source.Freshness != reportv1alpha1.TrafficFreshnessCurrent {
		return false
	}
	switch {
	case condition.Status == reportv1alpha1.TrafficConditionTrue && condition.Reason == reportv1alpha1.TrafficReasonAcceptedByGateway:
		return true
	case condition.Status == reportv1alpha1.TrafficConditionUnknown && condition.Reason == reportv1alpha1.TrafficReasonPending:
		return true
	case condition.Status == reportv1alpha1.TrafficConditionFalse && condition.Reason == reportv1alpha1.TrafficReasonGatewayRejected:
		return true
	default:
		return false
	}
}

func validConditionCombination(conditionType reportv1alpha1.TrafficConditionType, status reportv1alpha1.TrafficConditionStatus, reason reportv1alpha1.TrafficConditionReason) bool {
	if conditionType == reportv1alpha1.TrafficConditionBackendPolicyUnsupportedFields {
		return status == reportv1alpha1.TrafficConditionTrue && reason == reportv1alpha1.TrafficReasonUnsupportedField
	}
	switch status {
	case reportv1alpha1.TrafficConditionTrue:
		return reason == reportv1alpha1.TrafficReasonAcceptedByGateway
	case reportv1alpha1.TrafficConditionUnknown:
		return reason == reportv1alpha1.TrafficReasonPending
	case reportv1alpha1.TrafficConditionFalse:
		return reason == reportv1alpha1.TrafficReasonConflictingPolicy ||
			reason == reportv1alpha1.TrafficReasonNoTranslatorAvailable ||
			reason == reportv1alpha1.TrafficReasonGatewayRejected ||
			reason == reportv1alpha1.TrafficReasonTranslationFailed
	default:
		return false
	}
}

func canaryPrimary(group *omev1beta1.RolloutGroup) (reportv1alpha1.RuntimeComponentType, omev1beta1.ComponentType, bool) {
	if group == nil {
		return "", "", false
	}
	primary, valid := canaryevidence.Primary(group.Components)
	if !valid {
		return "", "", false
	}
	return projectComponent(primary), primary, true
}

func validCanaryStatus(status *omev1beta1.CanaryStatus, totalSteps int) bool {
	if status == nil || totalSteps == 0 || status.CurrentStep < 0 || status.ObservedTrafficWeight < 0 || status.ObservedTrafficWeight > 100 || !canaryevidence.SafeRevisionHash(status.CanaryRevisionHash) {
		return false
	}
	if int(status.CurrentStep) == totalSteps {
		return status.ObservedTrafficWeight == 100 && status.StableRevisionHash == ""
	}
	return int(status.CurrentStep) < totalSteps &&
		canaryevidence.SafeRevisionHash(status.StableRevisionHash) &&
		status.StableRevisionHash != status.CanaryRevisionHash
}

func revisionTargetHash(isvcName string, component omev1beta1.ComponentType, revisionName string) (string, bool) {
	hash := canaryevidence.RevisionHash(isvcName, component, revisionName)
	return hash, hash != ""
}

func projectComponent(component omev1beta1.ComponentType) reportv1alpha1.RuntimeComponentType {
	switch component {
	case omev1beta1.EngineComponent:
		return reportv1alpha1.RuntimeComponentEngine
	case omev1beta1.DecoderComponent:
		return reportv1alpha1.RuntimeComponentDecoder
	case omev1beta1.RouterComponent:
		return reportv1alpha1.RuntimeComponentRouter
	default:
		return ""
	}
}

func sanitizeEndpoint(value *knapis.URL) (string, bool) {
	if value == nil || value.IsEmpty() {
		return "", false
	}
	u := value.URL()
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.RawPath != "" {
		return "", false
	}
	if len(u.Path) > maxEndpointPathBytes || (u.Path != "" && !strings.HasPrefix(u.Path, "/")) || strings.IndexFunc(u.Path, unicode.IsControl) >= 0 {
		return "", false
	}
	hostname := strings.ToLower(u.Hostname())
	if hostname == "" || (net.ParseIP(hostname) == nil && len(utilvalidation.IsDNS1123Subdomain(hostname)) > 0) {
		return "", false
	}
	if port := u.Port(); port != "" {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return "", false
		}
	}
	sanitized := (&url.URL{Scheme: u.Scheme, Host: strings.ToLower(u.Host), Path: u.Path}).String()
	if len(sanitized) > maxEndpointBytes {
		return "", false
	}
	return sanitized, true
}

func source(evidence reportv1alpha1.EvidenceLevel, freshness reportv1alpha1.TrafficFreshness) reportv1alpha1.TrafficValueSource {
	return reportv1alpha1.TrafficValueSource{Evidence: evidence, Freshness: freshness}
}
