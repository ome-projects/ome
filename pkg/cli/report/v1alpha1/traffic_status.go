package v1alpha1

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/report"
)

// TrafficStatusReportKind identifies the traffic status report schema.
const TrafficStatusReportKind = "TrafficStatusReport"

// TrafficSourceKind is the closed set of objects read by traffic status.
type TrafficSourceKind string

const TrafficSourceInferenceService TrafficSourceKind = "InferenceService"

// TrafficState summarizes controller-reported traffic evidence. It does not
// claim that a data plane has realized the reported configuration.
type TrafficState string

const (
	TrafficStateReported    TrafficState = "Reported"
	TrafficStatePending     TrafficState = "Pending"
	TrafficStateDegraded    TrafficState = "Degraded"
	TrafficStatePartial     TrafficState = "Partial"
	TrafficStateUnavailable TrafficState = "Unavailable"
	TrafficStateInvalid     TrafficState = "Invalid"
)

// TrafficFreshness states whether evidence is bound to the current subject
// generation. Some status fields have no generation marker and remain
// Unverifiable even when neighboring conditions are current.
type TrafficFreshness string

const (
	TrafficFreshnessCurrent      TrafficFreshness = "Current"
	TrafficFreshnessStale        TrafficFreshness = "Stale"
	TrafficFreshnessUnverifiable TrafficFreshness = "Unverifiable"
	TrafficFreshnessUnavailable  TrafficFreshness = "Unavailable"
)

// TrafficTranslator is the allowlisted translator inferred from an exact
// emitted-policy GVK. When no recognized policy proves one, it is unavailable.
type TrafficTranslator string

const (
	TrafficTranslatorEnvoyGateway TrafficTranslator = "envoy-gateway"
	TrafficTranslatorIstio        TrafficTranslator = "istio"
	TrafficTranslatorUnavailable  TrafficTranslator = "Unavailable"
)

// TrafficAlgorithm is the closed set written by the OME traffic controller.
type TrafficAlgorithm string

const (
	TrafficAlgorithmDefault        TrafficAlgorithm = "Default"
	TrafficAlgorithmRoundRobin     TrafficAlgorithm = "RoundRobin"
	TrafficAlgorithmLeastRequest   TrafficAlgorithm = "LeastRequest"
	TrafficAlgorithmRandom         TrafficAlgorithm = "Random"
	TrafficAlgorithmConsistentHash TrafficAlgorithm = "ConsistentHash"
	TrafficAlgorithmUnknown        TrafficAlgorithm = "Unknown"
)

// TrafficPolicyKind identifies one recognized emitted policy resource.
type TrafficPolicyKind string

const (
	TrafficPolicyBackendTrafficPolicy TrafficPolicyKind = "BackendTrafficPolicy"
	TrafficPolicyDestinationRule      TrafficPolicyKind = "DestinationRule"
)

// TrafficConditionType is the closed set of TrafficStatus conditions consumed
// by this report. At most one condition of each type is retained.
type TrafficConditionType string

const (
	TrafficConditionBackendPolicyReady             TrafficConditionType = "BackendPolicyReady"
	TrafficConditionBackendPolicyUnsupportedFields TrafficConditionType = "BackendPolicyUnsupportedFields"
)

type TrafficConditionStatus string

const (
	TrafficConditionTrue    TrafficConditionStatus = "True"
	TrafficConditionFalse   TrafficConditionStatus = "False"
	TrafficConditionUnknown TrafficConditionStatus = "Unknown"
)

// TrafficConditionReason is an allowlist. Arbitrary Kubernetes condition
// reasons and messages are never copied into a report.
type TrafficConditionReason string

const (
	TrafficReasonAcceptedByGateway     TrafficConditionReason = "AcceptedByGateway"
	TrafficReasonConflictingPolicy     TrafficConditionReason = "ConflictingPolicy"
	TrafficReasonUnsupportedField      TrafficConditionReason = "UnsupportedField"
	TrafficReasonNoTranslatorAvailable TrafficConditionReason = "NoTranslatorAvailable"
	TrafficReasonGatewayRejected       TrafficConditionReason = "GatewayRejected"
	TrafficReasonPending               TrafficConditionReason = "Pending"
	TrafficReasonTranslationFailed     TrafficConditionReason = "TranslationFailed"
	TrafficReasonNotReported           TrafficConditionReason = "NotReported"
	TrafficReasonUnknown               TrafficConditionReason = "Unknown"
)

type TrafficUnsupportedState string

const (
	TrafficUnsupportedNone    TrafficUnsupportedState = "None"
	TrafficUnsupportedPresent TrafficUnsupportedState = "Present"
	TrafficUnsupportedUnknown TrafficUnsupportedState = "Unknown"
)

type TrafficAllocationRole string

const (
	TrafficRoleStable TrafficAllocationRole = "stable"
	TrafficRoleCanary TrafficAllocationRole = "canary"
	TrafficRoleOther  TrafficAllocationRole = "other"
)

// TrafficIssueCode is a stable, message-free diagnostic category.
type TrafficIssueCode string

const (
	TrafficIssueTrafficStatusMissing     TrafficIssueCode = "TrafficStatusMissing"
	TrafficIssueAlgorithmInvalid         TrafficIssueCode = "AlgorithmInvalid"
	TrafficIssuePolicyReferenceInvalid   TrafficIssueCode = "PolicyReferenceInvalid"
	TrafficIssuePolicyKindUnsupported    TrafficIssueCode = "PolicyKindUnsupported"
	TrafficIssuePolicyConditionMissing   TrafficIssueCode = "PolicyConditionMissing"
	TrafficIssueConditionInvalid         TrafficIssueCode = "ConditionInvalid"
	TrafficIssueConditionConflict        TrafficIssueCode = "ConditionConflict"
	TrafficIssueRouteInvalid             TrafficIssueCode = "RouteInvalid"
	TrafficIssueRoutesTruncated          TrafficIssueCode = "RoutesTruncated"
	TrafficIssueEndpointInvalid          TrafficIssueCode = "EndpointInvalid"
	TrafficIssueEndpointsTruncated       TrafficIssueCode = "EndpointsTruncated"
	TrafficIssueCanaryInvalid            TrafficIssueCode = "CanaryInvalid"
	TrafficIssueAllocationInvalid        TrafficIssueCode = "AllocationInvalid"
	TrafficIssueAllocationConflict       TrafficIssueCode = "AllocationConflict"
	TrafficIssueAllocationsTruncated     TrafficIssueCode = "AllocationsTruncated"
	TrafficIssueUnknownComponentStatus   TrafficIssueCode = "UnknownComponentStatus"
	TrafficIssueStatusCombinationInvalid TrafficIssueCode = "StatusCombinationInvalid"
)

// TrafficValueSource qualifies a single report value.
type TrafficValueSource struct {
	Evidence  EvidenceLevel    `json:"evidence"`
	Freshness TrafficFreshness `json:"freshness"`
}

// TrafficSourceReference identifies the single parent object read by the
// command. Versioning and arbitrary metadata are deliberately absent.
type TrafficSourceReference struct {
	Kind        TrafficSourceKind `json:"kind"`
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	UID         string            `json:"uid"`
	Generation  int64             `json:"generation"`
	Evidence    EvidenceLevel     `json:"evidence"`
	CollectedAt time.Time         `json:"collectedAt"`
}

type TrafficConditionValue struct {
	Status TrafficConditionStatus `json:"status"`
	Reason TrafficConditionReason `json:"reason"`
}

type TrafficSummarySources struct {
	State       TrafficValueSource `json:"state"`
	Translator  TrafficValueSource `json:"translator"`
	Algorithm   TrafficValueSource `json:"algorithm"`
	PolicyReady TrafficValueSource `json:"policyReady"`
	Unsupported TrafficValueSource `json:"unsupported"`
}

type TrafficSummary struct {
	State       TrafficState            `json:"state"`
	Translator  TrafficTranslator       `json:"translator"`
	Algorithm   TrafficAlgorithm        `json:"algorithm"`
	PolicyReady TrafficConditionValue   `json:"policyReady"`
	Unsupported TrafficUnsupportedState `json:"unsupported"`
	Source      TrafficSummarySources   `json:"source"`
}

type TrafficPolicy struct {
	APIVersion string             `json:"apiVersion"`
	Kind       TrafficPolicyKind  `json:"kind"`
	Namespace  string             `json:"namespace"`
	Name       string             `json:"name"`
	Source     TrafficValueSource `json:"source"`
}

type TrafficRoute struct {
	Name   string             `json:"name"`
	Source TrafficValueSource `json:"source"`
}

// TrafficEndpoint is a sanitized service endpoint. CA data, audiences,
// credentials, queries, and fragments never enter this shape.
type TrafficEndpoint struct {
	URL    string             `json:"url"`
	Source TrafficValueSource `json:"source"`
}

type TrafficCanary struct {
	Component          RuntimeComponentType `json:"component"`
	CurrentStep        int32                `json:"currentStep"`
	TotalSteps         int32                `json:"totalSteps"`
	ObservedTraffic    int32                `json:"observedTraffic"`
	StableRevisionHash string               `json:"stableRevisionHash"`
	CanaryRevisionHash string               `json:"canaryRevisionHash"`
	Source             TrafficValueSource   `json:"source"`
}

type TrafficAllocation struct {
	Component    RuntimeComponentType  `json:"component"`
	Role         TrafficAllocationRole `json:"role"`
	RevisionName string                `json:"revisionName"`
	RevisionHash string                `json:"revisionHash"`
	Percent      int32                 `json:"percent"`
	Source       TrafficValueSource    `json:"source"`
}

type TrafficCondition struct {
	Type               TrafficConditionType   `json:"type"`
	Status             TrafficConditionStatus `json:"status"`
	Reason             TrafficConditionReason `json:"reason"`
	ObservedGeneration int64                  `json:"observedGeneration"`
	LastTransitionTime time.Time              `json:"lastTransitionTime"`
	Source             TrafficValueSource     `json:"source"`
}

type TrafficIssue struct {
	Code      TrafficIssueCode     `json:"code"`
	Component RuntimeComponentType `json:"component,omitempty"`
}

type TrafficWarning struct {
	Code WarningCode `json:"code"`
}

type TrafficStatusContent struct {
	Summary     TrafficSummary      `json:"summary"`
	Policy      *TrafficPolicy      `json:"policy,omitempty"`
	Routes      []TrafficRoute      `json:"routes"`
	Endpoints   []TrafficEndpoint   `json:"endpoints"`
	Canary      *TrafficCanary      `json:"canary,omitempty"`
	Allocations []TrafficAllocation `json:"allocations"`
	Conditions  []TrafficCondition  `json:"conditions"`
	Issues      []TrafficIssue      `json:"issues"`
}

type TrafficStatusReport struct {
	APIVersion  string                   `json:"apiVersion"`
	Kind        string                   `json:"kind"`
	Metadata    Metadata                 `json:"metadata"`
	CollectedAt time.Time                `json:"collectedAt"`
	Sources     []TrafficSourceReference `json:"sources"`
	Content     TrafficStatusContent     `json:"content"`
	Warnings    []TrafficWarning         `json:"warnings"`
}

func NewTrafficStatusReport(metadata Metadata, content TrafficStatusContent, clock Clock) TrafficStatusReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (TrafficStatusReport{
		APIVersion: APIVersion, Kind: TrafficStatusReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []TrafficSourceReference{}, Content: content,
		Warnings: []TrafficWarning{},
	}).Canonical()
}

func (r TrafficStatusReport) Canonical() TrafficStatusReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = TrafficStatusReportKind
	result.CollectedAt = r.CollectedAt.UTC()
	result.Sources = append([]TrafficSourceReference{}, r.Sources...)
	for i := range result.Sources {
		if result.Sources[i].CollectedAt.IsZero() {
			result.Sources[i].CollectedAt = result.CollectedAt
		} else {
			result.Sources[i].CollectedAt = result.Sources[i].CollectedAt.UTC()
		}
	}
	sort.Slice(result.Sources, func(i, j int) bool {
		a, b := result.Sources[i], result.Sources[j]
		return slices.Compare([]string{string(a.Kind), a.Namespace, a.Name, a.UID, strconv.FormatInt(a.Generation, 10), string(a.Evidence), a.CollectedAt.Format(time.RFC3339Nano)}, []string{string(b.Kind), b.Namespace, b.Name, b.UID, strconv.FormatInt(b.Generation, 10), string(b.Evidence), b.CollectedAt.Format(time.RFC3339Nano)}) < 0
	})
	result.Content = r.Content.Canonical()
	result.Warnings = append([]TrafficWarning{}, r.Warnings...)
	sort.Slice(result.Warnings, func(i, j int) bool { return result.Warnings[i].Code < result.Warnings[j].Code })
	result.Warnings = dedupeTrafficWarnings(result.Warnings)
	return result
}

func (r TrafficStatusReport) Table() report.Table { return r.Canonical().Content.Table() }

func (r TrafficStatusReport) WideTable() report.Table { return r.Canonical().Content.WideTable() }

func (c TrafficStatusContent) Canonical() TrafficStatusContent {
	result := c
	if c.Policy != nil {
		policy := *c.Policy
		result.Policy = &policy
	}
	result.Routes = append([]TrafficRoute{}, c.Routes...)
	sort.Slice(result.Routes, func(i, j int) bool {
		return compareTrafficRoute(result.Routes[i], result.Routes[j]) < 0
	})
	result.Routes = dedupeTrafficRoutes(result.Routes)
	result.Endpoints = append([]TrafficEndpoint{}, c.Endpoints...)
	sort.Slice(result.Endpoints, func(i, j int) bool {
		return compareTrafficEndpoint(result.Endpoints[i], result.Endpoints[j]) < 0
	})
	result.Endpoints = dedupeTrafficEndpoints(result.Endpoints)
	if c.Canary != nil {
		canary := *c.Canary
		result.Canary = &canary
	}
	result.Allocations = append([]TrafficAllocation{}, c.Allocations...)
	sort.Slice(result.Allocations, func(i, j int) bool {
		return compareTrafficAllocation(result.Allocations[i], result.Allocations[j]) < 0
	})
	result.Allocations = dedupeTrafficAllocations(result.Allocations)
	result.Conditions = append([]TrafficCondition{}, c.Conditions...)
	for i := range result.Conditions {
		result.Conditions[i].LastTransitionTime = result.Conditions[i].LastTransitionTime.UTC()
	}
	sort.Slice(result.Conditions, func(i, j int) bool {
		return compareTrafficCondition(result.Conditions[i], result.Conditions[j]) < 0
	})
	result.Conditions = dedupeTrafficConditions(result.Conditions)
	result.Issues = append([]TrafficIssue{}, c.Issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		if rank := cmp.Compare(trafficComponentRank(result.Issues[i].Component), trafficComponentRank(result.Issues[j].Component)); rank != 0 {
			return rank < 0
		}
		if result.Issues[i].Component != result.Issues[j].Component {
			return result.Issues[i].Component < result.Issues[j].Component
		}
		return result.Issues[i].Code < result.Issues[j].Code
	})
	result.Issues = dedupeTrafficIssues(result.Issues)
	return result
}

func (c TrafficStatusContent) Table() report.Table {
	c = c.Canonical()
	rows := trafficSummaryRows(c)
	rows = append(rows, []string{"ROUTES", "-", strconv.Itoa(len(c.Routes)), trafficSourceCell(routesSource(c.Routes, c.Summary.Source.PolicyReady))})
	rows = append(rows, []string{"ENDPOINTS", "-", strconv.Itoa(len(c.Endpoints)), trafficSourceCell(endpointsSource(c.Endpoints))})
	if c.Canary != nil {
		rows = append(rows, []string{"CANARY", string(c.Canary.Component), canaryStepCell(c.Canary), trafficSourceCell(c.Canary.Source)})
	}
	for _, allocation := range c.Allocations {
		rows = append(rows, []string{"WEIGHT", string(allocation.Component), fmt.Sprintf("%s:%s=%d%%", allocation.Role, allocation.RevisionHash, allocation.Percent), trafficSourceCell(allocation.Source)})
	}
	rows = append(rows, trafficIssueRows(c.Issues)...)
	return report.Table{Headers: []string{"FIELD", "COMP", "VALUE", "SOURCE"}, Rows: rows}
}

func (c TrafficStatusContent) WideTable() report.Table {
	c = c.Canonical()
	rows := trafficSummaryRows(c)
	if c.Policy != nil {
		rows = append(rows, []string{"POLICY", "-", strings.Join([]string{c.Policy.APIVersion, string(c.Policy.Kind), c.Policy.Namespace, c.Policy.Name}, "/"), trafficSourceCell(c.Policy.Source)})
	}
	for _, route := range c.Routes {
		rows = append(rows, []string{"ROUTE", "-", route.Name, trafficSourceCell(route.Source)})
	}
	for _, endpoint := range c.Endpoints {
		rows = append(rows, []string{"ENDPOINT", "-", endpoint.URL, trafficSourceCell(endpoint.Source)})
	}
	if c.Canary != nil {
		rows = append(rows, []string{"CANARY", string(c.Canary.Component), canaryStepCell(c.Canary), trafficSourceCell(c.Canary.Source)})
	}
	for _, allocation := range c.Allocations {
		rows = append(rows, []string{"TARGET", string(allocation.Component), fmt.Sprintf("%s:%s=%d%%", allocation.Role, allocation.RevisionName, allocation.Percent), trafficSourceCell(allocation.Source)})
	}
	for _, condition := range c.Conditions {
		value := fmt.Sprintf("%s=%s/%s gen=%d at=%s", condition.Type, condition.Status, condition.Reason, condition.ObservedGeneration, condition.LastTransitionTime.UTC().Format(time.RFC3339))
		rows = append(rows, []string{"CONDITION", "-", value, trafficSourceCell(condition.Source)})
	}
	rows = append(rows, trafficIssueRows(c.Issues)...)
	return report.Table{Headers: []string{"FIELD", "COMP", "VALUE", "SOURCE"}, Rows: rows}
}

func canaryStepCell(canary *TrafficCanary) string {
	return fmt.Sprintf("%d/%d @ %d%%", canaryDisplayStep(canary), canary.TotalSteps, canary.ObservedTraffic)
}

func canaryDisplayStep(canary *TrafficCanary) int32 {
	displayStep := canary.CurrentStep + 1
	if canary.CurrentStep >= canary.TotalSteps {
		displayStep = canary.TotalSteps
	}
	return displayStep
}

func trafficSummaryRows(c TrafficStatusContent) [][]string {
	return [][]string{
		{"STATE", "-", string(c.Summary.State), trafficSourceCell(c.Summary.Source.State)},
		{"TRANSLATOR", "-", string(c.Summary.Translator), trafficSourceCell(c.Summary.Source.Translator)},
		{"ALGORITHM", "-", string(c.Summary.Algorithm), trafficSourceCell(c.Summary.Source.Algorithm)},
		{"POLICY-READY", "-", string(c.Summary.PolicyReady.Status) + "/" + string(c.Summary.PolicyReady.Reason), trafficSourceCell(c.Summary.Source.PolicyReady)},
		{"UNSUPPORTED", "-", string(c.Summary.Unsupported), trafficSourceCell(c.Summary.Source.Unsupported)},
	}
}

func trafficIssueRows(issues []TrafficIssue) [][]string {
	rows := make([][]string, 0, len(issues))
	for _, issue := range issues {
		component := "-"
		if issue.Component != "" {
			component = string(issue.Component)
		}
		rows = append(rows, []string{"ISSUE", component, string(issue.Code), "Computed/Unverifiable"})
	}
	return rows
}

func routesSource(routes []TrafficRoute, fallback TrafficValueSource) TrafficValueSource {
	if len(routes) == 0 {
		return fallback
	}
	return routes[0].Source
}

func endpointsSource(endpoints []TrafficEndpoint) TrafficValueSource {
	if len(endpoints) == 0 {
		return TrafficValueSource{Evidence: EvidenceReported, Freshness: TrafficFreshnessUnverifiable}
	}
	return endpoints[0].Source
}

func trafficSourceCell(source TrafficValueSource) string {
	if source.Evidence == "" || source.Freshness == "" {
		return "-"
	}
	return string(source.Evidence) + "/" + string(source.Freshness)
}

func compareTrafficRoute(a, b TrafficRoute) int {
	return slices.Compare([]string{a.Name, string(a.Source.Evidence), string(a.Source.Freshness)}, []string{b.Name, string(b.Source.Evidence), string(b.Source.Freshness)})
}

func compareTrafficEndpoint(a, b TrafficEndpoint) int {
	return slices.Compare([]string{a.URL, string(a.Source.Evidence), string(a.Source.Freshness)}, []string{b.URL, string(b.Source.Evidence), string(b.Source.Freshness)})
}

func compareTrafficAllocation(a, b TrafficAllocation) int {
	for _, result := range []int{
		cmp.Compare(trafficComponentRank(a.Component), trafficComponentRank(b.Component)),
		cmp.Compare(a.Component, b.Component), cmp.Compare(trafficRoleRank(a.Role), trafficRoleRank(b.Role)),
		cmp.Compare(a.Role, b.Role), cmp.Compare(a.RevisionName, b.RevisionName),
		cmp.Compare(a.RevisionHash, b.RevisionHash), cmp.Compare(a.Percent, b.Percent),
		cmp.Compare(a.Source.Evidence, b.Source.Evidence), cmp.Compare(a.Source.Freshness, b.Source.Freshness),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareTrafficCondition(a, b TrafficCondition) int {
	for _, result := range []int{
		cmp.Compare(trafficConditionRank(a.Type), trafficConditionRank(b.Type)), cmp.Compare(a.Type, b.Type),
		cmp.Compare(a.Status, b.Status), cmp.Compare(a.Reason, b.Reason), cmp.Compare(a.ObservedGeneration, b.ObservedGeneration),
		a.LastTransitionTime.Compare(b.LastTransitionTime), cmp.Compare(a.Source.Evidence, b.Source.Evidence), cmp.Compare(a.Source.Freshness, b.Source.Freshness),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func trafficComponentRank(component RuntimeComponentType) int {
	switch component {
	case RuntimeComponentEngine:
		return 0
	case RuntimeComponentDecoder:
		return 1
	case RuntimeComponentRouter:
		return 2
	default:
		return 3
	}
}

func trafficRoleRank(role TrafficAllocationRole) int {
	switch role {
	case TrafficRoleStable:
		return 0
	case TrafficRoleCanary:
		return 1
	default:
		return 2
	}
}

func trafficConditionRank(condition TrafficConditionType) int {
	if condition == TrafficConditionBackendPolicyReady {
		return 0
	}
	return 1
}

func dedupeTrafficRoutes(values []TrafficRoute) []TrafficRoute {
	return slices.Compact(values)
}

func dedupeTrafficEndpoints(values []TrafficEndpoint) []TrafficEndpoint {
	return slices.Compact(values)
}

func dedupeTrafficAllocations(values []TrafficAllocation) []TrafficAllocation {
	return slices.Compact(values)
}

func dedupeTrafficConditions(values []TrafficCondition) []TrafficCondition {
	return slices.Compact(values)
}

func dedupeTrafficIssues(values []TrafficIssue) []TrafficIssue {
	return slices.Compact(values)
}

func dedupeTrafficWarnings(values []TrafficWarning) []TrafficWarning {
	return slices.Compact(values)
}
