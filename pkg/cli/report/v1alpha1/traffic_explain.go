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

// TrafficExplainReportKind identifies the stable traffic explanation schema.
const TrafficExplainReportKind = "TrafficExplainReport"

// TrafficExplainState summarizes the relationship between declared intent,
// controller-reported support, and controller-reported realization evidence.
type TrafficExplainState string

const (
	TrafficExplainNoIntent    TrafficExplainState = "NoIntent"
	TrafficExplainConsistent  TrafficExplainState = "Consistent"
	TrafficExplainPending     TrafficExplainState = "Pending"
	TrafficExplainUnsupported TrafficExplainState = "Unsupported"
	TrafficExplainMismatch    TrafficExplainState = "Mismatch"
	TrafficExplainPartial     TrafficExplainState = "Partial"
	TrafficExplainUnavailable TrafficExplainState = "Unavailable"
	TrafficExplainInvalid     TrafficExplainState = "Invalid"
)

type TrafficIntentState string

const (
	TrafficIntentAbsent   TrafficIntentState = "Absent"
	TrafficIntentDeclared TrafficIntentState = "Declared"
	TrafficIntentInvalid  TrafficIntentState = "Invalid"
)

type TrafficSupportState string

const (
	TrafficSupportNotApplicable TrafficSupportState = "NotApplicable"
	TrafficSupportHonored       TrafficSupportState = "Honored"
	TrafficSupportPending       TrafficSupportState = "Pending"
	TrafficSupportRejected      TrafficSupportState = "Rejected"
	TrafficSupportPartial       TrafficSupportState = "Partial"
	TrafficSupportUnavailable   TrafficSupportState = "Unavailable"
	TrafficSupportInvalid       TrafficSupportState = "Invalid"
)

type TrafficRealizationState string

const (
	TrafficRealizationUnavailable TrafficRealizationState = "Unavailable"
	TrafficRealizationReported    TrafficRealizationState = "Reported"
	TrafficRealizationPartial     TrafficRealizationState = "Partial"
	TrafficRealizationInvalid     TrafficRealizationState = "Invalid"
)

type TrafficFeatureState string

const (
	TrafficFeatureAbsent   TrafficFeatureState = "Absent"
	TrafficFeatureDeclared TrafficFeatureState = "Declared"
	TrafficFeatureInvalid  TrafficFeatureState = "Invalid"
)

// TrafficFeatureVariant is an allowlist of non-sensitive selector classes.
// Header and cookie names are deliberately represented only by a count.
type TrafficFeatureVariant string

const (
	TrafficFeatureVariantUnavailable TrafficFeatureVariant = "Unavailable"
	TrafficFeatureVariantHeader      TrafficFeatureVariant = "Header"
	TrafficFeatureVariantCookie      TrafficFeatureVariant = "Cookie"
	TrafficFeatureVariantSourceIP    TrafficFeatureVariant = "SourceIP"
	TrafficFeatureVariantMetadata    TrafficFeatureVariant = "Metadata"
	TrafficFeatureVariantUnknown     TrafficFeatureVariant = "Unknown"
)

type TrafficExtensionKind string

const (
	TrafficExtensionCircuitBreaker   TrafficExtensionKind = "CircuitBreaker"
	TrafficExtensionRetry            TrafficExtensionKind = "Retry"
	TrafficExtensionTimeout          TrafficExtensionKind = "Timeout"
	TrafficExtensionEnvoyPassthrough TrafficExtensionKind = "EnvoyPassthrough"
	TrafficExtensionIstioPassthrough TrafficExtensionKind = "IstioPassthrough"
)

type TrafficComparisonField string

const (
	TrafficComparisonAlgorithm    TrafficComparisonField = "algorithm"
	TrafficComparisonPolicy       TrafficComparisonField = "policy"
	TrafficComparisonCanaryWeight TrafficComparisonField = "canary-weight"
)

type TrafficComparisonState string

const (
	TrafficComparisonMatch         TrafficComparisonState = "Match"
	TrafficComparisonMismatch      TrafficComparisonState = "Mismatch"
	TrafficComparisonUnverifiable  TrafficComparisonState = "Unverifiable"
	TrafficComparisonInvalid       TrafficComparisonState = "Invalid"
	TrafficComparisonNotApplicable TrafficComparisonState = "NotApplicable"
)

type TrafficExplainIssueCode string

const (
	TrafficExplainIssueDeclaredSpecInvalid        TrafficExplainIssueCode = "DeclaredSpecInvalid"
	TrafficExplainIssueDeclaredAnnotationsInvalid TrafficExplainIssueCode = "DeclaredAnnotationsInvalid"
	TrafficExplainIssueAlgorithmMismatch          TrafficExplainIssueCode = "AlgorithmMismatch"
	TrafficExplainIssueIntentNotReported          TrafficExplainIssueCode = "IntentNotReported"
	TrafficExplainIssueUnsupportedDeclaredFields  TrafficExplainIssueCode = "UnsupportedDeclaredFields"
	TrafficExplainIssueNoTranslatorAvailable      TrafficExplainIssueCode = "NoTranslatorAvailable"
	TrafficExplainIssuePolicyRejected             TrafficExplainIssueCode = "PolicyRejected"
	TrafficExplainIssueReportedEvidenceInvalid    TrafficExplainIssueCode = "ReportedEvidenceInvalid"
	TrafficExplainIssueReportedEvidenceStale      TrafficExplainIssueCode = "ReportedEvidenceStale"
	TrafficExplainIssueRealizationUnavailable     TrafficExplainIssueCode = "RealizationUnavailable"
)

// TrafficExplainSourceReference identifies the parent snapshot without
// serializing UIDs, resource versions, or arbitrary metadata.
type TrafficExplainSourceReference struct {
	Kind        TrafficSourceKind `json:"kind"`
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	Generation  int64             `json:"generation"`
	Evidence    EvidenceLevel     `json:"evidence"`
	CollectedAt time.Time         `json:"collectedAt"`
}

type TrafficDeclaredFeature struct {
	State      TrafficFeatureState   `json:"state"`
	Variant    TrafficFeatureVariant `json:"variant"`
	InputCount int                   `json:"inputCount"`
}

type TrafficDeclaredExtension struct {
	Kind  TrafficExtensionKind `json:"kind"`
	Count int                  `json:"count"`
}

type TrafficDeclaredIntent struct {
	State            TrafficIntentState         `json:"state"`
	Algorithm        TrafficAlgorithm           `json:"algorithm"`
	ConsistentHash   TrafficDeclaredFeature     `json:"consistentHash"`
	EndpointOverride TrafficDeclaredFeature     `json:"endpointOverride"`
	Extensions       []TrafficDeclaredExtension `json:"extensions"`
	Source           TrafficValueSource         `json:"source"`
}

type TrafficExplainSummary struct {
	State             TrafficExplainState     `json:"state"`
	Intent            TrafficIntentState      `json:"intent"`
	Support           TrafficSupportState     `json:"support"`
	Realization       TrafficRealizationState `json:"realization"`
	Source            TrafficValueSource      `json:"source"`
	SupportSource     TrafficValueSource      `json:"supportSource"`
	RealizationSource TrafficValueSource      `json:"realizationSource"`
}

type TrafficExplainComparison struct {
	Field  TrafficComparisonField `json:"field"`
	State  TrafficComparisonState `json:"state"`
	Source TrafficValueSource     `json:"source"`
}

type TrafficExplainIssue struct {
	Code      TrafficExplainIssueCode `json:"code"`
	Component RuntimeComponentType    `json:"component,omitempty"`
}

type TrafficExplainContent struct {
	Summary     TrafficExplainSummary      `json:"summary"`
	Intent      TrafficDeclaredIntent      `json:"intent"`
	Reported    TrafficStatusContent       `json:"reported"`
	Comparisons []TrafficExplainComparison `json:"comparisons"`
	Issues      []TrafficExplainIssue      `json:"issues"`
}

type TrafficExplainReport struct {
	APIVersion  string                          `json:"apiVersion"`
	Kind        string                          `json:"kind"`
	Metadata    Metadata                        `json:"metadata"`
	CollectedAt time.Time                       `json:"collectedAt"`
	Sources     []TrafficExplainSourceReference `json:"sources"`
	Content     TrafficExplainContent           `json:"content"`
	Warnings    []TrafficWarning                `json:"warnings"`
}

func NewTrafficExplainReport(metadata Metadata, content TrafficExplainContent, clock Clock) TrafficExplainReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (TrafficExplainReport{
		APIVersion: APIVersion, Kind: TrafficExplainReportKind,
		Metadata: metadata, CollectedAt: clock.Now().UTC(),
		Sources: []TrafficExplainSourceReference{}, Content: content,
		Warnings: []TrafficWarning{},
	}).Canonical()
}

func (r TrafficExplainReport) Canonical() TrafficExplainReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = TrafficExplainReportKind
	result.CollectedAt = r.CollectedAt.UTC()
	result.Sources = append([]TrafficExplainSourceReference{}, r.Sources...)
	for i := range result.Sources {
		if result.Sources[i].CollectedAt.IsZero() {
			result.Sources[i].CollectedAt = result.CollectedAt
		} else {
			result.Sources[i].CollectedAt = result.Sources[i].CollectedAt.UTC()
		}
	}
	sort.Slice(result.Sources, func(i, j int) bool {
		a, b := result.Sources[i], result.Sources[j]
		return slices.Compare([]string{
			string(a.Kind), a.Namespace, a.Name,
			strconv.FormatInt(a.Generation, 10), string(a.Evidence),
			a.CollectedAt.Format(time.RFC3339Nano),
		}, []string{
			string(b.Kind), b.Namespace, b.Name,
			strconv.FormatInt(b.Generation, 10), string(b.Evidence),
			b.CollectedAt.Format(time.RFC3339Nano),
		}) < 0
	})
	result.Sources = slices.Compact(result.Sources)
	result.Content = r.Content.Canonical()
	result.Warnings = append([]TrafficWarning{}, r.Warnings...)
	sort.Slice(result.Warnings, func(i, j int) bool {
		return result.Warnings[i].Code < result.Warnings[j].Code
	})
	result.Warnings = dedupeTrafficWarnings(result.Warnings)
	return result
}

func (r TrafficExplainReport) Table() report.Table {
	return r.Canonical().Content.Table()
}

func (r TrafficExplainReport) WideTable() report.Table {
	r = r.Canonical()
	content := r.Content.WideTable()
	rows := [][]string{
		{"REPORT", "api-version", "-", r.APIVersion, "-"},
		{"REPORT", "kind", "-", r.Kind, "-"},
		{"SUBJECT", "namespace", "-", r.Metadata.Namespace, "-"},
		{"SUBJECT", "name", "-", r.Metadata.Name, "-"},
		{"REPORT", "collected-at", "-", r.CollectedAt.Format(time.RFC3339Nano), "-"},
	}
	for _, source := range r.Sources {
		rows = append(rows, []string{
			"SOURCE", string(source.Kind), string(source.Evidence),
			fmt.Sprintf("%s/%s gen=%d at=%s", source.Namespace, source.Name,
				source.Generation, source.CollectedAt.Format(time.RFC3339Nano)),
			string(source.Evidence) + "/Current",
		})
	}
	rows = append(rows, content.Rows...)
	for _, warning := range r.Warnings {
		rows = append(rows, []string{
			"WARNING", "-", "-", string(warning.Code),
			"Computed/Unverifiable",
		})
	}
	return report.Table{Headers: content.Headers, Rows: rows}
}

func (c TrafficExplainContent) Canonical() TrafficExplainContent {
	result := c
	result.Intent.Extensions = append([]TrafficDeclaredExtension{}, c.Intent.Extensions...)
	sort.Slice(result.Intent.Extensions, func(i, j int) bool {
		a, b := result.Intent.Extensions[i], result.Intent.Extensions[j]
		if rank := cmp.Compare(trafficExtensionRank(a.Kind), trafficExtensionRank(b.Kind)); rank != 0 {
			return rank < 0
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Count < b.Count
	})
	result.Intent.Extensions = slices.Compact(result.Intent.Extensions)
	result.Reported = c.Reported.Canonical()
	result.Summary.SupportSource = trafficExplainSupportSource(result.Summary.Support, result.Reported)
	result.Summary.RealizationSource = trafficExplainRealizationSource(result.Summary.Realization, result.Reported)
	result.Comparisons = append([]TrafficExplainComparison{}, c.Comparisons...)
	sort.Slice(result.Comparisons, func(i, j int) bool {
		a, b := result.Comparisons[i], result.Comparisons[j]
		if rank := cmp.Compare(trafficComparisonRank(a.Field), trafficComparisonRank(b.Field)); rank != 0 {
			return rank < 0
		}
		if a.Field != b.Field {
			return a.Field < b.Field
		}
		if a.State != b.State {
			return a.State < b.State
		}
		return compareTrafficValueSource(a.Source, b.Source) < 0
	})
	result.Comparisons = slices.Compact(result.Comparisons)
	result.Issues = append([]TrafficExplainIssue{}, c.Issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		a, b := result.Issues[i], result.Issues[j]
		if rank := cmp.Compare(trafficComponentRank(a.Component), trafficComponentRank(b.Component)); rank != 0 {
			return rank < 0
		}
		if a.Component != b.Component {
			return a.Component < b.Component
		}
		return a.Code < b.Code
	})
	result.Issues = slices.Compact(result.Issues)
	return result
}

func (c TrafficExplainContent) Table() report.Table {
	c = c.Canonical()
	rows := [][]string{
		{"SUMMARY", string(c.Summary.State), "-", trafficSourceCell(c.Summary.Source)},
		{"INTENT", string(c.Intent.State), string(c.Intent.Algorithm), trafficSourceCell(c.Intent.Source)},
		{"SUPPORT", string(c.Summary.Support), "-", trafficSourceCell(c.Summary.SupportSource)},
		{"TRANSLATE", string(c.Reported.Summary.Source.Translator.Evidence), string(c.Reported.Summary.Translator), trafficSourceCell(c.Reported.Summary.Source.Translator)},
		{"REALIZE", string(c.Summary.Realization), trafficCompactRealizationCell(c.Reported), trafficSourceCell(c.Summary.RealizationSource)},
	}
	for _, comparison := range c.Comparisons {
		rows = append(rows, []string{
			"CHECK", string(comparison.State), string(comparison.Field),
			trafficSourceCell(comparison.Source),
		})
	}
	for _, issue := range c.Issues {
		rows = append(rows, []string{
			"ISSUE", "-", string(issue.Code), "Computed/Unverifiable",
		})
	}
	return report.Table{
		Headers: []string{"LAYER", "STATE", "VALUE", "SOURCE"}, Rows: rows,
	}
}

func (c TrafficExplainContent) WideTable() report.Table {
	c = c.Canonical()
	rows := [][]string{
		{"SUMMARY", "state", string(c.Summary.State), "-", trafficSourceCell(c.Summary.Source)},
		{"SUMMARY", "intent", string(c.Summary.Intent), "-", trafficSourceCell(c.Intent.Source)},
		{"SUMMARY", "support", string(c.Summary.Support), "-", trafficSourceCell(c.Summary.SupportSource)},
		{"SUMMARY", "realization", string(c.Summary.Realization), trafficRealizationCell(c.Reported), trafficSourceCell(c.Summary.RealizationSource)},
		{"INTENT", "algorithm", string(c.Intent.State), string(c.Intent.Algorithm), trafficSourceCell(c.Intent.Source)},
		{"INTENT", "consistent-hash", string(c.Intent.ConsistentHash.State), trafficFeatureCell(c.Intent.ConsistentHash), trafficSourceCell(c.Intent.Source)},
		{"INTENT", "endpoint-override", string(c.Intent.EndpointOverride.State), trafficFeatureCell(c.Intent.EndpointOverride), trafficSourceCell(c.Intent.Source)},
	}
	for _, extension := range c.Intent.Extensions {
		rows = append(rows, []string{
			"EXTENSION", string(extension.Kind), "Declared",
			strconv.Itoa(extension.Count), trafficSourceCell(c.Intent.Source),
		})
	}
	rows = append(rows, trafficExplainReportedRows(c.Reported)...)
	for _, comparison := range c.Comparisons {
		rows = append(rows, []string{
			"CHECK", string(comparison.Field), string(comparison.State), "-",
			trafficSourceCell(comparison.Source),
		})
	}
	for _, issue := range c.Issues {
		component := "-"
		if issue.Component != "" {
			component = string(issue.Component)
		}
		rows = append(rows, []string{
			"ISSUE", component, "-", string(issue.Code),
			"Computed/Unverifiable",
		})
	}
	return report.Table{
		Headers: []string{"LAYER", "FIELD", "STATE", "VALUE", "SOURCE"},
		Rows:    rows,
	}
}

func trafficExplainReportedRows(c TrafficStatusContent) [][]string {
	rows := [][]string{
		{"REPORTED", "algorithm", trafficExplainAlgorithmState(c.Summary), string(c.Summary.Algorithm), trafficSourceCell(c.Summary.Source.Algorithm)},
		{"REPORTED", "translator", "Computed", string(c.Summary.Translator), trafficSourceCell(c.Summary.Source.Translator)},
		{"REPORTED", "policy-ready", string(c.Summary.PolicyReady.Status), string(c.Summary.PolicyReady.Reason), trafficSourceCell(c.Summary.Source.PolicyReady)},
		{"REPORTED", "unsupported", string(c.Summary.Unsupported), "-", trafficSourceCell(c.Summary.Source.Unsupported)},
	}
	if c.Policy != nil {
		rows = append(rows, []string{
			"REPORTED", "policy", "Reported",
			strings.Join([]string{c.Policy.APIVersion, string(c.Policy.Kind), c.Policy.Namespace, c.Policy.Name}, "/"),
			trafficSourceCell(c.Policy.Source),
		})
	}
	for _, route := range c.Routes {
		rows = append(rows, []string{"REPORTED", "route", "Reported", route.Name, trafficSourceCell(route.Source)})
	}
	for _, endpoint := range c.Endpoints {
		rows = append(rows, []string{"OBSERVED", "endpoint", "Reported", endpoint.URL, trafficSourceCell(endpoint.Source)})
	}
	if c.Canary != nil {
		rows = append(rows, []string{
			"OBSERVED", "canary", "Reported",
			fmt.Sprintf("%s step=%d/%d traffic=%d%%", c.Canary.Component, canaryDisplayStep(c.Canary), c.Canary.TotalSteps, c.Canary.ObservedTraffic),
			trafficSourceCell(c.Canary.Source),
		}, []string{
			"OBSERVED", "stable-revision", "Reported", trafficOptionalCell(c.Canary.StableRevisionHash),
			trafficSourceCell(c.Canary.Source),
		}, []string{
			"OBSERVED", "canary-revision", "Reported", trafficOptionalCell(c.Canary.CanaryRevisionHash),
			trafficSourceCell(c.Canary.Source),
		})
	}
	for _, allocation := range c.Allocations {
		rows = append(rows, []string{
			"OBSERVED", "target", "Reported",
			fmt.Sprintf("%s/%s/%s=%d%%", allocation.Component, allocation.Role, allocation.RevisionName, allocation.Percent),
			trafficSourceCell(allocation.Source),
		})
	}
	for _, condition := range c.Conditions {
		rows = append(rows, []string{
			"REPORTED", "condition", "Reported",
			fmt.Sprintf("%s=%s/%s gen=%d at=%s", condition.Type, condition.Status, condition.Reason, condition.ObservedGeneration, condition.LastTransitionTime.UTC().Format(time.RFC3339)),
			trafficSourceCell(condition.Source),
		})
	}
	for _, issue := range c.Issues {
		component := "-"
		if issue.Component != "" {
			component = string(issue.Component)
		}
		rows = append(rows, []string{
			"REPORTED-ISSUE", component, "-", string(issue.Code),
			"Computed/Unverifiable",
		})
	}
	return rows
}

func trafficExplainAlgorithmState(summary TrafficSummary) string {
	if summary.Source.Algorithm.Freshness == TrafficFreshnessUnavailable {
		return "Unavailable"
	}
	if summary.Algorithm == TrafficAlgorithmUnknown {
		return "Invalid"
	}
	return "Reported"
}

func trafficOptionalCell(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

// Support is a computed interpretation of independent controller fields.
func trafficExplainSupportSource(state TrafficSupportState, content TrafficStatusContent) TrafficValueSource {
	result := TrafficValueSource{Evidence: EvidenceComputed, Freshness: TrafficFreshnessCurrent}
	if state == TrafficSupportNotApplicable {
		return result
	}
	if state == TrafficSupportInvalid {
		result.Freshness = TrafficFreshnessUnverifiable
	}
	sources := []TrafficValueSource{content.Summary.Source.PolicyReady, content.Summary.Source.Unsupported}
	if content.Policy != nil {
		sources = append(sources, content.Policy.Source)
	}
	for _, source := range sources {
		if trafficFreshnessRank(source.Freshness) > trafficFreshnessRank(result.Freshness) {
			result.Freshness = source.Freshness
		}
	}
	return result
}

func trafficExplainRealizationSource(state TrafficRealizationState, content TrafficStatusContent) TrafficValueSource {
	result := trafficRealizationSource(content)
	if state == TrafficRealizationInvalid || state == TrafficRealizationPartial {
		// Dropped or truncated inputs also contribute evidence; retained current
		// routes cannot make the computed invalid/partial conclusion current.
		if result.Evidence == EvidenceUnavailable || trafficFreshnessRank(result.Freshness) < trafficFreshnessRank(TrafficFreshnessUnverifiable) {
			result.Freshness = TrafficFreshnessUnverifiable
		}
		result.Evidence = EvidenceComputed
	}
	return result
}

func trafficFeatureCell(feature TrafficDeclaredFeature) string {
	if feature.State == TrafficFeatureAbsent {
		return "-"
	}
	return fmt.Sprintf("%s inputs=%d", feature.Variant, feature.InputCount)
}

func trafficRealizationCell(content TrafficStatusContent) string {
	return fmt.Sprintf("routes=%d endpoints=%d targets=%d", len(content.Routes), len(content.Endpoints), len(content.Allocations))
}

func trafficCompactRealizationCell(content TrafficStatusContent) string {
	return fmt.Sprintf("r=%d e=%d w=%d", len(content.Routes), len(content.Endpoints), len(content.Allocations))
}

func trafficRealizationSource(content TrafficStatusContent) TrafficValueSource {
	sources := make([]TrafficValueSource, 0, len(content.Routes)+len(content.Endpoints)+len(content.Allocations)+1)
	for _, route := range content.Routes {
		sources = append(sources, route.Source)
	}
	for _, endpoint := range content.Endpoints {
		sources = append(sources, endpoint.Source)
	}
	for _, allocation := range content.Allocations {
		sources = append(sources, allocation.Source)
	}
	if content.Canary != nil {
		sources = append(sources, content.Canary.Source)
	}
	if len(sources) == 0 {
		return TrafficValueSource{
			Evidence:  EvidenceUnavailable,
			Freshness: TrafficFreshnessUnavailable,
		}
	}
	result := TrafficValueSource{Evidence: EvidenceReported, Freshness: TrafficFreshnessCurrent}
	for _, candidate := range sources {
		if trafficFreshnessRank(candidate.Freshness) > trafficFreshnessRank(result.Freshness) {
			result.Freshness = candidate.Freshness
		}
	}
	return result
}

func trafficFreshnessRank(value TrafficFreshness) int {
	switch value {
	case TrafficFreshnessCurrent:
		return 0
	case TrafficFreshnessUnverifiable:
		return 1
	case TrafficFreshnessStale:
		return 2
	default:
		return 3
	}
}

func trafficExtensionRank(value TrafficExtensionKind) int {
	switch value {
	case TrafficExtensionCircuitBreaker:
		return 0
	case TrafficExtensionRetry:
		return 1
	case TrafficExtensionTimeout:
		return 2
	case TrafficExtensionEnvoyPassthrough:
		return 3
	case TrafficExtensionIstioPassthrough:
		return 4
	default:
		return 5
	}
}

func trafficComparisonRank(value TrafficComparisonField) int {
	switch value {
	case TrafficComparisonAlgorithm:
		return 0
	case TrafficComparisonPolicy:
		return 1
	case TrafficComparisonCanaryWeight:
		return 2
	default:
		return 3
	}
}

func compareTrafficValueSource(a, b TrafficValueSource) int {
	return slices.Compare([]string{string(a.Evidence), string(a.Freshness)}, []string{string(b.Evidence), string(b.Freshness)})
}
