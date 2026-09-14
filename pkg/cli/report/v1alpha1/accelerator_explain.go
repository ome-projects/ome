package v1alpha1

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const AcceleratorExplainReportKind = "AcceleratorExplainReport"

type AcceleratorExplainState string

const (
	AcceleratorExplainReported      AcceleratorExplainState = "Reported"
	AcceleratorExplainPartial       AcceleratorExplainState = "Partial"
	AcceleratorExplainNotConfigured AcceleratorExplainState = "NotConfigured"
	AcceleratorExplainInvalid       AcceleratorExplainState = "Invalid"
)

type AcceleratorIntentState string

const (
	AcceleratorIntentClass         AcceleratorIntentState = "Class"
	AcceleratorIntentPolicy        AcceleratorIntentState = "Policy"
	AcceleratorIntentNotConfigured AcceleratorIntentState = "NotConfigured"
	AcceleratorIntentInvalid       AcceleratorIntentState = "Invalid"
)

type AcceleratorPolicy string

const (
	AcceleratorPolicyBestFit        AcceleratorPolicy = "BestFit"
	AcceleratorPolicyCheapest       AcceleratorPolicy = "Cheapest"
	AcceleratorPolicyMostCapable    AcceleratorPolicy = "MostCapable"
	AcceleratorPolicyFirstAvailable AcceleratorPolicy = "FirstAvailable"
)

type AcceleratorSelectorSource string

const (
	AcceleratorSelectorSourceService   AcceleratorSelectorSource = "Service"
	AcceleratorSelectorSourceComponent AcceleratorSelectorSource = "Component"
)

type AcceleratorSelectionState string

const (
	AcceleratorSelectionReported      AcceleratorSelectionState = "Reported"
	AcceleratorSelectionNotReported   AcceleratorSelectionState = "NotReported"
	AcceleratorSelectionUnavailable   AcceleratorSelectionState = "Unavailable"
	AcceleratorSelectionInvalid       AcceleratorSelectionState = "Invalid"
	AcceleratorSelectionNotConfigured AcceleratorSelectionState = "NotConfigured"
)

type AcceleratorReasonState string

const (
	AcceleratorReasonReported    AcceleratorReasonState = "Reported"
	AcceleratorReasonNotReported AcceleratorReasonState = "NotReported"
	AcceleratorReasonUnavailable AcceleratorReasonState = "Unavailable"
	AcceleratorReasonInvalid     AcceleratorReasonState = "Invalid"
)

type AcceleratorClassState string

const (
	AcceleratorClassObserved       AcceleratorClassState = "Observed"
	AcceleratorClassNotRequested   AcceleratorClassState = "NotRequested"
	AcceleratorClassNotFound       AcceleratorClassState = "NotFound"
	AcceleratorClassForbidden      AcceleratorClassState = "Forbidden"
	AcceleratorClassUnsupportedAPI AcceleratorClassState = "UnsupportedAPI"
	AcceleratorClassUnreadable     AcceleratorClassState = "Unreadable"
	AcceleratorClassInvalid        AcceleratorClassState = "Invalid"
)

type AcceleratorRequestState string

const (
	AcceleratorRequestsReported      AcceleratorRequestState = "Reported"
	AcceleratorRequestsUnavailable   AcceleratorRequestState = "Unavailable"
	AcceleratorRequestsNotConfigured AcceleratorRequestState = "NotConfigured"
	AcceleratorRequestsInvalid       AcceleratorRequestState = "Invalid"
)

type AcceleratorExplainIssueCode string

const (
	AcceleratorIssueActiveConfigurationUnavailable AcceleratorExplainIssueCode = "ActiveConfigurationUnavailable"
	AcceleratorIssueActiveRevisionInconsistent     AcceleratorExplainIssueCode = "ActiveRevisionInconsistent"
	AcceleratorIssueClassForbidden                 AcceleratorExplainIssueCode = "ClassForbidden"
	AcceleratorIssueClassInvalid                   AcceleratorExplainIssueCode = "ClassInvalid"
	AcceleratorIssueClassNotFound                  AcceleratorExplainIssueCode = "ClassNotFound"
	AcceleratorIssueClassUnreadable                AcceleratorExplainIssueCode = "ClassUnreadable"
	AcceleratorIssueClassUnsupportedAPI            AcceleratorExplainIssueCode = "ClassUnsupportedAPI"
	AcceleratorIssueDeclaredClassInvalid           AcceleratorExplainIssueCode = "DeclaredClassInvalid"
	AcceleratorIssueNodeSelectorInvalid            AcceleratorExplainIssueCode = "NodeSelectorInvalid"
	AcceleratorIssuePolicyInvalid                  AcceleratorExplainIssueCode = "PolicyInvalid"
	AcceleratorIssueReportedClassMismatch          AcceleratorExplainIssueCode = "ReportedClassMismatch"
	AcceleratorIssueRequestsInvalid                AcceleratorExplainIssueCode = "RequestsInvalid"
	AcceleratorIssueSelectionNotReported           AcceleratorExplainIssueCode = "SelectionNotReported"
	AcceleratorIssueSelectionUnexpected            AcceleratorExplainIssueCode = "SelectionUnexpected"
	AcceleratorIssueStatusInvalid                  AcceleratorExplainIssueCode = "StatusInvalid"
	AcceleratorIssueStatusStale                    AcceleratorExplainIssueCode = "StatusStale"
	AcceleratorIssueStatusUnobserved               AcceleratorExplainIssueCode = "StatusUnobserved"
	AcceleratorIssueUnexpectedComponentEvidence    AcceleratorExplainIssueCode = "UnexpectedComponentEvidence"
)

type AcceleratorWarningCode string

const (
	AcceleratorWarningPartialData       AcceleratorWarningCode = "PartialData"
	AcceleratorWarningSourceUnavailable AcceleratorWarningCode = "SourceUnavailable"
	AcceleratorWarningStaleEvidence     AcceleratorWarningCode = "StaleEvidence"
)

type AcceleratorWarning struct {
	Code AcceleratorWarningCode `json:"code"`
}

type AcceleratorSourceReference struct {
	Kind              string            `json:"kind"`
	Namespace         string            `json:"namespace,omitempty"`
	Name              string            `json:"name"`
	UID               string            `json:"uid,omitempty"`
	Generation        int64             `json:"generation,omitempty"`
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	Evidence          EvidenceLevel     `json:"evidence"`
	CollectedAt       time.Time         `json:"collectedAt"`
	UnavailableReason UnavailableReason `json:"unavailableReason,omitempty"`
}

type AcceleratorExplainSummary struct {
	State           AcceleratorExplainState `json:"state"`
	StatusFreshness StatusFreshness         `json:"statusFreshness,omitempty"`
}

type AcceleratorIntent struct {
	State         AcceleratorIntentState    `json:"state"`
	Policy        AcceleratorPolicy         `json:"policy,omitempty"`
	PolicySource  AcceleratorSelectorSource `json:"policySource,omitempty"`
	DeclaredClass string                    `json:"declaredClass,omitempty"`
	ClassSource   AcceleratorSelectorSource `json:"classSource,omitempty"`
}

type AcceleratorReason struct {
	State  AcceleratorReasonState `json:"state"`
	Digest string                 `json:"digest,omitempty"`
}

type AcceleratorSelectionObservation struct {
	State  AcceleratorSelectionState `json:"state"`
	Class  string                    `json:"class,omitempty"`
	Reason AcceleratorReason         `json:"reason"`
}

type AcceleratorClassObservation struct {
	State AcceleratorClassState `json:"state"`
	Name  string                `json:"name,omitempty"`
}

type AcceleratorResourceRequest struct {
	Name     string `json:"name"`
	Quantity string `json:"quantity"`
}

type AcceleratorRequestObservation struct {
	State     AcceleratorRequestState      `json:"state"`
	Base      []AcceleratorResourceRequest `json:"base"`
	Effective []AcceleratorResourceRequest `json:"effective"`
}

type AcceleratorExplainComponent struct {
	Type      RuntimeComponentType            `json:"type"`
	Intent    AcceleratorIntent               `json:"intent"`
	Selection AcceleratorSelectionObservation `json:"selection"`
	Class     AcceleratorClassObservation     `json:"class"`
	Requests  AcceleratorRequestObservation   `json:"requests"`
	Issues    []AcceleratorExplainIssueCode   `json:"issues"`
}

type AcceleratorExplainIssue struct {
	Code      AcceleratorExplainIssueCode `json:"code"`
	Component RuntimeComponentType        `json:"component,omitempty"`
}

type AcceleratorExplainContent struct {
	Summary    AcceleratorExplainSummary     `json:"summary"`
	Components []AcceleratorExplainComponent `json:"components"`
	Issues     []AcceleratorExplainIssue     `json:"issues"`
}

type AcceleratorExplainReport struct {
	APIVersion  string                       `json:"apiVersion"`
	Kind        string                       `json:"kind"`
	Metadata    Metadata                     `json:"metadata"`
	CollectedAt time.Time                    `json:"collectedAt"`
	Sources     []AcceleratorSourceReference `json:"sources"`
	Content     AcceleratorExplainContent    `json:"content"`
	Warnings    []AcceleratorWarning         `json:"warnings"`
}

func NewAcceleratorExplainReport(
	metadata Metadata,
	content AcceleratorExplainContent,
	clock Clock,
) AcceleratorExplainReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (AcceleratorExplainReport{
		APIVersion: APIVersion, Kind: AcceleratorExplainReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []AcceleratorSourceReference{},
		Content: content, Warnings: []AcceleratorWarning{},
	}).Canonical()
}

func (r AcceleratorExplainReport) Canonical() AcceleratorExplainReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = AcceleratorExplainReportKind
	result.CollectedAt = r.CollectedAt.UTC()
	result.Sources = append([]AcceleratorSourceReference{}, r.Sources...)
	for i := range result.Sources {
		if result.Sources[i].CollectedAt.IsZero() {
			result.Sources[i].CollectedAt = result.CollectedAt
		} else {
			result.Sources[i].CollectedAt = result.Sources[i].CollectedAt.UTC()
		}
	}
	sort.Slice(result.Sources, func(i, j int) bool {
		return acceleratorSourceKey(result.Sources[i]) < acceleratorSourceKey(result.Sources[j])
	})
	result.Sources = dedupeAcceleratorSources(result.Sources)
	result.Content = r.Content.Canonical()
	result.Warnings = append([]AcceleratorWarning{}, r.Warnings...)
	sort.Slice(result.Warnings, func(i, j int) bool { return result.Warnings[i].Code < result.Warnings[j].Code })
	result.Warnings = dedupeAcceleratorWarnings(result.Warnings)
	return result
}

func (r AcceleratorExplainReport) Table() report.Table { return r.Canonical().Content.Table() }

func (r AcceleratorExplainReport) WideTable() report.Table { return r.Canonical().Content.WideTable() }

func (c AcceleratorExplainContent) Canonical() AcceleratorExplainContent {
	result := c
	result.Components = make([]AcceleratorExplainComponent, len(c.Components))
	for i := range c.Components {
		component := c.Components[i]
		component.Requests.Base = canonicalAcceleratorRequests(component.Requests.Base)
		component.Requests.Effective = canonicalAcceleratorRequests(component.Requests.Effective)
		component.Issues = append([]AcceleratorExplainIssueCode{}, component.Issues...)
		sort.Slice(component.Issues, func(i, j int) bool { return component.Issues[i] < component.Issues[j] })
		component.Issues = dedupeAcceleratorIssueCodes(component.Issues)
		result.Components[i] = component
	}
	sort.Slice(result.Components, func(i, j int) bool {
		return acceleratorComponentRank(result.Components[i].Type) < acceleratorComponentRank(result.Components[j].Type)
	})
	result.Issues = append([]AcceleratorExplainIssue{}, c.Issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		left, right := result.Issues[i], result.Issues[j]
		if acceleratorComponentRank(left.Component) != acceleratorComponentRank(right.Component) {
			return acceleratorComponentRank(left.Component) < acceleratorComponentRank(right.Component)
		}
		if left.Component != right.Component {
			return left.Component < right.Component
		}
		return left.Code < right.Code
	})
	result.Issues = dedupeAcceleratorIssues(result.Issues)
	return result
}

func (c AcceleratorExplainContent) Table() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{"COMP", "POLICY", "CLASS", "SEL", "REQUESTS", "ISS"}}
	for _, component := range canonical.Components {
		className := component.Selection.Class
		if className == "" {
			className = component.Intent.DeclaredClass
		}
		table.Rows = append(table.Rows, []string{
			string(component.Type), compactAcceleratorPolicy(component.Intent),
			printers.OrDash(printers.BoundedMiddleCell(className, 16)),
			compactAcceleratorSelection(component.Selection.State),
			compactAcceleratorRequests(component.Requests), strconv.Itoa(len(component.Issues)),
		})
	}
	return table
}

func (c AcceleratorExplainContent) WideTable() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{"SCOPE", "COMP", "FIELD", "VALUE", "EVIDENCE"}}
	table.Rows = append(table.Rows, []string{"SUMMARY", "-", "STATE", string(canonical.Summary.State), "Computed"})
	for _, component := range canonical.Components {
		comp := string(component.Type)
		table.Rows = append(table.Rows, []string{"INTENT", comp, "MODE", string(component.Intent.State), "Declared"})
		if component.Intent.Policy != "" {
			table.Rows = append(table.Rows, []string{
				"INTENT", comp, "POLICY", string(component.Intent.Policy),
				"Declared/" + string(component.Intent.PolicySource),
			})
		}
		if component.Intent.DeclaredClass != "" {
			table.Rows = append(table.Rows, []string{
				"INTENT", comp, "CLASS", component.Intent.DeclaredClass,
				"Declared/" + string(component.Intent.ClassSource),
			})
		}
		table.Rows = append(table.Rows,
			[]string{"SELECTION", comp, "STATE", string(component.Selection.State), selectionEvidence(component.Selection.State)},
		)
		if component.Selection.Class != "" {
			table.Rows = append(table.Rows, []string{"SELECTION", comp, "CLASS", component.Selection.Class, "Reported"})
		}
		if component.Selection.Reason.Digest != "" {
			table.Rows = append(table.Rows, []string{
				"SELECTION", comp, "REASON", component.Selection.Reason.Digest, "Reported/Redacted",
			})
		}
		table.Rows = append(table.Rows, []string{
			"CLASS", comp, "STATE", string(component.Class.State), classEvidence(component.Class.State),
		})
		if len(component.Requests.Base) > 0 {
			table.Rows = append(table.Rows, []string{
				"REQUEST", comp, "BASE", joinAcceleratorRequests(component.Requests.Base), "Computed",
			})
		}
		if len(component.Requests.Effective) > 0 || component.Requests.State == AcceleratorRequestsReported {
			table.Rows = append(table.Rows, []string{
				"REQUEST", comp, "EFFECTIVE", joinAcceleratorRequests(component.Requests.Effective), "Reported",
			})
		}
		for _, issue := range component.Issues {
			table.Rows = append(table.Rows, []string{"ISSUE", comp, string(issue), "-", "Computed"})
		}
	}
	return table
}

func compactAcceleratorPolicy(intent AcceleratorIntent) string {
	if intent.State == AcceleratorIntentClass {
		return "Explicit"
	}
	if intent.Policy == AcceleratorPolicyFirstAvailable {
		return "FirstAvail"
	}
	return printers.OrDash(string(intent.Policy))
}

func compactAcceleratorSelection(state AcceleratorSelectionState) string {
	switch state {
	case AcceleratorSelectionNotReported:
		return "Missing"
	case AcceleratorSelectionNotConfigured:
		return "None"
	case AcceleratorSelectionUnavailable:
		return "Unavail"
	default:
		return string(state)
	}
}

func compactAcceleratorRequests(requests AcceleratorRequestObservation) string {
	if len(requests.Effective) > 0 {
		return compactAcceleratorRequestList(requests.Effective)
	}
	if len(requests.Base) > 0 {
		return printers.BoundedCell("base:"+compactAcceleratorRequestList(requests.Base), 18)
	}
	switch requests.State {
	case AcceleratorRequestsNotConfigured:
		return "None"
	case AcceleratorRequestsUnavailable:
		return "Unknown"
	default:
		return printers.OrDash(string(requests.State))
	}
}

func compactAcceleratorRequestList(items []AcceleratorResourceRequest) string {
	if len(items) == 0 {
		return "None"
	}
	value := items[0].Name + "=" + items[0].Quantity
	if len(items) > 1 {
		value += ",+" + strconv.Itoa(len(items)-1)
	}
	return printers.BoundedCell(value, 18)
}

func joinAcceleratorRequests(items []AcceleratorResourceRequest) string {
	if len(items) == 0 {
		return "None"
	}
	parts := make([]string, len(items))
	for i := range items {
		parts[i] = items[i].Name + "=" + items[i].Quantity
	}
	return strings.Join(parts, ",")
}

func canonicalAcceleratorRequests(items []AcceleratorResourceRequest) []AcceleratorResourceRequest {
	result := append([]AcceleratorResourceRequest{}, items...)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].Quantity < result[j].Quantity
	})
	if result == nil {
		result = []AcceleratorResourceRequest{}
	}
	return result
}

func acceleratorComponentRank(component RuntimeComponentType) int {
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

func selectionEvidence(state AcceleratorSelectionState) string {
	if state == AcceleratorSelectionReported {
		return "Reported"
	}
	if state == AcceleratorSelectionNotConfigured {
		return "Declared"
	}
	return "Unavailable"
}

func classEvidence(state AcceleratorClassState) string {
	if state == AcceleratorClassObserved {
		return "Observed"
	}
	if state == AcceleratorClassNotRequested {
		return "-"
	}
	return "Unavailable"
}

func acceleratorSourceKey(source AcceleratorSourceReference) string {
	return strings.Join([]string{
		source.Kind, source.Namespace, source.Name, source.UID,
		strconv.FormatInt(source.Generation, 10), source.ResourceVersion,
		string(source.Evidence),
		source.CollectedAt.Format(time.RFC3339Nano), string(source.UnavailableReason),
	}, "\x00")
}

func dedupeAcceleratorSources(items []AcceleratorSourceReference) []AcceleratorSourceReference {
	return dedupeByKey(items, acceleratorSourceKey)
}

func dedupeAcceleratorWarnings(items []AcceleratorWarning) []AcceleratorWarning {
	return dedupeByKey(items, func(item AcceleratorWarning) string { return string(item.Code) })
}

func dedupeAcceleratorIssueCodes(items []AcceleratorExplainIssueCode) []AcceleratorExplainIssueCode {
	return dedupeByKey(items, func(item AcceleratorExplainIssueCode) string { return string(item) })
}

func dedupeAcceleratorIssues(items []AcceleratorExplainIssue) []AcceleratorExplainIssue {
	return dedupeByKey(items, func(item AcceleratorExplainIssue) string {
		return string(item.Component) + "\x00" + string(item.Code)
	})
}

func dedupeByKey[T any](items []T, key func(T) string) []T {
	if len(items) == 0 {
		return []T{}
	}
	result := make([]T, 0, len(items))
	last := ""
	for i, item := range items {
		current := key(item)
		if i == 0 || current != last {
			result = append(result, item)
		}
		last = current
	}
	return result
}
