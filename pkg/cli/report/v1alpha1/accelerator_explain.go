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
	AcceleratorRequestsNotReported   AcceleratorRequestState = "NotReported"
	AcceleratorRequestsUnavailable   AcceleratorRequestState = "Unavailable"
	AcceleratorRequestsNotConfigured AcceleratorRequestState = "NotConfigured"
	AcceleratorRequestsInvalid       AcceleratorRequestState = "Invalid"
)

type AcceleratorBaseRequestState string

const (
	AcceleratorBaseRequestsAvailable   AcceleratorBaseRequestState = "Available"
	AcceleratorBaseRequestsUnavailable AcceleratorBaseRequestState = "Unavailable"
	AcceleratorBaseRequestsInvalid     AcceleratorBaseRequestState = "Invalid"
)

type AcceleratorExplainIssueCode string

const (
	AcceleratorIssueActiveConfigurationUnavailable AcceleratorExplainIssueCode = "ActiveConfigurationUnavailable"
	AcceleratorIssueActiveRevisionInconsistent     AcceleratorExplainIssueCode = "ActiveRevisionInconsistent"
	AcceleratorIssueBaseRequestsUnavailable        AcceleratorExplainIssueCode = "BaseRequestsUnavailable"
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
	AcceleratorIssueRequestsNotReported            AcceleratorExplainIssueCode = "RequestsNotReported"
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
	Generation        int64             `json:"generation,omitempty"`
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
	BaseState      AcceleratorBaseRequestState  `json:"baseState"`
	Base           []AcceleratorResourceRequest `json:"base"`
	EffectiveState AcceleratorRequestState      `json:"effectiveState"`
	Effective      []AcceleratorResourceRequest `json:"effective"`
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

func (r AcceleratorExplainReport) WideTable() report.Table {
	canonical := r.Canonical()
	table := canonical.Content.WideTable()
	for _, source := range canonical.Sources {
		evidence := string(source.Evidence)
		name := source.Name
		if source.Namespace != "" {
			name = source.Namespace + "/" + source.Name
		}
		table.Rows = append(table.Rows,
			[]string{"SOURCE", source.Kind, "NAME", name, evidence},
			[]string{
				"SOURCE", source.Kind, "GENERATION",
				optionalAcceleratorGeneration(source.Generation), evidence,
			},
			[]string{
				"SOURCE", source.Kind, "COLLECTED_AT",
				source.CollectedAt.Format(time.RFC3339Nano), evidence,
			},
		)
		if source.UnavailableReason != "" {
			table.Rows = append(table.Rows, []string{
				"SOURCE", source.Kind, "UNAVAILABLE_REASON",
				string(source.UnavailableReason), evidence,
			})
		}
	}
	for _, warning := range canonical.Warnings {
		table.Rows = append(table.Rows, []string{
			"WARNING", "-", "CODE", string(warning.Code), "Computed",
		})
	}
	return table
}

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
		left, right := result.Components[i], result.Components[j]
		if acceleratorComponentRank(left.Type) != acceleratorComponentRank(right.Type) {
			return acceleratorComponentRank(left.Type) < acceleratorComponentRank(right.Type)
		}
		if left.Type != right.Type {
			return left.Type < right.Type
		}
		return acceleratorComponentCanonicalKey(left) < acceleratorComponentCanonicalKey(right)
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
			compactAcceleratorRequests(component.Requests),
			strconv.Itoa(compactAcceleratorIssueCount(canonical, component)),
		})
	}
	return table
}

func (c AcceleratorExplainContent) WideTable() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{"SCOPE", "COMP", "FIELD", "VALUE", "EVIDENCE"}}
	table.Rows = append(table.Rows, []string{"SUMMARY", "-", "STATE", string(canonical.Summary.State), "Computed"})
	table.Rows = append(table.Rows, []string{
		"SUMMARY", "-", "STATUS_FRESHNESS",
		printers.OrDash(string(canonical.Summary.StatusFreshness)), "Computed",
	})
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
		table.Rows = append(table.Rows, []string{
			"SELECTION", comp, "REASON_STATE", string(component.Selection.Reason.State),
			reasonEvidence(component.Selection.Reason.State),
		})
		if component.Selection.Reason.Digest != "" {
			table.Rows = append(table.Rows, []string{
				"SELECTION", comp, "REASON_DIGEST", component.Selection.Reason.Digest,
				"Reported/Redacted",
			})
		}
		table.Rows = append(table.Rows, []string{
			"CLASS", comp, "STATE", string(component.Class.State), classEvidence(component.Class.State),
		})
		if component.Class.Name != "" {
			table.Rows = append(table.Rows, []string{
				"CLASS", comp, "NAME", component.Class.Name, classEvidence(component.Class.State),
			})
		}
		table.Rows = append(table.Rows, []string{
			"REQUEST", comp, "BASE_STATE", string(component.Requests.BaseState),
			baseRequestEvidence(component.Requests.BaseState),
		})
		if component.Requests.BaseState == AcceleratorBaseRequestsAvailable {
			table.Rows = append(table.Rows, []string{
				"REQUEST", comp, "BASE", joinAcceleratorRequests(component.Requests.Base), "Computed",
			})
		}
		table.Rows = append(table.Rows, []string{
			"REQUEST", comp, "EFFECTIVE_STATE", string(component.Requests.EffectiveState),
			requestEvidence(component.Requests.EffectiveState),
		})
		if component.Requests.EffectiveState == AcceleratorRequestsReported {
			table.Rows = append(table.Rows, []string{
				"REQUEST", comp, "EFFECTIVE", joinAcceleratorRequests(component.Requests.Effective), "Reported",
			})
		}
	}
	for _, issue := range allAcceleratorIssues(canonical) {
		table.Rows = append(table.Rows, []string{
			"ISSUE", printers.OrDash(string(issue.Component)), string(issue.Code), "-", "Computed",
		})
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
	switch requests.BaseState {
	case AcceleratorBaseRequestsUnavailable:
		return "base:Unknown"
	case AcceleratorBaseRequestsInvalid:
		return "base:Invalid"
	}
	if len(requests.Effective) > 0 {
		return compactAcceleratorRequestList(requests.Effective)
	}
	if len(requests.Base) > 0 {
		return printers.BoundedCell("base:"+compactAcceleratorRequestList(requests.Base), 18)
	}
	switch requests.EffectiveState {
	case AcceleratorRequestsNotConfigured:
		return "None"
	case AcceleratorRequestsNotReported, AcceleratorRequestsUnavailable:
		return "Unknown"
	default:
		return printers.OrDash(string(requests.EffectiveState))
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

func acceleratorComponentCanonicalKey(component AcceleratorExplainComponent) string {
	var key strings.Builder
	appendValue := func(value string) {
		key.WriteString(strconv.Itoa(len(value)))
		key.WriteByte(':')
		key.WriteString(value)
	}
	appendValue(string(component.Type))
	appendValue(string(component.Intent.State))
	appendValue(string(component.Intent.Policy))
	appendValue(string(component.Intent.PolicySource))
	appendValue(component.Intent.DeclaredClass)
	appendValue(string(component.Intent.ClassSource))
	appendValue(string(component.Selection.State))
	appendValue(component.Selection.Class)
	appendValue(string(component.Selection.Reason.State))
	appendValue(component.Selection.Reason.Digest)
	appendValue(string(component.Class.State))
	appendValue(component.Class.Name)
	appendValue(string(component.Requests.BaseState))
	// Counts frame the variable-length domains; individual string lengths
	// alone cannot distinguish request tokens from state or issue tokens.
	appendValue(strconv.Itoa(len(component.Requests.Base)))
	for _, request := range component.Requests.Base {
		appendValue(request.Name)
		appendValue(request.Quantity)
	}
	appendValue(string(component.Requests.EffectiveState))
	appendValue(strconv.Itoa(len(component.Requests.Effective)))
	for _, request := range component.Requests.Effective {
		appendValue(request.Name)
		appendValue(request.Quantity)
	}
	appendValue(strconv.Itoa(len(component.Issues)))
	for _, issue := range component.Issues {
		appendValue(string(issue))
	}
	return key.String()
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

func reasonEvidence(state AcceleratorReasonState) string {
	if state == AcceleratorReasonReported {
		return "Reported"
	}
	return "Unavailable"
}

func requestEvidence(state AcceleratorRequestState) string {
	switch state {
	case AcceleratorRequestsReported:
		return "Reported"
	case AcceleratorRequestsNotConfigured:
		return "Declared"
	default:
		return "Unavailable"
	}
}

func baseRequestEvidence(state AcceleratorBaseRequestState) string {
	if state == AcceleratorBaseRequestsAvailable {
		return "Computed"
	}
	return "Unavailable"
}

func optionalAcceleratorGeneration(generation int64) string {
	if generation == 0 {
		return "-"
	}
	return strconv.FormatInt(generation, 10)
}

func compactAcceleratorIssueCount(
	content AcceleratorExplainContent,
	component AcceleratorExplainComponent,
) int {
	knownComponents := make(map[RuntimeComponentType]struct{}, len(content.Components))
	for _, candidate := range content.Components {
		knownComponents[candidate.Type] = struct{}{}
	}
	keys := make(map[string]struct{}, len(component.Issues)+len(content.Issues))
	for _, code := range component.Issues {
		keys[string(component.Type)+"\x00"+string(code)] = struct{}{}
	}
	for _, issue := range content.Issues {
		_, represented := knownComponents[issue.Component]
		if issue.Component != component.Type && issue.Component != "" && represented {
			continue
		}
		keys[string(issue.Component)+"\x00"+string(issue.Code)] = struct{}{}
	}
	return len(keys)
}

func allAcceleratorIssues(content AcceleratorExplainContent) []AcceleratorExplainIssue {
	issues := append([]AcceleratorExplainIssue{}, content.Issues...)
	for _, component := range content.Components {
		for _, code := range component.Issues {
			issues = append(issues, AcceleratorExplainIssue{
				Code: code, Component: component.Type,
			})
		}
	}
	sort.Slice(issues, func(i, j int) bool {
		left, right := issues[i], issues[j]
		if acceleratorComponentRank(left.Component) != acceleratorComponentRank(right.Component) {
			return acceleratorComponentRank(left.Component) < acceleratorComponentRank(right.Component)
		}
		if left.Component != right.Component {
			return left.Component < right.Component
		}
		return left.Code < right.Code
	})
	return dedupeAcceleratorIssues(issues)
}

func acceleratorSourceKey(source AcceleratorSourceReference) string {
	return strings.Join([]string{
		source.Kind, source.Namespace, source.Name,
		strconv.FormatInt(source.Generation, 10), string(source.Evidence),
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
