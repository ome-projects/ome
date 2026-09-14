package v1alpha1

import (
	"cmp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/report"
)

const (
	// RolloutValidationReportKind identifies the rollout validation schema.
	RolloutValidationReportKind = "RolloutValidationReport"
	maxRolloutValidationChecks  = 16
	maxRolloutValidationIssues  = 32
	maxValidationIssueCellWidth = 38
)

// RolloutValidationState summarizes every deterministic and observable check.
type RolloutValidationState string

const (
	RolloutValidationValid        RolloutValidationState = "Valid"
	RolloutValidationInvalid      RolloutValidationState = "Invalid"
	RolloutValidationUnverifiable RolloutValidationState = "Unverifiable"
)

// RolloutValidationResult is the result of one bounded check.
type RolloutValidationResult string

const (
	RolloutValidationResultValid         RolloutValidationResult = "Valid"
	RolloutValidationResultInvalid       RolloutValidationResult = "Invalid"
	RolloutValidationResultUnverifiable  RolloutValidationResult = "Unverifiable"
	RolloutValidationResultNotApplicable RolloutValidationResult = "NotApplicable"
)

// RolloutValidationFreshness qualifies the generation of reported evidence.
type RolloutValidationFreshness string

const (
	RolloutValidationFreshnessCurrent       RolloutValidationFreshness = "Current"
	RolloutValidationFreshnessStale         RolloutValidationFreshness = "Stale"
	RolloutValidationFreshnessUnverifiable  RolloutValidationFreshness = "Unverifiable"
	RolloutValidationFreshnessNotApplicable RolloutValidationFreshness = "NotApplicable"
)

// RolloutValidationCheckName is the closed set of validation checks.
type RolloutValidationCheckName string

const (
	RolloutValidationCheckRolloutReferences    RolloutValidationCheckName = "RolloutReferences"
	RolloutValidationCheckRolloutPlan          RolloutValidationCheckName = "RolloutPlan"
	RolloutValidationCheckRolloutOrdering      RolloutValidationCheckName = "RolloutOrdering"
	RolloutValidationCheckRolloutResolution    RolloutValidationCheckName = "RolloutResolution"
	RolloutValidationCheckTrafficSpec          RolloutValidationCheckName = "TrafficSpec"
	RolloutValidationCheckTrafficReadiness     RolloutValidationCheckName = "TrafficReadiness"
	RolloutValidationCheckScalingPolicy        RolloutValidationCheckName = "ScalingPolicy"
	RolloutValidationCheckAutoscalerSpec       RolloutValidationCheckName = "AutoscalerSpec"
	RolloutValidationCheckAutoscalerResolution RolloutValidationCheckName = "AutoscalerResolution"
)

// RolloutValidationIssueCode is a stable, message-free diagnostic category.
type RolloutValidationIssueCode string

const (
	RolloutValidationIssueRolloutReferenceInvalid                RolloutValidationIssueCode = "RolloutReferenceInvalid"
	RolloutValidationIssueRolloutPlanInvalid                     RolloutValidationIssueCode = "RolloutPlanInvalid"
	RolloutValidationIssueRolloutOrderingInvalid                 RolloutValidationIssueCode = "RolloutOrderingInvalid"
	RolloutValidationIssueRolloutResolutionMissing               RolloutValidationIssueCode = "RolloutResolutionMissing"
	RolloutValidationIssueRolloutResolutionStale                 RolloutValidationIssueCode = "RolloutResolutionStale"
	RolloutValidationIssueRolloutResolutionMalformed             RolloutValidationIssueCode = "RolloutResolutionMalformed"
	RolloutValidationIssueRolloutResolutionFailed                RolloutValidationIssueCode = "RolloutResolutionFailed"
	RolloutValidationIssueRolloutResolutionFreshnessUnverifiable RolloutValidationIssueCode = "RolloutResolutionFreshnessUnverifiable"
	RolloutValidationIssueRolloutPrerequisiteUnverifiable        RolloutValidationIssueCode = "RolloutPrerequisiteUnverifiable"
	RolloutValidationIssueTrafficSpecInvalid                     RolloutValidationIssueCode = "TrafficSpecInvalid"
	RolloutValidationIssueTrafficAnnotationInvalid               RolloutValidationIssueCode = "TrafficAnnotationInvalid"
	RolloutValidationIssueTrafficEvidenceMissing                 RolloutValidationIssueCode = "TrafficEvidenceMissing"
	RolloutValidationIssueTrafficEvidenceStale                   RolloutValidationIssueCode = "TrafficEvidenceStale"
	RolloutValidationIssueTrafficEvidenceMalformed               RolloutValidationIssueCode = "TrafficEvidenceMalformed"
	RolloutValidationIssueTrafficNotReady                        RolloutValidationIssueCode = "TrafficNotReady"
	RolloutValidationIssueScalingPolicyInvalid                   RolloutValidationIssueCode = "ScalingPolicyInvalid"
	RolloutValidationIssueAutoscalerSpecInvalid                  RolloutValidationIssueCode = "AutoscalerSpecInvalid"
	RolloutValidationIssueAutoscalerReferenceInvalid             RolloutValidationIssueCode = "AutoscalerReferenceInvalid"
	RolloutValidationIssueAutoscalerEvidenceMissing              RolloutValidationIssueCode = "AutoscalerEvidenceMissing"
	RolloutValidationIssueAutoscalerEvidenceStale                RolloutValidationIssueCode = "AutoscalerEvidenceStale"
	RolloutValidationIssueAutoscalerEvidenceMalformed            RolloutValidationIssueCode = "AutoscalerEvidenceMalformed"
	RolloutValidationIssueAutoscalerResolutionFailed             RolloutValidationIssueCode = "AutoscalerResolutionFailed"
	RolloutValidationIssueChecksTruncated                        RolloutValidationIssueCode = "ChecksTruncated"
	RolloutValidationIssueIssuesTruncated                        RolloutValidationIssueCode = "IssuesTruncated"
	RolloutValidationIssueEvidenceMalformed                      RolloutValidationIssueCode = "EvidenceMalformed"
)

// RolloutValidationSummary carries the aggregate assertion result.
type RolloutValidationSummary struct {
	State RolloutValidationState `json:"state"`
}

// RolloutValidationCheck records one typed validation result.
type RolloutValidationCheck struct {
	Check     RolloutValidationCheckName `json:"check"`
	Component RuntimeComponentType       `json:"component,omitempty"`
	Result    RolloutValidationResult    `json:"result"`
	Evidence  EvidenceLevel              `json:"evidence"`
	Freshness RolloutValidationFreshness `json:"freshness"`
}

// RolloutValidationIssue scopes one code to a check and optional component.
type RolloutValidationIssue struct {
	Code      RolloutValidationIssueCode `json:"code"`
	Check     RolloutValidationCheckName `json:"check,omitempty"`
	Component RuntimeComponentType       `json:"component,omitempty"`
}

// RolloutValidationContent is shared by terminal and machine output.
type RolloutValidationContent struct {
	Summary RolloutValidationSummary `json:"summary"`
	Checks  []RolloutValidationCheck `json:"checks"`
	Issues  []RolloutValidationIssue `json:"issues"`
}

// RolloutValidationReport is a read-only, identity-bound diagnostic report.
type RolloutValidationReport struct {
	APIVersion  string                   `json:"apiVersion"`
	Kind        string                   `json:"kind"`
	Metadata    Metadata                 `json:"metadata"`
	CollectedAt time.Time                `json:"collectedAt"`
	Sources     []RolloutSourceReference `json:"sources"`
	Content     RolloutValidationContent `json:"content"`
	Warnings    []RolloutWarning         `json:"warnings"`
}

// NewRolloutValidationReport builds a canonical report with an injectable clock.
func NewRolloutValidationReport(
	metadata Metadata,
	content RolloutValidationContent,
	clock Clock,
) RolloutValidationReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (RolloutValidationReport{
		APIVersion: APIVersion, Kind: RolloutValidationReportKind,
		Metadata: metadata, CollectedAt: clock.Now().UTC(),
		Sources: []RolloutSourceReference{}, Content: content,
		Warnings: []RolloutWarning{},
	}).Canonical()
}

// Canonical returns a bounded, deeply copied, deterministically ordered report.
func (r RolloutValidationReport) Canonical() RolloutValidationReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = RolloutValidationReportKind
	result.CollectedAt = r.CollectedAt.UTC()
	result.Sources = append([]RolloutSourceReference{}, r.Sources...)
	for i := range result.Sources {
		result.Sources[i].Kind = canonicalRolloutSourceKind(result.Sources[i].Kind)
		result.Sources[i].Evidence = canonicalRolloutEvidenceLevel(result.Sources[i].Evidence)
		if result.Sources[i].CollectedAt.IsZero() {
			result.Sources[i].CollectedAt = result.CollectedAt
		} else {
			result.Sources[i].CollectedAt = result.Sources[i].CollectedAt.UTC()
		}
	}
	sort.SliceStable(result.Sources, func(i, j int) bool {
		a, b := result.Sources[i], result.Sources[j]
		return cmp.Or(
			cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Namespace, b.Namespace),
			cmp.Compare(a.Name, b.Name), cmp.Compare(a.UID, b.UID),
			cmp.Compare(a.Generation, b.Generation), cmp.Compare(a.Evidence, b.Evidence),
		) < 0 || (a.Kind == b.Kind && a.Namespace == b.Namespace && a.Name == b.Name &&
			a.UID == b.UID && a.Generation == b.Generation && a.Evidence == b.Evidence &&
			a.CollectedAt.Before(b.CollectedAt))
	})
	result.Warnings = append([]RolloutWarning{}, r.Warnings...)
	for i := range result.Warnings {
		result.Warnings[i].Code = canonicalRolloutWarningCode(result.Warnings[i].Code)
	}
	sort.Slice(result.Warnings, func(i, j int) bool { return result.Warnings[i].Code < result.Warnings[j].Code })
	result.Warnings = slices.Compact(result.Warnings)
	result.Content = r.Content.Canonical()
	return result
}

// Canonical returns a bounded, deeply copied, deterministically ordered body.
func (c RolloutValidationContent) Canonical() RolloutValidationContent {
	result := c
	result.Summary.State = canonicalRolloutValidationState(c.Summary.State)
	result.Checks = make([]RolloutValidationCheck, 0, min(len(c.Checks), maxRolloutValidationChecks))
	malformed := false
	for _, check := range c.Checks {
		check.Check = canonicalRolloutValidationCheck(check.Check)
		if check.Check == "" {
			malformed = true
			continue
		}
		check.Component = canonicalRolloutComponentType(check.Component)
		check.Result = canonicalRolloutValidationResult(check.Result)
		check.Evidence = canonicalRolloutEvidenceLevel(check.Evidence)
		check.Freshness = canonicalRolloutValidationFreshness(check.Freshness)
		result.Checks = append(result.Checks, check)
	}
	sort.SliceStable(result.Checks, func(i, j int) bool {
		return compareRolloutValidationChecks(result.Checks[i], result.Checks[j]) < 0
	})
	checksTruncated := len(result.Checks) > maxRolloutValidationChecks
	if checksTruncated {
		result.Checks = result.Checks[:maxRolloutValidationChecks]
	}

	result.Issues = make([]RolloutValidationIssue, 0, len(c.Issues)+3)
	for _, issue := range c.Issues {
		issue.Code = canonicalRolloutValidationIssueCode(issue.Code)
		issue.Check = canonicalRolloutValidationCheck(issue.Check)
		issue.Component = canonicalRolloutComponentType(issue.Component)
		result.Issues = append(result.Issues, issue)
	}
	if malformed {
		result.Issues = append(result.Issues, RolloutValidationIssue{Code: RolloutValidationIssueEvidenceMalformed})
	}
	if checksTruncated {
		result.Issues = append(result.Issues, RolloutValidationIssue{Code: RolloutValidationIssueChecksTruncated})
	}
	sort.Slice(result.Issues, func(i, j int) bool {
		return compareRolloutValidationIssues(result.Issues[i], result.Issues[j]) < 0
	})
	result.Issues = slices.Compact(result.Issues)
	if len(result.Issues) > maxRolloutValidationIssues {
		result.Issues = slices.DeleteFunc(result.Issues, func(issue RolloutValidationIssue) bool {
			return issue == (RolloutValidationIssue{Code: RolloutValidationIssueIssuesTruncated})
		})
		result.Issues = result.Issues[:min(len(result.Issues), maxRolloutValidationIssues-1)]
		result.Issues = append(result.Issues, RolloutValidationIssue{Code: RolloutValidationIssueIssuesTruncated})
		sort.Slice(result.Issues, func(i, j int) bool {
			return compareRolloutValidationIssues(result.Issues[i], result.Issues[j]) < 0
		})
	}
	return result
}

// Table derives the compact human view from typed content.
func (r RolloutValidationReport) Table() report.Table { return r.Content.Table() }

// WideTable includes bounded issue codes for troubleshooting.
func (r RolloutValidationReport) WideTable() report.Table { return r.Content.WideTable() }

// Table returns a terminal-safe result matrix containing only closed values.
func (c RolloutValidationContent) Table() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{"CHECK", "COMP", "RESULT", "SOURCE"}}
	table.Rows = append(table.Rows, []string{
		"OVERALL", "-", string(canonical.Summary.State), "Computed/Current",
	})
	for _, check := range canonical.Checks {
		table.Rows = append(table.Rows, []string{
			validationCheckLabel(check.Check), orDash(string(check.Component)), string(check.Result),
			string(check.Evidence) + "/" + string(check.Freshness),
		})
	}
	return table
}

// WideTable returns one row per check with scoped stable issue codes.
func (c RolloutValidationContent) WideTable() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{"CHECK", "COMP", "RESULT", "EVIDENCE", "FRESHNESS", "ISSUES"}}
	table.Rows = append(table.Rows, []string{
		"OVERALL", "-", string(canonical.Summary.State), "Computed", "Current",
		validationIssueCell(canonical.Issues, "", ""),
	})
	for _, check := range canonical.Checks {
		table.Rows = append(table.Rows, []string{
			validationCheckLabel(check.Check), orDash(string(check.Component)), string(check.Result),
			string(check.Evidence), string(check.Freshness),
			validationIssueCell(canonical.Issues, check.Check, check.Component),
		})
	}
	return table
}

func compareRolloutValidationChecks(a, b RolloutValidationCheck) int {
	return cmp.Or(
		cmp.Compare(validationCheckRank(a.Check), validationCheckRank(b.Check)),
		cmp.Compare(validationComponentRank(a.Component), validationComponentRank(b.Component)),
		cmp.Compare(a.Component, b.Component), cmp.Compare(a.Result, b.Result),
		cmp.Compare(a.Evidence, b.Evidence), cmp.Compare(a.Freshness, b.Freshness),
	)
}

func compareRolloutValidationIssues(a, b RolloutValidationIssue) int {
	return cmp.Or(
		cmp.Compare(a.Code, b.Code),
		cmp.Compare(validationCheckRank(a.Check), validationCheckRank(b.Check)),
		cmp.Compare(validationComponentRank(a.Component), validationComponentRank(b.Component)),
		cmp.Compare(a.Component, b.Component),
	)
}

func validationIssueCell(
	issues []RolloutValidationIssue,
	check RolloutValidationCheckName,
	component RuntimeComponentType,
) string {
	values := make([]string, 0)
	for _, issue := range issues {
		if issue.Check == check && issue.Component == component {
			values = append(values, string(issue.Code))
		}
	}
	if len(values) == 0 {
		return "-"
	}
	joined := strings.Join(values, ",")
	if len(joined) <= maxValidationIssueCellWidth {
		return joined
	}
	return values[0] + ",+" + strconv.Itoa(len(values)-1)
}

func validationCheckRank(value RolloutValidationCheckName) int {
	switch value {
	case RolloutValidationCheckRolloutReferences:
		return 0
	case RolloutValidationCheckRolloutPlan:
		return 1
	case RolloutValidationCheckRolloutOrdering:
		return 2
	case RolloutValidationCheckRolloutResolution:
		return 3
	case RolloutValidationCheckTrafficSpec:
		return 4
	case RolloutValidationCheckTrafficReadiness:
		return 5
	case RolloutValidationCheckScalingPolicy:
		return 6
	case RolloutValidationCheckAutoscalerSpec:
		return 7
	case RolloutValidationCheckAutoscalerResolution:
		return 8
	default:
		return 9
	}
}

func validationComponentRank(value RuntimeComponentType) int {
	if value == "" {
		return -1
	}
	return componentOrder(value)
}

func validationCheckLabel(value RolloutValidationCheckName) string {
	switch value {
	case RolloutValidationCheckRolloutReferences:
		return "ROLLOUT-REFS"
	case RolloutValidationCheckRolloutPlan:
		return "ROLLOUT-PLAN"
	case RolloutValidationCheckRolloutOrdering:
		return "ROLLOUT-ORDER"
	case RolloutValidationCheckRolloutResolution:
		return "ROLLOUT-RESOLVE"
	case RolloutValidationCheckTrafficSpec:
		return "TRAFFIC-SPEC"
	case RolloutValidationCheckTrafficReadiness:
		return "TRAFFIC-READY"
	case RolloutValidationCheckScalingPolicy:
		return "SCALING-POLICY"
	case RolloutValidationCheckAutoscalerSpec:
		return "AUTOSCALER-SPEC"
	case RolloutValidationCheckAutoscalerResolution:
		return "AUTOSCALER-RESOLVE"
	default:
		return "UNKNOWN"
	}
}

func canonicalRolloutValidationState(value RolloutValidationState) RolloutValidationState {
	switch value {
	case RolloutValidationValid, RolloutValidationInvalid, RolloutValidationUnverifiable:
		return value
	default:
		return RolloutValidationUnverifiable
	}
}

func canonicalRolloutValidationResult(value RolloutValidationResult) RolloutValidationResult {
	switch value {
	case RolloutValidationResultValid, RolloutValidationResultInvalid,
		RolloutValidationResultUnverifiable, RolloutValidationResultNotApplicable:
		return value
	default:
		return RolloutValidationResultUnverifiable
	}
}

func canonicalRolloutValidationFreshness(value RolloutValidationFreshness) RolloutValidationFreshness {
	switch value {
	case RolloutValidationFreshnessCurrent, RolloutValidationFreshnessStale,
		RolloutValidationFreshnessUnverifiable, RolloutValidationFreshnessNotApplicable:
		return value
	default:
		return RolloutValidationFreshnessUnverifiable
	}
}

func canonicalRolloutValidationCheck(value RolloutValidationCheckName) RolloutValidationCheckName {
	switch value {
	case RolloutValidationCheckRolloutReferences, RolloutValidationCheckRolloutPlan,
		RolloutValidationCheckRolloutOrdering, RolloutValidationCheckRolloutResolution,
		RolloutValidationCheckTrafficSpec, RolloutValidationCheckTrafficReadiness,
		RolloutValidationCheckScalingPolicy, RolloutValidationCheckAutoscalerSpec,
		RolloutValidationCheckAutoscalerResolution:
		return value
	default:
		return ""
	}
}

func canonicalRolloutValidationIssueCode(value RolloutValidationIssueCode) RolloutValidationIssueCode {
	switch value {
	case RolloutValidationIssueRolloutReferenceInvalid, RolloutValidationIssueRolloutPlanInvalid,
		RolloutValidationIssueRolloutOrderingInvalid, RolloutValidationIssueRolloutResolutionMissing,
		RolloutValidationIssueRolloutResolutionStale, RolloutValidationIssueRolloutResolutionMalformed,
		RolloutValidationIssueRolloutResolutionFailed,
		RolloutValidationIssueRolloutResolutionFreshnessUnverifiable,
		RolloutValidationIssueRolloutPrerequisiteUnverifiable,
		RolloutValidationIssueTrafficSpecInvalid,
		RolloutValidationIssueTrafficAnnotationInvalid, RolloutValidationIssueTrafficEvidenceMissing,
		RolloutValidationIssueTrafficEvidenceStale, RolloutValidationIssueTrafficEvidenceMalformed,
		RolloutValidationIssueTrafficNotReady, RolloutValidationIssueScalingPolicyInvalid,
		RolloutValidationIssueAutoscalerSpecInvalid, RolloutValidationIssueAutoscalerReferenceInvalid,
		RolloutValidationIssueAutoscalerEvidenceMissing, RolloutValidationIssueAutoscalerEvidenceStale,
		RolloutValidationIssueAutoscalerEvidenceMalformed, RolloutValidationIssueAutoscalerResolutionFailed,
		RolloutValidationIssueChecksTruncated, RolloutValidationIssueIssuesTruncated,
		RolloutValidationIssueEvidenceMalformed:
		return value
	default:
		return RolloutValidationIssueEvidenceMalformed
	}
}
