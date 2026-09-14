package v1alpha1

import (
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/cli/report"
)

const RolloutExplainReportKind = "RolloutExplainReport"

var rolloutPortableDigestPattern = regexp.MustCompile(`^rp1:[0-9a-f]{12}$`)

type RolloutPlanMode string

const (
	RolloutPlanModeUnknown RolloutPlanMode = "Unknown"
	RolloutPlanModeLive    RolloutPlanMode = "Live"
	RolloutPlanModePinned  RolloutPlanMode = "Pinned"
)

type RolloutPlanReason string

const (
	RolloutPlanReasonUnknown             RolloutPlanReason = "Unknown"
	RolloutPlanReasonPinned              RolloutPlanReason = "Pinned"
	RolloutPlanReasonNoActiveRun         RolloutPlanReason = "NoActiveRun"
	RolloutPlanReasonPolicyNotFound      RolloutPlanReason = "PolicyNotFound"
	RolloutPlanReasonPolicyNotReady      RolloutPlanReason = "PolicyNotReady"
	RolloutPlanReasonProgressionMismatch RolloutPlanReason = "ProgressionMismatch"
	RolloutPlanReasonPlanInvalid         RolloutPlanReason = "PlanInvalid"
	RolloutPlanReasonProviderUnbound     RolloutPlanReason = "ProviderUnbound"
	RolloutPlanReasonInSync              RolloutPlanReason = "InSync"
	RolloutPlanReasonPolicyNewer         RolloutPlanReason = "PolicyNewerThanRun"
	RolloutPlanReasonSpecNewer           RolloutPlanReason = "SpecNewerThanRun"
)

type RolloutPlanView string

const (
	RolloutPlanViewDeclared  RolloutPlanView = "Declared"
	RolloutPlanViewLive      RolloutPlanView = "Live"
	RolloutPlanViewEffective RolloutPlanView = "Effective"
)

type RolloutPlanSource string

const (
	RolloutPlanSourceUnknown   RolloutPlanSource = "Unknown"
	RolloutPlanSourceInline    RolloutPlanSource = "Inline"
	RolloutPlanSourcePolicy    RolloutPlanSource = "Policy"
	RolloutPlanSourceDefaulted RolloutPlanSource = "Defaulted"
)

// RolloutProgressionOrigin describes how the progression body came to exist.
// Unknown is intentional for pinned inline blue-green bodies: the controller
// materializes the default into the pinned plan, so status cannot distinguish
// an explicitly authored empty blueGreen block from an omitted progression.
type RolloutProgressionOrigin string

const (
	RolloutProgressionOriginUnknown    RolloutProgressionOrigin = "Unknown"
	RolloutProgressionOriginConfigured RolloutProgressionOrigin = "Configured"
	RolloutProgressionOriginDefaulted  RolloutProgressionOrigin = "Defaulted"
	RolloutProgressionOriginPolicy     RolloutProgressionOrigin = "Policy"
)

type RolloutSettingSource string

const (
	RolloutSettingConfigured    RolloutSettingSource = "Configured"
	RolloutSettingDefaulted     RolloutSettingSource = "Defaulted"
	RolloutSettingOperatorValue RolloutSettingSource = "OperatorDefault"
	RolloutSettingUnresolved    RolloutSettingSource = "Unresolved"
)

type RolloutSettingEffect string

const (
	RolloutSettingEffectUnknown                RolloutSettingEffect = "Unknown"
	RolloutSettingEffectApplied                RolloutSettingEffect = "Applied"
	RolloutSettingEffectIgnoredFinalGroup      RolloutSettingEffect = "IgnoredFinalGroup"
	RolloutSettingEffectIgnoredSingleComponent RolloutSettingEffect = "IgnoredSingleComponent"
	RolloutSettingEffectIgnoredPlanShape       RolloutSettingEffect = "IgnoredPlanShape"
)

type RolloutPlanSelection struct {
	Mode     RolloutPlanMode `json:"mode"`
	Evidence EvidenceLevel   `json:"evidence"`
}

type RolloutPlanCondition struct {
	State    RolloutConditionState `json:"state"`
	Reason   RolloutPlanReason     `json:"reason,omitempty"`
	Evidence EvidenceLevel         `json:"evidence"`
}

type RolloutExplainSummary struct {
	EffectivePlan RolloutPlanSelection `json:"effectivePlan"`
	PlanReady     RolloutPlanCondition `json:"planReady"`
	PlanDrift     RolloutPlanCondition `json:"planDrift"`
}

type RolloutPolicyReference struct {
	Kind        string        `json:"kind"`
	Name        string        `json:"name"`
	Progression string        `json:"progression"`
	Generation  int64         `json:"generation,omitempty"`
	Digest      string        `json:"digest,omitempty"`
	Evidence    EvidenceLevel `json:"evidence"`
}

type RolloutSetting struct {
	Value  string               `json:"value,omitempty"`
	Source RolloutSettingSource `json:"source"`
	Effect RolloutSettingEffect `json:"effect"`
}

type RolloutRollingUpdateSettings struct {
	MaxSurge       RolloutSetting `json:"maxSurge"`
	MaxUnavailable RolloutSetting `json:"maxUnavailable"`
}

type RolloutMaintainRatioSettings struct {
	Tolerance RolloutSetting `json:"tolerance"`
}

type RolloutPlanAnalysis struct {
	MetricCount    int32  `json:"metricCount"`
	Interval       string `json:"interval"`
	InitialDelay   string `json:"initialDelay,omitempty"`
	FailureLimit   int32  `json:"failureLimit"`
	OnInconclusive string `json:"onInconclusive"`
}

type RolloutPlanStep struct {
	Index    int32                `json:"index"`
	Capacity string               `json:"capacity"`
	Traffic  int32                `json:"traffic"`
	Gate     RolloutGate          `json:"gate"`
	Pause    string               `json:"pause,omitempty"`
	Analysis *RolloutPlanAnalysis `json:"analysis,omitempty"`
}

type RolloutPlanGroup struct {
	Index             int                           `json:"index"`
	Evidence          EvidenceLevel                 `json:"evidence"`
	Source            RolloutPlanSource             `json:"source"`
	Strategy          RolloutStrategy               `json:"strategy"`
	ProgressionOrigin RolloutProgressionOrigin      `json:"progressionOrigin"`
	Defaulted         bool                          `json:"defaulted"`
	Components        []RuntimeComponentType        `json:"components"`
	Order             []RuntimeComponentType        `json:"order"`
	Policy            *RolloutPolicyReference       `json:"policy,omitempty"`
	ShadowedPolicy    *RolloutPolicyReference       `json:"shadowedPolicy,omitempty"`
	PortableDigest    string                        `json:"portableDigest,omitempty"`
	Soak              *RolloutSetting               `json:"soak,omitempty"`
	RollingUpdate     *RolloutRollingUpdateSettings `json:"rollingUpdate,omitempty"`
	MaintainRatio     *RolloutMaintainRatioSettings `json:"maintainRatio,omitempty"`
	Steps             []RolloutPlanStep             `json:"steps"`
}

type RolloutExplainHoldKind string

const (
	RolloutHoldUnknown        RolloutExplainHoldKind = "Unknown"
	RolloutHoldGlobalPause    RolloutExplainHoldKind = "GlobalPause"
	RolloutHoldPlanParked     RolloutExplainHoldKind = "PlanParked"
	RolloutHoldCanaryPreStep  RolloutExplainHoldKind = "CanaryPreStep"
	RolloutHoldManualGate     RolloutExplainHoldKind = "ManualGate"
	RolloutHoldTimedGate      RolloutExplainHoldKind = "TimedGate"
	RolloutHoldAnalysisGate   RolloutExplainHoldKind = "AnalysisGate"
	RolloutHoldObservedPaused RolloutExplainHoldKind = "ObservedPaused"
)

type RolloutExplainHold struct {
	Kind      RolloutExplainHoldKind `json:"kind"`
	Evidence  EvidenceLevel          `json:"evidence"`
	Group     *int                   `json:"group,omitempty"`
	Step      *int32                 `json:"step,omitempty"`
	Component RuntimeComponentType   `json:"component,omitempty"`
}

type RolloutExplainIssueCode string

const (
	RolloutExplainIssueUnknown                RolloutExplainIssueCode = "Unknown"
	RolloutExplainIssueDeclaredPlanMalformed  RolloutExplainIssueCode = "DeclaredPlanMalformed"
	RolloutExplainIssueLivePlanMalformed      RolloutExplainIssueCode = "LivePlanMalformed"
	RolloutExplainIssueEffectivePlanMalformed RolloutExplainIssueCode = "EffectivePlanMalformed"
	RolloutExplainIssueActiveRunMalformed     RolloutExplainIssueCode = "ActiveRunMalformed"
	RolloutExplainIssuePlanConditionMalformed RolloutExplainIssueCode = "PlanConditionMalformed"
	RolloutExplainIssueResolutionMissing      RolloutExplainIssueCode = "ResolutionMissing"
	RolloutExplainIssueResolutionMalformed    RolloutExplainIssueCode = "ResolutionMalformed"
	RolloutExplainIssuePolicyBodyUnavailable  RolloutExplainIssueCode = "PolicyBodyUnavailable"
	RolloutExplainIssuePlanTruncated          RolloutExplainIssueCode = "PlanTruncated"
)

type RolloutExplainIssue struct {
	Code  RolloutExplainIssueCode `json:"code"`
	View  RolloutPlanView         `json:"view,omitempty"`
	Group *int                    `json:"group,omitempty"`
}

type RolloutExplainContent struct {
	Summary         RolloutExplainSummary `json:"summary"`
	DeclaredGroups  []RolloutPlanGroup    `json:"declaredGroups"`
	LiveGroups      []RolloutPlanGroup    `json:"liveGroups"`
	EffectiveGroups []RolloutPlanGroup    `json:"effectiveGroups"`
	Observed        RolloutStatusContent  `json:"observed"`
	Holds           []RolloutExplainHold  `json:"holds"`
	Issues          []RolloutExplainIssue `json:"issues"`
}

type RolloutExplainReport struct {
	APIVersion  string                   `json:"apiVersion"`
	Kind        string                   `json:"kind"`
	Metadata    Metadata                 `json:"metadata"`
	CollectedAt time.Time                `json:"collectedAt"`
	Sources     []RolloutSourceReference `json:"sources"`
	Content     RolloutExplainContent    `json:"content"`
	Warnings    []RolloutWarning         `json:"warnings"`
}

func NewRolloutExplainReport(metadata Metadata, content RolloutExplainContent, clock Clock) RolloutExplainReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (RolloutExplainReport{
		APIVersion: APIVersion, Kind: RolloutExplainReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []RolloutSourceReference{},
		Content: content, Warnings: []RolloutWarning{},
	}).Canonical()
}

func (r RolloutExplainReport) Canonical() RolloutExplainReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = RolloutExplainReportKind
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
	sort.Slice(result.Sources, func(i, j int) bool {
		return compareRolloutExplainSources(result.Sources[i], result.Sources[j]) < 0
	})
	result.Content = r.Content.Canonical()
	result.Warnings = append([]RolloutWarning{}, r.Warnings...)
	for i := range result.Warnings {
		result.Warnings[i].Code = canonicalRolloutWarningCode(result.Warnings[i].Code)
	}
	sort.Slice(result.Warnings, func(i, j int) bool { return result.Warnings[i].Code < result.Warnings[j].Code })
	result.Warnings = slices.Compact(result.Warnings)
	return result
}

func (r RolloutExplainReport) Table() report.Table { return r.Canonical().Content.Table() }

// WideTable derives the complete legacy operator view from the report's typed
// content. It remains separate from the default outline so machine formats and
// detailed troubleshooting output stay stable.
func (r RolloutExplainReport) WideTable() report.Table {
	return r.Canonical().Content.WideTable()
}

func (c RolloutExplainContent) Canonical() RolloutExplainContent {
	result := c
	result.Summary.EffectivePlan.Mode = canonicalRolloutPlanMode(c.Summary.EffectivePlan.Mode)
	result.Summary.EffectivePlan.Evidence = canonicalRolloutEvidenceLevel(c.Summary.EffectivePlan.Evidence)
	result.Summary.PlanReady = canonicalRolloutPlanCondition(c.Summary.PlanReady)
	result.Summary.PlanDrift = canonicalRolloutPlanCondition(c.Summary.PlanDrift)
	result.DeclaredGroups = canonicalRolloutPlanGroups(c.DeclaredGroups)
	result.LiveGroups = canonicalRolloutPlanGroups(c.LiveGroups)
	result.EffectiveGroups = canonicalRolloutPlanGroups(c.EffectiveGroups)
	result.Observed = c.Observed.Canonical()
	result.Holds = canonicalRolloutExplainHolds(c.Holds)
	result.Issues = canonicalRolloutExplainIssues(c.Issues)
	return result
}

func (c RolloutExplainContent) Table() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{"VIEW", "ITEM", "DETAIL"}}
	for _, view := range []struct {
		name   RolloutPlanView
		groups []RolloutPlanGroup
	}{
		{name: RolloutPlanViewDeclared, groups: canonical.DeclaredGroups},
		{name: RolloutPlanViewLive, groups: canonical.LiveGroups},
		{name: RolloutPlanViewEffective, groups: canonical.EffectiveGroups},
	} {
		rows := compactRolloutPlanRows(canonical, view.name, view.groups)
		rows[0][0] = string(view.name)
		table.Rows = append(table.Rows, rows...)
	}
	return table
}

func compactRolloutPlanRows(
	content RolloutExplainContent,
	view RolloutPlanView,
	groups []RolloutPlanGroup,
) [][]string {
	plan := fmt.Sprintf("groups=%d", len(groups))
	if view == RolloutPlanViewLive {
		plan = fmt.Sprintf("mode=%s; %s", RolloutPlanModeLive, plan)
	}
	if view == RolloutPlanViewEffective {
		plan = fmt.Sprintf(
			"mode=%s; evidence=%s; %s",
			content.Summary.EffectivePlan.Mode,
			content.Summary.EffectivePlan.Evidence,
			plan,
		)
	}
	rows := [][]string{{"", "PLAN", plan}}
	if view == RolloutPlanViewEffective {
		rows = append(rows,
			[]string{"", "READY", compactRolloutPlanCondition(content.Summary.PlanReady)},
			[]string{"", "DRIFT", compactRolloutPlanCondition(content.Summary.PlanDrift)},
		)
		rows = append(rows, compactRolloutHoldRows(content.Holds, nil, nil)...)
	}
	for _, group := range groups {
		rows = append(rows, compactRolloutGroupRows(content, view, group)...)
	}
	if view == RolloutPlanViewEffective {
		for _, component := range content.Observed.Components {
			if component.Group == nil {
				rows = append(rows, compactObservedComponentRows(content, component)...)
			}
		}
	}
	if issues := compactRolloutPlanIssues(content, view); len(issues) > 0 {
		rows = append(rows, []string{"", "ISSUES", strings.Join(issues, ",")})
	}
	return rows
}

func compactRolloutPlanCondition(condition RolloutPlanCondition) string {
	return fmt.Sprintf(
		"%s; evidence=%s",
		rolloutPlanConditionCell(condition),
		orDash(string(condition.Evidence)),
	)
}

func compactRolloutGroupRows(
	content RolloutExplainContent,
	view RolloutPlanView,
	group RolloutPlanGroup,
) [][]string {
	rows := [][]string{{
		"",
		fmt.Sprintf("GROUP %d", group.Index),
		fmt.Sprintf(
			"%s; components=%s; source=%s/%s",
			group.Strategy,
			rolloutComponentsCell(group.Components),
			group.Evidence,
			group.Source,
		),
	}}
	config := rolloutPlanConfigParts(group)
	for index, value := range config {
		item := ""
		if index == 0 {
			item = "CONFIG"
		}
		rows = append(rows, []string{"", item, value})
	}
	if view == RolloutPlanViewEffective {
		groupIndex := group.Index
		rows = append(rows, compactRolloutHoldRows(content.Holds, &groupIndex, nil)...)
		phase, sequence, revisions, traffic := observedGroupCells(content.Observed, group)
		for _, value := range []struct{ item, detail string }{
			{item: "PHASE", detail: phase},
			{item: "SEQUENCE", detail: sequence},
			{item: "REVISIONS", detail: revisions},
			{item: "TRAFFIC", detail: traffic},
		} {
			if value.detail != "-" {
				rows = append(rows, []string{"", value.item, value.detail})
			}
		}
	}
	for _, step := range group.Steps {
		rows = append(rows, []string{
			"",
			fmt.Sprintf("STEP %d/%d", step.Index+1, len(group.Steps)),
			fmt.Sprintf(
				"capacity=%s; traffic=%d%%; gate=%s",
				orDash(step.Capacity),
				step.Traffic,
				step.Gate,
			),
		})
		if view == RolloutPlanViewEffective {
			groupIndex, stepIndex := group.Index, step.Index
			rows = append(
				rows,
				compactRolloutHoldRows(content.Holds, &groupIndex, &stepIndex)...,
			)
		}
	}
	if issues := compactRolloutGroupIssues(content, view, group); len(issues) > 0 {
		rows = append(rows, []string{"", "ISSUES", strings.Join(issues, ",")})
	}
	return rows
}

func compactRolloutHoldRows(
	holds []RolloutExplainHold,
	group *int,
	step *int32,
) [][]string {
	rows := [][]string{}
	for _, hold := range holds {
		if compareOptionalInt(hold.Group, group) != 0 || compareOptionalInt32(hold.Step, step) != 0 {
			continue
		}
		detail := fmt.Sprintf("%s; evidence=%s", hold.Kind, hold.Evidence)
		if hold.Component != "" {
			detail += "; component=" + string(hold.Component)
		}
		rows = append(rows, []string{"", "HOLD", detail})
	}
	return rows
}

func compactRolloutPlanIssues(content RolloutExplainContent, view RolloutPlanView) []string {
	values := []string{}
	for _, issue := range content.Issues {
		if issue.Group != nil || (issue.View == "" && view != RolloutPlanViewEffective) ||
			(issue.View != "" && issue.View != view) {
			continue
		}
		values = append(values, string(issue.Code))
	}
	if view != RolloutPlanViewEffective {
		return values
	}
	for _, issue := range content.Observed.Issues {
		if issue.Group == nil && issue.Component == "" {
			values = append(values, string(issue.Code))
		}
	}
	return values
}

func compactRolloutGroupIssues(
	content RolloutExplainContent,
	view RolloutPlanView,
	group RolloutPlanGroup,
) []string {
	values := []string{}
	for _, issue := range content.Issues {
		if issue.Group == nil || *issue.Group != group.Index ||
			(issue.View == "" && view != RolloutPlanViewEffective) ||
			(issue.View != "" && issue.View != view) {
			continue
		}
		values = append(values, string(issue.Code))
	}
	if view != RolloutPlanViewEffective {
		return values
	}
	for _, issue := range content.Observed.Issues {
		matchesGroup := issue.Group != nil && *issue.Group == group.Index
		matchesComponent := issue.Group == nil && issue.Component != "" &&
			slices.Contains(group.Components, issue.Component)
		if matchesGroup || matchesComponent {
			values = append(values, string(issue.Code))
		}
	}
	return values
}

func compactObservedComponentRows(
	content RolloutExplainContent,
	component RolloutComponentStatus,
) [][]string {
	rows := [][]string{{
		"",
		"COMPONENT " + string(component.Type),
		fmt.Sprintf(
			"%s; phase=%s; evidence=%s",
			component.Strategy,
			component.Phase,
			content.Observed.Summary.Evidence,
		),
	}}
	revisions := []string{}
	for _, value := range []struct{ key, value string }{
		{key: "current", value: component.RolledOutRevisionHash},
		{key: "ready", value: component.ReadyRevisionHash},
		{key: "previous", value: component.PreviousRevisionHash},
	} {
		if value.value != "" {
			revisions = append(revisions, value.key+"="+value.value)
		}
	}
	if len(revisions) > 0 {
		rows = append(rows, []string{"", "REVISIONS", strings.Join(revisions, ",")})
	}
	traffic := []string{}
	for _, target := range component.Traffic {
		traffic = append(traffic, fmt.Sprintf(
			"%s:%s=%d%%", component.Type, target.RevisionHash, target.Percent,
		))
	}
	if len(traffic) > 0 {
		rows = append(rows, []string{"", "TRAFFIC", strings.Join(traffic, ",")})
	}
	issues := []string{}
	for _, issue := range content.Observed.Issues {
		if issue.Group == nil && issue.Component == component.Type {
			issues = append(issues, string(issue.Code))
		}
	}
	if len(issues) > 0 {
		rows = append(rows, []string{
			"", "ISSUES " + string(component.Type), strings.Join(issues, ","),
		})
	}
	return rows
}

// WideTable returns the deterministic, complete operator view that preceded
// the compact plan outline.
func (c RolloutExplainContent) WideTable() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{
		"VIEW", "GROUP", "EVIDENCE", "PLAN-MODE", "SOURCE", "STRATEGY", "COMPONENTS",
		"CONFIG", "STEP", "GATE", "PHASE", "SEQUENCE", "PLAN-READY", "DRIFT", "HOLD",
		"REVISIONS", "TRAFFIC", "ISSUES",
	}, Rows: [][]string{}}
	for _, view := range []struct {
		name   RolloutPlanView
		groups []RolloutPlanGroup
	}{
		{name: RolloutPlanViewDeclared, groups: canonical.DeclaredGroups},
		{name: RolloutPlanViewLive, groups: canonical.LiveGroups},
		{name: RolloutPlanViewEffective, groups: canonical.EffectiveGroups},
	} {
		for _, group := range view.groups {
			steps := group.Steps
			if len(steps) == 0 {
				steps = []RolloutPlanStep{{Index: -1}}
			}
			for _, step := range steps {
				table.Rows = append(table.Rows, rolloutPlanTableRow(canonical, view.name, group, step))
			}
		}
		showEmptyLive := view.name == RolloutPlanViewLive &&
			canonical.Summary.EffectivePlan.Mode == RolloutPlanModePinned
		if (showEmptyLive || view.name == RolloutPlanViewEffective) && len(view.groups) == 0 {
			table.Rows = append(table.Rows, rolloutPlanSummaryTableRow(canonical, view.name))
		}
	}
	for _, component := range canonical.Observed.Components {
		if component.Group == nil {
			table.Rows = append(table.Rows, observedIndependentTableRow(canonical, component))
		}
	}
	return table
}

func rolloutPlanSummaryTableRow(content RolloutExplainContent, view RolloutPlanView) []string {
	mode := RolloutPlanModeLive
	evidence := EvidenceDeclared
	planReady, drift, holds := "-", "-", "-"
	if view == RolloutPlanViewEffective {
		mode = content.Summary.EffectivePlan.Mode
		evidence = content.Summary.EffectivePlan.Evidence
		planReady = rolloutPlanConditionCell(content.Summary.PlanReady)
		drift = rolloutPlanConditionCell(content.Summary.PlanDrift)
		holds = rolloutExplainHoldsCell(content.Holds, nil, nil)
	}
	return []string{
		string(view), "-", string(evidence),
		string(mode), "-", "-", "-", "groups=0", "-", "-", "-", "-",
		planReady, drift, holds, "-", "-",
		rolloutExplainCombinedIssuesCell(content, view, nil),
	}
}

func observedIndependentTableRow(
	content RolloutExplainContent,
	component RolloutComponentStatus,
) []string {
	revisions := []string{}
	for _, value := range []struct{ key, value string }{
		{key: "current", value: component.RolledOutRevisionHash},
		{key: "ready", value: component.ReadyRevisionHash},
		{key: "previous", value: component.PreviousRevisionHash},
	} {
		if value.value != "" {
			revisions = append(revisions, value.key+"="+value.value)
		}
	}
	traffic := make([]string, 0, len(component.Traffic))
	for _, target := range component.Traffic {
		traffic = append(traffic, fmt.Sprintf("%s:%s=%d%%", component.Type, target.RevisionHash, target.Percent))
	}
	return []string{
		"Observed", "-", string(content.Observed.Summary.Evidence), "-", "-", string(component.Strategy),
		string(component.Type), "-", "-", "-", orDash(string(component.Phase)), "-", "-", "-",
		rolloutExplainHoldsCell(content.Holds, nil, nil), orDash(strings.Join(revisions, ",")),
		orDash(strings.Join(traffic, ",")), rolloutIssueDisplay(rolloutIssuesForComponent(content.Observed.Issues, component)),
	}
}

func rolloutPlanTableRow(
	content RolloutExplainContent,
	view RolloutPlanView,
	group RolloutPlanGroup,
	step RolloutPlanStep,
) []string {
	phase, sequence, revisions, traffic := "-", "-", "-", "-"
	if view == RolloutPlanViewEffective {
		phase, sequence, revisions, traffic = observedGroupCells(content.Observed, group)
	}
	stepValue, gate := "-", "-"
	if step.Index >= 0 {
		stepValue = fmt.Sprintf("%d/%d %s/%d%%", step.Index+1, len(group.Steps), orDash(step.Capacity), step.Traffic)
		gate = string(step.Gate)
	}
	index := group.Index
	planReady, drift, holds := "-", "-", "-"
	if view == RolloutPlanViewEffective {
		planReady = rolloutPlanConditionCell(content.Summary.PlanReady)
		drift = rolloutPlanConditionCell(content.Summary.PlanDrift)
		stepIndex := step.Index
		if stepIndex < 0 {
			holds = rolloutExplainHoldsCell(content.Holds, &index, nil)
		} else {
			holds = rolloutExplainHoldsCell(content.Holds, &index, &stepIndex)
		}
	}
	planMode := "-"
	if view == RolloutPlanViewEffective {
		planMode = string(content.Summary.EffectivePlan.Mode)
	} else if view == RolloutPlanViewLive {
		planMode = string(RolloutPlanModeLive)
	}
	return []string{
		string(view), strconv.Itoa(group.Index), string(group.Evidence), planMode, string(group.Source),
		string(group.Strategy), rolloutComponentsCell(group.Components), rolloutPlanConfigCell(group),
		stepValue, gate, phase, sequence, planReady, drift, holds, revisions, traffic,
		rolloutExplainCombinedIssuesCell(content, view, &group),
	}
}

func rolloutPlanConditionCell(condition RolloutPlanCondition) string {
	value := string(condition.State)
	if value == "" {
		return "-"
	}
	if condition.Reason != "" {
		value += "/" + string(condition.Reason)
	}
	return value
}

func observedGroupCells(content RolloutStatusContent, planGroup RolloutPlanGroup) (string, string, string, string) {
	phase, sequence, revisions := "-", "-", "-"
	var observed *RolloutGroupStatus
	for index := range content.Groups {
		if content.Groups[index].Index == planGroup.Index {
			observed = &content.Groups[index]
			break
		}
	}
	if observed == nil {
		for index := range content.Groups {
			candidate := &content.Groups[index]
			if candidate.Strategy == RolloutStrategySequential &&
				allRolloutComponentsPresent(candidate.Components, planGroup.Components) {
				observed = candidate
				break
			}
		}
	}
	if observed != nil {
		phase = orDash(string(observed.Phase))
		sequenceParts := []string{}
		if observed.CurrentComponent != "" {
			sequenceParts = append(sequenceParts, "current="+string(observed.CurrentComponent))
		}
		if observed.PreviousComponent != "" {
			sequenceParts = append(sequenceParts, "previous="+string(observed.PreviousComponent))
		}
		if len(sequenceParts) > 0 {
			sequence = strings.Join(sequenceParts, ",")
		}
		parts := []string{}
		for _, value := range []struct{ key, value string }{
			{key: "stable", value: observed.StableRevisionHash},
			{key: "target", value: observed.TargetRevisionHash},
			{key: "rejected", value: observed.RejectedRevisionHash},
		} {
			if value.value != "" {
				parts = append(parts, value.key+"="+value.value)
			}
		}
		if len(parts) > 0 {
			revisions = strings.Join(parts, ",")
		}
	}
	trafficParts := []string{}
	for _, component := range content.Components {
		if component.Group == nil || observed == nil || *component.Group != observed.Index {
			continue
		}
		if observed.Strategy == RolloutStrategySequential &&
			!slices.Contains(planGroup.Components, component.Type) {
			continue
		}
		for _, target := range component.Traffic {
			trafficParts = append(trafficParts, fmt.Sprintf(
				"%s:%s=%d%%", component.Type, target.RevisionHash, target.Percent,
			))
		}
	}
	traffic := "-"
	if len(trafficParts) > 0 {
		traffic = strings.Join(trafficParts, ",")
	}
	return phase, sequence, revisions, traffic
}

func allRolloutComponentsPresent(haystack, needles []RuntimeComponentType) bool {
	for _, needle := range needles {
		if !slices.Contains(haystack, needle) {
			return false
		}
	}
	return len(needles) > 0
}

func rolloutPlanConfigCell(group RolloutPlanGroup) string {
	parts := rolloutPlanConfigParts(group)
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ",")
}

func rolloutPlanConfigParts(group RolloutPlanGroup) []string {
	parts := []string{}
	switch group.ProgressionOrigin {
	case RolloutProgressionOriginDefaulted:
		parts = append(parts, "strategy=defaulted")
	case RolloutProgressionOriginUnknown:
		parts = append(parts, "origin=Unknown")
	}
	if group.Policy != nil {
		policy := "policy=" + group.Policy.Kind + "/" + group.Policy.Name
		if group.Policy.Generation > 0 {
			policy += "@" + strconv.FormatInt(group.Policy.Generation, 10)
		}
		parts = append(parts, policy)
		if group.Policy.Digest != "" {
			parts = append(parts, "digest="+group.Policy.Digest)
		}
	}
	if group.PortableDigest != "" && (group.Policy == nil || group.Policy.Digest != group.PortableDigest) {
		parts = append(parts, "digest="+group.PortableDigest)
	}
	if group.ShadowedPolicy != nil {
		parts = append(parts, "shadowed="+group.ShadowedPolicy.Kind+"/"+group.ShadowedPolicy.Name)
	}
	if group.Soak != nil {
		parts = append(parts, "soak="+settingCell(*group.Soak))
	}
	if group.RollingUpdate != nil {
		parts = append(parts,
			"surge="+settingCell(group.RollingUpdate.MaxSurge),
			"unavailable="+settingCell(group.RollingUpdate.MaxUnavailable),
		)
	}
	if group.MaintainRatio != nil {
		parts = append(parts, "ratio="+settingCell(group.MaintainRatio.Tolerance))
	}
	return parts
}

func settingCell(setting RolloutSetting) string {
	detail := string(setting.Source)
	if setting.Effect != RolloutSettingEffectApplied {
		detail += ";" + string(setting.Effect)
	}
	if setting.Value == "" {
		return detail
	}
	return setting.Value + "(" + detail + ")"
}

func rolloutComponentsCell(components []RuntimeComponentType) string {
	if len(components) == 0 {
		return "-"
	}
	values := make([]string, len(components))
	for i := range components {
		values[i] = string(components[i])
	}
	return strings.Join(values, ",")
}

func rolloutExplainHoldsCell(holds []RolloutExplainHold, group *int, step *int32) string {
	values := []string{}
	for _, hold := range holds {
		if hold.Group != nil && (group == nil || *hold.Group != *group) {
			continue
		}
		if hold.Step != nil && (step == nil || *hold.Step != *step) {
			continue
		}
		if hold.Group == nil || group != nil {
			values = append(values, string(hold.Kind))
		}
	}
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ",")
}

func rolloutExplainIssuesCell(issues []RolloutExplainIssue, view RolloutPlanView, group *int) string {
	values := []string{}
	for _, issue := range issues {
		if issue.View != "" && issue.View != view {
			continue
		}
		if issue.Group != nil && (group == nil || *issue.Group != *group) {
			continue
		}
		values = append(values, string(issue.Code))
	}
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ",")
}

func rolloutExplainCombinedIssuesCell(
	content RolloutExplainContent,
	view RolloutPlanView,
	group *RolloutPlanGroup,
) string {
	var groupIndex *int
	if group != nil {
		groupIndex = &group.Index
	}
	values := []string{}
	if projected := rolloutExplainIssuesCell(content.Issues, view, groupIndex); projected != "-" {
		values = append(values, projected)
	}
	if view == RolloutPlanViewEffective || view == "" {
		observed := make([]RolloutIssue, 0, len(content.Observed.Issues))
		for _, issue := range content.Observed.Issues {
			if group != nil {
				if issue.Group != nil && *issue.Group != group.Index {
					continue
				}
				if issue.Component != "" && !slices.Contains(group.Components, issue.Component) {
					continue
				}
			}
			observed = append(observed, issue)
		}
		if projected := rolloutIssueDisplay(observed); projected != "-" {
			values = append(values, projected)
		}
	}
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ",")
}

func canonicalRolloutPlanGroups(groups []RolloutPlanGroup) []RolloutPlanGroup {
	result := make([]RolloutPlanGroup, len(groups))
	for i := range groups {
		result[i] = groups[i].canonical()
	}
	sort.Slice(result, func(i, j int) bool { return compareRolloutPlanGroups(result[i], result[j]) < 0 })
	return result
}

func (g RolloutPlanGroup) canonical() RolloutPlanGroup {
	result := g
	result.Evidence = canonicalRolloutEvidenceLevel(g.Evidence)
	result.Source = canonicalRolloutPlanSource(g.Source)
	result.Strategy = canonicalRolloutStrategy(g.Strategy)
	result.ProgressionOrigin = canonicalRolloutProgressionOrigin(g.ProgressionOrigin)
	if g.ProgressionOrigin == "" {
		switch {
		case g.Defaulted || result.Source == RolloutPlanSourceDefaulted:
			result.ProgressionOrigin = RolloutProgressionOriginDefaulted
		case result.Source == RolloutPlanSourcePolicy:
			result.ProgressionOrigin = RolloutProgressionOriginPolicy
		default:
			result.ProgressionOrigin = RolloutProgressionOriginConfigured
		}
	}
	result.Defaulted = result.ProgressionOrigin == RolloutProgressionOriginDefaulted
	result.Components = canonicalRolloutComponentSlice(g.Components)
	result.Order = canonicalRolloutComponentSlice(g.Order)
	result.Policy = canonicalRolloutPolicyReference(g.Policy)
	result.ShadowedPolicy = canonicalRolloutPolicyReference(g.ShadowedPolicy)
	if rolloutPortableDigestPattern.MatchString(g.PortableDigest) {
		result.PortableDigest = g.PortableDigest
	} else {
		result.PortableDigest = ""
	}
	result.Soak = canonicalRolloutSettingPointer(g.Soak)
	if g.RollingUpdate != nil {
		settings := *g.RollingUpdate
		settings.MaxSurge = canonicalRolloutSetting(settings.MaxSurge)
		settings.MaxUnavailable = canonicalRolloutSetting(settings.MaxUnavailable)
		result.RollingUpdate = &settings
	}
	if g.MaintainRatio != nil {
		settings := *g.MaintainRatio
		settings.Tolerance = canonicalRolloutSetting(settings.Tolerance)
		result.MaintainRatio = &settings
	}
	result.Steps = make([]RolloutPlanStep, len(g.Steps))
	for i := range g.Steps {
		result.Steps[i] = g.Steps[i]
		result.Steps[i].Gate = canonicalRolloutGate(g.Steps[i].Gate)
		result.Steps[i].Capacity = canonicalRolloutCapacity(g.Steps[i].Capacity)
		if result.Steps[i].Traffic < 0 || result.Steps[i].Traffic > 100 {
			result.Steps[i].Traffic = 0
		}
		result.Steps[i].Pause = canonicalRolloutDuration(g.Steps[i].Pause)
		if g.Steps[i].Analysis != nil {
			analysis := *g.Steps[i].Analysis
			if analysis.MetricCount < 0 || analysis.MetricCount > 10 {
				analysis.MetricCount = 0
			}
			analysis.Interval = canonicalRolloutDuration(analysis.Interval)
			analysis.InitialDelay = canonicalRolloutDuration(analysis.InitialDelay)
			if analysis.FailureLimit < 0 {
				analysis.FailureLimit = 0
			}
			switch analysis.OnInconclusive {
			case "Hold", "Rollback", "RollbackOnStall":
			default:
				analysis.OnInconclusive = "Unknown"
			}
			result.Steps[i].Analysis = &analysis
		}
	}
	sort.Slice(result.Steps, func(i, j int) bool { return result.Steps[i].Index < result.Steps[j].Index })
	return result
}

func canonicalRolloutComponentSlice(values []RuntimeComponentType) []RuntimeComponentType {
	result := make([]RuntimeComponentType, 0, len(values))
	for _, value := range values {
		if canonical := canonicalRolloutComponentType(value); canonical != "" {
			result = append(result, canonical)
		}
	}
	return result
}

func canonicalRolloutPolicyReference(value *RolloutPolicyReference) *RolloutPolicyReference {
	if value == nil {
		return nil
	}
	result := *value
	result.Evidence = canonicalRolloutEvidenceLevel(result.Evidence)
	if result.Kind != "RolloutPolicy" || len(utilvalidation.IsDNS1123Subdomain(result.Name)) != 0 {
		return nil
	}
	switch result.Progression {
	case "canary", "blueGreen", "rollingUpdate":
	default:
		result.Progression = ""
	}
	if result.Generation < 0 {
		result.Generation = 0
	}
	if !rolloutPortableDigestPattern.MatchString(result.Digest) {
		result.Digest = ""
	}
	return &result
}

func canonicalRolloutSettingPointer(value *RolloutSetting) *RolloutSetting {
	if value == nil {
		return nil
	}
	result := canonicalRolloutSetting(*value)
	if (result.Source == RolloutSettingConfigured || result.Source == RolloutSettingDefaulted) &&
		result.Value == "" {
		return nil
	}
	return &result
}

func canonicalRolloutSetting(value RolloutSetting) RolloutSetting {
	result := value
	switch result.Source {
	case RolloutSettingConfigured, RolloutSettingDefaulted,
		RolloutSettingOperatorValue, RolloutSettingUnresolved:
	default:
		result.Source = RolloutSettingUnresolved
		result.Value = ""
	}
	if result.Source == RolloutSettingConfigured || result.Source == RolloutSettingDefaulted {
		if canonicalRolloutCapacity(result.Value) == "" && canonicalRolloutDuration(result.Value) == "" {
			result.Value = ""
		}
	} else {
		result.Value = ""
	}
	switch result.Effect {
	case RolloutSettingEffectApplied, RolloutSettingEffectIgnoredFinalGroup,
		RolloutSettingEffectIgnoredSingleComponent, RolloutSettingEffectIgnoredPlanShape:
	default:
		result.Effect = RolloutSettingEffectUnknown
	}
	return result
}

func canonicalRolloutCapacity(value string) string {
	if value == "" {
		return ""
	}
	percentage := strings.HasSuffix(value, "%")
	raw := strings.TrimSuffix(value, "%")
	parsed, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || parsed < 0 || (percentage && parsed > 100) || strconv.FormatInt(parsed, 10) != raw {
		return ""
	}
	return value
}

func canonicalRolloutDuration(value string) string {
	if value == "" {
		return ""
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 || duration.String() != value {
		return ""
	}
	return value
}

func canonicalRolloutExplainHolds(values []RolloutExplainHold) []RolloutExplainHold {
	result := make([]RolloutExplainHold, 0, len(values))
	for _, value := range values {
		value.Kind = canonicalRolloutExplainHoldKind(value.Kind)
		value.Evidence = canonicalRolloutEvidenceLevel(value.Evidence)
		value.Component = canonicalRolloutComponentType(value.Component)
		if value.Group != nil {
			group := *value.Group
			value.Group = &group
		}
		if value.Step != nil {
			step := *value.Step
			value.Step = &step
		}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool {
		return compareRolloutExplainHolds(result[i], result[j]) < 0
	})
	return slices.CompactFunc(result, func(a, b RolloutExplainHold) bool {
		return compareRolloutExplainHolds(a, b) == 0
	})
}

func canonicalRolloutExplainIssues(values []RolloutExplainIssue) []RolloutExplainIssue {
	result := make([]RolloutExplainIssue, len(values))
	for i := range values {
		result[i] = values[i]
		result[i].Code = canonicalRolloutExplainIssueCode(values[i].Code)
		result[i].View = canonicalRolloutPlanView(values[i].View)
		if values[i].Group != nil {
			group := *values[i].Group
			result[i].Group = &group
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return compareRolloutExplainIssues(result[i], result[j]) < 0
	})
	return slices.CompactFunc(result, func(a, b RolloutExplainIssue) bool {
		return compareRolloutExplainIssues(a, b) == 0
	})
}

func canonicalRolloutPlanCondition(value RolloutPlanCondition) RolloutPlanCondition {
	value.State = canonicalRolloutConditionState(value.State)
	value.Reason = canonicalRolloutPlanReason(value.Reason)
	value.Evidence = canonicalRolloutEvidenceLevel(value.Evidence)
	return value
}

func canonicalRolloutPlanMode(value RolloutPlanMode) RolloutPlanMode {
	switch value {
	case RolloutPlanModeLive, RolloutPlanModePinned:
		return value
	default:
		return RolloutPlanModeUnknown
	}
}

func canonicalRolloutPlanSource(value RolloutPlanSource) RolloutPlanSource {
	switch value {
	case RolloutPlanSourceInline, RolloutPlanSourcePolicy, RolloutPlanSourceDefaulted:
		return value
	default:
		return RolloutPlanSourceUnknown
	}
}

func canonicalRolloutPlanView(value RolloutPlanView) RolloutPlanView {
	switch value {
	case "", RolloutPlanViewDeclared, RolloutPlanViewLive, RolloutPlanViewEffective:
		return value
	default:
		return ""
	}
}

func canonicalRolloutProgressionOrigin(value RolloutProgressionOrigin) RolloutProgressionOrigin {
	switch value {
	case RolloutProgressionOriginConfigured, RolloutProgressionOriginDefaulted,
		RolloutProgressionOriginPolicy:
		return value
	default:
		return RolloutProgressionOriginUnknown
	}
}

func canonicalRolloutPlanReason(value RolloutPlanReason) RolloutPlanReason {
	switch value {
	case "", RolloutPlanReasonPinned, RolloutPlanReasonNoActiveRun,
		RolloutPlanReasonPolicyNotFound, RolloutPlanReasonPolicyNotReady,
		RolloutPlanReasonProgressionMismatch, RolloutPlanReasonPlanInvalid,
		RolloutPlanReasonProviderUnbound, RolloutPlanReasonInSync,
		RolloutPlanReasonPolicyNewer, RolloutPlanReasonSpecNewer:
		return value
	default:
		return RolloutPlanReasonUnknown
	}
}

func canonicalRolloutExplainHoldKind(value RolloutExplainHoldKind) RolloutExplainHoldKind {
	switch value {
	case RolloutHoldUnknown, RolloutHoldGlobalPause, RolloutHoldPlanParked, RolloutHoldCanaryPreStep,
		RolloutHoldManualGate, RolloutHoldTimedGate, RolloutHoldAnalysisGate,
		RolloutHoldObservedPaused:
		return value
	default:
		return RolloutHoldUnknown
	}
}

func canonicalRolloutExplainIssueCode(value RolloutExplainIssueCode) RolloutExplainIssueCode {
	switch value {
	case RolloutExplainIssueUnknown, RolloutExplainIssueDeclaredPlanMalformed,
		RolloutExplainIssueLivePlanMalformed, RolloutExplainIssueEffectivePlanMalformed,
		RolloutExplainIssueActiveRunMalformed, RolloutExplainIssuePlanConditionMalformed,
		RolloutExplainIssueResolutionMissing, RolloutExplainIssueResolutionMalformed,
		RolloutExplainIssuePolicyBodyUnavailable, RolloutExplainIssuePlanTruncated:
		return value
	default:
		return RolloutExplainIssueUnknown
	}
}

func compareRolloutExplainSources(a, b RolloutSourceReference) int {
	for _, result := range []int{
		cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Name, b.Name), cmp.Compare(a.UID, b.UID),
		cmp.Compare(a.Generation, b.Generation), cmp.Compare(a.Evidence, b.Evidence),
		a.CollectedAt.Compare(b.CollectedAt),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRolloutPlanGroups(a, b RolloutPlanGroup) int {
	for _, result := range []int{
		cmp.Compare(a.Index, b.Index), cmp.Compare(a.Evidence, b.Evidence),
		cmp.Compare(a.Source, b.Source), cmp.Compare(a.Strategy, b.Strategy),
		slices.Compare(a.Components, b.Components), slices.Compare(a.Order, b.Order),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRolloutExplainHolds(a, b RolloutExplainHold) int {
	for _, result := range []int{
		cmp.Compare(a.Kind, b.Kind), compareOptionalInt(a.Group, b.Group),
		compareOptionalInt32(a.Step, b.Step),
		cmp.Compare(a.Component, b.Component), cmp.Compare(a.Evidence, b.Evidence),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareOptionalInt32(a, b *int32) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}
	return cmp.Compare(*a, *b)
}

func compareRolloutExplainIssues(a, b RolloutExplainIssue) int {
	for _, result := range []int{
		cmp.Compare(a.Code, b.Code), cmp.Compare(a.View, b.View), compareOptionalInt(a.Group, b.Group),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}
