package v1alpha1

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/report"
)

// RolloutStatusReportKind identifies the rollout status report schema.
const RolloutStatusReportKind = "RolloutStatusReport"

// RolloutSourceKind is the closed set of objects read by rollout status.
type RolloutSourceKind string

const (
	// RolloutSourceInferenceService is the single object read by rollout status.
	RolloutSourceInferenceService RolloutSourceKind = "InferenceService"
)

// RolloutState is the bounded aggregate state of the reported rollout.
type RolloutState string

const (
	RolloutStateUnknown       RolloutState = "Unknown"
	RolloutStateNotConfigured RolloutState = "NotConfigured"
	RolloutStateSucceeded     RolloutState = "Succeeded"
	RolloutStateInProgress    RolloutState = "InProgress"
	RolloutStatePaused        RolloutState = "Paused"
	RolloutStateStaged        RolloutState = "Staged"
	RolloutStateFailed        RolloutState = "Failed"
	RolloutStateRollingBack   RolloutState = "RollingBack"
	RolloutStateRolledBack    RolloutState = "RolledBack"
)

// RolloutEpochState identifies whether a rollout conclusion can be bound to
// the current InferenceService generation.
type RolloutEpochState string

const (
	RolloutEpochNotApplicable RolloutEpochState = "NotApplicable"
	RolloutEpochUnverifiable  RolloutEpochState = "Unverifiable"
)

// RolloutStrategy identifies the progression applied to a rollout group.
type RolloutStrategy string

const (
	RolloutStrategyUnknown       RolloutStrategy = "Unknown"
	RolloutStrategyIndependent   RolloutStrategy = "Independent"
	RolloutStrategyCanary        RolloutStrategy = "Canary"
	RolloutStrategyBlueGreen     RolloutStrategy = "BlueGreen"
	RolloutStrategyRollingUpdate RolloutStrategy = "RollingUpdate"
	RolloutStrategySequential    RolloutStrategy = "Sequential"
)

// RolloutPhase is the allowlisted union of shipped component and coordination
// phases. Unknown input is projected as Unknown rather than copied verbatim.
type RolloutPhase string

const (
	RolloutPhaseUnknown               RolloutPhase = "Unknown"
	RolloutPhaseStable                RolloutPhase = "Stable"
	RolloutPhaseCanarying             RolloutPhase = "Canarying"
	RolloutPhaseBlueGreenStandby      RolloutPhase = "BlueGreenStandby"
	RolloutPhasePending               RolloutPhase = "Pending"
	RolloutPhasePaused                RolloutPhase = "Paused"
	RolloutPhasePromoting             RolloutPhase = "Promoting"
	RolloutPhaseRollingBack           RolloutPhase = "RollingBack"
	RolloutPhaseRolledBack            RolloutPhase = "RolledBack"
	RolloutPhaseFailed                RolloutPhase = "Failed"
	RolloutPhaseIdle                  RolloutPhase = "Idle"
	RolloutPhaseSurging               RolloutPhase = "Surging"
	RolloutPhaseWaiting               RolloutPhase = "Waiting"
	RolloutPhaseShifting              RolloutPhase = "Shifting"
	RolloutPhaseUpdating              RolloutPhase = "Updating"
	RolloutPhaseDraining              RolloutPhase = "Draining"
	RolloutPhaseScalingDown           RolloutPhase = "ScalingDown"
	RolloutPhaseStaged                RolloutPhase = "Staged"
	RolloutPhaseAwaitingNextComponent RolloutPhase = "AwaitingNextComponent"
)

// RolloutGate identifies how the active canary step advances.
type RolloutGate string

const (
	RolloutGateUnknown   RolloutGate = "Unknown"
	RolloutGateImmediate RolloutGate = "Immediate"
	RolloutGateManual    RolloutGate = "Manual"
	RolloutGateTimed     RolloutGate = "Timed"
	RolloutGateAnalysis  RolloutGate = "Analysis"
)

// RolloutAnalysisState is a message-free summary of the latest analysis
// results. Metric names, values, thresholds, queries, and messages are absent.
type RolloutAnalysisState string

const (
	RolloutAnalysisUnobserved   RolloutAnalysisState = "Unobserved"
	RolloutAnalysisPassing      RolloutAnalysisState = "Passing"
	RolloutAnalysisFailing      RolloutAnalysisState = "Failing"
	RolloutAnalysisInconclusive RolloutAnalysisState = "Inconclusive"
)

// RolloutConditionState is a bounded condition-status projection.
type RolloutConditionState string

const (
	RolloutConditionNotApplicable RolloutConditionState = "NotApplicable"
	RolloutConditionUnobserved    RolloutConditionState = "Unobserved"
	RolloutConditionTrue          RolloutConditionState = "True"
	RolloutConditionFalse         RolloutConditionState = "False"
	RolloutConditionUnknown       RolloutConditionState = "Unknown"
	RolloutConditionInvalid       RolloutConditionState = "Invalid"
)

// RolloutRevisionRole identifies a safe revision-hash relation.
type RolloutRevisionRole string

const (
	RolloutRevisionCurrent  RolloutRevisionRole = "Current"
	RolloutRevisionTarget   RolloutRevisionRole = "Target"
	RolloutRevisionPrevious RolloutRevisionRole = "Previous"
	RolloutRevisionOther    RolloutRevisionRole = "Other"
)

// RolloutIssueCode is a stable message-free rollout diagnostic.
type RolloutIssueCode string

const (
	RolloutIssueSpecMalformed          RolloutIssueCode = "SpecMalformed"
	RolloutIssueStatusMalformed        RolloutIssueCode = "StatusMalformed"
	RolloutIssueGroupStatusMissing     RolloutIssueCode = "GroupStatusMissing"
	RolloutIssueGroupStatusUnexpected  RolloutIssueCode = "GroupStatusUnexpected"
	RolloutIssueComponentStatusMissing RolloutIssueCode = "ComponentStatusMissing"
	RolloutIssueCanaryStatusMissing    RolloutIssueCode = "CanaryStatusMissing"
	RolloutIssueCanaryStatusUnexpected RolloutIssueCode = "CanaryStatusUnexpected"
	RolloutIssueCanaryStepInvalid      RolloutIssueCode = "CanaryStepInvalid"
	RolloutIssueRevisionNameInvalid    RolloutIssueCode = "RevisionNameInvalid"
	RolloutIssueTrafficInvalid         RolloutIssueCode = "TrafficInvalid"
	RolloutIssueAnalysisInconclusive   RolloutIssueCode = "AnalysisInconclusive"
	RolloutIssueEpochUnverifiable      RolloutIssueCode = "EpochUnverifiable"
)

// RolloutSourceReference is an allowlisted source identity. It intentionally
// has no ResourceVersion or arbitrary message field.
type RolloutSourceReference struct {
	Kind        RolloutSourceKind `json:"kind"`
	Namespace   string            `json:"namespace,omitempty"`
	Name        string            `json:"name"`
	UID         string            `json:"uid,omitempty"`
	Generation  int64             `json:"generation,omitempty"`
	Evidence    EvidenceLevel     `json:"evidence"`
	CollectedAt time.Time         `json:"collectedAt"`
}

// RolloutWarning is deliberately code-only.
type RolloutWarning struct {
	Code WarningCode `json:"code"`
}

// RolloutSummary is the bounded aggregate of rollout-owned evidence. Evidence
// and Epoch transitively qualify ReportedState, CoordinationReady, and every
// status-derived field in Groups and Components.
type RolloutSummary struct {
	State             RolloutState          `json:"state"`
	ReportedState     RolloutState          `json:"reportedState"`
	Evidence          EvidenceLevel         `json:"evidence"`
	Epoch             RolloutEpochState     `json:"epoch"`
	CoordinationReady RolloutConditionState `json:"coordinationReady"`
}

// RolloutStepStatus is the bounded projection of the active canary step.
type RolloutStepStatus struct {
	Index           int32                `json:"index"`
	Total           int32                `json:"total"`
	Capacity        string               `json:"capacity"`
	TargetTraffic   int32                `json:"targetTraffic"`
	ObservedTraffic int32                `json:"observedTraffic"`
	Gate            RolloutGate          `json:"gate"`
	Analysis        RolloutAnalysisState `json:"analysis,omitempty"`
	EnteredAt       *time.Time           `json:"enteredAt,omitempty"`
}

// RolloutGroupStatus describes one declared or controller-collapsed group.
// Status-derived fields are qualified by RolloutSummary.Evidence and Epoch.
type RolloutGroupStatus struct {
	Index                int                    `json:"index"`
	Strategy             RolloutStrategy        `json:"strategy"`
	Phase                RolloutPhase           `json:"phase"`
	Components           []RuntimeComponentType `json:"components"`
	CurrentComponent     RuntimeComponentType   `json:"currentComponent,omitempty"`
	PreviousComponent    RuntimeComponentType   `json:"previousComponent,omitempty"`
	ObservedSurge        string                 `json:"observedSurge,omitempty"`
	StableRevisionHash   string                 `json:"stableRevisionHash,omitempty"`
	TargetRevisionHash   string                 `json:"targetRevisionHash,omitempty"`
	RejectedRevisionHash string                 `json:"rejectedRevisionHash,omitempty"`
	Step                 *RolloutStepStatus     `json:"step,omitempty"`
	TransitionedAt       *time.Time             `json:"transitionedAt,omitempty"`
}

// RolloutTrafficTarget identifies a traffic allocation by safe revision hash.
type RolloutTrafficTarget struct {
	RevisionHash string              `json:"revisionHash"`
	Percent      int32               `json:"percent"`
	Role         RolloutRevisionRole `json:"role"`
}

// RolloutComponentStatus is the safe per-component rollout observation.
// Status-derived fields are qualified by RolloutSummary.Evidence and Epoch.
type RolloutComponentStatus struct {
	Type                  RuntimeComponentType   `json:"type"`
	Strategy              RolloutStrategy        `json:"strategy"`
	Group                 *int                   `json:"group,omitempty"`
	Phase                 RolloutPhase           `json:"phase"`
	RolledOutRevisionHash string                 `json:"rolledOutRevisionHash,omitempty"`
	ReadyRevisionHash     string                 `json:"readyRevisionHash,omitempty"`
	PreviousRevisionHash  string                 `json:"previousRevisionHash,omitempty"`
	Traffic               []RolloutTrafficTarget `json:"traffic"`
}

// RolloutIssue scopes one stable diagnostic code without arbitrary text.
type RolloutIssue struct {
	Code      RolloutIssueCode     `json:"code"`
	Group     *int                 `json:"group,omitempty"`
	Component RuntimeComponentType `json:"component,omitempty"`
}

// RolloutStatusContent is the typed body shared by all output formats.
type RolloutStatusContent struct {
	Summary    RolloutSummary           `json:"summary"`
	Groups     []RolloutGroupStatus     `json:"groups"`
	Components []RolloutComponentStatus `json:"components"`
	Issues     []RolloutIssue           `json:"issues"`
}

// RolloutStatusReport is the dedicated read-only rollout output contract.
type RolloutStatusReport struct {
	APIVersion  string                   `json:"apiVersion"`
	Kind        string                   `json:"kind"`
	Metadata    Metadata                 `json:"metadata"`
	CollectedAt time.Time                `json:"collectedAt"`
	Sources     []RolloutSourceReference `json:"sources"`
	Content     RolloutStatusContent     `json:"content"`
	Warnings    []RolloutWarning         `json:"warnings"`
}

// NewRolloutStatusReport builds a canonical report with an injectable clock.
func NewRolloutStatusReport(metadata Metadata, content RolloutStatusContent, clock Clock) RolloutStatusReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (RolloutStatusReport{
		APIVersion: APIVersion, Kind: RolloutStatusReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []RolloutSourceReference{},
		Content: content, Warnings: []RolloutWarning{},
	}).Canonical()
}

// Canonical returns a deeply copied, deterministically ordered report.
func (r RolloutStatusReport) Canonical() RolloutStatusReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = RolloutStatusReportKind
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
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.UID != b.UID {
			return a.UID < b.UID
		}
		if a.Generation != b.Generation {
			return a.Generation < b.Generation
		}
		if a.Evidence != b.Evidence {
			return a.Evidence < b.Evidence
		}
		return a.CollectedAt.Before(b.CollectedAt)
	})
	result.Warnings = append([]RolloutWarning{}, r.Warnings...)
	for i := range result.Warnings {
		result.Warnings[i].Code = canonicalRolloutWarningCode(result.Warnings[i].Code)
	}
	sort.Slice(result.Warnings, func(i, j int) bool { return result.Warnings[i].Code < result.Warnings[j].Code })
	result.Content = r.Content.Canonical()
	return result
}

// Canonical returns deeply copied, deterministically ordered content.
func (c RolloutStatusContent) Canonical() RolloutStatusContent {
	result := c
	result.Summary.State = canonicalRolloutState(c.Summary.State)
	result.Summary.ReportedState = canonicalRolloutState(c.Summary.ReportedState)
	result.Summary.Evidence = canonicalRolloutEvidenceLevel(c.Summary.Evidence)
	result.Summary.Epoch = canonicalRolloutEpochState(c.Summary.Epoch)
	result.Summary.CoordinationReady = canonicalRolloutConditionState(c.Summary.CoordinationReady)
	result.Groups = make([]RolloutGroupStatus, len(c.Groups))
	for i := range c.Groups {
		result.Groups[i] = c.Groups[i]
		result.Groups[i].Strategy = canonicalRolloutStrategy(c.Groups[i].Strategy)
		result.Groups[i].Phase = canonicalRolloutPhase(c.Groups[i].Phase)
		result.Groups[i].Components = make([]RuntimeComponentType, 0, len(c.Groups[i].Components))
		for _, component := range c.Groups[i].Components {
			if canonical := canonicalRolloutComponentType(component); canonical != "" {
				result.Groups[i].Components = append(result.Groups[i].Components, canonical)
			}
		}
		result.Groups[i].CurrentComponent = canonicalRolloutComponentType(c.Groups[i].CurrentComponent)
		result.Groups[i].PreviousComponent = canonicalRolloutComponentType(c.Groups[i].PreviousComponent)
		if c.Groups[i].Step != nil {
			step := *c.Groups[i].Step
			step.Gate = canonicalRolloutGate(step.Gate)
			step.Analysis = canonicalRolloutAnalysisState(step.Analysis)
			if step.EnteredAt != nil {
				entered := step.EnteredAt.UTC()
				step.EnteredAt = &entered
			}
			result.Groups[i].Step = &step
		}
		if c.Groups[i].TransitionedAt != nil {
			transitioned := c.Groups[i].TransitionedAt.UTC()
			result.Groups[i].TransitionedAt = &transitioned
		}
	}
	sort.Slice(result.Groups, func(i, j int) bool {
		return compareRolloutGroups(result.Groups[i], result.Groups[j]) < 0
	})
	result.Components = make([]RolloutComponentStatus, 0, len(c.Components))
	for i := range c.Components {
		component := c.Components[i]
		component.Type = canonicalRolloutComponentType(component.Type)
		if component.Type == "" {
			continue
		}
		component.Strategy = canonicalRolloutStrategy(component.Strategy)
		component.Phase = canonicalRolloutPhase(component.Phase)
		if c.Components[i].Group != nil {
			group := *c.Components[i].Group
			component.Group = &group
		}
		component.Traffic = append([]RolloutTrafficTarget{}, c.Components[i].Traffic...)
		for trafficIndex := range component.Traffic {
			component.Traffic[trafficIndex].Role = canonicalRolloutRevisionRole(
				component.Traffic[trafficIndex].Role,
			)
		}
		sort.Slice(component.Traffic, func(a, b int) bool {
			return compareRolloutTrafficTargets(
				component.Traffic[a], component.Traffic[b],
			) < 0
		})
		result.Components = append(result.Components, component)
	}
	sort.Slice(result.Components, func(i, j int) bool {
		return compareRolloutComponents(result.Components[i], result.Components[j]) < 0
	})
	result.Issues = make([]RolloutIssue, len(c.Issues))
	for i := range c.Issues {
		result.Issues[i] = c.Issues[i]
		result.Issues[i].Code = canonicalRolloutIssueCode(c.Issues[i].Code)
		result.Issues[i].Component = canonicalRolloutComponentType(c.Issues[i].Component)
		if c.Issues[i].Group != nil {
			group := *c.Issues[i].Group
			result.Issues[i].Group = &group
		}
	}
	sort.Slice(result.Issues, func(i, j int) bool {
		a, b := result.Issues[i], result.Issues[j]
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		if compareOptionalInt(a.Group, b.Group) != 0 {
			return compareOptionalInt(a.Group, b.Group) < 0
		}
		return a.Component < b.Component
	})
	return result
}

// Table derives the human view from the report's typed content.
func (r RolloutStatusReport) Table() report.Table { return r.Content.Table() }

// WideTable derives the complete legacy human view from the report's typed
// content. It is intentionally separate from the default compact matrix so
// machine formats and detailed operator output remain stable.
func (r RolloutStatusReport) WideTable() report.Table { return r.Content.WideTable() }

// Table returns a deterministic component matrix sized for an operator's
// terminal. Summary evidence occupies the service column while component
// progress is read vertically under the engine, decoder, and router columns.
func (c RolloutStatusContent) Table() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{"FIELD", "SERVICE", "ENGINE", "DECODER", "ROUTER"}}
	table.Rows = append(table.Rows,
		[]string{"STATE", orDash(string(canonical.Summary.State)), "-", "-", "-"},
		[]string{"REPORTED", orDash(string(canonical.Summary.ReportedState)), "-", "-", "-"},
		[]string{"EVIDENCE", orDash(string(canonical.Summary.Evidence)), "-", "-", "-"},
		[]string{"EPOCH", orDash(string(canonical.Summary.Epoch)), "-", "-", "-"},
		[]string{"COORDINATION", orDash(string(canonical.Summary.CoordinationReady)), "-", "-", "-"},
	)

	groups := make(map[int]RolloutGroupStatus, len(canonical.Groups))
	for _, group := range canonical.Groups {
		groups[group.Index] = group
	}

	componentValues := make(map[RuntimeComponentType]map[string][]string, 3)
	for _, component := range canonical.Components {
		values := compactRolloutComponentValues(component, groups)
		if componentValues[component.Type] == nil {
			componentValues[component.Type] = make(map[string][]string, len(values))
		}
		for field, value := range values {
			componentValues[component.Type][field] = append(
				componentValues[component.Type][field], value,
			)
		}
	}

	for _, field := range []string{
		"GROUP", "STRATEGY", "GROUP-PHASE", "PHASE", "GROUP-CURRENT",
		"GROUP-PREVIOUS", "STEP", "GATE", "CAPACITY", "TRAFFIC",
		"ROLLED-OUT", "READY", "PREVIOUS",
	} {
		row := []string{field, "-"}
		hasValue := false
		for _, componentType := range []RuntimeComponentType{
			RuntimeComponentEngine, RuntimeComponentDecoder, RuntimeComponentRouter,
		} {
			value := compactRolloutCell(componentValues[componentType][field])
			row = append(row, value)
			if value != "-" {
				hasValue = true
			}
		}
		if hasValue {
			table.Rows = append(table.Rows, row)
		}
	}
	if len(canonical.Issues) > 0 {
		issuesByComponent := make(map[RuntimeComponentType][]RolloutIssue, 3)
		serviceIssues := make([]RolloutIssue, 0, len(canonical.Issues))
		for _, issue := range canonical.Issues {
			if issue.Component == "" {
				serviceIssues = append(serviceIssues, issue)
				continue
			}
			issuesByComponent[issue.Component] = append(issuesByComponent[issue.Component], issue)
		}
		table.Rows = append(table.Rows, []string{
			"ISSUES",
			rolloutIssueDisplay(serviceIssues),
			compactRolloutIssueDisplay(issuesByComponent[RuntimeComponentEngine]),
			compactRolloutIssueDisplay(issuesByComponent[RuntimeComponentDecoder]),
			compactRolloutIssueDisplay(issuesByComponent[RuntimeComponentRouter]),
		})
	}
	return table
}

func compactRolloutComponentValues(
	component RolloutComponentStatus,
	groups map[int]RolloutGroupStatus,
) map[string]string {
	values := map[string]string{
		"GROUP":          "-",
		"STRATEGY":       orDash(string(component.Strategy)),
		"GROUP-PHASE":    "-",
		"PHASE":          orDash(string(component.Phase)),
		"GROUP-CURRENT":  "-",
		"GROUP-PREVIOUS": "-",
		"STEP":           "-",
		"GATE":           "-",
		"CAPACITY":       "-",
		"TRAFFIC":        "-",
		"ROLLED-OUT":     orDash(component.RolledOutRevisionHash),
		"READY":          orDash(component.ReadyRevisionHash),
		"PREVIOUS":       orDash(component.PreviousRevisionHash),
	}
	if component.Group == nil {
		return values
	}
	values["GROUP"] = fmt.Sprintf("%d", *component.Group)
	values["GROUP-PHASE"] = string(RolloutPhaseUnknown)
	group, found := groups[*component.Group]
	if !found {
		return values
	}
	values["GROUP-PHASE"] = orDash(string(group.Phase))
	values["GROUP-CURRENT"] = orDash(string(group.CurrentComponent))
	values["GROUP-PREVIOUS"] = orDash(string(group.PreviousComponent))
	if group.Step != nil {
		values["STEP"] = fmt.Sprintf("%d/%d", group.Step.Index+1, group.Step.Total)
		values["GATE"] = orDash(string(group.Step.Gate))
		values["CAPACITY"] = orDash(group.Step.Capacity)
		values["TRAFFIC"] = fmt.Sprintf(
			"%d%% -> %d%%", group.Step.TargetTraffic, group.Step.ObservedTraffic,
		)
	}
	return values
}

func compactRolloutCell(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, "; ")
}

func compactRolloutIssueDisplay(issues []RolloutIssue) string {
	withoutComponentScope := make([]RolloutIssue, len(issues))
	copy(withoutComponentScope, issues)
	for i := range withoutComponentScope {
		withoutComponentScope[i].Component = ""
	}
	return rolloutIssueDisplay(withoutComponentScope)
}

// WideTable returns the deterministic, complete operator-focused rollout
// view that preceded the compact component matrix.
func (c RolloutStatusContent) WideTable() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{
		"STATE", "REPORTED-STATE", "EVIDENCE", "EPOCH", "GROUP", "STRATEGY", "GROUP-PHASE",
		"CURRENT-COMPONENT", "PREVIOUS-COMPONENT", "COMPONENT", "COMPONENT-PHASE",
		"STEP", "GATE", "CAPACITY", "TARGET-TRAFFIC", "OBSERVED-TRAFFIC", "ROLLED-OUT", "READY", "PREVIOUS", "ISSUES",
	}}
	groups := make(map[int]RolloutGroupStatus, len(canonical.Groups))
	for _, group := range canonical.Groups {
		groups[group.Index] = group
	}
	for _, component := range canonical.Components {
		issues := rolloutIssueDisplay(rolloutIssuesForComponent(canonical.Issues, component))
		groupName, strategy, groupPhase, currentComponent, previousComponent, step, gate, capacity, targetTraffic, observedTraffic := "-", orDash(string(component.Strategy)), "-", "-", "-", "-", "-", "-", "-", "-"
		if component.Group != nil {
			groupName = fmt.Sprintf("%d", *component.Group)
			groupPhase = string(RolloutPhaseUnknown)
			if group, found := groups[*component.Group]; found {
				groupPhase = orDash(string(group.Phase))
				currentComponent = orDash(string(group.CurrentComponent))
				previousComponent = orDash(string(group.PreviousComponent))
				if group.Step != nil {
					step = fmt.Sprintf("%d/%d", group.Step.Index+1, group.Step.Total)
					gate = orDash(string(group.Step.Gate))
					capacity = orDash(group.Step.Capacity)
					targetTraffic = fmt.Sprintf("%d%%", group.Step.TargetTraffic)
					observedTraffic = fmt.Sprintf("%d%%", group.Step.ObservedTraffic)
				}
			}
		}
		table.Rows = append(table.Rows, []string{
			orDash(string(canonical.Summary.State)), orDash(string(canonical.Summary.ReportedState)),
			orDash(string(canonical.Summary.Evidence)), orDash(string(canonical.Summary.Epoch)),
			groupName, strategy, groupPhase,
			currentComponent, previousComponent,
			orDash(string(component.Type)), orDash(string(component.Phase)),
			step, gate, capacity, targetTraffic, observedTraffic,
			orDash(component.RolledOutRevisionHash), orDash(component.ReadyRevisionHash),
			orDash(component.PreviousRevisionHash), issues,
		})
	}
	if len(table.Rows) == 0 {
		table.Rows = append(table.Rows, rolloutIssueOnlyRow(canonical.Summary, canonical.Issues))
	} else if unmatched := rolloutIssuesWithoutComponentMatch(canonical.Issues, canonical.Components); len(unmatched) > 0 {
		table.Rows = append(table.Rows, rolloutIssueOnlyRow(canonical.Summary, unmatched))
	}
	return table
}

func rolloutIssueOnlyRow(summary RolloutSummary, issues []RolloutIssue) []string {
	return []string{
		orDash(string(summary.State)), orDash(string(summary.ReportedState)),
		orDash(string(summary.Evidence)), orDash(string(summary.Epoch)),
		"-", "-", "-", "-", "-", "-", "-", "-", "-", "-",
		"-", "-", "-", "-", "-", rolloutIssueDisplay(issues),
	}
}

func rolloutRevisionRoleOrder(role RolloutRevisionRole) int {
	switch role {
	case RolloutRevisionCurrent:
		return 0
	case RolloutRevisionTarget:
		return 1
	case RolloutRevisionPrevious:
		return 2
	default:
		return 3
	}
}

func compareRolloutGroups(a, b RolloutGroupStatus) int {
	for _, result := range []int{
		cmp.Compare(a.Index, b.Index),
		cmp.Compare(a.Strategy, b.Strategy),
		cmp.Compare(a.Phase, b.Phase),
		slices.Compare(a.Components, b.Components),
		cmp.Compare(a.CurrentComponent, b.CurrentComponent),
		cmp.Compare(a.PreviousComponent, b.PreviousComponent),
		cmp.Compare(a.ObservedSurge, b.ObservedSurge),
		cmp.Compare(a.StableRevisionHash, b.StableRevisionHash),
		cmp.Compare(a.TargetRevisionHash, b.TargetRevisionHash),
		cmp.Compare(a.RejectedRevisionHash, b.RejectedRevisionHash),
		compareRolloutSteps(a.Step, b.Step),
		compareOptionalTime(a.TransitionedAt, b.TransitionedAt),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRolloutComponents(a, b RolloutComponentStatus) int {
	for _, result := range []int{
		cmp.Compare(componentOrder(a.Type), componentOrder(b.Type)),
		cmp.Compare(a.Type, b.Type),
		cmp.Compare(a.Strategy, b.Strategy),
		compareOptionalInt(a.Group, b.Group),
		cmp.Compare(a.Phase, b.Phase),
		cmp.Compare(a.RolledOutRevisionHash, b.RolledOutRevisionHash),
		cmp.Compare(a.ReadyRevisionHash, b.ReadyRevisionHash),
		cmp.Compare(a.PreviousRevisionHash, b.PreviousRevisionHash),
		compareRolloutTrafficSlices(a.Traffic, b.Traffic),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func canonicalRolloutStrategy(strategy RolloutStrategy) RolloutStrategy {
	switch strategy {
	case RolloutStrategyUnknown,
		RolloutStrategyIndependent,
		RolloutStrategyCanary,
		RolloutStrategyBlueGreen,
		RolloutStrategyRollingUpdate,
		RolloutStrategySequential:
		return strategy
	default:
		return RolloutStrategyUnknown
	}
}

func canonicalRolloutSourceKind(kind RolloutSourceKind) RolloutSourceKind {
	if kind == RolloutSourceInferenceService {
		return kind
	}
	return RolloutSourceInferenceService
}

func canonicalRolloutEvidenceLevel(evidence EvidenceLevel) EvidenceLevel {
	switch evidence {
	case EvidenceDeclared,
		EvidenceReported,
		EvidenceObserved,
		EvidenceComputed,
		EvidenceUnavailable:
		return evidence
	default:
		return EvidenceUnavailable
	}
}

func canonicalRolloutWarningCode(code WarningCode) WarningCode {
	switch code {
	case WarningPartialData,
		WarningSourceUnavailable,
		WarningStaleEvidence,
		WarningTruncated:
		return code
	default:
		return WarningPartialData
	}
}

func canonicalRolloutState(state RolloutState) RolloutState {
	switch state {
	case RolloutStateUnknown,
		RolloutStateNotConfigured,
		RolloutStateSucceeded,
		RolloutStateInProgress,
		RolloutStatePaused,
		RolloutStateStaged,
		RolloutStateFailed,
		RolloutStateRollingBack,
		RolloutStateRolledBack:
		return state
	default:
		return RolloutStateUnknown
	}
}

func canonicalRolloutEpochState(epoch RolloutEpochState) RolloutEpochState {
	switch epoch {
	case RolloutEpochNotApplicable, RolloutEpochUnverifiable:
		return epoch
	default:
		return RolloutEpochUnverifiable
	}
}

func canonicalRolloutConditionState(condition RolloutConditionState) RolloutConditionState {
	switch condition {
	case RolloutConditionNotApplicable,
		RolloutConditionUnobserved,
		RolloutConditionTrue,
		RolloutConditionFalse,
		RolloutConditionUnknown,
		RolloutConditionInvalid:
		return condition
	default:
		return RolloutConditionInvalid
	}
}

func canonicalRolloutComponentType(component RuntimeComponentType) RuntimeComponentType {
	switch component {
	case RuntimeComponentEngine, RuntimeComponentDecoder, RuntimeComponentRouter:
		return component
	default:
		return ""
	}
}

func canonicalRolloutPhase(phase RolloutPhase) RolloutPhase {
	switch phase {
	case RolloutPhaseUnknown,
		RolloutPhaseStable,
		RolloutPhaseCanarying,
		RolloutPhaseBlueGreenStandby,
		RolloutPhasePending,
		RolloutPhasePaused,
		RolloutPhasePromoting,
		RolloutPhaseRollingBack,
		RolloutPhaseRolledBack,
		RolloutPhaseFailed,
		RolloutPhaseIdle,
		RolloutPhaseSurging,
		RolloutPhaseWaiting,
		RolloutPhaseShifting,
		RolloutPhaseUpdating,
		RolloutPhaseDraining,
		RolloutPhaseScalingDown,
		RolloutPhaseStaged,
		RolloutPhaseAwaitingNextComponent:
		return phase
	default:
		return RolloutPhaseUnknown
	}
}

func canonicalRolloutGate(gate RolloutGate) RolloutGate {
	switch gate {
	case RolloutGateUnknown,
		RolloutGateImmediate,
		RolloutGateManual,
		RolloutGateTimed,
		RolloutGateAnalysis:
		return gate
	default:
		return RolloutGateUnknown
	}
}

func canonicalRolloutAnalysisState(analysis RolloutAnalysisState) RolloutAnalysisState {
	switch analysis {
	case "":
		return ""
	case RolloutAnalysisUnobserved,
		RolloutAnalysisPassing,
		RolloutAnalysisFailing,
		RolloutAnalysisInconclusive:
		return analysis
	default:
		return RolloutAnalysisUnobserved
	}
}

func canonicalRolloutRevisionRole(role RolloutRevisionRole) RolloutRevisionRole {
	switch role {
	case RolloutRevisionCurrent,
		RolloutRevisionTarget,
		RolloutRevisionPrevious,
		RolloutRevisionOther:
		return role
	default:
		return RolloutRevisionOther
	}
}

func canonicalRolloutIssueCode(code RolloutIssueCode) RolloutIssueCode {
	switch code {
	case RolloutIssueSpecMalformed,
		RolloutIssueStatusMalformed,
		RolloutIssueGroupStatusMissing,
		RolloutIssueGroupStatusUnexpected,
		RolloutIssueComponentStatusMissing,
		RolloutIssueCanaryStatusMissing,
		RolloutIssueCanaryStatusUnexpected,
		RolloutIssueCanaryStepInvalid,
		RolloutIssueRevisionNameInvalid,
		RolloutIssueTrafficInvalid,
		RolloutIssueAnalysisInconclusive,
		RolloutIssueEpochUnverifiable:
		return code
	default:
		return RolloutIssueStatusMalformed
	}
}

func compareRolloutTrafficSlices(a, b []RolloutTrafficTarget) int {
	for i := 0; i < min(len(a), len(b)); i++ {
		if result := compareRolloutTrafficTargets(a[i], b[i]); result != 0 {
			return result
		}
	}
	return cmp.Compare(len(a), len(b))
}

func compareRolloutTrafficTargets(a, b RolloutTrafficTarget) int {
	for _, result := range []int{
		cmp.Compare(rolloutRevisionRoleOrder(a.Role), rolloutRevisionRoleOrder(b.Role)),
		cmp.Compare(a.Role, b.Role),
		cmp.Compare(a.RevisionHash, b.RevisionHash),
		cmp.Compare(a.Percent, b.Percent),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRolloutSteps(a, b *RolloutStepStatus) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}
	for _, result := range []int{
		cmp.Compare(a.Index, b.Index),
		cmp.Compare(a.Total, b.Total),
		cmp.Compare(a.Capacity, b.Capacity),
		cmp.Compare(a.TargetTraffic, b.TargetTraffic),
		cmp.Compare(a.ObservedTraffic, b.ObservedTraffic),
		cmp.Compare(a.Gate, b.Gate),
		cmp.Compare(a.Analysis, b.Analysis),
		compareOptionalTime(a.EnteredAt, b.EnteredAt),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareOptionalTime(a, b *time.Time) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}
	if a.Equal(*b) {
		return 0
	}
	if a.Before(*b) {
		return -1
	}
	return 1
}

func compareOptionalInt(a, b *int) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}
	if *a < *b {
		return -1
	}
	if *a > *b {
		return 1
	}
	return 0
}

func rolloutIssueDisplay(issues []RolloutIssue) string {
	values := make([]string, len(issues))
	for i, issue := range issues {
		value := string(issue.Code)
		parts := []string{}
		if issue.Group != nil {
			parts = append(parts, fmt.Sprintf("group=%d", *issue.Group))
		}
		if issue.Component != "" {
			parts = append(parts, "component="+string(issue.Component))
		}
		if len(parts) > 0 {
			value += "(" + strings.Join(parts, ",") + ")"
		}
		values[i] = value
	}
	return orDash(strings.Join(values, ","))
}

func rolloutIssuesForComponent(issues []RolloutIssue, component RolloutComponentStatus) []RolloutIssue {
	result := make([]RolloutIssue, 0, len(issues))
	for _, issue := range issues {
		if rolloutIssueMatchesComponent(issue, component) {
			result = append(result, issue)
		}
	}
	return result
}

func rolloutIssuesWithoutComponentMatch(
	issues []RolloutIssue,
	components []RolloutComponentStatus,
) []RolloutIssue {
	result := make([]RolloutIssue, 0, len(issues))
	for _, issue := range issues {
		matched := false
		for _, component := range components {
			if rolloutIssueMatchesComponent(issue, component) {
				matched = true
				break
			}
		}
		if !matched {
			result = append(result, issue)
		}
	}
	return result
}

func rolloutIssueMatchesComponent(issue RolloutIssue, component RolloutComponentStatus) bool {
	if issue.Group != nil && (component.Group == nil || *issue.Group != *component.Group) {
		return false
	}
	return issue.Component == "" || issue.Component == component.Type
}
