package v1alpha1

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/report"
)

const AutoscaleExplainReportKind = "AutoscaleExplainReport"

// AutoscaleExplainState summarizes the declared and reported relationship.
// ReportedMismatch is deliberately observational and makes no causal claim.
type AutoscaleExplainState string

const (
	AutoscaleExplainConsistent       AutoscaleExplainState = "Consistent"
	AutoscaleExplainPartial          AutoscaleExplainState = "Partial"
	AutoscaleExplainReportedMismatch AutoscaleExplainState = "ReportedMismatch"
	AutoscaleExplainUnsupported      AutoscaleExplainState = "Unsupported"
	AutoscaleExplainInvalid          AutoscaleExplainState = "Invalid"
)

type AutoscaleDesiredState string

const (
	AutoscaleDesiredAvailable   AutoscaleDesiredState = "Available"
	AutoscaleDesiredUnavailable AutoscaleDesiredState = "Unavailable"
	AutoscaleDesiredUnsupported AutoscaleDesiredState = "Unsupported"
	AutoscaleDesiredInvalid     AutoscaleDesiredState = "Invalid"
)

type AutoscaleReportedState string

const (
	AutoscaleReportedAvailable   AutoscaleReportedState = "Available"
	AutoscaleReportedNotReported AutoscaleReportedState = "NotReported"
	AutoscaleReportedUnavailable AutoscaleReportedState = "Unavailable"
	AutoscaleReportedInvalid     AutoscaleReportedState = "Invalid"
)

type AutoscaleReconciliationState string

const (
	AutoscaleReconciliationConsistent       AutoscaleReconciliationState = "Consistent"
	AutoscaleReconciliationReportedMismatch AutoscaleReconciliationState = "ReportedMismatch"
	AutoscaleReconciliationNotReported      AutoscaleReconciliationState = "NotReported"
	AutoscaleReconciliationUnavailable      AutoscaleReconciliationState = "Unavailable"
	AutoscaleReconciliationInvalid          AutoscaleReconciliationState = "Invalid"
)

type AutoscaleBoundsState string

const (
	AutoscaleBoundsAvailable   AutoscaleBoundsState = "Available"
	AutoscaleBoundsUnavailable AutoscaleBoundsState = "Unavailable"
	AutoscaleBoundsInvalid     AutoscaleBoundsState = "Invalid"
)

type AutoscaleScaleToZeroState string

const (
	AutoscaleScaleToZeroNotRequested AutoscaleScaleToZeroState = "NotRequested"
	AutoscaleScaleToZeroEligible     AutoscaleScaleToZeroState = "Eligible"
	AutoscaleScaleToZeroUnsupported  AutoscaleScaleToZeroState = "Unsupported"
	AutoscaleScaleToZeroUnavailable  AutoscaleScaleToZeroState = "Unavailable"
	AutoscaleScaleToZeroInvalid      AutoscaleScaleToZeroState = "Invalid"
)

type AutoscaleScalingPolicyState string

const (
	AutoscaleScalingPolicyAvailable   AutoscaleScalingPolicyState = "Available"
	AutoscaleScalingPolicyUnsupported AutoscaleScalingPolicyState = "Unsupported"
	AutoscaleScalingPolicyInvalid     AutoscaleScalingPolicyState = "Invalid"
)

type AutoscaleScalingPolicySource string

const (
	AutoscaleScalingPolicySourceISVC    AutoscaleScalingPolicySource = "isvc"
	AutoscaleScalingPolicySourceRuntime AutoscaleScalingPolicySource = "runtime"
	AutoscaleScalingPolicySourceDefault AutoscaleScalingPolicySource = "default"
)

type AutoscaleScalingMode string

const (
	AutoscaleScalingIndependent  AutoscaleScalingMode = "Independent"
	AutoscaleScalingProportional AutoscaleScalingMode = "Proportional"
	AutoscaleScalingPinned       AutoscaleScalingMode = "Pinned"
	AutoscaleScalingUnknown      AutoscaleScalingMode = "Unknown"
)

// AutoscaleExplainIssueCode is a bounded, message-free explanation code.
type AutoscaleExplainIssueCode string

const (
	AutoscaleExplainIssuePolicyResolutionUnavailable AutoscaleExplainIssueCode = "PolicyResolutionUnavailable"
	AutoscaleExplainIssuePolicyReferenceInvalid      AutoscaleExplainIssueCode = "PolicyReferenceInvalid"
	AutoscaleExplainIssueDeploymentModeUnsupported   AutoscaleExplainIssueCode = "DeploymentModeUnsupported"
	AutoscaleExplainIssueAutoscalerClassInvalid      AutoscaleExplainIssueCode = "AutoscalerClassInvalid"
	AutoscaleExplainIssueKEDATriggersRequired        AutoscaleExplainIssueCode = "KEDATriggersRequired"
	AutoscaleExplainIssueKEDAConfigurationInvalid    AutoscaleExplainIssueCode = "KEDAConfigurationInvalid"
	AutoscaleExplainIssueHPAMetricMalformed          AutoscaleExplainIssueCode = "HPAMetricMalformed"
	AutoscaleExplainIssueKEDAIdleNotBelowMinimum     AutoscaleExplainIssueCode = "KEDAIdleNotBelowMinimum"
	AutoscaleExplainIssueReservedHPANameCollision    AutoscaleExplainIssueCode = "ReservedHPANameCollision"
	AutoscaleExplainIssueLegacyAutoscalerInvalid     AutoscaleExplainIssueCode = "LegacyAutoscalerInvalid"
	AutoscaleExplainIssueReplicaBoundsInvalid        AutoscaleExplainIssueCode = "ReplicaBoundsInvalid"
	AutoscaleExplainIssueScaleToZeroInvalid          AutoscaleExplainIssueCode = "ScaleToZeroInvalid"
	AutoscaleExplainIssueScaleToZeroUnsupported      AutoscaleExplainIssueCode = "ScaleToZeroUnsupported"
	AutoscaleExplainIssueScalingPolicyUnsupported    AutoscaleExplainIssueCode = "ScalingPolicyUnsupported"
	AutoscaleExplainIssueScalingPolicyInvalid        AutoscaleExplainIssueCode = "ScalingPolicyInvalid"
	AutoscaleExplainIssueInheritanceUnavailable      AutoscaleExplainIssueCode = "InheritanceUnavailable"
	AutoscaleExplainIssueActiveRevisionInconsistent  AutoscaleExplainIssueCode = "ActiveRevisionInconsistent"
	AutoscaleExplainIssueStatusNotReported           AutoscaleExplainIssueCode = "StatusNotReported"
	AutoscaleExplainIssueStatusStale                 AutoscaleExplainIssueCode = "StatusStale"
	AutoscaleExplainIssueStatusUnobserved            AutoscaleExplainIssueCode = "StatusUnobserved"
	AutoscaleExplainIssueStatusInvalid               AutoscaleExplainIssueCode = "StatusInvalid"
	AutoscaleExplainIssueReportedEvidenceInvalid     AutoscaleExplainIssueCode = "ReportedEvidenceInvalid"
	AutoscaleExplainIssueReportedEvidencePartial     AutoscaleExplainIssueCode = "ReportedEvidencePartial"
	AutoscaleExplainIssueReportedClassMismatch       AutoscaleExplainIssueCode = "ReportedClassMismatch"
	AutoscaleExplainIssueReportedOwnershipMismatch   AutoscaleExplainIssueCode = "ReportedOwnershipMismatch"
	AutoscaleExplainIssueReportedSpecSourceMismatch  AutoscaleExplainIssueCode = "ReportedSpecSourceMismatch"
	AutoscaleExplainIssueReportedTargetMismatch      AutoscaleExplainIssueCode = "ReportedTargetMismatch"
	AutoscaleExplainIssueReportedComponentUnexpected AutoscaleExplainIssueCode = "ReportedComponentUnexpected"
)

type AutoscaleExplainWarningCode string

const (
	AutoscaleExplainWarningPartialData              AutoscaleExplainWarningCode = "PartialData"
	AutoscaleExplainWarningStaleEvidence            AutoscaleExplainWarningCode = "StaleEvidence"
	AutoscaleExplainWarningUnsupportedConfiguration AutoscaleExplainWarningCode = "UnsupportedConfiguration"
)

type AutoscaleExplainWarning struct {
	Code AutoscaleExplainWarningCode `json:"code"`
}

type AutoscaleExplainIssue struct {
	Code      AutoscaleExplainIssueCode `json:"code"`
	Component RuntimeComponentType      `json:"component,omitempty"`
}

type AutoscaleExplainSummary struct {
	State           AutoscaleExplainState `json:"state"`
	StatusFreshness StatusFreshness       `json:"statusFreshness"`
}

type AutoscaleActiveRevision struct {
	Namespace string              `json:"namespace"`
	Name      string              `json:"name"`
	UID       string              `json:"uid,omitempty"`
	Role      RuntimeRevisionRole `json:"role"`
}

type AutoscaleActiveConfigurationState string

const (
	AutoscaleActiveConfigurationAvailable   AutoscaleActiveConfigurationState = "Available"
	AutoscaleActiveConfigurationUnavailable AutoscaleActiveConfigurationState = "Unavailable"
)

type AutoscaleActiveConfiguration struct {
	State       AutoscaleActiveConfigurationState `json:"state"`
	Origin      ConfigurationOrigin               `json:"origin,omitempty"`
	Consistency RevisionConsistency               `json:"consistency,omitempty"`
	Runtime     *RuntimeObjectReference           `json:"runtime,omitempty"`
	Revision    *AutoscaleActiveRevision          `json:"revision,omitempty"`
	Inheritance *RuntimeInheritance               `json:"inheritance,omitempty"`
}

type AutoscaleScalingPolicy struct {
	State  AutoscaleScalingPolicyState  `json:"state"`
	Mode   AutoscaleScalingMode         `json:"mode"`
	Source AutoscaleScalingPolicySource `json:"source"`
}

// AutoscaleTargetIdentity omits a state field so it can only exist when the
// enclosing explicit state says the target is available/reported.
type AutoscaleTargetIdentity struct {
	APIVersion string              `json:"apiVersion"`
	Kind       AutoscaleTargetKind `json:"kind"`
	Namespace  string              `json:"namespace"`
	Name       string              `json:"name"`
}

type AutoscaleExplainBounds struct {
	State       AutoscaleBoundsState `json:"state"`
	MinReplicas *int32               `json:"minReplicas,omitempty"`
	MaxReplicas *int32               `json:"maxReplicas,omitempty"`
}

type AutoscaleDesiredConfiguration struct {
	State        AutoscaleDesiredState     `json:"state"`
	Class        AutoscaleClass            `json:"class"`
	ManagedBy    AutoscaleManagedBy        `json:"managedBy"`
	SpecSource   AutoscaleSpecSource       `json:"specSource"`
	Target       *AutoscaleTargetIdentity  `json:"target,omitempty"`
	Bounds       AutoscaleExplainBounds    `json:"bounds"`
	ScaleToZero  AutoscaleScaleToZeroState `json:"scaleToZero"`
	MetricCount  *int                      `json:"metricCount"`
	TriggerCount *int                      `json:"triggerCount"`
}

type AutoscaleReportedConfiguration struct {
	State       AutoscaleReportedState    `json:"state"`
	Class       AutoscaleClass            `json:"class"`
	ManagedBy   AutoscaleManagedBy        `json:"managedBy"`
	SpecSource  AutoscaleSpecSource       `json:"specSource"`
	TargetState AutoscaleTargetState      `json:"targetState"`
	Target      *AutoscaleTargetIdentity  `json:"target,omitempty"`
	Replicas    AutoscaleReplicaStatus    `json:"replicas"`
	Conditions  AutoscaleConditionsStatus `json:"conditions"`
}

type AutoscaleReconciliation struct {
	State  AutoscaleReconciliationState `json:"state"`
	Issues []AutoscaleExplainIssueCode  `json:"issues"`
}

type AutoscaleExplainComponent struct {
	Type                 RuntimeComponentType           `json:"type"`
	DeploymentMode       DeploymentMode                 `json:"deploymentMode"`
	DeploymentModeSource DeploymentModeSource           `json:"deploymentModeSource"`
	Desired              AutoscaleDesiredConfiguration  `json:"desired"`
	Reported             AutoscaleReportedConfiguration `json:"reported"`
	Reconciliation       AutoscaleReconciliation        `json:"reconciliation"`
}

type AutoscaleExplainContent struct {
	Summary             AutoscaleExplainSummary      `json:"summary"`
	ActiveConfiguration AutoscaleActiveConfiguration `json:"activeConfiguration"`
	ScalingPolicy       AutoscaleScalingPolicy       `json:"scalingPolicy"`
	Components          []AutoscaleExplainComponent  `json:"components"`
	Issues              []AutoscaleExplainIssue      `json:"issues"`
}

// AutoscaleExplainReport is the stable, allowlisted machine and table output.
type AutoscaleExplainReport struct {
	APIVersion  string                    `json:"apiVersion"`
	Kind        string                    `json:"kind"`
	Metadata    Metadata                  `json:"metadata"`
	CollectedAt time.Time                 `json:"collectedAt"`
	Sources     []RuntimeSourceReference  `json:"sources"`
	Content     AutoscaleExplainContent   `json:"content"`
	Warnings    []AutoscaleExplainWarning `json:"warnings"`
}

func NewAutoscaleExplainReport(metadata Metadata, content AutoscaleExplainContent, clock Clock) AutoscaleExplainReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (AutoscaleExplainReport{
		APIVersion: APIVersion, Kind: AutoscaleExplainReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []RuntimeSourceReference{}, Content: content,
		Warnings: []AutoscaleExplainWarning{},
	}).Canonical()
}

func (r AutoscaleExplainReport) Canonical() AutoscaleExplainReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = AutoscaleExplainReportKind
	result.CollectedAt = r.CollectedAt.UTC()
	result.Sources = append([]RuntimeSourceReference{}, r.Sources...)
	for i := range result.Sources {
		if result.Sources[i].CollectedAt.IsZero() {
			result.Sources[i].CollectedAt = result.CollectedAt
		} else {
			result.Sources[i].CollectedAt = result.Sources[i].CollectedAt.UTC()
		}
	}
	sort.Slice(result.Sources, func(i, j int) bool { return runtimeSourceLess(result.Sources[i], result.Sources[j]) })
	result.Sources = dedupeRuntimeSources(result.Sources)
	result.Content = r.Content.Canonical()
	result.Warnings = append([]AutoscaleExplainWarning{}, r.Warnings...)
	sort.Slice(result.Warnings, func(i, j int) bool { return result.Warnings[i].Code < result.Warnings[j].Code })
	result.Warnings = dedupeAutoscaleExplainWarnings(result.Warnings)
	return result
}

func (r AutoscaleExplainReport) Table() report.Table {
	return r.Canonical().Content.Table()
}

func (c AutoscaleExplainContent) Canonical() AutoscaleExplainContent {
	result := c
	if c.ActiveConfiguration.State == AutoscaleActiveConfigurationUnavailable {
		result.ActiveConfiguration.Origin = ""
		result.ActiveConfiguration.Consistency = ""
		result.ActiveConfiguration.Runtime = nil
		result.ActiveConfiguration.Revision = nil
		result.ActiveConfiguration.Inheritance = nil
	} else {
		result.ActiveConfiguration.Runtime = copyRuntimeObjectReference(c.ActiveConfiguration.Runtime)
		if c.ActiveConfiguration.Revision != nil {
			revision := *c.ActiveConfiguration.Revision
			result.ActiveConfiguration.Revision = &revision
		}
		if c.ActiveConfiguration.Inheritance != nil {
			inheritance := *c.ActiveConfiguration.Inheritance
			inheritance.Sources = append([]RuntimeObjectReference{}, c.ActiveConfiguration.Inheritance.Sources...)
			result.ActiveConfiguration.Inheritance = &inheritance
		}
	}
	result.Components = make([]AutoscaleExplainComponent, len(c.Components))
	for i := range c.Components {
		result.Components[i] = canonicalAutoscaleExplainComponent(c.Components[i])
	}
	sort.Slice(result.Components, func(i, j int) bool {
		if componentOrder(result.Components[i].Type) != componentOrder(result.Components[j].Type) {
			return componentOrder(result.Components[i].Type) < componentOrder(result.Components[j].Type)
		}
		if result.Components[i].Type != result.Components[j].Type {
			return result.Components[i].Type < result.Components[j].Type
		}
		return autoscaleExplainComponentSortKey(result.Components[i]) < autoscaleExplainComponentSortKey(result.Components[j])
	})
	result.Issues = append([]AutoscaleExplainIssue{}, c.Issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		if componentOrder(result.Issues[i].Component) != componentOrder(result.Issues[j].Component) {
			return componentOrder(result.Issues[i].Component) < componentOrder(result.Issues[j].Component)
		}
		if result.Issues[i].Component != result.Issues[j].Component {
			return result.Issues[i].Component < result.Issues[j].Component
		}
		return result.Issues[i].Code < result.Issues[j].Code
	})
	result.Issues = dedupeAutoscaleExplainIssues(result.Issues)
	return result
}

func autoscaleExplainComponentSortKey(component AutoscaleExplainComponent) string {
	encoded, err := json.Marshal(component)
	if err != nil {
		// The contract has no generally non-JSON-marshalable fields. Retain a
		// deterministic structural fallback for adversarial in-memory values.
		return fmt.Sprintf("%#v", component)
	}
	return string(encoded)
}

func (c AutoscaleExplainContent) Table() report.Table {
	canonical := c.Canonical()
	unattachedIssues := canonical.unattachedIssues()
	if len(canonical.Components) == 0 {
		return report.Table{
			Headers: []string{"FIELD", "SERVICE"},
			Rows: [][]string{
				{"STATE", explainStateCell(canonical.Summary.State)},
				{"POLICY", policyCell(canonical.ScalingPolicy)},
				{"WHY", serviceWhyCell(unattachedIssues)},
			},
		}
	}
	table := report.Table{
		Headers: []string{"FIELD"},
		Rows: [][]string{
			{"STATE"},
			{"MODE"},
			{"POLICY"},
			{"DESIRED"},
			{"RANGE"},
			{"ZERO"},
			{"EXPECTED-TARGET"},
			{"REPORTED"},
			{"REPORTED-TARGET"},
			{"CUR/DES"},
			{"LAST-SCALE"},
			{"CONDITION-EVIDENCE"},
			{"CONDITIONS"},
			{"CHECK"},
			{"WHY"},
		},
	}
	for _, component := range canonical.Components {
		table.Headers = append(table.Headers, strings.ToUpper(string(component.Type)))
		values := []string{
			explainStateCell(canonical.Summary.State),
			deploymentModeCell(component.DeploymentMode),
			policyCell(canonical.ScalingPolicy),
			desiredCell(component.Desired),
			boundsCell(component.Desired.Bounds),
			scaleToZeroCell(component.Desired.ScaleToZero),
			desiredTargetCell(component.Desired),
			reportedCell(component.Reported),
			reportedTargetCell(component.Reported),
			reportedReplicaCell(component.Reported.Replicas),
			reportedLastScaleCell(component.Reported.Replicas),
			reportedConditionEvidenceCell(component.Reported.Conditions),
			reportedConditionsCell(component.Reported.Conditions),
			reconciliationCell(component.Reconciliation.State),
			whyCell(component.Reconciliation.Issues),
		}
		for row := range table.Rows {
			table.Rows[row] = append(table.Rows[row], values[row])
		}
	}
	if len(unattachedIssues) > 0 {
		table.Headers = append(table.Headers, "SERVICE")
		values := []string{
			explainStateCell(canonical.Summary.State),
			"-",
			policyCell(canonical.ScalingPolicy),
			"-",
			"-",
			"-",
			"-",
			"-",
			"-",
			"-",
			"-",
			"-",
			"-",
			"-",
			serviceWhyCell(unattachedIssues),
		}
		for row := range table.Rows {
			table.Rows[row] = append(table.Rows[row], values[row])
		}
	}
	return table
}

func (c AutoscaleExplainContent) unattachedIssues() []AutoscaleExplainIssue {
	result := []AutoscaleExplainIssue{}
	for _, issue := range c.Issues {
		attached := false
		for _, component := range c.Components {
			if issue.Component != "" && issue.Component != component.Type {
				continue
			}
			for _, code := range component.Reconciliation.Issues {
				if code == issue.Code {
					attached = true
					break
				}
			}
			if attached {
				break
			}
		}
		if !attached {
			result = append(result, issue)
		}
	}
	return result
}

func canonicalAutoscaleExplainComponent(component AutoscaleExplainComponent) AutoscaleExplainComponent {
	result := component
	result.Desired.Target = copyAutoscaleTargetIdentity(component.Desired.Target)
	if component.Desired.State != AutoscaleDesiredAvailable {
		result.Desired.Target = nil
	}
	result.Desired.Bounds.MinReplicas = copyInt32(component.Desired.Bounds.MinReplicas)
	result.Desired.Bounds.MaxReplicas = copyInt32(component.Desired.Bounds.MaxReplicas)
	if component.Desired.Bounds.State != AutoscaleBoundsAvailable {
		result.Desired.Bounds.MinReplicas = nil
		result.Desired.Bounds.MaxReplicas = nil
	}
	result.Desired.MetricCount = copyInt(component.Desired.MetricCount)
	result.Desired.TriggerCount = copyInt(component.Desired.TriggerCount)
	result.Reported.Target = copyAutoscaleTargetIdentity(component.Reported.Target)
	if component.Reported.TargetState != AutoscaleTargetReported {
		result.Reported.Target = nil
	}
	status := canonicalAutoscaleComponent(AutoscaleComponentStatus{
		Replicas: component.Reported.Replicas, Conditions: component.Reported.Conditions,
	})
	result.Reported.Replicas = status.Replicas
	result.Reported.Conditions = status.Conditions
	result.Reconciliation.Issues = canonicalAutoscaleExplainIssueCodes(component.Reconciliation.Issues)
	return result
}

func copyAutoscaleTargetIdentity(value *AutoscaleTargetIdentity) *AutoscaleTargetIdentity {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func copyInt(value *int) *int {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func dedupeRuntimeSources(values []RuntimeSourceReference) []RuntimeSourceReference {
	write := 0
	for _, value := range values {
		if write > 0 && !runtimeSourceLess(values[write-1], value) && !runtimeSourceLess(value, values[write-1]) {
			continue
		}
		values[write] = value
		write++
	}
	return values[:write]
}

func dedupeAutoscaleExplainWarnings(values []AutoscaleExplainWarning) []AutoscaleExplainWarning {
	write := 0
	for _, value := range values {
		if value.Code == "" || (write > 0 && values[write-1].Code == value.Code) {
			continue
		}
		values[write] = value
		write++
	}
	return values[:write]
}

func dedupeAutoscaleExplainIssues(values []AutoscaleExplainIssue) []AutoscaleExplainIssue {
	write := 0
	for _, value := range values {
		if value.Code == "" || (write > 0 && values[write-1] == value) {
			continue
		}
		values[write] = value
		write++
	}
	return values[:write]
}

func canonicalAutoscaleExplainIssueCodes(values []AutoscaleExplainIssueCode) []AutoscaleExplainIssueCode {
	result := append([]AutoscaleExplainIssueCode{}, values...)
	sort.Slice(result, func(i, j int) bool {
		left, right := autoscaleExplainIssuePriority(result[i]), autoscaleExplainIssuePriority(result[j])
		if left != right {
			return left < right
		}
		return result[i] < result[j]
	})
	write := 0
	for _, value := range result {
		if value == "" || (write > 0 && result[write-1] == value) {
			continue
		}
		result[write] = value
		write++
	}
	return result[:write]
}

func autoscaleExplainIssuePriority(value AutoscaleExplainIssueCode) int {
	switch value {
	case AutoscaleExplainIssueAutoscalerClassInvalid,
		AutoscaleExplainIssuePolicyReferenceInvalid,
		AutoscaleExplainIssueKEDATriggersRequired,
		AutoscaleExplainIssueKEDAConfigurationInvalid,
		AutoscaleExplainIssueHPAMetricMalformed,
		AutoscaleExplainIssueKEDAIdleNotBelowMinimum,
		AutoscaleExplainIssueReservedHPANameCollision,
		AutoscaleExplainIssueLegacyAutoscalerInvalid,
		AutoscaleExplainIssueReplicaBoundsInvalid,
		AutoscaleExplainIssueScaleToZeroInvalid,
		AutoscaleExplainIssueScalingPolicyInvalid,
		AutoscaleExplainIssueStatusInvalid,
		AutoscaleExplainIssueReportedEvidenceInvalid:
		return 0
	case AutoscaleExplainIssueDeploymentModeUnsupported,
		AutoscaleExplainIssueScaleToZeroUnsupported,
		AutoscaleExplainIssueScalingPolicyUnsupported:
		return 1
	case AutoscaleExplainIssueReportedClassMismatch,
		AutoscaleExplainIssueReportedOwnershipMismatch,
		AutoscaleExplainIssueReportedSpecSourceMismatch,
		AutoscaleExplainIssueReportedTargetMismatch,
		AutoscaleExplainIssueReportedComponentUnexpected:
		return 2
	case AutoscaleExplainIssuePolicyResolutionUnavailable,
		AutoscaleExplainIssueInheritanceUnavailable,
		AutoscaleExplainIssueActiveRevisionInconsistent,
		AutoscaleExplainIssueStatusNotReported,
		AutoscaleExplainIssueStatusStale,
		AutoscaleExplainIssueStatusUnobserved,
		AutoscaleExplainIssueReportedEvidencePartial:
		return 3
	default:
		return 4
	}
}

func explainStateCell(value AutoscaleExplainState) string {
	switch value {
	case AutoscaleExplainConsistent:
		return "OK"
	case AutoscaleExplainReportedMismatch:
		return "Mismatch"
	default:
		return orDash(string(value))
	}
}

func deploymentModeCell(value DeploymentMode) string {
	switch value {
	case DeploymentModeRawDeployment:
		return "Raw"
	case DeploymentModeOMENative:
		return "Native"
	case DeploymentModeMultiNode:
		return "Multi"
	case DeploymentModeVirtualDeployment:
		return "Virtual"
	default:
		return orDash(string(value))
	}
}

func policyCell(value AutoscaleScalingPolicy) string {
	if value.Mode == "" {
		return "-"
	}
	return string(value.Mode)
}

func desiredCell(value AutoscaleDesiredConfiguration) string {
	switch value.State {
	case AutoscaleDesiredAvailable:
		return string(value.Class) + "/" + string(value.SpecSource)
	case AutoscaleDesiredUnavailable:
		return "?/" + orDash(string(value.SpecSource))
	case AutoscaleDesiredUnsupported:
		return "unsupported"
	case AutoscaleDesiredInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func boundsCell(value AutoscaleExplainBounds) string {
	if value.State != AutoscaleBoundsAvailable || value.MinReplicas == nil || value.MaxReplicas == nil {
		if value.State == AutoscaleBoundsInvalid {
			return "invalid"
		}
		return "?"
	}
	return strconv.FormatInt(int64(*value.MinReplicas), 10) + ".." + strconv.FormatInt(int64(*value.MaxReplicas), 10)
}

func scaleToZeroCell(value AutoscaleScaleToZeroState) string {
	switch value {
	case AutoscaleScaleToZeroNotRequested:
		return "-"
	case AutoscaleScaleToZeroEligible:
		return "yes"
	case AutoscaleScaleToZeroUnavailable:
		return "?"
	case AutoscaleScaleToZeroUnsupported:
		return "unsupported"
	case AutoscaleScaleToZeroInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func reportedCell(value AutoscaleReportedConfiguration) string {
	switch value.State {
	case AutoscaleReportedAvailable:
		return string(value.Class) + "/" + string(value.SpecSource) + "/" + string(value.ManagedBy)
	case AutoscaleReportedNotReported:
		return "-"
	case AutoscaleReportedUnavailable:
		return "?"
	case AutoscaleReportedInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func desiredTargetCell(value AutoscaleDesiredConfiguration) string {
	switch value.State {
	case AutoscaleDesiredAvailable:
		return autoscaleTargetIdentityCell(value.Target)
	case AutoscaleDesiredUnavailable:
		return "?"
	case AutoscaleDesiredUnsupported:
		return "unsupported"
	case AutoscaleDesiredInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func reportedTargetCell(value AutoscaleReportedConfiguration) string {
	switch value.TargetState {
	case AutoscaleTargetReported:
		return autoscaleTargetIdentityCell(value.Target)
	case AutoscaleTargetNotReported:
		return "-"
	case AutoscaleTargetUnavailable:
		return "?"
	case AutoscaleTargetInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func autoscaleTargetIdentityCell(value *AutoscaleTargetIdentity) string {
	if value == nil || value.Kind == "" || value.Name == "" {
		return "?"
	}
	return string(value.Kind) + "/" + value.Name
}

func reportedReplicaCell(value AutoscaleReplicaStatus) string {
	switch value.State {
	case AutoscaleReplicasReported:
		return autoscaleReplicaPair(value.CurrentReplicas, value.DesiredReplicas, "")
	case AutoscaleReplicasAmbiguous:
		return autoscaleReplicaPair(value.CurrentReplicas, value.DesiredReplicas, "?")
	case AutoscaleReplicasNotReported:
		return "-"
	case AutoscaleReplicasUnavailable:
		return "?"
	case AutoscaleReplicasInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func reportedLastScaleCell(value AutoscaleReplicaStatus) string {
	switch value.State {
	case AutoscaleReplicasReported, AutoscaleReplicasAmbiguous:
		return autoscaleTimeCell(value.LastScaleTime)
	case AutoscaleReplicasNotReported:
		return "-"
	case AutoscaleReplicasUnavailable:
		return "?"
	case AutoscaleReplicasInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func reportedConditionEvidenceCell(value AutoscaleConditionsStatus) string {
	switch value.State {
	case AutoscaleConditionsReported:
		return "reported"
	case AutoscaleConditionsNotReported:
		return "missing"
	case AutoscaleConditionsUnavailable:
		return "unknown"
	case AutoscaleConditionsInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func reportedConditionsCell(value AutoscaleConditionsStatus) string {
	switch value.State {
	case AutoscaleConditionsReported:
		return autoscaleConditionsCell(value.Items)
	case AutoscaleConditionsNotReported:
		return "-"
	case AutoscaleConditionsUnavailable:
		if len(value.Items) == 0 {
			return "?"
		}
		return autoscaleConditionsCell(value.Items)
	case AutoscaleConditionsInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func autoscaleReplicaPair(current, desired *int32, suffix string) string {
	if current == nil || desired == nil {
		return "?"
	}
	return strconv.FormatInt(int64(*current), 10) + "/" + strconv.FormatInt(int64(*desired), 10) + suffix
}

func reconciliationCell(value AutoscaleReconciliationState) string {
	switch value {
	case AutoscaleReconciliationConsistent:
		return "match"
	case AutoscaleReconciliationReportedMismatch:
		return "mismatch"
	case AutoscaleReconciliationNotReported:
		return "missing"
	case AutoscaleReconciliationUnavailable:
		return "unknown"
	case AutoscaleReconciliationInvalid:
		return "invalid"
	default:
		return "?"
	}
}

func whyCell(issues []AutoscaleExplainIssueCode) string {
	issues = canonicalAutoscaleExplainIssueCodes(issues)
	if len(issues) == 0 {
		return "-"
	}
	result := explainIssueAlias(issues[0])
	if len(issues) > 1 {
		result += ",+" + strconv.Itoa(len(issues)-1)
	}
	return result
}

func serviceWhyCell(issues []AutoscaleExplainIssue) string {
	if len(issues) == 0 {
		return "-"
	}
	values := make([]string, 0, len(issues))
	for _, issue := range issues {
		prefix := ""
		switch issue.Component {
		case RuntimeComponentEngine, RuntimeComponentDecoder, RuntimeComponentRouter:
			prefix = strings.ToLower(string(issue.Component)) + ":"
		}
		values = append(values, prefix+explainIssueAlias(issue.Code))
	}
	return strings.Join(values, ",")
}

func explainIssueAlias(value AutoscaleExplainIssueCode) string {
	switch value {
	case AutoscaleExplainIssueAutoscalerClassInvalid:
		return "class-invalid"
	case AutoscaleExplainIssueKEDATriggersRequired:
		return "keda-triggers"
	case AutoscaleExplainIssueKEDAConfigurationInvalid:
		return "keda-config-invalid"
	case AutoscaleExplainIssueHPAMetricMalformed:
		return "hpa-metric-invalid"
	case AutoscaleExplainIssueKEDAIdleNotBelowMinimum:
		return "keda-idle-invalid"
	case AutoscaleExplainIssuePolicyResolutionUnavailable:
		return "policy-unavailable"
	case AutoscaleExplainIssuePolicyReferenceInvalid:
		return "policy-ref-invalid"
	case AutoscaleExplainIssueDeploymentModeUnsupported:
		return "mode-unsupported"
	case AutoscaleExplainIssueReportedClassMismatch:
		return "class-mismatch"
	case AutoscaleExplainIssueReportedOwnershipMismatch:
		return "owner-mismatch"
	case AutoscaleExplainIssueReportedSpecSourceMismatch:
		return "source-mismatch"
	case AutoscaleExplainIssueReportedTargetMismatch:
		return "target-mismatch"
	case AutoscaleExplainIssueReportedComponentUnexpected:
		return "unexpected-component"
	case AutoscaleExplainIssueInheritanceUnavailable:
		return "inheritance-unavailable"
	case AutoscaleExplainIssueStatusStale:
		return "status-stale"
	case AutoscaleExplainIssueStatusUnobserved:
		return "status-unobserved"
	case AutoscaleExplainIssueStatusNotReported:
		return "status-missing"
	case AutoscaleExplainIssueStatusInvalid, AutoscaleExplainIssueReportedEvidenceInvalid:
		return "status-invalid"
	case AutoscaleExplainIssueActiveRevisionInconsistent:
		return "revision-inconsistent"
	case AutoscaleExplainIssueReservedHPANameCollision:
		return "keda-name-conflict"
	case AutoscaleExplainIssueLegacyAutoscalerInvalid:
		return "legacy-invalid"
	case AutoscaleExplainIssueReplicaBoundsInvalid:
		return "bounds-invalid"
	case AutoscaleExplainIssueScaleToZeroInvalid:
		return "zero-invalid"
	case AutoscaleExplainIssueScaleToZeroUnsupported:
		return "zero-unsupported"
	case AutoscaleExplainIssueScalingPolicyUnsupported:
		return "policy-unsupported"
	case AutoscaleExplainIssueScalingPolicyInvalid:
		return "policy-invalid"
	case AutoscaleExplainIssueReportedEvidencePartial:
		return "status-partial"
	default:
		text := strings.ToLower(string(value))
		if len(text) > 20 {
			return text[:19] + "~"
		}
		return orDash(text)
	}
}
