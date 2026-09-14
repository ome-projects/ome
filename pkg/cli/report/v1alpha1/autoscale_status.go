package v1alpha1

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const AutoscaleStatusReportKind = "AutoscaleStatusReport"

type AutoscaleSourceKind string

const AutoscaleSourceInferenceService AutoscaleSourceKind = "InferenceService"

type AutoscaleState string

const (
	AutoscaleStateReported    AutoscaleState = "Reported"
	AutoscaleStatePartial     AutoscaleState = "Partial"
	AutoscaleStateUnavailable AutoscaleState = "Unavailable"
	AutoscaleStateInvalid     AutoscaleState = "Invalid"
)

type AutoscaleComponentState string

const (
	AutoscaleComponentReported    AutoscaleComponentState = "Reported"
	AutoscaleComponentPartial     AutoscaleComponentState = "Partial"
	AutoscaleComponentNotReported AutoscaleComponentState = "NotReported"
	AutoscaleComponentInvalid     AutoscaleComponentState = "Invalid"
)

type AutoscaleClass string

const (
	AutoscaleClassHPA      AutoscaleClass = "HPA"
	AutoscaleClassKEDA     AutoscaleClass = "KEDA"
	AutoscaleClassExternal AutoscaleClass = "External"
	AutoscaleClassNone     AutoscaleClass = "None"
	AutoscaleClassUnknown  AutoscaleClass = "Unknown"
)

type AutoscaleManagedBy string

const (
	AutoscaleManagedByOME      AutoscaleManagedBy = "ome"
	AutoscaleManagedByExternal AutoscaleManagedBy = "external"
	AutoscaleManagedByNone     AutoscaleManagedBy = "none"
	AutoscaleManagedByUnknown  AutoscaleManagedBy = "Unknown"
)

type AutoscaleSpecSource string

const (
	AutoscaleSpecSourceISVC    AutoscaleSpecSource = "isvc"
	AutoscaleSpecSourcePolicy  AutoscaleSpecSource = "policy"
	AutoscaleSpecSourceRuntime AutoscaleSpecSource = "runtime"
	AutoscaleSpecSourceLegacy  AutoscaleSpecSource = "legacy"
	AutoscaleSpecSourceDefault AutoscaleSpecSource = "default"
	AutoscaleSpecSourceUnknown AutoscaleSpecSource = "Unknown"
)

type AutoscaleTargetState string

const (
	AutoscaleTargetReported    AutoscaleTargetState = "Reported"
	AutoscaleTargetNotReported AutoscaleTargetState = "NotReported"
	AutoscaleTargetUnavailable AutoscaleTargetState = "Unavailable"
	AutoscaleTargetInvalid     AutoscaleTargetState = "Invalid"
)

type AutoscaleTargetKind string

const (
	AutoscaleTargetDeployment       AutoscaleTargetKind = "Deployment"
	AutoscaleTargetInferenceReplica AutoscaleTargetKind = "InferenceReplica"
)

type AutoscaleReplicaState string

const (
	AutoscaleReplicasReported    AutoscaleReplicaState = "Reported"
	AutoscaleReplicasAmbiguous   AutoscaleReplicaState = "Ambiguous"
	AutoscaleReplicasNotReported AutoscaleReplicaState = "NotReported"
	AutoscaleReplicasUnavailable AutoscaleReplicaState = "Unavailable"
	AutoscaleReplicasInvalid     AutoscaleReplicaState = "Invalid"
)

type AutoscaleConditionType string

const (
	AutoscaleConditionAbleToScale    AutoscaleConditionType = "AbleToScale"
	AutoscaleConditionScalingActive  AutoscaleConditionType = "ScalingActive"
	AutoscaleConditionScalingLimited AutoscaleConditionType = "ScalingLimited"
	AutoscaleConditionReady          AutoscaleConditionType = "Ready"
	AutoscaleConditionActive         AutoscaleConditionType = "Active"
	AutoscaleConditionFallback       AutoscaleConditionType = "Fallback"
	AutoscaleConditionPaused         AutoscaleConditionType = "Paused"
)

type AutoscaleConditionStatus string

const (
	AutoscaleConditionTrue    AutoscaleConditionStatus = "True"
	AutoscaleConditionFalse   AutoscaleConditionStatus = "False"
	AutoscaleConditionUnknown AutoscaleConditionStatus = "Unknown"
)

type AutoscaleConditionsState string

const (
	AutoscaleConditionsReported    AutoscaleConditionsState = "Reported"
	AutoscaleConditionsNotReported AutoscaleConditionsState = "NotReported"
	AutoscaleConditionsUnavailable AutoscaleConditionsState = "Unavailable"
	AutoscaleConditionsInvalid     AutoscaleConditionsState = "Invalid"
)

type AutoscaleIssueCode string

const (
	AutoscaleIssueUnknownComponentStatus   AutoscaleIssueCode = "UnknownComponentStatus"
	AutoscaleIssueAutoscalerNotReported    AutoscaleIssueCode = "AutoscalerNotReported"
	AutoscaleIssueScaleTargetNotReported   AutoscaleIssueCode = "ScaleTargetNotReported"
	AutoscaleIssueClassInvalid             AutoscaleIssueCode = "ClassInvalid"
	AutoscaleIssueManagedByInvalid         AutoscaleIssueCode = "ManagedByInvalid"
	AutoscaleIssueOwnershipMismatch        AutoscaleIssueCode = "OwnershipMismatch"
	AutoscaleIssueSpecSourceInvalid        AutoscaleIssueCode = "SpecSourceInvalid"
	AutoscaleIssueUnexpectedScalerEvidence AutoscaleIssueCode = "UnexpectedScalerEvidence"
	AutoscaleIssueReplicaEvidenceAmbiguous AutoscaleIssueCode = "ReplicaEvidenceAmbiguous"
	AutoscaleIssueReplicaEvidenceInvalid   AutoscaleIssueCode = "ReplicaEvidenceInvalid"
	AutoscaleIssueScaleTargetInvalid       AutoscaleIssueCode = "ScaleTargetInvalid"
	AutoscaleIssueConditionInvalid         AutoscaleIssueCode = "ConditionInvalid"
	AutoscaleIssueConditionConflict        AutoscaleIssueCode = "ConditionConflict"
)

type AutoscaleWarningCode string

const (
	AutoscaleWarningPartialData AutoscaleWarningCode = "PartialData"
)

// AutoscaleSourceReference identifies the one allowlisted parent object used
// to build a status report. It deliberately omits Kubernetes versioning and
// metadata fields that are not part of the evidence contract.
type AutoscaleSourceReference struct {
	Kind        AutoscaleSourceKind `json:"kind"`
	Namespace   string              `json:"namespace"`
	Name        string              `json:"name"`
	UID         string              `json:"uid"`
	Generation  int64               `json:"generation"`
	Evidence    EvidenceLevel       `json:"evidence"`
	CollectedAt time.Time           `json:"collectedAt"`
}

type AutoscaleSummary struct {
	State AutoscaleState `json:"state"`
}

type AutoscaleTarget struct {
	State      AutoscaleTargetState `json:"state"`
	APIVersion string               `json:"apiVersion,omitempty"`
	Kind       AutoscaleTargetKind  `json:"kind,omitempty"`
	Namespace  string               `json:"namespace,omitempty"`
	Name       string               `json:"name,omitempty"`
}

type AutoscaleReplicaStatus struct {
	State           AutoscaleReplicaState `json:"state"`
	CurrentReplicas *int32                `json:"currentReplicas,omitempty"`
	DesiredReplicas *int32                `json:"desiredReplicas,omitempty"`
	LastScaleTime   *time.Time            `json:"lastScaleTime,omitempty"`
}

type AutoscaleCondition struct {
	Type               AutoscaleConditionType   `json:"type"`
	Status             AutoscaleConditionStatus `json:"status"`
	LastTransitionTime time.Time                `json:"lastTransitionTime"`
}

type AutoscaleConditionsStatus struct {
	State AutoscaleConditionsState `json:"state"`
	Items []AutoscaleCondition     `json:"items"`
}

type AutoscaleComponentStatus struct {
	Type       RuntimeComponentType      `json:"type"`
	State      AutoscaleComponentState   `json:"state"`
	Class      AutoscaleClass            `json:"class"`
	ManagedBy  AutoscaleManagedBy        `json:"managedBy"`
	SpecSource AutoscaleSpecSource       `json:"specSource"`
	Target     AutoscaleTarget           `json:"target"`
	Replicas   AutoscaleReplicaStatus    `json:"replicas"`
	Conditions AutoscaleConditionsStatus `json:"conditions"`
}

type AutoscaleIssue struct {
	Code      AutoscaleIssueCode   `json:"code"`
	Component RuntimeComponentType `json:"component,omitempty"`
}

type AutoscaleWarning struct {
	Code AutoscaleWarningCode `json:"code"`
}

type AutoscaleStatusContent struct {
	Summary    AutoscaleSummary           `json:"summary"`
	Components []AutoscaleComponentStatus `json:"components"`
	Issues     []AutoscaleIssue           `json:"issues"`
}

// AutoscaleStatusReport is a dedicated message-free status contract. Values
// in it are controller-reported evidence, never a claim about current cluster
// state.
type AutoscaleStatusReport struct {
	APIVersion  string                     `json:"apiVersion"`
	Kind        string                     `json:"kind"`
	Metadata    Metadata                   `json:"metadata"`
	CollectedAt time.Time                  `json:"collectedAt"`
	Sources     []AutoscaleSourceReference `json:"sources"`
	Content     AutoscaleStatusContent     `json:"content"`
	Warnings    []AutoscaleWarning         `json:"warnings"`
}

func NewAutoscaleStatusReport(metadata Metadata, content AutoscaleStatusContent, clock Clock) AutoscaleStatusReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (AutoscaleStatusReport{
		APIVersion: APIVersion, Kind: AutoscaleStatusReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []AutoscaleSourceReference{},
		Content: content, Warnings: []AutoscaleWarning{},
	}).Canonical()
}

func (r AutoscaleStatusReport) Canonical() AutoscaleStatusReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = AutoscaleStatusReportKind
	result.CollectedAt = r.CollectedAt.UTC()
	result.Sources = append([]AutoscaleSourceReference{}, r.Sources...)
	for i := range result.Sources {
		if result.Sources[i].CollectedAt.IsZero() {
			result.Sources[i].CollectedAt = result.CollectedAt
		} else {
			result.Sources[i].CollectedAt = result.Sources[i].CollectedAt.UTC()
		}
	}
	sort.Slice(result.Sources, func(i, j int) bool { return autoscaleSourceLess(result.Sources[i], result.Sources[j]) })
	result.Content = r.Content.Canonical()
	result.Warnings = append([]AutoscaleWarning{}, r.Warnings...)
	sort.Slice(result.Warnings, func(i, j int) bool { return result.Warnings[i].Code < result.Warnings[j].Code })
	result.Warnings = dedupeAutoscaleWarnings(result.Warnings)
	return result
}

func (r AutoscaleStatusReport) Table() report.Table { return r.Canonical().Content.Table() }

// WideTable returns the complete legacy autoscaling table. The default table
// is deliberately compact; operators can opt into this form when they need
// full target identities, timestamps, condition names, and evidence columns.
func (r AutoscaleStatusReport) WideTable() report.Table {
	return r.Canonical().Content.WideTable()
}

func (c AutoscaleStatusContent) Canonical() AutoscaleStatusContent {
	result := c
	result.Components = make([]AutoscaleComponentStatus, len(c.Components))
	for i := range c.Components {
		result.Components[i] = canonicalAutoscaleComponent(c.Components[i])
	}
	sort.Slice(result.Components, func(i, j int) bool {
		return compareAutoscaleComponents(result.Components[i], result.Components[j]) < 0
	})
	result.Issues = append([]AutoscaleIssue{}, c.Issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		if autoscaleComponentRank(result.Issues[i].Component) != autoscaleComponentRank(result.Issues[j].Component) {
			return autoscaleComponentRank(result.Issues[i].Component) < autoscaleComponentRank(result.Issues[j].Component)
		}
		if result.Issues[i].Component != result.Issues[j].Component {
			return result.Issues[i].Component < result.Issues[j].Component
		}
		return result.Issues[i].Code < result.Issues[j].Code
	})
	result.Issues = dedupeAutoscaleIssues(result.Issues)
	return result
}

func (c AutoscaleStatusContent) Table() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{"FIELD", "SERVICE", "ENGINE", "DECODER", "ROUTER"}}
	componentTypes := []RuntimeComponentType{
		RuntimeComponentEngine,
		RuntimeComponentDecoder,
		RuntimeComponentRouter,
	}
	valuesByComponent := make(map[RuntimeComponentType]map[string][]string, len(componentTypes))
	unknownComponents := 0
	for _, component := range canonical.Components {
		if autoscaleComponentRank(component.Type) >= len(componentTypes) {
			unknownComponents++
			continue
		}
		if valuesByComponent[component.Type] == nil {
			valuesByComponent[component.Type] = make(map[string][]string)
		}
		for field, value := range compactAutoscaleComponentValues(component) {
			valuesByComponent[component.Type][field] = append(
				valuesByComponent[component.Type][field], value,
			)
		}
	}

	stateRow := []string{"STATE", compactAutoscaleCell([]string{string(canonical.Summary.State)})}
	for _, componentType := range componentTypes {
		stateRow = append(stateRow, compactAutoscaleCell(valuesByComponent[componentType]["STATE"]))
	}
	table.Rows = append(table.Rows, stateRow)

	for _, field := range []string{
		"CLASS",
		"MANAGED-BY",
		"SPEC-SOURCE",
		"TARGET-KIND",
		"TARGET-NAME",
		"TARGET-EVIDENCE",
		"CURRENT",
		"DESIRED",
		"REPLICA-EVIDENCE",
		"LAST-SCALE",
		"COND-EVIDENCE",
		"ABLE-TO-SCALE",
		"SCALING-ACTIVE",
		"SCALING-LIMITED",
		"READY",
		"ACTIVE",
		"FALLBACK",
		"PAUSED",
	} {
		row := []string{field, "-"}
		hasValue := false
		for _, componentType := range componentTypes {
			value := compactAutoscaleCell(valuesByComponent[componentType][field])
			row = append(row, value)
			if value != "-" {
				hasValue = true
			}
		}
		if hasValue {
			table.Rows = append(table.Rows, row)
		}
	}
	if unknownComponents > 0 {
		table.Rows = append(table.Rows, []string{
			"OTHER",
			printers.BoundedCell(strconv.Itoa(unknownComponents)+" omitted", compactAutoscaleCellWidth),
			"-", "-", "-",
		})
	}
	if len(canonical.Issues) > 0 {
		scopes := append([]RuntimeComponentType{""}, componentTypes...)
		aliasesByScope := make([][]string, len(scopes))
		issueRows := 0
		for index, scope := range scopes {
			aliasesByScope[index] = compactAutoscaleIssueAliases(scope, canonical.Issues)
			issueRows = max(issueRows, len(aliasesByScope[index]))
		}
		// Keep one alias per scope and row. Joining aliases into one bounded
		// cell would hide every issue after the first truncation point.
		for issueIndex := range issueRows {
			row := []string{"ISSUES"}
			for _, aliases := range aliasesByScope {
				alias := "-"
				if issueIndex < len(aliases) {
					alias = aliases[issueIndex]
				}
				row = append(row, alias)
			}
			table.Rows = append(table.Rows, row)
		}
	}
	return table
}

const compactAutoscaleCellWidth = 13

func compactAutoscaleComponentValues(component AutoscaleComponentStatus) map[string]string {
	values := map[string]string{
		"STATE":            string(component.State),
		"CLASS":            string(component.Class),
		"MANAGED-BY":       string(component.ManagedBy),
		"SPEC-SOURCE":      string(component.SpecSource),
		"TARGET-KIND":      compactAutoscaleTargetKind(component.Target),
		"TARGET-NAME":      compactAutoscaleTargetName(component.Target),
		"TARGET-EVIDENCE":  string(component.Target.State),
		"CURRENT":          autoscaleInt32Cell(component.Replicas.CurrentReplicas),
		"DESIRED":          autoscaleInt32Cell(component.Replicas.DesiredReplicas),
		"REPLICA-EVIDENCE": string(component.Replicas.State),
		"LAST-SCALE":       compactAutoscaleTimeCell(component.Replicas.LastScaleTime),
		"COND-EVIDENCE":    string(component.Conditions.State),
	}
	for _, condition := range component.Conditions.Items {
		field := compactAutoscaleConditionField(condition.Type)
		if field == "" {
			continue
		}
		if previous := values[field]; previous != "" {
			values[field] = previous + "/" + string(condition.Status)
			continue
		}
		values[field] = string(condition.Status)
	}
	return values
}

func compactAutoscaleTargetKind(target AutoscaleTarget) string {
	if target.State != AutoscaleTargetReported {
		return "-"
	}
	switch target.Kind {
	case AutoscaleTargetInferenceReplica:
		return "IR"
	case AutoscaleTargetDeployment:
		return "Deployment"
	default:
		return "Unknown"
	}
}

func compactAutoscaleTargetName(target AutoscaleTarget) string {
	if target.State != AutoscaleTargetReported {
		return "-"
	}
	return printers.BoundedMiddleCell(target.Name, compactAutoscaleCellWidth)
}

func compactAutoscaleTimeCell(value *time.Time) string {
	if value == nil {
		return "-"
	}
	return value.UTC().Format("Jan02 15:04Z")
}

func compactAutoscaleConditionField(condition AutoscaleConditionType) string {
	switch condition {
	case AutoscaleConditionAbleToScale:
		return "ABLE-TO-SCALE"
	case AutoscaleConditionScalingActive:
		return "SCALING-ACTIVE"
	case AutoscaleConditionScalingLimited:
		return "SCALING-LIMITED"
	case AutoscaleConditionReady:
		return "READY"
	case AutoscaleConditionActive:
		return "ACTIVE"
	case AutoscaleConditionFallback:
		return "FALLBACK"
	case AutoscaleConditionPaused:
		return "PAUSED"
	default:
		return ""
	}
}

func compactAutoscaleCell(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	clean := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			clean = append(clean, value)
		}
	}
	if len(clean) == 0 {
		return "-"
	}
	return printers.BoundedCell(strings.Join(clean, ";"), compactAutoscaleCellWidth)
}

func compactAutoscaleIssueAliases(component RuntimeComponentType, issues []AutoscaleIssue) []string {
	values := make([]string, 0, len(issues))
	for _, issue := range issues {
		if component == "" {
			if issue.Component == "" || autoscaleComponentRank(issue.Component) >= 3 {
				values = append(values, compactAutoscaleIssueAlias(issue.Code))
			}
			continue
		}
		if issue.Component == component {
			values = append(values, compactAutoscaleIssueAlias(issue.Code))
		}
	}
	return values
}

func compactAutoscaleIssueAlias(code AutoscaleIssueCode) string {
	switch code {
	case AutoscaleIssueUnknownComponentStatus:
		return "UnknownComp"
	case AutoscaleIssueAutoscalerNotReported:
		return "NoAutoscaler"
	case AutoscaleIssueScaleTargetNotReported:
		return "NoTarget"
	case AutoscaleIssueClassInvalid:
		return "BadClass"
	case AutoscaleIssueManagedByInvalid:
		return "BadManager"
	case AutoscaleIssueOwnershipMismatch:
		return "OwnerMismatch"
	case AutoscaleIssueSpecSourceInvalid:
		return "BadSpecSource"
	case AutoscaleIssueUnexpectedScalerEvidence:
		return "UnexpectedEv"
	case AutoscaleIssueReplicaEvidenceAmbiguous:
		return "ReplicaAmbig"
	case AutoscaleIssueReplicaEvidenceInvalid:
		return "BadReplica"
	case AutoscaleIssueScaleTargetInvalid:
		return "BadTarget"
	case AutoscaleIssueConditionInvalid:
		return "BadCondition"
	case AutoscaleIssueConditionConflict:
		return "CondConflict"
	default:
		// Future values are outside the closed issue vocabulary. Avoid
		// echoing an untrusted value while retaining a stable 40-bit identity.
		digest := sha256.Sum256([]byte(code))
		return "X#" + hex.EncodeToString(digest[:5])
	}
}

// WideTable returns the deterministic complete autoscaling view that preceded
// the compact component matrix.
func (c AutoscaleStatusContent) WideTable() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{
		"STATE", "COMPONENT", "COMPONENT-STATE", "CLASS", "MANAGED-BY", "SPEC-SOURCE",
		"TARGET", "TARGET-EVIDENCE", "CURRENT", "DESIRED", "REPLICA-EVIDENCE", "LAST-SCALE", "CONDITION-EVIDENCE", "CONDITIONS", "ISSUES",
	}, Rows: [][]string{}}
	if len(canonical.Components) == 0 {
		table.Rows = append(table.Rows, []string{
			string(canonical.Summary.State), "-", "-", "-", "-", "-", "-",
			"-", "-", "-", "-", "-", "-", "-", autoscaleIssuesCell("", canonical.Issues),
		})
		return table
	}
	for _, component := range canonical.Components {
		table.Rows = append(table.Rows, []string{
			string(canonical.Summary.State), string(component.Type), string(component.State),
			string(component.Class), string(component.ManagedBy), string(component.SpecSource),
			autoscaleTargetCell(component.Target), string(component.Target.State), autoscaleInt32Cell(component.Replicas.CurrentReplicas),
			autoscaleInt32Cell(component.Replicas.DesiredReplicas), string(component.Replicas.State),
			autoscaleTimeCell(component.Replicas.LastScaleTime), string(component.Conditions.State),
			autoscaleConditionsCell(component.Conditions.Items),
			autoscaleIssuesCell(component.Type, canonical.Issues),
		})
	}
	return table
}

func canonicalAutoscaleComponent(component AutoscaleComponentStatus) AutoscaleComponentStatus {
	result := component
	result.Replicas.CurrentReplicas = copyInt32(component.Replicas.CurrentReplicas)
	result.Replicas.DesiredReplicas = copyInt32(component.Replicas.DesiredReplicas)
	if component.Replicas.LastScaleTime != nil {
		value := component.Replicas.LastScaleTime.UTC()
		result.Replicas.LastScaleTime = &value
	}
	if component.Replicas.State != AutoscaleReplicasReported && component.Replicas.State != AutoscaleReplicasAmbiguous {
		result.Replicas.CurrentReplicas = nil
		result.Replicas.DesiredReplicas = nil
		result.Replicas.LastScaleTime = nil
	}
	result.Conditions = component.Conditions
	result.Conditions.Items = append([]AutoscaleCondition{}, component.Conditions.Items...)
	for i := range result.Conditions.Items {
		result.Conditions.Items[i].LastTransitionTime = result.Conditions.Items[i].LastTransitionTime.UTC()
	}
	sort.Slice(result.Conditions.Items, func(i, j int) bool {
		if autoscaleConditionRank(result.Conditions.Items[i].Type) != autoscaleConditionRank(result.Conditions.Items[j].Type) {
			return autoscaleConditionRank(result.Conditions.Items[i].Type) < autoscaleConditionRank(result.Conditions.Items[j].Type)
		}
		if result.Conditions.Items[i].Type != result.Conditions.Items[j].Type {
			return result.Conditions.Items[i].Type < result.Conditions.Items[j].Type
		}
		if result.Conditions.Items[i].Status != result.Conditions.Items[j].Status {
			return result.Conditions.Items[i].Status < result.Conditions.Items[j].Status
		}
		return result.Conditions.Items[i].LastTransitionTime.Before(result.Conditions.Items[j].LastTransitionTime)
	})
	result.Conditions.Items = dedupeAutoscaleConditions(result.Conditions.Items)
	if component.Conditions.State != AutoscaleConditionsReported && component.Conditions.State != AutoscaleConditionsUnavailable {
		result.Conditions.Items = []AutoscaleCondition{}
	}
	return result
}

func autoscaleSourceLess(a, b AutoscaleSourceReference) bool {
	for _, result := range []int{
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Name, b.Name),
		cmp.Compare(a.UID, b.UID),
		cmp.Compare(a.Generation, b.Generation),
		cmp.Compare(a.Evidence, b.Evidence),
		a.CollectedAt.Compare(b.CollectedAt),
	} {
		if result != 0 {
			return result < 0
		}
	}
	return false
}

func compareAutoscaleComponents(a, b AutoscaleComponentStatus) int {
	for _, result := range []int{
		cmp.Compare(autoscaleComponentRank(a.Type), autoscaleComponentRank(b.Type)),
		cmp.Compare(a.Type, b.Type),
		cmp.Compare(a.State, b.State),
		cmp.Compare(a.Class, b.Class),
		cmp.Compare(a.ManagedBy, b.ManagedBy),
		cmp.Compare(a.SpecSource, b.SpecSource),
		compareAutoscaleTargets(a.Target, b.Target),
		compareAutoscaleReplicas(a.Replicas, b.Replicas),
		compareAutoscaleConditions(a.Conditions, b.Conditions),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareAutoscaleTargets(a, b AutoscaleTarget) int {
	for _, result := range []int{
		cmp.Compare(a.State, b.State), cmp.Compare(a.APIVersion, b.APIVersion),
		cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareAutoscaleReplicas(a, b AutoscaleReplicaStatus) int {
	for _, result := range []int{
		cmp.Compare(a.State, b.State), compareAutoscaleInt32Pointers(a.CurrentReplicas, b.CurrentReplicas),
		compareAutoscaleInt32Pointers(a.DesiredReplicas, b.DesiredReplicas),
		compareAutoscaleTimePointers(a.LastScaleTime, b.LastScaleTime),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareAutoscaleConditions(a, b AutoscaleConditionsStatus) int {
	if result := cmp.Compare(a.State, b.State); result != 0 {
		return result
	}
	return slices.CompareFunc(a.Items, b.Items, compareAutoscaleCondition)
}

func compareAutoscaleCondition(a, b AutoscaleCondition) int {
	for _, result := range []int{
		cmp.Compare(autoscaleConditionRank(a.Type), autoscaleConditionRank(b.Type)),
		cmp.Compare(a.Type, b.Type), cmp.Compare(a.Status, b.Status),
		a.LastTransitionTime.Compare(b.LastTransitionTime),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareAutoscaleInt32Pointers(a, b *int32) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	default:
		return cmp.Compare(*a, *b)
	}
}

func compareAutoscaleTimePointers(a, b *time.Time) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	default:
		return a.Compare(*b)
	}
}

func autoscaleConditionRank(condition AutoscaleConditionType) int {
	switch condition {
	case AutoscaleConditionAbleToScale:
		return 0
	case AutoscaleConditionScalingActive:
		return 1
	case AutoscaleConditionScalingLimited:
		return 2
	case AutoscaleConditionReady:
		return 3
	case AutoscaleConditionActive:
		return 4
	case AutoscaleConditionFallback:
		return 5
	case AutoscaleConditionPaused:
		return 6
	default:
		return 7
	}
}

func autoscaleComponentRank(component RuntimeComponentType) int {
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

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func dedupeAutoscaleConditions(values []AutoscaleCondition) []AutoscaleCondition {
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func dedupeAutoscaleIssues(values []AutoscaleIssue) []AutoscaleIssue {
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func dedupeAutoscaleWarnings(values []AutoscaleWarning) []AutoscaleWarning {
	result := values[:0]
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func autoscaleTargetCell(target AutoscaleTarget) string {
	if target.State != AutoscaleTargetReported {
		return "-"
	}
	return string(target.Kind) + "/" + target.Namespace + "/" + target.Name
}

func autoscaleInt32Cell(value *int32) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatInt(int64(*value), 10)
}

func autoscaleTimeCell(value *time.Time) string {
	if value == nil {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func autoscaleConditionsCell(conditions []AutoscaleCondition) string {
	if len(conditions) == 0 {
		return "-"
	}
	values := make([]string, len(conditions))
	for i, condition := range conditions {
		values[i] = string(condition.Type) + "=" + string(condition.Status)
	}
	return strings.Join(values, ",")
}

func autoscaleIssuesCell(component RuntimeComponentType, issues []AutoscaleIssue) string {
	values := make([]string, 0, len(issues))
	for _, issue := range issues {
		if issue.Component == "" || issue.Component == component {
			values = append(values, string(issue.Code))
		}
	}
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ",")
}
