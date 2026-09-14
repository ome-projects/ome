package v1alpha1

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// InstanceListReportKind identifies the instance-list report schema.
const InstanceListReportKind = "InstanceListReport"

type InstanceListState string

const (
	InstanceListStateReported    InstanceListState = "Reported"
	InstanceListStatePartial     InstanceListState = "Partial"
	InstanceListStateUnavailable InstanceListState = "Unavailable"
)

type InstanceEvidenceState string

const (
	InstanceEvidenceReported    InstanceEvidenceState = "Reported"
	InstanceEvidenceStale       InstanceEvidenceState = "Stale"
	InstanceEvidenceMalformed   InstanceEvidenceState = "Malformed"
	InstanceEvidenceUnavailable InstanceEvidenceState = "Unavailable"
)

type InstanceIndexSetState string

const (
	InstanceIndexSetDense       InstanceIndexSetState = "Dense"
	InstanceIndexSetSparse      InstanceIndexSetState = "Sparse"
	InstanceIndexSetEmpty       InstanceIndexSetState = "Empty"
	InstanceIndexSetMalformed   InstanceIndexSetState = "Malformed"
	InstanceIndexSetNotReported InstanceIndexSetState = "NotReported"
)

type InstancePhase string

const (
	InstancePhasePending    InstancePhase = "Pending"
	InstancePhaseCreating   InstancePhase = "Creating"
	InstancePhaseReady      InstancePhase = "Ready"
	InstancePhaseUpdating   InstancePhase = "Updating"
	InstancePhaseRestarting InstancePhase = "Restarting"
	InstancePhaseMigrating  InstancePhase = "Migrating"
	InstancePhaseFailed     InstancePhase = "Failed"
	InstancePhaseDeleting   InstancePhase = "Deleting"
)

type InstanceIssueCode string

const (
	InstanceIssueCollectionUnavailable     InstanceIssueCode = "CollectionUnavailable"
	InstanceIssueCollectionTruncated       InstanceIssueCode = "CollectionTruncated"
	InstanceIssueIdentityRejected          InstanceIssueCode = "IdentityRejected"
	InstanceIssueDuplicateComponent        InstanceIssueCode = "DuplicateComponent"
	InstanceIssueParentGenerationMissing   InstanceIssueCode = "ParentGenerationMissing"
	InstanceIssueParentGenerationInvalid   InstanceIssueCode = "ParentGenerationInvalid"
	InstanceIssueParentGenerationStale     InstanceIssueCode = "ParentGenerationStale"
	InstanceIssueParentGenerationAhead     InstanceIssueCode = "ParentGenerationAhead"
	InstanceIssueStatusUnobserved          InstanceIssueCode = "StatusUnobserved"
	InstanceIssueObservedGenerationInvalid InstanceIssueCode = "ObservedGenerationInvalid"
	InstanceIssueStaleGeneration           InstanceIssueCode = "StaleGeneration"
	InstanceIssueRevisionInvalid           InstanceIssueCode = "RevisionInvalid"
	InstanceIssueAggregateInvalid          InstanceIssueCode = "AggregateInvalid"
	InstanceIssueStatusesNotReported       InstanceIssueCode = "StatusesNotReported"
	InstanceIssueStatusRowsTruncated       InstanceIssueCode = "StatusRowsTruncated"
	InstanceIssueOutputTruncated           InstanceIssueCode = "OutputTruncated"
	InstanceIssueSparseIndices             InstanceIssueCode = "SparseIndices"
	InstanceIssueDuplicateIndex            InstanceIssueCode = "DuplicateIndex"
	InstanceIssueIndexInvalid              InstanceIssueCode = "IndexInvalid"
	InstanceIssuePhaseInvalid              InstanceIssueCode = "PhaseInvalid"
	InstanceIssueInstanceRevisionInvalid   InstanceIssueCode = "InstanceRevisionInvalid"
	InstanceIssuePodCountsInvalid          InstanceIssueCode = "PodCountsInvalid"
	InstanceIssueIncarnationInvalid        InstanceIssueCode = "IncarnationInvalid"
)

type InstanceIdentityRejectionReason string

const (
	InstanceIdentityRejectionMetadata        InstanceIdentityRejectionReason = "Metadata"
	InstanceIdentityRejectionLabel           InstanceIdentityRejectionReason = "Label"
	InstanceIdentityRejectionParentReference InstanceIdentityRejectionReason = "ParentReference"
	InstanceIdentityRejectionOwnerReference  InstanceIdentityRejectionReason = "OwnerReference"
	InstanceIdentityRejectionComponent       InstanceIdentityRejectionReason = "Component"
)

type InstanceListWarning struct {
	Code WarningCode `json:"code"`
}

type InstanceListSummary struct {
	State      InstanceListState `json:"state"`
	Components int               `json:"components"`
	Instances  int               `json:"instances"`
	Truncated  bool              `json:"truncated"`
}

type InstanceListComponent struct {
	Type                 RuntimeComponentType  `json:"type"`
	State                InstanceEvidenceState `json:"state"`
	InferenceReplica     string                `json:"inferenceReplica"`
	Generation           int64                 `json:"generation"`
	ObservedGeneration   int64                 `json:"observedGeneration"`
	IndexSet             InstanceIndexSetState `json:"indexSet"`
	CurrentRevision      string                `json:"currentRevision,omitempty"`
	UpdateRevision       string                `json:"updateRevision,omitempty"`
	Replicas             int32                 `json:"replicas"`
	ReadyReplicas        int32                 `json:"readyReplicas"`
	ServingReplicas      int32                 `json:"servingReplicas"`
	AvailableReplicas    int32                 `json:"availableReplicas"`
	UpdatedReplicas      int32                 `json:"updatedReplicas"`
	UpdatedReadyReplicas int32                 `json:"updatedReadyReplicas"`
}

type InstancePodCounts struct {
	Total     int32 `json:"total"`
	Serving   int32 `json:"serving"`
	Available int32 `json:"available"`
}

type InstanceListInstance struct {
	Component          RuntimeComponentType  `json:"component"`
	InferenceReplica   string                `json:"inferenceReplica"`
	Index              int32                 `json:"index"`
	Incarnation        int64                 `json:"incarnation"`
	Phase              InstancePhase         `json:"phase"`
	RunningRevision    string                `json:"runningRevision,omitempty"`
	TargetRevision     string                `json:"targetRevision,omitempty"`
	Pods               InstancePodCounts     `json:"pods"`
	Admitted           bool                  `json:"admitted"`
	OperationPresent   bool                  `json:"operationPresent"`
	LastFailurePresent bool                  `json:"lastFailurePresent"`
	Evidence           InstanceEvidenceState `json:"evidence"`
}

type InstanceListIssue struct {
	Code              InstanceIssueCode               `json:"code"`
	Component         RuntimeComponentType            `json:"component,omitempty"`
	InferenceReplica  string                          `json:"inferenceReplica,omitempty"`
	Index             *int32                          `json:"index,omitempty"`
	RejectionReason   InstanceIdentityRejectionReason `json:"rejectionReason,omitempty"`
	UnavailableReason UnavailableReason               `json:"unavailableReason,omitempty"`
}

type InstanceListContent struct {
	Summary    InstanceListSummary     `json:"summary"`
	Components []InstanceListComponent `json:"components"`
	Instances  []InstanceListInstance  `json:"instances"`
	Issues     []InstanceListIssue     `json:"issues"`
}

type InstanceListReport struct {
	APIVersion  string                `json:"apiVersion"`
	Kind        string                `json:"kind"`
	Metadata    Metadata              `json:"metadata"`
	CollectedAt time.Time             `json:"collectedAt"`
	Sources     []SourceReference     `json:"sources"`
	Content     InstanceListContent   `json:"content"`
	Warnings    []InstanceListWarning `json:"warnings"`
}

func NewInstanceListReport(metadata Metadata, content InstanceListContent, clock Clock) InstanceListReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (InstanceListReport{
		APIVersion: APIVersion, Kind: InstanceListReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []SourceReference{},
		Content: content, Warnings: []InstanceListWarning{},
	}).Canonical()
}

func (r InstanceListReport) Canonical() InstanceListReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = InstanceListReportKind
	result.CollectedAt = r.CollectedAt.UTC()
	result.Sources = append([]SourceReference{}, r.Sources...)
	for i := range result.Sources {
		if result.Sources[i].CollectedAt.IsZero() {
			result.Sources[i].CollectedAt = result.CollectedAt
		} else {
			result.Sources[i].CollectedAt = result.Sources[i].CollectedAt.UTC()
		}
	}
	sort.SliceStable(result.Sources, func(i, j int) bool { return sourceLess(result.Sources[i], result.Sources[j]) })
	result.Content = r.Content.Canonical()
	result.Warnings = append([]InstanceListWarning{}, r.Warnings...)
	sort.Slice(result.Warnings, func(i, j int) bool { return result.Warnings[i].Code < result.Warnings[j].Code })
	result.Warnings = slices.Compact(result.Warnings)
	return result
}

func (r InstanceListReport) Table() report.Table { return r.Content.Table() }

func (c InstanceListContent) Canonical() InstanceListContent {
	result := c
	result.Components = append([]InstanceListComponent{}, c.Components...)
	sort.Slice(result.Components, func(i, j int) bool {
		return compareInstanceComponents(result.Components[i], result.Components[j]) < 0
	})
	result.Instances = append([]InstanceListInstance{}, c.Instances...)
	sort.Slice(result.Instances, func(i, j int) bool {
		return compareInstances(result.Instances[i], result.Instances[j]) < 0
	})
	result.Issues = make([]InstanceListIssue, len(c.Issues))
	for i := range c.Issues {
		result.Issues[i] = c.Issues[i]
		if c.Issues[i].Index != nil {
			value := *c.Issues[i].Index
			result.Issues[i].Index = &value
		}
	}
	sort.Slice(result.Issues, func(i, j int) bool { return compareInstanceIssues(result.Issues[i], result.Issues[j]) < 0 })
	return result
}

func (c InstanceListContent) Table() report.Table {
	canonical := c.Canonical()
	table := report.Table{Headers: []string{"COMP", "IDX/INC", "PHASE", "PODS", "REVS", "AOF", "EVIDENCE"}, Rows: [][]string{}}
	instances := make(map[string][]InstanceListInstance, len(canonical.Components))
	components := make(map[string]struct{}, len(canonical.Components))
	for _, instance := range canonical.Instances {
		key := instanceComponentKey(instance.Component, instance.InferenceReplica)
		instances[key] = append(instances[key], instance)
	}
	for _, component := range canonical.Components {
		key := instanceComponentKey(component.Type, component.InferenceReplica)
		components[key] = struct{}{}
		componentInstances := instances[key]
		if len(componentInstances) == 0 {
			table.Rows = append(table.Rows, []string{
				printers.BoundedCell(string(component.Type), 7), "-", "-", "-", "-", "---",
				printers.BoundedCell(componentEvidenceCell(component, canonical.Issues), 12),
			})
			continue
		}
		for _, instance := range componentInstances {
			table.Rows = append(table.Rows, instanceTableRow(instance, component, canonical.Issues))
		}
	}
	for _, instance := range canonical.Instances {
		key := instanceComponentKey(instance.Component, instance.InferenceReplica)
		if _, found := components[key]; found {
			continue
		}
		table.Rows = append(table.Rows, instanceTableRow(instance, InstanceListComponent{}, canonical.Issues))
	}
	if len(table.Rows) == 0 {
		evidence := summaryEvidenceCell(canonical.Summary)
		if global, present := globalEvidenceCell(canonical.Summary, canonical.Issues); present {
			evidence = global
		}
		table.Rows = append(table.Rows, []string{"-", "-", "-", "-", "-", "---", printers.BoundedCell(evidence, 12)})
	} else if evidence, present := globalEvidenceCell(canonical.Summary, canonical.Issues); present {
		table.Rows = append(table.Rows, []string{"summary", "-", "-", "-", "-", "---", evidence})
	}
	return table
}

func instanceTableRow(
	instance InstanceListInstance,
	component InstanceListComponent,
	issues []InstanceListIssue,
) []string {
	return []string{
		printers.BoundedCell(string(instance.Component), 7),
		printers.BoundedCell(fmt.Sprintf("%d/%d", instance.Index, instance.Incarnation), 10),
		printers.BoundedCell(string(instance.Phase), 10),
		printers.BoundedCell(fmt.Sprintf("%d/%d/%d", instance.Pods.Serving, instance.Pods.Available, instance.Pods.Total), 9),
		printers.BoundedMiddleCell(instanceRevisionCell(instance), 11),
		instancePresenceCell(instance),
		printers.BoundedCell(instanceRowEvidenceCell(instance, component, issues), 12),
	}
}

func instanceRowEvidenceCell(
	instance InstanceListInstance,
	component InstanceListComponent,
	issues []InstanceListIssue,
) string {
	component.Type = instance.Component
	component.InferenceReplica = instance.InferenceReplica
	component.State = instance.Evidence
	return componentEvidenceCell(component, issues)
}

func instanceRevisionCell(instance InstanceListInstance) string {
	if instance.RunningRevision == "" && instance.TargetRevision == "" {
		return "-"
	}
	if instance.TargetRevision == "" || instance.TargetRevision == instance.RunningRevision {
		return instance.RunningRevision
	}
	if instance.RunningRevision == "" {
		return ">" + instance.TargetRevision
	}
	return instance.RunningRevision + ">" + instance.TargetRevision
}

func instancePresenceCell(instance InstanceListInstance) string {
	result := []byte{'-', '-', '-'}
	if instance.Admitted {
		result[0] = 'A'
	}
	if instance.OperationPresent {
		result[1] = 'O'
	}
	if instance.LastFailurePresent {
		result[2] = 'F'
	}
	return string(result)
}

func instanceEvidenceCell(state InstanceEvidenceState, component InstanceListComponent) string {
	switch state {
	case InstanceEvidenceReported:
		if component.IndexSet == InstanceIndexSetEmpty {
			return "EMPTY"
		}
		if component.IndexSet == InstanceIndexSetSparse {
			return "SPARSE"
		}
		return "OK"
	case InstanceEvidenceStale:
		return fmt.Sprintf("STALE:%d/%d", component.ObservedGeneration, component.Generation)
	case InstanceEvidenceMalformed:
		return "BAD"
	default:
		return "UNAVAILABLE"
	}
}

func componentEvidenceCell(component InstanceListComponent, issues []InstanceListIssue) string {
	bestRank := int(^uint(0) >> 1)
	bestCell := ""
	for _, issue := range issues {
		if issue.Component != component.Type || issue.InferenceReplica != component.InferenceReplica {
			continue
		}
		rank, cell, present := componentIssueEvidence(issue.Code)
		if present && rank < bestRank {
			bestRank, bestCell = rank, cell
		}
	}
	if bestCell != "" {
		return bestCell
	}
	return instanceEvidenceCell(component.State, component)
}

func componentIssueEvidence(code InstanceIssueCode) (int, string, bool) {
	switch code {
	case InstanceIssueDuplicateComponent:
		return 0, "BAD:DUP-COMP", true
	case InstanceIssueDuplicateIndex:
		return 1, "BAD:DUP-IDX", true
	case InstanceIssueObservedGenerationInvalid:
		return 2, "BAD:GEN", true
	case InstanceIssueParentGenerationInvalid, InstanceIssueParentGenerationAhead:
		return 3, "BAD:PARENT", true
	case InstanceIssueRevisionInvalid, InstanceIssueInstanceRevisionInvalid:
		return 4, "BAD:REVISION", true
	case InstanceIssuePodCountsInvalid, InstanceIssueAggregateInvalid:
		return 5, "BAD:COUNTS", true
	case InstanceIssuePhaseInvalid:
		return 6, "BAD:PHASE", true
	case InstanceIssueIndexInvalid, InstanceIssueIncarnationInvalid:
		return 7, "BAD:INSTANCE", true
	case InstanceIssueStatusRowsTruncated:
		return 8, "ROWS-LIMIT", true
	case InstanceIssueStatusesNotReported:
		return 9, "NOT-REPORTED", true
	case InstanceIssueStatusUnobserved:
		return 10, "NOT-OBSERVED", true
	case InstanceIssueParentGenerationMissing:
		return 11, "PARENT-UNAVL", true
	case InstanceIssueParentGenerationStale:
		return 12, "STALE:PARENT", true
	default:
		return 0, "", false
	}
}

func globalEvidenceCell(summary InstanceListSummary, issues []InstanceListIssue) (string, bool) {
	if summary.Truncated {
		return "TRUNCATED", true
	}
	for _, issue := range issues {
		switch issue.Code {
		case InstanceIssueCollectionUnavailable:
			return "SOURCE-UNAVL", true
		case InstanceIssueIdentityRejected:
			return "REJECTED", true
		case InstanceIssueCollectionTruncated, InstanceIssueOutputTruncated:
			return "TRUNCATED", true
		}
	}
	return "", false
}

func summaryEvidenceCell(summary InstanceListSummary) string {
	switch summary.State {
	case InstanceListStateReported:
		return "OK"
	case InstanceListStatePartial:
		return "PARTIAL"
	default:
		return "UNAVAILABLE"
	}
}

func instanceComponentKey(component RuntimeComponentType, ir string) string {
	return string(component) + "\x00" + ir
}

func instanceComponentRank(component RuntimeComponentType) int {
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

func compareInstanceComponents(a, b InstanceListComponent) int {
	for _, comparison := range []int{
		cmp.Compare(instanceComponentRank(a.Type), instanceComponentRank(b.Type)),
		cmp.Compare(a.Type, b.Type), cmp.Compare(a.InferenceReplica, b.InferenceReplica),
		cmp.Compare(a.Generation, b.Generation), cmp.Compare(a.ObservedGeneration, b.ObservedGeneration),
		cmp.Compare(a.State, b.State), cmp.Compare(a.IndexSet, b.IndexSet),
		cmp.Compare(a.CurrentRevision, b.CurrentRevision), cmp.Compare(a.UpdateRevision, b.UpdateRevision),
		cmp.Compare(a.Replicas, b.Replicas), cmp.Compare(a.ReadyReplicas, b.ReadyReplicas),
		cmp.Compare(a.ServingReplicas, b.ServingReplicas), cmp.Compare(a.AvailableReplicas, b.AvailableReplicas),
		cmp.Compare(a.UpdatedReplicas, b.UpdatedReplicas), cmp.Compare(a.UpdatedReadyReplicas, b.UpdatedReadyReplicas),
	} {
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}

func compareInstances(a, b InstanceListInstance) int {
	for _, comparison := range []int{
		cmp.Compare(instanceComponentRank(a.Component), instanceComponentRank(b.Component)),
		cmp.Compare(a.Component, b.Component), cmp.Compare(a.Index, b.Index),
		cmp.Compare(a.InferenceReplica, b.InferenceReplica), cmp.Compare(a.Incarnation, b.Incarnation),
		cmp.Compare(a.Phase, b.Phase), cmp.Compare(a.RunningRevision, b.RunningRevision),
		cmp.Compare(a.TargetRevision, b.TargetRevision), cmp.Compare(a.Pods.Total, b.Pods.Total),
		cmp.Compare(a.Pods.Serving, b.Pods.Serving),
		cmp.Compare(a.Pods.Available, b.Pods.Available), cmp.Compare(boolOrder(a.Admitted), boolOrder(b.Admitted)),
		cmp.Compare(boolOrder(a.OperationPresent), boolOrder(b.OperationPresent)), cmp.Compare(boolOrder(a.LastFailurePresent), boolOrder(b.LastFailurePresent)),
		cmp.Compare(a.Evidence, b.Evidence),
	} {
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}

func boolOrder(value bool) int {
	if value {
		return 1
	}
	return 0
}

func compareInstanceIssues(a, b InstanceListIssue) int {
	for _, comparison := range []int{
		cmp.Compare(a.Code, b.Code), cmp.Compare(instanceComponentRank(a.Component), instanceComponentRank(b.Component)),
		cmp.Compare(a.Component, b.Component), cmp.Compare(a.InferenceReplica, b.InferenceReplica),
		compareOptionalInstanceIndex(a.Index, b.Index),
		cmp.Compare(a.RejectionReason, b.RejectionReason),
		cmp.Compare(a.UnavailableReason, b.UnavailableReason),
	} {
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}

func compareOptionalInstanceIndex(a, b *int32) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return cmp.Compare(*a, *b)
}
