// Package instanceprojection turns current InferenceReplica instance status
// into the typed, read-only CLI report contract.
package instanceprojection

import (
	"container/heap"
	"errors"
	"sort"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/instancecollection"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	ErrInferenceServiceRequired        = errors.New("instance projection requires an InferenceService")
	ErrInferenceServiceIdentityInvalid = errors.New("instance projection requires a safe InferenceService identity")
	ErrMaxInstancesInvalid             = errors.New("instance projection requires a positive instance limit")
	ErrCollectionEvidenceInvalid       = errors.New("instance projection collection evidence is invalid")
)

const (
	invalidIdentityName = "INVALID"
	maxUIDLength        = 128
)

type Input struct {
	InferenceService      *omev1beta1.InferenceService
	Collection            instancecollection.Result
	CollectionUnavailable reportv1alpha1.UnavailableReason
	MaxInstances          int
}

func Project(input Input, clock reportv1alpha1.Clock) (reportv1alpha1.InstanceListReport, error) {
	if input.InferenceService == nil {
		return reportv1alpha1.InstanceListReport{}, ErrInferenceServiceRequired
	}
	if !validSourceIdentity(
		input.InferenceService.Namespace, input.InferenceService.Name, input.InferenceService.UID,
	) || input.InferenceService.Generation <= 0 {
		return reportv1alpha1.InstanceListReport{}, ErrInferenceServiceIdentityInvalid
	}
	if input.MaxInstances <= 0 {
		return reportv1alpha1.InstanceListReport{}, ErrMaxInstancesInvalid
	}
	if input.CollectionUnavailable != "" && !validUnavailableReason(input.CollectionUnavailable) {
		return reportv1alpha1.InstanceListReport{}, ErrCollectionEvidenceInvalid
	}
	isvc := input.InferenceService
	content := reportv1alpha1.InstanceListContent{
		Summary:    reportv1alpha1.InstanceListSummary{State: reportv1alpha1.InstanceListStateReported},
		Components: []reportv1alpha1.InstanceListComponent{},
		Instances:  []reportv1alpha1.InstanceListInstance{},
		Issues:     []reportv1alpha1.InstanceListIssue{},
	}
	reportValue := reportv1alpha1.NewInstanceListReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name}, content, clock,
	)
	reportValue.Sources = append(reportValue.Sources, reportv1alpha1.SourceReference{
		Kind: "InferenceService", Namespace: isvc.Namespace, Name: isvc.Name,
		UID: string(isvc.UID), Generation: isvc.Generation,
		Evidence: reportv1alpha1.EvidenceReported,
	})
	hasStale, hasMalformed, hasUnavailable := false, false, false
	rowIssueBudget := input.MaxInstances
	outputTruncated := false
	if input.CollectionUnavailable != "" {
		hasUnavailable = true
		reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
		reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
			Code:              reportv1alpha1.InstanceIssueCollectionUnavailable,
			UnavailableReason: input.CollectionUnavailable,
		})
		reportValue.Sources = append(reportValue.Sources, reportv1alpha1.SourceReference{
			Kind: "InferenceReplicaList", Namespace: isvc.Namespace, Name: isvc.Name,
			Evidence: reportv1alpha1.EvidenceUnavailable, UnavailableReason: input.CollectionUnavailable,
		})
	}
	if input.Collection.Truncated {
		reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
		reportValue.Content.Summary.Truncated = true
		reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
			Code: reportv1alpha1.InstanceIssueCollectionTruncated,
		})
	}
	for _, rejection := range input.Collection.Rejected {
		reportReason, ok := identityRejectionReason(rejection.Reason)
		if !ok {
			return reportv1alpha1.InstanceListReport{}, ErrCollectionEvidenceInvalid
		}
		hasMalformed = true
		reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
		reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
			Code: reportv1alpha1.InstanceIssueIdentityRejected, InferenceReplica: safeName(rejection.Name),
			RejectionReason: reportReason,
		})
	}
	componentCounts := make(map[omev1beta1.ComponentType]int, len(input.Collection.Items))
	itemIdentities := make(map[string]int, len(input.Collection.Items))
	for i := range input.Collection.Items {
		if !validCollectedReplicaIdentity(&input.Collection.Items[i], isvc) {
			return reportv1alpha1.InstanceListReport{}, ErrCollectionEvidenceInvalid
		}
		componentCounts[input.Collection.Items[i].Spec.Component]++
		itemIdentities[replicaIdentityKey(
			input.Collection.Items[i].Name, input.Collection.Items[i].Spec.Component,
		)]++
	}
	statusRowsTruncated := make(map[string]struct{}, len(input.Collection.StatusRowsTruncated))
	for _, truncation := range input.Collection.StatusRowsTruncated {
		key := replicaIdentityKey(truncation.Name, truncation.Component)
		if len(validation.IsDNS1123Subdomain(truncation.Name)) != 0 ||
			!validComponent(truncation.Component) || itemIdentities[key] != 1 {
			return reportv1alpha1.InstanceListReport{}, ErrCollectionEvidenceInvalid
		}
		if _, duplicate := statusRowsTruncated[key]; duplicate {
			return reportv1alpha1.InstanceListReport{}, ErrCollectionEvidenceInvalid
		}
		statusRowsTruncated[key] = struct{}{}
	}
	items := append([]omev1beta1.InferenceReplica{}, input.Collection.Items...)
	sort.Slice(items, func(i, j int) bool {
		left, right := &items[i], &items[j]
		if componentRank(left.Spec.Component) != componentRank(right.Spec.Component) {
			return componentRank(left.Spec.Component) < componentRank(right.Spec.Component)
		}
		if left.Spec.Component != right.Spec.Component {
			return left.Spec.Component < right.Spec.Component
		}
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		if left.UID != right.UID {
			return left.UID < right.UID
		}
		return left.ResourceVersion < right.ResourceVersion
	})
	remainingStatusRows := input.MaxInstances
	for i := range items {
		ir := &items[i]
		identityKey := replicaIdentityKey(ir.Name, ir.Spec.Component)
		_, collectionTruncatedRows := statusRowsTruncated[identityKey]
		if collectionTruncatedRows && len(ir.Status.InstanceStatuses) != 0 {
			return reportv1alpha1.InstanceListReport{}, ErrCollectionEvidenceInvalid
		}
		currentRevision, updateRevision := ir.Status.CurrentRevision, ir.Status.UpdateRevision
		state := reportv1alpha1.InstanceEvidenceReported
		indexSet := reportv1alpha1.InstanceIndexSetEmpty
		skipRows := false
		switch parentGenerationEvidence(ir, isvc.Generation) {
		case parentGenerationMissing:
			state = reportv1alpha1.InstanceEvidenceUnavailable
			indexSet = reportv1alpha1.InstanceIndexSetNotReported
			skipRows = true
			hasUnavailable = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueParentGenerationMissing,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		case parentGenerationInvalid:
			state = reportv1alpha1.InstanceEvidenceMalformed
			indexSet = reportv1alpha1.InstanceIndexSetMalformed
			skipRows = true
			hasMalformed = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueParentGenerationInvalid,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		case parentGenerationStale:
			state = reportv1alpha1.InstanceEvidenceStale
			hasStale = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueParentGenerationStale,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		case parentGenerationAhead:
			state = reportv1alpha1.InstanceEvidenceMalformed
			indexSet = reportv1alpha1.InstanceIndexSetMalformed
			skipRows = true
			hasMalformed = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueParentGenerationAhead,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		}
		switch {
		case ir.Status.ObservedGeneration == 0 && ir.Generation > 0:
			if state != reportv1alpha1.InstanceEvidenceMalformed {
				state = reportv1alpha1.InstanceEvidenceUnavailable
			}
			indexSet = reportv1alpha1.InstanceIndexSetNotReported
			skipRows = true
			hasUnavailable = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueStatusUnobserved,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		case ir.Generation <= 0 || ir.Status.ObservedGeneration < 0 || ir.Status.ObservedGeneration > ir.Generation:
			state = reportv1alpha1.InstanceEvidenceMalformed
			indexSet = reportv1alpha1.InstanceIndexSetMalformed
			skipRows = true
			hasMalformed = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueObservedGenerationInvalid,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		case ir.Status.ObservedGeneration < ir.Generation:
			if state == reportv1alpha1.InstanceEvidenceReported {
				state = reportv1alpha1.InstanceEvidenceStale
			}
			hasStale = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueStaleGeneration,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		}
		if !validAggregateStatus(ir.Status) {
			state = reportv1alpha1.InstanceEvidenceMalformed
			indexSet = reportv1alpha1.InstanceIndexSetMalformed
			skipRows = true
			hasMalformed = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueAggregateInvalid,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		}
		if !validOptionalRevision(ir.Status.CurrentRevision) || !validOptionalRevision(ir.Status.UpdateRevision) {
			if !validOptionalRevision(currentRevision) {
				currentRevision = ""
			}
			if !validOptionalRevision(updateRevision) {
				updateRevision = ""
			}
			state = reportv1alpha1.InstanceEvidenceMalformed
			indexSet = reportv1alpha1.InstanceIndexSetMalformed
			skipRows = true
			hasMalformed = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueRevisionInvalid,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		}
		if !collectionTruncatedRows && len(ir.Status.InstanceStatuses) == 0 && ir.Status.Replicas > 0 {
			if state == reportv1alpha1.InstanceEvidenceReported {
				state = reportv1alpha1.InstanceEvidenceUnavailable
			}
			indexSet = reportv1alpha1.InstanceIndexSetNotReported
			skipRows = true
			hasUnavailable = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueStatusesNotReported,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		}
		duplicateComponent := componentCounts[ir.Spec.Component] > 1
		if duplicateComponent {
			state = reportv1alpha1.InstanceEvidenceMalformed
			indexSet = reportv1alpha1.InstanceIndexSetMalformed
			skipRows = true
			hasMalformed = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueDuplicateComponent,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		}
		rowsOverBudget := collectionTruncatedRows ||
			(!skipRows && len(ir.Status.InstanceStatuses) > remainingStatusRows)
		if rowsOverBudget {
			if state != reportv1alpha1.InstanceEvidenceMalformed {
				state = reportv1alpha1.InstanceEvidenceUnavailable
			}
			indexSet = reportv1alpha1.InstanceIndexSetNotReported
			skipRows = true
			hasUnavailable = true
			outputTruncated = true
			reportValue.Content.Summary.Truncated = true
			reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
				Code:      reportv1alpha1.InstanceIssueStatusRowsTruncated,
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
			})
		}
		if !rowsOverBudget {
			remainingStatusRows -= len(ir.Status.InstanceStatuses)
			classifiedIndexSet := classifyIndexSet(ir.Status.InstanceStatuses)
			if !skipRows {
				indexSet = classifiedIndexSet
			}
			duplicateIndex, hasDuplicateIndex := firstDuplicateIndex(ir.Status.InstanceStatuses)
			if hasDuplicateIndex {
				state = reportv1alpha1.InstanceEvidenceMalformed
				indexSet = reportv1alpha1.InstanceIndexSetMalformed
				skipRows = true
				hasMalformed = true
				reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
				reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
					Code:      reportv1alpha1.InstanceIssueDuplicateIndex,
					Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name, Index: &duplicateIndex,
				})
			}
			rowIssues, invalidRows, issuesTruncated := validateInstanceRows(
				ir.Status.InstanceStatuses, componentType(ir.Spec.Component), ir.Name, rowIssueBudget,
			)
			rowIssueBudget -= len(rowIssues)
			if issuesTruncated {
				outputTruncated = true
				reportValue.Content.Summary.Truncated = true
				reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
			}
			if invalidRows {
				state = reportv1alpha1.InstanceEvidenceMalformed
				indexSet = reportv1alpha1.InstanceIndexSetMalformed
				skipRows = true
				hasMalformed = true
				reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
				reportValue.Content.Issues = append(reportValue.Content.Issues, rowIssues...)
			}
			if indexSet == reportv1alpha1.InstanceIndexSetSparse {
				reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
					Code:      reportv1alpha1.InstanceIssueSparseIndices,
					Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
				})
			}
		}
		reportValue.Content.Components = append(reportValue.Content.Components, reportv1alpha1.InstanceListComponent{
			Type: componentType(ir.Spec.Component), State: state, InferenceReplica: ir.Name,
			Generation: ir.Generation, ObservedGeneration: ir.Status.ObservedGeneration, IndexSet: indexSet,
			CurrentRevision: currentRevision, UpdateRevision: updateRevision,
			Replicas: ir.Status.Replicas, ReadyReplicas: ir.Status.ReadyReplicas,
			ServingReplicas: ir.Status.ServingReplicas, AvailableReplicas: ir.Status.AvailableReplicas,
			UpdatedReplicas: ir.Status.UpdatedReplicas, UpdatedReadyReplicas: ir.Status.UpdatedReadyReplicas,
		})
		reportValue.Sources = append(reportValue.Sources, reportv1alpha1.SourceReference{
			Kind: "InferenceReplica", Namespace: ir.Namespace, Name: ir.Name,
			UID: string(ir.UID), Generation: ir.Generation,
			Evidence: reportv1alpha1.EvidenceReported,
		})
		if skipRows {
			continue
		}

		rows := append([]omev1beta1.OMENativeInstanceStatus{}, ir.Status.InstanceStatuses...)
		sort.Slice(rows, func(i, j int) bool { return rows[i].Index < rows[j].Index })
		for rowIndex := range rows {
			if len(reportValue.Content.Instances) >= input.MaxInstances {
				outputTruncated = true
				reportValue.Content.Summary.Truncated = true
				reportValue.Content.Summary.State = reportv1alpha1.InstanceListStatePartial
				break
			}
			row := rows[rowIndex]
			reportValue.Content.Instances = append(reportValue.Content.Instances, reportv1alpha1.InstanceListInstance{
				Component: componentType(ir.Spec.Component), InferenceReplica: ir.Name,
				Index: row.Index, Incarnation: row.Incarnation, Phase: instancePhase(row.Phase),
				RunningRevision: row.RunningRevision, TargetRevision: row.TargetRevision,
				Pods: reportv1alpha1.InstancePodCounts{
					Total: row.PodCount, Serving: row.ServingPodCount,
					Available: row.AvailablePodCount,
				},
				Admitted: row.Admitted, OperationPresent: row.Operation != nil,
				LastFailurePresent: row.LastFailure != nil, Evidence: state,
			})
		}
	}

	reportValue.Content.Summary.Components = len(componentCounts)
	reportValue.Content.Summary.Instances = len(reportValue.Content.Instances)
	if input.CollectionUnavailable != "" && len(reportValue.Content.Components) == 0 {
		reportValue.Content.Summary.State = reportv1alpha1.InstanceListStateUnavailable
	}
	if outputTruncated {
		reportValue.Content.Issues = append(reportValue.Content.Issues, reportv1alpha1.InstanceListIssue{
			Code: reportv1alpha1.InstanceIssueOutputTruncated,
		})
	}
	if reportValue.Content.Summary.Truncated {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.InstanceListWarning{Code: reportv1alpha1.WarningTruncated})
	}
	if hasStale {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.InstanceListWarning{Code: reportv1alpha1.WarningStaleEvidence})
	}
	if hasMalformed {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.InstanceListWarning{Code: reportv1alpha1.WarningPartialData})
	}
	if hasUnavailable {
		reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.InstanceListWarning{Code: reportv1alpha1.WarningSourceUnavailable})
	}
	return reportValue.Canonical(), nil
}

func validateInstanceRows(
	rows []omev1beta1.OMENativeInstanceStatus,
	component reportv1alpha1.RuntimeComponentType,
	inferenceReplica string,
	maxIssues int,
) ([]reportv1alpha1.InstanceListIssue, bool, bool) {
	candidates := make(rowIssueHeap, 0, min(len(rows), maxIssues))
	totalIssues := 0
	for i := range rows {
		row := &rows[i]
		add := func(code reportv1alpha1.InstanceIssueCode) {
			totalIssues++
			if maxIssues == 0 {
				return
			}
			candidate := rowIssueCandidate{code: code, index: row.Index}
			if len(candidates) < maxIssues {
				heap.Push(&candidates, candidate)
				return
			}
			if rowIssueLess(candidate, candidates[0]) {
				candidates[0] = candidate
				heap.Fix(&candidates, 0)
			}
		}
		if row.Index < 0 {
			add(reportv1alpha1.InstanceIssueIndexInvalid)
		}
		if row.Incarnation < 0 {
			add(reportv1alpha1.InstanceIssueIncarnationInvalid)
		}
		if !validInstancePhase(row.Phase) {
			add(reportv1alpha1.InstanceIssuePhaseInvalid)
		}
		if !validOptionalRevision(row.RunningRevision) || !validOptionalRevision(row.TargetRevision) {
			add(reportv1alpha1.InstanceIssueInstanceRevisionInvalid)
		}
		if !validPodCounts(row) {
			add(reportv1alpha1.InstanceIssuePodCountsInvalid)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return rowIssueLess(candidates[i], candidates[j]) })
	issues := make([]reportv1alpha1.InstanceListIssue, len(candidates))
	for i := range candidates {
		index := candidates[i].index
		issues[i] = reportv1alpha1.InstanceListIssue{
			Code: candidates[i].code, Component: component, InferenceReplica: inferenceReplica, Index: &index,
		}
	}
	return issues, totalIssues > 0, totalIssues > len(issues)
}

type rowIssueCandidate struct {
	code  reportv1alpha1.InstanceIssueCode
	index int32
}

type rowIssueHeap []rowIssueCandidate

func (h rowIssueHeap) Len() int           { return len(h) }
func (h rowIssueHeap) Less(i, j int) bool { return rowIssueLess(h[j], h[i]) }
func (h rowIssueHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *rowIssueHeap) Push(value any)    { *h = append(*h, value.(rowIssueCandidate)) }
func (h *rowIssueHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

func rowIssueLess(left, right rowIssueCandidate) bool {
	if left.code != right.code {
		return left.code < right.code
	}
	return left.index < right.index
}

func validInstancePhase(phase omev1beta1.OMENativeInstancePhase) bool {
	switch phase {
	case omev1beta1.OMENativeInstancePending,
		omev1beta1.OMENativeInstanceCreating,
		omev1beta1.OMENativeInstanceReady,
		omev1beta1.OMENativeInstanceUpdating,
		omev1beta1.OMENativeInstanceRestarting,
		omev1beta1.OMENativeInstanceMigrating,
		omev1beta1.OMENativeInstanceFailed,
		omev1beta1.OMENativeInstanceDeleting:
		return true
	default:
		return false
	}
}

func validOptionalRevision(value string) bool {
	return value == "" || len(validation.IsDNS1123Subdomain(value)) == 0
}

func validPodCounts(row *omev1beta1.OMENativeInstanceStatus) bool {
	if row.PodCount < 0 || row.ServingPodCount < 0 || row.AvailablePodCount < 0 {
		return false
	}
	return row.ServingPodCount <= row.PodCount && row.AvailablePodCount <= row.ServingPodCount
}

func validAggregateStatus(status omev1beta1.InferenceReplicaStatus) bool {
	if status.Replicas < 0 || status.ReadyReplicas < 0 || status.ServingReplicas < 0 ||
		status.AvailableReplicas < 0 || status.UpdatedReplicas < 0 || status.UpdatedReadyReplicas < 0 {
		return false
	}
	if len(status.InstanceStatuses) > 0 && int(status.Replicas) != len(status.InstanceStatuses) {
		return false
	}
	return status.AvailableReplicas <= status.ServingReplicas &&
		status.ServingReplicas <= status.ReadyReplicas && status.ReadyReplicas <= status.Replicas &&
		status.UpdatedReplicas <= status.Replicas && status.UpdatedReadyReplicas <= status.UpdatedReplicas &&
		status.UpdatedReadyReplicas <= status.ReadyReplicas
}

func validSourceIdentity(namespace, name string, uid types.UID) bool {
	return len(validation.IsDNS1123Label(namespace)) == 0 &&
		len(validation.IsDNS1123Subdomain(name)) == 0 && validUID(uid)
}

func validCollectedReplicaIdentity(ir *omev1beta1.InferenceReplica, isvc *omev1beta1.InferenceService) bool {
	requireRelationshipLabel := len(validation.IsValidLabelValue(isvc.Name)) == 0
	if ir == nil || ir.Namespace != isvc.Namespace || !validSourceIdentity(ir.Namespace, ir.Name, ir.UID) ||
		(requireRelationshipLabel && ir.Labels[constants.InferenceServiceLabel] != isvc.Name) ||
		ir.Labels[constants.OMEComponentLabel] != string(ir.Spec.Component) ||
		ir.Spec.ParentRef.Name != isvc.Name || !validComponent(ir.Spec.Component) {
		return false
	}
	return validOwner(ir.OwnerReferences, isvc)
}

func validOwner(owners []metav1.OwnerReference, isvc *omev1beta1.InferenceService) bool {
	var controller *metav1.OwnerReference
	for i := range owners {
		if owners[i].Controller == nil || !*owners[i].Controller {
			continue
		}
		if controller != nil {
			return false
		}
		controller = &owners[i]
	}
	return controller != nil && controller.APIVersion == omev1beta1.SchemeGroupVersion.String() &&
		controller.Kind == "InferenceService" && controller.Name == isvc.Name && controller.UID == isvc.UID
}

func validComponent(component omev1beta1.ComponentType) bool {
	switch component {
	case omev1beta1.EngineComponent, omev1beta1.DecoderComponent, omev1beta1.RouterComponent:
		return true
	default:
		return false
	}
}

type parentGenerationState int

const (
	parentGenerationCurrent parentGenerationState = iota
	parentGenerationMissing
	parentGenerationInvalid
	parentGenerationStale
	parentGenerationAhead
)

func parentGenerationEvidence(
	ir *omev1beta1.InferenceReplica,
	isvcGeneration int64,
) parentGenerationState {
	raw, present := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	if !present {
		return parentGenerationMissing
	}
	if len(raw) == 0 || len(raw) > 19 {
		return parentGenerationInvalid
	}
	for i := range raw {
		if raw[i] < '0' || raw[i] > '9' {
			return parentGenerationInvalid
		}
	}
	generation, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || generation <= 0 || strconv.FormatInt(generation, 10) != raw {
		return parentGenerationInvalid
	}
	if generation < isvcGeneration {
		return parentGenerationStale
	}
	if generation > isvcGeneration {
		return parentGenerationAhead
	}
	return parentGenerationCurrent
}

func replicaIdentityKey(name string, component omev1beta1.ComponentType) string {
	return string(component) + "\x00" + name
}

func safeName(name string) string {
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return invalidIdentityName
	}
	return name
}

func validUID(uid types.UID) bool {
	if len(uid) == 0 || len(uid) > maxUIDLength {
		return false
	}
	for _, value := range []byte(uid) {
		if (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z') ||
			(value >= '0' && value <= '9') || value == '-' || value == '_' || value == '.' {
			continue
		}
		return false
	}
	return true
}

func firstDuplicateIndex(rows []omev1beta1.OMENativeInstanceStatus) (int32, bool) {
	seen := make(map[int32]struct{}, len(rows))
	var duplicate int32
	found := false
	for i := range rows {
		if _, exists := seen[rows[i].Index]; exists {
			if !found || rows[i].Index < duplicate {
				duplicate = rows[i].Index
			}
			found = true
			continue
		}
		seen[rows[i].Index] = struct{}{}
	}
	return duplicate, found
}

func classifyIndexSet(rows []omev1beta1.OMENativeInstanceStatus) reportv1alpha1.InstanceIndexSetState {
	if len(rows) == 0 {
		return reportv1alpha1.InstanceIndexSetEmpty
	}
	indices := make([]int32, len(rows))
	for i := range rows {
		indices[i] = rows[i].Index
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	for i, index := range indices {
		if index != int32(i) {
			return reportv1alpha1.InstanceIndexSetSparse
		}
	}
	return reportv1alpha1.InstanceIndexSetDense
}

func componentType(component omev1beta1.ComponentType) reportv1alpha1.RuntimeComponentType {
	return reportv1alpha1.RuntimeComponentType(component)
}

func componentRank(component omev1beta1.ComponentType) int {
	switch component {
	case omev1beta1.EngineComponent:
		return 0
	case omev1beta1.DecoderComponent:
		return 1
	case omev1beta1.RouterComponent:
		return 2
	default:
		return 3
	}
}

func instancePhase(phase omev1beta1.OMENativeInstancePhase) reportv1alpha1.InstancePhase {
	return reportv1alpha1.InstancePhase(phase)
}

func identityRejectionReason(reason instancecollection.RejectionReason) (reportv1alpha1.InstanceIdentityRejectionReason, bool) {
	switch reason {
	case instancecollection.RejectionMetadata:
		return reportv1alpha1.InstanceIdentityRejectionMetadata, true
	case instancecollection.RejectionLabel:
		return reportv1alpha1.InstanceIdentityRejectionLabel, true
	case instancecollection.RejectionParentReference:
		return reportv1alpha1.InstanceIdentityRejectionParentReference, true
	case instancecollection.RejectionOwnerReference:
		return reportv1alpha1.InstanceIdentityRejectionOwnerReference, true
	case instancecollection.RejectionComponent:
		return reportv1alpha1.InstanceIdentityRejectionComponent, true
	default:
		return "", false
	}
}

func validUnavailableReason(reason reportv1alpha1.UnavailableReason) bool {
	switch reason {
	case reportv1alpha1.UnavailableNotFound,
		reportv1alpha1.UnavailableForbidden,
		reportv1alpha1.UnavailableUnsupportedAPI,
		reportv1alpha1.UnavailableStaleGeneration,
		reportv1alpha1.UnavailableMalformedPayload,
		reportv1alpha1.UnavailableNotConfigured,
		reportv1alpha1.UnavailableUnreadable,
		reportv1alpha1.UnavailableCycle,
		reportv1alpha1.UnavailableMaxDepthExceeded,
		reportv1alpha1.UnavailableDisabled:
		return true
	default:
		return false
	}
}
