// Package migrationhistoryprojection joins authoritative InferenceReplica
// migration state with separately labeled bounded parent and audit history.
package migrationhistoryprojection

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/migrationcollection"
	"sigs.k8s.io/ome/pkg/cli/migrationhistorycollection"
	"sigs.k8s.io/ome/pkg/cli/migrationprojection"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

var (
	ErrInferenceServiceRequired        = errors.New("inference service is required")
	ErrInferenceServiceIdentityInvalid = errors.New("inference service identity is invalid")
	ErrInvalidComponent                = errors.New("component must be engine, decoder, or router")
	ErrInvalidLimits                   = errors.New("projection limits must be positive and internally consistent")
)

const (
	auditReasonAutoRecover             = "AutoRecover"
	auditReasonForceDelete             = "ForceDelete"
	auditOutcomeRelocate               = "relocate-recreate"
	auditOutcomeForceDeleteUnreachable = "force-delete-unreachable"
	auditOutcomeForceDeleteFinalizer   = "finalizer-report"
)

// Limits bounds all input scanning and emitted records.
type Limits struct {
	MaxRecords                 int
	MaxScannedRecordsPerSource int
	MaxNodeHints               int
	MaxScannedNodeHints        int
	MaxEvents                  int
	MaxScannedEvents           int
	MaxAuditBytes              int
}

type auditLedger struct {
	Entries []auditEntry `json:"entries"`
}

// auditEntry is a CLI-local wire type. Unknown additive fields are ignored,
// while arbitrary payload fields never reach a report.
type auditEntry struct {
	RequestUUID     string   `json:"requestUUID"`
	Component       string   `json:"component"`
	SourceInstance  *int32   `json:"sourceInstance"`
	SurgeInstance   *int32   `json:"surgeInstance,omitempty"`
	Phase           string   `json:"phase"`
	FromNode        string   `json:"fromNode,omitempty"`
	HintTargetNodes []string `json:"hintTargetNodes,omitempty"`
	StartedAt       string   `json:"startedAt"`
	CompletedAt     string   `json:"completedAt,omitempty"`
	Outcome         string   `json:"outcome,omitempty"`
	// Reason is retained only to recognize controller-owned record types. It
	// is never copied into a report because manual request reasons are
	// caller-supplied text and force-delete rows carry pod UIDs.
	Reason string `json:"reason,omitempty"`
}

// Project builds a redacted, deterministic report. Only InferenceReplica
// status records retain Authoritative evidence.
func Project(
	snapshot migrationhistorycollection.Result,
	componentFilter string,
	limits Limits,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.MigrationHistoryReport, error) {
	if snapshot.InferenceService == nil {
		return reportv1alpha1.MigrationHistoryReport{}, ErrInferenceServiceRequired
	}
	parent := snapshot.InferenceService
	if !validParent(parent) {
		return reportv1alpha1.MigrationHistoryReport{}, ErrInferenceServiceIdentityInvalid
	}
	filter, filterSet := component(componentFilter)
	if componentFilter != "" && !filterSet {
		return reportv1alpha1.MigrationHistoryReport{}, ErrInvalidComponent
	}
	if !validLimits(limits) {
		return reportv1alpha1.MigrationHistoryReport{}, ErrInvalidLimits
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	now := clock.Now().UTC()
	reportValue := reportv1alpha1.NewMigrationHistoryReport(
		reportv1alpha1.Metadata{Namespace: parent.Namespace, Name: parent.Name},
		reportv1alpha1.MigrationHistoryContent{
			Records: []reportv1alpha1.MigrationHistoryRecord{},
			Issues:  []reportv1alpha1.MigrationHistoryIssue{},
		},
		reportv1alpha1.ClockFunc(func() time.Time { return now }),
	)

	parentFreshness := parentFreshness(parent)
	reportValue.Sources = append(reportValue.Sources,
		authoritativeSource(snapshot, now),
		reportv1alpha1.MigrationHistorySource{
			Kind:      reportv1alpha1.MigrationHistorySourceInferenceService,
			Namespace: parent.Namespace, Name: parent.Name,
			Evidence:     reportv1alpha1.MigrationHistoryEvidenceParent,
			Availability: reportv1alpha1.MigrationHistoryAvailabilityAvailable,
			Freshness:    parentFreshness, ObservedItems: len(parent.Status.MigrationHistory),
			Bounded: true, CollectedAt: now,
		},
		auditSource(snapshot.Audit, now),
	)

	projectAuthoritative(snapshot, filter, filterSet, limits, now, &reportValue)
	projectParent(parent, filter, filterSet, limits, parentFreshness, &reportValue)
	projectAudit(snapshot.Audit, filter, filterSet, limits, &reportValue)
	analyzeCorrelations(&reportValue)

	canonical := reportValue.Canonical()
	if len(canonical.Content.Records) > limits.MaxRecords {
		canonical.Content.Records = canonical.Content.Records[:limits.MaxRecords]
		addIssue(&canonical, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueOutputTruncated})
		addWarning(&canonical, reportv1alpha1.MigrationHistoryWarningTruncated)
	}
	recomputeSummary(&canonical)
	return canonical.Canonical(), nil
}

func authoritativeSource(
	snapshot migrationhistorycollection.Result,
	now time.Time,
) reportv1alpha1.MigrationHistorySource {
	availability, reason := availability(snapshot.Replicas.Availability)
	return reportv1alpha1.MigrationHistorySource{
		Kind:      reportv1alpha1.MigrationHistorySourceInferenceReplica,
		Namespace: snapshot.InferenceService.Namespace, Name: "related-inferencereplicas",
		Evidence:     reportv1alpha1.MigrationHistoryEvidenceAuthoritative,
		Availability: availability, UnavailableReason: reason,
		Freshness:     reportv1alpha1.MigrationHistoryFreshnessUnverifiable,
		ObservedPages: snapshot.Replicas.ObservedPages, ObservedItems: snapshot.Replicas.ObservedItems,
		Bounded: true, Truncated: snapshot.Replicas.Truncated, CollectedAt: now,
	}
}

func auditSource(observation migrationhistorycollection.AuditObservation, now time.Time) reportv1alpha1.MigrationHistorySource {
	available, reason := availability(observation.Availability)
	return reportv1alpha1.MigrationHistorySource{
		Kind:      reportv1alpha1.MigrationHistorySourceConfigMap,
		Namespace: observation.Namespace, Name: observation.Name,
		Evidence:     reportv1alpha1.MigrationHistoryEvidenceAudit,
		Availability: available, UnavailableReason: reason,
		Freshness: reportv1alpha1.MigrationHistoryFreshnessUnverifiable,
		Bounded:   true, CollectedAt: now,
	}
}

func projectAuthoritative(
	snapshot migrationhistorycollection.Result,
	filter reportv1alpha1.RuntimeComponentType,
	filterSet bool,
	limits Limits,
	now time.Time,
	reportValue *reportv1alpha1.MigrationHistoryReport,
) {
	if snapshot.Replicas.Availability != migrationhistorycollection.AvailabilityAvailable {
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuthoritativeUnavailable})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningPartial)
		return
	}
	if snapshot.Replicas.Truncated {
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuthoritativeTruncated})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningTruncated)
	}
	componentFilter := ""
	if filterSet {
		componentFilter = string(filter)
	}
	projected, err := migrationprojection.Project(
		migrationcollection.Result{
			InferenceService:  snapshot.InferenceService,
			InferenceReplicas: snapshot.InferenceReplicas,
			Completeness: migrationcollection.Completeness{
				ObservedPages: snapshot.Replicas.ObservedPages,
				ObservedItems: snapshot.Replicas.ObservedItems,
				Truncated:     snapshot.Replicas.Truncated,
			},
		},
		componentFilter,
		migrationprojection.Limits{
			MaxRecords:        limits.MaxScannedRecordsPerSource,
			MaxScannedRecords: limits.MaxScannedRecordsPerSource,
			MaxNodeHints:      limits.MaxNodeHints, MaxScannedNodeHints: limits.MaxScannedNodeHints,
		},
		reportv1alpha1.ClockFunc(func() time.Time { return now }),
	)
	if err != nil {
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuthoritativeUnavailable})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningPartial)
		return
	}
	authoritativeFreshnessSet := false
	for _, source := range projected.Sources {
		if source.Kind != reportv1alpha1.MigrationSourceInferenceReplica {
			continue
		}
		freshness := mapFreshness(source.Freshness)
		for i := range reportValue.Sources {
			if reportValue.Sources[i].Evidence != reportv1alpha1.MigrationHistoryEvidenceAuthoritative {
				continue
			}
			if !authoritativeFreshnessSet || freshnessRank(freshness) > freshnessRank(reportValue.Sources[i].Freshness) {
				reportValue.Sources[i].Freshness = freshness
			}
		}
		authoritativeFreshnessSet = true
	}
	for _, record := range projected.Content.Migrations {
		reportValue.Content.Records = append(reportValue.Content.Records, reportv1alpha1.MigrationHistoryRecord{
			Evidence:   reportv1alpha1.MigrationHistoryEvidenceAuthoritative,
			SourceName: record.SourceName, RequestID: record.RequestID,
			Component: record.Component, SourceInstance: record.SourceInstance,
			ReplacementInstance: copyInt32(record.SurgeInstance), Trigger: record.Trigger,
			Mode:  reportv1alpha1.MigrationHistoryModeUnknown,
			Phase: mapPhase(string(record.Phase)), State: mapClassification(record.Classification),
			Freshness: mapFreshness(record.Freshness), FromNode: record.FromNode,
			TargetNodeHints: append([]string{}, record.TargetNodeHints...),
			RequestedAt:     copyTime(record.RequestedAt), StartedAt: copyTime(record.StartedAt),
			AllocatedAt: copyTime(record.AllocatedAt), Deadline: copyTime(record.Deadline),
			CompletedAt: copyTime(record.CompletedAt), Outcome: record.Outcome,
			Message: record.Message, AuthoritativeIssues: append([]reportv1alpha1.MigrationIssueCode{}, record.Issues...),
		})
	}
	for _, issue := range projected.Content.Issues {
		if issue.Code == reportv1alpha1.MigrationIssueSourcesTruncated || issue.Code == reportv1alpha1.MigrationIssueRecordsTruncated {
			markSourceTruncated(reportValue, reportv1alpha1.MigrationHistoryEvidenceAuthoritative)
			addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{
				Code: reportv1alpha1.MigrationHistoryIssueAuthoritativeTruncated,
			})
			addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningTruncated)
			continue
		}
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{
			Code:      reportv1alpha1.MigrationHistoryIssueRecordInvalid,
			Evidence:  reportv1alpha1.MigrationHistoryEvidenceAuthoritative,
			RequestID: safeRequestID(issue.RequestID),
		})
	}
}

func projectParent(
	parent *omev1beta1.InferenceService,
	filter reportv1alpha1.RuntimeComponentType,
	filterSet bool,
	limits Limits,
	freshness reportv1alpha1.MigrationHistoryFreshness,
	reportValue *reportv1alpha1.MigrationHistoryReport,
) {
	entries := parent.Status.MigrationHistory
	if len(entries) > limits.MaxScannedRecordsPerSource {
		entries = entries[:limits.MaxScannedRecordsPerSource]
		markSourceTruncated(reportValue, reportv1alpha1.MigrationHistoryEvidenceParent)
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueParentTruncated})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningTruncated)
	}
	for i := range entries {
		record, componentOK := parentRecord(entries[i], parent.Name, freshness, limits)
		if filterSet && (!componentOK || record.Component != filter) {
			continue
		}
		reportValue.Content.Records = append(reportValue.Content.Records, record)
	}
}

func parentRecord(
	entry omev1beta1.MigrationHistoryEntry,
	sourceName string,
	freshness reportv1alpha1.MigrationHistoryFreshness,
	limits Limits,
) (reportv1alpha1.MigrationHistoryRecord, bool) {
	record := reportv1alpha1.MigrationHistoryRecord{
		Evidence: reportv1alpha1.MigrationHistoryEvidenceParent, SourceName: sourceName,
		RequestID: safeRequestID(entry.ID), SourceInstance: entry.Instance,
		ReplacementInstance: copyInt32(entry.ReplacementInstance), Trigger: reportv1alpha1.MigrationTriggerUnknown,
		Mode: mapMode(string(entry.Mode)), Phase: mapPhase(string(entry.Phase)), Freshness: freshness,
		RequestedAt: metaTime(entry.RequestedAt), StartedAt: metaTimePtr(entry.StartedAt),
		CompletedAt: metaTimePtr(entry.CompletedAt), EventCount: min(len(entry.Events), limits.MaxEvents),
		Outcome:         phaseOutcome(mapPhase(string(entry.Phase))),
		Message:         printers.BoundedCell(entry.OutcomeReason, reportv1alpha1.MigrationHistoryMessageMaxDisplayWidth),
		TargetNodeHints: []string{}, AuthoritativeIssues: []reportv1alpha1.MigrationIssueCode{},
		Issues: []reportv1alpha1.MigrationHistoryIssueCode{},
	}
	component, componentOK := component(string(entry.Component))
	if componentOK {
		record.Component = component
	} else {
		record.Component = reportv1alpha1.RuntimeComponentType("unknown")
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueComponentInvalid)
	}
	if record.RequestID == "INVALID" {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueRequestIDInvalid)
	}
	if entry.Instance < 0 {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueInstanceInvalid)
	}
	if entry.ReplacementInstance != nil && (*entry.ReplacementInstance < 0 || *entry.ReplacementInstance == entry.Instance) {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueReplacementInvalid)
	}
	if record.Mode == reportv1alpha1.MigrationHistoryModeUnknown {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueModeInvalid)
	}
	if record.Phase == reportv1alpha1.MigrationHistoryPhaseUnknown {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssuePhaseInvalid)
	}
	if record.RequestedAt == nil {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueTimestampInvalid)
	}
	validateRecordTimes(&record)
	validateEvents(entry.Events, limits, &record)
	finishRecordState(&record)
	return record, componentOK
}

func projectAudit(
	observation migrationhistorycollection.AuditObservation,
	filter reportv1alpha1.RuntimeComponentType,
	filterSet bool,
	limits Limits,
	reportValue *reportv1alpha1.MigrationHistoryReport,
) {
	sourceIndex := sourceIndex(reportValue, reportv1alpha1.MigrationHistoryEvidenceAudit)
	switch observation.Availability {
	case migrationhistorycollection.AvailabilityAbsent:
		return
	case migrationhistorycollection.AvailabilityForbidden, migrationhistorycollection.AvailabilityUnreadable:
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuditUnavailable})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningPartial)
		return
	case migrationhistorycollection.AvailabilityInvalid:
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuditIdentityInvalid})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningPartial)
		return
	case migrationhistorycollection.AvailabilityAvailable:
	default:
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuditUnavailable})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningPartial)
		return
	}
	if observation.HistoryJSON == "" {
		return
	}
	if len(observation.HistoryJSON) > limits.MaxAuditBytes {
		setSourceUnavailable(reportValue, sourceIndex, reportv1alpha1.MigrationHistoryUnavailablePayloadTooLarge)
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuditPayloadTooLarge})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningPartial)
		return
	}
	var ledger auditLedger
	if err := json.Unmarshal([]byte(observation.HistoryJSON), &ledger); err != nil {
		setSourceUnavailable(reportValue, sourceIndex, reportv1alpha1.MigrationHistoryUnavailableMalformed)
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuditMalformed})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningPartial)
		return
	}
	entries := ledger.Entries
	if sourceIndex >= 0 {
		reportValue.Sources[sourceIndex].ObservedItems = len(entries)
	}
	if len(entries) > limits.MaxScannedRecordsPerSource {
		entries = entries[:limits.MaxScannedRecordsPerSource]
		markSourceTruncated(reportValue, reportv1alpha1.MigrationHistoryEvidenceAudit)
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: reportv1alpha1.MigrationHistoryIssueAuditTruncated})
		addWarning(reportValue, reportv1alpha1.MigrationHistoryWarningTruncated)
	}
	for _, entry := range entries {
		if forceDeleteAuditEntry(entry) {
			continue
		}
		record, componentOK := projectAuditRecord(entry, observation.Name, limits)
		if filterSet && (!componentOK || record.Component != filter) {
			continue
		}
		reportValue.Content.Records = append(reportValue.Content.Records, record)
	}
}

func projectAuditRecord(
	entry auditEntry,
	sourceName string,
	limits Limits,
) (reportv1alpha1.MigrationHistoryRecord, bool) {
	sourceInstance := int32(-1)
	if entry.SourceInstance != nil {
		sourceInstance = *entry.SourceInstance
	}
	record := reportv1alpha1.MigrationHistoryRecord{
		Evidence: reportv1alpha1.MigrationHistoryEvidenceAudit, SourceName: sourceName,
		RequestID: safeRequestID(entry.RequestUUID), SourceInstance: sourceInstance,
		ReplacementInstance: copyInt32(entry.SurgeInstance), Trigger: reportv1alpha1.MigrationTriggerUnknown,
		Mode: reportv1alpha1.MigrationHistoryModeUnknown, Phase: mapPhase(entry.Phase),
		Freshness: reportv1alpha1.MigrationHistoryFreshnessUnverifiable,
		FromNode:  safeNode(entry.FromNode), TargetNodeHints: safeNodes(entry.HintTargetNodes, limits, nil),
		StartedAt: parseTime(entry.StartedAt), CompletedAt: parseTime(entry.CompletedAt),
		Outcome:             auditOutcome(entry),
		Message:             printers.BoundedCell(entry.Outcome, reportv1alpha1.MigrationHistoryMessageMaxDisplayWidth),
		AuthoritativeIssues: []reportv1alpha1.MigrationIssueCode{}, Issues: []reportv1alpha1.MigrationHistoryIssueCode{},
	}
	if entry.Reason == auditReasonAutoRecover {
		record.Trigger = reportv1alpha1.MigrationTriggerAuto
	}
	componentValue, componentOK := component(entry.Component)
	if componentOK {
		record.Component = componentValue
	} else if entry.Component == "" {
		record.Component = reportv1alpha1.RuntimeComponentType("unknown")
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueComponentUnavailable)
	} else {
		record.Component = reportv1alpha1.RuntimeComponentType("unknown")
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueComponentInvalid)
	}
	if record.RequestID == "INVALID" {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueRequestIDInvalid)
	}
	if entry.SourceInstance == nil || *entry.SourceInstance < 0 {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueInstanceInvalid)
	}
	if entry.SurgeInstance != nil {
		if *entry.SurgeInstance == -1 && entry.Phase == "Started" {
			record.ReplacementInstance = nil
		} else if *entry.SurgeInstance < 0 || *entry.SurgeInstance == record.SourceInstance {
			addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueReplacementInvalid)
		}
	}
	if record.Phase == reportv1alpha1.MigrationHistoryPhaseUnknown {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssuePhaseInvalid)
	}
	if entry.FromNode != "" && record.FromNode == "" {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueNodeInvalid)
	}
	if len(entry.HintTargetNodes) > limits.MaxScannedNodeHints || len(entry.HintTargetNodes) > limits.MaxNodeHints {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueNodeHintsTruncated)
	}
	for _, hint := range firstStrings(entry.HintTargetNodes, limits.MaxScannedNodeHints) {
		if !safeResourceName(hint) {
			addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueNodeInvalid)
		}
	}
	if record.StartedAt == nil || (entry.CompletedAt != "" && record.CompletedAt == nil) {
		addRecordIssue(&record, reportv1alpha1.MigrationHistoryIssueTimestampInvalid)
	}
	validateRecordTimes(&record)
	finishRecordState(&record)
	return record, componentOK
}

func analyzeCorrelations(reportValue *reportv1alpha1.MigrationHistoryReport) {
	type groupKey struct {
		evidence reportv1alpha1.MigrationHistoryEvidence
		request  string
	}
	within := make(map[groupKey]int)
	byRequest := make(map[string][]int)
	for i, record := range reportValue.Content.Records {
		if record.RequestID == "" || record.RequestID == "INVALID" {
			continue
		}
		within[groupKey{record.Evidence, record.RequestID}]++
		byRequest[record.RequestID] = append(byRequest[record.RequestID], i)
	}
	for key, count := range within {
		if count <= 1 {
			continue
		}
		addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{
			Code:     reportv1alpha1.MigrationHistoryIssueDuplicateWithinSource,
			Evidence: key.evidence, RequestID: key.request,
		})
		for _, index := range byRequest[key.request] {
			if reportValue.Content.Records[index].Evidence == key.evidence {
				addRecordIssue(&reportValue.Content.Records[index], reportv1alpha1.MigrationHistoryIssueDuplicateWithinSource)
				finishRecordState(&reportValue.Content.Records[index])
			}
		}
	}
	requests := make([]string, 0, len(byRequest))
	for request := range byRequest {
		requests = append(requests, request)
	}
	sort.Strings(requests)
	for _, request := range requests {
		indexes := byRequest[request]
		if crossSourceIdentityConflict(reportValue.Content.Records, indexes) {
			addGroupIssue(reportValue, request, indexes, reportv1alpha1.MigrationHistoryIssueCrossSourceIdentityConflict)
		}
		if chronologyConflict(reportValue.Content.Records, indexes) {
			addGroupIssue(reportValue, request, indexes, reportv1alpha1.MigrationHistoryIssueChronologyConflict)
		}
		if terminalOutcomeConflict(reportValue.Content.Records, indexes) {
			addGroupIssue(reportValue, request, indexes, reportv1alpha1.MigrationHistoryIssueTerminalOutcomeConflict)
		}
	}
}

func crossSourceIdentityConflict(records []reportv1alpha1.MigrationHistoryRecord, indexes []int) bool {
	for i, leftIndex := range indexes {
		left := records[leftIndex]
		if left.Component == "unknown" || left.SourceInstance < 0 {
			continue
		}
		for _, rightIndex := range indexes[i+1:] {
			right := records[rightIndex]
			if left.Evidence == right.Evidence || right.Component == "unknown" || right.SourceInstance < 0 {
				continue
			}
			if left.Component != right.Component || left.SourceInstance != right.SourceInstance {
				return true
			}
			if left.ReplacementInstance != nil && right.ReplacementInstance != nil &&
				*left.ReplacementInstance != *right.ReplacementInstance {
				return true
			}
		}
	}
	return false
}

func chronologyConflict(records []reportv1alpha1.MigrationHistoryRecord, indexes []int) bool {
	var latestStart *time.Time
	var earliestCompletion *time.Time
	for _, index := range indexes {
		record := records[index]
		for _, candidate := range []*time.Time{record.RequestedAt, record.StartedAt, record.AllocatedAt} {
			if candidate != nil && (latestStart == nil || candidate.After(*latestStart)) {
				latestStart = candidate
			}
		}
		if record.CompletedAt != nil && (earliestCompletion == nil || record.CompletedAt.Before(*earliestCompletion)) {
			earliestCompletion = record.CompletedAt
		}
	}
	return latestStart != nil && earliestCompletion != nil && earliestCompletion.Before(*latestStart)
}

func terminalOutcomeConflict(records []reportv1alpha1.MigrationHistoryRecord, indexes []int) bool {
	var outcome reportv1alpha1.MigrationOutcome
	set := false
	for _, index := range indexes {
		record := records[index]
		if !terminalPhase(record.Phase) || record.Outcome == reportv1alpha1.MigrationOutcomeUnknown {
			continue
		}
		recordOutcome := comparableTerminalOutcome(record.Outcome)
		if !set {
			outcome, set = recordOutcome, true
			continue
		}
		if recordOutcome != outcome {
			return true
		}
	}
	return false
}

func comparableTerminalOutcome(outcome reportv1alpha1.MigrationOutcome) reportv1alpha1.MigrationOutcome {
	if outcome == reportv1alpha1.MigrationOutcomeRelocationConfirmed {
		return reportv1alpha1.MigrationOutcomeRelocated
	}
	return outcome
}

func addGroupIssue(
	reportValue *reportv1alpha1.MigrationHistoryReport,
	request string,
	indexes []int,
	code reportv1alpha1.MigrationHistoryIssueCode,
) {
	addIssue(reportValue, reportv1alpha1.MigrationHistoryIssue{Code: code, RequestID: request})
	for _, index := range indexes {
		addRecordIssue(&reportValue.Content.Records[index], code)
		finishRecordState(&reportValue.Content.Records[index])
	}
}

func recomputeSummary(reportValue *reportv1alpha1.MigrationHistoryReport) {
	summary := reportv1alpha1.MigrationHistorySummary{Truncated: hasTruncation(reportValue)}
	requests := make(map[string]struct{})
	for _, record := range reportValue.Content.Records {
		if record.RequestID != "" && record.RequestID != "INVALID" {
			requests[record.RequestID] = struct{}{}
		}
		summary.Records++
		switch record.Evidence {
		case reportv1alpha1.MigrationHistoryEvidenceAuthoritative:
			summary.Authoritative++
			if record.State == reportv1alpha1.MigrationHistoryStateActive {
				summary.ActiveAuthoritative++
			}
			if record.State == reportv1alpha1.MigrationHistoryStateTerminal {
				summary.TerminalAuthoritative++
			}
		case reportv1alpha1.MigrationHistoryEvidenceParent:
			summary.ParentSummary++
		case reportv1alpha1.MigrationHistoryEvidenceAudit:
			summary.Audit++
		}
		if record.State == reportv1alpha1.MigrationHistoryStateInvalid {
			summary.Invalid++
		}
	}
	summary.Requests = len(requests)
	switch {
	case len(reportValue.Content.Issues) > 0 || summary.Invalid > 0:
		summary.State = reportv1alpha1.MigrationHistoryReportPartial
	case summary.Records > 0:
		summary.State = reportv1alpha1.MigrationHistoryReportReported
	default:
		summary.State = reportv1alpha1.MigrationHistoryReportEmpty
	}
	reportValue.Content.Summary = summary
}

func validateRecordTimes(record *reportv1alpha1.MigrationHistoryRecord) {
	invalid := false
	for _, value := range []*time.Time{record.RequestedAt, record.StartedAt, record.AllocatedAt, record.Deadline, record.CompletedAt} {
		if value != nil && value.IsZero() {
			invalid = true
		}
	}
	if record.StartedAt != nil && record.RequestedAt != nil && record.StartedAt.Before(*record.RequestedAt) {
		invalid = true
	}
	start := record.StartedAt
	if start == nil {
		start = record.RequestedAt
	}
	if record.AllocatedAt != nil && start != nil && record.AllocatedAt.Before(*start) {
		invalid = true
	}
	if record.CompletedAt != nil && start != nil && record.CompletedAt.Before(*start) {
		invalid = true
	}
	if record.Deadline != nil && start != nil && record.Deadline.Before(*start) {
		invalid = true
	}
	terminal := terminalPhase(record.Phase)
	if (terminal && record.CompletedAt == nil) || (!terminal && record.CompletedAt != nil) {
		invalid = true
	}
	if invalid {
		addRecordIssue(record, reportv1alpha1.MigrationHistoryIssueTimestampInvalid)
	}
}

func validateEvents(events []omev1beta1.MigrationEvent, limits Limits, record *reportv1alpha1.MigrationHistoryRecord) {
	if len(events) > limits.MaxEvents || len(events) > limits.MaxScannedEvents {
		addRecordIssue(record, reportv1alpha1.MigrationHistoryIssueEventsTruncated)
	}
	for _, event := range events[:min(len(events), limits.MaxScannedEvents)] {
		if event.At.IsZero() || (record.RequestedAt != nil && event.At.Time.Before(*record.RequestedAt)) ||
			(record.CompletedAt != nil && event.At.Time.After(*record.CompletedAt)) {
			addRecordIssue(record, reportv1alpha1.MigrationHistoryIssueEventInvalid)
		}
	}
}

func finishRecordState(record *reportv1alpha1.MigrationHistoryRecord) {
	if len(record.Issues) > 0 || len(record.AuthoritativeIssues) > 0 {
		record.State = reportv1alpha1.MigrationHistoryStateInvalid
		return
	}
	if terminalPhase(record.Phase) {
		record.State = reportv1alpha1.MigrationHistoryStateTerminal
		return
	}
	switch record.Phase {
	case reportv1alpha1.MigrationHistoryPhaseAccepted,
		reportv1alpha1.MigrationHistoryPhaseSurgePending,
		reportv1alpha1.MigrationHistoryPhaseSurgeReady,
		reportv1alpha1.MigrationHistoryPhaseDraining,
		reportv1alpha1.MigrationHistoryPhasePending,
		reportv1alpha1.MigrationHistoryPhaseInProgress,
		reportv1alpha1.MigrationHistoryPhaseStarted:
		record.State = reportv1alpha1.MigrationHistoryStateActive
	default:
		record.State = reportv1alpha1.MigrationHistoryStateUnknown
	}
}

func component(value string) (reportv1alpha1.RuntimeComponentType, bool) {
	switch value {
	case string(omev1beta1.EngineComponent):
		return reportv1alpha1.RuntimeComponentEngine, true
	case string(omev1beta1.DecoderComponent):
		return reportv1alpha1.RuntimeComponentDecoder, true
	case string(omev1beta1.RouterComponent):
		return reportv1alpha1.RuntimeComponentRouter, true
	default:
		return "", false
	}
}

func mapMode(value string) reportv1alpha1.MigrationHistoryMode {
	if value == string(omev1beta1.MigrationModeSurge) {
		return reportv1alpha1.MigrationHistoryModeSurge
	}
	return reportv1alpha1.MigrationHistoryModeUnknown
}

func mapPhase(value string) reportv1alpha1.MigrationHistoryPhase {
	switch value {
	case string(omev1beta1.MigrationPhaseAccepted):
		return reportv1alpha1.MigrationHistoryPhaseAccepted
	case string(omev1beta1.MigrationPhaseSurgePending):
		return reportv1alpha1.MigrationHistoryPhaseSurgePending
	case string(omev1beta1.MigrationPhaseSurgeReady):
		return reportv1alpha1.MigrationHistoryPhaseSurgeReady
	case string(omev1beta1.MigrationPhaseDraining):
		return reportv1alpha1.MigrationHistoryPhaseDraining
	case string(omev1beta1.MigrationPhasePending):
		return reportv1alpha1.MigrationHistoryPhasePending
	case string(omev1beta1.MigrationPhaseInProgress):
		return reportv1alpha1.MigrationHistoryPhaseInProgress
	case "Started":
		return reportv1alpha1.MigrationHistoryPhaseStarted
	case string(omev1beta1.MigrationPhaseCompleted):
		return reportv1alpha1.MigrationHistoryPhaseCompleted
	case string(omev1beta1.MigrationPhaseFailed):
		return reportv1alpha1.MigrationHistoryPhaseFailed
	case string(omev1beta1.MigrationPhaseRelocated):
		return reportv1alpha1.MigrationHistoryPhaseRelocated
	default:
		return reportv1alpha1.MigrationHistoryPhaseUnknown
	}
}

func mapClassification(value reportv1alpha1.MigrationClassification) reportv1alpha1.MigrationHistoryState {
	switch value {
	case reportv1alpha1.MigrationClassificationActive:
		return reportv1alpha1.MigrationHistoryStateActive
	case reportv1alpha1.MigrationClassificationTerminal:
		return reportv1alpha1.MigrationHistoryStateTerminal
	case reportv1alpha1.MigrationClassificationInvalid:
		return reportv1alpha1.MigrationHistoryStateInvalid
	default:
		return reportv1alpha1.MigrationHistoryStateUnknown
	}
}

func mapFreshness(value reportv1alpha1.StatusFreshness) reportv1alpha1.MigrationHistoryFreshness {
	switch value {
	case reportv1alpha1.StatusFreshnessCurrent:
		return reportv1alpha1.MigrationHistoryFreshnessCurrent
	case reportv1alpha1.StatusFreshnessStale:
		return reportv1alpha1.MigrationHistoryFreshnessStale
	case reportv1alpha1.StatusFreshnessUnobserved:
		return reportv1alpha1.MigrationHistoryFreshnessUnobserved
	default:
		return reportv1alpha1.MigrationHistoryFreshnessInvalid
	}
}

func parentFreshness(parent *omev1beta1.InferenceService) reportv1alpha1.MigrationHistoryFreshness {
	switch {
	case parent.Generation <= 0 || parent.Status.ObservedGeneration < 0 || parent.Status.ObservedGeneration > parent.Generation:
		return reportv1alpha1.MigrationHistoryFreshnessInvalid
	case parent.Status.ObservedGeneration == 0:
		return reportv1alpha1.MigrationHistoryFreshnessUnobserved
	case parent.Status.ObservedGeneration < parent.Generation:
		return reportv1alpha1.MigrationHistoryFreshnessStale
	default:
		return reportv1alpha1.MigrationHistoryFreshnessCurrent
	}
}

func availability(value migrationhistorycollection.Availability) (reportv1alpha1.MigrationHistoryAvailability, reportv1alpha1.MigrationHistoryUnavailableReason) {
	switch value {
	case migrationhistorycollection.AvailabilityAvailable:
		return reportv1alpha1.MigrationHistoryAvailabilityAvailable, ""
	case migrationhistorycollection.AvailabilityAbsent:
		return reportv1alpha1.MigrationHistoryAvailabilityUnavailable, reportv1alpha1.MigrationHistoryUnavailableNotFound
	case migrationhistorycollection.AvailabilityForbidden:
		return reportv1alpha1.MigrationHistoryAvailabilityUnavailable, reportv1alpha1.MigrationHistoryUnavailableForbidden
	case migrationhistorycollection.AvailabilityInvalid:
		return reportv1alpha1.MigrationHistoryAvailabilityUnavailable, reportv1alpha1.MigrationHistoryUnavailableInvalidIdentity
	default:
		return reportv1alpha1.MigrationHistoryAvailabilityUnavailable, reportv1alpha1.MigrationHistoryUnavailableUnreadable
	}
}

func auditOutcome(entry auditEntry) reportv1alpha1.MigrationOutcome {
	if entry.Reason == auditReasonAutoRecover && entry.Outcome == auditOutcomeRelocate {
		return reportv1alpha1.MigrationOutcomeRelocated
	}
	switch entry.Phase {
	case "Started":
		return reportv1alpha1.MigrationOutcomeInProgress
	case "Completed":
		return reportv1alpha1.MigrationOutcomeCompleted
	case "Failed":
		return reportv1alpha1.MigrationOutcomeFailed
	default:
		return reportv1alpha1.MigrationOutcomeUnknown
	}
}

func forceDeleteAuditEntry(entry auditEntry) bool {
	return entry.Reason == auditReasonForceDelete &&
		(entry.Outcome == auditOutcomeForceDeleteUnreachable || entry.Outcome == auditOutcomeForceDeleteFinalizer)
}

func phaseOutcome(phase reportv1alpha1.MigrationHistoryPhase) reportv1alpha1.MigrationOutcome {
	switch phase {
	case reportv1alpha1.MigrationHistoryPhaseCompleted:
		return reportv1alpha1.MigrationOutcomeCompleted
	case reportv1alpha1.MigrationHistoryPhaseFailed:
		return reportv1alpha1.MigrationOutcomeFailed
	case reportv1alpha1.MigrationHistoryPhaseRelocated:
		return reportv1alpha1.MigrationOutcomeRelocated
	case reportv1alpha1.MigrationHistoryPhaseAccepted, reportv1alpha1.MigrationHistoryPhaseSurgePending,
		reportv1alpha1.MigrationHistoryPhaseSurgeReady, reportv1alpha1.MigrationHistoryPhaseDraining,
		reportv1alpha1.MigrationHistoryPhasePending, reportv1alpha1.MigrationHistoryPhaseInProgress,
		reportv1alpha1.MigrationHistoryPhaseStarted:
		return reportv1alpha1.MigrationOutcomeInProgress
	default:
		return reportv1alpha1.MigrationOutcomeUnknown
	}
}

func terminalPhase(phase reportv1alpha1.MigrationHistoryPhase) bool {
	return phase == reportv1alpha1.MigrationHistoryPhaseCompleted ||
		phase == reportv1alpha1.MigrationHistoryPhaseFailed ||
		phase == reportv1alpha1.MigrationHistoryPhaseRelocated
}

func validParent(parent *omev1beta1.InferenceService) bool {
	return parent.Name != "" && parent.Namespace != "" && parent.UID != "" &&
		len(utilvalidation.IsDNS1123Subdomain(parent.Name)) == 0 &&
		len(utilvalidation.IsDNS1123Label(parent.Namespace)) == 0
}

func validLimits(limits Limits) bool {
	return limits.MaxRecords > 0 && limits.MaxScannedRecordsPerSource >= limits.MaxRecords &&
		limits.MaxNodeHints > 0 && limits.MaxScannedNodeHints >= limits.MaxNodeHints &&
		limits.MaxEvents > 0 && limits.MaxScannedEvents >= limits.MaxEvents && limits.MaxAuditBytes > 0
}

func safeRequestID(value string) string {
	if value == "" || len(value) > 63 || !alphaNumeric(value[0]) || !alphaNumeric(value[len(value)-1]) {
		return "INVALID"
	}
	for i := range len(value) {
		if !alphaNumeric(value[i]) && value[i] != '-' && value[i] != '_' && value[i] != '.' {
			return "INVALID"
		}
	}
	return value
}

func safeNode(value string) string {
	if safeResourceName(value) {
		return value
	}
	return ""
}

func safeNodes(values []string, limits Limits, record *reportv1alpha1.MigrationHistoryRecord) []string {
	result := make([]string, 0, min(len(values), limits.MaxNodeHints))
	for _, value := range firstStrings(values, limits.MaxScannedNodeHints) {
		if safeResourceName(value) {
			result = append(result, value)
		} else if record != nil {
			addRecordIssue(record, reportv1alpha1.MigrationHistoryIssueNodeInvalid)
		}
		if len(result) == limits.MaxNodeHints {
			break
		}
	}
	sort.Strings(result)
	return compactStrings(result)
}

func safeResourceName(value string) bool {
	return value != "" && len(utilvalidation.IsDNS1123Subdomain(value)) == 0
}

func alphaNumeric(char byte) bool {
	return char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9'
}

func parseTime(value string) *time.Time {
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.IsZero() {
		return nil
	}
	result := parsed.UTC()
	return &result
}

func metaTime(value metav1.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func metaTimePtr(value *metav1.Time) *time.Time {
	if value == nil || value.IsZero() {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func copyTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.UTC()
	return &result
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func firstStrings(values []string, limit int) []string {
	if len(values) <= limit {
		return values
	}
	return values[:limit]
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func addRecordIssue(record *reportv1alpha1.MigrationHistoryRecord, code reportv1alpha1.MigrationHistoryIssueCode) {
	for _, existing := range record.Issues {
		if existing == code {
			return
		}
	}
	record.Issues = append(record.Issues, code)
}

func addIssue(reportValue *reportv1alpha1.MigrationHistoryReport, issue reportv1alpha1.MigrationHistoryIssue) {
	for _, existing := range reportValue.Content.Issues {
		if existing == issue {
			return
		}
	}
	reportValue.Content.Issues = append(reportValue.Content.Issues, issue)
}

func addWarning(reportValue *reportv1alpha1.MigrationHistoryReport, code reportv1alpha1.MigrationHistoryWarningCode) {
	for _, existing := range reportValue.Warnings {
		if existing.Code == code {
			return
		}
	}
	reportValue.Warnings = append(reportValue.Warnings, reportv1alpha1.MigrationHistoryWarning{Code: code})
}

func markSourceTruncated(reportValue *reportv1alpha1.MigrationHistoryReport, evidence reportv1alpha1.MigrationHistoryEvidence) {
	for i := range reportValue.Sources {
		if reportValue.Sources[i].Evidence == evidence {
			reportValue.Sources[i].Truncated = true
		}
	}
}

func setSourceUnavailable(
	reportValue *reportv1alpha1.MigrationHistoryReport,
	index int,
	reason reportv1alpha1.MigrationHistoryUnavailableReason,
) {
	if index < 0 {
		return
	}
	reportValue.Sources[index].Availability = reportv1alpha1.MigrationHistoryAvailabilityUnavailable
	reportValue.Sources[index].UnavailableReason = reason
}

func sourceIndex(reportValue *reportv1alpha1.MigrationHistoryReport, evidence reportv1alpha1.MigrationHistoryEvidence) int {
	for i := range reportValue.Sources {
		if reportValue.Sources[i].Evidence == evidence {
			return i
		}
	}
	return -1
}

func hasTruncation(reportValue *reportv1alpha1.MigrationHistoryReport) bool {
	for _, source := range reportValue.Sources {
		if source.Truncated {
			return true
		}
	}
	for _, issue := range reportValue.Content.Issues {
		switch issue.Code {
		case reportv1alpha1.MigrationHistoryIssueAuthoritativeTruncated,
			reportv1alpha1.MigrationHistoryIssueParentTruncated,
			reportv1alpha1.MigrationHistoryIssueAuditTruncated,
			reportv1alpha1.MigrationHistoryIssueOutputTruncated:
			return true
		}
	}
	return false
}

func freshnessRank(value reportv1alpha1.MigrationHistoryFreshness) int {
	switch value {
	case reportv1alpha1.MigrationHistoryFreshnessCurrent:
		return 0
	case reportv1alpha1.MigrationHistoryFreshnessStale:
		return 1
	case reportv1alpha1.MigrationHistoryFreshnessUnobserved:
		return 2
	case reportv1alpha1.MigrationHistoryFreshnessInvalid:
		return 3
	default:
		return 4
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
