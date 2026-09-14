// Package migrationprojection converts a bounded InferenceReplica snapshot
// into the safe, versioned migration status report.
package migrationprojection

import (
	"errors"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/migrationcollection"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
)

var (
	ErrInferenceServiceRequired        = errors.New("inference service is required")
	ErrInferenceServiceIdentityInvalid = errors.New("inference service identity is invalid")
	ErrInvalidComponent                = errors.New("component must be engine, decoder, or router")
	ErrInvalidLimits                   = errors.New("projection limits must be positive")
)

// Limits bounds output as well as all migration-record and target-hint work.
type Limits struct {
	MaxRecords          int
	MaxScannedRecords   int
	MaxNodeHints        int
	MaxScannedNodeHints int
}

type sourceSnapshot struct {
	ir          *omev1beta1.InferenceReplica
	name        string
	component   reportv1alpha1.RuntimeComponentType
	freshness   reportv1alpha1.StatusFreshness
	bound       bool
	usable      bool
	duplicate   bool
	reportIndex int
}

type recordCandidate struct {
	record   reportv1alpha1.MigrationRecord
	safeID   string
	idUsable bool
}

// Project validates source identity before reading status and emits only
// allowlisted evidence and bounded, sanitized controller messages from current
// InferenceReplica records.
func Project(
	snapshot migrationcollection.Result,
	componentFilter string,
	limits Limits,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.MigrationStatusReport, error) {
	if snapshot.InferenceService == nil {
		return reportv1alpha1.MigrationStatusReport{}, ErrInferenceServiceRequired
	}
	if !validParentIdentity(snapshot.InferenceService) {
		return reportv1alpha1.MigrationStatusReport{}, ErrInferenceServiceIdentityInvalid
	}
	filter, filterSet := canonicalComponent(omev1beta1.ComponentType(componentFilter))
	if componentFilter != "" && !filterSet {
		return reportv1alpha1.MigrationStatusReport{}, ErrInvalidComponent
	}
	if limits.MaxRecords <= 0 || limits.MaxScannedRecords <= 0 || limits.MaxRecords > limits.MaxScannedRecords ||
		limits.MaxNodeHints <= 0 || limits.MaxScannedNodeHints <= 0 || limits.MaxNodeHints > limits.MaxScannedNodeHints {
		return reportv1alpha1.MigrationStatusReport{}, ErrInvalidLimits
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	now := clock.Now().UTC()
	parent := snapshot.InferenceService

	report := reportv1alpha1.NewMigrationStatusReport(
		reportv1alpha1.Metadata{Namespace: parent.Namespace, Name: parent.Name},
		reportv1alpha1.MigrationStatusContent{
			Migrations: []reportv1alpha1.MigrationRecord{},
			Issues:     []reportv1alpha1.MigrationIssue{},
		},
		reportv1alpha1.ClockFunc(func() time.Time { return now }),
	)
	report.Sources = append(report.Sources, reportv1alpha1.MigrationSourceReference{
		Kind: reportv1alpha1.MigrationSourceInferenceService, Namespace: parent.Namespace,
		Name: parent.Name, UID: string(parent.UID), Generation: parent.Generation,
		Freshness: reportv1alpha1.StatusFreshnessCurrent, CollectedAt: now,
	})

	sources := make([]sourceSnapshot, 0, len(snapshot.InferenceReplicas))
	componentCounts := map[reportv1alpha1.RuntimeComponentType]int{}
	requireRelationshipLabel := len(utilvalidation.IsValidLabelValue(parent.Name)) == 0
	for index := range snapshot.InferenceReplicas {
		ir := &snapshot.InferenceReplicas[index]
		component, componentValid := canonicalComponent(ir.Spec.Component)
		if filterSet && (!componentValid || component != filter) {
			continue
		}
		source := validateSource(parent, ir, requireRelationshipLabel, now, &report)
		if source.bound {
			componentCounts[source.component]++
		}
		sources = append(sources, source)
	}
	for index := range sources {
		source := &sources[index]
		if source.bound && componentCounts[source.component] > 1 {
			source.duplicate = true
			source.freshness = reportv1alpha1.StatusFreshnessInvalid
			report.Sources[source.reportIndex].Freshness = reportv1alpha1.StatusFreshnessInvalid
			addReportIssue(&report, reportv1alpha1.MigrationIssue{
				Code: reportv1alpha1.MigrationIssueSourceDuplicate, SourceName: source.name,
				Component: source.component,
			})
		}
	}

	recordCount := 0
	recordScanTruncated := false
	for _, source := range sources {
		if !source.usable {
			continue
		}
		if len(source.ir.Status.Migrations) > limits.MaxScannedRecords-recordCount {
			recordScanTruncated = true
			break
		}
		recordCount += len(source.ir.Status.Migrations)
	}
	candidates := make([]recordCandidate, 0, recordCount)
	requestCounts := make(map[string]int, recordCount)
	if recordScanTruncated {
		addReportIssue(&report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueRecordsTruncated})
		addWarning(&report, reportv1alpha1.MigrationWarningTruncated)
	} else {
		for _, source := range sources {
			if !source.usable {
				continue
			}
			for index := range source.ir.Status.Migrations {
				candidate := projectRecord(source, source.ir.Status.Migrations[index], limits.MaxNodeHints, limits.MaxScannedNodeHints)
				if source.duplicate {
					addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueSourceDuplicate)
				}
				if candidate.idUsable {
					requestCounts[candidate.safeID]++
				}
				candidates = append(candidates, candidate)
			}
		}
	}
	for index := range candidates {
		if candidates[index].idUsable && requestCounts[candidates[index].safeID] > 1 {
			addRecordIssue(&candidates[index].record, reportv1alpha1.MigrationIssueRequestDuplicate)
		}
		if recordHasInvalidIssue(candidates[index].record.Issues) {
			candidates[index].record.Classification = reportv1alpha1.MigrationClassificationInvalid
			candidates[index].record.Outcome = reportv1alpha1.MigrationOutcomeUnknown
		}
	}

	allRecords := make([]reportv1alpha1.MigrationRecord, len(candidates))
	for index := range candidates {
		allRecords[index] = candidates[index].record
	}
	allRecords = (reportv1alpha1.MigrationStatusReport{
		Content: reportv1alpha1.MigrationStatusContent{Migrations: allRecords},
	}).Canonical().Content.Migrations
	if len(allRecords) > limits.MaxRecords {
		allRecords = allRecords[:limits.MaxRecords]
		addReportIssue(&report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueRecordsTruncated})
		addWarning(&report, reportv1alpha1.MigrationWarningTruncated)
	}
	report.Content.Migrations = allRecords
	for _, record := range allRecords {
		for _, code := range record.Issues {
			addReportIssue(&report, reportv1alpha1.MigrationIssue{
				Code: code, SourceName: record.SourceName, RequestID: record.RequestID,
				Component: record.Component,
			})
		}
	}
	if snapshot.Completeness.Truncated {
		addReportIssue(&report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueSourcesTruncated})
		addWarning(&report, reportv1alpha1.MigrationWarningTruncated)
	}

	for _, record := range report.Content.Migrations {
		switch record.Classification {
		case reportv1alpha1.MigrationClassificationActive:
			report.Content.Summary.Active++
		case reportv1alpha1.MigrationClassificationTerminal:
			report.Content.Summary.Terminal++
		default:
			report.Content.Summary.Invalid++
		}
	}
	report.Content.Summary.Records = len(report.Content.Migrations)
	switch {
	case len(report.Content.Issues) > 0:
		report.Content.Summary.State = reportv1alpha1.MigrationReportStatePartial
	case len(report.Content.Migrations) == 0:
		report.Content.Summary.State = reportv1alpha1.MigrationReportStateEmpty
	default:
		report.Content.Summary.State = reportv1alpha1.MigrationReportStateReported
	}
	report.Content.Capacity = reportv1alpha1.MigrationCapacityEvidence{
		Evidence: reportv1alpha1.EvidenceComputed, ConcurrencyLimit: reportv1alpha1.EvidenceUnavailable,
		RateLimit: reportv1alpha1.EvidenceUnavailable,
	}
	if report.Content.Summary.State == reportv1alpha1.MigrationReportStatePartial {
		report.Content.Capacity.Evidence = reportv1alpha1.EvidenceUnavailable
		addWarning(&report, reportv1alpha1.MigrationWarningPartialData)
	} else {
		for _, record := range report.Content.Migrations {
			if record.Classification == reportv1alpha1.MigrationClassificationActive && record.SurgeInstance != nil && record.AllocatedAt != nil {
				report.Content.Capacity.ActiveAllocated++
			}
		}
	}
	return report.Canonical(), nil
}

func validParentIdentity(parent *omev1beta1.InferenceService) bool {
	return len(utilvalidation.IsDNS1123Label(parent.Namespace)) == 0 &&
		len(utilvalidation.IsDNS1123Subdomain(parent.Name)) == 0 &&
		safeUID(parent.UID) && parent.Generation > 0
}

func validateSource(
	parent *omev1beta1.InferenceService,
	ir *omev1beta1.InferenceReplica,
	requireRelationshipLabel bool,
	now time.Time,
	report *reportv1alpha1.MigrationStatusReport,
) sourceSnapshot {
	name := ir.Name
	identityValid := ir.Namespace == parent.Namespace && len(utilvalidation.IsDNS1123Subdomain(ir.Name)) == 0 && safeUID(ir.UID) && ir.Generation > 0
	if !identityValid {
		name = "INVALID"
	}
	component, componentValid := canonicalComponent(ir.Spec.Component)
	labelValid := ir.Labels[constants.InferenceServicePodLabelKey] == parent.Name
	parentValid := ir.Spec.ParentRef.Name == parent.Name
	ownerValid := hasExactControllerOwner(ir.OwnerReferences, parent)
	bound := identityValid && componentValid && (!requireRelationshipLabel || labelValid) && parentValid && ownerValid
	freshness := reportv1alpha1.StatusFreshnessInvalid
	observedGeneration := int64(0)
	statusUsable := false
	if bound {
		observedGeneration = ir.Status.ObservedGeneration
		switch {
		case observedGeneration == ir.Generation:
			freshness = reportv1alpha1.StatusFreshnessCurrent
			statusUsable = true
		case observedGeneration == 0:
			freshness = reportv1alpha1.StatusFreshnessUnobserved
		case observedGeneration > 0 && observedGeneration < ir.Generation:
			freshness = reportv1alpha1.StatusFreshnessStale
			statusUsable = true
		default:
			freshness = reportv1alpha1.StatusFreshnessInvalid
		}
	}
	reportIndex := len(report.Sources)
	report.Sources = append(report.Sources, reportv1alpha1.MigrationSourceReference{
		Kind: reportv1alpha1.MigrationSourceInferenceReplica, Namespace: parent.Namespace,
		Name: name, UID: safeUIDValue(ir.UID), Generation: ir.Generation,
		ObservedGeneration: observedGeneration, Freshness: freshness, CollectedAt: now,
	})

	source := sourceSnapshot{
		ir: ir, name: name, component: component, freshness: freshness,
		bound: bound, usable: statusUsable, reportIndex: reportIndex,
	}
	if !identityValid {
		addReportIssue(report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueSourceIdentityInvalid, SourceName: name})
	}
	if requireRelationshipLabel && !labelValid {
		addReportIssue(report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueSourceLabelMismatch, SourceName: name, Component: component})
	}
	if !parentValid {
		addReportIssue(report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueSourceParentMismatch, SourceName: name, Component: component})
	}
	if !ownerValid {
		addReportIssue(report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueSourceOwnerMismatch, SourceName: name, Component: component})
	}
	if !componentValid {
		addReportIssue(report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueSourceComponentInvalid, SourceName: name})
	}
	if bound && freshness == reportv1alpha1.StatusFreshnessStale {
		addReportIssue(report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueSourceStale, SourceName: name, Component: component})
	}
	if bound && freshness == reportv1alpha1.StatusFreshnessUnobserved {
		addReportIssue(report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueSourceUnobserved, SourceName: name, Component: component})
	}
	if bound && freshness == reportv1alpha1.StatusFreshnessInvalid {
		addReportIssue(report, reportv1alpha1.MigrationIssue{Code: reportv1alpha1.MigrationIssueSourceGenerationInvalid, SourceName: name, Component: component})
	}
	return source
}

func projectRecord(source sourceSnapshot, input omev1beta1.MigrationStatus, maxNodeHints, maxScannedNodeHints int) recordCandidate {
	record := reportv1alpha1.MigrationRecord{
		RequestID: input.RequestUUID, SourceName: source.name, Component: source.component,
		SourceInstance: input.SourceInstance, SurgeInstance: copyInt32(input.SurgeInstance),
		Freshness: source.freshness, Attempt: input.Attempt,
		RequestedAtEvidence: reportv1alpha1.EvidenceUnavailable,
		StartedAt:           timeFromMeta(input.StartedAt), AllocatedAt: timeFromMetaPointer(input.AllocatedAt),
		Deadline: timeFromMeta(input.Deadline), CompletedAt: timeFromMetaPointer(input.CompletedAt),
		TargetNodeHints: []string{}, Issues: []reportv1alpha1.MigrationIssueCode{},
	}
	candidate := recordCandidate{record: record, safeID: input.RequestUUID, idUsable: safeRequestID(input.RequestUUID)}
	if !candidate.idUsable {
		candidate.record.RequestID = "INVALID"
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueRequestInvalid)
	}

	candidate.record.Trigger, _ = canonicalTrigger(input.Trigger)
	if candidate.record.Trigger == reportv1alpha1.MigrationTriggerUnknown {
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueTriggerInvalid)
	}
	candidate.record.Phase, candidate.record.Classification = canonicalPhase(input.Phase)
	if candidate.record.Phase == reportv1alpha1.MigrationPhaseUnknown {
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssuePhaseInvalid)
	}
	if !triggerAllowsPhase(candidate.record.Trigger, candidate.record.Phase) {
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueTriggerPhaseConflict)
	}
	if input.SourceInstance < 0 {
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueSourceIndexInvalid)
	}
	if input.SurgeInstance != nil && (*input.SurgeInstance < 0 || *input.SurgeInstance == input.SourceInstance) {
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueSurgeIndexInvalid)
	}
	validateAllocationShape(&candidate.record)
	validateTerminalShape(&candidate.record)
	validateTimestamps(&candidate.record)
	if input.Trigger == omev1beta1.MigrationTriggerAuto {
		if input.Attempt <= 0 {
			addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueAttemptInvalid)
		}
	} else if input.Attempt != 0 {
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueAttemptInvalid)
	}
	if input.Succeeded != nil && (input.Trigger != omev1beta1.MigrationTriggerAuto || input.Phase != omev1beta1.MigrationPhaseRelocated || !*input.Succeeded) {
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueSucceededInvalid)
	}

	if input.FromNode != "" {
		if safeNodeName(input.FromNode) {
			candidate.record.FromNode = input.FromNode
		} else {
			addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueNodeHintInvalid)
		}
	}
	validHints := make([]string, 0, min(len(input.HintTargetNodes), maxNodeHints))
	if len(input.HintTargetNodes) > maxScannedNodeHints {
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueNodeHintsTruncated)
	} else {
		for _, hint := range input.HintTargetNodes {
			if !safeNodeName(hint) {
				addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueNodeHintInvalid)
				continue
			}
			validHints = append(validHints, hint)
		}
		sort.Strings(validHints)
		validHints = compactStrings(validHints)
		if len(validHints) > maxNodeHints {
			validHints = validHints[:maxNodeHints]
			addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueNodeHintsTruncated)
		}
	}
	candidate.record.TargetNodeHints = validHints
	candidate.record.ReasonEvidence = reasonEvidence(candidate.record.Trigger, input.Reason)
	if input.Message == "" {
		candidate.record.MessageEvidence = reportv1alpha1.MigrationMessageAbsent
	} else {
		candidate.record.Message = printers.BoundedCell(input.Message, reportv1alpha1.MigrationMessageMaxDisplayWidth)
		candidate.record.MessageEvidence = reportv1alpha1.MigrationMessagePresent
	}
	candidate.record.Outcome = migrationOutcome(candidate.record.Phase, input.Succeeded)
	if source.freshness == reportv1alpha1.StatusFreshnessStale {
		addRecordIssue(&candidate.record, reportv1alpha1.MigrationIssueSourceStale)
	}
	if recordHasInvalidIssue(candidate.record.Issues) {
		candidate.record.Classification = reportv1alpha1.MigrationClassificationInvalid
		candidate.record.Outcome = reportv1alpha1.MigrationOutcomeUnknown
	}
	return candidate
}

func canonicalComponent(value omev1beta1.ComponentType) (reportv1alpha1.RuntimeComponentType, bool) {
	switch value {
	case omev1beta1.EngineComponent:
		return reportv1alpha1.RuntimeComponentEngine, true
	case omev1beta1.DecoderComponent:
		return reportv1alpha1.RuntimeComponentDecoder, true
	case omev1beta1.RouterComponent:
		return reportv1alpha1.RuntimeComponentRouter, true
	default:
		return "", false
	}
}

func canonicalTrigger(value omev1beta1.MigrationTrigger) (reportv1alpha1.MigrationTrigger, bool) {
	switch value {
	case omev1beta1.MigrationTriggerManual:
		return reportv1alpha1.MigrationTriggerManual, true
	case omev1beta1.MigrationTriggerAuto:
		return reportv1alpha1.MigrationTriggerAuto, true
	default:
		return reportv1alpha1.MigrationTriggerUnknown, false
	}
}

func canonicalPhase(value omev1beta1.MigrationPhase) (reportv1alpha1.MigrationPhase, reportv1alpha1.MigrationClassification) {
	switch value {
	case omev1beta1.MigrationPhaseAccepted:
		return reportv1alpha1.MigrationPhaseAccepted, reportv1alpha1.MigrationClassificationActive
	case omev1beta1.MigrationPhaseSurgePending:
		return reportv1alpha1.MigrationPhaseSurgePending, reportv1alpha1.MigrationClassificationActive
	case omev1beta1.MigrationPhaseSurgeReady:
		return reportv1alpha1.MigrationPhaseSurgeReady, reportv1alpha1.MigrationClassificationActive
	case omev1beta1.MigrationPhaseDraining:
		return reportv1alpha1.MigrationPhaseDraining, reportv1alpha1.MigrationClassificationActive
	case omev1beta1.MigrationPhaseCompleted:
		return reportv1alpha1.MigrationPhaseCompleted, reportv1alpha1.MigrationClassificationTerminal
	case omev1beta1.MigrationPhaseFailed:
		return reportv1alpha1.MigrationPhaseFailed, reportv1alpha1.MigrationClassificationTerminal
	case omev1beta1.MigrationPhaseRelocated:
		return reportv1alpha1.MigrationPhaseRelocated, reportv1alpha1.MigrationClassificationTerminal
	default:
		return reportv1alpha1.MigrationPhaseUnknown, reportv1alpha1.MigrationClassificationInvalid
	}
}

func triggerAllowsPhase(trigger reportv1alpha1.MigrationTrigger, phase reportv1alpha1.MigrationPhase) bool {
	if trigger == reportv1alpha1.MigrationTriggerManual {
		return phase != reportv1alpha1.MigrationPhaseUnknown && phase != reportv1alpha1.MigrationPhaseRelocated
	}
	return trigger == reportv1alpha1.MigrationTriggerAuto && phase == reportv1alpha1.MigrationPhaseRelocated
}

func validateAllocationShape(record *reportv1alpha1.MigrationRecord) {
	hasSurge := record.SurgeInstance != nil
	hasAllocatedAt := record.AllocatedAt != nil
	invalid := hasSurge != hasAllocatedAt
	switch record.Phase {
	case reportv1alpha1.MigrationPhaseAccepted:
		invalid = invalid || hasSurge || hasAllocatedAt
	case reportv1alpha1.MigrationPhaseSurgePending, reportv1alpha1.MigrationPhaseSurgeReady,
		reportv1alpha1.MigrationPhaseDraining, reportv1alpha1.MigrationPhaseCompleted:
		invalid = invalid || !hasSurge || !hasAllocatedAt
	case reportv1alpha1.MigrationPhaseRelocated:
		invalid = invalid || hasSurge || hasAllocatedAt
	}
	if invalid {
		addRecordIssue(record, reportv1alpha1.MigrationIssueAllocationInvalid)
	}
}

func validateTerminalShape(record *reportv1alpha1.MigrationRecord) {
	terminal := record.Classification == reportv1alpha1.MigrationClassificationTerminal
	if terminal != (record.CompletedAt != nil) {
		addRecordIssue(record, reportv1alpha1.MigrationIssueTerminalShapeInvalid)
	}
}

func validateTimestamps(record *reportv1alpha1.MigrationRecord) {
	invalid := record.StartedAt == nil || record.Deadline == nil
	if record.StartedAt != nil && record.Deadline != nil && record.Deadline.Before(*record.StartedAt) {
		invalid = true
	}
	if record.AllocatedAt != nil && record.StartedAt != nil && record.AllocatedAt.Before(*record.StartedAt) {
		invalid = true
	}
	if record.CompletedAt != nil {
		if record.StartedAt != nil && record.CompletedAt.Before(*record.StartedAt) {
			invalid = true
		}
		if record.AllocatedAt != nil && record.CompletedAt.Before(*record.AllocatedAt) {
			invalid = true
		}
	}
	if invalid {
		addRecordIssue(record, reportv1alpha1.MigrationIssueTimestampInvalid)
	}
}

func reasonEvidence(trigger reportv1alpha1.MigrationTrigger, reason string) reportv1alpha1.MigrationReasonEvidence {
	if reason == "" {
		return reportv1alpha1.MigrationReasonUnavailable
	}
	if trigger == reportv1alpha1.MigrationTriggerManual {
		return reportv1alpha1.MigrationReasonOperatorSupplied
	}
	if trigger == reportv1alpha1.MigrationTriggerAuto && reason == "AutoRecover" {
		return reportv1alpha1.MigrationReasonAutoRecover
	}
	return reportv1alpha1.MigrationReasonController
}

func migrationOutcome(phase reportv1alpha1.MigrationPhase, succeeded *bool) reportv1alpha1.MigrationOutcome {
	switch phase {
	case reportv1alpha1.MigrationPhaseAccepted, reportv1alpha1.MigrationPhaseSurgePending,
		reportv1alpha1.MigrationPhaseSurgeReady, reportv1alpha1.MigrationPhaseDraining:
		return reportv1alpha1.MigrationOutcomeInProgress
	case reportv1alpha1.MigrationPhaseCompleted:
		return reportv1alpha1.MigrationOutcomeCompleted
	case reportv1alpha1.MigrationPhaseFailed:
		return reportv1alpha1.MigrationOutcomeFailed
	case reportv1alpha1.MigrationPhaseRelocated:
		if succeeded != nil && *succeeded {
			return reportv1alpha1.MigrationOutcomeRelocationConfirmed
		}
		return reportv1alpha1.MigrationOutcomeRelocated
	default:
		return reportv1alpha1.MigrationOutcomeUnknown
	}
}

func hasExactControllerOwner(references []metav1.OwnerReference, parent *omev1beta1.InferenceService) bool {
	controllers := 0
	matched := false
	for _, reference := range references {
		if reference.Controller == nil || !*reference.Controller {
			continue
		}
		controllers++
		matched = reference.APIVersion == omev1beta1.SchemeGroupVersion.String() &&
			reference.Kind == "InferenceService" && reference.Name == parent.Name && reference.UID == parent.UID
	}
	return controllers == 1 && matched
}

func safeUID(uid types.UID) bool {
	value := string(uid)
	if value == "" || len(value) > 128 {
		return false
	}
	for index := range len(value) {
		char := value[index]
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' || char == '.') {
			return false
		}
	}
	return true
}

func safeUIDValue(uid types.UID) string {
	if !safeUID(uid) {
		return ""
	}
	return string(uid)
}

func safeRequestID(value string) bool {
	return value != "" && len(value) <= 63 && !strings.Contains(value, "/") && len(utilvalidation.IsQualifiedName(value)) == 0
}

func safeNodeName(value string) bool {
	return value != "" && len(value) <= 253 && len(utilvalidation.IsDNS1123Subdomain(value)) == 0
}

func timeFromMeta(value metav1.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func timeFromMetaPointer(value *metav1.Time) *time.Time {
	if value == nil || value.IsZero() {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	write := 1
	for read := 1; read < len(values); read++ {
		if values[read] == values[write-1] {
			continue
		}
		values[write] = values[read]
		write++
	}
	return values[:write]
}

func addRecordIssue(record *reportv1alpha1.MigrationRecord, code reportv1alpha1.MigrationIssueCode) {
	for _, existing := range record.Issues {
		if existing == code {
			return
		}
	}
	record.Issues = append(record.Issues, code)
}

func addReportIssue(report *reportv1alpha1.MigrationStatusReport, issue reportv1alpha1.MigrationIssue) {
	for _, existing := range report.Content.Issues {
		if existing == issue {
			return
		}
	}
	report.Content.Issues = append(report.Content.Issues, issue)
}

func addWarning(report *reportv1alpha1.MigrationStatusReport, code reportv1alpha1.MigrationWarningCode) {
	for _, warning := range report.Warnings {
		if warning.Code == code {
			return
		}
	}
	report.Warnings = append(report.Warnings, reportv1alpha1.MigrationWarning{Code: code})
}

func recordHasInvalidIssue(issues []reportv1alpha1.MigrationIssueCode) bool {
	for _, issue := range issues {
		switch issue {
		case reportv1alpha1.MigrationIssueSourceStale,
			reportv1alpha1.MigrationIssueNodeHintInvalid,
			reportv1alpha1.MigrationIssueNodeHintsTruncated:
			continue
		default:
			return true
		}
	}
	return false
}
