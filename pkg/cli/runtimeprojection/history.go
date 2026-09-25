package runtimeprojection

import (
	"errors"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

// ProjectHistory projects bounded runtime revision evidence into the safe,
// versioned history report contract.
func ProjectHistory(
	isvc *v1beta1.InferenceService,
	state *effective.RuntimeState,
	clock reportv1alpha1.Clock,
) (reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent], error) {
	if !validInputs(isvc, state) {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent]{}, ErrInvalidEvidence
	}
	live := state.LiveConfiguration()
	if !validStateEvidence(state, live) {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent]{}, ErrInvalidEvidence
	}
	notConfigured := noRuntimeConfigurationDeclared(isvc)
	_, runtimeIdentity, ok := projectInheritance(state, live, notConfigured)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent]{}, ErrInvalidEvidence
	}
	if _, valid := projectLiveConfiguration(state, live, runtimeIdentity, notConfigured); !valid {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent]{}, ErrInvalidEvidence
	}
	activeConfiguration, valid := projectActiveConfiguration(state, runtimeIdentity, notConfigured)
	if !valid || !validPinConfiguration(state, activeConfiguration) {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent]{}, ErrInvalidEvidence
	}
	observation, completeness, ok := projectHistoryCollectionState(state)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent]{}, ErrInvalidEvidence
	}

	revisions := state.RevisionObservations()
	entries := make([]reportv1alpha1.RuntimeRevisionEntry, 0, len(revisions))
	for i := range revisions {
		if !revisions[i].ObjectReturned() {
			continue
		}
		entry, valid := projectRevisionEntry(revisions[i])
		if !valid {
			return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent]{}, ErrInvalidEvidence
		}
		entries = append(entries, entry)
	}

	issues, sources, warnings, ok := projectHistoryDiagnostics(isvc, state, live, revisions, notConfigured)
	if !ok {
		return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent]{}, ErrInvalidEvidence
	}
	content := reportv1alpha1.RuntimeHistoryContent{
		Runtime:        copyRuntimeReference(runtimeIdentity),
		Observation:    observation,
		Completeness:   completeness,
		RequestedPages: state.HistoryRequestedPages,
		ObservedPages:  state.HistoryObservedPages,
		Revisions:      entries,
		Issues:         issues,
	}
	reportValue := reportv1alpha1.NewRuntimeHistoryReport(
		reportv1alpha1.Metadata{Namespace: isvc.Namespace, Name: isvc.Name}, content, clock,
	)
	reportValue.Sources = sources
	reportValue.Warnings = warnings
	return reportValue.Canonical(), nil
}

func projectHistoryCollectionState(
	state *effective.RuntimeState,
) (reportv1alpha1.HistoryObservationState, reportv1alpha1.HistoryCompleteness, bool) {
	if !validHistoryCollectionEvidence(state) {
		return "", "", false
	}
	if !state.HistoryRequested {
		return reportv1alpha1.HistoryObservationStateNotRequested,
			reportv1alpha1.HistoryCompletenessNotRequested, true
	}
	if state.HistoryComplete {
		return reportv1alpha1.HistoryObservationStateComplete,
			reportv1alpha1.HistoryCompletenessRetentionBounded, true
	}
	if state.HistoryObservedPages > 0 || state.HistoryTruncated {
		return reportv1alpha1.HistoryObservationStatePartial,
			reportv1alpha1.HistoryCompletenessIncomplete, true
	}
	return reportv1alpha1.HistoryObservationStateUnavailable,
		reportv1alpha1.HistoryCompletenessIncomplete, true
}

func validHistoryCollectionEvidence(state *effective.RuntimeState) bool {
	maxInt := int(^uint(0) >> 1)
	if state == nil || state.HistoryPageLimit <= 0 || state.HistoryPageLimit > maxInt/2 ||
		state.HistoryRequestLimit < state.HistoryPageLimit || state.HistoryPages < 0 ||
		state.HistoryRequestLimit > 2*state.HistoryPageLimit ||
		state.HistoryRequestedPages < 0 || state.HistoryObservedPages < 0 ||
		state.HistoryPages != state.HistoryRequestedPages ||
		state.HistoryRequestedPages > state.HistoryRequestLimit ||
		state.HistoryObservedPages > state.HistoryPageLimit ||
		state.HistoryObservedPages > state.HistoryRequestedPages {
		return false
	}

	listFailures := 0
	for _, issue := range state.SourceIssues() {
		if issue.Code != effective.RuntimeSourceIssueRevisionListFailed {
			continue
		}
		if issue.RevisionName != "" {
			return false
		}
		listFailures++
	}
	if listFailures > 1 {
		return false
	}

	if !state.HistoryRequested {
		return !state.HistoryComplete && !state.HistoryTruncated &&
			state.HistoryPages == 0 && state.HistoryNamespace() == "" && listFailures == 0
	}
	if state.RuntimeName == "" {
		return !state.HistoryComplete && !state.HistoryTruncated &&
			state.HistoryPages == 0 && state.HistoryNamespace() == "" && listFailures == 0
	}
	if state.HistoryNamespace() == "" || state.HistoryRequestedPages == 0 {
		return false
	}
	if state.HistoryComplete {
		return !state.HistoryTruncated &&
			validSuccessfulHistoryPageCounts(state) && listFailures == 0
	}
	if state.HistoryTruncated {
		return validSuccessfulHistoryPageCounts(state) && listFailures == 0
	}
	if listFailures != 1 {
		return false
	}
	gap := state.HistoryRequestedPages - state.HistoryObservedPages
	// A validation failure can follow a successful response (gap 0), and an
	// ordinary request failure contributes one unobserved request (gap 1).
	// After one recovered expiration, the discarded successful prefix plus the
	// expired continuation account for at least two additional requests.
	return (gap == 0 && state.HistoryObservedPages > 0) ||
		(gap == 1 && state.HistoryObservedPages < state.HistoryPageLimit) ||
		(gap >= 2 && gap <= state.HistoryPageLimit+1)
}

func validSuccessfulHistoryPageCounts(state *effective.RuntimeState) bool {
	if state.HistoryObservedPages == 0 {
		return false
	}
	gap := state.HistoryRequestedPages - state.HistoryObservedPages
	// A successful non-restarted collection has no gap. A recovered expired
	// continuation has at least one discarded observed page plus the failed 410
	// request, so the only other valid gap is two or greater.
	return gap == 0 || gap >= 2 && gap <= state.HistoryPageLimit
}

func projectRevisionEntry(
	observation effective.RuntimeRevisionObservation,
) (reportv1alpha1.RuntimeRevisionEntry, bool) {
	if !observation.ObjectReturned() || !validReturnedRevisionIdentity(observation) {
		return reportv1alpha1.RuntimeRevisionEntry{}, false
	}
	if !validVerifiedShortHash(observation.ShortHash) {
		return reportv1alpha1.RuntimeRevisionEntry{}, false
	}
	roles := observation.Roles()
	projectedRoles := make([]reportv1alpha1.RuntimeRevisionRole, len(roles))
	for i := range roles {
		role, ok := mapRevisionRole(roles[i])
		if !ok {
			return reportv1alpha1.RuntimeRevisionEntry{}, false
		}
		projectedRoles[i] = role
	}
	consistency, ok := mapRevisionConsistency(observation.Consistency)
	if !ok {
		return reportv1alpha1.RuntimeRevisionEntry{}, false
	}
	relation, ok := mapRevisionRelation(observation.RelationToLive)
	if !ok {
		return reportv1alpha1.RuntimeRevisionEntry{}, false
	}
	issues, ok := projectConsistencyIssues(observation.ConsistencyCodes(), observation.Disabled)
	if !ok {
		return reportv1alpha1.RuntimeRevisionEntry{}, false
	}
	var createdAt *time.Time
	if !observation.CreationTimestamp.IsZero() {
		utc := observation.CreationTimestamp.Time.UTC()
		createdAt = &utc
	}
	var source *reportv1alpha1.RuntimeObjectReference
	if observation.SourceName != "" {
		kind, known := mapRuntimeKind(observation.SourceKind)
		if !known {
			kind = reportv1alpha1.RuntimeKindUnknown
		}
		source = &reportv1alpha1.RuntimeObjectReference{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       kind,
			Namespace:  observation.SourceNamespace,
			Name:       observation.SourceName,
		}
	}
	return reportv1alpha1.RuntimeRevisionEntry{
		Revision: reportv1alpha1.RuntimeRevisionReference{
			Namespace: observation.ReturnedNamespace(),
			Name:      observation.ReturnedName(),
			UID:       observation.UID,
			CreatedAt: createdAt,
		},
		Source:         source,
		Hash:           observation.ShortHash,
		Roles:          projectedRoles,
		Consistency:    consistency,
		RelationToLive: relation,
		Issues:         issues,
	}, true
}

func validReturnedRevisionIdentity(observation effective.RuntimeRevisionObservation) bool {
	return observation.ReturnedName() != "" && observation.ReturnedNamespace() != ""
}

func mapRevisionRole(value effective.RuntimeRevisionRole) (reportv1alpha1.RuntimeRevisionRole, bool) {
	switch value {
	case effective.RuntimeRevisionRoleRequested:
		return reportv1alpha1.RuntimeRevisionRoleRequested, true
	case effective.RuntimeRevisionRoleReported:
		return reportv1alpha1.RuntimeRevisionRoleReported, true
	case effective.RuntimeRevisionRoleActive:
		return reportv1alpha1.RuntimeRevisionRoleActive, true
	case effective.RuntimeRevisionRoleHistory:
		return reportv1alpha1.RuntimeRevisionRoleHistory, true
	default:
		return "", false
	}
}

func mapRevisionConsistency(value effective.RevisionConsistencyState) (reportv1alpha1.RevisionConsistency, bool) {
	switch value {
	case effective.RevisionConsistencyConsistent:
		return reportv1alpha1.RevisionConsistencyConsistent, true
	case effective.RevisionConsistencyInconsistent:
		return reportv1alpha1.RevisionConsistencyInconsistent, true
	case effective.RevisionConsistencyUnknown:
		return reportv1alpha1.RevisionConsistencyUnknown, true
	default:
		return "", false
	}
}

func mapRevisionRelation(value effective.RuntimeHashRelation) (reportv1alpha1.RevisionRelation, bool) {
	switch value {
	case effective.RuntimeHashRelationUnknown:
		return reportv1alpha1.RevisionRelationUnknown, true
	case effective.RuntimeHashRelationEqual:
		return reportv1alpha1.RevisionRelationMatchesLive, true
	case effective.RuntimeHashRelationDifferent:
		return reportv1alpha1.RevisionRelationDiffersFromLive, true
	case effective.RuntimeHashRelationAmbiguous:
		return reportv1alpha1.RevisionRelationAmbiguous, true
	default:
		return "", false
	}
}

func projectConsistencyIssues(
	codes []effective.RevisionConsistencyCode,
	disabled bool,
) ([]reportv1alpha1.RuntimeIssueCode, bool) {
	issues := make([]reportv1alpha1.RuntimeIssueCode, 0, len(codes)+1)
	for _, code := range codes {
		issue, ok := mapConsistencyIssue(code)
		if !ok {
			return nil, false
		}
		issues = append(issues, issue)
	}
	if disabled {
		issues = append(issues, reportv1alpha1.RuntimeIssueRevisionDisabled)
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i] < issues[j] })
	issues = deduplicateIssueCodes(issues)
	return issues, true
}

func mapConsistencyIssue(value effective.RevisionConsistencyCode) (reportv1alpha1.RuntimeIssueCode, bool) {
	switch value {
	case effective.RevisionConsistencyCreatedBy:
		return reportv1alpha1.RuntimeIssueRevisionNotOMEManaged, true
	case effective.RevisionConsistencySourceName,
		effective.RevisionConsistencySourceKind,
		effective.RevisionConsistencySourceNamespace:
		return reportv1alpha1.RuntimeIssueRevisionSourceMismatch, true
	case effective.RevisionConsistencyHashLabelInvalid:
		return reportv1alpha1.RuntimeIssueRevisionHashInvalid, true
	case effective.RevisionConsistencyHashLabelMismatch:
		return reportv1alpha1.RuntimeIssueRevisionHashMismatch, true
	case effective.RevisionConsistencyNameHash:
		return reportv1alpha1.RuntimeIssueRevisionNameMismatch, true
	case effective.RevisionConsistencyOrdinal:
		return reportv1alpha1.RuntimeIssueRevisionOrdinalUnexpected, true
	case effective.RevisionConsistencyPayloadCanonicality:
		return reportv1alpha1.RuntimeIssueRevisionPayloadNonCanonical, true
	case effective.RevisionConsistencyUnexpectedDataObject:
		return reportv1alpha1.RuntimeIssueRevisionDataObjectPresent, true
	case effective.RevisionConsistencyMalformedPayload:
		return reportv1alpha1.RuntimeIssueRevisionPayloadMalformed, true
	case effective.RevisionConsistencyReturnedIdentity:
		return reportv1alpha1.RuntimeIssueRevisionIdentityMismatch, true
	case effective.RevisionConsistencyDuplicateIdentity:
		return reportv1alpha1.RuntimeIssueDuplicateRevision, true
	case effective.RevisionConsistencyConflictingIdentity:
		return reportv1alpha1.RuntimeIssueConflictingRevision, true
	case effective.RevisionConsistencyDuplicateContentHash:
		return reportv1alpha1.RuntimeIssueDuplicateRevisionContent, true
	case effective.RevisionConsistencyShortHashCollision:
		return reportv1alpha1.RuntimeIssueRevisionHashCollision, true
	default:
		return "", false
	}
}

func projectHistoryDiagnostics(
	isvc *v1beta1.InferenceService,
	state *effective.RuntimeState,
	live *effective.LiveConfiguration,
	observations []effective.RuntimeRevisionObservation,
	notConfigured bool,
) ([]reportv1alpha1.RuntimeIssue, []reportv1alpha1.RuntimeSourceReference, []reportv1alpha1.RuntimeWarning, bool) {
	issues := []reportv1alpha1.RuntimeIssue{}
	sources := projectBaseSources(isvc, live)
	warnings := []reportv1alpha1.RuntimeWarning{}
	invalidDeclaredKind := invalidDeclaredRuntimeKind(isvc)
	if invalidDeclaredKind {
		issues = append(issues, reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueInvalidDeclaredKind})
	}
	switch state.StatusFreshness {
	case effective.StatusFreshnessCurrent:
	case effective.StatusFreshnessUnknown:
		issues = append(issues, reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueStatusUnobserved})
		warnings = append(warnings, reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningPartialData})
	case effective.StatusFreshnessStale:
		issues = append(issues, reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueStatusStale})
		warnings = append(warnings,
			reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningPartialData},
			reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningStaleEvidence},
		)
	case effective.StatusFreshnessInconsistent:
		issues = append(issues, reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueStatusInvalid})
		warnings = append(warnings,
			reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningPartialData},
			reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningStaleEvidence},
		)
	default:
		return nil, nil, nil, false
	}
	if state.DriftState == effective.RuntimeDriftStateMalformed {
		issues = append(issues, reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueReportedDriftConflict})
	}
	if live == nil || live.Runtime.DeclaredInheritance.State() == effective.InheritanceUnavailable {
		issues = append(issues, reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueInheritanceUnavailable})
		if live != nil {
			warnings = append(warnings,
				reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningPartialData},
				reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningSourceUnavailable},
			)
		}
	}
	if live != nil {
		for _, advisory := range live.Advisories {
			switch advisory.Code {
			case effective.RuntimeAdvisoryDeclaredCompatibilityMismatch:
				issues = append(issues, reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueDeclaredCompatibilityMismatch})
			case effective.RuntimeAdvisoryInvalidDeclaredKind:
				if !invalidDeclaredKind {
					return nil, nil, nil, false
				}
			default:
				return nil, nil, nil, false
			}
		}
	}

	for _, observation := range observations {
		if observation.ObjectReturned() {
			sources = append(sources, reportv1alpha1.RuntimeSourceReference{
				Kind: "ControllerRevision", Namespace: observation.ReturnedNamespace(), Name: observation.ReturnedName(),
				UID: observation.UID, Evidence: reportv1alpha1.EvidenceObserved,
			})
			continue
		}
		reason, found := failedRevisionReason(state.SourceIssues(), observation.ExpectedName())
		if found {
			sources = append(sources, reportv1alpha1.RuntimeSourceReference{
				Kind: "ControllerRevision", Namespace: observation.ExpectedNamespace(), Name: observation.ExpectedName(),
				Evidence: reportv1alpha1.EvidenceUnavailable, UnavailableReason: reason,
			})
		}
	}

	for _, sourceIssue := range state.SourceIssues() {
		if notConfigured && liveSourceIssue(sourceIssue.Code) {
			continue
		}
		issue, reason, valid := projectSourceIssue(sourceIssue)
		if !valid {
			return nil, nil, nil, false
		}
		issues = append(issues, issue)
		warnings = append(warnings,
			reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningPartialData},
			reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningSourceUnavailable},
		)
		if sourceIssue.Code == effective.RuntimeSourceIssueRevisionListFailed {
			sources = append(sources, reportv1alpha1.RuntimeSourceReference{
				Kind: "ControllerRevisionList", Namespace: state.HistoryNamespace(), Name: state.RuntimeName,
				Evidence: reportv1alpha1.EvidenceUnavailable, UnavailableReason: reason,
			})
		}
	}
	if live == nil && state.RuntimeName != "" {
		source, valid := unavailableLiveSource(state)
		if !valid {
			if !invalidDeclaredKind {
				return nil, nil, nil, false
			}
		} else {
			sources = append(sources, source)
		}
	}
	if state.HistoryRequested && !state.HistoryComplete && !state.HistoryTruncated {
		issues = append(issues, reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueHistoryUnavailable})
		warnings = append(warnings, reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningPartialData})
	}
	if state.HistoryTruncated {
		issues = append(issues, reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueHistoryTruncated})
		warnings = append(warnings,
			reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningPartialData},
			reportv1alpha1.RuntimeWarning{Code: reportv1alpha1.WarningTruncated},
		)
	}
	issues = deduplicateIssues(issues)
	warnings = deduplicateWarnings(warnings)
	return issues, deduplicateSources(sources), warnings, true
}

func projectSourceIssue(
	issue effective.RuntimeSourceIssue,
) (reportv1alpha1.RuntimeIssue, reportv1alpha1.UnavailableReason, bool) {
	switch issue.Code {
	case effective.RuntimeSourceIssueLiveNotFound:
		return reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueLiveRuntimeUnavailable},
			reportv1alpha1.UnavailableNotFound, true
	case effective.RuntimeSourceIssueLiveDisabled:
		return reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueLiveRuntimeUnavailable},
			reportv1alpha1.UnavailableDisabled, true
	case effective.RuntimeSourceIssueLiveUnavailable:
		return reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueLiveRuntimeUnavailable},
			classifySourceError(errors.Unwrap(issue)), true
	case effective.RuntimeSourceIssueRevisionNotFound:
		return reportv1alpha1.RuntimeIssue{
			Code: reportv1alpha1.RuntimeIssueRevisionNotFound, Revision: issue.RevisionName,
		}, reportv1alpha1.UnavailableNotFound, true
	case effective.RuntimeSourceIssueRevisionGetFailed:
		return reportv1alpha1.RuntimeIssue{
			Code: reportv1alpha1.RuntimeIssueRevisionUnavailable, Revision: issue.RevisionName,
		}, classifySourceError(errors.Unwrap(issue)), true
	case effective.RuntimeSourceIssueRevisionListFailed:
		return reportv1alpha1.RuntimeIssue{Code: reportv1alpha1.RuntimeIssueHistoryUnavailable},
			classifySourceError(errors.Unwrap(issue)), true
	default:
		return reportv1alpha1.RuntimeIssue{}, "", false
	}
}

func classifySourceError(err error) reportv1alpha1.UnavailableReason {
	switch {
	case apierrors.IsNotFound(err):
		return reportv1alpha1.UnavailableNotFound
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		return reportv1alpha1.UnavailableForbidden
	case meta.IsNoMatchError(err):
		return reportv1alpha1.UnavailableUnsupportedAPI
	default:
		return reportv1alpha1.UnavailableUnreadable
	}
}

func failedRevisionReason(
	issues []effective.RuntimeSourceIssue,
	revisionName string,
) (reportv1alpha1.UnavailableReason, bool) {
	for _, issue := range issues {
		if issue.RevisionName != revisionName {
			continue
		}
		_, reason, ok := projectSourceIssue(issue)
		return reason, ok
	}
	return "", false
}

func deduplicateIssueCodes(values []reportv1alpha1.RuntimeIssueCode) []reportv1alpha1.RuntimeIssueCode {
	result := make([]reportv1alpha1.RuntimeIssueCode, 0, len(values))
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func deduplicateIssues(values []reportv1alpha1.RuntimeIssue) []reportv1alpha1.RuntimeIssue {
	sort.Slice(values, func(i, j int) bool {
		if values[i].Code != values[j].Code {
			return values[i].Code < values[j].Code
		}
		return values[i].Revision < values[j].Revision
	})
	result := make([]reportv1alpha1.RuntimeIssue, 0, len(values))
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}

func deduplicateWarnings(values []reportv1alpha1.RuntimeWarning) []reportv1alpha1.RuntimeWarning {
	sort.Slice(values, func(i, j int) bool { return values[i].Code < values[j].Code })
	result := make([]reportv1alpha1.RuntimeWarning, 0, len(values))
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}
