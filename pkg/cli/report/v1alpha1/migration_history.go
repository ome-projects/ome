package v1alpha1

import (
	"cmp"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const (
	// MigrationHistoryReportKind identifies the migration history schema.
	MigrationHistoryReportKind = "MigrationHistoryReport"
	// MigrationHistoryMessageMaxDisplayWidth bounds allowlisted operational
	// summaries in every output format.
	MigrationHistoryMessageMaxDisplayWidth = 256
)

// MigrationHistoryEvidence separates authoritative work state from bounded
// historical evidence.
type MigrationHistoryEvidence string

const (
	MigrationHistoryEvidenceUnknown       MigrationHistoryEvidence = "Unknown"
	MigrationHistoryEvidenceAuthoritative MigrationHistoryEvidence = "Authoritative"
	MigrationHistoryEvidenceParent        MigrationHistoryEvidence = "ParentSummary"
	MigrationHistoryEvidenceAudit         MigrationHistoryEvidence = "AuditHistory"
	MigrationHistoryEvidenceReport        MigrationHistoryEvidence = "Report"
)

// MigrationHistorySourceKind is the closed set of source object kinds.
type MigrationHistorySourceKind string

const (
	MigrationHistorySourceUnknown          MigrationHistorySourceKind = "Unknown"
	MigrationHistorySourceInferenceReplica MigrationHistorySourceKind = "InferenceReplicaStatus"
	MigrationHistorySourceInferenceService MigrationHistorySourceKind = "InferenceServiceHistory"
	MigrationHistorySourceConfigMap        MigrationHistorySourceKind = "ConfigMap"
)

// MigrationHistoryAvailability reports whether a source was readable.
type MigrationHistoryAvailability string

const (
	MigrationHistoryAvailabilityAvailable   MigrationHistoryAvailability = "Available"
	MigrationHistoryAvailabilityUnavailable MigrationHistoryAvailability = "Unavailable"
)

// MigrationHistoryUnavailableReason is intentionally message-free.
type MigrationHistoryUnavailableReason string

const (
	MigrationHistoryUnavailableNotFound        MigrationHistoryUnavailableReason = "NotFound"
	MigrationHistoryUnavailableForbidden       MigrationHistoryUnavailableReason = "Forbidden"
	MigrationHistoryUnavailableUnreadable      MigrationHistoryUnavailableReason = "Unreadable"
	MigrationHistoryUnavailableInvalidIdentity MigrationHistoryUnavailableReason = "InvalidIdentity"
	MigrationHistoryUnavailableMalformed       MigrationHistoryUnavailableReason = "MalformedPayload"
	MigrationHistoryUnavailablePayloadTooLarge MigrationHistoryUnavailableReason = "PayloadTooLarge"
)

// MigrationHistoryFreshness states what generation relationship is known.
type MigrationHistoryFreshness string

const (
	MigrationHistoryFreshnessCurrent      MigrationHistoryFreshness = "Current"
	MigrationHistoryFreshnessStale        MigrationHistoryFreshness = "Stale"
	MigrationHistoryFreshnessUnobserved   MigrationHistoryFreshness = "Unobserved"
	MigrationHistoryFreshnessInvalid      MigrationHistoryFreshness = "Invalid"
	MigrationHistoryFreshnessUnverifiable MigrationHistoryFreshness = "Unverifiable"
)

// MigrationHistoryMode is the allowlisted parent-summary migration mode.
type MigrationHistoryMode string

const (
	MigrationHistoryModeUnknown MigrationHistoryMode = "Unknown"
	MigrationHistoryModeSurge   MigrationHistoryMode = "Surge"
)

// MigrationHistoryPhase spans current IR, legacy parent, and audit phases.
type MigrationHistoryPhase string

const (
	MigrationHistoryPhaseUnknown      MigrationHistoryPhase = "Unknown"
	MigrationHistoryPhaseAccepted     MigrationHistoryPhase = "Accepted"
	MigrationHistoryPhaseSurgePending MigrationHistoryPhase = "SurgePending"
	MigrationHistoryPhaseSurgeReady   MigrationHistoryPhase = "SurgeReady"
	MigrationHistoryPhaseDraining     MigrationHistoryPhase = "Draining"
	MigrationHistoryPhasePending      MigrationHistoryPhase = "Pending"
	MigrationHistoryPhaseInProgress   MigrationHistoryPhase = "InProgress"
	MigrationHistoryPhaseStarted      MigrationHistoryPhase = "Started"
	MigrationHistoryPhaseCompleted    MigrationHistoryPhase = "Completed"
	MigrationHistoryPhaseFailed       MigrationHistoryPhase = "Failed"
	MigrationHistoryPhaseRelocated    MigrationHistoryPhase = "Relocated"
)

// MigrationHistoryState classifies a record without changing its authority.
type MigrationHistoryState string

const (
	MigrationHistoryStateUnknown  MigrationHistoryState = "Unknown"
	MigrationHistoryStateActive   MigrationHistoryState = "Active"
	MigrationHistoryStateTerminal MigrationHistoryState = "Terminal"
	MigrationHistoryStateInvalid  MigrationHistoryState = "Invalid"
)

// MigrationHistoryReportState summarizes report completeness.
type MigrationHistoryReportState string

const (
	MigrationHistoryReportEmpty    MigrationHistoryReportState = "Empty"
	MigrationHistoryReportReported MigrationHistoryReportState = "Reported"
	MigrationHistoryReportPartial  MigrationHistoryReportState = "Partial"
)

// MigrationHistoryIssueCode is a stable, hostile-text-free diagnostic.
type MigrationHistoryIssueCode string

const (
	MigrationHistoryIssueAuthoritativeUnavailable    MigrationHistoryIssueCode = "AuthoritativeUnavailable"
	MigrationHistoryIssueAuthoritativeTruncated      MigrationHistoryIssueCode = "AuthoritativeTruncated"
	MigrationHistoryIssueParentTruncated             MigrationHistoryIssueCode = "ParentHistoryTruncated"
	MigrationHistoryIssueAuditUnavailable            MigrationHistoryIssueCode = "AuditUnavailable"
	MigrationHistoryIssueAuditIdentityInvalid        MigrationHistoryIssueCode = "AuditIdentityInvalid"
	MigrationHistoryIssueAuditMalformed              MigrationHistoryIssueCode = "AuditMalformed"
	MigrationHistoryIssueAuditPayloadTooLarge        MigrationHistoryIssueCode = "AuditPayloadTooLarge"
	MigrationHistoryIssueAuditTruncated              MigrationHistoryIssueCode = "AuditHistoryTruncated"
	MigrationHistoryIssueOutputTruncated             MigrationHistoryIssueCode = "OutputTruncated"
	MigrationHistoryIssueRecordInvalid               MigrationHistoryIssueCode = "RecordInvalid"
	MigrationHistoryIssueRequestIDInvalid            MigrationHistoryIssueCode = "RequestIDInvalid"
	MigrationHistoryIssueComponentUnavailable        MigrationHistoryIssueCode = "ComponentUnavailable"
	MigrationHistoryIssueComponentInvalid            MigrationHistoryIssueCode = "ComponentInvalid"
	MigrationHistoryIssueInstanceInvalid             MigrationHistoryIssueCode = "InstanceInvalid"
	MigrationHistoryIssueReplacementInvalid          MigrationHistoryIssueCode = "ReplacementInvalid"
	MigrationHistoryIssueModeInvalid                 MigrationHistoryIssueCode = "ModeInvalid"
	MigrationHistoryIssuePhaseInvalid                MigrationHistoryIssueCode = "PhaseInvalid"
	MigrationHistoryIssueTimestampInvalid            MigrationHistoryIssueCode = "TimestampInvalid"
	MigrationHistoryIssueEventInvalid                MigrationHistoryIssueCode = "EventInvalid"
	MigrationHistoryIssueEventsTruncated             MigrationHistoryIssueCode = "EventsTruncated"
	MigrationHistoryIssueNodeInvalid                 MigrationHistoryIssueCode = "NodeInvalid"
	MigrationHistoryIssueNodeHintsTruncated          MigrationHistoryIssueCode = "NodeHintsTruncated"
	MigrationHistoryIssueDuplicateWithinSource       MigrationHistoryIssueCode = "DuplicateWithinSource"
	MigrationHistoryIssueCrossSourceIdentityConflict MigrationHistoryIssueCode = "CrossSourceIdentityConflict"
	MigrationHistoryIssueChronologyConflict          MigrationHistoryIssueCode = "ChronologyConflict"
	MigrationHistoryIssueTerminalOutcomeConflict     MigrationHistoryIssueCode = "TerminalOutcomeConflict"
)

// MigrationHistoryWarningCode reports only stable completeness categories.
type MigrationHistoryWarningCode string

const (
	MigrationHistoryWarningPartial   MigrationHistoryWarningCode = "PartialData"
	MigrationHistoryWarningTruncated MigrationHistoryWarningCode = "Truncated"
)

// MigrationHistorySource describes one bounded source without exposing UIDs,
// resource versions, labels, annotations, or ConfigMap payloads.
type MigrationHistorySource struct {
	Kind              MigrationHistorySourceKind        `json:"kind"`
	Namespace         string                            `json:"namespace"`
	Name              string                            `json:"name"`
	Evidence          MigrationHistoryEvidence          `json:"evidence"`
	Availability      MigrationHistoryAvailability      `json:"availability"`
	UnavailableReason MigrationHistoryUnavailableReason `json:"unavailableReason,omitempty"`
	Freshness         MigrationHistoryFreshness         `json:"freshness"`
	ObservedPages     int                               `json:"observedPages"`
	ObservedItems     int                               `json:"observedItems"`
	Bounded           bool                              `json:"bounded"`
	Truncated         bool                              `json:"truncated"`
	CollectedAt       time.Time                         `json:"collectedAt"`
}

// MigrationHistorySummary contains counts over the emitted bounded records.
type MigrationHistorySummary struct {
	State                 MigrationHistoryReportState `json:"state"`
	Requests              int                         `json:"requests"`
	Records               int                         `json:"records"`
	Authoritative         int                         `json:"authoritative"`
	ParentSummary         int                         `json:"parentSummary"`
	Audit                 int                         `json:"audit"`
	ActiveAuthoritative   int                         `json:"activeAuthoritative"`
	TerminalAuthoritative int                         `json:"terminalAuthoritative"`
	Invalid               int                         `json:"invalid"`
	Truncated             bool                        `json:"truncated"`
}

// MigrationHistoryRecord is one allowlisted observation from one source.
// Evidence, rather than phase, determines whether it is work authority.
type MigrationHistoryRecord struct {
	Evidence            MigrationHistoryEvidence    `json:"evidence"`
	SourceName          string                      `json:"sourceName"`
	RequestID           string                      `json:"requestID"`
	Component           RuntimeComponentType        `json:"component"`
	SourceInstance      int32                       `json:"sourceInstance"`
	ReplacementInstance *int32                      `json:"replacementInstance,omitempty"`
	Trigger             MigrationTrigger            `json:"trigger"`
	Mode                MigrationHistoryMode        `json:"mode"`
	Phase               MigrationHistoryPhase       `json:"phase"`
	State               MigrationHistoryState       `json:"state"`
	Freshness           MigrationHistoryFreshness   `json:"freshness"`
	FromNode            string                      `json:"fromNode,omitempty"`
	TargetNodeHints     []string                    `json:"targetNodeHints"`
	RequestedAt         *time.Time                  `json:"requestedAt,omitempty"`
	StartedAt           *time.Time                  `json:"startedAt,omitempty"`
	AllocatedAt         *time.Time                  `json:"allocatedAt,omitempty"`
	Deadline            *time.Time                  `json:"deadline,omitempty"`
	CompletedAt         *time.Time                  `json:"completedAt,omitempty"`
	EventCount          int                         `json:"eventCount"`
	Outcome             MigrationOutcome            `json:"outcome"`
	Message             string                      `json:"message,omitempty"`
	AuthoritativeIssues []MigrationIssueCode        `json:"authoritativeIssues"`
	Issues              []MigrationHistoryIssueCode `json:"issues"`
}

// MigrationHistoryIssue scopes a stable diagnostic to evidence or request.
type MigrationHistoryIssue struct {
	Code      MigrationHistoryIssueCode `json:"code"`
	Evidence  MigrationHistoryEvidence  `json:"evidence,omitempty"`
	RequestID string                    `json:"requestID,omitempty"`
}

// MigrationHistoryContent is shared by all output formats.
type MigrationHistoryContent struct {
	Summary MigrationHistorySummary  `json:"summary"`
	Records []MigrationHistoryRecord `json:"records"`
	Issues  []MigrationHistoryIssue  `json:"issues"`
}

// MigrationHistoryWarning is deliberately code-only.
type MigrationHistoryWarning struct {
	Code MigrationHistoryWarningCode `json:"code"`
}

// MigrationHistoryReport is the versioned read-only output contract.
type MigrationHistoryReport struct {
	APIVersion  string                    `json:"apiVersion"`
	Kind        string                    `json:"kind"`
	Metadata    Metadata                  `json:"metadata"`
	CollectedAt time.Time                 `json:"collectedAt"`
	Sources     []MigrationHistorySource  `json:"sources"`
	Content     MigrationHistoryContent   `json:"content"`
	Warnings    []MigrationHistoryWarning `json:"warnings"`
}

// NewMigrationHistoryReport builds a canonical report with an injectable clock.
func NewMigrationHistoryReport(metadata Metadata, content MigrationHistoryContent, clock Clock) MigrationHistoryReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (MigrationHistoryReport{
		APIVersion: APIVersion, Kind: MigrationHistoryReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []MigrationHistorySource{},
		Content: content, Warnings: []MigrationHistoryWarning{},
	}).Canonical()
}

// Canonical returns a deeply copied, sanitized, deterministic report.
func (r MigrationHistoryReport) Canonical() MigrationHistoryReport {
	out := r
	out.APIVersion = APIVersion
	out.Kind = MigrationHistoryReportKind
	out.CollectedAt = r.CollectedAt.UTC()
	out.Metadata.Namespace = canonicalResourceName(r.Metadata.Namespace)
	out.Metadata.Name = canonicalResourceName(r.Metadata.Name)

	out.Sources = make([]MigrationHistorySource, len(r.Sources))
	for i := range r.Sources {
		out.Sources[i] = canonicalHistorySource(r.Sources[i], out.CollectedAt)
	}
	sort.Slice(out.Sources, func(i, j int) bool {
		a, b := out.Sources[i], out.Sources[j]
		return cmp.Or(
			cmp.Compare(historyEvidenceRank(a.Evidence), historyEvidenceRank(b.Evidence)),
			cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name),
		) < 0
	})

	out.Content.Summary.State = canonicalHistoryReportState(r.Content.Summary.State)
	out.Content.Records = make([]MigrationHistoryRecord, len(r.Content.Records))
	for i := range r.Content.Records {
		out.Content.Records[i] = canonicalHistoryRecord(r.Content.Records[i])
	}
	sort.Slice(out.Content.Records, func(i, j int) bool {
		return compareHistoryRecords(out.Content.Records[i], out.Content.Records[j]) < 0
	})
	out.Content.Issues = make([]MigrationHistoryIssue, len(r.Content.Issues))
	for i := range r.Content.Issues {
		out.Content.Issues[i] = canonicalHistoryIssue(r.Content.Issues[i])
	}
	sort.Slice(out.Content.Issues, func(i, j int) bool {
		a, b := out.Content.Issues[i], out.Content.Issues[j]
		return cmp.Or(cmp.Compare(a.Code, b.Code), cmp.Compare(a.RequestID, b.RequestID), cmp.Compare(a.Evidence, b.Evidence)) < 0
	})
	out.Content.Issues = slices.Compact(out.Content.Issues)

	out.Warnings = make([]MigrationHistoryWarning, len(r.Warnings))
	for i := range r.Warnings {
		code := r.Warnings[i].Code
		if code != MigrationHistoryWarningPartial && code != MigrationHistoryWarningTruncated {
			code = MigrationHistoryWarningPartial
		}
		out.Warnings[i] = MigrationHistoryWarning{Code: code}
	}
	sort.Slice(out.Warnings, func(i, j int) bool { return out.Warnings[i].Code < out.Warnings[j].Code })
	out.Warnings = slices.Compact(out.Warnings)
	return out
}

func canonicalHistorySource(source MigrationHistorySource, collectedAt time.Time) MigrationHistorySource {
	out := source
	out.Kind = canonicalHistorySourceKind(source.Kind)
	out.Namespace = canonicalResourceName(source.Namespace)
	out.Name = canonicalResourceName(source.Name)
	out.Evidence = canonicalHistoryEvidence(source.Evidence, false)
	out.Availability = canonicalHistoryAvailability(source.Availability)
	out.UnavailableReason = canonicalHistoryUnavailableReason(source.UnavailableReason)
	out.Freshness = canonicalHistoryFreshness(source.Freshness)
	if out.ObservedPages < 0 {
		out.ObservedPages = 0
	}
	if out.ObservedItems < 0 {
		out.ObservedItems = 0
	}
	if source.CollectedAt.IsZero() {
		out.CollectedAt = collectedAt
	} else {
		out.CollectedAt = source.CollectedAt.UTC()
	}
	return out
}

func canonicalHistoryRecord(record MigrationHistoryRecord) MigrationHistoryRecord {
	out := record
	out.Evidence = canonicalHistoryEvidence(record.Evidence, false)
	out.SourceName = canonicalResourceName(record.SourceName)
	if !safeIdentifier(record.RequestID) {
		out.RequestID = "INVALID"
	}
	out.Component = canonicalRuntimeComponent(record.Component)
	out.ReplacementInstance = copyMigrationInt32(record.ReplacementInstance)
	out.Trigger = canonicalMigrationTrigger(record.Trigger)
	out.Mode = canonicalHistoryMode(record.Mode)
	out.Phase = canonicalHistoryPhase(record.Phase)
	out.State = canonicalHistoryState(record.State)
	out.Freshness = canonicalHistoryFreshness(record.Freshness)
	if !safeResourceName(record.FromNode) {
		out.FromNode = ""
	}
	out.TargetNodeHints = canonicalHistoryNodes(record.TargetNodeHints)
	out.RequestedAt = copyTime(record.RequestedAt)
	out.StartedAt = copyTime(record.StartedAt)
	out.AllocatedAt = copyTime(record.AllocatedAt)
	out.Deadline = copyTime(record.Deadline)
	out.CompletedAt = copyTime(record.CompletedAt)
	if out.EventCount < 0 {
		out.EventCount = 0
	}
	out.Outcome = canonicalMigrationOutcome(record.Outcome)
	out.Message = printers.BoundedCell(record.Message, MigrationHistoryMessageMaxDisplayWidth)
	out.AuthoritativeIssues = canonicalAuthoritativeIssues(record.AuthoritativeIssues)
	out.Issues = canonicalHistoryIssueCodes(record.Issues)
	return out
}

func canonicalHistoryIssue(issue MigrationHistoryIssue) MigrationHistoryIssue {
	issue.Code = canonicalHistoryIssueCode(issue.Code)
	if issue.Evidence != "" {
		issue.Evidence = canonicalHistoryEvidence(issue.Evidence, true)
	}
	if issue.RequestID != "" && !safeIdentifier(issue.RequestID) {
		issue.RequestID = "INVALID"
	}
	return issue
}

// Table returns a compact view whose natural width is at most 80 columns.
func (r MigrationHistoryReport) Table() report.Table {
	canonical := r.Canonical()
	table := report.Table{Headers: []string{"EVID", "REQUEST/COMP", "PHASE/STATE", "WHEN", "DETAIL"}}
	for _, record := range canonical.Content.Records {
		table.Rows = append(table.Rows, []string{
			compactHistoryEvidence(record.Evidence), compactHistorySubject(record),
			compactHistoryPhase(record.Phase) + "/" + compactHistoryState(record.State),
			compactHistoryTime(record), compactHistoryMessage(record),
		})
	}
	for _, issue := range canonical.Content.Issues {
		table.Rows = append(table.Rows, []string{
			compactHistoryEvidence(orReportEvidence(issue.Evidence)), compactHistoryIssueSubject(issue),
			string(canonical.Content.Summary.State) + "/?", "-", printers.BoundedCell(string(issue.Code), 20),
		})
	}
	if len(table.Rows) == 0 {
		table.Rows = [][]string{{"REPORT", "-/-", string(canonical.Content.Summary.State) + "/?", "-", "-"}}
	}
	return table
}

// WideTable returns all allowlisted fields and source availability.
func (r MigrationHistoryReport) WideTable() report.Table {
	canonical := r.Canonical()
	table := report.Table{Headers: []string{
		"EVIDENCE", "AVAILABILITY", "WINDOW", "SOURCE", "REQUEST", "COMPONENT",
		"SOURCE-INSTANCE", "REPLACEMENT", "TRIGGER", "MODE", "PHASE", "STATE", "FRESHNESS",
		"FROM-NODE", "TARGET-NODES", "REQUESTED", "STARTED", "ALLOCATED", "DEADLINE",
		"COMPLETED", "EVENTS", "OUTCOME", "DETAIL", "ISSUES",
	}}
	seenSources := make(map[string]struct{}, len(canonical.Sources))
	for _, record := range canonical.Content.Records {
		source := findHistorySource(canonical.Sources, record)
		seenSources[historySourceKey(source)] = struct{}{}
		table.Rows = append(table.Rows, wideHistoryRecordRow(record, source))
	}
	for _, source := range canonical.Sources {
		if _, seen := seenSources[historySourceKey(source)]; seen {
			continue
		}
		table.Rows = append(table.Rows, wideHistorySourceRow(source))
	}
	for _, issue := range canonical.Content.Issues {
		table.Rows = append(table.Rows, []string{
			"Report", "-", "-", "-", dash(issue.RequestID), "-", "-", "-", "-", "-", "-",
			string(canonical.Content.Summary.State), "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", string(issue.Code),
		})
	}
	if len(table.Rows) == 0 {
		table.Rows = append(table.Rows, []string{
			"Report", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-",
			string(canonical.Content.Summary.State), "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", "-",
		})
	}
	return table
}

func wideHistoryRecordRow(record MigrationHistoryRecord, source MigrationHistorySource) []string {
	return []string{
		string(record.Evidence), string(source.Availability), historyWindow(source), dash(record.SourceName),
		dash(record.RequestID), dash(string(record.Component)), strconv.FormatInt(int64(record.SourceInstance), 10),
		formatHistoryInt32(record.ReplacementInstance), string(record.Trigger), string(record.Mode), string(record.Phase),
		string(record.State), string(record.Freshness), dash(record.FromNode), dash(strings.Join(record.TargetNodeHints, ",")),
		formatHistoryTime(record.RequestedAt), formatHistoryTime(record.StartedAt), formatHistoryTime(record.AllocatedAt),
		formatHistoryTime(record.Deadline), formatHistoryTime(record.CompletedAt), strconv.Itoa(record.EventCount),
		string(record.Outcome), dash(record.Message),
		joinHistoryRecordIssues(record),
	}
}

func wideHistorySourceRow(source MigrationHistorySource) []string {
	issue := "-"
	if source.UnavailableReason != "" {
		issue = string(source.UnavailableReason)
	}
	return []string{
		string(source.Evidence), string(source.Availability), historyWindow(source), source.Name,
		"-", "-", "-", "-", "-", "-", "-", "-", string(source.Freshness), "-", "-", "-", "-", "-", "-", "-", "-", "-", "-", issue,
	}
}

func compareHistoryRecords(a, b MigrationHistoryRecord) int {
	if order := compareOptionalTimesNewestFirst(historyRecordTime(a), historyRecordTime(b)); order != 0 {
		return order
	}
	return cmp.Or(
		cmp.Compare(a.RequestID, b.RequestID),
		cmp.Compare(historyEvidenceRank(a.Evidence), historyEvidenceRank(b.Evidence)),
		cmp.Compare(a.SourceName, b.SourceName), cmp.Compare(a.Component, b.Component),
		cmp.Compare(a.SourceInstance, b.SourceInstance), cmp.Compare(a.Phase, b.Phase),
		compareOptionalHistoryInt32(a.ReplacementInstance, b.ReplacementInstance),
		cmp.Compare(a.Trigger, b.Trigger), cmp.Compare(a.Mode, b.Mode),
		cmp.Compare(a.State, b.State), cmp.Compare(a.Freshness, b.Freshness),
		cmp.Compare(a.FromNode, b.FromNode), slices.Compare(a.TargetNodeHints, b.TargetNodeHints),
		compareOptionalTimesNewestFirst(a.RequestedAt, b.RequestedAt),
		compareOptionalTimesNewestFirst(a.StartedAt, b.StartedAt),
		compareOptionalTimesNewestFirst(a.AllocatedAt, b.AllocatedAt),
		compareOptionalTimesNewestFirst(a.Deadline, b.Deadline),
		compareOptionalTimesNewestFirst(a.CompletedAt, b.CompletedAt),
		cmp.Compare(a.EventCount, b.EventCount), cmp.Compare(a.Outcome, b.Outcome),
		cmp.Compare(a.Message, b.Message), slices.Compare(a.AuthoritativeIssues, b.AuthoritativeIssues),
		slices.Compare(a.Issues, b.Issues),
	)
}

func compareOptionalHistoryInt32(a, b *int32) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return 1
	}
	if b == nil {
		return -1
	}
	return cmp.Compare(*a, *b)
}

func historyRecordTime(record MigrationHistoryRecord) *time.Time {
	for _, candidate := range []*time.Time{record.CompletedAt, record.AllocatedAt, record.StartedAt, record.RequestedAt} {
		if candidate != nil && !candidate.IsZero() {
			return candidate
		}
	}
	return nil
}

func compactHistoryEvidence(value MigrationHistoryEvidence) string {
	switch value {
	case MigrationHistoryEvidenceAuthoritative:
		return "AUTH"
	case MigrationHistoryEvidenceParent:
		return "PARENT"
	case MigrationHistoryEvidenceAudit:
		return "AUDIT"
	default:
		return "REPORT"
	}
}

func compactHistorySubject(record MigrationHistoryRecord) string {
	request := record.RequestID
	if len(request) > 8 {
		request = request[:8]
	}
	component := "?"
	switch record.Component {
	case RuntimeComponentEngine:
		component = "E"
	case RuntimeComponentDecoder:
		component = "D"
	case RuntimeComponentRouter:
		component = "R"
	}
	return request + "/" + component
}

func compactHistoryIssueSubject(issue MigrationHistoryIssue) string {
	request := issue.RequestID
	if request == "" {
		request = "-"
	} else if len(request) > 8 {
		request = request[:8]
	}
	return request + "/-"
}

func compactHistoryPhase(value MigrationHistoryPhase) string {
	return printers.BoundedCell(string(value), 12)
}

func compactHistoryState(value MigrationHistoryState) string {
	switch value {
	case MigrationHistoryStateActive:
		return "A"
	case MigrationHistoryStateTerminal:
		return "T"
	case MigrationHistoryStateInvalid:
		return "!"
	default:
		return "?"
	}
}

func compactHistoryTime(record MigrationHistoryRecord) string {
	value := historyRecordTime(record)
	if value == nil {
		return "-"
	}
	return value.UTC().Format("01-02T15:04Z")
}

func compactHistoryMessage(record MigrationHistoryRecord) string {
	if len(record.Issues) > 0 {
		return printers.BoundedCell(string(record.Issues[0]), 20)
	}
	if len(record.AuthoritativeIssues) > 0 {
		return printers.BoundedCell(string(record.AuthoritativeIssues[0]), 20)
	}
	if record.Message != "" {
		return printers.BoundedCell(record.Message, 20)
	}
	return "-"
}

func findHistorySource(sources []MigrationHistorySource, record MigrationHistoryRecord) MigrationHistorySource {
	for _, source := range sources {
		if source.Evidence == record.Evidence && source.Name == record.SourceName {
			return source
		}
	}
	for _, source := range sources {
		if source.Evidence == record.Evidence {
			return source
		}
	}
	return MigrationHistorySource{Availability: MigrationHistoryAvailabilityUnavailable}
}

func historyWindow(source MigrationHistorySource) string {
	result := fmt.Sprintf("%dp/%di/", source.ObservedPages, source.ObservedItems)
	if source.Bounded {
		result += "B/"
	}
	if source.Truncated {
		return result + "T"
	}
	return result + "C"
}

func historySourceKey(source MigrationHistorySource) string {
	return string(source.Evidence) + "\x00" + source.Name
}

func formatHistoryInt32(value *int32) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatInt(int64(*value), 10)
}

func formatHistoryTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func joinHistoryRecordIssues(record MigrationHistoryRecord) string {
	values := make([]string, 0, len(record.AuthoritativeIssues)+len(record.Issues))
	for _, issue := range record.AuthoritativeIssues {
		values = append(values, string(issue))
	}
	for _, issue := range record.Issues {
		values = append(values, string(issue))
	}
	return dash(strings.Join(values, ","))
}

func dash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func orReportEvidence(value MigrationHistoryEvidence) MigrationHistoryEvidence {
	if value == "" {
		return MigrationHistoryEvidenceReport
	}
	return value
}

func canonicalResourceName(value string) string {
	if safeResourceName(value) {
		return value
	}
	return "invalid"
}

func canonicalRuntimeComponent(value RuntimeComponentType) RuntimeComponentType {
	switch value {
	case RuntimeComponentEngine, RuntimeComponentDecoder, RuntimeComponentRouter:
		return value
	default:
		return RuntimeComponentType("unknown")
	}
}

func canonicalMigrationTrigger(value MigrationTrigger) MigrationTrigger {
	switch value {
	case MigrationTriggerManual, MigrationTriggerAuto, MigrationTriggerUnknown:
		return value
	default:
		return MigrationTriggerUnknown
	}
}

func canonicalMigrationOutcome(value MigrationOutcome) MigrationOutcome {
	switch value {
	case MigrationOutcomeUnknown, MigrationOutcomeInProgress, MigrationOutcomeCompleted,
		MigrationOutcomeFailed, MigrationOutcomeRelocated, MigrationOutcomeRelocationConfirmed:
		return value
	default:
		return MigrationOutcomeUnknown
	}
}

func canonicalHistoryEvidence(value MigrationHistoryEvidence, allowReport bool) MigrationHistoryEvidence {
	switch value {
	case MigrationHistoryEvidenceUnknown, MigrationHistoryEvidenceAuthoritative,
		MigrationHistoryEvidenceParent, MigrationHistoryEvidenceAudit:
		return value
	case MigrationHistoryEvidenceReport:
		if allowReport {
			return value
		}
	}
	if allowReport {
		return MigrationHistoryEvidenceReport
	}
	return MigrationHistoryEvidenceUnknown
}

func canonicalHistorySourceKind(value MigrationHistorySourceKind) MigrationHistorySourceKind {
	switch value {
	case MigrationHistorySourceUnknown, MigrationHistorySourceInferenceReplica,
		MigrationHistorySourceInferenceService, MigrationHistorySourceConfigMap:
		return value
	default:
		return MigrationHistorySourceUnknown
	}
}

func canonicalHistoryAvailability(value MigrationHistoryAvailability) MigrationHistoryAvailability {
	if value == MigrationHistoryAvailabilityAvailable {
		return value
	}
	return MigrationHistoryAvailabilityUnavailable
}

func canonicalHistoryUnavailableReason(value MigrationHistoryUnavailableReason) MigrationHistoryUnavailableReason {
	switch value {
	case "", MigrationHistoryUnavailableNotFound, MigrationHistoryUnavailableForbidden,
		MigrationHistoryUnavailableUnreadable, MigrationHistoryUnavailableInvalidIdentity,
		MigrationHistoryUnavailableMalformed, MigrationHistoryUnavailablePayloadTooLarge:
		return value
	default:
		return MigrationHistoryUnavailableUnreadable
	}
}

func canonicalHistoryFreshness(value MigrationHistoryFreshness) MigrationHistoryFreshness {
	switch value {
	case MigrationHistoryFreshnessCurrent, MigrationHistoryFreshnessStale,
		MigrationHistoryFreshnessUnobserved, MigrationHistoryFreshnessInvalid,
		MigrationHistoryFreshnessUnverifiable:
		return value
	default:
		return MigrationHistoryFreshnessInvalid
	}
}

func canonicalHistoryMode(value MigrationHistoryMode) MigrationHistoryMode {
	if value == MigrationHistoryModeSurge {
		return value
	}
	return MigrationHistoryModeUnknown
}

func canonicalHistoryPhase(value MigrationHistoryPhase) MigrationHistoryPhase {
	switch value {
	case MigrationHistoryPhaseAccepted, MigrationHistoryPhaseSurgePending,
		MigrationHistoryPhaseSurgeReady, MigrationHistoryPhaseDraining,
		MigrationHistoryPhasePending, MigrationHistoryPhaseInProgress,
		MigrationHistoryPhaseStarted, MigrationHistoryPhaseCompleted,
		MigrationHistoryPhaseFailed, MigrationHistoryPhaseRelocated,
		MigrationHistoryPhaseUnknown:
		return value
	default:
		return MigrationHistoryPhaseUnknown
	}
}

func canonicalHistoryState(value MigrationHistoryState) MigrationHistoryState {
	switch value {
	case MigrationHistoryStateActive, MigrationHistoryStateTerminal,
		MigrationHistoryStateInvalid, MigrationHistoryStateUnknown:
		return value
	default:
		return MigrationHistoryStateUnknown
	}
}

func canonicalHistoryReportState(value MigrationHistoryReportState) MigrationHistoryReportState {
	switch value {
	case MigrationHistoryReportEmpty, MigrationHistoryReportReported, MigrationHistoryReportPartial:
		return value
	default:
		return MigrationHistoryReportPartial
	}
}

func canonicalHistoryNodes(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if safeResourceName(value) {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return slices.Compact(result)
}

func canonicalAuthoritativeIssues(values []MigrationIssueCode) []MigrationIssueCode {
	result := make([]MigrationIssueCode, 0, len(values))
	for _, value := range values {
		if knownMigrationIssueCode(value) {
			result = append(result, value)
		} else {
			result = append(result, MigrationIssueRequestInvalid)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return slices.Compact(result)
}

func knownMigrationIssueCode(value MigrationIssueCode) bool {
	switch value {
	case MigrationIssueSourceIdentityInvalid, MigrationIssueSourceLabelMismatch,
		MigrationIssueSourceParentMismatch, MigrationIssueSourceOwnerMismatch,
		MigrationIssueSourceComponentInvalid, MigrationIssueSourceDuplicate,
		MigrationIssueSourceStale, MigrationIssueSourceUnobserved,
		MigrationIssueSourceGenerationInvalid, MigrationIssueRequestInvalid,
		MigrationIssueRequestDuplicate, MigrationIssueTriggerInvalid,
		MigrationIssuePhaseInvalid, MigrationIssueTriggerPhaseConflict,
		MigrationIssueSourceIndexInvalid, MigrationIssueSurgeIndexInvalid,
		MigrationIssueAttemptInvalid, MigrationIssueAllocationInvalid,
		MigrationIssueTimestampInvalid, MigrationIssueTerminalShapeInvalid,
		MigrationIssueSucceededInvalid, MigrationIssueNodeHintInvalid,
		MigrationIssueNodeHintsTruncated, MigrationIssueRecordsTruncated,
		MigrationIssueSourcesTruncated:
		return true
	default:
		return false
	}
}

func canonicalHistoryIssueCodes(values []MigrationHistoryIssueCode) []MigrationHistoryIssueCode {
	result := make([]MigrationHistoryIssueCode, len(values))
	for i, value := range values {
		result[i] = canonicalHistoryIssueCode(value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return slices.Compact(result)
}

func canonicalHistoryIssueCode(value MigrationHistoryIssueCode) MigrationHistoryIssueCode {
	switch value {
	case MigrationHistoryIssueAuthoritativeUnavailable, MigrationHistoryIssueAuthoritativeTruncated,
		MigrationHistoryIssueParentTruncated, MigrationHistoryIssueAuditUnavailable,
		MigrationHistoryIssueAuditIdentityInvalid, MigrationHistoryIssueAuditMalformed,
		MigrationHistoryIssueAuditPayloadTooLarge, MigrationHistoryIssueAuditTruncated,
		MigrationHistoryIssueOutputTruncated, MigrationHistoryIssueRecordInvalid,
		MigrationHistoryIssueRequestIDInvalid, MigrationHistoryIssueComponentUnavailable,
		MigrationHistoryIssueComponentInvalid, MigrationHistoryIssueInstanceInvalid,
		MigrationHistoryIssueReplacementInvalid, MigrationHistoryIssueModeInvalid,
		MigrationHistoryIssuePhaseInvalid, MigrationHistoryIssueTimestampInvalid,
		MigrationHistoryIssueEventInvalid, MigrationHistoryIssueEventsTruncated,
		MigrationHistoryIssueNodeInvalid, MigrationHistoryIssueNodeHintsTruncated,
		MigrationHistoryIssueDuplicateWithinSource, MigrationHistoryIssueCrossSourceIdentityConflict,
		MigrationHistoryIssueChronologyConflict, MigrationHistoryIssueTerminalOutcomeConflict:
		return value
	default:
		return MigrationHistoryIssueRecordInvalid
	}
}

func historyEvidenceRank(value MigrationHistoryEvidence) int {
	switch value {
	case MigrationHistoryEvidenceAuthoritative:
		return 0
	case MigrationHistoryEvidenceParent:
		return 1
	case MigrationHistoryEvidenceAudit:
		return 2
	default:
		return 3
	}
}
