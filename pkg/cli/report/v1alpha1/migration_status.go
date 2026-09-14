package v1alpha1

import (
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"slices"
	"sort"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// MigrationStatusReportKind identifies the migration status output schema.
const MigrationStatusReportKind = "MigrationStatusReport"

// MigrationMessageMaxDisplayWidth bounds the sanitized controller message in
// every output format while retaining enough text for an actionable blocker.
const MigrationMessageMaxDisplayWidth = 256

// MigrationSourceKind is the closed set read by migration status.
type MigrationSourceKind string

const (
	MigrationSourceInferenceService MigrationSourceKind = "InferenceService"
	MigrationSourceInferenceReplica MigrationSourceKind = "InferenceReplica"
)

// MigrationReportState summarizes the completeness of the bounded snapshot.
type MigrationReportState string

const (
	MigrationReportStateEmpty    MigrationReportState = "Empty"
	MigrationReportStateReported MigrationReportState = "Reported"
	MigrationReportStatePartial  MigrationReportState = "Partial"
)

// MigrationTrigger is the allowlisted live-record trigger.
type MigrationTrigger string

const (
	MigrationTriggerUnknown MigrationTrigger = "Unknown"
	MigrationTriggerManual  MigrationTrigger = "Manual"
	MigrationTriggerAuto    MigrationTrigger = "Auto"
)

// MigrationPhase is the allowlisted current InferenceReplica phase set.
type MigrationPhase string

const (
	MigrationPhaseUnknown      MigrationPhase = "Unknown"
	MigrationPhaseAccepted     MigrationPhase = "Accepted"
	MigrationPhaseSurgePending MigrationPhase = "SurgePending"
	MigrationPhaseSurgeReady   MigrationPhase = "SurgeReady"
	MigrationPhaseDraining     MigrationPhase = "Draining"
	MigrationPhaseCompleted    MigrationPhase = "Completed"
	MigrationPhaseFailed       MigrationPhase = "Failed"
	MigrationPhaseRelocated    MigrationPhase = "Relocated"
)

// MigrationClassification identifies whether a record is executable history.
type MigrationClassification string

const (
	MigrationClassificationActive   MigrationClassification = "Active"
	MigrationClassificationTerminal MigrationClassification = "Terminal"
	MigrationClassificationInvalid  MigrationClassification = "Invalid"
)

// MigrationReasonEvidence reveals provenance without copying arbitrary reason text.
type MigrationReasonEvidence string

const (
	MigrationReasonUnavailable      MigrationReasonEvidence = "Unavailable"
	MigrationReasonOperatorSupplied MigrationReasonEvidence = "OperatorSupplied"
	MigrationReasonAutoRecover      MigrationReasonEvidence = "AutoRecover"
	MigrationReasonController       MigrationReasonEvidence = "ControllerReported"
)

// MigrationMessageEvidence records only whether the API reported a message.
type MigrationMessageEvidence string

const (
	MigrationMessageAbsent  MigrationMessageEvidence = "Absent"
	MigrationMessagePresent MigrationMessageEvidence = "Present"
)

// MigrationOutcome is an allowlisted interpretation of phase and succeeded.
type MigrationOutcome string

const (
	MigrationOutcomeUnknown             MigrationOutcome = "Unknown"
	MigrationOutcomeInProgress          MigrationOutcome = "InProgress"
	MigrationOutcomeCompleted           MigrationOutcome = "Completed"
	MigrationOutcomeFailed              MigrationOutcome = "Failed"
	MigrationOutcomeRelocated           MigrationOutcome = "Relocated"
	MigrationOutcomeRelocationConfirmed MigrationOutcome = "RelocationConfirmed"
)

// MigrationIssueCode is a stable, message-free diagnostic.
type MigrationIssueCode string

const (
	MigrationIssueSourceIdentityInvalid   MigrationIssueCode = "SourceIdentityInvalid"
	MigrationIssueSourceLabelMismatch     MigrationIssueCode = "SourceLabelMismatch"
	MigrationIssueSourceParentMismatch    MigrationIssueCode = "SourceParentMismatch"
	MigrationIssueSourceOwnerMismatch     MigrationIssueCode = "SourceOwnerMismatch"
	MigrationIssueSourceComponentInvalid  MigrationIssueCode = "SourceComponentInvalid"
	MigrationIssueSourceDuplicate         MigrationIssueCode = "SourceComponentDuplicate"
	MigrationIssueSourceStale             MigrationIssueCode = "SourceGenerationStale"
	MigrationIssueSourceUnobserved        MigrationIssueCode = "SourceGenerationUnobserved"
	MigrationIssueSourceGenerationInvalid MigrationIssueCode = "SourceGenerationInvalid"
	MigrationIssueRequestInvalid          MigrationIssueCode = "RequestIDInvalid"
	MigrationIssueRequestDuplicate        MigrationIssueCode = "RequestIDDuplicate"
	MigrationIssueTriggerInvalid          MigrationIssueCode = "TriggerInvalid"
	MigrationIssuePhaseInvalid            MigrationIssueCode = "PhaseInvalid"
	MigrationIssueTriggerPhaseConflict    MigrationIssueCode = "TriggerPhaseConflict"
	MigrationIssueSourceIndexInvalid      MigrationIssueCode = "SourceIndexInvalid"
	MigrationIssueSurgeIndexInvalid       MigrationIssueCode = "SurgeIndexInvalid"
	MigrationIssueAttemptInvalid          MigrationIssueCode = "AttemptInvalid"
	MigrationIssueAllocationInvalid       MigrationIssueCode = "AllocationInvalid"
	MigrationIssueTimestampInvalid        MigrationIssueCode = "TimestampInvalid"
	MigrationIssueTerminalShapeInvalid    MigrationIssueCode = "TerminalShapeInvalid"
	MigrationIssueSucceededInvalid        MigrationIssueCode = "SucceededInvalid"
	MigrationIssueNodeHintInvalid         MigrationIssueCode = "NodeHintInvalid"
	MigrationIssueNodeHintsTruncated      MigrationIssueCode = "NodeHintsTruncated"
	MigrationIssueRecordsTruncated        MigrationIssueCode = "RecordsTruncated"
	MigrationIssueSourcesTruncated        MigrationIssueCode = "SourcesTruncated"
)

// MigrationWarningCode identifies report-wide incomplete evidence.
type MigrationWarningCode string

const (
	MigrationWarningPartialData MigrationWarningCode = "PartialData"
	MigrationWarningTruncated   MigrationWarningCode = "Truncated"
)

// MigrationSourceReference is an allowlisted source identity. It omits
// resourceVersion, annotations, labels, owner lists, and arbitrary messages.
type MigrationSourceReference struct {
	Kind               MigrationSourceKind `json:"kind"`
	Namespace          string              `json:"namespace"`
	Name               string              `json:"name"`
	UID                string              `json:"uid,omitempty"`
	Generation         int64               `json:"generation,omitempty"`
	ObservedGeneration int64               `json:"observedGeneration,omitempty"`
	Freshness          StatusFreshness     `json:"freshness"`
	CollectedAt        time.Time           `json:"collectedAt"`
}

// MigrationSummary reports bounded record counts.
type MigrationSummary struct {
	State    MigrationReportState `json:"state"`
	Records  int                  `json:"records"`
	Active   int                  `json:"active"`
	Terminal int                  `json:"terminal"`
	Invalid  int                  `json:"invalid"`
}

// MigrationCapacityEvidence exposes only what the live records prove. The
// current API does not publish concurrency or rate limits.
type MigrationCapacityEvidence struct {
	Evidence         EvidenceLevel `json:"evidence"`
	ActiveAllocated  int           `json:"activeAllocated"`
	ConcurrencyLimit EvidenceLevel `json:"concurrencyLimitEvidence"`
	RateLimit        EvidenceLevel `json:"rateLimitEvidence"`
}

// MigrationRecord is one safe, bounded live InferenceReplica record.
type MigrationRecord struct {
	RequestID           string                   `json:"requestID"`
	SourceName          string                   `json:"sourceName"`
	Component           RuntimeComponentType     `json:"component"`
	SourceInstance      int32                    `json:"sourceInstance"`
	SurgeInstance       *int32                   `json:"surgeInstance,omitempty"`
	Trigger             MigrationTrigger         `json:"trigger"`
	Phase               MigrationPhase           `json:"phase"`
	Classification      MigrationClassification  `json:"classification"`
	Freshness           StatusFreshness          `json:"freshness"`
	FromNode            string                   `json:"fromNode,omitempty"`
	TargetNodeHints     []string                 `json:"targetNodeHints"`
	Attempt             int32                    `json:"attempt,omitempty"`
	RequestedAt         *time.Time               `json:"requestedAt,omitempty"`
	RequestedAtEvidence EvidenceLevel            `json:"requestedAtEvidence"`
	StartedAt           *time.Time               `json:"startedAt,omitempty"`
	AllocatedAt         *time.Time               `json:"allocatedAt,omitempty"`
	Deadline            *time.Time               `json:"deadline,omitempty"`
	CompletedAt         *time.Time               `json:"completedAt,omitempty"`
	ReasonEvidence      MigrationReasonEvidence  `json:"reasonEvidence"`
	Message             string                   `json:"message,omitempty"`
	MessageEvidence     MigrationMessageEvidence `json:"messageEvidence"`
	Outcome             MigrationOutcome         `json:"outcome"`
	Issues              []MigrationIssueCode     `json:"issues"`
}

// MigrationIssue scopes a stable diagnostic without copying hostile fields.
type MigrationIssue struct {
	Code       MigrationIssueCode   `json:"code"`
	SourceName string               `json:"sourceName,omitempty"`
	RequestID  string               `json:"requestID,omitempty"`
	Component  RuntimeComponentType `json:"component,omitempty"`
}

// MigrationStatusContent is shared by all output formats.
type MigrationStatusContent struct {
	Summary    MigrationSummary          `json:"summary"`
	Capacity   MigrationCapacityEvidence `json:"capacity"`
	Migrations []MigrationRecord         `json:"migrations"`
	Issues     []MigrationIssue          `json:"issues"`
}

// MigrationWarning is deliberately code-only.
type MigrationWarning struct {
	Code MigrationWarningCode `json:"code"`
}

// MigrationStatusReport is the versioned read-only output contract.
type MigrationStatusReport struct {
	APIVersion  string                     `json:"apiVersion"`
	Kind        string                     `json:"kind"`
	Metadata    Metadata                   `json:"metadata"`
	CollectedAt time.Time                  `json:"collectedAt"`
	Sources     []MigrationSourceReference `json:"sources"`
	Content     MigrationStatusContent     `json:"content"`
	Warnings    []MigrationWarning         `json:"warnings"`
}

// NewMigrationStatusReport builds a canonical report with an injectable clock.
func NewMigrationStatusReport(metadata Metadata, content MigrationStatusContent, clock Clock) MigrationStatusReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (MigrationStatusReport{
		APIVersion: APIVersion, Kind: MigrationStatusReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []MigrationSourceReference{},
		Content: content, Warnings: []MigrationWarning{},
	}).Canonical()
}

// Canonical returns a deeply copied, deterministically ordered report.
func (r MigrationStatusReport) Canonical() MigrationStatusReport {
	out := r
	out.APIVersion = APIVersion
	out.Kind = MigrationStatusReportKind
	out.CollectedAt = r.CollectedAt.UTC()
	out.Sources = append([]MigrationSourceReference{}, r.Sources...)
	for i := range out.Sources {
		if out.Sources[i].CollectedAt.IsZero() {
			out.Sources[i].CollectedAt = out.CollectedAt
		} else {
			out.Sources[i].CollectedAt = out.Sources[i].CollectedAt.UTC()
		}
	}
	sort.SliceStable(out.Sources, func(i, j int) bool {
		a, b := out.Sources[i], out.Sources[j]
		if sourceKindRank(a.Kind) != sourceKindRank(b.Kind) {
			return sourceKindRank(a.Kind) < sourceKindRank(b.Kind)
		}
		if order := cmp.Or(
			cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name),
			cmp.Compare(a.UID, b.UID), cmp.Compare(a.Generation, b.Generation),
			cmp.Compare(a.ObservedGeneration, b.ObservedGeneration),
			cmp.Compare(a.Freshness, b.Freshness),
		); order != 0 {
			return order < 0
		}
		return a.CollectedAt.Before(b.CollectedAt)
	})

	out.Content.Migrations = append([]MigrationRecord{}, r.Content.Migrations...)
	for i := range out.Content.Migrations {
		record := &out.Content.Migrations[i]
		record.SurgeInstance = copyMigrationInt32(record.SurgeInstance)
		record.RequestedAt = copyTime(record.RequestedAt)
		record.StartedAt = copyTime(record.StartedAt)
		record.AllocatedAt = copyTime(record.AllocatedAt)
		record.Deadline = copyTime(record.Deadline)
		record.CompletedAt = copyTime(record.CompletedAt)
		record.Message = printers.BoundedCell(record.Message, MigrationMessageMaxDisplayWidth)
		record.TargetNodeHints = sortedUniqueStrings(record.TargetNodeHints)
		record.Issues = sortedUniqueIssueCodes(record.Issues)
	}
	sort.SliceStable(out.Content.Migrations, func(i, j int) bool {
		return compareMigrationRecords(out.Content.Migrations[i], out.Content.Migrations[j]) < 0
	})
	out.Content.Issues = append([]MigrationIssue{}, r.Content.Issues...)
	sort.SliceStable(out.Content.Issues, func(i, j int) bool {
		a, b := out.Content.Issues[i], out.Content.Issues[j]
		return cmp.Or(
			cmp.Compare(a.Code, b.Code), cmp.Compare(a.Component, b.Component),
			cmp.Compare(a.SourceName, b.SourceName), cmp.Compare(a.RequestID, b.RequestID),
		) < 0
	})
	out.Warnings = append([]MigrationWarning{}, r.Warnings...)
	sort.SliceStable(out.Warnings, func(i, j int) bool { return out.Warnings[i].Code < out.Warnings[j].Code })
	return out
}

// Table returns the compact human view. Its maximum natural width is 80
// display columns; JSON and YAML retain the detailed timestamps and evidence.
func (r MigrationStatusReport) Table() report.Table {
	canonical := r.Canonical()
	table := report.Table{Headers: []string{"SUBJECT/COMP", "STATUS", "DETAIL"}}
	table.Rows = make([][]string, 0, len(canonical.Content.Migrations)+len(canonical.Content.Issues))
	seenIssues := make(map[MigrationIssue]struct{})
	for _, migration := range canonical.Content.Migrations {
		subject := compactRequestID(migration.RequestID) + "/" + compactComponent(migration.Component)
		status := compactPhase(migration.Phase) + "/" + compactClassification(migration.Classification) + "/" + compactFreshness(migration.Freshness)
		if migration.Message != "" {
			table.Rows = append(table.Rows, []string{subject, status, compactMigrationMessage(migration.Message)})
		}
		if len(migration.Issues) == 0 {
			if migration.Message == "" {
				table.Rows = append(table.Rows, []string{subject, status, "-"})
			}
			continue
		}
		for _, code := range migration.Issues {
			table.Rows = append(table.Rows, []string{subject, status, compactMigrationIssueCode(code)})
			seenIssues[MigrationIssue{
				Code: code, SourceName: migration.SourceName, RequestID: migration.RequestID,
				Component: migration.Component,
			}] = struct{}{}
		}
	}
	status := compactReportState(canonical.Content.Summary.State) + "/" + reportFreshness(canonical)
	for _, issue := range canonical.Content.Issues {
		if _, seen := seenIssues[issue]; seen {
			continue
		}
		table.Rows = append(table.Rows, []string{
			compactMigrationIssueSubject(issue), status, compactMigrationIssueCode(issue.Code),
		})
		seenIssues[issue] = struct{}{}
	}
	if len(table.Rows) == 0 {
		table.Rows = [][]string{{"-/-", status, "-"}}
	}
	return table
}

func compactMigrationMessage(value string) string {
	return printers.BoundedCell("MSG: "+value, 26)
}

func compactMigrationIssueSubject(issue MigrationIssue) string {
	request := "REPORT"
	if issue.RequestID != "" {
		request = compactRequestID(issue.RequestID)
	} else if issue.SourceName != "" {
		request = compactSourceName(issue.SourceName)
	}
	component := "-"
	if issue.Component != "" {
		component = compactComponent(issue.Component)
	}
	return request + "/" + component
}

func compactSourceName(value string) string {
	const cellWidth = 8

	valid := safeResourceName(value)
	if valid && len(value) <= cellWidth {
		return value
	}
	// The sentinel cannot occur at the start of a valid resource name, so a
	// digest token cannot collide with an unabridged short name. Seven base64url
	// characters retain 42 bits of SHA-256 while keeping the cell at 8 columns.
	digest := sha256.Sum256([]byte(value))
	token := base64.RawURLEncoding.EncodeToString(digest[:])[:cellWidth-1]
	if valid {
		return "~" + token
	}
	return "!" + token
}

func reportFreshness(report MigrationStatusReport) string {
	freshness := StatusFreshnessCurrent
	for _, source := range report.Sources {
		if freshnessRank(source.Freshness) > freshnessRank(freshness) {
			freshness = source.Freshness
		}
	}
	for _, migration := range report.Content.Migrations {
		if freshnessRank(migration.Freshness) > freshnessRank(freshness) {
			freshness = migration.Freshness
		}
	}
	return compactFreshness(freshness)
}

func freshnessRank(value StatusFreshness) int {
	switch value {
	case StatusFreshnessCurrent:
		return 0
	case StatusFreshnessStale:
		return 1
	case StatusFreshnessUnobserved:
		return 2
	case StatusFreshnessInvalid:
		return 3
	default:
		return 4
	}
}

func sourceKindRank(kind MigrationSourceKind) int {
	if kind == MigrationSourceInferenceService {
		return 0
	}
	return 1
}

func compareMigrationRecords(a, b MigrationRecord) int {
	return cmp.Or(
		cmp.Compare(a.RequestID, b.RequestID), cmp.Compare(a.Component, b.Component),
		cmp.Compare(a.SourceName, b.SourceName), cmp.Compare(a.SourceInstance, b.SourceInstance),
		compareMigrationInt32Pointers(a.SurgeInstance, b.SurgeInstance),
		cmp.Compare(a.Trigger, b.Trigger), cmp.Compare(a.Phase, b.Phase),
		cmp.Compare(a.Classification, b.Classification), cmp.Compare(a.Freshness, b.Freshness),
		cmp.Compare(a.FromNode, b.FromNode), slices.Compare(a.TargetNodeHints, b.TargetNodeHints),
		cmp.Compare(a.Attempt, b.Attempt), compareMigrationTimePointers(a.RequestedAt, b.RequestedAt),
		cmp.Compare(a.RequestedAtEvidence, b.RequestedAtEvidence),
		compareMigrationTimePointers(a.StartedAt, b.StartedAt),
		compareMigrationTimePointers(a.AllocatedAt, b.AllocatedAt),
		compareMigrationTimePointers(a.Deadline, b.Deadline),
		compareMigrationTimePointers(a.CompletedAt, b.CompletedAt),
		cmp.Compare(a.ReasonEvidence, b.ReasonEvidence), cmp.Compare(a.Message, b.Message),
		cmp.Compare(a.MessageEvidence, b.MessageEvidence),
		cmp.Compare(a.Outcome, b.Outcome), slices.Compare(a.Issues, b.Issues),
	)
}

func compareMigrationInt32Pointers(a, b *int32) int {
	if a == nil || b == nil {
		switch {
		case a == nil && b != nil:
			return -1
		case a != nil && b == nil:
			return 1
		default:
			return 0
		}
	}
	return cmp.Compare(*a, *b)
}

func compareMigrationTimePointers(a, b *time.Time) int {
	if a == nil || b == nil {
		switch {
		case a == nil && b != nil:
			return -1
		case a != nil && b == nil:
			return 1
		default:
			return 0
		}
	}
	if a.Equal(*b) {
		return 0
	}
	if a.Before(*b) {
		return -1
	}
	return 1
}

func copyMigrationInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func copyTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func sortedUniqueStrings(values []string) []string {
	out := append([]string{}, values...)
	sort.Strings(out)
	return slices.Compact(out)
}

func sortedUniqueIssueCodes(values []MigrationIssueCode) []MigrationIssueCode {
	out := append([]MigrationIssueCode{}, values...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return slices.Compact(out)
}

func compactRequestID(value string) string {
	if !safeIdentifier(value) {
		return "INVALID"
	}
	if len(value) > 8 {
		return value[:8]
	}
	return value
}

func compactComponent(value RuntimeComponentType) string {
	switch value {
	case RuntimeComponentEngine, RuntimeComponentDecoder, RuntimeComponentRouter:
		return string(value)
	default:
		return "?"
	}
}

func compactPhase(value MigrationPhase) string {
	switch value {
	case MigrationPhaseAccepted, MigrationPhaseSurgePending, MigrationPhaseSurgeReady,
		MigrationPhaseDraining, MigrationPhaseCompleted, MigrationPhaseFailed,
		MigrationPhaseRelocated:
		return string(value)
	case MigrationPhaseUnknown:
		return "?"
	default:
		return "?"
	}
}

func compactClassification(value MigrationClassification) string {
	switch value {
	case MigrationClassificationActive, MigrationClassificationTerminal, MigrationClassificationInvalid:
		return string(value)
	default:
		return "?"
	}
}

func compactFreshness(value StatusFreshness) string {
	switch value {
	case StatusFreshnessCurrent, StatusFreshnessStale, StatusFreshnessUnobserved, StatusFreshnessInvalid:
		return string(value)
	default:
		return "?"
	}
}

func compactMigrationIssueCode(value MigrationIssueCode) string {
	switch value {
	case MigrationIssueSourceIdentityInvalid,
		MigrationIssueSourceLabelMismatch,
		MigrationIssueSourceParentMismatch,
		MigrationIssueSourceOwnerMismatch,
		MigrationIssueSourceComponentInvalid,
		MigrationIssueSourceDuplicate,
		MigrationIssueSourceStale,
		MigrationIssueSourceUnobserved,
		MigrationIssueSourceGenerationInvalid,
		MigrationIssueRequestInvalid,
		MigrationIssueRequestDuplicate,
		MigrationIssueTriggerInvalid,
		MigrationIssuePhaseInvalid,
		MigrationIssueTriggerPhaseConflict,
		MigrationIssueSourceIndexInvalid,
		MigrationIssueSurgeIndexInvalid,
		MigrationIssueAttemptInvalid,
		MigrationIssueAllocationInvalid,
		MigrationIssueTimestampInvalid,
		MigrationIssueTerminalShapeInvalid,
		MigrationIssueSucceededInvalid,
		MigrationIssueNodeHintInvalid,
		MigrationIssueNodeHintsTruncated,
		MigrationIssueRecordsTruncated,
		MigrationIssueSourcesTruncated:
		return string(value)
	default:
		return "?"
	}
}

func compactReportState(value MigrationReportState) string {
	switch value {
	case MigrationReportStateEmpty, MigrationReportStateReported, MigrationReportStatePartial:
		return string(value)
	default:
		return "?"
	}
}

func safeIdentifier(value string) bool {
	if value == "" || len(value) > 63 || !asciiAlphaNumeric(value[0]) || !asciiAlphaNumeric(value[len(value)-1]) {
		return false
	}
	for i := range len(value) {
		char := value[i]
		if !asciiAlphaNumeric(char) && char != '-' && char != '_' && char != '.' {
			return false
		}
	}
	return true
}

func safeResourceName(value string) bool {
	if value == "" || len(value) > 253 || !asciiLowerNumeric(value[0]) || !asciiLowerNumeric(value[len(value)-1]) {
		return false
	}
	for i := range len(value) {
		char := value[i]
		if !asciiLowerNumeric(char) && char != '-' && char != '.' {
			return false
		}
	}
	return true
}

func asciiLowerNumeric(char byte) bool {
	return char >= 'a' && char <= 'z' || char >= '0' && char <= '9'
}

func asciiAlphaNumeric(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}
