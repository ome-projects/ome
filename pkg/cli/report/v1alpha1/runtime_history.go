package v1alpha1

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

const RuntimeHistoryReportKind = "RuntimeHistoryReport"

// RuntimeRevisionEntry is an allowlisted revision history observation.
type RuntimeRevisionEntry struct {
	Revision       RuntimeRevisionReference `json:"revision"`
	Source         *RuntimeObjectReference  `json:"source,omitempty"`
	Hash           string                   `json:"hash,omitempty"`
	Roles          []RuntimeRevisionRole    `json:"roles"`
	Consistency    RevisionConsistency      `json:"consistency"`
	RelationToLive RevisionRelation         `json:"relationToLive"`
	Issues         []RuntimeIssueCode       `json:"issues"`
}

// RuntimeHistoryContent reports a bounded runtime revision history window.
type RuntimeHistoryContent struct {
	Runtime        *RuntimeObjectReference `json:"runtime,omitempty"`
	Observation    HistoryObservationState `json:"observation"`
	Completeness   HistoryCompleteness     `json:"completeness"`
	RequestedPages int                     `json:"requestedPages"`
	ObservedPages  int                     `json:"observedPages"`
	Revisions      []RuntimeRevisionEntry  `json:"revisions"`
	Issues         []RuntimeIssue          `json:"issues"`
}

// NewRuntimeHistoryReport creates a canonical runtime history report.
func NewRuntimeHistoryReport(metadata Metadata, content RuntimeHistoryContent, clock Clock) RuntimeEnvelope[RuntimeHistoryContent] {
	return newRuntimeEnvelope(metadata, content, clock)
}

func (RuntimeHistoryContent) runtimeReportKind() string {
	return RuntimeHistoryReportKind
}

// Canonical returns a deeply copied deterministic history report content.
func (c RuntimeHistoryContent) Canonical() RuntimeHistoryContent {
	result := c
	result.Runtime = copyRuntimeObjectReference(c.Runtime)
	result.Revisions = make([]RuntimeRevisionEntry, len(c.Revisions))
	for i := range c.Revisions {
		result.Revisions[i] = c.Revisions[i].canonical()
	}
	sort.Slice(result.Revisions, func(i, j int) bool {
		return compareRuntimeRevisionEntries(result.Revisions[i], result.Revisions[j]) < 0
	})
	result.Issues = append([]RuntimeIssue{}, c.Issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		if result.Issues[i].Code != result.Issues[j].Code {
			return result.Issues[i].Code < result.Issues[j].Code
		}
		return result.Issues[i].Revision < result.Issues[j].Revision
	})
	return result
}

func compareRuntimeRevisionEntries(a, b RuntimeRevisionEntry) int {
	if result := compareOptionalTimesNewestFirst(a.Revision.CreatedAt, b.Revision.CreatedAt); result != 0 {
		return result
	}
	for _, result := range []int{
		cmp.Compare(a.Revision.Namespace, b.Revision.Namespace),
		cmp.Compare(a.Revision.Name, b.Revision.Name),
		cmp.Compare(a.Revision.UID, b.Revision.UID),
		cmp.Compare(a.Hash, b.Hash),
		compareRuntimeObjectReferences(a.Source, b.Source),
		slices.Compare(a.Roles, b.Roles),
		cmp.Compare(a.Consistency, b.Consistency),
		cmp.Compare(a.RelationToLive, b.RelationToLive),
		slices.Compare(a.Issues, b.Issues),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRuntimeObjectReferences(a, b *RuntimeObjectReference) int {
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
		cmp.Compare(a.APIVersion, b.APIVersion),
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Name, b.Name),
		cmp.Compare(a.UID, b.UID),
		cmp.Compare(a.Generation, b.Generation),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

// Table returns a compact deterministic revision history view. Every cell has
// a fixed display-width bound so the complete table fits an 80-column terminal.
func (c RuntimeHistoryContent) Table() report.Table {
	canonical := c.Canonical()
	table := report.Table{
		Headers: []string{"WINDOW", "REVISION", "CREATED", "ROLES", "CHECK", "LIVE", "ISSUES"},
		Rows:    make([][]string, 0, len(canonical.Revisions)),
	}
	window := compactRuntimeHistoryWindow(canonical)
	for _, entry := range canonical.Revisions {
		table.Rows = append(table.Rows, []string{
			window,
			compactRuntimeHistoryIdentity(entry.Revision.Name),
			compactRuntimeHistoryTime(entry.Revision.CreatedAt),
			compactRuntimeHistoryRoles(entry.Roles),
			compactRuntimeHistoryConsistency(entry.Consistency),
			compactRuntimeHistoryRelation(entry.RelationToLive),
			compactRuntimeHistoryIssues(len(entry.Issues), len(canonical.Issues)),
		})
	}
	if len(table.Rows) == 0 {
		table.Rows = append(table.Rows, []string{
			window, "-", "-", "-", "-", "-", compactRuntimeHistoryIssues(0, len(canonical.Issues)),
		})
	}
	return table
}

// WideTable returns the complete legacy human-readable revision history view.
func (c RuntimeHistoryContent) WideTable() report.Table {
	canonical := c.Canonical()
	table := report.Table{
		Headers: []string{
			"OBSERVATION", "COMPLETENESS", "PAGES", "REVISION", "CREATED", "HASH", "ROLES", "SOURCE",
			"CONSISTENCY", "RELATION", "REVISION-ISSUES", "REPORT-ISSUES",
		},
		Rows: make([][]string, 0, len(canonical.Revisions)),
	}
	observation := orDash(string(canonical.Observation))
	completeness := orDash(string(canonical.Completeness))
	pages := fmt.Sprintf("%d/%d", canonical.ObservedPages, canonical.RequestedPages)
	reportIssues := joinRuntimeIssues(canonical.Issues)
	for _, entry := range canonical.Revisions {
		table.Rows = append(table.Rows, []string{
			observation,
			completeness,
			pages,
			orDash(entry.Revision.Name),
			formatRevisionTime(entry.Revision.CreatedAt),
			orDash(entry.Hash),
			joinRevisionRoles(entry.Roles),
			runtimeReferenceDisplay(entry.Source),
			orDash(string(entry.Consistency)),
			orDash(string(entry.RelationToLive)),
			joinIssueCodes(entry.Issues),
			reportIssues,
		})
	}
	if len(table.Rows) == 0 {
		table.Rows = append(table.Rows, []string{
			observation, completeness, pages, "-", "-", "-", "-", "-", "-", "-", "-", reportIssues,
		})
	}
	return table
}

const compactRuntimeHistoryRevisionWidth = 14

func compactRuntimeHistoryWindow(content RuntimeHistoryContent) string {
	return compactRuntimeHistoryObservation(content.Observation) + "/" +
		compactRuntimeHistoryCompleteness(content.Completeness) + "/" +
		compactRuntimeHistoryPageCount(content.ObservedPages) + "/" +
		compactRuntimeHistoryPageCount(content.RequestedPages)
}

func compactRuntimeHistoryObservation(observation HistoryObservationState) string {
	switch observation {
	case HistoryObservationStateNotRequested:
		return "N"
	case HistoryObservationStateComplete:
		return "C"
	case HistoryObservationStatePartial:
		return "P"
	case HistoryObservationStateUnavailable:
		return "U"
	default:
		return "?"
	}
}

func compactRuntimeHistoryCompleteness(completeness HistoryCompleteness) string {
	switch completeness {
	case HistoryCompletenessNotRequested:
		return "N"
	case HistoryCompletenessRetentionBounded:
		return "B"
	case HistoryCompletenessIncomplete:
		return "I"
	default:
		return "?"
	}
}

func compactRuntimeHistoryPageCount(count int) string {
	switch {
	case count < 0:
		return "?"
	case count > 99:
		return "99+"
	default:
		return strconv.Itoa(count)
	}
}

func compactRuntimeHistoryIdentity(value string) string {
	const normalizedIdentityLimit = 1024
	identity := orDash(value)
	digest := sha256.Sum256([]byte(identity))
	clean := printers.BoundedMiddleCell(identity, normalizedIdentityLimit)
	if printers.BoundedMiddleCell(clean, compactRuntimeHistoryRevisionWidth) == clean {
		return clean
	}
	prefix := printers.BoundedCell(clean, compactRuntimeHistoryRevisionWidth-1-8)
	return prefix + "#" + hex.EncodeToString(digest[:4])
}

func compactRuntimeHistoryTime(createdAt *time.Time) string {
	if createdAt == nil || createdAt.IsZero() {
		return "-"
	}
	return createdAt.UTC().Format("06-01-02T15:04Z")
}

func compactRuntimeHistoryRoles(roles []RuntimeRevisionRole) string {
	hasActive := false
	hasRequested := false
	hasReported := false
	hasHistory := false
	hasUnknown := false
	for _, role := range roles {
		switch role {
		case RuntimeRevisionRoleActive:
			hasActive = true
		case RuntimeRevisionRoleRequested:
			hasRequested = true
		case RuntimeRevisionRoleReported:
			hasReported = true
		case RuntimeRevisionRoleHistory:
			hasHistory = true
		default:
			hasUnknown = true
		}
	}
	var result strings.Builder
	if hasActive {
		result.WriteByte('A')
	}
	if hasRequested {
		result.WriteByte('Q')
	}
	if hasReported {
		result.WriteByte('R')
	}
	if hasHistory {
		result.WriteByte('H')
	}
	if hasUnknown {
		result.WriteByte('?')
	}
	return orDash(result.String())
}

func compactRuntimeHistoryConsistency(consistency RevisionConsistency) string {
	switch consistency {
	case RevisionConsistencyConsistent:
		return "OK"
	case RevisionConsistencyInconsistent:
		return "BAD"
	case RevisionConsistencyUnknown:
		return "?"
	case "":
		return "-"
	default:
		return "OTHER"
	}
}

func compactRuntimeHistoryRelation(relation RevisionRelation) string {
	switch relation {
	case RevisionRelationMatchesLive:
		return "MATCH"
	case RevisionRelationDiffersFromLive:
		return "DIFF"
	case RevisionRelationAmbiguous:
		return "AMB"
	case RevisionRelationUnknown:
		return "?"
	case "":
		return "-"
	default:
		return "OTHER"
	}
}

func compactRuntimeHistoryIssues(revisionIssues, reportIssues int) string {
	return "R" + compactRuntimeHistoryIssueCount(revisionIssues) +
		"/G" + compactRuntimeHistoryIssueCount(reportIssues)
}

func compactRuntimeHistoryIssueCount(count int) string {
	if count > 9 {
		return "9+"
	}
	return strconv.Itoa(count)
}

func (entry RuntimeRevisionEntry) canonical() RuntimeRevisionEntry {
	result := entry
	result.Revision.CreatedAt = canonicalTimePointer(entry.Revision.CreatedAt)
	result.Source = copyRuntimeObjectReference(entry.Source)
	result.Roles = append([]RuntimeRevisionRole{}, entry.Roles...)
	sort.Slice(result.Roles, func(i, j int) bool {
		if revisionRoleOrder(result.Roles[i]) != revisionRoleOrder(result.Roles[j]) {
			return revisionRoleOrder(result.Roles[i]) < revisionRoleOrder(result.Roles[j])
		}
		return result.Roles[i] < result.Roles[j]
	})
	result.Issues = append([]RuntimeIssueCode{}, entry.Issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		return result.Issues[i] < result.Issues[j]
	})
	return result
}

func revisionRoleOrder(role RuntimeRevisionRole) int {
	switch role {
	case RuntimeRevisionRoleActive:
		return 0
	case RuntimeRevisionRoleRequested:
		return 1
	case RuntimeRevisionRoleReported:
		return 2
	case RuntimeRevisionRoleHistory:
		return 3
	default:
		return 4
	}
}

func formatRevisionTime(createdAt *time.Time) string {
	if createdAt == nil || createdAt.IsZero() {
		return "-"
	}
	return createdAt.UTC().Format(time.RFC3339)
}

func compareOptionalTimesNewestFirst(a, b *time.Time) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return 1
	}
	if b == nil {
		return -1
	}
	if a.Equal(*b) {
		return 0
	}
	if a.After(*b) {
		return -1
	}
	return 1
}

func joinRevisionRoles(roles []RuntimeRevisionRole) string {
	values := make([]string, len(roles))
	for i, role := range roles {
		values[i] = string(role)
	}
	return orDash(strings.Join(values, ","))
}

func joinIssueCodes(issues []RuntimeIssueCode) string {
	values := make([]string, len(issues))
	for i, issue := range issues {
		values[i] = string(issue)
	}
	return orDash(strings.Join(values, ","))
}
