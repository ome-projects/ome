package v1alpha1

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// InstanceRetryBlocksReportKind identifies the retry-block report schema.
const InstanceRetryBlocksReportKind = "InstanceRetryBlocksReport"

type InstanceRetryBlocksState string

const (
	InstanceRetryBlocksReported    InstanceRetryBlocksState = "Reported"
	InstanceRetryBlocksEmpty       InstanceRetryBlocksState = "Empty"
	InstanceRetryBlocksPartial     InstanceRetryBlocksState = "Partial"
	InstanceRetryBlocksUnavailable InstanceRetryBlocksState = "Unavailable"
)

type InstanceRetryBlockState string

const (
	InstanceRetryBlockBackoff         InstanceRetryBlockState = "Backoff"
	InstanceRetryBlockHeld            InstanceRetryBlockState = "Held"
	InstanceRetryBlockRetryInProgress InstanceRetryBlockState = "RetryInProgress"
)

type InstanceRetryBlockIssueCode string

const (
	InstanceRetryBlockIssueCollectionUnavailable     InstanceRetryBlockIssueCode = "CollectionUnavailable"
	InstanceRetryBlockIssueCollectionTruncated       InstanceRetryBlockIssueCode = "CollectionTruncated"
	InstanceRetryBlockIssueIdentityRejected          InstanceRetryBlockIssueCode = "IdentityRejected"
	InstanceRetryBlockIssueDuplicateComponent        InstanceRetryBlockIssueCode = "DuplicateComponent"
	InstanceRetryBlockIssueComponentNotProjected     InstanceRetryBlockIssueCode = "ComponentNotProjected"
	InstanceRetryBlockIssueBlocksTruncated           InstanceRetryBlockIssueCode = "RetryBlocksTruncated"
	InstanceRetryBlockIssueParentGenerationMissing   InstanceRetryBlockIssueCode = "ParentGenerationMissing"
	InstanceRetryBlockIssueParentGenerationInvalid   InstanceRetryBlockIssueCode = "ParentGenerationInvalid"
	InstanceRetryBlockIssueParentGenerationStale     InstanceRetryBlockIssueCode = "ParentGenerationStale"
	InstanceRetryBlockIssueParentGenerationAhead     InstanceRetryBlockIssueCode = "ParentGenerationAhead"
	InstanceRetryBlockIssueStatusUnobserved          InstanceRetryBlockIssueCode = "StatusUnobserved"
	InstanceRetryBlockIssueObservedGenerationInvalid InstanceRetryBlockIssueCode = "ObservedGenerationInvalid"
	InstanceRetryBlockIssueStatusStale               InstanceRetryBlockIssueCode = "StatusStale"
	InstanceRetryBlockIssueTargetRevisionInvalid     InstanceRetryBlockIssueCode = "TargetRevisionInvalid"
	InstanceRetryBlockIssueStateInvalid              InstanceRetryBlockIssueCode = "StateInvalid"
	InstanceRetryBlockIssueAttemptsInvalid           InstanceRetryBlockIssueCode = "AttemptsInvalid"
	InstanceRetryBlockIssueTimestampsInvalid         InstanceRetryBlockIssueCode = "TimestampsInvalid"
	InstanceRetryBlockIssueDuplicateTarget           InstanceRetryBlockIssueCode = "DuplicateTarget"
)

type InstanceRetryBlocksSummary struct {
	State     InstanceRetryBlocksState `json:"state"`
	Blocks    int                      `json:"blocks"`
	Held      int                      `json:"held"`
	Eligible  int                      `json:"eligible"`
	Truncated bool                     `json:"truncated"`
}

type InstanceRetryBlock struct {
	Component        RuntimeComponentType    `json:"component"`
	InferenceReplica string                  `json:"inferenceReplica"`
	TargetRevision   string                  `json:"targetRevision"`
	State            InstanceRetryBlockState `json:"state"`
	AttemptsStarted  int32                   `json:"attemptsStarted"`
	NextRetryAt      *time.Time              `json:"nextRetryAt,omitempty"`
	FirstFailureAt   *time.Time              `json:"firstFailureAt,omitempty"`
	LastFailureAt    *time.Time              `json:"lastFailureAt,omitempty"`
	Reason           string                  `json:"reason,omitempty"`
	ReasonTruncated  bool                    `json:"reasonTruncated"`
	ReleaseEligible  bool                    `json:"releaseEligible"`
	Freshness        StatusFreshness         `json:"freshness"`
}

type InstanceRetryBlockIssue struct {
	Code              InstanceRetryBlockIssueCode `json:"code"`
	Component         RuntimeComponentType        `json:"component,omitempty"`
	InferenceReplica  string                      `json:"inferenceReplica,omitempty"`
	TargetRevision    string                      `json:"targetRevision,omitempty"`
	UnavailableReason UnavailableReason           `json:"unavailableReason,omitempty"`
}

type InstanceRetryBlocksContent struct {
	Summary InstanceRetryBlocksSummary `json:"summary"`
	Blocks  []InstanceRetryBlock       `json:"blocks"`
	Issues  []InstanceRetryBlockIssue  `json:"issues"`
}

type InstanceRetryBlocksReport struct {
	APIVersion  string                     `json:"apiVersion"`
	Kind        string                     `json:"kind"`
	Metadata    Metadata                   `json:"metadata"`
	CollectedAt time.Time                  `json:"collectedAt"`
	Sources     []SourceReference          `json:"sources"`
	Content     InstanceRetryBlocksContent `json:"content"`
	Warnings    []Warning                  `json:"warnings"`
}

func NewInstanceRetryBlocksReport(
	metadata Metadata,
	content InstanceRetryBlocksContent,
	clock Clock,
) InstanceRetryBlocksReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (InstanceRetryBlocksReport{
		APIVersion: APIVersion, Kind: InstanceRetryBlocksReportKind,
		Metadata: metadata, CollectedAt: clock.Now().UTC(),
		Sources: []SourceReference{}, Content: content, Warnings: []Warning{},
	}).Canonical()
}

func (r InstanceRetryBlocksReport) Canonical() InstanceRetryBlocksReport {
	result := r
	result.APIVersion = APIVersion
	result.Kind = InstanceRetryBlocksReportKind
	result.CollectedAt = r.CollectedAt.UTC()
	result.Sources = append([]SourceReference{}, r.Sources...)
	for i := range result.Sources {
		if result.Sources[i].CollectedAt.IsZero() {
			result.Sources[i].CollectedAt = result.CollectedAt
		} else {
			result.Sources[i].CollectedAt = result.Sources[i].CollectedAt.UTC()
		}
	}
	sort.SliceStable(result.Sources, func(i, j int) bool {
		return sourceLess(result.Sources[i], result.Sources[j])
	})
	result.Content = r.Content.Canonical()
	result.Warnings = append([]Warning{}, r.Warnings...)
	sort.Slice(result.Warnings, func(i, j int) bool {
		if result.Warnings[i].Code != result.Warnings[j].Code {
			return result.Warnings[i].Code < result.Warnings[j].Code
		}
		return result.Warnings[i].Message < result.Warnings[j].Message
	})
	result.Warnings = slices.Compact(result.Warnings)
	return result
}

func (r InstanceRetryBlocksReport) Table() report.Table { return r.Content.Table() }

func (c InstanceRetryBlocksContent) Canonical() InstanceRetryBlocksContent {
	result := c
	result.Blocks = make([]InstanceRetryBlock, len(c.Blocks))
	for i := range c.Blocks {
		result.Blocks[i] = c.Blocks[i]
		result.Blocks[i].NextRetryAt = canonicalTimePointer(c.Blocks[i].NextRetryAt)
		result.Blocks[i].FirstFailureAt = canonicalTimePointer(c.Blocks[i].FirstFailureAt)
		result.Blocks[i].LastFailureAt = canonicalTimePointer(c.Blocks[i].LastFailureAt)
	}
	sort.Slice(result.Blocks, func(i, j int) bool {
		return compareInstanceRetryBlocks(result.Blocks[i], result.Blocks[j]) < 0
	})
	result.Issues = append([]InstanceRetryBlockIssue{}, c.Issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		return compareInstanceRetryBlockIssues(result.Issues[i], result.Issues[j]) < 0
	})
	result.Issues = slices.Compact(result.Issues)
	return result
}

func (c InstanceRetryBlocksContent) Table() report.Table {
	canonical := c.Canonical()
	table := report.Table{
		Headers: []string{"COMP", "STATE", "TARGET", "ATT", "NEXT", "REL", "REASON"},
		Rows:    [][]string{},
	}
	for _, block := range canonical.Blocks {
		reason := "-"
		if block.Reason != "" {
			reason = printers.BoundedCell(block.Reason, 13)
		}
		table.Rows = append(table.Rows, []string{
			printers.BoundedCell(string(block.Component), 7),
			compactInstanceRetryBlockState(block.State),
			compactInstanceRetryRevision(block.TargetRevision),
			compactAttempts(block.AttemptsStarted),
			compactRetryTime(block.NextRetryAt),
			boolCell(block.ReleaseEligible),
			reason,
		})
	}
	for _, issue := range canonical.Issues {
		component := "-"
		if issue.Component != "" {
			component = printers.BoundedCell(string(issue.Component), 7)
		}
		target := "-"
		if issue.TargetRevision != "" {
			target = compactInstanceRetryRevision(issue.TargetRevision)
		}
		table.Rows = append(table.Rows, []string{
			component, "ISSUE", target, "-", "-", "NO", compactInstanceRetryIssue(issue.Code),
		})
	}
	if len(table.Rows) == 0 {
		table.Rows = append(table.Rows, []string{"-", "EMPTY", "-", "-", "-", "NO", "-"})
	}
	return table
}

func compareInstanceRetryBlocks(a, b InstanceRetryBlock) int {
	if rank := compareRetryComponent(a.Component, b.Component); rank != 0 {
		return rank
	}
	if a.InferenceReplica != b.InferenceReplica {
		if a.InferenceReplica < b.InferenceReplica {
			return -1
		}
		return 1
	}
	if a.TargetRevision != b.TargetRevision {
		if a.TargetRevision < b.TargetRevision {
			return -1
		}
		return 1
	}
	if order := cmp.Compare(a.State, b.State); order != 0 {
		return order
	}
	if order := cmp.Compare(a.AttemptsStarted, b.AttemptsStarted); order != 0 {
		return order
	}
	if order := compareRetryTimePointers(a.NextRetryAt, b.NextRetryAt); order != 0 {
		return order
	}
	if order := compareRetryTimePointers(a.FirstFailureAt, b.FirstFailureAt); order != 0 {
		return order
	}
	if order := compareRetryTimePointers(a.LastFailureAt, b.LastFailureAt); order != 0 {
		return order
	}
	if order := cmp.Compare(a.Reason, b.Reason); order != 0 {
		return order
	}
	if order := compareRetryBools(a.ReasonTruncated, b.ReasonTruncated); order != 0 {
		return order
	}
	if order := compareRetryBools(a.ReleaseEligible, b.ReleaseEligible); order != 0 {
		return order
	}
	return cmp.Compare(a.Freshness, b.Freshness)
}

func compareRetryTimePointers(a, b *time.Time) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	case a.Equal(*b):
		return 0
	case a.Before(*b):
		return -1
	default:
		return 1
	}
}

func compareRetryBools(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}

func compareInstanceRetryBlockIssues(a, b InstanceRetryBlockIssue) int {
	if rank := compareRetryComponent(a.Component, b.Component); rank != 0 {
		return rank
	}
	left := string(a.Code) + "\x00" + a.InferenceReplica + "\x00" + a.TargetRevision + "\x00" + string(a.UnavailableReason)
	right := string(b.Code) + "\x00" + b.InferenceReplica + "\x00" + b.TargetRevision + "\x00" + string(b.UnavailableReason)
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func compareRetryComponent(a, b RuntimeComponentType) int {
	rank := func(component RuntimeComponentType) int {
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
	if rank(a) != rank(b) {
		return rank(a) - rank(b)
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compactInstanceRetryBlockState(state InstanceRetryBlockState) string {
	switch state {
	case InstanceRetryBlockBackoff:
		return "BACKOFF"
	case InstanceRetryBlockHeld:
		return "HELD"
	case InstanceRetryBlockRetryInProgress:
		return "RUNNING"
	default:
		return "INVALID"
	}
}

func compactInstanceRetryRevision(value string) string {
	const width = 14
	if len(utilvalidation.IsDNS1123Subdomain(value)) == 0 && len(value) <= width {
		return value
	}
	digest := sha256.Sum256([]byte(value))
	suffix := hex.EncodeToString(digest[:4])
	if len(utilvalidation.IsDNS1123Subdomain(value)) != 0 {
		return "!" + suffix
	}
	return value[:width-len(suffix)-1] + "#" + suffix
}

func compactAttempts(value int32) string {
	switch {
	case value < 0:
		return "?"
	case value > 99:
		return "99+"
	default:
		return strconv.FormatInt(int64(value), 10)
	}
}

func compactRetryTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "-"
	}
	return value.UTC().Format("06-01-02T15:04Z")
}

func boolCell(value bool) string {
	if value {
		return "YES"
	}
	return "NO"
}

func compactInstanceRetryIssue(code InstanceRetryBlockIssueCode) string {
	aliases := map[InstanceRetryBlockIssueCode]string{
		InstanceRetryBlockIssueCollectionUnavailable:     "COLL_UNAV",
		InstanceRetryBlockIssueCollectionTruncated:       "COLL_TRUNC",
		InstanceRetryBlockIssueIdentityRejected:          "ID_REJECT",
		InstanceRetryBlockIssueDuplicateComponent:        "DUP_COMP",
		InstanceRetryBlockIssueComponentNotProjected:     "NO_COMPONENT",
		InstanceRetryBlockIssueBlocksTruncated:           "BLOCK_TRUNC",
		InstanceRetryBlockIssueParentGenerationMissing:   "PARENT_MISS",
		InstanceRetryBlockIssueParentGenerationInvalid:   "PARENT_BAD",
		InstanceRetryBlockIssueParentGenerationStale:     "PARENT_STALE",
		InstanceRetryBlockIssueParentGenerationAhead:     "PARENT_AHEAD",
		InstanceRetryBlockIssueStatusUnobserved:          "STATUS_UNOBS",
		InstanceRetryBlockIssueObservedGenerationInvalid: "OBSGEN_BAD",
		InstanceRetryBlockIssueStatusStale:               "STATUS_STALE",
		InstanceRetryBlockIssueTargetRevisionInvalid:     "TARGET_BAD",
		InstanceRetryBlockIssueStateInvalid:              "STATE_BAD",
		InstanceRetryBlockIssueAttemptsInvalid:           "ATT_BAD",
		InstanceRetryBlockIssueTimestampsInvalid:         "TIME_BAD",
		InstanceRetryBlockIssueDuplicateTarget:           "DUP_TARGET",
	}
	if alias, found := aliases[code]; found {
		return alias
	}
	digest := sha256.Sum256([]byte(code))
	return fmt.Sprintf("X#%s", hex.EncodeToString(digest[:5]))
}
