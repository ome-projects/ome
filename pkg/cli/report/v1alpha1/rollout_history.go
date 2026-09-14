package v1alpha1

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/cli/report"
)

// RolloutHistoryReportKind identifies the rollout-history report schema.
const RolloutHistoryReportKind = "RolloutHistoryReport"

var (
	rolloutHistoryRevisionPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)
	rolloutHistoryRunHashPattern  = regexp.MustCompile(`^[0-9a-f]{12}$`)
)

// RolloutHistoryState summarizes the usable evidence in the retained window.
type RolloutHistoryState string

const (
	RolloutHistoryStateUnknown     RolloutHistoryState = "Unknown"
	RolloutHistoryStateUnavailable RolloutHistoryState = "Unavailable"
	RolloutHistoryStateEmpty       RolloutHistoryState = "Empty"
	RolloutHistoryStateReported    RolloutHistoryState = "Reported"
	RolloutHistoryStatePartial     RolloutHistoryState = "Partial"
)

// RolloutHistoryCompleteness states the API's durable-history bound.
type RolloutHistoryCompleteness string

const (
	// RolloutHistoryRetentionBounded means the report contains only the open
	// run, the single lastRun slot, and current status retained on the ISVC.
	RolloutHistoryRetentionBounded RolloutHistoryCompleteness = "RetentionBounded"
)

// RolloutHistoryRunSlot identifies one of the two run-model slots.
type RolloutHistoryRunSlot string

const (
	RolloutHistoryRunActive RolloutHistoryRunSlot = "Active"
	RolloutHistoryRunLast   RolloutHistoryRunSlot = "Last"
)

// RolloutHistoryRunOutcome is an allowlisted active or terminal outcome.
type RolloutHistoryRunOutcome string

const (
	RolloutHistoryRunActiveState RolloutHistoryRunOutcome = "Active"
	RolloutHistoryRunCompleted   RolloutHistoryRunOutcome = "Completed"
	RolloutHistoryRunRolledBack  RolloutHistoryRunOutcome = "RolledBack"
	RolloutHistoryRunSuperseded  RolloutHistoryRunOutcome = "Superseded"
)

// RolloutHistoryView identifies which retained source a provenance row came
// from. Current is the resolution view a new run would pin now.
type RolloutHistoryView string

const (
	RolloutHistoryViewActive  RolloutHistoryView = "ActiveRun"
	RolloutHistoryViewLast    RolloutHistoryView = "LastRun"
	RolloutHistoryViewCurrent RolloutHistoryView = "Current"
)

// RolloutHistoryIssueCode is a stable, message-free diagnostic.
type RolloutHistoryIssueCode string

const (
	RolloutHistoryIssueRunStatusUnavailable       RolloutHistoryIssueCode = "RunStatusUnavailable"
	RolloutHistoryIssueActiveRunMalformed         RolloutHistoryIssueCode = "ActiveRunMalformed"
	RolloutHistoryIssueLastRunMalformed           RolloutHistoryIssueCode = "LastRunMalformed"
	RolloutHistoryIssueCurrentResolutionMissing   RolloutHistoryIssueCode = "CurrentResolutionMissing"
	RolloutHistoryIssueCurrentResolutionMalformed RolloutHistoryIssueCode = "CurrentResolutionMalformed"
)

// RolloutHistorySummary describes the bounded window and qualifies the
// current status-derived revision rows.
type RolloutHistorySummary struct {
	State           RolloutHistoryState        `json:"state"`
	Completeness    RolloutHistoryCompleteness `json:"completeness"`
	CurrentState    RolloutState               `json:"currentState"`
	CurrentEvidence EvidenceLevel              `json:"currentEvidence"`
	CurrentEpoch    RolloutEpochState          `json:"currentEpoch"`
	ActiveRuns      int                        `json:"activeRuns"`
	RetainedRuns    int                        `json:"retainedRuns"`
	Revisions       int                        `json:"revisions"`
}

// RolloutHistoryTarget is one controller-pinned component target hash.
type RolloutHistoryTarget struct {
	Component    RuntimeComponentType `json:"component"`
	RevisionHash string               `json:"revisionHash"`
}

// RolloutHistoryRun is one active or retained run slot. It deliberately has
// no plan body, condition message, annotation, or arbitrary extension field.
type RolloutHistoryRun struct {
	Slot       RolloutHistoryRunSlot    `json:"slot"`
	Outcome    RolloutHistoryRunOutcome `json:"outcome"`
	RunID      string                   `json:"runID,omitempty"`
	OpenedAt   *time.Time               `json:"openedAt,omitempty"`
	PinnedAt   *time.Time               `json:"pinnedAt,omitempty"`
	ClosedAt   *time.Time               `json:"closedAt,omitempty"`
	GroupCount int                      `json:"groupCount"`
	Targets    []RolloutHistoryTarget   `json:"targets"`
}

// RolloutHistoryProvenance is the allowlisted source identity for one group.
// Portable digests prove body equality without serializing the body itself.
type RolloutHistoryProvenance struct {
	View           RolloutHistoryView      `json:"view"`
	Group          int                     `json:"group"`
	Source         RolloutPlanSource       `json:"source"`
	Policy         *RolloutPolicyReference `json:"policy,omitempty"`
	PortableDigest string                  `json:"portableDigest"`
	DigestEvidence EvidenceLevel           `json:"digestEvidence"`
	ShadowedPolicy *RolloutPolicyReference `json:"shadowedPolicy,omitempty"`
	ObservedAt     *time.Time              `json:"observedAt,omitempty"`
}

// RolloutHistoryRevision is one validated current status revision relation.
type RolloutHistoryRevision struct {
	Component    RuntimeComponentType `json:"component"`
	Role         RolloutRevisionRole  `json:"role"`
	RevisionHash string               `json:"revisionHash"`
	Phase        RolloutPhase         `json:"phase"`
}

// RolloutHistoryIssue scopes a fixed diagnostic without hostile input text.
type RolloutHistoryIssue struct {
	Code      RolloutHistoryIssueCode `json:"code"`
	View      RolloutHistoryView      `json:"view,omitempty"`
	Group     *int                    `json:"group,omitempty"`
	Component RuntimeComponentType    `json:"component,omitempty"`
}

// RolloutHistoryContent is the typed, allowlisted rollout-history body.
type RolloutHistoryContent struct {
	Summary      RolloutHistorySummary      `json:"summary"`
	Runs         []RolloutHistoryRun        `json:"runs"`
	Provenance   []RolloutHistoryProvenance `json:"provenance"`
	Revisions    []RolloutHistoryRevision   `json:"revisions"`
	StatusIssues []RolloutIssue             `json:"statusIssues"`
	Issues       []RolloutHistoryIssue      `json:"issues"`
}

// RolloutHistoryReport is the stable CLI-owned v1alpha1 report.
type RolloutHistoryReport struct {
	APIVersion  string                   `json:"apiVersion"`
	Kind        string                   `json:"kind"`
	Metadata    Metadata                 `json:"metadata"`
	CollectedAt time.Time                `json:"collectedAt"`
	Sources     []RolloutSourceReference `json:"sources"`
	Content     RolloutHistoryContent    `json:"content"`
	Warnings    []RolloutWarning         `json:"warnings"`
}

// NewRolloutHistoryReport creates a canonical rollout-history report.
func NewRolloutHistoryReport(
	metadata Metadata,
	content RolloutHistoryContent,
	clock Clock,
) RolloutHistoryReport {
	if clock == nil {
		clock = SystemClock{}
	}
	return (RolloutHistoryReport{
		APIVersion: APIVersion, Kind: RolloutHistoryReportKind, Metadata: metadata,
		CollectedAt: clock.Now().UTC(), Sources: []RolloutSourceReference{},
		Content: content, Warnings: []RolloutWarning{},
	}).Canonical()
}

// Canonical returns a deterministic deep copy.
func (r RolloutHistoryReport) Canonical() RolloutHistoryReport {
	out := r
	out.APIVersion = APIVersion
	out.Kind = RolloutHistoryReportKind
	out.CollectedAt = r.CollectedAt.UTC()
	out.Sources = append([]RolloutSourceReference{}, r.Sources...)
	for i := range out.Sources {
		out.Sources[i].Kind = canonicalRolloutSourceKind(out.Sources[i].Kind)
		out.Sources[i].Evidence = canonicalRolloutEvidenceLevel(out.Sources[i].Evidence)
		if out.Sources[i].CollectedAt.IsZero() {
			out.Sources[i].CollectedAt = out.CollectedAt
		} else {
			out.Sources[i].CollectedAt = out.Sources[i].CollectedAt.UTC()
		}
	}
	sort.SliceStable(out.Sources, func(i, j int) bool {
		return compareRolloutHistorySources(out.Sources[i], out.Sources[j]) < 0
	})
	out.Warnings = append([]RolloutWarning{}, r.Warnings...)
	for i := range out.Warnings {
		out.Warnings[i].Code = canonicalRolloutWarningCode(out.Warnings[i].Code)
	}
	sort.Slice(out.Warnings, func(i, j int) bool { return out.Warnings[i].Code < out.Warnings[j].Code })
	out.Content = r.Content.canonical(r.Metadata.Name)
	return out
}

// Canonical returns a deep, deterministically ordered copy. A direct content
// value can validate a syntactically safe run ID; the report additionally
// binds that ID to its metadata.name.
func (c RolloutHistoryContent) Canonical() RolloutHistoryContent {
	return c.canonical("")
}

func (c RolloutHistoryContent) canonical(subjectName string) RolloutHistoryContent {
	out := c
	out.Summary.State = canonicalRolloutHistoryState(c.Summary.State)
	out.Summary.Completeness = RolloutHistoryRetentionBounded
	out.Summary.CurrentState = canonicalRolloutState(c.Summary.CurrentState)
	out.Summary.CurrentEvidence = canonicalRolloutEvidenceLevel(c.Summary.CurrentEvidence)
	out.Summary.CurrentEpoch = canonicalRolloutEpochState(c.Summary.CurrentEpoch)

	out.Runs = make([]RolloutHistoryRun, 0, len(c.Runs))
	for _, value := range c.Runs {
		if run, ok := canonicalRolloutHistoryRun(value, subjectName); ok {
			out.Runs = append(out.Runs, run)
		}
	}
	sort.Slice(out.Runs, func(i, j int) bool {
		return compareRolloutHistoryRuns(out.Runs[i], out.Runs[j]) < 0
	})

	out.Provenance = make([]RolloutHistoryProvenance, 0, len(c.Provenance))
	for _, value := range c.Provenance {
		if provenance, ok := canonicalRolloutHistoryProvenance(value); ok {
			out.Provenance = append(out.Provenance, provenance)
		}
	}
	sort.Slice(out.Provenance, func(i, j int) bool {
		return compareRolloutHistoryProvenance(out.Provenance[i], out.Provenance[j]) < 0
	})

	out.Revisions = make([]RolloutHistoryRevision, 0, len(c.Revisions))
	for _, value := range c.Revisions {
		if revision, ok := canonicalRolloutHistoryRevision(value); ok {
			out.Revisions = append(out.Revisions, revision)
		}
	}
	sort.Slice(out.Revisions, func(i, j int) bool {
		return compareRolloutHistoryRevisions(out.Revisions[i], out.Revisions[j]) < 0
	})

	status := (RolloutStatusContent{Issues: c.StatusIssues}).Canonical()
	out.StatusIssues = status.Issues
	out.Issues = make([]RolloutHistoryIssue, 0, len(c.Issues))
	for _, value := range c.Issues {
		if issue, ok := canonicalRolloutHistoryIssue(value); ok {
			out.Issues = append(out.Issues, issue)
		}
	}
	sort.Slice(out.Issues, func(i, j int) bool {
		return compareRolloutHistoryIssues(out.Issues[i], out.Issues[j]) < 0
	})
	out.Summary.ActiveRuns = 0
	out.Summary.RetainedRuns = 0
	for _, run := range out.Runs {
		if run.Slot == RolloutHistoryRunActive {
			out.Summary.ActiveRuns++
		} else {
			out.Summary.RetainedRuns++
		}
	}
	out.Summary.Revisions = len(out.Revisions)
	return out
}

func canonicalRolloutHistoryRun(
	value RolloutHistoryRun,
	subjectName string,
) (RolloutHistoryRun, bool) {
	if value.Slot != RolloutHistoryRunActive && value.Slot != RolloutHistoryRunLast {
		return RolloutHistoryRun{}, false
	}
	if value.Slot == RolloutHistoryRunActive {
		if value.Outcome != RolloutHistoryRunActiveState || !validRolloutHistoryRunID(value.RunID, subjectName) {
			return RolloutHistoryRun{}, false
		}
	} else if value.Outcome != RolloutHistoryRunCompleted &&
		value.Outcome != RolloutHistoryRunRolledBack &&
		value.Outcome != RolloutHistoryRunSuperseded {
		return RolloutHistoryRun{}, false
	}
	out := value
	if value.Slot == RolloutHistoryRunLast {
		out.RunID = ""
	}
	out.OpenedAt = canonicalRolloutHistoryTime(value.OpenedAt)
	out.PinnedAt = canonicalRolloutHistoryTime(value.PinnedAt)
	out.ClosedAt = canonicalRolloutHistoryTime(value.ClosedAt)
	if out.GroupCount < 0 || out.GroupCount > 3 {
		out.GroupCount = 0
	}
	out.Targets = make([]RolloutHistoryTarget, 0, len(value.Targets))
	for _, target := range value.Targets {
		component := canonicalRolloutComponentType(target.Component)
		if component == "" || !rolloutHistoryRevisionPattern.MatchString(target.RevisionHash) {
			continue
		}
		out.Targets = append(out.Targets, RolloutHistoryTarget{
			Component: component, RevisionHash: target.RevisionHash,
		})
	}
	sort.Slice(out.Targets, func(i, j int) bool {
		return compareRolloutHistoryTargets(out.Targets[i], out.Targets[j]) < 0
	})
	return out, true
}

func validRolloutHistoryRunID(value, subjectName string) bool {
	separator := strings.LastIndexByte(value, '-')
	if separator < 1 || !rolloutHistoryRunHashPattern.MatchString(value[separator+1:]) ||
		len(utilvalidation.IsDNS1123Subdomain(value[:separator])) != 0 {
		return false
	}
	return subjectName == "" || value[:separator] == subjectName
}

func canonicalRolloutHistoryProvenance(
	value RolloutHistoryProvenance,
) (RolloutHistoryProvenance, bool) {
	if (value.View != RolloutHistoryViewActive && value.View != RolloutHistoryViewLast &&
		value.View != RolloutHistoryViewCurrent) || value.Group < 0 || value.Group > 2 ||
		(value.Source != RolloutPlanSourceInline && value.Source != RolloutPlanSourcePolicy) ||
		(value.PortableDigest == "" && value.View != RolloutHistoryViewCurrent) ||
		(value.PortableDigest != "" && !rolloutPortableDigestPattern.MatchString(value.PortableDigest)) {
		return RolloutHistoryProvenance{}, false
	}
	out := value
	out.Policy = canonicalRolloutPolicyReference(value.Policy)
	out.ShadowedPolicy = canonicalRolloutPolicyReference(value.ShadowedPolicy)
	if value.Source == RolloutPlanSourcePolicy && out.Policy == nil {
		return RolloutHistoryProvenance{}, false
	}
	if value.Source == RolloutPlanSourceInline {
		out.Policy = nil
	}
	out.DigestEvidence = rolloutHistoryDigestEvidence(value)
	out.ObservedAt = canonicalRolloutHistoryTime(value.ObservedAt)
	return out, true
}

func rolloutHistoryDigestEvidence(value RolloutHistoryProvenance) EvidenceLevel {
	if value.View == RolloutHistoryViewActive {
		return EvidenceComputed
	}
	if value.View == RolloutHistoryViewLast {
		return EvidenceReported
	}
	if value.PortableDigest == "" {
		return EvidenceUnavailable
	}
	if value.Source == RolloutPlanSourceInline {
		return EvidenceComputed
	}
	return EvidenceReported
}

func canonicalRolloutHistoryRevision(
	value RolloutHistoryRevision,
) (RolloutHistoryRevision, bool) {
	component := canonicalRolloutComponentType(value.Component)
	if component == "" || !rolloutHistoryRevisionPattern.MatchString(value.RevisionHash) {
		return RolloutHistoryRevision{}, false
	}
	role := canonicalRolloutRevisionRole(value.Role)
	if role == RolloutRevisionOther {
		return RolloutHistoryRevision{}, false
	}
	return RolloutHistoryRevision{
		Component: component, Role: role, RevisionHash: value.RevisionHash,
		Phase: canonicalRolloutPhase(value.Phase),
	}, true
}

func canonicalRolloutHistoryIssue(
	value RolloutHistoryIssue,
) (RolloutHistoryIssue, bool) {
	switch value.Code {
	case RolloutHistoryIssueRunStatusUnavailable,
		RolloutHistoryIssueActiveRunMalformed,
		RolloutHistoryIssueLastRunMalformed,
		RolloutHistoryIssueCurrentResolutionMissing,
		RolloutHistoryIssueCurrentResolutionMalformed:
	default:
		return RolloutHistoryIssue{}, false
	}
	out := value
	switch out.View {
	case "", RolloutHistoryViewActive, RolloutHistoryViewLast, RolloutHistoryViewCurrent:
	default:
		out.View = ""
	}
	out.Component = canonicalRolloutComponentType(out.Component)
	if out.Group != nil {
		group := *out.Group
		if group < 0 || group > 2 {
			out.Group = nil
		} else {
			out.Group = &group
		}
	}
	return out, true
}

func canonicalRolloutHistoryTime(value *time.Time) *time.Time {
	if value == nil || value.IsZero() {
		return nil
	}
	result := value.UTC()
	return &result
}

func canonicalRolloutHistoryState(value RolloutHistoryState) RolloutHistoryState {
	switch value {
	case RolloutHistoryStateUnknown, RolloutHistoryStateUnavailable,
		RolloutHistoryStateEmpty, RolloutHistoryStateReported,
		RolloutHistoryStatePartial:
		return value
	default:
		return RolloutHistoryStateUnknown
	}
}

// Table derives the bounded compact human view. Every cell has a fixed
// display-width domain; with tabwriter's three-column padding the full line is
// at most 79 display columns.
func (r RolloutHistoryReport) Table() report.Table {
	canonical := r.Canonical()
	table := report.Table{Headers: []string{"TYPE", "STATE", "COMP", "IDENT", "TIME", "DETAIL", "ISS"}}
	issueCount := len(canonical.Content.Issues) + len(canonical.Content.StatusIssues)
	table.Rows = append(table.Rows, []string{
		"WINDOW", compactRolloutHistoryState(string(canonical.Content.Summary.State)), "-", "-", "-",
		"bounded", compactRolloutHistoryIssueCount(issueCount),
	})
	for _, run := range canonical.Content.Runs {
		detail := fmt.Sprintf("G:%d", run.GroupCount)
		if run.Slot == RolloutHistoryRunActive {
			detail = fmt.Sprintf("G:%d T:%d", run.GroupCount, len(run.Targets))
		}
		table.Rows = append(table.Rows, []string{
			compactRolloutHistoryType(strings.ToUpper(string(run.Slot))),
			compactRolloutHistoryState(string(run.Outcome)), "-",
			compactRolloutHistoryRunID(run.RunID),
			compactRolloutHistoryTime(runTime(run)),
			printers.BoundedCell(detail, 10),
			compactRolloutHistoryIssueCount(issueCount),
		})
		if run.Slot == RolloutHistoryRunActive {
			for _, target := range run.Targets {
				table.Rows = append(table.Rows, []string{
					"TARGET", "Active", printers.BoundedCell(string(target.Component), 7), target.RevisionHash,
					"-", "run-target", compactRolloutHistoryIssueCount(issueCount),
				})
			}
		}
	}
	for _, provenance := range canonical.Content.Provenance {
		table.Rows = append(table.Rows, []string{
			compactRolloutHistoryView(provenance.View),
			compactRolloutHistoryState(string(provenance.Source)),
			fmt.Sprintf("g%d", provenance.Group),
			orDash(strings.TrimPrefix(provenance.PortableDigest, "rp1:")),
			compactRolloutHistoryTime(provenance.ObservedAt),
			compactRolloutHistoryPolicy(provenance),
			compactRolloutHistoryIssueCount(issueCount),
		})
	}
	for _, revision := range canonical.Content.Revisions {
		table.Rows = append(table.Rows, []string{
			"REV", compactRolloutHistoryPhase(revision.Phase),
			printers.BoundedCell(string(revision.Component), 7), revision.RevisionHash,
			"-", printers.BoundedCell(string(revision.Role), 10),
			compactRolloutHistoryIssueCount(issueCount),
		})
	}
	return table
}

// WideTable returns every full, safe allowlisted field without serializing
// the pinned rollout plan or any arbitrary status text.
func (r RolloutHistoryReport) WideTable() report.Table {
	canonical := r.Canonical()
	table := report.Table{Headers: []string{
		"WINDOW", "RECORD", "STATE", "RUN-ID", "COMPONENT", "REVISION", "ROLE",
		"OPENED", "PINNED", "CLOSED", "VIEW", "GROUP", "SOURCE", "POLICY",
		"POLICY-GENERATION", "DIGEST", "SHADOWED-POLICY", "SHADOWED-DIGEST", "PHASE",
		"DIGEST-EVIDENCE", "ISSUES",
	}}
	issues := wideRolloutHistoryIssues(canonical.Content)
	for _, run := range canonical.Content.Runs {
		row := emptyRolloutHistoryWideRow(canonical.Content.Summary.Completeness, issues)
		row[1], row[2], row[3] = string(run.Slot), string(run.Outcome), orDash(run.RunID)
		row[7], row[8], row[9] = formatRolloutHistoryTime(run.OpenedAt), formatRolloutHistoryTime(run.PinnedAt), formatRolloutHistoryTime(run.ClosedAt)
		table.Rows = append(table.Rows, row)
		for _, target := range run.Targets {
			targetRow := emptyRolloutHistoryWideRow(canonical.Content.Summary.Completeness, issues)
			targetRow[1], targetRow[2] = string(run.Slot), string(run.Outcome)
			targetRow[4], targetRow[5], targetRow[6] = string(target.Component), target.RevisionHash, "RunTarget"
			table.Rows = append(table.Rows, targetRow)
		}
	}
	for _, provenance := range canonical.Content.Provenance {
		row := emptyRolloutHistoryWideRow(canonical.Content.Summary.Completeness, issues)
		row[10], row[11], row[12], row[15] = string(provenance.View), fmt.Sprintf("%d", provenance.Group), string(provenance.Source), provenance.PortableDigest
		row[19] = string(provenance.DigestEvidence)
		row[8] = formatRolloutHistoryTime(provenance.ObservedAt)
		if provenance.Policy != nil {
			row[13] = rolloutHistoryPolicyDisplay(provenance.Policy)
			if provenance.Policy.Generation > 0 {
				row[14] = fmt.Sprintf("%d", provenance.Policy.Generation)
			}
		}
		if provenance.ShadowedPolicy != nil {
			row[16] = rolloutHistoryPolicyDisplay(provenance.ShadowedPolicy)
			row[17] = orDash(provenance.ShadowedPolicy.Digest)
		}
		table.Rows = append(table.Rows, row)
	}
	for _, revision := range canonical.Content.Revisions {
		row := emptyRolloutHistoryWideRow(canonical.Content.Summary.Completeness, issues)
		row[1], row[2] = "CurrentStatus", string(canonical.Content.Summary.CurrentState)
		row[4], row[5], row[6], row[18] = string(revision.Component), revision.RevisionHash, string(revision.Role), string(revision.Phase)
		table.Rows = append(table.Rows, row)
	}
	if len(table.Rows) == 0 {
		row := emptyRolloutHistoryWideRow(canonical.Content.Summary.Completeness, issues)
		row[1], row[2] = "Window", string(canonical.Content.Summary.State)
		table.Rows = append(table.Rows, row)
	}
	return table
}

func emptyRolloutHistoryWideRow(completeness RolloutHistoryCompleteness, issues string) []string {
	row := make([]string, 21)
	for i := range row {
		row[i] = "-"
	}
	row[0], row[20] = string(completeness), issues
	return row
}

func runTime(run RolloutHistoryRun) *time.Time {
	if run.Slot == RolloutHistoryRunLast {
		return run.ClosedAt
	}
	return run.OpenedAt
}

func compactRolloutHistoryType(value string) string {
	return printers.BoundedCell(value, 6)
}

func compactRolloutHistoryState(value string) string {
	return printers.BoundedCell(value, 10)
}

func compactRolloutHistoryPhase(value RolloutPhase) string {
	switch value {
	case RolloutPhaseBlueGreenStandby:
		return "BGStandby"
	case RolloutPhaseRollingBack:
		return "RollingBk"
	case RolloutPhaseScalingDown:
		return "ScaleDown"
	case RolloutPhaseAwaitingNextComponent:
		return "AwaitNext"
	default:
		return compactRolloutHistoryState(string(value))
	}
}

func compactRolloutHistoryRunID(value string) string {
	if value == "" {
		return "-"
	}
	separator := strings.LastIndexByte(value, '-')
	if separator >= 0 {
		return value[separator+1:]
	}
	return printers.BoundedMiddleCell(value, 14)
}

func compactRolloutHistoryView(value RolloutHistoryView) string {
	switch value {
	case RolloutHistoryViewActive:
		return "PROV-A"
	case RolloutHistoryViewLast:
		return "PROV-L"
	case RolloutHistoryViewCurrent:
		return "PROV-C"
	default:
		return "PROV-?"
	}
}

func compactRolloutHistoryPolicy(value RolloutHistoryProvenance) string {
	policy := value.Policy
	if value.Source == RolloutPlanSourceInline {
		policy = value.ShadowedPolicy
	}
	if policy == nil {
		return "-"
	}
	return compactRolloutHistoryIdentity(policy.Name, 10)
}

func compactRolloutHistoryIdentity(value string, width int) string {
	clean := printers.BoundedMiddleCell(value, 1024)
	if printers.BoundedMiddleCell(clean, width) == clean {
		return clean
	}
	digest := sha256.Sum256([]byte(clean))
	fingerprint := hex.EncodeToString(digest[:])[:8]
	if width <= len(fingerprint) {
		return fingerprint[:width]
	}
	prefixWidth := width - len(fingerprint) - 1
	// Policy names are validated DNS-1123 identifiers, so bytes are terminal
	// columns here and slicing cannot split a grapheme.
	prefix := clean[:min(len(clean), prefixWidth)]
	return prefix + "#" + fingerprint
}

func compactRolloutHistoryTime(value *time.Time) string {
	if value == nil {
		return "-"
	}
	return value.UTC().Format("01-02T15:04")
}

func formatRolloutHistoryTime(value *time.Time) string {
	if value == nil {
		return "-"
	}
	return value.UTC().Format(time.RFC3339)
}

func compactRolloutHistoryIssueCount(count int) string {
	if count > 99 {
		return "99+"
	}
	return fmt.Sprintf("%d", count)
}

func rolloutHistoryPolicyDisplay(value *RolloutPolicyReference) string {
	if value == nil {
		return "-"
	}
	return value.Kind + "/" + value.Name
}

func wideRolloutHistoryIssues(content RolloutHistoryContent) string {
	values := make([]string, 0, len(content.Issues)+len(content.StatusIssues))
	for _, issue := range content.Issues {
		values = append(values, string(issue.Code))
	}
	for _, issue := range content.StatusIssues {
		values = append(values, "Status:"+string(issue.Code))
	}
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ",")
}

func compareRolloutHistorySources(a, b RolloutSourceReference) int {
	for _, result := range []int{
		cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Name, b.Name), cmp.Compare(a.UID, b.UID),
		cmp.Compare(a.Generation, b.Generation), cmp.Compare(a.Evidence, b.Evidence),
		a.CollectedAt.Compare(b.CollectedAt),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRolloutHistoryRuns(a, b RolloutHistoryRun) int {
	for _, result := range []int{
		cmp.Compare(rolloutHistoryRunSlotRank(a.Slot), rolloutHistoryRunSlotRank(b.Slot)),
		cmp.Compare(a.Outcome, b.Outcome), cmp.Compare(a.RunID, b.RunID),
		compareOptionalTime(a.OpenedAt, b.OpenedAt), compareOptionalTime(a.PinnedAt, b.PinnedAt),
		compareOptionalTime(a.ClosedAt, b.ClosedAt), cmp.Compare(a.GroupCount, b.GroupCount),
		compareRolloutHistoryTargetSlices(a.Targets, b.Targets),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func rolloutHistoryRunSlotRank(value RolloutHistoryRunSlot) int {
	if value == RolloutHistoryRunActive {
		return 0
	}
	return 1
}

func compareRolloutHistoryTargetSlices(a, b []RolloutHistoryTarget) int {
	for i := 0; i < min(len(a), len(b)); i++ {
		if result := compareRolloutHistoryTargets(a[i], b[i]); result != 0 {
			return result
		}
	}
	return cmp.Compare(len(a), len(b))
}

func compareRolloutHistoryTargets(a, b RolloutHistoryTarget) int {
	for _, result := range []int{
		cmp.Compare(rolloutComponentOrder(a.Component), rolloutComponentOrder(b.Component)),
		cmp.Compare(a.Component, b.Component), cmp.Compare(a.RevisionHash, b.RevisionHash),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRolloutHistoryProvenance(a, b RolloutHistoryProvenance) int {
	for _, result := range []int{
		cmp.Compare(rolloutHistoryViewRank(a.View), rolloutHistoryViewRank(b.View)),
		cmp.Compare(a.Group, b.Group), cmp.Compare(a.Source, b.Source),
		cmp.Compare(a.PortableDigest, b.PortableDigest),
		cmp.Compare(rolloutHistoryPolicySortKey(a.Policy), rolloutHistoryPolicySortKey(b.Policy)),
		cmp.Compare(rolloutHistoryPolicySortKey(a.ShadowedPolicy), rolloutHistoryPolicySortKey(b.ShadowedPolicy)),
		compareOptionalTime(a.ObservedAt, b.ObservedAt),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func rolloutHistoryViewRank(value RolloutHistoryView) int {
	switch value {
	case RolloutHistoryViewActive:
		return 0
	case RolloutHistoryViewLast:
		return 1
	default:
		return 2
	}
}

func rolloutHistoryPolicySortKey(value *RolloutPolicyReference) string {
	if value == nil {
		return ""
	}
	return strings.Join([]string{
		value.Kind, value.Name, value.Progression, fmt.Sprintf("%020d", value.Generation),
		value.Digest, string(value.Evidence),
	}, "\x00")
}

func compareRolloutHistoryRevisions(a, b RolloutHistoryRevision) int {
	for _, result := range []int{
		cmp.Compare(rolloutComponentOrder(a.Component), rolloutComponentOrder(b.Component)),
		cmp.Compare(a.Component, b.Component),
		cmp.Compare(rolloutRevisionRoleOrder(a.Role), rolloutRevisionRoleOrder(b.Role)),
		cmp.Compare(a.Role, b.Role), cmp.Compare(a.RevisionHash, b.RevisionHash),
		cmp.Compare(a.Phase, b.Phase),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func compareRolloutHistoryIssues(a, b RolloutHistoryIssue) int {
	for _, result := range []int{
		cmp.Compare(a.Code, b.Code), cmp.Compare(rolloutHistoryViewRank(a.View), rolloutHistoryViewRank(b.View)),
		compareOptionalInt(a.Group, b.Group), cmp.Compare(rolloutComponentOrder(a.Component), rolloutComponentOrder(b.Component)),
		cmp.Compare(a.Component, b.Component),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func rolloutComponentOrder(value RuntimeComponentType) int {
	return slices.Index([]RuntimeComponentType{
		RuntimeComponentEngine, RuntimeComponentDecoder, RuntimeComponentRouter,
	}, value)
}
