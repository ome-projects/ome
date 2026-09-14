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
	RolloutHistoryIssueActiveTargetUnavailable    RolloutHistoryIssueCode = "ActiveTargetUnavailable"
	RolloutHistoryIssueActiveTargetMalformed      RolloutHistoryIssueCode = "ActiveTargetMalformed"
	RolloutHistoryIssueLastRunMalformed           RolloutHistoryIssueCode = "LastRunMalformed"
	RolloutHistoryIssueRunChronologyMalformed     RolloutHistoryIssueCode = "RunChronologyMalformed"
	RolloutHistoryIssueCurrentResolutionMissing   RolloutHistoryIssueCode = "CurrentResolutionMissing"
	RolloutHistoryIssueCurrentResolutionMalformed RolloutHistoryIssueCode = "CurrentResolutionMalformed"
)

// RolloutHistoryRevisionReady identifies the newest revision whose pods
// reached Ready. It is intentionally distinct from a run's pinned target.
const RolloutHistoryRevisionReady RolloutRevisionRole = "Ready"

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
	RevisionHash string               `json:"revisionHash,omitempty"`
	Evidence     EvidenceLevel        `json:"evidence"`
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

// RolloutHistorySourceReference is the minimal source identity needed to
// qualify this bounded report. Object UIDs are deliberately not part of the
// rollout-history contract.
type RolloutHistorySourceReference struct {
	Kind        RolloutSourceKind `json:"kind"`
	Namespace   string            `json:"namespace,omitempty"`
	Name        string            `json:"name"`
	Generation  int64             `json:"generation,omitempty"`
	Evidence    EvidenceLevel     `json:"evidence"`
	CollectedAt time.Time         `json:"collectedAt"`
}

// RolloutHistoryReport is the stable CLI-owned v1alpha1 report.
type RolloutHistoryReport struct {
	APIVersion  string                          `json:"apiVersion"`
	Kind        string                          `json:"kind"`
	Metadata    Metadata                        `json:"metadata"`
	CollectedAt time.Time                       `json:"collectedAt"`
	Sources     []RolloutHistorySourceReference `json:"sources"`
	Content     RolloutHistoryContent           `json:"content"`
	Warnings    []RolloutWarning                `json:"warnings"`
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
		CollectedAt: clock.Now().UTC(), Sources: []RolloutHistorySourceReference{},
		Content: content, Warnings: []RolloutWarning{},
	}).Canonical()
}

// Canonical returns a deterministic deep copy.
func (r RolloutHistoryReport) Canonical() RolloutHistoryReport {
	out := r
	out.APIVersion = APIVersion
	out.Kind = RolloutHistoryReportKind
	out.CollectedAt = r.CollectedAt.UTC()
	out.Sources = make([]RolloutHistorySourceReference, 0, len(r.Sources))
	for _, value := range r.Sources {
		if value.Kind != RolloutSourceInferenceService || value.Generation < 0 ||
			len(utilvalidation.IsDNS1123Label(value.Namespace)) != 0 ||
			len(utilvalidation.IsDNS1123Subdomain(value.Name)) != 0 {
			continue
		}
		value.Evidence = canonicalRolloutEvidenceLevel(value.Evidence)
		if value.CollectedAt.IsZero() {
			value.CollectedAt = out.CollectedAt
		} else {
			value.CollectedAt = value.CollectedAt.UTC()
		}
		out.Sources = append(out.Sources, value)
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
	if slices.ContainsFunc(out.Content.Issues, func(issue RolloutHistoryIssue) bool {
		return issue.Code == RolloutHistoryIssueRunChronologyMalformed
	}) && !slices.Contains(out.Warnings, RolloutWarning{Code: WarningPartialData}) {
		out.Warnings = append(out.Warnings, RolloutWarning{Code: WarningPartialData})
		sort.Slice(out.Warnings, func(i, j int) bool { return out.Warnings[i].Code < out.Warnings[j].Code })
	}
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
	if rolloutHistoryChronologyConflicts(out.Runs) {
		out.Runs = slices.DeleteFunc(out.Runs, func(run RolloutHistoryRun) bool {
			return run.Slot == RolloutHistoryRunLast
		})
		out.Provenance = slices.DeleteFunc(out.Provenance, func(value RolloutHistoryProvenance) bool {
			return value.View == RolloutHistoryViewLast
		})
		chronology := RolloutHistoryIssue{
			Code: RolloutHistoryIssueRunChronologyMalformed, View: RolloutHistoryViewLast,
		}
		if !slices.Contains(out.Issues, chronology) {
			out.Issues = append(out.Issues, chronology)
			sort.Slice(out.Issues, func(i, j int) bool {
				return compareRolloutHistoryIssues(out.Issues[i], out.Issues[j]) < 0
			})
		}
		out.Summary.State = RolloutHistoryStatePartial
	}
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

func rolloutHistoryChronologyConflicts(runs []RolloutHistoryRun) bool {
	var activeOpened, lastClosed *time.Time
	for index := range runs {
		switch runs[index].Slot {
		case RolloutHistoryRunActive:
			activeOpened = runs[index].OpenedAt
		case RolloutHistoryRunLast:
			lastClosed = runs[index].ClosedAt
		}
	}
	return activeOpened != nil && lastClosed != nil && lastClosed.After(*activeOpened)
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
	componentCounts := make(map[RuntimeComponentType]int, len(value.Targets))
	for _, target := range value.Targets {
		if component := canonicalRolloutComponentType(target.Component); component != "" {
			componentCounts[component]++
		}
	}
	out.Targets = make([]RolloutHistoryTarget, 0, len(value.Targets))
	for _, target := range value.Targets {
		component := canonicalRolloutComponentType(target.Component)
		if component == "" || componentCounts[component] != 1 || (target.RevisionHash != "" &&
			!rolloutHistoryRevisionPattern.MatchString(target.RevisionHash)) {
			continue
		}
		evidence := EvidenceUnavailable
		if target.RevisionHash != "" {
			evidence = EvidenceReported
		}
		out.Targets = append(out.Targets, RolloutHistoryTarget{
			Component: component, RevisionHash: target.RevisionHash, Evidence: evidence,
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
	role := canonicalRolloutHistoryRevisionRole(value.Role)
	if role == RolloutRevisionOther {
		return RolloutHistoryRevision{}, false
	}
	return RolloutHistoryRevision{
		Component: component, Role: role, RevisionHash: value.RevisionHash,
		Phase: canonicalRolloutPhase(value.Phase),
	}, true
}

func canonicalRolloutHistoryRevisionRole(role RolloutRevisionRole) RolloutRevisionRole {
	switch role {
	case RolloutRevisionCurrent, RolloutHistoryRevisionReady, RolloutRevisionPrevious:
		return role
	default:
		return RolloutRevisionOther
	}
}

func canonicalRolloutHistoryIssue(
	value RolloutHistoryIssue,
) (RolloutHistoryIssue, bool) {
	switch value.Code {
	case RolloutHistoryIssueRunStatusUnavailable,
		RolloutHistoryIssueActiveRunMalformed,
		RolloutHistoryIssueActiveTargetUnavailable,
		RolloutHistoryIssueActiveTargetMalformed,
		RolloutHistoryIssueLastRunMalformed,
		RolloutHistoryIssueRunChronologyMalformed,
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
	table.Rows = append(table.Rows, []string{
		"CURR", compactRolloutHistoryState(string(canonical.Content.Summary.CurrentState)), "-",
		printers.BoundedCell(string(canonical.Content.Summary.CurrentEvidence), 12), "-",
		compactRolloutHistoryEpoch(canonical.Content.Summary.CurrentEpoch),
		compactRolloutHistoryIssueCount(issueCount),
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
					"TARGET", compactRolloutHistoryState(string(target.Evidence)),
					printers.BoundedCell(string(target.Component), 7), orDash(target.RevisionHash),
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
		"ROW", "NAMESPACE", "NAME", "COLLECTED-AT", "COMPLETENESS", "WINDOW-STATE",
		"CURRENT-STATE", "CURRENT-EVIDENCE", "CURRENT-EPOCH", "ACTIVE-RUNS",
		"RETAINED-RUNS", "REVISIONS", "SOURCE-KIND", "SOURCE-NAMESPACE", "SOURCE-NAME",
		"SOURCE-GENERATION", "SOURCE-EVIDENCE", "SOURCE-COLLECTED-AT", "RECORD", "OUTCOME",
		"RUN-ID", "GROUP-COUNT", "COMPONENT", "REVISION", "REVISION-EVIDENCE", "ROLE",
		"PHASE", "OPENED-AT", "PINNED-AT", "CLOSED-AT", "OBSERVED-AT", "VIEW", "GROUP",
		"SOURCE", "POLICY", "POLICY-PROGRESSION", "POLICY-GENERATION", "POLICY-EVIDENCE",
		"POLICY-DIGEST", "DIGEST", "DIGEST-EVIDENCE", "SHADOWED-POLICY", "SHADOWED-PROGRESSION",
		"SHADOWED-GENERATION", "SHADOWED-EVIDENCE", "SHADOWED-DIGEST", "ISSUE-CODE", "ISSUE-VIEW",
		"ISSUE-GROUP", "ISSUE-COMPONENT", "WARNING",
	}}
	summary := emptyRolloutHistoryWideRow()
	summary[historyWideRow] = "Summary"
	summary[historyWideNamespace], summary[historyWideName] = orDash(canonical.Metadata.Namespace), canonical.Metadata.Name
	summary[historyWideCollectedAt] = canonical.CollectedAt.Format(time.RFC3339)
	summary[historyWideCompleteness] = string(canonical.Content.Summary.Completeness)
	summary[historyWideWindowState] = string(canonical.Content.Summary.State)
	summary[historyWideCurrentState] = string(canonical.Content.Summary.CurrentState)
	summary[historyWideCurrentEvidence] = string(canonical.Content.Summary.CurrentEvidence)
	summary[historyWideCurrentEpoch] = string(canonical.Content.Summary.CurrentEpoch)
	summary[historyWideActiveRuns] = fmt.Sprintf("%d", canonical.Content.Summary.ActiveRuns)
	summary[historyWideRetainedRuns] = fmt.Sprintf("%d", canonical.Content.Summary.RetainedRuns)
	summary[historyWideRevisionCount] = fmt.Sprintf("%d", canonical.Content.Summary.Revisions)
	table.Rows = append(table.Rows, summary)
	for _, source := range canonical.Sources {
		row := emptyRolloutHistoryWideRow()
		row[historyWideRow] = "Source"
		row[historyWideSourceKind], row[historyWideSourceNamespace], row[historyWideSourceName] =
			string(source.Kind), orDash(source.Namespace), source.Name
		if source.Generation > 0 {
			row[historyWideSourceGeneration] = fmt.Sprintf("%d", source.Generation)
		}
		row[historyWideSourceEvidence] = string(source.Evidence)
		row[historyWideSourceCollectedAt] = source.CollectedAt.Format(time.RFC3339)
		table.Rows = append(table.Rows, row)
	}
	for _, run := range canonical.Content.Runs {
		row := emptyRolloutHistoryWideRow()
		row[historyWideRow], row[historyWideRecord], row[historyWideOutcome] = "Run", string(run.Slot), string(run.Outcome)
		row[historyWideRunID], row[historyWideGroupCount] = orDash(run.RunID), fmt.Sprintf("%d", run.GroupCount)
		row[historyWideOpenedAt], row[historyWidePinnedAt], row[historyWideClosedAt] =
			formatRolloutHistoryTime(run.OpenedAt), formatRolloutHistoryTime(run.PinnedAt), formatRolloutHistoryTime(run.ClosedAt)
		table.Rows = append(table.Rows, row)
		for _, target := range run.Targets {
			targetRow := emptyRolloutHistoryWideRow()
			targetRow[historyWideRow], targetRow[historyWideRecord] = "Target", string(run.Slot)
			targetRow[historyWideComponent], targetRow[historyWideRevision] = string(target.Component), orDash(target.RevisionHash)
			targetRow[historyWideRevisionEvidence], targetRow[historyWideRole] = string(target.Evidence), "RunTarget"
			table.Rows = append(table.Rows, targetRow)
		}
	}
	for _, provenance := range canonical.Content.Provenance {
		row := emptyRolloutHistoryWideRow()
		row[historyWideRow], row[historyWideView] = "Provenance", string(provenance.View)
		row[historyWideGroup], row[historyWidePlanSource] = fmt.Sprintf("%d", provenance.Group), string(provenance.Source)
		row[historyWideDigest], row[historyWideDigestEvidence] = orDash(provenance.PortableDigest), string(provenance.DigestEvidence)
		row[historyWideObservedAt] = formatRolloutHistoryTime(provenance.ObservedAt)
		if provenance.Policy != nil {
			row[historyWidePolicy] = rolloutHistoryPolicyDisplay(provenance.Policy)
			row[historyWidePolicyProgression] = orDash(provenance.Policy.Progression)
			if provenance.Policy.Generation > 0 {
				row[historyWidePolicyGeneration] = fmt.Sprintf("%d", provenance.Policy.Generation)
			}
			row[historyWidePolicyEvidence] = string(provenance.Policy.Evidence)
			row[historyWidePolicyDigest] = orDash(provenance.Policy.Digest)
		}
		if provenance.ShadowedPolicy != nil {
			row[historyWideShadowedPolicy] = rolloutHistoryPolicyDisplay(provenance.ShadowedPolicy)
			row[historyWideShadowedProgression] = orDash(provenance.ShadowedPolicy.Progression)
			if provenance.ShadowedPolicy.Generation > 0 {
				row[historyWideShadowedGeneration] = fmt.Sprintf("%d", provenance.ShadowedPolicy.Generation)
			}
			row[historyWideShadowedEvidence] = string(provenance.ShadowedPolicy.Evidence)
			row[historyWideShadowedDigest] = orDash(provenance.ShadowedPolicy.Digest)
		}
		table.Rows = append(table.Rows, row)
	}
	for _, revision := range canonical.Content.Revisions {
		row := emptyRolloutHistoryWideRow()
		row[historyWideRow], row[historyWideRecord] = "Revision", "CurrentStatus"
		row[historyWideComponent], row[historyWideRevision] = string(revision.Component), revision.RevisionHash
		row[historyWideRevisionEvidence] = string(canonical.Content.Summary.CurrentEvidence)
		row[historyWideRole], row[historyWidePhase] = string(revision.Role), string(revision.Phase)
		table.Rows = append(table.Rows, row)
	}
	for _, issue := range canonical.Content.StatusIssues {
		row := emptyRolloutHistoryWideRow()
		row[historyWideRow], row[historyWideIssueCode] = "StatusIssue", string(issue.Code)
		row[historyWideIssueGroup] = formatRolloutHistoryGroup(issue.Group)
		row[historyWideIssueComponent] = orDash(string(issue.Component))
		table.Rows = append(table.Rows, row)
	}
	for _, issue := range canonical.Content.Issues {
		row := emptyRolloutHistoryWideRow()
		row[historyWideRow], row[historyWideIssueCode] = "HistoryIssue", string(issue.Code)
		row[historyWideIssueView] = orDash(string(issue.View))
		row[historyWideIssueGroup] = formatRolloutHistoryGroup(issue.Group)
		row[historyWideIssueComponent] = orDash(string(issue.Component))
		table.Rows = append(table.Rows, row)
	}
	for _, warning := range canonical.Warnings {
		row := emptyRolloutHistoryWideRow()
		row[historyWideRow], row[historyWideWarning] = "Warning", string(warning.Code)
		table.Rows = append(table.Rows, row)
	}
	return table
}

const (
	historyWideRow = iota
	historyWideNamespace
	historyWideName
	historyWideCollectedAt
	historyWideCompleteness
	historyWideWindowState
	historyWideCurrentState
	historyWideCurrentEvidence
	historyWideCurrentEpoch
	historyWideActiveRuns
	historyWideRetainedRuns
	historyWideRevisionCount
	historyWideSourceKind
	historyWideSourceNamespace
	historyWideSourceName
	historyWideSourceGeneration
	historyWideSourceEvidence
	historyWideSourceCollectedAt
	historyWideRecord
	historyWideOutcome
	historyWideRunID
	historyWideGroupCount
	historyWideComponent
	historyWideRevision
	historyWideRevisionEvidence
	historyWideRole
	historyWidePhase
	historyWideOpenedAt
	historyWidePinnedAt
	historyWideClosedAt
	historyWideObservedAt
	historyWideView
	historyWideGroup
	historyWidePlanSource
	historyWidePolicy
	historyWidePolicyProgression
	historyWidePolicyGeneration
	historyWidePolicyEvidence
	historyWidePolicyDigest
	historyWideDigest
	historyWideDigestEvidence
	historyWideShadowedPolicy
	historyWideShadowedProgression
	historyWideShadowedGeneration
	historyWideShadowedEvidence
	historyWideShadowedDigest
	historyWideIssueCode
	historyWideIssueView
	historyWideIssueGroup
	historyWideIssueComponent
	historyWideWarning
	historyWideColumnCount
)

func emptyRolloutHistoryWideRow() []string {
	row := make([]string, historyWideColumnCount)
	for i := range row {
		row[i] = "-"
	}
	return row
}

func formatRolloutHistoryGroup(value *int) string {
	if value == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *value)
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

func compactRolloutHistoryEpoch(value RolloutEpochState) string {
	switch value {
	case RolloutEpochNotApplicable:
		return "N/A"
	case RolloutEpochUnverifiable:
		return "Unverified"
	default:
		return "Unknown"
	}
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

func compareRolloutHistorySources(a, b RolloutHistorySourceReference) int {
	for _, result := range []int{
		cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Namespace, b.Namespace),
		cmp.Compare(a.Name, b.Name), cmp.Compare(a.Generation, b.Generation),
		cmp.Compare(a.Evidence, b.Evidence),
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
		cmp.Compare(a.Evidence, b.Evidence),
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
		cmp.Compare(rolloutHistoryRevisionRoleOrder(a.Role), rolloutHistoryRevisionRoleOrder(b.Role)),
		cmp.Compare(a.Role, b.Role), cmp.Compare(a.RevisionHash, b.RevisionHash),
		cmp.Compare(a.Phase, b.Phase),
	} {
		if result != 0 {
			return result
		}
	}
	return 0
}

func rolloutHistoryRevisionRoleOrder(role RolloutRevisionRole) int {
	switch role {
	case RolloutRevisionCurrent:
		return 0
	case RolloutHistoryRevisionReady:
		return 1
	case RolloutRevisionPrevious:
		return 2
	default:
		return 3
	}
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
