package replay

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// rfc3339 matches the timestamps the engine embeds in event messages, so
// they can be rewritten to the scenario-relative form the rest of the
// trace uses.
var rfc3339 = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)

// normalizer replaces the identifiers nothing in the fixture names — the
// operation ids and object UIDs the engine and the apiserver mint at
// random — with a token derived from first-appearance order. Two runs of
// one scenario therefore render byte-identically while a changed write
// still shows up as a diff. Names the fixture does control, revisions
// above all, are printed as the engine sees them.
type normalizer struct {
	start time.Time
	ops   map[string]string
	uids  map[string]string
}

func newNormalizer(start time.Time) *normalizer {
	return &normalizer{
		start: start,
		ops:   map[string]string{},
		uids:  map[string]string{},
	}
}

func (n *normalizer) op(id string) string {
	if id == "" {
		return ""
	}
	if token, ok := n.ops[id]; ok {
		return token
	}
	token := fmt.Sprintf("op#%d", len(n.ops)+1)
	n.ops[id] = token
	return token
}

func (n *normalizer) uid(uid string) string {
	if uid == "" {
		return ""
	}
	if token, ok := n.uids[uid]; ok {
		return token
	}
	token := fmt.Sprintf("uid#%d", len(n.uids)+1)
	n.uids[uid] = token
	return token
}

// at renders an absolute instant relative to the scenario start.
func (n *normalizer) at(t time.Time) string {
	if t.IsZero() {
		return "nil"
	}
	d := t.Sub(n.start)
	if d < 0 {
		return "t-" + d.Abs().String()
	}
	return "t+" + d.String()
}

func (n *normalizer) atMeta(t *metav1.Time) string {
	if t == nil || t.IsZero() {
		return "nil"
	}
	return n.at(t.Time)
}

// text rewrites every registered identifier inside a free-form string —
// the event messages the engine composes from ids and revision names.
func (n *normalizer) text(s string) string {
	if s == "" {
		return s
	}
	for _, table := range []map[string]string{n.ops, n.uids} {
		for _, from := range sortedByLengthDesc(table) {
			s = strings.ReplaceAll(s, from, table[from])
		}
	}
	return rfc3339.ReplaceAllStringFunc(s, func(match string) string {
		parsed, err := time.Parse(time.RFC3339, match)
		if err != nil {
			return match
		}
		return n.at(parsed)
	})
}

// sortedByLengthDesc orders the replacement keys longest-first so an
// identifier that is a prefix of another cannot shadow it. The order is
// total, because map iteration order is not.
func sortedByLengthDesc(table map[string]string) []string {
	keys := make([]string, 0, len(table))
	for k := range table {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) > len(keys[j])
		}
		return keys[i] < keys[j]
	})
	return keys
}

// Trace accumulates the recorded effects of one run, already normalized.
type Trace struct {
	norm  *normalizer
	lines []string
	tick  int
	now   func() time.Time
}

func newTrace(norm *normalizer, now func() time.Time) *Trace {
	return &Trace{norm: norm, now: now}
}

// String renders the trace as the golden file stores it: one effect per
// line, newline-terminated.
func (t *Trace) String() string {
	if len(t.lines) == 0 {
		return ""
	}
	return strings.Join(t.lines, "\n") + "\n"
}

func (t *Trace) emit(format string, args ...any) {
	t.lines = append(t.lines, fmt.Sprintf("tick=%d ", t.tick)+fmt.Sprintf(format, args...))
}

func (t *Trace) beginTick(tick int, note string) {
	t.tick = tick
	if note != "" {
		t.emit("note %s", note)
	}
}

func (t *Trace) applied(ev TimelineEvent, detail string) {
	line := "apply " + ev.Key()
	if detail != "" {
		line += " " + detail
	}
	t.emit("%s now=%s", line, t.norm.at(t.now()))
}

// RowCommitted renders one resolved row write as the row's whole
// projected state, in a fixed field order. Every committed write reads
// the same way — the first one and the twentieth — so a golden line is
// the row at that instant rather than a delta the reader has to fold into
// the ones above it. A mutation that did not commit left no state to
// report, so its line carries the attempt and the verdict: the attempt
// itself is engine behavior worth locking.
func (t *Trace) RowCommitted(commit RowCommit) {
	verb := "write"
	if commit.Remove {
		verb = "remove"
	}
	if commit.Outcome != PreconditionOK {
		t.emit("row=%d %s no-op precondition=%s", commit.Index, verb, commit.Outcome)
		return
	}
	// A remove leaves nothing behind, so it reports the state it removed.
	row := commit.After
	if row == nil {
		row = commit.Before
	}
	if row == nil {
		t.emit("row=%d %s - precondition=%s", commit.Index, verb, commit.Outcome)
		return
	}
	t.emit("row=%d %s %s precondition=%s", commit.Index, verb,
		strings.Join(t.rowFields(*row), " "), commit.Outcome)
}

// RetryBlockCommitted renders one resolved retry-block write.
func (t *Trace) RetryBlockCommitted(commit RetryBlockCommit) {
	t.emit("retryblock target=%s %s %s", commit.TargetRevision, commit.Disposition,
		t.retryBlockDiff(commit.Before, commit.After))
}

func (t *Trace) event(eventType, reason, message string) {
	t.emit("event %s %s %s", eventType, reason, t.norm.text(message))
}

func (t *Trace) object(kind, verb, name, detail string) {
	line := fmt.Sprintf("%s %s name=%s", kind, verb, name)
	if detail != "" {
		line += " " + detail
	}
	t.emit("%s", line)
}

func (t *Trace) callback(format string, args ...any) {
	t.emit("%s", fmt.Sprintf(format, args...))
}

// rowFields renders a whole row in the order every trace line uses.
// Incarnation, phase, running revision, readySince and the operation are
// always present, an absent one as nil, so the shape of the line does not
// depend on which fields a particular write touched. The rest appear when
// the row carries them.
//
// ReadyPodCount, ScheduledPodCount and NodesOccupied are materialized only
// on the status publication path, which the driver does not run, and the
// workload package forbids reading them anywhere else. They are therefore
// outside what a row line can honestly report.
func (t *Trace) rowFields(row types.InstanceStatus) []string {
	parts := []string{
		"incarnation=" + fmt.Sprint(row.Incarnation),
		"phase=" + phaseOrNil(row.Phase),
		"runningRevision=" + orNil(row.RunningRevision),
	}
	if row.TargetRevision != "" {
		parts = append(parts, "targetRevision="+row.TargetRevision)
	}
	if row.ActiveOrdinal != 0 {
		parts = append(parts, "activeOrdinal="+fmt.Sprint(row.ActiveOrdinal))
	}
	if row.PodCount != 0 {
		parts = append(parts, "podCount="+fmt.Sprint(row.PodCount))
	}
	if row.ServingPodCount != 0 {
		parts = append(parts, "servingPodCount="+fmt.Sprint(row.ServingPodCount))
	}
	if row.AvailablePodCount != 0 {
		parts = append(parts, "availablePodCount="+fmt.Sprint(row.AvailablePodCount))
	}
	if row.Admitted {
		parts = append(parts, "admitted=true")
	}
	parts = append(parts,
		"readySince="+t.norm.atMeta(row.ReadySince),
		"op="+t.operation(row.Operation))
	if len(row.Conditions) > 0 {
		parts = append(parts, "conditions="+t.conditions(row.Conditions))
	}
	if row.LastFailure != nil {
		parts = append(parts, "lastFailure="+t.failure(row.LastFailure))
	}
	if len(row.Announced) > 0 {
		parts = append(parts, "announced=["+strings.Join(t.announcements(row.Announced), " ")+"]")
	}
	return parts
}

// announcements renders the row's once-only markers with their episode
// normalized, so a trace shows which episode a message was delivered in
// without carrying the minted operation id the normalizer exists to
// hide.
func (t *Trace) announcements(entries []string) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		reason, episode, found := strings.Cut(entry, "@")
		if !found {
			out = append(out, entry)
			continue
		}
		if strings.HasPrefix(episode, "#") {
			out = append(out, entry)
			continue
		}
		out = append(out, reason+"@"+t.norm.op(episode))
	}
	return out
}

func (t *Trace) operation(op *types.InstanceOperation) string {
	if op == nil {
		return "nil"
	}
	parts := []string{
		"type=" + string(op.Type),
		"step=" + orNil(op.Step),
	}
	if op.TargetRevision != "" {
		parts = append(parts, "target="+op.TargetRevision)
	}
	if op.Reason != "" {
		parts = append(parts, "reason="+t.norm.text(op.Reason))
	}
	if op.Waiting != "" {
		parts = append(parts, "waiting="+op.Waiting)
	}
	if op.Strategy != "" {
		parts = append(parts, "strategy="+string(op.Strategy))
	}
	if op.SurgeIndex != nil {
		parts = append(parts, "surgeIndex="+indexOrNil(op.SurgeIndex))
	}
	if op.RetryCount != 0 {
		parts = append(parts, "retryCount="+fmt.Sprint(op.RetryCount))
	}
	parts = append(parts,
		"started="+t.norm.at(op.StartedAt.Time),
		"lastProgressAt="+t.norm.at(op.LastProgressAt.Time),
		"deadline="+t.norm.at(op.Deadline.Time))
	return t.norm.op(op.ID) + "{" + strings.Join(parts, " ") + "}"
}

func (t *Trace) retryBlockDiff(before, after *types.RetryBlock) string {
	render := func(b *types.RetryBlock) string {
		if b == nil {
			return "nil"
		}
		parts := []string{
			"state=" + string(b.State),
			"attempts=" + fmt.Sprint(b.AttemptsStarted),
		}
		if b.NextRetryAt != nil {
			parts = append(parts, "nextRetryAt="+t.norm.atMeta(b.NextRetryAt))
		}
		if b.Reason != "" {
			parts = append(parts, "reason="+t.norm.text(b.Reason))
		}
		return "{" + strings.Join(parts, " ") + "}"
	}
	return render(before) + "→" + render(after)
}

func (t *Trace) conditions(conds []metav1.Condition) string {
	if len(conds) == 0 {
		return "nil"
	}
	parts := make([]string, 0, len(conds))
	for _, c := range conds {
		parts = append(parts, fmt.Sprintf("%s=%s(%s)", c.Type, c.Status, c.Reason))
	}
	sort.Strings(parts)
	return "[" + strings.Join(parts, ",") + "]"
}

func (t *Trace) failure(f *types.InstanceTermination) string {
	if f == nil {
		return "nil"
	}
	parts := []string{"pod=" + f.PodName, "reason=" + f.Reason}
	if f.ContainerName != "" {
		parts = append(parts, "container="+f.ContainerName)
	}
	if f.ExitCode != nil {
		parts = append(parts, fmt.Sprintf("exitCode=%d", *f.ExitCode))
	}
	if f.Message != "" {
		parts = append(parts, "message="+t.norm.text(f.Message))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

func phaseOrNil(p types.InstancePhase) string {
	if p == "" {
		return "nil"
	}
	return string(p)
}

func orNil(s string) string {
	if s == "" {
		return "nil"
	}
	return s
}

func indexOrNil(i *int32) string {
	if i == nil {
		return "nil"
	}
	return fmt.Sprint(*i)
}
