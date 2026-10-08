package replay

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// rfc3339 matches the timestamps the engines embed in event messages, so
// they can be rewritten to the scenario-relative form the trace uses.
var rfc3339 = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)

// normalizer renders instants relative to the scenario start. Every other
// identity the pass mints — run ids, target ids, revision names — is a
// deterministic function of the fixture and the injected clock and prints
// verbatim.
type normalizer struct {
	start time.Time
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

func (n *normalizer) atValue(t metav1.Time) string {
	if t.IsZero() {
		return "nil"
	}
	return n.at(t.Time)
}

// text rewrites every timestamp inside a free-form string.
func (n *normalizer) text(s string) string {
	return rfc3339.ReplaceAllStringFunc(s, func(match string) string {
		parsed, err := time.Parse(time.RFC3339, match)
		if err != nil {
			return match
		}
		return n.at(parsed)
	})
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

func (t *Trace) event(eventType, reason, message string) {
	t.emit("event %s %s %s", eventType, reason, t.norm.text(message))
}

func (t *Trace) write(w writeRecord) {
	line := fmt.Sprintf("%s %s name=%s", w.kind, w.verb, w.name)
	if w.detail != "" {
		line += " " + w.detail
	}
	t.emit("%s", line)
}

func (t *Trace) line(format string, args ...any) {
	t.emit("%s", fmt.Sprintf(format, args...))
}

func joinFields(parts []string) string {
	return strings.Join(parts, " ")
}
