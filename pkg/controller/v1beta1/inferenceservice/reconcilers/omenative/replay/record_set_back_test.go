package replay

import (
	"context"
	"strings"
	"testing"
)

// A record with no earlier revision has nothing to go back to, so the event
// is refused; and the event takes no variant.
func TestRecordSetBackNeedsAnEarlierRevision(t *testing.T) {
	s, err := Parse([]byte(scenarioHead+`  - tick: 1
  - tick: 2
    events:
      - record.setBack: {component: engine}
`), testVocabulary())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), s, Options{}); err == nil || !strings.Contains(err.Error(), "no earlier rolled-out revision") {
		t.Fatalf("a set-back with nothing to go back to: %v", err)
	}
	if _, err := Parse([]byte(scenarioHead+"  - tick: 1\n    events:\n      - record.setBack[hard]: {component: engine}\n"), testVocabulary()); err == nil {
		t.Fatal("parsed a set-back with a variant")
	}
}

// After one roll the record names two revisions. The write puts the latest
// one back on the earlier one, and the pass of the same tick finds every pod
// Ready on the running revision and records that revision again.
func TestRecordSetBackIsLeveledByThePassThatReadsIt(t *testing.T) {
	s, err := Parse([]byte(scenarioHead+`  - tick: 1
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
      - member.surgePodCreated: {component: engine}
      - member.readyOnTarget: {component: engine}
      - member.oldPodsGone: {component: engine}
      - member.converged: {component: engine}
  - tick: 3
    events:
      - record.setBack: {component: engine}
`), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	trace, err := Run(context.Background(), s, Options{})
	if err != nil {
		t.Fatalf("%v\n%s", err, trace)
	}
	var applied, after string
	for _, line := range strings.Split(trace, "\n") {
		switch {
		case strings.HasPrefix(line, "tick=3 apply record.setBack "):
			applied = line
		case strings.HasPrefix(line, "tick=3 revisions component=engine "):
			after = line
		}
	}
	was, now, moved := strings.Cut(traceField(applied, "latest="), "→")
	if !moved || was == now {
		t.Fatalf("the write is not traced as a move of the record: %q", applied)
	}
	if latest := traceField(after, "latest="); latest == "" || latest == traceField(after, "previous=") {
		t.Fatalf("the pass left the record on the earlier revision: %q", after)
	}
}

// traceField is the value a trace line carries after a key.
func traceField(line, key string) string {
	_, rest, ok := strings.Cut(line, key)
	if !ok {
		return ""
	}
	value, _, _ := strings.Cut(rest, " ")
	return value
}
