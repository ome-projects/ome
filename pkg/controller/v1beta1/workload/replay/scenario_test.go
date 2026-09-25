package replay

import (
	"strings"
	"testing"
)

// The scenario file is the replay suite's whole input, so a malformed one
// must fail at parse with the author's mistake named rather than at replay
// with a trace that silently omitted an event.

func TestParseRejectsAnUnknownField(t *testing.T) {
	_, err := Parse([]byte("scenario: x\nnosuchfield: 1\n"), Vocabulary{})
	if err == nil {
		t.Fatal("an unknown YAML key must fail the parse")
	}
}

func TestParseAcceptsTheMinimalScenario(t *testing.T) {
	s, err := Parse([]byte(minimalScenario), Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.Scenario != "minimal" {
		t.Fatalf("scenario name: got %q want %q", s.Scenario, "minimal")
	}
	if len(s.Timeline) != 1 {
		t.Fatalf("timeline: got %d entries want 1", len(s.Timeline))
	}
}

func TestSplitEventKeySeparatesTheVariant(t *testing.T) {
	for _, tc := range []struct {
		key         string
		id, variant string
		wantErr     bool
	}{
		{key: "pod.ready", id: "pod.ready"},
		{key: "pod.deleted[source]", id: "pod.deleted", variant: "source"},
		{key: "pod.deleted[source", wantErr: true},
	} {
		id, variant, err := splitEventKey(tc.key)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%q: want an error", tc.key)
			}
			continue
		}
		if err != nil || id != tc.id || variant != tc.variant {
			t.Errorf("%q: got (%q, %q, %v) want (%q, %q, nil)", tc.key, id, variant, err, tc.id, tc.variant)
		}
	}
}

func TestParseDurationNamesTheFieldItRejected(t *testing.T) {
	if d, err := ParseDuration("grace", ""); err != nil || d != 0 {
		t.Fatalf("an empty duration is zero: got (%v, %v)", d, err)
	}
	_, err := ParseDuration("grace", "soon")
	if err == nil || !strings.Contains(err.Error(), "grace") {
		t.Fatalf("error must name the field: got %v", err)
	}
}

func TestSortedEventIDsIncludesTheClock(t *testing.T) {
	ids := SortedEventIDs()
	found := false
	for i, id := range ids {
		if id == ClockAdvance {
			found = true
		}
		if i > 0 && ids[i-1] > id {
			t.Fatalf("ids are not sorted at %d: %q then %q", i, ids[i-1], id)
		}
	}
	if !found {
		t.Errorf("%s is applicable and must be listed", ClockAdvance)
	}
}

func TestPodRefRunnerNameDefaults(t *testing.T) {
	if got := (PodRef{}).RunnerName(); got != DefaultRunner {
		t.Errorf("unset runner: got %q want %q", got, DefaultRunner)
	}
	if got := (PodRef{Runner: "worker"}).RunnerName(); got != "worker" {
		t.Errorf("named runner: got %q want %q", got, "worker")
	}
}
