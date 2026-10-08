package replay

import (
	"strings"
	"testing"
)

func testVocabulary() Vocabulary {
	return Vocabulary{
		Events: map[string][]string{
			"ctrl.resync":    {},
			"spec.revision":  {"new", "revert", "rejected"},
			"ctrl.staleRead": {"missesOpen", "missesClose", "missesRepin", "anchorMissing", "phaseRegressed"},
			"canary.parked":  {},
			"record.legacy":  {"noTargetID"},
		},
		Cells: map[string]struct{}{"run/example-cell": {}},
	}
}

const scenarioHead = `scenario: t
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 1, image: registry.example.com/runtime:v1}
timeline:
`

// A bare event key, a one-key mapping and a duration scalar are the three
// spellings a timeline event takes.
func TestParseAcceptsTheThreeEventForms(t *testing.T) {
	s, err := Parse([]byte(scenarioHead+`  - tick: 1
    events:
      - ctrl.resync
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
      - clock.advance: 1m
`), testVocabulary())
	if err != nil {
		t.Fatal(err)
	}
	events := s.Timeline[0].Events
	if len(events) != 3 || events[0].Key() != "ctrl.resync" || events[1].Key() != "spec.revision[new]" || events[1].Args.Image == "" || events[2].Args.Duration != "1m" {
		t.Fatalf("events = %+v", events)
	}
}

// The parser refuses what the vocabulary does not declare, what the engines
// produce on their own, and what the driver cannot stage.
func TestParseRefusesWhatCannotBeStaged(t *testing.T) {
	cases := map[string]string{
		"undeclared event":    "      - pod.exploded\n",
		"undeclared variant":  "      - spec.revision[sideways]: {component: engine, image: x/y:z}\n",
		"produced event":      "      - canary.parked\n",
		"unapplied event":     "      - record.legacy[noTargetID]\n",
		"unknown argument":    "      - spec.revision[new]: {component: engine, image: x/y:z, pod: 1}\n",
		"clock without value": "      - clock.advance\n",
	}
	for name, event := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(scenarioHead+"  - tick: 1\n    events:\n"+event), testVocabulary())
			if err == nil {
				t.Fatalf("parsed %q", event)
			}
		})
	}
	_, err := Parse([]byte(strings.Replace(scenarioHead, "\"run/example-cell\"", "\"run/no.such\"", 1)+"  - tick: 1\n"), testVocabulary())
	if err == nil || !strings.Contains(err.Error(), "not in the vocabulary") {
		t.Fatalf("unknown cell: %v", err)
	}
	_, err = Parse([]byte(scenarioHead+"  - tick: 2\n"), testVocabulary())
	if err == nil || !strings.Contains(err.Error(), "consecutive") {
		t.Fatalf("tick numbering: %v", err)
	}
}

// Every stageable event declares the arguments it reads, and every
// declared argument set belongs to a stageable event or the clock.
func TestEventArgKeysMatchAppliers(t *testing.T) {
	for id := range eventArgKeys {
		if id == ClockAdvance {
			continue
		}
		if _, ok := eventAppliers[id]; !ok {
			t.Errorf("%s declares arguments but has no applier", id)
		}
	}
	for id := range eventAppliers {
		if _, produced := producedEvents[id]; produced {
			t.Errorf("%s is both staged and produced", id)
		}
	}
}
