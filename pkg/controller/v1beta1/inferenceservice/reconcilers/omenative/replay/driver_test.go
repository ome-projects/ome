package replay

import (
	"context"
	"strings"
	"testing"
)

const plainWalk = `scenario: plain
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 2, image: registry.example.com/runtime:v1}
    groups:
      - components: [engine]
        blueGreen: {}
  config: {requeue: 10s}
timeline:
  - tick: 1
    note: settled
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
  - tick: 3
    events:
      - ctrl.resync
`

// A run opens on the pass after a template edit, once the projection has
// caught up, and the trace carries the note, the applied event and the
// pinned run.
func TestRunOpensAfterTheProjectionCatchesUp(t *testing.T) {
	s, err := Parse([]byte(plainWalk), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	trace, err := Run(context.Background(), s, Options{})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	for _, want := range []string{
		"tick=1 note settled",
		"tick=2 apply spec.revision[new] component=engine image=registry.example.com/runtime:v2 generation=2",
		"tick=2 run outcome=none requeueAfter=10s",
		"tick=3 run outcome=opened stateChanged=true",
		"tick=3 flush which=boundary result=written",
	} {
		if !strings.Contains(trace, want) {
			t.Errorf("trace lacks %q\n%s", want, trace)
		}
	}
}

// A stale read before any pass has nothing to serve, and a stale-read
// variant the cluster does not bear out fails the run.
func TestStaleReadClaimsAreChecked(t *testing.T) {
	early := strings.Replace(plainWalk, "  - tick: 1\n    note: settled\n", "  - tick: 1\n    events:\n      - ctrl.staleRead[missesOpen]\n", 1)
	s, err := Parse([]byte(early), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), s, Options{}); err == nil || !strings.Contains(err.Error(), "no cache snapshot") {
		t.Fatalf("stale read with nothing to serve: %v", err)
	}
	unmet := strings.Replace(plainWalk, "  - tick: 3\n    events:\n      - ctrl.resync\n", "  - tick: 3\n    events:\n      - ctrl.staleRead[missesClose]\n", 1)
	s, err = Parse([]byte(unmet), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), s, Options{}); err == nil || !strings.Contains(err.Error(), "does not miss") {
		t.Fatalf("unmet stale claim: %v", err)
	}
}
