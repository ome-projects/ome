package replay_test

import (
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/replay"
)

// coldCreate is the smallest scenario that exercises every layer of the
// driver: a plan with no rows, one reconcile that materializes the pod,
// and one that promotes it once the kubelet reports it Ready.
const coldCreate = `
scenario: cold-create
arrows: [T-empty-create]
initial:
  spec:
    replicas: 1
    strategy: SurgeThenDrain
    image: registry.example.com/runtime:v1
    instanceReadyTimeout: 30m
  config:
    requeueOperation: 5s
    requeueGate: 3s
timeline:
  - tick: 1
  - tick: 2
    events:
      - pod.ready: {pod: {index: 0}}
`

func TestRunRecordsCreateAndPromote(t *testing.T) {
	scenario, err := replay.Parse([]byte(coldCreate), replay.Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	trace, err := replay.Run(context.Background(), scenario, replay.Options{})
	if err != nil {
		t.Fatalf("run: %v\ntrace:\n%s", err, trace)
	}
	for _, want := range []string{"pod create", "row=0 write", "pass result="} {
		if !strings.Contains(trace, want) {
			t.Fatalf("trace missing %q:\n%s", want, trace)
		}
	}
	t.Logf("trace:\n%s", trace)
}

// TestRunIsDeterministic is the package-local form of the repository's
// determinism gate: the same scenario replayed twice must produce the
// same bytes.
func TestRunIsDeterministic(t *testing.T) {
	scenario, err := replay.Parse([]byte(coldCreate), replay.Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	first, err := replay.Run(context.Background(), scenario, replay.Options{})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for i := 0; i < 5; i++ {
		next, err := replay.Run(context.Background(), scenario, replay.Options{})
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if next != first {
			t.Fatalf("run %d diverged:\nfirst:\n%s\ngot:\n%s", i, first, next)
		}
	}
}

// TestParseRejectsUndeclaredEvent pins the loader contract: the table's
// event vocabulary is the scenario's vocabulary.
func TestParseRejectsUndeclaredEvent(t *testing.T) {
	vocab := replay.Vocabulary{Events: map[string][]string{"pod.ready": {"source", "target"}}}
	_, err := replay.Parse([]byte(coldCreate), vocab)
	if err != nil {
		t.Fatalf("declared event rejected: %v", err)
	}
	bad := strings.Replace(coldCreate, "pod.ready", "pod.invented", 1)
	if _, err := replay.Parse([]byte(bad), vocab); err == nil {
		t.Fatalf("an undeclared event must fail to load")
	}
	badVariant := strings.Replace(coldCreate, "pod.ready:", "pod.ready[sideways]:", 1)
	if _, err := replay.Parse([]byte(badVariant), vocab); err == nil {
		t.Fatalf("an undeclared variant must fail to load")
	}
}
