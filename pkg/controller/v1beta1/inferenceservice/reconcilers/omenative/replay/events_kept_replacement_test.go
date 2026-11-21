package replay

import (
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// keptReplacementWalk surges engine Instance 0 of two to version 2, sets
// the template back to version 1 once the new pod serves, and ends the
// surge on version 2: the old pod leaves and the new pod stays.
const keptReplacementWalk = `scenario: kept-replacement
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 2, image: registry.example.com/runtime:v1}
    groups:
      - components: [engine]
        rollingUpdate: {}
  config: {requeue: 10s}
timeline:
  - tick: 1
    note: settled
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
  - tick: 3
    events:
      - member.surgePodCreated: {component: engine, index: 0}
  - tick: 4
    events:
      - member.readyOnTarget: {component: engine, index: 0}
  - tick: 5
    events:
      - spec.revision[revert]: {component: engine, image: registry.example.com/runtime:v1}
  - tick: 6
    events:
      - member.oldPodsGone: {component: engine, index: 0, image: registry.example.com/runtime:v2}
`

// A surge whose replacement the engine keeps ends on the replacement's
// version: the old pod is gone, the row runs version 2 with no attempt
// open, and the member's target stays version 1.
func TestASurgeEndsOnTheReplacementTheEngineKept(t *testing.T) {
	view := lastTick(t, keptReplacementWalk)
	pods := enginePods(view)
	want := []string{"svc-a-engine-0-default-1", "svc-a-engine-1-default-0"}
	if got := podNames(pods); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("engine pods after the surge ended = %v, want %v", got, want)
	}
	version1 := pods["svc-a-engine-1-default-0"].Labels[query.LabelRevisionHash]
	version2 := pods["svc-a-engine-0-default-1"].Labels[query.LabelRevisionHash]
	if version2 == version1 || !isReady(pods["svc-a-engine-0-default-1"]) {
		t.Fatalf("Instance 0 must serve from its version 2 pod; its pod runs %s and version 1 is %s", version2, version1)
	}
	ir := view.Replicas[v1beta1.EngineComponent]
	if ir == nil {
		t.Fatal("the view holds no engine replica")
	}
	if got := query.RevisionHashFromControllerRevisionName(ir.Status.UpdateRevision); got != version1 {
		t.Errorf("the update revision is %s, want version 1 (%s)", got, version1)
	}
	seen := false
	for _, row := range ir.Status.InstanceStatuses {
		if row.Index != 0 {
			continue
		}
		seen = true
		if got := query.RevisionHashFromControllerRevisionName(row.RunningRevision); got != version2 {
			t.Errorf("row 0 runs %s, want version 2 (%s)", got, version2)
		}
		if row.Phase != v1beta1.OMENativeInstanceReady || row.Operation != nil {
			t.Errorf("row 0 reads phase %s with operation %v, want Ready with no attempt open", row.Phase, row.Operation)
		}
	}
	if !seen {
		t.Error("the engine replica publishes no row 0")
	}
}

// An Instance cannot end on a version none of its pods runs: the event
// fails the run and the error names the event and the version.
func TestEndingASurgeOnAVersionWithNoPodFailsTheEvent(t *testing.T) {
	walk := strings.Replace(keptReplacementWalk,
		"      - member.oldPodsGone: {component: engine, index: 0, image: registry.example.com/runtime:v2}\n",
		"      - member.oldPodsGone: {component: engine, index: 0, image: registry.example.com/runtime:v3}\n", 1)
	s, err := Parse([]byte(walk), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = Run(context.Background(), s, Options{})
	if err == nil || !strings.Contains(err.Error(), "member.oldPodsGone") || !strings.Contains(err.Error(), "runtime:v3") {
		t.Fatalf("run error = %v, want one that names member.oldPodsGone and the version with no pod", err)
	}
}
