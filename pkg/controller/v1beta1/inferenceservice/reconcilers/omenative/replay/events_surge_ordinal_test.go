package replay

import (
	"context"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// secondReplacementWalk rolls engine Instance 0 of two to version 2 by a
// surge, drains its version 1 pod, sets the template back to version 1
// and surges Instance 0 again without naming the new pod's ordinal.
const secondReplacementWalk = `scenario: second-replacement
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
      - member.oldPodsGone: {component: engine, index: 0}
  - tick: 5
    events:
      - spec.revision[revert]: {component: engine, image: registry.example.com/runtime:v1}
  - tick: 6
    events:
      - member.surgePodCreated: {component: engine, index: 0}
`

// staleReplacementWalk pushes version 2, surges engine Instance 0 toward it,
// pushes version 3 before that pod is Ready and surges Instance 0 again.
const staleReplacementWalk = `scenario: stale-replacement
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
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v3}
  - tick: 5
    events:
      - member.surgePodCreated: {component: engine, index: 0}
`

// lastTick replays a walk and returns the view of its last tick.
func lastTick(t *testing.T, walk string) TickView {
	t.Helper()
	s, err := Parse([]byte(walk), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	var last TickView
	trace, err := Run(context.Background(), s, Options{Observer: func(view TickView) { last = view }})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	return last
}

// enginePods indexes the engine pods of a view by name.
func enginePods(view TickView) map[string]*corev1.Pod {
	pods := map[string]*corev1.Pod{}
	for _, pod := range view.Pods[v1beta1.EngineComponent] {
		pods[pod.Name] = pod
	}
	return pods
}

func podNames(pods map[string]*corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for name := range pods {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func isReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// A second replacement of an Instance whose serving pod holds ordinal 1
// takes ordinal 0, so the cluster keeps the serving pod beside the new
// one: three engine pods, Instance 0 with one of each version.
func TestASecondReplacementTakesTheFreeOrdinal(t *testing.T) {
	pods := enginePods(lastTick(t, secondReplacementWalk))
	want := []string{"svc-a-engine-0-default-0", "svc-a-engine-0-default-1", "svc-a-engine-1-default-0"}
	if got := podNames(pods); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("engine pods after the second surge = %v, want %v", got, want)
	}
	version1 := pods["svc-a-engine-1-default-0"].Labels[query.LabelRevisionHash]
	switch {
	case pods["svc-a-engine-0-default-0"].Labels[query.LabelRevisionHash] != version1:
		t.Errorf("the new pod of Instance 0 does not run version 1")
	case pods["svc-a-engine-0-default-1"].Labels[query.LabelRevisionHash] == version1:
		t.Errorf("the serving pod of Instance 0 no longer runs version 2")
	case !isReady(pods["svc-a-engine-0-default-1"]) || isReady(pods["svc-a-engine-0-default-0"]):
		t.Errorf("Instance 0 must serve from its version 2 pod while the version 1 pod is not Ready")
	}
}

// A surge that names the ordinal of a pod the Instance already has fails
// the run, and the error names the event and the pod.
func TestANamedOrdinalHeldByAPodFailsTheEvent(t *testing.T) {
	walk := strings.Replace(secondReplacementWalk,
		"  - tick: 6\n    events:\n      - member.surgePodCreated: {component: engine, index: 0}\n",
		"  - tick: 6\n    events:\n      - member.surgePodCreated: {component: engine, index: 0, ordinal: 1}\n", 1)
	s, err := Parse([]byte(walk), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = Run(context.Background(), s, Options{})
	if err == nil {
		t.Fatal("a surge at the serving pod's ordinal ran; it must fail")
	}
	for _, want := range []string{"member.surgePodCreated", "svc-a-engine-0-default-1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

// A replacement an earlier attempt left unready on another revision gives
// its ordinal to the new one: Instance 0 publishes two pods, the serving
// version 1 pod and the version 3 pod at the ordinal the version 2 pod held.
func TestAStaleReplacementGivesItsOrdinalUp(t *testing.T) {
	view := lastTick(t, staleReplacementWalk)
	pods := enginePods(view)
	want := []string{"svc-a-engine-0-default-0", "svc-a-engine-0-default-1", "svc-a-engine-1-default-0"}
	if got := podNames(pods); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("engine pods after the second surge = %v, want %v", got, want)
	}
	ir := view.Replicas[v1beta1.EngineComponent]
	if ir == nil {
		t.Fatal("the engine replica is missing from the view")
	}
	target := query.RevisionHashFromControllerRevisionName(ir.Status.UpdateRevision)
	if got := pods["svc-a-engine-0-default-1"].Labels[query.LabelRevisionHash]; got != target {
		t.Errorf("the replacement of Instance 0 runs %s, want the update revision %s", got, target)
	}
	for _, row := range ir.Status.InstanceStatuses {
		if row.Index == 0 && row.PodCount != 2 {
			t.Errorf("Instance 0 publishes podCount=%d, want 2: the version 1 pod and the version 3 pod", row.PodCount)
		}
	}
}
