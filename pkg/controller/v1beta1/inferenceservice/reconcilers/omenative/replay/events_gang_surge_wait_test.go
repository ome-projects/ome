package replay

import (
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// gangSurgeWaitWalk is two engine gangs with no rollout group. Version 2 is
// pushed at tick 2, and the replacement of gang 0 finds no node: its row is
// built at index 2 with the wait the event names. At tick 3 the replacement
// reports Ready.
const gangSurgeWaitWalk = `scenario: gang-surge-waits-for-a-node
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 2, image: registry.example.com/runtime:v1}
  config: {requeue: 10s}
timeline:
  - tick: 1
  - tick: 2
    events:
      - spec.revision[new]: {component: engine, image: registry.example.com/runtime:v2}
      - member.surgePodCreated: {component: engine, index: 0, replacement: 2, waiting: Unschedulable}
  - tick: 3
    events:
      - member.readyOnTarget: {component: engine, index: 2}
`

// A gang's replacement that finds no node carries the wait on its own row,
// as the engine names it there. The word leaves once the replacement is
// Ready.
func TestAGangSurgeReplacementCarriesItsWait(t *testing.T) {
	waits := map[int]string{}
	trace, err := runWalk(t, gangSurgeWaitWalk, func(view TickView) {
		for _, row := range view.Replicas[v1beta1.EngineComponent].Status.InstanceStatuses {
			if row.Index == 2 && row.Operation != nil {
				waits[view.Tick] = row.Operation.Waiting
			}
		}
	})
	if err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	if got, ok := waits[2]; !ok || got != "Unschedulable" {
		t.Errorf("on tick 2 the replacement's row names the wait %q (operation published: %v), want Unschedulable", got, ok)
	}
	if got := waits[3]; got != "" {
		t.Errorf("on tick 3 the replacement is Ready and its row still names the wait %q", got)
	}
	if !strings.Contains(trace, "replacement=2 waiting=Unschedulable") {
		t.Errorf("the trace does not name the wait of the replacement:\n%s", trace)
	}
}
