package replay

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestWiderBudgetAndEpochAt hold the two readings the budgets-held-to
// view rests on: the looser of two budgets, no limit being the loosest,
// and the epoch that applied at an instant, the first one standing in for an
// attempt that began before any epoch was recorded.
func TestWiderBudgetAndEpochAt(t *testing.T) {
	if got := widerBudget(1, 4); got != 4 {
		t.Fatalf("widerBudget(1, 4) = %d, want 4", got)
	}
	if got := widerBudget(escalation.BudgetNoLimit, 4); got != escalation.BudgetNoLimit {
		t.Fatalf("no limit is the loosest budget, got %d", got)
	}
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	d := &driver{budgetEpochs: []budgetEpoch{
		{since: start, maxUnavailable: 4, maxSurge: 0},
		{since: start.Add(10 * time.Second), maxUnavailable: 1, maxSurge: 0},
	}}
	for _, tc := range []struct {
		at   time.Time
		want int32
	}{
		{start.Add(-time.Minute), 4},
		{start, 4},
		{start.Add(5 * time.Second), 4},
		{start.Add(10 * time.Second), 1},
		{start.Add(time.Hour), 1},
	} {
		if got := d.epochAt(tc.at).maxUnavailable; got != tc.want {
			t.Fatalf("epochAt(%s).maxUnavailable = %d, want %d", tc.at.Sub(start), got, tc.want)
		}
	}
}

// TestBudgetsHeldToKeepTheSurgeSlotOfAStandingReplacement states the
// reading for a row that carries no attempt: a replacement still standing
// beyond its pod set keeps the surge budget of the epoch it was created
// in, a replacement on its way out keeps nothing, and a row within its set
// widens nothing.
func TestBudgetsHeldToKeepTheSurgeSlotOfAStandingReplacement(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	d := &driver{budgetEpochs: []budgetEpoch{
		{since: start, maxUnavailable: 0, maxSurge: 4},
		{since: start.Add(10 * time.Second), maxUnavailable: 0, maxSurge: 1},
	}}
	podAt := func(created time.Time, terminating bool) *corev1.Pod {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(created)}}
		if terminating {
			now := metav1.NewTime(created.Add(time.Minute))
			pod.DeletionTimestamp = &now
		}
		return pod
	}
	rows := []types.InstanceStatus{{Index: 0, Phase: types.InstancePhaseFailed}, {Index: 1, Phase: types.InstancePhaseReady}}
	source := podAt(time.Time{}, false)
	if got := d.budgetsHeldTo(rows, map[int32][]*corev1.Pod{0: {source, podAt(start.Add(5*time.Second), false)}, 1: {source}}, 1); got.MaxSurge != 4 {
		t.Fatalf("a standing replacement of the first epoch keeps its slot: MaxSurge = %d, want 4", got.MaxSurge)
	}
	if got := d.budgetsHeldTo(rows, map[int32][]*corev1.Pod{0: {source, podAt(start.Add(5*time.Second), true)}, 1: {source}}, 1); got.MaxSurge != 1 {
		t.Fatalf("a replacement on its way out keeps nothing: MaxSurge = %d, want 1", got.MaxSurge)
	}
	if got := d.budgetsHeldTo(rows, map[int32][]*corev1.Pod{0: {source}, 1: {source}}, 1); got.MaxSurge != 1 {
		t.Fatalf("a row within its set widens nothing: MaxSurge = %d, want 1", got.MaxSurge)
	}
}

// budgetLoweredScenario rolls four Instances two at a time and lowers the
// drain budget to one while the first two attempts are open.
const budgetLoweredScenario = `
scenario: budget-lowered
arrows: [T-ready-recreate]
initial:
  spec:
    replicas: 4
    strategy: RecreatePod
    maxSurge: "0"
    maxUnavailable: "2"
    image: registry.example.com/runtime:v1
    instanceReadyTimeout: 30m
  config:
    requeueOperation: 5s
    requeueGate: 3s
  rows:
    - {index: 0, phase: Ready, runningRevision: current, readySince: 0s}
    - {index: 1, phase: Ready, runningRevision: current, readySince: 0s}
    - {index: 2, phase: Ready, runningRevision: current, readySince: 0s}
    - {index: 3, phase: Ready, runningRevision: current, readySince: 0s}
  pods:
    - {index: 0, ready: true, serving: true, routed: true}
    - {index: 1, ready: true, serving: true, routed: true}
    - {index: 2, ready: true, serving: true, routed: true}
    - {index: 3, ready: true, serving: true, routed: true}
timeline:
  - tick: 1
    note: four Instances serve version 1
  - tick: 2
    note: version 2 is pushed and the budget of two takes Instances 0 and 1 out of the serving gate
    events:
      - clock.advance: 5s
      - spec.revision: {image: registry.example.com/runtime:v2}
  - tick: 3
    note: the operator lowers the drain budget to one while both attempts are open
    events:
      - clock.advance: 5s
      - spec.strategy: {maxUnavailable: "1"}
  - tick: 4
    note: the old pods leave rotation and are deleted
    events:
      - clock.advance: 5s
      - endpoint.rotationOut[source]: {pod: {index: 0}}
      - endpoint.rotationOut[source]: {pod: {index: 1}}
  - tick: 5
    note: the old pods are gone and the version 2 pods are created
    events:
      - clock.advance: 5s
      - pod.deleted[source]: {pod: {index: 0}}
      - pod.deleted[source]: {pod: {index: 1}}
  - tick: 6
    note: the version 2 pods serve, so Instances 0 and 1 promote and the budget of one admits Instance 2
    events:
      - clock.advance: 5s
      - pod.ready[target]: {pod: {index: 0}}
      - endpoint.rotationIn[target]: {pod: {index: 0}}
      - pod.ready[target]: {pod: {index: 1}}
      - endpoint.rotationIn[target]: {pod: {index: 1}}
  - tick: 7
    note: Instance 2's old pod leaves rotation under the budget of one
    events:
      - clock.advance: 5s
      - endpoint.rotationOut[source]: {pod: {index: 2}}
`

// TestBudgetsHeldToKeepTheBudgetAnOpenAttemptWasAdmittedUnder runs a
// lowering with the observer on: while the two attempts admitted under the
// budget of two are open, the view holds the tick to two; once they land,
// to the spec's one, so the capacity invariant reads the roll as the
// operator would, with no finding on the attempts the edit cannot undo.
func TestBudgetsHeldToKeepTheBudgetAnOpenAttemptWasAdmittedUnder(t *testing.T) {
	scenario, err := Parse([]byte(budgetLoweredScenario), Vocabulary{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	heldTo := map[int]int32{}
	specBudget := map[int]string{}
	var findings []Finding
	observe := func(view TickView) {
		if view.Spec.HeldTo == nil {
			t.Fatalf("tick %d: the view carries no budgets to hold the tick to", view.Tick)
		}
		heldTo[view.Tick] = view.Spec.HeldTo.MaxUnavailable
		specBudget[view.Tick] = view.Spec.MaxUnavailable.String()
		findings = append(findings, CheckCapacity(view)...)
	}
	if trace, err := Run(context.Background(), scenario, Options{Observer: observe}); err != nil {
		t.Fatalf("run: %v\n%s", err, trace)
	}
	for tick, want := range map[int]int32{1: 2, 2: 2, 3: 2, 4: 2, 5: 2, 6: 1, 7: 1} {
		if heldTo[tick] != want {
			t.Errorf("tick %d: budget held to %d, want %d (spec %s)", tick, heldTo[tick], want, specBudget[tick])
		}
	}
	if specBudget[3] != "1" || specBudget[2] != "2" {
		t.Errorf("the spec's own budget reads %s before and %s after the edit, want 2 and 1", specBudget[2], specBudget[3])
	}
	if len(findings) != 0 {
		t.Errorf("the capacity invariant found %d thing(s) wrong with the roll; the first: %s", len(findings), findings[0].Detail)
	}
}
