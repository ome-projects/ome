package types

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

// layoutOf is a leader beside workers wide workers.
func layoutOf(workers int32) []RunnerPlan {
	return []RunnerPlan{{Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: workers}}
}

// runnersForPicture is one reading of an Instance's layout at a revision.
type runnersForPicture struct {
	name string
	plan ComponentPlan
	inst InstancePlan
	rev  string
	want []RunnerPlan
}

// runnersForPictures are the layouts an Instance renders: the plan's
// layout, or the Instance's own entry when the plan carries none, every
// worker at the width the revision recorded when the plan knows it, in the
// layout's order, and nil when there is no layout.
func runnersForPictures() []runnersForPicture {
	single := []RunnerPlan{{Name: RunnerDefault, Size: 1}}
	widths := map[string]int32{"rev-a": 7, "rev-b": 3, "rev-z": 0}
	planned := ComponentPlan{Runners: layoutOf(7), WorkerSizeByRevision: widths}
	bare := ComponentPlan{WorkerSizeByRevision: widths}
	return []runnersForPicture{
		{name: "the plan's layout at the width the revision recorded", plan: planned, inst: InstancePlan{Runners: single}, rev: "rev-b", want: layoutOf(3)},
		{name: "the plan's layout at its own width when the revision recorded that width", plan: planned, inst: InstancePlan{Runners: layoutOf(2)}, rev: "rev-a", want: layoutOf(7)},
		{name: "the plan's layout as the target renders it at a revision with no recorded width", plan: planned, inst: InstancePlan{Runners: layoutOf(2)}, rev: "rev-unknown", want: layoutOf(7)},
		{name: "the plan's layout as the target renders it at no revision", plan: planned, inst: InstancePlan{Runners: layoutOf(2)}, rev: "", want: layoutOf(7)},
		{name: "the plan's layout at a recorded width of zero keeps the leader", plan: planned, inst: InstancePlan{}, rev: "rev-z", want: layoutOf(0)},
		{name: "the Instance's entry at the recorded width when the plan carries no layout", plan: bare, inst: InstancePlan{Runners: layoutOf(9)}, rev: "rev-b", want: layoutOf(3)},
		{name: "the Instance's entry at its own width at a revision with no recorded width", plan: bare, inst: InstancePlan{Runners: layoutOf(9)}, rev: "rev-unknown", want: layoutOf(9)},
		{name: "the Instance's entry at its own width at no revision", plan: bare, inst: InstancePlan{Runners: layoutOf(9)}, rev: "", want: layoutOf(9)},
		{name: "nil when neither the plan nor the Instance carries a layout", plan: bare, inst: InstancePlan{}, rev: "rev-a", want: nil},
		{name: "nil for an empty layout on both", plan: ComponentPlan{Runners: []RunnerPlan{}, WorkerSizeByRevision: widths}, inst: InstancePlan{Runners: []RunnerPlan{}}, rev: "rev-a", want: nil},
		{name: "the Instance's entry when the plan's layout is empty", plan: ComponentPlan{Runners: []RunnerPlan{}, WorkerSizeByRevision: widths}, inst: InstancePlan{Runners: layoutOf(9)}, rev: "rev-b", want: layoutOf(3)},
		{name: "a single-pod layout keeps its one runner at a recorded width", plan: ComponentPlan{Runners: single, WorkerSizeByRevision: widths}, inst: InstancePlan{Runners: layoutOf(9)}, rev: "rev-b", want: single},
		{name: "every worker takes the width, the rest keep theirs, in order", plan: ComponentPlan{Runners: []RunnerPlan{{Name: RunnerWorker, Size: 2}, {Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: 5}, {Name: RunnerDefault, Size: 4}}, WorkerSizeByRevision: widths}, inst: InstancePlan{}, rev: "rev-b", want: []RunnerPlan{{Name: RunnerWorker, Size: 3}, {Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: 3}, {Name: RunnerDefault, Size: 4}}},
		{name: "the plan's layout with no widths recorded at all", plan: ComponentPlan{Runners: layoutOf(7)}, inst: InstancePlan{Runners: layoutOf(2)}, rev: "rev-b", want: layoutOf(7)},
	}
}

// TestComponentPlan_RunnersFor_OnEveryPicture holds the layout an Instance
// renders at a revision on every picture, runner for runner and in order,
// and nil (not an empty layout) where there is nothing to render.
func TestComponentPlan_RunnersFor_OnEveryPicture(t *testing.T) {
	for _, c := range runnersForPictures() {
		t.Run(c.name, func(t *testing.T) {
			got := c.plan.RunnersFor(c.inst, c.rev)
			if diff := cmp.Diff(c.want, got); diff != "" {
				t.Errorf("layout (-want +got):\n%s", diff)
			}
			if c.want == nil && got != nil {
				t.Errorf("layout is %#v, want nil", got)
			}
			if count := c.plan.TotalPodsFor(c.inst, c.rev); count != (InstancePlan{Runners: got}).TotalPods() {
				t.Errorf("TotalPodsFor counts %d pods, the layout has %d", count, (InstancePlan{Runners: got}).TotalPods())
			}
		})
	}
}

// TestComponentPlan_RunnersFor_LeavesThePlanAndTheEntryAsTheyWere: reading
// a layout at another width changes neither the plan's layout nor the
// Instance's entry, and the layout read at another width is the caller's
// own, so writing into it reaches neither.
func TestComponentPlan_RunnersFor_LeavesThePlanAndTheEntryAsTheyWere(t *testing.T) {
	widths := map[string]int32{"rev-b": 3}
	plan := ComponentPlan{Runners: layoutOf(7), WorkerSizeByRevision: widths}
	inst := InstancePlan{Runners: layoutOf(5)}

	fromPlan := plan.RunnersFor(inst, "rev-b")
	if diff := cmp.Diff(layoutOf(3), fromPlan); diff != "" {
		t.Fatalf("layout at the recorded width (-want +got):\n%s", diff)
	}
	fromPlan[1].Size = 99
	fromPlan[0].Name = "elsewhere"
	if diff := cmp.Diff(layoutOf(7), plan.Runners); diff != "" {
		t.Errorf("the plan's layout after a reading at another width and a write into it (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(layoutOf(5), inst.Runners); diff != "" {
		t.Errorf("the Instance's entry after a reading at another width and a write into it (-want +got):\n%s", diff)
	}

	bare := ComponentPlan{WorkerSizeByRevision: widths}
	fromEntry := bare.RunnersFor(inst, "rev-b")
	if diff := cmp.Diff(layoutOf(3), fromEntry); diff != "" {
		t.Fatalf("entry at the recorded width (-want +got):\n%s", diff)
	}
	fromEntry[1].Size = 99
	if diff := cmp.Diff(layoutOf(5), inst.Runners); diff != "" {
		t.Errorf("the Instance's entry after its own reading at another width and a write into it (-want +got):\n%s", diff)
	}

	for _, rev := range []string{"rev-b", "rev-unknown", ""} {
		plan.RunnersFor(inst, rev)
		bare.RunnersFor(inst, rev)
	}
	if diff := cmp.Diff(layoutOf(7), plan.Runners); diff != "" {
		t.Errorf("the plan's layout after readings at every kind of revision (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(layoutOf(5), inst.Runners); diff != "" {
		t.Errorf("the Instance's entry after readings at every kind of revision (-want +got):\n%s", diff)
	}
}
