package types

import (
	"flag"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateLayoutGolden = flag.Bool("update-layout", false, "re-record testdata/layout_allocations.json from this run")

// layoutShape is one shape of a Component at rest: how many Instances it
// has and how many pods each runs.
type layoutShape struct {
	name            string
	instances, pods int
}

// layoutShapes are the budget's shapes: the largest reference replica of
// single-pod Instances and a quarter of it, 160 gangs of a leader and seven
// workers, and the largest reference gang service, 1,725 gangs of a
// leader and one worker.
func layoutShapes() []layoutShape {
	return []layoutShape{
		{name: "1000x1", instances: 1000, pods: 1},
		{name: "4000x1", instances: 4000, pods: 1},
		{name: "160x8", instances: 160, pods: 8},
		{name: "1725x2", instances: 1725, pods: 2},
	}
}

// restingRevision is the revision every Instance of a resting Component
// runs, which is the roll's target.
const restingRevision = "svc-engine-rev-a"

// restingPlan is the plan of a Component at rest as BuildPlan builds it:
// the layout the spec revision renders, every Instance's entry at that
// layout, and the width the spec revision recorded for a gang.
func restingPlan(shape layoutShape) ComponentPlan {
	layout := []RunnerPlan{{Name: RunnerDefault, Size: 1}}
	widths := map[string]int32{}
	if shape.pods > 1 {
		layout = []RunnerPlan{{Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: int32(shape.pods - 1)}}
		widths[restingRevision] = int32(shape.pods - 1)
	}
	plan := ComponentPlan{
		Component:            ComponentEngine,
		Replicas:             int32(shape.instances),
		Instances:            make([]InstancePlan, shape.instances),
		Runners:              layout,
		WorkerSizeByRevision: widths,
	}
	for i := range plan.Instances {
		plan.Instances[i] = InstancePlan{Index: int32(i), Incarnation: 1, Runners: layout}
	}
	return plan
}

// layoutSink keeps the layouts a measured read builds from being optimized
// away.
var layoutSink int

// readEveryLayout reads every Instance's layout at the revision it runs, as
// the create pass does on every pass.
func readEveryLayout(plan ComponentPlan) {
	runners := 0
	for i := range plan.Instances {
		runners += len(plan.RunnersFor(plan.Instances[i], restingRevision))
	}
	layoutSink = runners
}

// checkLayoutsAtRest holds what every Instance reads at rest: the layout
// the plan renders, runner for runner.
func checkLayoutsAtRest(t *testing.T, shape layoutShape, plan ComponentPlan) {
	t.Helper()
	for i := range plan.Instances {
		got := plan.RunnersFor(plan.Instances[i], restingRevision)
		if diff := cmp.Diff(plan.Runners, got); diff != "" {
			t.Fatalf("%s: Instance %d reads another layout than the plan renders (-want +got):\n%s", shape.name, i, diff)
		}
	}
}

// BenchmarkRunnersForAtRest is the cost of reading every Instance's layout
// at the revision it runs over a Component at rest, per shape.
func BenchmarkRunnersForAtRest(b *testing.B) {
	for _, shape := range layoutShapes() {
		plan := restingPlan(shape)
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				readEveryLayout(plan)
			}
		})
	}
}

// TestRunnersForAtRestStaysWithinBudget holds the allocations and the bytes
// of reading every Instance's layout at the revision it runs over a
// Component at rest, to the recorded golden, at 1,000 and 4,000 single-pod
// Instances, 160 gangs of eight and 1,725 gangs of two. Beside the golden:
// every Instance reads the layout the plan renders.
func TestRunnersForAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "layout_allocations.json"), *updateLayoutGolden)
	defer golden.Close()
	for _, shape := range layoutShapes() {
		plan := restingPlan(shape)
		checkLayoutsAtRest(t, shape, plan)
		golden.Check(shape.name+"/layout/allocations", func() { readEveryLayout(plan) })
		golden.CheckBytes(shape.name+"/layout/bytes", func() { readEveryLayout(plan) })
	}
}
