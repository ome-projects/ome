package holds

import (
	"flag"
	"path/filepath"
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateRowsGolden = flag.Bool("update-rows", false, "re-record testdata/rows_allocations.json from this run")

// rowsShape is one shape of a Component at rest: how many Instances it has
// and how many pods each runs.
type rowsShape struct {
	name            string
	instances, pods int
}

// rowsShapes are the budget's shapes: the largest reference replica of
// single-pod Instances and a quarter of it, 160 gangs of a leader and seven
// workers, and the largest reference gang service, 1,725 gangs of a
// leader and one worker.
func rowsShapes() []rowsShape {
	return []rowsShape{
		{name: "1000x1", instances: 1000, pods: 1},
		{name: "4000x1", instances: 4000, pods: 1},
		{name: "160x8", instances: 160, pods: 8},
		{name: "1725x2", instances: 1725, pods: 2},
	}
}

// restingComponent is a Component at rest as the hold pass reads it: the
// plan's entry for every Instance at the layout the shape runs, and every
// row Ready on the spec revision with every pod serving.
func restingComponent(shape rowsShape) (types.ComponentPlan, []types.InstanceStatus) {
	layout := []types.RunnerPlan{{Name: types.RunnerDefault, Size: 1}}
	if shape.pods > 1 {
		layout = []types.RunnerPlan{{Name: types.RunnerLeader, Size: 1}, {Name: types.RunnerWorker, Size: int32(shape.pods - 1)}}
	}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  int32(shape.instances),
		Instances: make([]types.InstancePlan, shape.instances),
		Runners:   layout,
	}
	statuses := make([]types.InstanceStatus, shape.instances)
	pods := int32(shape.pods)
	for i := range statuses {
		plan.Instances[i] = types.InstancePlan{Index: int32(i), Incarnation: 1, Runners: layout}
		statuses[i] = types.InstanceStatus{
			Index: int32(i), Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "svc-engine-rev-a",
			PodCount: pods, ReadyPodCount: pods, ServingPodCount: pods, AvailablePodCount: pods, ScheduledPodCount: pods,
			Admitted: true,
		}
	}
	return plan, statuses
}

// rowsSink keeps the rows a measured build makes from being optimized away.
var rowsSink []Row

// checkRowsAtRest holds what the rows read at rest: one row per observed
// row in order, each at the plan's pod count with the plan's entry, none
// held out by the scale-down.
func checkRowsAtRest(t *testing.T, shape rowsShape, plan types.ComponentPlan, statuses []types.InstanceStatus) {
	t.Helper()
	rows := RowsForPlan(plan, statuses, nil)
	if len(rows) != len(statuses) {
		t.Fatalf("%s: %d rows over %d observed rows", shape.name, len(rows), len(statuses))
	}
	for i := range rows {
		if rows[i].Status.Index != statuses[i].Index || rows[i].Desired != int32(shape.pods) || rows[i].Instance != &plan.Instances[i] || rows[i].Excluded {
			t.Fatalf("%s: row %d reads index %d, %d desired pods, the plan's entry %t, excluded %t; a resting row reads its own index, the plan's %d pods, the plan's entry and no exclusion",
				shape.name, i, rows[i].Status.Index, rows[i].Desired, rows[i].Instance == &plan.Instances[i], rows[i].Excluded, shape.pods)
		}
	}
}

// BenchmarkRowsForPlanAtRest is the cost of building the hold pass's rows
// over a Component at rest, per shape.
func BenchmarkRowsForPlanAtRest(b *testing.B) {
	for _, shape := range rowsShapes() {
		plan, statuses := restingComponent(shape)
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				rowsSink = RowsForPlan(plan, statuses, nil)
			}
		})
	}
}

// TestRowsForPlanAtRestStaysWithinBudget holds the allocations and the bytes
// of building the hold pass's rows over a Component at rest, as every pass
// builds them ahead of the hold pass, to the recorded golden, at 1,000 and
// 4,000 single-pod Instances, 160 gangs of eight and 1,725 gangs of two.
// Beside the golden: one row per observed row, in order, each at the plan's
// pod count with the plan's entry and none held out.
func TestRowsForPlanAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "rows_allocations.json"), *updateRowsGolden)
	defer golden.Close()
	for _, shape := range rowsShapes() {
		plan, statuses := restingComponent(shape)
		checkRowsAtRest(t, shape, plan, statuses)
		golden.Check(shape.name+"/rows/allocations", func() { rowsSink = RowsForPlan(plan, statuses, nil) })
		golden.CheckBytes(shape.name+"/rows/bytes", func() { rowsSink = RowsForPlan(plan, statuses, nil) })
	}
}
