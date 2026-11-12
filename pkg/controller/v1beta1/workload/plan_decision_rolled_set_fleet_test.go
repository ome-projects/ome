package workload_test

import (
	"context"
	"flag"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateRolledSetGolden = flag.Bool("update-rolled-set", false, "re-record testdata/rolled_set_allocations.json from this run")

// rolledSetShape is one shape of a Component at rest: how many Instances it
// has and how many pods each runs.
type rolledSetShape struct {
	name            string
	instances, pods int
}

// rolledSetShapes are the budget's shapes: the largest reference replica of
// single-pod Instances and a quarter of it, 160 gangs of a leader and seven
// workers, and the largest reference gang service, 1,725 gangs of a
// leader and one worker.
func rolledSetShapes() []rolledSetShape {
	return []rolledSetShape{
		{name: "1000x1", instances: 1000, pods: 1},
		{name: "4000x1", instances: 4000, pods: 1},
		{name: "160x8", instances: 160, pods: 8},
		{name: "1725x2", instances: 1725, pods: 2},
	}
}

// rolledSetFixture is one Component at rest as the update selection's
// rolled-set reading sees it: every row Ready on the spec revision since an
// hour, the rows indexed as a pass indexes them on entry, every pod serving
// on that revision, the plan over the rows and the revision the rows run
// as the roll's target.
type rolledSetFixture struct {
	ctx      context.Context
	input    types.ReconcileInput
	plan     types.ComponentPlan
	target   *appsv1.ControllerRevision
	snapshot *workload.ObservedSnapshot
}

func newRolledSetFixture(tb testing.TB, shape rolledSetShape) *rolledSetFixture {
	tb.Helper()
	const running = "svc-engine-rev-a"
	readySince := metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	f := &rolledSetFixture{input: fleetInput(shape.instances, shape.instances, shape.pods, running)}
	f.input.PassWake = &types.PassWake{}
	for i := range f.input.ObservedState.InstanceStatuses {
		f.input.ObservedState.InstanceStatuses[i].ReadySince = &readySince
	}
	f.input.ObservedState.IndexRows()
	plan, err := workload.BuildPlan(types.ComponentEngine, f.input.DesiredSpec, f.input.ObservedState)
	if err != nil {
		tb.Fatal(err)
	}
	f.plan = plan
	f.target = &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: running, Namespace: f.input.Key.Namespace}}
	f.snapshot = workload.SnapshotWithPodsForTest(f.input, servingComponentPods(f.input, plan, running))
	f.ctx = logr.NewContext(context.Background(), logr.Discard())
	return f
}

// servingComponentPods is every pod of the Component at rest, by Instance:
// each Instance's pods named and labelled as the renderer writes them,
// serving on the revision the Instance's row names.
func servingComponentPods(input types.ReconcileInput, plan types.ComponentPlan, running string) map[int32][]*corev1.Pod {
	byIdx := make(map[int32][]*corev1.Pod, len(plan.Instances))
	for _, inst := range plan.Instances {
		for _, runner := range inst.Runners {
			for ordinal := int32(0); ordinal < runner.Size; ordinal++ {
				name := query.PodName(input.Key.OwnerName, input.Key.Component, inst.Index, runner.Name, ordinal)
				pod := observationPod(name, "node-0", true, true, false)
				pod.Labels = map[string]string{
					query.LabelRevisionHash: query.RevisionHashFromControllerRevisionName(running),
					query.LabelInstanceIdx:  strconv.FormatInt(int64(inst.Index), 10),
					query.LabelRunner:       runner.Name,
				}
				byIdx[inst.Index] = append(byIdx[inst.Index], pod)
			}
		}
	}
	return byIdx
}

// read reads every planned Instance for the roll's open work, as the
// update selection does on every pass.
func (f *rolledSetFixture) read(tb testing.TB) workload.UpdateSelection {
	tb.Helper()
	selection, err := workload.RolledSetReadingForTest(f.ctx, f.input, f.plan, f.target, f.snapshot)
	if err != nil {
		tb.Fatalf("rolled-set reading: %v", err)
	}
	return selection
}

// checkAtRest holds what the reading records over a Component at rest:
// no Instance holds a slot, none is crashed, and no next pass is asked
// for.
func (f *rolledSetFixture) checkAtRest(t *testing.T, shape rolledSetShape) {
	t.Helper()
	selection := f.read(t)
	recorded := len(selection.RolledNotServing) + len(selection.CrashedNotServing) + len(selection.CrashedOutOfRotation)
	if recorded != 0 || f.input.PassWake.Pending() != 0 {
		t.Errorf("%s: the reading recorded %d Instances and asks for the next pass in %s; a Component at rest is the roll's open work nowhere", shape.name, recorded, f.input.PassWake.Pending())
	}
}

// BenchmarkRolledSetReadingAtRest is the cost of reading every Instance of a
// Component at rest for the roll's open work, per shape.
func BenchmarkRolledSetReadingAtRest(b *testing.B) {
	for _, shape := range rolledSetShapes() {
		f := newRolledSetFixture(b, shape)
		b.Run(shape.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				f.read(b)
			}
		})
	}
}

// TestRolledSetReadingAtRestStaysWithinBudget holds the allocations and the
// bytes of the update selection's rolled-set reading over a Component at
// rest, every planned Instance read once, to the recorded golden, at 1,000
// and 4,000 single-pod Instances, 160 gangs of eight and 1,725 gangs of
// two. Beside the golden: the reading records no Instance and asks for
// no next pass.
func TestRolledSetReadingAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "rolled_set_allocations.json"), *updateRolledSetGolden)
	defer golden.Close()
	for _, shape := range rolledSetShapes() {
		f := newRolledSetFixture(t, shape)
		f.checkAtRest(t, shape)
		golden.Check(shape.name+"/rolled-set/allocations", func() { f.read(t) })
		golden.CheckBytes(shape.name+"/rolled-set/bytes", func() { f.read(t) })
	}
}
