package workload_test

import (
	"context"
	"flag"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/holds"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updatePlanRecordsGoldens = flag.Bool("update-plan-records", false, "re-record testdata/plan_records_allocations.json from this run")

// planRecordShape is one shape of a Component at rest: how many Instances
// it has and how many pods each runs.
type planRecordShape struct {
	name            string
	instances, pods int
}

// planRecordShapes are the budget's shapes: the largest reference replica
// of single-pod Instances and a quarter of it, 160 gangs of a leader and
// seven workers, and the largest reference gang service, 1,725 gangs
// of a leader and one worker.
func planRecordShapes() []planRecordShape {
	return []planRecordShape{
		{name: "1000x1", instances: 1000, pods: 1},
		{name: "4000x1", instances: 4000, pods: 1},
		{name: "160x8", instances: 160, pods: 8},
		{name: "1725x2", instances: 1725, pods: 2},
	}
}

// planRecordsFixture is one Component at rest as the per-row records of a
// pass read it: every row Ready on the spec revision with its pods
// serving, the rows indexed as a pass indexes them on entry, the pods as
// the cache lists them, the plan over the rows and the revision the rows
// run. The logger is the manager's, zap at the info level, which takes
// none of the pass's debug lines.
type planRecordsFixture struct {
	ctx      context.Context
	input    workloadtypes.ReconcileInput
	plan     workloadtypes.ComponentPlan
	target   *appsv1.ControllerRevision
	snapshot *workload.ObservedSnapshot
	pods     []*corev1.Pod
	byIdx    map[int32][]*corev1.Pod
	podsFn   func(context.Context) (map[int32][]*corev1.Pod, error)
	// writes counts the row mutations the fixture was asked for.
	writes int
}

// newPlanRecordsFixture builds the resting Component of one shape.
func newPlanRecordsFixture(tb testing.TB, shape planRecordShape) *planRecordsFixture {
	tb.Helper()
	const running = "svc-engine-rev-a"
	f := &planRecordsFixture{input: fleetInput(shape.instances, shape.instances, shape.pods, running)}
	f.input.MutateInstance = func(context.Context, int32, func(*workloadtypes.InstanceStatus) bool) error {
		f.writes++
		return nil
	}
	// The rows are indexed once, as a pass indexes them on entry, so every
	// lookup of a row by Instance is one map read.
	f.input.ObservedState.IndexRows()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, f.input.DesiredSpec, f.input.ObservedState)
	if err != nil {
		tb.Fatal(err)
	}
	f.plan = plan
	f.target = &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: running, Namespace: f.input.Key.Namespace}}
	f.pods = restingComponentPods(f.input, plan, running)
	f.byIdx = query.BucketPodsByInstanceIdx(f.pods)
	f.podsFn = func(context.Context) (map[int32][]*corev1.Pod, error) { return f.byIdx, nil }
	f.snapshot = workload.SnapshotWithPodsForTest(f.input, f.byIdx)
	f.ctx = logr.NewContext(context.Background(), zap.New(zap.WriteTo(io.Discard)))
	return f
}

// restingComponentPods is every pod of the Component at rest, in Instance,
// runner and ordinal order: each Instance's pods as the renderer labels
// them, serving on the revision the Instance's row names.
func restingComponentPods(input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan, running string) []*corev1.Pod {
	var pods []*corev1.Pod
	for _, inst := range plan.Instances {
		for _, runner := range inst.Runners {
			for ordinal := int32(0); ordinal < runner.Size; ordinal++ {
				pods = append(pods, restingRunnerPod(input, inst.Index, runner.Name, ordinal, running))
			}
		}
	}
	return pods
}

// buckets groups the Component's pods by Instance, as the plan's
// observation does once per pass, and the create pass, the promotion and
// the publication again.
func (f *planRecordsFixture) buckets() {
	query.BucketPodsByInstanceIdx(f.pods)
}

// holdPass builds the hold pass's rows and runs every authority over them,
// as the top of a pass does.
func (f *planRecordsFixture) holdPass(tb testing.TB) holds.Result {
	tb.Helper()
	result, err := holds.Run(f.ctx, holds.PassInput{
		Input: f.input,
		Plan:  f.plan,
		Rows:  holds.RowsForPlan(f.plan, f.input.ObservedState.InstanceStatuses, nil),
		Pods:  f.podsFn,
	})
	if err != nil {
		tb.Fatalf("hold pass: %v", err)
	}
	return result
}

// escalationPass runs the deadline and failure pass over every row, as the
// end of a pass does.
func (f *planRecordsFixture) escalationPass(tb testing.TB) {
	tb.Helper()
	err := escalation.Run(f.ctx, escalation.PassInput{
		Input:  f.input,
		Plan:   f.plan,
		Target: f.target,
		Pods:   f.podsFn,
	})
	if err != nil {
		tb.Fatalf("escalation pass: %v", err)
	}
}

// scaleDownScan names the rows the plan does not cover, as the plan does
// once per pass and the replica pass twice more.
func (f *planRecordsFixture) scaleDownScan() []int32 {
	return workload.ScaleDownExtras(f.input.ObservedState.InstanceStatuses, f.plan)
}

// scaleDownPicture is the shape's Component after its count fell to three
// quarters of its rows: the plan keeps three rows in four and the scan
// names the rest.
func scaleDownPicture(tb testing.TB, shape planRecordShape) *planRecordsFixture {
	tb.Helper()
	f := &planRecordsFixture{input: fleetInput(shape.instances*3/4, shape.instances, shape.pods, "svc-engine-rev-a")}
	f.input.ObservedState.IndexRows()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, f.input.DesiredSpec, f.input.ObservedState)
	if err != nil {
		tb.Fatal(err)
	}
	f.plan = plan
	return f
}

// checkScaleDownScan holds the scan to the plan: the rows the plan has no
// entry for, in row order, are the extras, and every other row is kept.
func (f *planRecordsFixture) checkScaleDownScan(t *testing.T, shape planRecordShape, wantExtras int) {
	t.Helper()
	planned := make(map[int32]struct{}, len(f.plan.Instances))
	for _, inst := range f.plan.Instances {
		planned[inst.Index] = struct{}{}
	}
	var want []int32
	for _, row := range f.input.ObservedState.InstanceStatuses {
		if _, ok := planned[row.Index]; !ok {
			want = append(want, row.Index)
		}
	}
	got := f.scaleDownScan()
	if len(want) != wantExtras || len(got) != len(want) {
		t.Fatalf("%s: the scan names %d rows, the plan leaves %d uncovered, want %d", shape.name, len(got), len(want), wantExtras)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: extra %d is row %d, want row %d", shape.name, i, got[i], want[i])
		}
	}
}

// updateSelection reads every planned Instance for the roll, as the plan
// does.
func (f *planRecordsFixture) updateSelection(tb testing.TB) (workload.UpdateSelection, time.Duration) {
	tb.Helper()
	selection, wait, err := workload.PlanUpdateSelectionForTest(f.ctx, f.input, f.plan, f.target, f.snapshot)
	if err != nil {
		tb.Fatalf("update selection: %v", err)
	}
	return selection, wait
}

// checkAtRest holds what the records read over a Component at rest: one
// bucket per Instance holding its pods in list order, a hold pass and an
// escalation pass that write no row, a scan that names no row, and a
// selection that starts nothing and waits on nothing.
func (f *planRecordsFixture) checkAtRest(t *testing.T, shape planRecordShape) {
	t.Helper()
	byIdx := query.BucketPodsByInstanceIdx(f.pods)
	if len(byIdx) != shape.instances {
		t.Errorf("%s: %d buckets for %d Instances", shape.name, len(byIdx), shape.instances)
	}
	next := 0
	for _, inst := range f.plan.Instances {
		bucket := byIdx[inst.Index]
		if len(bucket) != shape.pods {
			t.Errorf("%s: Instance %d has %d pods in its bucket, want %d", shape.name, inst.Index, len(bucket), shape.pods)
			continue
		}
		for _, pod := range bucket {
			if pod != f.pods[next] {
				t.Errorf("%s: Instance %d's bucket holds %s where the list has %s", shape.name, inst.Index, pod.Name, f.pods[next].Name)
			}
			next++
		}
	}
	writesBefore := f.writes
	f.holdPass(t)
	if f.writes != writesBefore {
		t.Errorf("%s: the hold pass wrote %d rows; a Component at rest is not written", shape.name, f.writes-writesBefore)
	}
	f.escalationPass(t)
	if f.writes != writesBefore {
		t.Errorf("%s: the escalation pass wrote %d rows; a Component at rest is not written", shape.name, f.writes-writesBefore)
	}
	f.checkScaleDownScan(t, shape, 0)
	selection, wait := f.updateSelection(t)
	listed := len(selection.Items) + len(selection.RetryBlockDenied) + len(selection.NotServingOutsideRoll) + len(selection.RolledNotServing)
	if listed != 0 || wait != 0 {
		t.Errorf("%s: the selection lists %d Instances and waits %s; nothing moves at rest", shape.name, listed, wait)
	}
}

// BenchmarkPlanRecordsAtRest is the cost of each per-row record of one
// pass over a Component at rest, per shape, and of the scale-down scan over
// the same Component with a quarter of its rows leaving.
func BenchmarkPlanRecordsAtRest(b *testing.B) {
	for _, shape := range planRecordShapes() {
		f := newPlanRecordsFixture(b, shape)
		leaving := scaleDownPicture(b, shape)
		sites := []struct {
			name string
			run  func()
		}{
			{name: "pod-buckets", run: f.buckets},
			{name: "hold-pass", run: func() { f.holdPass(b) }},
			{name: "update-selection", run: func() { f.updateSelection(b) }},
			{name: "escalation-pass", run: func() { f.escalationPass(b) }},
			{name: "scale-down-scan-at-rest", run: func() { f.scaleDownScan() }},
			{name: "scale-down-scan-quarter-leaving", run: func() { leaving.scaleDownScan() }},
		}
		for _, site := range sites {
			b.Run(shape.name+"/"+site.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					site.run()
				}
			})
		}
	}
}

// TestPlanRecordsAtRestStayWithinBudget holds the allocations and the bytes
// of the per-row records of one pass over a Component at rest, each
// measured alone, to the recorded golden: the pod buckets by Instance, the
// hold pass over every row, the update selection over every planned
// Instance, the escalation pass over every row, and the scale-down scan
// over every row, at rest and with a quarter of the rows leaving, at 1,000
// and 4,000 single-pod Instances, 160 gangs of eight and 1,725 gangs of
// two. Beside the golden: the buckets hold every Instance's pods in list
// order, the hold pass and the escalation pass write nothing, the scan
// names the rows the plan leaves out and no other, and the selection
// starts nothing.
func TestPlanRecordsAtRestStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "plan_records_allocations.json"), *updatePlanRecordsGoldens)
	defer golden.Close()
	for _, shape := range planRecordShapes() {
		f := newPlanRecordsFixture(t, shape)
		f.checkAtRest(t, shape)
		leaving := scaleDownPicture(t, shape)
		leaving.checkScaleDownScan(t, shape, shape.instances-shape.instances*3/4)
		golden.Check(shape.name+"/pod-buckets/allocations", f.buckets)
		golden.CheckBytes(shape.name+"/pod-buckets/bytes", f.buckets)
		golden.Check(shape.name+"/hold-pass/allocations", func() { f.holdPass(t) })
		golden.CheckBytes(shape.name+"/hold-pass/bytes", func() { f.holdPass(t) })
		golden.Check(shape.name+"/update-selection/allocations", func() { f.updateSelection(t) })
		golden.CheckBytes(shape.name+"/update-selection/bytes", func() { f.updateSelection(t) })
		golden.Check(shape.name+"/escalation-pass/allocations", func() { f.escalationPass(t) })
		golden.CheckBytes(shape.name+"/escalation-pass/bytes", func() { f.escalationPass(t) })
		golden.Check(shape.name+"/scale-down-scan-at-rest/allocations", func() { f.scaleDownScan() })
		golden.CheckBytes(shape.name+"/scale-down-scan-at-rest/bytes", func() { f.scaleDownScan() })
		golden.Check(shape.name+"/scale-down-scan-quarter-leaving/allocations", func() { leaving.scaleDownScan() })
		golden.CheckBytes(shape.name+"/scale-down-scan-quarter-leaving/bytes", func() { leaving.scaleDownScan() })
	}
}
