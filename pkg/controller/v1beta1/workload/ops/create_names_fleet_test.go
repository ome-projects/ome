package ops

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateCreateNamesGoldens = flag.Bool("update-create-names", false, "re-record testdata/create_names_allocations.json from this run")

// createNamesShape is one Component at rest the Create pass reads its pod
// names over: its Instances and the runners of each.
type createNamesShape struct {
	name      string
	instances int
	runners   []workload.RunnerPlan
}

// createNamesShapes are the budget's shapes: the largest reference replica
// at 4,000 single-pod Instances, 160 gangs of a leader and seven workers,
// and the largest reference gang service, 1,725 gangs of a leader
// and one worker.
func createNamesShapes() []createNamesShape {
	return []createNamesShape{
		{name: "4000x1", instances: 4000, runners: []workload.RunnerPlan{{Name: workload.RunnerDefault, Size: 1}}},
		{name: "160x8", instances: 160, runners: []workload.RunnerPlan{{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: 7}}},
		{name: "1725x2", instances: 1725, runners: []workload.RunnerPlan{{Name: workload.RunnerLeader, Size: 1}, {Name: workload.RunnerWorker, Size: 1}}},
	}
}

const (
	createNamesOwner     = "llama-70b"
	createNamesNamespace = "prod"
	createNamesRevision  = "llama-70b-engine-5f7d9c2b1a"
)

// createNamesFixture is one Component at rest: every Instance Ready on one
// revision with its row indexed as a pass indexes it, every pod of every
// Instance Running and reported Ready by its kubelet, and the force-delete
// policy of the reference shapes, under which a Running pod whose Ready the control
// plane took away is read too.
type createNamesFixture struct {
	input      workload.ReconcileInput
	plan       workload.ComponentPlan
	byInstance map[int32][]*corev1.Pod
}

func newCreateNamesFixture(shape createNamesShape) *createNamesFixture {
	component := workload.ComponentEngine
	plan := workload.ComponentPlan{
		Component:            component,
		Replicas:             int32(shape.instances),
		Instances:            make([]workload.InstancePlan, shape.instances),
		InstanceReadyTimeout: 30 * time.Minute,
	}
	rows := make([]workload.InstanceStatus, shape.instances)
	byInstance := make(map[int32][]*corev1.Pod, shape.instances)
	for i := range plan.Instances {
		index := int32(i)
		plan.Instances[i] = workload.InstancePlan{Index: index, Incarnation: 1, Runners: shape.runners}
		rows[i] = workload.InstanceStatus{Index: index, Incarnation: 1, Phase: workload.InstancePhaseReady, RunningRevision: createNamesRevision}
		for _, runner := range shape.runners {
			for ordinal := int32(0); ordinal < runner.Size; ordinal++ {
				byInstance[index] = append(byInstance[index], reportedReadyPod(query.PodName(createNamesOwner, component, index, runner.Name, ordinal)))
			}
		}
	}
	input := workload.ReconcileInput{
		Key:           workload.Key{Namespace: createNamesNamespace, OwnerName: createNamesOwner, Component: component},
		ObservedState: workload.WorkloadObservedState{InstanceStatuses: rows},
		ForceDelete:   &workload.ForceDeletePolicy{OverdueSlack: 2 * time.Minute, NodeUnreachableThreshold: 5 * time.Minute},
		Clock:         clocktesting.NewFakeClock(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)),
	}
	input.ObservedState.IndexRows()
	return &createNamesFixture{input: input, plan: plan, byInstance: byInstance}
}

// reportedReadyPod is a pod whose kubelet reports it Running and Ready.
func reportedReadyPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: createNamesNamespace},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

// lastInstance is the Instance with the highest index, whose names carry
// the most digits.
func (f *createNamesFixture) lastInstance() workload.InstancePlan {
	return f.plan.Instances[len(f.plan.Instances)-1]
}

// sweep is the Create pass's sweep of pods with no report from their
// kubelet over the whole Component, as every Create pass runs it.
func (f *createNamesFixture) sweep(tb testing.TB) (map[int32]bool, time.Time) {
	tb.Helper()
	held, at, err := recoverUnknownPhaseInstances(context.Background(), workload.Deps{}, f.input, f.plan, f.byInstance)
	if err != nil {
		tb.Fatalf("recoverUnknownPhaseInstances: %v", err)
	}
	return held, at
}

// BenchmarkExpectedPodNames is the cost of reading one Instance's pod names,
// as the Create pass reads them for every Instance on every pass.
func BenchmarkExpectedPodNames(b *testing.B) {
	for _, shape := range createNamesShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newCreateNamesFixture(shape)
			inst := f.lastInstance()
			b.ReportAllocs()
			for b.Loop() {
				expectedPodNamesForInstance(f.input, f.plan, inst)
			}
		})
	}
}

// BenchmarkUnknownPhaseSweepAtRest is the cost of the Create pass's sweep
// of unreported pods over a Component whose every pod reports.
func BenchmarkUnknownPhaseSweepAtRest(b *testing.B) {
	for _, shape := range createNamesShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newCreateNamesFixture(shape)
			b.ReportAllocs()
			for b.Loop() {
				f.sweep(b)
			}
		})
	}
}

// TestCreateNamesAtRestStayWithinBudget holds, per shape, the allocations
// and the bytes of reading one Instance's pod names and of the Create
// pass's sweep of unreported pods over the whole Component at rest, to the
// recorded golden. Beside the golden: the names read are the Instance's
// pods' names in runner and ordinal order, and the sweep over a Component
// whose every pod reports holds no Instance and names no next moment.
func TestCreateNamesAtRestStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "create_names_allocations.json"), *updateCreateNamesGoldens)
	defer golden.Close()
	for _, shape := range createNamesShapes() {
		f := newCreateNamesFixture(shape)
		inst := f.lastInstance()
		f.holdNames(t, shape, inst)
		golden.Check(shape.name+"/names-of-one-instance/allocations", func() { expectedPodNamesForInstance(f.input, f.plan, inst) })
		golden.CheckBytes(shape.name+"/names-of-one-instance/bytes", func() { expectedPodNamesForInstance(f.input, f.plan, inst) })
		if held, at := f.sweep(t); len(held) != 0 || !at.IsZero() {
			t.Errorf("%s: the sweep over a Component whose every pod reports held %d Instance(s) and named %v; it holds none and names no moment", shape.name, len(held), at)
		}
		golden.Check(shape.name+"/sweep/allocations", func() { f.sweep(t) })
		golden.CheckBytes(shape.name+"/sweep/bytes", func() { f.sweep(t) })
	}
}

// holdNames holds the names read for inst to the pods' names: one per
// runner and ordinal, in that order, each the formatted form.
func (f *createNamesFixture) holdNames(t *testing.T, shape createNamesShape, inst workload.InstancePlan) {
	t.Helper()
	got := expectedPodNamesForInstance(f.input, f.plan, inst)
	if len(got) != int(inst.TotalPods()) {
		t.Fatalf("%s: %d names read for an Instance of %d pods", shape.name, len(got), inst.TotalPods())
	}
	i := 0
	for _, runner := range inst.Runners {
		for ordinal := int32(0); ordinal < runner.Size; ordinal++ {
			want := fmt.Sprintf("%s-%s-%d-%s-%d", createNamesOwner, f.plan.Component, inst.Index, runner.Name, ordinal)
			if got[i].Name != want || got[i].Ordinal != ordinal || got[i].Runner != runner {
				t.Errorf("%s: name %d read as %+v, want %q at ordinal %d of runner %s", shape.name, i, got[i], want, ordinal, runner.Name)
			}
			i++
		}
	}
}
