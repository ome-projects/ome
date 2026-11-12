package workload_test

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updatePodBucketsGoldens = flag.Bool("update-pod-buckets", false, "re-record testdata/pod_buckets_allocations.json from this run")

// podBucketShape is one shape of a Component at rest: how many Instances
// it has and how many pods each runs.
type podBucketShape struct {
	name            string
	instances, pods int
}

// podBucketShapes are the budget's shapes: the largest reference replica
// of single-pod Instances and a quarter of it, 160 gangs of a leader and
// seven workers, and the largest reference gang service, 1,725 gangs
// of a leader and one worker.
func podBucketShapes() []podBucketShape {
	return []podBucketShape{
		{name: "1000x1", instances: 1000, pods: 1},
		{name: "4000x1", instances: 4000, pods: 1},
		{name: "160x8", instances: 160, pods: 8},
		{name: "1725x2", instances: 1725, pods: 2},
	}
}

// nodesPresentExcept reads every Node as present but the ones it names.
type nodesPresentExcept map[string]struct{}

func (n nodesPresentExcept) NodeExists(_ context.Context, name string) (bool, error) {
	_, missing := n[name]
	return !missing, nil
}

// podBucketsFixture is one Component at rest as the readers of its pods see
// it in one pass: every row Ready on the spec revision with its pods
// serving, the pods as the cache lists them, the plan over the rows, the
// revision the rows run, and the pass's one cached list of the pods, taken
// once from the client and read first by the plan's observation.
type podBucketsFixture struct {
	ctx          context.Context
	client       client.Client
	input        workloadtypes.ReconcileInput
	plan         workloadtypes.ComponentPlan
	target       *appsv1.ControllerRevision
	expectations *workloadtypes.Expectations
	nodes        nodesPresentExcept
	// rows is the store the rows live in, which the create pass writes
	// through and the fixture reads back into the input.
	rows *testAtomicMutationStore
	// pods is the list the pass took from the client, the slice every
	// reader of the pass is handed.
	pods []*corev1.Pod
	// list is the pass's cached list, holding pods.
	list *workloadtypes.CachedPodList
}

// newPodBucketsFixture builds the Component of one shape behind a fake
// client that serves its pods through the OMENative pod index as the
// informer cache does, takes the pass's list once, and reads it through
// the plan's observation, as the first reader of a pass does.
func newPodBucketsFixture(tb testing.TB, shape podBucketShape) *podBucketsFixture {
	tb.Helper()
	const running = "svc-engine-rev-a"
	input := fleetInput(shape.instances, shape.instances, shape.pods, running)
	rows := installTestAtomicMutationStore(&input)
	applyToStore := input.ApplyInstanceMutationsWithRetryBlock
	// A batch whose every mutation the pass found unchanged on its copy
	// is answered as the controller's writer answers it: no read, no
	// Mutate call, no write.
	input.ApplyInstanceMutationsWithRetryBlock = func(ctx context.Context, mutations []workloadtypes.InstanceMutation, targetRevision string, mutateRetryBlock func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition) error {
		if mutateRetryBlock == nil && everyMutationUnchangedOnCopy(mutations) {
			return nil
		}
		return applyToStore(ctx, mutations, targetRevision, mutateRetryBlock)
	}
	input.ObservedState.IndexRows()
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, input.DesiredSpec, input.ObservedState)
	if err != nil {
		tb.Fatal(err)
	}
	target := revisionWithPodSpec(tb, running, input.Key.Namespace, input.DesiredSpec.PodSpec)
	objects := []client.Object{target}
	for i := 0; i < restingFleetNodes; i++ {
		objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i)}})
	}
	for _, pod := range restingComponentPods(input, plan, running) {
		objects = append(objects, pod)
	}
	f := &podBucketsFixture{
		ctx:          logr.NewContext(context.Background(), logr.Discard()),
		input:        input,
		plan:         plan,
		target:       target,
		expectations: workloadtypes.NewExpectations(),
		nodes:        nodesPresentExcept{},
		rows:         rows,
	}
	f.client = fakeclient.New(tb, fakeclient.WithObjects(objects...),
		fakeclient.WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor))
	f.list = workloadtypes.NewCachedPodList(f.client)
	pods, err := f.list.Pods(f.ctx, func(ctx context.Context) ([]*corev1.Pod, error) {
		return query.ListOMENativePodsByName(ctx, f.client, input.Key.Namespace, input.Key.OwnerName, plan.Component, true)
	})
	if err != nil {
		tb.Fatalf("list the pods: %v", err)
	}
	f.pods = pods
	f.settle(tb)
	f.observation(tb, f.list)
	return f
}

// settle runs create passes until one writes no row, reading the rows the
// store holds back into the input after each: the first passes write what
// the fixture leaves to the engine, the Ready time of every row.
func (f *podBucketsFixture) settle(tb testing.TB) {
	tb.Helper()
	for i := 0; i < 8; i++ {
		writesBefore := f.rows.writes
		f.createPass(tb)
		f.rows.sync(&f.input)
		f.input.ObservedState.IndexRows()
		if f.rows.writes == writesBefore {
			return
		}
	}
	tb.Fatalf("the Component did not come to rest in 8 create passes")
}

// everyMutationUnchangedOnCopy reports whether the pass found every
// mutation of the batch unchanged on its copy of the row; an empty batch
// is one.
func everyMutationUnchangedOnCopy(mutations []workloadtypes.InstanceMutation) bool {
	for i := range mutations {
		if !mutations[i].UnchangedOnCopy {
			return false
		}
	}
	return true
}

// listed hands the pass's list out as the cache listed it, so a reader
// that takes a fresh cached list pays for no list of the client.
func (f *podBucketsFixture) listed(context.Context) ([]*corev1.Pod, error) {
	return f.pods, nil
}

// deps are what the pass's readers call out to: the list's client, the
// expectations and the Node reading.
func (f *podBucketsFixture) deps(list *workloadtypes.CachedPodList) workloadtypes.Deps {
	return workloadtypes.Deps{Client: list.Client(), Expectations: f.expectations, NodePresence: f.nodes}
}

// passInput is the input of one pass over list, with the per-pass records
// the engine's entry gives a pass.
func (f *podBucketsFixture) passInput(list *workloadtypes.CachedPodList) workloadtypes.ReconcileInput {
	input := f.input
	input.CachedPods = list
	input.Pacing = &workloadtypes.APIPacing{}
	input.PromoteWindow = &workloadtypes.PromoteWindow{}
	input.PassWake = &workloadtypes.PassWake{}
	input.Swept = &workloadtypes.SweptPods{}
	input.Held = &workloadtypes.HeldInstances{}
	return input
}

// observation reads the pass's pods through the plan's observation over
// list, the first reader of a pass, and returns its buckets by Instance.
func (f *podBucketsFixture) observation(tb testing.TB, list *workloadtypes.CachedPodList) map[int32][]*corev1.Pod {
	tb.Helper()
	input := f.passInput(list)
	snapshot := workload.NewObservedSnapshot(f.deps(list), input, f.plan.Component, input.ObservedState.InstanceStatuses)
	byInstance, err := snapshot.CachedPods(f.ctx)
	if err != nil {
		tb.Fatalf("observation: %v", err)
	}
	return byInstance
}

// freshListObservation is the plan's observation over a list no reader of
// the pass has read yet: the list is taken from the pods as listed, and
// the observation is the first to read it.
func (f *podBucketsFixture) freshListObservation(tb testing.TB) {
	tb.Helper()
	list := workloadtypes.NewCachedPodList(f.client)
	if _, err := list.Pods(f.ctx, f.listed); err != nil {
		tb.Fatalf("take the list: %v", err)
	}
	f.observation(tb, list)
}

// createPass is the create pass over the pass's list after the plan's
// observation read it.
func (f *podBucketsFixture) createPass(tb testing.TB) {
	tb.Helper()
	f.createPassOver(tb, f.list)
}

// createPassOver is the create pass over list.
func (f *podBucketsFixture) createPassOver(tb testing.TB, list *workloadtypes.CachedPodList) {
	tb.Helper()
	if _, err := workloadops.Create(f.ctx, f.deps(list), f.passInput(list), f.plan, f.target); err != nil {
		tb.Fatalf("create pass: %v", err)
	}
}

// freshListPass is the two readers in the order a pass runs them over a
// list no reader has read yet: the plan's observation, then the create
// pass.
func (f *podBucketsFixture) freshListPass(tb testing.TB) {
	tb.Helper()
	list := workloadtypes.NewCachedPodList(f.client)
	if _, err := list.Pods(f.ctx, f.listed); err != nil {
		tb.Fatalf("take the list: %v", err)
	}
	f.observation(tb, list)
	f.createPassOver(tb, list)
}

// checkAtRest holds what the readers of a pass at rest see: the
// observation's buckets hold every Instance's pods in list order, the pass
// hands every reader the one list it took, and the create pass over that
// list lists nothing and writes no pod and no row.
func (f *podBucketsFixture) checkAtRest(t *testing.T, shape podBucketShape) {
	t.Helper()
	byInstance := f.observation(t, f.list)
	if len(byInstance) != shape.instances {
		t.Errorf("%s: %d buckets for %d Instances", shape.name, len(byInstance), shape.instances)
	}
	for _, inst := range f.plan.Instances {
		if got := len(byInstance[inst.Index]); got != shape.pods {
			t.Errorf("%s: Instance %d has %d pods in its bucket, want %d", shape.name, inst.Index, got, shape.pods)
		}
	}
	// The list is in name order, not Instance order; each Instance's bucket
	// holds its pods in the order the list has them.
	seen := make(map[int32]int, shape.instances)
	for _, pod := range f.pods {
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok {
			t.Fatalf("%s: %s carries no Instance index", shape.name, pod.Name)
		}
		at := seen[idx]
		seen[idx]++
		if bucket := byInstance[idx]; at >= len(bucket) || bucket[at] != pod {
			t.Errorf("%s: Instance %d's bucket does not hold %s at %d, the list's order", shape.name, idx, pod.Name, at)
		}
	}
	again, err := f.list.Pods(f.ctx, f.listed)
	if err != nil {
		t.Fatalf("%s: read the list again: %v", shape.name, err)
	}
	if len(again) != len(f.pods) || &again[0] != &f.pods[0] || f.list.Lists() != 1 {
		t.Errorf("%s: a later reader got a list of its own (%d lists)", shape.name, f.list.Lists())
	}
	listsBefore, podWritesBefore, rowWritesBefore := f.list.Lists(), f.list.PodWrites(), f.rows.writes
	f.createPass(t)
	if f.list.Lists() != listsBefore || f.list.PodWrites() != podWritesBefore || f.rows.writes != rowWritesBefore {
		t.Errorf("%s: the create pass listed %d times, wrote %d pods and %d rows; at rest it lists and writes nothing",
			shape.name, f.list.Lists()-listsBefore, f.list.PodWrites()-podWritesBefore, f.rows.writes-rowWritesBefore)
	}
}

// TestPodBucketsLeaveOutAPodOnAMissingNode: a pod bound to a Node the
// cluster no longer holds is in no bucket of the plan's observation and is
// read as gone, while the pass's list still holds it for the readers that
// take the list as the cache gave it.
func TestPodBucketsLeaveOutAPodOnAMissingNode(t *testing.T) {
	f := newPodBucketsFixture(t, podBucketShape{name: "8x1", instances: 8, pods: 1})
	gone := f.pods[3]
	f.nodes = nodesPresentExcept{gone.Spec.NodeName: {}}
	input := f.passInput(f.list)
	snapshot := workload.NewObservedSnapshot(f.deps(f.list), input, f.plan.Component, input.ObservedState.InstanceStatuses)
	byInstance, err := snapshot.CachedPods(f.ctx)
	if err != nil {
		t.Fatalf("observation: %v", err)
	}
	for idx, bucket := range byInstance {
		for _, pod := range bucket {
			if pod == gone {
				t.Errorf("Instance %d's bucket holds %s, a pod on a missing Node", idx, gone.Name)
			}
		}
	}
	if gonePods := snapshot.GonePods(); len(gonePods) != 1 || gonePods[0] != gone {
		t.Errorf("gone pods = %v, want [%s]", gonePods, gone.Name)
	}
	pods, err := f.list.Pods(f.ctx, f.listed)
	if err != nil {
		t.Fatalf("read the list: %v", err)
	}
	if len(pods) != len(f.pods) || pods[3] != gone {
		t.Errorf("the pass's list lost the pod on the missing Node: %d pods", len(pods))
	}
}

// BenchmarkPodBuckets is the cost of the two readers of a pass's pods
// at rest, per shape: the plan's observation over a list no reader has read
// yet, the create pass over the list the observation read, and the two in
// the order a pass runs them over a list no reader has read yet.
func BenchmarkPodBuckets(b *testing.B) {
	for _, shape := range podBucketShapes() {
		f := newPodBucketsFixture(b, shape)
		b.Run(shape.name+"/observation", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				f.freshListObservation(b)
			}
		})
		b.Run(shape.name+"/create-pass", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				f.createPass(b)
			}
		})
		b.Run(shape.name+"/observation-then-create-pass", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				f.freshListPass(b)
			}
		})
	}
}

// TestPodBucketsStayWithinBudget holds the allocations and the bytes of the
// two readers of a pass's pods at rest to the recorded golden: the plan's
// observation over a list no reader has read yet, which buckets the pods by
// Instance, the create pass over the list the observation read, each
// measured alone, and the two in the order a pass runs them over a list no
// reader has read yet, at 1,000 and 4,000 single-pod Instances, 160 gangs
// of eight and 1,725 gangs of two. Beside the golden: the buckets hold
// every Instance's pods in list order, every reader is handed the one list
// the pass took, and the create pass lists and writes nothing.
func TestPodBucketsStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "pod_buckets_allocations.json"), *updatePodBucketsGoldens)
	defer golden.Close()
	for _, shape := range podBucketShapes() {
		f := newPodBucketsFixture(t, shape)
		f.checkAtRest(t, shape)
		golden.Check(shape.name+"/observation/allocations", func() { f.freshListObservation(t) })
		golden.CheckBytes(shape.name+"/observation/bytes", func() { f.freshListObservation(t) })
		golden.Check(shape.name+"/create-pass/allocations", func() { f.createPass(t) })
		golden.CheckBytes(shape.name+"/create-pass/bytes", func() { f.createPass(t) })
		golden.Check(shape.name+"/observation-then-create-pass/allocations", func() { f.freshListPass(t) })
		golden.CheckBytes(shape.name+"/observation-then-create-pass/bytes", func() { f.freshListPass(t) })
	}
}
