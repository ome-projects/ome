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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updatePassAtRestGoldens = flag.Bool("update-pass-at-rest", false, "re-record testdata/pass_at_rest_allocations.json from this run")

// restingFleetNodes is how many Nodes the pods of a resting fleet spread
// over.
const restingFleetNodes = 8

// restingFleetSizes are the budget's shapes: the largest reference replica
// at 4,000 single-pod Instances and a quarter of it.
var restingFleetSizes = []int{1000, 4000}

// restingFleet is one Component of single-pod Instances at rest: every row
// Ready on the spec revision, every pod Running, Ready and serving on a
// Node the cluster holds, behind a fake client that serves the pods through
// the OMENative pod index as the informer cache does, with the pod lists
// and the writes of each pass counted. The rows live in the in-memory
// store the engine's tests write through.
type restingFleet struct {
	client       client.Client
	input        workloadtypes.ReconcileInput
	plan         workloadtypes.ComponentPlan
	target       *appsv1.ControllerRevision
	expectations *workloadtypes.Expectations
	rows         *testAtomicMutationStore
	podLists     *fakeclient.Calls
	writes       *fakeclient.Calls
}

// newRestingFleet builds the fleet and runs passes over it until one writes
// nothing, so the measured pass is the steady one.
func newRestingFleet(t testing.TB, ctx context.Context, instances int) *restingFleet {
	t.Helper()
	const running = "svc-engine-rev-a"
	input := fleetInput(instances, instances, 1, running)
	rows := installTestAtomicMutationStore(&input)
	plan, err := workload.BuildPlan(workloadtypes.ComponentEngine, input.DesiredSpec, input.ObservedState)
	if err != nil {
		t.Fatal(err)
	}
	target := revisionWithPodSpec(t, running, input.Key.Namespace, input.DesiredSpec.PodSpec)
	objects := []client.Object{target}
	for i := 0; i < restingFleetNodes; i++ {
		objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("node-%d", i)}})
	}
	for i := 0; i < instances; i++ {
		objects = append(objects, restingPod(input, int32(i), running))
	}
	podLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&corev1.PodList{})
	writes := fakeclient.CountingWrites()
	c := fakeclient.New(t, fakeclient.WithObjects(objects...),
		fakeclient.WithIndex(&corev1.Pod{}, query.OMENativePodIndexField, query.OMENativePodIndexExtractor),
		podLists, writes)
	f := &restingFleet{
		client:       c,
		input:        input,
		plan:         plan,
		target:       target,
		expectations: workloadtypes.NewExpectations(),
		rows:         rows,
		podLists:     podLists,
		writes:       writes,
	}
	f.settle(t, ctx)
	return f
}

// restingPod is the serving pod of the single-pod Instance at index as the
// renderer labels it, running the revision the Instance's row names, bound
// to one of the fleet's Nodes.
func restingPod(input workloadtypes.ReconcileInput, index int32, running string) *corev1.Pod {
	return restingRunnerPod(input, index, workloadtypes.RunnerDefault, 0, running)
}

// restingRunnerPod is the serving pod at ordinal of the runner of the
// Instance at index as the renderer labels it, running the revision the
// Instance's row names, bound to one of the fleet's Nodes.
func restingRunnerPod(input workloadtypes.ReconcileInput, index int32, runner string, ordinal int32, running string) *corev1.Pod {
	key := input.Key
	pod := testfixture.ServingPod(query.PodName(key.OwnerName, key.Component, index, runner, ordinal))
	pod.Namespace = key.Namespace
	pod.Labels = testfixture.PodLabels(key.OwnerName, key.Component, index, runner, 1, ordinal)
	pod.Labels[query.LabelRevisionHash] = query.RevisionFromName(running).Hash()
	pod.Spec.NodeName = fmt.Sprintf("node-%d", int(index)%restingFleetNodes)
	pod.Spec.Containers = input.DesiredSpec.PodSpec.DeepCopy().Containers
	return pod
}

// pass runs one engine pass over the fleet with the deps and the input a
// pass is given: the pass's cached pod list over the client, as the adapter
// wires it.
func (f *restingFleet) pass(ctx context.Context) error {
	cachedPods := workloadtypes.NewCachedPodList(f.client)
	input := f.input
	input.CachedPods = cachedPods
	deps := workloadtypes.Deps{Client: cachedPods.Client(), Expectations: f.expectations}
	_, err := workload.Reconcile(ctx, deps, input, f.plan, f.target)
	return err
}

// passCounting runs one pass and returns the pod lists it made through the
// client, and its writes: the client's and the rows'.
func (f *restingFleet) passCounting(t testing.TB, ctx context.Context) (podLists, writes int) {
	t.Helper()
	listsBefore, writesBefore, rowWritesBefore := f.podLists.Count(), f.writes.Count(), f.rows.writes
	if err := f.pass(ctx); err != nil {
		t.Fatalf("pass: %v", err)
	}
	f.rows.sync(&f.input)
	return f.podLists.Count() - listsBefore, f.writes.Count() - writesBefore + f.rows.writes - rowWritesBefore
}

// settle runs passes until one writes nothing: the first ones write what
// the fixture leaves to the engine.
func (f *restingFleet) settle(t testing.TB, ctx context.Context) {
	t.Helper()
	for i := 0; i < 8; i++ {
		if _, writes := f.passCounting(t, ctx); writes == 0 {
			return
		}
	}
	t.Fatalf("the fleet did not come to rest in 8 passes")
}

// passCost is the cost of one pass over the resting fleet: the smallest of
// allocbudget.Samples passes after one that is not read. A pass at rest does
// the same work every time, so the first fills the buffer pools the
// stand-in's encoders draw from and the reading is the pass's own cost
// whatever the collector did to the pools; another goroutine can add to a
// reading, never take from it, so the smallest is the pass's own.
func (f *restingFleet) passCost(t testing.TB, ctx context.Context) (allocations, bytes int64) {
	t.Helper()
	var err error
	allocations, bytes = allocbudget.Smallest(func() {
		if err == nil {
			err = f.pass(ctx)
		}
	})
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	return allocations, bytes
}

// BenchmarkPassAtRest is the cost of one engine pass over a Component at
// rest at fleet size, with the pod lists it makes beside the time.
func BenchmarkPassAtRest(b *testing.B) {
	ctx := logr.NewContext(context.Background(), logr.Discard())
	for _, instances := range restingFleetSizes {
		b.Run(fmt.Sprintf("%dx1", instances), func(b *testing.B) {
			f := newRestingFleet(b, ctx, instances)
			b.ReportAllocs()
			start := f.podLists.Count()
			b.ResetTimer()
			for b.Loop() {
				if err := f.pass(ctx); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(f.podLists.Count()-start)/float64(b.N), "pod-lists/op")
		})
	}
}

// TestPassAtRestStaysWithinBudget holds the cost of one engine pass over a
// Component at rest, at 1,000 and 4,000 single-pod Instances, to the
// recorded golden: its allocations and its bytes at the golden's tolerance,
// and its cached lists of the Component's pods exactly. A pass at rest
// writes nothing.
func TestPassAtRestStaysWithinBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("fleet-size budget; skipped under -short")
	}
	golden := allocbudget.Open(t, filepath.Join("testdata", "pass_at_rest_allocations.json"), *updatePassAtRestGoldens)
	defer golden.Close()
	ctx := logr.NewContext(context.Background(), logr.Discard())
	for _, instances := range restingFleetSizes {
		name := fmt.Sprintf("%dx1", instances)
		f := newRestingFleet(t, ctx, instances)
		allocations, bytes := f.passCost(t, ctx)
		golden.CheckCount(name+"/allocations", allocations)
		golden.CheckCount(name+"/bytes", bytes)
		podLists, writes := f.passCounting(t, ctx)
		golden.CheckCount(name+"/pod-lists", int64(podLists))
		if writes != 0 {
			t.Errorf("%s: the pass wrote %d times; a Component at rest is not written", name, writes)
		}
	}
}
