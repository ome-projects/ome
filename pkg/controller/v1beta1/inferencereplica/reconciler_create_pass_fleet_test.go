package inferencereplica

import (
	"context"
	"flag"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateCreatePassGoldens = flag.Bool("update-create-pass", false, "re-record testdata/create_pass_allocations.json from this run")

// createPassShapes are the budget's shapes: the status write's three (the
// largest reference replica at 4,000 single-pod Instances, a quarter of
// it, 160 gangs of eight) and the largest reference gang service,
// 1,725 gangs of a leader and one worker.
func createPassShapes() []statusPublicationShape {
	return append(statusPublicationShapes(), statusPublicationShape{name: "gang/1725x2", instances: 1725, pods: 2})
}

// createPassFixture is one replica at rest behind the fake apiserver, with
// the engine's create pass wired through this controller's status closures
// as a pass wires it, and the counts of the replica's live reads, the pod
// lists and the writes the create pass makes.
type createPassFixture struct {
	deps         workloadtypes.Deps
	input        workloadtypes.ReconcileInput
	plan         workloadtypes.ComponentPlan
	target       *appsv1.ControllerRevision
	pods         *podsFromMemory
	replicaReads *fakeclient.Calls
	writes       *fakeclient.Calls
}

// podsFromMemory answers every Pod list from a list held in memory, the
// way the informer cache answers the engine, so the fake apiserver's JSON
// round trip over the pods is not in the count; every other call reaches
// the client. The pods are handed out uncopied: a create pass over a
// replica at rest reads them and writes none.
type podsFromMemory struct {
	client.Client
	pods  []corev1.Pod
	lists int
}

func (c *podsFromMemory) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	pods, ok := list.(*corev1.PodList)
	if !ok {
		return c.Client.List(ctx, list, opts...)
	}
	c.lists++
	pods.Items = c.pods
	return nil
}

// newCreatePassFixture stores the replica in the column form production
// writes, every Instance Ready on the spec revision with every pod serving,
// and builds the engine's input from the pass's decoded copy through the
// controller's own builder, so the create pass commits through the same
// status closures a pass gives it. The create pass is run until one writes
// nothing, so the measured one is the steady one.
func newCreatePassFixture(tb testing.TB, shape statusPublicationShape) *createPassFixture {
	tb.Helper()
	ir := baselineIR("llama-engine", "default", int32(shape.instances))
	setRunnersOfShape(ir, shape)
	ir.Status = *irstatustest.LogicalStatus(irstatustest.ReadySinceRows(shape.instances))
	count := int32(shape.instances)
	ir.Status.ObservedGeneration = ir.Generation
	ir.Status.ReadyReplicas, ir.Status.ServingReplicas, ir.Status.AvailableReplicas = count, count, count
	ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = count, count
	ir.Status.CurrentRevision, ir.Status.UpdateRevision = irstatustest.Revision, irstatustest.Revision
	selection, err := irstatus.SelectCandidate(&ir.Status, publicationColumnBound)
	if err != nil || selection.Selected != irstatus.EncodingColumnarV2 {
		tb.Fatalf("%s: SelectCandidate() = %+v, %v; the column form must be selected", shape.name, selection, err)
	}
	stored := ir.DeepCopy()
	stored.Status = *selection.SelectedCandidate().Status

	pods := make([]corev1.Pod, 0, shape.instances*shape.pods)
	for _, pod := range statusPublicationPods(ir, shape) {
		pods = append(pods, *pod)
	}

	replicaReads := fakeclient.CountingVerb(fakeclient.Get).OnKind(&v1beta1.InferenceReplica{})
	writes := fakeclient.CountingWrites()
	apiserver := fakeclient.New(tb,
		fakeclient.WithObjects(stored),
		fakeclient.WithStatusSubresource(&v1beta1.InferenceReplica{}),
		replicaReads, writes)

	decoder := irstatus.NewDecoder(publicationColumnBound)
	reconciler := &Reconciler{
		Client:                apiserver,
		APIReader:             apiserver,
		Log:                   logf.Log.WithName("create-pass"),
		Recorder:              record.NewFakeRecorder(16),
		Expectations:          workloadtypes.NewExpectations(),
		InstanceStatusDecoder: decoder,
		InstanceStatusTarget:  irstatus.EncodingColumnarV2,
	}
	passCopy := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(context.Background(), irstatus.NewReader(apiserver, decoder), client.ObjectKeyFromObject(stored), passCopy); err != nil {
		tb.Fatalf("%s: decode the pass's copy: %v", shape.name, err)
	}
	input := reconciler.buildReconcileInput(context.Background(), passCopy, passCopy.Spec.Runners, nil, nil, nil, lifecycleSettings{}, 0, coordination.GroupDefaults{})
	component := v1beta1convert.ComponentTypeToWorkload(passCopy.Spec.Component)
	plan, err := workload.BuildPlan(component, input.DesiredSpec, input.ObservedState)
	if err != nil {
		tb.Fatalf("%s: BuildPlan: %v", shape.name, err)
	}
	cache := &podsFromMemory{Client: apiserver, pods: pods}
	f := &createPassFixture{
		deps: workloadtypes.Deps{
			Client:       cache,
			APIReader:    apiserver,
			Recorder:     reconciler.Recorder,
			Expectations: reconciler.Expectations,
		},
		input:        input,
		plan:         plan,
		target:       &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: irstatustest.Revision, Namespace: ir.Namespace}},
		pods:         cache,
		replicaReads: replicaReads,
		writes:       writes,
	}
	f.settle(tb)
	return f
}

// run is one create pass over the fixture, with the per-pass records the
// engine's entry gives a pass.
func (f *createPassFixture) run(tb testing.TB) {
	tb.Helper()
	input := f.input
	input.ObservedState.IndexRows()
	input.Pacing = &workloadtypes.APIPacing{}
	input.PromoteWindow = &workloadtypes.PromoteWindow{}
	input.PassWake = &workloadtypes.PassWake{}
	input.Swept = &workloadtypes.SweptPods{}
	input.Held = &workloadtypes.HeldInstances{}
	if _, err := workloadops.Create(context.Background(), f.deps, input, f.plan, f.target); err != nil {
		tb.Fatalf("Create: %v", err)
	}
}

// runCounting runs one create pass and returns the replica's live reads,
// the pod lists and the writes it made.
func (f *createPassFixture) runCounting(tb testing.TB) (replicaReads, podLists, writes int) {
	tb.Helper()
	readsBefore, listsBefore, writesBefore := f.replicaReads.Count(), f.pods.lists, f.writes.Count()
	f.run(tb)
	return f.replicaReads.Count() - readsBefore, f.pods.lists - listsBefore, f.writes.Count() - writesBefore
}

// settle runs create passes until one writes nothing: the first ones write
// what the fixture leaves to the controller.
func (f *createPassFixture) settle(tb testing.TB) {
	tb.Helper()
	for i := 0; i < 8; i++ {
		if _, _, writes := f.runCounting(tb); writes == 0 {
			return
		}
	}
	tb.Fatalf("the replica did not come to rest in 8 create passes")
}

// BenchmarkCreatePassAtRest is the cost of the create pass of one pass over
// a replica at rest at fleet size, with the replica's live reads it makes
// beside the time.
func BenchmarkCreatePassAtRest(b *testing.B) {
	for _, shape := range createPassShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newCreatePassFixture(b, shape)
			b.ReportAllocs()
			start := f.replicaReads.Count()
			b.ResetTimer()
			for b.Loop() {
				f.run(b)
			}
			b.StopTimer()
			b.ReportMetric(float64(f.replicaReads.Count()-start)/float64(b.N), "replica-reads/op")
		})
	}
}

// TestCreatePassAtRestStaysWithinBudget holds the allocations and the bytes
// of the create pass of one pass over a replica at rest, at 1,000 and 4,000
// single-pod Instances, at 160 gangs of eight and at 1,725 gangs of two, to
// the recorded golden, with the live reads of the replica and the pod lists
// it makes. A create pass over a replica at rest reads the replica live no
// times, since its finishes are found unchanged on the pass's copy, and
// writes nothing.
func TestCreatePassAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "create_pass_allocations.json"), *updateCreatePassGoldens)
	defer golden.Close()
	for _, shape := range createPassShapes() {
		f := newCreatePassFixture(t, shape)
		golden.Check(shape.name+"/allocations", func() { f.run(t) })
		golden.CheckBytes(shape.name+"/bytes", func() { f.run(t) })
		replicaReads, podLists, writes := f.runCounting(t)
		golden.CheckCount(shape.name+"/replica-live-reads", int64(replicaReads))
		golden.CheckCount(shape.name+"/pod-lists", int64(podLists))
		if replicaReads != 0 {
			t.Errorf("%s: the create pass read the replica live %d times; a batch of finishes found unchanged on the pass's copy is answered without a read", shape.name, replicaReads)
		}
		if writes != 0 {
			t.Errorf("%s: the create pass wrote %d times; a replica at rest is not written", shape.name, writes)
		}
	}
}
