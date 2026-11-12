package inferencereplica

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateStatusPublicationGoldens = flag.Bool("update-status-publication", false, "re-record testdata/status_publication_allocations.json from this run")

// statusPublicationShape is one replica the status write runs over: its
// Instances, and the pods of each, one or a leader with seven workers.
type statusPublicationShape struct {
	name      string
	instances int
	pods      int
}

// statusPublicationShapes are the budget's shapes: the largest reference
// replica at 4,000 Instances, a quarter of it, and 160 gangs of eight.
func statusPublicationShapes() []statusPublicationShape {
	return []statusPublicationShape{
		{name: "readySince/1000", instances: 1000, pods: 1},
		{name: "readySince/4000", instances: 4000, pods: 1},
		{name: "gang/160x8", instances: 160, pods: 8},
	}
}

// statusPublicationFixture is one replica at rest behind the fake
// apiserver, the pass's copy decoded from it as a pass decodes its entry
// read, the Component's pods as the pass lists them, and the counts of the
// replica's live reads and the writes the status write makes.
type statusPublicationFixture struct {
	reconciler *Reconciler
	key        client.ObjectKey
	passCopy   *v1beta1.InferenceReplica
	pods       []*corev1.Pod
	// list is the pass's cached list holding pods, the one the status
	// write reads.
	list             *workloadtypes.CachedPodList
	plan             workloadtypes.ComponentPlan
	target           *appsv1.ControllerRevision
	instancesAtStart []int32
	apiserver        client.Client
	replicaReads     *fakeclient.Calls
	writes           *fakeclient.Calls
}

// slicesFromMemory answers every EndpointSlice list from a list held in
// memory, the way the informer cache answers the availability reading, so
// the fake apiserver's JSON round trip over the slices is not in the count;
// every other call reaches the client.
type slicesFromMemory struct {
	client.Client
	slices []discoveryv1.EndpointSlice
}

func (c *slicesFromMemory) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	slices, ok := list.(*discoveryv1.EndpointSliceList)
	if !ok {
		return c.Client.List(ctx, list, opts...)
	}
	slices.Items = c.slices
	return nil
}

// everyNodePresent is the pass's Node reading over a cluster that has lost
// no Node under the pods.
type everyNodePresent struct{}

func (everyNodePresent) NodeExists(context.Context, string) (bool, error) { return true, nil }

// newStatusPublicationFixture stores the replica in the column form
// the manager writes, every Instance Ready on the spec revision with every
// pod serving and a Ready endpoint, and decodes the pass's copy from the
// store. The status write is run until one writes nothing, so the measured
// one is the steady one.
func newStatusPublicationFixture(tb testing.TB, shape statusPublicationShape) *statusPublicationFixture {
	tb.Helper()
	ir := baselineIR("llama-engine", "default", int32(shape.instances))
	setRunnersOfShape(ir, shape)
	ir.Status = *irstatustest.LogicalStatus(irstatustest.ReadySinceRows(shape.instances))
	count := int32(shape.instances)
	ir.Status.ObservedGeneration = ir.Generation
	ir.Status.ReadyReplicas, ir.Status.ServingReplicas, ir.Status.AvailableReplicas = count, count, count
	ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = count, count
	ir.Status.UpdateRevision = irstatustest.Revision
	selection, err := irstatus.SelectCandidate(&ir.Status, publicationColumnBound)
	if err != nil || selection.Selected != irstatus.EncodingColumnarV2 {
		tb.Fatalf("%s: SelectCandidate() = %+v, %v; the column form must be selected", shape.name, selection, err)
	}
	stored := ir.DeepCopy()
	stored.Status = *selection.SelectedCandidate().Status

	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	pods := statusPublicationPods(ir, shape)
	var slices []discoveryv1.EndpointSlice
	for _, obj := range publicationEndpointSlices(ir, component, pods) {
		slices = append(slices, *obj.(*discoveryv1.EndpointSlice))
	}

	replicaReads := fakeclient.CountingVerb(fakeclient.Get).OnKind(&v1beta1.InferenceReplica{})
	writes := fakeclient.CountingWrites()
	apiserver := fakeclient.New(tb,
		fakeclient.WithObjects(stored),
		fakeclient.WithStatusSubresource(&v1beta1.InferenceReplica{}),
		replicaReads, writes)
	decoder := irstatus.NewDecoder(publicationColumnBound)
	reconciler := &Reconciler{
		Client:                &slicesFromMemory{Client: apiserver, slices: slices},
		APIReader:             apiserver,
		Log:                   logf.Log.WithName("status-publication"),
		Recorder:              record.NewFakeRecorder(16),
		Expectations:          workloadtypes.NewExpectations(),
		InstanceStatusDecoder: decoder,
		InstanceStatusTarget:  irstatus.EncodingColumnarV2,
	}
	list, _ := passListOf(tb, reconciler.Client, pods)
	f := &statusPublicationFixture{
		reconciler:   reconciler,
		key:          client.ObjectKeyFromObject(stored),
		pods:         pods,
		list:         list,
		plan:         statusPublicationPlan(component, shape),
		target:       &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: irstatustest.Revision, Namespace: ir.Namespace}},
		apiserver:    apiserver,
		replicaReads: replicaReads,
		writes:       writes,
	}
	f.decodePassCopy(tb)
	f.settle(tb)
	// The measured pass decodes its copy from the store, as every pass does.
	f.decodePassCopy(tb)
	return f
}

// setRunnersOfShape gives the replica the shape's runners: the one default
// runner it has for a single-pod Instance, a leader and the workers for a
// gang.
func setRunnersOfShape(ir *v1beta1.InferenceReplica, shape statusPublicationShape) {
	if shape.pods <= 1 {
		return
	}
	template := ir.Spec.Runners[0].Template
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: template},
		{Name: v1beta1.RunnerNameWorker, Size: int32(shape.pods - 1), Template: *template.DeepCopy()},
	}
}

// statusPublicationPods are the Component's pods at rest: one serving pod
// per single-pod Instance, a serving leader and workers per gang, bound
// over the nodes, every pod with its own address.
func statusPublicationPods(ir *v1beta1.InferenceReplica, shape statusPublicationShape) []*corev1.Pod {
	hash := query.RevisionFromName(irstatustest.Revision).Hash()
	pods := make([]*corev1.Pod, 0, shape.instances*shape.pods)
	add := func(index int, runner string, ordinal int) {
		pod := podForIR(ir, int32(index), runner, int32(ordinal), true, true)
		pod.Labels[query.LabelRevisionHash] = hash
		if shape.pods > 1 {
			pod.Labels[query.LabelPodGroup] = fmt.Sprintf("%s-%d", ir.Name, index)
		}
		n := len(pods)
		pod.Spec.NodeName = fmt.Sprintf("node-%d", n%publicationNodes)
		pod.Status.Phase = corev1.PodRunning
		pod.Status.PodIP = fmt.Sprintf("10.%d.%d.%d", n/65536%256, n/256%256, n%256)
		pods = append(pods, pod)
	}
	for i := 0; i < shape.instances; i++ {
		if shape.pods == 1 {
			add(i, string(v1beta1.RunnerNameDefault), 0)
			continue
		}
		add(i, string(v1beta1.RunnerNameLeader), 0)
		for ordinal := 0; ordinal < shape.pods-1; ordinal++ {
			add(i, string(v1beta1.RunnerNameWorker), ordinal)
		}
	}
	return pods
}

// statusPublicationPlan is the plan of the shape's Component, every
// Instance on its first attempt: one default runner per single-pod
// Instance, a leader and the workers per gang.
func statusPublicationPlan(component workloadtypes.ComponentType, shape statusPublicationShape) workloadtypes.ComponentPlan {
	if shape.pods == 1 {
		return publicationPlan(component, shape.instances)
	}
	plan := workloadtypes.ComponentPlan{
		Component: component,
		Replicas:  int32(shape.instances),
		Instances: make([]workloadtypes.InstancePlan, shape.instances),
	}
	for i := range plan.Instances {
		plan.Instances[i] = workloadtypes.InstancePlan{
			Index: int32(i), Incarnation: 1,
			Runners: []workloadtypes.RunnerPlan{
				{Name: workloadtypes.RunnerLeader, Size: 1},
				{Name: workloadtypes.RunnerWorker, Size: int32(shape.pods - 1)},
			},
		}
	}
	return plan
}

// decodePassCopy reads the replica from the store and decodes it, as a
// pass decodes its entry read from the cache; the copy is the one the
// status write is given.
func (f *statusPublicationFixture) decodePassCopy(tb testing.TB) {
	tb.Helper()
	passCopy := &v1beta1.InferenceReplica{}
	if _, err := irstatus.GetDecoded(context.Background(), irstatus.NewReader(f.apiserver, f.reconciler.InstanceStatusDecoder), f.key, passCopy); err != nil {
		tb.Fatalf("decode the pass's copy: %v", err)
	}
	f.passCopy = passCopy
	f.instancesAtStart = instanceIndices(passCopy.Status.InstanceStatuses)
}

// run is one status write of a pass over the fixture.
func (f *statusPublicationFixture) run(tb testing.TB) {
	tb.Helper()
	if _, err := f.reconciler.aggregateAndWriteStatus(context.Background(), f.passCopy, f.pods, f.list, f.plan, f.target,
		everyNodePresent{}, false, nil, f.instancesAtStart); err != nil {
		tb.Fatalf("aggregateAndWriteStatus: %v", err)
	}
}

// runCounting runs one status write and returns the replica's live reads
// and the writes it made.
func (f *statusPublicationFixture) runCounting(tb testing.TB) (replicaReads, writes int) {
	tb.Helper()
	readsBefore, writesBefore := f.replicaReads.Count(), f.writes.Count()
	f.run(tb)
	return f.replicaReads.Count() - readsBefore, f.writes.Count() - writesBefore
}

// settle runs status writes until one writes nothing: the first ones write
// what the fixture leaves to the controller.
func (f *statusPublicationFixture) settle(tb testing.TB) {
	tb.Helper()
	for i := 0; i < 8; i++ {
		if _, writes := f.runCounting(tb); writes == 0 {
			return
		}
	}
	tb.Fatalf("the replica did not come to rest in 8 status writes")
}

// BenchmarkStatusPublicationAtRest is the cost of the status write of one
// pass over a replica at rest at fleet size, with the replica's live reads
// it makes beside the time.
func BenchmarkStatusPublicationAtRest(b *testing.B) {
	for _, shape := range statusPublicationShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newStatusPublicationFixture(b, shape)
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

// TestStatusPublicationAtRestStaysWithinBudget holds the allocations and
// the bytes of the status write of one pass over a replica at rest, at
// 1,000 and 4,000 single-pod Instances and at 160 gangs of eight, to the
// recorded golden, with the live reads of the replica it makes. A status
// write over a replica at rest reads the replica live no times, since the
// pass's copy already carries the status it would write, and writes
// nothing.
func TestStatusPublicationAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "status_publication_allocations.json"), *updateStatusPublicationGoldens)
	defer golden.Close()
	for _, shape := range statusPublicationShapes() {
		newStatusPublicationFixture(t, shape).holdAtRest(t, golden, shape.name)
	}
}

// holdAtRest holds one status write over the resting fixture to the golden's
// rows named for the shape: the replica's live reads exactly and none, the
// allocations and the bytes within the tolerance, and no write.
func (f *statusPublicationFixture) holdAtRest(t *testing.T, golden *allocbudget.File, shape string) {
	t.Helper()
	replicaReads, writes := f.runCounting(t)
	golden.CheckCount(shape+"/replica-live-reads", int64(replicaReads))
	switch {
	case replicaReads != 0:
		t.Errorf("%s: the status write read the replica live %d times; a status the pass's copy already carries is answered without a read", shape, replicaReads)
	case writes != 0:
		t.Errorf("%s: the status write wrote %d times; a replica at rest is not written", shape, writes)
	}
	golden.Check(shape+"/allocations", func() { f.run(t) })
	golden.CheckBytes(shape+"/bytes", func() { f.run(t) })
}
