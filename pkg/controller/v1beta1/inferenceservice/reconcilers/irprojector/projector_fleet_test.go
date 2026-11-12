package irprojector

import (
	"context"
	"flag"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateProjectionGoldens = flag.Bool("update-projection", false, "re-record testdata/projection_allocations.json from this run")

// restShape is one replica at rest the two readers of a service pass run
// over: Instances of one pod each, every row Ready on the spec revision with
// its own readySince, stored in the column form the manager writes.
type restShape struct {
	name      string
	instances int
}

// restShapes are the budget's shapes: the largest reference replica at
// 4,000 Instances and a quarter of it.
func restShapes() []restShape {
	return []restShape{
		{name: "readySince/1000", instances: 1000},
		{name: "readySince/4000", instances: 4000},
	}
}

const (
	// restColumnBound is the row bound the operator configures for the
	// column form of the status.
	restColumnBound = 20000
	restNamespace   = "fleet"
	restService     = "fleet"
)

// cachedReplica answers a Get of the one replica from a copy held in
// memory, one deep copy per read, the way the informer cache answers the
// controller. Every other call reaches the fake apiserver it wraps, whose
// reads cost a JSON decode, as a live read does.
type cachedReplica struct {
	client.Client
	key   client.ObjectKey
	held  *v1beta1.InferenceReplica
	reads int
}

func (c *cachedReplica) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok || key != c.key || c.held == nil {
		return c.Client.Get(ctx, key, obj, opts...)
	}
	c.reads++
	c.held.DeepCopyInto(ir)
	return nil
}

// refresh takes the stored replica as the copy the cache holds, or none
// when the store has none.
func (c *cachedReplica) refresh(tb testing.TB) {
	tb.Helper()
	held := &v1beta1.InferenceReplica{}
	switch err := c.Client.Get(context.Background(), c.key, held); {
	case apierrors.IsNotFound(err):
		c.held = nil
	case err != nil:
		tb.Fatalf("refresh the cached replica: %v", err)
	default:
		c.held = held
	}
}

// restFixture is one service with one engine replica at rest behind the
// fake apiserver: the projector's input as the component reconciler builds
// it, the cache the two readers read through, the service as the cache
// holds it, and the counts of the live reads and the writes each reader
// makes.
type restFixture struct {
	params           Params
	cache            *cachedReplica
	apiserver        client.WithWatch
	service          *v1beta1.InferenceService
	replicaLiveReads *fakeclient.Calls
	serviceLiveReads *fakeclient.Calls
	writes           *fakeclient.Calls
}

// restServiceObject is the service at rest: one OMENative engine at the
// shape's count under the most common strategy, no autoscaler.
func restServiceObject(shape restShape) *v1beta1.InferenceService {
	tenPercent := intstr.FromString("10%")
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: restService, Namespace: restNamespace,
			UID: types.UID(restService + "-isvc-uid"), Generation: 1,
		},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(constants.OMENative),
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: ptr.To(shape.instances),
					MaxReplicas: shape.instances,
					Autoscaler:  &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
					Lifecycle: &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{
						Type:          v1beta1.UpdateStrategyRecreatePod,
						RollingUpdate: &v1beta1.RollingUpdate{MaxUnavailable: &tenPercent},
					}},
				},
			},
		},
	}
}

// restParams is the projector's input for the service's engine: the pod
// metadata and template the component reconciler renders, the resolved
// autoscaler, the cache as the client and the fake apiserver as the live
// reader.
func restParams(isvc *v1beta1.InferenceService, cache *cachedReplica, live client.Reader) Params {
	return Params{
		ISVC:         isvc,
		Component:    v1beta1.EngineComponent,
		ComponentExt: &isvc.Spec.Engine.ComponentExtensionSpec,
		ObjectMeta: metav1.ObjectMeta{
			Name: isvc.Name + "-engine", Namespace: isvc.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
		},
		PodSpec:            &corev1.PodSpec{Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "serving:v1"}}},
		ResolvedAutoscaler: isvc.Spec.Engine.Autoscaler,
		Client:             cache,
		Reader:             live,
	}
}

// restStatus is the status the replica controller publishes for the replica
// at rest: every row Ready on the spec revision with its own readySince,
// every counter at the count, in the column form.
func restStatus(tb testing.TB, shape restShape, generation int64) v1beta1.InferenceReplicaStatus {
	tb.Helper()
	status := irstatustest.LogicalStatus(irstatustest.ReadySinceRows(shape.instances))
	count := int32(shape.instances)
	status.ObservedGeneration = generation
	status.ReadyReplicas, status.ServingReplicas, status.AvailableReplicas = count, count, count
	status.UpdatedReplicas, status.UpdatedReadyReplicas = count, count
	status.UpdateRevision = irstatustest.Revision
	selection, err := irstatus.SelectCandidate(status, restColumnBound)
	if err != nil || selection.Selected != irstatus.EncodingColumnarV2 {
		tb.Fatalf("%s: SelectCandidate() = %+v, %v; the column form must be selected", shape.name, selection, err)
	}
	return *selection.SelectedCandidate().Status
}

// newRestFixture lets the projector create the replica, publishes its status
// at rest as the replica controller would, and runs the two readers until
// one pass writes nothing, so the measured pass is the steady one.
func newRestFixture(tb testing.TB, shape restShape) *restFixture {
	tb.Helper()
	isvc := restServiceObject(shape)
	replicaLiveReads := fakeclient.CountingVerb(fakeclient.Get).OnKind(&v1beta1.InferenceReplica{})
	serviceLiveReads := fakeclient.CountingVerb(fakeclient.Get).OnKind(&v1beta1.InferenceService{})
	writes := fakeclient.CountingWrites()
	apiserver := fakeclient.New(tb,
		fakeclient.WithObjects(isvc),
		fakeclient.WithStatusSubresource(&v1beta1.InferenceReplica{}, &v1beta1.InferenceService{}),
		replicaLiveReads, serviceLiveReads, writes)
	cache := &cachedReplica{Client: apiserver, key: client.ObjectKey{Namespace: isvc.Namespace, Name: InferenceReplicaName(isvc.Name, v1beta1.EngineComponent)}}
	f := &restFixture{
		params:           restParams(isvc, cache, apiserver),
		cache:            cache,
		apiserver:        apiserver,
		replicaLiveReads: replicaLiveReads,
		serviceLiveReads: serviceLiveReads,
		writes:           writes,
	}
	f.project(tb)
	stored := &v1beta1.InferenceReplica{}
	if err := apiserver.Get(context.Background(), cache.key, stored); err != nil {
		tb.Fatalf("%s: the first projection did not create the replica: %v", shape.name, err)
	}
	stored.Status = restStatus(tb, shape, stored.Generation)
	if err := apiserver.Status().Update(context.Background(), stored); err != nil {
		tb.Fatalf("%s: publish the replica status at rest: %v", shape.name, err)
	}
	f.refresh(tb)
	f.settle(tb)
	return f
}

// refresh takes the store's replica and service as the copies the cache
// holds, as the informer does after a write.
func (f *restFixture) refresh(tb testing.TB) {
	tb.Helper()
	f.cache.refresh(tb)
	service := &v1beta1.InferenceService{}
	if err := f.apiserver.Get(context.Background(), client.ObjectKeyFromObject(f.params.ISVC), service); err != nil {
		tb.Fatalf("refresh the cached service: %v", err)
	}
	f.service = service
}

// project is one projection of the engine, as the component reconciler
// makes it on every pass.
func (f *restFixture) project(tb testing.TB) {
	tb.Helper()
	if _, err := EnsureInferenceReplica(context.Background(), f.params); err != nil {
		tb.Fatalf("EnsureInferenceReplica: %v", err)
	}
}

// aggregate is one aggregation of the engine's status onto the pass's copy
// of the service, one deep copy of the service as the informer hands it.
func (f *restFixture) aggregate(tb testing.TB) {
	tb.Helper()
	passCopy := f.service.DeepCopy()
	if err := AggregateIRStatus(context.Background(), f.cache, f.apiserver, passCopy, engineOMENativeModes); err != nil {
		tb.Fatalf("AggregateIRStatus: %v", err)
	}
}

// projectCounting runs one projection and returns the replica's live reads,
// its cache reads and the writes it made.
func (f *restFixture) projectCounting(tb testing.TB) (liveReads, cacheReads, writes int) {
	tb.Helper()
	liveBefore, cacheBefore, writesBefore := f.replicaLiveReads.Count(), f.cache.reads, f.writes.Count()
	f.project(tb)
	return f.replicaLiveReads.Count() - liveBefore, f.cache.reads - cacheBefore, f.writes.Count() - writesBefore
}

// aggregateCounting runs one aggregation and returns the service's live
// reads, the replica's cache reads and the writes it made.
func (f *restFixture) aggregateCounting(tb testing.TB) (liveReads, cacheReads, writes int) {
	tb.Helper()
	liveBefore, cacheBefore, writesBefore := f.serviceLiveReads.Count(), f.cache.reads, f.writes.Count()
	f.aggregate(tb)
	return f.serviceLiveReads.Count() - liveBefore, f.cache.reads - cacheBefore, f.writes.Count() - writesBefore
}

// settle runs the two readers until one pass writes nothing: the first
// passes write what the fixture leaves to the controller.
func (f *restFixture) settle(tb testing.TB) {
	tb.Helper()
	for i := 0; i < 8; i++ {
		_, _, projected := f.projectCounting(tb)
		_, _, aggregated := f.aggregateCounting(tb)
		f.refresh(tb)
		if projected+aggregated == 0 {
			return
		}
	}
	tb.Fatalf("the service did not come to rest in 8 passes")
}

// BenchmarkProjectionAtRest is the cost of the projection of one service
// pass over a replica at rest at fleet size, with the replica's live reads
// it makes beside the time.
func BenchmarkProjectionAtRest(b *testing.B) {
	for _, shape := range restShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newRestFixture(b, shape)
			b.ReportAllocs()
			start := f.replicaLiveReads.Count()
			b.ResetTimer()
			for b.Loop() {
				f.project(b)
			}
			b.StopTimer()
			b.ReportMetric(float64(f.replicaLiveReads.Count()-start)/float64(b.N), "replica-live-reads/op")
		})
	}
}

// TestProjectionAtRestStaysWithinBudget holds the allocations of the
// projection of one service pass over a replica at rest, at 1,000 and 4,000
// Instances, to the recorded golden, with the live reads of the replica and
// the cache reads it makes. A projection over a replica at rest is decided
// on the cached copy, so it reads the replica live no times, and writes
// nothing.
func TestProjectionAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "projection_allocations.json"), *updateProjectionGoldens)
	defer golden.Close()
	for _, shape := range restShapes() {
		f := newRestFixture(t, shape)
		golden.Check(shape.name+"/allocations", func() { f.project(t) })
		liveReads, cacheReads, writes := f.projectCounting(t)
		golden.CheckCount(shape.name+"/replica-live-reads", int64(liveReads))
		golden.CheckCount(shape.name+"/replica-cache-reads", int64(cacheReads))
		if liveReads != 0 {
			t.Errorf("%s: the projection read the replica live %d times; a cached copy the projection leaves unchanged is the pass's answer", shape.name, liveReads)
		}
		if writes != 0 {
			t.Errorf("%s: the projection wrote %d times; a replica at rest is not written", shape.name, writes)
		}
	}
}
