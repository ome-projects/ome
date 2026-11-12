package revision

import (
	"context"
	"flag"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateConfirmGoldens = flag.Bool("update-revision-confirm", false, "re-record testdata/revision_confirm_allocations.json from this run")

// confirmShape is one spec revision a replica pass confirms on every pass:
// the templates it records.
type confirmShape struct {
	name     string
	template Template
}

// confirmShapes are the budget's shapes: a single-pod Component and a gang
// of a leader and seven workers.
func confirmShapes() []confirmShape {
	return []confirmShape{
		{name: "single", template: Template{PodSpec: basicPodSpecForRevision()}},
		{name: "gang/1+7", template: Template{
			PodSpec: basicPodSpecForRevision(), WorkerPodSpec: basicPodSpecForRevision(),
			WorkerSize: 7, TopologyKey: "topology.kubernetes.io/zone",
		}},
	}
}

// cachedRevision answers a Get of the one revision from a copy held in
// memory, one deep copy per read, the way the informer cache answers the
// controller. Every other call reaches the fake apiserver it wraps, whose
// reads cost a JSON decode, as a live read does.
type cachedRevision struct {
	client.Client
	key   client.ObjectKey
	held  *appsv1.ControllerRevision
	reads int
}

func (c *cachedRevision) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	cr, ok := obj.(*appsv1.ControllerRevision)
	if !ok || key != c.key || c.held == nil {
		return c.Client.Get(ctx, key, obj, opts...)
	}
	c.reads++
	c.held.DeepCopyInto(cr)
	return nil
}

// refresh takes the stored revision as the copy the cache holds, or none
// when the store has none.
func (c *cachedRevision) refresh(tb testing.TB) {
	tb.Helper()
	held := &appsv1.ControllerRevision{}
	switch err := c.Client.Get(context.Background(), c.key, held); {
	case apierrors.IsNotFound(err):
		c.held = nil
	case err != nil:
		tb.Fatalf("refresh the cached revision: %v", err)
	default:
		c.held = held
	}
}

// confirmFixture is one projected replica whose spec revision is minted
// and at rest behind the fake apiserver: the cache the confirmation reads
// through, the fake apiserver as the live reader, the inputs of the
// confirmation, and the counts of the live reads and the writes it makes.
type confirmFixture struct {
	replica   *v1beta1.InferenceReplica
	scope     *metav1.OwnerReference
	key       Key
	hash      string
	raw       []byte
	cache     *cachedRevision
	apiserver client.WithWatch
	liveReads *fakeclient.Calls
	writes    *fakeclient.Calls
}

// newConfirmFixture mints the revision through the production path, then
// confirms it until one call writes nothing, so the measured call is the
// steady one.
func newConfirmFixture(tb testing.TB, shape confirmShape) *confirmFixture {
	tb.Helper()
	service := minimalISVC("svc", "prod", 1)
	replica := projectedReplica("svc-engine", "replica-1", service)
	scope := ScopeOwner(replica, testISVCGVK.GroupKind())
	key := testKey(service, workload.ComponentEngine)
	hash, raw, err := HashTemplate(shape.template, nil, service.UID)
	if err != nil {
		tb.Fatalf("%s: HashTemplate: %v", shape.name, err)
	}
	liveReads := fakeclient.CountingVerb(fakeclient.Get).OnKind(&appsv1.ControllerRevision{})
	writes := fakeclient.CountingWrites()
	apiserver := fakeclient.New(tb, fakeclient.WithObjects(service, replica), liveReads, writes)
	f := &confirmFixture{
		replica: replica, scope: scope, key: key, hash: hash, raw: raw,
		cache:     &cachedRevision{Client: apiserver, key: client.ObjectKey{Namespace: key.Namespace, Name: Name(key, hash)}},
		apiserver: apiserver,
		liveReads: liveReads,
		writes:    writes,
	}
	f.settle(tb)
	return f
}

// confirm is one confirmation of the spec revision, as the replica pass
// makes it on every pass.
func (f *confirmFixture) confirm(tb testing.TB) {
	tb.Helper()
	cr, collision, err := EnsureControllerRevisionFromHash(context.Background(), f.cache, f.apiserver, f.replica, testIRGVK, f.key, f.hash, f.raw, f.scope)
	if err != nil || collision || cr == nil || cr.Name != f.cache.key.Name {
		tb.Fatalf("EnsureControllerRevisionFromHash: collision=%v err=%v cr=%v", collision, err, cr)
	}
}

// confirmCounting runs one confirmation and returns the revision's live
// reads, its cache reads and the writes it made.
func (f *confirmFixture) confirmCounting(tb testing.TB) (liveReads, cacheReads, writes int) {
	tb.Helper()
	liveBefore, cacheBefore, writesBefore := f.liveReads.Count(), f.cache.reads, f.writes.Count()
	f.confirm(tb)
	return f.liveReads.Count() - liveBefore, f.cache.reads - cacheBefore, f.writes.Count() - writesBefore
}

// settle confirms until one call writes nothing: the first call mints the
// revision.
func (f *confirmFixture) settle(tb testing.TB) {
	tb.Helper()
	for i := 0; i < 4; i++ {
		_, _, writes := f.confirmCounting(tb)
		f.cache.refresh(tb)
		if writes == 0 {
			return
		}
	}
	tb.Fatalf("the revision did not come to rest in 4 confirmations")
}

// BenchmarkRevisionConfirmAtRest is the cost of confirming the spec
// revision of a replica at rest, as every replica pass does, with the
// revision's live reads it makes beside the time.
func BenchmarkRevisionConfirmAtRest(b *testing.B) {
	for _, shape := range confirmShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newConfirmFixture(b, shape)
			b.ReportAllocs()
			start := f.liveReads.Count()
			b.ResetTimer()
			for b.Loop() {
				f.confirm(b)
			}
			b.StopTimer()
			b.ReportMetric(float64(f.liveReads.Count()-start)/float64(b.N), "revision-live-reads/op")
		})
	}
}

// TestRevisionConfirmAtRestStaysWithinBudget holds the allocations of
// confirming the spec revision of a replica at rest, for a single-pod
// Component and a gang of eight, to the recorded golden, with the live
// reads of the revision and the cache reads it makes. A revision at rest is
// confirmed from the cached copy, so it is read live no times, and nothing
// is written.
func TestRevisionConfirmAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "revision_confirm_allocations.json"), *updateConfirmGoldens)
	defer golden.Close()
	for _, shape := range confirmShapes() {
		f := newConfirmFixture(t, shape)
		golden.Check(shape.name+"/allocations", func() { f.confirm(t) })
		liveReads, cacheReads, writes := f.confirmCounting(t)
		golden.CheckCount(shape.name+"/revision-live-reads", int64(liveReads))
		golden.CheckCount(shape.name+"/revision-cache-reads", int64(cacheReads))
		if liveReads != 0 {
			t.Errorf("%s: the confirmation read the revision live %d times; a cached copy with nothing to write is the pass's answer", shape.name, liveReads)
		}
		if writes != 0 {
			t.Errorf("%s: the confirmation wrote %d times; a revision at rest is not written", shape.name, writes)
		}
	}
}
