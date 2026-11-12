package revision

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateRetentionGoldens = flag.Bool("update-revision-retention", false, "re-record testdata/revision_retention_allocations.json from this run")

// retentionShape is one replica's revision history at rest: the revisions
// the replica has run, the newest the one every row runs, under the limit
// the sweep keeps the others to.
type retentionShape struct {
	name   string
	minted int
	limit  int
}

// retentionShapes are the budget's shapes: a replica that has run one
// revision, and one that has run more than the limit the chart sets, ten,
// so its history is what the sweep keeps.
func retentionShapes() []retentionShape {
	return []retentionShape{
		{name: "one-revision", minted: 1, limit: 10},
		{name: "ten-kept", minted: 14, limit: 10},
	}
}

// kept is the revisions no row runs that the history holds at rest: every
// older one up to the limit.
func (s retentionShape) kept() int {
	return min(s.minted-1, s.limit)
}

// cachedRevisions answers a List of the history's revisions from copies held
// in memory, one deep copy per revision as the informer cache answers the
// controller, and counts the lists. Every other call reaches the fake
// apiserver it wraps, whose reads cost a JSON decode, as a live read does.
type cachedRevisions struct {
	client.Client
	held  []appsv1.ControllerRevision
	lists int
}

func (c *cachedRevisions) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	revisions, ok := list.(*appsv1.ControllerRevisionList)
	if !ok {
		return c.Client.List(ctx, list, opts...)
	}
	c.lists++
	revisions.Items = make([]appsv1.ControllerRevision, len(c.held))
	for i := range c.held {
		c.held[i].DeepCopyInto(&revisions.Items[i])
	}
	return nil
}

// refresh takes the stored revisions as the copies the cache holds.
func (c *cachedRevisions) refresh(tb testing.TB) {
	tb.Helper()
	list := &appsv1.ControllerRevisionList{}
	if err := c.Client.List(context.Background(), list); err != nil {
		tb.Fatalf("refresh the cached revisions: %v", err)
	}
	c.held = list.Items
}

// retentionFixture is one replica's revision history at rest behind the
// fake apiserver: the cache the sweep reads through, the fake apiserver as
// the live reader, the sweep's inputs, and the counts of the live lists and
// the writes it makes.
type retentionFixture struct {
	key       Key
	limit     int
	live      string
	cache     *cachedRevisions
	apiserver client.WithWatch
	liveLists *fakeclient.Calls
	writes    *fakeclient.Calls
}

// newRetentionFixture mints the history through the production path, the
// newest revision the one every row runs; then sweeps until one sweep
// writes nothing, so the measured sweep is the steady one over a history
// the sweep itself trimmed to the limit.
func newRetentionFixture(tb testing.TB, shape retentionShape) *retentionFixture {
	tb.Helper()
	service := minimalISVC("svc", "prod", 1)
	replica := projectedReplica("svc-engine", "replica-1", service)
	scope := ScopeOwner(replica, testISVCGVK.GroupKind())
	key := testKey(service, workload.ComponentEngine)
	liveLists := fakeclient.CountingVerb(fakeclient.List).OnKind(&appsv1.ControllerRevisionList{})
	writes := fakeclient.CountingWrites()
	apiserver := fakeclient.New(tb, fakeclient.WithObjects(service, replica), liveLists, writes)
	f := &retentionFixture{
		key: key, limit: shape.limit,
		cache:     &cachedRevisions{Client: apiserver},
		apiserver: apiserver,
		liveLists: liveLists,
		writes:    writes,
	}
	for i := 0; i < shape.minted; i++ {
		template := Template{PodSpec: &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: fmt.Sprintf("serving:v%d", i+1)}}}}
		hash, raw, err := HashTemplate(template, nil, service.UID)
		if err != nil {
			tb.Fatalf("%s: HashTemplate: %v", shape.name, err)
		}
		if _, collision, err := EnsureControllerRevisionFromHash(context.Background(), apiserver, apiserver, replica, testIRGVK, key, hash, raw, scope); err != nil || collision {
			tb.Fatalf("%s: mint revision %d: collision=%v err=%v", shape.name, i+1, collision, err)
		}
		f.live = Name(key, hash)
	}
	f.cache.refresh(tb)
	f.settle(tb)
	return f
}

// sweep is one retention sweep of the history, as the replica pass makes it
// after every status write.
func (f *retentionFixture) sweep(tb testing.TB) {
	tb.Helper()
	if err := RetainControllerRevisions(context.Background(), f.cache, f.apiserver, f.key, f.limit, f.live); err != nil {
		tb.Fatalf("RetainControllerRevisions: %v", err)
	}
}

// sweepCounting runs one sweep and returns the live lists, the cache lists
// and the writes it made.
func (f *retentionFixture) sweepCounting(tb testing.TB) (liveLists, cacheLists, writes int) {
	tb.Helper()
	liveBefore, cacheBefore, writesBefore := f.liveLists.Count(), f.cache.lists, f.writes.Count()
	f.sweep(tb)
	return f.liveLists.Count() - liveBefore, f.cache.lists - cacheBefore, f.writes.Count() - writesBefore
}

// settle sweeps until one sweep writes nothing: the first one trims the
// history to the limit.
func (f *retentionFixture) settle(tb testing.TB) {
	tb.Helper()
	for i := 0; i < 4; i++ {
		_, _, writes := f.sweepCounting(tb)
		f.cache.refresh(tb)
		if writes == 0 {
			return
		}
	}
	tb.Fatalf("the history did not come to rest in 4 sweeps")
}

// revisions reads the history the live store holds.
func (f *retentionFixture) revisions(tb testing.TB) []appsv1.ControllerRevision {
	tb.Helper()
	list := &appsv1.ControllerRevisionList{}
	if err := f.apiserver.List(context.Background(), list); err != nil {
		tb.Fatalf("list the stored revisions: %v", err)
	}
	return list.Items
}

// BenchmarkRevisionRetentionAtRest is the cost of the retention sweep over a
// replica's history at rest, as every replica pass makes it, with the live
// lists it makes beside the time.
func BenchmarkRevisionRetentionAtRest(b *testing.B) {
	for _, shape := range retentionShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newRetentionFixture(b, shape)
			b.ReportAllocs()
			start := f.liveLists.Count()
			b.ResetTimer()
			for b.Loop() {
				f.sweep(b)
			}
			b.StopTimer()
			b.ReportMetric(float64(f.liveLists.Count()-start)/float64(b.N), "revision-live-lists/op")
		})
	}
}

// TestRevisionRetentionAtRestStaysWithinBudget holds the allocations of the
// retention sweep over a replica's history at rest, for a history of one
// revision and one kept at the limit of ten, to the recorded golden, with
// the live lists and the cache lists it makes. A history at rest keeps the
// revision every row runs and the kept ones, and the sweep writes nothing.
func TestRevisionRetentionAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "revision_retention_allocations.json"), *updateRetentionGoldens)
	defer golden.Close()
	for _, shape := range retentionShapes() {
		f := newRetentionFixture(t, shape)
		golden.Check(shape.name+"/allocations", func() { f.sweep(t) })
		liveLists, cacheLists, writes := f.sweepCounting(t)
		golden.CheckCount(shape.name+"/revision-live-lists", int64(liveLists))
		golden.CheckCount(shape.name+"/revision-cache-lists", int64(cacheLists))
		if writes != 0 {
			t.Errorf("%s: the sweep wrote %d times; a history at rest is not written", shape.name, writes)
		}
		if got, want := len(f.revisions(t)), shape.kept()+1; got != want {
			t.Errorf("%s: the history holds %d revisions, want %d: the one every row runs and the %d kept", shape.name, got, want, shape.kept())
		}
	}
}
