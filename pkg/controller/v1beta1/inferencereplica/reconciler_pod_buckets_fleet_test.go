package inferencereplica

import (
	"context"
	"flag"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updatePodBucketsGoldens = flag.Bool("update-pod-buckets", false, "re-record testdata/pod_buckets_allocations.json from this run")

// podBucketsFixture is a replica at rest whose pass took its pods once, as
// the engine takes them, with the two readers of that list at the end of
// the pass: the current-revision promotion and the status write.
type podBucketsFixture struct {
	*publicationFixture
	// pods is the list the pass took, the slice every reader of the pass
	// is handed.
	pods []*corev1.Pod
}

// newPodBucketsFixture builds the replica, takes the pass's list and keeps
// the slice the list hands out.
func newPodBucketsFixture(tb testing.TB, shape publicationShape) *podBucketsFixture {
	tb.Helper()
	f := &podBucketsFixture{publicationFixture: newPublicationFixture(tb, shape)}
	f.takeThePassList(tb)
	pods, err := f.pass.cachedPods.Pods(context.Background(), componentPodLister(f.pass.reconciler.Client, f.pass.ir))
	if err != nil {
		tb.Fatalf("the pass's list: %v", err)
	}
	f.pods = pods
	return f
}

// promotion is the current-revision promotion over the pass's list, read
// through the list's buckets by Instance; at rest it finds the current
// revision already the spec target.
func (f *podBucketsFixture) promotion(tb testing.TB) {
	tb.Helper()
	buckets := f.pass.cachedPods.ByInstance(f.pods, query.BucketPodsByInstanceIdx)
	if ended, _, err := f.pass.promoteCurrentRevision(context.Background(), nil, buckets); ended || err != nil {
		tb.Fatalf("promotion: ended=%t err=%v", ended, err)
	}
}

// statusWrite is the status write over the pass's list; at rest it
// publishes what the replica already shows and writes nothing.
func (f *podBucketsFixture) statusWrite(tb testing.TB) {
	tb.Helper()
	p := f.pass
	if _, err := p.reconciler.aggregateAndWriteStatus(context.Background(), p.ir, f.pods, p.cachedPods, p.plan, p.specTarget, p.nodes, p.holdObserved, p.hold, p.instancesAtStart); err != nil {
		tb.Fatalf("status write: %v", err)
	}
}

// checkAtRest holds what the end of a pass at rest does over the pass's
// list: the promotion and the status write each list nothing and write
// nothing, and so does the publication that runs both.
func (f *podBucketsFixture) checkAtRest(t *testing.T, shape publicationShape) {
	t.Helper()
	listsBefore, writesBefore := f.podLists.Count(), f.writes.Count()
	f.promotion(t)
	f.statusWrite(t)
	lists, writes := f.publishCounting(t)
	if f.podLists.Count() != listsBefore || f.writes.Count() != writesBefore || lists != 0 || writes != 0 {
		t.Errorf("%s: the end of the pass listed %d times and wrote %d times; at rest it lists and writes nothing",
			shape.name, f.podLists.Count()-listsBefore, f.writes.Count()-writesBefore)
	}
}

// BenchmarkPodBucketsAtTheEndOfThePass is the cost of the promotion, the
// status write and the publication that runs both over the pass's list,
// per shape.
func BenchmarkPodBucketsAtTheEndOfThePass(b *testing.B) {
	for _, shape := range publicationShapes() {
		f := newPodBucketsFixture(b, shape)
		sites := []struct {
			name string
			run  func()
		}{
			{name: "promotion", run: func() { f.promotion(b) }},
			{name: "status-write", run: func() { f.statusWrite(b) }},
			{name: "pass-tail", run: func() { f.publish(b) }},
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

// TestPodBucketsAtTheEndOfThePassStayWithinBudget holds the allocations
// and the bytes of the two readers of the pass's pods at the end of a pass
// at rest, each measured alone, and of the publication that runs both, to
// the recorded golden: the current-revision promotion and the status
// write, each over the list the pass took, at 1,000 and 4,000 Instances.
// Beside the golden: each lists nothing and writes nothing.
func TestPodBucketsAtTheEndOfThePassStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "pod_buckets_allocations.json"), *updatePodBucketsGoldens)
	defer golden.Close()
	for _, shape := range publicationShapes() {
		f := newPodBucketsFixture(t, shape)
		f.checkAtRest(t, shape)
		golden.Check(shape.name+"/promotion/allocations", func() { f.promotion(t) })
		golden.CheckBytes(shape.name+"/promotion/bytes", func() { f.promotion(t) })
		golden.Check(shape.name+"/status-write/allocations", func() { f.statusWrite(t) })
		golden.CheckBytes(shape.name+"/status-write/bytes", func() { f.statusWrite(t) })
		golden.Check(shape.name+"/pass-tail/allocations", func() { f.publish(t) })
		golden.CheckBytes(shape.name+"/pass-tail/bytes", func() { f.publish(t) })
	}
}
