package irprojector

import (
	"flag"
	"path/filepath"
	"testing"

	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateAggregationGoldens = flag.Bool("update-aggregation", false, "re-record testdata/aggregation_allocations.json from this run")

// BenchmarkAggregationAtRest is the cost of the status aggregation of one
// service pass over a replica at rest at fleet size, with the service's live
// reads it makes beside the time.
func BenchmarkAggregationAtRest(b *testing.B) {
	for _, shape := range restShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newRestFixture(b, shape)
			b.ReportAllocs()
			start := f.serviceLiveReads.Count()
			b.ResetTimer()
			for b.Loop() {
				f.aggregate(b)
			}
			b.StopTimer()
			b.ReportMetric(float64(f.serviceLiveReads.Count()-start)/float64(b.N), "service-live-reads/op")
		})
	}
}

// TestAggregationAtRestStaysWithinBudget holds the allocations of the status
// aggregation of one service pass over a replica at rest, at 1,000 and 4,000
// Instances, to the recorded golden, with the live reads of the service and
// the cache reads of the replica it makes. An aggregation over a replica at
// rest writes nothing.
func TestAggregationAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "aggregation_allocations.json"), *updateAggregationGoldens)
	defer golden.Close()
	for _, shape := range restShapes() {
		f := newRestFixture(t, shape)
		golden.Check(shape.name+"/allocations", func() { f.aggregate(t) })
		liveReads, cacheReads, writes := f.aggregateCounting(t)
		golden.CheckCount(shape.name+"/service-live-reads", int64(liveReads))
		golden.CheckCount(shape.name+"/replica-cache-reads", int64(cacheReads))
		if writes != 0 {
			t.Errorf("%s: the aggregation wrote %d times; a service at rest is not written", shape.name, writes)
		}
	}
}
