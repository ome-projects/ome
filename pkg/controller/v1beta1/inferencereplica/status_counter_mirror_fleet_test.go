package inferencereplica

import (
	"flag"
	"path/filepath"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateCounterMirrorGoldens = flag.Bool("update-counter-mirror", false, "re-record testdata/counter_mirror_allocations.json from this run")

// counterMirrorFixture is the mirror's input at the end of one status write
// over a replica at rest: the pass's copy of the replica and the publication
// the write computed over its pods.
type counterMirrorFixture struct {
	passCopy    *v1beta1.InferenceReplica
	publication []workloadtypes.InstanceStatus
}

// newCounterMirrorFixture builds the resting replica the status write is
// measured over and computes the publication of one write over it.
func newCounterMirrorFixture(tb testing.TB, shape statusPublicationShape) counterMirrorFixture {
	tb.Helper()
	f := newCopyPathFixture(tb, shape)
	publication, _, err := f.reconciler.computePublication(f.passCopy, f.publication)
	if err != nil {
		tb.Fatalf("computePublication: %v", err)
	}
	return counterMirrorFixture{passCopy: f.passCopy, publication: publication}
}

// run is the mirror of one status write over the fixture: the publication's
// phase and pod counters onto the rows of the pass's copy.
func (f counterMirrorFixture) run() {
	mirrorInstanceCounters(&f.passCopy.Status, f.publication)
}

// BenchmarkCounterMirrorAtRest is the cost of mirroring the publication's
// counters onto the pass's copy over a replica at rest, per shape.
func BenchmarkCounterMirrorAtRest(b *testing.B) {
	for _, shape := range heldDeadlineShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newCounterMirrorFixture(b, shape)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				f.run()
			}
		})
	}
}

// TestCounterMirrorAtRestStaysWithinBudget holds the allocations and the
// bytes of the mirror over a replica at rest, at 1,000 and 4,000 single-pod
// Instances and at 160 and 500 gangs of eight and 1,725 gangs of two, to the
// recorded golden. At rest the publication lists the copy's Instances in the
// copy's order, and every row carries its entry's counters once mirrored.
func TestCounterMirrorAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "counter_mirror_allocations.json"), *updateCounterMirrorGoldens)
	defer golden.Close()
	for _, shape := range heldDeadlineShapes() {
		f := newCounterMirrorFixture(t, shape)
		rows := f.passCopy.Status.InstanceStatuses
		if len(rows) != len(f.publication) {
			t.Fatalf("%s: %d rows and %d publication entries; a publication built from the rows has one entry per row", shape.name, len(rows), len(f.publication))
		}
		for i := range rows {
			if rows[i].Index != f.publication[i].Index {
				t.Fatalf("%s: row %d is Instance %d and its publication entry Instance %d; a publication built from the rows keeps their order", shape.name, i, rows[i].Index, f.publication[i].Index)
			}
		}
		f.run()
		for i := range rows {
			if rows[i].PodCount != f.publication[i].PodCount || rows[i].ServingPodCount != f.publication[i].ServingPodCount || len(rows[i].NodesOccupied) != len(f.publication[i].NodesOccupied) {
				t.Fatalf("%s: Instance %d after the mirror: pods=%d serving=%d nodes=%v; the entry says pods=%d serving=%d nodes=%v", shape.name, rows[i].Index,
					rows[i].PodCount, rows[i].ServingPodCount, rows[i].NodesOccupied, f.publication[i].PodCount, f.publication[i].ServingPodCount, f.publication[i].NodesOccupied)
			}
		}
		golden.Check(shape.name+"/allocations", f.run)
		golden.CheckBytes(shape.name+"/bytes", f.run)
	}
}
