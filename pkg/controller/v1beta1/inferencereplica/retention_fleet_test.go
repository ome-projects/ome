package inferencereplica

import (
	"context"
	"flag"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var updateRevisionSweepGoldens = flag.Bool("update-revision-sweep", false, "re-record testdata/revision_sweep_allocations.json from this run")

// revisionSweepShape is one replica the revision sweep runs over: its
// Instances, each a row the sweep reads two revision names off.
type revisionSweepShape struct {
	name string
	rows int
}

// revisionSweepShapes are the budget's shapes: the largest reference
// replica at 4,000 Instances, a quarter of it, and 160 gangs of eight. The
// sweep reads rows, not pods, so the gang shape is 160 rows.
func revisionSweepShapes() []revisionSweepShape {
	return []revisionSweepShape{
		{name: "readySince/1000", rows: 1000},
		{name: "readySince/4000", rows: 4000},
		{name: "gang/160x8", rows: 160},
	}
}

// revisionSweepFixture is one replica at rest whose history is one
// revision, the one every row runs, behind the fake apiserver: the pass's
// copy the sweep reads, the reconciler wired over one store for the cache
// and the live reader, and the counts of the history's lists and the
// writes the sweep makes.
type revisionSweepFixture struct {
	reconciler *Reconciler
	recorder   *record.FakeRecorder
	passCopy   *v1beta1.InferenceReplica
	revision   client.ObjectKey
	store      client.Client
	lists      *fakeclient.Calls
	writes     *fakeclient.Calls
}

// newRevisionSweepFixture builds the pass's copy as a pass holds it after
// its status write, every row Ready on the one revision with its own
// readySince, the history limit the chart sets on the spec, and the one
// revision stored. The cache and the live reader are one store, so the
// sweep's own list costs the same whichever reader answers it; the revision
// package's lock prices the two readers apart.
func newRevisionSweepFixture(tb testing.TB, shape revisionSweepShape) *revisionSweepFixture {
	tb.Helper()
	ir := baselineIR("llama-engine", "default", int32(shape.rows))
	ir.Spec.RevisionHistoryLimit = ptr.To(int32(testRevisionRetention))
	running := seedControllerRevision(ir, "a", 1)
	ir.Status = *irstatustest.LogicalStatus(irstatustest.ReadySinceRows(shape.rows))
	for i := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[i].RunningRevision = running.Name
	}
	ir.Status.CurrentRevision, ir.Status.UpdateRevision = running.Name, running.Name

	lists := fakeclient.CountingVerb(fakeclient.List).OnKind(&appsv1.ControllerRevisionList{})
	writes := fakeclient.CountingWrites()
	store := fakeclient.New(tb, fakeclient.WithObjects(running), lists, writes)
	recorder := record.NewFakeRecorder(16)
	return &revisionSweepFixture{
		reconciler: &Reconciler{
			Client:    store,
			APIReader: store,
			Log:       logf.Log.WithName("revision-sweep"),
			Recorder:  recorder,
		},
		recorder: recorder,
		passCopy: ir,
		revision: client.ObjectKeyFromObject(running),
		store:    store,
		lists:    lists,
		writes:   writes,
	}
}

// run is one revision sweep, as the replica pass makes it after its status
// write.
func (f *revisionSweepFixture) run() {
	f.reconciler.sweepRevisions(context.Background(), f.passCopy)
}

// runCounting runs one sweep and returns the history's lists and the writes
// it made.
func (f *revisionSweepFixture) runCounting() (lists, writes int) {
	listsBefore, writesBefore := f.lists.Count(), f.writes.Count()
	f.run()
	return f.lists.Count() - listsBefore, f.writes.Count() - writesBefore
}

// historyWhole reports whether the one revision is still stored.
func (f *revisionSweepFixture) historyWhole(tb testing.TB) bool {
	tb.Helper()
	err := f.store.Get(context.Background(), f.revision, &appsv1.ControllerRevision{})
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		tb.Fatalf("read the stored revision: %v", err)
	}
	return true
}

// BenchmarkRevisionSweepAtRest is the cost of the revision sweep at the end
// of one pass over a replica at rest at fleet size, with the history's
// lists it makes beside the time.
func BenchmarkRevisionSweepAtRest(b *testing.B) {
	for _, shape := range revisionSweepShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newRevisionSweepFixture(b, shape)
			b.ReportAllocs()
			start := f.lists.Count()
			b.ResetTimer()
			for b.Loop() {
				f.run()
			}
			b.StopTimer()
			b.ReportMetric(float64(f.lists.Count()-start)/float64(b.N), "history-lists/op")
		})
	}
}

// TestRevisionSweepAtRestStaysWithinBudget holds the allocations and the
// bytes of the revision sweep at the end of one pass over a replica at
// rest, at 1,000 and 4,000 single-pod Instances and at 160 gangs of eight,
// to the recorded golden. The sweep reads the two revision names every row
// carries and lists the history once to find nothing over the limit; a
// history at rest is neither written nor warned about.
func TestRevisionSweepAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "revision_sweep_allocations.json"), *updateRevisionSweepGoldens)
	defer golden.Close()
	for _, shape := range revisionSweepShapes() {
		f := newRevisionSweepFixture(t, shape)
		lists, writes := f.runCounting()
		switch {
		case lists != 1:
			t.Errorf("%s: the sweep listed the history %d times; want once", shape.name, lists)
		case writes != 0:
			t.Errorf("%s: the sweep wrote %d times; a history at rest is not written", shape.name, writes)
		case !f.historyWhole(t):
			t.Errorf("%s: the sweep deleted the revision every row runs", shape.name)
		}
		if events := fakeclient.RecordedEvents(f.recorder); len(events) != 0 {
			t.Errorf("%s: the sweep raised %v; a sweep at rest raises nothing", shape.name, events)
		}
		golden.Check(shape.name+"/allocations", f.run)
		golden.CheckBytes(shape.name+"/bytes", f.run)
	}
}
