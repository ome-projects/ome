package inferencereplica

import (
	"context"
	"flag"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateHeldDeadlinesGoldens = flag.Bool("update-held-deadlines", false, "re-record testdata/held_deadlines_allocations.json from this run")

// heldDeadlineWindow is the readiness window the pass hands the deadline
// step at the end of a pass: the one the chart configures.
const heldDeadlineWindow = 30 * time.Minute

// heldDeadlineShapes are the shapes the deadline step is measured at: the
// shapes the status write is measured at, and the two largest reference gang services,
// 500 gangs of eight and 1,725 gangs of a leader and one
// worker.
func heldDeadlineShapes() []statusPublicationShape {
	return append(statusPublicationShapes(),
		statusPublicationShape{name: "gang/500x8", instances: 500, pods: 8},
		statusPublicationShape{name: "gang/1725x2", instances: 1725, pods: 2},
	)
}

// heldDeadlineFixture is the deadline step's input at the end of one pass
// over a replica at rest: the pass's copy of the replica, the Component's
// pods by Instance as the status write buckets them, no pause and the
// readiness window.
type heldDeadlineFixture struct {
	*statusPublicationFixture
	byIndex map[int32][]*corev1.Pod
}

// newHeldDeadlineFixture builds the resting replica the status write is
// measured over and buckets its pods by Instance.
func newHeldDeadlineFixture(tb testing.TB, shape statusPublicationShape) heldDeadlineFixture {
	tb.Helper()
	f := newStatusPublicationFixture(tb, shape)
	pods, _, err := query.SplitPodsOnMissingNodes(context.Background(), everyNodePresent{}, f.pods)
	if err != nil {
		tb.Fatalf("read the nodes under the pods: %v", err)
	}
	return heldDeadlineFixture{statusPublicationFixture: f, byIndex: query.BucketPodsByInstanceIdx(pods)}
}

// run is the deadline step of one pass over the fixture.
func (f heldDeadlineFixture) run(tb testing.TB) {
	tb.Helper()
	if err := f.reconciler.reconcileHeldDeadlines(context.Background(), f.passCopy, f.byIndex, false, heldDeadlineWindow); err != nil {
		tb.Fatalf("reconcileHeldDeadlines: %v", err)
	}
}

// runCounting runs the deadline step once and returns the replica's live
// reads and the writes it made.
func (f heldDeadlineFixture) runCounting(tb testing.TB) (replicaReads, writes int) {
	tb.Helper()
	readsBefore, writesBefore := f.replicaReads.Count(), f.writes.Count()
	f.run(tb)
	return f.replicaReads.Count() - readsBefore, f.writes.Count() - writesBefore
}

// BenchmarkHeldDeadlinesAtRest is the cost of the deadline step of one pass
// over a replica at rest, per shape.
func BenchmarkHeldDeadlinesAtRest(b *testing.B) {
	for _, shape := range heldDeadlineShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newHeldDeadlineFixture(b, shape)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				f.run(b)
			}
		})
	}
}

// TestHeldDeadlinesAtRestStayWithinBudget holds the allocations and the
// bytes of the deadline step of one pass over a replica at rest, at 1,000
// and 4,000 single-pod Instances and at 160 and 500 gangs of eight and
// 1,725 gangs of two, to the recorded golden, with the replica's live reads
// it makes. At rest no Instance has an operation open and no move record is
// open, so the step has no clock to stop or start: it reads the replica
// live no times and writes nothing.
func TestHeldDeadlinesAtRestStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "held_deadlines_allocations.json"), *updateHeldDeadlinesGoldens)
	defer golden.Close()
	for _, shape := range heldDeadlineShapes() {
		f := newHeldDeadlineFixture(t, shape)
		replicaReads, writes := f.runCounting(t)
		golden.CheckCount(shape.name+"/replica-live-reads", int64(replicaReads))
		if replicaReads != 0 {
			t.Errorf("%s: the deadline step read the replica live %d times; a replica with no clock to move is not read", shape.name, replicaReads)
		}
		if writes != 0 {
			t.Errorf("%s: the deadline step wrote %d times; a replica at rest is not written", shape.name, writes)
		}
		golden.Check(shape.name+"/allocations", func() { f.run(t) })
		golden.CheckBytes(shape.name+"/bytes", func() { f.run(t) })
	}
}

// The pause stops an open move record's clock and the first pass after the
// pause starts it again, whether or not any Instance has an operation open:
// a replica whose rows are all Ready still carries the record while the
// move waits to start.
func TestReconcileHeldDeadlines_PauseStopsAMoveRecordsClockWithNoOperationOpen(t *testing.T) {
	g := gomega.NewWithT(t)

	surgeIdx := int32(1)
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	future := metav1.NewTime(now.Add(20 * time.Minute))
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-a"},
	}
	entry := acceptedEntry("u-paused-move-at-rest")
	entry.Phase = v1beta1.MigrationPhaseSurgePending
	entry.SurgeInstance = &surgeIdx
	entry.Deadline = future
	ir.Status.Migrations = []v1beta1.MigrationStatus{entry}
	r, c := newReconciler(t, ir)
	r.Clock = clocktesting.NewFakeClock(now)
	key := types.NamespacedName{Name: ir.Name, Namespace: ir.Namespace}

	g.Expect(r.reconcileHeldDeadlines(context.Background(), ir, nil, true, 0)).To(gomega.Succeed())
	got := &v1beta1.InferenceReplica{}
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Status.Migrations).To(gomega.HaveLen(1))
	g.Expect(got.Status.Migrations[0].Deadline.IsZero()).To(gomega.BeTrue(),
		"a pause must stop the move record's clock when no Instance has an operation open")
	g.Expect(got.Status.InstanceStatuses[0].Operation).To(gomega.BeNil())

	g.Expect(r.reconcileHeldDeadlines(context.Background(), got, nil, false, heldDeadlineWindow)).To(gomega.Succeed())
	g.Expect(c.Get(context.Background(), key, got)).To(gomega.Succeed())
	g.Expect(got.Status.Migrations[0].Deadline.Time).To(gomega.BeTemporally("==", now.Add(heldDeadlineWindow)),
		"the first pass after the pause must start the record's clock again with a full window from the pass's clock")
}

// A replica at rest gives the deadline step nothing to do: no Instance has
// an operation open and no move record is open, so it reads the replica
// live no times and writes nothing, under a pause as at the end of a pass.
func TestReconcileHeldDeadlines_ARestingReplicaIsNeitherReadNorWritten(t *testing.T) {
	g := gomega.NewWithT(t)
	for _, paused := range []bool{false, true} {
		f := newHeldDeadlineFixture(t, statusPublicationShape{name: "readySince/8", instances: 8, pods: 1})
		readsBefore, writesBefore := f.replicaReads.Count(), f.writes.Count()
		g.Expect(f.reconciler.reconcileHeldDeadlines(context.Background(), f.passCopy, f.byIndex, paused, heldDeadlineWindow)).To(gomega.Succeed())
		g.Expect(f.replicaReads.Count()-readsBefore).To(gomega.BeZero(), "paused=%v: the replica was read live", paused)
		g.Expect(f.writes.Count()-writesBefore).To(gomega.BeZero(), "paused=%v: the replica was written", paused)
	}
}
