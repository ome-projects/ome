package inferencereplica

import (
	"context"
	"flag"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateConditionPromotionGoldens = flag.Bool("update-condition-promotion", false, "re-record testdata/condition_promotion_allocations.json from this run")

// conditionPromotionReadyTimeout is the readiness deadline the operator
// configures in the fixtures: the condition the pass writes says it is set.
const conditionPromotionReadyTimeout = 30 * time.Minute

// conditionPromotionFixture is a replica at rest with the three writes a
// pass makes beside its status write: the gang pass's scheduling
// condition, the readiness deadline condition and the current revision's
// promotion. The pass's copy carries both conditions and names the
// promoted revision, as a copy decoded at the start of a pass at rest does.
type conditionPromotionFixture struct {
	*statusPublicationFixture
	writeCondition func(context.Context, metav1.Condition) error
}

// newConditionPromotionFixture settles the two conditions on the stored
// replica, decodes the pass's copy from the store and builds the condition
// writer over that copy, as buildReconcileInput does once per pass.
func newConditionPromotionFixture(tb testing.TB, shape statusPublicationShape) *conditionPromotionFixture {
	tb.Helper()
	f := &conditionPromotionFixture{statusPublicationFixture: newStatusPublicationFixture(tb, shape)}
	f.reconciler.Clock = clocktesting.NewFakeClock(publicationClock)
	f.bindConditionWriter()
	f.writeGangSchedulingCondition(tb)
	f.reportReadyTimeout(tb)
	f.decodePassCopy(tb)
	f.bindConditionWriter()
	return f
}

// bindConditionWriter builds the condition writer over the pass's copy.
func (f *conditionPromotionFixture) bindConditionWriter() {
	f.writeCondition = buildWriteAggregateCondition(f.reconciler.statusWriter(), f.reconciler.liveReader(), f.passCopy)
}

// gangSchedulingAvailableCondition is the condition the gang pass writes on
// every pass of a Component whose gangs the cluster can schedule, or that
// has none.
func gangSchedulingAvailableCondition(generation int64) metav1.Condition {
	return metav1.Condition{
		Type:               string(workloadtypes.ConditionGangSchedulingUnavailable),
		Status:             metav1.ConditionFalse,
		Reason:             string(workloadtypes.ReasonGangSchedulingAvailable),
		Message:            "Gang scheduling is available or not required",
		LastTransitionTime: metav1.NewTime(publicationClock),
		ObservedGeneration: generation,
	}
}

// writeGangSchedulingCondition writes the gang pass's condition through the
// pass's condition writer.
func (f *conditionPromotionFixture) writeGangSchedulingCondition(tb testing.TB) {
	tb.Helper()
	if err := f.writeCondition(context.Background(), gangSchedulingAvailableCondition(f.passCopy.Generation)); err != nil {
		tb.Fatalf("write the gang scheduling condition: %v", err)
	}
}

// reportReadyTimeout writes the readiness deadline condition as the pass
// does, through the reconciler's own report.
func (f *conditionPromotionFixture) reportReadyTimeout(tb testing.TB) {
	tb.Helper()
	input := workloadtypes.ReconcileInput{WriteAggregateCondition: f.writeCondition}
	f.reconciler.reportInstanceReadyTimeout(context.Background(), f.reconciler.Log, f.passCopy, input, conditionPromotionReadyTimeout)
}

// promote runs the current revision's promotion over the pass's pods, as
// the end of the pass does before the status write.
func (f *conditionPromotionFixture) promote(tb testing.TB) {
	tb.Helper()
	if err := promoteCurrentRevisionOverPods(context.Background(), f.reconciler.statusWriter(), f.reconciler.liveReader(), f.passCopy, f.target.Name, podRevisionsOf(f.pods)); err != nil {
		tb.Fatalf("promote the current revision: %v", err)
	}
}

// counting runs op and returns the replica's live reads and the writes it
// made.
func (f *conditionPromotionFixture) counting(op func()) (replicaReads, writes int) {
	readsBefore, writesBefore := f.replicaReads.Count(), f.writes.Count()
	op()
	return f.replicaReads.Count() - readsBefore, f.writes.Count() - writesBefore
}

// writesAtRest are the three writes, each named as the golden names it.
func (f *conditionPromotionFixture) writesAtRest(tb testing.TB) map[string]func() {
	return map[string]func(){
		"gang-scheduling-condition": func() { f.writeGangSchedulingCondition(tb) },
		"ready-timeout-condition":   func() { f.reportReadyTimeout(tb) },
		"promotion":                 func() { f.promote(tb) },
	}
}

// conditionPromotionSites is the order the golden's rows and the
// benchmarks take.
var conditionPromotionSites = []string{"gang-scheduling-condition", "ready-timeout-condition", "promotion"}

// BenchmarkConditionWritesAndPromotionAtRest is the cost of each of the
// three writes at the end of one pass over a replica at rest at fleet
// size, with the replica's live reads it makes beside the time.
func BenchmarkConditionWritesAndPromotionAtRest(b *testing.B) {
	for _, shape := range statusPublicationShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newConditionPromotionFixture(b, shape)
			ops := f.writesAtRest(b)
			for _, site := range conditionPromotionSites {
				op := ops[site]
				b.Run(site, func(b *testing.B) {
					b.ReportAllocs()
					start := f.replicaReads.Count()
					b.ResetTimer()
					for b.Loop() {
						op()
					}
					b.StopTimer()
					b.ReportMetric(float64(f.replicaReads.Count()-start)/float64(b.N), "replica-reads/op")
				})
			}
		})
	}
}

// TestConditionWritesAndPromotionAtRestStayWithinBudget holds the three
// writes beside the status write of one pass over a replica at rest, at
// 1,000 and 4,000 single-pod Instances and at 160 gangs of eight, to the
// recorded golden: the replica's live reads each makes exactly, and its
// allocations and bytes within the tolerance. A condition the copy already
// carries as asked, and a current revision the rows and the pods already
// call for, write nothing.
func TestConditionWritesAndPromotionAtRestStayWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "condition_promotion_allocations.json"), *updateConditionPromotionGoldens)
	defer golden.Close()
	for _, shape := range statusPublicationShapes() {
		f := newConditionPromotionFixture(t, shape)
		ops := f.writesAtRest(t)
		for _, site := range conditionPromotionSites {
			op := ops[site]
			name := shape.name + "/" + site
			replicaReads, writes := f.counting(op)
			golden.CheckCount(name+"/replica-live-reads", int64(replicaReads))
			if writes != 0 {
				t.Errorf("%s: wrote %d times; a replica at rest is not written", name, writes)
			}
			golden.Check(name+"/allocations", op)
			golden.CheckBytes(name+"/bytes", op)
		}
		if f.passCopy.Status.CurrentRevision != f.target.Name {
			t.Errorf("%s: the copy names current revision %q after the promotion, want the target %q", shape.name, f.passCopy.Status.CurrentRevision, f.target.Name)
		}
	}
}
