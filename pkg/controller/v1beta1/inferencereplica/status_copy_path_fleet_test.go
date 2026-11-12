package inferencereplica

import (
	"flag"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateCopyPathGoldens = flag.Bool("update-copy-path", false, "re-record testdata/copy_path_allocations.json from this run")

// copyPathFixture is the status write's decision on the pass's copy: the
// resting replica's copy and the publication the write builds over its
// pods.
type copyPathFixture struct {
	*statusPublicationFixture
	publication statusPublication
}

// newCopyPathFixture builds the resting replica the status write is
// measured over and the publication input of one write.
func newCopyPathFixture(tb testing.TB, shape statusPublicationShape) copyPathFixture {
	tb.Helper()
	f := newStatusPublicationFixture(tb, shape)
	return copyPathFixture{statusPublicationFixture: f, publication: f.publicationInput(tb)}
}

// decide is the copy-path decision of one status write over the fixture's
// copy; at rest it settles, so the copy is left at rest for the next call.
func (f copyPathFixture) decide(tb testing.TB) bool {
	tb.Helper()
	_, settled, err := f.reconciler.publicationSettledOnCopy(f.passCopy, f.publication)
	if err != nil {
		tb.Fatalf("publicationSettledOnCopy: %v", err)
	}
	return settled
}

// BenchmarkPublicationCopyPathAtRest is the cost of the status write's
// decision on the pass's copy over a replica at rest, per shape.
func BenchmarkPublicationCopyPathAtRest(b *testing.B) {
	for _, shape := range heldDeadlineShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newCopyPathFixture(b, shape)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				f.decide(b)
			}
		})
	}
}

// TestPublicationCopyPathAtRestStaysWithinBudget holds the allocations and
// the bytes of the status write's decision on the pass's copy over a
// replica at rest, at 1,000 and 4,000 single-pod Instances and at 160 and
// 500 gangs of eight and 1,725 gangs of two, to the recorded golden. The
// decision settles at every shape: the replica is at rest, so the copy
// already carries the status the write would publish, and the live object
// is neither read nor written.
func TestPublicationCopyPathAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "copy_path_allocations.json"), *updateCopyPathGoldens)
	defer golden.Close()
	for _, shape := range heldDeadlineShapes() {
		f := newCopyPathFixture(t, shape)
		readsBefore, writesBefore := f.replicaReads.Count(), f.writes.Count()
		if !f.decide(t) {
			t.Errorf("%s: the decision on the copy did not settle; the replica is at rest", shape.name)
		}
		if reads := f.replicaReads.Count() - readsBefore; reads != 0 {
			t.Errorf("%s: the decision on the copy read the replica live %d times", shape.name, reads)
		}
		if writes := f.writes.Count() - writesBefore; writes != 0 {
			t.Errorf("%s: the decision on the copy wrote %d times", shape.name, writes)
		}
		golden.Check(shape.name+"/allocations", func() { f.decide(t) })
		golden.CheckBytes(shape.name+"/bytes", func() { f.decide(t) })
	}
}

// publishedOnACopy is the status a full publication writes over a deep copy
// of the replica, with the pod-derived fields cleared as the store holds
// them: what the live path would write.
func (f copyPathFixture) publishedOnACopy(tb testing.TB) *v1beta1.InferenceReplicaStatus {
	tb.Helper()
	full := f.passCopy.DeepCopy()
	if _, _, err := f.reconciler.publish(full, f.publication); err != nil {
		tb.Fatalf("publish: %v", err)
	}
	irstatus.ClearPodDerivedObservations(full.Status.InstanceStatuses)
	return &full.Status
}

// A replica at rest settles on the copy, and the copy then carries what a
// full publication would write: the same rows, counters, revision pair,
// conditions and hold, with the publication's pod counters mirrored onto
// the rows for the rest of the pass.
func TestPublicationSettledOnCopy_ARestingReplicaSettlesAndMirrorsThePublication(t *testing.T) {
	f := newCopyPathFixture(t, probeShape)
	want := f.publishedOnACopy(t)
	if !f.decide(t) {
		t.Fatalf("the decision on the copy did not settle; the replica is at rest")
	}
	for _, row := range f.passCopy.Status.InstanceStatuses {
		if row.ReadyPodCount != 1 || row.ServingPodCount != 1 || len(row.NodesOccupied) != 1 {
			t.Errorf("Instance %d on the copy: readyPods=%d servingPods=%d nodes=%v; the publication's pod counters are mirrored onto the copy", row.Index, row.ReadyPodCount, row.ServingPodCount, row.NodesOccupied)
		}
	}
	got := f.passCopy.Status.DeepCopy()
	irstatus.ClearPodDerivedObservations(got.InstanceStatuses)
	if !publishedStatusEqual(got, want) {
		t.Errorf("the settled copy differs from a full publication:\n got %+v\nwant %+v", got, want)
	}
}

// A row the publication would change calls for the live object: a pod out
// of serving, which the publication reads as a Restarting row, and each
// field the write sets on a row, moved on the copy alone, leave the
// decision unsettled and the copy as it was. A pod-derived field is the
// pass's view of its pods, never stored: it is cleared before the compare
// and decides nothing.
func TestPublicationSettledOnCopy_AChangedRowFieldCallsForTheLiveObject(t *testing.T) {
	row := func(f copyPathFixture) *v1beta1.OMENativeInstanceStatus {
		return &f.passCopy.Status.InstanceStatuses[2]
	}
	for _, tc := range []struct {
		name    string
		change  func(copyPathFixture)
		settles bool
	}{
		{name: "phase, a pod out of serving", change: func(f copyPathFixture) { setServing(f.pods[2], false) }},
		{name: "pod count", change: func(f copyPathFixture) { row(f).PodCount = 2 }},
		{name: "serving pods", change: func(f copyPathFixture) { row(f).ServingPodCount = 0 }},
		{name: "available pods", change: func(f copyPathFixture) { row(f).AvailablePodCount = 0 }},
		{name: "admitted", change: func(f copyPathFixture) { row(f).Admitted = false }},
		{name: "ready pods, cleared before the compare", change: func(f copyPathFixture) { row(f).ReadyPodCount = 1 }, settles: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCopyPathFixture(t, probeShape)
			tc.change(f)
			before := f.passCopy.Status.DeepCopy()
			irstatus.ClearPodDerivedObservations(before.InstanceStatuses)
			settled := f.decide(t)
			if settled != tc.settles {
				t.Fatalf("settled = %v, want %v", settled, tc.settles)
			}
			if tc.settles {
				return
			}
			if !publishedStatusEqual(&f.passCopy.Status, before) {
				t.Errorf("the copy changed although the decision left it to the live object:\n got %+v\nwant %+v", f.passCopy.Status, *before)
			}
		})
	}
}

// A field beside the rows the publication would change calls for the live
// object as well, and the copy keeps what it had: a counter, the revision
// pair and a condition.
func TestPublicationSettledOnCopy_AChangedFieldBesideTheRowsCallsForTheLiveObject(t *testing.T) {
	for name, change := range map[string]func(*v1beta1.InferenceReplicaStatus){
		"a counter":           func(s *v1beta1.InferenceReplicaStatus) { s.ReadyReplicas = 0 },
		"the update revision": func(s *v1beta1.InferenceReplicaStatus) { s.UpdateRevision = "another-revision" },
		"a condition": func(s *v1beta1.InferenceReplicaStatus) {
			for i := range s.Conditions {
				if s.Conditions[i].Type == InferenceReplicaConditionReady {
					s.Conditions[i].Status = metav1.ConditionFalse
					s.Conditions[i].Reason = "Probe"
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newCopyPathFixture(t, probeShape)
			change(&f.passCopy.Status)
			before := f.passCopy.Status.DeepCopy()
			irstatus.ClearPodDerivedObservations(before.InstanceStatuses)
			if f.decide(t) {
				t.Errorf("the decision settled although the publication changes %s", name)
			}
			if !publishedStatusEqual(&f.passCopy.Status, before) {
				t.Errorf("the copy changed although the decision left it to the live object:\n got %+v\nwant %+v", f.passCopy.Status, *before)
			}
		})
	}
}

// A replica with no revision target recomputes its conditions on the copy
// of the copy alone: a condition the publication would change calls for
// the live object and the copy keeps what it had. With a target, the
// rewrite of the current-revision condition copies the conditions on the
// way; without one, only the trial's own copy keeps the write off the copy.
func TestPublicationSettledOnCopy_AConditionChangeWithNoTargetLeavesTheCopyAsItWas(t *testing.T) {
	f := newCopyPathFixture(t, probeShape)
	f.publication.target = nil
	for i := range f.passCopy.Status.Conditions {
		if f.passCopy.Status.Conditions[i].Type == InferenceReplicaConditionReady {
			f.passCopy.Status.Conditions[i].Status = metav1.ConditionFalse
			f.passCopy.Status.Conditions[i].Reason = "Probe"
		}
	}
	before := f.passCopy.Status.DeepCopy()
	irstatus.ClearPodDerivedObservations(before.InstanceStatuses)
	if f.decide(t) {
		t.Errorf("the decision settled although the publication changes the Ready condition")
	}
	if !publishedStatusEqual(&f.passCopy.Status, before) {
		t.Errorf("the copy changed although the decision left it to the live object:\n got %+v\nwant %+v", f.passCopy.Status.Conditions, before.Conditions)
	}
}
