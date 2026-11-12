package inferencereplica

import (
	"context"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// replaceOwner deletes the stored replica and creates it again under the
// same name with another UID, as a delete and a create between two passes
// leave it.
func (f *conditionPromotionFixture) replaceOwner(tb testing.TB) {
	tb.Helper()
	replaced := f.stored(tb)
	if err := f.apiserver.Delete(context.Background(), replaced); err != nil {
		tb.Fatalf("delete the replica: %v", err)
	}
	replaced.ResourceVersion = ""
	replaced.UID = types.UID("another-uid")
	if err := f.apiserver.Create(context.Background(), replaced); err != nil {
		tb.Fatalf("create the replica again: %v", err)
	}
}

// conditionOnCopy reads one condition of the pass's copy.
func (f *conditionPromotionFixture) conditionOnCopy(t *testing.T, condType string) metav1.Condition {
	t.Helper()
	cond := apimeta.FindStatusCondition(f.passCopy.Status.Conditions, condType)
	if cond == nil {
		t.Fatalf("the pass's copy carries no %s condition", condType)
	}
	return *cond
}

func TestWriteAggregateCondition_AConditionRecordedOnTheCopyIsAnsweredWithoutARead(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	for i := 0; i < 3; i++ {
		reads, writes := f.counting(func() { f.writeGangSchedulingCondition(t) })
		expectCounts(t, "the gang scheduling condition recorded on the copy", reads, 0, writes, 0)
		reads, writes = f.counting(func() { f.reportReadyTimeout(t) })
		expectCounts(t, "the readiness deadline condition recorded on the copy", reads, 0, writes, 0)
	}
}

func TestWriteAggregateCondition_AWrittenConditionIsMirroredOntoTheCopy(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	cond := metav1.Condition{Type: InferenceReplicaConditionCurrentRevisionWithdrawn, Status: metav1.ConditionTrue, Reason: "ProbeReason", Message: "a condition no pass has written", ObservedGeneration: f.passCopy.Generation}
	write := func() {
		if err := f.writeCondition(context.Background(), cond); err != nil {
			t.Fatalf("write the condition: %v", err)
		}
	}
	reads, writes := f.counting(write)
	expectCounts(t, "a condition the copy does not carry", reads, 1, writes, 1)
	if stored, onCopy := f.storedCondition(t, cond.Type), f.conditionOnCopy(t, cond.Type); onCopy != stored {
		t.Errorf("the copy carries %+v after the write, the store %+v", onCopy, stored)
	}
	reads, writes = f.counting(write)
	expectCounts(t, "the same condition again", reads, 0, writes, 0)
}

func TestWriteAggregateCondition_ACopyBehindTheStoresOwnConditionTakesTheStoredOne(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	gang := string(workloadtypes.ConditionGangSchedulingUnavailable)
	// The store's condition transitioned an hour before this pass's clock.
	f.storeStatus(t, func(status *v1beta1.InferenceReplicaStatus) {
		apimeta.FindStatusCondition(status.Conditions, gang).LastTransitionTime = metav1.NewTime(publicationClock.Add(-time.Hour))
	})
	recorded := f.storedCondition(t, gang)
	apimeta.RemoveStatusCondition(&f.passCopy.Status.Conditions, gang)
	reads, writes := f.counting(func() { f.writeGangSchedulingCondition(t) })
	expectCounts(t, "a copy behind the store's own condition", reads, 1, writes, 0)
	if onCopy := f.conditionOnCopy(t, gang); onCopy != recorded {
		t.Errorf("the copy carries %+v after the read, the store %+v", onCopy, recorded)
	}
}

func TestWriteAggregateCondition_AStoreChangedBehindARestingCopyIsWrittenByThePassThatShowsIt(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	gang := string(workloadtypes.ConditionGangSchedulingUnavailable)
	f.storeStatus(t, func(status *v1beta1.InferenceReplicaStatus) {
		apimeta.SetStatusCondition(&status.Conditions, metav1.Condition{Type: gang, Status: metav1.ConditionTrue, Reason: "ProbeReason", Message: "changed behind the copy", ObservedGeneration: f.passCopy.Generation})
	})
	reads, writes := f.counting(func() { f.writeGangSchedulingCondition(t) })
	expectCounts(t, "the pass whose copy records the condition as asked", reads, 0, writes, 0)
	f.decodePassCopy(t)
	f.bindConditionWriter()
	reads, writes = f.counting(func() { f.writeGangSchedulingCondition(t) })
	expectCounts(t, "the pass whose copy shows the change", reads, 1, writes, 1)
	if got := f.storedCondition(t, gang); got.Status != metav1.ConditionFalse || got.Reason != string(workloadtypes.ReasonGangSchedulingAvailable) {
		t.Errorf("the stored condition after the pass: %+v, want False and %s", got, workloadtypes.ReasonGangSchedulingAvailable)
	}
}

func TestWriteAggregateCondition_ARestingCopyOverAReplacedOwnerReadsNothing(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	f.replaceOwner(t)
	reads, writes := f.counting(func() { f.writeGangSchedulingCondition(t) })
	expectCounts(t, "a condition recorded on the copy over a replaced owner", reads, 0, writes, 0)
	cond := gangSchedulingAvailableCondition(f.passCopy.Generation)
	cond.Reason = "ProbeReason"
	reads, writes = f.counting(func() {
		if err := f.writeCondition(context.Background(), cond); err == nil {
			t.Error("a condition the copy does not record is written nowhere on a replaced owner, and the writer says so")
		}
	})
	expectCounts(t, "a changed condition over a replaced owner", reads, 1, writes, 0)
}

func TestPromoteCurrentRevision_AReplicaAtRestIsAnsweredWithoutARead(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	for i := 0; i < 3; i++ {
		reads, writes := f.counting(func() { f.promote(t) })
		expectCounts(t, "the promotion at rest", reads, 0, writes, 0)
	}
}

func TestPromoteCurrentRevision_AStoreChangedBehindARestingCopyIsWrittenByThePassThatShowsIt(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	f.storeStatus(t, func(status *v1beta1.InferenceReplicaStatus) { status.CurrentRevision = irstatustest.OtherRevision })
	reads, writes := f.counting(func() { f.promote(t) })
	expectCounts(t, "the pass whose copy names the promoted revision", reads, 0, writes, 0)
	if got := f.stored(t).Status.CurrentRevision; got != irstatustest.OtherRevision {
		t.Errorf("the store names %q after a pass that read nothing, want it left at %q", got, irstatustest.OtherRevision)
	}
	f.decodePassCopy(t)
	reads, writes = f.counting(func() { f.promote(t) })
	expectCounts(t, "the pass whose copy shows the change", reads, 1, writes, 1)
	if got := f.stored(t).Status.CurrentRevision; got != f.target.Name {
		t.Errorf("the stored current revision is %q after the pass, want %q", got, f.target.Name)
	}
	if f.passCopy.Status.CurrentRevision != f.target.Name {
		t.Errorf("the copy names %q after the write, want %q", f.passCopy.Status.CurrentRevision, f.target.Name)
	}
}

func TestPromoteCurrentRevision_ARestingCopyOverAReplacedOwnerReadsNothing(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	f.replaceOwner(t)
	reads, writes := f.counting(func() { f.promote(t) })
	expectCounts(t, "a promotion with nothing to write over a replaced owner", reads, 0, writes, 0)
}
