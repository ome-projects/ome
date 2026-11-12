package inferencereplica

import (
	"context"
	"strconv"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// storedCondition reads one condition of the stored replica.
func (f *conditionPromotionFixture) storedCondition(t *testing.T, condType string) metav1.Condition {
	t.Helper()
	cond := apimeta.FindStatusCondition(f.stored(t).Status.Conditions, condType)
	if cond == nil {
		t.Fatalf("the stored replica carries no %s condition", condType)
	}
	return *cond
}

// expectWrites fails unless op made the writes given, and reports the live
// reads beside them.
func (f *conditionPromotionFixture) expectWrites(tb testing.TB, step string, op func(), want int) {
	tb.Helper()
	reads, writes := f.counting(op)
	if writes != want {
		tb.Errorf("%s: live reads=%d writes=%d, want %d writes", step, reads, writes, want)
	}
}

func TestWriteAggregateCondition_AConditionRecordedAsAskedIsNotWrittenAgain(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	gang := string(workloadtypes.ConditionGangSchedulingUnavailable)
	timeout := string(workloadtypes.ConditionInstanceReadyTimeoutUnconfigured)
	before := map[string]metav1.Condition{gang: f.storedCondition(t, gang), timeout: f.storedCondition(t, timeout)}
	for i := 0; i < 3; i++ {
		f.expectWrites(t, "the gang scheduling condition recorded as asked", func() { f.writeGangSchedulingCondition(t) }, 0)
		f.expectWrites(t, "the readiness deadline condition recorded as asked", func() { f.reportReadyTimeout(t) }, 0)
	}
	for condType, recorded := range before {
		if after := f.storedCondition(t, condType); after != recorded {
			t.Errorf("the stored %s condition moved with nothing to write: %+v, was %+v", condType, after, recorded)
		}
	}
}

func TestWriteAggregateCondition_ANewConditionIsWrittenAtThePassClock(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	ready := metav1.Condition{Type: InferenceReplicaConditionCurrentRevisionWithdrawn, Status: metav1.ConditionTrue, Reason: "ProbeReason", Message: "a condition no pass has written", ObservedGeneration: f.passCopy.Generation}
	f.expectWrites(t, "a condition the store does not carry", func() {
		if err := f.writeCondition(context.Background(), ready); err != nil {
			t.Fatalf("write the condition: %v", err)
		}
	}, 1)
	expectTransitionAtThePassClock(t, f.stored(t).Status.Conditions, ready.Type)
	f.expectWrites(t, "the same condition again", func() {
		if err := f.writeCondition(context.Background(), ready); err != nil {
			t.Fatalf("write the condition again: %v", err)
		}
	}, 0)
}

// TestWriteAggregateCondition_AChangedReasonMessageOrGenerationIsWrittenAndTheTimeKept:
// a condition whose status stands and whose reason, message or observed
// generation differs is written, and the recorded transition time is kept.
func TestWriteAggregateCondition_AChangedReasonMessageOrGenerationIsWrittenAndTheTimeKept(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*metav1.Condition)
		field  func(metav1.Condition) string
	}{
		{name: "reason", change: func(c *metav1.Condition) { c.Reason = "ProbeReason" }, field: func(c metav1.Condition) string { return c.Reason }},
		{name: "message", change: func(c *metav1.Condition) { c.Message = "another message" }, field: func(c metav1.Condition) string { return c.Message }},
		{name: "observed generation", change: func(c *metav1.Condition) { c.ObservedGeneration++ }, field: func(c metav1.Condition) string { return strconv.FormatInt(c.ObservedGeneration, 10) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newConditionPromotionFixture(t, probeShape)
			gang := string(workloadtypes.ConditionGangSchedulingUnavailable)
			recorded := f.storedCondition(t, gang)
			changed := gangSchedulingAvailableCondition(f.passCopy.Generation)
			tc.change(&changed)
			changed.LastTransitionTime = metav1.NewTime(publicationClock.Add(time.Hour))
			f.expectWrites(t, "the condition with another "+tc.name, func() {
				if err := f.writeCondition(context.Background(), changed); err != nil {
					t.Fatalf("write the changed condition: %v", err)
				}
			}, 1)
			after := f.storedCondition(t, gang)
			if got, want := tc.field(after), tc.field(changed); got != want {
				t.Errorf("the stored %s is %q, want %q", tc.name, got, want)
			}
			if !after.LastTransitionTime.Time.Equal(recorded.LastTransitionTime.Time) {
				t.Errorf("the transition time moved to %s while the status stood; want %s", after.LastTransitionTime.Time, recorded.LastTransitionTime.Time)
			}
		})
	}
}

func TestWriteAggregateCondition_AChangedStatusIsWrittenAtTheTimeItNames(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	gang := string(workloadtypes.ConditionGangSchedulingUnavailable)
	changed := gangSchedulingAvailableCondition(f.passCopy.Generation)
	changed.Status = metav1.ConditionTrue
	changed.LastTransitionTime = metav1.NewTime(publicationClock.Add(time.Hour))
	f.expectWrites(t, "the condition with another status", func() {
		if err := f.writeCondition(context.Background(), changed); err != nil {
			t.Fatalf("write the changed condition: %v", err)
		}
	}, 1)
	after := f.storedCondition(t, gang)
	if after.Status != changed.Status {
		t.Errorf("the stored status is %q, want %q", after.Status, changed.Status)
	}
	if !after.LastTransitionTime.Time.Equal(changed.LastTransitionTime.Time) {
		t.Errorf("the transition time is %s after the status changed, want the time the condition names, %s", after.LastTransitionTime.Time, changed.LastTransitionTime.Time)
	}
}

func TestWriteAggregateCondition_AChangedMessageIsWrittenAndTheTimeKept(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	gang := string(workloadtypes.ConditionGangSchedulingUnavailable)
	recorded := f.storedCondition(t, gang)
	changed := gangSchedulingAvailableCondition(f.passCopy.Generation)
	changed.Message = "another message"
	changed.LastTransitionTime = metav1.NewTime(publicationClock.Add(time.Hour))
	f.expectWrites(t, "the condition with another message", func() {
		if err := f.writeCondition(context.Background(), changed); err != nil {
			t.Fatalf("write the changed condition: %v", err)
		}
	}, 1)
	after := f.storedCondition(t, gang)
	if after.Message != changed.Message {
		t.Errorf("the stored message is %q, want %q", after.Message, changed.Message)
	}
	if !after.LastTransitionTime.Time.Equal(recorded.LastTransitionTime.Time) {
		t.Errorf("the transition time moved to %s while the status stood; want %s", after.LastTransitionTime.Time, recorded.LastTransitionTime.Time)
	}
}

func TestWriteAggregateCondition_ACopyBehindTheStoresOwnConditionWritesNothing(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	gang := string(workloadtypes.ConditionGangSchedulingUnavailable)
	recorded := f.storedCondition(t, gang)
	// The copy predates the pass that wrote the condition.
	apimeta.RemoveStatusCondition(&f.passCopy.Status.Conditions, gang)
	f.expectWrites(t, "a copy behind the store's own condition", func() { f.writeGangSchedulingCondition(t) }, 0)
	if after := f.storedCondition(t, gang); after != recorded {
		t.Errorf("the stored condition moved: %+v, was %+v", after, recorded)
	}
}

func TestPromoteCurrentRevision_AReplicaAtRestIsNotWritten(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	for i := 0; i < 3; i++ {
		f.expectWrites(t, "the promotion at rest", func() { f.promote(t) }, 0)
	}
	if got := f.stored(t).Status.CurrentRevision; got != f.target.Name {
		t.Errorf("the stored current revision is %q, want %q", got, f.target.Name)
	}
}

func TestPromoteCurrentRevision_ACopyBehindTheStoresPromotionWritesNothing(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	// The copy predates the pass that promoted the revision.
	f.passCopy.Status.CurrentRevision = irstatustest.OtherRevision
	f.expectWrites(t, "a copy behind the store's promotion", func() { f.promote(t) }, 0)
	if got := f.stored(t).Status.CurrentRevision; got != f.target.Name {
		t.Errorf("the stored current revision is %q, want %q", got, f.target.Name)
	}
	if f.passCopy.Status.CurrentRevision != irstatustest.OtherRevision {
		t.Errorf("the copy's current revision moved to %q with nothing written; the pass that shows the store's write brings the name", f.passCopy.Status.CurrentRevision)
	}
}

func TestPromoteCurrentRevision_RowsRestingOnTheTargetPromoteTheStore(t *testing.T) {
	f := newConditionPromotionFixture(t, probeShape)
	f.storeStatus(t, func(status *v1beta1.InferenceReplicaStatus) { status.CurrentRevision = irstatustest.OtherRevision })
	f.decodePassCopy(t)
	f.expectWrites(t, "rows resting on the target under another current revision", func() { f.promote(t) }, 1)
	if got := f.stored(t).Status.CurrentRevision; got != f.target.Name {
		t.Errorf("the stored current revision is %q, want %q", got, f.target.Name)
	}
	if f.passCopy.Status.CurrentRevision != f.target.Name {
		t.Errorf("the copy's current revision is %q after the write, want %q", f.passCopy.Status.CurrentRevision, f.target.Name)
	}
}
