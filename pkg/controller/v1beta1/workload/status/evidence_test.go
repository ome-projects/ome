package status

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The waiting token is written only for an owner the authority may speak
// for, and only when its precedence rule takes the row.
func TestRecordWaiting_IncumbentTokenIsKeptUnlessOutranked(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	anyOwner := func(types.RowOwner) bool { return true }
	firstWriterWins := func(token, current string) bool { return current == "" || current == token }

	w := newRowWriter(openRow(0, types.InstancePhaseUpdating))
	entered, held, err := RecordWaiting(context.Background(), w.input(now), 0, "Unschedulable", anyOwner, firstWriterWins, nil)
	if err != nil || !entered || !held {
		t.Fatalf("mark = entered:%v held:%v err:%v", entered, held, err)
	}
	entered, held, err = RecordWaiting(context.Background(), w.input(now), 0, "Unschedulable", anyOwner, firstWriterWins, nil)
	if err != nil || entered || !held {
		t.Fatalf("re-mark = entered:%v held:%v err:%v — the edge fires once, the hold stands", entered, held, err)
	}

	noOwner := func(types.RowOwner) bool { return false }
	_, held, err = RecordWaiting(context.Background(), w.input(now), 0, "QuotaExceeded", noOwner, firstWriterWins, nil)
	if err != nil || held {
		t.Fatalf("an authority refused by its own owner rule must not hold: held=%v err=%v", held, err)
	}

	if err := ReleaseWaiting(context.Background(), w.input(now), 0, "QuotaExceeded"); err != nil {
		t.Fatal(err)
	}
	if w.rows[0].Operation.Waiting != "Unschedulable" {
		t.Fatal("releasing another authority's token must leave the incumbent alone")
	}
	if err := ReleaseWaiting(context.Background(), w.input(now), 0, "Unschedulable"); err != nil {
		t.Fatal(err)
	}
	if w.rows[0].Operation.Waiting != "" {
		t.Fatalf("waiting = %q, want released", w.rows[0].Operation.Waiting)
	}
}

// Evidence is compared by which pod is held, why, and since when — never
// by its message, which moves on every pass.
func TestSameEvidence_KeysOnTheMoment(t *testing.T) {
	at := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	recorded := &types.InstanceTermination{PodName: "engine-0", Reason: "Unschedulable", Message: "3 nodes", Time: at}
	moved := &types.InstanceTermination{PodName: "engine-0", Reason: "Unschedulable", Message: "7 nodes", Time: at}
	later := &types.InstanceTermination{PodName: "engine-0", Reason: "Unschedulable", Time: metav1.NewTime(at.Add(time.Minute))}

	if !sameEvidence(recorded, moved) {
		t.Fatal("a changed message is the same hold")
	}
	if sameEvidence(recorded, later) {
		t.Fatal("a fresh moment is a fresh episode")
	}
	if !sameEvidence(nil, nil) {
		t.Fatal("no evidence to compare leaves the token to decide")
	}
	if sameEvidence(nil, recorded) {
		t.Fatal("an unrecorded hold is not the recorded one")
	}
}

// The refresh lands only on a Failed row still carrying the generic
// reason, and keeps the recorded time; a row that has since left Failed
// or named a concrete cause of its own keeps what it has.
func TestRecordRefreshedFailure_ReplacesOnlyTheGenericReasonInPlace(t *testing.T) {
	failedAt := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	const generic = "DeadlineExceeded"
	concrete := &types.InstanceTermination{PodName: "engine-0", ContainerName: "main", Reason: "CrashLoopBackOff", Time: metav1.NewTime(failedAt.Add(time.Hour))}
	refresh := RecordRefreshedFailure(concrete, generic)

	row := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseFailed, LastFailure: &types.InstanceTermination{Reason: generic, Time: failedAt}}
	if !refresh(&row) {
		t.Fatal("a Failed row on the generic reason must be refreshed")
	}
	if row.LastFailure.Reason != "CrashLoopBackOff" || row.LastFailure.PodName != "engine-0" || !row.LastFailure.Time.Equal(&failedAt) {
		t.Fatalf("LastFailure = %+v, want the concrete cause at the original time", row.LastFailure)
	}
	if refresh(&row) {
		t.Fatal("a concrete reason already on the record must not be replaced")
	}

	notFailed := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating, LastFailure: &types.InstanceTermination{Reason: generic, Time: failedAt}}
	if refresh(&notFailed) {
		t.Fatal("a row that left Failed must keep what it has")
	}
	noRecord := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseFailed}
	if refresh(&noRecord) {
		t.Fatal("a row with no record has nothing to refresh")
	}
}

// The refusal carrier records the moment and nothing else, and both
// edges are idempotent.
func TestRecordCapacityRefusal_ReportsOnlyTheFirstRefusal(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	w := newRowWriter(openRow(0, types.InstancePhaseCreating))

	w.rows[0].Operation.Waiting = types.WaitingReasonUnschedulable
	recorded, incumbent, err := RecordCapacityRefusal(context.Background(), w.input(now), 0)
	if err != nil || !recorded {
		t.Fatalf("record = %v, %v", recorded, err)
	}
	if incumbent != types.WaitingReasonUnschedulable {
		t.Fatalf("incumbent = %q, want the token the row reported when the refusal landed", incumbent)
	}
	if at := w.rows[0].Operation.CapacityRefusedAt; at == nil || !at.Time.Equal(now) {
		t.Fatalf("refusedAt = %v, want %v", at, now)
	}
	recorded, _, err = RecordCapacityRefusal(context.Background(), w.input(now), 0)
	if err != nil || recorded {
		t.Fatalf("re-record = %v, %v — the edge fires once", recorded, err)
	}

	if err := ClearCapacityRefusal(context.Background(), w.input(now), 0); err != nil {
		t.Fatal(err)
	}
	if w.rows[0].Operation.CapacityRefusedAt != nil {
		t.Fatal("completion must retire the record")
	}
	before := w.writes
	if err := ClearCapacityRefusal(context.Background(), w.input(now), 0); err != nil {
		t.Fatal(err)
	}
	if w.writes != before {
		t.Fatal("clearing an absent record must write nothing")
	}
}

// The note writes: recorded once on a Ready, operation-free row, cleared
// exactly on a row of any phase with the record's time kept; others stay.
func TestCrashLoopNote_RecordedOnceAndCleared(t *testing.T) {
	at := metav1.NewTime(time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC))
	const note = "crash loop on a held revision"
	rows := map[int32]*types.InstanceStatus{
		0: {Index: 0, Phase: types.InstancePhaseReady, LastFailure: &types.InstanceTermination{PodName: "p-0", Reason: "Error", Message: "back-off", Time: at}},
		1: {Index: 1, Phase: types.InstancePhaseReady, LastFailure: &types.InstanceTermination{PodName: "p-1", Reason: "Error", Time: at}},
		2: {Index: 2, Phase: types.InstancePhaseFailed, LastFailure: &types.InstanceTermination{PodName: "p-2", Reason: "Error", Message: "back-off; " + note, Time: at}},
		3: {Index: 3, Phase: types.InstancePhaseReady, LastFailure: &types.InstanceTermination{PodName: "p-3", Reason: "Error", Message: "back-off", Time: at},
			Operation: &types.InstanceOperation{ID: "u-1", Type: types.InstanceOperationUpdate}},
		4: {Index: 4, Phase: types.InstancePhaseReady},
	}
	writes := 0
	input := types.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			if mutate(rows[idx]) {
				writes++
			}
			return nil
		},
	}
	ctx := context.Background()
	for _, idx := range []int32{0, 0, 1, 3, 4} {
		if err := RecordCrashLoopNote(ctx, input, idx, note); err != nil {
			t.Fatalf("RecordCrashLoopNote(%d): %v", idx, err)
		}
	}
	if writes != 2 {
		t.Fatalf("writes = %d, want one per row that takes the note", writes)
	}
	if rows[0].LastFailure.Message != "back-off; "+note || !FailureNoted(rows[0], note) {
		t.Fatalf("row 0 = %+v, want the note appended once", rows[0].LastFailure)
	}
	if rows[1].LastFailure.Message != note {
		t.Fatalf("row 1 = %+v, want the note as the whole message", rows[1].LastFailure)
	}
	if rows[3].LastFailure.Message != "back-off" || rows[4].LastFailure != nil {
		t.Fatalf("a row an operation claims, or with no record, must be untouched: %+v %+v", rows[3], rows[4])
	}

	writes = 0
	for _, idx := range []int32{0, 0, 1, 2, 3, 4} {
		if err := ClearCrashLoopNote(ctx, input, idx, note); err != nil {
			t.Fatalf("ClearCrashLoopNote(%d): %v", idx, err)
		}
	}
	if writes != 3 {
		t.Fatalf("writes = %d, want one per row that carried the note", writes)
	}
	for idx, want := range map[int32]string{0: "back-off", 1: "", 2: "back-off"} {
		if got := rows[idx].LastFailure; got.Message != want || !got.Time.Equal(&at) || FailureNoted(rows[idx], note) {
			t.Fatalf("row %d after the clear = %+v, want message %q with the time kept", idx, got, want)
		}
	}
	if rows[2].Phase != types.InstancePhaseFailed {
		t.Fatalf("the clear touches nothing but the message; row 2 = %+v", rows[2])
	}
}
