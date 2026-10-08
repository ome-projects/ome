package status

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestStampStep_GuardedAndWireStable(t *testing.T) {
	now := time.Date(2026, time.August, 15, 12, 0, 0, 123456789, time.UTC)
	expected := terminalStatusFixture(3)
	persisted := cloneTerminalStatus(expected)
	persisted.Operation.StartedAt = metav1.NewTime(persisted.Operation.StartedAt.Truncate(time.Second))
	persisted.Operation.LastProgressAt = metav1.NewTime(persisted.Operation.LastProgressAt.Truncate(time.Second))
	store := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{3: persisted}}
	input := types.ReconcileInput{
		OwnerObject:                          &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		Clock:                                clocktesting.NewFakeClock(now),
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}

	claimed, err := StampStep(context.Background(), input, &expected, types.UpdateStepSurgeDrain, true)
	if err != nil || !claimed {
		t.Fatalf("transition: claimed=%v err=%v", claimed, err)
	}
	if store.writes != 1 || store.statuses[3].Operation.Step != types.UpdateStepSurgeDrain {
		t.Fatalf("persisted transition: writes=%d row=%+v", store.writes, store.statuses[3])
	}
	if !store.statuses[3].Operation.LastProgressAt.Time.Equal(now) {
		t.Fatalf("LastProgressAt=%v want %v", store.statuses[3].Operation.LastProgressAt, now)
	}

	stale := terminalStatusFixture(3)
	stale.Operation.ID = "stale-attempt"
	claimed, err = StampStep(context.Background(), input, &stale, types.UpdateStepSurgeDrainSettle, true)
	if err != nil || claimed || store.writes != 1 {
		t.Fatalf("stale transition: claimed=%v err=%v writes=%d", claimed, err, store.writes)
	}
}

// TestDemoteUnbacked_KeepsARowAnOperationClaimed pins the truth-pass write: Ready with no Operation
// demotes to Pending preserving everything else, and a row an operation
// claimed since selection is left untouched by the fresh-row guard.
func TestDemoteUnbacked_KeepsARowAnOperationClaimed(t *testing.T) {
	rows := map[int32]*types.InstanceStatus{
		0: {Index: 0, Incarnation: 3, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"},
		1: {Index: 1, Incarnation: 2, Phase: types.InstancePhaseReady,
			Operation: &types.InstanceOperation{ID: "u-1", Type: types.InstanceOperationUpdate}},
	}
	input := types.ReconcileInput{
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			mutate(rows[idx])
			return nil
		},
	}

	demoted, err := DemoteUnbacked(context.Background(), input, 0)
	if err != nil || !demoted {
		t.Fatalf("unbacked row: demoted=%v err=%v", demoted, err)
	}
	if rows[0].Phase != types.InstancePhasePending {
		t.Errorf("phase = %s, want Pending", rows[0].Phase)
	}
	if rows[0].Incarnation != 3 || rows[0].RunningRevision != "rev-a" {
		t.Errorf("the demotion rewrote more than the phase: %+v", rows[0])
	}

	demoted, err = DemoteUnbacked(context.Background(), input, 1)
	if err != nil || demoted {
		t.Fatalf("claimed row: demoted=%v err=%v", demoted, err)
	}
	if rows[1].Phase != types.InstancePhaseReady {
		t.Errorf("phase = %s, want the claiming operation's row untouched", rows[1].Phase)
	}
}

// The clearing stamp ends the attempt and records its cause; a slot the
// writer had to append is never resurrected, and an already-disposed row
// is left alone.
func TestStampFailed_NeverResurrectsAnEmptySlot(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	termination := &types.InstanceTermination{Reason: "OOMKilled", Time: metav1.NewTime(now)}

	w := newRowWriter(openRow(0, types.InstancePhaseUpdating))
	if err := StampFailed(context.Background(), w.input(now), 0, termination); err != nil {
		t.Fatal(err)
	}
	got := w.rows[0]
	if got.Phase != types.InstancePhaseFailed || got.Operation != nil || got.LastFailure == nil {
		t.Fatalf("row = %+v, want Failed with no operation and a record", got)
	}
	if err := StampFailed(context.Background(), w.input(now), 0, termination); err != nil {
		t.Fatal(err)
	}
	if w.writes != 1 {
		t.Fatalf("writes = %d, want the second call to be a no-op", w.writes)
	}

	empty := newRowWriter()
	if err := StampFailed(context.Background(), empty.input(now), 9, termination); err != nil {
		t.Fatal(err)
	}
	if empty.writes != 0 {
		t.Fatal("a slot with no phase must not be resurrected")
	}
}

// The disposition's clearing stamp ends only the attempt the pass
// observed: a row that has no operation, or carries another attempt, by
// the time the write lands concluded or re-opened since the observation
// and is left exactly as it is.
func TestStampFailedEndingAttempt_EndsOnlyTheAttemptObserved(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	termination := &types.InstanceTermination{Reason: "DeadlineExceeded", Time: metav1.NewTime(now)}
	observed := types.InstanceOperation{ID: "update-0-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepInPlace, TargetRevision: "rev-b"}

	open := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-a", Operation: &observed}
	w := newRowWriter(open)
	ended, err := StampFailedEndingAttempt(context.Background(), w.input(now), 0, observed, termination)
	if err != nil {
		t.Fatal(err)
	}
	if got := w.rows[0]; !ended || got.Phase != types.InstancePhaseFailed || got.Operation != nil || got.LastFailure == nil || got.LastFailure.Reason != "DeadlineExceeded" {
		t.Fatalf("row = %+v (ended=%v), want Failed with the operation cleared and the record on it", got, ended)
	}
	ended, err = StampFailedEndingAttempt(context.Background(), w.input(now), 0, observed, termination)
	if err != nil {
		t.Fatal(err)
	}
	if ended || w.writes != 1 {
		t.Fatalf("ended=%v writes=%d, want the second end to be a no-op", ended, w.writes)
	}

	promoted := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: "rev-b"}
	reopened := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-a",
		Operation: &types.InstanceOperation{ID: "update-0-2", Type: types.InstanceOperationUpdate, Step: types.UpdateStepInPlace, TargetRevision: "rev-c"}}
	for name, fresh := range map[string]types.InstanceStatus{"promoted": promoted, "re-opened": reopened} {
		rw := newRowWriter(fresh)
		ended, err := StampFailedEndingAttempt(context.Background(), rw.input(now), 0, observed, termination)
		if err != nil {
			t.Fatal(err)
		}
		if got := rw.rows[0]; ended || rw.writes != 0 || got.Phase != fresh.Phase || got.LastFailure != nil {
			t.Fatalf("%s row = %+v (ended=%v), want it left alone: the attempt observed is not on it", name, got, ended)
		}
	}

	appended := newRowWriter()
	if ended, err := StampFailedEndingAttempt(context.Background(), appended.input(now), 9, observed, termination); err != nil || ended || appended.writes != 0 {
		t.Fatalf("ended=%v err=%v writes=%d, want a slot with no phase left unresurrected", ended, err, appended.writes)
	}
}

// The fresh-row read tells an attempt still on the row from one re-opened
// under another attempt and from one concluded with no attempt left, and
// writes nothing while it reads.
func TestAttemptStandingOnFreshRow_ReadsWithoutWriting(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	observed := types.InstanceOperation{ID: "update-0-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-b"}
	for name, tc := range map[string]struct {
		row  types.InstanceStatus
		want AttemptStanding
	}{
		"on the row":  {row: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &observed}, want: AttemptOnRow},
		"re-opened":   {row: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{ID: "update-0-2", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-c"}}, want: AttemptReopened},
		"promoted":    {row: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: "rev-b"}, want: AttemptConcluded},
		"appended":    {row: types.InstanceStatus{Index: 0}, want: AttemptConcluded},
		"other index": {row: types.InstanceStatus{Index: 3, Phase: types.InstancePhaseUpdating, Operation: &observed}, want: AttemptConcluded},
	} {
		w := newRowWriter(tc.row)
		got, err := AttemptStandingOnFreshRow(context.Background(), w.input(now), 0, observed)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want || w.writes != 0 {
			t.Fatalf("%s: standing = %v writes = %d, want %v with nothing written", name, got, w.writes, tc.want)
		}
	}
}

// The deadline backstop keeps the Operation, because an operator reading
// the row wants to see what was in flight when the clock ran out.
func TestStampFailedKeepingOperation_KeepsTheOperationOnTheRow(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	termination := &types.InstanceTermination{Reason: "DeadlineExceeded", Time: now}
	mutate := StampFailedKeepingOperation(termination)

	row := openRow(0, types.InstancePhaseUpdating)
	if !mutate(&row) || row.Phase != types.InstancePhaseFailed || row.Operation == nil {
		t.Fatalf("row = %+v, want Failed with the operation preserved", row)
	}
	if mutate(&row) {
		t.Fatal("an already-Failed row is a no-op")
	}
	concluded := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating}
	if mutate(&concluded) {
		t.Fatal("a row whose attempt already ended has nothing left to expire")
	}
	appended := types.InstanceStatus{Index: 0}
	if mutate(&appended) {
		t.Fatal("a slot with no phase must not be resurrected")
	}
}

// The stuck-pod stamp needs no operation: the wedged-pod recovery shape
// under a settled row has none, and the row still fails with the
// diagnostics on it.
func TestStampFailedOnStuckPod_NeedsNoOperation(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	termination := &types.InstanceTermination{PodName: "engine-0-default-0", Reason: "CrashLoopBackOff", Time: now}
	mutate := StampFailedOnStuckPod(termination, 0)

	settled := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	if !mutate(&settled) || settled.Phase != types.InstancePhaseFailed || settled.LastFailure == nil {
		t.Fatalf("row = %+v, want Failed with the record and no operation required", settled)
	}
	inFlight := openRow(0, types.InstancePhaseUpdating)
	if !mutate(&inFlight) || inFlight.Operation == nil {
		t.Fatalf("row = %+v, want the operation in flight preserved", inFlight)
	}
	if mutate(&inFlight) {
		t.Fatal("an already-Failed row is a no-op")
	}
	appended := types.InstanceStatus{Index: 0}
	if mutate(&appended) {
		t.Fatal("a slot with no phase must not be resurrected")
	}
	untouched := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady, LastFailure: settled.LastFailure}
	if !StampFailedOnStuckPod(nil, 0)(&untouched) || untouched.LastFailure != settled.LastFailure {
		t.Fatal("a nil termination leaves LastFailure alone")
	}
}

// A Restart stamped over a Failed row that already carries a Restart is
// that repair's next attempt, so the new operation continues the spent
// one's retry count; a Restart opened on any other row starts at zero.
func TestStampRestarting_ContinuesASpentRepairsRetryCount(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	rows := map[int32]*types.InstanceStatus{
		0: {Index: 0, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a",
			Operation: &types.InstanceOperation{ID: "restart-0-1", Type: types.InstanceOperationRestart, Step: types.RestartStepDrain, RetryCount: 1}},
		1: {Index: 1, Incarnation: 2, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"},
		2: {Index: 2, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a",
			Operation: &types.InstanceOperation{ID: "update-2-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, RetryCount: 1}},
	}
	input := types.ReconcileInput{
		Clock: clocktesting.NewFakeClock(now),
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			mutate(rows[idx])
			return nil
		},
	}
	for idx, want := range map[int32]int32{0: 2, 1: 0, 2: 0} {
		inc, err := StampRestarting(context.Background(), input, idx, "again", time.Hour, nil)
		if err != nil {
			t.Fatalf("row %d: %v", idx, err)
		}
		got := rows[idx]
		if inc != 3 || got.Incarnation != 3 || got.Phase != types.InstancePhaseRestarting || got.Operation == nil {
			t.Fatalf("row %d = %+v, want Restarting at incarnation 3", idx, got)
		}
		if got.Operation.RetryCount != want {
			t.Errorf("row %d: RetryCount = %d, want %d", idx, got.Operation.RetryCount, want)
		}
	}
}

// TestStampRestartRevision_OnlyAnOpenRepairRecordsIt: the stamp moves the
// running revision of a Restarting row and leaves every other row as it
// is, so a repair that ended, or a row another pass owns, never has its
// revision moved under it; recording the revision the row already carries
// writes nothing.
func TestStampRestartRevision_OnlyAnOpenRepairRecordsIt(t *testing.T) {
	openRepair := func() *types.InstanceOperation {
		return &types.InstanceOperation{ID: "restart-0-1", Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}
	}
	rows := map[int32]*types.InstanceStatus{
		0: {Index: 0, Incarnation: 2, Phase: types.InstancePhaseRestarting, RunningRevision: "rev-a", Operation: openRepair()},
		1: {Index: 1, Incarnation: 2, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"},
		2: {Index: 2, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", Operation: openRepair()},
		3: {Index: 3, Incarnation: 2, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-a", TargetRevision: "rev-b",
			Operation: &types.InstanceOperation{ID: "update-3-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-b"}},
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
	for idx, want := range map[int32]string{0: "rev-b", 1: "rev-a", 2: "rev-a", 3: "rev-a"} {
		if err := StampRestartRevision(context.Background(), input, idx, "rev-b"); err != nil {
			t.Fatalf("row %d: %v", idx, err)
		}
		if got := rows[idx].RunningRevision; got != want {
			t.Errorf("row %d: RunningRevision = %q, want %q", idx, got, want)
		}
	}
	if writes != 1 {
		t.Errorf("writes = %d, want 1: only the open repair's row is written", writes)
	}
	if err := StampRestartRevision(context.Background(), input, 0, "rev-b"); err != nil {
		t.Fatalf("second stamp: %v", err)
	}
	if writes != 1 {
		t.Errorf("writes = %d after stamping the revision the row already carries, want 1", writes)
	}
}

// The unpark write: a Failed, operation-free row returns to Ready with
// its anchor and record time kept and the note appended once; other rows stay.
func TestUnparkServing_ReturnsAParkToReadyKeepingItsAnchor(t *testing.T) {
	readySince := metav1.NewTime(time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC))
	crashedAt := metav1.NewTime(readySince.Add(10 * time.Minute))
	record := func() *types.InstanceTermination {
		return &types.InstanceTermination{PodName: "p-0", ContainerName: "runner", Reason: "Error", Message: "back-off", Time: crashedAt}
	}
	rows := map[int32]*types.InstanceStatus{
		0: {Index: 0, Incarnation: 3, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", ReadySince: &readySince, LastFailure: record()},
		1: {Index: 1, Incarnation: 2, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", ReadySince: &readySince, LastFailure: record(),
			Operation: &types.InstanceOperation{ID: "r-1", Type: types.InstanceOperationRestart}},
		2: {Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince, LastFailure: record()},
		3: {Index: 3, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", ReadySince: &readySince},
		4: {Index: 4, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", ReadySince: &readySince, LastFailure: record()},
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
	const note = "crash loop on a held revision"
	for idx := int32(0); idx < 4; idx++ {
		if err := UnparkServing(context.Background(), input, idx, note); err != nil {
			t.Fatalf("UnparkServing(%d): %v", idx, err)
		}
	}
	if writes != 1 {
		t.Fatalf("writes = %d, want the one park", writes)
	}
	got := rows[0]
	if got.Phase != types.InstancePhaseReady || got.Operation != nil || got.Incarnation != 3 || got.RunningRevision != "rev-a" {
		t.Fatalf("park after unpark = %+v, want Ready with everything else kept", got)
	}
	if got.ReadySince == nil || !got.ReadySince.Equal(&readySince) {
		t.Fatalf("ReadySince = %v, want the promotion %v kept", got.ReadySince, readySince)
	}
	if got.LastFailure == nil || !got.LastFailure.Time.Equal(&crashedAt) || got.LastFailure.Message != "back-off; "+note {
		t.Fatalf("record = %+v, want its time kept and the note appended", got.LastFailure)
	}
	if rows[1].Phase != types.InstancePhaseFailed || rows[2].Phase != types.InstancePhaseReady || rows[3].Phase != types.InstancePhaseFailed {
		t.Fatalf("rows an operation claims, already Ready, or without a record must be untouched: %+v %+v %+v", rows[1], rows[2], rows[3])
	}
	if err := UnparkServing(context.Background(), input, 4, ""); err != nil {
		t.Fatalf("UnparkServing(4): %v", err)
	}
	if rows[4].Phase != types.InstancePhaseReady || rows[4].LastFailure.Message != "back-off" {
		t.Fatalf("an unpark with no note = %+v, want Ready with the message as it was", rows[4])
	}
}

// The stuck-pod stamp is decided on the pass's observation and applied
// to the fresh row, and only a Restart bumps a row's incarnation: a
// blamed pod below the fresh row's incarnation is the set a repair
// claimed since the observation, and the stamp is withheld. A pod at the
// row's incarnation, a pod with no label, and a row that records no
// incarnation are stamped.
func TestStampFailedOnStuckPod_WithholdsFromAPodTheRepairReplaced(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	termination := &types.InstanceTermination{PodName: "engine-0-default-0", Reason: "CrashLoopBackOff", Time: now}
	claimed := func() types.InstanceStatus {
		return types.InstanceStatus{Index: 0, Incarnation: 2, Phase: types.InstancePhaseRestarting,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationRestart, Step: types.RestartStepDrain}}
	}
	row := claimed()
	if StampFailedOnStuckPod(termination, 1)(&row) || row.Phase != types.InstancePhaseRestarting || row.LastFailure != nil {
		t.Fatalf("row = %+v, want the repair's claim kept over a pod of the incarnation it replaces", row)
	}
	row = claimed()
	if !StampFailedOnStuckPod(termination, 2)(&row) || row.Phase != types.InstancePhaseFailed || row.Operation == nil {
		t.Fatalf("row = %+v, want Failed with the operation preserved for a pod of the rebuild", row)
	}
	row = claimed()
	if !StampFailedOnStuckPod(termination, 0)(&row) || row.Phase != types.InstancePhaseFailed {
		t.Fatalf("row = %+v, want Failed for a pod that carries no incarnation", row)
	}
	unrecorded := types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	if !StampFailedOnStuckPod(termination, 1)(&unrecorded) || unrecorded.Phase != types.InstancePhaseFailed {
		t.Fatalf("row = %+v, want Failed for a row that records no incarnation", unrecorded)
	}
}

// ApplyStamp runs the stamp through the single-row seam and reports
// whether it changed the row; the seam's own answer is not visible to
// the caller otherwise.
func TestApplyStamp_ReportsWhetherTheStampTook(t *testing.T) {
	rows := map[int32]*types.InstanceStatus{0: {Index: 0, Phase: types.InstancePhaseReady}}
	writes := 0
	input := types.ReconcileInput{MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
		if mutate(rows[idx]) {
			writes++
		}
		return nil
	}}
	stamp := StampFailedOnStuckPod(&types.InstanceTermination{Reason: "CrashLoopBackOff"}, 0)
	committed, err := ApplyStamp(context.Background(), input, 0, stamp)
	if err != nil || !committed || writes != 1 {
		t.Fatalf("first apply: committed=%v err=%v writes=%d, want the stamp to take once", committed, err, writes)
	}
	committed, err = ApplyStamp(context.Background(), input, 0, stamp)
	if err != nil || committed || writes != 1 {
		t.Fatalf("second apply: committed=%v err=%v writes=%d, want a no-op on the Failed row", committed, err, writes)
	}
}

// The two Update entry stamps never continue an attempt parked after its
// disposition: a new attempt opens over it with its own identity and
// deadline, as it does over a Failed row, and the recreate bumps the
// incarnation so the parked set becomes the one it drains.
func TestUpdateEntryStamps_OpenANewAttemptOverAParkedOne(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	parked := func() types.InstanceStatus {
		return types.InstanceStatus{
			Index: 0, Incarnation: 2, Phase: types.InstancePhaseUpdating, TargetRevision: "rev-b",
			Operation: &types.InstanceOperation{
				ID: "update-0-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepParked, TargetRevision: "rev-b",
				Waiting: string(types.RolloutHoldGateRetryBlock),
			},
		}
	}

	w := newRowWriter(parked())
	inc, err := StampRecreating(context.Background(), w.input(now), 0, "rev-b", "revision rev-a -> rev-b", types.UpdateStrategyRecreatePod, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	row := w.rows[0]
	if inc != 3 || row.Incarnation != 3 || row.Operation == nil || row.Operation.ID == "update-0-1" ||
		row.Operation.Waiting != "" || row.Operation.Deadline.IsZero() || row.Operation.Step != types.UpdateStepDrain {
		t.Fatalf("recreate over a parked attempt = incarnation %d row %+v, want a fresh attempt at a bumped incarnation", inc, row)
	}

	w = newRowWriter(parked())
	if err := StampUpdatingInPlace(context.Background(), w.input(now), 0, "rev-b", types.UpdateStrategyInPlaceIfPossible, time.Hour); err != nil {
		t.Fatal(err)
	}
	row = w.rows[0]
	if row.Operation == nil || row.Operation.ID == "update-0-1" || row.Operation.Waiting != "" ||
		row.Operation.Deadline.IsZero() || row.Operation.Step != types.UpdateStepInPlace {
		t.Fatalf("in-place over a parked attempt = %+v, want a fresh attempt with its own deadline", row)
	}
}

// The parking stamp keeps the attempt on the row with the wait named and
// the clock parked, records the failure, and lands only on a row whose
// Update attempt is still the one observed - a row re-opened by another
// attempt since, at any revision, is left alone; the follow stamp moves
// the phase with the set and the wait with the ladder, and leaves every
// other row alone.
func TestParkedAttemptStamps_KeepTheAttemptAndFollowTheSet(t *testing.T) {
	now := time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC)
	termination := &types.InstanceTermination{PodName: "engine-0", Reason: "CrashLoopBackOff", Time: metav1.NewTime(now)}
	attempt := types.InstanceStatus{
		Index: 0, Incarnation: 2, Phase: types.InstancePhaseUpdating, TargetRevision: "rev-b",
		Operation: &types.InstanceOperation{
			ID: "update-0-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain,
			TargetRevision: "rev-b", Deadline: metav1.NewTime(now.Add(time.Hour)),
		},
	}

	w := newRowWriter(attempt)
	if _, err := StampFailedParkingAttempt(context.Background(), w.input(now), 0, *attempt.Operation, termination, string(types.RolloutHoldGateRetryBlock)); err != nil {
		t.Fatal(err)
	}
	row := w.rows[0]
	if row.Phase != types.InstancePhaseFailed || !types.OperationParked(row.Operation) || row.Operation.ID != "update-0-1" ||
		row.Operation.Step != types.UpdateStepParked || row.Operation.TargetRevision != "rev-b" ||
		!row.Operation.Deadline.IsZero() || row.LastFailure == nil || row.LastFailure.Reason != "CrashLoopBackOff" {
		t.Fatalf("parked row = %+v, want Failed with the attempt kept on the parked step, its clock parked and the failure recorded", row)
	}
	if _, err := StampFailedParkingAttempt(context.Background(), w.input(now), 0, *attempt.Operation, termination, string(types.RolloutHoldGateRetryBlock)); err != nil {
		t.Fatal(err)
	}
	if w.writes != 1 {
		t.Fatalf("writes = %d, want the second park to be a no-op", w.writes)
	}

	concluded := newRowWriter(types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady})
	if _, err := StampFailedParkingAttempt(context.Background(), concluded.input(now), 0, *attempt.Operation, termination, string(types.RolloutHoldGateRetryBlock)); err != nil {
		t.Fatal(err)
	}
	if concluded.writes != 0 {
		t.Fatal("a row whose attempt already ended has nothing to park")
	}

	for _, fresh := range []*types.InstanceOperation{
		{ID: "update-0-2", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-c", Deadline: metav1.NewTime(now.Add(time.Hour))},
		{ID: "update-0-2", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-b", Deadline: metav1.NewTime(now.Add(time.Hour))},
	} {
		reopened := attempt
		reopened.Operation = fresh
		rw := newRowWriter(reopened)
		if _, err := StampFailedParkingAttempt(context.Background(), rw.input(now), 0, *attempt.Operation, termination, string(types.RolloutHoldGateRetryBlock)); err != nil {
			t.Fatal(err)
		}
		if got := rw.rows[0]; rw.writes != 0 || got.Phase != types.InstancePhaseUpdating || got.Operation != fresh || got.LastFailure != nil {
			t.Fatalf("a row re-opened by attempt %s at %s = %+v, want it left alone: neither parked nor cleared", fresh.ID, fresh.TargetRevision, got)
		}
	}

	follow := FollowParkedAttempt(true, string(types.RolloutHoldGateHeld))
	if !follow(&row) || row.Phase != types.InstancePhaseUpdating || row.Operation.Waiting != string(types.RolloutHoldGateHeld) {
		t.Fatalf("follow while serving = %+v, want Updating naming the hold", row)
	}
	if follow(&row) {
		t.Fatal("a row already where the set puts it is a no-op")
	}
	if !FollowParkedAttempt(false, "")(&row) || row.Phase != types.InstancePhaseFailed || row.Operation.Waiting != string(types.RolloutHoldGateHeld) {
		t.Fatalf("follow while down = %+v, want Failed with the wait kept", row)
	}
	inFlight := openRow(0, types.InstancePhaseUpdating)
	if FollowParkedAttempt(false, "")(&inFlight) || inFlight.Phase != types.InstancePhaseUpdating {
		t.Fatal("an attempt in flight is not followed")
	}

	waits := ParkedAttemptWaits(string(types.RolloutHoldGateBudget))
	if !waits(&row) || row.Operation.Waiting != string(types.RolloutHoldGateBudget) {
		t.Fatalf("the budget's denial must be named on the parked attempt, got %+v", row.Operation)
	}
	if waits(&row) || ParkedAttemptWaits("Unschedulable")(&row) || ParkedAttemptWaits(string(types.RolloutHoldGateBudget))(&inFlight) {
		t.Fatal("a wait already named, a token that is no gate, and an attempt in flight all write nothing")
	}
}

// TestEntryStamps_LadderFlipFollowsTheAttempt pins the RetryBlock half of
// the update entry stamps, which run again on every pass of their
// attempt: the pass that opens an attempt flips a due Backoff block to
// RetryInProgress, a later pass of an attempt opened once the block was
// due lands that flip if it is still owed, and a later pass of an attempt
// open since before the block's failure starts nothing on the ladder, so
// the block keeps counting that failure's wave.
func TestEntryStamps_LadderFlipFollowsTheAttempt(t *testing.T) {
	const target = "rev-b"
	now := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	stamps := []struct {
		name  string
		step  string
		stamp func(types.ReconcileInput) error
	}{
		{name: "surge", step: types.UpdateStepSurge, stamp: func(in types.ReconcileInput) error {
			return StampSurging(context.Background(), in, 0, target, types.UpdateStrategySurgeThenDrain, time.Hour)
		}},
		{name: "recreate", step: types.UpdateStepDrain, stamp: func(in types.ReconcileInput) error {
			_, err := StampRecreating(context.Background(), in, 0, target, "revision rev-a -> rev-b", types.UpdateStrategyRecreatePod, time.Hour)
			return err
		}},
		{name: "in-place", step: types.UpdateStepInPlace, stamp: func(in types.ReconcileInput) error {
			return StampUpdatingInPlace(context.Background(), in, 0, target, types.UpdateStrategyInPlaceIfPossible, time.Hour)
		}},
	}
	cases := []struct {
		name string
		// openedAgo is how long before now the attempt in flight opened;
		// zero leaves the row Ready, so the stamp opens the attempt.
		openedAgo time.Duration
		// dueIn is when the Backoff block becomes due, relative to now.
		dueIn    time.Duration
		wantFlip bool
	}{
		{name: "the pass that opens the attempt flips the due block", dueIn: -time.Second, wantFlip: true},
		{name: "an attempt opened once the block was due lands the owed flip", openedAgo: 5 * time.Second, dueIn: -10 * time.Second, wantFlip: true},
		{name: "an attempt open since before the failure leaves the block in Backoff", openedAgo: 10 * time.Minute, dueIn: 20 * time.Second},
	}
	for _, st := range stamps {
		for _, tc := range cases {
			t.Run(st.name+"/"+tc.name, func(t *testing.T) {
				row := types.InstanceStatus{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"}
				if tc.openedAgo > 0 {
					opened := metav1.NewTime(now.Add(-tc.openedAgo))
					row.Phase = types.InstancePhaseUpdating
					row.TargetRevision = target
					row.Operation = &types.InstanceOperation{
						ID: "update-0", Type: types.InstanceOperationUpdate, Step: st.step, TargetRevision: target,
						StartedAt: opened, LastProgressAt: opened, Deadline: metav1.NewTime(now.Add(time.Hour)),
					}
				}
				due := metav1.NewTime(now.Add(tc.dueIn))
				failed := metav1.NewTime(now.Add(-time.Minute))
				block := types.RetryBlock{TargetRevision: target, State: types.RetryBlockBackoff, AttemptsStarted: 1,
					NextRetryAt: &due, FirstFailureAt: &failed, LastFailureAt: &failed}
				in := types.ReconcileInput{
					Clock: clocktesting.NewFakeClock(now),
					MutateInstance: func(_ context.Context, _ int32, mutate func(*types.InstanceStatus) bool) error {
						mutate(&row)
						return nil
					},
					MutateRetryBlock: func(_ context.Context, _ string, mutate func(*types.RetryBlock) types.RetryBlockDisposition) error {
						mutate(&block)
						return nil
					},
				}

				if err := st.stamp(in); err != nil {
					t.Fatalf("stamp: %v", err)
				}
				if flipped := block.State == types.RetryBlockRetryInProgress; flipped != tc.wantFlip {
					t.Fatalf("block state = %s, want flipped=%v", block.State, tc.wantFlip)
				}
				if block.AttemptsStarted != 1 {
					t.Fatalf("a stamp counts no failure, got AttemptsStarted=%d", block.AttemptsStarted)
				}
			})
		}
	}
}

// The stuck-pod stamp is decided on the pass's pod observation and applied
// to the fresh row, and it reads the row's phase and the blamed pod's
// incarnation alone: a row the restart pass promoted later in the same
// pass, Ready with the operation cleared at the incarnation the stuck pod
// carries, is stamped Failed like the Restarting row the observation
// showed. Only the deadline stamp withholds itself from a cleared
// operation.
func TestStampFailedOnStuckPod_LandsOnARowPromotedSinceTheObservation(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, time.September, 21, 0, 0, 0, 0, time.UTC))
	termination := &types.InstanceTermination{PodName: "engine-0-default-0", Reason: "CrashLoopBackOff", Time: now}
	promoted := types.InstanceStatus{Index: 0, Incarnation: 2, Phase: types.InstancePhaseReady, ReadySince: &now}

	stuck := promoted
	if !StampFailedOnStuckPod(termination, 2)(&stuck) || stuck.Phase != types.InstancePhaseFailed || stuck.Operation != nil {
		t.Fatalf("row = %+v, want the promoted row stamped Failed with no operation: the stuck stamp does not read the cleared operation", stuck)
	}
	deadline := promoted
	if StampFailedKeepingOperation(termination)(&deadline) || deadline.Phase != types.InstancePhaseReady {
		t.Fatalf("row = %+v, want the deadline stamp withheld from a row whose operation the promote cleared", deadline)
	}
}
