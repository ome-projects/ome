package status_test

// ClearFailedInstanceOperation: the operator-reset release of a parked
// attempt clears only a repair-owned Operation (Create / Restart) of a
// Failed Instance and writes nothing for every other shape.
// CloseSpentRepair: the restart pass's close of a parked repair with no
// live pod left clears only the Restart operation the pass read, and
// writes nothing for a row that has moved on.

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// recordingMutate is a MutateInstance seam over one in-memory status:
// it applies the callback and records whether a write was requested.
func recordingMutate(s *types.InstanceStatus) (func(context.Context, int32, func(*types.InstanceStatus) bool) error, *bool) {
	wrote := false
	return func(_ context.Context, _ int32, mutate func(*types.InstanceStatus) bool) error {
		wrote = mutate(s)
		return nil
	}, &wrote
}

func parkedOperation(typ types.InstanceOperationType) *types.InstanceOperation {
	now := metav1.Now()
	return &types.InstanceOperation{ID: "op-3-1", Type: typ, Step: "WaitReady", StartedAt: now, Deadline: now}
}

func TestResetOwnsOperation(t *testing.T) {
	if !status.ResetOwnsOperation(nil) {
		t.Fatalf("no Operation must be reset-owned")
	}
	owned := []types.InstanceOperationType{types.InstanceOperationCreate, types.InstanceOperationRestart}
	for _, typ := range owned {
		if !status.ResetOwnsOperation(parkedOperation(typ)) {
			t.Errorf("%s must be reset-owned", typ)
		}
	}
	foreign := []types.InstanceOperationType{types.InstanceOperationUpdate, types.InstanceOperationMigrate, types.InstanceOperationDelete}
	for _, typ := range foreign {
		if status.ResetOwnsOperation(parkedOperation(typ)) {
			t.Errorf("%s must NOT be reset-owned", typ)
		}
	}
}

func TestClearFailedInstanceOperation(t *testing.T) {
	now := metav1.Now()
	failure := &types.InstanceTermination{PodName: "engine-3-0", Reason: "OOMKilled", Time: now}

	cases := []struct {
		name        string
		status      types.InstanceStatus
		wantCleared bool
	}{
		{
			name:        "failed with preserved Restart clears it",
			status:      types.InstanceStatus{Index: 3, Phase: types.InstancePhaseFailed, Operation: parkedOperation(types.InstanceOperationRestart), LastFailure: failure, Incarnation: 2},
			wantCleared: true,
		},
		{
			name:        "failed with preserved Create clears it",
			status:      types.InstanceStatus{Index: 3, Phase: types.InstancePhaseFailed, Operation: parkedOperation(types.InstanceOperationCreate), LastFailure: failure, Incarnation: 1},
			wantCleared: true,
		},
		{
			name:   "failed with preserved Update is refused",
			status: types.InstanceStatus{Index: 3, Phase: types.InstancePhaseFailed, Operation: parkedOperation(types.InstanceOperationUpdate), LastFailure: failure},
		},
		{
			name:        "failed with a parked Update attempt clears it",
			status:      types.InstanceStatus{Index: 3, Phase: types.InstancePhaseFailed, Operation: parkedUpdateAttempt(), LastFailure: failure, Incarnation: 2},
			wantCleared: true,
		},
		{
			name:        "a parked Update attempt read Updating clears it and reads Failed",
			status:      types.InstanceStatus{Index: 3, Phase: types.InstancePhaseUpdating, Operation: parkedUpdateAttempt(), LastFailure: failure, Incarnation: 2},
			wantCleared: true,
		},
		{
			name:   "updating with an Update attempt in flight writes nothing",
			status: types.InstanceStatus{Index: 3, Phase: types.InstancePhaseUpdating, Operation: parkedOperation(types.InstanceOperationUpdate), LastFailure: failure},
		},
		{
			name:   "failed with preserved Migrate is refused",
			status: types.InstanceStatus{Index: 3, Phase: types.InstancePhaseFailed, Operation: parkedOperation(types.InstanceOperationMigrate)},
		},
		{
			name:   "failed with preserved Delete is refused",
			status: types.InstanceStatus{Index: 3, Phase: types.InstancePhaseFailed, Operation: parkedOperation(types.InstanceOperationDelete)},
		},
		{
			name:   "failed without operation writes nothing",
			status: types.InstanceStatus{Index: 3, Phase: types.InstancePhaseFailed, LastFailure: failure},
		},
		{
			name:   "non-failed with operation writes nothing",
			status: types.InstanceStatus{Index: 3, Phase: types.InstancePhaseRestarting, Operation: parkedOperation(types.InstanceOperationRestart)},
		},
		{
			name:   "fresh-empty slot writes nothing",
			status: types.InstanceStatus{Index: 3},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.status
			before := s
			mutate, wrote := recordingMutate(&s)

			cleared, err := status.ClearFailedInstanceOperation(context.Background(), mutate, 3)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cleared != tc.wantCleared || *wrote != tc.wantCleared {
				t.Fatalf("cleared=%v wrote=%v, want both %v", cleared, *wrote, tc.wantCleared)
			}
			if !tc.wantCleared {
				if s.Operation != before.Operation || s.Phase != before.Phase {
					t.Fatalf("status must be untouched: got %+v", s)
				}
				return
			}
			if s.Operation != nil {
				t.Fatalf("Operation must be cleared, got %+v", s.Operation)
			}
			if s.Phase != types.InstancePhaseFailed {
				t.Fatalf("Phase must read Failed, got %q", s.Phase)
			}
			if s.LastFailure != failure || s.Incarnation != before.Incarnation {
				t.Fatalf("LastFailure and Incarnation must survive: got %+v", s)
			}
		})
	}
}

func TestClearFailedInstanceOperation_SeamErrorPropagates(t *testing.T) {
	boom := errors.New("re-read IR: simulated outage")
	mutate := func(context.Context, int32, func(*types.InstanceStatus) bool) error { return boom }

	cleared, err := status.ClearFailedInstanceOperation(context.Background(), mutate, 3)
	if !errors.Is(err, boom) {
		t.Fatalf("seam error must propagate, got %v", err)
	}
	if cleared {
		t.Fatalf("a failed write must not report cleared")
	}
}

func TestCloseSpentRepair(t *testing.T) {
	now := metav1.Now()
	failure := &types.InstanceTermination{PodName: "engine-3-0", Reason: "CrashLoopBackOff", Time: now}
	parked := func(typ types.InstanceOperationType, id string, phase types.InstancePhase) types.InstanceStatus {
		return types.InstanceStatus{
			Index: 3, Incarnation: 2, Phase: phase, RunningRevision: "engine-rev",
			Operation:   &types.InstanceOperation{ID: id, Type: typ, Step: "Drain", StartedAt: now, Deadline: now, RetryCount: 1},
			LastFailure: failure,
		}
	}
	cases := []struct {
		name       string
		status     types.InstanceStatus
		wantClosed bool
	}{
		{"spent repair the pass read", parked(types.InstanceOperationRestart, "restart-3-1", types.InstancePhaseFailed), true},
		{"repair re-armed since: another attempt", parked(types.InstanceOperationRestart, "restart-3-2", types.InstancePhaseFailed), false},
		{"repair re-armed since: in flight", parked(types.InstanceOperationRestart, "restart-3-1", types.InstancePhaseRestarting), false},
		{"parked Create attempt", parked(types.InstanceOperationCreate, "restart-3-1", types.InstancePhaseFailed), false},
		{"already a fresh start", types.InstanceStatus{Index: 3, Incarnation: 2, Phase: types.InstancePhaseFailed, LastFailure: failure}, false},
		{"slot deleted underneath", types.InstanceStatus{Index: 3}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.status
			before := s
			mutate, wrote := recordingMutate(&s)
			closed, err := status.CloseSpentRepair(context.Background(), types.ReconcileInput{MutateInstance: mutate}, 3, "restart-3-1")
			if err != nil {
				t.Fatalf("CloseSpentRepair: %v", err)
			}
			if closed != tc.wantClosed || *wrote != tc.wantClosed {
				t.Fatalf("closed=%v wrote=%v, want %v", closed, *wrote, tc.wantClosed)
			}
			if !tc.wantClosed {
				if s.Operation != before.Operation || s.Phase != before.Phase {
					t.Fatalf("row changed without a close: %+v", s)
				}
				return
			}
			if s.Operation != nil {
				t.Errorf("operation = %+v, want cleared", s.Operation)
			}
			if s.Phase != types.InstancePhaseFailed || s.Incarnation != 2 || s.RunningRevision != "engine-rev" || s.LastFailure != failure {
				t.Errorf("row = %+v, want phase, incarnation, revision and failure record kept", s)
			}
		})
	}
	t.Run("seam error propagates", func(t *testing.T) {
		want := errors.New("status write refused")
		mutate := func(context.Context, int32, func(*types.InstanceStatus) bool) error { return want }
		if _, err := status.CloseSpentRepair(context.Background(), types.ReconcileInput{MutateInstance: mutate}, 3, "restart-3-1"); !errors.Is(err, want) {
			t.Fatalf("err = %v, want %v", err, want)
		}
	})
}

// parkedUpdateAttempt is an Update attempt parked after its disposition.
func parkedUpdateAttempt() *types.InstanceOperation {
	parked := parkedOperation(types.InstanceOperationUpdate)
	parked.Step = types.UpdateStepParked
	parked.Waiting = string(types.RolloutHoldGateRetryBlock)
	return parked
}

// A parked Update attempt is the reset's to clear: it is a spent attempt
// like a parked repair, not a continuation the rollout machinery owns.
func TestResetOwnsOperation_ParkedUpdateAttempt(t *testing.T) {
	parked := parkedUpdateAttempt()
	if !status.ResetOwnsOperation(parked) {
		t.Fatalf("a parked Update attempt must be reset-owned")
	}
}

// A recreate the gang verdict ended keeps an Update continuation no pass
// re-drives at its pinned revision, so the reset owns it as it owns a
// parked attempt; any other Update continuation stays the rollout's.
func TestResetOwnsRow_RecreateEndedByTheGangVerdict(t *testing.T) {
	drain := &types.InstanceOperation{ID: "update-3-1", Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "rev-b"}
	verdict := &types.InstanceTermination{Reason: types.PodGroupOwnershipConflictReason, Message: "PodGroup engine-3 is controlled by StatefulSet/other-owner, not by this owner"}
	ended := types.InstanceStatus{Index: 3, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a", Operation: drain, LastFailure: verdict, Incarnation: 2}
	if !status.ResetOwnsRow(&ended) {
		t.Fatalf("a recreate the gang verdict ended must be reset-owned")
	}
	deadline := ended
	deadline.LastFailure = &types.InstanceTermination{Reason: "DeadlineExceeded"}
	if status.ResetOwnsRow(&deadline) {
		t.Errorf("an Update continuation ended any other way stays the rollout's")
	}

	store := map[int32]*types.InstanceStatus{3: &ended}
	mutate := func(_ context.Context, idx int32, fn func(*types.InstanceStatus) bool) error {
		fn(store[idx])
		return nil
	}
	cleared, err := status.ClearFailedInstanceOperation(context.Background(), mutate, 3)
	if err != nil || !cleared {
		t.Fatalf("ClearFailedInstanceOperation: cleared=%v err=%v, want the kept recreate cleared", cleared, err)
	}
	if got := store[3]; got.Operation != nil || got.Phase != types.InstancePhaseFailed || got.LastFailure != verdict || got.RunningRevision != "rev-a" || got.Incarnation != 2 {
		t.Errorf("row after the reset = %+v, want Failed with no operation and everything else kept", got)
	}
}
