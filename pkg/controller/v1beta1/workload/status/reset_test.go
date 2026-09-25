package status_test

// ClearFailedInstanceOperation: the operator-reset release of a parked
// attempt clears only a repair-owned Operation (Create / Restart) of a
// Failed Instance and writes nothing for every other shape.

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
				t.Fatalf("Phase must stay Failed, got %q", s.Phase)
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
