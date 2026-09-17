package workload_test

// ClearFailedInstanceOperation: the operator-reset release of a parked
// attempt clears only a repair-owned Operation (Create / Restart) of a
// Failed Instance and writes nothing for every other shape.

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
)

// recordingMutate is a MutateInstance seam over one in-memory status:
// it applies the callback and records whether a write was requested.
func recordingMutate(s *workload.InstanceStatus) (func(context.Context, int32, func(*workload.InstanceStatus) bool) error, *bool) {
	wrote := false
	return func(_ context.Context, _ int32, mutate func(*workload.InstanceStatus) bool) error {
		wrote = mutate(s)
		return nil
	}, &wrote
}

func parkedOperation(typ workload.InstanceOperationType) *workload.InstanceOperation {
	now := metav1.Now()
	return &workload.InstanceOperation{ID: "op-3-1", Type: typ, Step: "WaitReady", StartedAt: now, Deadline: now}
}

func TestResetOwnsOperation(t *testing.T) {
	if !workload.ResetOwnsOperation(nil) {
		t.Fatalf("no Operation must be reset-owned")
	}
	owned := []workload.InstanceOperationType{workload.InstanceOperationCreate, workload.InstanceOperationRestart}
	for _, typ := range owned {
		if !workload.ResetOwnsOperation(parkedOperation(typ)) {
			t.Errorf("%s must be reset-owned", typ)
		}
	}
	foreign := []workload.InstanceOperationType{workload.InstanceOperationUpdate, workload.InstanceOperationMigrate, workload.InstanceOperationDelete}
	for _, typ := range foreign {
		if workload.ResetOwnsOperation(parkedOperation(typ)) {
			t.Errorf("%s must NOT be reset-owned", typ)
		}
	}
}

func TestClearFailedInstanceOperation(t *testing.T) {
	now := metav1.Now()
	failure := &workload.InstanceTermination{PodName: "engine-3-0", Reason: "OOMKilled", Time: now}

	cases := []struct {
		name        string
		status      workload.InstanceStatus
		wantCleared bool
	}{
		{
			name:        "failed with preserved Restart clears it",
			status:      workload.InstanceStatus{Index: 3, Phase: workload.InstancePhaseFailed, Operation: parkedOperation(workload.InstanceOperationRestart), LastFailure: failure, Incarnation: 2},
			wantCleared: true,
		},
		{
			name:        "failed with preserved Create clears it",
			status:      workload.InstanceStatus{Index: 3, Phase: workload.InstancePhaseFailed, Operation: parkedOperation(workload.InstanceOperationCreate), LastFailure: failure, Incarnation: 1},
			wantCleared: true,
		},
		{
			name:   "failed with preserved Update is refused",
			status: workload.InstanceStatus{Index: 3, Phase: workload.InstancePhaseFailed, Operation: parkedOperation(workload.InstanceOperationUpdate), LastFailure: failure},
		},
		{
			name:   "failed with preserved Migrate is refused",
			status: workload.InstanceStatus{Index: 3, Phase: workload.InstancePhaseFailed, Operation: parkedOperation(workload.InstanceOperationMigrate)},
		},
		{
			name:   "failed with preserved Delete is refused",
			status: workload.InstanceStatus{Index: 3, Phase: workload.InstancePhaseFailed, Operation: parkedOperation(workload.InstanceOperationDelete)},
		},
		{
			name:   "failed without operation writes nothing",
			status: workload.InstanceStatus{Index: 3, Phase: workload.InstancePhaseFailed, LastFailure: failure},
		},
		{
			name:   "non-failed with operation writes nothing",
			status: workload.InstanceStatus{Index: 3, Phase: workload.InstancePhaseRestarting, Operation: parkedOperation(workload.InstanceOperationRestart)},
		},
		{
			name:   "fresh-empty slot writes nothing",
			status: workload.InstanceStatus{Index: 3},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.status
			before := s
			mutate, wrote := recordingMutate(&s)

			cleared, err := workload.ClearFailedInstanceOperation(context.Background(), mutate, 3)
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
			if s.Phase != workload.InstancePhaseFailed {
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
	mutate := func(context.Context, int32, func(*workload.InstanceStatus) bool) error { return boom }

	cleared, err := workload.ClearFailedInstanceOperation(context.Background(), mutate, 3)
	if !errors.Is(err, boom) {
		t.Fatalf("seam error must propagate, got %v", err)
	}
	if cleared {
		t.Fatalf("a failed write must not report cleared")
	}
}
