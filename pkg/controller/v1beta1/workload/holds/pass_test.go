package holds

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	fixture "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestRun_ReleasesBeforeRecording: a row that stops waiting on one
// authority and starts waiting on another hands over inside one pass.
// Left unheld for even one pass, the deadline-parking step reads the
// empty token as admission and restarts a whole InstanceReadyTimeout.
func TestRun_ReleasesBeforeRecording(t *testing.T) {
	// The row reports a quota refusal that has been retired, while a pod
	// of the same attempt has no placement.
	store, in := newStore(creatingRow(types.RejectionReasonQuotaExceeded))
	pods := map[int32][]*corev1.Pod{
		0: {fixture.UnschedulablePod("svc-a-engine-0-default-0", schedulerMessage, time.Now())},
	}
	apply(t, in, types.ComponentPlan{}, pods)
	if got := store.waiting(0); got != types.WaitingReasonUnschedulable {
		t.Errorf("waiting = %q, want the scheduler to take the row in the same pass", got)
	}
	if store.writes != 2 {
		t.Errorf("writes = %d, want 2 (one release, one record)", store.writes)
	}
}

// TestRun_SkipsRowsNoFactMayTouch: a deferred scale-down victim
// receives no lifecycle mutation before its wave admits it, and a Failed
// row has no attempt left for a fact to hold. The pause reaches both,
// because it releases its own token wherever it left one.
func TestRun_SkipsRowsNoFactMayTouch(t *testing.T) {
	pods := map[int32][]*corev1.Pod{
		0: {fixture.UnschedulablePod("svc-a-engine-0-default-0", schedulerMessage, time.Now())},
	}
	run := func(t *testing.T, row types.InstanceStatus, excluded bool) *rowStore {
		t.Helper()
		store, in := newStore(row)
		rows := rowsFor(in, 1)
		rows[0].Excluded = excluded
		if _, err := Run(context.Background(), PassInput{
			Input: in,
			Plan:  types.ComponentPlan{},
			Rows:  rows,
			Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
				return pods, nil
			},
		}); err != nil {
			t.Fatalf("Run: %v", err)
		}
		return store
	}

	t.Run("a deferred scale-down victim", func(t *testing.T) {
		if got := run(t, creatingRow(""), true).waiting(0); got != "" {
			t.Errorf("waiting = %q, want none before the wave admits the row", got)
		}
	})

	t.Run("a Failed row", func(t *testing.T) {
		row := creatingRow("")
		row.Phase = types.InstancePhaseFailed
		if got := run(t, row, false).waiting(0); got != "" {
			t.Errorf("waiting = %q, want none on a row with no attempt left", got)
		}
	})

	t.Run("a parked attempt in either phase", func(t *testing.T) {
		for _, phase := range []types.InstancePhase{types.InstancePhaseUpdating, types.InstancePhaseFailed} {
			row := types.InstanceStatus{Index: 0, Phase: phase, Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: types.UpdateStepParked, TargetRevision: "rev-b",
				Waiting: string(types.RolloutHoldGateRetryBlock),
			}}
			if got := run(t, row, false).waiting(0); got != string(types.RolloutHoldGateRetryBlock) {
				t.Errorf("%s: waiting = %q, want the parked attempt's own wait left alone", phase, got)
			}
		}
	})

	t.Run("the pause releases on both", func(t *testing.T) {
		row := creatingRow(types.WaitingReasonPaused)
		row.Phase = types.InstancePhaseFailed
		store, in := newStore(row)
		rows := rowsFor(in, 1)
		rows[0].Excluded = true
		if _, err := Run(context.Background(), PassInput{
			Input: in,
			Plan:  types.ComponentPlan{},
			Rows:  rows,
			Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
				return nil, nil
			},
		}); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the pause's own token released", got)
		}
	})
}

// TestRun_NoWaitTouchesNoRow: the common case — every reconcile of
// every healthy row — reaches no mutation at all.
func TestRun_NoWaitTouchesNoRow(t *testing.T) {
	store, in := newStore(creatingRow(""))
	apply(t, in, types.ComponentPlan{}, nil)
	if store.writes != 0 {
		t.Errorf("writes = %d, want 0 on a row nothing is waiting on", store.writes)
	}
}

// TestRun_ARowsReadingNeverReachesTheRowAfterIt: what an authority reads
// on one row is that row's alone. The first row's attempt was refused for
// quota and waits on it; the row after it is one the quota hold never
// reads, a Failed row or a leaving Instance the wave has not admitted, and
// it carries no token after the pass.
func TestRun_ARowsReadingNeverReachesTheRowAfterIt(t *testing.T) {
	cases := map[string]struct {
		phase    types.InstancePhase
		excluded bool
	}{
		"a Failed row": {phase: types.InstancePhaseFailed},
		"a leaving Instance the wave has not admitted": {phase: types.InstancePhaseCreating, excluded: true},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			second := creatingRow("")
			second.Index = 1
			second.Phase = c.phase
			second.Operation = operation("create-1", types.InstanceOperationCreate, "CreatePods", "")
			store, in := newStore(refused(creatingRow("")), second)
			rows := rowsFor(in, 1)
			rows[1].Excluded = c.excluded
			if _, err := Run(context.Background(), PassInput{
				Input: in,
				Plan:  types.ComponentPlan{},
				Rows:  rows,
				Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
					return nil, nil
				},
			}); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got := store.waiting(0); got != types.RejectionReasonQuotaExceeded {
				t.Errorf("waiting(0) = %q, want the quota refusal recorded", got)
			}
			if got := store.waiting(1); got != "" {
				t.Errorf("waiting(1) = %q, want no token on a row the quota hold never read", got)
			}
			if store.writes != 1 {
				t.Errorf("writes = %d, want 1 (the first row's record)", store.writes)
			}
		})
	}
}
