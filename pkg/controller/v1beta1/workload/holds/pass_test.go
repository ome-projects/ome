package holds

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
		0: {unschedulablePod("svc-a-engine-0-default-0", schedulerMessage, metav1.Now())},
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
		0: {unschedulablePod("svc-a-engine-0-default-0", schedulerMessage, metav1.Now())},
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
