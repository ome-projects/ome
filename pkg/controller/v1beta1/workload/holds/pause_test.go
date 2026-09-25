package holds

import (
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestPause_RecordsAndReleasesTheHold: the pause authority records the
// token on every attempt a paused Component is actually holding — and on
// no other row — then releases exactly that token once the pause is
// lifted. Which owners it holds is the whole rule: scale-down runs while
// paused, a committed create finishes the set it started, and repair
// runs under a standard pause but not a frozen one.
func TestPause_RecordsAndReleasesTheHold(t *testing.T) {
	build := func(rows ...types.InstanceStatus) (*rowStore, types.ReconcileInput) {
		return newStore(append(rows,
			types.InstanceStatus{Index: 1, Incarnation: 1, Phase: types.InstancePhaseDeleting,
				Operation: operation("delete-1", types.InstanceOperationDelete, "Drain", "")},
			types.InstanceStatus{Index: 2, Incarnation: 1, Phase: types.InstancePhaseReady},
			types.InstanceStatus{Index: 3, Incarnation: 1, Phase: types.InstancePhaseCreating,
				Operation: operation("create-3", types.InstanceOperationCreate, "CreatePods", "")},
			types.InstanceStatus{Index: 4, Incarnation: 2, Phase: types.InstancePhaseRestarting,
				Operation: operation("restart-4", types.InstanceOperationRestart, "Drain", "")},
		)...)
	}
	// held is the row a pause stands in front of: an update in flight.
	held := func(waiting string) types.InstanceStatus {
		return types.InstanceStatus{Index: 0, Incarnation: 1, Phase: types.InstancePhaseUpdating,
			Operation: operation("update-0", types.InstanceOperationUpdate, "InPlace", waiting)}
	}

	t.Run("paused records the hold", func(t *testing.T) {
		store, in := build(held(""))
		apply(t, in, types.ComponentPlan{Paused: true}, nil)
		if got := store.waiting(0); got != types.WaitingReasonPaused {
			t.Errorf("held attempt waiting = %q, want %q", got, types.WaitingReasonPaused)
		}
		if got := store.waiting(1); got != "" {
			t.Errorf("Delete attempt waiting = %q, want none (scale-down runs while paused)", got)
		}
		if got := store.waiting(2); got != "" {
			t.Errorf("idle row waiting = %q, want untouched", got)
		}
		if got := store.waiting(3); got != "" {
			t.Errorf("Create attempt waiting = %q, want none (it finishes the set it started)", got)
		}
		if got := store.waiting(4); got != "" {
			t.Errorf("Restart attempt waiting = %q, want none (repair runs under a standard pause)", got)
		}
	})

	t.Run("a frozen pause holds the repair too", func(t *testing.T) {
		store, in := build(held(""))
		apply(t, in, types.ComponentPlan{Paused: true, PauseFreeze: true}, nil)
		if got := store.waiting(4); got != types.WaitingReasonPaused {
			t.Errorf("frozen Restart attempt waiting = %q, want %q", got, types.WaitingReasonPaused)
		}
		if got := store.waiting(3); got != "" {
			t.Errorf("Create attempt under freeze waiting = %q, want none", got)
		}
	})

	t.Run("another authority keeps the row", func(t *testing.T) {
		store, in := build(refused(held(types.RejectionReasonQuotaExceeded)))
		apply(t, in, types.ComponentPlan{Paused: true}, nil)
		if got := store.waiting(0); got != types.RejectionReasonQuotaExceeded {
			t.Errorf("quota-held attempt waiting = %q, want the quota token kept", got)
		}
	})

	t.Run("unpause releases the hold", func(t *testing.T) {
		store, in := build(held(types.WaitingReasonPaused))
		apply(t, in, types.ComponentPlan{}, nil)
		if got := store.waiting(0); got != "" {
			t.Errorf("released attempt waiting = %q, want the pause token cleared", got)
		}
	})

	t.Run("a pause the owner does not hold keeps the token it left", func(t *testing.T) {
		store, in := newStore(
			types.InstanceStatus{Index: 0, Incarnation: 1, Phase: types.InstancePhaseCreating,
				Operation: operation("create-0", types.InstanceOperationCreate, "CreatePods", types.WaitingReasonPaused)},
		)
		apply(t, in, types.ComponentPlan{Paused: true}, nil)
		if got := store.waiting(0); got != types.WaitingReasonPaused {
			t.Errorf("waiting = %q, want the episode's token kept while the pause is in force", got)
		}
	})
}

// TestPauseHeldOperation_Owners pins the owner column of the pause's
// table row on its own, so a change to which attempts a pause stands in
// front of fails here rather than only through a pass.
func TestPauseHeldOperation_Owners(t *testing.T) {
	for _, tc := range []struct {
		owner  types.RowOwner
		freeze bool
		want   bool
	}{
		{owner: types.OwnerNone, want: false},
		{owner: types.OwnerDelete, want: false},
		{owner: types.OwnerCreate, want: false},
		{owner: types.OwnerRestart, want: false},
		{owner: types.OwnerRestart, freeze: true, want: true},
		{owner: types.OwnerUpdate, want: true},
		{owner: types.OwnerMigrate, want: true},
	} {
		got := pauseHeldOperation(tc.owner, types.ComponentPlan{Paused: true, PauseFreeze: tc.freeze})
		if got != tc.want {
			t.Errorf("pauseHeldOperation(%s, freeze=%v) = %v, want %v", tc.owner, tc.freeze, got, tc.want)
		}
	}
}
