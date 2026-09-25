package holds

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// gangInput returns the pass input for a row whose PodGroup reports
// state, with the message an operator would read.
func gangInput(in types.ReconcileInput, idx int32, state types.GangState, message string) types.ReconcileInput {
	gangs := types.NewGangObservations()
	gangs.Record(idx, types.GangObservation{Name: "svc-a-engine-0", State: state, Message: message})
	in.Gangs = gangs
	return in
}

// TestGang_RecordsAndReleasesTheNameWait: a deterministic PodGroup name
// still held by an object being collected parks the row. The wait
// resolves itself, so the arm records it, leaves the group's own
// explanation on LastFailure, and releases the token once the name is
// usable again. A group the scheduler FAILED is not a wait at all — the
// PodGroup pass rebuilds it.
func TestGang_RecordsAndReleasesTheNameWait(t *testing.T) {
	const message = "podgroup svc-a-engine-0 is terminating"

	t.Run("records the wait", func(t *testing.T) {
		store, in := newStore(creatingRow(""))
		apply(t, gangInput(in, 0, types.GangStateTerminating, message), types.ComponentPlan{}, nil)
		if got := store.waiting(0); got != types.WaitingReasonPodGroupTerminating {
			t.Errorf("waiting = %q, want %q", got, types.WaitingReasonPodGroupTerminating)
		}
		if lf := store.lastFailure(0); lf == nil || lf.Message != message {
			t.Errorf("lastFailure = %+v, want the group's own explanation", lf)
		}
	})

	t.Run("a failed group is not a wait", func(t *testing.T) {
		store, in := newStore(creatingRow(""))
		apply(t, gangInput(in, 0, types.GangStateFailed, message), types.ComponentPlan{}, nil)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none (the PodGroup pass resets a failed group)", got)
		}
	})

	t.Run("released once the name is usable", func(t *testing.T) {
		store, in := newStore(creatingRow(types.WaitingReasonPodGroupTerminating))
		apply(t, gangInput(in, 0, types.GangStateNone, ""), types.ComponentPlan{}, nil)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the token released", got)
		}
	})

	t.Run("a migration row is left to its record", func(t *testing.T) {
		row := creatingRow("")
		row.Phase = types.InstancePhaseMigrating
		row.Operation = operation("migrate-0", types.InstanceOperationMigrate, "CreateSurge", "")
		store, in := newStore(row)
		apply(t, gangInput(in, 0, types.GangStateTerminating, message), types.ComponentPlan{}, nil)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none on a row the migration record owns", got)
		}
	})

	t.Run("a gang-surge source reads no group of its own", func(t *testing.T) {
		surge := int32(1)
		row := creatingRow("")
		row.Phase = types.InstancePhaseUpdating
		row.Operation = operation("update-0", types.InstanceOperationUpdate, "GangSurgeTarget", "")
		row.Operation.SurgeIndex = &surge
		store, in := newStore(row)
		apply(t, gangInput(in, 0, types.GangStateTerminating, message), types.ComponentPlan{}, nil)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none: the attempt lives under the surge index", got)
		}
	})

	t.Run("the scheduler keeps a row it reached first", func(t *testing.T) {
		store, in := newStore(creatingRow(types.WaitingReasonUnschedulable))
		pods := map[int32][]*corev1.Pod{0: {unschedulablePod("svc-a-engine-0-default-0", schedulerMessage, metav1.Now())}}
		apply(t, gangInput(in, 0, types.GangStateTerminating, message), types.ComponentPlan{}, pods)
		if got := store.waiting(0); got != types.WaitingReasonUnschedulable {
			t.Errorf("waiting = %q, want the incumbent token kept", got)
		}
	})
}
