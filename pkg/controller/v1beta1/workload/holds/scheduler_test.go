package holds

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const schedulerMessage = "0/3 nodes are available: 3 Insufficient nvidia.com/gpu"

// creatingRow is a single-pod Create attempt — the shape a first rollout
// presents while its pod waits for placement.
func creatingRow(waiting string) types.InstanceStatus {
	return types.InstanceStatus{
		Index:     0,
		Phase:     types.InstancePhaseCreating,
		PodCount:  1,
		Operation: operation("create-0", types.InstanceOperationCreate, "CreatePods", waiting),
	}
}

// TestScheduler_RecordsAndReleasesTheHold: a pod with no placement is
// queued on cluster capacity, so the hold is recorded with the
// scheduler's own message on LastFailure and released the moment the pod
// is placed. The row is never failed here — only the operator's grace
// does that, in the escalation pass.
func TestScheduler_RecordsAndReleasesTheHold(t *testing.T) {
	since := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	pod := unschedulablePod("svc-a-engine-0-default-0", schedulerMessage, since)

	t.Run("records the hold with the scheduler's message", func(t *testing.T) {
		store, in := newStore(creatingRow(""))
		apply(t, in, types.ComponentPlan{}, map[int32][]*corev1.Pod{0: {pod}})
		if got := store.waiting(0); got != types.WaitingReasonUnschedulable {
			t.Errorf("waiting = %q, want %q", got, types.WaitingReasonUnschedulable)
		}
		lf := store.lastFailure(0)
		if lf == nil || lf.Message != schedulerMessage || lf.PodName != pod.Name {
			t.Errorf("lastFailure = %+v, want the scheduler's message on the unplaceable pod", lf)
		}
		if lf != nil && !lf.Time.Equal(&since) {
			t.Errorf("lastFailure.Time = %v, want the condition transition %v", lf.Time, since)
		}
	})

	t.Run("released once the pod is placed", func(t *testing.T) {
		store, in := newStore(creatingRow(types.WaitingReasonUnschedulable))
		placed := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: "ns"},
			Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			}},
		}
		apply(t, in, types.ComponentPlan{}, map[int32][]*corev1.Pod{0: {placed}})
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the token released", got)
		}
	})

	t.Run("released once the pod is gone", func(t *testing.T) {
		store, in := newStore(creatingRow(types.WaitingReasonUnschedulable))
		apply(t, in, types.ComponentPlan{}, nil)
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want the token released", got)
		}
	})

	t.Run("edge-triggered across passes", func(t *testing.T) {
		store, in := newStore(creatingRow(""))
		pods := map[int32][]*corev1.Pod{0: {pod}}
		apply(t, in, types.ComponentPlan{}, pods)
		// Re-observe what the pass wrote, the way the next reconcile
		// does, and run twice more against an unchanged cluster.
		store.writes = 0
		apply(t, store.input(), types.ComponentPlan{}, pods)
		apply(t, store.input(), types.ComponentPlan{}, pods)
		if store.writes != 0 {
			t.Errorf("status writes on an unchanged hold = %d, want 0", store.writes)
		}
	})

	t.Run("a teardown row is left to DeleteBatch", func(t *testing.T) {
		row := creatingRow("")
		row.Phase = types.InstancePhaseDeleting
		row.Operation = operation("delete-0", types.InstanceOperationDelete, "Drain", "")
		store, in := newStore(row)
		apply(t, in, types.ComponentPlan{}, map[int32][]*corev1.Pod{0: {pod}})
		if got := store.waiting(0); got != "" {
			t.Errorf("waiting = %q, want none on a row DeleteBatch owns", got)
		}
	})

	t.Run("another authority keeps the row", func(t *testing.T) {
		store, in := newStore(refused(creatingRow(types.RejectionReasonQuotaExceeded)))
		apply(t, in, types.ComponentPlan{}, map[int32][]*corev1.Pod{0: {pod}})
		if got := store.waiting(0); got != types.RejectionReasonQuotaExceeded {
			t.Errorf("waiting = %q, want the incumbent token kept", got)
		}
	})
}
