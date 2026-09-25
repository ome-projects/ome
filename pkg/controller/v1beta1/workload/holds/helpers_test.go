package holds

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// rowStore is the adapter stand-in the hold pass writes through: the
// persisted rows, kept separate from the pass-start observation so a
// test can tell a write that landed from one the observation predates.
type rowStore struct {
	rows []types.InstanceStatus
	// writes counts the mutations that committed, which is what the
	// edge trigger is measured in.
	writes int
}

// newStore seeds the store and returns the ReconcileInput whose
// observation is a snapshot of those rows at pass start.
func newStore(rows ...types.InstanceStatus) (*rowStore, types.ReconcileInput) {
	store := &rowStore{rows: rows}
	return store, store.input()
}

// input is the next pass's ReconcileInput over the same store: the
// observation is a snapshot of what the previous pass left persisted.
func (s *rowStore) input() types.ReconcileInput {
	observed := make([]types.InstanceStatus, len(s.rows))
	for i := range s.rows {
		observed[i] = copyRow(s.rows[i])
	}
	return types.ReconcileInput{
		Key:            types.Key{Namespace: "ns", OwnerName: "svc-a", Component: types.ComponentEngine},
		ObservedState:  types.WorkloadObservedState{InstanceStatuses: observed},
		MutateInstance: s.mutate,
	}
}

func (s *rowStore) mutate(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
	for i := range s.rows {
		if s.rows[i].Index != idx {
			continue
		}
		row := copyRow(s.rows[i])
		if mutate(&row) {
			s.rows[i] = row
			s.writes++
		}
		return nil
	}
	return nil
}

// copyRow is the value copy the store writes through, so a mutation
// that commits cannot reach back into the pass-start observation.
func copyRow(s types.InstanceStatus) types.InstanceStatus {
	out := s
	if s.Operation != nil {
		op := *s.Operation
		out.Operation = &op
	}
	if s.LastFailure != nil {
		lf := *s.LastFailure
		out.LastFailure = &lf
	}
	return out
}

// waiting returns the row's recorded Waiting token, or "" when the row
// has no operation.
func (s *rowStore) waiting(idx int32) string {
	for i := range s.rows {
		if s.rows[i].Index == idx && s.rows[i].Operation != nil {
			return s.rows[i].Operation.Waiting
		}
	}
	return ""
}

func (s *rowStore) lastFailure(idx int32) *types.InstanceTermination {
	for i := range s.rows {
		if s.rows[i].Index == idx {
			return s.rows[i].LastFailure
		}
	}
	return nil
}

// rowsFor turns the input's observation into the pass's rows, the way
// the dispatcher does.
func rowsFor(in types.ReconcileInput, desired int32) []Row {
	out := make([]Row, 0, len(in.ObservedState.InstanceStatuses))
	for _, s := range in.ObservedState.InstanceStatuses {
		out = append(out, Row{Status: s, Desired: desired})
	}
	return out
}

// apply runs the hold pass over the input with no pods observed.
func apply(t *testing.T, in types.ReconcileInput, plan types.ComponentPlan, pods map[int32][]*corev1.Pod) Result {
	t.Helper()
	res, err := Run(context.Background(), PassInput{
		Deps:  types.Deps{},
		Input: in,
		Plan:  plan,
		Rows:  rowsFor(in, 1),
		Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
			return pods, nil
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return res
}

// operation is a terse in-flight operation of the given owner.
func operation(id string, typ types.InstanceOperationType, step, waiting string) *types.InstanceOperation {
	return &types.InstanceOperation{ID: id, Type: typ, Step: step, Waiting: waiting}
}

// refused stamps the recorded quota refusal the create site leaves
// behind, which is what the quota authority reads and what the deadline
// park is anchored to.
func refused(row types.InstanceStatus) types.InstanceStatus {
	at := metav1.Now()
	row.Operation.CapacityRefusedAt = &at
	return row
}

// unschedulablePod is a live pod the scheduler reports it cannot place.
func unschedulablePod(name, message string, since metav1.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             types.WaitingReasonUnschedulable,
			Message:            message,
			LastTransitionTime: since,
		}}},
	}
}
