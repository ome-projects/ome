package holds

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// retiredFixture is one observed row and a row writer that applies the
// callback to it, counting the writes that changed it.
func retiredFixture(row types.InstanceStatus) (types.ReconcileInput, *types.InstanceStatus, *int) {
	statuses := []types.InstanceStatus{row}
	shared := &statuses[0]
	writes := 0
	return types.ReconcileInput{
		ObservedState: types.WorkloadObservedState{InstanceStatuses: statuses},
		MutateInstance: func(_ context.Context, _ int32, mutate func(*types.InstanceStatus) bool) error {
			if mutate(shared) {
				writes++
			}
			return nil
		},
	}, shared, &writes
}

// TestRecordRetiredReplacementTerminating_NamesTheWaitOnBothRowsOfThePair:
// the token lands on the source holding its surge, Failed or not, and on
// the replacement at its cleanup step; the failure record that retired
// the replacement stays; a row already reporting the wait is not written
// again.
func TestRecordRetiredReplacementTerminating_NamesTheWaitOnBothRowsOfThePair(t *testing.T) {
	failure := &types.InstanceTermination{Reason: "ContainersNotReady", Message: "the replacement gang never became ready"}
	rows := map[string]types.InstanceStatus{
		"the Failed source with its surge kept": {Index: 0, Phase: types.InstancePhaseFailed, LastFailure: failure,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge}},
		"the source still Updating": {Index: 0, Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge}},
		"the replacement at its cleanup step": {Index: 1, Phase: types.InstancePhaseCreating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTargetCleanup}},
	}
	for name, row := range rows {
		t.Run(name, func(t *testing.T) {
			input, shared, writes := retiredFixture(row)
			if err := RecordRetiredReplacementTerminating(context.Background(), input, types.ComponentPlan{}, row.Index); err != nil {
				t.Fatalf("record: %v", err)
			}
			if shared.Operation.Waiting != types.WaitingReasonRetiredReplacementTerminating {
				t.Fatalf("waiting=%q, want %s", shared.Operation.Waiting, types.WaitingReasonRetiredReplacementTerminating)
			}
			if shared.LastFailure != row.LastFailure {
				t.Fatalf("the failure record moved: %+v, want %+v", shared.LastFailure, row.LastFailure)
			}
			if shared.Phase != row.Phase {
				t.Fatalf("phase %s, want %s untouched", shared.Phase, row.Phase)
			}
			if err := RecordRetiredReplacementTerminating(context.Background(), input, types.ComponentPlan{}, row.Index); err != nil {
				t.Fatalf("record again: %v", err)
			}
			if *writes != 1 {
				t.Fatalf("writes=%d, want 1: re-observing the wait writes nothing", *writes)
			}
		})
	}
}

// TestRecordRetiredReplacementTerminating_OnlyAnUpdateRowCarriesIt: a
// teardown, a create and a row with no operation are refused, the token
// takes a row from the pause, and another fact already on the row keeps
// it.
func TestRecordRetiredReplacementTerminating_OnlyAnUpdateRowCarriesIt(t *testing.T) {
	cases := map[string]struct {
		row  types.InstanceStatus
		want string
	}{
		"a teardown": {row: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseDeleting,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationDelete}}, want: ""},
		"a create": {row: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseCreating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationCreate}}, want: ""},
		"no operation": {row: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}, want: ""},
		"a paused surge": {row: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, Waiting: types.WaitingReasonPaused}},
			want: types.WaitingReasonRetiredReplacementTerminating},
		"a surge the scheduler holds": {row: types.InstanceStatus{Index: 0, Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, Waiting: types.WaitingReasonUnschedulable}},
			want: types.WaitingReasonUnschedulable},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			input, shared, _ := retiredFixture(tc.row)
			if err := RecordRetiredReplacementTerminating(context.Background(), input, types.ComponentPlan{}, 0); err != nil {
				t.Fatalf("record: %v", err)
			}
			got := ""
			if shared.Operation != nil {
				got = shared.Operation.Waiting
			}
			if got != tc.want {
				t.Fatalf("waiting=%q, want %q", got, tc.want)
			}
		})
	}
}

// TestRetiredReplacementHold_ThePassReleasesItUnderAPauseOnceNoMemberStands:
// the pass keeps the recorded wait while a retired member stands, paused or
// not, and without a pause the pair's end is what takes it away; under a
// pause that end does not run, so the pass releases the token once no
// member of the retired index stands, and the pause reports the rows.
func TestRetiredReplacementHold_ThePassReleasesItUnderAPauseOnceNoMemberStands(t *testing.T) {
	const surgeIdx = int32(1)
	member := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "retired-leader"}}
	cases := map[string]struct {
		paused  bool
		members []*corev1.Pod
		want    string
	}{
		"paused, a member stands":  {paused: true, members: []*corev1.Pod{member}, want: types.WaitingReasonRetiredReplacementTerminating},
		"paused, no member stands": {paused: true, want: types.WaitingReasonPaused},
		"not paused, no member":    {paused: false, want: types.WaitingReasonRetiredReplacementTerminating},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			statuses := []types.InstanceStatus{
				{Index: 0, Phase: types.InstancePhaseUpdating, Operation: &types.InstanceOperation{
					Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, SurgeIndex: ptr.To(surgeIdx),
					Waiting: types.WaitingReasonRetiredReplacementTerminating}},
				{Index: surgeIdx, Phase: types.InstancePhaseCreating, Operation: &types.InstanceOperation{
					Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTargetCleanup,
					Waiting: types.WaitingReasonRetiredReplacementTerminating}},
			}
			in := types.ReconcileInput{
				ObservedState: types.WorkloadObservedState{InstanceStatuses: statuses},
				MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
					for i := range statuses {
						if statuses[i].Index == idx {
							mutate(&statuses[i])
						}
					}
					return nil
				},
			}
			plan := types.ComponentPlan{Paused: tc.paused, UpdateStrategy: types.UpdateStrategy{Type: types.UpdateStrategySurgeThenDrain}}
			rows := []Row{{Status: &statuses[0], Desired: 2}, {Status: &statuses[1], Desired: 2}}
			if _, err := Run(context.Background(), PassInput{
				Input: in, Plan: plan, Rows: rows,
				Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
					return map[int32][]*corev1.Pod{surgeIdx: tc.members}, nil
				},
			}); err != nil {
				t.Fatalf("Run: %v", err)
			}
			for _, s := range statuses {
				if s.Operation.Waiting != tc.want {
					t.Errorf("row %d waiting=%q, want %q", s.Index, s.Operation.Waiting, tc.want)
				}
			}
		})
	}
}
