package holds

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	fixture "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/testfixture"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

const (
	gangLeader = "svc-a-engine-0-default-0"
	gangWorker = "svc-a-engine-0-default-1"
)

// boundPod is a running pod placed on node.
func boundPod(name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:       corev1.PodSpec{NodeName: node},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// applyGang runs the hold pass with the row planned as a two-pod gang,
// which is what names the leader and the worker as pods the attempt
// needs.
func applyGang(t *testing.T, store *rowStore, pods map[int32][]*corev1.Pod) {
	t.Helper()
	applyGangUnder(t, store, pods, types.Deps{}, nil)
}

// applyGangUnder is applyGang with a force-delete policy in force and a
// reader the node read can consult.
func applyGangUnder(t *testing.T, store *rowStore, pods map[int32][]*corev1.Pod, deps types.Deps, policy *types.ForceDeletePolicy) {
	t.Helper()
	in := store.input()
	in.ForceDelete = policy
	rows := rowsFor(in, 2)
	for i := range rows {
		rows[i].Instance = &types.InstancePlan{Index: rows[i].Status.Index, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}}
	}
	if _, err := Run(context.Background(), PassInput{
		Deps:  deps,
		Input: in,
		Plan:  types.ComponentPlan{Component: types.ComponentEngine},
		Rows:  rows,
		Pods: func(context.Context) (map[int32][]*corev1.Pod, error) {
			return pods, nil
		},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// placementWaitOf returns the row's recorded placement wait, or nil.
func (s *rowStore) placementWaitOf(idx int32) *metav1.Time {
	for i := range s.rows {
		if s.rows[i].Index == idx && s.rows[i].Operation != nil {
			return s.rows[i].Operation.PlacementWaitSince
		}
	}
	return nil
}

// gangRow is a two-pod gang's attempt in flight.
func gangRow() types.InstanceStatus {
	row := creatingRow("")
	row.PodCount = 2
	return row
}

// TestPlacementWait_GangMemberReplacedAfterNodeLoss_KeepsTheFirstWait: a
// gang's leader waits for placement while the worker's node stops
// reporting. The attempt's placement wait starts at the leader's wait
// and holds while the leader is placed, while the worker's name is freed
// and while the rebuilt worker waits on the scheduler in turn. It is
// retired once both pods run on a node. Recording and retiring it ride on
// the hold's own writes.
func TestPlacementWait_GangMemberReplacedAfterNodeLoss_KeepsTheFirstWait(t *testing.T) {
	leaderSince := time.Now().Add(-18 * time.Minute).Truncate(time.Second)
	store, _ := newStore(gangRow())

	// The leader has no placement and the worker's node stopped reporting.
	applyGang(t, store, map[int32][]*corev1.Pod{0: {
		fixture.UnschedulablePod(gangLeader, schedulerMessage, leaderSince),
		quietPod(gangWorker, "node-a"),
	}})
	got := store.placementWaitOf(0)
	if got == nil || !got.Time.Equal(leaderSince) {
		t.Fatalf("placement wait = %v, want the leader's wait start %v", got, leaderSince)
	}
	if store.writes != 1 {
		t.Errorf("writes = %d, want 1: the start rides on the hold's record", store.writes)
	}

	// The leader is placed; the worker's name is still held.
	applyGang(t, store, map[int32][]*corev1.Pod{0: {boundPod(gangLeader, "node-b"), quietPod(gangWorker, "node-a")}})
	// The held name is freed and the rebuilt worker is not created yet.
	applyGang(t, store, map[int32][]*corev1.Pod{0: {boundPod(gangLeader, "node-b")}})
	if got := store.placementWaitOf(0); got == nil || !got.Time.Equal(leaderSince) {
		t.Fatalf("placement wait = %v, want it kept at %v while the worker is missing", got, leaderSince)
	}

	// The rebuilt worker waits on the scheduler.
	applyGang(t, store, map[int32][]*corev1.Pod{0: {
		boundPod(gangLeader, "node-b"),
		fixture.UnschedulablePod(gangWorker, schedulerMessage, time.Now()),
	}})
	if got := store.placementWaitOf(0); got == nil || !got.Time.Equal(leaderSince) {
		t.Fatalf("placement wait = %v, want it kept at %v: the attempt has waited since then", got, leaderSince)
	}
	if got := store.waiting(0); got != types.WaitingReasonUnschedulable {
		t.Errorf("waiting = %q, want %q", got, types.WaitingReasonUnschedulable)
	}

	// Both pods run on a node.
	writes := store.writes
	applyGang(t, store, map[int32][]*corev1.Pod{0: {boundPod(gangLeader, "node-b"), boundPod(gangWorker, "node-c")}})
	if got := store.placementWaitOf(0); got != nil {
		t.Errorf("placement wait = %v, want it retired once every pod runs on a node", got)
	}
	if store.writes-writes != 1 {
		t.Errorf("writes = %d, want 1: the retirement rides on the hold's release", store.writes-writes)
	}
}

// TestPlacementWait_NameFreedBySweep_KeepsTheStartUntilTheRebuildIsPlaced:
// a gang's leader is placed while the worker's node has stopped reporting.
// In the pass whose force-delete sweep frees the worker's name, the worker
// is still listed and bound, yet it has to be rebuilt: the hold's token is
// released and the attempt keeps its start. The start is retired only once
// the rebuilt worker runs on a node.
func TestPlacementWait_NameFreedBySweep_KeepsTheStartUntilTheRebuildIsPlaced(t *testing.T) {
	since := metav1.NewTime(time.Now().Add(-14 * time.Minute).Truncate(time.Second))
	row := gangRow()
	row.Operation.Waiting = types.WaitingReasonNodeUnknown
	row.Operation.PlacementWaitSince = &since
	store, _ := newStore(row)
	policy := &types.ForceDeletePolicy{OverdueSlack: time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	// No Node object stands behind node-gone, so the sweep frees the
	// worker's name in this pass.
	deps := types.Deps{Client: fakeclient.New(t)}

	applyGangUnder(t, store, map[int32][]*corev1.Pod{0: {boundPod(gangLeader, "node-b"), quietPod(gangWorker, "node-gone")}}, deps, policy)
	if got := store.waiting(0); got != "" {
		t.Errorf("waiting = %q, want the token released: the sweep frees the name", got)
	}
	if got := store.placementWaitOf(0); got == nil || !got.Equal(&since) {
		t.Fatalf("placement wait = %v, want it kept at %v in the pass that frees the name", got, since)
	}

	// The worker is gone and its rebuild is not created yet.
	applyGangUnder(t, store, map[int32][]*corev1.Pod{0: {boundPod(gangLeader, "node-b")}}, deps, policy)
	if got := store.placementWaitOf(0); got == nil || !got.Equal(&since) {
		t.Fatalf("placement wait = %v, want it kept at %v while the worker is missing", got, since)
	}

	// The rebuilt worker runs on a node.
	applyGangUnder(t, store, map[int32][]*corev1.Pod{0: {boundPod(gangLeader, "node-b"), boundPod(gangWorker, "node-c")}}, deps, policy)
	if got := store.placementWaitOf(0); got != nil {
		t.Errorf("placement wait = %v, want it retired once the rebuilt worker runs on a node", got)
	}
}

// TestPlacementWait_SchedulerWaitAlone_RecordsNothing: with no name held
// by a node that stopped reporting, a pod waiting for placement is
// measured from its own PodScheduled transition, so the operation records
// no placement wait.
func TestPlacementWait_SchedulerWaitAlone_RecordsNothing(t *testing.T) {
	store, _ := newStore(gangRow())
	applyGang(t, store, map[int32][]*corev1.Pod{0: {
		boundPod(gangLeader, "node-b"),
		fixture.UnschedulablePod(gangWorker, schedulerMessage, time.Now().Add(-time.Minute)),
	}})
	if got := store.placementWaitOf(0); got != nil {
		t.Errorf("placement wait = %v, want none for a scheduler wait alone", got)
	}
	if got := store.waiting(0); got != types.WaitingReasonUnschedulable {
		t.Errorf("waiting = %q, want %q", got, types.WaitingReasonUnschedulable)
	}
}

// TestPlacementWait_HeldNameAlone_StartsNow: a held name carries no start
// of its own (the held pod may have been created days ago), so the wait
// starts when the hold pass first sees it.
func TestPlacementWait_HeldNameAlone_StartsNow(t *testing.T) {
	store, _ := newStore(gangRow())
	worker := quietPod(gangWorker, "node-a")
	worker.CreationTimestamp = metav1.NewTime(time.Now().Add(-72 * time.Hour))
	before := time.Now().Add(-time.Second)
	applyGang(t, store, map[int32][]*corev1.Pod{0: {boundPod(gangLeader, "node-b"), worker}})
	got := store.placementWaitOf(0)
	if got == nil || got.Time.Before(before) {
		t.Errorf("placement wait = %v, want the moment the held name was first seen", got)
	}
}

// TestPlacementWait_FailedRow_RecordsNothing: an attempt that ended
// waits for nothing, so a held name on a Failed row records no start; the
// attempt after it starts its own wait.
func TestPlacementWait_FailedRow_RecordsNothing(t *testing.T) {
	row := gangRow()
	row.Phase = types.InstancePhaseFailed
	store, _ := newStore(row)
	applyGang(t, store, map[int32][]*corev1.Pod{0: {
		fixture.UnschedulablePod(gangLeader, schedulerMessage, time.Now()),
		quietPod(gangWorker, "node-a"),
	}})
	if got := store.placementWaitOf(0); got != nil {
		t.Errorf("placement wait = %v, want none on a Failed row", got)
	}
}

// TestPlacementWait_TeardownRow_RecordsNothing: a teardown is
// DeleteBatch's, so neither placement hold nor its start is recorded on
// it.
func TestPlacementWait_TeardownRow_RecordsNothing(t *testing.T) {
	row := gangRow()
	row.Phase = types.InstancePhaseDeleting
	row.Operation = operation("delete-0", types.InstanceOperationDelete, "Drain", "")
	store, _ := newStore(row)
	applyGang(t, store, map[int32][]*corev1.Pod{0: {
		fixture.UnschedulablePod(gangLeader, schedulerMessage, time.Now()),
		quietPod(gangWorker, "node-a"),
	}})
	if got := store.placementWaitOf(0); got != nil {
		t.Errorf("placement wait = %v, want none on a teardown", got)
	}
	if store.writes != 0 {
		t.Errorf("writes = %d, want 0", store.writes)
	}
}

// readyClearedPod is a running pod whose PodReady the control plane set
// False while its own last report still says Ready: ContainersReady and
// the serving gate True. A node that stops reporting leaves this shape,
// and so does a kubelet that lags behind a readiness-gate write.
func readyClearedPod(name, node string) *corev1.Pod {
	pod := boundPod(name, node)
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: query.ServingConditionType}}
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}
	return pod
}

// gangRowSince is gangRow with the attempt's placement wait recorded at
// since and waiting as the row's token.
func gangRowSince(since metav1.Time, waiting string) types.InstanceStatus {
	row := gangRow()
	row.Operation.Waiting = waiting
	row.Operation.PlacementWaitSince = &since
	return row
}

// TestPlacementWait_ReadyClearedOnReadyNode_CountsAsPlaced: a worker whose
// PodReady is False while its node posts Ready is the kubelet's lag behind
// a readiness-gate write, not a name held by a node that stopped
// reporting. It counts as placed, so with the leader placed too the start
// is retired.
func TestPlacementWait_ReadyClearedOnReadyNode_CountsAsPlaced(t *testing.T) {
	since := metav1.NewTime(time.Now().Add(-10 * time.Minute).Truncate(time.Second))
	store, _ := newStore(gangRowSince(since, ""))
	policy := &types.ForceDeletePolicy{OverdueSlack: time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	deps := nodeReader(t, "node-a", corev1.ConditionTrue, time.Hour)

	applyGangUnder(t, store, map[int32][]*corev1.Pod{0: {boundPod(gangLeader, "node-b"), readyClearedPod(gangWorker, "node-a")}}, deps, policy)
	if got := store.placementWaitOf(0); got != nil {
		t.Errorf("placement wait = %v, want it retired: the worker's node posts Ready", got)
	}
}

// TestPlacementWait_ReadyClearedNameFreedBySweep_KeepsTheStart: a worker
// whose PodReady is False on a node unreachable past the threshold is
// freed by the force-delete sweep in this pass. It carries no deletion
// timestamp and is still listed as bound, yet its member has to be
// rebuilt, so it does not count as placed: the token is released and the
// attempt keeps its start.
func TestPlacementWait_ReadyClearedNameFreedBySweep_KeepsTheStart(t *testing.T) {
	since := metav1.NewTime(time.Now().Add(-10 * time.Minute).Truncate(time.Second))
	store, _ := newStore(gangRowSince(since, types.WaitingReasonNodeUnknown))
	policy := &types.ForceDeletePolicy{OverdueSlack: time.Minute, NodeUnreachableThreshold: 5 * time.Minute}
	deps := nodeReader(t, "node-a", corev1.ConditionUnknown, 10*time.Minute)

	applyGangUnder(t, store, map[int32][]*corev1.Pod{0: {boundPod(gangLeader, "node-b"), readyClearedPod(gangWorker, "node-a")}}, deps, policy)
	if got := store.waiting(0); got != "" {
		t.Errorf("waiting = %q, want the token released: the sweep frees the name", got)
	}
	if got := store.placementWaitOf(0); got == nil || !got.Equal(&since) {
		t.Errorf("placement wait = %v, want it kept at %v in the pass that frees the name", got, since)
	}
}

// endedDrainPair is a gang surge whose replacement gang, at index 2, has
// taken over serving and whose source, at index 0, reads Failed at the
// drain step: the ending was written on the source alone, and the
// replacement's row stays live with op as its operation.
func endedDrainPair(op types.InstanceOperation, lastFailure *types.InstanceTermination) []types.InstanceStatus {
	surgeIdx := int32(2)
	op.ID, op.Type, op.Step = "update-2", types.InstanceOperationUpdate, types.UpdateStepGangSurgeTarget
	return []types.InstanceStatus{
		{
			Index: 0, Phase: types.InstancePhaseFailed, PodCount: 2,
			Operation: &types.InstanceOperation{
				ID: "update-0", Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain, SurgeIndex: &surgeIdx,
			},
		},
		{Index: 2, Phase: types.InstancePhaseCreating, PodCount: 2, Operation: &op, LastFailure: lastFailure},
	}
}

// TestPlacementWait_ReplacementOfFailedDrainSource_RecordsNothing: the
// placement grace ended the attempt on the source, which reads Failed at
// the drain step, while the replacement's row stays live with its hold.
// The replacement's leader is on a node that stopped reporting, so its
// name is held, and the refused worker stays until the name is freed. No
// start is recorded while the source reads Failed, so passes over the
// stopped pair write nothing.
func TestPlacementWait_ReplacementOfFailedDrainSource_RecordsNothing(t *testing.T) {
	workerSince := time.Now().Add(-20 * time.Minute).Truncate(time.Second)
	worker := fixture.UnschedulablePod("svc-a-engine-2-default-1", schedulerMessage, workerSince)
	store, _ := newStore(endedDrainPair(types.InstanceOperation{Waiting: types.WaitingReasonUnschedulable},
		types.UnschedulableTermination(worker, schedulerMessage, metav1.NewTime(workerSince)))...)
	pods := map[int32][]*corev1.Pod{2: {quietPod("svc-a-engine-2-default-0", "node-a"), worker}}

	for pass := 1; pass <= 2; pass++ {
		applyGang(t, store, pods)
		if got := store.placementWaitOf(2); got != nil {
			t.Fatalf("pass %d: placement wait = %v, want none while the source reads Failed", pass, got)
		}
	}
	if store.writes != 0 {
		t.Errorf("writes = %d, want 0 over two passes of a pair where nothing changes", store.writes)
	}
}

// TestPlacementWait_ReplacementOfLiveSurgeSource_RecordsTheStart: while
// the source is live, at the Surge step or still Updating at the drain
// step, the pair's attempt is in flight. A replacement member's name held
// by a node that stopped reporting records the attempt's start on the
// replacement's row, as on any other row. The row records no start only
// once its draining source reads Failed.
func TestPlacementWait_ReplacementOfLiveSurgeSource_RecordsTheStart(t *testing.T) {
	for _, step := range []string{types.UpdateStepSurge, types.UpdateStepSurgeDrain} {
		t.Run(step, func(t *testing.T) {
			rows := endedDrainPair(types.InstanceOperation{}, nil)
			rows[0].Phase = types.InstancePhaseUpdating
			rows[0].Operation.Step = step
			store, _ := newStore(rows...)
			before := time.Now().Add(-time.Second)

			applyGang(t, store, map[int32][]*corev1.Pod{2: {
				boundPod("svc-a-engine-2-default-0", "node-b"),
				quietPod("svc-a-engine-2-default-1", "node-a"),
			}})
			got := store.placementWaitOf(2)
			if got == nil || got.Time.Before(before) {
				t.Errorf("placement wait = %v, want the moment the held name was first seen while the source is live", got)
			}
			if got := store.waiting(2); got != types.WaitingReasonNodeUnknown {
				t.Errorf("waiting = %q, want %q", got, types.WaitingReasonNodeUnknown)
			}
		})
	}
}

// TestPlacementWait_ReplacementOfFailedDrainSource_RetiresTheStart: the
// replacement's worker was on a node that stopped reporting, so the
// replacement's row recorded a start. The sweep freed the name and the
// loss ended the attempt on the source, which reads Failed at the drain
// step. The start belongs to the attempt that ended: it is retired before
// the source opens the next attempt, so the rebuilt worker waits on its
// own clock.
func TestPlacementWait_ReplacementOfFailedDrainSource_RetiresTheStart(t *testing.T) {
	since := metav1.NewTime(time.Now().Add(-6 * time.Minute).Truncate(time.Second))
	store, _ := newStore(endedDrainPair(types.InstanceOperation{PlacementWaitSince: &since}, nil)...)

	// The leader is placed; the freed worker is not rebuilt yet.
	applyGang(t, store, map[int32][]*corev1.Pod{2: {boundPod("svc-a-engine-2-default-0", "node-b")}})
	if got := store.placementWaitOf(2); got != nil {
		t.Errorf("placement wait = %v, want it retired once the source reads Failed", got)
	}
	if store.writes != 1 {
		t.Errorf("writes = %d, want 1", store.writes)
	}
}
