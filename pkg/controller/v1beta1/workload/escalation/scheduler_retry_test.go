package escalation_test

// The scheduling-retry contract: a pod of a multi-pod Instance that has
// waited for a node past lifecycle.schedulingRetry.after is written so the
// scheduler looks at it again, the k-th write after k times the wait and
// never past the cap; a pod inside the wait, a single-pod Instance, a
// paused attempt and an unconfigured policy are left alone. The write
// touches the pod alone: the row keeps its attempt and its hold.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// gangCreateRow is a two-pod Create attempt with a live deadline: the shape
// a first rollout of a gang presents while its members wait for placement.
func gangCreateRow(now time.Time) []types.InstanceStatus {
	return []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 2,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationCreate,
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
}

// memberWaitingForNode is a Pending pod with no node and no scheduling
// attempt on record, created at the given time.
func memberWaitingForNode(name string, created time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(created)},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
}

// boundMember is a pod the scheduler has placed and the kubelet has not
// started yet.
func boundMember(name string, created time.Time) *corev1.Pod {
	pod := memberWaitingForNode(name, created)
	pod.Spec.NodeName = "node-a"
	return pod
}

// withSchedulingRetries puts the record of earlier writes on the pod.
func withSchedulingRetries(pod *corev1.Pod, count int) *corev1.Pod {
	pod.Annotations = map[string]string{
		constants.InferenceServicePodSchedulingRetryAnnotationKey: fmt.Sprintf(`{"count":%d}`, count),
	}
	return pod
}

func retryPolicy(after time.Duration, limit int32) *types.SchedulingRetryPolicy {
	return &types.SchedulingRetryPolicy{After: after, MaxAttempts: limit}
}

// schedulingRetriesOf reads the count the pass left on the stored pod; a
// pod never written reads 0.
func schedulingRetriesOf(t *testing.T, c client.Client, name string) int {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, pod); err != nil {
		t.Fatalf("read pod %s: %v", name, err)
	}
	raw, ok := pod.Annotations[constants.InferenceServicePodSchedulingRetryAnnotationKey]
	if !ok {
		return 0
	}
	var rec struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		t.Fatalf("pod %s: scheduling retry record %q: %v", name, raw, err)
	}
	return rec.Count
}

// runSchedulingRetryPass drives the hold and escalation passes over a store
// holding the pods, with a recorder, and returns the store.
func runSchedulingRetryPass(t *testing.T, input types.ReconcileInput, plan types.ComponentPlan, byIdx map[int32][]*corev1.Pod, rec *record.FakeRecorder) client.Client {
	t.Helper()
	c := escalation.ObservedPodClientForTest(input, plan, byIdx)
	deps := types.Deps{Client: c, APIReader: c}
	if rec != nil {
		deps.Recorder = rec
	}
	if err := runEscalationPass(t, deps, input, plan, byIdx); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	return c
}

// TestEscalation_SchedulingRetry_MemberLeftPendingIsWrittenAfterTheWait:
// a gang of a leader and a worker; the leader is bound, the worker has
// waited ten minutes for a node with no attempt on record. Past the
// two-minute wait the worker is written once, the leader is not, the row
// keeps its attempt, and the operator is told.
func TestEscalation_SchedulingRetry_MemberLeftPendingIsWrittenAfterTheWait(t *testing.T) {
	now := time.Now()
	input, rows := escalationFixture(gangCreateRow(now))
	input.SchedulingRetry = retryPolicy(2*time.Minute, 2)
	input.EventTarget = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "owner"}}
	events := record.NewFakeRecorder(4)
	pods := map[int32][]*corev1.Pod{0: {
		boundMember("engine-0-leader-0", now.Add(-10*time.Minute)),
		memberWaitingForNode("engine-0-worker-0", now.Add(-10*time.Minute)),
	}}

	c := runSchedulingRetryPass(t, input, gangPlan(0), pods, events)

	if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != 1 {
		t.Errorf("worker: scheduling retries got %d want 1", got)
	}
	if got := schedulingRetriesOf(t, c, "engine-0-leader-0"); got != 0 {
		t.Errorf("leader: scheduling retries got %d want 0 (a bound pod is not waiting for a node)", got)
	}
	row := rows.store[0]
	if row.Phase != types.InstancePhaseCreating || row.Operation == nil || row.Operation.Type != types.InstanceOperationCreate {
		t.Errorf("row: got phase %q op %+v want the Create attempt kept", row.Phase, row.Operation)
	}
	if len(rows.warns) != 0 || len(rows.blocks) != 0 {
		t.Errorf("warns %v blocks %v: want none, the write is not a failure", rows.warns, rows.blocks)
	}
	select {
	case ev := <-events.Events:
		if !strings.Contains(ev, string(types.EventReasonSchedulingRequestedAgain)) || !strings.Contains(ev, "engine-0-worker-0") || !strings.Contains(ev, "(1 of 2)") {
			t.Errorf("event: got %q want %s naming the worker and the count", ev, types.EventReasonSchedulingRequestedAgain)
		}
	default:
		t.Errorf("event: got none want one %s", types.EventReasonSchedulingRequestedAgain)
	}
	select {
	case ev := <-events.Events:
		t.Errorf("event: got a second %q want one", ev)
	default:
	}
}

// TestEscalation_SchedulingRetry_MemberInsideTheWaitIsLeftAlone: a worker
// that has waited ninety seconds against a two-minute wait is a slow
// member, not a lost one; nothing is written and no event is sent.
func TestEscalation_SchedulingRetry_MemberInsideTheWaitIsLeftAlone(t *testing.T) {
	now := time.Now()
	input, _ := escalationFixture(gangCreateRow(now))
	input.SchedulingRetry = retryPolicy(2*time.Minute, 2)
	input.EventTarget = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "owner"}}
	events := record.NewFakeRecorder(4)
	pods := map[int32][]*corev1.Pod{0: {
		boundMember("engine-0-leader-0", now.Add(-90*time.Second)),
		memberWaitingForNode("engine-0-worker-0", now.Add(-90*time.Second)),
	}}
	c := escalation.ObservedPodClientForTest(input, gangPlan(0), pods)
	before := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "engine-0-worker-0"}, before); err != nil {
		t.Fatal(err)
	}

	if err := runEscalationPass(t, types.Deps{Client: c, APIReader: c, Recorder: events}, input, gangPlan(0), pods); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	after := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "engine-0-worker-0"}, after); err != nil {
		t.Fatal(err)
	}
	if after.ResourceVersion != before.ResourceVersion {
		t.Errorf("worker: written inside the wait (resourceVersion %s -> %s); want left alone", before.ResourceVersion, after.ResourceVersion)
	}
	if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != 0 {
		t.Errorf("worker: scheduling retries got %d want 0", got)
	}
	select {
	case ev := <-events.Events:
		t.Errorf("event: got %q want none", ev)
	default:
	}
}

// TestEscalation_SchedulingRetry_SecondWriteWaitsTwiceTheWait: after one
// write the next is due at twice the wait from the start, so a worker at
// three minutes with one write keeps it, and one at five minutes gets the
// second.
func TestEscalation_SchedulingRetry_SecondWriteWaitsTwiceTheWait(t *testing.T) {
	for _, tc := range []struct {
		name   string
		waited time.Duration
		want   int
	}{
		{"three minutes with one write keeps one", 3 * time.Minute, 1},
		{"five minutes with one write gets the second", 5 * time.Minute, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			input, _ := escalationFixture(gangCreateRow(now))
			input.SchedulingRetry = retryPolicy(2*time.Minute, 3)
			pods := map[int32][]*corev1.Pod{0: {
				boundMember("engine-0-leader-0", now.Add(-tc.waited)),
				withSchedulingRetries(memberWaitingForNode("engine-0-worker-0", now.Add(-tc.waited)), 1),
			}}
			c := runSchedulingRetryPass(t, input, gangPlan(0), pods, nil)
			if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != tc.want {
				t.Errorf("worker: scheduling retries got %d want %d", got, tc.want)
			}
		})
	}
}

// TestEscalation_SchedulingRetry_StopsAtTheCap: a worker that carries as
// many writes as the policy allows is never written again, however long
// it waits; the deadline owns it from there.
func TestEscalation_SchedulingRetry_StopsAtTheCap(t *testing.T) {
	now := time.Now()
	input, _ := escalationFixture(gangCreateRow(now))
	input.SchedulingRetry = retryPolicy(2*time.Minute, 2)
	pods := map[int32][]*corev1.Pod{0: {
		boundMember("engine-0-leader-0", now.Add(-time.Hour)),
		withSchedulingRetries(memberWaitingForNode("engine-0-worker-0", now.Add(-time.Hour)), 2),
	}}
	c := runSchedulingRetryPass(t, input, gangPlan(0), pods, nil)
	if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != 2 {
		t.Errorf("worker: scheduling retries got %d want 2 (the cap)", got)
	}
}

// TestEscalation_SchedulingRetry_UnconfiguredWritesNothing: with no policy
// a worker Pending for an hour is left to the scheduler's own queue and
// the deadline, as when no policy is set.
func TestEscalation_SchedulingRetry_UnconfiguredWritesNothing(t *testing.T) {
	now := time.Now()
	input, _ := escalationFixture(gangCreateRow(now))
	pods := map[int32][]*corev1.Pod{0: {
		boundMember("engine-0-leader-0", now.Add(-time.Hour)),
		memberWaitingForNode("engine-0-worker-0", now.Add(-time.Hour)),
	}}
	c := runSchedulingRetryPass(t, input, gangPlan(0), pods, nil)
	if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != 0 {
		t.Errorf("worker: scheduling retries got %d want 0", got)
	}
}

// TestEscalation_SchedulingRetry_SinglePodInstanceIsLeftToTheScheduler:
// the policy covers multi-pod Instances; a single pod Pending for an hour
// is not written.
func TestEscalation_SchedulingRetry_SinglePodInstanceIsLeftToTheScheduler(t *testing.T) {
	now := time.Now()
	input, _ := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	input.SchedulingRetry = retryPolicy(2*time.Minute, 2)
	pods := map[int32][]*corev1.Pod{0: {memberWaitingForNode("engine-0-default-0", now.Add(-time.Hour))}}
	c := runSchedulingRetryPass(t, input, singleInstancePlan(0, 1), pods, nil)
	if got := schedulingRetriesOf(t, c, "engine-0-default-0"); got != 0 {
		t.Errorf("single pod: scheduling retries got %d want 0", got)
	}
}

// TestEscalation_SchedulingRetry_PausedAttemptIsLeftAlone: an attempt the
// operator paused writes nothing to its pods.
func TestEscalation_SchedulingRetry_PausedAttemptIsLeftAlone(t *testing.T) {
	now := time.Now()
	rows := gangCreateRow(now)
	rows[0].Operation.Waiting = types.WaitingReasonPaused
	input, _ := escalationFixture(rows)
	input.SchedulingRetry = retryPolicy(2*time.Minute, 2)
	pods := map[int32][]*corev1.Pod{0: {
		boundMember("engine-0-leader-0", now.Add(-time.Hour)),
		memberWaitingForNode("engine-0-worker-0", now.Add(-time.Hour)),
	}}
	c := runSchedulingRetryPass(t, input, gangPlan(0), pods, nil)
	if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != 0 {
		t.Errorf("worker under a pause: scheduling retries got %d want 0", got)
	}
}

// TestEscalation_SchedulingRetry_RuledOutMemberIsWrittenAndKeepsItsHold: a
// worker the scheduler ruled out three minutes ago carries the
// Unschedulable hold and, past the two-minute wait, is written as well;
// the hold stays on the row and nothing is failed. The scheduler's queue
// leaves a pod it still holds where it is on such a write, so the write
// costs nothing when the ruling is current.
func TestEscalation_SchedulingRetry_RuledOutMemberIsWrittenAndKeepsItsHold(t *testing.T) {
	now := time.Now()
	input, rows := escalationFixture(gangCreateRow(now))
	input.SchedulingRetry = retryPolicy(2*time.Minute, 2)
	input.UnschedulableGrace = 15 * time.Minute
	worker := unschedulablePod("engine-0-worker-0", now.Add(-3*time.Minute))
	worker.CreationTimestamp = metav1.NewTime(now.Add(-10 * time.Minute))
	pods := map[int32][]*corev1.Pod{0: {boundMember("engine-0-leader-0", now.Add(-10*time.Minute)), worker}}

	c := runSchedulingRetryPass(t, input, gangPlan(0), pods, nil)

	if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != 1 {
		t.Errorf("worker: scheduling retries got %d want 1", got)
	}
	row := rows.store[0]
	if row.Phase != types.InstancePhaseCreating || row.Operation == nil || row.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("row: got phase %q op %+v want Creating with the Unschedulable hold kept", row.Phase, row.Operation)
	}
	if len(rows.warns) != 0 {
		t.Errorf("warns: got %v want none", rows.warns)
	}
}

// TestEscalation_SchedulingRetry_WaitRunsFromTheSchedulersRuling: a worker
// created ten minutes ago that the scheduler ruled out one minute ago is
// one the scheduler is visibly working on; the wait runs from the ruling,
// so nothing is written yet.
func TestEscalation_SchedulingRetry_WaitRunsFromTheSchedulersRuling(t *testing.T) {
	now := time.Now()
	input, _ := escalationFixture(gangCreateRow(now))
	input.SchedulingRetry = retryPolicy(2*time.Minute, 2)
	worker := unschedulablePod("engine-0-worker-0", now.Add(-time.Minute))
	worker.CreationTimestamp = metav1.NewTime(now.Add(-10 * time.Minute))
	pods := map[int32][]*corev1.Pod{0: {boundMember("engine-0-leader-0", now.Add(-10*time.Minute)), worker}}
	c := runSchedulingRetryPass(t, input, gangPlan(0), pods, nil)
	if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != 0 {
		t.Errorf("worker: scheduling retries got %d want 0 (the ruling is one minute old)", got)
	}
}

// TestEscalation_SchedulingRetry_PastTheGraceNothingIsWrittenAndTheAttemptEnds:
// a worker the scheduler ruled out an hour ago under a ten-minute placement
// grace is past the grace, so this pass ends the attempt and retires its
// pods; a pod on its way out is not written for the scheduler.
func TestEscalation_SchedulingRetry_PastTheGraceNothingIsWrittenAndTheAttemptEnds(t *testing.T) {
	now := time.Now()
	input, rows := escalationFixture(gangCreateRow(now))
	input.SchedulingRetry = retryPolicy(2*time.Minute, 2)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"
	pods := map[int32][]*corev1.Pod{0: {
		boundMember("engine-0-leader-0", now.Add(-time.Hour)),
		unschedulablePod("engine-0-worker-0", now.Add(-time.Hour)),
	}}

	c := runSchedulingRetryPass(t, input, gangPlan(0), pods, nil)

	if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != 0 {
		t.Errorf("worker: scheduling retries got %d want 0 (the grace ends the attempt this pass)", got)
	}
	if row := rows.store[0]; row.Phase != types.InstancePhaseFailed {
		t.Errorf("row: got phase %q want Failed, the placement grace having run out", row.Phase)
	}
}

// TestEscalation_SchedulingRetry_GangSurgeReplacementMemberIsWritten: the
// replacement gang of a surge lives on its own row, which the plan does
// not list; its member left Pending past the wait is written like a
// Create's.
func TestEscalation_SchedulingRetry_GangSurgeReplacementMemberIsWritten(t *testing.T) {
	now := time.Now()
	surgeIdx := int32(2)
	rows := []types.InstanceStatus{
		{
			Index: 0, Phase: types.InstancePhaseUpdating, PodCount: 2, RunningRevision: "engine-old",
			Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, TargetRevision: "engine-new",
				SurgeIndex: &surgeIdx, Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
			},
		},
		{
			Index: surgeIdx, Phase: types.InstancePhaseCreating,
			Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget, TargetRevision: "engine-new",
				Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
			},
		},
	}
	input, _ := escalationFixture(rows)
	input.SchedulingRetry = retryPolicy(2*time.Minute, 2)
	pods := map[int32][]*corev1.Pod{
		0:        {servingPod("engine-0-leader-0"), servingPod("engine-0-worker-0")},
		surgeIdx: {boundMember("engine-2-leader-0", now.Add(-10*time.Minute)), memberWaitingForNode("engine-2-worker-0", now.Add(-10*time.Minute))},
	}
	c := runSchedulingRetryPass(t, input, gangPlan(0), pods, nil)
	if got := schedulingRetriesOf(t, c, "engine-2-worker-0"); got != 1 {
		t.Errorf("replacement worker: scheduling retries got %d want 1", got)
	}
	if got := schedulingRetriesOf(t, c, "engine-0-worker-0"); got != 0 {
		t.Errorf("source worker: scheduling retries got %d want 0 (it serves)", got)
	}
}
