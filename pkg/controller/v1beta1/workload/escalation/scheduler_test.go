package escalation_test

// Scheduler-hold contract: a pod the scheduler reports it cannot place
// is waiting on cluster capacity, not failing. The escalation pass
// records the hold on the Instance operation, the deadline-parking step
// stops the InstanceReadyTimeout clock while it lasts, and only an
// operator-configured grace turns the hold into a terminal,
// environment-caused failure that never blames the revision.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/holds"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/replay"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const schedulerMessage = "0/3 nodes are available: 3 Insufficient nvidia.com/gpu"

// unschedulablePod builds a Pending pod the scheduler has ruled out,
// carrying the scheduler's own message and the moment it did so.
func unschedulablePod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.PodScheduled,
				Status:             corev1.ConditionFalse,
				Reason:             types.WaitingReasonUnschedulable,
				Message:            schedulerMessage,
				LastTransitionTime: metav1.NewTime(since),
			}},
		},
	}
}

// unroutedPod builds a running pod a surge's drain step has taken out of
// rotation: ContainersReady, the serving gate off and PodReady False.
func unroutedPod(name string) *corev1.Pod {
	pod := servingPod(name)
	for i := range pod.Status.Conditions {
		switch pod.Status.Conditions[i].Type {
		case "ome.io/serving", corev1.PodReady:
			pod.Status.Conditions[i].Status = corev1.ConditionFalse
		}
	}
	return pod
}

// creatingInstance is a single-pod Create attempt with a live deadline —
// the shape a first rollout presents while its pod waits for placement.
func creatingInstance(deadline time.Time) []types.InstanceStatus {
	return []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 1,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationCreate,
			Deadline: metav1.NewTime(deadline),
		},
	}}
}

// TestEscalation_UnschedulablePodRecordsHold: with no grace configured
// the row is not failed — the hold is recorded as the operation's
// Waiting token and the scheduler's message lands on LastFailure, so
// `kubectl describe` names the constraint that has no placement.
func TestEscalation_UnschedulablePodRecordsHold(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (queued for placement, not failed)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
		t.Errorf("LastFailure: got %+v want the scheduler message %q", got.LastFailure, schedulerMessage)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (the cluster, not the revision, has no placement)", rec.blocks)
	}
}

// TestEscalation_UnschedulableHoldIsEdgeTriggered: a pod queued for
// hours re-derives the same token and message every pass, so the second
// pass writes nothing.
func TestEscalation_UnschedulableHoldIsEdgeTriggered(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	writes := 0
	inner := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
		return inner(ctx, idx, func(s *types.InstanceStatus) bool {
			changed := mutate(s)
			if changed {
				writes++
			}
			return changed
		})
	}
	pods := map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}

	for pass := 0; pass < 3; pass++ {
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1), pods); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	}

	if writes != 1 {
		t.Errorf("status writes across three passes: got %d want 1 (edge-triggered)", writes)
	}
}

// TestEscalation_UnschedulableHoldClearedOnSchedule: once the scheduler
// places the pod the hold is gone, so the token is cleared and the
// deadline clock restarts.
func TestEscalation_UnschedulableHoldClearedOnSchedule(t *testing.T) {
	now := time.Now()
	insts := creatingInstance(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = types.WaitingReasonUnschedulable
	input, rec := escalationFixture(insts)

	scheduled := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-default-0"},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		}},
	}
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {scheduled}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want cleared", got)
	}
	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating", rec.store[0].Phase)
	}
}

// TestEscalation_UnschedulablePastGraceFailsEnvironmentCaused: past the
// configured grace the attempt ends. The evidence names the scheduler's
// message and the revision keeps a clean retry ladder — no placement is
// something no corrected revision can fix.
func TestEscalation_UnschedulablePastGraceFailsEnvironmentCaused(t *testing.T) {
	now := time.Now()
	insts := creatingInstance(now.Add(30 * time.Minute))
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
		t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
	}
	if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, schedulerMessage) {
		t.Errorf("LastFailure.Message: got %+v want the scheduler message", got.LastFailure)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_UnschedulableWithoutGraceNeverEscalates: with the grace
// unconfigured an unplaceable pod waits for an operator. The parked
// deadline (already elapsed here, stamped before the park landed) must
// not fail it either.
func TestEscalation_UnschedulableWithoutGraceNeverEscalates(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(-time.Hour)))

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-24*time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (no grace configured, deadline parked)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestEscalation_UnschedulableGraceRunsFromCondition: the window is
// measured from the PodScheduled transition, not from pod creation, so a
// pod that only just became unplaceable is still inside a grace shorter
// than its own age.
func TestEscalation_UnschedulableGraceRunsFromCondition(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	input.UnschedulableGrace = 10 * time.Minute

	pod := unschedulablePod("engine-0-default-0", now.Add(-time.Minute))
	pod.CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Hour))
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {pod}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (one minute of hold against a ten minute grace)", rec.store[0].Phase)
	}
}

// gatedUnschedulablePod builds a pod that a queue's gate held from its
// creation until the queue admitted it, after which the scheduler found it
// no node. PodScheduled keeps the time Kubernetes wrote at creation, because
// the refusal leaves the status False.
func gatedUnschedulablePod(name, message string, created time.Time) *corev1.Pod {
	pod := unschedulablePod(name, created)
	pod.CreationTimestamp = metav1.NewTime(created)
	pod.Labels = map[string]string{constants.KueueQueueLabelKey: "queue-a"}
	pod.Status.Conditions[0].Message = message
	return pod
}

// gatedPasses runs one escalation pass per call on a fake clock, feeding each
// pass the rows the previous one wrote, and returns row 0 after the pass.
func gatedPasses(t *testing.T, clock *clocktesting.FakeClock, input types.ReconcileInput, rec *escalationRecorder,
	plan types.ComponentPlan, pods map[int32][]*corev1.Pod) func(after time.Duration) types.InstanceStatus {
	return func(after time.Duration) types.InstanceStatus {
		t.Helper()
		clock.Step(after)
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
		if err := runEscalationPass(t, types.Deps{}, input, plan, pods); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		return rec.store[0]
	}
}

// TestEscalation_UnschedulableGatedPodGraceRunsFromTheVerdict: a pod waits
// two hours behind a queue's gate, the queue admits it, and the scheduler
// finds it no node. Its PodScheduled time is its creation, so the ten minute
// grace runs from the pass that first sees the refusal. That pass records
// its time as the attempt's placement wait, and the Instance is failed
// eleven minutes later, not the moment its pod is admitted.
func TestEscalation_UnschedulableGatedPodGraceRunsFromTheVerdict(t *testing.T) {
	now := time.Now()
	clock := clocktesting.NewFakeClock(now)
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	input.Clock = clock
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"
	pass := gatedPasses(t, clock, input, rec, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {gatedUnschedulablePod("engine-0-default-0", schedulerMessage, now.Add(-2*time.Hour))}})

	got := pass(0)
	if got.Phase != types.InstancePhaseCreating {
		t.Fatalf("first pass Phase: got %q want Creating (the refusal is new, the gate is not)", got.Phase)
	}
	if got.Operation == nil || got.Operation.PlacementWaitSince == nil || !got.Operation.PlacementWaitSince.Time.Equal(now) {
		t.Fatalf("first pass Operation: got %+v want the placement wait recorded at the first pass", got.Operation)
	}
	if got.LastFailure == nil || !got.LastFailure.Time.Time.Equal(now) {
		t.Fatalf("first pass LastFailure: got %+v want the hold stamped at the first pass", got.LastFailure)
	}
	if got = pass(9 * time.Minute); got.Phase != types.InstancePhaseCreating {
		t.Fatalf("nine minutes on Phase: got %q want Creating", got.Phase)
	}
	if !got.Operation.PlacementWaitSince.Time.Equal(now) || !got.LastFailure.Time.Time.Equal(now) {
		t.Errorf("nine minutes on: got wait %v and record %v, want both at %v (the start stays put)",
			got.Operation.PlacementWaitSince, got.LastFailure.Time, now)
	}
	if got = pass(2 * time.Minute); got.Phase != types.InstancePhaseFailed {
		t.Errorf("eleven minutes on Phase: got %q want Failed", got.Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
}

// TestEscalation_UnschedulableGatedRestartKeepsTheExitAndFailsPastGrace: OME
// opened a repair after the pod's container died, so the row's LastFailure
// names the dead container. The rebuilt pod waits two hours behind its
// queue's gate, and after admission the scheduler finds it no node. The
// wait leaves the exit record in place for the retry schedule, so the start
// of the wait cannot live there. The attempt's placement wait carries the
// start instead. OME still holds the repair nine minutes on and fails it
// once the ten minute grace has passed.
func TestEscalation_UnschedulableGatedRestartKeepsTheExitAndFailsPastGrace(t *testing.T) {
	now := time.Now()
	clock := clocktesting.NewFakeClock(now)
	insts := restartingInstance(now.Add(30 * time.Minute))
	exitCode := int32(137)
	exit := &types.InstanceTermination{
		PodName: "engine-0-default-0", ContainerName: "main", Reason: "OOMKilled", ExitCode: &exitCode,
		Time: metav1.NewTime(now.Add(-3 * time.Hour)),
	}
	recorded := *exit
	insts[0].LastFailure = &recorded
	input, rec := escalationFixture(insts)
	input.Clock = clock
	input.UnschedulableGrace = 10 * time.Minute
	pass := gatedPasses(t, clock, input, rec, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {gatedUnschedulablePod("engine-0-default-0", schedulerMessage, now.Add(-2*time.Hour))}})

	got := pass(0)
	if got.Phase != types.InstancePhaseRestarting || got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Fatalf("first pass: got Phase=%q Operation=%+v want Restarting and held on the scheduler", got.Phase, got.Operation)
	}
	if got.LastFailure == nil || got.LastFailure.ContainerName != exit.ContainerName {
		t.Fatalf("first pass LastFailure: got %+v want the exit record kept", got.LastFailure)
	}
	if got.Operation.PlacementWaitSince == nil || !got.Operation.PlacementWaitSince.Time.Equal(now) {
		t.Fatalf("first pass PlacementWaitSince: got %v want the first pass", got.Operation.PlacementWaitSince)
	}
	if got = pass(9 * time.Minute); got.Phase != types.InstancePhaseRestarting {
		t.Fatalf("nine minutes on Phase: got %q want Restarting", got.Phase)
	}
	if got = pass(2 * time.Minute); got.Phase != types.InstancePhaseFailed {
		t.Errorf("eleven minutes on Phase: got %q want Failed (the grace has passed since the refusal was first seen)", got.Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
}

// TestEscalation_UnschedulableGatedGangWorkerIsHeldOnAdmission: a gang of a
// leader and a worker waits two hours behind a queue's gate. On admission
// the scheduler binds the leader first, and the worker still reads "waiting
// for gang sibling". The pass records the worker's hold and fails nothing.
func TestEscalation_UnschedulableGatedGangWorkerIsHeldOnAdmission(t *testing.T) {
	now := time.Now()
	created := now.Add(-2 * time.Hour)
	insts := creatingInstance(now.Add(30 * time.Minute))
	insts[0].PodCount = 2
	input, rec := escalationFixture(insts)
	input.Clock = clocktesting.NewFakeClock(now)
	input.UnschedulableGrace = 10 * time.Minute
	leader := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-0-leader-0", CreationTimestamp: metav1.NewTime(created)},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(now),
			}},
		},
	}
	worker := gatedUnschedulablePod("engine-0-worker-0", "waiting for gang sibling engine-0-leader-0 to be placed", created)

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {leader, worker}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (the gang is being placed)", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.PodName != worker.Name || !got.LastFailure.Time.Time.Equal(now) {
		t.Errorf("LastFailure: got %+v want the worker's hold stamped now", got.LastFailure)
	}
	if got.Operation == nil || got.Operation.PlacementWaitSince == nil || !got.Operation.PlacementWaitSince.Time.Equal(now) {
		t.Errorf("Operation: got %+v want the gang's placement wait recorded now", got.Operation)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestEscalation_UnschedulableGatedHoldIsEdgeTriggered: the scheduler refuses
// a pod that waited behind its queue's gate, and passes run at zero, one and
// three minutes. The first pass writes the waiting token and the start of the
// placement wait in one write. It also writes the scheduler's record, unless
// the row keeps a container's exit record on LastFailure. The next two
// passes write nothing.
func TestEscalation_UnschedulableGatedHoldIsEdgeTriggered(t *testing.T) {
	for _, tc := range []struct {
		name string
		exit bool
	}{{name: "no exit record"}, {name: "exit record kept", exit: true}} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			clock := clocktesting.NewFakeClock(now)
			insts := creatingInstance(now.Add(30 * time.Minute))
			if tc.exit {
				insts[0].LastFailure = &types.InstanceTermination{
					PodName: "engine-0-default-0", ContainerName: "main", Reason: "Error", Time: metav1.NewTime(now.Add(-time.Hour)),
				}
			}
			input, rec := escalationFixture(insts)
			input.Clock = clock
			writes := 0
			inner := input.MutateInstance
			input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
				return inner(ctx, idx, func(s *types.InstanceStatus) bool {
					changed := mutate(s)
					if changed {
						writes++
					}
					return changed
				})
			}
			pass := gatedPasses(t, clock, input, rec, singleInstancePlan(0, 1),
				map[int32][]*corev1.Pod{0: {gatedUnschedulablePod("engine-0-default-0", schedulerMessage, now.Add(-2*time.Hour))}})

			for i := 0; i < 3; i++ {
				pass(time.Duration(i) * time.Minute)
			}
			if writes != 1 {
				t.Errorf("status writes across three passes: got %d want 1 (edge-triggered)", writes)
			}
			if got := rec.store[0].Operation; got == nil || got.PlacementWaitSince == nil || !got.PlacementWaitSince.Time.Equal(now) {
				t.Errorf("Operation: got %+v want the placement wait kept at the first pass", got)
			}
		})
	}
}

// queuedPod builds a pod created behind a queue's gate that the queue has
// not admitted yet.
func queuedPod(name string, created time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
			Labels:            map[string]string{constants.KueueQueueLabelKey: "queue-a"},
		},
		Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: "kueue.x-k8s.io/admission"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonSchedulingGated,
				LastTransitionTime: metav1.NewTime(created),
			}},
		},
	}
}

// TestEscalation_UnschedulableGatedRequeueStartsAFreshGrace: the queue
// admits a pod and the scheduler finds it no node. Five minutes later the
// queue evicts it, and OME creates the pod again behind the gate. Two hours
// on, the queue admits it again and the scheduler refuses it again. The wait
// behind the gate does not count. The pass that sees the pod gated retires
// the recorded start, and the ten minute grace starts again at the second
// refusal. OME still holds the Instance nine minutes on and fails it eleven
// minutes on.
func TestEscalation_UnschedulableGatedRequeueStartsAFreshGrace(t *testing.T) {
	now := time.Now()
	clock := clocktesting.NewFakeClock(now)
	input, rec := escalationFixture(creatingInstance(now.Add(24 * time.Hour)))
	input.Clock = clock
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"
	pods := map[int32][]*corev1.Pod{0: {gatedUnschedulablePod("engine-0-default-0", schedulerMessage, now.Add(-time.Hour))}}
	pass := gatedPasses(t, clock, input, rec, singleInstancePlan(0, 1), pods)

	if got := pass(0); got.Operation == nil || got.Operation.PlacementWaitSince == nil {
		t.Fatalf("first refusal: got Operation %+v want the placement wait recorded", got.Operation)
	}
	requeued := now.Add(5 * time.Minute)
	pods[0] = []*corev1.Pod{queuedPod("engine-0-default-0", requeued)}
	if got := pass(5 * time.Minute); got.Operation == nil || got.Operation.PlacementWaitSince != nil {
		t.Fatalf("back behind the gate: got Operation %+v want the placement wait retired", got.Operation)
	}
	pods[0] = []*corev1.Pod{gatedUnschedulablePod("engine-0-default-0", schedulerMessage, requeued)}
	got := pass(2 * time.Hour)
	if got.Phase != types.InstancePhaseCreating {
		t.Fatalf("second refusal Phase: got %q want Creating (the wait behind the gate does not count)", got.Phase)
	}
	if want := clock.Now(); got.Operation == nil || got.Operation.PlacementWaitSince == nil || !got.Operation.PlacementWaitSince.Time.Equal(want) {
		t.Fatalf("second refusal Operation: got %+v want the placement wait recorded at %v", got.Operation, want)
	}
	if got = pass(9 * time.Minute); got.Phase != types.InstancePhaseCreating {
		t.Fatalf("nine minutes on Phase: got %q want Creating", got.Phase)
	}
	if got = pass(2 * time.Minute); got.Phase != types.InstancePhaseFailed {
		t.Errorf("eleven minutes on Phase: got %q want Failed", got.Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_UnschedulableBesideAQueuedMemberFailsPastGrace: a queue
// admits a gang's leader after two hours but not yet its worker, and the
// scheduler refuses the leader. The leader's grace runs from the first pass
// that sees the refusal, though the worker still waits behind the gate. OME
// still holds the Instance nine minutes on and fails it eleven minutes on.
func TestEscalation_UnschedulableBesideAQueuedMemberFailsPastGrace(t *testing.T) {
	now := time.Now()
	clock := clocktesting.NewFakeClock(now)
	insts := creatingInstance(now.Add(24 * time.Hour))
	insts[0].PodCount = 2
	input, rec := escalationFixture(insts)
	input.Clock = clock
	input.UnschedulableGrace = 10 * time.Minute
	created := now.Add(-2 * time.Hour)
	leader := gatedUnschedulablePod("engine-0-default-0", "waiting for gang sibling engine-0-default-1 to be placed", created)
	pass := gatedPasses(t, clock, input, rec, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {leader, queuedPod("engine-0-default-1", created)}})

	got := pass(0)
	if got.Phase != types.InstancePhaseCreating {
		t.Fatalf("first pass Phase: got %q want Creating (the refusal is new)", got.Phase)
	}
	if got.Operation == nil || got.Operation.PlacementWaitSince == nil || !got.Operation.PlacementWaitSince.Time.Equal(now) {
		t.Fatalf("first pass Operation: got %+v want the placement wait recorded at the first pass", got.Operation)
	}
	if got = pass(9 * time.Minute); got.Phase != types.InstancePhaseCreating {
		t.Fatalf("nine minutes on Phase: got %q want Creating", got.Phase)
	}
	if got = pass(2 * time.Minute); got.Phase != types.InstancePhaseFailed {
		t.Errorf("eleven minutes on Phase: got %q want Failed (a refused pod keeps its grace beside a gated one)", got.Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_UnschedulableUngatedPodKeepsItsConditionTime: a pod without
// the queue-name label was refused at its creation an hour ago, under a ten
// minute grace. Its PodScheduled time is the scheduler's refusal, so the
// first pass that sees it fails the attempt, and no placement wait is
// recorded for it.
func TestEscalation_UnschedulableUngatedPodKeepsItsConditionTime(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	input.Clock = clocktesting.NewFakeClock(now)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"
	pod := gatedUnschedulablePod("engine-0-default-0", schedulerMessage, now.Add(-time.Hour))
	pod.Labels = nil

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {pod}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0]; got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (an hour of hold against a ten minute grace)", got.Phase)
	}
}

// TestReconcileGatedDeadlines_ParksUnschedulableInstance: the scheduler
// hold parks the InstanceReadyTimeout clock exactly as a quota block
// does, and releasing it restarts the clock from that point.
func TestReconcileGatedDeadlines_ParksUnschedulableInstance(t *testing.T) {
	now := time.Now()
	insts := []types.InstanceStatus{{
		Index: 0,
		Phase: types.InstancePhaseCreating,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationCreate,
			Waiting:  types.WaitingReasonUnschedulable,
			Deadline: metav1.NewTime(now.Add(-time.Minute)),
		},
	}}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("unschedulable Deadline: got %v want zero (parked)", (*store)[0].Operation.Deadline)
	}

	released := append([]types.InstanceStatus(nil), (*store)...)
	released[0].Operation = &types.InstanceOperation{Type: types.InstanceOperationCreate}
	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, released, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines after placement: %v", err)
	}
	if (*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("released Deadline: got zero want a restarted clock")
	}
}

// TestReconcileGatedDeadlines_ParksUnschedulableGangSurgeSource: a gang
// surge creates its pods under the SURGE index while the governing
// operation and its deadline live on the SOURCE, so the source's clock
// must park on the surge row's hold, whichever member the gang scheduler
// rejected.
func TestReconcileGatedDeadlines_ParksUnschedulableGangSurgeSource(t *testing.T) {
	now := time.Now()
	surge := int32(1)
	insts := []types.InstanceStatus{
		{
			Index: 0,
			Phase: types.InstancePhaseUpdating,
			Operation: &types.InstanceOperation{
				Type:       types.InstanceOperationUpdate,
				Step:       types.UpdateStepSurge,
				SurgeIndex: &surge,
				Deadline:   metav1.NewTime(now.Add(10 * time.Minute)),
			},
		},
		{
			Index: 1,
			Phase: types.InstancePhaseCreating,
			Operation: &types.InstanceOperation{
				Type:    types.InstanceOperationUpdate,
				Step:    types.UpdateStepGangSurgeTarget,
				Waiting: types.WaitingReasonUnschedulable,
			},
		},
	}
	input, store, _ := expireFixture(insts)

	if err := escalation.ReconcileGatedDeadlines(context.Background(), input, insts, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	if !(*store)[0].Operation.Deadline.IsZero() {
		t.Errorf("source Deadline: got %v want zero (parked on the surge row's hold)", (*store)[0].Operation.Deadline)
	}
}

// migrationPair is the durable shape of a migration in flight: both rows
// carry a Migrate operation with its own deadline, and SurgeIndex points
// at the sibling from either side.
func migrationPair(deadline time.Time) []types.InstanceStatus {
	source, surge := int32(0), int32(1)
	op := func(sibling *int32) *types.InstanceOperation {
		return &types.InstanceOperation{
			Type:        types.InstanceOperationMigrate,
			Step:        "CreateSurge",
			RequestUUID: "mig-1",
			SurgeIndex:  sibling,
			Deadline:    metav1.NewTime(deadline),
		}
	}
	return []types.InstanceStatus{
		{Index: source, Phase: types.InstancePhaseMigrating, PodCount: 1, Operation: op(&surge)},
		{Index: surge, Phase: types.InstancePhaseCreating, PodCount: 1, Operation: op(&source)},
	}
}

// TestEscalation_UnschedulableMigrationSurgeHoldsTheSurgeRow: a
// migration's replacement has nowhere to land. The hold belongs to the
// row whose own pod cannot be placed — the surge — and the serving
// source is left untouched, since stamping a wait onto a healthy
// serving replica would be a status lie.
func TestEscalation_UnschedulableMigrationSurgeHoldsTheSurgeRow(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(migrationPair(now.Add(30 * time.Minute)))
	plan := types.ComponentPlan{Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
	}}

	if err := runEscalationPass(t, types.Deps{}, input, plan, map[int32][]*corev1.Pod{
		0: {servingPod("engine-0-default-0")},
		1: {unschedulablePod("engine-1-default-0", now.Add(-5*time.Minute))},
	}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[1].Operation; got == nil || got.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("surge Operation.Waiting: got %+v want %q", got, types.WaitingReasonUnschedulable)
	}
	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("source Operation.Waiting: got %+v want unset (its own pod is placed and serving)", got)
	}
	for _, s := range rec.store {
		if s.Phase == types.InstancePhaseFailed {
			t.Errorf("instance %d: got Failed want the migration still in flight", s.Index)
		}
	}

	// The park reaches BOTH rows: the surge through its own token, the
	// source through the SurgeIndex hop.
	parked := append([]types.InstanceStatus(nil), rec.store...)
	parkInput, store, _ := expireFixture(parked)
	if err := escalation.ReconcileGatedDeadlines(context.Background(), parkInput, parked, map[int32]bool{}, 30*time.Minute); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	for _, s := range *store {
		if !s.Operation.Deadline.IsZero() {
			t.Errorf("instance %d Deadline: got %v want zero (parked)", s.Index, s.Operation.Deadline)
		}
	}
}

// TestEscalation_UnschedulableMigrationSurgePastGraceFails: past the
// grace the replacement's attempt ends. The surge row is stamped Failed
// with its Operation preserved — the migration record remains the pair's
// terminal authority — and no revision is blamed for a cluster with no
// room. The serving source keeps its phase.
func TestEscalation_UnschedulableMigrationSurgePastGraceFails(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(migrationPair(now.Add(30 * time.Minute)))
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"
	plan := types.ComponentPlan{Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
		{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
	}}

	if err := runEscalationPass(t, types.Deps{}, input, plan, map[int32][]*corev1.Pod{
		0: {servingPod("engine-0-default-0")},
		1: {unschedulablePod("engine-1-default-0", now.Add(-time.Hour))},
	}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[1].Phase != types.InstancePhaseFailed {
		t.Errorf("surge Phase: got %q want Failed", rec.store[1].Phase)
	}
	if rec.store[1].Operation == nil {
		t.Errorf("surge Operation: got nil want preserved (the migration record owns the pair)")
	}
	if rec.store[0].Phase != types.InstancePhaseMigrating {
		t.Errorf("source Phase: got %q want Migrating (it is serving)", rec.store[0].Phase)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_UnschedulableHoldIgnoresMessageChurn: the scheduler
// recomputes its message every cycle — node counts move as the cluster
// breathes — so the hold must be edge-triggered on the held STATE. The
// message reaches operators once, at hold start.
func TestEscalation_UnschedulableHoldIgnoresMessageChurn(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	writes := 0
	inner := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
		return inner(ctx, idx, func(s *types.InstanceStatus) bool {
			changed := mutate(s)
			if changed {
				writes++
			}
			return changed
		})
	}

	since := now.Add(-5 * time.Minute)
	for pass, message := range []string{
		schedulerMessage,
		"0/4 nodes are available: 4 Insufficient nvidia.com/gpu",
		"0/9 nodes are available: 9 Insufficient nvidia.com/gpu",
	} {
		pod := unschedulablePod("engine-0-default-0", since)
		pod.Status.Conditions[0].Message = message
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {pod}}); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	}

	if writes != 1 {
		t.Errorf("status writes across three churning messages: got %d want 1", writes)
	}
	if got := rec.store[0].LastFailure; got == nil || got.Message != schedulerMessage {
		t.Errorf("LastFailure.Message: got %+v want the message from hold start", got)
	}
}

// TestEscalation_UnschedulableYieldsToAnotherWaitingToken: an operation
// already parked on a different external wait keeps that token — the
// writer that set it owns it — and is never grace-escalated as
// unschedulable on evidence this code did not record.
func TestEscalation_UnschedulableYieldsToAnotherWaitingToken(t *testing.T) {
	now := time.Now()
	insts := creatingInstance(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = types.RejectionReasonQuotaExceeded
	refusedAt := metav1.NewTime(now)
	insts[0].Operation.CapacityRefusedAt = &refusedAt
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Operation == nil || got.Operation.Waiting != types.RejectionReasonQuotaExceeded {
		t.Errorf("Operation.Waiting: got %+v want the quota token untouched", got.Operation)
	}
	if got.Phase == types.InstancePhaseFailed {
		t.Errorf("Phase: got Failed want the quota wait left to its own writer")
	}
}

// TestEscalation_UnschedulableGraceWarnsAfterAHoldWasRecorded: the hold
// records the Unschedulable reason on LastFailure a pass before the
// grace elapses. The escalation that follows must still announce itself
// — an operator watching events would otherwise see the row go Failed
// in silence.
func TestEscalation_UnschedulableGraceWarnsAfterAHoldWasRecorded(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(creatingInstance(now.Add(30 * time.Minute)))
	input.ObservedState.UpdateRevision = "own-engine-newhash"

	// Pass 1: inside the grace, so only the hold is recorded.
	input.UnschedulableGrace = time.Hour
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-10*time.Minute))}}); err != nil {
		t.Fatalf("escalation pass 1: %v", err)
	}
	if rec.store[0].LastFailure == nil {
		t.Fatalf("pass 1 must record the hold on LastFailure")
	}
	if len(rec.warns) != 0 {
		t.Fatalf("pass 1 warns: got %v want none", rec.warns)
	}

	// Pass 2: the hold outlasts the grace.
	input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	input.UnschedulableGrace = 5 * time.Minute
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-10*time.Minute))}}); err != nil {
		t.Fatalf("escalation pass 2: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", rec.store[0].Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed across both passes: got %v want exactly one", rec.warns)
	}
	if len(rec.warns) == 1 && !strings.Contains(rec.warns[0], schedulerMessage) {
		t.Errorf("warning: got %q want the scheduler message", rec.warns[0])
	}
}

// TestEscalation_UnschedulableGangPastGraceKeepsItsOperation: a gang
// surge is not a disposable attempt — the abandon path consumes its
// Failed-with-Operation continuation — so the grace escalation takes the
// plain stamp instead of the disposition. The revision is still
// blameless: a gang with nowhere to land says nothing about its pod
// template.
func TestEscalation_UnschedulableGangPastGraceKeepsItsOperation(t *testing.T) {
	now := time.Now()
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 2,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepGangSurgeTarget,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {
			unschedulablePod("engine-0-leader-0", now.Add(-time.Hour)),
			unschedulablePod("engine-0-worker-0", now.Add(-time.Hour)),
		}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if got.Operation == nil {
		t.Errorf("Operation: got nil want preserved (the abandon path consumes the continuation)")
	}
	if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
		t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_UnschedulableGangPastGraceEndsTheSourceSurgeToo: the
// hold stands on the replacement gang's row, but the attempt it ends is
// the source's surge, which the update pass retires only once the source
// reads Failed. So the grace writes Failed, with the operation kept and
// the scheduler's record, on both rows of the pair in one pass, raises
// one InstanceFailed per row and charges no revision. The source's own
// pods keep serving throughout.
func TestEscalation_UnschedulableGangPastGraceEndsTheSourceSurgeToo(t *testing.T) {
	now := time.Now()
	surgeIdx := int32(2)
	insts := []types.InstanceStatus{
		{
			Index:           0,
			Phase:           types.InstancePhaseUpdating,
			PodCount:        2,
			RunningRevision: "own-engine-oldhash",
			TargetRevision:  "own-engine-newhash",
			Operation: &types.InstanceOperation{
				Type:           types.InstanceOperationUpdate,
				Step:           types.UpdateStepSurge,
				SurgeIndex:     &surgeIdx,
				TargetRevision: "own-engine-newhash",
			},
		},
		{
			Index:          2,
			Phase:          types.InstancePhaseCreating,
			PodCount:       2,
			TargetRevision: "own-engine-newhash",
			Operation: &types.InstanceOperation{
				Type:           types.InstanceOperationUpdate,
				Step:           types.UpdateStepGangSurgeTarget,
				TargetRevision: "own-engine-newhash",
			},
		},
	}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"
	plan := types.ComponentPlan{Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
		{Index: 2, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
	}}

	if err := runEscalationPass(t, types.Deps{}, input, plan, map[int32][]*corev1.Pod{
		0: {servingPod("engine-0-leader-0"), servingPod("engine-0-worker-0")},
		2: {
			unschedulablePod("engine-2-leader-0", now.Add(-time.Hour)),
			unschedulablePod("engine-2-worker-0", now.Add(-time.Hour)),
		},
	}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	for _, row := range rec.store {
		if row.Phase != types.InstancePhaseFailed {
			t.Errorf("Instance %d Phase: got %q want Failed", row.Index, row.Phase)
		}
		if row.Operation == nil {
			t.Errorf("Instance %d Operation: got nil want kept (the update pass ends the pair from it)", row.Index)
		}
		if row.LastFailure == nil || row.LastFailure.Reason != types.WaitingReasonUnschedulable || row.LastFailure.PodName != "engine-2-leader-0" {
			t.Errorf("Instance %d LastFailure: got %+v want the scheduler's record naming engine-2-leader-0", row.Index, row.LastFailure)
		}
	}
	if op := rec.store[0].Operation; op != nil && (op.Step != types.UpdateStepSurge || op.SurgeIndex == nil || *op.SurgeIndex != surgeIdx) {
		t.Errorf("source Operation: got %+v want the surge kept with its surge index", op)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
	if len(rec.warns) != 2 {
		t.Errorf("WarnInstanceFailed: got %v want one call per row of the pair", rec.warns)
	}
}

// TestEscalation_UnschedulableMemberPastGraceEndsTheDrainingSourceAlone:
// once the replacement gang has taken over serving, the source stands at
// its drain step and a member rebuilt after a loss carries the hold on
// the replacement's row. Past the grace the attempt is ended on the
// source alone: Failed with its surge kept and the scheduler's record
// naming the member, one InstanceFailed for the source and no revision
// charged. The replacement's row is left as it was, so the hold pass
// keeps reading it and releases the token once the member is retired.
func TestEscalation_UnschedulableMemberPastGraceEndsTheDrainingSourceAlone(t *testing.T) {
	now := time.Now()
	surgeIdx := int32(2)
	insts := []types.InstanceStatus{
		{
			Index:           0,
			Phase:           types.InstancePhaseUpdating,
			PodCount:        2,
			RunningRevision: "own-engine-oldhash",
			TargetRevision:  "own-engine-newhash",
			Operation: &types.InstanceOperation{
				Type:           types.InstanceOperationUpdate,
				Step:           types.UpdateStepSurgeDrain,
				SurgeIndex:     &surgeIdx,
				TargetRevision: "own-engine-newhash",
				RetryCount:     1,
			},
		},
		{
			Index:          2,
			Phase:          types.InstancePhaseCreating,
			PodCount:       2,
			TargetRevision: "own-engine-newhash",
			Operation: &types.InstanceOperation{
				Type:           types.InstanceOperationUpdate,
				Step:           types.UpdateStepGangSurgeTarget,
				TargetRevision: "own-engine-newhash",
			},
		},
	}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = "own-engine-newhash"
	plan := types.ComponentPlan{Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
		{Index: 2, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
	}}

	if err := runEscalationPass(t, types.Deps{}, input, plan, map[int32][]*corev1.Pod{
		0: {unroutedPod("engine-0-leader-0"), unroutedPod("engine-0-worker-0")},
		2: {servingPod("engine-2-leader-0"), unschedulablePod("engine-2-worker-0", now.Add(-time.Hour))},
	}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	source := rec.store[0]
	if source.Phase != types.InstancePhaseFailed {
		t.Errorf("source Phase: got %q want Failed", source.Phase)
	}
	if op := source.Operation; op == nil || op.Step != types.UpdateStepSurgeDrain || op.SurgeIndex == nil || *op.SurgeIndex != surgeIdx {
		t.Errorf("source Operation: got %+v want the drain-step surge kept with its surge index", op)
	}
	if source.LastFailure == nil || source.LastFailure.Reason != types.WaitingReasonUnschedulable || source.LastFailure.PodName != "engine-2-worker-0" {
		t.Errorf("source LastFailure: got %+v want the scheduler's record naming engine-2-worker-0", source.LastFailure)
	}
	replacement := rec.store[1]
	if replacement.Phase != types.InstancePhaseCreating {
		t.Errorf("replacement Phase: got %q want Creating (the row stays live so its hold is released with the member)", replacement.Phase)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call, for the source", rec.warns)
	}
}

// TestEscalation_UnschedulableHoldClearedWhenPodDeleted: the pod going
// away ends the hold as surely as it being placed. The token is released
// and the parked clock restarts from the release, so the attempt gets a
// full window to try again rather than inheriting an expired one.
func TestEscalation_UnschedulableHoldClearedWhenPodDeleted(t *testing.T) {
	now := time.Now()
	insts := creatingInstance(now.Add(30 * time.Minute))
	insts[0].Operation.Waiting = types.WaitingReasonUnschedulable
	// Parked while the hold lasted.
	insts[0].Operation.Deadline = metav1.Time{}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute

	// The pod is gone: an empty bucket for the instance.
	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if got := rec.store[0].Operation; got == nil || got.Waiting != "" {
		t.Errorf("Operation.Waiting: got %+v want cleared", got)
	}
	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (a vanished pod is not a failure)", rec.store[0].Phase)
	}

	released := append([]types.InstanceStatus(nil), rec.store...)
	parkInput, store, _ := expireFixture(released)
	const timeout = 30 * time.Minute
	if err := escalation.ReconcileGatedDeadlines(context.Background(), parkInput, released, map[int32]bool{}, timeout); err != nil {
		t.Fatalf("ReconcileGatedDeadlines: %v", err)
	}
	rearmed := (*store)[0].Operation.Deadline
	if rearmed.IsZero() {
		t.Fatalf("Deadline: got zero want a clock restarted at the release")
	}
	if remaining := time.Until(rearmed.Time); remaining < timeout-time.Minute {
		t.Errorf("Deadline: got %s of headroom want a full window measured from the release", remaining)
	}
}

// TestEscalation_UnschedulableDeleteRowIsLeftToDeleteBatch: a scale-down
// of a pod that never got placed still has an unschedulable pod under
// it, and DeleteBatch may be pacing the removal past any grace. The
// scheduler hold must not touch that row: DeleteBatch owns its progress
// and completion, and a Failed stamp over a teardown it is still driving
// is a status lie.
func TestEscalation_UnschedulableDeleteRowIsLeftToDeleteBatch(t *testing.T) {
	now := time.Now()
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseDeleting,
		PodCount: 1,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationDelete,
			Step:     "Drain",
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseDeleting {
		t.Errorf("Phase: got %q want Deleting (DeleteBatch owns the row)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != "" {
		t.Errorf("Operation: got %+v want no hold recorded on a teardown", got.Operation)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// routedViewClient is a fake client the source-rotation reading can
// read EndpointSlices from; with none seeded, every labeled source reads
// as routed.
func routedViewClient(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, discoveryv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatalf("scheme: %v", err)
		}
	}
	return fake.NewClientBuilder().WithScheme(scheme).Build()
}

// backoffPod builds a pod wedged in a terminal kubelet waiting state on
// a named revision — a leftover from a prior attempt when its revision
// is not the one in flight.
func backoffPod(name, revision string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{query.LabelRevisionHash: query.RevisionFromName(revision).Hash()},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
			}},
		},
	}
}

// TestEscalation_UnschedulableSurgeBlamesOnlyTheTarget: a single-pod
// surge shares its Instance index with the source, so the blamed set
// holds both. When the source is wedged on its own image and the
// replacement simply has nowhere to land, the hold escalation must judge
// the replacement alone — reading the source's pull failure would charge
// the target revision a RetryBlock for a shortage of nodes.
//
// The source is still in rotation: a source out of rotation is the
// surge's own report, and that report outranks the scheduler's on the
// row, so the grace would not be what ends the attempt.
func TestEscalation_UnschedulableSurgeBlamesOnlyTheTarget(t *testing.T) {
	now := time.Now()
	const targetRev = "own-engine-newhash"
	insts := []types.InstanceStatus{{
		Index:           0,
		Phase:           types.InstancePhaseUpdating,
		PodCount:        1,
		RunningRevision: "own-engine-oldhash",
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepSurge,
			TargetRevision: targetRev,
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = targetRev

	target := unschedulablePod("engine-0-default-1", now.Add(-time.Hour))
	target.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName(targetRev).Hash()}
	source := backoffPod("engine-0-default-0", "own-engine-oldhash")
	source.Status.Conditions = []corev1.PodCondition{{Type: query.ServingConditionType, Status: corev1.ConditionTrue}}
	if err := runEscalationPass(t, types.Deps{Client: routedViewClient(t)}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {source, target}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed", got.Phase)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (no node is not the revision's fault)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want exactly one", rec.warns)
	}
}

// TestEscalation_UnschedulableSupersededLeftoverIsSilent: the
// disposition declines to act when the only workload-caused pod belongs
// to a superseded revision — charging the fresh target for a leftover's
// failure would wedge recovery. Nothing is written, so nothing may be
// announced either; an unconditional event would re-fire every pass for
// as long as the leftover lingers.
func TestEscalation_UnschedulableSupersededLeftoverIsSilent(t *testing.T) {
	now := time.Now()
	const targetRev = "own-engine-newhash"
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 1,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationCreate,
			TargetRevision: targetRev,
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 10 * time.Minute
	input.ObservedState.UpdateRevision = targetRev

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
		map[int32][]*corev1.Pod{0: {
			backoffPod("engine-0-default-0", "own-engine-oldhash"),
			unschedulablePod("engine-0-default-1", now.Add(-time.Hour)),
		}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase == types.InstancePhaseFailed {
		t.Errorf("Phase: got Failed want untouched (the disposition wrote nothing)")
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none — nothing was written to announce", rec.warns)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none", rec.blocks)
	}
}

// TestEscalation_UnschedulableGangHoldIsOrderIndependent: pods arrive
// from an informer cache in map order, which is unstable between passes.
// A gang whose members are all unplaceable must still pick the same one
// every pass, or the recorded PodName alternates and the row rewrites
// its hold forever despite nothing having changed.
func TestEscalation_UnschedulableGangHoldIsOrderIndependent(t *testing.T) {
	now := time.Now()
	insts := []types.InstanceStatus{{
		Index:    0,
		Phase:    types.InstancePhaseCreating,
		PodCount: 3,
		Operation: &types.InstanceOperation{
			Type:           types.InstanceOperationUpdate,
			Step:           types.UpdateStepGangSurgeTarget,
			TargetRevision: "own-engine-newhash",
			Deadline:       metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}}
	input, rec := escalationFixture(insts)
	writes := 0
	inner := input.MutateInstance
	input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
		return inner(ctx, idx, func(s *types.InstanceStatus) bool {
			changed := mutate(s)
			if changed {
				writes++
			}
			return changed
		})
	}

	since := now.Add(-5 * time.Minute)
	leader := unschedulablePod("engine-0-leader-0", since)
	worker0 := unschedulablePod("engine-0-worker-0", since)
	worker1 := unschedulablePod("engine-0-worker-1", since)
	// Every permutation the cache could hand back.
	for pass, bucket := range [][]*corev1.Pod{
		{leader, worker0, worker1},
		{worker1, leader, worker0},
		{worker0, worker1, leader},
		{worker1, worker0, leader},
	} {
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 3),
			map[int32][]*corev1.Pod{0: bucket}); err != nil {
			t.Fatalf("escalation pass %d: %v", pass, err)
		}
		input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), rec.store...)
	}

	if writes != 1 {
		t.Errorf("status writes across four cache orderings: got %d want 1", writes)
	}
	if got := rec.store[0].LastFailure; got == nil || got.PodName != leader.Name {
		t.Errorf("LastFailure.PodName: got %+v want the stable choice %s", got, leader.Name)
	}
}

// restartingInstance is a repair in flight with a live deadline — the
// shape Phase C presents while the rebuilt pod waits for placement.
func restartingInstance(deadline time.Time) []types.InstanceStatus {
	return []types.InstanceStatus{{
		Index:       0,
		Incarnation: 2,
		Phase:       types.InstancePhaseRestarting,
		PodCount:    1,
		Operation: &types.InstanceOperation{
			Type:     types.InstanceOperationRestart,
			Step:     "Drain",
			Reason:   "pod lost",
			Deadline: metav1.NewTime(deadline),
		},
	}}
}

// TestEscalation_UnschedulableRestartHoldsThenFailsPastGrace: a repair
// whose rebuilt pod has no placement is held, not failed — the token
// parks the InstanceReadyTimeout clock so the cluster's shortage is not
// charged to the repair, and the scheduler's own message reaches
// LastFailure. Past a configured grace the attempt ends
// environment-caused, so no revision is blamed.
func TestEscalation_UnschedulableRestartHoldsThenFailsPastGrace(t *testing.T) {
	now := time.Now()

	t.Run("held while the grace is unconfigured", func(t *testing.T) {
		input, rec := escalationFixture(restartingInstance(now.Add(30 * time.Minute)))
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseRestarting {
			t.Errorf("Phase: got %q want Restarting (queued for placement, not failed)", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
			t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
		}
		if !types.OperationExternallyHeld(got.Operation) {
			t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
		}
		if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
			t.Errorf("LastFailure: got %+v want the scheduler message", got.LastFailure)
		}
		if got.Operation != nil && got.Operation.Reason != "pod lost" {
			t.Errorf("Operation.Reason: got %q want the repair cause preserved", got.Operation.Reason)
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none", rec.blocks)
		}
	})

	t.Run("failed past the configured grace", func(t *testing.T) {
		input, rec := escalationFixture(restartingInstance(now.Add(30 * time.Minute)))
		input.UnschedulableGrace = 10 * time.Minute
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseFailed {
			t.Errorf("Phase: got %q want Failed", got.Phase)
		}
		if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
			t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
		}
	})
}

// TestEscalation_UnschedulableRollHoldsThenFailsPastGrace: an update
// whose pod the scheduler cannot place is waiting on cluster capacity.
// Both mechanisms answer it the same way — the hold takes the operation's
// waiting token, which parks the InstanceReadyTimeout clock so a
// shortage of nodes is not charged to the roll, and the scheduler's own
// message reaches LastFailure. Past a configured grace the attempt ends
// environment-caused, so the revision keeps a clean retry ladder.
func TestEscalation_UnschedulableRollHoldsThenFailsPastGrace(t *testing.T) {
	now := time.Now()
	for _, step := range []string{"InPlace", "Drain"} {
		t.Run(step+" held while the grace is unconfigured", func(t *testing.T) {
			input, rec := escalationFixture([]types.InstanceStatus{
				rollInFlight(0, step, 1, now.Add(30*time.Minute)),
			})
			if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
				map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-5*time.Minute))}}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			got := rec.store[0]
			if got.Phase != types.InstancePhaseUpdating {
				t.Errorf("Phase: got %q want Updating (queued for placement, not failed)", got.Phase)
			}
			if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
				t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
			}
			if !types.OperationExternallyHeld(got.Operation) {
				t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
			}
			if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
				t.Errorf("LastFailure: got %+v want the scheduler message", got.LastFailure)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none", rec.blocks)
			}
		})

		t.Run(step+" failed past the configured grace", func(t *testing.T) {
			input, rec := escalationFixture([]types.InstanceStatus{
				rollInFlight(0, step, 1, now.Add(30*time.Minute)),
			})
			input.UnschedulableGrace = 10 * time.Minute
			input.ObservedState.UpdateRevision = updateTarget().Name
			if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
				map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-0", now.Add(-time.Hour))}}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			got := rec.store[0]
			if got.Phase != types.InstancePhaseFailed {
				t.Errorf("Phase: got %q want Failed", got.Phase)
			}
			if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
				t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
			}
			if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, schedulerMessage) {
				t.Errorf("LastFailure.Message: got %+v want the scheduler message", got.LastFailure)
			}
			if len(rec.blocks) != 0 {
				t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
			}
		})
	}
}

// TestEscalation_UnschedulableDrainHoldsThenFailsPastGrace: past the
// surge step the source is already out of rotation, so a replacement the
// scheduler cannot place leaves the Instance serving nothing — and it is
// still a wait on cluster capacity, not a fault of the roll. The hold
// takes the operation's waiting token, which parks the shared deadline so
// the drain is not failed for a shortage of nodes, and the scheduler's
// message reaches LastFailure. Past a configured grace the attempt ends
// environment-caused, with the revision's retry ladder untouched.
func TestEscalation_UnschedulableDrainHoldsThenFailsPastGrace(t *testing.T) {
	now := time.Now()

	t.Run("held while the grace is unconfigured", func(t *testing.T) {
		input, rec := escalationFixture([]types.InstanceStatus{
			rollInFlight(0, types.UpdateStepSurgeDrain, 1, now.Add(30*time.Minute)),
		})
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-1", now.Add(-5*time.Minute))}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseUpdating {
			t.Errorf("Phase: got %q want Updating (queued for placement, not failed)", got.Phase)
		}
		if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
			t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
		}
		if !types.OperationExternallyHeld(got.Operation) {
			t.Errorf("Operation: got %+v want externally held so the deadline parks", got.Operation)
		}
		if got.LastFailure == nil || got.LastFailure.Message != schedulerMessage {
			t.Errorf("LastFailure: got %+v want the scheduler message", got.LastFailure)
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none", rec.blocks)
		}
	})

	t.Run("failed past the configured grace", func(t *testing.T) {
		input, rec := escalationFixture([]types.InstanceStatus{
			rollInFlight(0, types.UpdateStepSurgeDrain, 1, now.Add(30*time.Minute)),
		})
		input.UnschedulableGrace = 10 * time.Minute
		input.ObservedState.UpdateRevision = updateTarget().Name
		if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 1),
			map[int32][]*corev1.Pod{0: {unschedulablePod("engine-0-default-1", now.Add(-time.Hour))}}); err != nil {
			t.Fatalf("escalation pass: %v", err)
		}
		got := rec.store[0]
		if got.Phase != types.InstancePhaseFailed {
			t.Errorf("Phase: got %q want Failed", got.Phase)
		}
		if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable {
			t.Errorf("LastFailure.Reason: got %+v want %q", got.LastFailure, types.WaitingReasonUnschedulable)
		}
		if got.LastFailure == nil || !strings.Contains(got.LastFailure.Message, schedulerMessage) {
			t.Errorf("LastFailure.Message: got %+v want the scheduler message", got.LastFailure)
		}
		if len(rec.blocks) != 0 {
			t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
		}
	})
}

// TestEscalation_UnschedulablePastGraceReleasesTheExclusion drives the
// hold arm end to end: an Instance holds a node exclusion for the
// revision it is rebuilding, the rebuild outlasts the grace with no
// placement, and the pass releases the exclusion through the disposition
// — the ledger projects nothing for it, the budget it spent stays spent,
// and one Normal event names the freed node — before the row goes
// Failed with the scheduler's verdict and no RetryBlock.
func TestEscalation_UnschedulablePastGraceReleasesTheExclusion(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	fc := clocktesting.NewFakeClock(now)
	c := fakeLedgerClient(t)
	owner := ledgerOwnerCM()
	seeded := &audit.Ledger{}
	seeded.UpsertEntry(audit.Entry{
		RequestUUID: "u-directive", Component: "engine", SourceInstance: 0,
		Phase: audit.PhaseCompleted, Reason: audit.ReasonAutoRecover,
		Outcome: audit.OutcomeRelocateRecreate, FromNode: "node-a", Revision: "own-engine-newhash",
		StartedAt:   now.Add(-time.Hour).UTC().Format(time.RFC3339),
		CompletedAt: now.Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	if err := audit.PersistLedgerForOwner(context.Background(), c, owner, corev1.SchemeGroupVersion.WithKind("ConfigMap"), seeded); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	insts := creatingInstance(now.Add(30 * time.Minute))
	insts[0].Operation.TargetRevision = "own-engine-newhash"
	insts[0].RunningRevision = "own-engine-newhash"
	input, rec := dispositionFixtureInput(fc, &insts, "own-engine-newhash", nil, owner)
	input.UnschedulableGrace = 10 * time.Minute
	input.Disposition = types.DispositionDeps{AutoMigrateMaxAttempts: 3}
	recorder := record.NewFakeRecorder(8)
	plan := singleInstancePlan(0, 1)
	plan.MigrationMode = types.MigrationModeAuto
	pod := unschedulableRebuild("engine-0-default-0", "node-a",
		"0/2 nodes are available: 1 node(s) didn't match Pod's node affinity/selector, 1 Insufficient nvidia.com/gpu", now.Add(-time.Hour))

	if err := runEscalationPass(t, types.Deps{Client: c, Clock: fc, Recorder: recorder}, input, plan,
		map[int32][]*corev1.Pod{0: {pod}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	ledger := loadLedger(t, c)
	if exclusions := audit.RecentAutoRecoverExclusions(ledger, "engine", 0, 3); len(exclusions) != 0 {
		t.Errorf("exclusions: got %v want none after the release", exclusions)
	}
	if got := audit.CountAutoRecoverAttempts(ledger, "engine", 0); got != 1 {
		t.Errorf("budget: got %d want 1 (the released directive still counts)", got)
	}
	if events := drainEvents(recorder); countEventsWithReason(events, types.EventReasonNodeExclusionReleased) != 1 {
		t.Errorf("events: got %v want one %s", events, types.EventReasonNodeExclusionReleased)
	}
	if insts[0].Phase != types.InstancePhaseFailed || insts[0].Operation != nil {
		t.Errorf("instance: got Phase=%q Operation=%+v want Failed with no operation", insts[0].Phase, insts[0].Operation)
	}
	if insts[0].LastFailure == nil || insts[0].LastFailure.Reason != types.WaitingReasonUnschedulable {
		t.Errorf("LastFailure: got %+v want the scheduler's verdict", insts[0].LastFailure)
	}
	if len(rec.blockCalls) != 0 {
		t.Errorf("RetryBlock calls: got %+v want none (no placement is not the revision's fault)", rec.blockCalls)
	}
}

// gangDomainFullMessage is the scheduler's report for a gang held to the
// topology domain where its other members run, when that domain cannot
// take the member being placed.
const gangDomainFullMessage = "0/8 nodes are available: domain domain-a cannot fit all remaining gang members ns/engine-0. preemption: not eligible"

// gangDomainFullWorker is the gang's worker the scheduler cannot place because
// the domain the leader already runs in cannot take it.
func gangDomainFullWorker(since time.Time) *corev1.Pod {
	pod := unschedulablePod("engine-0-worker-0", since)
	pod.Status.Conditions[0].Message = gangDomainFullMessage
	return pod
}

// TestEscalation_UnschedulableGangDomainFullEndsOnTheShorterGrace: a
// repair's leader is placed and running, but its worker has waited six
// minutes because the domain the leader runs in cannot take it. More
// capacity elsewhere cannot help, so the five minute full-domain grace
// ends the attempt well before the fifteen minute general grace would.
// The failure is the environment's: the scheduler's message is recorded
// and no revision is charged.
func TestEscalation_UnschedulableGangDomainFullEndsOnTheShorterGrace(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(restartingGang(now.Add(30 * time.Minute)))
	input.UnschedulableGrace = 15 * time.Minute
	input.GangDomainFullGrace = 5 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {
			servingPod("engine-0-leader-0"),
			gangDomainFullWorker(now.Add(-6 * time.Minute)),
		}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (six minutes against a five minute full-domain grace)", got.Phase)
	}
	if got.LastFailure == nil || got.LastFailure.Reason != types.WaitingReasonUnschedulable ||
		!strings.Contains(got.LastFailure.Message, "cannot fit all remaining gang members") {
		t.Errorf("LastFailure: got %+v want the scheduler's full-domain message", got.LastFailure)
	}
	if len(rec.blocks) != 0 {
		t.Errorf("RetryBlock: got %v want none (environment-caused)", rec.blocks)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
}

// TestEscalation_UnschedulableGangDomainFullWaitsWhileAMemberTerminates: the
// same full-domain report while another pod of the attempt is still
// being deleted. That pod may be what fills the domain, and its room
// frees as it goes, so the hold waits on the general grace instead.
func TestEscalation_UnschedulableGangDomainFullWaitsWhileAMemberTerminates(t *testing.T) {
	now := time.Now()
	input, rec := escalationFixture(restartingGang(now.Add(30 * time.Minute)))
	input.UnschedulableGrace = 15 * time.Minute
	input.GangDomainFullGrace = 5 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2),
		map[int32][]*corev1.Pod{0: {
			terminating(servingPod("engine-0-leader-0")),
			gangDomainFullWorker(now.Add(-6 * time.Minute)),
		}}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseRestarting {
		t.Errorf("Phase: got %q want Restarting (a terminating member keeps the general grace)", got.Phase)
	}
	if got.Operation == nil || got.Operation.Waiting != types.WaitingReasonUnschedulable {
		t.Errorf("Operation.Waiting: got %+v want %q", got.Operation, types.WaitingReasonUnschedulable)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestEscalation_UnschedulableGangDomainFullGraceChoosesTheShorter: the
// full-domain grace never makes a hold wait longer than the general
// grace, applies to any other scheduler report not at all, and applies on
// its own when the general grace is unconfigured.
func TestEscalation_UnschedulableGangDomainFullGraceChoosesTheShorter(t *testing.T) {
	now := time.Now()
	otherReport := func(since time.Time) *corev1.Pod {
		return unschedulablePod("engine-0-worker-0", since)
	}
	cases := []struct {
		name        string
		general     time.Duration
		domainGrace time.Duration
		worker      *corev1.Pod
		wantFailure bool
	}{
		{"full-domain report, general grace shorter", 10 * time.Minute, 30 * time.Minute, gangDomainFullWorker(now.Add(-12 * time.Minute)), true},
		{"full-domain report, full-domain grace unconfigured", 15 * time.Minute, 0, gangDomainFullWorker(now.Add(-6 * time.Minute)), false},
		{"full-domain report, general grace unconfigured", 0, 5 * time.Minute, gangDomainFullWorker(now.Add(-6 * time.Minute)), true},
		{"full-domain report inside the full-domain grace", 15 * time.Minute, 5 * time.Minute, gangDomainFullWorker(now.Add(-4 * time.Minute)), false},
		{"other report keeps the general grace", 15 * time.Minute, 5 * time.Minute, otherReport(now.Add(-6 * time.Minute)), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input, rec := escalationFixture(restartingGang(now.Add(30 * time.Minute)))
			input.UnschedulableGrace = tc.general
			input.GangDomainFullGrace = tc.domainGrace
			if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2),
				map[int32][]*corev1.Pod{0: {servingPod("engine-0-leader-0"), tc.worker}}); err != nil {
				t.Fatalf("escalation pass: %v", err)
			}
			if failed := rec.store[0].Phase == types.InstancePhaseFailed; failed != tc.wantFailure {
				t.Errorf("Failed: got %v want %v (Phase %q)", failed, tc.wantFailure, rec.store[0].Phase)
			}
		})
	}
}

// gangWaitingOnWorker is a two-pod gang's Create attempt whose leader is
// placed and serving while the worker waits for placement since
// workerSince. placementWait, when set, is the attempt's recorded
// placement wait.
func gangWaitingOnWorker(now, workerSince time.Time, placementWait *time.Time) ([]types.InstanceStatus, map[int32][]*corev1.Pod) {
	insts := creatingInstance(now.Add(30 * time.Minute))
	insts[0].PodCount = 2
	insts[0].Operation.Waiting = types.WaitingReasonUnschedulable
	if placementWait != nil {
		at := metav1.NewTime(*placementWait)
		insts[0].Operation.PlacementWaitSince = &at
	}
	leader := servingPod("engine-0-default-0")
	leader.Spec.NodeName = "node-b"
	return insts, map[int32][]*corev1.Pod{0: {leader, unschedulablePod("engine-0-default-1", workerSince)}}
}

// TestEscalation_UnschedulableGrace_ReplacedGangMemberRunsFromPlacementWait:
// a gang whose worker was rebuilt after its node stopped reporting has
// waited for placement since the attempt's recorded placement wait, not
// since the rebuilt worker's own PodScheduled transition. Past the grace
// from that start the attempt ends, and the ending retires the recorded
// wait so the attempt after it runs on its own clock.
func TestEscalation_UnschedulableGrace_ReplacedGangMemberRunsFromPlacementWait(t *testing.T) {
	now := time.Now()
	attemptSince := now.Add(-18 * time.Minute)
	insts, pods := gangWaitingOnWorker(now, now.Add(-time.Minute), &attemptSince)
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 15 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pods); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	got := rec.store[0]
	if got.Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (18 minutes of placement wait against a 15 minute grace)", got.Phase)
	}
	if len(rec.warns) != 1 {
		t.Errorf("WarnInstanceFailed: got %v want one call", rec.warns)
	}
	if got.Operation != nil && got.Operation.PlacementWaitSince != nil {
		t.Errorf("PlacementWaitSince: got %v want it retired by the ending", got.Operation.PlacementWaitSince)
	}
}

// TestEscalation_UnschedulableGrace_NoPlacementWaitRunsFromPod: an
// attempt with no recorded placement wait (a scheduler wait alone, or an
// operation written before the field existed) is measured from the
// unplaceable pod's own PodScheduled transition, as before.
func TestEscalation_UnschedulableGrace_NoPlacementWaitRunsFromPod(t *testing.T) {
	now := time.Now()
	insts, pods := gangWaitingOnWorker(now, now.Add(-time.Minute), nil)
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 15 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pods); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (one minute of hold against a 15 minute grace)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// TestEscalation_UnschedulableGrace_LaterPlacementWaitKeepsPodWait: a
// recorded placement wait never makes the grace run later than the
// unplaceable pod's own wait; the earlier of the two counts.
func TestEscalation_UnschedulableGrace_LaterPlacementWaitKeepsPodWait(t *testing.T) {
	now := time.Now()
	attemptSince := now.Add(-30 * time.Second)
	insts, pods := gangWaitingOnWorker(now, now.Add(-20*time.Minute), &attemptSince)
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 15 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pods); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (the worker has waited 20 minutes against a 15 minute grace)", rec.store[0].Phase)
	}
}

// TestEscalation_UnschedulableGrace_GangDomainFullRunsFromPlacementWait:
// the replacement for a gang member whose node stopped reporting is
// refused because the domain the leader runs in cannot take it. The
// full-domain grace, like the general one, runs from the attempt's
// placement wait: the replacement's own refusal is one minute old, but
// six minutes of the attempt's wait against a five minute full-domain
// grace end the attempt.
func TestEscalation_UnschedulableGrace_GangDomainFullRunsFromPlacementWait(t *testing.T) {
	now := time.Now()
	attemptSince := now.Add(-6 * time.Minute)
	insts, pods := gangWaitingOnWorker(now, now.Add(-time.Minute), &attemptSince)
	pods[0][1].Status.Conditions[0].Message = gangDomainFullMessage
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 15 * time.Minute
	input.GangDomainFullGrace = 5 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pods); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseFailed {
		t.Errorf("Phase: got %q want Failed (six minutes of placement wait against a five minute full-domain grace)", rec.store[0].Phase)
	}
}

// TestEscalation_UnschedulableGrace_TerminatingMemberKeepsGeneralGrace: the
// rebuilt member is refused because the leader's domain cannot take it,
// but the leader is itself terminating, and its room may free as it goes.
// The full-domain grace then does not apply, even though the attempt's
// placement wait is past it: the general grace runs from that start and
// has not elapsed.
func TestEscalation_UnschedulableGrace_TerminatingMemberKeepsGeneralGrace(t *testing.T) {
	now := time.Now()
	attemptSince := now.Add(-6 * time.Minute)
	insts, pods := gangWaitingOnWorker(now, now.Add(-time.Minute), &attemptSince)
	pods[0][0] = terminating(pods[0][0])
	pods[0][1].Status.Conditions[0].Message = gangDomainFullMessage
	input, rec := escalationFixture(insts)
	input.UnschedulableGrace = 15 * time.Minute
	input.GangDomainFullGrace = 5 * time.Minute

	if err := runEscalationPass(t, types.Deps{}, input, singleInstancePlan(0, 2), pods); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}

	if rec.store[0].Phase != types.InstancePhaseCreating {
		t.Errorf("Phase: got %q want Creating (a terminating member keeps the general grace: six minutes against 15)", rec.store[0].Phase)
	}
	if len(rec.warns) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", rec.warns)
	}
}

// batchedRowStore wires the pass's status writes through the replay
// harness's row store, which applies a batch the way the status adapter
// does: a batch that names one Instance twice while its mutations report
// their commit is refused whole, and nothing in it lands. It returns the
// input, the store, and the indexes warned about, in order.
func batchedRowStore(insts []types.InstanceStatus) (types.ReconcileInput, *replay.RowStore, *[]int32) {
	store := replay.NewRowStore(insts, nil, nil)
	warned := &[]int32{}
	input := types.ReconcileInput{
		WarnInstanceFailed: func(idx int32, _, _ string) { *warned = append(*warned, idx) },
	}
	store.Install(&input)
	input.ObservedState.InstanceStatuses = append([]types.InstanceStatus(nil), insts...)
	return input, store, warned
}

// waitingSince records that the attempt has waited for placement since
// since, with the scheduler's token on the row.
func waitingSince(row types.InstanceStatus, since time.Time) types.InstanceStatus {
	op := *row.Operation
	at := metav1.NewTime(since)
	op.PlacementWaitSince = &at
	op.Waiting = types.WaitingReasonUnschedulable
	row.Operation = &op
	return row
}

// workerWaiting is a two-pod gang of Instance idx whose leader is placed
// and serving while the worker has waited one minute for placement.
func workerWaiting(idx int32, now time.Time) []*corev1.Pod {
	leader := servingPod(fmt.Sprintf("engine-%d-default-0", idx))
	leader.Spec.NodeName = "node-b"
	return []*corev1.Pod{leader, unschedulablePod(fmt.Sprintf("engine-%d-default-1", idx), now.Add(-time.Minute))}
}

// TestEscalation_PlacementWaitPastGraceInOneBatch_EveryEndingLands: an
// attempt whose grace runs from its recorded placement wait ends in the
// pass's batched status write beside another Instance whose own grace
// ended in the same pass. The row's own Failed write clears the start, so
// the batch names each Instance once, which the status adapter requires
// of a batch whose writes report their commit. A gang surge replacement
// whose source is draining stays live, and its start is cleared in a
// write of its own. A recreate ends through the deadline decision's own
// writes instead, and its start goes with them.
func TestEscalation_PlacementWaitPastGraceInOneBatch_EveryEndingLands(t *testing.T) {
	now := time.Now()
	attemptSince := now.Add(-18 * time.Minute)
	gang := func(indexes ...int32) []types.InstancePlan {
		var out []types.InstancePlan
		for _, idx := range indexes {
			out = append(out, types.InstancePlan{Index: idx, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}})
		}
		return out
	}
	// The other Instance is a one-pod Restart at index 9 whose rebuilt pod
	// has had no placement for an hour and records no placement wait.
	other := restartingInstance(now.Add(30 * time.Minute))[0]
	other.Index = 9
	otherPlan := types.InstancePlan{Index: 9, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}}
	otherPods := []*corev1.Pod{unschedulablePod("engine-9-default-0", now.Add(-time.Hour))}

	surgeIdx := int32(2)
	gangSource := func(step string) types.InstanceStatus {
		return types.InstanceStatus{
			Index: 0, Phase: types.InstancePhaseUpdating, PodCount: 2,
			RunningRevision: "own-engine-oldhash", TargetRevision: "own-engine-newhash",
			Operation: &types.InstanceOperation{
				Type: types.InstanceOperationUpdate, Step: step, SurgeIndex: &surgeIdx, TargetRevision: "own-engine-newhash",
			},
		}
	}
	replacement := waitingSince(types.InstanceStatus{
		Index: 2, Phase: types.InstancePhaseCreating, PodCount: 2, TargetRevision: "own-engine-newhash",
		Operation: &types.InstanceOperation{
			Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget, TargetRevision: "own-engine-newhash",
		},
	}, attemptSince)
	restart := restartingInstance(now.Add(30 * time.Minute))[0]
	restart.PodCount = 2
	migration := migrationPair(now.Add(30 * time.Minute))
	recreate := types.InstanceStatus{
		Index: 0, Phase: types.InstancePhaseUpdating, PodCount: 2,
		RunningRevision: "own-engine-oldhash", TargetRevision: "own-engine-newhash",
		Operation: &types.InstanceOperation{
			Type: types.InstanceOperationUpdate, Step: types.UpdateStepDrain, TargetRevision: "own-engine-newhash",
			Deadline: metav1.NewTime(now.Add(30 * time.Minute)),
		},
	}

	cases := []struct {
		name string
		rows []types.InstanceStatus
		plan []types.InstancePlan
		pods map[int32][]*corev1.Pod
		// ended are the Instances written Failed; live the ones left in
		// their phase. Every one of them ends the pass with no start.
		ended []int32
		live  []int32
	}{
		{
			name:  "a gang Restart",
			rows:  []types.InstanceStatus{waitingSince(restart, attemptSince)},
			plan:  gang(0),
			pods:  map[int32][]*corev1.Pod{0: workerWaiting(0, now)},
			ended: []int32{0},
		},
		{
			name: "a gang surge replacement beside its serving source",
			rows: []types.InstanceStatus{gangSource(types.UpdateStepSurge), replacement},
			plan: gang(0, 2),
			pods: map[int32][]*corev1.Pod{
				0: {servingPod("engine-0-default-0"), servingPod("engine-0-default-1")},
				2: workerWaiting(2, now),
			},
			ended: []int32{0, 2},
		},
		{
			name: "a gang surge replacement whose source drains",
			rows: []types.InstanceStatus{gangSource(types.UpdateStepSurgeDrain), replacement},
			plan: gang(0, 2),
			pods: map[int32][]*corev1.Pod{
				0: {unroutedPod("engine-0-default-0"), unroutedPod("engine-0-default-1")},
				2: workerWaiting(2, now),
			},
			ended: []int32{0},
			live:  []int32{2},
		},
		{
			name: "a migration's surge",
			rows: []types.InstanceStatus{migration[0], waitingSince(migration[1], attemptSince)},
			plan: []types.InstancePlan{
				{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
				{Index: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}},
			},
			pods: map[int32][]*corev1.Pod{
				0: {servingPod("engine-0-default-0")},
				1: {unschedulablePod("engine-1-default-0", now.Add(-time.Minute))},
			},
			ended: []int32{1},
			live:  []int32{0},
		},
		{
			name:  "a gang recreate, which takes the deadline decision",
			rows:  []types.InstanceStatus{waitingSince(recreate, attemptSince)},
			plan:  gang(0),
			pods:  map[int32][]*corev1.Pod{0: workerWaiting(0, now)},
			ended: []int32{0},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := append(append([]types.InstanceStatus(nil), tc.rows...), other)
			input, store, warned := batchedRowStore(rows)
			input.UnschedulableGrace = 15 * time.Minute
			pods := map[int32][]*corev1.Pod{9: otherPods}
			for idx, set := range tc.pods {
				pods[idx] = set
			}
			plan := types.ComponentPlan{Instances: append(append([]types.InstancePlan(nil), tc.plan...), otherPlan)}

			if err := runEscalationPass(t, types.Deps{}, input, plan, pods); err != nil {
				t.Errorf("escalation pass: %v", err)
			}

			stored := map[int32]types.InstanceStatus{}
			for _, row := range store.Rows() {
				stored[row.Index] = row
			}
			for _, idx := range append(append([]int32(nil), tc.ended...), other.Index) {
				if got := stored[idx]; got.Phase != types.InstancePhaseFailed {
					t.Errorf("Instance %d Phase: got %q want Failed", idx, got.Phase)
				}
			}
			for _, idx := range tc.live {
				if got := stored[idx]; got.Phase == types.InstancePhaseFailed || got.Phase == "" {
					t.Errorf("Instance %d Phase: got %q want it left live", idx, got.Phase)
				}
			}
			for _, idx := range append(append([]int32(nil), tc.ended...), tc.live...) {
				if op := stored[idx].Operation; op != nil && op.PlacementWaitSince != nil {
					t.Errorf("Instance %d PlacementWaitSince: got %v want it cleared", idx, op.PlacementWaitSince)
				}
			}
			want := len(tc.ended) + 1
			if len(*warned) != want {
				t.Errorf("WarnInstanceFailed: got %v want one call per Instance written Failed (%d)", *warned, want)
			}
		})
	}
}

// committedWrites counts the row writes a replay row store commits.
type committedWrites struct{ rows int }

func (c *committedWrites) RowCommitted(commit replay.RowCommit) {
	if commit.Outcome == replay.PreconditionOK {
		c.rows++
	}
}

func (c *committedWrites) RetryBlockCommitted(replay.RetryBlockCommit) {}

func (c *committedWrites) StatusConflict(replay.StatusConflict) {}

// TestEscalation_DrainStepPairStoppedOnHeldName_WritesNothing: a
// replacement gang has taken over serving, and its worker had no placement
// past the grace, so the attempt ended on the source, which reads Failed
// at the drain step. The replacement's leader is on a node that stopped
// reporting, so its name is held and the refused worker cannot be retired
// until the name is freed. Nothing changes between passes, and no pass
// writes: the hold pass records no placement wait on the replacement's row
// while its source reads Failed, so the escalation has none to clear.
func TestEscalation_DrainStepPairStoppedOnHeldName_WritesNothing(t *testing.T) {
	now := time.Now()
	surgeIdx := int32(2)
	worker := unschedulablePod("own-engine-2-default-1", now.Add(-20*time.Minute))
	leader := servingPod("own-engine-2-default-0")
	leader.Spec.NodeName = "node-b"
	leader.Status.Phase = corev1.PodUnknown
	source := types.InstanceStatus{
		Index: 0, Phase: types.InstancePhaseFailed, PodCount: 2,
		RunningRevision: "own-engine-oldhash", TargetRevision: "own-engine-newhash",
		Operation: &types.InstanceOperation{
			Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain, SurgeIndex: &surgeIdx,
			TargetRevision: "own-engine-newhash", RetryCount: 1, LastProgressAt: metav1.NewTime(now.Add(-30 * time.Minute)),
		},
		LastFailure: types.UnschedulableTermination(worker, types.WaitingReasonUnschedulable+": "+schedulerMessage, metav1.NewTime(now.Add(-time.Minute))),
	}
	replacement := types.InstanceStatus{
		Index: 2, Phase: types.InstancePhaseCreating, PodCount: 2, TargetRevision: "own-engine-newhash",
		Operation: &types.InstanceOperation{
			Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget, TargetRevision: "own-engine-newhash",
			Waiting: types.WaitingReasonUnschedulable,
		},
		LastFailure: types.UnschedulableTermination(worker, schedulerMessage, worker.Status.Conditions[0].LastTransitionTime),
	}
	pods := map[int32][]*corev1.Pod{
		0: {unroutedPod("own-engine-0-default-0"), unroutedPod("own-engine-0-default-1")},
		2: {leader, worker},
	}
	plan := types.ComponentPlan{Component: types.ComponentEngine, Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
		{Index: 2, Runners: []types.RunnerPlan{{Name: "default", Size: 2}}},
	}}

	writes := &committedWrites{}
	store := replay.NewRowStore([]types.InstanceStatus{source, replacement}, nil, writes)
	for pass := 1; pass <= 2; pass++ {
		input := types.ReconcileInput{UnschedulableGrace: 15 * time.Minute}
		input.Key.OwnerName, input.Key.Component = "own", types.ComponentEngine
		store.Install(&input)
		input.ObservedState.InstanceStatuses = store.Rows()
		if err := runEscalationPass(t, types.Deps{}, input, plan, pods); err != nil {
			t.Fatalf("pass %d: escalation pass: %v", pass, err)
		}
		for _, row := range store.Rows() {
			if row.Index == replacement.Index && (row.Operation == nil || row.Operation.PlacementWaitSince != nil) {
				t.Errorf("pass %d: replacement operation = %+v, want it kept with no placement wait", pass, row.Operation)
			}
		}
	}
	if writes.rows != 0 {
		t.Errorf("committed row writes = %d, want 0 over two passes of a pair where nothing changes", writes.rows)
	}
}

// TestEscalation_UnschedulableGrace_DrainSourceFailedAtPassStartRunsFromPodWait:
// a replacement gang of a leader and two workers has taken over serving.
// Worker 1's node stopped reporting sixteen minutes ago, so the
// replacement's row recorded when the attempt started waiting for
// placement. The sweep then freed worker 1's name, and the loss ended the
// attempt on the source, which reads Failed at the drain step in the rows
// the pass read at its start. Worker 2 has had no placement for one
// minute. In this pass the update pass rebuilds worker 1 and opens the
// next attempt before the escalation runs. The start the replacement's
// row carries in the rows the pass read belongs to the attempt that
// ended, so the hold is measured from worker 2's own wait: one minute
// against a 15 minute grace, and the next attempt goes on.
func TestEscalation_UnschedulableGrace_DrainSourceFailedAtPassStartRunsFromPodWait(t *testing.T) {
	now := time.Now()
	surgeIdx := int32(2)
	endedAttemptSince := metav1.NewTime(now.Add(-16 * time.Minute))
	source := types.InstanceStatus{
		Index: 0, Phase: types.InstancePhaseFailed, PodCount: 3,
		RunningRevision: "own-engine-oldhash", TargetRevision: "own-engine-newhash",
		Operation: &types.InstanceOperation{
			Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain, SurgeIndex: &surgeIdx,
			TargetRevision: "own-engine-newhash", RetryCount: 1, LastProgressAt: metav1.NewTime(now.Add(-20 * time.Minute)),
		},
		LastFailure: &types.InstanceTermination{Reason: "ReplacementLost", Time: metav1.NewTime(now.Add(-5 * time.Second))},
	}
	replacement := types.InstanceStatus{
		Index: 2, Phase: types.InstancePhaseCreating, PodCount: 2, TargetRevision: "own-engine-newhash",
		Operation: &types.InstanceOperation{
			Type: types.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget, TargetRevision: "own-engine-newhash",
			PlacementWaitSince: &endedAttemptSince,
		},
	}
	leader := servingPod("own-engine-2-default-0")
	leader.Spec.NodeName = "node-b"
	pods := map[int32][]*corev1.Pod{
		0: {unroutedPod("own-engine-0-default-0"), unroutedPod("own-engine-0-default-1"), unroutedPod("own-engine-0-default-2")},
		// Worker 1 was freed by the sweep and is not rebuilt yet.
		2: {leader, unschedulablePod("own-engine-2-default-2", now.Add(-time.Minute))},
	}
	plan := types.ComponentPlan{Component: types.ComponentEngine, Instances: []types.InstancePlan{
		{Index: 0, Runners: []types.RunnerPlan{{Name: "default", Size: 3}}},
		{Index: 2, Runners: []types.RunnerPlan{{Name: "default", Size: 3}}},
	}}
	store := replay.NewRowStore([]types.InstanceStatus{source, replacement}, nil, nil)
	var warned []int32
	input := types.ReconcileInput{
		UnschedulableGrace: 15 * time.Minute,
		WarnInstanceFailed: func(idx int32, _, _ string) { warned = append(warned, idx) },
	}
	input.Clock = clocktesting.NewFakeClock(now)
	input.Key.OwnerName, input.Key.Component = "own", types.ComponentEngine
	store.Install(&input)
	input.ObservedState.InstanceStatuses = store.Rows()
	podsOf := func(context.Context) (map[int32][]*corev1.Pod, error) { return pods, nil }
	ctx := context.Background()

	held, err := holds.Run(ctx, holds.PassInput{
		Input: input, Plan: plan, Rows: holds.RowsForPlan(plan, input.ObservedState.InstanceStatuses, nil), Pods: podsOf,
	})
	if err != nil {
		t.Fatalf("hold pass: %v", err)
	}
	if !held.Holding(replacement.Index, types.WaitingReasonUnschedulable) {
		t.Fatal("hold pass: no scheduler hold on the replacement's row")
	}
	// The update pass opens the next attempt: the source is Updating again
	// at the drain step, with one more retry counted and a fresh deadline.
	if err := input.MutateInstance(ctx, source.Index, func(s *types.InstanceStatus) bool {
		s.Phase = types.InstancePhaseUpdating
		op := *s.Operation
		op.RetryCount++
		op.StartedAt, op.LastProgressAt = metav1.NewTime(now), metav1.NewTime(now)
		op.Deadline = metav1.NewTime(now.Add(30 * time.Minute))
		s.Operation = &op
		return true
	}); err != nil {
		t.Fatalf("open the next attempt: %v", err)
	}

	if err := escalation.Run(ctx, escalation.PassInput{Input: input, Plan: plan, Pods: podsOf, Held: held}); err != nil {
		t.Fatalf("escalation pass: %v", err)
	}
	for _, row := range store.Rows() {
		if row.Index == source.Index && row.Phase != types.InstancePhaseUpdating {
			t.Errorf("source Phase: got %q want Updating (worker 2 has waited one minute of a 15 minute grace)", row.Phase)
		}
	}
	if len(warned) != 0 {
		t.Errorf("WarnInstanceFailed: got %v want none", warned)
	}
}
