package ops_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/replay"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// siblingCrash is two single-pod Instances on one revision under
// RecreateInstanceOnPodRestart, read at one pass: the revision's retry
// record as the pass finds it, Instance 0 in the phase the case names,
// and Instance 1 Ready on a set whose runner died inside the crash window
// a second ago. Instance 1's row records the death of the set before this
// one, so the pass reads this death as the revision's crash loop and asks
// the record whether it counts.
type siblingCrash struct {
	t    *testing.T
	isvc *v1beta1.InferenceService
	c    client.Client
	plan workload.ComponentPlan
	pod  *corev1.Pod
	// store persists the revision's retry record across the pass, as the
	// adapter does; the replay driver's store applies the same contract.
	store *replay.RowStore
	input workload.ReconcileInput
	// readySince is when Instance 1's row entered Ready on the set that
	// just died.
	readySince metav1.Time
}

// newSiblingCrash builds the pass. due is when the record came due;
// builtAt is when Instance 1's set was created; inst0 is Instance 0's
// row, the attempt's rebuild coming up or finished.
func newSiblingCrash(t *testing.T, now time.Time, block workload.RetryBlock, inst0 v1beta1.OMENativeInstanceStatus, builtAt time.Time) *siblingCrash {
	t.Helper()
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 2)
	// The record of the set before this one: the first death of Instance
	// 1, which the policy rebuilt without the record counting it.
	firstDeath := metav1.NewTime(builtAt.Add(-5 * time.Second))
	readySince := metav1.NewTime(builtAt.Add(10 * time.Second))
	diedAt := metav1.NewTime(now.Add(-time.Second))
	pod := podForInstance(isvc, 1, false, true)
	pod.Spec.Containers[0].Name = constants.MainContainerName
	pod.CreationTimestamp = metav1.NewTime(builtAt)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         constants.MainContainerName,
		RestartCount: 1,
		State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now)}},
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: "Error", ExitCode: 1,
			StartedAt:  pod.CreationTimestamp,
			FinishedAt: diedAt,
		}},
	}}
	pod.Status.Conditions = append(pod.Status.Conditions,
		corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: diedAt},
		corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: diedAt})
	inst1 := v1beta1.OMENativeInstanceStatus{
		Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: crashLoopRevision,
		ReadySince: &readySince,
		LastFailure: &v1beta1.InstanceTermination{
			PodName: pod.Name, ContainerName: constants.MainContainerName, Reason: "Error", Time: firstDeath,
		},
	}
	ir := instanceIR(isvc, workload.ComponentEngine, inst0, inst1)
	c := newFakeClient(t, isvc, ir, pod)
	h := &siblingCrash{t: t, isvc: isvc, c: c, pod: pod, readySince: readySince, store: replay.NewRowStore(nil, []workload.RetryBlock{block}, nil)}
	h.plan = buildPlanSinglePodEngine(2)
	h.plan.RestartPolicy = workload.RestartPolicyRecreateInstance
	h.plan.Instances[0].Incarnation = inst0.Incarnation
	in := buildTestInput(isvc, c, workload.ComponentEngine)
	in.Clock = clocktesting.NewFakeClock(now)
	in.PassWake = &workload.PassWake{}
	in.DesiredSpec.MinReadySeconds = int32(crashLoopWindow / time.Second)
	in.ObservedState.UpdateRevision = crashLoopRevision
	in.ObservedState.CurrentRevision = crashLoopRevision
	in.ObservedState.RetryBlocks = h.store.RetryBlocks()
	in.DesiredSpec.PodSpec = &corev1.PodSpec{Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "test:v1"}}}
	in.UpdateRetryPolicy = &workload.RetryPolicy{MaxAttempts: 3, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
	in.MutateRetryBlock = h.store.MutateRetryBlock
	h.input = in
	return h
}

// pass runs the restart pass over Instance 1 as the dispatcher would: the
// trigger reads the row and the repair acts on what it read.
func (h *siblingCrash) pass() {
	h.t.Helper()
	inst := h.plan.Instances[1]
	needs, reason := ops.DetectRestartTriggerWithPods(h.input, h.plan, inst, []*corev1.Pod{h.pod})
	if !needs {
		h.t.Fatalf("the first sighting of the death must select Instance 1, to record the crash on the row")
	}
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: h.c}, h.input, h.plan, inst, nil, reason); err != nil {
		h.t.Fatalf("Restart: %v", err)
	}
}

// row is Instance 1's row as persisted after the pass.
func (h *siblingCrash) row() v1beta1.OMENativeInstanceStatus {
	h.t.Helper()
	for _, row := range instanceStatusesOnIR(h.c, h.isvc, workload.ComponentEngine) {
		if row.Index == 1 {
			return row
		}
	}
	h.t.Fatalf("Instance 1 has no row")
	return v1beta1.OMENativeInstanceStatus{}
}

// Two single-pod Instances serve one revision under
// RecreateInstanceOnPodRestart, and the revision's retry record counts
// one failed attempt. Instance 1's set is the policy's own rebuild after
// that Instance's first death, built before the record came due, so the
// record never admitted it. Its runner dies inside the crash window.
//
// The record counts a death only when the set that died was built once
// the record came due, which is what a set the record's attempt rebuilds
// is. A set built before then dies inside the attempt under way: the row
// remembers the crash, the count does not move, and the rebuild takes its
// turn in that attempt, at once when no rebuild of the attempt is in
// flight. So "N failed attempt(s)" counts the attempts the record opened,
// not every death on the revision. The attempt's own set counts when it
// dies, whether or not the attempt still rebuilds another Instance, and a
// death while the record waits in backoff is the wave it already counts.
func TestRestart_SetBuiltBeforeTheRecordCameDue_DiesInsideTheOpenAttempt(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	due := metav1.NewTime(now.Add(-time.Minute))
	notYetDue := metav1.NewTime(now.Add(40 * time.Second))
	firstCount := metav1.NewTime(due.Add(-20 * time.Second))
	open := workload.RetryBlock{TargetRevision: crashLoopRevision, State: workload.RetryBlockRetryInProgress, AttemptsStarted: 1,
		FirstFailureAt: &firstCount, LastFailureAt: &firstCount, NextRetryAt: &due, Reason: "Error"}
	waiting := workload.RetryBlock{TargetRevision: crashLoopRevision, State: workload.RetryBlockBackoff, AttemptsStarted: 1,
		FirstFailureAt: &firstCount, LastFailureAt: &firstCount, NextRetryAt: &notYetDue, Reason: "Error"}
	inst0ReadySince := metav1.NewTime(due.Add(5 * time.Second))
	inst0ComingUp := v1beta1.OMENativeInstanceStatus{
		Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceRestarting, RunningRevision: crashLoopRevision,
		Operation: &v1beta1.InstanceOperation{ID: "attempt", Type: v1beta1.InstanceOperationRestart, Step: workload.RestartStepDrain,
			StartedAt: due, LastProgressAt: due, Deadline: metav1.NewTime(due.Add(30 * time.Minute))},
	}
	inst0Up := v1beta1.OMENativeInstanceStatus{
		Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: crashLoopRevision, ReadySince: &inst0ReadySince,
	}
	beforeDue := due.Add(-30 * time.Second)
	afterDue := due.Add(2 * time.Second)
	cases := []struct {
		name    string
		block   workload.RetryBlock
		inst0   v1beta1.OMENativeInstanceStatus
		builtAt time.Time
		// wantAttempts and wantState are the record after the pass;
		// wantRebuild says whether the pass opened Instance 1's rebuild.
		wantAttempts int32
		wantState    workload.RetryBlockState
		wantRebuild  bool
	}{
		{
			name:  "the policy's rebuild dies while the attempt still rebuilds Instance 0: remembered, not counted, waits its turn",
			block: open, inst0: inst0ComingUp, builtAt: beforeDue,
			wantAttempts: 1, wantState: workload.RetryBlockRetryInProgress,
		},
		{
			name:  "the policy's rebuild dies after the attempt rebuilt Instance 0: remembered, not counted, rebuilt as the attempt's next rebuild",
			block: open, inst0: inst0Up, builtAt: beforeDue,
			wantAttempts: 1, wantState: workload.RetryBlockRetryInProgress, wantRebuild: true,
		},
		{
			name:  "the attempt's own set dies after it came up: counted, the rebuild waits for the backoff",
			block: open, inst0: inst0Up, builtAt: afterDue,
			wantAttempts: 2, wantState: workload.RetryBlockBackoff,
		},
		{
			name:  "the attempt's own set dies while the attempt still rebuilds Instance 0: counted",
			block: open, inst0: inst0ComingUp, builtAt: afterDue,
			wantAttempts: 2, wantState: workload.RetryBlockBackoff,
		},
		{
			name:  "a death while the record waits in backoff is the wave it counts",
			block: waiting, inst0: inst0Up, builtAt: beforeDue,
			wantAttempts: 1, wantState: workload.RetryBlockBackoff,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newSiblingCrash(t, now, tc.block, tc.inst0, tc.builtAt)
			h.pass()
			b := workload.FindRetryBlock(h.store.RetryBlocks(), crashLoopRevision)
			if b == nil || b.AttemptsStarted != tc.wantAttempts || b.State != tc.wantState {
				t.Fatalf("retry record after the death = %+v, want %d attempt(s) in %s", b, tc.wantAttempts, tc.wantState)
			}
			row := h.row()
			if row.LastFailure == nil || row.LastFailure.PodName != h.pod.Name || !row.LastFailure.Time.After(h.readySince.Time) {
				t.Fatalf("the row must remember the death of its current set; got %+v (ready since %s)", row.LastFailure, h.readySince.Time)
			}
			rebuilt := row.Phase == v1beta1.OMENativeInstanceRestarting && row.Operation != nil && row.Operation.Type == v1beta1.InstanceOperationRestart
			if rebuilt != tc.wantRebuild {
				t.Fatalf("Instance 1 rebuilt this pass = %v (phase %s, operation %+v), want %v", rebuilt, row.Phase, row.Operation, tc.wantRebuild)
			}
		})
	}
}

// openAttemptAt is a revision's retry record with one attempt counted
// and its rebuild admitted at due, nothing in flight: the state the
// record reads once the attempt's set has come up.
func openAttemptAt(due time.Time) workload.RetryBlock {
	dueAt := metav1.NewTime(due)
	firstCount := metav1.NewTime(due.Add(-20 * time.Second))
	return workload.RetryBlock{TargetRevision: crashLoopRevision, State: workload.RetryBlockRetryInProgress, AttemptsStarted: 1,
		FirstFailureAt: &firstCount, LastFailureAt: &firstCount, NextRetryAt: &dueAt, Reason: "Error"}
}

// passSelects runs a pass that must select the row and leave its Restart
// unfinished; what names the step.
func passSelects(t *testing.T, h *pushedCrashLoop, what string) {
	t.Helper()
	if selected, done := h.pass(); !selected || done {
		t.Fatalf("%s; got selected=%v done=%v", what, selected, done)
	}
}

// requireRecordReads checks the revision's retry record as persisted.
func requireRecordReads(t *testing.T, h *pushedCrashLoop, state workload.RetryBlockState, attempts int32, what string) {
	t.Helper()
	if b := retryRecordOf(h); b == nil || b.State != state || b.AttemptsStarted != attempts {
		t.Fatalf("%s; got %+v, want %d attempt(s) in %s", what, b, attempts, state)
	}
}

// requireRebuildOpen checks that the row's set is being rebuilt, its set
// number raised by one.
func requireRebuildOpen(t *testing.T, h *pushedCrashLoop) {
	t.Helper()
	if row := h.row(); row.Phase != v1beta1.OMENativeInstanceRestarting || row.Incarnation != 2 || row.Operation == nil || row.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("row = %+v, want Restarting with the rebuild of the set open", row)
	}
}

// requireWakeWithin checks that the pass asked to come back within d.
func requireWakeWithin(t *testing.T, h *pushedCrashLoop, d time.Duration, what string) {
	t.Helper()
	if nextPass := h.wake.Pending(); nextPass <= 0 || nextPass > d {
		t.Fatalf("%s; got %v", what, nextPass)
	}
}

// One Instance under restart policy None serves a revision whose retry
// record reads RetryInProgress: its attempt rebuilt the set at the retry
// time, the set came up, and its runner died twice inside the crash
// window, the pod waiting in CrashLoopBackOff younger than the grace.
// The death is the record's own to count, since the set is the one its
// attempt built, and no repair answers it yet, so the two rules compose
// in that order: the row remembers the crash, the record stands at one
// attempt and the pass returns at the grace end; past the grace the
// repair opens, the crash counts once and the backoff paces the rebuild
// as the record's second attempt. A set built before the retry time,
// which the attempt never admitted, is rebuilt past the grace inside the
// open attempt with no count, under None as under every policy.
func TestRestart_TheAttemptsOwnSetCrashingUnderNoneCountsWhenTheRepairOpens(t *testing.T) {
	t.Run("the attempt's own set: remembered at the crash, counted when the repair opens, rebuilt at the backoff", func(t *testing.T) {
		h := newRolledRunnerCrash(t, workload.RestartPolicyNone, 70*time.Second, false /* waiting in CrashLoopBackOff */)
		h.blocks = []workload.RetryBlock{openAttemptAt(h.clk.Now().Add(-80 * time.Second))}

		passSelects(t, h, "the first sighting selects the row to remember the crash")
		requireRemembered(t, h)
		requireRecordReads(t, h, workload.RetryBlockRetryInProgress, 1, "no repair answers the crash inside the grace, so the record stands at its attempt")
		requireWakeWithin(t, h, 20*time.Second, "the pass must come back when the grace ends")

		h.clk.Step(30 * time.Second)
		passSelects(t, h, "the pod past the grace opens the repair")
		requireRemembered(t, h)
		requireRecordReads(t, h, workload.RetryBlockBackoff, 2, "the death of the attempt's own set counts once, at the pass the repair opens")
		requireWakeWithin(t, h, 40*time.Second, "the rebuild waits for the backoff")

		h.clk.Step(40 * time.Second)
		passSelects(t, h, "the backoff is due and the pod still waits, so the rebuild opens")
		requireRebuildOpen(t, h)
		requireRecordReads(t, h, workload.RetryBlockRetryInProgress, 2, "the rebuild is the record's second attempt, with no further count")
	})
	t.Run("a set built before the retry time: remembered at the crash, rebuilt past the grace inside the open attempt, uncounted", func(t *testing.T) {
		h := newRolledRunnerCrash(t, workload.RestartPolicyNone, 85*time.Second, false /* waiting in CrashLoopBackOff */)
		h.blocks = []workload.RetryBlock{openAttemptAt(h.clk.Now().Add(-80 * time.Second))}

		passSelects(t, h, "the first sighting selects the row to remember the crash")
		requireRemembered(t, h)
		requireRecordReads(t, h, workload.RetryBlockRetryInProgress, 1, "a set the attempt never admitted spends nothing")

		h.clk.Step(30 * time.Second)
		passSelects(t, h, "the pod past the grace is rebuilt as the attempt's next rebuild")
		requireRebuildOpen(t, h)
		requireRecordReads(t, h, workload.RetryBlockRetryInProgress, 1, "the rebuild stays inside the open attempt, with no count")
	})
}
