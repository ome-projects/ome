package ops_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A terminal pod occupies its stable name, so the owning operation must
// delete it before the target can be recreated. These tests pin that
// recycle on Create and Restart: the delete, its bookkeeping (RetryCount,
// LastProgressAt, LastFailure), the retry-ladder pacing, and the deadline
// bound.

const admissionRejectReason = "UnexpectedAdmissionError"

// rejectedPod marks pod as rejected by the kubelet at admission: phase
// Failed with a pod-level reason and no container ever started.
func rejectedPod(pod *corev1.Pod) *corev1.Pod {
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = admissionRejectReason
	pod.Status.Message = "Pod was rejected: resources unavailable"
	pod.Status.Conditions = nil
	return pod
}

func drainEvents(rec *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func podExists(c client.Client, pod *corev1.Pod) bool {
	err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
	return !apierrors.IsNotFound(err)
}

func listPods(t *testing.T, c client.Client, ns string) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace(ns)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return pods.Items
}

func TestCreate_RecyclesTerminalPodThenRecreates(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	dead := rejectedPod(podForInstance(isvc, 0, false, false))
	c := newFakeClient(t, isvc, dead)
	rec := record.NewFakeRecorder(16)
	deps := workload.Deps{Client: c, Recorder: rec}
	plan := buildPlanSinglePodEngine(1)

	// Pass 1: the dead occupant is deleted; nothing is created yet.
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	res, err := ops.Create(context.Background(), deps, input, plan, nil)
	if err != nil {
		t.Fatalf("Create pass 1: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected a requeue after recycling, got %+v", res)
	}
	if podExists(c, dead) {
		t.Fatalf("terminal pod %s must be deleted", dead.Name)
	}
	if got := listPods(t, c, "prod"); len(got) != 0 {
		t.Fatalf("no replacement may be created in the recycle pass, got %d pod(s)", len(got))
	}
	if workload.DefaultExpectations.Satisfied("prod", "llama-70b", workload.ComponentEngine, 0) {
		t.Fatalf("ExpectDeletes must record the in-flight delete")
	}
	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationCreate {
		t.Fatalf("status after recycle: %+v", s)
	}
	if s.Operation.RetryCount != 1 {
		t.Fatalf("RetryCount: got %d want 1", s.Operation.RetryCount)
	}
	if s.LastFailure == nil || s.LastFailure.PodName != dead.Name || s.LastFailure.Reason != admissionRejectReason {
		t.Fatalf("LastFailure must carry the dead pod's admission reason, got %+v", s.LastFailure)
	}
	events := drainEvents(rec)
	found := false
	for _, e := range events {
		if strings.Contains(e, string(workload.EventReasonTerminalPodRecycled)) && strings.Contains(e, dead.Name) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a TerminalPodRecycled event naming %s; got %v", dead.Name, events)
	}

	// Pass 2: the watch observed the delete; the target is recreated once.
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	input = buildTestInput(isvc, c, workload.ComponentEngine)
	if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
		t.Fatalf("Create pass 2: %v", err)
	}
	got := listPods(t, c, "prod")
	if len(got) != 1 || got[0].Name != dead.Name {
		t.Fatalf("expected exactly the recreated %s, got %d pod(s)", dead.Name, len(got))
	}
	if query.IsTerminalPod(&got[0]) {
		t.Fatalf("recreated pod must be a fresh object, got phase %q", got[0].Status.Phase)
	}
	s = instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || s.Operation.RetryCount != 1 {
		t.Fatalf("a create pass is not a recycle; RetryCount must stay 1, got %+v", s.Operation)
	}
}

// creatingStatusWithRecycles seeds a Creating status whose Create attempt
// has already recycled n times, the last one at lastProgress.
func creatingStatusWithRecycles(n int32, lastProgress, deadline time.Time) v1beta1.OMENativeInstanceStatus {
	return v1beta1.OMENativeInstanceStatus{
		Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			ID: "create-0-1", Type: v1beta1.InstanceOperationCreate, Step: "CreatePods",
			RetryCount:     n,
			StartedAt:      metav1.NewTime(lastProgress.Add(-time.Hour)),
			LastProgressAt: metav1.NewTime(lastProgress),
			Deadline:       metav1.NewTime(deadline),
		},
	}
}

// Repeated recycles within one attempt follow the update retry ladder:
// initialDelay × multiplier^(n-1), capped at maxDelay, measured from the
// previous recycle.
func TestCreate_RecyclePacedByRetryLadder(t *testing.T) {
	policy := &workload.RetryPolicy{MaxAttempts: 5, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
	cases := []struct {
		recycles int32
		delay    time.Duration
	}{{1, 20 * time.Second}, {2, 40 * time.Second}, {3, time.Minute}, {4, time.Minute}}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("after %d recycles", tc.recycles), func(t *testing.T) {
			resetExpectations(t)
			// Status timestamps round-trip at second precision.
			clk := clocktesting.NewFakeClock(time.Now().Truncate(time.Second))
			isvc := minimalISVC("llama-70b", "prod", 1)
			ir := instanceIR(isvc, workload.ComponentEngine,
				creatingStatusWithRecycles(tc.recycles, clk.Now(), clk.Now().Add(time.Hour)))
			dead := rejectedPod(podForInstance(isvc, 0, false, false))
			c := newFakeClient(t, isvc, ir, dead)
			deps := workload.Deps{Client: c}
			plan := buildPlanSinglePodEngine(1)
			run := func() {
				t.Helper()
				input := buildTestInput(isvc, c, workload.ComponentEngine)
				input.Clock = clk
				input.UpdateRetryPolicy = policy
				if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
					t.Fatalf("Create: %v", err)
				}
			}

			clk.Step(tc.delay - time.Second)
			run()
			if !podExists(c, dead) {
				t.Fatalf("recycled %v early: the ladder requires %v after recycle %d", tc.delay-time.Second, tc.delay, tc.recycles)
			}
			if s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]; s.Operation.RetryCount != tc.recycles {
				t.Fatalf("RetryCount must not move while paced, got %d", s.Operation.RetryCount)
			}

			clk.Step(time.Second)
			run()
			if podExists(c, dead) {
				t.Fatalf("recycle must proceed once %v has elapsed", tc.delay)
			}
			s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
			if s.Operation.RetryCount != tc.recycles+1 {
				t.Fatalf("RetryCount: got %d want %d", s.Operation.RetryCount, tc.recycles+1)
			}
			if !s.Operation.LastProgressAt.Time.Equal(clk.Now()) {
				t.Fatalf("LastProgressAt must anchor the next delay at the recycle time, got %v want %v", s.Operation.LastProgressAt.Time, clk.Now())
			}
		})
	}
}

// Without a configured policy there is no ladder: the recycle happens on
// the next pass regardless of how many came before.
func TestCreate_RecycleWithoutPolicyIsImmediate(t *testing.T) {
	resetExpectations(t)
	clk := clocktesting.NewFakeClock(time.Now())
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := instanceIR(isvc, workload.ComponentEngine, creatingStatusWithRecycles(5, clk.Now(), clk.Now().Add(time.Hour)))
	dead := rejectedPod(podForInstance(isvc, 0, false, false))
	c := newFakeClient(t, isvc, ir, dead)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clk
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(1), nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if podExists(c, dead) {
		t.Fatalf("with no policy the recycle must not wait")
	}
	if s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]; s.Operation.RetryCount != 6 {
		t.Fatalf("RetryCount: got %d want 6", s.Operation.RetryCount)
	}
}

// Past the attempt's deadline nothing is recycled: escalation owns the
// attempt from there.
func TestCreate_RecycleRefusedPastDeadline(t *testing.T) {
	resetExpectations(t)
	clk := clocktesting.NewFakeClock(time.Now())
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := instanceIR(isvc, workload.ComponentEngine, creatingStatusWithRecycles(0, clk.Now().Add(-time.Hour), clk.Now().Add(-time.Second)))
	dead := rejectedPod(podForInstance(isvc, 0, false, false))
	c := newFakeClient(t, isvc, ir, dead)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clk
	if _, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, buildPlanSinglePodEngine(1), nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if !podExists(c, dead) {
		t.Fatalf("an expired attempt must not recycle")
	}
	if s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]; s.Operation.RetryCount != 0 {
		t.Fatalf("RetryCount must stay 0 past the deadline, got %d", s.Operation.RetryCount)
	}
}

// A gang whose every member died is total loss and Create's to rebuild: all
// dead members are deleted in one pass and recreated together on the next,
// at the same incarnation.
func TestCreate_RecyclesWholeTerminalGangTogether(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceCreating,
		Operation: &v1beta1.InstanceOperation{
			ID: "create-0-1", Type: v1beta1.InstanceOperationCreate, Step: "CreatePods",
			StartedAt: metav1.Now(), LastProgressAt: metav1.Now(), Deadline: metav1.NewTime(time.Now().Add(time.Hour)),
		},
	})
	leader := rejectedPod(gangPod(isvc, 0, "leader", 0, 2, false, false))
	worker := rejectedPod(gangPod(isvc, 0, "worker", 0, 2, false, false))
	c := newFakeClient(t, isvc, ir, leader, worker)
	deps := workload.Deps{Client: c}
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
		t.Fatalf("Create pass 1: %v", err)
	}
	if got := listPods(t, c, "prod"); len(got) != 0 {
		t.Fatalf("both dead members must be deleted in one pass, %d remain", len(got))
	}
	if s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]; s.Operation == nil || s.Operation.RetryCount != 1 || s.Incarnation != 2 {
		t.Fatalf("status after gang recycle: %+v", s)
	}

	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	input = buildTestInput(isvc, c, workload.ComponentEngine)
	if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
		t.Fatalf("Create pass 2: %v", err)
	}
	got := listPods(t, c, "prod")
	if len(got) != 2 {
		t.Fatalf("both members must be recreated together, got %d", len(got))
	}
	for _, pod := range got {
		if pod.Labels[query.LabelInstanceIncarnation] != "2" || query.IsTerminalPod(&pod) {
			t.Fatalf("recreated member %s must be fresh at incarnation 2: labels=%v phase=%q", pod.Name, pod.Labels, pod.Status.Phase)
		}
	}
}

// Restart Phase B: a new-incarnation member that died is recycled and
// recreated at the same bumped incarnation within the same attempt.
func TestRestart_RecyclesTerminalNewIncarnationPod(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     2,
		Phase:           v1beta1.OMENativeInstanceRestarting,
		RunningRevision: "llama-70b-engine-" + testRevisionHash,
		Operation: &v1beta1.InstanceOperation{
			ID: "restart-0-1", Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
			StartedAt: metav1.Now(), LastProgressAt: metav1.Now(), Deadline: metav1.NewTime(time.Now().Add(time.Hour)),
		},
	}
	dead := rejectedPod(podAtIncarnation(isvc, 0, 2, false, false))
	c := newFakeClient(t, isvc, ir, dead)
	deps := workload.Deps{Client: c}

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	done, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart pass 1: %v", err)
	}
	if done || podExists(c, dead) {
		t.Fatalf("Restart must delete the dead new-incarnation pod and stay in flight (done=%v exists=%v)", done, podExists(c, dead))
	}
	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceRestarting || s.Incarnation != 2 {
		t.Fatalf("status after recycle: %+v", s)
	}
	if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationRestart || s.Operation.RetryCount != 1 {
		t.Fatalf("Restart operation must record the recycle, got %+v", s.Operation)
	}
	if s.LastFailure == nil || s.LastFailure.Reason != admissionRejectReason {
		t.Fatalf("LastFailure must carry the admission reason, got %+v", s.LastFailure)
	}

	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	input = buildTestInput(isvc, c, workload.ComponentEngine)
	plan = buildPlanSinglePodEngineForRestart(c, isvc)
	if _, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "trigger"); err != nil {
		t.Fatalf("Restart pass 2: %v", err)
	}
	got := listPods(t, c, "prod")
	if len(got) != 1 || got[0].Labels[query.LabelInstanceIncarnation] != "2" || query.IsTerminalPod(&got[0]) {
		t.Fatalf("expected one fresh pod at incarnation 2, got %+v", got)
	}
}
