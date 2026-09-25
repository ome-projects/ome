package ops_test

import (
	"context"
	"fmt"
	"reflect"
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

// succeededPod marks pod as a container that ran to completion: phase
// Succeeded. Nothing will run in it again, so it is as dead as a Failed
// one while it holds its stable name.
func succeededPod(pod *corev1.Pod) *corev1.Pod {
	pod.Status.Phase = corev1.PodSucceeded
	pod.Status.Reason = "Completed"
	pod.Status.Conditions = nil
	return pod
}

// evictedPod marks pod as evicted: the kubelet (or the eviction API)
// leaves phase Failed with the Evicted pod-level reason.
func evictedPod(pod *corev1.Pod) *corev1.Pod {
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = "Evicted"
	pod.Status.Message = "The node was low on resource: memory"
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

// The Create attempt owns the recycle of a dead occupant of one of its
// target names: the delete lands this pass and the name is rebuilt on
// the next.
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

// A Succeeded pod is recycled exactly like a Failed one: it holds the
// target name and never runs again. The second half pins the re-observed
// terminal phase inside the same attempt — the ladder paces it, so the
// pass defers instead of recycling twice.
func TestCreate_RecyclesSucceededTerminalPod(t *testing.T) {
	resetExpectations(t)
	clk := clocktesting.NewFakeClock(time.Now().Truncate(time.Second))
	isvc := minimalISVC("llama-70b", "prod", 1)
	dead := succeededPod(podForInstance(isvc, 0, false, false))
	c := newFakeClient(t, isvc, dead)
	deps := workload.Deps{Client: c}
	plan := buildPlanSinglePodEngine(1)
	policy := &workload.RetryPolicy{MaxAttempts: 5, InitialDelay: 20 * time.Second, MaxDelay: time.Minute, Multiplier: 2}
	run := func() {
		t.Helper()
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		input.Clock = clk
		input.UpdateRetryPolicy = policy
		if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	run()
	if podExists(c, dead) {
		t.Fatalf("the Succeeded occupant of %s must be deleted", dead.Name)
	}
	if got := listPods(t, c, "prod"); len(got) != 0 {
		t.Fatalf("no replacement may be created in the recycle pass, got %d pod(s)", len(got))
	}
	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil ||
		s.Operation.Type != v1beta1.InstanceOperationCreate || s.Operation.RetryCount != 1 {
		t.Fatalf("status after recycling a Succeeded pod: %+v", s)
	}

	// Re-observed within the ladder's delay: the row stays put and
	// nothing is recycled or created.
	if err := c.Create(context.Background(), succeededPod(podForInstance(isvc, 0, false, false))); err != nil {
		t.Fatalf("re-seed the terminal occupant: %v", err)
	}
	workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
	clk.Step(10 * time.Second)
	run()
	if got := listPods(t, c, "prod"); len(got) != 1 || !query.IsTerminalPod(&got[0]) {
		t.Fatalf("the paced pass must neither recycle nor create, got %+v", got)
	}
	if s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]; s.Operation.RetryCount != 1 {
		t.Fatalf("RetryCount must not move while paced, got %d", s.Operation.RetryCount)
	}
}

// Restart Phase B recycles a Succeeded new-incarnation pod like a Failed
// one: the attempt keeps its bumped incarnation and rebuilds the name.
func TestRestart_RecyclesSucceededNewIncarnationPod(t *testing.T) {
	assertRestartRecyclesDeadPod(t, succeededPod)
}

// An evicted pod is a terminal pod with the Evicted reason, so Restart's
// rebuild reaps and recreates it at the same bumped incarnation.
func TestRestart_RecyclesEvictedNewIncarnationPod(t *testing.T) {
	assertRestartRecyclesDeadPod(t, evictedPod)
}

// assertRestartRecyclesDeadPod drives a Restart attempt whose bumped-
// incarnation pod died in the way kill marks it: the dead occupant is
// deleted while the attempt stays in flight, and the next pass rebuilds
// the name at the same incarnation.
func assertRestartRecyclesDeadPod(t *testing.T, kill func(*corev1.Pod) *corev1.Pod) {
	t.Helper()
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
	dead := kill(podAtIncarnation(isvc, 0, 2, false, false))
	c := newFakeClient(t, isvc, ir, dead)
	deps := workload.Deps{Client: c}

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	done, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart pass 1: %v", err)
	}
	if done || podExists(c, dead) {
		t.Fatalf("Restart must delete the dead pod and stay in flight (done=%v exists=%v)", done, podExists(c, dead))
	}
	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceRestarting || s.Incarnation != 2 ||
		s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationRestart || s.Operation.RetryCount != 1 {
		t.Fatalf("status after recycle: %+v", s)
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

// A Failed row with no preserved operation is a fresh start for Create:
// the dead pods holding the index's names are recycled and the row is
// committed as a new Create attempt.
func TestCreate_FreshStartRecyclesTerminalPodOnFailedRow(t *testing.T) {
	for _, tc := range []struct {
		name string
		kill func(*corev1.Pod) *corev1.Pod
	}{
		{name: "failed pod", kill: rejectedPod},
		{name: "succeeded pod", kill: succeededPod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetExpectations(t)
			isvc := minimalISVC("llama-70b", "prod", 1)
			ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
				Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceFailed,
				RunningRevision: "llama-70b-engine-" + testRevisionHash,
			})
			dead := tc.kill(podForInstance(isvc, 0, false, false))
			c := newFakeClient(t, isvc, ir, dead)
			deps := workload.Deps{Client: c}
			plan := buildPlanSinglePodEngine(1)

			input := buildTestInput(isvc, c, workload.ComponentEngine)
			if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
				t.Fatalf("Create pass 1: %v", err)
			}
			if podExists(c, dead) {
				t.Fatalf("the dead occupant of %s must be recycled on the fresh start", dead.Name)
			}
			s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
			if s.Phase != v1beta1.OMENativeInstanceCreating || s.Operation == nil ||
				s.Operation.Type != v1beta1.InstanceOperationCreate {
				t.Fatalf("the Failed row must be committed as a Create attempt, got %+v", s)
			}

			workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
			input = buildTestInput(isvc, c, workload.ComponentEngine)
			if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
				t.Fatalf("Create pass 2: %v", err)
			}
			got := listPods(t, c, "prod")
			if len(got) != 1 || got[0].Name != dead.Name || query.IsTerminalPod(&got[0]) {
				t.Fatalf("expected exactly the rebuilt %s, got %+v", dead.Name, got)
			}
		})
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

// TestCreate_TerminalMemberRecycledWhileGangBlocked: a gang whose
// PodGroup name is unusable withholds NEW members, but a dead one still
// occupying its stable name must be cleared regardless — the group's
// phase is derived from its members, so the name cannot be rebuilt
// around one, and nothing else in the pass would remove it.
func TestCreate_TerminalMemberRecycledWhileGangBlocked(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	dead := rejectedPod(podForInstance(isvc, 0, false, false))
	c := newFakeClient(t, isvc, dead)
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}
	plan := buildPlanSinglePodEngine(1)

	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.Gangs = workload.NewGangObservations()
	input.Gangs.Record(0, workload.GangObservation{
		Name:    "llama-70b-engine-0",
		State:   workload.GangStateFailed,
		Message: "PodGroup llama-70b-engine-0 reported phase Failed",
	})
	if !input.Gangs.BlocksPods(0) {
		t.Fatalf("fixture: the gang must be blocking new members")
	}

	if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if podExists(c, dead) {
		t.Fatalf("terminal member %s must be recycled even while its gang is blocked", dead.Name)
	}
	if got := listPods(t, c, "prod"); len(got) != 0 {
		t.Fatalf("no member may be created against an unusable gang name, got %d pod(s)", len(got))
	}
}

// A terminal pod re-observed after the fresh start has claimed the index
// writes no phase: the Create attempt owns the row now, and the failure
// this phase reports is already on LastFailure. The row must not fall
// back to Failed because the same dead object is seen again.
func TestCreate_TerminalPodReobservedAfterTheFreshStartWritesNoPhase(t *testing.T) {
	for _, tc := range []struct {
		name string
		kill func(*corev1.Pod) *corev1.Pod
	}{
		{name: "failed pod", kill: rejectedPod},
		{name: "succeeded pod", kill: succeededPod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetExpectations(t)
			isvc := minimalISVC("llama-70b", "prod", 1)
			ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
				Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceFailed,
				RunningRevision: "llama-70b-engine-" + testRevisionHash,
			})
			dead := tc.kill(podForInstance(isvc, 0, false, false))
			c := newFakeClient(t, isvc, ir, dead)
			deps := workload.Deps{Client: c}
			plan := buildPlanSinglePodEngine(1)

			input := buildTestInput(isvc, c, workload.ComponentEngine)
			if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
				t.Fatalf("Create pass 1: %v", err)
			}
			claimed := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
			if claimed.Phase != v1beta1.OMENativeInstanceCreating || claimed.Operation == nil {
				t.Fatalf("the fresh start must claim the row, got %+v", claimed)
			}

			// The same dead object is observed once more, after the stamp.
			reobserved := tc.kill(podForInstance(isvc, 0, false, false))
			if err := c.Create(context.Background(), reobserved); err != nil {
				t.Fatalf("re-seed the terminal pod: %v", err)
			}
			workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
			input = buildTestInput(isvc, c, workload.ComponentEngine)
			if _, err := ops.Create(context.Background(), deps, input, plan, nil); err != nil {
				t.Fatalf("Create pass 2: %v", err)
			}

			after := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
			if after.Phase != v1beta1.OMENativeInstanceCreating {
				t.Errorf("Phase: got %q want Creating; a re-observed terminal phase writes none", after.Phase)
			}
			if after.Operation == nil || after.Operation.Type != v1beta1.InstanceOperationCreate {
				t.Errorf("Operation: got %+v want the Create attempt preserved", after.Operation)
			}
			if !reflect.DeepEqual(after.LastFailure, claimed.LastFailure) {
				t.Errorf("LastFailure: got %+v want it unchanged at %+v", after.LastFailure, claimed.LastFailure)
			}
		})
	}
}

// The recycle writes no phase and no step, so the same terminal pod
// observed again lands on a row that still reads Restarting at the same
// step — the pass either paces the next recycle on the ladder or finds
// the delete it already issued is enough.
func TestRestart_TerminalPodReobservedKeepsPhaseAndStep(t *testing.T) {
	for _, tc := range []struct {
		name string
		kill func(*corev1.Pod) *corev1.Pod
	}{
		{name: "failed pod", kill: rejectedPod},
		{name: "succeeded pod", kill: succeededPod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetExpectations(t)
			isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
			ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
				Index:           0,
				Incarnation:     2,
				Phase:           v1beta1.OMENativeInstanceRestarting,
				RunningRevision: "llama-70b-engine-" + testRevisionHash,
				Operation: &v1beta1.InstanceOperation{
					ID: "restart-0-1", Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
					StartedAt: metav1.Now(), LastProgressAt: metav1.Now(),
					Deadline: metav1.NewTime(time.Now().Add(time.Hour)),
				},
			}
			dead := tc.kill(podAtIncarnation(isvc, 0, 2, false, false))
			c := newFakeClient(t, isvc, ir, dead)
			deps := workload.Deps{Client: c}

			input := buildTestInput(isvc, c, workload.ComponentEngine)
			plan := buildPlanSinglePodEngineForRestart(c, isvc)
			if _, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "trigger"); err != nil {
				t.Fatalf("Restart pass 1: %v", err)
			}
			recycled := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]

			// The same dead object is observed once more.
			reobserved := tc.kill(podAtIncarnation(isvc, 0, 2, false, false))
			if err := c.Create(context.Background(), reobserved); err != nil {
				t.Fatalf("re-seed the terminal pod: %v", err)
			}
			workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)
			input = buildTestInput(isvc, c, workload.ComponentEngine)
			plan = buildPlanSinglePodEngineForRestart(c, isvc)
			done, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], "trigger")
			if err != nil {
				t.Fatalf("Restart pass 2: %v", err)
			}
			if done {
				t.Errorf("done: got true want false; a terminal pod is not a finished repair")
			}

			after := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
			if after.Phase != recycled.Phase {
				t.Errorf("Phase: got %q want %q unchanged", after.Phase, recycled.Phase)
			}
			if after.Operation == nil || recycled.Operation == nil ||
				after.Operation.Step != recycled.Operation.Step ||
				after.Operation.ID != recycled.Operation.ID {
				t.Errorf("Operation: got %+v want the same attempt at the same step as %+v", after.Operation, recycled.Operation)
			}
			if after.Incarnation != recycled.Incarnation {
				t.Errorf("Incarnation: got %d want %d unchanged", after.Incarnation, recycled.Incarnation)
			}
		})
	}
}

// Terminal pods (phase Failed or Succeeded) still occupy their stable
// names but never run again. Every presence and liveness decision must
// treat them as absent; the tests below pin that at the detector, the
// Create diff, and Restart's Phase B.

// gangPodsWithPhases returns one placeholder pod per phase, in order.
// An empty phase is a live, not-yet-observed pod.
func gangPodsWithPhases(phases ...corev1.PodPhase) []*corev1.Pod {
	pods := make([]*corev1.Pod, 0, len(phases))
	for i, phase := range phases {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("gang-pod-%d", i), Namespace: "prod",
		}}
		pod.Status.Phase = phase
		pods = append(pods, pod)
	}
	return pods
}

func gangInstancePlan(size int) workload.InstancePlan {
	inst := workload.InstancePlan{Index: 0, Incarnation: 74}
	for i := 0; i < size; i++ {
		inst.Runners = append(inst.Runners, workload.RunnerPlan{Name: fmt.Sprintf("r%d", i), Size: 1})
	}
	return inst
}

func TestDetectRestartTrigger_TerminalPodsAreAbsent(t *testing.T) {
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, RestartPolicy: workload.RestartPolicyRecreateInstance}
	createOwned := workload.InstanceStatus{
		Index: 0, Incarnation: 74, Phase: workload.InstancePhaseCreating, PodCount: 2,
		RunningRevision: gangLossRevision, TargetRevision: gangLossRevision,
		Operation: &workload.InstanceOperation{Type: workload.InstanceOperationCreate, Step: "CreatePods"},
	}
	ready := workload.InstanceStatus{Index: 0, Incarnation: 74, Phase: workload.InstancePhaseReady, PodCount: 2, RunningRevision: gangLossRevision}

	cases := []struct {
		name       string
		status     workload.InstanceStatus
		size       int
		pods       []*corev1.Pod
		want       bool
		wantReason string
	}{{
		// Below Ready only member loss fires; a Failed member is a lost member.
		name:   "forming gang with a failed member is member loss",
		status: createOwned, size: 2,
		pods: gangPodsWithPhases(corev1.PodFailed, corev1.PodRunning),
		want: true, wantReason: "gang member lost: 1 of 2 pods present",
	}, {
		name:   "forming gang with a succeeded member is member loss",
		status: createOwned, size: 2,
		pods: gangPodsWithPhases(corev1.PodSucceeded, corev1.PodPending),
		want: true, wantReason: "gang member lost",
	}, {
		// Every member terminal is total loss: Create's fresh-start path owns it.
		name:   "forming gang with every member terminal stays with create",
		status: createOwned, size: 2,
		pods: gangPodsWithPhases(corev1.PodFailed, corev1.PodFailed),
		want: false,
	}, {
		name:   "forming gang with live members is silent",
		status: createOwned, size: 2,
		pods: gangPodsWithPhases(corev1.PodPending, corev1.PodRunning),
		want: false,
	}, {
		// A Succeeded pod carries no failure detail; the count check catches it.
		name:   "ready instance with a succeeded pod is below desired",
		status: ready, size: 1,
		pods: gangPodsWithPhases(corev1.PodSucceeded),
		want: true, wantReason: "pod count 0 below desired 1",
	}, {
		name:   "ready gang with a failed member names the failure",
		status: ready, size: 2,
		pods: gangPodsWithPhases(corev1.PodRunning, corev1.PodFailed),
		want: true, wantReason: "Failed",
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := gangLossInput(tc.status)
			needs, reason := ops.DetectRestartTriggerWithPods(input, plan, gangInstancePlan(tc.size), tc.pods)
			if needs != tc.want {
				t.Fatalf("needsRestart = %v (reason %q), want %v", needs, reason, tc.want)
			}
			if needs && !strings.Contains(reason, tc.wantReason) {
				t.Fatalf("reason = %q, want it to contain %q", reason, tc.wantReason)
			}
		})
	}
}

// A Pending row is a demoted Ready row: its recorded pod count is the
// proof the gang was once complete, so a terminal member leaves it below
// desired with a survivor and the rebuild fires. A single-pod Pending row
// has no survivor and stays with Create.
func TestDetectRestartTrigger_PendingRowWithTerminalMember(t *testing.T) {
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, RestartPolicy: workload.RestartPolicyRecreateInstance}
	pending := func(podCount int32) workload.InstanceStatus {
		return workload.InstanceStatus{
			Index: 0, Incarnation: 74, Phase: workload.InstancePhasePending, PodCount: podCount,
			RunningRevision: gangLossRevision, TargetRevision: gangLossRevision,
		}
	}
	cases := []struct {
		name string
		size int
		pods []*corev1.Pod
		want bool
	}{{
		name: "failed member leaves a survivor", size: 2,
		pods: gangPodsWithPhases(corev1.PodFailed, corev1.PodRunning),
		want: true,
	}, {
		name: "succeeded member leaves a survivor", size: 2,
		pods: gangPodsWithPhases(corev1.PodSucceeded, corev1.PodRunning),
		want: true,
	}, {
		name: "single-pod row has no survivor", size: 1,
		pods: gangPodsWithPhases(corev1.PodFailed),
		want: false,
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := gangLossInput(pending(int32(tc.size)))
			needs, reason := ops.DetectRestartTriggerWithPods(input, plan, gangInstancePlan(tc.size), tc.pods)
			if needs != tc.want {
				t.Fatalf("needsRestart = %v (reason %q), want %v", needs, reason, tc.want)
			}
			if needs && !strings.Contains(reason, "gang member lost") {
				t.Fatalf("reason must name the loss; got %q", reason)
			}
		})
	}
}

// A pod that died at admission is a missing target: Create commits the
// Instance as Creating for it. No live pod can appear while the dead one
// still occupies the stable name.
func TestCreate_TerminalPodIsMissing(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	dead := podForInstance(isvc, 0, false, false)
	dead.Status.Phase = corev1.PodFailed
	dead.Status.Reason = "UnexpectedAdmissionError"
	c := newFakeClient(t, isvc, dead)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngine(1)

	result, err := ops.Create(context.Background(), workload.Deps{Client: c}, input, plan, nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("expected a requeue while the terminal pod occupies the name, got %+v", result)
	}
	insts := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)
	if len(insts) != 1 || insts[0].Phase != v1beta1.OMENativeInstanceCreating {
		t.Fatalf("Instance must be committed as Creating for its missing pod, got %+v", insts)
	}
	if insts[0].Operation == nil || insts[0].Operation.Type != v1beta1.InstanceOperationCreate {
		t.Fatalf("Operation: got %+v want a Create operation", insts[0].Operation)
	}
	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	for i := range pods.Items {
		if !query.IsTerminalPod(&pods.Items[i]) {
			t.Fatalf("no live pod may be created while %s is occupied by a terminal pod", pods.Items[i].Name)
		}
	}
}

// Restart Phase B/C: a new-incarnation pod that died is a missing target,
// never a Ready one — even when it still carries a stale ContainersReady
// condition. The Instance stays Restarting instead of promoting.
func TestRestart_TerminalNewIncarnationPodIsNotPromoted(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:           0,
		Incarnation:     2,
		Phase:           v1beta1.OMENativeInstanceRestarting,
		RunningRevision: "llama-70b-engine-" + testRevisionHash,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
		},
	}
	dead := podAtIncarnation(isvc, 0, 2, true /* stale ContainersReady */, false)
	dead.Status.Phase = corev1.PodFailed
	c := newFakeClient(t, isvc, ir, dead)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("Restart must not complete on a terminal new-incarnation pod")
	}
	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceRestarting || s.Operation == nil {
		t.Fatalf("Instance must stay Restarting with its operation, got %+v", s)
	}
	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	for i := range pods.Items {
		if !query.IsTerminalPod(&pods.Items[i]) {
			t.Fatalf("no live pod may be created while %s is occupied by a terminal pod", pods.Items[i].Name)
		}
	}
}
