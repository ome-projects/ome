package ops_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// isvcReadyAtIncarnation builds an ISVC whose InstanceStatus for engine
// index 0 is Phase=Ready at the given Incarnation — the steady-state from
// which a restart trigger fires.
func isvcReadyAtIncarnation(name, ns string, incarnation int64) (*v1beta1.InferenceService, *v1beta1.InferenceReplica) {
	isvc := minimalISVC(name, ns, 1)
	ir := instanceIR(isvc, workload.ComponentEngine,
		v1beta1.OMENativeInstanceStatus{Index: 0, Incarnation: incarnation, Phase: v1beta1.OMENativeInstanceReady},
	)
	return isvc, ir
}

// podAtIncarnation extends podForInstance to stamp a specific incarnation
// label. Restart pods at the bumped incarnation are distinguished from old
// pods by this label.
func podAtIncarnation(isvc *v1beta1.InferenceService, instanceIdx int32, incarnation int64, ready, serving bool) *corev1.Pod {
	pod := podForInstance(isvc, instanceIdx, ready, serving)
	pod.Labels[query.LabelInstanceIncarnation] = fmt.Sprintf("%d", incarnation)
	return pod
}

// sliceWithEndpoint constructs one routed-service endpoint for a restart Pod.
func sliceWithEndpoint(namespace, sliceName, serviceName string, pod *corev1.Pod, ready bool) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sliceName,
			Namespace: namespace,
			Labels:    map[string]string{discoveryv1.LabelServiceName: serviceName},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(ready)},
			TargetRef: &corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: pod.Namespace,
				Name:      pod.Name,
			},
		}},
	}
}

// buildPlanSinglePodEngineForRestart is the per-Restart-test plan builder.
// Same shape as buildPlanSinglePodEngine in create_test.go but reads the
// incarnation from the existing InstanceStatus so subsequent passes drive
// the post-bump value rather than re-stamping 1.
func buildPlanSinglePodEngineForRestart(c client.Client, isvc *v1beta1.InferenceService) workload.ComponentPlan {
	plan := buildPlanSinglePodEngine(1)
	for _, s := range instanceStatusesOnIR(c, isvc, workload.ComponentEngine) {
		if s.Index == 0 && s.Incarnation > 0 {
			plan.Instances[0].Incarnation = s.Incarnation
			break
		}
	}
	return plan
}

func TestRestart_NilClient(t *testing.T) {
	resetExpectations(t)
	plan := workload.ComponentPlan{Component: workload.ComponentEngine}
	inst := workload.InstancePlan{Index: 0, Incarnation: 1}
	if _, err := ops.Restart(context.Background(), workload.Deps{}, workload.ReconcileInput{}, plan, inst, ""); err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestRestart_FirstPassBumpsIncarnationAndPatchesStatus(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := podAtIncarnation(isvc, 0, 1, true /* ready */, true /* serving */)
	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "test trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false on first pass (drain not converged)")
	}

	// Status should now be Phase=Restarting, Incarnation=2.
	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("Phase: got %q want Restarting", s.Phase)
	}
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2 (bumped from 1)", s.Incarnation)
	}
	if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("Operation: %+v", s.Operation)
	}
	if s.Operation.Reason != "test trigger" {
		t.Errorf("Operation.Reason: got %q want %q", s.Operation.Reason, "test trigger")
	}
	if s.Operation.Step != "Drain" {
		t.Errorf("Operation.Step: got %q want Drain", s.Operation.Step)
	}
}

func TestRestart_DrainOldPod_FlipsServingFalse(t *testing.T) {
	// Pre-bumped status (Restarting at Inc=2); old pod at Inc=1 still has
	// serving=True. Restart should flip serving=False and requeue while
	// EndpointSlice still publishes the pod as Ready.
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
		},
	}
	pod := podAtIncarnation(isvc, 0, 1, true, true)
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, pod, true /* still Ready */)
	c := newFakeClient(t, isvc, ir, pod, slice)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while drain hasn't converged")
	}

	got := &corev1.Pod{}
	_ = c.Get(context.Background(), client.ObjectKeyFromObject(pod), got)
	if podreadiness.IsServing(got) {
		t.Errorf("pod %s ome.io/serving should be False, got %+v", got.Name, got.Status.Conditions)
	}
}

func TestRestart_DeletesOldPodOnceDrained(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "x",
		},
	}
	pod := podAtIncarnation(isvc, 0, 1, true, false /* already drained */)
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, pod, false /* Ready=false */)
	c := newFakeClient(t, isvc, ir, pod, slice)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after issuing delete (next pass creates new pod)")
	}

	// Old pod should be gone.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), got); err == nil {
		t.Errorf("old pod %s should be deleted", pod.Name)
	}
	if workload.DefaultExpectations.Satisfied("prod", "llama-70b", workload.ComponentEngine, 0) {
		t.Errorf("ExpectDeletes should record the in-flight delete")
	}
}

func TestRestart_RefusesToDeleteOrphanPod(t *testing.T) {
	// A pod under the OMENative selector but missing
	// ome.io/instance-incarnation is an orphan. Restart must emit
	// FoundOrphan and short-circuit rather than deleting it — the operator
	// re-labels or removes it manually.
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	orphan := podAtIncarnation(isvc, 0, 1, true, false)
	orphan.Name = "orphan-pod"
	delete(orphan.Labels, query.LabelInstanceIncarnation)
	c := newFakeClient(t, isvc, ir, orphan)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false: orphan must block Restart from advancing")
	}

	// Orphan pod must STILL exist.
	got := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(orphan), got); err != nil {
		t.Fatalf("orphan pod must still exist after Restart bails: %v", err)
	}
	if got.DeletionTimestamp != nil {
		t.Errorf("orphan pod must NOT have been deleted; got DeletionTimestamp=%v", got.DeletionTimestamp)
	}
}

func TestRestart_CreatesNewPodAtBumpedIncarnation(t *testing.T) {
	// Status is Restarting at Inc=2, no pods (old already deleted, new
	// not yet created). Restart should create the new pod at Inc=2.
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
	c := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "trigger")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false after creating new pod (not yet Ready)")
	}

	pods := &corev1.PodList{}
	_ = c.List(context.Background(), pods, client.InNamespace("prod"))
	if len(pods.Items) != 1 {
		t.Fatalf("pods: got %d want 1", len(pods.Items))
	}
	if got := pods.Items[0].Labels[query.LabelInstanceIncarnation]; got != "2" {
		t.Errorf("new pod %s incarnation label: got %q want 2", pods.Items[0].Name, got)
	}
}

func TestRestart_ConvergesAcrossPasses(t *testing.T) {
	// End-to-end: start with a Ready instance whose pod is Failed.
	// Trigger Restart and run it until done=true; assert the final state
	// has one new-incarnation pod, serving=True, and Phase=Ready.
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Pre-seed RunningRevision so Phase-B recreate has a hash to stamp.
	ir.Status.InstanceStatuses[0].RunningRevision = "llama-70b-engine-" + testRevisionHash
	failed := podAtIncarnation(isvc, 0, 1, true, true)
	failed.Status.Phase = corev1.PodFailed
	slice := sliceWithEndpoint("prod", "engine-svc-1", "llama-70b-engine-rev-"+testRevisionHash, failed, false /* drained */)
	c := newFakeClient(t, isvc, ir, failed, slice)

	const maxPasses = 8
	for pass := 0; pass < maxPasses; pass++ {
		// Re-read each pass: buildPlanSinglePodEngineForRestart needs current
		// status to pick up the new incarnation after Restart bumps it.
		fresh := &v1beta1.InferenceService{}
		_ = c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh)
		input := buildTestInput(fresh, c, workload.ComponentEngine)
		plan := buildPlanSinglePodEngineForRestart(c, fresh)

		// Fast-forward: simulate the fake watch observing every delete the
		// previous pass issued.
		workload.DefaultExpectations.Forget("prod", "llama-70b", workload.ComponentEngine, 0)

		// Synthesize ContainersReady on any new-incarnation pod the
		// previous pass created — the fake client doesn't run kubelet.
		makeNewPodReady(t, c, "prod", "llama-70b-engine-0-default-0", plan.Instances[0].Incarnation)

		done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod Failed")
		if err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if done {
			// Verify end state.
			s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
			if s.Phase != v1beta1.OMENativeInstanceReady {
				t.Errorf("final Phase: got %q want Ready", s.Phase)
			}
			if s.Incarnation != 2 {
				t.Errorf("final Incarnation: got %d want 2", s.Incarnation)
			}
			if s.Operation != nil {
				t.Errorf("final Operation: want nil, got %+v", s.Operation)
			}
			return
		}
	}
	t.Fatalf("Restart did not converge after %d passes", maxPasses)
}

// makeNewPodReady is a test helper that synthesizes ContainersReady=True
// on the named pod if it exists and carries the given incarnation label.
// It mimics what kubelet would write once the runtime is up.
func makeNewPodReady(t *testing.T, c client.Client, ns, name string, incarnation int64) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, pod); err != nil {
		return
	}
	if got := pod.Labels[query.LabelInstanceIncarnation]; got != fmt.Sprintf("%d", incarnation) {
		return
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.ContainersReady && cond.Status == corev1.ConditionTrue {
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.ContainersReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	})
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("synthesize ContainersReady: %v", err)
	}
}

// failedPodOOM builds a single-pod-engine pod at the given incarnation
// that is Phase=Failed with an OOMKilled-terminated main container.
func failedPodOOM(isvc *v1beta1.InferenceService, incarnation int64) *corev1.Pod {
	pod := podAtIncarnation(isvc, 0, incarnation, true /* ready */, true /* serving */)
	pod.Status.Phase = corev1.PodFailed
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "main",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason:   "OOMKilled",
			ExitCode: 137,
			Message:  "container killed due to memory limit",
		}},
	}}
	return pod
}

// Restart on a failed pod must preserve the pod's container-termination
// diagnostics into InstanceStatus.LastFailure on the FIRST pass — BEFORE
// Phase A drains and deletes the pod, so the trace survives the recreate.
// This is the core debuggability fix: a gang that keeps restarting now
// leaves a durable failure record instead of vanishing.
func TestRestart_CapturesLastFailureBeforeDrain(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	// Pre-seed RunningRevision so the Restart path has a hash later.
	ir.Status.InstanceStatuses[0].RunningRevision =
		"llama-70b-engine-" + testRevisionHash
	failed := failedPodOOM(isvc, 1)
	c := newFakeClient(t, isvc, ir, failed)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod Failed"); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.LastFailure == nil {
		t.Fatalf("LastFailure: nil, want OOMKilled diagnostics captured before drain")
	}
	if s.LastFailure.PodName != failed.Name {
		t.Errorf("LastFailure.PodName: got %q want %q", s.LastFailure.PodName, failed.Name)
	}
	if s.LastFailure.ContainerName != "main" {
		t.Errorf("LastFailure.ContainerName: got %q want main", s.LastFailure.ContainerName)
	}
	if s.LastFailure.Reason != "OOMKilled" {
		t.Errorf("LastFailure.Reason: got %q want OOMKilled", s.LastFailure.Reason)
	}
	if s.LastFailure.ExitCode == nil || *s.LastFailure.ExitCode != 137 {
		t.Errorf("LastFailure.ExitCode: got %v want 137", s.LastFailure.ExitCode)
	}
}

// A "pod count below desired" restart (the pod already vanished) has no
// pod to read diagnostics from and must NOT clobber a previously-recorded
// LastFailure — the most recent genuine failure trace is preserved.
func TestRestart_PodVanished_PreservesPriorLastFailure(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	s0 := &ir.Status.InstanceStatuses[0]
	s0.RunningRevision = "llama-70b-engine-" + testRevisionHash
	exit := int32(1)
	s0.LastFailure = &v1beta1.InstanceTermination{
		PodName:  "llama-70b-engine-0-default-0",
		Reason:   "Error",
		ExitCode: &exit,
	}
	// No pods exist → "pod count below desired" trigger.
	c := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], "pod count 0 below desired 1"); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.LastFailure == nil || s.LastFailure.Reason != "Error" {
		t.Fatalf("prior LastFailure must be preserved when no pod yields a fresh signal; got %+v", s.LastFailure)
	}
}

// DetectRestartTrigger must return a rich, operator-readable reason for a
// failed pod (container + terminated reason + exit code), not the bare
// "pod X Failed" — and the Restart event carries it.
func TestDetectRestartTrigger_RichReasonForFailedPod(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	failed := failedPodOOM(isvc, 1)
	c := newFakeClient(t, isvc, ir, failed)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	needs, reason, err := ops.DetectRestartTrigger(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0])
	if err != nil {
		t.Fatalf("DetectRestartTrigger: %v", err)
	}
	if !needs {
		t.Fatalf("expected needsRestart=true for a Failed pod")
	}
	if !strings.Contains(reason, "OOMKilled") || !strings.Contains(reason, "137") {
		t.Errorf("reason must name the termination cause; got %q", reason)
	}
}

// The RestartTriggered Warning event must carry the rich reason so the
// cause is visible in `kubectl describe` even after the pod is gone.
func TestRestart_EmitsRichRestartTriggeredEvent(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision =
		"llama-70b-engine-" + testRevisionHash
	failed := failedPodOOM(isvc, 1)
	c := newFakeClient(t, isvc, ir, failed)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	rec := record.NewFakeRecorder(16)

	// Pass the rich reason (as the dispatcher would, via DetectRestartTrigger).
	_, reason, _ := ops.DetectRestartTrigger(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0])
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c, Recorder: rec}, input, plan, plan.Instances[0], reason); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	var events []string
	for drained := false; !drained; {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			drained = true
		}
	}
	found := false
	for _, e := range events {
		if strings.Contains(e, string(workload.EventReasonRestartTriggered)) && strings.Contains(e, "OOMKilled") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a RestartTriggered event naming OOMKilled; got %v", events)
	}
}

// Gang-member loss below Phase=Ready.
//
// Under RecreateInstance only Restart bumps the Incarnation, and only the
// bump drains the survivors. A gang that loses a member before it reaches
// Ready therefore needs an owner: if restart detection returned on the
// phase alone, Create's backfill would re-materialize the missing pod at
// the unchanged Incarnation, leaving the survivors holding a topology
// domain the replacement cannot enter. These cases pin the boundary
// between that loss and an Instance that is merely slow to form.

// gangLossRevision is the revision the wedged Instance is both running
// and converging toward — the shape that leaves the update pass nothing
// to adopt.
const gangLossRevision = "engine-c4e45f68"

// gangLossInput builds the ReconcileInput the detector reads: one
// InstanceStatus for index 0 plus any retry blocks.
func gangLossInput(s workload.InstanceStatus, blocks ...workload.RetryBlock) workload.ReconcileInput {
	return workload.ReconcileInput{
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: []workload.InstanceStatus{s},
			RetryBlocks:      blocks,
			UpdateRevision:   gangLossRevision,
		},
	}
}

// gangLossPods returns n placeholder pods. Only the count is read — the
// predicate deliberately ignores readiness, container state and node
// assignment.
func gangLossPods(n int) []*corev1.Pod {
	pods := make([]*corev1.Pod, 0, n)
	for i := 0; i < n; i++ {
		pods = append(pods, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("gang-pod-%d", i), Namespace: "prod",
		}})
	}
	return pods
}

func TestDetectRestartTrigger_GangMemberLossAfterPodCountRefresh(t *testing.T) {
	status := workload.InstanceStatus{
		Index:           0,
		Incarnation:     74,
		Phase:           workload.InstancePhaseCreating,
		PodCount:        2,
		RunningRevision: gangLossRevision,
		TargetRevision:  gangLossRevision,
		Operation: &workload.InstanceOperation{
			Type: workload.InstanceOperationCreate,
			Step: "CreatePods",
		},
	}
	plan := workload.ComponentPlan{
		Component:     workload.ComponentEngine,
		RestartPolicy: workload.RestartPolicyRecreateInstance,
	}
	inst := workload.InstancePlan{Index: 0, Incarnation: 74, Runners: []workload.RunnerPlan{
		{Name: "leader", Size: 1},
		{Name: "worker", Size: 1},
	}}

	input := gangLossInput(status)
	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, inst, gangLossPods(2)); needs {
		t.Fatalf("complete gang unexpectedly needs restart: %q", reason)
	}

	// Publication derives PodCount from the current Pod list. The CreatePods
	// operation remains the durable proof that materialization was committed.
	status.PodCount = 1
	input = gangLossInput(status)
	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, inst, gangLossPods(1))
	if !needs {
		t.Fatal("member loss must survive the PodCount refresh from 2 to 1")
	}
	if !strings.Contains(reason, "gang member lost") {
		t.Fatalf("restart reason = %q, want gang member loss", reason)
	}
}

func TestDetectRestartTrigger_GangMemberLossBelowReady(t *testing.T) {
	// The production shape: a Create-owned Instance at a frozen incarnation
	// whose RunningRevision already equals its target.
	createOwned := func(phase workload.InstancePhase, podCount int32) workload.InstanceStatus {
		return workload.InstanceStatus{
			Index:           0,
			Incarnation:     74,
			Phase:           phase,
			PodCount:        podCount,
			RunningRevision: gangLossRevision,
			TargetRevision:  gangLossRevision,
			Operation:       &workload.InstanceOperation{Type: workload.InstanceOperationCreate, Step: "CreatePods"},
		}
	}

	cases := []struct {
		name     string
		status   workload.InstanceStatus
		live     int
		expected int32
		policy   workload.RestartPolicy
		blocks   []workload.RetryBlock
		want     bool
	}{{
		// Both members present but never Ready — indistinguishable from a
		// gang that simply takes hours to load weights. Must not fire.
		name:   "materialized and complete is silent",
		status: createOwned(workload.InstancePhaseCreating, 2),
		live:   2, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: false,
	}, {
		name:   "materialized then partial fires",
		status: createOwned(workload.InstancePhaseCreating, 2),
		live:   1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: true,
	}, {
		// Failed proves the attempt ran even after publication refreshes the
		// current count and the completed operation has been cleared.
		name: "failed with no operation survives count refresh",
		status: workload.InstanceStatus{
			Index: 0, Incarnation: 74, Phase: workload.InstancePhaseFailed, PodCount: 1,
			RunningRevision: gangLossRevision, TargetRevision: gangLossRevision,
		},
		live: 1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: true,
	}, {
		name:   "committed birth with one survivor fires",
		status: createOwned(workload.InstancePhaseCreating, 0),
		live:   1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: true,
	}, {
		name:   "current published count does not erase commit",
		status: createOwned(workload.InstancePhaseCreating, 1),
		live:   1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: true,
	}, {
		name: "legacy incomplete status is not proven materialized",
		status: workload.InstanceStatus{
			Index: 0, Incarnation: 74, Phase: workload.InstancePhaseCreating, PodCount: 1,
			RunningRevision: gangLossRevision, TargetRevision: gangLossRevision,
		},
		live: 1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: false,
	}, {
		name: "legacy complete status uses count fallback",
		status: workload.InstanceStatus{
			Index: 0, Incarnation: 74, Phase: workload.InstancePhaseCreating, PodCount: 2,
			RunningRevision: gangLossRevision, TargetRevision: gangLossRevision,
		},
		live: 1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: true,
	}, {
		name: "unknown create step retains legacy complete-count proof",
		status: func() workload.InstanceStatus {
			status := createOwned(workload.InstancePhaseCreating, 2)
			status.Operation.Step = ""
			return status
		}(),
		live: 1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: true,
	}, {
		name: "unknown create step with incomplete count is not a commit marker",
		status: func() workload.InstanceStatus {
			status := createOwned(workload.InstancePhaseCreating, 1)
			status.Operation.Step = ""
			return status
		}(),
		live: 1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: false,
	}, {
		// Nothing survives to strand a replacement; Create's fresh-start
		// path owns this and honors the RetryBlock.
		name:   "total loss stays with create",
		status: createOwned(workload.InstancePhaseCreating, 2),
		live:   0, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: false,
	}, {
		name:   "single-pod instance is out of scope",
		status: createOwned(workload.InstancePhaseCreating, 1),
		live:   0, expected: 1, policy: workload.RestartPolicyRecreateInstance,
		want: false,
	}, {
		name: "update-owned churn is not loss",
		status: workload.InstanceStatus{
			Index: 0, Incarnation: 74, Phase: workload.InstancePhaseUpdating, PodCount: 2,
			RunningRevision: gangLossRevision,
			Operation:       &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, Step: "Drain"},
		},
		live: 1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: false,
	}, {
		// A Restart attempt parked at Failed is spent, not in flight: only a
		// new Restart can rebuild the gang as a whole, so it re-arms.
		name: "spent restart attempt at failed re-arms",
		status: workload.InstanceStatus{
			Index: 0, Incarnation: 74, Phase: workload.InstancePhaseFailed, PodCount: 2,
			RunningRevision: gangLossRevision,
			Operation:       &workload.InstanceOperation{Type: workload.InstanceOperationRestart, Step: "Drain"},
		},
		live: 1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: true,
	}, {
		// A preserved Update operation keeps the update pass as owner even
		// at Failed (its abandon continuation consumes it).
		name: "failed with preserved update operation stays update-owned",
		status: workload.InstanceStatus{
			Index: 0, Incarnation: 74, Phase: workload.InstancePhaseFailed, PodCount: 2,
			RunningRevision: gangLossRevision,
			Operation:       &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, Step: "Drain"},
		},
		live: 1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: false,
	}, {
		name: "migrate-owned is suppressed",
		status: workload.InstanceStatus{
			Index: 0, Incarnation: 74, Phase: workload.InstancePhaseMigrating, PodCount: 2,
			RunningRevision: gangLossRevision,
		},
		live: 1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: false,
	}, {
		name:   "deleting is teardown",
		status: createOwned(workload.InstancePhaseDeleting, 2),
		live:   1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		want: false,
	}, {
		name:   "other restart policies keep create's self-heal",
		status: createOwned(workload.InstancePhaseCreating, 2),
		live:   1, expected: 2, policy: workload.RestartPolicyNone,
		want: false,
	}, {
		// Rebuilding would re-materialize a revision the disposition held.
		name:   "held revision is not rebuilt",
		status: createOwned(workload.InstancePhaseCreating, 2),
		live:   1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		blocks: []workload.RetryBlock{{TargetRevision: gangLossRevision, State: workload.RetryBlockHeld}},
		want:   false,
	}, {
		name:   "backoff on an unrelated revision does not block",
		status: createOwned(workload.InstancePhaseCreating, 2),
		live:   1, expected: 2, policy: workload.RestartPolicyRecreateInstance,
		blocks: []workload.RetryBlock{{TargetRevision: "other-revision", State: workload.RetryBlockHeld}},
		want:   true,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := gangLossInput(tc.status, tc.blocks...)
			plan := workload.ComponentPlan{Component: workload.ComponentEngine, RestartPolicy: tc.policy}
			inst := workload.InstancePlan{Index: 0, Incarnation: tc.status.Incarnation}
			for i := int32(0); i < tc.expected; i++ {
				inst.Runners = append(inst.Runners, workload.RunnerPlan{Name: fmt.Sprintf("r%d", i), Size: 1})
			}

			needs, reason := ops.DetectRestartTriggerWithPods(input, plan, inst, gangLossPods(tc.live))
			if needs != tc.want {
				t.Fatalf("needsRestart = %v (reason %q), want %v", needs, reason, tc.want)
			}
			if needs && !strings.Contains(reason, "gang member lost") {
				t.Errorf("reason must name the loss; got %q", reason)
			}
		})
	}
}

// A slow gang must stay untouched no matter how long it has been forming:
// the predicate reads no clock, so time cannot make it fire.
func TestDetectRestartTrigger_SlowGangNeverRecycled(t *testing.T) {
	status := workload.InstanceStatus{
		Index: 0, Incarnation: 74, Phase: workload.InstancePhaseCreating, PodCount: 2,
		RunningRevision: gangLossRevision, TargetRevision: gangLossRevision,
		Operation: &workload.InstanceOperation{
			Type: workload.InstanceOperationCreate, Step: "CreatePods",
			StartedAt: metav1.NewTime(time.Now().Add(-72 * time.Hour)),
			Deadline:  metav1.NewTime(time.Now().Add(-48 * time.Hour)),
		},
	}
	input := gangLossInput(status)
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, RestartPolicy: workload.RestartPolicyRecreateInstance}
	inst := workload.InstancePlan{Index: 0, Incarnation: 74, Runners: []workload.RunnerPlan{
		{Name: "leader", Size: 1}, {Name: "worker", Size: 1},
	}}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, inst, gangLossPods(2)); needs {
		t.Fatalf("a complete gang past its deadline must not restart; got reason %q", reason)
	}
}

// A Migrating Instance whose Operation has not been stamped yet must
// still be suppressed: Create consults the predicate directly, without
// the trigger's isMigrateOwnedStatus check in front of it.
func TestDetectRestartTrigger_MigratingPhaseWithoutOperation(t *testing.T) {
	status := workload.InstanceStatus{
		Index: 0, Incarnation: 74, Phase: workload.InstancePhaseMigrating, PodCount: 2,
		RunningRevision: gangLossRevision,
	}
	input := gangLossInput(status)
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, RestartPolicy: workload.RestartPolicyRecreateInstance}
	inst := workload.InstancePlan{Index: 0, Incarnation: 74, Runners: []workload.RunnerPlan{
		{Name: "leader", Size: 1}, {Name: "worker", Size: 1},
	}}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, inst, gangLossPods(1)); needs {
		t.Fatalf("a migrating Instance must not be recreated; got reason %q", reason)
	}
}

// runnerStatus builds the runner container status for post-Ready restart
// scenarios: startedAt is the current run's start; terminated, when
// non-nil, is the previous run's termination record.
func runnerStatus(name string, startedAt time.Time, terminated *corev1.ContainerStateTerminated) corev1.ContainerStatus {
	cs := corev1.ContainerStatus{
		Name:         name,
		RestartCount: 0,
		State: corev1.ContainerState{
			Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(startedAt)},
		},
	}
	if terminated != nil {
		cs.RestartCount = 1
		cs.LastTerminationState = corev1.ContainerState{Terminated: terminated}
	}
	return cs
}

// TestDetectRestartTrigger_RunnerRestartAfterReady is the regression lock
// for the in-place kubelet restart gap: a Ready multi-pod Instance whose
// runner container was restarted inside the same Pod UID (Pod present,
// phase Running) must trigger RecreateInstanceOnPodRestart even though no
// pod is missing or Failed.
func TestDetectRestartTrigger_RunnerRestartAfterReady(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	readySince := metav1.NewTime(time.Now().Add(-time.Hour))
	ir.Status.InstanceStatuses[0].ReadySince = &readySince

	pod := podForInstance(isvc, 0, true, true)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		runnerStatus(constants.MainContainerName, readySince.Add(10*time.Minute), &corev1.ContainerStateTerminated{
			Reason:     "OOMKilled",
			ExitCode:   137,
			FinishedAt: metav1.NewTime(readySince.Add(9 * time.Minute)),
		}),
	}

	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod})
	if !needs {
		t.Fatalf("expected restart trigger for a post-Ready in-place runner restart")
	}
	if !strings.Contains(reason, "OOMKilled") || !strings.Contains(reason, "137") {
		t.Errorf("reason must carry the termination cause; got %q", reason)
	}
}

// Boot-time restarts are forgiven: all restart evidence predates ReadySince
// (probe kills while loading weights), so the Ready Instance must not be
// recycled.
func TestDetectRestartTrigger_BootRestartsForgiven(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	readySince := metav1.NewTime(time.Now().Add(-time.Hour))
	ir.Status.InstanceStatuses[0].ReadySince = &readySince

	pod := podForInstance(isvc, 0, true, true)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		runnerStatus(constants.MainContainerName, readySince.Add(-5*time.Minute), &corev1.ContainerStateTerminated{
			Reason:     "Error",
			ExitCode:   1,
			FinishedAt: metav1.NewTime(readySince.Add(-6 * time.Minute)),
		}),
	}

	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("boot-time restarts must not trigger a recycle; got reason %q", reason)
	}
}

// Sidecar restarts never break the Instance's process group; only the
// runner container counts.
func TestDetectRestartTrigger_SidecarRestartIgnored(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	readySince := metav1.NewTime(time.Now().Add(-time.Hour))
	ir.Status.InstanceStatuses[0].ReadySince = &readySince

	pod := podForInstance(isvc, 0, true, true)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		runnerStatus(constants.MainContainerName, readySince.Add(-5*time.Minute), nil),
		runnerStatus(constants.ServingSidecarContainerName, readySince.Add(20*time.Minute), &corev1.ContainerStateTerminated{
			Reason:     "Error",
			ExitCode:   1,
			FinishedAt: metav1.NewTime(readySince.Add(19 * time.Minute)),
		}),
	}

	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("sidecar restart must not trigger a recycle; got reason %q", reason)
	}
}

// Instances promoted before ReadySince existed have no anchor; the trigger
// must stay silent rather than guess.
func TestDetectRestartTrigger_NilReadySinceStaysSilent(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)

	pod := podForInstance(isvc, 0, true, true)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		runnerStatus(constants.MainContainerName, time.Now(), &corev1.ContainerStateTerminated{
			Reason:     "Error",
			ExitCode:   1,
			FinishedAt: metav1.NewTime(time.Now().Add(-time.Minute)),
		}),
	}

	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("nil ReadySince must not trigger; got reason %q", reason)
	}
}
