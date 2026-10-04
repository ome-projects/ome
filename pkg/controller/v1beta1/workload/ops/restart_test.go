package ops_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
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
// the post-bump value rather than re-stamping 1, and carries the restart
// policy the pod-churn triggers answer to.
func buildPlanSinglePodEngineForRestart(c client.Client, isvc *v1beta1.InferenceService) workload.ComponentPlan {
	plan := buildPlanSinglePodEngine(1)
	plan.RestartPolicy = workload.RestartPolicyRecreateInstance
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
	if _, err := ops.Restart(context.Background(), workload.Deps{}, workload.ReconcileInput{}, plan, inst, nil, ""); err == nil {
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

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "test trigger")
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

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "trigger")
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

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "trigger")
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

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "trigger")
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
	c := newFakeClient(t, isvc, ir, fixtureRevision(t, isvc))
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "trigger")
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
	c := newFakeClient(t, isvc, ir, failed, slice, fixtureRevision(t, isvc))

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

		done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "pod Failed")
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

// makeNewPodReady is the fake kubelet a pass loop needs: on the named pod,
// if it exists and carries the given incarnation label, it synthesizes
// ContainersReady=True once the runtime is up and folds the controller's
// serving gate into PodReady=True once that gate is satisfied — the order
// kubelet writes them in, and the order the promote bar reads them in.
func makeNewPodReady(t *testing.T, c client.Client, ns, name string, incarnation int64) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, pod); err != nil {
		return
	}
	if got := pod.Labels[query.LabelInstanceIncarnation]; got != fmt.Sprintf("%d", incarnation) {
		return
	}
	changed := false
	if !podreadiness.IsContainersReady(pod) {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.ContainersReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
		})
		changed = true
	}
	if podreadiness.IsServing(pod) && !podreadiness.IsPodReady(pod) {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
		})
		changed = true
	}
	if !changed {
		return
	}
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("synthesize kubelet readiness: %v", err)
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

	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "pod Failed"); err != nil {
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

	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "pod count 0 below desired 1"); err != nil {
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
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c, Recorder: rec}, input, plan, plan.Instances[0], nil, reason); err != nil {
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

// A pair row that escalated keeps its Migrate operation while it reads
// Failed. The record still owns its end, so neither the lost-member
// rebuild nor any other restart trigger opens a Restart on it.
func TestDetectRestartTrigger_FailedMigrateRowStaysClaimed(t *testing.T) {
	status := workload.InstanceStatus{
		Index: 0, Incarnation: 74, Phase: workload.InstancePhaseFailed, PodCount: 2,
		RunningRevision: gangLossRevision,
		Operation:       &workload.InstanceOperation{Type: workload.InstanceOperationMigrate, Step: "CreatePods"},
	}
	input := gangLossInput(status)
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, RestartPolicy: workload.RestartPolicyRecreateInstance}
	inst := workload.InstancePlan{Index: 0, Incarnation: 74, Runners: []workload.RunnerPlan{
		{Name: "leader", Size: 1}, {Name: "worker", Size: 1},
	}}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, inst, gangLossPods(1)); needs {
		t.Fatalf("a Failed row claimed by a migration must not be recreated; got reason %q", reason)
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

// TestRestart_PromotionWaitsForPodReadyAndWindow: Phase C writes the serving
// gate on the rebuilt pod, then holds the Ready stamp at the shared promote
// bar — first until kubelet folds that gate into PodReady, then until the pod
// has held Ready for the Component's minReadySeconds window. The wait is
// reported as the window's remainder so the pass wakes on it.
func TestRestart_PromotionWaitsForPodReadyAndWindow(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 2)
	ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation: &v1beta1.InstanceOperation{
			Type: v1beta1.InstanceOperationRestart, Step: "Drain", StartedAt: metav1.Now(),
		},
	}
	// Phase A and B are done: the old incarnation is gone and the rebuilt pod
	// is up but still held out of rotation by the lifecycle gate.
	pod := podAtIncarnation(isvc, 0, 2, true /* ready */, false /* serving */)
	c := newFakeClient(t, isvc, ir, pod)

	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	clk := clocktesting.NewFakeClock(start)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.MinReadySeconds = 20

	restart := func() (bool, *workload.PromoteWindow) {
		t.Helper()
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		input.Clock = clk
		window := &workload.PromoteWindow{}
		input.PromoteWindow = window
		done, err := ops.Restart(context.Background(), workload.Deps{Client: c, Clock: clk}, input, plan, plan.Instances[0], nil, "pod Failed")
		if err != nil {
			t.Fatalf("Restart: %v", err)
		}
		return done, window
	}

	done, _ := restart()
	if done {
		t.Fatal("promoted on ContainersReady: the pod is not PodReady yet")
	}
	live := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
		t.Fatalf("get rebuilt pod: %v", err)
	}
	if !podreadiness.IsServing(live) {
		t.Fatal("Phase C must write the serving gate before waiting on PodReady")
	}
	if s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0); s == nil || s.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Fatalf("row left Restart before the promote bar: %+v", s)
	}

	// Kubelet folds the gate into PodReady 5s into the 20s window.
	makeNewPodReady(t, c, isvc.Namespace, pod.Name, 2)
	live = &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), live); err != nil {
		t.Fatalf("get PodReady pod: %v", err)
	}
	for i := range live.Status.Conditions {
		if live.Status.Conditions[i].Type == corev1.PodReady {
			live.Status.Conditions[i].LastTransitionTime = metav1.NewTime(start.Add(-5 * time.Second))
		}
	}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("age the Ready transition: %v", err)
	}

	done, window := restart()
	if done {
		t.Fatal("promoted inside the minReadySeconds window")
	}
	if got, want := window.Pending(), 15*time.Second; got != want {
		t.Fatalf("reported window remainder: got %s, want %s", got, want)
	}

	clk.SetTime(start.Add(15 * time.Second))
	done, window = restart()
	if !done {
		t.Fatal("expected the promote once the pod was PodReady past its window")
	}
	if got := window.Pending(); got != 0 {
		t.Fatalf("promoted pass still reported a wait: %s", got)
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceReady || s.Operation != nil {
		t.Fatalf("promote: got %+v, want Phase=Ready with no operation", s)
	}
}

// A repair whose attempt the deadline ended keeps its two exits: the
// rebuilt set coming up resumes the promote at the incarnation the pods
// carry, with no new attempt and no operator action.
func TestRestart_DeadlineFailedRowResumesItsPromote(t *testing.T) {
	resetExpectations(t)
	now := time.Now()
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 2)
	ir.Status.InstanceStatuses[0] = spentRepairRow(now, 0)
	// The rebuild came up after the deadline had already ended the attempt.
	pod := promotableRebuiltPod(isvc, 2, now)
	c := newFakeClient(t, isvc, ir, pod)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(now)

	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod})
	if !needs || reason != "" {
		t.Fatalf("restart trigger: got (%v, %q) want a resume with no new reason", needs, reason)
	}
	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, reason)
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if !done {
		t.Fatalf("Restart: done=false, want the promote to land on a set that is already PodReady")
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceReady || s.Operation != nil {
		t.Fatalf("row = %+v, want Ready with the spent operation cleared", s)
	}
	if s.Incarnation != 2 {
		t.Errorf("incarnation = %d, want 2: a set that came up is promoted, not rebuilt", s.Incarnation)
	}
}

// spentRepairRow is a Restart parked at Failed an hour ago, with retries
// re-arms already spent and the failure that parked it recorded: a crash
// loop, a cause outside the set the kubelet retries in place, so the
// ladder owes it a re-arm.
func spentRepairRow(now time.Time, retries int32) v1beta1.OMENativeInstanceStatus {
	return spentRepairRowOn(now, retries, "CrashLoopBackOff")
}

// spentRepairRowOn is spentRepairRow with the recorded failure's reason
// chosen by the test.
func spentRepairRowOn(now time.Time, retries int32, reason string) v1beta1.OMENativeInstanceStatus {
	return v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceFailed,
		Operation: &v1beta1.InstanceOperation{
			ID: "restart-0-1", Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "pod lost",
			StartedAt:  metav1.NewTime(now.Add(-2 * time.Hour)),
			Deadline:   metav1.NewTime(now.Add(-time.Hour)),
			RetryCount: retries,
		},
		LastFailure: &v1beta1.InstanceTermination{
			PodName: "llama-70b-engine-0-default-0", Reason: reason,
			Time: metav1.NewTime(now.Add(-time.Hour)),
		},
	}
}

// promotableRebuiltPod is a rebuilt pod at incarnation that cleared the
// promote bar: ContainersReady, serving, and PodReady folded by kubelet.
func promotableRebuiltPod(isvc *v1beta1.InferenceService, incarnation int64, now time.Time) *corev1.Pod {
	pod := podAtIncarnation(isvc, 0, incarnation, true /* ready */, true /* serving */)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(now.Add(-time.Minute)),
	})
	return pod
}

// wedgedRebuiltPod is a rebuilt pod at incarnation parked in a terminal
// waiting reason since created.
func wedgedRebuiltPod(isvc *v1beta1.InferenceService, incarnation int64, reason string, created time.Time) *corev1.Pod {
	pod := podAtIncarnation(isvc, 0, incarnation, false, false)
	pod.CreationTimestamp = metav1.NewTime(created)
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "main",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}}
	return pod
}

// evictedRebuiltPod is a rebuilt pod at incarnation the kubelet evicted
// after it parked: phase Failed with the eviction on the pod-level
// reason, its container terminated, readiness withdrawn. It holds the
// row's stable name and will never run again.
func evictedRebuiltPod(isvc *v1beta1.InferenceService, incarnation int64, now time.Time) *corev1.Pod {
	pod := podAtIncarnation(isvc, 0, incarnation, false, false)
	pod.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = "Evicted"
	pod.Status.Message = "The node was low on resource: memory."
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "main",
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 137, Reason: "Error", FinishedAt: metav1.NewTime(now.Add(-time.Minute)),
		}},
	}}
	return pod
}

// retryLadder is a lifecycle.updateRetry ladder for the trigger tests.
func retryLadder(maxAttempts int32, initial time.Duration) *workload.RetryPolicy {
	return &workload.RetryPolicy{MaxAttempts: maxAttempts, InitialDelay: initial, MaxDelay: 10 * time.Minute, Multiplier: 2}
}

// spentRepairFixture is a parked repair whose rebuilt pod is still
// wedged in a crash loop, with the ladder and the failure age the test
// names.
//
// now must be whole seconds: status timestamps round-trip at second
// precision, and the ladder is measured from the stored failure time.
func spentRepairFixture(t *testing.T, now time.Time, retries int32, failedAgo time.Duration, ladder *workload.RetryPolicy) (workload.ReconcileInput, workload.ComponentPlan, *corev1.Pod, client.Client, *v1beta1.InferenceService) {
	t.Helper()
	return spentRepairFixtureOn(t, now, retries, failedAgo, ladder, "CrashLoopBackOff")
}

// spentRepairFixtureOn is spentRepairFixture with the wedge's reason,
// recorded on the row and shown by the pod, chosen by the test.
func spentRepairFixtureOn(t *testing.T, now time.Time, retries int32, failedAgo time.Duration, ladder *workload.RetryPolicy, reason string) (workload.ReconcileInput, workload.ComponentPlan, *corev1.Pod, client.Client, *v1beta1.InferenceService) {
	t.Helper()
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 2)
	row := spentRepairRowOn(now, retries, reason)
	row.LastFailure.Time = metav1.NewTime(now.Add(-failedAgo))
	ir.Status.InstanceStatuses[0] = row
	pod := wedgedRebuiltPod(isvc, 2, reason, now.Add(-failedAgo-time.Minute))
	c := newFakeClient(t, isvc, ir, pod)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.Clock = clocktesting.NewFakeClock(now)
	input.UpdateRetryPolicy = ladder
	input.PassWake = &workload.PassWake{}
	return input, plan, pod, c, isvc
}

// A parked repair whose set stays wedged re-arms once the ladder's delay
// has elapsed since the recorded failure: a whole rebuild at the next
// incarnation whose operation counts the re-arm, and the wedged pod
// deleted for it. Once the re-arm has landed the row is an open repair,
// which the trigger drives without counting a second one.
func TestDetectRestartTrigger_SpentRepairReArmsWhenTheLadderIsDue(t *testing.T) {
	now := time.Now()
	input, plan, pod, c, isvc := spentRepairFixture(t, now, 0, 2*time.Minute, retryLadder(3, time.Minute))

	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod})
	if !needs {
		t.Fatalf("restart trigger: got false, want the re-arm now that the ladder is due")
	}
	if !strings.Contains(reason, "repair re-armed (attempt 1)") || !strings.Contains(reason, "CrashLoopBackOff") {
		t.Errorf("reason = %q, want the attempt number and the failure that parked the last one", reason)
	}
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, reason); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceRestarting || s.Incarnation != 3 {
		t.Fatalf("row = %+v, want Restarting at incarnation 3", s)
	}
	if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationRestart || s.Operation.RetryCount != 1 || s.Operation.Reason != reason {
		t.Errorf("operation = %+v, want a fresh Restart counting one re-arm under the trigger's reason", s.Operation)
	}
	stored := &corev1.Pod{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), stored); err == nil {
		t.Errorf("the wedged pod must be deleted by the re-armed drain")
	}

	input = buildTestInput(isvc, c, workload.ComponentEngine)
	input.UpdateRetryPolicy = retryLadder(3, time.Minute)
	needs, reason = ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil)
	if !needs || reason != "" {
		t.Errorf("open repair: got (%v, %q) want it driven on with no second re-arm", needs, reason)
	}
}

// Before the ladder's delay has elapsed the repair stays parked, and the
// pass is told when to come back: nothing in the cluster will wake it.
func TestDetectRestartTrigger_SpentRepairWaitsForTheLadder(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	input, plan, pod, _, _ := spentRepairFixture(t, now, 0, 20*time.Second, retryLadder(3, time.Minute))

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("restart trigger: got true (%q) want false 20s into a 1m delay", reason)
	}
	if got := input.PassWake.Pending(); got != 40*time.Second {
		t.Errorf("wake-up = %v, want the 40s left on the ladder", got)
	}
}

// The second re-arm waits out the ladder's second rung, not its first.
func TestDetectRestartTrigger_SpentRepairLadderGrows(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	input, plan, pod, _, _ := spentRepairFixture(t, now, 1, 90*time.Second, retryLadder(3, time.Minute))

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("restart trigger: got true (%q) want false 90s into the 2m second rung", reason)
	}
	if got := input.PassWake.Pending(); got != 30*time.Second {
		t.Errorf("wake-up = %v, want the 30s left on the second rung", got)
	}
}

// No ladder configured is the fail-safe: the parked repair is left as it
// is, with no wake-up, and a Restart pass that reaches the row writes
// nothing.
func TestDetectRestartTrigger_SpentRepairWithoutALadderStaysParked(t *testing.T) {
	now := time.Now()
	input, plan, pod, c, isvc := spentRepairFixture(t, now, 0, time.Hour, nil)

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("restart trigger: got true (%q) want false with no ladder configured", reason)
	}
	if got := input.PassWake.Pending(); got != 0 {
		t.Errorf("wake-up = %v, want none: nothing is coming due", got)
	}
	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "")
	if err != nil || done {
		t.Fatalf("Restart on a parked row: done=%v err=%v, want (false, nil)", done, err)
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Incarnation != 2 || s.Operation == nil || s.Operation.ID != "restart-0-1" {
		t.Fatalf("row = %+v, want the parked repair untouched", s)
	}
}

// A ladder spent to its last rung re-arms nothing more.
func TestDetectRestartTrigger_SpentRepairStopsAtMaxAttempts(t *testing.T) {
	now := time.Now()
	input, plan, pod, _, _ := spentRepairFixture(t, now, 3, time.Hour, retryLadder(3, time.Minute))

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("restart trigger: got true (%q) want false with every re-arm spent", reason)
	}
	if got := input.PassWake.Pending(); got != 0 {
		t.Errorf("wake-up = %v, want none: the ladder is spent", got)
	}
}

// A Held block against the revision the repair rebuilds denies the
// re-arm, exactly as it denies every other rebuild of that revision.
func TestDetectRestartTrigger_SpentRepairHeldRevisionDeniesTheReArm(t *testing.T) {
	now := time.Now()
	input, plan, pod, _, _ := spentRepairFixture(t, now, 0, time.Hour, retryLadder(3, time.Minute))
	input.ObservedState.InstanceStatuses[0].RunningRevision = "llama-70b-engine-abc12345"
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{TargetRevision: "llama-70b-engine-abc12345", State: workload.RetryBlockHeld}}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("restart trigger: got true (%q) want false under a Held block", reason)
	}
}

// A set still carrying the incarnation the attempt meant to drain is not
// a set that came up: it is rebuilt on the ladder like a wedged one.
func TestDetectRestartTrigger_SpentRepairOldIncarnationSetReArms(t *testing.T) {
	now := time.Now()
	input, plan, wedged, c, isvc := spentRepairFixture(t, now, 0, time.Hour, retryLadder(3, time.Minute))
	// The row's stable name is held by the incarnation the attempt never
	// drained, healthy and serving, instead of by a rebuilt pod.
	if err := c.Delete(context.Background(), wedged); err != nil {
		t.Fatalf("delete rebuilt pod: %v", err)
	}
	old := promotableRebuiltPod(isvc, 1, now)
	if err := c.Create(context.Background(), old); err != nil {
		t.Fatalf("create old pod: %v", err)
	}

	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{old})
	if !needs || !strings.Contains(reason, "repair re-armed") {
		t.Fatalf("restart trigger: got (%v, %q) want a re-arm, not a resume", needs, reason)
	}
}

// spentRepairCloseCases are the parks a close is owed to once no live
// pod is left: whatever cause parked the repair, whether or not a ladder
// is configured, and however many re-arms the ladder has spent.
var spentRepairCloseCases = []struct {
	name    string
	reason  string
	retries int32
	ladder  *workload.RetryPolicy
}{
	{"crash loop/ladder configured", "CrashLoopBackOff", 0, retryLadder(3, time.Minute)},
	{"crash loop/no ladder", "CrashLoopBackOff", 0, nil},
	{"crash loop/ladder spent", "CrashLoopBackOff", 3, retryLadder(3, time.Minute)},
	{"kubelet-retried cause/ladder configured", "ImagePullBackOff", 0, retryLadder(3, time.Minute)},
	{"kubelet-retried cause/no ladder", "CreateContainerConfigError", 0, nil},
}

// assertSpentRepairClosed drives the close of a spent repair whose set
// is gone: the trigger fires with the close and wakes nothing, the
// Restart pass clears the spent operation and asks for the pass after,
// the row keeps its phase, incarnation and failure record, and the
// restart pass has nothing further to decide on the fresh start.
func assertSpentRepairClosed(t *testing.T, input workload.ReconcileInput, plan workload.ComponentPlan, c client.Client, isvc *v1beta1.InferenceService, pods []*corev1.Pod, failureReason string, ladder *workload.RetryPolicy) {
	t.Helper()
	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], pods)
	if !needs || !strings.Contains(reason, "closed") {
		t.Fatalf("restart trigger: got (%v, %q) want the close of a repair with no live pod left", needs, reason)
	}
	if got := input.PassWake.Pending(); got != 0 {
		t.Errorf("wake-up = %v, want none: the close is owed now", got)
	}
	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, reason)
	if err != nil || done {
		t.Fatalf("Restart on the parked row: done=%v err=%v, want (false, nil): the Create pass rebuilds the row on the pass after", done, err)
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Operation != nil || s.Incarnation != 2 {
		t.Fatalf("row = %+v, want Failed with the spent operation cleared and the incarnation kept", s)
	}
	if s.LastFailure == nil || s.LastFailure.Reason != failureReason {
		t.Errorf("lastFailure = %+v, want the reason %s kept on the row", s.LastFailure, failureReason)
	}
	after := buildTestInput(isvc, c, workload.ComponentEngine)
	after.UpdateRetryPolicy = ladder
	after.PassWake = &workload.PassWake{}
	if needs, reason := ops.DetectRestartTriggerWithPods(after, plan, plan.Instances[0], pods); needs {
		t.Fatalf("restart trigger after the close: got true (%q) want false: the fresh start is the Create pass's", reason)
	}
}

// A parked repair with no pod left has nothing to resume or re-arm, so
// the trigger closes it: the spent operation is cleared and the row is
// the fresh start the Create pass rebuilds, whatever parked the repair
// and whether or not a ladder is configured.
func TestDetectRestartTrigger_SpentRepairWithNoPodsCloses(t *testing.T) {
	for _, tc := range spentRepairCloseCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			input, plan, wedged, c, isvc := spentRepairFixtureOn(t, now, tc.retries, time.Hour, tc.ladder, tc.reason)
			if err := c.Delete(context.Background(), wedged); err != nil {
				t.Fatalf("delete the parked pod: %v", err)
			}
			assertSpentRepairClosed(t, input, plan, c, isvc, nil, tc.reason, tc.ladder)
		})
	}
}

// A parked repair whose only pod is in a terminal phase — evicted after
// the park — has no live pod to resume or re-arm either: the trigger
// closes it, and the dead occupant is left where it is for the Create
// pass's recycle, which frees the name before the rebuild.
func TestDetectRestartTrigger_SpentRepairWithOnlyATerminalPodCloses(t *testing.T) {
	for _, tc := range spentRepairCloseCases {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			input, plan, wedged, c, isvc := spentRepairFixtureOn(t, now, tc.retries, time.Hour, tc.ladder, tc.reason)
			if err := c.Delete(context.Background(), wedged); err != nil {
				t.Fatalf("delete the parked pod: %v", err)
			}
			evicted := evictedRebuiltPod(isvc, 2, now)
			if err := c.Create(context.Background(), evicted); err != nil {
				t.Fatalf("create the evicted pod: %v", err)
			}
			assertSpentRepairClosed(t, input, plan, c, isvc, []*corev1.Pod{evicted}, tc.reason, tc.ladder)
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(evicted), &corev1.Pod{}); err != nil {
				t.Errorf("the evicted pod must stay for the Create pass's recycle: %v", err)
			}
		})
	}
}

// A parked repair whose pod is still live keeps its exits exactly: a set
// that came up resumes, a wedged set re-arms on the ladder, and a pod
// still terminating holds its name until it is gone, so neither the
// close nor a re-arm is decided on it.
func TestDetectRestartTrigger_SpentRepairWithALivePodIsNotClosed(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	t.Run("a set that came up resumes", func(t *testing.T) {
		input, plan, wedged, c, isvc := spentRepairFixture(t, now, 0, time.Hour, retryLadder(3, time.Minute))
		if err := c.Delete(context.Background(), wedged); err != nil {
			t.Fatalf("delete the parked pod: %v", err)
		}
		up := promotableRebuiltPod(isvc, 2, now)
		if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{up}); !needs || reason != "" {
			t.Fatalf("restart trigger: got (%v, %q) want the resume, which carries no reason", needs, reason)
		}
	})
	t.Run("a wedged set re-arms on the ladder", func(t *testing.T) {
		input, plan, pod, _, _ := spentRepairFixture(t, now, 0, 2*time.Minute, retryLadder(3, time.Minute))
		if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); !needs || !strings.Contains(reason, "repair re-armed") {
			t.Fatalf("restart trigger: got (%v, %q) want the ladder's re-arm", needs, reason)
		}
	})
	t.Run("a terminating pod holds its name", func(t *testing.T) {
		input, plan, pod, _, _ := spentRepairFixture(t, now, 0, time.Hour, retryLadder(3, time.Minute))
		terminating := pod.DeepCopy()
		terminating.DeletionTimestamp = &metav1.Time{Time: now}
		terminating.Finalizers = []string{"example.com/termination"}
		if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{terminating}); needs {
			t.Fatalf("restart trigger: got true (%q) want false while the pod still terminates", reason)
		}
		if got := input.PassWake.Pending(); got != 0 {
			t.Errorf("wake-up = %v, want none: the pod leaving is the next event", got)
		}
	})
}

// kubeletRetriedReasons are the waiting reasons the kubelet retries in
// place and a fresh pod set cannot clear: the workload-caused set. A
// repair parked on one waits for the configuration or image instead of
// re-arming on the ladder.
var kubeletRetriedReasons = []string{"CreateContainerConfigError", "ErrImagePull", "ImagePullBackOff", "InvalidImageName"}

// A repair parked on a cause the kubelet retries in place owes no re-arm:
// past the ladder's first delay and past its whole span alike, the row
// stays Failed with its reason, no wake-up is deposited, no Restarting
// stamp lands and the wedged pod is left where the kubelet retries it.
func TestDetectRestartTrigger_SpentRepairOnAKubeletRetriedCauseOwesNoReArm(t *testing.T) {
	for _, reason := range kubeletRetriedReasons {
		for _, failedAgo := range []time.Duration{2 * time.Minute, time.Hour} {
			t.Run(fmt.Sprintf("%s/%s", reason, failedAgo), func(t *testing.T) {
				now := time.Now()
				input, plan, pod, c, isvc := spentRepairFixtureOn(t, now, 0, failedAgo, retryLadder(3, time.Minute), reason)

				if needs, got := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
					t.Fatalf("restart trigger: got true (%q) want false: the kubelet retries %s in place", got, reason)
				}
				if got := input.PassWake.Pending(); got != 0 {
					t.Errorf("wake-up = %v, want none: no re-arm is coming due", got)
				}
				done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "")
				if err != nil || done {
					t.Fatalf("Restart on the parked row: done=%v err=%v, want (false, nil)", done, err)
				}
				s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
				if s == nil || s.Phase != v1beta1.OMENativeInstanceFailed || s.Incarnation != 2 ||
					s.Operation == nil || s.Operation.ID != "restart-0-1" || s.Operation.RetryCount != 0 {
					t.Fatalf("row = %+v, want the parked repair untouched", s)
				}
				if s.LastFailure == nil || s.LastFailure.Reason != reason {
					t.Errorf("lastFailure = %+v, want the reason %s kept on the row", s.LastFailure, reason)
				}
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{}); err != nil {
					t.Errorf("the wedged pod must stay for the kubelet to retry: %v", err)
				}
			})
		}
	}
}

// RepairRetryAt owes a re-arm to every parked cause but the ones the
// kubelet retries in place, and to a park that recorded no failure at
// all, which is measured from the operation's own timestamps.
func TestRepairRetryAt_OwesNoReArmToAKubeletRetriedCause(t *testing.T) {
	now := time.Now()
	input := workload.ReconcileInput{UpdateRetryPolicy: retryLadder(3, time.Minute), Clock: clocktesting.NewFakeClock(now)}
	parked := func(reason string) *workload.InstanceStatus {
		return &workload.InstanceStatus{
			Index: 0, Incarnation: 2, Phase: workload.InstancePhaseFailed,
			Operation: &workload.InstanceOperation{
				ID: "restart-0-1", Type: workload.InstanceOperationRestart, Step: "Drain",
				StartedAt: metav1.NewTime(now.Add(-2 * time.Hour)), LastProgressAt: metav1.NewTime(now.Add(-90 * time.Minute)),
			},
			LastFailure: &workload.InstanceTermination{PodName: "llama-70b-engine-0-default-0", Reason: reason, Time: metav1.NewTime(now.Add(-time.Hour))},
		}
	}
	for _, reason := range kubeletRetriedReasons {
		if at, owed := ops.RepairRetryAt(input, parked(reason)); owed {
			t.Errorf("%s: owed a re-arm at %v, want none: the kubelet retries it in place", reason, at)
		}
	}
	for _, reason := range []string{"CrashLoopBackOff", "RunContainerError", "CreateContainerError", "DeadlineExceeded", ""} {
		row := parked(reason)
		at, owed := ops.RepairRetryAt(input, row)
		if !owed {
			t.Errorf("%q: owed no re-arm, want one on the ladder", reason)
			continue
		}
		if want := row.LastFailure.Time.Add(time.Minute); !at.Equal(want) {
			t.Errorf("%q: re-arm at %v, want %v, the ladder's first delay from the recorded failure", reason, at, want)
		}
	}
	row := parked("CrashLoopBackOff")
	row.LastFailure = nil
	if at, owed := ops.RepairRetryAt(input, row); !owed || !at.Equal(row.Operation.LastProgressAt.Add(time.Minute)) {
		t.Errorf("no failure recorded: (%v, %v), want a re-arm measured from the operation's last progress", at, owed)
	}
}

// A ladder spent on a cause the kubelet retries in place is not what
// holds the row: the park waits for the configuration or image and
// resumes on its own, so it is not read as exhausted.
func TestRepairRetriesExhausted_SkipsAKubeletRetriedCause(t *testing.T) {
	now := time.Now()
	input := workload.ReconcileInput{UpdateRetryPolicy: retryLadder(3, time.Minute)}
	spent := func(reason string) *workload.InstanceStatus {
		return &workload.InstanceStatus{
			Phase:       workload.InstancePhaseFailed,
			Operation:   &workload.InstanceOperation{ID: "restart-0-4", Type: workload.InstanceOperationRestart, RetryCount: 3},
			LastFailure: &workload.InstanceTermination{Reason: reason, Time: metav1.NewTime(now.Add(-time.Hour))},
		}
	}
	for _, reason := range kubeletRetriedReasons {
		if ops.RepairRetriesExhausted(input, spent(reason)) {
			t.Errorf("%s: read as exhausted, want a park that waits for the fix", reason)
		}
	}
	if !ops.RepairRetriesExhausted(input, spent("CrashLoopBackOff")) {
		t.Errorf("CrashLoopBackOff: a spent ladder must still read as exhausted")
	}
}

// A re-arm already in flight is driven on whatever the recorded failure
// says: the exclusion reads a parked row only, and the open repair runs
// to its own park.
func TestDetectRestartTrigger_OpenRepairOnAKubeletRetriedCauseIsDrivenOn(t *testing.T) {
	now := time.Now()
	input, plan, _, _, _ := spentRepairFixtureOn(t, now, 1, time.Minute, retryLadder(3, time.Minute), "CreateContainerConfigError")
	input.ObservedState.InstanceStatuses[0].Phase = workload.InstancePhaseRestarting

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil); !needs || reason != "" {
		t.Fatalf("open repair: got (%v, %q) want it driven on with no second re-arm", needs, reason)
	}
}

// The recovery from such a park is the kubelet's: once the pod it left
// in place comes up, the repair resumes its promote at the incarnation
// the pod carries, with no re-arm in between and whether or not a ladder
// is configured.
func TestRestart_KubeletRetriedCauseClearedResumesThePromote(t *testing.T) {
	for _, ladder := range []*workload.RetryPolicy{retryLadder(3, time.Minute), nil} {
		resetExpectations(t)
		now := time.Now()
		isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 2)
		ir.Status.InstanceStatuses[0] = spentRepairRowOn(now, 0, "ImagePullBackOff")
		pod := promotableRebuiltPod(isvc, 2, now)
		c := newFakeClient(t, isvc, ir, pod)
		plan := buildPlanSinglePodEngineForRestart(c, isvc)
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		input.Clock = clocktesting.NewFakeClock(now)
		input.UpdateRetryPolicy = ladder
		input.PassWake = &workload.PassWake{}

		needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod})
		if !needs || reason != "" {
			t.Fatalf("restart trigger: got (%v, %q) want a resume with no new reason", needs, reason)
		}
		done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, reason)
		if err != nil || !done {
			t.Fatalf("Restart: done=%v err=%v, want the promote to land", done, err)
		}
		s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
		if s == nil || s.Phase != v1beta1.OMENativeInstanceReady || s.Operation != nil || s.Incarnation != 2 {
			t.Fatalf("row = %+v, want Ready at incarnation 2 with the spent operation cleared", s)
		}
	}
}

// TestRestart_OperatorConfigChangesDoNotChangeWhatItDecides: a repair
// waiting out its promote window reads none of the knobs that belong to
// other work — the gang clamp is applied where a PodGroup is built, the
// migration caps bound requests a repair never raises, and the
// RetryBlock history cap shapes a prune of superseded blocks. The
// requeue cadence is read only to pace the next wake-up, and the window
// remainder the pass already holds outranks it.
func TestRestart_OperatorConfigChangesDoNotChangeWhatItDecides(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	// One pass over a repair whose rebuilt pod is PodReady but still
	// inside the Component's minReadySeconds window.
	run := func(t *testing.T, change func(*workload.ReconcileInput, *workload.ComponentPlan)) (bool, time.Duration, v1beta1.OMENativeInstanceStatus) {
		t.Helper()
		resetExpectations(t)
		isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 2)
		ir.Status.InstanceStatuses[0] = v1beta1.OMENativeInstanceStatus{
			Index:       0,
			Incarnation: 2,
			Phase:       v1beta1.OMENativeInstanceRestarting,
			Operation: &v1beta1.InstanceOperation{
				ID: "restart-0-1", Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "pod lost",
				StartedAt: metav1.NewTime(start), LastProgressAt: metav1.NewTime(start),
				Deadline: metav1.NewTime(start.Add(time.Hour)),
			},
		}
		pod := podAtIncarnation(isvc, 0, 2, true /* ready */, true /* serving */)
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(start.Add(-5 * time.Second)),
		})
		c := newFakeClient(t, isvc, ir, pod)
		clk := clocktesting.NewFakeClock(start)
		plan := buildPlanSinglePodEngineForRestart(c, isvc)
		plan.MinReadySeconds = 20
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		input.Clock = clk
		window := &workload.PromoteWindow{}
		input.PromoteWindow = window
		if change != nil {
			change(&input, &plan)
		}

		done, err := ops.Restart(context.Background(), workload.Deps{Client: c, Clock: clk}, input, plan, plan.Instances[0], nil, "pod lost")
		if err != nil {
			t.Fatalf("Restart: %v", err)
		}
		s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
		if s == nil {
			t.Fatalf("instance 0 status: missing")
		}
		return done, window.Pending(), *s
	}

	baseDone, basePending, baseStatus := run(t, nil)
	if baseDone || basePending == 0 {
		t.Fatalf("baseline: done=%v pending=%v want a repair still waiting out its window", baseDone, basePending)
	}

	for _, tc := range []struct {
		name   string
		change func(*workload.ReconcileInput, *workload.ComponentPlan)
	}{
		{
			name: "gang schedule clamp",
			change: func(_ *workload.ReconcileInput, plan *workload.ComponentPlan) {
				plan.GangScheduleTimeout = &workload.GangScheduleTimeoutClamp{Min: time.Minute, Max: 10 * time.Minute}
			},
		},
		{
			name: "requeue cadence",
			change: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.Requeue = workload.RequeueIntervals{Operation: 9 * time.Minute, Gate: 8 * time.Minute}
			},
		},
		{
			name: "migration audit caps",
			change: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.MigrationAudit = &workload.MigrationAuditPolicy{MaxInFlight: 1, MaxPerWindow: 2, Window: time.Hour}
			},
		},
		{
			name: "retry block history",
			change: func(in *workload.ReconcileInput, _ *workload.ComponentPlan) {
				in.ObservedState.RetryBlocks = []workload.RetryBlock{
					{TargetRevision: "llama-70b-engine-stale001", State: workload.RetryBlockHeld},
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done, pending, status := run(t, tc.change)
			if done != baseDone {
				t.Errorf("done: got %v want %v", done, baseDone)
			}
			if pending != basePending {
				t.Errorf("promote window remainder: got %v want the baseline %v", pending, basePending)
			}
			if !reflect.DeepEqual(status, baseStatus) {
				t.Errorf("row: got %+v want the baseline %+v", status, baseStatus)
			}
		})
	}
}

// Op-less crash-loop repair: a Ready row whose pod is wedged in a
// terminal kubelet waiting reason on the revision it is supposed to be
// running has no other repair path, so the restart trigger opens one
// whatever the restart policy says.

const crashLoopRevision = "llama-70b-engine-" + testRevisionHash

// crashLoopingPod is a pod of instance 0 whose runner is parked in a
// terminal waiting reason, created `age` ago.
func crashLoopingPod(isvc *v1beta1.InferenceService, reason string, age time.Duration) *corev1.Pod {
	pod := podForInstance(isvc, 0, false /* ready */, false /* serving */)
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-age))
	pod.Status.Phase = corev1.PodPending
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  constants.MainContainerName,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}},
	}}
	return pod
}

// crashLoopInput wires a Ready, operation-free row on crashLoopRevision
// with the stuck-pod grace configured.
func crashLoopInput(t *testing.T, grace time.Duration) (workload.ReconcileInput, workload.ComponentPlan, *corev1.Pod) {
	t.Helper()
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision = crashLoopRevision
	pod := crashLoopingPod(isvc, "CrashLoopBackOff", time.Hour)
	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = grace
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone
	return input, plan, pod
}

// A Ready row with no operation whose pod has been wedged in a terminal
// waiting reason longer than the grace opens a Restart even under
// RestartPolicy=None: the pod cannot recover on its own and nothing else
// repairs it.
func TestDetectRestartTrigger_OpLessCrashLoopRepairsUnderPolicyNone(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)

	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod})
	if !needs {
		t.Fatalf("expected a restart trigger for an operation-less crash loop under RestartPolicy=None")
	}
	if !strings.Contains(reason, "CrashLoopBackOff") || !strings.Contains(reason, pod.Name) {
		t.Errorf("reason must name the pod and the kubelet reason; got %q", reason)
	}
}

// The repair is paced by the grace: evidence younger than
// lifecycle.stuckPodGracePeriod is an ordinary backoff the kubelet may
// still resolve.
func TestDetectRestartTrigger_CrashLoopWithinGraceStaysSilent(t *testing.T) {
	input, plan, pod := crashLoopInput(t, 2*time.Hour)

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("a wedge inside the grace must not trigger; got reason %q", reason)
	}
}

// An unconfigured grace disables the repair outright, exactly as it
// disables the stuck-pod fast escalation.
func TestDetectRestartTrigger_UnconfiguredGraceDisablesCrashLoopRepair(t *testing.T) {
	input, plan, pod := crashLoopInput(t, 0)

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("no configured grace means no repair; got reason %q", reason)
	}
}

// A wedged pod whose revision-hash label disagrees with the Component's
// current revision is a leftover, not this row's workload: there is no
// revision for a repair to rebuild on, so it belongs to the escalation
// pass's wedged-pod edge to Failed.
func TestDetectRestartTrigger_OffRevisionWedgeLeftToEscalation(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)
	pod.Labels[query.LabelRevisionHash] = "otherrev"

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("an off-revision leftover must not open a repair; got reason %q", reason)
	}
}

// A serving pod set is never repaired on the strength of a container
// waiting reason: the workload is answering traffic and a recycle would
// take it out.
func TestDetectRestartTrigger_CrashLoopOnServingPodSetStaysSilent(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("a fully serving pod set must not be recycled; got reason %q", reason)
	}
}

// The repair answers to the same RetryBlock authority every rebuild does:
// a revision held after repeated failures is not re-materialized on a
// loop. A Held block is the one gate verdict with no time bound: it
// denies every fresh attempt at that target until a different target
// arrives.
func TestDetectRestartTrigger_CrashLoopDeniedByHeldRetryBlock(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)
	input.ObservedState.RetryBlocks = []workload.RetryBlock{{
		TargetRevision: crashLoopRevision,
		State:          workload.RetryBlockHeld,
	}}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("a held RetryBlock must deny the repair; got reason %q", reason)
	}
}

// RestartPolicy=None keeps its meaning for mere container restarts: a
// runner that died and came back inside the same pod is not a wedge, and
// the operator asked for it to be left alone.
func TestDetectRestartTrigger_ContainerRestartStillHonoursPolicyNone(t *testing.T) {
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
	input.StuckPodGrace = time.Minute
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("RestartPolicy=None must still ignore a bare container restart; got reason %q", reason)
	}
}

// Once the repair is open, the row is re-selected on every pass so the
// Restart state machine advances — a repair that stalls at Phase=Drain
// under RestartPolicy=None would be worse than never opening one.
func TestDetectRestartTrigger_OpenRepairKeepsDrivingUnderPolicyNone(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index:       0,
		Incarnation: 2,
		Phase:       v1beta1.OMENativeInstanceRestarting,
		Operation:   &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationRestart, Step: "Drain"},
	})
	c := newFakeClient(t, isvc, ir)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone

	if needs, _ := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil); !needs {
		t.Fatalf("an in-flight Restart must keep being driven under RestartPolicy=None")
	}
}

// A row below Ready is not this trigger's: an Instance still forming
// boots through waiting states the kubelet resolves on its own, and the
// phases before Ready are owned by the passes that materialize them.
func TestDetectRestartTrigger_PendingRowIsNotACrashLoopRepair(t *testing.T) {
	input, plan, pod := crashLoopInput(t, time.Minute)
	for i := range input.ObservedState.InstanceStatuses {
		input.ObservedState.InstanceStatuses[i].Phase = workload.InstancePhasePending
	}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
		t.Fatalf("a Pending row belongs to another pass; got reason %q", reason)
	}
}

// A gang is repaired as a unit: one wedged member opens ONE Restart for
// the Instance, which is what rebuilds the whole pod set. The repair is
// selected per Instance, so the healthy members are neither separately
// triggered nor separately counted.
func TestDetectRestartTrigger_GangWedgedOnOneMemberOpensOneRepair(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision = crashLoopRevision
	ir.Status.InstanceStatuses[0].PodCount = 2

	healthy := podForInstance(isvc, 0, true /* ready */, true /* serving */)
	wedged := crashLoopingPod(isvc, "CrashLoopBackOff", time.Hour)
	wedged.Name += "-1"
	wedged.Labels[query.LabelPodOrdinal] = "1"

	c := newFakeClient(t, isvc, ir, healthy, wedged)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone
	plan.Instances[0].Runners = []workload.RunnerPlan{{Name: "default", Size: 2}}

	pods := []*corev1.Pod{healthy, wedged}
	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], pods)
	if !needs {
		t.Fatal("a gang wedged on one member must open a repair for the Instance")
	}
	if !strings.Contains(reason, wedged.Name) {
		t.Errorf("the repair must name the wedged member; got %q", reason)
	}
	if !ops.RestartOpensUnavailability(input, plan.Instances[0], pods) {
		t.Error("the gang repair takes the Instance offline, so it must be admitted like any fresh attempt")
	}
}

// Once the repair is stamped it owns the pod set, and the stamp is
// idempotent: the same wedged pod re-observed on the next pass is the
// same evidence the repair already acted on, so nothing is armed a
// second time — no fresh incarnation, no second operation.
func TestRestart_ReObservedCrashLoopArmsNothingASecondTime(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision = crashLoopRevision
	pod := crashLoopingPod(isvc, "CrashLoopBackOff", time.Hour)
	c := newFakeClient(t, isvc, ir, pod)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone

	// The stamp lands: the row leaves Ready at a bumped incarnation.
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "pod CrashLoopBackOff"); err != nil {
		t.Fatalf("Restart (stamp): %v", err)
	}
	armed := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if armed.Phase != v1beta1.OMENativeInstanceRestarting || armed.Incarnation != 2 {
		t.Fatalf("stamp: got Phase=%q Incarnation=%d want Restarting at 2", armed.Phase, armed.Incarnation)
	}

	// The same reason is read again on the next pass, off the same pod.
	resetExpectations(t)
	input = buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan = buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "pod CrashLoopBackOff"); err != nil {
		t.Fatalf("Restart (re-observation): %v", err)
	}

	again := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if again.Incarnation != armed.Incarnation {
		t.Errorf("Incarnation: got %d want %d (the stamp is idempotent)", again.Incarnation, armed.Incarnation)
	}
	if again.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("Phase: got %q want Restarting (the repair still owns the row)", again.Phase)
	}
	if again.Operation == nil || again.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("Operation: got %+v want the one open repair", again.Operation)
	}
}

// Losing the wedged pod in the same pass that stamps the repair only
// completes work the repair was going to do: the drain has nothing left
// to flip or delete, so the stamp lands unchanged. From there the
// repair owns the pod set and recreates the missing member at the
// bumped incarnation, which is the same answer whether the pod was lost
// at the stamp or after it.
func TestRestart_PodLostAsTheRepairStampCommitsStillDrains(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	ir.Status.InstanceStatuses[0].RunningRevision = crashLoopRevision
	// The pod the crash-loop trigger read is gone by the time the stamp
	// commits, so the pass sees the index with no pods at all.
	c := newFakeClient(t, isvc, ir, fixtureRevision(t, isvc))
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone

	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "pod CrashLoopBackOff"); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	s := instanceStatusesOnIR(c, isvc, workload.ComponentEngine)[0]
	if s.Phase != v1beta1.OMENativeInstanceRestarting {
		t.Errorf("Phase: got %q want Restarting (the stamp lands whatever happened to the pod)", s.Phase)
	}
	if s.Incarnation != 2 {
		t.Errorf("Incarnation: got %d want 2", s.Incarnation)
	}
	if s.Operation == nil || s.Operation.Type != v1beta1.InstanceOperationRestart {
		t.Fatalf("Operation: got %+v want an open repair", s.Operation)
	}

	// The repair owns the pod set from here: the next pass rebuilds the
	// missing member at the bumped incarnation.
	resetExpectations(t)
	input = buildTestInput(isvc, c, workload.ComponentEngine)
	input.StuckPodGrace = time.Minute
	input.ObservedState.CurrentRevision = crashLoopRevision
	plan = buildPlanSinglePodEngineForRestart(c, isvc)
	plan.RestartPolicy = workload.RestartPolicyNone
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], nil, "pod CrashLoopBackOff"); err != nil {
		t.Fatalf("Restart (rebuild): %v", err)
	}

	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods: got %d want 1 (the repair recreates the lost member)", len(pods.Items))
	}
	if got := pods.Items[0].Labels[query.LabelInstanceIncarnation]; got != "2" {
		t.Errorf("new pod %s incarnation label: got %q want 2", pods.Items[0].Name, got)
	}
}

// A new target revision meets a crash-loop repair in one of two windows.
// While the repair is open the target waits: the update trigger declines
// a row below Ready, and the roll re-enters from Ready or from Failed once
// the repair resolves. Before the repair has opened, the row is a Ready
// row off the target, so the trigger fires and the roll takes the row —
// the repair never opens on it.
func TestDetectUpdateTrigger_NewTargetAgainstACrashLoopRepair(t *testing.T) {
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
		Namespace: "prod", Name: "llama-70b-engine-newtarget",
	}}

	t.Run("repair open: the target waits", func(t *testing.T) {
		resetExpectations(t)
		isvc := minimalISVC("llama-70b", "prod", 1)
		ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
			Index:           0,
			Incarnation:     2,
			Phase:           v1beta1.OMENativeInstanceRestarting,
			RunningRevision: crashLoopRevision,
			Operation:       &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "pod CrashLoopBackOff"},
		})
		c := newFakeClient(t, isvc, ir)
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		input.ObservedState.CurrentRevision = crashLoopRevision
		plan := buildPlanSinglePodEngineForRestart(c, isvc)

		trigger, _, err := ops.DetectUpdateTriggerWithPods(context.Background(), workload.Deps{Client: c},
			input, plan, plan.Instances[0], target, nil)
		if err != nil {
			t.Fatalf("DetectUpdateTriggerWithPods: %v", err)
		}
		if trigger {
			t.Fatalf("a row below Ready must not be rolled; the new target waits for the open repair")
		}
	})

	t.Run("repair not yet open: the roll takes the row", func(t *testing.T) {
		input, plan, pod := crashLoopInput(t, time.Minute)
		if needs, _ := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); !needs {
			t.Fatalf("the fixture must hold a crash loop the repair would otherwise open on")
		}
		if !ops.RepairYieldsToTarget(input, plan, plan.Instances[0], target, []*corev1.Pod{pod}) {
			t.Fatalf("a Ready row off the target must be yielded to the roll")
		}
		dec := ops.EvaluateUpdateTrigger(input, plan.Instances[0], target, []*corev1.Pod{pod})
		if !dec.Trigger {
			t.Fatalf("the update trigger must take the off-target row; decision = %+v", dec)
		}
	})
}

// gangMarkerRevision is the replacement gang's target.
const gangMarkerRevision = "llama-70b-engine-gangtgt1"

// gangMarkerRow is the surge target marker at markerIndex: Creating with
// an Update operation whose step says whose lifecycle owns it.
func gangMarkerRow(markerIndex int32, step string) *workload.InstanceStatus {
	return &workload.InstanceStatus{
		Index:          markerIndex,
		Incarnation:    1,
		Phase:          workload.InstancePhaseCreating,
		PodCount:       2,
		TargetRevision: gangMarkerRevision,
		Operation: &workload.InstanceOperation{
			Type:           workload.InstanceOperationUpdate,
			Step:           step,
			TargetRevision: gangMarkerRevision,
		},
	}
}

// TestDetectRestartTrigger_GangSurgeMarkerIsNotRepairedOnItsOwn: the
// restart trigger declines a row whose non-Create operation is
// preserved, so a terminal replacement pod is not recreated behind the
// surge's back. The marker converges only through the stuck or deadline
// escalation, and the same holds once it has been retired.
func TestDetectRestartTrigger_GangSurgeMarkerIsNotRepairedOnItsOwn(t *testing.T) {
	for _, step := range []string{
		workload.UpdateStepGangSurgeTarget,
		workload.UpdateStepGangSurgeTargetCleanup,
	} {
		for _, phase := range []corev1.PodPhase{corev1.PodFailed, corev1.PodSucceeded} {
			t.Run(step+"/"+string(phase), func(t *testing.T) {
				runGangMarkerTerminalPodCase(t, step, phase)
			})
		}
	}
}

// runGangMarkerTerminalPodCase drives one (marker step, terminal phase)
// pair through the restart trigger.
func runGangMarkerTerminalPodCase(t *testing.T, step string, phase corev1.PodPhase) {
	t.Helper()
	markerIndex := int32(2)
	marker := gangMarkerRow(markerIndex, step)
	input := workload.ReconcileInput{
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: []workload.InstanceStatus{*marker},
			CurrentRevision:  gangMarkerRevision,
		},
		StuckPodGrace: time.Minute,
	}
	plan := workload.ComponentPlan{
		RestartPolicy: workload.RestartPolicyRecreateInstance,
		Instances: []workload.InstancePlan{{
			Index: markerIndex, Incarnation: 1,
			Runners: []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		}},
	}
	terminal := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "llama-70b-engine-2-leader-0",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Status: corev1.PodStatus{Phase: phase},
	}

	if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{terminal}); needs {
		t.Fatalf("the marker's pods belong to the surge, not the repair; got reason %q", reason)
	}
}

// TestDetectRestartTrigger_PromotedGangTargetOwnsAMissingMember: the
// promote clears the operation and stamps the index Ready, which is an
// ordinary Instance in every respect. Losing a member as that lands, or
// at any point after it, is therefore the restart pass's business — the
// surge has no claim on the index any more.
func TestDetectRestartTrigger_PromotedGangTargetOwnsAMissingMember(t *testing.T) {
	markerIndex := int32(2)
	promoted := workload.InstanceStatus{
		Index:           markerIndex,
		Incarnation:     1,
		Phase:           workload.InstancePhaseReady,
		PodCount:        2,
		RunningRevision: gangMarkerRevision,
		ReadySince:      &metav1.Time{Time: time.Now().Add(-time.Hour)},
	}
	input := workload.ReconcileInput{
		ObservedState: workload.WorkloadObservedState{
			InstanceStatuses: []workload.InstanceStatus{promoted},
			CurrentRevision:  gangMarkerRevision,
		},
		StuckPodGrace: time.Minute,
	}
	plan := workload.ComponentPlan{
		RestartPolicy: workload.RestartPolicyRecreateInstance,
		Instances: []workload.InstancePlan{{
			Index: markerIndex, Incarnation: 1,
			Runners: []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 1}},
		}},
	}
	survivor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "llama-70b-engine-2-leader-0",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{survivor})
	if !needs {
		t.Fatalf("a promoted gang index missing a member must be repaired by the restart pass")
	}
	if reason == "" {
		t.Errorf("the repair must name why it was opened")
	}
}

// A settled Pending row that still records the revision it ran is a Ready
// row demoted for losing every pod. Under RecreateInstanceOnPodRestart the
// pod-count trigger reads it exactly as it reads Ready, so the loss is
// repaired at that revision with the incarnation bump instead of falling
// to a first materialization at the target. A row that never ran a
// revision, a row holding its pod set, an operation-owned row, and every
// other policy are left where they were, and pod-level evidence stays
// anchored on Ready.
func TestDetectRestartTrigger_DemotedRowRepairsAtRunningRevision(t *testing.T) {
	demoted := func(t *testing.T, mutate func(*v1beta1.OMENativeInstanceStatus)) (workload.ReconcileInput, workload.ComponentPlan, *v1beta1.InferenceService) {
		t.Helper()
		resetExpectations(t)
		isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
		row := &ir.Status.InstanceStatuses[0]
		row.Phase = v1beta1.OMENativeInstancePending
		row.RunningRevision = "llama-70b-engine-" + testRevisionHash
		if mutate != nil {
			mutate(row)
		}
		c := newFakeClient(t, isvc, ir)
		input := buildTestInput(isvc, c, workload.ComponentEngine)
		plan := buildPlanSinglePodEngineForRestart(c, isvc)
		return input, plan, isvc
	}

	t.Run("no pods opens the repair", func(t *testing.T) {
		input, plan, _ := demoted(t, nil)
		needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil)
		if !needs || reason != "pod count 0 below desired 1" {
			t.Fatalf("trigger = (%t, %q), want the pod-count repair", needs, reason)
		}
	})

	t.Run("a policy other than RecreateInstance leaves it to Create", func(t *testing.T) {
		input, plan, _ := demoted(t, nil)
		plan.RestartPolicy = workload.RestartPolicyNone
		if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil); needs {
			t.Fatalf("policy None repairs nothing; got reason %q", reason)
		}
	})

	t.Run("a row that never ran a revision is a first materialization", func(t *testing.T) {
		input, plan, _ := demoted(t, func(row *v1beta1.OMENativeInstanceStatus) { row.RunningRevision = "" })
		if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil); needs {
			t.Fatalf("a row with no running revision belongs to Create; got reason %q", reason)
		}
	})

	t.Run("a row holding its pod set is not a loss", func(t *testing.T) {
		input, plan, isvc := demoted(t, nil)
		pod := podAtIncarnation(isvc, 0, 1, true, true)
		if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
			t.Fatalf("a demoted row whose pods are all present has nothing to repair; got reason %q", reason)
		}
	})

	t.Run("pod evidence stays anchored on Ready", func(t *testing.T) {
		readySince := metav1.NewTime(time.Now().Add(-time.Hour))
		input, plan, isvc := demoted(t, func(row *v1beta1.OMENativeInstanceStatus) { row.ReadySince = &readySince })
		pod := podAtIncarnation(isvc, 0, 1, true, true)
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{
			runnerStatus(constants.MainContainerName, readySince.Add(20*time.Minute), nil),
		}
		if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{pod}); needs {
			t.Fatalf("a container restart below Ready is the boot path; got reason %q", reason)
		}
	})

	t.Run("an operation keeps the row with its owner", func(t *testing.T) {
		input, plan, _ := demoted(t, func(row *v1beta1.OMENativeInstanceStatus) {
			row.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepSurge}
		})
		if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil); needs {
			t.Fatalf("an Update-owned row is not the restart pass's to repair; got reason %q", reason)
		}
	})

	t.Run("a block held against the running revision denies the rebuild", func(t *testing.T) {
		input, plan, _ := demoted(t, nil)
		input.ObservedState.RetryBlocks = []workload.RetryBlock{{TargetRevision: "llama-70b-engine-" + testRevisionHash, State: workload.RetryBlockHeld}}
		if needs, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil); needs {
			t.Fatalf("below Ready a rebuild answers to the revision's RetryBlock; got reason %q", reason)
		}
	})

	t.Run("a block on another revision does not deny it", func(t *testing.T) {
		input, plan, _ := demoted(t, nil)
		input.ObservedState.RetryBlocks = []workload.RetryBlock{{TargetRevision: "llama-70b-engine-0000other", State: workload.RetryBlockHeld}}
		if needs, _ := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], nil); !needs {
			t.Fatalf("a block against a revision the row never ran must not hold its repair")
		}
	})
}

// The repair of a demoted row is the ordinary Restart: the incarnation is
// bumped, the row moves to Restarting, and the pod set is recreated at the
// revision the row records rather than at the Component's target, which
// the pause withholds.
func TestRestart_DemotedRowRebuildsAtRunningRevision(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	running := "llama-70b-engine-" + testRevisionHash
	ir.Status.InstanceStatuses[0].Phase = v1beta1.OMENativeInstancePending
	ir.Status.InstanceStatuses[0].RunningRevision = running
	c := newFakeClient(t, isvc, ir, fixtureRevision(t, isvc))
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-0000target", Namespace: "prod"}}
	input.ObservedState.UpdateRevision = target.Name
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	// A demoted row off an open target is the Create pass's; the repair
	// reaches it under a pause, which withholds the roll.
	plan.Paused = true

	done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], target, "pod count 0 below desired 1")
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if done {
		t.Fatalf("expected done=false while the rebuilt pod is not yet Ready")
	}

	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceRestarting || s.Incarnation != 2 || s.RunningRevision != running {
		t.Fatalf("row after the first pass = %+v, want Restarting at incarnation 2 still recording %s", s, running)
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods: got %d want 1", len(pods.Items))
	}
	labels := pods.Items[0].Labels
	if got := labels[query.LabelInstanceIncarnation]; got != "2" {
		t.Errorf("rebuilt pod incarnation label = %q, want 2", got)
	}
	if want := query.RevisionFromName(running).Hash(); labels[query.LabelRevisionHash] != want {
		t.Errorf("rebuilt pod revision label = %q, want the running revision %q, never the target", labels[query.LabelRevisionHash], want)
	}
}

// The restart pass repairs an Instance only at a revision the Component
// still wants. RepairYieldsToTarget is the selection's question: a row
// whose rebuild revision is not the roll target is left to the pass that
// rebuilds it AT the target — the update pass for a Ready or Failed row,
// the Create pass for a superseded first materialization and for a
// demoted row with no pod left. An open Restart, a Migrate-owned row and
// a demoted row that holds pods again are never yielded.
func TestRepairYieldsToTarget(t *testing.T) {
	const running = "llama-70b-engine-" + testRevisionHash
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "llama-70b-engine-0000next", Namespace: "prod"}}
	sameTarget := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: running, Namespace: "prod"}}
	survivor := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "survivor"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	dead := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "dead"}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	inst := workload.InstancePlan{Index: 0, Incarnation: 1, Runners: []workload.RunnerPlan{{Name: "default", Size: 1}}}

	cases := []struct {
		name   string
		row    *workload.InstanceStatus
		target *appsv1.ControllerRevision
		pods   []*corev1.Pod
		paused bool
		blocks []workload.RetryBlock
		want   bool
	}{
		{name: "Ready on the target is repaired", row: &workload.InstanceStatus{Phase: workload.InstancePhaseReady, RunningRevision: running}, target: sameTarget, want: false},
		{name: "Ready off the target yields", row: &workload.InstanceStatus{Phase: workload.InstancePhaseReady, RunningRevision: running}, target: target, want: true},
		{name: "a paused Component keeps the repair", row: &workload.InstanceStatus{Phase: workload.InstancePhaseReady, RunningRevision: running}, target: target, paused: true, want: false},
		{name: "a target the RetryBlock holds keeps the repair", row: &workload.InstanceStatus{Phase: workload.InstancePhaseReady, RunningRevision: running}, target: target,
			blocks: []workload.RetryBlock{{TargetRevision: target.Name, State: workload.RetryBlockHeld}}, want: false},
		{name: "a target in Backoff only paces the roll and yields", row: &workload.InstanceStatus{Phase: workload.InstancePhaseReady, RunningRevision: running}, target: target,
			blocks: []workload.RetryBlock{{TargetRevision: target.Name, State: workload.RetryBlockBackoff}}, want: true},
		{name: "Failed off the target yields", row: &workload.InstanceStatus{Phase: workload.InstancePhaseFailed, RunningRevision: running}, target: target, want: true},
		{name: "a superseded first materialization yields to its retirement", row: &workload.InstanceStatus{
			Phase:     workload.InstancePhaseCreating,
			Operation: &workload.InstanceOperation{Type: workload.InstanceOperationCreate, Step: "CreatePods", TargetRevision: running},
		}, target: target, pods: []*corev1.Pod{survivor}, want: true},
		{name: "an open Restart is driven whatever the target", row: &workload.InstanceStatus{
			Phase: workload.InstancePhaseRestarting, RunningRevision: running,
			Operation: &workload.InstanceOperation{Type: workload.InstanceOperationRestart, Step: workload.RestartStepDrain},
		}, target: target, want: false},
		{name: "a Migrate-owned row is its record's", row: &workload.InstanceStatus{Phase: workload.InstancePhaseMigrating, RunningRevision: running}, target: target, want: false},
		{name: "a demoted row with no pod left yields to Create", row: &workload.InstanceStatus{Phase: workload.InstancePhasePending, RunningRevision: running}, target: target, want: true},
		{name: "a demoted row whose only pod is terminal yields to Create", row: &workload.InstanceStatus{Phase: workload.InstancePhasePending, RunningRevision: running}, target: target, pods: []*corev1.Pod{dead}, want: true},
		{name: "a demoted row holding a survivor keeps its repair", row: &workload.InstanceStatus{Phase: workload.InstancePhasePending, RunningRevision: running}, target: target, pods: []*corev1.Pod{survivor}, want: false},
		{name: "a row recording no revision has nothing to compare", row: &workload.InstanceStatus{Phase: workload.InstancePhaseReady}, target: target, want: false},
		{name: "no target, nothing to yield to", row: &workload.InstanceStatus{Phase: workload.InstancePhaseReady, RunningRevision: running}, target: nil, want: false},
		{name: "no row", row: nil, target: target, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := workload.ReconcileInput{}
			input.ObservedState.RetryBlocks = tc.blocks
			if tc.row != nil {
				row := *tc.row
				row.Index = 0
				input.ObservedState.InstanceStatuses = []workload.InstanceStatus{row}
			}
			plan := workload.ComponentPlan{Component: workload.ComponentEngine, Paused: tc.paused}
			if got := ops.RepairYieldsToTarget(input, plan, inst, tc.target, tc.pods); got != tc.want {
				t.Fatalf("RepairYieldsToTarget = %v, want %v", got, tc.want)
			}
		})
	}
}

// mintEngineRevision stores spec (and workerSpec, for a gang) as an engine
// ControllerRevision through the production revision machinery, so the
// name, the hash and the stored payload are what the adapter writes.
func mintEngineRevision(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, spec, workerSpec *corev1.PodSpec) *appsv1.ControllerRevision {
	t.Helper()
	key := revision.Key{Namespace: isvc.Namespace, Name: isvc.Name + "-" + string(workload.ComponentEngine)}
	cr, _, err := revision.EnsureControllerRevisionWithWorker(context.Background(), c, c, isvc,
		v1beta1.SchemeGroupVersion.WithKind("InferenceService"), key, spec, workerSpec, nil, nil, isvc.UID)
	if err != nil {
		t.Fatalf("mint revision: %v", err)
	}
	return cr
}

// restartingRow rewrites the engine Instance 0 row as an open Restart at
// incarnation 2 running rev, with the old pods already gone: the shape
// Phase B of the repair rebuilds from.
func restartingRow(t *testing.T, c client.Client, isvc *v1beta1.InferenceService, rev string) {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: irName(isvc, workload.ComponentEngine)}
	if err := c.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("get IR: %v", err)
	}
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{
		Index:           0,
		Incarnation:     2,
		Phase:           v1beta1.OMENativeInstanceRestarting,
		RunningRevision: rev,
		Operation:       &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationRestart, Step: "Drain", Reason: "pod count 0 below desired 1"},
	}}
	if err := c.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("stamp Restarting row: %v", err)
	}
}

// TestRestart_RendersTheStampedRevisionTemplate: the rebuild renders the
// template the stamped revision stores, not the Component's current one.
// The running revision was minted with one image and the desired
// template has moved to another under a pause, which withholds the roll
// the rebuild would otherwise follow, so a pod rendered from the wrong
// source is visible by its image: it must carry the running revision's
// image under the running revision's label, for a single pod and for
// both members of a gang.
func TestRestart_RendersTheStampedRevisionTemplate(t *testing.T) {
	const (
		runningImage       = "registry.example.com/runtime:v1"
		runningWorkerImage = "registry.example.com/runtime-worker:v1"
		movedImage         = "registry.example.com/runtime:v2"
		movedWorkerImage   = "registry.example.com/runtime-worker:v2"
	)
	cases := []struct {
		name string
		gang bool
	}{
		{name: "single pod"},
		{name: "gang", gang: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetExpectations(t)
			isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
			c := newFakeClient(t, isvc, ir)
			var workerSpec *corev1.PodSpec
			if tc.gang {
				workerSpec = &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: runningWorkerImage}}}
			}
			running := mintEngineRevision(t, c, isvc, &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: runningImage}}}, workerSpec)
			restartingRow(t, c, isvc, running.Name)

			input := buildTestInput(isvc, c, workload.ComponentEngine)
			input.DesiredSpec.PodSpec = &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: movedImage}}}
			var plan workload.ComponentPlan
			if tc.gang {
				input.DesiredSpec.WorkerPodSpec = &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: movedWorkerImage}}}
				plan = buildPlanGangEngine(workload.RestartPolicyRecreateInstance)
			} else {
				plan = buildPlanSinglePodEngineForRestart(c, isvc)
			}
			target := mintEngineRevision(t, c, isvc, input.DesiredSpec.PodSpec, input.DesiredSpec.WorkerPodSpec)
			input.ObservedState.UpdateRevision = target.Name
			plan.Paused = true

			done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], target, "pod count 0 below desired 1")
			if err != nil {
				t.Fatalf("Restart: %v", err)
			}
			if done {
				t.Fatalf("expected done=false while the rebuilt pods are not yet Ready")
			}
			pods := &corev1.PodList{}
			if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			want := 1
			if tc.gang {
				want = 2
			}
			if len(pods.Items) != want {
				t.Fatalf("pods: got %d want %d", len(pods.Items), want)
			}
			for _, pod := range pods.Items {
				if got := pod.Labels[query.LabelRevisionHash]; got != query.RevisionOf(running).Hash() {
					t.Errorf("pod %s revision label = %q, want the running revision %q", pod.Name, got, query.RevisionOf(running).Hash())
				}
				wantImage := runningImage
				if pod.Labels[query.LabelRunner] == "worker" {
					wantImage = runningWorkerImage
				}
				if got := pod.Spec.Containers[0].Image; got != wantImage {
					t.Errorf("pod %s image = %q, want %q: the rebuild must render the stamped revision's template, not the current one", pod.Name, got, wantImage)
				}
			}
		})
	}
}

// TestRestart_RebuildWithNoPodYetFollowsTheTarget: an open repair whose
// rebuild has created no pod yet renders the roll target when the row is
// off it, and the row records the target first: the pods come back on the
// target's image under the target's label and the row's running revision
// names it, for a single pod and for both members of a gang. The rebuild
// keeps the revision it stamped once a pod of its own exists, and when the
// RetryBlock holds the target.
func TestRestart_RebuildWithNoPodYetFollowsTheTarget(t *testing.T) {
	const (
		runningImage = "registry.example.com/runtime:v1"
		movedImage   = "registry.example.com/runtime:v2"
	)
	cases := []struct {
		name string
		gang bool
		// leaderRebuilt seeds a pod of the rebuild before the pass.
		leaderRebuilt bool
		// held puts the target under a Held RetryBlock.
		held    bool
		follows bool
	}{
		{name: "single pod", follows: true},
		{name: "gang", gang: true, follows: true},
		{name: "gang whose leader is already rebuilt", gang: true, leaderRebuilt: true},
		{name: "target the RetryBlock holds", held: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetExpectations(t)
			isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
			c := newFakeClient(t, isvc, ir)
			spec := func(image string) *corev1.PodSpec {
				return &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: image}}}
			}
			var runningWorker, movedWorker *corev1.PodSpec
			if tc.gang {
				runningWorker, movedWorker = spec(runningImage), spec(movedImage)
			}
			running := mintEngineRevision(t, c, isvc, spec(runningImage), runningWorker)
			target := mintEngineRevision(t, c, isvc, spec(movedImage), movedWorker)
			restartingRow(t, c, isvc, running.Name)
			if tc.leaderRebuilt {
				leader := gangPod(isvc, 0, "leader", 0, 2, false, false)
				leader.Labels[query.LabelRevisionHash] = query.RevisionOf(running).Hash()
				if err := c.Create(context.Background(), leader); err != nil {
					t.Fatalf("seed the rebuilt leader: %v", err)
				}
			}

			input := buildTestInput(isvc, c, workload.ComponentEngine)
			input.DesiredSpec.PodSpec = spec(movedImage)
			input.DesiredSpec.WorkerPodSpec = movedWorker
			input.ObservedState.UpdateRevision = target.Name
			if tc.held {
				input.ObservedState.RetryBlocks = []workload.RetryBlock{{TargetRevision: target.Name, State: workload.RetryBlockHeld}}
			}
			plan := buildPlanSinglePodEngineForRestart(c, isvc)
			if tc.gang {
				plan = buildPlanGangEngine(workload.RestartPolicyRecreateInstance)
			}

			done, err := ops.Restart(context.Background(), workload.Deps{Client: c}, input, plan, plan.Instances[0], target, "pod count 0 below desired 1")
			if err != nil {
				t.Fatalf("Restart: %v", err)
			}
			if done {
				t.Fatalf("expected done=false while the rebuilt pods are not yet Ready")
			}

			wantRevision, wantImage := running, runningImage
			if tc.follows {
				wantRevision, wantImage = target, movedImage
			}
			pods := &corev1.PodList{}
			if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			if want := len(plan.Instances[0].Runners); len(pods.Items) != want {
				t.Fatalf("pods: got %d want %d", len(pods.Items), want)
			}
			for _, pod := range pods.Items {
				if tc.leaderRebuilt && pod.Labels[query.LabelRunner] == "leader" {
					continue
				}
				if got := pod.Labels[query.LabelRevisionHash]; got != query.RevisionOf(wantRevision).Hash() {
					t.Errorf("pod %s revision label = %q, want %s", pod.Name, got, wantRevision.Name)
				}
				if got := pod.Spec.Containers[0].Image; got != wantImage {
					t.Errorf("pod %s image = %q, want %q", pod.Name, got, wantImage)
				}
			}
			row := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
			if row == nil || row.Phase != v1beta1.OMENativeInstanceRestarting || row.RunningRevision != wantRevision.Name {
				t.Fatalf("row = %+v, want Restarting recording %s", row, wantRevision.Name)
			}
		})
	}
}

// TestRestart_HoldsWhenTheStampedRevisionIsGone: a rebuild whose stamped
// revision has no ControllerRevision left creates nothing rather than
// rendering the current template under that revision's label. It says so
// once per attempt and leaves the attempt open for its deadline.
func TestRestart_HoldsWhenTheStampedRevisionIsGone(t *testing.T) {
	resetExpectations(t)
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	c := newFakeClient(t, isvc, ir)
	const gone = "llama-70b-engine-0000gone"
	restartingRow(t, c, isvc, gone)
	plan := buildPlanSinglePodEngineForRestart(c, isvc)
	rec := record.NewFakeRecorder(8)
	deps := workload.Deps{Client: c, Recorder: rec}

	for pass := 0; pass < 2; pass++ {
		fresh := &v1beta1.InferenceService{}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(isvc), fresh); err != nil {
			t.Fatalf("get isvc: %v", err)
		}
		input := buildTestInput(fresh, c, workload.ComponentEngine)
		done, err := ops.Restart(context.Background(), deps, input, plan, plan.Instances[0], nil, "pod count 0 below desired 1")
		if err != nil {
			t.Fatalf("pass %d: Restart: %v", pass, err)
		}
		if done {
			t.Fatalf("pass %d: a rebuild with no template to render from cannot be done", pass)
		}
	}
	pods := &corev1.PodList{}
	if err := c.List(context.Background(), pods, client.InNamespace("prod")); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("the rebuild created %d pod(s) from the current template under the gone revision's label", len(pods.Items))
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceRestarting || s.Operation == nil || s.RunningRevision != gone {
		t.Fatalf("row = %+v, want the Restart left open on %s", s, gone)
	}
	var warnings []string
	for {
		select {
		case e := <-rec.Events:
			if strings.Contains(e, string(workload.EventReasonRepairRevisionGone)) {
				warnings = append(warnings, e)
			}
			continue
		default:
		}
		break
	}
	if len(warnings) != 1 {
		t.Fatalf("%s events = %v, want exactly one across two passes", workload.EventReasonRepairRevisionGone, warnings)
	}
	if !strings.Contains(warnings[0], gone) || !strings.HasPrefix(warnings[0], "Warning ") {
		t.Fatalf("event %q must be a Warning naming the revision %s", warnings[0], gone)
	}
}

// A crash loop the running revision's ladder holds, on a single-pod
// Instance whose runner keeps dying after serving and coming back: the
// hold parks the row while the pod is down and names the loop while it serves.

// crashLoopWindow is the proven window of these stories: the stuck-pod
// grace with no minReadySeconds configured.
const crashLoopWindow = 90 * time.Second

// heldCrashLoop is such an Instance: an operation-free row on
// crashLoopRevision remembering its set's crash, and that set's pod, back
// up (Ready for readyFor) when serving, else parked in CrashLoopBackOff.
type heldCrashLoop struct {
	t          *testing.T
	isvc       *v1beta1.InferenceService
	c          client.Client
	clk        *clocktesting.FakeClock
	pod        *corev1.Pod
	plan       workload.ComponentPlan
	readySince metav1.Time
	crashedAt  metav1.Time
	warnings   []string
}

func newHeldCrashLoop(t *testing.T, phase v1beta1.OMENativeInstancePhase, serving bool, readyFor time.Duration) *heldCrashLoop {
	t.Helper()
	resetExpectations(t)
	now := time.Now().Truncate(time.Second)
	readySince := metav1.NewTime(now.Add(-time.Hour))
	crashedAt := metav1.NewTime(readySince.Add(20 * time.Minute))
	isvc, ir := isvcReadyAtIncarnation("llama-70b", "prod", 1)
	pod := podAtIncarnation(isvc, 0, 1, serving, true /* serving gate */)
	pod.Spec.Containers[0].Name = constants.MainContainerName
	pod.CreationTimestamp = metav1.NewTime(readySince.Add(-5 * time.Minute))
	pod.Status.Phase = corev1.PodRunning
	exit := int32(1)
	crashed := &corev1.ContainerStateTerminated{
		Reason: "Error", ExitCode: exit,
		StartedAt:  metav1.NewTime(readySince.Add(5 * time.Minute)),
		FinishedAt: crashedAt,
	}
	if serving {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{runnerStatus(constants.MainContainerName, crashedAt.Add(time.Minute), crashed)}
		pod.Status.ContainerStatuses[0].Ready = true
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(now.Add(-readyFor)),
		})
	} else {
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:                 constants.MainContainerName,
			RestartCount:         2,
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off restarting failed container"}},
			LastTerminationState: corev1.ContainerState{Terminated: crashed},
		}}
		down := metav1.NewTime(now.Add(-time.Minute))
		pod.Status.Conditions = append(pod.Status.Conditions,
			corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: down},
			corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: down})
	}
	row := &ir.Status.InstanceStatuses[0]
	row.Phase = phase
	row.ReadySince = &readySince
	row.RunningRevision = crashLoopRevision
	row.LastFailure = &v1beta1.InstanceTermination{
		PodName: pod.Name, ContainerName: constants.MainContainerName, Reason: "Error", ExitCode: &exit,
		Message: "back-off restarting failed container",
		Time:    crashedAt,
	}
	c := newFakeClient(t, isvc, ir, pod)
	h := &heldCrashLoop{t: t, isvc: isvc, c: c, clk: clocktesting.NewFakeClock(now), pod: pod, readySince: readySince, crashedAt: crashedAt}
	h.plan = buildPlanSinglePodEngineForRestart(c, isvc)
	return h
}

// input is a pass's view of the row as persisted. held puts the running
// revision's ladder in Held, with the row's crash counted on it.
func (h *heldCrashLoop) input(held bool) workload.ReconcileInput {
	h.t.Helper()
	in := buildTestInput(h.isvc, h.c, workload.ComponentEngine)
	in.Clock = h.clk
	in.StuckPodGrace = crashLoopWindow
	in.ObservedState.UpdateRevision = crashLoopRevision
	in.ObservedState.CurrentRevision = crashLoopRevision
	in.DesiredSpec.PodSpec = &corev1.PodSpec{Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "test:v1"}}}
	if held {
		first := metav1.NewTime(h.readySince.Add(10 * time.Minute))
		last := h.crashedAt
		in.ObservedState.RetryBlocks = []workload.RetryBlock{{
			TargetRevision: crashLoopRevision, State: workload.RetryBlockHeld, AttemptsStarted: 3,
			FirstFailureAt: &first, LastFailureAt: &last, Reason: "Error",
		}}
	}
	in.WarnInstanceFailed = func(idx int32, podName, reason string) {
		h.warnings = append(h.warnings, fmt.Sprintf("instance=%d pod=%s %s", idx, podName, reason))
	}
	return in
}

// pass runs the restart pass's selection and, when it selects the row,
// its Restart, returning whether the row was selected and whether the
// Restart reported done.
func (h *heldCrashLoop) pass(in workload.ReconcileInput) (selected, done bool) {
	h.t.Helper()
	needs, reason := ops.DetectRestartTriggerWithPods(in, h.plan, h.plan.Instances[0], []*corev1.Pod{h.pod})
	if !needs {
		return false, false
	}
	done, err := ops.Restart(context.Background(), workload.Deps{Client: h.c}, in, h.plan, h.plan.Instances[0], nil, reason)
	if err != nil {
		h.t.Fatalf("Restart: %v", err)
	}
	return true, done
}

func (h *heldCrashLoop) row() v1beta1.OMENativeInstanceStatus {
	h.t.Helper()
	rows := instanceStatusesOnIR(h.c, h.isvc, workload.ComponentEngine)
	if len(rows) != 1 {
		h.t.Fatalf("rows = %d, want 1", len(rows))
	}
	return rows[0]
}

// namesCrashLoop reports whether the row's failure record names the
// crash loop beside the kubelet's own message.
func namesCrashLoop(row v1beta1.OMENativeInstanceStatus) bool {
	return row.LastFailure != nil && strings.Contains(row.LastFailure.Message, "crash loop")
}

// A held ladder parks nothing a serving set sits on: the row stays Ready
// naming the crash loop, no warning fires, and it is not selected again.
func TestRestart_HeldLadderKeepsAServingSetReadyAndNamesTheLoop(t *testing.T) {
	h := newHeldCrashLoop(t, v1beta1.OMENativeInstanceReady, true /* serving */, 30*time.Second)

	selected, done := h.pass(h.input(true))
	if !selected {
		t.Fatalf("the hold must take the row once, to name the crash loop on it")
	}
	row := h.row()
	if row.Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("row reads %s while its only pod is Ready and serving; an Instance whose only pod serves is not Failed (warnings=%v)", row.Phase, h.warnings)
	}
	if !done {
		t.Fatalf("naming the loop leaves nothing in flight; got done=false")
	}
	if !namesCrashLoop(row) {
		t.Fatalf("the row must name the crash loop on its failure record; got %+v", row.LastFailure)
	}
	if row.ReadySince == nil || !row.ReadySince.Time.Equal(h.readySince.Time) {
		t.Fatalf("ReadySince must stay at the promotion the crash is measured from; got %v want %v", row.ReadySince, h.readySince)
	}
	if len(h.warnings) != 0 {
		t.Fatalf("no InstanceFailed warning while the set serves; got %v", h.warnings)
	}
	if selected, _ := h.pass(h.input(true)); selected {
		t.Fatalf("a named serving set must not be selected on every pass")
	}
}

// Once the set is out of serving the hold parks the row:
// Failed, no operation, the crash recorded, one warning naming the hold.
func TestRestart_HeldLadderParksTheSetOnceItLeavesServing(t *testing.T) {
	h := newHeldCrashLoop(t, v1beta1.OMENativeInstanceReady, false /* down */, 0)

	selected, done := h.pass(h.input(true))
	if !selected {
		t.Fatalf("a held ladder must park a set that is down")
	}
	if done {
		t.Fatalf("a park is not a repair that finished; got done=true")
	}
	row := h.row()
	if row.Phase != v1beta1.OMENativeInstanceFailed || row.Operation != nil {
		t.Fatalf("row = %s with operation %+v, want Failed with none", row.Phase, row.Operation)
	}
	if row.LastFailure == nil || row.LastFailure.PodName != h.pod.Name {
		t.Fatalf("the park must record the crashed pod; got %+v", row.LastFailure)
	}
	if len(h.warnings) != 1 || !strings.Contains(h.warnings[0], "held after 3 failed attempts") {
		t.Fatalf("one InstanceFailed warning naming the hold; got %v", h.warnings)
	}
}

// A park whose only pod is Ready and serving returns to Ready, named under
// the hold and plain after a release, anchors kept; a down pod stays parked.
func TestRestart_CrashLoopParkReturnsToReadyWhileItsSetServes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		held  bool
		named bool
	}{
		{name: "under the held ladder, naming the loop", held: true, named: true},
		{name: "after the hold is released, as plain Ready", held: false, named: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHeldCrashLoop(t, v1beta1.OMENativeInstanceFailed, true /* serving */, 30*time.Second)

			selected, done := h.pass(h.input(tc.held))
			if !selected {
				t.Fatalf("a crash-loop park whose only pod is Ready and serving must return to Ready; the restart pass left it Failed")
			}
			row := h.row()
			if row.Phase != v1beta1.OMENativeInstanceReady || row.Operation != nil {
				t.Fatalf("row = %s with operation %+v, want Ready with none", row.Phase, row.Operation)
			}
			if !done {
				t.Fatalf("the unpark leaves nothing in flight; got done=false")
			}
			if named := namesCrashLoop(row); named != tc.named {
				t.Fatalf("names the crash loop = %v, want %v; record %+v", named, tc.named, row.LastFailure)
			}
			if row.ReadySince == nil || !row.ReadySince.Time.Equal(h.readySince.Time) {
				t.Fatalf("ReadySince must stay at the promotion; got %v want %v", row.ReadySince, h.readySince)
			}
			if row.LastFailure == nil || !row.LastFailure.Time.Equal(&h.crashedAt) {
				t.Fatalf("the failure record keeps its time; got %+v", row.LastFailure)
			}
			if len(h.warnings) != 0 {
				t.Fatalf("no InstanceFailed warning on the unpark; got %v", h.warnings)
			}
		})
	}
	t.Run("while the pod is down it stays parked", func(t *testing.T) {
		h := newHeldCrashLoop(t, v1beta1.OMENativeInstanceFailed, false /* down */, 0)
		if selected, _ := h.pass(h.input(true)); selected {
			t.Fatalf("a park whose pod is down is not selected")
		}
		if row := h.row(); row.Phase != v1beta1.OMENativeInstanceFailed {
			t.Fatalf("row = %s, want Failed", row.Phase)
		}
	})
}

// The naming leaves on its own: a set that holds Ready for the proven
// window, or a released hold, clears the note and leaves plain Ready.
func TestRestart_ProvenSetClearsTheCrashLoopNote(t *testing.T) {
	for _, tc := range []struct {
		name    string
		held    bool
		advance time.Duration
	}{
		{name: "the set holds Ready for the window", held: true, advance: crashLoopWindow},
		{name: "the hold is released", held: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHeldCrashLoop(t, v1beta1.OMENativeInstanceReady, true /* serving */, 30*time.Second)
			if selected, _ := h.pass(h.input(true)); !selected {
				t.Fatalf("the hold must take the row once, to name the crash loop on it")
			}
			if row := h.row(); row.Phase != v1beta1.OMENativeInstanceReady || !namesCrashLoop(row) {
				t.Fatalf("row = %s record %+v, want Ready naming the crash loop", row.Phase, row.LastFailure)
			}

			h.clk.Step(tc.advance)
			selected, done := h.pass(h.input(tc.held))
			if !selected || !done {
				t.Fatalf("the stale note must be cleared in one pass; selected=%v done=%v", selected, done)
			}
			row := h.row()
			if row.Phase != v1beta1.OMENativeInstanceReady || namesCrashLoop(row) {
				t.Fatalf("row = %s record %+v, want plain Ready with the note cleared", row.Phase, row.LastFailure)
			}
			if row.LastFailure == nil || row.LastFailure.Message != "back-off restarting failed container" {
				t.Fatalf("the kubelet's own message stays as history; got %+v", row.LastFailure)
			}
			if len(h.warnings) != 0 {
				t.Fatalf("no InstanceFailed warning; got %v", h.warnings)
			}
			if tc.held {
				// Proven under the hold, the row is left alone; after a
				// release the running revision's ladder reads the row
				// afresh, which is its own story.
				if selected, _ := h.pass(h.input(true)); selected {
					t.Fatalf("a plain Ready row under the hold is not selected")
				}
			}
		})
	}
}
