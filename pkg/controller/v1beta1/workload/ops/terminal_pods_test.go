package ops_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

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
