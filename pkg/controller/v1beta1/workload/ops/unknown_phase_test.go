package ops

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// fdUnknownPod turns a force-delete fixture pod into one the kubelet has
// stopped reporting.
func fdUnknownPod(pod *corev1.Pod) *corev1.Pod {
	pod.Status.Phase = corev1.PodUnknown
	return pod
}

// fdWithdrawnPod turns a force-delete fixture pod into one the control
// plane marked not Ready while its own last report still says Ready: not
// Terminating, Running, ContainersReady and the serving gate True,
// PodReady False — a serving pod whose node stopped heartbeating.
func fdWithdrawnPod(pod *corev1.Pod) *corev1.Pod {
	pod.DeletionTimestamp = nil
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: query.ServingConditionType}}
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
			{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionFalse},
		},
	}
	return pod
}

// A serving pod whose Ready the control plane withdrew is swept like a
// phase-Unknown one: on a node unreachable past the threshold the sweep
// force-deletes it and the step withholds for the pass; on a node posting
// Ready the shape is the kubelet's lag behind the gate write, nothing is
// deleted, and the step is not withheld at all.
func TestRecoverUnknownPhaseTargets_WithdrawnReadyFollowsTheNode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		node    *corev1.Node
		holding bool
		deletes int
	}{
		{"node unreachable past the threshold", fdNodeUnreachable("node-a", 10*time.Minute), true, 1},
		{"node unreachable inside the threshold", fdNodeUnreachable("node-a", time.Minute), true, 0},
		{"node posting Ready", fdNodeReady("node-a"), false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var deletes []recordedDeleteOpts
			funcs := fdDeleteRecorder(&deletes)
			pod := fdWithdrawnPod(fdTerminatingPod("engine-0-default-0", "node-a", overdueTS))
			isvc := fdISVC("llama")
			c := fdFakeClient(t, &funcs, isvc, pod, tc.node)
			deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}

			holding, _, err := recoverUnknownPhaseTargets(context.Background(), deps, fdInput(isvc, fdPolicy()), 0,
				[]*corev1.Pod{pod}, []podTarget{{Name: pod.Name}})
			if err != nil {
				t.Fatalf("recoverUnknownPhaseTargets: %v", err)
			}
			if holding != tc.holding {
				t.Errorf("holding = %v, want %v", holding, tc.holding)
			}
			if len(deletes) != tc.deletes {
				t.Fatalf("deletes: got %+v want %d", deletes, tc.deletes)
			}
			if tc.deletes == 1 && (deletes[0].grace == nil || *deletes[0].grace != 0) {
				t.Errorf("GracePeriodSeconds: got %v want 0", deletes[0].grace)
			}
		})
	}
}

// A pod that is Terminating AND Unknown on a dead node reaches the
// rebuild paths through neither sweep of its own: the stuck-Terminating
// sweep is not wired into Restart Phase B or the Create batch, and the
// node-death arm declines a pod already on its way out. Routed to the
// Terminating arm, the same node-death evidence still frees the name
// instead of holding it with the attempt deadline parked.
func TestRecoverUnknownPhaseTargets_TerminatingPodTakesTheStuckSweep(t *testing.T) {
	var deletes []recordedDeleteOpts
	funcs := fdDeleteRecorder(&deletes)
	pod := fdUnknownPod(fdTerminatingPod("engine-0-default-0", "dead-node", overdueTS))
	isvc := fdISVC("llama")
	c := fdFakeClient(t, &funcs, isvc, fdStoredCopy(pod), fdNodeUnreachable("dead-node", 10*time.Minute))
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}
	input := fdInput(isvc, fdPolicy())

	holding, _, err := recoverUnknownPhaseTargets(context.Background(), deps, input, 0,
		[]*corev1.Pod{pod}, []podTarget{{Name: pod.Name}})
	if err != nil {
		t.Fatalf("recoverUnknownPhaseTargets: %v", err)
	}
	if !holding {
		t.Errorf("the step must not proceed on the pass that frees the name")
	}
	if len(deletes) != 1 || deletes[0].name != pod.Name {
		t.Fatalf("deletes: got %+v want one for %s", deletes, pod.Name)
	}
	if deletes[0].grace == nil || *deletes[0].grace != 0 {
		t.Errorf("GracePeriodSeconds: got %v want 0", deletes[0].grace)
	}
}

// The evidence for a silent node turns actionable on a clock, and a node
// that has stopped reporting emits no event to wake anyone on. Report
// the next policy boundary so a caller without a poll of its own can
// carry it into its requeue.
func TestRecoverUnknownPhaseTargets_ReportsTheNextPolicyBoundary(t *testing.T) {
	isvc := fdISVC("llama")
	pod := fdUnknownPod(fdTerminatingPod("engine-0-default-0", "dying-node", overdueTS))
	pod.DeletionTimestamp = nil
	policy := fdPolicy()
	// Unreachable for less than the threshold: evidence present, not yet
	// actionable, so the boundary is the remainder of the threshold.
	young := 2 * time.Minute
	c := fdFakeClient(t, nil, isvc, pod, fdNodeUnreachable("dying-node", young))
	deps := workload.Deps{Client: c, Recorder: record.NewFakeRecorder(16)}
	input := fdInput(isvc, policy)
	input.MutateInstance = func(context.Context, int32, func(*workload.InstanceStatus) bool) error { return nil }

	holding, at, err := recoverUnknownPhaseTargets(context.Background(), deps, input, 0,
		[]*corev1.Pod{pod}, []podTarget{{Name: pod.Name}})
	if err != nil {
		t.Fatalf("recoverUnknownPhaseTargets: %v", err)
	}
	if !holding {
		t.Errorf("the name is still held, so the step must not proceed")
	}
	want := fdNow.Add(policy.NodeUnreachableThreshold - young)
	if !at.Equal(want) {
		t.Errorf("next policy boundary: got %v want %v", at, want)
	}
	if wait := until(fdNow, at); wait <= 0 || wait > policy.NodeUnreachableThreshold {
		t.Errorf("requeue delay: got %v want a positive delay inside the threshold", wait)
	}
}
