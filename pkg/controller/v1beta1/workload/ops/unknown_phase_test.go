package ops

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// fdUnknownPod turns a force-delete fixture pod into one the kubelet has
// stopped reporting.
func fdUnknownPod(pod *corev1.Pod) *corev1.Pod {
	pod.Status.Phase = corev1.PodUnknown
	return pod
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
