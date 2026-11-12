package ops_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A repair opened for a runner that restarted after Ready records that
// exit even when a member of the gang sits in phase Failed beside it,
// refused by the kubelet before any container ran. The refused member's
// pod-level record names no container, and the rebuilt gang's next death
// is read against the record, so the exit the repair answers is what it
// keeps.
func TestRestart_RecordsTheRunnersExitOverAMemberTheKubeletRefused(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	readySince := metav1.NewTime(time.Now().Add(-40 * time.Second).Truncate(time.Second))
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceReady, PodCount: 2,
		RunningRevision: fixtureRevisionName(), ReadySince: &readySince,
	})
	died := readySince.Add(10 * time.Second)
	leader := gangPod(isvc, 0, "leader", 0, 2, true, true)
	leader.UID = "leader-pod-uid"
	leader.Status.Phase = corev1.PodRunning
	leader.Status.ContainerStatuses = []corev1.ContainerStatus{
		runnerStatus(constants.MainContainerName, died.Add(2*time.Second), &corev1.ContainerStateTerminated{
			Reason: "Error", ExitCode: 1, FinishedAt: metav1.NewTime(died),
		}),
	}
	worker := rejectedPod(gangPod(isvc, 0, "worker", 0, 2, false, false))
	worker.UID = "worker-pod-uid"
	c := newFakeClient(t, isvc, ir, leader, worker)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	fires, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{leader, worker})
	if !fires || !strings.Contains(reason, "restarted after Ready") {
		t.Fatalf("trigger = (%t, %q), want the runner's restart after Ready", fires, reason)
	}
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c, APIReader: c}, input, plan, plan.Instances[0], nil, reason); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceRestarting || s.LastFailure == nil {
		t.Fatalf("row = %+v, want Restarting with the exit recorded", s)
	}
	if s.LastFailure.PodName != leader.Name || s.LastFailure.ContainerName != constants.MainContainerName || s.LastFailure.ExitCode == nil || *s.LastFailure.ExitCode != 1 {
		t.Errorf("LastFailure = %+v, want the leader's exit %s/%s (exit 1), not the refused worker's pod-level record", s.LastFailure, leader.Name, constants.MainContainerName)
	}
}

// A repair opened for a runner restart the kubelet shows no termination
// for yet (the new run's start is on the container status, the run that
// ended is not) records the runner with its exit unobserved, ahead of a
// member of the gang that sits in phase Failed beside it, refused before
// any container ran.
func TestRestart_RecordsTheRunnersUnobservedExitOverAMemberTheKubeletRefused(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	readySince := metav1.NewTime(time.Now().Add(-40 * time.Second).Truncate(time.Second))
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceReady, PodCount: 2,
		RunningRevision: fixtureRevisionName(), ReadySince: &readySince,
	})
	leader := gangPod(isvc, 0, "leader", 0, 2, true, true)
	leader.UID = "leader-pod-uid"
	leader.Status.Phase = corev1.PodRunning
	leader.Status.ContainerStatuses = []corev1.ContainerStatus{runnerStatus(constants.MainContainerName, readySince.Add(12*time.Second), nil)}
	worker := rejectedPod(gangPod(isvc, 0, "worker", 0, 2, false, false))
	worker.UID = "worker-pod-uid"
	c := newFakeClient(t, isvc, ir, leader, worker)
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	fires, reason := ops.DetectRestartTriggerWithPods(input, plan, plan.Instances[0], []*corev1.Pod{leader, worker})
	if !fires || !strings.Contains(reason, "restarted after Ready") {
		t.Fatalf("trigger = (%t, %q), want the runner's restart after Ready", fires, reason)
	}
	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c, APIReader: c}, input, plan, plan.Instances[0], nil, reason); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceRestarting || s.LastFailure == nil {
		t.Fatalf("row = %+v, want Restarting with the exit recorded", s)
	}
	if s.LastFailure.PodName != leader.Name || s.LastFailure.ContainerName != constants.MainContainerName || s.LastFailure.Reason != workload.RunnerExitUnobservedReason {
		t.Errorf("LastFailure = %+v, want the leader named with its exit unobserved, not the refused worker's pod-level record", s.LastFailure)
	}
}

// A repair rebuilding a gang for its runner's exit keeps that exit on the
// row when the kubelet refuses one of the rebuilt members: the refused
// member is deleted for its replacement with the attempt's retry count
// raised, and the record the rebuilt gang's next death is read against
// still names the runner's container.
func TestRestart_ReplacingARefusedRebuiltMemberKeepsTheRunnersExit(t *testing.T) {
	resetExpectations(t)
	isvc := minimalISVC("llama-70b", "prod", 1)
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	exitCode := int32(1)
	leader := gangPod(isvc, 0, "leader", 0, 2, false, false)
	leader.UID = "leader-pod-uid"
	leader.Status.Phase = corev1.PodPending
	exit := v1beta1.InstanceTermination{
		PodName: leader.Name, ContainerName: constants.MainContainerName, Reason: "Error", ExitCode: &exitCode,
		Time: metav1.NewTime(now.Add(-30 * time.Second)),
	}
	ir := instanceIR(isvc, workload.ComponentEngine, v1beta1.OMENativeInstanceStatus{
		Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceRestarting, PodCount: 2,
		RunningRevision: fixtureRevisionName(), LastFailure: &exit,
		Operation: &v1beta1.InstanceOperation{
			ID: "restart-0-1", Type: v1beta1.InstanceOperationRestart, Step: "Drain",
			Reason:    "pod " + leader.Name + " container " + constants.MainContainerName + " restarted after Ready: Error (exit 1)",
			StartedAt: exit.Time, LastProgressAt: exit.Time, Deadline: metav1.NewTime(now.Add(time.Hour)),
		},
	})
	worker := rejectedPod(gangPod(isvc, 0, "worker", 0, 2, false, false))
	worker.UID = "worker-pod-uid"
	worker.CreationTimestamp = metav1.NewTime(now.Add(-20 * time.Second))
	c := newFakeClient(t, isvc, ir, leader, worker, fixtureRevision(t, isvc))
	input := buildTestInput(isvc, c, workload.ComponentEngine)
	plan := buildPlanGangEngine(workload.RestartPolicyRecreateInstance)

	if _, err := ops.Restart(context.Background(), workload.Deps{Client: c, APIReader: c}, input, plan, plan.Instances[0], nil, "trigger"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if podExists(c, worker) {
		t.Fatalf("the refused worker %s must be deleted for its replacement", worker.Name)
	}
	s := findInstanceStatusOnIR(c, isvc, workload.ComponentEngine, 0)
	if s == nil || s.Phase != v1beta1.OMENativeInstanceRestarting || s.Operation == nil || s.Operation.RetryCount != 1 {
		t.Fatalf("row = %+v, want the repair still in flight with its retry count raised once", s)
	}
	if s.LastFailure == nil || s.LastFailure.PodName != leader.Name || s.LastFailure.ContainerName != constants.MainContainerName {
		t.Errorf("LastFailure = %+v, want the leader's exit kept over the refused worker's pod-level record", s.LastFailure)
	}
}
