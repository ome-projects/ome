package escalation_test

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestPruneOutlivedRetryBlocks_BesideAPodOnAStoppedNode stages the cleanup
// of a revision's retry record beside a pod whose node stopped reporting.
// Two Instances run the revision and have been Ready on it for an hour. A
// crash of the first, half an hour ago, left the record: one attempt spent,
// in backoff. Its rebuilt pod has served ever since, so the rows have
// outlived the failure and the record goes. Then the node under the second
// Instance's pod stops: Ready is False on the pod while its containers and
// its serving gate read true. That pod takes no traffic. The cleanup
// reads its Instance as serving and removes the record all the same.
func TestPruneOutlivedRetryBlocks_BesideAPodOnAStoppedNode(t *testing.T) {
	const rev = "own-engine-current1"
	now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	readySince := metav1.NewTime(now.Add(-time.Hour))
	crashed := metav1.NewTime(now.Add(-30 * time.Minute))
	due := metav1.NewTime(now.Add(-29 * time.Minute))
	// podOf is the pod of one Instance on the revision: its containers
	// ready and its serving gate on since the Instance entered Ready, and
	// its Ready condition as the case names it.
	podOf := func(idx int32, ready corev1.ConditionStatus, readyMoved time.Time) *corev1.Pod {
		pod := enginePod("llama-70b", "prod", idx)
		pod.CreationTimestamp = metav1.NewTime(readySince.Add(-time.Minute))
		pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(rev)
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: readySince},
			{Type: corev1.PodReady, Status: ready, LastTransitionTime: metav1.NewTime(readyMoved)},
			{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: readySince},
		}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: constants.MainContainerName, Image: "test:v1", Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: readySince}},
		}}
		return pod
	}
	plan := minimalPlan()
	plan.Replicas = 2
	plan.Instances = append(plan.Instances, types.InstancePlan{Index: 1, Incarnation: 1, Runners: []types.RunnerPlan{{Name: "default", Size: 1}}})
	for _, tc := range []struct {
		name   string
		second *corev1.Pod
		pruned bool
	}{
		{name: "every pod Ready: the record goes", second: podOf(1, corev1.ConditionTrue, readySince.Time), pruned: true},
		{name: "a pod on a node that stopped five minutes ago: the record goes", second: podOf(1, corev1.ConditionFalse, now.Add(-5*time.Minute)), pruned: true},
		{name: "the node is back and the pod Ready for ten seconds: the record goes", second: podOf(1, corev1.ConditionTrue, now.Add(-10*time.Second)), pruned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := minimalInput(t)
			input.Clock = clocktesting.NewFakeClock(now)
			input.StuckPodGrace = time.Minute
			removed := recordRetryBlockRemovals(&input)
			input.ObservedState.CurrentRevision = rev
			input.ObservedState.UpdateRevision = rev
			input.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: rev, PodCount: 1, ServingPodCount: 1, ReadySince: &readySince,
					LastFailure: &types.InstanceTermination{PodName: "llama-70b-engine-0-default-0", ContainerName: constants.MainContainerName, Reason: "Error", Time: crashed}},
				{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: rev, PodCount: 1, ServingPodCount: 1, ReadySince: &readySince},
			}
			input.ObservedState.RetryBlocks = []types.RetryBlock{{TargetRevision: rev, State: types.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &due, Reason: "Error"}}
			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: rev, CreationTimestamp: metav1.NewTime(readySince.Add(-time.Hour))}}
			pods := func(context.Context) (map[int32][]*corev1.Pod, error) {
				return map[int32][]*corev1.Pod{0: {podOf(0, corev1.ConditionTrue, readySince.Time)}, 1: {tc.second}}, nil
			}

			if err := escalation.PruneOutlivedRetryBlocks(context.Background(), input, plan, target, pods); err != nil {
				t.Fatalf("outlived prune: %v", err)
			}
			if got := len(*removed) == 1 && (*removed)[0] == rev; got != tc.pruned {
				t.Fatalf("pruned=%v want %v (removed %v)", got, tc.pruned, *removed)
			}
		})
	}
}
