package escalation_test

import (
	"context"
	"fmt"
	"slices"
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

// TestPruneOutlivedRetryBlocks_AGangShortOfAMemberKeepsItsBlock: the plan
// asks eight pods of a gang that has been Ready on the revision for an
// hour. With one member gone and seven pods serving, the retry record of
// the revision stays: the gang is read against the eight pods the plan
// asks for, not against the seven it has. With all eight serving the gang
// has outlived the failure and the record goes.
func TestPruneOutlivedRetryBlocks_AGangShortOfAMemberKeepsItsBlock(t *testing.T) {
	const (
		rev      = "own-engine-current1"
		gangSize = 8
	)
	now := time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC)
	readySince := metav1.NewTime(now.Add(-time.Hour))
	due := metav1.NewTime(now.Add(2 * time.Minute))
	member := func(ordinal int) *corev1.Pod {
		pod := enginePod("llama-70b", "prod", 0)
		pod.Name = fmt.Sprintf("%s-%d", pod.Name, ordinal)
		pod.Labels[query.LabelPodOrdinal] = fmt.Sprintf("%d", ordinal)
		pod.Labels[query.LabelRevisionHash] = query.RevisionHashFromControllerRevisionName(rev)
		pod.CreationTimestamp = metav1.NewTime(readySince.Add(-time.Minute))
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: readySince},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: readySince},
			{Type: query.ServingConditionType, Status: corev1.ConditionTrue, LastTransitionTime: readySince},
		}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: constants.MainContainerName, Image: "test:v1", Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: readySince}},
		}}
		return pod
	}
	for _, tc := range []struct {
		name    string
		pods    int
		removed []string
	}{
		{name: "seven of the eight pods serve and the record stays", pods: gangSize - 1},
		{name: "all eight pods serve and the record goes", pods: gangSize, removed: []string{rev}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := minimalInput(t)
			input.Clock = clocktesting.NewFakeClock(now)
			input.StuckPodGrace = time.Minute
			removed := recordRetryBlockRemovals(&input)
			input.ObservedState.CurrentRevision = rev
			input.ObservedState.UpdateRevision = rev
			input.ObservedState.InstanceStatuses = []types.InstanceStatus{
				{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: rev, PodCount: int32(tc.pods), ServingPodCount: int32(tc.pods), ReadySince: &readySince},
			}
			input.ObservedState.RetryBlocks = []types.RetryBlock{{TargetRevision: rev, State: types.RetryBlockBackoff, AttemptsStarted: 1, NextRetryAt: &due, Reason: "CrashLoopBackOff"}}
			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: rev, CreationTimestamp: metav1.NewTime(readySince.Add(-time.Hour))}}
			gang := make([]*corev1.Pod, 0, tc.pods)
			for ordinal := 0; ordinal < tc.pods; ordinal++ {
				gang = append(gang, member(ordinal))
			}
			pods := func(context.Context) (map[int32][]*corev1.Pod, error) {
				return map[int32][]*corev1.Pod{0: gang}, nil
			}

			if err := escalation.PruneOutlivedRetryBlocks(context.Background(), input, singleInstancePlan(0, gangSize), target, pods); err != nil {
				t.Fatalf("outlived prune: %v", err)
			}
			if !slices.Equal(*removed, tc.removed) {
				t.Fatalf("retry records removed: %v, want %v", *removed, tc.removed)
			}
		})
	}
}
