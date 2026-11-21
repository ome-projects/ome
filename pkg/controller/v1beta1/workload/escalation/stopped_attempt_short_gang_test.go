package escalation_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestFollowStoppedAttempts_AGangShortOfAMemberIsNotReadAsWhole: the plan
// asks eight pods of a gang whose stopped attempt reads Failed. With seven
// of them serving the row stays Failed and nothing is written: the set is
// read against the eight pods the plan asks for, not against the seven it
// has. With all eight serving the row reads Updating, in one write.
func TestFollowStoppedAttempts_AGangShortOfAMemberIsNotReadAsWhole(t *testing.T) {
	const gangSize = 8
	now := time.Now()
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: parkedTarget}}
	for _, tc := range []struct {
		name   string
		pods   int
		want   workloadtypes.InstancePhase
		writes int
	}{
		{name: "seven of the eight pods serve and the row stays Failed", pods: gangSize - 1, want: workloadtypes.InstancePhaseFailed},
		{name: "all eight pods serve and the row reads Updating", pods: gangSize, want: workloadtypes.InstancePhaseUpdating, writes: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := parkedRow(workloadtypes.InstancePhaseFailed, "RetryBlock")
			row.PodCount = int32(tc.pods)
			input, rec := escalationFixture([]workloadtypes.InstanceStatus{row})
			input.Clock = clocktesting.NewFakeClock(now)
			input.StuckPodGrace = parkedGrace
			writes := 0
			seam := input.MutateInstance
			input.MutateInstance = func(ctx context.Context, idx int32, mutate func(*workloadtypes.InstanceStatus) bool) error {
				writes++
				return seam(ctx, idx, mutate)
			}
			gang := make([]*corev1.Pod, 0, tc.pods)
			for ordinal := 0; ordinal < tc.pods; ordinal++ {
				pod := crashedPod(true)
				pod.Name = fmt.Sprintf("engine-0-default-%d", ordinal)
				gang = append(gang, pod)
			}
			err := escalation.FollowStoppedAttempts(context.Background(), escalation.PassInput{
				Input:  input,
				Plan:   singleInstancePlan(0, gangSize),
				Target: target,
				Pods:   func(context.Context) (map[int32][]*corev1.Pod, error) { return map[int32][]*corev1.Pod{0: gang}, nil },
			})
			if err != nil {
				t.Fatalf("FollowStoppedAttempts: %v", err)
			}
			if got := rec.store[0].Phase; got != tc.want || writes != tc.writes {
				t.Fatalf("Phase = %s with %d write(s), want %s with %d", got, writes, tc.want, tc.writes)
			}
		})
	}
}
