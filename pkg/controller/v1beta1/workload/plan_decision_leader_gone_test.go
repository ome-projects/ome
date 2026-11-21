package workload

import (
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestCountExtraPodSurge_AShortenedWorkerHoldsItsSlotOnItsKubeletsCount: a
// gang of one leader and one worker rolls under a surge budget of one. Its
// Instance lists a third pod, an old worker that shows 30 s of its 480 s and
// stands 5 s past the deletion time the API server shows. With
// lifecycle.leaderGoneGracePeriod unset the old worker holds no surge slot.
// With the field at 30 s its kubelet may still be counting, so the pass
// counts it against the budget until 30 s after that deletion time, and that
// instant is the next release.
func TestCountExtraPodSurge_AShortenedWorkerHoldsItsSlotOnItsKubeletsCount(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	deletionTime := metav1.NewTime(now.Add(-5 * time.Second))
	old := leaderGoneSidePod(types.ComponentEngine, types.RunnerWorker, 1, true, now)
	old.Spec.TerminationGracePeriodSeconds = ptr.To(int64(480))
	old.DeletionTimestamp = &deletionTime
	old.DeletionGracePeriodSeconds = ptr.To(int64(30))
	pods := map[int32][]*corev1.Pod{0: {
		leaderGoneSidePod(types.ComponentEngine, types.RunnerLeader, 0, false, now),
		leaderGoneSidePod(types.ComponentEngine, types.RunnerWorker, 0, false, now),
		old,
	}}
	// count runs the pass's count of the surge slots for a replica whose
	// lifecycle sets the field to grace, and leaves it unset for zero.
	count := func(grace time.Duration) UpdateSelection {
		p := &updateSelectionPass{
			input: types.ReconcileInput{
				Clock: clocktesting.NewFakeClock(now),
				ObservedState: types.WorkloadObservedState{InstanceStatuses: []types.InstanceStatus{
					{Index: 0, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"},
				}},
			},
			plan: types.ComponentPlan{
				Component: types.ComponentEngine,
				Replicas:  1,
				Instances: []types.InstancePlan{{Index: 0, Runners: []types.RunnerPlan{
					{Name: types.RunnerLeader, Size: 1},
					{Name: types.RunnerWorker, Size: 1},
				}}},
			},
			pods:      pods,
			selection: UpdateSelection{Strategy: types.UpdateStrategySurgeThenDrain, SurgeBudget: 1},
		}
		if grace > 0 {
			p.input.DesiredSpec.Lifecycle.LeaderGoneGracePeriod = &metav1.Duration{Duration: grace}
		}
		p.countExtraPodSurge()
		return p.selection
	}
	for _, tc := range []struct {
		name        string
		field       time.Duration
		slots       int32
		terminating []string
		release     time.Time
	}{
		{name: "the field unset"},
		{name: "the field at 30 s", field: 30 * time.Second, slots: 1, terminating: []string{old.Name}, release: deletionTime.Add(30 * time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := count(tc.field).ExtraPodSurge
			if got.Slots != tc.slots || !slices.Equal(got.Terminating, tc.terminating) || !got.NextRelease.Equal(tc.release) {
				t.Fatalf("slots = %d, terminating pods = %v, next release %s; want %d, %v, %s",
					got.Slots, got.Terminating, got.NextRelease, tc.slots, tc.terminating, tc.release)
			}
		})
	}
}
