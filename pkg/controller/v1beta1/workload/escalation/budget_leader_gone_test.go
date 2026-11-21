package escalation_test

import (
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestExtraPodSurgeInFlight_AShortenedWorkerCountsOnItsKubeletsCount: a gang
// of one leader and one worker lists a third pod, an old worker that shows
// 30 s of its 480 s and stands 5 s past the deletion time the API server
// shows. With lifecycle.leaderGoneGracePeriod unset the old worker no
// longer holds a surge slot. With the field at 30 s its kubelet may still be
// counting, so it holds the slot until 30 s after that deletion time, and
// that instant is the next release.
func TestExtraPodSurgeInFlight_AShortenedWorkerCountsOnItsKubeletsCount(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	deletionTime := now.Add(-5 * time.Second)
	gang := []types.RunnerPlan{{Name: types.RunnerLeader, Size: 1}, {Name: types.RunnerWorker, Size: 1}}
	plan := types.ComponentPlan{
		Component: types.ComponentEngine,
		Replicas:  1,
		Instances: []types.InstancePlan{{Index: 0, Incarnation: 1, Runners: gang}},
	}
	statuses := []types.InstanceStatus{{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "prior-rev"}}
	old := terminatingSurgePod(0, 2, deletionTime)
	old.Labels[query.LabelRunner] = types.RunnerWorker
	old.Spec.TerminationGracePeriodSeconds = ptr.To(int64(480))
	old.DeletionGracePeriodSeconds = ptr.To(int64(30))
	pods := map[int32][]*corev1.Pod{0: {terminatingSurgePod(0, 0, time.Time{}), terminatingSurgePod(0, 1, time.Time{}), old}}

	unset := escalation.ExtraPodSurgeInFlight(plan, statuses, pods, nil, types.Lifecycle{}, now)
	if unset.Slots != 0 || len(unset.Terminating) != 0 {
		t.Fatalf("with the field unset: slots = %d, terminating pods = %v, want none", unset.Slots, unset.Terminating)
	}

	lifecycle := types.Lifecycle{LeaderGoneGracePeriod: &metav1.Duration{Duration: 30 * time.Second}}
	set := escalation.ExtraPodSurgeInFlight(plan, statuses, pods, nil, lifecycle, now)
	if set.Slots != 1 || !slices.Equal(set.Terminating, []string{old.Name}) {
		t.Fatalf("with the field at 30 s: slots = %d, terminating pods = %v, want one slot held by %s", set.Slots, set.Terminating, old.Name)
	}
	if want := deletionTime.Add(30 * time.Second); !set.NextRelease.Equal(want) {
		t.Fatalf("the next release is %s after the deletion time, want 30s", set.NextRelease.Sub(deletionTime))
	}
}
