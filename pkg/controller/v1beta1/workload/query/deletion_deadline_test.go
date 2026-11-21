package query

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// deadlinePod is a Terminating pod of runner whose deletion time is
// deletionTime. own is the grace its spec carries and shown the grace it
// shows, each in seconds and left out when zero.
func deadlinePod(runner string, own, shown int64, deletionTime time.Time) *corev1.Pod {
	at := metav1.NewTime(deletionTime)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:         "prod",
		Name:              "svc-engine-0-" + runner + "-0",
		Labels:            map[string]string{LabelRunner: runner},
		DeletionTimestamp: &at,
	}}
	if own > 0 {
		pod.Spec.TerminationGracePeriodSeconds = &own
	}
	if shown > 0 {
		pod.DeletionGracePeriodSeconds = &shown
	}
	return pod
}

// TestDeletionDeadline: a pod's deadline is its deletion time. A worker of
// a replica that sets lifecycle.leaderGoneGracePeriod, and that shows a
// grace under its own or at or under the field's value, may have been given
// that grace by a delete after its first one. Its kubelet counts the grace
// from that later delete, so its deadline is the deletion time plus the
// larger of the grace it shows and the field's value.
func TestDeletionDeadline(t *testing.T) {
	deletionTime := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	lifecycleWith := func(grace time.Duration) workload.Lifecycle {
		if grace == 0 {
			return workload.Lifecycle{}
		}
		return workload.Lifecycle{LeaderGoneGracePeriod: &metav1.Duration{Duration: grace}}
	}
	cases := []struct {
		name   string
		field  time.Duration
		runner string
		own    int64
		shown  int64
		// want is how long after the deletion time the deadline is.
		want time.Duration
	}{
		{name: "the field unset, a worker that shows 30 s of its 480 s", runner: workload.RunnerWorker, own: 480, shown: 30},
		{name: "a worker nobody shortened, with its own 480 s", field: 30 * time.Second, runner: workload.RunnerWorker, own: 480, shown: 480},
		{name: "a worker the step shortened to the field's 30 s", field: 30 * time.Second, runner: workload.RunnerWorker, own: 480, shown: 30, want: 30 * time.Second},
		{name: "a worker the API server left 1 s, its leader gone later than the 30 s", field: 30 * time.Second, runner: workload.RunnerWorker, own: 480, shown: 1, want: 30 * time.Second},
		{name: "a worker somebody shortened to 60 s, above the field", field: 30 * time.Second, runner: workload.RunnerWorker, own: 480, shown: 60, want: 60 * time.Second},
		{name: "a worker the step shortened to 20 s, above its own 10 s", field: 20 * time.Second, runner: workload.RunnerWorker, own: 10, shown: 20, want: 20 * time.Second},
		{name: "a worker deleted with more than its own grace, above the field", field: 30 * time.Second, runner: workload.RunnerWorker, own: 480, shown: 600},
		{name: "a worker that shows 30 s and no grace of its own", field: 30 * time.Second, runner: workload.RunnerWorker, shown: 30, want: 30 * time.Second},
		{name: "a worker that shows 60 s and no grace of its own", field: 30 * time.Second, runner: workload.RunnerWorker, shown: 60},
		{name: "a worker that shows no grace", field: 30 * time.Second, runner: workload.RunnerWorker, own: 480},
		{name: "a field of 400 ms counts as 1 s", field: 400 * time.Millisecond, runner: workload.RunnerWorker, own: 480, shown: 1, want: time.Second},
		{name: "a leader that shows 30 s of its 480 s", field: 30 * time.Second, runner: workload.RunnerLeader, own: 480, shown: 30},
		{name: "the pod of a single-pod Instance that shows 30 s of its 480 s", field: 30 * time.Second, runner: workload.RunnerDefault, own: 480, shown: 30},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := deadlinePod(tc.runner, tc.own, tc.shown, deletionTime)
			if got := DeletionDeadline(pod, lifecycleWith(tc.field)); !got.Equal(deletionTime.Add(tc.want)) {
				t.Fatalf("the deadline is %s after the deletion time, want %s", got.Sub(deletionTime), tc.want)
			}
		})
	}
	running := deadlinePod(workload.RunnerWorker, 480, 0, deletionTime)
	running.DeletionTimestamp = nil
	for name, pod := range map[string]*corev1.Pod{"a pod nobody deleted": running, "no pod": nil} {
		if got := DeletionDeadline(pod, lifecycleWith(30*time.Second)); !got.IsZero() {
			t.Errorf("%s: the deadline is %s, want the zero time", name, got)
		}
	}
}
