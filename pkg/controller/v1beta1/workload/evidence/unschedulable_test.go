package evidence

// Which refused pods carry no start of their own: a pod a queue gated from its
// creation keeps its creation as its PodScheduled time. The hold pass and the
// escalation that act on the answer are tested with them.

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

// TestUnschedulableSinceTheGate: a labeled pod whose PodScheduled time is no
// later than its creation may still carry the time it was created behind its
// queue's gate, so the answer is yes. Every other case answers no.
func TestUnschedulableSinceTheGate(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	queued := map[string]string{constants.KueueQueueLabelKey: "queue-a"}
	pod := func(labels map[string]string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: labels, CreationTimestamp: metav1.NewTime(created)}}
	}
	for _, tc := range []struct {
		name  string
		pod   *corev1.Pod
		since metav1.Time
		want  bool
	}{
		{name: "a labeled pod whose time is its creation has no start of its own", pod: pod(queued), since: metav1.NewTime(created), want: true},
		{name: "a labeled pod whose time is after its creation was refused then", pod: pod(queued), since: metav1.NewTime(created.Add(time.Second))},
		{name: "a pod without the label was refused at its time", pod: pod(nil), since: metav1.NewTime(created)},
		{name: "no time gives no answer", pod: pod(queued)},
		{name: "no pod gives no answer", since: metav1.NewTime(created)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := UnschedulableSinceTheGate(tc.pod, tc.since); got != tc.want {
				t.Errorf("UnschedulableSinceTheGate: got %v want %v", got, tc.want)
			}
		})
	}
}
