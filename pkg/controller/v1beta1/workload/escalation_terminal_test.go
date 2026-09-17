package workload

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// servingPodInPhase builds a pod that reads healthy — ContainersReady and
// in the serving rotation — in the given phase. On a terminal phase that
// models conditions the kubelet never cleared after the pod died.
func servingPodInPhase(name string, phase corev1.PodPhase) *corev1.Pod {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod"}}
	pod.Status.Phase = phase
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: query.ServingConditionType, Status: corev1.ConditionTrue},
	}
	return pod
}

// The failed-while-serving guard must never treat a terminal pod as
// health: it is failure evidence that has to reach escalation, whatever
// conditions it still carries and whether or not a serving sibling would
// cover the desired count.
func TestPodSetFullyServing_TerminalPodDisqualifies(t *testing.T) {
	serving := servingPodInPhase("serving", corev1.PodRunning)
	cases := []struct {
		name    string
		pods    []*corev1.Pod
		desired int32
		want    bool
	}{
		{"healthy set", []*corev1.Pod{serving}, 1, true},
		{"failed pod with stale healthy conditions", []*corev1.Pod{servingPodInPhase("dead", corev1.PodFailed)}, 1, false},
		{"succeeded pod with stale healthy conditions", []*corev1.Pod{servingPodInPhase("done", corev1.PodSucceeded)}, 1, false},
		{"serving sibling does not cover a failed member", []*corev1.Pod{serving, servingPodInPhase("dead", corev1.PodFailed)}, 1, false},
		{"deleting pod is still excluded, not disqualifying", []*corev1.Pod{serving, func() *corev1.Pod {
			p := servingPodInPhase("draining", corev1.PodRunning)
			now := metav1.Now()
			p.DeletionTimestamp = &now
			p.Status.Conditions = nil
			return p
		}()}, 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := podSetFullyServing(tc.pods, tc.desired); got != tc.want {
				t.Fatalf("podSetFullyServing = %v, want %v", got, tc.want)
			}
		})
	}
}
