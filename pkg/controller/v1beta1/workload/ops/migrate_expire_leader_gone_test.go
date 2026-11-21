package ops

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestSourcePodPastSlack_AShortenedWorkerIsReadOnItsKubeletsCount: a source
// worker shows 30 s of its 480 s, the grace a delete after its first one
// named, and stands 20 s past the deletion time the API server shows. The
// policy's slack is 10 s. With lifecycle.leaderGoneGracePeriod unset the
// worker is past the slack. With the field at 30 s its kubelet may still
// be counting, so it is not past the slack until 40 s after that deletion
// time.
func TestSourcePodPastSlack_AShortenedWorkerIsReadOnItsKubeletsCount(t *testing.T) {
	policy := &workload.ForceDeletePolicy{OverdueSlack: 10 * time.Second, NodeUnreachableThreshold: time.Minute}
	worker := func(past time.Duration) *corev1.Pod {
		return endingIn(memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1), 30, -past)
	}
	cases := []struct {
		name   string
		policy *workload.ForceDeletePolicy
		field  time.Duration
		pod    *corev1.Pod
		want   bool
	}{
		{name: "no policy", field: 30 * time.Second, pod: worker(10 * time.Minute)},
		{name: "the field unset, 20 s past the deletion time", policy: policy, pod: worker(20 * time.Second), want: true},
		{name: "the field at 30 s, 20 s past the deletion time", policy: policy, field: 30 * time.Second, pod: worker(20 * time.Second)},
		{name: "the field at 30 s, 40 s past the deletion time", policy: policy, field: 30 * time.Second, pod: worker(40 * time.Second)},
		{name: "the field at 30 s, 41 s past the deletion time", policy: policy, field: 30 * time.Second, pod: worker(41 * time.Second), want: true},
		{name: "a pod nobody deleted", policy: policy, pod: memberPod("svc-engine-0-worker-0", workload.RunnerWorker, 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := fdInput(fdISVC("svc"), tc.policy)
			if tc.field > 0 {
				input.DesiredSpec.Lifecycle.LeaderGoneGracePeriod = &metav1.Duration{Duration: tc.field}
			}
			if got := sourcePodPastSlack(input, tc.pod); got != tc.want {
				t.Fatalf("sourcePodPastSlack = %v, want %v", got, tc.want)
			}
		})
	}
}
