package ops

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestHeldInstanceKeepsAFailedAttempt_ReadsTheAttemptsOwnPodWhenTheSourceIsGone
// holds the rule the partition's hold stands on. A held Instance whose
// attempt toward the target failed is named while a pod of that attempt
// stands, whether its old pod still serves beside the failed replacement
// (SurgeThenDrain) or is gone and only the recreated pod that never served
// stands (RecreatePod). An Instance with no pod left is the create pass's
// to rebuild and is not named; a row whose attempt still claims the
// update, or whose target is another revision, is not either.
func TestHeldInstanceKeepsAFailedAttempt_ReadsTheAttemptsOwnPodWhenTheSourceIsGone(t *testing.T) {
	const oldHash, targetHash, otherHash = "aaaa1111", "bbbb2222", "cccc3333"
	oldRevision, targetRevision := "owner-engine-"+oldHash, "owner-engine-"+targetHash
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: targetRevision}}
	failed := func(targetOfAttempt string, op *workload.InstanceOperation) *workload.InstanceStatus {
		return &workload.InstanceStatus{Index: 0, Phase: workload.InstancePhaseFailed, RunningRevision: oldRevision, TargetRevision: targetOfAttempt, Operation: op}
	}
	stopped := &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepParked, TargetRevision: targetRevision}
	claimed := &workload.InstanceOperation{Type: workload.InstanceOperationUpdate, Step: workload.UpdateStepDrain, TargetRevision: targetRevision}
	leaving := func(pod *corev1.Pod) { pod.DeletionTimestamp = &metav1.Time{Time: time.Now()} }
	cases := []struct {
		name string
		row  *workload.InstanceStatus
		pods []*corev1.Pod
		want bool
	}{
		{"the source serves beside the failed replacement", failed(targetRevision, nil), []*corev1.Pod{podOf(0, oldHash), podOf(0, targetHash)}, true},
		{"the source stands and the failed replacement is on its way out", failed(targetRevision, nil), []*corev1.Pod{podOf(0, oldHash), podOf(0, targetHash, leaving)}, true},
		{"the source is gone and the recreated pod stands", failed(targetRevision, stopped), []*corev1.Pod{podOf(0, targetHash)}, true},
		{"the source is gone and the recreated pod stands with no operation", failed(targetRevision, nil), []*corev1.Pod{podOf(0, targetHash)}, true},
		{"no pod is left", failed(targetRevision, stopped), nil, false},
		{"the only pod left is on its way out", failed(targetRevision, stopped), []*corev1.Pod{podOf(0, targetHash, leaving)}, false},
		{"the attempt still claims the update", failed(targetRevision, claimed), []*corev1.Pod{podOf(0, targetHash)}, false},
		{"the attempt was toward a revision the roll has left", failed("owner-engine-"+otherHash, nil), []*corev1.Pod{podOf(0, oldHash), podOf(0, otherHash)}, false},
		{"the row is Ready", &workload.InstanceStatus{Index: 0, Phase: workload.InstancePhaseReady, RunningRevision: oldRevision, TargetRevision: targetRevision}, []*corev1.Pod{podOf(0, oldHash)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HeldInstanceKeepsAFailedAttempt(tc.row, target, tc.pods); got != tc.want {
				t.Errorf("HeldInstanceKeepsAFailedAttempt = %t, want %t", got, tc.want)
			}
		})
	}
}
