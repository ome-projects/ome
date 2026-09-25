package evidence_test

// The crash-loop wedge on a row no operation owns. The shape is narrow on
// purpose: a Ready row with no operation, a pod on the Component's
// current revision, wedged past the configured grace, and a pod set that
// is not fully serving.

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const clRevision = "engine-abc123"

// crashLoopPod is a pod of the current revision parked in
// CrashLoopBackOff since age ago.
func crashLoopPod(name string, age time.Duration, hash string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(tNow.Add(-age)),
			Labels:            map[string]string{query.LabelRevisionHash: hash},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			}},
		},
	}
}

func crashLoopInput(grace time.Duration) types.ReconcileInput {
	return types.ReconcileInput{
		Clock:         clocktesting.NewFakeClock(tNow),
		StuckPodGrace: grace,
		ObservedState: types.WorkloadObservedState{CurrentRevision: clRevision},
	}
}

func TestCrashLoopWedge_NamesAStuckPodOnTheCurrentRevision(t *testing.T) {
	hash := query.RevisionFromName(clRevision).Hash()
	ready := &types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	wedged := crashLoopPod("engine-0-default-0", 10*time.Minute, hash)

	for _, tc := range []struct {
		name  string
		input types.ReconcileInput
		row   *types.InstanceStatus
		pods  []*corev1.Pod
		want  bool
	}{
		{
			name:  "Ready row, wedged pod on the current revision",
			input: crashLoopInput(time.Minute),
			row:   ready,
			pods:  []*corev1.Pod{wedged},
			want:  true,
		},
		{
			name:  "unset grace disables the shape",
			input: crashLoopInput(0),
			row:   ready,
			pods:  []*corev1.Pod{wedged},
		},
		{
			name:  "still inside the grace",
			input: crashLoopInput(time.Hour),
			row:   ready,
			pods:  []*corev1.Pod{wedged},
		},
		{
			name:  "an operation in flight owns the row",
			input: crashLoopInput(time.Minute),
			row: &types.InstanceStatus{
				Index: 0, Phase: types.InstancePhaseReady,
				Operation: &types.InstanceOperation{Type: types.InstanceOperationUpdate},
			},
			pods: []*corev1.Pod{wedged},
		},
		{
			name:  "a row that is not Ready is some other pass's",
			input: crashLoopInput(time.Minute),
			row:   &types.InstanceStatus{Index: 0, Phase: types.InstancePhasePending},
			pods:  []*corev1.Pod{wedged},
		},
		{
			name:  "an off-revision wedge is a leftover the escalation owns",
			input: crashLoopInput(time.Minute),
			row:   ready,
			pods:  []*corev1.Pod{crashLoopPod("engine-0-default-0", 10*time.Minute, "deadbeef")},
		},
		{
			name:  "a deleting pod is on its way out",
			input: crashLoopInput(time.Minute),
			row:   ready,
			pods: func() []*corev1.Pod {
				p := crashLoopPod("engine-0-default-0", 10*time.Minute, hash)
				ts := metav1.NewTime(tNow)
				p.DeletionTimestamp = &ts
				return []*corev1.Pod{p}
			}(),
		},
		{
			name:  "no row at all",
			input: crashLoopInput(time.Minute),
			row:   nil,
			pods:  []*corev1.Pod{wedged},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, got := evidence.CrashLoopWedge(tc.input, tc.row, 1, tc.pods)
			if got != tc.want {
				t.Fatalf("wedged: got %v (%q) want %v", got, reason, tc.want)
			}
			if got && reason == "" {
				t.Error("a wedge must name itself for the repair's reason")
			}
		})
	}
}

// TestCrashLoopWedge_FullyServingSetIsNeverAWedge: the Instance is
// answering traffic, so a container waiting reason is not grounds to
// recycle it.
func TestCrashLoopWedge_FullyServingSetIsNeverAWedge(t *testing.T) {
	serving := servingPod("engine-0-default-0")
	serving.Labels = map[string]string{query.LabelRevisionHash: query.RevisionFromName(clRevision).Hash()}
	serving.Status.ContainerStatuses = append(serving.Status.ContainerStatuses, corev1.ContainerStatus{
		Name:  "sidecar",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
	})
	row := &types.InstanceStatus{Index: 0, Phase: types.InstancePhaseReady}
	if _, got := evidence.CrashLoopWedge(crashLoopInput(time.Minute), row, 1, []*corev1.Pod{serving}); got {
		t.Error("fully serving pod set: got a wedge, want none")
	}
}
