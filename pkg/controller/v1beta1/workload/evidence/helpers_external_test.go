package evidence_test

// Pod fixtures shared by the classifier tests in this package. Each one
// builds a shape a classifier must recognize (or must refuse to), so a
// test reads as "this shape, that verdict" rather than as forty lines of
// pod status.

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
)

const probeMessage = "containers with unready status: [main]"

// servingPod is the healthy shape: containers running and ready, and the
// serving condition set, so no limbo classifier may claim it.
func servingPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
				{Type: "ome.io/serving", Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// runningNotReadyPod builds the readiness-limbo shape: phase Running,
// every container started, ContainersReady=False with the kubelet's own
// reason and message.
func runningNotReadyPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.ContainersReady,
				Status:             corev1.ConditionFalse,
				Reason:             evidence.ReasonContainersNotReady,
				Message:            probeMessage,
				LastTransitionTime: metav1.NewTime(since),
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// gateNotFoldedPod builds the gate-limbo shape: phase Running, the
// container running and Ready, ContainersReady=True, and a declared
// readiness gate whose condition nobody has satisfied, so Ready stays
// False.
func gateNotFoldedPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Spec: corev1.PodSpec{
			ReadinessGates: []corev1.PodReadinessGate{{ConditionType: podreadiness.ConditionType}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:               corev1.ContainersReady,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: metav1.NewTime(since),
				},
				{
					Type:               corev1.PodReady,
					Status:             corev1.ConditionFalse,
					Reason:             "ReadinessGatesNotReady",
					Message:            "corresponding condition of pod readiness gate \"ome.io/serving\" does not exist",
					LastTransitionTime: metav1.NewTime(since),
				},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "main",
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}
