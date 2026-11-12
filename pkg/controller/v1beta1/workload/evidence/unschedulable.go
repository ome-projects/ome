package evidence

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
)

// UnschedulableSinceTheGate reports whether since, the pod's PodScheduled
// time from types.PodUnschedulable, may be its creation time. Such a time
// marks when the pod was created behind a queue's scheduling gate, not when
// the scheduler refused it. A pod with the queue-name label is gated from
// its creation until its queue admits it. Kubernetes writes
// PodScheduled=False when it creates such a pod. When the scheduler later
// refuses the pod, the status stays False, so the condition keeps that time.
// The pod cannot show whether a queue really held it, so a labeled pod
// refused in the second it was created gets the same answer. For such a pod
// the wait starts when OME first sees the refusal.
func UnschedulableSinceTheGate(pod *corev1.Pod, since metav1.Time) bool {
	if pod == nil || since.IsZero() || since.After(pod.CreationTimestamp.Time) {
		return false
	}
	_, queued := pod.Labels[constants.KueueQueueLabelKey]
	return queued
}
