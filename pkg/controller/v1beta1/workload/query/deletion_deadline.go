package query

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// DeletionDeadline is the instant from which a Terminating pod has stood
// longer than its kubelet owes it, and the zero time for a pod that is not
// Terminating. It is the pod's DeletionTimestamp. With
// lifecycle.leaderGoneGracePeriod set it is later for a worker pod that
// shows a grace under its own spec's, or at or under the field's value: a
// later delete may have shortened that grace, and the kubelet counts a
// shortened grace from the moment it learns of it. Such a pod gets the larger
// of the grace it shows and the field's value on top.
func DeletionDeadline(pod *corev1.Pod, lifecycle workload.Lifecycle) time.Time {
	if pod == nil || pod.DeletionTimestamp == nil {
		return time.Time{}
	}
	at := pod.DeletionTimestamp.Time
	bound, set := lifecycle.LeaderGoneGraceSeconds()
	shown := pod.DeletionGracePeriodSeconds
	if !set || shown == nil || pod.Labels[LabelRunner] != workload.RunnerWorker {
		return at
	}
	if own := pod.Spec.TerminationGracePeriodSeconds; *shown > bound && (own == nil || *shown >= *own) {
		return at
	}
	return at.Add(time.Duration(max(*shown, bound)) * time.Second)
}
