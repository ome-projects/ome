package types

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// WaitingReasonNodeUnknown is the InstanceOperation.Waiting token
// recorded while a pod the operation is rebuilding holds one of its
// stable names in phase Unknown.
//
// Unknown says the node stopped reporting, not that the pod died, so the
// name may not be recycled: a kubelet that comes back could still be
// running the container. The wait ends when the pod leaves the phase or
// disappears, or when the force-delete sweep's node-death evidence
// proves no kubelet is left to run it.
//
// ENVIRONMENT-CAUSED: a silent node says nothing about the pod template,
// so the token never charges a revision's retry ladder.
const WaitingReasonNodeUnknown = "NodeUnknown"

// OperationNodeUnknown reports whether an operation is parked on a pod
// whose node has stopped reporting it.
func OperationNodeUnknown(op *InstanceOperation) bool {
	return op != nil && op.Waiting == WaitingReasonNodeUnknown
}

// PodPhaseUnknown reports whether the kubelet has stopped reporting pod:
// phase Unknown, which is neither a running pod nor a terminal one.
func PodPhaseUnknown(pod *corev1.Pod) bool {
	return pod != nil && pod.Status.Phase == corev1.PodUnknown
}

// NodeUnknownTermination is the evidence record for the hold: which pod
// is holding the name and which node went quiet under it.
//
// Time is the pod's creation rather than now, for the reason every hold
// record has a stable moment — re-observing the same wait produces an
// identical record, so the status no-op guard keeps the pass write-free,
// while a same-name successor starts a fresh episode.
func NodeUnknownTermination(pod *corev1.Pod) *InstanceTermination {
	if pod == nil {
		return nil
	}
	node := pod.Spec.NodeName
	if node == "" {
		node = "<unbound>"
	}
	return &InstanceTermination{
		PodName: pod.Name,
		Reason:  WaitingReasonNodeUnknown,
		Message: fmt.Sprintf("%s: pod %s is in phase Unknown on node %s; its kubelet has stopped reporting, so the name is held rather than recycled",
			WaitingReasonNodeUnknown, pod.Name, node),
		Time: pod.CreationTimestamp,
	}
}
