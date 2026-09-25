package types

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// WaitingReasonSourceUnrouted is the InstanceOperation.Waiting token
// recorded while a surge is still at its Surge step and the source it is
// replacing has left the rotation. The surge does not unroute its own
// source until the drain step, so a source out of rotation before then
// was taken out by something else, and the Instance is serving nothing
// while its replacement comes up.
//
// Deliberately NOT one of the externally-held waits: those park the
// attempt deadline because an operator or another controller owns the
// wait. Here the workload is worse off the longer it lasts, so the
// deadline must keep running and the roll must stay escalatable — the
// token reports the state, it does not excuse it.
const WaitingReasonSourceUnrouted = "SourceUnrouted"

// OperationSourceUnrouted reports whether an operation is reporting that
// the source it is replacing has left the rotation.
func OperationSourceUnrouted(op *InstanceOperation) bool {
	return op != nil && op.Waiting == WaitingReasonSourceUnrouted
}

// SourceUnroutedTermination is the evidence record for the report: which
// source pod stopped taking traffic while its replacement was still
// coming up.
//
// Time is the pod's creation rather than now, for the reason every hold
// record has a stable moment — re-observing the same state produces an
// identical record, so the status no-op guard keeps the pass write-free.
func SourceUnroutedTermination(pod *corev1.Pod) *InstanceTermination {
	if pod == nil {
		return nil
	}
	return &InstanceTermination{
		PodName: pod.Name,
		Reason:  WaitingReasonSourceUnrouted,
		Message: fmt.Sprintf("%s: source pod %s is out of the routed Service while its surge replacement is not serving yet; the surge did not take it out",
			WaitingReasonSourceUnrouted, pod.Name),
		Time: pod.CreationTimestamp,
	}
}
