package types

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WaitingReasonUnschedulable is the InstanceOperation.Waiting token
// recorded while a pod of the operation carries PodScheduled=False with
// the scheduler's Unschedulable reason. It lives beside the apiserver
// rejection tokens for the same reason they do: the string is fixed
// Kubernetes API semantics, identical on every cluster, not
// operator-tunable behavior.
//
// ENVIRONMENT-CAUSED: the cluster has no placement for a pod the
// revision renders correctly, so the token is deliberately absent from
// workloadCausedWaitingReasons and never charges a retry ladder.
const WaitingReasonUnschedulable = corev1.PodReasonUnschedulable

// OperationUnschedulable reports whether an operation is parked on the
// scheduler-hold waiting token — the scheduler has ruled that one of its
// pods cannot be placed anywhere in the cluster.
func OperationUnschedulable(op *InstanceOperation) bool {
	return op != nil && op.Waiting == WaitingReasonUnschedulable
}

// OperationExternallyHeld reports whether an operation is parked on any
// wait outside the workload's control: admission having refused its
// create for lack of quota, the scheduler finding no placement for its
// pods, the gang scheduler unable to admit its PodGroup, a node that
// stopped reporting a pod whose name the operation needs, or an operator
// pause holding the attempt at a step boundary. Each is a wait an
// operator or another controller resolves, so the InstanceReadyTimeout
// clock must not run through any of them.
//
// The quota wait is read off both the recorded refusal and its token.
// The refusal is recorded on the pass admission said no and reported as
// a token from the pass after, and the token outlives the record by one
// pass once a create lands; the clock stays parked for as long as the
// row reports the wait either way.
func OperationExternallyHeld(op *InstanceOperation) bool {
	return OperationCapacityRefused(op) || OperationQuotaHeld(op) || OperationUnschedulable(op) ||
		OperationGangHeld(op) || OperationNodeUnknown(op) || OperationPaused(op)
}

// OperationQuotaHeld reports whether the operation carries the quota
// authority's token: the hold pass has reported the recorded refusal.
func OperationQuotaHeld(op *InstanceOperation) bool {
	return op != nil && op.Waiting == RejectionReasonQuotaExceeded
}

// PodUnschedulable reports whether a live pod is one the scheduler
// cannot place, and returns the scheduler's own explanation plus the
// moment PodScheduled last transitioned.
//
// That transition — not the pod's creation — is the start of the hold,
// and it is what a grace window must be measured from: creation time
// would also count the queueing before the scheduler ever ruled, so a
// busy scheduler would shorten the operator's window.
func PodUnschedulable(pod *corev1.Pod) (message string, since metav1.Time, ok bool) {
	if pod == nil || pod.DeletionTimestamp != nil {
		return "", metav1.Time{}, false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type != corev1.PodScheduled {
			continue
		}
		if cond.Status != corev1.ConditionFalse || cond.Reason != WaitingReasonUnschedulable {
			return "", metav1.Time{}, false
		}
		return cond.Message, cond.LastTransitionTime, true
	}
	return "", metav1.Time{}, false
}

// FirstUnschedulablePod returns the live pod with no placement whose
// name sorts lowest, with the scheduler's message and when the hold
// started. One unplaceable pod holds the whole Instance: a gang
// converges as a unit, and a single-pod Instance has nothing else to
// wait on.
//
// Chosen by NAME rather than by position. Pods arrive bucketed from an
// informer cache, whose order is a map iteration and differs between
// passes; taking whichever came first would make a gang with several
// unplaceable members record a different pod every pass and rewrite its
// hold forever, which is exactly what the edge trigger exists to
// prevent.
func FirstUnschedulablePod(pods []*corev1.Pod) (*corev1.Pod, string, metav1.Time) {
	var chosen *corev1.Pod
	var message string
	var since metav1.Time
	for _, pod := range pods {
		podMessage, podSince, ok := PodUnschedulable(pod)
		if !ok {
			continue
		}
		if chosen != nil && pod.Name >= chosen.Name {
			continue
		}
		chosen, message, since = pod, podMessage, podSince
	}
	return chosen, message, since
}

// UnschedulableTermination is the evidence record for a scheduler hold:
// the pod that could not be placed, the token, and the scheduler's
// message. Time is the condition transition, so re-observing an
// unchanged hold produces an identical record and the status no-op
// guard keeps the pass write-free.
func UnschedulableTermination(pod *corev1.Pod, message string, since metav1.Time) *InstanceTermination {
	return &InstanceTermination{
		PodName: pod.Name,
		Reason:  WaitingReasonUnschedulable,
		Message: message,
		Time:    since,
	}
}

// PodAdmissionGated reports whether a pod is still held by a scheduling
// gate — queued for admission (e.g. by Kueue) and not yet allowed to run.
// A gated pod is waiting on an external admission authority, not stuck, so
// the InstanceReadyTimeout clock must not run while it is gated.
func PodAdmissionGated(pod *corev1.Pod) bool {
	return pod != nil && len(pod.Spec.SchedulingGates) > 0
}
