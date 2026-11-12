package types

import (
	"maps"
	"slices"
	"strings"

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

// externalHoldTokens are the waits an operator or another controller
// resolves, so the InstanceReadyTimeout clock does not run while an
// operation reports one: admission refused a create for lack of quota,
// the apiserver could not reach an admission webhook for a write, the
// scheduler found no placement for a pod, the gang's PodGroup name is
// still being collected, a node stopped reporting a pod the operation
// needs, a provisioner has not readied capacity for a withheld pod, or
// an operator pause holds the attempt at a step boundary. The hold
// authorities write exactly these tokens plus two reports that keep the
// clock running: the source-rotation report, which names a workload
// getting worse, and the retired-replacement report, which names the
// engine's own deletion of a retired gang replacement, bounded by the
// delete grace.
var externalHoldTokens = map[string]struct{}{
	RejectionReasonQuotaExceeded:        {},
	RejectionReasonAdmissionUnavailable: {},
	WaitingReasonUnschedulable:          {},
	WaitingReasonPodGroupTerminating:    {},
	WaitingReasonNodeUnknown:            {},
	WaitingReasonCapacityProvisioning:   {},
	WaitingReasonPaused:                 {},
}

// ExternalHoldTokens lists the waiting tokens that stop the
// InstanceReadyTimeout clock, sorted.
func ExternalHoldTokens() []string {
	return slices.Sorted(maps.Keys(externalHoldTokens))
}

// OperationWaitingOn reports whether op carries token as its wait.
func OperationWaitingOn(op *InstanceOperation, token string) bool {
	return op != nil && op.Waiting == token
}

// OperationExternallyHeld reports whether an operation waits on
// something outside the workload's control, one of externalHoldTokens or
// the recorded quota refusal, so the InstanceReadyTimeout clock must not
// run through it.
//
// The quota wait is read off both the recorded refusal and its token.
// The refusal is recorded on the pass admission said no and reported as
// a token from the pass after, and the token outlives the record by one
// pass once a create lands; the clock stays stopped for as long as the
// row reports the wait either way.
func OperationExternallyHeld(op *InstanceOperation) bool {
	if op == nil {
		return false
	}
	_, held := externalHoldTokens[op.Waiting]
	return held || OperationCapacityRefused(op)
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

// gangDomainFullPhrases are the reasons the gang-packing scheduler plugin
// (scheduler/pkg/plugins/gangpack) gives when it keeps a gang in the
// topology domain where some of its members already run and that domain
// cannot take the rest. Nothing frees such a gang except removing the
// members already placed: capacity elsewhere does not help. The phrases
// are that plugin's own report, defined in this repository and identical
// on every cluster, not operator-tunable behavior.
var gangDomainFullPhrases = []string{
	"cannot fit all remaining gang members",
	"cannot fit remaining gang members",
	"has no node feasible for gang member",
	"already has members in multiple topology domains",
}

// SchedulerReportsGangDomainFull reports whether the scheduler's
// message says the gang is held to a topology domain that cannot take
// its remaining members.
func SchedulerReportsGangDomainFull(message string) bool {
	for _, phrase := range gangDomainFullPhrases {
		if strings.Contains(message, phrase) {
			return true
		}
	}
	return false
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
