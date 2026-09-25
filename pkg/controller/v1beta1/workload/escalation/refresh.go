// refresh.go — the one effect the escalation pass has on a
// row that is already Failed.
//
// Failed is sticky: the pass takes no second edge there, so a row failed
// on the generic deadline backstop keeps reporting "DeadlineExceeded"
// even after the kubelet parks its pod in a reason that names the actual
// cause. The record is the only trace left once the pod is recreated or
// torn down, so it is refreshed once — reason, pod, container and
// message — and nothing else: the recorded time stays where it is, and
// no event, RetryBlock, phase or operation moves. Edge-triggered by the
// recorded reason itself, which stops being the generic one the moment
// the refresh lands. This file decides whether there is anything to
// refresh; the write is status.RecordRefreshedFailure.
package escalation

import "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"

// refreshedFailureEvidence returns the record that should replace a
// Failed row's LastFailure, or nil when there is nothing to refresh:
// the recorded reason is already concrete, or no pod of the row names a
// terminal kubelet reason past the stuck-pod grace.
//
// Only the generic deadline reason is refreshable. A concrete reason
// already on the record is the FIRST named cause, and an operator
// reading it must not have it replaced by whatever state the pod
// happens to be in later.
//
// The recorded time survives the refresh. What changes is the cause,
// not when the row failed, and moving the stamp forward would both
// misdate the failure and make a row that is merely wedged look like it
// keeps failing anew.
func refreshedFailureEvidence(row types.InstanceStatus, rowEvidence InstanceEvidence) *types.InstanceTermination {
	if rowEvidence.StuckPod == nil || rowEvidence.StuckReason == "" {
		return nil
	}
	if row.LastFailure == nil || row.LastFailure.Reason != DeadlineExceededReason {
		return nil
	}
	return types.PodTerminationWithReason(rowEvidence.StuckPod, rowEvidence.StuckReason, row.LastFailure.Time)
}
