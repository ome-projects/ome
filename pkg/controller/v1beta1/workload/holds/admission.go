package holds

import (
	"context"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// admissionHold is the apiserver unable to consult an admission webhook
// on a pod write the operation issued. Admission gave no answer about
// the object, so the Instance is waiting on infrastructure an operator
// repairs, not stuck: the token parks the InstanceReadyTimeout clock
// and nothing escalates while it stands.
//
// Like the quota refusal, the condition is an apiserver answer to a
// write the create or patch site made, and no later pass can ask for it
// again. Unlike the quota refusal it is recorded as the token itself,
// through RecordAdmissionRefusal, with the apiserver's own words as its
// evidence: an operator reading the row sees which webhook the apiserver
// could not reach. The site that recorded it releases it once a write
// of the same pods goes through admission (ReleaseAdmissionRefusal);
// the pass only keeps the recorded report. A serving pod set does not
// retire it: a surge or in-place roll writes to a row that is in
// rotation, so rotation says nothing about whether admission took the
// write, and a pass that retired the token on it would have the site
// re-enter the wait, rewrite the row and announce it again every pass.
var admissionHold = authority{
	token:  types.RejectionReasonAdmissionUnavailable,
	mayOwn: anyOwner,
	report: reportAdmission,
}

// reportAdmission keeps the row's recorded wait: it cannot re-ask the
// apiserver, so the token stands until the write site releases it, and
// the row's pods being in rotation is no release. The record cannot go
// stale past its operation: the token lives on the Operation, which
// every completion clears.
func reportAdmission(_ context.Context, _ PassInput, row Row) (reading, error) {
	if !types.OperationAdmissionHeld(row.Status.Operation) {
		return reading{}, nil
	}
	return reading{waiting: true, evidence: admissionEvidence(row.Status)}, nil
}

// admissionEvidence is the record the row already carries for this wait,
// or nil when LastFailure names something else, so re-marking the hold
// is a no-op write.
func admissionEvidence(s types.InstanceStatus) *types.InstanceTermination {
	if s.LastFailure == nil || s.LastFailure.Reason != types.RejectionReasonAdmissionUnavailable {
		return nil
	}
	return s.LastFailure
}

// RecordAdmissionRefusal records, from the create or patch site, that
// the apiserver could not take podName's write to admission, with the
// apiserver's message as the row's evidence. It reports entered: the
// transition INTO the wait, which is the one edge the caller announces,
// so an operator hears of the outage once per episode and not once per
// pass, and not at all for a row another authority already reports.
//
// The evidence keeps the moment the wait began: re-recording an
// unchanged wait with a moving message is write-free.
func RecordAdmissionRefusal(ctx context.Context, input types.ReconcileInput, plan types.ComponentPlan, idx int32, podName, message string) (entered bool, err error) {
	var observed types.InstanceStatus
	if row := input.ObservedState.Instance(idx); row != nil {
		observed = *row
	}
	evidence := &types.InstanceTermination{
		PodName: podName,
		Reason:  types.RejectionReasonAdmissionUnavailable,
		Message: message,
		Time:    heldSince(observed, types.RejectionReasonAdmissionUnavailable, input.Now()),
	}
	entered, _, err = mark(ctx, input, plan, idx, admissionHold, evidence)
	return entered, err
}

// ReleaseAdmissionRefusal retires the wait once a write of the row's
// pods went through admission. Another authority's token is left alone.
func ReleaseAdmissionRefusal(ctx context.Context, input types.ReconcileInput, idx int32) error {
	return release(ctx, input, idx, types.RejectionReasonAdmissionUnavailable)
}
