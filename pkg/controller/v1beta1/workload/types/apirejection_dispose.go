package types

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DisposeAPIRejection ends an attempt the apiserver permanently rejected.
// There is no pod — the rejection itself is the evidence — so LastFailure
// is built from the classified rejection rather than from container
// status, and the Operation is cleared + Phase=Failed in the same single
// mutation every other terminal disposition uses.
//
// A workload-caused rejection (RejectionReasonInvalidPodSpec) additionally
// records a RetryBlock against the attempt's target revision BEFORE the
// clear, so the same revision is not re-admitted while a corrected one is.
// Writer ordering matches the deadline disposition: block first, clear
// second, so a crash between the two re-enters here and the writer's wave
// dedup refreshes the block without recounting. An environment-caused
// rejection blames no revision and records no block.
//
// blameRevision lets a caller withhold that blame when the rejected pod
// is not a faithful render of the target revision — a migration surge
// carries a placement overlay the revision never asked for, so holding
// the revision for the overlay's fault would wedge an innocent rollout.
//
// The target revision is the attempt's Operation.TargetRevision, falling
// back to the owner's UpdateRevision for an unpinned Create (the revision
// the rejected pod was rendered from). heldRevision names the revision
// that was blocked, empty when none was.
//
// Non-permanent classes are a no-op: they carry their own pacing at the
// call site and the attempt is still alive.
//
// Lives in the leaf types package beside RecordUpdateFailureInRetryBlock,
// which it composes, so the create and in-place patch paths in
// workload/ops reach it without a workload → workload/ops import cycle.
func DisposeAPIRejection(ctx context.Context, input ReconcileInput, inst InstanceStatus, rejection APIRejection, blameRevision bool) (heldRevision string, err error) {
	if !rejection.Class.Permanent() {
		return "", nil
	}
	if rejection.Class == APIRejectionPermanentWorkload && blameRevision {
		heldRevision = rejectionTargetRevision(input, inst)
		if heldRevision != "" {
			if err := RecordUpdateFailureInRetryBlock(ctx, input, heldRevision, rejection.Reason, true); err != nil {
				return "", fmt.Errorf("record retry block for rejected attempt (instance=%d rev=%s): %w", inst.Index, heldRevision, err)
			}
		}
	}
	termination := &InstanceTermination{
		Reason:  rejection.Reason,
		Message: rejection.Message,
		Time:    metav1.NewTime(input.Now()),
	}
	if err := FailInstanceClearingOperation(ctx, input, inst.Index, termination); err != nil {
		return heldRevision, fmt.Errorf("clear operation + stamp Failed (instance=%d): %w", inst.Index, err)
	}
	return heldRevision, nil
}

// rejectionTargetRevision resolves the revision a rejected attempt was
// converging toward: the pin the operation carries, else the owner's
// current UpdateRevision (an unpinned Create renders from it).
func rejectionTargetRevision(input ReconcileInput, inst InstanceStatus) string {
	if inst.Operation != nil && inst.Operation.TargetRevision != "" {
		return inst.Operation.TargetRevision
	}
	return input.ObservedState.UpdateRevision
}
