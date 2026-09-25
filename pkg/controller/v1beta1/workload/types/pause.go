package types

// WaitingReasonPaused is the InstanceOperation.Waiting token recorded
// while an operator pause holds an attempt at a step boundary. It lives
// beside the other external-hold tokens because it is the same kind of
// wait: the attempt is fine, and only someone outside the workload can
// end the hold.
//
// ENVIRONMENT-CAUSED in the same sense as the scheduler hold: the
// revision is not on trial, so the token never charges a retry ladder.
const WaitingReasonPaused = "Paused"

// OperationPaused reports whether an operation is parked on the
// operator-pause hold.
func OperationPaused(op *InstanceOperation) bool {
	return op != nil && op.Waiting == WaitingReasonPaused
}
