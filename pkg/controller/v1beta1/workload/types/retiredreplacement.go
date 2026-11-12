package types

// WaitingReasonRetiredReplacementTerminating is the InstanceOperation.Waiting
// token both rows of a gang surge carry while the members of a retired
// replacement gang terminate: the source holding its surge, and the
// replacement at its cleanup step. The pair ends in one write once the last
// member is gone, so until then the token is what names the wait on the
// rows.
//
// Not one of the externally-held waits: the deletion is the engine's own
// and the delete grace bounds it, so the deadline clock does not stop for
// the token, nothing escalates while it stands, and nothing releases it.
// It leaves the rows with the operation when the pair ends.
const WaitingReasonRetiredReplacementTerminating = "RetiredReplacementTerminating"

// OperationRetiredReplacementTerminating reports whether an operation is
// reporting that the retired replacement gang's members are terminating.
func OperationRetiredReplacementTerminating(op *InstanceOperation) bool {
	return op != nil && op.Waiting == WaitingReasonRetiredReplacementTerminating
}
