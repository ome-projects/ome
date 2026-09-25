package types

import "testing"

func TestOperationPaused(t *testing.T) {
	if OperationPaused(nil) {
		t.Error("no operation is not a paused one")
	}
	if OperationPaused(&InstanceOperation{}) {
		t.Error("an operation with no waiting token is not paused")
	}
	if OperationPaused(&InstanceOperation{Waiting: WaitingReasonNodeUnknown}) {
		t.Error("another authority's token is not the pause hold")
	}
	if !OperationPaused(&InstanceOperation{Waiting: WaitingReasonPaused}) {
		t.Error("the pause token reads as paused")
	}
}
