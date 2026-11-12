package inferencereplica

import (
	"context"
	"testing"
)

// TestBlockedRenderConditionTakesThePassClock: the Ready=False a replica
// without pod templates reports transitions at the pass's clock, never at
// the wall clock.
func TestBlockedRenderConditionTakesThePassClock(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	r, _ := clockedReconciler(t, ir)
	blocked := &renderBlocked{Reason: ReasonRenderFailed, Message: "no runtime matches the model"}
	if _, err := r.reportRenderBlocked(context.Background(), ir, blocked); err != nil {
		t.Fatalf("report the blocked render: %v", err)
	}
	expectTransitionAtThePassClock(t, getFreshIR(t, r, ir).Status.Conditions, InferenceReplicaConditionReady)
}
