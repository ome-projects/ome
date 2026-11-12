package inferencereplica

import (
	"context"
	"testing"
	"time"

	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestInstanceReadyTimeoutConditionTakesThePassClock: the condition that
// reports the Component's readiness deadline transitions at the pass's
// clock, never at the wall clock.
func TestInstanceReadyTimeoutConditionTakesThePassClock(t *testing.T) {
	ir := baselineIR("llama-engine", "prod", 1)
	r, _ := clockedReconciler(t, ir)
	input := workloadtypes.ReconcileInput{
		OwnerObject:             ir,
		WriteAggregateCondition: buildWriteAggregateCondition(r.statusWriter(), r.Client, ir),
	}
	r.reportInstanceReadyTimeout(context.Background(), r.Log, ir, input, 10*time.Minute)
	expectTransitionAtThePassClock(t, getFreshIR(t, r, ir).Status.Conditions, string(workloadtypes.ConditionInstanceReadyTimeoutUnconfigured))
}
