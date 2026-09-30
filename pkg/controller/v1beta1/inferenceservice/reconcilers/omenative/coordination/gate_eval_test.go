package coordination

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestGateWaitsOnPeerCounters pins which holds a peer's serving counters
// release: the two gates that simulate the step against them. A hold on
// any other gate waits on a revision, the plan pin, or the Component's own
// budget or retry state, and a peer's counter churn must not wake it.
func TestGateWaitsOnPeerCounters(t *testing.T) {
	waits := map[v1beta1.RolloutHoldGate]bool{
		v1beta1.RolloutHoldGatePairing:    true,
		v1beta1.RolloutHoldGateRatio:      true,
		v1beta1.RolloutHoldGateSequential: false,
		v1beta1.RolloutHoldGatePlan:       false,
		v1beta1.RolloutHoldGateBudget:     false,
		v1beta1.RolloutHoldGateRetryBlock: false,
		v1beta1.RolloutHoldGateHeld:       false,
		"":                                false,
	}
	for gate, want := range waits {
		if got := GateWaitsOnPeerCounters(gate); got != want {
			t.Errorf("GateWaitsOnPeerCounters(%q) = %v, want %v", gate, got, want)
		}
	}
}
