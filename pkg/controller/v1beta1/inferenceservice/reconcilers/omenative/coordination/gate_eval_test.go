package coordination

import (
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
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

// TestEvaluatePlanGate_HoldsUntilARunIsPinned pins the plan gate taken
// alone, the question the workload asks for a start it admits without the
// capacity consult: a grouped Component is held while no run is pinned and
// admitted once one is, whatever its serving counters read.
func TestEvaluatePlanGate_HoldsUntilARunIsPinned(t *testing.T) {
	isvc := mkUnavailFixture(iosInt(1))
	isvc.Status.Rollout = nil
	allowed, gate, reason := EvaluatePlanGate(isvc, v1beta1.EngineComponent)
	if allowed || gate != v1beta1.RolloutHoldGatePlan || !strings.Contains(reason, "not pinned") {
		t.Fatalf("plan gate with no run open = (%v, %q, %q), want the plan hold", allowed, gate, reason)
	}
	pinActiveRun(isvc)
	if allowed, gate, reason := EvaluatePlanGate(isvc, v1beta1.EngineComponent); !allowed {
		t.Fatalf("plan gate with a run open = (%v, %q, %q), want the start admitted to the capacity gates", allowed, gate, reason)
	}
	if allowed, _, _ := EvaluatePlanGate(nil, v1beta1.EngineComponent); !allowed {
		t.Fatal("a nil service holds nothing, as the full gate stack short-circuits on one")
	}
}

// TestEvaluateUpdateGate_DarkRowConsultIsHeldWithoutAndWithARun pins what
// the gate stack answers when asked about the recreate of an Instance that
// serves nothing, which is why the workload's repair admission does not
// ask it. The stack has no notion of a start that takes nothing further
// offline: with no run open the plan gate holds every consult, and with a
// run open the unavailability gate counts the dark Instance once as
// unavailability already open and again as the start it is asked about,
// so a budget of one reads as exhausted by the only start that can end
// the outage.
func TestEvaluateUpdateGate_DarkRowConsultIsHeldWithoutAndWithARun(t *testing.T) {
	isvc := mkUnavailFixture(iosInt(1))
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {
			Lifecycle: &v1beta1.LifecycleStatus{Replicas: 2, ReadyReplicas: 1, ServingReplicas: 1},
		},
	}
	isvc.Status.Rollout = nil
	client := fakeClientForISVC(isvc)

	allowed, gate, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, forwardTarget(isvc, v1beta1.EngineComponent), nil, GroupDefaults{},
		workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if allowed || gate != v1beta1.RolloutHoldGatePlan {
		t.Fatalf("dark row consult with no run open = (%v, %q, %q), want the plan hold", allowed, gate, reason)
	}

	pinActiveRun(isvc)
	allowed, gate, reason = EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, forwardTarget(isvc, v1beta1.EngineComponent), nil, GroupDefaults{},
		workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if allowed || gate != v1beta1.RolloutHoldGateBudget {
		t.Fatalf("dark row consult with a run open = (%v, %q, %q), want the unavailability budget denial", allowed, gate, reason)
	}
	for _, counted := range []string{"current 1", "would become 2"} {
		if !strings.Contains(reason, counted) {
			t.Errorf("the denial must show the dark Instance counted as open unavailability and again as the start; got %q", reason)
		}
	}
}
