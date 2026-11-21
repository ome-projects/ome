package coordination

import (
	"context"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestGateWaitsOnPeerCounters pins which holds a peer's status releases:
// the two gates that simulate the step against the peer's serving counts,
// and the Held gate, under which the batch step names a peer whose retry
// record is Held and which that peer's release or corrected revision ends.
// A hold on any other gate waits on a revision, the plan pin, or the
// Component's own budget or retry schedule, and a peer's churn must not
// wake it.
func TestGateWaitsOnPeerCounters(t *testing.T) {
	waits := map[v1beta1.RolloutHoldGate]bool{
		v1beta1.RolloutHoldGatePairing:    true,
		v1beta1.RolloutHoldGateRatio:      true,
		v1beta1.RolloutHoldGateHeld:       true,
		v1beta1.RolloutHoldGateSequential: false,
		v1beta1.RolloutHoldGatePlan:       false,
		v1beta1.RolloutHoldGateBudget:     false,
		v1beta1.RolloutHoldGateRetryBlock: false,
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

	allowed, gate, reason := EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, "", forwardTarget(isvc, v1beta1.EngineComponent), nil, GroupDefaults{},
		workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if allowed || gate != v1beta1.RolloutHoldGatePlan {
		t.Fatalf("dark row consult with no run open = (%v, %q, %q), want the plan hold", allowed, gate, reason)
	}

	pinActiveRun(isvc)
	allowed, gate, reason = EvaluateUpdateGate(context.Background(), client, isvc, v1beta1.EngineComponent, "", forwardTarget(isvc, v1beta1.EngineComponent), nil, GroupDefaults{},
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

// TestEvaluateReplicaPlanGate_HoldsWhileTheSpecTrailsTheService checks the
// plan gate as a replica's own pass asks it. A grouped Component starts no
// update while the service's generation is ahead of the one the replica's
// spec was written for: the roll target in hand may be one the operator
// has already replaced. The hold ends once the two agree.
func TestEvaluateReplicaPlanGate_HoldsWhileTheSpecTrailsTheService(t *testing.T) {
	isvc := mkUnavailFixture(iosInt(1))
	isvc.Generation = 5
	replica := func(component v1beta1.ComponentType, stamp string) *v1beta1.InferenceReplica {
		ir := &v1beta1.InferenceReplica{}
		ir.Spec.Component = component
		if stamp != "" {
			ir.Annotations = map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: stamp}
		}
		return ir
	}

	allowed, gate, reason := EvaluateReplicaPlanGate(isvc, replica(v1beta1.EngineComponent, "4"))
	if allowed || gate != v1beta1.RolloutHoldGatePlan || !strings.Contains(reason, "generation 5") || !strings.Contains(reason, "generation 4") ||
		!strings.Contains(reason, "look at the service's conditions and events") {
		t.Fatalf("a spec written for generation 4 under a service at generation 5 = (%v, %q, %q), want the plan hold naming both generations and the service to look at", allowed, gate, reason)
	}

	admitted := map[string]*v1beta1.InferenceReplica{
		"a spec written for the service's generation":  replica(v1beta1.EngineComponent, "5"),
		"a spec newer than the service this pass read": replica(v1beta1.EngineComponent, "6"),
		"a replica with no written generation":         replica(v1beta1.EngineComponent, ""),
		"a written generation that does not parse":     replica(v1beta1.EngineComponent, "five"),
		"a Component outside every group":              replica(v1beta1.DecoderComponent, "4"),
	}
	for name, ir := range admitted {
		if allowed, gate, reason := EvaluateReplicaPlanGate(isvc, ir); !allowed {
			t.Errorf("%s = (%v, %q, %q), want the start admitted", name, allowed, gate, reason)
		}
	}
	if allowed, _, _ := EvaluateReplicaPlanGate(isvc, nil); !allowed {
		t.Error("a nil replica holds nothing")
	}
	if allowed, _, _ := EvaluateReplicaPlanGate(nil, replica(v1beta1.EngineComponent, "4")); !allowed {
		t.Error("a nil service holds nothing, as the plan gate holds nothing on one")
	}

	isvc.Status.Rollout = nil
	allowed, gate, reason = EvaluateReplicaPlanGate(isvc, replica(v1beta1.EngineComponent, "5"))
	if allowed || gate != v1beta1.RolloutHoldGatePlan || !strings.Contains(reason, "no active run") {
		t.Fatalf("with no active run = (%v, %q, %q), want the hold that names the missing run", allowed, gate, reason)
	}
}
