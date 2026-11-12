package coordination

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// TestGateContext_ACanaryUnitResolvesForTheBatchStepAndThePairingGates reads
// the gate context of a canary unit's members and of a canary over the
// router alone: the unit resolves to its members with the default pacing
// and no coordination policy, the drain gate reads the unit past the group
// check, and the router short-circuits as before.
func TestGateContext_ACanaryUnitResolvesForTheBatchStepAndThePairingGates(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	isvc := canaryUnitService()
	c := canaryUnitClient(t, canaryStaged(eng, 30, 27, 0, 0), canaryStaged(dec, 10, 9, 0, 0))
	for _, comp := range []v1beta1.ComponentType{eng, dec} {
		gateCtx := ResolveGateContextWithDefaults(context.Background(), c, isvc, comp, keepRatioDecoderFirst(5))
		if gateCtx.ShortCircuit || gateCtx.Hold || gateCtx.Scope != CanaryUnit {
			t.Fatalf("%s: shortCircuit=%v hold=%v scope=%d; wanted the canary unit", comp, gateCtx.ShortCircuit, gateCtx.Hold, gateCtx.Scope)
		}
		if got := gateCtx.Group.Components; len(got) != 2 || got[0] != eng || got[1] != dec {
			t.Errorf("%s: the unit's members read %v; wanted [engine decoder]", comp, got)
		}
		if gateCtx.Group.Policy != "" {
			t.Errorf("%s: the unit carries the coordination policy %q; a canary unit has none", comp, gateCtx.Group.Policy)
		}
		target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: batchRevision(comp, "target")}}
		allowed, _, reason := EvaluatePairingDrain(context.Background(), c, isvc, comp, target, keepRatioDecoderFirst(5), nil)
		if !allowed || reason != "target revision declares no pairing protocol" {
			t.Errorf("%s: the drain gate answered allowed=%v %q; it reads the unit and finds no protocol on the target", comp, allowed, reason)
		}
	}
	router := canaryUnitService(v1beta1.RouterComponent)
	gateCtx := ResolveGateContextWithDefaults(context.Background(), c, router, v1beta1.RouterComponent, keepRatioDecoderFirst(5))
	if !gateCtx.ShortCircuit || gateCtx.Scope == CanaryUnit {
		t.Errorf("a canary over the router alone resolved shortCircuit=%v scope=%d; wanted the short circuit", gateCtx.ShortCircuit, gateCtx.Scope)
	}
	allowed, _, _ := EvaluateUpdateGate(context.Background(), c, router, v1beta1.RouterComponent, "", nil, nil, keepRatioDecoderFirst(5), workloadtypes.UpdateStrategyRecreatePod, 0, 0)
	if !allowed {
		t.Errorf("a canary over the router alone is held by the update gate; nothing paces a unit of one member")
	}
}
