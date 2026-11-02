package coordination

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The pictures below are a prefill/decode service of 2 engines and 6
// decoders, both replaced one at a time by RecreatePod (maxUnavailable 1)
// in one blueGreen group, whose pairing protocol is changed from pair-v1
// to pair-v2 and, while that change is half done, to pair-v3. The engine's
// floor is 1 and never holds a step; the decoder's floor is 5 of 6, and an
// old engine is the only partner its old decoders have.

// threeCohortCRs are the revisions of both Components for pair-v1, pair-v2
// and the roll target; the target revision hash is the one forwardTarget
// builds for the gate, so rows on it read as promoted to the batch step.
func threeCohortCRs(targetProtocol string) []client.Object {
	var out []client.Object
	for _, comp := range pairingComponents {
		out = append(out,
			pairingCR("prod", "llama", comp, "v1", "pair-v1"),
			pairingCR("prod", "llama", comp, "v2", "pair-v2"),
			pairingCR("prod", "llama", comp, "target", targetProtocol),
		)
	}
	return out
}

// rolledPairingIR is sizedPairingIR with the counters the budget gate and
// the batch step read: the roll's update revision is the target, and every
// listed row serves.
func rolledPairingIR(component v1beta1.ComponentType, strategy v1beta1.UpdateStrategyType, replicas, maxUnavailable int, hashes ...string) *v1beta1.InferenceReplica {
	ir := sizedPairingIR("prod", "llama", component, servingInstances("llama", component, hashes...), strategy, replicas, maxUnavailable)
	ir.Status.UpdateRevision = "llama-" + string(component) + "-target"
	ir.Status.Replicas = int32(replicas)
	ir.Status.ServingReplicas = int32(len(hashes))
	ir.Status.ReadyReplicas = int32(len(hashes))
	return ir
}

// askPairing asks the pairing gate about a drain-first start of component
// whose row runs the revision of hash.
func askPairing(t *testing.T, isvc *v1beta1.InferenceService, objs []client.Object, component v1beta1.ComponentType, hash string) (bool, string) {
	t.Helper()
	gate := pairingGate(t, isvc, component, objs...)
	return gate.CheckPairing("llama-"+string(component)+"-"+hash, workloadtypes.UpdateStrategyRecreatePod, 0, 0)
}

// askStack asks the whole gate stack about the same start.
func askStack(t *testing.T, isvc *v1beta1.InferenceService, objs []client.Object, component v1beta1.ComponentType, hash string) (bool, v1beta1.RolloutHoldGate, string) {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithObjects(objs...))
	return EvaluateUpdateGate(context.Background(), c, isvc, component, "llama-"+string(component)+"-"+hash, forwardTarget(isvc, component), nil, GroupDefaults{},
		workloadtypes.UpdateStrategyRecreatePod, 0, 0)
}

// deadEndPicture is the fleet once one decoder has moved on to pair-v3:
// engines {v1:1, v2:1}, decoders {v1:4, v2:1, v3:1}. The decoder sits at
// its floor of 5 (the v3 decoder has no partner), and every step of either
// side takes one more decoder out of pairing while a replacement starts.
func deadEndPicture() []client.Object {
	return append(threeCohortCRs("pair-v3"),
		rolledPairingIR(v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 2, 1, "v1", "v2"),
		rolledPairingIR(v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 6, 1, "v1", "v1", "v1", "v1", "v2", "target"),
	)
}

// Every start of both sides is held by the pairable floor alone, so the
// gate admits exactly one: the v2 engine's move to pair-v3, whose
// replacement gives the v3 decoder its partner and costs the decoder one
// pairable Instance while it starts. The v1 engine (the only partner of
// four decoders) and both decoder steps stay held with the floor's own
// words, and the whole stack lets the admitted step through.
func TestCheckPairing_FloorDeadEndAdmitsTheStepThatRestoresAPartner(t *testing.T) {
	isvc := pairingISVC("pair-v3")
	objs := deadEndPicture()

	ok, reason := askPairing(t, isvc, objs, v1beta1.EngineComponent, "v2")
	if !ok {
		t.Fatalf("the v2 engine's move to pair-v3 is the one step that keeps the transition moving; held: %s", reason)
	}
	for _, want := range []string{"every step", `engine of cohort "pair-v2"`, `decoder of cohort "pair-v3"`, "partner"} {
		if !strings.Contains(reason, want) {
			t.Errorf("the admitted step must say why it was let through (want %q): %s", want, reason)
		}
	}
	held := []struct {
		component v1beta1.ComponentType
		hash      string
		shortfall string
	}{
		{v1beta1.EngineComponent, "v1", "decoder with 1 pairable"},
		{v1beta1.DecoderComponent, "v1", "decoder with 4 pairable"},
		{v1beta1.DecoderComponent, "v2", "decoder with 4 pairable"},
	}
	for _, tc := range held {
		ok, reason := askPairing(t, isvc, objs, tc.component, tc.hash)
		if ok {
			t.Fatalf("%s from %s: must stay held by the floor, got allowed (%s)", tc.component, tc.hash, reason)
		}
		if !strings.Contains(reason, "below its floor of 5") || !strings.Contains(reason, tc.shortfall) {
			t.Errorf("%s from %s: the hold must name the decoder's shortfall against its floor: %s", tc.component, tc.hash, reason)
		}
	}

	allowed, gate, reason := askStack(t, isvc, objs, v1beta1.EngineComponent, "v2")
	if !allowed {
		t.Fatalf("the admitted step must pass the whole stack, held by %s: %s", gate, reason)
	}
	allowed, gate, _ = askStack(t, isvc, objs, v1beta1.DecoderComponent, "v2")
	if allowed || gate != v1beta1.RolloutHoldGatePairing {
		t.Fatalf("the v2 decoder's step = (%v, %q), want the pairing hold", allowed, gate)
	}
}

// With a third engine on pair-v2 the v2 engine's move leaves the v2
// decoder a partner, so the ordinary simulation admits it and nothing is a
// dead end: the floors keep holding the v1 engine and both decoder steps
// exactly as they do with no escape in play (the v2 decoder's move would
// leave both v2 engines without a partner, 1 of 3 under the engine's floor
// of 2).
func TestCheckPairing_FloorHoldsWhileAnotherStepIsFree(t *testing.T) {
	isvc := pairingISVC("pair-v3")
	objs := append(threeCohortCRs("pair-v3"),
		rolledPairingIR(v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 3, 1, "v1", "v2", "v2"),
		rolledPairingIR(v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 6, 1, "v1", "v1", "v1", "v1", "v2", "target"),
	)
	ok, reason := askPairing(t, isvc, objs, v1beta1.EngineComponent, "v2")
	if !ok {
		t.Fatalf("one of two v2 engines may move while the other keeps the v2 decoder paired: %s", reason)
	}
	if strings.Contains(reason, "every step") {
		t.Errorf("an ordinary admission must not read as the dead-end escape: %s", reason)
	}
	for _, tc := range []struct {
		component v1beta1.ComponentType
		hash      string
		shortfall string
	}{
		{v1beta1.EngineComponent, "v1", "decoder with 1 pairable"},
		{v1beta1.DecoderComponent, "v1", "decoder with 4 pairable"},
		{v1beta1.DecoderComponent, "v2", "engine with 1 pairable"},
	} {
		ok, reason := askPairing(t, isvc, objs, tc.component, tc.hash)
		if ok {
			t.Fatalf("%s from %s: the floor holds this step while the v2 engine's move is free, got allowed (%s)", tc.component, tc.hash, reason)
		}
		if !strings.Contains(reason, "below its floor of") || !strings.Contains(reason, tc.shortfall) {
			t.Errorf("%s from %s: the hold must be the floor's: %s", tc.component, tc.hash, reason)
		}
	}
}

// The one-by-one mutual wall and the last hop answer as before once the
// replicas carry desired counts and budgets: a 1x1 service's first mover
// is admitted through the wall, the lone decoder's hop is admitted through
// the last-hop escape once an engine serves the target, and the last old
// engine beside it stays held until the decoder flips.
func TestCheckPairing_EscapesKeepTheirAnswersBesideTheFloor(t *testing.T) {
	isvc := pairingISVC("pair-v2")
	wall := append(threeCohortCRs("pair-v2"),
		rolledPairingIR(v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 1, 1, "v1"),
		rolledPairingIR(v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 1, 1, "v1"),
	)
	for _, comp := range pairingComponents {
		ok, reason := askPairing(t, isvc, wall, comp, "v1")
		if !ok || !strings.Contains(reason, "deadlocking") {
			t.Fatalf("%s: the 1x1 wall must admit the first mover: ok=%v %s", comp, ok, reason)
		}
	}

	lastHop := append(threeCohortCRs("pair-v2"),
		rolledPairingIR(v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 2, 1, "v1", "target"),
		rolledPairingIR(v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 1, 1, "v1"),
	)
	ok, reason := askPairing(t, isvc, lastHop, v1beta1.DecoderComponent, "v1")
	if !ok || !strings.Contains(reason, "last serving instance") {
		t.Fatalf("the lone decoder's hop must go through the last-hop escape: ok=%v %s", ok, reason)
	}
	ok, reason = askPairing(t, isvc, lastHop, v1beta1.EngineComponent, "v1")
	if ok || !strings.Contains(reason, "would leave no pairable serving engine+decoder pair") {
		t.Fatalf("the last old engine must wait for the decoder to flip: ok=%v %s", ok, reason)
	}
}

// The picture at the second protocol change, with the first pair-v2 pod of
// each side just serving: engines {v1:1, v2:1}, decoders {v1:5, v2:1}. A v1
// decoder moving to pair-v3 would keep the decoder at its floor, but would
// put a pair-v3 decoder beside engines that cannot answer: either engine's
// move to pair-v3 would take the decoder below 5. That step waits; the v2
// engine's move (the v1 decoders keep their partner) and the v2 decoder's
// move (its replacement waits on the same engine, costing nothing) go.
// Once an engine serves pair-v3 the v1 decoder moves, and a decoder with a
// budget of two never waited.
func TestCheckPairing_FirstMoverWaitsUntilThePeerCanAnswer(t *testing.T) {
	isvc := pairingISVC("pair-v3")
	atTheChange := append(threeCohortCRs("pair-v3"),
		rolledPairingIR(v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 2, 1, "v1", "v2"),
		rolledPairingIR(v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 6, 1, "v1", "v1", "v1", "v1", "v1", "v2"),
	)
	ok, reason := askPairing(t, isvc, atTheChange, v1beta1.DecoderComponent, "v1")
	if ok {
		t.Fatalf("a v1 decoder moving first would strand the transition; it must wait for an engine on pair-v3")
	}
	for _, want := range []string{`decoder on cohort "pair-v3" with no engine to pair with`, "below its floor of 5", `holding until a serving engine runs cohort "pair-v3"`} {
		if !strings.Contains(reason, want) {
			t.Errorf("the hold must say what the step would strand and what lifts it (want %q): %s", want, reason)
		}
	}
	if ok, reason := askPairing(t, isvc, atTheChange, v1beta1.DecoderComponent, "v2"); !ok {
		t.Fatalf("the v2 decoder's move costs the decoder nothing it does not already lack: %s", reason)
	}
	if ok, reason := askPairing(t, isvc, atTheChange, v1beta1.EngineComponent, "v2"); !ok {
		t.Fatalf("the v2 engine's move keeps five v1 decoders paired and must go: %s", reason)
	}
	ok, reason = askPairing(t, isvc, atTheChange, v1beta1.EngineComponent, "v1")
	if ok || !strings.Contains(reason, "decoder with 1 pairable") {
		t.Fatalf("the v1 engine is the only partner of five decoders and stays held: ok=%v %s", ok, reason)
	}

	// The engine's replacement serves pair-v3: the v2 decoder, without a
	// partner since its engine left, moves at no cost; a v1 decoder waits
	// for it on the floor alone, and moves once it has served.
	engineServesTarget := append(threeCohortCRs("pair-v3"),
		rolledPairingIR(v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 2, 1, "v1", "target"),
		rolledPairingIR(v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 6, 1, "v1", "v1", "v1", "v1", "v1", "v2"),
	)
	if ok, reason := askPairing(t, isvc, engineServesTarget, v1beta1.DecoderComponent, "v2"); !ok {
		t.Fatalf("the unpartnered v2 decoder's move to pair-v3 must go: %s", reason)
	}
	ok, reason = askPairing(t, isvc, engineServesTarget, v1beta1.DecoderComponent, "v1")
	if ok || !strings.Contains(reason, "decoder with 4 pairable") || strings.Contains(reason, "no engine to pair with") {
		t.Fatalf("a v1 decoder waits on the floor alone while the v2 decoder is out of pairing: ok=%v %s", ok, reason)
	}
	bothServeTarget := append(threeCohortCRs("pair-v3"),
		rolledPairingIR(v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 2, 1, "v1", "target"),
		rolledPairingIR(v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 6, 1, "v1", "v1", "v1", "v1", "v1", "target"),
	)
	if ok, reason := askPairing(t, isvc, bothServeTarget, v1beta1.DecoderComponent, "v1"); !ok {
		t.Fatalf("with pair-v3 served on both sides the v1 decoders roll on: %s", reason)
	}

	twoOut := append(threeCohortCRs("pair-v3"),
		rolledPairingIR(v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 2, 1, "v1", "v2"),
		rolledPairingIR(v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 6, 2, "v1", "v1", "v1", "v1", "v1", "v2"),
	)
	if ok, reason := askPairing(t, isvc, twoOut, v1beta1.DecoderComponent, "v1"); !ok {
		t.Fatalf("a decoder allowed two out of pairing leaves room for the engine's answer and must go: %s", reason)
	}
}

// The batch step and the pairable floor can hold each other. With engines
// {v1:1, v2:1} and decoders {v1:4, v2:2} (both sides on RecreatePod one at
// a time, the first batch of each served) the decoder's third drain would
// open batch 3 while the engine's batch 2 has not served, and the engine's
// second drain is held by the floor until five decoders run pair-v2. The
// batch step therefore does not wait on a peer the pairing gate holds
// still: the decoder goes, the engine stays held. The same picture turned
// around, 6 engines beside 2 decoders, frees the engine. A peer with an
// attempt open is moving, so the batch wait stands.
func TestEvaluateUpdateGate_BatchStepYieldsToAPeerThePairFloorHoldsStill(t *testing.T) {
	isvc := pairingISVC("pair-v2")
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod

	twoBySix := append(threeCohortCRs("pair-v2"),
		rolledPairingIR(eng, recreate, 2, 1, "target", "v1"),
		rolledPairingIR(dec, recreate, 6, 1, "target", "target", "v1", "v1", "v1", "v1"),
	)
	allowed, gate, reason := askStack(t, isvc, twoBySix, dec, "v1")
	if !allowed {
		t.Fatalf("the decoder is the only side that can move and must go, held by %s: %s", gate, reason)
	}
	allowed, gate, reason = askStack(t, isvc, twoBySix, eng, "v1")
	if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "decoder with 2 pairable") {
		t.Fatalf("the last v1 engine = (%v, %q, %q), want the floor's pairing hold", allowed, gate, reason)
	}

	sixByTwo := append(threeCohortCRs("pair-v2"),
		rolledPairingIR(eng, recreate, 6, 1, "target", "target", "v1", "v1", "v1", "v1"),
		rolledPairingIR(dec, recreate, 2, 1, "target", "v1"),
	)
	allowed, gate, reason = askStack(t, isvc, sixByTwo, eng, "v1")
	if !allowed {
		t.Fatalf("the engine is the only side that can move and must go, held by %s: %s", gate, reason)
	}
	allowed, gate, reason = askStack(t, isvc, sixByTwo, dec, "v1")
	if allowed || gate != v1beta1.RolloutHoldGatePairing || !strings.Contains(reason, "engine with 2 pairable") {
		t.Fatalf("the last v1 decoder = (%v, %q, %q), want the floor's pairing hold", allowed, gate, reason)
	}

	// The engine's second drain is open and its pod not yet serving: the
	// decoder's third drain waits for that batch as the batch step says.
	engines := rolledPairingIR(eng, recreate, 2, 1, "target", "v1")
	engines.Status.InstanceStatuses[1] = v1beta1.OMENativeInstanceStatus{
		Index: 1, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: "llama-engine-v1", TargetRevision: "llama-engine-target",
		Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepDrain, TargetRevision: "llama-engine-target"},
	}
	engines.Status.ServingReplicas, engines.Status.ReadyReplicas = 1, 1
	peerMoving := append(threeCohortCRs("pair-v2"), engines,
		rolledPairingIR(dec, recreate, 6, 1, "target", "target", "v1", "v1", "v1", "v1"))
	allowed, gate, reason = askStack(t, isvc, peerMoving, dec, "v1")
	if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != "decoder batch 3 waits for engine batch 2 to serve (1 of 2 serving)" {
		t.Fatalf("with the engine's drain open the batch wait stands = (%v, %q, %q)", allowed, gate, reason)
	}
}

// The waiver reaches only the pairing peer. A router that rolls drain-first
// is batched beside the engine and the decoder, and the pairing gate never
// holds it still: with the last old engine held by the floor, the decoder's
// third drain waits on the routers' batch while it has not served and goes
// once it has, whichever order the group lists its members in.
func TestEvaluateUpdateGate_BatchStepWaivesOnlyThePairingPeer(t *testing.T) {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	for _, order := range [][]v1beta1.ComponentType{{eng, dec, rtr}, {rtr, eng, dec}} {
		isvc := pairingISVC("pair-v2")
		isvc.Spec.Rollout.Groups[0].Components = order
		isvc = pinActiveRun(isvc)
		picture := func(routers ...string) []client.Object {
			return append(threeCohortCRs("pair-v2"),
				rolledPairingIR(eng, recreate, 2, 1, "target", "v1"),
				rolledPairingIR(dec, recreate, 6, 1, "target", "target", "v1", "v1", "v1", "v1"),
				rolledPairingIR(rtr, recreate, 2, 1, routers...),
			)
		}
		allowed, gate, reason := askStack(t, isvc, picture("v1", "v1"), dec, "v1")
		if allowed || gate != v1beta1.RolloutHoldGateRatio || reason != "decoder batch 3 waits for router batch 2 to serve (0 of 2 serving)" {
			t.Fatalf("%v: the decoder waits on the routers' batch beside the held engine = (%v, %q, %q)", order, allowed, gate, reason)
		}
		if allowed, gate, reason := askStack(t, isvc, picture("target", "target"), dec, "v1"); !allowed {
			t.Fatalf("%v: with the routers' batch served the decoder goes past the held engine, held by %s: %s", order, gate, reason)
		}
	}
}

// walkRow is one Instance of a walked roll: the cohort hash it runs and,
// while its replacement starts, the ticks left until that replacement
// serves (zero when the row serves).
type walkRow struct {
	hash    string
	pending int
}

// walkReplica publishes the rows as the replica would: a serving row
// settled on its revision, a pending one in a drain-first attempt toward
// the target with no serving pod.
func walkReplica(component v1beta1.ComponentType, replicas, maxUnavailable int, rows []walkRow) *v1beta1.InferenceReplica {
	ir := rolledPairingIR(component, v1beta1.UpdateStrategyRecreatePod, replicas, maxUnavailable)
	target := "llama-" + string(component) + "-target"
	serving := int32(0)
	for i, r := range rows {
		row := v1beta1.OMENativeInstanceStatus{Index: int32(i), Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "llama-" + string(component) + "-" + r.hash, ServingPodCount: 1}
		if r.pending > 0 {
			row.Phase, row.ServingPodCount, row.TargetRevision = v1beta1.OMENativeInstanceUpdating, 0, target
			row.Operation = &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: workloadtypes.UpdateStepDrain, TargetRevision: target}
		} else {
			serving++
		}
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, row)
	}
	ir.Status.ServingReplicas, ir.Status.ReadyReplicas = serving, serving
	return ir
}

// pairableServingRows counts the serving rows of one side whose cohort
// some serving row of the other side shares, the way the floor reads it.
func pairableServingRows(side, other []walkRow) int {
	n := 0
	for _, r := range side {
		if r.pending > 0 {
			continue
		}
		for _, o := range other {
			if o.pending == 0 && o.hash == r.hash {
				n++
				break
			}
		}
	}
	return n
}

// walkRoll drives both sides through the gate stack one tick at a time:
// each tick serves the replacements that are due, then asks the stack
// about every serving row of each side still off the target, in the given
// side order, and starts the first one admitted per side (one at a time,
// the budget). It fails when a tick admits nothing while rows remain, or
// when a side's pairable serving rows fall under its floor.
func walkRoll(t *testing.T, isvc *v1beta1.InferenceService, crs []client.Object, shape map[v1beta1.ComponentType][2]int, rows map[v1beta1.ComponentType][]walkRow, delay map[v1beta1.ComponentType]int, order []v1beta1.ComponentType, floors map[v1beta1.ComponentType]int) {
	t.Helper()
	done := func() bool {
		for _, side := range pairingComponents {
			for _, r := range rows[side] {
				if r.hash != "target" || r.pending > 0 {
					return false
				}
			}
		}
		return true
	}
	for tick := 0; tick < 60 && !done(); tick++ {
		for _, side := range pairingComponents {
			for i := range rows[side] {
				if rows[side][i].pending > 0 {
					if rows[side][i].pending--; rows[side][i].pending == 0 {
						rows[side][i].hash = "target"
					}
				}
			}
		}
		for _, side := range pairingComponents {
			if n := pairableServingRows(rows[side], rows[pairingPeer(side)]); n < floors[side] {
				t.Fatalf("tick %d: the %s has %d pairable serving Instances, under its floor of %d: %v", tick, side, n, floors[side], rows)
			}
		}
		moved := false
		for _, side := range order {
			objs := append(append([]client.Object{}, crs...),
				walkReplica(v1beta1.EngineComponent, shape[v1beta1.EngineComponent][0], shape[v1beta1.EngineComponent][1], rows[v1beta1.EngineComponent]),
				walkReplica(v1beta1.DecoderComponent, shape[v1beta1.DecoderComponent][0], shape[v1beta1.DecoderComponent][1], rows[v1beta1.DecoderComponent]))
			for i, r := range rows[side] {
				if r.hash == "target" || r.pending > 0 {
					continue
				}
				if allowed, _, _ := askStack(t, isvc, objs, side, r.hash); allowed {
					rows[side][i].pending = delay[side]
					moved = true
					break
				}
			}
		}
		pending := false
		for _, side := range pairingComponents {
			for _, r := range rows[side] {
				pending = pending || r.pending > 0
			}
		}
		if !moved && !pending && !done() {
			t.Fatalf("tick %d: nothing moves and nothing is in flight: %v", tick, rows)
		}
	}
	if !done() {
		t.Fatalf("the roll did not converge: %v", rows)
	}
}

// The whole roll, through the whole stack, from the second protocol change
// to the end, and the plain one-change roll of both shapes: every tick
// admits a step or waits on a replacement in flight, and the side with a
// floor of 5 keeps 5 pairable serving Instances at every tick, whichever
// side's replacements are slow and whichever side the controller asks
// first.
func TestEvaluateUpdateGate_ProtocolChangeKeepsOneMoverAndTheFloor(t *testing.T) {
	two, six := [2]int{2, 1}, [2]int{6, 1}
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	pictures := []struct {
		name     string
		protocol string
		shape    map[v1beta1.ComponentType][2]int
		rows     map[v1beta1.ComponentType][]string
		floors   map[v1beta1.ComponentType]int
	}{
		{
			name: "2 engines beside 6 decoders at the second change", protocol: "pair-v3",
			shape:  map[v1beta1.ComponentType][2]int{eng: two, dec: six},
			rows:   map[v1beta1.ComponentType][]string{eng: {"v1", "v2"}, dec: {"v1", "v1", "v1", "v1", "v1", "v2"}},
			floors: map[v1beta1.ComponentType]int{eng: 1, dec: 5},
		},
		{
			name: "2 engines beside 6 decoders, one change", protocol: "pair-v2",
			shape:  map[v1beta1.ComponentType][2]int{eng: two, dec: six},
			rows:   map[v1beta1.ComponentType][]string{eng: {"v1", "v1"}, dec: {"v1", "v1", "v1", "v1", "v1", "v1"}},
			floors: map[v1beta1.ComponentType]int{eng: 1, dec: 5},
		},
		{
			name: "6 engines beside 2 decoders, one change", protocol: "pair-v2",
			shape:  map[v1beta1.ComponentType][2]int{eng: six, dec: two},
			rows:   map[v1beta1.ComponentType][]string{eng: {"v1", "v1", "v1", "v1", "v1", "v1"}, dec: {"v1", "v1"}},
			floors: map[v1beta1.ComponentType]int{eng: 5, dec: 1},
		},
	}
	orders := [][]v1beta1.ComponentType{{eng, dec}, {dec, eng}}
	delays := []map[v1beta1.ComponentType]int{{eng: 1, dec: 1}, {eng: 3, dec: 1}, {eng: 1, dec: 3}, {eng: 3, dec: 3}}
	for _, pic := range pictures {
		for _, order := range orders {
			for _, delay := range delays {
				t.Run(fmt.Sprintf("%s/%s first/engine %d decoder %d", pic.name, order[0], delay[eng], delay[dec]), func(t *testing.T) {
					rows := map[v1beta1.ComponentType][]walkRow{}
					for side, hashes := range pic.rows {
						for _, h := range hashes {
							rows[side] = append(rows[side], walkRow{hash: h})
						}
					}
					walkRoll(t, pairingISVC(pic.protocol), threeCohortCRs(pic.protocol), pic.shape, rows, delay, order, pic.floors)
				})
			}
		}
	}
}
