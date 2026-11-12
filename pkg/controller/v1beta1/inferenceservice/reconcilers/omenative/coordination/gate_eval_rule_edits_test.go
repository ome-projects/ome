package coordination

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// A group's ratio rule, its budgets and the group itself are read from the
// spec as it is now at every start, while the run keeps its revision and
// the rest of the plan it recorded: a raised tolerance plans the next
// batch, a removed rule leaves each side on its own step or on the
// operator's default, a lowered group budget sizes the next batch while
// the open attempts finish, and a removed group lets both sides roll on
// their own budgets. The members stay as recorded: a member added or
// removed waits for the next run.

// heldUnderTolerance is the picture the tests start from: 12 prefill beside
// 8 decode under RecreatePod at 10 percent, the run opened with the group's
// tolerance at 5, the first batch (2 prefill and 1 decode) serving and one
// more prefill start open. Under 5 the prefill side's batches read 2, 1, 2,
// 1, ..., so its third start opens batch 3 and waits for the decoder's
// batch 2; under 20 they read 2, 2, 2, 2, 1, 2, 1 and the same start belongs
// to batch 2.
func heldUnderTolerance() (batchGroupFixture, map[v1beta1.ComponentType]batchMemberFixture) {
	engine, decoder := twelveEight(v1beta1.UpdateStrategyRecreatePod)
	engine.promoted, engine.inFlight, decoder.promoted = 2, 1, 1
	group := batchGroupFixture{maxUnavailable: "10%", tolerance: ptr.To(int32(5))}
	return group, map[v1beta1.ComponentType]batchMemberFixture{v1beta1.EngineComponent: engine, v1beta1.DecoderComponent: decoder}
}

// ruleOn is the operator's ratio rule with no default tolerance, so the
// group's own tolerance plans the batches.
var ruleOn = GroupDefaults{PrefillDecode: PrefillDecodeRules{KeepRatio: true}}

func TestEvaluateUpdateGate_ARaisedToleranceReachesTheNextStart(t *testing.T) {
	eng := v1beta1.EngineComponent
	recreate := workloadtypes.UpdateStrategyRecreatePod
	recorded, members := heldUnderTolerance()

	isvc := batchService(recorded, members)
	allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), eng, recreate, 0, ruleOn)
	if allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "engine batch 3 waits for decoder batch 2 to serve") {
		t.Fatalf("under the recorded tolerance of 5 the third prefill start opens batch 3 and waits; got allowed=%v gate=%s reason=%q", allowed, gate, reason)
	}

	raised := recorded
	raised.edit = func(spec *v1beta1.RolloutSpec) { spec.Groups[0].MaintainRatio.Tolerance = ptr.To(int32(20)) }
	isvc = batchService(raised, members)
	if got := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.MaintainRatio.Tolerance; got == nil || *got != 5 {
		t.Fatalf("the run must keep the plan it recorded; its tolerance reads %v", got)
	}
	allowed, gate, reason = batchGateWith(t, isvc, batchClient(t, members), eng, recreate, 0, ruleOn)
	if !allowed {
		t.Fatalf("with the tolerance raised to 20 the third prefill start belongs to batch 2 and must be admitted; got gate=%s reason=%q", gate, reason)
	}
	gateCtx := ResolveGateContextWithDefaults(context.Background(), batchClient(t, members), isvc, eng, ruleOn)
	gateCtx.TargetRevision = forwardTarget(isvc, eng)
	batch, err := gateCtx.readBatchStep()
	if err != nil || !batch.governs {
		t.Fatalf("the batch step must govern the pair: governs=%v err=%v", batch.governs, err)
	}
	wantMarks, _ := pairedBatchPlan(12, 8, 2, 1, 20)
	if got := batch.members[batch.acting].marks; pictures(marksAsPictures(got)) != pictures(marksAsPictures(wantMarks)) {
		t.Fatalf("the prefill marks read %v, want the plan at 20, %v", got, wantMarks)
	}
	engine, decoder := members[eng], members[v1beta1.DecoderComponent]
	engine.inFlight = 2
	fourStarted := map[v1beta1.ComponentType]batchMemberFixture{eng: engine, v1beta1.DecoderComponent: decoder}
	isvc = batchService(raised, fourStarted)
	allowed, _, reason = batchGateWith(t, isvc, batchClient(t, fourStarted), eng, recreate, 0, ruleOn)
	if allowed || !strings.Contains(reason, "engine batch 3 waits for decoder batch 2 to serve") {
		t.Fatalf("with 4 prefill started the next start opens batch 3 of the new plan and waits for the decoder's batch 2; got allowed=%v reason=%q", allowed, reason)
	}

	engine.inFlight = 0
	got, stalled := driveBatchRoll(t, raised, engine, decoder, recreate, ruleOn)
	if want := []rollPicture{{4, 2}, {6, 3}, {8, 4}, {9, 5}, {11, 6}, {12, 7}, {12, 8}}; stalled || pictures(got) != pictures(want) {
		t.Errorf("from 2:1 the rest of the roll walks the plan at 20:\n got %s (stalled %v)\nwant %s", pictures(got), stalled, pictures(want))
	}
}

// marksAsPictures renders cumulative marks as pictures for a message.
func marksAsPictures(marks []int32) []rollPicture {
	out := make([]rollPicture, 0, len(marks))
	for _, m := range marks {
		out = append(out, rollPicture{engine: m})
	}
	return out
}

func TestEvaluateUpdateGate_ARemovedRatioRuleLeavesEachSideOnItsOwnStep(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := workloadtypes.UpdateStrategyRecreatePod
	recorded, members := heldUnderTolerance()
	removed := recorded
	removed.edit = func(spec *v1beta1.RolloutSpec) { spec.Groups[0].MaintainRatio = nil }

	t.Run("the operator configured no default tolerance", func(t *testing.T) {
		isvc := batchService(removed, members)
		if isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.MaintainRatio == nil {
			t.Fatal("the run must keep the rule it recorded")
		}
		allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), eng, recreate, 0, ruleOn)
		if !allowed {
			t.Fatalf("with the rule removed the prefill side steps by its own budget of 2, so its third start is batch 2 and must be admitted; got gate=%s reason=%q", gate, reason)
		}
		engine, decoder := members[eng], members[dec]
		engine.inFlight = 0
		got, stalled := driveBatchRoll(t, removed, engine, decoder, recreate, ruleOn)
		if want := []rollPicture{{4, 2}, {6, 3}, {8, 4}, {10, 5}, {12, 6}, {12, 7}, {12, 8}}; stalled || pictures(got) != pictures(want) {
			t.Errorf("from 2:1 each side walks its own step, the batches still in order:\n got %s (stalled %v)\nwant %s", pictures(got), stalled, pictures(want))
		}
	})

	t.Run("the operator configured a default tolerance of 5", func(t *testing.T) {
		isvc := batchService(removed, members)
		allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), eng, recreate, 0, keepRatio(5))
		if allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "engine batch 3 waits for decoder batch 2 to serve") {
			t.Fatalf("with the rule removed the operator's default of 5 plans the batches, so the third prefill start still waits; got allowed=%v gate=%s reason=%q", allowed, gate, reason)
		}
	})

	t.Run("a router paced by the serving-ratio tolerance", func(t *testing.T) {
		rtr := v1beta1.RouterComponent
		surge := workloadtypes.UpdateStrategySurgeThenDrain
		pair := map[v1beta1.ComponentType]batchMemberFixture{
			eng: {desired: 12, strategy: v1beta1.UpdateStrategySurgeThenDrain, surge: "10%", promoted: 6},
			rtr: {desired: 8, strategy: v1beta1.UpdateStrategySurgeThenDrain, surge: "10%", promoted: 1},
		}
		group := batchGroupFixture{maxSurge: "25%", tolerance: ptr.To(int32(5)), components: []v1beta1.ComponentType{eng, rtr}}
		isvc := batchService(group, pair)
		allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, pair), eng, surge, 0, GroupDefaults{})
		if allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "outpace") {
			t.Fatalf("with 6 of 12 engines beside 1 of 8 routers on version 2 the version-progress check holds the engine; got allowed=%v gate=%s reason=%q", allowed, gate, reason)
		}
		group.edit = func(spec *v1beta1.RolloutSpec) { spec.Groups[0].MaintainRatio = nil }
		isvc = batchService(group, pair)
		allowed, gate, reason = batchGateWith(t, isvc, batchClient(t, pair), eng, surge, 0, GroupDefaults{})
		if !allowed {
			t.Fatalf("with the rule removed no tolerance stands between the engine and the router; got gate=%s reason=%q", gate, reason)
		}
	})
}

func TestEvaluateUpdateGate_ARemovedGroupLetsBothSidesRollOnTheirOwnBudgets(t *testing.T) {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	recreate := workloadtypes.UpdateStrategyRecreatePod
	engine, decoder := twelveEight(v1beta1.UpdateStrategyRecreatePod)
	engine.inFlight, decoder.promoted = 2, 1
	members := map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder}
	recorded := batchGroupFixture{maxUnavailable: "10%", tolerance: ptr.To(int32(5))}

	isvc := batchService(recorded, members)
	allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), dec, recreate, 0, ruleOn)
	if allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "decoder batch 2 waits for engine batch 1 to serve") {
		t.Fatalf("under the recorded group the decoder's second start waits for prefill batch 1; got allowed=%v gate=%s reason=%q", allowed, gate, reason)
	}

	t.Run("the only group is removed", func(t *testing.T) {
		gone := recorded
		gone.edit = func(spec *v1beta1.RolloutSpec) { spec.Groups = nil }
		isvc := batchService(gone, members)
		if len(isvc.Status.Rollout.ActiveRun.Plan.Groups) != 1 {
			t.Fatal("the run must keep the group it recorded")
		}
		assertRemovedGroupReadsForThePairingGatesAlone(t, isvc, members, "0")
	})

	t.Run("the pair's group is removed beside a router group", func(t *testing.T) {
		withRouter := map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder, rtr: {desired: 2, strategy: v1beta1.UpdateStrategyRecreatePod, step: "1"}}
		two := recorded
		two.recordedBeside = []v1beta1.RolloutGroup{{Components: []v1beta1.ComponentType{rtr}, RollingUpdate: &v1beta1.GroupRollingUpdate{}}}
		two.edit = func(spec *v1beta1.RolloutSpec) { spec.Groups = spec.Groups[1:] }
		isvc := batchService(two, withRouter)
		assertRemovedGroupReadsForThePairingGatesAlone(t, isvc, withRouter, "0")
		gateCtx := ResolveGateContextWithDefaults(context.Background(), batchClient(t, withRouter), isvc, rtr, ruleOn)
		if gateCtx.ShortCircuit || gateCtx.Hold || gateCtx.Group.Name != "1" {
			t.Fatalf("the router's group keeps the name the run recorded for it; got short=%v hold=%v group=%+v", gateCtx.ShortCircuit, gateCtx.Hold, gateCtx.Group)
		}
	})

	t.Run("the group's status entry is pruned while the run stays open", func(t *testing.T) {
		gone := recorded
		gone.edit = func(spec *v1beta1.RolloutSpec) { spec.Groups = nil }
		isvc := batchService(gone, members)
		isvc.Status.RolloutCoordination.Groups[0].Phase = v1beta1.CoordinationPhaseScalingDown
		c := testClient(batchReplica(eng, members[eng]), batchReplica(dec, members[dec]))
		if _, err := Reconcile(context.Background(), ReconcileInputs{
			ISVC: isvc, Client: c, Reader: c, Now: time.Now(),
			ComponentDeploymentModes: testOMENativeModes(eng, dec),
		}); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if isvc.Status.RolloutCoordination != nil {
			t.Fatalf("the removed group's entry must be pruned although the run still records it; got %+v", isvc.Status.RolloutCoordination)
		}
		if isvc.Status.Rollout.ActiveRun == nil || len(isvc.Status.Rollout.ActiveRun.Plan.Groups) != 1 {
			t.Fatal("the coordination pass must not touch the run")
		}
	})
}

func TestEvaluateUpdateGate_AMemberEditMidRunKeepsTheRecordedMembers(t *testing.T) {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	recreate := workloadtypes.UpdateStrategyRecreatePod
	recorded, members := heldUnderTolerance()
	for _, edit := range []struct {
		name  string
		apply func(spec *v1beta1.RolloutSpec)
	}{
		{name: "the router added to the group", apply: func(spec *v1beta1.RolloutSpec) {
			spec.Groups[0].Components = []v1beta1.ComponentType{eng, dec, rtr}
		}},
		{name: "the decoder removed from the group", apply: func(spec *v1beta1.RolloutSpec) {
			spec.Groups[0].Components = []v1beta1.ComponentType{eng}
		}},
	} {
		t.Run(edit.name, func(t *testing.T) {
			edited := recorded
			edited.edit = edit.apply
			isvc := batchService(edited, members)
			allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), eng, recreate, 0, ruleOn)
			if allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "engine batch 3 waits for decoder batch 2 to serve") {
				t.Fatalf("the recorded pair keeps pacing the prefill side's third start; got allowed=%v gate=%s reason=%q", allowed, gate, reason)
			}
			gateCtx := ResolveGateContextWithDefaults(context.Background(), batchClient(t, members), isvc, eng, ruleOn)
			if got := gateCtx.Group.Components; gateCtx.Scope != CoordinationGroup || len(got) != 2 || got[0] != eng || got[1] != dec {
				t.Fatalf("the gates read the members the run recorded; got scope=%d members=%v", gateCtx.Scope, got)
			}
			engine, decoder := members[eng], members[dec]
			engine.inFlight = 0
			got, stalled := driveBatchRoll(t, edited, engine, decoder, recreate, ruleOn)
			if want := []rollPicture{{3, 2}, {5, 3}, {6, 4}, {8, 5}, {9, 6}, {11, 7}, {12, 8}}; stalled || pictures(got) != pictures(want) {
				t.Errorf("from 2:1 the roll walks the recorded plan at 5:\n got %s (stalled %v)\nwant %s", pictures(got), stalled, pictures(want))
			}
		})
	}
}

func TestEvaluateUpdateGate_ALoweredGroupBudgetSizesTheNextBatchWhileTheOpenAttemptsFinish(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := workloadtypes.UpdateStrategyRecreatePod
	engine, decoder := twelveEight(v1beta1.UpdateStrategyRecreatePod)
	engine.inFlight, decoder.inFlight = 2, 1
	members := map[v1beta1.ComponentType]batchMemberFixture{eng: engine, dec: decoder}
	recorded := batchGroupFixture{maxUnavailable: "10%", tolerance: ptr.To(int32(5))}
	stepsOf := func(isvc *v1beta1.InferenceService) map[v1beta1.ComponentType]int32 {
		t.Helper()
		gateCtx := ResolveGateContextWithDefaults(context.Background(), batchClient(t, members), isvc, eng, ruleOn)
		gateCtx.TargetRevision = forwardTarget(isvc, eng)
		batch, err := gateCtx.readBatchStep()
		if err != nil || !batch.governs {
			t.Fatalf("the batch step must govern the pair: governs=%v err=%v", batch.governs, err)
		}
		out := map[v1beta1.ComponentType]int32{}
		for _, m := range batch.members {
			out[m.component] = m.step
		}
		return out
	}

	isvc := batchService(recorded, members)
	if steps := stepsOf(isvc); steps[eng] != 2 || steps[dec] != 1 {
		t.Fatalf("under the recorded budget of 10 percent the steps read %v, want 2 and 1", steps)
	}

	lowered := recorded
	lowered.edit = func(spec *v1beta1.RolloutSpec) {
		spec.Groups[0].RollingUpdate.MaxUnavailable = ptr.To(intstr.FromString("5%"))
	}
	isvc = batchService(lowered, members)
	if got := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.RollingUpdate.MaxUnavailable; got == nil || got.String() != "10%" {
		t.Fatalf("the run must keep the budget it recorded; it reads %v", got)
	}
	if steps := stepsOf(isvc); steps[eng] != 1 || steps[dec] != 1 {
		t.Fatalf("with the group's budget lowered to 5 percent the steps read %v, want 1 and 1", steps)
	}
	allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), eng, recreate, 0, ruleOn)
	if allowed || gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(reason, "engine batch 3 waits for decoder batch 2 to serve") {
		t.Fatalf("the 2 prefill and 1 decode open at the edit finish first: the next prefill start is batch 3 of the plan at steps 1 and 1 and waits; got allowed=%v gate=%s reason=%q", allowed, gate, reason)
	}

	engine.inFlight, engine.promoted, decoder.inFlight, decoder.promoted = 0, 2, 0, 1
	got, stalled := driveBatchRoll(t, lowered, engine, decoder, recreate, ruleOn)
	if stalled || len(got) == 0 || got[len(got)-1] != (rollPicture{12, 8}) {
		t.Fatalf("the roll must finish under the lowered budget; got %s (stalled %v)", pictures(got), stalled)
	}
	last := rollPicture{2, 1}
	for _, p := range got {
		if p.engine-last.engine > 1 || p.decoder-last.decoder > 1 {
			t.Errorf("a round after the edit moved more than 1 prefill or 1 decode: %s then %s (all rounds %s)", last, p, pictures(got))
		}
		last = p
	}
}

// assertRemovedGroupReadsForThePairingGatesAlone holds each pairing side of a
// recorded group the spec no longer declares to its own budget: its start is
// admitted with two Instances already out, and the gate context hands the
// recorded group, under its recorded name, to the pairing gates alone.
func assertRemovedGroupReadsForThePairingGatesAlone(t *testing.T, isvc *v1beta1.InferenceService, members map[v1beta1.ComponentType]batchMemberFixture, wantGroup string) {
	t.Helper()
	for _, comp := range []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent} {
		allowed, gate, reason := batchGateWith(t, isvc, batchClient(t, members), comp, workloadtypes.UpdateStrategyRecreatePod, 2, ruleOn)
		if !allowed {
			t.Fatalf("with its group removed the %s rolls on its own budget and nothing holds it; got gate=%s reason=%q", comp, gate, reason)
		}
		gateCtx := ResolveGateContextWithDefaults(context.Background(), batchClient(t, members), isvc, comp, ruleOn)
		if gateCtx.ShortCircuit || gateCtx.Scope != RecordedGroupPairing || gateCtx.Group.Name != wantGroup || !groupPairsEngineAndDecoder(gateCtx.Group) {
			t.Fatalf("the %s reads its recorded group %q for the pairing gates alone; got short=%v scope=%d group=%+v", comp, wantGroup, gateCtx.ShortCircuit, gateCtx.Scope, gateCtx.Group)
		}
	}
}
