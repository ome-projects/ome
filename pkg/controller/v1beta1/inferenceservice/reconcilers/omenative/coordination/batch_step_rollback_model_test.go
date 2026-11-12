package coordination

import (
	"fmt"
	"math/rand"
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
)

// This file widens the batch-step model of
// pd_pacing_gates_never_wait_on_each_other_test.go and
// batch_step_model_test.go, whose world and runner it shares, to the
// operator's rollback and pause: a rollback mid-roll, where the operator
// re-applies the previous revision and the roll turns around on both sides
// at once; the canary's rollback, where the unit's members return to the
// stable revision through their pacing under a step partition of 0, which
// outranks the operator's partition, while the pushed revision stays
// their update revision; and a pause and resume, a rule of this world
// under which no attempt opens or advances while partitions, rollbacks
// and gang losses still land (the gates are asked under the paused world;
// the engine's own pause code is not driven).
// Each lands at every moment of the roll (idle, settling, rotating,
// draining, the last Instance, a side held on its spent retries), with the
// operator's partition set, raised, lowered or cleared before or after it,
// on the reference prefill:decode shapes under both strategies and the
// chart's settings, and the roll is then read to its end: no two sides
// waiting on each other, no side on itself, the budgets, the batch promise
// and the two sides' shares against the tolerance.
//
// What a rollback leaves of each row follows the engine. An Instance on the
// revision the roll returns to is promoted and one on the pushed revision
// is to move. A recreate in flight is rebuilt on the revision it last ran:
// the recreate keeps that revision as the running one, so the batch step
// reads the Instance as promoted while nothing serves. A surge at its first
// step ends and its source stays. A failed surge, a stopped attempt and a
// failed drain return to the revision they run. A created Instance is
// created again on the target. The retry record belongs to the pushed
// revision and holds nothing toward the revision the roll returns to.
//
// OME_BATCH_STEP_ROLLBACK_HUNT=full widens the configurations and the
// bounds for a long local run; OME_BATCH_STEP_ROLLBACK_HUNT_ONLY=<text> keeps
// the configurations whose name contains the text.

// rollbackText names the rollback and pause knobs in a configuration's name.
func (c huntConfig) rollbackText() string {
	var s string
	if c.rollbacks > 0 {
		s += fmt.Sprintf(" rollbacks=%d", c.rollbacks)
	}
	if c.canaryRollbacks > 0 {
		s += fmt.Sprintf(" canaryRollbacks=%d", c.canaryRollbacks)
	}
	if c.pauses > 0 {
		s += fmt.Sprintf(" pauses=%d", c.pauses)
	}
	return s
}

// rolledBackRow is what a rollback leaves of one row, read against the
// revision the roll returns to. ok is false for a row the model does not
// turn around: a replacement the pairing gate keeps beside its source, an
// in-place patch, a gang short of a member, a pod on a lost node, a member
// the scheduler refuses.
func rolledBackRow(r huntRow) (row huntRow, ok bool) {
	switch r {
	case huntOld:
		return huntPromoted, true
	case huntPromoted, huntPromotedUncounted:
		return huntOld, true
	case huntOpenDrain, huntFailedUnserving:
		return huntDrainBack, true
	case huntOpenSurge, huntFailedSurge, huntRetriesStopped:
		return huntPromoted, true
	case huntCreating:
		return huntCreating, true
	}
	return r, false
}

// turnsAround reports a state a rollback may land on: every row is one the
// model turns around, every status is written for the push, and no
// rollback stands yet.
func (w *huntWorld) turnsAround(st *huntState) bool {
	for _, s := range st.sides {
		if s.behind || s.reversed {
			return false
		}
		for _, r := range s.rows {
			if _, ok := rolledBackRow(r); !ok {
				return false
			}
		}
	}
	return true
}

// turnMoves lists the operator's rollback, the canary's rollback and the
// pause a state still offers: the rollback on a rolling group, the
// canary's rollback while the canary's steps run, a pause while none
// stands and the roll is not complete.
func (w *huntWorld) turnMoves(st *huntState) []huntMove {
	var out []huntMove
	if st.rollbacks > 0 && w.cfg.canary == nil && w.turnsAround(st) {
		out = append(out, huntMove{kind: "rollback"})
	}
	if st.canaryRollbacks > 0 && w.cfg.canary != nil && st.canaryStep >= 0 && int(st.canaryStep) < len(w.cfg.canary) && w.turnsAround(st) {
		out = append(out, huntMove{kind: "canary-rollback"})
	}
	if st.pauses > 0 && !st.paused && !st.complete() {
		out = append(out, huntMove{kind: "pause"})
	}
	return out
}

// turnAround turns the roll around on every side: the rows are re-read
// against the revision the roll returns to, the retry record stays with
// the pushed revision, the operator's partition is kept. The operator's
// rollback changes the pod templates, so each side's status predates it
// until the side's next pass writes it, and a peer reads it as not yet
// observed until then. The canary's rollback changes the pacing alone,
// which leaves the status fresh: it names the stable revision on the
// pacing and writes a step partition of 0, the canary past its last step,
// which outranks the operator's partition as a step's does.
func (w *huntWorld) turnAround(next *huntState, canary bool) {
	for i := range next.sides {
		s := &next.sides[i]
		s.inherited = 0
		for idx, r := range s.rows {
			s.rows[idx], _ = rolledBackRow(r)
			if s.rows[idx] == huntPromoted {
				s.inherited++
			}
		}
		s.schedule, s.attempts = scheduleNone, 0
		s.reversed = true
		s.behind = !canary
		if canary {
			s.revert = true
			s.canaryPartition = 0
		}
	}
	if canary {
		next.canaryStep = int32(len(w.cfg.canary))
	}
}

// rollbackModelConfig is one configuration of the pair at a reference
// shape, both sides at 10 percent: one rollback, one pause and two
// partition changes tried at every moment, one failure, and in full mode
// one scale; on gangs one member lost mid-surge and one lost on a standing
// gang.
func rollbackModelConfig(family string, shape [2]int32, es, ds v1beta1.UpdateStrategyType, setting batchModelSetting, gang int32, protocol, full bool) huntConfig {
	cfg := huntConfig{
		members: []huntMember{
			{comp: v1beta1.EngineComponent, desired: shape[0], strategy: es, budget: "10%", gang: gang},
			{comp: v1beta1.DecoderComponent, desired: shape[1], strategy: ds, budget: "10%", gang: gang},
		},
		defaults: setting.defaults, tolerance: setting.tolerance, protocolChange: protocol, maxAttempts: 3,
		fails: 1, parts: 2, rollbacks: 1, pauses: 1, scaleDeltas: []int32{1, -1, 2, -2},
		probes: true, shareBound: true,
	}
	if full {
		cfg.scales = 1
	}
	if gang > 1 {
		cfg.losses, cfg.refills = 1, 1
	}
	cfg.name = family + ": " + cfg.String() + " " + setting.name
	return cfg
}

// rollbackModelCanaryConfig rolls the pair as a canary unit through the
// steps and back through the canary's rollback: no ratio rule, the
// operator's partition inert under the step's partition and under the
// rollback's partition of 0, one pause.
func rollbackModelCanaryConfig(shape [2]int32, es, ds v1beta1.UpdateStrategyType, steps []string, gang int32, protocol bool) huntConfig {
	cfg := rollbackModelConfig("canary", shape, es, ds, batchModelSetting{name: "off"}, gang, protocol, false)
	cfg.canary = steps
	cfg.rollbacks, cfg.canaryRollbacks, cfg.parts, cfg.shareBound = 0, 1, 1, false
	cfg.name = "canary: " + cfg.String()
	return cfg
}

// rollbackModelConfigs lists the configurations: the reference shapes
// under the strategy pairs with the rollback, the pause and the partitions
// tried at every moment under the settings; gangs; the pair as a canary
// unit rolled back by the canary; and in full mode a seeded sample over
// the whole space.
func rollbackModelConfigs(full bool) []huntConfig {
	out := rollbackModelRolledConfigs(full)
	out = append(out, rollbackModelGangConfigs(full)...)
	out = append(out, rollbackModelCanaryConfigs(full)...)
	if full {
		out = append(out, rollbackModelSample()...)
	}
	return out
}

// rollbackModelPairs are the strategy pairs explored: both sides
// RecreatePod or both SurgeThenDrain, and in full mode the mixed pairs.
func rollbackModelPairs(full bool) [][2]v1beta1.UpdateStrategyType {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	pairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}}
	if full {
		pairs = append(pairs, [2]v1beta1.UpdateStrategyType{recreate, surge}, [2]v1beta1.UpdateStrategyType{surge, recreate})
	}
	return pairs
}

// rollbackModelProtocols says whether a pairing-protocol change is tried
// beside each configuration: in full mode, with and without one.
func rollbackModelProtocols(full bool) []bool {
	if full {
		return []bool{false, true}
	}
	return []bool{false}
}

// rollbackModelStepLists are the canary's step capacities explored: two
// steps, and three in full mode.
func rollbackModelStepLists(full bool) [][]string {
	lists := [][]string{{"25%", "100%"}}
	if full {
		lists = append(lists, []string{"10%", "50%", "100%"})
	}
	return lists
}

// rollbackModelRolledConfigs are the reference shapes under the strategy
// pairs with the rollback, the pause and the partitions tried at every
// moment under three settings, every setting in full mode.
func rollbackModelRolledConfigs(full bool) []huntConfig {
	settings := batchModelSettings()
	rolled := settings[:3]
	if full {
		rolled = settings
	}
	var out []huntConfig
	for _, shape := range batchModelShapes {
		for _, pair := range rollbackModelPairs(full) {
			for _, setting := range rolled {
				for _, protocol := range rollbackModelProtocols(full) {
					out = append(out, rollbackModelConfig("rollback-and-pause", shape, pair[0], pair[1], setting, 0, protocol, full))
				}
			}
		}
	}
	return out
}

// rollbackModelGangConfigs are gangs of four and eight pods at two shapes
// under no setting; in full mode gangs of two to eight at every shape
// under no setting and the chart's. The rollback lands at the moments no
// gang is short of a member.
func rollbackModelGangConfigs(full bool) []huntConfig {
	type gangShape struct {
		shape [2]int32
		gang  int32
	}
	settings := batchModelSettings()
	gangShapes := []gangShape{{[2]int32{12, 8}, 8}, {[2]int32{12, 4}, 4}}
	gangSettings := []batchModelSetting{settings[0]}
	if full {
		gangShapes = nil
		for _, shape := range batchModelShapes {
			for _, gang := range []int32{2, 4, 8} {
				gangShapes = append(gangShapes, gangShape{shape, gang})
			}
		}
		gangSettings = []batchModelSetting{settings[0], settings[2]}
	}
	var out []huntConfig
	for _, gs := range gangShapes {
		for _, pair := range rollbackModelPairs(false) {
			for _, setting := range gangSettings {
				for _, protocol := range rollbackModelProtocols(full) {
					out = append(out, rollbackModelConfig("gangs", gs.shape, pair[0], pair[1], setting, gs.gang, protocol, full))
				}
			}
		}
	}
	return out
}

// rollbackModelCanaryConfigs roll the pair as a canary unit, single pods
// at three shapes through two steps; in full mode every shape, three steps
// too, gangs of four and a protocol change beside each.
func rollbackModelCanaryConfigs(full bool) []huntConfig {
	canaryShapes := [][2]int32{{12, 8}, {20, 10}, {12, 4}}
	canaryGangs := []int32{0}
	if full {
		canaryShapes = batchModelShapes
		canaryGangs = []int32{0, 4}
	}
	var out []huntConfig
	for _, shape := range canaryShapes {
		for _, pair := range rollbackModelPairs(false) {
			for _, steps := range rollbackModelStepLists(full) {
				for _, protocol := range rollbackModelProtocols(full) {
					for _, gang := range canaryGangs {
						out = append(out, rollbackModelCanaryConfig(shape, pair[0], pair[1], steps, gang, protocol))
					}
				}
			}
		}
	}
	return out
}

// rollbackModelSample is a seeded sample over the whole space, with up to
// three partition changes among the moves; the roll turns around once per
// path, as in every family.
func rollbackModelSample() []huntConfig {
	settings := batchModelSettings()
	pairs := rollbackModelPairs(true)
	stepLists := rollbackModelStepLists(true)
	rng := rand.New(rand.NewSource(20261109))
	var out []huntConfig
	for n := 0; n < 100; n++ {
		shape := batchModelShapes[rng.Intn(len(batchModelShapes))]
		pair := pairs[rng.Intn(len(pairs))]
		gang := []int32{0, 0, 2, 4, 8}[rng.Intn(5)]
		protocol := rng.Intn(3) == 0
		if rng.Intn(5) == 0 {
			out = append(out, rollbackModelCanaryConfig(shape, pair[0], pair[1], stepLists[rng.Intn(len(stepLists))], gang, protocol))
			continue
		}
		cfg := rollbackModelConfig("sample", shape, pair[0], pair[1], settings[rng.Intn(len(settings))], gang, protocol, true)
		cfg.parts = int8(1 + rng.Intn(3))
		if rng.Intn(4) == 0 {
			cfg.maxAttempts = 1
		}
		out = append(out, cfg)
	}
	return out
}

// rollbackReadings reads one settled state the batch step governs for the
// Instances a rollback is rebuilding: a recreate turned around keeps the
// revision the roll returns to as its running revision, so the batch step
// counts it as serving, and a peer's wait on that side's batch ends while
// nothing of the Instance serves. The reading names the side, the count and
// the batch step's serving figure.
func (w *huntWorld) rollbackReadings(st *huntState, batch batchStep) []huntFinding {
	var out []huntFinding
	for _, bm := range batch.members {
		for i, m := range w.cfg.members {
			if m.comp != bm.component {
				continue
			}
			if rebuilding := st.sides[i].count(huntDrainBack); rebuilding > 0 {
				out = append(out, huntFinding{class: "rebuilt-after-a-rollback-read-as-serving", state: st,
					message: fmt.Sprintf("the %s has %d Instance(s) the rollback is rebuilding with nothing serving, and the batch step counts them among its %d serving of %d movable, so a peer's wait on this side's batch ends %d Instance(s) early",
						m.comp, rebuilding, bm.promoted, bm.movable(), rebuilding)})
			}
		}
	}
	return out
}

// rollbackModelKnown are the batch-step model's known classes and the four
// a rollback adds: right after a rollback each side's batches are planned
// again from the Instances still on the revision the roll returns to, so
// the sides' positions and shares are the previous roll's until the step
// closes the gap.
func rollbackModelKnown() map[string]string {
	known := map[string]string{
		"rebuilt-after-a-rollback-read-as-serving": "a recreate a rollback turned around keeps the revision the roll returns to as its running revision, the reading of the updated counter, so the batch step counts it as serving while its pods are rebuilt and the other side's batch opens up to the per-Component unavailability budget early (held by TestBatchStepRollback_ARecreateTurnedAroundIsReadAsServingWhileItIsRebuilt)",
		"batch-promise-after-a-rollback":           "a rollback plans each side's batches again from the Instances still on the revision the roll returns to, so the sides stand where the previous roll left them, no wider apart than the rollback found them, until the step closes the gap",
		"ratio-promise-after-a-rollback":           "a rollback plans both sides' batches again from the Instances still on the revision the roll returns to, so the sides stand where the previous roll left them, no wider apart than the rollback found them, until the step closes the gap",
		"version-mix-after-a-rollback":             "a rollback turns the roll around: each side's share toward the revision it returns to is what the previous roll had left there, and the step closes the gap from there",
	}
	for class, reading := range batchModelKnown {
		known[class] = reading
	}
	return known
}

// rollbackModelRunner reads the rollback model's classes.
func rollbackModelRunner() huntModelRunner {
	return huntModelRunner{fullEnv: "OME_BATCH_STEP_ROLLBACK_HUNT", onlyEnv: "OME_BATCH_STEP_ROLLBACK_HUNT_ONLY", failing: batchModelFailing, known: rollbackModelKnown()}
}

// TestBatchStepRollback_ReferenceShapesRolledBackAndPausedUnderAPartition
// drives the full gate stack over the reference prefill/decode
// shapes with the operator's rollback, the canary's rollback and a pause
// landing at every moment of the roll beside the operator's partitions:
// no two sides wait on each other, no budget is exceeded, and every batch
// gap and share gap has a known cause.
func TestBatchStepRollback_ReferenceShapesRolledBackAndPausedUnderAPartition(t *testing.T) {
	runner := rollbackModelRunner()
	runner.explore(t, rollbackModelConfigs(runner.full()))
}

// TestBatchStepRollback_ARecreateTurnedAroundIsReadAsServingWhileItIsRebuilt
// records what the batch step reads right after a rollback. 12 prefill
// beside 8 decode, both RecreatePod at 10 percent, no setting. The first
// batch is open: 2 prefill and 1 decode are drained and their pods come up
// on version 2. The operator rolls back to version 1. The three Instances
// are rebuilt on version 1 with nothing serving, and the batch step reads
// the prefill side as 12 of 12 serving version 1 and the decoder as 8 of 8:
// the recreate keeps the revision the Instance last ran as its running
// revision, which is the target again, the reading of the updated counter.
// The explorer reports the picture as
// rebuilt-after-a-rollback-read-as-serving; this test fails the day the
// step reads such an Instance as still coming up.
func TestBatchStepRollback_ARecreateTurnedAroundIsReadAsServingWhileItIsRebuilt(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	cfg := pair([2]int32{12, 8}, recreate, recreate, "10%", GroupDefaults{}, false)
	cfg.fails, cfg.parks, cfg.scales, cfg.rollbacks = 0, 0, 0, 1
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	if st.sides[0].count(huntOpenDrain) != 2 || st.sides[1].count(huntOpenDrain) != 1 {
		t.Fatalf("the first batch must open 2 prefill and 1 decode drains: %s", st)
	}
	st = w.apply(st, huntMove{kind: "rollback"})
	if st.sides[0].count(huntDrainBack) != 2 || st.sides[1].count(huntDrainBack) != 1 {
		t.Fatalf("the rollback must turn the three open drains around: %s", st)
	}
	gate := ResolveGateContextWithDefaults(w.ctx, w.client(st), w.service(st), v1beta1.EngineComponent, w.cfg.defaults)
	gate.TargetRevision = w.rollTargetFor(st, 0)
	batch, err := gate.readBatchStep()
	if err != nil || !batch.governs {
		t.Fatalf("the batch step must govern the pair after the rollback (err %v): %s", err, st)
	}
	for _, bm := range batch.members {
		if bm.promoted != bm.desired || bm.open != 0 {
			t.Fatalf("the batch step must read the %s as %d of %d serving the revision the roll returns to while its drains are rebuilt, got promoted %d open %d: %s",
				bm.component, bm.desired, bm.desired, bm.promoted, bm.open, st)
		}
	}
	found := false
	for _, f := range w.check(st) {
		if f.class == "rebuilt-after-a-rollback-read-as-serving" {
			found = true
			t.Logf("%s", f.message)
		}
	}
	if !found {
		t.Fatalf("the explorer must read the picture as rebuilt-after-a-rollback-read-as-serving: %s", st)
	}
	end := w.playOut(st)
	if !end.complete() {
		t.Fatalf("the roll back must finish once the rebuilt Instances serve: %s", end)
	}
}

// TestBatchStepRollback_TheCanarysRollbackReturnsEveryInstanceWithTheOperatorsPartitionSetAside
// records what the canary's rollback leaves of a prefill/decode unit. 12
// prefill beside 8 decode, both RecreatePod at 10 percent, a canary of two
// steps (25 percent, then all). The first step's capacity serves version 2
// (3 prefill, 2 decode) and the operator has a partition of 2 on the
// prefill side, inert under the step's. The canary rolls back: every
// member's pacing names the stable revision and carries a step partition
// of 0, which the replica's reader takes over the operator's partition as
// it took the step's, so every Instance returns to version 1 while the
// operator's partition of 2 stays written on the prefill side.
func TestBatchStepRollback_TheCanarysRollbackReturnsEveryInstanceWithTheOperatorsPartitionSetAside(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	cfg := rollbackModelCanaryConfig([2]int32{12, 8}, recreate, recreate, []string{"25%", "100%"}, 0, false)
	cfg.fails, cfg.pauses, cfg.probes = 0, 0, false
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	st = serveUntil(t, w, st, func(st *huntState) bool { return st.done() })
	if st.sides[0].count(huntPromoted) != 3 || st.sides[1].count(huntPromoted) != 2 {
		t.Fatalf("the first step must serve 3 prefill and 2 decode on version 2: %s", st)
	}
	st = w.apply(st, huntMove{kind: "partition", side: 0, delta: 2})
	st = w.apply(st, huntMove{kind: "canary-rollback"})
	for i, m := range w.cfg.members {
		ir := w.replica(i, st.sides[i])
		if ir.Spec.Pacing == nil || ir.Spec.Pacing.RollbackToRevision == nil || *ir.Spec.Pacing.RollbackToRevision != w.oldRevision(i) || ir.Spec.Pacing.Partition == nil || *ir.Spec.Pacing.Partition != 0 {
			t.Fatalf("the %s replica must name the stable revision on its pacing with a step partition of 0 after the canary's rollback, got %+v", m.comp, ir.Spec.Pacing)
		}
		if ir.Status.UpdateRevision != batchRevision(m.comp, "target") {
			t.Fatalf("the %s replica's update revision must stay the pushed one under the rollback, got %s", m.comp, ir.Status.UpdateRevision)
		}
		if irprojector.IRPartition(ir) != 0 {
			t.Fatalf("the %s replica's reader must take the step partition of 0 over the operator's, got %d", m.comp, irprojector.IRPartition(ir))
		}
	}
	if st.sides[0].partition != 2 {
		t.Fatalf("the operator's partition of 2 must stay written on the prefill side: %s", st)
	}
	end := w.playOut(st)
	if !end.complete() {
		t.Fatalf("the rollback must return every Instance to version 1, the two above the operator's partition included: %s", end)
	}
}
