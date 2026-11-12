package coordination

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// This file widens the batch-step model of
// pd_pacing_gates_never_wait_on_each_other_test.go, whose world and
// builders it shares, to the reference prefill:decode shapes (12:8,
// 15:15, 16:8, 20:10, 24:8, 30:10 and 12:4) and to what a fleet does to a
// roll the batch step paces: a partition set, raised, lowered or cleared
// on either side or both at every moment of the roll (idle, settling,
// rotating, draining, the last Instance, a side held on its spent
// retries); a scale of one side, of both or of the ratio at the same
// moments; gangs of two to eight pods that lose a member mid-batch,
// whether the gang is old, promoted, surging or the replacement standing
// beside its source; and the pair rolled as a canary unit, each side's
// replica carrying the step's partition while the production gate answers
// with the pairing gate alone. Beside the world's own readings (no two
// sides waiting on each other, no side on itself, the budgets, the batch
// promise, the classifier) it reads the two sides' new-version shares
// against the group's tolerance (shareReadings) and names the cause of a
// gap wider than the tolerance plus one step: a known cause is reported
// with its first picture, a gap with no known cause fails.
//
// OME_BATCH_STEP_HUNT=full widens the configurations and the bounds for
// a long local run; OME_BATCH_STEP_HUNT_ONLY=<text> keeps the configurations
// whose name contains the text.

// shareTolerance is the tolerance the group's shares are read against: the
// group's maintainRatio tolerance, else the operator's default under
// keepRatio, which plans the batches to it (GateContext.batchTolerance);
// nil when neither is set, so no share is read.
func (w *huntWorld) shareTolerance() *int32 {
	if w.cfg.tolerance != nil {
		return w.cfg.tolerance
	}
	if keepRatio, _ := huntRulesOf(w.cfg.defaults); keepRatio || w.cfg.ratioRule {
		return w.cfg.defaults.RatioTolerancePercent
	}
	return nil
}

// shareSide is one pairing side as the share reading sees it: the batch
// step's member and the created Instances the step counts among its starts
// (a scale-up's Instances come up on the new version and read promoted).
type shareSide struct {
	index   int
	member  batchMember
	created int32
}

// ownStarts is the member with the created Instances taken out of its
// starts: what the roll itself has started.
func (s shareSide) ownStarts() batchMember {
	own := s.member
	own.promoted -= s.created
	return own
}

// shareReadings reads the two sides' new-version shares at one state the
// batch step governs. A side's share is the Instances started toward the
// new version, as the step counts them, over the Instances it may move.
// Started, not serving, is what the step plans; the serving mix lags it by
// the attempts open. One open batch leaves the two shares at most the
// tolerance plus one step of either side apart. A wider gap is read by its
// cause, in this order: maintainRatio alone, under which the step pairs
// batch numbers and plans no shares at all; a scale-up, whose created
// Instances widen the movable count at once and count as started once they
// serve; a scale-down that retired old Instances; a partition change that moved
// the movable count under the shares; a side whose retries are spent and
// which the other side runs one batch past; a protocol change, where the
// pairing waiver lets a side run ahead; else ratio-excess. A side whose status predates the push reads nothing
// started, so the state is skipped. Apart from the gap, a side whose
// created Instances move the step's reading of its next batch is reported
// as progress-from-a-scale-up.
func (w *huntWorld) shareReadings(st *huntState, batch batchStep) []huntFinding {
	tolerance := w.shareTolerance()
	if tolerance == nil {
		return nil
	}
	members := map[v1beta1.ComponentType]batchMember{}
	for _, bm := range batch.members {
		members[bm.component] = bm
	}
	eng, dec := w.pairIndex()
	var sides []shareSide
	for _, i := range []int{eng, dec} {
		s := st.sides[i]
		bm, ok := members[w.cfg.members[i].comp]
		if s.behind || !ok || bm.movable() <= 0 {
			return nil
		}
		var created int32
		for idx := int32(len(s.rows)) - s.added; idx < int32(len(s.rows)); idx++ {
			if idx >= 0 && s.rows[idx].promotedRow() {
				created++
			}
		}
		sides = append(sides, shareSide{index: i, member: bm, created: created})
	}
	var out []huntFinding
	add := func(class, format string, args ...any) {
		out = append(out, huntFinding{class: class, message: fmt.Sprintf(format, args...), state: st})
	}
	for _, s := range sides {
		if s.created == 0 {
			continue
		}
		if own := s.ownStarts(); own.nextBatch() != s.member.nextBatch() {
			add("progress-from-a-scale-up", "the %s's %d Instances a scale-up created serve on the new version and the batch step reads them as starts: its next start belongs to batch %d, not batch %d (%d started by the roll of %d movable)",
				s.member.component, s.created, s.member.nextBatch(), own.nextBatch(), own.started(), own.movable())
		}
	}
	e, d := sides[0], sides[1]
	movableE, movableD := int64(e.member.movable()), int64(d.member.movable())
	bound := int64(*tolerance)*movableE*movableD + max64(int64(e.member.step)*movableD, int64(d.member.step)*movableE)*100
	gap := shareGap(e.member.started(), d.member.started(), movableE, movableD)
	if gap <= bound {
		return out
	}
	keepRatio, _ := huntRulesOf(w.cfg.defaults)
	se, sd := st.sides[e.index], st.sides[d.index]
	class := "ratio-excess"
	switch {
	case st.reversed():
		// A rollback turns the roll around: each side's share toward the
		// revision it returns to is what the previous roll had left there.
		class = "version-mix-after-a-rollback"
	case !keepRatio:
		// maintainRatio alone: the step pairs batch numbers and plans no
		// shares, so every gap is this reading's.
		class = "version-mix-by-batch-number"
	case se.rebuilt > 0 || sd.rebuilt > 0:
		// A gang the roll rebuilt with no gate asked is a start the plan
		// never made.
		class = "version-mix-after-a-rebuild"
	case se.followed > 0 || sd.followed > 0:
		// The coordinator's follower write is a scale the operator did not
		// make, one service pass after the anchor's.
		class = "version-mix-from-a-follower-scale"
	case se.added > 0 || sd.added > 0:
		// Created Instances widen the movable count at once and count as
		// started once they serve; either way the gap is the scale-up's.
		class = "version-mix-from-a-scale-up"
	case se.removed > 0 || sd.removed > 0:
		class = "version-mix-from-a-scale-down"
	case se.parts < w.cfg.parts || sd.parts < w.cfg.parts:
		class = "version-mix-after-a-partition-change"
	case se.schedule == scheduleHeld || sd.schedule == scheduleHeld:
		class = "version-mix-beside-a-held-side"
	case w.transition(st):
		class = "ratio-excess-in-protocol-change"
	}
	add(class, "the engine has started %d of %d movable (%d percent) and the decoder %d of %d (%d percent): %d points apart, more than the tolerance of %d plus one step (%d and %d)",
		e.member.started(), movableE, int64(e.member.started())*100/movableE, d.member.started(), movableD, int64(d.member.started())*100/movableD,
		gap/(movableE*movableD), *tolerance, e.member.step, d.member.step)
	return out
}

// shareGap is the gap between two shares, startedE over movableE and
// startedD over movableD, scaled by 100 times both movable counts so a
// whole-percent tolerance compares in integers (batch_plan.go:scaledSkew).
func shareGap(startedE, startedD int32, movableE, movableD int64) int64 {
	gap := (int64(startedE)*movableD - int64(startedD)*movableE) * 100
	if gap < 0 {
		gap = -gap
	}
	return gap
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// batchModelShapes are the reference prefill:decode shapes.
var batchModelShapes = [][2]int32{{12, 8}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}}

// batchModelSetting is one operator setting of the pair: the group's
// maintainRatio tolerance and the prefill/decode rules the chart ships.
type batchModelSetting struct {
	name      string
	defaults  GroupDefaults
	tolerance *int32
}

// batchModelSettings are the settings explored: none; the ratio rule at
// the tolerance the model uses; the chart's (keepRatio at the
// default tolerance of 5, the decoder first); keepRatio alone; the
// decoder first alone; the ratio rule beside the chart's.
func batchModelSettings() []batchModelSetting {
	dec := v1beta1.DecoderComponent
	twenty := ptr.To(int32(20))
	return []batchModelSetting{
		{name: "off"},
		{name: "maintainRatio=20", tolerance: twenty},
		{name: "chart", defaults: huntRules(true, ptr.To(int32(5)), dec)},
		{name: "keepRatio=5", defaults: huntRules(true, ptr.To(int32(5)), "")},
		{name: "firstSide=decoder", defaults: huntRules(false, nil, dec)},
		{name: "maintainRatio=20/chart", defaults: huntRules(true, ptr.To(int32(5)), dec), tolerance: twenty},
	}
}

// batchModelConfig is one configuration of the pair at a reference
// shape, both sides at 10 percent, with the operator's moves tried at
// every moment and the shares read: two partition changes, one scale (by
// one or by half, up or down, on one side, both or the ratio), two
// failures, one attempt whose retries stopped, and on gangs one member
// lost mid-surge and one lost on a standing gang, per side.
func batchModelConfig(family string, shape [2]int32, es, ds v1beta1.UpdateStrategyType, setting batchModelSetting, gang int32, protocol bool) huntConfig {
	cfg := huntConfig{
		members: []huntMember{
			{comp: v1beta1.EngineComponent, desired: shape[0], strategy: es, budget: "10%", gang: gang},
			{comp: v1beta1.DecoderComponent, desired: shape[1], strategy: ds, budget: "10%", gang: gang},
		},
		defaults: setting.defaults, tolerance: setting.tolerance, protocolChange: protocol, maxAttempts: 3,
		fails: 2, scales: 1, parts: 2, losses: 1, refills: 1, scaleDeltas: []int32{1, -1, 2, -2},
		probes: true, shareBound: true,
	}
	cfg.name = family + ": " + cfg.String() + " " + setting.name
	return cfg
}

// batchModelCanaryConfig rolls the pair as a canary unit through the given
// step capacities: no ratio rule (admission refuses one on a canary group),
// the operator's partition inert under the step's, one scale and one
// partition change per side among the moves.
func batchModelCanaryConfig(shape [2]int32, es, ds v1beta1.UpdateStrategyType, steps []string, gang int32, protocol bool) huntConfig {
	cfg := batchModelConfig("canary", shape, es, ds, batchModelSetting{name: "off"}, gang, protocol)
	cfg.canary = steps
	cfg.parts, cfg.shareBound = 1, false
	cfg.name = "canary: " + cfg.String()
	return cfg
}

// batchModelConfigs lists the configurations: the reference shapes under
// both strategies with the operator's partitions and scales tried at
// every moment under the settings; gangs of two to eight pods losing a
// member mid-batch; the pair as a canary unit at two steps; and in full
// mode every setting, the mixed strategy pairs, a protocol change beside
// each, three canary steps and a seeded sample over the whole space.
func batchModelConfigs(full bool) []huntConfig {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	settings := batchModelSettings()
	pairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}}
	if full {
		pairs = append(pairs, [2]v1beta1.UpdateStrategyType{recreate, surge}, [2]v1beta1.UpdateStrategyType{surge, recreate})
	}
	var out []huntConfig
	// The partitions and scales at every moment, single pods.
	rolled := settings[:3]
	if full {
		rolled = settings
	}
	for _, shape := range batchModelShapes {
		for _, pair := range pairs {
			for _, setting := range rolled {
				out = append(out, batchModelConfig("partitions-and-scales", shape, pair[0], pair[1], setting, 0, false))
				if full {
					out = append(out, batchModelConfig("partitions-and-scales", shape, pair[0], pair[1], setting, 0, true))
				}
			}
		}
	}
	// Gangs losing a member mid-batch.
	type gangShape struct {
		shape [2]int32
		gang  int32
	}
	gangShapes := []gangShape{{[2]int32{12, 8}, 8}, {[2]int32{12, 4}, 4}, {[2]int32{15, 15}, 2}, {[2]int32{16, 8}, 8}}
	gangSettings := []batchModelSetting{settings[0], settings[2]}
	protocols := []bool{false}
	if full {
		gangShapes = nil
		for _, shape := range batchModelShapes {
			for _, gang := range []int32{2, 4, 8} {
				gangShapes = append(gangShapes, gangShape{shape, gang})
			}
		}
		protocols = []bool{false, true}
	}
	for _, gs := range gangShapes {
		for _, pair := range pairs[:2] {
			for _, setting := range gangSettings {
				for _, protocol := range protocols {
					out = append(out, batchModelConfig("gangs", gs.shape, pair[0], pair[1], setting, gs.gang, protocol))
				}
			}
		}
	}
	// The pair as a canary unit.
	canaryShapes := [][2]int32{{12, 8}, {20, 10}, {12, 4}}
	stepLists := [][]string{{"25%", "100%"}}
	if full {
		canaryShapes = batchModelShapes
		stepLists = append(stepLists, []string{"10%", "50%", "100%"})
	}
	for _, shape := range canaryShapes {
		for _, pair := range pairs[:2] {
			for _, steps := range stepLists {
				for _, protocol := range []bool{false, true} {
					out = append(out, batchModelCanaryConfig(shape, pair[0], pair[1], steps, 0, protocol))
					if full {
						out = append(out, batchModelCanaryConfig(shape, pair[0], pair[1], steps, 4, protocol))
					}
				}
			}
		}
	}
	if !full {
		return out
	}
	// A seeded sample over the whole space.
	rng := rand.New(rand.NewSource(20261102))
	allPairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}, {recreate, surge}, {surge, recreate}}
	for n := 0; n < 150; n++ {
		shape := batchModelShapes[rng.Intn(len(batchModelShapes))]
		pair := allPairs[rng.Intn(len(allPairs))]
		gang := []int32{0, 0, 2, 4, 8}[rng.Intn(5)]
		protocol := rng.Intn(3) == 0
		if rng.Intn(5) == 0 {
			steps := stepLists[rng.Intn(len(stepLists))]
			out = append(out, batchModelCanaryConfig(shape, pair[0], pair[1], steps, gang, protocol))
			continue
		}
		cfg := batchModelConfig("sample", shape, pair[0], pair[1], settings[rng.Intn(len(settings))], gang, protocol)
		cfg.parts, cfg.scales = int8(1+rng.Intn(3)), int8(1+rng.Intn(2))
		if rng.Intn(4) == 0 {
			cfg.maxAttempts = 1
		}
		out = append(out, cfg)
	}
	return out
}

// batchModelFailing are the classes that name a bug: a hold with no way
// out or two gates waiting on each other; a budget exceeded; a share gap
// with no known cause; the step's reading of the rows disagreeing with
// the model's; a side more than one batch ahead of a peer that waits on
// its own pod; a start before the named side; a replacement stranded
// without a partner; a wait on a partition's held Instances; a read the
// gate could not make; a run cut short by the depth bound.
var batchModelFailing = map[string]bool{
	"no-way-out": true, "budget": true, "ratio-excess": true, "classifier": true, "batch-promise": true, "ratio-promise": true,
	"runs-ahead-of-a-peer-coming-up": true, "starts-before-the-named-side": true, "stranded-replacement": true,
	"waits-on-held-instances": true, "read": true, "depth": true,
}

// batchModelKnown are the classes that record a known reading of the current
// engine: each names what the operator sees and the test or decision that
// holds it. A finding of any other class fails as a reading to classify.
var batchModelKnown = map[string]string{
	"progress-from-a-scale-up":                        "the batch step counts a scaled-up side's created Instances as starts, so the side's old Instances roll later than planned",
	"version-mix-by-batch-number":                     "under maintainRatio alone the step pairs batch numbers and plans no shares, so sides whose steps differ in share drift apart with no hold",
	"version-mix-from-a-scale-up":                     "a scale-up widens the movable count at once and builds its Instances on the new version with no gate asked, so the scaled side's share drops and then jumps",
	"version-mix-from-a-scale-down":                   "a scale-down retires the highest indices whatever version they run, so the shares jump apart and close as the roll goes on",
	"version-mix-after-a-partition-change":            "a partition change moves the movable count under the shares; the roll closes the gap from there",
	"version-mix-beside-a-held-side":                  "a side whose retries are spent is run one batch past by the other side",
	"ratio-excess-in-protocol-change":                 "during a protocol change the pairing waiver lets a side run ahead of a peer the pairing gate holds still, by design",
	"batch-promise-after-a-rebuild":                   "a gang short of a worker on the old version is rebuilt onto the target by the roll with no gate asked, a start the batch plan never made, so its side runs a batch ahead (held by TestBatchStepEvents_AGangShortOfAWorkerIsRebuiltByTheRollWithNoGateAsked and TestReconcile_GangShortOfAWorker_RollRebuildsItWhileTheGateHoldsEveryStart)",
	"ratio-promise-after-a-rebuild":                   "a gang the roll rebuilt with no gate asked moves its side a batch ahead of the planned batches (TestBatchStepEvents_AGangShortOfAWorkerIsRebuiltByTheRollWithNoGateAsked)",
	"version-mix-after-a-rebuild":                     "a gang the roll rebuilt with no gate asked counts as a start, so the shares drift by one Instance (TestBatchStepEvents_AGangShortOfAWorkerIsRebuiltByTheRollWithNoGateAsked)",
	"batch-promise-after-scale":                       "a scale re-plans the batches from the new counts",
	"batch-promise-under-partition":                   "a partition ends one side early",
	"batch-promise-in-protocol-change":                "the pairing waiver lets a side run ahead by design",
	"ratio-promise-after-scale":                       "a scale re-plans the planned batches from the new counts",
	"ratio-promise-under-partition":                   "a partition ends one side early under the planned batches",
	"ratio-promise-in-protocol-change":                "the pairing waiver lets a side run ahead of the planned batches by design",
	"budget-after-scale":                              "attempts open above a budget a scale-down lowered finish before anything more opens",
	"waits-on-held-peer":                              "where a spent retry schedule leaves the other side is an open question",
	"held":                                            "every side with Instances left is held on its spent retries, or with the named side's Held record",
	"partition-paced":                                 "a hold the operator's partition explains: clearing it lets the roll finish",
	"ratio-rule":                                      "the serving band or the lockstep holds a side where the batch step does not govern, with its own known holds",
	"ratio-rule-waits-on-the-pairing-gate":            "during a protocol change the lockstep holds a side for running ahead of a peer the pairing gates hold still for that side's old Instances",
	"stranded-replacement-after-scale":                "a side scaled up builds its new Instances on the new cohort with no old pod to keep",
	"starts-before-the-named-side-in-protocol-change": "the pairing waiver lets the other side go first by design",
}

// batchModelRunner reads the batch-step model's classes.
func batchModelRunner() huntModelRunner {
	return huntModelRunner{fullEnv: "OME_BATCH_STEP_HUNT", onlyEnv: "OME_BATCH_STEP_HUNT_ONLY", failing: batchModelFailing, known: batchModelKnown}
}

// named reports whether a configuration's name contains one of the texts
// of the ONLY variable; an unset variable names every configuration.
func (r huntModelRunner) named(name string, only []string) bool {
	for _, text := range only {
		if text == "" || strings.Contains(name, text) {
			return true
		}
	}
	return false
}

// huntModelRunner explores a model's configurations several at a time
// (four, sixteen in full mode) and reads each class it finds as a bug
// (failing), a known reading of the current engine or a reading still to classify, which fails.
// The environment variable fullEnv set to "full" widens the bounds and
// onlyEnv keeps the configurations whose name contains its value.
type huntModelRunner struct {
	fullEnv, onlyEnv string
	failing          map[string]bool
	known            map[string]string
}

func (r huntModelRunner) full() bool { return os.Getenv(r.fullEnv) == "full" }

// fails reports a class the runner fails a configuration on: one that
// names a bug, or one it does not know.
func (r huntModelRunner) fails(class string) bool {
	_, known := r.known[class]
	return r.failing[class] || !known
}

// reading is how the runner reads a class in the summary.
func (r huntModelRunner) reading(class string) string {
	switch {
	case r.failing[class]:
		return "FAILS"
	case r.known[class] != "":
		return r.known[class]
	}
	return "UNKNOWN CLASS, fails until classified"
}

// huntModelExample is the first picture of one class in one configuration.
type huntModelExample struct {
	cfg     huntConfig
	finding huntFinding
}

// huntModelSummary gathers what every configuration found.
type huntModelSummary struct {
	runner                              huntModelRunner
	mu                                  sync.Mutex
	configs, states, probes, runs, gate int
	totals                              map[string]int // states per class
	seen                                map[string]int // configurations per class
	examples                            map[string][]huntModelExample
}

func (s *huntModelSummary) add(cfg huntConfig, w *huntWorld, rep huntReport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs++
	s.states += rep.states
	s.probes += rep.probes
	s.runs += rep.runs
	s.gate += w.gateCalls
	for class, n := range rep.counts {
		s.totals[class] += n
		s.seen[class]++
	}
	for class, f := range rep.findings {
		if len(s.examples[class]) < 6 {
			s.examples[class] = append(s.examples[class], huntModelExample{cfg: cfg, finding: f})
		}
	}
}

// log prints the counts and, per class, how it is read and its first
// pictures.
func (s *huntModelSummary) log(t *testing.T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Logf("%d configurations, %d states, %d probes, %d random runs, %d gate questions", s.configs, s.states, s.probes, s.runs, s.gate)
	classes := make([]string, 0, len(s.totals))
	for class := range s.totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	for _, class := range classes {
		t.Logf("%s: %d states across %d configurations: %s", class, s.totals[class], s.seen[class], s.runner.reading(class))
		for _, ex := range s.examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg.name, ex.finding.message, huntPath(ex.finding.state), ex.finding.state)
		}
	}
}

// explore explores every configuration, several at a time, and fails a
// configuration on a class the runner fails. The summary is logged once
// every configuration has run.
func (r huntModelRunner) explore(t *testing.T, configs []huntConfig) {
	t.Helper()
	only := strings.Split(os.Getenv(r.onlyEnv), ",")
	maxStates, maxDepth, runs, workers := 30, 40, 2, 4
	if r.full() {
		maxStates, maxDepth, runs, workers = 80, 80, 3, 16
	}
	admission := make(chan struct{}, workers)
	summary := &huntModelSummary{runner: r, totals: map[string]int{}, seen: map[string]int{}, examples: map[string][]huntModelExample{}}
	t.Cleanup(func() { summary.log(t) })
	for _, cfg := range configs {
		if !r.named(cfg.name, only) {
			continue
		}
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()
			admission <- struct{}{}
			defer func() { <-admission }()
			w := newHuntWorld(t, cfg)
			rep := w.explore(maxStates, maxDepth)
			w.randomRuns(&rep, 1, runs, 4*maxDepth)
			summary.add(cfg, w, rep)
			classes := make([]string, 0, len(rep.counts))
			for class := range rep.counts {
				classes = append(classes, class)
			}
			sort.Strings(classes)
			var parts []string
			for _, class := range classes {
				parts = append(parts, fmt.Sprintf("%s=%d", class, rep.counts[class]))
				if r.fails(class) {
					f := rep.findings[class]
					t.Errorf("%s: %d states\n    %s\n    path: %s\n    state: %s", class, rep.counts[class], f.message, huntPath(f.state), f.state)
				}
			}
			t.Logf("%d states, depth %d, %d probes, %d gate questions: %s", rep.states, rep.depth, rep.probes, w.gateCalls, strings.Join(parts, " "))
		})
	}
}

// TestBatchStep_ReferenceShapesUnderPartitionsScalesAndGangs drives the
// full gate stack over the reference prefill/decode shapes with the
// operator's partitions and scales at every moment of the roll, gangs of
// two to eight pods losing a member mid-batch, and the pair as a canary
// unit: no two sides wait on each other, no budget is exceeded, and every
// share gap wider than the tolerance plus one step has a known cause.
func TestBatchStep_ReferenceShapesUnderPartitionsScalesAndGangs(t *testing.T) {
	runner := batchModelRunner()
	runner.explore(t, batchModelConfigs(runner.full()))
}

// serveUntil plays progress moves until the condition holds.
func serveUntil(t *testing.T, w *huntWorld, st *huntState, until func(*huntState) bool) *huntState {
	t.Helper()
	for step := 0; !until(st); step++ {
		mv, ok := w.nextProgress(st, step)
		if !ok || step > 400 {
			t.Fatalf("the roll stopped before the condition held: %s", st)
		}
		st = w.apply(st, mv)
	}
	return st
}

// TestBatchStep_AScaledUpSidesCreatedInstancesAreReadAsBatchesAlreadyStarted
// records what the batch step does today when a side is scaled up mid-roll.
// 12 prefill beside 8 decode, both SurgeThenDrain at 10 percent, no
// setting but the ratio rule at a tolerance of 20. The first batch serves
// (2 prefill and 1 decode on the new version) and the second opens (2
// prefill surges, 1 decode surge). The operator scales the prefill side to
// 16 and the four created Instances come up on the new version, as do the
// two open prefill surges. The batch step reads the prefill side at 8
// started of 16, so its next start belongs to batch 5 and waits for
// decoder batch 4 (1 of 4 serving), while the roll itself has started 4
// prefill and would be at batch 3. The
// prefill side's old Instances stand until the decoder has rolled 4, then
// the roll goes on to its end. The explorer reports the picture as
// progress-from-a-scale-up; this test fails the day created Instances stop
// counting as the roll's progress.
func TestBatchStep_AScaledUpSidesCreatedInstancesAreReadAsBatchesAlreadyStarted(t *testing.T) {
	surge := v1beta1.UpdateStrategySurgeThenDrain
	cfg := pair([2]int32{12, 8}, surge, surge, "10%", GroupDefaults{}, false)
	cfg.fails, cfg.scales, cfg.shareBound = 0, 1, true
	cfg.tolerance = ptr.To(int32(20))
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	st = serveUntil(t, w, st, func(st *huntState) bool {
		return st.sides[0].count(huntPromoted) >= 2 && st.sides[1].count(huntPromoted) >= 1
	})
	if st.sides[0].count(huntOpenSurge) != 2 || st.sides[1].count(huntOpenSurge) != 1 {
		t.Fatalf("the second batch must be open when the operator scales: %s", st)
	}
	st = w.apply(st, huntMove{kind: "scale", side: 0, delta: 4})
	// The created Instances and the open prefill surges serve; the decoder's
	// open surge stands, so the batch step's answer is what holds the
	// prefill side's next start.
	for {
		coming := -1
		for idx, r := range st.sides[0].rows {
			if r == huntCreating || r == huntOpenSurge {
				coming = idx
				break
			}
		}
		if coming < 0 {
			break
		}
		st = w.apply(st, huntMove{kind: "serve", side: 0, idx: coming})
	}
	want := "engine batch 5 waits for decoder batch 4 to serve (1 of 4 serving)"
	if h := st.holds[0]; h == nil || h.gate != v1beta1.RolloutHoldGateRatio || h.reason != want {
		t.Fatalf("after the scale-up the engine's hold = %s, want Ratio %q (created Instances read as starts); state %s", h, want, st)
	}
	found := false
	for _, f := range w.check(st) {
		if f.class == "progress-from-a-scale-up" {
			found = true
			t.Logf("%s", f.message)
		}
	}
	if !found {
		t.Fatalf("the explorer must read the picture as progress-from-a-scale-up: %s", st)
	}
	end := w.playOut(st)
	if !end.complete() {
		t.Fatalf("the roll must finish after the scale-up: %s", end)
	}
}

// TestBatchStep_UnderMaintainRatioAloneADecoderScaledToElevenOutrunsTheEngine
// records that under maintainRatio alone the batch step pairs batch
// numbers, not shares. 20 prefill beside 10 decode, both RecreatePod at
// 10 percent, the ratio rule at a tolerance of 20. The first batch is
// open (2 prefill, 1 decode) when the operator scales the decoder to 11,
// so the decoder's step becomes 2 of 11 (10 percent rounded up) against
// the prefill side's 2 of 20. Each batch moves 10 percent of one side
// and 18 percent of the other, no gate reads the shares, and the decoder
// finishes its roll while the prefill side has started at most 12 of 20:
// more than 40 points apart with no hold on the way. The explorer reports
// the picture as version-mix-by-batch-number; this test fails the day the
// step plans the two sides' batches to the tolerance under maintainRatio.
func TestBatchStep_UnderMaintainRatioAloneADecoderScaledToElevenOutrunsTheEngine(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	cfg := pair([2]int32{20, 10}, recreate, recreate, "10%", GroupDefaults{}, false)
	cfg.tolerance = ptr.To(int32(20))
	cfg.fails, cfg.scales, cfg.shareBound = 0, 1, true
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	if st.sides[0].count(huntOpenDrain) != 2 || st.sides[1].count(huntOpenDrain) != 1 {
		t.Fatalf("the first batch must open 2 prefill and 1 decode: %s", st)
	}
	st = w.apply(st, huntMove{kind: "scale", side: 1, delta: 1})
	var atDecoderEnd *huntState
	for step := 0; !st.complete() && step < 400; step++ {
		if atDecoderEnd == nil && st.sides[1].complete() {
			atDecoderEnd = st
		}
		mv, ok := w.nextProgress(st, step)
		if !ok {
			t.Fatalf("the roll stopped: %s", st)
		}
		st = w.apply(st, mv)
	}
	if !st.complete() {
		t.Fatalf("the roll must finish: %s", st)
	}
	if atDecoderEnd == nil {
		t.Fatalf("the decoder never finished before the engine: %s", st)
	}
	engine := atDecoderEnd.sides[0]
	started := engine.count(huntPromoted, huntOpenDrain)
	if started > 12 {
		t.Fatalf("when the decoder finished the engine had started %d of 20; the batch numbers no longer pace the two sides apart: %s", started, atDecoderEnd)
	}
	found := false
	for _, f := range w.check(atDecoderEnd) {
		if f.class == "version-mix-by-batch-number" {
			found = true
			t.Logf("%s", f.message)
		}
	}
	if !found {
		t.Fatalf("the explorer must read the picture as version-mix-by-batch-number: %s", atDecoderEnd)
	}
	for _, h := range atDecoderEnd.holds {
		if h != nil && h.gate == v1beta1.RolloutHoldGateRatio && strings.Contains(h.reason, "skew") {
			t.Fatalf("no gate should have read the shares under the batch step, got %s", h)
		}
	}
}

// TestBatchStep_APromotedGangShortOfAWorkerStillSettlesItsBatch records how
// the batch step reads a gang the repair is refilling. 12 prefill beside 8
// decode in gangs of 8 pods, both RecreatePod at 10 percent, no setting.
// The first batch served (2 prefill gangs, 1 decode gang) and one of the two
// new prefill gangs lost a worker: its leader serves, the replica's serving
// count leaves it out, and the repair refills it. The batch step reads the
// gang by its revision, so prefill batch 1 still counts as served and the
// decoder's second batch opens while 7 of the gang's 8 pods stand; a step
// that read the gang by its pods would hold the decoder on "decoder batch 2
// waits for engine batch 1 to serve (1 of 2 serving)". The pairing gate
// reads the gang the same way, by its serving leader. A gang short of a
// worker on the old version is not offered a start until the refill lands.
// This test fails the day a gang short of a worker stops settling its batch.
func TestBatchStep_APromotedGangShortOfAWorkerStillSettlesItsBatch(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	recreate := v1beta1.UpdateStrategyRecreatePod
	cfg := gangs(pair([2]int32{12, 8}, recreate, recreate, "10%", GroupDefaults{}, false), 8, 8)
	cfg.fails, cfg.scales, cfg.refills = 0, 0, 1
	w := newHuntWorld(t, cfg)
	st := w.picture(map[v1beta1.ComponentType][]huntRow{
		eng: append([]huntRow{huntPromotedShort, huntPromoted}, rows(huntOld, 10)...),
		dec: append([]huntRow{huntPromoted}, rows(huntOld, 7)...),
	}, nil)
	if st.sides[1].count(huntOpenDrain) != 1 {
		t.Fatalf("the decoder's second batch must open while prefill gang 0 is short of a worker (the step reads the gang by its revision): %s", st)
	}
	if st.sides[0].count(huntOpenDrain) != 2 || st.sides[0].rows[0] != huntPromotedShort {
		t.Fatalf("the prefill side must open its second batch beside the short gang, which is not offered: %s", st)
	}
	end := w.playOut(st)
	if !end.complete() {
		t.Fatalf("the roll must finish once the repair refills the gang: %s", end)
	}
}
