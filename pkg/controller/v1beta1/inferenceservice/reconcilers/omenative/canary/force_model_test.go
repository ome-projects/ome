package canary

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// This file walks the disruptive force through sequences of what an
// operator and the cluster do, and checks after every pass the rules the
// force must keep. Each pass runs in the controller's order: the settle
// step, then the replica projection, then the canary pass, then the check
// for a request no group reads. Every sequence of three moves is walked on
// a single engine, with and without a running force; a seeded random walk
// covers longer sequences and a prefill/decode unit.

// forceMove is one thing an operator or the cluster does between passes.
type forceMove struct {
	name  string
	apply func(w *forceWorld)
}

// forceWorld is the service under the walk and what the walk has counted.
type forceWorld struct {
	isvc     *v1beta1.InferenceService
	in       ReconcileInputs
	rec      *record.FakeRecorder
	pd       bool
	runs     int
	starts   map[string]int
	removals map[string]int
	open     int
	trail    []string
	seen     map[string]int
	// stepShare is the weight the step held when the running force started.
	stepShare int32
}

func (w *forceWorld) runID() string { return v1beta1.ActiveRolloutRunID(w.isvc) }

func (w *forceWorld) record() *v1beta1.CanaryStatus {
	return rollout.CanaryStatusFor(&w.isvc.Status, v1beta1.EngineComponent)
}

func (w *forceWorld) keysPresent() bool {
	for _, key := range forceKeys {
		if _, ok := w.isvc.Annotations[key]; ok {
			return true
		}
	}
	return false
}

// oldPods sets the version 1 Instances the engine (and a decoder) keep.
func (w *forceWorld) oldPods(old, kept int32) {
	w.in.PerRevisionPods = map[string]int32{"new": w.in.DesiredReplicas - old, "old": old}
	w.in.StableInstancesKept = i32(kept)
	if d, ok := w.in.Secondaries[v1beta1.DecoderComponent]; ok {
		d.StableInstancesKept = i32(kept)
		w.in.Secondaries[v1beta1.DecoderComponent] = d
	}
}

var forceMoves = []forceMove{
	{"request", func(w *forceWorld) { requestForce(w.isvc, w.runID()) }},
	{"request-other-run", func(w *forceWorld) { requestForce(w.isvc, "other") }},
	{"remove-mode", func(w *forceWorld) { delete(w.isvc.Annotations, constants.RolloutForceModeAnnotation) }},
	{"remove-run", func(w *forceWorld) { delete(w.isvc.Annotations, constants.RolloutForceRunAnnotation) }},
	{"edit-run", func(w *forceWorld) {
		if _, ok := w.isvc.Annotations[constants.RolloutForceRunAnnotation]; ok {
			w.isvc.Annotations[constants.RolloutForceRunAnnotation] = "mistyped"
		}
	}},
	{"pause", func(w *forceWorld) { setAnnotation(w.isvc, constants.PausedRolloutAnnotation, "true") }},
	{"unpause", func(w *forceWorld) { delete(w.isvc.Annotations, constants.PausedRolloutAnnotation) }},
	{"rollback", func(w *forceWorld) { setAnnotation(w.isvc, constants.RolloutRollbackAnnotation, "true") }},
	{"config-off", func(w *forceWorld) { w.in.ForceMaxDuration = 0 }},
	{"config-on", func(w *forceWorld) { w.in.ForceMaxDuration = forceLimit }},
	{"time-limit-passes", func(w *forceWorld) { w.in.Now = w.in.Now.Add(forceLimit) }},
	{"new-run", func(w *forceWorld) {
		w.runs++
		w.isvc.Status.Rollout.ActiveRun.RunID = fmt.Sprintf("run-%d", w.runs+1)
	}},
	{"old-pods-gone", func(w *forceWorld) { w.oldPods(0, 0) }},
	{"old-pods-back", func(w *forceWorld) { w.oldPods(w.in.DesiredReplicas/2, w.in.DesiredReplicas/2) }},
	{"new-pods-ready", func(w *forceWorld) { w.oldPods(0, 0); w.in.PerRevisionPods["new"] = w.in.DesiredReplicas }},
	{"crash", func(w *forceWorld) { w.in.Crash = engineCrash() }},
}

func setAnnotation(isvc *v1beta1.InferenceService, key, value string) {
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	isvc.Annotations[key] = value
}

func newSingleEngineWorld(t *testing.T) *forceWorld {
	isvc, in, rec := forcedFixture(t)
	return &forceWorld{isvc: isvc, in: in, rec: rec, starts: map[string]int{}, removals: map[string]int{}, seen: map[string]int{}}
}

func newPairedWorld(t *testing.T) *forceWorld {
	isvc, in, rec := pdUnitOf(quarterThenAll(), 4, 4, map[string]int32{"new": 1, "old": 3}, 1)
	in.ForceMaxDuration = forceLimit
	mustReconcile(t, isvc, in)
	drain(rec)
	return &forceWorld{isvc: isvc, in: in, rec: rec, pd: true, starts: map[string]int{}, removals: map[string]int{}, seen: map[string]int{}}
}

var (
	forceStartedRe = regexp.MustCompile(EventReasonRolloutForceStarted + `.* run ([^\s:;,]+)`)
	forceEndedRe   = regexp.MustCompile(EventReasonRolloutForceEnded + `.* for run (\S+) ended \((\w+)\)`)
)

// passFacts is what held at the start of a pass, read before it runs.
type passFacts struct {
	paused, configured, crash bool
	runID                     string
	weight                    int32
}

// pass runs one controller pass and checks every rule against it.
func (w *forceWorld) pass(t *testing.T, label string) {
	t.Helper()
	w.in.Now = w.in.Now.Add(time.Minute)
	paused, _ := constants.RolloutPauseState(w.isvc.Annotations)
	facts := passFacts{paused: paused, configured: w.in.ForceMaxDuration > 0, crash: w.in.Crash != nil, runID: w.runID()}
	if cs := w.record(); cs != nil {
		facts.weight = cs.ObservedTrafficWeight
	}
	w.trail = append(w.trail, label)

	SettleForces(w.isvc, rollout.Policies{}, w.rec, w.in.Now, w.in.ForceMaxDuration)
	w.checkProjection(t)
	res := mustReconcile(t, w.isvc, w.in)
	for _, key := range res.Consume {
		delete(w.isvc.Annotations, key)
	}
	for _, key := range RefuseUnclaimedForce(w.isvc, rollout.Policies{}, w.rec) {
		delete(w.isvc.Annotations, key)
	}
	w.in.Crash = nil
	w.checkPass(t, facts, drain(w.rec))
}

func (w *forceWorld) fail(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Fatalf("after %s: %s", strings.Join(w.trail, " → "), fmt.Sprintf(format, args...))
}

// checkProjection holds one rule: the replicas are written with the force's
// partition 0 and 100% budget only while the force is still allowed.
func (w *forceWorld) checkProjection(t *testing.T) {
	t.Helper()
	if UpdateStrategyOverride(w.isvc, rollout.Policies{}, v1beta1.EngineComponent) == nil {
		return
	}
	cs := w.record()
	if !cs.Forced.Running() {
		w.fail(t, "the replica is written with the force's budget, but no force runs")
	}
	in := w.in
	in.RunActive = v1beta1.RolloutRunActive(w.isvc)
	if cause, _ := forceEndCause(in, cs); cause != "" {
		w.fail(t, "the replica is written with the force's budget, but the force must end (%s)", cause)
	}
	if paused, _ := constants.RolloutPauseState(w.isvc.Annotations); paused {
		w.fail(t, "the replica is written with the force's budget on a paused rollout")
	}
}

func (w *forceWorld) checkPass(t *testing.T, facts passFacts, events []string) {
	t.Helper()
	ended := 0
	for _, ev := range events {
		if m := forceStartedRe.FindStringSubmatch(ev); m != nil {
			w.started(t, facts, m[1], ev)
		}
		if strings.Contains(ev, EventReasonRolloutForceRefused) {
			w.seen["refused"]++
		}
		if m := forceEndedRe.FindStringSubmatch(ev); m != nil {
			w.seen["ended "+m[2]]++
			ended++
			w.open--
			if m[2] == string(v1beta1.CanaryForceEndedByRemoval) {
				w.removals[m[1]]++
			}
		}
	}
	cs := w.record()
	running := cs != nil && cs.Forced.Running()
	if ended > 1 {
		w.fail(t, "one force ended %d times in one pass: %q", ended, events)
	}
	if want := 0; running {
		want = 1
		if w.open != want {
			w.fail(t, "%d forces started but not ended, while the record runs", w.open)
		}
	} else if w.open != want {
		w.fail(t, "%d forces started but not ended, while no force runs: %+v", w.open, cs.Forced)
	}
	if running {
		w.checkRunning(t, facts, cs)
	}
	w.checkCrash(t, facts, cs, events)
}

// started counts a force that started and checks its run, the rollout's
// state and how often the run was forced.
func (w *forceWorld) started(t *testing.T, facts passFacts, run, ev string) {
	t.Helper()
	w.starts[run]++
	w.open++
	w.stepShare = facts.weight
	if run != facts.runID {
		w.fail(t, "a force started for run %s while run %s is open", run, facts.runID)
	}
	if facts.paused || !facts.configured {
		w.fail(t, "a force started on a paused or unconfigured rollout: %q", ev)
	}
	if w.starts[run] > 1 {
		w.seen["forced again after a removal"]++
	}
	if w.starts[run] > 1+w.removals[run] {
		w.fail(t, "run %s was forced %d times with %d removals between", run, w.starts[run], w.removals[run])
	}
}

// checkRunning checks what a force still in effect projects, and that its
// rollout is neither paused nor unconfigured.
func (w *forceWorld) checkRunning(t *testing.T, facts passFacts, cs *v1beta1.CanaryStatus) {
	t.Helper()
	if facts.paused || !facts.configured {
		w.fail(t, "a force runs on a paused or unconfigured rollout: %+v", cs.Forced)
	}
	if cs.Forced.RunID != w.runID() {
		w.fail(t, "the running force names run %s, the open run is %s", cs.Forced.RunID, w.runID())
	}
	p, ok := EffectivePartition(w.isvc, rollout.Policies{}, v1beta1.EngineComponent, w.in.DesiredReplicas)
	s := UpdateStrategyOverride(w.isvc, rollout.Policies{}, v1beta1.EngineComponent)
	if !ok || *p != 0 || s == nil || s.Type != v1beta1.UpdateStrategyRecreatePod {
		w.fail(t, "a running force projects partition 0 under RecreatePod, got %v %v %+v", p, ok, s)
	}
	if got := weightOn(w.isvc, v1beta1.EngineComponent, "new"); got != 100 {
		w.fail(t, "a running force sends all traffic to version 2, got %d%%", got)
	}
	if cs.ObservedTrafficWeight != w.stepShare {
		w.fail(t, "a running force keeps the step's recorded share %d, got %d", w.stepShare, cs.ObservedTrafficWeight)
	}
}

// checkCrash checks where the traffic goes on the pass that read a crash.
func (w *forceWorld) checkCrash(t *testing.T, facts passFacts, cs *v1beta1.CanaryStatus, events []string) {
	t.Helper()
	crashed := ""
	for _, ev := range events {
		if strings.Contains(ev, EventReasonCanaryPodCrashed) {
			crashed = ev
		}
	}
	if !facts.crash || crashed == "" {
		return
	}
	newWeight := weightOn(w.isvc, v1beta1.EngineComponent, "new")
	switch {
	case !forcedThisCanary(cs):
		w.seen["crash on an unforced canary"]++
		// The stop records weight 0, as on any canary. The traffic leaves
		// version 2 unless version 2 pods serve and no version 1 pod does:
		// the traffic writer never moves traffic onto a side with no pod.
		pods := w.in.PerRevisionPods
		heldOnServing := pods["old"] == 0 && pods["new"] > 0
		if cs.ObservedTrafficWeight != 0 || !strings.Contains(crashed, "back on") || (!heldOnServing && newWeight != 0) {
			w.fail(t, "a crash on a canary no force drove returns the step's traffic to the stable side, got %d%% on version 2, recorded %d: %q",
				newWeight, cs.ObservedTrafficWeight, crashed)
		}
	case w.in.StableInstancesKept != nil && *w.in.StableInstancesKept == 0:
		w.seen["crash on a forced canary with no version 1 kept"]++
		if newWeight != 100 || strings.Contains(crashed, "back on") {
			w.fail(t, "with no version 1 Instance kept the traffic stays on version 2, got %d%%: %q", newWeight, crashed)
		}
	}
}

// checkAnswered holds one rule after a quiet pass: a request still on the object
// belongs to the force running for it.
func (w *forceWorld) checkAnswered(t *testing.T) {
	t.Helper()
	if !w.keysPresent() {
		return
	}
	cs := w.record()
	if cs == nil || !cs.Forced.Running() || w.isvc.Annotations[constants.RolloutForceRunAnnotation] != cs.Forced.RunID {
		w.fail(t, "a request stays on the object with no force running for it: %v, record %+v", w.isvc.Annotations, cs)
	}
}

// step makes one move, runs the pass that reads it and a quiet pass after.
func (w *forceWorld) step(t *testing.T, m forceMove) {
	t.Helper()
	m.apply(w)
	w.pass(t, m.name)
	w.pass(t, "quiet")
	w.checkAnswered(t)
}

// forceModelCoverage is what a walk must reach for its checks to mean
// anything: every way a force ends, both crash rules, a refusal, and a run
// forced again after a removal.
var forceModelCoverage = []string{
	"ended " + string(v1beta1.CanaryForceCompleted),
	"ended " + string(v1beta1.CanaryForceEndedByCrash),
	"ended " + string(v1beta1.CanaryForceEndedByRollback),
	"ended " + string(v1beta1.CanaryForceEndedByRunClose),
	"ended " + string(v1beta1.CanaryForceEndedByPause),
	"ended " + string(v1beta1.CanaryForceEndedByRemoval),
	"ended " + string(v1beta1.CanaryForceEndedByChange),
	"ended " + string(v1beta1.CanaryForceExpired),
	"crash on an unforced canary",
	"crash on a forced canary with no version 1 kept",
	"refused",
	"forced again after a removal",
}

func expectForceModelCoverage(t *testing.T, seen map[string]int) {
	t.Helper()
	for _, want := range forceModelCoverage {
		if seen[want] == 0 {
			t.Errorf("the walk never reached %q; reached %v", want, seen)
		}
	}
}

func TestForceModel_EveryThreeMoves(t *testing.T) {
	seen := map[string]int{}
	defer expectForceModelCoverage(t, seen)
	for _, running := range []bool{false, true} {
		for _, a := range forceMoves {
			for _, b := range forceMoves {
				for _, c := range forceMoves {
					w := newSingleEngineWorld(t)
					w.seen = seen
					if running {
						w.step(t, forceMoves[0])
					}
					for _, m := range []forceMove{a, b, c} {
						w.step(t, m)
					}
				}
			}
		}
	}
}

func TestForceModel_RandomWalks(t *testing.T) {
	r := rand.New(rand.NewSource(15))
	seen := map[string]int{}
	defer expectForceModelCoverage(t, seen)
	for walk := 0; walk < 400; walk++ {
		w := newSingleEngineWorld(t)
		if walk%2 == 1 {
			w = newPairedWorld(t)
		}
		w.seen = seen
		for i := 0; i < 20; i++ {
			m := forceMoves[r.Intn(len(forceMoves))]
			if r.Intn(3) == 0 {
				m = forceMoves[0]
			}
			w.step(t, m)
		}
	}
}
