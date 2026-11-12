package canary

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/scaling"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// This file widens the canary step model of canary_steps_always_reach_an_end_test.go
// to the reference shapes and to the moves a fleet meets: a node lost
// under one Instance of every member, a gang short of one member, a resize
// of one side, of both sides or of the ratio, a decoder whose count follows
// the engine's, and a metrics source that breaches or answers nothing. It
// also reads the hold text of every quiet state: a state that cannot move on
// its own must say why in what the operator reads (the phase, the record,
// the plan conditions, the pause verb or an event of the pass), and a state
// whose way out is not named is a finding beside a state with no way out.
// Every adverse move is also made once at every moment of the straight run,
// with the run then played out to its end.

// gateSample is the standing answer of the metrics source: healthy, a
// breach on every sample, or nothing readable. reads counts the samples that
// read a standing breach or gap, so the world mends the source only once the
// gate has seen it.
type gateSample struct {
	failing, gap bool
	reads        int8
}

func (s gateSample) String() string {
	switch {
	case s.failing:
		return fmt.Sprintf("failing(%d)", s.reads)
	case s.gap:
		return fmt.Sprintf("gap(%d)", s.reads)
	}
	return "pass"
}

// gatePrometheusAddress is the bundled source the analysis steps query; the
// model answers the queries itself.
const gatePrometheusAddress = "http://prometheus.example.com:9090"

// gateSampler answers the analysis gate from the state's standing answer,
// dated by the state's clock at whole seconds, as the manager's sampler
// dates a landed query.
type gateSampler struct {
	st       *gateState
	answered bool
}

func (s *gateSampler) Get(_ SampleRequest, since time.Time) (analysis.Result, time.Time, bool) {
	at := s.st.now.Truncate(time.Second)
	if !at.After(since) {
		return analysis.Result{}, time.Time{}, false
	}
	s.answered = true
	metric := analysis.MetricResult{Name: "error-rate", Threshold: "0.1", Operator: v1beta1.ComparisonLTE}
	switch {
	case s.st.sample.failing:
		metric.Value, metric.Message = "0.9", "0.9 is above 0.1"
		return analysis.Result{Outcome: analysis.Fail, Metrics: []analysis.MetricResult{metric}}, at, true
	case s.st.sample.gap:
		metric.Message = "no data"
		return analysis.Result{Outcome: analysis.Inconclusive, Metrics: []analysis.MetricResult{metric}}, at, true
	}
	metric.Value, metric.Passed = "0.0", true
	return analysis.Result{Outcome: analysis.Pass, Metrics: []analysis.MetricResult{metric}}, at, true
}

// gateEvents keeps the events a pass records: the text an operator reads
// beside the status, which the hold reading quotes.
type gateEvents struct{ messages []string }

func (e *gateEvents) Event(_ runtime.Object, eventType, reason, message string) {
	e.messages = append(e.messages, eventType+" "+reason+" "+message)
}

func (e *gateEvents) Eventf(_ runtime.Object, eventType, reason, format string, args ...interface{}) {
	e.messages = append(e.messages, eventType+" "+reason+" "+fmt.Sprintf(format, args...))
}

func (e *gateEvents) AnnotatedEventf(obj runtime.Object, _ map[string]string, eventType, reason, format string, args ...interface{}) {
	e.Eventf(obj, eventType, reason, format, args...)
}

// gateBake is the time an analysis step serves before a passing sample may
// advance it.
func gateBake() *v1beta1.RolloutPause {
	return &v1beta1.RolloutPause{Duration: &metav1.Duration{Duration: 5 * time.Minute}}
}

// gateAnalysis is one metric read every minute after a minute's warm-up;
// two breaches roll the step back.
func gateAnalysis(policy *v1beta1.OnInconclusive) *v1beta1.RolloutAnalysis {
	return &v1beta1.RolloutAnalysis{
		Interval:       metav1.Duration{Duration: time.Minute},
		InitialDelay:   &metav1.Duration{Duration: time.Minute},
		FailureLimit:   2,
		OnInconclusive: policy,
		Metrics:        []v1beta1.AnalysisMetric{{Name: "error-rate", Query: "rate(errors[1m])", Operator: v1beta1.ComparisonLTE, Threshold: "0.1"}},
	}
}

// gateAnalysisDeadlines are the moments an analysis step waits for: the end
// of its warm-up, the next sample once the throttle opens, and the stall
// timeout measured from the last conclusive sample or the step's entry.
func gateAnalysisDeadlines(cs *v1beta1.CanaryStatus, step v1beta1.RolloutGroupStep) []time.Time {
	a := step.Analysis
	if a == nil || cs.StepEnteredTime == nil {
		return nil
	}
	var out []time.Time
	if a.InitialDelay != nil {
		out = append(out, cs.StepEnteredTime.Add(a.InitialDelay.Duration))
	}
	if cs.LastEvaluationTime != nil && a.Interval.Duration > 0 {
		out = append(out, cs.LastEvaluationTime.Add(a.Interval.Duration))
	}
	anchor := cs.LastConclusiveEvaluationTime
	if anchor == nil {
		anchor = cs.StepEnteredTime
	}
	return append(out, anchor.Add(cellReadyTimeout))
}

// gateFollowerPolicy declares the decoder a follower of the engine at the
// ratio of their declared counts, with the decoder's own bounds left open
// so the coordinator's answer is the ratio's.
func gateFollowerPolicy(isvc *v1beta1.InferenceService, cfg gateConfig) {
	var engine, decoder int32
	for _, m := range cfg.members {
		switch m.comp {
		case v1beta1.EngineComponent:
			engine = m.desired
		case v1beta1.DecoderComponent:
			decoder = m.desired
		}
	}
	if engine == 0 || decoder == 0 || isvc.Spec.Decoder == nil {
		return
	}
	isvc.Spec.Decoder.MinReplicas = ptr.To(1)
	isvc.Spec.ScalingPolicy = &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional, Proportional: &v1beta1.ProportionalPolicy{
		Anchor: v1beta1.EngineComponent,
		Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: *resource.NewMilliQuantity(int64(decoder)*1000/int64(engine), resource.DecimalSI)},
	}}
}

// followerCount is the decoder count the scaling coordinator writes for an
// engine count, through the coordinator's own arithmetic.
func (w *gateWorld) followerCount(st *gateState, engine int32) (int32, bool) {
	policy := st.isvc.Spec.ScalingPolicy
	if policy == nil || policy.Proportional == nil || st.isvc.Spec.Decoder == nil {
		return 0, false
	}
	ratio, ok := policy.Proportional.Ratios[v1beta1.DecoderComponent]
	if !ok {
		return 0, false
	}
	count, err := scaling.Target(engine, ratio.MilliValue(), scaling.FollowerBounds(&st.isvc.Spec.Decoder.ComponentExtensionSpec))
	if err != nil {
		w.fail("follower count: %v", err)
	}
	return count.Target, true
}

// memberIndex is the index of a Component among the members, -1 when the
// shape has none.
func (w *gateWorld) memberIndex(comp v1beta1.ComponentType) int {
	for i, m := range w.cfg.members {
		if m.comp == comp {
			return i
		}
	}
	return -1
}

// analysisLive reports a unit at a step with a metric gate whose record is
// live: the metrics source's answer still decides something.
func (w *gateWorld) analysisLive(st *gateState) bool {
	for _, u := range w.units(st) {
		cs := u.record
		if cs == nil || cs.CurrentStep < 0 || int(cs.CurrentStep) >= len(u.group.Canary.Steps) {
			continue
		}
		switch u.state {
		case rollout.CanaryStateStaging, rollout.CanaryStateServing, rollout.CanaryStatePreHold, rollout.CanaryStateDraining:
			if u.group.Canary.Steps[cs.CurrentStep].Analysis != nil {
				return true
			}
		}
	}
	return false
}

// anythingComingUp reports an Instance of any member that is building, lost
// or short: capacity the world can still bring up.
func (w *gateWorld) anythingComingUp(st *gateState) bool {
	for _, m := range st.members {
		for _, r := range m.rows {
			if r.kind != gateRowServing {
				return true
			}
		}
	}
	return false
}

// The two nodes a loss names: the one under the first Instance of every
// member, which a canary keeps on the stable revision, and the one under
// the last, which a canary moves first.
const (
	gateNodeFirst = 0
	gateNodeLast  = -1
)

func gateNodeName(node int) string {
	if node == gateNodeLast {
		return "the node under the last Instances"
	}
	return "the node under the first Instances"
}

// nodeRow is the row of a member that a node hosts.
func nodeRow(m gateMemberState, node int) int {
	if node == gateNodeLast {
		return len(m.rows) - 1
	}
	return node
}

// firstServingPerRevision lists, per revision a member serves, the first
// serving Instance, in index order.
func (w *gateWorld) firstServingPerRevision(st *gateState, i int) []int {
	var out []int
	seen := map[string]bool{}
	for idx, r := range st.members[i].rows {
		if r.kind != gateRowServing || seen[r.rev] {
			continue
		}
		seen[r.rev] = true
		out = append(out, idx)
	}
	return out
}

// faultMoves are the moves of the wider world, each under its budget: a
// node lost under one Instance of every member, a gang member lost, a resize
// of one side, of both sides or of the ratio, and the metrics source
// breaching or answering nothing while a metric gate decides.
func (w *gateWorld) faultMoves(st *gateState) []gateMove {
	var out []gateMove
	b := st.budgets
	if b.nodeLosses > 0 {
		out = append(out, gateMove{kind: "nodeloss", idx: gateNodeFirst}, gateMove{kind: "nodeloss", idx: gateNodeLast})
	}
	if b.memberLosses > 0 {
		for i := range st.members {
			if w.cfg.members[i].gang < 2 {
				continue
			}
			for _, idx := range w.firstServingPerRevision(st, i) {
				out = append(out, gateMove{kind: "memberloss", member: i, idx: idx})
			}
		}
	}
	if b.resizes > 0 {
		out = append(out, w.resizeMoves(st)...)
	}
	if w.analysisLive(st) && !st.sample.failing && !st.sample.gap {
		if b.analysisFails > 0 {
			out = append(out, gateMove{kind: "analysis-fail"})
		}
		if b.analysisGaps > 0 {
			out = append(out, gateMove{kind: "analysis-gap"})
		}
	}
	return out
}

// resizeMoves resize one side by half of its count, up and down; a
// prefill/decode pair also resizes both sides together, up and down, and
// the ratio alone (the engine up by half, the decoder down by half). The
// router is not resized beside a pair, and a follower is resized through
// its anchor.
func (w *gateWorld) resizeMoves(st *gateState) []gateMove {
	var out []gateMove
	for i, m := range st.members {
		comp := w.cfg.members[i].comp
		if (comp == v1beta1.RouterComponent && len(st.members) > 1) || (w.cfg.follower && comp == v1beta1.DecoderComponent) {
			continue
		}
		half := m.desired / 2
		if half < 1 {
			continue
		}
		out = append(out, gateMove{kind: "resize", member: i, delta: half})
		if m.desired-half >= 1 {
			out = append(out, gateMove{kind: "resize", member: i, delta: -half})
		}
	}
	engine, decoder := w.memberIndex(v1beta1.EngineComponent), w.memberIndex(v1beta1.DecoderComponent)
	if engine >= 0 && decoder >= 0 && !w.cfg.follower && st.members[engine].desired >= 2 && st.members[decoder].desired >= 2 {
		out = append(out, gateMove{kind: "resize-both", delta: 1}, gateMove{kind: "resize-both", delta: -1}, gateMove{kind: "reshape"})
	}
	return out
}

// applyFault makes one move of the wider world on next.
func (w *gateWorld) applyFault(next *gateState, mv gateMove) {
	switch mv.kind {
	case "nodeloss":
		next.budgets.nodeLosses--
		for i := range next.members {
			w.loseInstance(next, i, nodeRow(next.members[i], mv.idx))
		}
	case "memberloss":
		next.budgets.memberLosses--
		w.loseInstance(next, mv.member, mv.idx)
	case "resize":
		next.budgets.resizes--
		w.resize(next, mv.member, mv.delta)
	case "resize-both":
		next.budgets.resizes--
		for _, comp := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
			i := w.memberIndex(comp)
			w.resize(next, i, mv.delta*(next.members[i].desired/2))
		}
	case "reshape":
		next.budgets.resizes--
		engine, decoder := w.memberIndex(v1beta1.EngineComponent), w.memberIndex(v1beta1.DecoderComponent)
		w.resize(next, engine, next.members[engine].desired/2)
		w.resize(next, decoder, -(next.members[decoder].desired / 2))
	case "analysis-fail":
		next.budgets.analysisFails--
		next.sample = gateSample{failing: true}
	case "analysis-gap":
		next.budgets.analysisGaps--
		next.sample = gateSample{gap: true}
	case "analysis-recover":
		next.sample = gateSample{}
	default:
		w.fail("unknown move %q", mv.kind)
	}
}

// loseInstance takes the pods of a serving Instance off its node: a gang is
// left short of one member with the rest serving, a single pod is gone.
func (w *gateWorld) loseInstance(st *gateState, i, idx int) {
	if idx < 0 || idx >= len(st.members[i].rows) {
		return
	}
	r := &st.members[i].rows[idx]
	if r.kind != gateRowServing {
		return
	}
	r.kind = gateRowLost
	if w.cfg.members[i].gang > 1 {
		r.kind = gateRowShort
	}
}

// resize changes one member's count by delta, the highest Instances
// removed on the way down and new Instances building toward the target on
// the way up, as a scale does; a follower's count moves with its anchor in
// the same write.
func (w *gateWorld) resize(st *gateState, i int, delta int32) {
	if delta == 0 {
		return
	}
	st.scaled = true
	m := &st.members[i]
	m.desired += delta
	m.generation++
	st.isvc.Generation++
	if delta < 0 {
		m.rows = m.rows[:len(m.rows)+int(delta)]
	} else {
		target := m.updateRev
		if m.rollbackTo != "" {
			target = query.RevisionFromName(m.rollbackTo).Hash()
		}
		for k := int32(0); k < delta; k++ {
			m.rows = append(m.rows, gateRow{kind: gateRowCreating, target: target})
		}
	}
	if w.cfg.follower && w.cfg.members[i].comp == v1beta1.EngineComponent {
		if count, ok := w.followerCount(st, m.desired); ok {
			decoder := w.memberIndex(v1beta1.DecoderComponent)
			w.resize(st, decoder, count-st.members[decoder].desired)
		}
	}
}

// metricMessages is what the record says about the last sample.
func metricMessages(cs *v1beta1.CanaryStatus) string {
	parts := make([]string, 0, len(cs.MetricResults))
	for _, m := range cs.MetricResults {
		switch {
		case m.Message != "":
			parts = append(parts, m.Name+": "+m.Message)
		case m.Passed:
			parts = append(parts, m.Name+" passed")
		default:
			parts = append(parts, m.Name+" breached")
		}
	}
	return strings.Join(parts, ", ")
}

// holdNamed reads what an operator sees at a quiet state and reports whether
// it names the wait, with the reading. The pause verb, a plan condition out
// of sync, a warning event of the pass, and for each unit the record and
// the phase its state calls for: a capacity wait recorded, a Paused phase
// at a manual gate or beside a metric result, the pre-step hold, the
// Failed reason, the RolledBack phase. A phase that claims movement
// (Pending with no wait recorded, Canarying, Promoting, RollingBack) at a
// state where nothing moves names nothing.
func (w *gateWorld) holdNamed(st *gateState) (bool, string) {
	var reading []string
	named := false
	name := func(format string, args ...any) {
		named = true
		reading = append(reading, fmt.Sprintf(format, args...))
	}
	if paused, _ := constants.RolloutPauseState(st.isvc.Annotations); paused {
		name("paused by the operator")
	}
	if c := st.isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanDriftCondition)); c != nil && c.Status == corev1.ConditionTrue {
		name("drift: %s", c.Message)
	}
	if c := st.isvc.Status.GetCondition(apis.ConditionType(v1beta1.RolloutPlanReadyCondition)); c != nil && c.Status == corev1.ConditionFalse {
		name("plan: %s", c.Message)
	}
	for _, e := range st.passEvents {
		if strings.HasPrefix(e, corev1.EventTypeWarning) {
			name("event: %s", e)
		}
	}
	for _, u := range w.units(st) {
		cs := u.record
		if cs == nil {
			reading = append(reading, fmt.Sprintf("%s: no record, phase %q", u.primary, u.phase))
			continue
		}
		reading = append(reading, fmt.Sprintf("%s: %s at step %d, phase %s", u.primary, u.state, cs.CurrentStep, u.phase))
		switch u.state {
		case rollout.CanaryStateFailed:
			if cs.Failed != nil {
				name("%s stopped: %s", u.primary, cs.Failed.Reason)
			} else {
				name("%s phase Failed", u.primary)
			}
		case rollout.CanaryStateRolledBack:
			name("%s rolled back, revision %s rejected", u.primary, cs.RolledBackRevisionHash)
		case rollout.CanaryStatePreHold:
			name("%s holds before its step for a promote", u.primary)
		case rollout.CanaryStateStaging, rollout.CanaryStateServing, rollout.CanaryStateDraining:
			if cs.CapacityWaitSince != nil {
				name("%s waits for capacity since %s", u.primary, cs.CapacityWaitSince.UTC().Format(time.TimeOnly))
			}
			if u.phase != v1beta1.RolloutPhasePaused || cs.CurrentStep < 0 || int(cs.CurrentStep) >= len(u.group.Canary.Steps) {
				continue
			}
			step := u.group.Canary.Steps[cs.CurrentStep]
			switch {
			case step.Analysis != nil && len(cs.MetricResults) > 0:
				name("%s analysis: %s", u.primary, metricMessages(cs))
			case step.Analysis == nil && step.Pause != nil && step.Pause.Duration == nil:
				name("%s paused at a manual step for a promote", u.primary)
			}
		}
	}
	return named, strings.Join(reading, "; ")
}

// momentKey names a moment of a run by what the step machine shows: each
// unit's state and step, and in the full mode its phase and whether
// anything is still coming up.
func (w *gateWorld) momentKey(st *gateState, full bool) string {
	var b strings.Builder
	for _, u := range w.units(st) {
		step := int32(-1)
		if u.record != nil {
			step = u.record.CurrentStep
		}
		fmt.Fprintf(&b, "%s/%s/%d;", u.primary, u.state, step)
		if full {
			fmt.Fprintf(&b, "%s;", u.phase)
		}
	}
	if full {
		fmt.Fprintf(&b, "moving=%t", w.anythingComingUp(st))
	}
	return b.String()
}

// probeMoves are the adverse moves a moment is probed with: every one in
// the full mode; in the bounded mode one of each kind, with both nodes of a
// node loss and a resize up and one down.
func (w *gateWorld) probeMoves(st *gateState, full bool) []gateMove {
	moves := w.adverseMoves(st)
	if full {
		return moves
	}
	seen := map[string]bool{}
	var out []gateMove
	for _, mv := range moves {
		key := mv.kind
		switch mv.kind {
		case "nodeloss":
			key += fmt.Sprint(mv.idx)
		case "resize":
			key += fmt.Sprint(mv.delta > 0)
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, mv)
	}
	return out
}

// probeMoments walks the straight run and, at the first state of each
// moment it passes through, makes each probe move once and plays the run
// out from there on the progress and release moves alone, recording every
// state on the way.
func (w *gateWorld) probeMoments(rep *gateReport, maxMoves int, full bool) {
	st := w.initial()
	w.settle(st)
	seen := map[string]bool{}
	for depth := 0; depth < maxMoves && !w.finished(st); depth++ {
		if moment := w.momentKey(st, full); !seen[moment] {
			seen[moment] = true
			for _, mv := range w.probeMoves(st, full) {
				w.playOut(rep, w.apply(st, mv), maxMoves, full)
			}
		}
		moves := append(w.progressMoves(st), w.releaseMoves(st, full, false)...)
		if len(moves) == 0 {
			return
		}
		st = w.apply(st, moves[0])
	}
}

// playOut plays a run from st toward an end on the progress and release
// moves alone: the world brings up what it can, the clock reaches each
// deadline and the operator answers each hold with the verb it names. A
// play-out that reaches no end inside the bound is reported under depth.
func (w *gateWorld) playOut(rep *gateReport, st *gateState, maxMoves int, full bool) {
	rep.runs++
	for depth := 0; ; depth++ {
		w.record(rep, st, full)
		if w.finished(st) {
			return
		}
		moves := append(w.progressMoves(st), w.releaseMoves(st, full, false)...)
		if len(moves) == 0 {
			return
		}
		if depth >= maxMoves {
			rep.counts["depth"]++
			if _, ok := rep.findings["depth"]; !ok {
				rep.findings["depth"] = gateFinding{class: "depth", message: fmt.Sprintf("a play-out did not reach an end in %d moves: %s", depth, w.describe(st)), state: st}
			}
			return
		}
		st = w.apply(st, moves[0])
		if len(st.path) > rep.depth {
			rep.depth = len(st.path)
		}
	}
}

// gateKnownFinding marks a finding the model keeps visible without failing
// until the behaviour changes: the class it would fail under, the
// configurations and messages it matches, and why in plain words. A fix
// removes the entry.
type gateKnownFinding struct {
	name    string
	class   string
	matches func(cfg gateConfig, f gateFinding) bool
	why     string
}

// gateKnownFindings are the findings kept visible without failing the test:
// each names a reading the engine produces under a decision still open, the
// message that identifies it, and the change that removes it. Empty while
// every reading has its decision.
var gateKnownFindings []gateKnownFinding

// gateKnownFindingFor is the name of the known finding f matches under cfg,
// empty when none does.
func gateKnownFindingFor(cfg gateConfig, f gateFinding) string {
	for _, k := range gateKnownFindings {
		if k.class == f.class && k.matches(cfg, f) {
			return "known-" + k.name
		}
	}
	return ""
}

// gateFleet is one reference prefill/decode shape with its routers.
type gateFleet struct {
	name                     string
	prefill, decode, routers int32
}

var gateFleets = []gateFleet{
	{"12:8", 12, 8, 2},
	{"15:15", 15, 15, 2},
	{"16:8", 16, 8, 2},
	{"20:10", 20, 10, 2},
	{"24:8", 24, 8, 2},
	{"30:10", 30, 10, 2},
	{"12:4", 12, 4, 2},
}

// gateGangs are the pods per Instance the reference shapes run, engine and
// decoder: single pods and gangs of two to eight. The bounded mode runs the
// first three; the full mode runs them all.
var gateGangs = [][2]int32{{1, 1}, {2, 2}, {4, 4}, {8, 8}, {8, 4}, {2, 8}}

func gateGangChoices(full bool) [][2]int32 {
	if full {
		return gateGangs
	}
	return gateGangs[:3]
}

// gateFleetShape is a reference fleet as the model's shape: the routers,
// then the engine and the decoder with their pods per Instance.
func gateFleetShape(f gateFleet, gang [2]int32) gateShape {
	name := fmt.Sprintf("pd%s+router%d", f.name, f.routers)
	if gang[0] > 1 || gang[1] > 1 {
		name += fmt.Sprintf("/gang%dx%d", gang[0], gang[1])
	}
	return gateShape{name: name, members: []gateMember{
		{comp: v1beta1.RouterComponent, desired: f.routers, gang: 1},
		{comp: v1beta1.EngineComponent, desired: f.prefill, gang: gang[0]},
		{comp: v1beta1.DecoderComponent, desired: f.decode, gang: gang[1]},
	}}
}

// gateFleetStepNames are the step lists the reference shapes run under:
// the reference lists and the two with a metric gate.
var gateFleetStepNames = append(append([]string(nil), gateStepNames...), "two-analysis", "three-analysis")

// gateFleetBudgets is how many times the world may do each costly thing in
// one run on a reference shape.
func gateFleetBudgets(full bool, gang bool) gateBudgets {
	b := gateBudgets{lostPods: 1, lostFlushes: 1, pushes: 1, edits: 1, removals: 1, rollbacks: 1, pauses: 1, resumes: 1,
		nodeLosses: 1, resizes: 1, analysisFails: 1, analysisGaps: 1}
	if gang {
		b.memberLosses = 1
	}
	if full {
		b.forces, b.lostPods, b.scales = 1, 2, 1
	}
	return b
}

// gateFleetConfig is one reference-fleet configuration.
func gateFleetConfig(f gateFleet, gang [2]int32, strategy v1beta1.UpdateStrategyType, steps string, drain int32, partition *int32, follower bool, full bool) gateConfig {
	cfg := gateConfigFor(gateFleetShape(f, gang), strategy, steps, drain, partition, full)
	cfg.budgets = gateFleetBudgets(full, gang[0] > 1 || gang[1] > 1)
	cfg.follower = follower
	cfg.name = gateFleetShape(f, gang).name + " " + cfg.String()
	return cfg
}

// gateFleetConfigs lists the reference-fleet configurations: in the bounded mode
// every shape under both strategies with the step lists, gangs, drain
// windows and the follower cycling through, fourteen in all; in the full
// mode every shape under both strategies and every step list with the gangs
// cycling, every shape with a follower and with an operator partition, and a
// seeded sample over the whole space.
func gateFleetConfigs(full bool) []gateConfig {
	strategies := []v1beta1.UpdateStrategyType{v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain}
	drain := func(n int) int32 {
		if n%2 == 1 {
			return 600
		}
		return 0
	}
	gangs := gateGangChoices(full)
	var out []gateConfig
	for fi, f := range gateFleets {
		for si, strategy := range strategies {
			n := fi*len(strategies) + si
			out = append(out, gateFleetConfig(f, gangs[n%len(gangs)], strategy, gateFleetStepNames[n%len(gateFleetStepNames)], drain(n), nil, n%3 == 0, full))
		}
	}
	if full {
		for fi, f := range gateFleets {
			for si, strategy := range strategies {
				for ti, steps := range gateFleetStepNames {
					n := fi + si + ti
					out = append(out, gateFleetConfig(f, gangs[n%len(gangs)], strategy, steps, drain(n), nil, false, full))
				}
			}
			out = append(out,
				gateFleetConfig(f, gangs[fi%len(gangs)], strategies[fi%2], "two-analysis", 600, nil, true, full),
				gateFleetConfig(f, gangs[(fi+1)%len(gangs)], strategies[(fi+1)%2], "three-manual", 0, ptr.To(int32(2)), false, full))
		}
	}
	// The seeded sample over the whole space runs in the full mode; the
	// bounded mode keeps the fourteen shape-and-strategy configurations.
	rng := rand.New(rand.NewSource(20261102))
	sample := 0
	if full {
		sample = 40
	}
	for n := 0; n < sample; n++ {
		f := gateFleets[rng.Intn(len(gateFleets))]
		var partition *int32
		if rng.Intn(4) == 0 {
			partition = ptr.To(int32(1 + rng.Intn(3)))
		}
		out = append(out, gateFleetConfig(f, gangs[rng.Intn(len(gangs))], strategies[rng.Intn(2)], gateFleetStepNames[rng.Intn(len(gateFleetStepNames))], drain(rng.Intn(2)), partition, rng.Intn(3) == 0, full))
	}
	return out
}

// TestCanarySteps_ReferenceShapesHoldOnlyWithANamedWayOut explores a canary
// over the reference prefill/decode shapes behind two routers, single pods
// and gangs of two to eight, under both strategies at a 10 percent budget,
// every step list including the metric-gated ones, with a decoder that
// follows the engine's count in some configurations. Beside the moves of
// the reference model, the world loses a node under one Instance of every
// member, loses a gang member, resizes one side, both sides or the ratio,
// and makes the metrics source breach or answer nothing; every such move is
// also made once at every moment of the straight run. A quiet state must
// have a way out that is named in the status, and a capacity wait must have
// capacity coming.
func TestCanarySteps_ReferenceShapesHoldOnlyWithANamedWayOut(t *testing.T) {
	gateExploreConfigs(t, gateFleetConfigs(os.Getenv("OME_CANARY_HUNT") == "full"))
}

// unitDone reports whether the unit that drives comp stands at its done
// mark: the cutover is written and the last held Instance rolls.
func (w *gateWorld) unitDone(st *gateState, comp v1beta1.ComponentType) bool {
	for _, u := range w.units(st) {
		if gateGroupHas(u.group, comp) && u.state == rollout.CanaryStateDone {
			return true
		}
	}
	return false
}

// gateKnownFindingWhy is the words of the known finding a report class names,
// empty for any other class.
func gateKnownFindingWhy(class string) string {
	for _, k := range gateKnownFindings {
		if "known-"+k.name == class {
			return k.why
		}
	}
	return ""
}
