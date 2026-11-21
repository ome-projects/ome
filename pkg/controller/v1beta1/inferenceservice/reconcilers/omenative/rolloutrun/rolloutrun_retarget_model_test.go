package rolloutrun_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/rollout"
)

// This file drives the run layer (rolloutrun.Reconcile) through a push that
// another change replaces before, while or after its run opens: the operator
// pushes version 2 and then, at any point between the controllers' steps,
// pushes version 1 back or pushes version 3. The steps are the service
// controller's pass (the run layer over the replicas as last published, the
// canary bind on an open, the projector reaching every member) and each
// member's replica pass (an attempt toward a revision the spec no longer
// names turned around, the starts the real gate stack admits, the
// Create pass rebuilding lost Instances, the status write). The change lands
// before the replicas were projected, after the projection but before a
// replica wrote its status, after the run opened, while 0, 1 or more
// Instances have an attempt or a rebuild toward the pushed revision, or
// after the roll finished. The world is the run-layer model's: each member's
// InferenceReplica as published and the service's status as the service
// controller writes it. The groups are one rollingUpdate, blueGreen or canary
// group of an engine alone, an engine and a decoder, or the pair with a
// router, of 1 to 4 Instances in gangs of 1 to 8 pods, with and without a
// pairing protocol on the push, with an Instance rebuilding after a loss
// when the push lands, and with a plan that does not resolve (a policy
// reference naming no RolloutPolicy).
//
// At every state the model reads: a run the run layer opened names, once
// the replicas are published fresh, only revisions they still target (a
// retarget closes it as replaced, RolloutRunSuperseded, and the status record
// names the replaced run's revisions); a fresh divergence opens a run; every run
// opened is closed with a reason by the end; every attempt or rebuild toward
// a replaced revision is turned around and no pod of it is left; no pod is
// built toward a revision the plan gate holds; the budgets hold; the roll
// ends with the run closed and the group at rest. The engine's own readings
// around a change that lands within one projection of the replica are
// reported as known classes. OME_RUN_RETARGET_HUNT=full widens the shapes,
// the sample and the bounds.
//
// The replica is the model's own. The repair's rebuild, the Create pass's
// refill of a lost Instance and its retire of a refill onto a moved target,
// and a move's replacement are written here from their rules in
// workload/ops, not run from it; the ops package's cases read those paths
// (restart_test.go, create_test.go, create_external_test.go,
// migrate_test.go). The canary executor is not run either
// (retargetKnownClasses).

// retargetKind is the group's progression.
type retargetKind uint8

const (
	retargetRolling retargetKind = iota
	retargetBlueGreen
	retargetCanary
)

var retargetKindNames = [...]string{"rollingUpdate", "blueGreen", "canary"}

// retargetProtocolOld and retargetProtocolNew are the pairing protocols
// version 1 and the pushed versions carry when the push changes it.
const (
	retargetProtocolOld = "pair-v1"
	retargetProtocolNew = "pair-v2"
)

// retargetBudgets is how many times the world may make each costly move in
// one path.
type retargetBudgets struct {
	pushes, rollbacks, losses, fails int8
}

// retargetConfig is one configuration under exploration.
type retargetConfig struct {
	name    string
	members []runMemberConfig
	kind    retargetKind
	// steps are the canary's capacity percents in order, the last 100.
	steps []int32
	// protocol makes the push change the pairing protocol.
	protocol bool
	// brokenAtPush starts the world with the last Instance of the first
	// member rebuilding after a loss when the push lands.
	brokenAtPush bool
	// unresolvable names a RolloutPolicy that does not exist, so no plan
	// composes and the run layer holds every push.
	unresolvable bool
	maxAttempts  int32
	budgets      retargetBudgets
}

func (c retargetConfig) String() string {
	parts := make([]string, 0, len(c.members))
	for _, m := range c.members {
		p := fmt.Sprintf("%s=%d", m.comp, m.desired)
		if m.pods > 1 {
			p += fmt.Sprintf("x%d", m.pods)
		}
		parts = append(parts, p+"/"+m.short()+"/"+m.budget)
	}
	s := strings.Join(parts, " + ") + " " + retargetKindNames[c.kind]
	if c.kind == retargetCanary {
		steps := make([]string, 0, len(c.steps))
		for _, p := range c.steps {
			steps = append(steps, strconv.Itoa(int(p)))
		}
		s += "@" + strings.Join(steps, ",")
	}
	if c.protocol {
		s += " protocol"
	}
	if c.brokenAtPush {
		s += " broken-at-push"
	}
	if c.unresolvable {
		s += " unresolvable-plan"
	}
	return s + fmt.Sprintf(" pushes=%d rollbacks=%d losses=%d fails=%d", c.budgets.pushes, c.budgets.rollbacks, c.budgets.losses, c.budgets.fails)
}

// retargetRun is one run the run layer's events reported: the revisions it recorded
// per member, the spec's revision when it opened, and its close.
type retargetRun struct {
	id       string
	targets  map[v1beta1.ComponentType]int8
	openedAt int8   // the spec's revision when the run opened
	closed   string // the outcome once closed, "" while open
	reason   string // the close event's words
}

// retargetState is the run-layer model's state with the record of the
// change under test: the runs the run layer's events reported, the highest revision
// pushed so far, the revisions a pod was built toward while no run named
// them, and the attempts the replicas turned around.
type retargetState struct {
	*runState
	runs []retargetRun
	// top is the highest revision pushed so far; the first push is 2.
	top int8
	// builtWithoutRun records the revisions a rebuild or a creation was
	// built toward while no run named them.
	builtWithoutRun map[int8]bool
	// turnarounds counts the attempts toward a replaced revision the
	// replica passes turned around.
	turnarounds int
}

func (st *retargetState) clone() *retargetState {
	out := &retargetState{
		runState: st.runState.clone(), runs: make([]retargetRun, len(st.runs)),
		top: st.top, builtWithoutRun: map[int8]bool{}, turnarounds: st.turnarounds,
	}
	for rev, built := range st.builtWithoutRun {
		out.builtWithoutRun[rev] = built
	}
	for i, r := range st.runs {
		targets := make(map[v1beta1.ComponentType]int8, len(r.targets))
		for k, v := range r.targets {
			targets[k] = v
		}
		r.targets = targets
		out.runs[i] = r
	}
	return out
}

// lastRun is the status record of the last closed run, nil while the
// status keeps none.
func (st *retargetState) lastRun() *v1beta1.RolloutRunRecord {
	if st.service.Status.Rollout == nil {
		return nil
	}
	return st.service.Status.Rollout.LastRun
}

// openRuns are the reported runs not yet closed.
func (st *retargetState) openRuns() []retargetRun {
	var out []retargetRun
	for _, r := range st.runs {
		if r.closed == "" {
			out = append(out, r)
		}
	}
	return out
}

// runsNaming are the reported runs that recorded rev for some member.
func (st *retargetState) runsNaming(rev int8) []retargetRun {
	var out []retargetRun
	for _, r := range st.runs {
		for _, t := range r.targets {
			if t == rev {
				out = append(out, r)
				break
			}
		}
	}
	return out
}

// replaced reports a revision the operator pushed and then moved off: a
// pushed revision other than the one the spec names now.
func (st *retargetState) replaced(rev int8) bool {
	return rev >= 2 && rev <= st.top && rev != st.rev
}

// key hashes what decides the future: the run-layer state, the canary
// record, the runs reported and closed, the revisions named and built
// without a run, and the turnarounds so far.
func (st *retargetState) key() string {
	var b strings.Builder
	b.WriteString(st.runState.key())
	fmt.Fprintf(&b, "|top=%d turned=%d runs=%d/%d", st.top, min(st.turnarounds, 3), len(st.runs), len(st.openRuns()))
	for rev := int8(2); rev <= runLastRevision; rev++ {
		if st.builtWithoutRun[rev] {
			fmt.Fprintf(&b, "|built=%d", rev)
		}
	}
	for _, r := range st.runs {
		fmt.Fprintf(&b, "|%s>%s", retargetTargetsWords(r.targets), r.closed)
	}
	for comp, cs := range st.service.Status.Components {
		if cs.Canary != nil {
			fmt.Fprintf(&b, "|canary %s step=%d rev=%s stable=%s rejected=%s phase=%s", comp, cs.Canary.CurrentStep, cs.Canary.CanaryRevisionHash, cs.Canary.StableRevisionHash, cs.Canary.RolledBackRevisionHash, cs.RolloutPhase)
		}
	}
	return b.String()
}

func (st *retargetState) String() string {
	runs := make([]string, 0, len(st.runs))
	for _, r := range st.runs {
		s := r.id + " toward " + retargetTargetsWords(r.targets)
		if r.closed != "" {
			s += " closed " + r.closed
		} else {
			s += " open"
		}
		runs = append(runs, s)
	}
	if len(runs) == 0 {
		runs = append(runs, "none")
	}
	canaryWords := ""
	for comp, cs := range st.service.Status.Components {
		if cs.Canary != nil {
			canaryWords = fmt.Sprintf("; canary %s step %d phase %s rejected %q", comp, cs.Canary.CurrentStep, cs.RolloutPhase, cs.Canary.RolledBackRevisionHash)
		}
	}
	return fmt.Sprintf("%s; runs: %s%s; top=%d", st.runState.String(), strings.Join(runs, ", "), canaryWords, st.top)
}

func retargetTargetsWords(targets map[v1beta1.ComponentType]int8) string {
	comps := make([]string, 0, len(targets))
	for comp := range targets {
		comps = append(comps, string(comp))
	}
	sort.Strings(comps)
	parts := make([]string, 0, len(comps))
	for _, comp := range comps {
		parts = append(parts, fmt.Sprintf("%s=%d", comp, targets[v1beta1.ComponentType(comp)]))
	}
	return strings.Join(parts, ",")
}

// retargetEvent is one event the run layer recorded in a pass.
type retargetEvent struct {
	reason, message string
}

// retargetRecorder collects the run layer's events of one pass.
type retargetRecorder struct {
	events []retargetEvent
}

func (r *retargetRecorder) Event(_ runtime.Object, _, reason, message string) {
	r.events = append(r.events, retargetEvent{reason: reason, message: message})
}

func (r *retargetRecorder) Eventf(_ runtime.Object, _, reason, format string, args ...interface{}) {
	r.events = append(r.events, retargetEvent{reason: reason, message: fmt.Sprintf(format, args...)})
}

func (r *retargetRecorder) AnnotatedEventf(_ runtime.Object, _ map[string]string, _, reason, format string, args ...interface{}) {
	r.events = append(r.events, retargetEvent{reason: reason, message: fmt.Sprintf(format, args...)})
}

// retargetReader serves the run-layer model's replicas and revisions with
// the pairing protocol each revision carries when the push changes it, and
// the protocol the projector wrote on each replica.
type retargetReader struct {
	runReader
	w  *retargetWorld
	st *retargetState
}

func (r retargetReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.runReader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	switch out := obj.(type) {
	case *appsv1.ControllerRevision:
		if proto := r.w.protocolOf(runRevisionOf(out.Name)); proto != "" {
			out.Annotations = map[string]string{query.LabelPairingProtocol: proto}
		}
	case *v1beta1.InferenceReplica:
		for i, mc := range r.w.cfg.members {
			if key.Name == runServiceName+"-"+string(mc.comp) {
				if proto := r.w.protocolOf(r.st.members[i].target); proto != "" {
					out.Spec.PairingProtocol = ptr.To(proto)
				}
			}
		}
	}
	return nil
}

// retargetClient is the run layer's client: it reads through the reader
// and writes nothing, since the model sets no verb annotation.
type retargetClient struct {
	client.Client
	reads retargetReader
}

func (c retargetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.reads.Get(ctx, key, obj, opts...)
}

// retargetMove is one thing that happens between two states: a tick of the
// controllers with no change, an Instance coming up, a pod, a gang or a gang
// member lost, an attempt failing, or the operator's change (a push or a
// push back) landing at one point of the tick: before the service pass, after
// it, or after one member's replica pass.
type retargetMove struct {
	kind        string
	member, idx int
	phase       int
}

func (m retargetMove) String() string {
	switch m.kind {
	case "tick":
		return "tick"
	case "push", "rollback":
		return fmt.Sprintf("%s at phase %d", m.kind, m.phase)
	case "backoff", "prune":
		return fmt.Sprintf("%s member %d", m.kind, m.member)
	}
	return fmt.Sprintf("%s member %d row %d", m.kind, m.member, m.idx)
}

// retargetWorld builds the objects the run layer and the gates read from a
// state and runs the controllers' steps over them.
type retargetWorld struct {
	t   *testing.T
	cfg retargetConfig
	ctx context.Context
	// run is the run-layer world the replicas, the status writes, the
	// projection and the group observation are rendered through.
	run      *runWorld
	recorder *retargetRecorder
	// clock is the time the run layer is handed: one second per pass, so
	// run ids and the opening times are reproducible.
	clock time.Time
	// runPasses counts the run layer's passes; trial passes, which ask
	// whether a tick would change anything, are not counted.
	runPasses int
	trial     bool
	// cur is the state a tick runs over, for the steps the run-layer world
	// calls back with its own view of the state.
	cur *retargetState
}

func newRetargetWorld(t *testing.T, cfg retargetConfig) *retargetWorld {
	t.Helper()
	run := newRunWorld(t, runConfig{members: cfg.members, rollingUpdate: cfg.kind == retargetRolling, setting: runSettingNamed("off"), maxAttempts: cfg.maxAttempts})
	w := &retargetWorld{t: t, cfg: cfg, ctx: context.Background(), run: run, recorder: &retargetRecorder{},
		clock: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	run.readerFor = func(*runState) client.Reader { return w.reader(w.cur) }
	run.targetObject = w.revisionObject
	run.beforePass = func(st *runState, i int) { w.turnAround(w.cur, &st.members[i]) }
	run.projectedTarget = func(*runState) int8 { return w.projectedTarget(w.cur) }
	run.projectedPartition = func(_ *runState, i int) int32 { return w.projectedPartition(w.cur, i) }
	run.planGateForExemptStarts = true
	return w
}

// protocolOf is the pairing protocol a revision carries: none unless the
// push changes it, then the old one on version 1 and the new one after.
func (w *retargetWorld) protocolOf(rev int8) string {
	switch {
	case !w.cfg.protocol || rev <= 0:
		return ""
	case rev == 1:
		return retargetProtocolOld
	}
	return retargetProtocolNew
}

// revisionObject is the ControllerRevision of one version of a member, with
// the protocol it carries.
func (w *retargetWorld) revisionObject(comp v1beta1.ComponentType, rev int8) *appsv1.ControllerRevision {
	cr := runRevisionObject(comp, rev)
	if proto := w.protocolOf(rev); proto != "" {
		cr.Annotations = map[string]string{query.LabelPairingProtocol: proto}
	}
	return cr
}

func (w *retargetWorld) reader(st *retargetState) retargetReader {
	return retargetReader{runReader: w.run.reader(st.runState), w: w, st: st}
}

// group is the one rollout group the spec declares: every member under one
// progression, or a reference to a policy that does not exist.
func (w *retargetWorld) group() v1beta1.RolloutGroup {
	comps := make([]v1beta1.ComponentType, 0, len(w.cfg.members))
	for _, m := range w.cfg.members {
		comps = append(comps, m.comp)
	}
	g := v1beta1.RolloutGroup{Components: comps}
	if w.cfg.unresolvable {
		g.PolicyRef = &v1beta1.RolloutPolicyRef{Name: "missing-policy", Progression: v1beta1.RolloutProgressionRollingUpdate}
		return g
	}
	switch w.cfg.kind {
	case retargetRolling:
		g.RollingUpdate = &v1beta1.GroupRollingUpdate{
			MaxSurge:       ptr.To(intstr.Parse(w.cfg.members[0].budget)),
			MaxUnavailable: ptr.To(intstr.Parse(w.cfg.members[0].budget)),
		}
	case retargetBlueGreen:
		g.BlueGreen = &v1beta1.GroupBlueGreen{}
	case retargetCanary:
		steps := make([]v1beta1.RolloutGroupStep, 0, len(w.cfg.steps))
		for i, p := range w.cfg.steps {
			step := v1beta1.RolloutGroupStep{Capacity: intstr.FromString(strconv.Itoa(int(p)) + "%"), Traffic: p}
			if i < len(w.cfg.steps)-1 {
				step.Pause = &v1beta1.RolloutPause{}
			}
			steps = append(steps, step)
		}
		g.Canary = &v1beta1.GroupCanary{Steps: steps}
	}
	return g
}

// spec is the service spec as the operator has it at a state: the one
// group, and the pairing protocol the spec's revision carries.
func (w *retargetWorld) spec(st *retargetState) v1beta1.InferenceServiceSpec {
	spec := v1beta1.InferenceServiceSpec{Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{w.group()}}}
	if proto := w.protocolOf(st.rev); proto != "" {
		spec.Rollout.PairingProtocol = ptr.To(proto)
	}
	return spec
}

// primary is the Component the canary's record is kept under.
func (w *retargetWorld) primary() v1beta1.ComponentType {
	g := w.group()
	return rollout.PrimaryOf(&g)
}

// canaryRecord is the canary unit's record, nil with no canary.
func (w *retargetWorld) canaryRecord(st *retargetState) *v1beta1.CanaryStatus {
	if w.cfg.kind != retargetCanary {
		return nil
	}
	return rollout.CanaryStatusFor(&st.service.Status, w.primary())
}

// rejectedRevision is the revision the canary holds a rejection of, 0 for
// none; stableRevision the revision the revert returns to.
func (w *retargetWorld) rejectedRevision(st *retargetState) int8 {
	cs := w.canaryRecord(st)
	if cs == nil {
		return 0
	}
	return runRevisionOfHash(cs.RolledBackRevisionHash)
}

func (w *retargetWorld) stableRevision(st *retargetState) int8 {
	cs := w.canaryRecord(st)
	if cs == nil || cs.StableRevisionHash == "" {
		return 1
	}
	return runRevisionOfHash(cs.StableRevisionHash)
}

// projectedTarget is the revision the projector writes on a member: the
// spec's, or the canary's stable revision while the unit holds a rejection
// of the spec's revision.
func (w *retargetWorld) projectedTarget(st *retargetState) int8 {
	if rej := w.rejectedRevision(st); rej != 0 && rej == st.rev {
		return w.stableRevision(st)
	}
	return st.rev
}

// projectedPartition is the partition the projector writes on a member:
// the user's, or under a canary the hold the canary resolves for the
// Component (the step's share under a recorded run, every Instance before
// one is recorded).
func (w *retargetWorld) projectedPartition(st *retargetState, i int) int32 {
	partition := st.partitions[i]
	if w.cfg.kind != retargetCanary {
		return partition
	}
	if resolve := canary.PartitionResolver(st.service, rollout.Policies{}, w.cfg.members[i].comp); resolve != nil {
		if p := resolve(st.desired[i]); p != nil {
			partition = *p
		}
	}
	return partition
}

// initial is the moment after the push of version 2: the service's
// generation moved, no replica has been projected yet, every Instance runs
// version 1 and serves (the last Instance of the first member rebuilding
// after a loss when the configuration says so), and no run is open.
func (w *retargetWorld) initial() *retargetState {
	b := w.cfg.budgets
	st := &retargetState{runState: &runState{gen: 2, rev: 2, grouped: true, pushes: b.pushes - 1, rollbacks: b.rollbacks}, top: 2, builtWithoutRun: map[int8]bool{}}
	st.service = &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: runServiceName, Namespace: runNamespace, Generation: 2}}
	st.service.Spec = w.spec(st)
	for i, mc := range w.cfg.members {
		m := runMember{
			rows: make([]runRow, mc.desired), target: 1, statusTarget: 1, current: 1,
			irGen: 1, statusGen: 1, annotation: 1,
			userPartition: mc.partition, replicas: mc.desired,
			ready: metav1.ConditionTrue, readyTrue: true,
			fails: b.fails, losses: b.losses,
		}
		for idx := range m.rows {
			m.rows[idx] = runRow{kind: runReady, rev: 1}
		}
		if i == 0 && w.cfg.brokenAtPush {
			m.rows[len(m.rows)-1] = runRow{kind: runRebuild, rev: 1, to: 1}
		}
		m.published = append([]runRow(nil), m.rows...)
		st.members = append(st.members, m)
		st.desired = append(st.desired, mc.desired)
		st.partitions = append(st.partitions, mc.partition)
		st.holds = append(st.holds, nil)
	}
	for i := range st.members {
		w.run.passing = i
		w.run.writeStatus(i, &st.members[i])
		w.run.passing = -1
	}
	return st
}

// fresh reports whether every member's replica is one consistent snapshot
// for the run layer: projected for the spec's generation and its status
// written for its own, the reading observeGroupTargets makes.
func (w *retargetWorld) fresh(st *retargetState) bool {
	for i := range st.members {
		m := &st.members[i]
		if m.statusGen != m.irGen || m.annotation != st.gen {
			return false
		}
	}
	return true
}

// retargetRunID reads the run id off a run event's words ("run <id> ...").
func retargetRunID(message string) string {
	fields := strings.Fields(message)
	if len(fields) < 2 || fields[0] != "run" {
		return ""
	}
	return fields[1]
}

func (w *retargetWorld) memberIndex(comp v1beta1.ComponentType) int {
	for i, m := range w.cfg.members {
		if m.comp == comp {
			return i
		}
	}
	return -1
}

func (w *retargetWorld) note(st *retargetState, class, format string, args ...any) {
	st.events = append(st.events, runEvent{class: class, message: fmt.Sprintf(format, args...)})
}

// recordRunEvents reads the run layer's events of one pass into the
// state's record of runs: an open with the revisions the run recorded, a
// close with the outcome the status keeps and the event's words. A close
// for a new target must leave the replaced run's revisions in the status
// record with the outcome RolloutRunSuperseded.
func (w *retargetWorld) recordRunEvents(st *retargetState) {
	for _, e := range w.recorder.events {
		switch e.reason {
		case rolloutrun.EventRunOpened, rolloutrun.EventPlanRepinned:
			if e.reason == rolloutrun.EventPlanRepinned && !strings.Contains(e.message, "adopted") {
				continue
			}
			run := st.activeRun()
			if run == nil {
				w.note(st, "run-event-opened-with-no-run-in-status", "%s", e.message)
				continue
			}
			targets := map[v1beta1.ComponentType]int8{}
			for _, t := range run.TargetRevisions {
				targets[t.Component] = runRevisionOfHash(t.Revision)
			}
			st.runs = append(st.runs, retargetRun{id: retargetRunID(e.message), targets: targets, openedAt: st.rev})
		case rolloutrun.EventRunClosed:
			w.recordRunClose(st, e.message)
		}
	}
}

// recordRunClose matches a close event to the reported run it names and
// reads the status record beside it. A close event while the status keeps
// no record of a closed run is the twin of a run the status closed with
// no event, and is reported as a class of its own.
func (w *retargetWorld) recordRunClose(st *retargetState, message string) {
	id := retargetRunID(message)
	last := st.lastRun()
	for i := range st.runs {
		r := &st.runs[i]
		if r.id != id || r.closed != "" {
			continue
		}
		if last == nil {
			r.closed, r.reason = "no-record", message
			w.note(st, "run-event-closed-with-no-record-in-status", "the close says %q while the status keeps no record of a closed run", message)
			return
		}
		r.closed, r.reason = string(last.Outcome), message
		if last.Outcome != v1beta1.RolloutRunSuperseded && !strings.Contains(message, "new target revision") {
			return
		}
		named := map[v1beta1.ComponentType]int8{}
		for _, t := range last.TargetRevisions {
			named[t.Component] = runRevisionOfHash(t.Revision)
		}
		if last.Outcome != v1beta1.RolloutRunSuperseded || retargetTargetsWords(named) != retargetTargetsWords(r.targets) {
			w.note(st, "replaced-run-not-recorded-in-status", "the close says %q while the status record reads %s toward %s", message, last.Outcome, retargetTargetsWords(named))
		}
		return
	}
	w.note(st, "run-event-closed-a-run-no-event-opened", "%s", message)
}

// readFreshRun reads the run against replicas published fresh: a run that
// names a revision a member no longer targets should have closed on this
// pass, and a member whose published pair disagrees or whose rows trail
// its target with no run open should have opened one. The canary's hold
// (a rejected target held at the stable revision), a non-canary member
// resting at its partition and a plan that does not resolve are the readings the run
// layer excuses.
func (w *retargetWorld) readFreshRun(st *retargetState) {
	rejected := w.rejectedRevision(st)
	if run := st.activeRun(); run != nil {
		for _, t := range run.TargetRevisions {
			i := w.memberIndex(t.Component)
			if i < 0 {
				continue
			}
			recorded, published := runRevisionOfHash(t.Revision), st.members[i].statusTarget
			if recorded == published || (rejected != 0 && published == rejected && recorded == w.stableRevision(st)) {
				continue
			}
			w.note(st, "run-names-a-revision-the-fresh-replica-left", "run %s records %s=%d while the replica, published fresh, targets version %d", run.RunID, t.Component, recorded, published)
		}
		return
	}
	if strings.HasPrefix(st.planCondition(), "False/") {
		return
	}
	for i := range st.members {
		m := &st.members[i]
		if rejected != 0 && m.statusTarget == rejected {
			continue
		}
		var off int32
		for _, r := range m.published {
			if r.rev != m.statusTarget {
				off++
			}
		}
		if w.cfg.kind != retargetCanary && m.userPartition > 0 && off >= 1 && off <= m.userPartition {
			continue
		}
		if m.statusTarget != m.current || off > 0 {
			w.note(st, "no-run-over-a-fresh-divergence", "%s is published fresh with update revision %d, current %d and %d row(s) off the target, and no run is open (plan %s)", w.cfg.members[i].comp, m.statusTarget, m.current, off, st.planCondition())
		}
	}
}

// servicePass is one pass of the service controller: the run layer over
// the replicas as published, the canary bind when a run opened, the
// projector reaching every member in its order, then the group machine.
func (w *retargetWorld) servicePass(st *retargetState) {
	svc := st.service
	svc.Generation = st.gen
	svc.Spec = w.spec(st)
	fresh := w.fresh(st)
	reads := w.reader(st)
	w.recorder.events = nil
	w.clock = w.clock.Add(time.Second)
	if !w.trial {
		w.runPasses++
	}
	out, err := rolloutrun.Reconcile(w.ctx, rolloutrun.Inputs{Client: retargetClient{reads: reads}, Reader: reads, Recorder: w.recorder, ISVC: svc, Now: w.clock})
	if err != nil {
		w.t.Fatalf("run layer: %v (%s)", err, st)
	}
	w.recordRunEvents(st)
	if out.Opened {
		for _, g := range rollout.CanaryGroups(svc, rollout.Policies{}) {
			if err := canary.BindRun(w.ctx, nil, svc, g, canary.RunOpen{Adopted: out.Adopted, Closed: out.Closed}); err != nil {
				w.t.Fatalf("canary bind: %v (%s)", err, st)
			}
		}
	}
	if fresh {
		w.readFreshRun(st)
	}
	for _, i := range w.run.projectorOrder() {
		w.run.project(st.runState, i)
	}
	w.run.writeGroupEntries(st.runState, svc, w.clock)
}

// turnAround is the replica's rule for an attempt toward a revision its
// spec no longer names: a surge replacement is removed and the source
// keeps serving; a recreate's new pods are removed and the Instance is
// rebuilt at the target; a rebuild or a creation is moved to the target,
// unless the partition holds the Instance. A spec move is no failure, so
// no retry record is written.
func (w *retargetWorld) turnAround(st *retargetState, m *runMember) {
	held := w.run.heldIndices(m)
	for idx, r := range m.rows {
		if r.to == m.target || held[idx] {
			continue
		}
		switch r.kind {
		case runSurge:
			m.rows[idx] = runRow{kind: runReady, rev: r.rev, scaled: r.scaled}
		case runDrain:
			m.rows[idx] = runRow{kind: runRebuild, rev: r.rev, to: m.target, scaled: r.scaled}
		case runRebuild, runCreating:
			m.rows[idx].to = m.target
		default:
			continue
		}
		st.turnarounds++
	}
}

// replicaPass is one pass of one member's replica controller through the
// run-layer world. A rebuild follows the roll target only while the
// real plan gate admits a start on it; else it keeps the revision
// its row records. A rebuild that follows records the revision it builds
// toward on its row before its first pod exists, as the repair's own
// record does (workload/status/decision.go), so the status write carries
// it. Then a reading of what the pass built: a rebuild or a creation
// toward a revision no open run names is recorded on the state.
func (w *retargetWorld) replicaPass(st *retargetState, i int) {
	_, hold := w.run.pass(st.runState, i)
	st.holds[i] = hold
	m := &st.members[i]
	admits, _, _ := coordination.EvaluatePlanGate(st.service, w.cfg.members[i].comp)
	recorded := false
	for idx, r := range m.rows {
		if r.kind != runRebuild || r.to <= 0 || r.rev == r.to {
			continue
		}
		if admits {
			m.rows[idx].rev = r.to
		} else {
			m.rows[idx].to = r.rev
		}
		recorded = true
	}
	if recorded {
		w.run.passing = i
		w.run.writeStatus(i, m)
		w.run.passing = -1
	}
	comp := w.cfg.members[i].comp
	for _, r := range m.rows {
		if (r.kind != runRebuild && r.kind != runCreating) || r.to < 2 || w.openRunNames(st, comp, r.to) {
			continue
		}
		if !st.builtWithoutRun[r.to] {
			st.builtWithoutRun[r.to] = true
			w.noteBuiltWithoutRun(st, comp, r.to)
		}
	}
}

// openRunNames reports whether the open run records rev for comp.
func (w *retargetWorld) openRunNames(st *retargetState, comp v1beta1.ComponentType, rev int8) bool {
	run := st.activeRun()
	if run == nil {
		return false
	}
	for _, t := range run.TargetRevisions {
		if t.Component == comp && runRevisionOfHash(t.Revision) == rev {
			return true
		}
	}
	return false
}

// noteBuiltWithoutRun records a rebuild toward a revision no run names:
// the redirect of an attempt in flight, or a rebuild following a target
// under a run that names another revision; under a plan that does not
// resolve, a revision built regardless of the hold.
func (w *retargetWorld) noteBuiltWithoutRun(st *retargetState, comp v1beta1.ComponentType, rev int8) {
	if w.cfg.unresolvable {
		w.note(st, "rebuild-follows-a-revision-of-an-unresolvable-plan", "%s rebuilds an Instance toward version %d while the plan reads %s and no run is open", comp, rev, st.planCondition())
		return
	}
	w.note(st, "known-rebuild-follows-a-target-no-run-names", "%s rebuilds an Instance toward version %d while no run names it (plan %s)", comp, rev, st.planCondition())
}

// applyChange lands the operator's change on the spec: a push names the
// next revision, a rollback names version 1. A pushed revision a pod was
// built toward while no run named it, replaced before any run did, is
// recorded: nothing in the status will ever name it.
func (w *retargetWorld) applyChange(st *retargetState, mv retargetMove) {
	before := st.rev
	switch mv.kind {
	case "push":
		st.top++
		st.rev = st.top
		st.pushes--
	case "rollback":
		st.rev = 1
		st.rollbacks--
	}
	st.gen++
	if st.replaced(before) && st.builtWithoutRun[before] && len(st.runsNaming(before)) == 0 {
		w.note(st, "known-replaced-revision-built-under-no-run", "a pod was built toward version %d while no run named it, and the %s replaced version %d before a run opened: no run and no close will ever name it", before, mv.kind, before)
	}
}

// tick is one round of the controllers: the service pass, then every
// member's replica pass in the projector's order, with the operator's
// change landing at the phase it names: 0 before the service pass, 1 after
// it, 1+j after the j-th member's pass.
func (w *retargetWorld) tick(st *retargetState, change *retargetMove) {
	w.cur = st
	defer func() { w.cur = nil }()
	if change != nil && change.phase == 0 {
		w.applyChange(st, *change)
	}
	w.servicePass(st)
	if change != nil && change.phase == 1 {
		w.applyChange(st, *change)
	}
	for j, i := range w.run.projectorOrder() {
		w.replicaPass(st, i)
		if change != nil && change.phase == j+2 {
			w.applyChange(st, *change)
		}
	}
}

// phases are the points of a tick the operator's change may land at.
func (w *retargetWorld) phases() int {
	return len(w.cfg.members) + 1
}

// moves are everything that may happen next: a tick, an Instance coming
// up, a retry record moving, a pod, a gang or a gang member lost, an attempt
// failing, the operator's push or push back at every phase of the tick.
func (w *retargetWorld) moves(st *retargetState) (progress, adverse []retargetMove) {
	progress = append(progress, retargetMove{kind: "tick"})
	for i := range st.members {
		progress = append(progress, w.progressMoves(st, i)...)
		adverse = append(adverse, w.adverseMoves(st, i)...)
	}
	return progress, append(adverse, w.changeMoves(st)...)
}

// progressMoves are one member's moves that advance on their own: the
// first and the last Instance of each kind in flight comes up, a backoff
// ends, a retry record its rows outlived is removed.
func (w *retargetWorld) progressMoves(st *retargetState, i int) []retargetMove {
	m := &st.members[i]
	var out []retargetMove
	first, last := map[runRowKind]int{}, map[runRowKind]int{}
	for idx, r := range m.rows {
		if _, ok := first[r.kind]; !ok {
			first[r.kind] = idx
		}
		last[r.kind] = idx
	}
	for _, kind := range []runRowKind{runDrain, runSurge, runCreating, runRebuild} {
		idx, ok := first[kind]
		if !ok {
			continue
		}
		out = append(out, retargetMove{kind: "serve", member: i, idx: idx})
		if last[kind] != idx {
			out = append(out, retargetMove{kind: "serve", member: i, idx: last[kind]})
		}
	}
	switch {
	case m.retry == runRetryBackoff:
		out = append(out, retargetMove{kind: "backoff", member: i})
	case m.retry != runRetryNone && m.retry != runRetryHeld && !m.inFlight() && m.count(runFailedUnserving, runFailedSurge) == 0:
		out = append(out, retargetMove{kind: "prune", member: i})
	}
	return out
}

// adverseMoves are one member's costly moves: one attempt of each kind
// fails, one Ready Instance of each revision loses its pods or, in a gang,
// one member.
func (w *retargetWorld) adverseMoves(st *retargetState, i int) []retargetMove {
	m := &st.members[i]
	var out []retargetMove
	seenFail, seenLoss := map[runRowKind]bool{}, map[int8]bool{}
	for idx, r := range m.rows {
		switch r.kind {
		case runDrain, runSurge:
			if m.fails > 0 && !seenFail[r.kind] {
				seenFail[r.kind] = true
				out = append(out, retargetMove{kind: "fail", member: i, idx: idx})
			}
		case runReady:
			if m.losses > 0 && !seenLoss[r.rev] {
				seenLoss[r.rev] = true
				out = append(out, retargetMove{kind: "lose", member: i, idx: idx})
				if w.cfg.members[i].pods > 1 {
					out = append(out, retargetMove{kind: "lose-member", member: i, idx: idx})
				}
			}
		}
	}
	return out
}

// changeMoves are the operator's changes, each at every phase of the tick.
func (w *retargetWorld) changeMoves(st *retargetState) []retargetMove {
	var out []retargetMove
	for phase := 0; phase < w.phases(); phase++ {
		if st.pushes > 0 && st.top < runLastRevision {
			out = append(out, retargetMove{kind: "push", phase: phase})
		}
		if st.rollbacks > 0 && st.rev >= 2 {
			out = append(out, retargetMove{kind: "rollback", phase: phase})
		}
	}
	return out
}

// apply makes one move and runs one tick of the controllers over it.
func (w *retargetWorld) apply(st *retargetState, mv retargetMove) *retargetState {
	next := st.clone()
	next.path = append(next.path, runMove{kind: mv.kind, words: mv.String()})
	next.events = nil
	var change *retargetMove
	switch mv.kind {
	case "push", "rollback":
		change = &mv
	case "tick":
	default:
		w.worldMove(&next.members[mv.member], mv)
	}
	w.tick(next, change)
	return next
}

// worldMove is a move of the world on one member's rows: an Instance comes
// up on the revision it was moving toward, an attempt fails, a pod set or a
// gang member is lost.
func (w *retargetWorld) worldMove(m *runMember, mv retargetMove) {
	var r runRow
	if mv.idx < len(m.rows) {
		r = m.rows[mv.idx]
	}
	switch mv.kind {
	case "serve":
		m.rows[mv.idx] = runRow{kind: runReady, rev: r.to, scaled: r.scaled}
		if m.retry != runRetryNone && m.retry != runRetryHeld && m.retryRev == r.to {
			m.retry, m.attempts = runRetryNone, 0
		}
	case "fail":
		if r.kind == runSurge {
			m.rows[mv.idx].kind = runFailedSurge
		} else {
			m.rows[mv.idx].kind = runFailedUnserving
		}
		m.fails--
		if m.retry != runRetryBackoff && m.retry != runRetryHeld {
			m.attempts++
			m.retryRev = r.to
			m.retry = runRetryBackoff
			if m.attempts >= w.cfg.maxAttempts {
				m.retry = runRetryHeld
			}
		}
	case "lose":
		m.rows[mv.idx] = runRow{kind: runLost, rev: r.rev, scaled: r.scaled}
		m.losses--
	case "lose-member":
		m.rows[mv.idx] = runRow{kind: runShort, rev: r.rev, scaled: r.scaled}
		m.losses--
	case "backoff":
		m.retry = runRetryDue
	case "prune":
		m.retry, m.attempts = runRetryNone, 0
	}
}

// complete reports the end of the roll with nothing of a replaced revision
// left: every replica projected for the spec and its status written, every
// Instance Ready, and the run closed with every Instance on the spec's
// revision. A canary group also rests under its step: the run open names
// the spec's revision for every member and every Instance runs it or the
// stable revision the step holds. A plan that does not resolve rests with no run and
// every Instance Ready, whatever revision the rebuild followed.
func (w *retargetWorld) complete(st *retargetState) bool {
	if !w.fresh(st) {
		return false
	}
	for i := range st.members {
		for _, r := range st.members[i].rows {
			if r.kind != runReady {
				return false
			}
		}
	}
	if w.cfg.unresolvable {
		return st.activeRun() == nil && !w.rowOnAReplacedRevision(st)
	}
	if st.activeRun() == nil {
		return w.run.membersComplete(st.runState)
	}
	if w.cfg.kind != retargetCanary {
		return false
	}
	for _, t := range st.activeRun().TargetRevisions {
		if runRevisionOfHash(t.Revision) != st.rev {
			return false
		}
	}
	for i := range st.members {
		for _, r := range st.members[i].rows {
			if r.rev != st.rev && r.rev != w.stableRevision(st) {
				return false
			}
		}
	}
	return true
}

// rowOnAReplacedRevision reports an Instance that runs, or moves toward, a
// revision the operator pushed and then moved off.
func (w *retargetWorld) rowOnAReplacedRevision(st *retargetState) bool {
	for i := range st.members {
		for _, r := range st.members[i].rows {
			if st.replaced(r.rev) || (r.kind != runReady && st.replaced(r.to)) {
				return true
			}
		}
	}
	return false
}

// stepHoldsARowOffTheSpec reports a canary member whose step partition
// holds an Instance off the spec's revision: the hold the canary executor
// releases when it ends a canary the operator took back, which this model
// does not run. A failed surge replacement under such an Instance keeps
// its slot and holds the member's other starts behind the budget, the
// run-layer model's known class failed-replacement-under-a-held-instance.
func (w *retargetWorld) stepHoldsARowOffTheSpec(st *retargetState) bool {
	if w.cfg.kind != retargetCanary {
		return false
	}
	for i := range st.members {
		m := &st.members[i]
		held := w.run.heldIndices(m)
		for idx, r := range m.rows {
			if r.rev != st.rev && held[idx] {
				return true
			}
		}
	}
	return false
}

// stillKey is the state's key without the readings of its last pass, so a
// tick that changes nothing reads as the same state.
func (w *retargetWorld) stillKey(st *retargetState) string {
	events := st.events
	st.events = nil
	defer func() { st.events = events }()
	return st.key()
}

// quiet reports a state nothing moves on its own: no Instance to come up,
// no retry record to move, and a tick of the controllers that changes
// nothing.
func (w *retargetWorld) quiet(st *retargetState) bool {
	progress, _ := w.moves(st)
	if len(progress) > 1 {
		return false
	}
	trial := st.clone()
	passes, calls := w.runPasses, w.run.gateCalls
	w.trial = true
	w.tick(trial, nil)
	w.trial = false
	w.runPasses, w.run.gateCalls = passes, calls
	return w.stillKey(trial) == w.stillKey(st)
}

// retargetKnownClasses are the readings the explorer reports without
// failing, each in words: the engine's own rule, or a reading the model
// leaves open. Once a line is removed, its class fails the test if it
// comes back.
var retargetKnownClasses = map[string]string{
	"known-rebuild-follows-a-target-no-run-names":                      "an attempt in flight is redirected to a new target before the run retargets, and a rebuild with no pod yet follows a new target while a run recorded toward the previous one stands: the plan gate asks for a recorded run, not for one that names the target (workload/ops/update_recreate.go, update_surge.go, restart.go rebuildFollowsTarget)",
	"known-replaced-revision-built-under-no-run":                       "a push replaced within one projection of the replica (the push back or the next push landing before the replica published the pushed revision fresh and a run opened toward it) opens no run toward the pushed revision, so no run closes with the outcome RolloutRunSuperseded and no event names it; the pod a rebuild built toward it is turned around; the run that opens, if any, is the change's own and closes Completed (the status records no revision that was built but never ran under a run)",
	"known-canary-step-hold-stands-until-the-executor-ends-the-canary": "after a push back under a canary the step's partition still holds an Instance on the replaced revision, and a failed surge replacement under it keeps its slot: the canary executor (canary.Reconcile, rollTakenBack) ends a canary the operator took back and releases the hold, and this model does not run the executor",
}

// check reads every invariant at one state: the readings of the last pass,
// the budgets, and on a state nothing moves on its own: a run left open
// naming a replaced revision, a pod of a replaced revision left behind, a
// run the status closed with no close event, or a hold with no way out.
func (w *retargetWorld) check(st *retargetState) []runFinding {
	var out []runFinding
	add := func(class, format string, args ...any) {
		out = append(out, runFinding{class: class, message: fmt.Sprintf(format, args...), state: st.runState, picture: st.String()})
	}
	for _, e := range st.events {
		add(e.class, "%s", e.message)
	}
	for _, f := range w.run.budgetFindings(st.runState) {
		add(f.class, "%s", f.message)
	}
	if st.activeRun() == nil && len(st.openRuns()) > 0 {
		add("run-closed-in-status-with-no-close-event", "the status holds no run while %d run(s) the events opened never closed with an event: %s", len(st.openRuns()), st)
	}
	if w.complete(st) || !w.quiet(st) {
		return out
	}
	if run := st.activeRun(); run != nil {
		for _, t := range run.TargetRevisions {
			if st.replaced(runRevisionOfHash(t.Revision)) {
				add("run-open-naming-a-replaced-revision", "run %s still records %s=%d while the spec names version %d and nothing moves", run.RunID, t.Component, runRevisionOfHash(t.Revision), st.rev)
				return out
			}
		}
	}
	if w.rowOnAReplacedRevision(st) {
		class := "pod-of-a-replaced-revision-left-behind"
		if w.stepHoldsARowOffTheSpec(st) {
			class = "known-canary-step-hold-stands-until-the-executor-ends-the-canary"
		}
		add(class, "an Instance runs or moves toward a replaced revision while the spec names version %d and nothing moves (plan %s): %s", st.rev, st.planCondition(), w.run.publishedPicture(st.runState))
		return out
	}
	if w.stepHoldsARowOffTheSpec(st) {
		add("known-canary-step-hold-stands-until-the-executor-ends-the-canary", "the step's partition holds an Instance off version %d and nothing moves (plan %s): %s", st.rev, st.planCondition(), w.run.publishedPicture(st.runState))
		return out
	}
	class, message := w.run.classifyStopped(st.runState)
	add(class, "%s", message)
	return out
}

func (w *retargetWorld) record(rep *runReport, st *retargetState) {
	for _, f := range w.check(st) {
		rep.counts[f.class]++
		if _, ok := rep.findings[f.class]; !ok {
			rep.findings[f.class] = f
		}
	}
}

// explore walks one configuration breadth-first to a bound with a hash of
// visited states, then plays seeded runs that favour the moves finishing a
// roll, to reach the depths the walk does not.
func (w *retargetWorld) explore(maxStates, maxDepth, plays int, seed int64) runReport {
	rep := runReport{findings: map[string]runFinding{}, counts: map[string]int{}}
	depthFinding := func(st *retargetState) {
		rep.counts["depth"]++
		if _, ok := rep.findings["depth"]; !ok {
			rep.findings["depth"] = runFinding{class: "depth", message: "the roll did not end within the depth bound", state: st.runState, picture: st.String()}
		}
	}
	start := w.initial()
	w.tick(start, nil)
	visited := map[string]bool{start.key(): true}
	frontier := []*retargetState{start}
	for len(frontier) > 0 && rep.states < maxStates {
		st := frontier[0]
		frontier = frontier[1:]
		rep.states++
		rep.depth = max(rep.depth, len(st.path))
		w.record(&rep, st)
		if w.complete(st) {
			continue
		}
		if len(st.path) >= maxDepth {
			depthFinding(st)
			continue
		}
		progress, adverse := w.moves(st)
		for _, mv := range append(progress, adverse...) {
			child := w.apply(st, mv)
			if k := child.key(); !visited[k] {
				visited[k] = true
				frontier = append(frontier, child)
			}
		}
	}
	rng := rand.New(rand.NewSource(seed))
	for play := 0; play < plays; play++ {
		st := w.initial()
		w.tick(st, nil)
		rep.runs++
		for depth := 0; !w.complete(st); depth++ {
			w.record(&rep, st)
			progress, adverse := w.moves(st)
			if len(adverse) == 0 && w.quiet(st) {
				break
			}
			if depth >= 4*maxDepth {
				depthFinding(st)
				break
			}
			mv := progress[rng.Intn(len(progress))]
			if len(adverse) > 0 && rng.Intn(4) == 0 {
				mv = adverse[rng.Intn(len(adverse))]
			}
			st = w.apply(st, mv)
			rep.depth = max(rep.depth, len(st.path))
		}
		w.record(&rep, st)
	}
	return rep
}

// retargetMember is one member of a configuration: single pods or a gang,
// RecreatePod or SurgeThenDrain at its own budget.
func retargetMember(comp v1beta1.ComponentType, desired, pods int32, strategy v1beta1.UpdateStrategyType, budget string) runMemberConfig {
	return runMemberConfig{comp: comp, desired: desired, pods: pods, strategy: strategy, budget: budget}
}

// retargetConfigOf builds one configuration and names it.
func retargetConfigOf(kind retargetKind, members []runMemberConfig, budgets retargetBudgets, with func(*retargetConfig)) retargetConfig {
	cfg := retargetConfig{members: members, kind: kind, maxAttempts: 2, budgets: budgets}
	if kind == retargetCanary {
		cfg.steps = []int32{50, 100}
	}
	if with != nil {
		with(&cfg)
	}
	cfg.name = cfg.String()
	return cfg
}

// retargetConfigs lists the configurations to explore. The bounded mode
// runs one shape of gangs beside routers (two engine gangs of a
// leader and a worker beside two routers, one Instance rebuilding when the
// push lands) and one configuration of every other kind: the pair of an
// engine and a decoder under rollingUpdate with a protocol change, a
// blueGreen group of gangs, a canary over the pair and a plan that does
// not resolve. The full mode runs every kind over gangs of one, two, four
// and eight pods and one to four Instances, with and without the protocol
// change and the rebuilding Instance, with a router beside the pair, a
// second push and losses and failures thrown in.
func retargetConfigs(full bool) []retargetConfig {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	engine, decoder, router := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	budgets := retargetBudgets{pushes: 2, rollbacks: 1, losses: 1}
	broken := func(c *retargetConfig) { c.brokenAtPush = true }
	protocol := func(c *retargetConfig) { c.protocol = true }
	both := func(c *retargetConfig) { c.brokenAtPush, c.protocol = true, true }
	unresolvable := func(c *retargetConfig) { c.unresolvable, c.brokenAtPush = true, true }
	var out []retargetConfig
	add := func(cfg retargetConfig) { out = append(out, cfg) }
	add(retargetConfigOf(retargetRolling, []runMemberConfig{retargetMember(engine, 2, 2, recreate, "1"), retargetMember(router, 2, 1, surge, "1")}, budgets, broken))
	add(retargetConfigOf(retargetRolling, []runMemberConfig{retargetMember(engine, 4, 1, recreate, "1"), retargetMember(decoder, 2, 1, recreate, "1")}, budgets, protocol))
	add(retargetConfigOf(retargetBlueGreen, []runMemberConfig{retargetMember(engine, 3, 4, surge, "1"), retargetMember(decoder, 1, 8, surge, "1")}, budgets, broken))
	add(retargetConfigOf(retargetCanary, []runMemberConfig{retargetMember(engine, 2, 1, recreate, "1"), retargetMember(decoder, 2, 1, recreate, "1")}, budgets, nil))
	add(retargetConfigOf(retargetRolling, []runMemberConfig{retargetMember(engine, 2, 1, recreate, "1"), retargetMember(decoder, 1, 1, recreate, "1")}, budgets, unresolvable))
	kinds := []retargetKind{retargetRolling, retargetBlueGreen, retargetCanary}
	flags := []func(*retargetConfig){nil, broken, protocol, both}
	if !full {
		// One engine of every gang width and count under every kind, the
		// flags alternating, and the pair beside a router once per kind.
		for k, kind := range kinds {
			for p, pods := range []int32{1, 2, 4, 8} {
				for n := int32(1); n <= 4; n++ {
					strategy := []v1beta1.UpdateStrategyType{recreate, surge}[(p+int(n))%2]
					add(retargetConfigOf(kind, []runMemberConfig{retargetMember(engine, n, pods, strategy, "1")}, budgets, flags[(k+p+int(n))%len(flags)]))
				}
			}
			add(retargetConfigOf(kind, []runMemberConfig{retargetMember(engine, 4, 2, recreate, "1"), retargetMember(decoder, 2, 2, recreate, "1"), retargetMember(router, 2, 1, surge, "1")}, budgets, both))
		}
		return out
	}
	wide := retargetBudgets{pushes: 2, rollbacks: 1, losses: 2, fails: 1}
	for _, kind := range kinds {
		for _, pods := range []int32{1, 2, 4, 8} {
			for n := int32(1); n <= 4; n++ {
				for _, strategy := range []v1beta1.UpdateStrategyType{recreate, surge} {
					members := []runMemberConfig{retargetMember(engine, n, pods, strategy, "1")}
					pair := append(append([]runMemberConfig(nil), members...), retargetMember(decoder, max(n/2, 1), pods, strategy, "1"))
					for _, with := range flags {
						add(retargetConfigOf(kind, members, wide, with))
						add(retargetConfigOf(kind, pair, wide, with))
					}
				}
			}
		}
		add(retargetConfigOf(kind, []runMemberConfig{retargetMember(engine, 4, 2, recreate, "1"), retargetMember(decoder, 2, 2, recreate, "1"), retargetMember(router, 2, 1, surge, "1")}, wide, both))
		add(retargetConfigOf(kind, []runMemberConfig{retargetMember(engine, 3, 8, surge, "1"), retargetMember(decoder, 3, 4, surge, "1"), retargetMember(router, 1, 1, surge, "1")}, wide, broken))
	}
	for _, n := range []int32{1, 2, 4} {
		add(retargetConfigOf(retargetRolling, []runMemberConfig{retargetMember(engine, n, 2, recreate, "1"), retargetMember(decoder, n, 1, recreate, "1")}, wide, unresolvable))
		add(retargetConfigOf(retargetBlueGreen, []runMemberConfig{retargetMember(engine, n, 4, surge, "1")}, wide, unresolvable))
	}
	return out
}

// retargetWorkers is how many configurations run at once:
// OME_RUN_RETARGET_HUNT_WORKERS, else the run-layer model's count.
func retargetWorkers() int {
	if v, err := strconv.Atoi(os.Getenv("OME_RUN_RETARGET_HUNT_WORKERS")); err == nil && v > 0 {
		return v
	}
	return runWorkers()
}

// retargetExplore explores every configuration keep admits (and, when
// OME_RUN_RETARGET_HUNT_ONLY is set, whose name contains it) and reports
// each broken invariant with the shortest path to it. A finding of a class
// not listed in retargetKnownClasses fails the test.
func retargetExplore(t *testing.T, keep func(retargetConfig) bool) {
	t.Helper()
	full := os.Getenv("OME_RUN_RETARGET_HUNT") == "full"
	maxStates, maxDepth, plays := 8000, 40, 16
	if full {
		maxStates, maxDepth, plays = 24000, 64, 64
	}
	only := os.Getenv("OME_RUN_RETARGET_HUNT_ONLY")
	var configs []retargetConfig
	for _, cfg := range retargetConfigs(full) {
		if keep(cfg) && strings.Contains(cfg.name, only) {
			configs = append(configs, cfg)
		}
	}
	started := time.Now()
	summary := &runSummary{totals: map[string]int{}, configsWith: map[string]int{}, examples: map[string][]runExample{}}
	jobs := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < retargetWorkers(); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				cfg := configs[n]
				w := newRetargetWorld(t, cfg)
				rep := w.explore(maxStates, maxDepth, plays, int64(n+1))
				w.run.runPasses = w.runPasses
				summary.add(runConfig{name: cfg.name}, w.run, rep)
				if full {
					var parts []string
					for class, c := range rep.counts {
						parts = append(parts, fmt.Sprintf("%s=%d", class, c))
					}
					sort.Strings(parts)
					t.Logf("config %d/%d [%s]: %d states, depth %d: %s", n+1, len(configs), cfg.name, rep.states, rep.depth, strings.Join(parts, " "))
				}
			}
		}()
	}
	for n := range configs {
		jobs <- n
	}
	close(jobs)
	wg.Wait()
	t.Logf("%d configurations, %d states, %d random runs, %d gate questions, %d run passes, %s", len(configs), summary.states, summary.plays, summary.gateCalls, summary.runPasses, time.Since(started).Round(time.Second))
	runReportClasses(t, summary, retargetKnownClasses)
}

// TestRunRetarget_RollingAndBlueGreenGroupsReplaceAPushCleanly explores the
// rollingUpdate and blueGreen groups: a push replaced by a push back or a
// next push at every point of the controllers' steps, with lost pods, gangs
// and gang members, failed attempts and a plan that does not resolve.
func TestRunRetarget_RollingAndBlueGreenGroupsReplaceAPushCleanly(t *testing.T) {
	retargetExplore(t, func(c retargetConfig) bool { return c.kind != retargetCanary })
}

// TestRunRetarget_CanaryGroupsReplaceAPushCleanly explores the same space
// over a canary group resting under its first step.
func TestRunRetarget_CanaryGroupsReplaceAPushCleanly(t *testing.T) {
	retargetExplore(t, func(c retargetConfig) bool { return c.kind == retargetCanary })
}

// retargetParseMove reads one move back from the words retargetMove.String
// prints.
func retargetParseMove(s string) (retargetMove, error) {
	s = strings.TrimSpace(s)
	var mv retargetMove
	if s == "tick" {
		return retargetMove{kind: "tick"}, nil
	}
	if n, err := fmt.Sscanf(s, "%s at phase %d", &mv.kind, &mv.phase); err == nil && n == 2 {
		return mv, nil
	}
	if n, err := fmt.Sscanf(s, "%s member %d row %d", &mv.kind, &mv.member, &mv.idx); err == nil && n == 3 {
		return mv, nil
	}
	if n, err := fmt.Sscanf(s, "%s member %d", &mv.kind, &mv.member); err == nil && n == 2 {
		return mv, nil
	}
	return retargetMove{}, fmt.Errorf("cannot read move %q", s)
}

// TestRunRetarget_ReplayAPath replays a printed path on the configuration
// OME_RUN_RETARGET_HUNT_ONLY names (OME_RUN_RETARGET_HUNT_PATH holds the
// moves joined by " -> ") and prints the state and its readings after each
// move.
func TestRunRetarget_ReplayAPath(t *testing.T) {
	only, path := os.Getenv("OME_RUN_RETARGET_HUNT_ONLY"), os.Getenv("OME_RUN_RETARGET_HUNT_PATH")
	if only == "" {
		t.Skip("set OME_RUN_RETARGET_HUNT_ONLY to a configuration name and OME_RUN_RETARGET_HUNT_PATH to a path")
	}
	var cfg *retargetConfig
	for _, c := range retargetConfigs(os.Getenv("OME_RUN_RETARGET_HUNT") == "full") {
		if c.name == only {
			c := c
			cfg = &c
			break
		}
	}
	if cfg == nil {
		t.Fatalf("no configuration named %q", only)
	}
	w := newRetargetWorld(t, *cfg)
	st := w.initial()
	w.tick(st, nil)
	t.Logf("start: %s", st)
	for _, s := range strings.Split(path, "->") {
		if strings.TrimSpace(s) == "" {
			continue
		}
		mv, err := retargetParseMove(s)
		if err != nil {
			t.Fatal(err)
		}
		st = w.apply(st, mv)
		t.Logf("after %s: %s", mv, st)
		for _, f := range w.check(st) {
			t.Logf("  %s: %s", f.class, f.message)
		}
	}
	progress, adverse := w.moves(st)
	t.Logf("complete=%t quiet=%t progress=%v adverse=%d", w.complete(st), w.quiet(st), progress, len(adverse))
}

// TestRunRetarget_APushBackWithinOneProjectionOpensNoRunTowardThePushedRevision
// records the reading of the first bounded configuration. Two engine gangs
// of a leader and a worker beside two routers in one rollingUpdate group;
// engine Instance 1 is rebuilding after a loss, with no pod yet, when the
// operator pushes version 2. The service pass projects version 2 onto the
// replica, and the operator pushes version 1 back in the same second,
// before the replica published version 2 and before any run opened. With the finding listed,
// the rebuild follows version 2 the moment the projection carries it, is
// turned around when the push back is projected, and comes up on version
// 1; one run opens, toward version 1 for the Instance still off it, and
// closes Completed. In both readings no run ever names version 2 and no
// close reads RolloutRunSuperseded. With no finding listed: the rebuild
// keeps version 1 while no run is recorded, so the push back finds every
// Instance on version 1, nothing was built toward version 2 and no run
// opens at all.
func TestRunRetarget_APushBackWithinOneProjectionOpensNoRunTowardThePushedRevision(t *testing.T) {
	cfg := retargetConfigs(false)[0]
	w := newRetargetWorld(t, cfg)
	st := w.initial()
	w.tick(st, nil)
	if len(st.runs) != 0 {
		t.Fatalf("no run opens on the pass that projects the push, the replica not yet fresh; got %s", st)
	}
	reason, known := retargetKnownFindings["a-push-back-within-one-projection-builds-the-pushed-revision-under-no-run"]
	broken := st.members[0].rows[1]
	switch {
	case known && (broken.kind != runRebuild || broken.to != 2):
		t.Fatalf("with the finding listed, the open rebuild of engine Instance 1 follows version 2 before any run names it; got %s", st)
	case !known && (broken.kind != runRebuild || broken.to != 1):
		t.Fatalf("the open rebuild keeps version 1 while no run is recorded; got %s", st)
	}
	st = w.apply(st, retargetMove{kind: "rollback", phase: 0})
	for !w.complete(st) {
		progress, _ := w.moves(st)
		st = w.apply(st, progress[len(progress)-1])
	}
	if len(st.runsNaming(2)) != 0 {
		t.Fatalf("no run names version 2; got %s", st)
	}
	for _, r := range st.runs {
		if r.closed == string(v1beta1.RolloutRunSuperseded) {
			t.Fatalf("no close reads RolloutRunSuperseded; got %s", st)
		}
	}
	if !known {
		if st.builtWithoutRun[2] || len(st.runs) != 0 {
			t.Fatalf("nothing is built toward version 2 and no run opens; got %s", st)
		}
		return
	}
	t.Logf("listed finding: %s", reason)
	if len(st.runs) != 1 || st.runs[0].closed != string(v1beta1.RolloutRunCompleted) || st.runs[0].targets[v1beta1.EngineComponent] != 1 {
		t.Fatalf("with the finding listed, one run opens toward version 1 and closes Completed; got %s", st)
	}
	if !st.builtWithoutRun[2] {
		t.Fatalf("with the finding listed, a pod is built toward version 2 while no run names it; got %s", st)
	}
}

// retargetKnownFindings are the findings of this model's story locks that
// the tests below accept, each with the reading that holds it: the lock requires
// the listed reading while its line is here, and the behaviour the story asks
// for when its line is absent.
var retargetKnownFindings = map[string]string{}

// TestRunRetarget_AnUnresolvablePlanKeepsAPodOfTheReplacedRevision tells the
// story of a plan that does not resolve. Two engine Instances and one decoder in
// one rollout group whose policy reference names no RolloutPolicy; engine
// Instance 1 is rebuilding after a loss, with no pod yet, when the operator
// pushes version 2. The run layer holds the plan (RolloutPlanReady False):
// the plan gate holds every start toward version 2 and the previous
// revision keeps serving. With the finding listed, the rebuild follows version 2 all the same and
// comes up serving it. The operator pushes version 1 back: the Instance on
// version 2 stays there, held by the plan gate, until the plan is repaired.
// With no finding listed: the rebuild obeys the plan gate as every start
// does, so no Instance serves the held revision and the push back finds
// every Instance on version 1.
func TestRunRetarget_AnUnresolvablePlanKeepsAPodOfTheReplacedRevision(t *testing.T) {
	cfg := retargetConfigs(false)[4]
	if !cfg.unresolvable {
		t.Fatalf("the fifth bounded configuration is the plan that does not resolve; got %s", cfg.name)
	}
	w := newRetargetWorld(t, cfg)
	st := w.initial()
	w.tick(st, nil)
	st = w.apply(st, retargetMove{kind: "tick"})
	if !strings.HasPrefix(st.planCondition(), "False/") {
		t.Fatalf("the run layer holds the plan; got %s", st)
	}
	broken := st.members[0].rows[1]
	reason, known := retargetKnownFindings["an-unresolvable-plan-keeps-a-pod-of-the-replaced-revision"]
	switch {
	case known && (broken.kind != runRebuild || broken.to != 2):
		t.Fatalf("with the finding listed, the rebuild follows the held revision; got %s", st)
	case !known && (broken.kind != runRebuild || broken.to != 1):
		t.Fatalf("under a plan that does not resolve the rebuild renders the revision the Instance ran; got %s", st)
	}
	st = w.apply(st, retargetMove{kind: "serve", member: 0, idx: 1})
	st = w.apply(st, retargetMove{kind: "rollback", phase: 0})
	for round := 0; round < 8 && !w.quiet(st); round++ {
		progress, _ := w.moves(st)
		st = w.apply(st, progress[len(progress)-1])
	}
	if !known {
		if !w.complete(st) || w.rowOnAReplacedRevision(st) {
			t.Fatalf("the push back finds every Instance on version 1 with nothing of version 2 left; got %s", st)
		}
		return
	}
	t.Logf("listed finding: %s", reason)
	if !w.quiet(st) || st.members[0].rows[1].rev != 2 || st.holds[0] == nil || st.holds[0].gate != v1beta1.RolloutHoldGatePlan {
		t.Fatalf("with the finding listed, the Instance stays on version 2 behind the plan gate after the push back; got %s", st)
	}
}
