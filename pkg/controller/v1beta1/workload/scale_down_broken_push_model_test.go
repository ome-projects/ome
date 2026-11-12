package workload_test

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the production engine of a prefill/decode service while
// the operator scales a side down during a push that cannot land: the new
// revision's image cannot be pulled, or its runner crashes on start. It
// runs on the world of scale_beside_a_roll_model_test.go: one
// workload.Reconcile loop per Component on the recovery harness, the
// coordination gates wired between the Components, the plan's choice of
// which Instance a scale-down removes (BuildPlan, scaleDownRank) and the
// scale-down wave (ops.DeleteBatch) the engine's own.
//
// Every configuration (a reference shape, pods per Instance, a strategy
// per side at ten percent, the sides in one rolling group or in none, the
// kubelet stopping a deleted pod at once or two passes later as the slower
// scheduler twin does) is pushed from version 1 to the broken revision
// three ways: both sides to an image no pod can pull, both sides to a
// runner that crashes on start, and the decoder alone to the unpullable
// image beside a sound version 2 on the prefill side. The moments of that
// push are kept: the first replacement standing with no failure recorded
// yet, the first attempt failed, a later attempt in flight, the retry
// schedule spent. At each moment the operator scales a side down by one,
// to one, by a step or half, or both sides at once, and the roll is played
// to its end; in full mode the operator then corrects the push and the
// roll must finish at the new counts. Seeded random runs mix two scales
// and the correction into one push.
//
// At every tick after the scale the model reads the rows the wave took
// against the rows that stay: a sound serving Instance must never leave
// while an Instance on the failing revision (a Failed row, or a row whose
// revision the retry schedule reads as failing) stays; a Component whose
// roll is open must say so in its status (a hold, a retry record with an
// attempt, a Failed row or an open attempt); the Ready and serving counts
// the replica publishes must agree with its pods. At the held moment the
// scale must leave the Held record standing. The scale world's own checks
// run beside these: the budgets, the version mix under a ratio rule, a
// source kept under its surge, and at every settled state the roll must be
// complete or held with the hold named, with every side at its count.
// OME_BROKEN_PUSH_SCALE_HUNT=full widens the configurations, the moves and
// the bounds; OME_BROKEN_PUSH_SCALE_HUNT_ONLY=<text> keeps the
// configurations whose name carries the text.

const (
	brokenPushFullEnv = "OME_BROKEN_PUSH_SCALE_HUNT"
	brokenPushOnlyEnv = "OME_BROKEN_PUSH_SCALE_HUNT_ONLY"
	// brokenPushStopLag is how many kubelet passes a deleted pod keeps
	// running on the slower scheduler twin before the kubelet reports it
	// stopped, so the rows the wave is deleting outlive the scale's settle
	// by a few passes.
	brokenPushStopLag = 2
	// brokenPushReportTicks is how many ticks in a row a Component whose
	// roll is open may stand with nothing in its status that says why
	// before the model reads it as a push not reported: one pass between
	// an attempt ending and the next being admitted says nothing yet.
	brokenPushReportTicks = 2
)

// brokenPushWay is how the push is broken.
type brokenPushWay uint8

const (
	// wayUnpullable pushes both sides to an image no pod can pull.
	wayUnpullable brokenPushWay = iota
	// wayCrashStart pushes both sides to a runner that crashes on start.
	wayCrashStart
	// wayMixed pushes the decoder to the unpullable image and the prefill
	// side to version 2 (the scale world's held roll).
	wayMixed
)

var brokenPushWayNames = [...]string{"unpullable", "crash-start", "mixed"}

func (y brokenPushWay) String() string { return brokenPushWayNames[y] }

// brokenPushMoment is where in the broken push the scale lands.
type brokenPushMoment uint8

const (
	// pushAtPending: the first replacement stands and no failure is
	// recorded yet; the revision is not read as failing.
	pushAtPending brokenPushMoment = iota
	// pushAtFailed: the first attempt failed; a Failed row or a Backoff
	// record names the revision.
	pushAtFailed
	// pushAtRetrying: a later attempt is in flight after a failure.
	pushAtRetrying
	// pushAtHeld: the retry schedule is spent.
	pushAtHeld
)

var brokenPushMomentNames = [...]string{"pending", "failed", "retrying", "held"}

var brokenPushMoments = []brokenPushMoment{pushAtPending, pushAtFailed, pushAtRetrying, pushAtHeld}

func (m brokenPushMoment) String() string { return brokenPushMomentNames[m] }

// brokenPushConfig is one configuration: a scale world configuration, the
// sides in one rolling group or in none, and the kubelet's stop timing.
type brokenPushConfig struct {
	scaleConfig
	ungrouped bool
	slowStop  bool
}

func (c brokenPushConfig) String() string {
	name := c.scaleConfig.String()
	if c.ungrouped {
		name += "+ungrouped"
	}
	if c.slowStop {
		name += "+slowstop"
	}
	return name
}

// brokenPushMoveKind is the operator's scale-down.
type brokenPushMoveKind uint8

const (
	downByOne brokenPushMoveKind = iota
	downToOne
	downByStep
	downByHalf
	bothByOne
	bothToOne
)

var brokenPushMoveKindNames = [...]string{"down by one", "down to one", "down by a step", "down by half", "both sides down by one", "both sides down to one"}

// brokenPushMove is one scale-down at one moment: the kind and, for the
// one-side kinds, the position of the side in w.sides.
type brokenPushMove struct {
	kind brokenPushMoveKind
	side int
}

func (m brokenPushMove) String() string {
	if m.kind == bothByOne || m.kind == bothToOne {
		return brokenPushMoveKindNames[m.kind]
	}
	return fmt.Sprintf("side %d %s", m.side, brokenPushMoveKindNames[m.kind])
}

// rowReading is one row as the model read it before a pass: sound and
// serving, on the failing revision, a source whose replacement is coming
// up, or a rebuild in progress.
type rowReading struct {
	sound    bool
	failing  bool
	surging  bool
	rebuild  bool
	leaving  bool
	deleting bool
	// surgeStep is the scale world's own reading of a surging source: an
	// update at the Surge step of a source that has not failed.
	surgeStep bool
}

// brokenPushModel runs the scale-downs over one configuration of the scale
// world.
type brokenPushModel struct {
	w    *scaleWorld
	cfg  brokenPushConfig
	full bool
	way  brokenPushWay
	// bound is the most ticks one play may take; budget the most ticks
	// one configuration may spend, the plays left past it counted as
	// skipped so a slower machine reads the same first plays.
	bound, budget int
	// broken names the sides whose image the push broke.
	broken map[*scaleSide]bool
	// prev is each side's rows as the model read them before the last
	// tick, by index.
	prev map[*scaleSide]map[int32]rowReading
	// unreported counts, per side, the ticks in a row an open roll said
	// nothing about itself.
	unreported map[*scaleSide]int
	// releases counts the held records the model removed itself, as the
	// release annotation does.
	releases int
	// choices counts what the wave took, by kind, for the report.
	choices map[string]int
	probes  int
	skipped int
	runs    int
}

func newBrokenPushModel(w *scaleWorld, cfg brokenPushConfig, full bool) *brokenPushModel {
	budget := 6000
	if full {
		budget = 60000
	}
	return &brokenPushModel{w: w, cfg: cfg, full: full, bound: scaleTickBound(full), budget: budget,
		broken: map[*scaleSide]bool{}, prev: map[*scaleSide]map[int32]rowReading{}, unreported: map[*scaleSide]int{},
		choices: map[string]int{}}
}

// prepare shapes the world the configuration names: the members in no
// rollout group, and the kubelet's slower stop.
func (m *brokenPushModel) prepare() {
	w := m.w
	if m.cfg.ungrouped {
		w.isvc.Spec.Rollout = &v1beta1.RolloutSpec{}
		w.recordPlan()
	}
	if m.cfg.slowStop {
		for _, s := range w.sides {
			s.h.stopLag = brokenPushStopLag
		}
	}
}

// pushBroken points the prefill and decode sides at image and records the
// plan, as a push of the service does; the router keeps its revision, as
// it does in the reference cases.
func (m *brokenPushModel) pushBroken(image string, text string) {
	w := m.w
	m.broken = map[*scaleSide]bool{}
	for _, s := range w.sides {
		if s.comp == workloadtypes.ComponentRouter {
			continue
		}
		s.image = image
		m.broken[s] = true
		w.render(s)
	}
	w.recordPlan()
	w.path = append(w.path, text)
}

// push breaks the push the way names.
func (m *brokenPushModel) push(way brokenPushWay) {
	w := m.w
	m.way = way
	switch way {
	case wayUnpullable:
		m.pushBroken(badImage, "push an image no pod can pull on both sides")
	case wayCrashStart:
		m.pushBroken(crashStartImage, "push a runner that crashes on start on both sides")
	case wayMixed:
		w.pushHeld()
		m.broken = map[*scaleSide]bool{w.heldSide(): true}
		if w.cfg.settings.firstSide == v1beta1.DecoderComponent {
			m.broken[w.prefill()] = true
		}
	}
}

// watched is the side the moments are read on: the decoder, or the
// engine of an engine-only service.
func (m *brokenPushModel) watched() *scaleSide { return m.w.heldSide() }

// attemptToward reports whether the row carries an Update attempt toward
// the revision.
func attemptToward(r workloadtypes.InstanceStatus, rev string) bool {
	return r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationUpdate && r.Operation.TargetRevision == rev
}

// failedToward reports whether the row is Failed with the revision as the
// target of the attempt that failed.
func failedToward(r workloadtypes.InstanceStatus, rev string) bool {
	return r.Phase == workloadtypes.InstancePhaseFailed && (r.TargetRevision == rev || attemptToward(r, rev))
}

// momentOf reads the furthest moment of the broken push a side has
// reached, and whether it has reached any. A Failed row keeps the
// attempt that ended on it, which is not an attempt in flight.
func (m *brokenPushModel) momentOf(s *scaleSide) (brokenPushMoment, bool) {
	target := s.h.target.Name
	blk := s.h.findBlock(target)
	if blk != nil && blk.State == workloadtypes.RetryBlockHeld {
		return pushAtHeld, true
	}
	failed := blk != nil && (blk.State == workloadtypes.RetryBlockBackoff || blk.LastFailureAt != nil || blk.AttemptsStarted >= 2)
	open := false
	for _, r := range m.w.rows(s) {
		if failedToward(r, target) {
			failed = true
			continue
		}
		if attemptToward(r, target) {
			open = true
		}
	}
	switch {
	case failed && open:
		return pushAtRetrying, true
	case failed:
		return pushAtFailed, true
	case open:
		return pushAtPending, true
	}
	return pushAtPending, false
}

// captureMoments plays the broken push from idle and keeps a snapshot at
// the first tick of each moment on the watched side; the held moment is
// the settled end with the schedule spent. A push that never spends the
// schedule within the bound is counted under its own class.
func (m *brokenPushModel) captureMoments(idle *scaleSnapshot, way brokenPushWay) map[brokenPushMoment]*scaleSnapshot {
	w := m.w
	w.restore(idle)
	m.push(way)
	snaps := map[brokenPushMoment]*scaleSnapshot{}
	settling := w.startSettling()
	settled := false
	for i := 0; i < m.bound && !settled; i++ {
		settled = settling.observe(w, w.tick())
		moment, reached := m.momentOf(m.watched())
		if !reached || moment == pushAtHeld || snaps[moment] != nil {
			continue
		}
		w.path = append(w.path, "at "+moment.String())
		snaps[moment] = w.snapshot()
		w.counts["moment-"+moment.String()]++
		w.path = w.path[:len(w.path)-1]
	}
	if moment, reached := m.momentOf(m.watched()); settled && reached && moment == pushAtHeld {
		w.path = append(w.path, "at held")
		snaps[pushAtHeld] = w.snapshot()
		w.counts["moment-held"]++
		w.path = w.path[:len(w.path)-1]
	} else {
		w.note("held-not-reached", "the %s push did not spend the retry schedule within the bound: %s", way, w.picture())
	}
	return snaps
}

// sideIndex is the position of a side in w.sides.
func (m *brokenPushModel) sideIndex(s *scaleSide) int {
	for i, side := range m.w.sides {
		if side == s {
			return i
		}
	}
	return 0
}

// brokenSides names the sides whose image the push broke, read off the
// spec each side rolls on.
func (m *brokenPushModel) brokenSides() []*scaleSide {
	var out []*scaleSide
	for _, s := range m.w.sides {
		if s.image == badImage || s.image == crashStartImage {
			out = append(out, s)
		}
	}
	return out
}

// moves lists the scale-downs tried from a moment: the watched side by
// one and to one, the other side by one and both sides by one; in full
// mode the watched side by a step and by half, the other side to one and
// both sides to one.
func (m *brokenPushModel) moves() []brokenPushMove {
	watched := m.sideIndex(m.watched())
	out := []brokenPushMove{{kind: downByOne, side: watched}, {kind: downToOne, side: watched}}
	other := -1
	if m.w.decode() != nil {
		other = 1 - watched
	}
	if other >= 0 {
		out = append(out, brokenPushMove{kind: downByOne, side: other}, brokenPushMove{kind: bothByOne})
	}
	if m.full {
		out = append(out, brokenPushMove{kind: downByStep, side: watched}, brokenPushMove{kind: downByHalf, side: watched})
		if other >= 0 {
			out = append(out, brokenPushMove{kind: downToOne, side: other}, brokenPushMove{kind: bothToOne})
		}
	}
	return out
}

// countAfter is the count a side is asked for by a kind of scale-down.
func (m *brokenPushModel) countAfter(s *scaleSide, kind brokenPushMoveKind) int32 {
	n := s.h.replicas
	switch kind {
	case downByOne, bothByOne:
		return n - 1
	case downToOne, bothToOne:
		return 1
	case downByStep:
		return n - m.w.stepOf(n)
	case downByHalf:
		return n - n/2
	}
	return n
}

// scaleSide writes a side's new count: through the anchor when a scaling
// policy sizes the decoder, which a direct write on the follower would
// not hold.
func (m *brokenPushModel) scaleSide(s *scaleSide, n int32) {
	w := m.w
	if w.policy != nil && s.comp == workloadtypes.ComponentDecoder {
		w.setReplicas(w.prefill(), w.prefill().h.replicas-(s.h.replicas-n))
		return
	}
	w.setReplicas(s, n)
}

// applyMove makes one scale-down on the world.
func (m *brokenPushModel) applyMove(mv brokenPushMove) {
	w := m.w
	w.path = append(w.path, mv.String())
	if mv.kind == bothByOne || mv.kind == bothToOne {
		for _, s := range []*scaleSide{w.prefill(), w.decode()} {
			if s == nil {
				continue
			}
			if w.policy != nil && s.comp == workloadtypes.ComponentDecoder {
				continue
			}
			w.setReplicas(s, m.countAfter(s, mv.kind))
		}
		return
	}
	s := w.sides[mv.side]
	m.scaleSide(s, m.countAfter(s, mv.kind))
}

// readRows classifies a side's rows as the plan reads them before a
// pass: sound and serving (Ready, no operation open, the whole pod set in
// rotation, the revision not read as failing), on the failing revision
// (Failed toward the target, or on a revision the retry schedule reads as
// failing), a source whose SurgeThenDrain replacement is coming up, a
// rebuild in progress, a row that is not an Instance of the count, and a
// row the delete wave owns.
func (m *brokenPushModel) readRows(s *scaleSide) map[int32]rowReading {
	w := m.w
	byIndex := w.livePodsByIndex(s)
	target := s.h.target.Name
	out := map[int32]rowReading{}
	for _, r := range w.rows(s) {
		row := r
		failing := failedToward(row, target) || workloadtypes.RowOnFailingRevision(&row, s.h.blocks)
		update := row.Operation != nil && row.Operation.Type == workloadtypes.InstanceOperationUpdate
		surging := row.Phase == workloadtypes.InstancePhaseUpdating && update &&
			(row.Operation.Step == workloadtypes.UpdateStepSurge || row.Operation.Step == workloadtypes.UpdateStepSurgeDrain || row.Operation.Step == workloadtypes.UpdateStepSurgeDrainSettle)
		out[row.Index] = rowReading{
			sound:     row.Phase == workloadtypes.InstancePhaseReady && row.Operation == nil && !failing && w.servingSet(s, byIndex[row.Index]),
			failing:   failing,
			surging:   surging,
			rebuild:   workloadtypes.RebuildComingUp(&row),
			leaving:   replacementOrLeaving(row),
			deleting:  workloadtypes.Owner(&row) == workloadtypes.OwnerDelete,
			surgeStep: update && row.Operation.Step == workloadtypes.UpdateStepSurge && row.Phase != workloadtypes.InstancePhaseFailed,
		}
	}
	return out
}

// beginReadings takes every side's rows as they stand when the operator's
// move lands, so the next passes' choices are read against them.
func (m *brokenPushModel) beginReadings() {
	w := m.w
	for _, s := range w.sides {
		m.prev[s] = m.readRows(s)
		m.unreported[s] = 0
	}
}

// checkSelection reads what the last pass took on every side, against
// the rows as the model read them before it.
func (m *brokenPushModel) checkSelection() {
	for _, s := range m.w.sides {
		before := m.prev[s]
		now := m.readRows(s)
		m.prev[s] = now
		if before != nil {
			m.readSideSelection(s, before, now)
		}
	}
}

// readSideSelection compares one side's rows before and after a pass: a
// row that became the delete wave's or left the rows was taken. A sound
// serving Instance taken while an Instance on the failing revision stays
// is the finding this model is for; the staying Instance's attempt in
// flight or not names the class.
func (m *brokenPushModel) readSideSelection(s *scaleSide, before, now map[int32]rowReading) {
	var removedSound, staysFailed, staysRetrying []int32
	surgingBefore := 0
	for _, was := range before {
		if was.surgeStep {
			surgingBefore++
		}
	}
	countBelow := int(s.h.replicas) < surgingBefore
	for idx, was := range before {
		after, present := now[idx]
		switch {
		case !present || (after.deleting && !was.deleting):
			if m.countRemoval(was, countBelow) {
				removedSound = append(removedSound, idx)
			}
		case after.deleting || !was.failing || was.leaving:
		case m.attemptOpen(s, idx):
			staysRetrying = append(staysRetrying, idx)
		default:
			staysFailed = append(staysFailed, idx)
		}
	}
	sortIndices(removedSound)
	sortIndices(staysFailed)
	sortIndices(staysRetrying)
	if len(removedSound) == 0 {
		return
	}
	w := m.w
	if len(staysFailed) > 0 {
		w.note("serving-removed-while-a-failed-one-stays", "%s: the scale-down takes serving Instance(s) %v while Instance(s) %v, failed or idle on the failing revision, stay: %s", s.comp, removedSound, staysFailed, w.picture())
	}
	if len(staysRetrying) > 0 {
		w.note("serving-removed-while-a-retry-stays", "%s: the scale-down takes serving Instance(s) %v while Instance(s) %v retry an attempt on the failing revision: %s", s.comp, removedSound, staysRetrying, w.picture())
	}
}

// countRemoval counts a row the wave took by what it was, for the
// report, and reports whether it was a sound serving Instance. A source
// the scale world read as surging and the plan's own rule took (its
// replacement retried on the failing revision, or the asked count below
// the surging sources) moves from the scale world's
// finding class into the expected one.
func (m *brokenPushModel) countRemoval(was rowReading, countBelow bool) bool {
	switch {
	case was.leaving || was.deleting:
	case was.failing && was.surging:
		m.choices["took a source retrying on the failing revision"]++
		if was.surgeStep {
			m.expectSurgeRemoval("retrying-source-removed")
		}
	case was.failing:
		m.choices["took an Instance on the failing revision"]++
	case was.surging && countBelow:
		m.choices["took a source under its surge, the count below the surging sources"]++
		if was.surgeStep {
			m.expectSurgeRemoval("source-removed-count-below-surging")
		}
	case was.surging:
		m.choices["took a source under its surge"]++
	case was.rebuild:
		m.choices["took a rebuild in progress"]++
	case was.sound:
		m.choices["took a sound serving Instance"]++
		return true
	default:
		m.choices["took another row"]++
	}
	return false
}

// expectSurgeRemoval moves one count of the scale world's reading of a
// source removed under its surge, taken at this tick, into the expected
// class; the scale world's example goes with its last count.
func (m *brokenPushModel) expectSurgeRemoval(class string) {
	w := m.w
	const finding = "source-removed-under-its-surge"
	if w.counts[finding] <= 0 {
		return
	}
	w.counts[finding]--
	w.counts[class]++
	if w.counts[finding] == 0 {
		delete(w.counts, finding)
		delete(w.findings, finding)
	}
}

// sortIndices orders Instance indices ascending, in place.
func sortIndices(indices []int32) {
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
}

// attemptOpen reports whether the row at idx carries an Update attempt in
// flight now.
func (m *brokenPushModel) attemptOpen(s *scaleSide, idx int32) bool {
	for _, r := range m.w.rows(s) {
		if r.Index == idx {
			return r.Phase != workloadtypes.InstancePhaseFailed && attemptToward(r, s.h.target.Name)
		}
	}
	return false
}

// checkPushReported holds the status of a side whose roll is open to
// saying why: a hold the pass recorded, a retry record with an attempt
// started, a Failed row, an attempt open toward the target, or a row the
// delete wave is draining, since the scale-down runs before the update
// pass and is visible work. Two ticks in a row with none of them is a
// push the operator cannot see.
func (m *brokenPushModel) checkPushReported() {
	w := m.w
	for _, s := range m.brokenSides() {
		if w.sideComplete(s) || s.paused {
			m.unreported[s] = 0
			continue
		}
		target := s.h.target.Name
		said := s.h.hold != nil
		if blk := s.h.findBlock(target); blk != nil && (blk.AttemptsStarted > 0 || blk.State != workloadtypes.RetryBlockRetryInProgress) {
			said = true
		}
		for _, r := range w.rows(s) {
			row := r
			if row.Phase == workloadtypes.InstancePhaseFailed || attemptToward(row, target) || workloadtypes.Owner(&row) == workloadtypes.OwnerDelete {
				said = true
			}
		}
		if said {
			m.unreported[s] = 0
			continue
		}
		m.unreported[s]++
		if m.unreported[s] == brokenPushReportTicks {
			w.note("push-not-reported", "%s: the roll is open and the status names no hold, no retry record, no Failed row and no attempt for %d ticks: %s", s.comp, brokenPushReportTicks, w.picture())
		}
	}
}

// podCounts reads the band the published Ready and serving counts of a
// side must fall in. The low end counts the settled Instances (Ready with
// no operation open, not leaving) whose live pods are Ready and in the
// serving gate; the high end counts every row with a pod set whose
// containers pass their probes, or whose serving gate is set, the pods
// being deleted counted in. An Instance counts when at least its pod count of pods
// qualifies: a surge source keeps its old pod beside the replacement.
func (m *brokenPushModel) podCounts(s *scaleSide) (lowReady, highReady, lowServing, highServing int32) {
	w := m.w
	live := w.livePodsByIndex(s)
	all := query.BucketPodsByInstanceIdx(w.pods(s))
	per := w.podsPerInstance(s)
	count := func(pods []*corev1.Pod, ready, serving func(*corev1.Pod) bool) (bool, bool) {
		var readyPods, servingPods int32
		for _, pod := range pods {
			if ready(pod) {
				readyPods++
			}
			if serving(pod) {
				servingPods++
			}
		}
		return readyPods >= per, servingPods >= per
	}
	podReady := func(pod *corev1.Pod) bool { return podConditionTrue(pod, corev1.PodReady) }
	podServing := func(pod *corev1.Pod) bool {
		return podConditionTrue(pod, corev1.PodReady) && podreadiness.IsServing(pod)
	}
	processUp := func(pod *corev1.Pod) bool {
		return podConditionTrue(pod, corev1.ContainersReady) || podConditionTrue(pod, corev1.PodReady)
	}
	for _, r := range w.rows(s) {
		row := r
		kept := workloadtypes.Owner(&row) != workloadtypes.OwnerDelete && row.Phase == workloadtypes.InstancePhaseReady && row.Operation == nil
		if ready, serving := count(live[row.Index], podReady, podServing); kept {
			if ready {
				lowReady++
			}
			if serving {
				lowServing++
			}
		}
		if ready, serving := count(all[row.Index], processUp, podreadiness.IsServing); true {
			if ready {
				highReady++
			}
			if serving {
				highServing++
			}
		}
	}
	return lowReady, highReady, lowServing, highServing
}

// checkCounters holds the counts the replica publishes to the pods: the
// Instance count to the rows, the Ready and serving counts inside the
// band the pods allow (podCounts).
func (m *brokenPushModel) checkCounters() {
	w := m.w
	for _, s := range w.sides {
		rows := w.rows(s)
		if len(rows) == 0 {
			continue
		}
		_, counters := s.h.publication()
		lowReady, highReady, lowServing, highServing := m.podCounts(s)
		if counters.Replicas != int32(len(rows)) {
			w.note("replica-count-off", "%s publishes %d replicas for %d rows: %s", s.comp, counters.Replicas, len(rows), w.picture())
		}
		if counters.ReadyReplicas < lowReady || counters.ReadyReplicas > highReady {
			w.note("ready-count-off", "%s publishes %d Ready Instances while its pods allow %d to %d: %s", s.comp, counters.ReadyReplicas, lowReady, highReady, w.picture())
		}
		if counters.ServingReplicas < lowServing || counters.ServingReplicas > highServing {
			w.note("serving-count-off", "%s publishes %d serving Instances while its pods allow %d to %d: %s", s.comp, counters.ServingReplicas, lowServing, highServing, w.picture())
		}
	}
}

// playOutReading ticks until the world has settled or the bound is
// reached, reading the wave's choices, the status and the counters after
// every tick; it reports whether the world settled.
func (m *brokenPushModel) playOutReading() bool {
	w := m.w
	settling := w.startSettling()
	for i := 0; i < m.bound; i++ {
		key := w.tick()
		m.checkSelection()
		m.checkPushReported()
		m.checkCounters()
		if settling.observe(w, key) {
			return true
		}
	}
	return false
}

// releaseHeldRecord removes the record of rev from a loop's records, as
// the status write behind the release annotation does.
func releaseHeldRecord(h *recoveryHarness, rev string) {
	h.mutateRetryBlock(rev, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		return workloadtypes.RetryBlockRemove
	})
}

// correct is the operator's way out of the broken push: the registry
// serves the image again and the held revisions are released, or a
// corrected revision is pushed over the runner that crashes.
func (m *brokenPushModel) correct() {
	w := m.w
	text := "image back"
	switch m.way {
	case wayUnpullable, wayMixed:
		for _, s := range w.sides {
			s.h.badImageHeals = true
		}
	case wayCrashStart:
		for _, s := range m.brokenSides() {
			s.image = fixedImage
			w.render(s)
		}
		w.recordPlan()
		text = "push a corrected revision"
	}
	m.releaseHeld()
	w.path = append(w.path, text+", held revisions released")
}

// releaseHeld removes the Held record of every side held on its target,
// as the operator's release does, and reports how many it removed.
func (m *brokenPushModel) releaseHeld() int {
	released := 0
	for _, s := range m.w.sides {
		if s.h.heldOn(s.h.target.Name) {
			releaseHeldRecord(s.h, s.h.target.Name)
			m.releases++
			released++
		}
	}
	return released
}

// finishCorrection reads the end after the correction. A retry schedule
// whose last attempt was in its backoff at the correction turns Held one
// pass later, which the operator answers with a release; the roll must
// then finish.
func (m *brokenPushModel) finishCorrection(settled bool) {
	w := m.w
	if settled && !w.complete() && m.releaseHeld() > 0 {
		w.counts["released-again-after-the-correction"]++
		w.path = append(w.path, "release the held revisions again")
		settled = m.playOutReading()
	}
	m.classifyCorrection(settled)
}

// classifyCorrection reads the end after the correction: the roll must be
// complete at every side's count. A roll left open only by a partition
// that holds an Instance with a failed replacement, the hold naming the
// Instance and its ways out, is the known class of the held-exits model.
func (m *brokenPushModel) classifyCorrection(settled bool) {
	w := m.w
	switch {
	case !settled:
		w.note("did-not-settle-after-the-correction", "the world kept changing to the end of the bound after the correction: %s", w.picture())
	case w.complete():
		w.counts["complete-after-the-correction"]++
	case m.partitionHoldsAFailedInstance():
		w.note(knownPartitionHoldsFailedInstance, "after the correction the partition holds an Instance with a failed replacement, and the hold names it: %s", w.picture())
	default:
		w.note("roll-open-after-the-correction", "the roll did not finish after the correction: %s", w.picture())
	}
}

// partitionHoldsAFailedInstance reports whether every side whose roll is
// open holds on a retry record because the partition keeps an Instance
// with a failed replacement toward the target, the hold naming the
// partition.
func (m *brokenPushModel) partitionHoldsAFailedInstance() bool {
	w := m.w
	open := 0
	for _, s := range w.sides {
		if w.sideComplete(s) {
			continue
		}
		open++
		hold := s.h.hold
		if hold == nil || hold.Gate != workloadtypes.RolloutHoldGateRetryBlock || !strings.Contains(hold.Reason, "partition") {
			return false
		}
	}
	return open > 0
}

// checkHoldKept reads a play from the held moment: without a release, the
// Held record the operator saw before the scale must still stand after it.
func (m *brokenPushModel) checkHoldKept(moment brokenPushMoment, heldAtEntry map[*scaleSide]bool) {
	w := m.w
	if moment != pushAtHeld || m.releases > 0 {
		return
	}
	for _, s := range m.brokenSides() {
		if heldAtEntry[s] && !s.h.heldOn(s.h.target.Name) && !w.sideComplete(s) {
			w.note("held-record-removed-by-the-scale", "%s was held before the scale and its Held record is gone after it with no release: %s", s.comp, w.picture())
		}
	}
}

// play restores a moment, makes one scale-down, reads the passes after
// it to the settled end and, in full mode or at the held moment, corrects
// the push and asks that the roll finish.
func (m *brokenPushModel) play(entry *scaleSnapshot, way brokenPushWay, moment brokenPushMoment, mv brokenPushMove) {
	w := m.w
	if w.ticks >= m.budget {
		m.skipped++
		return
	}
	w.restore(entry)
	m.way = way
	m.releases = 0
	heldAtEntry := map[*scaleSide]bool{}
	for _, s := range m.brokenSides() {
		heldAtEntry[s] = s.h.heldOn(s.h.target.Name)
	}
	m.applyMove(mv)
	m.beginReadings()
	m.probes++
	settled := m.playOutReading()
	m.checkHoldKept(moment, heldAtEntry)
	w.classifyEnd(settled)
	if m.full || moment == pushAtHeld {
		m.correct()
		m.finishCorrection(m.playOutReading())
	}
}

// ways lists the ways the push is broken for this world: both sides to
// the unpullable image, both to the crashing runner, and, where a decoder
// exists, the decoder alone beside a sound version 2.
func (m *brokenPushModel) ways() []brokenPushWay {
	out := []brokenPushWay{wayUnpullable, wayCrashStart}
	if m.w.decode() != nil {
		out = append(out, wayMixed)
	}
	return out
}

// explore runs one configuration: each way of breaking the push with its
// moments kept, every scale-down from every moment played to its end,
// then the random runs.
func (m *brokenPushModel) explore(seed int64) {
	w := m.w
	m.prepare()
	w.reachIdle()
	idle := w.snapshot()
	for _, way := range m.ways() {
		snaps := m.captureMoments(idle, way)
		for _, moment := range brokenPushMoments {
			snap := snaps[moment]
			if snap == nil {
				continue
			}
			for _, mv := range m.moves() {
				m.play(snap, way, moment, mv)
			}
		}
	}
	m.randomRuns(idle, seed)
}

// randomRuns plays the broken push from idle with up to two scale-downs
// at random ticks and the correction at a later one, under a fixed seed.
func (m *brokenPushModel) randomRuns(idle *scaleSnapshot, seed int64) {
	w := m.w
	rng := rand.New(rand.NewSource(seed))
	runs := 2
	if m.full {
		runs = 8
	}
	ways := m.ways()
	for run := 0; run < runs; run++ {
		if w.ticks >= m.budget {
			m.skipped++
			continue
		}
		w.restore(idle)
		w.path = append(w.path, fmt.Sprintf("random run %d", run+1))
		m.push(ways[rng.Intn(len(ways))])
		m.releases = 0
		m.runs++
		m.probes++
		m.beginReadings()
		left, corrected := 2, false
		settling := w.startSettling()
		settled := false
		for i := 0; i < m.bound && !settled; i++ {
			switch {
			case left > 0 && rng.Intn(6) == 0:
				moves := m.moves()
				m.applyMove(moves[rng.Intn(len(moves))])
				left--
				m.beginReadings()
				settling = w.startSettling()
			case left == 0 && !corrected && rng.Intn(6) == 0:
				m.correct()
				corrected = true
				settling = w.startSettling()
			}
			key := w.tick()
			m.checkSelection()
			m.checkPushReported()
			m.checkCounters()
			settled = settling.observe(w, key)
		}
		if corrected {
			m.finishCorrection(settled)
		} else {
			w.classifyEnd(settled)
		}
	}
}

// brokenPushReferenceConfigs are the configurations every run explores:
// the reference shape of three and three with a router under both
// strategies, the reference shapes under their reference settings,
// the sides in one rolling group and in none, the gangs on the smallest
// shape, an engine-only service, and the slower stop of the second
// scheduler twin.
func brokenPushReferenceConfigs() []brokenPushConfig {
	recreate, surge := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain
	return []brokenPushConfig{
		{scaleConfig: scaleConfig{shape: scaleShape{3, 3, 1}, settings: scaleSettings{prefill: surge, decode: surge, router: true}}},
		{scaleConfig: scaleConfig{shape: scaleShape{3, 3, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, router: true}}, slowStop: true},
		{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate}}},
		{scaleConfig: scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true}}},
		{scaleConfig: scaleConfig{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate}}, ungrouped: true},
		{scaleConfig: scaleConfig{shape: scaleShape{12, 4, 1}, settings: scaleSettings{prefill: surge, decode: surge}}, ungrouped: true, slowStop: true},
		{scaleConfig: scaleConfig{shape: scaleShape{12, 4, 2}, settings: scaleSettings{prefill: recreate, decode: recreate}}},
		{scaleConfig: scaleConfig{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: recreate, decode: surge, router: true}}},
		{scaleConfig: scaleConfig{shape: scaleShape{15, 15, 1}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true, firstSide: v1beta1.DecoderComponent}}},
		{scaleConfig: scaleConfig{shape: scaleShape{3, 0, 1}, settings: scaleSettings{prefill: surge, decode: surge}}},
		{scaleConfig: scaleConfig{shape: scaleShape{12, 0, 1}, settings: scaleSettings{prefill: recreate, decode: recreate}}, slowStop: true},
	}
}

// brokenPushFullSample is how many sampled configurations the full run
// adds to the reference ones.
const brokenPushFullSample = 36

// brokenPushConfigs lists the configurations of a run: the reference ones
// and, in full mode, a seeded sample of the product of shapes, gang
// sizes, strategy pairs, settings, the group and the stop timing.
func brokenPushConfigs(full bool) []brokenPushConfig {
	configs := brokenPushReferenceConfigs()
	if !full {
		return configs
	}
	seen := map[string]bool{}
	for _, cfg := range configs {
		seen[cfg.String()] = true
	}
	var all []brokenPushConfig
	for _, shape := range brokenPushShapes {
		for _, p := range []int32{1, 2, 4, 8} {
			if (shape[0]+shape[1])*p <= scaleFullPodCeiling {
				all = append(all, brokenPushShapeConfigs(scaleShape{shape[0], shape[1], p}, seen)...)
			}
		}
	}
	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	if len(all) > brokenPushFullSample {
		all = all[:brokenPushFullSample]
	}
	return append(configs, all...)
}

// brokenPushShapes are the reference prefill:decode shapes the full run
// samples, the three-and-three shape and an engine-only service
// among them.
var brokenPushShapes = [][2]int32{{12, 8}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}, {3, 3}, {12, 0}}

// brokenPushVariants are the settings the full run crosses the shapes
// with.
var brokenPushVariants = []scaleSettings{
	{},
	{maintainRatio: true},
	{keepRatio: true},
	{keepRatio: true, firstSide: v1beta1.DecoderComponent},
	{partition: 2},
	{policy: true},
	{router: true},
	{canary: true},
}

// brokenPushStrategies are the strategy pairs the full run crosses the
// shapes with.
var brokenPushStrategies = [][2]workloadtypes.UpdateStrategyType{
	{workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategyRecreatePod},
	{workloadtypes.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategySurgeThenDrain},
	{workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain},
	{workloadtypes.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategyRecreatePod},
}

// brokenPushShapeConfigs lists one shape's configurations: every strategy
// pair and variant that fits it, grouped and ungrouped, with the kubelet
// stopping at once and two passes later, the ones already listed left out.
func brokenPushShapeConfigs(shape scaleShape, seen map[string]bool) []brokenPushConfig {
	var out []brokenPushConfig
	for _, pair := range brokenPushStrategies {
		for _, variant := range brokenPushVariants {
			settings := variant
			settings.prefill, settings.decode = pair[0], pair[1]
			for _, ungrouped := range []bool{false, true} {
				if !brokenPushSettingsFit(shape, settings, ungrouped) {
					continue
				}
				out = append(out, brokenPushTimings(scaleConfig{shape: shape, settings: settings}, ungrouped, seen)...)
			}
		}
	}
	return out
}

// brokenPushSettingsFit reports whether a variant makes sense on a shape:
// the pair settings need a decoder, and the group settings need a group.
func brokenPushSettingsFit(shape scaleShape, settings scaleSettings, ungrouped bool) bool {
	pairSetting := settings.keepRatio || settings.maintainRatio || settings.policy || settings.canary
	groupSetting := settings.keepRatio || settings.maintainRatio || settings.canary
	if shape.decode == 0 && pairSetting {
		return false
	}
	return !(ungrouped && groupSetting)
}

// brokenPushTimings lists a configuration with the kubelet stopping a
// deleted pod at once and two passes later, each once.
func brokenPushTimings(cfg scaleConfig, ungrouped bool, seen map[string]bool) []brokenPushConfig {
	var out []brokenPushConfig
	for _, slow := range []bool{false, true} {
		full := brokenPushConfig{scaleConfig: cfg, ungrouped: ungrouped, slowStop: slow}
		if !seen[full.String()] {
			seen[full.String()] = true
			out = append(out, full)
		}
	}
	return out
}

// brokenPushExpectedEnds are the ends a play may reach that are not
// findings: the roll held on the spent schedule, paused, complete after
// the correction (with a second release where the schedule turned Held
// after the first), and a source removed by the plan's own rule, because
// its replacement was being retried on the failing revision or the asked
// count was below the surging sources.
var brokenPushExpectedEnds = map[string]bool{
	"held": true, "ends-paused": true, "complete-after-the-correction": true, "released-again-after-the-correction": true,
	"source-removed-count-below-surging": true, "retrying-source-removed": true,
}

// knownPartitionHoldsFailedInstance is the known picture of this model.
const knownPartitionHoldsFailedInstance = "known-partition-holds-the-failed-instance-named-by-the-hold"

// knownBrokenPushClasses are the finding classes this model shows on main
// today, each with the picture behind it. A finding in a known class is
// reported and does not fail the run; a change of behaviour removes its entry and the
// lock test that holds the picture.
var knownBrokenPushClasses = map[string]string{
	knownPartitionHoldsFailedInstance: "a scale to one under a partition of two keeps the one Instance the partition holds, whose replacement failed before the image came back; the roll never retries it while the partition holds it, and the hold names the Instance and the two ways out, lowering the partition or pushing another revision (the held-exits model's known class, an open question; held by TestScaleDownBrokenPush_AScaleToOneUnderAPartitionKeepsTheFailedInstanceAfterTheCorrection)",
}

// brokenPushResult is what one configuration's exploration found.
type brokenPushResult struct {
	cfg                                                     brokenPushConfig
	states, ticks, passes, gateCalls, probes, skipped, runs int
	counts, choices                                         map[string]int
	findings                                                map[string]scaleFinding
	took                                                    time.Duration
}

// brokenPushExplore runs the model over every configuration, one subtest
// per configuration in parallel, and reports every class, failing on the
// classes that are neither expected ends nor known.
func brokenPushExplore(t *testing.T) {
	t.Helper()
	full := os.Getenv(brokenPushFullEnv) == "full"
	only := os.Getenv(brokenPushOnlyEnv)
	start := time.Now()
	var configs []brokenPushConfig
	for _, cfg := range brokenPushConfigs(full) {
		if only == "" || strings.Contains(cfg.String(), only) {
			configs = append(configs, cfg)
		}
	}
	results := make([]brokenPushResult, len(configs))
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				began := time.Now()
				w := newScaleWorld(t, cfg.scaleConfig)
				m := newBrokenPushModel(w, cfg, full)
				m.explore(int64(i + 1))
				results[i] = brokenPushResult{cfg: cfg, states: len(w.seen), ticks: w.ticks, passes: w.passes, gateCalls: w.gateCalls,
					probes: m.probes, skipped: m.skipped, runs: m.runs, counts: w.counts, choices: m.choices, findings: w.findings, took: time.Since(began)}
			})
		}
	})
	brokenPushReport(t, configs, results, time.Since(start))
}

// brokenPushReport prints every configuration's counts, what the waves
// took, and every class with its examples, and fails the classes the
// engine does not owe.
func brokenPushReport(t *testing.T, configs []brokenPushConfig, results []brokenPushResult, took time.Duration) {
	t.Helper()
	totals, configurations, choices := map[string]int{}, map[string]int{}, map[string]int{}
	examples := map[string][]scaleExample{}
	var states, ticks, passes, gateCalls, probes, skipped, runs int
	for n, res := range results {
		states += res.states
		ticks += res.ticks
		passes += res.passes
		gateCalls += res.gateCalls
		probes += res.probes
		skipped += res.skipped
		runs += res.runs
		var parts []string
		for class, c := range res.counts {
			totals[class] += c
			parts = append(parts, fmt.Sprintf("%s=%d", class, c))
		}
		sort.Strings(parts)
		for choice, c := range res.choices {
			choices[choice] += c
		}
		t.Logf("config %d/%d [%s]: %d states, %d ticks, %d plays (%d skipped past the tick budget), %d random runs, %s: %s",
			n+1, len(configs), res.cfg, res.states, res.ticks, res.probes, res.skipped, res.runs, res.took.Round(time.Second), strings.Join(parts, " "))
		for class, f := range res.findings {
			configurations[class]++
			if len(examples[class]) < 6 {
				examples[class] = append(examples[class], scaleExample{cfg: res.cfg.scaleConfig, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	var taken []string
	for choice, c := range choices {
		taken = append(taken, fmt.Sprintf("%s: %d", choice, c))
	}
	sort.Strings(taken)
	t.Logf("%d configurations, %d states, %d ticks, %d passes, %d gate questions, %d plays (%d skipped past the tick budget), %d random runs, %s; the waves %s",
		len(configs), states, ticks, passes, gateCalls, probes, skipped, runs, took.Round(time.Second), strings.Join(taken, "; "))
	for _, class := range classes {
		t.Logf("%s: %d states in %d configurations", class, totals[class], configurations[class])
		if brokenPushExpectedEnds[class] || strings.HasPrefix(class, "moment-") {
			continue
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.state)
		}
		if known := knownBrokenPushClasses[class]; known != "" {
			t.Logf("  known: %s", known)
			continue
		}
		if known := knownScaleFindingClasses[class]; known != "" {
			t.Logf("  known: %s", known)
			continue
		}
		t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg, examples[class][0].finding.message)
	}
}

// TestScaleDownBrokenPush_EveryScaleDownLeavesTheRollAWayOut explores
// every configuration: a scale-down of one side or both, by one, to one,
// by a step or half, at every moment of a push whose image cannot be
// pulled or whose runner crashes on start, takes the Instances on the
// failing revision before a sound serving one, leaves the status saying
// the push has not landed, keeps the published counts true to the pods,
// keeps a Held record standing until a release, and ends held or, after
// the correction, complete at every side's count, within its budgets.
func TestScaleDownBrokenPush_EveryScaleDownLeavesTheRollAWayOut(t *testing.T) {
	brokenPushExplore(t)
}

// TestScaleDownBrokenPush_AScaleToOneUnderAPartitionKeepsTheFailedInstanceAfterTheCorrection
// holds the picture the model found: 20 prefill under SurgeThenDrain with
// a partition of 2 and 10 decode under RecreatePod, both pushed to an
// image no pod can pull. While the first prefill replacements stand, the
// operator scales the prefill side to 1. The plan keeps a source whose
// replacement is coming up; its attempt fails; the registry serves the
// image again and the operator releases the held revisions. The one
// Instance left is held by the partition of 2 with its failed replacement
// toward the target, so the roll never retries it, and the hold names the
// Instance and the two ways out. When the engine retries it, or the
// partition no longer holds a failed Instance, this test fails, and the
// known class and this lock go with it.
func TestScaleDownBrokenPush_AScaleToOneUnderAPartitionKeepsTheFailedInstanceAfterTheCorrection(t *testing.T) {
	surge, recreate := workloadtypes.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategyRecreatePod
	cfg := brokenPushConfig{scaleConfig: scaleConfig{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: surge, decode: recreate, partition: 2}}}
	w := newScaleWorld(t, cfg.scaleConfig)
	m := newBrokenPushModel(w, cfg, false)
	m.prepare()
	w.reachIdle()
	idle := w.snapshot()
	snaps := m.captureMoments(idle, wayUnpullable)
	if snaps[pushAtPending] == nil {
		t.Fatalf("the first replacement never stood: %s", w.picture())
	}
	w.restore(snaps[pushAtPending])
	m.way = wayUnpullable
	m.applyMove(brokenPushMove{kind: downToOne, side: 0})
	m.beginReadings()
	if !m.playOutReading() {
		t.Fatalf("the world did not settle after the scale: %s", w.picture())
	}
	m.correct()
	settled := m.playOutReading()
	if settled && !w.complete() && m.releaseHeld() > 0 {
		settled = m.playOutReading()
	}
	if !settled {
		t.Fatalf("the world did not settle after the correction: %s", w.picture())
	}
	prefill := w.prefill()
	if w.complete() || !m.partitionHoldsAFailedInstance() {
		t.Fatalf("the picture moved: the roll ends %s; if the partition no longer holds a failed Instance after the correction, remove the known class and this lock", w.picture())
	}
	t.Logf("after the correction the prefill side holds: %s", prefill.h.hold.Reason)
}
