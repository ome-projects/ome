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
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the production engine of a prefill/decode service out
// of every state in which a roll stands held, and asks whether each way
// out the operator or the world has takes the roll to its end. It runs on
// the world of scale_beside_a_roll_model_test.go: one workload.Reconcile
// loop per Component on the recovery harness, the coordination gates
// wired between the Components the way the replica adapter wires them,
// the follower count of a scaling policy and the partition a canary step
// projects, all from production code.
//
// The ways in. A roll stands held when the retry schedule of a side's
// target revision is spent (the Held record: an image no pod can pull,
// every attempt the schedule allows failed), when the operator pauses it,
// when the cluster has no room for the pods an attempt builds (the
// attempt waits for placement and the budget it holds admits nothing
// else) and when the admission webhook refuses every pod create. Each is
// reached on every configuration of the scale model (a reference shape,
// pods per Instance, a strategy per side at ten percent, with or without
// a ratio rule, keepRatio, a first side, a partition, a canary, a scaling
// policy, a router beside the pair, a Sequential chain) at the moments of
// a roll.
//
// The ways out. From a spent schedule: a corrected push, a push back to
// version 1, the operator's release of the held revision (the record
// removed, as the ome.io/release-held-revision annotation does) once the
// registry serves the image again; each alone and after a scale of the
// held side or the other side, a partition change, a policy change, a
// pause, a lost pod, node or gang member, or a promoted canary. From a
// pause: the resume, alone and after a push, a push back or a scale. From
// no room: the room coming back, alone and after a push or a push back.
// From a refusing webhook: the webhook answering again, alone and after a
// push or a push back.
//
// What fails the test. A way out that should end the roll and leaves it
// held, waiting on a hold with nothing to wait for, or with two gates
// waiting on each other; a release that admits no fresh attempt; a world
// that never settles; a side above its budget at any tick (the per-tick
// checks of the scale model run here too). The ends the engine owes are
// counted, not failed: a release of a revision that is still broken ends
// held again with the schedule counted from the start; the image coming
// back alone, a scale, a partition change or a policy change alone leave
// the roll held; a pause alone ends paused; a fault still standing holds
// the roll.
//
// OME_HELD_EXITS_HUNT=full widens the configurations, the moments, the
// ways out and the random runs; OME_HELD_EXITS_HUNT_ONLY=<text> keeps the
// configurations whose name carries the text.

const (
	heldExitsFullEnv = "OME_HELD_EXITS_HUNT"
	heldExitsOnlyEnv = "OME_HELD_EXITS_HUNT_ONLY"
	// heldExitsNoRoom is the scheduler's answer while the cluster has no
	// room for a pod.
	heldExitsNoRoom = "0/4 nodes are available: insufficient accelerators"
)

// heldEntry is a way a roll comes to stand held.
type heldEntry uint8

const (
	entrySpentSchedule heldEntry = iota // the target's retry schedule is spent on a side by an image no pod can pull
	entrySpentCrash                     // the target's retry schedule is spent on a side by a runner that crashes on start
	entryPaused                         // the operator paused the rollout
	entryNoRoom                         // the cluster has no room for an attempt's pods
	entryWebhookDown                    // the admission webhook refuses every pod create
)

var heldEntries = []heldEntry{entrySpentSchedule, entrySpentCrash, entryPaused, entryNoRoom, entryWebhookDown}

var heldEntryNames = [...]string{"spent-schedule", "spent-crash", "paused", "no-room", "webhook-down"}

// spent reports an entry whose roll stands on a spent retry schedule.
func (e heldEntry) spent() bool { return e == entrySpentSchedule || e == entrySpentCrash }

func (e heldEntry) String() string { return heldEntryNames[e] }

// heldStepKind is one step of a way out: a move of the scale model, or
// one of the operator's and the world's moves this file adds.
type heldStepKind uint8

const (
	stepMove        heldStepKind = iota // the scaleMove in the step
	stepRelease                         // the Held record of every held side is removed
	stepImageBack                       // the registry serves the broken image again
	stepRoomBack                        // the cluster has room again
	stepWebhookBack                     // the admission webhook answers again
	stepPauseOnly                       // the operator pauses and leaves the pause standing
)

var heldStepKindNames = [...]string{"", "release the held revision", "image back", "room back", "webhook back", "pause"}

// heldStep is one step of a way out.
type heldStep struct {
	kind heldStepKind
	move scaleMove
}

func (s heldStep) String() string {
	if s.kind == stepMove {
		return s.move.String()
	}
	return heldStepKindNames[s.kind]
}

func moveStep(kind scaleMoveKind, side int, delta int32) heldStep {
	return heldStep{kind: stepMove, move: scaleMove{kind: kind, side: side, delta: delta}}
}

func plainStep(kind heldStepKind) heldStep { return heldStep{kind: kind} }

// heldOutcome is the end a way out owes.
type heldOutcome uint8

const (
	owesComplete  heldOutcome = iota // every side complete at its count
	owesHeldAgain                    // the schedule spent again, counted from the start: a release while the image is still broken
	owesHeld                         // the roll still held: no corrective step was taken
	owesPaused                       // the run ends paused: the operator did not resume
	owesFault                        // the fault still stands: no room, or the webhook still refusing
)

// heldSequence is one way out: its steps in order and the end it owes.
type heldSequence struct {
	steps []heldStep
	owes  heldOutcome
}

func (q heldSequence) String() string {
	parts := make([]string, 0, len(q.steps))
	for _, s := range q.steps {
		parts = append(parts, s.String())
	}
	return strings.Join(parts, " then ")
}

func wayOut(owes heldOutcome, steps ...heldStep) heldSequence {
	return heldSequence{steps: steps, owes: owes}
}

// joinSteps is one step list made of several, each copied.
func joinSteps(parts ...[]heldStep) []heldStep {
	var out []heldStep
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// heldPrelude is a move the operator or the world makes while the roll
// stands held, before any corrective step: the end it owes when nothing
// follows it, and the step that follows the corrective one (the resume
// of a pause).
type heldPrelude struct {
	steps []heldStep
	alone heldOutcome
	after []heldStep
}

// heldModel runs the ways out over one configuration of the scale world.
type heldModel struct {
	w    *scaleWorld
	full bool
	// entry is the way in under exploration.
	entry heldEntry
	// bound is the most ticks one play may take; budget the most ticks
	// one configuration may spend, the ways out left past it counted as
	// skipped so a slower machine reads the same first ways out.
	bound, budget int
	// releasedAt is, per side, when its Held record was removed by the
	// release step, so a record counted again can be told from the one
	// released.
	releasedAt map[*scaleSide]time.Time
	probes     int
	skipped    int
	runs       int
}

func newHeldModel(w *scaleWorld, full bool) *heldModel {
	budget := 6000
	if full {
		budget = 40000
	}
	return &heldModel{w: w, full: full, bound: scaleTickBound(full), budget: budget, releasedAt: map[*scaleSide]time.Time{}}
}

// heldSideIndex is the position in w.sides of the side the held roll
// pushes to the image no pod can pull.
func (m *heldModel) heldSideIndex() int {
	for i, s := range m.w.sides {
		if s == m.w.heldSide() {
			return i
		}
	}
	return 0
}

// otherSideIndex is the position of the pairing peer of the held side, or
// -1 on an engine-only service.
func (m *heldModel) otherSideIndex() int {
	if m.w.decode() == nil {
		return -1
	}
	return 1 - m.heldSideIndex()
}

// scaleOf is the operator's scale of a side by delta: through the anchor
// when a scaling policy sizes the decoder, which a direct write on the
// follower would not hold.
func (m *heldModel) scaleOf(side int, delta int32) heldStep {
	if m.w.policy != nil && side != 0 {
		return moveStep(moveAnchorScale, 0, delta)
	}
	return moveStep(moveScale, side, delta)
}

// partitionStep is the operator's change of the partition on the prefill
// side: cleared when one stands, set to two otherwise, and raised by one
// in full mode as a second step.
func (m *heldModel) partitionStep() []heldStep {
	if p := m.w.prefill().partition; p > 0 {
		out := []heldStep{moveStep(movePartition, 0, 0)}
		if m.full {
			out = append(out, moveStep(movePartition, 0, p+1))
		}
		return out
	}
	return []heldStep{moveStep(movePartition, 0, 2)}
}

// policyStep is the operator's change of the scaling policy: edited when
// one stands, added otherwise.
func (m *heldModel) policyStep() heldStep {
	if m.w.policy != nil {
		return moveStep(movePolicyEdit, 0, 0)
	}
	return moveStep(movePolicyAdd, 0, 0)
}

// lossSteps are the world's losses on a side: a pod of the first Instance
// a loss touches, and in full mode the node under it and, in a gang, a
// worker of it.
func (m *heldModel) lossSteps(side int) []heldStep {
	if side < 0 {
		return nil
	}
	indices := m.w.lossIndices(m.w.sides[side])
	if len(indices) == 0 {
		return nil
	}
	loss := func(kind scaleMoveKind) heldStep {
		return heldStep{kind: stepMove, move: scaleMove{kind: kind, side: side, index: indices[0]}}
	}
	out := []heldStep{loss(moveLosePod)}
	if m.full {
		out = append(out, loss(moveLoseNode))
		if m.w.cfg.shape.pods > 1 {
			out = append(out, loss(moveLoseMember))
		}
	}
	return out
}

// preludesAtHeld lists the moves made while the schedule is spent: a
// scale of the held side each way, a pause, a partition change, a change
// of the standing policy, a loss on the held side, a promoted canary; in
// full mode a policy added too, a scale of the other side and of both,
// the ratio rule changed, the policy removed and the losses of the other
// side.
func (m *heldModel) preludesAtHeld() []heldPrelude {
	held, other := m.heldSideIndex(), m.otherSideIndex()
	resume := []heldStep{moveStep(moveResume, 0, 0)}
	out := []heldPrelude{
		{steps: []heldStep{m.scaleOf(held, 1)}, alone: owesHeld},
		{steps: []heldStep{m.scaleOf(held, -1)}, alone: owesHeld},
		{steps: []heldStep{plainStep(stepPauseOnly)}, alone: owesPaused, after: resume},
		{steps: m.partitionStep(), alone: owesHeld},
	}
	if m.full || m.w.policy != nil {
		out = append(out, heldPrelude{steps: []heldStep{m.policyStep()}, alone: owesHeld})
	}
	for _, loss := range m.lossSteps(held) {
		out = append(out, heldPrelude{steps: []heldStep{loss}, alone: owesHeld})
	}
	if m.w.canaryStep >= 0 {
		out = append(out, heldPrelude{steps: []heldStep{moveStep(moveCanaryPromote, 0, 0)}, alone: owesHeld})
	}
	if !m.full || other < 0 {
		return out
	}
	step := m.w.stepOf(m.w.sides[held].h.replicas)
	out = append(out,
		heldPrelude{steps: []heldStep{m.scaleOf(other, 1)}, alone: owesHeld},
		heldPrelude{steps: []heldStep{m.scaleOf(other, -1)}, alone: owesHeld},
		heldPrelude{steps: []heldStep{m.scaleOf(held, step)}, alone: owesHeld},
		heldPrelude{steps: []heldStep{m.scaleOf(held, -m.w.sides[held].h.replicas/2)}, alone: owesHeld},
	)
	if m.w.policy == nil {
		out = append(out, heldPrelude{steps: []heldStep{moveStep(moveScaleBoth, 0, 1)}, alone: owesHeld})
	} else {
		out = append(out, heldPrelude{steps: []heldStep{moveStep(movePolicyRemove, 0, 0)}, alone: owesHeld})
	}
	ratio := moveRatioRuleAdd
	if m.w.maintainRatio {
		ratio = moveRatioRuleRemove
	}
	if m.w.canaryStep < 0 {
		out = append(out, heldPrelude{steps: []heldStep{moveStep(ratio, 0, 0)}, alone: owesHeld})
	}
	for _, loss := range m.lossSteps(other) {
		out = append(out, heldPrelude{steps: []heldStep{loss}, alone: owesHeld})
	}
	return out
}

// correctives are the steps that take a roll out of a spent schedule: a
// corrected push, a push back to version 1 and, for the image no pod can
// pull, the image back and the held revision released (in full mode the
// release before the image too). A crashing runner has no registry to
// come back: its correctives are the two pushes.
func (m *heldModel) correctives() [][]heldStep {
	out := [][]heldStep{
		{moveStep(movePush, 0, 0)},
		{moveStep(moveRollBack, 0, 0)},
	}
	if m.entry == entrySpentCrash {
		return out
	}
	out = append(out, []heldStep{plainStep(stepImageBack), plainStep(stepRelease)})
	if m.full {
		out = append(out, []heldStep{plainStep(stepRelease), plainStep(stepImageBack)})
	}
	return out
}

// spentSequences lists the ways out of a spent retry schedule: the release
// alone and the image back alone, every corrective alone, every prelude
// followed by the first corrective, and in full mode every prelude alone
// and before every corrective. In bounded mode a prelude alone is played
// for the pause only: the scale model plays the other moves alone at the
// held moment.
func (m *heldModel) spentSequences() []heldSequence {
	correctives := m.correctives()
	out := []heldSequence{wayOut(owesHeldAgain, plainStep(stepRelease))}
	if m.entry == entrySpentSchedule {
		out = append(out, wayOut(owesHeld, plainStep(stepImageBack)))
	}
	for _, c := range correctives {
		out = append(out, wayOut(owesComplete, c...))
	}
	for _, p := range m.preludesAtHeld() {
		if m.full || p.alone == owesPaused {
			out = append(out, wayOut(p.alone, joinSteps(p.steps, p.after)...))
		}
		for i, c := range correctives {
			if i > 0 && !m.full {
				break
			}
			out = append(out, wayOut(owesComplete, joinSteps(p.steps, c, p.after)...))
		}
	}
	return out
}

// pausedSequences lists the ways out of a pause: the resume alone and
// after a push, a push back or a scale; a push alone leaves the run
// paused. Full mode adds a scale-down, a partition change, a policy change
// and a loss before the resume, and a push after it.
func (m *heldModel) pausedSequences() []heldSequence {
	resume, push, back := moveStep(moveResume, 0, 0), moveStep(movePush, 0, 0), moveStep(moveRollBack, 0, 0)
	out := []heldSequence{
		wayOut(owesComplete, resume),
		wayOut(owesComplete, push, resume),
		wayOut(owesComplete, back, resume),
		wayOut(owesComplete, m.scaleOf(0, 1), resume),
		wayOut(owesPaused, push),
	}
	if !m.full {
		return out
	}
	out = append(out,
		wayOut(owesComplete, m.scaleOf(0, -1), resume),
		wayOut(owesComplete, joinSteps(m.partitionStep(), []heldStep{resume})...),
		wayOut(owesComplete, m.policyStep(), resume),
		wayOut(owesComplete, resume, push),
	)
	for _, loss := range m.lossSteps(0) {
		out = append(out, wayOut(owesComplete, loss, resume))
	}
	return out
}

// faultSequences lists the ways out of a standing fault (no room, or the
// webhook refusing): the fault's end alone and after a push or a push
// back; a push alone leaves the fault standing. Full mode adds a scale
// each way, a pause resumed after the fault ends, and a loss.
func (m *heldModel) faultSequences(end heldStepKind) []heldSequence {
	back, push, rollBack := plainStep(end), moveStep(movePush, 0, 0), moveStep(moveRollBack, 0, 0)
	out := []heldSequence{
		wayOut(owesComplete, back),
		wayOut(owesComplete, push, back),
		wayOut(owesComplete, rollBack, back),
		wayOut(owesFault, push),
	}
	if !m.full {
		return out
	}
	out = append(out,
		wayOut(owesComplete, m.scaleOf(0, 1), back),
		wayOut(owesComplete, m.scaleOf(0, -1), back),
		wayOut(owesComplete, plainStep(stepPauseOnly), back, moveStep(moveResume, 0, 0)),
	)
	for _, loss := range m.lossSteps(0) {
		out = append(out, wayOut(owesComplete, loss, back))
	}
	return out
}

// sequencesFrom lists the ways out tried from an entry.
func (m *heldModel) sequencesFrom(e heldEntry) []heldSequence {
	switch e {
	case entrySpentSchedule, entrySpentCrash:
		return m.spentSequences()
	case entryPaused:
		return m.pausedSequences()
	case entryNoRoom:
		return m.faultSequences(stepRoomBack)
	case entryWebhookDown:
		return m.faultSequences(stepWebhookBack)
	}
	return nil
}

// heldStart is one state an entry is laid on: a moment of the roll and
// the snapshot of the world at it.
type heldStart struct {
	moment scaleMoment
	snap   *scaleSnapshot
}

// pushCrash pushes a revision whose runner crashes on start on the held
// side and version 2 on the other; where the decoder is named to move
// first both sides get the crashing revision, as pushHeld does with the
// image no pod can pull.
func (m *heldModel) pushCrash() {
	w := m.w
	both := w.cfg.settings.firstSide == v1beta1.DecoderComponent
	for _, s := range w.sides {
		s.image = fixedImage
		if s == w.heldSide() || (both && s.comp == workloadtypes.ComponentEngine) {
			s.image = crashStartImage
		}
		w.render(s)
	}
	w.recordPlan()
	w.path = append(w.path, "push a revision whose runner crashes on start")
}

// captureCrashHeld plays the crashing push from idle and keeps the state
// where the held side's retry schedule is spent; nil when the bound
// passes first, which is counted as a class of its own.
func (m *heldModel) captureCrashHeld(idle *scaleSnapshot) *scaleSnapshot {
	w := m.w
	w.restore(idle)
	m.pushCrash()
	settled := w.playOut(m.bound)
	if !w.momentReached(scaleAtHeld) {
		w.note("held-not-reached", "the crashing push did not spend the held side's retry schedule within the bound: %s", w.picture())
		return nil
	}
	if !settled {
		w.note("entry-spent-crash-not-settled", "the crashing push spent the schedule and the world still moves: %s", w.picture())
	}
	return w.snapshot()
}

// startsFor lists the states an entry is laid on: the held roll's own
// state for a spent schedule; for the others, in bounded mode the
// rotating moment of the plain roll (the settling one, then the last,
// then the canary step when the roll never shows it), every moment in
// full mode.
func (m *heldModel) startsFor(e heldEntry, snaps map[scaleMoment]*scaleSnapshot) []heldStart {
	switch e {
	case entrySpentSchedule:
		return []heldStart{{moment: scaleAtHeld, snap: snaps[scaleAtHeld]}}
	case entrySpentCrash:
		return []heldStart{{moment: scaleAtHeld, snap: m.captureCrashHeld(snaps[scaleAtIdle])}}
	}
	moments := []scaleMoment{scaleAtRotating, scaleAtSettling, scaleAtLast, scaleAtCanaryStep}
	if m.full {
		moments = []scaleMoment{scaleAtSettling, scaleAtRotating, scaleAtDraining, scaleAtLast, scaleAtCanaryStep}
	}
	var out []heldStart
	for _, moment := range moments {
		if snaps[moment] == nil {
			continue
		}
		out = append(out, heldStart{moment: moment, snap: snaps[moment]})
		if !m.full {
			break
		}
	}
	return out
}

// reachEntry restores a start's snapshot and lays the entry on it: the
// held roll stands as captured; the pause, the missing room and the
// refusing webhook are laid on the plain roll and the world is played
// until it settles or the bound is reached. The entry's end is counted
// under its own class.
func (m *heldModel) reachEntry(e heldEntry, start heldStart) {
	w := m.w
	w.restore(start.snap)
	if e.spent() {
		return
	}
	w.path = append(w.path, "at "+start.moment.String())
	switch e {
	case entryPaused:
		w.setPaused(true)
	case entryNoRoom:
		for _, s := range w.sides {
			s.h.holdPlacement(heldExitsNoRoom)
		}
	case entryWebhookDown:
		for _, s := range w.sides {
			s.h.admissionDown = true
		}
	}
	w.path = append(w.path, e.String())
	class := "entry-" + e.String()
	if !w.playOut(m.bound) {
		class += "-not-settled"
	}
	w.note(class, "the %s entry at the %s moment: %s", e, start.moment, w.picture())
}

// removeRetryBlock removes the record of rev from a loop's records, as the
// status write behind the release annotation does.
func removeRetryBlock(h *recoveryHarness, rev string) {
	h.mutateRetryBlock(rev, func(*workloadtypes.RetryBlock) workloadtypes.RetryBlockDisposition {
		return workloadtypes.RetryBlockRemove
	})
}

// applyStep makes one step of a way out on the world.
func (m *heldModel) applyStep(st heldStep) {
	w := m.w
	switch st.kind {
	case stepMove:
		w.apply(st.move)
		return
	case stepRelease:
		for _, s := range w.sides {
			if s.h.heldOn(s.h.target.Name) {
				m.releasedAt[s] = w.clk.Now()
				removeRetryBlock(s.h, s.h.target.Name)
			}
		}
	case stepImageBack:
		for _, s := range w.sides {
			s.h.badImageHeals = true
		}
	case stepRoomBack:
		for _, s := range w.sides {
			s.h.releasePlacement()
		}
	case stepWebhookBack:
		for _, s := range w.sides {
			s.h.admissionDown = false
		}
	case stepPauseOnly:
		w.setPaused(true)
	}
	w.path = append(w.path, st.String())
}

// play restores the entry, makes the steps of one way out with a tick
// between them, plays the world out and reads the end against what the
// way out owes.
func (m *heldModel) play(entry *scaleSnapshot, q heldSequence) {
	w := m.w
	if w.ticks >= m.budget {
		m.skipped++
		return
	}
	w.restore(entry)
	m.releasedAt = map[*scaleSide]time.Time{}
	for i, st := range q.steps {
		if i > 0 {
			w.tick()
		}
		m.applyStep(st)
	}
	m.probes++
	m.classify(q, w.playOut(m.bound))
}

// classify reads the end of a way out against what it owes.
func (m *heldModel) classify(q heldSequence, settled bool) {
	w := m.w
	switch q.owes {
	case owesFault:
		w.note("fault-still-standing", "the fault stands and the roll waits on it: %s", w.picture())
		return
	case owesHeld, owesPaused:
		w.classifyEnd(settled)
		return
	}
	if !settled {
		w.note("did-not-settle", "the world kept changing to the end of the bound after %s: %s", q, w.picture())
		return
	}
	if w.complete() {
		w.note("complete", "the roll ended after %s", q)
		return
	}
	if q.owes == owesHeldAgain {
		m.classifyHeldAgain()
		return
	}
	m.classifyLeftHeld(q)
}

// anyHeld reports a side whose target's retry schedule is spent.
func (m *heldModel) anyHeld() bool {
	for _, s := range m.w.sides {
		if s.h.heldOn(s.h.target.Name) {
			return true
		}
	}
	return false
}

// classifyHeldAgain reads the end of a release made while the image is
// still broken: a released side must have counted a fresh attempt, and
// the roll then holds on the schedule spent again.
func (m *heldModel) classifyHeldAgain() {
	w := m.w
	fresh := false
	for s, at := range m.releasedAt {
		b := s.h.findBlock(s.h.target.Name)
		if b != nil && b.FirstFailureAt != nil && !b.FirstFailureAt.Time.Before(at) {
			fresh = true
		}
	}
	switch {
	case !fresh:
		w.note("release-admits-no-attempt", "the release of the held revision admitted no fresh attempt: %s", w.picture())
	case m.anyHeld():
		w.note("held-again-after-release", "the released schedule was counted again and is spent: %s", w.picture())
	default:
		w.classifyEnd(true)
	}
}

// classifyLeftHeld reads the end of a way out that owed a complete roll
// and left an unfinished one: a roll still held is the way out leaving it
// so; a side still paused is a resume not taken; a roll waiting on an
// Instance the partition holds with a failed attempt toward the target is
// a known picture; every other picture is the scale model's reading of an
// unfinished roll at rest.
func (m *heldModel) classifyLeftHeld(q heldSequence) {
	w := m.w
	if m.anyHeld() {
		w.note("held-after-the-way-out", "%s left the roll held: %s", q, w.picture())
		return
	}
	for _, s := range w.sides {
		if s.paused {
			w.note("paused-after-the-way-out", "%s left a side paused: %s", q, w.picture())
			return
		}
	}
	if class := m.heldFailedAttemptClass(); class != "" {
		w.note(class, "%s left the roll waiting on an Instance the partition holds with a failed attempt toward the target: %s", q, w.picture())
		return
	}
	w.classifyEnd(true)
}

// heldFailedAttemptClass reads the picture of a roll whose every unfinished
// side waits on Instances the partition holds with a failed attempt toward
// the target, named by the side's hold (the RetryBlock gate with the
// partition in its reason). Empty for any other picture.
func (m *heldModel) heldFailedAttemptClass() string {
	w := m.w
	class := ""
	for _, s := range w.sides {
		if w.sideComplete(s) {
			continue
		}
		hold := s.h.hold
		if hold == nil || hold.Gate != workloadtypes.RolloutHoldGateRetryBlock || !strings.Contains(hold.Reason, "which the partition of") {
			return ""
		}
		class = knownHeldFailedAttemptNamed
	}
	return class
}

// correctiveFor is the step that ends an entry in a random run: one of
// the correctives for a spent schedule, the resume for a pause, the
// fault's end otherwise.
func (m *heldModel) correctiveFor(e heldEntry, rng *rand.Rand) []heldStep {
	switch e {
	case entrySpentSchedule, entrySpentCrash:
		correctives := m.correctives()
		return correctives[rng.Intn(len(correctives))]
	case entryPaused:
		return []heldStep{moveStep(moveResume, 0, 0)}
	case entryNoRoom:
		return []heldStep{plainStep(stepRoomBack)}
	}
	return []heldStep{plainStep(stepWebhookBack)}
}

// randomRuns plays, from an entry, one or two moves drawn at random from
// the scale model's held-moment moves, then the entry's corrective step,
// under a fixed seed; every such run owes a complete roll. The moves are
// drawn one at a time on the world as it stands after the one before, so
// a move always fits the state it lands on (a policy edit follows no
// policy removal), and the recorded steps are then replayed from the
// entry.
func (m *heldModel) randomRuns(entry *scaleSnapshot, e heldEntry, seed int64) {
	w := m.w
	if w.decode() == nil {
		return
	}
	runs := 1
	if m.full {
		runs = 8
	}
	rng := rand.New(rand.NewSource(seed))
	for run := 0; run < runs; run++ {
		w.restore(entry)
		var steps []heldStep
		for k := 1 + rng.Intn(2); k > 0; k-- {
			mv, ok := m.drawMove(rng)
			if !ok {
				continue
			}
			step := heldStep{kind: stepMove, move: mv}
			steps = append(steps, step)
			m.applyStep(step)
			w.tick()
		}
		steps = append(steps, m.correctiveFor(e, rng)...)
		m.runs++
		m.play(entry, wayOut(owesComplete, steps...))
	}
}

// drawMove draws one of the scale model's held-moment moves that fits the
// world as it stands: the list names the policy moves from the
// configuration's setting, so a policy is edited or removed only while
// one stands and added only while none does. False when eight draws fit
// nothing.
func (m *heldModel) drawMove(rng *rand.Rand) (scaleMove, bool) {
	moves := m.w.allMoves(scaleAtHeld)
	for try := 0; try < 8; try++ {
		mv := moves[rng.Intn(len(moves))]
		switch mv.kind {
		case movePolicyEdit, movePolicyRemove:
			if m.w.policy == nil {
				continue
			}
		case movePolicyAdd:
			if m.w.policy != nil {
				continue
			}
		}
		return mv, true
	}
	return scaleMove{}, false
}

// explore runs one configuration: the plain roll with its moments kept
// and the held roll, then every entry at its moments with every way out
// played from it, then the random runs.
func (m *heldModel) explore(seed int64) {
	w := m.w
	w.reachIdle()
	snaps := w.captureMoments(m.bound)
	for _, e := range heldEntries {
		m.entry = e
		for _, start := range m.startsFor(e, snaps) {
			if start.snap == nil {
				continue
			}
			m.reachEntry(e, start)
			entry := w.snapshot()
			for _, q := range m.sequencesFrom(e) {
				m.play(entry, q)
			}
			m.randomRuns(entry, e, seed)
		}
	}
}

// The known picture of this model: reported, not failed, until a fix
// removes its entry and the lock test that holds it.
const knownHeldFailedAttemptNamed = "known-held-failed-attempt-named-by-the-hold"

// knownHeldExitClasses are the finding classes this model shows on main
// today, each with the picture behind it.
var knownHeldExitClasses = map[string]string{
	knownHeldFailedAttemptNamed: "an Instance whose attempt toward the target failed under the spent schedule is held by a partition after the release (a canary step's partition raised by a scale-up, or the operator's) and the roll never retries it while the partition holds it; the hold names the Instance and the two ways out, lowering the partition (the canary's next step does) or pushing another revision (held by TestHeldExits_ACanaryStepRaisedByAScaleUpHoldsTheFailedInstanceUntilThePromote and TestHeldExits_APartitionSetAtHeldNamesTheFailedInstancesItHoldsAfterTheRelease)",
}

// heldExitsExpected reports an end the engine owes, counted and not
// failed: the roll complete, held with no corrective step taken, paused
// with the pause standing, held again after a release of a revision still
// broken, waiting on a fault still standing, or an entry's own end.
func heldExitsExpected(class string) bool {
	switch class {
	case "complete", "held", "ends-paused", "held-again-after-release", "fault-still-standing":
		return true
	}
	return strings.HasPrefix(class, "entry-")
}

// heldResult is what one configuration's exploration found.
type heldResult struct {
	cfg                                                     scaleConfig
	states, ticks, passes, gateCalls, probes, skipped, runs int
	counts                                                  map[string]int
	findings                                                map[string]scaleFinding
	took                                                    time.Duration
}

// heldExitsExplore runs the model over every configuration, one subtest
// per configuration in parallel, and reports every class, failing on the
// classes that are neither owed ends nor known.
func heldExitsExplore(t *testing.T) {
	t.Helper()
	full := os.Getenv(heldExitsFullEnv) == "full"
	only := os.Getenv(heldExitsOnlyEnv)
	start := time.Now()
	var configs []scaleConfig
	for _, cfg := range scaleConfigs(full) {
		if only == "" || strings.Contains(cfg.String(), only) {
			configs = append(configs, cfg)
		}
	}
	results := make([]heldResult, len(configs))
	t.Run("configurations", func(t *testing.T) {
		for i, cfg := range configs {
			t.Run(strings.ReplaceAll(cfg.String(), "/", "_"), func(t *testing.T) {
				t.Parallel()
				began := time.Now()
				w := newScaleWorld(t, cfg)
				m := newHeldModel(w, full)
				m.explore(int64(i + 1))
				results[i] = heldResult{cfg: cfg, states: len(w.seen), ticks: w.ticks, passes: w.passes, gateCalls: w.gateCalls,
					probes: m.probes, skipped: m.skipped, runs: m.runs, counts: w.counts, findings: w.findings, took: time.Since(began)}
			})
		}
	})
	heldExitsReport(t, configs, results, time.Since(start))
}

// heldExitsReport prints every configuration's counts and every class with
// its examples, and fails the classes the engine does not owe.
func heldExitsReport(t *testing.T, configs []scaleConfig, results []heldResult, took time.Duration) {
	t.Helper()
	totals, configurations := map[string]int{}, map[string]int{}
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
		t.Logf("config %d/%d [%s]: %d states, %d ticks, %d ways out (%d skipped past the tick budget), %d random runs, %s: %s",
			n+1, len(configs), res.cfg, res.states, res.ticks, res.probes, res.skipped, res.runs, res.took.Round(time.Second), strings.Join(parts, " "))
		for class, f := range res.findings {
			configurations[class]++
			if len(examples[class]) < 6 {
				examples[class] = append(examples[class], scaleExample{cfg: res.cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d ticks, %d passes, %d gate questions, %d ways out (%d skipped past the tick budget), %d random runs, %s",
		len(configs), states, ticks, passes, gateCalls, probes, skipped, runs, took.Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d states in %d configurations", class, totals[class], configurations[class])
		if heldExitsExpected(class) {
			continue
		}
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.finding.message, ex.finding.path, ex.finding.state)
		}
		if known := knownHeldExitClasses[class]; known != "" {
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

// TestHeldExits_EveryWayOutEndsTheRoll explores every configuration: a
// roll held by a spent retry schedule, a pause, a cluster with no room or
// a refusing webhook, and every way out of it the operator or the world
// has, ends complete at every side's count, or in the end the engine owes
// (held again after a release of a revision still broken, held with no
// corrective step taken, paused with the pause standing, waiting on a
// fault still standing), within its budgets.
func TestHeldExits_EveryWayOutEndsTheRoll(t *testing.T) {
	heldExitsExplore(t)
}

// heldWorldAt builds a configuration's world, pushes the decoder's image no
// pod can pull and plays the roll until the decoder's retry schedule is
// spent, failing the test when it never is.
func heldWorldAt(t *testing.T, cfg scaleConfig) (*scaleWorld, *heldModel) {
	t.Helper()
	w := newScaleWorld(t, cfg)
	m := newHeldModel(w, false)
	w.reachIdle()
	w.pushHeld()
	if !w.playOut(m.bound) || !w.momentReached(scaleAtHeld) {
		t.Fatalf("the held side's retry schedule was not spent: %s", w.picture())
	}
	return w, m
}

// heldFailedRows lists a side's Failed rows the partition holds, with the
// pods each has, one line per row.
func heldFailedRows(w *scaleWorld, s *scaleSide) []string {
	held := w.heldIndices(s)
	byIndex := w.livePodsByIndex(s)
	var out []string
	for _, r := range w.rows(s) {
		if r.Phase != workloadtypes.InstancePhaseFailed || !held[r.Index] {
			continue
		}
		var pods []string
		for _, pod := range byIndex[r.Index] {
			pods = append(pods, fmt.Sprintf("%s on %s %s ready=%t", pod.Name, scaleImageShort(pod.Spec.Containers[0].Image), pod.Status.Phase, podConditionTrue(pod, corev1.PodReady)))
		}
		step := "no operation"
		if r.Operation != nil {
			step = fmt.Sprintf("%s/%s toward %s waiting %q", r.Operation.Type, r.Operation.Step, w.short(s, r.Operation.TargetRevision), r.Operation.Waiting)
		}
		out = append(out, fmt.Sprintf("Instance %d: %s on %s, target %s, %s, pods [%s]", r.Index, r.Phase, w.short(s, r.RunningRevision), w.short(s, r.TargetRevision), step, strings.Join(pods, "; ")))
	}
	return out
}

// TestHeldExits_APartitionSetAtHeldNamesTheFailedInstancesItHoldsAfterTheRelease:
// 15 prefill and 15 decode under RecreatePod at ten percent with keepRatio
// and the decoder first; both sides are pushed to an image no pod can pull
// until the decoder's retry schedule is spent. The operator sets a
// partition of 2 on the prefill side, the registry serves the image
// again, and the operator releases the held revision. Both sides roll; the
// partition holds the two lowest prefill Instances, whose recreate
// attempts had failed under the spent schedule: their old pods are gone,
// their rows read Failed with the attempt stopped and kept on the row, and nothing serves on
// them. The prefill side must name them in its hold with the two ways
// out, and clearing the partition must end the roll.
func TestHeldExits_APartitionSetAtHeldNamesTheFailedInstancesItHoldsAfterTheRelease(t *testing.T) {
	recreate := workloadtypes.UpdateStrategyRecreatePod
	cfg := scaleConfig{shape: scaleShape{15, 15, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, keepRatio: true, firstSide: v1beta1.DecoderComponent}}
	w, m := heldWorldAt(t, cfg)
	for _, st := range []heldStep{moveStep(movePartition, 0, 2), plainStep(stepImageBack), plainStep(stepRelease)} {
		m.applyStep(st)
		w.tick()
	}
	if !w.playOut(m.bound) {
		t.Fatalf("the world did not settle after the release: %s", w.picture())
	}
	prefill := w.prefill()
	rows := heldFailedRows(w, prefill)
	t.Logf("after the release: %s\n  %s", w.picture(), strings.Join(rows, "\n  "))
	if w.complete() || w.sideComplete(prefill) {
		t.Fatalf("the picture moved: the prefill side ended complete under the partition; retire this lock: %s", w.picture())
	}
	if len(rows) == 0 {
		t.Fatalf("the picture moved: no held prefill Instance reads Failed; retire this lock: %s", w.picture())
	}
	hold := prefill.h.hold
	if hold == nil || hold.Gate != workloadtypes.RolloutHoldGateRetryBlock || !strings.Contains(hold.Reason, "Instances 0, 1") ||
		!strings.Contains(hold.Reason, "partition of 2") || !strings.Contains(hold.Reason, "lower the partition") {
		t.Fatalf("the prefill side must name the two held Instances, the partition and the ways out; got %v", hold)
	}
	m.applyStep(moveStep(movePartition, 0, 0))
	if !w.playOut(m.bound) || !w.complete() {
		t.Fatalf("clearing the partition must end the roll: %s", w.picture())
	}
	t.Logf("clearing the partition ends the roll: %s", w.picture())
}

// TestHeldExits_ACanaryStepRaisedByAScaleUpHoldsTheFailedInstanceUntilThePromote
// holds the picture the model found. 20 prefill and 10 decode, the prefill
// under RecreatePod and the decoder under SurgeThenDrain at ten percent,
// rolled as a canary unit stepping 25 then 100 percent; the decoder is
// pushed to an image no pod can pull until its retry schedule is spent.
// The operator scales the decoder to 11, the registry serves the image
// again, and the operator releases the held revision. The step's partition
// now holds 8 of 11, Instance 7 among them, whose replacement had failed
// under the spent schedule; the decoder rolls 3 and then holds, naming
// Instance 7, the partition and the two ways out. Promoting the canary
// lowers the partition and ends the roll. When the step retries the held
// Instance on its own, this test fails and the known class goes with it.
func TestHeldExits_ACanaryStepRaisedByAScaleUpHoldsTheFailedInstanceUntilThePromote(t *testing.T) {
	cfg := scaleConfig{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: workloadtypes.UpdateStrategyRecreatePod, decode: workloadtypes.UpdateStrategySurgeThenDrain, canary: true}}
	w, m := heldWorldAt(t, cfg)
	for _, st := range []heldStep{moveStep(moveScale, 1, 1), plainStep(stepImageBack), plainStep(stepRelease)} {
		m.applyStep(st)
		w.tick()
	}
	if !w.playOut(m.bound) {
		t.Fatalf("the world did not settle after the release: %s", w.picture())
	}
	decode := w.decode()
	rows := heldFailedRows(w, decode)
	t.Logf("after the release: %s\n  %s", w.picture(), strings.Join(rows, "\n  "))
	if w.complete() {
		t.Fatalf("the picture moved: the roll ended at the step; remove the known class and this lock: %s", w.picture())
	}
	hold := decode.h.hold
	if hold == nil || hold.Gate != workloadtypes.RolloutHoldGateRetryBlock || !strings.Contains(hold.Reason, "which the partition of") {
		t.Fatalf("the picture moved: the decoder's hold reads %v, not the held Instance with its failed replacement; remove the known class and this lock", hold)
	}
	m.applyStep(moveStep(moveCanaryPromote, 0, 0))
	if !w.playOut(m.bound) || !w.complete() {
		t.Fatalf("promoting the canary must end the roll: %s", w.picture())
	}
	t.Logf("promoting the canary ends the roll: %s", w.picture())
}
