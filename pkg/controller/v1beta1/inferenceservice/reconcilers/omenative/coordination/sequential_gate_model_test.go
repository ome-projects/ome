package coordination_test

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
)

// This file is the gate model of the Sequential rule over the reference
// chains. It runs the world of
// sequential_chains_always_reach_their_end_test.go (the production gate
// stack, the run layer and the group machine over published replicas)
// through one plain roll of each chain, stops at every moment of that roll
// (the push landed, the replicas projected, the first member's first batch
// serving, its last starts open, the wait after it running, the second and
// the third member's first batch serving, the chain at rest), lands one
// operator move or fault there (a partition set, raised, lowered or cleared
// on any member; a scale of any member up or down by one or by half; a pod
// or a gang member lost; an attempt failed; a pause; a second push; a push
// back to version 1), plays the roll out with the moves that advance it by
// themselves, and names the end it reaches and every answer the gate gave
// on the way. The chains are the reference prefill:decode shapes in every
// member order (the engine then the decoder, the decoder then the engine, a
// router of two behind the pair or ahead of it) under every strategy pair,
// with and without a wait after readiness, with a partition in place
// before the push, single pods and gangs. The full mode also explores
// every configuration breadth-first with the chain model's own moves and
// names the ends the walk reaches. An end outside the table of ends, an
// answer outside the table of answers, a per-state reading outside the
// chain model's known classes, or a listed end the mode no longer reaches,
// fails.
//
// A canary step's partition is not among the moves: a canary group's
// members are never chain members (ResolveGroups skips canary groups), and
// the projector clears spec.pacing.partition on every member no canary
// drives, so no chain member carries a step.
//
// OME_SEQUENTIAL_HUNT=full runs the whole matrix of chains and the walk for
// a long local run; OME_SEQUENTIAL_HUNT_ONLY=<text> keeps the configurations
// whose name contains the text.

// seqGateMoment is a point of a plain chain roll an operator move lands at.
type seqGateMoment string

const (
	seqMomentPushed         seqGateMoment = "pushed"          // the push landed; no replica projected yet
	seqMomentProjected      seqGateMoment = "projected"       // every replica projected; the first member's first starts open
	seqMomentFirstRotating  seqGateMoment = "first-rotating"  // the first member's first batch serves and its next starts are open
	seqMomentFirstLast      seqGateMoment = "first-last"      // the first member's last starts are open
	seqMomentWaiting        seqGateMoment = "waiting"         // the first member finished; the wait after it runs
	seqMomentSecondRotating seqGateMoment = "second-rotating" // the second member's first batch serves and its next starts are open
	seqMomentThirdRotating  seqGateMoment = "third-rotating"  // the third member's first batch serves and its next starts are open
	seqMomentRest           seqGateMoment = "rest"            // the plain roll played out to its end
	seqMomentWalk           seqGateMoment = "walk"            // a state of the breadth-first walk, not a moment of the plain roll
)

// seqGatePlayOutBound is the most moves a play-out makes before it is read
// as never ending; a chain of 30 prefill and 10 decode pushed twice needs
// about a hundred.
const seqGatePlayOutBound = 240

// seqGateClass is one end a probe reaches, or one answer the gate gives:
// its reading in words and whether the bounded and the full mode reach it
// at present.
type seqGateClass struct {
	words         string
	bounded, full bool
}

// seqGateEnds names every end a probe or the walk reaches at present.
// An end outside the table fails, and a listed end the mode no longer
// reaches fails too, so a change of the engine that removes an end is seen
// and the table rewritten with it.
var seqGateEnds = map[string]seqGateClass{
	"ended-run-closed": {bounded: true, full: true,
		words: "every member rests on the pushed version with every Instance Ready and the run closed Completed; the next push opens a fresh run"},
	"ended-back-on-version-one": {bounded: true, full: true,
		words: "after the push back every member rests on version 1 with every Instance Ready; the run toward version 2 was replaced by one toward version 1, which closed Completed; no later member started before its turn"},
	"ended-on-version-three": {bounded: true, full: true,
		words: "after the second push every member rests on version 3 with every Instance Ready; the run toward version 2 was replaced by one toward version 3, which closed Completed"},
	"ended-at-a-partition-run-closed": {bounded: false, full: false,
		words: "every member rests at the shape its spec asks for, partition included, and the run closed Completed: the close read a member's rest on its own rows (the current engine reaches it through no chain)"},
	"rests-at-a-partition-with-no-run-opened": {bounded: true, full: true,
		words: "the push found every member's partition holding every Instance it would have moved, so no member diverged and no run opened: the service rests with the spec on version 2, every Instance on version 1 under the partitions and no run on file, until a partition is lowered or the next push asks for more than the partitions hold (a member resting at the shape its partition asks for is not pending work)"},
	"rests-at-a-partition-with-no-run-after-a-retarget": {bounded: false, full: true,
		words: "a push or a push back retargeted the run, which closed with the outcome a retarget gives it, and every member then rests at its partition (the Instances off the new target are no more than the partition holds), which is no divergence, so no run opens: the service rests with its last run closed by the retarget and a version mix under the partitions until the partition is lowered or the next push asks for more than it holds (the push back's own picture is told apart below)"},
	"rests-at-a-partition-on-the-rejected-version-with-no-run-after-a-push-back": {bounded: true, full: true,
		words: "after the push back to version 1 a member rests at its partition with the held Instances on version 2, and no run is open: the push back retargeted the run toward version 2, which closed with the outcome a retarget gives it, the held Instances are the only ones off version 1, and a member resting at its partition is no divergence, so no run opens toward version 1; the service rests with its last run closed by the retarget and the rejected version serving under the partition"},
	"known-run-stays-open-while-a-member-rests-at-its-partition": {bounded: true, full: true,
		words: "the chain reached its end with a member resting at its partition (between one and the partition of its Instances on another version), the group reads Idle, and the run stays open for good: the run layer closes a run on an unconverged member only through a Staged group phase, which a Sequential chain never computes, and the staged member diverges no more, so no new run opens either (a gap)"},
	"rests-at-a-partition-on-the-rejected-version-after-a-push-back": {bounded: true, full: true,
		words: "after the push back to version 1 a member rests at its partition with the held Instances on version 2, the version the operator pushed back from: the partition holds the lowest-indexed Instances off the target, and after a push back those are Instances on the rejected version, while the Instances it held before already run version 1; the run toward version 1 stays open as above"},
	"known-held-record-outlives-the-retired-instance": {bounded: true, full: true,
		words: "an Instance whose retries are spent is retired by a scale-down and its Held retry record stays: every Instance runs the new version, the member's current revision never moves, and the run never closes until a new revision or a release of the held revision (a gap)"},
	"known-retries-spent": {bounded: true, full: true,
		words: "the only unfinished member's retries are spent and nothing waits on it; a new revision or a release of the held revision ends it"},
	"known-spent-retries-hold-the-chain": {bounded: true, full: true,
		words: "an earlier member whose retries are spent with Instances left on the old version keeps every later member waiting under \"Sequential waiting on <c>\"; the hold names the member, not its spent retries"},
	"known-failed-replacement-toward-the-target-waits-under-the-partition": {bounded: false, full: true,
		words: "a surge replacement that failed toward the roll target stands under an Instance the partition holds, and its retry is a start the partition refuses; the member holds with the Instance and the exits named until the operator lowers the partition or pushes another revision (by design)"},
	"known-failed-replacement-under-a-held-instance-keeps-its-slot": {bounded: false, full: true,
		words: "a surge replacement that failed beside its serving source keeps its surge slot; once a partition holds that Instance no start comes, and under a surge budget of one the member's movable Instances never start (left open)"},
}

// seqGateAnswers names every answer the gate stack gives a start in a
// chain, by its reading. An answer outside the table fails, and a listed
// answer the mode no longer hears fails too.
var seqGateAnswers = map[string]seqGateClass{
	"admitted: the member's own turn": {bounded: true, full: true,
		words: "the Sequential gate names the asking member as the active one, every member before it done for the next member's turn, no wait running, and admits the start"},
	"held: no run open yet": {bounded: true, full: true,
		words: "the plan gate holds every start between the push and the pass the run layer opens the run on, which follows the pass the member's status records the new target"},
	"held: Sequential waiting on an earlier member": {bounded: true, full: true,
		words: "the Sequential gate names an earlier member of the chain as the active one and holds the start"},
	"held: the wait after the earlier member runs": {bounded: true, full: true,
		words: "the member's turn has come and the wait after the previous member's recorded finish has not passed"},
	"held: the earlier member is not Ready under the wait": {bounded: true, full: true,
		words: "the member's turn has come and the previous member's Ready condition reads False (a pod lost under its serving minimum, a scale-up Instance still building), so the start waits for its readiness; the wait's clock stands"},
	"held: the group's unavailability budget": {bounded: true, full: true,
		words: "the group's own unavailability budget (the operator default of 25 percent of the member) is spent by the member's drains and rebuilds in flight: a lost pod being rebuilt counts against it"},
}

// seqGateShapes are the reference prefill:decode shapes,
// prefill first.
var seqGateShapes = [][2]int32{{12, 8}, {12, 4}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}}

// seqGateRouters is the router count behind a chain; the reference shapes run two
// to four under SurgeThenDrain with a surge budget of one.
const seqGateRouters int32 = 2

// seqGateOrder is one member order of a chain.
type seqGateOrder struct {
	name  string
	comps []v1beta1.ComponentType
}

// seqGateOrders are the member orders: the engine then the decoder, the
// decoder then the engine, the router of two behind the pair, and the
// router ahead of it.
func seqGateOrders() []seqGateOrder {
	eng, dec, rtr := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	return []seqGateOrder{
		{name: "engine then decoder", comps: []v1beta1.ComponentType{eng, dec}},
		{name: "decoder then engine", comps: []v1beta1.ComponentType{dec, eng}},
		{name: "engine, decoder, then router", comps: []v1beta1.ComponentType{eng, dec, rtr}},
		{name: "router, then engine, then decoder", comps: []v1beta1.ComponentType{rtr, eng, dec}},
	}
}

// seqGateStrategyPairs are the strategies of the first and the second
// non-router member: both drain first, both surge, and the two mixed pairs.
func seqGateStrategyPairs() [][2]v1beta1.UpdateStrategyType {
	drain, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	return [][2]v1beta1.UpdateStrategyType{{drain, drain}, {surge, surge}, {drain, surge}, {surge, drain}}
}

// seqGateBudgets are the moves a probe may spend: one of each fault and
// operator move, two partition edits so a set can be followed by its
// clearing.
func seqGateBudgets() seqBudgets {
	return seqBudgets{fails: 1, losses: 1, scales: 1, partitionEdits: 2, pushes: 1, rollbacks: 1, pauses: 1}
}

// seqGateConfig builds one chain: the members in the order's sequence, the
// engine and the decoder at the shape's counts under the pair's strategies
// at 10 percent, the router of two surging one at a time, the partitions
// in chain order.
func seqGateConfig(shape [2]int32, order seqGateOrder, pair [2]v1beta1.UpdateStrategyType, wait time.Duration, partitions []int32, retries int32) seqConfig {
	cfg := seqConfig{afterReady: wait, maxAttempts: retries, budgets: seqGateBudgets()}
	side := 0
	for i, comp := range order.comps {
		m := seqMemberConfig{comp: comp, pods: 1, budget: "10%"}
		switch comp {
		case v1beta1.EngineComponent:
			m.desired = shape[0]
		case v1beta1.DecoderComponent:
			m.desired = shape[1]
		default:
			m.desired, m.budget = seqGateRouters, "1"
		}
		if comp == v1beta1.RouterComponent {
			m.strategy = v1beta1.UpdateStrategySurgeThenDrain
		} else {
			m.strategy = pair[side]
			side++
		}
		if i < len(partitions) {
			m.partition = partitions[i]
		}
		cfg.members = append(cfg.members, m)
	}
	cfg.name = cfg.String()
	return cfg
}

// seqGateConfigs lists the chains to drive. The full mode runs the whole
// matrix: every reference shape in every member order under every
// strategy pair, with and without a wait, a partition of two in place
// before the push on the first or the second member of a two-member chain,
// gangs of four and two and of eight pods, and one attempt before the
// retries stop. The bounded mode keeps a covering subset.
func seqGateConfigs(full bool) []seqConfig {
	if !full {
		return seqGateBoundedConfigs()
	}
	var out []seqConfig
	waits := []time.Duration{0, 10 * time.Minute}
	for _, shape := range seqGateShapes {
		for _, order := range seqGateOrders() {
			for _, pair := range seqGateStrategyPairs() {
				for _, wait := range waits {
					out = append(out, seqGateConfig(shape, order, pair, wait, nil, 3))
				}
				if len(order.comps) > 2 {
					continue
				}
				for _, partitions := range [][]int32{{2, 0}, {0, 2}} {
					for _, wait := range waits {
						out = append(out, seqGateConfig(shape, order, pair, wait, partitions, 3))
					}
				}
			}
		}
		engineFirst := seqGateOrders()[0]
		for _, pair := range seqGateStrategyPairs()[:2] {
			out = append(out, seqGateConfig(shape, engineFirst, pair, 10*time.Minute, nil, 3).withPods(4, 2))
			out = append(out, seqGateConfig(shape, engineFirst, pair, 10*time.Minute, nil, 3).withPods(8, 8))
		}
		out = append(out, seqGateConfig(shape, engineFirst, seqGateStrategyPairs()[0], 0, nil, 1))
	}
	return out
}

// seqGateBoundedConfigs is the bounded mode's covering subset: every
// reference shape, every order, every strategy pair, both waits, a
// partition before the push on the first and on the second member, gangs,
// and one attempt before the retries stop.
func seqGateBoundedConfigs() []seqConfig {
	orders, pairs := seqGateOrders(), seqGateStrategyPairs()
	wait := 10 * time.Minute
	return []seqConfig{
		seqGateConfig(seqGateShapes[0], orders[0], pairs[0], 0, nil, 3),
		seqGateConfig(seqGateShapes[0], orders[0], pairs[1], wait, nil, 3),
		seqGateConfig(seqGateShapes[1], orders[1], pairs[0], wait, nil, 3),
		seqGateConfig(seqGateShapes[3], orders[0], pairs[2], 0, []int32{2, 0}, 3),
		seqGateConfig(seqGateShapes[4], orders[1], pairs[3], wait, []int32{0, 2}, 3),
		seqGateConfig(seqGateShapes[5], orders[2], pairs[0], wait, nil, 3),
		seqGateConfig(seqGateShapes[6], orders[3], pairs[1], 0, nil, 3),
		seqGateConfig(seqGateShapes[6], orders[3], pairs[0], wait, nil, 3),
		seqGateConfig(seqGateShapes[2], orders[0], pairs[1], 0, nil, 3).withPods(4, 2),
		seqGateConfig(seqGateShapes[0], orders[0], pairs[0], wait, nil, 1).withPods(8, 8),
		seqGateConfig(seqGateShapes[3], orders[1], pairs[1], 0, []int32{2, 0}, 1),
		seqGateConfig(seqGateShapes[0], orders[2], pairs[3], 0, nil, 3),
	}
}

// seqGateMomentState is the chain at one moment of its plain roll.
type seqGateMomentState struct {
	moment seqGateMoment
	state  *seqState
}

// seqGateMomentStates drives one plain roll of the chain and returns the
// state at each moment it reaches, in order. A moment the chain does not
// reach (a member with nothing to start, a wait the chain has none of) is
// left out, and so is a moment whose state repeats an earlier one.
func (w *seqWorld) seqGateMomentStates() []seqGateMomentState {
	var out []seqGateMomentState
	seen := map[string]bool{}
	add := func(moment seqGateMoment, st *seqState, ok bool) {
		if !ok || seen[st.key()] {
			return
		}
		seen[st.key()] = true
		out = append(out, seqGateMomentState{moment: moment, state: st})
	}
	st := w.initial()
	w.settle(st, w.order(0))
	add(seqMomentPushed, st, true)
	projected := w.projectAll(st)
	add(seqMomentProjected, projected, projected.members[0].inFlight())
	rotating, ok := w.seqGateRotating(projected, 0)
	add(seqMomentFirstRotating, rotating, ok)
	last, ok := w.seqGateLastStarts(projected, 0)
	add(seqMomentFirstLast, last, ok)
	next := w.rollTo(projected, 0)
	if w.cfg.afterReady > 0 {
		add(seqMomentWaiting, next, w.waitRunning(next, 0))
		next = w.seqGateWaitOut(next, 0)
	}
	rotating, ok = w.seqGateRotating(next, 1)
	add(seqMomentSecondRotating, rotating, ok)
	if len(w.cfg.members) > 2 {
		third := w.rollTo(next, 1)
		if w.cfg.afterReady > 0 {
			third = w.seqGateWaitOut(third, 1)
		}
		rotating, ok = w.seqGateRotating(third, 2)
		add(seqMomentThirdRotating, rotating, ok)
	}
	rest, _ := w.seqGatePlayOut(projected, func(*seqState) {})
	add(seqMomentRest, rest, true)
	return out
}

// seqGateRotating serves every start the member has open and reports the
// state with its next starts open; not ok when the member has nothing open
// before or after.
func (w *seqWorld) seqGateRotating(st *seqState, member int) (*seqState, bool) {
	var open int
	for _, r := range st.members[member].rows {
		if r.open() {
			open++
		}
	}
	if open == 0 {
		return st, false
	}
	st = w.serveOn(st, member, open)
	return st, st.members[member].inFlight()
}

// seqGateLastStarts serves the member one Instance at a time until every
// Instance it may still move has started and some are open; not ok when the
// member finishes without such a moment.
func (w *seqWorld) seqGateLastStarts(st *seqState, member int) (*seqState, bool) {
	for round := 0; round < 128; round++ {
		if !st.members[member].inFlight() {
			return st, false
		}
		if w.seqGateUnstarted(st, member) == 0 {
			return st, true
		}
		st = w.serveOn(st, member, 1)
	}
	return st, false
}

// seqGateUnstarted counts the member's Instances off the target that no
// attempt moves and no partition holds.
func (w *seqWorld) seqGateUnstarted(st *seqState, member int) int {
	m := &st.members[member]
	held := w.heldIndices(member, m)
	var n int
	for idx, r := range m.rows {
		if (r.kind == seqReady || r.kind == seqShort) && r.rev != m.target && !held[idx] {
			n++
		}
	}
	return n
}

// seqGateWaitOut lets the wait after a member pass when it is running.
func (w *seqWorld) seqGateWaitOut(st *seqState, member int) *seqState {
	if !w.waitRunning(st, member) {
		return st
	}
	return w.apply(st, seqMove{kind: "wait", member: member})
}

// seqGatePlayOut plays a state out with the moves that advance the roll by
// themselves, the first one offered each time, until the chain is complete
// or nothing advances it any more, visiting every state on the way; ended
// is false when the bound ran out first.
func (w *seqWorld) seqGatePlayOut(st *seqState, visit func(*seqState)) (*seqState, bool) {
	for n := 0; n < seqGatePlayOutBound; n++ {
		visit(st)
		if w.complete(st) {
			return st, true
		}
		moves := w.progressMoves(st)
		if len(moves) == 0 {
			return st, true
		}
		st = w.apply(st, moves[0])
	}
	return st, false
}

// seqGateProbe is one list of moves landed at one moment, each followed by
// a play-out unless the probe lands them together.
type seqGateProbe struct {
	name  string
	moves []seqMove
	// together applies every move before the play-out, as one operator
	// edit; otherwise the engine plays out between the moves.
	together bool
}

// seqGateProbes lists the probes to land at a state: the operator's
// partition and scale on every member, a pod or a gang member lost on
// every member, a failed attempt on every member with one open, a pause, a
// second push and a push back; at rest, the plain roll itself.
func (w *seqWorld) seqGateProbes(st *seqState, moment seqGateMoment) []seqGateProbe {
	var out []seqGateProbe
	if moment == seqMomentRest {
		out = append(out, seqGateProbe{name: "no move"})
	}
	for i := range st.members {
		out = append(out, w.seqGatePartitionProbes(st, i)...)
		out = append(out, w.seqGateScaleProbes(st, i)...)
		out = append(out, w.seqGateFaultProbes(st, i)...)
	}
	if whole := w.seqGateHoldEveryMemberWhole(st); len(whole) > 0 {
		out = append(out, seqGateProbe{name: "partition every member whole", moves: whole, together: true})
	}
	out = append(out, seqGateProbe{name: "pause", moves: []seqMove{{kind: "pause"}}})
	out = append(out, seqGateProbe{name: "push", moves: []seqMove{{kind: "push"}}})
	if st.rev >= 2 {
		out = append(out, seqGateProbe{name: "rollback", moves: []seqMove{{kind: "rollback"}}})
	}
	return out
}

// seqGateHoldEveryMemberWhole is the operator holding every member whole
// before or under a roll: each member's partition set to its Instance
// count, so no Instance of any member may move. Empty when they already are.
func (w *seqWorld) seqGateHoldEveryMemberWhole(st *seqState) []seqMove {
	var moves []seqMove
	for i := range st.members {
		if st.partitions[i] != st.desired[i] {
			moves = append(moves, seqMove{kind: "partition", member: i, value: st.desired[i]})
		}
	}
	return moves
}

// seqGatePartitionProbes are the partition moves on one member: set to
// half, to one and to the whole count, raised by one or cleared when one
// stands, and set to half then cleared.
func (w *seqWorld) seqGatePartitionProbes(st *seqState, member int) []seqGateProbe {
	n := st.desired[member]
	values := []int32{n / 2, 1, n}
	if p := st.partitions[member]; p > 0 {
		values = append(values, 0, p+1)
	}
	var out []seqGateProbe
	seen := map[int32]bool{st.partitions[member]: true}
	for _, v := range values {
		if v < 0 || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, seqGateProbe{name: fmt.Sprintf("partition member %d to %d", member, v), moves: []seqMove{{kind: "partition", member: member, value: v}}})
	}
	if n/2 > 0 && st.partitions[member] != n/2 {
		out = append(out, seqGateProbe{name: fmt.Sprintf("partition member %d to %d then cleared", member, n/2),
			moves: []seqMove{{kind: "partition", member: member, value: n / 2}, {kind: "partition", member: member, value: 0}}})
	}
	return out
}

// seqGateScaleProbes are the scales of one member: up and down by one and
// by half; a scaling policy's follower is resized by the policy, not by
// hand.
func (w *seqWorld) seqGateScaleProbes(st *seqState, member int) []seqGateProbe {
	if w.cfg.scalingPolicy && member == w.cfg.follower() {
		return nil
	}
	n := st.desired[member]
	deltas := []int32{1, -1, n / 2, -(n / 2)}
	var out []seqGateProbe
	seen := map[int32]bool{n: true}
	for _, delta := range deltas {
		v := n + delta
		if v < 1 || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, seqGateProbe{name: fmt.Sprintf("scale member %d to %d", member, v), moves: []seqMove{{kind: "scale", member: member, value: v}}})
	}
	return out
}

// seqGateFaultProbes are the faults on one member: its first serving
// Instance loses every pod, or one pod of its gang; its first open attempt
// fails; and that attempt fails and the member is then scaled down to
// below the failed Instance, which retires it under its retry record.
func (w *seqWorld) seqGateFaultProbes(st *seqState, member int) []seqGateProbe {
	var out []seqGateProbe
	m := &st.members[member]
	ready, open := -1, -1
	for idx, r := range m.rows {
		if ready < 0 && r.kind == seqReady {
			ready = idx
		}
		if open < 0 && r.open() {
			open = idx
		}
	}
	if ready >= 0 {
		out = append(out, seqGateProbe{name: fmt.Sprintf("lose member %d row %d", member, ready), moves: []seqMove{{kind: "lose", member: member, idx: ready}}})
		if w.cfg.members[member].pods > 1 {
			out = append(out, seqGateProbe{name: fmt.Sprintf("lose a gang member of member %d row %d", member, ready), moves: []seqMove{{kind: "lose-member", member: member, idx: ready}}})
		}
	}
	if open >= 0 {
		out = append(out, seqGateProbe{name: fmt.Sprintf("fail member %d row %d", member, open), moves: []seqMove{{kind: "fail", member: member, idx: open}}})
	}
	if open >= 1 {
		out = append(out, seqGateProbe{name: fmt.Sprintf("fail member %d row %d then scale to %d", member, open, open),
			moves: []seqMove{{kind: "fail", member: member, idx: open}, {kind: "scale", member: member, value: int32(open)}}})
	}
	return out
}

// seqGateEndClass names the end a play-out or the walk reached: the chain
// complete on the version the spec names, or the stopped state's class as
// the chain model classifies it, a rest at a partition after a push back
// told apart.
func (w *seqWorld) seqGateEndClass(st *seqState, ended bool) string {
	if !ended {
		return "depth"
	}
	if w.complete(st) {
		if w.seqGateRestsAtAPartition(st) {
			return w.seqGateRestWithNoRunClass(st)
		}
		switch st.rev {
		case 1:
			return "ended-back-on-version-one"
		case 3:
			return "ended-on-version-three"
		}
		return "ended-run-closed"
	}
	class, _ := w.classifyStopped(st)
	if class == "known-run-stays-open-while-a-member-rests-at-its-partition" && st.rev == 1 && w.seqGateHeldOnALaterVersion(st) {
		return "rests-at-a-partition-on-the-rejected-version-after-a-push-back"
	}
	return class
}

// seqGateRestsAtAPartition reports a member resting at its partition.
func (w *seqWorld) seqGateRestsAtAPartition(st *seqState) bool {
	for i := range st.members {
		if w.restsAtPartition(&st.members[i], st.rev) {
			return true
		}
	}
	return false
}

// seqGateRestWithNoRunClass names a chain at rest at a partition with no
// run open: a push the partitions held whole, the close that read the rest
// on the rows, or the push back whose retarget closed the run with nothing
// left to open one for.
func (w *seqWorld) seqGateRestWithNoRunClass(st *seqState) string {
	switch {
	case st.lastRunOutcome() == "":
		return "rests-at-a-partition-with-no-run-opened"
	case st.lastRunOutcome() == v1beta1.RolloutRunCompleted:
		return "ended-at-a-partition-run-closed"
	case st.lastRunOutcome() != v1beta1.RolloutRunSuperseded:
		return "rests-at-a-partition-with-no-run"
	case st.rev == 1 && w.seqGateHeldOnALaterVersion(st):
		return "rests-at-a-partition-on-the-rejected-version-with-no-run-after-a-push-back"
	}
	return "rests-at-a-partition-with-no-run-after-a-retarget"
}

// seqGateHeldOnALaterVersion reports a member whose partition holds an
// Instance on a version above the spec's.
func (w *seqWorld) seqGateHeldOnALaterVersion(st *seqState) bool {
	for i := range st.members {
		m := &st.members[i]
		if !w.restsAtPartition(m, st.rev) {
			continue
		}
		for _, r := range m.rows {
			if r.rev > st.rev {
				return true
			}
		}
	}
	return false
}

// seqGateAnswerClass names one answer of the gate stack: an admitted start
// by the Sequential gate's own reason for it, a held one by the gate and the
// reason.
func (w *seqWorld) seqGateAnswerClass(st *seqState, member int, allowed bool, gate v1beta1.RolloutHoldGate, reason string) string {
	if allowed {
		_, own := coordination.CheckSequentialGate(w.ctx, w.reader(st), st.service, w.cfg.members[member].comp)
		switch own {
		case "Sequential active Component":
			return "admitted: the member's own turn"
		case "Sequential group idle":
			return "admitted: no member in flight"
		}
		return "admitted: " + own
	}
	switch gate {
	case v1beta1.RolloutHoldGatePlan:
		return "held: no run open yet"
	case v1beta1.RolloutHoldGateBudget:
		if strings.Contains(reason, "surge") {
			return "held: the group's surge budget"
		}
		return "held: the group's unavailability budget"
	case v1beta1.RolloutHoldGateSequential:
		return w.seqGateSequentialHoldClass(member, reason)
	}
	return fmt.Sprintf("held: %s: %s", gate, reason)
}

// seqGateSequentialHoldClass names a Sequential hold: a wait on an earlier
// or a later member of the chain, the wait after readiness running, or the
// earlier member not Ready under it.
func (w *seqWorld) seqGateSequentialHoldClass(member int, reason string) string {
	switch {
	case strings.HasPrefix(reason, "Sequential waiting on "):
		active := v1beta1.ComponentType(strings.TrimPrefix(reason, "Sequential waiting on "))
		for i, mc := range w.cfg.members {
			if mc.comp != active {
				continue
			}
			if i < member {
				return "held: Sequential waiting on an earlier member"
			}
			return "held: Sequential waiting on a later member"
		}
	case strings.Contains(reason, "held while"):
		return "held: the earlier member is not Ready under the wait"
	case strings.Contains(reason, "waiting out"):
		return "held: the wait after the earlier member runs"
	}
	return "held: Sequential: " + reason
}

// seqGateExample is the first picture of one class in one configuration.
type seqGateExample struct {
	cfg     string
	moment  seqGateMoment
	probe   string
	message string
	state   *seqState
}

// seqGateTally gathers what one configuration's probes and walk found.
type seqGateTally struct {
	probes, states int
	ends           map[string]int // probes and walk ends per end
	answers        map[string]int // gate answers per class
	readings       map[string]int // states per per-state class
	examples       map[string]seqGateExample
}

func newSeqGateTally() *seqGateTally {
	return &seqGateTally{ends: map[string]int{}, answers: map[string]int{}, readings: map[string]int{}, examples: map[string]seqGateExample{}}
}

// example keeps the first picture of a class.
func (ta *seqGateTally) example(class string, ex seqGateExample) {
	if _, ok := ta.examples[class]; !ok {
		ta.examples[class] = ex
	}
}

// read tallies the chain model's readings of one state.
func (ta *seqGateTally) read(w *seqWorld, st *seqState, moment seqGateMoment, probe string) {
	ta.states++
	for _, f := range w.check(st) {
		ta.readings[f.class]++
		ta.example(f.class, seqGateExample{cfg: w.cfg.name, moment: moment, probe: probe, message: f.message, state: st})
	}
}

// end tallies the end a path reached.
func (ta *seqGateTally) end(w *seqWorld, st *seqState, ended bool, moment seqGateMoment, probe string) {
	class := w.seqGateEndClass(st, ended)
	ta.ends[class]++
	ta.example(class, seqGateExample{cfg: w.cfg.name, moment: moment, probe: probe, message: w.seqGateEndWords(st), state: st})
}

// seqGateRunProbe lands a probe's moves at a moment, each followed by a
// play-out, reads every state on the way with the chain model's checks, and
// tallies the end.
func (w *seqWorld) seqGateRunProbe(ta *seqGateTally, ms seqGateMomentState, probe seqGateProbe) {
	ta.probes++
	st, ended := ms.state, true
	visit := func(s *seqState) { ta.read(w, s, ms.moment, probe.name) }
	for _, mv := range probe.moves {
		st = w.apply(st, mv)
		if !probe.together {
			st, ended = w.seqGatePlayOut(st, visit)
		}
	}
	if len(probe.moves) == 0 || probe.together {
		st, ended = w.seqGatePlayOut(st, visit)
	}
	ta.end(w, st, ended, ms.moment, probe.name)
}

// seqGateWalk explores the configuration breadth-first with the chain
// model's own moves and seeded runs, naming the end of every state the
// walk cannot move on from.
func (w *seqWorld) seqGateWalk(ta *seqGateTally) {
	w.visit = func(st *seqState) {
		ta.states++
		if w.complete(st) || len(w.progressMoves(st)) == 0 {
			ta.end(w, st, true, seqMomentWalk, seqPath(st))
		}
	}
	defer func() { w.visit = nil }()
	rep := w.explore(200, 48)
	w.randomRuns(&rep, 1, 8, 192)
	for class, n := range rep.counts {
		ta.readings[class] += n
		f := rep.findings[class]
		ta.example(class, seqGateExample{cfg: w.cfg.name, moment: seqMomentWalk, probe: seqPath(f.state), message: f.message, state: f.state})
	}
}

// seqGateEndWords is an end in words: the run, the group and each member's
// hold.
func (w *seqWorld) seqGateEndWords(st *seqState) string {
	parts := make([]string, 0, len(st.members))
	for i, mc := range w.cfg.members {
		parts = append(parts, fmt.Sprintf("%s holds on %s", mc.comp, st.holds[i]))
	}
	run := "no run open, last run " + string(st.lastRunOutcome())
	if r := st.activeRun(); r != nil {
		run = "run " + r.RunID + " open"
	}
	return run + "; group " + w.groupPhases(st) + "; " + strings.Join(parts, "; ")
}

// seqGateSummary gathers what every configuration found.
type seqGateSummary struct {
	mu                                                     sync.Mutex
	configs, probes, states, gateCalls, runPasses, moments int
	ends, answers, readings                                map[string]int
	endsSeen, answersSeen                                  map[string]int // configurations per class
	examples                                               map[string][]seqGateExample
}

func newSeqGateSummary() *seqGateSummary {
	return &seqGateSummary{ends: map[string]int{}, answers: map[string]int{}, readings: map[string]int{}, endsSeen: map[string]int{}, answersSeen: map[string]int{}, examples: map[string][]seqGateExample{}}
}

func (s *seqGateSummary) add(w *seqWorld, ta *seqGateTally, moments int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs++
	s.moments += moments
	s.probes += ta.probes
	s.states += ta.states
	s.gateCalls += w.gateCalls
	s.runPasses += w.runPasses
	for class, n := range ta.ends {
		s.ends[class] += n
		s.endsSeen[class]++
	}
	for class, n := range ta.answers {
		s.answers[class] += n
		s.answersSeen[class]++
	}
	for class, n := range ta.readings {
		s.readings[class] += n
	}
	for class, ex := range ta.examples {
		if len(s.examples[class]) < 4 {
			s.examples[class] = append(s.examples[class], ex)
		}
	}
}

// log prints the counts and, per class, its reading and its first
// pictures.
func (s *seqGateSummary) log(t *testing.T, started time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Logf("%d configurations, %d moments, %d probes, %d states, %d gate questions, %d run passes, %s",
		s.configs, s.moments, s.probes, s.states, s.gateCalls, s.runPasses, time.Since(started).Round(time.Second))
	s.logClasses(t, "end", s.ends, s.endsSeen, func(class string) string { return seqGateEnds[class].words })
	s.logClasses(t, "answer", s.answers, s.answersSeen, func(class string) string { return seqGateAnswers[class].words })
	s.logClasses(t, "reading", s.readings, nil, func(class string) string { return seqKnownClasses[class] })
}

func (s *seqGateSummary) logClasses(t *testing.T, kind string, counts, seen map[string]int, words func(string) string) {
	classes := make([]string, 0, len(counts))
	for class := range counts {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	for _, class := range classes {
		line := fmt.Sprintf("%s %s: %d", kind, class, counts[class])
		if seen != nil {
			line += fmt.Sprintf(" across %d configurations", seen[class])
		}
		if w := words(class); w != "" {
			line += ": " + w
		}
		t.Log(line)
		for _, ex := range s.examples[class] {
			t.Logf("  [%s] at %s, %s\n    %s\n    path: %s\n    state: %s", ex.cfg, ex.moment, ex.probe, ex.message, seqPath(ex.state), ex.state)
		}
	}
}

// seqGateRunConfig drives one chain through every moment and probe, and
// through the walk in the full mode, and fails on an end or an answer
// outside its table, on a reading outside the chain model's known classes,
// and on a play-out that did not end.
func seqGateRunConfig(t *testing.T, cfg seqConfig, full bool, summary *seqGateSummary) {
	w := newSeqWorld(t, cfg)
	ta := newSeqGateTally()
	w.answers = func(st *seqState, member int, allowed bool, gate v1beta1.RolloutHoldGate, reason string) {
		class := w.seqGateAnswerClass(st, member, allowed, gate, reason)
		ta.answers[class]++
		ta.example(class, seqGateExample{cfg: cfg.name, message: fmt.Sprintf("%s asked and heard %q", w.cfg.members[member].comp, reason), state: st})
	}
	moments := w.seqGateMomentStates()
	for _, ms := range moments {
		for _, probe := range w.seqGateProbes(ms.state, ms.moment) {
			w.seqGateRunProbe(ta, ms, probe)
		}
	}
	if full {
		w.seqGateWalk(ta)
	}
	summary.add(w, ta, len(moments))
	seqGateFailOutsideTheTables(t, ta)
	t.Logf("%d moments, %d probes, %d states, %d gate questions: ends %v", len(moments), ta.probes, ta.states, w.gateCalls, ta.ends)
}

// seqGateFailOutsideTheTables fails a configuration on an end or an answer
// outside its table and on a reading outside the chain model's known
// classes, each with its first picture.
func seqGateFailOutsideTheTables(t *testing.T, ta *seqGateTally) {
	t.Helper()
	for class := range ta.ends {
		if _, ok := seqGateEnds[class]; !ok {
			ex := ta.examples[class]
			t.Errorf("end %s is outside the table: at %s, %s: %s\n    path: %s\n    state: %s", class, ex.moment, ex.probe, ex.message, seqPath(ex.state), ex.state)
		}
	}
	for class := range ta.answers {
		if _, ok := seqGateAnswers[class]; !ok {
			ex := ta.examples[class]
			t.Errorf("answer %s is outside the table: %s\n    path: %s\n    state: %s", class, ex.message, seqPath(ex.state), ex.state)
		}
	}
	for class := range ta.readings {
		if _, ok := seqKnownClasses[class]; !ok {
			ex := ta.examples[class]
			t.Errorf("reading %s is outside the chain model's known classes: at %s, %s: %s\n    path: %s\n    state: %s", class, ex.moment, ex.probe, ex.message, seqPath(ex.state), ex.state)
		}
	}
}

// seqGateCheckTables fails on a listed end or answer the mode reaches no
// more, so a change of the engine that removes one is seen and the table
// rewritten with it; each mode checks its own column.
func seqGateCheckTables(t *testing.T, summary *seqGateSummary, full bool) {
	summary.mu.Lock()
	defer summary.mu.Unlock()
	reached := func(c seqGateClass) bool {
		if full {
			return c.full
		}
		return c.bounded
	}
	for class, end := range seqGateEnds {
		if reached(end) && summary.ends[class] == 0 {
			t.Errorf("end %s is listed as reached and no path reached it; if a change of the engine removed it, rewrite the table", class)
		}
	}
	for class, answer := range seqGateAnswers {
		if reached(answer) && summary.answers[class] == 0 {
			t.Errorf("answer %s is listed as heard and no start heard it; if a change of the engine removed it, rewrite the table", class)
		}
	}
}

// TestSequentialGate_ReferenceChainsUnderEveryMoveAtEveryMoment drives the
// reference chains through one plain roll each, lands every operator move
// and fault at every moment of it, plays the roll out, and holds the ends
// and the gate's answers to their tables.
func TestSequentialGate_ReferenceChainsUnderEveryMoveAtEveryMoment(t *testing.T) {
	full := os.Getenv("OME_SEQUENTIAL_HUNT") == "full"
	only := os.Getenv("OME_SEQUENTIAL_HUNT_ONLY")
	workers := 4
	if full {
		workers = 16
	}
	admission := make(chan struct{}, workers)
	summary := newSeqGateSummary()
	started := time.Now()
	t.Cleanup(func() {
		summary.log(t, started)
		if only == "" {
			seqGateCheckTables(t, summary, full)
		}
	})
	for _, cfg := range seqGateConfigs(full) {
		if only != "" && !strings.Contains(cfg.name, only) {
			continue
		}
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()
			admission <- struct{}{}
			defer func() { <-admission }()
			seqGateRunConfig(t, cfg, full, summary)
		})
	}
}

// seqGateChain builds the chain the locks below share: 12 engines then 8
// decoders, RecreatePod at 10 percent, with the given partition on the
// engine and wait after it.
func seqGateChain(t *testing.T, partition int32, wait time.Duration) *seqWorld {
	t.Helper()
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	return newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, v1beta1.UpdateStrategyRecreatePod, "10%", []int32{partition, 0}, wait, 3, seqGateBudgets()))
}

// seqGateRest plays a world's plain roll out to its rest.
func seqGateRest(w *seqWorld) *seqState {
	st := w.initial()
	w.settle(st, w.order(0))
	st, _ = w.seqGatePlayOut(w.projectAll(st), func(*seqState) {})
	return st
}

// seqGateVersions counts a member's Instances on version 1 and on version 2.
func seqGateVersions(m *seqMember) (one, two int) {
	for _, r := range m.rows {
		switch r.rev {
		case 1:
			one++
		case 2:
			two++
		}
	}
	return one, two
}

// seqGateOnOrToward counts a member's Instances on rev or with an attempt
// open toward it.
func seqGateOnOrToward(m *seqMember, rev int8) int {
	n := 0
	for _, r := range m.rows {
		if r.rev == rev || (r.open() && r.to == rev) {
			n++
		}
	}
	return n
}

// TestSequentialGate_APushBackUnderAPartitionKeepsInstancesOnTheRejectedVersion
// records what a partition holds after a push back. 12 engines of which
// the partition keeps 2 on version 1, then 8 decoders, both RecreatePod at
// 10 percent. Version 2 rolls: engines 2 to 11 move, engines 0 and 1 stay
// on version 1, the decoders roll, and the run stays open behind the
// engine resting at its partition. The operator pushes version 1 back. The
// partition holds the lowest-indexed Instances off the target, which are
// now engines 2 and 3 on version 2: engines 4 to 11 return to version 1,
// engines 2 and 3 stay on the version the operator pushed back from, the
// decoders return to version 1, and the chain rests with the run open. The
// same push back landed while the engine's first batch serves ends the
// same way. The operator who asked for 2 Instances kept on version 1 has 2
// kept on version 2.
func TestSequentialGate_APushBackUnderAPartitionKeepsInstancesOnTheRejectedVersion(t *testing.T) {
	for _, tc := range []struct {
		name string
		at   func(w *seqWorld) *seqState
	}{
		{name: "at rest", at: seqGateRest},
		{name: "while the first batch serves", at: func(w *seqWorld) *seqState {
			st := w.initial()
			w.settle(st, w.order(0))
			st, _ = w.seqGateRotating(w.projectAll(st), 0)
			return st
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := seqGateChain(t, 2, 0)
			st := tc.at(w)
			st = w.apply(st, seqMove{kind: "rollback"})
			st, ended := w.seqGatePlayOut(st, func(*seqState) {})
			if !ended || !w.membersComplete(st) {
				t.Fatalf("the chain must rest after the push back, got %s", st)
			}
			one, two := seqGateVersions(&st.members[0])
			if one != 10 || two != 2 || st.members[0].rows[2].rev != 2 || st.members[0].rows[3].rev != 2 {
				t.Fatalf("the engine must rest with engines 2 and 3 on version 2 and the rest on version 1 after the push back; a change here is the partition holding other Instances, got %s", st.members[0])
			}
			if one, two := seqGateVersions(&st.members[1]); one != 8 || two != 0 {
				t.Fatalf("every decoder must return to version 1, got %s", st.members[1])
			}
			if class := w.seqGateEndClass(st, ended); class != "rests-at-a-partition-on-the-rejected-version-after-a-push-back" {
				t.Fatalf("the end must read as the rest on the rejected version, got %s (%s)", class, st)
			}
			if st.activeRun() == nil {
				t.Fatalf("the run closed behind the engine resting at its partition; the known gap is fixed, so this test is updated")
			}
		})
	}
}

// seqGateScaleUpUnderTheWait is one reading of a scale-up of the finished
// member while the next member waits: the strategy of both members and
// whether one Instance short takes the member's Ready condition to False.
type seqGateScaleUpUnderTheWait struct {
	name     string
	strategy v1beta1.UpdateStrategyType
	notReady bool
}

// seqGateHoldIs reports a member's hold naming the gate and carrying the
// text.
func seqGateHoldIs(h *seqHold, gate v1beta1.RolloutHoldGate, text string) bool {
	return h != nil && h.gate == gate && strings.Contains(h.reason, text)
}

// seqGateScaledUpUnderTheWait drives the chain to the wait after the first
// member, scales that member to 13 and checks the picture the reading
// expects: the new Instance building on version 2, the Ready condition as
// the strategy reads it, and the next member held on the readiness or on
// the clock.
func (tc seqGateScaleUpUnderTheWait) scaledUpUnderTheWait(t *testing.T, w *seqWorld) *seqState {
	t.Helper()
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.rollTo(w.projectAll(st), 0)
	if !seqGateHoldIs(st.holds[1], v1beta1.RolloutHoldGateSequential, "waiting out") {
		t.Fatalf("the picture is wrong: the decoder must wait out the ten minutes, got hold %s (%s)", st.holds[1], st)
	}
	st = w.apply(st, seqMove{kind: "scale", member: 0, value: 13})
	st = w.projectAll(st)
	engine := st.members[0]
	if len(engine.rows) != 13 || engine.rows[12].kind != seqCreating || engine.rows[12].to != 2 {
		t.Fatalf("the picture is wrong: the thirteenth engine must be building toward version 2, got %s", engine)
	}
	if (engine.ready == metav1.ConditionFalse) != tc.notReady {
		t.Fatalf("the engine's Ready condition reads %s one Instance short; want not Ready = %t (%s)", engine.ready, tc.notReady, engine)
	}
	want := "waiting out"
	if tc.notReady {
		want = "held while engine is not Ready"
	}
	if !seqGateHoldIs(st.holds[1], v1beta1.RolloutHoldGateSequential, want) {
		t.Fatalf("the decoder must hold with %q, got hold %s", want, st.holds[1])
	}
	return st
}

// TestSequentialGate_AScaleUpOfTheFinishedMemberUnderTheWaitHoldsTheNextMemberUntilItServes
// records the readiness rule under the wait. 12 engines then 8 decoders,
// SurgeThenDrain at 10 percent, a ten minute wait between them. The
// engines finish and the decoders wait out the ten minutes. The operator
// scales the engine to 13: the new Instance builds on version 2, and a
// surging member declares no unavailability budget, so the engine's Ready
// condition reads False while it serves 12 of 13 and the decoder's start
// is held on the engine's readiness, not on the clock. Once the Instance
// serves the engine is Ready again, the wait's clock stands, and the
// decoders start as soon as the ten minutes have passed; the chain ends
// with the run closed. Under RecreatePod at 10 percent the same scale-up
// leaves the engine Ready, one Instance short being inside its own
// unavailability budget, and the decoders keep waiting out the clock.
func TestSequentialGate_AScaleUpOfTheFinishedMemberUnderTheWaitHoldsTheNextMemberUntilItServes(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	for _, tc := range []seqGateScaleUpUnderTheWait{
		{name: "a surging engine reads not Ready one Instance short", strategy: v1beta1.UpdateStrategySurgeThenDrain, notReady: true},
		{name: "a drain-first engine at 10 percent stays Ready one Instance short", strategy: v1beta1.UpdateStrategyRecreatePod},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newSeqWorld(t, seqChain([]int32{12, 8}, []v1beta1.ComponentType{eng, dec}, tc.strategy, "10%", nil, 10*time.Minute, 3, seqGateBudgets()))
			w.settleOrder = []int{0, 1}
			st := tc.scaledUpUnderTheWait(t, w)
			st = w.apply(st, seqMove{kind: "serve", member: 0, idx: 12})
			if !seqGateHoldIs(st.holds[1], v1beta1.RolloutHoldGateSequential, "waiting out") {
				t.Fatalf("once the engine serves whole the decoder must wait out the ten minutes from the recorded finish, got hold %s", st.holds[1])
			}
			st = w.apply(st, seqMove{kind: "wait", member: 0})
			if !st.members[1].inFlight() {
				t.Fatalf("the decoders must start once the wait passed, got %s", st)
			}
			st, ended := w.seqGatePlayOut(st, func(*seqState) {})
			if class := w.seqGateEndClass(st, ended); class != "ended-run-closed" {
				t.Fatalf("the chain must end with the run closed, got %s (%s)", class, st)
			}
		})
	}
}

// TestSequentialGate_APartitionSetAfterTheChainEndedChangesNothing records
// a partition over a converged member. 12 engines then 8 decoders roll
// version 2 to the end and the run closes. The operator sets a partition
// of 6 on the engine: no Instance runs another version, so the partition
// holds none, no member holds, no run opens, and every Instance stays on
// version 2. The next push rolls 6 engines and keeps 6 on version 2, as
// the partition asks.
func TestSequentialGate_APartitionSetAfterTheChainEndedChangesNothing(t *testing.T) {
	w := seqGateChain(t, 0, 0)
	st := seqGateRest(w)
	if class := w.seqGateEndClass(st, true); class != "ended-run-closed" {
		t.Fatalf("the picture is wrong: the plain roll must end with the run closed, got %s (%s)", class, st)
	}
	st = w.apply(st, seqMove{kind: "partition", member: 0, value: 6})
	st, ended := w.seqGatePlayOut(st, func(*seqState) {})
	if class := w.seqGateEndClass(st, ended); class != "ended-run-closed" || st.holds[0] != nil || st.holds[1] != nil {
		t.Fatalf("a partition over a converged member must open nothing and hold nothing, got %s (%s)", class, st)
	}
	if one, two := seqGateVersions(&st.members[0]); one != 0 || two != 12 {
		t.Fatalf("every engine must stay on version 2, got %s", st.members[0])
	}
	st = w.apply(st, seqMove{kind: "push"})
	st, ended = w.seqGatePlayOut(st, func(*seqState) {})
	var three int
	for _, r := range st.members[0].rows {
		if r.rev == 3 {
			three++
		}
	}
	if _, two := seqGateVersions(&st.members[0]); three != 6 || two != 6 {
		t.Fatalf("the next push must move 6 engines and keep 6 on version 2, got %s", st.members[0])
	}
	if class := w.seqGateEndClass(st, ended); class != "known-run-stays-open-while-a-member-rests-at-its-partition" {
		t.Fatalf("the chain must rest at the partition with the run open, the known gap, got %s (%s)", class, st)
	}
}

// TestSequentialGate_AChainThatStartsWithTheRouterWaitsForTheRoutersSpec
// records the member order against the projector's: 2 routers, then 12
// engines, then 8 decoders. The projector writes the engine's and the
// decoder's spec before the router's. Between those writes no run opens,
// since the router's replica still carries the service generation before
// the push, so the plan gate holds the engine's and the decoder's starts
// and nothing starts out of order; once the router's spec lands the run
// opens, the router rolls first and alone, the engine waits on it under
// the Sequential gate, the decoder follows the engine, and the chain ends
// with the run closed.
func TestSequentialGate_AChainThatStartsWithTheRouterWaitsForTheRoutersSpec(t *testing.T) {
	w := newSeqWorld(t, seqGateConfig(seqGateShapes[0], seqGateOrders()[3], seqGateStrategyPairs()[0], 0, nil, 3))
	st := w.initial()
	w.settle(st, w.order(0))
	// The projector reaches the engine and the decoder; their replicas
	// reconcile before the router's spec lands.
	next := st.clone()
	w.project(next, 1)
	w.project(next, 2)
	w.settle(next, []int{1, 2})
	if next.members[1].inFlight() || next.members[2].inFlight() || next.activeRun() != nil {
		t.Fatalf("no member may start and no run may open before the router's spec lands, got %s", next)
	}
	if h := next.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGatePlan {
		t.Fatalf("the engine must be held by the plan gate while the router's spec has not landed, got hold %s (%s)", h, next)
	}
	// The router's spec lands: the run opens and the router rolls first.
	w.project(next, 0)
	w.settle(next, w.order(0))
	if next.activeRun() == nil || !next.members[0].inFlight() || next.members[1].inFlight() || next.members[2].inFlight() {
		t.Fatalf("the router must roll first and alone under an open run, got %s", next)
	}
	if h := next.holds[1]; h == nil || h.gate != v1beta1.RolloutHoldGateSequential || h.reason != "Sequential waiting on router" {
		t.Fatalf("the engine must wait on the router, got hold %s (%s)", h, next)
	}
	st, ended := w.seqGatePlayOut(next, func(s *seqState) {
		if len(s.breaks) != 0 {
			t.Fatalf("no member may start out of order, got %v (%s)", s.breaks, s)
		}
	})
	if class := w.seqGateEndClass(st, ended); class != "ended-run-closed" {
		t.Fatalf("the chain must end with the run closed, got %s (%s)", class, st)
	}
}

// seqGateRollUntil plays the engine's roll out, one progress move a pass,
// until n of its Instances are Ready on rev.
func (w *seqWorld) seqGateRollUntil(st *seqState, rev int8, n int) *seqState {
	for guard := 0; guard < seqGatePlayOutBound; guard++ {
		ready := 0
		for _, r := range st.members[0].rows {
			if r.kind == seqReady && r.rev == rev {
				ready++
			}
		}
		if ready >= n {
			return st
		}
		moves := w.progressMoves(st)
		if len(moves) == 0 {
			return st
		}
		st = w.apply(st, moves[0])
	}
	return st
}

// seqGateLoseAnIdleRow loses the pods of the engine's highest-indexed Instance
// that is Ready on rev with no attempt open, and returns its index.
func (w *seqWorld) seqGateLoseAnIdleRow(t *testing.T, st *seqState, rev int8) (*seqState, int) {
	t.Helper()
	for idx := len(st.members[0].rows) - 1; idx >= 0; idx-- {
		if r := st.members[0].rows[idx]; r.kind == seqReady && r.rev == rev {
			return w.apply(st, seqMove{kind: "lose", member: 0, idx: idx}), idx
		}
	}
	t.Fatalf("no engine Instance is Ready on version %d with no attempt open: %s", rev, st.members[0])
	return st, -1
}

// seqGatePlayOutKeeping plays the engine out by its first progress move a
// pass, never serving the engine's Instance idx, until nothing else is left,
// and returns the classes of every break the passes recorded on the way.
func (w *seqWorld) seqGatePlayOutKeeping(st *seqState, idx int) (*seqState, map[string]bool) {
	seen := map[string]bool{}
	for guard := 0; guard < seqGatePlayOutBound; guard++ {
		var next *seqMove
		for _, mv := range w.progressMoves(st) {
			if mv.kind == "serve" && mv.member == 0 && mv.idx == idx {
				continue
			}
			mv := mv
			next = &mv
			break
		}
		if next == nil {
			return st, seen
		}
		st = w.apply(st, *next)
		for _, b := range st.breaks {
			seen[b.class] = true
		}
	}
	return st, seen
}

// seqGateEngineRestsExcept reports the engine with every Instance Ready on
// rev except idx, which is still rebuilt toward rev.
func seqGateEngineRestsExcept(m *seqMember, rev int8, idx int) bool {
	for k, r := range m.rows {
		if k == idx {
			if r.kind != seqRebuild || r.to != rev {
				return false
			}
			continue
		}
		if r.kind != seqReady || r.rev != rev {
			return false
		}
	}
	return true
}

// TestSequentialGate_ARebuiltInstanceHoldsTheNextMemberOnAFirstPushAndUnderARestoredRevision
// is 12 engines then 8 decoders, both RecreatePod at 10 percent. On a first
// push, an engine Instance that loses its pods before its turn is rebuilt
// toward version 2 and the decoder waits for it to serve: the engine's
// current revision moves only once every Instance is Ready. When version 2
// had landed, was pushed back and is pushed again before the roll back
// finished, the same rebuild holds the decoder the same way: the engine's
// current revision, cleared by the push back, comes back only once no
// attempt is open on any Instance, the rebuild of that Instance included,
// so the engine reads in flight until the rebuilt Instance serves.
func TestSequentialGate_ARebuiltInstanceHoldsTheNextMemberOnAFirstPushAndUnderARestoredRevision(t *testing.T) {
	t.Run("first push: the decoder waits for the rebuilt Instance", func(t *testing.T) {
		w := seqGateChain(t, 0, 0)
		st := w.initial()
		w.settle(st, w.order(0))
		st = w.seqGateRollUntil(w.projectAll(st), 2, 4)
		st, idx := w.seqGateLoseAnIdleRow(t, st, 1)
		st, seen := w.seqGatePlayOutKeeping(st, idx)
		eng, dec := &st.members[0], &st.members[1]
		if len(seen) != 0 {
			t.Fatalf("no start may slip past the rebuilt Instance on a first push, got %v", seen)
		}
		if !seqGateEngineRestsExcept(eng, 2, idx) || eng.current != 1 {
			t.Fatalf("the engine must stand with every Instance on version 2 but Instance %d still rebuilt toward it, its current revision on version 1, got %s", idx, eng)
		}
		if one, _ := seqGateVersions(dec); one != 8 || !seqGateHoldIs(st.holds[1], v1beta1.RolloutHoldGateSequential, "Sequential waiting on engine") {
			t.Fatalf("the decoder must wait on the engine with every Instance on version 1, got %s hold=%v", dec, st.holds[1])
		}
		st = w.apply(st, seqMove{kind: "serve", member: 0, idx: idx})
		if st, ended := w.seqGatePlayOut(st, func(*seqState) {}); !ended || w.seqGateEndClass(st, ended) != "ended-run-closed" {
			t.Fatalf("once the rebuilt Instance serves the chain must end with its run closed, got %s", st)
		}
	})
	t.Run("push back pushed again: the decoder waits for the rebuilt Instance too", func(t *testing.T) {
		w := seqGateChain(t, 0, 0)
		st := w.initial()
		w.settle(st, w.order(0))
		st = w.rollTo(w.projectAll(st), 0)
		st = w.apply(st, seqMove{kind: "rollback"})
		st = w.seqGateRollUntil(st, 1, 4)
		st = w.apply(st, seqMove{kind: "push"})
		for guard := 0; st.members[0].target != 2 && guard < seqGatePlayOutBound; guard++ {
			st = w.apply(st, w.progressMoves(st)[0])
		}
		// The decoder may already have an Instance on or toward version 2
		// from before the push back; it may start no other.
		st, idx := w.seqGateLoseAnIdleRow(t, st, 1)
		decoderOnTwo := seqGateOnOrToward(&st.members[1], 2)
		st, seen := w.seqGatePlayOutKeeping(st, idx)
		eng, dec := &st.members[0], &st.members[1]
		if len(seen) != 0 {
			t.Fatalf("no start may slip past the rebuilt Instance under a cleared current revision, got %v", seen)
		}
		if !seqGateEngineRestsExcept(eng, 2, idx) || eng.current != 0 || eng.statusTarget != 2 {
			t.Fatalf("the engine must stand with every Instance on version 2 but Instance %d still rebuilt toward it, its current revision still cleared, got %s", idx, eng)
		}
		if seqGateOnOrToward(dec, 2) != decoderOnTwo || dec.inFlight() || !seqGateHoldIs(st.holds[1], v1beta1.RolloutHoldGateSequential, "Sequential waiting on engine") {
			t.Fatalf("the decoder must wait on the engine with the %d Instance(s) it already had on version 2 and no attempt open, got %s hold=%v", decoderOnTwo, dec, st.holds[1])
		}
		st = w.apply(st, seqMove{kind: "serve", member: 0, idx: idx})
		if st, ended := w.seqGatePlayOut(st, func(*seqState) {}); !ended || w.seqGateEndClass(st, ended) != "ended-run-closed" {
			t.Fatalf("once the rebuilt Instance serves the chain must end with its run closed, got %s", st)
		}
	})
}

// TestSequentialGate_ReplayAPath replays the path OME_SEQUENTIAL_HUNT_PATH
// names (as the model prints it) on the configuration of this model whose
// name is OME_SEQUENTIAL_HUNT_ONLY, and prints every surface before and
// after the last move, the way the chain model's replay does. Skipped
// without both variables.
func TestSequentialGate_ReplayAPath(t *testing.T) {
	name, path := os.Getenv("OME_SEQUENTIAL_HUNT_ONLY"), os.Getenv("OME_SEQUENTIAL_HUNT_PATH")
	if name == "" || path == "" {
		t.Skip("set OME_SEQUENTIAL_HUNT_ONLY to a configuration name and OME_SEQUENTIAL_HUNT_PATH to a path")
	}
	var cfg *seqConfig
	for _, c := range seqGateConfigs(true) {
		if c.name == name {
			c := c
			cfg = &c
			break
		}
	}
	if cfg == nil {
		t.Fatalf("no configuration named %q", name)
	}
	w := newSeqWorld(t, *cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	moves := seqParsePath(path)
	for n, mv := range moves {
		if n == len(moves)-1 {
			t.Logf("before the last move %s:\n%s", mv, w.surfaces(st))
		}
		st = w.apply(st, mv)
	}
	t.Logf("after the last move:\n%s", w.surfaces(st))
	for _, f := range w.check(st) {
		t.Logf("finding %s: %s", f.class, f.message)
	}
	t.Logf("end: %s", w.seqGateEndClass(st, true))
}
