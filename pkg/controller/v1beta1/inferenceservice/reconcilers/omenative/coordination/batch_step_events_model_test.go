package coordination

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/scaling"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file widens the batch-step model of
// pd_pacing_gates_never_wait_on_each_other_test.go and
// batch_step_model_test.go, whose world and runner it shares, to the
// events a cluster and an operator land on a batch while it is open: a
// partition edit; a scale-up or a scale-down of either side, by the
// operator alone or through a proportional scaling policy whose follower
// the coordinator sizes one service pass after the anchor; a gang of two
// or four pods that loses a member, has a member the scheduler cannot
// place, or has a member on a node that stops reporting; and two of these
// in one pass, before the admission that follows. Both pacing strategies
// step in batches of one to three Instances.
//
// Beside the world's own readings it reads, at every settled state: every
// hold against the hold reasons the engine emits (the group's
// ratio.batchWait, ratio.lost, pairing.drainHeld, budget.surgeExhausted,
// budget.unavailableExhausted and member.held; the replica's gate.deny
// for the holds it raises on its own), the batches'
// fill against the number of starts the batch step admits from the peers'
// settled counts, and the two sides' waits on each other. A hold no table
// names, a batch filled past or short of what the step admits with no
// known cause, two sides each waiting on the other, and the world's own
// failing classes fail the test; every other reading is a known class
// that names what the operator sees and the test that
// holds it.
//
// OME_BATCH_STEP_EVENTS_HUNT=full widens the configurations and the
// bounds for a long local run; OME_BATCH_STEP_EVENTS_HUNT_ONLY=<text>[,<text>]
// keeps the configurations whose name contains one of the texts.

// eventText names the event knobs in a configuration's name.
func (c huntConfig) eventText() string {
	var s string
	if c.follower != "" {
		s += " follower=" + string(c.follower)
	}
	if c.refusals > 0 {
		s += fmt.Sprintf(" refusals=%d", c.refusals)
	}
	if c.lostNodes > 0 {
		s += fmt.Sprintf(" lostNodes=%d", c.lostNodes)
		if c.forceDelete {
			s += "/forceDelete"
		}
	}
	if c.noPlacementGrace {
		s += " noPlacementGrace"
	}
	if c.pairs {
		s += " pairs"
	}
	return s
}

// promotedRow reports a row running the target with no attempt elsewhere,
// what the batch step counts as promoted: a promoted Instance whole, one
// whose rebuilt pods are not yet counted, a promoted gang short of a
// worker, whether the worker's pod is gone or on a lost node, or a recreate
// a rollback turned around, which keeps the revision it returns to as its
// running revision while its pods are rebuilt.
func (r huntRow) promotedRow() bool {
	switch r {
	case huntPromoted, huntPromotedUncounted, huntPromotedShort, huntPromotedUnreported, huntPromotedLostNode, huntDrainBack:
		return true
	}
	return false
}

// lostNodeStands reports a side with a pod on a node that stopped
// reporting, whatever stands behind it.
func (s huntSide) lostNodeStands() bool {
	for _, r := range s.rows {
		if r.lostNode() {
			return true
		}
	}
	return false
}

// eventRow writes the publication of one of the event rows: a gang short
// of a worker publishes Restarting with no operation while its set is
// short (workload/observation.go:TakeInlineV1Publication; the engine
// reads such a row as Ready), an attempt the scheduler refuses or a lost
// node blocks publishes as the open attempt it is, and a repair waiting
// on a lost node's pod publishes its Restart with nothing serving.
func eventRow(row *v1beta1.OMENativeInstanceStatus, kind huntRow, m huntMember, old, target string, attempt func(step string) *v1beta1.InstanceOperation) {
	g := m.pods()
	switch kind {
	case huntOldLostMember, huntOldUnreported, huntOldLostNode:
		row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceRestarting, old, g, g-1, g-1
	case huntPromotedUnreported:
		row.Phase, row.RunningRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount = v1beta1.OMENativeInstanceRestarting, target, g, g-1, g-1
	case huntPromotedLostNode:
		row.Phase, row.RunningRevision, row.PodCount = v1beta1.OMENativeInstanceRestarting, target, g
		row.Operation = &v1beta1.InstanceOperation{ID: "refill", Type: v1beta1.InstanceOperationRestart}
	case huntDrainBlocked, huntDrainRefused:
		row.Phase, row.RunningRevision, row.TargetRevision, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, attempt(workloadtypes.UpdateStepDrain)
	case huntSurgeLostNode:
		row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, g, g-1, g-1, attempt(workloadtypes.UpdateStepSurge)
	case huntSourceDrainBlocked:
		row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, g, attempt(workloadtypes.UpdateStepSurgeDrain)
	case huntSurgeRefused:
		row.Phase, row.RunningRevision, row.TargetRevision, row.PodCount, row.ReadyPodCount, row.ServingPodCount, row.Operation = v1beta1.OMENativeInstanceUpdating, old, target, 2, 1, 1, attempt(workloadtypes.UpdateStepSurge)
		if m.isGang() {
			row.PodCount, row.ReadyPodCount, row.ServingPodCount = g, g, g
		}
	case huntCreatingRefused:
		row.Phase, row.TargetRevision, row.PodCount = v1beta1.OMENativeInstanceCreating, target, g
		row.Operation = &v1beta1.InstanceOperation{ID: "create", Type: v1beta1.InstanceOperationCreate, TargetRevision: target}
	}
}

// eventGangPods lists the pods of a gang in one of the event rows as the
// drain gate lists them during a protocol change: a lost or unreported
// worker is left out (its Ready is gone or revoked), a replacement stands
// not Ready while it builds and Ready once it serves, and a set whose pods
// are leaving or waiting on the scheduler routes nothing.
func (w *huntWorld) eventGangPods(i, idx int, kind huntRow, s huntSide, oldProto, newProto string) []client.Object {
	short := int(w.cfg.members[i].pods() - 1)
	slot := int(w.slotOf(s, idx))
	var out []client.Object
	switch kind {
	case huntOldLostMember, huntOldUnreported, huntOldLostNode:
		out = append(out, w.gangPods(i, idx, oldProto, true)[:short]...)
	case huntPromotedUnreported:
		out = append(out, w.gangPods(i, idx, newProto, true)[:short]...)
	case huntSurgeLostNode:
		out = append(out, w.gangPods(i, idx, oldProto, true)[:short]...)
		out = append(out, w.gangPods(i, slot, newProto, false)...)
	case huntSourceDrainBlocked:
		out = append(out, w.gangPods(i, slot, newProto, true)...)
	case huntSurgeRefused:
		out = append(out, w.gangPods(i, idx, oldProto, true)...)
		out = append(out, w.gangPods(i, slot, newProto, false)...)
	case huntCreatingRefused:
		out = append(out, w.gangPods(i, idx, newProto, false)...)
	}
	return out
}

// startedSurgeRow is the row a surge start writes on an old row: a
// replacement building beside a source short of a worker, or beside one
// with a pod on a lost node.
func startedSurgeRow(kind huntRow) huntRow {
	switch kind {
	case huntOldLostMember:
		return huntSurgeShort
	case huntOldUnreported, huntOldLostNode:
		return huntSurgeLostNode
	}
	return huntOpenSurge
}

// startedDrainRow is the row a recreate writes on an old row: one whose
// pods wait on a lost node's pod stands blocked.
func startedDrainRow(kind huntRow) huntRow {
	if kind == huntOldUnreported || kind == huntOldLostNode {
		return huntDrainBlocked
	}
	return huntOpenDrain
}

// rollTakes reports whether the roll takes a gang that lost a worker, the
// repair yielding it (ops/restart.go:RepairYieldsToTarget): the target
// open and the retry schedule admitting a fresh start (not Held, not in
// a backoff still running, no retry under way), and the Instance not held
// by the partition. Otherwise the repair refills it on its revision.
func (w *huntWorld) rollTakes(side huntSide, idx int) bool {
	switch side.schedule {
	case scheduleHeld, scheduleBackoff:
		return false
	case scheduleRetrying:
		if side.inFlight() {
			return false
		}
	}
	return !side.held()[idx]
}

// followerOwed reports the follower side whose count the coordinator has
// yet to write under the policy in force, and by how much: the engine's
// arithmetic (scaling.Target) over the anchor's count, with a follower
// minimum of one and no maximum.
func (w *huntWorld) followerOwed(st *huntState) (side int, delta int32, owed bool) {
	follower, anchor := w.policySides(st)
	if follower < 0 {
		return -1, 0, false
	}
	count, err := scaling.Target(int32(len(st.sides[anchor].rows)), st.policy.milli, scaling.Bounds{Min: 1})
	if err != nil {
		w.t.Fatalf("follower count: %v", err)
	}
	delta = count.Target - int32(len(st.sides[follower].rows))
	return follower, delta, delta != 0
}

// eventProgress lists the progress moves of the event rows on side i: the
// scheduler places a refused member; the placement grace ends a refused
// attempt (lifecycle.unschedulableGracePeriod, left unset under
// noPlacementGrace); the taint manager evicts a pod whose node stopped
// reporting; the force-delete policy removes the evicted pod; the
// coordinator writes the follower's count.
func (w *huntWorld) eventProgress(st *huntState, i int, rowsOf func(huntRow) []int) []huntMove {
	var out []huntMove
	for _, kind := range []huntRow{huntDrainRefused, huntSurgeRefused, huntCreatingRefused} {
		for _, idx := range rowsOf(kind) {
			out = append(out, huntMove{kind: "place", side: i, idx: idx})
			if !w.cfg.noPlacementGrace && kind != huntCreatingRefused {
				out = append(out, huntMove{kind: "expire", side: i, idx: idx})
			}
		}
	}
	for _, kind := range []huntRow{huntOldUnreported, huntPromotedUnreported} {
		for _, idx := range rowsOf(kind) {
			out = append(out, huntMove{kind: "evict", side: i, idx: idx})
		}
	}
	if w.cfg.forceDelete {
		for _, kind := range []huntRow{huntOldLostNode, huntDrainBlocked, huntSourceDrainBlocked, huntPromotedLostNode} {
			for _, idx := range rowsOf(kind) {
				out = append(out, huntMove{kind: "force-delete", side: i, idx: idx})
			}
		}
	}
	if side, delta, owed := w.followerOwed(st); owed && side == i {
		out = append(out, huntMove{kind: "follower-pass", side: i, delta: delta})
	}
	return out
}

// eventMoves lists the cluster's events on side i: the scheduler refuses
// a member of an open attempt or of a created Instance; a worker's node
// stops reporting under an old or a promoted gang.
func (w *huntWorld) eventMoves(i int, s huntSide) []huntMove {
	var out []huntMove
	seen := map[huntRow]bool{}
	for idx, r := range s.rows {
		if seen[r] {
			continue
		}
		seen[r] = true
		switch {
		case s.refusals > 0 && refusedRow(r) != r:
			out = append(out, huntMove{kind: "refuse", side: i, idx: idx})
		case s.lostNodes > 0 && unreportedRow(r) != r:
			out = append(out, huntMove{kind: "lostnode", side: i, idx: idx})
		}
	}
	return out
}

// pairMoves combines the single events of a state two at a time, each
// pair landing in one pass before the admission that follows: at most
// huntPairLimit pairs, spread over the combinations in a fixed order.
func (w *huntWorld) pairMoves(st *huntState, singles []huntMove) []huntMove {
	var pairs []huntMove
	for a := range singles {
		for b := a + 1; b < len(singles); b++ {
			first, second := singles[a], singles[b]
			if !first.event() || !second.event() || !pairable(first, second) {
				continue
			}
			first.second = &second
			pairs = append(pairs, first)
		}
	}
	if len(pairs) <= huntPairLimit {
		return pairs
	}
	stride := (len(pairs) + huntPairLimit - 1) / huntPairLimit
	var out []huntMove
	for k := (len(st.path) * 7) % stride; k < len(pairs); k += stride {
		out = append(out, pairs[k])
	}
	return out
}

// huntPairLimit is how many pairs of events one state offers.
const huntPairLimit = 10

// pairable reports two single events that may land in one pass: not two
// of the same kind on one side (two scales of a side in a pass are one
// scale), and not two on one row.
func pairable(first, second huntMove) bool {
	if first.kind == second.kind && first.side == second.side && (first.kind == "scale" || first.kind == "partition") {
		return false
	}
	return first.side != second.side || first.idx != second.idx || first.kind != second.kind
}

// applies reports whether an event still applies to a state: the row it
// names is one it may land on and the side has the event left.
func (w *huntWorld) applies(st *huntState, mv huntMove) bool {
	if mv.side >= len(st.sides) {
		return false
	}
	s := st.sides[mv.side]
	n := int32(len(s.rows))
	switch mv.kind {
	case "repush":
		return w.repushApplies(st)
	case "scale":
		if mv.both {
			other := st.sides[mv.side2]
			return s.scales > 0 && other.scales > 0 && n+mv.delta >= 1 && int32(len(other.rows))+mv.delta2 >= 1
		}
		return s.scales > 0 && n+mv.delta >= 1
	case "partition":
		return s.parts > 0 && mv.delta <= n && (!mv.both || st.sides[mv.side2].parts > 0)
	}
	if mv.idx >= len(s.rows) || !w.cfg.members[mv.side].isGang() && mv.kind != "refuse" {
		return false
	}
	r := s.rows[mv.idx]
	switch mv.kind {
	case "lose":
		return ((r == huntOld || r == huntPromoted) && s.refills > 0) || ((r == huntOpenSurge || r == huntReplacementHeld) && s.losses > 0)
	case "lostnode":
		return s.lostNodes > 0 && unreportedRow(r) != r
	case "refuse":
		return s.refusals > 0 && refusedRow(r) != r
	}
	return false
}

// applyEvent makes one of the event moves on the state.
func (w *huntWorld) applyEvent(next *huntState, mv huntMove) {
	side := &next.sides[mv.side]
	switch mv.kind {
	case "refuse":
		side.refusals--
		side.rows[mv.idx] = refusedRow(side.rows[mv.idx])
	case "place":
		side.rows[mv.idx] = placedRow(side.rows[mv.idx])
	case "expire":
		// The attempt ends environment-caused: the retry record is left
		// alone (escalation/scheduler.go), so the next pass retries at once.
		side.expired++
		side.rows[mv.idx] = expiredRow(side.rows[mv.idx], w.cfg.members[mv.side].isGang())
	case "lostnode":
		side.lostNodes--
		side.rows[mv.idx] = unreportedRow(side.rows[mv.idx])
	case "evict":
		side.rows[mv.idx] = evictedRow(side.rows[mv.idx])
	case "force-delete":
		side.rows[mv.idx] = freedRow(side.rows[mv.idx])
	case "follower-pass":
		side.followed++
		resizeSide(side, mv.delta)
		for i := range next.sides {
			next.sides[i].canaryPartition = w.canaryPartition(next, i)
		}
	}
}

// refusedRow is the row the scheduler's refusal of a member writes; the
// row itself when it carries nothing the scheduler places.
func refusedRow(r huntRow) huntRow {
	switch r {
	case huntOpenDrain:
		return huntDrainRefused
	case huntOpenSurge:
		return huntSurgeRefused
	case huntCreating:
		return huntCreatingRefused
	}
	return r
}

// placedRow is the row a refused member's placement writes.
func placedRow(r huntRow) huntRow {
	switch r {
	case huntDrainRefused:
		return huntOpenDrain
	case huntSurgeRefused:
		return huntOpenSurge
	case huntCreatingRefused:
		return huntCreating
	}
	return r
}

// expiredRow is the row the placement grace leaves: a recreate ends with
// nothing serving; a surge ends over its serving source, a gang keeping
// the attempt until its replacement is retired.
func expiredRow(r huntRow, gang bool) huntRow {
	switch {
	case r == huntDrainRefused:
		return huntFailedUnserving
	case r == huntSurgeRefused && gang:
		return huntFailedGangKept
	case r == huntSurgeRefused:
		return huntFailedSurge
	}
	return r
}

// unreportedRow is the row a worker's node stopping reporting writes.
func unreportedRow(r huntRow) huntRow {
	switch r {
	case huntOld:
		return huntOldUnreported
	case huntPromoted:
		return huntPromotedUnreported
	}
	return r
}

// evictedRow is the row the taint manager's eviction writes: the pod is
// Terminating on its lost node and the gang reads short of a member.
func evictedRow(r huntRow) huntRow {
	switch r {
	case huntOldUnreported:
		return huntOldLostNode
	case huntPromotedUnreported:
		return huntPromotedLostNode
	}
	return r
}

// freedRow is the row the force delete of the lost node's pod writes: the
// roll's or the repair's pods go on.
func freedRow(r huntRow) huntRow {
	switch r {
	case huntOldLostNode:
		return huntOldLostMember
	case huntDrainBlocked:
		return huntOpenDrain
	case huntSourceDrainBlocked:
		return huntPromoted
	case huntPromotedLostNode:
		return huntPromotedShort
	}
	return r
}

// sidesWaitOnEachOther reports a standstill where each pairing side's hold
// is a batch wait on the other.
func (w *huntWorld) sidesWaitOnEachOther(st *huntState) bool {
	eng, dec := w.pairIndex()
	if eng < 0 || dec < 0 {
		return false
	}
	for _, pair := range [][2]int{{eng, dec}, {dec, eng}} {
		h := st.holds[pair[0]]
		if h == nil || h.gate != v1beta1.RolloutHoldGateRatio {
			return false
		}
		if peer, ok := w.batchWaitPeer(h.reason); !ok || peer != pair[1] {
			return false
		}
	}
	return true
}

// huntTableEvent names the lock-table event a hold is: the coordination
// table's for the batch step, the ratio rule, the drain-time pairing gate,
// the group budgets and a Held named side; the instance table's gate.deny
// for the holds the replica raises on its own (the start-time pairing
// gate, its own budgets, its retry record, the plan gate). A hold neither
// table names is a finding.
func huntTableEvent(h *huntHold) (string, bool) {
	reason := h.reason
	switch h.gate {
	case v1beta1.RolloutHoldGateRatio:
		switch {
		case strings.Contains(reason, "to serve first"):
			return "coordination/ratio.batchWait[firstSide]", true
		case strings.Contains(reason, " waits for ") && strings.Contains(reason, " batch "):
			return "coordination/ratio.batchWait[previousBatch]", true
		case strings.Contains(reason, "outpace") || strings.Contains(reason, "skew"):
			return "coordination/ratio.lost", true
		}
	case v1beta1.RolloutHoldGateHeld:
		return "coordination/member.held[set]", true
	case v1beta1.RolloutHoldGatePairing:
		return pairingTableEvent(reason)
	case v1beta1.RolloutHoldGateBudget:
		switch {
		case strings.HasPrefix(reason, "per-Component"):
			return "instance/gate.deny[Budget]", true
		case strings.Contains(reason, "surge budget"):
			return "coordination/budget.surgeExhausted", true
		case strings.Contains(reason, "unavailable budget"):
			return "coordination/budget.unavailableExhausted", true
		}
	case v1beta1.RolloutHoldGateRetryBlock:
		return "instance/gate.deny[RetryBlock]", true
	case v1beta1.RolloutHoldGatePlan:
		return "instance/gate.deny[Plan]", true
	}
	return "", false
}

// pairingTableEvent names a pairing hold: the drain-time gate's three
// reasons are the coordination table's pairing.drainHeld variants, the
// start-time gate's holds are the instance table's gate.deny[Pairing].
func pairingTableEvent(reason string) (string, bool) {
	drain := strings.Contains(reason, "holding the drain")
	switch {
	case drain && strings.Contains(reason, "cannot pair"):
		return "coordination/pairing.drainHeld[noPartner]", true
	case drain && strings.Contains(reason, "below its"):
		return "coordination/pairing.drainHeld[belowMinimum]", true
	case drain && strings.Contains(reason, "no pairable serving engine+decoder pair"):
		return "coordination/pairing.drainHeld[lastPair]", true
	case strings.HasPrefix(reason, "pairing protocol transition") || strings.HasPrefix(reason, "pairing gate"):
		return "instance/gate.deny[Pairing]", true
	}
	return "", false
}

// eventReadings reads one settled state for the event classes: every hold
// against the tables (tableReadings), the batches' fill against what the
// step admits (fillReadings), and a follower count the coordinator has yet
// to write.
func (w *huntWorld) eventReadings(st *huntState, batch batchStep) []huntFinding {
	out := w.tableReadings(st)
	if batch.governs {
		out = append(out, w.fillReadings(st, batch)...)
	}
	if side, delta, owed := w.followerOwed(st); owed {
		out = append(out, huntFinding{class: "follower-count-a-pass-behind", state: st,
			message: fmt.Sprintf("the %s follows the other side's count one service pass later: %d Instances owed while the gates plan from %d", w.cfg.members[side].comp, delta, len(st.sides[side].rows))})
	}
	return out
}

// tableReadings names every hold of the state by its table event; a hold
// no table names is the class wait-no-table-names. A hold on a side with a
// pod on a node that stopped reporting, raised by the group's
// unavailability budget, is the budget counting the outage beside the
// start that would end it, until the eviction.
func (w *huntWorld) tableReadings(st *huntState) []huntFinding {
	var out []huntFinding
	for i, h := range st.holds {
		if h == nil {
			continue
		}
		id, ok := huntTableEvent(h)
		if !ok {
			out = append(out, huntFinding{class: "wait-no-table-names", state: st, message: fmt.Sprintf("the %s holds on %s, which no hold reason names", w.cfg.members[i].comp, h)})
			continue
		}
		if id == "coordination/budget.unavailableExhausted" && st.sides[i].count(huntOldUnreported) > 0 {
			out = append(out, huntFinding{class: "budget-counts-an-unreported-member-twice", state: st,
				message: fmt.Sprintf("the %s holds on %s while a worker of one of its gangs stands on a node that stopped reporting: the gang is already out of the serving count and its own rebuild is charged again until the taint manager evicts the pod", w.cfg.members[i].comp, h)})
		}
	}
	return out
}

// admissibleStarts is how many starts the batch step admits a member from
// zero, given the other members as they stand: the first start the step
// would hold ends the count.
func admissibleStarts(batch batchStep, acting int) int32 {
	probe := batch
	probe.acting = acting
	probe.members = append([]batchMember(nil), batch.members...)
	movable := batch.members[acting].movable()
	var k int32
	for k = 0; k < movable; k++ {
		probe.members[acting].promoted, probe.members[acting].open, probe.members[acting].ended = k, 0, 0
		if gate, _ := probe.hold(""); gate != "" {
			break
		}
	}
	return k
}

// fillReadings reads each pairing side's starts against what the batch
// step admits from the peer's settled count: a side with more starts than
// that filled a batch past its size (batch-overfilled), a side with fewer
// starts than the batch the peer has moved past while it still has
// Instances to start left one short (batch-underfilled). Each is read by
// its cause, and with none it fails.
func (w *huntWorld) fillReadings(st *huntState, batch batchStep) []huntFinding {
	var out []huntFinding
	eng, dec := w.pairIndex()
	for _, pair := range [][2]int{{eng, dec}, {dec, eng}} {
		me, peer := w.batchMemberOf(batch, pair[0]), w.batchMemberOf(batch, pair[1])
		if me < 0 || peer < 0 || st.sides[pair[0]].behind || st.sides[pair[1]].behind {
			continue
		}
		member := batch.members[me]
		if admissible := admissibleStarts(batch, me); member.started() > admissible {
			out = append(out, huntFinding{class: "batch-overfilled" + w.fillCause(st, pair[0], pair[1]), state: st,
				message: fmt.Sprintf("the %s has started %d of %d movable while the batch step admits %d from the %s's settled count: %d start(s) the plan never made", member.component, member.started(), member.movable(), admissible, batch.members[peer].component, member.started()-admissible)})
		}
		other := batch.members[peer]
		opened := openedBatch(other)
		if opened < 2 || member.started() >= member.mark(opened-1) || member.offTarget-member.held-member.open-member.ended <= 0 {
			continue
		}
		out = append(out, huntFinding{class: "batch-underfilled" + w.fillCause(st, pair[0], pair[1]), state: st,
			message: fmt.Sprintf("the %s has opened batch %d while the %s has started %d of the %d its batch %d asks for, with Instances left to start", other.component, opened, member.component, member.started(), member.mark(opened-1), opened-1)})
	}
	return out
}

// openedBatch is the last batch a member has opened a start of: the first
// batch whose mark covers its started count, so a planned batch of no
// Instances is never read as opened; zero with nothing started.
func openedBatch(m batchMember) int32 {
	if m.started() == 0 {
		return 0
	}
	for b := int32(1); b < 2*m.desired+4; b++ {
		if m.mark(b) >= m.started() {
			return b
		}
	}
	return 0
}

// batchMemberOf is the index of side i among the batch step's members, -1
// when the step does not count it.
func (w *huntWorld) batchMemberOf(batch batchStep, i int) int {
	for k, bm := range batch.members {
		if bm.component == w.cfg.members[i].comp {
			return k
		}
	}
	return -1
}

// fillCause names why a side's batches sit off the step's count, in the
// order the causes are read: a gang the roll rebuilt with no gate asked,
// the coordinator's follower write, a scale-up whose created Instances
// count as starts once they serve, a scale-down that retired settled
// Instances, an attempt that ended and was re-opened by a retry (an ended
// attempt settles its batch, the retry counts as open again), a partition
// change that moved the marks, the pairing waiver during a protocol change
// (which admits a start beside a peer the pairing gate holds still,
// whatever else stands), a gang out on a lost node; with none the class
// stands alone and fails. An attempt the placement grace ended is no
// cause: the Instance reads as still to start, so the peer keeps waiting
// on its batch and the side's next start is that attempt again.
func (w *huntWorld) fillCause(st *huntState, me, peer int) string {
	s, p := st.sides[me], st.sides[peer]
	switch {
	case s.rebuilt > 0 || p.rebuilt > 0:
		return "-by-a-rebuild"
	case s.followed > 0 || p.followed > 0:
		return "-by-a-follower-scale"
	case s.added > 0 || p.added > 0:
		return "-by-a-scale-up"
	case s.removed > 0 || p.removed > 0:
		return "-after-a-scale-down"
	case s.attempts > 0 || p.attempts > 0:
		return "-after-a-retry"
	case s.parts < w.cfg.parts || p.parts < w.cfg.parts || st.partitioned():
		return "-after-a-partition-change"
	case w.cfg.protocolChange:
		return "-in-protocol-change"
	case s.lostNodeStands() || p.lostNodeStands():
		return "-beside-a-lost-node"
	}
	return ""
}

// eventModelShapes are the reference prefill:decode shapes with the
// budget each side steps by: ten percent, and the whole counts of one, two
// and three Instances a batch.
var eventModelShapes = []struct {
	shape  [2]int32
	budget string
}{
	{[2]int32{12, 8}, "10%"}, {[2]int32{12, 8}, "2"}, {[2]int32{16, 8}, "3"}, {[2]int32{12, 4}, "1"},
	{[2]int32{15, 15}, "10%"}, {[2]int32{20, 10}, "10%"}, {[2]int32{24, 8}, "10%"}, {[2]int32{30, 10}, "10%"},
	{[2]int32{16, 8}, "1"}, {[2]int32{20, 10}, "2"}, {[2]int32{24, 8}, "3"},
}

// eventModelConfig is one configuration of the pair with the events on:
// two partition changes and one scale (the anchor's alone under a
// follower), one failure, and on gangs one member lost mid-surge and one
// lost on a standing gang, per side; the knobs the family sets.
func eventModelConfig(family string, shape [2]int32, budget string, es, ds v1beta1.UpdateStrategyType, setting batchModelSetting, gang int32) huntConfig {
	cfg := huntConfig{
		members: []huntMember{
			{comp: v1beta1.EngineComponent, desired: shape[0], strategy: es, budget: budget, gang: gang},
			{comp: v1beta1.DecoderComponent, desired: shape[1], strategy: ds, budget: budget, gang: gang},
		},
		defaults: setting.defaults, tolerance: setting.tolerance, maxAttempts: 3,
		fails: 1, scales: 1, parts: 2, losses: 1, refills: 1, scaleDeltas: []int32{1, -1, 2, -2},
		probes: true, shareBound: true, eventReadings: true,
	}
	cfg.name = family + ": " + cfg.String() + " " + setting.name
	return cfg
}

// eventFamily is one of the model's families: which knobs it turns on.
type eventFamily struct {
	name                string
	follower            v1beta1.ComponentType
	refusals, lostNodes int8
	forceDelete, pairs  bool
	noPlacementGrace    bool
	gangs               []int32
	settings            []batchModelSetting
	shapes              []int
	strategies          [][2]v1beta1.UpdateStrategyType
	protocolChange      bool
}

// eventModelFamilies lists the families: the follower written by the
// coordinator; a member the scheduler refuses, on single pods and gangs;
// a worker's node that stops reporting, with and without the force-delete
// policy; and two events in one pass. Full mode widens each over every
// shape and the first three settings, the follower and refused families
// over every setting, and adds the anchor on the other side, a refusal
// under the mixed strategy pairs, a second refusal with no placement
// grace, a protocol change beside two events, and a seeded sample over the
// whole space.
func eventModelFamilies(full bool) []eventFamily {
	settings := batchModelSettings()
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	both := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}}
	mixed := [][2]v1beta1.UpdateStrategyType{{recreate, surge}, {surge, recreate}}
	out := []eventFamily{
		{name: "follower", follower: v1beta1.DecoderComponent, gangs: []int32{0}, settings: settings[:3], shapes: []int{0, 2, 3}, strategies: both},
		{name: "refused", refusals: 1, gangs: []int32{0, 2}, settings: settings[:2], shapes: []int{0, 1, 3}, strategies: both},
		{name: "lostnode", lostNodes: 1, gangs: []int32{2, 4}, settings: settings[:1], shapes: []int{0, 3}, strategies: both},
		{name: "lostnode", lostNodes: 1, forceDelete: true, gangs: []int32{2, 4}, settings: settings[:1], shapes: []int{0, 3}, strategies: both},
		{name: "pairs", refusals: 1, lostNodes: 1, pairs: true, gangs: []int32{2}, settings: settings[2:3], shapes: []int{0, 2}, strategies: both},
	}
	if !full {
		return out
	}
	all := make([]int, len(eventModelShapes))
	for i := range all {
		all[i] = i
	}
	for i := range out {
		out[i].shapes = all
		out[i].settings = settings[:3]
		if out[i].lostNodes == 0 {
			out[i].settings = settings
		}
		if out[i].refusals > 0 && !out[i].pairs {
			out[i].gangs = []int32{0, 2, 4}
		}
	}
	out = append(out,
		eventFamily{name: "follower", follower: v1beta1.EngineComponent, gangs: []int32{0, 2}, settings: settings[:3], shapes: all, strategies: both},
		eventFamily{name: "refused", refusals: 1, gangs: []int32{0, 2}, settings: settings[:3], shapes: all, strategies: mixed},
		eventFamily{name: "refused", refusals: 2, noPlacementGrace: true, gangs: []int32{2, 4}, settings: settings[:3], shapes: all, strategies: both},
		eventFamily{name: "pairs", refusals: 1, lostNodes: 1, pairs: true, protocolChange: true, gangs: []int32{2, 4}, settings: settings[:3], shapes: all, strategies: both},
	)
	return out
}

// eventModelConfigs lists the configurations: every family over its
// shapes, settings, strategy pairs and gang sizes; in full mode a seeded
// sample over the whole space beside them.
func eventModelConfigs(full bool) []huntConfig {
	var out []huntConfig
	for _, family := range eventModelFamilies(full) {
		for _, shape := range family.shapes {
			for _, pair := range family.strategies {
				for _, setting := range family.settings {
					for _, gang := range family.gangs {
						out = append(out, family.config(eventModelShapes[shape].shape, eventModelShapes[shape].budget, pair, setting, gang))
					}
				}
			}
		}
	}
	if !full {
		return out
	}
	settings := batchModelSettings()
	families := eventModelFamilies(true)
	rng := rand.New(rand.NewSource(20261104))
	for n := 0; n < 150; n++ {
		family := families[rng.Intn(len(families))]
		family.pairs, family.protocolChange = rng.Intn(3) == 0, rng.Intn(3) == 0
		shape := eventModelShapes[rng.Intn(len(eventModelShapes))]
		pair := family.strategies[rng.Intn(len(family.strategies))]
		cfg := family.config(shape.shape, shape.budget, pair, settings[rng.Intn(len(settings))], []int32{0, 2, 4}[rng.Intn(3)])
		cfg.fails, cfg.parts, cfg.scales = int8(rng.Intn(3)), int8(1+rng.Intn(3)), int8(1+rng.Intn(2))
		if rng.Intn(4) == 0 {
			cfg.maxAttempts = 1
		}
		cfg.name = "sample: " + cfg.String() + " " + settings[0].name
		out = append(out, cfg)
	}
	return out
}

// config builds one configuration of the family.
func (f eventFamily) config(shape [2]int32, budget string, pair [2]v1beta1.UpdateStrategyType, setting batchModelSetting, gang int32) huntConfig {
	cfg := eventModelConfig(f.name, shape, budget, pair[0], pair[1], setting, gang)
	cfg.follower, cfg.refusals, cfg.lostNodes, cfg.forceDelete, cfg.pairs, cfg.noPlacementGrace, cfg.protocolChange = f.follower, f.refusals, f.lostNodes, f.forceDelete, f.pairs, f.noPlacementGrace, f.protocolChange
	if gang == 0 {
		cfg.lostNodes = 0
	}
	cfg.name = f.name + ": " + cfg.String() + " " + setting.name
	return cfg
}

// eventModelFailing are the classes that name a bug: the batch-step
// model's, a hold no table names, a batch filled past or short of what the
// step admits with no known cause, and two sides each waiting on the
// other.
func eventModelFailing() map[string]bool {
	out := map[string]bool{"wait-no-table-names": true, "batch-overfilled": true, "batch-underfilled": true, "sides-wait-on-each-other": true}
	for class := range batchModelFailing {
		out[class] = true
	}
	return out
}

// eventModelKnown are the classes that record a known reading of the current
// engine beside the batch-step model's: each names what the operator sees
// and the test that holds it.
func eventModelKnown() map[string]string {
	out := map[string]string{
		"batch-overfilled-by-a-rebuild":              "a gang short of a worker on the old version is rebuilt onto the target by the roll with no gate asked (skipsCoordinationGate), a start the batch plan never made, which the step counts from then on (held by TestBatchStepEvents_AGangShortOfAWorkerIsRebuiltByTheRollWithNoGateAsked and TestReconcile_GangShortOfAWorker_RollRebuildsItWhileTheGateHoldsEveryStart)",
		"batch-underfilled-by-a-rebuild":             "the peer's rebuild of a short gang, a start no gate was asked for, opened its next batch over this side's batch still short (the same reading)",
		"batch-overfilled-by-a-scale-up":             "a scale-up's created Instances come up on the new version and count as starts (TestBatchStep_AScaledUpSidesCreatedInstancesAreReadAsBatchesAlreadyStarted)",
		"batch-underfilled-by-a-scale-up":            "a scale-up's created Instances counted as the peer's starts opened its next batch early",
		"batch-overfilled-by-a-follower-scale":       "the coordinator's follower write is a scale-up whose created Instances count as starts, one service pass after the anchor's (TestBatchStepEvents_AFollowersCountLandsOneServicePassAfterTheAnchors)",
		"batch-underfilled-by-a-follower-scale":      "the coordinator's follower write re-planned the batches under the open ones",
		"version-mix-from-a-follower-scale":          "the coordinator's follower write widens or narrows the follower's movable count one service pass after the anchor's",
		"follower-count-a-pass-behind":               "the coordinator sizes the follower from the anchor's replica count, which the projector writes in the same pass, so the follower's count lands one service pass later and the batch step plans from the old one meanwhile (TestBatchStepEvents_AFollowersCountLandsOneServicePassAfterTheAnchors)",
		"batch-overfilled-after-a-scale-down":        "a scale-down retired settled Instances of the peer, so the starts already made stand past what the step admits now",
		"batch-underfilled-after-a-scale-down":       "a scale-down re-planned the batches under the open ones",
		"batch-overfilled-after-a-partition-change":  "a partition change moved the marks under the starts already made",
		"batch-underfilled-after-a-partition-change": "a partition ends one side early or re-plans the marks under the open batch",
		"batch-overfilled-after-a-retry":             "an attempt that ended settled its batch for the peer, whose starts stand, and its retry counts as open again (the decided reading of an ended attempt)",
		"batch-underfilled-after-a-retry":            "an attempt that ended settled its batch early for the peer, and its retry re-opened it (the decided reading of an ended attempt)",
		"batch-overfilled-beside-a-lost-node":        "a gang with a worker on a node that stopped reporting is counted by its revision while the serving count leaves it out, so the step's admission and the roll's plan differ by that gang",
		"batch-underfilled-beside-a-lost-node":       "the peer's batch opened over a gang this side has out on a node that stopped reporting",
		"batch-overfilled-in-protocol-change":        "the pairing waiver lets a side run ahead of a peer the pairing gates hold still, by design",
		"batch-underfilled-in-protocol-change":       "the pairing waiver lets a side run ahead of a peer the pairing gates hold still, by design",
		"waits-on-a-lost-node-without-force-delete":  "with no force-delete policy (the chart's default) a pod on a node that stopped reporting stands until an operator removes it: the attempt or the repair behind it waits, the peer waits on that Instance's batch, and no hold names the pod",
		"budget-counts-an-unreported-member-twice":   "between the node's death and the taint manager's eviction the gang's pod is live, so the roll's rebuild of it is not exempt from the group's unavailability budget, which already counts the gang out: the start waits on the budget until the eviction (TestBatchStepEvents_ALostNodeUnderAnOldGangHoldsTheBatchUntilThePodIsRemoved)",
	}
	for class, text := range batchModelKnown {
		out[class] = text
	}
	return out
}

// eventModelRunner reads the event model's classes.
func eventModelRunner() huntModelRunner {
	return huntModelRunner{fullEnv: "OME_BATCH_STEP_EVENTS_HUNT", onlyEnv: "OME_BATCH_STEP_EVENTS_HUNT_ONLY", failing: eventModelFailing(), known: eventModelKnown()}
}

// TestBatchStepEvents_ReferenceShapesUnderFollowersRefusalsAndLostNodes
// drives the full gate stack over the reference prefill/decode
// shapes stepping one to three Instances a batch under both strategies,
// with a partition edit, a scale of either side by the operator or by the
// scaling coordinator, a gang member lost, refused placement or left on a
// node that stopped reporting, and two such events in one pass: every
// hold is one the engine's hold reasons name, no batch fills past or short of what the
// step admits without a known cause, no two sides wait on each other, no
// hold stands without a way out, and every other reading is a known class.
func TestBatchStepEvents_ReferenceShapesUnderFollowersRefusalsAndLostNodes(t *testing.T) {
	runner := eventModelRunner()
	runner.explore(t, eventModelConfigs(runner.full()))
}

// TestBatchStepEvents_AGangShortOfAWorkerIsRebuiltByTheRollWithNoGateAsked
// records what the roll does with an old gang that loses a worker
// mid-batch. 12 prefill beside 8 decode in gangs of 2, both RecreatePod at
// 10 percent, the chart's setting (keepRatio at 5, the decoder first). The
// decoder's first drain is open and every prefill start waits on "engine
// batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)".
// Prefill gang 5, still on version 1, loses a worker. The repair yields
// the gang to the roll (RepairYieldsToTarget) and the roll rebuilds it
// onto version 2 in its next pass with no gate asked
// (skipsCoordinationGate): the first-side wait that holds every other
// prefill start is never consulted for it, and the prefill side stands at
// one start against a plan of none. The explorer reads the picture as
// batch-overfilled-by-a-rebuild; this test fails the day the rebuild of a
// short gang is paced by the batch step or counted apart from its starts.
func TestBatchStepEvents_AGangShortOfAWorkerIsRebuiltByTheRollWithNoGateAsked(t *testing.T) {
	lockShortGangStartsWithNoGateAsked(t, v1beta1.UpdateStrategyRecreatePod, huntOpenDrain, huntOpenDrain)
}

// TestBatchStepEvents_AGangShortOfAWorkerSurgesByTheRollWithNoGateAsked is
// the same loss under SurgeThenDrain on both sides: the decoder's first
// surge is open and every prefill start waits on the first side. The
// short gang serves nothing until its set is whole again, so the roll
// surges its replacement onto version 2 with no gate asked, as the
// recreate rebuilds it; a gate that holds the side for its peer would
// otherwise leave the gang short for the whole hold. This test fails the
// day a surge of a short gang is put to the gate again.
func TestBatchStepEvents_AGangShortOfAWorkerSurgesByTheRollWithNoGateAsked(t *testing.T) {
	lockShortGangStartsWithNoGateAsked(t, v1beta1.UpdateStrategySurgeThenDrain, huntOpenSurge, huntSurgeShort)
}

// lockShortGangStartsWithNoGateAsked drives the shared picture: the
// decoder's first attempt (peerOpen) is open and every prefill start
// waits on it; prefill gang 5 loses a worker and the roll starts it with
// no gate asked, its row reading started; the hold still names the first
// side for every other start; the explorer reads batch-overfilled-by-a-rebuild;
// and the roll finishes.
func lockShortGangStartsWithNoGateAsked(t *testing.T, strategy v1beta1.UpdateStrategyType, peerOpen, started huntRow) {
	t.Helper()
	chart := batchModelSettings()[2]
	cfg := eventModelConfig("lock", [2]int32{12, 8}, "10%", strategy, strategy, chart, 2)
	cfg.fails, cfg.scales, cfg.parts = 0, 0, 0
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	want := "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)"
	if st.sides[0].inFlight() || st.sides[1].count(peerOpen) != 1 || st.holds[0] == nil || st.holds[0].reason != want {
		t.Fatalf("the decoder's first attempt must be open and every prefill start held on %q: %s", want, st)
	}
	lost := 5
	st = w.apply(st, huntMove{kind: "lose", side: 0, idx: lost})
	if st.sides[0].rows[lost] != started {
		t.Fatalf("the roll must start the short gang onto version 2 with no gate asked while decoder batch 1 has not served, got %s (hold %s)", st.sides[0].rows[lost], st.holds[0])
	}
	if h := st.holds[0]; h == nil || h.reason != want {
		t.Fatalf("every other prefill start must still wait on the first side, got %s", h)
	}
	if got := st.sides[0].count(started); got != 1 || st.sides[0].rebuilt != 1 {
		t.Fatalf("the prefill side must have the one rebuild open and nothing else: %s", st.sides[0])
	}
	found := false
	for _, f := range w.check(st) {
		if f.class == "batch-overfilled-by-a-rebuild" {
			found = true
			t.Logf("%s", f.message)
		}
	}
	if !found {
		t.Fatalf("the explorer must read the picture as batch-overfilled-by-a-rebuild: %s", st)
	}
	if end := w.playOut(st); !end.complete() {
		t.Fatalf("the roll must finish after the rebuild: %s", end)
	}
}

// settleWith makes one move and runs the passes in the given order, the
// way the two replica controllers may interleave after a status write.
func settleWith(w *huntWorld, st *huntState, mv huntMove, order []int) *huntState {
	next := st.clone()
	next.path = append(next.path, mv)
	w.applyRaw(next, mv)
	w.settle(next, order)
	return next
}

// TestBatchStepEvents_ARefusedAttemptEndsOnTheGraceWithTheRetriesUntouched
// records how a member the scheduler cannot place reads to the batch step.
// 12 prefill beside 8 decode, both RecreatePod at 10 percent, the chart's
// setting. The decoder's first drain is refused placement. While it waits
// the prefill side's first batch waits on it under the batch step ("engine
// batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)"), a
// hold the coordination table names, with the cause on the decoder's row
// alone (the instance table's pod.unschedulable) and the way out the
// scheduler's or the operator's placement grace. When the grace ends the
// attempt the retry record is left alone (environment-caused), so the
// decoder is never Held; with no record the attempt counts as neither
// started nor settled, and the decoder starts the same Instance again in
// its next pass as the batch it belongs to. A prefill pass that reads the
// ended attempt first keeps waiting on decoder batch 1 and names the
// Instance ("...; decoder Instance 0 starts again"; the engine's own row
// adds the reason, "Unschedulable"); one that runs after the retry waits
// on the open drain again; and the first-side rule stops the prefill side
// at its first batch until a decoder Instance serves. This test fails the
// day an attempt the grace ended settles its batch for the peer, spends
// the retry record, or goes unnamed in the hold.
func TestBatchStepEvents_ARefusedAttemptEndsOnTheGraceWithTheRetriesUntouched(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	chart := batchModelSettings()[2]
	cfg := eventModelConfig("lock", [2]int32{12, 8}, "10%", recreate, recreate, chart, 0)
	cfg.fails, cfg.scales, cfg.parts, cfg.refusals = 0, 0, 0, 2
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.apply(st, huntMove{kind: "refuse", side: 1, idx: 0})
	want := "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving)"
	if st.sides[1].rows[0] != huntDrainRefused || st.sides[0].inFlight() || st.holds[0] == nil || st.holds[0].reason != want {
		t.Fatalf("the refused decoder drain must stand open with every prefill start held on %q: %s", want, st)
	}
	if id, ok := huntTableEvent(st.holds[0]); !ok || id != "coordination/ratio.batchWait[firstSide]" {
		t.Fatalf("the table must name the wait, got %q %v", id, ok)
	}
	// The decoder's pass runs first: the retry re-opens the Instance and
	// the prefill side reads it open again.
	after := settleWith(w, st, huntMove{kind: "expire", side: 1, idx: 0}, []int{1, 0})
	if after.sides[1].schedule != scheduleNone || after.sides[1].attempts != 0 || after.sides[1].rows[0] != huntOpenDrain {
		t.Fatalf("the placement grace must leave the retry record alone and the decoder retry at once, got %s", after.sides[1])
	}
	if after.sides[0].inFlight() || after.holds[0] == nil || after.holds[0].reason != want {
		t.Fatalf("a prefill pass after the retry must wait again on %q: %s", want, after)
	}
	// The prefill's pass runs first, over the ended attempt: with no retry
	// record the Instance is still to start, so the prefill side keeps
	// waiting on decoder batch 1 and the hold names the Instance.
	st = settleWith(w, after, huntMove{kind: "refuse", side: 1, idx: 0}, w.order(1))
	ended := st.clone()
	w.applyRaw(ended, huntMove{kind: "expire", side: 1, idx: 0})
	named := "engine batch 1 waits for decoder batch 1 to serve first (0 of 1 serving; decoder Instance 0 starts again)"
	if admitted, hold := w.pass(ended, 0); admitted != 0 || hold == nil || hold.reason != named {
		t.Fatalf("a prefill pass before the retry must keep waiting on the attempt the grace ended and name it, %q: admitted %d, hold %v: %s", named, admitted, hold, ended)
	}
	// The decoder's next start is that attempt again, asked as its first
	// batch and admitted; the prefill side then waits on the open drain.
	st = settleWith(w, st, huntMove{kind: "expire", side: 1, idx: 0}, []int{0, 1})
	if st.sides[0].inFlight() || st.sides[1].rows[0] != huntOpenDrain || st.sides[1].expired != 2 || st.sides[1].schedule != scheduleNone || st.holds[0] == nil || st.holds[0].reason != want {
		t.Fatalf("after the retry the prefill side must have nothing started and wait on %q, the decoder's attempt open again with the record untouched: %s", want, st)
	}
	for _, h := range st.holds {
		if h != nil && h.gate == v1beta1.RolloutHoldGateHeld {
			t.Fatalf("no hold may read Held for an attempt the placement grace ended: %s", h)
		}
	}
	runner := eventModelRunner()
	for _, f := range w.check(st) {
		if runner.fails(f.class) {
			t.Fatalf("the picture breaks an invariant: %s: %s", f.class, f.message)
		}
	}
	if end := w.playOut(st); !end.complete() {
		t.Fatalf("the roll must finish once the retry serves: %s", end)
	}
}

// TestBatchStepEvents_AFollowersCountLandsOneServicePassAfterTheAnchors
// records the window a proportional scaling policy opens under the batch
// step. 12 prefill beside 8 decode, both RecreatePod at 10 percent, no
// setting, the decoder following the engine at a ratio of 0.666. After the
// first batch serves the operator scales the engine to 24. The coordinator
// reads the anchor's count off its replica, which the same pass projects,
// so the decoder's count lands one service pass later: until then the
// batch step plans 24 prefill at a step of 3 beside 8 decode at a step of
// 1, and the decoder's next drain belongs to a batch sized for 8. The
// follower pass then writes 16, the step re-plans both sides (3 and 2),
// and the eight created decoders come up on version 2 and count as starts.
// The explorer reads the window as follower-count-a-pass-behind; this
// test fails the day the follower's count lands with the anchor's.
func TestBatchStepEvents_AFollowersCountLandsOneServicePassAfterTheAnchors(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	cfg := eventModelConfig("lock", [2]int32{12, 8}, "10%", recreate, recreate, batchModelSettings()[0], 0)
	cfg.fails, cfg.parts, cfg.follower = 0, 0, v1beta1.DecoderComponent
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	st = serveUntil(t, w, st, func(st *huntState) bool {
		return st.sides[0].count(huntPromoted) >= 2 && st.sides[1].count(huntPromoted) >= 1
	})
	st = w.apply(st, huntMove{kind: "scale", side: 0, delta: 12})
	side, delta, owed := w.followerOwed(st)
	if !owed || side != 1 || delta != 8 {
		t.Fatalf("the decoder must owe 8 Instances after the anchor's scale, got side %d delta %d owed %v", side, delta, owed)
	}
	steps := func(st *huntState) [2][2]int32 {
		gate := ResolveGateContextWithDefaults(w.ctx, w.client(st), w.service(st), v1beta1.EngineComponent, w.cfg.defaults)
		gate.TargetRevision = w.targets[0]
		batch, err := gate.readBatchStep()
		if err != nil || !batch.governs {
			t.Fatalf("the batch step must govern: governs=%v err=%v", batch.governs, err)
		}
		var out [2][2]int32
		for _, bm := range batch.members {
			k := 0
			if bm.component == v1beta1.DecoderComponent {
				k = 1
			}
			out[k] = [2]int32{bm.desired, bm.step}
		}
		return out
	}
	if got := steps(st); got != [2][2]int32{{24, 3}, {8, 1}} {
		t.Fatalf("before the follower pass the step plans from 24:8, got %v", got)
	}
	found := false
	for _, f := range w.check(st) {
		if f.class == "follower-count-a-pass-behind" {
			found = true
			t.Logf("%s", f.message)
		}
	}
	if !found {
		t.Fatalf("the explorer must read the window as follower-count-a-pass-behind: %s", st)
	}
	st = w.apply(st, huntMove{kind: "follower-pass", side: 1, delta: 8})
	if got := steps(st); got != [2][2]int32{{24, 3}, {16, 2}} {
		t.Fatalf("after the follower pass the step plans from 24:16, got %v", got)
	}
	if st.sides[1].count(huntCreating) != 8 || st.sides[1].followed != 1 {
		t.Fatalf("the follower pass must create 8 decoders on the target: %s", st.sides[1])
	}
	if end := w.playOut(st); !end.complete() || len(end.sides[1].rows) != 16 {
		t.Fatalf("the roll must finish at 24:16: %s", end)
	}
}

// TestBatchStepEvents_ALostNodeUnderAnOldGangHoldsTheBatchUntilThePodIsRemoved
// records what a node that stops reporting under a gang's worker does to
// an open batch. 12 prefill beside 8 decode in gangs of 2, both RecreatePod
// at 10 percent in a group whose own unavailability budget is 10 percent
// too, no setting, no force-delete policy (the chart's default). The first
// prefill batch has served and the decoder's first drain is open; the node
// under a worker of prefill gang 4, still on version 1, stops reporting.
// The pod stands with its Ready revoked, so the gang leaves the serving
// count and the group's unavailability budget counts it out: when the
// decoder's drain serves and prefill batch 2 opens, its second drain holds
// on "unavailable budget 2 exhausted ... counted as unavailable: Instance 2
// being updated, Instance 4 restarting", the gang counted beside the
// drain. The taint manager evicts the pod;
// the roll's rebuild of the gang, now short of a live member, opens with no
// gate asked and waits on the Terminating pod no kubelet removes. The roll
// stands: the decoder waits on the prefill batch the blocked rebuild
// belongs to, a hold the table names, while nothing on the prefill replica
// names the pod. The explorer reads the two pictures as
// budget-counts-an-unreported-member-twice and
// waits-on-a-lost-node-without-force-delete; with the policy on, the force
// delete frees the pod and the roll finishes. This test fails the day the
// unreported gang stops costing the group budget beside its own rebuild,
// or the blocked rebuild gains a way out of its own.
func TestBatchStepEvents_ALostNodeUnderAnOldGangHoldsTheBatchUntilThePodIsRemoved(t *testing.T) {
	recreate := v1beta1.UpdateStrategyRecreatePod
	cfg := eventModelConfig("lock", [2]int32{12, 8}, "10%", recreate, recreate, batchModelSettings()[0], 2)
	cfg.fails, cfg.scales, cfg.parts, cfg.lostNodes, cfg.groupUnavail = 0, 0, 0, 1, "10%"
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	st = w.apply(st, huntMove{kind: "serve", side: 0, idx: 0})
	st = w.apply(st, huntMove{kind: "serve", side: 0, idx: 1})
	if st.sides[0].inFlight() || st.sides[1].count(huntOpenDrain) != 1 {
		t.Fatalf("prefill batch 1 must have served with the decoder's first drain still open: %s", st)
	}
	lost := 4
	st = w.apply(st, huntMove{kind: "lostnode", side: 0, idx: lost})
	if st.sides[0].rows[lost] != huntOldUnreported {
		t.Fatalf("the gang must stand with its worker's Ready revoked, got %s", st.sides[0].rows[lost])
	}
	st = w.apply(st, huntMove{kind: "serve", side: 1, idx: 0})
	want := "unavailable budget 2 exhausted"
	if h := st.holds[0]; st.sides[0].count(huntOpenDrain) != 1 || h == nil || h.gate != v1beta1.RolloutHoldGateBudget || !strings.HasPrefix(h.reason, want) || !strings.Contains(h.reason, "Instance 4 restarting") {
		t.Fatalf("prefill batch 2 must open one drain and hold the second on %q with the unreported gang counted out: %s", want, st)
	}
	classes := map[string]bool{}
	for _, f := range w.check(st) {
		classes[f.class] = true
	}
	if !classes["budget-counts-an-unreported-member-twice"] {
		t.Fatalf("the explorer must read the window as budget-counts-an-unreported-member-twice: %v", classes)
	}
	st = w.apply(st, huntMove{kind: "evict", side: 0, idx: lost})
	st = w.apply(st, huntMove{kind: "serve", side: 0, idx: 2})
	if st.sides[0].rows[lost] != huntDrainBlocked {
		t.Fatalf("after the eviction the rebuild must open with no gate asked and wait on the Terminating pod, got %s (hold %s)", st.sides[0].rows[lost], st.holds[0])
	}
	end := w.playOut(st)
	if end.complete() || end.sides[0].rows[lost] != huntDrainBlocked {
		t.Fatalf("with no force-delete policy the blocked rebuild must stand: %s", end)
	}
	class, message := w.classifyStandstill(end)
	if class != "waits-on-a-lost-node-without-force-delete" {
		t.Fatalf("the standstill reads %s (%s), want waits-on-a-lost-node-without-force-delete", class, message)
	}
	if h := end.holds[1]; h == nil || !strings.HasPrefix(h.reason, "decoder batch ") || !strings.Contains(h.reason, "waits for engine batch ") {
		t.Fatalf("the decoder must wait on the prefill batch the blocked rebuild belongs to, got %s", h)
	}
	cfg.forceDelete = true
	w = newHuntWorld(t, cfg)
	if end := w.playOut(w.apply(end, huntMove{kind: "force-delete", side: 0, idx: lost})); !end.complete() {
		t.Fatalf("with the policy on the roll must finish once the pod is removed: %s", end)
	}
}
