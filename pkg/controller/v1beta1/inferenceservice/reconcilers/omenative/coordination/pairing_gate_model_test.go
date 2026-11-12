package coordination

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// This file drives the two pairing gates (CheckPairing at a start,
// EvaluatePairingDrain before an old pod leaves) and the gate stack that
// calls them over the reference prefill/decode shapes during a
// pairing-protocol change, in the world of
// pd_pacing_gates_never_wait_on_each_other_test.go. Beside that world's
// moves (a partition, a scale, a policy or rule edit, a gang member lost,
// a failure, a spent retry schedule, a canary step) it pushes a further
// revision as the one event at seven seed moments of a plain roll (idle,
// settling, rotating, draining, the last Instance, a canary step, held),
// in a breadth-first walk of 30 states from the first admission (80 in
// full mode) and in two random runs (three): a second protocol change
// while the first is open, a change back to the protocol the roll started
// from, or a protocol change on a roll that had none, which is where a
// kept replacement held to a revision the roll moved past, a peer whose promoted pod
// is not yet counted, and the pairable minimum of each side meet.
//
// Beside the world's readings (no two sides waiting on each other, no
// budget exceeded, the batch promise, the classifier) it reads the
// pairable capacity after every step the gates admitted: no side falls
// below its pairable minimum unless it already was, the fleet keeps a
// pairable pair, and a standstill names the gate the pairing gates wait
// on. A step the gates admit as the cheapest of a held picture or as an
// escape is reported in a
// class of its own.
//
// OME_PAIRING_HUNT=full widens the configurations and the bounds for
// a long local run; OME_PAIRING_HUNT_ONLY=<text> keeps the configurations whose
// name contains the text.

// huntPairable is the pairable capacity of one state: each pairing side's
// serving pods by cohort, the same as the gates count them (a promoted
// Instance whose pods the counters have not caught up with left out, as
// it is for the one status write the gates read it in), each side's
// Instances with the cohorts of their serving pods, each side's pairable
// minimum, and the Instances with a partner.
type huntPairable struct {
	serving        map[v1beta1.ComponentType]map[string]int32
	gatesServing   map[v1beta1.ComponentType]map[string]int32
	instances      map[v1beta1.ComponentType]map[string][]string
	gatesInstances map[v1beta1.ComponentType]map[string][]string
	minimums       map[v1beta1.ComponentType]pairFloor
	pairable       map[v1beta1.ComponentType]int32
	gatesPairable  map[v1beta1.ComponentType]int32
	pair           bool
}

// servingCohorts are the cohorts of one row's pods in rotation: its own
// pods' when they serve, its replacement's once that serves. A promoted
// Instance whose pods the counters have not caught up with serves its own
// cohort, the middle revision's after a further push.
func (w *huntWorld) servingCohorts(s huntSide, idx int) []string {
	source, attempted := w.rowProtocols(s, idx)
	var out []string
	switch s.rows[idx] {
	case huntOld, huntRetriesStopped, huntFailedSurge, huntFailedGangKept, huntOpenSurge, huntSurgeShort, huntSurgeLostNode, huntSurgeRefused,
		huntOldShort, huntOldLostMember, huntOldUnreported, huntOldLostNode:
		out = append(out, source)
	case huntReplacementHeld, huntReplacementShort:
		out = append(out, source, attempted)
	case huntPromoted, huntPromotedShort, huntPromotedUnreported, huntSourceDrainBlocked:
		out = append(out, attempted)
	case huntPromotedUncounted:
		if s.srcMiddle(idx) {
			out = append(out, source)
		} else {
			out = append(out, attempted)
		}
	}
	return out
}

// countedByGates reports a row whose serving pods the gates count: every
// row but a promoted Instance whose counters still read the publication
// before its pods served.
func countedByGates(r huntRow) bool { return r != huntPromotedUncounted }

// pairablePicture reads the pairable capacity of a state with the gates'
// own counting (pairableInstanceCount, pairableServingPairExists) and
// each side's pairable minimum as the gate context resolves it: under the
// group's pacing while a group stands, the recorded group's once an edit
// removed it, the side's own budget in a canary unit.
func (w *huntWorld) pairablePicture(st *huntState) huntPairable {
	p := huntPairable{
		serving:        map[v1beta1.ComponentType]map[string]int32{},
		gatesServing:   map[v1beta1.ComponentType]map[string]int32{},
		instances:      map[v1beta1.ComponentType]map[string][]string{},
		gatesInstances: map[v1beta1.ComponentType]map[string][]string{},
		minimums:       map[v1beta1.ComponentType]pairFloor{},
		pairable:       map[v1beta1.ComponentType]int32{},
		gatesPairable:  map[v1beta1.ComponentType]int32{},
	}
	eng, dec := w.pairIndex()
	if eng < 0 || dec < 0 {
		return p
	}
	svc := w.service(st)
	var groupPacing *v1beta1.CoordinationPacing
	if groups := svc.Spec.Rollout.Groups; len(groups) > 0 {
		if ru := groups[0].RollingUpdate; ru != nil {
			groupPacing = &v1beta1.CoordinationPacing{MaxUnavailable: ru.MaxUnavailable, MaxSurge: ru.MaxSurge}
		}
	} else if ru := w.isvc.Spec.Rollout.Groups[0].RollingUpdate; ru != nil {
		groupPacing = &v1beta1.CoordinationPacing{MaxUnavailable: ru.MaxUnavailable, MaxSurge: ru.MaxSurge}
	}
	for _, i := range []int{eng, dec} {
		comp, s := w.cfg.members[i].comp, st.sides[i]
		p.serving[comp], p.gatesServing[comp] = map[string]int32{}, map[string]int32{}
		p.instances[comp], p.gatesInstances[comp] = map[string][]string{}, map[string][]string{}
		for idx, r := range s.rows {
			for _, cohort := range w.servingCohorts(s, idx) {
				p.serving[comp][cohort]++
				key := fmt.Sprint(idx)
				p.instances[comp][key] = append(p.instances[comp][key], cohort)
				if countedByGates(r) {
					p.gatesServing[comp][cohort]++
					p.gatesInstances[comp][key] = append(p.gatesInstances[comp][key], cohort)
				}
			}
		}
		ir := w.replica(i, s)
		if w.cfg.canary != nil {
			p.minimums[comp] = ownPairableMinimum(ir)
		} else {
			p.minimums[comp] = resolvePairFloor(ir, pacingWithDefaults(groupPacing, w.cfg.defaults))
		}
	}
	for _, comp := range pairingComponents {
		p.pairable[comp] = pairableInstanceCount(p.instances[comp], p.serving[pairingPeer(comp)])
		p.gatesPairable[comp] = pairableInstanceCount(p.gatesInstances[comp], p.gatesServing[pairingPeer(comp)])
	}
	p.pair = pairableServingPairExists(p.serving)
	return p
}

// servingSides counts the pairing sides with a serving Instance.
func (p huntPairable) servingSides() int {
	n := 0
	for _, comp := range pairingComponents {
		if totalServing(p.serving[comp]) > 0 {
			n++
		}
	}
	return n
}

// breach names the first side a step took below its pairable minimum
// while it stood at or above the smaller of the minimum and its count
// before the step, the gates' own rule for a hold on the minimum; "" when
// none.
func (p huntPairable) breach(before huntPairable) string {
	return breachOf(p.pairable, before.pairable, p.minimums)
}

func breachOf(after, before map[v1beta1.ComponentType]int32, minimums map[v1beta1.ComponentType]pairFloor) string {
	for _, comp := range pairingComponents {
		minimum := minimums[comp]
		if !minimum.Applies() {
			continue
		}
		if after[comp] < minimum.floor && after[comp] < before[comp] {
			return fmt.Sprintf("the %s keeps %d pairable Instance(s), below its %s, from %d before the step", comp, after[comp], minimum, before[comp])
		}
	}
	return ""
}

// drainDecision reports a move whose capacity change is the drain gate's
// answer: an old pod or a removed replacement leaving rotation on a
// release, a removal, a surge that serves and is promoted, a mended
// replacement promoted, or the further push removing the replacements
// it does not keep.
func drainDecision(mv huntMove) bool {
	switch mv.kind {
	case "release", "remove", "serve", "mend", "repush":
		return true
	}
	return false
}

// readPairingBreach reads the pairable capacity across one move: parent is
// the state the move landed on, moved the state after the move and before
// the admission that follows, settled the state after it. A drop below the
// pairable minimum, or the last pairable pair lost, is the drain gate's
// when the move is its decision and the start-time gate's when the
// admission's starts made it; nil when the capacity held.
func (w *huntWorld) readPairingBreach(parent, moved, settled *huntState, mv huntMove) *huntFinding {
	if !w.transition(settled) {
		return nil
	}
	target := w.targetProtocol(settled)
	before, afterMove, after := w.pairablePicture(parent), w.pairablePicture(moved), w.pairablePicture(settled)
	if drainDecision(mv) && before.transitionInFlight(target) {
		if f := w.drainBreach(parent, moved, settled, mv, before, afterMove); f != nil {
			return f
		}
	}
	why, pairLost := after.breach(afterMove), afterMove.pair && !after.pair && after.servingSides() == 2
	if why == "" && !pairLost {
		return nil
	}
	if !afterMove.transitionInFlight(target) {
		// The gates are inert while no serving Instance they count runs a
		// protocol other than the target's; a promoted Instance whose pods
		// are not yet counted is not a serving Instance to them.
		return &huntFinding{class: "step-below-the-pairable-minimum-outside-a-transition", state: settled,
			message: fmt.Sprintf("the starts admitted after %s left %s while the gates read no transition in flight (serving %v, the gates count %v)", mv, why, afterMove.serving, afterMove.gatesServing)}
	}
	if comp, ok := breachSide(after, afterMove); ok {
		if f := w.lossOfTheStartsThemselves(moved, settled, mv, comp, why, afterMove, after); f != nil {
			return f
		}
	}
	class, reason := w.classifyAdmittedStart(moved, settled)
	switch {
	case class != "":
		return &huntFinding{class: class, state: settled, message: fmt.Sprintf("the gate admitted the start as %q; %s", reason, why)}
	case why != "":
		return &huntFinding{class: "step-below-the-pairable-minimum", state: settled,
			message: fmt.Sprintf("the starts admitted after %s left %s (the pairing gate's reading of the first: %q)", mv, why, reason)}
	}
	return &huntFinding{class: "step-left-no-pairable-pair", state: settled,
		message: fmt.Sprintf("the starts admitted after %s left no serving engine paired with a serving decoder: %v", mv, after.serving)}
}

// lossOfTheStartsThemselves reads a side's drop below its minimum against
// the starts admitted on it: a drop no larger than the in-place patches
// opened on that side is the patches' own pods leaving rotation, which
// the gate credits back and the budget bounds; a drop the Instances the
// gates count do not show beyond those patches falls on promoted
// Instances whose counters lag one status write. Nil when the drop is
// more than either.
func (w *huntWorld) lossOfTheStartsThemselves(moved, settled *huntState, mv huntMove, comp v1beta1.ComponentType, why string, afterMove, after huntPairable) *huntFinding {
	var inPlace int32
	for _, s := range w.admittedDrainStarts(moved, settled) {
		if s.inPlace && w.cfg.members[s.side].comp == comp {
			inPlace++
		}
	}
	podsLoss := afterMove.pairable[comp] - after.pairable[comp]
	gatesLoss := afterMove.gatesPairable[comp] - after.gatesPairable[comp]
	switch {
	case podsLoss <= inPlace:
		return &huntFinding{class: "in-place-patch-below-the-pairable-minimum", state: settled,
			message: fmt.Sprintf("the starts admitted after %s left %s: %d in-place patch(es) of the %s took their own pods out of rotation", mv, why, inPlace, comp)}
	case gatesLoss <= inPlace:
		return &huntFinding{class: "step-below-the-pairable-minimum-behind-the-counters", state: settled,
			message: fmt.Sprintf("the starts admitted after %s left %s by the pods, while the Instances the gates count lost nothing beyond %d in-place patch(es) (%v counted of %v)", mv, why, inPlace, after.gatesPairable, after.pairable)}
	}
	return nil
}

// breachSide is the first side a step took below its pairable minimum by
// the pods' counting.
func breachSide(after, before huntPairable) (v1beta1.ComponentType, bool) {
	for _, comp := range pairingComponents {
		minimum := after.minimums[comp]
		if minimum.Applies() && after.pairable[comp] < minimum.floor && after.pairable[comp] < before.pairable[comp] {
			return comp, true
		}
	}
	return "", false
}

// drainBreach reads the drain gate's decision across one move: a side
// taken below its pairable minimum (a kept replacement removed by the push
// is its own class) or the last pairable pair lost; nil when the capacity
// held.
func (w *huntWorld) drainBreach(parent, moved, settled *huntState, mv huntMove, before, afterMove huntPairable) *huntFinding {
	if why := afterMove.breach(before); why != "" {
		class := "drain-below-the-pairable-minimum"
		if w.replacementRemoved(parent, moved) {
			class = "replacement-removed-below-the-pairable-minimum"
		}
		return &huntFinding{class: class, state: settled, message: fmt.Sprintf("after %s the drain gate's answer left %s", mv, why)}
	}
	if before.pair && !afterMove.pair && afterMove.servingSides() == 2 {
		return &huntFinding{class: "drain-left-no-pairable-pair", state: settled,
			message: fmt.Sprintf("after %s no serving engine pairs with a serving decoder: %v", mv, afterMove.serving)}
	}
	return nil
}

// targetProtocol is the pairing protocol of the revision the roll moves to
// at a state.
func (w *huntWorld) targetProtocol(st *huntState) string {
	protocols := w.cfg.protocols()
	if st.pushed {
		return protocols[2]
	}
	return protocols[1]
}

// transitionInFlight reports a picture in which the pairing gates are
// active: some serving pod the gates count runs a protocol other than the
// target's.
func (p huntPairable) transitionInFlight(target string) bool {
	for _, comp := range pairingComponents {
		for proto, n := range p.gatesServing[comp] {
			if n > 0 && proto != "" && proto != target {
				return true
			}
		}
	}
	return false
}

// admittedStart is one drain-first start the admission after a move
// opened: its side and row, whether it patches the pod in place, and
// whether the roll opened it with no gate asked.
type admittedStart struct {
	side, idx int
	inPlace   bool
	exempt    bool
}

// admittedDrainStarts lists the drain-first starts the admission opened
// between moved and settled, in side and index order.
func (w *huntWorld) admittedDrainStarts(moved, settled *huntState) []admittedStart {
	var out []admittedStart
	for i := range w.cfg.members {
		for idx, r := range settled.sides[i].rows {
			if !r.openDrain() || idx >= len(moved.sides[i].rows) || moved.sides[i].rows[idx].openDrain() {
				continue
			}
			out = append(out, admittedStart{side: i, idx: idx, inPlace: r == huntOpenInPlace, exempt: idx < 64 && settled.sides[i].exempt&(1<<idx) != 0})
		}
	}
	return out
}

// classifyAdmittedStart names what admitted the drain-first starts that
// took a side below its pairable minimum. A start the roll opened with no
// gate asked (a gang short of a worker it rebuilds) and a patch in place,
// whose returning pod the gate credits, are read in classes of their own.
// For the rest the pairing gate is asked again, on the picture the
// admission began with, about the first of them, and the escape it took
// is named when it took one: the rule that admits the cheapest step
// when the minimum holds every step, or the one-by-one wall and the final
// hop that keep a transition live. The gate's reason is returned beside
// the class, empty when the start was admitted on the ordinary rules.
func (w *huntWorld) classifyAdmittedStart(moved, settled *huntState) (class, reason string) {
	starts := w.admittedDrainStarts(moved, settled)
	target := w.targetProtocol(moved)
	inPlaceOnly := len(starts) > 0
	for k := range starts {
		s := &starts[k]
		if s.exempt {
			return "rebuild-with-no-gate-asked-below-the-pairable-minimum", "a gang short of a worker rebuilt by the roll with no gate asked"
		}
		if !s.inPlace {
			inPlaceOnly = false
		}
		if source, _ := w.rowProtocols(moved.sides[s.side], s.idx); source == target && !s.inPlace {
			return "same-cohort-step-below-the-pairable-minimum", fmt.Sprintf("%s Instance %d already runs the target protocol %q, so the start-time gate charged its step to no cohort", w.cfg.members[s.side].comp, s.idx, target)
		}
	}
	// Each start is asked again, with its own mechanics, on the picture
	// before the admission; the first whose answer names a rule of the
	// gate names the class.
	for _, s := range starts {
		m := w.cfg.members[s.side]
		if m.comp != v1beta1.EngineComponent && m.comp != v1beta1.DecoderComponent {
			continue
		}
		gate := ResolveGateContextWithDefaults(w.ctx, w.client(moved), w.service(moved), m.comp, w.cfg.defaults)
		gate.TargetRevision = w.rollTargetFor(moved, s.side)
		strategy := workloadtypes.UpdateStrategyRecreatePod
		if s.inPlace {
			strategy = workloadtypes.UpdateStrategyInPlaceIfPossible
		}
		w.gateCalls++
		answer := gate.pairingVerdict(w.fromRevisionFor(moved.sides[s.side], s.side, s.idx), strategy, 0, 0).reason
		if reason == "" {
			reason = answer
		}
		switch {
		case strings.Contains(answer, "holds every step of the engine and the decoder"):
			return "cheapest-step-below-the-pairable-minimum", answer
		case strings.Contains(answer, "last serving instance"), strings.Contains(answer, "final hop"), strings.Contains(answer, "no serving engine+decoder pair exists"):
			return "escape-step-below-the-pairable-minimum", answer
		case strings.Contains(answer, "no pairing protocol transition in flight"):
			return "step-below-the-pairable-minimum-outside-a-transition", answer
		}
	}
	if inPlaceOnly {
		return "in-place-patch-below-the-pairable-minimum", "an in-place patch whose returning pod the gate credits"
	}
	return "", reason
}

// replacementRemoved reports a move that removed a kept replacement: a row
// kept with its replacement in rotation before the move stands as a plain
// old Instance after it, its attempt gone (the further push's answer to a
// replacement the drain gate does not keep, or the removal move).
func (w *huntWorld) replacementRemoved(parent, moved *huntState) bool {
	for i := range parent.sides {
		for idx, r := range parent.sides[i].rows {
			if r == huntReplacementHeld && idx < len(moved.sides[i].rows) && moved.sides[i].rows[idx] == huntOld {
				return true
			}
		}
	}
	return false
}

// pairingStandstill reads a standstill the shared classifier could not
// name by the pairing gates' part in it: a side the pairing gates hold
// still (its start under the Pairing gate, or its old pods kept by the
// drain gate) beside the other side's hold names the two gates that wait
// on each other; a kept replacement on each side names the two holds the
// kept replacements wait across. A stop the operator's partition does not
// explain is read on the picture with every partition cleared, where the
// shared classifier read it.
func (w *huntWorld) pairingStandstill(st *huntState, class, message string) (string, string) {
	if class != "no-way-out" && class != "sides-wait-on-each-other" {
		return class, message
	}
	eng, dec := w.pairIndex()
	if eng < 0 || dec < 0 {
		return class, message
	}
	probe := st
	if st.partitioned() && w.cfg.canary == nil {
		probe = w.withoutPartitions(st)
	}
	kept := func(i int) bool { return probe.sides[i].count(huntReplacementHeld) > 0 }
	pairingHeld := func(i int) bool {
		h := probe.holds[i]
		return (h != nil && h.gate == v1beta1.RolloutHoldGatePairing) || kept(i)
	}
	switch e, d := pairingHeld(eng), pairingHeld(dec); {
	case kept(eng) && kept(dec):
		return "kept-replacements-wait-on-each-other-across-" + holdName(probe.holds[eng]) + "-and-" + holdName(probe.holds[dec]), message
	case e && d:
		return "pairing-gates-wait-on-each-other-across-" + holdName(probe.holds[eng]) + "-and-" + holdName(probe.holds[dec]), message
	case e:
		return "pairing-gate-waits-on-" + holdName(probe.holds[dec]), message
	case d:
		return "pairing-gate-waits-on-" + holdName(probe.holds[eng]), message
	}
	return class, message
}

// holdName names the gate behind a hold in a class name.
func holdName(h *huntHold) string {
	if h == nil {
		return "no-hold"
	}
	switch h.gate {
	case v1beta1.RolloutHoldGateRatio:
		if strings.Contains(h.reason, "outpace") || strings.Contains(h.reason, "skew") {
			return "the-ratio-rule"
		}
		return "the-batch-step"
	case v1beta1.RolloutHoldGateBudget:
		return "the-budget"
	case v1beta1.RolloutHoldGateHeld:
		return "the-held-record"
	case v1beta1.RolloutHoldGateRetryBlock:
		return "the-retry-schedule"
	case v1beta1.RolloutHoldGateSequential:
		return "the-sequential-gate"
	case v1beta1.RolloutHoldGatePlan:
		return "the-plan-gate"
	case v1beta1.RolloutHoldGatePairing:
		return "the-pairing-gate"
	}
	return strings.ToLower(string(h.gate))
}

// pairingModelConfig is one configuration of the pair at a reference
// shape, both sides at 10 percent, with the world's moves tried at every
// moment, the shares read, the pairable capacity read, and the further
// push offered once: the protocol change at the push unless the push is a
// plain one the further push changes.
func pairingModelConfig(family string, shape [2]int32, es, ds v1beta1.UpdateStrategyType, setting batchModelSetting, gang int32, repush huntRepush) huntConfig {
	cfg := batchModelConfig(family, shape, es, ds, setting, gang, repush != repushLate)
	cfg.repush, cfg.pairingReadings = repush, true
	cfg.name = family + ": " + cfg.String() + " " + setting.name
	return cfg
}

// pairingModelCanaryConfig rolls the pair as a canary unit through the
// given steps with the protocol change at the push; a canary unit is not
// offered a further push.
func pairingModelCanaryConfig(shape [2]int32, es, ds v1beta1.UpdateStrategyType, steps []string, gang int32) huntConfig {
	cfg := batchModelCanaryConfig(shape, es, ds, steps, gang, true)
	cfg.pairingReadings = true
	return cfg
}

// withRouter adds a router of two Instances rolling by the given strategy
// to the group.
func withRouterMember(cfg huntConfig, strategy v1beta1.UpdateStrategyType) huntConfig {
	family, _, _ := strings.Cut(cfg.name, ": ")
	cfg.members = append(cfg.members, huntMember{comp: v1beta1.RouterComponent, desired: 2, strategy: strategy, budget: "10%"})
	cfg.name = family + ": " + cfg.String() + " router"
	return cfg
}

// withFollower starts the roll under a proportional scaling policy sizing
// the follower from the other side, with one edit of the policy among the
// moves.
func withFollower(cfg huntConfig, follower v1beta1.ComponentType) huntConfig {
	cfg.follower, cfg.policyEdits = follower, 1
	cfg.name += fmt.Sprintf(" follower=%s policyEdits=1", follower)
	return cfg
}

// withEdits adds one edit of the rollout block among the moves: the rule
// added, moved or removed, the group's budget lowered, the group removed,
// the router added or the decoder removed.
func withEdits(cfg huntConfig) huntConfig {
	cfg.edits = 1
	cfg.name += " edits=1"
	return cfg
}

// pairingPick is one configuration of the model before it is built: the
// shape, the two strategies, the setting, the gang size, the further push,
// and the extras beside the pair (a router, a scaling policy with one
// edit, one edit of the rollout block, a surging side held to the given
// maxUnavailable).
type pairingPick struct {
	family      string
	shape       [2]int32
	es, ds      v1beta1.UpdateStrategyType
	setting     batchModelSetting
	gang        int32
	repush      huntRepush
	router      v1beta1.UpdateStrategyType
	follower    v1beta1.ComponentType
	edits       bool
	minimumSide v1beta1.ComponentType
	minimumAt   string
}

// pairingModelSettings are the settings explored: none; the chart's
// (keepRatio at 5, the decoder first); keepRatio alone; the decoder
// first alone; the ratio rule at the wide tolerance; the ratio rule
// beside the chart's setting.
func pairingModelSettings() (off, chart, keep, first, ratio, ratioChart batchModelSetting) {
	settings := batchModelSettings()
	return settings[0], settings[2], settings[3], settings[4], settings[1], settings[5]
}

// pairingModelBoundedPicks are the configurations CI explores: the
// smallest reference shape under both drain-first and both surging at
// two settings, 20:10 and 12:4 under one setting each and 30:10 once,
// with the second change offered at each seed moment, in the walk and in
// the random runs; the mixed and in-place
// pairs, the change back, the change on a plain roll, gangs of two to
// eight, a router, a scaling policy, a rollout-block edit, the ratio rule,
// and a side with none or one Instance allowed out of pairing. The 15:15
// shape is covered by the change back, the late change, a gang and a
// minimum pick; full mode samples every shape under every setting.
func pairingModelBoundedPicks() []pairingPick {
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	off, chart, keep, first, ratio, ratioChart := pairingModelSettings()
	type pick = pairingPick
	var picks []pick
	for _, pair := range [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}} {
		for _, setting := range []batchModelSetting{off, chart} {
			picks = append(picks, pick{family: "second-change", shape: [2]int32{12, 8}, es: pair[0], ds: pair[1], setting: setting, repush: repushSecondChange})
		}
	}
	for _, shape := range [][2]int32{{20, 10}, {12, 4}} {
		picks = append(picks,
			pick{family: "second-change", shape: shape, es: recreate, ds: recreate, setting: off, repush: repushSecondChange},
			pick{family: "second-change", shape: shape, es: surge, ds: surge, setting: chart, repush: repushSecondChange})
	}
	picks = append(picks, pick{family: "second-change", shape: [2]int32{30, 10}, es: recreate, ds: recreate, setting: off, repush: repushSecondChange})
	picks = append(picks,
		pick{family: "second-change", shape: [2]int32{12, 8}, es: recreate, ds: surge, setting: chart, repush: repushSecondChange},
		pick{family: "second-change", shape: [2]int32{16, 8}, es: surge, ds: recreate, setting: off, repush: repushSecondChange},
		pick{family: "second-change", shape: [2]int32{24, 8}, es: recreate, ds: surge, setting: keep, repush: repushSecondChange},
		pick{family: "second-change", shape: [2]int32{12, 4}, es: surge, ds: recreate, setting: first, repush: repushSecondChange},
		pick{family: "back", shape: [2]int32{12, 8}, es: surge, ds: surge, setting: chart, repush: repushBack},
		pick{family: "back", shape: [2]int32{16, 8}, es: recreate, ds: recreate, setting: off, repush: repushBack},
		pick{family: "back", shape: [2]int32{15, 15}, es: recreate, ds: surge, setting: chart, repush: repushBack},
		pick{family: "late-change", shape: [2]int32{12, 8}, es: recreate, ds: recreate, setting: chart, repush: repushLate},
		pick{family: "late-change", shape: [2]int32{24, 8}, es: surge, ds: surge, setting: off, repush: repushLate},
		pick{family: "late-change", shape: [2]int32{15, 15}, es: recreate, ds: surge, setting: chart, repush: repushLate},
		pick{family: "late-change", shape: [2]int32{20, 10}, es: surge, ds: surge, setting: first, repush: repushLate},
		pick{family: "in-place", shape: [2]int32{12, 4}, es: inPlace, ds: surge, setting: chart, repush: repushSecondChange},
		pick{family: "in-place", shape: [2]int32{12, 8}, es: surge, ds: inPlace, setting: off, repush: repushSecondChange},
		pick{family: "in-place", shape: [2]int32{12, 8}, es: inPlace, ds: inPlace, setting: keep, repush: repushLate},
		pick{family: "gangs", shape: [2]int32{12, 8}, es: surge, ds: surge, setting: chart, gang: 2, repush: repushSecondChange},
		pick{family: "gangs", shape: [2]int32{12, 4}, es: recreate, ds: recreate, setting: off, gang: 4, repush: repushSecondChange},
		pick{family: "gangs", shape: [2]int32{15, 15}, es: surge, ds: recreate, setting: chart, gang: 8, repush: repushLate},
		pick{family: "router", shape: [2]int32{12, 8}, es: surge, ds: surge, setting: chart, repush: repushSecondChange, router: surge},
		pick{family: "router", shape: [2]int32{16, 8}, es: recreate, ds: recreate, setting: off, repush: repushLate, router: recreate},
		pick{family: "policy", shape: [2]int32{24, 8}, es: recreate, ds: recreate, setting: chart, repush: repushSecondChange, follower: dec},
		pick{family: "policy", shape: [2]int32{12, 8}, es: surge, ds: surge, setting: ratio, repush: repushLate, follower: dec},
		pick{family: "edits", shape: [2]int32{12, 8}, es: surge, ds: surge, setting: chart, repush: repushSecondChange, edits: true},
		pick{family: "edits", shape: [2]int32{16, 8}, es: recreate, ds: recreate, setting: off, repush: repushBack, edits: true},
		pick{family: "ratio", shape: [2]int32{16, 8}, es: surge, ds: surge, setting: ratio, repush: repushSecondChange},
		pick{family: "ratio", shape: [2]int32{12, 8}, es: recreate, ds: recreate, setting: ratioChart, repush: repushLate},
		pick{family: "minimum", shape: [2]int32{12, 8}, es: recreate, ds: surge, setting: chart, repush: repushSecondChange, minimumSide: dec, minimumAt: "0"},
		pick{family: "minimum", shape: [2]int32{15, 15}, es: surge, ds: recreate, setting: first, repush: repushLate, minimumSide: eng, minimumAt: "1"},
		pick{family: "minimum", shape: [2]int32{12, 4}, es: surge, ds: surge, setting: off, repush: repushSecondChange, minimumSide: dec, minimumAt: "0"},
	)
	return picks
}

// pairingModelConfigs lists the configurations: the bounded picks, in
// full mode the seeded sample over the product of the shapes, pairs,
// settings and pushes too, and the pair as a canary unit with the
// protocol change at the push, at two shapes bounded and every shape and
// step list in full mode.
func pairingModelConfigs(full bool) []huntConfig {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	picks := pairingModelBoundedPicks()
	if full {
		picks = append(picks, pairingModelFullPicks()...)
	}
	var out []huntConfig
	for _, p := range picks {
		cfg := pairingModelConfig(p.family, p.shape, p.es, p.ds, p.setting, p.gang, p.repush)
		if p.router != "" {
			cfg = withRouterMember(cfg, p.router)
		}
		if p.follower != "" {
			cfg = withFollower(cfg, p.follower)
		}
		if p.edits {
			cfg = withEdits(cfg)
		}
		if p.minimumSide != "" {
			cfg = outOfPairingOf(cfg, p.minimumSide, p.minimumAt)
			cfg.name = p.family + ": " + cfg.name + " " + p.setting.name
		}
		out = append(out, cfg)
	}
	canaryShapes := [][2]int32{{12, 8}, {20, 10}}
	stepLists := [][]string{{"25%", "100%"}}
	if full {
		canaryShapes = batchModelShapes
		stepLists = append(stepLists, []string{"10%", "50%", "100%"})
	}
	for _, shape := range canaryShapes {
		for _, pair := range [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}} {
			for _, steps := range stepLists {
				out = append(out, pairingModelCanaryConfig(shape, pair[0], pair[1], steps, 0))
			}
		}
	}
	return out
}

// pairingModelFullPicks is the seeded sample full mode adds: the product
// of the reference shapes, the seven strategy pairs, the six settings,
// the three pushes, gangs of one to eight pods, a router beside one in
// four, a scaling policy beside one in six, an edit beside one in six and
// a side held to none or one Instance out of pairing beside one in five.
func pairingModelFullPicks() []pairingPick {
	type pick = pairingPick
	recreate, surge, inPlace := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible
	pairs := [][2]v1beta1.UpdateStrategyType{{recreate, recreate}, {surge, surge}, {recreate, surge}, {surge, recreate}, {inPlace, surge}, {surge, inPlace}, {inPlace, recreate}}
	settings := batchModelSettings()
	pushes := []huntRepush{repushSecondChange, repushBack, repushLate}
	rng := rand.New(rand.NewSource(20261110))
	var out []pick
	for n := 0; n < 300; n++ {
		p := pick{family: "sample", shape: batchModelShapes[rng.Intn(len(batchModelShapes))], setting: settings[rng.Intn(len(settings))], repush: pushes[rng.Intn(len(pushes))]}
		pair := pairs[rng.Intn(len(pairs))]
		p.es, p.ds = pair[0], pair[1]
		p.gang = []int32{0, 0, 0, 2, 4, 8}[rng.Intn(6)]
		if rng.Intn(4) == 0 {
			p.router = []v1beta1.UpdateStrategyType{recreate, surge}[rng.Intn(2)]
		}
		if rng.Intn(6) == 0 {
			p.follower = pairingComponents[rng.Intn(2)]
		}
		p.edits = rng.Intn(6) == 0
		if rng.Intn(5) == 0 {
			if p.es == surge {
				p.minimumSide, p.minimumAt = v1beta1.EngineComponent, []string{"0", "1"}[rng.Intn(2)]
			} else if p.ds == surge {
				p.minimumSide, p.minimumAt = v1beta1.DecoderComponent, []string{"0", "1"}[rng.Intn(2)]
			}
		}
		out = append(out, p)
	}
	return out
}

// pairingModelFailing are the classes that name a bug: the batch-step
// model's, the pairable capacity lost to a step the gates admitted on their
// ordinary rules, the last pairable pair lost, the pairing gates waiting on
// each other or on another gate, and the lockstep waiting on a peer the
// pairing gates hold still, which the lockstep waives.
var pairingModelFailing = map[string]bool{
	"no-way-out": true, "budget": true, "ratio-excess": true, "classifier": true, "batch-promise": true, "ratio-promise": true,
	"starts-before-the-named-side": true, "stranded-replacement": true,
	"waits-on-held-instances": true, "read": true, "depth": true, "sides-wait-on-each-other": true,
	"step-below-the-pairable-minimum": true, "step-left-no-pairable-pair": true,
	"drain-below-the-pairable-minimum": true, "drain-left-no-pairable-pair": true,
	"pairing-gates-wait-on-each-other": true, "ratio-rule-waits-on-the-pairing-gate": true,
}

// pairingModelKnown are the classes that record a known reading of today's
// engine, each with what the operator sees and the test or decision that
// holds it. A finding of any other class fails as a reading to classify;
// the pairing-gate-waits-on-<gate> classes among them.
func pairingModelKnown() map[string]string {
	known := map[string]string{
		"cheapest-step-below-the-pairable-minimum":                                    "when the pairable minimum holds every step of both sides the start-time gate admits the one step that costs the fewest pairable Instances, by design (the start-time gate's rule for a picture in which every step of both sides is held, pairing.go)",
		"escape-step-below-the-pairable-minimum":                                      "both sides down to one serving Instance with no new-cohort capacity, or the last Instance of a side beside a peer already on the new cohort, is admitted rather than held for good, by design (mutualPairingWall, lastHopEscape)",
		"in-place-patch-below-the-pairable-minimum":                                   "an in-place patch takes its pod out of rotation and returns it on the new cohort; the gate credits the return when the other side already serves that cohort, so the patch's own unavailability is the budget's to bound, not the minimum's (CheckPairing credits an in-place start); a side scaled up reads below its minimum until its created Instances serve, and a patch beside that costs its own pod alone",
		"rebuild-with-no-gate-asked-below-the-pairable-minimum":                       "a gang short of a worker still serves through its leader, and the roll rebuilds it onto the target with no gate asked (plan_decision.go, skipsCoordinationGate), so the rebuild costs a pairable Instance the gate would have held (the batch-step model's rebuild classes)",
		"step-below-the-pairable-minimum-outside-a-transition":                        "the gates are inert while no serving Instance they count runs a protocol other than the target's; an Instance promoted onto another protocol whose pods are not yet counted is not serving to them, so a push back lands a batch of starts on the old cohort one status write early",
		"same-cohort-step-below-the-pairable-minimum":                                 "a gap: while a transition is in flight, a drain-first step of an Instance that already runs the target protocol (after a push back to a protocol the fleet still serves) is charged to no cohort, since the start-time gate simulates losses from the old cohorts only; the unavailability budget alone paces it, so it takes its side below the pairable minimum or removes the last partner of the other side's cohort, which the same step was held for before the push back (TestCheckPairing_APushBackChargesNoStepOfTheTargetCohort holds the current answer)",
		"kept-replacements-wait-on-each-other-across-the-batch-step-and-the-budget":   "a gap: with three cohorts in flight each side's kept replacement waits on the other side's: the drain gate keeps an engine's old pod for the decoder's last Instance of the first cohort, the engine's batch step waits for that surge to settle before its next batch (a member's next batch waits for its own surges), the decoder's kept replacement waits for the engine Instances that batch holds and spends the decoder's one surge slot, so the decoder's Instance of the first cohort never starts (TestPairingGates_KeptReplacementsWaitOnEachOther holds the current picture)",
		"kept-replacements-wait-on-each-other-across-the-budget-and-the-batch-step":   "a gap: the same ring with the sides swapped: the engine's kept replacement spends its one surge slot and is kept for the decoder's Instances on the middle protocol, the decoder's kept replacement holds its own next batch and is kept for the engine's Instances on the first protocol; reached on the reference 12:8 shape, both surging under the chart's setting, after a scale of each side and a partition (TestPairingGates_KeptReplacementsWaitOnEachOtherOnTheReferenceShape holds the current picture)",
		"pairing-gate-waits-on-the-batch-step":                                        "a gap: with a router in the group the ring runs through three members: the decoder's kept replacement is kept for the engine's Instances on the middle protocol, the engine's next batch waits for the router's batch, and the router's batch waits for the decoder's batch, which the kept replacement leaves unsettled, since the batch step waives a kept replacement for the pairing peer only, not for a third member (TestPairingGates_AKeptReplacementWaitsOnTheRouterThroughTheBatchStep holds the current picture)",
		"pairing-gates-wait-on-each-other-across-the-batch-step-and-the-pairing-gate": "a gap: the two pairing gates hold each other across the surge budget: the engine's replacement on the last protocol is kept because no decoder serves it (the drain gate's first rule), the kept replacement spends the engine's one surge slot and holds its next batch, and every decoder step is held on the engine's pairable minimum since a decoder scaled down by its policy to two Instances partners half the engines; the cheapest-step rule sees an engine step the budget cannot open (TestPairingGates_ThePairingGatesHoldEachOtherAcrossTheSurgeBudget holds the current picture)",
		"replacement-removed-below-the-pairable-minimum":                              "a gap: the drain gate's answer to a replacement leaving (the push moved the target, the attempt would start again) reads the other side's pairable minimum but not the leaving side's own, so a source whose protocol has no partner left stays alone and its side falls below its minimum (TestEvaluatePairingDrain_AReplacementRemovedAfterAPushBackIsNotChargedToItsOwnSide holds the current answer)",
		"step-below-the-pairable-minimum-behind-the-counters":                         "the start-time gate reads the counters of the last status write, which do not yet count a promoted Instance's serving pods, and the world lets several promoted Instances stand uncounted at once; on the Instances the gate counts the step costs nothing, by the pods it strands the uncounted ones for the length of the step (the same one-write lag the batch-step model reads)",
		"runs-ahead-of-a-peer-coming-up":                                              "the batch-step model's reading of a side opening a start while the other side's promoted Instances are not yet counted as serving, reached here with no push: after an edit of the rule's tolerance, and beside a side Held on a spent schedule with a gang member lost; the batch-step model owns it",
		"waits-on-a-lost-node-without-force-delete":                                   "a pod on a node that stopped reporting stands until an operator removes it when no force-delete policy is configured",
		"budget-after-edit":                 "attempts open above a group budget an edit lowered finish before anything more opens",
		"batch-promise-after-edit":          "an edit of the rollout block re-plans the batches under the roll; the picture inherited from the old plan may sit outside the new one until the side behind catches up",
		"ratio-promise-after-edit":          "an edit of the rollout block re-plans the planned batches under the roll",
		"version-mix-from-a-follower-scale": "the coordinator's follower write is a scale the operator did not make, one service pass after the anchor's (TestBatchStepEvents_AFollowersCountLandsOneServicePassAfterTheAnchors)",
	}
	for class, reading := range batchModelKnown {
		if !pairingModelFailing[class] {
			known[class] = reading
		}
	}
	return known
}

// pairingModelRunner reads the pairing gate model's classes.
func pairingModelRunner() huntModelRunner {
	return huntModelRunner{fullEnv: "OME_PAIRING_HUNT", onlyEnv: "OME_PAIRING_HUNT_ONLY", failing: pairingModelFailing, known: pairingModelKnown()}
}

// TestPairingGates_EveryProtocolChangeOfTheReferenceShapesEnds drives the
// gate stack over the reference prefill/decode shapes during a
// pairing-protocol change with a further revision pushed at seven seed
// moments of a plain roll, in a 30-state walk and in two random runs,
// beside the world's partitions, scales, policy and rule
// edits, gang losses, failures, spent schedules and canary steps: no two
// gates wait on each other, no budget is exceeded, no step the gates admit
// on their ordinary rules takes a side below its pairable minimum or
// leaves the fleet without a pairable pair, and every roll ends.
func TestPairingGates_EveryProtocolChangeOfTheReferenceShapesEnds(t *testing.T) {
	runner := pairingModelRunner()
	runner.explore(t, pairingModelConfigs(runner.full()))
}

// huntPathMoves reads a path the explorer printed back into its moves: a
// start that serves, fails or is released, a schedule spent or backing
// off, a partition set or a scale on one side or both, a scaling policy
// set, a further revision pushed.
func huntPathMoves(path string) []huntMove {
	var out []huntMove
	for _, text := range strings.Split(path, " -> ") {
		var mv huntMove
		switch {
		case text == "push a further revision":
			mv.kind = "repush"
		case strings.HasPrefix(text, "partition side "):
			mv.kind = "partition"
			n, _ := fmt.Sscanf(text, "partition side %d to %d and side %d to %d", &mv.side, &mv.delta, &mv.side2, &mv.delta2)
			mv.both = n == 4
		case strings.HasPrefix(text, "scale side "):
			mv.kind = "scale"
			n, _ := fmt.Sscanf(text, "scale side %d by %d and side %d by %d", &mv.side, &mv.delta, &mv.side2, &mv.delta2)
			mv.both = n == 4
		case strings.HasPrefix(text, "policy: "):
			mv.kind = "policy"
			var follower, anchor, ratio string
			fmt.Sscanf(text, "policy: %s follows %s at %s", &follower, &anchor, &ratio)
			whole, frac, _ := strings.Cut(ratio, ".")
			var w, f int64
			fmt.Sscanf(whole, "%d", &w)
			fmt.Sscanf(frac, "%d", &f)
			mv.policy = huntPolicy{follower: v1beta1.ComponentType(follower), milli: w*1000 + f}
		default:
			fmt.Sscanf(text, "%s side %d row %d", &mv.kind, &mv.side, &mv.idx)
		}
		out = append(out, mv)
	}
	return out
}

// TestCheckPairing_APushBackChargesNoStepOfTheTargetCohort is the
// start-time gate on a protocol change pushed back while half done: 20
// engines and 10 decoders, both RecreatePod, the engine two out at a time
// and the decoder one. Nineteen engines and six decoders run pair-v2; one
// engine and four decoders still run pair-v1, and that engine is the four
// decoders' only partner. Toward pair-v2 the gate holds the engine's step:
// the decoder would keep 6 pairable Instances, under its minimum of 9. The
// operator pushes back to pair-v1. The same step now moves an Instance
// already on the target protocol, and the gate charges it to no cohort: it
// is admitted with no reason, and the four decoders have no partner for
// the length of the recreate. The test holds that answer, so a gate that
// charges the step is seen: it flips the second half.
func TestCheckPairing_APushBackChargesNoStepOfTheTargetCohort(t *testing.T) {
	hashes := func(hash string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = hash
		}
		return out
	}
	engines := append(hashes("v2", 19), "v1")
	decoders := append(hashes("v2", 6), hashes("v1", 4)...)
	picture := func(target string) []client.Object {
		return append(threeCohortCRs(target),
			rolledPairingIR(v1beta1.EngineComponent, v1beta1.UpdateStrategyRecreatePod, 20, 2, engines...),
			rolledPairingIR(v1beta1.DecoderComponent, v1beta1.UpdateStrategyRecreatePod, 10, 1, decoders...))
	}
	if left := pairableCohortCount(map[string]int32{"pair-v2": 6, "pair-v1": 4}, map[string]int32{"pair-v2": 19}); left != 6 {
		t.Fatalf("with the last pair-v1 engine out the decoder keeps %d pairable Instances by the gate's own count, want 6", left)
	}
	ok, reason := askPairing(t, pairingISVC("pair-v2"), picture("pair-v2"), v1beta1.EngineComponent, "v1")
	if ok || !strings.Contains(reason, "decoder with 6 pairable") || !strings.Contains(reason, "of 9 (desired 10 minus maxUnavailable 1)") {
		t.Fatalf("toward pair-v2 the last pair-v1 engine's step must be held for the four decoders it partners, got (%v, %q)", ok, reason)
	}
	ok, reason = askPairing(t, pairingISVC("pair-v1"), picture("pair-v1"), v1beta1.EngineComponent, "v1")
	if !ok || reason != "" {
		t.Fatalf("the gate admits a step of the target cohort with no reason, charging it to no cohort; got (%v, %q). A gate that charges it holds with the decoder's shortfall of 6 under 9, and this lock then flips", ok, reason)
	}
	allowed, gate, reason := askStack(t, pairingISVC("pair-v1"), picture("pair-v1"), v1beta1.EngineComponent, "v1")
	if !allowed {
		t.Fatalf("the whole stack admits the step today; held by %s: %s", gate, reason)
	}
}

// TestPairingGates_KeptReplacementsWaitOnEachOther replays the one
// standstill the exploration found that no single gate explains: 12
// engines and 4 decoders, both SurgeThenDrain at 10 percent (one surge slot
// each), the decoder allowed no Instance out of pairing, a second protocol
// change pushed while the first is open. The operator scales, partitions
// and pushes along the way; at the end the engine has 11 Instances, 7 on
// the middle protocol, 3 on the last, and one surge whose old pod is the
// only partner of the decoder's one Instance still on the first protocol,
// so the drain gate keeps it. The engine's next batch waits for that surge
// to settle. The decoder's one surge slot is spent by its own kept
// replacement, kept for the 7 engines on the middle protocol, so its
// first-protocol Instance never starts and nothing moves, with every
// partition cleared too. The test holds that picture, so a change of the
// batch step's or the budget's reading of a kept replacement is seen.
func TestPairingGates_KeptReplacementsWaitOnEachOther(t *testing.T) {
	surge := v1beta1.UpdateStrategySurgeThenDrain
	off, _, _, _, _, _ := pairingModelSettings()
	cfg := outOfPairingOf(pairingModelConfig("minimum", [2]int32{12, 4}, surge, surge, off, 0, repushSecondChange), v1beta1.DecoderComponent, "0")
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	path := "serve side 0 row 0 -> serve side 1 row 0 -> serve side 0 row 1 -> release side 0 row 0 -> partition side 0 to 6 -> serve side 0 row 3 -> serve side 1 row 1 -> scale side 1 by +2 -> serve side 1 row 5 -> serve side 0 row 2 -> serve side 0 row 10 -> partition side 1 to 6 -> serve side 1 row 4 -> serve side 0 row 11 -> scale side 0 by -1 -> partition side 0 to 3 -> serve side 0 row 7 -> serve side 0 row 9 -> serve side 0 row 8 -> push a further revision -> serve side 0 row 3 -> serve side 0 row 4 -> partition side 1 to 3 -> serve side 1 row 3 -> release side 0 row 3 -> release side 0 row 4 -> serve side 0 row 6 -> serve side 1 row 4 -> serve side 0 row 5 -> serve side 1 row 5"
	for _, mv := range huntPathMoves(path) {
		st = w.apply(st, mv)
	}
	const class = "kept-replacements-wait-on-each-other-across-the-batch-step-and-the-budget"
	var classes []string
	found := false
	for _, f := range w.check(st) {
		classes = append(classes, f.class)
		found = found || f.class == class
	}
	if !found {
		t.Fatalf("the replayed path must end in the standstill this lock holds (%s); read %v at %s", class, classes, st)
	}
	eng, dec := w.pairIndex()
	cleared := w.withoutPartitions(st)
	if cleared.complete() {
		t.Fatalf("with every partition cleared the roll must still stand, today; it ended: %s", cleared)
	}
	if cleared.sides[eng].count(huntReplacementHeld) == 0 || cleared.sides[dec].count(huntReplacementHeld) == 0 {
		t.Fatalf("each side keeps a replacement whose old pod the drain gate holds for the other side: %s", cleared)
	}
	if h := cleared.holds[eng]; h == nil || h.gate != v1beta1.RolloutHoldGateRatio || !strings.Contains(h.reason, "waits for engine batch") {
		t.Fatalf("the engine's next batch waits for its own kept replacement to settle, got %s", h)
	}
	if h := cleared.holds[dec]; h == nil || h.gate != v1beta1.RolloutHoldGateBudget || !strings.Contains(h.reason, "surge budget") {
		t.Fatalf("the decoder's surge slot is spent by its own kept replacement, got %s", h)
	}
}

// huntReplay plays a path the explorer printed from the first admission
// and returns the state it ends in.
func huntReplay(t *testing.T, cfg huntConfig, path string) (*huntWorld, *huntState) {
	t.Helper()
	w := newHuntWorld(t, cfg)
	st := w.initial()
	w.settle(st, w.order(0))
	for _, mv := range huntPathMoves(path) {
		if mv.kind == "follower-pass" {
			// The coordinator's write carries the count the policy owes at
			// that moment.
			_, mv.delta, _ = w.followerOwed(st)
		}
		st = w.apply(st, mv)
	}
	return w, st
}

// requireStandstill checks that the state reads as the given class and
// that the roll still stands with every partition cleared, and returns the
// cleared picture.
func requireStandstill(t *testing.T, w *huntWorld, st *huntState, class string) *huntState {
	t.Helper()
	var classes []string
	found := false
	for _, f := range w.check(st) {
		classes = append(classes, f.class)
		found = found || f.class == class
	}
	if !found {
		t.Fatalf("the replayed path must end in the standstill this lock holds (%s); read %v at %s", class, classes, st)
	}
	cleared := w.withoutPartitions(st)
	if cleared.complete() {
		t.Fatalf("with every partition cleared the roll must still stand, today; it ended: %s", cleared)
	}
	return cleared
}

// requireHold checks a side's hold on the cleared picture: its gate and a
// phrase of its text.
func requireHold(t *testing.T, st *huntState, side int, gate v1beta1.RolloutHoldGate, phrase, why string) {
	t.Helper()
	if h := st.holds[side]; h == nil || h.gate != gate || !strings.Contains(h.reason, phrase) {
		t.Fatalf("%s: want a %s hold saying %q, got %s", why, gate, phrase, h)
	}
}

// TestPairingGates_KeptReplacementsWaitOnEachOtherOnTheReferenceShape is
// the same ring with the sides swapped, on 12 prefill and 8 decode
// both SurgeThenDrain at 10 percent under the chart's setting (keepRatio at
// 5, the decoder first). The operator scales the engine to 6 and the
// decoder to 12 and partitions the engine; the decoder's fifth surge fails
// and its schedule is spent; the second protocol change lands. At the end
// the engine's one surge slot is spent by a replacement kept for the
// decoder's Instances on the middle protocol, and the decoder's next batch
// waits for its own kept replacement, kept for the engine's Instances on
// the first protocol. Nothing moves, with every partition cleared too.
func TestPairingGates_KeptReplacementsWaitOnEachOtherOnTheReferenceShape(t *testing.T) {
	surge := v1beta1.UpdateStrategySurgeThenDrain
	_, chart, _, _, _, _ := pairingModelSettings()
	cfg := pairingModelConfig("second-change", [2]int32{12, 8}, surge, surge, chart, 0, repushSecondChange)
	path := "scale side 0 by -6 and side 1 by +4 -> serve side 1 row 11 -> serve side 1 row 8 -> serve side 1 row 9 -> serve side 1 row 10 -> serve side 0 row 0 -> serve side 0 row 1 -> serve side 1 row 0 -> serve side 1 row 1 -> serve side 0 row 2 -> serve side 1 row 2 -> partition side 0 to 6 -> serve side 1 row 3 -> serve side 1 row 4 -> serve side 1 row 6 -> fail side 1 row 5 -> spent side 1 -> push a further revision -> fail side 1 row 0 -> backoff side 1 -> serve side 1 row 1"
	w, st := huntReplay(t, cfg, path)
	cleared := requireStandstill(t, w, st, "kept-replacements-wait-on-each-other-across-the-budget-and-the-batch-step")
	eng, dec := w.pairIndex()
	if cleared.sides[eng].count(huntReplacementHeld) == 0 || cleared.sides[dec].count(huntReplacementHeld) == 0 {
		t.Fatalf("each side keeps a replacement whose old pod the drain gate holds for the other side: %s", cleared)
	}
	requireHold(t, cleared, eng, v1beta1.RolloutHoldGateBudget, "surge budget", "the engine's surge slot is spent by its own kept replacement")
	requireHold(t, cleared, dec, v1beta1.RolloutHoldGateRatio, "waits for decoder batch", "the decoder's next batch waits for its own kept replacement to settle")
}

// TestPairingGates_AKeptReplacementWaitsOnTheRouterThroughTheBatchStep is
// the ring through a third member: 12 prefill rebuilt one at a time, 4
// decode and a router of 2 surging, a second protocol change. After the
// operator's scales and partitions the decoder's kept replacement waits for
// the engine's Instances on the middle protocol, the engine's next batch
// waits for the router's batch, and the router's batch waits for the
// decoder's, which the kept replacement leaves unsettled: the batch step
// waives a kept replacement for the pairing peer, not for the router.
func TestPairingGates_AKeptReplacementWaitsOnTheRouterThroughTheBatchStep(t *testing.T) {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	off, _, _, _, _, _ := pairingModelSettings()
	cfg := withRouterMember(pairingModelConfig("router", [2]int32{12, 4}, recreate, surge, off, 0, repushSecondChange), surge)
	path := "serve side 2 row 0 -> serve side 1 row 0 -> partition side 0 to 6 -> serve side 0 row 0 -> serve side 0 row 1 -> serve side 0 row 1 -> release side 1 row 0 -> serve side 2 row 1 -> serve side 1 row 1 -> serve side 0 row 8 -> serve side 0 row 0 -> scale side 0 by -6 and side 1 by -2 -> partition side 1 to 1 -> scale side 2 by +1 -> serve side 2 row 2 -> partition side 0 to 3 -> serve side 0 row 5 -> serve side 0 row 5 -> partition side 1 to 2 -> partition side 2 to 2 -> partition side 2 to 3 -> push a further revision -> serve side 0 row 3 -> serve side 0 row 4 -> serve side 0 row 4 -> serve side 0 row 3"
	w, st := huntReplay(t, cfg, path)
	cleared := requireStandstill(t, w, st, "pairing-gate-waits-on-the-batch-step")
	eng, dec := w.pairIndex()
	if cleared.sides[dec].count(huntReplacementHeld) == 0 {
		t.Fatalf("the decoder keeps a replacement whose old pod the drain gate holds for the engine: %s", cleared)
	}
	requireHold(t, cleared, eng, v1beta1.RolloutHoldGateRatio, "waits for router batch", "the engine's next batch waits for the router's")
	requireHold(t, cleared, 2, v1beta1.RolloutHoldGateRatio, "waits for decoder batch", "the router's batch waits for the decoder's, which its kept replacement leaves unsettled")
}

// TestPairingGates_ThePairingGatesHoldEachOtherAcrossTheSurgeBudget is the
// two pairing gates holding each other: 12 prefill surging one at a time
// and 4 decode rebuilt one at a time under keepRatio, the decoder sized by
// a scaling policy the operator sets to a sixth of the engine, so two
// decoders partner twelve engines; a second protocol change lands. The
// engine's replacement on the last protocol is kept because no decoder
// serves it, the kept replacement spends the engine's one surge slot and
// holds its next batch, and every decoder step is held on the engine's
// pairable minimum of 9, since each decoder is the only partner of six
// engines. The cheapest-step rule sees an engine step the budget cannot
// open, so it admits nothing.
func TestPairingGates_ThePairingGatesHoldEachOtherAcrossTheSurgeBudget(t *testing.T) {
	recreate, surge := v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain
	_, _, keep, _, _, _ := pairingModelSettings()
	cfg := withFollower(pairingModelConfig("policy", [2]int32{12, 4}, surge, recreate, keep, 0, repushSecondChange), v1beta1.DecoderComponent)
	path := "serve side 0 row 0 -> serve side 1 row 0 -> serve side 0 row 1 -> release side 0 row 0 -> policy: decoder follows engine at 0.166 -> serve side 1 row 0 -> follower-pass side 1 -> partition side 0 to 6 -> serve side 0 row 3 -> serve side 0 row 2 -> serve side 0 row 10 -> partition side 1 to 2 -> serve side 0 row 11 -> partition side 0 to 12 -> partition side 1 to 0 -> push a further revision"
	w, st := huntReplay(t, cfg, path)
	cleared := requireStandstill(t, w, st, "pairing-gates-wait-on-each-other-across-the-batch-step-and-the-pairing-gate")
	eng, dec := w.pairIndex()
	if cleared.sides[eng].count(huntReplacementHeld) == 0 || len(cleared.sides[dec].rows) != 2 {
		t.Fatalf("the engine keeps a replacement no decoder pairs with, beside a decoder of two Instances: %s", cleared)
	}
	requireHold(t, cleared, eng, v1beta1.RolloutHoldGateRatio, "waits for engine batch", "the engine's next batch waits for its own kept replacement to settle")
	requireHold(t, cleared, dec, v1beta1.RolloutHoldGatePairing, "of 9 (desired 12 minus maxUnavailable 3)", "every decoder step is held on the engine's pairable minimum")
}

// TestEvaluatePairingDrain_AReplacementRemovedAfterAPushBackIsNotChargedToItsOwnSide
// is the drain gate's answer to a replacement leaving after the target
// moved: 12 engines surging with three allowed out of pairing and 4
// decoders, the protocol changed from pair-v1 to pair-v2 and pushed back
// while the engine's first Instance keeps both its pods in rotation (the
// v1 source and the v2 replacement), eight engines run v2, three run v1,
// and every decoder runs v2. The push back asks whether the v2
// replacement may leave so the surge can start again. Today the gate reads
// the decoders' minimum, which the replacement's leaving does not touch,
// and says yes, though the source it leaves behind runs v1 with no partner
// and the engine keeps 8 pairable Instances of its minimum of 9 by the
// gate's own count. The test holds that answer, so a gate that reads the
// leaving side's own minimum is seen: it flips the test.
func TestEvaluatePairingDrain_AReplacementRemovedAfterAPushBackIsNotChargedToItsOwnSide(t *testing.T) {
	eng, dec := v1beta1.EngineComponent, v1beta1.DecoderComponent
	isvc := pairingISVC("v1")
	engines := sizedReplica(isvc, eng, v1beta1.UpdateStrategySurgeThenDrain, 12, 3)
	rows := []v1beta1.OMENativeInstanceStatus{surgingRow(isvc.Name, eng, 0, "v1", "v2", workloadtypes.UpdateStepSurge, 2)}
	for i := 1; i <= 8; i++ {
		rows = append(rows, settledRow(isvc.Name, eng, i, "v2"))
	}
	for i := 9; i <= 11; i++ {
		rows = append(rows, settledRow(isvc.Name, eng, i, "v1"))
	}
	var decoderRows []v1beta1.OMENativeInstanceStatus
	for i := 0; i < 4; i++ {
		decoderRows = append(decoderRows, settledRow(isvc.Name, dec, i, "v2"))
	}
	objs := []client.Object{withRows(engines, rows...), withRows(sizedReplica(isvc, dec, v1beta1.UpdateStrategyInPlaceIfPossible, 4, 1), decoderRows...)}
	for _, comp := range pairingComponents {
		for _, proto := range []string{"v1", "v2"} {
			objs = append(objs, pairingCR(isvc.Namespace, isvc.Name, comp, proto, proto))
		}
	}
	objs = append(objs, fleetPods(eng, 0, 1, 0, "v1", true)...)
	objs = append(objs, fleetPods(eng, 0, 1, 1, "v2", true)...)
	objs = append(objs, fleetPods(eng, 1, 8, 0, "v2", true)...)
	objs = append(objs, fleetPods(eng, 9, 3, 0, "v1", true)...)
	objs = append(objs, fleetPods(dec, 0, 4, 0, "v2", true)...)

	minimum := resolvePairFloor(engines, v1beta1.CoordinationPacing{})
	decoders := map[string]int32{"v2": 4}
	before := map[string][]string{"0": {"v1", "v2"}}
	after := map[string][]string{"0": {"v1"}}
	for i := 1; i <= 11; i++ {
		proto := "v2"
		if i >= 9 {
			proto = "v1"
		}
		before[fmt.Sprint(i)], after[fmt.Sprint(i)] = []string{proto}, []string{proto}
	}
	if kept, left := pairableInstanceCount(before, decoders), pairableInstanceCount(after, decoders); kept != 9 || left != 8 || left >= minimum.floor {
		t.Fatalf("by the gate's own count the engine keeps %d pairable Instances with the replacement and %d without, against its %s", kept, left, minimum)
	}
	allowed, gate, reason := drainGate(t, isvc, eng, []string{"llama-engine-0-default-1"}, objs...)
	if !allowed {
		t.Fatalf("the gate lets the replacement leave, reading the decoders' minimum alone; got the %s hold %q. A gate that reads the engine's own minimum holds it, and this lock then flips", gate, reason)
	}
}
