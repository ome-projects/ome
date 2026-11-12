package workload_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/utils"
)

// This file drives the production engine of a prefill/decode service, and
// in full mode of an engine of hundreds of single-pod Instances, while the
// operator edits the rollout's strategy and budgets at every moment of a
// roll. It
// runs in the shared world of scale_beside_a_roll_model_test.go: one
// workload.Reconcile loop per Component on the recovery harness, the
// coordination gates wired between the Components, the follower count of
// a scaling policy and the partition a canary step projects. The edits
// are the moves under test, on the prefill side, the decode side or the
// group, at idle, settling, rotating, draining, the last Instance, held
// and a canary step, once or twice in one roll:
//   - the update strategy flipped between SurgeThenDrain, RecreatePod and
//     InPlaceIfPossible, with the budget rewritten onto the field the new
//     strategy uses, with the type alone changed and the old field left
//     standing, or with both fields written;
//   - the side's own maxSurge or maxUnavailable raised or lowered, as a
//     percentage and as a count;
//   - the rolling group's maxSurge and maxUnavailable raised or lowered.
//
// A partition set or cleared, a pause, a roll back, a second push and a
// pod lost ride beside them; seeded random runs mix up to three moves.
// The pairing protocol and the scales are left to the models that drive
// them (the pairing gate model and the scaling model).
//
// Beside the shared world's readings (the attempts in flight against the
// budgets, the version mix, a settled end that is complete, paused or
// held on a spent retry schedule, no two gates waiting on each other)
// three are this model's, read in pods that serve:
//   - serving-below-the-promised-minimum: a side keeps at least its count
//     less the unavailability budget the spec names now serving, the
//     budget being the tighter of the side's own maxUnavailable and the
//     group's; the attempts open when a budget is lowered finish first,
//     and the Instances the world's faults took down are not the roll's.
//   - pods-above-the-surge-ceiling: a side never has more pods than its
//     count plus the surge budget the spec names now, the tighter of the
//     side's own maxSurge and the group's; the replacements open when a
//     budget is lowered finish first.
//   - pods-left-beside-the-roll: at a complete end no pod stands under an
//     index with no row or on a revision the spec does not name for it.
//
// OME_STRATEGY_EDIT_HUNT=full widens the configurations, the moves and
// the bounds; OME_STRATEGY_EDIT_HUNT_ONLY=<text> keeps the configurations
// whose name carries the text.

const (
	strategyEditFullEnv = "OME_STRATEGY_EDIT_HUNT"
	strategyEditOnlyEnv = "OME_STRATEGY_EDIT_HUNT_ONLY"
	// strategyEditFullSample is how many sampled configurations the full
	// run adds to the reference ones; strategyEditFullTickBudget is the
	// most ticks one configuration may spend in full mode, three times the
	// scaling hunt's, so the moves on the large shapes are reached.
	strategyEditFullSample     = 36
	strategyEditFullTickBudget = 24000
	// strategyEditFleetTickBudget is the most ticks an engine of hundreds
	// of single-pod Instances may spend: a tick there costs seconds, since
	// the fake apiserver serialises every pod on every read.
	strategyEditFleetTickBudget = 900
)

// scaleRolling is a side's own rollingUpdate budgets as the operator wrote
// them: each field a count or a percentage, or empty for unset. Unwritten,
// the configuration's budget stands on the field the strategy uses.
type scaleRolling struct {
	written            bool
	surge, unavailable string
}

func (r scaleRolling) String() string {
	if !r.written {
		return "budgets=configured"
	}
	return fmt.Sprintf("surge=%s unavailable=%s", budgetFieldText(r.surge), budgetFieldText(r.unavailable))
}

// budgetFieldText names a budget field as written: the value, or "unset".
func budgetFieldText(v string) string {
	if v == "" {
		return "unset"
	}
	return v
}

// budgetValue parses a budget field as written; an empty field is unset.
func budgetValue(v string) *intstr.IntOrString {
	if v == "" {
		return nil
	}
	parsed := intstr.Parse(v)
	return &parsed
}

// rollingAsWritten reads a side's rollingUpdate block as written fields:
// what an operator who changes the type alone leaves standing.
func (w *scaleWorld) rollingAsWritten(s *scaleSide) scaleRolling {
	rolling := w.rollingUpdateOf(s)
	out := scaleRolling{written: true}
	if rolling.MaxSurge != nil {
		out.surge = rolling.MaxSurge.String()
	}
	if rolling.MaxUnavailable != nil {
		out.unavailable = rolling.MaxUnavailable.String()
	}
	return out
}

// setStrategy writes a side's update strategy and the budgets the operator
// wrote with it, and records the attempts open at the edit for the
// readings that let them finish.
func (w *scaleWorld) setStrategy(s *scaleSide, strategy workloadtypes.UpdateStrategyType, budgets scaleRolling) {
	w.noteOpenAttempts(s)
	s.strategy = strategy
	s.rolling = budgets
	w.render(s)
}

// setBudgets writes a side's own maxSurge and maxUnavailable.
func (w *scaleWorld) setBudgets(s *scaleSide, budgets scaleRolling) {
	w.noteOpenAttempts(s)
	s.rolling = budgets
	w.render(s)
}

// setGroupBudget writes the rolling group's maxSurge and maxUnavailable on
// the live spec, which the coordination gates read as it is now.
func (w *scaleWorld) setGroupBudget(text string) {
	for _, s := range w.sides {
		w.noteOpenAttempts(s)
	}
	group := &w.isvc.Spec.Rollout.Groups[0]
	if group.RollingUpdate == nil {
		return
	}
	value := intstr.Parse(text)
	group.RollingUpdate.MaxSurge = &value
	group.RollingUpdate.MaxUnavailable = &value
}

// noteOpenAttempts hands the attempts a side has open to the strategy
// hunt's readings, when one is installed, before an edit changes what
// the budgets allow.
func (w *scaleWorld) noteOpenAttempts(s *scaleSide) {
	if w.openAttempts != nil {
		w.openAttempts(s)
	}
}

// strategyEditHunt is the model's reading of one world: per side, the
// attempts open at the last edit that the budgets let finish, and the
// clock the readings were taken on.
type strategyEditHunt struct {
	w *scaleWorld
	// clk is the world's clock the readings were taken on; a restore
	// replaces the clock, and the readings start over.
	clk *clocktesting.FakeClock
	// drainsOpen and surgesOpen are, per side, the Instances out of
	// rotation and the replacements in flight when the last edit landed,
	// read in pods; drainRows and surgeRows the same read in rows, the
	// way the engine charges its budgets. A lowered budget lets them
	// finish before it binds.
	drainsOpen map[*scaleSide]int32
	surgesOpen map[*scaleSide]int32
	drainRows  map[*scaleSide]int32
	surgeRows  map[*scaleSide]int32
}

// installStrategyEditHunt puts the model's moves and checks on a world.
func installStrategyEditHunt(w *scaleWorld) {
	h := &strategyEditHunt{w: w}
	h.reset()
	w.moves = h.moves
	w.moreChecks = h.check
	w.endChecks = h.endCheck
	w.openAttempts = h.noteOpen
	w.pastBudget = h.pastBudget
	w.endClass = h.endClass
}

// reset starts the readings over on the world's current clock.
func (h *strategyEditHunt) reset() {
	h.clk = h.w.clk
	h.drainsOpen = map[*scaleSide]int32{}
	h.surgesOpen = map[*scaleSide]int32{}
	h.drainRows = map[*scaleSide]int32{}
	h.surgeRows = map[*scaleSide]int32{}
}

// noteOpen records what a side has in flight as an edit lands: the
// Instances out of rotation and the replacements coming up, which a
// lowered budget lets finish.
func (h *strategyEditHunt) noteOpen(s *scaleSide) {
	if h.clk != h.w.clk {
		h.reset()
	}
	rows := h.w.rows(s)
	h.drainsOpen[s] = max(h.drainsOpen[s], s.h.replicas-h.servingInstances(s))
	h.surgesOpen[s] = max(h.surgesOpen[s], h.extraPods(s)/max(h.w.podsPerInstance(s), 1), escalation.CurrentSurgeInFlight(rows))
	h.drainRows[s] = max(h.drainRows[s], escalation.CurrentUnavailableInFlight(rows))
	h.surgeRows[s] = max(h.surgeRows[s], escalation.CurrentSurgeInFlight(rows))
}

// pastBudget is how many attempts beyond budget the shared budget reading
// allows a side in flight: the attempts open in rows when the last edit
// landed, until the attempts in flight are back inside the budget.
func (h *strategyEditHunt) pastBudget(s *scaleSide, surge bool, budget int32) int32 {
	if h.clk != h.w.clk {
		h.reset()
	}
	rows := h.w.rows(s)
	open, recorded := escalation.CurrentUnavailableInFlight(rows), h.drainRows
	if surge {
		open, recorded = escalation.CurrentSurgeInFlight(rows), h.surgeRows
	}
	if open <= budget {
		recorded[s] = 0
	}
	return max(recorded[s]-budget, 0)
}

// strategyEditFleet reports a configuration of one engine of hundreds of
// single-pod Instances, the shape the budget flips are driven on.
func strategyEditFleet(cfg scaleConfig) bool {
	return cfg.shape.decode == 0 && cfg.shape.prefill >= 100
}

// moves lists the moves tried from a moment: the strategy flips, the
// budget edits and the group budget edits on each side, then the moves
// that ride beside an edit, each listed once. An engine of hundreds
// tries its own shorter list.
func (h *strategyEditHunt) moves(m scaleMoment, full bool) []scaleMove {
	w := h.w
	if strategyEditFleet(w.cfg) {
		return h.fleetMoves(m)
	}
	var out []scaleMove
	for side, s := range w.pair() {
		if !full && side > 0 && m != scaleAtRotating {
			continue
		}
		out = append(out, h.flips(side, s, m, full)...)
		out = append(out, h.budgets(side, s, m, full)...)
	}
	out = append(out, h.groupBudgets(m, full)...)
	out = append(out, h.beside(m, full)...)
	seen := map[string]bool{}
	var kept []scaleMove
	for _, mv := range out {
		if text := mv.String(); !seen[text] {
			seen[text] = true
			kept = append(kept, mv)
		}
	}
	return kept
}

// fleetMoves lists the moves an engine of hundreds tries, while settling
// and rotating: the strategy flipped to each other strategy with the
// budget rewritten and with the type alone changed, and while rotating
// its budget lowered to five percent and raised to a quarter. A budget
// of one Instance on three hundred outruns the tick bound and is left
// to the reference shapes.
func (h *strategyEditHunt) fleetMoves(m scaleMoment) []scaleMove {
	if m != scaleAtSettling && m != scaleAtRotating {
		return nil
	}
	s := h.w.prefill()
	var out []scaleMove
	for _, target := range h.flipTargets(s, m, true) {
		out = append(out, scaleMove{kind: moveStrategy, side: 0, strategy: target},
			scaleMove{kind: moveStrategy, side: 0, strategy: target, budgets: h.w.rollingAsWritten(s)})
	}
	if m != scaleAtRotating {
		return out
	}
	written := h.w.rollingAsWritten(s)
	for _, value := range []string{"5%", "25%"} {
		edited := written
		if s.strategy == workloadtypes.UpdateStrategySurgeThenDrain {
			edited.surge = value
		} else {
			edited.unavailable = value
		}
		out = append(out, scaleMove{kind: moveBudget, side: 0, budgets: edited})
	}
	return out
}

// flipTargets lists the strategies a side may be flipped to: the other
// drain-first or surge strategy always, and InPlaceIfPossible in full
// mode or on a single-pod side while rotating.
func (h *strategyEditHunt) flipTargets(s *scaleSide, m scaleMoment, full bool) []workloadtypes.UpdateStrategyType {
	recreate, surge, inPlace := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategyInPlaceIfPossible
	withInPlace := full || (h.w.cfg.shape.pods == 1 && m == scaleAtRotating)
	switch s.strategy {
	case surge:
		if withInPlace {
			return []workloadtypes.UpdateStrategyType{recreate, inPlace}
		}
		return []workloadtypes.UpdateStrategyType{recreate}
	case recreate:
		if withInPlace {
			return []workloadtypes.UpdateStrategyType{surge, inPlace}
		}
		return []workloadtypes.UpdateStrategyType{surge}
	}
	return []workloadtypes.UpdateStrategyType{recreate, surge}
}

// flips lists the strategy flips of a side at a moment: to each target
// with the budget rewritten onto the new strategy's field; with the type
// alone changed, the old field left standing, while settling and rotating
// in bounded mode and always in full mode; with both fields written, and
// the flip taken back once the roll settles, on the prefill side while
// rotating in bounded mode and everywhere in full mode. In bounded mode
// the flip to InPlaceIfPossible comes in its plain form alone.
func (h *strategyEditHunt) flips(side int, s *scaleSide, m scaleMoment, full bool) []scaleMove {
	w := h.w
	both := scaleRolling{written: true, surge: w.cfg.settings.budgetText(), unavailable: w.cfg.settings.budgetText()}
	var out []scaleMove
	for _, target := range h.flipTargets(s, m, full) {
		out = append(out, scaleMove{kind: moveStrategy, side: side, strategy: target})
		if !full && target == workloadtypes.UpdateStrategyInPlaceIfPossible {
			continue
		}
		if full || m == scaleAtSettling || m == scaleAtRotating {
			out = append(out, scaleMove{kind: moveStrategy, side: side, strategy: target, budgets: w.rollingAsWritten(s)})
		}
		if full || (m == scaleAtRotating && side == 0) {
			out = append(out, scaleMove{kind: moveStrategy, side: side, strategy: target, budgets: both},
				scaleMove{kind: moveStrategyTwice, side: side, strategy: target, strategy2: s.strategy})
		}
	}
	return out
}

// budgetValues lists the values a budget edit writes on the field a
// side's strategy uses at a moment: lowered to one Instance and raised
// to a quarter in bounded mode, the raise alone at idle and at a canary
// step and the lowering alone at the last Instance and held; in full
// mode one and two Instances, five, twenty-five, fifty and a hundred
// percent, and half the count as a count.
func (h *strategyEditHunt) budgetValues(s *scaleSide, m scaleMoment, full bool) []string {
	if full {
		return []string{"1", "2", "5%", "25%", "50%", "100%", fmt.Sprint(max(s.h.replicas/2, 1))}
	}
	switch m {
	case scaleAtSettling, scaleAtRotating, scaleAtDraining:
		return []string{"1", "25%"}
	case scaleAtIdle, scaleAtCanaryStep:
		return []string{"25%"}
	}
	return []string{"1"}
}

// budgets lists the budget edits of a side at a moment: each value on
// the field the strategy uses, the other field left as written (the
// decode side's lowering alone in bounded mode); and the other field
// written at one Instance beside it, on the prefill side while rotating
// in bounded mode and always in full mode, since under SurgeThenDrain
// the unavailability budget caps the drains and under a drain-first
// strategy the surge budget caps nothing until the strategy flips.
func (h *strategyEditHunt) budgets(side int, s *scaleSide, m scaleMoment, full bool) []scaleMove {
	written := h.w.rollingAsWritten(s)
	surges := s.strategy == workloadtypes.UpdateStrategySurgeThenDrain
	values := h.budgetValues(s, m, full)
	if !full && side > 0 {
		values = values[:1]
	}
	var out []scaleMove
	for _, value := range values {
		edited := written
		if surges {
			edited.surge = value
		} else {
			edited.unavailable = value
		}
		out = append(out, scaleMove{kind: moveBudget, side: side, budgets: edited})
	}
	if full || (m == scaleAtRotating && side == 0) {
		other := written
		if surges {
			other.unavailable = "1"
		} else {
			other.surge = "1"
		}
		out = append(out, scaleMove{kind: moveBudget, side: side, budgets: other})
	}
	return out
}

// groupBudgets lists the rolling group's budget edits at a moment, when
// the configuration has a rolling group: one Instance and half in bounded
// mode while rotating; one Instance, five, twenty-five, fifty and a
// hundred percent everywhere in full mode.
func (h *strategyEditHunt) groupBudgets(m scaleMoment, full bool) []scaleMove {
	groups := h.w.isvc.Spec.Rollout.Groups
	if len(groups) == 0 || groups[0].RollingUpdate == nil {
		return nil
	}
	values := []string{"1", "50%"}
	switch {
	case full:
		values = []string{"1", "5%", "25%", "50%", "100%"}
	case m != scaleAtRotating:
		return nil
	}
	var out []scaleMove
	for _, value := range values {
		out = append(out, scaleMove{kind: moveGroupBudget, text: value})
	}
	return out
}

// beside lists the moves that ride beside an edit at a moment, for the
// random runs to mix with the edits: in bounded mode a partition of half
// the prefill count, a pause, a roll back, a second push and a pod lost
// on the prefill side's oldest Instance while rotating, a roll back
// while settling and a second push at the last Instance; in full mode
// the partition set and cleared, the pause, the roll back, the second
// push and the world's losses on both sides at every moment.
func (h *strategyEditHunt) beside(m scaleMoment, full bool) []scaleMove {
	w := h.w
	prefill := w.prefill()
	half := scaleMove{kind: movePartition, side: 0, delta: prefill.h.replicas / 2}
	pause := scaleMove{kind: movePause, side: 0, delta: 0}
	var out []scaleMove
	if !full {
		switch m {
		case scaleAtRotating:
			out = append(out, half, pause, scaleMove{kind: moveRollBack}, scaleMove{kind: movePush})
			if idx := w.lossIndices(prefill); len(idx) > 0 {
				out = append(out, scaleMove{kind: moveLosePod, side: 0, index: idx[0]})
			}
		case scaleAtSettling:
			out = append(out, scaleMove{kind: moveRollBack})
		case scaleAtLast:
			out = append(out, scaleMove{kind: movePush})
		}
		return out
	}
	out = append(out, half, scaleMove{kind: movePartition, side: 0, delta: 0}, pause)
	if m != scaleAtIdle {
		out = append(out, scaleMove{kind: moveRollBack}, scaleMove{kind: movePush})
	}
	for side, s := range w.pair() {
		for _, idx := range w.lossIndices(s) {
			out = append(out, scaleMove{kind: moveLosePod, side: side, index: idx}, scaleMove{kind: moveLoseNode, side: side, index: idx})
			if w.cfg.shape.pods > 1 {
				out = append(out, scaleMove{kind: moveLoseMember, side: side, index: idx})
			}
		}
	}
	return out
}

// check runs the model's per-tick readings on every side.
func (h *strategyEditHunt) check() {
	if h.clk != h.w.clk {
		h.reset()
	}
	for _, s := range h.w.sides {
		h.checkServingMinimum(s)
		h.checkSurgeCeiling(s)
	}
}

// groupPacing is the rolling group's pacing a side rolls under, as the
// gates read it now; false when the side rolls in no rolling group.
func (h *strategyEditHunt) groupPacing(s *scaleSide) (v1beta1.CoordinationPacing, bool) {
	groups := coordination.ResolveGroups(rollout.EffectiveCoordination(h.w.isvc, rollout.Policies{}), h.w.defaults)
	group, ok := coordination.MembershipFor(groups, s.component())
	if !ok || group.Policy != v1beta1.CoordinationPolicyRollingUpdate {
		return v1beta1.CoordinationPacing{}, false
	}
	return group.Pacing, true
}

// tighter is the tighter of two budgets, where a negative one is no
// budget at all.
func tighter(a, b int32) int32 {
	switch {
	case a < 0:
		return b
	case b < 0:
		return a
	}
	return min(a, b)
}

// budgetOf resolves one budget field against a count the way the API
// documents it: a count as written, a percentage rounded up; negative
// when the field is unset. The model does its own arithmetic here so
// the engine's budget helpers stay the thing under test.
func budgetOf(field *intstr.IntOrString, n int32) int32 {
	if field == nil {
		return -1
	}
	return utils.ScaledCountFromIntOrString(field, n, true)
}

// promisedUnavailable is the most Instances of a side the spec lets the
// roll keep out of rotation now: the tighter of the side's own
// maxUnavailable and the rolling group's, or negative when neither names
// one.
func (h *strategyEditHunt) promisedUnavailable(s *scaleSide) int32 {
	n := s.h.replicas
	own := budgetOf(s.h.lifecycle.UpdateStrategy.RollingUpdate.MaxUnavailable, n)
	pacing, ok := h.groupPacing(s)
	if !ok {
		return own
	}
	return tighter(own, budgetOf(pacing.MaxUnavailable, n))
}

// promisedSurge is the most extra Instances' worth of pods the spec lets
// a side carry now: the tighter of the side's own maxSurge and the
// rolling group's, or negative when neither names one.
func (h *strategyEditHunt) promisedSurge(s *scaleSide) int32 {
	n := s.h.replicas
	own := budgetOf(s.h.lifecycle.UpdateStrategy.RollingUpdate.MaxSurge, n)
	pacing, ok := h.groupPacing(s)
	if !ok {
		return own
	}
	return tighter(own, budgetOf(pacing.MaxSurge, n))
}

// servesAFullSet reports an Instance served by the pods under one index:
// one full pod set, all Ready and in the serving gate, on one image.
func (h *strategyEditHunt) servesAFullSet(s *scaleSide, pods []*corev1.Pod) bool {
	byImage := map[string][]*corev1.Pod{}
	for _, pod := range pods {
		image := podImage(pod)
		byImage[image] = append(byImage[image], pod)
	}
	for _, set := range byImage {
		if h.w.servingSet(s, set) {
			return true
		}
	}
	return false
}

// podImage is the image a pod's first container runs.
func podImage(pod *corev1.Pod) string {
	if len(pod.Spec.Containers) == 0 {
		return ""
	}
	return pod.Spec.Containers[0].Image
}

// servingInstances counts the Instances of a side in rotation: a row
// that is not leaving and not a replacement gang's slot, served by its
// own pods or by the replacement gang its surge builds.
func (h *strategyEditHunt) servingInstances(s *scaleSide) int32 {
	w := h.w
	byIndex := w.livePodsByIndex(s)
	var n int32
	for _, r := range w.rows(s) {
		if replacementOrLeaving(r) {
			continue
		}
		if h.servesAFullSet(s, byIndex[r.Index]) {
			n++
			continue
		}
		if r.Operation != nil && r.Operation.SurgeIndex != nil && h.servesAFullSet(s, byIndex[*r.Operation.SurgeIndex]) {
			n++
		}
	}
	return n
}

// faultedDown counts the Instances a fault of this run took pods from
// that are not in rotation now: their outage is the world's, not the
// roll's.
func (h *strategyEditHunt) faultedDown(s *scaleSide) int32 {
	byIndex := h.w.livePodsByIndex(s)
	var n int32
	for idx := range h.w.faulted[s] {
		if !h.servesAFullSet(s, byIndex[idx]) {
			n++
		}
	}
	return n
}

// extraPods counts a side's live pods beyond one full set per Instance
// of its count.
func (h *strategyEditHunt) extraPods(s *scaleSide) int32 {
	return max(int32(len(s.h.livePods()))-s.h.replicas*h.w.podsPerInstance(s), 0)
}

// checkServingMinimum holds a side to the serving minimum the spec
// promises: its count less the unavailability budget named now, less the
// Instances the world's faults took down. The attempts open when the
// budget was lowered finish first: until the Instances out of rotation
// are back inside the new budget, the count open at the edit is allowed.
func (h *strategyEditHunt) checkServingMinimum(s *scaleSide) {
	promised := h.promisedUnavailable(s)
	if promised < 0 || h.w.scaled {
		return
	}
	n := s.h.replicas
	down := n - h.servingInstances(s) - h.faultedDown(s)
	if h.drainsOpen[s] > promised && down <= promised {
		h.drainsOpen[s] = 0
	}
	if allowed := max(promised, h.drainsOpen[s]); down > allowed {
		h.w.note("serving-below-the-promised-minimum", "%s has %d of %d Instances out of rotation; the spec promises at most %d (own %s, group %s): %s",
			s.comp, down, n, allowed, h.w.rollingAsWritten(s), h.groupBudgetText(s), h.w.picture())
	}
}

// checkSurgeCeiling holds a side's live pods under its count plus the
// surge budget named now. The replacements open when the budget was
// lowered finish first.
func (h *strategyEditHunt) checkSurgeCeiling(s *scaleSide) {
	promised := h.promisedSurge(s)
	if promised < 0 || h.w.scaled {
		return
	}
	per := max(h.w.podsPerInstance(s), 1)
	extra := (h.extraPods(s) + per - 1) / per
	if h.surgesOpen[s] > promised && extra <= promised {
		h.surgesOpen[s] = 0
	}
	if allowed := max(promised, h.surgesOpen[s]); extra > allowed {
		h.w.note("pods-above-the-surge-ceiling", "%s has %d extra Instances' worth of pods beside its %d; the spec allows at most %d (own %s, group %s): %s",
			s.comp, extra, s.h.replicas, allowed, h.w.rollingAsWritten(s), h.groupBudgetText(s), h.w.picture())
	}
}

// groupBudgetText names the rolling group's budgets as the gates read
// them now, or says the side rolls in no rolling group.
func (h *strategyEditHunt) groupBudgetText(s *scaleSide) string {
	pacing, ok := h.groupPacing(s)
	if !ok {
		return "none"
	}
	return fmt.Sprintf("surge=%s unavailable=%s", pacing.MaxSurge.String(), pacing.MaxUnavailable.String())
}

// endClass names the two ends the model knows by their exact picture, so
// every other hold with no way out keeps its own class: a side held by
// the lockstep beside a peer resting at its partition, and in-place
// attempts standing on pods whose node stopped reporting.
func (h *strategyEditHunt) endClass(class string) string {
	switch {
	case class == "holds-for-good" && h.heldByTheLockstepAtThePeersPartition():
		return "held-by-the-lockstep-at-the-peers-partition"
	case class == "stands-still-without-a-hold" && h.inPlaceAttemptsStandOnLostNodes():
		return "in-place-attempt-stands-on-a-lost-node"
	}
	return class
}

// heldByTheLockstepAtThePeersPartition reports a side holding on the
// lockstep ("would outpace") while every other side of the pair rests
// complete with a partition holding Instances on the old version.
func (h *strategyEditHunt) heldByTheLockstepAtThePeersPartition() bool {
	w := h.w
	onLockstep, peerAtPartition := false, false
	for _, s := range w.pair() {
		if w.sideComplete(s) {
			if partitionValue(w.effectivePartition(s)) > 0 && status.HeldInstanceCount(w.rows(s), s.h.target.Name) > 0 {
				peerAtPartition = true
			}
			continue
		}
		hold := s.h.hold
		if hold == nil || hold.Gate != workloadtypes.RolloutHoldGateRatio || !strings.Contains(hold.Reason, "outpace") {
			return false
		}
		onLockstep = true
	}
	return onLockstep && peerAtPartition
}

// inPlaceAttemptsStandOnLostNodes reports that every side still rolling
// names no hold and stands only on in-place attempts whose pods sit on a
// node that stopped reporting.
func (h *strategyEditHunt) inPlaceAttemptsStandOnLostNodes() bool {
	w := h.w
	found := false
	for _, s := range w.sides {
		if w.sideComplete(s) {
			continue
		}
		if s.h.hold != nil {
			return false
		}
		byIndex := w.livePodsByIndex(s)
		for _, r := range w.rows(s) {
			if r.Phase == workloadtypes.InstancePhaseReady && r.Operation == nil {
				continue
			}
			if r.Operation == nil || r.Operation.Type != workloadtypes.InstanceOperationUpdate || r.Operation.Step != workloadtypes.UpdateStepInPlace {
				return false
			}
			for _, pod := range byIndex[r.Index] {
				if !s.h.deadNodes[pod.Spec.NodeName] {
					return false
				}
			}
			found = true
		}
	}
	return found
}

// endCheck reads a complete settled end: every live pod stands under an
// index with a row and runs the image the spec names for it, unless the
// partition holds its Instance.
func (h *strategyEditHunt) endCheck() {
	w := h.w
	for _, s := range w.sides {
		indices := map[int32]bool{}
		for _, r := range w.rows(s) {
			indices[r.Index] = true
		}
		held := w.heldIndices(s)
		for _, pod := range s.h.livePods() {
			idx, ok := query.InstanceIdxFromLabels(pod)
			switch {
			case !ok || !indices[idx]:
				w.note("pods-left-beside-the-roll", "%s pod %s stands under an index with no row at a complete end: %s", s.comp, pod.Name, w.picture())
			case podImage(pod) != s.image && !held[idx]:
				w.note("pods-left-beside-the-roll", "%s pod %s runs %s at a complete end while the spec names %s: %s", s.comp, pod.Name, scaleImageShort(podImage(pod)), scaleImageShort(s.image), w.picture())
			}
		}
	}
}

// strategyEditReferenceConfigs are the configurations every run explores:
// the reference shapes under their reference settings, each side
// on SurgeThenDrain, RecreatePod or InPlaceIfPossible.
func strategyEditReferenceConfigs() []scaleConfig {
	recreate, surge, inPlace := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategyInPlaceIfPossible
	return []scaleConfig{
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: surge, decode: surge}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate}},
		{shape: scaleShape{16, 8, 1}, settings: scaleSettings{prefill: surge, decode: recreate, maintainRatio: true}},
		{shape: scaleShape{15, 15, 1}, settings: scaleSettings{prefill: recreate, decode: surge, keepRatio: true, firstSide: v1beta1.DecoderComponent}},
		{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: surge, decode: surge, canary: true}},
		{shape: scaleShape{24, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, partition: 2}},
		{shape: scaleShape{30, 10, 1}, settings: scaleSettings{prefill: surge, decode: surge, policy: true}},
		{shape: scaleShape{12, 4, 2}, settings: scaleSettings{prefill: surge, decode: surge, keepRatio: true}},
		{shape: scaleShape{12, 4, 4}, settings: scaleSettings{prefill: recreate, decode: recreate}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: recreate, decode: recreate, router: true}},
		{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: inPlace, decode: inPlace}},
	}
}

// strategyEditConfigs lists the configurations of a run: the reference
// ones, and in full mode an engine of five hundred single-pod Instances
// under SurgeThenDrain and engines of three hundred under RecreatePod and
// InPlaceIfPossible for the budget flips, and a seeded sample of the
// product of shapes, gang sizes, strategy pairs and settings.
func strategyEditConfigs(full bool) []scaleConfig {
	configs := strategyEditReferenceConfigs()
	if !full {
		return configs
	}
	recreate, surge, inPlace := workloadtypes.UpdateStrategyRecreatePod, workloadtypes.UpdateStrategySurgeThenDrain, workloadtypes.UpdateStrategyInPlaceIfPossible
	configs = append(configs,
		scaleConfig{shape: scaleShape{500, 0, 1}, settings: scaleSettings{prefill: surge, decode: surge}},
		scaleConfig{shape: scaleShape{300, 0, 1}, settings: scaleSettings{prefill: recreate, decode: recreate}},
		scaleConfig{shape: scaleShape{300, 0, 1}, settings: scaleSettings{prefill: inPlace, decode: inPlace}},
	)
	shapes := [][2]int32{{12, 8}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}}
	pods := []int32{1, 2, 4, 8}
	strategies := [][2]workloadtypes.UpdateStrategyType{{recreate, recreate}, {surge, surge}, {recreate, surge}, {surge, recreate}, {inPlace, inPlace}, {surge, inPlace}}
	variants := []scaleSettings{
		{},
		{maintainRatio: true},
		{keepRatio: true},
		{keepRatio: true, firstSide: v1beta1.DecoderComponent},
		{keepRatio: true, firstSide: v1beta1.EngineComponent},
		{partition: 2},
		{canary: true},
		{policy: true},
		{router: true},
		{sequential: true},
		{blueGreen: true},
		{maintainRatio: true, policy: true},
	}
	seen := map[string]bool{}
	for _, cfg := range configs {
		seen[cfg.String()] = true
	}
	var all []scaleConfig
	for _, shape := range shapes {
		for _, p := range pods {
			if (shape[0]+shape[1])*p > scaleFullPodCeiling {
				continue
			}
			for _, pair := range strategies {
				for _, variant := range variants {
					settings := variant
					settings.prefill, settings.decode = pair[0], pair[1]
					cfg := scaleConfig{shape: scaleShape{shape[0], shape[1], p}, settings: settings}
					if !seen[cfg.String()] {
						seen[cfg.String()] = true
						all = append(all, cfg)
					}
				}
			}
		}
	}
	rng := rand.New(rand.NewSource(1))
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	if len(all) > strategyEditFullSample {
		all = all[:strategyEditFullSample]
	}
	return append(configs, all...)
}

// knownStrategyEditFindingClasses are the finding classes the model shows
// at present: the two ends endClass names, and the shared world's
// pictures of the version mix after a move that re-plans the batches,
// which a strategy or budget edit is one of. A finding in a known class
// is reported and does not fail the run; a change of behaviour removes its entry and the
// lock test that holds the picture.
var knownStrategyEditFindingClasses = strategyEditKnownClasses()

func strategyEditKnownClasses() map[string]string {
	out := map[string]string{
		"held-by-the-lockstep-at-the-peers-partition": "under the ratio rule the lockstep reads a peer's share over all its Instances, not over the ones its partition lets the roll move, so a side patched in place beside a peer resting at its partition holds on the lockstep's outpace reason for as long as the partition stands; the batch step plans over the movable Instances and the lockstep does not (held by TestStrategyEdit_APartitionOnThePeerHoldsAnInPlaceSideUnderTheRatioRule)",
		"in-place-attempt-stands-on-a-lost-node":      "an in-place attempt patches a pod whose node stopped reporting and waits for a kubelet that never answers; the force-delete policy that clears such a pod on the repair and the drain-first paths is never asked, so the attempt stands at its InPlace step with no hold (held by TestStrategyEdit_AnInPlaceAttemptOnAPodWhoseNodeStoppedReportingStandsStill)",
	}
	for class, picture := range knownScaleFindingClasses {
		out[class] = strings.Replace(picture, "(a resize, a partition, a roll back)", "(a resize, a partition, a roll back, a strategy or budget edit)", 1)
	}
	return out
}

// TestStrategyEdit_EveryEditLeavesTheRollAWayOut explores every
// configuration: the strategy flipped, the side's budgets or the group's
// raised or lowered at any moment of a roll, once or twice, beside a
// partition, a pause, a roll back, a second push or a pod lost, leaves a
// roll that finishes on the version the spec names, holds on a spent
// retry schedule, or waits paused; with every side inside the serving
// minimum and under the surge ceiling the spec names now, and no pod
// left beside the roll.
func TestStrategyEdit_EveryEditLeavesTheRollAWayOut(t *testing.T) {
	full, only := scaleHuntMode(strategyEditFullEnv, strategyEditOnlyEnv)
	runScaleHunt(t, scaleHunt{
		full:    full,
		runs:    scaleHuntRuns(full),
		configs: keepScaleConfigs(strategyEditConfigs(full), func(scaleConfig) bool { return true }, only),
		setup: func(w *scaleWorld) {
			installStrategyEditHunt(w)
			switch {
			case strategyEditFleet(w.cfg):
				w.tickBudget = strategyEditFleetTickBudget
			case full:
				w.tickBudget = strategyEditFullTickBudget
			}
		},
		expectedEnds: scaleExpectedEnds,
		known:        knownStrategyEditFindingClasses,
	})
}

// TestStrategyEdit_APartitionOnThePeerHoldsAnInPlaceSideUnderTheRatioRule
// holds the picture the model found: 20 prefill and 10 decode patched in
// place at ten percent under the ratio rule at a tolerance of 20, with a
// partition of 10 on the prefill side set before the push. The prefill
// side rests at 10 of 20 on version 2, as the partition asks. The decoder
// reaches 7 of 10 and holds on "in-place update would outpace peer
// new-revision rollout past tolerance" for good: the lockstep reads the
// prefill side's share over all 20 Instances, not over the 10 the roll may
// move, so the peer never finishes in its eyes. The batch step plans over
// the movable Instances; the lockstep does not. The known picture: a
// partition under maintainRatio with a reporting gap, where the hold
// names neither the partition nor clearing it as the way out. When the
// decoder finishes beside a peer resting at its partition, this test
// fails, and the known class and this lock go with it.
func TestStrategyEdit_APartitionOnThePeerHoldsAnInPlaceSideUnderTheRatioRule(t *testing.T) {
	inPlace := workloadtypes.UpdateStrategyInPlaceIfPossible
	cfg := scaleConfig{shape: scaleShape{20, 10, 1}, settings: scaleSettings{prefill: inPlace, decode: inPlace, maintainRatio: true, policy: true}}
	w := newScaleWorld(t, cfg)
	installStrategyEditHunt(w)
	w.reachIdle()
	w.apply(scaleMove{kind: movePartition, side: 0, delta: 10})
	w.push(fixedImage)
	settled := w.playOut(scaleTickBound(false))
	prefill, decode := w.prefill(), w.decode()
	if !settled || !w.sideComplete(prefill) {
		t.Fatalf("the prefill side did not rest at its partition: settled=%t %s", settled, w.picture())
	}
	if w.sideComplete(decode) {
		t.Fatalf("the picture moved: the decoder finished beside a prefill side resting at its partition; remove the known class and this lock: %s", w.picture())
	}
	hold := decode.h.hold
	if hold == nil || hold.Gate != workloadtypes.RolloutHoldGateRatio || !strings.Contains(hold.Reason, "outpace") {
		t.Fatalf("the picture moved: the decoder stands on %v, not on the lockstep: %s", hold, w.picture())
	}
	_, promoted, old, _ := w.rollCounts(decode)
	t.Logf("the decoder rests at %d of 10 on version 2 with %d on version 1 beside a prefill side at 10 of 20 under a partition of 10; its hold: %s", promoted, old, hold.Reason)
}

// TestStrategyEdit_AnInPlaceAttemptOnAPodWhoseNodeStoppedReportingStandsStill
// holds the picture the model found: 12 prefill and 8 decode patched in
// place at ten percent, with a force-delete policy that clears a pod
// whose node stopped reporting after five minutes. The node under the
// prefill side's last Instance stops reporting, and the operator pushes
// version 2. Every Instance on a live node moves. The Instances on the
// lost node open their in-place attempt, patch a pod no kubelet will
// restart, and wait: the attempt stands at its InPlace step with no hold
// named, the roll never finishes, and the force-delete policy, which the
// repair and the drain-first paths apply to such a pod, is never asked.
// When the roll finishes past the lost node, this test fails, and the
// lock goes with it.
func TestStrategyEdit_AnInPlaceAttemptOnAPodWhoseNodeStoppedReportingStandsStill(t *testing.T) {
	inPlace := workloadtypes.UpdateStrategyInPlaceIfPossible
	cfg := scaleConfig{shape: scaleShape{12, 8, 1}, settings: scaleSettings{prefill: inPlace, decode: inPlace}}
	w := newScaleWorld(t, cfg)
	installStrategyEditHunt(w)
	w.reachIdle()
	prefill := w.prefill()
	w.apply(scaleMove{kind: moveLoseNode, side: 0, index: prefill.h.replicas - 1})
	w.push(fixedImage)
	settled := w.playOut(scaleTickBound(false))
	if !settled {
		t.Fatalf("the world did not settle: %s", w.picture())
	}
	if w.complete() {
		t.Fatalf("the picture moved: the roll finished past the lost node; remove this lock: %s", w.picture())
	}
	standing := 0
	for _, r := range w.rows(prefill) {
		if r.Operation != nil && r.Operation.Type == workloadtypes.InstanceOperationUpdate && r.Operation.Step == workloadtypes.UpdateStepInPlace {
			standing++
		}
	}
	if standing == 0 || prefill.h.hold != nil {
		t.Fatalf("the picture moved: %d attempts stand at the InPlace step and the side holds on %v: %s", standing, prefill.h.hold, w.picture())
	}
	t.Logf("%d in-place attempts stand on the lost node's pods with no hold: %s", standing, w.picture())
}
