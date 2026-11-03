package allocation

import (
	"fmt"
	"maps"
	"math"
	"slices"
)

// MemberState is what the transition knows about one member cluster under the
// currently applied allocation.
type MemberState struct {
	Known    bool
	Eligible bool
	Applied  bool
	Routable bool
	Drained  bool
	Absent   bool
	Ready    int32
	// Occupied includes instances whose removal is still in progress.
	Occupied int32
}

// Transition retains its original allocation throughout a move, including when
// the destination changes. Replacing From with a surged intermediate allocation
// would manufacture more budget on every retarget or restart.
type Transition struct {
	From    Plan
	Current map[string]int32
	Desired Plan
	Members map[string]MemberState
	// Surge is the shared allowance above the larger of the original and
	// desired floors; zero permits no excess.
	Surge int64
	// RolloutReserved is additional rollout capacity already authorized but not
	// included in Occupied. It consumes the same allowance as placement growth.
	RolloutReserved int32
}

// Step is one persist-before-apply mutation. Reductions and growth are separate
// steps so a requested deletion cannot be mistaken for released resources.
type Step struct {
	// A zero target cannot authorize deletion of an occupied, undrained member.
	Targets  map[string]int32
	Drain    []string
	Resume   []string
	Reason   string
	Complete bool
	// Message explains a hold in operator terms; empty uses the reason's text.
	Message string
	// CleanupReleased reports a complete step whose wait for members to fit
	// within the plan's replicas was ended by an operator request.
	CleanupReleased bool
}

// Advance moves toward Desired while retaining old replicas until replacement
// replicas are ready and routable. A zero target requires confirmed traffic
// drain before removal. Unknown standing members hold the transition.
func Advance(in Transition) (Step, error) {
	return advanceTransition(in, false)
}

// AdvanceWholeMembers preserves each member's full replica policy during a move.
// A target changes directly to its desired floor, so a replacement must fit in
// the shared allowance in full. It does not select admission-race candidates.
func AdvanceWholeMembers(in Transition) (Step, error) {
	return advanceTransition(in, true)
}

func advanceTransition(in Transition, wholeMembers bool) (Step, error) {
	t, err := readTransition(in, wholeMembers)
	if err != nil {
		return Step{}, err
	}
	return t.decide()
}

// transition is one call of Advance or AdvanceWholeMembers as it reads its
// input: every cluster a plan or an observation names, in name order, the
// replica budget the move may spend, and the step it builds.
type transition struct {
	in           Transition
	wholeMembers bool
	// fromReplicas and desiredReplicas are the replica totals of the original
	// and the desired plan, unassigned replicas included.
	fromReplicas    int64
	desiredReplicas int64
	members         []memberView
	step            Step
	// targetsGrow and targetsShrink report a member whose desired target is
	// above, or below, its applied one.
	targetsGrow   bool
	targetsShrink bool
	// inUse counts every replica a member holds or is targeted for, plus the
	// rollout's reservation; growth fits in the allowance above it.
	inUse int64
	// servingReady counts the ready replicas of serving members, up to each
	// member's applied target.
	servingReady int64
	// readyWithoutTarget counts the ready replicas of routable members whose
	// applied target is zero.
	readyWithoutTarget int64
	// retirementAllowance is how many original replicas may still be retired
	// now: the plan's shrink and the serving replacements, less the originals
	// already retired or drained.
	retirementAllowance int64
	// readyAboveDesired is the ready capacity above the desired total; it
	// funds the drain of a routable member no plan targets.
	readyAboveDesired int64
	// servingAboveMinimum is the serving capacity above the smaller plan
	// total; a reduction of serving replicas spends it.
	servingAboveMinimum int64
	// growthAllowance is what the surge allowance leaves for growth;
	// budgetBlocked records a whole-member growth it could not fit.
	growthAllowance int64
	budgetBlocked   bool
}

// memberView is one cluster as the transition reads it: its target in the
// original plan, in the applied allocation and in the desired plan, and what
// the cluster reports.
type memberView struct {
	name     string
	original int32
	current  int32
	goal     int32
	state    MemberState
}

// readTransition validates the input and reads it once: the plan totals, the
// clusters in name order, and the budget. The first problem in that order is
// the error.
func readTransition(in Transition, wholeMembers bool) (*transition, error) {
	t := &transition{in: in, wholeMembers: wholeMembers}
	var err error
	if t.fromReplicas, err = planTotal(in.From); err != nil {
		return nil, fmt.Errorf("original plan: %w", err)
	}
	if t.desiredReplicas, err = planTotal(in.Desired); err != nil {
		return nil, fmt.Errorf("desired plan: %w", err)
	}
	if _, err := planTotal(Plan{Targets: in.Current}); err != nil {
		return nil, fmt.Errorf("current targets: %w", err)
	}
	if in.RolloutReserved < 0 || in.Surge < 0 {
		return nil, fmt.Errorf("surge allowance and rollout reservation must be nonnegative")
	}
	if err := t.readMembers(); err != nil {
		return nil, err
	}
	if err := t.readBudget(); err != nil {
		return nil, err
	}
	return t, nil
}

// readMembers lists every cluster a plan or an observation names, in name
// order, with its three targets and its observation. The step starts at the
// applied targets. An observation under an empty name is an error.
func (t *transition) readMembers() error {
	names := make(map[string]struct{})
	for _, targets := range []map[string]int32{t.in.Desired.Targets, t.in.From.Targets, t.in.Current} {
		for name := range targets {
			names[name] = struct{}{}
		}
	}
	for name := range t.in.Members {
		if name == "" {
			return fmt.Errorf("home observations require nonempty cluster names")
		}
		names[name] = struct{}{}
	}
	t.step = Step{Targets: make(map[string]int32, len(names))}
	for _, name := range slices.Sorted(maps.Keys(names)) {
		t.members = append(t.members, memberView{
			name:     name,
			original: t.in.From.Targets[name],
			current:  t.in.Current[name],
			goal:     t.in.Desired.Targets[name],
			state:    t.in.Members[name],
		})
		t.step.Targets[name] = t.in.Current[name]
	}
	return nil
}

// readBudget validates each member's observation and counts it into the
// budget, in name order, then derives the allowances a step may spend.
func (t *transition) readBudget() error {
	t.inUse = int64(t.in.RolloutReserved)
	t.retirementAllowance = max(int64(0), t.fromReplicas-t.desiredReplicas)
	for _, member := range t.members {
		if member.state.inconsistent() {
			return fmt.Errorf("cluster %q has inconsistent ready/occupied observations", member.name)
		}
		if err := t.countMember(member); err != nil {
			return err
		}
	}
	// Replicas can serve before the first plan completes. A retarget that
	// raises the total must keep them; only readiness above the desired total
	// funds their removal.
	if t.fromReplicas-int64(t.in.From.Unassigned) < t.desiredReplicas {
		t.retirementAllowance = min(t.retirementAllowance, max(int64(0), t.servingReady-t.desiredReplicas))
	}
	t.readyAboveDesired = max(int64(0), t.servingReady+t.readyWithoutTarget-t.desiredReplicas)
	// A resumed original member still needs publication before it can back
	// retirement elsewhere. Only current serving capacity can fund a loss.
	t.servingAboveMinimum = max(int64(0), t.servingReady-min(t.fromReplicas, t.desiredReplicas))
	return nil
}

// countMember adds one member to the budget: its occupied replicas to the
// replicas in use, its retired and drained originals out of the retirement
// allowance, and its serving replicas above the original count into it.
func (t *transition) countMember(member memberView) error {
	t.targetsGrow = t.targetsGrow || member.goal > member.current
	t.targetsShrink = t.targetsShrink || member.goal < member.current
	occupied := int64(max(member.current, member.state.Occupied))
	if occupied > math.MaxInt64-t.inUse {
		return fmt.Errorf("occupied replicas exceed int64")
	}
	t.inUse += occupied
	t.retirementAllowance -= int64(max(int32(0), member.original-member.current))
	if member.state.Drained {
		t.retirementAllowance -= int64(min(member.original, member.current))
	}
	if member.state.serving() {
		t.servingReady += int64(min(member.state.Ready, member.current))
		t.retirementAllowance += int64(max(int32(0), min(member.state.Ready, member.current)-member.original))
	}
	if member.routableWithoutTarget() {
		t.readyWithoutTarget += int64(member.state.Ready)
	}
	return nil
}

// inconsistent reports an observation that cannot be true: a negative count,
// more ready than occupied, an absent member that is unknown or still
// occupied, or a drained member that still routes traffic.
func (s MemberState) inconsistent() bool {
	return s.Ready < 0 || s.Occupied < 0 || s.Ready > s.Occupied ||
		(s.Absent && (!s.Known || s.Occupied > 0)) || (s.Drained && s.Routable)
}

// serving reports a member whose target is applied and whose replicas take
// traffic, so its ready replicas count as serving capacity.
func (s MemberState) serving() bool {
	return s.Known && s.Applied && s.Routable
}

// routableWithoutTarget reports a routable member whose applied target is
// zero; its ready replicas still serve until it drains.
func (m memberView) routableWithoutTarget() bool {
	return m.current == 0 && m.state.Known && m.state.Routable
}

// unknownStanding reports a member that holds a target, an original target or
// replicas, and whose observation is missing.
func (m memberView) unknownStanding() bool {
	return !m.state.Known && (m.current > 0 || m.original > 0 || m.state.Occupied > 0)
}

// needsResume reports a drained member that both the applied and the desired
// plan keep; it must publish again before anything else moves.
func (m memberView) needsResume() bool {
	return m.goal > 0 && m.current > 0 && m.state.Drained
}

// aboveBothPlans reports an applied target above the original and the desired
// one: surplus no plan backs.
func (m memberView) aboveBothPlans() bool {
	return m.current > max(m.original, m.goal)
}

// occupiedWithoutTarget reports a member no plan targets that still holds
// undrained replicas.
func (m memberView) occupiedWithoutTarget() bool {
	return m.current == 0 && m.goal == 0 && m.state.Occupied > 0 && !m.state.Drained
}

// reducible reports a member above its goal whose applied target is known and
// in effect, so a reduction can be accounted.
func (m memberView) reducible() bool {
	return m.current > m.goal && m.state.Known && m.state.Applied
}

// growable reports a member below its goal that is known and eligible, and
// whose applied target, if it has one, is in effect.
func (m memberView) growable() bool {
	return m.goal > m.current && m.state.Known && m.state.Eligible &&
		(m.current == 0 || m.state.Applied)
}

// unsettled reports a member whose applied target is not yet in effect, or
// that still holds replicas under a zero target.
func (m memberView) unsettled() bool {
	return (m.current > 0 && (!m.state.Known || !m.state.Applied)) ||
		(m.current == 0 && m.state.Occupied > 0)
}

// decide names the step the move takes next, the first of these that applies:
//
//  1. ObservationUnknown: a standing member has no observation; nothing moves.
//  2. AwaitingMemberConvergence, resuming: a drained member both plans keep
//     publishes again before anything else moves.
//  3. AwaitingMemberConvergence, reducing: the drains and reductions the
//     allowances fund.
//  4. AwaitingMemberConvergence, growing: the targets the surge allowance funds.
//  5. ReplacementNotReady, or SurgeBudgetExhausted when growth could not be
//     funded: targets still differ from their goals and nothing moves yet.
//  6. AwaitingMemberConvergence: every target is at its goal, but a member has
//     not applied its target or has not emptied.
//  7. Complete.
func (t *transition) decide() (Step, error) {
	if t.hasUnknownStandingMember() {
		return t.step.withReason("ObservationUnknown"), nil
	}
	if t.resumeDrainedMembers() {
		return t.step.withReason("AwaitingMemberConvergence"), nil
	}
	t.planReductions()
	if t.movesReplicas() {
		return t.step.withReason("AwaitingMemberConvergence"), nil
	}
	if err := t.planGrowth(); err != nil {
		return Step{}, err
	}
	if t.movesReplicas() {
		return t.step.withReason("AwaitingMemberConvergence"), nil
	}
	if t.targetsGrow || t.targetsShrink {
		return t.step.withReason(t.waitingReason()), nil
	}
	if t.hasUnsettledMember() {
		return t.step.withReason("AwaitingMemberConvergence"), nil
	}
	t.step.Complete = true
	return t.step, nil
}

// withReason is the step with its reason set.
func (s Step) withReason(reason string) Step {
	s.Reason = reason
	return s
}

func (t *transition) hasUnknownStandingMember() bool {
	return slices.ContainsFunc(t.members, memberView.unknownStanding)
}

// resumeDrainedMembers lists the drained members both plans keep and reports
// whether there are any.
func (t *transition) resumeDrainedMembers() bool {
	for _, member := range t.members {
		if member.needsResume() {
			t.step.Resume = append(t.step.Resume, member.name)
		}
	}
	return len(t.step.Resume) > 0
}

// movesReplicas reports a step that drains a member or changes a target.
func (t *transition) movesReplicas() bool {
	return len(t.step.Drain) > 0 || !sameTargets(t.step.Targets, t.in.Current)
}

func (t *transition) hasUnsettledMember() bool {
	return slices.ContainsFunc(t.members, memberView.unsettled)
}

// waitingReason names why a move short of its goals waits: for budget when
// growth could not be funded, otherwise for replacements to serve.
func (t *transition) waitingReason() string {
	if t.targetsGrow && (t.growthAllowance == 0 || t.budgetBlocked) {
		return "SurgeBudgetExhausted"
	}
	return "ReplacementNotReady"
}

// planReductions takes members down toward their goals, surplus above both
// plans first: ready surplus may already back an earlier reduction and must
// keep that protection.
func (t *transition) planReductions() {
	for _, member := range t.reductionOrder() {
		t.planReduction(member)
	}
}

// reductionOrder is the members in name order, those above both plans first.
func (t *transition) reductionOrder() []memberView {
	ordered := slices.Clone(t.members)
	slices.SortStableFunc(ordered, func(left, right memberView) int {
		leftExtra, rightExtra := left.aboveBothPlans(), right.aboveBothPlans()
		switch {
		case leftExtra == rightExtra:
			return 0
		case leftExtra:
			return -1
		}
		return 1
	})
	return ordered
}

// planReduction takes one member toward its goal: a member no plan targets
// drains once the ready capacity elsewhere covers the desired total, an
// absent member's target drops to its goal at once, and any other member
// gives up what the allowances fund.
func (t *transition) planReduction(member memberView) {
	switch {
	case member.occupiedWithoutTarget():
		t.drainWithoutTarget(member)
	case member.state.Absent && member.current > member.goal:
		t.step.Targets[member.name] = member.goal
	case member.reducible():
		t.reduceFunded(member)
	}
}

// drainWithoutTarget drains a member no plan targets. A routable one waits
// until the ready capacity above the desired total covers its own ready
// replicas, and spends that capacity.
func (t *transition) drainWithoutTarget(member memberView) {
	if member.state.Routable {
		if int64(member.state.Ready) > t.readyAboveDesired {
			return
		}
		t.readyAboveDesired -= int64(member.state.Ready)
	}
	t.step.Drain = append(t.step.Drain, member.name)
}

// reduceFunded takes a member down by what its unbacked replicas and the
// retirement allowance fund. A serving member also needs serving capacity
// above the minimum for every ready replica it gives up. A member that
// empties drains instead of taking a zero target. A whole-member move takes
// no partial reduction.
func (t *transition) reduceFunded(member memberView) {
	unbacked := t.unbackedReplicas(member)
	reduce := int32(min(int64(member.current-member.goal), int64(unbacked)+max(int64(0), t.retirementAllowance)))
	var servingCost int64
	if member.state.Routable {
		unready := max(int32(0), member.current-member.state.Ready)
		reduce = int32(min(int64(reduce), int64(unready)+t.servingAboveMinimum))
		servingCost = int64(max(int32(0), reduce-unready))
	}
	if reduce == 0 || (t.wholeMembers && reduce < member.current-member.goal) {
		return
	}
	t.servingAboveMinimum -= servingCost
	t.retirementAllowance -= int64(max(int32(0), reduce-unbacked))
	if member.current == reduce && !member.state.Drained {
		t.step.Drain = append(t.step.Drain, member.name)
		return
	}
	t.step.Targets[member.name] -= reduce
}

// unbackedReplicas counts the replicas of a member that back nothing and can
// be cancelled without spending the allowances: all of a drained member's;
// while the member awaits publication, or under a whole-member move, those
// above the larger of its original count and its ready count. The ready
// replicas of a serving member back the minimum, so none of its are unbacked.
func (t *transition) unbackedReplicas(member memberView) int32 {
	switch {
	case member.state.Drained:
		return member.current
	case !member.state.Routable || member.state.Ready == 0 || t.wholeMembers:
		return max(int32(0), member.current-max(member.original, member.state.Ready))
	}
	return 0
}

// planGrowth raises members below their goals by what the surge allowance
// above the larger plan total leaves after the replicas in use. A
// whole-member move grows a member only by its whole shortfall. Otherwise,
// growth up to the replicas a serving member already has ready starts no
// replica, so it spends no allowance.
func (t *transition) planGrowth() error {
	limit := max(t.fromReplicas, t.desiredReplicas)
	if t.in.Surge > math.MaxInt64-limit {
		return fmt.Errorf("floor plus surge allowance exceeds int64")
	}
	t.growthAllowance = max(int64(0), limit+t.in.Surge-t.inUse)
	for _, member := range t.members {
		if !member.growable() {
			continue
		}
		var ready int32
		if !t.wholeMembers && member.state.serving() {
			ready = max(int32(0), min(member.goal, member.state.Ready)-member.current)
		}
		add := ready + int32(min(int64(member.goal-member.current-ready), t.growthAllowance))
		if t.wholeMembers && add < member.goal-member.current {
			t.budgetBlocked = true
			continue
		}
		t.step.Targets[member.name] += add
		t.growthAllowance -= int64(add - ready)
	}
	return nil
}

func planTotal(plan Plan) (int64, error) {
	if plan.Unassigned < 0 {
		return 0, fmt.Errorf("unassigned replicas must be nonnegative")
	}
	total := int64(plan.Unassigned)
	for name, count := range plan.Targets {
		if name == "" || count < 0 {
			return 0, fmt.Errorf("targets require nonempty cluster names and nonnegative counts")
		}
		if int64(count) > math.MaxInt64-total {
			return 0, fmt.Errorf("replica total exceeds int64")
		}
		total += int64(count)
	}
	return total, nil
}

func sameTargets(a, b map[string]int32) bool {
	for name, count := range a {
		if b[name] != count {
			return false
		}
	}
	for name, count := range b {
		if a[name] != count {
			return false
		}
	}
	return true
}
