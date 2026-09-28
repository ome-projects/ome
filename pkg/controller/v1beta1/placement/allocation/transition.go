package allocation

import (
	"fmt"
	"maps"
	"math"
	"slices"
)

// Home records observations belonging to the currently applied allocation.
type Home struct {
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
	Homes   map[string]Home
	// MaxSurge is the shared allowance above the larger of the original and
	// desired floors. Nil blocks moves between homes; zero permits no excess.
	MaxSurge *int32
	// RolloutReserved is additional rollout capacity already authorized but not
	// included in Occupied. It consumes the same allowance as placement growth.
	RolloutReserved int32
}

// Step is one persist-before-apply mutation. Reductions and growth are separate
// steps so a requested deletion cannot be mistaken for released resources.
type Step struct {
	// A zero target cannot authorize deletion of an occupied, undrained home.
	Targets  map[string]int32
	Drain    []string
	Resume   []string
	Reason   string
	Complete bool
}

// Advance moves toward Desired while retaining old replicas until replacement
// replicas are ready and routable. A zero target requires confirmed traffic
// drain before removal. Unknown standing homes hold the transition.
func Advance(in Transition) (Step, error) {
	fromFloor, err := planTotal(in.From)
	if err != nil {
		return Step{}, fmt.Errorf("original plan: %w", err)
	}
	desiredFloor, err := planTotal(in.Desired)
	if err != nil {
		return Step{}, fmt.Errorf("desired plan: %w", err)
	}
	if _, err := planTotal(Plan{Targets: in.Current}); err != nil {
		return Step{}, fmt.Errorf("current targets: %w", err)
	}
	if in.RolloutReserved < 0 || (in.MaxSurge != nil && *in.MaxSurge < 0) {
		return Step{}, fmt.Errorf("surge allowance and rollout reservation must be nonnegative")
	}
	all := maps.Clone(in.Desired.Targets)
	if all == nil {
		all = make(map[string]int32)
	}
	for name := range in.From.Targets {
		all[name] = in.Desired.Targets[name]
	}
	for name := range in.Current {
		all[name] = in.Desired.Targets[name]
	}
	for name := range in.Homes {
		if name == "" {
			return Step{}, fmt.Errorf("home observations require nonempty cluster names")
		}
		all[name] = in.Desired.Targets[name]
	}
	names := slices.Sorted(maps.Keys(all))
	out := Step{Targets: make(map[string]int32, len(names))}
	var grow, shrink bool
	var ready, orphanReady int64
	used := int64(in.RolloutReserved)
	credit := max(int64(0), fromFloor-desiredFloor)
	for _, name := range names {
		current, goal, original := in.Current[name], in.Desired.Targets[name], in.From.Targets[name]
		home := in.Homes[name]
		if home.Ready < 0 || home.Occupied < 0 || home.Ready > home.Occupied || (home.Absent && (!home.Known || home.Occupied > 0)) || (home.Drained && home.Routable) {
			return Step{}, fmt.Errorf("cluster %q has inconsistent ready/occupied observations", name)
		}
		out.Targets[name] = current
		grow = grow || goal > current
		shrink = shrink || goal < current
		occupied := int64(max(current, home.Occupied))
		if occupied > math.MaxInt64-used {
			return Step{}, fmt.Errorf("occupied replicas exceed int64")
		}
		used += occupied
		credit -= int64(max(int32(0), original-current))
		if home.Drained {
			credit -= int64(min(original, current))
		}
		if home.Known && home.Applied && home.Routable {
			ready += int64(min(home.Ready, current))
			credit += int64(max(int32(0), min(home.Ready, current)-original))
		}
		if current == 0 && home.Known && home.Routable {
			orphanReady += int64(home.Ready)
		}
	}
	// Replicas can serve before the first plan completes. Retargeting a growing
	// floor must protect them; only readiness above the desired floor funds
	// their removal.
	if fromFloor-int64(in.From.Unassigned) < desiredFloor {
		credit = min(credit, max(int64(0), ready-desiredFloor))
	}
	orphanCredit := max(int64(0), ready+orphanReady-desiredFloor)
	for _, name := range names {
		if !in.Homes[name].Known && (in.Current[name] > 0 || in.From.Targets[name] > 0 || in.Homes[name].Occupied > 0) {
			out.Reason = "ObservationUnknown"
			return out, nil
		}
	}
	if grow && shrink && in.MaxSurge == nil {
		out.Reason = "MigrationBlocked"
		return out, nil
	}
	for _, name := range names {
		if in.Desired.Targets[name] > 0 && in.Current[name] > 0 && in.Homes[name].Drained {
			out.Resume = append(out.Resume, name)
		}
	}
	if len(out.Resume) > 0 {
		out.Reason = "AwaitingMemberConvergence"
		return out, nil
	}
	// Cancel abandoned surplus before retiring original replicas. Ready surplus
	// may already back an earlier reduction and must retain that protection.
	reductions := slices.Clone(names)
	slices.SortStableFunc(reductions, func(a, b string) int {
		aExtra := in.Current[a] > max(in.From.Targets[a], in.Desired.Targets[a])
		bExtra := in.Current[b] > max(in.From.Targets[b], in.Desired.Targets[b])
		if aExtra == bExtra {
			return 0
		}
		if aExtra {
			return -1
		}
		return 1
	})
	for _, name := range reductions {
		home := in.Homes[name]
		current, goal, original := in.Current[name], in.Desired.Targets[name], in.From.Targets[name]
		if current == 0 && goal == 0 && home.Occupied > 0 && !home.Drained {
			if home.Routable {
				if int64(home.Ready) > orphanCredit {
					continue
				}
				orphanCredit -= int64(home.Ready)
			}
			out.Drain = append(out.Drain, name)
			continue
		}
		if current > goal && home.Absent {
			out.Targets[name] = goal
			continue
		}
		if current <= goal || !home.Known || !home.Applied {
			continue
		}
		var unbacked int32
		if home.Drained {
			unbacked = current
		} else if !home.Routable || home.Ready == 0 {
			unbacked = max(int32(0), current-original)
		}
		reduce := int32(min(int64(current-goal), int64(unbacked)+max(int64(0), credit)))
		if reduce == 0 {
			continue
		}
		spent := int64(max(int32(0), reduce-unbacked))
		if current == reduce && !home.Drained {
			out.Drain = append(out.Drain, name)
			credit -= spent
			continue
		}
		out.Targets[name] -= reduce
		credit -= spent
	}
	if len(out.Drain) > 0 || !sameTargets(out.Targets, in.Current) {
		out.Reason = "AwaitingMemberConvergence"
		return out, nil
	}
	limit := max(fromFloor, desiredFloor)
	if in.MaxSurge != nil {
		if int64(*in.MaxSurge) > math.MaxInt64-limit {
			return Step{}, fmt.Errorf("floor plus surge allowance exceeds int64")
		}
		limit += int64(*in.MaxSurge)
	}
	available := max(int64(0), limit-used)
	for _, name := range names {
		home := in.Homes[name]
		current, goal := in.Current[name], in.Desired.Targets[name]
		if goal <= current || !home.Known || !home.Eligible || (current > 0 && !home.Applied) {
			continue
		}
		add := int32(min(int64(goal-current), available))
		out.Targets[name] += add
		available -= int64(add)
	}
	if !sameTargets(out.Targets, in.Current) {
		out.Reason = "AwaitingMemberConvergence"
		return out, nil
	}
	if grow || shrink {
		out.Reason = "ReplacementNotReady"
		if grow && available == 0 {
			out.Reason = "SurgeBudgetExhausted"
		}
		return out, nil
	}
	for _, name := range names {
		home := in.Homes[name]
		if (in.Current[name] > 0 && (!home.Known || !home.Applied)) || (in.Current[name] == 0 && home.Occupied > 0) {
			out.Reason = "AwaitingMemberConvergence"
			return out, nil
		}
	}
	out.Complete = true
	return out, nil
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
