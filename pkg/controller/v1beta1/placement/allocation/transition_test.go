package allocation

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func serving(count int32) MemberState {
	return MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: count, Occupied: count}
}

func moving() Transition {
	budget := int32(1)
	return Transition{
		From: Plan{Targets: map[string]int32{"a": 4}}, Current: map[string]int32{"a": 4},
		Desired: Plan{Targets: map[string]int32{"b": 4}}, Surge: int64(budget),
		Members: map[string]MemberState{"a": serving(4), "b": {Known: true, Eligible: true}},
	}
}

func advance(t *testing.T, in Transition) Step {
	t.Helper()
	before := maps.Clone(in.Current)
	step, err := Advance(in)
	require.NoError(t, err)
	if diff := cmp.Diff(before, in.Current); diff != "" {
		t.Fatalf("planning must not mutate persisted targets (-want +got):\n%s", diff)
	}
	return step
}

func TestTransitionWaitsForReadyRoutableReplacementAndRemoval(t *testing.T) {
	in := moving()
	for next := int32(1); next <= 4; next++ {
		step := advance(t, in)
		if diff := cmp.Diff(map[string]int32{"a": 5 - next, "b": next}, step.Targets); diff != "" {
			t.Fatalf("result mismatch (-want +got):\n%s", diff)
		}
		in.Current = step.Targets
		in.Members["b"] = MemberState{Known: true, Eligible: true, Applied: true, Occupied: next}
		step = advance(t, in)
		if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
			t.Fatalf("admission without readiness cannot release the donor (-want +got):\n%s", diff)
		}
		in.Members["b"] = MemberState{Known: true, Eligible: true, Applied: true, Occupied: next, Ready: next}
		step = advance(t, in)
		if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
			t.Fatalf("readiness without routability cannot release the donor (-want +got):\n%s", diff)
		}
		in.Members["b"] = serving(next)
		step = advance(t, in)
		if next == 4 {
			if diff := cmp.Diff([]string{"a"}, step.Drain); diff != "" {
				t.Fatalf("result mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
				t.Fatalf("traffic must drain before a zero-target member is removed (-want +got):\n%s", diff)
			}
			member := in.Members["a"]
			member.Drained = true
			member.Routable = false
			in.Members["a"] = member
			step = advance(t, in)
		}
		if diff := cmp.Diff(map[string]int32{"a": 4 - next, "b": next}, step.Targets); diff != "" {
			t.Fatalf("result mismatch (-want +got):\n%s", diff)
		}
		in.Current = step.Targets
		step = advance(t, in)
		if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
			t.Fatalf("a requested reduction is not released capacity (-want +got):\n%s", diff)
		}
		require.False(t, step.Complete)
		in.Members["a"] = serving(4 - next)
	}
	step := advance(t, in)
	require.True(t, step.Complete)
	if diff := cmp.Diff(int32(4), in.From.Targets["a"]); diff != "" {
		t.Fatalf("the original allocation survives the entire move (-want +got):\n%s", diff)
	}
}

func TestTransitionSurgeIsSharedWithRollout(t *testing.T) {
	tests := []struct {
		name      string
		allowance int64
		rollout   int32
		want      Step
	}{
		{name: "rollout consumes allowance", allowance: 1, rollout: 1,
			want: Step{Targets: map[string]int32{"a": 4, "b": 0}, Reason: "SurgeBudgetExhausted"}},
		{name: "released allowance permits growth", allowance: 1,
			want: Step{Targets: map[string]int32{"a": 4, "b": 1}, Reason: "AwaitingMemberConvergence"}},
		{name: "zero forbids excess", allowance: 0,
			want: Step{Targets: map[string]int32{"a": 4, "b": 0}, Reason: "SurgeBudgetExhausted"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := moving()
			in.Surge, in.RolloutReserved = tt.allowance, tt.rollout
			got := advance(t, in)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("step mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTransitionRetargetDoesNotManufactureSurge(t *testing.T) {
	in := moving()
	in.Current = advance(t, in).Targets
	in.Members["b"] = serving(1)
	in.Desired = Plan{Targets: map[string]int32{"c": 4}}
	in.Members["c"] = MemberState{Known: true, Eligible: true}
	step := advance(t, in)
	if diff := cmp.Diff([]string{"b"}, step.Drain); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	compareTargets(t, in.Current, step.Targets)
	in.Members["b"] = MemberState{Known: true, Applied: true, Drained: true, Ready: 1, Occupied: 1}
	step = advance(t, in)
	if diff := cmp.Diff(map[string]int32{"a": 4, "b": 0, "c": 0}, step.Targets); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	in.Current = step.Targets
	step = advance(t, in)
	require.Zero(t, step.Targets["c"], "a terminating surplus still occupies the allowance")
	in.Members["b"] = MemberState{Known: true, Absent: true}
	step = advance(t, in)
	if diff := cmp.Diff(int32(1), step.Targets["c"]); diff != "" {
		t.Fatalf("observed removal permits progress to the new destination (-want +got):\n%s", diff)
	}
}

func TestTransitionRetargetPreservesEarlierReplacement(t *testing.T) {
	in := moving()
	in.Current = map[string]int32{"a": 3, "b": 1}
	in.Members["a"], in.Members["b"] = serving(3), serving(1)
	in.Desired = Plan{Targets: map[string]int32{"c": 4}}
	in.Members["c"] = MemberState{Known: true, Eligible: true}
	step := advance(t, in)
	require.Empty(t, step.Drain, "the abandoned destination still backs an original replica")
	if diff := cmp.Diff(map[string]int32{"a": 3, "b": 1, "c": 1}, step.Targets); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	in.Current = step.Targets
	in.Members["c"] = serving(1)
	step = advance(t, in)
	if diff := cmp.Diff([]string{"b"}, step.Drain); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(int32(3), step.Targets["a"]); diff != "" {
		t.Fatalf("drain and reduction cannot spend the same replacement (-want +got):\n%s", diff)
	}
}

func TestTransitionCancelsUnadmittedSurplus(t *testing.T) {
	in := moving()
	in.Current["b"] = 1
	in.Members["b"] = MemberState{Known: true, Applied: true, Occupied: 1}
	in.Desired = Plan{Targets: map[string]int32{"c": 4}}
	in.Members["c"] = MemberState{Known: true, Eligible: true}
	step := advance(t, in)
	if diff := cmp.Diff([]string{"b"}, step.Drain); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	member := in.Members["b"]
	member.Drained = true
	in.Members["b"] = member
	step = advance(t, in)
	if diff := cmp.Diff(map[string]int32{"a": 4, "b": 0, "c": 0}, step.Targets); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestTransitionUnknownMemberPreservesAllocations(t *testing.T) {
	in := moving()
	in.Members["a"] = MemberState{}
	step := advance(t, in)
	compareTargets(t, in.Current, step.Targets)
	if diff := cmp.Diff("ObservationUnknown", step.Reason); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	in.Members["a"] = MemberState{Known: true, Absent: true}
	step = advance(t, in)
	require.Zero(t, step.Targets["a"], "authoritative absence can release the old request")
	require.Zero(t, step.Targets["b"], "release and growth remain separate steps")
	in.Current = step.Targets
	step = advance(t, in)
	if diff := cmp.Diff(int32(4), step.Targets["b"]); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestTransitionInitialPlacementAndExplicitScaleDown(t *testing.T) {
	in := Transition{
		Desired: Plan{Targets: map[string]int32{"a": 2, "b": 2, "c": 0}},
		Members: map[string]MemberState{"a": {Known: true, Eligible: true}, "b": {Known: true, Eligible: true}},
	}
	step := advance(t, in)
	if diff := cmp.Diff(in.Desired.Targets, step.Targets); diff != "" {
		t.Fatalf("initial placement needs no migration surplus (-want +got):\n%s", diff)
	}
	in.Current = step.Targets
	in.Members["a"], in.Members["b"] = serving(2), serving(2)
	require.True(t, advance(t, in).Complete)
	in.From = in.Desired
	in.Desired = Plan{Targets: map[string]int32{"a": 1, "b": 1}}
	step = advance(t, in)
	if diff := cmp.Diff(map[string]int32{"a": 1, "b": 1, "c": 0}, step.Targets); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	in.Current = step.Targets
	step = advance(t, in)
	require.True(t, step.Complete, "allocation completion is distinct from member admission")
}

func TestTransitionUnassignedFloorRetainsExistingMember(t *testing.T) {
	in := moving()
	in.Desired = Plan{Unassigned: 4}
	step := advance(t, in)
	compareTargets(t, in.Current, step.Targets)
	require.Empty(t, step.Drain)
	require.False(t, step.Complete)
}

func TestTransitionDrainCreditCannotBeSpentTwice(t *testing.T) {
	budget := int32(1)
	in := Transition{
		From:    Plan{Targets: map[string]int32{"a": 1, "b": 1}},
		Current: map[string]int32{"a": 1, "b": 1, "c": 1},
		Desired: Plan{Targets: map[string]int32{"c": 2}}, Surge: int64(budget),
		Members: map[string]MemberState{"a": serving(1), "b": serving(1), "c": serving(1)},
	}
	step := advance(t, in)
	if diff := cmp.Diff([]string{"a"}, step.Drain); diff != "" {
		t.Fatalf("one ready replacement cannot drain two old members (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestTransitionWaitsForEligibilityAndAppliedTargets(t *testing.T) {
	in := moving()
	in.Members["b"] = MemberState{Known: true}
	step := advance(t, in)
	compareTargets(t, in.Current, step.Targets)
	in.Current["b"] = 1
	in.Members["b"] = MemberState{Known: true, Eligible: true, Occupied: 1, Ready: 1, Routable: true}
	step = advance(t, in)
	if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
		t.Fatalf("an observation from another applied plan cannot fund removal (-want +got):\n%s", diff)
	}
	in.Desired = Plan{Targets: maps.Clone(in.Current)}
	require.False(t, advance(t, in).Complete, "a member must acknowledge its persisted request")
}

func TestTransitionResumedMemberWaitsForServingReplacement(t *testing.T) {
	for _, planner := range []struct {
		name    string
		advance func(Transition) (Step, error)
	}{
		{name: "replica steps", advance: Advance},
		{name: "whole members", advance: AdvanceWholeMembers},
	} {
		t.Run(planner.name, func(t *testing.T) {
			for _, tt := range []struct {
				name    string
				applied bool
				routed  bool
				want    Step
			}{
				{name: "awaiting publication", applied: true,
					want: Step{Targets: map[string]int32{"a": 4, "b": 4}, Reason: "ReplacementNotReady"}},
				{name: "awaiting application", routed: true,
					want: Step{Targets: map[string]int32{"a": 4, "b": 4}, Reason: "ReplacementNotReady"}},
				{name: "verified serving", applied: true, routed: true,
					want: Step{Targets: map[string]int32{"a": 4, "b": 4}, Drain: []string{"b"}, Reason: "AwaitingMemberConvergence"}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					in := moving()
					in.Current["b"], in.Members["b"] = 4, serving(4)
					in.Desired = Plan{Targets: map[string]int32{"a": 4}}
					in.Members["a"] = MemberState{Known: true, Applied: tt.applied, Routable: tt.routed, Ready: 4, Occupied: 4}
					got, err := planner.advance(in)
					require.NoError(t, err)
					if diff := cmp.Diff(tt.want, got); diff != "" {
						t.Fatalf("step (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestTransitionUnpublishedReadyMemberIsNotSurplus(t *testing.T) {
	for _, planner := range []struct {
		name    string
		advance func(Transition) (Step, error)
	}{
		{name: "replica steps", advance: Advance},
		{name: "whole members", advance: AdvanceWholeMembers},
	} {
		t.Run(planner.name, func(t *testing.T) {
			// The first plan never completed, so the serving member has no original
			// floor. Losing its publication must not turn it into surplus.
			in := Transition{
				Current: map[string]int32{"a": 3, "b": 3}, Desired: Plan{Targets: map[string]int32{"b": 3}}, Surge: 3,
				Members: map[string]MemberState{
					"a": {Known: true, Applied: true, Ready: 3, Occupied: 3},
					"b": {Known: true, Eligible: true, Applied: true, Occupied: 3},
				},
			}
			got, err := planner.advance(in)
			require.NoError(t, err)
			want := Step{Targets: map[string]int32{"a": 3, "b": 3}, Reason: "ReplacementNotReady"}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("unpublished member (-want +got):\n%s", diff)
			}
			in.Members["b"] = serving(3)
			got, err = planner.advance(in)
			require.NoError(t, err)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("serving replacement cannot retire an unpublished member (-want +got):\n%s", diff)
			}
			in.Members["a"] = serving(3)
			got, err = planner.advance(in)
			require.NoError(t, err)
			want.Drain, want.Reason = []string{"a"}, "AwaitingMemberConvergence"
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("published serving replacement (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTransitionAccountsForUnplannedOccupancy(t *testing.T) {
	in := moving()
	in.Members["c"] = serving(1)
	step := advance(t, in)
	require.Zero(t, step.Targets["b"], "owned instances outside the plan still consume surge")
	require.Contains(t, step.Targets, "c", "an unplanned member must receive a zero target")
	if diff := cmp.Diff([]string{"c"}, step.Drain); diff != "" {
		t.Fatalf("unplanned member must drain (-want +got):\n%s", diff)
	}
	in.Members["c"] = MemberState{Occupied: 1}
	if diff := cmp.Diff("ObservationUnknown", advance(t, in).Reason); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestTransitionProtectsReplicasDuringInitialRetarget(t *testing.T) {
	tests := []struct {
		name    string
		from    Plan
		current map[string]int32
		members map[string]MemberState
		want    map[string]int32
	}{
		{name: "first full allocation", current: map[string]int32{"a": 4}, members: map[string]MemberState{"a": serving(4)}, want: map[string]int32{"a": 4, "b": 1}},
		{name: "first serving replica", current: map[string]int32{"a": 1}, members: map[string]MemberState{"a": serving(1)}, want: map[string]int32{"a": 1, "b": 4}},
		{name: "previously unassigned floor", from: Plan{Unassigned: 4}, current: map[string]int32{"a": 4}, members: map[string]MemberState{"a": serving(4)}, want: map[string]int32{"a": 4, "b": 1}},
		{name: "unplanned serving replica", members: map[string]MemberState{"a": serving(1)}, want: map[string]int32{"a": 0, "b": 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := moving()
			in.From, in.Current, in.Members = tt.from, tt.current, tt.members
			in.Members["b"] = MemberState{Known: true, Eligible: true}
			got := advance(t, in)
			want := Step{Targets: tt.want, Reason: "AwaitingMemberConvergence"}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Fatalf("retarget must grow before draining (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTransitionRejectsInvalidAccounting(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Transition)
	}{
		{"negative unassigned", func(in *Transition) { in.From.Unassigned = -1 }},
		{"negative desired", func(in *Transition) { in.Desired.Targets["b"] = -1 }},
		{"negative current", func(in *Transition) { in.Current["a"] = -1 }},
		{"empty target name", func(in *Transition) { in.Current[""] = 1 }},
		{"negative allowance", func(in *Transition) { in.Surge = -1 }},
		{"negative rollout reservation", func(in *Transition) { in.RolloutReserved = -1 }},
		{"ready exceeds occupied", func(in *Transition) { in.Members["a"] = MemberState{Known: true, Ready: 5, Occupied: 4} }},
		{"absent but occupied", func(in *Transition) { in.Members["a"] = MemberState{Known: true, Absent: true, Occupied: 1} }},
		{"drained but routable", func(in *Transition) { in.Members["a"] = MemberState{Known: true, Drained: true, Routable: true} }},
		{"empty member name", func(in *Transition) { in.Members[""] = MemberState{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, planner := range []struct {
				name    string
				advance func(Transition) (Step, error)
			}{
				{name: "replica steps", advance: Advance},
				{name: "whole members", advance: AdvanceWholeMembers},
			} {
				t.Run(planner.name, func(t *testing.T) {
					in := moving()
					tt.change(&in)
					got, err := planner.advance(in)
					require.Error(t, err)
					if diff := cmp.Diff(Step{}, got); diff != "" {
						t.Fatalf("invalid accounting returned a step (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

func TestTransitionsConvergeWithinSharedBudget(t *testing.T) {
	names := []string{"a", "b", "c", "d"}
	for scenario := range 1000 {
		t.Run(fmt.Sprintf("scenario_%04d", scenario), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(scenario), 31))
			partition := func(total int32) Plan {
				plan := Plan{Targets: map[string]int32{}}
				for range total {
					plan.Targets[names[rng.IntN(len(names))]]++
				}
				return plan
			}
			floor := int32(1 + rng.IntN(30))
			budget := int32(1 + rng.IntN(5))
			in := Transition{From: partition(floor), Desired: partition(floor), Surge: int64(budget),
				RolloutReserved: int32(rng.IntN(int(budget))), Members: map[string]MemberState{}}
			in.Current = maps.Clone(in.From.Targets)
			if scenario%2 == 0 {
				in.From = Plan{Unassigned: floor}
			}
			for _, name := range names {
				in.Members[name] = serving(in.Current[name])
			}
			complete := false
			for iteration := range 256 {
				if iteration == 3 || iteration == 7 {
					in.Desired = partition(floor)
				}
				step := advance(t, in)
				for _, name := range step.Resume {
					member := in.Members[name]
					member.Drained, member.Routable = false, true
					in.Members[name] = member
				}
				var requested, ready int32
				for _, name := range names {
					requested += step.Targets[name]
					member := in.Members[name]
					if member.Routable {
						ready += min(member.Ready, step.Targets[name])
					}
				}
				for _, name := range step.Drain {
					member := in.Members[name]
					if member.Routable {
						ready -= min(member.Ready, step.Targets[name])
					}
					member.Drained, member.Routable = true, false
					in.Members[name] = member
				}
				require.LessOrEqual(t, requested+in.RolloutReserved, floor+budget, "scenario %d iteration %d", scenario, iteration)
				require.GreaterOrEqual(t, ready, floor, "a move or drain must retain the serving floor: scenario %d iteration %d: input %+v step %+v", scenario, iteration, in, step)
				if step.Complete {
					compareTargets(t, in.Desired.Targets, step.Targets)
					complete = true
					break
				}
				require.True(t, len(step.Drain) > 0 || len(step.Resume) > 0 || !cmp.Equal(step.Targets, in.Current), "transition stalled: scenario %d iteration %d: %+v", scenario, iteration, in)
				for _, name := range names {
					if step.Targets[name] != in.Current[name] {
						in.Members[name] = serving(step.Targets[name])
					}
				}
				in.Current = step.Targets
			}
			require.True(t, complete, "scenario %d did not converge", scenario)
		})
	}
}

func compareTargets(t *testing.T, want, got map[string]int32) {
	t.Helper()
	want, got = maps.Clone(want), maps.Clone(got)
	maps.DeleteFunc(want, func(_ string, value int32) bool { return value == 0 })
	maps.DeleteFunc(got, func(_ string, value int32) bool { return value == 0 })
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("target mismatch (-want +got):\n%s", diff)
	}
}

func TestMemberViewPredicatesReadOneCluster(t *testing.T) {
	applied := MemberState{Known: true, Eligible: true, Applied: true}
	for _, tt := range []struct {
		name   string
		member memberView
		want   map[string]bool
	}{
		{"an unknown member with an original target is standing", memberView{original: 2}, map[string]bool{"unknownStanding": true}},
		{"an unknown member with nothing is not standing", memberView{}, map[string]bool{}},
		{"a drained member both plans keep resumes", memberView{current: 2, goal: 2, state: MemberState{Known: true, Drained: true}}, map[string]bool{"needsResume": true, "unsettled": true, "stillGrows": true}},
		{"a target above both plans is surplus", memberView{original: 1, current: 3, goal: 2, state: applied}, map[string]bool{"aboveBothPlans": true, "reducible": true, "stillGrows": true}},
		{"occupied replicas under no target must drain", memberView{state: MemberState{Known: true, Occupied: 2}}, map[string]bool{"occupiedWithoutTarget": true, "unsettled": true}},
		{"a ready routable member under a zero target still serves", memberView{state: MemberState{Known: true, Routable: true, Ready: 1, Occupied: 1}}, map[string]bool{"routableWithoutTarget": true, "occupiedWithoutTarget": true, "unsettled": true}},
		{"an empty eligible member grows without an applied target", memberView{goal: 3, state: MemberState{Known: true, Eligible: true}}, map[string]bool{"growable": true, "stillGrows": true}},
		{"a member with a target grows only once it is applied", memberView{current: 1, goal: 3, state: MemberState{Known: true, Eligible: true}}, map[string]bool{"unsettled": true, "stillGrows": true}},
		{"a member below its goal grows even with enough ready replicas", memberView{original: 2, current: 2, goal: 3, state: serving(4)}, map[string]bool{"growable": true, "stillGrows": true}},
		{"a member at its goal grows until it serves the goal", memberView{original: 6, current: 6, goal: 6, state: MemberState{Known: true, Applied: true, Ready: 6, Occupied: 6}}, map[string]bool{"stillGrows": true}},
		{"a member at its goal that serves it is done growing", memberView{original: 6, current: 6, goal: 6, state: serving(6)}, map[string]bool{}},
		{"a member with too few ready replicas for its goal still grows", memberView{original: 2, current: 6, goal: 4, state: MemberState{Known: true, Applied: true, Routable: true, Ready: 3, Occupied: 6}}, map[string]bool{"aboveBothPlans": true, "reducible": true, "stillGrows": true, "trimmable": true}},
		{"a member that shrinks below its original count does not grow", memberView{original: 6, current: 6, goal: 3, state: MemberState{Known: true, Applied: true, Routable: true, Ready: 2, Occupied: 6}}, map[string]bool{"reducible": true, "trimmable": true}},
		{"a member that shrinks back to its original count does not grow", memberView{original: 4, current: 6, goal: 4, state: MemberState{Known: true, Applied: true, Routable: true, Ready: 3, Occupied: 6}}, map[string]bool{"aboveBothPlans": true, "reducible": true, "trimmable": true}},
		{"a member with no ready replica is not trimmed", memberView{original: 6, current: 6, goal: 3, state: MemberState{Known: true, Applied: true, Routable: true, Occupied: 6}}, map[string]bool{"reducible": true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := map[string]bool{
				"unknownStanding":       tt.member.unknownStanding(),
				"needsResume":           tt.member.needsResume(),
				"aboveBothPlans":        tt.member.aboveBothPlans(),
				"occupiedWithoutTarget": tt.member.occupiedWithoutTarget(),
				"routableWithoutTarget": tt.member.routableWithoutTarget(),
				"reducible":             tt.member.reducible(),
				"growable":              tt.member.growable(),
				"unsettled":             tt.member.unsettled(),
				"stillGrows":            tt.member.stillGrows(),
				"trimmable":             tt.member.trimmable(),
			}
			for name, value := range got {
				if value != tt.want[name] {
					t.Errorf("%s = %v, want %v", name, value, tt.want[name])
				}
			}
		})
	}
}

// TestTransitionReadyReplicasCoverGrowth raises both members' targets from 6 to
// 8 while each already serves 16 ready replicas, so the replicas in use leave
// no surge allowance. A move that is not whole-member raises the targets anyway,
// because no replica starts. A whole-member move still waits for budget, and so
// does growth on a member that does not serve.
func TestTransitionReadyReplicasCoverGrowth(t *testing.T) {
	in := Transition{
		From: Plan{Targets: map[string]int32{"a": 6, "b": 6}}, Current: map[string]int32{"a": 6, "b": 6},
		Desired: Plan{Targets: map[string]int32{"a": 8, "b": 8}}, Surge: 16,
		Members: map[string]MemberState{"a": serving(16), "b": serving(16)},
	}
	if diff := cmp.Diff(Step{Targets: map[string]int32{"a": 8, "b": 8}, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("ready replicas must cover the growth (-want +got):\n%s", diff)
	}
	whole, err := AdvanceWholeMembers(in)
	require.NoError(t, err)
	if diff := cmp.Diff(Step{Targets: map[string]int32{"a": 6, "b": 6}, Reason: "SurgeBudgetExhausted"}, whole); diff != "" {
		t.Fatalf("a whole-member move must wait for budget (-want +got):\n%s", diff)
	}
	in.Members["b"] = MemberState{Known: true, Eligible: true, Applied: true, Ready: 16, Occupied: 16}
	if diff := cmp.Diff(Step{Targets: map[string]int32{"a": 8, "b": 6}, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("a member that does not serve must not cover its growth (-want +got):\n%s", diff)
	}
}

// TestTransitionGrowingTotalReleasesUnreadyReplicasOfAShrinkingMember grows the
// total of three members from 8 to 9 replicas. Member c's target falls from 3
// to 1 while only 1 of its 3 replicas is ready. Member b's falls from 3 to 2
// with all 3 ready, and only 5 of member a's 6 are ready. Member d has no
// target. The ready replicas add up to exactly 9, so no ready replica may leave
// yet. c also keeps its 2 replicas that are not ready while a still has a
// replica to make ready, because a replica that OME does not count as ready can
// still serve. Once a serves all 6, b gives up 1 ready replica. Then c gives up
// its 2 replicas that are not ready, one per pass, because only 1 ready replica
// is above the old total of 8.
func TestTransitionGrowingTotalReleasesUnreadyReplicasOfAShrinkingMember(t *testing.T) {
	in := Transition{
		From:    Plan{Targets: map[string]int32{"a": 2, "b": 3, "c": 3, "d": 0}},
		Current: map[string]int32{"a": 6, "b": 3, "c": 3, "d": 0},
		Desired: Plan{Targets: map[string]int32{"a": 6, "b": 2, "c": 1, "d": 0}}, Surge: 9,
		Members: map[string]MemberState{
			"a": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 5, Occupied: 6},
			"b": serving(3),
			"c": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 1, Occupied: 3},
			"d": {Known: true, Eligible: true},
		},
	}
	if diff := cmp.Diff(Step{Targets: map[string]int32{"a": 6, "b": 3, "c": 3, "d": 0}, Reason: "ReplacementNotReady"}, advance(t, in)); diff != "" {
		t.Fatalf("every member must keep its replicas while a still has one to make ready (-want +got):\n%s", diff)
	}
	in.Members["a"] = serving(6)
	want := map[string]int32{"a": 6, "b": 2, "c": 3, "d": 0}
	if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("once a serves all 6, b must give up one ready replica (-want +got):\n%s", diff)
	}
	in.Current = want
	in.Members["b"] = serving(2)
	for _, c := range []int32{2, 1} {
		want = map[string]int32{"a": 6, "b": 2, "c": c, "d": 0}
		if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
			t.Fatalf("c must give up one replica that is not ready per pass (-want +got):\n%s", diff)
		}
		in.Current = want
		in.Members["c"] = MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 1, Occupied: c}
	}
	require.True(t, advance(t, in).Complete)
}

// TestTransitionShrinkingMemberGivesUpReplicasThatAreNotReady lowers member a
// from 4 replicas to 2 and grows member b from 0 to 4, so the total grows from
// 4 to 6. Only 2 of a's replicas are ready, for example because the other 2
// crash-loop. a keeps all 4 while b grows and starts. Once b serves all 4, a's
// 2 ready replicas are exactly what the new total still needs. The ready
// replicas above the old total of 4 cover a's 2 replicas that are not ready, so
// a gives them up at once. The same holds in a first plan, which is a move that
// starts before the service's first placement has finished, so a has no
// original count.
func TestTransitionShrinkingMemberGivesUpReplicasThatAreNotReady(t *testing.T) {
	for _, tt := range []struct {
		name string
		from Plan
	}{
		{name: "after an earlier plan", from: Plan{Targets: map[string]int32{"a": 4}}},
		{name: "first plan", from: Plan{Unassigned: 4}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := Transition{
				From: tt.from, Current: map[string]int32{"a": 4},
				Desired: Plan{Targets: map[string]int32{"a": 2, "b": 4}}, Surge: 6,
				Members: map[string]MemberState{
					"a": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 2, Occupied: 4},
					"b": {Known: true, Eligible: true},
				},
			}
			want := map[string]int32{"a": 4, "b": 4}
			if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
				t.Fatalf("b must grow while a keeps all its replicas (-want +got):\n%s", diff)
			}
			in.Current = want
			in.Members["b"] = MemberState{Known: true, Eligible: true, Applied: true, Occupied: 4}
			if diff := cmp.Diff(Step{Targets: want, Reason: "ReplacementNotReady"}, advance(t, in)); diff != "" {
				t.Fatalf("a must keep all its replicas while b's do not serve (-want +got):\n%s", diff)
			}
			in.Members["b"] = serving(4)
			want = map[string]int32{"a": 2, "b": 4}
			if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
				t.Fatalf("once b serves, a must give up its replicas that are not ready (-want +got):\n%s", diff)
			}
			in.Current = want
			in.Members["a"] = serving(2)
			require.True(t, advance(t, in).Complete)
		})
	}
}

// TestTransitionShrinkingMemberKeepsReadyReplicasTheNewTotalNeeds grows member
// a from 3 replicas to 9. It lowers member b from 6 to 3 and member c from 4 to
// 3, so the total grows from 13 to 15. a serves all 9. 4 of b's 6 replicas are
// ready, and 2 of c's 4. The ready replicas add up to exactly 15, which is 2
// above the old total of 13. b gives up its 2 replicas that are not ready, and
// that uses up those 2. c gives up its replica above its new target in the
// next pass. b keeps all 4 ready replicas, because the new total needs them.
func TestTransitionShrinkingMemberKeepsReadyReplicasTheNewTotalNeeds(t *testing.T) {
	in := Transition{
		From:    Plan{Targets: map[string]int32{"a": 3, "b": 6, "c": 4}},
		Current: map[string]int32{"a": 9, "b": 6, "c": 4},
		Desired: Plan{Targets: map[string]int32{"a": 9, "b": 3, "c": 3}}, Surge: 15,
		Members: map[string]MemberState{
			"a": serving(9),
			"b": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 4, Occupied: 6},
			"c": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 2, Occupied: 4},
		},
	}
	want := map[string]int32{"a": 9, "b": 4, "c": 4}
	if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("b must give up only its replicas that are not ready (-want +got):\n%s", diff)
	}
	in.Current = want
	in.Members["b"] = serving(4)
	want = map[string]int32{"a": 9, "b": 4, "c": 3}
	if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("c must give up its replica above its new target (-want +got):\n%s", diff)
	}
	in.Current = want
	in.Members["c"] = MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 2, Occupied: 3}
	if diff := cmp.Diff(Step{Targets: want, Reason: "ReplacementNotReady"}, advance(t, in)); diff != "" {
		t.Fatalf("b must keep its ready replicas while the new total needs them (-want +got):\n%s", diff)
	}
}

// TestTransitionUnreadyReplicasLeaveAtOnceOnlyFromAServingMember lowers member
// a from 4 replicas to 2 and grows member b from 0 to 5, so the total grows
// from 4 to 7. b already runs its 5 replicas. Only 2 of a's replicas are ready,
// and those 2 are exactly what the new total still needs. a gives up its other
// 2 at once when b serves and a takes traffic with a ready replica. a keeps all
// 4 while b's replicas are not ready or b's traffic is not published yet, while
// a's own traffic is not published, under a whole-member move, and when none of
// a's replicas is ready. In the last case a fault across a whole cluster, such
// as a router outage, can hide replicas that come back.
func TestTransitionUnreadyReplicasLeaveAtOnceOnlyFromAServingMember(t *testing.T) {
	twoReady := MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 2, Occupied: 4}
	unpublished, noneReady := twoReady, twoReady
	unpublished.Routable = false
	noneReady.Ready = 0
	starting := MemberState{Known: true, Eligible: true, Applied: true, Occupied: 5}
	unpublishedReplacements := MemberState{Known: true, Eligible: true, Applied: true, Ready: 5, Occupied: 5}
	released := Step{Targets: map[string]int32{"a": 2, "b": 5}, Reason: "AwaitingMemberConvergence"}
	kept := Step{Targets: map[string]int32{"a": 4, "b": 5}, Reason: "ReplacementNotReady"}
	for _, tt := range []struct {
		name    string
		advance func(Transition) (Step, error)
		a, b    MemberState
		want    Step
	}{
		{name: "serving member", advance: Advance, a: twoReady, b: serving(5), want: released},
		{name: "replacements not serving", advance: Advance, a: twoReady, b: starting, want: kept},
		{name: "replacements not published", advance: Advance, a: twoReady, b: unpublishedReplacements, want: kept},
		{name: "traffic not published", advance: Advance, a: unpublished, b: serving(5), want: kept},
		{name: "whole-member move", advance: AdvanceWholeMembers, a: twoReady, b: serving(5), want: kept},
		{name: "no ready replica", advance: Advance, a: noneReady, b: serving(5), want: kept},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.advance(Transition{
				From: Plan{Targets: map[string]int32{"a": 4}}, Current: map[string]int32{"a": 4, "b": 5},
				Desired: Plan{Targets: map[string]int32{"a": 2, "b": 5}}, Surge: 7,
				Members: map[string]MemberState{"a": tt.a, "b": tt.b},
			})
			require.NoError(t, err)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("a's replicas that are not ready (-want +got):\n%s", diff)
			}
		})
	}
}

// TestTransitionRetargetKeepsUnreadyReplicasWhileAMemberGrowsBack covers a move
// whose plan changed after member a had shrunk from 4 replicas to 2 and member
// c had grown from 0 to 4. The new plan raises a back to 4, keeps c at 4, and
// lowers member b from 4 to 2 while 3 of b's replicas are ready. The total grows
// from 8 to 10, and the ready replicas are 1 above the old total of 8. Still b
// keeps its replica that is not ready while a's target is below 4, and while
// a's new replicas are not ready. Once a serves all 4, b gives up that replica,
// and then a ready one.
func TestTransitionRetargetKeepsUnreadyReplicasWhileAMemberGrowsBack(t *testing.T) {
	in := Transition{
		From:    Plan{Targets: map[string]int32{"a": 4, "b": 4}},
		Current: map[string]int32{"a": 2, "b": 4, "c": 4},
		Desired: Plan{Targets: map[string]int32{"a": 4, "b": 2, "c": 4}}, Surge: 10,
		Members: map[string]MemberState{
			"a": serving(2),
			"b": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: 4},
			"c": serving(4),
		},
	}
	want := map[string]int32{"a": 4, "b": 4, "c": 4}
	if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("a must grow back while b keeps all its replicas (-want +got):\n%s", diff)
	}
	in.Current = want
	in.Members["a"] = MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 2, Occupied: 4}
	if diff := cmp.Diff(Step{Targets: want, Reason: "ReplacementNotReady"}, advance(t, in)); diff != "" {
		t.Fatalf("b must keep all its replicas while a's new replicas are not ready (-want +got):\n%s", diff)
	}
	in.Members["a"] = serving(4)
	for _, b := range []int32{3, 2} {
		want = map[string]int32{"a": 4, "b": b, "c": 4}
		if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
			t.Fatalf("once a serves all 4, b must give up one replica per pass (-want +got):\n%s", diff)
		}
		in.Current = want
		in.Members["b"] = serving(b)
	}
	require.True(t, advance(t, in).Complete)
}

// TestTransitionUnreadyReplicasStayWhileAnUnchangedMemberDoesNotServe raises
// the total of three members from 18 to 19. Member a grows from 6 to 10 and
// serves all 10. Member b falls from 6 to 3. One of b's 6 replicas is being
// updated. Its old copy serves, but OME does not count it as ready, and b keeps
// it at any count. Member c keeps its 6, but c does not serve them all, because
// its routers are down or 2 of its replicas crash-loop. b keeps all 6 replicas
// while c does not serve, because a lower count would remove one of b's ready
// replicas. Once c serves all 6 again, the ready replicas above the new total
// pay for 2 of b's replicas.
func TestTransitionUnreadyReplicasStayWhileAnUnchangedMemberDoesNotServe(t *testing.T) {
	for _, tt := range []struct {
		name string
		c    MemberState
	}{
		{name: "routers down", c: MemberState{Known: true, Eligible: true, Applied: true, Occupied: 6}},
		{name: "two replicas crash-loop", c: MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 4, Occupied: 6}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := Transition{
				From:    Plan{Targets: map[string]int32{"a": 6, "b": 6, "c": 6}},
				Current: map[string]int32{"a": 10, "b": 6, "c": 6},
				Desired: Plan{Targets: map[string]int32{"a": 10, "b": 3, "c": 6}}, Surge: 19,
				Members: map[string]MemberState{
					"a": serving(10),
					"b": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 5, Occupied: 6},
					"c": tt.c,
				},
			}
			if diff := cmp.Diff(Step{Targets: map[string]int32{"a": 10, "b": 6, "c": 6}, Reason: "ReplacementNotReady"}, advance(t, in)); diff != "" {
				t.Fatalf("b must keep all its replicas while c does not serve (-want +got):\n%s", diff)
			}
			in.Members["c"] = serving(6)
			if diff := cmp.Diff(Step{Targets: map[string]int32{"a": 10, "b": 4, "c": 6}, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
				t.Fatalf("once c serves, b must give up the replicas the ready replicas pay for (-want +got):\n%s", diff)
			}
		})
	}
}

// TestTransitionUnreadyReplicasLeaveOnlyAsFastAsReadyReplicasAllow grows member
// a from 6 replicas to 9 and lowers member b from 6 to 4, so the total grows
// from 12 to 13. a serves all 9. b runs 6 replicas: 2 ready, 2 ready on a node
// that is being drained, and 2 restarting after the drain evicted them. OME
// counts 4 of b's replicas as ready. b's own scale-down removes a ready replica
// on a node being drained before a restarting one, so every replica b gives up
// may be a ready one. The 13 ready replicas are 1 above the old total of 12, so
// b gives up only 1 replica. b then serves 3 ready replicas and holds 5 until
// the restarted replicas are ready. Then b gives up 1 more, which the ready
// replicas above the new total pay for.
func TestTransitionUnreadyReplicasLeaveOnlyAsFastAsReadyReplicasAllow(t *testing.T) {
	in := Transition{
		From:    Plan{Targets: map[string]int32{"a": 6, "b": 6}},
		Current: map[string]int32{"a": 9, "b": 6},
		Desired: Plan{Targets: map[string]int32{"a": 9, "b": 4}}, Surge: 13,
		Members: map[string]MemberState{
			"a": serving(9),
			"b": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 4, Occupied: 6},
		},
	}
	want := map[string]int32{"a": 9, "b": 5}
	if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("b must give up only the replica that the ready replicas above 12 cover (-want +got):\n%s", diff)
	}
	in.Current = want
	in.Members["b"] = MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: 5}
	if diff := cmp.Diff(Step{Targets: want, Reason: "ReplacementNotReady"}, advance(t, in)); diff != "" {
		t.Fatalf("b must hold 5 while the ready replicas add up to the old total (-want +got):\n%s", diff)
	}
	in.Members["b"] = serving(5)
	want = map[string]int32{"a": 9, "b": 4}
	if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("once b's restarted replicas are ready, b must give up 1 more (-want +got):\n%s", diff)
	}
	in.Current = want
	in.Members["b"] = serving(4)
	require.True(t, advance(t, in).Complete)
}

// TestTransitionUnreadyReplicasWaitForFundedReductions grows member a from 2
// replicas to 6. It lowers member b from 4 to 2 and member c from 4 to 3, so
// the total grows from 10 to 11. a serves all 6, and 3 of the 4 replicas on
// each of b and c are ready. The ready replicas above the new total pay for
// b's two reductions, one per pass. b's first reduction counts on b's replica
// that is not ready leaving first. So c gives up its replica that is not ready
// only after b reaches its target, in a pass where nothing else moves.
func TestTransitionUnreadyReplicasWaitForFundedReductions(t *testing.T) {
	in := Transition{
		From:    Plan{Targets: map[string]int32{"a": 2, "b": 4, "c": 4}},
		Current: map[string]int32{"a": 6, "b": 4, "c": 4},
		Desired: Plan{Targets: map[string]int32{"a": 6, "b": 2, "c": 3}}, Surge: 11,
		Members: map[string]MemberState{
			"a": serving(6),
			"b": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: 4},
			"c": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: 4},
		},
	}
	for _, b := range []int32{3, 2} {
		want := map[string]int32{"a": 6, "b": b, "c": 4}
		if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
			t.Fatalf("b must give up one replica per pass while c keeps its 4 (-want +got):\n%s", diff)
		}
		in.Current = want
		in.Members["b"] = serving(b)
	}
	want := map[string]int32{"a": 6, "b": 2, "c": 3}
	if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("once b reaches its target, c must give up its replica that is not ready (-want +got):\n%s", diff)
	}
	in.Current = want
	in.Members["c"] = serving(3)
	require.True(t, advance(t, in).Complete)
}

// TestTransitionGrowingTotalKeepsAMemberWithNoReadyReplica grows the total from
// 5 to 6 replicas. The plan removes member a, whose only replica is not ready,
// lowers member b from 4 to 3 while 3 of its replicas are ready, and grows
// member c from 0 to 3. b keeps its replica that is not ready while c grows,
// and gives it up once c serves. The ready replicas then add up to exactly 6,
// so no ready replica above the new total pays for a's replica, and the move
// waits. A member with no ready replica keeps its original replicas, because a
// fault across a whole cluster can hide replicas that come back. a drains once
// its replica is ready.
func TestTransitionGrowingTotalKeepsAMemberWithNoReadyReplica(t *testing.T) {
	in := Transition{
		From:    Plan{Targets: map[string]int32{"a": 1, "b": 4}},
		Current: map[string]int32{"a": 1, "b": 4, "c": 0},
		Desired: Plan{Targets: map[string]int32{"a": 0, "b": 3, "c": 3}}, Surge: 6,
		Members: map[string]MemberState{
			"a": {Known: true, Eligible: true, Applied: true, Occupied: 1},
			"b": {Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: 4},
			"c": {Known: true, Eligible: true},
		},
	}
	want := map[string]int32{"a": 1, "b": 4, "c": 3}
	if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("c must grow while a and b keep their replicas (-want +got):\n%s", diff)
	}
	in.Current = want
	in.Members["c"] = serving(3)
	want = map[string]int32{"a": 1, "b": 3, "c": 3}
	if diff := cmp.Diff(Step{Targets: want, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("once c serves, b must give up its replica that is not ready (-want +got):\n%s", diff)
	}
	in.Current = want
	in.Members["b"] = serving(3)
	if diff := cmp.Diff(Step{Targets: want, Reason: "ReplacementNotReady"}, advance(t, in)); diff != "" {
		t.Fatalf("a must keep its replica while no ready replica is above the new total (-want +got):\n%s", diff)
	}
	in.Members["a"] = serving(1)
	if diff := cmp.Diff(Step{Targets: want, Drain: []string{"a"}, Reason: "AwaitingMemberConvergence"}, advance(t, in)); diff != "" {
		t.Fatalf("a must drain once its replica is ready (-want +got):\n%s", diff)
	}
}
