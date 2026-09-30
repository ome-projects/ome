package allocation

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
)

func serving(count int32) Home {
	return Home{Known: true, Eligible: true, Applied: true, Routable: true, Ready: count, Occupied: count}
}

func moving() Transition {
	budget := int32(1)
	return Transition{
		From: Plan{Targets: map[string]int32{"a": 4}}, Current: map[string]int32{"a": 4},
		Desired: Plan{Targets: map[string]int32{"b": 4}}, MaxSurge: &budget,
		Homes: map[string]Home{"a": serving(4), "b": {Known: true, Eligible: true}},
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
		in.Homes["b"] = Home{Known: true, Eligible: true, Applied: true, Occupied: next}
		step = advance(t, in)
		if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
			t.Fatalf("admission without readiness cannot release the donor (-want +got):\n%s", diff)
		}
		in.Homes["b"] = Home{Known: true, Eligible: true, Applied: true, Occupied: next, Ready: next}
		step = advance(t, in)
		if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
			t.Fatalf("readiness without routability cannot release the donor (-want +got):\n%s", diff)
		}
		in.Homes["b"] = serving(next)
		step = advance(t, in)
		if next == 4 {
			if diff := cmp.Diff([]string{"a"}, step.Drain); diff != "" {
				t.Fatalf("result mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
				t.Fatalf("traffic must drain before a zero-target home is removed (-want +got):\n%s", diff)
			}
			home := in.Homes["a"]
			home.Drained = true
			home.Routable = false
			in.Homes["a"] = home
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
		in.Homes["a"] = serving(4 - next)
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
		allowance *int32
		rollout   int32
		want      Step
	}{
		{name: "rollout consumes allowance", allowance: ptr.To(int32(1)), rollout: 1,
			want: Step{Targets: map[string]int32{"a": 4, "b": 0}, Reason: "SurgeBudgetExhausted"}},
		{name: "released allowance permits growth", allowance: ptr.To(int32(1)),
			want: Step{Targets: map[string]int32{"a": 4, "b": 1}, Reason: "AwaitingMemberConvergence"}},
		{name: "unset blocks migration",
			want: Step{Targets: map[string]int32{"a": 4, "b": 0}, Reason: "MigrationBlocked"}},
		{name: "zero forbids excess", allowance: ptr.To(int32(0)),
			want: Step{Targets: map[string]int32{"a": 4, "b": 0}, Reason: "SurgeBudgetExhausted"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := moving()
			in.MaxSurge, in.RolloutReserved = tt.allowance, tt.rollout
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
	in.Homes["b"] = serving(1)
	in.Desired = Plan{Targets: map[string]int32{"c": 4}}
	in.Homes["c"] = Home{Known: true, Eligible: true}
	step := advance(t, in)
	if diff := cmp.Diff([]string{"b"}, step.Drain); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	compareTargets(t, in.Current, step.Targets)
	in.Homes["b"] = Home{Known: true, Applied: true, Drained: true, Ready: 1, Occupied: 1}
	step = advance(t, in)
	if diff := cmp.Diff(map[string]int32{"a": 4, "b": 0, "c": 0}, step.Targets); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	in.Current = step.Targets
	step = advance(t, in)
	require.Zero(t, step.Targets["c"], "a terminating surplus still occupies the allowance")
	in.Homes["b"] = Home{Known: true, Absent: true}
	step = advance(t, in)
	if diff := cmp.Diff(int32(1), step.Targets["c"]); diff != "" {
		t.Fatalf("observed removal permits progress to the new destination (-want +got):\n%s", diff)
	}
}

func TestTransitionRetargetPreservesEarlierReplacement(t *testing.T) {
	in := moving()
	in.Current = map[string]int32{"a": 3, "b": 1}
	in.Homes["a"], in.Homes["b"] = serving(3), serving(1)
	in.Desired = Plan{Targets: map[string]int32{"c": 4}}
	in.Homes["c"] = Home{Known: true, Eligible: true}
	step := advance(t, in)
	require.Empty(t, step.Drain, "the abandoned destination still backs an original replica")
	if diff := cmp.Diff(map[string]int32{"a": 3, "b": 1, "c": 1}, step.Targets); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	in.Current = step.Targets
	in.Homes["c"] = serving(1)
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
	in.Homes["b"] = Home{Known: true, Applied: true, Occupied: 1}
	in.Desired = Plan{Targets: map[string]int32{"c": 4}}
	in.Homes["c"] = Home{Known: true, Eligible: true}
	step := advance(t, in)
	if diff := cmp.Diff([]string{"b"}, step.Drain); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	home := in.Homes["b"]
	home.Drained = true
	in.Homes["b"] = home
	step = advance(t, in)
	if diff := cmp.Diff(map[string]int32{"a": 4, "b": 0, "c": 0}, step.Targets); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestTransitionUnknownHomePreservesAllocations(t *testing.T) {
	in := moving()
	in.Homes["a"] = Home{}
	step := advance(t, in)
	compareTargets(t, in.Current, step.Targets)
	if diff := cmp.Diff("ObservationUnknown", step.Reason); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
	in.Homes["a"] = Home{Known: true, Absent: true}
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
		Homes:   map[string]Home{"a": {Known: true, Eligible: true}, "b": {Known: true, Eligible: true}},
	}
	step := advance(t, in)
	if diff := cmp.Diff(in.Desired.Targets, step.Targets); diff != "" {
		t.Fatalf("initial placement needs no migration surplus (-want +got):\n%s", diff)
	}
	in.Current = step.Targets
	in.Homes["a"], in.Homes["b"] = serving(2), serving(2)
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

func TestTransitionUnassignedFloorRetainsExistingHome(t *testing.T) {
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
		Desired: Plan{Targets: map[string]int32{"c": 2}}, MaxSurge: &budget,
		Homes: map[string]Home{"a": serving(1), "b": serving(1), "c": serving(1)},
	}
	step := advance(t, in)
	if diff := cmp.Diff([]string{"a"}, step.Drain); diff != "" {
		t.Fatalf("one ready replacement cannot drain two old homes (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestTransitionWaitsForEligibilityAndAppliedTargets(t *testing.T) {
	in := moving()
	in.Homes["b"] = Home{Known: true}
	step := advance(t, in)
	compareTargets(t, in.Current, step.Targets)
	in.Current["b"] = 1
	in.Homes["b"] = Home{Known: true, Eligible: true, Occupied: 1, Ready: 1, Routable: true}
	step = advance(t, in)
	if diff := cmp.Diff(in.Current, step.Targets); diff != "" {
		t.Fatalf("an observation from another applied plan cannot fund removal (-want +got):\n%s", diff)
	}
	in.Desired = Plan{Targets: maps.Clone(in.Current)}
	require.False(t, advance(t, in).Complete, "a member must acknowledge its persisted request")
}

func TestTransitionResumedHomeRequiresServingEvidence(t *testing.T) {
	for _, planner := range []struct {
		name    string
		advance func(Transition) (Step, error)
	}{
		{name: "replica steps", advance: Advance},
		{name: "whole homes", advance: AdvanceWholeHomes},
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
					in.Current["b"], in.Homes["b"] = 4, serving(4)
					in.Desired = Plan{Targets: map[string]int32{"a": 4}}
					in.Homes["a"] = Home{Known: true, Applied: tt.applied, Routable: tt.routed, Ready: 4, Occupied: 4}
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

func TestTransitionAccountsForUnplannedOccupancy(t *testing.T) {
	in := moving()
	in.Homes["c"] = serving(1)
	step := advance(t, in)
	require.Zero(t, step.Targets["b"], "owned instances outside the plan still consume surge")
	require.Contains(t, step.Targets, "c", "an unplanned home must receive a zero target")
	if diff := cmp.Diff([]string{"c"}, step.Drain); diff != "" {
		t.Fatalf("unplanned home must drain (-want +got):\n%s", diff)
	}
	in.Homes["c"] = Home{Occupied: 1}
	if diff := cmp.Diff("ObservationUnknown", advance(t, in).Reason); diff != "" {
		t.Fatalf("result mismatch (-want +got):\n%s", diff)
	}
}

func TestTransitionProtectsReplicasDuringInitialRetarget(t *testing.T) {
	tests := []struct {
		name    string
		from    Plan
		current map[string]int32
		homes   map[string]Home
		want    map[string]int32
	}{
		{name: "first full allocation", current: map[string]int32{"a": 4}, homes: map[string]Home{"a": serving(4)}, want: map[string]int32{"a": 4, "b": 1}},
		{name: "first serving replica", current: map[string]int32{"a": 1}, homes: map[string]Home{"a": serving(1)}, want: map[string]int32{"a": 1, "b": 4}},
		{name: "previously unassigned floor", from: Plan{Unassigned: 4}, current: map[string]int32{"a": 4}, homes: map[string]Home{"a": serving(4)}, want: map[string]int32{"a": 4, "b": 1}},
		{name: "unplanned serving replica", homes: map[string]Home{"a": serving(1)}, want: map[string]int32{"a": 0, "b": 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := moving()
			in.From, in.Current, in.Homes = tt.from, tt.current, tt.homes
			in.Homes["b"] = Home{Known: true, Eligible: true}
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
		{"negative allowance", func(in *Transition) { *in.MaxSurge = -1 }},
		{"negative rollout reservation", func(in *Transition) { in.RolloutReserved = -1 }},
		{"ready exceeds occupied", func(in *Transition) { in.Homes["a"] = Home{Known: true, Ready: 5, Occupied: 4} }},
		{"absent but occupied", func(in *Transition) { in.Homes["a"] = Home{Known: true, Absent: true, Occupied: 1} }},
		{"drained but routable", func(in *Transition) { in.Homes["a"] = Home{Known: true, Drained: true, Routable: true} }},
		{"empty home name", func(in *Transition) { in.Homes[""] = Home{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, planner := range []struct {
				name    string
				advance func(Transition) (Step, error)
			}{
				{name: "replica steps", advance: Advance},
				{name: "whole homes", advance: AdvanceWholeHomes},
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
			in := Transition{From: partition(floor), Desired: partition(floor), MaxSurge: &budget,
				RolloutReserved: int32(rng.IntN(int(budget))), Homes: map[string]Home{}}
			in.Current = maps.Clone(in.From.Targets)
			if scenario%2 == 0 {
				in.From = Plan{Unassigned: floor}
			}
			for _, name := range names {
				in.Homes[name] = serving(in.Current[name])
			}
			complete := false
			for iteration := range 256 {
				if iteration == 3 || iteration == 7 {
					in.Desired = partition(floor)
				}
				step := advance(t, in)
				for _, name := range step.Resume {
					home := in.Homes[name]
					home.Drained, home.Routable = false, true
					in.Homes[name] = home
				}
				var requested, ready int32
				for _, name := range names {
					requested += step.Targets[name]
					home := in.Homes[name]
					if home.Routable {
						ready += min(home.Ready, step.Targets[name])
					}
				}
				for _, name := range step.Drain {
					home := in.Homes[name]
					if home.Routable {
						ready -= min(home.Ready, step.Targets[name])
					}
					home.Drained, home.Routable = true, false
					in.Homes[name] = home
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
						in.Homes[name] = serving(step.Targets[name])
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
