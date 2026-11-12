package types

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// runnersForCopied is the reading RunnersFor replaces, kept here as the
// reference: the layout copied for every Instance, then every worker set
// to the width the revision recorded.
func runnersForCopied(p ComponentPlan, inst InstancePlan, rev string) []RunnerPlan {
	base := p.Runners
	if len(base) == 0 {
		base = inst.Runners
	}
	out := append([]RunnerPlan(nil), base...)
	size, known := p.WorkerSizeByRevision[rev]
	if rev == "" || !known {
		return out
	}
	for i := range out {
		if out[i].Name == RunnerWorker {
			out[i].Size = size
		}
	}
	return out
}

// randomLayout is one of the layouts a plan or an Instance's entry carries:
// none, an empty one, a single runner, a leader and workers of any width,
// workers on both sides of a leader, or a leader alone.
func randomLayout(rng *rand.Rand) []RunnerPlan {
	switch rng.IntN(6) {
	case 0:
		return nil
	case 1:
		return []RunnerPlan{}
	case 2:
		return []RunnerPlan{{Name: RunnerDefault, Size: 1}}
	case 3:
		return layoutOf(int32(rng.IntN(9)))
	case 4:
		return []RunnerPlan{{Name: RunnerWorker, Size: int32(rng.IntN(5))}, {Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: int32(rng.IntN(5))}}
	default:
		return []RunnerPlan{{Name: RunnerLeader, Size: 1}}
	}
}

// randomLayoutDraw is one plan, one Instance and one revision: widths
// recorded for some revisions or none, the revision asked for among them or
// not, equal to the layout's width or not, and empty one draw in eight.
func randomLayoutDraw(rng *rand.Rand) (ComponentPlan, InstancePlan, string) {
	plan := ComponentPlan{Runners: randomLayout(rng)}
	inst := InstancePlan{Index: int32(rng.IntN(5000)), Runners: randomLayout(rng)}
	revisions := []string{"rev-a", "rev-b", "rev-c", "rev-d"}
	if rng.IntN(4) > 0 {
		plan.WorkerSizeByRevision = map[string]int32{}
		for _, revision := range revisions {
			if rng.IntN(2) == 0 {
				plan.WorkerSizeByRevision[revision] = int32(rng.IntN(9))
			}
		}
	}
	rev := revisions[rng.IntN(len(revisions))]
	if rng.IntN(8) == 0 {
		rev = ""
	}
	return plan, inst, rev
}

// sameLayout reports whether got is layout itself, not a copy of it.
func sameLayout(got, layout []RunnerPlan) bool {
	return len(got) > 0 && len(layout) > 0 && &got[0] == &layout[0]
}

// TestComponentPlan_RunnersFor_AgreesWithTheCopyingFormOnRandomPlans draws
// 200,000 plans and holds the layout RunnersFor reads to the copied one,
// runner for runner and nil where the copy is nil; the plan's and the
// Instance's layouts are as they were after the reading, and a layout read
// at another width is the caller's own, so a write into it reaches
// neither.
func TestComponentPlan_RunnersFor_AgreesWithTheCopyingFormOnRandomPlans(t *testing.T) {
	rng := rand.New(rand.NewPCG(27, 1106))
	const draws = 200000
	var shared, copied, none int
	for draw := 0; draw < draws; draw++ {
		plan, inst, rev := randomLayoutDraw(rng)
		planBefore := slices.Clone(plan.Runners)
		instBefore := slices.Clone(inst.Runners)
		want := runnersForCopied(plan, inst, rev)
		got := plan.RunnersFor(inst, rev)
		if diff := cmp.Diff(want, got); diff != "" || (want == nil) != (got == nil) {
			t.Fatalf("draw %d: plan %+v, entry %+v, revision %q: layout (-copied +read):\n%s\nnil: copied %t, read %t", draw, plan, inst, rev, diff, want == nil, got == nil)
		}
		if count := plan.TotalPodsFor(inst, rev); count != (InstancePlan{Runners: got}).TotalPods() {
			t.Fatalf("draw %d: TotalPodsFor counts %d, the layout has %d", draw, count, (InstancePlan{Runners: got}).TotalPods())
		}
		switch {
		case got == nil:
			none++
		case sameLayout(got, plan.Runners) || sameLayout(got, inst.Runners):
			shared++
		default:
			copied++
			got[0].Size = -1
		}
		if !slices.Equal(planBefore, plan.Runners) || !slices.Equal(instBefore, inst.Runners) {
			t.Fatalf("draw %d: the reading at %q changed the plan's layout (%+v, was %+v) or the entry (%+v, was %+v)", draw, rev, plan.Runners, planBefore, inst.Runners, instBefore)
		}
	}
	t.Logf("%d draws: %d read a layout itself, %d a copy at another width, %d none", draws, shared, copied, none)
}
