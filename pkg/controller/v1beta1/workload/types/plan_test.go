package types

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestDeadlineAt pins the unconfigured case: with no readiness window an
// operation must open with NO deadline (the zero Time the backstop reads
// as "never expires"), never one equal to its own start time, which the
// very next pass would read as already elapsed.
func TestDeadlineAt(t *testing.T) {
	now := metav1.NewTime(time.Now().Truncate(time.Second))

	if got := DeadlineAt(now, 30*time.Minute); !got.Time.Equal(now.Add(30 * time.Minute)) {
		t.Errorf("configured window: got %v want %v", got, now.Add(30*time.Minute))
	}
	for _, timeout := range []time.Duration{0, -time.Minute} {
		if got := DeadlineAt(now, timeout); !got.IsZero() {
			t.Errorf("timeout %v: got deadline %v want the zero Time", timeout, got)
		}
	}
}

// A per-Instance topology key overrides the Component's; a row with none
// falls back so a plan need only name the exceptions.
func TestComponentPlanTopologyKeyForInstance(t *testing.T) {
	plan := ComponentPlan{
		TopologyKey:          "rack",
		InstanceTopologyKeys: map[int32]string{1: "island-a"},
	}
	if got := plan.TopologyKeyForInstance(1); got != "island-a" {
		t.Errorf("per-Instance key: got %q want %q", got, "island-a")
	}
	if got := plan.TopologyKeyForInstance(2); got != "rack" {
		t.Errorf("fallback: got %q want %q", got, "rack")
	}
}

func TestInstancePlanTotalPodsSumsEveryRunner(t *testing.T) {
	plan := InstancePlan{Runners: []RunnerPlan{{Size: 1}, {Size: 4}}}
	if got := plan.TotalPods(); got != 5 {
		t.Errorf("total pods: got %d want 5", got)
	}
	if got := (InstancePlan{}).TotalPods(); got != 0 {
		t.Errorf("no runners: got %d want 0", got)
	}
}

// The surge slot must avoid both the steady-state indices and the slots
// siblings claimed this pass but have not materialized yet: sharing one
// releases every source at once when the shared surge turns Ready.
func TestAllocateSurgeIndexExcludesClaimedSlots(t *testing.T) {
	claimed := int32(1)
	instances := []InstanceStatus{
		{Index: 0, Operation: &InstanceOperation{SurgeIndex: &claimed}},
		{Index: 2},
	}
	if got := AllocateSurgeIndex(instances); got != 3 {
		t.Errorf("lowest slot free of indices and claims: got %d want 3", got)
	}
	if got := AllocateSurgeIndex(nil); got != 0 {
		t.Errorf("an empty Component starts at 0: got %d", got)
	}
	if got := AllocateSurgeIndex([]InstanceStatus{{Index: 1}}); got != 0 {
		t.Errorf("a hole below the highest index is still the lowest free: got %d want 0", got)
	}
}

// TestSurgeIndexAllocator_ListsTheIndexesThePassClaimed: the allocator
// keeps the indexes the surges of its pass took apart from the ones the
// rows held when it was built, so the create pass can leave them to the
// surges; an index offered and never claimed is not among them.
func TestSurgeIndexAllocator_ListsTheIndexesThePassClaimed(t *testing.T) {
	surges := NewSurgeIndexAllocator([]InstanceStatus{{Index: 1}, {Index: 2}})
	if got := surges.Claimed(); got != nil {
		t.Fatalf("nothing claimed yet: got %v", got)
	}
	first := surges.Next()
	surges.Claim(first)
	second := surges.Next()
	if first != 0 || second != 3 {
		t.Fatalf("the pass takes 0 then is offered 3: got %d and %d", first, second)
	}
	if got := surges.Claimed(); len(got) != 1 {
		t.Fatalf("one index claimed, the offered one not among them: got %v", got)
	} else if _, ok := got[0]; !ok {
		t.Fatalf("index 0 should be the claimed one: got %v", got)
	}
}

// TestSurgeIndexAllocator_HandsOutTheLowestFreeIndexesInOrder: rows 0, 1,
// 2, 4 and 6 serve, row 1's surge in flight has claimed 3 and row 4's has
// claimed 8. The surges of one pass take 5, 7, 9, 10 and 11 in that order:
// the lowest index no row holds and no surge has claimed, each once. An
// index offered but not claimed is offered again. The index is in the
// replacement's pod names, so the sequence is part of what an operator
// sees.
func TestSurgeIndexAllocator_HandsOutTheLowestFreeIndexesInOrder(t *testing.T) {
	three, eight := int32(3), int32(8)
	rows := []InstanceStatus{
		{Index: 0},
		{Index: 1, Operation: &InstanceOperation{SurgeIndex: &three}},
		{Index: 2},
		{Index: 4, Operation: &InstanceOperation{SurgeIndex: &eight}},
		{Index: 6},
	}
	surges := NewSurgeIndexAllocator(rows)
	if got := surges.Next(); got != 5 {
		t.Fatalf("first surge: got %d want 5", got)
	}
	if got := surges.Next(); got != 5 {
		t.Fatalf("an index not yet claimed is offered again: got %d want 5", got)
	}
	var got []int32
	for i := 0; i < 5; i++ {
		idx := surges.Next()
		surges.Claim(idx)
		got = append(got, idx)
	}
	if diff := cmp.Diff([]int32{5, 7, 9, 10, 11}, got); diff != "" {
		t.Errorf("surge indexes of the pass (-want +got):\n%s", diff)
	}
	if got := AllocateSurgeIndex(rows); got != 5 {
		t.Errorf("one index outside a pass: got %d want 5", got)
	}
}

// TestSurgeIndexAllocator_AgreesWithOneReadOfTheRowsPerSurge: over random
// rows and claims, the indexes one pass's allocator hands out, each
// claimed in turn, are the ones a fresh read of the rows gives each surge
// once the earlier surges' claims are recorded on their sources, as the
// gang surge records its claim in the snapshot.
func TestSurgeIndexAllocator_AgreesWithOneReadOfTheRowsPerSurge(t *testing.T) {
	rng := rand.New(rand.NewSource(3137))
	for round := 0; round < 2000; round++ {
		var rows []InstanceStatus
		for idx := int32(0); idx < 48; idx++ {
			if rng.Intn(3) == 0 {
				continue
			}
			row := InstanceStatus{Index: idx}
			if rng.Intn(6) == 0 {
				claim := int32(rng.Intn(60))
				row.Operation = &InstanceOperation{SurgeIndex: &claim}
			}
			rows = append(rows, row)
		}
		surges := NewSurgeIndexAllocator(append([]InstanceStatus(nil), rows...))
		for i := range rows {
			if rows[i].Operation != nil {
				continue
			}
			want := AllocateSurgeIndex(rows)
			got := surges.Next()
			if got != want {
				t.Fatalf("round %d, source %d: the pass's allocator offers %d, one read of the rows gives %d", round, rows[i].Index, got, want)
			}
			surges.Claim(got)
			claimed := got
			rows[i].Operation = &InstanceOperation{SurgeIndex: &claimed}
		}
	}
}

// TestComponentPlan_RunnersFor pins how an Instance's layout is resolved for
// the revision it renders: the target's layout at the width that revision
// recorded, the target's own layout for the target or a revision recording
// no width, and the Instance's entry when the plan carries no target layout.
func TestComponentPlan_RunnersFor(t *testing.T) {
	gang := []RunnerPlan{{Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: 3}}
	plan := ComponentPlan{
		Runners:              gang,
		WorkerSizeByRevision: map[string]int32{"svc-engine-old00000": 2},
	}
	inst := InstancePlan{Index: 0, Runners: []RunnerPlan{{Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: 9}}}

	if got := plan.RunnersFor(inst, "svc-engine-old00000"); got[1].Size != 2 || got[0].Size != 1 || len(got) != 2 {
		t.Fatalf("a recorded width is rendered: got %+v", got)
	}
	if got := plan.RunnersFor(inst, "svc-engine-new00000"); got[1].Size != 3 {
		t.Fatalf("a revision recording no width renders the target's: got %+v", got)
	}
	if got := plan.RunnersFor(inst, ""); got[1].Size != 3 {
		t.Fatalf("no revision renders the target's layout: got %+v", got)
	}
	if plan.Runners[1].Size != 3 {
		t.Fatalf("RunnersFor must leave the target layout as it was: %+v", plan.Runners)
	}
	bare := ComponentPlan{WorkerSizeByRevision: map[string]int32{"svc-engine-old00000": 2}}
	if got := bare.RunnersFor(inst, "svc-engine-old00000"); got[1].Size != 2 {
		t.Fatalf("without a target layout the Instance's entry is the base: got %+v", got)
	}
	single := ComponentPlan{Runners: []RunnerPlan{{Name: RunnerDefault, Size: 1}}, WorkerSizeByRevision: map[string]int32{"svc-router-old00000": 2}}
	if got := single.RunnersFor(InstancePlan{}, "svc-router-old00000"); len(got) != 1 || got[0].Size != 1 {
		t.Fatalf("a single-pod layout has no width to set: got %+v", got)
	}
}

// TestComponentPlan_TotalPodsFor holds the count of the layout an Instance
// renders at a revision, read without building it: the target's layout at
// the width the revision recorded, the target's own width for a revision
// recording none or no revision, and the Instance's entry when the plan
// carries no target layout.
func TestComponentPlan_TotalPodsFor(t *testing.T) {
	plan := ComponentPlan{
		Runners:              []RunnerPlan{{Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: 3}},
		WorkerSizeByRevision: map[string]int32{"svc-engine-old00000": 2, "": 7},
	}
	inst := InstancePlan{Index: 0, Runners: []RunnerPlan{{Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: 9}}}
	cases := []struct {
		name string
		plan ComponentPlan
		rev  string
		want int32
	}{
		{name: "a recorded width is counted", plan: plan, rev: "svc-engine-old00000", want: 3},
		{name: "a revision recording no width counts the target's", plan: plan, rev: "svc-engine-new00000", want: 4},
		{name: "no revision counts the target's layout, whatever the map records under the empty name", plan: plan, rev: "", want: 4},
		{name: "without a target layout the Instance's entry is counted at the recorded width",
			plan: ComponentPlan{WorkerSizeByRevision: map[string]int32{"svc-engine-old00000": 2}}, rev: "svc-engine-old00000", want: 3},
		{name: "without a target layout the Instance's entry is counted as it is",
			plan: ComponentPlan{}, rev: "svc-engine-new00000", want: 10},
		{name: "a single-pod layout has no width to set",
			plan: ComponentPlan{Runners: []RunnerPlan{{Name: RunnerDefault, Size: 1}}, WorkerSizeByRevision: map[string]int32{"svc-router-old00000": 2}}, rev: "svc-router-old00000", want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.plan.TotalPodsFor(inst, tc.rev); got != tc.want {
				t.Errorf("TotalPodsFor = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestComponentPlan_TotalPodsFor_CountsTheLayoutRunnersForBuilds draws
// 200,000 plans (a target layout of one default runner, a leader beside
// zero to seven workers, two worker runners, or none; an Instance entry of
// the same kinds; widths recorded for none, some or every revision, the
// empty name included) and holds TotalPodsFor at every revision, the empty
// and an unrecorded one included, to the pod count of the layout
// RunnersFor builds.
func TestComponentPlan_TotalPodsFor_CountsTheLayoutRunnersForBuilds(t *testing.T) {
	r := rand.New(rand.NewSource(25))
	layouts := func() []RunnerPlan {
		switch r.Intn(5) {
		case 0:
			return nil
		case 1:
			return []RunnerPlan{{Name: RunnerDefault, Size: 1}}
		case 2:
			return []RunnerPlan{{Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: int32(r.Intn(8))}}
		case 3:
			return []RunnerPlan{{Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: int32(r.Intn(8))}, {Name: RunnerWorker, Size: int32(r.Intn(8))}}
		default:
			return []RunnerPlan{{Name: RunnerWorker, Size: int32(r.Intn(8))}, {Name: RunnerDefault, Size: int32(r.Intn(3))}}
		}
	}
	revisions := []string{"", "svc-engine-rev-a", "svc-engine-rev-b", "svc-engine-rev-c"}
	var draws, counted int
	for draws < 200000 {
		plan := ComponentPlan{Runners: layouts(), WorkerSizeByRevision: map[string]int32{}}
		for _, rev := range revisions {
			if r.Intn(2) == 0 {
				plan.WorkerSizeByRevision[rev] = int32(r.Intn(9))
			}
		}
		inst := InstancePlan{Index: int32(r.Intn(4)), Runners: layouts()}
		for _, rev := range append(revisions, "svc-engine-rev-unrecorded") {
			want := InstancePlan{Runners: plan.RunnersFor(inst, rev)}.TotalPods()
			if got := plan.TotalPodsFor(inst, rev); got != want {
				t.Fatalf("plan %+v, Instance %+v, revision %q: TotalPodsFor = %d, the layout RunnersFor builds has %d pods", plan, inst, rev, got, want)
			}
			counted += int(want)
			draws++
		}
	}
	t.Logf("%d readings, %d pods counted", draws, counted)
}
