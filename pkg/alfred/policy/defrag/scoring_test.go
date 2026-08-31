package defrag

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const epsilon = 1e-9

func almost(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > epsilon {
		t.Fatalf("%s = %.10f, want %.10f", name, got, want)
	}
}

func fragOf(t *testing.T, cs *PoolScore, size int64) float64 {
	t.Helper()
	for _, sf := range cs.PerSize {
		if sf.Size == size {
			return sf.Frag
		}
	}
	t.Fatalf("size %d not in PerSize %+v", size, cs.PerSize)
	return 0
}

// tenSpreadNodes builds the OEP's signature trap: ten 8-GPU nodes, each with
// one 1-GPU occupant — 70 GPUs free, none of them visible to an 8-GPU
// demand. movable selects OME workloads vs non-OME occupants.
func tenSpreadNodes(movable bool) *snapshot.ClusterSnapshot {
	b := testutil.NewSnapshot()
	for i := 1; i <= 10; i++ {
		node := fmt.Sprintf("node%d", i)
		b.WithNode(node, "h100", 8)
		if movable {
			b.WithInstance(fmt.Sprintf("prod/svc-%d", i), v1beta1.EngineComponent, constants.OMENative, node, 1)
		} else {
			b.WithOtherOccupant(node, 1)
		}
	}
	return b.Build()
}

// TestSignatureTrapObserved pins the OEP worked example: Frag(8)=1.0 with 70
// GPUs free, and the small sizes need no hand-tuned down-weighting.
func TestSignatureTrapObserved(t *testing.T) {
	scores := ComputeScores(tenSpreadNodes(false), config.Default())
	cs := scores.PerPool["h100"]
	if cs == nil {
		t.Fatal("h100 pool missing")
	}
	if cs.TotalFree != 70 {
		t.Fatalf("TotalFree = %d, want 70", cs.TotalFree)
	}
	almost(t, "Frag(1)", fragOf(t, cs, 1), 0)
	almost(t, "Frag(2)", fragOf(t, cs, 2), 1.0/7)
	almost(t, "Frag(4)", fragOf(t, cs, 4), 3.0/7)
	almost(t, "Frag(8)", fragOf(t, cs, 8), 1.0)
}

// TestScoreMeasuresOpportunityNotState: the same fragmented shape scores
// zero when only non-OME occupants (which Alfred may never move) cause it,
// and positive when movable workloads do.
func TestScoreMeasuresOpportunityNotState(t *testing.T) {
	cfg := config.Default()

	immovable := ComputeScores(tenSpreadNodes(false), cfg).PerPool["h100"]
	almost(t, "immovable FReclaimable", immovable.FReclaimable, 0)
	almost(t, "immovable Score", immovable.Score, 0)

	movable := ComputeScores(tenSpreadNodes(true), cfg).PerPool["h100"]
	// Worked through the OEP formulas with defaults (lambda=0.3, prior
	// {1:.1, 2:.1, 4:.2, 8:.6}; demand = 10 GPUs at size 1):
	//   weights: w1=0.73 w2=0.03 w4=0.06 w8=0.18
	//   F_observed = 0.03*(1/7) + 0.06*(3/7) + 0.18*1        = 0.21
	//   repacked free = [0,6,8,8,8,8,8,8,8,8]
	//   F_best     = 0.06*(1/35) + 0.18*(3/35)               = 0.6/35
	almost(t, "movable FObserved", movable.FObserved, 0.21)
	almost(t, "movable FBest", movable.FBest, 0.6/35)
	almost(t, "movable FReclaimable", movable.FReclaimable, 0.21-0.6/35)
	almost(t, "movable Score", movable.Score, 0.21-0.6/35)
}

// TestPendingPressureEligible: a pending pod the repack could seat raises
// P(c) by its age urgency; the noisy-OR score is at least each term.
func TestPendingPressureEligible(t *testing.T) {
	b := testutil.NewSnapshot()
	for i := 1; i <= 10; i++ {
		node := fmt.Sprintf("node%d", i)
		b.WithNode(node, "h100", 8)
		b.WithInstance(fmt.Sprintf("prod/svc-%d", i), v1beta1.EngineComponent, constants.OMENative, node, 1)
	}
	// Pending 8-GPU pod, aged exactly tau (30m default): u = 1 - e^-1.
	b.WithPendingPod(8, 30*time.Minute, "h100")
	scores := ComputeScores(b.Build(), config.Default())
	cs := scores.PerPool["h100"]

	wantU := 1 - math.Exp(-1)
	almost(t, "PendingPressure", cs.PendingPressure, wantU)
	if cs.Score < cs.PendingPressure-epsilon || cs.Score < cs.FReclaimable-epsilon {
		t.Fatalf("noisy-OR score %v must dominate both terms (P=%v, FR=%v)", cs.Score, cs.PendingPressure, cs.FReclaimable)
	}
}

// TestPendingPressureIneligible: a pending pod no repack could seat is
// capacity shortage, not fragmentation — it must not wake Alfred.
func TestPendingPressureIneligible(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8)
	b.WithOtherOccupant("node1", 8)
	b.WithOtherOccupant("node2", 8)
	b.WithPendingPod(8, time.Hour, "h100")

	cs := ComputeScores(b.Build(), config.Default()).PerPool["h100"]
	almost(t, "PendingPressure", cs.PendingPressure, 0)
	almost(t, "Score", cs.Score, 0)
	if cs.TotalFree != 0 {
		t.Fatalf("TotalFree = %d, want 0", cs.TotalFree)
	}
}

// TestMaxOverPools: a healthy pool must not dilute a sick one.
func TestMaxOverPools(t *testing.T) {
	b := testutil.NewSnapshot()
	for i := 1; i <= 10; i++ {
		node := fmt.Sprintf("h100-%d", i)
		b.WithNode(node, "h100", 8)
		b.WithInstance(fmt.Sprintf("prod/svc-%d", i), v1beta1.EngineComponent, constants.OMENative, node, 1)
	}
	b.WithNode("a100-1", "a100", 4)
	b.WithNode("a100-2", "a100", 4)

	scores := ComputeScores(b.Build(), config.Default())
	almost(t, "a100 score", scores.PerPool["a100"].Score, 0)
	almost(t, "gate = max", scores.FragmentationScore, scores.PerPool["h100"].Score)
	if scores.FragmentationScore <= 0 {
		t.Fatal("fragmented pool must drive the gate above zero")
	}
}

// TestUnschedulableFreeCapacityIsAMirage: free GPUs on an unhealthy node
// must not count as slots — they cannot seat anything.
func TestUnschedulableFreeCapacityIsAMirage(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8, testutil.NodeUnhealthy())
	b.WithOtherOccupant("node1", 1)

	cs := ComputeScores(b.Build(), config.Default()).PerPool["h100"]
	if cs.TotalFree != 7 {
		t.Fatalf("TotalFree = %d, want 7 (unhealthy node excluded)", cs.TotalFree)
	}
	almost(t, "Frag(8)", fragOf(t, cs, 8), 1.0)
}

// pinScenario: node1 holds a movable-looking 1-GPU workload (7 free); node2
// holds a 7-GPU immovable occupant (1 free). Consolidating the small pod
// into node2's hole frees a full 8-GPU node — exactly what the repack must
// find, and exactly what a volume-pinned workload forbids.
func pinScenario(mode constants.DeploymentModeType, pinned bool, omenativeAvailable bool) *snapshot.ClusterSnapshot {
	b := testutil.NewSnapshot().
		WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8).
		WithOMENative(omenativeAvailable).
		WithInstance("prod/svc-a", v1beta1.EngineComponent, mode, "node1", 1)
	b.WithOtherOccupant("node2", 7)
	if pinned {
		b.WithModel("prod/svc-a", &snapshot.ModelAvailability{
			Key:            snapshot.ModelKey{Kind: "BaseModel", Namespace: "prod", Name: "rwo-model"},
			Backend:        snapshot.BackendPVC,
			PVCAccessModes: []string{"ReadWriteOnce"},
			VolumePinned:   true,
		})
	}
	return b.Build()
}

const pinScenarioReclaimable = 0.2175 // worked example in the test bodies

func TestRepackConsolidatesIntoHoles(t *testing.T) {
	cs := ComputeScores(pinScenario(constants.OMENative, false, true), config.Default()).PerPool["h100"]
	// Observed [7,1]: F_obs = 0.03*0.25 + 0.06*0.5 + 0.18*1 = 0.2175.
	// Repacked [8,0] (pod fills node2's hole): F_best = 0.
	almost(t, "FObserved", cs.FObserved, pinScenarioReclaimable)
	almost(t, "FBest", cs.FBest, 0)
	almost(t, "FReclaimable", cs.FReclaimable, pinScenarioReclaimable)
}

func TestVolumePinnedWorkloadIsNotRepackable(t *testing.T) {
	cs := ComputeScores(pinScenario(constants.OMENative, true, true), config.Default()).PerPool["h100"]
	almost(t, "pinned FReclaimable", cs.FReclaimable, 0)
}

func TestUnresolvedModelIsNotRepackable(t *testing.T) {
	key := types.NamespacedName{Namespace: "prod", Name: "svc-a"}
	tests := []struct {
		name   string
		mutate func(*snapshot.ClusterSnapshot)
	}{
		{
			name: "missing availability",
			mutate: func(s *snapshot.ClusterSnapshot) {
				s.Workloads[key].ModelKey = snapshot.ModelKey{Kind: snapshot.ModelKindBaseModel, Namespace: "prod", Name: "missing"}
			},
		},
		{
			name: "resolve error",
			mutate: func(s *snapshot.ClusterSnapshot) {
				model := snapshot.ModelKey{Kind: snapshot.ModelKindBaseModel, Namespace: "prod", Name: "broken"}
				s.Workloads[key].ModelKey = model
				s.Models[model] = &snapshot.ModelAvailability{Key: model, ResolveError: "bounded test failure"}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := pinScenario(constants.OMENative, false, true)
			tt.mutate(snap)
			cs := ComputeScores(snap, config.Default()).PerPool["h100"]
			if cs.FObserved <= 0 {
				t.Fatal("unresolved model occupancy must remain observed")
			}
			almost(t, "FReclaimable", cs.FReclaimable, 0)
			almost(t, "PendingPressure", cs.PendingPressure, 0)
			almost(t, "Score", cs.Score, 0)
		})
	}
}

func TestOMENativeUnavailableExcludesFromRepack(t *testing.T) {
	unavailable := ComputeScores(pinScenario(constants.OMENative, false, false), config.Default()).PerPool["h100"]
	almost(t, "degraded FReclaimable", unavailable.FReclaimable, 0)

	available := ComputeScores(pinScenario(constants.OMENative, false, true), config.Default()).PerPool["h100"]
	almost(t, "available FReclaimable", available.FReclaimable, pinScenarioReclaimable)
}

func TestLWSCountsAsDemandButNeverRepacks(t *testing.T) {
	cs := ComputeScores(pinScenario(constants.MultiNode, false, true), config.Default()).PerPool["h100"]
	// Same shape, but the workload is LWS-backed: real demand, no
	// executable move — nothing reclaimable.
	almost(t, "lws FReclaimable", cs.FReclaimable, 0)
	if cs.FObserved <= 0 {
		t.Fatal("LWS workload must still count as demand (FObserved > 0)")
	}
}

// TestRepackFallbackNeverGoesNegative: a movable pod on an excluded node is
// lifted "from nowhere" (no capacity contribution) and can win the only
// schedulable seat; the displaced resident then has nowhere to go and stays
// home. Its home bin must clamp at zero free, not double-book below it.
func TestRepackFallbackNeverGoesNegative(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("healthy", "h100", 8).
		WithNode("sick", "h100", 8, testutil.NodeUnhealthy())
	// "apex" sorts before "zeta", so the sick node's pod is placed first
	// and takes the healthy node's lifted seat.
	b.WithInstance("prod/apex", v1beta1.EngineComponent, constants.OMENative, "sick", 8)
	b.WithInstance("prod/zeta", v1beta1.EngineComponent, constants.OMENative, "healthy", 8)
	snap := b.Build()
	cfg := config.Default()

	repacked := repackPool(snap, cfg, "h100", schedulableBins(snap, cfg, "h100"))
	for _, bin := range repacked {
		if bin.free < 0 {
			t.Fatalf("repacked bin %s has negative free capacity %d", bin.name, bin.free)
		}
	}

	cs := ComputeScores(snap, cfg).PerPool["h100"]
	for name, v := range map[string]float64{
		"FObserved": cs.FObserved, "FBest": cs.FBest,
		"FReclaimable": cs.FReclaimable, "Score": cs.Score,
	} {
		if math.IsNaN(v) || v < 0 || v > 1 {
			t.Fatalf("%s = %v, want a value in [0, 1]", name, v)
		}
	}
}

// TestExcludedNodePodCannotFabricateReclaimable: seating a movable pod off an
// unhealthy node consumes schedulable free capacity without creating a single
// new slot. FBest must normalize by the observed TotalFree — letting the
// denominator shrink with the repack would report reclaimable fragmentation
// where no migration improves anything.
func TestExcludedNodePodCannotFabricateReclaimable(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("healthy", "h100", 8).
		WithNode("sick", "h100", 8, testutil.NodeUnhealthy())
	b.WithOtherOccupant("healthy", 4) // 4 free on the only schedulable node
	b.WithInstance("prod/evac", v1beta1.EngineComponent, constants.OMENative, "sick", 4)

	cs := ComputeScores(b.Build(), config.Default()).PerPool["h100"]
	almost(t, "FReclaimable", cs.FReclaimable, 0)
	almost(t, "Score", cs.Score, 0)
}

// TestPendingPressureZeroTauIsZeroNotNaN: tau <= 0 cannot come from a
// validated config, but a direct caller must get 0, not NaN (0/0), which
// would silently break every downstream gauge and alert threshold.
func TestPendingPressureZeroTauIsZeroNotNaN(t *testing.T) {
	b := testutil.NewSnapshot().WithNode("node1", "h100", 8)
	b.WithPendingPod(8, 0, "h100") // age 0 with tau 0 is the 0/0 case
	got := pendingPressure(b.Build(), "h100", []int64{8}, map[int64]int64{8: 0}, map[int64]int64{8: 1}, 0)
	if math.IsNaN(got) || got != 0 {
		t.Fatalf("pendingPressure with tau=0 = %v, want 0", got)
	}
}

// TestParsePriorRejectsMalformedKeys: keys must be canonical whole integers
// — "8x", "08", and "+8" must not alias size 8 (colliding aliases would make
// the kept weight depend on map iteration order) — and the surviving weights
// renormalize to a proper distribution.
func TestParsePriorRejectsMalformedKeys(t *testing.T) {
	got := parsePrior(map[string]float64{
		"8x": 0.3, "08": 0.05, "+8": 0.05, "4": 0.5, "-1": 0.05, "": 0.05,
	})
	if len(got) != 1 || got[4] != 1.0 {
		t.Fatalf("parsePrior = %v, want exactly {4: 1.0}", got)
	}
}

// TestParsePriorNormalizesTolerantSums: validation tolerates a prior sum in
// [0.99, 1.01]; parsePrior must renormalize so that at lambda=1 the blended
// weights stay exactly convex and no published score can exceed 1.
func TestParsePriorNormalizesTolerantSums(t *testing.T) {
	got := parsePrior(map[string]float64{"4": 0.505, "8": 0.505}) // sum 1.01 passes validation
	almost(t, "prior[4]", got[4], 0.5)
	almost(t, "prior[8]", got[8], 0.5)
}

// TestSnapToLadderEmptyLadder: an empty ladder cannot come from a validated
// config, but a direct caller must get 0, not an index panic.
func TestSnapToLadderEmptyLadder(t *testing.T) {
	if got := snapToLadder(8, nil); got != 0 {
		t.Fatalf("snapToLadder(8, nil) = %d, want 0", got)
	}
}

func TestSlotsForMultiNodeSizes(t *testing.T) {
	bins := []binState{
		{name: "n1", free: 8, cap: 8},
		{name: "n2", free: 8, cap: 8},
		{name: "n3", free: 4, cap: 8},
	}
	if got := slotsForSize(bins, 16); got != 1 {
		t.Fatalf("Slots(16) = %d, want 1 (two fully-free 8-GPU nodes)", got)
	}
	if got := slotsForSize(bins, 8); got != 2 {
		t.Fatalf("Slots(8) = %d, want 2", got)
	}
	if got := slotsForSize(bins, 4); got != 5 {
		t.Fatalf("Slots(4) = %d, want 5", got)
	}

	// Heterogeneous shapes: a fully-free 4-GPU node cannot host an
	// 8-GPU member pod, so it must not count toward an 8x2 group.
	mixed := []binState{
		{name: "big", free: 8, cap: 8},
		{name: "small", free: 4, cap: 4},
	}
	if got := slotsForSize(mixed, 16); got != 0 {
		t.Fatalf("Slots(16) = %d, want 0 (one 8-GPU node plus a 4-GPU node seats no 16-GPU group)", got)
	}
}

func TestPublishScoresSetsGauges(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	PublishScores(tenSpreadNodes(true), config.Default(), m)

	if got := promtestutil.ToFloat64(m.ClusterFragmentationScore); math.Abs(got-(0.21-0.6/35)) > epsilon {
		t.Fatalf("cluster_fragmentation_score = %v", got)
	}
	if got := promtestutil.ToFloat64(m.FragmentationObserved.WithLabelValues("h100", "8")); got != 1.0 {
		t.Fatalf("fragmentation_observed{h100,8} = %v, want 1.0", got)
	}
	if got := promtestutil.ToFloat64(m.FragmentationReclaimable.WithLabelValues("h100")); got <= 0 {
		t.Fatalf("fragmentation_reclaimable{h100} = %v, want > 0", got)
	}
	if got := promtestutil.ToFloat64(m.PendingPressure.WithLabelValues("h100")); got != 0 {
		t.Fatalf("pending_pressure{h100} = %v, want 0", got)
	}
}

func TestExecutionScoringUsesOnlyEligibleOMENative(t *testing.T) {
	base := func(mode constants.DeploymentModeType) *snapshot.ClusterSnapshot {
		return pinScenario(mode, false, true)
	}
	steady := func() *snapshot.ClusterSnapshot {
		return makeOMENativeSteady(t, base(constants.OMENative), "svc-a")
	}
	assertObservedOnly := func(t *testing.T, snap *snapshot.ClusterSnapshot) {
		t.Helper()
		scores := ComputeScores(snap, config.Default())
		cs := scores.PerPool["h100"]
		if cs.FObserved <= 0 {
			t.Fatal("workload occupancy must remain visible in FObserved")
		}
		almost(t, "FReclaimable", cs.FReclaimable, 0)
		almost(t, "PendingPressure", cs.PendingPressure, 0)
		almost(t, "Score", cs.Score, 0)
		almost(t, "FragmentationScore", scores.FragmentationScore, 0)
	}

	t.Run("RawDeployment", func(t *testing.T) {
		assertObservedOnly(t, base(constants.RawDeployment))
	})
	t.Run("LWS", func(t *testing.T) {
		assertObservedOnly(t, base(constants.MultiNode))
	})
	t.Run("executor unavailable despite legacy true", func(t *testing.T) {
		snap := steady()
		snap.OMENativeAvailable = true
		snap.OMENativeExecutor.Available = false
		assertObservedOnly(t, snap)
	})
	t.Run("invalid observation", func(t *testing.T) {
		snap := steady()
		snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "svc-a"}].Components[v1beta1.EngineComponent].ObservationValid = false
		assertObservedOnly(t, snap)
	})

	t.Run("eligible OMENative", func(t *testing.T) {
		cs := ComputeScores(steady(), config.Default()).PerPool["h100"]
		almost(t, "FObserved", cs.FObserved, pinScenarioReclaimable)
		almost(t, "FReclaimable", cs.FReclaimable, pinScenarioReclaimable)
		almost(t, "Score", cs.Score, pinScenarioReclaimable)
	})

	t.Run("legacy compatibility boolean ignored", func(t *testing.T) {
		available := steady()
		available.OMENativeAvailable = true
		legacyFalse := steady()
		legacyFalse.OMENativeAvailable = false
		gotAvailable := ComputeScores(available, config.Default()).PerPool["h100"]
		gotLegacyFalse := ComputeScores(legacyFalse, config.Default()).PerPool["h100"]
		almost(t, "legacy bool FReclaimable", gotLegacyFalse.FReclaimable, gotAvailable.FReclaimable)
		almost(t, "legacy bool Score", gotLegacyFalse.Score, gotAvailable.Score)
	})
}

func TestObservedOnlyRepackCannotCreatePendingPressure(t *testing.T) {
	b := testutil.NewSnapshot()
	for i := 1; i <= 10; i++ {
		node := fmt.Sprintf("node%d", i)
		b.WithNode(node, "h100", 8)
		b.WithInstance(fmt.Sprintf("prod/raw-%d", i), v1beta1.EngineComponent, constants.RawDeployment, node, 1)
	}
	// Even a currently open slot cannot make pending pressure executable:
	// no eligible OMENative repack created that slot.
	b.WithNode("open", "h100", 8)
	b.WithPendingPod(8, 30*time.Minute, "h100")
	cs := ComputeScores(b.Build(), config.Default()).PerPool["h100"]
	if cs.FObserved <= 0 {
		t.Fatal("Raw fragmentation must remain observed")
	}
	almost(t, "FReclaimable", cs.FReclaimable, 0)
	almost(t, "PendingPressure", cs.PendingPressure, 0)
	almost(t, "Score", cs.Score, 0)
}

func TestCrossPoolAtomicInstanceIsNotRepackable(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("a-source", "a100", 8).
		WithNode("a-target", "a100", 8).
		WithNode("h-source", "h100", 8).
		WithNode("h-target", "h100", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 1, "a-source", "h-source")
	b.WithOtherOccupant("a-target", 7)
	b.WithOtherOccupant("h-target", 7)
	b.WithPendingPod(8, 30*time.Minute, "a100")
	b.WithPendingPod(8, 30*time.Minute, "h100")
	scores := ComputeScores(b.Build(), config.Default())

	for _, pool := range []string{"a100", "h100"} {
		cs := scores.PerPool[pool]
		if cs.FObserved <= 0 {
			t.Fatalf("%s cross-pool occupancy must remain observed", pool)
		}
		almost(t, pool+" FReclaimable", cs.FReclaimable, 0)
		almost(t, pool+" PendingPressure", cs.PendingPressure, 0)
		almost(t, pool+" Score", cs.Score, 0)
	}
}
