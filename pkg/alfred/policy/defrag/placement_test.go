package defrag

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Missing captured nodes must withhold a previously selected move, including
// when another target still lets the arithmetic policy produce a candidate.
func TestRevalidatePlacementMissingCapturedNode(t *testing.T) {
	for _, missing := range []string{"source", "target"} {
		t.Run(missing, func(t *testing.T) {
			snap := testutil.NewSnapshot().WithNode("source", "h100", 8).
				WithNode("target", "h100", 8).WithNode("alternate", "h100", 8).
				WithInstance("prod/mover", v1beta1.EngineComponent, constants.OMENative, "source", 1).
				WithOtherOccupant("target", 7).WithOtherOccupant("alternate", 7).Build()
			cfg := lowGate()
			cfg.Policies.Defragmentation.Scoring.SizeLadder = []int{8}
			cfg.Policies.Defragmentation.Scoring.SizePrior = map[string]float64{"8": 1}
			*cfg.Policies.Defragmentation.Scoring.DemandBlendLambda = 1
			candidates := executables(evaluate(t, snap, cfg))
			if len(candidates) != 1 {
				t.Fatalf("expected initial candidate: %+v", candidates)
			}
			c := candidates[0]
			pod := snap.Workloads[c.Workload].Components[c.Component].Instances[0].Pods[0]
			targets := map[types.NamespacedName]string{{Namespace: pod.Namespace, Name: pod.Name}: "target"}
			delete(snap.Nodes, missing)
			if missing == "target" && len(executables(evaluate(t, snap, cfg))) != 1 {
				t.Fatal("alternate target must preserve arithmetic eligibility")
			}
			if _, ok := RevalidatePlacement(snap, cfg, c, targets); ok {
				t.Fatal("missing captured node did not withhold the move")
			}
		})
	}
}

// Replaying current window authorization must reject routine moves and lost
// emergencies, while actual positive placement retains the emergency boost.
func TestRevalidatePlacementWindowAndEmergency(t *testing.T) {
	for _, tc := range []struct {
		name         string
		closed       bool
		agedPending  bool
		clearPending bool
		want         bool
	}{
		{name: "routine open", want: true},
		{name: "routine closed", closed: true},
		{name: "emergency closed", closed: true, agedPending: true, want: true},
		{name: "emergency demand cleared", closed: true, agedPending: true, clearPending: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testutil.NewSnapshot().WithNode("source", "h100", 8).
				WithNode("hole", "h100", 8).WithNode("wide", "h100", 8).
				WithInstance("prod/mover", v1beta1.EngineComponent, constants.OMENative, "source", 1).
				WithOtherOccupant("hole", 7).WithOtherOccupant("wide", 4)
			if tc.agedPending {
				b.WithPendingPodIn("prod", 8, 30*time.Minute, "h100")
			}
			snap := b.Build()
			cfg := lowGate()
			cfg.EmergencyPendingAgeMinutes = 15
			cfg.MaintenanceWindows = []config.MaintenanceWindow{{Days: []string{"Thu"}, Start: "09:00", End: "17:00"}}
			cfg.Policies.Defragmentation.Scoring.SizeLadder = []int{4, 8}
			cfg.Policies.Defragmentation.Scoring.SizePrior = map[string]float64{"4": 1, "8": 1}
			*cfg.Policies.Defragmentation.Scoring.DemandBlendLambda = 1
			candidates := executables(evaluate(t, snap, cfg))
			if len(candidates) != 1 {
				t.Fatalf("expected initial arithmetic candidate: %+v", candidates)
			}
			c := candidates[0]
			almost(t, "arithmetic benefit", c.Benefit, 0.5)
			if c.Emergency != tc.agedPending {
				t.Fatalf("initial emergency=%t, want %t", c.Emergency, tc.agedPending)
			}
			if tc.closed {
				cfg.MaintenanceWindows[0].Days = []string{"Mon"}
			}
			if tc.clearPending {
				snap.PendingPods = nil
			}
			pod := snap.Workloads[c.Workload].Components[c.Component].Instances[0].Pods[0]
			targets := map[types.NamespacedName]string{{Namespace: pod.Namespace, Name: pod.Name}: "wide"}
			got, ok := RevalidatePlacement(snap, cfg, c, targets)
			if ok != tc.want {
				t.Fatalf("placement eligibility=%t, want %t: %+v", ok, tc.want, got)
			}
			if ok {
				almost(t, "actual benefit", got.Benefit, 1.0/3.0)
				wantScore := 11.0 / 60.0
				if tc.agedPending {
					wantScore = 11.0 / 30.0
				}
				almost(t, "actual score", got.Score, wantScore)
				if got.Emergency != tc.agedPending {
					t.Fatalf("actual emergency=%t, want %t", got.Emergency, tc.agedPending)
				}
			}
		})
	}
}

// Catches scoring only part of a gang, using the hypothetical greedy targets,
// or forgetting that a positive capacity gain still has to exceed move cost.
func TestRevalidateCompletePlacementBenefit(t *testing.T) {
	for _, tc := range []struct {
		name        string
		targets     []string
		extraEmpty  int
		want        bool
		wantBenefit float64
	}{
		{name: "both holes", targets: []string{"target1", "target2"}, want: true, wantBenefit: 0.5},
		{name: "one hole", targets: []string{"target1", "empty1"}, want: true, wantBenefit: 0.25},
		{name: "empty nodes only", targets: []string{"empty1", "empty2"}},
		{name: "incomplete gang", targets: []string{"target1"}},
		{name: "overcommitted surge", targets: []string{"target1", "target1"}},
		{name: "source target", targets: []string{"target1", "source2"}},
		{name: "unknown target", targets: []string{"target1", "missing"}},
		{name: "positive below cost", targets: []string{"target1", "empty1"}, extraEmpty: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := testutil.NewSnapshot().WithNode("source1", "h100", 8).WithNode("source2", "h100", 8).
				WithNode("target1", "h100", 8).WithNode("target2", "h100", 8).
				WithNode("empty1", "h100", 8).WithNode("empty2", "h100", 8).
				WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 1, "source1", "source2").
				WithOtherOccupant("target1", 7).WithOtherOccupant("target2", 7)
			for i := range tc.extraEmpty {
				b.WithNode(fmt.Sprintf("extra-%d", i), "h100", 8)
			}
			snap := b.Build()
			cfg := lowGate()
			*cfg.Policies.Defragmentation.FragmentationThreshold = 0.01
			cfg.Policies.Defragmentation.Scoring.SizeLadder = []int{8}
			cfg.Policies.Defragmentation.Scoring.SizePrior = map[string]float64{"8": 1}
			*cfg.Policies.Defragmentation.Scoring.DemandBlendLambda = 0
			candidates := executables(evaluate(t, snap, cfg))
			if len(candidates) != 1 {
				t.Fatalf("arithmetic plan must be executable: %+v", candidates)
			}
			c := candidates[0]
			pods := snap.Workloads[c.Workload].Components[c.Component].Instances[0].Pods
			targets := map[types.NamespacedName]string{}
			for i, target := range tc.targets {
				targets[types.NamespacedName{Namespace: pods[i].Namespace, Name: pods[i].Name}] = target
			}
			got, ok := RevalidatePlacement(snap, cfg, c, targets)
			if ok != tc.want {
				t.Fatalf("placement eligibility=%t, want %t: %+v", ok, tc.want, got)
			}
			if ok {
				almost(t, "actual benefit", got.Benefit, tc.wantBenefit)
				almost(t, "actual score", got.Score, tc.wantBenefit-0.15)
				if got.Emergency {
					t.Fatal("no aged pending demand should produce an emergency")
				}
			}
			if snap.Nodes["source1"].FreeGPUs != 7 || snap.Nodes["target1"].FreeGPUs != 1 {
				t.Fatal("placement scoring mutated the accepted baseline")
			}
		})
	}
}

func TestRevalidatePlacementRequiresCPUCompanion(t *testing.T) {
	b := testutil.NewSnapshot().WithNode("source1", "h100", 8).WithNode("source2", "h100", 8).
		WithNode("target", "h100", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 1, "source1", "source2").
		WithOtherOccupant("target", 7)
	snap := b.Build()
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "wide"}]
	inst := w.Components[v1beta1.EngineComponent].Instances[0]
	inst.Pods[1].GPUs = 0
	inst.TotalGPUs = 1
	snap.Nodes["source2"].AllocatedGPUs = 0
	snap.Nodes["source2"].FreeGPUs = 8
	cfg := lowGate()
	cfg.Policies.Defragmentation.Scoring.SizeLadder = []int{8}
	cfg.Policies.Defragmentation.Scoring.SizePrior = map[string]float64{"8": 1}
	*cfg.Policies.Defragmentation.Scoring.DemandBlendLambda = 0
	candidates := executables(evaluate(t, snap, cfg))
	if len(candidates) != 1 {
		t.Fatalf("CPU companion fixture has no candidate: %+v", candidates)
	}
	targets := map[types.NamespacedName]string{}
	for _, pod := range inst.Pods {
		targets[types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}] = "target"
	}
	got, ok := RevalidatePlacement(snap, cfg, candidates[0], targets)
	if !ok {
		t.Fatal("complete GPU plus CPU placement must remain eligible")
	}
	almost(t, "CPU does not consume GPU capacity", got.Benefit, 0.5)
	delete(targets, types.NamespacedName{Namespace: inst.Pods[1].Namespace, Name: inst.Pods[1].Name})
	if _, ok := RevalidatePlacement(snap, cfg, candidates[0], targets); ok {
		t.Fatal("missing CPU companion was accepted as a complete instance move")
	}
}
