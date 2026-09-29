package defrag

import (
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/testutil"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

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
