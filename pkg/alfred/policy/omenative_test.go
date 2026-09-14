package policy

import (
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestOMENativeEligibilityDoesNotApplyCooldown(t *testing.T) {
	snap := testutil.NewSnapshot().
		WithNode("source", "h100", 8).
		WithNode("target", "h100", 8).
		WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "source", 1).
		Build()
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
	comp := w.Components[v1beta1.EngineComponent]
	recent := snap.Timestamp.Add(-time.Minute)
	w.LastMigration = &recent

	if got := OMENativeEligibility(snap, w, comp, comp.Instances[0]); got != "" {
		t.Fatalf("cooldown leaked into shared eligibility: got %q, want eligible", got)
	}
}

func TestPlanAtomicSurgeIsDeterministicAndQuarantinesTargets(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("source-a", "h100", 8).
		WithNode("source-b", "h100", 8).
		WithNode("target-a", "h100", 8).
		WithNode("target-b", "h100", 8).
		WithNode("target-quarantined", "h100", 8, testutil.NodeUnknown()).
		WithNode("target-cordoned", "h100", 8, testutil.NodeCordoned()).
		WithNode("other-pool", "l4", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 2, "source-b", "source-a")
	b.WithOtherOccupant("target-a", 6)
	b.WithOtherOccupant("target-b", 6)
	b.WithOtherOccupant("target-quarantined", 6)
	b.WithOtherOccupant("target-cordoned", 6)
	snap := b.Build()
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "wide"}]
	inst := w.Components[v1beta1.EngineComponent].Instances[0]

	plan, ok := PlanAtomicSurge(snap, config.Default(), w, inst)
	if !ok {
		t.Fatal("two 2-GPU targets must seat the complete two-pod surge")
	}
	if got, want := plan.PlacementTargetNodes, []string{"target-a", "target-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("placement targets = %v, want %v", got, want)
	}
	if got, want := plan.HintTargetNodes, []string{"target-a", "target-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("hint targets = %v, want %v", got, want)
	}
	if len(plan.Moves) != 2 {
		t.Fatalf("moves = %+v, want complete two-pod footprint", plan.Moves)
	}
	if plan.Moves[0].TargetNode != "target-a" || plan.Moves[1].TargetNode != "target-b" {
		t.Fatalf("deterministic fullest-first placements = %+v, want target-a then target-b", plan.Moves)
	}
	var footprint int64
	for _, move := range plan.Moves {
		footprint += move.GPUs
		if move.FromNode != "source-a" && move.FromNode != "source-b" {
			t.Fatalf("move lost source footprint: %+v", move)
		}
	}
	if footprint != 4 {
		t.Fatalf("planned footprint = %d, want 4", footprint)
	}
}

func TestPlanAtomicSurgeFiltersSourceCADDeleteAndSpotTargets(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("source-a", "h100", 8).
		WithNode("source-b", "h100", 8).
		WithNode("a-ca-delete", "h100", 8, testutil.NodeScaleDownMarked()).
		WithNode("b-spot", "h100", 8, testutil.NodePreemptible()).
		WithNode("c-clear", "h100", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 1, "source-a", "source-b")
	snap := b.Build()
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "wide"}]
	inst := w.Components[v1beta1.EngineComponent].Instances[0]

	plan, ok := PlanAtomicSurge(snap, config.Default(), w, inst)
	if !ok {
		t.Fatal("clear target has room for the complete surge")
	}
	if got, want := plan.PlacementTargetNodes, []string{"c-clear"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("default target filters = %v, want %v", got, want)
	}
	for _, move := range plan.Moves {
		if move.TargetNode == "source-a" || move.TargetNode == "source-b" {
			t.Fatalf("planner reused a current Instance node: %+v", plan.Moves)
		}
	}

	for _, override := range []string{"migrate", "ignore"} {
		w.SpotPolicy = override
		plan, ok = PlanAtomicSurge(snap, config.Default(), w, inst)
		if !ok || !reflect.DeepEqual(plan.PlacementTargetNodes, []string{"c-clear"}) {
			t.Fatalf("cluster spot avoidance must remain authoritative for %s workloads: %+v, ok=%t", override, plan, ok)
		}
	}
	w.SpotPolicy = ""

	cfg := config.Default()
	*cfg.SpotPolicy.AvoidAsTarget = false
	plan, ok = PlanAtomicSurge(snap, cfg, w, inst)
	if !ok {
		t.Fatal("spot and clear targets have room when cluster spot avoidance is disabled")
	}
	if got, want := plan.PlacementTargetNodes, []string{"b-spot", "c-clear"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("spot-enabled targets = %v, want %v", got, want)
	}
	for _, override := range []string{"migrate", "ignore"} {
		w.SpotPolicy = override
		plan, ok = PlanAtomicSurge(snap, cfg, w, inst)
		if !ok || !reflect.DeepEqual(plan.PlacementTargetNodes, []string{"b-spot", "c-clear"}) {
			t.Fatalf("%s must retain cluster-permitted spot targets: %+v, ok=%t", override, plan, ok)
		}
	}

	w.SpotPolicy = "avoid"
	plan, ok = PlanAtomicSurge(snap, cfg, w, inst)
	if !ok || !reflect.DeepEqual(plan.PlacementTargetNodes, []string{"c-clear"}) {
		t.Fatalf("workload spot avoidance must override the permissive cluster default: %+v, ok=%t", plan, ok)
	}
}

func TestPlanAtomicSurgeHonorsModelPlacement(t *testing.T) {
	key := snapshot.ModelKey{Kind: snapshot.ModelKindBaseModel, Namespace: "prod", Name: "weights"}
	tests := []struct {
		name  string
		avail *snapshot.ModelAvailability
		want  []string
	}{
		{
			name: "per-node readiness",
			avail: &snapshot.ModelAvailability{Key: key, Backend: snapshot.BackendPerNode,
				NodesReady: []string{"source", "target-b"}},
			want: []string{"target-b"},
		},
		{
			name: "pvc topology",
			avail: &snapshot.ModelAvailability{Key: key, Backend: snapshot.BackendPVC,
				PVCTopologyNodes: []string{"target-a"}},
			want: []string{"target-a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testutil.NewSnapshot().
				WithNode("source", "h100", 8).
				WithNode("target-a", "h100", 8).
				WithNode("target-b", "h100", 8).
				WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "source", 1).
				WithModel("prod/model", tt.avail)
			snap := b.Build()
			w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
			plan, ok := PlanAtomicSurge(snap, config.Default(), w, w.Components[v1beta1.EngineComponent].Instances[0])
			if !ok || !reflect.DeepEqual(plan.PlacementTargetNodes, tt.want) {
				t.Fatalf("model-constrained plan = %+v, ok=%t, want targets %v", plan, ok, tt.want)
			}
		})
	}
}

func TestPlanAtomicSurgeLargestPodFirstAndBoundsHints(t *testing.T) {
	b := testutil.NewSnapshot().
		WithNode("source-a", "h100", 8).
		WithNode("source-b", "h100", 8).
		WithNode("target-a", "h100", 8).
		WithNode("target-b", "h100", 8).
		WithNode("target-c", "h100", 8).
		WithNode("target-d", "h100", 8).
		WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 1, "source-a", "source-b")
	b.WithOtherOccupant("target-a", 4)
	b.WithOtherOccupant("target-b", 3)
	b.WithOtherOccupant("target-c", 2)
	b.WithOtherOccupant("target-d", 1)
	snap := b.Build()
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "wide"}]
	inst := w.Components[v1beta1.EngineComponent].Instances[0]
	inst.Pods[0].GPUs = 4
	inst.TotalGPUs = 5

	plan, ok := PlanAtomicSurge(snap, config.Default(), w, inst)
	if !ok {
		t.Fatal("unequal two-pod footprint must fit")
	}
	if len(plan.Moves) != 2 || plan.Moves[0].GPUs != 4 || plan.Moves[1].GPUs != 1 {
		t.Fatalf("moves = %+v, want largest pod first", plan.Moves)
	}
	if got, want := plan.PlacementTargetNodes, []string{"target-a", "target-b", "target-c", "target-d"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("exhaustive placement targets = %v, want %v", got, want)
	}
	if got, want := plan.HintTargetNodes, []string{"target-a", "target-b", "target-c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bounded hints = %v, want %v", got, want)
	}
}

func TestPlanAtomicSurgeRejectsInconsistentInstanceSourceEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*snapshot.Instance)
	}{
		{name: "missing source-node membership", mutate: func(inst *snapshot.Instance) {
			delete(inst.NodesSet, "source-b")
		}},
		{name: "wrong source-node count", mutate: func(inst *snapshot.Instance) {
			inst.NodesSet["source-a"] = 2
		}},
		{name: "extra source-node membership", mutate: func(inst *snapshot.Instance) {
			inst.NodesSet["target-a"] = 1
		}},
		{name: "missing pod source", mutate: func(inst *snapshot.Instance) {
			inst.Pods[1].Node = ""
		}},
		{name: "wrong instance identity", mutate: func(inst *snapshot.Instance) {
			inst.Pods[1].InstanceIndex++
		}},
		{name: "wrong incarnation identity", mutate: func(inst *snapshot.Instance) {
			inst.Pods[1].Incarnation++
		}},
		{name: "wrong aggregate footprint", mutate: func(inst *snapshot.Instance) {
			inst.TotalGPUs++
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testutil.NewSnapshot().
				WithNode("source-a", "h100", 8).
				WithNode("source-b", "h100", 8).
				WithNode("target-a", "h100", 8).
				WithNode("target-b", "h100", 8).
				WithMultiPodInstance("prod/wide", v1beta1.EngineComponent, constants.OMENative, 1, "source-a", "source-b")
			snap := b.Build()
			w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "wide"}]
			inst := w.Components[v1beta1.EngineComponent].Instances[0]
			tt.mutate(inst)

			if plan, ok := PlanAtomicSurge(snap, config.Default(), w, inst); ok {
				t.Fatalf("inconsistent Instance source evidence produced unsafe plan: %+v", plan)
			}
		})
	}
}

func TestModelAdvisoryReason(t *testing.T) {
	key := snapshot.ModelKey{Kind: snapshot.ModelKindBaseModel, Namespace: "prod", Name: "weights"}
	tests := []struct {
		name  string
		avail *snapshot.ModelAvailability
		want  string
	}{
		{name: "missing", want: AdvisoryModelUnresolved},
		{name: "resolution error", avail: &snapshot.ModelAvailability{Key: key, ResolveError: "missing pvc"}, want: AdvisoryModelUnresolved},
		{name: "pinned", avail: &snapshot.ModelAvailability{Key: key, Backend: snapshot.BackendPVC, VolumePinned: true}, want: AdvisoryVolumePinned},
		{name: "movable", avail: &snapshot.ModelAvailability{Key: key, Backend: snapshot.BackendPVC}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := testutil.NewSnapshot().WithNode("source", "h100", 8).
				WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "source", 1)
			if tt.avail != nil {
				b.WithModel("prod/model", tt.avail)
			} else {
				b.ConfigureWorkload("prod/model", func(w *snapshot.Workload) { w.ModelKey = key })
			}
			snap := b.Build()
			w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
			if got := ModelAdvisoryReason(snap, w); got != tt.want {
				t.Fatalf("ModelAdvisoryReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAtomicSurgeExcludesMaintenanceNodes(t *testing.T) {
	snap := testutil.NewSnapshot().WithNode("source", "h100", 8).
		WithNode("maintenance", "h100", 8).WithNode("target", "h100", 8).
		WithInstance("prod/model", v1beta1.EngineComponent, constants.OMENative, "source", 2).Build()
	snap.Nodes["maintenance"].Maintenance.Requested = true
	w := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "model"}]
	plan, ok := PlanAtomicSurge(snap, config.Default(), w, w.Components[v1beta1.EngineComponent].Instances[0])
	if !ok || len(plan.PlacementTargetNodes) != 1 || plan.PlacementTargetNodes[0] != "target" ||
		len(plan.Moves) != 1 || plan.Moves[0].TargetNode != "target" {
		t.Fatalf("surge plan included maintenance target: %+v, ok=%t", plan, ok)
	}
	snap.Nodes["target"].Maintenance.Requested = true
	if plan, ok := PlanAtomicSurge(snap, config.Default(), w, w.Components[v1beta1.EngineComponent].Instances[0]); ok {
		t.Fatalf("surge succeeded without an available target: %+v", plan)
	}
}
