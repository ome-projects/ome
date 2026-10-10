package policy_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	topologyLabel = "cloud.google.com/gke-tpu-topology"
	idLabel       = "cloud.google.com/gke-tpu-partition-2x2x2-id"
	stateLabel    = "cloud.google.com/gke-tpu-partition-2x2x2-state"
)

func partition(id, state string) testutil.NodeOption {
	return testutil.NodeLabels(map[string]string{idLabel: id, stateLabel: state})
}

// tpuSliceSnapshot places a two-host 2x2x2 TPU gang on source-a/source-b
// (partition "p-src") and adds the given free-capacity nodes.
func tpuSliceSnapshot(extra func(*testutil.SnapshotBuilder)) (*snapshot.ClusterSnapshot, *snapshot.Instance) {
	b := testutil.NewSnapshot().
		WithNode("source-a", "", 0, partition("p-src", "HEALTHY"), testutil.NodeMaintenance("de-schedule")).
		WithNode("source-b", "", 0, partition("p-src", "HEALTHY"), testutil.NodeMaintenance("de-schedule"))
	extra(b)
	snap := b.WithMultiPodTPUInstance("prod/tpu", v1beta1.EngineComponent, constants.OMENative, 4, "source-a", "source-b").Build()
	inst := snap.Workloads[types.NamespacedName{Namespace: "prod", Name: "tpu"}].Components[v1beta1.EngineComponent].Instances[0]
	for i := range inst.Pods {
		inst.Pods[i].TPUSliceProvisioned = true
		inst.Pods[i].NodeSelector = map[string]string{topologyLabel: "2x2x2"}
	}
	return snap, inst
}

func enabledConfig() *config.Config {
	cfg := config.Default()
	enabled := true
	cfg.TPUSliceMigrationEnabled = &enabled
	return cfg
}

func TestPlanTPUSliceReplacementCountsFreePartitions(t *testing.T) {
	snap, inst := tpuSliceSnapshot(func(b *testutil.SnapshotBuilder) {
		b.WithNode("free-1a", "", 0, partition("p-free-1", "HEALTHY")).
			WithNode("free-1b", "", 0, partition("p-free-1", "HEALTHY")).
			WithNode("free-2a", "", 0, partition("p-free-2", "HEALTHY")).
			WithNode("free-2b", "", 0, partition("p-free-2", "HEALTHY"))
	})
	plan, reason := policy.PlanTPUSliceReplacement(snap, enabledConfig(), inst)
	if reason != "" || plan == nil || plan.Topology != "2x2x2" || plan.FreePartitions != 2 {
		t.Fatalf("plan=%+v reason=%q, want two free 2x2x2 partitions", plan, reason)
	}
}

func TestPlanTPUSliceReplacementRejectsUnusablePartitions(t *testing.T) {
	tests := []struct {
		name  string
		nodes func(*testutil.SnapshotBuilder)
	}{
		{"one host under maintenance", func(b *testutil.SnapshotBuilder) {
			b.WithNode("m-a", "", 0, partition("p-m", "HEALTHY")).
				WithNode("m-b", "", 0, partition("p-m", "HEALTHY"), testutil.NodeMaintenance("de-schedule"))
		}},
		{"one host cordoned", func(b *testutil.SnapshotBuilder) {
			b.WithNode("c-a", "", 0, partition("p-c", "HEALTHY"), testutil.NodeCordoned()).
				WithNode("c-b", "", 0, partition("p-c", "HEALTHY"))
		}},
		{"one host unhealthy", func(b *testutil.SnapshotBuilder) {
			b.WithNode("u-a", "", 0, partition("p-u", "HEALTHY")).
				WithNode("u-b", "", 0, partition("p-u", "HEALTHY"), testutil.NodeUnhealthy())
		}},
		{"partition state is not healthy", func(b *testutil.SnapshotBuilder) {
			b.WithNode("s-a", "", 0, partition("p-s", "DEGRADED")).
				WithNode("s-b", "", 0, partition("p-s", "DEGRADED"))
		}},
		{"one host holds an accelerator pod", func(b *testutil.SnapshotBuilder) {
			b.WithNode("o-a", "", 0, partition("p-o", "HEALTHY")).
				WithNode("o-b", "", 0, partition("p-o", "HEALTHY")).
				WithMultiPodTPUInstance("prod/other", v1beta1.EngineComponent, constants.OMENative, 4, "o-b")
		}},
		{"no partition label", func(b *testutil.SnapshotBuilder) {
			b.WithNode("n-a", "", 0).WithNode("n-b", "", 0)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap, inst := tpuSliceSnapshot(tt.nodes)
			plan, reason := policy.PlanTPUSliceReplacement(snap, enabledConfig(), inst)
			if plan != nil || reason != policy.AdvisoryNoTPUSliceCapacity {
				t.Fatalf("plan=%+v reason=%q, want %s", plan, reason, policy.AdvisoryNoTPUSliceCapacity)
			}
		})
	}
}

func TestPlanTPUSliceReplacementStaysUnmodeled(t *testing.T) {
	free := func(b *testutil.SnapshotBuilder) {
		b.WithNode("f-a", "", 0, partition("p-f", "HEALTHY")).WithNode("f-b", "", 0, partition("p-f", "HEALTHY"))
	}
	tests := []struct {
		name   string
		cfg    func() *config.Config
		mutate func(*snapshot.Instance)
	}{
		{"path off by default", config.Default, func(*snapshot.Instance) {}},
		{"a pod is not slice-provisioned", enabledConfig, func(i *snapshot.Instance) { i.Pods[1].TPUSliceProvisioned = false }},
		{"a pod has no topology", enabledConfig, func(i *snapshot.Instance) { i.Pods[0].NodeSelector = nil }},
		{"pods disagree on topology", enabledConfig, func(i *snapshot.Instance) {
			i.Pods[1].NodeSelector = map[string]string{topologyLabel: "2x2x4"}
		}},
		{"instance also holds GPUs", enabledConfig, func(i *snapshot.Instance) { i.TotalGPUs = 1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap, inst := tpuSliceSnapshot(free)
			tt.mutate(inst)
			plan, reason := policy.PlanTPUSliceReplacement(snap, tt.cfg(), inst)
			if plan != nil || reason != policy.AdvisoryAcceleratorPlacementUnmodeled {
				t.Fatalf("plan=%+v reason=%q, want %s", plan, reason, policy.AdvisoryAcceleratorPlacementUnmodeled)
			}
		})
	}
}

func TestPlanTPUSliceReplacementUsesConfiguredLabels(t *testing.T) {
	snap, inst := tpuSliceSnapshot(func(b *testutil.SnapshotBuilder) {
		b.WithNode("x-a", "", 0, testutil.NodeLabels(map[string]string{"example.com/part-2x2x2": "p-x", "example.com/part-2x2x2-st": "OK"}))
	})
	for i := range inst.Pods {
		inst.Pods[i].NodeSelector = map[string]string{"example.com/topo": "2x2x2"}
	}
	cfg := enabledConfig()
	cfg.TPUSlicePartitions.TopologyLabel = "example.com/topo"
	cfg.TPUSlicePartitions.IDLabel = "example.com/part-%s"
	cfg.TPUSlicePartitions.StateLabel = "example.com/part-%s-st"
	cfg.TPUSlicePartitions.HealthyState = "OK"
	plan, reason := policy.PlanTPUSliceReplacement(snap, cfg, inst)
	if reason != "" || plan == nil || plan.FreePartitions != 1 {
		t.Fatalf("plan=%+v reason=%q, want the one configured partition", plan, reason)
	}
}
