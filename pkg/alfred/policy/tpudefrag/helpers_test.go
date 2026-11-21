package tpudefrag

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const (
	podLabel      = "cloud.google.com/gce-topology-block"
	topologyLabel = "cloud.google.com/gke-tpu-topology"
)

// cluster builds synthetic TPU pods out of 2x2x4 partitions. Partition P has
// hosts P-0..P-3; its 2x2x2 halves are P-01 (hosts 0 and 1) and P-23 (hosts 2
// and 3); each host is its own 2x2x1 partition.
type cluster struct {
	t *testing.T
	b *testutil.SnapshotBuilder
}

func newCluster(t *testing.T) *cluster {
	return &cluster{t: t, b: testutil.NewSnapshot()}
}

func idLabel(shape string) string {
	return fmt.Sprintf("cloud.google.com/gke-tpu-partition-%s-id", shape)
}
func stateLabel(shape string) string {
	return fmt.Sprintf("cloud.google.com/gke-tpu-partition-%s-state", shape)
}

// partition adds the four hosts of 2x2x4 partition id in TPU pod pod.
func (c *cluster) partition(pod, id string, opts ...func(host int) []testutil.NodeOption) *cluster {
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("%s-%d", id, i)
		half := id + "-01"
		if i >= 2 {
			half = id + "-23"
		}
		labels := map[string]string{
			podLabel:                           pod,
			idLabel("2x2x4"):                   id,
			stateLabel("2x2x4"):                "HEALTHY",
			idLabel("2x2x2"):                   half,
			stateLabel("2x2x2"):                "HEALTHY",
			idLabel("2x2x1"):                   name,
			stateLabel("2x2x1"):                "HEALTHY",
			"node.kubernetes.io/instance-type": "tpu7x",
		}
		nodeOpts := []testutil.NodeOption{testutil.NodeLabels(labels)}
		for _, o := range opts {
			nodeOpts = append(nodeOpts, o(i)...)
		}
		c.b.WithNode(name, "", 0, nodeOpts...)
	}
	return c
}

// instance adds an OMENative TPU Instance with one pod per node.
func (c *cluster) instance(workload string, nodes ...string) *cluster {
	c.b.WithMultiPodTPUInstance(workload, v1beta1.EngineComponent, constants.OMENative, 4, nodes...)
	return c
}

// build returns the snapshot with every TPU pod slice-provisioned on the
// topology its pod count implies.
func (c *cluster) build() *snapshot.ClusterSnapshot {
	snap := c.b.Build()
	topology := map[int]string{1: "2x2x1", 2: "2x2x2", 4: "2x2x4"}
	for _, w := range snap.Workloads {
		for _, comp := range w.Components {
			for _, inst := range comp.Instances {
				for i := range inst.Pods {
					inst.Pods[i].TPUSliceProvisioned = true
					inst.Pods[i].NodeSelector = map[string]string{topologyLabel: topology[len(inst.Pods)]}
				}
			}
		}
	}
	return snap
}

// testConfig enables the policy on 2x2x4 in recommend-only mode.
func testConfig() *config.Config {
	cfg := config.Default()
	on := true
	cfg.TPUSliceMigrationEnabled = &on
	td := &cfg.Policies.TPUDefragmentation
	td.Enabled = &on
	td.TargetShapes = []string{"2x2x4"}
	td.MinGap = 1
	td.MaxBusyHostsPerPartition = 3
	return cfg
}

// executeConfig is testConfig in execute mode with a window covering
// testutil.ReferenceTime (a Thursday, 12:00 UTC).
func executeConfig() *config.Config {
	cfg := testConfig()
	cfg.Mode = config.ModeExecute
	cfg.MaintenanceWindows = []config.MaintenanceWindow{{Days: []string{"Thu"}, Start: "00:00", End: "23:59"}}
	return cfg
}

// fenced marks a host cordoned by drain with the given fence fields.
func fenced(drain, partition string, fencedAt time.Time, free bool) func(int) []testutil.NodeOption {
	return func(int) []testutil.NodeOption {
		raw, _ := json.Marshal(Fence{Drain: drain, Unit: "2x2x4", Partition: partition,
			Deadline: testutil.ReferenceTime.Add(time.Hour), FencedAt: fencedAt, FreeAtFence: free})
		return []testutil.NodeOption{testutil.NodeCordoned(), func(n *snapshot.Node) { n.DefragmentationFence = string(raw) }}
	}
}

func key(workload string, instance int32) InstanceKey {
	ns, name, _ := cut(workload)
	return InstanceKey{Workload: types.NamespacedName{Namespace: ns, Name: name}, Component: v1beta1.EngineComponent, Instance: instance}
}

func cut(s string) (string, string, bool) {
	for i := range s {
		if s[i] == '/' {
			return s[:i], s[i+1:], true
		}
	}
	return "", s, false
}
