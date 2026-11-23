package tpudefrag

import (
	"fmt"
	"testing"

	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// TestAncestorChipsFindsTheLargestWholeFreeAncestor: A's single-host Instance
// fits only in K, since B's hosts are another machine type. Every K host lies
// in the wholly free 2x2x4 K, so A is never drained, even when K-0's 2x2x2
// is unhealthy or missing from the labels.
func TestAncestorChipsFindsTheLargestWholeFreeAncestor(t *testing.T) {
	for name, mutate := range map[string]func(*snapshot.Node){
		"2x2x2 unhealthy": func(n *snapshot.Node) { n.Labels[stateLabel("2x2x2")] = "UNHEALTHY" },
		"2x2x2 not labeled": func(n *snapshot.Node) {
			delete(n.Labels, idLabel("2x2x2"))
			delete(n.Labels, stateLabel("2x2x2"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			snap := newCluster(t).partition("pod-a", "A").partition("pod-a", "B").partition("pod-a", "K").full("C").
				instance("prod/a", "A-0").instance("prod/b", "B-0").build()
			snap.Workloads[key("prod/b", 0).Workload].Movable = false
			for i := 0; i < 4; i++ {
				snap.Nodes[fmt.Sprintf("B-%d", i)].Labels["node.kubernetes.io/instance-type"] = "other"
			}
			snap.Workloads[key("prod/a", 0).Workload].Components["engine"].Instances[0].Pods[0].
				NodeSelector["node.kubernetes.io/instance-type"] = "tpu7x"
			mutate(snap.Nodes["K-0"])
			mutate(snap.Nodes["K-1"])
			if plan := Build(snap, testConfig(), now, nil); len(plan.Intents) != 0 {
				t.Fatalf("intents = %+v, want none: every placement breaks the whole free K", plan.Intents)
			}
		})
	}
}

// TestLayoutCountsHostsByTheirOwnSmallestShape: hosts whose pool labels no
// 2x2x1 partition still count as free in their TPU pod's packing bound.
func TestLayoutCountsHostsByTheirOwnSmallestShape(t *testing.T) {
	snap := newCluster(t).partition("pod-a", "A").partition("pod-b", "Z").build()
	for i := 0; i < 4; i++ {
		n := snap.Nodes[fmt.Sprintf("Z-%d", i)]
		delete(n.Labels, idLabel("2x2x1"))
		delete(n.Labels, stateLabel("2x2x1"))
	}
	stats := buildLayout(snap, testConfig()).stats()
	if stats["pod-a"].freeHosts != 4 || stats["pod-b"].freeHosts != 4 {
		t.Fatalf("free hosts pod-a=%d pod-b=%d, want 4 and 4", stats["pod-a"].freeHosts, stats["pod-b"].freeHosts)
	}
}
