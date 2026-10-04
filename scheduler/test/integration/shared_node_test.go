package integration

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"
)

// TestGangSharesTheOnlyNodeWithRoom: a two-member gang whose only room is two
// slots on one node binds both members to that node. The plugin reads a node's
// room in members, so a packed pool whose only free capacity is what a gang
// vacated on one node takes the gang back.
//
// One domain: a node with room for two members beside a node that is full. The
// gang must land whole on the node with room, not wait for a second node.
func TestGangSharesTheOnlyNodeWithRoom(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-shared-node"
	createNamespace(t, tc, ns)

	if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode("sn-a1", "a", 2), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node sn-a1: %v", err)
	}
	if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode("sn-a2", "a", 1), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create node sn-a2: %v", err)
	}
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, preBoundPod("sn-occupant", ns, "sn-a2"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create occupant of sn-a2: %v", err)
	}

	pg := schedutil.MakePG("shared", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}
	podNames := []string{"sn-0", "sn-1"}
	for _, name := range podNames {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, "shared"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", name, err)
		}
	}

	for _, name := range podNames {
		if node := waitForPodBound(t, tc, ns, name, 30*time.Second); node != "sn-a1" {
			t.Errorf("pod %s bound to %s, want sn-a1, the one node with room for the gang", name, node)
		}
	}
}
