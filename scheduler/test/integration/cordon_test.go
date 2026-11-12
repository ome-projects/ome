package integration

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"
)

// TestGangAvoidsCordonedDomain covers the cordoned domain: a domain's
// nodes keep their accelerator capacity but become unschedulable (cordoned) —
// as a drain or an autoscaler cordon does. The gang must land in a schedulable
// domain, not take the cordoned one.
//
// The guarded failure mode: if freeByDomain counted cordoned nodes as free,
// Choose could take the cordoned domain; the framework's NodeUnschedulable Filter
// would then reject every node in it, and the gang would wait there forever
// (assignmentStale keeps the assignment because the domain still looks free) while the
// schedulable domain sat unused.
//
// Two domains, each 2 nodes; domain "a" is cordoned. A 2-member gang must bind in
// domain "b". No autoscaler needed — cordoning is just spec.unschedulable.
func TestGangAvoidsCordonedDomain(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-cordon"
	createNamespace(t, tc, ns)

	// Domain "a": 2 gpu nodes, cordoned. Domain "b": 2 gpu nodes, schedulable.
	for _, n := range []string{"cd-a1", "cd-a2"} {
		node := makeGPUNode(n, "a", 1)
		node.Spec.Unschedulable = true
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, node, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create cordoned node %s: %v", n, err)
		}
	}
	for _, n := range []string{"cd-b1", "cd-b2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n, "b", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n, err)
		}
	}

	pg := schedutil.MakePG("cordon", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}
	podNames := []string{"cd-0", "cd-1"}
	for _, name := range podNames {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, "cordon"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", name, err)
		}
	}

	domainB := map[string]bool{"cd-b1": true, "cd-b2": true}
	bound := map[string]bool{}
	for _, name := range podNames {
		node := waitForPodBound(t, tc, ns, name, 30*time.Second)
		if !domainB[node] {
			t.Errorf("pod %s bound to %s, want a node of the schedulable domain b (cd-b1/cd-b2)", name, node)
		}
		if bound[node] {
			t.Errorf("two gang pods bound to the same node %s", node)
		}
		bound[node] = true
	}
}
