package gangpack

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kube-scheduler/framework"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

const testKey = "nvidia.com/gpu.clique"

var gpu = v1.ResourceName("nvidia.com/gpu")

func gpuNode(name, domain, alloc string) *v1.Node {
	labels := map[string]string{}
	if domain != "" {
		labels[testKey] = domain
	}
	status := v1.NodeStatus{Allocatable: v1.ResourceList{v1.ResourcePods: resource.MustParse("110")}}
	if alloc != "" {
		status.Allocatable[gpu] = resource.MustParse(alloc)
	}
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}, Status: status}
}

func gpuPod(req string) *v1.Pod {
	return &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{
		Resources: v1.ResourceRequirements{Requests: v1.ResourceList{gpu: resource.MustParse(req)}},
	}}}}
}

// gangGPUPod is a gpuPod that is also a gang member (namespace + pod-group label),
// so placement's inspectBoundGangMembers can match it while free-ness still comes from its
// gpu request.
func gangGPUPod(ns, pg, req string) *v1.Pod {
	p := gpuPod(req)
	p.Namespace = ns
	p.Labels = map[string]string{podGroupLabel: pg}
	return p
}

// cordoned returns the node marked unschedulable (spec.unschedulable), as a drain
// or an autoscaler cordon would.
func cordoned(n *v1.Node) *v1.Node {
	n.Spec.Unschedulable = true
	return n
}

// tainted returns the node with a NoSchedule taint of the given key.
func tainted(n *v1.Node, key string) *v1.Node {
	n.Spec.Taints = append(n.Spec.Taints, v1.Taint{Key: key, Effect: v1.TaintEffectNoSchedule})
	return n
}

// TestNodeSchedulable: a node is schedulable for the pod only when it is not
// cordoned and its NoSchedule taints are tolerated — capacity alone is not enough.
func TestNodeSchedulable(t *testing.T) {
	pod := gpuPod("4") // no tolerations
	if !nodeSchedulable(gpuNode("n", "a", "4"), pod) {
		t.Fatal("plain node should be schedulable")
	}
	if nodeSchedulable(cordoned(gpuNode("n", "a", "4")), pod) {
		t.Fatal("cordoned node should not be schedulable")
	}
	if nodeSchedulable(tainted(gpuNode("n", "a", "4"), "x"), pod) {
		t.Fatal("node with an untolerated NoSchedule taint should not be schedulable")
	}
	tolerant := gpuPod("4")
	tolerant.Spec.Tolerations = []v1.Toleration{{Key: "x", Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule}}
	if !nodeSchedulable(tainted(gpuNode("n", "a", "4"), "x"), tolerant) {
		t.Fatal("node with a tolerated taint should be schedulable")
	}
	if nodeSchedulable(nil, pod) {
		t.Fatal("nil node should not be schedulable")
	}
}

// TestFreeByDomainExcludesUnschedulable: a cordoned (or untolerated-taint) node
// has capacity but is NOT counted free — otherwise Choose would pin a domain the
// framework's Filter then rejects, wedging the gang.
func TestFreeByDomainExcludesUnschedulable(t *testing.T) {
	pod := gpuPod("4")
	nodes := []framework.NodeInfo{
		nodeInfo(cordoned(gpuNode("a1", "a", "4"))),     // a: cordoned
		nodeInfo(tainted(gpuNode("a2", "a", "4"), "x")), // a: untolerated taint
		nodeInfo(gpuNode("b1", "b", "4")),               // b: free
	}
	free := freeByDomain(nodes, testKey, pod)
	if free["a"] != 0 {
		t.Fatalf("domain a free=%d, want 0 (all nodes unschedulable)", free["a"])
	}
	if free["b"] != 1 {
		t.Fatalf("domain b free=%d, want 1", free["b"])
	}
}

func nodeInfo(node *v1.Node, pods ...*v1.Pod) framework.NodeInfo {
	ni := schedulerframework.NewNodeInfo(pods...)
	ni.SetNode(node)
	return ni
}

func TestDomainOf(t *testing.T) {
	if got := domainOf(gpuNode("n", "d7", "4"), testKey); got != "d7" {
		t.Fatalf("domainOf = %q want d7", got)
	}
	if got := domainOf(gpuNode("n", "", "4"), testKey); got != "" {
		t.Fatalf("domainOf(no label) = %q want empty", got)
	}
	if got := domainOf(nil, testKey); got != "" {
		t.Fatalf("domainOf(nil) = %q want empty", got)
	}
}

// TestFreeByDomain: build topology.FreeByDomain from a framework node snapshot,
// free-ness inferred from whether the GANG POD's own requests fit — no configured
// accelerator name. Only nodes that both carry a domain label and could hold the
// pod's resource (allocatable > 0 for it) participate; free = the pod fits; an
// all-occupied domain still appears at 0; irrelevant and no-label nodes ignored.
func TestFreeByDomain(t *testing.T) {
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("n1", "a", "4")),              // free
		nodeInfo(gpuNode("n2", "a", "4")),              // free
		nodeInfo(gpuNode("n3", "a", "4"), gpuPod("4")), // occupied -> a stays 2
		nodeInfo(gpuNode("n4", "b", "4")),              // free
		nodeInfo(gpuNode("n5", "d", "4"), gpuPod("4")), // only-occupied domain -> d:0
		nodeInfo(gpuNode("n6", "c", "")),               // labeled but no GPU -> ignored
		nodeInfo(gpuNode("n7", "", "4")),               // no domain -> ignored
	}
	// The gang pod declares what it needs (gpu: 4); free-ness comes from that fit.
	got := freeByDomain(nodes, testKey, gpuPod("4"))
	want := topology.FreeByDomain{"a": 2, "b": 1, "c": 0, "d": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("freeByDomain = %v want %v", got, want)
	}
}

func TestNodeFitsPodChecksStandardResourcesAndPodCapacity(t *testing.T) {
	node := gpuNode("n", "a", "4")
	node.Status.Allocatable[v1.ResourceCPU] = resource.MustParse("2")
	node.Status.Allocatable[v1.ResourceMemory] = resource.MustParse("2Gi")
	pod := gangGPUPod("team", "pf", "4")
	pod.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("1500m")
	pod.Spec.Containers[0].Resources.Requests[v1.ResourceMemory] = resource.MustParse("1Gi")
	if !nodeFitsPod(nodeInfo(node), pod) {
		t.Fatal("pod should fit all advertised resources")
	}
	cpuUsed := gpuPod("0")
	cpuUsed.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = resource.MustParse("1")
	if nodeFitsPod(nodeInfo(node, cpuUsed), pod) {
		t.Fatal("remaining CPU is insufficient")
	}
	node.Status.Allocatable[v1.ResourcePods] = resource.MustParse("1")
	if nodeFitsPod(nodeInfo(node, &v1.Pod{}), gpuPod("1")) {
		t.Fatal("node pod capacity is exhausted")
	}
}

func TestNodeFitsPodChecksSelectorAndRequiredAffinity(t *testing.T) {
	node := gpuNode("n", "a", "4")
	node.Labels["pool"] = "serving"
	pod := gpuPod("4")
	pod.Spec.NodeSelector = map[string]string{"pool": "other"}
	if nodeFitsPod(nodeInfo(node), pod) {
		t.Fatal("nodeSelector mismatch must reject the node")
	}
	pod.Spec.NodeSelector = nil
	pod.Spec.Affinity = &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{{
			MatchExpressions: []v1.NodeSelectorRequirement{{Key: "pool", Operator: v1.NodeSelectorOpIn, Values: []string{"other"}}},
		}}},
	}}
	if nodeFitsPod(nodeInfo(node), pod) {
		t.Fatal("required node affinity mismatch must reject the node")
	}
}

func TestFeasibleByDomainMatchesHeterogeneousMembers(t *testing.T) {
	aGPU := gpuNode("a-gpu", "a", "8")
	aGPU.Labels["kind"] = "gpu"
	aCPU := gpuNode("a-cpu", "a", "4")
	aCPU.Labels["kind"] = "cpu"
	b1 := gpuNode("b1", "b", "8")
	b1.Labels["kind"] = "cpu"
	b2 := gpuNode("b2", "b", "8")
	b2.Labels["kind"] = "cpu"
	large := gpuPod("8")
	large.Spec.NodeSelector = map[string]string{"kind": "gpu"}
	small := gpuPod("4")
	small.Spec.NodeSelector = map[string]string{"kind": "cpu"}

	got := feasibleByDomain([]framework.NodeInfo{nodeInfo(aGPU), nodeInfo(aCPU), nodeInfo(b1), nodeInfo(b2)}, testKey, []*v1.Pod{large, small})
	if got["a"] != 2 || got["b"] != 0 {
		t.Fatalf("feasibleByDomain = %v, want a:2 b:0", got)
	}
}

func TestFeasibleAtLeastIgnoresInfeasibleSurplusMember(t *testing.T) {
	n1 := gpuNode("n1", "a", "4")
	n1.Labels["pool"] = "usable"
	n2 := gpuNode("n2", "a", "4")
	n2.Labels["pool"] = "usable"
	nodes := []framework.NodeInfo{nodeInfo(n1), nodeInfo(n2)}
	current := gangGPUPod("team", "pf", "4")
	impossible := gangGPUPod("team", "pf", "4")
	impossible.Spec.NodeSelector = map[string]string{"pool": "absent"}
	feasible := gangGPUPod("team", "pf", "4")

	free := feasibleAtLeastByDomain(nodes, testKey, []*v1.Pod{current, impossible, feasible}, 2)
	if free["a"] < 2 {
		t.Fatalf("free = %v, want current plus a feasible surplus member", free)
	}
	candidates := matchingCandidateNodesForNeed(nodes, testKey, "a", []*v1.Pod{current, impossible, feasible}, 2)
	if !candidates.Equal(sets.New("n1", "n2")) {
		t.Fatalf("candidates = %v, want both nodes", candidates)
	}
}

// TestPlacedInDomain counts a gang's members occupying a specific domain — the
// input the reservation reconciler uses to learn a pinned gang's real footprint.
// Only the named gang's pods in the named domain count: a sibling gang's members,
// this gang's members in another domain, and non-gang pods are all excluded.
func TestPlacedInDomain(t *testing.T) {
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4"), gangPod("team", "pf")),     // pf in a  -> count
		nodeInfo(gpuNode("a2", "a", "4"), gangPod("team", "pf")),     // pf in a  -> count
		nodeInfo(gpuNode("a3", "a", "4"), gangPod("team", "decode")), // other gang -> skip
		nodeInfo(gpuNode("a4", "a", "4")),                            // free -> skip
		nodeInfo(gpuNode("b1", "b", "4"), gangPod("team", "pf")),     // pf but wrong domain -> skip
	}
	if got := placedInDomain(nodes, "team", "pf", testKey, "a"); got != 2 {
		t.Fatalf("placedInDomain(team/pf, a) = %d want 2", got)
	}
	if got := placedInDomain(nodes, "team", "pf", testKey, "b"); got != 1 {
		t.Fatalf("placedInDomain(team/pf, b) = %d want 1", got)
	}
	if got := placedInDomain(nodes, "team", "absent", testKey, "a"); got != 0 {
		t.Fatalf("placedInDomain(team/absent, a) = %d want 0", got)
	}
}

// TestGangRoomIsCountedInMembers: the feasibility reading measures a node's room
// in gang members, the way placement fills it, not in nodes. A two-member gang
// fits one node with room for two just as it fits two nodes with room for one
// each; it does not fit one node with room for one.
func TestGangRoomIsCountedInMembers(t *testing.T) {
	leader := gangGPUPod("team", "pf", "1")
	leader.Name = "leader"
	worker := gangGPUPod("team", "pf", "1")
	worker.Name = "worker"
	gang := []*v1.Pod{leader, worker}
	cases := []struct {
		name       string
		nodes      []framework.NodeInfo
		feasible   bool
		candidates sets.Set[string]
	}{
		{
			name:       "one node with room for two",
			nodes:      []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "2"))},
			feasible:   true,
			candidates: sets.New("a1"),
		},
		{
			name:       "one node with room for one",
			nodes:      []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "1"))},
			feasible:   false,
			candidates: sets.New[string](),
		},
		{
			name:       "two nodes with room for one each",
			nodes:      []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "1")), nodeInfo(gpuNode("a2", "a", "1"))},
			feasible:   true,
			candidates: sets.New("a1", "a2"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			free := feasibleAtLeastByDomain(tc.nodes, testKey, gang, len(gang))
			if fits := free["a"] >= len(gang); fits != tc.feasible {
				t.Fatalf("free = %v, want domain a feasible=%v for a gang of %d", free, tc.feasible, len(gang))
			}
			candidates := matchingCandidateNodesForNeed(tc.nodes, testKey, "a", gang, len(gang))
			if !candidates.Equal(tc.candidates) {
				t.Fatalf("candidates = %v, want %v", candidates, tc.candidates)
			}
		})
	}
}

// TestFeasibleByDomainReadsSharedRoomPerDomain: a domain fits a gang when its
// members can share one node with room for them, exactly as when each member
// has a node of its own; a domain whose nodes hold one member only does not.
func TestFeasibleByDomainReadsSharedRoomPerDomain(t *testing.T) {
	gang := []*v1.Pod{gangGPUPod("team", "pf", "1"), gangGPUPod("team", "pf", "1")}
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "2")),                                    // a: one node, room for two
		nodeInfo(gpuNode("b1", "b", "1")), nodeInfo(gpuNode("b2", "b", "1")), // b: two nodes, room for one each
		nodeInfo(gpuNode("c1", "c", "1")),              // c: one node, room for one
		nodeInfo(gpuNode("d1", "d", "2"), gpuPod("1")), // d: one node, one slot taken
	}
	got := feasibleByDomain(nodes, testKey, gang)
	want := topology.FreeByDomain{"a": 2, "b": 2, "c": 0, "d": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("feasibleByDomain = %v want %v", got, want)
	}
}

// TestMembersThatMustStayApartKeepOneNodeEach: a gang whose members carry a
// required anti-affinity against each other is never read as sharing a node,
// however much room the node has.
func TestMembersThatMustStayApartKeepOneNodeEach(t *testing.T) {
	apart := func(name string) *v1.Pod {
		p := gangGPUPod("team", "pf", "1")
		p.Name = name
		p.Spec.Affinity = &v1.Affinity{PodAntiAffinity: &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{
				TopologyKey:   v1.LabelHostname,
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{podGroupLabel: "pf"}},
			}},
		}}
		return p
	}
	gang := []*v1.Pod{apart("leader"), apart("worker")}
	oneNode := []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "2"))}
	if free := feasibleByDomain(oneNode, testKey, gang); free["a"] != 0 {
		t.Fatalf("free = %v, want a:0 (members that must stay apart cannot share a1)", free)
	}
	twoNodes := []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "2")), nodeInfo(gpuNode("a2", "a", "1"))}
	if free := feasibleByDomain(twoNodes, testKey, gang); free["a"] != 2 {
		t.Fatalf("free = %v, want a:2 (one member per node)", free)
	}
}
