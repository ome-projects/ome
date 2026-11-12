package gangpack

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kube-scheduler/framework"
)

// memberAsking is a gang member of team/pf with the given requests.
func memberAsking(cpu, memory, storage, accelerators string) *v1.Pod {
	pod := gangGPUPod("team", "pf", accelerators)
	requests := pod.Spec.Containers[0].Resources.Requests
	requests[v1.ResourceCPU] = resource.MustParse(cpu)
	requests[v1.ResourceMemory] = resource.MustParse(memory)
	requests[v1.ResourceEphemeralStorage] = resource.MustParse(storage)
	return pod
}

// nodeWithRoom is node n1 in domain a with 4 CPUs, 8Gi of memory, 20Gi of
// ephemeral storage, 8 accelerators and 3 pod slots.
func nodeWithRoom() *v1.Node {
	node := gpuNode("n1", "a", "8")
	node.Status.Allocatable[v1.ResourceCPU] = resource.MustParse("4")
	node.Status.Allocatable[v1.ResourceMemory] = resource.MustParse("8Gi")
	node.Status.Allocatable[v1.ResourceEphemeralStorage] = resource.MustParse("20Gi")
	node.Status.Allocatable[v1.ResourcePods] = resource.MustParse("3")
	return node
}

// runningOnTheNode is the pod nodeWithRoom already runs: 2 CPUs, 4Gi, 10Gi and
// 4 accelerators, so 2 CPUs, 4Gi, 10Gi, 4 accelerators and 2 slots are left.
func runningOnTheNode() *v1.Pod { return memberAsking("2", "4Gi", "10Gi", "4") }

// memberFitsNode reads the member against the node that already runs the
// given pods, as the room reading does for every node of a domain.
func memberFitsNode(node *v1.Node, member *v1.Pod, running ...*v1.Pod) bool {
	entry := nodeInfo(node, running...)
	return needOf(member).fits(entry, entry.Node())
}

// withNoScheduleTaint marks the node with a NoSchedule taint of the given key.
func withNoScheduleTaint(node *v1.Node, key string) {
	node.Spec.Taints = append(node.Spec.Taints, v1.Taint{Key: key, Effect: v1.TaintEffectNoSchedule})
}

// requiringNodeLabel is a required node affinity on one label value.
func requiringNodeLabel(key, value string) *v1.Affinity {
	return &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{{
			MatchExpressions: []v1.NodeSelectorRequirement{{Key: key, Operator: v1.NodeSelectorOpIn, Values: []string{value}}},
		}}},
	}}
}

// preferringNodeLabel is a preferred node affinity on one label value.
func preferringNodeLabel(key, value string) *v1.Affinity {
	return &v1.Affinity{NodeAffinity: &v1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []v1.PreferredSchedulingTerm{{
			Weight: 10,
			Preference: v1.NodeSelectorTerm{MatchExpressions: []v1.NodeSelectorRequirement{
				{Key: key, Operator: v1.NodeSelectorOpIn, Values: []string{value}},
			}},
		}},
	}}
}

// TestMemberFitReadsTheNodeAgainstTheMember holds which nodes a member may
// land on at all, room aside: the node takes new pods, its hard taints are
// tolerated, a member bound by name is bound to this node, and the member's
// node selector and required node affinity match. Preferences keep nobody out.
func TestMemberFitReadsTheNodeAgainstTheMember(t *testing.T) {
	for _, tt := range []struct {
		name   string
		node   func(*v1.Node)
		member func(*v1.Pod)
		want   bool
	}{
		{name: "a member the node has room for", want: true},
		{name: "a cordoned node", node: func(n *v1.Node) { n.Spec.Unschedulable = true }},
		{name: "a NoSchedule taint the member does not tolerate", node: func(n *v1.Node) { withNoScheduleTaint(n, "drain") }},
		{name: "a NoExecute taint the member does not tolerate", node: func(n *v1.Node) {
			n.Spec.Taints = []v1.Taint{{Key: "drain", Effect: v1.TaintEffectNoExecute}}
		}},
		{name: "a PreferNoSchedule taint keeps nobody out", node: func(n *v1.Node) {
			n.Spec.Taints = []v1.Taint{{Key: "drain", Effect: v1.TaintEffectPreferNoSchedule}}
		}, want: true},
		{name: "a NoSchedule taint the member tolerates", node: func(n *v1.Node) { withNoScheduleTaint(n, "drain") }, member: func(p *v1.Pod) {
			p.Spec.Tolerations = []v1.Toleration{{Key: "drain", Operator: v1.TolerationOpExists, Effect: v1.TaintEffectNoSchedule}}
		}, want: true},
		{name: "a member bound by name to another node", member: func(p *v1.Pod) { p.Spec.NodeName = "n2" }},
		{name: "a member bound by name to this node", member: func(p *v1.Pod) { p.Spec.NodeName = "n1" }, want: true},
		{name: "a node selector the node does not match", member: func(p *v1.Pod) { p.Spec.NodeSelector = map[string]string{"pool": "other"} }},
		{name: "a node selector the node matches", node: func(n *v1.Node) { n.Labels["pool"] = "serving" }, member: func(p *v1.Pod) {
			p.Spec.NodeSelector = map[string]string{"pool": "serving"}
		}, want: true},
		{name: "a required node affinity the node does not match", member: func(p *v1.Pod) { p.Spec.Affinity = requiringNodeLabel("pool", "other") }},
		{name: "a required node affinity the node matches", node: func(n *v1.Node) { n.Labels["pool"] = "serving" }, member: func(p *v1.Pod) {
			p.Spec.Affinity = requiringNodeLabel("pool", "serving")
		}, want: true},
		{name: "a preferred node affinity the node does not match keeps nobody out", member: func(p *v1.Pod) {
			p.Spec.Affinity = preferringNodeLabel("pool", "other")
		}, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node, member := nodeWithRoom(), memberAsking("1", "1Gi", "1Gi", "1")
			if tt.node != nil {
				tt.node(node)
			}
			if tt.member != nil {
				tt.member(member)
			}
			if got := memberFitsNode(node, member, runningOnTheNode()); got != tt.want {
				t.Fatalf("fits = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMemberFitCountsEachDimensionToTheUnit holds that the member fits when
// each request is at most what the node has left, CPU in millicores, memory
// and storage in bytes, each accelerator kind in units, and that one pod slot
// must be free beside them.
func TestMemberFitCountsEachDimensionToTheUnit(t *testing.T) {
	fpga := v1.ResourceName("example.com/fpga")
	askingAnFPGA := memberAsking("1", "1Gi", "1Gi", "1")
	askingAnFPGA.Spec.Containers[0].Resources.Requests[fpga] = resource.MustParse("1")
	for _, tt := range []struct {
		name   string
		member *v1.Pod
		slots  string
		want   bool
	}{
		{name: "exactly the CPU, memory, storage and accelerators left", member: memberAsking("2", "4Gi", "10Gi", "4"), want: true},
		{name: "one millicore more than is left", member: memberAsking("2001m", "4Gi", "10Gi", "4")},
		{name: "one byte of memory more than is left", member: memberAsking("2", "4294967297", "10Gi", "4")},
		{name: "one byte of storage more than is left", member: memberAsking("2", "4Gi", "10737418241", "4")},
		{name: "one accelerator more than is left", member: memberAsking("2", "4Gi", "10Gi", "5")},
		{name: "an accelerator kind the node does not have", member: askingAnFPGA},
		{name: "a member that asks for nothing", member: memberAsking("0", "0", "0", "0"), want: true},
		{name: "the last pod slot", member: memberAsking("1", "1Gi", "1Gi", "1"), slots: "2", want: true},
		{name: "no pod slot left", member: memberAsking("1", "1Gi", "1Gi", "1"), slots: "1"},
		{name: "a member that asks for nothing still needs a pod slot", member: memberAsking("0", "0", "0", "0"), slots: "1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			node := nodeWithRoom()
			if tt.slots != "" {
				node.Status.Allocatable[v1.ResourcePods] = resource.MustParse(tt.slots)
			}
			if got := memberFitsNode(node, tt.member, runningOnTheNode()); got != tt.want {
				t.Fatalf("fits = %v, want %v", got, tt.want)
			}
		})
	}
}

// entryWithoutTotals is a snapshot entry whose allocatable or requested total
// is missing; every other reading comes from the entry it wraps.
type entryWithoutTotals struct {
	framework.NodeInfo
	allocatable framework.Resource
	requested   framework.Resource
}

func (e entryWithoutTotals) GetAllocatable() framework.Resource { return e.allocatable }
func (e entryWithoutTotals) GetRequested() framework.Resource   { return e.requested }

// TestMemberFitRefusesWhatItCannotRead holds that a missing entry, node,
// total or member is never a fit, and never a crash.
func TestMemberFitRefusesWhatItCannotRead(t *testing.T) {
	node := nodeWithRoom()
	entry := nodeInfo(node, runningOnTheNode())
	member := memberAsking("1", "1Gi", "1Gi", "1")
	for _, tt := range []struct {
		name string
		got  bool
	}{
		{name: "no snapshot entry", got: needOf(member).fits(nil, node)},
		{name: "no node behind the entry", got: needOf(member).fits(entry, nil)},
		{name: "no member", got: needOf(nil).fits(entry, node)},
		{name: "an entry without an allocatable total", got: needOf(member).fits(entryWithoutTotals{NodeInfo: entry, requested: entry.GetRequested()}, node)},
		{name: "an entry without a requested total", got: needOf(member).fits(entryWithoutTotals{NodeInfo: entry, allocatable: entry.GetAllocatable()}, node)},
	} {
		if tt.got {
			t.Errorf("%s: fits = true, want false", tt.name)
		}
	}
	if !needOf(member).fits(entry, node) {
		t.Fatal("fits = false for a member the node has room for, want true")
	}
}
