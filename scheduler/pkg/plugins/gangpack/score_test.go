package gangpack

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kube-scheduler/framework"
)

// scoreNodes runs PreScore + Score + NormalizeScore for a standalone pod over the
// given node snapshot, returning the final normalized score per node name. It
// mirrors what the framework does across the scoring phase.
func scoreNodes(t *testing.T, g *GangPack, pod *v1.Pod, nodes []framework.NodeInfo) map[string]int64 {
	t.Helper()
	state := newCycleState()
	if st := g.PreScore(context.Background(), state, pod, nodes); st != nil && !st.IsSuccess() {
		if st.Code() == framework.Skip {
			return nil // packing not applicable this cycle
		}
		t.Fatalf("PreScore returned %v", st)
	}
	scores := make(framework.NodeScoreList, 0, len(nodes))
	for _, ni := range nodes {
		s, st := g.Score(context.Background(), state, pod, ni)
		if st != nil && !st.IsSuccess() {
			t.Fatalf("Score(%s) returned %v", ni.Node().Name, st)
		}
		scores = append(scores, framework.NodeScore{Name: ni.Node().Name, Score: s})
	}
	if st := g.NormalizeScore(context.Background(), state, pod, scores); st != nil && !st.IsSuccess() {
		t.Fatalf("NormalizeScore returned %v", st)
	}
	out := make(map[string]int64, len(scores))
	for _, s := range scores {
		out[s.Name] = s.Score
	}
	return out
}

func packer() *GangPack {
	return &GangPack{topologyKey: testKey, standaloneDomainPacking: true}
}

// TestScore_PacksIntoPartlyUsedDomain is the core 2x2x2 case: two partitions of 2
// nodes each. Partition "a" already has one node occupied (1 free), partition "b"
// is empty (2 free). A single-host standalone replica must be steered to the free
// node of the partly-used partition "a", so partition "b" stays whole.
func TestScore_PacksIntoPartlyUsedDomain(t *testing.T) {
	pod := gpuPod("4")
	occupant := gpuPod("4") // fills a2
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")),           // a: free
		nodeInfo(gpuNode("a2", "a", "4"), occupant), // a: occupied -> a has 1 free
		nodeInfo(gpuNode("b1", "b", "4")),           // b: free
		nodeInfo(gpuNode("b2", "b", "4")),           // b: free -> b has 2 free
	}
	got := scoreNodes(t, packer(), pod, nodes)
	// a1 is in the fuller domain (1 free) -> top score; b nodes (2 free) -> lower.
	if got["a1"] != framework.MaxNodeScore {
		t.Fatalf("a1 score=%d, want %d (pack into partly-used domain)", got["a1"], framework.MaxNodeScore)
	}
	if got["b1"] >= got["a1"] || got["b2"] >= got["a1"] {
		t.Fatalf("empty-domain nodes should score below the partly-used domain: a1=%d b1=%d b2=%d",
			got["a1"], got["b1"], got["b2"])
	}
	if got["b1"] != got["b2"] {
		t.Fatalf("nodes in the same domain must tie: b1=%d b2=%d", got["b1"], got["b2"])
	}
}

// TestScore_AllEmptyDomainsNeutral: the first replica sees only empty domains, so
// there is no packing signal and every node scores 0 (other plugins decide).
func TestScore_AllEmptyDomainsNeutral(t *testing.T) {
	pod := gpuPod("4")
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")),
		nodeInfo(gpuNode("a2", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")),
	}
	got := scoreNodes(t, packer(), pod, nodes)
	for name, s := range got {
		if s != 0 {
			t.Fatalf("all-empty domains should be neutral, %s=%d", name, s)
		}
	}
}

// TestScore_ThreeDomainsGradient: fuller domains rank strictly higher.
// a: 1 free (fullest), b: 2 free, c: 3 free (emptiest).
func TestScore_ThreeDomainsGradient(t *testing.T) {
	pod := gpuPod("4")
	occ := func() *v1.Pod { return gpuPod("4") }
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")),
		nodeInfo(gpuNode("a2", "a", "4"), occ()),
		nodeInfo(gpuNode("a3", "a", "4"), occ()), // a: 1 free
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")),
		nodeInfo(gpuNode("b3", "b", "4"), occ()), // b: 2 free
		nodeInfo(gpuNode("c1", "c", "4")),
		nodeInfo(gpuNode("c2", "c", "4")),
		nodeInfo(gpuNode("c3", "c", "4")), // c: 3 free
	}
	got := scoreNodes(t, packer(), pod, nodes)
	if !(got["a1"] > got["b1"] && got["b1"] > got["c1"]) {
		t.Fatalf("expected a>b>c gradient, got a=%d b=%d c=%d", got["a1"], got["b1"], got["c1"])
	}
}

// TestScore_DisabledIsNeutral: with the toggle off, PreScore skips and no scores
// are produced.
func TestScore_DisabledIsNeutral(t *testing.T) {
	g := &GangPack{topologyKey: testKey, standaloneDomainPacking: false}
	pod := gpuPod("4")
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")),
		nodeInfo(gpuNode("a2", "a", "4"), gpuPod("4")),
		nodeInfo(gpuNode("b1", "b", "4")),
	}
	if got := scoreNodes(t, g, pod, nodes); got != nil {
		t.Fatalf("disabled packing should Skip, got scores %v", got)
	}
}

// TestScore_NoTopologyKeyIsNeutral: without a pool-wide topology key, packing
// cannot apply (standalone pods carry no per-gang key), so PreScore skips.
func TestScore_NoTopologyKeyIsNeutral(t *testing.T) {
	g := &GangPack{standaloneDomainPacking: true} // topologyKey == ""
	pod := gpuPod("4")
	nodes := []framework.NodeInfo{nodeInfo(gpuNode("a1", "a", "4"))}
	if got := scoreNodes(t, g, pod, nodes); got != nil {
		t.Fatalf("no topology key should Skip, got %v", got)
	}
}

// TestScore_PinnedGangMemberSkipped: when PreFilter has pinned a domain (gang
// member), the domain is already fixed; packing must not run and fight it.
func TestScore_PinnedGangMemberSkipped(t *testing.T) {
	g := packer()
	pod := gpuPod("4")
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")),
		nodeInfo(gpuNode("a2", "a", "4"), gpuPod("4")),
		nodeInfo(gpuNode("b1", "b", "4")),
	}
	state := newCycleState()
	writePin(state, "a", gangInfo{key: "ns/g", topologyKey: testKey})
	if st := g.PreScore(context.Background(), state, pod, nodes); st == nil || st.Code() != framework.Skip {
		t.Fatalf("pinned gang member should Skip PreScore, got %v", st)
	}
}

// TestScore_DeferredGangPreservesBestFit verifies that deferring a hard-spread
// gang's pin does not discard GangPack's packing preference. PreScore sees only
// domains with nodes that survived the framework filters, then ranks the fuller
// whole-gang-feasible domain above the emptier one.
func TestScore_DeferredGangPreservesBestFit(t *testing.T) {
	g := &GangPack{}
	pod := gpuPod("4")
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")),
		nodeInfo(gpuNode("b1", "b", "4")),
	}
	state := newCycleState()
	writeDeferredPin(state, &deferredPinState{
		gang: gangInfo{key: "ns/g", topologyKey: testKey},
		need: 2,
		free: map[string]int{"a": 2, "b": 3},
	})

	if status := g.PreScore(context.Background(), state, pod, nodes); !status.IsSuccess() {
		t.Fatalf("PreScore = %v, want Success", status)
	}
	scores := make(framework.NodeScoreList, 0, len(nodes))
	for _, node := range nodes {
		score, status := g.Score(context.Background(), state, pod, node)
		if !status.IsSuccess() {
			t.Fatalf("Score(%s) = %v", node.Node().Name, status)
		}
		scores = append(scores, framework.NodeScore{Name: node.Node().Name, Score: score})
	}
	if status := g.NormalizeScore(context.Background(), state, pod, scores); !status.IsSuccess() {
		t.Fatalf("NormalizeScore = %v", status)
	}
	if scores[0].Score != framework.MaxNodeScore || scores[1].Score != 0 {
		t.Fatalf("deferred gang scores = %+v, want a=%d b=0", scores, framework.MaxNodeScore)
	}
}

// TestScore_NodeWithoutDomainLabelNeutral: a node not in any domain scores 0 even
// while other nodes pack. Uses two real domains so there is a packing gradient
// (a: 1 free = fullest, b: 2 free) for the domainless node to lose to.
func TestScore_NodeWithoutDomainLabelNeutral(t *testing.T) {
	pod := gpuPod("4")
	nodes := []framework.NodeInfo{
		nodeInfo(gpuNode("a1", "a", "4")),
		nodeInfo(gpuNode("a2", "a", "4"), gpuPod("4")), // a: 1 free -> a1 packs
		nodeInfo(gpuNode("b1", "b", "4")),
		nodeInfo(gpuNode("b2", "b", "4")), // b: 2 free
		nodeInfo(gpuNode("x1", "", "4")),  // no domain label
	}
	got := scoreNodes(t, packer(), pod, nodes)
	if got["x1"] != 0 {
		t.Fatalf("node without a domain label should score 0, got %d", got["x1"])
	}
	if got["a1"] <= got["x1"] {
		t.Fatalf("a1 (%d) should outrank the domainless node x1 (%d)", got["a1"], got["x1"])
	}
}
