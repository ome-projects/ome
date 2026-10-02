package gangpack

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kube-scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

// Domain-level bin-packing score for standalone whole-node pods and deferred
// hard-spread gangs.
//
// Standalone pods — e.g. single-host TPU replicas that take one node of a slice
// — are steered toward domains that are already partly used, so partly-filled
// domains fill before empty ones are opened. That keeps whole domains free for
// multi-host gangs instead of letting single-host replicas fragment every slice.
// Gang members normally arrive already pinned from PreFilter. The exception is
// an initial member with a hard topology spread constraint: its pin is deferred
// until Reserve, so this score preserves best-fit ordering among the domains
// that survived the framework's filters.
//
// The built-in node-level MostAllocated cannot do this: accelerator nodes are
// whole-node, so every empty candidate looks identical; the packing signal only
// exists at the domain (topology-label) level.
//
// Assumes homogeneous domain sizes (every slice has the same node count), so a
// domain's free-node count directly measures how full it is.

const preScoreStateKey framework.StateKey = "gangpack.domainpack"

// domainPackState carries the per-cycle domain free-node counts computed once in
// PreScore and read per-node in Score.
type domainPackState struct {
	topologyKey string
	free        topology.FreeByDomain
	maxFree     int
}

// Clone satisfies framework.StateData. free is only read after PreScore, so a
// shallow copy is a full copy for our purposes.
func (s *domainPackState) Clone() framework.StateData {
	c := *s
	return &c
}

// Interface assertions: OMEGangPack also participates in scoring.
var (
	_ framework.PreScorePlugin  = &GangPack{}
	_ framework.ScorePlugin     = &GangPack{}
	_ framework.ScoreExtensions = &GangPack{}
)

// packingTopologyKey returns the domain label to pack an unpinned pod against,
// or "" when packing does not apply: it is disabled, no topology key is
// configured, or the pod is a pinned gang member (domain already chosen in
// PreFilter, so scoring must not fight it).
func (g *GangPack) packingTopologyKey(state framework.CycleState) string {
	if !g.standaloneDomainPacking {
		return ""
	}
	if readPin(state) != nil {
		return ""
	}
	return g.topologyKey
}

// PreScore computes domain packing state once per scheduling cycle, so Score is
// a cheap per-node lookup. Standalone pods count free filtered nodes directly;
// deferred gangs keep the full-gang capacity calculated in PreFilter but drop
// domains with no node left after the regular filters. Returns Skip (which also
// skips Score) when domain packing does not apply.
func (g *GangPack) PreScore(_ context.Context, state framework.CycleState, pod *v1.Pod, nodes []framework.NodeInfo) *framework.Status {
	if plan := readDeferredPin(state); plan != nil {
		filteredDomains := make(map[string]bool)
		for _, node := range nodes {
			if node != nil && node.Node() != nil {
				filteredDomains[domainOf(node.Node(), plan.gang.topologyKey)] = true
			}
		}
		free := make(topology.FreeByDomain)
		maxFree := 0
		for domain, capacity := range plan.free {
			if !filteredDomains[domain] {
				continue
			}
			free[domain] = capacity
			if capacity > maxFree {
				maxFree = capacity
			}
		}
		if len(free) == 0 {
			return framework.NewStatus(framework.Skip)
		}
		state.Write(preScoreStateKey, &domainPackState{topologyKey: plan.gang.topologyKey, free: free, maxFree: maxFree})
		return nil
	}
	tk := g.packingTopologyKey(state)
	if tk == "" {
		return framework.NewStatus(framework.Skip)
	}
	free := freeByDomain(nodes, tk, pod)
	if len(free) == 0 {
		return framework.NewStatus(framework.Skip)
	}
	maxFree := 0
	for _, f := range free {
		if f > maxFree {
			maxFree = f
		}
	}
	state.Write(preScoreStateKey, &domainPackState{topologyKey: tk, free: free, maxFree: maxFree})
	return nil
}

// Score ranks a candidate node by how full its domain already is: fewer free nodes
// in the domain → higher raw score (MostAllocated at domain granularity). Nodes
// with no domain label are neutral. Raw range is [0, maxFree]; NormalizeScore
// rescales it to [0, MaxNodeScore].
func (g *GangPack) Score(_ context.Context, state framework.CycleState, _ *v1.Pod, nodeInfo framework.NodeInfo) (int64, *framework.Status) {
	s := readDomainPackState(state)
	if s == nil {
		return 0, nil
	}
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return 0, nil
	}
	domain := domainOf(nodeInfo.Node(), s.topologyKey)
	if domain == "" {
		return 0, nil
	}
	free, ok := s.free[domain]
	if !ok {
		return 0, nil
	}
	raw := int64(s.maxFree - free)
	if raw < 0 {
		raw = 0
	}
	return raw, nil
}

// ScoreExtensions returns the plugin so NormalizeScore runs.
func (g *GangPack) ScoreExtensions() framework.ScoreExtensions { return g }

// NormalizeScore rescales the raw per-domain-fullness scores to the framework's
// [0, MaxNodeScore] range. When every candidate has the same raw score — a single
// domain, or all-empty domains (e.g. the first replica of a service) — the packing
// signal is a wash, so scores are left at zero and other plugins decide.
func (g *GangPack) NormalizeScore(_ context.Context, _ framework.CycleState, _ *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	var maxRaw int64
	for i := range scores {
		if scores[i].Score > maxRaw {
			maxRaw = scores[i].Score
		}
	}
	if maxRaw == 0 {
		return nil
	}
	for i := range scores {
		scores[i].Score = scores[i].Score * framework.MaxNodeScore / maxRaw
	}
	return nil
}

func readDomainPackState(state framework.CycleState) *domainPackState {
	if state == nil {
		return nil
	}
	v, err := state.Read(preScoreStateKey)
	if err != nil {
		return nil
	}
	s, ok := v.(*domainPackState)
	if !ok {
		return nil
	}
	return s
}
