package defrag

import (
	"sort"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// rankTargets returns the pool's feasible placement targets for one per-pod
// footprint of a workload, consolidation-ranked: fullest first (ascending
// free, name tie-break), because filling partial holes is what frees whole
// nodes. The bins already exclude unhealthy, cordoned, CA-deleting, suspect,
// and (by default) spot nodes; this adds the per-candidate filters — source
// exclusion, per-workload spot avoidance, and storage-aware model
// availability. The full ranked list is returned so simulation can preserve
// every distinct node required by a successful atomic placement proof.
func rankTargets(snap *snapshot.ClusterSnapshot, cfg *config.Config, bins []binState,
	w *snapshot.Workload, podGPUs int64, exclude map[string]bool) []string {

	avoidSpot := workloadAvoidsSpotTarget(w, cfg)
	allowed, constrained := modelNodeSet(snap, w)
	feasible := make([]binState, 0, len(bins))
	for _, bin := range bins {
		if exclude[bin.name] || bin.free < podGPUs {
			continue
		}
		node := snap.Nodes[bin.name]
		if node == nil {
			continue
		}
		if avoidSpot && node.Preemptible {
			continue
		}
		if constrained {
			if _, ok := allowed[bin.name]; !ok {
				continue
			}
		}
		feasible = append(feasible, bin)
	}
	sort.Slice(feasible, func(i, j int) bool {
		if feasible[i].free != feasible[j].free {
			return feasible[i].free < feasible[j].free
		}
		return feasible[i].name < feasible[j].name
	})
	ranked := make([]string, len(feasible))
	for i, bin := range feasible {
		ranked[i] = bin.name
	}
	return ranked
}

// workloadAvoidsSpotTarget resolves the per-workload spot-policy annotation
// against the cluster default: "avoid" forces preemptible targets out even
// when the cluster allows them; "migrate" and "ignore" accept whatever the
// cluster-wide avoidAsTarget already left in the bins.
func workloadAvoidsSpotTarget(w *snapshot.Workload, cfg *config.Config) bool {
	switch w.SpotPolicy {
	case "avoid":
		return true
	case "migrate", "ignore":
		return false
	default:
		return *cfg.SpotPolicy.AvoidAsTarget
	}
}

// modelNodeSet is the storage-aware target constraint, built once per
// ranking call for constant-time membership checks. Per-node models allow
// only nodes with the readiness label (the same predicate the scheduler
// enforces through the readiness nodeSelector); PVC-backed models allow the
// nodes satisfying the volume's CSI topology and must NOT be filtered on
// per-node readiness. constrained=false means no model constraint applies —
// no model, an unresolvable one (ResolveError downgrades to advisory
// upstream, before placement runs), or an unconstrained PVC.
func modelNodeSet(snap *snapshot.ClusterSnapshot, w *snapshot.Workload) (allowed map[string]struct{}, constrained bool) {
	if w.ModelKey.Zero() {
		return nil, false
	}
	avail, ok := snap.Models[w.ModelKey]
	if !ok || avail.ResolveError != "" {
		return nil, false
	}
	var nodes []string
	switch avail.Backend {
	case snapshot.BackendPerNode:
		nodes = avail.NodesReady
	case snapshot.BackendPVC:
		if avail.PVCTopologyNodes == nil {
			return nil, false
		}
		nodes = avail.PVCTopologyNodes
	default:
		return nil, false
	}
	allowed = make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		allowed[node] = struct{}{}
	}
	return allowed, true
}
