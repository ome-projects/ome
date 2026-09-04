package defrag

import (
	"sort"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/constants"
)

// repackItem is one movable per-pod footprint to be hypothetically replaced.
type repackItem struct {
	gpus int64
	node string
	pod  string
}

// repackPool computes the step-3 hypothetical: lift every movable footprint
// off its node, first-fit-decreasing it back onto the pool's schedulable
// bins, and return the resulting free distribution. Everything else —
// non-OME occupants and every non-executable workload (Raw, LWS, invalid or
// busy OMENative, unavailable executor, disabled surface, pinned volume) —
// stays fixed in place. FFD is the same heuristic family candidate simulation
// uses; global optimality is explicitly not promised (Non-Goals), so F_best
// is a bound, not a plan.
func repackPool(snap *snapshot.ClusterSnapshot, cfg *config.Config, pool string, observed []binState) []binState {
	bins := make([]binState, len(observed))
	copy(bins, observed)
	binIndex := map[string]int{}
	for i, bin := range bins {
		binIndex[bin.name] = i
	}

	items := movableFootprints(snap, cfg, pool)

	// Lift: a movable pod's GPUs return to its node's free pool when the
	// node is a schedulable bin. Pods on excluded nodes (unhealthy,
	// cordoned, ...) are lifted "from nowhere" — they can move off, but
	// their source free capacity is not usable either way.
	for _, item := range items {
		if i, ok := binIndex[item.node]; ok {
			bins[i].free += item.gpus
		}
	}

	// First-fit-decreasing: items descending (name tie-break for
	// determinism), bins fixed in ASCENDING post-lift free order —
	// fullest first. Bin order is what makes this a consolidating
	// repack: trying the emptiest node first would spread the lifted
	// pods right back out and report zero reclaimable fragmentation.
	sort.Slice(items, func(i, j int) bool {
		if items[i].gpus != items[j].gpus {
			return items[i].gpus > items[j].gpus
		}
		return items[i].pod < items[j].pod
	})
	sort.Slice(bins, func(i, j int) bool {
		if bins[i].free != bins[j].free {
			return bins[i].free < bins[j].free
		}
		return bins[i].name < bins[j].name
	})
	binIndex = map[string]int{}
	for i, bin := range bins {
		binIndex[bin.name] = i
	}

	for _, item := range items {
		placed := false
		for i := range bins {
			if bins[i].free >= item.gpus && item.gpus <= bins[i].cap {
				bins[i].free -= item.gpus
				placed = true
				break
			}
		}
		if placed {
			continue
		}
		// Nowhere to place: the pod stays where it is. If its node is
		// a bin, re-consume the lifted capacity; on an excluded node
		// it never entered the distribution. Clamp at zero: an
		// excluded-node pod contributes no lifted capacity, so it can
		// win this pod's seat and leave the home bin over-committed.
		// The clamp errs conservative — it can only understate the
		// repacked free capacity, never invent it.
		if i, ok := binIndex[item.node]; ok {
			bins[i].free -= item.gpus
			if bins[i].free < 0 {
				bins[i].free = 0
			}
		}
	}
	return bins
}

// movableFootprints enumerates the per-pod footprints candidate enumeration
// could ever emit as executable — the only mass a repack may move. Multi-pod
// instances contribute per-pod items deliberately: per the OEP's footprint
// model, a node-shape at a time is the unit of blocking, instance pods need
// not co-locate (cross-node adjacency is out of scope, Q-039), and F_best is
// a bound, not a plan — candidate simulation is what enforces
// instance-atomic moves.
func movableFootprints(snap *snapshot.ClusterSnapshot, cfg *config.Config, pool string) []repackItem {
	var items []repackItem
	for _, workload := range snap.Workloads {
		for _, component := range workload.Components {
			for _, instance := range component.Instances {
				if !movableForRepack(snap, cfg, workload, component, instance, pool) {
					continue
				}
				for _, pod := range instance.Pods {
					if pod.GPUs == 0 || pod.Node == "" {
						continue
					}
					node, ok := snap.Nodes[pod.Node]
					if !ok || node.GPUPool != pool {
						continue
					}
					items = append(items, repackItem{gpus: pod.GPUs, node: pod.Node, pod: pod.Namespace + "/" + pod.Name})
				}
			}
		}
	}
	return items
}

// movableForRepack mirrors candidate enumeration's exact executable Alpha
// baseline. RawDeployment and LWS are observed demand only; they never enter
// the FFD hypothetical.
func movableForRepack(snap *snapshot.ClusterSnapshot, cfg *config.Config, workload *snapshot.Workload,
	component *snapshot.Component, instance *snapshot.Instance, pool string) bool {
	if !*cfg.OMENativeMigrationEnabled {
		return false
	}
	if component.DeploymentMode != constants.OMENative {
		return false
	}
	if !instanceInPool(snap, instance, pool) {
		return false
	}
	if modelMovabilityReason(snap, workload) != "" {
		return false
	}
	return omenativeExecutionEligibility(snap, cfg, workload, component, instance, snap.Timestamp) == ""
}
