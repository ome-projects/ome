package defrag

import (
	"sort"

	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// footprint is one pod-shaped block of GPUs a simulated move lifts from a
// source node and re-places on a target.
type footprint struct {
	node string
	gpus int64
	// name is namespace/name of the source pod — the deterministic
	// tie-break for equal-sized footprints (it must travel with the
	// element: a parallel slice would decouple from the sort's swaps).
	name string
}

// instanceFootprints returns the instance's per-pod footprints, largest
// first (pod-name tie-break) for deterministic placement.
func instanceFootprints(inst *snapshot.Instance) []footprint {
	prints := make([]footprint, 0, len(inst.Pods))
	for _, pod := range inst.Pods {
		if pod.GPUs == 0 || pod.Node == "" {
			continue
		}
		prints = append(prints, footprint{
			node: pod.Node,
			gpus: pod.GPUs,
			name: pod.Namespace + "/" + pod.Name,
		})
	}
	sort.SliceStable(prints, func(i, j int) bool {
		if prints[i].gpus != prints[j].gpus {
			return prints[i].gpus > prints[j].gpus
		}
		return prints[i].name < prints[j].name
	})
	return prints
}

// weightedFrag recomputes F_observed over a bin distribution with fixed
// weights and a fixed denominator — the same shared-TotalFree rule scoring
// uses, so simulated deltas measure slot change, not denominator drift.
func weightedFrag(bins []binState, ladder []int64, weights map[int64]float64, totalFree int64) float64 {
	var f float64
	for _, size := range ladder {
		f += weights[size] * fragForSize(slotsForSize(bins, size), size, totalFree)
	}
	return f
}

// cloneBins copies a bin distribution so a simulation never mutates the
// per-pool distribution shared across candidates.
func cloneBins(bins []binState) []binState {
	out := make([]binState, len(bins))
	copy(out, bins)
	return out
}

func binIndexByName(bins []binState) map[string]int {
	index := make(map[string]int, len(bins))
	for i, bin := range bins {
		index[bin.name] = i
	}
	return index
}

// placeThenFree simulates a surge-shaped move: every replacement footprint
// must fit a ranked target while the sources still hold their GPUs; only
// then are the sources freed and the distribution re-scored. ok=false means
// no surge-feasible placement exists (NoSurgeHeadroom). A source on an
// excluded node is not a bin; its freed GPUs stay invisible, exactly like
// scoring's schedulable-bins rule.
func placeThenFree(observed []binState, sources []footprint, ranked []string) (after []binState, targets []string, ok bool) {
	bins := cloneBins(observed)
	index := binIndexByName(bins)

	for _, src := range sources {
		placed := false
		for _, target := range ranked {
			i, isBin := index[target]
			if !isBin || bins[i].free < src.gpus {
				continue
			}
			bins[i].free -= src.gpus
			targets = append(targets, target)
			placed = true
			break
		}
		if !placed {
			return nil, nil, false
		}
	}
	for _, src := range sources {
		if i, isBin := index[src.node]; isBin {
			bins[i].free += src.gpus
			if bins[i].free > bins[i].cap {
				bins[i].free = bins[i].cap
			}
		}
	}
	return bins, targets, true
}

// canSeat reports whether any single bin could seat the pending pod's
// footprint. Demands wider than every node are gang business — one move
// cannot unblock them — and report false by construction.
func canSeat(bins []binState, gpus int64) bool {
	if gpus <= 0 {
		return false
	}
	for _, bin := range bins {
		if bin.free >= gpus {
			return true
		}
	}
	return false
}
