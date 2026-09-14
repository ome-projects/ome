package defrag

import "sigs.k8s.io/ome/pkg/alfred/policy"

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

// simulateSurgePlan applies a shared atomic placement proof to Defrag's
// scoring bins. Replacement capacity is consumed first; source capacity is
// released only after the complete plan has been validated.
func simulateSurgePlan(observed []binState, moves []policy.SurgeMove) ([]binState, bool) {
	bins := cloneBins(observed)
	index := binIndexByName(bins)

	for _, move := range moves {
		i, ok := index[move.TargetNode]
		if !ok || move.GPUs <= 0 || bins[i].free < move.GPUs {
			return nil, false
		}
		bins[i].free -= move.GPUs
	}
	for _, move := range moves {
		if i, ok := index[move.FromNode]; ok {
			bins[i].free += move.GPUs
			if bins[i].free > bins[i].cap {
				bins[i].free = bins[i].cap
			}
		}
	}
	return bins, true
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
