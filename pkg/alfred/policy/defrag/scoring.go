// Package defrag implements Policy #1 (Defragmentation) of OEP-0008.
//
// This file is the fragmentation-scoring pipeline (OEP-0008 §Fragmentation
// scoring): a pure function of the ClusterSnapshot that answers "how much of
// the cluster's free GPU capacity could defragmentation make usable that is
// not usable now?" — per hardware pool (Node.GPUPool), never mixed across
// pools.
package defrag

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/metrics"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// SizeFrag is one (size, Frag) sample of a pool's observed distribution.
type SizeFrag struct {
	Size  int64
	Slots int64
	Frag  float64
}

// PoolScore is the full scoring breakdown for one hardware pool
// (Node.GPUPool).
type PoolScore struct {
	Pool string

	// TotalFree is the pool's schedulable free GPU capacity.
	TotalFree int64
	// PerSize is Frag(c,s) over the ladder, ascending by size.
	PerSize []SizeFrag

	// FObserved is the demand-weighted observed fragmentation.
	FObserved float64
	// FBest is FObserved recomputed on the FFD-repacked hypothetical
	// snapshot (movable instances repacked, everything else fixed),
	// normalized by the same observed TotalFree so the difference
	// measures slot gain, not denominator drift.
	FBest float64
	// FReclaimable = max(0, FObserved - FBest): the share executable
	// OMENative migration could actually fix.
	FReclaimable float64

	// PendingPressure is P(c): age-weighted pressure from pending pods for
	// which executable repacking creates a seat that does not exist in the
	// observed distribution. Ineligible pendings never wake Alfred.
	PendingPressure float64

	// Score = 1 - (1-FReclaimable)(1-P): noisy-OR of the two wake
	// signals.
	Score float64
}

// Scores is the cluster-level scoring result.
type Scores struct {
	PerPool map[string]*PoolScore
	// FragmentationScore is the executable gate value: max over pools. The
	// separate advisory enumeration gate is derived per pool from the exact
	// observed-minus-reclaimable gap and never widens this score.
	FragmentationScore float64
}

// binState is one node's (free, capacity) pair in a real or hypothetical
// free distribution.
type binState struct {
	name string
	free int64
	cap  int64
}

// ComputeScores runs scoring steps 1-4 for every hardware pool. Pure:
// reads the snapshot and config, writes nothing.
func ComputeScores(snap *snapshot.ClusterSnapshot, cfg *config.Config) *Scores {
	scoring := cfg.Policies.Defragmentation.Scoring
	ladder := int64Ladder(scoring.SizeLadder)
	prior := parsePrior(scoring.SizePrior)
	lambda := *scoring.DemandBlendLambda
	tau := cfg.PendingUrgencyTau()

	demand := demandByPoolAndSize(snap, ladder)

	scores := &Scores{PerPool: map[string]*PoolScore{}}
	for _, pool := range snap.GPUPools() {
		cs := scorePool(snap, cfg, pool, ladder, prior, lambda, tau, demand[pool])
		scores.PerPool[pool] = cs
		if cs.Score > scores.FragmentationScore {
			scores.FragmentationScore = cs.Score
		}
	}
	return scores
}

// PublishScores computes and publishes the fragmentation gauges. Its
// signature matches observer.Loop.Scorer, which is how the observation loop
// wires it in.
func PublishScores(snap *snapshot.ClusterSnapshot, cfg *config.Config, m *metrics.Metrics) {
	scores := ComputeScores(snap, cfg)
	// Self-contained reset: a pool whose last node left, or a reload that
	// shortened the ladder, must not leave stale series alerting on
	// capacity that no longer exists. (The observation loop also resets
	// snapshot gauges; this keeps PublishScores correct on its own.)
	m.FragmentationObserved.Reset()
	m.FragmentationReclaimable.Reset()
	m.PendingPressure.Reset()
	m.ClusterFragmentationScore.Set(scores.FragmentationScore)
	for pool, cs := range scores.PerPool {
		for _, sf := range cs.PerSize {
			m.FragmentationObserved.WithLabelValues(pool, fmt.Sprintf("%d", sf.Size)).Set(sf.Frag)
		}
		m.FragmentationReclaimable.WithLabelValues(pool).Set(cs.FReclaimable)
		m.PendingPressure.WithLabelValues(pool).Set(cs.PendingPressure)
	}
}

func scorePool(snap *snapshot.ClusterSnapshot, cfg *config.Config, pool string,
	ladder []int64, prior map[int64]float64, lambda float64, tau time.Duration,
	demandGPUs map[int64]int64) *PoolScore {

	cs := &PoolScore{Pool: pool}

	observed := schedulableBins(snap, cfg, pool)
	for _, bin := range observed {
		cs.TotalFree += bin.free
	}

	weights := demandWeights(ladder, demandGPUs, prior, lambda)
	observedSlots := map[int64]int64{}

	// Step 1 + 2 on the observed distribution.
	for _, size := range ladder {
		slots := slotsForSize(observed, size)
		observedSlots[size] = slots
		frag := fragForSize(slots, size, cs.TotalFree)
		cs.PerSize = append(cs.PerSize, SizeFrag{Size: size, Slots: slots, Frag: frag})
		cs.FObserved += weights[size] * frag
	}

	// Step 3: FFD repack of movable footprints, then re-score on the
	// hypothetical distribution with the same weights — and the SAME
	// denominator: FReclaimable measures the gain in usable free GPUs,
	// so both sides normalize by the observed free capacity. Repacking
	// can consume capacity (a pod seated off an excluded node); letting
	// the denominator shrink with it would fabricate reclaimable
	// fragmentation out of a shrinking base with no slot improvement.
	repacked := repackPool(snap, cfg, pool, observed)
	bestSlots := map[int64]int64{}
	for _, size := range ladder {
		slots := slotsForSize(repacked, size)
		bestSlots[size] = slots
		cs.FBest += weights[size] * fragForSize(slots, size, cs.TotalFree)
	}
	cs.FReclaimable = math.Max(0, cs.FObserved-cs.FBest)

	// Step 4: pending pressure over repack-seatable pendings only.
	cs.PendingPressure = pendingPressure(snap, pool, ladder, observedSlots, bestSlots, tau)

	// Noisy-OR: fixable shape damage or a starving fixable pod each
	// independently justify waking the policy.
	cs.Score = 1 - (1-cs.FReclaimable)*(1-cs.PendingPressure)
	return cs
}

// schedulableBins returns the pool's free distribution over nodes whose
// free capacity is actually usable for placement. Free GPUs on cordoned,
// unhealthy, CA-deleting, or suspect nodes are a mirage — counting them
// would inflate Slots and put Alfred to sleep on a cluster whose "free"
// capacity cannot seat anything.
func schedulableBins(snap *snapshot.ClusterSnapshot, cfg *config.Config, pool string) []binState {
	avoidSpot := *cfg.SpotPolicy.AvoidAsTarget
	var bins []binState
	for _, node := range snap.PoolNodes(pool) {
		if !nodeSchedulable(node, avoidSpot) {
			continue
		}
		bins = append(bins, binState{name: node.Name, free: node.FreeGPUs, cap: node.TotalGPUs})
	}
	sort.Slice(bins, func(i, j int) bool {
		if bins[i].free != bins[j].free {
			return bins[i].free > bins[j].free
		}
		return bins[i].name < bins[j].name
	})
	return bins
}

func nodeSchedulable(node *snapshot.Node, avoidSpot bool) bool {
	if node.Unhealthy || node.Cordoned || node.ScaleDownMarked || node.Suspect {
		return false
	}
	if avoidSpot && node.Preemptible {
		return false
	}
	return true
}

// slotsForSize counts how many size-s demands the distribution can seat.
// Within-node sizes use floor(free/s) per node — floor handles heterogeneous
// node sizes with no special cases (a 4-GPU node contributes zero size-8
// slots because free <= 4). Sizes larger than every node draw slots only
// from fully-free node groups.
func slotsForSize(bins []binState, size int64) int64 {
	var maxCap int64
	for _, bin := range bins {
		if bin.cap > maxCap {
			maxCap = bin.cap
		}
	}
	if maxCap == 0 {
		return 0
	}
	if size <= maxCap {
		var slots int64
		for _, bin := range bins {
			slots += bin.free / size
		}
		return slots
	}
	// Multi-node shape: only fully-free nodes of the group's node shape
	// (maxCap) can join — a smaller fully-free node cannot host a
	// maxCap-sized member pod, so counting it would fake group capacity.
	nodesNeeded := (size + maxCap - 1) / maxCap
	var fullyFree int64
	for _, bin := range bins {
		if bin.cap == maxCap && bin.free == bin.cap {
			fullyFree++
		}
	}
	return fullyFree / nodesNeeded
}

// fragForSize is Frag(c,s) = 1 - Slots*s/TotalFree, defined 0 when
// TotalFree is 0: a fully-utilized pool has nothing migration could fix.
func fragForSize(slots, size, totalFree int64) float64 {
	if totalFree == 0 {
		return 0
	}
	frag := 1 - float64(slots*size)/float64(totalFree)
	if frag < 0 {
		return 0
	}
	return frag
}

// demandWeights blends observed demand share with the static prior:
// w(s) = (1-lambda)*demandShare(s) + lambda*prior(s). The prior keeps latent
// large-shape fragmentation visible when nothing of that shape is deployed
// yet.
func demandWeights(ladder []int64, demandGPUs map[int64]int64, prior map[int64]float64, lambda float64) map[int64]float64 {
	var totalDemand int64
	for _, gpus := range demandGPUs {
		totalDemand += gpus
	}
	weights := map[int64]float64{}
	for _, size := range ladder {
		var share float64
		if totalDemand > 0 {
			share = float64(demandGPUs[size]) / float64(totalDemand)
		}
		weights[size] = (1-lambda)*share + lambda*prior[size]
	}
	return weights
}

// demandByPoolAndSize counts pool GPU demand at each snapped ladder size,
// from running OME instance footprints (per-pod, per-node — a 2x8 Instance
// is 16 GPUs of size-8 demand) plus pending pods. LWS-backed instances count
// too: they are real demand even though selection will refuse to move them.
// A pending pod with no pool attribution counts toward every pool.
func demandByPoolAndSize(snap *snapshot.ClusterSnapshot, ladder []int64) map[string]map[int64]int64 {
	demand := map[string]map[int64]int64{}
	add := func(pool string, size, gpus int64) {
		bySize, ok := demand[pool]
		if !ok {
			bySize = map[int64]int64{}
			demand[pool] = bySize
		}
		bySize[size] += gpus
	}
	nodePool := func(name string) (string, bool) {
		node, ok := snap.Nodes[name]
		if !ok {
			return "", false
		}
		return node.GPUPool, true
	}

	for _, workload := range snap.Workloads {
		for _, component := range workload.Components {
			for _, instance := range component.Instances {
				for _, pod := range instance.Pods {
					if pod.GPUs == 0 || pod.Node == "" {
						continue
					}
					pool, ok := nodePool(pod.Node)
					if !ok {
						continue
					}
					add(pool, snapToLadder(pod.GPUs, ladder), pod.GPUs)
				}
			}
		}
	}
	for _, pending := range snap.PendingPods {
		if pending.GPUsNeeded == 0 {
			continue
		}
		size := snapToLadder(pending.GPUsNeeded, ladder)
		if pending.GPUPool != "" {
			add(pending.GPUPool, size, pending.GPUsNeeded)
			continue
		}
		for _, pool := range snap.GPUPools() {
			add(pool, size, pending.GPUsNeeded)
		}
	}
	return demand
}

// snapToLadder rounds a footprint up to the smallest ladder size that holds
// it; footprints beyond the ladder count at the largest size (the only shape
// within-node fragmentation can deny them).
func snapToLadder(gpus int64, ladder []int64) int64 {
	if len(ladder) == 0 {
		// Validation forbids an empty ladder; guard the final-element
		// index anyway for direct callers.
		return 0
	}
	for _, size := range ladder {
		if gpus <= size {
			return size
		}
	}
	return ladder[len(ladder)-1]
}

// pendingPressure is P(c) = 1 - prod over eligible p of (1 - u(p)) with
// u(p) = 1 - exp(-age/tau). A pending is eligible only when executable
// repacking creates a seat (Slots_observed == 0 and Slots_best >= 1).
func pendingPressure(snap *snapshot.ClusterSnapshot, pool string, ladder []int64,
	observedSlots, bestSlots map[int64]int64, tau time.Duration) float64 {
	if tau <= 0 {
		// Config validation forbids a non-positive tau; guard anyway
		// so a direct caller cannot push NaN (0/0) into the gauges.
		return 0
	}
	survive := 1.0
	now := snap.Timestamp
	for _, pending := range snap.PendingPods {
		if pending.GPUsNeeded == 0 {
			continue
		}
		if pending.GPUPool != "" && pending.GPUPool != pool {
			continue
		}
		size := snapToLadder(pending.GPUsNeeded, ladder)
		if observedSlots[size] >= 1 || bestSlots[size] < 1 {
			continue
		}
		age := now.Sub(pending.PendingSince)
		if age < 0 {
			age = 0
		}
		urgency := 1 - math.Exp(-age.Minutes()/tau.Minutes())
		survive *= 1 - urgency
	}
	return 1 - survive
}

func int64Ladder(ladder []int) []int64 {
	out := make([]int64, len(ladder))
	for i, size := range ladder {
		out[i] = int64(size)
	}
	return out
}

func parsePrior(prior map[string]float64) map[int64]float64 {
	out := map[int64]float64{}
	var sum float64
	for key, weight := range prior {
		// Canonical whole-string parse: neither "8x" nor the aliases
		// "08" / "+8" may map to size 8 — colliding keys would make the
		// kept weight depend on map iteration order (config validation
		// applies the same rule).
		size, err := strconv.ParseInt(key, 10, 64)
		if err == nil && size > 0 && strconv.FormatInt(size, 10) == key {
			out[size] = weight
			sum += weight
		}
	}
	// Validation tolerates a prior sum in [0.99, 1.01]; normalize so the
	// blended weights stay exactly convex and no score can leave [0, 1].
	if sum > 0 {
		for size := range out {
			out[size] /= sum
		}
	}
	return out
}
