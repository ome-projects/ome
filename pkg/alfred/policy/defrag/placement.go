package defrag

import (
	"slices"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

// RevalidatePlacement scores the complete simulated replacement placement,
// keyed by original source Pod, instead of the candidate's arithmetic plan.
// snap must be built from the accepted scheduling capture. The same baseline,
// weights and denominator apply before and after the whole instance move.
// This proves simulated benefit only; migration hints do not bind live targets.
func RevalidatePlacement(snap *snapshot.ClusterSnapshot, cfg *config.Config, candidate policy.Candidate,
	targets map[types.NamespacedName]string) (policy.Candidate, bool) {
	var current policy.Candidate
	found := false
	for _, c := range (&Policy{}).Evaluate(snap, cfg) {
		if c.Executable && c.Workload == candidate.Workload && c.Component == candidate.Component &&
			c.Instance == candidate.Instance && c.FromNode == candidate.FromNode {
			current, found = c, true
			break
		}
	}
	if !found || candidate.Policy != PolicyName {
		return policy.Candidate{}, false
	}
	w := snap.Workloads[current.Workload]
	var instance *snapshot.Instance
	for _, inst := range w.Components[current.Component].Instances {
		if inst.Index == current.Instance {
			instance = inst
			break
		}
	}
	if instance == nil || len(targets) != len(instance.Pods) {
		return policy.Candidate{}, false
	}
	plan, ok := policy.PlanAtomicSurge(snap, cfg, w, instance)
	if !ok {
		return policy.Candidate{}, false
	}
	pool := ""
	for _, pod := range instance.Pods {
		target := targets[types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}]
		if target == "" || !slices.Contains(plan.PlacementTargetNodes, target) {
			return policy.Candidate{}, false
		}
		if pod.GPUs > 0 {
			pool = snap.Nodes[pod.Node].GPUPool
			if snap.Nodes[target].GPUPool != pool {
				return policy.Candidate{}, false
			}
		}
	}
	for i := range plan.Moves {
		move := &plan.Moves[i]
		// SurgeMove.Pod is the validated namespace/name of a GPU source.
		for key, target := range targets {
			if key.String() == move.Pod {
				move.TargetNode = target
				break
			}
		}
	}
	scoring := cfg.Policies.Defragmentation.Scoring
	ladder := int64Ladder(scoring.SizeLadder)
	bins := schedulableBins(snap, cfg, pool)
	var totalFree int64
	for _, bin := range bins {
		totalFree += bin.free
	}
	ctx := &evalCtx{snap: snap, cfg: cfg, pool: pool, bins: bins, ladder: ladder,
		weights:   demandWeights(ladder, demandByPoolAndSize(snap, ladder)[pool], parsePrior(scoring.SizePrior), *scoring.DemandBlendLambda),
		totalFree: totalFree, pendings: poolPendings(snap, pool), costWeight: costWeight(cfg.Policies.Defragmentation.Aggressiveness)}
	ctx.before = weightedFrag(bins, ladder, ctx.weights, totalFree)
	after, ok := simulateSurgePlan(bins, plan.Moves)
	if !ok {
		return policy.Candidate{}, false
	}
	current.Benefit, current.Cost, current.Score, current.Emergency = scorePlacement(ctx, w, current.FromNode, after)
	if current.Score <= 0 || !current.Emergency && !maintenanceOpen(cfg, snap.Timestamp) {
		return policy.Candidate{}, false
	}
	return current, true
}
