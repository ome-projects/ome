package placement

import (
	"context"
	"maps"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/plan"
)

// singleMoveEmpty requires physical absence on every retained registration.
// A new admission race cannot inherit occupied or unknown movement resources.
func (r *Reconciler) singleMoveEmpty(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, standing *placementObservations) bool {
	winner, known := standing.get(winnerCluster(source))
	if !known || winner.state != homeAbsent || len(source.Status.Placement.Candidates) == 0 {
		return false
	}
	inventory := map[string]v1beta1.CandidatePlacement{}
	for _, cluster := range clusters {
		inventory[cluster.Name] = v1beta1.CandidatePlacement{Cluster: cluster.Name, Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: cluster.UID}}
	}
	for _, candidate := range source.Status.Placement.Candidates {
		inventory[candidate.Cluster] = candidate
	}
	for _, name := range slices.Sorted(maps.Keys(inventory)) {
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		observed, err := r.observePlannedHome(cctx, source, inventory[name], declaredComponents(source))
		cancel()
		if err != nil || !observed.Home.Known || !observed.Home.Absent {
			return false
		}
	}
	return true
}

// emptySingleMove revokes movement authority before a fresh admission race.
// Its caller must verify all homes are empty and the loss grace has expired.
func emptySingleMove(source *v1beta1.InferenceService) plan.Proposal {
	out := plan.Proposal{Mode: v1beta1.PlacementModeSingle, InputDigest: source.Status.Placement.Plan.InputDigest, Assignments: map[string]v1beta1.CandidateAllocationStatus{}}
	for _, candidate := range source.Status.Placement.Candidates {
		out.Assignments[candidate.Cluster] = v1beta1.CandidateAllocationStatus{ClusterUID: candidate.Allocation.ClusterUID, RaceCandidate: true}
	}
	return out
}
