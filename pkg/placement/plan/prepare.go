package plan

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func prepare(source *v1beta1.InferenceService, proposal Proposal) (*v1beta1.PlacementStatus, error) {
	if proposal.InputDigest == "" || proposal.UnassignedReplicas < 0 || proposal.OriginalUnassignedReplicas < 0 {
		return nil, fmt.Errorf("placement plan requires an input digest and nonnegative unassigned floors")
	}
	out := &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{
		SourceUID: source.UID, ObservedGeneration: source.Generation, InputDigest: proposal.InputDigest,
		PauseSurge:         proposal.PauseSurge,
		UnassignedReplicas: proposal.UnassignedReplicas, OriginalUnassignedReplicas: proposal.OriginalUnassignedReplicas,
	}}
	originalTotal := int64(proposal.OriginalUnassignedReplicas)
	for name, input := range proposal.Assignments {
		if name == "" || input.ClusterUID == "" || input.OriginalReplicas < 0 || input.CurrentReplicas < 0 || input.DesiredReplicas < 0 {
			return nil, fmt.Errorf("cluster %q requires an identity and nonnegative floors", name)
		}
		assignment := input.DeepCopy()
		if assignment.Weight != nil && (*assignment.Weight < 0 || assignment.Capacity != nil) {
			return nil, fmt.Errorf("cluster %q requires a nonnegative static weight or capacity, not both", name)
		}
		slices.Sort(assignment.MatchingTerms)
		for i, term := range assignment.MatchingTerms {
			if term < 0 || (i > 0 && term == assignment.MatchingTerms[i-1]) {
				return nil, fmt.Errorf("cluster %q matching terms must be distinct nonnegative indexes", name)
			}
		}
		if len(assignment.MatchingTerms) == 0 {
			assignment.MatchingTerms = nil
		}
		if assignment.Capacity != nil {
			if err := normalizeCapacity(assignment.Capacity); err != nil {
				return nil, fmt.Errorf("cluster %q: %w", name, err)
			}
		}
		if int64(input.DesiredReplicas) > math.MaxInt64-out.Plan.AssignedReplicas || int64(input.OriginalReplicas) > math.MaxInt64-originalTotal {
			return nil, fmt.Errorf("placement floor exceeds int64")
		}
		out.Plan.AssignedReplicas += int64(input.DesiredReplicas)
		originalTotal += int64(input.OriginalReplicas)
		out.Candidates = append(out.Candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: assignment})
	}
	if int64(proposal.UnassignedReplicas) > math.MaxInt64-out.Plan.AssignedReplicas {
		return nil, fmt.Errorf("requested floor exceeds int64")
	}
	out.Plan.RequestedReplicas = out.Plan.AssignedReplicas + int64(proposal.UnassignedReplicas)
	slices.SortFunc(out.Candidates, func(a, b v1beta1.CandidatePlacement) int { return cmp.Compare(a.Cluster, b.Cluster) })
	// Only concrete API structs are encoded, so JSON serialization cannot fail.
	encoded, _ := json.Marshal(identity(out))
	digest := sha256.Sum256(encoded)
	out.Plan.ID = hex.EncodeToString(digest[:])
	return out, nil
}

func normalizeCapacity(sample *v1beta1.PlacementCapacitySample) error {
	if sample.DemandFingerprint == "" || sample.Replicas < 0 || len(sample.Pools) == 0 {
		return fmt.Errorf("capacity requires a demand fingerprint, nonnegative replicas, and pool evidence")
	}
	slices.SortFunc(sample.Pools, func(a, b v1beta1.PlacementCapacityPool) int {
		if a.ResourceName != b.ResourceName {
			return cmp.Compare(a.ResourceName, b.ResourceName)
		}
		return cmp.Compare(a.ResourceFlavor, b.ResourceFlavor)
	})
	capacity := int64(math.MaxInt64)
	for i := range sample.Pools {
		pool := &sample.Pools[i]
		if pool.ResourceName == "" || pool.ResourceFlavor == "" || pool.Demand <= 0 || pool.Allocatable < 0 || pool.ReportUID == "" || pool.ReportResourceVersion == "" || pool.ObservedAt.IsZero() || !pool.Attribution.Complete || pool.Attribution.FlavorUID == "" || pool.Attribution.FlavorSetHash == "" {
			return fmt.Errorf("capacity pool requires identified hardware, demand, and complete attribution")
		}
		if i > 0 && pool.ResourceName == sample.Pools[i-1].ResourceName && pool.ResourceFlavor == sample.Pools[i-1].ResourceFlavor {
			return fmt.Errorf("capacity contains duplicate resource/flavor evidence")
		}
		first := sample.Pools[0]
		if pool.ReportUID != first.ReportUID || pool.ReportResourceVersion != first.ReportResourceVersion || pool.Attribution.FlavorSetHash != first.Attribution.FlavorSetHash {
			return fmt.Errorf("capacity pools must belong to one report and attribution mapping")
		}
		if len(pool.Attribution.NodeLabels) == 0 {
			pool.Attribution.NodeLabels = nil
		}
		capacity = min(capacity, pool.Allocatable/pool.Demand)
	}
	if capacity != sample.Replicas {
		return fmt.Errorf("capacity replicas do not match pool evidence")
	}
	return nil
}

// identity removes report liveness data, retaining the hardware, source report
// incarnation, and demand mapping that actually determine an allocation.
func identity(status *v1beta1.PlacementStatus) *v1beta1.PlacementStatus {
	out := status.DeepCopy()
	out.Cluster, out.Phase, out.Endpoint = "", "", nil
	out.Plan.ID, out.Plan.Revision = "", 0
	for i := range out.Candidates {
		candidate := &out.Candidates[i]
		*candidate = v1beta1.CandidatePlacement{Cluster: candidate.Cluster, Allocation: candidate.Allocation}
		if candidate.Allocation != nil && candidate.Allocation.Capacity != nil {
			for j := range candidate.Allocation.Capacity.Pools {
				pool := &candidate.Allocation.Capacity.Pools[j]
				pool.ObservedAt = metav1.Time{}
				pool.ReportResourceVersion = ""
			}
		}
	}
	return out
}

func equivalent(stored, proposed *v1beta1.PlacementStatus) bool {
	if stored == nil || stored.Plan == nil || stored.Plan.Revision <= 0 {
		return false
	}
	return equalAllocation(identity(stored), identity(proposed))
}
