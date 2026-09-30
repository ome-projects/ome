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
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

func prepare(source *v1beta1.InferenceService, proposal Proposal) (*v1beta1.PlacementStatus, error) {
	if proposal.Mode != "" && proposal.Mode != source.Spec.Placement.Mode {
		return nil, fmt.Errorf("placement plan mode differs from source intent")
	}
	if proposal.InputDigest == "" || proposal.UnassignedReplicas < 0 || proposal.OriginalUnassignedReplicas < 0 {
		return nil, fmt.Errorf("placement plan requires an input digest and nonnegative unassigned floors")
	}
	out := &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{
		Mode: proposal.Mode, Winner: proposal.Winner, SingleMove: proposal.SingleMove.DeepCopy(), AdoptionDigest: proposal.AdoptionDigest,
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
		if (proposal.Mode == v1beta1.PlacementModeAll || proposal.Mode == v1beta1.PlacementModeSingle) && ((assignment.CurrentReplicas > 0 && assignment.CurrentHome == nil) || (assignment.DesiredReplicas > 0 && assignment.DesiredHome == nil)) {
			return nil, fmt.Errorf("cluster %q full-policy counts require resolved home authority", name)
		}
		for _, home := range []*v1beta1.PlacementHomePolicy{assignment.CurrentHome, assignment.DesiredHome} {
			if home == nil {
				continue
			}
			if home.InputDigest == "" {
				return nil, fmt.Errorf("cluster %q full home requires source intent identity", name)
			}
			if _, err := protocol.ValidateReplicaFloors(home.ReplicaFloors); err != nil {
				return nil, fmt.Errorf("cluster %q: %w", name, err)
			}
			if proposal.PauseSurge {
				if _, err := protocol.ValidatePositiveReplicaFloors(home.ReplicaFloors); err != nil {
					return nil, fmt.Errorf("cluster %q: %w", name, err)
				}
			}
			slices.SortFunc(home.ReplicaFloors, func(a, b v1beta1.PlacementComponentFloor) int { return cmp.Compare(a.Component, b.Component) })
		}
		if assignment.DesiredHome != nil {
			floor, _ := protocol.ValidateReplicaFloors(assignment.DesiredHome.ReplicaFloors)
			if floor != assignment.DesiredReplicas {
				return nil, fmt.Errorf("cluster %q desired home differs from its reserved floor", name)
			}
		}
		if assignment.CurrentHome != nil {
			floor, _ := protocol.ValidateReplicaFloors(assignment.CurrentHome.ReplicaFloors)
			if floor != assignment.CurrentReplicas && !(assignment.CurrentReplicas == 0 && assignment.DrainRequested) {
				return nil, fmt.Errorf("cluster %q current home differs from its reserved floor", name)
			}
		}
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
		if source.Spec.Placement.Mode == v1beta1.PlacementModeSplitByCapacity && assignment.DesiredReplicas > 0 {
			if assignment.Capacity == nil {
				return nil, fmt.Errorf("cluster %q positive capacity allocation requires demand authority", name)
			}
			if err := protocol.ValidateDemandComponents(source, assignment.Capacity.DemandContract); err != nil {
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
	if proposal.Mode != v1beta1.PlacementModeSingle && proposal.Winner != "" {
		return nil, fmt.Errorf("only Single placement can commit a winner")
	}
	if proposal.SingleMove != nil && (proposal.Mode != v1beta1.PlacementModeSingle || proposal.Winner == "" || !proposal.PauseSurge) {
		return nil, fmt.Errorf("Single movement requires a retained winner and shared surge pause")
	}
	if proposal.Mode == v1beta1.PlacementModeSingle {
		selected := proposal.Winner
		if proposal.SingleMove != nil {
			selected = proposal.SingleMove.Selected
		}
		if proposal.Winner != "" {
			winner, exists := proposal.Assignments[proposal.Winner]
			if !exists || winner.RaceCandidate || (proposal.SingleMove == nil && winner.DesiredHome == nil) || (proposal.SingleMove != nil && winner.OriginalReplicas <= 0 && (winner.CurrentHome == nil || winner.CurrentReplicas <= 0)) {
				return nil, fmt.Errorf("Single winner requires retained full-home authority")
			}
		}
		if proposal.SingleMove != nil && selected != "" {
			a, exists := proposal.Assignments[selected]
			restoringOriginal := selected == proposal.Winner && a.OriginalReplicas > 0 && a.CurrentReplicas == 0
			if !exists || a.DesiredHome == nil || a.DesiredReplicas <= 0 || (!restoringOriginal && (a.CurrentHome == nil || a.CurrentReplicas <= 0)) || a.RaceCandidate {
				return nil, fmt.Errorf("selected Single replacement requires full-home authority")
			}
		}
		for name, a := range proposal.Assignments {
			if (a.DesiredReplicas > 0 || a.DesiredHome != nil) && ((selected != "" && name != selected) || (selected == "" && !a.RaceCandidate && name != proposal.Winner)) {
				return nil, fmt.Errorf("Single desired home is neither a race candidate nor its committed winner")
			}
		}
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
	if err := protocol.ValidateDemand(sample.DemandContract); err != nil {
		return err
	}
	if sample.DemandFingerprint != sample.DemandContract.Fingerprint {
		return fmt.Errorf("capacity and member rendering have different demand fingerprints")
	}
	slices.SortFunc(sample.DemandContract.Components, func(a, b v1beta1.PlacementComponentDemand) int {
		return cmp.Compare(a.Component, b.Component)
	})
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

// CanonicalCapacity validates evidence and its rendering contract on an owned
// copy. Stable list ordering makes equivalent observations share plan identity.
func CanonicalCapacity(sample *v1beta1.PlacementCapacitySample) (*v1beta1.PlacementCapacitySample, error) {
	if sample == nil {
		return nil, fmt.Errorf("capacity evidence is required")
	}
	out := sample.DeepCopy()
	if err := normalizeCapacity(out); err != nil {
		return nil, err
	}
	return out, nil
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
