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

// prepare turns a proposal into the status the store persists: every cluster's
// allocation checked and put in canonical order, the Single-mode rules applied,
// the replica totals summed and the plan identity computed.
func prepare(source *v1beta1.InferenceService, proposal Proposal) (*v1beta1.PlacementStatus, error) {
	if proposal.Mode != "" && proposal.Mode != source.Spec.Placement.Mode {
		return nil, fmt.Errorf("placement plan mode differs from source intent")
	}
	if proposal.InputDigest == "" || proposal.UnassignedReplicas < 0 || proposal.OriginalUnassignedReplicas < 0 {
		return nil, fmt.Errorf("placement plan requires an input digest and nonnegative unassigned floors")
	}
	var candidates []v1beta1.CandidatePlacement
	for name, input := range proposal.Assignments {
		allocation, err := validateAssignment(source, proposal, name, input)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: allocation})
	}
	if err := validateSingleMode(proposal); err != nil {
		return nil, err
	}
	return planStatus(source, proposal, candidates)
}

// validateAssignment checks one cluster's allocation against the proposal and
// returns the copy the plan stores, its lists in canonical order. Every error
// names the cluster.
func validateAssignment(source *v1beta1.InferenceService, proposal Proposal, name string, input v1beta1.CandidateAllocationStatus) (*v1beta1.CandidateAllocationStatus, error) {
	if name == "" || input.ClusterUID == "" || input.OriginalReplicas < 0 || input.CurrentReplicas < 0 || input.DesiredReplicas < 0 {
		return nil, fmt.Errorf("cluster %q requires an identity and nonnegative floors", name)
	}
	assignment := input.DeepCopy()
	if countsNeedClusterAuthority(proposal.Mode, assignment) {
		return nil, fmt.Errorf("cluster %q full-policy counts require resolved home authority", name)
	}
	for _, policy := range []*v1beta1.PlacementHomePolicy{assignment.CurrentHome, assignment.DesiredHome} {
		if policy == nil {
			continue
		}
		if err := validateFullPolicy(name, policy, proposal.PauseSurge); err != nil {
			return nil, err
		}
	}
	if assignment.DesiredHome != nil && reservedReplicas(assignment.DesiredHome) != assignment.DesiredReplicas {
		return nil, fmt.Errorf("cluster %q desired home differs from its reserved floor", name)
	}
	if assignment.CurrentHome != nil && reservedReplicas(assignment.CurrentHome) != assignment.CurrentReplicas && !(assignment.CurrentReplicas == 0 && assignment.DrainRequested) {
		return nil, fmt.Errorf("cluster %q current home differs from its reserved floor", name)
	}
	if assignment.Weight != nil && (*assignment.Weight < 0 || assignment.Capacity != nil) {
		return nil, fmt.Errorf("cluster %q requires a nonnegative static weight or capacity, not both", name)
	}
	if !normalizeMatchingTerms(assignment) {
		return nil, fmt.Errorf("cluster %q matching terms must be distinct nonnegative indexes", name)
	}
	if err := normalizeComponentMatches(assignment); err != nil {
		return nil, fmt.Errorf("cluster %q: %w", name, err)
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
	return assignment, nil
}

// countsNeedClusterAuthority reports a full-policy plan (All or Single) that
// counts current or desired replicas on a cluster without the policy record
// that authorizes them.
func countsNeedClusterAuthority(mode v1beta1.PlacementMode, a *v1beta1.CandidateAllocationStatus) bool {
	if mode != v1beta1.PlacementModeAll && mode != v1beta1.PlacementModeSingle {
		return false
	}
	return (a.CurrentReplicas > 0 && a.CurrentHome == nil) || (a.DesiredReplicas > 0 && a.DesiredHome == nil)
}

// validateFullPolicy checks one cluster's full policy record: it names the
// source intent it came from, its replica counts are valid (and all positive
// while surge is paused), and they are stored in component order.
func validateFullPolicy(name string, policy *v1beta1.PlacementHomePolicy, pauseSurge bool) error {
	if policy.InputDigest == "" {
		return fmt.Errorf("cluster %q full home requires source intent identity", name)
	}
	if _, err := protocol.ValidateReplicaFloors(policy.ReplicaFloors); err != nil {
		return fmt.Errorf("cluster %q: %w", name, err)
	}
	if pauseSurge {
		if err := protocol.ValidatePositiveReplicaFloors(policy.ReplicaFloors); err != nil {
			return fmt.Errorf("cluster %q: %w", name, err)
		}
	}
	slices.SortFunc(policy.ReplicaFloors, func(a, b v1beta1.PlacementComponentFloor) int { return cmp.Compare(a.Component, b.Component) })
	return nil
}

// reservedReplicas is the whole-replica count a validated policy record holds.
func reservedReplicas(policy *v1beta1.PlacementHomePolicy) int32 {
	replicas, _ := protocol.ValidateReplicaFloors(policy.ReplicaFloors)
	return replicas
}

// normalizeMatchingTerms sorts the cluster's affinity term indexes and reports
// whether they are distinct and nonnegative; an empty list is stored as nil.
func normalizeMatchingTerms(assignment *v1beta1.CandidateAllocationStatus) bool {
	slices.Sort(assignment.MatchingTerms)
	for i, term := range assignment.MatchingTerms {
		if term < 0 || (i > 0 && term == assignment.MatchingTerms[i-1]) {
			return false
		}
	}
	if len(assignment.MatchingTerms) == 0 {
		assignment.MatchingTerms = nil
	}
	return true
}

// validateSingleMode applies the rules that tie a Single plan's committed
// cluster, its move and its race together; other modes may name neither a
// committed cluster nor a move.
func validateSingleMode(proposal Proposal) error {
	if proposal.Mode != v1beta1.PlacementModeSingle && proposal.Winner != "" {
		return fmt.Errorf("only Single placement can commit a winner")
	}
	if proposal.SingleMove != nil && (proposal.Mode != v1beta1.PlacementModeSingle || proposal.Winner == "" || !proposal.PauseSurge) {
		return fmt.Errorf("Single movement requires a retained winner and shared surge pause")
	}
	if proposal.Mode != v1beta1.PlacementModeSingle {
		return nil
	}
	moving := proposal.SingleMove != nil
	selected := proposal.Winner
	if moving {
		selected = proposal.SingleMove.Selected
	}
	if proposal.Winner != "" {
		committed, exists := proposal.Assignments[proposal.Winner]
		if !exists || !committedKeepsFullAuthority(committed, moving) {
			return fmt.Errorf("Single winner requires retained full-home authority")
		}
	}
	if moving && selected != "" {
		replacement, exists := proposal.Assignments[selected]
		if !exists || !replacementHasFullAuthority(replacement, selected == proposal.Winner) {
			return fmt.Errorf("selected Single replacement requires full-home authority")
		}
	}
	for name, a := range proposal.Assignments {
		if desiredOutsideRace(name, a, selected, proposal.Winner) {
			return fmt.Errorf("Single desired home is neither a race candidate nor its committed winner")
		}
	}
	return nil
}

// committedKeepsFullAuthority reports whether the committed cluster of a
// Single plan still holds the record that lets it serve: while it stays, its
// desired policy; during a move, its original replicas or a current policy
// with running replicas. A race candidate never counts as committed.
func committedKeepsFullAuthority(a v1beta1.CandidateAllocationStatus, moving bool) bool {
	if a.RaceCandidate {
		return false
	}
	if !moving {
		return a.DesiredHome != nil
	}
	return a.OriginalReplicas > 0 || (a.CurrentHome != nil && a.CurrentReplicas > 0)
}

// replacementHasFullAuthority reports whether the cluster a Single move
// selects holds a desired policy with replicas and a current policy with
// running replicas. The committed cluster may be selected again with none
// running when its original replicas are on record.
func replacementHasFullAuthority(a v1beta1.CandidateAllocationStatus, committed bool) bool {
	if a.RaceCandidate || a.DesiredHome == nil || a.DesiredReplicas <= 0 {
		return false
	}
	restoringOriginal := committed && a.OriginalReplicas > 0 && a.CurrentReplicas == 0
	return restoringOriginal || (a.CurrentHome != nil && a.CurrentReplicas > 0)
}

// desiredOutsideRace reports a cluster that a Single plan gives desired
// replicas or a desired policy although it is not the selected cluster or,
// with nothing selected, neither a race candidate nor the committed cluster.
func desiredOutsideRace(name string, a v1beta1.CandidateAllocationStatus, selected, committed string) bool {
	if a.DesiredReplicas <= 0 && a.DesiredHome == nil {
		return false
	}
	if selected != "" {
		return name != selected
	}
	return !a.RaceCandidate && name != committed
}

// planStatus assembles the status the store persists: the plan header, the
// candidates in cluster order, the replica totals and the plan identity, a
// digest over everything that decides the allocation.
func planStatus(source *v1beta1.InferenceService, proposal Proposal, candidates []v1beta1.CandidatePlacement) (*v1beta1.PlacementStatus, error) {
	out := &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{
		Mode: proposal.Mode, Winner: proposal.Winner, SingleMove: proposal.SingleMove.DeepCopy(), AdoptionDigest: proposal.AdoptionDigest,
		SourceUID: source.UID, ObservedGeneration: source.Generation, InputDigest: proposal.InputDigest,
		PauseSurge:         proposal.PauseSurge,
		UnassignedReplicas: proposal.UnassignedReplicas, OriginalUnassignedReplicas: proposal.OriginalUnassignedReplicas,
	}, Candidates: candidates}
	// The original counts are summed only to catch an overflow; the plan keeps
	// them per cluster.
	originalTotal := int64(proposal.OriginalUnassignedReplicas)
	for _, candidate := range out.Candidates {
		if int64(candidate.Allocation.DesiredReplicas) > math.MaxInt64-out.Plan.AssignedReplicas || int64(candidate.Allocation.OriginalReplicas) > math.MaxInt64-originalTotal {
			return nil, fmt.Errorf("placement floor exceeds int64")
		}
		out.Plan.AssignedReplicas += int64(candidate.Allocation.DesiredReplicas)
		originalTotal += int64(candidate.Allocation.OriginalReplicas)
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

// normalizeComponentMatches orders per-component matches canonically, so the
// same match always persists the same plan.
func normalizeComponentMatches(assignment *v1beta1.CandidateAllocationStatus) error {
	slices.SortFunc(assignment.ComponentMatches, func(a, b v1beta1.ComponentAffinityMatch) int { return cmp.Compare(a.Component, b.Component) })
	for i := range assignment.ComponentMatches {
		match := &assignment.ComponentMatches[i]
		if !placementComponent(match.Component) {
			return fmt.Errorf("component match names unsupported component %q", match.Component)
		}
		if i > 0 && match.Component == assignment.ComponentMatches[i-1].Component {
			return fmt.Errorf("component %q has more than one match", match.Component)
		}
		if match.Weight != nil && (*match.Weight < 0 || assignment.Capacity != nil) {
			return fmt.Errorf("component %q requires a nonnegative static weight, and none beside capacity", match.Component)
		}
	}
	if len(assignment.ComponentMatches) == 0 {
		assignment.ComponentMatches = nil
	}
	return nil
}

// placementComponent reports a component a plan can match: the engine, the
// decoder or the router.
func placementComponent(component v1beta1.ComponentType) bool {
	return component == v1beta1.EngineComponent || component == v1beta1.DecoderComponent || component == v1beta1.RouterComponent
}

// normalizeCapacity checks one cluster's capacity sample and puts its lists
// in canonical order: the demand contract carries the sample's fingerprint,
// every pool is identified and from the one report, no resource and flavor
// repeats, and the sample's replica count is the smallest pool quotient.
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
	slices.SortFunc(sample.Pools, comparePools)
	capacity := int64(math.MaxInt64)
	first := &sample.Pools[0]
	for i := range sample.Pools {
		pool := &sample.Pools[i]
		if !poolIdentified(pool) {
			return fmt.Errorf("capacity pool requires identified hardware, demand, and complete attribution")
		}
		if i > 0 && comparePools(*pool, sample.Pools[i-1]) == 0 {
			return fmt.Errorf("capacity contains duplicate resource/flavor evidence")
		}
		if !sameReport(pool, first) {
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

// comparePools orders pools by resource name, then by flavor.
func comparePools(a, b v1beta1.PlacementCapacityPool) int {
	return cmp.Or(cmp.Compare(a.ResourceName, b.ResourceName), cmp.Compare(a.ResourceFlavor, b.ResourceFlavor))
}

// poolIdentified reports a pool that names its hardware, carries a positive
// demand and a nonnegative allocatable count, comes from an identified and
// dated report, and has a complete attribution with its flavor identity.
func poolIdentified(pool *v1beta1.PlacementCapacityPool) bool {
	return pool.ResourceName != "" && pool.ResourceFlavor != "" &&
		pool.Demand > 0 && pool.Allocatable >= 0 &&
		pool.ReportUID != "" && pool.ReportResourceVersion != "" && !pool.ObservedAt.IsZero() &&
		pool.Attribution.Complete && pool.Attribution.FlavorUID != "" && pool.Attribution.FlavorSetHash != ""
}

// sameReport reports two pools from one report and one attribution mapping.
func sameReport(a, b *v1beta1.PlacementCapacityPool) bool {
	return a.ReportUID == b.ReportUID && a.ReportResourceVersion == b.ReportResourceVersion && a.Attribution.FlavorSetHash == b.Attribution.FlavorSetHash
}

// CanonicalCapacity checks a capacity sample and its rendering contract on an owned
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

// identity is the part of a placement status that decides the plan ID: each
// cluster's allocation with its hardware, report UID and demand mapping. The
// report's liveness fields, the plan's own ID and revision, and the serving
// state are left out.
func identity(status *v1beta1.PlacementStatus) *v1beta1.PlacementStatus {
	out := status.DeepCopy()
	out.Cluster, out.Phase, out.Endpoint = "", "", nil
	out.Plan.ID, out.Plan.Revision = "", 0
	for i := range out.Candidates {
		candidate := &out.Candidates[i]
		*candidate = v1beta1.CandidatePlacement{Cluster: candidate.Cluster, Allocation: candidate.Allocation}
		// The plan ID leaves MinWeight out: when present it equals Weight, whose
		// key the digest already carries, and plans stored before MinWeight
		// existed keep their ID.
		if candidate.Allocation != nil {
			clearMinWeight(candidate.Allocation)
		}
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

// clearMinWeight removes MinWeight from an allocation and its component
// matches. Plan identity and the split input digest leave MinWeight out, so
// both match those of plans stored before MinWeight existed.
func clearMinWeight(a *v1beta1.CandidateAllocationStatus) {
	a.MinWeight = nil
	for i := range a.ComponentMatches {
		a.ComponentMatches[i].MinWeight = nil
	}
}

// WithoutMinWeight returns a copy of the allocation without MinWeight, for
// digests that must match those of allocations stored without it.
func WithoutMinWeight(a v1beta1.CandidateAllocationStatus) v1beta1.CandidateAllocationStatus {
	out := a.DeepCopy()
	clearMinWeight(out)
	return *out
}

// equivalent reports whether proposed decides the same allocation as a
// stored plan that has a revision, liveness fields aside.
func equivalent(stored, proposed *v1beta1.PlacementStatus) bool {
	if stored == nil || stored.Plan == nil || stored.Plan.Revision <= 0 {
		return false
	}
	return equalAllocation(identity(stored), identity(proposed))
}
