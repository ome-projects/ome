package placement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/affinity"
	"sigs.k8s.io/ome/pkg/placement/capacity"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// desiredSplitPlan computes the whole matched set independently of readiness,
// admission, and local usage. Retained outgoing allocations remain explicit.
func desiredSplitPlan(source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, samples map[string]capacity.Sample) (plan.Proposal, error) {
	return desiredSplitPlanFromSnapshot(source, clusters, samples, true)
}

// Actuation requires registration UIDs to verify the cluster it writes to.
// Offline snapshots can omit server identities because projection writes none.
func desiredSplitPlanFromSnapshot(source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, samples map[string]capacity.Sample, requireRegistrationUID bool) (plan.Proposal, error) {
	selector, err := placementSelector(source)
	if err != nil {
		return plan.Proposal{}, err
	}
	selectors, err := componentSelectors(source)
	if err != nil {
		return plan.Proposal{}, err
	}
	if selector == nil || (placementMode(source) != v1beta1.PlacementModeSplit && placementMode(source) != v1beta1.PlacementModeSplitByCapacity) {
		return plan.Proposal{}, fmt.Errorf("an explicit split mode is required")
	}
	floor := splitDesiredReplicas(source)
	if floor <= 0 {
		return plan.Proposal{}, replicaCountUnresolved(source)
	}
	proposal := plan.Proposal{Assignments: map[string]v1beta1.CandidateAllocationStatus{}}
	if existing := source.Status.Placement; existing != nil && existing.Plan != nil {
		if existing.Plan.SourceUID != source.UID {
			return plan.Proposal{}, fmt.Errorf("stored plan belongs to a different source incarnation")
		}
		proposal.OriginalUnassignedReplicas = existing.Plan.OriginalUnassignedReplicas
		proposal.PauseSurge = existing.Plan.PauseSurge
		for _, candidate := range existing.Candidates {
			if candidate.Allocation == nil {
				return plan.Proposal{}, fmt.Errorf("stored candidate %q has no allocation authority", candidate.Cluster)
			}
			assignment := *candidate.Allocation.DeepCopy()
			assignment.DesiredReplicas = 0
			assignment.MatchingTerms, assignment.Weight, assignment.MinWeight, assignment.MaxWeight, assignment.ComponentMatches = nil, nil, nil, nil, nil
			if placementMode(source) != v1beta1.PlacementModeSplitByCapacity {
				assignment.Capacity = nil
			}
			proposal.Assignments[candidate.Cluster] = assignment
		}
	}
	weights := map[string]int64{}
	ratioForm := affinity.UsesRatios(source.Spec.Placement.ClusterAffinity)
	for i := range clusters {
		cluster := &clusters[i]
		if !cluster.DeletionTimestamp.IsZero() {
			continue
		}
		match, matches := selector.Match(cluster)
		if !matches {
			continue
		}
		if cluster.Name == "" || (requireRegistrationUID && cluster.UID == "") {
			return plan.Proposal{}, fmt.Errorf("matched registration requires a name and UID")
		}
		if _, duplicate := weights[cluster.Name]; duplicate {
			return plan.Proposal{}, fmt.Errorf("duplicate matched registration %q", cluster.Name)
		}
		assignment := proposal.Assignments[cluster.Name]
		if assignment.ClusterUID != "" && assignment.ClusterUID != cluster.UID {
			return plan.Proposal{}, fmt.Errorf("cluster %q incarnation changed; its standing allocation is unverified", cluster.Name)
		}
		assignment.ClusterUID, assignment.MatchingTerms = cluster.UID, match.TermIndexes
		assignment.ComponentMatches = componentMatchesOn(source, selectors, cluster)
		if placementMode(source) == v1beta1.PlacementModeSplit {
			assignment.Weight, assignment.MinWeight = ptr.To(match.Weight), ptr.To(match.Weight)
			if ratioForm {
				assignment.MaxWeight = ptr.To(match.MaxWeight)
			}
			weights[cluster.Name] = match.Weight
		} else {
			sample, exists := samples[cluster.Name]
			if !exists || sample.ClusterUID != cluster.UID {
				return plan.Proposal{}, fmt.Errorf("CapacityUnknown: verified sample is missing for cluster %q", cluster.Name)
			}
			assignment.Capacity, err = capacityEvidence(sample)
			if err == nil {
				err = protocol.ValidateDemandComponents(source, assignment.Capacity.DemandContract)
			}
			if err != nil {
				return plan.Proposal{}, fmt.Errorf("CapacityUnknown: cluster %q: %w", cluster.Name, err)
			}
			weights[cluster.Name] = sample.Weight
		}
		proposal.Assignments[cluster.Name] = assignment
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return plan.Proposal{}, err
	}
	desired, err := allocation.Apportion(floor, weights, minimums.ceilings(source)[minimums.primary])
	if err != nil {
		return plan.Proposal{}, err
	}
	if err := checkDesiredSplitFloors(source, minimums, desired.Targets, proposal.Assignments); err != nil {
		return plan.Proposal{}, err
	}
	proposal.UnassignedReplicas = desired.Unassigned
	for name, replicas := range desired.Targets {
		assignment := proposal.Assignments[name]
		assignment.DesiredReplicas = replicas
		proposal.Assignments[name] = assignment
	}
	// The digest describes desired intent only. Intermediate movement and fresh
	// heartbeat timestamps cannot reset a transition's original floor. MinWeight
	// repeats Weight, so it stays out and stored plans keep their digest.
	intent := make(map[string]v1beta1.CandidateAllocationStatus, len(weights))
	for name := range weights {
		stored := plan.WithoutMinWeight(proposal.Assignments[name])
		assignment := &stored
		assignment.OriginalReplicas, assignment.CurrentReplicas, assignment.DrainRequested = 0, 0, false
		if assignment.Capacity != nil {
			for i := range assignment.Capacity.Pools {
				assignment.Capacity.Pools[i].ObservedAt = metav1.Time{}
				assignment.Capacity.Pools[i].ReportResourceVersion = ""
			}
		}
		intent[name] = *assignment
	}
	encoded, err := json.Marshal(struct {
		Spec        v1beta1.InferenceServiceSpec
		Annotations map[string]string
		Labels      map[string]string
		Assignments map[string]v1beta1.CandidateAllocationStatus
		Unassigned  int32
	}{Spec: source.Spec, Annotations: planIntentAnnotations(source.Annotations), Labels: source.Labels, Assignments: intent, Unassigned: desired.Unassigned})
	if err != nil {
		return plan.Proposal{}, fmt.Errorf("encode placement intent: %w", err)
	}
	digest := sha256.Sum256(encoded)
	proposal.InputDigest = hex.EncodeToString(digest[:])
	return proposal, nil
}

// componentMatchesOn records the weight of each component other than the
// primary one on a cluster the primary component matched, when the terms set
// weights in static Split: the decoder's, and the router's when routers are
// divided. Every other plan records none, so it apportions by primary targets.
func componentMatchesOn(source *v1beta1.InferenceService, selectors map[v1beta1.ComponentType]*affinity.Selector, cluster *v1beta1.WorkloadCluster) []v1beta1.ComponentAffinityMatch {
	if placementMode(source) != v1beta1.PlacementModeSplit || !affinity.SetsWeights(source.Spec.Placement.ClusterAffinity) {
		return nil
	}
	var out []v1beta1.ComponentAffinityMatch
	for _, component := range placedComponents(source)[1:] {
		if component == v1beta1.RouterComponent && !routerDivided(source) {
			continue
		}
		if match, matches := selectors[component].Match(cluster); matches {
			entry := v1beta1.ComponentAffinityMatch{Component: component, Weight: ptr.To(match.Weight), MinWeight: ptr.To(match.Weight)}
			if affinity.UsesRatios(source.Spec.Placement.ClusterAffinity) {
				entry.MaxWeight = ptr.To(match.MaxWeight)
			}
			out = append(out, entry)
		}
	}
	return out
}

// checkDesiredSplitFloors holds a plan whose secondary floors cannot be met:
// fewer replicas than members that hold the primary component, or a share
// above the component's cap. A share that follows the primary shares stays
// within a cap scaled from the primary's, so only a component apportioned by
// its own weights, or with a cap of its own, is checked.
func checkDesiredSplitFloors(source *v1beta1.InferenceService, minimums splitMinimums, targets map[string]int32, assignments map[string]v1beta1.CandidateAllocationStatus) error {
	weights := splitWeightsOf(source, assignments)
	floors, err := apportionSplitFloors(minimums, targets, weights)
	if err != nil {
		return err
	}
	split := splitSpecOf(source)
	ceilings := minimums.ceilings(source)
	own := weights.ownWeights()
	for _, name := range slices.Sorted(maps.Keys(floors)) {
		for _, share := range floors[name] {
			ceiling := ceilings[share.Component]
			if ceiling <= 0 || share.Component == minimums.primary {
				continue
			}
			if _, independent := own[share.Component]; !independent && split.Ceiling(share.Component) == 0 {
				continue
			}
			if share.Replicas > ceiling {
				return fmt.Errorf("%s share %d on cluster %q exceeds its ceiling of %d replicas from split.maxPerCluster", share.Component, share.Replicas, name, ceiling)
			}
		}
	}
	return nil
}

func capacityEvidence(sample capacity.Sample) (*v1beta1.PlacementCapacitySample, error) {
	out := &v1beta1.PlacementCapacitySample{DemandFingerprint: sample.DemandFingerprint, DemandContract: sample.DemandContract.DeepCopy(), Replicas: sample.Weight}
	for _, pool := range sample.Pools {
		out.Pools = append(out.Pools, v1beta1.PlacementCapacityPool{
			ResourceName: pool.ResourceName, ResourceFlavor: pool.ResourceFlavor,
			Demand: pool.Demand, Allocatable: pool.Allocatable, ObservedAt: pool.ObservedAt,
			ReportUID: pool.ReportUID, ReportResourceVersion: pool.ReportResourceVersion, Attribution: *pool.Attribution.DeepCopy(),
		})
	}
	return plan.CanonicalCapacity(out)
}
