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

// desiredSplitPlanFromSnapshot is the split plan for one pass: the stored
// assignments, every matched registration with its weight, the apportioned
// shares and the digest of the intent they came from. Actuation requires
// registration UIDs to verify the cluster it writes to; offline snapshots can
// omit server identities because projection writes none.
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
	desiredReplicas := splitDesiredReplicas(source)
	if desiredReplicas <= 0 {
		return plan.Proposal{}, replicaCountUnresolved(source)
	}
	proposal, err := storedSplitAssignments(source)
	if err != nil {
		return plan.Proposal{}, err
	}
	matcher := splitMatcher{source: source, selector: selector, selectors: selectors, samples: samples, requireUID: requireRegistrationUID}
	weights, err := matcher.weights(clusters, proposal.Assignments)
	if err != nil {
		return plan.Proposal{}, err
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return plan.Proposal{}, err
	}
	shares, err := allocation.Apportion(desiredReplicas, weights, minimums.ceilings(source)[minimums.primary])
	if err != nil {
		return plan.Proposal{}, err
	}
	if err := checkDesiredSplitFloors(source, minimums, shares.Targets, proposal.Assignments); err != nil {
		return plan.Proposal{}, err
	}
	proposal.UnassignedReplicas = shares.Unassigned
	for name, replicas := range shares.Targets {
		assignment := proposal.Assignments[name]
		assignment.DesiredReplicas = replicas
		proposal.Assignments[name] = assignment
	}
	proposal.InputDigest, err = splitIntentDigest(source, proposal.Assignments, weights, shares.Unassigned)
	if err != nil {
		return plan.Proposal{}, err
	}
	return proposal, nil
}

// storedSplitAssignments starts the proposal from the plan the source holds:
// its original unassigned count, its pause, and each candidate's allocation
// with the fields this pass recomputes cleared. A source without a plan
// starts empty.
func storedSplitAssignments(source *v1beta1.InferenceService) (plan.Proposal, error) {
	proposal := plan.Proposal{Assignments: map[string]v1beta1.CandidateAllocationStatus{}}
	existing := source.Status.Placement
	if existing == nil || existing.Plan == nil {
		return proposal, nil
	}
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
	return proposal, nil
}

// splitMatcher matches registrations for one split plan: the primary
// component's selector, each component's selector, and the capacity samples
// the capacity mode weighs by.
type splitMatcher struct {
	source     *v1beta1.InferenceService
	selector   *affinity.Selector
	selectors  map[v1beta1.ComponentType]*affinity.Selector
	samples    map[string]capacity.Sample
	requireUID bool
}

// weights records every live registration the selector matches on its
// assignment (identity, terms, component matches and weight) and returns the
// weights the apportionment divides by, keyed by cluster name.
func (m splitMatcher) weights(clusters []v1beta1.WorkloadCluster, assignments map[string]v1beta1.CandidateAllocationStatus) (map[string]int64, error) {
	weights := map[string]int64{}
	for i := range clusters {
		cluster := &clusters[i]
		if !cluster.DeletionTimestamp.IsZero() {
			continue
		}
		match, matches := m.selector.Match(cluster)
		if !matches {
			continue
		}
		if cluster.Name == "" || (m.requireUID && cluster.UID == "") {
			return nil, fmt.Errorf("matched registration requires a name and UID")
		}
		if _, duplicate := weights[cluster.Name]; duplicate {
			return nil, fmt.Errorf("duplicate matched registration %q", cluster.Name)
		}
		assignment := assignments[cluster.Name]
		if assignment.ClusterUID != "" && assignment.ClusterUID != cluster.UID {
			return nil, fmt.Errorf("cluster %q incarnation changed; its standing allocation is unverified", cluster.Name)
		}
		assignment.ClusterUID, assignment.MatchingTerms = cluster.UID, match.TermIndexes
		assignment.ComponentMatches = componentMatchesOn(m.source, m.selectors, cluster)
		weight, err := m.weightOn(cluster, match, &assignment)
		if err != nil {
			return nil, err
		}
		weights[cluster.Name] = weight
		assignments[cluster.Name] = assignment
	}
	return weights, nil
}

// weightOn is the cluster's weight in the apportionment, recorded on its
// assignment: in static Split the matched terms' weight with its bounds, in
// SplitByCapacity the verified capacity sample's replicas with the sample.
func (m splitMatcher) weightOn(cluster *v1beta1.WorkloadCluster, match affinity.Match, assignment *v1beta1.CandidateAllocationStatus) (int64, error) {
	if placementMode(m.source) == v1beta1.PlacementModeSplit {
		assignment.Weight, assignment.MinWeight = ptr.To(match.Weight), ptr.To(match.Weight)
		if affinity.UsesRatios(m.source.Spec.Placement.ClusterAffinity) {
			assignment.MaxWeight = ptr.To(match.MaxWeight)
		}
		return match.Weight, nil
	}
	sample, exists := m.samples[cluster.Name]
	if !exists || sample.ClusterUID != cluster.UID {
		return 0, fmt.Errorf("CapacityUnknown: verified sample is missing for cluster %q", cluster.Name)
	}
	recorded, err := recordedCapacitySample(sample)
	if err == nil {
		err = protocol.ValidateDemandComponents(m.source, recorded.DemandContract)
	}
	if err != nil {
		return 0, fmt.Errorf("CapacityUnknown: cluster %q: %w", cluster.Name, err)
	}
	assignment.Capacity = recorded
	return sample.Weight, nil
}

// splitIntentDigest hashes what the plan asks for: the spec, the labels, the
// intent annotations, each matched assignment without the movement and
// report-time fields that change between passes, and the unassigned count.
// MinWeight repeats Weight, so it stays out and stored plans keep their digest.
func splitIntentDigest(source *v1beta1.InferenceService, assignments map[string]v1beta1.CandidateAllocationStatus, weights map[string]int64, unassigned int32) (string, error) {
	intent := make(map[string]v1beta1.CandidateAllocationStatus, len(weights))
	for name := range weights {
		assignment := plan.WithoutMinWeight(assignments[name])
		assignment.OriginalReplicas, assignment.CurrentReplicas, assignment.DrainRequested = 0, 0, false
		if assignment.Capacity != nil {
			for i := range assignment.Capacity.Pools {
				assignment.Capacity.Pools[i].ObservedAt = metav1.Time{}
				assignment.Capacity.Pools[i].ReportResourceVersion = ""
			}
		}
		intent[name] = assignment
	}
	encoded, err := json.Marshal(struct {
		Spec        v1beta1.InferenceServiceSpec
		Annotations map[string]string
		Labels      map[string]string
		Assignments map[string]v1beta1.CandidateAllocationStatus
		Unassigned  int32
	}{Spec: source.Spec, Annotations: planIntentAnnotations(source.Annotations), Labels: source.Labels, Assignments: intent, Unassigned: unassigned})
	if err != nil {
		return "", fmt.Errorf("encode placement intent: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// componentMatchesOn records the weight of each component other than the
// primary one on a cluster the primary component matched, when the terms set
// weights in static Split: the decoder's, and the router's when routers are
// divided. Every other plan records none, so it apportions by primary targets.
func componentMatchesOn(source *v1beta1.InferenceService, selectors map[v1beta1.ComponentType]*affinity.Selector, cluster *v1beta1.WorkloadCluster) []v1beta1.ComponentAffinityMatch {
	if placementMode(source) != v1beta1.PlacementModeSplit || !affinity.UsesRatios(source.Spec.Placement.ClusterAffinity) {
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
// within a cap scaled from the primary's, so only a component with a cap of
// its own in split.maxPerCluster is checked.
func checkDesiredSplitFloors(source *v1beta1.InferenceService, minimums splitMinimums, targets map[string]int32, assignments map[string]v1beta1.CandidateAllocationStatus) error {
	weights := splitWeightsOf(source, assignments)
	floors, err := apportionSplitFloors(minimums, targets, weights)
	if err != nil {
		return err
	}
	split := splitSpecOf(source)
	ceilings := minimums.ceilings(source)
	for _, name := range slices.Sorted(maps.Keys(floors)) {
		for _, share := range floors[name] {
			ceiling := ceilings[share.Component]
			if ceiling <= 0 || share.Component == minimums.primary || split.Ceiling(share.Component) == 0 {
				continue
			}
			if share.Replicas > ceiling {
				return fmt.Errorf("%s share %d on cluster %q exceeds its ceiling of %d replicas from split.maxPerCluster", share.Component, share.Replicas, name, ceiling)
			}
		}
	}
	return nil
}

// recordedCapacitySample is the sample as the plan records it, in canonical
// form.
func recordedCapacitySample(sample capacity.Sample) (*v1beta1.PlacementCapacitySample, error) {
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
