package placement

import (
	"fmt"
	"maps"
	"math"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

// ProjectSplitReplicas returns a copy of source with the replica bounds of the
// cluster named member. Placement mode Split divides the replicas of source
// across several clusters. The bounds are the minimum and maximum replicas the
// named cluster runs once the plan is complete. ProjectSplitReplicas returns
// nil and no error when the plan gives that cluster no replicas.
//
// It plans without a running controller, from source and clusters alone. It
// reuses the controller's planning code, so its answers match the controller's.
//
// clusters must list every WorkloadCluster. A WorkloadCluster without
// metadata.uid, as a manifest file holds it, is accepted. The function ignores
// the status of source and changes neither source nor clusters. The copy has
// an empty status and differs from source only in the engine, decoder and
// router replica bounds. It is not the full InferenceService that OME writes
// to the cluster.
//
// It returns an error and no copy when:
//   - source does not use ClusterAffinity placement in mode Split.
//   - source declares no engine.
//   - the replica bounds of the engine, decoder or router are invalid.
//   - clusters holds no WorkloadCluster named member.
//   - the controller's planning code refuses the plan.
//   - some replicas match no WorkloadCluster in clusters. This error is an
//     *UnassignedReplicasError, which errors.As tells apart from the others.
func ProjectSplitReplicas(source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, member string) (*v1beta1.InferenceService, error) {
	if err := checkProjectionSource(source); err != nil {
		return nil, err
	}
	if member == "" || !slices.ContainsFunc(clusters, func(cluster v1beta1.WorkloadCluster) bool { return cluster.Name == member }) {
		return nil, fmt.Errorf("member %q has no WorkloadCluster registration in the inputs", member)
	}
	snapshot := source.DeepCopy()
	snapshot.Status = v1beta1.InferenceServiceStatus{}
	proposal, err := desiredSplitPlanFromSnapshot(snapshot, clusters, nil, false)
	if err != nil {
		return nil, err
	}
	if proposal.UnassignedReplicas != 0 {
		return nil, &UnassignedReplicasError{Replicas: proposal.UnassignedReplicas}
	}
	if proposal.Assignments[member].DesiredReplicas == 0 {
		return nil, nil
	}
	candidates := settledPlanCandidates(proposal.Assignments)
	target := slices.IndexFunc(candidates, func(candidate v1beta1.CandidatePlacement) bool { return candidate.Cluster == member })
	if target < 0 {
		return nil, fmt.Errorf("member %q is missing from the planned member list", member)
	}
	bounds, err := splitPlannedBounds(snapshot, candidates, candidates[target], nil)
	if err != nil {
		return nil, err
	}
	setPlannedReplicas(snapshot, bounds)
	return snapshot, nil
}

// UnassignedReplicasError reports replicas the planner gave no member because
// no registration in the inputs matches the source. Registering a matching
// cluster clears it, so the source itself may be sound.
type UnassignedReplicasError struct {
	Replicas int32
}

func (e *UnassignedReplicasError) Error() string {
	return fmt.Sprintf("%d replicas are unassigned: supply the complete matching WorkloadCluster inventory", e.Replicas)
}

// checkProjectionSource returns an error for a source that ProjectSplitReplicas
// cannot plan. The source must use ClusterAffinity placement in mode Split and
// declare an engine. Each declared component, in name order, must pass
// checkProjectedBounds, so the first invalid one in that order is reported.
func checkProjectionSource(source *v1beta1.InferenceService) error {
	if source == nil || source.Spec.Placement == nil || source.Spec.Placement.Policy != v1beta1.PlacementPolicyClusterAffinity || source.Spec.Placement.Mode != v1beta1.PlacementModeSplit {
		return fmt.Errorf("projection supports only explicit ClusterAffinity placement with mode Split")
	}
	if source.Spec.Engine == nil {
		return fmt.Errorf("projection requires a declared engine component")
	}
	components := declaredComponents(source)
	slices.Sort(components)
	for _, component := range components {
		if err := checkProjectedBounds(component, v1beta1.SpecOf(&source.Spec, component).Extension); err != nil {
			return err
		}
	}
	return nil
}

// checkProjectedBounds returns an error when a declared component's
// minReplicas does not fit in an int32, or when
// validation.ValidateReplicaBounds rejects its minReplicas and maxReplicas.
func checkProjectedBounds(component v1beta1.ComponentType, extension *v1beta1.ComponentExtensionSpec) error {
	if extension.MinReplicas != nil && int64(*extension.MinReplicas) > math.MaxInt32 {
		return fmt.Errorf("%s.minReplicas must fit a nonnegative int32 replica count", component)
	}
	var maximum *int
	if extension.MaxReplicas != 0 {
		maximum = &extension.MaxReplicas
	}
	if err := validation.ValidateReplicaBounds(extension.MinReplicas, maximum); err != nil {
		return fmt.Errorf("invalid source %s replica bounds: %w", component, err)
	}
	return nil
}

// settledPlanCandidates lists the plan's member clusters sorted by name, the
// order the controller stores them in. Each member's current replicas equal its
// desired replicas, as they do once the plan is complete.
func settledPlanCandidates(assignments map[string]v1beta1.CandidateAllocationStatus) []v1beta1.CandidatePlacement {
	planned := make([]v1beta1.CandidatePlacement, 0, len(assignments))
	for _, name := range slices.Sorted(maps.Keys(assignments)) {
		assignment := assignments[name]
		planned = append(planned, v1beta1.CandidatePlacement{Cluster: name, Allocation: &assignment})
	}
	return settledStatusCandidates(planned)
}
