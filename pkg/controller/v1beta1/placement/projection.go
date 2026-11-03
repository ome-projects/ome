package placement

import (
	"fmt"
	"maps"
	"math"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

// ProjectSplitReplicas copies a source with one member's final desired replica
// bounds, using the controller's planner. It ignores source status and does not
// derive a complete member manifest. Supply the full registration inventory.
func ProjectSplitReplicas(source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, member string) (*v1beta1.InferenceService, error) {
	if source == nil || source.Spec.Placement == nil || source.Spec.Placement.Policy != v1beta1.PlacementPolicyClusterAffinity || source.Spec.Placement.Mode != v1beta1.PlacementModeSplit {
		return nil, fmt.Errorf("projection supports only explicit ClusterAffinity placement with mode Split")
	}
	if source.Spec.Engine == nil {
		return nil, fmt.Errorf("projection requires a declared engine component")
	}
	extensions := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent:  engineExtension(source),
		v1beta1.DecoderComponent: decoderExtension(source),
	}
	if source.Spec.Router != nil {
		extensions[v1beta1.RouterComponent] = &source.Spec.Router.ComponentExtensionSpec
	}
	for _, component := range slices.Sorted(maps.Keys(extensions)) {
		extension := extensions[component]
		if extension == nil {
			continue
		}
		if extension.MinReplicas != nil && int64(*extension.MinReplicas) > math.MaxInt32 {
			return nil, fmt.Errorf("%s.minReplicas must fit a nonnegative int32 replica count", component)
		}
		var maximum *int
		if extension.MaxReplicas != 0 {
			maximum = &extension.MaxReplicas
		}
		if err := validation.ValidateReplicaBounds(extension.MinReplicas, maximum); err != nil {
			return nil, fmt.Errorf("invalid source %s replica bounds: %w", component, err)
		}
	}
	registered := false
	for _, cluster := range clusters {
		registered = registered || cluster.Name == member
	}
	if member == "" || !registered {
		return nil, fmt.Errorf("member %q has no WorkloadCluster registration in the inputs", member)
	}
	snapshot := source.DeepCopy()
	snapshot.Status = v1beta1.InferenceServiceStatus{}
	proposal, err := desiredSplitPlanFromSnapshot(snapshot, clusters, nil, false)
	if err != nil {
		return nil, err
	}
	if proposal.UnassignedReplicas != 0 {
		return nil, fmt.Errorf("%d replicas are unassigned: supply the complete matching WorkloadCluster inventory", proposal.UnassignedReplicas)
	}
	if proposal.Assignments[member].DesiredReplicas == 0 {
		return nil, nil
	}
	// Final desired bounds use every member's desired share as its current
	// share. Retained replicas and admission do not enter this snapshot.
	candidates := make([]v1beta1.CandidatePlacement, 0, len(proposal.Assignments))
	var target v1beta1.CandidatePlacement
	for _, name := range slices.Sorted(maps.Keys(proposal.Assignments)) {
		assignment := proposal.Assignments[name]
		assignment.CurrentReplicas = assignment.DesiredReplicas
		candidate := v1beta1.CandidatePlacement{Cluster: name, Allocation: &assignment}
		candidates = append(candidates, candidate)
		if name == member {
			target = candidate
		}
	}
	bounds, err := splitPlannedBounds(snapshot, candidates, target, nil)
	if err != nil {
		return nil, err
	}
	setPlannedReplicas(snapshot, bounds)
	return snapshot, nil
}
