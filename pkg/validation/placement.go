package validation

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/placement/affinity"
)

// ValidatePlacement checks explicit placement syntax independently of member
// health or runtime resolution. An absent block leaves local services unchanged.
func ValidatePlacement(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil || spec.Placement == nil {
		return nil
	}
	p := spec.Placement
	if !p.UsesClusterAffinity() {
		return fmt.Errorf("spec.placement.policy must explicitly select ClusterAffinity")
	}
	split := p.Mode == v1beta1.PlacementModeSplit || p.Mode == v1beta1.PlacementModeSplitByCapacity
	if !split && p.Mode != v1beta1.PlacementModeSingle && p.Mode != v1beta1.PlacementModeAll {
		return fmt.Errorf("spec.placement.mode must explicitly select Single, All, Split, or SplitByCapacity")
	}
	if _, err := affinity.Compile(p.ClusterAffinity, p.Mode == v1beta1.PlacementModeSplit); err != nil {
		return fmt.Errorf("spec.placement.%w", err)
	}
	if p.ReplacementTimeout != nil && (p.Mode != v1beta1.PlacementModeSingle || p.ReplacementTimeout.Duration <= 0) {
		return fmt.Errorf("spec.placement.replacementTimeout must be positive and is permitted only for Single")
	}
	if p.Split == nil {
		return nil
	}
	if !split {
		return fmt.Errorf("spec.placement.split is permitted only for Split and SplitByCapacity")
	}
	s := p.Split
	if s.Replicas != nil && *s.Replicas < 1 {
		return fmt.Errorf("spec.placement.split.replicas must be positive when specified")
	}
	if s.MaxReplicasPerCluster < 0 {
		return fmt.Errorf("spec.placement.split.maxReplicasPerCluster must be nonnegative")
	}
	return nil
}

// ValidatePlacementIntent rejects obsolete selector annotations and requires
// an accepted allocation to retain its explicit placement policy.
func ValidatePlacementIntent(isvc *v1beta1.InferenceService) error {
	if isvc == nil {
		return nil
	}
	if !isvc.Spec.Placement.UsesClusterAffinity() {
		if isvc.Status.Placement != nil && isvc.Status.Placement.Plan != nil {
			return fmt.Errorf("an accepted placement plan requires spec.placement.policy: ClusterAffinity")
		}
	}
	for _, key := range []string{constants.AcceleratorRequirements, constants.ClusterSelector} {
		if _, exists := isvc.Annotations[key]; exists {
			return fmt.Errorf("annotation %s is unsupported; declare spec.placement.mode and migrate restrictions to clusterAffinity", key)
		}
	}
	return ValidatePlacement(&isvc.Spec)
}

// ValidatePlacementPolicyUpdate prevents removing source ownership while
// member workloads can still hold its allocation authority.
func ValidatePlacementPolicyUpdate(oldSpec, newSpec *v1beta1.InferenceServiceSpec) error {
	if oldSpec.Placement.UsesClusterAffinity() && !newSpec.Placement.UsesClusterAffinity() {
		return fmt.Errorf("spec.placement.policy ClusterAffinity cannot be removed; drain and recreate the service to change placement ownership")
	}
	return nil
}
