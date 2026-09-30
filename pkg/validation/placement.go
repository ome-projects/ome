package validation

import (
	"fmt"

	"k8s.io/apimachinery/pkg/labels"

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
	if p.Policy == "" || p.Policy == v1beta1.PlacementPolicyLegacy {
		if p.ClusterAffinity != nil || p.MaxSurge != nil || p.ReplacementTimeout != nil || p.Mode == v1beta1.PlacementModeSplitByCapacity {
			return fmt.Errorf("clusterAffinity, maxSurge, replacementTimeout, and SplitByCapacity require spec.placement.policy: ClusterAffinity")
		}
		return validateLegacyPlacement(spec)
	}
	if !p.UsesClusterAffinity() {
		return fmt.Errorf("spec.placement.policy must be Legacy or ClusterAffinity")
	}
	split := p.Mode == v1beta1.PlacementModeSplit || p.Mode == v1beta1.PlacementModeSplitByCapacity
	if !split && p.Mode != v1beta1.PlacementModeSingle && p.Mode != v1beta1.PlacementModeAll {
		return fmt.Errorf("spec.placement.mode must explicitly select Single, All, Split, or SplitByCapacity")
	}
	if p.HasLegacyFields() {
		return fmt.Errorf("spec.placement requirements, clusterSelector, and capacityFactors are unsupported; migrate selectors to clusterAffinity and traffic factors to spec.routing.capacityFactors")
	}
	if _, err := affinity.Compile(p.ClusterAffinity, p.Mode == v1beta1.PlacementModeSplit); err != nil {
		return fmt.Errorf("spec.placement.%w", err)
	}
	if p.MaxSurge != nil && *p.MaxSurge < 0 {
		return fmt.Errorf("spec.placement.maxSurge must be nonnegative whole replicas")
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
	if s.HasLegacyFields() {
		return fmt.Errorf("spec.placement.split spread and minReplicasPerCluster are unsupported; mode and affinity weights determine exact shares")
	}
	if s.Replicas != nil && *s.Replicas < 1 {
		return fmt.Errorf("spec.placement.split.replicas must be positive when specified")
	}
	if s.MaxReplicasPerCluster < 0 {
		return fmt.Errorf("spec.placement.split.maxReplicasPerCluster must be nonnegative")
	}
	return nil
}

// ValidatePlacementIntent keeps the selected policy authoritative. Legacy
// annotations are accepted only outside the explicit ClusterAffinity contract.
func ValidatePlacementIntent(isvc *v1beta1.InferenceService) error {
	if isvc == nil {
		return nil
	}
	if !isvc.Spec.Placement.UsesClusterAffinity() {
		if isvc.Status.Placement != nil && isvc.Status.Placement.Plan != nil {
			return fmt.Errorf("an accepted placement plan requires spec.placement.policy: ClusterAffinity")
		}
		return ValidatePlacement(&isvc.Spec)
	}
	for _, key := range []string{constants.AcceleratorRequirements, constants.ClusterSelector} {
		if _, exists := isvc.Annotations[key]; exists {
			return fmt.Errorf("annotation %s is unsupported; declare spec.placement.mode and migrate restrictions to clusterAffinity", key)
		}
	}
	return ValidatePlacement(&isvc.Spec)
}

func validateLegacyPlacement(spec *v1beta1.InferenceServiceSpec) error {
	p := spec.Placement
	if p == nil {
		return nil
	}
	for _, sel := range []struct{ field, value string }{
		{"requirements", p.Requirements},
		{"clusterSelector", p.ClusterSelector},
	} {
		if sel.value == "" {
			continue
		}
		if _, err := labels.Parse(sel.value); err != nil {
			return fmt.Errorf("spec.placement.%s is not a valid label selector %q: %w", sel.field, sel.value, err)
		}
	}
	s := p.Split
	if s == nil {
		return nil
	}
	if s.Replicas != nil && *s.Replicas < 1 {
		return fmt.Errorf("spec.placement.split.replicas must be >= 1 when set, got %d", *s.Replicas)
	}
	if s.MaxReplicasPerCluster < 0 {
		return fmt.Errorf("spec.placement.split.maxReplicasPerCluster must be >= 0, got %d", s.MaxReplicasPerCluster)
	}
	if s.MinReplicasPerCluster < 0 {
		return fmt.Errorf("spec.placement.split.minReplicasPerCluster must be >= 0, got %d", s.MinReplicasPerCluster)
	}
	if s.MaxReplicasPerCluster > 0 && s.MinReplicasPerCluster > s.MaxReplicasPerCluster {
		return fmt.Errorf("spec.placement.split.minReplicasPerCluster (%d) must not exceed maxReplicasPerCluster (%d)",
			s.MinReplicasPerCluster, s.MaxReplicasPerCluster)
	}
	return nil
}

// ValidatePlacementPolicyUpdate prevents a policy downgrade from handing planned
// workloads to the admission-driven controller, including without a stored plan.
func ValidatePlacementPolicyUpdate(oldSpec, newSpec *v1beta1.InferenceServiceSpec) error {
	if oldSpec.Placement.UsesClusterAffinity() && !newSpec.Placement.UsesClusterAffinity() {
		return fmt.Errorf("spec.placement.policy ClusterAffinity cannot be removed; drain and recreate the service to use Legacy")
	}
	return nil
}
