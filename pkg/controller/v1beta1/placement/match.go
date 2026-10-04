package placement

import (
	"sort"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/affinity"
	"sigs.k8s.io/ome/pkg/validation"
)

// MatchReason explains why placement currently has no eligible candidate.
type MatchReason string

const (
	MatchReasonNoRequirements    MatchReason = "NoPlacementIntent"
	MatchReasonMalformedSelector MatchReason = "InvalidPlacementIntent"
	MatchReasonNoReadyClusters   MatchReason = "NoReadyClusters"
	MatchReasonNoMatch           MatchReason = "NoMatch"
)

// placementSelector is shared by actuation, standing observations, and cluster
// event filtering. Health and admission capability never alter its matched set.
func placementSelector(isvc *v1beta1.InferenceService) (*affinity.Selector, error) {
	if err := validation.ValidatePlacementIntent(isvc); err != nil {
		return nil, err
	}
	if isvc == nil || isvc.Spec.Placement == nil {
		return nil, nil
	}
	return affinity.Compile(isvc.Spec.Placement.ClusterAffinity, isvc.Spec.Placement.Mode == v1beta1.PlacementModeSplit)
}

// MatchCandidates returns sorted Ready members of the matched set. Replica
// apportionment uses all matching registrations, independently of this gate.
func MatchCandidates(isvc *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) ([]string, MatchReason, error) {
	selector, err := placementSelector(isvc)
	if err != nil {
		return nil, MatchReasonMalformedSelector, err
	}
	if selector == nil {
		return nil, MatchReasonNoRequirements, nil
	}
	matched := 0
	var out []string
	for i := range clusters {
		cluster := &clusters[i]
		if !cluster.DeletionTimestamp.IsZero() {
			continue
		}
		if _, matches := selector.Match(cluster); !matches {
			continue
		}
		matched++
		if clusterReady(cluster) {
			out = append(out, cluster.Name)
		}
	}
	sort.Strings(out)
	switch {
	case len(out) > 0:
		return out, "", nil
	case matched == 0:
		return nil, MatchReasonNoMatch, nil
	default:
		return nil, MatchReasonNoReadyClusters, nil
	}
}
