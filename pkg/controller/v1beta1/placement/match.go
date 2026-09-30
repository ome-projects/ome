package placement

import (
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

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
func placementSelector(isvc *v1beta1.InferenceService) (*clusterMatcher, error) {
	if err := validation.ValidatePlacementIntent(isvc); err != nil {
		return nil, err
	}
	if isvc == nil {
		return nil, nil
	}
	if !isvc.Spec.Placement.UsesClusterAffinity() {
		selector, has, err := requirementSelector(isvc)
		if err != nil || !has {
			return nil, err
		}
		return &clusterMatcher{legacy: selector}, nil
	}
	selector, err := affinity.Compile(isvc.Spec.Placement.ClusterAffinity, isvc.Spec.Placement.Mode == v1beta1.PlacementModeSplit)
	return &clusterMatcher{affinity: selector}, err
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

// clusterMatcher keeps legacy label expressions separate from affinity fields;
// virtual metadata.name has different semantics from an actual label of that name.
type clusterMatcher struct {
	legacy   labels.Selector
	affinity *affinity.Selector
}

func (s *clusterMatcher) Match(cluster *v1beta1.WorkloadCluster) (affinity.Match, bool) {
	if s == nil {
		return affinity.Match{}, false
	}
	if s.legacy != nil {
		return affinity.Match{}, s.legacy.Matches(workloadClusterSelectorSet(cluster))
	}
	return s.affinity.Match(cluster)
}

func requirementSelector(isvc *v1beta1.InferenceService) (sel labels.Selector, hasReq bool, err error) {
	requirements, clusterSelector := placementInputs(isvc)
	sel = labels.Everything()
	for _, raw := range []string{requirements, clusterSelector} {
		if raw == "" {
			continue
		}
		parsed, perr := labels.Parse(raw)
		if perr != nil {
			return nil, false, perr
		}
		reqs, _ := parsed.Requirements()
		sel = sel.Add(reqs...)
		hasReq = true
	}
	return sel, hasReq, nil
}

func placementInputs(isvc *v1beta1.InferenceService) (requirements, clusterSelector string) {
	if p := isvc.Spec.Placement; p != nil {
		return p.Requirements, p.ClusterSelector
	}
	return isvc.Annotations[AcceleratorRequirementsAnnotation], isvc.Annotations[ClusterSelectorAnnotation]
}

func workloadClusterSelectorSet(cluster *v1beta1.WorkloadCluster) labels.Set {
	set := make(labels.Set, len(cluster.Labels)+1)
	for key, value := range cluster.Labels {
		set[key] = value
	}
	set[metav1.ObjectNameField] = cluster.Name
	return set
}
