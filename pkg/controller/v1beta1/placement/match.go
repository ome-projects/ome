package placement

import (
	"sort"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// MatchReason explains why MatchCandidates produced an empty candidate set, so
// the caller can surface an actionable status instead of an indistinguishable
// nil. It is "" when at least one candidate matched.
type MatchReason string

const (
	// MatchReasonNoRequirements: the ISVC declared no accelerator/capability
	// requirements at all. Candidates are the clusters whose
	// labels SATISFY the ISVC's requirements; with no requirements there is
	// nothing to satisfy, so we deliberately match NOTHING rather than fan out
	// to every Ready cluster (the unsafe default this guards against).
	MatchReasonNoRequirements MatchReason = "NoRequirements"
	// MatchReasonMalformedSelector: a requirement selector failed to parse.
	MatchReasonMalformedSelector MatchReason = "MalformedSelector"
	// MatchReasonNoReadyClusters: requirements were declared but no Ready
	// WorkloadCluster exists at all.
	MatchReasonNoReadyClusters MatchReason = "NoReadyClusters"
	// MatchReasonNoMatch: Ready clusters exist but none satisfy the requirements.
	MatchReasonNoMatch MatchReason = "NoMatch"
)

// requirementSelector builds the AND-combined label selector an ISVC's candidate
// clusters must satisfy: the accelerator/capability requirements
// (AcceleratorRequirementsAnnotation) intersected with the optional
// operator-imposed cluster-selector (ClusterSelectorAnnotation). hasReq reports
// whether the ISVC expressed ANY requirement; an ISVC with no requirement is NOT
// fanned out fleet-wide (see MatchReasonNoRequirements).
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

// placementInputs returns the (requirements, clusterSelector) label-selector
// strings for an ISVC, preferring the structured spec.placement over the legacy
// ome.io/accelerator-requirements and ome.io/cluster-selector annotations. The
// struct wins WHOLESALE when present (no per-field merge with the annotations),
// so an ISVC that sets spec.placement is described entirely by it. When
// spec.placement is nil the annotations remain authoritative.
func placementInputs(isvc *v1beta1.InferenceService) (requirements, clusterSelector string) {
	if p := isvc.Spec.Placement; p != nil {
		return p.Requirements, p.ClusterSelector
	}
	return isvc.Annotations[AcceleratorRequirementsAnnotation], isvc.Annotations[ClusterSelectorAnnotation]
}

// workloadClusterSelectorSet exposes immutable object identity alongside the
// cluster's labels. The real object name is authoritative when a label uses the
// same key, and the copy keeps selector evaluation from mutating informer data.
func workloadClusterSelectorSet(cluster *v1beta1.WorkloadCluster) labels.Set {
	set := make(labels.Set, len(cluster.Labels)+1)
	for key, value := range cluster.Labels {
		set[key] = value
	}
	set[metav1.ObjectNameField] = cluster.Name
	return set
}

// MatchCandidates returns the names (sorted) of Ready WorkloadClusters whose
// capability labels and metadata.name satisfy the ISVC's selectors. The
// requirements are the AND of the ISVC's accelerator-requirements annotation
// and the optional cluster-selector annotation. An ISVC that declares NO
// requirement matches NO cluster (it is not fanned out fleet-wide). When the
// result is empty, the returned MatchReason explains why; err is non-nil only
// for a malformed selector.
func MatchCandidates(isvc *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) ([]string, MatchReason, error) {
	sel, hasReq, err := requirementSelector(isvc)
	if err != nil {
		return nil, MatchReasonMalformedSelector, err
	}
	if !hasReq {
		return nil, MatchReasonNoRequirements, nil
	}

	var ready int
	var out []string
	for i := range clusters {
		c := &clusters[i]
		if !apimeta.IsStatusConditionTrue(c.Status.Conditions, v1beta1.WorkloadClusterReady) {
			continue
		}
		ready++
		if sel.Matches(workloadClusterSelectorSet(c)) {
			out = append(out, c.Name)
		}
	}
	sort.Strings(out)
	switch {
	case len(out) > 0:
		return out, "", nil
	case ready == 0:
		return nil, MatchReasonNoReadyClusters, nil
	default:
		return nil, MatchReasonNoMatch, nil
	}
}
