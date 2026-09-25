package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func wc(name string, ready bool, labels map[string]string) v1beta1.WorkloadCluster {
	st := metav1.ConditionFalse
	if ready {
		st = metav1.ConditionTrue
	}
	return v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: v1beta1.WorkloadClusterStatus{Conditions: []metav1.Condition{
			{Type: v1beta1.WorkloadClusterReady, Status: st, Reason: "x"},
		}},
	}
}

// isvcReq builds a source ISVC whose accelerator requirement / cluster selector
// annotations drive candidate matching.
func isvcReq(accel, selector string) *v1beta1.InferenceService {
	ann := map[string]string{}
	if accel != "" {
		ann[AcceleratorRequirementsAnnotation] = accel
	}
	if selector != "" {
		ann[ClusterSelectorAnnotation] = selector
	}
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", Annotations: ann}}
}

func TestMatchCandidates(t *testing.T) {
	clusters := []v1beta1.WorkloadCluster{
		wc("cluster-a", true, map[string]string{"gpu": "gb300", "provider": "prov-a"}),
		wc("cluster-b", true, map[string]string{"gpu": "gb300", "provider": "prov-b"}),
		wc("cluster-a-h100", true, map[string]string{"gpu": "h100", "provider": "prov-a"}),
		wc("cluster-a-down", false, map[string]string{"gpu": "gb300", "provider": "prov-a"}),
	}

	// accelerator requirement "gpu=gb300" -> the two Ready gb300 clusters, sorted.
	got, reason, err := MatchCandidates(isvcReq("gpu=gb300", ""), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-a", "cluster-b"}, got)
	assert.Equal(t, MatchReason(""), reason)

	// accelerator requirement AND-ed with cluster-selector.
	got, _, err = MatchCandidates(isvcReq("gpu=gb300", "provider=prov-a"), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-a"}, got)

	// cluster-selector alone is a sufficient requirement (counts as a requirement).
	got, _, err = MatchCandidates(isvcReq("", "gpu=gb300,provider=prov-b"), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-b"}, got)

	// metadata.name is available without a duplicate WorkloadCluster label and
	// remains AND-ed with the capability requirement.
	got, _, err = MatchCandidates(isvcReq(
		"gpu=gb300",
		"metadata.name in (cluster-a-h100,cluster-a-down,cluster-b)",
	), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-b"}, got)

	// Guard: an ISVC with NO requirement must NOT fan out to every
	// Ready cluster — it matches NOTHING with reason NoRequirements.
	got, reason, err = MatchCandidates(isvcReq("", ""), clusters)
	assert.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, MatchReasonNoRequirements, reason)

	// requirements declared but nothing satisfies them.
	got, reason, err = MatchCandidates(isvcReq("gpu=mi300x", ""), clusters)
	assert.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, MatchReasonNoMatch, reason)

	// requirements declared but no Ready clusters at all.
	got, reason, err = MatchCandidates(isvcReq("gpu=gb300", ""), []v1beta1.WorkloadCluster{
		wc("cluster-a-down", false, map[string]string{"gpu": "gb300"}),
	})
	assert.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, MatchReasonNoReadyClusters, reason)

	// malformed selector -> error + MalformedSelector reason.
	_, reason, err = MatchCandidates(isvcReq("!!!", ""), clusters)
	assert.Error(t, err)
	assert.Equal(t, MatchReasonMalformedSelector, reason)
}

func TestMatchCandidatesMetadataNameIsAuthoritative(t *testing.T) {
	clusters := []v1beta1.WorkloadCluster{
		wc("cluster-real", true, map[string]string{"metadata.name": "cluster-spoofed"}),
	}

	got, _, err := MatchCandidates(isvcReq("", "metadata.name=cluster-real"), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-real"}, got)
	assert.Equal(t, "cluster-spoofed", clusters[0].Labels["metadata.name"],
		"selector evaluation must not mutate the WorkloadCluster labels")

	got, reason, err := MatchCandidates(isvcReq("", "metadata.name=cluster-spoofed"), clusters)
	assert.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, MatchReasonNoMatch, reason)
}

// isvcPlacement builds a source ISVC whose candidate matching is driven by the
// structured spec.placement field rather than annotations.
func isvcPlacement(p *v1beta1.PlacementSpec) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod"},
		Spec:       v1beta1.InferenceServiceSpec{Placement: p},
	}
}

// TestMatchCandidates_PlacementSpec covers the structured-field path and its
// precedence over the legacy annotations.
func TestMatchCandidates_PlacementSpec(t *testing.T) {
	clusters := []v1beta1.WorkloadCluster{
		wc("cluster-a", true, map[string]string{"gpu": "gb300", "provider": "prov-a"}),
		wc("cluster-b", true, map[string]string{"gpu": "gb300", "provider": "prov-b"}),
	}

	// spec.placement.requirements drives matching.
	got, reason, err := MatchCandidates(isvcPlacement(&v1beta1.PlacementSpec{Requirements: "gpu=gb300"}), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-a", "cluster-b"}, got)
	assert.Equal(t, MatchReason(""), reason)

	// requirements AND clusterSelector, both from the struct.
	got, _, err = MatchCandidates(isvcPlacement(&v1beta1.PlacementSpec{
		Requirements: "gpu=gb300", ClusterSelector: "provider=prov-b",
	}), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-b"}, got)

	// clusterSelector alone in the struct is a sufficient requirement.
	got, _, err = MatchCandidates(isvcPlacement(&v1beta1.PlacementSpec{ClusterSelector: "provider=prov-a"}), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-a"}, got)

	// A structured clusterSelector can select directly by immutable object name.
	got, _, err = MatchCandidates(isvcPlacement(&v1beta1.PlacementSpec{
		ClusterSelector: "metadata.name=cluster-b",
	}), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-b"}, got)

	// empty spec.placement (mode only) declares no requirement -> no fan-out.
	_, reason, err = MatchCandidates(isvcPlacement(&v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSingle}), clusters)
	assert.NoError(t, err)
	assert.Equal(t, MatchReasonNoRequirements, reason)

	// malformed selector in the struct surfaces as MalformedSelector.
	_, reason, err = MatchCandidates(isvcPlacement(&v1beta1.PlacementSpec{Requirements: "!!!"}), clusters)
	assert.Error(t, err)
	assert.Equal(t, MatchReasonMalformedSelector, reason)
}

// TestPlacementInputs_StructWinsOverAnnotations verifies the struct is used
// WHOLESALE when present: annotations are ignored entirely, not merged.
func TestPlacementInputs_StructWinsOverAnnotations(t *testing.T) {
	isvc := isvcPlacement(&v1beta1.PlacementSpec{Requirements: "gpu=gb300"})
	isvc.Annotations = map[string]string{
		AcceleratorRequirementsAnnotation: "gpu=h100",
		ClusterSelectorAnnotation:         "provider=prov-a",
	}
	req, sel := placementInputs(isvc)
	assert.Equal(t, "gpu=gb300", req, "struct requirements win over annotation")
	assert.Equal(t, "", sel, "struct clusterSelector (empty) wins; annotation ignored")

	// nil placement falls back to annotations.
	isvc.Spec.Placement = nil
	req, sel = placementInputs(isvc)
	assert.Equal(t, "gpu=h100", req)
	assert.Equal(t, "provider=prov-a", sel)
}

// TestDeclaresPlacementRequirement_StructAndAnnotations covers the field-index
// eligibility predicate over both input sources.
func TestDeclaresPlacementRequirement_StructAndAnnotations(t *testing.T) {
	assert.True(t, declaresPlacementRequirement(isvcPlacement(&v1beta1.PlacementSpec{Requirements: "gpu=gb300"})))
	assert.True(t, declaresPlacementRequirement(isvcPlacement(&v1beta1.PlacementSpec{ClusterSelector: "provider=prov-a"})))
	assert.False(t, declaresPlacementRequirement(isvcPlacement(&v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeAll})))
	assert.False(t, declaresPlacementRequirement(isvcPlacement(nil)))
	assert.True(t, declaresPlacementRequirement(isvcReq("gpu=gb300", "")))
	assert.True(t, IsPlacementEligible(isvcReq("gpu=gb300", "")))
}
