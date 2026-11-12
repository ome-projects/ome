package placement

import (
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// requireMinWeightEqualsWeight checks that every assignment and component
// match carries MinWeight equal to Weight.
func requireMinWeightEqualsWeight(t *testing.T, assignments map[string]v1beta1.CandidateAllocationStatus) {
	t.Helper()
	for name, a := range assignments {
		if a.Weight == nil || a.MinWeight == nil || *a.MinWeight != *a.Weight {
			t.Fatalf("%s: weight %v, minWeight %v, want equal and set", name, a.Weight, a.MinWeight)
		}
		for _, m := range a.ComponentMatches {
			if m.Weight == nil || m.MinWeight == nil || *m.MinWeight != *m.Weight {
				t.Fatalf("%s %s: weight %v, minWeight %v, want equal and set", name, m.Component, m.Weight, m.MinWeight)
			}
		}
	}
}

func TestDesiredSplitPlanWritesMinWeightInBothForms(t *testing.T) {
	integer := componentSplitSource(4, 12, map[string]int32{"member-a": 2, "member-b": 1}, map[string]int32{"member-a": 1, "member-b": 3})
	ratios := ratioPlanSource(
		map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, 3), "member-b": ratio(1, 1)},
		map[string]*v1beta1.ReplicaRatio{"member-a": ratio(1, 2), "member-b": ratio(1, 1)},
	)
	for name, source := range map[string]*v1beta1.InferenceService{"integer": integer, "ratio": ratios} {
		proposal, err := desiredSplitPlan(source, twoMembers(), nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if a := proposal.Assignments["member-a"]; len(a.ComponentMatches) == 0 {
			t.Fatalf("%s: no component matches recorded", name)
		}
		requireMinWeightEqualsWeight(t, proposal.Assignments)
	}
}

// TestDesiredSplitPlanRecomputesStoredMinWeight reloads a stored plan whose
// MinWeight disagrees with the terms; the proposal carries the terms' weight.
func TestDesiredSplitPlanRecomputesStoredMinWeight(t *testing.T) {
	source := componentSplitSource(4, 12, map[string]int32{"member-a": 2, "member-b": 1}, map[string]int32{"member-a": 1, "member-b": 3})
	first, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{SourceUID: source.UID}}
	for _, name := range []string{"member-a", "member-b"} {
		stored := first.Assignments[name]
		stored.MinWeight = ptr.To(int64(99))
		for i := range stored.ComponentMatches {
			stored.ComponentMatches[i].MinWeight = ptr.To(int64(99))
		}
		source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: &stored})
	}
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	requireMinWeightEqualsWeight(t, proposal.Assignments)
}

// TestSplitInputDigestExcludesMinWeight checks the exact input digest of
// unweighted and weighted sources. Their identity must not change: a different
// digest re-plans every stored service on upgrade.
func TestSplitInputDigestExcludesMinWeight(t *testing.T) {
	engineRatios := ratioSplitSource(4, nil)
	engineRatios.Spec.Decoder = nil
	engineRatios.Spec.Engine.MaxReplicas = 12
	engineRatios.Spec.Placement.ClusterAffinity = weightedTermsOn(map[string]v1beta1.ClusterAffinityWeights{
		"member-a": {Engine: ratio(2, 3)}, "member-b": {Engine: ratio(1, 1)},
	})
	componentRatios := ratioSplitSource(4, ptr.To(12))
	componentRatios.Spec.Engine.MaxReplicas, componentRatios.Spec.Decoder.MaxReplicas = 12, 24
	componentRatios.Spec.Placement.ClusterAffinity = weightedTermsOn(map[string]v1beta1.ClusterAffinityWeights{
		"member-a": {Default: ratio(2, 1), Decoder: ratio(1, 2)}, "member-b": {Default: ratio(1, 1), Decoder: ratio(3, 1)},
	})
	for name, tt := range map[string]struct {
		source *v1beta1.InferenceService
		want   string
	}{
		"no weights":                 {srcISVCSplit("accelerator=gpu-a", 6), "579355844133b8e6a0383138cb6cec3894ce7a5560991e7357175c69446150f0"},
		"engine ratios":              {engineRatios, "e5da61127f2d9487bd6f73c9ad1b0ce8c42e35bfbd07e20366c18eca9e18fcc2"},
		"default and decoder ratios": {componentRatios, "eea2b71730bd8a610f36f58386983997adc45d3eed7814b5f984a7b8bb7a15d0"},
	} {
		proposal, err := desiredSplitPlan(tt.source, twoMembers(), nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		requireMinWeightEqualsWeight(t, proposal.Assignments)
		if proposal.InputDigest != tt.want {
			t.Fatalf("%s: input digest %s, want %s", name, proposal.InputDigest, tt.want)
		}
	}
}
