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

// TestSplitInputDigestExcludesMinWeight checks the exact input digest of integer-form
// sources. Integer-form identity must not change: a different digest re-plans
// every stored integer-form service on upgrade.
func TestSplitInputDigestExcludesMinWeight(t *testing.T) {
	engineWeights := srcISVCSplit("accelerator=gpu-a", 6)
	engineWeights.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
	for name, tt := range map[string]struct {
		source *v1beta1.InferenceService
		want   string
	}{
		"no weights":     {srcISVCSplit("accelerator=gpu-a", 6), "579355844133b8e6a0383138cb6cec3894ce7a5560991e7357175c69446150f0"},
		"engine weights": {engineWeights, "e1d893b7f8200fcd5d721091023fccf630c980dcf65de82223ce31ea909bc618"},
		"engine and decoder weights": {
			componentSplitSource(4, 12, map[string]int32{"member-a": 2, "member-b": 1}, map[string]int32{"member-a": 1, "member-b": 3}),
			"0eb7d96da117d8603b81e529b78ca666b7043ec627b94fe6f72bc69bf79336df",
		},
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
