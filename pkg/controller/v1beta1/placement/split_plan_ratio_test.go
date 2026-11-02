package placement

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func ratio(min int32, max ...int32) *v1beta1.ReplicaRatio {
	r := &v1beta1.ReplicaRatio{Min: ptr.To(min)}
	if len(max) > 0 {
		r.Max = ptr.To(max[0])
	}
	return r
}

// ratioTerms builds one term per member, each setting the given ratios.
func ratioTerms(engine, decoder map[string]*v1beta1.ReplicaRatio) []v1beta1.ClusterAffinityTerm {
	var out []v1beta1.ClusterAffinityTerm
	for _, name := range []string{"member-a", "member-b"} {
		out = append(out, v1beta1.ClusterAffinityTerm{
			Weights:     &v1beta1.ClusterAffinityWeights{EngineReplicas: engine[name], DecoderReplicas: decoder[name]},
			MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}},
		})
	}
	return out
}

// ratioPlanSource is an engine and decoder source of 12 minimum and 40 maximum
// replicas, divided by the given engine and decoder ratios.
func ratioPlanSource(engine, decoder map[string]*v1beta1.ReplicaRatio) *v1beta1.InferenceService {
	source := ratioSplitSource(12, ptr.To(12))
	source.Spec.Engine.MaxReplicas = 40
	source.Spec.Decoder.MaxReplicas = 40
	source.Spec.Placement.ClusterAffinity = ratioTerms(engine, decoder)
	return source
}

func TestDesiredSplitPlanRecordsMaxWeight(t *testing.T) {
	source := ratioPlanSource(
		map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, 3), "member-b": ratio(1, 1)},
		map[string]*v1beta1.ReplicaRatio{"member-a": ratio(1, 2), "member-b": ratio(1, 1)},
	)
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	type got struct {
		Weight, MaxWeight int64
		Desired           int32
		Decoder           [2]int64
	}
	out := map[string]got{}
	for name, a := range proposal.Assignments {
		g := got{Weight: *a.Weight, MaxWeight: *a.MaxWeight, Desired: a.DesiredReplicas}
		for _, m := range a.ComponentMatches {
			if m.Component == v1beta1.DecoderComponent {
				g.Decoder = [2]int64{*m.Weight, *m.MaxWeight}
			}
		}
		out[name] = g
	}
	want := map[string]got{
		"member-a": {Weight: 2, MaxWeight: 3, Desired: 8, Decoder: [2]int64{1, 2}},
		"member-b": {Weight: 1, MaxWeight: 1, Desired: 4, Decoder: [2]int64{1, 1}},
	}
	if diff := cmp.Diff(want, out); diff != "" {
		t.Fatalf("plan mismatch (-want +got):\n%s", diff)
	}
}

func TestDesiredSplitPlanIntegerFormHasNoMaxWeight(t *testing.T) {
	source := componentSplitSource(12, 12, map[string]int32{"member-a": 2, "member-b": 1}, map[string]int32{"member-a": 1, "member-b": 1})
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(proposal.Assignments) != 2 {
		t.Fatalf("want two assignments, got %d", len(proposal.Assignments))
	}
	want := map[string][2]int64{"member-a": {2, 8}, "member-b": {1, 4}}
	for name, a := range proposal.Assignments {
		if a.MaxWeight != nil {
			t.Errorf("%s: MaxWeight = %d, want nil", name, *a.MaxWeight)
		}
		if got := [2]int64{*a.Weight, int64(a.DesiredReplicas)}; got != want[name] {
			t.Errorf("%s: weight and desired = %v, want %v", name, got, want[name])
		}
		if len(a.ComponentMatches) == 0 {
			t.Errorf("%s: want component matches", name)
		}
		for _, m := range a.ComponentMatches {
			if m.MaxWeight != nil {
				t.Errorf("%s/%s: component MaxWeight = %d, want nil", name, m.Component, *m.MaxWeight)
			}
		}
	}
}

func TestDesiredSplitPlanDigestCoversMaxRatio(t *testing.T) {
	digest := func(maxA int32) string {
		source := ratioPlanSource(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, maxA), "member-b": ratio(1, 1)}, nil)
		proposal, err := desiredSplitPlan(source, twoMembers(), nil)
		if err != nil {
			t.Fatal(err)
		}
		return proposal.InputDigest
	}
	if digest(3) == digest(4) {
		t.Fatal("changing only a maximum ratio left the plan digest unchanged")
	}
}

// decoderOnlyRatioSource is a ratio-form Split source with only a decoder, of
// 6 minimum and 24 maximum replicas, divided 2 to 1 by decoder ratios.
func decoderOnlyRatioSource() *v1beta1.InferenceService {
	source := ratioSplitSource(0, ptr.To(6))
	source.Spec.Engine = nil
	source.Spec.Decoder.MaxReplicas = 24
	source.Spec.Placement.ClusterAffinity = ratioTerms(nil,
		map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, 2), "member-b": ratio(1, 1)})
	return source
}

func TestDesiredSplitPlanDecoderOnlyRatioForm(t *testing.T) {
	source := decoderOnlyRatioSource()
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int32{}
	for name, a := range proposal.Assignments {
		got[name] = a.DesiredReplicas
	}
	if diff := cmp.Diff(map[string]int32{"member-a": 4, "member-b": 2}, got); diff != "" {
		t.Fatalf("decoder shares mismatch (-want +got):\n%s", diff)
	}
	checkBounds(t, map[string]bounds{
		"member-a": {v1beta1.DecoderComponent: {4, 16}},
		"member-b": {v1beta1.DecoderComponent: {2, 8}},
	}, boundsOf(t, source, settledOn(t, source, twoMembers()), "member-a", "member-b"))
}

func TestDesiredSplitPlanRatioFormNamesThePrimaryMinimum(t *testing.T) {
	source := decoderOnlyRatioSource()
	source.Spec.Decoder.MinReplicas = nil
	_, err := desiredSplitPlan(source, twoMembers(), nil)
	if err == nil {
		t.Fatal("want ReplicaCountUnresolved for a decoder without minReplicas")
	}
	if msg := err.Error(); !strings.Contains(msg, "ReplicaCountUnresolved") || !strings.Contains(msg, "decoder.minReplicas") || strings.Contains(msg, "split.replicas") {
		t.Fatalf("error = %q, want ReplicaCountUnresolved naming decoder.minReplicas and not split.replicas", msg)
	}
}

func TestDesiredSplitPlanReloadClearsMaxWeight(t *testing.T) {
	source := ratioPlanSource(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, 3), "member-b": ratio(1, 1)}, nil)
	source.UID = "uid-1"
	source.Status.Placement = &v1beta1.PlacementStatus{
		Plan: &v1beta1.PlacementPlanStatus{SourceUID: source.UID},
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "member-gone", Allocation: &v1beta1.CandidateAllocationStatus{
			DesiredReplicas: 3, Weight: ptr.To[int64](5), MaxWeight: ptr.To[int64](7),
		}}},
	}
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	gone, ok := proposal.Assignments["member-gone"]
	if !ok {
		t.Fatal("want the stored allocation retained")
	}
	if gone.MaxWeight != nil {
		t.Fatalf("stored MaxWeight = %d, want nil after reload", *gone.MaxWeight)
	}
}
