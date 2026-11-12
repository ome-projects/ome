package placement

import (
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
)

type bounds = map[v1beta1.ComponentType]plannedBound

// weightedTermsOn builds one term per named member with the given weights.
func weightedTermsOn(weights map[string]v1beta1.ClusterAffinityWeights) []v1beta1.ClusterAffinityTerm {
	var out []v1beta1.ClusterAffinityTerm
	for _, name := range sortedKeys(weights) {
		w := weights[name]
		out = append(out, v1beta1.ClusterAffinityTerm{
			Weights:     &w,
			MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}},
		})
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	var out []string
	for name := range m {
		out = append(out, name)
	}
	slices.Sort(out)
	return out
}

// engineRatios gives each named member the given engine ratio.
func engineRatios(ratios map[string]*v1beta1.ReplicaRatio) []v1beta1.ClusterAffinityTerm {
	weights := map[string]v1beta1.ClusterAffinityWeights{}
	for name, r := range ratios {
		weights[name] = v1beta1.ClusterAffinityWeights{Engine: r}
	}
	return weightedTermsOn(weights)
}

// engineRatioSource is an engine-only ratio-form Split source.
func engineRatioSource(minimum, maximum int, terms []v1beta1.ClusterAffinityTerm) *v1beta1.InferenceService {
	source := ratioSplitSource(minimum, nil)
	source.Spec.Decoder = nil
	source.Spec.Engine.MaxReplicas = maximum
	source.Spec.Placement.ClusterAffinity = terms
	return source
}

func membersNamed(names ...string) []v1beta1.WorkloadCluster {
	var out []v1beta1.WorkloadCluster
	for _, name := range names {
		out = append(out, wc(name, true, map[string]string{"accelerator": "gpu-a"}))
	}
	return out
}

// settledOn plans the source over the clusters and settles every member at its
// desired share.
func settledOn(t *testing.T, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) []v1beta1.CandidatePlacement {
	t.Helper()
	proposal, err := desiredSplitPlan(source, clusters, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out []v1beta1.CandidatePlacement
	for _, name := range sortedKeys(proposal.Assignments) {
		a := proposal.Assignments[name]
		a.CurrentReplicas = a.DesiredReplicas
		out = append(out, v1beta1.CandidatePlacement{Cluster: name, Allocation: &a})
	}
	return out
}

// boundsOf returns the planned bounds of each named member.
func boundsOf(t *testing.T, source *v1beta1.InferenceService, candidates []v1beta1.CandidatePlacement, names ...string) map[string]bounds {
	t.Helper()
	out := map[string]bounds{}
	for _, candidate := range candidates {
		if !slices.Contains(names, candidate.Cluster) {
			continue
		}
		got, err := splitPlannedBounds(source, candidates, candidate, nil)
		if err != nil {
			t.Fatalf("%s: %v", candidate.Cluster, err)
		}
		out[candidate.Cluster] = got
	}
	return out
}

func checkBounds(t *testing.T, want, got map[string]bounds) {
	t.Helper()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("bounds mismatch (-want +got):\n%s", diff)
	}
}

func TestRatioBoundsSummaryExample(t *testing.T) {
	source := ratioPlanSource(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, 3), "member-b": ratio(1, 1)}, nil)
	source.Spec.Decoder.MinReplicas = nil
	got := boundsOf(t, source, settledOn(t, source, twoMembers()), "member-a", "member-b")
	checkBounds(t, map[string]bounds{
		"member-a": {v1beta1.EngineComponent: {8, 30}, v1beta1.DecoderComponent: {8, 30}},
		"member-b": {v1beta1.EngineComponent: {4, 10}, v1beta1.DecoderComponent: {4, 10}},
	}, got)
}

func TestRatioBoundsEqualClusters(t *testing.T) {
	names := []string{"member-a", "member-b", "member-c"}
	source := engineRatioSource(9, 30, engineRatios(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(1), "member-b": ratio(1), "member-c": ratio(1)}))
	got := boundsOf(t, source, settledOn(t, source, membersNamed(names...)), names...)
	want := map[string]bounds{}
	for _, name := range names {
		want[name] = bounds{v1beta1.EngineComponent: {3, 10}}
	}
	checkBounds(t, want, got)
}

func TestRatioBoundsDecoderOwnRatios(t *testing.T) {
	source := ratioSplitSource(12, ptr.To(6))
	source.Spec.Engine.MaxReplicas = 24
	source.Spec.Decoder.MaxReplicas = 9
	source.Spec.Placement.ClusterAffinity = ratioTerms(
		map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, 2), "member-b": ratio(1, 1)},
		map[string]*v1beta1.ReplicaRatio{"member-a": ratio(1, 2), "member-b": ratio(1, 1)},
	)
	got := boundsOf(t, source, settledOn(t, source, twoMembers()), "member-a", "member-b")
	checkBounds(t, map[string]bounds{
		"member-a": {v1beta1.EngineComponent: {8, 16}, v1beta1.DecoderComponent: {3, 6}},
		"member-b": {v1beta1.EngineComponent: {4, 8}, v1beta1.DecoderComponent: {3, 3}},
	}, got)
}

// TestRatioBoundsOmittedMinAndMaxCountAsOne divides 12 to 40 engines by ratios
// {max 3} and {min 2}. The omitted values count as 1, so the min ratios are 1
// and 2 (4 and 8 engines) and the max ratios are 3 and 1 (30 and 10).
func TestRatioBoundsOmittedMinAndMaxCountAsOne(t *testing.T) {
	source := engineRatioSource(12, 40, engineRatios(map[string]*v1beta1.ReplicaRatio{
		"member-a": {Max: ptr.To[int32](3)}, "member-b": {Min: ptr.To[int32](2)},
	}))
	got := boundsOf(t, source, settledOn(t, source, twoMembers()), "member-a", "member-b")
	checkBounds(t, map[string]bounds{
		"member-a": {v1beta1.EngineComponent: {4, 30}},
		"member-b": {v1beta1.EngineComponent: {8, 10}},
	}, got)
}

func TestRatioBoundsRaisesMaxToShare(t *testing.T) {
	source := engineRatioSource(8, 8, engineRatios(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(3, 1), "member-b": ratio(1, 3)}))
	candidates := settledOn(t, source, twoMembers())
	got := boundsOf(t, source, candidates, "member-a", "member-b")
	checkBounds(t, map[string]bounds{
		"member-a": {v1beta1.EngineComponent: {6, 6}},
		"member-b": {v1beta1.EngineComponent: {2, 6}},
	}, got)
	for _, tt := range []struct {
		member string
		share  int32
		raised map[v1beta1.ComponentType]int32
	}{
		{member: "member-a", share: 6, raised: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4}},
		{member: "member-b", share: 2, raised: map[v1beta1.ComponentType]int32{}},
	} {
		member := candidates[slices.IndexFunc(candidates, func(c v1beta1.CandidatePlacement) bool { return c.Cluster == tt.member })]
		minimums, err := splitComponentMinimums(source)
		if err != nil {
			t.Fatal(err)
		}
		shares, _, err := minimums.memberShares(source, candidates, member, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(shares) != 1 || shares[0].Replicas != tt.share {
			t.Fatalf("%s shares = %+v, want %d engines", tt.member, shares, tt.share)
		}
		_, raised, err := ratioCeilings(source, candidates, member, shares)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(tt.raised, raised); diff != "" {
			t.Errorf("%s raised (-want +got):\n%s", tt.member, diff)
		}
	}
}

func TestRatioBoundsNoSourceMax(t *testing.T) {
	source := ratioPlanSource(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, 3), "member-b": ratio(1, 1)},
		map[string]*v1beta1.ReplicaRatio{"member-a": ratio(1, 2), "member-b": ratio(1, 1)})
	source.Spec.Engine.MaxReplicas = 0
	source.Spec.Decoder.MaxReplicas = 0
	got := boundsOf(t, source, settledOn(t, source, twoMembers()), "member-a", "member-b")
	checkBounds(t, map[string]bounds{
		"member-a": {v1beta1.EngineComponent: {8, 8}, v1beta1.DecoderComponent: {6, 6}},
		"member-b": {v1beta1.EngineComponent: {4, 4}, v1beta1.DecoderComponent: {6, 6}},
	}, got)
}

// TestRatioBoundsDrainingClusterFixedAtShare removes member-c from the terms of
// a 9 to 30 engine source. member-c drains at its 3 engines and cannot grow;
// member-a and member-b divide all 30 of the maximum.
func TestRatioBoundsDrainingClusterFixedAtShare(t *testing.T) {
	source := engineRatioSource(9, 30, engineRatios(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(1), "member-b": ratio(1)}))
	assigned := func(desired int32) *v1beta1.CandidateAllocationStatus {
		return &v1beta1.CandidateAllocationStatus{Matched: true, CurrentReplicas: 3, OriginalReplicas: 3, DesiredReplicas: desired, Weight: ptr.To[int64](1), MaxWeight: ptr.To[int64](1)}
	}
	candidates := []v1beta1.CandidatePlacement{
		{Cluster: "member-a", Allocation: assigned(5)},
		{Cluster: "member-b", Allocation: assigned(4)},
		{Cluster: "member-c", Allocation: &v1beta1.CandidateAllocationStatus{CurrentReplicas: 3, OriginalReplicas: 3, DrainRequested: true}},
	}
	got := boundsOf(t, source, candidates, "member-a", "member-b", "member-c")
	checkBounds(t, map[string]bounds{
		"member-a": {v1beta1.EngineComponent: {3, 15}},
		"member-b": {v1beta1.EngineComponent: {3, 15}},
		"member-c": {v1beta1.EngineComponent: {3, 3}},
	}, got)
}

// TestRatioBoundsZeroShareClusterGetsNoMaximum divides a minimum of 2 over
// three equal clusters, so member-c gets no member. The two clusters with a
// member divide all 9 of the maximum.
func TestRatioBoundsZeroShareClusterGetsNoMaximum(t *testing.T) {
	names := []string{"member-a", "member-b", "member-c"}
	source := engineRatioSource(2, 9, engineRatios(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(1), "member-b": ratio(1), "member-c": ratio(1)}))
	candidates := settledOn(t, source, membersNamed(names...))
	for _, candidate := range candidates {
		if candidate.Cluster == "member-c" && candidate.Allocation.DesiredReplicas != 0 {
			t.Fatalf("member-c has a share of %d, want none", candidate.Allocation.DesiredReplicas)
		}
	}
	got := boundsOf(t, source, candidates, "member-a", "member-b")
	checkBounds(t, map[string]bounds{
		"member-a": {v1beta1.EngineComponent: {1, 5}},
		"member-b": {v1beta1.EngineComponent: {1, 4}},
	}, got)
}

func TestRatioBoundsUndividedRouterKeepsPolicy(t *testing.T) {
	withRouter := func(routerRatios bool) *v1beta1.InferenceService {
		source := ratioPlanSource(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(2, 3), "member-b": ratio(1, 1)}, nil)
		source.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2), MaxReplicas: 6}}
		if routerRatios {
			for i := range source.Spec.Placement.ClusterAffinity {
				source.Spec.Placement.ClusterAffinity[i].Weights.Router = ratio(1)
			}
		}
		return source
	}
	source := withRouter(false)
	got := boundsOf(t, source, settledOn(t, source, twoMembers()), "member-a", "member-b")
	for name, b := range got {
		if _, ok := b[v1beta1.RouterComponent]; ok {
			t.Fatalf("%s: undivided router got a bound %+v", name, b[v1beta1.RouterComponent])
		}
	}
	source = withRouter(true)
	got = boundsOf(t, source, settledOn(t, source, twoMembers()), "member-a", "member-b")
	for _, name := range []string{"member-a", "member-b"} {
		if diff := cmp.Diff(plannedBound{1, 3}, got[name][v1beta1.RouterComponent]); diff != "" {
			t.Errorf("%s divided router bound (-want +got):\n%s", name, diff)
		}
	}
}

// TestRatioBoundsUnweightedSplitBlockUnchanged: a source without weights keeps
// the cap from split.maxPerCluster and ignores the source's maxReplicas.
func TestRatioBoundsUnweightedSplitBlockUnchanged(t *testing.T) {
	source := ratioSplitSource(60, ptr.To(40))
	source.Spec.Engine.MaxReplicas = 7
	source.Spec.Decoder.MaxReplicas = 7
	source.Spec.Placement.Split = &v1beta1.SplitSpec{MaxPerCluster: engineCap(50)}
	got := boundsOf(t, source, settledOn(t, source, twoMembers()), "member-a")
	checkBounds(t, map[string]bounds{
		"member-a": {v1beta1.EngineComponent: {30, 50}, v1beta1.DecoderComponent: {20, 34}},
	}, got)
}

func TestSplitSharesChangedInPlaceRatioHeadroom(t *testing.T) {
	for _, tt := range []struct {
		name    string
		maximum int
		want    bool
	}{
		{name: "maximum above minimum", maximum: 20, want: true},
		{name: "maximum equal to minimum", maximum: 12, want: false},
		{name: "no maximum", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := engineRatioSource(12, tt.maximum, engineRatios(map[string]*v1beta1.ReplicaRatio{"member-a": ratio(1), "member-b": ratio(1)}))
			observations := map[string]plannedMemberObservation{}
			var candidates []v1beta1.CandidatePlacement
			for _, name := range []string{"member-a", "member-b"} {
				candidates = append(candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: &v1beta1.CandidateAllocationStatus{
					Matched: true, OriginalReplicas: 6, CurrentReplicas: 6, DesiredReplicas: 6, Weight: ptr.To[int64](1), MaxWeight: ptr.To[int64](1),
				}})
				observations[name] = plannedMemberObservation{
					Member:         &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(6)}}}},
					MemberState:    allocation.MemberState{Known: true, Eligible: true},
					ComponentReady: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 6},
				}
			}
			source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{}, Candidates: candidates}
			if got := splitSharesChangedInPlace(source, observations); got != tt.want {
				t.Fatalf("splitSharesChangedInPlace = %v, want %v", got, tt.want)
			}
		})
	}
}
