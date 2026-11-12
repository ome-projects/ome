package placement

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
)

// engineCap is split.maxPerCluster with only an engine key, so the other
// components take the engine's cap in their ratio. Zero declares no cap.
func engineCap(n int32) *v1beta1.SplitCeilings {
	if n == 0 {
		return nil
	}
	return &v1beta1.SplitCeilings{Engine: ptr.To(n)}
}

// memberBands plans the source over both members and returns each member's
// [min, max] for every component, as setPlannedReplicas writes them.
func memberBands(t *testing.T, source *v1beta1.InferenceService) map[string]map[v1beta1.ComponentType][2]int {
	t.Helper()
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	candidates := settledCandidates(proposal.Assignments)
	out := map[string]map[v1beta1.ComponentType][2]int{}
	for _, candidate := range candidates {
		bounds, err := splitPlannedBounds(source, candidates, candidate, nil)
		if err != nil {
			t.Fatal(err)
		}
		member := source.DeepCopy()
		setPlannedReplicas(member, bounds)
		out[candidate.Cluster] = map[v1beta1.ComponentType][2]int{
			v1beta1.EngineComponent:  {*member.Spec.Engine.MinReplicas, member.Spec.Engine.MaxReplicas},
			v1beta1.DecoderComponent: {*member.Spec.Decoder.MinReplicas, member.Spec.Decoder.MaxReplicas},
			v1beta1.RouterComponent:  {*member.Spec.Router.MinReplicas, member.Spec.Router.MaxReplicas},
		}
	}
	return out
}

func TestSplitMaxPerClusterCapsEachComponent(t *testing.T) {
	// An unweighted source: engines 2 and 2, decoders 6 and 6, routers 2 and 1.
	type band = map[v1beta1.ComponentType][2]int
	e, d, r := v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent
	for _, tt := range []struct {
		name  string
		split *v1beta1.SplitSpec
		want  map[string]band
	}{
		{
			name:  "without a cap every component is fixed at its share",
			split: nil,
			want:  map[string]band{"member-a": {e: {2, 2}, d: {6, 6}, r: {2, 2}}, "member-b": {e: {2, 2}, d: {6, 6}, r: {1, 1}}},
		},
		{
			name:  "an engine key scales the other caps",
			split: &v1beta1.SplitSpec{MaxPerCluster: &v1beta1.SplitCeilings{Engine: ptr.To[int32](3)}},
			want:  map[string]band{"member-a": {e: {2, 3}, d: {6, 9}, r: {2, 3}}, "member-b": {e: {2, 3}, d: {6, 9}, r: {1, 3}}},
		},
		{
			name:  "a router key replaces the scaled router cap",
			split: &v1beta1.SplitSpec{MaxPerCluster: &v1beta1.SplitCeilings{Engine: ptr.To[int32](3), Router: ptr.To[int32](8)}},
			want:  map[string]band{"member-a": {e: {2, 3}, d: {6, 9}, r: {2, 8}}, "member-b": {e: {2, 3}, d: {6, 9}, r: {1, 8}}},
		},
		{
			name:  "a router key alone leaves the engine and decoder fixed",
			split: &v1beta1.SplitSpec{MaxPerCluster: &v1beta1.SplitCeilings{Router: ptr.To[int32](8)}},
			want:  map[string]band{"member-a": {e: {2, 2}, d: {6, 6}, r: {2, 8}}, "member-b": {e: {2, 2}, d: {6, 6}, r: {1, 8}}},
		},
		{
			name:  "default caps every component the map does not name, without scaling",
			split: &v1beta1.SplitSpec{MaxPerCluster: &v1beta1.SplitCeilings{Default: ptr.To[int32](10), Router: ptr.To[int32](4)}},
			want:  map[string]band{"member-a": {e: {2, 10}, d: {6, 10}, r: {2, 4}}, "member-b": {e: {2, 10}, d: {6, 10}, r: {1, 4}}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := withoutWeights(dividedRouterSource(3, nil))
			source.Spec.Placement.Split = tt.split
			if diff := cmp.Diff(tt.want, memberBands(t, source)); diff != "" {
				t.Errorf("member bands (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSplitMaxPerClusterHoldsAShareAboveItsCap(t *testing.T) {
	for _, tt := range []struct {
		name     string
		ceilings v1beta1.SplitCeilings
		wantErr  string
	}{
		{name: "router", ceilings: v1beta1.SplitCeilings{Router: ptr.To[int32](1)}, wantErr: `router share 2 on cluster "member-a" exceeds its ceiling of 1 replicas from split.maxPerCluster`},
		{name: "decoder that follows the engine shares", ceilings: v1beta1.SplitCeilings{Decoder: ptr.To[int32](5)}, wantErr: `decoder share 6 on cluster "member-a" exceeds its ceiling of 5 replicas from split.maxPerCluster`},
		{name: "engine, from default", ceilings: v1beta1.SplitCeilings{Default: ptr.To[int32](1)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := withoutWeights(dividedRouterSource(3, nil))
			source.Spec.Placement.Split = &v1beta1.SplitSpec{MaxPerCluster: &tt.ceilings}
			_, err := desiredSplitPlan(source, twoMembers(), nil)
			if err == nil {
				t.Fatal("the plan was accepted")
			}
			var limit *allocation.TargetLimitError
			if tt.wantErr == "" && !errors.As(err, &limit) {
				t.Fatalf("want the engine cap to hold the plan, got %v", err)
			}
			if tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tt.wantErr)
			}
		})
	}
}
