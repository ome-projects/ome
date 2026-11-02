package placement

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
)

// dividedRouterSource adds a router with router weights to a Split source,
// which divides router.minReplicas across the members. A member missing from
// routerWeights gets no router weight on its term.
func dividedRouterSource(routers int, routerWeights map[string]int32) *v1beta1.InferenceService {
	threeToOne := map[string]int32{"member-a": 3, "member-b": 1}
	source := componentSplitSource(4, 12, threeToOne, threeToOne)
	source.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(routers), MaxReplicas: 8}}
	for i, name := range []string{"member-a", "member-b"} {
		if weight, set := routerWeights[name]; set {
			source.Spec.Placement.ClusterAffinity[i].Weights.Router = ptr.To(weight)
		}
	}
	return source
}

// engineWeightToDefault moves each term's engine weight into weights.default,
// which also weighs the routers and divides them.
func engineWeightToDefault(source *v1beta1.InferenceService) *v1beta1.InferenceService {
	for i := range source.Spec.Placement.ClusterAffinity {
		w := source.Spec.Placement.ClusterAffinity[i].Weights
		w.Default, w.Engine = w.Engine, nil
	}
	return source
}

// withoutWeights clears every term's weights block, so every component
// splits evenly over the matched clusters.
func withoutWeights(source *v1beta1.InferenceService) *v1beta1.InferenceService {
	for i := range source.Spec.Placement.ClusterAffinity {
		source.Spec.Placement.ClusterAffinity[i].Weights = nil
	}
	return source
}

// routerFloors plans the source over both members, settles every member, and
// returns each member's router floor; a member without one reports -1.
func routerFloors(t *testing.T, source *v1beta1.InferenceService) (map[string]int32, error) {
	t.Helper()
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		return nil, err
	}
	candidates := settledCandidates(proposal.Assignments)
	out := map[string]int32{}
	for _, candidate := range candidates {
		floors, err := splitMemberFloors(source, candidates, candidate, nil)
		if err != nil {
			t.Fatalf("%s: %v", candidate.Cluster, err)
		}
		router, divided := floorMap(floors)[v1beta1.RouterComponent]
		if !divided {
			router = -1
		}
		out[candidate.Cluster] = router
	}
	return out, nil
}

func TestDividedRouterFloors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		source  func() *v1beta1.InferenceService
		want    map[string]int32
		wantErr string
	}{
		{name: "routers with the engine's weights follow the engine shares", source: func() *v1beta1.InferenceService {
			return dividedRouterSource(3, map[string]int32{"member-a": 3, "member-b": 1})
		}, want: map[string]int32{"member-a": 2, "member-b": 1}},
		{name: "routers with their own weights split by them", source: func() *v1beta1.InferenceService {
			return dividedRouterSource(4, map[string]int32{"member-a": 1, "member-b": 1})
		}, want: map[string]int32{"member-a": 2, "member-b": 2}},
		{name: "fewer routers than engine members holds the plan", source: func() *v1beta1.InferenceService {
			return dividedRouterSource(1, map[string]int32{"member-a": 3, "member-b": 1})
		}, wantErr: "router minimum 1 cannot give each of the 2 homes"},
		{name: "a term without a router weight contributes the engine's", source: func() *v1beta1.InferenceService {
			return dividedRouterSource(4, map[string]int32{"member-a": 1})
		}, want: map[string]int32{"member-a": 2, "member-b": 2}},
		{name: "an engine weight leaves every member the full router policy", source: func() *v1beta1.InferenceService {
			return dividedRouterSource(3, nil)
		}, want: map[string]int32{"member-a": -1, "member-b": -1}},
		{name: "a default weight divides the routers by it", source: func() *v1beta1.InferenceService {
			return engineWeightToDefault(dividedRouterSource(3, nil))
		}, want: map[string]int32{"member-a": 2, "member-b": 1}},
		{name: "a router weight beside default divides the routers by the router weight", source: func() *v1beta1.InferenceService {
			return engineWeightToDefault(dividedRouterSource(4, map[string]int32{"member-a": 1, "member-b": 1}))
		}, want: map[string]int32{"member-a": 2, "member-b": 2}},
		{name: "a default weight with fewer routers than engine members holds the plan", source: func() *v1beta1.InferenceService {
			return engineWeightToDefault(dividedRouterSource(1, nil))
		}, wantErr: "router minimum 1 cannot give each of the 2"},
		{name: "a source without weights divides the routers by the engine shares", source: func() *v1beta1.InferenceService {
			return withoutWeights(dividedRouterSource(3, nil))
		}, want: map[string]int32{"member-a": 2, "member-b": 1}},
		{name: "a source without weights and fewer routers than engine members holds the plan", source: func() *v1beta1.InferenceService {
			return withoutWeights(dividedRouterSource(1, nil))
		}, wantErr: "router minimum 1 cannot give each of the 2"},
		{name: "split replicas scale the router minimum in the declared ratio", source: func() *v1beta1.InferenceService {
			source := dividedRouterSource(4, map[string]int32{"member-a": 3, "member-b": 1})
			source.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](8)}
			return source
		}, want: map[string]int32{"member-a": 6, "member-b": 2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := routerFloors(t, tt.source())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("router floors (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDividedRouterMemberBounds(t *testing.T) {
	for _, tt := range []struct {
		name       string
		unweighted bool
		ceiling    int32
		want       map[string][2]int
	}{
		{name: "routers are pinned at their share", want: map[string][2]int{"member-a": {2, 2}, "member-b": {1, 1}}},
		{name: "a local ceiling scales by the router ratio", ceiling: 3, want: map[string][2]int{"member-a": {2, 3}, "member-b": {1, 3}}},
		{name: "routers without weights are fixed at their share", unweighted: true, want: map[string][2]int{"member-a": {2, 2}, "member-b": {1, 1}}},
		{name: "routers without weights scale a local ceiling by the router ratio", unweighted: true, ceiling: 3, want: map[string][2]int{"member-a": {2, 3}, "member-b": {1, 3}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := dividedRouterSource(3, map[string]int32{"member-a": 3, "member-b": 1})
			if tt.unweighted {
				withoutWeights(source)
			}
			if tt.ceiling > 0 {
				source.Spec.Placement.Split = &v1beta1.SplitSpec{MaxPerCluster: engineCap(tt.ceiling)}
			}
			proposal, err := desiredSplitPlan(source, twoMembers(), nil)
			if err != nil {
				t.Fatal(err)
			}
			candidates := settledCandidates(proposal.Assignments)
			got := map[string][2]int{}
			for _, candidate := range candidates {
				bounds, err := splitPlannedBounds(source, candidates, candidate, nil)
				if err != nil {
					t.Fatal(err)
				}
				member := source.DeepCopy()
				setPlannedReplicas(member, bounds)
				got[candidate.Cluster] = [2]int{*member.Spec.Router.MinReplicas, member.Spec.Router.MaxReplicas}
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("router bounds (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDividedRouterHoldsUntilReplacementsAreReady(t *testing.T) {
	source := dividedRouterSource(4, map[string]int32{"member-a": 3, "member-b": 1})
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{ID: "plan-a"}, Candidates: settledCandidates(proposal.Assignments)}
	// The routers move from 2,2 to 3,1 while member-a's third router starts.
	member := func(routers int) *v1beta1.InferenceService {
		copied := source.DeepCopy()
		copied.Spec.Placement = nil
		copied.Spec.Router.MinReplicas = ptr.To(routers)
		return copied
	}
	observations := map[string]plannedMemberObservation{
		"member-a": {Member: member(2), ComponentReady: map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 2}, MemberState: allocation.MemberState{Known: true, Routable: true}},
		"member-b": {Member: member(2), ComponentReady: map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 2}, MemberState: allocation.MemberState{Known: true, Routable: true}},
	}
	got, err := splitSecondaryHolds(source, observations)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]map[v1beta1.ComponentType]int32{"member-b": {v1beta1.RouterComponent: 2}}
	if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("holds (-want +got):\n%s", diff)
	}
	observations["member-a"] = plannedMemberObservation{Member: member(3), ComponentReady: map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 3}, MemberState: allocation.MemberState{Known: true, Routable: true}}
	if got, err = splitSecondaryHolds(source, observations); err != nil || len(got) > 0 {
		t.Errorf("holds after the replacement is ready = %v, %v; want none", got, err)
	}
}
