package placement

import (
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestSplitPause_WeightsBlockWhileMembersServe_Ends gives the autoscaled Split
// service terms with a weights block and engine weights 2:1, which move the
// engine shares from 6/6 to 8/4. In the decoder weights case, decoder weights
// 1:3 move the decoder shares from 4/4 to 2/6. In the default weights case,
// weights.default 2:1 replaces weight, and the decoders follow the engines to
// 5/3. Each autoscaler keeps 9 engines and 6 decoders, more than every new
// share, so the pause ends.
func TestSplitPause_WeightsBlockWhileMembersServe_Ends(t *testing.T) {
	engines := map[string]int32{"member-a": 8, "member-b": 4}
	for _, tt := range []struct {
		name     string
		terms    []v1beta1.ClusterAffinityTerm
		decoders map[string]int32
	}{
		{name: "decoder weights", terms: perMemberTerms(map[string]int32{"member-a": 2, "member-b": 1}, map[string]int32{"member-a": 1, "member-b": 3}, nil),
			decoders: map[string]int32{"member-a": 2, "member-b": 6}},
		{name: "default weights", terms: defaultWeightsTerms(map[string]int32{"member-a": 2, "member-b": 1}),
			decoders: map[string]int32{"member-a": 5, "member-b": 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := autoscaledSplitFixture(t)
			changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) { s.Spec.Placement.ClusterAffinity = tt.terms })
			paused := false
			got := serveAutoscaledSplit(t, f, autoscaledAllBusy, autoscaledAllBusy, func(got *v1beta1.InferenceService) bool {
				paused = paused || got.Status.Placement.Plan.PauseSurge
				return autoscaledAllConverged(got)
			})
			if !paused {
				t.Fatalf("the change did not pause member growth: %s", describeWeightedSplit(t, f, got))
			}
			if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
				t.Fatalf("every member serves its new shares but growth stayed paused: %s", describeWeightedSplit(t, f, got))
			}
			for _, candidate := range got.Status.Placement.Candidates {
				if a := candidate.Allocation; a.CurrentReplicas != engines[candidate.Cluster] || a.DesiredReplicas != engines[candidate.Cluster] {
					t.Fatalf("%s did not take an engine share of %d: %+v", candidate.Cluster, engines[candidate.Cluster], a)
				}
				member, _ := allMemberOn(t, f, candidate.Cluster)
				if decoders := memberMinimums(member)[v1beta1.DecoderComponent]; decoders != tt.decoders[candidate.Cluster] {
					t.Fatalf("%s has a decoder minimum of %d, not its share of %d", candidate.Cluster, decoders, tt.decoders[candidate.Cluster])
				}
			}
		})
	}
}

// TestSplitPause_MemberBelowWeightedDecoderShare_Waits sets engine weights 2:1
// and decoder weights 1:3 on the autoscaled Split service, so member-b's decoder
// share rises from 4 to 6. The pause stays on while member-b serves 5
// decoders, and ends once it serves 6.
func TestSplitPause_MemberBelowWeightedDecoderShare_Waits(t *testing.T) {
	f := autoscaledSplitFixture(t)
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 2, "member-b": 1}, map[string]int32{"member-a": 1, "member-b": 3}, nil)
	})
	running := func(decoders int32) func(string) map[v1beta1.ComponentType]int32 {
		return func(name string) map[v1beta1.ComponentType]int32 {
			if name == "member-b" {
				return map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 9, v1beta1.DecoderComponent: decoders, v1beta1.RouterComponent: 2}
			}
			return autoscaledAllBusy
		}
	}
	held := serveSplitMembers(t, f, running(5), autoscaledAllBusy, func(*v1beta1.InferenceService) bool { return false })
	expectCleanupWait(t, f, held, "member-b's sixth decoder")
	expectNoReplacementHolds(t, f, held)
	got := serveSplitMembers(t, f, running(6), autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member-b serves its decoder share of 6 but growth stayed paused: %s", describeWeightedSplit(t, f, got))
	}
}

// TestSplitPause_DecoderWeightsDuringAJoin_EndsOnceAllServe adds member-c to
// the autoscaled Split service with engine weights 2, 2 and 1 for member-a,
// member-b and member-c, and minimums of 15 engines and 10 decoders. The
// engines split 6/6/3 and the decoders 4/4/2. While member-c serves nothing and
// the join keeps the pause on, decoder weights of 1 on every term split the
// decoders 4/3/3. The pause stays on while member-c serves its old 2 decoders,
// and ends once it serves 3. The autoscalers on member-a and member-b keep 9
// engines and 6 decoders throughout.
func TestSplitPause_DecoderWeightsDuringAJoin_EndsOnceAllServe(t *testing.T) {
	f := autoscaledSplitFixture(t)
	registerAutoscaledAllMember(t, f, "member-c")
	engineWeights := map[string]int32{"member-a": 2, "member-b": 2, "member-c": 1}
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(15), ptr.To(10)
		s.Spec.Placement.ClusterAffinity = perMemberTerms(engineWeights, nil, nil)
	})
	running := func(joiner map[v1beta1.ComponentType]int32) func(string) map[v1beta1.ComponentType]int32 {
		return func(name string) map[v1beta1.ComponentType]int32 {
			if name == "member-c" {
				return joiner
			}
			return autoscaledAllBusy
		}
	}
	joining := serveSplitMembers(t, f, running(map[v1beta1.ComponentType]int32{}), autoscaledAllBusy, func(*v1beta1.InferenceService) bool { return false })
	if !joining.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the join did not keep the pause on while member-c serves nothing: %s", describeWeightedSplit(t, f, joining))
	}
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Placement.ClusterAffinity = perMemberTerms(engineWeights, map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1}, nil)
	})
	held := serveSplitMembers(t, f, running(map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 3, v1beta1.DecoderComponent: 2, v1beta1.RouterComponent: 2}), autoscaledAllBusy, func(*v1beta1.InferenceService) bool { return false })
	expectCleanupWait(t, f, held, "member-c's third decoder")
	expectNoReplacementHolds(t, f, held)
	got := serveSplitMembers(t, f, running(map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 3, v1beta1.DecoderComponent: 3, v1beta1.RouterComponent: 2}), autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("every member serves its new shares but growth stayed paused: %s", describeWeightedSplit(t, f, got))
	}
	for name, want := range map[string][2]int32{"member-a": {6, 4}, "member-b": {6, 3}, "member-c": {3, 3}} {
		member, _ := allMemberOn(t, f, name)
		if minimums := memberMinimums(member); minimums[v1beta1.EngineComponent] != want[0] || minimums[v1beta1.DecoderComponent] != want[1] {
			t.Fatalf("%s has minimums %v, not %d engines and %d decoders", name, minimums, want[0], want[1])
		}
	}
}

// TestSplitPause_MemberBelowDividedRouterShare_Waits divides 4 routers 2/2
// over the autoscaled Split service, then sets engine weights 2:1 and router
// weights 3:1, so the router shares move to 3/1. member-b runs 3 routers
// throughout, so it keeps none for member-a. The pause stays on while member-a
// serves 2 routers, and ends once it serves 3.
func TestSplitPause_MemberBelowDividedRouterShare_Waits(t *testing.T) {
	f := autoscaledSplitFixture(t)
	divideAutoscaledSplitRouters(t, f)
	moveRoutersToMemberA(t, f)
	requested := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 9, v1beta1.DecoderComponent: 6, v1beta1.RouterComponent: 3}
	held := serveSplitMembers(t, f, memberARuns(2, requested), requested, func(*v1beta1.InferenceService) bool { return false })
	expectCleanupWait(t, f, held, "member-a's third router")
	expectNoReplacementHolds(t, f, held)
	got := serveSplitMembers(t, f, memberARuns(3, requested), requested, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member-a serves its router share of 3 but growth stayed paused: %s", describeWeightedSplit(t, f, got))
	}
}

// TestSplitPause_MemberKeepsRoutersForReplacement_Waits divides 4 routers 2/2
// over the autoscaled Split service, then sets engine weights 2:1 and router
// weights 3:1, so the router shares move to 3/1. Both members run 2 routers,
// so member-b keeps its second router and the pause stays on. Once member-a
// serves 3, the pause ends and member-b's router minimum falls to 1.
func TestSplitPause_MemberKeepsRoutersForReplacement_Waits(t *testing.T) {
	f := autoscaledSplitFixture(t)
	divideAutoscaledSplitRouters(t, f)
	moveRoutersToMemberA(t, f)
	held := serveSplitMembers(t, f, memberARuns(2, autoscaledAllBusy), autoscaledAllBusy, func(*v1beta1.InferenceService) bool { return false })
	expectCleanupWait(t, f, held, "member-a's third router")
	observed := f.reconciler.observeSplitMembers(t.Context(), held, slices.Sorted(maps.Keys(f.workers)))
	holds, err := splitSecondaryHolds(held, observed)
	if diff := cmp.Diff(map[string]map[v1beta1.ComponentType]int32{"member-b": {v1beta1.RouterComponent: 2}}, holds); err != nil || diff != "" {
		t.Fatalf("member-b does not keep its second router for member-a: %v; holds (-want +got):\n%s\n%s", err, diff, describeWeightedSplit(t, f, held))
	}
	got := serveSplitMembers(t, f, memberARuns(3, autoscaledAllBusy), autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member-a serves its router share of 3 but growth stayed paused: %s", describeWeightedSplit(t, f, got))
	}
	if member, _ := allMemberOn(t, f, "member-b"); memberMinimums(member)[v1beta1.RouterComponent] != 1 {
		t.Fatalf("member-b kept a router minimum of %d after member-a served 3, not its share of 1", memberMinimums(member)[v1beta1.RouterComponent])
	}
}

// TestSplitPause_JoiningMemberBelowRouterShare_Waits adds member-c to the
// autoscaled Split service with engine weights 2, 2 and 1, minimums of 15
// engines and 10 decoders, and 6 routers divided by router weights of 1 on
// every term. The engines split 6/6/3, the decoders 4/4/2 and the routers
// 2/2/2. The pause stays on while member-c serves 1 router, and ends once it
// serves 2. The autoscalers on member-a and member-b keep 9 engines and 6
// decoders throughout.
func TestSplitPause_JoiningMemberBelowRouterShare_Waits(t *testing.T) {
	f := autoscaledSplitFixture(t)
	registerAutoscaledAllMember(t, f, "member-c")
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(15), ptr.To(10)
		s.Spec.Router.MinReplicas, s.Spec.Router.MaxReplicas = ptr.To(6), 12
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 2, "member-b": 2, "member-c": 1}, nil, map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1})
	})
	running := func(routers int32) func(string) map[v1beta1.ComponentType]int32 {
		return func(name string) map[v1beta1.ComponentType]int32 {
			if name == "member-c" {
				return map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 3, v1beta1.DecoderComponent: 2, v1beta1.RouterComponent: routers}
			}
			return autoscaledAllBusy
		}
	}
	held := serveSplitMembers(t, f, running(1), autoscaledAllBusy, func(*v1beta1.InferenceService) bool { return false })
	expectCleanupWait(t, f, held, "member-c's second router")
	expectNoReplacementHolds(t, f, held)
	got := serveSplitMembers(t, f, running(2), autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member-c serves its router share of 2 but growth stayed paused: %s", describeWeightedSplit(t, f, got))
	}
	for _, name := range []string{"member-a", "member-b", "member-c"} {
		if member, _ := allMemberOn(t, f, name); memberMinimums(member)[v1beta1.RouterComponent] != 2 {
			t.Fatalf("%s has a router minimum of %d, not its share of 2", name, memberMinimums(member)[v1beta1.RouterComponent])
		}
	}
}

// divideAutoscaledSplitRouters raises the autoscaled Split service to 4 routers
// and divides them by router weights of 1 on both terms, then waits until each
// member settles on 2.
func divideAutoscaledSplitRouters(t *testing.T, f *backendFixture) {
	t.Helper()
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Router.MinReplicas, s.Spec.Router.MaxReplicas = ptr.To(4), 8
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 1, "member-b": 1}, nil, map[string]int32{"member-a": 1, "member-b": 1})
	})
	if divided := serveAutoscaledSplit(t, f, autoscaledAllRunning, autoscaledAllRunning, autoscaledAllConverged); !autoscaledAllConverged(divided) || divided.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the service did not settle on 2 routers per member: %s", describeWeightedSplit(t, f, divided))
	}
}

// moveRoutersToMemberA sets engine weights 2:1 and router weights 3:1, which
// move the engine shares to 8/4 and the router shares to 3/1.
func moveRoutersToMemberA(t *testing.T, f *backendFixture) {
	t.Helper()
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 2, "member-b": 1}, nil, map[string]int32{"member-a": 3, "member-b": 1})
	})
}

// memberARuns has member-a run 9 engines, 6 decoders and the given number of
// routers, and every other member run others.
func memberARuns(routers int32, others map[v1beta1.ComponentType]int32) func(string) map[v1beta1.ComponentType]int32 {
	return func(name string) map[v1beta1.ComponentType]int32 {
		if name == "member-a" {
			return map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 9, v1beta1.DecoderComponent: 6, v1beta1.RouterComponent: routers}
		}
		return others
	}
}

// perMemberTerms gives each member one term with its engine weight in
// weights.engine. A member named in decoder or router also gets those weights.
func perMemberTerms(engine, decoder, router map[string]int32) []v1beta1.ClusterAffinityTerm {
	var out []v1beta1.ClusterAffinityTerm
	for _, name := range slices.Sorted(maps.Keys(engine)) {
		term := v1beta1.ClusterAffinityTerm{Weights: &v1beta1.ClusterAffinityWeights{Engine: ptr.To(engine[name])}, MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}}}
		d, hasDecoder := decoder[name]
		r, hasRouter := router[name]
		if hasDecoder {
			term.Weights.Decoder = ptr.To(d)
		}
		if hasRouter {
			term.Weights.Router = ptr.To(r)
		}
		out = append(out, term)
	}
	return out
}

// defaultWeightsTerms gives each member one term whose weights block sets
// weights.default, which means the same as weight.
func defaultWeightsTerms(weights map[string]int32) []v1beta1.ClusterAffinityTerm {
	var out []v1beta1.ClusterAffinityTerm
	for _, name := range slices.Sorted(maps.Keys(weights)) {
		out = append(out, v1beta1.ClusterAffinityTerm{
			Weights:     &v1beta1.ClusterAffinityWeights{Default: ptr.To(weights[name])},
			MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}},
		})
	}
	return out
}

// expectCleanupWait fails the test unless the move finished and growth stays
// paused with reason AwaitingMemberCleanup. want names what the pause waits for.
func expectCleanupWait(t *testing.T, f *backendFixture, held *v1beta1.InferenceService, want string) {
	t.Helper()
	if condition := held.Status.GetCondition(v1beta1.PlacementConverged); !held.Status.Placement.Plan.PauseSurge || condition == nil || condition.Reason != "AwaitingMemberCleanup" {
		t.Fatalf("the move did not finish and wait for %s: %s", want, describeWeightedSplit(t, f, held))
	}
}

// expectNoReplacementHolds fails the test when a member keeps decoders or
// routers for a replacement on another member, so that only a member below its
// own share can explain a pause that stays on.
func expectNoReplacementHolds(t *testing.T, f *backendFixture, source *v1beta1.InferenceService) {
	t.Helper()
	observed := f.reconciler.observeSplitMembers(t.Context(), source, slices.Sorted(maps.Keys(f.workers)))
	if holds, err := splitSecondaryHolds(source, observed); err != nil || len(holds) > 0 {
		t.Fatalf("a member keeps replicas for a replacement elsewhere: %v (%v); %s", holds, err, describeWeightedSplit(t, f, source))
	}
}

// describeWeightedSplit adds each member's minimums and ready counts per
// component to describeAutoscaledAll.
func describeWeightedSplit(t *testing.T, f *backendFixture, source *v1beta1.InferenceService) string {
	t.Helper()
	out := describeAutoscaledAll(t, f, source)
	names := slices.Sorted(maps.Keys(f.workers))
	observed := f.reconciler.observeSplitMembers(t.Context(), source, names)
	for _, name := range names {
		if member, ok := allMemberOn(t, f, name); ok {
			out += fmt.Sprintf("; %s minimums=%v ready=%v", name, memberMinimums(member), observed[name].ComponentReady)
		}
	}
	return out
}
