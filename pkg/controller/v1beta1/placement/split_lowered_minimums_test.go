package placement

import (
	"maps"
	"slices"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// TestSplitCleanupWaitFollowsMovesOnly: a complete Split step with a local
// ceiling ends the pause while autoscalers keep replicas above the new shares.
// That applies whether the shares fell, rose, moved or stayed, when a member
// joins, and when the terms set component weights or divide the routers. The
// wait stays for a plan without a positive ceiling, a member below its new
// engine or router share or not eligible, and a member that leaves or drains.
// It also stays while a member keeps routers until their replacements take
// traffic, for unassigned replicas and for a capacity split.
func TestSplitCleanupWaitFollowsMovesOnly(t *testing.T) {
	type edit func(*v1beta1.InferenceService, map[string]plannedMemberObservation)
	assigned := func(name string, change func(*v1beta1.CandidateAllocationStatus)) edit {
		return func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			for _, candidate := range s.Status.Placement.Candidates {
				if candidate.Cluster == name {
					change(candidate.Allocation)
				}
			}
		}
	}
	// dividedRouters divides 4 routers by router weights 1 and 3, which give a
	// and b router shares of 1 and 3. Each member's spec sets its router minimum
	// from minimums, and it runs the routers in ready.
	dividedRouters := func(minimums, ready map[string]int32) edit {
		return func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			routerWeights := map[string]int32{"a": 1, "b": 3}
			s.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(4), MaxReplicas: 8}}
			s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"a": 1, "b": 1}, nil, routerWeights)
			withRatioHeadroom(s)
			for _, candidate := range s.Status.Placement.Candidates {
				candidate.Allocation.Weight = ptr.To[int64](1)
				candidate.Allocation.ComponentMatches = []v1beta1.ComponentAffinityMatch{{Component: v1beta1.RouterComponent, Weight: ptr.To(int64(routerWeights[candidate.Cluster]))}}
			}
			for name, observation := range o {
				observation.Member.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(int(minimums[name])), MaxReplicas: 8}}
				observation.ComponentReady[v1beta1.RouterComponent] = ready[name]
				o[name] = observation
			}
		}
	}
	ends := allocation.Step{Targets: map[string]int32{"a": 3, "b": 3}, Complete: true}
	waiting := allocation.Step{Targets: map[string]int32{"a": 3, "b": 3}, Reason: "AwaitingMemberCleanup"}
	for _, tt := range []struct {
		name string
		edit edit
		want allocation.Step
	}{
		{name: "lower shares on members that all stay", want: ends},
		{name: "rollout surge in flight", want: ends, edit: func(_ *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			observation := o["a"]
			observation.RolloutReserved = 1
			o["a"] = observation
		}},
		{name: "unchanged shares", want: allocation.Step{Targets: map[string]int32{"a": 6, "b": 6}, Complete: true}, edit: func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			s.Status.Placement.Plan.AssignedReplicas = 12
			for _, candidate := range s.Status.Placement.Candidates {
				candidate.Allocation.CurrentReplicas, candidate.Allocation.DesiredReplicas = 6, 6
			}
			for name, observation := range o {
				observation.Member.Spec.Engine.MinReplicas = ptr.To(6)
				observation.ComponentReady = map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 6}
				observation.MemberState.Ready, observation.MemberState.Occupied = 6, 9
				o[name] = observation
			}
		}},
		{name: "no local ceiling", want: waiting, edit: func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Spec.Placement.Split = nil
		}},
		{name: "zero local ceiling", want: waiting, edit: func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Spec.Placement.Split.MaxPerCluster = engineCap(0)
		}},
		{name: "member below its new minimum", want: waiting, edit: func(_ *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			observation := o["b"]
			observation.ComponentReady = map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 2}
			o["b"] = observation
		}},
		{name: "member not eligible", want: waiting, edit: func(_ *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			observation := o["b"]
			observation.MemberState.Eligible = false
			o["b"] = observation
		}},
		{name: "one share rose and another fell", want: ends, edit: assigned("a", func(a *v1beta1.CandidateAllocationStatus) { a.OriginalReplicas = 2 })},
		{name: "raised shares", want: ends, edit: func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			for _, candidate := range s.Status.Placement.Candidates {
				candidate.Allocation.OriginalReplicas = 2
			}
		}},
		{name: "member spec behind its raised share", want: waiting, edit: func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			assigned("a", func(a *v1beta1.CandidateAllocationStatus) { a.OriginalReplicas = 2 })(s, o)
			observation := o["a"]
			observation.Member.Spec.Engine.MinReplicas = ptr.To(2)
			observation.ComponentReady = map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 2}
			o["a"] = observation
		}},
		{name: "added member that serves", want: ends, edit: assigned("b", func(a *v1beta1.CandidateAllocationStatus) { a.OriginalReplicas = 0 })},
		{name: "added member not ready", want: waiting, edit: func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			assigned("b", func(a *v1beta1.CandidateAllocationStatus) { a.OriginalReplicas = 0 })(s, o)
			observation := o["b"]
			observation.ComponentReady = map[v1beta1.ComponentType]int32{}
			o["b"] = observation
		}},
		{name: "released member", want: allocation.Step{Targets: map[string]int32{"a": 3, "b": 3, "c": 0}, Reason: "AwaitingMemberCleanup"}, edit: func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			s.Status.Placement.Candidates = append(s.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: "c", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "c-uid", OriginalReplicas: 3}})
			o["c"] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Absent: true}}
		}},
		{name: "member asked to drain", want: waiting, edit: assigned("b", func(a *v1beta1.CandidateAllocationStatus) { a.DrainRequested = true })},
		{name: "component weights", want: ends, edit: func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{
				Weights:     &v1beta1.ClusterAffinityWeights{Default: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1)}},
				MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"a", "b"}}},
			}}
			withRatioHeadroom(s)
		}},
		{name: "divided routers at their shares", want: ends, edit: dividedRouters(map[string]int32{"a": 1, "b": 3}, map[string]int32{"a": 1, "b": 3})},
		{name: "member below its router share", want: waiting, edit: dividedRouters(map[string]int32{"a": 1, "b": 3}, map[string]int32{"a": 1, "b": 2})},
		{name: "member keeps routers for a replacement", want: waiting, edit: func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			// Every member runs at least its router share, but b takes no
			// traffic yet, so a keeps 2 routers until it does.
			dividedRouters(map[string]int32{"a": 2, "b": 3}, map[string]int32{"a": 2, "b": 3})(s, o)
			observation := o["b"]
			observation.MemberState.Routable = false
			o["b"] = observation
		}},
		{name: "unassigned replicas", want: waiting, edit: func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Status.Placement.Plan.OriginalUnassignedReplicas = 1
		}},
		{name: "capacity split", want: waiting, edit: func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := srcISVCMode(v1beta1.PlacementModeSplit, "")
			s.Spec.Engine.MinReplicas, s.Spec.Placement.Split = ptr.To(6), &v1beta1.SplitSpec{MaxPerCluster: engineCap(18)}
			s.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{InputDigest: "intent", AssignedReplicas: 6, PauseSurge: true}}
			o := map[string]plannedMemberObservation{}
			// Each member began the change at a share of 6, took 3, and its
			// autoscaler keeps more replicas than that.
			for _, m := range []struct {
				name     string
				occupied int32
			}{{"a", 6}, {"b", 5}} {
				s.Status.Placement.Candidates = append(s.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: m.name, Allocation: &v1beta1.CandidateAllocationStatus{
					ClusterUID: types.UID(m.name + "-uid"), OriginalReplicas: 6, CurrentReplicas: 3, DesiredReplicas: 3,
				}})
				member := s.DeepCopy()
				member.Spec.Engine.MinReplicas = ptr.To(3)
				o[m.name] = plannedMemberObservation{
					Member: member, ComponentReady: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 3},
					MemberState: allocation.MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: m.occupied}, PauseAcknowledged: true,
				}
			}
			if tt.edit != nil {
				tt.edit(s, o)
			}
			got, err := advanceSplitPlan(s, o)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("step (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSplitLowerMinimumsEndThePause lowers the minimums of an autoscaled Split
// service whose members all stay while their autoscalers keep more replicas
// than the new shares. The pause ends once each member has applied its lower
// share. A member that leaves keeps the cleanup wait.
func TestSplitLowerMinimumsEndThePause(t *testing.T) {
	t.Run("lower minimums on members that all stay", func(t *testing.T) {
		f := autoscaledSplitFixture(t)
		setAutoscaledAll(t, f, 6, 4, "")
		got := serveAutoscaledSplit(t, f, autoscaledAllRunning, autoscaledAllBusy, autoscaledAllConverged)
		if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
			t.Fatalf("member growth stayed paused: %s", describeAutoscaledAll(t, f, got))
		}
		for _, candidate := range got.Status.Placement.Candidates {
			if a := candidate.Allocation; a.OriginalReplicas != 3 || a.CurrentReplicas != 3 || a.DesiredReplicas != 3 {
				t.Fatalf("%s did not keep its lower share: %+v", candidate.Cluster, a)
			}
			member, ok := allMemberOn(t, f, candidate.Cluster)
			if !ok {
				t.Fatalf("%s lost its member", candidate.Cluster)
			}
			if policy, err := protocol.FromDerived(member); err != nil || policy == nil || policy.PauseSurge {
				t.Fatalf("%s still defers autoscaler growth: %+v (%v)", candidate.Cluster, policy, err)
			}
		}
	})
	for _, tt := range []struct {
		name            string
		engine, decoder int
		selector        string
	}{
		{name: "member that leaves keeps the wait", engine: 6, decoder: 4, selector: "metadata.name=member-a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := autoscaledSplitFixture(t)
			setAutoscaledAll(t, f, tt.engine, tt.decoder, tt.selector)
			got := serveAutoscaledSplit(t, f, autoscaledAllBusy, autoscaledAllBusy, func(got *v1beta1.InferenceService) bool {
				if !got.Status.Placement.Plan.PauseSurge {
					t.Fatalf("the change released the pause: %s", describeAutoscaledAll(t, f, got))
				}
				return false
			})
			if condition := got.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || condition.Status == corev1.ConditionTrue {
				t.Fatalf("the change converged without the cleanup wait: %s", describeAutoscaledAll(t, f, got))
			}
		})
	}
}

// TestSplitLowerMinimumsEndAnAcceptedPause starts from a lower minimum that the
// plan already accepted with a pause. Each member has applied its lower share
// and its autoscaler keeps more replicas than that, so the pause ends.
func TestSplitLowerMinimumsEndAnAcceptedPause(t *testing.T) {
	f := autoscaledSplitFixture(t)
	setAutoscaledAll(t, f, 6, 4, "")
	source := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	proposal, err := desiredSplitPlan(source, f.clusters, nil)
	if err != nil {
		t.Fatal(err)
	}
	if proposal.PauseSurge = splitPauseRequired(proposal); !proposal.PauseSurge {
		t.Fatal("the lower minimum did not pause growth")
	}
	store := plan.Store{Client: f.reconciler.Client, Reader: f.reconciler.APIReader}
	if _, err := store.Persist(t.Context(), source, proposal); err != nil {
		t.Fatal(err)
	}
	got := serveAutoscaledSplit(t, f, autoscaledAllRunning, autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("the accepted lower minimum stayed paused: %s", describeAutoscaledAll(t, f, got))
	}
}

// TestSplitPauseWaitsForADecoderReplacement lowers the engine minimum while new
// weights move a decoder from member-b to member-a. member-b keeps its old
// decoder until member-a's new one serves, so the pause holds until then and
// ends after it, with the autoscalers still above the shares.
func TestSplitPauseWaitsForADecoderReplacement(t *testing.T) {
	f := autoscaledSplitFixture(t)
	source := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	source.Spec.Engine.MinReplicas = ptr.To(10)
	source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 3, "member-b": 2})
	source.Spec.Placement.Split = nil
	if err := f.reconciler.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	serve := func(decoders map[string]int32, busy bool) *v1beta1.InferenceService {
		var got *v1beta1.InferenceService
		for range 8 {
			for _, name := range []string{"member-a", "member-b"} {
				member, present := allMemberOn(t, f, name)
				if !present {
					t.Fatalf("%s lost its member", name)
				}
				requested := memberMinimums(member)
				if busy {
					requested = autoscaledAllBusy
				}
				running := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: int32(*member.Spec.Engine.MinReplicas), v1beta1.DecoderComponent: decoders[name], v1beta1.RouterComponent: 2}
				runAutoscaledSplitMember(t, f, name, running, requested)
			}
			routeAutoscaledAll(t, f)
			got = f.reconcile(t)
		}
		return got
	}
	held := serve(map[string]int32{"member-a": 4, "member-b": 4}, false)
	holds, err := splitSecondaryHolds(held, f.reconciler.observeSplitMembers(t.Context(), held, []string{"member-a", "member-b"}))
	if err != nil {
		t.Fatal(err)
	}
	if holds["member-b"][v1beta1.DecoderComponent] != 4 {
		t.Fatalf("member-b does not keep its old decoder for the replacement: %v; %s", holds, describeAutoscaledAll(t, f, held))
	}
	if !held.Status.Placement.Plan.PauseSurge {
		t.Fatalf("growth resumed while member-b keeps an old decoder for member-a's replacement: %s", describeAutoscaledAll(t, f, held))
	}
	got := serve(map[string]int32{"member-a": 5, "member-b": 4}, true)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("the served replacement did not end the pause: %s", describeAutoscaledAll(t, f, got))
	}
}

// TestSplitPauseWaitsForUnequalDecoderShares: at weights 2:1, lowering the
// fleet minimums from 6 engines and 30 decoders to 4 and 29 lowers the engine
// shares from 4/2 to 3/1, but member-a's decoder share rises while
// member-b's falls. member-b keeps its old decoders until member-a's new ones
// serve, so the pause holds until then and ends after it.
func TestSplitPauseWaitsForUnequalDecoderShares(t *testing.T) {
	f := singleFixture(t, true)
	f.source.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	f.source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
	f.source.Spec.Engine.MinReplicas, f.source.Spec.Engine.MaxReplicas = ptr.To(6), 36
	f.source.Spec.Decoder.MinReplicas, f.source.Spec.Decoder.MaxReplicas = ptr.To(30), 180
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	serve := func(decoders map[string]int32) *v1beta1.InferenceService {
		var got *v1beta1.InferenceService
		for range 8 {
			for _, name := range []string{"member-a", "member-b"} {
				member, present := allMemberOn(t, f, name)
				if !present {
					t.Fatalf("%s lost its member", name)
				}
				requested := memberMinimums(member)
				running := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: requested[v1beta1.EngineComponent], v1beta1.DecoderComponent: requested[v1beta1.DecoderComponent]}
				if n, ok := decoders[name]; ok {
					running[v1beta1.DecoderComponent] = n
					requested[v1beta1.DecoderComponent] = max(requested[v1beta1.DecoderComponent], n)
				}
				runAutoscaledSplitMember(t, f, name, running, requested)
			}
			routeAutoscaledAll(t, f)
			got = f.reconcile(t)
		}
		return got
	}
	if settled := serve(nil); !autoscaledAllConverged(settled) || settled.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the service did not settle on its first shares: %s", describeAutoscaledAll(t, f, settled))
	}
	before := map[string]int32{}
	for _, name := range []string{"member-a", "member-b"} {
		member, _ := allMemberOn(t, f, name)
		before[name] = memberMinimums(member)[v1beta1.DecoderComponent]
	}
	source := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	source.Spec.Engine.MinReplicas, source.Spec.Decoder.MinReplicas = ptr.To(4), ptr.To(29)
	if err := f.reconciler.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	held := serve(before)
	a, _ := allMemberOn(t, f, "member-a")
	b, _ := allMemberOn(t, f, "member-b")
	gained := memberMinimums(a)[v1beta1.DecoderComponent]
	if gained <= before["member-a"] || memberMinimums(b)[v1beta1.DecoderComponent] >= before["member-b"] {
		t.Fatalf("the decoder shares did not move from member-b to member-a: before %v, now %d/%d", before, gained, memberMinimums(b)[v1beta1.DecoderComponent])
	}
	if !held.Status.Placement.Plan.PauseSurge {
		t.Fatalf("growth resumed before member-a's new decoders served: %s", describeAutoscaledAll(t, f, held))
	}
	got := serve(map[string]int32{"member-a": gained, "member-b": before["member-b"]})
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("the served decoders did not end the pause: %s", describeAutoscaledAll(t, f, got))
	}
}

// TestSplitPauseWaitsForAnIneligibleMember repeats the unequal-weight change
// while member-a's cluster is not ready. member-a keeps its old bounds, so its
// spec cannot show that it serves its new decoder minimum, and the pause holds.
func TestSplitPauseWaitsForAnIneligibleMember(t *testing.T) {
	f := singleFixture(t, true)
	f.source.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	f.source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
	f.source.Spec.Engine.MinReplicas, f.source.Spec.Engine.MaxReplicas = ptr.To(6), 36
	f.source.Spec.Decoder.MinReplicas, f.source.Spec.Decoder.MaxReplicas = ptr.To(30), 180
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	var got *v1beta1.InferenceService
	for range 8 {
		for _, name := range []string{"member-a", "member-b"} {
			member, present := allMemberOn(t, f, name)
			if !present {
				t.Fatalf("%s lost its member", name)
			}
			counts := memberMinimums(member)
			runAutoscaledSplitMember(t, f, name, counts, counts)
		}
		routeAutoscaledAll(t, f)
		got = f.reconcile(t)
	}
	if !autoscaledAllConverged(got) || got.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the service did not settle on its first shares: %s", describeAutoscaledAll(t, f, got))
	}
	cluster := &v1beta1.WorkloadCluster{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKey{Name: "member-a"}, cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Status.Conditions = nil
	if err := f.reconciler.Update(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	source := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	source.Spec.Engine.MinReplicas, source.Spec.Decoder.MinReplicas = ptr.To(4), ptr.To(29)
	if err := f.reconciler.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	for range 12 {
		for _, name := range []string{"member-a", "member-b"} {
			running := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4, v1beta1.DecoderComponent: 20}
			requested := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 6, v1beta1.DecoderComponent: 30}
			if name == "member-b" {
				running = map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 2, v1beta1.DecoderComponent: 10}
				requested = map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 3, v1beta1.DecoderComponent: 15}
			}
			runAutoscaledSplitMember(t, f, name, running, requested)
		}
		routeAutoscaledAll(t, f)
		got = f.reconcile(t)
	}
	observed := f.reconciler.observeSplitMembers(t.Context(), got, []string{"member-b"})
	if a := observed["member-a"]; a.MemberState.Eligible || memberMinimums(a.Member)[v1beta1.DecoderComponent] != 20 || a.ComponentReady[v1beta1.DecoderComponent] != 20 {
		t.Fatalf("member-a is not an ineligible member on its old bounds: eligible=%t minimums=%v ready=%v", a.MemberState.Eligible, memberMinimums(a.Member), a.ComponentReady)
	}
	if !got.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the pause ended while member-a, not eligible, keeps its old decoder minimum: %s", describeAutoscaledAll(t, f, got))
	}
}

// TestSplitLowerMinimumKeepsServingCapacity lowers the minimum of a fixed-count
// Split service while one member serves nothing. The move keeps the healthy
// member at its old share until the other one serves, so the service never
// runs below the new minimum.
func TestSplitLowerMinimumKeepsServingCapacity(t *testing.T) {
	f := singleFixture(t, false)
	f.source.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	f.source.Spec.Placement.Split = &v1beta1.SplitSpec{}
	f.source.Spec.Engine.MinReplicas, f.source.Spec.Engine.MaxReplicas = ptr.To(12), 12
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	six := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 6}
	if got := serveAutoscaledSplit(t, f, six, six, autoscaledAllConverged); !autoscaledAllConverged(got) || got.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the service did not settle on its first shares: %s", describeAutoscaledAll(t, f, got))
	}
	source := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), client.ObjectKeyFromObject(f.source), source); err != nil {
		t.Fatal(err)
	}
	source.Spec.Engine.MinReplicas = ptr.To(6)
	if err := f.reconciler.Update(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	var got *v1beta1.InferenceService
	for range 4 {
		for _, name := range []string{"member-a", "member-b"} {
			member, present := allMemberOn(t, f, name)
			if !present {
				t.Fatalf("%s lost its member", name)
			}
			requested := memberMinimums(member)
			running := requested
			if name == "member-b" {
				running = map[v1beta1.ComponentType]int32{}
			}
			runAutoscaledSplitMember(t, f, name, running, requested)
		}
		routeAutoscaledAll(t, f)
		got = f.reconcile(t)
	}
	healthy, _ := allMemberOn(t, f, "member-a")
	if *healthy.Spec.Engine.MinReplicas < 6 {
		t.Fatalf("the healthy member fell to %d while the other member serves nothing: %s", *healthy.Spec.Engine.MinReplicas, describeAutoscaledAll(t, f, got))
	}
}

// withRatioHeadroom drops the split block, which weights replace, and gives
// the engine a maxReplicas of 36, so each of two members may run up to 18
// engines, the per-member headroom engineCap(18) gives an unweighted source.
func withRatioHeadroom(s *v1beta1.InferenceService) {
	s.Spec.Placement.Split = nil
	s.Spec.Engine.MaxReplicas = 36
}

// autoscaledSplitFixture converges a Split service with an engine, a decoder
// and a router over member-a and member-b at equal weights: fleet minimums of
// 12 engines and 8 decoders give each member 6 and 4, plus 2 routers. A local
// ceiling of 18 engines lets each member's autoscaler scale above its share.
func autoscaledSplitFixture(t *testing.T) *backendFixture {
	t.Helper()
	return autoscaledSplitFixtureWith(t, &v1beta1.SplitSpec{MaxPerCluster: engineCap(18)})
}

// autoscaledSplitFixtureWith is autoscaledSplitFixture with the given split
// block, which must let each member's autoscaler scale above its share.
func autoscaledSplitFixtureWith(t *testing.T, split *v1beta1.SplitSpec) *backendFixture {
	t.Helper()
	f := singleFixture(t, true)
	f.source.Spec.Placement.Mode = v1beta1.PlacementModeSplit
	f.source.Spec.Placement.Split = split
	f.source.Spec.Engine.MinReplicas, f.source.Spec.Engine.MaxReplicas = ptr.To(12), 36
	f.source.Spec.Decoder.MinReplicas, f.source.Spec.Decoder.MaxReplicas = ptr.To(8), 24
	f.source.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(2), MaxReplicas: 2}, Runner: f.source.Spec.Engine.Runner.DeepCopy()}
	if err := f.reconciler.Update(t.Context(), f.source); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if settled := serveAutoscaledSplit(t, f, autoscaledAllRunning, autoscaledAllRunning, autoscaledAllConverged); !autoscaledAllConverged(settled) || settled.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the service did not settle on its first shares: %s", describeAutoscaledAll(t, f, settled))
	}
	return f
}

// serveAutoscaledSplit is serveAutoscaledAll with each member run by
// runAutoscaledSplitMember.
func serveAutoscaledSplit(t *testing.T, f *backendFixture, running, requested map[v1beta1.ComponentType]int32, stop func(*v1beta1.InferenceService) bool) *v1beta1.InferenceService {
	t.Helper()
	return serveSplitMembers(t, f, func(string) map[v1beta1.ComponentType]int32 { return running }, requested, stop)
}

// serveSplitMembers is serveAutoscaledSplit with each member running
// running(name) while it asks for requested.
func serveSplitMembers(t *testing.T, f *backendFixture, running func(string) map[v1beta1.ComponentType]int32, requested map[v1beta1.ComponentType]int32, stop func(*v1beta1.InferenceService) bool) *v1beta1.InferenceService {
	t.Helper()
	var got *v1beta1.InferenceService
	for range 12 {
		for _, name := range slices.Sorted(maps.Keys(f.workers)) {
			if _, ok := allMemberOn(t, f, name); ok {
				runAutoscaledSplitMember(t, f, name, running(name), requested)
				continue
			}
			for _, kind := range []client.Object{&v1beta1.InferenceReplica{}, &corev1.Pod{}} {
				if err := f.workers[name].DeleteAllOf(t.Context(), kind, client.InNamespace(f.source.Namespace)); err != nil {
					t.Fatal(err)
				}
			}
		}
		routeAutoscaledAll(t, f)
		if got = f.reconcile(t); stop(got) {
			break
		}
	}
	return got
}

// runAutoscaledSplitMember is runAutoscaledAllMember for a Split member, whose
// minimums are in its spec rather than in its execution policy: each component
// runs running[c] while asking for requested[c], and a paused component's limit
// follows the projector from the member's minimums.
func runAutoscaledSplitMember(t *testing.T, f *backendFixture, name string, running, requested map[v1beta1.ComponentType]int32) {
	t.Helper()
	worker := f.workers[name]
	member, _ := allMemberOn(t, f, name)
	policy, err := protocol.FromDerived(member)
	if err != nil || policy == nil {
		t.Fatalf("%s has no member execution policy: %v", name, err)
	}
	minimums := memberMinimums(member)
	for _, component := range declaredComponents(f.source) {
		ir := sizeAutoscaledAllComponent(t, worker, member, component, running[component])
		wasPaused, limit := ir.Spec.PlacementExecution != nil && ir.Spec.PlacementExecution.PauseSurge, ir.Spec.PlacementReplicaLimit
		ir.Generation++
		ir.Spec.Replicas, ir.Spec.PlacementExecution, ir.Spec.PlacementReplicaLimit = ptr.To(max(minimums[component], requested[component])), policy.DeepCopy(), nil
		if policy.PauseSurge {
			next := max(minimums[component], requested[component])
			if wasPaused && limit != nil {
				next = max(minimums[component], min(*limit, requested[component]))
			}
			ir.Spec.PlacementReplicaLimit = ptr.To(next)
		}
		if ir.Annotations == nil {
			ir.Annotations = map[string]string{}
		}
		ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = strconv.FormatInt(member.Generation, 10)
		ir.Status.ObservedGeneration, ir.Status.PlacementObservedGeneration = ir.Generation, ir.Generation
		if err := worker.Update(t.Context(), ir); err != nil {
			t.Fatal(err)
		}
	}
	member.Status.URL = &apis.URL{Scheme: "https", Host: name + ".example.com"}
	member.Status.SetCondition(v1beta1.IngressReady, &apis.Condition{Status: corev1.ConditionTrue})
	if err := worker.Status().Update(t.Context(), member); err != nil {
		t.Fatal(err)
	}
}
