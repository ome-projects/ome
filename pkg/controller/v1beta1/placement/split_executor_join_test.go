package placement

import (
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// twoRoutersEach gives every member two routers. Split divides the router
// minimum across the members, so the minimum is two times their count.
func twoRoutersEach(s *v1beta1.InferenceService, members int) {
	s.Spec.Router.MinReplicas = ptr.To(2 * members)
}

// TestSplitJoinEndsThePauseOnceTheMemberServes adds member-c to the autoscaled
// Split service while the autoscalers on member-a and member-b keep more
// replicas than their shares. The pause stays on while member-c serves
// nothing, and ends once it serves its share.
func TestSplitJoinEndsThePauseOnceTheMemberServes(t *testing.T) {
	f := autoscaledSplitFixture(t)
	registerAutoscaledAllMember(t, f, "member-c")
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(18), ptr.To(12)
		s.Spec.Placement.ClusterAffinity = testAffinity("")
		twoRoutersEach(s, 3)
	})
	running := func(ready bool) func(string) map[v1beta1.ComponentType]int32 {
		return func(name string) map[v1beta1.ComponentType]int32 {
			if name == "member-c" && !ready {
				return map[v1beta1.ComponentType]int32{}
			}
			return autoscaledAllBusy
		}
	}
	held := serveSplitMembers(t, f, running(false), autoscaledAllBusy, func(got *v1beta1.InferenceService) bool {
		if !got.Status.Placement.Plan.PauseSurge {
			t.Fatalf("the pause ended while member-c serves nothing: %s", describeAutoscaledAll(t, f, got))
		}
		return false
	})
	if autoscaledAllConverged(held) {
		t.Fatalf("the join converged while member-c serves nothing: %s", describeAutoscaledAll(t, f, held))
	}
	got := serveSplitMembers(t, f, running(true), autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member-c serves its share but growth stayed paused: %s", describeAutoscaledAll(t, f, got))
	}
	if a := candidateOf(t, got, "member-c").Allocation; a.CurrentReplicas != 6 || a.DesiredReplicas != 6 {
		t.Fatalf("member-c did not take its share: %+v", a)
	}
}

// TestSplitJoinWaitsForARecoveredMemberOnOldBounds adds member-c and raises
// the decoder minimum while member-a is not eligible, so its decoder share
// rises from 4 to 6 while its spec keeps 4. Once member-a is eligible again
// with 4 ready decoders, the pause stays on.
func TestSplitJoinWaitsForARecoveredMemberOnOldBounds(t *testing.T) {
	f := autoscaledSplitFixture(t)
	setClusterReady(t, f, "member-a", false)
	registerAutoscaledAllMember(t, f, "member-c")
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(18), ptr.To(18)
		s.Spec.Placement.ClusterAffinity = testAffinity("")
		twoRoutersEach(s, 3)
	})
	running := func(name string) map[v1beta1.ComponentType]int32 {
		if name == "member-a" {
			return autoscaledAllRunning
		}
		return autoscaledAllBusy
	}
	held := serveSplitMembers(t, f, running, autoscaledAllBusy, func(*v1beta1.InferenceService) bool { return false })
	if !held.Status.Placement.Plan.PauseSurge {
		t.Fatalf("growth resumed while member-a was not eligible: %s", describeAutoscaledAll(t, f, held))
	}
	observed := f.reconciler.observeSplitMembers(t.Context(), held, []string{"member-b", "member-c"})
	if old := observed["member-a"]; old.MemberState.Eligible || old.Member == nil || memberMinimums(old.Member)[v1beta1.DecoderComponent] != 4 || old.ComponentReady[v1beta1.DecoderComponent] != 4 {
		t.Fatalf("member-a is not an ineligible member on its old bounds: state=%+v ready=%v", old.MemberState, old.ComponentReady)
	}
	setClusterReady(t, f, "member-a", true)
	if got := f.reconcile(t); !got.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the pause ended while member-a serves 4 decoders against its new share of 6: %s", describeAutoscaledAll(t, f, got))
	}
}

// TestSplitJoinEndsAtTheJoinersExactShare adds member-c while the autoscalers
// on member-a and member-b keep 9 engines and 6 decoders. member-c serves
// exactly its share of 6 engines and 4 decoders, and the pause ends.
func TestSplitJoinEndsAtTheJoinersExactShare(t *testing.T) {
	f := autoscaledSplitFixture(t)
	registerAutoscaledAllMember(t, f, "member-c")
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(18), ptr.To(12)
		s.Spec.Placement.ClusterAffinity = testAffinity("")
		twoRoutersEach(s, 3)
	})
	running := func(name string) map[v1beta1.ComponentType]int32 {
		if name == "member-c" {
			return autoscaledAllRunning
		}
		return autoscaledAllBusy
	}
	got := serveSplitMembers(t, f, running, autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member-c serves exactly its share but growth stayed paused: %s", describeAutoscaledAll(t, f, got))
	}
}

// TestSplitJoinFundedByFallingSharesEndsThePause adds member-c without a new
// minimum, so the engine shares of member-a and member-b fall from 6 to 4.
// member-c needs 3 decoders while the move runs and keeps 2 after it, and the
// pause ends once it serves 4 engines and 3 decoders.
func TestSplitJoinFundedByFallingSharesEndsThePause(t *testing.T) {
	f := autoscaledSplitFixture(t)
	registerAutoscaledAllMember(t, f, "member-c")
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Placement.ClusterAffinity = testAffinity("")
		twoRoutersEach(s, 3)
	})
	running := func(ready bool) func(string) map[v1beta1.ComponentType]int32 {
		return func(name string) map[v1beta1.ComponentType]int32 {
			switch {
			case name != "member-c":
				return autoscaledAllBusy
			case !ready:
				return map[v1beta1.ComponentType]int32{}
			}
			return map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4, v1beta1.DecoderComponent: 3, v1beta1.RouterComponent: 2}
		}
	}
	held := serveSplitMembers(t, f, running(false), autoscaledAllBusy, func(*v1beta1.InferenceService) bool { return false })
	if !held.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the pause ended while member-c serves nothing: %s", describeAutoscaledAll(t, f, held))
	}
	got := serveSplitMembers(t, f, running(true), autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member-c serves its share but growth stayed paused: %s", describeAutoscaledAll(t, f, got))
	}
	for _, candidate := range got.Status.Placement.Candidates {
		if a := candidate.Allocation; a.CurrentReplicas != 4 || a.DesiredReplicas != 4 {
			t.Fatalf("%s did not take an engine share of 4: %+v", candidate.Cluster, a)
		}
	}
	if member, _ := allMemberOn(t, f, "member-c"); *member.Spec.Decoder.MinReplicas != 2 {
		t.Fatalf("member-c kept a decoder minimum of %d after the move, not 2", *member.Spec.Decoder.MinReplicas)
	}
}

// TestSplitJoinWaitsForEveryRaisedDecoder adds member-c and raises the decoder
// minimum to 18, so the decoder share of every member is 6. The pause stays
// on while member-c serves 5 decoders, and ends at 6.
func TestSplitJoinWaitsForEveryRaisedDecoder(t *testing.T) {
	f := autoscaledSplitFixture(t)
	registerAutoscaledAllMember(t, f, "member-c")
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(18), ptr.To(18)
		s.Spec.Placement.ClusterAffinity = testAffinity("")
		twoRoutersEach(s, 3)
	})
	running := func(decoders int32) func(string) map[v1beta1.ComponentType]int32 {
		return func(name string) map[v1beta1.ComponentType]int32 {
			if name == "member-c" {
				return map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 6, v1beta1.DecoderComponent: decoders, v1beta1.RouterComponent: 2}
			}
			return autoscaledAllBusy
		}
	}
	held := serveSplitMembers(t, f, running(5), autoscaledAllBusy, func(*v1beta1.InferenceService) bool { return false })
	if !held.Status.Placement.Plan.PauseSurge {
		t.Fatalf("the pause ended while member-c serves 5 decoders against a share of 6: %s", describeAutoscaledAll(t, f, held))
	}
	got := serveSplitMembers(t, f, running(6), autoscaledAllBusy, autoscaledAllConverged)
	if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
		t.Fatalf("member-c serves 6 decoders but growth stayed paused: %s", describeAutoscaledAll(t, f, got))
	}
}
