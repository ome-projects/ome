package placement

import (
	"testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestSplitLeave_DecoderWeightsDuringAJoin_JoinerGetsItsEngines replaces
// member-b with member-c on the autoscaled Split service in one change, with
// decoder weights 1 and 2 on member-a and member-c. member-b no longer matches,
// so it has no decoder weight and its share falls to the one decoder every
// member with engines is guaranteed. Its running decoders are counted against
// the 4 its copy holds instead, so they do not count as 24 or more engines.
// member-c receives its 6 engines and member-b drains. With busy autoscalers the
// pause then waits for them to come down, because a member left.
func TestSplitLeave_DecoderWeightsDuringAJoin_JoinerGetsItsEngines(t *testing.T) {
	for _, tt := range []struct {
		name      string
		running   func(*backendFixture) func(string) map[v1beta1.ComponentType]int32
		requested map[v1beta1.ComponentType]int32
		converges bool
	}{
		{name: "members run their minimums", running: func(f *backendFixture) func(string) map[v1beta1.ComponentType]int32 {
			return func(name string) map[v1beta1.ComponentType]int32 {
				member, _ := allMemberOn(t, f, name)
				minimums := memberMinimums(member)
				return map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: minimums[v1beta1.EngineComponent], v1beta1.DecoderComponent: minimums[v1beta1.DecoderComponent], v1beta1.RouterComponent: 2}
			}
		}, requested: map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 2}, converges: true},
		{name: "busy autoscalers", running: func(*backendFixture) func(string) map[v1beta1.ComponentType]int32 {
			return func(string) map[v1beta1.ComponentType]int32 { return autoscaledAllBusy }
		}, requested: autoscaledAllBusy},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := autoscaledSplitFixture(t)
			registerAutoscaledAllMember(t, f, "member-c")
			changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
				s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 1, "member-c": 1}, map[string]int32{"member-a": 1, "member-c": 2}, nil)
			})
			var got *v1beta1.InferenceService
			for range 3 {
				got = serveSplitMembers(t, f, tt.running(f), tt.requested, func(got *v1beta1.InferenceService) bool {
					return !got.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(got)
				})
			}
			for _, candidate := range got.Status.Placement.Candidates {
				want := map[string]int32{"member-a": 6, "member-b": 0, "member-c": 6}[candidate.Cluster]
				if candidate.Allocation.CurrentReplicas != want {
					t.Fatalf("%s ended with an engine target of %d, want %d: %s", candidate.Cluster, candidate.Allocation.CurrentReplicas, want, describeWeightedSplit(t, f, got))
				}
			}
			if tt.converges && (got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got)) {
				t.Fatalf("the move did not finish: %s", describeWeightedSplit(t, f, got))
			}
		})
	}
}

// TestSplitLeave_TwoMembersLeaveUnderLoad_MoveFinishes runs member-a, member-b
// and member-c with decoder weights 3, 1 and 1, then keeps only member-a while
// every autoscaler is busy. member-c's copy holds 1 decoder while its share
// that follows the engines is 2, so its running decoders are counted against 2,
// as before, and the move finishes.
func TestSplitLeave_TwoMembersLeaveUnderLoad_MoveFinishes(t *testing.T) {
	f := autoscaledSplitFixture(t)
	minimums := func(name string) map[v1beta1.ComponentType]int32 {
		member, _ := allMemberOn(t, f, name)
		return memberMinimums(member)
	}
	busy := func(name string) map[v1beta1.ComponentType]int32 {
		running := minimums(name)
		for component, count := range autoscaledAllBusy {
			running[component] = max(running[component], count)
		}
		return running
	}
	done := func(source *v1beta1.InferenceService) bool {
		return !source.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(source)
	}
	terms := func(engine, decoder map[string]int32) {
		changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
			s.Spec.Placement.ClusterAffinity = perMemberTerms(engine, decoder, nil)
		})
	}
	terms(map[string]int32{"member-a": 1, "member-b": 1}, map[string]int32{"member-a": 3, "member-b": 1})
	serveSplitMembers(t, f, minimums, nil, done)
	registerAutoscaledAllMember(t, f, "member-c")
	terms(map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1}, map[string]int32{"member-a": 3, "member-b": 1, "member-c": 1})
	serveSplitMembers(t, f, minimums, nil, done)
	terms(map[string]int32{"member-a": 1}, map[string]int32{"member-a": 3})
	var got *v1beta1.InferenceService
	for range 3 {
		got = serveSplitMembers(t, f, busy, autoscaledAllBusy, done)
	}
	if !done(got) {
		t.Fatalf("the move did not finish: %s", describeWeightedSplit(t, f, got))
	}
}
