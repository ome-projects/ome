package placement

import (
	"maps"
	"slices"
	"testing"

	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestSplitShrink_MembersAtTheirTarget_StartNoDecoder runs the autoscaled Split
// service on member-a, member-b and member-c with 4 engines, 2 decoders and 2
// routers each, from minimums of 12 engines and 6 decoders. The change lowers
// member-b's engine weight to 3 and the engine minimum to 11. No member starts
// an instance during the move. member-a and member-c keep a decoder minimum of
// 2 throughout, and member-b's engine target falls to 3.
func TestSplitShrink_MembersAtTheirTarget_StartNoDecoder(t *testing.T) {
	running := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 4, v1beta1.DecoderComponent: 2, v1beta1.RouterComponent: 2}
	for _, tt := range []struct {
		name    string
		decoder map[string]int32
	}{
		{name: "decoders follow the engines"},
		{name: "decoder weights", decoder: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := autoscaledSplitFixture(t)
			registerAutoscaledAllMember(t, f, "member-c")
			changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
				s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(12), ptr.To(6)
				s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 4, "member-b": 4, "member-c": 4}, tt.decoder, nil)
			})
			settled := serveAutoscaledSplit(t, f, running, running, func(got *v1beta1.InferenceService) bool {
				return !got.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(got)
			})
			if settled.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(settled) {
				t.Fatalf("the service did not settle on three members: %s", describeWeightedSplit(t, f, settled))
			}
			changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
				s.Spec.Engine.MinReplicas = ptr.To(11)
				s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 4, "member-b": 3, "member-c": 4}, tt.decoder, nil)
			})
			raised := ""
			got := serveAutoscaledSplit(t, f, running, running, func(got *v1beta1.InferenceService) bool {
				for _, name := range []string{"member-a", "member-c"} {
					member, _ := allMemberOn(t, f, name)
					if decoders := memberMinimums(member)[v1beta1.DecoderComponent]; decoders != 2 && raised == "" {
						raised = describeWeightedSplit(t, f, got)
					}
				}
				return !got.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(got)
			})
			if raised != "" {
				t.Errorf("a member at its engine target got a decoder minimum other than 2 during the move: %s", raised)
			}
			for _, candidate := range got.Status.Placement.Candidates {
				if candidate.Cluster == "member-b" && candidate.Allocation.CurrentReplicas != 3 {
					t.Fatalf("member-b's engine target stayed at %d: %s", candidate.Allocation.CurrentReplicas, describeWeightedSplit(t, f, got))
				}
			}
			if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
				t.Fatalf("the move did not finish: %s", describeWeightedSplit(t, f, got))
			}
		})
	}
}

// TestSplitJoin_DecoderWeights_JoinerGetsItsEngines adds member-c to the
// autoscaled Split service with engine weights 2, 1 and 1 and decoder weights
// that favor member-c. The engines go from 6/6 to 6/3/3. member-a reaches its
// engine target at once and ends with fewer decoders, but it keeps the 4 it
// holds while member-c has no engines yet. Its running decoders therefore do
// not count as extra engines, and member-c receives its 3 engines.
func TestSplitJoin_DecoderWeights_JoinerGetsItsEngines(t *testing.T) {
	idle := map[v1beta1.ComponentType]int32{v1beta1.RouterComponent: 2}
	for _, tt := range []struct {
		name      string
		decoder   map[string]int32
		running   func(*backendFixture) func(string) map[v1beta1.ComponentType]int32
		requested map[v1beta1.ComponentType]int32
	}{
		// Every autoscaler runs and asks for 9 engines and 6 decoders.
		{name: "busy autoscalers", decoder: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 2},
			running: func(*backendFixture) func(string) map[v1beta1.ComponentType]int32 {
				return func(string) map[v1beta1.ComponentType]int32 { return autoscaledAllBusy }
			}, requested: autoscaledAllBusy},
		// Every member runs exactly its minimums.
		{name: "members run their minimums", decoder: map[string]int32{"member-a": 1, "member-b": 1, "member-c": 6},
			running: func(f *backendFixture) func(string) map[v1beta1.ComponentType]int32 {
				return func(name string) map[v1beta1.ComponentType]int32 {
					member, _ := allMemberOn(t, f, name)
					minimums := memberMinimums(member)
					return map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: minimums[v1beta1.EngineComponent], v1beta1.DecoderComponent: minimums[v1beta1.DecoderComponent], v1beta1.RouterComponent: 2}
				}
			}, requested: idle},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := autoscaledSplitFixture(t)
			registerAutoscaledAllMember(t, f, "member-c")
			changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
				s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 2, "member-b": 1, "member-c": 1}, tt.decoder, nil)
			})
			var got *v1beta1.InferenceService
			for range 3 {
				got = serveSplitMembers(t, f, tt.running(f), tt.requested, func(got *v1beta1.InferenceService) bool {
					return !got.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(got)
				})
			}
			if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
				t.Fatalf("member-c never received its engines: %s", describeWeightedSplit(t, f, got))
			}
		})
	}
}

// TestSplitJoin_MembersReachTheirTargets_MoveFinishes adds member-c to the
// autoscaled Split service with engine weights 2, 1 and 1, so the engines go
// from 6/6 to 6/3/3.
//   - With the decoder minimum raised to 12 and decoder weights 1/1/4, member-a
//     and member-b run and ask for 9 engines and 9 decoders. Their decoders
//     still count against the shares the move planned, so member-c receives
//     its engines.
//   - With decoder weights 1/1/2, member-c reaches its 3 engines while
//     member-b still runs 6, and 15 engines in flight carry 10 decoders. member-c
//     takes its final 4 rather than 5 of them, so member-b's reduction does not
//     wait for a fifth decoder that member-c never starts.
func TestSplitJoin_MembersReachTheirTargets_MoveFinishes(t *testing.T) {
	busy := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 9, v1beta1.DecoderComponent: 9, v1beta1.RouterComponent: 2}
	for _, tt := range []struct {
		name     string
		decoders int
		weightC  int32
		// running returns what a member runs; busy members also ask for it.
		running func(name string, minimums map[v1beta1.ComponentType]int32) (running map[v1beta1.ComponentType]int32, busy bool)
	}{
		{name: "raised decoder minimum with busy autoscalers", decoders: 12, weightC: 4,
			running: func(name string, minimums map[v1beta1.ComponentType]int32) (map[v1beta1.ComponentType]int32, bool) {
				if name == "member-c" {
					return minimums, false
				}
				return busy, true
			}},
		{name: "member-c never starts more than 4 decoders", decoders: 8, weightC: 2,
			running: func(name string, minimums map[v1beta1.ComponentType]int32) (map[v1beta1.ComponentType]int32, bool) {
				running := maps.Clone(minimums)
				if name == "member-c" {
					running[v1beta1.DecoderComponent] = min(running[v1beta1.DecoderComponent], 4)
				}
				return running, false
			}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := autoscaledSplitFixture(t)
			registerAutoscaledAllMember(t, f, "member-c")
			changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
				s.Spec.Decoder.MinReplicas = ptr.To(tt.decoders)
				s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 2, "member-b": 1, "member-c": 1}, map[string]int32{"member-a": 1, "member-b": 1, "member-c": tt.weightC}, nil)
			})
			var got *v1beta1.InferenceService
			for range 24 {
				for _, name := range slices.Sorted(maps.Keys(f.workers)) {
					if member, ok := allMemberOn(t, f, name); ok {
						running, busy := tt.running(name, memberMinimums(member))
						var requested map[v1beta1.ComponentType]int32
						if busy {
							requested = running
						}
						runAutoscaledSplitMember(t, f, name, running, requested)
					}
				}
				routeAutoscaledAll(t, f)
				if got = f.reconcile(t); !got.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(got) {
					break
				}
			}
			if got.Status.Placement.Plan.PauseSurge || !autoscaledAllConverged(got) {
				t.Fatalf("the move did not finish: %s", describeWeightedSplit(t, f, got))
			}
		})
	}
}

// TestSplitShrink_InTwoSteps_MembersAtTheirTargetStartNoDecoder runs 3, 3 and
// 4 engines on member-a, member-b and member-c from minimums of 10 engines and
// 4 decoders, then lowers member-c's weight to 1 and the engine minimum to 7.
// member-b runs one engine short for the first two passes, so member-c's
// reduction lands in two steps. While member-c still holds extra engines,
// member-a and member-b get no decoder they did not hold, and member-a never
// starts one, so the move finishes.
func TestSplitShrink_InTwoSteps_MembersAtTheirTargetStartNoDecoder(t *testing.T) {
	f := autoscaledSplitFixture(t)
	registerAutoscaledAllMember(t, f, "member-c")
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas, s.Spec.Decoder.MinReplicas = ptr.To(10), ptr.To(4)
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 3, "member-b": 3, "member-c": 4}, nil, nil)
	})
	pass := func(run func(name string, minimums map[v1beta1.ComponentType]int32) map[v1beta1.ComponentType]int32) *v1beta1.InferenceService {
		for _, name := range slices.Sorted(maps.Keys(f.workers)) {
			if member, ok := allMemberOn(t, f, name); ok {
				runAutoscaledSplitMember(t, f, name, run(name, memberMinimums(member)), nil)
			}
		}
		routeAutoscaledAll(t, f)
		return f.reconcile(t)
	}
	done := func(source *v1beta1.InferenceService) bool {
		return !source.Status.Placement.Plan.PauseSurge && autoscaledAllConverged(source)
	}
	var got *v1beta1.InferenceService
	for range 24 {
		if got = pass(func(_ string, minimums map[v1beta1.ComponentType]int32) map[v1beta1.ComponentType]int32 {
			minimums[v1beta1.RouterComponent] = 2
			return minimums
		}); done(got) {
			break
		}
	}
	if !done(got) {
		t.Fatalf("the service did not settle on three members: %s", describeWeightedSplit(t, f, got))
	}
	held, _ := allMemberOn(t, f, "member-a")
	heldDecoders := memberMinimums(held)[v1beta1.DecoderComponent]
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		s.Spec.Engine.MinReplicas = ptr.To(7)
		s.Spec.Placement.ClusterAffinity = perMemberTerms(map[string]int32{"member-a": 3, "member-b": 3, "member-c": 1}, nil, nil)
	})
	for i := range 30 {
		got = pass(func(name string, minimums map[v1beta1.ComponentType]int32) map[v1beta1.ComponentType]int32 {
			running := maps.Clone(minimums)
			running[v1beta1.RouterComponent] = 2
			switch name {
			case "member-a":
				running[v1beta1.DecoderComponent] = min(running[v1beta1.DecoderComponent], heldDecoders)
			case "member-b":
				if i < 2 {
					running[v1beta1.EngineComponent]--
				}
			}
			return running
		})
		if done(got) {
			return
		}
	}
	t.Fatalf("the move did not finish: %s", describeWeightedSplit(t, f, got))
}
