package placement

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// holdsMember describes one split member of the secondary-hold tests: its
// allocation, the decoder minimum its copy carries, its ready decoders and
// how it was observed.
type holdsMember struct {
	current, desired int32
	decoders         int32
	ready            int32
	drain, full      bool
	unknown          bool
	unserving        bool
	noAllocation     bool
	noCopy           bool
	policyDecoders   int32
}

// holdsSource is a split source of 5 engines and 12 decoders over up to three
// members, apportioned by the engine targets alone.
func holdsSource() *v1beta1.InferenceService {
	source := ratioSplitSource(5, ptr.To(12))
	source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{ID: "plan-a"}}
	return source
}

// holdsWorld writes the members into the source's candidates and returns
// their observations, in name order.
func holdsWorld(t *testing.T, source *v1beta1.InferenceService, members map[string]holdsMember) map[string]plannedMemberObservation {
	t.Helper()
	observations := map[string]plannedMemberObservation{}
	for _, name := range []string{"member-a", "member-b", "member-c"} {
		m, present := members[name]
		if !present {
			continue
		}
		candidate := v1beta1.CandidatePlacement{Cluster: name}
		if !m.noAllocation {
			candidate.Allocation = &v1beta1.CandidateAllocationStatus{ClusterUID: types.UID("uid-" + name), CurrentReplicas: m.current, DesiredReplicas: m.desired, DrainRequested: m.drain}
			if m.full {
				candidate.Allocation.CurrentHome = &v1beta1.PlacementHomePolicy{InputDigest: "digest"}
			}
		}
		source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, candidate)
		observed := plannedMemberObservation{
			Candidate:      candidate,
			ComponentReady: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: m.current, v1beta1.DecoderComponent: m.ready},
			MemberState:    allocation.MemberState{Known: !m.unknown, Routable: !m.unserving},
		}
		if !m.noCopy {
			copied := ratioSplitSource(int(m.current), ptr.To(int(m.decoders)))
			copied.Spec.Placement = nil
			if m.policyDecoders > 0 {
				policy := &v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 1, SourceUID: source.UID, ClusterUID: types.UID("uid-" + name),
					ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: m.current}, {Component: v1beta1.DecoderComponent, Replicas: m.policyDecoders}}}
				raw, err := protocol.Encode(policy)
				if err != nil {
					t.Fatal(err)
				}
				copied.Labels = map[string]string{PlacementOriginLabel: string(source.UID)}
				copied.Annotations = map[string]string{constants.PlacementExecution: raw}
			}
			observed.Member = copied
		}
		observations[name] = observed
	}
	return observations
}

func decoderHolds(holds map[string]int32) map[string]map[v1beta1.ComponentType]int32 {
	out := map[string]map[v1beta1.ComponentType]int32{}
	for name, count := range holds {
		out[name] = map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: count}
	}
	return out
}

// TestSplitSecondaryHoldsNeedAResolvedPrimaryCount: a source whose primary
// count cannot be read gives the count's refusal and no holds.
func TestSplitSecondaryHoldsNeedAResolvedPrimaryCount(t *testing.T) {
	for _, tt := range []struct {
		name   string
		source *v1beta1.InferenceService
		want   string
	}{
		{name: "an engine minimum of zero", source: func() *v1beta1.InferenceService {
			source := ratioSplitSource(0, ptr.To(12))
			source.Spec.Placement.ClusterAffinity = weightedAffinity(map[string]int32{"member-a": 2, "member-b": 1})
			return source
		}(), want: "ReplicaCountUnresolved: declare a positive engine.minReplicas"},
		{name: "a split count of zero", source: func() *v1beta1.InferenceService {
			source := srcISVCSplit("accelerator=gpu-a", 0)
			source.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(12)}}
			return source
		}(), want: "ReplicaCountUnresolved: declare split.replicas or a positive engine.minReplicas"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{ID: "plan-a"}}
			observations := holdsWorld(t, tt.source, map[string]holdsMember{"member-a": {current: 3, desired: 3, decoders: 9, ready: 9}})
			got, err := splitSecondaryHolds(tt.source, observations)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("splitSecondaryHolds error = %v, want %q", err, tt.want)
			}
			if got != nil {
				t.Fatalf("holds = %v, want none", got)
			}
		})
	}
}

// TestSplitSecondaryHoldsCountOnlyMembersThatStandOnAShare: a member that
// runs no replicas, keeps a full policy (runs the whole service), is asked to
// drain, or has no allocation lends no ready decoders to a release, whatever
// it reports; the same members all standing release the hold.
func TestSplitSecondaryHoldsCountOnlyMembersThatStandOnAShare(t *testing.T) {
	for _, tt := range []struct {
		name    string
		members map[string]holdsMember
		want    map[string]int32
	}{
		{name: "a member running nothing", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 9, ready: 9},
			"member-b": {current: 0, desired: 0, decoders: 5, ready: 5},
			"member-c": {current: 2, desired: 2, decoders: 5, ready: 3},
		}, want: map[string]int32{"member-a": 9}},
		{name: "a member under a full policy", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 9, ready: 9},
			"member-b": {current: 2, desired: 2, decoders: 5, ready: 5, full: true},
		}, want: map[string]int32{"member-a": 9}},
		{name: "a member asked to drain", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 9, ready: 9},
			"member-b": {current: 2, desired: 2, decoders: 5, ready: 5, drain: true},
		}, want: map[string]int32{"member-a": 9}},
		{name: "a member without an allocation", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 9, ready: 9},
			"member-b": {noAllocation: true, decoders: 5, ready: 5},
		}, want: map[string]int32{"member-a": 9}},
		{name: "the same members all standing", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 9, ready: 9},
			"member-b": {current: 2, desired: 2, decoders: 5, ready: 5},
		}, want: map[string]int32{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := holdsSource()
			observations := holdsWorld(t, source, tt.members)
			got, err := splitSecondaryHolds(source, observations)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(decoderHolds(tt.want), got); diff != "" {
				t.Errorf("holds (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSplitSecondaryHoldsReadTheMemberAsObserved: ready decoders release a
// hold only from a member that serves; a member observed off rotation still
// sheds its unready decoders and lowers its ready ones against the fleet's
// spare; a member that was not observed keeps what its copy carries; the
// minimum a copy carries is read from its policy before its spec.
func TestSplitSecondaryHoldsReadTheMemberAsObserved(t *testing.T) {
	for _, tt := range []struct {
		name    string
		members map[string]holdsMember
		want    map[string]int32
	}{
		{name: "ready decoders on a member off rotation release nothing", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 9, ready: 9},
			"member-b": {current: 2, desired: 2, decoders: 5, ready: 5, unserving: true},
		}, want: map[string]int32{"member-a": 9}},
		{name: "an unobserved member keeps its copy's minimum", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 9, ready: 9, unknown: true},
			"member-b": {current: 2, desired: 2, decoders: 5, ready: 5},
		}, want: map[string]int32{"member-a": 9}},
		{name: "a member off rotation sheds its unready decoders at once", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 9, ready: 6, unserving: true},
			"member-b": {current: 2, desired: 2, decoders: 5, ready: 5},
		}, want: map[string]int32{}},
		{name: "a member off rotation lowers its ready decoders against the fleet's spare", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 9, ready: 9, unserving: true},
			"member-b": {current: 2, desired: 2, decoders: 5, ready: 14},
		}, want: map[string]int32{}},
		{name: "a member without a copy holds nothing", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, noCopy: true, ready: 9},
			"member-b": {current: 2, desired: 2, decoders: 5, ready: 3},
		}, want: map[string]int32{}},
		{name: "the copy's policy minimum counts before its spec", members: map[string]holdsMember{
			"member-a": {current: 3, desired: 3, decoders: 3, policyDecoders: 9, ready: 9},
			"member-b": {current: 2, desired: 2, decoders: 5, ready: 3},
		}, want: map[string]int32{"member-a": 9}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := holdsSource()
			observations := holdsWorld(t, source, tt.members)
			got, err := splitSecondaryHolds(source, observations)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(decoderHolds(tt.want), got); diff != "" {
				t.Errorf("holds (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSplitSecondaryHoldsFollowThePlacementMode: holds are computed for the
// two split modes and for no other; nothing is held outside them.
func TestSplitSecondaryHoldsFollowThePlacementMode(t *testing.T) {
	members := map[string]holdsMember{
		"member-a": {current: 3, desired: 3, decoders: 9, ready: 9},
		"member-b": {current: 2, desired: 2, decoders: 5, ready: 3},
	}
	for _, tt := range []struct {
		mode v1beta1.PlacementMode
		want map[string]map[v1beta1.ComponentType]int32
	}{
		{mode: v1beta1.PlacementModeSplit, want: decoderHolds(map[string]int32{"member-a": 9})},
		{mode: v1beta1.PlacementModeSplitByCapacity, want: decoderHolds(map[string]int32{"member-a": 9})},
		{mode: v1beta1.PlacementModeSingle, want: nil},
		{mode: "", want: nil},
	} {
		t.Run(string(tt.mode), func(t *testing.T) {
			source := holdsSource()
			source.Spec.Placement.Mode = tt.mode
			observations := holdsWorld(t, source, members)
			got, err := splitSecondaryHolds(source, observations)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("holds (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSplitSecondaryHoldsReportBothSecondariesOfAMember: a member keeping
// decoders and routers for replacements elsewhere reports both under its
// name.
func TestSplitSecondaryHoldsReportBothSecondariesOfAMember(t *testing.T) {
	source := dividedRouterSource(4, map[string]int32{"member-a": 3, "member-b": 1})
	proposal, err := desiredSplitPlan(source, twoMembers(), nil)
	if err != nil {
		t.Fatal(err)
	}
	source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{ID: "plan-a"}, Candidates: settledCandidates(proposal.Assignments)}
	// Both copies still carry the even split of 6 decoders and 2 routers while
	// the plan moves to 9 and 3 on member-a, 3 and 1 on member-b.
	member := func() *v1beta1.InferenceService {
		copied := source.DeepCopy()
		copied.Spec.Placement = nil
		copied.Spec.Decoder.MinReplicas = ptr.To(6)
		copied.Spec.Router.MinReplicas = ptr.To(2)
		return copied
	}
	ready := map[v1beta1.ComponentType]int32{v1beta1.DecoderComponent: 6, v1beta1.RouterComponent: 2}
	observations := map[string]plannedMemberObservation{
		"member-a": {Member: member(), ComponentReady: ready, MemberState: allocation.MemberState{Known: true, Routable: true}},
		"member-b": {Member: member(), ComponentReady: ready, MemberState: allocation.MemberState{Known: true, Routable: true}},
	}
	got, err := splitSecondaryHolds(source, observations)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]map[v1beta1.ComponentType]int32{"member-b": {v1beta1.DecoderComponent: 6, v1beta1.RouterComponent: 2}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("holds (-want +got):\n%s", diff)
	}
}

// TestSplitSecondaryHoldsRefuseACandidateWithoutAName: a candidate the plan
// cannot identify fails the apportionment, and the refusal is returned with
// no holds.
func TestSplitSecondaryHoldsRefuseACandidateWithoutAName(t *testing.T) {
	source := holdsSource()
	observations := holdsWorld(t, source, map[string]holdsMember{"member-a": {current: 3, desired: 3, decoders: 9, ready: 9}})
	source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Allocation: &v1beta1.CandidateAllocationStatus{CurrentReplicas: 1, DesiredReplicas: 1}})
	got, err := splitSecondaryHolds(source, observations)
	if err == nil || !strings.HasSuffix(err.Error(), "require an identified allocation") {
		t.Fatalf("splitSecondaryHolds error = %v, want the apportionment's refusal", err)
	}
	if got != nil {
		t.Fatalf("holds = %v, want none", got)
	}
}

// TestSplitSecondaryHoldsAnswerNothingWithoutSecondaries: a source with no
// decoder or divided router has no secondary minimum to hold; the answer is
// no map at all, as outside the split modes.
func TestSplitSecondaryHoldsAnswerNothingWithoutSecondaries(t *testing.T) {
	source := holdsSource()
	source.Spec.Decoder = nil
	observations := holdsWorld(t, source, map[string]holdsMember{"member-a": {current: 3, desired: 3, decoders: 9, ready: 9}})
	got, err := splitSecondaryHolds(source, observations)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("holds = %v, want none", got)
	}
}
