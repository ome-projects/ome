package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/plan"
)

// minimumsAt is a complete member policy at the plan's inputs: an engine
// minimum and a decoder minimum.
func minimumsAt(engine, decoder int32) *v1beta1.PlacementHomePolicy {
	return &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{
		{Component: v1beta1.EngineComponent, Replicas: engine},
		{Component: v1beta1.DecoderComponent, Replicas: decoder},
	}}
}

// loweringMember is a matched member that holds an engine minimum of 6 and a
// decoder minimum of 4 and is asked for 3 and 2: the shape local scaling
// takes.
func loweringMember() v1beta1.CandidateAllocationStatus {
	return v1beta1.CandidateAllocationStatus{
		ClusterUID: "member-uid", Matched: true,
		CurrentHome: minimumsAt(6, 4), CurrentReplicas: 6, OriginalReplicas: 6,
		DesiredHome: minimumsAt(3, 2), DesiredReplicas: 3,
	}
}

// loweredMember is loweringMember once the plan takes its new minimums: the
// new policy, and the new count as what it runs and where its change began.
func loweredMember() v1beta1.CandidateAllocationStatus {
	a := loweringMember()
	a.CurrentHome = minimumsAt(3, 2)
	a.CurrentReplicas, a.OriginalReplicas = 3, 3
	return a
}

// twoLoweringMembers is an accepted All plan whose members a and b both
// lower their minimums; edit changes one thing about it.
func twoLoweringMembers(edit func(*plan.Proposal)) plan.Proposal {
	proposal := plan.Proposal{Mode: v1beta1.PlacementModeAll, InputDigest: "intent", Assignments: map[string]v1beta1.CandidateAllocationStatus{
		"a": loweringMember(), "b": loweringMember(),
	}}
	if edit != nil {
		edit(&proposal)
	}
	return proposal
}

// memberB edits member b of a proposal.
func memberB(change func(*v1beta1.CandidateAllocationStatus)) func(*plan.Proposal) {
	return func(proposal *plan.Proposal) {
		b := proposal.Assignments["b"]
		change(&b)
		proposal.Assignments["b"] = b
	}
}

// TestAcceptAllLocalMinimumsLeavesThePlanWhenAnyMemberMoves holds that one
// member that leaves, joins, drains, waits or grows keeps every member's
// current minimums, so growth keeps the surge pause.
func TestAcceptAllLocalMinimumsLeavesThePlanWhenAnyMemberMoves(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*plan.Proposal)
	}{
		{name: "growth is paused", edit: func(p *plan.Proposal) { p.PauseSurge = true }},
		{name: "a first plan is still adopting its members", edit: func(p *plan.Proposal) { p.AdoptionDigest = "intent" }},
		{name: "a single move is in flight", edit: func(p *plan.Proposal) { p.SingleMove = &v1beta1.PlacementSingleMoveStatus{} }},
		{name: "a member is still being inventoried", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) { a.InventoryPending = true })},
		{name: "a member waits for its inputs", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) { a.HomeInputsPending = true })},
		{name: "a member is asked to drain", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) { a.DrainRequested = true })},
		{name: "an unmatched member still holds a policy", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.Matched, a.CurrentReplicas, a.DesiredHome, a.DesiredReplicas = false, 0, nil, 0
		})},
		{name: "an unmatched member still runs replicas", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.Matched, a.CurrentHome, a.CurrentReplicas, a.DesiredHome, a.DesiredReplicas = false, nil, 2, nil, 0
		})},
		{name: "a member has no accepted policy", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) { a.CurrentHome = nil })},
		{name: "a member has no resolved policy", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) { a.DesiredHome = nil })},
		{name: "a member's policy is behind the plan's inputs", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.DesiredHome.InputDigest = "older"
		})},
		{name: "a member's engine minimum rises", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.DesiredHome, a.DesiredReplicas = minimumsAt(7, 2), 7
		})},
		{name: "a member's engine minimum rises by one", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.CurrentHome, a.CurrentReplicas, a.OriginalReplicas = minimumsAt(2, 4), 2, 2
		})},
		{name: "a member's decoder minimum rises while its engine minimum falls", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.DesiredHome = minimumsAt(3, 5)
		})},
		{name: "a member's decoder minimum rises by one", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.CurrentHome = minimumsAt(6, 1)
		})},
		{name: "a member's policy adds a component", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.DesiredHome.ReplicaFloors = append(a.DesiredHome.ReplicaFloors, v1beta1.PlacementComponentFloor{Component: v1beta1.RouterComponent, Replicas: 1})
		})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			proposal := twoLoweringMembers(tt.edit)
			before := twoLoweringMembers(tt.edit)
			acceptAllLocalMinimums(&proposal)
			if diff := cmp.Diff(before, proposal); diff != "" {
				t.Fatalf("the plan changed (-before +after):\n%s", diff)
			}
		})
	}
}

// TestAcceptAllLocalMinimumsTakesLoweredMinimumsAtOnce holds that when every
// member stays and none grows, each member whose count falls takes its new
// policy and count at once, and the members that need nothing keep theirs.
func TestAcceptAllLocalMinimumsTakesLoweredMinimumsAtOnce(t *testing.T) {
	atItsMinimums := func(a *v1beta1.CandidateAllocationStatus) {
		a.CurrentHome, a.CurrentReplicas, a.OriginalReplicas = minimumsAt(3, 2), 3, 3
	}
	for _, tt := range []struct {
		name  string
		edit  func(*plan.Proposal)
		wantB v1beta1.CandidateAllocationStatus
	}{
		{name: "two members lower their minimums", wantB: loweredMember()},
		{name: "a member already at its minimums keeps them", edit: memberB(atItsMinimums), wantB: func() v1beta1.CandidateAllocationStatus {
			a := loweringMember()
			atItsMinimums(&a)
			return a
		}()},
		{name: "a member whose engine minimum is unchanged keeps its current policy", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.CurrentHome, a.CurrentReplicas, a.OriginalReplicas = minimumsAt(3, 4), 3, 3
		}), wantB: func() v1beta1.CandidateAllocationStatus {
			a := loweringMember()
			a.CurrentHome, a.CurrentReplicas, a.OriginalReplicas = minimumsAt(3, 4), 3, 3
			return a
		}()},
		{name: "a member whose policy drops a component", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			a.DesiredHome.ReplicaFloors = a.DesiredHome.ReplicaFloors[:1]
		}), wantB: func() v1beta1.CandidateAllocationStatus {
			a := loweredMember()
			a.CurrentHome.ReplicaFloors = a.CurrentHome.ReplicaFloors[:1]
			a.DesiredHome.ReplicaFloors = a.DesiredHome.ReplicaFloors[:1]
			return a
		}()},
		{name: "an unmatched member that left is passed over", edit: memberB(func(a *v1beta1.CandidateAllocationStatus) {
			*a = v1beta1.CandidateAllocationStatus{ClusterUID: a.ClusterUID}
		}), wantB: v1beta1.CandidateAllocationStatus{ClusterUID: "member-uid"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			proposal := twoLoweringMembers(tt.edit)
			acceptAllLocalMinimums(&proposal)
			if diff := cmp.Diff(loweredMember(), proposal.Assignments["a"]); diff != "" {
				t.Fatalf("member a (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantB, proposal.Assignments["b"]); diff != "" {
				t.Fatalf("member b (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(twoLoweringMembers(tt.edit).InputDigest, proposal.InputDigest); diff != "" || proposal.PauseSurge || proposal.AdoptionDigest != "" {
				t.Fatalf("the plan's own fields changed: %+v", proposal)
			}
		})
	}
}

// TestAcceptAllLocalMinimumsCopiesTheTakenPolicy holds that the policy a
// member takes is its own copy: a later change to the resolved policy does
// not reach the accepted one.
func TestAcceptAllLocalMinimumsCopiesTheTakenPolicy(t *testing.T) {
	proposal := twoLoweringMembers(nil)
	acceptAllLocalMinimums(&proposal)
	a := proposal.Assignments["a"]
	a.DesiredHome.ReplicaFloors[0].Replicas = 9
	if diff := cmp.Diff(minimumsAt(3, 2), a.CurrentHome); diff != "" {
		t.Fatalf("the accepted policy follows the resolved one (-want +got):\n%s", diff)
	}
}
