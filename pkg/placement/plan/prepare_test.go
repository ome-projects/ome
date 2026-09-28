package plan

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPlanIdentity(t *testing.T) {
	for _, tt := range []struct {
		name      string
		mutate    func(*v1beta1.InferenceService, *Proposal)
		unchanged bool
	}{
		{name: "same input", unchanged: true},
		{name: "term order", unchanged: true, mutate: func(_ *v1beta1.InferenceService, p *Proposal) {
			a := p.Assignments["member-b"]
			a.MatchingTerms = []int32{0, 2}
			p.Assignments["member-b"] = a
		}},
		{name: "heartbeat", unchanged: true, mutate: func(_ *v1beta1.InferenceService, p *Proposal) {
			a := p.Assignments["member-a"]
			a.Capacity.Pools[0].ObservedAt.Time = a.Capacity.Pools[0].ObservedAt.Add(time.Minute)
			a.Capacity.Pools[0].ReportResourceVersion = "2"
		}},
		{name: "source incarnation", mutate: func(s *v1beta1.InferenceService, _ *Proposal) { s.UID = "source-b" }},
		{name: "source generation", mutate: func(s *v1beta1.InferenceService, _ *Proposal) { s.Generation++ }},
		{name: "resolved demand", mutate: func(_ *v1beta1.InferenceService, p *Proposal) {
			p.Assignments["member-a"].Capacity.DemandFingerprint = "demand-b"
		}},
		{name: "hardware", mutate: func(_ *v1beta1.InferenceService, p *Proposal) {
			a := p.Assignments["member-a"]
			a.Capacity.Pools[0].Allocatable = 10
			a.Capacity.Replicas = 5
		}},
		{name: "report incarnation", mutate: func(_ *v1beta1.InferenceService, p *Proposal) {
			p.Assignments["member-a"].Capacity.Pools[0].ReportUID = "report-b"
		}},
		{name: "flavor incarnation", mutate: func(_ *v1beta1.InferenceService, p *Proposal) {
			p.Assignments["member-a"].Capacity.Pools[0].Attribution.FlavorUID = "flavor-b"
		}},
		{name: "unassigned floor", mutate: func(_ *v1beta1.InferenceService, p *Proposal) { p.UnassignedReplicas = 2 }},
		{name: "original unassigned floor", mutate: func(_ *v1beta1.InferenceService, p *Proposal) { p.OriginalUnassignedReplicas = 2 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			proposal := proposalFixture()
			a := proposal.Assignments["member-a"]
			a.Weight = nil
			a.Capacity = capacityFixture()
			proposal.Assignments["member-a"] = a
			first, err := prepare(source, proposal)
			if err != nil {
				t.Fatal(err)
			}
			before := first.DeepCopy()
			if tt.mutate != nil {
				tt.mutate(source, &proposal)
			}
			got, err := prepare(source, proposal)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.unchanged, first.Plan.ID == got.Plan.ID); diff != "" {
				t.Fatalf("stable identity:\n%s", diff)
			}
			if diff := cmp.Diff(before, first); diff != "" {
				t.Fatalf("input aliased output:\n%s", diff)
			}
			if tt.unchanged && tt.name == "heartbeat" {
				store := storeFixture(t, source, interceptor.Funcs{})
				// The committed evidence remains the accepted snapshot, even as reports refresh.
				stored := mustPersist(t, store, source, proposal)
				proposal.Assignments["member-a"].Capacity.Pools[0].ReportResourceVersion = "3"
				again := mustPersist(t, store, stored, proposal)
				if diff := cmp.Diff(stored, again); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
}

func TestInvalidProposal(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*Proposal, *v1beta1.CandidateAllocationStatus)
	}{
		{name: "missing input digest", mutate: func(p *Proposal, _ *v1beta1.CandidateAllocationStatus) { p.InputDigest = "" }},
		{name: "negative unassigned", mutate: func(p *Proposal, _ *v1beta1.CandidateAllocationStatus) { p.UnassignedReplicas = -1 }},
		{name: "negative original unassigned", mutate: func(p *Proposal, _ *v1beta1.CandidateAllocationStatus) { p.OriginalUnassignedReplicas = -1 }},
		{name: "empty cluster name", mutate: func(p *Proposal, _ *v1beta1.CandidateAllocationStatus) { p.Assignments[""] = p.Assignments["member-b"] }},
		{name: "missing cluster identity", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.ClusterUID = "" }},
		{name: "negative original", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.OriginalReplicas = -1 }},
		{name: "negative current", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.CurrentReplicas = -1 }},
		{name: "negative desired", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.DesiredReplicas = -1 }},
		{name: "negative weight", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) {
			a.Capacity = nil
			a.Weight = ptr.To(int64(-1))
		}},
		{name: "ambiguous weight", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.Weight = ptr.To(int64(1)) }},
		{name: "negative term", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.MatchingTerms = []int32{-1} }},
		{name: "duplicate term", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.MatchingTerms = []int32{1, 1} }},
		{name: "missing demand fingerprint", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.Capacity.DemandFingerprint = "" }},
		{name: "missing pool evidence", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.Capacity.Pools = nil }},
		{name: "incomplete attribution", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) {
			a.Capacity.Pools[0].Attribution.Complete = false
		}},
		{name: "inconsistent capacity", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) { a.Capacity.Replicas++ }},
		{name: "duplicate pool", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) {
			a.Capacity.Pools = append(a.Capacity.Pools, a.Capacity.Pools[0])
		}},
		{name: "incoherent report", mutate: func(_ *Proposal, a *v1beta1.CandidateAllocationStatus) {
			pool := a.Capacity.Pools[0]
			pool.ResourceName = "example.com/accelerator"
			pool.ReportUID = "report-b"
			a.Capacity.Pools = append(a.Capacity.Pools, pool)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			proposal := proposalFixture()
			a := proposal.Assignments["member-a"]
			a.Weight = nil
			a.Capacity = capacityFixture()
			tt.mutate(&proposal, &a)
			proposal.Assignments["member-a"] = a
			got, err := prepare(sourceFixture(), proposal)
			if err == nil || got != nil {
				t.Fatalf("want rejection, got %v, %v", got, err)
			}
		})
	}
}
