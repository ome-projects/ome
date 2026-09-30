package plan

import (
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestCanonicalCapacity(t *testing.T) {
	for _, tt := range []struct {
		name              string
		nilInput, invalid bool
	}{
		{name: "canonical copy"},
		{name: "nil evidence", nilInput: true},
		{name: "partial evidence", invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			input := capacityFixture()
			if tt.nilInput {
				input = nil
			}
			if tt.invalid {
				input.DemandContract.Fingerprint = "invalid"
			}
			before := input.DeepCopy()
			got, err := CanonicalCapacity(input)
			if diff := cmp.Diff(tt.nilInput || tt.invalid, err != nil); diff != "" {
				t.Fatalf("error: %s: %v", diff, err)
			}
			if err != nil {
				if diff := cmp.Diff((*v1beta1.PlacementCapacitySample)(nil), got); diff != "" {
					t.Fatal(diff)
				}
			} else {
				want := before.DeepCopy()
				slices.Reverse(want.DemandContract.Components)
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatal(diff)
				}
				got.DemandContract.Components[0].RenderingHash = "changed"
				got.Pools[0].Attribution.FlavorUID = "changed"
			}
			if diff := cmp.Diff(before, input); diff != "" {
				t.Fatalf("input changed: %s", diff)
			}
		})
	}
}

func TestCapacityPlanRequiresCompleteDemand(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.InferenceService, *v1beta1.CandidateAllocationStatus)
		wantErr bool
	}{
		{name: "engine and decoder"},
		{name: "missing capacity", edit: func(_ *v1beta1.InferenceService, a *v1beta1.CandidateAllocationStatus) { a.Capacity = nil }, wantErr: true},
		{name: "missing decoder contract", edit: func(_ *v1beta1.InferenceService, a *v1beta1.CandidateAllocationStatus) {
			a.Capacity.DemandContract.Components = a.Capacity.DemandContract.Components[:1]
		}, wantErr: true},
		{name: "undeclared decoder", edit: func(s *v1beta1.InferenceService, _ *v1beta1.CandidateAllocationStatus) { s.Spec.Decoder = nil }, wantErr: true},
		{name: "outgoing home retains old inventory", edit: func(s *v1beta1.InferenceService, a *v1beta1.CandidateAllocationStatus) {
			s.Spec.Decoder = nil
			a.DesiredReplicas = 0
		}},
		{name: "outgoing static home has no contract", edit: func(_ *v1beta1.InferenceService, a *v1beta1.CandidateAllocationStatus) {
			a.Capacity = nil
			a.DesiredReplicas = 0
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			source.Spec.Placement.Mode = v1beta1.PlacementModeSplitByCapacity
			source.Spec.Engine, source.Spec.Decoder = &v1beta1.EngineSpec{}, &v1beta1.DecoderSpec{}
			a := v1beta1.CandidateAllocationStatus{ClusterUID: "registration-a", CurrentReplicas: 1, DesiredReplicas: 1, Capacity: capacityFixture()}
			if tt.edit != nil {
				tt.edit(source, &a)
			}
			proposal := Proposal{InputDigest: "intent", Assignments: map[string]v1beta1.CandidateAllocationStatus{"member-a": a}}
			got, err := prepare(source, proposal)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error: %s: %v", diff, err)
			}
			if err != nil && got != nil {
				t.Fatal("invalid plan returned authority")
			}
		})
	}
}

func TestSameAllocation(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*v1beta1.InferenceService)
		want bool
	}{
		{name: "identical allocation", want: true},
		{name: "observation changes", edit: func(s *v1beta1.InferenceService) {
			s.ResourceVersion = "changed"
			s.Status.Placement.Candidates[0].ReadyReplicas++
			s.Status.Placement.Candidates[0].AppliedPlanID = "observed"
		}, want: true},
		{name: "candidate ordering", edit: func(s *v1beta1.InferenceService) { slices.Reverse(s.Status.Placement.Candidates) }, want: true},
		{name: "pruned contract", edit: func(s *v1beta1.InferenceService) {
			s.Status.Placement.Candidates[0].Allocation.Capacity.DemandContract = nil
		}},
		{name: "changed contract", edit: func(s *v1beta1.InferenceService) {
			s.Status.Placement.Candidates[0].Allocation.Capacity.DemandContract.Components[0].RenderingHash = strings.Repeat("e", 64)
		}},
		{name: "pruned pool", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation.Capacity.Pools = nil }},
		{name: "changed floor", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation.CurrentReplicas++ }},
		{name: "changed pause", edit: func(s *v1beta1.InferenceService) {
			s.Status.Placement.Plan.PauseSurge = !s.Status.Placement.Plan.PauseSurge
		}},
		{name: "duplicate candidate", edit: func(s *v1beta1.InferenceService) {
			s.Status.Placement.Candidates[1] = *s.Status.Placement.Candidates[0].DeepCopy()
		}},
		{name: "missing allocation", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation = nil }},
		{name: "missing plan", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan = nil }},
		{name: "missing status", edit: func(s *v1beta1.InferenceService) { s.Status.Placement = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			proposal := proposalFixture()
			a := proposal.Assignments["member-a"]
			a.Weight, a.Capacity = nil, capacityFixture()
			proposal.Assignments["member-a"] = a
			status, err := prepare(source, proposal)
			if err != nil {
				t.Fatal(err)
			}
			source.Status.Placement = status
			live := source.DeepCopy()
			if tt.edit != nil {
				tt.edit(live)
			}
			if diff := cmp.Diff(tt.want, SameAllocation(source, live)); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.want, SameAllocation(live, source)); diff != "" {
				t.Fatalf("reverse: %s", diff)
			}
			if SameAllocation(nil, live) || SameAllocation(source, nil) {
				t.Fatal("nil source authorized")
			}
		})
	}
}
