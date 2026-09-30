package plan

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func fullHomeProposal() Proposal {
	home := &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 3}, {Component: v1beta1.RouterComponent, Replicas: 1}}}
	return Proposal{Mode: v1beta1.PlacementModeAll, InputDigest: "intent", AdoptionDigest: "initial-intent", Assignments: map[string]v1beta1.CandidateAllocationStatus{
		"member-a": {ClusterUID: "cluster-uid", Matched: true, CurrentHome: home.DeepCopy(), DesiredHome: home.DeepCopy(), OriginalReplicas: 3, CurrentReplicas: 3, DesiredReplicas: 3},
	}}
}

func TestFullHomeAuthoritySurvivesStorage(t *testing.T) {
	for _, tt := range []struct {
		name  string
		prune func(*v1beta1.PlacementStatus)
	}{
		{name: "complete authority"},
		{name: "current home pruned", prune: func(s *v1beta1.PlacementStatus) { s.Candidates[0].Allocation.CurrentHome = nil }},
		{name: "desired home pruned", prune: func(s *v1beta1.PlacementStatus) { s.Candidates[0].Allocation.DesiredHome = nil }},
		{name: "matched intent pruned", prune: func(s *v1beta1.PlacementStatus) { s.Candidates[0].Allocation.Matched = false }},
		{name: "bootstrap intent pruned", prune: func(s *v1beta1.PlacementStatus) { s.Plan.AdoptionDigest = "" }},
		{name: "mode pruned", prune: func(s *v1beta1.PlacementStatus) { s.Plan.Mode = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			source.Spec.Placement.Mode = v1beta1.PlacementModeAll
			hooks := interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if tt.prune != nil {
					tt.prune(obj.(*v1beta1.InferenceService).Status.Placement)
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			}}
			store := storeFixture(t, source, hooks)
			got, err := store.Persist(t.Context(), source, fullHomeProposal())
			if tt.prune != nil {
				if !errors.Is(err, ErrSchemaUnsupported) {
					t.Fatalf("pruned authority accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(fullHomeProposal().Assignments["member-a"], *got.Status.Placement.Candidates[0].Allocation); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestFullHomeReservationValidation(t *testing.T) {
	for _, tt := range []struct {
		name    string
		edit    func(*v1beta1.CandidateAllocationStatus)
		wantErr bool
	}{
		{name: "complete home"},
		{name: "partial current floor", edit: func(a *v1beta1.CandidateAllocationStatus) { a.CurrentReplicas = 2 }, wantErr: true},
		{name: "partial draining floor", edit: func(a *v1beta1.CandidateAllocationStatus) { a.CurrentReplicas = 2; a.DrainRequested = true }, wantErr: true},
		{name: "drained whole home", edit: func(a *v1beta1.CandidateAllocationStatus) { a.CurrentReplicas = 0; a.DrainRequested = true }},
		{name: "zero before drain", edit: func(a *v1beta1.CandidateAllocationStatus) { a.CurrentReplicas = 0 }, wantErr: true},
		{name: "wrong desired floor", edit: func(a *v1beta1.CandidateAllocationStatus) { a.DesiredReplicas = 4 }, wantErr: true},
		{name: "unidentified source policy", edit: func(a *v1beta1.CandidateAllocationStatus) { a.CurrentHome.InputDigest = "" }, wantErr: true},
		{name: "missing current policy", edit: func(a *v1beta1.CandidateAllocationStatus) { a.CurrentHome = nil }, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := sourceFixture()
			source.Spec.Placement.Mode = v1beta1.PlacementModeAll
			proposal := fullHomeProposal()
			a := proposal.Assignments["member-a"]
			if tt.edit != nil {
				tt.edit(&a)
			}
			proposal.Assignments["member-a"] = a
			_, err := prepare(source, proposal)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("%s: %v", diff, err)
			}
		})
	}
}
