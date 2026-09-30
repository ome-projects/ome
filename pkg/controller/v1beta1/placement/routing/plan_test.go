package routing

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func plannedRoutingSource() *v1beta1.InferenceService {
	source := splitISVC([]string{"a", "b"}, []int32{3, 3}, []int32{3, 3})
	source.Status.Placement.Plan = &v1beta1.PlacementPlanStatus{ID: "plan-a", Revision: 1, SourceUID: source.UID, ObservedGeneration: source.Generation}
	for i := range source.Status.Placement.Candidates {
		source.Status.Placement.Candidates[i].Allocation = &v1beta1.CandidateAllocationStatus{CurrentReplicas: 3, DesiredReplicas: 3}
	}
	return source
}

func TestBuildSpecPlannedDrain(t *testing.T) {
	for _, tt := range []struct {
		name   string
		drain  []bool
		want   []int32
		status metav1.ConditionStatus
	}{
		{name: "both standing homes retain traffic", drain: []bool{false, false}, want: []int32{1, 1}, status: metav1.ConditionTrue},
		{name: "retiring home withdraws before deletion", drain: []bool{true, false}, want: []int32{0, 1}, status: metav1.ConditionTrue},
		{name: "whole placement can publish empty capacity", drain: []bool{true, true}, want: []int32{0, 0}, status: metav1.ConditionFalse},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := plannedRoutingSource()
			for i, drain := range tt.drain {
				source.Status.Placement.Candidates[i].Allocation.DrainRequested = drain
			}
			spec, status, _ := buildSpec(source, ResolvedProbePolicy{}, ResolvedCapacityPolicy{}, nil, nil)
			got := make([]int32, len(spec.Entries))
			for i := range spec.Entries {
				got[i] = spec.Entries[i].Weight
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("weights (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.status, status); diff != "" {
				t.Errorf("routable status (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(source.Status.Placement.Plan.ID, spec.PlacementPlanID); diff != "" {
				t.Errorf("allocation identity (-want +got):\n%s", diff)
			}
		})
	}
}

func TestApplyRequiresCurrentAllocation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		edit     func(*v1beta1.InferenceService)
		noReader bool
		wantErr  bool
	}{
		{name: "current plan can publish"},
		{name: "newer plan blocks stale routing", edit: func(s *v1beta1.InferenceService) {
			s.Status.Placement.Plan.ID = "plan-b"
			s.Status.Placement.Plan.Revision++
		}, wantErr: true},
		{name: "same digest newer revision blocks stale routing", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.Revision++ }, wantErr: true},
		{name: "newer intent blocks stale routing", edit: func(s *v1beta1.InferenceService) { s.Generation++ }, wantErr: true},
		{name: "new source incarnation blocks stale routing", edit: func(s *v1beta1.InferenceService) { s.UID = "replacement-source" }, wantErr: true},
		{name: "direct reader is required", noReader: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := plannedRoutingSource()
			live := source.DeepCopy()
			if tt.edit != nil {
				tt.edit(live)
			}
			r, cl := newReconciler(t, controllerTestConfig(), live)
			if tt.noReader {
				r.APIReader = nil
			}
			spec, _, _ := buildSpec(source, ResolvedProbePolicy{}, ResolvedCapacityPolicy{}, nil, nil)
			err := r.apply(t.Context(), source, spec)
			if (err != nil) != tt.wantErr {
				t.Fatalf("apply error = %v, want error %t", err, tt.wantErr)
			}
			stored := &v1beta1.TrafficMap{}
			found := cl.Get(t.Context(), client.ObjectKeyFromObject(source), stored) == nil
			if diff := cmp.Diff(!tt.wantErr, found); diff != "" {
				t.Errorf("traffic map existence (-want +got):\n%s", diff)
			}
		})
	}
}
