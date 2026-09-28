package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestWritePlacementRejectsStaleSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*v1beta1.InferenceService)
	}{
		{name: "generation", mutate: func(s *v1beta1.InferenceService) { s.Generation++ }},
		{name: "source incarnation", mutate: func(s *v1beta1.InferenceService) { s.UID = "source-new" }},
		{name: "plan revision", mutate: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.Revision++ }},
		{name: "plan identity", mutate: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.ID = "plan-new" }},
		{name: "deleted source", mutate: func(s *v1beta1.InferenceService) {
			s.DeletionTimestamp = ptr.To(metav1.Now())
			s.Finalizers = []string{"example.com/retain"}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := srcISVC("accelerator=gpu-a")
			source.Generation = 7
			source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{ID: "plan-a", Revision: 1}, Phase: v1beta1.PlacementPhasePlaced}
			live := source.DeepCopy()
			tt.mutate(live)
			r, cp := newPlacer(testScheme(t), fakeClusters{}, live)
			key := client.ObjectKeyFromObject(source)
			before := &v1beta1.InferenceService{}
			if err := cp.Get(t.Context(), key, before); err != nil {
				t.Fatal(err)
			}
			result, err := r.writePlacement(t.Context(), source, placementResult{phase: v1beta1.PlacementPhasePending})
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(r.requeue(), result.RequeueAfter); diff != "" {
				t.Fatal(diff)
			}
			got := &v1beta1.InferenceService{}
			if err := cp.Get(t.Context(), key, got); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(before, got); diff != "" {
				t.Fatalf("stale result changed source:\n%s", diff)
			}
		})
	}
}

func TestMergeCandidateObservationsPreservesAuthority(t *testing.T) {
	stored := []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "registration-a", OriginalReplicas: 6, CurrentReplicas: 6, DesiredReplicas: 3}, AppliedPlanID: "plan-a", ObservationKnown: true}}
	for _, tt := range []struct {
		name     string
		observed []v1beta1.CandidatePlacement
		want     []v1beta1.CandidatePlacement
	}{
		{name: "missing observation", want: []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: stored[0].Allocation, AppliedPlanID: "plan-a"}}},
		{name: "unidentified observation", observed: []v1beta1.CandidatePlacement{{Cluster: "member-a", ReadyReplicas: 2, ObservationKnown: true, AppliedPlanID: "plan-b"}}, want: []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: stored[0].Allocation, ReadyReplicas: 2}}},
		{name: "identified observation", observed: []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "registration-a"}, ReadyReplicas: 2, ObservationKnown: true, AppliedPlanID: "plan-a"}}, want: []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: stored[0].Allocation, ReadyReplicas: 2, ObservationKnown: true, AppliedPlanID: "plan-a"}}},
		{name: "replacement incarnation", observed: []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "registration-b"}, ObservationKnown: true, AppliedPlanID: "plan-a"}}, want: []v1beta1.CandidatePlacement{{Cluster: "member-a", Allocation: stored[0].Allocation}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeCandidateObservations(stored, tt.observed)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
			got[0].Allocation.CurrentReplicas = 0
			if diff := cmp.Diff(int32(6), stored[0].Allocation.CurrentReplicas); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
