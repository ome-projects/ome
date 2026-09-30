package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func satisfiedSource() *v1beta1.InferenceService {
	source := srcISVCSplit("", 18)
	source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{
		ID: "plan-a", Revision: 1, SourceUID: source.UID, ObservedGeneration: source.Generation, RequestedReplicas: 18, AssignedReplicas: 18,
	}}
	for _, name := range []string{"member-a", "member-b", "member-c"} {
		source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{
			Cluster: name, Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "registration-a", CurrentReplicas: 6, DesiredReplicas: 6},
			ObservationKnown: true, AppliedPlanID: "plan-a", AdmittedReplicas: 6,
		})
	}
	return source
}

func TestPlacementSatisfaction(t *testing.T) {
	for _, tt := range []struct {
		name   string
		edit   func(*v1beta1.InferenceService)
		status corev1.ConditionStatus
		reason string
	}{
		{name: "all shares admitted", status: corev1.ConditionTrue, reason: "AssignedFloorsAdmitted"},
		{name: "surplus cannot cover another share", edit: func(s *v1beta1.InferenceService) {
			s.Status.Placement.Candidates[0].AdmittedReplicas = 12
			s.Status.Placement.Candidates[1].AdmittedReplicas = 2
		}, status: corev1.ConditionFalse, reason: "AllocationShortfall"},
		{name: "surplus does not break satisfaction", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].AdmittedReplicas = 12 }, status: corev1.ConditionTrue, reason: "AssignedFloorsAdmitted"},
		{name: "unknown member", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].ObservationKnown = false }, status: corev1.ConditionUnknown, reason: "AwaitingMemberApplication"},
		{name: "stale applied identity", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].AppliedPlanID = "old-plan" }, status: corev1.ConditionUnknown, reason: "AwaitingMemberApplication"},
		{name: "unassigned floor", edit: func(s *v1beta1.InferenceService) {
			s.Status.Placement.Plan.UnassignedReplicas = 2
			s.Status.Placement.Plan.RequestedReplicas = 20
		}, status: corev1.ConditionFalse, reason: "AllocationShortfall"},
		{name: "zero target needs no admission", edit: func(s *v1beta1.InferenceService) {
			s.Status.Placement.Candidates[0].Allocation.DesiredReplicas = 0
			s.Status.Placement.Candidates[0].ObservationKnown = false
			s.Status.Placement.Plan.AssignedReplicas, s.Status.Placement.Plan.RequestedReplicas = 12, 12
		}, status: corev1.ConditionTrue, reason: "AssignedFloorsAdmitted"},
		{name: "zero home requires member acknowledgement", edit: func(s *v1beta1.InferenceService) {
			c := &s.Status.Placement.Candidates[0]
			c.Allocation.DesiredReplicas = 0
			c.Allocation.DesiredHome = &v1beta1.PlacementHomePolicy{InputDigest: "source-intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}}}
			c.AppliedPlanID = ""
			s.Status.Placement.Plan.AssignedReplicas, s.Status.Placement.Plan.RequestedReplicas = 12, 12
		}, status: corev1.ConditionUnknown, reason: "AwaitingMemberApplication"},
		{name: "acknowledged zero home needs no admitted replicas", edit: func(s *v1beta1.InferenceService) {
			c := &s.Status.Placement.Candidates[0]
			c.Allocation.DesiredReplicas, c.AdmittedReplicas = 0, 0
			c.Allocation.DesiredHome = &v1beta1.PlacementHomePolicy{InputDigest: "source-intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent}}}
			s.Status.Placement.Plan.AssignedReplicas, s.Status.Placement.Plan.RequestedReplicas = 12, 12
		}, status: corev1.ConditionTrue, reason: "AssignedFloorsAdmitted"},
		{name: "source changed", edit: func(s *v1beta1.InferenceService) { s.Generation++ }, status: corev1.ConditionUnknown, reason: "AwaitingCurrentPlan"},
		{name: "source recreated", edit: func(s *v1beta1.InferenceService) { s.UID = "source-b" }, status: corev1.ConditionUnknown, reason: "AwaitingCurrentPlan"},
		{name: "plan identity missing", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Plan.ID = "" }, status: corev1.ConditionUnknown, reason: "AwaitingCurrentPlan"},
		{name: "cluster identity missing", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation.ClusterUID = "" }, status: corev1.ConditionUnknown, reason: "AwaitingMemberApplication"},
		{name: "missing assignment", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates = s.Status.Placement.Candidates[1:] }, status: corev1.ConditionUnknown, reason: "AllocationObservationIncomplete"},
		{name: "unidentified assignment", edit: func(s *v1beta1.InferenceService) { s.Status.Placement.Candidates[0].Allocation = nil }, status: corev1.ConditionUnknown, reason: "AllocationObservationIncomplete"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := satisfiedSource()
			if tt.edit != nil {
				tt.edit(source)
			}
			before := source.DeepCopy()
			got := placementSatisfactionConditions(source)
			if len(got) != 1 {
				t.Fatalf("conditions = %v, want one", got)
			}
			if diff := cmp.Diff(tt.status, got[0].cond.Status); diff != "" {
				t.Errorf("status (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.reason, got[0].cond.Reason); diff != "" {
				t.Errorf("reason (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Errorf("source mutated (-want +got):\n%s", diff)
			}
		})
	}
	for _, status := range []*v1beta1.PlacementStatus{nil, {}} {
		source := srcISVC("")
		source.Status.Placement = status
		if got := placementSatisfactionConditions(source); len(got) != 0 {
			t.Errorf("unplanned source conditions = %v", got)
		}
	}
}

func TestShortfallDoesNotOverrideServingReadiness(t *testing.T) {
	source := satisfiedSource()
	source.Status.Placement.Candidates[1].AdmittedReplicas = 2
	r, cp := newPlacer(testScheme(t), fakeClusters{}, source)
	_, err := r.writePlacement(t.Context(), source, placementResult{phase: v1beta1.PlacementPhasePlaced, candidates: source.Status.Placement.Candidates, ready: true})
	if err != nil {
		t.Fatal(err)
	}
	stored := &v1beta1.InferenceService{}
	if err := cp.Get(t.Context(), client.ObjectKeyFromObject(source), stored); err != nil {
		t.Fatal(err)
	}
	for condition, want := range map[apis.ConditionType]corev1.ConditionStatus{apis.ConditionReady: corev1.ConditionTrue, v1beta1.PlacementSatisfied: corev1.ConditionFalse} {
		got := stored.Status.GetCondition(condition)
		if got == nil {
			t.Fatalf("missing condition %s", condition)
		}
		if diff := cmp.Diff(want, got.Status); diff != "" {
			t.Errorf("%s (-want +got):\n%s", condition, diff)
		}
	}
}

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
