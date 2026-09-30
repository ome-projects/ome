package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

func TestSingleMoveProposal(t *testing.T) {
	for _, tt := range []struct {
		name               string
		edit               func(*v1beta1.InferenceService, *placementObservations, *[]v1beta1.WorkloadCluster, *[]string)
		selected           string
		desiredA, desiredB int32
		pending, wantErr   bool
	}{
		{name: "bounded race preserves original floor", desiredB: 3},
		{name: "new floor does not rewrite occupied probe", desiredB: 4, edit: func(s *v1beta1.InferenceService, _ *placementObservations, _ *[]v1beta1.WorkloadCluster, _ *[]string) {
			s.Spec.Engine.MinReplicas = ptr.To(4)
		}},
		{name: "unresolved candidate keeps its accepted goal", desiredB: 3, pending: true, edit: func(_ *v1beta1.InferenceService, _ *placementObservations, _ *[]v1beta1.WorkloadCluster, eligible *[]string) {
			*eligible = nil
		}},
		{name: "restored affinity cancels probes", selected: "member-a", desiredA: 3, edit: func(_ *v1beta1.InferenceService, o *placementObservations, _ *[]v1beta1.WorkloadCluster, _ *[]string) {
			o.matches["member-a"] = true
		}},
		{name: "unmatched selection restores original", selected: "member-a", desiredA: 3, edit: func(s *v1beta1.InferenceService, o *placementObservations, _ *[]v1beta1.WorkloadCluster, _ *[]string) {
			s.Status.Placement.Plan.SingleMove.Selected = "member-b"
			o.matches["member-b"] = false
		}},
		{name: "accepted handoff finishes cleanup", selected: "member-b", desiredB: 3, edit: func(s *v1beta1.InferenceService, _ *placementObservations, _ *[]v1beta1.WorkloadCluster, _ *[]string) {
			s.Status.Placement.Plan.Winner = "member-b"
			s.Status.Placement.Plan.SingleMove.Selected = "member-b"
			s.Status.Placement.Candidates[1].Allocation.RaceCandidate = false
		}},
		{name: "replaced registration holds", wantErr: true, edit: func(_ *v1beta1.InferenceService, _ *placementObservations, clusters *[]v1beta1.WorkloadCluster, _ *[]string) {
			(*clusters)[1].UID = "replacement-uid"
		}},
		{name: "unidentified registration holds", wantErr: true, edit: func(_ *v1beta1.InferenceService, _ *placementObservations, clusters *[]v1beta1.WorkloadCluster, _ *[]string) {
			(*clusters)[1].UID = ""
		}},
		{name: "missing authority holds", wantErr: true, edit: func(s *v1beta1.InferenceService, _ *placementObservations, _ *[]v1beta1.WorkloadCluster, _ *[]string) {
			s.Status.Placement.Candidates[1].Allocation = nil
		}},
		{name: "different source authority holds", wantErr: true, edit: func(s *v1beta1.InferenceService, _ *placementObservations, _ *[]v1beta1.WorkloadCluster, _ *[]string) {
			s.Status.Placement.Plan.SourceUID = "older-source"
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source, proposal, obs := replacementRaceFixture()
			source.Spec.Engine.MinReplicas = ptr.To(3)
			activeReplacement(&proposal, obs, "member-b", false)
			source.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeSingle, Winner: proposal.Winner, SourceUID: source.UID, SingleMove: proposal.SingleMove.DeepCopy()}}
			scheme := testScheme(t)
			connections := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}
			var clusters []v1beta1.WorkloadCluster
			var objects []client.Object
			for _, name := range []string{"member-a", "member-b", "member-c"} {
				a := proposal.Assignments[name]
				source.Status.Placement.Candidates = append(source.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: name, Allocation: a.DeepCopy()})
				cluster := readyWC(name, nil)
				clusters = append(clusters, *cluster)
				objects = append(objects, cluster)
				connections.m[name] = workloadcluster.NewNeverCachingClient(emptyWorker(scheme))
			}
			eligible := []string{"member-b", "member-c"}
			standing := &placementObservations{matches: map[string]bool{"member-b": true, "member-c": true}}
			if tt.edit != nil {
				tt.edit(source, standing, &clusters, &eligible)
			}
			before := source.DeepCopy()
			r, _ := newPlacer(scheme, connections, objects...)
			got, err := r.singleMoveProposal(t.Context(), source, clusters, eligible, standing)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error %v: %s", err, diff)
			}
			if tt.wantErr {
				return
			}
			if diff := cmp.Diff(tt.selected, got.SingleMove.Selected); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff([]int32{tt.desiredA, tt.desiredB}, []int32{got.Assignments["member-a"].DesiredReplicas, got.Assignments["member-b"].DesiredReplicas}); diff != "" {
				t.Fatal(diff)
			}
			if diff := cmp.Diff(tt.pending, got.Assignments["member-b"].HomeInputsPending); diff != "" {
				t.Fatal(diff)
			}
			for _, name := range []string{"member-a", "member-b"} {
				if diff := cmp.Diff(proposal.Assignments[name].CurrentHome, got.Assignments[name].CurrentHome); diff != "" {
					t.Fatalf("occupied home changed: %s", diff)
				}
				if diff := cmp.Diff(proposal.Assignments[name].OriginalReplicas, got.Assignments[name].OriginalReplicas); diff != "" {
					t.Fatalf("original budget changed: %s", diff)
				}
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Fatalf("source mutated: %s", diff)
			}
		})
	}
}
