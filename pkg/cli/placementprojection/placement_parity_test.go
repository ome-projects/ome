package placementprojection

import (
	"reflect"
	"testing"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestProjectExplainMatchesAuthoritativeClusterName(t *testing.T) {
	for _, tc := range []struct {
		name     string
		selector string
		want     v.PlacementValue
	}{
		{name: "equality", selector: "metadata.name=demo-a", want: "True"},
		{name: "in", selector: "metadata.name in (demo-a,demo-b)", want: "True"},
		{name: "negative-set-membership", selector: "metadata.name " + "not" + "in (demo-a,demo-b)", want: "False"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := fixture(t)
			snapshot.InferenceService.Spec.Placement = &ome.PlacementSpec{
				ClusterSelector: tc.selector,
			}

			got, err := ProjectExplain(snapshot, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Content.Clusters) != 1 {
				t.Fatalf("clusters=%+v", got.Content.Clusters)
			}
			if got.Content.Clusters[0].ComputedSelectorCompatible != tc.want {
				t.Fatalf("compatible=%q want %q", got.Content.Clusters[0].ComputedSelectorCompatible, tc.want)
			}
		})
	}
}

func TestProjectExplainClusterNameOverridesSpoofedLabelWithoutMutation(t *testing.T) {
	snapshot := fixture(t)
	snapshot.WorkloadClusters[0].Labels["metadata.name"] = "spoofed"
	before := snapshot.WorkloadClusters[0].DeepCopy()

	for _, tc := range []struct {
		name     string
		selector string
		want     v.PlacementValue
	}{
		{name: "real-name", selector: "metadata.name=demo-a", want: "True"},
		{name: "spoofed-name", selector: "metadata.name=spoofed", want: "False"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot.InferenceService.Spec.Placement = &ome.PlacementSpec{
				ClusterSelector: tc.selector,
			}
			got, err := ProjectExplain(snapshot, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Content.Clusters) != 1 || got.Content.Clusters[0].ComputedSelectorCompatible != tc.want {
				t.Fatalf("clusters=%+v want compatibility %q", got.Content.Clusters, tc.want)
			}
			if !reflect.DeepEqual(before, snapshot.WorkloadClusters[0].DeepCopy()) {
				t.Fatal("selector evaluation mutated WorkloadCluster labels")
			}
		})
	}
}

func TestProjectExplainANDComposesRequirementsAndClusterSelector(t *testing.T) {
	for _, tc := range []struct {
		name         string
		requirements string
		selector     string
		want         v.PlacementValue
	}{
		{name: "both-match", requirements: "accelerator=cpu", selector: "metadata.name=demo-a", want: "True"},
		{name: "requirements-miss", requirements: "accelerator=gpu", selector: "metadata.name=demo-a", want: "False"},
		{name: "cluster-selector-miss", requirements: "accelerator=cpu", selector: "metadata.name=demo-b", want: "False"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := fixture(t)
			snapshot.InferenceService.Spec.Placement = &ome.PlacementSpec{
				Requirements:    tc.requirements,
				ClusterSelector: tc.selector,
			}

			got, err := ProjectExplain(snapshot, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Content.Clusters) != 1 || got.Content.Clusters[0].ComputedSelectorCompatible != tc.want {
				t.Fatalf("clusters=%+v want compatibility %q", got.Content.Clusters, tc.want)
			}
		})
	}
}

func TestProjectStatusReportsCandidateCountsForRecognizedModes(t *testing.T) {
	for _, mode := range []ome.PlacementMode{
		ome.PlacementModeSingle,
		ome.PlacementModeAll,
		ome.PlacementModeSplit,
	} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name              string
				admitted, ready   int32
				wantState         v.PlacementValue
				wantAdmittedValue *int32
				wantReadyValue    *int32
			}{
				{name: "positive", admitted: 5, ready: 4, wantState: "Reported", wantAdmittedValue: int32Pointer(5), wantReadyValue: int32Pointer(4)},
				{name: "zero", admitted: 0, ready: 0, wantState: "Unknown"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					snapshot := fixture(t)
					snapshot.InferenceService.Spec.Placement.Mode = mode
					snapshot.InferenceService.Status.Placement.Candidates = []ome.CandidatePlacement{{
						Cluster:          "demo-a",
						AdmittedReplicas: tc.admitted,
						ReadyReplicas:    tc.ready,
					}}

					got, err := ProjectStatus(snapshot, fixtureClock)
					if err != nil {
						t.Fatal(err)
					}
					if len(got.Content.Placement.Homes) != 1 {
						t.Fatalf("homes=%+v", got.Content.Placement.Homes)
					}
					home := got.Content.Placement.Homes[0]
					if home.AdmittedReplicas.State != tc.wantState || home.ReadyReplicas.State != tc.wantState {
						t.Fatalf("counts=%+v/%+v want state %q", home.AdmittedReplicas, home.ReadyReplicas, tc.wantState)
					}
					if !reflect.DeepEqual(home.AdmittedReplicas.Value, tc.wantAdmittedValue) || !reflect.DeepEqual(home.ReadyReplicas.Value, tc.wantReadyValue) {
						t.Fatalf("count values=%v/%v want %v/%v", home.AdmittedReplicas.Value, home.ReadyReplicas.Value, tc.wantAdmittedValue, tc.wantReadyValue)
					}
				})
			}
		})
	}
}

func TestProjectStatusUnknownModeDoesNotInterpretCandidateCounts(t *testing.T) {
	snapshot := fixture(t)
	snapshot.InferenceService.Spec.Placement.Mode = "Future"
	snapshot.InferenceService.Status.Placement.Candidates = []ome.CandidatePlacement{{
		Cluster:          "demo-a",
		AdmittedReplicas: 5,
		ReadyReplicas:    4,
	}}

	got, err := ProjectStatus(snapshot, fixtureClock)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Content.Placement.Homes) != 1 {
		t.Fatalf("homes=%+v", got.Content.Placement.Homes)
	}
	home := got.Content.Placement.Homes[0]
	if home.AdmittedReplicas.State != "NotApplicable" || home.AdmittedReplicas.Value != nil ||
		home.ReadyReplicas.State != "NotApplicable" || home.ReadyReplicas.Value != nil {
		t.Fatalf("counts=%+v/%+v", home.AdmittedReplicas, home.ReadyReplicas)
	}
}

func TestProjectStatusRejectsNegativeCandidateCounts(t *testing.T) {
	for _, tc := range []struct {
		name            string
		admitted, ready int32
	}{
		{name: "negative-admitted", admitted: -1},
		{name: "negative-ready", ready: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := fixture(t)
			snapshot.InferenceService.Spec.Placement.Mode = ome.PlacementModeAll
			snapshot.InferenceService.Status.Placement.Candidates = []ome.CandidatePlacement{{
				Cluster:          "demo-a",
				AdmittedReplicas: tc.admitted,
				ReadyReplicas:    tc.ready,
			}}

			got, err := ProjectStatus(snapshot, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			if got.Content.Placement.HomePreview.State != "MalformedPayload" || len(got.Content.Placement.Homes) != 0 {
				t.Fatalf("placement=%+v", got.Content.Placement)
			}
		})
	}
}

func int32Pointer(value int32) *int32 {
	return &value
}
