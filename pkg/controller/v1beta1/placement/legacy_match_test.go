package placement

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestLegacySelectorContract(t *testing.T) {
	for _, tt := range []struct {
		name        string
		placement   *v1beta1.PlacementSpec
		annotations map[string]string
		want        []string
		invalid     bool
	}{
		{name: "no intent"},
		{name: "empty typed block", placement: &v1beta1.PlacementSpec{}},
		{name: "mode only matches nothing", placement: &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSingle}},
		{name: "explicit Legacy", placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyLegacy, Requirements: "accelerator=gpu-a"}, want: []string{"a", "b"}},
		{name: "annotations AND together", annotations: map[string]string{AcceleratorRequirementsAnnotation: "accelerator=gpu-a", ClusterSelectorAnnotation: "region=east"}, want: []string{"b"}},
		{name: "typed selectors AND together", placement: &v1beta1.PlacementSpec{Requirements: "accelerator=gpu-a", ClusterSelector: "region=west"}, want: []string{"a"}},
		{name: "typed block wins without field merge", placement: &v1beta1.PlacementSpec{ClusterSelector: "region=west"}, annotations: map[string]string{AcceleratorRequirementsAnnotation: "accelerator=gpu-a"}, want: []string{"a", "c"}},
		{name: "empty typed block masks annotations", placement: &v1beta1.PlacementSpec{}, annotations: map[string]string{ClusterSelectorAnnotation: "region=west"}},
		{name: "real object name overrides label", placement: &v1beta1.PlacementSpec{ClusterSelector: "metadata.name=a"}, want: []string{"a"}},
		{name: "name exclusion", placement: &v1beta1.PlacementSpec{ClusterSelector: "metadata.name notin (a,b)"}, want: []string{"c"}},
		{name: "malformed annotation", annotations: map[string]string{ClusterSelectorAnnotation: "!!!"}, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := isvcPlacement(tt.placement)
			source.Annotations = tt.annotations
			clusters := []v1beta1.WorkloadCluster{wc("a", true, map[string]string{"accelerator": "gpu-a", "region": "west", "metadata.name": "wrong"}), wc("b", true, map[string]string{"accelerator": "gpu-a", "region": "east"}), wc("c", true, map[string]string{"accelerator": "gpu-b", "region": "west"})}
			before := source.DeepCopy()
			got, _, err := MatchCandidates(source, clusters)
			if diff := cmp.Diff(tt.invalid, err != nil); diff != "" {
				t.Fatalf("error (-want +got):\n%s; %v", diff, err)
			}
			if diff := cmp.Diff(len(tt.want) > 0 || tt.invalid, IsPlacementEligible(source)); diff != "" {
				t.Errorf("legacy participation (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatal(diff)
			}
			for _, c := range clusters {
				matches := false
				for _, name := range tt.want {
					matches = matches || name == c.Name
				}
				if diff := cmp.Diff(matches || tt.invalid, clusterAffectsISVC(source, c.Name, c.Labels)); diff != "" {
					t.Errorf("event selection for %s:\n%s", c.Name, diff)
				}
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
