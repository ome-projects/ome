package routing

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestLegacyRoutingContinuesUpdating(t *testing.T) {
	for _, tt := range []struct {
		name       string
		annotation bool
		mode       v1beta1.PlacementMode
	}{
		{name: "annotation winner", annotation: true},
		{name: "implicit Single"},
		{name: "All factors", mode: v1beta1.PlacementModeAll},
		{name: "Split factors", mode: v1beta1.PlacementModeSplit},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := splitISVC([]string{"a", "b"}, []int32{3, 3}, []int32{3, 3})
			source.Spec.Placement = &v1beta1.PlacementSpec{Mode: tt.mode, Requirements: "accelerator=gpu-a", CapacityFactors: map[string]resource.Quantity{"a": resource.MustParse("2")}}
			single := tt.mode == ""
			if single {
				source.Status.Placement.Cluster = "a"
				source.Status.Placement.Endpoint = apis.HTTPS("a.example.com")
				source.Status.Placement.Candidates = nil
			}
			if tt.annotation {
				source.Spec.Placement = nil
				source.Annotations = map[string]string{"ome.io/cluster-selector": "region=west"}
			}
			r, cl := newReconciler(t, controllerTestConfig(), source)
			for _, changed := range []bool{false, true} {
				if changed {
					if err := cl.Get(t.Context(), client.ObjectKeyFromObject(source), source); err != nil {
						t.Fatal(err)
					}
					if single {
						source.Status.Placement.Cluster = "b"
						source.Status.Placement.Endpoint = apis.HTTPS("b.example.com")
					} else {
						source.Status.Placement.Candidates[0].ReadyReplicas = 0
					}
					if err := cl.Status().Update(t.Context(), source); err != nil {
						t.Fatal(err)
					}
				}
				reconcile(t, r)
				got, exists := getTrafficMap(t, cl)
				if !exists {
					t.Fatal("legacy routing lost TrafficMap")
				}
				weights := map[string]int32{}
				for _, entry := range got.Spec.Entries {
					weights[entry.Cluster] = entry.Weight
				}
				want := map[string]int32{"a": 2, "b": 1}
				mode := tt.mode
				if single {
					mode = v1beta1.PlacementModeSingle
					want = map[string]int32{"a": 1}
					if changed {
						want = map[string]int32{"b": 1}
					}
				} else if changed {
					want = map[string]int32{"a": 0, "b": 1}
				}
				if diff := cmp.Diff(want, weights); diff != "" {
					t.Fatal(diff)
				}
				if diff := cmp.Diff(mode, got.Spec.Mode); diff != "" {
					t.Fatal(diff)
				}
				if got.Spec.PlacementPlanID != "" {
					t.Fatal("legacy routing acquired plan authority")
				}
			}
		})
	}
}
