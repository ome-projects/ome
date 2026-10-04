package validation

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPlacementRequiresExplicitPolicy(t *testing.T) {
	for _, tt := range []struct {
		name, payload string
		invalid       bool
	}{
		{name: "ordinary local", payload: `{}`},
		{name: "annotation source", payload: `{"metadata":{"annotations":{"ome.io/cluster-selector":"region=west"}}}`, invalid: true},
		{name: "empty selector annotation", payload: `{"metadata":{"annotations":{"ome.io/cluster-selector":""}}}`, invalid: true},
		{name: "empty placement", payload: `{"spec":{"placement":{}}}`, invalid: true},
		{name: "mode without policy", payload: `{"spec":{"placement":{"mode":"Single"}}}`, invalid: true},
		{name: "retired policy", payload: `{"spec":{"placement":{"policy":"Legacy","mode":"Single"}}}`, invalid: true},
		{name: "unknown policy", payload: `{"spec":{"placement":{"policy":"Unknown","mode":"Single"}}}`, invalid: true},
		{name: "explicit policy", payload: `{"spec":{"placement":{"policy":"ClusterAffinity","mode":"Single"}}}`},
		{name: "plan requires policy", payload: `{"status":{"placement":{"plan":{"id":"accepted"}}}}`, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := &v1beta1.InferenceService{}
			if err := json.Unmarshal([]byte(tt.payload), source); err != nil {
				t.Fatal(err)
			}
			before := source.DeepCopy()
			err := ValidatePlacementIntent(source)
			if diff := cmp.Diff(tt.invalid, err != nil); diff != "" {
				t.Fatalf("validation (-want +got):\n%s; %v", diff, err)
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

func TestPlacementPolicyHandoff(t *testing.T) {
	for _, tt := range []struct {
		name     string
		old, new *v1beta1.PlacementSpec
		invalid  bool
	}{
		{name: "local remains local"},
		{name: "local adds placement", new: &v1beta1.PlacementSpec{}},
		{name: "unselected policy changes mode", old: &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSingle}, new: &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeAll}},
		{name: "explicit policy selection", old: &v1beta1.PlacementSpec{}, new: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}},
		{name: "new policy retained", old: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, new: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}},
		{name: "block placement removal", old: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, invalid: true},
		{name: "block policy removal", old: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, new: &v1beta1.PlacementSpec{}, invalid: true},
		{name: "block explicit downgrade", old: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, new: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicy("Legacy")}, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePlacementPolicyUpdate(&v1beta1.InferenceServiceSpec{Placement: tt.old}, &v1beta1.InferenceServiceSpec{Placement: tt.new})
			if diff := cmp.Diff(tt.invalid, err != nil); diff != "" {
				t.Fatalf("handoff (-want +got):\n%s; %v", diff, err)
			}
		})
	}
}
