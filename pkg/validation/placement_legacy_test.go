package validation

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestPlacementPolicyCompatibility(t *testing.T) {
	for _, tt := range []struct {
		name, payload string
		invalid       bool
	}{
		{name: "ordinary local", payload: `{}`},
		{name: "annotation source", payload: `{"metadata":{"annotations":{"ome.io/cluster-selector":"region=west"}}}`},
		{name: "derived legacy copy", payload: `{"metadata":{"labels":{"ome.io/placement-origin":"source"}},"spec":{"placement":{"requirements":"accelerator=gpu-a"}}}`},
		{name: "implicit mode", payload: `{"spec":{"placement":{}}}`},
		{name: "null legacy selectors", payload: `{"spec":{"placement":{"requirements":null,"clusterSelector":null}}}`},
		{name: "zero legacy split fields", payload: `{"spec":{"placement":{"mode":"Split","split":{"spread":false,"minReplicasPerCluster":0}}}}`},
		{name: "legacy factors", payload: `{"spec":{"placement":{"capacityFactors":{"a":"2"}}}}`},
		{name: "legacy anti-sliver", payload: `{"spec":{"placement":{"mode":"Split","split":{"minReplicasPerCluster":2,"maxReplicasPerCluster":4}}}}`},
		{name: "inconsistent anti-sliver", payload: `{"spec":{"placement":{"mode":"Split","split":{"minReplicasPerCluster":5,"maxReplicasPerCluster":4}}}}`, invalid: true},
		{name: "invalid legacy selector", payload: `{"spec":{"placement":{"requirements":"!!!"}}}`, invalid: true},
		{name: "affinity requires opt-in", payload: `{"spec":{"placement":{"mode":"Single","clusterAffinity":[{"matchExpressions":[{"key":"accelerator","operator":"Exists"}]}]}}}`, invalid: true},
		{name: "surge requires opt-in", payload: `{"spec":{"placement":{"mode":"Single","maxSurge":0}}}`, invalid: true},
		{name: "replacement timeout requires opt-in", payload: `{"spec":{"placement":{"mode":"Single","replacementTimeout":"5m"}}}`, invalid: true},
		{name: "capacity requires opt-in", payload: `{"spec":{"placement":{"mode":"SplitByCapacity"}}}`, invalid: true},
		{name: "unknown policy", payload: `{"spec":{"placement":{"policy":"Unknown","mode":"Single"}}}`, invalid: true},
		{name: "new policy cannot inherit packing", payload: `{"spec":{"placement":{"policy":"ClusterAffinity","mode":"Split","split":{"spread":false}}}}`, invalid: true},
		{name: "plan cannot return to legacy", payload: `{"spec":{"placement":{"mode":"Split"}},"status":{"placement":{"plan":{"id":"accepted"}}}}`, invalid: true},
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
		{name: "local enables legacy", new: &v1beta1.PlacementSpec{}},
		{name: "legacy changes mode", old: &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeSingle}, new: &v1beta1.PlacementSpec{Mode: v1beta1.PlacementModeAll}},
		{name: "legacy opts in", old: &v1beta1.PlacementSpec{}, new: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}},
		{name: "new policy retained", old: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, new: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}},
		{name: "block placement removal", old: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, invalid: true},
		{name: "block policy removal", old: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, new: &v1beta1.PlacementSpec{}, invalid: true},
		{name: "block explicit downgrade", old: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity}, new: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyLegacy}, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePlacementPolicyUpdate(&v1beta1.InferenceServiceSpec{Placement: tt.old}, &v1beta1.InferenceServiceSpec{Placement: tt.new})
			if diff := cmp.Diff(tt.invalid, err != nil); diff != "" {
				t.Fatalf("handoff (-want +got):\n%s; %v", diff, err)
			}
		})
	}
}
