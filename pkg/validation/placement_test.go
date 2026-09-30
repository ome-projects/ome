package validation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestValidatePlacement(t *testing.T) {
	for _, tt := range []struct{ name, placement, wantError string }{
		{name: "local service", placement: `null`},
		{name: "unconstrained single", placement: `{"mode":"Single"}`},
		{name: "bounded replacement retries", placement: `{"mode":"Single","replacementTimeout":"5m"}`},
		{name: "zero replacement timeout", placement: `{"mode":"Single","replacementTimeout":"0s"}`, wantError: "replacementTimeout"},
		{name: "replacement timeout only for Single", placement: `{"mode":"All","replacementTimeout":"5m"}`, wantError: "replacementTimeout"},
		{name: "unconstrained all", placement: `{"mode":"All"}`},
		{name: "equal split", placement: `{"mode":"Split","split":{"replicas":6}}`},
		{name: "capacity split", placement: `{"mode":"SplitByCapacity","split":{"replicas":6,"maxReplicasPerCluster":4},"maxSurge":1}`},
		{name: "weighted static", placement: `{"mode":"Split","clusterAffinity":[{"weight":3,"matchFields":[{"key":"metadata.name","operator":"In","values":["member-a"]}]}]}`},
		{name: "missing mode", placement: `{}`, wantError: "explicitly select"},
		{name: "unsupported mode", placement: `{"mode":"Unexpected"}`, wantError: "explicitly select"},
		{name: "empty affinity", placement: `{"mode":"Single","clusterAffinity":[]}`, wantError: "clusterAffinity"},
		{name: "null affinity", placement: `{"mode":"Single","clusterAffinity":null}`, wantError: "clusterAffinity"},
		{name: "empty term", placement: `{"mode":"Single","clusterAffinity":[{}]}`, wantError: "requirement"},
		{name: "weight forbidden in capacity mode", placement: `{"mode":"SplitByCapacity","clusterAffinity":[{"weight":1,"matchExpressions":[{"key":"accelerator","operator":"Exists"}]}]}`, wantError: "weight"},
		{name: "unknown match field", placement: `{"mode":"Single","clusterAffinity":[{"matchFields":[{"key":"metadata.namespace","operator":"In","values":["team-a"]}]}]}`, wantError: "metadata.name"},
		{name: "negative surge", placement: `{"mode":"Single","maxSurge":-1}`, wantError: "maxSurge"},
		{name: "split settings for single", placement: `{"mode":"Single","split":{}}`, wantError: "permitted only"},
		{name: "zero requested floor", placement: `{"mode":"Split","split":{"replicas":0}}`, wantError: "replicas"},
		{name: "negative per-home ceiling", placement: `{"mode":"Split","split":{"maxReplicasPerCluster":-1}}`, wantError: "maxReplicasPerCluster"},
		{name: "legacy requirements", placement: `{"mode":"Single","requirements":"accelerator=gpu-a"}`, wantError: "migrate"},
		{name: "empty legacy requirements", placement: `{"mode":"Single","requirements":""}`, wantError: "migrate"},
		{name: "null legacy requirements", placement: `{"mode":"Single","requirements":null}`, wantError: "migrate"},
		{name: "empty legacy selector", placement: `{"mode":"Single","clusterSelector":""}`, wantError: "migrate"},
		{name: "empty legacy capacity factors", placement: `{"mode":"Single","capacityFactors":{}}`, wantError: "migrate"},
		{name: "explicit packed", placement: `{"mode":"Split","split":{"spread":false}}`, wantError: "unsupported"},
		{name: "zero anti-sliver", placement: `{"mode":"Split","split":{"minReplicasPerCluster":0}}`, wantError: "unsupported"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var placement *v1beta1.PlacementSpec
			if err := json.Unmarshal([]byte(tt.placement), &placement); err != nil {
				t.Fatal(err)
			}
			if placement != nil {
				placement.Policy = v1beta1.PlacementPolicyClusterAffinity
			}
			err := ValidatePlacement(&v1beta1.InferenceServiceSpec{Placement: placement})
			if diff := cmp.Diff(tt.wantError == "", err == nil); diff != "" {
				t.Fatalf("error presence:\n%s\n%v", diff, err)
			}
			if err != nil && !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("want %q in %v", tt.wantError, err)
			}
		})
	}
}

func TestValidatePlacementIntent(t *testing.T) {
	for _, tt := range []struct {
		name        string
		annotations map[string]string
		invalid     bool
	}{
		{name: "local service"},
		{name: "unrelated local metadata", annotations: map[string]string{"example.com/owner": "team-a"}},
		{name: "empty obsolete requirement", annotations: map[string]string{constants.AcceleratorRequirements: ""}, invalid: true},
		{name: "obsolete cluster selector", annotations: map[string]string{constants.ClusterSelector: "region=west"}, invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations}}
			if tt.invalid {
				source.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle}
			}
			before := source.DeepCopy()
			err := ValidatePlacementIntent(source)
			if diff := cmp.Diff(tt.invalid, err != nil); diff != "" {
				t.Fatalf("%s\n%v", diff, err)
			}
			if diff := cmp.Diff(before, source); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}
