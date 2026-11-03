package validation

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestValidatePlacement_ModeAndAffinityShapes_RejectsWhatTheModeForbids(t *testing.T) {
	for _, tt := range []struct{ name, placement, wantError string }{
		{name: "local service", placement: `null`},
		{name: "unconstrained single", placement: `{"mode":"Single"}`},
		{name: "bounded replacement retries", placement: `{"mode":"Single","replacementTimeout":"5m"}`},
		{name: "zero replacement timeout", placement: `{"mode":"Single","replacementTimeout":"0s"}`, wantError: "replacementTimeout"},
		{name: "replacement timeout only for Single", placement: `{"mode":"All","replacementTimeout":"5m"}`, wantError: "replacementTimeout"},
		{name: "unconstrained all", placement: `{"mode":"All"}`},
		{name: "equal split", placement: `{"mode":"Split","split":{"replicas":6}}`},
		{name: "capacity split", placement: `{"mode":"SplitByCapacity","split":{"replicas":6,"maxPerCluster":{"default":4}},"maxSurge":1}`},
		{name: "weighted static", placement: `{"mode":"Split","clusterAffinity":[{"weights":{"default":3},"matchFields":[{"key":"metadata.name","operator":"In","values":["member-a"]}]}]}`},
		{name: "missing mode", placement: `{}`, wantError: "explicitly select"},
		{name: "unsupported mode", placement: `{"mode":"Unexpected"}`, wantError: "explicitly select"},
		{name: "empty affinity", placement: `{"mode":"Single","clusterAffinity":[]}`, wantError: "clusterAffinity"},
		{name: "null affinity", placement: `{"mode":"Single","clusterAffinity":null}`, wantError: "clusterAffinity"},
		{name: "empty term", placement: `{"mode":"Single","clusterAffinity":[{}]}`, wantError: "requirement"},
		{name: "weights forbidden in capacity mode", placement: `{"mode":"SplitByCapacity","clusterAffinity":[{"weights":{"default":1},"matchExpressions":[{"key":"accelerator","operator":"Exists"}]}]}`, wantError: "weights"},
		{name: "unknown match field", placement: `{"mode":"Single","clusterAffinity":[{"matchFields":[{"key":"metadata.namespace","operator":"In","values":["team-a"]}]}]}`, wantError: "metadata.name"},
		{name: "split settings for single", placement: `{"mode":"Single","split":{}}`, wantError: "permitted only"},
		{name: "zero requested floor", placement: `{"mode":"Split","split":{"replicas":0}}`, wantError: "replicas"},
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

// A service that still applies the removed ComponentAffinity policy is refused
// with the policy named and the weights block pointed at, whether or not it
// already holds an accepted placement plan.
func TestValidatePlacementIntent_RemovedPolicy_NamesTheReplacement(t *testing.T) {
	const remedy = "the ComponentAffinity policy was removed, set each component's weight in the weights block of each clusterAffinity term"
	for _, tt := range []struct {
		name      string
		plan      *v1beta1.PlacementPlanStatus
		wantError string
	}{
		{name: "before any plan", wantError: "spec.placement.policy must be ClusterAffinity; " + remedy},
		{name: "with an accepted plan", plan: &v1beta1.PlacementPlanStatus{ID: "plan-1", Revision: 1}, wantError: "an accepted placement plan requires spec.placement.policy ClusterAffinity; " + remedy},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := &v1beta1.InferenceService{
				Spec: v1beta1.InferenceServiceSpec{
					Engine:    &v1beta1.EngineSpec{},
					Placement: &v1beta1.PlacementSpec{Policy: "ComponentAffinity", Mode: v1beta1.PlacementModeSplit},
				},
				Status: v1beta1.InferenceServiceStatus{Placement: &v1beta1.PlacementStatus{Plan: tt.plan}},
			}
			err := ValidatePlacementIntent(source)
			if err == nil || err.Error() != tt.wantError {
				t.Fatalf("want %q, got %v", tt.wantError, err)
			}
			if tt.plan != nil && !errors.Is(err, ErrAcceptedPlanRequiresClusterAffinity) {
				t.Fatalf("the plan refusal is not the shared error value: %v", err)
			}
		})
	}
}

func TestValidatePlacementIntent_ObsoleteAnnotations_RejectsWithoutMutating(t *testing.T) {
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

// TestValidatePlacementModeUpdate_ModeChanges_RejectsEveryChangeNamingBothModes walks every ordered pair of the four
// modes and the absent block: a pair that keeps the effective mode is
// admitted whatever else changed, every other pair is refused with both
// values named.
func TestValidatePlacementModeUpdate_ModeChanges_RejectsEveryChangeNamingBothModes(t *testing.T) {
	modes := []v1beta1.PlacementMode{"", v1beta1.PlacementModeSingle, v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity}
	spec := func(mode v1beta1.PlacementMode, terms []v1beta1.ClusterAffinityTerm) *v1beta1.InferenceServiceSpec {
		if mode == "" {
			return &v1beta1.InferenceServiceSpec{}
		}
		return &v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: mode, ClusterAffinity: terms}}
	}
	terms := []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: "In", Values: []string{"cluster-a"}}}}}
	word := func(mode v1beta1.PlacementMode) string {
		if mode == "" {
			return "unset (no spec.placement block)"
		}
		return string(mode)
	}
	for _, before := range modes {
		for _, after := range modes {
			t.Run(word(before)+" to "+word(after), func(t *testing.T) {
				// The affinity changes alongside, so the mode alone decides.
				err := ValidatePlacementModeUpdate(spec(before, nil), spec(after, terms))
				if before == after {
					if err != nil {
						t.Fatalf("the mode is kept, want no error, got %v", err)
					}
					return
				}
				if err == nil {
					t.Fatalf("the mode changes from %s to %s, want a refusal", word(before), word(after))
				}
				for _, want := range []string{"spec.placement.mode is fixed for the life of the service", "from " + word(before) + " to " + word(after), ReasonPlacementModeImmutable} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("want %q in %v", want, err)
					}
				}
			})
		}
	}
	t.Run("a block without a mode reads as no mode", func(t *testing.T) {
		if err := ValidatePlacementModeUpdate(&v1beta1.InferenceServiceSpec{Placement: &v1beta1.PlacementSpec{}}, &v1beta1.InferenceServiceSpec{}); err != nil {
			t.Fatal(err)
		}
	})
}
