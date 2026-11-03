package v1beta1

import (
	"strings"
	"testing"
)

// A term weighs components only through its weights block, so the CRD carries
// no scalar weight and no rule that reads one.
func TestInferenceServiceCRD_AffinityTermWeighsOnlyThroughWeights(t *testing.T) {
	spec := requireSchemaProperty(t, loadInferenceServiceCRDSchema(t), "spec")
	affinity := requireSchemaProperty(t, requireSchemaProperty(t, spec, "placement"), "clusterAffinity")
	if affinity.Items == nil || affinity.Items.Schema == nil {
		t.Fatal("spec.placement.clusterAffinity has no item schema")
	}
	term := *affinity.Items.Schema

	requireSchemaProperty(t, term, "weights")
	if _, found := term.Properties["weight"]; found {
		t.Error("spec.placement.clusterAffinity[] has a weight property; weights is the only way to weigh a term")
	}
	for _, validation := range term.XValidations {
		if strings.Contains(validation.Rule, "has(self.weight)") {
			t.Errorf("spec.placement.clusterAffinity[] keeps a rule on the scalar weight: %q", validation.Rule)
		}
	}
}
