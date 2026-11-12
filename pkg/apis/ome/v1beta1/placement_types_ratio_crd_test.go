package v1beta1

import (
	"strings"
	"testing"
)

const ratioSplitOnlyRule = "!has(self.clusterAffinity) || !self.clusterAffinity.exists(t, has(t.weights)) || (self.mode == 'Split' && !has(self.split))"

func TestInferenceServiceCRD_RatioRules(t *testing.T) {
	root := loadInferenceServiceCRDSchema(t)
	spec := requireSchemaProperty(t, root, "spec")
	placement := requireSchemaProperty(t, spec, "placement")
	requireSchemaRules(t, "spec.placement", placement, ratioSplitOnlyRule)
	for _, v := range placement.XValidations {
		if v.Rule != ratioSplitOnlyRule && containsAny(v.Rule, "has(t.weights.default)") {
			t.Errorf("spec.placement keeps a rule that reads single weight keys: %q", v.Rule)
		}
	}

	affinity := requireSchemaProperty(t, placement, "clusterAffinity")
	weights := requireSchemaProperty(t, *affinity.Items.Schema, "weights")
	if len(weights.Properties) != 4 {
		t.Errorf("weights keys = %d, want 4: only default, engine, decoder and router hold a ratio", len(weights.Properties))
	}
	for _, key := range []string{"default", "engine", "decoder", "router"} {
		ratio := requireSchemaProperty(t, weights, key)
		if ratio.Type != "object" {
			t.Errorf("%s type = %q, want object: every weight key holds a {min, max} ratio", key, ratio.Type)
		}
		if len(ratio.Required) != 0 {
			t.Errorf("%s required = %v, want none: an omitted min or max counts as 1", key, ratio.Required)
		}
		for _, field := range []string{"min", "max"} {
			bound := requireSchemaProperty(t, ratio, field)
			if bound.Minimum == nil || *bound.Minimum != 1 {
				t.Errorf("%s.%s minimum = %v, want 1", key, field, bound.Minimum)
			}
		}
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
