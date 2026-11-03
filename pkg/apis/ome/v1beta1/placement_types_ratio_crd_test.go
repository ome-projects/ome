package v1beta1

import "testing"

const (
	ratioNoMixingRule  = "!has(self.clusterAffinity) || !(self.clusterAffinity.exists(t, has(t.weights) && (has(t.weights.defaultReplicas) || has(t.weights.engineReplicas) || has(t.weights.decoderReplicas) || has(t.weights.routerReplicas))) && self.clusterAffinity.exists(t, has(t.weights) && (has(t.weights.default) || has(t.weights.engine) || has(t.weights.decoder) || has(t.weights.router))))"
	ratioSplitOnlyRule = "!has(self.clusterAffinity) || !self.clusterAffinity.exists(t, has(t.weights) && (has(t.weights.defaultReplicas) || has(t.weights.engineReplicas) || has(t.weights.decoderReplicas) || has(t.weights.routerReplicas))) || (self.mode == 'Split' && !has(self.split))"
)

func TestInferenceServiceCRD_RatioMixingRules(t *testing.T) {
	root := loadInferenceServiceCRDSchema(t)
	spec := requireSchemaProperty(t, root, "spec")
	placement := requireSchemaProperty(t, spec, "placement")
	requireSchemaRules(t, "spec.placement", placement, ratioNoMixingRule, ratioSplitOnlyRule)

	affinity := requireSchemaProperty(t, placement, "clusterAffinity")
	weights := requireSchemaProperty(t, *affinity.Items.Schema, "weights")
	for _, key := range []string{"defaultReplicas", "engineReplicas", "decoderReplicas", "routerReplicas"} {
		ratio := requireSchemaProperty(t, weights, key)
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
