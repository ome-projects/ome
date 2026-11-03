package v1beta1

import (
	"strings"
	"testing"
)

// split.maxPerCluster caps each component by key. It is the only per-cluster
// cap, so split carries no maxReplicasPerCluster and no rule that reads one.
func TestInferenceServiceCRD_SplitCapsEachComponent(t *testing.T) {
	spec := requireSchemaProperty(t, loadInferenceServiceCRDSchema(t), "spec")
	split := requireSchemaProperty(t, requireSchemaProperty(t, spec, "placement"), "split")
	ceilings := requireSchemaProperty(t, split, "maxPerCluster")
	if ceilings.MinProperties == nil || *ceilings.MinProperties != 1 {
		t.Errorf("split.maxPerCluster minProperties = %v, want 1", ceilings.MinProperties)
	}
	for _, key := range []string{"default", "engine", "decoder", "router"} {
		cap := requireSchemaProperty(t, ceilings, key)
		if cap.Minimum == nil || *cap.Minimum != 1 {
			t.Errorf("split.maxPerCluster.%s minimum = %v, want 1", key, cap.Minimum)
		}
	}
	if len(ceilings.Properties) != 4 {
		t.Errorf("split.maxPerCluster has %d keys, want default, engine, decoder and router", len(ceilings.Properties))
	}
	if _, found := split.Properties["maxReplicasPerCluster"]; found {
		t.Error("split has a maxReplicasPerCluster property; maxPerCluster is the only per-cluster cap")
	}
	for _, validation := range split.XValidations {
		if strings.Contains(validation.Rule, "maxReplicasPerCluster") {
			t.Errorf("split keeps a rule on maxReplicasPerCluster: %q", validation.Rule)
		}
	}
}
