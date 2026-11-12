package validation

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/affinity"
)

var docsYAMLBlock = regexp.MustCompile("(?s)```yaml\n(.*?)```")

// docsIntegerWeight finds a weight key that holds an integer, inline or on its
// own line under a weights block.
var docsIntegerWeight = regexp.MustCompile(`(?m)weights:\s*\{[^\n]*\b(default|engine|decoder|router):\s*\d|weights:\s*\n(\s+\w+:.*\n)*?\s+(default|engine|decoder|router):\s*\d`)

// docsAliasKey finds a *Replicas weight key, which OME does not read.
var docsAliasKey = regexp.MustCompile(`\b(default|engine|decoder|router)Replicas:`)

// The number of complete InferenceService examples in the placement docs that
// set weights.
const docsRatioExamples = 3

// Every complete InferenceService example in the placement docs must decode
// strictly and pass the checks the API server applies to weights: weights only
// in Split without a split block. No YAML block, fragments included, gives a
// weight key an integer or uses a *Replicas key.
func TestPlacementDocsExamplesDoNotMixForms(t *testing.T) {
	doc, err := os.ReadFile("../../docs/multicluster-placement.md")
	if err != nil {
		t.Fatal(err)
	}
	withRatios := 0
	for i, m := range docsYAMLBlock.FindAllSubmatch(doc, -1) {
		if found := docsIntegerWeight.Find(m[1]); found != nil {
			t.Errorf("yaml block %d gives a weight key an integer: %q", i+1, found)
		}
		if found := docsAliasKey.Find(m[1]); found != nil {
			t.Errorf("yaml block %d uses a *Replicas key: %q", i+1, found)
		}
		if !strings.Contains(string(m[1]), "kind: InferenceService") {
			continue
		}
		var isvc v1beta1.InferenceService
		if err := yaml.UnmarshalStrict(m[1], &isvc); err != nil {
			t.Errorf("yaml block %d does not decode as an InferenceService: %v", i+1, err)
			continue
		}
		if err := droppedKeys(m[1], &isvc); err != nil {
			t.Errorf("yaml block %d (%s): %v", i+1, isvc.Name, err)
			continue
		}
		if isvc.Spec.Placement == nil {
			continue
		}
		if err := ValidateRatioForm(isvc.Spec.Placement); err != nil {
			t.Errorf("yaml block %d (%s): %v", i+1, isvc.Name, err)
		}
		if affinity.UsesRatios(isvc.Spec.Placement.ClusterAffinity) {
			withRatios++
		}
	}
	if withRatios < docsRatioExamples {
		t.Fatalf("found %d InferenceService examples that set weights, want at least %d", withRatios, docsRatioExamples)
	}
}

// The integer and alias checks catch what they are meant to catch.
func TestPlacementDocsPatterns(t *testing.T) {
	for _, block := range []string{
		"- weights: {engine: 3, decoder: 1}\n",
		"- weights: {engine: {min: 3}, decoder: 1}\n",
		"weights:\n  engine: {min: 1}\n  default: 3\n",
	} {
		if docsIntegerWeight.FindString(block) == "" {
			t.Errorf("integer weight not found in %q", block)
		}
	}
	for _, block := range []string{
		"- weights: {engine: {min: 3}, decoder: {min: 1}}\n",
		"weights:\n  default: {min: 1}\nmatchFields: []\nreplicas: 3\n",
	} {
		if found := docsIntegerWeight.FindString(block); found != "" {
			t.Errorf("integer weight %q reported in %q", found, block)
		}
	}
	if docsAliasKey.FindString("weights: {engineReplicas: {min: 1}}") == "" {
		t.Error("*Replicas key not found")
	}
}

// droppedKeys reports a key of the example that the decoded object does not
// carry. Strict decoding does not reach inside PlacementSpec, which has its own
// decoder, so a misspelled key there would otherwise vanish.
func droppedKeys(src []byte, decoded *v1beta1.InferenceService) error {
	orig, err := yaml.YAMLToJSON(src)
	if err != nil {
		return err
	}
	round, err := json.Marshal(decoded)
	if err != nil {
		return err
	}
	var a, b any
	if err := json.Unmarshal(orig, &a); err != nil {
		return err
	}
	if err := json.Unmarshal(round, &b); err != nil {
		return err
	}
	return missingKey("", a, b)
}

func missingKey(path string, want, got any) error {
	switch w := want.(type) {
	case map[string]any:
		g, _ := got.(map[string]any)
		for k, v := range w {
			gv, ok := g[k]
			if !ok {
				return fmt.Errorf("key %s.%s is unknown and was dropped", path, k)
			}
			if err := missingKey(path+"."+k, v, gv); err != nil {
				return err
			}
		}
	case []any:
		g, _ := got.([]any)
		for i, v := range w {
			if i >= len(g) {
				return fmt.Errorf("list %s lost element %d", path, i)
			}
			if err := missingKey(fmt.Sprintf("%s[%d]", path, i), v, g[i]); err != nil {
				return err
			}
		}
	}
	return nil
}
