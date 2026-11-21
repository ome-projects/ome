package v1beta1

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

// leaderGoneGraceRule is the rule every CRD carries on leaderGoneGracePeriod.
const leaderGoneGraceRule = "duration(self) > duration('0s')"

// TestLifecycleSpec_LeaderGoneGracePeriodOnTheWire: the field is a duration
// string under the replica's spec.lifecycle, it is left out when unset, and
// the generated copy does not share the value with its source.
func TestLifecycleSpec_LeaderGoneGracePeriodOnTheWire(t *testing.T) {
	spec := InferenceReplicaSpec{Lifecycle: &LifecycleSpec{LeaderGoneGracePeriod: &metav1.Duration{Duration: 30 * time.Second}}}
	encoded, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(encoded); !strings.Contains(got, `"lifecycle":{"leaderGoneGracePeriod":"30s"}`) {
		t.Fatalf("want lifecycle.leaderGoneGracePeriod as the string 30s, got %s", got)
	}
	var decoded InferenceReplicaSpec
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if lc := decoded.Lifecycle; lc == nil || lc.LeaderGoneGracePeriod == nil || lc.LeaderGoneGracePeriod.Duration != 30*time.Second {
		t.Fatalf("the value did not come back: %+v", lc)
	}

	unset, err := json.Marshal(LifecycleSpec{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(unset), "leaderGoneGracePeriod") {
		t.Fatalf("an unset field must be left out, got %s", unset)
	}

	copied := spec.Lifecycle.DeepCopy()
	if copied.LeaderGoneGracePeriod == spec.Lifecycle.LeaderGoneGracePeriod {
		t.Fatalf("the copy shares the value with its source: the generated copy is out of date, run make manifests")
	}
	if copied.LeaderGoneGracePeriod == nil || *copied.LeaderGoneGracePeriod != *spec.Lifecycle.LeaderGoneGracePeriod {
		t.Fatalf("the copy lost the value: %+v", copied)
	}
}

// TestCRDs_LeaderGoneGracePeriodMustBeAboveZero: every kind that carries the
// lifecycle block, the InferenceReplica, the InferenceService and both
// runtime kinds, has leaderGoneGracePeriod in each copy of the block, as a
// string with the rule that refuses a duration that is not above zero.
func TestCRDs_LeaderGoneGracePeriodMustBeAboveZero(t *testing.T) {
	kinds := []struct {
		file string
		// at is one path the kind is known to carry the block at; empty asks only for one block anywhere.
		at string
	}{
		{file: "ome.io_inferencereplicas.yaml", at: "spec.lifecycle"},
		{file: "ome.io_inferenceservices.yaml", at: "spec.engine.lifecycle"},
		{file: "ome.io_servingruntimes.yaml"},
		{file: "ome.io_clusterservingruntimes.yaml"},
	}
	for _, kind := range kinds {
		blocks := map[string]apiextensionsv1.JSONSchemaProps{}
		collectLifecycleBlocks(loadCRDSchemaFile(t, kind.file), "", blocks)
		paths := make([]string, 0, len(blocks))
		for path := range blocks {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		t.Logf("%s carries the lifecycle block at %v", kind.file, paths)
		if _, found := blocks[kind.at]; len(blocks) == 0 || (kind.at != "" && !found) {
			t.Errorf("%s: want a lifecycle block (at %q), found %v", kind.file, kind.at, paths)
		}
		for _, path := range paths {
			requireLeaderGoneGraceField(t, kind.file+" "+path, blocks[path])
		}
	}
}

// requireLeaderGoneGraceField reads that one copy of the lifecycle block has
// the field, as a string that carries the rule and a message.
func requireLeaderGoneGraceField(t *testing.T, where string, block apiextensionsv1.JSONSchemaProps) {
	t.Helper()
	field, found := block.Properties["leaderGoneGracePeriod"]
	if !found {
		t.Errorf("%s has no leaderGoneGracePeriod: the CRDs are out of date, run make manifests", where)
		return
	}
	if field.Type != "string" {
		t.Errorf("%s.leaderGoneGracePeriod is of type %q, want string", where, field.Type)
	}
	requireSchemaRules(t, where+".leaderGoneGracePeriod", field, leaderGoneGraceRule)
}

// collectLifecycleBlocks walks a schema and notes every OMENative lifecycle
// block by its path. A pod's container also has a property named lifecycle,
// so a block is told by the updateStrategy only the OMENative one has.
func collectLifecycleBlocks(schema apiextensionsv1.JSONSchemaProps, path string, into map[string]apiextensionsv1.JSONSchemaProps) {
	for name, property := range schema.Properties {
		at := strings.TrimPrefix(path+"."+name, ".")
		if _, omenative := property.Properties["updateStrategy"]; name == "lifecycle" && omenative {
			into[at] = property
			continue
		}
		collectLifecycleBlocks(property, at, into)
	}
	if schema.Items != nil && schema.Items.Schema != nil {
		collectLifecycleBlocks(*schema.Items.Schema, path+"[]", into)
	}
	if schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil {
		collectLifecycleBlocks(*schema.AdditionalProperties.Schema, path+"{}", into)
	}
}

// loadCRDSchemaFile reads the schema of one generated CRD by its file name.
func loadCRDSchemaFile(t *testing.T, name string) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "config", "crd", "full", name))
	if err != nil {
		t.Fatalf("read the CRD %s: %v", name, err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(data, crd); err != nil {
		t.Fatalf("parse the CRD %s: %v", name, err)
	}
	for _, version := range crd.Spec.Versions {
		if version.Schema != nil && version.Schema.OpenAPIV3Schema != nil {
			return *version.Schema.OpenAPIV3Schema
		}
	}
	t.Fatalf("the CRD %s has no version with a schema", name)
	return apiextensionsv1.JSONSchemaProps{}
}
