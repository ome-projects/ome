package v1beta1

import (
	"os"
	"path/filepath"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"sigs.k8s.io/yaml"
)

func TestKVCachePoolCRD(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("../../../..", "config/crd/full/ome.io_kvcachepools.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatal(err)
	}
	version := crd.Spec.Versions[0]
	if crd.Spec.Scope != apiextensionsv1.NamespaceScoped || version.Subresources == nil || version.Subresources.Status == nil {
		t.Fatal("the pool must be namespaced and expose the status subresource")
	}
	spec := requireSchemaProperty(t, *version.Schema.OpenAPIV3Schema, "spec")
	var internal apiextensions.JSONSchemaProps
	if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(&spec, &internal, nil); err != nil {
		t.Fatal(err)
	}
	validator, _, err := validation.NewSchemaValidator(&internal)
	if err != nil {
		t.Fatal(err)
	}
	structural, err := schema.NewStructural(&internal)
	if err != nil {
		t.Fatal(err)
	}
	rules := cel.NewValidator(structural, false, celconfig.PerCallLimit)
	for _, tc := range []struct {
		name     string
		provider string
		budget   any
		total    bool
		valid    bool
	}{
		{"per-node budget", "LMCache", "32Gi", false, true},
		{"integer bytes", "LMCache", int64(1 << 30), false, true},
		{"zero budget", "LMCache", "0", false, false},
		{"negative budget", "LMCache", "-1Gi", false, false},
		{"pool-wide budget", "LMCache", "32Gi", true, false},
		{"unsupported provider", "HiCache", "32Gi", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := map[string]any{
				"provider":       map[string]any{"name": tc.provider, "config": map[string]any{"l1CapacityPerNode": tc.budget}},
				"deploymentMode": "NodeLocal",
				"workloads":      []any{map[string]any{"name": "server", "nodeSelector": map[string]any{"node-type": "gpu"}}},
			}
			if tc.total {
				obj["cache"] = map[string]any{"capacity": "64Gi"}
			}
			path := field.NewPath("spec")
			errs := validation.ValidateCustomResource(path, obj, validator)
			ruleErrs, _ := rules.Validate(t.Context(), path, structural, obj, nil, celconfig.RuntimeCELCostBudget)
			errs = append(errs, ruleErrs...)
			if (len(errs) == 0) != tc.valid {
				t.Errorf("valid = %v, want %v: %v", len(errs) == 0, tc.valid, errs)
			}
		})
	}
}
