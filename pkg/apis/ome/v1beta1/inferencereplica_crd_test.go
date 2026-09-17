package v1beta1

import (
	"os"
	"path/filepath"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

// The generated full CRD is the schema a v2-capable manager verifies before
// it starts, so these tests pin the parts of it that the per-Instance
// representation union depends on: the union CEL rules on status, the
// single-value encoding enum, and the typed, atomic, minimum-bounded column
// schema.

func loadInferenceReplicaCRDSchema(t *testing.T, variant string) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "config", "crd", variant, "ome.io_inferencereplicas.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
	if len(crd.Spec.Versions) != 1 || crd.Spec.Versions[0].Schema == nil || crd.Spec.Versions[0].Schema.OpenAPIV3Schema == nil {
		t.Fatalf("%s: expected exactly one served version with a schema", path)
	}
	return *crd.Spec.Versions[0].Schema.OpenAPIV3Schema
}

func TestInferenceReplicaCRD_StatusRepresentationUnionRules(t *testing.T) {
	status := loadInferenceReplicaCRDSchema(t, "full").Properties["status"]

	wantRules := map[string]bool{
		"has(self.instanceStatusEncoding) || !has(self.instanceStatusColumns)":                                  false,
		"!has(self.instanceStatusEncoding) || (has(self.instanceStatusColumns) && !has(self.instanceStatuses))": false,
	}
	for _, rule := range status.XValidations {
		if _, ok := wantRules[rule.Rule]; ok {
			wantRules[rule.Rule] = true
			if rule.Message == "" {
				t.Errorf("union rule %q has no message", rule.Rule)
			}
		}
	}
	for rule, seen := range wantRules {
		if !seen {
			t.Errorf("status schema is missing union rule %q", rule)
		}
	}

	encoding, ok := status.Properties["instanceStatusEncoding"]
	if !ok {
		t.Fatal("status schema is missing instanceStatusEncoding")
	}
	if encoding.Type != "string" || len(encoding.Enum) != 1 || string(encoding.Enum[0].Raw) != `"ColumnarV2"` {
		t.Errorf("instanceStatusEncoding enum = %v, want exactly [\"ColumnarV2\"]", encoding.Enum)
	}

	dense, ok := status.Properties["instanceStatuses"]
	if !ok || dense.XListType == nil || *dense.XListType != "map" {
		t.Errorf("instanceStatuses must remain a list-map keyed by index")
	}
}

func TestInferenceReplicaCRD_ColumnSchema(t *testing.T) {
	status := loadInferenceReplicaCRDSchema(t, "full").Properties["status"]
	columns, ok := status.Properties["instanceStatusColumns"]
	if !ok {
		t.Fatal("status schema is missing instanceStatusColumns")
	}
	if got, want := columns.Required, []string{"members", "phases"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("instanceStatusColumns.required = %v, want %v", got, want)
	}

	assertIndexSetString := func(name string, props apiextensionsv1.JSONSchemaProps) {
		t.Helper()
		if props.Type != "string" {
			t.Errorf("%s type = %q, want string", name, props.Type)
		}
		if props.MinLength == nil || *props.MinLength != 1 {
			t.Errorf("%s minLength = %v, want 1", name, props.MinLength)
		}
		if props.Pattern == "" {
			t.Errorf("%s has no index-set pattern", name)
		}
	}
	assertAtomicList := func(name string, props apiextensionsv1.JSONSchemaProps) {
		t.Helper()
		if props.Type != "array" {
			t.Errorf("%s type = %q, want array", name, props.Type)
		}
		if props.XListType == nil || *props.XListType != "atomic" {
			t.Errorf("%s x-kubernetes-list-type = %v, want atomic", name, props.XListType)
		}
		if props.MinItems == nil || *props.MinItems != 1 {
			t.Errorf("%s minItems = %v, want 1", name, props.MinItems)
		}
	}
	assertGroupItems := func(name string, props apiextensionsv1.JSONSchemaProps, valueType, valueFormat string) apiextensionsv1.JSONSchemaProps {
		t.Helper()
		if props.Items == nil || props.Items.Schema == nil {
			t.Fatalf("%s has no item schema", name)
		}
		item := *props.Items.Schema
		if got := item.Required; len(got) != 2 || got[0] != "indexes" || got[1] != "value" {
			t.Errorf("%s item required = %v, want [indexes value]", name, got)
		}
		assertIndexSetString(name+".indexes", item.Properties["indexes"])
		value := item.Properties["value"]
		if value.Type != valueType || value.Format != valueFormat {
			t.Errorf("%s.value = (%q, %q), want (%q, %q)", name, value.Type, value.Format, valueType, valueFormat)
		}
		return item
	}

	assertIndexSetString("members", columns.Properties["members"])
	assertIndexSetString("admitted", columns.Properties["admitted"])
	assertIndexSetString("activeOrdinalOne", columns.Properties["activeOrdinalOne"])

	for _, name := range []string{
		"rowOrder", "phases", "runningRevisions", "targetRevisions", "incarnations",
		"podCounts", "servingPodCounts", "availablePodCounts", "entries",
	} {
		props, ok := columns.Properties[name]
		if !ok {
			t.Errorf("instanceStatusColumns is missing %s", name)
			continue
		}
		assertAtomicList(name, props)
	}

	rowOrder := columns.Properties["rowOrder"]
	if rowOrder.Items == nil || rowOrder.Items.Schema == nil || rowOrder.Items.Schema.Format != "int32" || rowOrder.Items.Schema.Minimum == nil || *rowOrder.Items.Schema.Minimum != 0 {
		t.Error("rowOrder items must be int32 with minimum 0")
	}

	phaseItem := assertGroupItems("phases", columns.Properties["phases"], "string", "")
	if got := len(phaseItem.Properties["value"].Enum); got != 8 {
		t.Errorf("phases.value enum has %d values, want the 8 Instance phases", got)
	}
	assertGroupItems("runningRevisions", columns.Properties["runningRevisions"], "string", "")
	assertGroupItems("targetRevisions", columns.Properties["targetRevisions"], "string", "")
	if item := assertGroupItems("incarnations", columns.Properties["incarnations"], "integer", "int64"); len(item.XValidations) != 1 || item.XValidations[0].Rule != "self.value != 0" {
		t.Errorf("incarnations item rules = %v, want the nonzero rule", item.XValidations)
	}
	for _, name := range []string{"podCounts", "servingPodCounts", "availablePodCounts"} {
		item := assertGroupItems(name, columns.Properties[name], "integer", "int32")
		if value := item.Properties["value"]; value.Minimum == nil || *value.Minimum != 1 {
			t.Errorf("%s.value minimum = %v, want 1", name, value.Minimum)
		}
	}

	entries := columns.Properties["entries"]
	if entries.Items == nil || entries.Items.Schema == nil {
		t.Fatal("entries has no item schema")
	}
	entry := *entries.Items.Schema
	if len(entry.Required) != 1 || entry.Required[0] != "index" {
		t.Errorf("entry required = %v, want [index]", entry.Required)
	}
	if index := entry.Properties["index"]; index.Format != "int32" || index.Minimum == nil || *index.Minimum != 0 {
		t.Error("entry index must be int32 with minimum 0")
	}
	if len(entry.XValidations) != 1 || entry.XValidations[0].Rule != "has(self.conditions) || has(self.readySince) || has(self.operation) || has(self.lastFailure)" {
		t.Errorf("entry rules = %v, want the nonempty-entry rule", entry.XValidations)
	}
	for _, name := range []string{"conditions", "readySince", "operation", "lastFailure"} {
		if _, ok := entry.Properties[name]; !ok {
			t.Errorf("entry schema is missing %s", name)
		}
	}
	conditions := entry.Properties["conditions"]
	if conditions.XListType == nil || *conditions.XListType != "map" || len(conditions.XListMapKeys) != 1 || conditions.XListMapKeys[0] != "type" {
		t.Error("entry conditions must remain a list-map keyed by type")
	}
	if conditions.MinItems == nil || *conditions.MinItems != 1 {
		t.Errorf("entry conditions minItems = %v, want 1", conditions.MinItems)
	}
}

// The minimal CRD variant keeps an untyped status, so it carries neither the
// union rules nor the column schema; a v2-capable manager must not accept it.
func TestInferenceReplicaCRD_MinimalVariantKeepsUntypedStatus(t *testing.T) {
	status := loadInferenceReplicaCRDSchema(t, "minimal").Properties["status"]
	if status.XPreserveUnknownFields == nil || !*status.XPreserveUnknownFields {
		t.Fatal("minimal CRD status must preserve unknown fields")
	}
	if len(status.Properties) != 0 || len(status.XValidations) != 0 {
		t.Fatalf("minimal CRD status must be untyped, got %d properties and %d rules", len(status.Properties), len(status.XValidations))
	}
}
