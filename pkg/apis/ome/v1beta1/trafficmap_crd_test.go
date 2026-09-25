package v1beta1

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

func TestTrafficMapCRD_EntrySchema(t *testing.T) {
	root := loadTrafficMapCRDSchema(t)
	spec := requireSchemaProperty(t, root, "spec")
	entries := requireSchemaProperty(t, spec, "entries")
	if entries.Items == nil || entries.Items.Schema == nil {
		t.Fatal("spec.entries has no item schema")
	}
	weight := requireSchemaProperty(t, *entries.Items.Schema, "weight")
	if weight.Minimum == nil || *weight.Minimum != 0 {
		t.Errorf("spec.entries[].weight minimum = %v, want 0", weight.Minimum)
	}
	if weight.Maximum == nil || *weight.Maximum != 1_000_000 {
		t.Errorf("spec.entries[].weight maximum = %v, want 1000000", weight.Maximum)
	}
	entry := *entries.Items.Schema
	capacity := requireSchemaProperty(t, entry, "capacity")
	factor := requireSchemaProperty(t, capacity, "factor")
	if factor.Default != nil {
		t.Errorf("spec.entries[].capacity.factor default = %s, want none to avoid defaulting/update churn", factor.Default.Raw)
	}
	probe := requireSchemaProperty(t, entry, "probe")
	requireSchemaProperty(t, probe, "gated")
	policyDigest := requireSchemaProperty(t, probe, "policyDigest")
	if got, want := policyDigest.Pattern, `^sha256:[0-9a-f]{64}$`; got != want {
		t.Errorf("spec.entries[].probe.policyDigest pattern = %q, want %q", got, want)
	}
}

func TestTrafficMapCRD_PublisherStatusJournal(t *testing.T) {
	root := loadTrafficMapCRDSchema(t)
	status := requireSchemaProperty(t, root, "status")
	requireTrafficMapOptionalProperties(t, "status", status, "published", "publisher")
	if _, found := status.Properties["programmed"]; found {
		t.Error("status.programmed must not be in the TrafficMap CRD schema")
	}
	publisher := requireSchemaProperty(t, status, "publisher")
	requireTrafficMapRequiredProperties(t, "status.publisher", publisher, "publisherName")
	requireTrafficMapOptionalProperties(t, "status.publisher", publisher, "claimedTargets", "observedOptionsDigest")

	name := requireSchemaProperty(t, publisher, "publisherName")
	requireTrafficMapLengthBounds(t, "status.publisher.publisherName", name, 1, MaxTrafficMapPublisherNameLength)

	requireTrafficMapStringSetBounds(t, "status.publisher.claimedTargets",
		requireSchemaProperty(t, publisher, "claimedTargets"),
	)
	optionsDigest := requireSchemaProperty(t, publisher, "observedOptionsDigest")
	if got, want := optionsDigest.Pattern, `^sha256:[0-9a-f]{64}$`; got != want {
		t.Errorf("status.publisher.observedOptionsDigest pattern = %q, want %q", got, want)
	}

	if publisher.Default != nil {
		t.Errorf("status.publisher default = %s, want none", publisher.Default.Raw)
	}
}

func loadTrafficMapCRDSchema(t *testing.T) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "config", "crd", "full", "ome.io_trafficmaps.yaml")
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

func requireTrafficMapOptionalProperties(
	t *testing.T,
	path string,
	schema apiextensionsv1.JSONSchemaProps,
	names ...string,
) {
	t.Helper()
	for _, name := range names {
		if slices.Contains(schema.Required, name) {
			t.Errorf("%s required fields = %v, want %q optional", path, schema.Required, name)
		}
	}
}

func requireTrafficMapRequiredProperties(
	t *testing.T,
	path string,
	schema apiextensionsv1.JSONSchemaProps,
	names ...string,
) {
	t.Helper()
	for _, name := range names {
		if !slices.Contains(schema.Required, name) {
			t.Errorf("%s required fields = %v, missing %q", path, schema.Required, name)
		}
	}
}

func requireTrafficMapLengthBounds(
	t *testing.T,
	path string,
	schema apiextensionsv1.JSONSchemaProps,
	wantMin int64,
	wantMax int,
) {
	t.Helper()
	if schema.MinLength == nil || *schema.MinLength != wantMin {
		t.Errorf("%s minLength = %v, want %d", path, schema.MinLength, wantMin)
	}
	if schema.MaxLength == nil || *schema.MaxLength != int64(wantMax) {
		t.Errorf("%s maxLength = %v, want %d", path, schema.MaxLength, wantMax)
	}
}

func requireTrafficMapStringSetBounds(
	t *testing.T,
	path string,
	schema apiextensionsv1.JSONSchemaProps,
) {
	t.Helper()
	if schema.XListType == nil || *schema.XListType != "set" {
		t.Errorf("%s list type = %v, want set", path, schema.XListType)
	}
	if schema.MinItems == nil || *schema.MinItems != 1 {
		t.Errorf("%s minItems = %v, want 1", path, schema.MinItems)
	}
	if schema.MaxItems == nil || *schema.MaxItems != int64(MaxTrafficMapPublisherTargets) {
		t.Errorf("%s maxItems = %v, want %d", path, schema.MaxItems, MaxTrafficMapPublisherTargets)
	}
	if schema.Items == nil || schema.Items.Schema == nil {
		t.Fatalf("%s has no item schema", path)
	}
	requireTrafficMapLengthBounds(t, path+"[]", *schema.Items.Schema, 1, MaxTrafficMapPublisherTargetLength)
}
