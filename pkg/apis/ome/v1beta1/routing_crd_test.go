package v1beta1

import (
	"os"
	"path/filepath"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

const (
	routingCapacityFactorsRule = "!(has(self.routing) && has(self.routing.capacityFactors) && has(self.placement) && has(self.placement.capacityFactors))"

	routingProbeDisabledRule   = "!(has(self.disabled) && self.disabled) || (!has(self.path) && !has(self.method) && !has(self.acceptStatuses) && !has(self.gateStatuses) && !has(self.period) && !has(self.timeout) && !has(self.failureThreshold) && !has(self.successThreshold) && !has(self.allFailedPolicy))"
	routingProbeCompleteRule   = "(has(self.disabled) && self.disabled) || (has(self.path) && has(self.method) && has(self.acceptStatuses) && has(self.gateStatuses) && has(self.period) && has(self.timeout) && has(self.failureThreshold) && has(self.successThreshold) && has(self.allFailedPolicy))"
	routingPeriodPositiveRule  = "!has(self.period) || duration(self.period) > duration('0s')"
	routingTimeoutPositiveRule = "!has(self.timeout) || duration(self.timeout) > duration('0s')"
	routingTimeoutPeriodRule   = "!has(self.period) || !has(self.timeout) || duration(self.timeout) < duration(self.period)"
	routingProbeSafeGateRule   = "!has(self.gateStatuses) || !self.gateStatuses.exists(s, s == 401 || s == 403 || s == 429)"

	routingCapacityDisabledRule = "!(has(self.disabled) && self.disabled) || (!has(self.path) && !has(self.method) && !has(self.format) && !has(self.options) && !has(self.period) && !has(self.timeout) && !has(self.samples) && !has(self.quorum) && !has(self.maxAge))"
	routingCapacityCompleteRule = "(has(self.disabled) && self.disabled) || (has(self.path) && has(self.method) && has(self.format) && has(self.period) && has(self.timeout) && has(self.samples) && has(self.quorum) && has(self.maxAge))"
	routingCapacityMaxAgeRule   = "!has(self.period) || !has(self.maxAge) || duration(self.maxAge) >= duration(self.period)"
	routingCapacityQuorumRule   = "!has(self.samples) || !has(self.quorum) || self.quorum <= self.samples"

	routingPublisherOptionSizeRule = "self.all(k, size(k) > 0 && size(k) <= 128 && size(self[k]) > 0 && size(self[k]) <= 4096)"
)

func loadInferenceServiceCRDSchema(t *testing.T) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "config", "crd", "full", "ome.io_inferenceservices.yaml")
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

func TestInferenceServiceCRD_RoutingValidationRules(t *testing.T) {
	root := loadInferenceServiceCRDSchema(t)
	spec := requireSchemaProperty(t, root, "spec")
	requireSchemaRules(t, "spec", spec, routingCapacityFactorsRule)

	routing := requireSchemaProperty(t, spec, "routing")
	probe := requireSchemaProperty(t, routing, "probe")
	requireSchemaRules(t, "spec.routing.probe", probe,
		routingProbeDisabledRule,
		routingProbeCompleteRule,
		routingPeriodPositiveRule,
		routingTimeoutPositiveRule,
		routingTimeoutPeriodRule,
		routingProbeSafeGateRule,
	)
	gateStatuses := requireSchemaProperty(t, probe, "gateStatuses")
	if gateStatuses.MaxItems == nil || *gateStatuses.MaxItems != 500 {
		t.Errorf("spec.routing.probe.gateStatuses maxItems = %v, want 500", gateStatuses.MaxItems)
	}

	capacity := requireSchemaProperty(t, routing, "capacity")
	requireSchemaRules(t, "spec.routing.capacity", capacity,
		routingCapacityDisabledRule,
		routingCapacityCompleteRule,
		routingPeriodPositiveRule,
		routingTimeoutPositiveRule,
		routingTimeoutPeriodRule,
		routingCapacityMaxAgeRule,
		routingCapacityQuorumRule,
	)
	publisher := requireSchemaProperty(t, routing, "publisher")
	options := requireSchemaProperty(t, publisher, "options")
	requireSchemaRules(t, "spec.routing.publisher.options", options, routingPublisherOptionSizeRule)
	if options.MaxProperties == nil || *options.MaxProperties != MaxRoutingPublisherOptions {
		t.Errorf("spec.routing.publisher.options maxProperties = %v, want %d", options.MaxProperties, MaxRoutingPublisherOptions)
	}

	for name, schema := range map[string]apiextensionsv1.JSONSchemaProps{
		"spec.routing.probe.method":    requireSchemaProperty(t, probe, "method"),
		"spec.routing.capacity.method": requireSchemaProperty(t, capacity, "method"),
	} {
		if len(schema.Enum) != 3 {
			t.Errorf("%s enum = %v, want exactly GET, HEAD, and POST", name, schema.Enum)
		}
		want := map[string]bool{`"GET"`: false, `"HEAD"`: false, `"POST"`: false}
		for _, value := range schema.Enum {
			if _, found := want[string(value.Raw)]; found {
				want[string(value.Raw)] = true
			}
		}
		for value, found := range want {
			if !found {
				t.Errorf("%s enum %v is missing %s", name, schema.Enum, value)
			}
		}
	}
}

func requireSchemaProperty(t *testing.T, parent apiextensionsv1.JSONSchemaProps, name string) apiextensionsv1.JSONSchemaProps {
	t.Helper()
	property, found := parent.Properties[name]
	if !found {
		t.Fatalf("schema is missing property %q", name)
	}
	return property
}

func requireSchemaRules(t *testing.T, name string, schema apiextensionsv1.JSONSchemaProps, rules ...string) {
	t.Helper()
	want := make(map[string]bool, len(rules))
	for _, rule := range rules {
		want[rule] = false
	}
	for _, validation := range schema.XValidations {
		if _, found := want[validation.Rule]; found {
			want[validation.Rule] = true
			if validation.Message == "" {
				t.Errorf("%s validation %q has no message", name, validation.Rule)
			}
		}
	}
	for rule, found := range want {
		if !found {
			t.Errorf("%s schema is missing validation rule %q", name, rule)
		}
	}
}
