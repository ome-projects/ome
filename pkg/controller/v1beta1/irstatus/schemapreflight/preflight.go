// Package schemapreflight verifies, before a manager registers any
// controller or webhook, that the served InferenceReplica schema carries the
// ColumnarV2 representation union. A manager that can decode columns must
// never run against a cluster whose schema cannot store or validate them.
package schemapreflight

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"k8s.io/client-go/openapi"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// +kubebuilder:rbac:urls=/openapi/v3;/openapi/v3/*,verbs=get

const (
	jsonContentType    = "application/json"
	inferenceReplica   = "InferenceReplica"
	statusProperty     = "status"
	denseField         = "instanceStatuses"
	encodingField      = "instanceStatusEncoding"
	columnsField       = "instanceStatusColumns"
	listTypeExtension  = "x-kubernetes-list-type"
	groupVersionKindEx = "x-kubernetes-group-version-kind"
)

var (
	// groupVersionPath is the discovery key of the ome.io/v1beta1 document.
	groupVersionPath = "apis/" + v1beta1.SchemeGroupVersion.Group + "/" + v1beta1.SchemeGroupVersion.Version
	// statusSubresource is the served path of the InferenceReplica status
	// subresource inside that document.
	statusSubresource = "/" + groupVersionPath + "/namespaces/{namespace}/inferencereplicas/{name}/status"
)

// representationUnionRules are the CEL rules the status schema must carry so
// the API server itself rejects a status with both or neither representation.
var representationUnionRules = []string{
	"has(self.instanceStatusEncoding) || !has(self.instanceStatusColumns)",
	"!has(self.instanceStatusEncoding) || (has(self.instanceStatusColumns) && !has(self.instanceStatuses))",
}

// indexSetProperties are the status column properties stored as canonical
// index-set strings.
var indexSetProperties = []string{"members", "admitted", "activeOrdinalOne"}

// groupColumns are the {value, indexes} group list properties.
var groupColumns = []string{"phases", "runningRevisions", "targetRevisions", "incarnations", "podCounts", "servingPodCounts", "availablePodCounts"}

// atomicListProperties are every status column property stored as a list;
// all of them are atomic with at least one element when present.
var atomicListProperties = append(append([]string{}, groupColumns...), "rowOrder", "entries")

// requiredColumnProperties are the column properties the schema must mark
// required: the row set and the phase coverage have no default.
var requiredColumnProperties = []string{"members", "phases"}

// ErrSchemaIncompatible marks every preflight failure so callers can
// distinguish a schema problem from a discovery transport error.
var ErrSchemaIncompatible = errors.New("InferenceReplica status schema is not ColumnarV2-capable")

// Verify fetches the ome.io/v1beta1 OpenAPI v3 document through discovery
// and checks the InferenceReplica status schema: the status subresource, the
// retained dense field, the exact marker enum, the typed column properties
// with their minimums and atomic lists, and the representation-union CEL
// rules. Any missing, stale, minimal, or malformed schema returns an error
// wrapping ErrSchemaIncompatible.
func Verify(client openapi.Client) error {
	if client == nil {
		return fmt.Errorf("%w: no OpenAPI v3 discovery client", ErrSchemaIncompatible)
	}
	paths, err := client.Paths()
	if err != nil {
		return fmt.Errorf("list OpenAPI v3 discovery paths: %w", err)
	}
	groupVersion, ok := paths[groupVersionPath]
	if !ok {
		return fmt.Errorf("%w: discovery does not serve %s; the InferenceReplica CRD is not installed or OpenAPI discovery is forbidden", ErrSchemaIncompatible, groupVersionPath)
	}
	document, err := groupVersion.Schema(jsonContentType)
	if err != nil {
		return fmt.Errorf("fetch OpenAPI v3 document for %s: %w", groupVersionPath, err)
	}
	return VerifyDocument(document)
}

// VerifyDocument runs the schema checks against one OpenAPI v3 document for
// the ome.io/v1beta1 group version.
func VerifyDocument(document []byte) error {
	var doc struct {
		Paths      map[string]json.RawMessage `json:"paths"`
		Components struct {
			Schemas map[string]schema `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(document, &doc); err != nil {
		return fmt.Errorf("%w: parse OpenAPI v3 document: %v", ErrSchemaIncompatible, err)
	}
	if _, ok := doc.Paths[statusSubresource]; !ok {
		return fmt.Errorf("%w: status subresource path %s is not served", ErrSchemaIncompatible, statusSubresource)
	}
	root, ok := findInferenceReplicaSchema(doc.Components.Schemas)
	if !ok {
		return fmt.Errorf("%w: no component schema carries %s for %s/%s", ErrSchemaIncompatible, groupVersionKindEx, v1beta1.SchemeGroupVersion.String(), inferenceReplica)
	}
	status, ok := root.Properties[statusProperty]
	if !ok {
		return fmt.Errorf("%w: schema has no %s property", ErrSchemaIncompatible, statusProperty)
	}
	if len(status.Properties) == 0 {
		return fmt.Errorf("%w: %s is untyped (no properties); a minimal CRD cannot host a ColumnarV2-capable manager", ErrSchemaIncompatible, statusProperty)
	}
	if err := verifyStatus(status); err != nil {
		return fmt.Errorf("%w: %v", ErrSchemaIncompatible, err)
	}
	return nil
}

// schema is the subset of an OpenAPI v3 schema object the preflight reads.
type schema struct {
	Type        string             `json:"type"`
	Enum        []json.RawMessage  `json:"enum"`
	Properties  map[string]schema  `json:"properties"`
	Items       *schema            `json:"items"`
	Required    []string           `json:"required"`
	MinItems    *int64             `json:"minItems"`
	MinLength   *int64             `json:"minLength"`
	Minimum     *float64           `json:"minimum"`
	Pattern     string             `json:"pattern"`
	ListType    string             `json:"x-kubernetes-list-type"`
	Validations []validationRule   `json:"x-kubernetes-validations"`
	GVKs        []groupVersionKind `json:"x-kubernetes-group-version-kind"`
}

type validationRule struct {
	Rule string `json:"rule"`
}

type groupVersionKind struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

func findInferenceReplicaSchema(schemas map[string]schema) (schema, bool) {
	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		for _, gvk := range schemas[name].GVKs {
			if gvk.Group == v1beta1.SchemeGroupVersion.Group && gvk.Version == v1beta1.SchemeGroupVersion.Version && gvk.Kind == inferenceReplica {
				return schemas[name], true
			}
		}
	}
	return schema{}, false
}

func verifyStatus(status schema) error {
	dense, ok := status.Properties[denseField]
	if !ok || dense.Type != "array" {
		return fmt.Errorf("retained dense field %s.%s is missing or not an array", statusProperty, denseField)
	}
	if err := verifyMarker(status); err != nil {
		return err
	}
	columns, ok := status.Properties[columnsField]
	if !ok {
		return fmt.Errorf("%s.%s is missing", statusProperty, columnsField)
	}
	if err := verifyColumns(columns); err != nil {
		return fmt.Errorf("%s.%s: %w", statusProperty, columnsField, err)
	}
	for _, rule := range representationUnionRules {
		if !hasRule(status.Validations, rule) {
			return fmt.Errorf("%s lacks the representation-union rule %q", statusProperty, rule)
		}
	}
	return nil
}

func verifyMarker(status schema) error {
	marker, ok := status.Properties[encodingField]
	if !ok {
		return fmt.Errorf("%s.%s is missing", statusProperty, encodingField)
	}
	if marker.Type != "string" {
		return fmt.Errorf("%s.%s has type %q, want string", statusProperty, encodingField, marker.Type)
	}
	var values []string
	for _, raw := range marker.Enum {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("%s.%s enum carries a non-string value %s", statusProperty, encodingField, string(raw))
		}
		values = append(values, value)
	}
	want := []string{string(v1beta1.InstanceStatusEncodingColumnarV2)}
	if strings.Join(values, ",") != strings.Join(want, ",") {
		return fmt.Errorf("%s.%s enum is %v, want %v", statusProperty, encodingField, values, want)
	}
	return nil
}

func verifyColumns(columns schema) error {
	if columns.Type != "object" {
		return fmt.Errorf("type is %q, want object", columns.Type)
	}
	for _, name := range requiredColumnProperties {
		if !contains(columns.Required, name) {
			return fmt.Errorf("%s is not required", name)
		}
	}
	for _, name := range indexSetProperties {
		property, ok := columns.Properties[name]
		if !ok {
			return fmt.Errorf("index-set property %s is missing", name)
		}
		if err := verifyIndexSet(property); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	for _, name := range atomicListProperties {
		property, ok := columns.Properties[name]
		if !ok {
			return fmt.Errorf("list property %s is missing", name)
		}
		if property.Type != "array" {
			return fmt.Errorf("%s has type %q, want array", name, property.Type)
		}
		if property.ListType != "atomic" {
			return fmt.Errorf("%s has %s %q, want atomic", name, listTypeExtension, property.ListType)
		}
		if property.MinItems == nil || *property.MinItems != 1 {
			return fmt.Errorf("%s lacks minItems 1", name)
		}
		if property.Items == nil {
			return fmt.Errorf("%s has no items schema", name)
		}
	}
	for _, name := range groupColumns {
		items := columns.Properties[name].Items
		indexes, ok := items.Properties["indexes"]
		if !ok {
			return fmt.Errorf("%s items lack the indexes index set", name)
		}
		if err := verifyIndexSet(indexes); err != nil {
			return fmt.Errorf("%s items indexes: %w", name, err)
		}
		if _, ok := items.Properties["value"]; !ok {
			return fmt.Errorf("%s items lack value", name)
		}
	}
	for _, name := range []string{"podCounts", "servingPodCounts", "availablePodCounts"} {
		value := columns.Properties[name].Items.Properties["value"]
		if value.Minimum == nil || *value.Minimum != 1 {
			return fmt.Errorf("%s items value lacks minimum 1", name)
		}
	}
	if len(columns.Properties["incarnations"].Items.Validations) == 0 {
		return errors.New("incarnations items lack the nonzero value rule")
	}
	entries := columns.Properties["entries"].Items
	if _, ok := entries.Properties["index"]; !ok {
		return errors.New("entries items lack index")
	}
	if len(entries.Validations) == 0 {
		return errors.New("entries items lack the non-empty entry rule")
	}
	return nil
}

func verifyIndexSet(property schema) error {
	if property.Type != "string" {
		return fmt.Errorf("type is %q, want string", property.Type)
	}
	if property.MinLength == nil || *property.MinLength != 1 {
		return errors.New("lacks minLength 1")
	}
	if property.Pattern == "" {
		return errors.New("lacks the index-set pattern")
	}
	return nil
}

func hasRule(rules []validationRule, want string) bool {
	for _, rule := range rules {
		if rule.Rule == want {
			return true
		}
	}
	return false
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
