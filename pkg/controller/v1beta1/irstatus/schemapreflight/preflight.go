// Package schemapreflight verifies, before a manager registers any
// controller or webhook, that the served InferenceReplica schema carries the
// ColumnarV2 representation union. A manager that can decode columns must
// never run against a cluster whose schema cannot store or validate them.
// The three readySince columns are the one part the schema may lack: the
// manager then starts and stores each Instance's readySince in its entry,
// the form every schema keeps, and says so where an operator looks.
package schemapreflight

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"k8s.io/client-go/openapi"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
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
// index-set strings that every served schema must carry.
var indexSetProperties = []string{"members", "admitted", "activeOrdinalOne"}

// The readySince columns: a base time, an index set, a list of whole seconds
// from the base held to zero or more, and the CEL rule that keeps the three
// together. A schema without them would drop the columns from every write,
// and with them every row's readySince, without an error; the writer keeps
// the times in the entries under such a schema instead.
const (
	readySinceBaseProperty    = "readySinceBase"
	readySinceIndexesProperty = "readySinceIndexes"
	readySinceOffsetsProperty = "readySinceOffsets"
	readySinceColumnsRule     = "has(self.readySinceBase) == has(self.readySinceIndexes) && has(self.readySinceIndexes) == has(self.readySinceOffsets)"
)

// readySinceColumnProperties are the three readySince columns in the order
// a report names them.
var readySinceColumnProperties = []string{readySinceBaseProperty, readySinceIndexesProperty, readySinceOffsetsProperty}

// groupColumns are the {value, indexes} group list properties.
var groupColumns = []string{"phases", "runningRevisions", "targetRevisions", "incarnations", "podCounts", "servingPodCounts", "availablePodCounts"}

// countColumns are the group columns whose value is a pod count, so the
// schema must hold each value to at least one.
var countColumns = []string{"podCounts", "servingPodCounts", "availablePodCounts"}

// atomicListProperties are the status column properties stored as a list
// that every served schema must carry; all of them are atomic with at least
// one element when present.
var atomicListProperties = append(append([]string{}, groupColumns...), "rowOrder", "entries")

// requiredColumnProperties are the column properties the schema must mark
// required: the row set and the phase coverage have no default.
var requiredColumnProperties = []string{"members", "phases"}

// ErrSchemaIncompatible marks every preflight failure so callers can
// distinguish a schema problem from a discovery transport error.
var ErrSchemaIncompatible = errors.New("InferenceReplica status schema is not ColumnarV2-capable")

// Capabilities is what a served schema that passed the preflight stores
// beyond the ColumnarV2 union every manager requires.
type Capabilities struct {
	// MissingReadySinceColumns names the readySince columns the served
	// schema lacks, in the order readySinceBase, readySinceIndexes,
	// readySinceOffsets; empty when it stores all three. A manager starts
	// without them and stores each Instance's readySince in its entry.
	MissingReadySinceColumns []string
}

// ReadySinceForm is where a status writer stores each Instance's readySince
// under this schema: the columns when the schema stores all three, the
// entries otherwise.
func (c Capabilities) ReadySinceForm() irstatus.ReadySinceForm {
	if len(c.MissingReadySinceColumns) > 0 {
		return irstatus.ReadySinceInEntries
	}
	return irstatus.ReadySinceInColumns
}

// Verify fetches the ome.io/v1beta1 OpenAPI v3 document through discovery
// and checks the InferenceReplica status schema: the status subresource, the
// retained dense field, the exact encoding enum, the typed column properties
// with their minimums and atomic lists, and the representation-union CEL
// rules. Any missing, stale, minimal, or malformed schema returns an error
// wrapping ErrSchemaIncompatible; a schema that lacks the readySince columns
// passes and names them in the capabilities.
func Verify(client openapi.Client) (Capabilities, error) {
	document, err := Document(client)
	if err != nil {
		return Capabilities{}, err
	}
	return VerifyDocument(document)
}

// Document fetches the ome.io/v1beta1 OpenAPI v3 document through discovery.
// A group version discovery does not serve is an ErrSchemaIncompatible; a
// transport failure is returned as is.
func Document(client openapi.Client) ([]byte, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: no OpenAPI v3 discovery client", ErrSchemaIncompatible)
	}
	paths, err := client.Paths()
	if err != nil {
		return nil, fmt.Errorf("list OpenAPI v3 discovery paths: %w", err)
	}
	groupVersion, ok := paths[groupVersionPath]
	if !ok {
		return nil, fmt.Errorf("%w: discovery does not serve %s; the InferenceReplica CRD is not installed or OpenAPI discovery is forbidden", ErrSchemaIncompatible, groupVersionPath)
	}
	document, err := groupVersion.Schema(jsonContentType)
	if err != nil {
		return nil, fmt.Errorf("fetch OpenAPI v3 document for %s: %w", groupVersionPath, err)
	}
	return document, nil
}

// VerifyDocument runs the schema checks against one OpenAPI v3 document for
// the ome.io/v1beta1 group version.
func VerifyDocument(document []byte) (Capabilities, error) {
	var doc struct {
		Paths      map[string]json.RawMessage `json:"paths"`
		Components struct {
			Schemas map[string]schema `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(document, &doc); err != nil {
		return Capabilities{}, fmt.Errorf("%w: parse OpenAPI v3 document: %v", ErrSchemaIncompatible, err)
	}
	if _, ok := doc.Paths[statusSubresource]; !ok {
		return Capabilities{}, fmt.Errorf("%w: status subresource path %s is not served", ErrSchemaIncompatible, statusSubresource)
	}
	root, ok := findInferenceReplicaSchema(doc.Components.Schemas)
	if !ok {
		return Capabilities{}, fmt.Errorf("%w: no component schema carries %s for %s/%s", ErrSchemaIncompatible, groupVersionKindEx, v1beta1.SchemeGroupVersion.String(), inferenceReplica)
	}
	status, ok := root.Properties[statusProperty]
	if !ok {
		return Capabilities{}, fmt.Errorf("%w: schema has no %s property", ErrSchemaIncompatible, statusProperty)
	}
	if len(status.Properties) == 0 {
		return Capabilities{}, fmt.Errorf("%w: %s is untyped (no properties); a minimal CRD cannot host a ColumnarV2-capable manager", ErrSchemaIncompatible, statusProperty)
	}
	capabilities, err := verifyStatus(status)
	if err != nil {
		return Capabilities{}, fmt.Errorf("%w: %v", ErrSchemaIncompatible, err)
	}
	return capabilities, nil
}

// schema is the subset of an OpenAPI v3 schema object the preflight reads.
type schema struct {
	Type        string             `json:"type"`
	Format      string             `json:"format"`
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

func verifyStatus(status schema) (Capabilities, error) {
	dense, ok := status.Properties[denseField]
	if !ok || dense.Type != "array" {
		return Capabilities{}, fmt.Errorf("retained dense field %s.%s is missing or not an array", statusProperty, denseField)
	}
	if err := verifyEncodingField(status); err != nil {
		return Capabilities{}, err
	}
	columns, ok := status.Properties[columnsField]
	if !ok {
		return Capabilities{}, fmt.Errorf("%s.%s is missing", statusProperty, columnsField)
	}
	capabilities, err := verifyColumns(columns)
	if err != nil {
		return Capabilities{}, fmt.Errorf("%s.%s: %w", statusProperty, columnsField, err)
	}
	for _, rule := range representationUnionRules {
		if !hasRule(status.Validations, rule) {
			return Capabilities{}, fmt.Errorf("%s lacks the representation-union rule %q", statusProperty, rule)
		}
	}
	return capabilities, nil
}

// verifyEncodingField requires the encoding field to be a string whose enum
// is exactly the ColumnarV2 value.
func verifyEncodingField(status schema) error {
	encoding, ok := status.Properties[encodingField]
	if !ok {
		return fmt.Errorf("%s.%s is missing", statusProperty, encodingField)
	}
	if encoding.Type != "string" {
		return fmt.Errorf("%s.%s has type %q, want string", statusProperty, encodingField, encoding.Type)
	}
	var values []string
	for _, raw := range encoding.Enum {
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

// verifyColumns checks the column object property by property: the required
// ones, the index sets, every list with its atomic shape, the group lists
// with their value and index set, the pod counts held to one, the nonzero
// rule on the recreation counts, the readySince columns and the shape of the
// entries. It reports which readySince columns the schema lacks.
func verifyColumns(columns schema) (Capabilities, error) {
	if columns.Type != "object" {
		return Capabilities{}, fmt.Errorf("type is %q, want object", columns.Type)
	}
	for _, name := range requiredColumnProperties {
		if !slices.Contains(columns.Required, name) {
			return Capabilities{}, fmt.Errorf("%s is not required", name)
		}
	}
	for _, name := range indexSetProperties {
		property, ok := columns.Properties[name]
		if !ok {
			return Capabilities{}, fmt.Errorf("index-set property %s is missing", name)
		}
		if err := verifyIndexSet(property); err != nil {
			return Capabilities{}, fmt.Errorf("%s: %w", name, err)
		}
	}
	for _, name := range atomicListProperties {
		if err := verifyAtomicList(name, columns.Properties); err != nil {
			return Capabilities{}, err
		}
	}
	for _, name := range groupColumns {
		if err := verifyGroupItems(name, columns.Properties[name].Items); err != nil {
			return Capabilities{}, err
		}
	}
	for _, name := range countColumns {
		value := columns.Properties[name].Items.Properties["value"]
		if value.Minimum == nil || *value.Minimum != 1 {
			return Capabilities{}, fmt.Errorf("%s items value lacks minimum 1", name)
		}
	}
	if len(columns.Properties["incarnations"].Items.Validations) == 0 {
		return Capabilities{}, errors.New("incarnations items lack the nonzero value rule")
	}
	missing, err := verifyReadySinceColumns(columns)
	if err != nil {
		return Capabilities{}, err
	}
	if err := verifyEntries(columns.Properties["entries"].Items); err != nil {
		return Capabilities{}, err
	}
	return Capabilities{MissingReadySinceColumns: missing}, nil
}

// verifyReadySinceColumns names the readySince columns the schema lacks; a
// schema that lacks any of the three never receives them, so their shape is
// checked only when all three are present: the base time as a date-time
// string, the index set, the offsets as an atomic list of integers held to
// zero or more, and the rule that keeps the three together.
func verifyReadySinceColumns(columns schema) ([]string, error) {
	var missing []string
	for _, name := range readySinceColumnProperties {
		if _, ok := columns.Properties[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return missing, nil
	}
	base := columns.Properties[readySinceBaseProperty]
	if base.Type != "string" || base.Format != "date-time" {
		return nil, fmt.Errorf("%s is (%q, %q), want a date-time string", readySinceBaseProperty, base.Type, base.Format)
	}
	if err := verifyIndexSet(columns.Properties[readySinceIndexesProperty]); err != nil {
		return nil, fmt.Errorf("%s: %w", readySinceIndexesProperty, err)
	}
	if err := verifyAtomicList(readySinceOffsetsProperty, columns.Properties); err != nil {
		return nil, err
	}
	offsets := columns.Properties[readySinceOffsetsProperty].Items
	if offsets.Type != "integer" {
		return nil, fmt.Errorf("%s items have type %q, want integer", readySinceOffsetsProperty, offsets.Type)
	}
	if offsets.Minimum == nil || *offsets.Minimum != 0 {
		return nil, fmt.Errorf("%s items lack minimum 0", readySinceOffsetsProperty)
	}
	if !hasRule(columns.Validations, readySinceColumnsRule) {
		return nil, fmt.Errorf("the readySince columns lack the rule %q", readySinceColumnsRule)
	}
	return nil, nil
}

// verifyAtomicList requires the named property to be an atomic array with
// at least one item and an items schema.
func verifyAtomicList(name string, properties map[string]schema) error {
	property, ok := properties[name]
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
	return nil
}

// verifyGroupItems requires a group list's items to carry an index set and
// a value.
func verifyGroupItems(name string, items *schema) error {
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
	return nil
}

// verifyEntries requires the entries' items to carry an index and the
// non-empty entry rule.
func verifyEntries(entries *schema) error {
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
