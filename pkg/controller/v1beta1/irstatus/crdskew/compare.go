// Package crdskew compares the CRD schemas installed on a cluster with the
// schema this release of the manager ships, and names every field the
// release knows that an installed schema lacks. The apiserver drops such a
// field from every write without an error, so a manager running under CRDs
// older than its release would lose state silently; the comparison gives
// the operator the CRD name and the fields instead, in the log, in an event
// and in a condition on every InferenceService.
//
// Only the presence of fields is compared. A changed type or enum is refused
// by the apiserver with an error that names the field, so nothing is lost
// silently there.
package crdskew

import (
	"encoding/json"
	"sort"
	"strings"
)

// Shape is the part of a JSON schema the comparison reads: which fields a
// node has, what a list holds, what a map's values hold, and whether the
// node keeps unknown fields.
type Shape struct {
	Properties           map[string]Shape
	Items                *Shape
	AdditionalProperties *Shape
	PreserveUnknown      bool
}

// rawShape decodes a JSON schema object. "items" and "additionalProperties"
// may be a schema, a list or a boolean; only a schema is read.
type rawShape struct {
	Properties           map[string]rawShape `json:"properties"`
	Items                json.RawMessage     `json:"items"`
	AdditionalProperties json.RawMessage     `json:"additionalProperties"`
	PreserveUnknown      bool                `json:"x-kubernetes-preserve-unknown-fields"`
}

func (r rawShape) shape() Shape {
	s := Shape{PreserveUnknown: r.PreserveUnknown}
	if len(r.Properties) > 0 {
		s.Properties = make(map[string]Shape, len(r.Properties))
		for name, child := range r.Properties {
			s.Properties[name] = child.shape()
		}
	}
	s.Items = schemaOf(r.Items)
	s.AdditionalProperties = schemaOf(r.AdditionalProperties)
	return s
}

// schemaOf reads a schema object out of a raw "items" or
// "additionalProperties" value; a boolean, a list or nothing is no schema.
func schemaOf(raw json.RawMessage) *Shape {
	if len(raw) == 0 || !strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		return nil
	}
	var r rawShape
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil
	}
	s := r.shape()
	return &s
}

// ShapeOf reads the shape of any JSON schema value through its JSON form: a
// CRD's JSONSchemaProps or a schema of a served OpenAPI document.
func ShapeOf(schema any) (Shape, error) {
	raw, err := json.Marshal(schema)
	if err != nil {
		return Shape{}, err
	}
	var r rawShape
	if err := json.Unmarshal(raw, &r); err != nil {
		return Shape{}, err
	}
	return r.shape(), nil
}

// Shipped is the schema this release ships for each kind of its API group,
// by kind, as the chart installs it.
type Shipped map[string]Shape

// Knows reports whether this release ships a schema for the kind.
func (s Shipped) Knows(kind string) bool {
	_, ok := s[kind]
	return ok
}

// MissingFields returns, sorted, the dotted paths of the fields this
// release's schema for kind has and the installed schema lacks; nil when the
// release ships no schema for the kind. A list's items read "[]" and a map's
// values "[*]". A node of the installed schema that keeps unknown fields
// prunes nothing below it, so nothing below it is reported.
func (s Shipped) MissingFields(kind string, installed Shape) []string {
	shipped, ok := s[kind]
	if !ok {
		return nil
	}
	var missing []string
	walk(shipped, installed, "", &missing)
	sort.Strings(missing)
	return missing
}

func walk(shipped, installed Shape, path string, missing *[]string) {
	if installed.PreserveUnknown {
		return
	}
	for _, field := range sortedKeys(shipped.Properties) {
		fieldPath := field
		if path != "" {
			fieldPath = path + "." + field
		}
		installedField, present := installed.Properties[field]
		if !present {
			*missing = append(*missing, fieldPath)
			continue
		}
		walk(shipped.Properties[field], installedField, fieldPath, missing)
	}
	if shipped.Items != nil && installed.Items != nil {
		walk(*shipped.Items, *installed.Items, path+"[]", missing)
	}
	if shipped.AdditionalProperties != nil && installed.AdditionalProperties != nil {
		walk(*shipped.AdditionalProperties, *installed.AdditionalProperties, path+"[*]", missing)
	}
}

func sortedKeys(m map[string]Shape) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
