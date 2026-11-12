package crdskew

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func containerShape() Shape {
	return Shape{Properties: map[string]Shape{
		"name":  {},
		"image": {},
		"env":   {Items: &Shape{Properties: map[string]Shape{"name": {}, "value": {}}}},
	}}
}

// A shape that repeats a sub-schema, as every Component's pod template does,
// is stored once and reads back whole.
func TestEncodeShippedStoresARepeatedSubSchemaOnce(t *testing.T) {
	pod := Shape{Properties: map[string]Shape{
		"containers":     {Items: ptr(containerShape())},
		"initContainers": {Items: ptr(containerShape())},
		"nodeSelector":   {AdditionalProperties: &Shape{}},
	}}
	byKind := map[string]Shape{
		"A": {Properties: map[string]Shape{"spec": pod, "status": {PreserveUnknown: true}}},
		"B": {Properties: map[string]Shape{"template": pod}},
	}

	data, err := EncodeShipped(byKind)
	if err != nil {
		t.Fatal(err)
	}
	var file shippedFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	// The distinct nodes: the empty leaf, the env item, env, the container,
	// the container list, the node selector, the pod, A's status, A, B.
	if len(file.Nodes) != 10 {
		t.Fatalf("nodes = %d, want 10 distinct nodes:\n%s", len(file.Nodes), data)
	}
	if !strings.HasSuffix(string(data), "}\n") || !strings.Contains(string(data), "\n ") {
		t.Fatalf("the file is not indented with a final newline:\n%s", data)
	}

	decoded, err := DecodeShipped(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, Shipped(byKind)) {
		t.Fatalf("decoded = %+v, want %+v", decoded, byKind)
	}
	again, err := EncodeShipped(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(data) {
		t.Fatal("encoding the decoded shape again gives a different file")
	}
}

// A node's children come before it in the list and a kind's root is in the
// list; an index that breaks either rule is refused with a text that names
// the node or kind and the index, the nodes read before the kinds, and a file
// that is not JSON is refused as unparsable.
func TestDecodeShippedRefusesAnIndexOutsideTheNodesBefore(t *testing.T) {
	for name, tt := range map[string]struct {
		file    string
		wantErr string
	}{
		"a child after its parent":           {file: `{"kinds":{"A":0},"nodes":[{"properties":{"x":1}},{}]}`, wantErr: "shipped schema node 0 names child 1, which does not come before it"},
		"a node naming itself":               {file: `{"kinds":{"A":0},"nodes":[{"items":0}]}`, wantErr: "shipped schema node 0 names child 0, which does not come before it"},
		"a negative child":                   {file: `{"kinds":{"A":0},"nodes":[{},{"additionalProperties":-1}]}`, wantErr: "shipped schema node 1 names child -1, which does not come before it"},
		"a kind out of range":                {file: `{"kinds":{"A":3},"nodes":[{}]}`, wantErr: "shipped schema kind A names node 3 of 1"},
		"a kind just past the end":           {file: `{"kinds":{"A":2},"nodes":[{},{}]}`, wantErr: "shipped schema kind A names node 2 of 2"},
		"a kind counted by the end":          {file: `{"kinds":{"A":4,"B":0,"C":1},"nodes":[{},{},{}]}`, wantErr: "shipped schema kind A names node 4 of 3"},
		"a broken node before a broken kind": {file: `{"kinds":{"A":7},"nodes":[{},{"items":1}]}`, wantErr: "shipped schema node 1 names child 1, which does not come before it"},
		"not json":                           {file: `nope`, wantErr: "parse the shipped schema: "},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeShipped([]byte(tt.file))
			if err == nil || !strings.HasPrefix(err.Error(), tt.wantErr) {
				t.Fatalf("DecodeShipped() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestShippedSchemasCarriesEveryKindOfTheGroup(t *testing.T) {
	shipped := shippedSchemas(t)
	if len(shipped) == 0 {
		t.Fatal("the embedded shipped schema names no kind; run make manifests")
	}
	for kind, shape := range shipped {
		if _, ok := shape.Properties["spec"]; !ok {
			t.Errorf("kind %s ships without a spec", kind)
		}
	}
}

func ptr(s Shape) *Shape { return &s }

// The embedded file is what `make manifests` writes from the generated CRDs,
// so the manager compares against the schema the chart installs. A stale
// file under-reports: a field the chart has and the file lacks is never named
// when an installed CRD lacks it.
func TestShippedSchemasAreTheGeneratedCRDs(t *testing.T) {
	byKind := map[string]Shape{}
	for _, path := range generatedCRDPaths(t) {
		crd := loadCRD(t, path)
		shape, err := ShapeOf(servedSchemaOf(t, crd))
		if err != nil {
			t.Fatalf("%s: %v", crd.Name, err)
		}
		byKind[crd.Spec.Names.Kind] = shape
	}
	want, err := EncodeShipped(byKind)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != string(shippedSchemaJSON) {
		t.Fatal("zz_generated.shipped_schema.json is not the shape of the generated CRDs; run make manifests")
	}
}
