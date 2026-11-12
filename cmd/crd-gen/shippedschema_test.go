package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/crdskew"
)

const shippedSchemaFixtureCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: things.ome.io
spec:
  group: ome.io
  names:
    kind: Thing
    plural: things
  scope: Namespaced
  versions:
  - name: v1beta1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            properties:
              size:
                type: integer
              members:
                type: array
                items:
                  type: object
                  properties:
                    name:
                      type: string
          status:
            type: object
            x-kubernetes-preserve-unknown-fields: true
`

const shippedSchemaOtherGroupCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.other.example
spec:
  group: other.example
  names:
    kind: Widget
    plural: widgets
  scope: Namespaced
  versions:
  - name: v1beta1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
`

// The generator writes a file the manager decodes back to the CRDs' shape,
// for the manager's group only.
func TestRunShippedSchemaWritesTheShapeTheManagerReads(t *testing.T) {
	dir := t.TempDir()
	thing := filepath.Join(dir, "things.yaml")
	widget := filepath.Join(dir, "widgets.yaml")
	out := filepath.Join(dir, "shipped.json")
	if err := os.WriteFile(thing, []byte(shippedSchemaFixtureCRD), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(widget, []byte(shippedSchemaOtherGroupCRD), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := runShippedSchema([]string{"--out", out, thing, widget}); err != nil {
		t.Fatalf("runShippedSchema: %v", err)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	shipped, err := crdskew.DecodeShipped(data)
	if err != nil {
		t.Fatalf("decode the written file: %v", err)
	}
	if shipped.Knows("Widget") {
		t.Fatal("a CRD of another group was written")
	}
	thingShape, ok := shipped["Thing"]
	if !ok {
		t.Fatalf("kinds = %v, want Thing", reflect.ValueOf(shipped).MapKeys())
	}
	spec := thingShape.Properties["spec"]
	if _, ok := spec.Properties["size"]; !ok {
		t.Fatalf("spec = %+v, want a size field", spec)
	}
	members := spec.Properties["members"]
	if members.Items == nil || members.Items.Properties["name"].Properties != nil {
		t.Fatalf("spec.members = %+v, want a list of objects with a name", members)
	}
	if !thingShape.Properties["status"].PreserveUnknown {
		t.Fatalf("status = %+v, want unknown fields kept", thingShape.Properties["status"])
	}
}

func TestRunShippedSchemaRefusesAMissingOutputOrInput(t *testing.T) {
	if err := runShippedSchema([]string{"--out", filepath.Join(t.TempDir(), "x.json")}); err == nil {
		t.Fatal("no input files were accepted")
	}
	if err := runShippedSchema([]string{"some.yaml"}); err == nil {
		t.Fatal("no output file was accepted")
	}
	if err := runShippedSchema([]string{"--out", filepath.Join(t.TempDir(), "x.json"), filepath.Join(t.TempDir(), "absent.yaml")}); err == nil {
		t.Fatal("an absent input file was accepted")
	}
}
