package crdskew

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The InferenceReplica CRD types its whole status, so a status field it lacks
// is dropped by the apiserver; the InferenceService CRD keeps unknown status
// fields. The dropped-field cases use the first, the kept-field case the
// second.
var (
	inferenceReplicaKind = reflect.TypeOf(v1beta1.InferenceReplica{}).Name()
	inferenceServiceKind = reflect.TypeOf(v1beta1.InferenceService{}).Name()
)

// generatedCRDDir is the directory of the CRDs generated from this release's
// API types, the schema the chart installs.
func generatedCRDDir(t *testing.T, variant string) string {
	t.Helper()
	return filepath.Join("..", "..", "..", "..", "..", "config", "crd", variant)
}

func loadCRD(t *testing.T, path string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated CRD %s: %v", path, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("parse generated CRD %s: %v", path, err)
	}
	return &crd
}

// generatedCRD returns the generated CRD of one kind from the full variant.
func generatedCRD(t *testing.T, kind string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	for _, path := range generatedCRDPaths(t) {
		if crd := loadCRD(t, path); crd.Spec.Names.Kind == kind {
			return crd
		}
	}
	t.Fatalf("no generated CRD serves kind %s", kind)
	return nil
}

func generatedCRDPaths(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(generatedCRDDir(t, "full"), v1beta1.SchemeGroupVersion.Group+"_*.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no generated CRDs found: %v", err)
	}
	sort.Strings(paths)
	return paths
}

// servedSchemaOf returns the v1beta1 schema of a CRD, as the apiserver would
// store it.
func servedSchemaOf(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) *apiextensionsv1.JSONSchemaProps {
	t.Helper()
	for i := range crd.Spec.Versions {
		if crd.Spec.Versions[i].Name == v1beta1.SchemeGroupVersion.Version {
			return crd.Spec.Versions[i].Schema.OpenAPIV3Schema
		}
	}
	t.Fatalf("CRD %s has no %s schema", crd.Name, v1beta1.SchemeGroupVersion.Version)
	return nil
}

// shippedSchemas returns the schema this release ships, as the manager reads
// it from the generated file.
func shippedSchemas(t *testing.T) Shipped {
	t.Helper()
	shipped, err := ShippedSchemas()
	if err != nil {
		t.Fatalf("shipped schemas: %v", err)
	}
	return shipped
}

func sortedPropertyNames(props map[string]apiextensionsv1.JSONSchemaProps) []string {
	names := make([]string, 0, len(props))
	for name := range props {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// dropFirstStatusField removes the first status field of the schema and
// returns its name: the shape of a CRD one release behind a manager that
// added a status field.
func dropFirstStatusField(t *testing.T, schema *apiextensionsv1.JSONSchemaProps) string {
	t.Helper()
	status := schema.Properties["status"]
	names := sortedPropertyNames(status.Properties)
	if len(names) == 0 {
		t.Fatal("the status schema has no properties to drop")
	}
	delete(status.Properties, names[0])
	schema.Properties["status"] = status
	return names[0]
}

func TestMissingFieldsNamesTheFieldTheInstalledSchemaLacks(t *testing.T) {
	own := shippedSchemas(t)
	crd := generatedCRD(t, inferenceReplicaKind)
	schema := servedSchemaOf(t, crd)
	field := dropFirstStatusField(t, schema)

	finding, ok, err := own.CompareCRD(crd)
	if err != nil || !ok {
		t.Fatalf("CompareCRD: ok=%v err=%v", ok, err)
	}
	want := []string{"status." + field}
	if !reflect.DeepEqual(finding.MissingFields, want) {
		t.Fatalf("missing fields = %v, want %v", finding.MissingFields, want)
	}
	if finding.CRD != crd.Name || !finding.Lagging() {
		t.Fatalf("finding = %+v, want CRD %s lagging", finding, crd.Name)
	}
	for _, part := range []string{crd.Name, "status." + field, "drops"} {
		if !strings.Contains(finding.Sentence(), part) {
			t.Fatalf("sentence %q does not name %q", finding.Sentence(), part)
		}
	}
}

// A field dropped below the top level is reported with its full path, inside
// an object and inside a list's items.
func TestMissingFieldsNamesANestedFieldByItsPath(t *testing.T) {
	own := shippedSchemas(t)
	crd := generatedCRD(t, inferenceReplicaKind)
	schema := servedSchemaOf(t, crd)
	status := schema.Properties["status"]

	var want []string
	nested, listed := false, false
	for _, name := range sortedPropertyNames(status.Properties) {
		prop := status.Properties[name]
		switch {
		case !nested && len(prop.Properties) > 0:
			inner := sortedPropertyNames(prop.Properties)[0]
			delete(prop.Properties, inner)
			want = append(want, "status."+name+"."+inner)
			nested = true
		case !listed && prop.Items != nil && prop.Items.Schema != nil && len(prop.Items.Schema.Properties) > 0:
			inner := sortedPropertyNames(prop.Items.Schema.Properties)[0]
			delete(prop.Items.Schema.Properties, inner)
			want = append(want, "status."+name+"[]."+inner)
			listed = true
		default:
			continue
		}
		status.Properties[name] = prop
	}
	if !nested || !listed {
		t.Fatalf("the status schema offers no object (%v) or list of objects (%v) to drop a field from", nested, listed)
	}
	schema.Properties["status"] = status
	sort.Strings(want)

	finding, _, err := own.CompareCRD(crd)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(finding.MissingFields, want) {
		t.Fatalf("missing fields = %v, want %v", finding.MissingFields, want)
	}
}

// The case that must stay as today: a manager under the CRDs of its own
// release finds nothing to report, for every kind the chart installs.
func TestMissingFieldsEveryGeneratedCRDIsCurrent(t *testing.T) {
	own := shippedSchemas(t)
	for _, path := range generatedCRDPaths(t) {
		crd := loadCRD(t, path)
		finding, ok, err := own.CompareCRD(crd)
		if err != nil {
			t.Fatalf("%s: %v", crd.Name, err)
		}
		if !ok {
			t.Fatalf("%s: this release ships no schema for kind %s", crd.Name, crd.Spec.Names.Kind)
		}
		if finding.Lagging() {
			t.Errorf("%s at this release's schema reads as lagging: %v", crd.Name, finding.MissingFields)
		}
	}
}

// A node that keeps unknown fields prunes nothing below it, so a CRD whose
// status is untyped lacks no status field.
func TestMissingFieldsPreserveUnknownFieldsPrunesNothing(t *testing.T) {
	own := shippedSchemas(t)
	crd := generatedCRD(t, inferenceReplicaKind)
	schema := servedSchemaOf(t, crd)
	keep := true
	schema.Properties["status"] = apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &keep}

	finding, _, err := own.CompareCRD(crd)
	if err != nil {
		t.Fatal(err)
	}
	if finding.Lagging() {
		t.Fatalf("an untyped status reads as lagging: %v", finding.MissingFields)
	}
}

// The InferenceService CRD keeps unknown status fields, so a status field it
// lacks is stored anyway and is not reported; a spec field it lacks is.
func TestMissingFieldsInferenceServiceStatusKeepsUnknownFields(t *testing.T) {
	own := shippedSchemas(t)
	crd := generatedCRD(t, inferenceServiceKind)
	schema := servedSchemaOf(t, crd)
	if keep := schema.Properties["status"].XPreserveUnknownFields; keep == nil || !*keep {
		t.Skip("the InferenceService status no longer keeps unknown fields")
	}
	dropFirstStatusField(t, schema)
	finding, _, err := own.CompareCRD(crd)
	if err != nil {
		t.Fatal(err)
	}
	if finding.Lagging() {
		t.Fatalf("a status field under a node that keeps unknown fields reads as missing: %v", finding.MissingFields)
	}

	spec := schema.Properties["spec"]
	field := sortedPropertyNames(spec.Properties)[0]
	delete(spec.Properties, field)
	schema.Properties["spec"] = spec
	finding, _, err = own.CompareCRD(crd)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"spec." + field}; !reflect.DeepEqual(finding.MissingFields, want) {
		t.Fatalf("missing fields = %v, want %v", finding.MissingFields, want)
	}
}

func TestMissingFieldsUnknownKindIsNotCompared(t *testing.T) {
	own := shippedSchemas(t)
	if own.Knows("NotAKind") {
		t.Fatal("this release ships a schema for a kind it has no type for")
	}
	if got := own.MissingFields("NotAKind", Shape{}); got != nil {
		t.Fatalf("an unknown kind reports %v", got)
	}
	crd := generatedCRD(t, inferenceServiceKind)
	crd.Spec.Names.Kind = "NotAKind"
	if _, ok, _ := own.CompareCRD(crd); ok {
		t.Fatal("a CRD of an unknown kind was compared")
	}
}

func TestCompareCRDIgnoresOtherGroupsAndVersions(t *testing.T) {
	own := shippedSchemas(t)
	other := generatedCRD(t, inferenceServiceKind)
	other.Spec.Group = "other.example"
	if _, ok, _ := own.CompareCRD(other); ok {
		t.Fatal("a CRD of another group was compared")
	}
	unserved := generatedCRD(t, inferenceServiceKind)
	for i := range unserved.Spec.Versions {
		unserved.Spec.Versions[i].Served = false
	}
	if _, ok, _ := own.CompareCRD(unserved); ok {
		t.Fatal("a CRD that does not serve the manager's version was compared")
	}
	if _, ok, _ := own.CompareCRD(nil); ok {
		t.Fatal("a nil CRD was compared")
	}
}

// servedDocumentFor wraps CRD schemas the way the apiserver serves them in
// the group version's OpenAPI v3 document.
func servedDocumentFor(t *testing.T, crds ...*apiextensionsv1.CustomResourceDefinition) []byte {
	t.Helper()
	schemas := map[string]any{}
	for _, crd := range crds {
		raw, err := json.Marshal(servedSchemaOf(t, crd))
		if err != nil {
			t.Fatal(err)
		}
		var schema map[string]any
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatal(err)
		}
		schema["x-kubernetes-group-version-kind"] = []map[string]string{{
			"group":   crd.Spec.Group,
			"version": v1beta1.SchemeGroupVersion.Version,
			"kind":    crd.Spec.Names.Kind,
		}}
		schemas["io.ome."+v1beta1.SchemeGroupVersion.Version+"."+crd.Spec.Names.Kind] = schema
	}
	document, err := json.Marshal(map[string]any{
		"openapi":    "3.0.0",
		"components": map[string]any{"schemas": schemas},
	})
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func resourcesOf(crds ...*apiextensionsv1.CustomResourceDefinition) []metav1.APIResource {
	var resources []metav1.APIResource
	for _, crd := range crds {
		resources = append(resources,
			metav1.APIResource{Name: crd.Spec.Names.Plural, Kind: crd.Spec.Names.Kind},
			metav1.APIResource{Name: crd.Spec.Names.Plural + "/status", Kind: crd.Spec.Names.Kind})
	}
	return resources
}

// The served document, the source at start-up, and the CRD object, the
// source on a CRD change, give the same picture.
func TestCompareServedAgreesWithTheCRDObject(t *testing.T) {
	own := shippedSchemas(t)
	lagging := generatedCRD(t, inferenceReplicaKind)
	field := dropFirstStatusField(t, servedSchemaOf(t, lagging))
	var current *apiextensionsv1.CustomResourceDefinition
	for _, path := range generatedCRDPaths(t) {
		if crd := loadCRD(t, path); crd.Spec.Names.Kind != inferenceReplicaKind {
			current = crd
			break
		}
	}

	findings, err := own.CompareServed(servedDocumentFor(t, lagging, current), resourcesOf(lagging, current))
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 2 {
		t.Fatalf("findings = %+v, want one per top-level resource", findings)
	}
	byCRD := map[string]Finding{}
	for _, f := range findings {
		byCRD[f.CRD] = f
	}
	if got := byCRD[lagging.Name].MissingFields; !reflect.DeepEqual(got, []string{"status." + field}) {
		t.Fatalf("%s missing fields = %v, want status.%s", lagging.Name, got, field)
	}
	if got := byCRD[current.Name]; got.Lagging() {
		t.Fatalf("%s at this release's schema reads as lagging: %v", current.Name, got.MissingFields)
	}
	fromObject, _, err := own.CompareCRD(lagging)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fromObject, byCRD[lagging.Name]) {
		t.Fatalf("the CRD object reads %+v, the served document %+v", fromObject, byCRD[lagging.Name])
	}
}

func TestCompareServedSkipsKindsTheDocumentDoesNotCarry(t *testing.T) {
	own := shippedSchemas(t)
	crd := generatedCRD(t, inferenceServiceKind)
	findings, err := own.CompareServed(servedDocumentFor(t), resourcesOf(crd))
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %+v, want none for a kind the document does not carry", findings)
	}
	if _, err := own.CompareServed([]byte("not json"), nil); err == nil {
		t.Fatal("a malformed document was compared")
	}
}
