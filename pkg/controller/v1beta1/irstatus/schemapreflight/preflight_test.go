package schemapreflight

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/openapi"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// loadStatusSchema returns the served v1beta1 openAPIV3Schema of one
// generated CRD variant as a generic JSON object.
func loadCRDSchema(t *testing.T, variant string) map[string]any {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "..", "config", "crd", variant, "ome.io_inferencereplicas.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated CRD %s: %v", path, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("parse generated CRD: %v", err)
	}
	for _, version := range crd.Spec.Versions {
		if version.Name != v1beta1.SchemeGroupVersion.Version || version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
			continue
		}
		raw, err := json.Marshal(version.Schema.OpenAPIV3Schema)
		if err != nil {
			t.Fatalf("marshal schema: %v", err)
		}
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal schema: %v", err)
		}
		return out
	}
	t.Fatalf("CRD %s has no v1beta1 schema", path)
	return nil
}

// discoveryDocument wraps a CRD schema the way the API server serves it in
// the ome.io/v1beta1 OpenAPI v3 document.
func discoveryDocument(t *testing.T, root map[string]any, withStatusPath bool) []byte {
	t.Helper()
	root["x-kubernetes-group-version-kind"] = []map[string]string{{
		"group":   v1beta1.SchemeGroupVersion.Group,
		"version": v1beta1.SchemeGroupVersion.Version,
		"kind":    "InferenceReplica",
	}}
	paths := map[string]any{
		"/" + groupVersionPath + "/namespaces/{namespace}/inferencereplicas/{name}": map[string]any{},
	}
	if withStatusPath {
		paths[statusSubresource] = map[string]any{}
	}
	doc := map[string]any{
		"openapi": "3.0.0",
		"paths":   paths,
		"components": map[string]any{
			"schemas": map[string]any{
				"io.ome.v1beta1.InferenceReplica": root,
			},
		},
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}
	return raw
}

func statusOf(root map[string]any) map[string]any {
	return root["properties"].(map[string]any)["status"].(map[string]any)
}

func statusProperties(root map[string]any) map[string]any {
	return statusOf(root)["properties"].(map[string]any)
}

func columnProperties(root map[string]any) map[string]any {
	return statusProperties(root)["instanceStatusColumns"].(map[string]any)["properties"].(map[string]any)
}

// documentWith serves the generated full CRD as discovery does, after mutate
// has changed the InferenceReplica schema inside the document.
func documentWith(t *testing.T, withStatusPath bool, mutate func(root map[string]any)) []byte {
	t.Helper()
	document := discoveryDocument(t, loadCRDSchema(t, "full"), withStatusPath)
	var generic map[string]any
	if err := json.Unmarshal(document, &generic); err != nil {
		t.Fatal(err)
	}
	mutate(generic["components"].(map[string]any)["schemas"].(map[string]any)["io.ome.v1beta1.InferenceReplica"].(map[string]any))
	mutated, err := json.Marshal(generic)
	if err != nil {
		t.Fatal(err)
	}
	return mutated
}

func TestVerifyDocumentAcceptsGeneratedFullSchema(t *testing.T) {
	capabilities, err := VerifyDocument(discoveryDocument(t, loadCRDSchema(t, "full"), true))
	if err != nil {
		t.Fatalf("generated full CRD must pass the preflight: %v", err)
	}
	if len(capabilities.MissingReadySinceColumns) != 0 || capabilities.ReadySinceForm() != irstatus.ReadySinceInColumns {
		t.Fatalf("the generated full CRD stores every readySince column, got %+v", capabilities)
	}
}

func TestVerifyDocumentRejectsGeneratedMinimalSchema(t *testing.T) {
	_, err := VerifyDocument(discoveryDocument(t, loadCRDSchema(t, "minimal"), true))
	if !errors.Is(err, ErrSchemaIncompatible) || !strings.Contains(err.Error(), "untyped") {
		t.Fatalf("minimal CRD must be rejected as untyped, got %v", err)
	}
}

// TestVerifyDocumentStartsWithoutTheReadySinceColumns: a served schema that
// lacks a readySince column, the CRD of the release before the columns,
// passes the preflight and names what it lacks, so the manager starts and
// stores each Instance's readySince in its entry. A schema that has the
// three columns in another shape still fails, as the refusal cases say.
func TestVerifyDocumentStartsWithoutTheReadySinceColumns(t *testing.T) {
	tests := []struct {
		name     string
		drop     []string
		dropRule bool
	}{
		{name: "the three columns and their rule", drop: readySinceColumnProperties, dropRule: true},
		{name: "the base", drop: []string{readySinceBaseProperty}},
		{name: "the index set", drop: []string{readySinceIndexesProperty}},
		{name: "the offsets", drop: []string{readySinceOffsetsProperty}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			document := documentWith(t, true, func(root map[string]any) {
				for _, name := range tt.drop {
					delete(columnProperties(root), name)
				}
				if tt.dropRule {
					delete(statusProperties(root)["instanceStatusColumns"].(map[string]any), "x-kubernetes-validations")
				}
			})
			capabilities, err := VerifyDocument(document)
			if err != nil {
				t.Fatalf("a schema without %v must pass the preflight: %v", tt.drop, err)
			}
			if !slices.Equal(capabilities.MissingReadySinceColumns, tt.drop) {
				t.Fatalf("missing readySince columns = %v, want %v", capabilities.MissingReadySinceColumns, tt.drop)
			}
			if capabilities.ReadySinceForm() != irstatus.ReadySinceInEntries {
				t.Fatalf("a schema without %v must store each readySince in the entries, got %v", tt.drop, capabilities.ReadySinceForm())
			}
		})
	}
}

func TestVerifyDocumentRejectsStaleOrMissingFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(root map[string]any)
		noPath  bool
		wantErr string
	}{
		{
			name:    "status subresource path missing",
			mutate:  func(map[string]any) {},
			noPath:  true,
			wantErr: "status subresource path",
		},
		{
			name:    "group-version-kind extension missing",
			mutate:  func(root map[string]any) { root["x-kubernetes-group-version-kind"] = nil },
			wantErr: "no component schema carries",
		},
		{
			name:    "dense field missing",
			mutate:  func(root map[string]any) { delete(statusProperties(root), "instanceStatuses") },
			wantErr: "retained dense field",
		},
		{
			name:    "marker missing",
			mutate:  func(root map[string]any) { delete(statusProperties(root), "instanceStatusEncoding") },
			wantErr: "instanceStatusEncoding is missing",
		},
		{
			name: "marker enum widened",
			mutate: func(root map[string]any) {
				statusProperties(root)["instanceStatusEncoding"].(map[string]any)["enum"] = []string{"ColumnarV2", "SparseV3"}
			},
			wantErr: "enum is",
		},
		{
			name: "marker enum absent",
			mutate: func(root map[string]any) {
				delete(statusProperties(root)["instanceStatusEncoding"].(map[string]any), "enum")
			},
			wantErr: "enum is",
		},
		{
			name:    "columns missing",
			mutate:  func(root map[string]any) { delete(statusProperties(root), "instanceStatusColumns") },
			wantErr: "instanceStatusColumns is missing",
		},
		{
			name:    "union rules missing",
			mutate:  func(root map[string]any) { delete(statusOf(root), "x-kubernetes-validations") },
			wantErr: "representation-union rule",
		},
		{
			name: "one union rule reworded",
			mutate: func(root map[string]any) {
				statusOf(root)["x-kubernetes-validations"] = []map[string]string{{"rule": representationUnionRules[0]}}
			},
			wantErr: "representation-union rule",
		},
		{
			name: "phases list not atomic",
			mutate: func(root map[string]any) {
				columnProperties(root)["phases"].(map[string]any)["x-kubernetes-list-type"] = "map"
			},
			wantErr: "want atomic",
		},
		{
			name:    "entries minItems missing",
			mutate:  func(root map[string]any) { delete(columnProperties(root)["entries"].(map[string]any), "minItems") },
			wantErr: "lacks minItems 1",
		},
		{
			name:    "members minLength missing",
			mutate:  func(root map[string]any) { delete(columnProperties(root)["members"].(map[string]any), "minLength") },
			wantErr: "lacks minLength 1",
		},
		{
			name:    "members pattern missing",
			mutate:  func(root map[string]any) { delete(columnProperties(root)["members"].(map[string]any), "pattern") },
			wantErr: "lacks the index-set pattern",
		},
		{
			name: "required columns dropped",
			mutate: func(root map[string]any) {
				delete(statusProperties(root)["instanceStatusColumns"].(map[string]any), "required")
			},
			wantErr: "is not required",
		},
		{
			name: "count group minimum missing",
			mutate: func(root map[string]any) {
				items := columnProperties(root)["podCounts"].(map[string]any)["items"].(map[string]any)
				delete(items["properties"].(map[string]any)["value"].(map[string]any), "minimum")
			},
			wantErr: "lacks minimum 1",
		},
		{
			name: "incarnation nonzero rule missing",
			mutate: func(root map[string]any) {
				delete(columnProperties(root)["incarnations"].(map[string]any)["items"].(map[string]any), "x-kubernetes-validations")
			},
			wantErr: "nonzero value rule",
		},
		{
			name: "entry non-empty rule missing",
			mutate: func(root map[string]any) {
				delete(columnProperties(root)["entries"].(map[string]any)["items"].(map[string]any), "x-kubernetes-validations")
			},
			wantErr: "non-empty entry rule",
		},
		{
			name:    "group column missing",
			mutate:  func(root map[string]any) { delete(columnProperties(root), "runningRevisions") },
			wantErr: "runningRevisions is missing",
		},
		{
			name: "readySince base not a date-time",
			mutate: func(root map[string]any) {
				delete(columnProperties(root)["readySinceBase"].(map[string]any), "format")
			},
			wantErr: "want a date-time string",
		},
		{
			name: "readySince indexes pattern missing",
			mutate: func(root map[string]any) {
				delete(columnProperties(root)["readySinceIndexes"].(map[string]any), "pattern")
			},
			wantErr: "readySinceIndexes: lacks the index-set pattern",
		},
		{
			name: "readySince offsets list not atomic",
			mutate: func(root map[string]any) {
				columnProperties(root)["readySinceOffsets"].(map[string]any)["x-kubernetes-list-type"] = "set"
			},
			wantErr: "readySinceOffsets has x-kubernetes-list-type",
		},
		{
			name: "readySince offsets minimum missing",
			mutate: func(root map[string]any) {
				delete(columnProperties(root)["readySinceOffsets"].(map[string]any)["items"].(map[string]any), "minimum")
			},
			wantErr: "readySinceOffsets items lack minimum 0",
		},
		{
			name: "readySince offsets not integers",
			mutate: func(root map[string]any) {
				columnProperties(root)["readySinceOffsets"].(map[string]any)["items"].(map[string]any)["type"] = "string"
			},
			wantErr: "want integer",
		},
		{
			name: "readySince columns rule missing",
			mutate: func(root map[string]any) {
				delete(statusProperties(root)["instanceStatusColumns"].(map[string]any), "x-kubernetes-validations")
			},
			wantErr: "readySince columns lack the rule",
		},
		{
			name:    "index-set column missing",
			mutate:  func(root map[string]any) { delete(columnProperties(root), "activeOrdinalOne") },
			wantErr: "activeOrdinalOne is missing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := VerifyDocument(documentWith(t, !tt.noPath, tt.mutate))
			if err == nil {
				t.Fatal("expected the preflight to fail")
			}
			if !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("error must wrap ErrSchemaIncompatible: %v", err)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error %q does not mention %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestVerifyDocumentRejectsMalformedJSON(t *testing.T) {
	_, err := VerifyDocument([]byte("{"))
	if !errors.Is(err, ErrSchemaIncompatible) || !strings.Contains(err.Error(), "parse OpenAPI v3 document") {
		t.Fatalf("malformed document error = %v", err)
	}
}

type fakeGroupVersion struct {
	document []byte
	err      error
}

func (g fakeGroupVersion) Schema(contentType string) ([]byte, error) {
	if contentType != jsonContentType {
		return nil, errors.New("unsupported content type")
	}
	return g.document, g.err
}

func (g fakeGroupVersion) ServerRelativeURL() string { return "/openapi/v3/" + groupVersionPath }

type fakeOpenAPIClient struct {
	paths map[string]openapi.GroupVersion
	err   error
}

func (c fakeOpenAPIClient) Paths() (map[string]openapi.GroupVersion, error) { return c.paths, c.err }

func TestVerifyThroughDiscovery(t *testing.T) {
	good := discoveryDocument(t, loadCRDSchema(t, "full"), true)
	if _, err := Verify(fakeOpenAPIClient{paths: map[string]openapi.GroupVersion{groupVersionPath: fakeGroupVersion{document: good}}}); err != nil {
		t.Fatalf("good discovery must pass: %v", err)
	}

	if _, err := Verify(nil); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("nil client error = %v", err)
	}
	if _, err := Verify(fakeOpenAPIClient{err: errors.New("forbidden")}); err == nil || errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("discovery transport error must not be classified as a schema error: %v", err)
	}
	_, err := Verify(fakeOpenAPIClient{paths: map[string]openapi.GroupVersion{"apis/apps/v1": fakeGroupVersion{document: good}}})
	if !errors.Is(err, ErrSchemaIncompatible) || !strings.Contains(err.Error(), "does not serve") {
		t.Fatalf("missing group version error = %v", err)
	}
	_, err = Verify(fakeOpenAPIClient{paths: map[string]openapi.GroupVersion{groupVersionPath: fakeGroupVersion{err: errors.New("timeout")}}})
	if err == nil || errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("schema fetch error must not be classified as a schema error: %v", err)
	}
	stale := discoveryDocument(t, loadCRDSchema(t, "minimal"), true)
	if _, err := Verify(fakeOpenAPIClient{paths: map[string]openapi.GroupVersion{groupVersionPath: fakeGroupVersion{document: stale}}}); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("stale schema must fail: %v", err)
	}
}
