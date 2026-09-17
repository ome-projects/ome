package schemapreflight

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/client-go/openapi"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
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

func TestVerifyDocumentAcceptsGeneratedFullSchema(t *testing.T) {
	if err := VerifyDocument(discoveryDocument(t, loadCRDSchema(t, "full"), true)); err != nil {
		t.Fatalf("generated full CRD must pass the preflight: %v", err)
	}
}

func TestVerifyDocumentRejectsGeneratedMinimalSchema(t *testing.T) {
	err := VerifyDocument(discoveryDocument(t, loadCRDSchema(t, "minimal"), true))
	if !errors.Is(err, ErrSchemaIncompatible) || !strings.Contains(err.Error(), "untyped") {
		t.Fatalf("minimal CRD must be rejected as untyped, got %v", err)
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
			name:    "index-set column missing",
			mutate:  func(root map[string]any) { delete(columnProperties(root), "activeOrdinalOne") },
			wantErr: "activeOrdinalOne is missing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := loadCRDSchema(t, "full")
			document := discoveryDocument(t, root, !tt.noPath)
			if tt.mutate != nil {
				var generic map[string]any
				if err := json.Unmarshal(document, &generic); err != nil {
					t.Fatal(err)
				}
				schemaRoot := generic["components"].(map[string]any)["schemas"].(map[string]any)["io.ome.v1beta1.InferenceReplica"].(map[string]any)
				tt.mutate(schemaRoot)
				mutated, err := json.Marshal(generic)
				if err != nil {
					t.Fatal(err)
				}
				document = mutated
			}
			err := VerifyDocument(document)
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
	err := VerifyDocument([]byte("{"))
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
	if err := Verify(fakeOpenAPIClient{paths: map[string]openapi.GroupVersion{groupVersionPath: fakeGroupVersion{document: good}}}); err != nil {
		t.Fatalf("good discovery must pass: %v", err)
	}

	if err := Verify(nil); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("nil client error = %v", err)
	}
	if err := Verify(fakeOpenAPIClient{err: errors.New("forbidden")}); err == nil || errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("discovery transport error must not be classified as a schema error: %v", err)
	}
	err := Verify(fakeOpenAPIClient{paths: map[string]openapi.GroupVersion{"apis/apps/v1": fakeGroupVersion{document: good}}})
	if !errors.Is(err, ErrSchemaIncompatible) || !strings.Contains(err.Error(), "does not serve") {
		t.Fatalf("missing group version error = %v", err)
	}
	err = Verify(fakeOpenAPIClient{paths: map[string]openapi.GroupVersion{groupVersionPath: fakeGroupVersion{err: errors.New("timeout")}}})
	if err == nil || errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("schema fetch error must not be classified as a schema error: %v", err)
	}
	stale := discoveryDocument(t, loadCRDSchema(t, "minimal"), true)
	if err := Verify(fakeOpenAPIClient{paths: map[string]openapi.GroupVersion{groupVersionPath: fakeGroupVersion{document: stale}}}); !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("stale schema must fail: %v", err)
	}
}
