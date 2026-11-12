package crdskew

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// servedDocument is the part of an OpenAPI v3 document the comparison reads:
// the component schemas, each with the group, version and kind it serves.
type servedDocument struct {
	Components struct {
		Schemas map[string]servedSchema `json:"schemas"`
	} `json:"components"`
}

type servedSchema struct {
	rawShape
	GVKs []struct {
		Group   string `json:"group"`
		Version string `json:"version"`
		Kind    string `json:"kind"`
	} `json:"x-kubernetes-group-version-kind"`
}

// CompareServed compares the schemas the apiserver serves for the manager's
// group version with this release's. resources are that group version's
// resources from discovery: each top-level resource names its CRD by the
// plural and its served schema by the kind. A kind this release ships no
// schema for, and a kind the document does not carry, are not compared.
func (s Shipped) CompareServed(document []byte, resources []metav1.APIResource) ([]Finding, error) {
	var doc servedDocument
	if err := json.Unmarshal(document, &doc); err != nil {
		return nil, fmt.Errorf("parse the served OpenAPI v3 document: %w", err)
	}
	byKind := map[string]Shape{}
	for _, name := range sortedSchemaNames(doc.Components.Schemas) {
		schema := doc.Components.Schemas[name]
		for _, gvk := range schema.GVKs {
			if gvk.Group == v1beta1.SchemeGroupVersion.Group && gvk.Version == v1beta1.SchemeGroupVersion.Version {
				byKind[gvk.Kind] = schema.rawShape.shape()
			}
		}
	}
	var findings []Finding
	for _, resource := range resources {
		if strings.Contains(resource.Name, "/") || !s.Knows(resource.Kind) {
			continue
		}
		served, ok := byKind[resource.Kind]
		if !ok {
			continue
		}
		findings = append(findings, Finding{
			CRD:           resource.Name + "." + v1beta1.SchemeGroupVersion.Group,
			MissingFields: s.MissingFields(resource.Kind, served),
		})
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].CRD < findings[j].CRD })
	return findings, nil
}

// CompareCRD compares one CRD object's schema for the manager's version with
// this release's. ok is false when the CRD is not in the manager's API group,
// does not serve the manager's version, or serves a kind this release ships
// no schema for.
func (s Shipped) CompareCRD(crd *apiextensionsv1.CustomResourceDefinition) (Finding, bool, error) {
	if crd == nil || crd.Spec.Group != v1beta1.SchemeGroupVersion.Group || !s.Knows(crd.Spec.Names.Kind) {
		return Finding{}, false, nil
	}
	for _, version := range crd.Spec.Versions {
		if version.Name != v1beta1.SchemeGroupVersion.Version || !version.Served || version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
			continue
		}
		installed, err := ShapeOf(version.Schema.OpenAPIV3Schema)
		if err != nil {
			return Finding{}, false, fmt.Errorf("decode the schema of CRD %s: %w", crd.Name, err)
		}
		return Finding{
			CRD:           crd.Name,
			MissingFields: s.MissingFields(crd.Spec.Names.Kind, installed),
		}, true, nil
	}
	return Finding{}, false, nil
}

func sortedSchemaNames(schemas map[string]servedSchema) []string {
	names := make([]string, 0, len(schemas))
	for name := range schemas {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
