package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/crdskew"
)

// runShippedSchema writes the shape of the CRDs this release ships, read
// from the generated CRD files after their post-processing, to the file the
// manager embeds and compares the installed CRDs with at start-up.
func runShippedSchema(args []string) error {
	fs := flag.NewFlagSet("shippedschema", flag.ContinueOnError)
	out := fs.String("out", "", "the shipped-schema file to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" || fs.NArg() == 0 {
		return fmt.Errorf("usage: crd-gen shippedschema --out <file> <crd yaml>...")
	}
	byKind, err := shippedShapes(fs.Args())
	if err != nil {
		return err
	}
	data, err := crdskew.EncodeShipped(byKind)
	if err != nil {
		return err
	}
	return os.WriteFile(*out, data, 0o644) //nolint:gosec // a generated source file, world-readable like its neighbours
}

// shippedShapes reads the shape of the manager's version of every CRD of
// the manager's API group among the files, by kind.
func shippedShapes(paths []string) (map[string]crdskew.Shape, error) {
	byKind := map[string]crdskew.Shape{}
	for _, path := range paths {
		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil, err
		}
		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.Unmarshal(data, &crd); err != nil {
			return nil, fmt.Errorf("parse CRD %s: %w", path, err)
		}
		if crd.Spec.Group != v1beta1.SchemeGroupVersion.Group {
			continue
		}
		for _, version := range crd.Spec.Versions {
			if version.Name != v1beta1.SchemeGroupVersion.Version || !version.Served || version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
				continue
			}
			shape, err := crdskew.ShapeOf(version.Schema.OpenAPIV3Schema)
			if err != nil {
				return nil, fmt.Errorf("read the schema of CRD %s: %w", path, err)
			}
			byKind[crd.Spec.Names.Kind] = shape
		}
	}
	return byKind, nil
}
