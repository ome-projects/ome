package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// parseObject returns the Kubernetes object in a YAML document. It returns
// nil, nil when the document isn't one: it isn't a mapping, or it has no
// apiVersion or kind, like fragments of objects and Helm values. Invalid
// YAML, including duplicate keys, is an error.
func parseObject(text string) (*unstructured.Unstructured, error) {
	data, err := yaml.YAMLToJSONStrict([]byte(text))
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, nil // Not a mapping.
	}
	obj := &unstructured.Unstructured{Object: fields}
	if obj.GetAPIVersion() == "" || obj.GetKind() == "" {
		return nil, nil
	}
	return obj, nil
}

// docLine matches a line number in a YAML parser error. The parser counts
// lines from the start of the document.
var docLine = regexp.MustCompile(`\bline (\d+):`)

// yamlMessage returns err, an error from parsing doc, with its line
// numbers counted from the start of the file.
func yamlMessage(doc document, err error) string {
	return docLine.ReplaceAllStringFunc(err.Error(), func(match string) string {
		n, _ := strconv.Atoi(docLine.FindStringSubmatch(match)[1])
		return fmt.Sprintf("line %d:", doc.line+n-1)
	})
}
