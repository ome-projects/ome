// Package runtimeinheritance implements ServingRuntime
// inheritance resolution.
package runtimeinheritance

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/util/strategicpatch"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Merge overlays child onto parent (strategic-merge: child wins on
// any field it sets; parent fills the gaps).
func Merge(parent, child *v1beta1.ServingRuntimeSpec) (*v1beta1.ServingRuntimeSpec, error) {
	switch {
	case parent == nil && child == nil:
		return nil, nil
	case parent == nil:
		return child.DeepCopy(), nil
	case child == nil:
		return parent.DeepCopy(), nil
	}

	parentJSON, err := json.Marshal(parent)
	if err != nil {
		return nil, fmt.Errorf("marshal parent: %w", err)
	}
	childJSON, err := json.Marshal(child)
	if err != nil {
		return nil, fmt.Errorf("marshal child: %w", err)
	}
	// Several ServingRuntimeSpec fields (e.g. Containers) lack
	// omitempty, so a Go zero value marshals as `null` and strategic
	// merge would wipe the parent. Strip nulls to recover fill-only-nil.
	childJSON, err = StripJSONNulls(childJSON)
	if err != nil {
		return nil, fmt.Errorf("strip nulls from child: %w", err)
	}
	// Set after the strip above, which would drop these nulls.
	childJSON, err = NullStaleEnvSources(childJSON)
	if err != nil {
		return nil, fmt.Errorf("null stale env sources in child: %w", err)
	}

	mergedJSON, err := strategicpatch.StrategicMergePatch(parentJSON, childJSON, v1beta1.ServingRuntimeSpec{})
	if err != nil {
		return nil, fmt.Errorf("strategic merge: %w", err)
	}

	merged := &v1beta1.ServingRuntimeSpec{}
	if err := json.Unmarshal(mergedJSON, merged); err != nil {
		return nil, fmt.Errorf("unmarshal merged: %w", err)
	}
	return merged, nil
}

// NullStaleEnvSources sets the other half of every env entry in the override
// to null: `valueFrom` for an entry that sets `value`, `value` for one that
// sets `valueFrom`. An entry that sets both is left as written. Within a
// `valueFrom`, every source it does not set is nulled too. Strategic merge
// joins env entries by name and merges them field by field, so without the
// nulls an entry that changes its source keeps the base entry's other half or
// other source, and the API server rejects the env var.
func NullStaleEnvSources(patch []byte) ([]byte, error) {
	var doc any
	if err := json.Unmarshal(patch, &doc); err != nil {
		return nil, err
	}
	var walk func(v any)
	walk = func(v any) {
		switch t := v.(type) {
		case map[string]any:
			if env, ok := t["env"].([]any); ok {
				for _, e := range env {
					entry, ok := e.(map[string]any)
					if !ok {
						continue
					}
					_, hasSource := entry["valueFrom"]
					_, hasValue := entry["value"]
					if hasSource && !hasValue {
						entry["value"] = nil
					} else if hasValue && !hasSource {
						entry["valueFrom"] = nil
					}
					if source, ok := entry["valueFrom"].(map[string]any); ok {
						for _, key := range []string{"fieldRef", "resourceFieldRef", "configMapKeyRef", "secretKeyRef", "fileKeyRef"} {
							if _, set := source[key]; !set {
								source[key] = nil
							}
						}
					}
				}
			}
			for _, child := range t {
				walk(child)
			}
		case []any:
			for _, child := range t {
				walk(child)
			}
		}
	}
	walk(doc)
	return json.Marshal(doc)
}

// StripJSONNulls returns the JSON document with all null-valued object
// keys removed. Exported because the runtime-preset webhook needs the
// same treatment when marshaling a mutated runtime back to JSON — the
// embedded ServingRuntimePodSpec.Containers field lacks omitempty, and
// CRD validation rejects null arrays.
func StripJSONNulls(in []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(in, &v); err != nil {
		return nil, err
	}
	cleaned := stripNullsValue(v)
	return json.Marshal(cleaned)
}

func stripNullsValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if val == nil {
				continue
			}
			out[k] = stripNullsValue(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = stripNullsValue(val)
		}
		return out
	default:
		return v
	}
}
