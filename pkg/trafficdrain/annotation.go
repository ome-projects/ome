// Package trafficdrain defines the versioned InferenceService annotation used
// for durable, manual route-arm drains.
package trafficdrain

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"sigs.k8s.io/ome/pkg/constants"
)

// Override is one independently removable operator hold. ID is the key in the
// annotation's top-level JSON object; Cluster selects the TrafficMap arm and
// Reason records why the hold exists.
type Override struct {
	ID      string
	Cluster string
	Reason  string
}

// FromAnnotations parses TrafficDrainAnnotation. An absent annotation means no
// overrides. A present value is always parsed strictly so malformed intent
// cannot silently restore or redirect traffic.
func FromAnnotations(annotations map[string]string) ([]Override, error) {
	raw, ok := annotations[constants.TrafficDrainAnnotation]
	if !ok {
		return nil, nil
	}
	return Parse(raw)
}

// Parse decodes the traffic-drain value:
//
//	{"drain-id":{"cluster":"cluster-a","reason":"operator context"}}
//
// Both object levels reject duplicate and unknown fields. Overrides are sorted
// by ID so TrafficMap output is deterministic regardless of JSON member order.
func Parse(raw string) ([]Override, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	start, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("decode top-level object: %w", err)
	}
	if delim, ok := start.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("top-level value must be a JSON object keyed by override ID")
	}

	seen := make(map[string]struct{})
	overrides := make([]Override, 0)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("decode override ID: %w", err)
		}
		id, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("override ID must be a JSON object key")
		}
		if err := validateText("override ID", id); err != nil {
			return nil, err
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("duplicate override ID %q", id)
		}
		seen[id] = struct{}{}

		override, err := decodeOverride(decoder, id)
		if err != nil {
			return nil, err
		}
		overrides = append(overrides, override)
	}
	if end, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("close top-level object: %w", err)
	} else if delim, ok := end.(json.Delim); !ok || delim != '}' {
		return nil, fmt.Errorf("top-level value must end with a JSON object")
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("decode trailing data: %w", err)
		}
		return nil, fmt.Errorf("unexpected trailing JSON token %v", token)
	}

	sort.Slice(overrides, func(i, j int) bool { return overrides[i].ID < overrides[j].ID })
	return overrides, nil
}

func decodeOverride(decoder *json.Decoder, id string) (Override, error) {
	start, err := decoder.Token()
	if err != nil {
		return Override{}, fmt.Errorf("override %q: decode object: %w", id, err)
	}
	if delim, ok := start.(json.Delim); !ok || delim != '{' {
		return Override{}, fmt.Errorf("override %q must be a JSON object", id)
	}

	override := Override{ID: id}
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return Override{}, fmt.Errorf("override %q: decode field: %w", id, err)
		}
		field, ok := token.(string)
		if !ok {
			return Override{}, fmt.Errorf("override %q field name must be a string", id)
		}
		if _, duplicate := seen[field]; duplicate {
			return Override{}, fmt.Errorf("override %q has duplicate field %q", id, field)
		}
		seen[field] = struct{}{}

		var value string
		if err := decoder.Decode(&value); err != nil {
			return Override{}, fmt.Errorf("override %q field %q must be a string: %w", id, field, err)
		}
		switch field {
		case "cluster":
			override.Cluster = value
		case "reason":
			override.Reason = value
		default:
			return Override{}, fmt.Errorf("override %q has unknown field %q", id, field)
		}
	}
	if end, err := decoder.Token(); err != nil {
		return Override{}, fmt.Errorf("override %q: close object: %w", id, err)
	} else if delim, ok := end.(json.Delim); !ok || delim != '}' {
		return Override{}, fmt.Errorf("override %q must end with a JSON object", id)
	}

	if err := validateText(fmt.Sprintf("override %q cluster", id), override.Cluster); err != nil {
		return Override{}, err
	}
	if err := validateText(fmt.Sprintf("override %q reason", id), override.Reason); err != nil {
		return Override{}, err
	}
	return override, nil
}

func validateText(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must be non-empty", field)
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must not have leading or trailing whitespace", field)
	}
	return nil
}
