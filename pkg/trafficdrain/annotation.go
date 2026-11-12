// Package trafficdrain defines the versioned InferenceService annotation used
// for durable, manual per-cluster traffic holds.
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
// annotation's top-level JSON object; Cluster names the member cluster the hold
// applies to and Reason records why the hold exists.
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
	overrides := make([]Override, 0)
	err := readObject(decoder, topLevelErrors, func(id string) error {
		if err := validateText("override ID", id); err != nil {
			return err
		}
		override, err := decodeOverride(decoder, id)
		if err != nil {
			return err
		}
		overrides = append(overrides, override)
		return nil
	})
	if err != nil {
		return nil, err
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

// decodeOverride reads one override object: its cluster and reason, each a
// string, no other field, and neither blank.
func decodeOverride(decoder *json.Decoder, id string) (Override, error) {
	override := Override{ID: id}
	err := readObject(decoder, overrideErrors(id), func(field string) error {
		var value string
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("override %q field %q must be a string: %w", id, field, err)
		}
		switch field {
		case "cluster":
			override.Cluster = value
		case "reason":
			override.Reason = value
		default:
			return fmt.Errorf("override %q has unknown field %q", id, field)
		}
		return nil
	})
	if err != nil {
		return Override{}, err
	}
	if err := validateText(fmt.Sprintf("override %q cluster", id), override.Cluster); err != nil {
		return Override{}, err
	}
	if err := validateText(fmt.Sprintf("override %q reason", id), override.Reason); err != nil {
		return Override{}, err
	}
	return override, nil
}

// objectErrors are the errors one object level reports, worded for that level.
type objectErrors struct {
	open         func(err error) error
	notObject    func() error
	key          func(err error) error
	keyNotString func() error
	duplicateKey func(key string) error
	close        func(err error) error
	notClosed    func() error
}

// topLevelErrors word the errors of the annotation's outer object, whose
// keys are override IDs.
var topLevelErrors = objectErrors{
	open:         func(err error) error { return fmt.Errorf("decode top-level object: %w", err) },
	notObject:    func() error { return fmt.Errorf("top-level value must be a JSON object keyed by override ID") },
	key:          func(err error) error { return fmt.Errorf("decode override ID: %w", err) },
	keyNotString: func() error { return fmt.Errorf("override ID must be a JSON object key") },
	duplicateKey: func(key string) error { return fmt.Errorf("duplicate override ID %q", key) },
	close:        func(err error) error { return fmt.Errorf("close top-level object: %w", err) },
	notClosed:    func() error { return fmt.Errorf("top-level value must end with a JSON object") },
}

// overrideErrors word the errors of one override's object, whose keys are its
// fields.
func overrideErrors(id string) objectErrors {
	return objectErrors{
		open:         func(err error) error { return fmt.Errorf("override %q: decode object: %w", id, err) },
		notObject:    func() error { return fmt.Errorf("override %q must be a JSON object", id) },
		key:          func(err error) error { return fmt.Errorf("override %q: decode field: %w", id, err) },
		keyNotString: func() error { return fmt.Errorf("override %q field name must be a string", id) },
		duplicateKey: func(key string) error { return fmt.Errorf("override %q has duplicate field %q", id, key) },
		close:        func(err error) error { return fmt.Errorf("override %q: close object: %w", id, err) },
		notClosed:    func() error { return fmt.Errorf("override %q must end with a JSON object", id) },
	}
}

// readObject reads one JSON object from the decoder: the opening brace, every
// member through member(key) once the key is known to be a new string, and
// the closing brace. A key that repeats is an error; errs words each error
// for the object's level.
func readObject(decoder *json.Decoder, errs objectErrors, member func(key string) error) error {
	start, err := decoder.Token()
	if err != nil {
		return errs.open(err)
	}
	if delim, ok := start.(json.Delim); !ok || delim != '{' {
		return errs.notObject()
	}
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return errs.key(err)
		}
		key, ok := token.(string)
		if !ok {
			return errs.keyNotString()
		}
		if _, duplicate := seen[key]; duplicate {
			return errs.duplicateKey(key)
		}
		seen[key] = struct{}{}
		if err := member(key); err != nil {
			return err
		}
	}
	if end, err := decoder.Token(); err != nil {
		return errs.close(err)
	} else if delim, ok := end.(json.Delim); !ok || delim != '}' {
		return errs.notClosed()
	}
	return nil
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
