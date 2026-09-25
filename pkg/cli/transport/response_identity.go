package transport

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"
)

// unambiguousResponseIdentity checks only the identity envelope. Callers still
// validate the resource's exact GVK, name, namespace, UID and resource version.
// Additional API fields are allowed, and the original response is not rewritten.
func unambiguousResponseIdentity(raw []byte) bool {
	return uniqueJSONFields(raw) && identityObject(
		raw,
		[]string{"apiVersion", "kind", "metadata", "spec", "status"},
		true,
	)
}

// unambiguousTypedFields rejects noncanonical case aliases of every known
// field in an evidence-bearing typed object while allowing unknown future API
// fields. encoding/json otherwise treats a key such as "Status" as "status",
// which can merge or replace the proof a guarded action evaluates.
func unambiguousTypedFields(raw []byte, target any) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF {
		return false
	}
	return canonicalTypedFields(value, reflect.TypeOf(target))
}

func canonicalTypedFields(value any, target reflect.Type) bool {
	for target != nil && target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if target == nil || value == nil {
		return true
	}
	switch target.Kind() {
	case reflect.Struct:
		object, ok := value.(map[string]any)
		if !ok {
			return true
		}
		known := knownJSONFields(target)
		for key, child := range object {
			fieldType, exact := known[key]
			if !exact {
				for canonical := range known {
					if strings.EqualFold(key, canonical) {
						return false
					}
				}
				// Unknown fields, including their private schemas, remain
				// forward-compatible and are intentionally not traversed.
				continue
			}
			if !canonicalTypedFields(child, fieldType) {
				return false
			}
		}
	case reflect.Slice, reflect.Array:
		items, ok := value.([]any)
		if !ok {
			return true
		}
		for _, item := range items {
			if !canonicalTypedFields(item, target.Elem()) {
				return false
			}
		}
	case reflect.Map:
		object, ok := value.(map[string]any)
		if !ok {
			return true
		}
		for _, child := range object {
			if !canonicalTypedFields(child, target.Elem()) {
				return false
			}
		}
	}
	return true
}

func knownJSONFields(target reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type)
	for index := 0; index < target.NumField(); index++ {
		field := target.Field(index)
		if field.PkgPath != "" {
			continue
		}
		tag := field.Tag.Get("json")
		name := strings.Split(tag, ",")[0]
		if name == "-" {
			continue
		}
		if field.Anonymous && name == "" {
			embedded := field.Type
			for embedded.Kind() == reflect.Pointer {
				embedded = embedded.Elem()
			}
			if embedded.Kind() == reflect.Struct {
				for key, fieldType := range knownJSONFields(embedded) {
					fields[key] = fieldType
				}
				continue
			}
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = field.Type
	}
	return fields
}

// uniqueJSONFields rejects duplicate keys at every object depth while allowing
// unknown fields for API forward compatibility. encoding/json otherwise keeps
// the last duplicate, which is unsafe for evidence-bearing action responses.
func uniqueJSONFields(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var value func() bool
	value = func() bool {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		delim, composite := token.(json.Delim)
		if !composite {
			return true
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				key, ok := keyToken.(string)
				if err != nil || !ok || seen[key] {
					return false
				}
				seen[key] = true
				if !value() {
					return false
				}
			}
			end, err := decoder.Token()
			return err == nil && end == json.Delim('}')
		case '[':
			for decoder.More() {
				if !value() {
					return false
				}
			}
			end, err := decoder.Token()
			return err == nil && end == json.Delim(']')
		default:
			return false
		}
	}
	if !value() {
		return false
	}
	var trailing json.RawMessage
	return decoder.Decode(&trailing) == io.EOF
}

func identityObject(raw []byte, canonical []string, root bool) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return false
	}
	seen := make(map[string]bool)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return false
		}
		seen[key] = true
		for _, exact := range canonical {
			if strings.EqualFold(key, exact) && key != exact {
				return false
			}
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return false
		}
		if root && key == "metadata" && !identityObject(value, []string{
			"name", "namespace", "uid", "resourceVersion", "generation",
			"annotations", "labels", "finalizers",
		}, false) {
			return false
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return false
	}
	var trailing json.RawMessage
	return decoder.Decode(&trailing) == io.EOF
}
