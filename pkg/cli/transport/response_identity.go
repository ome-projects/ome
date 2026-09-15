package transport

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

// unambiguousResponseIdentity checks only the identity envelope. Callers still
// validate the resource's exact GVK, name, namespace, UID and resource version.
// Additional API fields are allowed, and the original response is not rewritten.
func unambiguousResponseIdentity(raw []byte) bool {
	return identityObject(raw, []string{"apiVersion", "kind", "metadata"}, true)
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
		if root && key == "metadata" && !identityObject(value, []string{"name", "namespace", "uid", "resourceVersion"}, false) {
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
