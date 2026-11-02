package runtimerevision

import (
	"encoding/json"
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/digest"
)

// shortHashLength is the number of hex characters of the content hash that a
// revision name ends with and that the hash label carries.
const shortHashLength = 8

// Hash returns the SHA-256 of the JSON-marshaled spec in two forms: the full
// hex (64 characters) for collision auditing and its shortHashLength-character
// prefix, which the revision name uses.
//
// Determinism: ServingRuntimeSpec contains only maps with string keys
// (Annotations, Labels, NodeSelector, ...); Go's encoding/json sorts
// string-keyed maps and serializes struct fields in declaration order.
// Same spec → same bytes → same hash across controller restarts.
func Hash(spec *v1beta1.ServingRuntimeSpec) (full, short string, err error) {
	if spec == nil {
		return "", "", fmt.Errorf("runtimerevision.Hash: nil spec")
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return "", "", fmt.Errorf("marshal spec: %w", err)
	}
	full = digest.Hex(b)
	return full, full[:shortHashLength], nil
}
