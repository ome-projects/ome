package capacity

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
)

// MappingFingerprint identifies the complete configured resource/flavor mapping.
// It includes unused flavors because they can change which pool owns a node.
// It does not validate identity, completeness, or hardware observations.
func MappingFingerprint(resources []string, flavors []Flavor) string {
	resources = slices.Clone(resources)
	slices.Sort(resources)
	resources = slices.Compact(resources)
	flavors = slices.Clone(flavors)
	slices.SortFunc(flavors, func(a, b Flavor) int { return cmp.Compare(a.Name, b.Name) })
	for i := range flavors {
		if len(flavors[i].NodeLabels) == 0 {
			flavors[i].NodeLabels = nil
		}
	}
	encoded, _ := json.Marshal(struct {
		Resources []string
		Flavors   []Flavor
	}{resources, flavors})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
