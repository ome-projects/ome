package acceleratorquota

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/capacity"
)

// attributeCapacity includes explicit zeros for every configured resource and
// observed flavor. Incomplete mappings carry evidence but cannot size placement.
func attributeCapacity(observed capacity.Result, flavors []capacity.Flavor, resources []string) ([]capacity.Capacity, map[string]*v1beta1.AcceleratorCapacityAttribution) {
	resources = slices.Clone(resources)
	slices.Sort(resources)
	resources = slices.Compact(resources)
	flavors = slices.Clone(flavors)
	slices.SortFunc(flavors, func(a, b capacity.Flavor) int { return cmp.Compare(a.Name, b.Name) })
	verified := true
	for i := range flavors {
		verified = verified && flavors[i].UID != ""
		if len(flavors[i].NodeLabels) == 0 {
			flavors[i].NodeLabels = nil
		}
	}
	encoded, _ := json.Marshal(struct {
		Resources []string
		Flavors   []capacity.Flavor
	}{resources, flavors})
	sum := sha256.Sum256(encoded)
	fingerprint := hex.EncodeToString(sum[:])
	incomplete := map[string]bool{}
	for _, missing := range observed.Unattributed {
		incomplete[missing.ResourceName] = true
	}
	index := map[string]capacity.Capacity{}
	for _, sample := range observed.Capacities {
		index[budgetKey(sample.ResourceName, sample.ResourceFlavor)] = sample
	}
	var samples []capacity.Capacity
	attributions := map[string]*v1beta1.AcceleratorCapacityAttribution{}
	for _, resource := range resources {
		for _, flavor := range flavors {
			key := budgetKey(resource, flavor.Name)
			sample := index[key]
			sample.ResourceName, sample.ResourceFlavor = resource, flavor.Name
			samples = append(samples, sample)
			attributions[key] = &v1beta1.AcceleratorCapacityAttribution{
				FlavorUID: flavor.UID, NodeLabels: maps.Clone(flavor.NodeLabels), FlavorSetHash: fingerprint,
				Complete: verified && !incomplete[resource],
			}
		}
	}
	return samples, attributions
}
