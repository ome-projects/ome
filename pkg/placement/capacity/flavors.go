package capacity

import (
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	quotacapacity "sigs.k8s.io/ome/pkg/quota/capacity"
)

// AttributeUnit maps each rendered pod set to one verified hardware flavor.
// Reports must be the member root's complete local observations, not fleet rows.
// This checks mapping provenance; Reader checks hardware quantities and freshness.
func AttributeUnit(unit UnitDemand, flavors []quotacapacity.Flavor, reports []v1beta1.AcceleratorCapacityStatus) (Demand, error) {
	if unit.Fingerprint == "" || len(unit.Pods) == 0 || unit.PrimaryUnits < 0 {
		return Demand{}, fmt.Errorf("flavor attribution requires measured replica demand")
	}
	mapping, rows, err := verifiedMapping(flavors, reports)
	if err != nil {
		return Demand{}, err
	}
	keys := flavorLabelKeys(flavors)
	pools := map[pair]Pool{}
	seen := map[string]bool{}
	for _, pod := range unit.Pods {
		id := string(pod.Component) + "/" + pod.Name
		if !validMeasuredPodSet(pod) || seen[id] {
			return Demand{}, fmt.Errorf("invalid measured pod set %q", id)
		}
		seen[id] = true
		if len(pod.Requests) == 0 {
			continue
		}
		flavor, err := flavorFor(id, pod.Spec, flavors, keys)
		if err != nil {
			return Demand{}, err
		}
		for name, quantity := range pod.Requests {
			demand, exact := wholeUnits(quantity)
			if !isExtendedResource(name) || !exact || demand <= 0 {
				return Demand{}, fmt.Errorf("pod set %s has invalid accelerator demand for %s", id, name)
			}
			key := pair{resource: string(name), flavor: flavor.Name}
			if row, exists := rows[key]; !exists || !row.Attribution.Complete {
				return Demand{}, fmt.Errorf("complete hardware attribution is missing for %s/%s", name, flavor.Name)
			}
			if err := addPoolDemand(pools, key, flavor, mapping, demand); err != nil {
				return Demand{}, err
			}
		}
	}
	if len(pools) == 0 {
		return Demand{}, fmt.Errorf("replica unit has no accelerator pools")
	}
	out := Demand{Pools: sortedPools(pools), PrimaryUnits: max(unit.PrimaryUnits, 1)}
	out.Fingerprint, err = fingerprint(struct {
		Unit, Mapping string
		Pools         []Pool
	}{unit.Fingerprint, mapping, out.Pools})
	if err != nil {
		return Demand{}, err
	}
	return out, nil
}

// validMeasuredPodSet reports whether a measured pod set is an engine or
// decoder set with a name, a positive count and a rendered spec.
func validMeasuredPodSet(pod PodDemand) bool {
	return pod.Spec != nil && pod.Name != "" && pod.Count > 0 &&
		(pod.Component == v1beta1.EngineComponent || pod.Component == v1beta1.DecoderComponent)
}

// flavorLabelKeys is the set of node-label keys the catalog uses; only those
// keys of a pod's scheduling constraints select hardware.
func flavorLabelKeys(flavors []quotacapacity.Flavor) sets.Set[string] {
	keys := sets.New[string]()
	for _, flavor := range flavors {
		for key := range flavor.NodeLabels {
			keys.Insert(key)
		}
	}
	return keys
}

// flavorFor selects the one flavor whose node labels satisfy the pod's
// hardware constraints; no match or more than one is an error.
func flavorFor(id string, spec *corev1.PodSpec, flavors []quotacapacity.Flavor, keys sets.Set[string]) (*quotacapacity.Flavor, error) {
	selector, err := hardwareSelector(spec, keys)
	if err != nil {
		return nil, fmt.Errorf("pod set %s: %w", id, err)
	}
	var selected *quotacapacity.Flavor
	for i := range flavors {
		flavor := &flavors[i]
		matches, err := selector.Match(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: flavor.NodeLabels}})
		if err != nil {
			return nil, fmt.Errorf("pod set %s: %w", id, err)
		}
		if !matches {
			continue
		}
		if selected != nil {
			return nil, fmt.Errorf("pod set %s matches multiple hardware flavors", id)
		}
		selected = flavor
	}
	if selected == nil {
		return nil, fmt.Errorf("pod set %s has no compatible hardware flavor", id)
	}
	return selected, nil
}

// addPoolDemand adds whole accelerator units to the pool of one resource and
// flavor, creating the pool from the flavor's identity on first use.
func addPoolDemand(pools map[pair]Pool, key pair, flavor *quotacapacity.Flavor, mapping string, demand int64) error {
	pool, exists := pools[key]
	if !exists {
		pool = Pool{ResourceName: key.resource, ResourceFlavor: key.flavor, FlavorUID: flavor.UID,
			NodeLabels: maps.Clone(flavor.NodeLabels), FlavorSetHash: mapping}
	}
	pool.Quantity.Add(*resource.NewQuantity(demand, resource.DecimalSI))
	if _, exact := wholeUnits(pool.Quantity); !exact {
		return fmt.Errorf("combined demand exceeds whole int64 units for %s/%s", key.resource, key.flavor)
	}
	pools[key] = pool
	return nil
}

// sortedPools lists the pools by resource name, then flavor name.
func sortedPools(pools map[pair]Pool) []Pool {
	out := slices.Collect(maps.Values(pools))
	slices.SortFunc(out, func(a, b Pool) int {
		return pair{a.ResourceName, a.ResourceFlavor}.compare(pair{b.ResourceName, b.ResourceFlavor})
	})
	return out
}

func verifiedMapping(flavors []quotacapacity.Flavor, reports []v1beta1.AcceleratorCapacityStatus) (string, map[pair]v1beta1.AcceleratorCapacityStatus, error) {
	if len(flavors) == 0 {
		return "", nil, fmt.Errorf("hardware flavor catalog is empty")
	}
	catalog := map[string]quotacapacity.Flavor{}
	for _, flavor := range flavors {
		if flavor.Name == "" || flavor.UID == "" {
			return "", nil, fmt.Errorf("hardware flavor has no verified identity")
		}
		if _, exists := catalog[flavor.Name]; exists {
			return "", nil, fmt.Errorf("duplicate hardware flavor %q", flavor.Name)
		}
		for key, value := range flavor.NodeLabels {
			if key == "" || value == "" {
				return "", nil, fmt.Errorf("flavor %q has an empty node-label key or value; reported label presence cannot be verified", flavor.Name)
			}
		}
		catalog[flavor.Name] = flavor
	}
	resources := sets.New[string]()
	rows := map[pair]v1beta1.AcceleratorCapacityStatus{}
	seen := map[pair]bool{}
	for _, row := range reports {
		key := pair{resource: row.ResourceName, flavor: row.ResourceFlavor}
		if seen[key] {
			return "", nil, fmt.Errorf("duplicate capacity report for %s/%s", key.resource, key.flavor)
		}
		seen[key] = true
		if row.Attribution == nil {
			continue
		}
		flavor, exists := catalog[row.ResourceFlavor]
		if !exists || len(row.PerCluster) != 0 || !isExtendedResource(corev1.ResourceName(row.ResourceName)) ||
			row.Attribution.FlavorUID != flavor.UID || !maps.Equal(row.Attribution.NodeLabels, flavor.NodeLabels) {
			return "", nil, fmt.Errorf("unverified local capacity mapping for %s/%s", key.resource, key.flavor)
		}
		resources.Insert(row.ResourceName)
		rows[key] = row
	}
	if len(resources) == 0 {
		return "", nil, fmt.Errorf("capacity reports have no identified resource mapping")
	}
	mapping := quotacapacity.MappingFingerprint(sets.List(resources), flavors)
	for resource := range resources {
		for _, flavor := range flavors {
			row, exists := rows[pair{resource: resource, flavor: flavor.Name}]
			if !exists || row.Attribution.FlavorSetHash != mapping {
				return "", nil, fmt.Errorf("capacity mapping is incomplete or changed for %s/%s", resource, flavor.Name)
			}
		}
	}
	return mapping, rows, nil
}

// Hardware selection uses the flavor catalog's label keys. Other scheduling
// constraints remain in the measured templates and do not imply hardware fit.
func hardwareSelector(spec *corev1.PodSpec, keys sets.Set[string]) (nodeaffinity.RequiredNodeAffinity, error) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{NodeSelector: map[string]string{}}}
	for key, value := range spec.NodeSelector {
		if keys.Has(key) {
			pod.Spec.NodeSelector[key] = value
		}
	}
	if spec.Affinity != nil && spec.Affinity.NodeAffinity != nil && spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		var terms []corev1.NodeSelectorTerm
		for _, term := range spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			var expressions []corev1.NodeSelectorRequirement
			for _, expression := range term.MatchExpressions {
				if keys.Has(expression.Key) {
					expressions = append(expressions, expression)
				}
			}
			if len(expressions) == 0 {
				terms = nil
				break
			}
			terms = append(terms, corev1.NodeSelectorTerm{MatchExpressions: expressions})
		}
		if len(terms) > 0 {
			selector := &corev1.NodeSelector{NodeSelectorTerms: terms}
			if _, err := nodeaffinity.NewNodeSelector(selector); err != nil {
				return nodeaffinity.RequiredNodeAffinity{}, err
			}
			pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: selector}}
		}
	}
	return nodeaffinity.GetRequiredNodeAffinity(pod), nil
}
