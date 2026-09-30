// Package tpuslice derives the TPU slice a workload needs from what the
// workload already declares: the topology its pods select and the chips each
// pod requests.
//
// A slice is granted whole, and a TPU session must request exactly the slice's
// topology. Enough free chips is therefore not a schedulable condition; a
// slice of this exact shape is. Every check here rejects a request that does
// not fill its topology exactly instead of rounding it to a nearby shape,
// because a near miss fails at session start rather than at admission.
//
// Pure: no I/O and no provider types.
package tpuslice

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// maxDims is the most dimensions a TPU topology has.
const maxDims = 3

// Topology is a slice shape of two or three positive dimensions, such as 2x4
// or 2x2x1. The zero value is the absent topology. Comparable, so it can key
// a map.
type Topology struct {
	dims [maxDims]int64
	n    int
}

// ParseTopology parses the canonical "<X>x<Y>" or "<X>x<Y>x<Z>" form.
//
// The same string is a node label value that pods select on, so only the
// canonical spelling is accepted: signs, spaces and leading zeros parse to the
// same numbers but match no node.
func ParseTopology(s string) (Topology, error) {
	parts := strings.Split(s, "x")
	if len(parts) < 2 || len(parts) > maxDims {
		return Topology{}, fmt.Errorf("topology %q: want 2 or 3 dimensions separated by 'x'", s)
	}
	var t Topology
	chips := int64(1)
	for _, p := range parts {
		if !canonicalDim(p) {
			return Topology{}, fmt.Errorf("topology %q: dimension %q is not a positive integer without leading zeros", s, p)
		}
		d, err := strconv.ParseInt(p, 10, 64)
		if err != nil || chips > math.MaxInt64/d {
			return Topology{}, fmt.Errorf("topology %q: chip count overflows", s)
		}
		chips *= d
		t.dims[t.n] = d
		t.n++
	}
	return t, nil
}

func canonicalDim(p string) bool {
	if p == "" || p[0] == '0' {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] < '0' || p[i] > '9' {
			return false
		}
	}
	return true
}

// IsZero reports whether t is the absent topology.
func (t Topology) IsZero() bool { return t.n == 0 }

// Dims returns the dimensions in order.
func (t Topology) Dims() []int64 {
	out := make([]int64, t.n)
	copy(out, t.dims[:t.n])
	return out
}

// String returns the canonical form, which ParseTopology round-trips.
func (t Topology) String() string {
	parts := make([]string, t.n)
	for i := 0; i < t.n; i++ {
		parts[i] = strconv.FormatInt(t.dims[i], 10)
	}
	return strings.Join(parts, "x")
}

// Chips is the number of chips in the topology: the product of its
// dimensions. Zero for the absent topology.
func (t Topology) Chips() int64 {
	if t.IsZero() {
		return 0
	}
	chips := int64(1)
	for i := 0; i < t.n; i++ {
		chips *= t.dims[i]
	}
	return chips
}

// Hosts is how many hosts of chipsPerHost chips the topology spans. A
// topology that is not a whole number of hosts is an error, because a host is
// the smallest unit a slice grants.
func (t Topology) Hosts(chipsPerHost int64) (int64, error) {
	if t.IsZero() {
		return 0, fmt.Errorf("empty topology")
	}
	if chipsPerHost <= 0 {
		return 0, fmt.Errorf("chips per host must be positive, got %d", chipsPerHost)
	}
	chips := t.Chips()
	if chips%chipsPerHost != 0 {
		return 0, fmt.Errorf("topology %s is %d chips, not a whole number of %d-chip hosts", t, chips, chipsPerHost)
	}
	return chips / chipsPerHost, nil
}

// Tile checks that pods requesting podChips[i] chips each fill t exactly.
//
// Every pod must request the same positive count, that count must divide
// chipsPerHost, and together the pods must request exactly t.Chips(). A pod
// cannot span hosts, a host shared with a pod outside the slice leaves the
// slice's session short of its topology, and a slice not filled exactly fails
// at session start.
func Tile(t Topology, chipsPerHost int64, podChips []int64) error {
	hosts, err := t.Hosts(chipsPerHost)
	if err != nil {
		return err
	}
	if len(podChips) == 0 {
		return fmt.Errorf("topology %s needs %d chips but no pod requests any", t, t.Chips())
	}
	perPod := podChips[0]
	for i, n := range podChips {
		if n <= 0 {
			return fmt.Errorf("pod %d requests no chips; every pod on a %s slice must request chips", i, t)
		}
		if n != perPod {
			return fmt.Errorf("pods request %d and %d chips; every pod on a slice must request the same count", perPod, n)
		}
	}
	if perPod > chipsPerHost {
		return fmt.Errorf("a pod requests %d chips but a host has %d; a pod cannot span hosts", perPod, chipsPerHost)
	}
	if chipsPerHost%perPod != 0 {
		return fmt.Errorf("%d chips per pod do not divide a %d-chip host exactly", perPod, chipsPerHost)
	}
	chips := t.Chips()
	if chips%perPod != 0 || int64(len(podChips)) != chips/perPod {
		return fmt.Errorf("%d pods x %d chips do not fill topology %s: it is %d chips on %d hosts, which takes %d pods",
			len(podChips), perPod, t, chips, hosts, chips/perPod)
	}
	return nil
}

// Keys name the node labels that carry a slice's accelerator and topology.
// They are provider-specific, so callers supply them from configuration.
type Keys struct {
	Accelerator string
	Topology    string
}

// Shape is the slice a set of pods selects.
type Shape struct {
	// Accelerator is the accelerator label value the pods select.
	Accelerator string
	Topology    Topology
}

// FromNodeSelector reads the slice shape a nodeSelector names.
//
// found is false, with a nil error, when the selector names no topology: the
// pods ask for no slice. A selector that names a topology must also name the
// accelerator, since a slice is always of one accelerator type.
func FromNodeSelector(nodeSelector map[string]string, keys Keys) (shape Shape, found bool, err error) {
	if keys.Accelerator == "" || keys.Topology == "" {
		return Shape{}, false, fmt.Errorf("accelerator and topology label keys must both be set")
	}
	raw, ok := nodeSelector[keys.Topology]
	if !ok {
		return Shape{}, false, nil
	}
	accelerator := nodeSelector[keys.Accelerator]
	if accelerator == "" {
		return Shape{}, false, fmt.Errorf("nodeSelector names topology %q under %s but no accelerator under %s", raw, keys.Topology, keys.Accelerator)
	}
	t, err := ParseTopology(raw)
	if err != nil {
		return Shape{}, false, fmt.Errorf("nodeSelector %s: %w", keys.Topology, err)
	}
	return Shape{Accelerator: accelerator, Topology: t}, true, nil
}

// ContainerChips is the count of resource the containers request, each read
// from its limit and falling back to its request. Callers pass a pod's app
// containers: the chips a pod holds for its lifetime are theirs.
func ContainerChips(containers []corev1.Container, resource corev1.ResourceName) int64 {
	var total int64
	for i := range containers {
		r := &containers[i].Resources
		if q, ok := r.Limits[resource]; ok {
			total += q.Value()
		} else if q, ok := r.Requests[resource]; ok {
			total += q.Value()
		}
	}
	return total
}
