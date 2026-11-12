// Package capacity sums the accelerator capacity a cluster actually has.
//
// The quota tree says what an admin authorized; this says what is installed.
// Comparing the two is what turns a budget from a number someone typed into one
// the cluster can honour, and it is the input to the reserved root's derived
// capacity.
//
// Pure: it takes a node list and returns totals, touching no cluster. Every
// judgement it makes is reported rather than assumed — capacity on an
// unschedulable node is counted separately rather than dropped, and accelerator
// capacity that matches no flavor is surfaced rather than ignored, because both
// are silent ways for a fleet to look smaller than it is.
package capacity

import (
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

// Options is config-driven with no in-code defaults: an unset field disables
// the behaviour it governs rather than assuming a value.
type Options struct {
	// AcceleratorResources are the extended resource names that count as
	// accelerator capacity, in full: "google.com/tpu", "nvidia.com/gpu",
	// "amd.com/gpu".
	//
	// Exact names rather than a pattern, because this list decides what a
	// budget is measured against and a pattern would quietly widen that. A
	// vendor nobody listed contributes nothing, which is loud rather than
	// silent: a budget written against unmeasured hardware exceeds a capacity
	// of zero and says so.
	//
	// Empty disables derivation entirely: Sum returns nothing rather than
	// guessing which of a node's resources are accelerators.
	AcceleratorResources []string
}

// Flavor is a hardware class identified by the node labels it requires,
// mirroring Kueue's ResourceFlavor.spec.nodeLabels. Reference-only — OME does
// not own node labelling.
type Flavor struct {
	Name string
	UID  types.UID
	// NodeLabels must all match for a node to belong to this flavor. An empty
	// map matches every node, which is how a catch-all flavor is expressed.
	NodeLabels map[string]string
}

// Capacity is the summed allocatable for one (resource, flavor) pair — the
// granularity Kueue keys quota by.
type Capacity struct {
	ResourceName   string
	ResourceFlavor string
	// Allocatable is the total on nodes that could accept work now.
	Allocatable resource.Quantity
	// Unavailable is the total on nodes that matched the flavor but were
	// cordoned or not Ready. Reported separately so a budget is sized against
	// what is schedulable while an operator can still see what is parked.
	Unavailable resource.Quantity
	// Nodes and UnavailableNodes are the node counts behind the two totals,
	// which is what tells an operator whether a shortfall is one dead machine
	// or a rack.
	Nodes            int
	UnavailableNodes int
}

// Unattributed is accelerator capacity that exists but belongs to no flavor.
// Never silently dropped: a node advertising chips that no flavor claims is
// either a missing ResourceFlavor or a labelling mistake, and both look
// identical to a shrinking fleet unless they are named.
type Unattributed struct {
	Node         string
	ResourceName string
	Quantity     resource.Quantity
	// Reason is why the node could not be attributed.
	Reason string
}

// Reasons a node's capacity goes unattributed.
const (
	// ReasonNoFlavor means no flavor's node labels matched.
	ReasonNoFlavor = "NoMatchingFlavor"
	// ReasonAmbiguous means two flavors matched equally specifically, so
	// counting the node into either would be a guess and counting it into both
	// would inflate the fleet.
	ReasonAmbiguous = "AmbiguousFlavor"
)

// Result is what a cluster has, split into what is usable and what is not
// accounted for.
type Result struct {
	// Capacities is sorted by (resource, flavor).
	Capacities []Capacity
	// Unattributed is sorted by (node, resource).
	Unattributed []Unattributed
}

// Sum totals allocatable accelerator capacity per (resource, flavor).
//
// A node belongs to the flavor whose node labels it matches most specifically;
// an exact tie is ambiguous and counted nowhere, because double-counting would
// authorize a budget larger than the hardware. Duplicate flavor names are a
// caller error rather than a data condition, so they are an error return.
func Sum(nodes []corev1.Node, flavors []Flavor, opts Options) (Result, error) {
	if len(opts.AcceleratorResources) == 0 {
		return Result{}, nil
	}
	if err := validateFlavors(flavors); err != nil {
		return Result{}, err
	}

	wanted := make(map[string]struct{}, len(opts.AcceleratorResources))
	for _, name := range opts.AcceleratorResources {
		wanted[name] = struct{}{}
	}

	totals := map[resourceFlavor]*Capacity{}
	var unattributed []Unattributed
	for i := range nodes {
		node := &nodes[i]
		chips := acceleratorsOf(node, wanted)
		if len(chips) == 0 {
			continue
		}
		flavor, reason := match(node, flavors)
		if flavor == "" {
			unattributed = append(unattributed, unattributedChips(node.Name, chips, reason)...)
			continue
		}
		addNode(totals, chips, flavor, available(node))
	}
	return sortedResult(totals, unattributed), nil
}

// validateFlavors rejects a flavor without a name and a name declared twice.
func validateFlavors(flavors []Flavor) error {
	seen := make(map[string]struct{}, len(flavors))
	for _, f := range flavors {
		if f.Name == "" {
			return fmt.Errorf("quota capacity: a flavor has no name")
		}
		if _, dup := seen[f.Name]; dup {
			return fmt.Errorf("quota capacity: flavor %q is declared more than once", f.Name)
		}
		seen[f.Name] = struct{}{}
	}
	return nil
}

// resourceFlavor is the (resource, flavor) pair a total and a mark are kept
// for, the granularity Kueue keys quota by.
type resourceFlavor struct{ resource, flavor string }

// less orders pairs by resource, then flavor: the order every list this
// package returns is in, so a caller can diff two snapshots without
// normalising first.
func (k resourceFlavor) less(o resourceFlavor) bool {
	if k.resource != o.resource {
		return k.resource < o.resource
	}
	return k.flavor < o.flavor
}

// key is the pair the capacity is kept for.
func (c Capacity) key() resourceFlavor {
	return resourceFlavor{resource: c.ResourceName, flavor: c.ResourceFlavor}
}

// addNode adds one node's chips to the totals of the flavor it belongs to.
// Each chip is a distinct resource, so the node counts once in every
// (resource, flavor) entry it feeds: a node advertising both GPUs and TPUs is
// one machine in each of its two entries.
func addNode(totals map[resourceFlavor]*Capacity, chips []chip, flavor string, usable bool) {
	for _, c := range chips {
		k := resourceFlavor{resource: c.name, flavor: flavor}
		t, ok := totals[k]
		if !ok {
			t = &Capacity{ResourceName: c.name, ResourceFlavor: flavor}
			totals[k] = t
		}
		if usable {
			t.Allocatable.Add(c.quantity)
			t.Nodes++
		} else {
			t.Unavailable.Add(c.quantity)
			t.UnavailableNodes++
		}
	}
}

// unattributedChips reports every chip of a node no flavor claims, with the
// reason the node could not be attributed.
func unattributedChips(nodeName string, chips []chip, reason string) []Unattributed {
	out := make([]Unattributed, 0, len(chips))
	for _, c := range chips {
		out = append(out, Unattributed{
			Node: nodeName, ResourceName: c.name, Quantity: c.quantity, Reason: reason,
		})
	}
	return out
}

// sortedResult orders the totals by (resource, flavor) and the unattributed
// chips by (node, resource). Both slices are nil rather than empty when nothing
// was found, so the zero Result means "no capacity" and a caller can test it
// without a length check.
func sortedResult(totals map[resourceFlavor]*Capacity, unattributed []Unattributed) Result {
	var out Result
	for _, t := range totals {
		out.Capacities = append(out.Capacities, *t)
	}
	sort.Slice(out.Capacities, func(i, j int) bool { return out.Capacities[i].key().less(out.Capacities[j].key()) })
	sort.Slice(unattributed, func(i, j int) bool {
		if unattributed[i].Node != unattributed[j].Node {
			return unattributed[i].Node < unattributed[j].Node
		}
		return unattributed[i].ResourceName < unattributed[j].ResourceName
	})
	out.Unattributed = unattributed
	return out
}

type chip struct {
	name     string
	quantity resource.Quantity
}

// acceleratorsOf returns the node's allocatable entries named in the configured
// set, sorted by name.
//
// Allocatable rather than Capacity: capacity is what the hardware reports,
// allocatable is what the kubelet will hand out after reservations, and a
// budget written against the former over-promises.
func acceleratorsOf(node *corev1.Node, wanted map[string]struct{}) []chip {
	var out []chip
	for name, q := range node.Status.Allocatable {
		if q.IsZero() {
			continue
		}
		if _, ok := wanted[string(name)]; !ok {
			continue
		}
		out = append(out, chip{name: string(name), quantity: q.DeepCopy()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// available reports whether the node could accept work now.
//
// Ready and not cordoned, deliberately without a taint check: accelerator pools
// are routinely tainted so that only workloads tolerating them land there, so
// treating a taint as unavailable would report a dedicated fleet as having no
// capacity at all.
func available(node *corev1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// match returns the flavor a node belongs to, or "" and the reason it could not
// be attributed. Specificity is the number of labels matched, so a catch-all
// flavor loses to any flavor that names a label.
func match(node *corev1.Node, flavors []Flavor) (string, string) {
	best, bestLabels, tied := "", -1, false
	for _, f := range flavors {
		if !matchesLabels(node, f.NodeLabels) {
			continue
		}
		switch n := len(f.NodeLabels); {
		case n > bestLabels:
			best, bestLabels, tied = f.Name, n, false
		case n == bestLabels:
			tied = true
		}
	}
	switch {
	case best == "":
		return "", ReasonNoFlavor
	case tied:
		return "", ReasonAmbiguous
	default:
		return best, ""
	}
}

func matchesLabels(node *corev1.Node, want map[string]string) bool {
	for k, v := range want {
		if node.Labels[k] != v {
			return false
		}
	}
	return true
}
