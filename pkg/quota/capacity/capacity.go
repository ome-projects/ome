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
	// Capacities is sorted by (resource, flavor) so a caller can diff two
	// snapshots without normalising first.
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
	seen := make(map[string]struct{}, len(flavors))
	for _, f := range flavors {
		if f.Name == "" {
			return Result{}, fmt.Errorf("quota capacity: a flavor has no name")
		}
		if _, dup := seen[f.Name]; dup {
			return Result{}, fmt.Errorf("quota capacity: flavor %q is declared more than once", f.Name)
		}
		seen[f.Name] = struct{}{}
	}

	wanted := make(map[string]struct{}, len(opts.AcceleratorResources))
	for _, name := range opts.AcceleratorResources {
		wanted[name] = struct{}{}
	}

	type key struct{ resource, flavor string }
	totals := map[key]*Capacity{}
	var unattributed []Unattributed

	for i := range nodes {
		node := &nodes[i]
		chips := acceleratorsOf(node, wanted)
		if len(chips) == 0 {
			continue
		}

		flavor, reason := match(node, flavors)
		if flavor == "" {
			for _, c := range chips {
				unattributed = append(unattributed, Unattributed{
					Node: node.Name, ResourceName: c.name, Quantity: c.quantity, Reason: reason,
				})
			}
			continue
		}

		usable := available(node)
		for _, c := range chips {
			k := key{resource: c.name, flavor: flavor}
			t, ok := totals[k]
			if !ok {
				t = &Capacity{ResourceName: c.name, ResourceFlavor: flavor}
				totals[k] = t
			}
			if usable {
				t.Allocatable.Add(c.quantity)
			} else {
				t.Unavailable.Add(c.quantity)
			}
		}
		// Counted once per node, not once per resource, so a node advertising
		// both GPUs and TPUs is one machine rather than two.
		for _, c := range chips {
			t := totals[key{resource: c.name, flavor: flavor}]
			if usable {
				t.Nodes++
			} else {
				t.UnavailableNodes++
			}
		}
	}

	// Nil rather than empty when nothing was found, so the zero Result means
	// "no capacity" and a caller can test it without a length check.
	var out Result
	for _, t := range totals {
		out.Capacities = append(out.Capacities, *t)
	}
	sort.Slice(out.Capacities, func(i, j int) bool {
		if out.Capacities[i].ResourceName != out.Capacities[j].ResourceName {
			return out.Capacities[i].ResourceName < out.Capacities[j].ResourceName
		}
		return out.Capacities[i].ResourceFlavor < out.Capacities[j].ResourceFlavor
	})
	sort.Slice(unattributed, func(i, j int) bool {
		if unattributed[i].Node != unattributed[j].Node {
			return unattributed[i].Node < unattributed[j].Node
		}
		return unattributed[i].ResourceName < unattributed[j].ResourceName
	})
	out.Unattributed = unattributed
	return out, nil
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
