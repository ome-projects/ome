package projection

import (
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/shares"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

// Capacity is one cluster's allocatable for one (resource, flavor) pair.
//
// It comes from that member's own root node, which its local controller derives
// from the hardware actually there. The hub measures nothing itself — it has no
// credential to read a member's nodes and no business holding one.
type Capacity struct {
	Cluster        string
	ResourceName   string
	ResourceFlavor string

	// Allocatable is what a proportional split divides by: chips on nodes that
	// are Ready and not cordoned. Chips on a cordoned or NotReady node count
	// toward the member's high-water mark but cannot run anything, so they earn
	// no share.
	Allocatable resource.Quantity

	// HighWaterMark is the member's damped reading of installed capacity. It
	// sizes nothing; it decides whether the member has the flavor installed at
	// all, so a cluster whose nodes are all cordoned keeps its copies at zero
	// rather than losing them.
	HighWaterMark resource.Quantity
}

// Fleet is what the hub knows about its members on one pass.
//
// Registered and Reported are separate on purpose, and the difference between
// them is the whole point of the type. A proportional split divides by the
// fleet, so the set it OUGHT to have heard from is an input -- never inferred
// from whoever happened to answer.
type Fleet struct {
	// Registered is every member an admin has registered, whatever its current
	// reachability.
	Registered []string

	// Reported is the subset that answered this pass. A member registered but
	// not reporting leaves every proportional budget unresolved: its capacity
	// would otherwise leave the basis silently and every surviving member's
	// share would grow to fill the gap, while the absent member still holds the
	// projection it last received -- so the fleet would have granted more than
	// the admin authorized, and the excess would surface only when the member
	// came back.
	Reported []string

	// Capacity is what the reporting members hold. A member that answered and
	// has none of a given (resource, flavor) pair simply has no entry for it and
	// is apportioned nothing, which is a different thing from not answering.
	Capacity []Capacity
}

// unreported names the registered members that did not report, sorted.
func (f Fleet) unreported() []string {
	reported := make(map[string]struct{}, len(f.Reported))
	for _, c := range f.Reported {
		reported[c] = struct{}{}
	}
	var out []string
	for _, c := range f.Registered {
		if _, ok := reported[c]; !ok {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

// capacityReadings is the fleet's capacity as a proportional split reads it:
// what the members report per resource and flavor pair, and the registered
// members that did not report, which hold every proportional split.
type capacityReadings struct {
	byPair     map[budgetPair]pairReadings
	unreported []string
}

// pairReadings is what the members report for one resource and flavor pair.
type pairReadings struct {
	// weights is each reporting member's allocatable, sorted by cluster so a
	// split's tiebreak sees the same order however the readings were gathered.
	weights []shares.Weight
	// installed names the members whose high-water mark is above zero: the
	// flavor is there, so the member keeps its allowance even at a zero share.
	installed map[string]bool
}

// readCapacity groups the fleet's readings per pair.
func readCapacity(fleet Fleet) capacityReadings {
	byPair := map[budgetPair]pairReadings{}
	for _, c := range fleet.Capacity {
		pair := budgetPair{resource: c.ResourceName, flavor: c.ResourceFlavor}
		readings := byPair[pair]
		readings.weights = append(readings.weights, shares.Weight{Cluster: c.Cluster, Capacity: c.Allocatable})
		if !c.HighWaterMark.IsZero() {
			if readings.installed == nil {
				readings.installed = map[string]bool{}
			}
			readings.installed[c.Cluster] = true
		}
		byPair[pair] = readings
	}
	for pair, readings := range byPair {
		sort.Slice(readings.weights, func(i, j int) bool { return readings.weights[i].Cluster < readings.weights[j].Cluster })
		byPair[pair] = readings
	}
	return capacityReadings{byPair: byPair, unreported: fleet.unreported()}
}

// ResolveOptions carry the fleet-level defaults a node may fall back on.
type ResolveOptions struct {
	// DefaultPolicy applies to a budget whose node declares no distribution.
	// Empty is not a policy: a node with nowhere left to fall back to is
	// reported unresolved rather than split by a guess.
	DefaultPolicy v1beta1.AcceleratorQuotaDistributionPolicy
}

// Resolution is one pass of the split.
type Resolution struct {
	// ByCluster is what to project, keyed by cluster name.
	ByCluster map[string][]Allowance
	// Unresolved names the leaves whose split could not be computed, and why.
	// A leaf appears here instead of in ByCluster, never in both: projecting a
	// half-resolved leaf would put a real budget on some clusters and silently
	// omit it on others, which reads as a working tenant that is quietly
	// missing capacity.
	Unresolved map[string]string
}

// Resolve computes every leaf's per-cluster allowance.
//
// Nothing here is fatal to the pass. A leaf whose policy cannot be determined,
// whose explicit split does not add up, or whose flavor no cluster reports yet
// is recorded as unresolved and skipped; the rest of the fleet is still
// projected. One misauthored tenant must not hold every other tenant's quota.
//
// The distinction that matters most is between "cannot" and "not yet".
// Proportional splits against capacity a member reports on its own root, so
// before members are up there is nothing to be proportional to — a startup
// ordering, not a misconfiguration. Both land in Unresolved, but the message
// says which, because one clears itself and the other needs an admin.
func Resolve(t *tree.Tree, fleet Fleet, opts ResolveOptions) Resolution {
	out := Resolution{
		ByCluster:  map[string][]Allowance{},
		Unresolved: map[string]string{},
	}
	readings := readCapacity(fleet)
	for _, leaf := range t.Leaves() {
		resolved, reason := resolveLeaf(leaf, readings, opts)
		if reason != "" {
			out.Unresolved[leaf.Name()] = reason
			continue
		}
		for _, a := range onItsClusters(resolved) {
			out.ByCluster[a.cluster] = append(out.ByCluster[a.cluster], a.Allowance)
		}
	}

	for c := range out.ByCluster {
		a := out.ByCluster[c]
		sort.Slice(a, func(i, j int) bool {
			if a[i].Node != a[j].Node {
				return a[i].Node < a[j].Node
			}
			if a[i].ResourceName != a[j].ResourceName {
				return a[i].ResourceName < a[j].ResourceName
			}
			return a[i].ResourceFlavor < a[j].ResourceFlavor
		})
		out.ByCluster[c] = a
	}
	return out
}

// placed is an allowance together with the cluster it landed on.
type placed struct {
	cluster string
	// installed is set when the cluster has capacity of the budget's flavor
	// installed, which keeps the allowance on it even when its share is zero.
	installed bool
	Allowance
}

// onItsClusters keeps a leaf's allowances on the clusters it belongs on.
//
// A leaf is on a cluster when it was apportioned something there, or when a
// Proportional budget's flavor is installed there. A zero share on a cluster
// that never had the flavor would create a queue that can admit nothing and a
// tenant wondering why. A zero share on a cluster whose nodes are all cordoned
// or NotReady is different: the queue there still holds the tenant's pending
// and admitted work, and deleting it would strand both until capacity returns.
// The allowances go together or not at all, so a copy carries every budget or
// none.
func onItsClusters(resolved []placed) []placed {
	keep := map[string]bool{}
	for _, a := range resolved {
		if a.installed || !a.Nominal.IsZero() {
			keep[a.cluster] = true
		}
	}
	out := make([]placed, 0, len(resolved))
	for _, a := range resolved {
		if keep[a.cluster] {
			out = append(out, a)
		}
	}
	return out
}

// resolveLeaf splits every budget on one leaf, or explains why it could not.
func resolveLeaf(leaf *tree.Node, readings capacityReadings, opts ResolveOptions) ([]placed, string) {
	var out []placed
	for _, b := range leaf.Quota.Spec.Budgets {
		split, reason := splitBudget(leaf.Name(), b, effectivePolicy(leaf.Quota, b, opts), readings)
		if reason != "" {
			return nil, reason
		}
		out = append(out, split...)
	}
	return out, ""
}

// splitBudget resolves one budget under its policy, or says why it cannot. No
// policy at any level is the case the default branch names.
func splitBudget(node string, b v1beta1.AcceleratorBudget, policy v1beta1.AcceleratorQuotaDistributionPolicy, readings capacityReadings) ([]placed, string) {
	switch policy {
	case v1beta1.AcceleratorQuotaDistributionExplicit:
		return explicit(node, b)
	case v1beta1.AcceleratorQuotaDistributionProportional:
		return proportional(node, b, readings)
	default:
		return nil, fmt.Sprintf(
			"budget %s/%s has no distribution policy, and none is configured fleet-wide",
			b.ResourceName, b.ResourceFlavor)
	}
}

// effectivePolicy is the budget's own policy, then the node's, then the fleet
// default. Each is a deliberate override of the one behind it.
func effectivePolicy(q *v1beta1.AcceleratorQuota, b v1beta1.AcceleratorBudget,
	opts ResolveOptions,
) v1beta1.AcceleratorQuotaDistributionPolicy {
	if b.Policy != "" {
		return b.Policy
	}
	if q.Spec.Distribution != nil && q.Spec.Distribution.Policy != "" {
		return q.Spec.Distribution.Policy
	}
	return opts.DefaultPolicy
}

// explicit takes the admin's split verbatim, once it adds up.
//
// The sum is checked here rather than trusted from admission, because a hub
// re-splits on every pass and a source can be edited between them. A split that
// is short leaves capacity stranded; one that is over hands the fleet more than
// was authorized, which is the failure the tree exists to prevent.
func explicit(node string, b v1beta1.AcceleratorBudget) ([]placed, string) {
	if len(b.PerCluster) == 0 {
		return nil, fmt.Sprintf(
			"budget %s/%s is Explicit but names no per-cluster shares",
			b.ResourceName, b.ResourceFlavor)
	}
	split := make([]shares.Share, 0, len(b.PerCluster))
	seen := map[string]struct{}{}
	for _, p := range b.PerCluster {
		if _, dup := seen[p.Cluster]; dup {
			return nil, fmt.Sprintf("budget %s/%s names cluster %s twice",
				b.ResourceName, b.ResourceFlavor, p.Cluster)
		}
		seen[p.Cluster] = struct{}{}
		split = append(split, shares.Share{Cluster: p.Cluster, Nominal: p.Nominal})
	}
	if sum := shares.Sum(split); sum.Cmp(b.Nominal) != 0 {
		return nil, fmt.Sprintf("budget %s/%s is %s but its per-cluster shares sum to %s",
			b.ResourceName, b.ResourceFlavor, b.Nominal.String(), sum.String())
	}
	sort.Slice(split, func(i, j int) bool { return split[i].Cluster < split[j].Cluster })
	return attach(node, b, split, nil), ""
}

// proportional splits the budget over the members in proportion to the
// capacity they report. It holds while a registered member has not reported,
// and waits while no member reports the flavor.
//
// The hold comes before the arithmetic, not after: a basis missing a
// registered member divides the fleet total among whoever answered, which
// over-grants by exactly the absent member's share. Holding is wrong by at
// most the change an admin just made; splitting anyway is wrong by the whole
// of what is missing.
func proportional(node string, b v1beta1.AcceleratorBudget, readings capacityReadings) ([]placed, string) {
	if len(readings.unreported) > 0 {
		return nil, fmt.Sprintf(
			"holding %s/%s: %v has not reported capacity, and splitting without it would over-grant the fleet",
			b.ResourceName, b.ResourceFlavor, readings.unreported)
	}
	reported := readings.byPair[budgetPair{resource: b.ResourceName, flavor: b.ResourceFlavor}]
	if len(reported.weights) == 0 {
		return nil, fmt.Sprintf(
			"no cluster has reported capacity for %s/%s yet, so a proportional split has nothing to divide by",
			b.ResourceName, b.ResourceFlavor)
	}
	split, err := shares.Proportional(b.Nominal, reported.weights)
	if err != nil {
		return nil, fmt.Sprintf("splitting %s/%s: %v", b.ResourceName, b.ResourceFlavor, err)
	}
	return attach(node, b, split, reported.installed), ""
}

// attach pairs a split with the budget it resolves; installed names the
// clusters that keep their allowance at a zero share.
func attach(node string, b v1beta1.AcceleratorBudget, split []shares.Share, installed map[string]bool) []placed {
	out := make([]placed, 0, len(split))
	for _, s := range split {
		out = append(out, placed{
			cluster:   s.Cluster,
			installed: installed[s.Cluster],
			Allowance: Allowance{
				Node:           node,
				ResourceName:   b.ResourceName,
				ResourceFlavor: b.ResourceFlavor,
				Nominal:        s.Nominal.DeepCopy(),
			},
		})
	}
	return out
}
