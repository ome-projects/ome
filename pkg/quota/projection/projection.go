// Package projection renders the copy of a fleet quota tree that belongs on one
// workload cluster.
//
// A management plane authors one tree for the whole fleet, but a member can only
// enforce numbers of its own. So each member receives its own copy: same shape,
// same names, and in place of every fleet-wide allowance the share that member
// was apportioned. Rendering is separated from applying for the same reason the
// Kueue materializer is — the mapping is where the mistakes live, and a pure
// function can be tested exhaustively against a table.
//
// Three properties of a projection are load-bearing and none of them is obvious:
//
//   - It carries a RESOLVED number and never an unresolved split. The policy that
//     produced the share, and the per-cluster breakdown it came from, are dropped:
//     a member that could see them could re-derive a different answer, and two
//     planes computing the same split is how they drift.
//   - Its ancestors come too, but WITHOUT budgets. A leaf's parentRef has to
//     resolve on the member or the tree does not assemble there, so every tier
//     between the leaf and the root is projected — as pure topology, because a
//     fleet total is meaningless on one cluster and a budget-less node is
//     unconstrained for containment.
//   - The reserved root is NOT projected. The member's own controller creates it,
//     bare, and derives that cluster's capacity onto its status. Projecting one
//     would put two writers on a single object, which is the thing the whole
//     origin-marking scheme exists to prevent.
package projection

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/tree"
)

// Allowance is one budget's resolved share on one cluster. The caller resolves
// these — from perCluster under Explicit, or from pkg/quota/shares under
// Proportional — because how a split is computed is policy and this is not.
type Allowance struct {
	Node           string
	ResourceName   string
	ResourceFlavor string
	Nominal        resource.Quantity
}

// Options carry the projector's identity.
type Options struct {
	// Origin identifies the management plane. It is written on every projection
	// and is what tells a member's controller that a node was written from
	// outside — and what lets one plane's sweep ignore another's copies.
	Origin string
}

// For renders every object that belongs on one cluster.
//
// A leaf is on a cluster when it has allowances there, zero or not. Which
// clusters those are is Resolve's decision, made from the split and the
// capacity each member reports; nothing here selects them.
//
// The result is ordered parent-first, then by name. That order is a contract,
// not presentation: the caller applies the copies one at a time, and a member
// refuses a node whose parentRef resolves to nothing yet.
func For(t *tree.Tree, cluster string, allowances []Allowance, opts Options) ([]*v1beta1.AcceleratorQuota, error) {
	if cluster == "" {
		return nil, fmt.Errorf("no cluster named")
	}
	if opts.Origin == "" {
		// An unmarked projection is indistinguishable from a node an admin
		// authored on the member, which would make it un-sweepable and would
		// let the member's webhook admit edits to it.
		return nil, fmt.Errorf("no origin configured, so a projection could not be told from a local node")
	}
	byLeaf, err := allowancesByLeaf(t, allowances)
	if err != nil {
		return nil, err
	}
	nodes := projectedNodes(t, byLeaf)
	out := make([]*v1beta1.AcceleratorQuota, 0, len(nodes))
	for _, node := range nodes {
		rendered, err := render(node, cluster, byLeaf[node.Name()], opts)
		if err != nil {
			return nil, err
		}
		out = append(out, rendered)
	}
	return out, nil
}

// allowancesByLeaf groups the allowances by the leaf each resolves. Only a leaf
// materializes a queue that can hold anything, so an allowance on a grouping
// tier has nowhere to go.
func allowancesByLeaf(t *tree.Tree, allowances []Allowance) (map[string][]Allowance, error) {
	byLeaf := map[string][]Allowance{}
	for _, a := range allowances {
		node, ok := t.Node(a.Node)
		if !ok {
			return nil, fmt.Errorf("allowance names %s, which is not in the tree", a.Node)
		}
		if !node.IsLeaf() {
			return nil, fmt.Errorf("allowance names %s, which is not a leaf", a.Node)
		}
		byLeaf[a.Node] = append(byLeaf[a.Node], a)
	}
	return byLeaf, nil
}

// projectedNodes is every leaf with an allowance and every tier between it and
// the root, so parentRef resolves on the member, in the order the copies are
// applied. The root itself is the member's to create.
func projectedNodes(t *tree.Tree, byLeaf map[string][]Allowance) []*tree.Node {
	include := map[string]*tree.Node{}
	for name := range byLeaf {
		node, _ := t.Node(name)
		include[name] = node
		for _, ancestor := range node.Ancestors() {
			if ancestor.Parent != nil {
				include[ancestor.Name()] = ancestor
			}
		}
	}
	nodes := slices.Collect(maps.Values(include))
	slices.SortFunc(nodes, parentFirst)
	return nodes
}

// parentFirst orders nodes shallowest first, so a parent is always applied
// before the children that name it: the member's webhook rejects a node whose
// parentRef resolves to nothing, and an alphabetical order would deny every
// leaf that sorts before its own parent and converge a tier per pass. Name
// breaks ties within a tier, so the order is the same on every pass.
func parentFirst(a, b *tree.Node) int {
	return cmp.Or(cmp.Compare(a.Depth, b.Depth), strings.Compare(a.Name(), b.Name()))
}

// render builds one node's projected copy.
func render(node *tree.Node, cluster string, given []Allowance, opts Options) (*v1beta1.AcceleratorQuota, error) {
	src := node.Quota
	out := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name: src.Name,
			Labels: map[string]string{
				v1beta1.AcceleratorQuotaOriginLabel: opts.Origin,
			},
			Annotations: map[string]string{
				// The source's UID, so a projection left behind by a source that
				// was deleted and recreated under the same name is recognisable
				// as a leftover rather than adopted as current.
				v1beta1.AcceleratorQuotaOriginUIDAnnotation: string(src.UID),
				// The source's generation, echoed because generation is
				// object-local: a projection's own generation has no relation to
				// the source's, so comparing them would report nonsense.
				v1beta1.AcceleratorQuotaSourceGenerationAnnotation: fmt.Sprintf("%d", src.Generation),
				// Which cluster this copy was computed for. A copy that turns up
				// on the wrong member is then self-evidently wrong.
				v1beta1.AcceleratorQuotaClusterAnnotation: cluster,
			},
		},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role: src.Spec.Role,
		},
	}
	if src.Spec.ParentRef != nil {
		out.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: src.Spec.ParentRef.Name}
	}

	if !node.IsLeaf() {
		// Topology only. Tiers and budgets both belong to leaves, and a fleet
		// total on a grouping tier would be a number no member can hold.
		return out, nil
	}

	out.Spec.PriorityTier = src.Spec.PriorityTier
	out.Spec.Preemption = src.Spec.Preemption.DeepCopy()

	budgets, err := projectBudgets(src, given)
	if err != nil {
		return nil, err
	}
	out.Spec.Budgets = budgets
	return out, nil
}

// projectBudgets pairs each resolved share with the source budget it resolves,
// keeping the pass-through limits and dropping everything that describes a split.
func projectBudgets(src *v1beta1.AcceleratorQuota, given []Allowance) ([]v1beta1.AcceleratorBudget, error) {
	sources := map[budgetPair]v1beta1.AcceleratorBudget{}
	for _, b := range src.Spec.Budgets {
		sources[budgetPair{resource: b.ResourceName, flavor: b.ResourceFlavor}] = b
	}

	out := make([]v1beta1.AcceleratorBudget, 0, len(given))
	for _, a := range given {
		b, ok := sources[budgetPair{resource: a.ResourceName, flavor: a.ResourceFlavor}]
		if !ok {
			return nil, fmt.Errorf("node %s has no budget for %s/%s to resolve",
				src.Name, a.ResourceName, a.ResourceFlavor)
		}
		projected := v1beta1.AcceleratorBudget{
			ResourceName:   b.ResourceName,
			ResourceFlavor: b.ResourceFlavor,
			Nominal:        a.Nominal.DeepCopy(),
			// Passed through untouched: both are cluster-local limits Kueue
			// enforces on the member, not fleet quantities to be split.
			BorrowingLimit: copyQuantity(b.BorrowingLimit),
			LendingLimit:   copyQuantity(b.LendingLimit),
		}
		// Policy and PerCluster are deliberately not carried. A projection is a
		// resolved number; shipping the split that produced it would invite a
		// second plane to re-derive it and disagree.
		out = append(out, projected)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ResourceName != out[j].ResourceName {
			return out[i].ResourceName < out[j].ResourceName
		}
		return out[i].ResourceFlavor < out[j].ResourceFlavor
	})
	return out, nil
}

// budgetPair identifies one budget line: a resource on a flavor.
type budgetPair struct{ resource, flavor string }

func copyQuantity(q *resource.Quantity) *resource.Quantity {
	if q == nil {
		return nil
	}
	c := q.DeepCopy()
	return &c
}
