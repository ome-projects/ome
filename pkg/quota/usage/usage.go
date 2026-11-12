// Package usage rolls observed consumption up the AcceleratorQuota tree.
//
// A quota tree is described by three distinct numbers:
//
//   - spec.budgets, the allowance an admin authorized;
//   - the root's status.capacity, the installed hardware, derived by
//     pkg/quota/capacity;
//   - status.budgets, the amount workloads hold right now, computed here.
//
// All three are needed to diagnose a tenant whose work will not start. The
// cause may be its authorized limit, or accelerators that have left the
// cluster, or no pending demand at all — three states that are
// indistinguishable from any single one of the numbers.
//
// Consumption is summed upward from the leaves. Only a leaf materializes a
// queue that can hold anything; internal nodes render as topology alone, with
// no resourceGroups of their own. Every accelerator in use is therefore
// charged to exactly one leaf, so summing a subtree cannot count the same one
// twice.
//
// That last property depends on how OME renders the tree, not on any guarantee
// from the enforcement backend — a backend free to charge an internal node
// directly would break it. It is restated at each place a sum is taken rather
// than assumed once here.
package usage

import (
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/quota/tree"
)

// Key is the (resource, flavor) pair quota is keyed by.
type Key struct {
	ResourceName   string
	ResourceFlavor string
}

// Observed is one leaf queue's reading for one Key.
type Observed struct {
	// Admitted is held by workloads the backend has fully admitted, including
	// anything borrowed.
	Admitted resource.Quantity

	// Reserved is held by workloads holding a quota reservation, whether or not
	// they are admitted yet. It is never below Admitted, and the gap is work
	// that owns the chips but has not started — a tenant can sit at its ceiling
	// with Admitted low, which reads as idle unless this is carried too.
	Reserved resource.Quantity

	// Borrowed is the part of Admitted above this queue's own nominal. It is
	// the backend's own figure, measured against the queue it actually wrote.
	Borrowed resource.Quantity
}

// Total is one node's rolled-up reading.
type Total struct {
	Admitted resource.Quantity
	Reserved resource.Quantity
	Borrowed resource.Quantity
}

// Roll sums the leaf readings onto every node of the tree.
//
// leaves is keyed by node name; a node absent from it contributes nothing,
// which is how "no queue materialized yet" differs from "a queue holding
// nothing". The result carries an entry only for nodes with something to
// report, so an idle fleet allocates nothing and writes no status.
//
// Admitted and Reserved sum freely: each leaf's figure is disjoint from its
// siblings', because a leaf is the only thing that holds quota. Borrowed does
// not sum: a leaf reports the backend's own figure, and a node above a leaf
// reports what it holds above its own nominal (borrowedAboveNominal).
func Roll(t *tree.Tree, leaves map[string]map[Key]Observed) map[string]map[Key]Total {
	if t == nil || len(leaves) == 0 {
		return nil
	}

	out := make(map[string]map[Key]Total)
	for _, node := range t.Nodes() {
		totals := subtreeTotals(t, node, leaves)
		if totals == nil {
			continue
		}
		if !node.IsLeaf() {
			borrowedAboveNominal(node, totals)
		}
		out[node.Name()] = totals
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// subtreeTotals sums the readings of the leaves under node, nil when none has
// one. Only a leaf carries the backend's borrowed figure: a loan between two
// siblings is internal to the subtree above them, so summing the children's
// figures would report that subtree as borrowing from outside itself.
func subtreeTotals(t *tree.Tree, node *tree.Node, leaves map[string]map[Key]Observed) map[Key]Total {
	var totals map[Key]Total
	for _, d := range t.Subtree(node.Name()) {
		if !d.IsLeaf() {
			continue
		}
		for key, o := range leaves[d.Name()] {
			if totals == nil {
				totals = make(map[Key]Total)
			}
			agg := totals[key]
			add(&agg.Admitted, o.Admitted)
			add(&agg.Reserved, o.Reserved)
			if node.IsLeaf() {
				add(&agg.Borrowed, o.Borrowed)
			}
			totals[key] = agg
		}
	}
	return totals
}

// borrowedAboveNominal recomputes an internal node's borrowed figure as the
// admitted quantity above what that node was authored to hold.
//
// A node with no authored budget for a pair reports no borrowing rather than
// treating an absent budget as a nominal of zero: a grouping tier sized later
// is unconstrained on that pair, not instantly and entirely in debt.
func borrowedAboveNominal(node *tree.Node, totals map[Key]Total) {
	nominal := make(map[Key]resource.Quantity, len(node.Quota.Spec.Budgets))
	for _, b := range node.Quota.Spec.Budgets {
		nominal[Key{ResourceName: b.ResourceName, ResourceFlavor: b.ResourceFlavor}] = b.Nominal
	}
	for key, t := range totals {
		t.Borrowed = resource.Quantity{}
		if n, authored := nominal[key]; authored {
			t.Borrowed = overage(t.Admitted, n)
		}
		totals[key] = t
	}
}

// overage is how far admitted stands above nominal, zero when it does not.
func overage(admitted resource.Quantity, nominal resource.Quantity) resource.Quantity {
	over := admitted.DeepCopy()
	over.Sub(nominal)
	if over.Sign() <= 0 {
		return resource.Quantity{}
	}
	return over
}

// add accumulates into dst without mutating src, which belongs to the caller.
func add(dst *resource.Quantity, src resource.Quantity) {
	if dst.IsZero() && dst.Format == "" {
		*dst = src.DeepCopy()
		return
	}
	dst.Add(src)
}
