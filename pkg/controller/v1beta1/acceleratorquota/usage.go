package acceleratorquota

import (
	"context"
	"fmt"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/quota/backend"
	"sigs.k8s.io/ome/pkg/quota/tree"
	"sigs.k8s.io/ome/pkg/quota/usage"
)

// usageReading is one pass's consumption figures, and whether they were
// obtained at all.
//
// The flag is not redundant with an empty map. A fleet holding nothing and a
// backend that could not be read both produce no totals, and writing zeros for
// the second would report an idle fleet on the strength of a failed API call —
// exactly when an operator is most likely to be looking.
type usageReading struct {
	observed bool
	totals   map[string]map[usage.Key]usage.Total
}

// forNode returns one node's totals and whether consumption is known at all.
func (u usageReading) forNode(name string) (map[usage.Key]usage.Total, bool) {
	if !u.observed {
		return nil, false
	}
	return u.totals[name], true
}

// rollUsage reads what the backend currently holds and sums it onto every node.
//
// Read on the ordinary reconcile cadence rather than from a watch on the
// backend's own objects. Consumption changes on every admission, so a watch
// would turn a busy fleet into a status write per admitted pod — each of which
// wakes the reconcile again. The resync tick is the flush interval, which keeps
// the write rate bounded by configuration instead of by tenant behaviour.
// Usage is observational: nothing in the control path reads it back, so being
// one tick stale costs nothing.
func (r *Reconciler) rollUsage(ctx context.Context, built *tree.Tree) (usageReading, error) {
	reader, ok := r.Materialize.Backend.(backend.UsageReader)
	if !ok {
		// A backend that does not report consumption leaves the numbers unset
		// rather than reporting zero, which would claim an idle fleet.
		return usageReading{}, nil
	}

	readings, err := reader.ReadUsage(ctx, built.Leaves())
	if err != nil {
		return usageReading{}, fmt.Errorf("reading %s usage: %w", r.Materialize.Backend.Name(), err)
	}
	return usageReading{observed: true, totals: usage.Roll(built, readings)}, nil
}

// equalBudgets compares two budget statuses entry by entry, in order.
//
// Order is part of the comparison rather than something to normalize away: the
// list is rendered from spec.budgets, so a reordering there is a real change an
// operator made and the status should follow it.
//
// Without this, a pass whose only change is consumption is indistinguishable
// from an idle one, and the status write is skipped. The figures would then be
// written once — on the first pass, riding along with the path and conditions —
// and never move again.
func equalBudgets(a, b []v1beta1.AcceleratorBudgetStatus) bool {
	return slices.EqualFunc(a, b, sameBudget)
}

// sameBudget compares one budget row: the pair it names, the nominal, the
// totals held against it and the per-member breakdown. The breakdown moves
// independently of the totals it sums to: two members can trade admitted work
// between them and leave the fleet figure unchanged, so comparing only the
// totals would keep the breakdown at whatever it held when first written.
func sameBudget(x, y v1beta1.AcceleratorBudgetStatus) bool {
	return x.ResourceName == y.ResourceName && x.ResourceFlavor == y.ResourceFlavor &&
		x.Nominal.Cmp(y.Nominal) == 0 &&
		x.Admitted.Cmp(y.Admitted) == 0 &&
		x.Reserved.Cmp(y.Reserved) == 0 &&
		x.Borrowed.Cmp(y.Borrowed) == 0 &&
		equalPerCluster(x.PerCluster, y.PerCluster)
}

// totalsKey is the (resource, flavor) pair a budget's totals are kept under.
func totalsKey(b v1beta1.AcceleratorBudget) usage.Key {
	return usage.Key{ResourceName: b.ResourceName, ResourceFlavor: b.ResourceFlavor}
}

// setHeldTotals copies the totals held against a budget onto its status row.
func setHeldTotals(row *v1beta1.AcceleratorBudgetStatus, held usage.Total) {
	row.Admitted = held.Admitted.DeepCopy()
	row.Reserved = held.Reserved.DeepCopy()
	row.Borrowed = held.Borrowed.DeepCopy()
}

// budgetStatus pairs each authored budget with what is being held against it.
//
// Driven by the spec's budgets rather than by what came back: the backend also
// reports the cpu and memory cover every ClusterQueue carries, and those are a
// ceiling deliberately set high enough not to bind, not an allowance anyone
// authored. Listing them here would bury a tenant's two accelerator lines under
// resources nobody is managing.
//
// A budget with nothing held against it still gets an entry, at zero. That is
// the difference between a budget nobody is using and a budget that does not
// exist, and both are worth being able to see.
func budgetStatus(node *tree.Node, totals map[usage.Key]usage.Total) []v1beta1.AcceleratorBudgetStatus {
	budgets := node.Quota.Spec.Budgets
	if len(budgets) == 0 {
		return nil
	}

	out := make([]v1beta1.AcceleratorBudgetStatus, 0, len(budgets))
	for _, b := range budgets {
		status := v1beta1.AcceleratorBudgetStatus{
			ResourceName:   b.ResourceName,
			ResourceFlavor: b.ResourceFlavor,
			Nominal:        b.Nominal.DeepCopy(),
		}
		if held, ok := totals[totalsKey(b)]; ok {
			setHeldTotals(&status, held)
		}
		out = append(out, status)
	}
	return out
}
