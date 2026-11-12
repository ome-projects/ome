// Package shares splits a fleet-wide allowance into per-cluster ones.
//
// A management plane authors one nominal for the whole fleet, but a workload
// cluster can only enforce a number of its own: Kueue's nominalQuota is
// per-cluster, so the fleet total has to be resolved into shares before it can
// be projected. This is that resolution and nothing else: no I/O, no knowledge
// of clusters beyond the names and weights it is handed.
//
// The shares sum to the total exactly. A split that loses a chip to rounding
// silently shrinks the fleet's usable capacity; one that gains a chip lets the
// fleet admit more than an admin authorized. Both are invisible in any single
// cluster's status.
//
// Exactness rules out floating point and truncating division. The method is
// largest remainder: every cluster takes its whole quotient, and the units that
// division could not place go to whoever was closest to earning one.
// Arithmetic is on inf.Dec rather than int64 because a budget may name any
// resource: cpu carries milli precision, and memory in binary units overflows
// MilliValue long before it overflows a cluster.
package shares

import (
	"fmt"
	"math/big"
	"sort"

	"gopkg.in/inf.v0"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Weight is one cluster's contribution to the basis a total is split by.
//
// For the Proportional policy this is that cluster's schedulable capacity of
// the flavor, as the cluster last reported it.
type Weight struct {
	Cluster  string
	Capacity resource.Quantity
}

// Share is one cluster's resolved allowance.
type Share struct {
	Cluster string
	Nominal resource.Quantity
}

// Proportional splits total across clusters in proportion to their weights.
//
// The result sums to total exactly, is ordered by cluster name, and is a
// function of the inputs alone: the same inputs always produce the same split,
// including which clusters receive the remainder. A split that reshuffled
// between reconciles would rewrite every projected CR and re-materialize every
// ClusterQueue in the fleet for no reason.
//
// Shares come back in the unit the total was written in: a budget of 4 splits
// into whole numbers, a budget of 4000m into thousandths. That is the only
// signal available about whether the resource can be subdivided at all, and
// getting it wrong writes a fractional accelerator quota that admits nothing.
//
// A total of zero is a valid split into zeroes; a cluster of zero weight is a
// valid share of zero. A basis that is entirely zero has nothing to be
// proportional to and is an error rather than an even split, because an even
// split is a different policy the admin did not ask for.
func Proportional(total resource.Quantity, weights []Weight) ([]Share, error) {
	if err := validateWeights(total, weights); err != nil {
		return nil, err
	}
	scale := decScale(total)
	basis, err := alignedBasis(weights)
	if err != nil {
		return nil, err
	}
	assigned := basis.apportion(unitsAt(total, scale))
	out := make([]Share, len(weights))
	for i, w := range weights {
		out[i] = Share{Cluster: w.Cluster, Nominal: quantityAt(assigned[i], scale, total.Format)}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Cluster < out[b].Cluster })
	return out, nil
}

// validateWeights refuses an empty cluster list, a negative total, an unnamed
// or repeated cluster and a negative capacity: the first problem in input order.
func validateWeights(total resource.Quantity, weights []Weight) error {
	if len(weights) == 0 {
		return fmt.Errorf("no clusters to split across")
	}
	if total.Sign() < 0 {
		return fmt.Errorf("total %s is negative", total.String())
	}
	seen := make(map[string]struct{}, len(weights))
	for _, w := range weights {
		if w.Cluster == "" {
			return fmt.Errorf("a weight carries no cluster name")
		}
		if _, dup := seen[w.Cluster]; dup {
			return fmt.Errorf("cluster %s appears twice", w.Cluster)
		}
		seen[w.Cluster] = struct{}{}
		if w.Capacity.Sign() < 0 {
			return fmt.Errorf("cluster %s has negative capacity %s", w.Cluster, w.Capacity.String())
		}
	}
	return nil
}

// basis is the weights as whole numbers at one common scale, with their sum.
//
// The weights set the ratio only, and a ratio is scale-free, so aligning them
// among themselves is enough; the answer keeps the unit the budget was written
// in. That is what keeps an indivisible resource indivisible: four chips over
// three clusters is 2/1/1, never 1.33 each. An operator who wants a finer
// split writes a finer budget, 4000m, and gets thousandths.
type basis struct {
	clusters []string
	units    []*big.Int
	sum      *big.Int
}

// alignedBasis renders every weight at the largest scale any weight needs, so
// nothing is rounded. A basis that sums to zero is an error.
func alignedBasis(weights []Weight) (basis, error) {
	weightScale := int32(0)
	for _, w := range weights {
		if s := decScale(w.Capacity); s > weightScale {
			weightScale = s
		}
	}
	b := basis{clusters: make([]string, len(weights)), units: make([]*big.Int, len(weights)), sum: new(big.Int)}
	for i, w := range weights {
		b.clusters[i] = w.Cluster
		b.units[i] = unitsAt(w.Capacity, weightScale)
		b.sum.Add(b.sum, b.units[i])
	}
	if b.sum.Sign() == 0 {
		return basis{}, fmt.Errorf("every cluster reports zero capacity, so there is nothing to be proportional to")
	}
	return b, nil
}

// portion is one cluster's place in the remainder pass: its index and the
// remainder its whole quotient left behind.
type portion struct {
	index     int
	remainder *big.Int
}

// apportion splits units by largest remainder. Every cluster takes its whole
// quotient units*weight/sum, which leaves at most one unit per cluster
// unplaced; those go one each to the largest remainders. An exact tie goes to
// the lexicographically first cluster, so the result is a function of the
// inputs and not of slice order.
func (b basis) apportion(units *big.Int) []*big.Int {
	assigned := make([]*big.Int, len(b.units))
	placed := new(big.Int)
	portions := make([]portion, len(b.units))
	for i, weight := range b.units {
		product := new(big.Int).Mul(units, weight)
		quotient, remainder := new(big.Int).QuoRem(product, b.sum, new(big.Int))
		assigned[i] = quotient
		placed.Add(placed, quotient)
		portions[i] = portion{index: i, remainder: remainder}
	}
	sort.Slice(portions, func(x, y int) bool {
		if c := portions[x].remainder.Cmp(portions[y].remainder); c != 0 {
			return c > 0
		}
		return b.clusters[portions[x].index] < b.clusters[portions[y].index]
	})
	one := big.NewInt(1)
	for leftover := new(big.Int).Sub(units, placed); leftover.Sign() > 0; leftover.Sub(leftover, one) {
		i := portions[0].index
		portions = portions[1:]
		assigned[i].Add(assigned[i], one)
	}
	return assigned
}

// Sum adds shares up, so a caller can check an Explicit split against the total
// the admin wrote. Explicit needs no apportionment, only that check.
func Sum(shares []Share) resource.Quantity {
	total := resource.Quantity{}
	for _, s := range shares {
		total.Add(s.Nominal)
	}
	return total
}

// decScale reports the decimal scale a quantity needs to be represented
// without loss. Taken from a copy: AsDec rewrites the receiver's internal
// representation, and these quantities belong to the caller.
func decScale(q resource.Quantity) int32 {
	local := q
	return int32(local.AsDec().Scale())
}

// unitsAt renders a quantity as a whole number of 10^-scale units. scale is the
// largest any input needs, so every value divides into it exactly and nothing
// is rounded here.
func unitsAt(q resource.Quantity, scale int32) *big.Int {
	local := q
	dec := local.AsDec()
	units := new(big.Int).Set(dec.UnscaledBig())
	if shift := scale - int32(dec.Scale()); shift > 0 {
		units.Mul(units, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(shift)), nil))
	}
	return units
}

// quantityAt rebuilds a quantity from whole 10^-scale units, keeping the
// total's format so a binary-suffixed budget does not come back in decimal.
func quantityAt(units *big.Int, scale int32, format resource.Format) resource.Quantity {
	dec := inf.NewDecBig(new(big.Int).Set(units), inf.Scale(scale))
	return *resource.NewDecimalQuantity(*dec, format)
}
