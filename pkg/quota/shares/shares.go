// Package shares splits a fleet-wide allowance into per-cluster ones.
//
// A management plane authors one nominal for the whole fleet, but a workload
// cluster can only enforce a number of its own: Kueue's nominalQuota is
// per-cluster, so the fleet total has to be resolved into shares before it can
// be projected. This is that resolution, and nothing else — no I/O, no
// knowledge of clusters beyond the names and weights it is handed.
//
// The one contract that matters is that the shares sum to the total EXACTLY.
// A split that loses a chip to rounding silently shrinks the fleet's usable
// capacity; one that gains a chip lets the fleet admit more than an admin
// authorized, which is the failure the whole tree exists to prevent. Both are
// invisible in any single cluster's status, so neither would be noticed from
// the outside.
//
// Exactness rules out floating point and rules out truncating division. The
// method here is largest-remainder: give every cluster its floor, then hand the
// units that division could not place to whoever was closest to earning one.
// Arithmetic is on inf.Dec rather than int64 because a budget may name any
// resource — cpu carries milli precision, and memory in binary units overflows
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
// function of the inputs alone — the same inputs always produce the same split,
// including which clusters receive the remainder. Determinism is not cosmetic:
// a split that reshuffled between reconciles would rewrite every projected CR
// and re-materialize every ClusterQueue in the fleet for no reason.
//
// Shares come back in the unit the total was written in: a budget of 4 splits
// into whole numbers, a budget of 4000m into thousandths. That is the only
// signal available about whether the resource can be subdivided at all, and
// getting it wrong writes a fractional accelerator quota that admits nothing.
//
// A total of zero is a valid split into zeroes; a cluster of zero weight is a
// valid share of zero. What cannot be resolved is a basis that is entirely
// zero — nothing to be proportional to — and that is an error rather than an
// even split, because an even split is a different policy the admin did not ask
// for.
func Proportional(total resource.Quantity, weights []Weight) ([]Share, error) {
	if len(weights) == 0 {
		return nil, fmt.Errorf("no clusters to split across")
	}
	if total.Sign() < 0 {
		return nil, fmt.Errorf("total %s is negative", total.String())
	}

	seen := make(map[string]struct{}, len(weights))
	for _, w := range weights {
		if w.Cluster == "" {
			return nil, fmt.Errorf("a weight carries no cluster name")
		}
		if _, dup := seen[w.Cluster]; dup {
			return nil, fmt.Errorf("cluster %s appears twice", w.Cluster)
		}
		seen[w.Cluster] = struct{}{}
		if w.Capacity.Sign() < 0 {
			return nil, fmt.Errorf("cluster %s has negative capacity %s", w.Cluster, w.Capacity.String())
		}
	}

	// The unit of the answer is the unit the admin wrote the budget in, and the
	// weights do not get a vote. They set the ratio only, and a ratio is
	// scale-free, so aligning them among themselves is enough.
	//
	// This is what keeps an indivisible resource indivisible. Four chips over
	// three clusters is 2/1/1, never 1.33 each: a fractional accelerator is a
	// number Kueue will store and no pod can ever be admitted against. An
	// operator who wants a finer split writes a finer budget — 4000m — and gets
	// thousandths, which is the right answer for cpu and the wrong one for
	// silicon. Only the person writing the budget knows which they have.
	scale := decScale(total)
	units := unitsAt(total, scale)

	weightScale := int32(0)
	for _, w := range weights {
		if s := decScale(w.Capacity); s > weightScale {
			weightScale = s
		}
	}
	basis := new(big.Int)
	weighted := make([]*big.Int, len(weights))
	for i, w := range weights {
		weighted[i] = unitsAt(w.Capacity, weightScale)
		basis.Add(basis, weighted[i])
	}
	if basis.Sign() == 0 {
		return nil, fmt.Errorf("every cluster reports zero capacity, so there is nothing to be proportional to")
	}

	// Floor first, remainders second. floor_i = total*w_i/basis leaves at most
	// len(weights)-1 units unplaced, which is what the pass below hands out.
	type portion struct {
		index     int
		remainder *big.Int
	}
	assigned := make([]*big.Int, len(weights))
	placed := new(big.Int)
	portions := make([]portion, len(weights))
	for i := range weights {
		product := new(big.Int).Mul(units, weighted[i])
		quotient, remainder := new(big.Int).QuoRem(product, basis, new(big.Int))
		assigned[i] = quotient
		placed.Add(placed, quotient)
		portions[i] = portion{index: i, remainder: remainder}
	}

	// Largest remainder wins, and an exact tie goes to the lexicographically
	// first cluster. The tiebreak exists only to make the result a function of
	// the inputs: without it the winner would depend on slice order, and a
	// caller that listed its clusters differently would rewrite the fleet.
	sort.Slice(portions, func(a, b int) bool {
		if c := portions[a].remainder.Cmp(portions[b].remainder); c != 0 {
			return c > 0
		}
		return weights[portions[a].index].Cluster < weights[portions[b].index].Cluster
	})

	one := big.NewInt(1)
	for leftover := new(big.Int).Sub(units, placed); leftover.Sign() > 0; leftover.Sub(leftover, one) {
		i := portions[0].index
		portions = portions[1:]
		assigned[i].Add(assigned[i], one)
	}

	out := make([]Share, len(weights))
	for i, w := range weights {
		out[i] = Share{
			Cluster: w.Cluster,
			Nominal: quantityAt(assigned[i], scale, total.Format),
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Cluster < out[b].Cluster })
	return out, nil
}

// Sum adds shares up, so a caller can check an Explicit split against the total
// the admin wrote. Explicit needs no apportionment, only that verdict.
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
