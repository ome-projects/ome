// Package routing computes the capacity-aware traffic weights that project a
// multi-cluster InferenceService's placement onto a gateway-neutral routing
// table. Placement decides WHERE replicas run and quota decides HOW MUCH each
// home may run; this package decides the WEIGHTS a gateway consumes.
//
// The weight math is pure: it depends only on per-home capacity inputs, has no
// Kubernetes or controller-runtime dependencies, and is exhaustively unit
// tested. The routing controller adapts placement status into these inputs and
// writes the results onto a TrafficMap.
package routing

import (
	"math/big"

	"k8s.io/apimachinery/pkg/api/resource"
)

// maxTrafficMapWeight is the strictest limit among the built-in TrafficMap
// consumers. Gateway API BackendRef.Weight rejects values above this bound.
const maxTrafficMapWeight = 1_000_000

// Home is the capacity input for one serving home's weight.
type Home struct {
	// Cluster identifies the home; carried through so a caller can associate the
	// index-aligned output weight back to its home.
	Cluster string

	// Allocated is the number of replicas the home is intended to run — the count
	// basis of the weight (Split apportionment intersected with the home's
	// quota-admitted count).
	Allocated int32

	// Ready is the home's live ready-replica count. It caps the count basis of
	// the weight, so partial readiness changes traffic proportionally. Zero gates
	// the home's weight to 0.
	Ready int32

	// Factor is the per-replica relative serving capacity of the home's
	// accelerator (baseline 1, higher = faster), so heterogeneous hardware is
	// weighted by capacity rather than raw replica count. Nil or non-positive is
	// treated as the multiplicative identity (1) — a home is never black-holed by
	// a missing or malformed factor. It is operator-supplied config, never
	// derived from raw hardware FLOPS.
	Factor *resource.Quantity

	// Reported is the home's own count of what it can currently serve, present
	// only when a capacity source is configured and the home answered. It is a
	// CEILING on Allocated and never raises it.
	//
	// Only the home knows its realized prefill/decode pairing: the control plane
	// computes MIN(ready_prefill, ready_decode), which is an upper bound on
	// usable pairs rather than a count of them. Allowing a report to raise a
	// share would reintroduce the reactive drift capacity planning exists to
	// remove, and would let a hot, buggy or stale home claim more traffic than
	// the plan ever granted it. Constrained to a ceiling it can say only "I
	// cannot yet serve what you planned for me".
	//
	// Nil means no report — the plan stands. A reported 0 is a real signal
	// (the home can serve nothing) and is distinct from nil, which is why this
	// is a pointer.
	Reported *int32

	// Reachable is the verdict of the active end-to-end probe against the home's
	// externally-addressable endpoint, ANDed into the health gate.
	//
	// Nil means the home was not probed — probing is off, or no verdict has been
	// reached yet — and never gates. That state is uniform across homes when the
	// prober itself is broken, so treating it as failure would zero the entire
	// fleet at once. False gates the home to weight 0; it is set only once the
	// caller's consecutive-failure threshold has been met, so a single failed
	// probe cannot move a large traffic share.
	Reachable *bool
}

// EffectiveAllocated is the home's allocation after the reported ceiling is
// applied: min(Allocated, Reported), or Allocated when the home did not report.
// It is the count the weight is actually computed from, and the value a caller
// should record as the entry's allocation provenance so the written table
// explains the weight it carries.
func (h Home) EffectiveAllocated() int32 {
	if h.Reported == nil || *h.Reported >= h.Allocated {
		return h.Allocated
	}
	if *h.Reported < 0 {
		// A negative report is malformed, not an instruction to serve nothing.
		// Fail open to the plan rather than silently black-holing the home.
		return h.Allocated
	}
	return *h.Reported
}

// RoutableReplicas is the capacity that is both allocated and currently ready.
// Readiness may lower an allocation but never raise it.
func (h Home) RoutableReplicas() int32 {
	allocated := h.EffectiveAllocated()
	if h.Ready < 0 {
		return 0
	}
	if h.Ready < allocated {
		return h.Ready
	}
	return allocated
}

// Weights computes the final, apply-verbatim traffic weights for a set of
// serving homes, index-aligned with homes.
//
// For each home: raw_h = min(effectiveAllocated_h, ready_h) * factor_h, gated to
// 0 when its probe says it is unreachable. The raw values
// are then reduced to their smallest whole-number ratio, yielding relative
// non-negative integers matching Gateway API and Envoy weighted-cluster
// semantics — a consumer applies them verbatim or divides by their sum for a
// percentage. A ratio that cannot fit the strictest built-in publisher limit is
// scaled proportionally into that range while preserving every positive arm.
//
// An all-zero capacity result remains all-zero. PreserveTraffic may ignore only
// probe gates, and only when every home has a conclusive failing verdict; the
// fallback still honors admitted, ready, and reported capacity.
func Weights(homes []Home) []int32 {
	return weights(homes, AllFailedPolicyPreserveTraffic)
}

// weights applies PreserveTraffic only when every home has a conclusive failing
// probe verdict. Unknown probe state never authorizes the fallback, and Drain
// preserves the all-zero result.
func weights(homes []Home, allFailedPolicy AllFailedPolicy) []int32 {
	if len(homes) == 0 {
		return []int32{}
	}

	raw, gcd := rawWeights(homes, false)
	if gcd.Sign() != 0 {
		return normalizeRawWeights(raw, gcd)
	}
	if allFailedPolicy != AllFailedPolicyPreserveTraffic || !allProbesFailed(homes) {
		return make([]int32, len(homes))
	}

	// Every probe conclusively failed, so PreserveTraffic may ignore only that
	// signal. Recompute from the remaining capacity gates; an unready,
	// unadmitted, or zero-reported-capacity home stays at zero.
	raw, gcd = rawWeights(homes, true)
	if gcd.Sign() == 0 {
		return make([]int32, len(homes))
	}
	return normalizeRawWeights(raw, gcd)
}

// rawWeights returns each home's unnormalized capacity and their GCD. When
// ignoreProbeGates is true, admitted, ready, and reported capacity still apply.
func rawWeights(homes []Home, ignoreProbeGates bool) ([]*big.Int, *big.Int) {
	// raw_h in milli-units: allocated * (factor scaled by 1000). Arbitrary-
	// precision integers keep both the Quantity conversion and multiplication
	// safe before the ratio is reduced to TrafficMap's int32 representation.
	raw := make([]*big.Int, len(homes))
	gcd := new(big.Int)
	for i := range homes {
		raw[i] = new(big.Int)
		h := &homes[i]
		capacity := h.RoutableReplicas()
		if capacity <= 0 || (!ignoreProbeGates && h.Reachable != nil && !*h.Reachable) {
			continue
		}
		raw[i].Mul(big.NewInt(int64(capacity)), factorMilli(h.Factor))
		gcd = new(big.Int).GCD(nil, nil, gcd, raw[i])
	}
	return raw, gcd
}

func allProbesFailed(homes []Home) bool {
	if len(homes) == 0 {
		return false
	}
	for i := range homes {
		if homes[i].Reachable == nil || *homes[i].Reachable {
			return false
		}
	}
	return true
}

// factorMilli returns the capacity factor scaled by 1000 (the resource.Quantity
// milli scale). Nil or non-positive resolves to 1000 — the identity factor 1.0.
// It avoids Quantity.MilliValue because that method may overflow int64.
func factorMilli(q *resource.Quantity) *big.Int {
	if q == nil {
		return big.NewInt(1000)
	}
	if q.Sign() <= 0 {
		return big.NewInt(1000)
	}

	quantity := q.DeepCopy()
	decimal := quantity.AsDec()
	milliExponent := int64(3) - int64(decimal.Scale())
	milli := new(big.Int).Set(decimal.UnscaledBig())
	if milliExponent >= 0 {
		return milli.Mul(milli, powerOfTen(milliExponent))
	}

	divisor := powerOfTen(-milliExponent)
	return ceilPositiveQuotient(milli, divisor)
}

// normalizeRawWeights first reduces the exact ratio by its GCD. If the reduced
// ratio cannot fit a built-in publisher, it scales every arm proportionally so
// the largest maps exactly to the supported limit, rounding positive arms
// upward so no serving home disappears. Exact representable ratios stay exact;
// unrepresentable ratios remain ordered, non-negative, and routable.
func normalizeRawWeights(raw []*big.Int, gcd *big.Int) []int32 {
	weights := make([]int32, len(raw))
	max := new(big.Int)
	for i := range raw {
		raw[i].Quo(raw[i], gcd)
		if raw[i].Cmp(max) > 0 {
			max.Set(raw[i])
		}
	}

	limit := big.NewInt(maxTrafficMapWeight)
	for i := range raw {
		if raw[i].Sign() == 0 {
			continue
		}
		if max.Cmp(limit) <= 0 {
			weights[i] = int32(raw[i].Int64())
			continue
		}
		scaled := new(big.Int).Mul(raw[i], limit)
		weights[i] = int32(ceilPositiveQuotient(scaled, max).Int64())
	}
	return weights
}

func ceilPositiveQuotient(numerator, denominator *big.Int) *big.Int {
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, denominator, remainder)
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}

func powerOfTen(exponent int64) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(exponent), nil)
}
