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

import "k8s.io/apimachinery/pkg/api/resource"

// Home is the capacity input for one serving home's weight.
type Home struct {
	// Cluster identifies the home; carried through so a caller can associate the
	// index-aligned output weight back to its home.
	Cluster string

	// Allocated is the number of replicas the home is intended to run — the count
	// basis of the weight (Split apportionment intersected with the home's
	// quota-admitted count).
	Allocated int32

	// Ready is the home's live ready-replica count — the binary health signal.
	// Zero gates the home's weight to 0 (unless every home is unhealthy).
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

// serving reports whether the home may carry traffic at all: it must have ready
// replicas, must not be probe-gated, and must have something left to serve after
// the reported ceiling.
func (h Home) serving() bool {
	if h.Ready <= 0 || h.EffectiveAllocated() <= 0 {
		return false
	}
	return h.Reachable == nil || *h.Reachable
}

// Weights computes the final, apply-verbatim traffic weights for a set of
// serving homes, index-aligned with homes.
//
// For each home: raw_h = effectiveAllocated_h * factor_h, gated to 0 when the
// home has no ready replicas or its probe says it is unreachable. The raw values
// are then reduced to their smallest whole-number ratio, yielding relative
// non-negative integers matching Gateway API and Envoy weighted-cluster
// semantics — a consumer applies them verbatim or divides by their sum for a
// percentage.
//
// When every home would be zero — all unhealthy, all unreachable, or nothing
// allocated — all homes are given equal weight 1, so traffic is never
// black-holed. That fallback is what bounds the blast radius of the observed
// inputs: a prober bug or a control-plane partition marks every home down at
// once, and spreading traffic beats dropping it.
func Weights(homes []Home) []int32 {
	weights := make([]int32, len(homes))
	if len(homes) == 0 {
		return weights
	}

	// raw_h in milli-units: allocated * (factor scaled by 1000). Milli keeps a
	// fractional factor (e.g. 0.5) exact without floating point; the shared 1000
	// scale cancels in the ratio reduction below.
	raw := make([]int64, len(homes))
	var g int64
	for i := range homes {
		h := &homes[i]
		if !h.serving() {
			continue // health-gated, probe-gated, or nothing to serve → weight 0
		}
		raw[i] = int64(h.EffectiveAllocated()) * factorMilli(h.Factor)
		g = gcd(g, raw[i])
	}

	if g == 0 {
		// Every home is zero — equal-weight fallback so traffic is never
		// black-holed.
		for i := range weights {
			weights[i] = 1
		}
		return weights
	}

	for i := range raw {
		weights[i] = int32(raw[i] / g)
	}
	return weights
}

// factorMilli returns the capacity factor scaled by 1000 (the resource.Quantity
// milli scale). Nil or non-positive resolves to 1000 — the identity factor 1.0.
func factorMilli(q *resource.Quantity) int64 {
	if q == nil {
		return 1000
	}
	m := q.MilliValue()
	if m <= 0 {
		return 1000
	}
	return m
}

// gcd is the greatest common divisor, treating 0 as the identity so it composes
// over a running fold that skips zero-weight homes.
func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	return a
}
