// Package routing computes the capacity-aware traffic weights that project a
// multi-cluster InferenceService's placement onto a gateway-neutral routing
// table. Placement decides WHERE replicas run and quota decides HOW MUCH each
// cluster may run; this package decides the WEIGHTS a gateway consumes.
//
// The weight math is pure: it depends only on per-cluster capacity inputs, has no
// Kubernetes API calls or controller-runtime dependencies. The routing
// controller adapts placement status and endpoint reports into these inputs and
// writes the results onto a TrafficMap.
package routing

import (
	"math/big"

	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// maxTrafficMapWeight is the strictest limit among the built-in TrafficMap
// consumers. Gateway API BackendRef.Weight rejects values above this bound.
const maxTrafficMapWeight = 1_000_000

// ClusterCapacity is the capacity input for one serving cluster's weight.
type ClusterCapacity struct {
	// Mode selects the capacity authority. In Reported mode a usable
	// ReportedWeight is the cluster's capacity in replica units: it already
	// includes hardware and workload normalization, and replica counts do not
	// cap it. Without one the cluster keeps its replica count, so a reporter outage
	// degrades to the replica-based weight rather than to zero.
	Mode           v1beta1.RoutingCapacityMode
	ReportedWeight *resource.Quantity

	// Cluster is the cluster name; carried through so a caller can associate the
	// index-aligned output weight back to its cluster.
	Cluster string

	// Allocated is the intended replica count — the count basis of the weight
	// (Split apportionment intersected with the cluster's quota-admitted count).
	// A usable ReportedWeight replaces it.
	Allocated int32

	// Ready caps the count basis of the weight, so partial readiness changes
	// traffic proportionally. Zero gates the cluster's weight to 0. A usable
	// ReportedWeight is not capped by it, and the routing controller supplies
	// a positive one only while Ready is positive.
	Ready int32

	// Factor applies whenever the replica count is the basis: the per-replica
	// capacity of the cluster's accelerator (baseline 1, higher = faster), so
	// heterogeneous hardware is weighted by capacity rather than raw replica
	// count. A usable ReportedWeight already includes it. Nil or non-positive is
	// treated as the multiplicative identity (1) — a cluster is never black-holed by
	// a missing or malformed factor. It is operator-supplied config, never
	// derived from raw hardware FLOPS.
	Factor *resource.Quantity

	// Reported is the accepted replica count in Ceiling mode. It can lower
	// Allocated but never raise it. Nil leaves Allocated unchanged; zero says
	// the cluster cannot serve traffic. Reported mode uses ReportedWeight instead.
	Reported *int32

	// Reachable is the verdict of the active end-to-end probe against the cluster's
	// externally-addressable endpoint, ANDed into the health gate.
	//
	// Nil means the cluster was not probed — probing is off, or no verdict has been
	// reached yet — and never gates. That state is uniform across clusters when the
	// prober itself is broken, so treating it as failure would zero the entire
	// fleet at once. False gates the cluster to weight 0; it is set only once the
	// caller's consecutive-failure threshold has been met, so a single failed
	// probe cannot move a large traffic share.
	Reachable *bool
}

// EffectiveAllocated is the cluster's allocation after the reported ceiling is
// applied: min(Allocated, Reported), or Allocated when the cluster did not report.
// A usable ReportedWeight replaces this count when calculating weight.
func (h ClusterCapacity) EffectiveAllocated() int32 {
	if h.Reported == nil || *h.Reported >= h.Allocated {
		return h.Allocated
	}
	if *h.Reported < 0 {
		// A negative report is malformed, not an instruction to serve nothing.
		// Fail open to the plan rather than silently black-holing the cluster.
		return h.Allocated
	}
	return *h.Reported
}

// RoutableReplicas is the capacity that is both allocated and currently ready.
// Readiness may lower an allocation but never raise it.
func (h ClusterCapacity) RoutableReplicas() int32 {
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
// serving clusters, index-aligned with clusters.
//
// Ceiling mode uses min(effectiveAllocated, ready) * factor. Reported mode uses
// a usable ReportedWeight directly and the Ceiling formula for a cluster without
// one. A failed probe gates either to zero. The raw values are then reduced to
// their smallest whole-number ratio, yielding relative non-negative integers
// matching Gateway API and Envoy weighted-cluster semantics — a consumer
// applies them verbatim or divides by their sum for a percentage. A ratio that cannot fit the strictest built-in publisher limit is
// scaled proportionally into that range while preserving every positive arm.
//
// An all-zero capacity result remains all-zero. PreserveTraffic may ignore only
// probe gates, and only when every cluster has a conclusive failing verdict; the
// fallback still honors the selected capacity mode.
func Weights(clusters []ClusterCapacity) []int32 {
	return weights(clusters, AllFailedPolicyPreserveTraffic)
}

// weights applies PreserveTraffic only when every cluster has a conclusive failing
// probe verdict. Unknown probe state never authorizes the fallback, and Drain
// preserves the all-zero result.
func weights(clusters []ClusterCapacity, allFailedPolicy AllFailedPolicy) []int32 {
	if len(clusters) == 0 {
		return []int32{}
	}

	raw, gcd := rawWeights(clusters, false)
	if gcd.Sign() != 0 {
		return normalizeRawWeights(raw, gcd)
	}
	if allFailedPolicy != AllFailedPolicyPreserveTraffic || !allProbesFailed(clusters) {
		return make([]int32, len(clusters))
	}

	// Every probe conclusively failed, so PreserveTraffic may ignore only that
	// signal. Recompute from the selected capacity mode; a zero reported weight
	// stays at zero, and a cluster without one weighs its ready replicas.
	raw, gcd = rawWeights(clusters, true)
	if gcd.Sign() == 0 {
		return make([]int32, len(clusters))
	}
	return normalizeRawWeights(raw, gcd)
}

// rawWeights returns each cluster's unnormalized capacity and their GCD. When
// ignoreProbeGates is true, the selected capacity mode still applies.
func rawWeights(clusters []ClusterCapacity, ignoreProbeGates bool) ([]*big.Int, *big.Int) {
	values := make([]*big.Rat, len(clusters))
	denominator := big.NewInt(1)
	for i := range clusters {
		values[i] = new(big.Rat)
		h := &clusters[i]
		if !ignoreProbeGates && h.Reachable != nil && !*h.Reachable {
			continue
		}
		values[i] = h.capacityValue()
		gcd := new(big.Int).GCD(nil, nil, denominator, values[i].Denom())
		denominator.Mul(new(big.Int).Quo(denominator, gcd), values[i].Denom())
	}
	raw := make([]*big.Int, len(clusters))
	gcd := new(big.Int)
	for i, value := range values {
		raw[i] = new(big.Int).Mul(value.Num(), new(big.Int).Quo(denominator, value.Denom()))
		gcd.GCD(nil, nil, gcd, raw[i])
	}
	return raw, gcd
}

func (h ClusterCapacity) capacityValue() *big.Rat {
	if h.usesReportedWeight() {
		if h.ReportedWeight.Sign() <= 0 {
			return new(big.Rat)
		}
		return quantityRatio(h.ReportedWeight)
	}
	if n := h.RoutableReplicas(); n > 0 {
		return new(big.Rat).SetFrac(new(big.Int).Mul(big.NewInt(int64(n)), factorMilli(h.Factor)), big.NewInt(1000))
	}
	return new(big.Rat)
}

func (h ClusterCapacity) hasReadyCapacity() bool {
	if h.usesReportedWeight() {
		return h.capacityValue().Sign() > 0
	}
	return h.Ready > 0
}

// usesReportedWeight reports whether the cluster's capacity is its reported
// weight rather than its replica count.
func (h ClusterCapacity) usesReportedWeight() bool {
	return h.Mode == v1beta1.RoutingCapacityModeReported && h.ReportedWeight != nil
}

// appliedFactor is the factor the weight multiplies by: none for a reported
// weight, which already includes the hardware.
func (h ClusterCapacity) appliedFactor() *resource.Quantity {
	if h.usesReportedWeight() {
		return nil
	}
	return h.Factor
}

func quantityRatio(q *resource.Quantity) *big.Rat {
	copy := q.DeepCopy()
	decimal := copy.AsDec()
	n := new(big.Int).Set(decimal.UnscaledBig())
	scale := int64(decimal.Scale())
	if scale < 0 {
		return new(big.Rat).SetInt(n.Mul(n, powerOfTen(-scale)))
	}
	return new(big.Rat).SetFrac(n, powerOfTen(scale))
}

func allProbesFailed(clusters []ClusterCapacity) bool {
	if len(clusters) == 0 {
		return false
	}
	for i := range clusters {
		if clusters[i].Reachable == nil || *clusters[i].Reachable {
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
// upward so no serving cluster disappears. Exact representable ratios stay exact;
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
