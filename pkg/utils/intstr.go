package utils

import "k8s.io/apimachinery/pkg/util/intstr"

// PtrIntOrStringFromString returns a pointer to an IntOrString carrying
// the supplied percentage string (e.g. "25%"). Convenience for pacing
// defaulters that need an addressable percent literal.
func PtrIntOrStringFromString(s string) *intstr.IntOrString {
	out := intstr.FromString(s)
	return &out
}

// ScaledCountFromIntOrString turns a budget into a count of at least 0: an integer
// as written, even above total; a percentage of total scaled (ceil when roundUp,
// floor otherwise) and capped at total; nil or a malformed string as 0.
func ScaledCountFromIntOrString(v *intstr.IntOrString, total int32, roundUp bool) int32 {
	if v == nil {
		return 0
	}
	if v.Type == intstr.Int {
		n := int32(v.IntValue())
		if n < 0 {
			return 0
		}
		return n
	}
	n, err := intstr.GetScaledValueFromIntOrPercent(v, int(total), roundUp)
	if err != nil || n < 0 {
		return 0
	}
	// Compare at full width: narrowing first lets a value above MaxInt32
	// wrap and slip past the clamp.
	if total >= 0 && n > int(total) {
		return total
	}
	return int32(n)
}

// AvailabilityFloor returns the minimum number of pods (out of total) that must
// remain available to satisfy a disruption budget, in precedence order:
//
//   - minAvailable set → that many must be available (a percent resolves via
//     ceil, then clamped to total);
//   - else maxUnavailable set → total minus that many may be down (floored at 0);
//   - else fallbackMaxUnavailable set → total minus that many (floored at 0);
//   - else total (strict — all must be available).
//
// Percent budgets resolve with ScaledCountFromIntOrString (ceil), the same
// resolver the rollout drain-budget math uses, so an availability check and the
// pacing gate agree byte-for-byte. total <= 0 yields 0.
func AvailabilityFloor(total int32, minAvailable, maxUnavailable, fallbackMaxUnavailable *intstr.IntOrString) int32 {
	if total <= 0 {
		return 0
	}
	if minAvailable != nil {
		f := ScaledCountFromIntOrString(minAvailable, total, true)
		if f > total {
			return total
		}
		return f
	}
	mu := maxUnavailable
	if mu == nil {
		mu = fallbackMaxUnavailable
	}
	if mu != nil {
		if f := total - ScaledCountFromIntOrString(mu, total, true); f > 0 {
			return f
		}
		return 0
	}
	return total
}
