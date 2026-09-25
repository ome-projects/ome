package utils

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestPtrIntOrStringFromString(t *testing.T) {
	p := PtrIntOrStringFromString("25%")
	if p == nil {
		t.Fatal("PtrIntOrStringFromString returned nil")
	}
	if p.Type != intstr.String {
		t.Fatalf("Type = %v, want String", p.Type)
	}
	if p.StrVal != "25%" {
		t.Fatalf("StrVal = %q, want %q", p.StrVal, "25%")
	}
	// Distinct addresses on each call (the value is addressable per call).
	q := PtrIntOrStringFromString("25%")
	if p == q {
		t.Fatal("expected distinct pointers from separate calls")
	}
}

// legacySurgeFromPercent is the reference percent-to-count rounding: the
// oracle ScaledCountFromIntOrString must match on the percent branch.
func legacySurgeFromPercent(percent int, replicas int32) int32 {
	if percent <= 0 || replicas <= 0 {
		return 0
	}
	if percent > 100 {
		percent = 100
	}
	num := int64(replicas) * int64(percent)
	out := num / 100
	if num%100 != 0 {
		out++
	}
	if out > int64(replicas) {
		out = int64(replicas)
	}
	return int32(out)
}

// legacyStrPercent mirrors the deleted strPercent / parsePercentString.
func legacyStrPercent(s string) int {
	if len(s) == 0 || s[len(s)-1] != '%' {
		return 0
	}
	out := 0
	for _, ch := range s[:len(s)-1] {
		if ch < '0' || ch > '9' {
			return 0
		}
		out = out*10 + int(ch-'0')
	}
	return out
}

// legacyMaxSurgeBudget reproduces the deleted coordination MaxSurgeBudget
// int+percent dispatch (== workload resolveIntOrPercentCeil) for the
// non-nil case, which is exactly what ScaledCountFromIntOrString(roundUp)
// replaces.
func legacyMaxSurgeBudget(v intstr.IntOrString, replicas int32) int32 {
	if v.Type == intstr.Int {
		n := int32(v.IntValue())
		if n < 0 {
			return 0
		}
		return n
	}
	return legacySurgeFromPercent(legacyStrPercent(v.StrVal), replicas)
}

func TestScaledCountFromIntOrString_Table(t *testing.T) {
	tests := []struct {
		name    string
		v       *intstr.IntOrString
		total   int32
		roundUp bool
		want    int32
	}{
		{"nil", nil, 4, true, 0},

		// Integer branch — returned as-is, floored at 0, NOT clamped to total.
		{"int zero", p(intstr.FromInt32(0)), 4, true, 0},
		{"int one", p(intstr.FromInt32(1)), 4, true, 1},
		{"int equals total", p(intstr.FromInt32(4)), 4, true, 4},
		{"int exceeds total (no clamp)", p(intstr.FromInt32(7)), 4, true, 7},
		{"int negative -> 0", p(intstr.FromInt32(-1)), 4, true, 0},
		{"int with zero total", p(intstr.FromInt32(2)), 0, true, 2},

		// Percent branch, roundUp=true (ceil) — the surge/budget semantics.
		{"0% -> 0", p(intstr.FromString("0%")), 4, true, 0},
		{"25% of 4 -> 1", p(intstr.FromString("25%")), 4, true, 1},
		{"25% of 1 -> 1 (ceil)", p(intstr.FromString("25%")), 1, true, 1},
		{"50% of 3 -> 2 (ceil)", p(intstr.FromString("50%")), 3, true, 2},
		{"100% of 4 -> 4", p(intstr.FromString("100%")), 4, true, 4},
		{"100% of 0 -> 0", p(intstr.FromString("100%")), 0, true, 0},
		{"150% of 4 -> 4 (clamp to total)", p(intstr.FromString("150%")), 4, true, 4},
		{"200% of 3 -> 3 (clamp to total)", p(intstr.FromString("200%")), 3, true, 3},
		{"33% of 10 -> 4 (ceil 3.3)", p(intstr.FromString("33%")), 10, true, 4},

		// Percent branch, roundUp=false (floor) — the unavailable/floor semantics.
		{"25% of 4 floor -> 1", p(intstr.FromString("25%")), 4, false, 1},
		{"25% of 1 floor -> 0", p(intstr.FromString("25%")), 1, false, 0},
		{"33% of 10 floor -> 3", p(intstr.FromString("33%")), 10, false, 3},
		{"100% of 4 floor -> 4", p(intstr.FromString("100%")), 4, false, 4},

		// Malformed string -> 0 (parse-failure contract).
		{"malformed no percent", p(intstr.FromString("25")), 4, true, 0},
		{"malformed garbage", p(intstr.FromString("abc%")), 4, true, 0},
		{"empty string", p(intstr.FromString("")), 4, true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ScaledCountFromIntOrString(tc.v, tc.total, tc.roundUp)
			if got != tc.want {
				t.Fatalf("ScaledCountFromIntOrString(%v, %d, %v) = %d, want %d",
					tc.v, tc.total, tc.roundUp, got, tc.want)
			}
		})
	}
}

// TestScaledCountFromIntOrString_MatchesLegacy exhaustively compares the
// new helper (roundUp=true) against the deleted hand-rolled
// MaxSurgeBudget/ceilPercent oracle across every int and percent value an
// operator could plausibly write, proving the budget math is unchanged.
func TestScaledCountFromIntOrString_MatchesLegacy(t *testing.T) {
	for total := int32(0); total <= 16; total++ {
		// Integer budgets, including negatives and over-total values.
		for n := int32(-2); n <= 20; n++ {
			v := intstr.FromInt32(n)
			want := legacyMaxSurgeBudget(v, total)
			got := ScaledCountFromIntOrString(&v, total, true)
			if got != want {
				t.Fatalf("int %d total %d: got %d want %d", n, total, got, want)
			}
		}
		// Percent budgets, including >100% (clamp-to-total path).
		for pct := 0; pct <= 200; pct++ {
			v := intstr.FromString(itoaPct(pct))
			want := legacyMaxSurgeBudget(v, total)
			got := ScaledCountFromIntOrString(&v, total, true)
			if got != want {
				t.Fatalf("percent %d%% total %d: got %d want %d", pct, total, got, want)
			}
		}
	}
}

func p(v intstr.IntOrString) *intstr.IntOrString { return &v }

func itoaPct(n int) string {
	// small, dependency-free int->"<n>%"
	if n == 0 {
		return "0%"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits) + "%"
}

func TestAvailabilityFloor_Table(t *testing.T) {
	tests := []struct {
		name           string
		total          int32
		minAvailable   *intstr.IntOrString
		maxUnavailable *intstr.IntOrString
		fallback       *intstr.IntOrString
		want           int32
	}{
		{"no budget -> strict (all)", 10, nil, nil, nil, 10},
		{"total zero -> 0", 0, nil, nil, nil, 0},

		// minAvailable takes precedence.
		{"minAvailable int 8", 10, p(intstr.FromInt32(8)), nil, nil, 8},
		{"minAvailable int over total -> clamp", 3, p(intstr.FromInt32(5)), nil, nil, 3},
		{"minAvailable 50% of 10 -> 5", 10, p(intstr.FromString("50%")), nil, nil, 5},
		{"minAvailable 55% of 10 -> 6 (ceil)", 10, p(intstr.FromString("55%")), nil, nil, 6},
		{"minAvailable wins over maxUnavailable", 10, p(intstr.FromInt32(9)), p(intstr.FromString("25%")), nil, 9},

		// maxUnavailable -> floor = total - unavailable.
		{"maxUnavailable 25% of 10 -> floor 7", 10, nil, p(intstr.FromString("25%")), nil, 7},
		{"maxUnavailable int 3 of 10 -> floor 7", 10, nil, p(intstr.FromInt32(3)), nil, 7},
		{"maxUnavailable exceeds total -> floor 0", 3, nil, p(intstr.FromInt32(5)), nil, 0},
		{"maxUnavailable 100% -> floor 0", 4, nil, p(intstr.FromString("100%")), nil, 0},

		// fallback (rollout budget) only when min/max unset.
		{"fallback used when others nil", 10, nil, nil, p(intstr.FromString("25%")), 7},
		{"maxUnavailable wins over fallback", 10, nil, p(intstr.FromString("10%")), p(intstr.FromString("50%")), 9},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := AvailabilityFloor(tc.total, tc.minAvailable, tc.maxUnavailable, tc.fallback)
			if got != tc.want {
				t.Fatalf("AvailabilityFloor(%d, min=%v, max=%v, fb=%v) = %d, want %d",
					tc.total, tc.minAvailable, tc.maxUnavailable, tc.fallback, got, tc.want)
			}
		})
	}
}

// A percentage large enough to scale past MaxInt32 must still clamp to total:
// comparing after narrowing to int32 lets the value wrap and slip through.
func TestScaledCountFromIntOrString_ClampsAboveMaxInt32(t *testing.T) {
	huge := intstr.FromString("300000000%")
	got := ScaledCountFromIntOrString(&huge, 1000, true)
	if got != 1000 {
		t.Fatalf("ScaledCountFromIntOrString(300000000%%, total=1000) = %d, want 1000", got)
	}
	if got < 0 {
		t.Fatalf("returned a negative count: %d", got)
	}
}
